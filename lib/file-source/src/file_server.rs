use std::{
    cmp,
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{self, Duration},
};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use file_source_common::{
    FileFingerprint, FileSourceInternalEvents, Fingerprinter, ReadFrom,
    checkpointer::{Checkpointer, CheckpointsView},
};
use futures::{
    Future, Sink, SinkExt,
    future::{Either, select},
};
use futures_util::future::join_all;
use indexmap::IndexMap;
use tokio::{
    fs::{self, remove_file},
    task::{Id, JoinSet},
    time::sleep,
};

use tracing::{debug, error, info, trace, warn};
use vector_common::internal_event::{DeliveryReadEvent, emit};

use crate::{
    FileTTLAction, FileTTLRemovalConfig,
    file_watcher::{FileWatcher, RawLineResult},
    paths_provider::{LogFileInfo, PathsProvider},
};

/// `FileServer` is a Source which cooperatively schedules reads over files,
/// converting the lines of said files into `LogLine` structures. As
/// `FileServer` is intended to be useful across multiple operating systems with
/// POSIX filesystem semantics `FileServer` must poll for changes. That is, no
/// event notification is used by `FileServer`.
///
/// `FileServer` is configured on a path to watch. The files do _not_ need to
/// exist at startup. `FileServer` will discover new files which match
/// its path in at most 60 seconds.
pub struct FileServer<PP, E: FileSourceInternalEvents>
where
    PP: PathsProvider,
{
    pub paths_provider: PP,
    pub max_read_bytes: usize,
    pub ignore_checkpoints: bool,
    pub read_from: ReadFrom,
    pub ignore_before: Option<DateTime<Utc>>,
    pub start_reading_at: Option<DateTime<Utc>>,
    pub max_line_bytes: usize,
    pub line_delimiter: Bytes,
    pub data_dir: PathBuf,
    pub glob_minimum_cooldown: Duration,
    pub fingerprinter: Fingerprinter,
    pub oldest_first: bool,
    pub remove_after: Option<Duration>,
    pub emitter: E,
    pub rotate_wait: Duration,
    pub ttl_removal_config: Option<FileTTLRemovalConfig>,
    // Source context is plumbed into the delivery event logs for extra metadata
    // This actually has to get converted to JSON string deeper in the stack for each emitted event
    // But since the performance hit of doing this shouldn't be too high, we keep it as a normal
    // map that can be normally used until we have to emit the event
    pub source_context: Option<HashMap<String, String>>,
    pub file_to_pod_map: Option<Arc<Mutex<HashMap<PathBuf, LogFileInfo>>>>,
    pub drain_on_shutdown: bool,
    /// File extensions that identify immutable archive files (e.g. `["gz"]`).
    /// These files are fingerprinted once and the mapping is cached, and they
    /// are marked as done when EOF is reached so they are never re-read.
    pub archive_extensions: Vec<String>,
}

/// `FileServer` as Source
///
/// The 'run' of `FileServer` performs the cooperative scheduling of reads over
/// `FileServer`'s configured files. Much care has been taking to make this
/// scheduling 'fair', meaning busy files do not drown out quiet files or vice
/// versa but there's no one perfect approach. Very fast files _will_ be lost if
/// your system aggressively rolls log files. `FileServer` will keep a file
/// handler open but should your system move so quickly that a file disappears
/// before `FileServer` is able to open it the contents will be lost. This should be a
/// rare occurrence.
///
/// Specific operating systems support evented interfaces that correct this
/// problem but your intrepid authors know of no generic solution.
impl<PP, E> FileServer<PP, E>
where
    PP: PathsProvider,
    E: FileSourceInternalEvents,
{
    /// Returns `true` if the path has an extension that matches one of the
    /// configured archive extensions (immutable files that never change).
    fn is_archive(&self, path: &Path) -> bool {
        path.extension().map_or(false, |ext| {
            self.archive_extensions
                .iter()
                .any(|archive_ext| ext == archive_ext.as_str())
        })
    }

    // Update the file-to-pod map with the given path and log file info.
    fn update_file_to_pod_map(&mut self, path: PathBuf, log_file_info_opt: Option<LogFileInfo>) {
        if self.file_to_pod_map.is_some() {
            if let Some(log_file_info) = log_file_info_opt {
                self.file_to_pod_map
                    .as_mut()
                    .unwrap()
                    .lock()
                    .unwrap()
                    .insert(path.clone(), log_file_info.clone());
            }
        }
    }
    // The first `shutdown_data` signal here is to stop this file
    // server from outputting new data; the second
    // `shutdown_checkpointer` is for finishing the background
    // checkpoint writer task, which has to wait for all
    // acknowledgements to be completed.
    pub async fn run<C, S1, S2>(
        mut self,
        mut chans: C,
        mut shutdown_data: S1,
        shutdown_checkpointer: S2,
        mut checkpointer: Checkpointer,
    ) -> Result<Shutdown, <C as Sink<Vec<Line>>>::Error>
    where
        C: Sink<Vec<Line>> + Unpin,
        <C as Sink<Vec<Line>>>::Error: std::error::Error,
        S1: Future + Unpin + Send + 'static,
        S2: Future + Unpin + Send + 'static,
    {
        let mut fp_map: IndexMap<FileFingerprint, FileWatcher> = Default::default();

        let mut backoff_cap: usize = 1;
        let mut lines = Vec::new();

        checkpointer.read_checkpoints(self.ignore_before).await;

        let mut known_small_files: HashMap<PathBuf, time::Instant> = HashMap::new();

        let mut existing_files = Vec::new();
        for (log_file_info_opt, path) in self.paths_provider.paths().into_iter() {
            if let Some(file_id) = self
                .fingerprinter
                .fingerprint_or_emit(&path, &mut known_small_files, &self.emitter)
                .await
            {
                self.update_file_to_pod_map(path.clone(), log_file_info_opt);
                existing_files.push((path, file_id));
            }
        }

        let metadata = join_all(
            existing_files
                .iter()
                .map(|(path, _file_id)| fs::metadata(path)),
        )
        .await;

        let created = metadata.into_iter().map(|m| {
            m.and_then(|m| m.created())
                .map(DateTime::<Utc>::from)
                .unwrap_or_else(|_| Utc::now())
        });

        let mut existing_files: Vec<(DateTime<Utc>, PathBuf, FileFingerprint)> = existing_files
            .into_iter()
            .zip(created)
            .map(|((path, file_id), key)| (key, path, file_id))
            .collect();

        existing_files.sort_by_key(|(key, _, _)| *key);

        let checkpoints = checkpointer.view();

        for (_key, path, file_id) in existing_files {
            self.watch_new_file(path, file_id, &mut fp_map, &checkpoints, true)
                .await;
        }
        self.emitter.emit_files_open(fp_map.len());

        let mut stats = TimingStats::default();

        // Wrap the checkpointer in Arc so the drain path can write
        // checkpoints while the checkpoint_writer task is still running.
        let checkpointer = Arc::new(checkpointer);
        let drain_checkpointer = if self.drain_on_shutdown {
            Some(Arc::clone(&checkpointer))
        } else {
            None
        };

        // Spawn the checkpoint writer task
        let checkpoint_task_handle = tokio::spawn(checkpoint_writer(
            checkpointer,
            self.glob_minimum_cooldown,
            shutdown_checkpointer,
            self.emitter.clone(),
        ));

        // Alright friends, how does this work?
        //
        // We want to avoid burning up users' CPUs. To do this we sleep after
        // reading lines out of files. But! We want to be responsive as well. We
        // keep track of a 'backoff_cap' to decide how long we'll wait in any
        // given loop. This cap grows each time we fail to read lines in an
        // exponential fashion to some hard-coded cap. To reduce time using glob,
        // we do not re-scan for major file changes (new files, moves, deletes),
        // or write new checkpoints, on every iteration.
        let mut next_glob_time = time::Instant::now();
        loop {
            // Glob find files to follow, but not too often.
            let now_time = time::Instant::now();
            if next_glob_time <= now_time {
                // Schedule the next glob time.
                next_glob_time = now_time.checked_add(self.glob_minimum_cooldown).unwrap();

                if stats.started_at.elapsed() > Duration::from_secs(1) {
                    stats.report();
                }

                if stats.started_at.elapsed() > Duration::from_secs(10) {
                    stats = TimingStats::default();
                }

                // Search (glob) for files to detect major file changes.
                let start = time::Instant::now();
                for (_file_id, watcher) in &mut fp_map {
                    watcher.set_file_findable(false); // assume not findable until found
                }
                for (log_file_info_opt, path) in self.paths_provider.paths().into_iter() {
                    // Fast path: skip expensive fingerprinting (gzip decompression
                    // + CRC64) for archived files whose path→fingerprint mapping
                    // is already known. Since .gz files are immutable, the
                    // fingerprint will never change for a given path.
                    if !self.ignore_checkpoints && self.is_archive(&path) {
                        if let Some(file_id) = checkpoints.get_archive_fingerprint(&path) {
                            if let Some(watcher) = fp_map.get_mut(&file_id) {
                                watcher.set_file_findable(true);
                            } else if !checkpoints.get_done(file_id) {
                                // Known archive fingerprint but no active watcher
                                // and not done — re-open it to resume reading.
                                self.watch_new_file(
                                    path,
                                    file_id,
                                    &mut fp_map,
                                    &checkpoints,
                                    false,
                                )
                                .await;
                                self.emitter.emit_files_open(fp_map.len());
                            }
                            // If done: skip entirely (no watcher, no fingerprint, no I/O).
                            continue;
                        }
                    }

                    if let Some(file_id) = self
                        .fingerprinter
                        .fingerprint_or_emit(&path, &mut known_small_files, &self.emitter)
                        .await
                    {
                        // Cache the path→fingerprint mapping for archived files so
                        // subsequent glob cycles can skip fingerprinting entirely.
                        if self.is_archive(&path) {
                            checkpoints.set_archive_path(file_id, &path);
                        }

                        if let Some(watcher) = fp_map.get_mut(&file_id) {
                            // file fingerprint matches a watched file
                            let was_found_this_cycle = watcher.file_findable();
                            watcher.set_file_findable(true);
                            if watcher.path == path {
                                trace!(
                                    message = "Continue watching file.",
                                    path = ?path,
                                );
                            } else if !was_found_this_cycle {
                                // matches a file with a different path
                                info!(
                                    message = "Watched file has been renamed.",
                                    path = ?path,
                                    old_path = ?watcher.path
                                );
                                watcher.update_path(path).await.ok(); // ok if this fails: might fix next cycle
                            } else {
                                info!(
                                    message = "More than one file has the same fingerprint.",
                                    path = ?path,
                                    old_path = ?watcher.path
                                );
                                let (old_path, new_path) = (&watcher.path, &path);
                                if let (Ok(old_modified_time), Ok(new_modified_time)) = (
                                    fs::metadata(old_path).await.and_then(|m| m.modified()),
                                    fs::metadata(new_path).await.and_then(|m| m.modified()),
                                ) && old_modified_time < new_modified_time
                                {
                                    info!(
                                        message = "Switching to watch most recently modified file.",
                                        new_modified_time = ?new_modified_time,
                                        old_modified_time = ?old_modified_time,
                                    );
                                    watcher.update_path(path).await.ok(); // ok if this fails: might fix next cycle
                                }
                            }
                        } else {
                            // untracked file fingerprint
                            self.update_file_to_pod_map(path.clone(), log_file_info_opt);
                            self.watch_new_file(path, file_id, &mut fp_map, &checkpoints, false)
                                .await;
                            self.emitter.emit_files_open(fp_map.len());
                        }
                    }
                }
                stats.record("discovery", start.elapsed());
            }

            // Cleanup the known_small_files
            if let Some(grace_period) = self.remove_after {
                let mut set = JoinSet::new();

                let remove_file_tasks: HashMap<Id, PathBuf> = known_small_files
                    .iter()
                    .filter(|&(_path, last_time_open)| last_time_open.elapsed() >= grace_period)
                    .map(|(path, _last_time_open)| path.clone())
                    .map(|path| {
                        let path_ = path.clone();
                        let abort_handle =
                            set.spawn(async move { (path_.clone(), remove_file(&path_).await) });
                        (abort_handle.id(), path)
                    })
                    .collect();

                while let Some(res) = set.join_next().await {
                    match res {
                        Ok((path, Ok(()))) => {
                            let removed = known_small_files.remove(&path);

                            if removed.is_some() {
                                self.emitter.emit_file_deleted(&path);
                            }
                        }
                        Ok((path, Err(err))) => {
                            if err.kind() == std::io::ErrorKind::NotFound {
                                // File already gone (e.g. kubelet cleaned up the pod volume).
                                // Treat as successful deletion.
                                info!(message = "File not found during deletion, assuming it was already removed externally.", path = ?path);
                                known_small_files.remove(&path);
                                self.emitter.emit_file_deleted(&path);
                            } else {
                                self.emitter.emit_file_delete_error(&path, err);
                            }
                        }
                        Err(join_err) => {
                            self.emitter.emit_file_delete_error(
                                remove_file_tasks
                                    .get(&join_err.id())
                                    .expect("panicked/cancelled task id not in task id pool"),
                                std::io::Error::other(join_err),
                            );
                        }
                    }
                }
            }

            // Collect lines by polling files.
            let mut global_bytes_read: usize = 0;
            let mut maxed_out_reading_single_file = false;
            for (&file_id, watcher) in &mut fp_map {
                if !watcher.should_read() {
                    continue;
                }

                // Skip reading gzip files already fully read, but still fall through
                // to the removal logic below so TTL cleanup can proceed.
                let is_done = !self.ignore_checkpoints && checkpoints.get_done(file_id);

                let start = time::Instant::now();
                let mut bytes_read: usize = 0;
                let mut lines_read: usize = 0;
                while !is_done
                    && let Ok(RawLineResult {
                        raw_line: Some(line),
                        discarded_for_size_and_truncated,
                    }) = watcher.read_line().await
                {
                    discarded_for_size_and_truncated.iter().for_each(|buf| {
                        self.emitter.emit_file_line_too_long(
                            &buf.clone(),
                            self.max_line_bytes,
                            buf.len(),
                        )
                    });

                    let sz = line.bytes.len();
                    trace!(
                        message = "Read bytes.",
                        path = ?watcher.path,
                        bytes = ?sz
                    );
                    stats.record_bytes(sz);

                    bytes_read += sz;
                    lines_read += 1;

                    lines.push(Line {
                        text: line.bytes,
                        filename: watcher.path.to_str().expect("not a valid path").to_owned(),
                        file_id,
                        start_offset: line.offset,
                        end_offset: watcher.get_file_position(),
                    });

                    if bytes_read > self.max_read_bytes {
                        maxed_out_reading_single_file = true;
                        break;
                    }
                }
                stats.record("reading", start.elapsed());
                if lines_read > 0 {
                    emit(DeliveryReadEvent {
                        path: watcher.path.to_str().expect("not a valid path").to_owned(),
                        bytes_read,
                        lines_read,
                        source_context: self.source_context.clone(),
                        emitted_after_multiline_agg: false,
                    });
                }
                if watcher.reached_eof() && self.is_archive(&watcher.path) {
                    //TODO: a vector event for done. important for debugging
                    info!(message = "File reached eof. Marking it done.", path = ?watcher.path);
                    checkpoints.set_done(file_id, &watcher.path);
                }

                if bytes_read > 0 {
                    global_bytes_read = global_bytes_read.saturating_add(bytes_read);
                } else {
                    // Should the file be removed
                    if let Some(grace_period) = self.remove_after
                        && watcher.last_read_success().elapsed() >= grace_period
                    {
                        // Only remove the file if it meets the TTL removal config
                        // If there is no TTL removal config, we always remove
                        if self.should_ttl_delete(&watcher.path) {
                            // Try to remove
                            match remove_file(&watcher.path).await {
                                Ok(()) => {
                                    self.emitter.emit_file_deleted(&watcher.path);
                                    watcher.set_dead();
                                }
                                Err(error) => {
                                    if error.kind() == std::io::ErrorKind::NotFound {
                                        // File already gone (e.g. kubelet cleaned up the pod volume).
                                        // Treat as successful deletion.
                                        info!(message = "File not found during deletion, assuming it was already removed externally.", path = ?watcher.path);
                                        self.emitter.emit_file_deleted(&watcher.path);
                                        watcher.set_dead();
                                    } else {
                                        // We will try again after some time.
                                        self.emitter.emit_file_delete_error(&watcher.path, error);
                                    }
                                }
                            }
                        }
                    }
                }

                // Do not move on to newer files if we are behind on an older file
                if self.oldest_first && maxed_out_reading_single_file {
                    break;
                }
            }

            for (_, watcher) in &mut fp_map {
                if !watcher.file_findable() && watcher.last_seen().elapsed() > self.rotate_wait {
                    watcher.set_dead();
                }
            }

            // A FileWatcher is dead when the underlying file has disappeared.
            // If the FileWatcher is dead we don't retain it; it will be deallocated.
            fp_map.retain(|file_id, watcher| {
                if watcher.dead() {
                    self.emitter
                        .emit_file_unwatched(&watcher.path, watcher.reached_eof());
                    checkpoints.set_dead(*file_id);
                    false
                } else {
                    true
                }
            });
            self.emitter.emit_files_open(fp_map.len());

            let start = time::Instant::now();
            let to_send = std::mem::take(&mut lines);

            let result = chans.send(to_send).await;
            match result {
                Ok(()) => {}
                Err(error) => {
                    error!(message = "Output channel closed.", %error);
                    return Err(error);
                }
            }
            stats.record("sending", start.elapsed());

            let start = time::Instant::now();
            // When no lines have been read we kick the backup_cap up by twice,
            // limited by the hard-coded cap. Else, we set the backup_cap to its
            // minimum on the assumption that next time through there will be
            // more lines to read promptly.
            backoff_cap = if global_bytes_read == 0 {
                cmp::min(2_048, backoff_cap.saturating_mul(2))
            } else {
                1
            };
            let backoff = backoff_cap.saturating_sub(global_bytes_read);

            // This works only if run inside tokio context since we are using
            // tokio's Timer. Outside of such context, this will panic on the first
            // call. Also since we are using block_on here and in the above code,
            // this should be run in its own thread. `spawn_blocking` fulfills
            // all of these requirements.
            let sleep = async move {
                if backoff > 0 {
                    sleep(Duration::from_millis(backoff as u64)).await;
                }
            };
            futures::pin_mut!(sleep);
            match select(shutdown_data, sleep).await {
                Either::Left((_, _)) => {
                    if self.drain_on_shutdown {
                        let drain_checkpointer = drain_checkpointer.as_ref().expect(
                            "drain_checkpointer must be set when drain_on_shutdown is true",
                        );
                        info!(message = "Shutdown signal received, draining files before exit.");

                        // Snapshot current EOF of each watched file as the drain target.
                        let mut drain_targets: IndexMap<FileFingerprint, u64> = IndexMap::new();
                        for (&file_id, watcher) in &fp_map {
                            match fs::metadata(&watcher.path).await {
                                Ok(meta) => {
                                    drain_targets.insert(file_id, meta.len());
                                }
                                Err(error) => {
                                    warn!(
                                        message = "Could not stat file for drain target, skipping.",
                                        path = ?watcher.path,
                                        ?error,
                                    );
                                }
                            }
                        }

                        // Round-robin drain loop.
                        while !drain_targets.is_empty() {
                            let mut progress = false;

                            for file_id in drain_targets.keys().copied().collect::<Vec<_>>() {
                                let Some(&target) = drain_targets.get(&file_id) else {
                                    continue;
                                };
                                let Some(watcher) = fp_map.get_mut(&file_id) else {
                                    drain_targets.swap_remove(&file_id);
                                    progress = true;
                                    continue;
                                };

                                let mut bytes_read: usize = 0;
                                while watcher.get_file_position() < target {
                                    match watcher.read_line().await {
                                        Ok(RawLineResult {
                                            raw_line: Some(line),
                                            discarded_for_size_and_truncated,
                                        }) => {
                                            for buf in &discarded_for_size_and_truncated {
                                                self.emitter.emit_file_line_too_long(
                                                    &buf.clone(),
                                                    self.max_line_bytes,
                                                    buf.len(),
                                                );
                                            }
                                            bytes_read += line.bytes.len();
                                            lines.push(Line {
                                                text: line.bytes,
                                                filename: watcher
                                                    .path
                                                    .to_str()
                                                    .expect("not a valid path")
                                                    .to_owned(),
                                                file_id,
                                                start_offset: line.offset,
                                                end_offset: watcher.get_file_position(),
                                            });
                                            if bytes_read > self.max_read_bytes {
                                                break;
                                            }
                                        }
                                        Ok(_) => break,
                                        Err(_) => {
                                            drain_targets.swap_remove(&file_id);
                                            progress = true;
                                            break;
                                        }
                                    }
                                }
                                progress |= bytes_read > 0;

                                // Persist checkpoint once a file reaches its drain target.
                                if drain_targets.contains_key(&file_id)
                                    && watcher.get_file_position() >= target
                                {
                                    drain_targets.swap_remove(&file_id);
                                    checkpoints.update(file_id, watcher.get_file_position());
                                    if let Err(error) = drain_checkpointer.write_checkpoints().await
                                    {
                                        error!(?error, "Error writing checkpoints during drain");
                                    }
                                    progress = true;
                                }
                            }

                            let to_send = std::mem::take(&mut lines);
                            if !to_send.is_empty() {
                                if let Err(error) = chans.send(to_send).await {
                                    error!(message = "Output channel closed during drain.", %error);
                                    break;
                                }
                            }

                            if !progress {
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                        }

                        info!(message = "Drain complete, shutting down file server.");
                    }

                    // Close the output channel FIRST. This allows downstream
                    // processing to complete and acknowledgements to flow back,
                    // which in turn lets shutdown_checkpointer resolve so the
                    // checkpoint writer task can finish. Awaiting the checkpoint
                    // task before closing the channel would deadlock when
                    // end-to-end acknowledgements are enabled.
                    chans
                        .close()
                        .await
                        .expect("error closing file_server data channel.");
                    // Await the checkpoint writer task. On clean shutdown it
                    // returns the `Arc<Checkpointer>` so we can do one final
                    // pre-shutdown write. If the task was cancelled by the
                    // runtime during topology teardown, or panicked, log and
                    // skip the final write rather than panicking ourselves --
                    // panicking here cascades through `handle_errors` and
                    // forces unnecessary container restarts.
                    let checkpointer = match checkpoint_task_handle.await {
                        Ok(checkpointer) => checkpointer,
                        Err(e) if e.is_cancelled() => {
                            warn!(
                                "checkpoint writer task cancelled during shutdown; \
                                 skipping final checkpoint write"
                            );
                            return Ok(Shutdown);
                        }
                        Err(e) => {
                            error!(
                                error = ?e,
                                "checkpoint writer task panicked; \
                                 skipping final checkpoint write"
                            );
                            return Ok(Shutdown);
                        }
                    };
                    if let Err(error) = checkpointer.write_checkpoints().await {
                        error!(?error, "Error writing checkpoints before shutdown");
                    }
                    return Ok(Shutdown);
                }
                Either::Right((_, future)) => shutdown_data = future,
            }
            stats.record("sleeping", start.elapsed());
        }
    }

    async fn watch_new_file(
        &self,
        path: PathBuf,
        file_id: FileFingerprint,
        fp_map: &mut IndexMap<FileFingerprint, FileWatcher>,
        checkpoints: &CheckpointsView,
        startup: bool,
    ) {
        // Determine the initial _requested_ starting point in the file. This can be overridden
        // once the file is actually opened and we determine it is compressed, older than we're
        // configured to read, etc.
        let fallback = if startup {
            self.read_from
        } else {
            // Always read new files that show up while we're running from the beginning. There's
            // not a good way to determine if they were moved or just created and written very
            // quickly, so just make sure we're not missing any data.
            ReadFrom::Beginning
        };

        // Always prefer the stored checkpoint unless the user has opted out.  Previously, the
        // checkpoint was only loaded for new files when Vector was started up, but the
        // `kubernetes_logs` source returns the files well after start-up, once it has populated
        // them from the k8s metadata, so we now just always use the checkpoints unless opted out.
        // https://github.com/vectordotdev/vector/issues/7139
        let read_from = if !self.ignore_checkpoints {
            checkpoints
                .get(file_id)
                .map(ReadFrom::Checkpoint)
                .unwrap_or(fallback)
        } else {
            fallback
        };

        // Skip opening gzip files that have already been fully read.
        if !self.ignore_checkpoints && checkpoints.get_done(file_id) && self.is_archive(&path) {
            debug!(message = "Skipping already-done archive file.", ?path,);
            return;
        }

        match FileWatcher::new(
            path.clone(),
            read_from,
            self.ignore_before,
            self.start_reading_at,
            self.max_line_bytes,
            self.line_delimiter.clone(),
        )
        .await
        {
            Ok(mut watcher) => {
                if !watcher.is_active {
                    // If the file is not active, we do not watch it but we should make sure that the file is still eligible for rediscovery if it gets modified again after the start_reading_at timestamp.
                    return;
                }
                if let ReadFrom::Checkpoint(file_position) = read_from {
                    self.emitter.emit_file_resumed(&path, file_position);
                } else {
                    self.emitter.emit_file_added(&path);
                }
                watcher.set_file_findable(true);
                fp_map.insert(file_id, watcher);
            }
            Err(error) => self.emitter.emit_file_watch_error(&path, error),
        };
    }

    // Determines per the TTL removal config whether the file should be deleted
    fn should_ttl_delete(&self, path: &Path) -> bool {
        match &self.ttl_removal_config {
            None => true, // No TTL config means we don't have any other TTL rules, so deletion is ok
            Some(ttl_removal_config) => {
                let path_str = path.to_string_lossy();
                let matches_pattern = ttl_removal_config
                    .patterns
                    .iter()
                    .any(|pattern| pattern.matches(&path_str));
                match ttl_removal_config.action {
                    // If Remove, we remove the file if it matches the pattern
                    // So if matches_patterns => it should participate in TTL removal
                    FileTTLAction::Remove => matches_pattern,
                    // If Keep, we keep the file if it matches the pattern
                    // So it only participates in TTL removal if it doesn't match the pattern
                    FileTTLAction::Keep => !matches_pattern,
                }
            }
        }
    }
}

async fn checkpoint_writer(
    checkpointer: Arc<Checkpointer>,
    sleep_duration: Duration,
    mut shutdown: impl Future + Unpin,
    emitter: impl FileSourceInternalEvents,
) -> Arc<Checkpointer> {
    loop {
        let sleep = sleep(sleep_duration);
        tokio::select! {
            _ = &mut shutdown => break,
            _ = sleep => {},
        }

        let emitter = emitter.clone();
        let checkpointer = Arc::clone(&checkpointer);
        let start = time::Instant::now();
        match checkpointer.write_checkpoints().await {
            Ok(count) => emitter.emit_file_checkpointed(count, start.elapsed()),
            Err(error) => emitter.emit_file_checkpoint_write_error(error),
        };
    }
    checkpointer
}

pub fn calculate_ignore_before(ignore_older_secs: Option<u64>) -> Option<DateTime<Utc>> {
    ignore_older_secs.map(|secs| Utc::now() - chrono::Duration::seconds(secs as i64))
}

/// Parse an ISO 8601 timestamp string into a DateTime<Utc>.
///
/// # Arguments
///
/// * `start_reading_at` - A string representing the timestamp to start reading at in ISO 8601 format.
///   Supports timezone-aware format (e.g., "2022-01-01T12:00:00Z")
///
/// # Returns
///
/// A `DateTime<Utc>` if the timestamp is valid, otherwise `None`.
pub fn parse_start_reading_at(start_reading_at: Option<String>) -> Option<DateTime<Utc>> {
    match start_reading_at {
        Some(s) => {
            // Try ISO 8601 format with timezone (RFC 3339)
            if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
                return Some(dt.with_timezone(&Utc));
            }

            // No valid ISO 8601 format found
            warn!(
                message = "Invalid timestamp provided for start_reading_at, expected ISO 8601 format, disabling timestamp filtering",
                error = "Failed to parse as ISO 8601",
                provided = %s,
                examples = "2022-01-01T12:00:00Z"
            );
            None
        }
        None => None,
    }
}

/// A sentinel type to signal that file server was gracefully shut down.
///
/// The purpose of this type is to clarify the semantics of the result values
/// returned from the [`FileServer::run`] for both the users of the file server,
/// and the implementors.
#[derive(Debug)]
pub struct Shutdown;

struct TimingStats {
    started_at: time::Instant,
    segments: BTreeMap<&'static str, Duration>,
    events: usize,
    bytes: usize,
}

impl TimingStats {
    fn record(&mut self, key: &'static str, duration: Duration) {
        let segment = self.segments.entry(key).or_default();
        *segment += duration;
    }

    fn record_bytes(&mut self, bytes: usize) {
        self.events += 1;
        self.bytes += bytes;
    }

    fn report(&self) {
        if !tracing::level_enabled!(tracing::Level::DEBUG) {
            return;
        }
        let total = self.started_at.elapsed();
        let counted: Duration = self.segments.values().sum();
        let other: Duration = total.saturating_sub(counted);
        let mut ratios = self
            .segments
            .iter()
            .map(|(k, v)| (*k, v.as_secs_f32() / total.as_secs_f32()))
            .collect::<BTreeMap<_, _>>();
        ratios.insert("other", other.as_secs_f32() / total.as_secs_f32());
        let (event_throughput, bytes_throughput) = if total.as_secs() > 0 {
            (
                self.events as u64 / total.as_secs(),
                self.bytes as u64 / total.as_secs(),
            )
        } else {
            (0, 0)
        };
        debug!(event_throughput = %scale(event_throughput), bytes_throughput = %scale(bytes_throughput), ?ratios);
    }
}

fn scale(bytes: u64) -> String {
    let units = ["", "k", "m", "g"];
    let mut bytes = bytes as f32;
    let mut i = 0;
    while bytes > 1000.0 && i <= 3 {
        bytes /= 1000.0;
        i += 1;
    }
    format!("{:.3}{}/sec", bytes, units[i])
}

impl Default for TimingStats {
    fn default() -> Self {
        Self {
            started_at: time::Instant::now(),
            segments: Default::default(),
            events: Default::default(),
            bytes: Default::default(),
        }
    }
}

#[derive(Debug)]
pub struct Line {
    pub text: Bytes,
    pub filename: String,
    pub file_id: FileFingerprint,
    pub start_offset: u64,
    pub end_offset: u64,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        io::Error,
        path::{Path, PathBuf},
        time::Duration,
    };

    use bytes::{Bytes, BytesMut};
    use file_source_common::{
        Checkpointer, FileSourceInternalEvents, FingerprintStrategy, Fingerprinter, ReadFrom,
    };
    use futures::{StreamExt, channel::mpsc};
    use tempfile::tempdir;
    use tokio::fs;

    use crate::{
        file_server::{FileServer, Line},
        paths_provider::PathsProvider,
    };

    // No-op FileSourceInternalEvents implementation for testing.
    // Error events panic so tests fail fast on unexpected errors.
    #[derive(Clone)]
    struct NoErrors;

    impl FileSourceInternalEvents for NoErrors {
        fn emit_file_added(&self, _: &Path) {}
        fn emit_file_resumed(&self, _: &Path, _: u64) {}
        fn emit_file_watch_error(&self, _: &Path, _: Error) {
            panic!("unexpected file watch error");
        }
        fn emit_file_unwatched(&self, _: &Path, _: bool) {}
        fn emit_file_deleted(&self, _: &Path) {}
        fn emit_file_delete_error(&self, _: &Path, _: Error) {
            panic!("unexpected file delete error");
        }
        fn emit_file_fingerprint_read_error(&self, _: &Path, _: Error) {
            panic!("unexpected fingerprint read error");
        }
        fn emit_file_checkpointed(&self, _: usize, _: Duration) {}
        fn emit_file_checksum_failed(&self, _: &Path) {
            panic!("unexpected file checksum failure");
        }
        fn emit_file_checkpoint_write_error(&self, _: Error) {
            panic!("unexpected checkpoint write error");
        }
        fn emit_files_open(&self, _: usize) {}
        fn emit_path_globbing_failed(&self, _: &Path, _: &Error) {
            panic!("unexpected path globbing failure");
        }
        fn emit_file_line_too_long(&self, _: &BytesMut, _: usize, _: usize) {
            panic!("unexpected line too long");
        }
    }

    // Simple PathsProvider that returns a fixed set of paths.
    struct TestPathsProvider {
        paths: Vec<PathBuf>,
    }

    impl PathsProvider for TestPathsProvider {
        type IntoIter = Vec<(Option<crate::paths_provider::LogFileInfo>, PathBuf)>;

        fn paths(&self) -> Self::IntoIter {
            self.paths.iter().map(|p| (None, p.clone())).collect()
        }
    }

    fn make_file_server(
        paths: Vec<PathBuf>,
        data_dir: PathBuf,
        drain_on_shutdown: bool,
    ) -> FileServer<TestPathsProvider, NoErrors> {
        FileServer {
            paths_provider: TestPathsProvider { paths },
            max_read_bytes: 2048,
            ignore_checkpoints: false,
            read_from: ReadFrom::Beginning,
            ignore_before: None,
            start_reading_at: None,
            max_line_bytes: 1024,
            line_delimiter: Bytes::from("\n"),
            data_dir,
            glob_minimum_cooldown: Duration::from_millis(100),
            fingerprinter: Fingerprinter::new(
                FingerprintStrategy::FirstLinesChecksum {
                    ignored_header_bytes: 0,
                    lines: 1,
                },
                1024,
                true,
            ),
            oldest_first: false,
            remove_after: None,
            emitter: NoErrors,
            rotate_wait: Duration::from_secs(u64::MAX / 2),
            ttl_removal_config: None,
            source_context: None,
            file_to_pod_map: None,
            drain_on_shutdown,
            archive_extensions: vec!["gz".to_string()],
        }
    }

    #[tokio::test]
    async fn drain_on_shutdown_false_preserves_existing_behavior() {
        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).await.unwrap();

        let log_path = tmp.path().join("test.log");
        // Write 200 lines, each ~50 bytes, total ~10KB which exceeds max_read_bytes of 2048.
        let content: String = (0..200)
            .map(|i| format!("line {:04} -- padding to make this longer\n", i))
            .collect();
        fs::write(&log_path, &content).await.unwrap();

        let file_server = make_file_server(vec![log_path], data_dir, false);
        let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);

        // Both shutdown signals resolve immediately.
        let shutdown_data = futures::future::ready(());
        let shutdown_checkpointer = futures::future::ready(());
        let checkpointer = Checkpointer::new(tmp.path().join("data").as_path());

        let result = file_server
            .run(tx, shutdown_data, shutdown_checkpointer, checkpointer)
            .await;
        assert!(result.is_ok());

        // Collect lines non-blocking since the channel is already closed.
        let mut received = Vec::new();
        while let Ok(batch) = rx.try_recv() {
            received.extend(batch);
        }

        // With immediate shutdown signal and drain_on_shutdown=false, only a partial
        // read should occur (first iteration reads up to max_read_bytes).
        assert!(
            received.len() < 200,
            "expected partial read, got {} lines",
            received.len()
        );
    }

    #[tokio::test]
    async fn drain_on_shutdown_reads_all_data() {
        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).await.unwrap();

        let log_path = tmp.path().join("test.log");
        let content: String = (0..200)
            .map(|i| format!("line {:04} -- padding to make this longer\n", i))
            .collect();
        fs::write(&log_path, &content).await.unwrap();

        let file_server = make_file_server(vec![log_path], data_dir, true);
        let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);

        let shutdown_data = futures::future::ready(());
        let shutdown_checkpointer = futures::future::ready(());
        let checkpointer = Checkpointer::new(tmp.path().join("data").as_path());

        // Spawn a collector to drain the bounded channel concurrently.
        let collector = tokio::spawn(async move {
            let mut lines = Vec::new();
            while let Some(batch) = rx.next().await {
                lines.extend(batch);
            }
            lines
        });

        let result = file_server
            .run(tx, shutdown_data, shutdown_checkpointer, checkpointer)
            .await;
        assert!(result.is_ok());

        let received = collector.await.unwrap();
        assert_eq!(
            received.len(),
            200,
            "expected all 200 lines, got {}",
            received.len()
        );

        // Verify first and last line content.
        assert_eq!(
            received[0].text,
            Bytes::from("line 0000 -- padding to make this longer")
        );
        assert_eq!(
            received[199].text,
            Bytes::from("line 0199 -- padding to make this longer")
        );
    }

    #[tokio::test]
    async fn drain_on_shutdown_multiple_files() {
        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).await.unwrap();

        let log_a = tmp.path().join("a.log");
        let log_b = tmp.path().join("b.log");

        let content_a: String = (0..100)
            .map(|i| format!("file_a line {:04} padding here\n", i))
            .collect();
        let content_b: String = (0..100)
            .map(|i| format!("file_b line {:04} padding here\n", i))
            .collect();

        fs::write(&log_a, &content_a).await.unwrap();
        fs::write(&log_b, &content_b).await.unwrap();

        let file_server = make_file_server(vec![log_a, log_b], data_dir, true);
        let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);

        let shutdown_data = futures::future::ready(());
        let shutdown_checkpointer = futures::future::ready(());
        let checkpointer = Checkpointer::new(tmp.path().join("data").as_path());

        let collector = tokio::spawn(async move {
            let mut lines = Vec::new();
            while let Some(batch) = rx.next().await {
                lines.extend(batch);
            }
            lines
        });

        let result = file_server
            .run(tx, shutdown_data, shutdown_checkpointer, checkpointer)
            .await;
        assert!(result.is_ok());

        let received = collector.await.unwrap();
        let from_a = received
            .iter()
            .filter(|l| l.filename.contains("a.log"))
            .count();
        let from_b = received
            .iter()
            .filter(|l| l.filename.contains("b.log"))
            .count();

        assert_eq!(from_a, 100, "expected 100 lines from a.log, got {}", from_a);
        assert_eq!(from_b, 100, "expected 100 lines from b.log, got {}", from_b);
        assert_eq!(received.len(), 200);
    }

    #[tokio::test]
    async fn drain_on_shutdown_writes_checkpoints() {
        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).await.unwrap();

        let log_path = tmp.path().join("test.log");
        let content = "line one\nline two\nline three\n";
        fs::write(&log_path, content).await.unwrap();

        let file_server = make_file_server(vec![log_path.clone()], data_dir.clone(), true);
        let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);

        let shutdown_data = futures::future::ready(());
        let shutdown_checkpointer = futures::future::ready(());
        let checkpointer = Checkpointer::new(data_dir.as_path());

        let collector = tokio::spawn(async move {
            let mut lines = Vec::new();
            while let Some(batch) = rx.next().await {
                lines.extend(batch);
            }
            lines
        });

        let result = file_server
            .run(tx, shutdown_data, shutdown_checkpointer, checkpointer)
            .await;
        assert!(result.is_ok());

        let received = collector.await.unwrap();
        assert_eq!(received.len(), 3);

        // Verify checkpoints were persisted by loading them in a fresh Checkpointer.
        let mut checkpointer = Checkpointer::new(data_dir.as_path());
        checkpointer.read_checkpoints(None).await;

        // Compute the file's fingerprint.
        let mut fingerprinter = Fingerprinter::new(
            FingerprintStrategy::FirstLinesChecksum {
                ignored_header_bytes: 0,
                lines: 1,
            },
            1024,
            true,
        );
        let mut known_small_files = HashMap::new();
        let fingerprint = fingerprinter
            .fingerprint_or_emit(&log_path, &mut known_small_files, &NoErrors)
            .await
            .expect("should be able to fingerprint the file");

        let position = checkpointer.view().get(fingerprint);
        let file_size = fs::metadata(&log_path).await.unwrap().len();
        assert_eq!(
            position,
            Some(file_size),
            "checkpoint position should equal file size after drain"
        );
    }

    /// Integration test for the drain-on-shutdown contract.
    ///
    /// Two independent FileServer runs execute the same steps 1–3 but
    /// diverge at step 4 based on the `drain_on_shutdown` flag.
    ///
    /// Run A (drain_on_shutdown = false):
    ///   1. Write X lines to a file whose total size exceeds `max_read_bytes`.
    ///   2. Start FileServer with an *immediate* shutdown signal (`ready(())`).
    ///      The main loop runs one iteration — reads up to `max_read_bytes`
    ///      (Y lines, where Y < X), sends them to the output channel.
    ///   3. Shutdown signal fires. The main loop's `select(shutdown, sleep)`
    ///      resolves to `Either::Left` and enters the shutdown path.
    ///   4. Only Y lines are delivered. The remaining X − Y lines on disk
    ///      are lost.
    ///
    /// Run B (drain_on_shutdown = true):
    ///   1–3. Same as Run A (fresh file, fresh FileServer, same immediate
    ///      shutdown signal).
    ///   4. The drain loop reads the remaining X − Y lines before closing
    ///      the channel. All X lines are delivered and the checkpoint is
    ///      set to EOF.
    #[tokio::test]
    async fn drain_on_shutdown_integration() {
        // Build a throwaway server just to read max_read_bytes, so the
        // test stays correct even if make_file_server's value changes.
        let max_read_bytes = {
            let tmp = tempdir().unwrap();
            make_file_server(vec![], tmp.path().to_path_buf(), false).max_read_bytes
        };

        // Each line is ~40 bytes.  We need total file size to comfortably
        // exceed max_read_bytes so that one main-loop iteration only reads
        // a fraction of the file (Y lines, where Y < X).
        const LINE_LEN: usize = 40;
        let x = max_read_bytes / LINE_LEN * 4; // ~4× what fits in one iteration
        let file_content: String = (0..x)
            .map(|i| format!("line {:04} -- padding to make this longer\n", i))
            .collect();

        // Steps 1–3 for each run: write X lines to a fresh file in a
        // fresh tempdir, start a new FileServer with an immediate shutdown
        // signal, let it run one read iteration, then enter the shutdown
        // path.  Each call gets its own isolated state (tempdir, file,
        // FileServer, channel, checkpointer) — the two runs share nothing.
        async fn run_file_server(content: &str, drain: bool) -> Vec<Line> {
            let tmp = tempdir().unwrap();
            let data_dir = tmp.path().join("data");
            fs::create_dir_all(&data_dir).await.unwrap();

            let log_path = tmp.path().join("test.log");
            fs::write(&log_path, content).await.unwrap();

            let file_server = make_file_server(vec![log_path], data_dir.clone(), drain);
            let (tx, rx) = mpsc::channel::<Vec<Line>>(2);

            // Immediate shutdown: the main loop gets exactly one read
            // iteration before the shutdown signal wins the select.
            let shutdown_data = futures::future::ready(());
            let shutdown_checkpointer = futures::future::ready(());
            let checkpointer = Checkpointer::new(data_dir.as_path());

            let collector = tokio::spawn(async move {
                let mut lines = Vec::new();
                let mut rx = rx;
                while let Some(batch) = rx.next().await {
                    lines.extend(batch);
                }
                lines
            });

            file_server
                .run(tx, shutdown_data, shutdown_checkpointer, checkpointer)
                .await
                .expect("FileServer::run should succeed");

            collector.await.expect("collector task panicked")
        }

        // ── Run A (drain_on_shutdown = false) ───────────────────────────
        // Step 4: no drain — only Y lines from the first read iteration
        // are delivered; the remaining X − Y lines on disk are lost.
        let received_a = run_file_server(&file_content, false).await;
        let y = received_a.len();
        assert!(
            y > 0 && y < x,
            "Run A: expected a partial read (0 < Y < {x}), got Y = {y}",
        );

        // ── Run B (drain_on_shutdown = true) ────────────────────────────
        // Step 4: the drain loop reads the remaining X − Y lines before
        // closing the channel.  All X lines are delivered.
        let received_b = run_file_server(&file_content, true).await;
        assert_eq!(
            received_b.len(),
            x,
            "Run B: expected all {x} lines with drain, got {}",
            received_b.len(),
        );
        assert_eq!(
            received_b[0].text,
            Bytes::from("line 0000 -- padding to make this longer"),
        );
        assert_eq!(
            received_b[x - 1].text,
            Bytes::from(format!("line {:04} -- padding to make this longer", x - 1)),
        );
    }
}
