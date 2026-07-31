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
use vector_common::internal_event::delivery_singleton;

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
    /// Source type passed through to the delivery singleton's `accumulate_read`.
    /// Used by the metric in `delivery_event.rs` to pick the correct topic
    /// fallback when a filename doesn't match the Lumberjack convention
    /// (e.g. `kubernetes_logs` falls back to `sawmill-service-log`).
    pub source_type: &'static str,
    /// Minimum backoff cap for the read loop (reset value after a non-empty
    /// read). Defaults to `1`.
    pub read_loop_min_backoff: usize,
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
    fn update_file_to_pod_map(&self, path: PathBuf, log_file_info_opt: Option<LogFileInfo>) {
        if let (Some(file_to_pod_map), Some(log_file_info)) =
            (&self.file_to_pod_map, log_file_info_opt)
        {
            file_to_pod_map.lock().unwrap().insert(path, log_file_info);
        }
    }

    // Keep the file-to-pod map aligned with watcher paths that may still be emitted on lines.
    fn update_file_to_pod_map_for_path_change(
        &self,
        old_path: &Path,
        new_path: PathBuf,
        log_file_info_opt: Option<LogFileInfo>,
    ) {
        if let Some(file_to_pod_map) = &self.file_to_pod_map {
            let mut file_to_pod_map = file_to_pod_map.lock().unwrap();
            let log_file_info =
                log_file_info_opt.or_else(|| file_to_pod_map.get(old_path).cloned());

            if let Some(log_file_info) = log_file_info {
                file_to_pod_map.insert(new_path.clone(), log_file_info);
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

        // Obtain the view early so the startup fingerprint loop can use the
        // archive_paths cache loaded from disk by read_checkpoints above.
        let checkpoints = checkpointer.view();

        let mut known_small_files: HashMap<PathBuf, time::Instant> = HashMap::new();

        let mut existing_files = Vec::new();
        for (log_file_info_opt, path) in self.paths_provider.paths().into_iter() {
            // For archive files whose path→fingerprint mapping was restored from
            // the checkpoint, skip the expensive gzip decompression + CRC64 and
            // use the cached fingerprint directly.
            let file_id = if !self.ignore_checkpoints && self.is_archive(&path) {
                match checkpoints.get_archive_fingerprint(&path) {
                    Some(id) => Some(id),
                    None => {
                        let id = self
                            .fingerprinter
                            .fingerprint_or_emit(&path, &mut known_small_files, &self.emitter)
                            .await;
                        if let Some(id) = id {
                            checkpoints.set_archive_path(id, &path);
                        }
                        id
                    }
                }
            } else {
                self.fingerprinter
                    .fingerprint_or_emit(&path, &mut known_small_files, &self.emitter)
                    .await
            };
            if let Some(file_id) = file_id {
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
                    // Resolve `file_id` for this path. For archives we treat the
                    // checkpoint cache purely as a fingerprint-computation
                    // optimization — once we have a `file_id`, all reconciliation
                    // (path comparison, rename detection, duplicate-fingerprint
                    // disambiguation, untracked-file handling) flows through the
                    // single shared block below. This avoids the class of bugs
                    // where a short-circuiting fast path skipped state
                    // reconciliation and left watchers bound to stale inodes.
                    let file_id = if !self.ignore_checkpoints && self.is_archive(&path) {
                        match checkpoints.get_archive_fingerprint(&path) {
                            Some(id) => Some(id),
                            None => {
                                let id = self
                                    .fingerprinter
                                    .fingerprint_or_emit(
                                        &path,
                                        &mut known_small_files,
                                        &self.emitter,
                                    )
                                    .await;
                                if let Some(id) = id {
                                    checkpoints.set_archive_path(id, &path);
                                }
                                id
                            }
                        }
                    } else {
                        self.fingerprinter
                            .fingerprint_or_emit(&path, &mut known_small_files, &self.emitter)
                            .await
                    };

                    if let Some(file_id) = file_id {
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
                                let old_path = watcher.path.clone();
                                if watcher.update_path(path.clone()).await.is_ok() {
                                    self.update_file_to_pod_map_for_path_change(
                                        &old_path,
                                        path,
                                        log_file_info_opt,
                                    );
                                } // ok if this fails: might fix next cycle
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
                                    let old_path = watcher.path.clone();
                                    if watcher.update_path(path.clone()).await.is_ok() {
                                        self.update_file_to_pod_map_for_path_change(
                                            &old_path,
                                            path,
                                            log_file_info_opt,
                                        );
                                    } // ok if this fails: might fix next cycle
                                }
                            }
                        } else {
                            let is_done_archive = !self.ignore_checkpoints
                                && self.is_archive(&path)
                                && checkpoints.get_done(file_id);
                            if !is_done_archive {
                                self.update_file_to_pod_map(path.clone(), log_file_info_opt);
                            }
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
            // Archives that reached EOF this cycle — removed from fp_map after the loop.
            let mut done_archives: Vec<(FileFingerprint, PathBuf)> = Vec::new();
            for (&file_id, watcher) in &mut fp_map {
                // Presence in fp_map means the file exists on disk; keep its
                // checkpoint live (clear death mark, refresh `modified`) even if
                // we don't read it this cycle.
                checkpoints.clear_dead(file_id);

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
                    // Pre-multiline read spot, gated off by default
                    // (EMIT_READ_EVENT_AFTER_MULTILINE_AGG). The counter fires
                    // inline; only the VEL `info!` log is batched by the
                    // process-global delivery singleton. `source_context` is
                    // borrowed and only cloned on first sight of this path.
                    delivery_singleton().accumulate_read(
                        watcher.path.to_str().expect("not a valid path").to_owned(),
                        bytes_read,
                        lines_read,
                        &self.source_context,
                        self.source_type,
                        vector_common::internal_event::vector_event::delivery_event::current_hour_time_parity_ms_value(),
                        false,
                    );
                }
                if watcher.reached_eof() && self.is_archive(&watcher.path) {
                    //TODO: a vector event for done. important for debugging
                    debug!(message = "File reached eof. Marking it done.", path = ?watcher.path);
                    checkpoints.set_done(file_id, &watcher.path);
                    done_archives.push((file_id, watcher.path.clone()));
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

            // Drop done-archive watchers: emit unwatched metric and remove directly
            // from fp_map without calling checkpoints.set_dead so that the done/
            // archive_paths entries survive until remove_after actually deletes the file.
            for (fng, path) in done_archives {
                self.emitter.emit_file_unwatched(&path, true);
                fp_map.swap_remove(&fng);
            }

            for (_, watcher) in &mut fp_map {
                // Reap a vanished archive immediately (terminal, no successor);
                // give a non-archive the `rotate_wait` grace so a live watcher
                // can hand its position to the rotated `.gz` via `update_path`.
                let expired = if self.is_archive(&watcher.path) {
                    !watcher.file_findable()
                } else {
                    !watcher.file_findable() && watcher.last_seen().elapsed() > self.rotate_wait
                };
                if expired {
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
                self.read_loop_min_backoff
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
                        //
                        // For archive watchers (e.g. .gz) we cannot use the on-disk
                        // file length as the target: `watcher.get_file_position()`
                        // tracks the *decompressed* byte position, while
                        // `fs::metadata(path).len()` is the *compressed* size on
                        // disk. Comparing them is wrong in both directions: with
                        // typical compression the decompressed content is larger
                        // than `target`, so the outer `file_position >= target`
                        // check fires early and silently drops the remainder; with
                        // small files where overhead exceeds savings the check
                        // never fires and the drain loop spins. Use `u64::MAX` as
                        // a sentinel and rely on `read_line` returning `Ok(None)`
                        // (decompressed-stream EOF) as the completion signal.
                        let mut drain_targets: IndexMap<FileFingerprint, u64> = IndexMap::new();
                        for (&file_id, watcher) in &fp_map {
                            if self.is_archive(&watcher.path) {
                                drain_targets.insert(file_id, u64::MAX);
                            } else {
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
                                        Ok(_) => {
                                            // No line available right now. For
                                            // archive watchers this means the
                                            // decompressed stream is exhausted —
                                            // a definitive completion signal,
                                            // because the size-based outer guard
                                            // can't fire for archives (target is
                                            // `u64::MAX`). Persist the checkpoint
                                            // and drop the drain target.
                                            if self.is_archive(&watcher.path) {
                                                drain_targets.swap_remove(&file_id);
                                                checkpoints
                                                    .update(file_id, watcher.get_file_position());
                                                if let Err(error) =
                                                    drain_checkpointer.write_checkpoints().await
                                                {
                                                    error!(
                                                        ?error,
                                                        "Error writing checkpoints during drain"
                                                    );
                                                }
                                                progress = true;
                                            }
                                            break;
                                        }
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

        // For done archives found without an existing watcher (restart scenario), apply
        // remove_after directly against the file's mtime — no watcher needed in fp_map.
        // The live-run case (archive that just reached EOF this process lifetime) is handled
        // separately: the reading loop removes the watcher via done_archives so the OS fd
        // is dropped immediately, and the next glob cycle calls back here for TTL cleanup.
        if !self.ignore_checkpoints && checkpoints.get_done(file_id) && self.is_archive(&path) {
            if let Some(remove_after) = self.remove_after {
                match tokio::fs::metadata(&path).await {
                    Ok(metadata) => {
                        let elapsed = metadata
                            .modified()
                            .ok()
                            .and_then(|mtime| mtime.elapsed().ok())
                            .unwrap_or_default();
                        if elapsed >= remove_after && self.should_ttl_delete(&path) {
                            debug!(
                                message = "Removing done archive past remove_after TTL.",
                                ?path
                            );
                            match tokio::fs::remove_file(&path).await {
                                Ok(()) => {
                                    self.emitter.emit_file_deleted(&path);
                                    checkpoints.set_dead(file_id);
                                }
                                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                    checkpoints.set_dead(file_id);
                                }
                                Err(e) => {
                                    warn!(message = "Failed to remove done archive.", ?path, error = %e);
                                }
                            }
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        // Already gone — nothing to do.
                    }
                    Err(e) => {
                        warn!(message = "Could not stat done archive for remove_after.", ?path, error = %e);
                    }
                }
            }
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
                // Creating a watcher means the file exists on disk; keep its
                // checkpoint live even for a caught-up `.gz` that never calls
                // `update`.
                checkpoints.clear_dead(file_id);
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

    // PathsProvider whose path list can be swapped from outside (simulates glob
    // returning different results as files are renamed / compressed / deleted).
    struct DynamicPathsProvider {
        paths: std::sync::Arc<std::sync::Mutex<Vec<PathBuf>>>,
    }

    impl PathsProvider for DynamicPathsProvider {
        type IntoIter = Vec<(Option<crate::paths_provider::LogFileInfo>, PathBuf)>;

        fn paths(&self) -> Self::IntoIter {
            self.paths
                .lock()
                .unwrap()
                .iter()
                .map(|p| (None, p.clone()))
                .collect()
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
            source_type: "file",
            read_loop_min_backoff: 1,
        }
    }

    fn test_log_file_info() -> crate::paths_provider::LogFileInfo {
        crate::paths_provider::LogFileInfo {
            pod_namespace: "dbr".to_string(),
            pod_name: "driver-pod".to_string(),
            pod_uid: "pod-uid".to_string(),
            container_name: "DEFAULT_CONTAINER_NAME".to_string(),
        }
    }

    #[test]
    fn file_to_pod_map_moves_metadata_when_watcher_path_changes() {
        let tmp = tempdir().unwrap();
        let file_to_pod_map = std::sync::Arc::new(std::sync::Mutex::new(HashMap::new()));
        let mut file_server = make_file_server(vec![], tmp.path().to_path_buf(), false);
        file_server.file_to_pod_map = Some(file_to_pod_map.clone());

        let old_path = PathBuf::from("/databricks/host-root/old/spark-master.out");
        let new_path = PathBuf::from("/databricks/host-root/new/spark-master.out.2026-06-05");
        let log_file_info = test_log_file_info();

        file_server.update_file_to_pod_map(old_path.clone(), Some(log_file_info.clone()));
        file_server.update_file_to_pod_map_for_path_change(
            &old_path,
            new_path.clone(),
            Some(log_file_info.clone()),
        );

        let file_to_pod_map = file_to_pod_map.lock().unwrap();
        assert_eq!(file_to_pod_map.get(&new_path), Some(&log_file_info));
        assert_eq!(
            file_to_pod_map.get(&old_path),
            Some(&log_file_info),
            "old watcher path should remain annotated for queued lines"
        );
    }

    #[test]
    fn file_to_pod_map_reuses_old_metadata_when_path_change_lacks_metadata() {
        let tmp = tempdir().unwrap();
        let file_to_pod_map = std::sync::Arc::new(std::sync::Mutex::new(HashMap::new()));
        let mut file_server = make_file_server(vec![], tmp.path().to_path_buf(), false);
        file_server.file_to_pod_map = Some(file_to_pod_map.clone());

        let old_path = PathBuf::from("/databricks/host-root/old/spark-master.out");
        let new_path = PathBuf::from("/databricks/host-root/old/spark-master.out.2026-06-05");
        let log_file_info = test_log_file_info();

        file_server.update_file_to_pod_map(old_path.clone(), Some(log_file_info.clone()));
        file_server.update_file_to_pod_map_for_path_change(&old_path, new_path.clone(), None);

        let file_to_pod_map = file_to_pod_map.lock().unwrap();
        assert_eq!(file_to_pod_map.get(&new_path), Some(&log_file_info));
        assert_eq!(
            file_to_pod_map.get(&old_path),
            Some(&log_file_info),
            "old watcher path should remain annotated for queued lines"
        );
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

    /// Reproduces the file-handle / data-loss bug triggered when a watched
    /// raw file is replaced by its gzip-compressed counterpart with the
    /// **same mtime** (which is `gzip(1)`'s default) and the raw file is
    /// then unlinked.
    ///
    /// The fingerprinter computes the archive's fingerprint from the
    /// decompressed first line, so it equals the raw file's fingerprint.
    /// In the discovery loop:
    ///   * `checkpoints.set_archive_path(file_id, ".gz")` runs
    ///     unconditionally after fingerprinting (file_server.rs ~line 272),
    ///     locking in the path → fingerprint mapping.
    ///   * The "more than one file has the same fingerprint" branch uses a
    ///     strict `old_mtime < new_mtime` comparison (file_server.rs ~line
    ///     302). With gzip's mtime preservation the mtimes are equal, so
    ///     `update_path` is **not** called and the watcher keeps pointing
    ///     at the raw file's inode.
    ///   * On subsequent cycles the cache hit at file_server.rs ~line 244
    ///     takes the fast path: it only sets `findable = true`. There is
    ///     no path / inode reconciliation, so even after the raw file is
    ///     unlinked the watcher continues to hold its fd on the deleted
    ///     inode and never reads new content from the `.gz`.
    ///
    /// Observable symptom: lines that live only inside the `.gz`
    /// (everything after the shared first line) are never delivered. This
    /// test asserts that `"beta"` reaches the output channel. With the bug
    /// it does not; with a fix that reconciles the watcher to the `.gz`
    /// inode, `gzip_reader_at_offset` resumes at the post-fingerprint
    /// position and delivers `"beta"`.
    #[tokio::test]
    async fn gzip_replacement_with_preserved_mtime_reconciles_watcher() {
        use async_compression::tokio::bufread::GzipEncoder;
        use tokio::io::AsyncReadExt;
        use tokio::sync::oneshot;

        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).await.unwrap();

        let log_path = tmp.path().join("app.log");
        let gz_path = tmp.path().join("app.log.gz");

        // Initial raw file. `"alpha\n"` will become the fingerprint line
        // for both the raw and the gzipped variants.
        fs::write(&log_path, b"alpha\n").await.unwrap();

        let file_server = make_file_server(vec![log_path.clone(), gz_path.clone()], data_dir, true);
        let (tx, rx) = mpsc::channel::<Vec<Line>>(8);

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let shutdown_data: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
            Box::pin(async move {
                let _ = shutdown_rx.await;
            });
        let shutdown_checkpointer = futures::future::ready(());
        let checkpointer = Checkpointer::new(tmp.path().join("data").as_path());

        let collector = tokio::spawn(async move {
            let mut rx = rx;
            let mut lines = Vec::new();
            while let Some(batch) = rx.next().await {
                lines.extend(batch);
            }
            lines
        });

        let log_path_m = log_path.clone();
        let gz_path_m = gz_path.clone();
        let manip: tokio::task::JoinHandle<DeletedFdSnapshot> = tokio::spawn(async move {
            // 1. Let the server discover `app.log` and read `"alpha"`.
            tokio::time::sleep(Duration::from_millis(500)).await;

            // 2. Create `app.log.gz` whose decompressed first line matches
            //    the raw file, plus an extra line (`"beta"`) that lives
            //    only inside the archive.
            let payload: &[u8] = b"alpha\nbeta\n";
            let mut encoder = GzipEncoder::new(payload);
            let mut gz_bytes = Vec::new();
            encoder.read_to_end(&mut gz_bytes).await.unwrap();
            fs::write(&gz_path_m, &gz_bytes).await.unwrap();

            // 3. Preserve mtime, as `gzip(1)` does by default. This is the
            //    trigger: the strict `<` mtime check at file_server.rs:302
            //    is false when mtimes are equal, so `update_path` is
            //    skipped even though `app.log.gz` has the same fingerprint.
            let raw_mtime = fs::metadata(&log_path_m).await.unwrap().modified().unwrap();
            let gz_file = std::fs::File::options()
                .write(true)
                .open(&gz_path_m)
                .unwrap();
            gz_file.set_modified(raw_mtime).unwrap();
            drop(gz_file);

            // 4. Let one or two discovery cycles run while both files
            //    coexist. The archive→fingerprint cache is populated here,
            //    but `update_path` is not called.
            tokio::time::sleep(Duration::from_millis(500)).await;

            // 5. Delete the raw file. The watcher's fd stays open on the
            //    now-unlinked inode. Subsequent discovery cycles take the
            //    fast path (cache hit) and only update `findable`, so the
            //    watcher is never reconciled to `app.log.gz`.
            fs::remove_file(&log_path_m).await.unwrap();

            // 6. Give the server time to run several more cycles.
            tokio::time::sleep(Duration::from_millis(700)).await;

            // 7. Probe /proc/self/fd for any descriptor whose readlink
            //    target is the deleted raw file. This runs *before*
            //    shutdown so we observe the steady-state of an actively
            //    running server, not the post-shutdown teardown.
            let snapshot = snapshot_deleted_fds(&log_path_m);

            let _ = shutdown_tx.send(());
            snapshot
        });

        let result = file_server
            .run(tx, shutdown_data, shutdown_checkpointer, checkpointer)
            .await;
        assert!(result.is_ok());
        let fd_snapshot = manip.await.unwrap();

        let received = collector.await.unwrap();
        let texts: Vec<String> = received
            .iter()
            .map(|l| String::from_utf8_lossy(&l.text).into_owned())
            .collect();

        assert!(
            texts.iter().any(|t| t == "alpha"),
            "expected to receive 'alpha' from app.log; got {:?}",
            texts,
        );

        // Direct symptom assertion: while the server is still running, no
        // open file descriptor should still point at the unlinked raw
        // file. The probe is implemented for Linux via /proc/self/fd; on
        // other platforms the snapshot is vacuously empty and this assert
        // is a no-op.
        assert!(
            fd_snapshot.leaked_fds.is_empty(),
            "FileServer held open fd(s) on the deleted raw file {:?}: {:?}",
            log_path,
            fd_snapshot.leaked_fds,
        );

        // Behavioural assertion: after reconciliation, the watcher should
        // resume from `gzip_reader_at_offset(reader, file_position)` and
        // deliver the post-fingerprint content. With the bug it does not.
        assert!(
            texts.iter().any(|t| t == "beta"),
            "expected the watcher to reconcile to app.log.gz and deliver \
             'beta', but it did not. The watcher is still bound to the \
             deleted raw inode (gzip-preserved-mtime bug). Received: {:?}",
            texts,
        );
    }

    /// Snapshot of any `/proc/self/fd/*` entries whose readlink target
    /// names the given path with the kernel's `" (deleted)"` suffix.
    /// Each entry is `(fd_number, readlink_target)`.
    #[derive(Debug, Default)]
    struct DeletedFdSnapshot {
        leaked_fds: Vec<(u32, String)>,
    }

    #[cfg(target_os = "linux")]
    fn snapshot_deleted_fds(unlinked_path: &Path) -> DeletedFdSnapshot {
        let marker = format!("{} (deleted)", unlinked_path.to_string_lossy());
        let mut leaked = Vec::new();
        if let Ok(entries) = std::fs::read_dir("/proc/self/fd") {
            for entry in entries.flatten() {
                let fd_num = entry.file_name().to_string_lossy().parse::<u32>().ok();
                let target = std::fs::read_link(entry.path())
                    .ok()
                    .map(|p| p.to_string_lossy().into_owned());
                if let (Some(fd), Some(t)) = (fd_num, target)
                    && t == marker
                {
                    leaked.push((fd, t));
                }
            }
        }
        DeletedFdSnapshot { leaked_fds: leaked }
    }

    #[cfg(not(target_os = "linux"))]
    fn snapshot_deleted_fds(_unlinked_path: &Path) -> DeletedFdSnapshot {
        DeletedFdSnapshot::default()
    }

    /// Count open file descriptors in `/proc/self/fd` whose readlink target
    /// equals `path` exactly (no `" (deleted)"` suffix — the file is still
    /// on disk). Returns 0 on non-Linux platforms, making the assertion
    /// vacuously true there.
    #[cfg(target_os = "linux")]
    fn count_open_fds_for_path(path: &Path) -> usize {
        let target = path.to_string_lossy().to_string();
        if let Ok(entries) = std::fs::read_dir("/proc/self/fd") {
            entries
                .flatten()
                .filter(|entry| {
                    std::fs::read_link(entry.path())
                        .ok()
                        .map(|t| t.to_string_lossy() == target)
                        .unwrap_or(false)
                })
                .count()
        } else {
            0
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn count_open_fds_for_path(_path: &Path) -> usize {
        0
    }

    /// Verifies that after a .gz archive is read to EOF and marked done,
    /// the FileServer holds zero OS file descriptors open for that file
    /// while the server is still running.
    #[tokio::test]
    async fn gz_no_fd_leak_after_file_marked_done() {
        use async_compression::tokio::bufread::GzipEncoder;
        use tokio::io::AsyncReadExt;
        use tokio::sync::oneshot;

        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).await.unwrap();

        let gz_path = tmp.path().join("app.log.gz");
        let payload: &[u8] = b"first line\nsecond line\n";
        let mut encoder = GzipEncoder::new(payload);
        let mut gz_bytes = Vec::new();
        encoder.read_to_end(&mut gz_bytes).await.unwrap();
        fs::write(&gz_path, &gz_bytes).await.unwrap();

        let file_server = make_file_server(vec![gz_path.clone()], data_dir.clone(), false);
        let (tx, rx) = mpsc::channel::<Vec<Line>>(8);

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let shutdown_data: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
            Box::pin(async move {
                let _ = shutdown_rx.await;
            });
        let checkpointer = Checkpointer::new(data_dir.as_path());

        let collector = tokio::spawn(async move {
            let mut lines = Vec::new();
            let mut rx = rx;
            while let Some(batch) = rx.next().await {
                lines.extend(batch);
            }
            lines
        });

        let gz_probe = gz_path.clone();
        let probe = tokio::spawn(async move {
            // Give the server time to fully read the gz file and call set_done.
            tokio::time::sleep(Duration::from_millis(800)).await;
            // Probe while the server is still running so we see steady-state fd usage.
            let open_count = count_open_fds_for_path(&gz_probe);
            let _ = shutdown_tx.send(());
            open_count
        });

        file_server
            .run(tx, shutdown_data, futures::future::ready(()), checkpointer)
            .await
            .unwrap();

        let open_count = probe.await.unwrap();
        let received = collector.await.unwrap();

        let texts: Vec<String> = received
            .iter()
            .map(|l| String::from_utf8_lossy(&l.text).into_owned())
            .collect();
        assert!(
            texts.iter().any(|t| t == "first line"),
            "expected gz content to be delivered; got: {texts:?}"
        );

        assert_eq!(
            open_count, 0,
            "expected 0 open fds on done gz file while server was still \
             running, found {open_count}"
        );
    }

    /// Verifies that a .gz archive previously marked done (checkpoint written by
    /// an earlier run) is deleted within its `remove_after` TTL when the server
    /// restarts, even though no new lines are delivered.
    #[tokio::test]
    async fn gz_honor_remove_after_on_restart_when_done() {
        use async_compression::tokio::bufread::GzipEncoder;
        use tokio::io::AsyncReadExt;
        use tokio::sync::oneshot;

        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).await.unwrap();

        let gz_path = tmp.path().join("app.log.gz");
        let payload: &[u8] = b"only line\n";
        let mut encoder = GzipEncoder::new(payload);
        let mut gz_bytes = Vec::new();
        encoder.read_to_end(&mut gz_bytes).await.unwrap();
        fs::write(&gz_path, &gz_bytes).await.unwrap();

        // Precondition: write a checkpoint that marks this file as done, exactly
        // as a previous Vector run would have left on disk.  Building it directly
        // avoids any timing dependency on a real FileServer run.
        //
        // get_state() only serialises fingerprints that are in the positions map,
        // so we must call both update_checkpoint (position) and set_done (done
        // flag + archive_paths) before writing.
        {
            let mut fingerprinter = Fingerprinter::new(
                FingerprintStrategy::FirstLinesChecksum {
                    ignored_header_bytes: 0,
                    lines: 1,
                },
                1024,
                true,
            );
            let mut known = HashMap::new();
            let fng = fingerprinter
                .fingerprint_or_emit(&gz_path, &mut known, &NoErrors)
                .await
                .expect("should fingerprint gz");

            let cp = Checkpointer::new(data_dir.as_path());
            let view = cp.view();
            view.update(fng, 10); // decompressed byte position after full read
            view.set_done(fng, &gz_path);
            cp.write_checkpoints()
                .await
                .expect("should write checkpoints");

            assert!(
                view.get_done(fng),
                "sanity: checkpoint should record is_done=true"
            );
        }

        // Run: restart with remove_after=1ms.  The checkpoint marks the file as
        // done, so watch_new_file returns early — no watcher, no remove_after.
        {
            let mut file_server = make_file_server(vec![gz_path.clone()], data_dir.clone(), false);
            // 1 ms grace period — the reading loop triggers removal immediately
            // on the first visit if a watcher exists.
            file_server.remove_after = Some(Duration::from_millis(1));

            let (tx, rx) = mpsc::channel::<Vec<Line>>(8);
            let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
            let shutdown_data: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
                Box::pin(async move {
                    let _ = shutdown_rx.await;
                });

            // Allow several glob cycles for remove_after to fire if it were going to.
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(600)).await;
                let _ = shutdown_tx.send(());
            });

            let checkpointer = Checkpointer::new(data_dir.as_path());
            let _drain = tokio::spawn(async move {
                let mut rx = rx;
                while let Some(_) = rx.next().await {}
            });

            file_server
                .run(tx, shutdown_data, futures::future::ready(()), checkpointer)
                .await
                .unwrap();
        }

        assert!(
            !gz_path.exists(),
            "expected done gz archive to be deleted by remove_after on restart, \
             but it still exists"
        );
    }

    /// Simulates a full log-rotation cycle, including the application
    /// immediately reopening the log file after logrotate:
    ///
    ///   Stage 0: active.json (logline1 / logline1b / logline1c) — server opens
    ///            and reads it.
    ///   Stage 1: rename active.json → active-1.json; create NEW active.json
    ///            with a Unix-timestamp first line.
    ///   Stage 2: gzip active-1.json → active-1.json.gz (mtime preserved,
    ///            as gzip -k does); delete active-1.json.
    ///
    /// Glob pattern: `active.json` and `active-*.json.gz`.
    /// `active-1.json` never matches and is never returned by the glob.
    ///
    /// Glob stages:
    ///   Stage 0: [active.json]
    ///   Stage 1: [active.json]  ← new file, different fingerprint
    ///   Stage 2: [active.json, active-1.json.gz]
    ///   Stage 3: [active.json, active-1.json.gz]  (after active-1.json deleted)
    ///
    /// The test asserts four things:
    ///   (a) After 2× rotate_wait no FD is held to the deleted active-1.json inode.
    ///   (b) Lines that exist only inside active-1.json.gz are eventually delivered
    ///       (proving the watcher reconciled to the .gz inode via update_path).
    ///   (c) The new active.json (first line: "{unix_secs} INFO new session started")
    ///       is picked up and its content delivered.
    ///   (d) The extra pre-rotation lines (logline1b, logline1c) survive the
    ///       rename+gzip and are still delivered.
    ///
    /// glob_minimum_cooldown = 1 s  (proportional to 1-minute production value)
    /// rotate_wait           = 20 s  (proportional to 5-minute production value)
    #[tokio::test]
    async fn rotation_while_original_file_still_being_read() {
        use async_compression::tokio::bufread::GzipEncoder;
        use std::sync::{Arc, Mutex};
        use tokio::io::AsyncReadExt;
        use tokio::sync::oneshot;

        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).await.unwrap();

        let active_json = tmp.path().join("active.json");
        let active_1_json = tmp.path().join("active-1.json");
        let active_1_gz = tmp.path().join("active-1.json.gz");

        // Each log line starts with yyyy/mm/dd hh:mm:ss.  A 1-second sleep
        // between writes ensures every timestamp is distinct, so each line has
        // a unique prefix and a unique fingerprint (FirstLinesChecksum uses the
        // first line only).
        use chrono::Local;
        let ts0 = Local::now().format("%Y/%m/%d %H:%M:%S").to_string();
        // Three lines; only the first drives the fingerprint. The extras slow the
        // read (8 bytes/2000 ms ≈ 20 s) so the watcher is still reading when the
        // .gz appears 3 s later, exercising survive-and-reconcile rather than EOF.
        let line0 = format!("{ts0} logline1");
        let line0b = format!("{ts0} logline1b");
        let line0c = format!("{ts0} logline1c");
        fs::write(&active_json, format!("{line0}\n{line0b}\n{line0c}\n").as_bytes())
            .await
            .unwrap();

        let current_paths: Arc<Mutex<Vec<PathBuf>>> =
            Arc::new(Mutex::new(vec![active_json.clone()]));

        let file_server = FileServer {
            paths_provider: DynamicPathsProvider {
                paths: current_paths.clone(),
            },
            max_read_bytes: 8,
            ignore_checkpoints: false,
            read_from: ReadFrom::Beginning,
            ignore_before: None,
            start_reading_at: None,
            max_line_bytes: 1024,
            line_delimiter: Bytes::from("\n"),
            data_dir: data_dir.clone(),
            glob_minimum_cooldown: Duration::from_secs(1),
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
            rotate_wait: Duration::from_secs(20),
            ttl_removal_config: None,
            source_context: None,
            file_to_pod_map: None,
            drain_on_shutdown: false,
            archive_extensions: vec!["gz".to_string()],
            source_type: "file",
            read_loop_min_backoff: 2000,
        };

        let (tx, rx) = mpsc::channel::<Vec<Line>>(32);
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let shutdown_data: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
            Box::pin(async move {
                let _ = shutdown_rx.await;
            });

        let collector = tokio::spawn(async move {
            let mut rx = rx;
            let mut lines: Vec<String> = Vec::new();
            while let Some(batch) = rx.next().await {
                for l in batch {
                    lines.push(String::from_utf8_lossy(&l.text).into_owned());
                }
            }
            lines
        });

        // The new active.json gets its own timestamp (≥1 s after line0 due to
        // the stage-0 sleep), guaranteeing a different fingerprint.
        let new_session_line = format!(
            "{} INFO new session started",
            Local::now().format("%Y/%m/%d %H:%M:%S")
        );

        let stage_driver = {
            let paths = current_paths.clone();
            let active_json = active_json.clone();
            let active_1_json = active_1_json.clone();
            let active_1_gz = active_1_gz.clone();
            let new_session_line = new_session_line.clone();
            let line0 = line0.clone();
            let line0b = line0b.clone();
            let line0c = line0c.clone();
            tokio::spawn(async move {
                // Stage 0 (2 s): server opens active.json and reads "logline1".
                tokio::time::sleep(Duration::from_secs(2)).await;

                // Stage 1 (3 s): rename active.json → active-1.json (never visible to
                // glob — doesn't match active.json or active-*.json.gz).  Immediately
                // create a new active.json with a datetime-prefixed first line,
                // simulating the application reopening the log file after logrotate.
                // Glob returns [active.json] for the new file; the old watcher
                // (logline1 fingerprint) becomes unfindable.
                fs::rename(&active_json, &active_1_json).await.unwrap();
                let new_content = format!("{new_session_line}\n");
                fs::write(&active_json, new_content.as_bytes()).await.unwrap();
                *paths.lock().unwrap() = vec![active_json.clone()];
                tokio::time::sleep(Duration::from_secs(3)).await; // ≥2 glob cycles @ 1 s cooldown

                // Stage 2 (2 s): compress active-1.json → active-1.json.gz with
                // preserved mtime (as gzip -k does).  active-1.json is never
                // returned by the glob.  Glob now returns [active.json, active-1.json.gz].
                // .gz carries the three original lines plus an archive-only
                // logline2, whose delivery proves the watcher switched inodes.
                let ts2 = chrono::Local::now().format("%Y/%m/%d %H:%M:%S").to_string();
                let gz_content = format!("{line0}\n{line0b}\n{line0c}\n{ts2} logline2\n");
                let extended = gz_content.as_bytes();
                let mut enc = GzipEncoder::new(extended);
                let mut gz_bytes = Vec::new();
                enc.read_to_end(&mut gz_bytes).await.unwrap();
                fs::write(&active_1_gz, &gz_bytes).await.unwrap();
                let raw_mtime = {
                    use std::fs::File;
                    File::open(&active_1_json)
                        .unwrap()
                        .metadata()
                        .unwrap()
                        .modified()
                        .unwrap()
                };
                std::fs::File::options()
                    .write(true)
                    .open(&active_1_gz)
                    .unwrap()
                    .set_modified(raw_mtime)
                    .unwrap();
                *paths.lock().unwrap() = vec![active_json.clone(), active_1_gz.clone()];
                tokio::time::sleep(Duration::from_secs(2)).await;

                // Stage 3 (41 s): delete active-1.json; glob unchanged.
                // Wait 2× rotate_wait + 1 glob cycle for any dead watcher to be expelled.
                fs::remove_file(&active_1_json).await.unwrap();
                tokio::time::sleep(Duration::from_secs(41)).await;

                let snapshot = snapshot_deleted_fds(&active_1_json);
                let _ = shutdown_tx.send(());
                snapshot
            })
        };

        let checkpointer = Checkpointer::new(data_dir.as_path());
        file_server
            .run(
                tx,
                shutdown_data,
                futures::future::ready(()),
                checkpointer,
            )
            .await
            .expect("FileServer::run failed");

        let fd_snapshot = stage_driver.await.expect("stage driver panicked");
        let lines = collector.await.expect("collector panicked");

        // (a) No FD should remain open to the deleted active-1.json inode after
        //     2× rotate_wait has elapsed.
        assert!(
            fd_snapshot.leaked_fds.is_empty(),
            "FileServer held open fd(s) to deleted active-1.json after \
             2× rotate_wait: {:?}",
            fd_snapshot.leaked_fds,
        );

        // (b) The archive-only line (yyyy/mm/dd hh:mm:ss logline2) exists only
        //     inside active-1.json.gz.  Its delivery proves the watcher reconciled
        //     to the .gz inode.
        assert!(
            lines.iter().any(|l| l.ends_with("logline2")),
            "expected a 'logline2' line from active-1.json.gz to be delivered; \
             received: {:?}",
            lines,
        );

        // (c) The new active.json (first line: "yyyy/mm/dd hh:mm:ss INFO new session
        //     started") is picked up independently of the rotation.
        assert!(
            lines.iter().any(|l| l == &new_session_line),
            "expected new-session line '{}' from new active.json to be delivered; \
             received: {:?}",
            new_session_line,
            lines,
        );

        // (d) logline1b / logline1c keep the read in progress across the
        //     rename+gzip; both must still be delivered from the archive.
        assert!(
            lines.iter().any(|l| l.ends_with("logline1b")),
            "expected 'logline1b' from the archived active.json content to be \
             delivered; received: {:?}",
            lines,
        );
        assert!(
            lines.iter().any(|l| l.ends_with("logline1c")),
            "expected 'logline1c' from the archived active.json content to be \
             delivered; received: {:?}",
            lines,
        );
    }

    /// Rotation where the original file is *fully processed* before its `.gz`
    /// archive appears. The fix keeps the live watcher across the gap so it hands
    /// off to the `.gz`, delivering each original line exactly once.
    ///
    ///   Stage 0: active.json (logline1 / logline1b / logline1c) — read to EOF.
    ///   Stage 1: rename active.json → active-1.json; create NEW active.json.
    ///            The original (non-archive) watcher goes unfindable but is kept
    ///            alive for rotate_wait (5 min) rather than reaped at EOF.
    ///   Stage 2 (30 s later, past the 10 s dead-retention but < rotate_wait):
    ///            gzip active-1.json → active-1.json.gz; the glob returns the .gz,
    ///            which shares the watcher's fingerprint, so update_path repoints
    ///            the live watcher to the .gz and it resumes at its EOF position.
    ///
    /// Asserts each original line is delivered exactly once (no re-delivery),
    /// which previously failed: the watcher was reaped at EOF and the checkpoint
    /// expired (dead-retention) before the .gz appeared.
    #[tokio::test]
    async fn rotation_while_original_file_still_was_done_no_redelivery() {
        use async_compression::tokio::bufread::GzipEncoder;
        use std::sync::{Arc, Mutex};
        use tokio::io::AsyncReadExt;
        use tokio::sync::oneshot;

        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).await.unwrap();

        let active_json = tmp.path().join("active.json");
        let active_1_json = tmp.path().join("active-1.json");
        let active_1_gz = tmp.path().join("active-1.json.gz");

        use chrono::Local;
        let ts0 = Local::now().format("%Y/%m/%d %H:%M:%S").to_string();
        let line0 = format!("{ts0} logline1");
        let line0b = format!("{ts0} logline1b");
        let line0c = format!("{ts0} logline1c");
        let orig_content = format!("{line0}\n{line0b}\n{line0c}\n");
        fs::write(&active_json, orig_content.as_bytes()).await.unwrap();

        // Fingerprint + full length of the original file, used to simulate the
        // checkpoint a fix would persist once the file is fully read.
        let orig_position = orig_content.len() as u64;
        let orig_fingerprint = {
            let mut fp = Fingerprinter::new(
                FingerprintStrategy::FirstLinesChecksum {
                    ignored_header_bytes: 0,
                    lines: 1,
                },
                1024,
                true,
            );
            let mut ksf = std::collections::HashMap::new();
            fp.fingerprint_or_emit(&active_json, &mut ksf, &NoErrors)
                .await
                .expect("failed to fingerprint original file")
        };

        let current_paths: Arc<Mutex<Vec<PathBuf>>> =
            Arc::new(Mutex::new(vec![active_json.clone()]));

        let file_server = FileServer {
            paths_provider: DynamicPathsProvider {
                paths: current_paths.clone(),
            },
            // Large read + minimum backoff: the original is drained to EOF within
            // the first glob cycles, before rotation.
            max_read_bytes: 2048,
            ignore_checkpoints: false,
            read_from: ReadFrom::Beginning,
            ignore_before: None,
            start_reading_at: None,
            max_line_bytes: 1024,
            line_delimiter: Bytes::from("\n"),
            data_dir: data_dir.clone(),
            glob_minimum_cooldown: Duration::from_secs(1),
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
            rotate_wait: Duration::from_secs(5 * 60),
            ttl_removal_config: None,
            source_context: None,
            file_to_pod_map: None,
            drain_on_shutdown: false,
            archive_extensions: vec!["gz".to_string()],
            source_type: "file",
            read_loop_min_backoff: 1,
        };

        let (tx, rx) = mpsc::channel::<Vec<Line>>(32);
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let shutdown_data: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
            Box::pin(async move {
                let _ = shutdown_rx.await;
            });

        // A real shutdown_checkpointer signal (rather than ready(())), so the
        // periodic checkpoint writer keeps ticking every glob_minimum_cooldown
        // during the run instead of exiting on its first select!.
        let (ckpt_shutdown_tx, ckpt_shutdown_rx) = oneshot::channel::<()>();
        let shutdown_checkpointer: std::pin::Pin<
            Box<dyn std::future::Future<Output = ()> + Send>,
        > = Box::pin(async move {
            let _ = ckpt_shutdown_rx.await;
        });

        // Shared buffer so the stage driver can observe delivery in real time and
        // stop as soon as the .gz re-delivery has landed.
        let delivered: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let collector = {
            let delivered = delivered.clone();
            tokio::spawn(async move {
                let mut rx = rx;
                while let Some(batch) = rx.next().await {
                    let mut buf = delivered.lock().unwrap();
                    for l in batch {
                        buf.push(String::from_utf8_lossy(&l.text).into_owned());
                    }
                }
            })
        };

        let new_session_line = format!(
            "{} INFO new session started",
            Local::now().format("%Y/%m/%d %H:%M:%S")
        );

        // Create the checkpointer up front and grab its shared view so the stage
        // driver can simulate the checkpoint a fix would persist. Use a short
        // 10 s dead-retention so the dead-fingerprint expiry would fire well
        // before the .gz appears at 30 s — the fix must keep the live watcher
        // regardless.
        let checkpointer = Checkpointer::new(data_dir.as_path())
            .with_dead_retention(chrono::Duration::seconds(10));
        let checkpoints_view = checkpointer.view();

        let stage_driver = {
            let paths = current_paths.clone();
            let active_json = active_json.clone();
            let active_1_json = active_1_json.clone();
            let active_1_gz = active_1_gz.clone();
            let new_session_line = new_session_line.clone();
            let line0 = line0.clone();
            let line0b = line0b.clone();
            let line0c = line0c.clone();
            let delivered = delivered.clone();
            let checkpoints_view = checkpoints_view.clone();
            let orig_fingerprint = orig_fingerprint;
            let orig_position = orig_position;
            tokio::spawn(async move {
                // Stage 0: wait until the batch notifier (delivered buffer) shows
                // every original line has been delivered — i.e. the file was read
                // to EOF — then simulate the checkpoint a fix would persist at that
                // point: record the original fingerprint at its full-content offset.
                let deliver_deadline = Duration::from_secs(30);
                let poll = Duration::from_millis(200);
                let mut waited = Duration::ZERO;
                loop {
                    let all_delivered = {
                        let buf = delivered.lock().unwrap();
                        [&line0, &line0b, &line0c].iter().all(|orig| {
                            buf.iter().any(|l| l.as_str() == orig.as_str())
                        })
                    };
                    if all_delivered || waited >= deliver_deadline {
                        break;
                    }
                    tokio::time::sleep(poll).await;
                    waited += poll;
                }
                checkpoints_view.update(orig_fingerprint, orig_position);

                // Stage 1: rename active.json → active-1.json (invisible to glob),
                // create a new active.json (app reopens its log).
                fs::rename(&active_json, &active_1_json).await.unwrap();
                fs::write(&active_json, format!("{new_session_line}\n").as_bytes())
                    .await
                    .unwrap();
                *paths.lock().unwrap() = vec![active_json.clone()];

                // Stage 2 (30 s after rotation, still < rotate_wait = 5 min, but
                // well past the 10 s dead-retention): compress active-1.json →
                // active-1.json.gz with preserved mtime; expose the .gz to the
                // glob.  It shares the original's first-line fingerprint.
                tokio::time::sleep(Duration::from_secs(30)).await;
                let gz_content = format!("{line0}\n{line0b}\n{line0c}\n");
                let mut enc = GzipEncoder::new(gz_content.as_bytes());
                let mut gz_bytes = Vec::new();
                enc.read_to_end(&mut gz_bytes).await.unwrap();
                fs::write(&active_1_gz, &gz_bytes).await.unwrap();
                let raw_mtime = {
                    use std::fs::File;
                    File::open(&active_1_json)
                        .unwrap()
                        .metadata()
                        .unwrap()
                        .modified()
                        .unwrap()
                };
                std::fs::File::options()
                    .write(true)
                    .open(&active_1_gz)
                    .unwrap()
                    .set_modified(raw_mtime)
                    .unwrap();
                *paths.lock().unwrap() = vec![active_json.clone(), active_1_gz.clone()];
                fs::remove_file(&active_1_json).await.unwrap();

                // Give the .gz a bounded window (20 s) to be globbed and read.
                // With the simulated checkpoint in place it should resume at EOF
                // and deliver nothing; the assertion below verifies no re-delivery.
                tokio::time::sleep(Duration::from_secs(20)).await;
                let _ = shutdown_tx.send(());
                let _ = ckpt_shutdown_tx.send(());
            })
        };

        file_server
            .run(tx, shutdown_data, shutdown_checkpointer, checkpointer)
            .await
            .expect("FileServer::run failed");

        stage_driver.await.expect("stage driver panicked");
        collector.await.expect("collector panicked");
        let lines = delivered.lock().unwrap().clone();

        // The live watcher survives the rotation gap (non-archive kept for
        // rotate_wait) and follows active.json -> active-1.json -> .gz via
        // update_path on the shared fingerprint, resuming at its EOF position.
        // So each original line is delivered exactly ONCE — no re-delivery.
        for orig in [&line0, &line0b, &line0c] {
            let count = lines.iter().filter(|l| l.as_str() == orig.as_str()).count();
            assert_eq!(
                count, 1,
                "expected original line {:?} to be delivered exactly once \
                 (watcher handed off to the .gz across rotation); got {} in {:?}",
                orig, count, lines,
            );
        }
    }

    // The FileServer read loop must never advance the persisted checkpoint on its own — the offset
    // is advanced only by the source's downstream map-closure (on ack, or at pull time for no-acks).
    // So a read-but-unforwarded tail stays at the last checkpointed offset and is re-read on restart
    // (at-least-once), not skipped. Fails if a change ever checkpoints read-but-unforwarded bytes.
    #[tokio::test]
    async fn drain_off_shutdown_does_not_checkpoint_undelivered_tail() {
        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).await.unwrap();

        let log_path = tmp.path().join("test.log");
        // Total size comfortably exceeds max_read_bytes (2048) so a single
        // read iteration cannot consume the whole file.
        let content: String = (0..200)
            .map(|i| format!("line {:04} -- padding to make this longer\n", i))
            .collect();
        fs::write(&log_path, &content).await.unwrap();
        let file_size = fs::metadata(&log_path).await.unwrap().len();

        let file_server = make_file_server(vec![log_path.clone()], data_dir.clone(), false);
        let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);

        // Immediate shutdown: exactly one read iteration before the select fires.
        let shutdown_data = futures::future::ready(());
        let shutdown_checkpointer = futures::future::ready(());
        let checkpointer = Checkpointer::new(data_dir.as_path());

        let result = file_server
            .run(tx, shutdown_data, shutdown_checkpointer, checkpointer)
            .await;
        assert!(result.is_ok());

        // Drain whatever was already handed to the (now-closed) channel.
        let mut received = Vec::new();
        while let Ok(batch) = rx.try_recv() {
            received.extend(batch);
        }
        // Partial read: some but not all lines were delivered.
        assert!(
            !received.is_empty() && received.len() < 200,
            "expected a partial read (0 < Y < 200), got {} lines",
            received.len()
        );

        // Reload the persisted checkpoint in a fresh Checkpointer.
        let mut checkpointer = Checkpointer::new(data_dir.as_path());
        checkpointer.read_checkpoints(None).await;
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
        let position = checkpointer.view().get(fingerprint).unwrap_or(0);

        // No downstream wiring advanced the offset, so the checkpoint must sit below EOF — a
        // checkpoint at EOF would skip the undelivered tail on restart (a real drop).
        assert!(
            position < file_size,
            "checkpoint ({position}) advanced to/past EOF ({file_size}) for \
             undelivered data — the tail would be dropped on restart"
        );
        assert_eq!(
            position, 0,
            "read loop must not advance the checkpoint for un-forwarded data; \
             expected checkpoint 0, got {position}"
        );
    }
}
