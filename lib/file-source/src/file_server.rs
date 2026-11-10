use std::{
    cmp,
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{self, Duration},
};

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
#[cfg(test)]
use file_source_common::CheckpointWriteHook;
use file_source_common::{
    FileFingerprint, FileReadStartedInfo, FileSourceInternalEvents, FileStopReason,
    FileUnwatchedInfo, Fingerprinter, ReadFrom,
    checkpointer::{Checkpointer, CheckpointsView},
};
use futures::{
    Future, FutureExt, Sink, SinkExt, StreamExt,
    future::{AbortHandle, Abortable, BoxFuture, Either, select},
    stream::FuturesUnordered,
};
use futures_util::future::join_all;
use indexmap::IndexMap;
use tokio::{
    fs::{self, remove_file},
    task::{Id, JoinSet},
    time::sleep,
};

use tracing::{debug, error, info, trace, warn};
use vector_common::internal_event::{
    delivery_singleton,
    vector_event::delivery_event::{
        REJECTION_REASON_LINE_TOO_LONG, current_hour_time_parity_ms_value,
        source_pod_id_from_file_path,
    },
};

use crate::{
    FileTTLAction, FileTTLRemovalConfig,
    admission::{
        AdmissionError, AdmissionKey, CheckpointGate, CheckpointGateResolver, CheckpointGateResult,
        FileSourceAdmission, FileSourceBacklogLease, OwnedFileSourceAdmissionPermit,
    },
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
    pub idle_read_interval: Duration,
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
    /// Maximum read-loop backoff in milliseconds. Must be positive.
    pub read_loop_max_backoff: usize,
    /// Optional process-scoped admission controller.
    pub admission: Option<Arc<dyn FileSourceAdmission>>,
    #[cfg(test)]
    test_readiness_hook: Option<TestReadinessHook>,
    #[cfg(test)]
    test_line_admission_hook: Option<TestLineAdmissionHook>,
    #[cfg(test)]
    test_before_send_hook: Option<TestBeforeSendHook>,
    #[cfg(test)]
    test_checkpoint_flush_hook: Option<Arc<TestCheckpointFlushHook>>,
}

#[cfg(test)]
type TestReadinessHook = Arc<dyn Fn(&Path) -> bool + Send + Sync>;
#[cfg(test)]
type TestLineAdmissionHook = Arc<dyn Fn(Line) -> Option<Line> + Send + Sync>;
#[cfg(test)]
type TestBeforeSendHook = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

#[cfg(test)]
struct TestCheckpointFlushHook {
    armed: std::sync::atomic::AtomicBool,
    unarmed_temporary_file_synced: tokio::sync::Semaphore,
    started: tokio::sync::Semaphore,
    resume: tokio::sync::Semaphore,
}

#[cfg(test)]
impl TestCheckpointFlushHook {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            armed: std::sync::atomic::AtomicBool::new(false),
            unarmed_temporary_file_synced: tokio::sync::Semaphore::new(0),
            started: tokio::sync::Semaphore::new(0),
            resume: tokio::sync::Semaphore::new(0),
        })
    }

    fn arm(&self) {
        assert!(!self.armed.swap(true, Ordering::SeqCst));
    }
}

#[cfg(test)]
impl CheckpointWriteHook for TestCheckpointFlushHook {
    fn after_temporary_file_sync(&self) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            if !self.armed.swap(false, Ordering::SeqCst) {
                self.unarmed_temporary_file_synced.add_permits(1);
                return;
            }
            self.started.add_permits(1);
            self.resume.acquire().await.unwrap().forget();
        })
    }
}

// Request IDs must be unique across FileServer instances: multiple source components can observe
// different paths for one pod and share one process-scoped admission coordinator.
static NEXT_ADMISSION_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

fn next_admission_request_id() -> Option<u64> {
    NEXT_ADMISSION_REQUEST_ID
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .ok()
}

/// Seeded multiplicative hasher for the dispatcher's per-record map operations. The keys are small
/// integers (fingerprints, offsets); SipHash costs more than the rest of a lookup, and a per-process
/// random seed keeps colliding keys unpredictable.
#[derive(Clone, Copy)]
struct DispatcherBuildHasher(u64);

impl Default for DispatcherBuildHasher {
    fn default() -> Self {
        static SEED: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
        Self(*SEED.get_or_init(|| {
            use std::hash::BuildHasher;
            std::collections::hash_map::RandomState::new().hash_one(0_u8) | 1
        }))
    }
}

impl std::hash::BuildHasher for DispatcherBuildHasher {
    type Hasher = DispatcherHasher;

    fn build_hasher(&self) -> DispatcherHasher {
        DispatcherHasher(self.0)
    }
}

struct DispatcherHasher(u64);

impl DispatcherHasher {
    const MULTIPLIER: u64 = 0x517c_c1b7_2722_0a95;

    #[inline]
    fn mix(&mut self, word: u64) {
        self.0 = (self.0.rotate_left(5) ^ word).wrapping_mul(Self::MULTIPLIER);
    }
}

impl std::hash::Hasher for DispatcherHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(8) {
            let mut word = [0_u8; 8];
            word[..chunk.len()].copy_from_slice(chunk);
            self.mix(u64::from_le_bytes(word));
        }
    }

    #[inline]
    fn write_u8(&mut self, value: u8) {
        self.mix(u64::from(value));
    }

    #[inline]
    fn write_u32(&mut self, value: u32) {
        self.mix(u64::from(value));
    }

    #[inline]
    fn write_u64(&mut self, value: u64) {
        self.mix(value);
    }

    #[inline]
    fn write_usize(&mut self, value: usize) {
        self.mix(value as u64);
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.0.rotate_left(26)
    }
}

type DispatcherMap<K, V> = HashMap<K, V, DispatcherBuildHasher>;
type DispatcherSet<K> = HashSet<K, DispatcherBuildHasher>;

struct ReadyAdmission {
    line: Line,
    permit: Result<OwnedFileSourceAdmissionPermit, AdmissionError>,
    gate: CheckpointGateResolver,
}

struct CompletedAdmissionWait {
    file_id: FileFingerprint,
    start_offset: u64,
    ready: Option<ReadyAdmission>,
}

/// One file's records sent together, in offset order, so they are finished in that order.
struct AcknowledgedAdmission {
    file_id: FileFingerprint,
    records: Vec<AcknowledgedRecord>,
}

struct AcknowledgedRecord {
    start_offset: u64,
    gate: CheckpointGateResolver,
    result: CheckpointGateResult,
}

struct PreparedLine {
    line: Line,
    payload_bytes: usize,
    pending: Option<(
        Box<dyn crate::admission::PendingAdmission>,
        CheckpointGateResolver,
    )>,
}

enum PrepareLineResult {
    Line(PreparedLine),
    NoLine,
    ReadError,
    AdmissionDeferred,
}

/// Keeps one file's throttled backlog from occupying the process-wide admission bound. This is a
/// refillable scheduling tranche, not an entitlement or service-weight quota.
const ADMISSION_READ_AHEAD_PER_FILE: usize = 4;
/// Bounds one file's granted records awaiting send or acknowledgement. Granted records hold no
/// pre-read reservation, so an unthrottled file may run this far ahead of its waits; one
/// acknowledgement round trip then covers many records instead of capping the file's rate.
const ADMISSION_IN_FLIGHT_PER_FILE: usize = 32;
const MAX_ADMISSION_RESERVATION_TRANCHE_SIZE: usize = 64;
/// Most records one file visit may admit. A visit keeps reading only while each permit is issued
/// without waiting, so the per-visit work (metrics, stats, scan bookkeeping) is shared by the run
/// while a throttled file still yields after its first waiting record. The budget bounds how long
/// one file can hold the scan loop ahead of the others (they each get a visit per pass) and is
/// half the in-flight bound so two files can fill their windows from one pass. It replaces
/// `max_read_bytes` for admitted reads, which would otherwise end the visit after one record.
const ADMISSION_VISIT_LINE_BUDGET: usize = 16;

/// Independently polls entitlement waits so one pod cannot block another pod's ready permit.
pub struct AdmissionDispatcher {
    reservation_tranche_size: usize,
    pending: FuturesUnordered<BoxFuture<'static, CompletedAdmissionWait>>,
    pending_abort_handles: DispatcherMap<FileFingerprint, Vec<(u64, AbortHandle)>>,
    ready_by_file: DispatcherMap<FileFingerprint, BTreeMap<u64, ReadyAdmission>>,
    ready_files: VecDeque<(FileFingerprint, u64)>,
    ready_file_set: DispatcherSet<FileFingerprint>,
    ready_file_generations: DispatcherMap<FileFingerprint, u64>,
    acknowledgements: FuturesUnordered<BoxFuture<'static, AcknowledgedAdmission>>,
    pending_files: DispatcherSet<FileFingerprint>,
    pending_replays: DispatcherMap<FileFingerprint, u64>,
    pending_offsets: DispatcherMap<FileFingerprint, VecDeque<u64>>,
    /// How many of the file's front pending offsets are sent and awaiting acknowledgement.
    acknowledging_files: DispatcherMap<FileFingerprint, usize>,
    refresh_after_acknowledgement: DispatcherSet<FileFingerprint>,
    backlog_leases: DispatcherMap<FileFingerprint, RetainedFileBacklog>,
    admission_tranches: DispatcherMap<FileFingerprint, RetainedAdmissionTranche>,
}

struct RetainedFileBacklog {
    metadata: crate::admission::AdmissionMetadata,
    _lease: Box<dyn FileSourceBacklogLease>,
}

struct RetainedAdmissionTranche {
    metadata: crate::admission::AdmissionMetadata,
    reservations: VecDeque<Box<dyn crate::admission::PendingAdmission>>,
}

impl AdmissionDispatcher {
    fn new(reservation_tranche_size: usize) -> Self {
        assert!(
            (1..=MAX_ADMISSION_RESERVATION_TRANCHE_SIZE).contains(&reservation_tranche_size),
            "admission reservation tranche size exceeds the supported bound"
        );
        Self {
            reservation_tranche_size,
            pending: FuturesUnordered::new(),
            pending_abort_handles: Default::default(),
            ready_by_file: Default::default(),
            ready_files: VecDeque::new(),
            ready_file_set: Default::default(),
            ready_file_generations: Default::default(),
            acknowledgements: FuturesUnordered::new(),
            pending_files: Default::default(),
            pending_replays: Default::default(),
            pending_offsets: Default::default(),
            acknowledging_files: Default::default(),
            refresh_after_acknowledgement: Default::default(),
            backlog_leases: Default::default(),
            admission_tranches: Default::default(),
        }
    }

    async fn retain_file_backlog(
        &mut self,
        file_id: FileFingerprint,
        admission: Arc<dyn FileSourceAdmission>,
        metadata: crate::admission::AdmissionMetadata,
    ) -> Result<(), AdmissionError> {
        if self
            .backlog_leases
            .get(&file_id)
            .is_some_and(|retained| retained.metadata == metadata)
        {
            return Ok(());
        }
        self.backlog_leases.remove(&file_id);
        if let Some(lease) = admission.retain_backlog(metadata.clone()).await? {
            self.backlog_leases.insert(
                file_id,
                RetainedFileBacklog {
                    metadata,
                    _lease: lease,
                },
            );
        }
        Ok(())
    }

    fn clear_file_backlog(&mut self, file_id: FileFingerprint) {
        self.backlog_leases.remove(&file_id);
        self.admission_tranches.remove(&file_id);
    }

    async fn take_or_reserve_file_credit(
        &mut self,
        file_id: FileFingerprint,
        admission: Arc<dyn FileSourceAdmission>,
        metadata: crate::admission::AdmissionMetadata,
        max_line_bytes: u64,
    ) -> Result<Box<dyn crate::admission::PendingAdmission>, AdmissionError> {
        if self
            .admission_tranches
            .get(&file_id)
            .is_some_and(|tranche| tranche.metadata != metadata)
        {
            self.admission_tranches.remove(&file_id);
        }
        if let Some(reservation) = self
            .admission_tranches
            .get_mut(&file_id)
            .and_then(|tranche| tranche.reservations.pop_front())
        {
            return Ok(reservation);
        }

        let request_ids = (0..self.reservation_tranche_size)
            .map(|_| next_admission_request_id().ok_or(AdmissionError::Internal))
            .collect::<Result<Vec<_>, _>>()?;
        let reservations = admission
            .reserve_tranche(metadata.clone(), request_ids, max_line_bytes)
            .await?;
        let mut reservations = VecDeque::from(reservations);
        let reservation = reservations
            .pop_front()
            .ok_or(AdmissionError::BoundsExceeded)?;
        self.admission_tranches.insert(
            file_id,
            RetainedAdmissionTranche {
                metadata,
                reservations,
            },
        );
        Ok(reservation)
    }

    fn insert_ready(&mut self, ready: ReadyAdmission) {
        let file_id = ready.line.file_id;
        let start_offset = ready.line.start_offset;
        let previous = self
            .ready_by_file
            .entry(file_id)
            .or_default()
            .insert(start_offset, ready);
        debug_assert!(previous.is_none());
        self.queue_ready_file(file_id);
    }

    /// Starts the same per-line wait protocol for normal reads and shutdown drain. Returns whether
    /// the permit was issued without waiting.
    fn enqueue(
        &mut self,
        line: Line,
        mut reservation: Box<dyn crate::admission::PendingAdmission>,
        gate: CheckpointGateResolver,
    ) -> bool {
        let file_id = line.file_id;
        let start_offset = line.start_offset;
        self.pending_files.insert(file_id);
        self.pending_replays.entry(file_id).or_insert(start_offset);
        self.pending_offsets
            .entry(file_id)
            .or_default()
            .push_back(start_offset);
        // A reservation whose permit needs no waiting skips the wait machinery (boxed future,
        // abort handle, `FuturesUnordered` round trip) but is ordered exactly like a completed wait.
        if let Some(permit) = reservation.try_wait() {
            self.insert_ready(ReadyAdmission { line, permit, gate });
            return true;
        }
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        self.pending_abort_handles
            .entry(file_id)
            .or_default()
            .push((start_offset, abort_handle));
        self.pending.push(
            async move {
                let ready = Abortable::new(
                    async {
                        ReadyAdmission {
                            line,
                            permit: reservation.wait().await,
                            gate,
                        }
                    },
                    abort_registration,
                )
                .await
                .ok();
                CompletedAdmissionWait {
                    file_id,
                    start_offset,
                    ready,
                }
            }
            .boxed(),
        );
        false
    }

    fn queue_ready_file(&mut self, file_id: FileFingerprint) {
        if self.acknowledging_files.contains_key(&file_id) {
            return;
        }
        let Some(start_offset) = self
            .pending_offsets
            .get(&file_id)
            .and_then(|offsets| offsets.front())
        else {
            return;
        };
        if !self
            .ready_by_file
            .get(&file_id)
            .is_some_and(|ready| ready.contains_key(start_offset))
        {
            return;
        }
        if self.ready_file_set.insert(file_id) {
            let generation = *self.ready_file_generations.entry(file_id).or_default();
            self.ready_files.push_back((file_id, generation));
        }
    }

    fn poll_waits(&mut self) {
        while let Some(Some(completed)) = self.pending.next().now_or_never() {
            self.record_completed_wait(completed);
        }
    }

    fn record_completed_wait(&mut self, completed: CompletedAdmissionWait) {
        // An emptied entry stays until the file's last record resolves (`finish_file`) so a steady
        // stream of waits does not reallocate it per record.
        if let Some(handles) = self.pending_abort_handles.get_mut(&completed.file_id)
            && let Some(position) = handles
                .iter()
                .position(|(offset, _)| *offset == completed.start_offset)
        {
            handles.swap_remove(position);
        }
        if let Some(ready) = completed.ready {
            self.insert_ready(ready);
        }
    }

    fn has_outstanding_work(&self) -> bool {
        !self.pending.is_empty() || !self.acknowledgements.is_empty()
    }

    /// Resolves when an admission wait or acknowledgement completes. A completed wait is recorded
    /// as ready; an acknowledgement is returned for the caller to finish. Both arms are
    /// cancellation safe, so a losing timer never drops a disposition.
    async fn wait_for_progress(&mut self) -> Option<AcknowledgedAdmission> {
        tokio::select! {
            Some(completed) = self.pending.next(), if !self.pending.is_empty() => {
                self.record_completed_wait(completed);
                None
            }
            Some(acknowledgement) = self.acknowledgements.next(), if !self.acknowledgements.is_empty() => {
                Some(acknowledgement)
            }
            else => None,
        }
    }

    fn next_ready(&mut self) -> Option<ReadyAdmission> {
        self.next_ready_run(1).pop()
    }

    /// Returns one file's ready front record and, when every permit supports batching, the ready
    /// records contiguous behind it. Sending them together lets one acknowledgement round trip
    /// cover the whole read-ahead tranche instead of one record per read pass.
    fn next_ready_run(&mut self, limit: usize) -> Vec<ReadyAdmission> {
        // One global ready queue registers all new waits and only repolls woken waits. Completed
        // later offsets remain buffered until the ordered front is sent and acknowledged.
        self.poll_waits();
        while let Some((file_id, generation)) = self.ready_files.pop_front() {
            if self
                .ready_file_generations
                .get(&file_id)
                .copied()
                .unwrap_or_default()
                != generation
                || !self.ready_file_set.remove(&file_id)
                || self.acknowledging_files.contains_key(&file_id)
            {
                continue;
            }
            let (Some(offsets), Some(ready_offsets)) = (
                self.pending_offsets.get(&file_id),
                self.ready_by_file.get_mut(&file_id),
            ) else {
                continue;
            };
            let mut run: Vec<ReadyAdmission> = Vec::new();
            for start_offset in offsets.iter().take(limit) {
                let batchable = |ready: &ReadyAdmission| {
                    ready
                        .permit
                        .as_ref()
                        .is_ok_and(|permit| permit.supports_batch())
                };
                if run.first().is_some_and(|front| !batchable(front))
                    || ready_offsets
                        .get(start_offset)
                        .is_none_or(|ready| !run.is_empty() && !batchable(ready))
                {
                    break;
                }
                let ready = ready_offsets
                    .remove(start_offset)
                    .expect("checked ready offset");
                debug_assert_eq!(*start_offset, ready.line.start_offset);
                run.push(ready);
            }
            if !run.is_empty() {
                return run;
            }
        }
        Vec::new()
    }

    fn next_ready_batch(&mut self) -> Vec<ReadyAdmission> {
        let mut ready = Vec::new();
        loop {
            let run = self.next_ready_run(usize::MAX);
            if run.is_empty() {
                return ready;
            }
            ready.extend(run);
        }
    }

    fn next_acknowledged(&mut self) -> Option<AcknowledgedAdmission> {
        self.acknowledgements.next().now_or_never().flatten()
    }

    fn file_has_read_ahead_capacity(&self, file_id: FileFingerprint) -> bool {
        let has_cached_reservation = self
            .admission_tranches
            .get(&file_id)
            .is_some_and(|tranche| !tranche.reservations.is_empty());
        let pending_offsets = self.pending_offsets.get(&file_id).map_or(0, VecDeque::len);
        let unresolved_waits = self.pending_abort_handles.get(&file_id).map_or(0, Vec::len);
        pending_offsets < ADMISSION_IN_FLIGHT_PER_FILE
            && unresolved_waits < ADMISSION_READ_AHEAD_PER_FILE
            && (has_cached_reservation
                || self.reservation_tranche_size == 1
                || unresolved_waits == 0)
    }

    fn finish_file(&mut self, file_id: FileFingerprint) {
        self.pending_abort_handles.remove(&file_id);
        self.ready_by_file.remove(&file_id);
        if self.ready_file_set.remove(&file_id) {
            self.ready_files.retain(|(queued, _)| *queued != file_id);
        }
        self.ready_file_generations.remove(&file_id);
        self.pending_files.remove(&file_id);
        self.pending_replays.remove(&file_id);
        self.pending_offsets.remove(&file_id);
        self.acknowledging_files.remove(&file_id);
        self.refresh_after_acknowledgement.remove(&file_id);
    }

    async fn refresh_file_metadata(&mut self, file_id: FileFingerprint) -> Option<u64> {
        if !self.pending_files.contains(&file_id) {
            self.clear_file_backlog(file_id);
            return None;
        }
        if self.acknowledging_files.contains_key(&file_id) {
            self.refresh_after_acknowledgement.insert(file_id);
            self.clear_file_backlog(file_id);
            return None;
        }
        let replay_offset = self.pending_replays.get(&file_id).copied();
        self.cancel_file(file_id).await;
        replay_offset
    }

    async fn cancel_file(&mut self, file_id: FileFingerprint) {
        if let Some(abort_handles) = self.pending_abort_handles.remove(&file_id) {
            for (_, abort_handle) in abort_handles {
                abort_handle.abort();
            }
        }
        self.poll_waits();
        let ready = self.ready_by_file.remove(&file_id).unwrap_or_default();
        self.admission_tranches.remove(&file_id);
        self.finish_file(file_id);
        self.clear_file_backlog(file_id);
        let cancellations = ready.into_values().filter_map(|ready| match ready {
            ReadyAdmission {
                permit: Ok(permit), ..
            } => Some(permit.cancel()),
            ReadyAdmission { permit: Err(_), .. } => None,
        });
        join_all(cancellations).await;
    }

    async fn cancel_all(&mut self) {
        let file_ids = self
            .pending_offsets
            .keys()
            .chain(self.backlog_leases.keys())
            .copied()
            .collect::<HashSet<_>>();
        for file_id in file_ids {
            self.cancel_file(file_id).await;
        }
    }

    fn start_acknowledgement(&mut self, file_id: FileFingerprint, start_offset: u64) {
        self.pending_files.insert(file_id);
        self.pending_replays.entry(file_id).or_insert(start_offset);
        let offsets = self.pending_offsets.entry(file_id).or_default();
        let acknowledging = self.acknowledging_files.entry(file_id).or_default();
        if offsets.get(*acknowledging).copied() != Some(start_offset) {
            debug_assert!(offsets.is_empty() && *acknowledging == 0);
            offsets.push_back(start_offset);
        }
        *acknowledging += 1;
    }

    fn finish_acknowledgement(
        &mut self,
        file_id: FileFingerprint,
        start_offset: u64,
        result: CheckpointGateResult,
    ) -> Option<u64> {
        if result == CheckpointGateResult::Rejected {
            self.finish_file(file_id);
            return None;
        }
        let acknowledging = self
            .acknowledging_files
            .get_mut(&file_id)
            .expect("acknowledged file has sent records");
        *acknowledging -= 1;
        let run_finished = *acknowledging == 0;
        if run_finished {
            self.acknowledging_files.remove(&file_id);
        }
        let offsets = self
            .pending_offsets
            .get_mut(&file_id)
            .expect("acknowledged file has a pending offset queue");
        let completed_offset = offsets.pop_front();
        debug_assert_eq!(completed_offset, Some(start_offset));
        if !run_finished {
            return None;
        }
        if self.refresh_after_acknowledgement.remove(&file_id) {
            let replay_offset = offsets.front().copied();
            if replay_offset.is_none() {
                self.finish_file(file_id);
            }
            return replay_offset;
        }
        if let Some(next_offset) = offsets.front().copied() {
            self.pending_replays.insert(file_id, next_offset);
            self.queue_ready_file(file_id);
        } else {
            self.finish_file(file_id);
        }
        None
    }
}

struct AbortTaskOnDrop<T>(Option<tokio::task::JoinHandle<T>>);

impl<T> AbortTaskOnDrop<T> {
    fn new(handle: tokio::task::JoinHandle<T>) -> Self {
        Self(Some(handle))
    }

    fn take(&mut self) -> tokio::task::JoinHandle<T> {
        self.0.take().expect("task handle taken once")
    }
}

impl<T> Drop for AbortTaskOnDrop<T> {
    fn drop(&mut self) {
        if let Some(handle) = &self.0 {
            handle.abort();
        }
    }
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
    /// Delivers one resolved admission through the source boundary. Both normal reads and
    /// shutdown drain use this protocol so send/acknowledgement failures always resolve the
    /// gate and restore the same replay offset.
    async fn dispatch_ready_admission<C>(
        &self,
        chans: &mut C,
        dispatcher: &mut AdmissionDispatcher,
        mut watcher: Option<&mut FileWatcher>,
        mut watchers: Option<&mut IndexMap<FileFingerprint, FileWatcher>>,
        ready: ReadyAdmission,
    ) -> Result<bool, C::Error>
    where
        C: Sink<Vec<Line>> + Unpin,
        C::Error: std::error::Error,
    {
        let file_id = ready.line.file_id;
        let start_offset = ready.line.start_offset;
        let permit = match ready.permit {
            Ok(permit) => permit,
            Err(error) => {
                warn!(
                    ?error,
                    ?file_id,
                    "Admission wait failed; preserving replay boundary"
                );
                let watcher = if let Some(watchers) = watchers.as_deref_mut() {
                    watchers.get_mut(&file_id)
                } else {
                    watcher.as_deref_mut()
                };
                if let Some(watcher) = watcher
                    && let Err(rewind_error) = watcher.rewind_to(start_offset).await
                {
                    self.emitter
                        .emit_file_watch_error(&watcher.path, rewind_error);
                    watcher.set_dead();
                }
                ready.gate.resolve(CheckpointGateResult::Rejected);
                dispatcher.cancel_file(file_id).await;
                return Ok(false);
            }
        };
        #[cfg(test)]
        if let Some(hook) = &self.test_before_send_hook {
            hook().await;
        }
        let mut send = Box::pin(chans.send(vec![ready.line]));
        let send_result = loop {
            if dispatcher.acknowledgements.is_empty() {
                break send.as_mut().await;
            }
            tokio::select! {
                result = send.as_mut() => break result,
                acknowledgement = dispatcher.acknowledgements.next() => {
                    let acknowledgement = acknowledgement
                        .expect("nonempty acknowledgement dispatcher returned no result");
                    let acknowledged_watcher = if let Some(watchers) = watchers.as_deref_mut() {
                        watchers.get_mut(&acknowledgement.file_id)
                    } else {
                        debug_assert_eq!(acknowledgement.file_id, file_id);
                        watcher.as_deref_mut()
                    };
                    self.finish_admission_acknowledgement(
                        dispatcher,
                        acknowledged_watcher,
                        acknowledgement,
                    )
                    .await;
                }
            }
        };
        if let Err(error) = send_result {
            let _ = permit.cancel().await;
            let watcher = if let Some(watchers) = watchers {
                watchers.get_mut(&file_id)
            } else {
                watcher
            };
            if let Some(watcher) = watcher
                && let Err(rewind_error) = watcher.rewind_to(start_offset).await
            {
                self.emitter
                    .emit_file_watch_error(&watcher.path, rewind_error);
                watcher.set_dead();
            }
            ready.gate.resolve(CheckpointGateResult::Rejected);
            dispatcher.cancel_file(file_id).await;
            return Err(error);
        }
        dispatcher.start_acknowledgement(file_id, start_offset);
        let acknowledgement = permit.sent();
        dispatcher.acknowledgements.push(
            async move {
                let result = if acknowledgement.await.is_ok() {
                    CheckpointGateResult::ActorAcknowledged
                } else {
                    CheckpointGateResult::Rejected
                };
                AcknowledgedAdmission {
                    file_id,
                    records: vec![AcknowledgedRecord {
                        start_offset,
                        gate: ready.gate,
                        result,
                    }],
                }
            }
            .boxed(),
        );
        Ok(true)
    }

    async fn dispatch_ready_admission_batch<C>(
        &self,
        chans: &mut C,
        dispatcher: &mut AdmissionDispatcher,
        watchers: &mut IndexMap<FileFingerprint, FileWatcher>,
        ready: Vec<ReadyAdmission>,
    ) -> Result<usize, C::Error>
    where
        C: Sink<Vec<Line>> + Unpin,
        C::Error: std::error::Error,
    {
        debug_assert!(!ready.is_empty());
        debug_assert!(ready.iter().all(|admission| {
            admission
                .permit
                .as_ref()
                .is_ok_and(|permit| permit.supports_batch())
        }));
        #[cfg(test)]
        if let Some(hook) = &self.test_before_send_hook {
            for _ in 0..ready.len() {
                hook().await;
            }
        }
        let mut lines = Vec::with_capacity(ready.len());
        let mut permits = Vec::with_capacity(ready.len());
        let mut metadata = Vec::with_capacity(ready.len());
        for ready in ready {
            let file_id = ready.line.file_id;
            let start_offset = ready.line.start_offset;
            let permit = ready
                .permit
                .expect("batch-ready admission must hold a permit");
            lines.push(ready.line);
            permits.push(permit);
            metadata.push((file_id, start_offset, ready.gate));
        }
        let admitted = metadata.len();
        let mut send = Box::pin(chans.send(lines));
        let send_result = loop {
            if dispatcher.acknowledgements.is_empty() {
                break send.as_mut().await;
            }
            tokio::select! {
                result = send.as_mut() => break result,
                acknowledgement = dispatcher.acknowledgements.next() => {
                    let acknowledgement = acknowledgement
                        .expect("nonempty acknowledgement dispatcher returned no result");
                    let watcher = watchers.get_mut(&acknowledgement.file_id);
                    self.finish_admission_acknowledgement(
                        dispatcher,
                        watcher,
                        acknowledgement,
                    )
                    .await;
                }
            }
        };
        if let Err(error) = send_result {
            let first = permits.remove(0);
            let _ = first.cancel_many(permits).await;
            // A file's records are in offset order: rewind to its first only, since a later
            // rewind would move the reader forward past the earlier unsent records.
            let mut rewound = HashSet::new();
            for (file_id, start_offset, gate) in metadata {
                if rewound.insert(file_id)
                    && let Some(watcher) = watchers.get_mut(&file_id)
                    && let Err(rewind_error) = watcher.rewind_to(start_offset).await
                {
                    self.emitter
                        .emit_file_watch_error(&watcher.path, rewind_error);
                    watcher.set_dead();
                }
                gate.resolve(CheckpointGateResult::Rejected);
                dispatcher.cancel_file(file_id).await;
            }
            return Err(error);
        }

        for (file_id, start_offset, _) in &metadata {
            dispatcher.start_acknowledgement(*file_id, *start_offset);
        }
        let first = permits.remove(0);
        let acknowledgements = first.sent_many(permits);
        // A file's run is contiguous here, and its records must finish in offset order.
        let mut runs: Vec<(FileFingerprint, Vec<(u64, CheckpointGateResolver)>)> = Vec::new();
        for (file_id, start_offset, gate) in metadata {
            match runs.last_mut() {
                Some((run_file, records)) if *run_file == file_id => {
                    records.push((start_offset, gate))
                }
                _ => runs.push((file_id, vec![(start_offset, gate)])),
            }
        }
        let mut result_senders = Vec::with_capacity(runs.len());
        for (file_id, records) in runs {
            let (send, receive) =
                tokio::sync::oneshot::channel::<Vec<Result<(), AdmissionError>>>();
            result_senders.push((send, records.len()));
            dispatcher.acknowledgements.push(
                async move {
                    let results = receive.await.unwrap_or_default();
                    let records = records
                        .into_iter()
                        .enumerate()
                        .map(|(index, (start_offset, gate))| AcknowledgedRecord {
                            start_offset,
                            gate,
                            result: match results.get(index) {
                                Some(Ok(())) => CheckpointGateResult::ActorAcknowledged,
                                Some(Err(_)) | None => CheckpointGateResult::Rejected,
                            },
                        })
                        .collect();
                    AcknowledgedAdmission { file_id, records }
                }
                .boxed(),
            );
        }
        tokio::spawn(async move {
            let mut results = acknowledgements.await.into_iter();
            for (send, len) in result_senders {
                let _ = send.send(results.by_ref().take(len).collect());
            }
        });
        Ok(admitted)
    }

    async fn finish_admission_acknowledgement(
        &self,
        dispatcher: &mut AdmissionDispatcher,
        mut watcher: Option<&mut FileWatcher>,
        acknowledgement: AcknowledgedAdmission,
    ) {
        let file_id = acknowledgement.file_id;
        let mut records = acknowledgement.records.into_iter();
        while let Some(record) = records.next() {
            if record.result == CheckpointGateResult::Rejected {
                // Later records in the run were already sent; replaying from the first rejection
                // keeps the checkpoint from passing it, at the cost of resending those records.
                if let Some(watcher) = watcher.as_deref_mut()
                    && let Err(error) = watcher.rewind_to(record.start_offset).await
                {
                    self.emitter.emit_file_watch_error(&watcher.path, error);
                    watcher.set_dead();
                }
                record.gate.resolve(CheckpointGateResult::Rejected);
                for later in records {
                    later.gate.resolve(CheckpointGateResult::Rejected);
                }
                dispatcher.cancel_file(file_id).await;
                return;
            }
            record.gate.resolve(record.result);
            if let Some(replay_offset) =
                dispatcher.finish_acknowledgement(file_id, record.start_offset, record.result)
            {
                if let Some(watcher) = watcher.as_deref_mut()
                    && let Err(error) = watcher.rewind_to(replay_offset).await
                {
                    self.emitter.emit_file_watch_error(&watcher.path, error);
                    watcher.set_dead();
                }
                dispatcher.cancel_file(file_id).await;
            }
        }
    }

    /// Reserves capacity before touching the reader, then constructs the one line boundary used
    /// by both the steady-state loop and shutdown drain. A failed reconciliation always restores
    /// the exact pre-read offset before the caller can consider a later record from this file.
    fn admission_metadata_for(
        &self,
        watcher: &FileWatcher,
    ) -> Option<crate::admission::AdmissionMetadata> {
        // Clone only the metadata: this runs for every record and the whole `LogFileInfo` holds
        // several owned strings.
        let admission = self.admission.as_ref()?;
        let metadata = self.file_to_pod_map.as_ref().and_then(|map| {
            map.lock()
                .ok()?
                .get(&watcher.path)?
                .admission_metadata
                .clone()
        });
        Some(metadata.unwrap_or_else(|| admission.unattributed_metadata()))
    }

    fn admission_metadata_from_file_info(
        &self,
        file_info: Option<&LogFileInfo>,
    ) -> Option<crate::admission::AdmissionMetadata> {
        self.admission.as_ref().map(|admission| {
            file_info
                .and_then(|info| info.admission_metadata.clone())
                .unwrap_or_else(|| admission.unattributed_metadata())
        })
    }

    async fn refresh_pending_file_metadata(
        &self,
        dispatcher: &mut AdmissionDispatcher,
        file_id: FileFingerprint,
        watcher: &mut FileWatcher,
        current_metadata: Option<crate::admission::AdmissionMetadata>,
        replacement_metadata: Option<crate::admission::AdmissionMetadata>,
    ) {
        if current_metadata == replacement_metadata {
            return;
        }
        if let Some(replay_offset) = dispatcher.refresh_file_metadata(file_id).await
            && let Err(error) = watcher.rewind_to(replay_offset).await
        {
            self.emitter.emit_file_watch_error(&watcher.path, error);
            watcher.set_dead();
        }
    }

    async fn prepare_line(
        &self,
        watcher: &mut FileWatcher,
        file_id: FileFingerprint,
        dispatcher: &mut AdmissionDispatcher,
    ) -> PrepareLineResult {
        let admission_metadata = self.admission_metadata_for(watcher);
        let reservation = if let (Some(admission), Some(metadata)) =
            (&self.admission, admission_metadata.clone())
        {
            match dispatcher
                .take_or_reserve_file_credit(
                    file_id,
                    Arc::clone(admission),
                    metadata,
                    self.max_line_bytes as u64,
                )
                .await
            {
                Ok(reservation) => Some(reservation),
                Err(error) => {
                    warn!(
                        ?error,
                        ?file_id,
                        "Deferring file after admission reservation failure"
                    );
                    return PrepareLineResult::AdmissionDeferred;
                }
            }
        } else {
            None
        };

        self.prepare_line_with_reservation(
            watcher,
            file_id,
            dispatcher,
            admission_metadata,
            reservation,
        )
        .await
    }

    async fn prepare_line_with_reservation(
        &self,
        watcher: &mut FileWatcher,
        file_id: FileFingerprint,
        dispatcher: &mut AdmissionDispatcher,
        admission_metadata: Option<crate::admission::AdmissionMetadata>,
        mut reservation: Option<Box<dyn crate::admission::PendingAdmission>>,
    ) -> PrepareLineResult {
        let raw = match watcher.read_line().await {
            Ok(RawLineResult {
                raw_line: Some(line),
                discarded_for_size_and_truncated,
            }) => {
                self.emit_line_too_long_events(&watcher.path, &discarded_for_size_and_truncated);
                line
            }
            Ok(_) => {
                dispatcher.clear_file_backlog(file_id);
                return PrepareLineResult::NoLine;
            }
            Err(error) => {
                dispatcher.clear_file_backlog(file_id);
                self.emitter.emit_file_watch_error(&watcher.path, error);
                return PrepareLineResult::ReadError;
            }
        };
        let reconciliation = match reservation.as_mut() {
            Some(reservation) => {
                let actual_payload_bytes = raw.bytes.len() as u64;
                match reservation.try_reconcile_payload_bytes(actual_payload_bytes) {
                    Some(result) => result,
                    None => {
                        reservation
                            .reconcile_payload_bytes(actual_payload_bytes)
                            .await
                    }
                }
            }
            None => Ok(()),
        };
        if let Err(error) = reconciliation {
            warn!(
                ?error,
                ?file_id,
                "Deferring file after admission reconciliation failure"
            );
            if let Err(rewind_error) = watcher.rewind_to(raw.offset).await {
                self.emitter
                    .emit_file_watch_error(&watcher.path, rewind_error);
                watcher.set_dead();
            }
            return PrepareLineResult::AdmissionDeferred;
        }
        if let (Some(admission), Some(metadata)) = (&self.admission, admission_metadata.clone())
            && let Err(error) = dispatcher
                .retain_file_backlog(file_id, Arc::clone(admission), metadata)
                .await
        {
            warn!(
                ?error,
                ?file_id,
                "Deferring file after backlog retention failure"
            );
            if let Err(rewind_error) = watcher.rewind_to(raw.offset).await {
                self.emitter
                    .emit_file_watch_error(&watcher.path, rewind_error);
                watcher.set_dead();
            }
            return PrepareLineResult::AdmissionDeferred;
        }

        let payload_bytes = raw.bytes.len();
        let (checkpoint_gate, gate) = if reservation.is_some() {
            let (gate, resolver) = CheckpointGate::closed();
            (gate, Some(resolver))
        } else {
            (CheckpointGate::Open, None)
        };
        PrepareLineResult::Line(PreparedLine {
            line: Line {
                text: raw.bytes,
                filename: watcher.path.to_str().expect("not a valid path").to_owned(),
                file_id,
                start_offset: raw.offset,
                end_offset: watcher.get_file_position(),
                admission_key: admission_metadata.map(|metadata| metadata.key),
                checkpoint_gate,
            },
            payload_bytes,
            pending: reservation.map(|reservation| {
                (
                    reservation,
                    gate.expect("admitted line has a closed checkpoint gate"),
                )
            }),
        })
    }

    fn emit_line_too_long_events(&self, path: &Path, discarded: &[BytesMut]) {
        // Called for every line read; nearly all have nothing discarded, and the delivery event
        // below ignores a zero count after the costly label lookup.
        if discarded.is_empty() {
            return;
        }
        for buf in discarded {
            self.emitter
                .emit_file_line_too_long(buf, self.max_line_bytes, buf.len());
        }

        let file_info = self.file_to_pod_map.as_ref().and_then(|file_to_pod_map| {
            file_to_pod_map
                .lock()
                .expect("file-to-pod map lock poisoned")
                .get(path)
                .cloned()
        });
        let service_system = self
            .source_context
            .as_ref()
            .and_then(|context| context.get("system"))
            .filter(|system| !system.is_empty())
            .cloned()
            .or_else(|| {
                file_info
                    .as_ref()
                    .and_then(|info| info.service_system.clone())
            });
        let source_pod_id = file_info
            .as_ref()
            .and_then(|info| info.source_pod_id.clone())
            .or_else(|| {
                path.to_str()
                    .and_then(source_pod_id_from_file_path)
                    .map(str::to_string)
            });

        delivery_singleton().emit_rejected(
            path.to_str().expect("not a valid path"),
            discarded.len(),
            &self.source_context,
            service_system.as_deref(),
            source_pod_id.as_deref(),
            self.source_type,
            current_hour_time_parity_ms_value(),
            REJECTION_REASON_LINE_TOO_LONG,
        );
    }

    /// Returns `true` if the path has an extension that matches one of the
    /// configured archive extensions (immutable files that never change).
    fn is_archive(&self, path: &Path) -> bool {
        path.extension().is_some_and(|ext| {
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
        // When the last discovery pass finished. Holding the setter for the whole run keeps the
        // gauge from expiring while this server is stuck in a pass, a drain, or a blocked send, so
        // its staleness stays visible. Set now so the startup scan counts from the start of `run`.
        let set_last_discovery_pass = self.emitter.discovery_pass_timestamp();
        set_last_discovery_pass(unix_now_seconds());

        let mut fp_map: IndexMap<FileFingerprint, FileWatcher> = Default::default();

        let mut backoff_cap: usize = 1;
        let mut lines = Vec::new();
        let reservation_tranche_size = self
            .admission
            .as_ref()
            .map_or(ADMISSION_READ_AHEAD_PER_FILE, |admission| {
                admission.reservation_tranche_size()
            });
        let mut admission_dispatcher = AdmissionDispatcher::new(reservation_tranche_size);

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

        #[cfg(test)]
        let checkpointer = match &self.test_checkpoint_flush_hook {
            Some(hook) => {
                checkpointer.with_write_hook(Arc::clone(hook) as Arc<dyn CheckpointWriteHook>)
            }
            None => checkpointer,
        };

        // Spawn the checkpoint writer task
        let mut checkpoint_task_handle = AbortTaskOnDrop::new(tokio::spawn(checkpoint_writer(
            checkpointer,
            self.glob_minimum_cooldown,
            shutdown_checkpointer,
            self.emitter.clone(),
        )));

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
        let mut next_admission_scan_file = None;
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
                let start = stats.start();
                for watcher in fp_map.values_mut() {
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
                    // `stat` is the metadata the fingerprinter already looked up; it is `None`
                    // when an archive's fingerprint came from the checkpoint cache.
                    let (file_id, stat) = if !self.ignore_checkpoints && self.is_archive(&path) {
                        match checkpoints.get_archive_fingerprint(&path) {
                            Some(id) => (Some(id), None),
                            None => {
                                let found = self
                                    .fingerprinter
                                    .fingerprint_with_stat_or_emit(
                                        &path,
                                        &mut known_small_files,
                                        &self.emitter,
                                    )
                                    .await;
                                if let Some((id, _)) = found {
                                    checkpoints.set_archive_path(id, &path);
                                }
                                found.unzip()
                            }
                        }
                    } else {
                        self.fingerprinter
                            .fingerprint_with_stat_or_emit(
                                &path,
                                &mut known_small_files,
                                &self.emitter,
                            )
                            .await
                            .unzip()
                    };

                    if let Some(file_id) = file_id {
                        if let Some(watcher) = fp_map.get_mut(&file_id) {
                            // file fingerprint matches a watched file
                            let was_found_this_cycle = watcher.file_findable();
                            watcher.set_file_findable(true);
                            let current_metadata = self.admission_metadata_for(watcher);
                            let replacement_metadata = log_file_info_opt.as_ref().map_or_else(
                                || current_metadata.clone(),
                                |info| self.admission_metadata_from_file_info(Some(info)),
                            );
                            if watcher.path == path {
                                trace!(
                                    message = "Continue watching file.",
                                    path = ?path,
                                );
                                watcher.refresh_last_seen_stat(stat);
                                self.refresh_pending_file_metadata(
                                    &mut admission_dispatcher,
                                    file_id,
                                    watcher,
                                    current_metadata,
                                    replacement_metadata,
                                )
                                .await;
                                self.update_file_to_pod_map(path, log_file_info_opt);
                            } else if !was_found_this_cycle {
                                // matches a file with a different path
                                info!(
                                    message = "Watched file has been renamed.",
                                    path = ?path,
                                    old_path = ?watcher.path
                                );
                                let old_path = watcher.path.clone();
                                if watcher.update_path(path.clone()).await.is_ok() {
                                    watcher.refresh_last_seen_stat(stat);
                                    self.refresh_pending_file_metadata(
                                        &mut admission_dispatcher,
                                        file_id,
                                        watcher,
                                        current_metadata,
                                        replacement_metadata,
                                    )
                                    .await;
                                    admission_dispatcher.clear_file_backlog(file_id);
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
                                        watcher.refresh_last_seen_stat(stat);
                                        self.refresh_pending_file_metadata(
                                            &mut admission_dispatcher,
                                            file_id,
                                            watcher,
                                            current_metadata,
                                            replacement_metadata,
                                        )
                                        .await;
                                        admission_dispatcher.clear_file_backlog(file_id);
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
                stats.record(TimingSegment::Discovery, start);

                // This pass has just refreshed each found file's stat.
                let now_ms = Utc::now().timestamp_millis();
                let max_unread_age = fp_map
                    .values()
                    .filter_map(|watcher| watcher.unread_age(now_ms))
                    .max()
                    .unwrap_or_default();
                self.emitter.emit_backlog_max_age(max_unread_age);

                let pass_end = time::Instant::now();
                set_last_discovery_pass(unix_now_seconds());
                let scheduled = next_glob_time;
                next_glob_time = next_discovery_time(
                    now_time,
                    pass_end,
                    next_glob_time,
                    self.glob_minimum_cooldown,
                );
                // With a zero cooldown the floor is the pass end itself, which is no rest at all.
                if !self.glob_minimum_cooldown.is_zero() && next_glob_time > scheduled {
                    self.emitter.emit_discovery_pass_delayed();
                    debug!(
                        message = "Slow discovery pass; delaying the next one.",
                        pass_duration_ms = pass_end.duration_since(now_time).as_millis() as u64,
                        extra_delay_ms =
                            next_glob_time.duration_since(scheduled).as_millis() as u64,
                    );
                }
            }

            const DELETE_KNOWN_SMALL_FILES: bool = false;
            // Cleanup the known_small_files
            if DELETE_KNOWN_SMALL_FILES && let Some(grace_period) = self.remove_after {
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
            let scan_start = next_admission_scan_file
                .and_then(|file_id| fp_map.get_index_of(&file_id))
                .unwrap_or_default();
            let scan_file_count = fp_map.len();
            let mut last_reserved_scan_index = None;
            for scan_offset in 0..scan_file_count {
                let scan_index = (scan_start + scan_offset) % scan_file_count;
                let (&file_id, watcher) = fp_map
                    .get_index_mut(scan_index)
                    .expect("file scan index must remain valid during one pass");
                #[cfg(test)]
                if self
                    .test_readiness_hook
                    .as_ref()
                    .is_some_and(|hook| !hook(&watcher.path))
                {
                    continue;
                }

                // A failed rewind marks the watcher dead after this pass's reap. Its reader still
                // sits past the rejected record, so reading it again would let later records
                // advance the checkpoint over that record before the next reap.
                if watcher.dead() {
                    continue;
                }

                if self.admission.is_some()
                    && !admission_dispatcher.file_has_read_ahead_capacity(file_id)
                {
                    continue;
                }

                // The current file contents were already read ahead to EOF. Wait for their
                // ordered dispositions before probing EOF again; non-EOF files keep filling the
                // bounded admission queue so same-file backlog remains visible.
                if watcher.reached_eof() && admission_dispatcher.pending_files.contains(&file_id) {
                    continue;
                }

                // Presence in fp_map means the file exists on disk; keep its
                // checkpoint live (clear death mark, refresh `modified`) even if
                // we don't read it this cycle.
                checkpoints.clear_dead(file_id);

                if !watcher.should_read() {
                    if watcher.reached_eof() {
                        admission_dispatcher.clear_file_backlog(file_id);
                    }
                    continue;
                }

                // Skip reading gzip files already fully read, but still fall through
                // to the removal logic below so TTL cleanup can proceed.
                let is_done = !self.ignore_checkpoints && checkpoints.get_done(file_id);

                let start = stats.start();
                let mut bytes_read: usize = 0;
                let mut lines_read: usize = 0;
                // `is_done` is fixed for this visit; the loop exits through its breaks.
                loop {
                    if is_done {
                        break;
                    }
                    let prepared = self
                        .prepare_line(watcher, file_id, &mut admission_dispatcher)
                        .await;
                    let PreparedLine {
                        line,
                        payload_bytes: sz,
                        pending,
                    } = match prepared {
                        PrepareLineResult::Line(line) => line,
                        PrepareLineResult::NoLine
                        | PrepareLineResult::ReadError
                        | PrepareLineResult::AdmissionDeferred => break,
                    };
                    trace!(
                        message = "Read bytes.",
                        path = ?watcher.path,
                        bytes = ?sz
                    );
                    stats.record_bytes(sz);
                    bytes_read += sz;
                    lines_read += 1;
                    #[cfg(test)]
                    let line = match &self.test_line_admission_hook {
                        Some(hook) => match hook(line) {
                            Some(admitted_line) => admitted_line,
                            None => {
                                maxed_out_reading_single_file = true;
                                break;
                            }
                        },
                        None => line,
                    };
                    if let Some((reservation, gate)) = pending {
                        let immediate = admission_dispatcher.enqueue(line, reservation, gate);
                        last_reserved_scan_index = Some(scan_index);
                        // Keep reading this file only while permits cost no waiting.
                        if !immediate
                            || lines_read >= ADMISSION_VISIT_LINE_BUDGET
                            || !admission_dispatcher.file_has_read_ahead_capacity(file_id)
                        {
                            break;
                        }
                        continue;
                    }
                    lines.push(line);

                    if bytes_read > self.max_read_bytes {
                        maxed_out_reading_single_file = true;
                        break;
                    }
                }
                stats.record(TimingSegment::Reading, start);
                if !is_done && watcher.take_read_started() {
                    let info = FileReadStartedInfo {
                        fingerprint: file_id,
                        lines_read: watcher.lines_read(),
                        bytes_read: watcher.bytes_read(),
                        stat: watcher.last_seen_stat(),
                    };
                    self.emitter.emit_file_read_started(&watcher.path, &info);
                }
                if lines_read > 0 {
                    // Pre-multiline read spot, gated off by default
                    // (EMIT_READ_EVENT_AFTER_MULTILINE_AGG). The counter fires
                    // inline; only the VEL `info!` log is batched by the
                    // process-global delivery singleton. `source_context` is
                    // borrowed and only cloned on first sight of this path.
                    let path = watcher.path.to_str().expect("not a valid path");
                    let source_pod_id = source_pod_id_from_file_path(path);
                    delivery_singleton().accumulate_read(
                        path.to_owned(),
                        bytes_read,
                        lines_read,
                        &self.source_context,
                        None,
                        source_pod_id,
                        self.source_type,
                        vector_common::internal_event::vector_event::delivery_event::current_hour_time_parity_ms_value(),
                        false,
                        None,
                        None,
                    );
                }
                if watcher.reached_eof()
                    && self.is_archive(&watcher.path)
                    && (self.admission.is_none()
                        || checkpoints
                            .get(file_id)
                            .is_some_and(|offset| offset >= watcher.get_file_position()))
                {
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
            if let Some(last_reserved_scan_index) = last_reserved_scan_index {
                // Resume after the last file that claimed a process slot. A static restart lets
                // earlier files reclaim every freed slot before later files are visited.
                next_admission_scan_file = fp_map
                    .get_index((last_reserved_scan_index + 1) % scan_file_count)
                    .map(|(file_id, _)| *file_id);
            }

            // Drop done-archive watchers: emit unwatched metric and remove directly
            // from fp_map without calling checkpoints.set_dead so that the done/
            // archive_paths entries survive until remove_after actually deletes the file.
            for (fng, path) in done_archives {
                // shift_remove, not swap_remove: swap_remove moves the newest watcher (the hot active file) into the
                // removed slot, which puts it first in the read order. With `oldest_first` the loop stops after the first
                // file that exhausts its read budget, so a hot file at the front starves every older rotated segment
                // until the kubelet deletes it unread.
                admission_dispatcher.clear_file_backlog(fng);
                if let Some(watcher) = fp_map.shift_remove(&fng) {
                    let info = unwatched_info(fng, &watcher, FileStopReason::ArchiveDone);
                    self.emitter.emit_file_unwatched(&path, &info);
                }
            }

            for (file_id, watcher) in &mut fp_map {
                // Reap a vanished archive immediately (terminal, no successor);
                // give a non-archive the `rotate_wait` grace so a live watcher
                // can hand its position to the rotated `.gz` via `update_path`.
                let expired = if self.is_archive(&watcher.path) {
                    !watcher.file_findable()
                } else {
                    !watcher.file_findable() && watcher.last_seen().elapsed() > self.rotate_wait
                };
                if expired && !admission_dispatcher.pending_files.contains(file_id) {
                    watcher.set_dead();
                }
            }

            // A FileWatcher is dead when the underlying file has disappeared.
            // If the FileWatcher is dead we don't retain it; it will be deallocated.
            fp_map.retain(|file_id, watcher| {
                if watcher.dead() {
                    admission_dispatcher.clear_file_backlog(*file_id);
                    let info = unwatched_info(*file_id, watcher, FileStopReason::FileGone);
                    self.emitter.emit_file_unwatched(&watcher.path, &info);
                    checkpoints.set_dead(*file_id);
                    false
                } else {
                    true
                }
            });
            self.emitter.emit_files_open(fp_map.len());

            let start = stats.start();
            let mut admission_progress = 0;
            let mut batchable = Vec::new();
            for ready in admission_dispatcher.next_ready_batch() {
                if ready
                    .permit
                    .as_ref()
                    .is_ok_and(|permit| permit.supports_batch())
                {
                    batchable.push(ready);
                    continue;
                }
                if !batchable.is_empty() {
                    admission_progress += self
                        .dispatch_ready_admission_batch(
                            &mut chans,
                            &mut admission_dispatcher,
                            &mut fp_map,
                            std::mem::take(&mut batchable),
                        )
                        .await?;
                }
                admission_progress += usize::from(
                    self.dispatch_ready_admission(
                        &mut chans,
                        &mut admission_dispatcher,
                        None,
                        Some(&mut fp_map),
                        ready,
                    )
                    .await?,
                );
            }
            if !batchable.is_empty() {
                admission_progress += self
                    .dispatch_ready_admission_batch(
                        &mut chans,
                        &mut admission_dispatcher,
                        &mut fp_map,
                        batchable,
                    )
                    .await?;
            }
            while let Some(acknowledgement) = admission_dispatcher.next_acknowledged() {
                let watcher = fp_map.get_mut(&acknowledgement.file_id);
                self.finish_admission_acknowledgement(
                    &mut admission_dispatcher,
                    watcher,
                    acknowledgement,
                )
                .await;
                admission_progress += 1;
            }
            let to_send = std::mem::take(&mut lines);

            if !to_send.is_empty() {
                #[cfg(test)]
                if self.admission.is_none()
                    && let Some(hook) = &self.test_before_send_hook
                {
                    hook().await;
                }

                if let Err(error) = chans.send(to_send).await {
                    error!(message = "Output channel closed.", %error);
                    return Err(error);
                }
            }
            stats.record(TimingSegment::Sending, start);

            let start = stats.start();
            // Admission sends and acknowledgements are active source progress even when a
            // bounded read-ahead tranche cannot read another line in the same iteration.
            let progress = global_bytes_read.saturating_add(admission_progress);
            backoff_cap = if progress == 0 {
                cmp::min(self.read_loop_max_backoff, backoff_cap.saturating_mul(2))
            } else {
                cmp::min(self.read_loop_min_backoff, self.read_loop_max_backoff)
            };
            let backoff = Duration::from_millis(backoff_cap.saturating_sub(progress) as u64);

            // This works only if run inside tokio context since we are using
            // tokio's Timer. Outside of such context, this will panic on the first
            // call. Also since we are using block_on here and in the above code,
            // this should be run in its own thread. `spawn_blocking` fulfills
            // all of these requirements.
            //
            // An admitted file cannot send its next record until the admission actor acknowledges
            // the previous one, which it does only after this loop yields. Waking for that
            // acknowledgement, or for a granted wait, keeps the backoff from costing every record.
            let dispatcher = &mut admission_dispatcher;
            let wait = Box::pin(async move {
                if backoff.is_zero() {
                    return None;
                }
                if !dispatcher.has_outstanding_work() {
                    sleep(backoff).await;
                    return None;
                }
                tokio::select! {
                    acknowledgement = dispatcher.wait_for_progress() => acknowledgement,
                    _ = sleep(backoff) => None,
                }
            });
            // Bind the outcome first so the wait's dispatcher borrow ends before either branch.
            //
            // The shutdown output must stay alive until `run` returns: dropping it tells the
            // topology this source has finished, before the final checkpoint write.
            let (woke, _shutdown_token) = match select(&mut shutdown_data, wait).await {
                Either::Left((token, _)) => (None, Some(token)),
                Either::Right((acknowledgement, _)) => (Some(acknowledgement), None),
            };
            match woke {
                None => {
                    if self.drain_on_shutdown {
                        // A line already crossed the source channel, so its actor disposition must
                        // finish before shutdown can decide whether to checkpoint or replay it.
                        while let Some(acknowledgement) =
                            admission_dispatcher.acknowledgements.next().await
                        {
                            let watcher = fp_map.get_mut(&acknowledgement.file_id);
                            self.finish_admission_acknowledgement(
                                &mut admission_dispatcher,
                                watcher,
                                acknowledgement,
                            )
                            .await;
                        }
                        // A pending wait has advanced its reader but has not crossed the source
                        // channel. Drop its reservation and restore the exact pre-read offset
                        // before drain considers any later record from that file.
                        let pending_replays =
                            std::mem::take(&mut admission_dispatcher.pending_replays);
                        admission_dispatcher.cancel_all().await;
                        drop(admission_dispatcher);
                        for (file_id, start_offset) in pending_replays {
                            let Some(watcher) = fp_map.get_mut(&file_id) else {
                                warn!(
                                    ?file_id,
                                    "Cannot restore pending admission: watcher disappeared"
                                );
                                continue;
                            };
                            if let Err(error) = watcher.rewind_to(start_offset).await {
                                self.emitter.emit_file_watch_error(&watcher.path, error);
                                watcher.set_dead();
                            }
                        }
                        info!(message = "Shutdown signal received, draining files before exit.");

                        // The drain deliberately never touches checkpoints. A checkpoint may only
                        // move past a line once downstream has taken it (the source's map closure
                        // does that as it pulls events), and a send here can still fail or be
                        // dropped at the reload deadline. The final `write_checkpoints` below
                        // then persists exactly what the consumer has taken.

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
                            if watcher.dead() {
                                continue;
                            }
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
                        let mut drain_admission_dispatcher =
                            AdmissionDispatcher::new(reservation_tranche_size);
                        while !drain_targets.is_empty() {
                            let mut progress = false;

                            while let Some(acknowledgement) =
                                drain_admission_dispatcher.next_acknowledged()
                            {
                                let watcher = fp_map.get_mut(&acknowledgement.file_id);
                                self.finish_admission_acknowledgement(
                                    &mut drain_admission_dispatcher,
                                    watcher,
                                    acknowledgement,
                                )
                                .await;
                                progress = true;
                            }

                            for file_id in drain_targets.keys().copied().collect::<Vec<_>>() {
                                let Some(&target) = drain_targets.get(&file_id) else {
                                    continue;
                                };
                                if drain_admission_dispatcher.pending_files.contains(&file_id) {
                                    continue;
                                }
                                let Some(watcher) = fp_map.get_mut(&file_id) else {
                                    drain_admission_dispatcher.clear_file_backlog(file_id);
                                    drain_targets.swap_remove(&file_id);
                                    progress = true;
                                    continue;
                                };
                                // An acknowledgement handled above may have failed a rewind.
                                if watcher.dead() {
                                    drain_admission_dispatcher.clear_file_backlog(file_id);
                                    drain_targets.swap_remove(&file_id);
                                    progress = true;
                                    continue;
                                }

                                let mut bytes_read: usize = 0;
                                while watcher.get_checkpoint_position() < target {
                                    match self
                                        .prepare_line(
                                            watcher,
                                            file_id,
                                            &mut drain_admission_dispatcher,
                                        )
                                        .await
                                    {
                                        PrepareLineResult::Line(PreparedLine {
                                            line,
                                            payload_bytes: line_bytes,
                                            pending,
                                        }) => {
                                            if let Some((reservation, gate)) = pending {
                                                let start_offset = line.start_offset;
                                                let mut drain_dispatcher = AdmissionDispatcher::new(
                                                    reservation_tranche_size,
                                                );
                                                drain_dispatcher.enqueue(line, reservation, gate);
                                                let Some(ready) = drain_dispatcher.next_ready()
                                                else {
                                                    debug!(
                                                        ?file_id,
                                                        "Deferring drain while admission is unavailable"
                                                    );
                                                    if let Err(error) =
                                                        watcher.rewind_to(start_offset).await
                                                    {
                                                        self.emitter.emit_file_watch_error(
                                                            &watcher.path,
                                                            error,
                                                        );
                                                        watcher.set_dead();
                                                    }
                                                    drain_targets.swap_remove(&file_id);
                                                    progress = true;
                                                    break;
                                                };
                                                let delivered = match self
                                                    .dispatch_ready_admission(
                                                        &mut chans,
                                                        &mut drain_admission_dispatcher,
                                                        Some(watcher),
                                                        None,
                                                        ready,
                                                    )
                                                    .await
                                                {
                                                    Ok(delivered) => delivered,
                                                    Err(error) => {
                                                        error!(message = "Output channel closed during admitted drain.", %error);
                                                        drain_targets.clear();
                                                        break;
                                                    }
                                                };
                                                if !delivered {
                                                    drain_targets.swap_remove(&file_id);
                                                    progress = true;
                                                    break;
                                                }
                                                progress = true;
                                                break;
                                            } else {
                                                lines.push(line);
                                            }
                                            bytes_read += line_bytes;
                                            if bytes_read > self.max_read_bytes {
                                                break;
                                            }
                                        }
                                        PrepareLineResult::NoLine => {
                                            // EOF on this watcher's open file. If the watcher is still
                                            // findable, its internal buffer may hold an unterminated
                                            // partial line that `read_line()` hasn't yet emitted
                                            // (it only emits buffered partials when `!file_findable()`).
                                            // Mark it not findable and re-read to flush the buffered
                                            // partial as a normal line before completing the drain.
                                            if watcher.file_findable() {
                                                watcher.set_file_findable(false);
                                                continue;
                                            }
                                            // Now !file_findable(), so any buffered partial has been
                                            // flushed or the buffer is empty. Everything on disk at
                                            // snapshot time has been read, so the drain is done even
                                            // if `target` was not reached: for archives the target is
                                            // the `u64::MAX` sentinel, and for a rotated-away watcher
                                            // `target` is the size of a different file now at the same
                                            // path, which this handle can never reach.
                                            drain_targets.swap_remove(&file_id);
                                            progress = true;
                                            break;
                                        }
                                        PrepareLineResult::ReadError
                                        | PrepareLineResult::AdmissionDeferred => {
                                            drain_admission_dispatcher.clear_file_backlog(file_id);
                                            drain_targets.swap_remove(&file_id);
                                            progress = true;
                                            break;
                                        }
                                    }
                                }
                                progress |= bytes_read > 0;

                                // The file reached its drain target.
                                if drain_targets.contains_key(&file_id)
                                    && watcher.get_checkpoint_position() >= target
                                    && !drain_admission_dispatcher.pending_files.contains(&file_id)
                                {
                                    drain_admission_dispatcher.clear_file_backlog(file_id);
                                    drain_targets.swap_remove(&file_id);
                                    progress = true;
                                }
                            }

                            let to_send = std::mem::take(&mut lines);
                            if !to_send.is_empty()
                                && let Err(error) = chans.send(to_send).await
                            {
                                error!(message = "Output channel closed during drain.", %error);
                                break;
                            }

                            if !progress {
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                        }

                        // A file only leaves the drain with acknowledgements outstanding after an
                        // error. Its sent records hold closed gates that only the dispatcher can
                        // resolve, and the event loop holding them must finish before the final
                        // checkpoint write (see `shutdown_checkpointer`), so reject them for replay.
                        drain_admission_dispatcher.cancel_all().await;
                        drop(drain_admission_dispatcher);

                        info!(message = "Drain complete, shutting down file server.");
                    } else {
                        // Lines already sent hold closed gates that only the dispatcher can resolve.
                        // The event loop holding them must finish before the final checkpoint write,
                        // so release them as rejected: they are replayed from the checkpoint.
                        admission_dispatcher.cancel_all().await;
                        drop(admission_dispatcher);
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
                    let checkpointer = match checkpoint_task_handle.take().await {
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
                Some(acknowledgement) => {
                    if let Some(acknowledgement) = acknowledgement {
                        let watcher = fp_map.get_mut(&acknowledgement.file_id);
                        self.finish_admission_acknowledgement(
                            &mut admission_dispatcher,
                            watcher,
                            acknowledgement,
                        )
                        .await;
                    }
                }
            }
            stats.record(TimingSegment::Sleeping, start);
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
            self.idle_read_interval,
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

fn unwatched_info(
    fingerprint: FileFingerprint,
    watcher: &FileWatcher,
    reason: FileStopReason,
) -> FileUnwatchedInfo {
    FileUnwatchedInfo {
        fingerprint,
        reason,
        reached_eof: watcher.reached_eof(),
        // Excludes bytes buffered for an unterminated line, matching the checkpoint.
        end_offset: watcher.get_checkpoint_position(),
        lines_read: watcher.lines_read(),
        bytes_read: watcher.bytes_read(),
        stat: watcher.last_seen_stat(),
    }
}

async fn checkpoint_writer(
    checkpointer: Checkpointer,
    sleep_duration: Duration,
    mut shutdown: impl Future + Unpin,
    emitter: impl FileSourceInternalEvents,
) -> Arc<Checkpointer> {
    let checkpointer = Arc::new(checkpointer);
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

/// Loop segments timed for the debug-level throughput report.
#[derive(Clone, Copy)]
enum TimingSegment {
    Discovery,
    Reading,
    Sending,
    Sleeping,
}

impl TimingSegment {
    const COUNT: usize = 4;

    const fn name(self) -> &'static str {
        match self {
            Self::Discovery => "discovery",
            Self::Reading => "reading",
            Self::Sending => "sending",
            Self::Sleeping => "sleeping",
        }
    }
}

/// Timings are only reported at debug level, so the clock is only read while that level is
/// enabled (re-checked whenever the stats window restarts).
struct TimingStats {
    started_at: time::Instant,
    enabled: bool,
    segments: [Option<Duration>; TimingSegment::COUNT],
    events: usize,
    bytes: usize,
}

impl TimingStats {
    /// Starts timing one segment; `None` when timings are not being collected.
    #[inline]
    fn start(&self) -> Option<time::Instant> {
        self.enabled.then(time::Instant::now)
    }

    #[inline]
    fn record(&mut self, segment: TimingSegment, start: Option<time::Instant>) {
        if let Some(start) = start {
            *self.segments[segment as usize].get_or_insert_default() += start.elapsed();
        }
    }

    fn record_bytes(&mut self, bytes: usize) {
        self.events += 1;
        self.bytes += bytes;
    }

    fn report(&self) {
        if !self.enabled {
            return;
        }
        let total = self.started_at.elapsed();
        let counted: Duration = self.segments.iter().flatten().sum();
        let other: Duration = total.saturating_sub(counted);
        let mut ratios = [
            TimingSegment::Discovery,
            TimingSegment::Reading,
            TimingSegment::Sending,
            TimingSegment::Sleeping,
        ]
        .into_iter()
        .filter_map(|segment| {
            self.segments[segment as usize]
                .map(|duration| (segment.name(), duration.as_secs_f32() / total.as_secs_f32()))
        })
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
            enabled: tracing::level_enabled!(tracing::Level::DEBUG),
            segments: Default::default(),
            events: Default::default(),
            bytes: Default::default(),
        }
    }
}

/// Upper bound on the rest after a slow discovery pass, as a multiple of `glob_minimum_cooldown`.
const DISCOVERY_MAX_REST_COOLDOWNS: u32 = 3;

/// When the discovery pass that ran from `pass_start` to `pass_end` may next run.
///
/// `scheduled` is the usual `pass_start + cooldown`. Once a pass takes longer than the cooldown,
/// that time has already passed and, without a floor, discovery would run back-to-back. Sources
/// run their passes on separate blocking threads, so they contend with one another, get slower,
/// and stay back-to-back after the load that triggered them goes away. The floor rests the source
/// for as long as the pass took (capped at `DISCOVERY_MAX_REST_COOLDOWNS × cooldown`), keeping it
/// busy at most about half the time so the contention can clear.
///
/// While `pass_duration ≤ cooldown / 2` the floor is not later than `scheduled`, so normal passes
/// keep their schedule exactly. Under overload a new file is found at most
/// `pass_duration + 3 × cooldown` after the previous pass ended, plus the read-loop backoff. A
/// cooldown of 0 disables the floor.
fn next_discovery_time(
    pass_start: time::Instant,
    pass_end: time::Instant,
    scheduled: time::Instant,
    cooldown: Duration,
) -> time::Instant {
    let rest = pass_end
        .saturating_duration_since(pass_start)
        .min(cooldown.saturating_mul(DISCOVERY_MAX_REST_COOLDOWNS));
    match pass_end.checked_add(rest) {
        Some(floor) => scheduled.max(floor),
        None => scheduled,
    }
}

fn unix_now_seconds() -> f64 {
    Utc::now().timestamp_millis() as f64 / 1000.0
}

#[derive(Debug)]
pub struct Line {
    pub text: Bytes,
    pub filename: String,
    pub file_id: FileFingerprint,
    pub start_offset: u64,
    pub end_offset: u64,
    /// Immutable admission identity used for the pre-read reservation, when enabled.
    pub admission_key: Option<AdmissionKey>,
    /// Closed until an enforcing permit is actor-acknowledged after channel send.
    pub checkpoint_gate: CheckpointGate,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        io::Error,
        path::{Path, PathBuf},
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        },
        time::{self, Duration},
    };

    use bytes::{Bytes, BytesMut};
    use file_source_common::{
        Checkpointer, FileReadStartedInfo, FileSourceInternalEvents, FileUnwatchedInfo,
        FingerprintStrategy, Fingerprinter, ReadFrom,
    };
    use futures::{
        StreamExt,
        channel::mpsc,
        future::{BoxFuture, FutureExt},
    };
    use tempfile::tempdir;
    use tokio::{
        fs,
        sync::{OwnedSemaphorePermit, Semaphore},
    };

    use crate::{
        FileTTLAction, FileTTLRemovalConfig,
        admission::{
            AdmissionError, AdmissionKey, AdmissionMetadata, CheckpointGate, CheckpointGateResult,
            FileSourceAdmission, FileSourceAdmissionPermit, OwnedFileSourceAdmissionPermit,
            PendingAdmission,
        },
        file_server::{
            ADMISSION_IN_FLIGHT_PER_FILE, ADMISSION_READ_AHEAD_PER_FILE,
            ADMISSION_VISIT_LINE_BUDGET, AdmissionDispatcher, FileServer, Line, ReadyAdmission,
            TestCheckpointFlushHook, next_discovery_time,
        },
        paths_provider::{LogFileInfo, PathsProvider},
    };

    // No-op FileSourceInternalEvents implementation for testing.
    // Error events panic so tests fail fast on unexpected errors.
    #[derive(Clone)]
    struct NoErrors;

    impl FileSourceInternalEvents for NoErrors {
        fn emit_file_added(&self, _: &Path) {}
        fn emit_file_resumed(&self, _: &Path, _: u64) {}
        fn emit_file_watch_error(&self, _: &Path, error: Error) {
            // A refused rewind (the path now holds a different file) is an expected, reported
            // outcome that tests exercise on purpose.
            assert!(
                error
                    .to_string()
                    .contains("cannot rewind to a rejected record"),
                "unexpected file watch error: {error}"
            );
        }
        fn emit_file_read_started(&self, _: &Path, _: &FileReadStartedInfo) {}
        fn emit_file_unwatched(&self, _: &Path, _: &FileUnwatchedInfo) {}
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
        fn emit_file_checkpoint_write_error(&self, error: Error) {
            panic!("unexpected checkpoint write error: {error:?}");
        }
        fn emit_files_open(&self, _: usize) {}
        fn emit_path_globbing_failed(&self, _: &Path, _: &Error) {
            panic!("unexpected path globbing failure");
        }
        fn emit_discovery_pass_delayed(&self) {}
        fn discovery_pass_timestamp(&self) -> Box<dyn Fn(f64) + Send + Sync> {
            Box::new(|_| {})
        }
        fn emit_backlog_max_age(&self, _: Duration) {}
        fn emit_file_line_too_long(&self, _: &BytesMut, _: usize, _: usize) {
            panic!("unexpected line too long");
        }
    }

    #[derive(Clone)]
    struct SmallFileEvents {
        failures: tokio::sync::mpsc::UnboundedSender<PathBuf>,
    }

    impl FileSourceInternalEvents for SmallFileEvents {
        fn emit_file_added(&self, path: &Path) {
            NoErrors.emit_file_added(path);
        }
        fn emit_file_resumed(&self, path: &Path, position: u64) {
            NoErrors.emit_file_resumed(path, position);
        }
        fn emit_file_watch_error(&self, path: &Path, error: Error) {
            NoErrors.emit_file_watch_error(path, error);
        }
        fn emit_file_read_started(&self, path: &Path, info: &FileReadStartedInfo) {
            NoErrors.emit_file_read_started(path, info);
        }
        fn emit_file_unwatched(&self, path: &Path, info: &FileUnwatchedInfo) {
            NoErrors.emit_file_unwatched(path, info);
        }
        fn emit_file_deleted(&self, path: &Path) {
            NoErrors.emit_file_deleted(path);
        }
        fn emit_file_delete_error(&self, path: &Path, error: Error) {
            NoErrors.emit_file_delete_error(path, error);
        }
        fn emit_file_fingerprint_read_error(&self, path: &Path, error: Error) {
            NoErrors.emit_file_fingerprint_read_error(path, error);
        }
        fn emit_file_checkpointed(&self, count: usize, duration: Duration) {
            NoErrors.emit_file_checkpointed(count, duration);
        }
        fn emit_file_checksum_failed(&self, path: &Path) {
            self.failures.send(path.to_path_buf()).unwrap();
        }
        fn emit_file_checkpoint_write_error(&self, error: Error) {
            NoErrors.emit_file_checkpoint_write_error(error);
        }
        fn emit_files_open(&self, count: usize) {
            NoErrors.emit_files_open(count);
        }
        fn emit_path_globbing_failed(&self, path: &Path, error: &Error) {
            NoErrors.emit_path_globbing_failed(path, error);
        }
        fn emit_discovery_pass_delayed(&self) {
            NoErrors.emit_discovery_pass_delayed();
        }
        fn discovery_pass_timestamp(&self) -> Box<dyn Fn(f64) + Send + Sync> {
            NoErrors.discovery_pass_timestamp()
        }
        fn emit_backlog_max_age(&self, age: Duration) {
            NoErrors.emit_backlog_max_age(age);
        }
        fn emit_file_line_too_long(&self, bytes: &BytesMut, limit: usize, size: usize) {
            NoErrors.emit_file_line_too_long(bytes, limit, size);
        }
    }

    // Records the discovery events; everything else behaves like `NoErrors`.
    #[derive(Clone, Default)]
    struct DiscoveryEvents {
        delayed: Arc<Mutex<usize>>,
        pass_timestamps: Arc<Mutex<Vec<f64>>>,
    }

    impl FileSourceInternalEvents for DiscoveryEvents {
        fn emit_file_added(&self, path: &Path) {
            NoErrors.emit_file_added(path);
        }
        fn emit_file_resumed(&self, path: &Path, position: u64) {
            NoErrors.emit_file_resumed(path, position);
        }
        fn emit_file_watch_error(&self, path: &Path, error: Error) {
            NoErrors.emit_file_watch_error(path, error);
        }
        fn emit_file_read_started(&self, path: &Path, info: &FileReadStartedInfo) {
            NoErrors.emit_file_read_started(path, info);
        }
        fn emit_file_unwatched(&self, path: &Path, info: &FileUnwatchedInfo) {
            NoErrors.emit_file_unwatched(path, info);
        }
        fn emit_file_deleted(&self, path: &Path) {
            NoErrors.emit_file_deleted(path);
        }
        fn emit_file_delete_error(&self, path: &Path, error: Error) {
            NoErrors.emit_file_delete_error(path, error);
        }
        fn emit_file_fingerprint_read_error(&self, path: &Path, error: Error) {
            NoErrors.emit_file_fingerprint_read_error(path, error);
        }
        fn emit_file_checkpointed(&self, count: usize, duration: Duration) {
            NoErrors.emit_file_checkpointed(count, duration);
        }
        fn emit_file_checksum_failed(&self, path: &Path) {
            NoErrors.emit_file_checksum_failed(path);
        }
        fn emit_file_checkpoint_write_error(&self, error: Error) {
            NoErrors.emit_file_checkpoint_write_error(error);
        }
        fn emit_files_open(&self, count: usize) {
            NoErrors.emit_files_open(count);
        }
        fn emit_path_globbing_failed(&self, path: &Path, error: &Error) {
            NoErrors.emit_path_globbing_failed(path, error);
        }
        fn emit_discovery_pass_delayed(&self) {
            *self.delayed.lock().unwrap() += 1;
        }
        fn discovery_pass_timestamp(&self) -> Box<dyn Fn(f64) + Send + Sync> {
            let pass_timestamps = Arc::clone(&self.pass_timestamps);
            Box::new(move |timestamp| pass_timestamps.lock().unwrap().push(timestamp))
        }
        fn emit_backlog_max_age(&self, age: Duration) {
            NoErrors.emit_backlog_max_age(age);
        }
        fn emit_file_line_too_long(&self, bytes: &BytesMut, limit: usize, size: usize) {
            NoErrors.emit_file_line_too_long(bytes, limit, size);
        }
    }

    // Records the per-pass backlog age reports; everything else behaves like `NoErrors`.
    #[derive(Clone, Default)]
    struct BacklogEvents {
        ages: Arc<Mutex<Vec<Duration>>>,
    }

    impl FileSourceInternalEvents for BacklogEvents {
        fn emit_file_added(&self, path: &Path) {
            NoErrors.emit_file_added(path);
        }
        fn emit_file_resumed(&self, path: &Path, position: u64) {
            NoErrors.emit_file_resumed(path, position);
        }
        fn emit_file_watch_error(&self, path: &Path, error: Error) {
            NoErrors.emit_file_watch_error(path, error);
        }
        fn emit_file_read_started(&self, path: &Path, info: &FileReadStartedInfo) {
            NoErrors.emit_file_read_started(path, info);
        }
        fn emit_file_unwatched(&self, path: &Path, info: &FileUnwatchedInfo) {
            NoErrors.emit_file_unwatched(path, info);
        }
        fn emit_file_deleted(&self, path: &Path) {
            NoErrors.emit_file_deleted(path);
        }
        fn emit_file_delete_error(&self, path: &Path, error: Error) {
            NoErrors.emit_file_delete_error(path, error);
        }
        fn emit_file_fingerprint_read_error(&self, path: &Path, error: Error) {
            NoErrors.emit_file_fingerprint_read_error(path, error);
        }
        fn emit_file_checkpointed(&self, count: usize, duration: Duration) {
            NoErrors.emit_file_checkpointed(count, duration);
        }
        fn emit_file_checksum_failed(&self, path: &Path) {
            NoErrors.emit_file_checksum_failed(path);
        }
        fn emit_file_checkpoint_write_error(&self, error: Error) {
            NoErrors.emit_file_checkpoint_write_error(error);
        }
        fn emit_files_open(&self, count: usize) {
            NoErrors.emit_files_open(count);
        }
        fn emit_path_globbing_failed(&self, path: &Path, error: &Error) {
            NoErrors.emit_path_globbing_failed(path, error);
        }
        fn emit_discovery_pass_delayed(&self) {
            NoErrors.emit_discovery_pass_delayed();
        }
        fn discovery_pass_timestamp(&self) -> Box<dyn Fn(f64) + Send + Sync> {
            NoErrors.discovery_pass_timestamp()
        }
        fn emit_backlog_max_age(&self, age: Duration) {
            self.ages.lock().unwrap().push(age);
        }
        fn emit_file_line_too_long(&self, bytes: &BytesMut, limit: usize, size: usize) {
            NoErrors.emit_file_line_too_long(bytes, limit, size);
        }
    }

    /// Records READ_STARTED and STOPPED (unwatched) events; everything else behaves like
    /// `NoErrors`.
    #[derive(Clone, Default)]
    struct LifecycleEvents {
        read_started: Arc<Mutex<Vec<(PathBuf, FileReadStartedInfo)>>>,
        unwatched: Arc<Mutex<Vec<(PathBuf, FileUnwatchedInfo)>>>,
    }

    impl FileSourceInternalEvents for LifecycleEvents {
        fn emit_file_added(&self, path: &Path) {
            NoErrors.emit_file_added(path);
        }
        fn emit_file_resumed(&self, path: &Path, position: u64) {
            NoErrors.emit_file_resumed(path, position);
        }
        fn emit_file_watch_error(&self, path: &Path, error: Error) {
            NoErrors.emit_file_watch_error(path, error);
        }
        fn emit_file_read_started(&self, path: &Path, info: &FileReadStartedInfo) {
            self.read_started
                .lock()
                .unwrap()
                .push((path.to_path_buf(), *info));
        }
        fn emit_file_unwatched(&self, path: &Path, info: &FileUnwatchedInfo) {
            self.unwatched
                .lock()
                .unwrap()
                .push((path.to_path_buf(), *info));
        }
        fn emit_file_deleted(&self, path: &Path) {
            NoErrors.emit_file_deleted(path);
        }
        fn emit_file_delete_error(&self, path: &Path, error: Error) {
            NoErrors.emit_file_delete_error(path, error);
        }
        fn emit_file_fingerprint_read_error(&self, path: &Path, error: Error) {
            NoErrors.emit_file_fingerprint_read_error(path, error);
        }
        fn emit_file_checkpointed(&self, count: usize, duration: Duration) {
            NoErrors.emit_file_checkpointed(count, duration);
        }
        fn emit_file_checksum_failed(&self, path: &Path) {
            NoErrors.emit_file_checksum_failed(path);
        }
        fn emit_file_checkpoint_write_error(&self, error: Error) {
            NoErrors.emit_file_checkpoint_write_error(error);
        }
        fn emit_files_open(&self, count: usize) {
            NoErrors.emit_files_open(count);
        }
        fn emit_path_globbing_failed(&self, path: &Path, error: &Error) {
            NoErrors.emit_path_globbing_failed(path, error);
        }
        fn emit_discovery_pass_delayed(&self) {
            NoErrors.emit_discovery_pass_delayed();
        }
        fn discovery_pass_timestamp(&self) -> Box<dyn Fn(f64) + Send + Sync> {
            NoErrors.discovery_pass_timestamp()
        }
        fn emit_backlog_max_age(&self, age: Duration) {
            NoErrors.emit_backlog_max_age(age);
        }
        fn emit_file_line_too_long(&self, bytes: &BytesMut, limit: usize, size: usize) {
            NoErrors.emit_file_line_too_long(bytes, limit, size);
        }
    }

    /// Runs `file_server` while `actions` runs alongside it, then shuts the server down.
    async fn run_file_server_during<P, E, F>(
        file_server: FileServer<P, E>,
        data_dir: &Path,
        actions: F,
    ) where
        P: PathsProvider,
        E: FileSourceInternalEvents,
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let (tx, rx) = mpsc::channel::<Vec<Line>>(8);
        let collector = tokio::spawn(rx.collect::<Vec<_>>());
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let shutdown_data: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
            Box::pin(async move {
                let _ = shutdown_rx.await;
            });
        let driver = tokio::spawn(async move {
            actions.await;
            let _ = shutdown_tx.send(());
        });

        file_server
            .run(
                tx,
                shutdown_data,
                futures::future::ready(()),
                Checkpointer::new(data_dir),
            )
            .await
            .unwrap();
        driver.await.unwrap();
        collector.await.unwrap();
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

    #[test]
    fn next_discovery_time_keeps_schedule_for_normal_passes() {
        let cooldown = Duration::from_secs(10);
        let start = time::Instant::now();
        let scheduled = start + cooldown;
        for pass_ms in [0, 1, 50, 1_000, 4_999, 5_000] {
            let end = start + Duration::from_millis(pass_ms);
            assert_eq!(
                next_discovery_time(start, end, scheduled, cooldown),
                scheduled,
                "pass of {pass_ms}ms"
            );
        }
        // Passes of 1 ms against the 10 ms cooldown the existing tests use.
        let cooldown = Duration::from_millis(10);
        let end = start + Duration::from_millis(1);
        assert_eq!(
            next_discovery_time(start, end, start + cooldown, cooldown),
            start + cooldown
        );
        // A zero cooldown keeps passing on every loop iteration.
        let end = start + Duration::from_secs(5);
        assert_eq!(next_discovery_time(start, end, start, Duration::ZERO), end);
    }

    #[test]
    fn next_discovery_time_rests_after_slow_passes_up_to_the_cap() {
        let cooldown = Duration::from_secs(10);
        let start = time::Instant::now();
        let scheduled = start + cooldown;
        // Just over half the cooldown: rest for the pass's duration.
        let end = start + Duration::from_millis(5_001);
        assert_eq!(
            next_discovery_time(start, end, scheduled, cooldown),
            end + Duration::from_millis(5_001)
        );
        // Longer than the cooldown: no longer back-to-back.
        let end = start + Duration::from_secs(25);
        assert_eq!(
            next_discovery_time(start, end, scheduled, cooldown),
            end + Duration::from_secs(25)
        );
        // Rest is capped at 3 cooldowns.
        let end = start + Duration::from_secs(120);
        assert_eq!(
            next_discovery_time(start, end, scheduled, cooldown),
            end + Duration::from_secs(30)
        );
        // An end before the start (not possible with a monotonic clock) adds no rest.
        assert_eq!(
            next_discovery_time(end, start, scheduled, cooldown),
            scheduled
        );
        // Huge cooldowns saturate instead of overflowing.
        assert_eq!(
            next_discovery_time(start, end, scheduled, Duration::MAX),
            end + Duration::from_secs(120)
        );
    }

    // PathsProvider that takes `delay` per call and records when each call ran.
    struct SlowPathsProvider {
        delay: Duration,
        calls: Arc<Mutex<Vec<(time::Instant, time::Instant)>>>,
    }

    impl PathsProvider for SlowPathsProvider {
        type IntoIter = Vec<(Option<crate::paths_provider::LogFileInfo>, PathBuf)>;

        fn paths(&self) -> Self::IntoIter {
            let start = time::Instant::now();
            std::thread::sleep(self.delay);
            self.calls
                .lock()
                .unwrap()
                .push((start, time::Instant::now()));
            Vec::new()
        }
    }

    async fn discovery_passes(
        delay: Duration,
        cooldown: Duration,
        run_for: Duration,
        emitter: impl FileSourceInternalEvents,
    ) -> Vec<(time::Instant, time::Instant)> {
        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).await.unwrap();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let base = make_file_server(Vec::new(), data_dir.clone(), false);
        let file_server = FileServer {
            paths_provider: SlowPathsProvider {
                delay,
                calls: Arc::clone(&calls),
            },
            max_read_bytes: base.max_read_bytes,
            ignore_checkpoints: base.ignore_checkpoints,
            read_from: base.read_from,
            ignore_before: base.ignore_before,
            start_reading_at: base.start_reading_at,
            max_line_bytes: base.max_line_bytes,
            line_delimiter: base.line_delimiter,
            data_dir: base.data_dir,
            glob_minimum_cooldown: cooldown,
            idle_read_interval: base.idle_read_interval,
            fingerprinter: base.fingerprinter,
            oldest_first: base.oldest_first,
            remove_after: base.remove_after,
            emitter,
            rotate_wait: base.rotate_wait,
            ttl_removal_config: base.ttl_removal_config,
            source_context: base.source_context,
            file_to_pod_map: base.file_to_pod_map,
            drain_on_shutdown: base.drain_on_shutdown,
            archive_extensions: base.archive_extensions,
            source_type: base.source_type,
            read_loop_min_backoff: base.read_loop_min_backoff,
            read_loop_max_backoff: base.read_loop_max_backoff,
            admission: None,
            test_readiness_hook: None,
            test_line_admission_hook: None,
            test_before_send_hook: None,
            test_checkpoint_flush_hook: None,
        };
        let (tx, _rx) = mpsc::channel::<Vec<Line>>(16);
        let shutdown = Box::pin(tokio::time::sleep(run_for));
        let checkpointer = Checkpointer::new(&data_dir);
        file_server
            .run(tx, shutdown, futures::future::ready(()), checkpointer)
            .await
            .unwrap();
        // The first call is the startup scan, which the loop's first pass follows immediately.
        calls.lock().unwrap().iter().skip(1).copied().collect()
    }

    #[tokio::test]
    async fn fast_discovery_passes_run_once_per_cooldown() {
        let cooldown = Duration::from_millis(100);
        let calls = discovery_passes(
            Duration::ZERO,
            cooldown,
            Duration::from_millis(1_000),
            NoErrors,
        )
        .await;
        assert!(calls.len() >= 3, "only {} passes", calls.len());
        for pair in calls.windows(2) {
            assert!(pair[1].0.duration_since(pair[0].0) >= cooldown);
        }
    }

    #[tokio::test]
    async fn slow_discovery_passes_do_not_run_back_to_back() {
        // Each pass takes longer than the cooldown. Before the rest floor, the next pass started
        // one read-loop backoff (1, 2, 4, ... ms) after the previous one ended.
        let delay = Duration::from_millis(150);
        let cooldown = Duration::from_millis(100);
        let calls = discovery_passes(delay, cooldown, Duration::from_millis(1_500), NoErrors).await;
        assert!(calls.len() >= 3, "only {} passes", calls.len());
        for pair in calls.windows(2) {
            let (prev_start, prev_end) = pair[0];
            let rest = pair[1].0.duration_since(prev_end);
            assert!(
                rest >= prev_end.duration_since(prev_start),
                "rested {rest:?} after a {:?} pass",
                prev_end.duration_since(prev_start)
            );
        }
    }

    #[tokio::test]
    async fn fast_discovery_passes_report_nothing() {
        let events = DiscoveryEvents::default();
        let calls = discovery_passes(
            Duration::ZERO,
            Duration::from_millis(100),
            Duration::from_millis(500),
            events.clone(),
        )
        .await;
        assert!(calls.len() >= 3, "only {} passes", calls.len());
        assert_eq!(*events.delayed.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn slow_discovery_passes_are_counted_as_delayed() {
        let events = DiscoveryEvents::default();
        let calls = discovery_passes(
            Duration::from_millis(150),
            Duration::from_millis(100),
            Duration::from_millis(1_000),
            events.clone(),
        )
        .await;
        assert!(calls.len() >= 2, "only {} passes", calls.len());
        assert_eq!(*events.delayed.lock().unwrap(), calls.len());
    }

    #[tokio::test]
    async fn zero_cooldown_passes_are_not_counted_as_delayed() {
        let events = DiscoveryEvents::default();
        let calls = discovery_passes(
            Duration::from_millis(5),
            Duration::ZERO,
            Duration::from_millis(200),
            events.clone(),
        )
        .await;
        assert!(calls.len() >= 2, "only {} passes", calls.len());
        assert_eq!(*events.delayed.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn discovery_pass_timestamp_is_set_at_start_and_after_each_pass() {
        let before = super::unix_now_seconds();
        let events = DiscoveryEvents::default();
        let calls = discovery_passes(
            Duration::ZERO,
            Duration::from_millis(100),
            Duration::from_millis(500),
            events.clone(),
        )
        .await;
        let after = super::unix_now_seconds();

        let timestamps = events.pass_timestamps.lock().unwrap().clone();
        // Once at the start of `run`, then once per pass.
        assert_eq!(timestamps.len(), calls.len() + 1, "{timestamps:?}");
        assert!(
            timestamps.windows(2).all(|w| w[0] <= w[1]),
            "{timestamps:?}"
        );
        assert!(timestamps[0] >= before && *timestamps.last().unwrap() <= after);
    }

    #[tokio::test]
    async fn discovery_pass_timestamp_handle_lives_as_long_as_the_file_server() {
        let events = DiscoveryEvents::default();
        let probe = Arc::clone(&events.pass_timestamps);
        let running = tokio::spawn(discovery_passes(
            Duration::ZERO,
            Duration::from_millis(100),
            Duration::from_millis(300),
            events.clone(),
        ));
        tokio::time::sleep(Duration::from_millis(150)).await;
        // `events`, `probe`, the server's emitter, and the setter the server holds.
        assert_eq!(Arc::strong_count(&probe), 4);
        running.await.unwrap();
        // The setter goes when `run` returns, so a removed source's gauge can expire.
        assert_eq!(Arc::strong_count(&probe), 2);
    }

    #[tokio::test]
    async fn backlog_reports_a_stale_file_until_it_is_read() {
        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).await.unwrap();
        let log_path = tmp.path().join("a.log");
        fs::write(&log_path, "line\n".repeat(10)).await.unwrap();
        // Written ten minutes ago and not read yet.
        std::fs::File::options()
            .write(true)
            .open(&log_path)
            .unwrap()
            .set_modified(std::time::SystemTime::now() - Duration::from_secs(600))
            .unwrap();

        let events = BacklogEvents::default();
        let file_server =
            make_file_server_with_emitter(vec![log_path], data_dir.clone(), false, events.clone());
        let (tx, rx) = mpsc::channel::<Vec<Line>>(16);
        let collector = tokio::spawn(rx.collect::<Vec<_>>());
        let shutdown = Box::pin(tokio::time::sleep(Duration::from_millis(500)));
        file_server
            .run(
                tx,
                shutdown,
                futures::future::ready(()),
                Checkpointer::new(&data_dir),
            )
            .await
            .unwrap();
        collector.await.unwrap();

        // The first pass runs before the first read, so the file's unread bytes are ten minutes
        // old; later passes see it caught up.
        let ages = events.ages.lock().unwrap().clone();
        assert!(ages.len() >= 2, "{ages:?}");
        let first = ages[0];
        assert!(
            first >= Duration::from_secs(600) && first < Duration::from_secs(660),
            "{ages:?}"
        );
        assert_eq!(ages.last(), Some(&Duration::ZERO), "{ages:?}");
    }

    #[tokio::test]
    async fn backlog_reports_an_unfinished_gzipped_file_until_it_is_read() {
        use std::io::Write;

        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).await.unwrap();
        // A rotated, compressed hourly file, written ten minutes ago and not read yet.
        let gz_path = tmp.path().join("a.log.gz");
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all("line\n".repeat(10).as_bytes()).unwrap();
        fs::write(&gz_path, encoder.finish().unwrap())
            .await
            .unwrap();
        std::fs::File::options()
            .write(true)
            .open(&gz_path)
            .unwrap()
            .set_modified(std::time::SystemTime::now() - Duration::from_secs(600))
            .unwrap();

        let events = BacklogEvents::default();
        let file_server =
            make_file_server_with_emitter(vec![gz_path], data_dir.clone(), false, events.clone());
        let (tx, rx) = mpsc::channel::<Vec<Line>>(16);
        let collector = tokio::spawn(rx.collect::<Vec<_>>());
        let shutdown = Box::pin(tokio::time::sleep(Duration::from_millis(500)));
        file_server
            .run(
                tx,
                shutdown,
                futures::future::ready(()),
                Checkpointer::new(&data_dir),
            )
            .await
            .unwrap();
        let lines: usize = collector.await.unwrap().iter().map(Vec::len).sum();
        assert_eq!(lines, 10);

        // Unread on the first pass; once read to the end it is done and no longer counted.
        let ages = events.ages.lock().unwrap().clone();
        assert!(ages.len() >= 2, "{ages:?}");
        let first = ages[0];
        assert!(
            first >= Duration::from_secs(600) && first < Duration::from_secs(660),
            "{ages:?}"
        );
        assert_eq!(ages.last(), Some(&Duration::ZERO), "{ages:?}");
    }

    fn make_file_server(
        paths: Vec<PathBuf>,
        data_dir: PathBuf,
        drain_on_shutdown: bool,
    ) -> FileServer<TestPathsProvider, NoErrors> {
        make_file_server_with_emitter(paths, data_dir, drain_on_shutdown, NoErrors)
    }

    fn make_file_server_with_emitter<E: FileSourceInternalEvents>(
        paths: Vec<PathBuf>,
        data_dir: PathBuf,
        drain_on_shutdown: bool,
        emitter: E,
    ) -> FileServer<TestPathsProvider, E> {
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
            idle_read_interval: Duration::from_secs(10),
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
            emitter,
            rotate_wait: Duration::from_secs(u64::MAX / 2),
            ttl_removal_config: None,
            source_context: None,
            file_to_pod_map: None,
            drain_on_shutdown,
            archive_extensions: vec!["gz".to_string()],
            source_type: "file",
            read_loop_min_backoff: 1,
            read_loop_max_backoff: 2_048,
            admission: None,
            test_readiness_hook: None,
            test_line_admission_hook: None,
            test_before_send_hook: None,
            test_checkpoint_flush_hook: None,
        }
    }

    #[tokio::test]
    async fn capped_backoff_reads_idle_appends_in_order_and_shuts_down() {
        use tokio::{io::AsyncWriteExt, sync::oneshot};

        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).await.unwrap();
        let path = tmp.path().join("active.log");
        fs::write(&path, b"warmup\n").await.unwrap();
        let mut file_server = make_file_server(vec![path.clone()], data_dir.clone(), false);
        file_server.read_loop_max_backoff = 50;
        file_server.idle_read_interval = Duration::from_millis(50);

        let (tx, mut rx) = mpsc::channel::<Vec<Line>>(8);
        let (lines_tx, mut lines_rx) = tokio::sync::mpsc::unbounded_channel();
        let collector = tokio::spawn(async move {
            while let Some(batch) = rx.next().await {
                for line in batch {
                    lines_tx.send(line.text).unwrap();
                }
            }
        });
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let server = tokio::spawn(file_server.run(
            tx,
            Box::pin(async move {
                let _ = shutdown_rx.await;
            }),
            futures::future::ready(()),
            Checkpointer::new(&data_dir),
        ));
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), lines_rx.recv())
                .await
                .unwrap(),
            Some(Bytes::from("warmup")),
        );
        // Let both exponential backoffs mature. Without the cap an append at this phase
        // waits for the remainder of a 2,048 ms sleep.
        tokio::time::sleep(Duration::from_millis(4_300)).await;
        let mut writer = fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .await
            .unwrap();
        let expected: Vec<_> = (0..20)
            .map(|i| Bytes::from(format!("record-{i}")))
            .collect();
        for line in &expected {
            writer.write_all(line).await.unwrap();
            writer.write_all(b"\n").await.unwrap();
        }
        writer.flush().await.unwrap();
        let received = tokio::time::timeout(Duration::from_millis(600), async {
            let mut received = Vec::new();
            for _ in &expected {
                received.push(lines_rx.recv().await.unwrap());
            }
            received
        })
        .await
        .expect("idle append exceeded the EOF retry plus polling budget");
        assert_eq!(received, expected);
        shutdown_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        collector.await.unwrap();
        assert_eq!(
            lines_rx.recv().await,
            None,
            "duplicate records after shutdown"
        );
    }

    async fn assert_small_files_survive_ttl(ttl_removal_config: Option<FileTTLRemovalConfig>) {
        use tokio::{io::AsyncWriteExt, sync::oneshot};

        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).await.unwrap();

        let empty_path = tmp.path().join("audit.json");
        let partial_path = tmp.path().join("partial.json");
        let rotated_path = tmp.path().join("rotated.log");
        let mut empty_writer = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&empty_path)
            .await
            .unwrap();
        let mut partial_writer = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&partial_path)
            .await
            .unwrap();
        partial_writer.write_all(b"partial audit").await.unwrap();
        partial_writer.flush().await.unwrap();
        fs::write(&rotated_path, b"rotated record\n").await.unwrap();

        let (failures_tx, mut failures_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut file_server = make_file_server_with_emitter(
            vec![
                empty_path.clone(),
                partial_path.clone(),
                rotated_path.clone(),
            ],
            data_dir.clone(),
            false,
            SmallFileEvents {
                failures: failures_tx,
            },
        );
        file_server.remove_after = Some(Duration::from_millis(50));
        file_server.glob_minimum_cooldown = Duration::from_millis(10);
        file_server.ttl_removal_config = ttl_removal_config;

        let (tx, mut rx) = mpsc::channel::<Vec<Line>>(8);
        let (delivered_tx, mut delivered_rx) = tokio::sync::mpsc::unbounded_channel();
        let collector = tokio::spawn(async move {
            let mut received = Vec::new();
            while let Some(batch) = rx.next().await {
                for line in &batch {
                    delivered_tx
                        .send((line.filename.clone(), line.text.clone()))
                        .unwrap();
                }
                received.extend(batch);
            }
            received
        });
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let checkpointer = Checkpointer::new(data_dir.as_path());
        let server = tokio::spawn(file_server.run(
            tx,
            Box::pin(async move {
                let _ = shutdown_rx.await;
            }),
            futures::future::ready(()),
            checkpointer,
        ));

        let mut failed_paths = Vec::new();
        for _ in 0..2 {
            failed_paths.push(
                tokio::time::timeout(Duration::from_secs(5), failures_rx.recv())
                    .await
                    .expect("small file was not discovered")
                    .expect("file server exited before discovering small files"),
            );
        }
        assert!(failed_paths.contains(&empty_path));
        assert!(failed_paths.contains(&partial_path));

        let rotated_line = tokio::time::timeout(Duration::from_secs(5), delivered_rx.recv())
            .await
            .expect("watched file was not read")
            .expect("file server exited before reading the watched file");
        assert_eq!(
            rotated_line,
            (
                rotated_path.to_str().unwrap().to_owned(),
                Bytes::from("rotated record"),
            )
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(empty_path.exists(), "empty file was TTL-deleted");
        assert_eq!(fs::read(&partial_path).await.unwrap(), b"partial audit");
        assert!(
            !rotated_path.exists(),
            "watched file should still be TTL-deleted"
        );

        empty_writer
            .write_all(b"late audit record\n")
            .await
            .unwrap();
        empty_writer.flush().await.unwrap();
        partial_writer.write_all(b" record\n").await.unwrap();
        partial_writer.flush().await.unwrap();

        let mut appended_lines = Vec::new();
        for _ in 0..2 {
            appended_lines.push(
                tokio::time::timeout(Duration::from_secs(5), delivered_rx.recv())
                    .await
                    .expect("late append was not delivered")
                    .expect("file server exited before delivering the late append"),
            );
        }
        appended_lines.sort();
        assert_eq!(
            appended_lines,
            vec![
                (
                    empty_path.to_str().unwrap().to_owned(),
                    Bytes::from("late audit record"),
                ),
                (
                    partial_path.to_str().unwrap().to_owned(),
                    Bytes::from("partial audit record"),
                ),
            ]
        );
        shutdown_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("file server did not shut down")
            .unwrap()
            .unwrap();
        let received = collector.await.unwrap();
        let mut texts: Vec<_> = received.iter().map(|line| line.text.clone()).collect();
        texts.sort();
        assert_eq!(
            texts,
            vec![
                Bytes::from("late audit record"),
                Bytes::from("partial audit record"),
                Bytes::from("rotated record"),
            ]
        );
    }

    #[tokio::test]
    async fn small_files_survive_ttl_without_removal_config() {
        assert_small_files_survive_ttl(None).await;
    }

    #[tokio::test]
    async fn small_files_survive_ttl_with_keep_patterns() {
        assert_small_files_survive_ttl(Some(FileTTLRemovalConfig {
            action: FileTTLAction::Keep,
            patterns: vec![
                glob::Pattern::new("*/audit.json").unwrap(),
                glob::Pattern::new("*/partial.json").unwrap(),
            ],
        }))
        .await;
    }

    fn test_log_file_info() -> crate::paths_provider::LogFileInfo {
        crate::paths_provider::LogFileInfo {
            pod_namespace: "dbr".to_string(),
            pod_name: "driver-pod".to_string(),
            pod_uid: "pod-uid".to_string(),
            container_name: "DEFAULT_CONTAINER_NAME".to_string(),
            service_system: Some("spark-driver".to_string()),
            source_pod_id: Some("driver-pod".to_string()),
            admission_metadata: None,
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

    /// A watcher whose file was renamed away keeps the old path, which now names a larger file.
    /// Its drain target (the new file's size) is unreachable through the old handle, so the drain
    /// must finish at EOF instead of spinning forever.
    #[tokio::test]
    async fn drain_on_shutdown_completes_when_path_now_names_larger_file() {
        use futures::FutureExt;

        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).await.unwrap();

        let log_path = tmp.path().join("a.log");
        let old_content: String = (0..10).map(|i| format!("old line {i}\n")).collect();
        fs::write(&log_path, &old_content).await.unwrap();

        let file_server = make_file_server(vec![log_path.clone()], data_dir.clone(), true);
        let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);
        let (shutdown_tx, shutdown_rx) = futures::channel::oneshot::channel::<()>();
        let shutdown = shutdown_rx.shared();
        let checkpointer = Checkpointer::new(data_dir.as_path());
        let server = tokio::spawn(file_server.run(tx, shutdown.clone(), shutdown, checkpointer));

        let mut received = Vec::new();
        while received.len() < 10 {
            received.extend(rx.next().await.expect("server exited early"));
        }

        // Rotate: the open watcher keeps `a.log` as its path, which now names a bigger file.
        fs::rename(&log_path, tmp.path().join("a.log.1"))
            .await
            .unwrap();
        let new_content: String = (0..100)
            .map(|i| format!("new line {i} with padding\n"))
            .collect();
        fs::write(&log_path, &new_content).await.unwrap();
        shutdown_tx.send(()).unwrap();

        let collector = tokio::spawn(async move { rx.collect::<Vec<_>>().await });
        let result = tokio::time::timeout(Duration::from_secs(10), server)
            .await
            .expect("drain did not finish for a rotated-away watcher");
        assert!(result.unwrap().is_ok());
        collector.await.unwrap();

        let old_lines: Vec<_> = received.iter().map(|line| line.text.clone()).collect();
        assert_eq!(old_lines[0], Bytes::from("old line 0"));
        assert_eq!(old_lines[9], Bytes::from("old line 9"));
    }

    /// Regression test for ES-2267297: when a watched file is rotated away and replaced by a
    /// larger file, the watcher's buffered unterminated tail line must be emitted during drain
    /// before the drain target is completed and the position is checkpointed. Without the fix,
    /// the unterminated tail is dropped and lost forever.
    #[tokio::test]
    async fn drain_on_shutdown_emits_buffered_unterminated_tail_before_completing() {
        use futures::FutureExt;

        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).await.unwrap();

        let log_path = tmp.path().join("a.log");
        // Write 10 lines with newlines, then one without a trailing newline.
        let mut old_content: String = (0..10).map(|i| format!("old line {i}\n")).collect();
        old_content.push_str("tail without newline");
        fs::write(&log_path, &old_content).await.unwrap();

        let file_server = make_file_server(vec![log_path.clone()], data_dir.clone(), true);
        let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);
        let (shutdown_tx, shutdown_rx) = futures::channel::oneshot::channel::<()>();
        let shutdown = shutdown_rx.shared();
        let checkpointer = Checkpointer::new(data_dir.as_path());
        let server = tokio::spawn(file_server.run(tx, shutdown.clone(), shutdown, checkpointer));

        // Wait until all 10 terminated lines are received.
        let mut received = Vec::new();
        while received.len() < 10 {
            received.extend(rx.next().await.expect("server exited early"));
        }

        // Rotate: the open watcher keeps `a.log` as its path, which now names a bigger file.
        fs::rename(&log_path, tmp.path().join("a.log.1"))
            .await
            .unwrap();
        let new_content: String = (0..100)
            .map(|i| format!("new line {i} with padding\n"))
            .collect();
        fs::write(&log_path, &new_content).await.unwrap();
        shutdown_tx.send(()).unwrap();

        let collector = tokio::spawn(async move { rx.collect::<Vec<_>>().await });
        let result = tokio::time::timeout(Duration::from_secs(10), server)
            .await
            .expect("drain did not finish for a rotated-away watcher");
        assert!(result.unwrap().is_ok());
        let drain_batches = collector.await.unwrap();

        // Collect all received lines including the tail from both pre-drain and drain phases.
        let mut all_lines: Vec<_> = received.iter().map(|line| line.text.clone()).collect();
        for batch in drain_batches {
            all_lines.extend(batch.iter().map(|line| line.text.clone()));
        }

        let expected_tail = Bytes::from("tail without newline");
        let tail_count = all_lines
            .iter()
            .filter(|line| *line == &expected_tail)
            .count();
        assert_eq!(
            tail_count, 1,
            "unterminated tail should be emitted exactly once"
        );
    }

    /// If the consumer stops reading mid-drain (e.g. the source's event loop hit its shutdown
    /// deadline and dropped the receiver), the drain must exit instead of blocking on send.
    #[tokio::test]
    async fn drain_on_shutdown_exits_when_receiver_dropped() {
        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).await.unwrap();

        let log_path = tmp.path().join("big.log");
        let content: String = (0..5_000)
            .map(|i| format!("line {i:05} -- padding to make this longer\n"))
            .collect();
        fs::write(&log_path, &content).await.unwrap();

        let file_server = make_file_server(vec![log_path], data_dir.clone(), true);
        let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);
        let checkpointer = Checkpointer::new(data_dir.as_path());

        // Shut down immediately so the whole file is left to the drain, read one batch, then
        // stop consuming.
        let server = tokio::spawn(file_server.run(
            tx,
            futures::future::ready(()),
            futures::future::ready(()),
            checkpointer,
        ));
        rx.next()
            .await
            .expect("expected at least one drained batch");
        drop(rx);

        let result = tokio::time::timeout(Duration::from_secs(10), server)
            .await
            .expect("drain blocked after the receiver was dropped");
        assert!(result.unwrap().is_ok());
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
        let checkpointer = Checkpointer::new(data_dir.as_path());

        // Like the source's map closure, the consumer is what advances checkpoints. The
        // checkpoint writer is only released once the consumer is done.
        let view = checkpointer.view();
        let (done_tx, done_rx) = futures::channel::oneshot::channel::<()>();
        let collector = tokio::spawn(async move {
            let mut lines = Vec::new();
            while let Some(batch) = rx.next().await {
                for line in &batch {
                    view.update(line.file_id, line.end_offset);
                }
                lines.extend(batch);
            }
            let _ = done_tx.send(());
            lines
        });

        let result = file_server
            .run(tx, shutdown_data, done_rx, checkpointer)
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

    // A gzip archive read to EOF reports READ_STARTED once, then STOPPED with
    // ARCHIVE_DONE, its decompressed end offset and lifetime totals.
    #[tokio::test]
    async fn gz_read_to_eof_emits_read_started_and_archive_done() {
        use async_compression::tokio::bufread::GzipEncoder;
        use file_source_common::FileStopReason;
        use tokio::io::AsyncReadExt;

        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).await.unwrap();
        let gz_path = tmp.path().join("app.log.gz");
        let payload: &[u8] = b"first line\nsecond line\n";
        let mut gz_bytes = Vec::new();
        GzipEncoder::new(payload)
            .read_to_end(&mut gz_bytes)
            .await
            .unwrap();
        fs::write(&gz_path, &gz_bytes).await.unwrap();

        let events = LifecycleEvents::default();
        let file_server = make_file_server_with_emitter(
            vec![gz_path.clone()],
            data_dir.clone(),
            false,
            events.clone(),
        );
        run_file_server_during(file_server, &data_dir, async {
            tokio::time::sleep(Duration::from_millis(800)).await;
        })
        .await;

        let read_started = events.read_started.lock().unwrap().clone();
        assert_eq!(read_started.len(), 1, "{read_started:?}");
        let (path, info) = &read_started[0];
        assert_eq!(path, &gz_path);
        assert_eq!((info.lines_read, info.bytes_read), (2, 21));
        assert_eq!(info.stat.size_bytes, Some(gz_bytes.len() as u64));
        assert!(info.stat.mtime_ms.is_some());

        let unwatched = events.unwatched.lock().unwrap().clone();
        assert_eq!(unwatched.len(), 1, "{unwatched:?}");
        let (path, stopped) = &unwatched[0];
        assert_eq!(path, &gz_path);
        assert_eq!(stopped.fingerprint, info.fingerprint);
        assert_eq!(stopped.reason, FileStopReason::ArchiveDone);
        assert!(stopped.reached_eof);
        assert_eq!(stopped.end_offset, payload.len() as u64);
        assert_eq!((stopped.lines_read, stopped.bytes_read), (2, 21));
        assert_eq!(stopped.stat.size_bytes, Some(gz_bytes.len() as u64));
    }

    // With `oldest_first`, finishing an archive must not move the newest (hot, active) file ahead
    // of the older archives still being read. Otherwise the active file exhausts the per-file read
    // budget every cycle, the loop stops after it, and the remaining archives starve.
    #[tokio::test]
    async fn oldest_first_order_survives_finished_archive() {
        use async_compression::tokio::bufread::GzipEncoder;
        use tokio::io::AsyncReadExt;
        use tokio::sync::oneshot;

        fn numbered_lines(prefix: &str, count: usize) -> String {
            (0..count)
                .map(|i| format!("{prefix}-{i:04} {}\n", "x".repeat(40)))
                .collect()
        }

        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).await.unwrap();

        // Each file is larger than `max_read_bytes` (2048), so it takes several read cycles.
        let mut paths = Vec::new();
        for name in ["a1", "a2", "a3"] {
            let path = tmp.path().join(format!("app.log.{name}.gz"));
            let mut gz_bytes = Vec::new();
            GzipEncoder::new(numbered_lines(name, 100).as_bytes())
                .read_to_end(&mut gz_bytes)
                .await
                .unwrap();
            fs::write(&path, &gz_bytes).await.unwrap();
            paths.push(path);
        }
        let active_path = tmp.path().join("app.log");
        fs::write(&active_path, numbered_lines("active", 400))
            .await
            .unwrap();
        paths.push(active_path);

        let mut file_server = make_file_server(paths, data_dir.clone(), false);
        file_server.oldest_first = true;
        let (tx, rx) = mpsc::channel::<Vec<Line>>(64);
        let collector = tokio::spawn(rx.collect::<Vec<_>>());
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let shutdown_data: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
            Box::pin(async move {
                let _ = shutdown_rx.await;
            });
        let driver = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1500)).await;
            let _ = shutdown_tx.send(());
        });
        file_server
            .run(
                tx,
                shutdown_data,
                futures::future::ready(()),
                Checkpointer::new(data_dir.as_path()),
            )
            .await
            .unwrap();
        driver.await.unwrap();

        let texts: Vec<String> = collector
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .map(|line| String::from_utf8_lossy(&line.text).into_owned())
            .collect();
        let expected: Vec<String> = [("a1", 100), ("a2", 100), ("a3", 100), ("active", 400)]
            .into_iter()
            .flat_map(|(prefix, count)| {
                (0..count).map(move |i| format!("{prefix}-{i:04} {}", "x".repeat(40)))
            })
            .collect();
        assert_eq!(
            texts, expected,
            "files must be delivered oldest first, each in full"
        );
    }

    // A file removed by `remove_after` while it ends in an unterminated line reports an
    // `end_offset` at the end of its last delivered line, not past the buffered bytes.
    #[tokio::test]
    async fn stopped_end_offset_excludes_buffered_partial_line() {
        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).await.unwrap();
        let path = tmp.path().join("app.log");
        fs::write(&path, b"first line\npartial").await.unwrap();

        let events = LifecycleEvents::default();
        let mut file_server = make_file_server_with_emitter(
            vec![path.clone()],
            data_dir.clone(),
            false,
            events.clone(),
        );
        file_server.remove_after = Some(Duration::from_millis(200));
        run_file_server_during(file_server, &data_dir, async {
            tokio::time::sleep(Duration::from_millis(800)).await;
        })
        .await;

        assert!(!path.exists(), "remove_after should have deleted the file");
        let unwatched = events.unwatched.lock().unwrap().clone();
        assert_eq!(unwatched.len(), 1, "{unwatched:?}");
        let stopped = &unwatched[0].1;
        assert_eq!((stopped.lines_read, stopped.bytes_read), (1, 10));
        assert_eq!(
            stopped.end_offset, 11,
            "end of `first line\\n`, excluding `partial`"
        );
    }

    // A rotated-away file reports STOPPED with FILE_GONE and its own last-seen size. Its old
    // path now holds the new file, whose size must not leak into the event.
    #[tokio::test]
    async fn rotated_away_file_emits_file_gone_with_its_last_seen_stat() {
        use file_source_common::FileStopReason;

        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).await.unwrap();
        let path = tmp.path().join("app.log");
        fs::write(&path, b"first line\n").await.unwrap();

        let events = LifecycleEvents::default();
        let mut file_server = make_file_server_with_emitter(
            vec![path.clone()],
            data_dir.clone(),
            false,
            events.clone(),
        );
        file_server.rotate_wait = Duration::ZERO;
        let rotate_path = path.clone();
        let rotated_path = tmp.path().join("app.log.1");
        run_file_server_during(file_server, &data_dir, async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            // Grow the file after the watcher was created, so its final size can only come
            // from a later discovery pass (glob cooldown is 100ms).
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(&rotate_path)
                .await
                .unwrap();
            tokio::io::AsyncWriteExt::write_all(&mut file, b"second line\n")
                .await
                .unwrap();
            drop(file);
            tokio::time::sleep(Duration::from_millis(500)).await;
            fs::rename(&rotate_path, &rotated_path).await.unwrap();
            fs::write(&rotate_path, b"new file line\n").await.unwrap();
            tokio::time::sleep(Duration::from_millis(800)).await;
        })
        .await;

        let read_started = events.read_started.lock().unwrap().clone();
        assert_eq!(read_started.len(), 2, "one per file: {read_started:?}");
        let original = read_started[0].1.fingerprint;
        assert_ne!(read_started[1].1.fingerprint, original);

        let unwatched = events.unwatched.lock().unwrap().clone();
        assert_eq!(unwatched.len(), 1, "{unwatched:?}");
        let (stopped_path, stopped) = &unwatched[0];
        assert_eq!(stopped_path, &path);
        assert_eq!(stopped.fingerprint, original);
        assert_eq!(stopped.reason, FileStopReason::FileGone);
        assert!(stopped.reached_eof);
        assert_eq!(stopped.end_offset, 23);
        assert_eq!((stopped.lines_read, stopped.bytes_read), (2, 21));
        assert_eq!(stopped.stat.size_bytes, Some(23));
        assert!(stopped.stat.mtime_ms.is_some());
        assert_eq!(
            read_started[1].1.stat.size_bytes,
            Some(14),
            "the new file's own size"
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
                while rx.next().await.is_some() {}
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
        fs::write(
            &active_json,
            format!("{line0}\n{line0b}\n{line0c}\n").as_bytes(),
        )
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
            idle_read_interval: Duration::from_secs(10),
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
            read_loop_max_backoff: 2_048,
            admission: None,
            test_readiness_hook: None,
            test_line_admission_hook: None,
            test_before_send_hook: None,
            test_checkpoint_flush_hook: None,
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
                fs::write(&active_json, new_content.as_bytes())
                    .await
                    .unwrap();
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
            .run(tx, shutdown_data, futures::future::ready(()), checkpointer)
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
        fs::write(&active_json, orig_content.as_bytes())
            .await
            .unwrap();

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
            idle_read_interval: Duration::from_secs(10),
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
            read_loop_max_backoff: 2_048,
            admission: None,
            test_readiness_hook: None,
            test_line_admission_hook: None,
            test_before_send_hook: None,
            test_checkpoint_flush_hook: None,
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
        let shutdown_checkpointer: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
            Box::pin(async move {
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
                        [&line0, &line0b, &line0c]
                            .iter()
                            .all(|orig| buf.iter().any(|l| l.as_str() == orig.as_str()))
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

    /// The drain must not persist a checkpoint past lines downstream has not taken. Here the
    /// consumer never polls while the server shuts down, then goes away (a reload whose deadline
    /// dropped the stream). Every file's persisted checkpoint must stay at what was consumed (0),
    /// and a restart must deliver every line again.
    #[tokio::test]
    async fn drain_on_shutdown_does_not_checkpoint_past_undelivered_lines() {
        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).await.unwrap();

        // Small files so the first read pass and the drain cover them completely.
        let names = ["a", "b", "c"];
        let mut paths = Vec::new();
        for name in names {
            let path = tmp.path().join(format!("{name}.log"));
            let content: String = (0..30)
                .map(|i| format!("{name} line {i:03} padding\n"))
                .collect();
            fs::write(&path, content).await.unwrap();
            paths.push(path);
        }

        // Run 1: stalled consumer. Nothing is polled until the server has exited.
        let file_server = make_file_server(paths.clone(), data_dir.clone(), true);
        let (tx, rx) = mpsc::channel::<Vec<Line>>(8);
        let result = file_server
            .run(
                tx,
                futures::future::ready(()),
                futures::future::ready(()),
                Checkpointer::new(data_dir.as_path()),
            )
            .await;
        assert!(result.is_ok());
        drop(rx);

        let mut persisted = Checkpointer::new(data_dir.as_path());
        persisted.read_checkpoints(None).await;
        let mut fingerprinter = Fingerprinter::new(
            FingerprintStrategy::FirstLinesChecksum {
                ignored_header_bytes: 0,
                lines: 1,
            },
            1024,
            true,
        );
        let mut known_small_files = HashMap::new();
        for path in &paths {
            let fingerprint = fingerprinter
                .fingerprint_or_emit(path, &mut known_small_files, &NoErrors)
                .await
                .expect("should be able to fingerprint the file");
            let position = persisted.view().get(fingerprint).unwrap_or(0);
            assert_eq!(
                position, 0,
                "{path:?}: checkpoint advanced to {position} although downstream consumed nothing"
            );
        }

        // Run 2: restart. Nothing was checkpointed, so every line is delivered again.
        let file_server = make_file_server(paths, data_dir.clone(), true);
        let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);
        let collector = tokio::spawn(async move {
            let mut lines = Vec::new();
            while let Some(batch) = rx.next().await {
                lines.extend(batch);
            }
            lines
        });
        file_server
            .run(
                tx,
                futures::future::ready(()),
                futures::future::ready(()),
                Checkpointer::new(data_dir.as_path()),
            )
            .await
            .expect("FileServer::run failed");
        let received = collector.await.unwrap();
        for name in names {
            let count = received
                .iter()
                .filter(|line| line.filename.ends_with(&format!("{name}.log")))
                .count();
            assert_eq!(count, 30, "{name}.log lines lost across restart");
        }
    }

    /// A live (still findable) file whose last bytes at shutdown are an unterminated line: the
    /// drain must emit those buffered bytes before completing, and must not checkpoint past
    /// bytes it hasn't emitted, so the next run neither loses nor re-delivers them.
    #[tokio::test]
    async fn drain_on_shutdown_emits_live_file_partial_line_exactly_once() {
        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).await.unwrap();
        let log_path = tmp.path().join("test.log");
        fs::write(&log_path, "line 1\nline 2\nline 3\npartial")
            .await
            .unwrap();

        async fn drain_once(log_path: &Path, data_dir: &Path) -> Vec<Bytes> {
            let file_server =
                make_file_server(vec![log_path.to_path_buf()], data_dir.to_path_buf(), true);
            let (tx, mut rx) = mpsc::channel::<Vec<Line>>(2);
            let checkpointer = Checkpointer::new(data_dir);
            // The consumer advances checkpoints, as the source's map closure does.
            let view = checkpointer.view();
            let (done_tx, done_rx) = futures::channel::oneshot::channel::<()>();
            let collector = tokio::spawn(async move {
                let mut lines = Vec::new();
                while let Some(batch) = rx.next().await {
                    for line in &batch {
                        view.update(line.file_id, line.end_offset);
                    }
                    lines.extend(batch.into_iter().map(|line| line.text));
                }
                let _ = done_tx.send(());
                lines
            });
            file_server
                .run(tx, futures::future::ready(()), done_rx, checkpointer)
                .await
                .expect("FileServer::run failed");
            collector.await.unwrap()
        }

        assert_eq!(
            drain_once(&log_path, &data_dir).await,
            vec!["line 1", "line 2", "line 3", "partial"]
        );
        // The checkpoint covers exactly what was emitted, so a restart re-delivers nothing.
        assert!(drain_once(&log_path, &data_dir).await.is_empty());
    }

    mod admission_tests {
        include!("file_server_admission_tests.rs");
    }
}
