use std::{
    error::Error,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::{
    FutureExt, Sink, Stream, StreamExt,
    future::{Either, select},
    pin_mut,
};
use tracing::Instrument;
use vector_lib::{
    ByteSizeOf,
    event::Event,
    file_source::{
        file_server::{FileServer, Line, Shutdown as FileServerShutdown},
        paths_provider::PathsProvider,
    },
    file_source_common::{
        Checkpointer, CheckpointsView, FileFingerprint, FilePosition, FileSourceInternalEvents,
    },
    source_sender::{CHUNK_SIZE, SendError},
};

use crate::SourceSender;

/// Runs a [`FileServer`] to completion, choosing the execution strategy via `run_async`.
///
/// - `false` (default): the legacy `spawn_blocking(move || rt.block_on(...))` wrapper. It pins a
///   blocking-pool thread for the source's lifetime and — because `spawn_blocking` tasks are not
///   cancellable — keeps the file server draining via its own shutdown-signal path even when the
///   source task is aborted (e.g. the wave-1 drain-deadline force-close in `RunningTopology::stop`),
///   so the abort is muted and shutdown can extend past `data_source_deadline` (the ~30 s
///   `event_processing_loop` tail). Exactly the historical behavior.
///
/// - `true`: run `file_server.run(...).await` directly on the surrounding async runtime, so the
///   source is cancelled at its next `.await` when aborted and no blocking-pool thread is held.
///   The per-event read path is fully `tokio::fs` / `AsyncBufRead`-based. The one synchronous call
///   is the directory glob in `K8sPathsProvider::paths()` (`glob::glob_with`), which
///   `FileServer::run` performs at startup and once per `glob_minimum_cooldown` (default 60 s) — so
///   on the async path it briefly occupies a runtime worker roughly once a minute rather than a
///   dedicated blocking thread. That small periodic cost is the reason this path is opt-in; the
///   `spawn_blocking` wrapper itself was flagged as unnecessary upstream (vectordotdev/vector#23743).
///
/// Gated on the `async_file_server` global flag so the change is a no-op by default
/// and rampable via config. Errors are unified to `String` for the `KubernetesLifecycleError` log
/// line: on the legacy path a sink error still becomes a panic surfaced as a `JoinError` (historical
/// behavior); on the async path the sink error is returned directly.
pub async fn run_file_server<PP, E, C, S, K>(
    file_server: FileServer<PP, E>,
    chans: C,
    shutdown: S,
    checkpoint_shutdown: K,
    checkpointer: Checkpointer,
    run_async: bool,
) -> Result<FileServerShutdown, String>
where
    PP: PathsProvider + Send + Sync + 'static,
    E: FileSourceInternalEvents,
    C: Sink<Vec<Line>> + Unpin + Send + 'static,
    <C as Sink<Vec<Line>>>::Error: Error + Send,
    S: Future + Unpin + Send + 'static,
    K: Future + Unpin + Send + 'static,
    <<PP as PathsProvider>::IntoIter as IntoIterator>::IntoIter: Send,
{
    let span = info_span!("file_server");

    if run_async {
        file_server
            .run(chans, shutdown, checkpoint_shutdown, checkpointer)
            .instrument(span)
            .await
            .map_err(|error| error.to_string())
    } else {
        let join_handle = tokio::task::spawn_blocking(move || {
            let _enter = span.enter();
            let rt = tokio::runtime::Handle::current();
            rt.block_on(file_server.run(chans, shutdown, checkpoint_shutdown, checkpointer))
                .expect("file server exited with an error")
        });
        join_handle.await.map_err(|error| error.to_string())
    }
}

/// Lets the event loop's completion release the file server's final checkpoint write.
///
/// The event loop advances the checkpoints as it delivers lines (see
/// [`DeliveryCheckpoints`]), which can be after the file server has already closed its output
/// channel. Writing the final checkpoint before the loop has finished leaves the on-disk
/// checkpoint behind lines the topology already delivered, so the next source instance (for
/// example after a config reload) reads and delivers them again. Pass the returned signal as the
/// file server's checkpoint shutdown and run the event loop through [`EventsDone::after`].
pub fn events_done() -> (
    EventsDone,
    impl Future<Output = ()> + Unpin + Send + 'static,
) {
    let (done, signal) = futures::channel::oneshot::channel::<()>();
    // A dropped loop also resolves the signal, so the file server never waits on a dead loop.
    (EventsDone(done), signal.map(|_| ()))
}

pub struct EventsDone(futures::channel::oneshot::Sender<()>);

impl EventsDone {
    pub async fn after<F: Future>(self, events: F) -> F::Output {
        let output = events.await;
        if self.0.send(()).is_err() {
            trace!(message = "File server exited before the event loop finished.");
        }
        output
    }
}

pub async fn complete_with_deadline_on_signal<F, S>(
    future: F,
    signal: S,
    deadline: Duration,
) -> Result<<F as Future>::Output, tokio::time::error::Elapsed>
where
    F: Future,
    S: Future<Output = ()>,
{
    pin_mut!(future);
    pin_mut!(signal);
    let future = match select(future, signal).await {
        Either::Left((future_output, _)) => return Ok(future_output),
        Either::Right(((), future)) => future,
    };
    pin_mut!(future);
    tokio::time::timeout(deadline, future).await
}

/// Holds back the checkpoints of lines the event loop has read until their events are sent.
///
/// The final checkpoint write waits for the event loop (see [`events_done`]), so it must not see
/// a line that the loop read but then dropped at its shutdown deadline; the next source instance
/// re-reads such lines instead of losing them.
#[derive(Clone)]
pub struct DeliveryCheckpoints {
    read: Arc<Mutex<Vec<(FileFingerprint, FilePosition)>>>,
    checkpoints: Arc<CheckpointsView>,
}

impl DeliveryCheckpoints {
    pub fn new(checkpoints: Arc<CheckpointsView>) -> Self {
        Self {
            read: Arc::default(),
            checkpoints,
        }
    }

    /// Records that a line ending at `position` has been read into an event.
    pub fn read(&self, file_id: FileFingerprint, position: FilePosition) {
        self.read
            .lock()
            .expect("checkpoint mutex is poisoned")
            .push((file_id, position));
    }

    fn take_read(&self) -> Vec<(FileFingerprint, FilePosition)> {
        std::mem::take(&mut *self.read.lock().expect("checkpoint mutex is poisoned"))
    }

    fn commit(&self, read: Vec<(FileFingerprint, FilePosition)>) {
        for (file_id, position) in read {
            self.checkpoints.update(file_id, position);
        }
    }
}

/// Sends `stream` to `out` until the stream ends, or until `deadline` after `signal` fires,
/// advancing `checkpoints` past each batch of lines once its events are sent.
///
/// Takes ownership of `stream` so that hitting the deadline drops it. The stream owns the file
/// server's receiver, so dropping it fails the file server's pending sends instead of leaving a
/// `drain_on_shutdown` drain blocked forever on a receiver nobody polls.
pub async fn send_stream_with_deadline_on_signal<St, E, S>(
    mut out: SourceSender,
    stream: St,
    checkpoints: DeliveryCheckpoints,
    signal: S,
    deadline: Duration,
) -> Result<Result<(), SendError>, tokio::time::error::Elapsed>
where
    St: Stream<Item = E> + Unpin,
    E: Into<Event> + ByteSizeOf,
    S: Future<Output = ()>,
{
    let send = async move {
        let mut batches = stream.ready_chunks(*CHUNK_SIZE);
        while let Some(events) = batches.next().await {
            // Every line read so far fed this batch, or an event still held upstream of it (a
            // partial line awaiting merge), as before.
            let read = checkpoints.take_read();
            out.send_batch(events).await?;
            checkpoints.commit(read);
        }
        Ok(())
    };
    complete_with_deadline_on_signal(send, signal, deadline).await
}

#[cfg(test)]
mod tests {
    use futures::{SinkExt, StreamExt, channel::mpsc};
    use tokio::time::Instant;
    use vector_lib::{
        event::{EventContainer, LogEvent},
        file_source::paths_provider::LogFileInfo,
        file_source_common::{FingerprintStrategy, Fingerprinter, ReadFrom},
    };

    use super::*;
    use crate::{
        config::ComponentKey, shutdown::SourceShutdownCoordinator,
        sources::kubernetes_logs::lifecycle::Lifecycle,
    };

    fn event() -> Event {
        Event::Log(LogEvent::from("line"))
    }

    #[tokio::test]
    async fn deadline_closes_upstream_channel() {
        // The downstream is never polled, so sends to `out` block once its buffer is full.
        let (out, _downstream) = SourceSender::new_test_sender_with_options(1, None);
        let (mut upstream, stream) = mpsc::channel::<Event>(2);
        for _ in 0..2 {
            upstream.send(event()).await.unwrap();
        }

        let result = send_stream_with_deadline_on_signal(
            out,
            stream,
            DeliveryCheckpoints::new(Arc::default()),
            futures::future::ready(()),
            Duration::from_millis(100),
        )
        .await;

        assert!(result.is_err(), "expected the shutdown deadline to elapse");
        assert!(
            upstream.send(event()).await.is_err(),
            "upstream sends must fail once the event loop hits its deadline"
        );
    }

    #[tokio::test]
    async fn events_done_releases_the_checkpoint_only_after_the_loop_finishes() {
        let (done, mut signal) = events_done();
        let (finish, finished) = futures::channel::oneshot::channel::<()>();
        let events = tokio::spawn(done.after(async move {
            let _ = finished.await;
            7
        }));

        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut signal)
                .await
                .is_err(),
            "the signal must not fire while the loop is still draining"
        );
        finish.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), signal)
            .await
            .expect("the signal must fire once the loop finishes");
        assert_eq!(events.await.unwrap(), 7);
    }

    #[tokio::test]
    async fn events_done_also_releases_the_checkpoint_when_the_loop_is_dropped() {
        let (done, signal) = events_done();
        drop(done);
        tokio::time::timeout(Duration::from_secs(5), signal)
            .await
            .expect("a dropped loop must not block the final checkpoint write");
    }

    /// Mirrors the `kubernetes_logs` source during a config reload: a `drain_on_shutdown` file
    /// server keeps sending after shutdown while the downstream has stopped accepting events. The
    /// source must still finish, and report a clean shutdown before the topology's force deadline.
    #[tokio::test]
    async fn draining_source_with_stalled_downstream_shuts_down() {
        let mut coordinator = SourceShutdownCoordinator::default();
        let id = ComponentKey::from("kubernetes_logs");
        let (global_shutdown, _force_shutdown) = coordinator.register_source(&id, false);

        let (out, _downstream) = SourceSender::new_test_sender_with_options(1, None);
        let (mut file_tx, file_rx) = mpsc::channel::<Event>(2);

        let mut lifecycle = Lifecycle::new();
        {
            let (slot, shutdown) = lifecycle.add();
            slot.bind(Box::pin(async move {
                shutdown.await;
                while file_tx.send(event()).await.is_ok() {}
            }));
        }
        {
            let (slot, shutdown) = lifecycle.add();
            slot.bind(Box::pin(
                send_stream_with_deadline_on_signal(
                    out,
                    file_rx,
                    DeliveryCheckpoints::new(Arc::default()),
                    shutdown,
                    Duration::from_millis(200),
                )
                .map(|_| ()),
            ));
        }
        let running = tokio::spawn(async move {
            let _token = lifecycle.run(global_shutdown).await;
        });

        let deadline = Instant::now() + Duration::from_secs(10);
        assert!(
            coordinator.shutdown_source(&id, deadline).await,
            "source should finish on its own instead of needing a force shutdown"
        );
        running.await.unwrap();
    }

    struct OnePath(std::path::PathBuf);

    impl PathsProvider for OnePath {
        type IntoIter = Vec<(Option<LogFileInfo>, std::path::PathBuf)>;

        fn paths(&self) -> Self::IntoIter {
            vec![(None, self.0.clone())]
        }
    }

    /// Mirrors a `kubernetes_logs` reload with a downstream that lags the file server's shutdown,
    /// or stops accepting events so the event loop hits its deadline. The next source instance
    /// must start right after the lines the old one delivered: earlier re-delivers them, later
    /// loses lines the event loop dropped.
    async fn next_instance_resumes_at_delivered_offset(
        drain_on_shutdown: bool,
        stall_downstream: bool,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        let log = dir.path().join("pod.log");
        std::fs::write(&log, "line 0\nline 1\nline 2\nline 3\nline 4\nline 5\n").unwrap();

        let file_server = FileServer {
            paths_provider: OnePath(log),
            max_read_bytes: 8, // one line per batch
            ignore_checkpoints: false,
            read_from: ReadFrom::Beginning,
            ignore_before: None,
            start_reading_at: None,
            max_line_bytes: 1024,
            line_delimiter: bytes::Bytes::from("\n"),
            data_dir: data_dir.clone(),
            glob_minimum_cooldown: Duration::from_millis(50),
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
            emitter: crate::internal_events::FileSourceInternalEventsEmitter {
                include_file_metric_tag: false,
            },
            rotate_wait: Duration::from_secs(u64::MAX / 2),
            ttl_removal_config: None,
            source_context: None,
            file_to_pod_map: None,
            drain_on_shutdown,
            archive_extensions: vec![],
            source_type: "kubernetes_logs",
            read_loop_min_backoff: 1,
            read_loop_max_backoff: 2_048,
            admission: None,
        };
        let checkpointer = Checkpointer::new(&data_dir);
        let checkpoints = DeliveryCheckpoints::new(checkpointer.view());
        let read_checkpoints = checkpoints.clone();
        let (tx, rx) = mpsc::channel::<Vec<Line>>(2);
        let (stop, stopped) = futures::channel::oneshot::channel::<()>();
        let stopped = stopped.map(|_| ()).shared();

        let (events_done, checkpoint_shutdown) = events_done();
        let server = tokio::spawn(run_file_server(
            file_server,
            tx,
            stopped.clone(),
            checkpoint_shutdown,
            checkpointer,
            true,
        ));

        // The source's event loop: a line's checkpoint is recorded as the line is read.
        let fingerprint = Arc::new(Mutex::new(None));
        let seen = Arc::clone(&fingerprint);
        let events = rx.flat_map(futures::stream::iter).map(move |line: Line| {
            *seen.lock().unwrap() = Some(line.file_id);
            read_checkpoints.read(line.file_id, line.end_offset);
            Event::Log(LogEvent::from(line.text))
        });
        let (mut out, downstream) = SourceSender::new_test_sender_with_options(1, None);
        let deadline = if stall_downstream {
            // Fill the one-event buffer so the event loop's first send blocks.
            out.send_event(event()).await.unwrap();
            Duration::from_millis(200)
        } else {
            Duration::from_secs(10)
        };
        let event_loop = tokio::spawn(events_done.after(send_stream_with_deadline_on_signal(
            out,
            events,
            checkpoints,
            stopped,
            deadline,
        )));

        let mut downstream = downstream.into_stream();
        let mut delivered: i64 = 0;
        if stall_downstream {
            // Nothing is consumed, so the event loop hits its deadline after shutdown and drops
            // the lines it has read.
            tokio::time::sleep(Duration::from_millis(100)).await;
            stop.send(()).unwrap();
            delivered = -1; // the filler event
        } else {
            // Slow downstream. Shutdown is requested as the first event arrives, while later
            // lines are still in the file server's channel.
            let mut stop = Some(stop);
            while let Some(item) = downstream.next().await {
                if let Some(stop) = stop.take() {
                    stop.send(()).unwrap();
                }
                delivered += item.events.len() as i64;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        let event_loop_result = event_loop.await.unwrap();
        // Whatever the event loop handed to the topology before it finished counts as delivered.
        while let Some(Some(item)) = downstream.next().now_or_never() {
            delivered += item.events.len() as i64;
        }
        server.await.unwrap().unwrap();
        assert_eq!(
            event_loop_result.is_err(),
            stall_downstream,
            "only a stalled downstream makes the event loop hit its deadline"
        );

        let mut next_instance = Checkpointer::new(&data_dir);
        next_instance.read_checkpoints(None).await;
        let fingerprint = fingerprint.lock().unwrap().expect("a line was read");
        let delivered_to = 7 * u64::try_from(delivered).unwrap(); // every line is 7 bytes
        assert_eq!(
            next_instance.view().get(fingerprint).unwrap_or(0),
            delivered_to,
            "the next instance must resume exactly after the delivered lines"
        );
    }

    #[tokio::test]
    async fn final_checkpoint_covers_lines_still_in_the_channel_at_shutdown() {
        next_instance_resumes_at_delivered_offset(false, false).await;
    }

    #[tokio::test]
    async fn final_checkpoint_covers_lines_still_in_the_channel_at_drain_shutdown() {
        next_instance_resumes_at_delivered_offset(true, false).await;
    }

    #[tokio::test]
    async fn final_checkpoint_skips_lines_dropped_at_the_shutdown_deadline() {
        next_instance_resumes_at_delivered_offset(false, true).await;
    }

    #[tokio::test]
    async fn final_checkpoint_skips_lines_dropped_at_the_drain_shutdown_deadline() {
        next_instance_resumes_at_delivered_offset(true, true).await;
    }
}
