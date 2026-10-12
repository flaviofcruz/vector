use std::{error::Error, future::Future, time::Duration};

use futures::{
    FutureExt, Sink, Stream,
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
    file_source_common::{Checkpointer, FileSourceInternalEvents},
    source_sender::SendError,
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
pub async fn run_file_server<PP, E, C, S>(
    file_server: FileServer<PP, E>,
    chans: C,
    shutdown: S,
    checkpointer: Checkpointer,
    run_async: bool,
) -> Result<FileServerShutdown, String>
where
    PP: PathsProvider + Send + Sync + 'static,
    E: FileSourceInternalEvents,
    C: Sink<Vec<Line>> + Unpin + Send + 'static,
    <C as Sink<Vec<Line>>>::Error: Error + Send,
    S: Future + Unpin + Send + 'static,
    <S as Future>::Output: Clone + Send + Sync,
    <<PP as PathsProvider>::IntoIter as IntoIterator>::IntoIter: Send,
{
    let span = info_span!("file_server");
    // These will need to be separated when this source is updated to support
    // end-to-end acknowledgements.
    let shutdown = shutdown.shared();
    let shutdown2 = shutdown.clone();

    if run_async {
        file_server
            .run(chans, shutdown, shutdown2, checkpointer)
            .instrument(span)
            .await
            .map_err(|error| error.to_string())
    } else {
        let join_handle = tokio::task::spawn_blocking(move || {
            let _enter = span.enter();
            let rt = tokio::runtime::Handle::current();
            rt.block_on(file_server.run(chans, shutdown, shutdown2, checkpointer))
                .expect("file server exited with an error")
        });
        join_handle.await.map_err(|error| error.to_string())
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

/// Sends `stream` to `out` until the stream ends, or until `deadline` after `signal` fires.
///
/// Takes ownership of `stream` so that hitting the deadline drops it. The stream owns the file
/// server's receiver, so dropping it fails the file server's pending sends instead of leaving a
/// `drain_on_shutdown` drain blocked forever on a receiver nobody polls.
pub async fn send_stream_with_deadline_on_signal<St, E, S>(
    mut out: SourceSender,
    mut stream: St,
    signal: S,
    deadline: Duration,
) -> Result<Result<(), SendError>, tokio::time::error::Elapsed>
where
    St: Stream<Item = E> + Unpin,
    E: Into<Event> + ByteSizeOf,
    S: Future<Output = ()>,
{
    complete_with_deadline_on_signal(out.send_event_stream(&mut stream), signal, deadline).await
}

#[cfg(test)]
mod tests {
    use futures::{SinkExt, channel::mpsc};
    use tokio::time::Instant;
    use vector_lib::event::LogEvent;

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
}
