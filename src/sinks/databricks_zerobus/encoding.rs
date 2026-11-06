//! Bounded CPU work that remains accounted for when a request is cancelled.

use std::{num::NonZeroUsize, sync::Arc, time::Instant};

use tokio::sync::Semaphore;

use super::error::ZerobusSinkError;

#[derive(Clone)]
pub(super) struct EncodingExecutor {
    slots: Option<Arc<Semaphore>>,
}

impl EncodingExecutor {
    pub(super) fn new(concurrency: Option<NonZeroUsize>) -> Self {
        Self {
            slots: concurrency.map(|limit| Arc::new(Semaphore::new(limit.get()))),
        }
    }

    pub(super) async fn run<F, T>(&self, encode: F) -> Result<T, ZerobusSinkError>
    where
        F: FnOnce() -> Result<T, ZerobusSinkError> + Send + 'static,
        T: Send + 'static,
    {
        let Some(slots) = &self.slots else {
            return timed_encoding(encode);
        };

        let queued = Instant::now();
        let permit = Arc::clone(slots)
            .acquire_owned()
            .await
            .expect("encoding semaphore is never closed");
        let span = tracing::Span::current();

        tokio::task::spawn_blocking(move || {
            // Cancelling a JoinHandle cannot stop running CPU work. The job owns its permit
            // until it exits, so request timeouts cannot accumulate unbounded detached jobs.
            let _permit = permit;
            span.in_scope(|| {
                metrics::histogram!("zerobus_encoding_wait_seconds").record(queued.elapsed());
                timed_encoding(encode)
            })
        })
        .await
        .map_err(|error| ZerobusSinkError::EncodingTaskFailed {
            message: error.to_string(),
        })?
    }
}

fn timed_encoding<F, T>(encode: F) -> Result<T, ZerobusSinkError>
where
    F: FnOnce() -> Result<T, ZerobusSinkError>,
{
    let start = Instant::now();
    let result = encode();
    metrics::histogram!("zerobus_encoding_seconds").record(start.elapsed());
    result
}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc, thread, time::Duration};

    use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
    use tokio::sync::oneshot;

    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn offloaded_encodes_overlap_on_a_single_driver_task() {
        let executor = EncodingExecutor::new(NonZeroUsize::new(2));
        let driver_thread = thread::current().id();
        let mut requests = FuturesUnordered::new();
        let mut started = Vec::new();
        let mut releases = Vec::new();

        for _ in 0..2 {
            let (start_tx, start_rx) = oneshot::channel();
            let (release_tx, release_rx) = mpsc::channel();
            requests.push(executor.run(move || {
                start_tx.send(thread::current().id()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                Ok(())
            }));
            started.push(start_rx);
            releases.push(release_tx);
        }

        // Poll the same collection shape as the sink driver. Both jobs must start before
        // either is released; inline encoding cannot meet this condition.
        assert!(requests.next().now_or_never().is_none());
        for start in started {
            let worker = tokio::time::timeout(Duration::from_secs(5), start)
                .await
                .expect("both encoding jobs should start concurrently")
                .unwrap();
            assert_ne!(worker, driver_thread);
        }
        for release in releases {
            release.send(()).unwrap();
        }
        while let Some(result) = requests.next().await {
            result.unwrap();
        }
    }

    #[tokio::test]
    async fn cancellation_does_not_release_a_running_workers_permit() {
        let executor = EncodingExecutor::new(NonZeroUsize::new(1));
        let first_executor = executor.clone();
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let first = tokio::spawn(async move {
            first_executor
                .run(move || {
                    started_tx.send(()).unwrap();
                    release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                    Ok(())
                })
                .await
        });
        started_rx.await.unwrap();
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert_eq!(executor.slots.as_ref().unwrap().available_permits(), 0);

        let second = executor.run(|| Ok(()));
        tokio::pin!(second);
        assert!(second.as_mut().now_or_never().is_none());
        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), second)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn omitted_concurrency_preserves_inline_execution() {
        let driver_thread = thread::current().id();
        let executor = EncodingExecutor::new(None);
        let encoding_thread = executor.run(|| Ok(thread::current().id())).await.unwrap();
        assert_eq!(driver_thread, encoding_thread);
    }

    #[tokio::test]
    async fn worker_panic_releases_capacity_and_remains_retryable() {
        let executor = EncodingExecutor::new(NonZeroUsize::new(1));
        let error = executor
            .run::<_, ()>(|| panic!("encoding worker failed"))
            .await
            .unwrap_err();
        assert!(matches!(error, ZerobusSinkError::EncodingTaskFailed { .. }));
        assert!(error.is_retryable());
        assert_eq!(executor.run(|| Ok(42)).await.unwrap(), 42);
    }
}
