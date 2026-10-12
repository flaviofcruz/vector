use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, ready},
};

use futures::{
    FutureExt, StreamExt,
    channel::oneshot,
    future::{BoxFuture, Either, select},
    pin_mut,
    stream::FuturesOrdered,
};

use crate::shutdown::{ShutdownSignal, ShutdownSignalToken};

/// Lifecycle encapsulates logic for managing a lifecycle of multiple futures
/// that are bounded together by a shared shutdown condition.
///
/// If any of the futures completes, or global shutdown it requested, all of the
/// managed futures are requested to shutdown. They can do so gracefully after
/// completing their work.
#[derive(Debug)]
pub struct Lifecycle<'bound> {
    futs: FuturesOrdered<BoxFuture<'bound, ()>>,
    fut_shutdowns: Vec<oneshot::Sender<()>>,
}

/// Holds a "global" shutdown signal or shutdown signal token.
/// Effectively used to hold the token or signal such that it can be dropped
/// after the shutdown is complete.
///
/// Dropping the [`ShutdownSignalToken`] tells the topology that this source has finished
/// shutting down, which disarms its force-shutdown deadline. It must therefore outlive every
/// managed future, or a future that never finishes (e.g. a stuck file drain) blocks a config
/// reload forever.
#[derive(Debug)]
pub enum GlobalShutdownToken {
    /// The global shutdown signal was consumed.
    Token(ShutdownSignalToken),
    /// The [`ShutdownSignal`] wasn't consumed, and still holds on to the
    /// [`ShutdownSignalToken`].
    Unused(ShutdownSignal),
}

impl<'bound> Lifecycle<'bound> {
    /// Create a new [`Lifecycle`].
    pub fn new() -> Self {
        Self {
            futs: FuturesOrdered::new(),
            fut_shutdowns: Vec::new(),
        }
    }

    /// Add a new future to be managed by the [`Lifecycle`].
    ///
    /// Returns a [`Slot`] to be bound with the `Future`, and
    /// a [`ShutdownHandle`] that is to be used by the bound future to wait for
    /// shutdown.
    pub fn add(&mut self) -> (Slot<'bound, '_>, ShutdownHandle) {
        let (tx, rx) = oneshot::channel();
        let slot = Slot {
            lifecycle: self,
            shutdown_trigger: tx,
        };
        let shutdown_handle = ShutdownHandle(rx);
        (slot, shutdown_handle)
    }

    /// Run the managed futures and keep track of the shutdown process.
    pub async fn run(mut self, mut global_shutdown: ShutdownSignal) -> GlobalShutdownToken {
        let first_task_fut = self.futs.next();
        pin_mut!(first_task_fut);

        let shutdown_signal_token = match select(first_task_fut, &mut global_shutdown).await {
            Either::Left((None, _)) => {
                trace!(message = "Lifecycle had no tasks upon run, we're done.");
                None
            }
            Either::Left((Some(()), _)) => {
                trace!(message = "Lifecycle had the first task completed.");
                None
            }
            Either::Right((shutdown_signal_token, _)) => {
                trace!(message = "Lifecycle got a global shutdown request.");
                Some(shutdown_signal_token)
            }
        };
        let token = match shutdown_signal_token {
            Some(token) => GlobalShutdownToken::Token(token),
            None => GlobalShutdownToken::Unused(global_shutdown),
        };

        // Send the shutdowns to all managed futures.
        for fut_shutdown in self.fut_shutdowns {
            if fut_shutdown.send(()).is_err() {
                trace!(
                    message = "Error while sending a future shutdown, \
                        the receiver is already dropped; \
                        this is not a problem."
                );
            }
        }

        // Wait for all the futures to complete.
        while let Some(()) = self.futs.next().await {
            trace!(message = "A lifecycle-managed future completed after shutdown was requested.");
        }

        // Return the global shutdown token so that caller can perform it's
        // cleanup.
        token
    }
}

/// Represents an unbounded slot at the lifecycle.
#[derive(Debug)]
pub struct Slot<'bound, 'lc> {
    lifecycle: &'lc mut Lifecycle<'bound>,
    shutdown_trigger: oneshot::Sender<()>,
}

impl<'bound> Slot<'bound, '_> {
    /// Bind the lifecycle slot to a concrete future.
    /// The passed future MUST start it's shutdown process when requested to
    /// shutdown via the signal passed from the corresponding
    /// [`ShutdownHandle`].
    pub fn bind(self, future: BoxFuture<'bound, ()>) {
        self.lifecycle.futs.push_back(future);
        self.lifecycle.fut_shutdowns.push(self.shutdown_trigger);
    }
}

/// A handle that allows waiting for the lifecycle-issued shutdown.
#[derive(Debug)]
pub struct ShutdownHandle(oneshot::Receiver<()>);

impl Future for ShutdownHandle {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        _ = ready!(self.0.poll_unpin(cx));
        Poll::Ready(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use super::*;
    use crate::{config::ComponentKey, shutdown::SourceShutdownCoordinator};

    /// The topology force-stops a source that misses its shutdown deadline, but only while the
    /// source still holds its shutdown token. A managed future that never finishes must not
    /// disarm that deadline.
    #[tokio::test]
    async fn stuck_managed_future_is_force_shut_down() {
        let mut coordinator = SourceShutdownCoordinator::default();
        let id = ComponentKey::from("kubernetes_logs");
        let (shutdown, force_shutdown) = coordinator.register_source(&id, false);

        let mut lifecycle = Lifecycle::new();
        let (slot, fut_shutdown) = lifecycle.add();
        slot.bind(Box::pin(async move {
            fut_shutdown.await;
            futures::future::pending::<()>().await;
        }));
        let running = tokio::spawn(lifecycle.run(shutdown));

        let deadline = Instant::now() + Duration::from_millis(200);
        let completed_cleanly = coordinator.shutdown_source(&id, deadline).await;
        assert!(
            !completed_cleanly,
            "source reported shutdown complete while a managed future was still running"
        );
        tokio::time::timeout(Duration::from_secs(5), force_shutdown)
            .await
            .expect("force shutdown was not triggered");
        running.abort();
    }

    #[tokio::test]
    async fn shutdown_completes_once_managed_futures_finish() {
        let mut coordinator = SourceShutdownCoordinator::default();
        let id = ComponentKey::from("kubernetes_logs");
        let (shutdown, _force_shutdown) = coordinator.register_source(&id, false);

        let mut lifecycle = Lifecycle::new();
        let (slot, fut_shutdown) = lifecycle.add();
        slot.bind(Box::pin(fut_shutdown));
        let running = tokio::spawn(async move {
            let _token = lifecycle.run(shutdown).await;
        });

        let deadline = Instant::now() + Duration::from_secs(5);
        assert!(coordinator.shutdown_source(&id, deadline).await);
        running.await.unwrap();
    }
}
