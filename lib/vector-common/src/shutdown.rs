#![allow(clippy::module_name_repetitions)]

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
};

use futures::{FutureExt, future};
use stream_cancel::{Trigger, Tripwire};
use tokio::time::{Instant, timeout_at};

use crate::{config::ComponentKey, trigger::DisabledTrigger};

pub async fn tripwire_handler(closed: bool) {
    std::future::poll_fn(|_| {
        if closed {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
}

/// When this struct goes out of scope and its internal refcount goes to 0 it is a signal that its
/// corresponding `Source` has completed executing and may be cleaned up.  It is the responsibility
/// of each `Source` to ensure that at least one copy of this handle remains alive for the entire
/// lifetime of the Source.
#[derive(Clone, Debug)]
pub struct ShutdownSignalToken {
    _shutdown_complete: Arc<Trigger>,
}

impl ShutdownSignalToken {
    fn new(shutdown_complete: Trigger) -> Self {
        Self {
            _shutdown_complete: Arc::new(shutdown_complete),
        }
    }
}

/// Passed to each `Source` to coordinate the global shutdown process.
#[pin_project::pin_project]
#[derive(Clone, Debug)]
pub struct ShutdownSignal {
    /// This will be triggered when global shutdown has begun, and is a sign to the Source to begin
    /// its shutdown process.
    #[pin]
    begin_shutdown: Option<Tripwire>,

    /// When a Source allows this to go out of scope it informs the global shutdown coordinator that
    /// this Source's local shutdown process is complete.
    /// Optional only so that `poll()` can move the handle out and return it.
    shutdown_complete: Option<ShutdownSignalToken>,
}

impl Future for ShutdownSignal {
    type Output = ShutdownSignalToken;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.as_mut().project().begin_shutdown.as_pin_mut() {
            Some(fut) => {
                let closed = ready!(fut.poll(cx));
                let mut pinned = self.project();
                pinned.begin_shutdown.set(None);
                if closed {
                    Poll::Ready(pinned.shutdown_complete.take().unwrap())
                } else {
                    Poll::Pending
                }
            }
            // TODO: This should almost certainly be a panic to avoid deadlocking in the case of a
            // poll-after-ready situation.
            None => Poll::Pending,
        }
    }
}

impl ShutdownSignal {
    #[must_use]
    pub fn new(tripwire: Tripwire, trigger: Trigger) -> Self {
        Self {
            begin_shutdown: Some(tripwire),
            shutdown_complete: Some(ShutdownSignalToken::new(trigger)),
        }
    }

    #[must_use]
    pub fn noop() -> Self {
        let (trigger, tripwire) = Tripwire::new();
        Self {
            begin_shutdown: Some(tripwire),
            shutdown_complete: Some(ShutdownSignalToken::new(trigger)),
        }
    }

    #[must_use]
    pub fn new_wired() -> (Trigger, ShutdownSignal, Tripwire) {
        let (trigger_shutdown, tripwire) = Tripwire::new();
        let (trigger, shutdown_done) = Tripwire::new();
        let shutdown = ShutdownSignal::new(tripwire, trigger);

        (trigger_shutdown, shutdown, shutdown_done)
    }
}

type IsInternal = bool;

/// Holds the shutdown handles for deferred (internal) sources, allowing them to be shut down
/// in a second wave after non-deferred sources have completed.
#[derive(Debug)]
pub struct DeferredSourceShutdowns {
    begun_triggers: HashMap<ComponentKey, Trigger>,
    force_triggers: HashMap<ComponentKey, Trigger>,
    complete_tripwires: HashMap<ComponentKey, Tripwire>,
}

impl DeferredSourceShutdowns {
    /// Returns true if there are any deferred sources to shut down.
    pub fn has_deferred_sources(&self) -> bool {
        !self.begun_triggers.is_empty()
    }

    /// Returns the set of deferred source component keys.
    pub fn deferred_keys(&self) -> std::collections::HashSet<ComponentKey> {
        self.begun_triggers.keys().cloned().collect()
    }

    /// Triggers shutdown of all deferred sources and returns a future that resolves once
    /// all have completed or been force-shutdown at the given deadline.
    pub fn shutdown_all(self, deadline: Option<Instant>) -> impl Future<Output = ()> {
        let mut complete_futures = Vec::new();

        let mut complete_tripwires = self.complete_tripwires;
        let mut force_triggers = self.force_triggers;

        for (id, trigger) in self.begun_triggers {
            trigger.cancel();

            let shutdown_complete_tripwire = complete_tripwires.remove(&id).unwrap_or_else(|| {
                panic!("shutdown_complete_tripwire for deferred source \"{id}\" not found")
            });
            let shutdown_force_trigger = force_triggers.remove(&id).unwrap_or_else(|| {
                panic!("shutdown_force_trigger for deferred source \"{id}\" not found")
            });

            complete_futures.push(SourceShutdownCoordinator::shutdown_source_complete(
                shutdown_complete_tripwire,
                shutdown_force_trigger,
                id,
                deadline,
            ));
        }

        futures::future::join_all(complete_futures).map(|_| ())
    }
}

#[derive(Debug, Default)]
pub struct SourceShutdownCoordinator {
    begun_triggers: HashMap<ComponentKey, (IsInternal, Trigger)>,
    force_triggers: HashMap<ComponentKey, Trigger>,
    complete_tripwires: HashMap<ComponentKey, Tripwire>,
}

impl SourceShutdownCoordinator {
    /// Creates the necessary Triggers and Tripwires for coordinating shutdown of this Source and
    /// stores them as needed.  Returns the `ShutdownSignal` for this Source as well as a Tripwire
    /// that will be notified if the Source should be forcibly shut down.
    pub fn register_source(
        &mut self,
        id: &ComponentKey,
        internal: bool,
    ) -> (ShutdownSignal, impl Future<Output = ()> + use<>) {
        let (shutdown_begun_trigger, shutdown_begun_tripwire) = Tripwire::new();
        let (force_shutdown_trigger, force_shutdown_tripwire) = Tripwire::new();
        let (shutdown_complete_trigger, shutdown_complete_tripwire) = Tripwire::new();

        self.begun_triggers
            .insert(id.clone(), (internal, shutdown_begun_trigger));
        self.force_triggers
            .insert(id.clone(), force_shutdown_trigger);
        self.complete_tripwires
            .insert(id.clone(), shutdown_complete_tripwire);

        let shutdown_signal =
            ShutdownSignal::new(shutdown_begun_tripwire, shutdown_complete_trigger);

        // `force_shutdown_tripwire` resolves even if canceled when we should *not* be shutting down.
        // `tripwire_handler` handles cancel by never resolving.
        let force_shutdown_tripwire = force_shutdown_tripwire.then(tripwire_handler);
        // Internal sources are still *deferred* — `shutdown_non_deferred` uses the `internal` flag
        // in `begun_triggers` to shut them down in wave 2, after non-deferred sources drain. They
        // get the same real wired `shutdown_signal` as external sources: cancelling their
        // begun-trigger resolves the source's `_ = &mut shutdown` arm, it drains and drops its
        // `ShutdownSignalToken`, and `shutdown_complete_tripwire` fires — so wave 2 completes on
        // natural source completion rather than the force deadline.
        (shutdown_signal, force_shutdown_tripwire)
    }

    /// Takes ownership of all internal state for the given source from another `ShutdownCoordinator`.
    ///
    /// # Panics
    ///
    /// Panics if the other coordinator already had its triggers removed.
    pub fn takeover_source(&mut self, id: &ComponentKey, other: &mut Self) {
        let existing = self.begun_triggers.insert(
            id.clone(),
            other.begun_triggers.remove(id).unwrap_or_else(|| {
                panic!(
                    "Other ShutdownCoordinator didn't have a shutdown_begun_trigger for \"{id}\""
                )
            }),
        );
        assert!(
            existing.is_none(),
            "ShutdownCoordinator already has a shutdown_begin_trigger for source \"{id}\""
        );

        let existing = self.force_triggers.insert(
            id.clone(),
            other.force_triggers.remove(id).unwrap_or_else(|| {
                panic!(
                    "Other ShutdownCoordinator didn't have a shutdown_force_trigger for \"{id}\""
                )
            }),
        );
        assert!(
            existing.is_none(),
            "ShutdownCoordinator already has a shutdown_force_trigger for source \"{id}\""
        );

        let existing = self.complete_tripwires.insert(
            id.clone(),
            other
                .complete_tripwires
                .remove(id)
                .unwrap_or_else(|| {
                    panic!(
                        "Other ShutdownCoordinator didn't have a shutdown_complete_tripwire for \"{id}\""
                    )
                }),
        );
        assert!(
            existing.is_none(),
            "ShutdownCoordinator already has a shutdown_complete_tripwire for source \"{id}\""
        );
    }

    /// Sends a signal to begin shutting down to all sources, and returns a future that
    /// resolves once all sources have either shut down completely, or have been sent the
    /// force shutdown signal.  The force shutdown signal will be sent to any sources that
    /// don't cleanly shut down before the given `deadline`.
    ///
    /// Non-deferred (external) sources are shut down first, then deferred (internal) sources.
    ///
    /// # Panics
    ///
    /// Panics if this coordinator has had its triggers removed (ie
    /// has been taken over with `Self::takeover_source`).
    pub fn shutdown_all(self, deadline: Option<Instant>) -> impl Future<Output = ()> {
        let (wave1_future, deferred) = self.shutdown_non_deferred(deadline);
        async move {
            wave1_future.await;
            deferred.shutdown_all(deadline).await;
        }
    }

    /// Sends a signal to begin shutting down only non-internal (data) sources, and returns:
    /// 1. A future that resolves once all non-internal sources have completed or been force-shutdown
    /// 2. A `DeferredSourceShutdowns` handle for shutting down internal sources later
    ///
    /// This enables a two-wave shutdown: first shut down data sources, then (after transforms/sinks
    /// have drained) shut down internal sources.
    ///
    /// # Panics
    ///
    /// Panics if this coordinator has had its triggers removed.
    pub fn shutdown_non_deferred(
        self,
        deadline: Option<Instant>,
    ) -> (impl Future<Output = ()>, DeferredSourceShutdowns) {
        let mut external_sources_complete_futures = Vec::new();
        let mut deferred_begun_triggers = HashMap::new();
        let mut deferred_force_triggers = HashMap::new();
        let mut deferred_complete_tripwires = HashMap::new();

        let mut shutdown_complete_tripwires = self.complete_tripwires;
        let mut shutdown_force_triggers = self.force_triggers;

        for (id, (internal, trigger)) in self.begun_triggers {
            let shutdown_complete_tripwire =
                shutdown_complete_tripwires.remove(&id).unwrap_or_else(|| {
                    panic!(
                        "shutdown_complete_tripwire for source \"{id}\" not found in the ShutdownCoordinator"
                    )
                });
            let shutdown_force_trigger = shutdown_force_triggers.remove(&id).unwrap_or_else(|| {
                panic!(
                    "shutdown_force_trigger for source \"{id}\" not found in the ShutdownCoordinator"
                )
            });

            if internal {
                deferred_begun_triggers.insert(id.clone(), trigger);
                deferred_force_triggers.insert(id.clone(), shutdown_force_trigger);
                deferred_complete_tripwires.insert(id, shutdown_complete_tripwire);
            } else {
                trigger.cancel();
                external_sources_complete_futures.push(
                    SourceShutdownCoordinator::shutdown_source_complete(
                        shutdown_complete_tripwire,
                        shutdown_force_trigger,
                        id,
                        deadline,
                    ),
                );
            }
        }

        let wave1_future = futures::future::join_all(external_sources_complete_futures).map(|_| ());

        let deferred = DeferredSourceShutdowns {
            begun_triggers: deferred_begun_triggers,
            force_triggers: deferred_force_triggers,
            complete_tripwires: deferred_complete_tripwires,
        };

        (wave1_future, deferred)
    }

    /// Sends the signal to the given source to begin shutting down. Returns a future that resolves
    /// when the source has finished shutting down cleanly or been sent the force shutdown signal.
    /// The returned future resolves to a bool that indicates if the source shut down cleanly before
    /// the given `deadline`. If the result is false then that means the source failed to shut down
    /// before `deadline` and had to be force-shutdown.
    ///
    /// # Panics
    ///
    /// Panics if this coordinator has had its triggers removed (ie
    /// has been taken over with `Self::takeover_source`).
    pub fn shutdown_source(
        &mut self,
        id: &ComponentKey,
        deadline: Instant,
    ) -> impl Future<Output = bool> + use<> {
        let (_, begin_shutdown_trigger) = self.begun_triggers.remove(id).unwrap_or_else(|| {
            panic!(
                "shutdown_begun_trigger for source \"{id}\" not found in the ShutdownCoordinator"
            )
        });
        // This is what actually triggers the source to begin shutting down.
        begin_shutdown_trigger.cancel();

        let shutdown_complete_tripwire = self
            .complete_tripwires
            .remove(id)
            .unwrap_or_else(|| {
                panic!(
                "shutdown_complete_tripwire for source \"{id}\" not found in the ShutdownCoordinator"
            )
            });
        let shutdown_force_trigger = self.force_triggers.remove(id).unwrap_or_else(|| {
            panic!(
                "shutdown_force_trigger for source \"{id}\" not found in the ShutdownCoordinator"
            )
        });
        SourceShutdownCoordinator::shutdown_source_complete(
            shutdown_complete_tripwire,
            shutdown_force_trigger,
            id.clone(),
            Some(deadline),
        )
    }

    /// Returned future will finish once all *current* sources have finished.
    #[must_use]
    pub fn shutdown_tripwire(&self) -> future::BoxFuture<'static, ()> {
        let futures = self
            .complete_tripwires
            .values()
            .cloned()
            .map(|tripwire| tripwire.then(tripwire_handler).boxed());

        future::join_all(futures)
            .map(|_| info!("All sources have finished."))
            .boxed()
    }

    fn shutdown_source_complete(
        shutdown_complete_tripwire: Tripwire,
        shutdown_force_trigger: Trigger,
        id: ComponentKey,
        deadline: Option<Instant>,
    ) -> impl Future<Output = bool> {
        async move {
            let fut = shutdown_complete_tripwire.then(tripwire_handler);
            if let Some(deadline) = deadline {
                // Call `shutdown_force_trigger.disable()` on drop.
                let shutdown_force_trigger = DisabledTrigger::new(shutdown_force_trigger);
                if timeout_at(deadline, fut).await.is_ok() {
                    shutdown_force_trigger.into_inner().disable();
                    true
                } else {
                    error!(
                        "Source '{}' failed to shutdown before deadline. Forcing shutdown.",
                        id,
                    );
                    shutdown_force_trigger.into_inner().cancel();
                    false
                }
            } else {
                fut.await;
                true
            }
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use tokio::time::{Duration, Instant};

    use super::*;
    use crate::shutdown::SourceShutdownCoordinator;

    #[tokio::test]
    async fn shutdown_coordinator_shutdown_source_clean() {
        let mut shutdown = SourceShutdownCoordinator::default();
        let id = ComponentKey::from("test");

        let (shutdown_signal, _) = shutdown.register_source(&id, false);

        let deadline = Instant::now() + Duration::from_secs(1);
        let shutdown_complete = shutdown.shutdown_source(&id, deadline);

        drop(shutdown_signal);

        let success = shutdown_complete.await;
        assert!(success);
    }

    #[tokio::test]
    async fn shutdown_coordinator_shutdown_source_force() {
        let mut shutdown = SourceShutdownCoordinator::default();
        let id = ComponentKey::from("test");

        let (_shutdown_signal, force_shutdown_tripwire) = shutdown.register_source(&id, false);

        let deadline = Instant::now() + Duration::from_secs(1);
        let shutdown_complete = shutdown.shutdown_source(&id, deadline);

        // Since we never drop the `ShutdownSignal` the `ShutdownCoordinator` assumes the Source is
        // still running and must force shutdown.
        let success = shutdown_complete.await;
        assert!(!success);

        let finished = futures::poll!(force_shutdown_tripwire.boxed());
        assert_eq!(finished, Poll::Ready(()));
    }

    #[tokio::test]
    async fn shutdown_non_deferred_only_shuts_down_external_sources() {
        let mut shutdown = SourceShutdownCoordinator::default();
        let external_id = ComponentKey::from("external");
        let internal_id = ComponentKey::from("internal");

        let (external_signal, _) = shutdown.register_source(&external_id, false);
        let (_internal_signal, _internal_force) = shutdown.register_source(&internal_id, true);

        let deadline = Instant::now() + Duration::from_secs(1);
        let (wave1_future, deferred) = shutdown.shutdown_non_deferred(Some(deadline));

        // The deferred handle should contain the internal source.
        assert!(deferred.has_deferred_sources());
        assert!(deferred.deferred_keys().contains(&internal_id));
        assert!(!deferred.deferred_keys().contains(&external_id));

        // Drop the external signal to simulate clean shutdown.
        drop(external_signal);

        // Wave 1 should complete since the external source shut down.
        wave1_future.await;

        // Deferred sources can be shut down in a second wave.
        let wave2_deadline = Instant::now() + Duration::from_secs(1);
        deferred.shutdown_all(Some(wave2_deadline)).await;
    }

    #[tokio::test]
    async fn shutdown_non_deferred_no_internal_sources() {
        let mut shutdown = SourceShutdownCoordinator::default();
        let ext1 = ComponentKey::from("ext1");
        let ext2 = ComponentKey::from("ext2");

        let (signal1, _) = shutdown.register_source(&ext1, false);
        let (signal2, _) = shutdown.register_source(&ext2, false);

        let deadline = Instant::now() + Duration::from_secs(1);
        let (wave1_future, deferred) = shutdown.shutdown_non_deferred(Some(deadline));

        // No deferred sources.
        assert!(!deferred.has_deferred_sources());

        // Drop signals to simulate clean shutdown.
        drop(signal1);
        drop(signal2);

        wave1_future.await;
        // Deferred shutdown with no sources should complete immediately.
        deferred
            .shutdown_all(Some(Instant::now() + Duration::from_secs(1)))
            .await;
    }

    #[tokio::test]
    async fn shutdown_non_deferred_external_force_shutdown() {
        let mut shutdown = SourceShutdownCoordinator::default();
        let external_id = ComponentKey::from("external");
        let internal_id = ComponentKey::from("internal");

        // Keep external signal alive to trigger force shutdown.
        let (_external_signal, external_force) = shutdown.register_source(&external_id, false);
        let (_internal_signal, _internal_force) = shutdown.register_source(&internal_id, true);

        // Very short deadline to trigger force shutdown quickly.
        let deadline = Instant::now() + Duration::from_millis(100);
        let (wave1_future, deferred) = shutdown.shutdown_non_deferred(Some(deadline));

        // Wave 1 should complete after force-shutting down the external source.
        wave1_future.await;

        // External source should have been force-shutdown (tripwire resolved).
        let finished = futures::poll!(external_force.boxed());
        assert_eq!(finished, Poll::Ready(()));

        // Deferred sources still need their own shutdown.
        assert!(deferred.has_deferred_sources());
        let wave2_deadline = Instant::now() + Duration::from_millis(100);
        deferred.shutdown_all(Some(wave2_deadline)).await;
    }

    #[tokio::test]
    async fn deferred_internal_source_completes_on_token_drop_not_force() {
        // A deferred (internal) source's wave-2 shutdown must complete when the source drains and
        // drops its `ShutdownSignalToken`, not only via the force-trigger at the deadline.
        let mut shutdown = SourceShutdownCoordinator::default();
        let internal_id = ComponentKey::from("internal_metrics");

        // Keep the signal alive, as a running source holds it for its whole lifetime.
        let (internal_signal, _internal_force) = shutdown.register_source(&internal_id, true);

        // No external sources, so wave 1 completes immediately and the internal source defers.
        let deadline = Instant::now() + Duration::from_secs(1);
        let (wave1_future, deferred) = shutdown.shutdown_non_deferred(Some(deadline));
        wave1_future.await;
        assert!(deferred.deferred_keys().contains(&internal_id));

        // Far-future deadline: wave-2 can only finish quickly by natural completion, so a prompt
        // resolve below distinguishes the wired signal from a deadline force-abort.
        let far_deadline = Instant::now() + Duration::from_secs(3600);
        let mut shutdown_all = Box::pin(deferred.shutdown_all(Some(far_deadline)));

        // Source still holds its token, so wave-2 must be pending (not completed at registration).
        assert!(
            futures::poll!(&mut shutdown_all).is_pending(),
            "wave-2 shutdown completed before the internal source finished -- its \
             completion is not wired to the source's real shutdown signal"
        );

        // Source finishes its drain and drops its token; wave-2 should resolve promptly.
        let start = Instant::now();
        drop(internal_signal);
        shutdown_all.await;
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "wave-2 shutdown did not complete promptly after the internal source \
             finished; it appears to be waiting on the force deadline instead of \
             natural completion"
        );
    }

    /// Guard the single-pass path (`SourceShutdownCoordinator::shutdown_all`).
    ///
    /// When `two_wave_shutdown` is disabled, `running.rs` calls the combined
    /// `SourceShutdownCoordinator::shutdown_all` rather than the `shutdown_non_deferred` +
    /// `DeferredSourceShutdowns::shutdown_all` pair. This confirms internal sources receiving the
    /// real wired signal does not hang or regress that path: external sources complete in wave 1,
    /// internal sources in wave 2, and `shutdown_all` resolves on the internal source dropping its
    /// token — well before the deadline, no force-trigger required.
    #[tokio::test]
    async fn single_pass_shutdown_all_internal_source_completes_naturally() {
        let mut coordinator = SourceShutdownCoordinator::default();
        let external_id = ComponentKey::from("data_source");
        let internal_id = ComponentKey::from("internal_metrics");

        let (external_signal, _ext_force) = coordinator.register_source(&external_id, false);
        // Keep the internal signal alive, simulating a running internal source.
        let (internal_signal, _int_force) = coordinator.register_source(&internal_id, true);

        // Use a far deadline: if the internal source is NOT properly wired, the future hangs
        // to the deadline; if it IS wired it completes as soon as the source drops its token.
        let far_deadline = Instant::now() + Duration::from_secs(3600);
        let mut shutdown_all_fut = Box::pin(coordinator.shutdown_all(Some(far_deadline)));

        // Pending while the external (wave-1) source is still held.
        assert!(
            futures::poll!(&mut shutdown_all_fut).is_pending(),
            "single-pass shutdown_all completed while the external source was still running"
        );

        // External source finishes → wave 1 drains, wave 2 cancels the internal begun-trigger.
        drop(external_signal);

        // Still pending: the internal source hasn't dropped its token, so its completion tripwire
        // hasn't fired. (A no-op signal would have fired it at registration, resolving here early.)
        assert!(
            futures::poll!(&mut shutdown_all_fut).is_pending(),
            "single-pass shutdown_all completed before the internal source finished -- \
             the internal source's completion is not wired to the real shutdown signal"
        );

        // Internal source finishes its drain; shutdown_all should resolve promptly (not at deadline).
        let start = Instant::now();
        drop(internal_signal);
        shutdown_all_fut.await;
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "single-pass shutdown_all did not complete promptly after all sources finished; \
             it appears to be waiting on the force deadline instead of natural completion"
        );
    }

    #[tokio::test]
    async fn two_wave_shutdown_timing() {
        // Verify that wave 1 completes before wave 2 starts.
        let mut shutdown = SourceShutdownCoordinator::default();
        let external_id = ComponentKey::from("data_source");
        let internal_id = ComponentKey::from("internal_logs");

        let (external_signal, _) = shutdown.register_source(&external_id, false);
        let (_internal_signal, _internal_force) = shutdown.register_source(&internal_id, true);

        let data_deadline = Instant::now() + Duration::from_secs(2);
        let (wave1_future, deferred) = shutdown.shutdown_non_deferred(Some(data_deadline));

        let start = Instant::now();

        // Drop external signal immediately (clean shutdown).
        drop(external_signal);
        wave1_future.await;

        let wave1_elapsed = start.elapsed();
        // Wave 1 should complete almost immediately (well under 1 second).
        assert!(wave1_elapsed < Duration::from_secs(1));

        // Wave 2 uses remaining main deadline.
        let main_deadline = Instant::now() + Duration::from_millis(100);
        deferred.shutdown_all(Some(main_deadline)).await;
    }
}
