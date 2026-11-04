//! Source admission using the same controller as adaptive sink requests.

use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use tokio::sync::OwnedSemaphorePermit;

use super::{AdaptiveConcurrencySettings, controller::Controller, instant_now};

/// Each source adapts its own polling and processing concurrency.
#[derive(Clone)]
pub(crate) struct SourceController(Arc<Controller<()>>, Arc<Mutex<u64>>);

impl SourceController {
    pub(crate) fn new(initial: usize, maximum: usize) -> Self {
        let settings = AdaptiveConcurrencySettings {
            initial_concurrency: initial,
            max_concurrency_limit: maximum,
            ..Default::default()
        };
        Self(
            Arc::new(Controller::new(None, settings, ())),
            Arc::new(Mutex::new(0)),
        )
    }

    pub(crate) async fn acquire(&self) -> SourcePermit {
        SourcePermit {
            controller: Arc::clone(&self.0),
            _permit: self.0.acquire().await,
            start: None,
            failures: Arc::clone(&self.1),
            failures_at_start: 0,
        }
    }

    pub(crate) fn current_limit(&self) -> usize {
        self.0.current_limit()
    }

    pub(crate) fn clamp_to_one(&self) {
        let _failures = self.1.lock().expect("Source feedback mutex poisoned");
        self.0.clamp_to_one();
    }

    pub(crate) fn note_failed_delivery(&self) {
        let mut failures = self.1.lock().expect("Source feedback mutex poisoned");
        *failures += 1;
        self.0.clamp_to_one();
    }

    #[cfg(test)]
    pub(crate) fn has_rtt_measurement(&self) -> bool {
        self.0.has_rtt_measurement()
    }
}

/// A lease on source capacity, including cancellation-safe processing accounting.
pub(crate) struct SourcePermit {
    controller: Arc<Controller<()>>,
    _permit: OwnedSemaphorePermit,
    start: Option<Instant>,
    failures: Arc<Mutex<u64>>,
    failures_at_start: u64,
}

impl SourcePermit {
    /// Empty receives never start a request or contribute latency samples.
    pub(crate) fn start(&mut self) {
        assert!(self.start.is_none(), "source request already started");
        let failures = self
            .failures
            .lock()
            .expect("Source feedback mutex poisoned");
        self.controller.start_request();
        self.failures_at_start = *failures;
        self.start = Some(instant_now());
    }

    pub(crate) fn finish(&mut self, back_pressure: bool, delivered: bool) -> bool {
        if let Some(start) = self.start.take() {
            // A cloud's delete-on-failed-ack policy can return Ok without delivery.
            // Suppress samples from requests overlapping that source's pressure.
            // Keep validation and adjustment atomic with respect to failure feedback.
            let failures = self
                .failures
                .lock()
                .expect("Source feedback mutex poisoned");
            let delivered = delivered && self.failures_at_start == *failures;
            // Explicit pressure must also take effect before the first good RTT.
            if back_pressure {
                self.controller.clamp_to_one();
            }
            if back_pressure || delivered {
                self.controller
                    .adjust_to_response_inner(start, back_pressure, delivered);
            } else {
                self.controller.cancel_request();
            }
            delivered
        } else {
            false
        }
    }
}

impl Drop for SourcePermit {
    fn drop(&mut self) {
        self.finish(false, false);
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use tokio::time::{Duration, advance};

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn empty_receives_and_cancellation_are_neutral() {
        let controller = SourceController::new(1, 4);
        let permit = controller.acquire().await;
        assert!(controller.acquire().now_or_never().is_none());
        drop(permit);
        let mut permit = controller.acquire().await;
        permit.start();
        assert_eq!(controller.0.load(), 1.0);
        advance(Duration::from_secs(10)).await;
        drop(permit);
        assert_eq!(controller.0.load(), 0.0);
        assert_eq!(controller.current_limit(), 1);
        assert!(controller.acquire().now_or_never().is_some());
    }

    async fn complete_wave(controller: &SourceController, duration: Duration) {
        let mut permits = Vec::new();
        for _ in 0..controller.current_limit() {
            let mut permit = controller.acquire().await;
            permit.start();
            permits.push(permit);
        }
        advance(duration).await;
        for mut permit in permits {
            permit.finish(false, true);
        }
        assert_eq!(controller.0.load(), 0.0);
    }

    #[tokio::test(start_paused = true)]
    async fn grows_to_bound_and_explicit_pressure_clamps_before_first_rtt() {
        let controller = SourceController::new(2, 4);
        let mut permit = controller.acquire().await;
        permit.start();
        permit.finish(true, false);
        drop(permit);
        assert_eq!(controller.current_limit(), 1);
        let mut reached_maximum = false;
        for _ in 0..12 {
            complete_wave(&controller, Duration::from_secs(1)).await;
            reached_maximum |= controller.current_limit() == 4;
            assert!((1..=4).contains(&controller.current_limit()));
        }
        assert!(reached_maximum);
        controller.clamp_to_one();
        assert_eq!(controller.current_limit(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn mixed_object_durations_remain_bounded_and_can_recover() {
        let controller = SourceController::new(2, 4);
        for wave in 0..20 {
            let millis = if wave % 2 == 0 { 100 } else { 1_000 };
            complete_wave(&controller, Duration::from_millis(millis)).await;
            assert!((1..=4).contains(&controller.current_limit()));
        }
        // Size variation may temporarily reduce ARC; it must not prevent recovery.
        let mut recovered = false;
        for _ in 0..100 {
            complete_wave(&controller, Duration::from_millis(100)).await;
            recovered |= controller.current_limit() == 4;
        }
        assert!(recovered);
    }

    #[tokio::test(start_paused = true)]
    async fn failure_after_rtt_history_suppresses_overlapping_success() {
        let controller = SourceController::new(2, 4);
        complete_wave(&controller, Duration::from_secs(1)).await;
        assert!(controller.has_rtt_measurement());
        let mut permit = controller.acquire().await;
        permit.start();
        advance(Duration::from_secs(1)).await;
        controller.note_failed_delivery();
        assert!(!permit.finish(false, true));
        assert_eq!(controller.current_limit(), 1);
        assert_eq!(controller.0.load(), 0.0);
    }

    #[tokio::test(start_paused = true)]
    async fn failure_cannot_interleave_with_successful_controller_adjustment() {
        let runtime = tokio::runtime::Handle::current();
        // Establish that this completion is due to grow the limit on an OS thread.
        let control = SourceController::new(2, 4);
        complete_wave(&control, Duration::from_secs(1)).await;
        let mut successful = control.acquire().await;
        successful.start();
        advance(Duration::from_secs(1)).await;
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let _runtime = runtime.enter();
                    assert!(successful.finish(false, true));
                })
                .join()
                .unwrap();
        });
        assert_eq!(control.current_limit(), 3);

        let controller = SourceController::new(2, 4);
        complete_wave(&controller, Duration::from_secs(1)).await;
        let mut permit = controller.acquire().await;
        permit.start();
        advance(Duration::from_secs(1)).await;

        // Stop completion at the controller lock, after it validates the generation.
        let inner = controller.0.inner.lock().unwrap();
        std::thread::scope(|scope| {
            let runtime = &runtime;
            let success = scope.spawn(move || {
                let _runtime = runtime.enter();
                permit.finish(false, true)
            });
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while controller.1.try_lock().is_ok() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "completion never acquired feedback lock"
                );
                std::thread::yield_now();
            }
            let failure = scope.spawn(|| controller.note_failed_delivery());
            drop(inner);
            assert!(success.join().unwrap());
            failure.join().unwrap();
        });
        assert_eq!(controller.current_limit(), 1);
        assert_eq!(controller.0.load(), 0.0);
    }

    #[tokio::test(start_paused = true)]
    async fn delete_on_failed_ack_does_not_train_a_success() {
        let controller = SourceController::new(2, 4);
        let mut permit = controller.acquire().await;
        permit.start();
        advance(Duration::from_secs(1)).await;
        controller.note_failed_delivery();
        assert_eq!(controller.current_limit(), 1);
        assert!(!permit.finish(false, true));
        assert_eq!(controller.0.load(), 0.0);
        assert_eq!(controller.current_limit(), 1);
    }
}
