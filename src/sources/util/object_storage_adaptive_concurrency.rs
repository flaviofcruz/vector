//! Opt-in per-queue ARC and process-wide active-object admission.
//!
//! Reserve capacity before receiving a notification, so local contention cannot
//! consume cloud redelivery attempts. Adaptive sources use bounded short polls.
//! The memory watermark stops new work; it cannot preempt or reserve bytes for
//! an already-admitted object.

use std::{
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use vector_lib::configurable::configurable_component;

use crate::{
    internal_events::{ObjectStorageAdaptiveCompletion, ObjectStorageAdaptiveConcurrency},
    sinks::util::adaptive_concurrency::source::{SourceController, SourcePermit},
};

const SAMPLE_INTERVAL: Duration = Duration::from_millis(250);
pub const ADAPTIVE_POLL_TIMEOUT: Duration = Duration::from_secs(5);
pub const ADAPTIVE_POLL_BACKOFF: Duration = Duration::from_secs(1);
static POD_GUARD: Mutex<Weak<PodGuard>> = Mutex::new(Weak::new());

/// Adaptive admission for a blob source. Absence preserves fixed concurrency.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct ObjectStorageAdaptiveConcurrencyConfig {
    /// Starting per-source limit, bounded by `client_concurrency`.
    #[serde(default = "default_initial_concurrency")]
    pub initial_concurrency: NonZeroUsize,

    /// Maximum notifications being polled or processed across adaptive sources.
    pub max_active_objects: NonZeroUsize,

    /// Stop admitting work at this cgroup memory usage, in bytes.
    pub memory_high_watermark_bytes: NonZeroU64,

    /// Resume admitting work below this cgroup memory usage, in bytes.
    pub memory_low_watermark_bytes: NonZeroU64,

    /// Cgroup usage file. For cgroup v1, configure `memory.usage_in_bytes`.
    #[serde(default = "default_memory_usage_path")]
    pub memory_usage_path: PathBuf,
}

const fn default_initial_concurrency() -> NonZeroUsize {
    NonZeroUsize::new(1).unwrap()
}

fn default_memory_usage_path() -> PathBuf {
    "/sys/fs/cgroup/memory.current".into()
}

#[derive(Debug, PartialEq, Eq)]
struct PodLimits {
    maximum: usize,
    high: u64,
    low: u64,
    usage_path: PathBuf,
}

struct MemoryState {
    sampled_at: Instant,
    pressured: bool,
}

impl MemoryState {
    const fn observe(&mut self, usage: Option<u64>, limits: &PodLimits) -> bool {
        self.pressured = match usage {
            None => true,
            Some(usage) if self.pressured => usage >= limits.low,
            Some(usage) => usage >= limits.high,
        };
        !self.pressured
    }
}

struct PodGuard {
    limits: PodLimits,
    active: Arc<Semaphore>,
    processing: AtomicUsize,
    memory: Mutex<MemoryState>,
}

impl PodGuard {
    fn new(limits: PodLimits) -> Result<Self, String> {
        let usage = Self::read_usage(&limits)?;
        let mut memory = MemoryState {
            sampled_at: Instant::now(),
            pressured: false,
        };
        memory.observe(Some(usage), &limits);
        Ok(Self {
            active: Arc::new(Semaphore::new(limits.maximum)),
            processing: AtomicUsize::new(0),
            limits,
            memory: Mutex::new(memory),
        })
    }

    fn read_usage(limits: &PodLimits) -> Result<u64, String> {
        std::fs::read_to_string(&limits.usage_path)
            .map_err(|error| format!("Cannot read adaptive memory usage: {error}"))?
            .trim()
            .parse()
            .map_err(|error| format!("Invalid adaptive memory usage: {error}"))
    }

    fn healthy(&self) -> bool {
        let mut memory = self.memory.lock().expect("Memory guard mutex poisoned");
        if memory.sampled_at.elapsed() >= SAMPLE_INTERVAL {
            let usage = Self::read_usage(&self.limits).ok();
            memory.observe(usage, &self.limits);
            memory.sampled_at = Instant::now();
        }
        !memory.pressured
    }

    fn active_count(&self) -> usize {
        self.processing.load(Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub struct AdaptiveObjectStorageConcurrency {
    controller: SourceController,
    pod: Arc<PodGuard>,
}

impl AdaptiveObjectStorageConcurrency {
    #[cfg(test)]
    pub(crate) fn for_test(
        initial: usize,
        maximum: usize,
        max_active: usize,
    ) -> (Self, tempfile::NamedTempFile) {
        use std::io::Write;

        let mut usage = tempfile::NamedTempFile::new().unwrap();
        write!(usage, "1").unwrap();
        let pod = PodGuard::new(PodLimits {
            maximum: max_active,
            high: 100,
            low: 50,
            usage_path: usage.path().into(),
        })
        .unwrap();
        (
            Self {
                controller: SourceController::new(initial, maximum),
                pod: Arc::new(pod),
            },
            usage,
        )
    }

    #[cfg(test)]
    pub(crate) fn sibling_for_test(&self, initial: usize, maximum: usize) -> Self {
        Self {
            controller: SourceController::new(initial, maximum),
            pod: Arc::clone(&self.pod),
        }
    }

    pub fn build(
        config: Option<&ObjectStorageAdaptiveConcurrencyConfig>,
        maximum: usize,
        batch_size: u64,
    ) -> Result<Option<Self>, String> {
        let Some(config) = config else {
            return Ok(None);
        };
        if maximum == 0
            || maximum > Semaphore::MAX_PERMITS
            || config.max_active_objects.get() > Semaphore::MAX_PERMITS
        {
            return Err("adaptive concurrency exceeds supported semaphore bounds".into());
        }
        if config.initial_concurrency.get() > maximum {
            return Err("adaptive initial_concurrency exceeds client_concurrency".into());
        }
        if batch_size != 1 {
            return Err("adaptive concurrency requires max_number_of_messages = 1".into());
        }
        if config.memory_low_watermark_bytes >= config.memory_high_watermark_bytes {
            return Err("adaptive memory low watermark must be below high watermark".into());
        }
        let limits = PodLimits {
            maximum: config.max_active_objects.get(),
            high: config.memory_high_watermark_bytes.get(),
            low: config.memory_low_watermark_bytes.get(),
            usage_path: config.memory_usage_path.clone(),
        };
        let mut shared = POD_GUARD.lock().expect("Pod guard registry mutex poisoned");
        let pod = match shared.upgrade() {
            Some(pod) if pod.limits != limits => {
                return Err("all adaptive sources must use identical pod-wide limits".into());
            }
            Some(pod) => pod,
            None => {
                let pod = Arc::new(PodGuard::new(limits)?);
                *shared = Arc::downgrade(&pod);
                pod
            }
        };
        Ok(Some(Self {
            controller: SourceController::new(config.initial_concurrency.get(), maximum),
            pod,
        }))
    }

    /// No queue message/visibility lease exists while waiting here.
    pub async fn acquire(&self) -> AdaptiveAdmission {
        loop {
            if !self.pod.healthy() {
                self.controller.clamp_to_one();
                self.emit();
                tokio::time::sleep(SAMPLE_INTERVAL).await;
                continue;
            }
            let permit = self.controller.acquire().await;
            let active = Arc::clone(&self.pod.active)
                .acquire_owned()
                .await
                .expect("Shared admission semaphore must remain open");
            // No visibility lease exists while either permit is pending.
            if self.pod.healthy() {
                self.emit();
                return AdaptiveAdmission {
                    owner: self.clone(),
                    permit,
                    active: Some(active),
                    started: None,
                };
            }
        }
    }

    fn emit(&self) {
        emit!(ObjectStorageAdaptiveConcurrency {
            limit: self.controller.current_limit(),
            active_objects: self.pod.active_count(),
            memory_pressure: !self.pod.healthy(),
        });
    }

    /// Delivery failure is independent of a cloud's delete-on-failure policy.
    pub fn note_failed_delivery(&self) {
        self.controller.note_failed_delivery();
    }

    #[cfg(test)]
    pub(crate) fn test_limit(&self) -> usize {
        self.controller.current_limit()
    }

    #[cfg(test)]
    pub(crate) fn test_has_rtt(&self) -> bool {
        self.controller.has_rtt_measurement()
    }
}

pub struct AdaptiveAdmission {
    owner: AdaptiveObjectStorageConcurrency,
    permit: SourcePermit,
    active: Option<OwnedSemaphorePermit>,
    started: Option<Instant>,
}

impl AdaptiveAdmission {
    /// Disabled-table deletion does not consume object-processing capacity.
    pub fn release_for_drain(&mut self) {
        assert!(self.started.is_none(), "cannot drain active processing");
        self.active.take();
    }

    /// The receive already reserved capacity; later pressure blocks subsequent polls.
    pub fn start_processing(&mut self) {
        assert!(self.active.is_some(), "processing requires a reservation");
        self.permit.start();
        self.started = Some(Instant::now());
        self.owner.pod.processing.fetch_add(1, Ordering::Relaxed);
        self.owner.emit();
    }

    pub fn finish(mut self, back_pressure: bool, delivered: bool, decoded_bytes: u64) {
        let delivered = self.permit.finish(back_pressure, delivered);
        if let Some(started) = self.started {
            emit!(ObjectStorageAdaptiveCompletion {
                elapsed: started.elapsed(),
                decoded_bytes,
                delivered,
            });
        }
    }
}

impl Drop for AdaptiveAdmission {
    fn drop(&mut self) {
        if self.started.is_some() {
            self.owner.pod.processing.fetch_sub(1, Ordering::Relaxed);
        }
        self.active.take();
        self.owner.emit();
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use tokio::time::advance;

    use super::*;

    fn config(path: PathBuf) -> ObjectStorageAdaptiveConcurrencyConfig {
        ObjectStorageAdaptiveConcurrencyConfig {
            initial_concurrency: NonZeroUsize::new(2).unwrap(),
            max_active_objects: NonZeroUsize::new(4).unwrap(),
            memory_high_watermark_bytes: NonZeroU64::new(100).unwrap(),
            memory_low_watermark_bytes: NonZeroU64::new(50).unwrap(),
            memory_usage_path: path,
        }
    }

    #[test]
    fn default_off_does_not_read_cgroup_or_change_batch_limits() {
        assert!(
            AdaptiveObjectStorageConcurrency::build(None, 2, 10)
                .unwrap()
                .is_none()
        );
    }

    async fn rejects_unacked_config<T>(queue_key: &str, mut queue: serde_json::Value)
    where
        T: crate::config::SourceConfig + Default + serde::Serialize + serde::de::DeserializeOwned,
    {
        let mut value = serde_json::to_value(T::default()).unwrap();
        assert!(value[queue_key]["adaptive_concurrency"].is_null());
        let _: T = serde_json::from_value(value.clone()).unwrap();
        queue["adaptive_concurrency"] =
            serde_json::to_value(config("/must/not/be/read".into())).unwrap();
        queue["max_number_of_messages"] = 1.into();
        value[queue_key] = queue;
        value["acknowledgements"] = false.into();
        let source: T = serde_json::from_value(value).unwrap();
        let (out, _rx) = crate::SourceSender::new_test();
        let context = crate::config::SourceContext::new_test(out, None);
        match source.build(context).await {
            Err(error) => assert!(
                error
                    .to_string()
                    .contains("requires end-to-end acknowledgements")
            ),
            Ok(_) => panic!("adaptive source must reject disabled acknowledgements"),
        }
    }

    #[cfg(feature = "sources-aws_s3")]
    #[tokio::test]
    async fn aws_adaptive_requires_acknowledgements_and_defaults_off() {
        rejects_unacked_config::<crate::sources::aws_s3::AwsS3Config>(
            "sqs",
            serde_json::json!({"queue_url": "https://example.invalid/q"}),
        )
        .await;
    }

    #[cfg(feature = "sources-azure_blob")]
    #[tokio::test]
    async fn azure_adaptive_requires_acknowledgements_and_defaults_off() {
        rejects_unacked_config::<crate::sources::azure_blob::AzureBlobConfig>(
            "queue",
            serde_json::json!({"queue_name": "q"}),
        )
        .await;
    }

    #[cfg(feature = "sources-gcp_gcs")]
    #[tokio::test]
    async fn gcp_adaptive_requires_acknowledgements_and_defaults_off() {
        rejects_unacked_config::<crate::sources::gcp_gcs::GcpGcsConfig>(
            "pubsub",
            serde_json::json!({"subscription": "test-sub"}),
        )
        .await;
    }

    #[test]
    fn rejects_invalid_configuration() {
        let (_unused, usage) = AdaptiveObjectStorageConcurrency::for_test(1, 4, 4);
        let mut config = config(usage.path().into());
        for (maximum, batch_size, expected) in [
            (
                1,
                1,
                "adaptive initial_concurrency exceeds client_concurrency",
            ),
            (
                0,
                1,
                "adaptive concurrency exceeds supported semaphore bounds",
            ),
            (
                usize::MAX,
                1,
                "adaptive concurrency exceeds supported semaphore bounds",
            ),
            (
                2,
                10,
                "adaptive concurrency requires max_number_of_messages = 1",
            ),
        ] {
            assert_eq!(
                AdaptiveObjectStorageConcurrency::build(Some(&config), maximum, batch_size).err(),
                Some(expected.into()),
            );
        }
        config.memory_low_watermark_bytes = config.memory_high_watermark_bytes;
        assert_eq!(
            AdaptiveObjectStorageConcurrency::build(Some(&config), 2, 1).err(),
            Some("adaptive memory low watermark must be below high watermark".into()),
        );
        config.memory_low_watermark_bytes = NonZeroU64::new(50).unwrap();
        config.max_active_objects = NonZeroUsize::new(usize::MAX).unwrap();
        assert_eq!(
            AdaptiveObjectStorageConcurrency::build(Some(&config), 2, 1).err(),
            Some("adaptive concurrency exceeds supported semaphore bounds".into()),
        );
        config.max_active_objects = NonZeroUsize::new(4).unwrap();
        for field in [
            "initial_concurrency",
            "max_active_objects",
            "memory_high_watermark_bytes",
            "memory_low_watermark_bytes",
        ] {
            let mut json = serde_json::to_value(&config).unwrap();
            json[field] = 0.into();
            assert!(
                serde_json::from_value::<ObjectStorageAdaptiveConcurrencyConfig>(json).is_err()
            );
        }
    }

    #[test]
    fn unreadable_initial_memory_usage_fails_configuration() {
        let directory = tempfile::tempdir().unwrap();
        let result = PodGuard::new(PodLimits {
            maximum: 4,
            high: 100,
            low: 50,
            usage_path: directory.path().join("missing"),
        });
        assert!(
            result
                .err()
                .unwrap()
                .starts_with("Cannot read adaptive memory usage:")
        );
    }

    #[tokio::test]
    async fn sources_share_one_guard_and_reject_conflicting_limits() {
        let (_unused, usage) = AdaptiveObjectStorageConcurrency::for_test(1, 4, 4);
        let mut config = config(usage.path().into());
        let first = AdaptiveObjectStorageConcurrency::build(Some(&config), 4, 1)
            .unwrap()
            .unwrap();
        let second = AdaptiveObjectStorageConcurrency::build(Some(&config), 4, 1)
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&first.pod, &second.pod));
        config.max_active_objects = NonZeroUsize::new(5).unwrap();
        assert!(AdaptiveObjectStorageConcurrency::build(Some(&config), 4, 1).is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn memory_hysteresis_and_unreadable_samples_fail_closed() {
        let (adaptive, usage) = AdaptiveObjectStorageConcurrency::for_test(2, 4, 4);
        assert!(adaptive.pod.healthy());
        std::fs::write(usage.path(), "100").unwrap();
        advance(SAMPLE_INTERVAL).await;
        assert!(!adaptive.pod.healthy());
        assert!(adaptive.acquire().now_or_never().is_none());
        assert_eq!(adaptive.controller.current_limit(), 1);
        std::fs::write(usage.path(), "70").unwrap();
        advance(SAMPLE_INTERVAL).await;
        assert!(!adaptive.pod.healthy());
        std::fs::write(usage.path(), "49").unwrap();
        advance(SAMPLE_INTERVAL).await;
        assert!(adaptive.pod.healthy());
        std::fs::write(usage.path(), "unreadable").unwrap();
        advance(SAMPLE_INTERVAL).await;
        assert!(!adaptive.pod.healthy());
        std::fs::write(usage.path(), "1").unwrap();
        advance(SAMPLE_INTERVAL).await;
        assert!(adaptive.pod.healthy());
    }

    #[tokio::test(start_paused = true)]
    async fn poll_reservations_bound_work_without_counting_idle_polls_as_objects() {
        let (busy, _usage) = AdaptiveObjectStorageConcurrency::for_test(2, 4, 1);
        let idle = busy.sibling_for_test(1, 4);
        let idle_poll = idle.acquire().await;
        assert_eq!(busy.pod.active_count(), 0);
        assert!(busy.acquire().now_or_never().is_none());
        drop(idle_poll);
        let mut active = busy.acquire().await;
        active.start_processing();
        assert_eq!(busy.pod.active_count(), 1);
        assert!(busy.acquire().now_or_never().is_none());
        drop(active);
        assert_eq!(busy.pod.active_count(), 0);
        let mut next = busy.acquire().await;
        next.start_processing();
    }

    #[tokio::test(start_paused = true)]
    async fn memory_pressure_after_reservation_does_not_reject_received_work() {
        let (adaptive, usage) = AdaptiveObjectStorageConcurrency::for_test(2, 4, 2);
        let mut admission = adaptive.acquire().await;
        std::fs::write(usage.path(), "100").unwrap();
        advance(SAMPLE_INTERVAL).await;
        admission.start_processing();
        assert_eq!(adaptive.pod.active_count(), 1);
        assert!(adaptive.acquire().now_or_never().is_none());
        drop(admission);
        std::fs::write(usage.path(), "1").unwrap();
        advance(SAMPLE_INTERVAL).await;
        assert!(adaptive.acquire().now_or_never().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn active_permit_is_held_through_ack_and_released_on_cancellation() {
        let (adaptive, _usage) = AdaptiveObjectStorageConcurrency::for_test(1, 1, 1);
        let mut admission = adaptive.acquire().await;
        admission.start_processing();
        advance(Duration::from_secs(30)).await;
        assert_eq!(adaptive.pod.active_count(), 1);
        assert!(adaptive.acquire().now_or_never().is_none());
        drop(admission);
        assert_eq!(adaptive.pod.active_count(), 0);
        let mut next = adaptive.acquire().await;
        next.start_processing();
        next.finish(false, true, 100);
        assert_eq!(adaptive.pod.active_count(), 0);
    }
}
