//! Shared, opt-in byte-rate pacing for the object-storage sources
//! (`aws_s3`, `azure_blob`, `gcp_gcs`).
//!
//! [`ObjectStorageRateLimiter`] holds one [`leaky_bucket::RateLimiter`], shared
//! across a source's polling workers, and caps the source instance's data rate.
//! Sources feed decoded (post-decompression) byte counts through a
//! [`ByteCounter`] and call [`ObjectStorageRateLimiter::pace`] once per batch;
//! the accumulation is the only source-side wiring, everything else lives here.
//! Disabled unless `max_bytes_per_sec` is set.

use std::num::NonZeroU64;
use std::time::Duration;

use vector_lib::configurable::configurable_component;

/// Byte-rate pacing configuration for an object-storage source.
#[configurable_component]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct ObjectStorageRateLimitConfig {
    /// Maximum sustained data rate, in decoded (post-decompression) bytes per
    /// second, across all of the source's polling workers. Unset disables pacing.
    #[configurable(metadata(docs::type_unit = "bytes"))]
    #[serde(default)]
    pub max_bytes_per_sec: Option<NonZeroU64>,

    /// Burst/smoothing horizon: the limiter may burst up to
    /// `max_bytes_per_sec * window_secs` bytes before pacing. No effect when
    /// `max_bytes_per_sec` is unset.
    #[configurable(metadata(docs::type_unit = "seconds"))]
    #[serde(default = "default_window_secs")]
    pub window_secs: u64,

    /// Observability-only threshold, in decoded bytes: warn when a single object
    /// exceeds it. Never drops or defers data. No effect when `max_bytes_per_sec`
    /// is unset.
    #[configurable(metadata(docs::type_unit = "bytes"))]
    #[serde(default)]
    pub warn_object_bytes: Option<NonZeroU64>,
}

const fn default_window_secs() -> u64 {
    30
}

/// Leaky-bucket parameters derived from [`ObjectStorageRateLimitConfig`]. Split
/// out (and always compiled) so the derivation is independent of the source
/// features that gate the limiter itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RateLimitParams {
    /// Bucket capacity / burst ceiling, in bytes (`rate * window`).
    pub capacity: usize,
    /// Byte-tokens added each `interval`.
    pub refill: usize,
    /// Refill cadence.
    pub interval: Duration,
}

impl ObjectStorageRateLimitConfig {
    /// Refills per second; sub-second cadence keeps pacing smooth.
    const REFILLS_PER_SEC: u64 = 10;

    /// Derive leaky-bucket parameters, or `None` when pacing is disabled.
    pub fn params(&self) -> Option<RateLimitParams> {
        let rate = self.max_bytes_per_sec?.get();
        let window = self.window_secs.max(1); // a zero window can't hold a refill
        Some(RateLimitParams {
            capacity: saturating_u64_to_usize(rate.saturating_mul(window)),
            refill: saturating_u64_to_usize((rate / Self::REFILLS_PER_SEC).max(1)),
            interval: Duration::from_millis(1000 / Self::REFILLS_PER_SEC),
        })
    }
}

fn saturating_u64_to_usize(v: u64) -> usize {
    usize::try_from(v).unwrap_or(usize::MAX)
}

#[cfg(any(
    feature = "sources-aws_s3",
    feature = "sources-azure_blob",
    feature = "sources-gcp_gcs"
))]
mod limiter {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use vector_lib::emit;

    use super::{ObjectStorageRateLimitConfig, saturating_u64_to_usize};
    use crate::internal_events::{ObjectStorageLargeObject, ObjectStoragePaced};

    /// Accumulator for decoded bytes processed. Cloneable so a source can hand a
    /// handle into its per-frame decode closure while retaining one to drain.
    #[derive(Clone, Default)]
    pub struct ByteCounter(Arc<AtomicU64>);

    impl ByteCounter {
        /// Add decoded bytes seen for a frame.
        pub fn add(&self, n: usize) {
            self.0.fetch_add(n as u64, Ordering::Relaxed);
        }

        /// Current accumulated total (does not reset).
        pub fn get(&self) -> u64 {
            self.0.load(Ordering::Relaxed)
        }

        /// Take and reset the accumulated total.
        pub fn take(&self) -> u64 {
            self.0.swap(0, Ordering::Relaxed)
        }
    }

    /// Built, ready-to-use byte-rate pacer for a source instance. Cloneable and
    /// cheap to share (the limiter is behind an `Arc`); store one on the source's
    /// shared state so all workers pace against a single global limit.
    #[derive(Clone)]
    pub struct ObjectStorageRateLimiter {
        limiter: Option<Arc<leaky_bucket::RateLimiter>>,
        warn_object_bytes: Option<u64>,
    }

    impl ObjectStorageRateLimiter {
        /// Build from config.
        pub fn new(config: &ObjectStorageRateLimitConfig) -> Self {
            let limiter = config.params().map(|params| {
                Arc::new(
                    leaky_bucket::RateLimiter::builder()
                        .initial(params.capacity) // start full: allow an initial burst
                        .max(params.capacity)
                        .refill(params.refill)
                        .interval(params.interval)
                        .build(),
                )
            });
            Self {
                limiter,
                warn_object_bytes: config.warn_object_bytes.map(std::num::NonZeroU64::get),
            }
        }

        /// Whether pacing is active.
        pub const fn is_enabled(&self) -> bool {
            self.limiter.is_some()
        }

        /// Pause to honor the configured rate, charging `decoded_bytes` of work.
        ///
        /// Call after a pulled batch is processed and its queue messages are
        /// acked/deleted, so nothing is in flight during the wait. A request for
        /// more than the bucket capacity waits across refill intervals rather than
        /// rejecting, so an oversized batch is paid back over time.
        pub async fn pace(&self, decoded_bytes: u64) {
            let Some(limiter) = &self.limiter else { return };
            if decoded_bytes == 0 {
                return;
            }
            let start = std::time::Instant::now();
            limiter
                .acquire(saturating_u64_to_usize(decoded_bytes))
                .await;
            emit!(ObjectStoragePaced {
                wait_seconds: start.elapsed().as_secs_f64(),
                bytes: decoded_bytes,
            });
        }

        /// Emit an observability warning if a single object's decoded size exceeds
        /// the configured `warn_object_bytes` threshold. Never drops data.
        pub fn note_object(&self, decoded_bytes: u64, bucket: &str, key: &str) {
            if self.warn_object_bytes.is_some_and(|t| decoded_bytes > t) {
                emit!(ObjectStorageLargeObject {
                    bytes: decoded_bytes,
                    bucket,
                    key,
                });
            }
        }
    }
}

#[cfg(any(
    feature = "sources-aws_s3",
    feature = "sources-azure_blob",
    feature = "sources-gcp_gcs"
))]
pub use limiter::{ByteCounter, ObjectStorageRateLimiter};

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::ObjectStorageRateLimitConfig;

    #[test]
    fn params_derivation() {
        let cases: &[(Option<u64>, u64, Option<(usize, usize, u64)>)] = &[
            // Disabled when no rate is set.
            (None, 30, None),
            // capacity = rate * window; refill = rate / 10; interval = 100ms.
            (Some(1_000), 30, Some((30_000, 100, 100))),
            // A zero window is floored to one second.
            (Some(1_000), 0, Some((1_000, 100, 100))),
            // Small rates still refill at least one byte per interval.
            (Some(5), 30, Some((150, 1, 100))),
            // rate * window saturates instead of overflowing.
            (
                Some(u64::MAX),
                30,
                Some((usize::MAX, (u64::MAX / 10) as usize, 100)),
            ),
        ];

        for &(max_bytes_per_sec, window_secs, expected) in cases {
            let config = ObjectStorageRateLimitConfig {
                max_bytes_per_sec: max_bytes_per_sec.and_then(NonZeroU64::new),
                window_secs,
                warn_object_bytes: None,
            };
            let got = config
                .params()
                .map(|p| (p.capacity, p.refill, p.interval.as_millis() as u64));
            assert_eq!(
                got, expected,
                "max_bytes_per_sec={max_bytes_per_sec:?} window_secs={window_secs}"
            );
        }
    }
}
