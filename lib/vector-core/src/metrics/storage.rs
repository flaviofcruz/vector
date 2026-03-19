use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use metrics::{HistogramFn, Key, atomics::AtomicU64};
use metrics_util::registry::Storage;
use vector_common::atomic::AtomicF64;

use crate::event::{MetricValue, metric::Bucket};

pub(super) struct VectorStorage;

impl Storage<Key> for VectorStorage {
    type Counter = Arc<AtomicU64>;
    type Gauge = Arc<AtomicF64>;
    type Histogram = Arc<Histogram>;

    fn counter(&self, _: &Key) -> Self::Counter {
        Arc::new(AtomicU64::new(0))
    }

    fn gauge(&self, _: &Key) -> Self::Gauge {
        Arc::new(AtomicF64::new(0.0))
    }

    fn histogram(&self, key: &Key) -> Self::Histogram {
        let use_integer = key
            .labels()
            .any(|l| l.key() == "buckets" && l.value() == "integer");
        if use_integer {
            Arc::new(Histogram::new_integer())
        } else {
            Arc::new(Histogram::new())
        }
    }
}

#[derive(Debug)]
pub(super) struct Histogram {
    buckets: Box<[(f64, AtomicU32); 26]>,
    power_of_two: bool,
    count: AtomicU64,
    sum: AtomicF64,
}

impl Histogram {
    const MIN_BUCKET: f64 = 1.0 / (1 << 12) as f64; // f64::powi() is not const yet
    const MIN_BUCKET_EXP: f64 = -12.0;
    const BUCKETS: usize = 26;
    pub(crate) fn new() -> Self {
        // Box to avoid having this large array inline to the structure, blowing
        // out cache coherence.
        //
        // The sequence here is based on powers of two. Other sequences are more
        // suitable for different distributions but since our present use case
        // is mostly non-negative and measures smallish latencies we cluster
        // around but never quite get to zero with an increasingly coarse
        // long-tail.
        let buckets = Box::new([
            (2.0f64.powi(-12), AtomicU32::new(0)),
            (2.0f64.powi(-11), AtomicU32::new(0)),
            (2.0f64.powi(-10), AtomicU32::new(0)),
            (2.0f64.powi(-9), AtomicU32::new(0)),
            (2.0f64.powi(-8), AtomicU32::new(0)),
            (2.0f64.powi(-7), AtomicU32::new(0)),
            (2.0f64.powi(-6), AtomicU32::new(0)),
            (2.0f64.powi(-5), AtomicU32::new(0)),
            (2.0f64.powi(-4), AtomicU32::new(0)),
            (2.0f64.powi(-3), AtomicU32::new(0)),
            (2.0f64.powi(-2), AtomicU32::new(0)),
            (2.0f64.powi(-1), AtomicU32::new(0)),
            (2.0f64.powi(0), AtomicU32::new(0)),
            (2.0f64.powi(1), AtomicU32::new(0)),
            (2.0f64.powi(2), AtomicU32::new(0)),
            (2.0f64.powi(3), AtomicU32::new(0)),
            (2.0f64.powi(4), AtomicU32::new(0)),
            (2.0f64.powi(5), AtomicU32::new(0)),
            (2.0f64.powi(6), AtomicU32::new(0)),
            (2.0f64.powi(7), AtomicU32::new(0)),
            (2.0f64.powi(8), AtomicU32::new(0)),
            (2.0f64.powi(9), AtomicU32::new(0)),
            (2.0f64.powi(10), AtomicU32::new(0)),
            (2.0f64.powi(11), AtomicU32::new(0)),
            (2.0f64.powi(12), AtomicU32::new(0)),
            (f64::INFINITY, AtomicU32::new(0)),
        ]);
        Self {
            buckets,
            power_of_two: true,
            count: AtomicU64::new(0),
            sum: AtomicF64::new(0.0),
        }
    }

    /// Integer buckets [1, 2, 3, ..., 25, +∞] for discrete count distributions
    /// like retry attempts per request.
    pub(crate) fn new_integer() -> Self {
        let buckets = Box::new([
            (1.0, AtomicU32::new(0)),
            (2.0, AtomicU32::new(0)),
            (3.0, AtomicU32::new(0)),
            (4.0, AtomicU32::new(0)),
            (5.0, AtomicU32::new(0)),
            (6.0, AtomicU32::new(0)),
            (7.0, AtomicU32::new(0)),
            (8.0, AtomicU32::new(0)),
            (9.0, AtomicU32::new(0)),
            (10.0, AtomicU32::new(0)),
            (11.0, AtomicU32::new(0)),
            (12.0, AtomicU32::new(0)),
            (13.0, AtomicU32::new(0)),
            (14.0, AtomicU32::new(0)),
            (15.0, AtomicU32::new(0)),
            (16.0, AtomicU32::new(0)),
            (17.0, AtomicU32::new(0)),
            (18.0, AtomicU32::new(0)),
            (19.0, AtomicU32::new(0)),
            (20.0, AtomicU32::new(0)),
            (21.0, AtomicU32::new(0)),
            (22.0, AtomicU32::new(0)),
            (23.0, AtomicU32::new(0)),
            (24.0, AtomicU32::new(0)),
            (25.0, AtomicU32::new(0)),
            (f64::INFINITY, AtomicU32::new(0)),
        ]);
        Self {
            buckets,
            power_of_two: false,
            count: AtomicU64::new(0),
            sum: AtomicF64::new(0.0),
        }
    }

    fn bucket_index(&self, value: f64) -> usize {
        if self.power_of_two {
            // The buckets are all powers of two, so compute the ceiling of the log_2 of the
            // value. Apply a lower bound to prevent zero or negative values from blowing up the log.
            let log = value.max(Self::MIN_BUCKET).log2().ceil();
            // Offset it based on the minimum bucket's exponent. The result will be non-negative
            // thanks to the `.max` above, so we can coerce it directly to `usize`.
            #[allow(clippy::cast_possible_truncation)]
            let index = (log - Self::MIN_BUCKET_EXP) as usize;
            index.min(Self::BUCKETS - 1)
        } else {
            // Integer buckets [1, 2, ..., 25, +∞]: linear scan for the first bucket that fits.
            for (i, (upper, _)) in self.buckets.iter().enumerate() {
                if value <= *upper {
                    return i;
                }
            }
            Self::BUCKETS - 1
        }
    }

    pub(super) fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    pub(super) fn sum(&self) -> f64 {
        self.sum.load(Ordering::Relaxed)
    }

    fn buckets(&self) -> Vec<Bucket> {
        self.buckets
            .iter()
            .map(|(upper_limit, count)| Bucket {
                upper_limit: *upper_limit,
                count: u64::from(count.load(Ordering::Relaxed)),
            })
            .collect()
    }

    pub(super) fn make_metric(&self) -> MetricValue {
        MetricValue::AggregatedHistogram {
            buckets: self.buckets(),
            count: self.count(),
            sum: self.sum(),
        }
    }
}

impl HistogramFn for Histogram {
    fn record(&self, value: f64) {
        let index = self.bucket_index(value);
        self.buckets[index].1.fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |cur| cur + value);
    }
}

#[cfg(test)]
mod test {
    use metrics::HistogramFn;
    use quickcheck::{QuickCheck, TestResult};

    use super::Histogram;

    // Adapted from https://users.rust-lang.org/t/assert-eq-for-float-numbers/7034/4?u=blt
    fn nearly_equal(a: f64, b: f64) -> bool {
        let abs_a = a.abs();
        let abs_b = b.abs();
        let diff = (a - b).abs();

        if a == b {
            // Handle infinities.
            true
        } else if a == 0.0 || b == 0.0 || diff < f64::MIN_POSITIVE {
            // One of a or b is zero (or both are extremely close to it,) use absolute error.
            diff < (f64::EPSILON * f64::MIN_POSITIVE)
        } else {
            // Use relative error.
            (diff / f64::min(abs_a + abs_b, f64::MAX)) < f64::EPSILON
        }
    }

    #[test]
    #[allow(clippy::needless_pass_by_value)] // `&[T]` does not implement `Arbitrary`
    fn histogram() {
        fn inner(values: Vec<f64>) -> TestResult {
            let sut = Histogram::new();
            let mut model_count: u64 = 0;
            let mut model_sum: f64 = 0.0;

            for value in values {
                if value.is_infinite() || value.is_nan() {
                    continue;
                }

                let index = sut.bucket_index(value);
                assert!(
                    value <= sut.buckets[index].0,
                    "Value {} is not less than the upper limit {}.",
                    value,
                    sut.buckets[index].0
                );
                if index > 0 {
                    assert!(
                        value > sut.buckets[index - 1].0,
                        "Value {} is not greater than the previous upper limit {}.",
                        value,
                        sut.buckets[index - 1].0
                    );
                }

                sut.record(value);
                model_count = model_count.wrapping_add(1);
                model_sum += value;

                assert_eq!(sut.count(), model_count);
                assert!(nearly_equal(sut.sum(), model_sum));
            }
            TestResult::passed()
        }

        QuickCheck::new()
            .tests(1_000)
            .max_tests(2_000)
            .quickcheck(inner as fn(Vec<f64>) -> TestResult);
    }

    #[test]
    fn histogram_integer_boundaries() {
        let h = Histogram::new_integer();

        // Exact integer values map to their expected index
        assert_eq!(h.bucket_index(1.0), 0);
        assert_eq!(h.bucket_index(2.0), 1);
        assert_eq!(h.bucket_index(25.0), 24);

        // Fractional values land in the next bucket up
        assert_eq!(h.bucket_index(1.5), 1); // > 1.0, so bucket 2.0
        assert_eq!(h.bucket_index(0.5), 0); // <= 1.0, so bucket 1.0

        // Values <= 0 land in the first bucket
        assert_eq!(h.bucket_index(0.0), 0);
        assert_eq!(h.bucket_index(-5.0), 0);

        // Values > 25 land in the +inf bucket
        assert_eq!(h.bucket_index(26.0), 25);
        assert_eq!(h.bucket_index(1000.0), 25);
    }
}
