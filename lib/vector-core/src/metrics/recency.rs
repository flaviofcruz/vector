// Copyright (c) 2021 Metrics Contributors
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

//! Metric recency.
//!
//! Copied from <https://github.com/metrics-rs/metrics/blob/main/metrics-util/src/registry/recency.rs>
//! Unused parts have been removed and `fn Recency::should_store` has been modified to take into
//! account of outstanding registered handles to avoid deleting them during expiry.
//!
//! THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//! IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//! FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//! AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//! LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//! OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//! SOFTWARE.
//!
//! `Recency` deals with the concept of removing metrics that have not been updated for a certain
//! amount of time.  In some use cases, metrics are tied to specific labels which are short-lived,
//! such as labels referencing a date or a version of software.  When these labels change, exporters
//! may still be emitting those older metrics which are no longer relevant.  In many cases, a
//! long-lived application could continue tracking metrics such that the unique number of metrics
//! grows until a significant portion of memory is required to track them all, even if the majority
//! of them are no longer used.
//!
//! As metrics are typically backed by atomic storage, exporters don't see the individual changes to
//! a metric, and so need a way to measure if a metric has changed since the last time it was
//! observed.  This could potentially be achieved by observing the value directly, but metrics like
//! gauges can be updated in such a way that their value is the same between two observations even
//! though it had actually been changed in between.
//!
//! We solve for this by tracking the generation of a metric, which represents the number of times
//! it has been modified. In doing so, we can compare the generation of a metric between
//! observations, which only ever increases monotonically.  This provides a universal mechanism that
//! works for all metric types.
//!
//! `Recency` uses the generation of a metric, along with a measurement of time when a metric is
//! observed, to build a complete picture that allows deciding if a given metric has gone "idle" or
//! not, and thus whether it should actually be deleted.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use metrics::{Counter, CounterFn, Gauge, GaugeFn, HistogramFn, atomics::AtomicU64};
use metrics_util::{
    Hashable, MetricKind, MetricKindMask,
    registry::{Registry, Storage},
};
use parking_lot::Mutex;
use quanta::{Clock, Instant};
use vector_common::atomic::AtomicF64;

use super::storage::Histogram;

/// The generation of a metric.
///
/// Generations are opaque and are not meant to be used directly, but meant to be used as a
/// comparison amongst each other in terms of ordering.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct Generation(usize);

/// Generation tracking for a metric.
///
/// Holds a generic interior value, and provides way to access the value such that each access
/// increments the "generation" of the value.  This provides a means to understand if the value has
/// been updated since the last time it was observed.
///
/// For example, if a gauge was observed to be X at one point in time, and then observed to be X
/// again at a later point in time, it could have changed in between the two observations.  It also
/// may not have changed, and thus `Generational` provides a way to determine if either of these
/// events occurred.
#[derive(Clone)]
pub(super) struct Generational<T> {
    inner: T,
    generation: Arc<AtomicUsize>,
}

impl<T> Generational<T> {
    /// Creates a new `Generational<T>`.
    fn new(inner: T) -> Generational<T> {
        Generational {
            inner,
            generation: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Gets a reference to the inner value.
    pub(super) fn get_inner(&self) -> &T {
        &self.inner
    }

    /// Gets the current generation.
    pub(super) fn get_generation(&self) -> Generation {
        Generation(self.generation.load(Ordering::Acquire))
    }

    /// Acquires a reference to the inner value, and increments the generation.
    pub(super) fn with_increment<F, V>(&self, f: F) -> V
    where
        F: Fn(&T) -> V,
    {
        let result = f(&self.inner);
        _ = self.generation.fetch_add(1, Ordering::AcqRel);
        result
    }
}

impl<T> CounterFn for Generational<T>
where
    T: CounterFn,
{
    fn increment(&self, value: u64) {
        self.with_increment(|c| c.increment(value));
    }

    fn absolute(&self, value: u64) {
        self.with_increment(|c| c.absolute(value));
    }
}

impl<T> GaugeFn for Generational<T>
where
    T: GaugeFn,
{
    fn increment(&self, value: f64) {
        self.with_increment(|g| g.increment(value));
    }

    fn decrement(&self, value: f64) {
        self.with_increment(|g| g.decrement(value));
    }

    fn set(&self, value: f64) {
        self.with_increment(|g| g.set(value));
    }
}

impl<T> HistogramFn for Generational<T>
where
    T: HistogramFn,
{
    fn record(&self, value: f64) {
        self.with_increment(|h| h.record(value));
    }
}

impl<T> From<Generational<T>> for Counter
where
    T: CounterFn + Send + Sync + 'static,
{
    fn from(inner: Generational<T>) -> Self {
        Self::from_arc(Arc::new(inner))
    }
}

impl<T> From<Generational<T>> for Gauge
where
    T: GaugeFn + Send + Sync + 'static,
{
    fn from(inner: Generational<T>) -> Self {
        Self::from_arc(Arc::new(inner))
    }
}

impl<T> From<Generational<T>> for metrics::Histogram
where
    T: HistogramFn + Send + Sync + 'static,
{
    fn from(inner: Generational<T>) -> Self {
        Self::from_arc(Arc::new(inner))
    }
}

/// Generational metric storage.
///
/// Tracks the "generation" of a metric, which is used to detect updates to metrics where the value
/// otherwise would not be sufficient to be used as an indicator.
pub(super) struct GenerationalStorage<S> {
    inner: S,
}

impl<S> GenerationalStorage<S> {
    /// Creates a new [`GenerationalStorage`].
    ///
    /// This wraps the given `storage` and provides generational semantics on top of it.
    pub(super) fn new(storage: S) -> Self {
        Self { inner: storage }
    }
}

impl<K, S: Storage<K>> Storage<K> for GenerationalStorage<S> {
    type Counter = Generational<S::Counter>;
    type Gauge = Generational<S::Gauge>;
    type Histogram = Generational<S::Histogram>;

    fn counter(&self, key: &K) -> Self::Counter {
        Generational::new(self.inner.counter(key))
    }

    fn gauge(&self, key: &K) -> Self::Gauge {
        Generational::new(self.inner.gauge(key))
    }

    fn histogram(&self, key: &K) -> Self::Histogram {
        Generational::new(self.inner.histogram(key))
    }
}

pub(super) trait KeyMatcher<K> {
    fn matches(&self, key: &K) -> bool;
}

/// Tracks recency of metric updates by their registry generation and time.
///
/// In many cases, a user may have a long-running process where metrics are stored over time using
/// labels that change for some particular reason, leaving behind versions of that metric with
/// labels that are no longer relevant to the current process state.  This can lead to cases where
/// metrics that no longer matter are still present in rendered output, adding bloat.
///
/// When coupled with [`Registry`], [`Recency`] can be used to track when the last update to a
/// metric has occurred for the purposes of removing idle metrics from the registry.  In addition,
/// it will remove the value from the registry itself to reduce the aforementioned bloat.
///
/// [`Recency`] is separate from [`Registry`] specifically to avoid imposing any slowdowns when
/// tracking recency does not matter, despite their otherwise tight coupling.
pub(super) struct Recency<K, M: KeyMatcher<K>> {
    mask: MetricKindMask,
    // Cache timeouts with recency entries so expiry also releases their keys.
    inner: Mutex<(Clock, HashMap<K, (Generation, Instant, Duration)>)>,
    global_idle_timeout: Option<Duration>,
    per_set_timeouts: Vec<(M, Duration)>,
}

impl<K, M> Recency<K, M>
where
    K: Clone + Eq + Hashable + std::fmt::Debug,
    M: KeyMatcher<K>,
{
    /// Creates a new [`Recency`].
    ///
    /// The first matching per-set timeout overrides `global_idle_timeout`. If neither specifies a
    /// timeout, the metric is not tracked for recency. Otherwise, a metric that has not been updated
    /// for longer than its timeout is subject to deletion the next time it is checked.
    ///
    /// The provided `clock` is used for tracking time, while `mask` controls which metrics
    /// are covered by the recency logic.  For example, if `mask` only contains counters and
    /// histograms, then gauges will not be considered for recency, and thus will never be deleted.
    ///
    /// Refer to the documentation for [`MetricKindMask`](crate::MetricKindMask) for more
    /// information on defining a metric kind mask.
    pub(super) fn new(
        clock: Clock,
        mask: MetricKindMask,
        global_idle_timeout: Option<Duration>,
        per_set_timeouts: Vec<(M, Duration)>,
    ) -> Self {
        Recency {
            mask,
            inner: Mutex::new((clock, HashMap::new())),
            global_idle_timeout,
            per_set_timeouts,
        }
    }

    /// Checks if the given counter should be stored, based on its known recency.
    ///
    /// If the given key has been updated recently enough, and should continue to be stored, this
    /// method will return `true` and will update the last update time internally.  If the given key
    /// has not been updated recently enough, the key will be removed from the given registry if the
    /// given generation also matches.
    pub(super) fn should_store_counter<S>(
        &self,
        key: &K,
        counter: &Generational<Arc<AtomicU64>>,
        registry: &Registry<K, S>,
    ) -> bool
    where
        S: Storage<K, Counter = Generational<Arc<AtomicU64>>>,
    {
        self.should_store(
            key,
            counter,
            registry,
            MetricKind::Counter,
            Registry::get_counter,
            Registry::delete_counter,
        )
    }

    /// Checks if the given gauge should be stored, based on its known recency.
    ///
    /// If the given key has been updated recently enough, and should continue to be stored, this
    /// method will return `true` and will update the last update time internally.  If the given key
    /// has not been updated recently enough, the key will be removed from the given registry if the
    /// given generation also matches.
    pub(super) fn should_store_gauge<S>(
        &self,
        key: &K,
        gauge: &Generational<Arc<AtomicF64>>,
        registry: &Registry<K, S>,
    ) -> bool
    where
        S: Storage<K, Gauge = Generational<Arc<AtomicF64>>>,
    {
        self.should_store(
            key,
            gauge,
            registry,
            MetricKind::Gauge,
            Registry::get_gauge,
            Registry::delete_gauge,
        )
    }

    /// Checks if the given histogram should be stored, based on its known recency.
    ///
    /// If the given key has been updated recently enough, and should continue to be stored, this
    /// method will return `true` and will update the last update time internally.  If the given key
    /// has not been updated recently enough, the key will be removed from the given registry if the
    /// given generation also matches.
    pub(super) fn should_store_histogram<S>(
        &self,
        key: &K,
        hist: &Generational<Arc<Histogram>>,
        registry: &Registry<K, S>,
    ) -> bool
    where
        S: Storage<K, Histogram = Generational<Arc<Histogram>>>,
    {
        self.should_store(
            key,
            hist,
            registry,
            MetricKind::Histogram,
            Registry::get_histogram,
            Registry::delete_histogram,
        )
    }

    fn should_store<G, F, S, T>(
        &self,
        key: &K,
        value: &Generational<Arc<T>>,
        registry: &Registry<K, S>,
        kind: MetricKind,
        get_op: G,
        delete_op: F,
    ) -> bool
    where
        G: Fn(&Registry<K, S>, &K) -> Option<Generational<Arc<T>>>,
        F: Fn(&Registry<K, S>, &K) -> bool,
        S: Storage<K>,
    {
        let generation = value.get_generation();
        if self.mask.matches(kind) {
            let mut guard = self.inner.lock();
            let (clock, entries) = &mut *guard;

            let now = clock.now();
            let deleted = if let Some((last_gen, last_update, timeout)) = entries.get_mut(key) {
                // If the value is the same as the latest value we have internally, and
                // we're over the idle timeout period, then remove it and continue.
                if *last_gen == generation {
                    // We don't want to delete the metric if there is an outstanding handle that
                    // could later update the shared value. So, here we look up the count of
                    // references to the inner value to see if there are more than expected.
                    //
                    // The magic value for `strong_count` below comes from:
                    // 1. The reference in the registry
                    // 2. The reference held by the value passed in here
                    // If there is another reference, then there is handle elsewhere.
                    let referenced = Arc::strong_count(&value.inner) > 2;
                    // If the delete returns false, that means that our generation counter is
                    // out-of-date, and that the metric has been updated since, so we don't
                    // actually want to delete it yet.
                    !referenced && (now - *last_update) > *timeout && delete_op(registry, key)
                } else {
                    // Value has changed, so mark it such.
                    *last_update = now;
                    *last_gen = generation;
                    false
                }
            } else {
                let timeout = self
                    .per_set_timeouts
                    .iter()
                    .find_map(|(matcher, timeout)| matcher.matches(key).then_some(*timeout))
                    .or(self.global_idle_timeout);
                if let Some(timeout) = timeout {
                    // A handle snapshot taken before a concurrent collection expired this key
                    // must not re-track it: nothing would ever remove that entry again.
                    let live = get_op(registry, key)
                        .is_some_and(|current| Arc::ptr_eq(&current.inner, &value.inner));
                    if !live {
                        return false;
                    }
                    entries.insert(key.clone(), (generation, now, timeout));
                }
                false
            };

            if deleted {
                entries.remove(key);
                return false;
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Weak, thread};

    use metrics::{Key, Label};

    use super::*;
    use crate::metrics::storage::VectorStorage;

    type TestRegistry = Registry<Key, GenerationalStorage<VectorStorage>>;

    struct Matcher {
        name: &'static str,
        calls: Arc<AtomicUsize>,
    }

    impl KeyMatcher<Key> for Matcher {
        fn matches(&self, key: &Key) -> bool {
            self.calls.fetch_add(1, Ordering::Relaxed);
            key.name() == self.name
        }
    }

    fn matcher(name: &'static str) -> Matcher {
        Matcher {
            name,
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn registry() -> TestRegistry {
        Registry::new(GenerationalStorage::new(VectorStorage))
    }

    // Observe ownership of real metric names and labels, including copies in caches
    // other than the registry and recency map.
    fn tracked_key(name: &str, id: usize) -> (Key, [Weak<str>; 2]) {
        let name: Arc<str> = Arc::from(name);
        let label: Arc<str> = Arc::from(id.to_string());
        let refs = [Arc::downgrade(&name), Arc::downgrade(&label)];
        (Key::from_parts(name, vec![Label::new("id", label)]), refs)
    }

    // Use separate handle snapshots, as the production recorder does. Visiting
    // under registry locks would deadlock when recency deletes an expired metric.
    fn collect(recency: &Recency<Key, Matcher>, registry: &TestRegistry) -> [usize; 3] {
        let mut retained = [0; 3];
        for (key, value) in registry.get_counter_handles() {
            retained[0] += usize::from(recency.should_store_counter(&key, &value, registry));
        }
        for (key, value) in registry.get_gauge_handles() {
            retained[1] += usize::from(recency.should_store_gauge(&key, &value, registry));
        }
        for (key, value) in registry.get_histogram_handles() {
            retained[2] += usize::from(recency.should_store_histogram(&key, &value, registry));
        }
        retained
    }

    fn assert_released(refs: &[[Weak<str>; 2]]) {
        assert!(
            refs.iter().flatten().all(|value| value.upgrade().is_none()),
            "expired metric names or labels are still retained"
        );
    }

    #[test]
    fn expired_keys_release_names_and_labels() {
        let (clock, time) = Clock::mock();
        let recency = Recency::<_, Matcher>::new(
            clock,
            MetricKindMask::ALL,
            Some(Duration::from_secs(10)),
            vec![],
        );
        let registry = registry();

        // Keep Recency alive across batches: memory must follow live cardinality,
        // not every label value observed over the process lifetime.
        for batch in 0..8 {
            let mut refs = Vec::new();
            for id in batch * 32..(batch + 1) * 32 {
                let (key, weak) = tracked_key("counter", id);
                registry.get_or_create_counter(&key, |value| CounterFn::increment(value, 1));
                refs.push(weak);
                let (key, weak) = tracked_key("gauge", id);
                registry.get_or_create_gauge(&key, |value| value.set(1.0));
                refs.push(weak);
                let (key, weak) = tracked_key("histogram", id);
                registry.get_or_create_histogram(&key, |value| value.record(1.0));
                refs.push(weak);
            }
            assert_eq!(collect(&recency, &registry), [32; 3]);
            assert_eq!(recency.inner.lock().1.len(), 96);
            time.increment(Duration::from_secs(11));
            assert_eq!(collect(&recency, &registry), [0; 3]);
            assert!(registry.get_counter_handles().is_empty());
            assert!(registry.get_gauge_handles().is_empty());
            assert!(registry.get_histogram_handles().is_empty());
            assert!(recency.inner.lock().1.is_empty());
            assert_released(&refs);
        }
    }

    #[test]
    fn registered_handles_protect_all_metric_kinds_until_dropped() {
        let (clock, time) = Clock::mock();
        let recency = Recency::<_, Matcher>::new(
            clock,
            MetricKindMask::ALL,
            Some(Duration::from_secs(10)),
            vec![],
        );
        let registry = registry();
        let (key, counter_refs) = tracked_key("counter", 0);
        let counter: Counter = registry.get_or_create_counter(&key, |value| value.clone().into());
        drop(key);
        let (key, gauge_refs) = tracked_key("gauge", 0);
        let gauge: Gauge = registry.get_or_create_gauge(&key, |value| value.clone().into());
        drop(key);
        let (key, histogram_refs) = tracked_key("histogram", 0);
        let histogram: metrics::Histogram =
            registry.get_or_create_histogram(&key, |value| value.clone().into());
        drop(key);

        assert_eq!(collect(&recency, &registry), [1; 3]);
        time.increment(Duration::from_secs(11));
        assert_eq!(collect(&recency, &registry), [1; 3]);
        counter.increment(1);
        gauge.set(1.0);
        histogram.record(1.0);
        drop((counter, gauge, histogram));
        // An update starts a fresh idle period even when the handle is dropped.
        assert_eq!(collect(&recency, &registry), [1; 3]);
        time.increment(Duration::from_secs(10));
        assert_eq!(collect(&recency, &registry), [1; 3]);
        time.increment(Duration::from_nanos(1));
        assert_eq!(collect(&recency, &registry), [0; 3]);
        assert!(recency.inner.lock().1.is_empty());
        assert_released(&[counter_refs, gauge_refs, histogram_refs]);
    }

    #[test]
    fn recreated_keys_get_a_fresh_timeout_and_cached_match() {
        let (clock, time) = Clock::mock();
        let matcher = matcher("counter");
        let calls = Arc::clone(&matcher.calls);
        let recency = Recency::new(
            clock,
            MetricKindMask::ALL,
            None,
            vec![(matcher, Duration::from_secs(10))],
        );
        let registry = registry();

        for incarnation in 1..=3 {
            let (key, refs) = tracked_key("counter", 0);
            registry.get_or_create_counter(&key, |value| CounterFn::increment(value, 1));
            drop(key);
            assert_eq!(collect(&recency, &registry), [1, 0, 0]);
            time.increment(Duration::from_secs(10));
            assert_eq!(collect(&recency, &registry), [1, 0, 0]);
            assert_eq!(calls.load(Ordering::Relaxed), incarnation);
            time.increment(Duration::from_nanos(1));
            assert_eq!(collect(&recency, &registry), [0; 3]);
            assert_eq!(calls.load(Ordering::Relaxed), incarnation);
            assert!(recency.inner.lock().1.is_empty());
            assert_released(&[refs]);
        }
    }

    #[test]
    fn first_matching_override_wins_with_or_without_a_default() {
        for default in [Some(Duration::from_secs(10)), None] {
            let (clock, time) = Clock::mock();
            let recency = Recency::new(
                clock,
                MetricKindMask::ALL,
                default,
                vec![
                    (matcher("short"), Duration::from_secs(3)),
                    (matcher("short"), Duration::from_secs(30)),
                    (matcher("long"), Duration::from_secs(20)),
                ],
            );
            let registry = registry();
            let mut refs = Vec::new();
            for name in ["short", "long", "unmatched"] {
                let (key, weak) = tracked_key(name, 0);
                registry.get_or_create_counter(&key, |value| CounterFn::increment(value, 1));
                refs.push(weak);
            }
            assert_eq!(collect(&recency, &registry), [3, 0, 0]);
            time.increment(Duration::from_secs(4));
            assert_eq!(collect(&recency, &registry), [2, 0, 0]);
            assert!(
                registry
                    .get_counter(&Key::from_parts("short", &[("id", "0")]))
                    .is_none()
            );
            time.increment(Duration::from_secs(7));
            assert_eq!(
                collect(&recency, &registry),
                [if default.is_some() { 1 } else { 2 }, 0, 0]
            );
            time.increment(Duration::from_secs(10));
            assert_eq!(
                collect(&recency, &registry),
                [usize::from(default.is_none()), 0, 0]
            );
            assert!(recency.inner.lock().1.is_empty());
            // Unmatched, non-expiring keys belong only to the registry; recency
            // must not accumulate an extra retained-key cache for them.
            registry.clear();
            assert_released(&refs);
        }
    }

    #[test]
    fn disabled_expiry_and_excluded_kinds_do_not_retain_keys() {
        for (mask, timeout) in [
            (MetricKindMask::ALL, None),
            (MetricKindMask::GAUGE, Some(Duration::from_secs(10))),
        ] {
            let (clock, time) = Clock::mock();
            let recency = Recency::<_, Matcher>::new(clock, mask, timeout, vec![]);
            let registry = registry();
            let (key, refs) = tracked_key("counter", 0);
            registry.get_or_create_counter(&key, |value| CounterFn::increment(value, 1));
            drop(key);
            assert_eq!(collect(&recency, &registry), [1, 0, 0]);
            time.increment(Duration::from_secs(100));
            assert_eq!(collect(&recency, &registry), [1, 0, 0]);
            assert!(recency.inner.lock().1.is_empty());
            registry.clear();
            assert_released(&[refs]);
        }
    }

    #[test]
    fn stale_snapshot_does_not_retrack_expired_key() {
        let (clock, time) = Clock::mock();
        let recency = Recency::<_, Matcher>::new(
            clock,
            MetricKindMask::ALL,
            Some(Duration::from_secs(10)),
            vec![],
        );
        let registry = registry();
        let (key, refs) = tracked_key("counter", 0);
        registry.get_or_create_counter(&key, |value| CounterFn::increment(value, 1));
        assert_eq!(collect(&recency, &registry), [1, 0, 0]);
        time.increment(Duration::from_secs(11));
        assert_eq!(collect(&recency, &registry), [0; 3]);
        // A concurrent collector can clone a handle between another collector's
        // reference check and its delete; afterwards that handle no longer backs
        // the registry, like one from an unrelated registry.
        let unrelated = self::registry();
        let stale = unrelated.get_or_create_counter(&key, Clone::clone);
        assert!(!recency.should_store_counter(&key, &stale, &registry));
        assert!(recency.inner.lock().1.is_empty());
        drop((key, stale, unrelated));
        assert_released(&[refs]);
    }

    #[test]
    fn concurrent_collection_preserves_handles_and_reclaims_dropped_keys() {
        let (clock, time) = Clock::mock();
        let recency = Recency::<_, Matcher>::new(
            clock,
            MetricKindMask::ALL,
            Some(Duration::from_secs(10)),
            vec![],
        );
        let registry = registry();
        let mut refs = Vec::new();
        let mut handles = Vec::new();
        for id in 0..32 {
            let (key, weak) = tracked_key("counter", id);
            let counter: Counter =
                registry.get_or_create_counter(&key, |value| value.clone().into());
            handles.push(counter);
            refs.push(weak);
        }
        assert_eq!(collect(&recency, &registry), [32, 0, 0]);
        time.increment(Duration::from_secs(11));
        thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    for _ in 0..32 {
                        assert_eq!(collect(&recency, &registry), [32, 0, 0]);
                    }
                });
            }
            scope.spawn(|| {
                for _ in 0..32 {
                    for handle in &handles {
                        handle.increment(1);
                    }
                }
            });
        });
        drop(handles);
        // Observe the final generation before starting the idle interval.
        collect(&recency, &registry);
        time.increment(Duration::from_secs(11));
        thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| collect(&recency, &registry));
            }
        });
        // Concurrent snapshots may temporarily protect each other's handles.
        assert_eq!(collect(&recency, &registry), [0; 3]);
        assert!(registry.get_counter_handles().is_empty());
        assert!(recency.inner.lock().1.is_empty());
        assert_released(&refs);
    }
}
