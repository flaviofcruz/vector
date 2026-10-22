//! A process-wide, short-lived cache of directory listings shared by every `kubernetes_logs`
//! source's discovery.
//!
//! Each source runs discovery on its own blocking thread over the same pod store, so without
//! sharing every pod directory is re-read once per source per cooldown (hundreds of times on hosts
//! with many sources). Listings are filesystem facts independent of any source's patterns, so one
//! global cache serves all sources.
//!
//! Staleness: a caller only accepts a listing whose read started at most `max_age` ago, where
//! `max_age = min(glob_minimum_cooldown / 2, MAX_LISTING_AGE)` for that caller. A file created at
//! time `t` is therefore visible to every pass that starts after `t + max_age`, so the worst-case
//! discovery delay grows from `cooldown` to `cooldown + max_age` (at most `cooldown + 2s`; under
//! overload the file server's slow-pass rest adds to this, see `next_discovery_time`). With
//! sources staggered over a 10s cooldown, a 2s cap already serves ~98% of lookups from the cache.
//!
//! Keys are the directory path's bytes, not `Path`s: hashing and comparing a `Path` walks its
//! components, which dominated the cost of a cache hit. Byte keys are at least as strict as `Path`
//! equality (`/a//b` and `/a/b` become separate entries). SipHash is kept because paths come from
//! pod annotations.
//!
//! Concurrency: the map is sharded and a shard lock is only held to look up or replace a slot,
//! never across a syscall. The `read_dir` itself runs in a per-slot `OnceLock`, so concurrent
//! callers of the same directory wait for one read instead of each issuing their own. Transient
//! read errors (see `is_stable`) are returned to the callers of that one read but not kept for
//! later callers.
//!
//! Memory: every entry older than `MAX_LISTING_AGE` is useless to all callers. Each shard drops
//! such entries when it is swept (at most once per `MAX_LISTING_AGE`, on a miss), so directories of
//! pods that left the store age out, and the total entry count is capped by evicting the oldest.

use std::{
    collections::{HashMap, hash_map::RandomState},
    ffi::{OsStr, OsString},
    hash::BuildHasher,
    io,
    path::Path,
    sync::{Arc, LazyLock, Mutex, MutexGuard, OnceLock},
    time::{Duration, Instant},
};

use super::listing_matcher::{Listing, is_stable, read_listing};

/// Upper bound on any caller's `max_age`, and the age after which entries are evicted.
pub(super) const MAX_LISTING_AGE: Duration = Duration::from_secs(2);

/// Enough for ~570 pods × (emptyDir base, its volumes, and the directories patterns descend
/// into) on the busiest observed hosts, with headroom.
pub(super) const MAX_ENTRIES: usize = 32_768;

const SHARDS: usize = 16;

trait Clock: Send + Sync {
    fn now(&self) -> Instant;
}

struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

type DirReader = dyn Fn(&Path) -> Listing + Send + Sync;

/// The listing age a source with this cooldown may accept.
pub(super) fn max_age_for_cooldown(glob_minimum_cooldown: Duration) -> Duration {
    (glob_minimum_cooldown / 2).min(MAX_LISTING_AGE)
}

struct Slot {
    /// When the slot was created, which is no later than when its read started, so ages computed
    /// from it overestimate the listing's real age.
    created: Instant,
    listing: OnceLock<Listing>,
}

#[derive(Default)]
struct Shard {
    slots: HashMap<OsString, Arc<Slot>>,
    last_sweep: Option<Instant>,
}

impl Shard {
    fn sweep(&mut self, now: Instant, capacity: usize) {
        self.slots
            .retain(|_, slot| now.saturating_duration_since(slot.created) <= MAX_LISTING_AGE);
        self.last_sweep = Some(now);
        while self.slots.len() >= capacity {
            let Some(oldest) = self
                .slots
                .iter()
                .min_by_key(|(_, slot)| slot.created)
                .map(|(path, _)| path.clone())
            else {
                break;
            };
            self.slots.remove(&oldest);
        }
    }
}

pub(super) struct ListingCache {
    shards: Vec<Mutex<Shard>>,
    hasher: RandomState,
    shard_capacity: usize,
    clock: Box<dyn Clock>,
    reader: Box<DirReader>,
}

impl ListingCache {
    /// The cache shared by every `kubernetes_logs` source in the process.
    pub(super) fn shared() -> Arc<Self> {
        static SHARED: LazyLock<Arc<ListingCache>> = LazyLock::new(|| {
            Arc::new(ListingCache::new(
                Box::new(SystemClock),
                Box::new(read_listing),
                MAX_ENTRIES,
            ))
        });
        Arc::clone(&SHARED)
    }

    fn new(clock: Box<dyn Clock>, reader: Box<DirReader>, max_entries: usize) -> Self {
        Self {
            shards: (0..SHARDS).map(|_| Mutex::default()).collect(),
            hasher: RandomState::new(),
            shard_capacity: max_entries.div_ceil(SHARDS).max(1),
            clock,
            reader,
        }
    }

    /// The listing of `dir`, read no earlier than `max_age` ago.
    pub(super) fn get(&self, dir: &Path, max_age: Duration) -> Listing {
        let slot = self.slot(dir, max_age);
        let listing = slot.listing.get_or_init(|| (self.reader)(dir)).clone();
        if !is_stable(&listing) {
            self.forget(dir, &slot);
        }
        listing
    }

    fn shard(&self, key: &OsStr) -> MutexGuard<'_, Shard> {
        let index = (self.hasher.hash_one(key) as usize) % self.shards.len();
        self.shards[index]
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Drops `slot` unless a newer one has already replaced it.
    fn forget(&self, dir: &Path, slot: &Arc<Slot>) {
        let key = dir.as_os_str();
        let mut shard = self.shard(key);
        if shard
            .slots
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(current, slot))
        {
            shard.slots.remove(key);
        }
    }

    fn slot(&self, dir: &Path, max_age: Duration) -> Arc<Slot> {
        let now = self.clock.now();
        let key = dir.as_os_str();
        let mut shard = self.shard(key);
        if let Some(slot) = shard.slots.get(key)
            && now.saturating_duration_since(slot.created) <= max_age
        {
            return Arc::clone(slot);
        }
        let sweep_due = shard
            .last_sweep
            .is_none_or(|last| now.saturating_duration_since(last) > MAX_LISTING_AGE);
        if sweep_due || shard.slots.len() >= self.shard_capacity {
            shard.sweep(now, self.shard_capacity);
        }
        let slot = Arc::new(Slot {
            created: now,
            listing: OnceLock::new(),
        });
        shard.slots.insert(key.to_owned(), Arc::clone(&slot));
        slot
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.lock().unwrap().slots.len())
            .sum()
    }
}

#[cfg(test)]
pub(super) mod test_support {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// A clock that only moves when told to, counting how often it is read.
    #[derive(Clone)]
    pub(in super::super) struct ManualClock {
        now: Arc<Mutex<Instant>>,
        pub(in super::super) calls: Arc<AtomicUsize>,
    }

    impl ManualClock {
        pub(in super::super) fn new() -> Self {
            Self {
                now: Arc::new(Mutex::new(Instant::now())),
                calls: Arc::default(),
            }
        }

        pub(in super::super) fn advance(&self, by: Duration) {
            *self.now.lock().unwrap() += by;
        }
    }

    impl Clock for ManualClock {
        fn now(&self) -> Instant {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.now.lock().unwrap()
        }
    }

    /// A private cache on `clock` that lists directories with `reader`.
    pub(in super::super) fn cache_with_reader(
        clock: &ManualClock,
        reader: impl Fn(&Path) -> Listing + Send + Sync + 'static,
    ) -> Arc<ListingCache> {
        Arc::new(ListingCache::new(
            Box::new(clock.clone()),
            Box::new(reader),
            MAX_ENTRIES,
        ))
    }

    /// A private cache on `clock` that counts its `read_dir`s.
    pub(in super::super) fn counting_cache(
        clock: &ManualClock,
        max_entries: usize,
    ) -> (Arc<ListingCache>, Arc<AtomicUsize>) {
        let reads = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&reads);
        let cache = ListingCache::new(
            Box::new(clock.clone()),
            Box::new(move |dir: &Path| {
                counter.fetch_add(1, Ordering::SeqCst);
                read_listing(dir)
            }),
            max_entries,
        );
        (Arc::new(cache), reads)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicUsize, Ordering},
        thread,
    };

    use tempfile::TempDir;

    use super::{test_support::*, *};

    fn names(listing: &Listing) -> Vec<String> {
        listing
            .as_ref()
            .expect("listable")
            .iter()
            .map(|entry| entry.name.to_string_lossy().into_owned())
            .collect()
    }

    const AGE: Duration = Duration::from_secs(2);

    #[test]
    fn max_age_is_half_cooldown_capped() {
        assert_eq!(MAX_LISTING_AGE, Duration::from_secs(2));
        assert_eq!(
            max_age_for_cooldown(Duration::from_secs(3)),
            Duration::from_millis(1_500)
        );
        assert_eq!(
            max_age_for_cooldown(Duration::from_secs(4)),
            MAX_LISTING_AGE
        );
        assert_eq!(
            max_age_for_cooldown(Duration::from_secs(1)),
            Duration::from_millis(500)
        );
        assert_eq!(
            max_age_for_cooldown(Duration::from_secs(10)),
            MAX_LISTING_AGE
        );
        assert_eq!(
            max_age_for_cooldown(Duration::from_secs(60)),
            MAX_LISTING_AGE
        );
        assert_eq!(max_age_for_cooldown(Duration::ZERO), Duration::ZERO);
    }

    #[test]
    fn hit_within_age_then_expiry_reveals_new_file() {
        let tmp = TempDir::new().unwrap();
        fs::write(tmp.path().join("a.log"), "").unwrap();
        let clock = ManualClock::new();
        let (cache, reads) = counting_cache(&clock, MAX_ENTRIES);

        assert_eq!(names(&cache.get(tmp.path(), AGE)), ["a.log"]);
        fs::write(tmp.path().join("b.log"), "").unwrap();
        clock.advance(AGE);
        // Exactly max_age old is still acceptable; the new file stays hidden.
        assert_eq!(names(&cache.get(tmp.path(), AGE)), ["a.log"]);
        assert_eq!(reads.load(Ordering::SeqCst), 1);

        clock.advance(Duration::from_nanos(1));
        assert_eq!(names(&cache.get(tmp.path(), AGE)), ["a.log", "b.log"]);
        assert_eq!(reads.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn keys_are_exact_path_bytes() {
        let tmp = TempDir::new().unwrap();
        fs::create_dir_all(tmp.path().join("a/b")).unwrap();
        fs::write(tmp.path().join("a/b/x.log"), "").unwrap();
        fs::create_dir_all(tmp.path().join("a/c")).unwrap();
        let clock = ManualClock::new();
        let (cache, reads) = counting_cache(&clock, MAX_ENTRIES);
        let plain = tmp.path().join("a/b");
        let doubled = PathBuf::from(format!("{}/a//b", tmp.path().display()));
        assert_eq!(
            plain, doubled,
            "Path equality ignores the doubled separator"
        );

        assert_eq!(names(&cache.get(&plain, AGE)), ["x.log"]);
        // A byte-different spelling of the same directory is a separate entry with the same
        // contents; a different directory never shares an entry.
        assert_eq!(names(&cache.get(&doubled, AGE)), ["x.log"]);
        assert!(names(&cache.get(&tmp.path().join("a/c"), AGE)).is_empty());
        assert_eq!(reads.load(Ordering::SeqCst), 3);
        assert_eq!(names(&cache.get(&plain, AGE)), ["x.log"]);
        assert_eq!(reads.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn zero_max_age_always_reads() {
        let tmp = TempDir::new().unwrap();
        let clock = ManualClock::new();
        let (cache, reads) = counting_cache(&clock, MAX_ENTRIES);
        let _ = cache.get(tmp.path(), Duration::ZERO);
        clock.advance(Duration::from_nanos(1));
        let _ = cache.get(tmp.path(), Duration::ZERO);
        assert_eq!(reads.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn stricter_caller_does_not_accept_older_listing() {
        let tmp = TempDir::new().unwrap();
        let clock = ManualClock::new();
        let (cache, reads) = counting_cache(&clock, MAX_ENTRIES);
        let _ = cache.get(tmp.path(), AGE);
        clock.advance(Duration::from_secs(1));
        let _ = cache.get(tmp.path(), AGE);
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        let _ = cache.get(tmp.path(), Duration::from_millis(500));
        assert_eq!(reads.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn transient_errors_are_not_reused() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        fs::write(dir.join("a.log"), b"x").unwrap();
        let clock = ManualClock::new();
        let reads = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&reads);
        let cache = cache_with_reader(&clock, move |dir| {
            if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(io::ErrorKind::Other)
            } else {
                read_listing(dir)
            }
        });

        assert_eq!(cache.get(&dir, AGE).unwrap_err(), io::ErrorKind::Other);
        assert_eq!(cache.len(), 0);
        // Within max_age, the next caller reads again and gets the real listing, which is kept.
        assert_eq!(names(&cache.get(&dir, AGE)), ["a.log"]);
        assert_eq!(names(&cache.get(&dir, AGE)), ["a.log"]);
        assert_eq!(reads.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn permission_denied_is_reused_like_absence() {
        let clock = ManualClock::new();
        let reads = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&reads);
        let cache = cache_with_reader(&clock, move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            Err(io::ErrorKind::PermissionDenied)
        });
        let dir = Path::new("/locked");
        assert!(cache.get(dir, AGE).is_err());
        assert!(cache.get(dir, AGE).is_err());
        assert_eq!(reads.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn deleted_dir_disappears_and_negative_entry_expires() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("pod");
        fs::create_dir(&dir).unwrap();
        let clock = ManualClock::new();
        let (cache, reads) = counting_cache(&clock, MAX_ENTRIES);

        assert!(cache.get(&dir, AGE).is_ok());
        fs::remove_dir(&dir).unwrap();
        clock.advance(AGE + Duration::from_nanos(1));
        assert_eq!(cache.get(&dir, AGE).unwrap_err(), io::ErrorKind::NotFound);

        // The absence is cached within max_age, then re-checked.
        fs::create_dir(&dir).unwrap();
        clock.advance(AGE);
        assert!(cache.get(&dir, AGE).is_err());
        assert_eq!(reads.load(Ordering::SeqCst), 2);
        clock.advance(Duration::from_nanos(1));
        assert!(cache.get(&dir, AGE).is_ok());
        assert_eq!(reads.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn entries_are_bounded_and_age_out() {
        let tmp = TempDir::new().unwrap();
        let clock = ManualClock::new();
        let max_entries = SHARDS * 2;
        let (cache, _) = counting_cache(&clock, max_entries);
        for index in 0..1_000 {
            let _ = cache.get(&tmp.path().join(index.to_string()), AGE);
            assert!(cache.len() <= max_entries, "{} entries", cache.len());
        }

        // Directories nobody asks about any more are dropped once every shard has been swept.
        let (cache, _) = counting_cache(&clock, MAX_ENTRIES);
        for index in 0..100 {
            let _ = cache.get(&tmp.path().join(format!("gone-{index}")), AGE);
        }
        assert_eq!(cache.len(), 100);
        clock.advance(MAX_LISTING_AGE + Duration::from_nanos(1));
        for index in 0..1_000 {
            let _ = cache.get(&tmp.path().join(format!("live-{index}")), AGE);
        }
        assert_eq!(cache.len(), 1_000);
    }

    #[test]
    fn concurrent_callers_share_one_read() {
        const THREADS: usize = 8;
        let tmp = TempDir::new().unwrap();
        fs::write(tmp.path().join("a.log"), "").unwrap();
        let clock = ManualClock::new();
        let reads = Arc::new(AtomicUsize::new(0));
        let cache = {
            let (reads, calls) = (Arc::clone(&reads), Arc::clone(&clock.calls));
            Arc::new(ListingCache::new(
                Box::new(clock.clone()),
                Box::new(move |dir: &Path| {
                    // Hold the read open until every caller has looked the slot up, so they all
                    // overlap with it.
                    let deadline = Instant::now() + Duration::from_secs(5);
                    while calls.load(Ordering::SeqCst) < THREADS && Instant::now() < deadline {
                        thread::yield_now();
                    }
                    reads.fetch_add(1, Ordering::SeqCst);
                    read_listing(dir)
                }),
                MAX_ENTRIES,
            ))
        };
        let results: Vec<_> = thread::scope(|scope| {
            let handles: Vec<_> = (0..THREADS)
                .map(|_| scope.spawn(|| names(&cache.get(tmp.path(), AGE))))
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect()
        });
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        assert!(results.iter().all(|names| names == &["a.log"]));
    }
}
