//! Periodically reports jemalloc allocator statistics as internal metrics.
//!
//! jemalloc exposes process-level allocator stats (`allocated`, `active`,
//! `resident`, `retained`, `metadata`, `mapped`) and the arena count via its
//! `mallctl` interface. `resident` is effectively the allocator's contribution
//! to RSS, so these give the authoritative memory picture — unlike allocation
//! tracing, which only counts bytes the application requested.
//!
//! The counters are cached and only refreshed when the `epoch` is advanced, so
//! we advance it once per cycle before reading.
use std::time::Duration;

use tikv_jemalloc_ctl::{arenas, epoch, raw, stats};
use tokio::time::interval;

use crate::internal_events::JemallocStats;

/// Interval between reads. Matches the allocation-tracing reporting cadence.
const JEMALLOC_STATS_INTERVAL_SECS: u64 = 5;

/// Reads jemalloc stats on an interval and emits them as `vector_jemalloc_*`
/// gauges. Runs for the life of the process.
pub async fn report_jemalloc_stats() {
    let mut interval = interval(Duration::from_secs(JEMALLOC_STATS_INTERVAL_SECS));
    loop {
        interval.tick().await;
        match collect() {
            Ok(event) => emit!(event),
            // A transient mallctl error must never take down Vector — skip the
            // cycle and try again next tick.
            Err(error) => warn!(message = "Failed to read jemalloc stats.", %error),
        }
    }
}

/// Advances the epoch (refreshing the cached counters) and reads the current
/// allocator stats.
fn collect() -> Result<JemallocStats, tikv_jemalloc_ctl::Error> {
    // Must advance the epoch first or every stat below is frozen at its previous
    // value (the gauges would flatline).
    epoch::advance()?;

    // Authoritative dirty/muzzy: jemalloc reports these as PAGE COUNTS per arena.
    // The `4096` index is MALLCTL_ARENAS_ALL — the merged all-arenas view — so
    // `stats.arenas.4096.pdirty` is total dirty pages across every arena. We
    // convert to bytes via the page size. This is the real held/purgeable memory,
    // far more reliable than deriving it as (resident - active - metadata), which
    // is noisy and can even go negative.
    // SAFETY: the mallctl names are valid, NUL-terminated, and read as size_t
    // (usize), matching jemalloc's return type for these counters.
    let page: usize = unsafe { raw::read(b"arenas.page\0")? };
    let pdirty: usize = unsafe { raw::read(b"stats.arenas.4096.pdirty\0")? };
    let pmuzzy: usize = unsafe { raw::read(b"stats.arenas.4096.pmuzzy\0")? };

    Ok(JemallocStats {
        allocated: stats::allocated::read()?,
        active: stats::active::read()?,
        resident: stats::resident::read()?,
        retained: stats::retained::read()?,
        metadata: stats::metadata::read()?,
        mapped: stats::mapped::read()?,
        narenas: arenas::narenas::read()?,
        dirty: pdirty.saturating_mul(page),
        muzzy: pmuzzy.saturating_mul(page),
    })
}
