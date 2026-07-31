//! Periodically emits jemalloc allocator stats as internal metrics.
//! `resident` gives the authoritative RSS contribution; counters are cached until the epoch advances.
use std::time::Duration;

use tikv_jemalloc_ctl::{arenas, epoch, raw, stats, stats_print};
use tokio::time::interval;

use crate::internal_events::JemallocStats;

const JEMALLOC_STATS_INTERVAL_SECS: u64 = 5;

/// How often (seconds) to log the full jemalloc `malloc_stats_print` JSON dump (per-size-class bins).
/// Captures C/FFI and pre-startup allocations invisible to the Rust tracer. 0 = disabled (default).
const JEMALLOC_STATS_DUMP_ENV: &str = "VECTOR_JEMALLOC_STATS_DUMP_SECS";
const JEMALLOC_STATS_DUMP_DEFAULT_SECS: u64 = 0;

/// Reads jemalloc stats on an interval and emits them as `vector_jemalloc_*` gauges.
pub async fn report_jemalloc_stats() {
    let dump_secs = std::env::var(JEMALLOC_STATS_DUMP_ENV)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(JEMALLOC_STATS_DUMP_DEFAULT_SECS);
    let dump_secs = (dump_secs > 0).then_some(dump_secs);
    if let Some(secs) = dump_secs {
        info!(
            message = "jemalloc full-stats dump enabled.",
            interval_secs = secs
        );
    }
    let mut interval = interval(Duration::from_secs(JEMALLOC_STATS_INTERVAL_SECS));
    let mut elapsed_since_dump = 0u64;
    loop {
        interval.tick().await;
        match collect() {
            Ok(event) => emit!(event),
            // A transient mallctl error must never take down Vector — skip the
            // cycle and try again next tick.
            Err(error) => warn!(message = "Failed to read jemalloc stats.", %error),
        }
        if let Some(secs) = dump_secs {
            elapsed_since_dump += JEMALLOC_STATS_INTERVAL_SECS;
            if elapsed_since_dump >= secs {
                elapsed_since_dump = 0;
                dump_full_stats();
            }
        }
    }
}

/// Logs the full jemalloc stats JSON (merged-arena, per-size-class bins; no per-arena or mutex sections).
fn dump_full_stats() {
    let mut options = stats_print::Options::default();
    options.json_format = true;
    options.skip_per_arena = true;
    options.skip_mutex_statistics = true;
    let mut buf = Vec::new();
    match stats_print::stats_print(&mut buf, options) {
        Ok(()) => info!(
            message = "jemalloc full stats dump.",
            stats = %String::from_utf8_lossy(&buf)
        ),
        Err(error) => warn!(message = "Failed to dump jemalloc stats.", %error),
    }
}

fn collect() -> Result<JemallocStats, tikv_jemalloc_ctl::Error> {
    // Advance the epoch to refresh cached counters before reading.
    epoch::advance()?;

    // Index 4096 is MALLCTL_ARENAS_ALL — the merged all-arenas view.
    // SAFETY: mallctl names are valid, NUL-terminated, and return size_t (usize).
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
