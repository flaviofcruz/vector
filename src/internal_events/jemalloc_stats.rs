use metrics::gauge;
use vector_lib::NamedInternalEvent;
use vector_lib::internal_event::InternalEvent;

/// Process-level jemalloc allocator statistics, read from the `mallctl` stats
/// interface. These surface the real memory breakdown (resident ≈ RSS) that
/// allocation tracing under-counts, and expose the arena count.
#[derive(Debug, NamedInternalEvent)]
pub struct JemallocStats {
    pub allocated: usize,
    pub active: usize,
    pub resident: usize,
    pub retained: usize,
    pub metadata: usize,
    pub mapped: usize,
    pub narenas: u32,
    /// Real dirty memory (jemalloc `pdirty` x page size): freed pages held
    /// resident for fast reuse, not yet returned to the OS. Authoritative —
    /// unlike the (resident - active - metadata) derivation.
    pub dirty: usize,
    /// Muzzy memory (jemalloc `pmuzzy` x page size): pages MADV_FREE'd but not
    /// yet MADV_DONTNEED'd. ~0 unless muzzy_decay_ms is raised.
    pub muzzy: usize,
}

impl InternalEvent for JemallocStats {
    fn emit(self) {
        gauge!("jemalloc_allocated_bytes").set(self.allocated as f64);
        gauge!("jemalloc_active_bytes").set(self.active as f64);
        gauge!("jemalloc_resident_bytes").set(self.resident as f64);
        gauge!("jemalloc_retained_bytes").set(self.retained as f64);
        gauge!("jemalloc_metadata_bytes").set(self.metadata as f64);
        gauge!("jemalloc_mapped_bytes").set(self.mapped as f64);
        gauge!("jemalloc_narenas").set(self.narenas as f64);
        gauge!("jemalloc_dirty_bytes").set(self.dirty as f64);
        gauge!("jemalloc_muzzy_bytes").set(self.muzzy as f64);
    }
}
