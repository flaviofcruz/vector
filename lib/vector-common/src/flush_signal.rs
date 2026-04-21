use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// A signal that the buffer reader can set to request downstream consumers
/// (e.g. `PartitionedBatcher`) to flush their pending batches without terminating.
///
/// This is used during shutdown when the writer is done but there are still
/// unacknowledged records in the buffer. The flush signal causes batchers to
/// drain their open batches so that the sink can process them and send
/// acknowledgements back, allowing the buffer to fully drain.
#[derive(Clone, Debug)]
pub struct FlushSignal(Arc<AtomicBool>);

impl FlushSignal {
    /// Creates a new `FlushSignal` in the unset state.
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    /// Sets the flush signal.
    pub fn set(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// Atomically reads and clears the flush signal, returning `true` if it was set.
    pub fn take(&self) -> bool {
        self.0.swap(false, Ordering::AcqRel)
    }
}

impl Default for FlushSignal {
    fn default() -> Self {
        Self::new()
    }
}

tokio::task_local! {
    /// Task-local storage for the flush signal. Set by the topology builder
    /// before running a sink task, and read by `PartitionedBatcher::new()`
    /// to automatically wire up flush-on-shutdown behavior for any sink
    /// that uses batched partitioning with a disk buffer.
    static FLUSH_SIGNAL: FlushSignal;
}

/// Sets the flush signal for the duration of the given future.
///
/// Call this in the sink task before running the sink so that any
/// `PartitionedBatcher` created within picks up the signal automatically.
pub fn with_flush_signal<F: std::future::Future>(
    signal: FlushSignal,
    f: F,
) -> tokio::task::futures::TaskLocalFuture<FlushSignal, F> {
    FLUSH_SIGNAL.scope(signal, f)
}

/// Attempts to read the current task-local flush signal.
///
/// Returns `Some(FlushSignal)` if one has been set for the current task
/// (i.e. the sink is backed by a disk buffer), or `None` otherwise.
pub fn get_task_flush_signal() -> Option<FlushSignal> {
    FLUSH_SIGNAL.try_with(|s| s.clone()).ok()
}
