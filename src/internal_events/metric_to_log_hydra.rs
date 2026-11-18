use metrics::counter;
use vector_lib::NamedInternalEvent;
use vector_lib::internal_event::{ComponentEventsDropped, INTENTIONAL, InternalEvent};

/// `count` Hydra drops for one reason, from `metric_to_log_hydra` or `hydra_remote_write`.
/// Increments `metric_to_log_hydra_dropped_total{reason}` by `count`; when `counts_as_loss`, also
/// emits the standard `component_discarded_events_total` discard.
#[derive(Debug, NamedInternalEvent)]
pub struct MetricToLogHydraDropped {
    /// `reason` label (from `DropReason::as_str`).
    pub reason: &'static str,
    /// Whether the drop counts as loss.
    pub counts_as_loss: bool,
    /// Number of samples dropped.
    pub count: usize,
}

impl InternalEvent for MetricToLogHydraDropped {
    fn emit(self) {
        counter!("metric_to_log_hydra_dropped_total", "reason" => self.reason)
            .increment(self.count as u64);
        if self.counts_as_loss {
            emit!(ComponentEventsDropped::<INTENTIONAL> {
                count: self.count,
                reason: self.reason,
            });
        }
    }
}
