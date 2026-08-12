use metrics::counter;
use vector_lib::internal_event::{ComponentEventsDropped, InternalEvent, INTENTIONAL};
use vector_lib::NamedInternalEvent;

/// One per `metric_to_log_hydra` drop. Increments `metric_to_log_hydra_dropped_total{reason}`;
/// when `counts_as_loss`, also emits the standard `component_discarded_events_total` discard.
#[derive(Debug, NamedInternalEvent)]
pub struct MetricToLogHydraDropped {
    /// `reason` label (from `DropReason::as_str`).
    pub reason: &'static str,
    /// Whether the drop counts as loss.
    pub counts_as_loss: bool,
}

impl InternalEvent for MetricToLogHydraDropped {
    fn emit(self) {
        counter!("metric_to_log_hydra_dropped_total", "reason" => self.reason).increment(1);
        if self.counts_as_loss {
            emit!(ComponentEventsDropped::<INTENTIONAL> {
                count: 1,
                reason: self.reason,
            });
        }
    }
}
