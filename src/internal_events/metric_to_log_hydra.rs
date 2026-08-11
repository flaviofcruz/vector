use metrics::counter;
use vector_lib::internal_event::{ComponentEventsDropped, Count, Registered, INTENTIONAL};

/// Emit payload for a `metric_to_log_hydra` drop: how many events were dropped, the
/// low-cardinality `reason` label (from `DropReason::as_str`), and whether this reason counts as
/// data loss (from `DropReason::counts_as_loss`).
#[derive(Clone, Copy)]
pub struct MetricToLogHydraDrop {
    pub count: usize,
    pub reason: &'static str,
    pub counts_as_loss: bool,
}

vector_lib::registered_event!(
    MetricToLogHydraEventsDropped => {
        events_dropped: Registered<ComponentEventsDropped<'static, INTENTIONAL>>
            = register!(ComponentEventsDropped::<INTENTIONAL>::from(
                "metric_to_log_hydra dropped a metric."
            )),
    }

    // `metric_to_log_hydra_dropped_total{reason="..."}` — the per-reason breakdown, recorded for
    // EVERY drop so even non-loss reasons (an invalid gauge value) stay observable.
    // `component_discarded_events_total{intentional="true"}` — the standard discard metric the
    // completeness/SLO dashboards consume, incremented only when the reason counts as loss.
    fn emit(&self, data: MetricToLogHydraDrop) {
        counter!("metric_to_log_hydra_dropped_total", "reason" => data.reason)
            .increment(data.count as u64);
        if data.counts_as_loss {
            self.events_dropped.emit(Count(data.count));
        }
    }
);
