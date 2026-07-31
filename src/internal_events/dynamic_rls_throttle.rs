use metrics::counter;
use vector_lib::NamedInternalEvent;
use vector_lib::internal_event::InternalEvent;

use crate::transforms::dynamic_rls_throttle::ThrottleMode;

/// Every keyed log, emitted before the drop decision so the per-`(topic, system)` baseline is
/// stable across shadow and enforce. Excludes keyless logs (attributable inflow, not total ingress).
#[derive(Debug, NamedInternalEvent)]
pub(crate) struct DynamicRlsThrottleInflow {
    pub topic: String,
    pub system: String,
}

impl InternalEvent for DynamicRlsThrottleInflow {
    fn emit(self) {
        counter!(
            "dynamic_rls_throttle_inflow_total",
            "topic" => self.topic,
            "system" => self.system,
        )
        .increment(1);
    }
}

/// An over-quota log. The `mode` tag says what happened: `shadow` = forwarded but would drop,
/// `enforce` = dropped. Carries the per-`(topic, system)` breakdown `component_discarded_events_total`
/// (tagged only by `intentional`) cannot.
#[derive(Debug, NamedInternalEvent)]
pub(crate) struct DynamicRlsThrottleOverLimit {
    pub topic: String,
    pub system: String,
    pub mode: ThrottleMode,
}

impl InternalEvent for DynamicRlsThrottleOverLimit {
    fn emit(self) {
        counter!(
            "dynamic_rls_throttle_over_limit_total",
            "topic" => self.topic,
            "system" => self.system,
            "mode" => self.mode.as_str(),
        )
        .increment(1);
    }
}
