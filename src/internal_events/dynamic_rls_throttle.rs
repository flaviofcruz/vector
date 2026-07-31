use metrics::{counter, gauge};
use vector_lib::NamedInternalEvent;
use vector_lib::internal_event::{InternalEvent, error_stage, error_type};

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

// --- Health of the throttle mechanism itself (does it work, vs the decision metrics above) ------
// Low-cardinality (no topic/system label); emitted inside the component span, so they carry its
// `component_id` and stay attributable per instance (e.g. the neon per-system vs group throttles).

/// Seconds since the last successful sidecar report. Climbs toward `max_staleness_secs` when the
/// round-trip is broken. A gauge, not a counter, so a steady value still reads as current.
#[derive(Debug, NamedInternalEvent)]
pub(crate) struct DynamicRlsThrottleReportFreshness {
    pub seconds_since_last_success: f64,
}

impl InternalEvent for DynamicRlsThrottleReportFreshness {
    fn emit(self) {
        gauge!("dynamic_rls_throttle_report_success_age_seconds")
            .set(self.seconds_since_last_success);
    }
}

/// A failed `ReportCounts` round-trip. Uses the standard `component_errors_total` contract so it
/// reuses the existing VA dashboards/alerts; this event owns the failure `error!` log. `error` is
/// the underlying cause.
#[derive(Debug, NamedInternalEvent)]
pub(crate) struct DynamicRlsThrottleReportError {
    pub reason: ReportErrorReason,
    pub error: String,
}

/// Cause of a failed report, mapped to the standard `error_type`: a slow/unreachable sidecar vs a
/// request/protocol error point at different causes.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ReportErrorReason {
    /// Exceeded `report_timeout_secs`.
    TimedOut,
    /// Connect refused, non-zero gRPC status, decode error, etc.
    RequestFailed,
}

// One arm per reason (not a computed value): the check-events lint requires the `error_type` tag to
// be a literal `error_type::` constant.
impl InternalEvent for DynamicRlsThrottleReportError {
    fn emit(self) {
        match self.reason {
            ReportErrorReason::TimedOut => {
                error!(
                    message = "Failed to report counts to the sidecar, failing open if stale.",
                    error = %self.error,
                    error_code = "rls_report",
                    error_type = error_type::TIMED_OUT,
                    stage = error_stage::PROCESSING,
                    internal_log_rate_limit = true,
                );
                counter!(
                    "component_errors_total",
                    "error_code" => "rls_report",
                    "error_type" => error_type::TIMED_OUT,
                    "stage" => error_stage::PROCESSING,
                )
                .increment(1);
            }
            ReportErrorReason::RequestFailed => {
                error!(
                    message = "Failed to report counts to the sidecar, failing open if stale.",
                    error = %self.error,
                    error_code = "rls_report",
                    error_type = error_type::REQUEST_FAILED,
                    stage = error_stage::PROCESSING,
                    internal_log_rate_limit = true,
                );
                counter!(
                    "component_errors_total",
                    "error_code" => "rls_report",
                    "error_type" => error_type::REQUEST_FAILED,
                    "stage" => error_stage::PROCESSING,
                )
                .increment(1);
            }
        }
    }
}

/// The reporter cleared a stale over-limit set (fail-open): enforcement silently disengaged. A
/// bespoke counter, not `component_errors_total` — fail-open is a designed safety response.
#[derive(Debug, NamedInternalEvent)]
pub(crate) struct DynamicRlsThrottleFailOpen;

impl InternalEvent for DynamicRlsThrottleFailOpen {
    fn emit(self) {
        counter!("dynamic_rls_throttle_fail_open_total").increment(1);
    }
}

/// `1.0` when the throttle mechanism is running, `0.0` when it is not because the sidecar descriptor
/// failed to load — throttling is off for the pod's whole life with no reporter running, otherwise
/// invisible. Distinct from whether enforcement drops (that is the `over_limit_total` `mode` tag).
/// Emitted once per reload; the non-operational path never re-emits, so the universe
/// `expire_metrics_per_metric_set` config must keep it past the 300s default expiry (see the
/// `last_config_reload_success` precedent).
#[derive(Debug, NamedInternalEvent)]
pub(crate) struct DynamicRlsThrottleOperational {
    pub operational: bool,
}

impl InternalEvent for DynamicRlsThrottleOperational {
    fn emit(self) {
        gauge!("dynamic_rls_throttle_operational").set(if self.operational { 1.0 } else { 0.0 });
    }
}
