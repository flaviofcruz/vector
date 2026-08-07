use metrics::counter;
use vector_lib::NamedInternalEvent;
use vector_lib::internal_event::{InternalEvent, error_stage, error_type};

/// What came of a stale-schema rejection.
///
/// Drift is invisible in aggregate — pods started after a widening commit fine
/// while older pods reject every batch, so throughput and error rates read as
/// half-healthy — making this the only signal that separates "recovered" from
/// "stuck". Unlabeled by table: a sink writes one table, `component_id` (from the
/// enclosing component span) separates instances, and the log line has the name.
#[derive(Debug, NamedInternalEvent)]
pub(crate) struct ZerobusSchemaReloadOutcome {
    pub outcome: SchemaReloadOutcome,
}

/// `NoProgress` is the one worth alerting on: Unity Catalog and the ingestion
/// server disagree about the table, so every batch fails permanently until
/// someone intervenes.
#[derive(Clone, Copy, Debug)]
pub(crate) enum SchemaReloadOutcome {
    /// Re-resolved a different schema; the batch is retried against it.
    Reloaded,
    /// The refetch returned the rejected schema, so the rejection is permanent.
    NoProgress,
}

impl SchemaReloadOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Reloaded => "reloaded",
            Self::NoProgress => "no_progress",
        }
    }
}

impl InternalEvent for ZerobusSchemaReloadOutcome {
    fn emit(self) {
        counter!(
            "zerobus_schema_reload_total",
            "outcome" => self.outcome.as_str(),
        )
        .increment(1);
    }
}

/// The server rejected the cached schema and the Unity Catalog refetch then
/// failed too (5xx, auth, timeout), so the sink cannot tell whether the table
/// was in fact widened correctly.
///
/// Carries both failures because this path returns the UC one *instead of* the
/// rejection, so `server_rejection` — which names the drifted column — would
/// otherwise be lost.
#[derive(Debug, NamedInternalEvent)]
pub(crate) struct ZerobusSchemaRefetchFailed {
    pub unity_catalog_failure: String,
    pub server_rejection: String,
}

impl InternalEvent for ZerobusSchemaRefetchFailed {
    fn emit(self) {
        error!(
            message = "Zerobus rejected the sink's schema as stale, but re-resolving it from Unity Catalog failed.",
            // The log keys are fixed by Vector's error-event contract (`error`
            // is what dashboards read), so they stay shorter than the fields.
            error = %self.unity_catalog_failure,
            rejection = %self.server_rejection,
            error_code = "zerobus_schema_reload",
            error_type = error_type::REQUEST_FAILED,
            stage = error_stage::SENDING,
            internal_log_rate_limit = true,
        );
        counter!(
            "component_errors_total",
            "error_code" => "zerobus_schema_reload",
            "error_type" => error_type::REQUEST_FAILED,
            "stage" => error_stage::SENDING,
        )
        .increment(1);
    }
}
