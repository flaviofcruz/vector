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

/// A write attempt failed with a retryable error and will be retried.
///
/// Counts attempts, not batches: a batch retried three times counts three. Permanent failures
/// are not counted here; they reach `component_errors_total` with their `error_code` instead.
#[derive(Debug, NamedInternalEvent)]
pub(crate) struct ZerobusRetryableError {
    pub error_code: &'static str,
}

impl InternalEvent for ZerobusRetryableError {
    fn emit(self) {
        counter!(
            "zerobus_retryable_errors_total",
            "error_code" => self.error_code,
        )
        .increment(1);
    }
}

/// An auth/token-class failure discarded the active stream; the rebuild backs
/// off before retrying. Worth its own signal because the failure itself is
/// non-retryable from the SDK's point of view: without this counter the
/// affected batches' only trace is a spike of `invalid_config`-shaped errors.
#[derive(Debug, NamedInternalEvent)]
pub(crate) struct ZerobusAuthStreamDiscarded {
    pub error_code: &'static str,
}

impl InternalEvent for ZerobusAuthStreamDiscarded {
    fn emit(self) {
        warn!(
            message = "Zerobus stream failed with an auth/token error; discarding it so it can be rebuilt once credentials recover. Batches are retried, not dropped.",
            error_code = self.error_code,
            error_type = error_type::REQUEST_FAILED,
            stage = error_stage::SENDING,
            internal_log_rate_limit = true,
        );
        counter!(
            "zerobus_auth_stream_discard_total",
            "error_code" => self.error_code,
        )
        .increment(1);
    }
}

/// A stream (re)build attempt itself failed with an auth/token-class error
/// (e.g. the token mint is down, or a grant has not propagated yet). Distinct
/// from [`ZerobusAuthStreamDiscarded`], which only fires on a failure of an
/// already-established stream: when the very first build fails — a sustained
/// token-mint outage — the discard counter stays at zero, so this is what makes
/// the outage visible. Unlabeled by table, matching the discard event: a sink
/// writes one table, `component_id` separates instances, and the log line has
/// the name.
#[derive(Debug, NamedInternalEvent)]
pub(crate) struct ZerobusAuthStreamBuildFailed {
    pub error_code: &'static str,
    pub error: String,
}

impl InternalEvent for ZerobusAuthStreamBuildFailed {
    fn emit(self) {
        warn!(
            message = "Zerobus stream creation failed with an auth/token error; the rebuild backs off until credentials recover. Batches are retried, not dropped.",
            error = %self.error,
            error_code = self.error_code,
            error_type = error_type::REQUEST_FAILED,
            stage = error_stage::SENDING,
            internal_log_rate_limit = true,
        );
        counter!(
            "zerobus_auth_stream_build_failure_total",
            "error_code" => self.error_code,
        )
        .increment(1);
    }
}

/// Ingestion recovered after one or more auth/token failures — the stream was
/// rebuilt (or a still-live stream delivered again) without a restart.
/// `failures` is the consecutive-failure count the backoff had reached, giving
/// a sense of the outage length.
#[derive(Debug, NamedInternalEvent)]
pub(crate) struct ZerobusAuthStreamRebuilt {
    pub failures: u32,
}

impl InternalEvent for ZerobusAuthStreamRebuilt {
    fn emit(self) {
        info!(
            message = "Zerobus ingestion recovered after auth/token failures; resuming on a rebuilt stream.",
            failures = self.failures,
            internal_log_rate_limit = true,
        );
        counter!("zerobus_auth_stream_rebuild_total").increment(1);
    }
}

/// Freshness counters for one acked batch, summed so they are bumped once per batch.
///
/// A negative lag means the producer or LP clock runs ahead of this host. It is counted
/// here and kept out of the histograms, so skew shows up instead of pulling latency below zero.
#[derive(Debug, NamedInternalEvent)]
pub(crate) struct ZerobusAckFreshness {
    pub table: String,
    pub negative_born_to_saved: u64,
    pub negative_lp_ingest_to_saved: u64,
    pub missing_born_at: u64,
    pub over_sla_born_to_saved: u64,
}

impl InternalEvent for ZerobusAckFreshness {
    fn emit(self) {
        counter!(
            "zerobus_ack_negative_latency_total",
            "table" => self.table.clone(),
            "series" => "born_to_saved",
        )
        .increment(self.negative_born_to_saved);
        counter!(
            "zerobus_ack_negative_latency_total",
            "table" => self.table.clone(),
            "series" => "lp_ingest_to_saved",
        )
        .increment(self.negative_lp_ingest_to_saved);
        counter!("zerobus_ack_over_sla_total", "table" => self.table.clone())
            .increment(self.over_sla_born_to_saved);
        counter!("zerobus_ack_missing_born_at_total", "table" => self.table)
            .increment(self.missing_born_at);
    }
}
