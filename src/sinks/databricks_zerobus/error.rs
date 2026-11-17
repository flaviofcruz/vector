//! Error types for the Zerobus sink.

use databricks_zerobus_ingest_sdk::{GrpcCode, ZerobusError};
use snafu::Snafu;
use vector_lib::event::EventStatus;

/// Errors that can occur when using the Zerobus sink.
#[derive(Debug, Snafu)]
#[allow(clippy::enum_variant_names)]
pub enum ZerobusSinkError {
    /// Configuration validation failed.
    #[snafu(display("Configuration error: {}", message))]
    ConfigError { message: String },

    /// Event encoding failed. `schema_constraint` marks a value the table's schema forbids
    /// (e.g. a null in a non-nullable column), as opposed to any other encoding failure.
    #[snafu(display("Encoding error: {}", message))]
    EncodingError {
        message: String,
        schema_constraint: bool,
    },

    /// A blocking encoding worker failed before it returned a batch.
    #[snafu(display("Encoding worker failed: {}", message))]
    EncodingTaskFailed { message: String },

    /// Zerobus SDK error.
    #[snafu(display("Zerobus error: {}", source))]
    ZerobusError { source: ZerobusError },

    /// Stream initialization failed.
    #[snafu(display("Stream initialization failed: {}", source))]
    StreamInitError { source: ZerobusError },

    /// Record ingestion failed, and the stream is still open.
    #[snafu(display("Record ingestion failed: {}", source))]
    IngestionError { source: ZerobusError },

    /// Record ingestion failed, and the failure closed the stream (or poisoned
    /// the multiplexed stream).
    ///
    /// The SDK hands the error that closed a stream to every batch still in
    /// flight on it, and a poisoned mux replays it for every later ingest, so
    /// it says nothing about this batch's data. It is never
    /// [`ErrorCategory::BadData`], whatever its code.
    #[snafu(display("Record ingestion failed and the stream closed: {}", source))]
    TerminalIngestionError { source: ZerobusError },

    /// The shared stream was closed concurrently (by shutdown or retry-driven
    /// replacement) before this ingest could run. Retryable: the next attempt
    /// will create a fresh stream via `get_or_create_stream`.
    #[snafu(display("Zerobus stream was closed concurrently"))]
    StreamClosed,

    /// Resolving the table schema from Unity Catalog failed. `retryable`
    /// distinguishes transient failures (network, 5xx, 408, 429) from
    /// permanent ones (404, 401, 403, ...). `status` is the HTTP status when
    /// Unity Catalog answered with an error, `None` when the request itself failed.
    #[snafu(display("Schema resolution failed: {}", message))]
    SchemaError {
        message: String,
        retryable: bool,
        status: Option<u16>,
    },

    /// The server rejected the sink's schema as stale, and the sink has since
    /// re-resolved the table and dropped the rejected stream. Retryable: the
    /// next attempt re-encodes against the refreshed schema on a fresh stream.
    ///
    /// Distinct from the underlying `InvalidSchema`, which the SDK reports as
    /// non-retryable because *its* schema is fixed. Here the schema has actually
    /// changed between attempts, so a retry is not a repeat of the same request.
    #[snafu(display("Zerobus schema reloaded after rejection ({}); retrying", message))]
    SchemaReloaded { message: String },

    /// A previous auth/token-class failure discarded the stream, and its
    /// rebuild is still backing off. Retryable: the batch must not be acked,
    /// and once the backoff elapses the next attempt rebuilds the stream.
    #[snafu(display(
        "Zerobus stream rebuild backing off after an auth failure; retry in {retry_after_ms}ms"
    ))]
    StreamRebuildBackoff { retry_after_ms: u64 },
}

/// How the sink treats a failed batch, from the error that failed it.
///
/// With end-to-end acknowledgements the sink's verdict decides whether the source
/// deletes the queue message (Delivered/Rejected) or keeps it for redelivery
/// (Errored). Only errors known to describe the data itself may drop a batch;
/// a failure of a dependency, of configuration, or of anything unrecognised must
/// not delete good data.
///
/// On disk-buffered sinks `Errored` is not replayed: the `disk_v2` ledger
/// finalizer ignores the status (`lib/vector-buffers/src/variants/disk_v2/ledger.rs`),
/// so the batch is dropped there either way. The difference is only in what is
/// counted: `zerobus_errored_batches_total` rather than
/// `component_discarded_events_total`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorCategory {
    /// Known bad data: retrying cannot help. The batch is `Rejected` (dropped).
    BadData,
    /// Transient: retried in the sink with a bounded budget, then `Errored`.
    Retryable,
    /// Everything else that is not retryable (configuration, permanent
    /// dependency failures, unknown codes): `Errored` at once, no in-sink retry.
    Dependency,
}

/// `error_code`s that are known to describe bad data, and so may drop a batch.
///
/// Any code not listed here (including codes added later) is [`ErrorCategory::Dependency`],
/// so a new failure mode requeues instead of silently deleting data. A listed code is bad
/// data only on an error about one batch (see `ZerobusSinkError::can_be_bad_data`): the
/// same code from a stream setup failure, or from the error that closed a stream, is
/// `Dependency`.
///
/// `invalid_argument` is also what the SDK returns for an empty batch, but the
/// service never sends one: a batch whose rows were all dropped by encoding is
/// acknowledged as delivered-nothing before it reaches the SDK.
pub const BAD_DATA_ERROR_CODES: &[&str] = &[
    "encoding_failed",
    "invalid_argument",
    "grpc_invalid_argument",
];

/// Whether an SDK error is auth/token-class: the credential itself was
/// rejected or could not be minted, as opposed to a transport or data problem.
///
/// The SDK classifies these as non-retryable because its own recovery cannot
/// fix them, but they are transient in practice — a token-mint 401
/// (`InvalidUCTokenError`, e.g. `invalid_authorization_details` cold-miss) or a
/// server `UNAUTHENTICATED`/`PERMISSION_DENIED` can clear on their own (grant
/// and RAR propagation is eventual), so the sink must rebuild the stream and
/// retry instead of rejecting every batch until restart. `PERMISSION_DENIED`
/// is included deliberately, with a known trade-off: a *permanently* revoked
/// grant would then retry indefinitely under LP's unbounded request retries and
/// blocking buffers, stalling the pod — that state must be alerted on
/// (`zerobus_auth_stream_discard_total`, or
/// `zerobus_auth_stream_build_failure_total` when the very first build fails)
/// rather than encoded away here.
/// Genuinely permanent misconfigurations (bad table, bad schema, bad
/// arguments) are excluded.
pub(crate) fn sdk_error_is_auth(error: &ZerobusError) -> bool {
    match error {
        ZerobusError::InvalidUCTokenError(_) => true,
        ZerobusError::CreateStreamError(status) | ZerobusError::StreamClosedError(status) => {
            matches!(
                status.code(),
                GrpcCode::Unauthenticated | GrpcCode::PermissionDenied
            )
        }
        _ => false,
    }
}

impl ZerobusSinkError {
    /// Whether the server rejected the sink's Arrow schema, making a refetch
    /// from Unity Catalog worth attempting.
    ///
    /// Every `InvalidSchema` qualifies, whatever the server's `causes` say: the
    /// only way to learn whether a rejection is fixable is to ask Unity Catalog
    /// and compare, which `ZerobusService::reload_schema_after_rejection` does.
    /// Judging a rejection unfixable from its cause skips that path entirely and
    /// drops the batch silently, and causes can look permanent without being so
    /// (`ALTER COLUMN TYPE` arrives as `TYPE_INCOMPATIBLE`).
    ///
    /// The SDK surfaces `InvalidSchema` from both stream setup (`build_arrow` →
    /// `StreamInitError`) and mid-stream recovery (`wait_for_offset`/`flush` →
    /// `IngestionError`), so both variants are inspected.
    pub const fn is_schema_rejection(&self) -> bool {
        let (Self::ZerobusError { source }
        | Self::StreamInitError { source }
        | Self::IngestionError { source }
        | Self::TerminalIngestionError { source }) = self
        else {
            return false;
        };
        matches!(source, ZerobusError::InvalidSchema { .. })
    }

    /// Whether the batch is sent again on a fresh stream before this error is
    /// answered: a failure that closed the stream under the batch and that the
    /// request retry layer would not retry. Such an error reaches every batch in
    /// flight on the stream, so it most likely says nothing about this one. A
    /// schema rejection is excluded; `recover_from_schema_rejection` handles it.
    pub fn retries_on_fresh_stream(&self) -> bool {
        matches!(self, Self::TerminalIngestionError { .. })
            && !self.is_retryable()
            && !self.is_schema_rejection()
    }

    /// Whether this error should be retried.
    ///
    /// Auth/token-class SDK errors (see [`sdk_error_is_auth`]) are retryable
    /// here even though the SDK marks them non-retryable: its recovery cannot
    /// fix a rejected credential, but the sink can discard the stream, back
    /// off, and rebuild once the mint recovers. The LP deployment leaves the
    /// request retry count unbounded and its buffers blocking, so the batches
    /// are held (not acked, not dropped) until the stream recovers.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::ZerobusError { source }
            | Self::StreamInitError { source }
            | Self::IngestionError { source }
            | Self::TerminalIngestionError { source } => {
                source.is_retryable() || sdk_error_is_auth(source)
            }
            Self::StreamClosed | Self::EncodingTaskFailed { .. } => true,
            Self::SchemaError { retryable, .. } => *retryable,
            Self::SchemaReloaded { .. } | Self::StreamRebuildBackoff { .. } => true,
            Self::ConfigError { .. } | Self::EncodingError { .. } => false,
        }
    }

    /// Whether this error is auth/token-class (see [`sdk_error_is_auth`]).
    pub fn is_auth_error(&self) -> bool {
        match self {
            Self::ZerobusError { source }
            | Self::StreamInitError { source }
            | Self::IngestionError { source }
            | Self::TerminalIngestionError { source } => sdk_error_is_auth(source),
            _ => false,
        }
    }

    /// Fixed classification of this error for the `error_code` metric label.
    ///
    /// Every value is a literal here, never derived from error text, so the label stays
    /// bounded; new variants extend this `match`.
    pub fn error_code(&self) -> &'static str {
        match self {
            Self::ConfigError { .. } => "config",
            Self::EncodingTaskFailed { .. } => "encoding_worker_failed",
            Self::EncodingError {
                schema_constraint: true,
                ..
            } => "encoding_schema_constraint",
            Self::EncodingError { .. } => "encoding_failed",
            Self::ZerobusError { source }
            | Self::StreamInitError { source }
            | Self::IngestionError { source }
            | Self::TerminalIngestionError { source } => sdk_error_code(source),
            Self::StreamClosed => "stream_closed_concurrently",
            Self::SchemaError { status, .. } => unity_catalog_error_code(*status),
            Self::SchemaReloaded { .. } => "schema_reloaded",
            Self::StreamRebuildBackoff { .. } => "stream_rebuild_backoff",
        }
    }

    /// Whether this error is about one batch, so its code may classify it as bad data.
    ///
    /// Only two places judge a single batch: the encoder, and the SDK rejecting
    /// a batch on a stream that stays open. Every other error carrying a
    /// bad-data code says nothing about the batch and would recur for every one:
    /// a stream setup failure (a CreateStream `INVALID_ARGUMENT` from a bad
    /// option, table name or header, or an SDK builder `InvalidArgument`), an
    /// SDK error outside ingestion, and the error that closed a stream.
    const fn can_be_bad_data(&self) -> bool {
        matches!(
            self,
            Self::IngestionError { .. } | Self::EncodingError { .. }
        )
    }

    /// Which of the three outcomes applies to a batch that failed with this error.
    pub fn category(&self) -> ErrorCategory {
        if self.is_retryable() {
            ErrorCategory::Retryable
        } else if self.can_be_bad_data() && BAD_DATA_ERROR_CODES.contains(&self.error_code()) {
            ErrorCategory::BadData
        } else {
            ErrorCategory::Dependency
        }
    }

    /// Event status to apply to a batch's finalizers when this error occurs.
    /// Only known bad data is `Rejected`; every other failure is `Errored`.
    pub fn event_status(&self) -> EventStatus {
        match self.category() {
            ErrorCategory::BadData => EventStatus::Rejected,
            ErrorCategory::Retryable | ErrorCategory::Dependency => EventStatus::Errored,
        }
    }
}

pub(crate) fn sdk_error_code(error: &ZerobusError) -> &'static str {
    match error {
        ZerobusError::InvalidSchema { .. } => "schema_mismatch",
        ZerobusError::InvalidArgument(_) => "invalid_argument",
        ZerobusError::CreateStreamError(status) | ZerobusError::StreamClosedError(status) => {
            grpc_error_code(grpc_status_number(status.code()))
        }
        ZerobusError::ChannelCreationError(_) => "channel_creation_failed",
        ZerobusError::FailedToEstablishTlsConnectionError => "tls_failed",
        ZerobusError::ConnectionTimeout(_) => "connection_timeout",
        ZerobusError::TokenFetchError(_) => "token_fetch_failed",
        ZerobusError::UnexpectedStreamResponseError(_) => "unexpected_response",
        ZerobusError::InvalidStateError(_) => "invalid_state",
        ZerobusError::InvalidZerobusEndpointError(_)
        | ZerobusError::InvalidUCEndpointError(_)
        | ZerobusError::InvalidTableName(_)
        | ZerobusError::InvalidUCTokenError(_) => "invalid_config",
        // `ZerobusError` is `#[non_exhaustive]`.
        _ => "other",
    }
}

/// Numeric value of an SDK gRPC code, as defined by the gRPC spec. The SDK's enum has no
/// integer conversion, and its `#[non_exhaustive]` requires the wildcard arm.
const fn grpc_status_number(code: GrpcCode) -> i32 {
    match code {
        GrpcCode::Ok => 0,
        GrpcCode::Cancelled => 1,
        GrpcCode::Unknown => 2,
        GrpcCode::InvalidArgument => 3,
        GrpcCode::DeadlineExceeded => 4,
        GrpcCode::NotFound => 5,
        GrpcCode::AlreadyExists => 6,
        GrpcCode::PermissionDenied => 7,
        GrpcCode::ResourceExhausted => 8,
        GrpcCode::FailedPrecondition => 9,
        GrpcCode::Aborted => 10,
        GrpcCode::OutOfRange => 11,
        GrpcCode::Unimplemented => 12,
        GrpcCode::Internal => 13,
        GrpcCode::Unavailable => 14,
        GrpcCode::DataLoss => 15,
        GrpcCode::Unauthenticated => 16,
        _ => -1,
    }
}

/// Classify a gRPC status by its numeric code, which the gRPC spec fixes. Matched as an
/// integer because the SDK's `tonic` is a different major version from Vector's.
const fn grpc_error_code(code: i32) -> &'static str {
    match code {
        1 => "grpc_cancelled",
        2 => "grpc_unknown",
        3 => "grpc_invalid_argument",
        4 => "grpc_deadline_exceeded",
        5 => "grpc_not_found",
        6 => "grpc_already_exists",
        7 => "grpc_permission_denied",
        8 => "grpc_resource_exhausted",
        9 => "grpc_failed_precondition",
        10 => "grpc_aborted",
        11 => "grpc_out_of_range",
        12 => "grpc_unimplemented",
        13 => "grpc_internal",
        14 => "grpc_unavailable",
        15 => "grpc_data_loss",
        16 => "grpc_unauthenticated",
        _ => "grpc_other",
    }
}

const fn unity_catalog_error_code(status: Option<u16>) -> &'static str {
    match status {
        None => "uc_request_failed",
        Some(401) => "uc_unauthenticated",
        Some(403) => "uc_permission_denied",
        Some(404) => "uc_not_found",
        Some(408) => "uc_timeout",
        Some(429) => "uc_rate_limited",
        Some(500..=599) => "uc_server_error",
        Some(_) => "uc_http_error",
    }
}

impl From<ZerobusError> for ZerobusSinkError {
    fn from(error: ZerobusError) -> Self {
        ZerobusSinkError::ZerobusError { source: error }
    }
}

/// Convert Zerobus errors to Vector event status.
impl From<ZerobusSinkError> for EventStatus {
    fn from(error: ZerobusSinkError) -> Self {
        error.event_status()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinks::databricks_zerobus::service::ZerobusRetryLogic;
    use crate::sinks::util::retries::RetryLogic;

    fn retryable_error() -> ZerobusError {
        // ChannelCreationError is always retryable
        ZerobusError::ChannelCreationError("connection reset".to_string())
    }

    fn non_retryable_error() -> ZerobusError {
        // InvalidArgument is never retryable
        ZerobusError::InvalidArgument("bad field".to_string())
    }

    #[test]
    fn retryable_ingestion_error_maps_to_errored() {
        let error = ZerobusSinkError::IngestionError {
            source: retryable_error(),
        };
        assert_eq!(EventStatus::from(error), EventStatus::Errored);
    }

    #[test]
    fn non_retryable_ingestion_error_maps_to_rejected() {
        let error = ZerobusSinkError::IngestionError {
            source: non_retryable_error(),
        };
        assert_eq!(EventStatus::from(error), EventStatus::Rejected);
    }

    #[test]
    fn retryable_stream_init_error_maps_to_errored() {
        let error = ZerobusSinkError::StreamInitError {
            source: retryable_error(),
        };
        assert_eq!(EventStatus::from(error), EventStatus::Errored);
    }

    /// A stream that cannot be set up fails every batch the same way, so its
    /// `InvalidArgument` (a bad option, table name or header) must not delete them.
    #[test]
    fn non_retryable_stream_init_error_maps_to_errored() {
        let error = ZerobusSinkError::StreamInitError {
            source: non_retryable_error(),
        };
        assert_eq!(EventStatus::from(error), EventStatus::Errored);
    }

    #[test]
    fn config_error_maps_to_errored() {
        let error = ZerobusSinkError::ConfigError {
            message: "bad config".to_string(),
        };
        assert_eq!(EventStatus::from(error), EventStatus::Errored);
    }

    #[test]
    fn encoding_error_maps_to_rejected() {
        let error = ZerobusSinkError::EncodingError {
            message: "encode failed".to_string(),
            schema_constraint: false,
        };
        assert_eq!(EventStatus::from(error), EventStatus::Rejected);
    }

    #[test]
    fn retry_logic_retryable_errors() {
        let logic = ZerobusRetryLogic;

        let error = ZerobusSinkError::IngestionError {
            source: retryable_error(),
        };
        assert!(logic.is_retriable_error(&error));

        let error = ZerobusSinkError::StreamInitError {
            source: retryable_error(),
        };
        assert!(logic.is_retriable_error(&error));

        let error = ZerobusSinkError::ZerobusError {
            source: retryable_error(),
        };
        assert!(logic.is_retriable_error(&error));
    }

    #[test]
    fn retry_logic_non_retryable_errors() {
        let logic = ZerobusRetryLogic;

        let error = ZerobusSinkError::IngestionError {
            source: non_retryable_error(),
        };
        assert!(!logic.is_retriable_error(&error));

        let error = ZerobusSinkError::ConfigError {
            message: "bad".to_string(),
        };
        assert!(!logic.is_retriable_error(&error));

        let error = ZerobusSinkError::EncodingError {
            message: "bad".to_string(),
            schema_constraint: false,
        };
        assert!(!logic.is_retriable_error(&error));
    }

    fn auth_sdk_errors() -> Vec<ZerobusError> {
        use databricks_zerobus_ingest_sdk::{GrpcCode, GrpcStatus};
        vec![
            // The incident shape: an OAuth token-mint 401 surfaces as this variant.
            ZerobusError::InvalidUCTokenError(
                "User is not authorized to the requested authorizations".to_string(),
            ),
            ZerobusError::CreateStreamError(GrpcStatus::new(
                GrpcCode::Unauthenticated,
                "bad credentials",
            )),
            // Grant/RAR propagation is eventual, so a denial can be transient;
            // it is in the auth class deliberately (see `sdk_error_is_auth`).
            ZerobusError::CreateStreamError(GrpcStatus::new(
                GrpcCode::PermissionDenied,
                "no grant",
            )),
            ZerobusError::StreamClosedError(GrpcStatus::new(
                GrpcCode::Unauthenticated,
                "expired token",
            )),
        ]
    }

    #[test]
    fn auth_errors_are_retryable_and_errored() {
        for source in auth_sdk_errors() {
            let error = ZerobusSinkError::IngestionError { source };
            assert!(error.is_auth_error(), "{error}");
            // The whole point: the batch must be kept (Errored), not rejected.
            assert!(
                error.is_retryable() && ZerobusRetryLogic.is_retriable_error(&error),
                "{error}"
            );
            assert_eq!(EventStatus::from(error), EventStatus::Errored);
        }
    }

    #[test]
    fn auth_errors_are_retryable_at_stream_init_too() {
        // A token-mint 401 during stream creation must not reject the batch
        // either: the retry rebuilds the stream once the mint recovers.
        for source in auth_sdk_errors() {
            let error = ZerobusSinkError::StreamInitError { source };
            assert!(error.is_auth_error(), "{error}");
            assert!(error.is_retryable(), "{error}");
        }
    }

    #[test]
    fn non_auth_errors_are_not_auth_errors() {
        // TokenFetchError (mint 5xx/network) is already SDK-retryable; it is not
        // in the auth class, so it keeps its existing recovery unchanged.
        let fetch = ZerobusSinkError::IngestionError {
            source: ZerobusError::TokenFetchError("mint 503".to_string()),
        };
        assert!(!fetch.is_auth_error());
        assert!(fetch.is_retryable());

        // Genuinely permanent config errors keep their rejection semantics.
        let argument = ZerobusSinkError::IngestionError {
            source: non_retryable_error(),
        };
        assert!(!argument.is_auth_error());
        assert!(!argument.is_retryable());

        let schema = ZerobusSinkError::SchemaError {
            message: "UC 404".to_string(),
            retryable: false,
            status: Some(404),
        };
        assert!(!schema.is_auth_error());
        assert!(!schema.is_retryable());
    }

    #[test]
    fn stream_rebuild_backoff_is_retryable() {
        let error = ZerobusSinkError::StreamRebuildBackoff { retry_after_ms: 1 };
        assert!(error.is_retryable());
        assert!(ZerobusRetryLogic.is_retriable_error(&error));
        assert!(!error.is_auth_error());
        assert_eq!(error.error_code(), "stream_rebuild_backoff");
        assert_eq!(EventStatus::from(error), EventStatus::Errored);
    }

    #[test]
    fn auth_error_codes_are_fixed() {
        use databricks_zerobus_ingest_sdk::{GrpcCode, GrpcStatus};
        assert_eq!(
            sdk_error_code(&ZerobusError::InvalidUCTokenError("no".to_string())),
            "invalid_config"
        );
        assert_eq!(
            sdk_error_code(&ZerobusError::CreateStreamError(GrpcStatus::new(
                GrpcCode::Unauthenticated,
                "no"
            ))),
            "grpc_unauthenticated"
        );
        assert_eq!(
            sdk_error_code(&ZerobusError::CreateStreamError(GrpcStatus::new(
                GrpcCode::PermissionDenied,
                "no"
            ))),
            "grpc_permission_denied"
        );
    }

    // NOTE: no positive test for `is_schema_rejection` — `InvalidSchema` is
    // `#[non_exhaustive]` with a crate-private constructor, so only a live server
    // rejection reaches the true branch. The testable half is the one that
    // matters for safety: nothing else may enter the refetch path.

    #[test]
    fn non_schema_errors_are_not_schema_rejections() {
        // A retryable SDK error keeps its own recovery (drop the stream, retry).
        assert!(
            !ZerobusSinkError::IngestionError {
                source: retryable_error()
            }
            .is_schema_rejection()
        );
        // Notably the client-side `InvalidArgument` from a generation-skewed
        // batch.
        assert!(
            !ZerobusSinkError::IngestionError {
                source: non_retryable_error()
            }
            .is_schema_rejection()
        );
        assert!(
            !ZerobusSinkError::StreamInitError {
                source: retryable_error()
            }
            .is_schema_rejection()
        );
        assert!(
            !ZerobusSinkError::EncodingError {
                message: "bad".to_string(),
                schema_constraint: false,
            }
            .is_schema_rejection()
        );
        // Variants that carry no `ZerobusError` at all take the early return.
        assert!(!ZerobusSinkError::StreamClosed.is_schema_rejection());
        assert!(
            !ZerobusSinkError::SchemaReloaded {
                message: "stale".to_string()
            }
            .is_schema_rejection()
        );
    }

    #[test]
    fn retryable_schema_error_maps_to_errored() {
        let error = ZerobusSinkError::SchemaError {
            message: "UC 503".to_string(),
            retryable: true,
            status: Some(503),
        };
        assert!(ZerobusRetryLogic.is_retriable_error(&error));
        assert_eq!(EventStatus::from(error), EventStatus::Errored);
    }

    #[test]
    fn non_retryable_schema_error_maps_to_errored() {
        // A Unity Catalog failure says nothing about the data: requeue, do not delete.
        let error = ZerobusSinkError::SchemaError {
            message: "UC 404".to_string(),
            retryable: false,
            status: Some(404),
        };
        assert!(!ZerobusRetryLogic.is_retriable_error(&error));
        assert_eq!(EventStatus::from(error), EventStatus::Errored);
    }

    #[test]
    fn error_codes_are_fixed_per_class() {
        let cases = [
            (
                ZerobusSinkError::EncodingError {
                    message: "null in required column".to_string(),
                    schema_constraint: true,
                },
                "encoding_schema_constraint",
            ),
            (
                ZerobusSinkError::EncodingError {
                    message: "bad".to_string(),
                    schema_constraint: false,
                },
                "encoding_failed",
            ),
            (
                ZerobusSinkError::ConfigError {
                    message: "bad".to_string(),
                },
                "config",
            ),
            (
                ZerobusSinkError::IngestionError {
                    source: non_retryable_error(),
                },
                "invalid_argument",
            ),
            (
                ZerobusSinkError::ZerobusError {
                    source: retryable_error(),
                },
                "channel_creation_failed",
            ),
            (ZerobusSinkError::StreamClosed, "stream_closed_concurrently"),
            (
                ZerobusSinkError::SchemaReloaded {
                    message: "stale".to_string(),
                },
                "schema_reloaded",
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(error.error_code(), expected, "{error}");
        }
    }

    // The SDK's `tonic::Status` can't be built here (a different `tonic` major version), so the
    // gRPC mapping is checked on the spec's numeric codes it is keyed by.
    #[test]
    fn grpc_error_codes_follow_status_code() {
        assert_eq!(grpc_error_code(3), "grpc_invalid_argument");
        // The SDK's ack-wait timeout ("Waiting for acknowledgement timed out") is DeadlineExceeded.
        assert_eq!(grpc_error_code(4), "grpc_deadline_exceeded");
        assert_eq!(grpc_error_code(5), "grpc_not_found");
        assert_eq!(grpc_error_code(7), "grpc_permission_denied");
        assert_eq!(grpc_error_code(14), "grpc_unavailable");
        assert_eq!(grpc_error_code(16), "grpc_unauthenticated");
        assert_eq!(grpc_error_code(0), "grpc_other");
        assert_eq!(grpc_error_code(99), "grpc_other");
    }

    #[test]
    fn unity_catalog_error_codes_follow_http_status() {
        let code = |status| {
            ZerobusSinkError::SchemaError {
                message: "UC failed".to_string(),
                retryable: false,
                status,
            }
            .error_code()
        };
        assert_eq!(code(None), "uc_request_failed");
        assert_eq!(code(Some(401)), "uc_unauthenticated");
        assert_eq!(code(Some(403)), "uc_permission_denied");
        assert_eq!(code(Some(404)), "uc_not_found");
        assert_eq!(code(Some(408)), "uc_timeout");
        assert_eq!(code(Some(429)), "uc_rate_limited");
        assert_eq!(code(Some(503)), "uc_server_error");
        assert_eq!(code(Some(400)), "uc_http_error");
    }

    use ErrorCategory::{BadData, Dependency, Retryable};

    fn grpc(code: GrpcCode) -> ZerobusError {
        ZerobusError::CreateStreamError(databricks_zerobus_ingest_sdk::GrpcStatus::new(code, "x"))
    }

    fn ingestion(source: ZerobusError) -> ZerobusSinkError {
        ZerobusSinkError::IngestionError { source }
    }

    fn init(source: ZerobusError) -> ZerobusSinkError {
        ZerobusSinkError::StreamInitError { source }
    }

    fn terminal(source: ZerobusError) -> ZerobusSinkError {
        ZerobusSinkError::TerminalIngestionError { source }
    }

    fn uc(retryable: bool, status: Option<u16>) -> ZerobusSinkError {
        ZerobusSinkError::SchemaError {
            message: "uc".to_string(),
            retryable,
            status,
        }
    }

    /// One representative of every way the sink can fail, with its expected category.
    /// New `ZerobusSinkError` variants must be added here (the exhaustive `match` in
    /// `error_code` forces them to have a code; this list forces a decision).
    fn all_errors() -> Vec<(ZerobusSinkError, ErrorCategory)> {
        let mut cases = vec![
            // Category 1: known bad data.
            (
                ZerobusSinkError::EncodingError {
                    message: "x".into(),
                    schema_constraint: false,
                },
                BadData,
            ),
            (
                ingestion(ZerobusError::InvalidArgument("x".into())),
                BadData,
            ),
            (ingestion(grpc(GrpcCode::InvalidArgument)), BadData),
            // Category 2: retryable.
            (
                ZerobusSinkError::EncodingTaskFailed {
                    message: "x".into(),
                },
                Retryable,
            ),
            (ZerobusSinkError::StreamClosed, Retryable),
            (
                ZerobusSinkError::SchemaReloaded {
                    message: "x".into(),
                },
                Retryable,
            ),
            (
                ZerobusSinkError::StreamRebuildBackoff { retry_after_ms: 1 },
                Retryable,
            ),
            (uc(true, Some(503)), Retryable),
            (uc(true, Some(408)), Retryable),
            (uc(true, Some(429)), Retryable),
            (uc(true, None), Retryable),
            (
                ingestion(ZerobusError::ChannelCreationError("x".into())),
                Retryable,
            ),
            (
                ingestion(ZerobusError::FailedToEstablishTlsConnectionError),
                Retryable,
            ),
            (
                ingestion(ZerobusError::TokenFetchError("x".into())),
                Retryable,
            ),
            (
                ingestion(ZerobusError::UnexpectedStreamResponseError("x".into())),
                Retryable,
            ),
            // The auth class is retryable here even though the SDK says otherwise.
            (
                ingestion(ZerobusError::InvalidUCTokenError("x".into())),
                Retryable,
            ),
            (ingestion(grpc(GrpcCode::Unauthenticated)), Retryable),
            (ingestion(grpc(GrpcCode::PermissionDenied)), Retryable),
            // Category 3: everything else.
            (
                ZerobusSinkError::ConfigError {
                    message: "x".into(),
                },
                Dependency,
            ),
            (
                ZerobusSinkError::EncodingError {
                    message: "x".into(),
                    schema_constraint: true,
                },
                Dependency,
            ),
            (uc(false, Some(404)), Dependency),
            (uc(false, Some(401)), Dependency),
            (uc(false, Some(403)), Dependency),
            (uc(false, Some(400)), Dependency),
            (uc(false, None), Dependency),
            (
                ingestion(ZerobusError::InvalidTableName("x".into())),
                Dependency,
            ),
            (
                ingestion(ZerobusError::InvalidStateError("x".into())),
                Dependency,
            ),
            (
                ingestion(ZerobusError::InvalidZerobusEndpointError("x".into())),
                Dependency,
            ),
            (
                ingestion(ZerobusError::InvalidUCEndpointError("x".into())),
                Dependency,
            ),
            (ingestion(grpc(GrpcCode::NotFound)), Dependency),
            (ingestion(grpc(GrpcCode::OutOfRange)), Dependency),
            (ingestion(grpc(GrpcCode::Unimplemented)), Dependency),
            // A bad-data code outside a single-batch error: the same failure recurs
            // for every batch, so it must not delete them.
            (init(ZerobusError::InvalidArgument("x".into())), Dependency),
            (init(grpc(GrpcCode::InvalidArgument)), Dependency),
            (
                ZerobusSinkError::ZerobusError {
                    source: ZerobusError::InvalidArgument("x".into()),
                },
                Dependency,
            ),
            (
                terminal(ZerobusError::InvalidArgument("x".into())),
                Dependency,
            ),
            (terminal(grpc(GrpcCode::InvalidArgument)), Dependency),
            (terminal(grpc(GrpcCode::NotFound)), Dependency),
            // A closed stream keeps the retryable and auth classes of its error.
            (terminal(grpc(GrpcCode::Unavailable)), Retryable),
            (terminal(grpc(GrpcCode::Unauthenticated)), Retryable),
        ];
        // Every gRPC code lands in exactly one category, and only InvalidArgument is bad data.
        for code in [
            GrpcCode::Cancelled,
            GrpcCode::Unknown,
            GrpcCode::DeadlineExceeded,
            GrpcCode::AlreadyExists,
            GrpcCode::ResourceExhausted,
            GrpcCode::FailedPrecondition,
            GrpcCode::Aborted,
            GrpcCode::Internal,
            GrpcCode::Unavailable,
            GrpcCode::DataLoss,
        ] {
            cases.push((ingestion(grpc(code)), Retryable));
        }
        cases
    }

    #[test]
    fn every_error_is_in_exactly_one_category() {
        for (error, expected) in all_errors() {
            assert_eq!(
                error.category(),
                expected,
                "{error} ({})",
                error.error_code()
            );
            let status = match expected {
                BadData => EventStatus::Rejected,
                Retryable | Dependency => EventStatus::Errored,
            };
            assert_eq!(error.event_status(), status, "{error}");
            // In-sink retry happens for the retryable category and nothing else.
            assert_eq!(
                ZerobusRetryLogic.is_retriable_error(&error),
                expected == Retryable,
                "{error}"
            );
        }
    }

    #[test]
    fn bad_data_codes_are_only_produced_by_non_retryable_errors() {
        let cases = all_errors();
        for code in BAD_DATA_ERROR_CODES {
            let producers: Vec<_> = cases
                .iter()
                .filter(|(error, _)| error.error_code() == *code)
                .collect();
            assert!(
                !producers.is_empty(),
                "no error produces bad-data code {code}"
            );
            assert!(
                producers.iter().all(|(error, _)| !error.is_retryable()),
                "bad-data code {code} is produced by a retryable error"
            );
        }
        // And nothing outside the list, or outside a single-batch error, is ever
        // classified as bad data.
        for (error, category) in &cases {
            assert_eq!(
                *category == BadData,
                BAD_DATA_ERROR_CODES.contains(&error.error_code())
                    && !error.is_retryable()
                    && error.can_be_bad_data(),
                "{error}"
            );
        }
    }

    #[test]
    fn error_codes_outside_the_known_list_are_dependency_errors() {
        // `other` is what an SDK error this build does not know about reports.
        assert_eq!(
            sdk_error_code(&ZerobusError::TokenFetchError("x".into())),
            "token_fetch_failed"
        );
        for error in [
            ZerobusSinkError::ConfigError {
                message: "x".into(),
            },
            uc(false, Some(404)),
            ingestion(ZerobusError::InvalidStateError("x".into())),
        ] {
            assert!(!BAD_DATA_ERROR_CODES.contains(&error.error_code()));
            assert_eq!(error.category(), Dependency, "{error}");
        }
    }

    /// Moving a failure out of the bad-data category keeps its `error_code`, so the
    /// metric series stays the same and only the verdict changes.
    #[test]
    fn stream_wide_bad_data_codes_keep_their_code_but_are_errored() {
        for (error, code) in [
            (
                init(ZerobusError::InvalidArgument("x".into())),
                "invalid_argument",
            ),
            (
                init(grpc(GrpcCode::InvalidArgument)),
                "grpc_invalid_argument",
            ),
            (
                terminal(ZerobusError::InvalidArgument("x".into())),
                "invalid_argument",
            ),
            (
                terminal(grpc(GrpcCode::InvalidArgument)),
                "grpc_invalid_argument",
            ),
        ] {
            assert_eq!(error.error_code(), code, "{error}");
            assert!(!error.is_retryable(), "{error}");
            assert_eq!(error.category(), Dependency, "{error}");
            assert_eq!(error.event_status(), EventStatus::Errored, "{error}");
        }
    }

    /// Only a non-retryable failure that closed the stream, and is not a schema
    /// rejection, is sent again on a fresh stream.
    #[test]
    fn only_non_retryable_terminal_failures_retry_on_a_fresh_stream() {
        for (error, expected) in [
            (terminal(grpc(GrpcCode::InvalidArgument)), true),
            (terminal(grpc(GrpcCode::NotFound)), true),
            (terminal(ZerobusError::InvalidStateError("x".into())), true),
            // Retried by the request retry layer already.
            (terminal(grpc(GrpcCode::Unavailable)), false),
            (terminal(grpc(GrpcCode::Unauthenticated)), false),
            // The stream stayed open, or never existed.
            (ingestion(ZerobusError::InvalidArgument("x".into())), false),
            (ingestion(grpc(GrpcCode::NotFound)), false),
            (init(grpc(GrpcCode::InvalidArgument)), false),
        ] {
            assert_eq!(error.retries_on_fresh_stream(), expected, "{error}");
        }
    }

    /// The SDK rejects an empty batch with `InvalidArgument` (code `invalid_argument`, bad
    /// data). The service never reaches it: an all-rows-dropped batch is acknowledged as
    /// delivered-nothing first (see `all_rows_dropped_by_codec_is_delivered_without_ingest`).
    #[test]
    fn empty_batch_error_code_is_known_bad_data() {
        let error = ingestion(ZerobusError::InvalidArgument("empty batch".into()));
        assert_eq!(error.error_code(), "invalid_argument");
        assert_eq!(error.category(), BadData);
    }
}
