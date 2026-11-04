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

    /// Zerobus SDK error.
    #[snafu(display("Zerobus error: {}", source))]
    ZerobusError { source: ZerobusError },

    /// Stream initialization failed.
    #[snafu(display("Stream initialization failed: {}", source))]
    StreamInitError { source: ZerobusError },

    /// Record ingestion failed.
    #[snafu(display("Record ingestion failed: {}", source))]
    IngestionError { source: ZerobusError },

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
        | Self::IngestionError { source }) = self
        else {
            return false;
        };
        matches!(source, ZerobusError::InvalidSchema { .. })
    }

    /// Whether this error should be retried.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::ZerobusError { source }
            | Self::StreamInitError { source }
            | Self::IngestionError { source } => source.is_retryable(),
            Self::StreamClosed => true,
            Self::SchemaError { retryable, .. } => *retryable,
            Self::SchemaReloaded { .. } => true,
            Self::ConfigError { .. } | Self::EncodingError { .. } => false,
        }
    }

    /// Fixed classification of this error for the `error_code` metric label.
    ///
    /// Every value is a literal here, never derived from error text, so the label stays
    /// bounded; new variants extend this `match`.
    pub fn error_code(&self) -> &'static str {
        match self {
            Self::ConfigError { .. } => "config",
            Self::EncodingError {
                schema_constraint: true,
                ..
            } => "encoding_schema_constraint",
            Self::EncodingError { .. } => "encoding_failed",
            Self::ZerobusError { source }
            | Self::StreamInitError { source }
            | Self::IngestionError { source } => sdk_error_code(source),
            Self::StreamClosed => "stream_closed_concurrently",
            Self::SchemaError { status, .. } => unity_catalog_error_code(*status),
            Self::SchemaReloaded { .. } => "schema_reloaded",
        }
    }

    /// Event status to apply to a batch's finalizers when this error occurs.
    pub fn event_status(&self) -> EventStatus {
        if self.is_retryable() {
            EventStatus::Errored
        } else {
            EventStatus::Rejected
        }
    }
}

fn sdk_error_code(error: &ZerobusError) -> &'static str {
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

    #[test]
    fn non_retryable_stream_init_error_maps_to_rejected() {
        let error = ZerobusSinkError::StreamInitError {
            source: non_retryable_error(),
        };
        assert_eq!(EventStatus::from(error), EventStatus::Rejected);
    }

    #[test]
    fn config_error_maps_to_rejected() {
        let error = ZerobusSinkError::ConfigError {
            message: "bad config".to_string(),
        };
        assert_eq!(EventStatus::from(error), EventStatus::Rejected);
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
    fn non_retryable_schema_error_maps_to_rejected() {
        let error = ZerobusSinkError::SchemaError {
            message: "UC 404".to_string(),
            retryable: false,
            status: Some(404),
        };
        assert!(!ZerobusRetryLogic.is_retriable_error(&error));
        assert_eq!(EventStatus::from(error), EventStatus::Rejected);
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
}
