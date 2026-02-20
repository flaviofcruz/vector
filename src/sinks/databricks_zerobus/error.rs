//! Error types for the Zerobus sink.

use databricks_zerobus_ingest_sdk::ZerobusError;
use snafu::Snafu;
use vector_lib::event::EventStatus;

/// Errors that can occur when using the Zerobus sink.
#[derive(Debug, Snafu)]
pub enum ZerobusSinkError {
    /// Configuration validation failed.
    #[snafu(display("Configuration error: {}", message))]
    ConfigError { message: String },

    /// Event encoding failed.
    #[snafu(display("Encoding error: {}", message))]
    EncodingError { message: String },

    /// Zerobus SDK error.
    #[snafu(display("Zerobus error: {}", source))]
    ZerobusError { source: ZerobusError },

    /// Stream initialization failed.
    #[snafu(display("Stream initialization failed: {}", message))]
    StreamInitError { message: String },

    /// Record ingestion failed.
    #[snafu(display("Record ingestion failed: {}", message))]
    IngestionError { message: String },
}

impl From<ZerobusError> for ZerobusSinkError {
    fn from(error: ZerobusError) -> Self {
        ZerobusSinkError::ZerobusError { source: error }
    }
}

/// Convert Zerobus errors to Vector event status.
impl From<ZerobusSinkError> for EventStatus {
    fn from(error: ZerobusSinkError) -> Self {
        match error {
            ZerobusSinkError::ConfigError { .. } => EventStatus::Rejected,
            ZerobusSinkError::EncodingError { .. } => EventStatus::Rejected,
            ZerobusSinkError::StreamInitError { .. } => EventStatus::Errored,
            ZerobusSinkError::IngestionError { .. } => EventStatus::Errored,
            ZerobusSinkError::ZerobusError { source } => {
                // Map retryable errors to Failed, non-retryable to Rejected
                if source.is_retryable() {
                    EventStatus::Errored
                } else {
                    EventStatus::Rejected
                }
            }
        }
    }
}
