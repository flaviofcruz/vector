use metrics::counter;
use vector_lib::NamedInternalEvent;
use vector_lib::internal_event::{InternalEvent, error_stage, error_type};

use crate::sources::azure_blob::queue::ProcessingError;

/// Increments `component_errors_total` for an Azure blob source failure.
///
/// Labels are bounded and static. Container and blob names are deliberately never used as labels
/// (high cardinality); they are only logged.
fn count_component_error(error_type: &'static str, error_code: &'static str, stage: &'static str) {
    counter!(
        "component_errors_total",
        "error_code" => error_code,
        "error_type" => error_type,
        "stage" => stage,
    )
    .increment(1);
}

/// Failed to receive messages from the Azure storage queue.
#[derive(Debug, NamedInternalEvent)]
pub struct AzureBlobQueueReceiveFailed<'a> {
    pub error: &'a azure_core_for_storage::Error,
}

impl InternalEvent for AzureBlobQueueReceiveFailed<'_> {
    fn emit(self) {
        error!(
            message = "Failed to receive messages from queue.",
            error = %self.error,
            error_code = "queue_receive_failed",
            error_type = error_type::REQUEST_FAILED,
            stage = error_stage::RECEIVING,
            internal_log_rate_limit = true,
        );
        count_component_error(
            error_type::REQUEST_FAILED,
            "queue_receive_failed",
            error_stage::RECEIVING,
        );
    }
}

/// Failed to fetch the queue's metadata (approximate depth) for the `queue_message_count` gauge.
#[derive(Debug, NamedInternalEvent)]
pub struct AzureBlobQueueMetadataFailed<'a> {
    pub error: &'a azure_core_for_storage::Error,
}

impl InternalEvent for AzureBlobQueueMetadataFailed<'_> {
    fn emit(self) {
        error!(
            message = "Failed to fetch queue metadata for depth metric.",
            error = %self.error,
            error_code = "queue_metadata_failed",
            error_type = error_type::REQUEST_FAILED,
            stage = error_stage::RECEIVING,
            internal_log_rate_limit = true,
        );
        count_component_error(
            error_type::REQUEST_FAILED,
            "queue_metadata_failed",
            error_stage::RECEIVING,
        );
    }
}

/// Timed out fetching the queue's metadata (approximate depth) for the `queue_message_count` gauge.
#[derive(Debug, NamedInternalEvent)]
pub struct AzureBlobQueueMetadataTimedOut;

impl InternalEvent for AzureBlobQueueMetadataTimedOut {
    fn emit(self) {
        error!(
            message = "Timed out fetching queue metadata for depth metric.",
            error_code = "queue_metadata_timed_out",
            error_type = error_type::TIMED_OUT,
            stage = error_stage::RECEIVING,
            internal_log_rate_limit = true,
        );
        count_component_error(
            error_type::TIMED_OUT,
            "queue_metadata_timed_out",
            error_stage::RECEIVING,
        );
    }
}

/// Failed to delete a processed (or drained) message from the Azure storage queue.
#[derive(Debug, NamedInternalEvent)]
pub struct AzureBlobQueueDeleteFailed<'a> {
    pub message_id: &'a str,
    pub error: &'a azure_core_for_storage::Error,
}

impl InternalEvent for AzureBlobQueueDeleteFailed<'_> {
    fn emit(self) {
        error!(
            message = "Failed to delete message from queue.",
            message_id = %self.message_id,
            error = %self.error,
            error_code = "queue_delete_failed",
            error_type = error_type::REQUEST_FAILED,
            stage = error_stage::PROCESSING,
            internal_log_rate_limit = true,
        );
        count_component_error(
            error_type::REQUEST_FAILED,
            "queue_delete_failed",
            error_stage::PROCESSING,
        );
    }
}

/// A queue message failed to be processed.
///
/// Emitted once per failure, from the per-message error path, or from the batch acknowledgement
/// site for delivery failures. The `error` carries the `error_type`, `error_code` and `stage`
/// labels. Permanent failures (retrying can never succeed) log at `warn`; all others at `error`.
#[derive(Debug, NamedInternalEvent)]
pub struct AzureBlobMessageProcessingError<'a> {
    pub message_id: &'a str,
    pub error: &'a ProcessingError,
}

impl InternalEvent for AzureBlobMessageProcessingError<'_> {
    fn emit(self) {
        let error_code = self.error.error_code();
        let error_type = self.error.error_type();
        let stage = self.error.stage();
        if self.error.is_permanent() {
            warn!(
                message = "Failed to process Azure blob queue message; retrying will not succeed.",
                message_id = %self.message_id,
                error = %self.error,
                error_code = error_code,
                error_type = error_type,
                stage = stage,
                internal_log_rate_limit = true,
            );
        } else {
            error!(
                message = "Failed to process Azure blob queue message.",
                message_id = %self.message_id,
                error = %self.error,
                error_code = error_code,
                error_type = error_type,
                stage = stage,
                internal_log_rate_limit = true,
            );
        }
        count_component_error(error_type, error_code, stage);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::trace_init;
    use vector_lib::event::MetricValue;
    use vector_lib::metrics::Controller;

    /// Current value of `component_errors_total` for the exact label set, or 0 if unseen.
    fn component_errors(error_type: &str, error_code: &str, stage: &str) -> f64 {
        Controller::get()
            .unwrap()
            .capture_metrics()
            .iter()
            .filter(|m| m.name() == "component_errors_total")
            .find(|m| {
                let tags = m.tags().expect("component_errors_total has tags");
                tags.get("error_code") == Some(error_code)
                    && tags.get("error_type") == Some(error_type)
                    && tags.get("stage") == Some(stage)
            })
            .map(|m| match m.value() {
                MetricValue::Counter { value } => *value,
                other => panic!("expected a counter, got {other:?}"),
            })
            .unwrap_or(0.0)
    }

    #[test]
    fn queue_receive_failure_is_counted() {
        trace_init();
        let err = azure_core_for_storage::error::ErrorKind::Io.into_error();
        let before = component_errors("request_failed", "queue_receive_failed", "receiving");
        emit!(AzureBlobQueueReceiveFailed { error: &err });
        let after = component_errors("request_failed", "queue_receive_failed", "receiving");
        assert!(after >= before + 1.0);
    }

    #[test]
    fn queue_metadata_timeout_is_counted() {
        trace_init();
        let before = component_errors("timed_out", "queue_metadata_timed_out", "receiving");
        emit!(AzureBlobQueueMetadataTimedOut);
        let after = component_errors("timed_out", "queue_metadata_timed_out", "receiving");
        assert!(after >= before + 1.0);
    }
}
