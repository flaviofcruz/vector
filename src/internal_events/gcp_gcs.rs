use metrics::counter;
use vector_lib::NamedInternalEvent;
use vector_lib::internal_event::{InternalEvent, error_stage, error_type};

use crate::sources::gcp_gcs::pubsub::ProcessingError;

#[derive(Debug, NamedInternalEvent)]
pub struct GcsObjectProcessingSucceeded<'a> {
    pub bucket: &'a str,
}

#[derive(Debug, NamedInternalEvent)]
pub struct GcsObjectProcessingFailed<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub error: &'a ProcessingError,
}

impl InternalEvent for GcsObjectProcessingSucceeded<'_> {
    fn emit(self) {
        debug!(
            message = "GCS object processing succeeded.",
            bucket = %self.bucket,
        );
        counter!("gcs_object_processing_succeeded_total").increment(1);
    }
}

impl InternalEvent for GcsObjectProcessingFailed<'_> {
    fn emit(self) {
        error!(
            message = "GCS object processing failed.",
            bucket = %self.bucket,
            key = %self.key,
            error = %self.error,
            error_code = "gcs_object_processing_failed",
            error_type = error_type::WRITER_FAILED,
            stage = error_stage::SENDING,
        );
        counter!(
            "component_errors_total",
            "error_code" => "gcs_object_processing_failed",
            "error_type" => error_type::WRITER_FAILED,
            "stage" => error_stage::SENDING,
        )
        .increment(1);
        counter!("gcs_object_processing_failed_total").increment(1);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct PubSubMessageReceiveError<'a, E> {
    pub error: &'a E,
}

impl<E: std::fmt::Display> InternalEvent for PubSubMessageReceiveError<'_, E> {
    fn emit(self) {
        error!(
            message = "Failed to fetch Pub/Sub messages.",
            error = %self.error,
            error_code = "failed_fetching_pubsub_messages",
            error_type = error_type::REQUEST_FAILED,
            stage = error_stage::RECEIVING,
        );
        counter!(
            "component_errors_total",
            "error_code" => "failed_fetching_pubsub_messages",
            "error_type" => error_type::REQUEST_FAILED,
            "stage" => error_stage::RECEIVING,
        )
        .increment(1);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct PubSubMessageReceiveSucceeded {
    pub count: usize,
}

impl InternalEvent for PubSubMessageReceiveSucceeded {
    fn emit(self) {
        trace!(message = "Received Pub/Sub messages.", count = %self.count);
        counter!("pubsub_message_receive_succeeded_total").increment(1);
        counter!("pubsub_message_received_messages_total").increment(self.count as u64);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct PubSubMessageProcessingSucceeded<'a> {
    pub message_id: &'a str,
}

impl InternalEvent for PubSubMessageProcessingSucceeded<'_> {
    fn emit(self) {
        trace!(
            message = "Processed Pub/Sub message successfully.",
            message_id = %self.message_id
        );
        counter!("pubsub_message_processing_succeeded_total").increment(1);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct PubSubMessageProcessingError<'a> {
    pub message_id: &'a str,
    pub error: &'a ProcessingError,
}

impl InternalEvent for PubSubMessageProcessingError<'_> {
    fn emit(self) {
        error!(
            message = "Failed to process Pub/Sub message.",
            message_id = %self.message_id,
            error = %self.error,
            error_code = "failed_processing_pubsub_message",
            error_type = error_type::PARSER_FAILED,
            stage = error_stage::PROCESSING,
        );
        counter!(
            "component_errors_total",
            "error_code" => "failed_processing_pubsub_message",
            "error_type" => error_type::PARSER_FAILED,
            "stage" => error_stage::PROCESSING,
        )
        .increment(1);
    }
}
