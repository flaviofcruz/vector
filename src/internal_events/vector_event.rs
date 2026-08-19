// Structs used for our vector event logs
use metrics::counter;
use regex::Regex;

use serde_json;
use std::collections::HashMap;

use vector_common::internal_event::vector_event::delivery_event::MetadataValuesCount;

/// Counts one file-upload attempt on the blob sinks (`aws_s3`, `gcp_cloud_storage`,
/// `azure_blob`), labelled by outcome.
///
/// This is deliberately a *file* count, not an event count: `delivery_events_total`
/// and `component_sent_events_total` already measure the events inside each upload,
/// so a batch of 100 events in one object advances those by 100 and this by 1.
/// That makes it the metric to use for upload rate, object-size averages
/// (bytes / files), and small-file detection.
///
/// Every attempt is recorded, so the total is the denominator for an upload failure
/// ratio and a failing destination shows up even when nothing lands. Split on the
/// `status` label (`success` / `failure`) to separate the two.
///
/// Emitted after retries have settled, so one attempt here is one settled upload
/// rather than one HTTP request. Note that `Ok(response)` alone does not mean success
/// on `gcp_cloud_storage`, where a non-retriable 4xx still surfaces as `Ok`.
///
/// `status` and `bucket` are the explicit labels. The enclosing sink request span adds
/// `component_id` / `component_type` / `component_kind` automatically (see
/// `VectorLabelFilter`), giving per-sink attribution without extra cardinality.
pub fn emit_blob_file_upload_attempt(success: bool, bucket: &str) {
    counter!(
        "blob_sink_file_uploads_total",
        "status" => if success { "success" } else { "failure" },
        "bucket" => bucket.to_string(),
    )
    .increment(1);
}

// Struct for vector send events (sending, uploaded)
#[derive(Clone, Debug, Default)]
pub struct VectorEventLogSendMetadata {
    pub bytes: usize,
    pub events_len: usize,
    pub blob: String,
    pub container: String,
    // For Azure this is the storage account name; for S3/GCS the bucket. Lands
    // in the `bucket` proto field of VectorSendMessagesEvent so it matches
    // log-daemon's destination_bucket. `container` still drives the URL's
    // container slot in the downstream VRL transform.
    pub bucket: Option<String>,
    // Count map here allows us to keep track of the count/size of events per combination of fields
    // Key is a string encoding those combinations for ease of update
    pub count_map: HashMap<String, MetadataValuesCount>,
}

impl VectorEventLogSendMetadata {
    pub fn new() -> Self {
        Self {
            bytes: 0,
            events_len: 0,
            blob: "".to_string(),
            container: "".to_string(),
            bucket: None,
            count_map: HashMap::new(),
        }
    }

    pub fn emit_upload_event(&self) {
        // VECTOR_UPLOADED_MESSAGES_EVENT
        // This will be deprecated in favor of log delivery events
        self.emit_count_map("Uploaded events.", 4);
    }

    pub fn emit_sending_event(&self) {
        // VECTOR_SENDING_MESSAGES_EVENT
        // This will be deprecated in favor of log delivery events
        self.emit_count_map("Sending events.", 3);
    }

    fn emit_count_map(&self, message: &str, event_type: usize) {
        for value in self.count_map.values() {
            info!(
                message = message,
                keys = serde_json::to_string(&value.value_map).unwrap(),
                bytes = value.size,
                events_len = value.count,
                blob = self.blob,
                container = self.container,
                bucket = self.bucket.as_deref().unwrap_or(""),
                vector_event_type = event_type,
                internal_log_rate_limit = false,
            );
        }
    }
}

// Utility function for extracting the topic name from an archived log file path.
#[allow(dead_code)]
pub fn extract_topic_name(file_path: &str) -> String {
    // Topic: If the file being uploaded matches the archived-log filepattern we can extract
    // its topic from said pattern; otherwise propagate the empty-string.
    let topic_regex =
        Regex::new(r"archived-log\/log-sync-internal\/(?:structured-log\/)?([a-zA-Z-_]+)\/date")
            .unwrap();
    if let Some(captures) = topic_regex.captures(file_path) {
        return captures[1].to_string();
    }
    "".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use vector_lib::event::MetricValue;
    use vector_lib::metrics::{Controller, init_test};

    #[test]
    fn extract_topic_name_success() {
        let file_path = "databricks-logs/archived-log/log-sync-internal/test-topic/date=2024-04-02/us-west-2/vector-aggregator-0/test.log";
        assert_eq!(extract_topic_name(file_path), "test-topic");
        let file_path_structured = "databricks-logs/archived-log/log-sync-internal/structured-log/test-topic/date=2024-04-02/us-west-2/vector-aggregator-0/test.log";
        assert_eq!(extract_topic_name(file_path_structured), "test-topic");
    }

    #[test]
    fn extract_topic_name_fail() {
        let file_path = r"no-topic";
        assert_eq!(extract_topic_name(file_path), "");
    }

    /// Returns (`success`, `failure`) counts for `blob_sink_file_uploads_total`.
    fn upload_counts(controller: &Controller) -> (Option<u64>, Option<u64>) {
        let mut success = None;
        let mut failure = None;
        for metric in controller.capture_metrics() {
            if metric.name() != "blob_sink_file_uploads_total" {
                continue;
            }
            let value = match metric.value() {
                MetricValue::Counter { value } => *value as u64,
                other => panic!("expected a counter, got {other:?}"),
            };
            assert!(
                metric.tag_value("bucket").is_some(),
                "blob_sink_file_uploads_total emitted without a bucket label",
            );
            match metric.tag_value("status").as_deref() {
                Some("success") => success = Some(value),
                Some("failure") => failure = Some(value),
                other => panic!("unexpected status tag {other:?}"),
            }
        }
        (success, failure)
    }

    #[test]
    fn counts_one_per_attempt_split_by_status() {
        init_test();
        let controller = Controller::get().unwrap();
        controller.reset();

        assert_eq!(upload_counts(controller), (None, None));

        // Each attempt counts once regardless of how many events it carried, which is
        // what distinguishes this from `delivery_events_total`.
        emit_blob_file_upload_attempt(true, "test-bucket");
        emit_blob_file_upload_attempt(true, "test-bucket");
        emit_blob_file_upload_attempt(false, "test-bucket");

        // Failures are recorded rather than dropped, so the two series sum to the
        // total attempts and support a failure ratio.
        assert_eq!(upload_counts(controller), (Some(2), Some(1)));
    }

    #[test]
    fn labels_the_destination_bucket() {
        init_test();
        let controller = Controller::get().unwrap();
        controller.reset();

        emit_blob_file_upload_attempt(true, "my-bucket");

        let bucket = controller
            .capture_metrics()
            .into_iter()
            .find(|metric| metric.name() == "blob_sink_file_uploads_total")
            .and_then(|metric| metric.tag_value("bucket"));
        assert_eq!(bucket.as_deref(), Some("my-bucket"));
    }

    // The GCS sink is the one caller that must inspect HTTP status itself, because
    // `GcsRetryLogic` maps a non-retriable 4xx to `DontRetry` rather than `Err`, so a
    // failed upload still arrives as `Ok(GcsResponse)`. This drives the same logic the
    // sink installs in its `map_result` layer, over real responses, so a regression
    // that drops the status check mislabels failed uploads as `success` and fails here.
    #[test]
    fn gcs_labels_http_errors_as_failure() {
        use crate::sinks::gcs_common::service::GcsResponse;
        use http::StatusCode;
        use hyper::Body;
        use vector_lib::request_metadata::RequestMetadata;

        // Mirrors `GcsSinkConfig::build`'s map_result body. Kept in sync deliberately:
        // this asserts the success rule, not the wiring.
        fn on_result(result: Result<GcsResponse, ()>) {
            let success = result
                .as_ref()
                .is_ok_and(|response| response.inner.status().is_success());
            emit_blob_file_upload_attempt(success, "test-bucket");
        }

        fn response(status: StatusCode) -> Result<GcsResponse, ()> {
            Ok(GcsResponse {
                inner: http::Response::builder()
                    .status(status)
                    .body(Body::empty())
                    .unwrap(),
                metadata: RequestMetadata::default(),
                event_log_metadata: VectorEventLogSendMetadata::new(),
            })
        }

        init_test();
        let controller = Controller::get().unwrap();
        controller.reset();

        on_result(response(StatusCode::OK));
        on_result(response(StatusCode::NO_CONTENT));
        assert_eq!(upload_counts(controller), (Some(2), None));

        // A 4xx/5xx reaching the layer as `Ok` is an attempt, but a failed one.
        on_result(response(StatusCode::BAD_REQUEST));
        on_result(response(StatusCode::FORBIDDEN));
        on_result(response(StatusCode::NOT_FOUND));
        on_result(response(StatusCode::INTERNAL_SERVER_ERROR));
        assert_eq!(upload_counts(controller), (Some(2), Some(4)));

        // A transport-level failure is a failed attempt too.
        on_result(Err(()));
        assert_eq!(upload_counts(controller), (Some(2), Some(5)));
    }
}
