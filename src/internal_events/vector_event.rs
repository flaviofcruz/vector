// Structs used for our vector event logs
use regex::Regex;

use serde_json;
use std::collections::HashMap;

use vector_common::internal_event::vector_event::delivery_event::MetadataValuesCount;

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
}
