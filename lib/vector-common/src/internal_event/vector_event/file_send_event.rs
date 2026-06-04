use super::delivery_event::MetadataValuesCount;
use std::collections::HashMap;
use std::env;
use std::sync::OnceLock;

pub static ENABLE_FILE_SEND_EVENTS: OnceLock<bool> = OnceLock::new();

// File send events aren't default enabled until the send/uploading events messages are deprecated
// Otherwise, we'll be double-sending events in VA which will be a lot of extra volume
pub fn enable_file_send_events() -> bool {
    *ENABLE_FILE_SEND_EVENTS.get_or_init(|| {
        env::var("ENABLE_FILE_SEND_EVENTS")
            .map(|v| v == "true")
            .unwrap_or(false)
    })
}

#[derive(Clone, Debug, Default)]
pub struct FileEventMetadata {
    pub bytes: usize,
    pub events_len: usize,
    pub blob: String,
    pub container: String,
}

impl FileEventMetadata {
    pub fn new(bytes: usize, events_len: usize, blob: String, container: String) -> Self {
        Self {
            bytes,
            events_len,
            blob,
            container,
        }
    }

    pub fn default() -> Self {
        Self {
            bytes: 0,
            events_len: 0,
            blob: "".to_string(),
            container: "".to_string(),
        }
    }
}

// Struct for vector file upload events (sending, uploaded)
// NOTE: This is NOT the same as vector sink delivery events
// Those are used to report general delivery stats for a sink whereas this is used to report single-file uploads
// i.e. the equivalent of LD delivery events vs. log sync events
#[derive(Clone, Debug, Default)]
pub struct VectorFileSendEvent {
    pub file_metadata: FileEventMetadata,
    // Count map here allows us to keep track of the count/size of events per combination of fields
    // Realistically, we should only have one combination of fields since one file upload should have the same metadata among all events
    // But this is still a useful pattern to have since it fits with existing parsing logic
    pub count_map: HashMap<String, MetadataValuesCount>,
}

impl VectorFileSendEvent {
    pub fn new() -> Self {
        Self {
            file_metadata: FileEventMetadata::default(),
            count_map: HashMap::new(),
        }
    }

    pub fn new_with_file_metadata(file_metadata: FileEventMetadata) -> Self {
        Self {
            file_metadata: file_metadata,
            count_map: HashMap::new(),
        }
    }

    pub fn emit_staged_event(&self) {
        if enable_file_send_events() {
            self.emit_file_send_event(
                "File send start.",
                "VECTOR_FILE_SEND_EVENT",
                "VECTOR_FILE_SEND_START",
            );
        }
    }

    pub fn emit_uploaded_event(&self) {
        if enable_file_send_events() {
            self.emit_file_send_event(
                "File send complete.",
                "VECTOR_FILE_SEND_EVENT",
                "VECTOR_FILE_SEND_COMPLETE",
            );
        }
    }

    pub fn emit_error_event(&self, error: String) {
        if enable_file_send_events() {
            info!(
                message = format!("File send error: {}", error),
                bytes = self.file_metadata.bytes,
                events_len = self.file_metadata.events_len,
                blob = self.file_metadata.blob,
                container = self.file_metadata.container,
                vector_event_type = "VECTOR_FILE_SEND_EVENT",
                file_send_event_type = "VECTOR_FILE_SEND_WARN",
                // Structured error reason so credential-expiry failures (e.g.
                // ExpiredToken) can be isolated from generic request failures in
                // the VEL send-event stream (ES-1899972). Mapped to the proto
                // VectorSendMessagesEvent.error_reason by the woodchuck VEL VRL.
                error_reason = error,
                internal_log_rate_limit = false,
            );
        }
    }

    fn emit_file_send_event(
        &self,
        message: &str,
        vector_event_type: &str,
        file_send_event_type: &str,
    ) {
        // We actually expect all events in a file to have the same metadata fields
        // But technically, we could send multiple per file if we misconfig + some logs in the same file have different metadata
        // Better to be more granular than less in this case, so we iterate through the metadata assuming it can have multiple
        for value in self.count_map.values() {
            info!(
                message = message,
                keys = serde_json::to_string(&value.value_map).unwrap(),
                bytes = value.size,
                events_len = value.count,
                blob = self.file_metadata.blob,
                container = self.file_metadata.container,
                vector_event_type = vector_event_type,
                file_send_event_type = file_send_event_type,
                internal_log_rate_limit = false,
            );
        }
    }
}

// NOTE: This adds up the count map but will only keep the first filename/blob/container etc.
pub fn combine_file_send_events(events: Vec<VectorFileSendEvent>) -> VectorFileSendEvent {
    let mut combined = VectorFileSendEvent::new();
    let mut combined_map: HashMap<String, MetadataValuesCount> = HashMap::new();
    // We're assuming we only use this to combine events from the same file upload
    // In that case, we just keep the first event's metadata assuming it's all the same
    if let Some(first_event) = events.first() {
        combined.file_metadata = first_event.file_metadata.clone();
    }
    for event in events {
        for (key, value) in &event.count_map {
            combined_map
                .entry(key.clone())
                .and_modify(|existing| {
                    existing.count += value.count;
                    existing.size += value.size;
                })
                .or_insert_with(|| value.clone());
        }
    }
    combined.count_map = combined_map;
    combined
}
