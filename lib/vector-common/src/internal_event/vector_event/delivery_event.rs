use crate::internal_event::{InternalEvent, NamedInternalEvent};
use chrono::Utc;
use metrics::counter;
use regex::Regex;
use std::collections::HashMap;
use std::env;
use std::ops::Add;
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicU32, Ordering},
};

// Sentinel emitted when a Lumberjack source's topic cannot be resolved from
// either the source context or the filename. The spelling — including the
// "Infered" typo — matches the value used elsewhere in the pipeline.
const LUMBERJACK_TOPIC_INFERRED_SENTINEL: &str = "lumberjackTopicInfered";

/// Extracts a Lumberjack topic from a source filename of the form
/// `<prefix>.<TableNameCamelCase>[LaMigration].pb[.base64][.gz]`, returning
/// the dash-cased table name (e.g. `"service-request-log"`). Returns `None`
/// for filenames that do not match the convention.
pub fn extract_topic_from_source_filename(path: &str) -> Option<String> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        // Lazy `+?` on the CamelCase capture lets the optional `LaMigration`
        // suffix match outside the capture group rather than being absorbed.
        Regex::new(r"\.([A-Z][A-Za-z0-9]+?)(LaMigration)?\.pb(?:\.base64)?(?:\.gz)?$")
            .expect("topic extraction regex must compile")
    });
    let captures = re.captures(path)?;
    let camel = captures.get(1)?.as_str();
    Some(camel_to_dash_case(camel))
}

/// Converts a CamelCase identifier to dash-case
/// (e.g. `"ServiceRequestLog"` -> `"service-request-log"`).
fn camel_to_dash_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for (i, c) in s.chars().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            out.push('-');
        }
        out.push(c.to_ascii_lowercase());
    }
    out
}

/// Resolves the topic label for a source-read event:
/// 1. `source_context.topic` if it is a concrete value (not the sentinel).
/// 2. Otherwise the topic inferred from the filename.
/// 3. Otherwise the inference sentinel, so unresolved cases are filterable
///    in downstream queries.
fn resolve_received_topic(source_context: &HashMap<String, String>, path: &str) -> String {
    let explicit = source_context.get("topic").map(String::as_str);
    match explicit {
        Some(t) if t != LUMBERJACK_TOPIC_INFERRED_SENTINEL => t.to_string(),
        _ => extract_topic_from_source_filename(path)
            .unwrap_or_else(|| LUMBERJACK_TOPIC_INFERRED_SENTINEL.to_string()),
    }
}

/// Hour-rounded Unix-milliseconds bucket for the current time, as a string.
/// The format (ms since epoch, rounded down to the hour) matches the
/// `timeParity` value carried by events through the rest of the pipeline,
/// so labels emitted from here are directly comparable to that field.
fn current_hour_time_parity_ms() -> String {
    const HOUR_MS: i64 = 60 * 60 * 1000;
    let now_ms = Utc::now().timestamp_millis();
    (now_ms - (now_ms % HOUR_MS)).to_string()
}

/// Reads `timeParity` from `value_map`, falling back to the current hour.
fn time_parity_from_value_map(value_map: &HashMap<String, String>) -> String {
    value_map
        .get("timeParity")
        .cloned()
        .unwrap_or_else(current_hour_time_parity_ms)
}

/// Reads `topic` from `value_map`, falling back to `"unknown"`.
fn topic_from_value_map(value_map: &HashMap<String, String>) -> String {
    value_map
        .get("topic")
        .cloned()
        .unwrap_or_else(|| "unknown".to_string())
}

// Env flag to gate when we should be emitting the read event (want to support both while we check the performance of the new spot)
pub static EMIT_READ_EVENT_AFTER_MULTILINE_AGG: OnceLock<bool> = OnceLock::new();
pub fn emit_read_event_after_multiline_agg() -> bool {
    *EMIT_READ_EVENT_AFTER_MULTILINE_AGG.get_or_init(|| {
        env::var("EMIT_READ_EVENT_AFTER_MULTILINE_AGG")
            .map(|v| v == "true")
            .unwrap_or(false)
    })
}

#[derive(Debug)]
pub struct DeliveryReadEvent {
    pub path: String,
    pub bytes_read: usize,
    pub lines_read: usize,
    pub source_context: Option<HashMap<String, String>>,
    // Read event is changing spots in the file source to after multilne agg. This new spot may not be as stable / performative
    // So we want to have a mark that tracks whether it's coming from this new spot that pairs with an ENV gate
    pub emitted_after_multiline_agg: bool,
}

impl DeliveryReadEvent {
    fn should_emit(&self) -> bool {
        // Emit only once either if it's before multiline and the flag is off, or after with flag on
        (self.emitted_after_multiline_agg && emit_read_event_after_multiline_agg())
            || (!self.emitted_after_multiline_agg && !emit_read_event_after_multiline_agg())
    }
}

impl NamedInternalEvent for DeliveryReadEvent {
    fn name(&self) -> &'static str {
        "DeliveryReadEvent"
    }
}

impl InternalEvent for DeliveryReadEvent {
    fn emit(self) {
        if self.should_emit() {
            let source_context = self.source_context.clone().unwrap_or_default();
            info!(
                message = "Delivery event: READ_MESSAGES.",
                file = %self.path,
                num_bytes = self.bytes_read,
                num_events = self.lines_read,
                delivery_event_type = "VECTOR_SOURCE_READ",
                vector_event_type = "VECTOR_LOG_DELIVERY_EVENT",
                internal_log_rate_limit = false,
                // info! needs explicit field names at compile time, so we can't just log the whole map as individual fields
                // Instead, we have to convert to JSON string and unwrap later on
                source_context = serde_json::to_string(&source_context).unwrap(),
            );

            counter!(
                "events_received_total",
                "delivery_event_type" => "VECTOR_SOURCE_READ",
                "time_parity" => current_hour_time_parity_ms(),
                "topic" => resolve_received_topic(&source_context, &self.path),
            )
            .increment(self.lines_read as u64);
        }
    }
}

/*
* Struct to help track the count/size of events per unique combination of specified fields
*/
#[derive(Clone, Debug)]
pub struct MetadataValuesCount {
    pub value_map: HashMap<String, String>,
    pub count: usize,
    pub size: usize,
}

// Struct for vector sink delivery events (staged, delivered)
// This doesn't match the actual delivery event structure (the real events are exploded from the count map)
// But similar to send events, it helps us easily extend granularity without needing a vector code change
#[derive(Clone, Debug, Default)]
pub struct VectorSinkDeliveryEvent {
    pub count_map: HashMap<String, MetadataValuesCount>,
    // Field for testing purposes use Arc<AtomicU32> to ensure something that carries info even across cloning
    pub delivered_call_count: Arc<AtomicU32>,
}

impl VectorSinkDeliveryEvent {
    pub fn new() -> Self {
        Self {
            count_map: HashMap::new(),
            delivered_call_count: Arc::new(AtomicU32::new(0)),
        }
    }

    pub fn with_count_map(count_map: HashMap<String, MetadataValuesCount>) -> Self {
        Self {
            count_map,
            delivered_call_count: Arc::new(AtomicU32::new(0)),
        }
    }

    pub fn emit_delivered_event(&self) {
        // Mark for testing
        self.delivered_call_count.fetch_add(1, Ordering::SeqCst);

        // VECTOR_DELIVERED_MESSAGES_EVENT
        self.emit_count_map(
            "Delivery event: SINK_DELIVERED_MESSAGES",
            "VECTOR_SINK_UPLOAD_DELIVERED",
        )
    }

    pub fn emit_staged_event(&self) {
        // VECTOR_STAGED_MESSAGES_EVENT
        self.emit_count_map(
            "Delivery event: SINK_STAGED_MESSAGES",
            "VECTOR_SINK_UPLOAD_STAGED",
        )
    }

    fn emit_count_map(&self, message: &str, delivery_event_type: &str) {
        for value in self.count_map.values() {
            info!(
                message = message,
                keys = serde_json::to_string(&value.value_map).unwrap(),
                delivery_event_type = delivery_event_type,
                vector_event_type = "VECTOR_LOG_DELIVERY_EVENT",
                num_events = value.count,
                num_bytes = value.size,
                // Specifying this allows us to emit without rate limiting (needed for high throughput sinks)
                internal_log_rate_limit = false,
            );

            counter!(
                "events_delivered_total",
                "delivery_event_type" => delivery_event_type.to_string(),
                "time_parity" => time_parity_from_value_map(&value.value_map),
                "topic" => topic_from_value_map(&value.value_map),
            )
            .increment(value.count as u64);
        }
    }
}

pub fn combine_sink_delivery_events(
    events: Vec<VectorSinkDeliveryEvent>,
) -> VectorSinkDeliveryEvent {
    let mut combined_map: HashMap<String, MetadataValuesCount> = HashMap::new();

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

    VectorSinkDeliveryEvent::with_count_map(combined_map)
}

impl Add<VectorSinkDeliveryEvent> for VectorSinkDeliveryEvent {
    type Output = VectorSinkDeliveryEvent;

    fn add(self, other: VectorSinkDeliveryEvent) -> Self::Output {
        combine_sink_delivery_events(vec![self, other])
    }
}

#[cfg(test)]
mod topic_inference_tests {
    use super::*;

    #[test]
    fn camel_to_dash_case_basic() {
        assert_eq!(camel_to_dash_case("ServiceRequestLog"), "service-request-log");
        assert_eq!(camel_to_dash_case("Log"), "log");
        assert_eq!(camel_to_dash_case("ABC"), "a-b-c");
    }

    #[test]
    fn extract_topic_standard_pb_base64() {
        assert_eq!(
            extract_topic_from_source_filename(
                "/var/lib/kubelet/pods/abc/volumes/logs/12345.ServiceRequestLog.pb.base64"
            )
            .as_deref(),
            Some("service-request-log"),
        );
    }

    #[test]
    fn extract_topic_standard_pb_base64_gz() {
        assert_eq!(
            extract_topic_from_source_filename(
                "/var/lib/kubelet/pods/abc/volumes/logs/12345.ProductEventLog.pb.base64.gz"
            )
            .as_deref(),
            Some("product-event-log"),
        );
    }

    #[test]
    fn extract_topic_la_migration_variant() {
        // The LaMigration suffix is stripped before dash-casing.
        assert_eq!(
            extract_topic_from_source_filename(
                "/var/log/pods/sample/12345.ServiceRequestLogLaMigration.pb.base64"
            )
            .as_deref(),
            Some("service-request-log"),
        );
    }

    #[test]
    fn extract_topic_bare_pb() {
        // Some files end at .pb without .base64.
        assert_eq!(
            extract_topic_from_source_filename(
                "/var/log/pods/sample/12345.BackgroundActivityLog.pb"
            )
            .as_deref(),
            Some("background-activity-log"),
        );
    }

    #[test]
    fn extract_topic_returns_none_for_non_lumberjack() {
        // HAL / nginx access logs don't follow the Lumberjack convention.
        assert!(
            extract_topic_from_source_filename(
                "/var/log/pods/sample/nginx/service-access.log"
            )
            .is_none()
        );
        assert!(
            extract_topic_from_source_filename("/var/log/pods/sample/audit.json").is_none()
        );
        assert!(extract_topic_from_source_filename("no-pattern-at-all").is_none());
    }

    #[test]
    fn resolve_received_topic_uses_explicit_context_when_present() {
        let mut ctx = HashMap::new();
        ctx.insert("topic".to_string(), "audit-log".to_string());
        assert_eq!(
            resolve_received_topic(&ctx, "/some/file.json"),
            "audit-log"
        );
    }

    #[test]
    fn resolve_received_topic_infers_when_context_is_sentinel() {
        let mut ctx = HashMap::new();
        ctx.insert("topic".to_string(), "lumberjackTopicInfered".to_string());
        assert_eq!(
            resolve_received_topic(&ctx, "/path/to/12345.ServiceRequestLog.pb.base64"),
            "service-request-log"
        );
    }

    #[test]
    fn resolve_received_topic_falls_back_to_sentinel_when_no_match() {
        let mut ctx = HashMap::new();
        ctx.insert("topic".to_string(), "lumberjackTopicInfered".to_string());
        assert_eq!(
            resolve_received_topic(&ctx, "/path/to/nothing-matching.log"),
            "lumberjackTopicInfered"
        );
    }

    #[test]
    fn resolve_received_topic_infers_when_context_missing() {
        let ctx = HashMap::new();
        assert_eq!(
            resolve_received_topic(&ctx, "/path/to/12345.AuditLog.pb.base64.gz"),
            "audit-log"
        );
    }
}
