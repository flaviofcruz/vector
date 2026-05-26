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

// Fallback topic used by the woodchuck VRL for kubernetes_logs events that
// have neither an explicit source_context.topic nor a Lumberjack filename
// match. Mirrors `event_logs.libsonnet:155`.
const KUBERNETES_LOGS_FALLBACK_TOPIC: &str = "sawmill-service-log";

/// Identifies which source emitted the event, used to align topic resolution
/// with the woodchuck VRL's per-source fallback behavior.
pub const SOURCE_TYPE_FILE: &str = "file";
pub const SOURCE_TYPE_KUBERNETES_LOGS: &str = "kubernetes_logs";

/// Canonical `deliveryMethod` values written by the woodchuck VRL into
/// `logMetadata.deliveryMethod`. Used both on the read side (where the VRL
/// hasn't run yet, so we infer the value from `source_type`) and to keep the
/// metric label aligned with what downstream consumers already see in VEL.
/// Mirrors `woodchuck/configuration/components/transforms/`:
/// `*_log_daemon_wrapper.libsonnet`, `diskless.libsonnet`,
/// `application_heartbeats.libsonnet`, `file_based_raw_proto_streaming.libsonnet`.
pub const DELIVERY_METHOD_FILE: &str = "VECTOR_WOODCHUCK_V2_FILE";
/// Read-side label for kubernetes_logs source events. The woodchuck VRL does
/// not currently stamp this value on sawmill events, so the corresponding
/// sink-side counter falls through to `"unknown"` until that VRL is updated.
pub const DELIVERY_METHOD_KUBERNETES_LOGS: &str = "VECTOR_WOODCHUCK_V2_KUBERNETES_LOGS";

/// Extracts a Lumberjack topic from a source filename of either the active
/// form (`<TableNameCamelCase>[LaMigration].pb[.base64][.gz]`) or the
/// archived/rotated form (`<prefix>.<TableNameCamelCase>[LaMigration].pb[.base64][.gz]`),
/// returning the dash-cased table name (e.g. `"service-request-log"`).
/// Returns `None` for filenames that do not match either form.
pub fn extract_topic_from_source_filename(path: &str) -> Option<String> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        // The CamelCase capture is allowed to start at the beginning of the
        // string, after a `/` (directory separator), or after a `.` (timestamp
        // / hostname separator) so both active and archived filenames match.
        // Lazy `+?` on the CamelCase capture lets the optional `LaMigration`
        // suffix match outside the capture group rather than being absorbed.
        Regex::new(r"(?:^|[/.])([A-Z][A-Za-z0-9]+?)(LaMigration)?\.pb(?:\.base64)?(?:\.gz)?$")
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

/// Resolves the topic label for a source-read event. Mirrors the per-source
/// branching in `event_logs.libsonnet`'s `ConvertDeliveryInfoLogs`:
/// 1. `source_context.topic` if it is a concrete value (not the sentinel).
/// 2. Otherwise the topic inferred from the filename.
/// 3. Otherwise a source-specific fallback: `"sawmill-service-log"` for
///    `kubernetes_logs` sources, the inference sentinel for everything else.
fn resolve_received_topic(
    source_context: &HashMap<String, String>,
    path: &str,
    source_type: &str,
) -> String {
    let explicit = source_context.get("topic").map(String::as_str);
    match explicit {
        Some(t) if t != LUMBERJACK_TOPIC_INFERRED_SENTINEL => t.to_string(),
        _ => extract_topic_from_source_filename(path).unwrap_or_else(|| {
            if source_type == SOURCE_TYPE_KUBERNETES_LOGS {
                KUBERNETES_LOGS_FALLBACK_TOPIC.to_string()
            } else {
                LUMBERJACK_TOPIC_INFERRED_SENTINEL.to_string()
            }
        }),
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

/// Reads `deliveryMethod` from `value_map`, falling back to `"unknown"`.
/// Mirrors `topic_from_value_map` / `time_parity_from_value_map`. Splitting
/// the completeness ratio by this label distinguishes file-source deliveries
/// (`VECTOR_WOODCHUCK_V2_FILE`) from diskless gRPC deliveries
/// (`VECTOR_WOODCHUCK_V2_DISKLESS`), which is necessary because diskless
/// events have no corresponding read-side counter.
fn delivery_method_from_value_map(value_map: &HashMap<String, String>) -> String {
    value_map
        .get("deliveryMethod")
        .cloned()
        .unwrap_or_else(|| "unknown".to_string())
}

/// Maps a source's `source_type` to the canonical `deliveryMethod` value the
/// VRL pipeline assigns to events emitted by that source. Used on the read
/// side, where `logMetadata.deliveryMethod` isn't populated yet.
fn delivery_method_for_source_type(source_type: &str) -> &'static str {
    match source_type {
        SOURCE_TYPE_FILE => DELIVERY_METHOD_FILE,
        SOURCE_TYPE_KUBERNETES_LOGS => DELIVERY_METHOD_KUBERNETES_LOGS,
        _ => "unknown",
    }
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
    /// Source component that emitted this read event. Used by the metric to
    /// pick the right topic fallback when the filename doesn't match a
    /// Lumberjack convention. See `SOURCE_TYPE_FILE` / `SOURCE_TYPE_KUBERNETES_LOGS`.
    pub source_type: &'static str,
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
                "delivery_events_total",
                "delivery_event_type" => "VECTOR_SOURCE_READ",
                "time_parity" => current_hour_time_parity_ms(),
                "topic" => resolve_received_topic(&source_context, &self.path, self.source_type),
                "delivery_method" => delivery_method_for_source_type(self.source_type),
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
                "delivery_events_total",
                "delivery_event_type" => delivery_event_type.to_string(),
                "time_parity" => time_parity_from_value_map(&value.value_map),
                "topic" => topic_from_value_map(&value.value_map),
                "delivery_method" => delivery_method_from_value_map(&value.value_map),
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
    fn extract_topic_la_migration_archived() {
        // Archived/rotated form: timestamp prefix, ends in .gz.
        assert_eq!(
            extract_topic_from_source_filename(
                "/var/log/pods/sample/2026-05-22-01.ServiceRequestLogLaMigration.pb.base64.gz"
            )
            .as_deref(),
            Some("service-request-log"),
        );
    }

    #[test]
    fn extract_topic_la_migration_active() {
        // Active form: filename starts directly with the CamelCase, no prefix.
        assert_eq!(
            extract_topic_from_source_filename(
                "/var/log/pods/sample/ServiceRequestLogLaMigration.pb.base64"
            )
            .as_deref(),
            Some("service-request-log"),
        );
    }

    #[test]
    fn extract_topic_active_form_no_suffix() {
        // Active form without LaMigration suffix.
        assert_eq!(
            extract_topic_from_source_filename(
                "/var/log/pods/sample/ProductEventLog.pb.base64"
            )
            .as_deref(),
            Some("product-event-log"),
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
            resolve_received_topic(&ctx, "/some/file.json", SOURCE_TYPE_FILE),
            "audit-log"
        );
    }

    #[test]
    fn resolve_received_topic_infers_when_context_is_sentinel() {
        let mut ctx = HashMap::new();
        ctx.insert("topic".to_string(), "lumberjackTopicInfered".to_string());
        assert_eq!(
            resolve_received_topic(
                &ctx,
                "/path/to/12345.ServiceRequestLog.pb.base64",
                SOURCE_TYPE_FILE,
            ),
            "service-request-log"
        );
    }

    #[test]
    fn resolve_received_topic_falls_back_to_sentinel_for_file_source() {
        let mut ctx = HashMap::new();
        ctx.insert("topic".to_string(), "lumberjackTopicInfered".to_string());
        assert_eq!(
            resolve_received_topic(&ctx, "/path/to/nothing-matching.log", SOURCE_TYPE_FILE),
            "lumberjackTopicInfered"
        );
    }

    #[test]
    fn resolve_received_topic_falls_back_to_sawmill_for_kubernetes_logs() {
        // kubernetes_logs source with non-Lumberjack filename and no useful
        // source_context falls back to "sawmill-service-log" to match the
        // woodchuck VRL's ConvertDeliveryInfoLogs branch for k8s logs.
        let ctx = HashMap::new();
        assert_eq!(
            resolve_received_topic(
                &ctx,
                "/var/log/pods/some-pod/container/0.log",
                SOURCE_TYPE_KUBERNETES_LOGS,
            ),
            "sawmill-service-log"
        );
    }

    #[test]
    fn resolve_received_topic_kubernetes_logs_still_prefers_filename_match() {
        // A kubernetes_logs source reading a Lumberjack proto file still
        // resolves via filename — the sawmill-service-log fallback only
        // applies when no other resolution is available.
        let ctx = HashMap::new();
        assert_eq!(
            resolve_received_topic(
                &ctx,
                "/var/log/pods/sample/12345.AuditLog.pb.base64",
                SOURCE_TYPE_KUBERNETES_LOGS,
            ),
            "audit-log"
        );
    }

    #[test]
    fn resolve_received_topic_infers_when_context_missing() {
        let ctx = HashMap::new();
        assert_eq!(
            resolve_received_topic(&ctx, "/path/to/12345.AuditLog.pb.base64.gz", SOURCE_TYPE_FILE),
            "audit-log"
        );
    }

    #[test]
    fn delivery_method_for_source_type_known_values() {
        assert_eq!(
            delivery_method_for_source_type(SOURCE_TYPE_FILE),
            DELIVERY_METHOD_FILE
        );
        assert_eq!(
            delivery_method_for_source_type(SOURCE_TYPE_KUBERNETES_LOGS),
            DELIVERY_METHOD_KUBERNETES_LOGS
        );
    }

    #[test]
    fn delivery_method_for_source_type_unknown_falls_back() {
        assert_eq!(delivery_method_for_source_type("vector"), "unknown");
        assert_eq!(delivery_method_for_source_type(""), "unknown");
    }

    #[test]
    fn delivery_method_from_value_map_reads_existing_key() {
        let mut value_map = HashMap::new();
        value_map.insert("deliveryMethod".to_string(), "VECTOR_WOODCHUCK_V2_DISKLESS".to_string());
        assert_eq!(
            delivery_method_from_value_map(&value_map),
            "VECTOR_WOODCHUCK_V2_DISKLESS"
        );
    }

    #[test]
    fn delivery_method_from_value_map_falls_back_when_missing() {
        let value_map = HashMap::new();
        assert_eq!(delivery_method_from_value_map(&value_map), "unknown");
    }
}
