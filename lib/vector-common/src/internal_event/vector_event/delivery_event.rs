use crate::internal_event::{InternalEvent, NamedInternalEvent};
use chrono::Utc;
use metrics::counter;
use std::collections::HashMap;
use std::env;
use std::ops::Add;
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicU32, Ordering},
};

// Computes an hour-rounded Unix-milliseconds bucket for the current time, as a string.
//
// Format matches the `timeParity` field convention used throughout woodchuck VRL
// transforms (e.g. `file_based_raw_proto_streaming.libsonnet`:
//     `time_parity = uploadTime - mod(uploadTime, 60 * 60 * 1000)`),
// so the counter's `time_parity` label is directly comparable to the values
// emitted into the existing vector-event-log Kafka topic / Lumberjack table.
//
// Used as a fallback in the receive-time path (where logMetadata has not yet been
// built by downstream VRL) and as the default for the delivered path when
// value_map["timeParity"] is missing.
fn current_hour_time_parity_ms() -> String {
    const HOUR_MS: i64 = 60 * 60 * 1000;
    let now_ms = Utc::now().timestamp_millis();
    (now_ms - (now_ms % HOUR_MS)).to_string()
}

// Reads `timeParity` from value_map when present (set upstream by the
// `file_based_raw_proto_streaming` and `file_based_unstructured_log_daemon_wrapper`
// VRL transforms in woodchuck, both of which round uploadTime to hour boundaries).
// Falls back to the current hour for events whose pipelines do not set logMetadata.
//
// woodchuck's DELIVERY_EVENT_LOG_GRANULARITY_FIELDS already includes "timeParity",
// so this field is populated in value_map for delivered/staged events by the
// existing build_map machinery — no Phase 2 plumbing required for the delivered side.
fn time_parity_from_value_map(value_map: &HashMap<String, String>) -> String {
    value_map
        .get("timeParity")
        .cloned()
        .unwrap_or_else(current_hour_time_parity_ms)
}

// Reads `topic` from value_map if present, otherwise "unknown". Topics are bounded
// (~100 distinct values) so this is M3-safe.
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

            // Emit an event-time-bucketed counter for M3.
            // Labels are bounded: topic (~100), delivery_event_type (small enum),
            // time_parity (24 rolling hours, hour-rounded Unix milliseconds).
            // pod_name/container_name/file are intentionally NOT included —
            // woodchuck's internal_metrics pipeline strips those before exporting.
            //
            // On the receive side we always compute time_parity from now() because
            // the file source's source_context map carries only user-provided keys
            // (topic etc.) and not the logMetadata struct that downstream VRL
            // transforms attach later. The hour granularity is what matters for
            // non-oscillation: as long as receive and deliver land in the same
            // hour-bucket (which they will whenever delivery latency stays under
            // ~1 hour), the ratio is well-defined.
            let topic = source_context
                .get("topic")
                .cloned()
                .unwrap_or_else(|| "unknown".to_string());
            counter!(
                "events_received_total",
                "delivery_event_type" => "VECTOR_SOURCE_READ",
                "time_parity" => current_hour_time_parity_ms(),
                "topic" => topic,
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

            // Emit an event-time-bucketed counter for M3. Reads timeParity from
            // value_map, which is populated by the build_map() machinery using the
            // existing DELIVERY_EVENT_LOG_GRANULARITY_FIELDS = ["timeParity", "topic",
            // "deliveryMethod"] convention. Because the upstream VRL transform sets
            // `logMetadata.timeParity = uploadTime - mod(uploadTime, 3600000)` at
            // receive time, the delivered counter increments with the event's
            // *receive-time* bucket — not the current wall-clock hour. This is what
            // makes the `delivered_total / received_total` ratio non-oscillating
            // across the receive→deliver gap.
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
