// Extra functions for event log reporting functionality

use serde_json;
use std::collections::HashMap;
use std::env;
use std::sync::OnceLock;

use crate::event::proto::EventWrapper;
use crate::event::{Event, EventArray, LogEvent, Value};

use vector_common::internal_event::vector_event::{
    EventWithEventLog, VectorSinkEventMetadata,
    delivery_event::{MetadataValuesCount, VectorSinkDeliveryEvent},
    delivery_path::DeliveryPathCount,
    file_send_event::{FileEventMetadata, VectorFileSendEvent},
};

use vector_common::byte_size_of::ByteSizeOf;

pub static EVENT_LOG_METADATA_FIELD: OnceLock<String> = OnceLock::new();
// Using the granularity tracking method, we want to support two different events with potentially different granularity
// One for send/upload events (EVENT_LOG_GRANULARITY_FIELDS) and one for delivery events (DELIVERY_EVENT_LOG_GRANULARITY_FIELDS)
pub static EVENT_LOG_GRANULARITY_FIELDS: OnceLock<Vec<String>> = OnceLock::new();
pub static DELIVERY_EVENT_LOG_GRANULARITY_FIELDS: OnceLock<Vec<String>> = OnceLock::new();
pub static EVENT_LOG_COUNT_OVERRIDE_FIELD: OnceLock<String> = OnceLock::new();
// The logMetadata sub-field holding the source-attribute map. Granularity fields control
// which event is emitted (they form the aggregation key, so each distinct combination is one
// event). Source identifiers do not: they are populated onto each emitted event from the first
// record seen in that group, so they never affect event-log fanout no matter their
// cardinality. Empty (the default) leaves it off.
pub static EVENT_LOG_SOURCE_ATTRIBUTES_FIELD: OnceLock<String> = OnceLock::new();

// Where we can find the log metadata object
pub fn get_event_log_metadata_field() -> &'static String {
    // Initialize the static variable once, or return the value if it's already initialized/computed
    EVENT_LOG_METADATA_FIELD
        .get_or_init(|| env::var("EVENT_LOG_METADATA_FIELD").unwrap_or_else(|_| "".to_string()))
}

// Where we can find the message count override field
pub fn get_event_log_count_override_field() -> &'static String {
    EVENT_LOG_COUNT_OVERRIDE_FIELD.get_or_init(|| {
        env::var("EVENT_LOG_COUNT_OVERRIDE_FIELD").unwrap_or_else(|_| "".to_string())
    })
}

// The single logMetadata sub-field holding the source-attribute map. Read once and copied
// verbatim onto file-send events, never used as a grouping key.
pub fn get_event_log_source_attributes_field() -> &'static String {
    EVENT_LOG_SOURCE_ATTRIBUTES_FIELD
        .get_or_init(|| env::var("EVENT_LOG_SOURCE_ATTRIBUTES_FIELD").unwrap_or_default())
}

// Within the log metadata object itself, these are fields we care to parse for
pub fn get_event_log_granularity_fields(for_delivery_events: bool) -> &'static Vec<String> {
    let (env_var, once_lock) = if for_delivery_events {
        (
            "DELIVERY_EVENT_LOG_GRANULARITY_FIELDS",
            &DELIVERY_EVENT_LOG_GRANULARITY_FIELDS,
        )
    } else {
        (
            "EVENT_LOG_GRANULARITY_FIELDS",
            &EVENT_LOG_GRANULARITY_FIELDS,
        )
    };
    once_lock.get_or_init(|| {
        let vec_string = env::var(env_var).unwrap_or_default();
        serde_json::from_str(&vec_string).unwrap_or_default()
    })
}

// Function to get the events of a desired field and encode them in a key so we more easily keep
// a map tracking size / count per unique combination of field values
fn build_key(
    event: &LogEvent,
    log_metadata_field: &str,
    granularity_fields: &Vec<String>,
    for_delivery_events: bool,
) -> String {
    let mut key_vals: Vec<String> = Vec::new();
    // Get the field that holds the metadata struct itself
    for key_part in granularity_fields {
        if for_delivery_events && key_part == "system" {
            if let Some(service_system) = event.metadata().delivery_event_service_system() {
                key_vals.push(format!("{}={}", key_part, service_system));
                continue;
            }
        }
        if let Ok(Some(val)) =
            event.parse_path_and_get_value(format!("{}.{}", log_metadata_field, key_part))
        {
            key_vals.push(format!("{}={}", key_part, val));
        }
    }
    key_vals.join("/")
}

// Creates a map with the values of the desired fields (i.e. {plane: PLANE_CONTROL})
fn build_map(
    event: &LogEvent,
    log_metadata_field: &str,
    granularity_fields: &Vec<String>,
    for_delivery_events: bool,
) -> HashMap<String, String> {
    let mut val_map = HashMap::new();
    for key_part in granularity_fields {
        if for_delivery_events && key_part == "system" {
            if let Some(service_system) = event.metadata().delivery_event_service_system() {
                val_map.insert(key_part.to_string(), service_system.to_string());
                continue;
            }
        }
        if let Ok(Some(val)) =
            event.parse_path_and_get_value(format!("{}.{}", log_metadata_field, key_part))
        {
            // Remove extra quotes from string
            val_map.insert(key_part.to_string(), val.to_string().replace("\"", ""));
        }
    }
    val_map
}

// Flattens the configured logMetadata sub-field (an object) to a string map. Not a granularity
// field, so it never affects which events are emitted. Null leaves are skipped.
fn build_source_attributes(
    event: &LogEvent,
    log_metadata_field: &str,
    source_attributes_field: &str,
) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if source_attributes_field.is_empty() {
        return map;
    }
    if let Ok(Some(val)) = event.parse_path_and_get_value(format!(
        "{}.{}",
        log_metadata_field, source_attributes_field
    )) {
        if let Some(obj) = val.as_object() {
            for (key, value) in obj {
                if matches!(value, Value::Null) {
                    continue;
                }
                map.insert(key.to_string(), value.to_string().replace('"', ""));
            }
        }
    }
    map
}

// Sometimes, an event might actually be a batch of messages
// To handle this, we allow the option for message itself to tell us the count to use
// If specified we use it, otherwise we default to 1
fn get_message_count(
    event: &Event,
    log_metadata_field: &str,
    message_count_override_field: &str,
    for_delivery_events: bool,
) -> usize {
    if message_count_override_field.is_empty() {
        return if for_delivery_events {
            event.as_log().metadata().delivery_event_count()
        } else {
            1
        };
    }
    let path = format!("{}.{}", log_metadata_field, message_count_override_field);
    match event.as_log().parse_path_and_get_value(path) {
        Ok(Some(value)) => {
            // If it's an integer, use that value as the count
            if let Some(count) = value.as_integer() {
                count as usize
            } else {
                1
            }
        }
        _ if for_delivery_events => event.as_log().metadata().delivery_event_count(),
        _ => 1,
    }
}

// Prefer the source-stamped `bytes` (true written-log bytes, carried in the
// metadata like the other granularity fields) when present, falling back to the
// in-memory `size_of()` estimate for events that don't carry it.
fn get_message_bytes(log_event: &LogEvent, log_metadata_field: &str) -> usize {
    let path = format!("{}.bytes", log_metadata_field);
    match log_event.parse_path_and_get_value(path) {
        Ok(Some(value)) => value
            .as_integer()
            .map_or_else(|| log_event.size_of(), |bytes| bytes as usize),
        _ => log_event.size_of(),
    }
}

/*
* On a list of events, iterate through them and track the counts per unique combination of
* specified fields
*
* The map here is String -> MetadataValuesCount
* where the String is an encoded key of the combination and values
* and MetadataValuesCount is a struct that holds the count, size, and a map of the values
*/
pub fn generate_count_map(
    events: &[Event],
    for_delivery_events: bool,
) -> HashMap<String, MetadataValuesCount> {
    let log_metadata_field = get_event_log_metadata_field();
    let granularity_fields = get_event_log_granularity_fields(for_delivery_events);
    let message_count_override_field = get_event_log_count_override_field();
    let source_attributes_field = get_event_log_source_attributes_field();
    let mut count_map = HashMap::new();
    for event in events {
        // Check if it's a log event (see enum defined in lib/vector-core/src/event/mod.rs)
        if let Event::Log(log_event) = event {
            let message_count = get_message_count(
                event,
                log_metadata_field,
                message_count_override_field,
                for_delivery_events,
            );
            let message_bytes = get_message_bytes(log_event, log_metadata_field);
            let accounting = log_event.metadata().delivery_path_accounting();
            let metric_count = if for_delivery_events && accounting.is_some() {
                0
            } else {
                message_count
            };
            count_map
                .entry(build_key(
                    log_event,
                    log_metadata_field,
                    granularity_fields,
                    for_delivery_events,
                ))
                .and_modify(|x: &mut MetadataValuesCount| {
                    x.count += message_count;
                    x.metric_count += metric_count;
                    x.size += message_bytes;
                })
                .or_insert(MetadataValuesCount {
                    value_map: build_map(
                        log_event,
                        log_metadata_field,
                        granularity_fields,
                        for_delivery_events,
                    ),
                    count: message_count,
                    metric_count,
                    size: message_bytes,
                    delivery_path_count: None,
                    // Not a grouping key: populated on file-send events only (skipped for
                    // delivery events, which never emit it) from the first record in the group.
                    source_attributes: if for_delivery_events {
                        HashMap::new()
                    } else {
                        build_source_attributes(
                            log_event,
                            log_metadata_field,
                            source_attributes_field,
                        )
                    },
                });

            if for_delivery_events {
                if let Some(accounting) = accounting {
                    for path_count in accounting.counts.iter().filter(|count| count.selected) {
                        let key = format!(
                            "delivery-path:{}",
                            serde_json::to_string(&(
                                &path_count.path,
                                &path_count.source,
                                path_count.selected,
                            ))
                            .expect("delivery path count keys must serialize")
                        );
                        count_map
                            .entry(key)
                            .and_modify(|value: &mut MetadataValuesCount| {
                                value.count = value.count.saturating_add(path_count.count as usize);
                                value.metric_count = value.count;
                                if let Some(existing) = &mut value.delivery_path_count {
                                    existing.count =
                                        existing.count.saturating_add(path_count.count);
                                }
                            })
                            .or_insert_with(|| MetadataValuesCount {
                                value_map: HashMap::new(),
                                count: path_count.count as usize,
                                metric_count: path_count.count as usize,
                                size: 0,
                                // Content ranges are needed only while transforming payloads,
                                // not in the per-request counter projection.
                                delivery_path_count: Some(DeliveryPathCount {
                                    path: path_count.path.clone(),
                                    source: path_count.source.clone(),
                                    count: path_count.count,
                                    selected: path_count.selected,
                                    content_ranges: Vec::new(),
                                }),
                                source_attributes: HashMap::new(),
                            });
                    }
                }
            }
        }
    }
    count_map
}

// Some sinks pass events with an extra EventWrapper around them, use this function to first unwrap
pub fn generate_count_map_event_wrapper(
    events: &Vec<EventWrapper>,
    for_delivery_events: bool,
) -> HashMap<String, MetadataValuesCount> {
    let event_map: Vec<Event> = events
        .iter()
        .map(|event: &EventWrapper| Event::from(event.clone()))
        .collect();
    generate_count_map(&event_map, for_delivery_events)
}

pub fn generate_count_map_from_event_array(
    events: &EventArray,
    for_delivery_events: bool,
) -> HashMap<String, MetadataValuesCount> {
    if let EventArray::Logs(logs) = events {
        let log_map: Vec<Event> = logs
            .iter()
            .map(|log: &LogEvent| Event::from(log.clone()))
            .collect();
        generate_count_map(&log_map, for_delivery_events)
    } else {
        HashMap::new()
    }
}

fn compute_event_log(event: Event) -> VectorSinkEventMetadata {
    let delivery_event =
        VectorSinkDeliveryEvent::with_count_map(generate_count_map(&vec![event.clone()], true));
    VectorSinkEventMetadata {
        delivery_event: delivery_event,
        file_send_event: None,
    }
}

fn compute_event_log_with_file_event(
    event: Event,
    file_metadata: FileEventMetadata,
) -> VectorSinkEventMetadata {
    let delivery_event =
        VectorSinkDeliveryEvent::with_count_map(generate_count_map(&vec![event.clone()], true));
    let file_send_event = Some(VectorFileSendEvent {
        file_metadata: file_metadata,
        count_map: generate_count_map(&vec![event.clone()], false),
    });
    VectorSinkEventMetadata {
        delivery_event: delivery_event,
        file_send_event: file_send_event,
    }
}

// Impl EventWithEventLog for the used log events
impl EventWithEventLog for Event {
    fn compute_event_log(&self) -> VectorSinkEventMetadata {
        compute_event_log(self.clone())
    }

    fn compute_event_log_with_file_event(
        &self,
        file_metadata: FileEventMetadata,
    ) -> VectorSinkEventMetadata {
        compute_event_log_with_file_event(self.clone(), file_metadata)
    }
}

#[cfg(test)]
mod tests {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use vector_common::internal_event::vector_event::delivery_event::{
        DeliveryEventStage, combine_sink_delivery_events, emit_delivery_counters,
    };
    use vector_common::internal_event::vector_event::delivery_path::{
        DeliveryPathAccounting, DeliverySourceLabels,
    };

    use super::*;

    fn event_with_delivery_count(count: usize) -> Event {
        let mut event = LogEvent::from("message");
        event.metadata_mut().set_delivery_event_count(count);
        event.into()
    }

    #[test]
    fn delivery_count_uses_reduced_event_lineage() {
        let event = event_with_delivery_count(4);

        assert_eq!(get_message_count(&event, "metadata", "count", true), 4);
    }

    #[test]
    fn non_delivery_count_ignores_reduced_event_lineage() {
        let event = event_with_delivery_count(4);

        assert_eq!(get_message_count(&event, "metadata", "count", false), 1);
    }

    #[test]
    fn explicit_message_count_overrides_reduced_event_lineage() {
        let mut event = event_with_delivery_count(4);
        event.as_mut_log().insert("log_metadata.message_count", 9);

        assert_eq!(
            get_message_count(&event, "log_metadata", "message_count", true),
            9
        );
    }

    #[test]
    fn mixed_batches_keep_vel_totals_but_emit_each_event_in_only_one_counter_form() {
        let mut event = event_with_delivery_count(7);
        let mut accounting = DeliveryPathAccounting::at_read(
            DeliverySourceLabels {
                process_generation_id: "source-process".to_owned(),
                time_parity: 3_600_000,
                topic: "service-log".to_owned(),
                delivery_method: "file".to_owned(),
                service_system: "service".to_owned(),
                source_pod_id: "pod".to_owned(),
            },
            ["blob".to_owned(), "online".to_owned()],
        );
        accounting.record_content(7);
        let mut selected = accounting.clone();
        selected.select_path("online");
        for accounting in [accounting, selected, DeliveryPathAccounting::default()] {
            let selected = accounting.counts.iter().any(|count| count.selected);
            event
                .metadata_mut()
                .set_delivery_path_accounting(Some(accounting));
            let enrolled_map = generate_count_map(&[event.clone()], true);
            assert_eq!(enrolled_map.len(), if selected { 2 } else { 1 });
            assert!(
                enrolled_map
                    .values()
                    .filter_map(|value| value.delivery_path_count.as_ref())
                    .all(|count| count.content_ranges.is_empty())
            );
            let unscoped = enrolled_map
                .values()
                .find(|v| v.delivery_path_count.is_none())
                .unwrap();
            assert_eq!(unscoped.count, 7);
            assert_eq!(unscoped.metric_count, 0);
            assert!(unscoped.size > 0);
            let recorder = DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            metrics::with_local_recorder(&recorder, || {
                emit_delivery_counters(enrolled_map.values(), DeliveryEventStage::Delivered);
            });
            let metrics = snapshotter.snapshot().into_vec();
            assert_eq!(metrics.len(), if selected { 4 } else { 2 });
            let mut byte_series = 0;
            for (key, _, _, value) in metrics {
                if key.key().name() == "delivery_event_bytes_total" {
                    byte_series += 1;
                    assert!(
                        key.key()
                            .labels()
                            .all(|label| label.key() != "delivery_path")
                    );
                    assert_eq!(value, DebugValue::Counter(unscoped.size as u64));
                } else {
                    assert_eq!(key.key().name(), "delivery_events_total");
                    assert!(selected);
                    assert_eq!(value, DebugValue::Counter(1));
                }
            }
            assert_eq!(byte_series, 2);

            // Exercise both count-map aggregation and merging across sink requests.
            let first = VectorSinkDeliveryEvent::with_count_map(generate_count_map(
                &[event.clone(), event_with_delivery_count(2)],
                true,
            ));
            let second = VectorSinkDeliveryEvent::with_count_map(generate_count_map(
                &[event.clone(), event_with_delivery_count(3)],
                true,
            ));
            let combined = combine_sink_delivery_events(vec![first.clone(), second.clone()]);
            let mut merged = first;
            merged.merge(second);
            for batch in [combined, merged] {
                let unscoped = batch
                    .count_map
                    .values()
                    .find(|v| v.delivery_path_count.is_none())
                    .unwrap();
                assert_eq!(unscoped.count, 19);
                assert_eq!(unscoped.metric_count, 5);
                for (stage, expected_label) in [
                    (DeliveryEventStage::Staged, "VECTOR_SINK_UPLOAD_STAGED"),
                    (
                        DeliveryEventStage::Delivered,
                        "VECTOR_SINK_UPLOAD_DELIVERED",
                    ),
                    (
                        DeliveryEventStage::SinkInputAccepted,
                        "VECTOR_SINK_INPUT_ACCEPTED",
                    ),
                ] {
                    let accepted = matches!(stage, DeliveryEventStage::SinkInputAccepted);
                    let recorder = DebuggingRecorder::new();
                    let snapshotter = recorder.snapshotter();
                    metrics::with_local_recorder(&recorder, || {
                        emit_delivery_counters(batch.count_map.values(), stage);
                    });
                    let metrics = snapshotter.snapshot().into_vec();
                    assert_eq!(metrics.len(), if selected && !accepted { 6 } else { 4 });
                    let mut byte_series = 0;
                    for (key, _, _, value) in metrics {
                        let labels = key
                            .key()
                            .labels()
                            .map(|l| (l.key(), l.value()))
                            .collect::<HashMap<_, _>>();
                        assert_eq!(labels["delivery_event_type"], expected_label);
                        if key.key().name() == "delivery_event_bytes_total" {
                            byte_series += 1;
                            assert!(!labels.contains_key("delivery_path"));
                            assert_eq!(value, DebugValue::Counter(unscoped.size as u64));
                            continue;
                        }
                        assert_eq!(key.key().name(), "delivery_events_total");
                        if let Some(path) = labels.get("delivery_path") {
                            assert_eq!(*path, "online");
                            assert_eq!(labels["source_process_generation_id"], "source-process");
                            assert_eq!(value, DebugValue::Counter(2));
                        } else {
                            assert_eq!(value, DebugValue::Counter(if accepted { 19 } else { 5 }));
                        }
                    }
                    assert_eq!(byte_series, 2);
                }
            }
        }
    }

    fn event_with_service_system(service_system: &str) -> LogEvent {
        let mut event = LogEvent::from("message");
        event
            .metadata_mut()
            .set_delivery_event_service_system(service_system.to_string());
        event
    }

    #[test]
    fn delivery_map_uses_internal_service_system() {
        let event = event_with_service_system("money-settings");

        assert_eq!(
            build_map(&event, "log_metadata", &vec!["system".to_string()], true,)
                .get("system")
                .map(String::as_str),
            Some("money-settings")
        );
    }

    #[test]
    fn internal_service_system_takes_precedence_over_event_field() {
        let mut event = event_with_service_system("source-system");
        event.insert("log_metadata.system", "explicit-system");

        assert_eq!(
            build_map(&event, "log_metadata", &vec!["system".to_string()], true,)
                .get("system")
                .map(String::as_str),
            Some("source-system")
        );
        assert_eq!(
            build_key(&event, "log_metadata", &vec!["system".to_string()], true),
            "system=source-system"
        );
    }

    #[test]
    fn file_send_map_does_not_use_delivery_service_system() {
        let event = event_with_service_system("money-settings");

        assert!(
            !build_map(&event, "log_metadata", &vec!["system".to_string()], false,)
                .contains_key("system")
        );
    }

    // ---- event-log grouping and stamping ----
    // build_key decides how many events are emitted: one per distinct combination of the
    // granularity fields. build_source_attributes reads the stamped object off an event and is
    // never part of that key, and aggregation keeps the first event's copy.

    // Builds a log event with the given logMetadata granularity leaves and source-attribute
    // sub-keys (under "source_attrs").
    fn log_event_with(
        granularity_leaves: &[(&str, &str)],
        attribute_leaves: &[(&str, &str)],
    ) -> LogEvent {
        let mut log = LogEvent::from("message");
        for &(name, value) in granularity_leaves {
            let path = format!("log_metadata.{name}");
            log.insert(path.as_str(), value);
        }
        for &(name, value) in attribute_leaves {
            let path = format!("log_metadata.source_attrs.{name}");
            log.insert(path.as_str(), value);
        }
        log
    }

    #[test]
    fn granularity_fields_determine_event_count() {
        // One event is emitted per distinct build_key result.
        struct Case {
            name: &'static str,
            granularity: Vec<&'static str>,
            events: Vec<Vec<(&'static str, &'static str)>>,
            expected_groups: usize,
        }
        let cases = vec![
            Case {
                name: "no granularity collapses to one event",
                granularity: vec![],
                events: vec![vec![("topic", "t1")], vec![("topic", "t2")]],
                expected_groups: 1,
            },
            Case {
                name: "one field splits by its value",
                granularity: vec!["topic"],
                events: vec![
                    vec![("topic", "t1")],
                    vec![("topic", "t1")],
                    vec![("topic", "t2")],
                ],
                expected_groups: 2,
            },
            Case {
                name: "two fields split by combination",
                granularity: vec!["topic", "pod"],
                events: vec![
                    vec![("topic", "t1"), ("pod", "a")],
                    vec![("topic", "t1"), ("pod", "b")],
                    vec![("topic", "t1"), ("pod", "a")],
                ],
                expected_groups: 2,
            },
        ];
        for case in cases {
            let granularity: Vec<String> =
                case.granularity.iter().map(|s| s.to_string()).collect();
            let keys: std::collections::HashSet<String> = case
                .events
                .iter()
                .map(|leaves| {
                    build_key(&log_event_with(leaves, &[]), "log_metadata", &granularity, false)
                })
                .collect();
            assert_eq!(keys.len(), case.expected_groups, "{}", case.name);
        }
    }

    #[test]
    fn source_attributes_are_read_from_the_events_object() {
        struct Case {
            name: &'static str,
            attrs: Vec<(&'static str, &'static str)>,
            expected: Vec<(&'static str, &'static str)>,
        }
        let cases = vec![
            Case {
                name: "single key",
                attrs: vec![("k", "v")],
                expected: vec![("k", "v")],
            },
            Case {
                name: "multiple keys",
                attrs: vec![("a", "1"), ("b", "2")],
                expected: vec![("a", "1"), ("b", "2")],
            },
            Case {
                name: "no object stays empty",
                attrs: vec![],
                expected: vec![],
            },
        ];
        for case in cases {
            let got = build_source_attributes(
                &log_event_with(&[], &case.attrs),
                "log_metadata",
                "source_attrs",
            );
            let want: HashMap<String, String> = case
                .expected
                .iter()
                .map(|&(k, v)| (k.to_string(), v.to_string()))
                .collect();
            assert_eq!(got, want, "{}", case.name);
        }
    }

    #[test]
    fn null_source_attribute_leaves_are_skipped() {
        let mut log = LogEvent::from("message");
        log.insert("log_metadata.source_attrs.present", "yes");
        log.insert("log_metadata.source_attrs.absent", Value::Null);
        let got = build_source_attributes(&log, "log_metadata", "source_attrs");
        assert_eq!(got.get("present").map(String::as_str), Some("yes"));
        assert!(!got.contains_key("absent"));
    }

    #[test]
    fn aggregating_a_group_keeps_the_first_source_attributes() {
        use vector_common::internal_event::vector_event::file_send_event::combine_file_send_events;

        // Same count-map key across events with differing source_attributes: aggregation keeps
        // the first event's, matching how a group stamps the first record it sees. Counts sum.
        let event = |attr: &str| {
            let mut count_map = HashMap::new();
            count_map.insert(
                "shared-key".to_string(),
                MetadataValuesCount {
                    value_map: HashMap::new(),
                    count: 1,
                    metric_count: 1,
                    size: 0,
                    delivery_path_count: None,
                    source_attributes: HashMap::from([("c".to_string(), attr.to_string())]),
                },
            );
            VectorFileSendEvent {
                file_metadata: FileEventMetadata::default(),
                count_map,
            }
        };
        let combined =
            combine_file_send_events(vec![event("first"), event("second"), event("third")]);
        let entry = combined.count_map.get("shared-key").unwrap();
        assert_eq!(
            entry.source_attributes.get("c").map(String::as_str),
            Some("first")
        );
        assert_eq!(entry.count, 3);
    }
}
