// Extra functions for event log reporting functionality

use serde_json;
use std::collections::HashMap;
use std::env;
use std::sync::OnceLock;

use crate::event::proto::EventWrapper;
use crate::event::{Event, EventArray, LogEvent};

use vector_common::internal_event::vector_event::{
    EventWithEventLog, VectorSinkEventMetadata,
    delivery_event::{MetadataValuesCount, VectorSinkDeliveryEvent},
    file_send_event::{FileEventMetadata, VectorFileSendEvent},
};

use vector_common::byte_size_of::ByteSizeOf;

pub static EVENT_LOG_METADATA_FIELD: OnceLock<String> = OnceLock::new();
// Using the granularity tracking method, we want to support two different events with potentially different granularity
// One for send/upload events (EVENT_LOG_GRANULARITY_FIELDS) and one for delivery events (DELIVERY_EVENT_LOG_GRANULARITY_FIELDS)
pub static EVENT_LOG_GRANULARITY_FIELDS: OnceLock<Vec<String>> = OnceLock::new();
pub static DELIVERY_EVENT_LOG_GRANULARITY_FIELDS: OnceLock<Vec<String>> = OnceLock::new();
pub static EVENT_LOG_COUNT_OVERRIDE_FIELD: OnceLock<String> = OnceLock::new();

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
            count_map
                .entry(build_key(
                    log_event,
                    log_metadata_field,
                    granularity_fields,
                    for_delivery_events,
                ))
                .and_modify(|x: &mut MetadataValuesCount| {
                    x.count += message_count;
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
                    size: message_bytes,
                });
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
}
