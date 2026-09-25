use bytes::{Buf, BufMut, BytesMut};
use quickcheck::{QuickCheck, TestResult};
use regex::Regex;
use similar_asserts::assert_eq;
use vector_buffers::encoding::Encodable;

use super::*;
use crate::config::log_schema;

fn encode_value<T: Encodable, B: BufMut>(value: T, buffer: &mut B) {
    value.encode(buffer).expect("encoding should not fail");
}

fn decode_value<T: Encodable, B: Buf + Clone>(buffer: B) -> T {
    T::decode(T::get_metadata(), buffer).expect("decoding should not fail")
}

// Ser/De the EventArray never loses bytes
#[test]
fn serde_eventarray_no_size_loss() {
    fn inner(events: EventArray) -> TestResult {
        let expected = events.clone();

        let mut buffer = BytesMut::with_capacity(64);
        encode_value(events, &mut buffer);

        let actual = decode_value::<EventArray, _>(buffer);
        assert_eq!(actual.size_of(), expected.size_of());

        TestResult::passed()
    }

    QuickCheck::new()
        .tests(1_000)
        .max_tests(10_000)
        .quickcheck(inner as fn(EventArray) -> TestResult);
}

// Ser/De the EventArray type through EncodeBytes -> DecodeBytes
#[test]
#[allow(clippy::neg_cmp_op_on_partial_ord)] // satisfying clippy leads to less
// clear expression
fn back_and_forth_through_bytes() {
    fn inner(events: EventArray) -> TestResult {
        let expected = events.clone();

        let mut buffer = BytesMut::with_capacity(64);
        encode_value(events, &mut buffer);

        let actual = decode_value::<EventArray, _>(buffer);

        assert_eq!(expected, actual);

        TestResult::passed()
    }

    QuickCheck::new()
        .tests(1_000)
        .max_tests(10_000)
        .quickcheck(inner as fn(EventArray) -> TestResult);
}

#[test]
fn delivery_event_count_round_trips_through_bytes() {
    let mut event = LogEvent::from("reduced log line");
    event.metadata_mut().set_delivery_event_count(7);
    let events = EventArray::from(Event::from(event));

    let mut buffer = BytesMut::with_capacity(64);
    encode_value(events, &mut buffer);

    let decoded = decode_value::<EventArray, _>(buffer);
    assert_eq!(
        decoded
            .into_events()
            .next()
            .unwrap()
            .as_log()
            .metadata()
            .delivery_event_count(),
        7
    );
}

#[test]
fn delivery_event_service_system_round_trips_through_bytes() {
    let mut event = LogEvent::from("log line");
    event
        .metadata_mut()
        .set_delivery_event_service_system("money-settings".to_string());
    let events = EventArray::from(Event::from(event));

    let mut buffer = BytesMut::with_capacity(64);
    encode_value(events, &mut buffer);

    let decoded = decode_value::<EventArray, _>(buffer);
    assert_eq!(
        decoded
            .into_events()
            .next()
            .unwrap()
            .as_log()
            .metadata()
            .delivery_event_service_system(),
        Some("money-settings")
    );
}

#[test]
fn delivery_path_accounting_preserves_source_labels_and_selection_through_disk_encoding() {
    use vector_common::internal_event::vector_event::delivery_path::{
        DeliveryPathAccounting, DeliverySourceLabels,
    };

    let source_labels = DeliverySourceLabels {
        process_generation_id: "original-process".to_owned(),
        time_parity: 3_600_000,
        topic: "service-log".to_owned(),
        delivery_method: "VECTOR_WOODCHUCK_V2_FILE".to_owned(),
        service_system: "money-settings".to_owned(),
        source_pod_id: "source-pod".to_owned(),
    };
    let mut first = LogEvent::from("first record");
    first
        .metadata_mut()
        .set_delivery_path_accounting(Some(DeliveryPathAccounting::at_read(
            source_labels.clone(),
            ["archive".to_owned(), "online".to_owned()],
        )));
    let mut selected = first.clone();
    selected
        .metadata_mut()
        .delivery_path_accounting_mut()
        .unwrap()
        .select_path("archive");
    selected.metadata_mut().merge(first.clone().into_parts().1);
    let mut later_source_labels = source_labels;
    later_source_labels.process_generation_id = "later-process".to_owned();
    later_source_labels.time_parity += 3_600_000;
    let mut later = LogEvent::from("later record");
    later
        .metadata_mut()
        .set_delivery_path_accounting(Some(DeliveryPathAccounting::at_read(
            later_source_labels,
            ["archive".to_owned()],
        )));
    selected.metadata_mut().merge(later.into_parts().1);
    selected.insert("message", "first record\nfirst record\nlater record");
    let expected = selected
        .metadata()
        .delivery_path_accounting()
        .unwrap()
        .clone();
    assert_eq!(expected.counts.len(), 4);
    let mut buffer = BytesMut::new();
    encode_value(EventArray::from(Event::from(selected)), &mut buffer);
    let decoded = decode_value::<EventArray, _>(buffer)
        .into_events()
        .next()
        .unwrap();
    assert_eq!(
        decoded.as_log().metadata().delivery_path_accounting(),
        Some(&expected)
    );
}

#[test]
fn uninstrumented_and_explicitly_empty_path_accounting_remain_distinct() {
    use vector_common::internal_event::vector_event::delivery_path::DeliveryPathAccounting;

    for accounting in [None, Some(DeliveryPathAccounting::default())] {
        let mut event = LogEvent::from("record");
        event
            .metadata_mut()
            .set_delivery_path_accounting(accounting.clone());
        let mut buffer = BytesMut::new();
        encode_value(EventArray::from(Event::from(event)), &mut buffer);
        let decoded = decode_value::<EventArray, _>(buffer)
            .into_events()
            .next()
            .unwrap();
        assert_eq!(
            decoded.as_log().metadata().delivery_path_accounting(),
            accounting.as_ref()
        );
    }
}

#[test]
fn serialization() {
    let mut event = LogEvent::from("raw log line");
    event.insert("foo", "bar");
    event.insert("bar", "baz");

    let expected_all = serde_json::json!({
        "message": "raw log line",
        "foo": "bar",
        "bar": "baz",
        "timestamp": event.get(log_schema().timestamp_key().unwrap().to_string().as_str()),
    });

    let actual_all = serde_json::to_value(event.all_event_fields().unwrap()).unwrap();
    assert_eq!(expected_all, actual_all);

    let rfc3339_re = Regex::new(r"\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z\z").unwrap();
    assert!(rfc3339_re.is_match(actual_all.pointer("/timestamp").unwrap().as_str().unwrap()));
}

#[test]
fn type_serialization() {
    use serde_json::json;

    let mut event = LogEvent::from("hello world");
    event.insert("int", 4);
    event.insert("float", 5.5);
    event.insert("bool", true);
    event.insert("string", "thisisastring");

    let map = serde_json::to_value(event.all_event_fields().unwrap()).unwrap();
    assert_eq!(map["float"], json!(5.5));
    assert_eq!(map["int"], json!(4));
    assert_eq!(map["bool"], json!(true));
    assert_eq!(map["string"], json!("thisisastring"));
}
