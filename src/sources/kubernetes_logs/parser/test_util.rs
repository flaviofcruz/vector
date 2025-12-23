#![cfg(test)]
use chrono::{DateTime, Utc};
use similar_asserts::assert_eq;
use vector_lib::{
    config::LogNamespace,
    event,
    lookup::{event_path, metadata_path},
};
use vrl::{value, value::Value};

use crate::{
    event::{Event, LogEvent},
    sources::kubernetes_logs::Config,
    transforms::{FunctionTransform, OutputBuffer},
};

/// Build a log event for test purposes.
///
/// The implementation is shared, and therefore consistent across all
/// the parsers.
pub fn make_log_event(
    message: Value,
    timestamp: &str,
    stream: &str,
    is_partial: bool,
    log_namespace: LogNamespace,
) -> Event {
    let timestamp = DateTime::parse_from_rfc3339(timestamp)
        .expect("invalid timestamp in test case")
        .with_timezone(&Utc);

    let log = match log_namespace {
        LogNamespace::Vector => {
            let mut log = LogEvent::from(value!(message));
            log.insert(metadata_path!(Config::NAME, "timestamp"), timestamp);
            log.insert(metadata_path!(Config::NAME, "stream"), stream);
            if is_partial {
                log.insert(metadata_path!(Config::NAME, event::PARTIAL), true);
            }

            log
        }
        LogNamespace::Legacy => {
            let mut log = LogEvent::default();

            log.insert(event_path!("message"), message);
            log.insert(event_path!("timestamp"), timestamp);
            log.insert(event_path!("stream"), stream);
            if is_partial {
                log.insert(event_path!(event::PARTIAL), true);
            }

            log
        }
    };

    Event::Log(log)
}

/// Shared logic for testing parsers.
///
/// Takes a parser builder and a list of test cases.
pub fn test_parser<B, L, S, F>(builder: B, loader: L, cases: Vec<(S, Vec<Event>)>)
where
    B: Fn() -> F,
    F: FunctionTransform,
    L: Fn(S) -> Event,
{
    for (message, expected) in cases {
        let input = loader(message);
        let mut parser = (builder)();
        let mut output = OutputBuffer::default();
        parser.transform(&mut output, input);

        let actual = output.into_events().collect::<Vec<_>>();

        assert_eq!(expected, actual, "expected left, actual right");
    }
}

pub fn compare_log_events_without_timestamp(
    log_namespace: LogNamespace,
    expected_log_events: Vec<Event>,
    output_log_events: Vec<Event>,
) {
    assert_eq!(
        expected_log_events.len(),
        output_log_events.len(),
        "expected and output log events have different lengths: {} != {}",
        expected_log_events.len(),
        output_log_events.len()
    );
    for (expected_log_event, output_log_event) in
        expected_log_events.iter().zip(output_log_events.iter())
    {
        let mut expected_log_event_owned = expected_log_event.clone();
        let expected_log_event_mut_log = expected_log_event_owned.as_mut_log();
        if log_namespace == LogNamespace::Vector {
            expected_log_event_mut_log.remove(metadata_path!(Config::NAME, "timestamp"));
            expected_log_event_mut_log.remove(metadata_path!(Config::NAME, "source_event_id"));
        } else {
            expected_log_event_mut_log.remove(event_path!("timestamp"));
            expected_log_event_mut_log.remove(metadata_path!(Config::NAME, "source_event_id"));
        };
        let mut output_log_owned = output_log_event.clone();
        let output_log_mut_log = output_log_owned.as_mut_log();
        if log_namespace == LogNamespace::Vector {
            output_log_mut_log.remove(metadata_path!(Config::NAME, "timestamp"));
            output_log_mut_log.remove(metadata_path!(Config::NAME, "source_event_id"));
        } else {
            output_log_mut_log.remove(event_path!("timestamp"));
            output_log_mut_log.remove(metadata_path!(Config::NAME, "source_event_id"));
        };
        assert_eq!(
            expected_log_event_owned, output_log_owned,
            "expected left, actual right"
        );
    }
}
