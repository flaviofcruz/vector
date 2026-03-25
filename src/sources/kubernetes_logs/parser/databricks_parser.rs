use bytes::Bytes;
use chrono::{DateTime, Utc};
use derivative::Derivative;
use vector_lib::{
    config::{LegacyKey, LogNamespace, log_schema},
    lookup::path,
};

use crate::{
    event::{Event, Value},
    internal_events::{DROP_EVENT, ParserMissingFieldError},
    sources::kubernetes_logs::{Config, transform_utils::get_message_path},
    transforms::{FunctionTransform, OutputBuffer},
};

const TIMESTAMP_KEY: &str = "timestamp";
// Reuse the STREAM key from the other parsers for the priority field.
const PRIORITY_KEY: &str = "stream";
const DEFAULT_PRIORITY: Bytes = Bytes::from_static(b"INFO");

/// Parser for structured and/or unstructured Databricks logs.
///
/// Expects logs to arrive in a structured or unstructured format. This parser never fails a parse;
/// instead, we will just pass on the message without transformation. If no timestamp can be
/// extracted from the log line, the event retains whatever timestamp was set at creation time
/// (the ingest timestamp), matching the behavior of the file source.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub(super) struct DatabricksParser {
    log_namespace: LogNamespace,
}

impl DatabricksParser {
    pub const fn new(log_namespace: LogNamespace) -> Self {
        Self { log_namespace }
    }
}

impl FunctionTransform for DatabricksParser {
    fn transform(&mut self, output: &mut OutputBuffer, mut event: Event) {
        let message_path = get_message_path(self.log_namespace);

        // Get the log field with the message, if it exists, and coerce it to bytes.
        let log = event.as_mut_log();
        let value = log.remove(&message_path).map(|s| s.coerce_to_bytes());
        match value {
            None => {
                // The message field was missing, inexplicably. If we can't find the message field, there's nothing for
                // us to actually decode, so there's no event we could emit, and so we just emit the error and return.
                emit!(ParserMissingFieldError::<{ DROP_EVENT }> {
                    field: &message_path.to_string()
                });
                return;
            }
            // Always parse the message as unstructured. Don't attempt to perform any formatting
            // on the log message; just insert the priority (and timestamp, if one was parsed).
            Some(s) => {
                let parsed_log = parse_log_as_unstructured(&s);

                drop(log.insert(&message_path, Value::Bytes(s.slice_ref(parsed_log.message))));

                if let Some(ts) = parsed_log.timestamp {
                    self.log_namespace.insert_source_metadata(
                        Config::NAME,
                        log,
                        log_schema().timestamp_key().map(LegacyKey::Overwrite),
                        path!(TIMESTAMP_KEY),
                        Value::Timestamp(ts),
                    );
                }
                let priority = if parsed_log.priority.is_empty() {
                    Value::Bytes(DEFAULT_PRIORITY)
                } else {
                    Value::Bytes(s.slice_ref(parsed_log.priority))
                };
                self.log_namespace.insert_source_metadata(
                    Config::NAME,
                    log,
                    Some(LegacyKey::Overwrite(path!(PRIORITY_KEY))),
                    path!(PRIORITY_KEY),
                    priority,
                );
            }
        }
        output.push(event);
    }
}

struct ParsedLog<'a> {
    timestamp: Option<DateTime<Utc>>,
    priority: &'a [u8],
    message: &'a [u8],
}

#[inline]
fn parse_log_as_unstructured(line: &[u8]) -> ParsedLog<'_> {
    ParsedLog {
        timestamp: None,
        priority: &[],
        message: line,
    }
}

#[cfg(test)]
pub mod tests {
    use super::{super::test_util, *};
    use crate::{event::LogEvent, test_util::trace_init};
    use bytes::Bytes;
    use vrl::value;

    /// Shared test cases.
    pub fn valid_cases(log_namespace: LogNamespace) -> Vec<(Bytes, Vec<Event>)> {
        vec![
            (
                Bytes::from(
                    "2016-10-06 00:17:09.669 INFO test_context: The content of the log entry 1",
                ),
                vec![test_util::make_log_event(
                    value!(
                        "2016-10-06 00:17:09.669 INFO test_context: The content of the log entry 1"
                    ),
                    "2016-10-06 00:17:09.669000000Z",
                    "INFO",
                    false,
                    log_namespace,
                )],
            ),
            (
                Bytes::from("2016-10-06 00:17:09.669 INFO test_context: First line of log entry 2"),
                vec![test_util::make_log_event(
                    value!("2016-10-06 00:17:09.669 INFO test_context: First line of log entry 2"),
                    "2016-10-06 00:17:09.669000000Z",
                    "INFO",
                    false,
                    log_namespace,
                )],
            ),
            (
                Bytes::from(
                    "2016-10-06 00:17:09.669 ERROR test_context: Second line of the log entry 2",
                ),
                vec![test_util::make_log_event(
                    value!(
                        "2016-10-06 00:17:09.669 ERROR test_context: Second line of the log entry 2"
                    ),
                    "2016-10-06 00:17:09.669000000Z",
                    "INFO",
                    false,
                    log_namespace,
                )],
            ),
        ]
    }

    #[test]
    fn test_parsing_valid_vector_namespace() {
        trace_init();
        for (message, expected) in valid_cases(LogNamespace::Vector) {
            let input = Event::Log(LogEvent::from(value!(message)));
            let mut parser = DatabricksParser::new(LogNamespace::Vector);
            let mut output = OutputBuffer::default();
            parser.transform(&mut output, input);

            let actual = output.into_events().collect::<Vec<_>>();

            test_util::compare_log_events_without_timestamp(LogNamespace::Vector, expected, actual);
        }
    }

    #[test]
    fn test_parsing_valid_legacy_namespace() {
        for (message, expected) in valid_cases(LogNamespace::Legacy) {
            let input = Event::Log(LogEvent::from(message));
            let mut parser = DatabricksParser::new(LogNamespace::Legacy);
            let mut output = OutputBuffer::default();
            parser.transform(&mut output, input);

            let actual = output.into_events().collect::<Vec<_>>();

            test_util::compare_log_events_without_timestamp(LogNamespace::Legacy, expected, actual);
        }
    }

    // Test handling of unparseable log lines of various types.
    fn test_parsing_invalid_messages(log_namespace: LogNamespace) {
        // Wrong/unexpected timestamp format. Handle as unstructured.
        let input_invalid_timestamp_bytes: Bytes = Bytes::from(
            "2016-10-06T00:17:10.113112111Z stderr test_context: Last line of the log entry 2",
        );
        let input_invalid_timestamp = if log_namespace == LogNamespace::Vector {
            Event::Log(LogEvent::from(value!(input_invalid_timestamp_bytes)))
        } else {
            Event::Log(LogEvent::from(input_invalid_timestamp_bytes))
        };
        let mut output = OutputBuffer::default();
        DatabricksParser::new(log_namespace).transform(&mut output, input_invalid_timestamp);
        let output_events = output.into_events().collect::<Vec<_>>();
        assert_eq!(output_events.len(), 1);
        // Other than the timestamp, the output log should follow some static format based on the
        // unparseable log message.
        let expected_log_events = vec![test_util::make_log_event(
            value!(
                "2016-10-06T00:17:10.113112111Z stderr test_context: Last line of the log entry 2"
            ),
            "2016-10-06 00:17:09.669000000Z",
            "INFO",
            false,
            log_namespace,
        )];
        test_util::compare_log_events_without_timestamp(
            log_namespace,
            expected_log_events,
            output_events,
        );
        // This is not valid UTF-8 string, ends with \n
        // 2021-08-05T17:35:26.640507539Z stdout P Hello World Привет Ми\xd1\n
        let input_invalid_utf8_bytes: Bytes = Bytes::from(vec![
            50, 48, 50, 49, 45, 48, 56, 45, 48, 53, 84, 49, 55, 58, 51, 53, 58, 50, 54, 46, 54, 52,
            48, 53, 48, 55, 53, 51, 57, 90, 32, 115, 116, 100, 111, 117, 116, 32, 80, 32, 72, 101,
            108, 108, 111, 32, 87, 111, 114, 108, 100, 32, 208, 159, 209, 128, 208, 184, 208, 178,
            208, 181, 209, 130, 32, 208, 156, 208, 184, 209, 10,
        ]);
        let input_invalid_utf8 = if log_namespace == LogNamespace::Vector {
            Event::Log(LogEvent::from(value!(input_invalid_utf8_bytes.clone())))
        } else {
            Event::Log(LogEvent::from(input_invalid_utf8_bytes.clone()))
        };
        output = OutputBuffer::default();
        DatabricksParser::new(log_namespace).transform(&mut output, input_invalid_utf8);
        let output_events = output.into_events().collect::<Vec<_>>();
        assert_eq!(output_events.len(), 1);
        let expected_log_events = vec![test_util::make_log_event(
            value!(input_invalid_utf8_bytes),
            "2016-10-06 00:17:09.669000000Z",
            "INFO",
            false,
            log_namespace,
        )];
        test_util::compare_log_events_without_timestamp(
            log_namespace,
            expected_log_events,
            output_events,
        );
    }

    #[test]
    fn test_parsing_invalid_vector_namespace() {
        test_parsing_invalid_messages(LogNamespace::Vector);
    }

    #[test]
    fn test_parsing_invalid_legacy_namespace() {
        test_parsing_invalid_messages(LogNamespace::Legacy);
    }

    #[test]
    fn test_parser_does_not_set_timestamp_legacy() {
        use vector_lib::lookup::event_path;

        let original_timestamp = DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        // Create a log event with a known timestamp already set (simulating the ingest timestamp).
        let mut log = crate::event::LogEvent::default();
        log.insert(event_path!("message"), "some log line");
        log.insert(event_path!("timestamp"), original_timestamp);
        let input = Event::Log(log);

        let mut parser = DatabricksParser::new(LogNamespace::Legacy);
        let mut output = OutputBuffer::default();
        parser.transform(&mut output, input);

        let events: Vec<_> = output.into_events().collect();
        assert_eq!(events.len(), 1);

        let log = events[0].as_log();
        // The timestamp field should still be the original ingest timestamp, not Utc::now().
        let ts = log.get(event_path!("timestamp")).expect("timestamp should exist");
        match ts {
            Value::Timestamp(t) => {
                assert_eq!(
                    *t, original_timestamp,
                    "parser should not overwrite the event timestamp"
                );
            }
            other => panic!("expected Timestamp value, got {:?}", other),
        }
    }

    #[test]
    fn test_parser_does_not_set_timestamp_vector() {
        use vector_lib::lookup::metadata_path;

        let input = Event::Log(LogEvent::from(value!("some log line")));

        let mut parser = DatabricksParser::new(LogNamespace::Vector);
        let mut output = OutputBuffer::default();
        parser.transform(&mut output, input);

        let events: Vec<_> = output.into_events().collect();
        assert_eq!(events.len(), 1);

        let log = events[0].as_log();
        // The parser should NOT have inserted a timestamp into the source metadata.
        let ts = log.get(metadata_path!(Config::NAME, "timestamp"));
        assert!(
            ts.is_none(),
            "parser should not set a timestamp in source metadata, but found: {:?}",
            ts
        );
    }
}
