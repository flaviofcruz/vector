use std::fs;
use std::time::Instant;

use vector_lib::event::{Event, Value};
use vrl::path::{OwnedTargetPath, parse_target_path};

use crate::internal_events::{RedactionDuration, RedactionPatternMatched};
use crate::transforms::{FunctionTransform, OutputBuffer};

use super::buffer::RedactionBuffer;
use super::config::RedactConfig;
use super::redactor::{Redactor, RuleMatchCounts};
use super::rules::parse_rules_toml_with_validation;

/// The Redact transform applies secret redaction to configurable log fields.
///
/// Architecture (matching universe redactor design):
/// - The `redactor` contains compiled regex patterns and is stateless
/// - The `buffer` provides mutable working space, avoiding repeated allocations
/// - Buffer separation enables safe sharing of the redactor across contexts
#[derive(Clone)]
pub struct Redact {
    redactor: Redactor,
    field_paths: Vec<OwnedTargetPath>,
    buffer: RedactionBuffer,
}

impl Redact {
    /// Create a new Redact transform from the configuration.
    pub fn new(config: &RedactConfig) -> crate::Result<Self> {
        // Load the rules file content
        let rules_content = fs::read_to_string(&config.rules_file).map_err(|e| {
            format!(
                "Failed to read rules file '{}': {}",
                config.rules_file.display(),
                e
            )
        })?;

        // Parse the rules into patterns (with optional example validation)
        let patterns = parse_rules_toml_with_validation(&rules_content, config.validate_examples)
            .map_err(|e| {
            format!(
                "Failed to parse rules file '{}': {}",
                config.rules_file.display(),
                e
            )
        })?;

        // Log the name of all the loaded redaction patterns for debugging
        info!(
            "Loaded {} redaction patterns from '{}': {}",
            patterns.len(),
            config.rules_file.display(),
            patterns
                .clone()
                .iter()
                .map(|f| f.id.clone())
                .collect::<Vec<_>>()
                .join(", ")
        );

        let redactor = Redactor::from_patterns(patterns);

        // Parse field paths
        let field_paths: Result<Vec<_>, _> = config
            .fields
            .iter()
            .map(|field| {
                parse_target_path(field)
                    .map_err(|e| format!("Invalid field path '{}': {}", field, e))
            })
            .collect();
        let field_paths = field_paths?;

        // Use default capacity (0) matching universe implementation.
        // The buffer will grow dynamically as needed, avoiding wasted memory
        // for small messages and no allocation when no redaction occurs.
        let buffer = RedactionBuffer::default();

        Ok(Redact {
            redactor,
            field_paths,
            buffer,
        })
    }

    /// Emit metrics for redaction operations.
    /// Emits the duration metric and pattern match counts.
    fn emit_metrics(&self, start: Instant, rule_index_counts: RuleMatchCounts) {
        let duration_millis = start.elapsed().as_millis() as f64;
        emit!(RedactionDuration { duration_millis });

        for (rule_index, count) in rule_index_counts {
            if let Some(rule_id) = self.redactor.get_rule_name(rule_index) {
                emit!(RedactionPatternMatched {
                    rule_id,
                    count: count as u64,
                });
            }
        }
    }

    /// Redact a single field value if it's a string/bytes.
    /// Accumulates rule match counts into the provided HashMap.
    fn redact_value(&mut self, value: &mut Value, rule_counts: &mut RuleMatchCounts) {
        match value {
            Value::Bytes(bytes) => {
                let (redacted, pos) = self.redactor.redact(&mut self.buffer, bytes, rule_counts);
                if pos > 0 {
                    *bytes = redacted.to_vec().into();
                }
            }

            // Recursively redact object values (keys are intentionally preserved).
            // This matches universe Spark job redactor behavior - see:
            // - DatabricksLogRedactor.redactValues() (DatabricksLogRedactor.scala:1581-1585)
            // - Test: DatabricksLogRedactorSuite.scala:330-335
            // Rationale: Keys provide structural context for debugging; secrets are in values.
            Value::Object(map) => {
                for (_, v) in map.iter_mut() {
                    self.redact_value(v, rule_counts);
                }
            }

            // Recursively redact array elements
            Value::Array(arr) => {
                for item in arr.iter_mut() {
                    self.redact_value(item, rule_counts);
                }
            }

            // Other types (integers, floats, booleans, null) don't need redaction
            _ => {}
        }
    }
}

impl FunctionTransform for Redact {
    fn transform(&mut self, output: &mut OutputBuffer, mut event: Event) {
        let log = event.as_mut_log();

        let start = Instant::now();
        let mut rule_index_counts = RuleMatchCounts::new();

        // Iterate over field paths by index to avoid borrowing conflict
        for i in 0..self.field_paths.len() {
            let field_path = &self.field_paths[i];
            if let Some(value) = log.get_mut(field_path) {
                self.redact_value(value, &mut rule_index_counts);
            }
        }

        self.emit_metrics(start, rule_index_counts);

        output.push(event);
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use tempfile::NamedTempFile;
    use vector_lib::event::LogEvent;

    use super::*;
    use crate::transforms::test::transform_one;

    fn create_test_rules_file() -> NamedTempFile {
        let mut file = NamedTempFile::new().expect("Failed to create temp file");
        let rules_content = r#"
[patterns.test_password]
pattern = 'password=\S+'
replacement = 'password=REDACTED'

[patterns.test_email]
pattern = '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
replacement = 'EMAIL_REDACTED'

[patterns.test_aws_key]
pattern = 'AKIA[0-9A-Z]{14}'
replacement = 'AWS_KEY_REDACTED'
"#;
        file.write_all(rules_content.as_bytes())
            .expect("Failed to write rules");
        file.flush().expect("Failed to flush");
        file
    }

    struct TestCase {
        name: &'static str,
        fields: Vec<&'static str>,
        input: Vec<(&'static str, Value)>,
        expected: Vec<(&'static str, &'static str)>,
    }

    fn create_config(fields: Vec<&str>, rules_file: &NamedTempFile) -> RedactConfig {
        RedactConfig {
            fields: fields.iter().map(|s| s.to_string()).collect(),
            rules_file: rules_file.path().to_path_buf(),
            validate_examples: false,
        }
    }

    #[test]
    fn test_redact_table() {
        let test_cases = vec![
            TestCase {
                name: "simple password redaction",
                fields: vec!["message"],
                input: vec![("message", Value::from("User login with password=secret123"))],
                expected: vec![("message", "User login with password=REDACTED")],
            },
            TestCase {
                name: "email redaction",
                fields: vec!["message"],
                input: vec![(
                    "message",
                    Value::from("Contact user@example.com for details"),
                )],
                expected: vec![("message", "Contact EMAIL_REDACTED for details")],
            },
            TestCase {
                name: "aws key redaction",
                fields: vec!["message"],
                input: vec![("message", Value::from("AWS Key: AKIATESTKEY1234567"))],
                expected: vec![("message", "AWS Key: AWS_KEY_REDACTED")],
            },
            TestCase {
                name: "multiple patterns in single field",
                fields: vec!["message"],
                input: vec![(
                    "message",
                    Value::from(
                        "User admin@site.com logged in with password=pass123 using AKIATESTKEY1234567",
                    ),
                )],
                expected: vec![(
                    "message",
                    "User EMAIL_REDACTED logged in with password=REDACTED using AWS_KEY_REDACTED",
                )],
            },
            TestCase {
                name: "multiple fields redaction",
                fields: vec!["message", "user_info"],
                input: vec![
                    ("message", Value::from("Login with password=secret")),
                    ("user_info", Value::from("Email: test@example.com")),
                ],
                expected: vec![
                    ("message", "Login with password=REDACTED"),
                    ("user_info", "Email: EMAIL_REDACTED"),
                ],
            },
            TestCase {
                name: "no match leaves unchanged",
                fields: vec!["message"],
                input: vec![("message", Value::from("This is a normal log message"))],
                expected: vec![("message", "This is a normal log message")],
            },
            TestCase {
                name: "only specified fields are redacted",
                fields: vec!["message"],
                input: vec![
                    ("message", Value::from("password=secret")),
                    ("other_field", Value::from("password=shouldnotredact")),
                ],
                expected: vec![
                    ("message", "password=REDACTED"),
                    ("other_field", "password=shouldnotredact"),
                ],
            },
            TestCase {
                name: "missing field doesn't error",
                fields: vec!["nonexistent"],
                input: vec![("message", Value::from("password=secret"))],
                expected: vec![("message", "password=secret")],
            },
        ];

        let rules_file = create_test_rules_file();

        for test_case in test_cases {
            let config = create_config(test_case.fields, &rules_file);

            let mut transform = Redact::new(&config).unwrap_or_else(|e| {
                panic!(
                    "Test '{}': Failed to create transform: {}",
                    test_case.name, e
                )
            });

            let mut log = LogEvent::default();
            for (key, value) in test_case.input {
                log.insert(key, value);
            }

            let event = Event::Log(log);
            let result = transform_one(&mut transform, event)
                .unwrap_or_else(|| panic!("Test '{}': Transform returned None", test_case.name));

            let result_log = result.as_log();
            for (field, expected_value) in test_case.expected {
                let actual = result_log
                    .get(field)
                    .unwrap_or_else(|| {
                        panic!("Test '{}': Field '{}' not found", test_case.name, field)
                    })
                    .as_bytes()
                    .unwrap_or_else(|| {
                        panic!("Test '{}': Field '{}' is not bytes", test_case.name, field)
                    });
                assert_eq!(
                    String::from_utf8_lossy(actual),
                    expected_value,
                    "Test '{}': Field '{}' mismatch",
                    test_case.name,
                    field
                );
            }
        }
    }

    #[test]
    fn test_redact_nested_structures() {
        let rules_file = create_test_rules_file();

        // Test nested object
        {
            let config = create_config(vec!["nested"], &rules_file);

            let mut transform = Redact::new(&config).expect("Failed to create transform");

            let mut log = LogEvent::default();
            let nested_map = vector_lib::btreemap! {
                "credentials" => Value::from("password=secret123"),
                "email" => Value::from("user@example.com"),
                "safe" => Value::from("normal text"),
            };
            log.insert("nested", Value::Object(nested_map));

            let event = Event::Log(log);
            let result = transform_one(&mut transform, event).expect("Transform failed");

            let result_log = result.as_log();
            let nested = result_log.get("nested").unwrap().as_object().unwrap();

            assert_eq!(
                String::from_utf8_lossy(nested.get("credentials").unwrap().as_bytes().unwrap()),
                "password=REDACTED"
            );
            assert_eq!(
                String::from_utf8_lossy(nested.get("email").unwrap().as_bytes().unwrap()),
                "EMAIL_REDACTED"
            );
            assert_eq!(
                String::from_utf8_lossy(nested.get("safe").unwrap().as_bytes().unwrap()),
                "normal text"
            );
        }

        // Test array
        {
            let config = create_config(vec!["items"], &rules_file);

            let mut transform = Redact::new(&config).expect("Failed to create transform");

            let mut log = LogEvent::default();
            log.insert(
                "items",
                vec![
                    Value::from("password=secret1"),
                    Value::from("user@example.com"),
                    Value::from("normal text"),
                ],
            );

            let event = Event::Log(log);
            let result = transform_one(&mut transform, event).expect("Transform failed");

            let result_log = result.as_log();
            let items = result_log.get("items").unwrap().as_array().unwrap();

            assert_eq!(
                String::from_utf8_lossy(items[0].as_bytes().unwrap()),
                "password=REDACTED"
            );
            assert_eq!(
                String::from_utf8_lossy(items[1].as_bytes().unwrap()),
                "EMAIL_REDACTED"
            );
            assert_eq!(
                String::from_utf8_lossy(items[2].as_bytes().unwrap()),
                "normal text"
            );
        }

        // Test deeply nested structure
        {
            let config = create_config(vec!["data"], &rules_file);

            let mut transform = Redact::new(&config).expect("Failed to create transform");

            let mut log = LogEvent::default();
            let deep_nested = vector_lib::btreemap! {
                "level1" => Value::Object(vector_lib::btreemap! {
                    "level2" => Value::Object(vector_lib::btreemap! {
                        "credentials" => Value::from("password=deep"),
                        "contact" => Value::from("admin@test.com"),
                    }),
                }),
            };
            log.insert("data", Value::Object(deep_nested));

            let event = Event::Log(log);
            let result = transform_one(&mut transform, event).expect("Transform failed");

            let result_log = result.as_log();
            let data = result_log.get("data").unwrap().as_object().unwrap();
            let level1 = data.get("level1").unwrap().as_object().unwrap();
            let level2 = level1.get("level2").unwrap().as_object().unwrap();

            assert_eq!(
                String::from_utf8_lossy(level2.get("credentials").unwrap().as_bytes().unwrap()),
                "password=REDACTED"
            );
            assert_eq!(
                String::from_utf8_lossy(level2.get("contact").unwrap().as_bytes().unwrap()),
                "EMAIL_REDACTED"
            );
        }

        // Test mixed array with nested objects
        {
            let config = create_config(vec!["mixed"], &rules_file);

            let mut transform = Redact::new(&config).expect("Failed to create transform");

            let mut log = LogEvent::default();
            log.insert(
                "mixed",
                vec![
                    Value::from("password=array_secret"),
                    Value::Object(vector_lib::btreemap! {
                        "inner" => Value::from("contact@example.com"),
                    }),
                ],
            );

            let event = Event::Log(log);
            let result = transform_one(&mut transform, event).expect("Transform failed");

            let result_log = result.as_log();
            let mixed = result_log.get("mixed").unwrap().as_array().unwrap();

            assert_eq!(
                String::from_utf8_lossy(mixed[0].as_bytes().unwrap()),
                "password=REDACTED"
            );

            let nested_obj = mixed[1].as_object().unwrap();
            assert_eq!(
                String::from_utf8_lossy(nested_obj.get("inner").unwrap().as_bytes().unwrap()),
                "EMAIL_REDACTED"
            );
        }
    }

    #[tokio::test]
    async fn emits_internal_events() {
        use crate::test_util::components::assert_transform_compliance;
        use crate::transforms::test::create_topology;
        use tokio::sync::mpsc;
        use tokio_stream::wrappers::ReceiverStream;

        assert_transform_compliance(async move {
            let rules_file = create_test_rules_file();
            let config = create_config(vec!["message"], &rules_file);

            let (tx, rx) = mpsc::channel(1);
            let (topology, mut out) = create_topology(ReceiverStream::new(rx), config).await;

            let log = LogEvent::from("User login with password=secret123");
            tx.send(log.into()).await.unwrap();

            _ = out.recv().await;

            drop(tx);
            topology.stop().await;
            assert_eq!(out.recv().await, None);
        })
        .await
    }

    #[test]
    fn test_redact_metrics() {
        use vector_lib::metrics::Controller;

        // Initialize the test metrics controller
        vector_lib::metrics::init_test();

        let rules_file = create_test_rules_file();
        let config = create_config(vec!["message", "user_info"], &rules_file);
        let mut transform = Redact::new(&config).expect("Failed to create transform");

        // Create an event with multiple fields that will be redacted
        let mut log = LogEvent::default();
        log.insert("message", Value::from("User login with password=secret123"));
        log.insert("user_info", Value::from("Email: test@example.com"));
        log.insert(
            "unprocessed",
            Value::from("This field is not configured for redaction"),
        );

        let event = Event::Log(log);
        let result = transform_one(&mut transform, event).expect("Transform failed");

        // Verify the event was transformed correctly
        let result_log = result.as_log();
        assert_eq!(
            String::from_utf8_lossy(result_log.get("message").unwrap().as_bytes().unwrap()),
            "User login with password=REDACTED"
        );
        assert_eq!(
            String::from_utf8_lossy(result_log.get("user_info").unwrap().as_bytes().unwrap()),
            "Email: EMAIL_REDACTED"
        );

        // Capture metrics
        let controller = Controller::get().expect("no controller");
        let metrics = controller.capture_metrics();

        // Check that patterns_matched_total was incremented
        let password_pattern_matched = metrics
            .iter()
            .find(|m| m.name() == "redaction_patterns_matched_total");
        assert!(
            password_pattern_matched.is_some(),
            "redaction patterns should have matched"
        );
    }

    #[test]
    fn test_redact_no_match_metrics() {
        use vector_lib::metrics::Controller;

        // Initialize the test metrics controller
        vector_lib::metrics::init_test();

        let rules_file = create_test_rules_file();
        let config = create_config(vec!["message"], &rules_file);
        let mut transform = Redact::new(&config).expect("Failed to create transform");

        // Create an event that won't match any patterns
        let mut log = LogEvent::default();
        log.insert("message", Value::from("This is a normal log message"));

        let event = Event::Log(log);
        let result = transform_one(&mut transform, event).expect("Transform failed");

        // Verify the event was not modified
        let result_log = result.as_log();
        assert_eq!(
            String::from_utf8_lossy(result_log.get("message").unwrap().as_bytes().unwrap()),
            "This is a normal log message"
        );

        // Capture metrics
        let controller = Controller::get().expect("no controller");
        let metrics = controller.capture_metrics();

        // Check that patterns_matched_total was NOT incremented (no redactions occurred)
        let any_pattern_matched = metrics
            .iter()
            .find(|m| m.name() == "redaction_patterns_matched_total");
        assert!(any_pattern_matched.is_none(), "no patterns should match");
    }

    #[test]
    fn test_redact_duration_metric() {
        use vector_lib::metrics::Controller;

        // Initialize the test metrics controller
        vector_lib::metrics::init_test();

        let rules_file = create_test_rules_file();
        let config = create_config(vec!["message"], &rules_file);
        let mut transform = Redact::new(&config).expect("Failed to create transform");

        // Create an event
        let mut log = LogEvent::default();
        log.insert("message", Value::from("User login with password=secret123"));

        let event = Event::Log(log);
        let _result = transform_one(&mut transform, event).expect("Transform failed");

        // Capture metrics
        let controller = Controller::get().expect("no controller");
        let metrics = controller.capture_metrics();

        // Check that duration metric was recorded
        let duration_metric = metrics
            .iter()
            .find(|m| m.name() == "redaction_duration_milliseconds");
        assert!(
            duration_metric.is_some(),
            "redaction_duration_milliseconds should be recorded"
        );
    }
}
