use metrics::{counter, histogram};
use vector_lib::internal_event::{InternalEvent, NamedInternalEvent};

/// Emitted when a redaction pattern matches and performs a redaction.
/// Tracks which pattern matched and how many times.
///
/// `count` represents the total number of times this specific redaction rule
/// matched and redacted content. It can be counted multiple times for the same rule
/// in the same log line / field value. It is meant to give an idea of how often a rule
/// is matched and redacted.
#[derive(Debug)]
pub struct RedactionPatternMatched<'a> {
    pub rule_id: &'a str,
    pub count: u64,
}

impl NamedInternalEvent for RedactionPatternMatched<'_> {
    fn name(&self) -> &'static str {
        "RedactionPatternMatched"
    }
}

impl InternalEvent for RedactionPatternMatched<'_> {
    fn emit(self) {
        if self.count > 0 {
            debug!(
                message = "Redaction pattern matched",
                rule_id = %self.rule_id,
                count = self.count,
                internal_log_rate_limit = true
            );
            counter!(
                "redaction_patterns_matched_total",
                "rule_id" => self.rule_id.to_string()
            )
            .increment(self.count);
        }
    }
}

/// Emitted to track the duration of redaction operations.
/// Records the time taken to scan and redact in milliseconds.
/// This is emitted once per log line (input to the "redact" transform).
#[derive(Debug)]
pub struct RedactionDuration {
    pub duration_millis: f64,
}

impl NamedInternalEvent for RedactionDuration {
    fn name(&self) -> &'static str {
        "RedactionDuration"
    }
}

impl InternalEvent for RedactionDuration {
    fn emit(self) {
        histogram!("redaction_duration_milliseconds").record(self.duration_millis);
    }
}
