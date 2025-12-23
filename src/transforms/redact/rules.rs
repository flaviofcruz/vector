//! This module is largely copied from:
//! https://github.com/databricks-eng/universe/tree/master/common/logging/redactor/filter/src/rules.rs
//!
//! The parsing logic is kept identical to ensure compatibility with existing Databricks rules files
//! present in the "universe" repository.

use regex::bytes::{Regex, RegexBuilder};
use serde::Deserialize;
use serde::de::{Deserializer, MapAccess, Visitor};
// Use the ordered TOML parser specifically for redact rules
use toml_ordered as toml;

use crate::transforms::redact::redactor::{GroupReplaceInfo, MatchValidation, RedactionPattern};

/// An error that can occur while loading rules from a TOML file.
#[derive(Clone, Debug)]
pub enum Error {
    Toml(toml::de::Error),
    PatternRegex(String, regex::Error),
    PreconditionRegex(String, regex::Error),
    DenyRegex(String, regex::Error),
    Validation(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Toml(te) => write!(f, "An error occurred parsing the TOML: {te}"),
            Error::PatternRegex(s, e) => write!(
                f,
                "The pattern in rule {s} is not a valid regular expression: {e}"
            ),
            Error::PreconditionRegex(s, e) => write!(
                f,
                "The precondition in rule {s} is not a valid regular expression: {e}"
            ),
            Error::DenyRegex(s, e) => write!(
                f,
                "The deny pattern in rule {s} is not a valid regular expression: {e}"
            ),
            Error::Validation(s) => write!(f, "Validation error: {s}"),
        }
    }
}

impl std::error::Error for Error {}

/// An example for a redaction rule.
/// It is used to validate the redaction rule config at startup.
#[derive(Deserialize, Debug, Clone)]
pub struct Example {
    pub input: String,
    pub output: String,
}

/// A hash replacement for a redaction rule.
/// It is used to replace a group of the pattern with a hash of the group.
#[derive(Deserialize)]
struct HashReplacement {
    group: usize,
    value: String,
    prefix: usize,
}

impl HashReplacement {
    fn to_group_replace_info(&self) -> GroupReplaceInfo {
        GroupReplaceInfo {
            index: self.group,
            replacement: self.value.as_bytes().into(),
            hash_prefix: self.prefix,
        }
    }
}

/// A match validation declaration for a redaction rule.
/// It is used to validate the match of the pattern before it is redacted.
#[derive(Deserialize)]
struct MatchValidationDeclaration {
    group: Option<usize>,
    min_entropy: Option<f64>,
    deny_pattern: Option<String>,
}

impl MatchValidationDeclaration {
    fn to_match_validation(&self, id: &str) -> Result<MatchValidation, Error> {
        let deny_pattern = self
            .deny_pattern
            .as_ref()
            .map(|p| Regex::new(p).map_err(|e| Error::DenyRegex(id.to_string(), e)))
            .transpose()?;

        Ok(MatchValidation {
            group: self.group.unwrap_or(0),
            min_entropy: self.min_entropy,
            deny_pattern,
        })
    }
}

/// Rule declaration contains all the information needed to create,
/// apply and test a redaction rule.
#[derive(Deserialize)]
struct RuleDeclaration {
    pattern: String,
    precondition: Option<String>,
    replacement: String,
    hash_replacement: Option<HashReplacement>,
    validation: Option<MatchValidationDeclaration>,
    #[serde(default)]
    examples: Vec<Example>,
}

impl RuleDeclaration {
    fn to_redaction_pattern(&self, id: &str) -> Result<RedactionPattern, Error> {
        let pattern = RegexBuilder::new(&self.pattern)
            .unicode(false)
            .build()
            .map_err(|e| Error::PatternRegex(id.to_string(), e))?;

        let precondition = self
            .precondition
            .as_ref()
            .map(|p| {
                RegexBuilder::new(p)
                    .unicode(false)
                    .build()
                    .map_err(|e| Error::PreconditionRegex(id.to_string(), e))
            })
            .unwrap_or_else(|| Ok(pattern.clone()))?;

        Ok(RedactionPattern::new(
            id.to_owned(),
            pattern,
            precondition,
            self.replacement.as_bytes().into(),
            self.hash_replacement
                .as_ref()
                .map(|hr| hr.to_group_replace_info()),
            self.validation
                .as_ref()
                .map(|v| v.to_match_validation(id))
                .transpose()?,
        ))
    }

    /// Validate examples for this rule declaration
    fn validate_examples(&self, id: &str, pattern: &RedactionPattern) -> Result<(), String> {
        for (example_idx, example) in self.examples.iter().enumerate() {
            // If input != output, verify precondition and pattern match
            if example.input != example.output {
                if !pattern.precondition.is_match(example.input.as_bytes()) {
                    return Err(format!(
                        "Rule '{}', example {}: precondition pattern '{}' doesn't match input '{}'",
                        id,
                        example_idx,
                        pattern.precondition.as_str(),
                        example.input
                    ));
                }

                if !pattern.pattern.is_match(example.input.as_bytes()) {
                    return Err(format!(
                        "Rule '{}', example {}: pattern '{}' doesn't match input '{}'",
                        id,
                        example_idx,
                        pattern.pattern.as_str(),
                        example.input
                    ));
                }
            }

            // Verify redaction produces expected output
            let mut output = Vec::new();
            pattern.redact_into(example.input.as_bytes(), &mut output);

            let out_str = String::from_utf8(output).map_err(|_| {
                format!(
                    "Rule '{}', example {}: output is not valid UTF-8",
                    id, example_idx
                )
            })?;

            if out_str != example.output {
                return Err(format!(
                    "Rule '{}', example {}: redaction of '{}' produced '{}' but expected '{}'",
                    id, example_idx, example.input, out_str, example.output
                ));
            }

            // Verify idempotency: redacting already-redacted data should produce the same result
            let mut output2 = Vec::new();
            pattern.redact_into(out_str.as_bytes(), &mut output2);
            let out_str2 = String::from_utf8(output2).map_err(|_| {
                format!(
                    "Rule '{}', example {}: second output is not valid UTF-8",
                    id, example_idx
                )
            })?;

            if out_str != out_str2 {
                return Err(format!(
                    "Rule '{}', example {}: redacting already-redacted data produced different result. \
                     This rule is not idempotent! First: '{}', Second: '{}'",
                    id, example_idx, out_str, out_str2
                ));
            }
        }

        Ok(())
    }
}

#[derive(Deserialize)]
struct RulesFile {
    // We preserve the order in which they were added to the file to fully match the
    // behavior of Redactor implementation in the scala project.
    #[serde(deserialize_with = "deserialize_ordered_map")]
    patterns: Vec<(String, RuleDeclaration)>,
}

fn deserialize_ordered_map<'de, D>(
    deserializer: D,
) -> Result<Vec<(String, RuleDeclaration)>, D::Error>
where
    D: Deserializer<'de>,
{
    struct OrderedMapVisitor;

    impl<'de> Visitor<'de> for OrderedMapVisitor {
        type Value = Vec<(String, RuleDeclaration)>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a map of string to RuleDeclaration")
        }

        fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
        where
            M: MapAccess<'de>,
        {
            let mut ordered = Vec::new();
            while let Some((key, value)) = map.next_entry()? {
                ordered.push((key, value));
            }
            Ok(ordered)
        }
    }

    deserializer.deserialize_map(OrderedMapVisitor)
}

impl RulesFile {
    /// Convert to patterns with optional example validation
    fn to_patterns_with_validation(&self, validate: bool) -> Result<Vec<RedactionPattern>, Error> {
        let mut patterns = Vec::new();

        for (id, decl) in &self.patterns {
            let pattern = decl.to_redaction_pattern(id)?;

            // Validate examples if requested
            if validate {
                decl.validate_examples(id, &pattern)
                    .map_err(Error::Validation)?;
                info!(
                    "Rule '{}' passed example validation for {} example(s)",
                    id,
                    decl.examples.len()
                );
            }

            patterns.push(pattern);
        }

        Ok(patterns)
    }
}

/// Parse rules from a TOML string with optional example validation
pub fn parse_rules_toml_with_validation(
    t: &str,
    validate_examples: bool,
) -> Result<Vec<RedactionPattern>, Error> {
    let rules_file = toml::from_str::<RulesFile>(t).map_err(Error::Toml)?;
    rules_file.to_patterns_with_validation(validate_examples)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_rule_order() {
        let contents = r#"
[patterns.z]
pattern='zzzzz'
replacement='redacted'

[patterns.x]
pattern='xxxxx'
replacement='redacted'

[patterns.a]
pattern='aaaaa'
replacement='redacted'

[patterns.j]
pattern='jjjjj'
replacement='redacted'

[patterns.g]
pattern='ggggg'
replacement='redacted'
            "#;
        let rules_file = toml::from_str::<RulesFile>(contents).expect("could not parse test data");
        let rule_names: Vec<_> = rules_file.patterns.into_iter().map(|(k, _)| k).collect();
        assert_eq!(rule_names, vec!["z", "x", "a", "j", "g"]);
    }
}
