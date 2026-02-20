//! This module is largely copied from:
//! https://github.com/databricks-eng/universe/tree/master/common/logging/redactor/filter/src/lib.rs
//!
//! We only make changes to remove dead code (like emitting of metrics of rule matched) from the
//! original source to keep the codebase clean. Refer to the original source for the full implementation
//! and explaination of the logic.

use std::collections::HashMap;
use std::mem::swap;

use md5::{Digest, Md5};
use regex::bytes::{Captures, Regex, RegexSet, RegexSetBuilder};
use regex_automata::util::interpolate;

use super::buffer::RedactionBuffer;
use super::entropy::entropy;

/// Map of rule indices to the number of times each rule matched.
pub type RuleMatchCounts = HashMap<usize, usize>;

#[derive(Clone)]
pub struct GroupReplaceInfo {
    pub(super) index: usize,
    pub(super) replacement: Box<[u8]>,
    pub(super) hash_prefix: usize,
}

#[derive(Clone)]
pub struct MatchValidation {
    pub(super) group: usize,
    pub(super) min_entropy: Option<f64>,
    pub(super) deny_pattern: Option<Regex>,
}

#[derive(Clone)]
pub struct RedactionPattern {
    pub(super) id: String,
    pub(super) pattern: Regex,
    pub(super) precondition: Regex,
    replacement: Box<[u8]>,
    has_expansion: bool,
    replace_group_info: Option<GroupReplaceInfo>,
    validation: Option<MatchValidation>,
}

impl RedactionPattern {
    pub fn new(
        id: String,
        pattern: Regex,
        precondition: Regex,
        replacement: Box<[u8]>,
        replace_group_info: Option<GroupReplaceInfo>,
        validation: Option<MatchValidation>,
    ) -> Self {
        let has_expansion = replacement.contains(&b'$')
            || validation.as_ref().map(|v| v.group != 0).unwrap_or(false);
        Self {
            id,
            pattern,
            precondition,
            replacement,
            has_expansion,
            replace_group_info,
            validation,
        }
    }

    /// Redacts the input buffer, appending the redacted bytes to the end of the provided `Vec`.
    /// The return value is the index in the output of the last place we redacted, and 0 if we didn't end up redacting anywhere.
    pub fn redact_into(&self, input: &[u8], output: &mut Vec<u8>) -> usize {
        let mut last_match = 0; // The end of the last match
        let mut last_redact = 0; // The end of the last place we redacted

        if !self.has_expansion {
            for m in self.pattern.find_iter(input) {
                output.extend_from_slice(&input[last_match..m.start()]);
                let replacement_len = if self.should_redact_match(m.as_bytes()) {
                    debug!(message = "Redacting match", rule = %self.id, pattern = %self.pattern.as_str());
                    output.extend_from_slice(&self.replacement);
                    self.replacement.len()
                } else {
                    output.extend_from_slice(m.as_bytes());
                    m.len()
                };
                last_match = m.end();
                last_redact = m.start() + replacement_len;
            }
        } else {
            for caps in self.pattern.captures_iter(input) {
                // NOTE: caps.get(0) will always return something, because it is the entire match,
                // and there is always at least the entire match.
                let m = caps.get(0).unwrap();
                output.extend_from_slice(&input[last_match..m.start()]);

                let check_group = self
                    .validation
                    .as_ref()
                    .and_then(|v| caps.get(v.group))
                    .unwrap_or(m);

                let replacement_len = if self.should_redact_match(check_group.as_bytes()) {
                    debug!(message = "Redacting match with groups", rule = %self.id, pattern = %self.pattern.as_str());
                    self.replace_into(caps, output)
                } else {
                    output.extend_from_slice(m.as_bytes());
                    m.len()
                };
                last_match = m.end();
                last_redact = m.start() + replacement_len;
            }
        }

        output.extend(&input[last_match..]);
        last_redact
    }

    fn should_redact_match(&self, input: &[u8]) -> bool {
        if let Some(validation) = &self.validation {
            if let Some(min_entropy) = validation.min_entropy {
                if entropy(input) < min_entropy {
                    return false;
                }
            }

            if let Some(deny_pattern) = validation.deny_pattern.as_ref() {
                if deny_pattern.is_match(input) {
                    return false;
                }
            }
        }

        true
    }

    /// Replace a match (represented by a set of captures) into a destination
    /// vector, returning the length of the replacement that was written.
    fn replace_into(&self, caps: Captures<'_>, dst: &mut Vec<u8>) -> usize {
        let start_length = dst.len();
        interpolate::bytes(
            &self.replacement,
            |index, dst| {
                let span = match caps.get(index) {
                    None => return,
                    Some(span) => span,
                };

                // If we're hashing a replacement and this is the group then make it happen.
                if let Some(info) = &self.replace_group_info {
                    if info.index == index {
                        dst.extend_from_slice(&info.replacement);
                        dst.push(b'(');
                        let hash = format!("{:x}", Md5::digest(span.as_bytes()));
                        dst.extend_from_slice(&hash.as_bytes()[..info.hash_prefix]);
                        dst.push(b')');
                        return;
                    }
                }

                // Otherwise just replace.
                dst.extend_from_slice(span.as_bytes());
            },
            |_| None,
            dst,
        );
        dst.len() - start_length
    }
}

/// A Redactor is a single string that encompasses a whole set of rules. The
/// rules are all compiled together into a single RegexSet that can be used
/// to efficiently determine whether or not *any* redaction needs to be done,
/// and if so, which rules we need to run.
#[derive(Clone)]
pub struct Redactor {
    redactions: Vec<RedactionPattern>,
    all_patterns: RegexSet,
}

impl Redactor {
    pub fn from_patterns<I: IntoIterator<Item = RedactionPattern>>(patterns: I) -> Self {
        let redactions: Vec<RedactionPattern> = patterns.into_iter().collect();
        let rp: Vec<_> = redactions.iter().map(|r| r.precondition.as_str()).collect();
        let all_patterns = RegexSetBuilder::new(&rp)
            .dfa_size_limit(16 * (1 << 20))
            .unicode(false)
            .build()
            .expect("Unable to build regex set");

        Redactor {
            redactions,
            all_patterns,
        }
    }

    /// Get the rule name by index
    pub fn get_rule_name(&self, index: usize) -> Option<&str> {
        self.redactions.get(index).map(|r| r.id.as_str())
    }

    /// Redacts the input chunk and returns the redacted output along with the
    /// index just past the last modification. If the returned index is zero,
    /// it means that there was no redaction, and the same chunk is returned.
    /// Matched rule indices and counts are accumulated into the provided HashMap.
    pub fn redact<'a, 'b>(
        &'a self,
        buffer: &'a mut RedactionBuffer,
        chunk: &'b [u8],
        matched_rules: &mut RuleMatchCounts,
    ) -> (&'b [u8], usize)
    where
        'a: 'b,
    {
        let any_matches = self.all_patterns.is_match(chunk);
        if !any_matches {
            return (chunk, 0);
        }

        let matches = self.all_patterns.matches(chunk);
        if !matches.matched_any() {
            // Easy case: nothing to redact, pass it straight through.
            (chunk, 0)
        } else {
            // Harder case: each match here is something we might want to
            // redact. We need to redact them all *individually*, and because
            // matches might span regular expression boundaries, we really
            // have no choice but to buffer.
            let (mut input, mut output) = buffer.get_fresh();
            input.extend(chunk);

            let mut length_redacted = 0;

            for index in matches {
                output.clear();
                let redacted_len = self.redactions[index].redact_into(input, output);
                if redacted_len > 0 {
                    *matched_rules.entry(index).or_insert(0) += 1;
                }
                length_redacted = redacted_len.max(length_redacted);
                swap(&mut input, &mut output);
            }

            (input.as_slice(), length_redacted)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_pattern(pattern: &str, replacement: &str) -> RedactionPattern {
        RedactionPattern::new(
            "test".into(),
            Regex::new(pattern).expect("Unable to make regex for pattern"),
            Regex::new(pattern).expect("Unable to make regex for precondition"),
            replacement.as_bytes().into(),
            None,
            None,
        )
    }

    fn create_named_test_pattern(name: &str, pattern: &str, replacement: &str) -> RedactionPattern {
        RedactionPattern::new(
            name.into(),
            Regex::new(pattern).expect("Unable to make regex for pattern"),
            Regex::new(pattern).expect("Unable to make regex for precondition"),
            replacement.as_bytes().into(),
            None,
            None,
        )
    }

    #[test]
    fn test_redaction_to_shorter() {
        let pattern = create_test_pattern("zz", "a");

        let mut output = Vec::new();
        let index = pattern.redact_into(b"xzzy", &mut output);

        assert_eq!(output[index], b'y', "Character after redaction mismatch!");
        assert_eq!(
            output[index - 1],
            b'a',
            "Last redaction character mismatch!"
        );
    }

    #[test]
    fn test_redaction_to_longer() {
        let pattern = create_test_pattern("zz", "aaa");

        let mut output = Vec::new();
        let index = pattern.redact_into(b"xzzy", &mut output);

        assert_eq!(output[index], b'y', "Character after redaction mismatch!");
        assert_eq!(
            output[index - 1],
            b'a',
            "Last redaction character mismatch!"
        );
    }

    #[test]
    fn test_redaction_to_longer_replacement() {
        let pattern = create_test_pattern("(zz)", "${1}a");

        let mut output = Vec::new();
        let index = pattern.redact_into(b"xzzy", &mut output);

        assert_eq!(output[index], b'y', "Character after redaction mismatch!");
        assert_eq!(
            output[index - 1],
            b'a',
            "Last redaction character mismatch!"
        );
    }

    #[test]
    fn test_redact_one_another() {
        let patterns = vec![
            create_named_test_pattern("abc_rule", "abc", "REDACTED_ABC"),
            create_named_test_pattern("xyz_rule", "xyz", "REDACTED_XYZ"),
        ];

        let redactor = Redactor::from_patterns(patterns);
        let mut buffer = RedactionBuffer::default();

        let mut rule_counts = RuleMatchCounts::new();
        let (redacted, index) = redactor.redact(&mut buffer, b"xyzzy", &mut rule_counts);
        assert_eq!(index, 12);
        assert_eq!(redacted, b"REDACTED_XYZzy");
        assert_eq!(rule_counts.get(&1).unwrap(), &1); // xyz_rule is at index 1

        let mut rule_counts = RuleMatchCounts::new();
        let (redacted, index) = redactor.redact(&mut buffer, b"zyxyz", &mut rule_counts);
        assert_eq!(index, 14);
        assert_eq!(redacted, b"zyREDACTED_XYZ");
        assert_eq!(rule_counts.get(&1).unwrap(), &1); // xyz_rule is at index 1

        let mut rule_counts = RuleMatchCounts::new();
        let (redacted, index) = redactor.redact(&mut buffer, b"xyzabc", &mut rule_counts);
        assert_ne!(index, 0);
        assert_eq!(redacted, b"REDACTED_XYZREDACTED_ABC");
        assert!(rule_counts.get(&0).unwrap() > &0); // abc_rule is at index 0
        assert!(rule_counts.get(&1).unwrap() > &0); // xyz_rule is at index 1
    }

    #[test]
    fn test_redact_entropy_only_on_specified_parts() {
        // This is based off the AWS secret access key rule that we have in the standard rules.
        let mut pattern = create_test_pattern(
            "([^A-Za-z0-9/+_$%.!\\-]|^)((?:[A-Za-z0-9/+]|%2F|%2B|%252F|%252B){40})([^A-Za-z0-9/+]|$)",
            "${1}REDACTED_POSSIBLE_SECRET_ACCESS_KEY${3}",
        );

        pattern.validation = Some(MatchValidation {
            min_entropy: Some(4.3),
            group: 0,
            deny_pattern: None,
        });

        // Completely artificial log line
        let input = r##"2024/12/31 20:37:14 UTC 2024/12/31 20:35:49.873 WARN com.databricks.clien-scal_pool_controller.watcher.NephosInfoServerless: "regionStatus to defind one -> Map(x86 -> 9), split untu20-gen2-aksdblet-db-k8s-imageLabel":"14.388 INFO DriverRunTermined from-dbsql.syncher
        at scalidatabricks.client.oauth2/tokenProvider" } sparkVersPoller-40f0-a95 specVersist(PoolSize mapping sets matcher.Manager.billingUsagerK8sConf[thread=Batcherdefault. PoolId(K8sCluster_ui_uri: "kube-eventTimestId=aioa-c64517)
                at scalidatabricks.clientCredInfo:  and
accountu20-gen2-aksdblet-db-k8s-imageLogger[worker_nodePoolId=InterBillingUsagerK8sResourceValidateOpt 1"##;

        {
            let mut output = Vec::new();
            pattern.redact_into(input.as_bytes(), &mut output);

            let out_str = String::from_utf8(output).expect("Output wasn't utf8!");
            assert_ne!(out_str, input, "Should redact with misconfigured rule");
        }

        pattern.validation.as_mut().unwrap().group = 2;

        {
            let mut output = Vec::new();
            pattern.redact_into(input.as_bytes(), &mut output);

            let out_str = String::from_utf8(output).expect("Output wasn't utf8!");
            assert_eq!(out_str, input, "Should not redact with proper rule");
        }
    }
}
