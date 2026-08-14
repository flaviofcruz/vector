//! Shared-corpus parity test.
//!
//! Drives every case in the vendored `cases.json` through [`super::match_shape`] and asserts the
//! Rust result equals the expected value. This is the same corpus the Java, Scala, and Python
//! suites run, so a green run here proves the Rust port is behavior-identical to the reference.

use serde_json::Value;

// Access DataShape from the proto bindings module (private to lib.rs, but accessible from submodules)
use crate::proto_bindings::compliance::DataShape;
use super::{is_allowed_shape, match_shape};

const CASES_JSON: &str = include_str!("vendored/cases.json");

#[test]
fn corpus_parity() {
    let root: Value = serde_json::from_str(CASES_JSON).expect("cases.json must be valid JSON");
    let cases = root
        .get("cases")
        .and_then(Value::as_object)
        .expect("cases.json must have a top-level `cases` object");

    let mut checked = 0usize;
    let mut failures: Vec<String> = Vec::new();

    for (shape_name, entries) in cases {
        let shape = match DataShape::from_str_name(shape_name) {
            Some(s) => s,
            None => panic!("cases.json references unknown DataShape name: {shape_name}"),
        };
        for entry in entries
            .as_array()
            .expect("each shape maps to an array of cases")
        {
            let expected = entry
                .get("expected")
                .and_then(Value::as_bool)
                .expect("case has a bool `expected`");
            let input = match entry.get("input") {
                Some(Value::String(s)) => s,
                // A JSON `null` input encodes the Java "null value" case. Rust's `&str` cannot be
                // null, so the null branch is unrepresentable in this API — assert the corpus
                // agrees it is never accepted, then skip (there is nothing to pass to match_shape).
                Some(Value::Null) => {
                    assert!(!expected, "{shape_name}: null input unexpectedly expects true");
                    continue;
                }
                other => panic!("{shape_name}: unexpected `input` type: {other:?}"),
            };
            let got = match_shape(shape, input);
            checked += 1;
            if got != expected {
                failures
                    .push(format!("{shape_name}: input {input:?} expected {expected} got {got}"));
            }
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {checked} corpus cases diverged from the reference:\n{}",
        failures.len(),
        failures.join("\n")
    );
    assert!(checked > 1000, "corpus unexpectedly small ({checked} cases)");
}

/// Union semantics: a value matching any listed shape is allowed; empty list allows nothing.
#[test]
fn is_allowed_shape_union() {
    let uuid = DataShape::Uuid as i32;
    let numeric = DataShape::Numeric as i32;
    assert!(is_allowed_shape(&[uuid, numeric], "12345"));
    assert!(is_allowed_shape(&[uuid, numeric], "550e8400-e29b-41d4-a716-446655440000"));
    assert!(!is_allowed_shape(&[uuid, numeric], "not a shape"));
    assert!(!is_allowed_shape(&[], "12345"));
}

/// Unspecified and unknown enum numbers never match (fail closed).
#[test]
fn unspecified_and_unknown_fail_closed() {
    assert!(!match_shape(DataShape::Unspecified, "anything"));
    assert!(!super::match_shape_i32(999_999, "anything"));
}
