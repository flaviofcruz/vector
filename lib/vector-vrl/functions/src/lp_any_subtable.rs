//! The `lp_any_subtable` VRL function: keys a union row's generic `google.protobuf.Any` payload by
//! its sub-table name, as the classic variant union does.
//!
//! Classic (Universe, `log-sync/structured-logs/logfoods-deployments/lumberjack/
//! variant_union_common/notebooks/variant_union_pipeline_lib.py`, `_generic_payload_df` and
//! `_dispatched_payload_col`) decodes a row whose generic Any member is populated through a
//! catalog-generated meta message (`lumberjack.<Table>AnySubtable`). The meta message has one oneof
//! member per catalog sub-table, named by the sub-table and typed by its message. Classic prepends
//! the row's member tag and a length to the Any's `value` and decodes that, so the payload VARIANT
//! is `{"<subtable_name>": {...}}` and nothing else. A row with a typed member keeps the typed arm,
//! keyed by the member's proto field name.
//!
//! This function makes the same rewrite on the wire bytes. When `subtable` (the route from
//! `lp_subtable_name`) is a key of `fields`, the payload container is replaced by one field
//! numbered `fields[subtable]` that holds the Any's `value`. The union descriptor must declare that
//! field in the payload message, named by the sub-table and typed by its message. Typed rows pass
//! through unchanged. A null route returns null: classic's union reads only rows whose
//! `sub_table_name` is a catalog sub-table (`.where(F.col("sub_table_name").isin(...))`), so a row
//! with no route, an Any of unknown type included, is not written.
//!
//! Call it after `apply_redaction!`: it only moves bytes the redaction already produced, and the
//! synthetic field never reaches a redaction plan. Opt-in by construction: the function is only
//! called from a VRL program that names it.

use std::collections::{BTreeMap, BTreeSet};

use crate::lp_wire::{MAX_FIELD_NUMBER, WireField, fields, write_varint};
use vrl::prelude::*;

/// The Any message's `value` field number.
const ANY_VALUE_FIELD: u64 = 2;

#[derive(Clone, Copy, Debug)]
pub struct LpAnySubtable;

impl Function for LpAnySubtable {
    fn identifier(&self) -> &'static str {
        "lp_any_subtable"
    }

    fn summary(&self) -> &'static str {
        "key a union row's generic Any payload by its sub-table name"
    }

    fn usage(&self) -> &'static str {
        indoc! {"
            Rewrites a union record's wire bytes (`message`) so that a generic Any payload is keyed
            by its sub-table. `container` is the field number of the payload message that holds
            the oneof (0 when `message` is that message) and `generic_field` is the Any member's
            field number. When `subtable` is a key of `fields`, the container is replaced by one
            field numbered `fields[subtable]` holding the Any's `value`. Other routes return
            `message` unchanged, and a null `subtable` returns null (classic does not write rows
            without a sub-table).

            Fallible (call as `lp_any_subtable!(...)`): errors on malformed wire bytes, or when
            `subtable` is a generic route but the record has no Any member.
        "}
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[
            Parameter {
                keyword: "message",
                kind: kind::BYTES,
                required: true,
            },
            Parameter {
                keyword: "container",
                kind: kind::INTEGER,
                required: true,
            },
            Parameter {
                keyword: "generic_field",
                kind: kind::INTEGER,
                required: true,
            },
            Parameter {
                keyword: "subtable",
                kind: kind::BYTES | kind::NULL,
                required: true,
            },
            Parameter {
                keyword: "fields",
                kind: kind::OBJECT,
                required: true,
            },
        ]
    }

    fn examples(&self) -> &'static [Example] {
        &[]
    }

    fn compile(
        &self,
        state: &state::TypeState,
        _ctx: &mut FunctionCompileContext,
        arguments: ArgumentList,
    ) -> Compiled {
        let message = arguments.required("message");
        let container = field_number(
            "container",
            arguments.required_literal("container", state)?,
            true,
        )?;
        let generic_field = field_number(
            "generic_field",
            arguments.required_literal("generic_field", state)?,
            false,
        )?;
        let subtable = arguments.required("subtable");
        let fields = subtable_fields(arguments.required_literal("fields", state)?, generic_field)?;
        Ok(LpAnySubtableFn {
            message,
            container,
            generic_field,
            subtable,
            fields,
        }
        .as_expr())
    }
}

fn compile_error(message: String) -> Box<dyn DiagnosticMessage> {
    Box::new(ExpressionError::Error {
        message: format!("lp_any_subtable: {message}"),
        labels: vec![],
        notes: vec![],
    })
}

/// A literal field number. Zero is allowed only when `allow_zero` (the "no container" case).
fn field_number(
    name: &str,
    value: Value,
    allow_zero: bool,
) -> Result<u64, Box<dyn DiagnosticMessage>> {
    let Value::Integer(number) = value else {
        return Err(compile_error(format!("{name} must be an integer literal")));
    };
    u64::try_from(number)
        .ok()
        .filter(|number| *number <= MAX_FIELD_NUMBER && (allow_zero || *number != 0))
        .ok_or_else(|| compile_error(format!("{name} is not a valid field number")))
}

/// The `fields` literal: sub-table name to the field number that carries it. Numbers must be
/// distinct and must not reuse the Any member's number.
fn subtable_fields(
    value: Value,
    generic_field: u64,
) -> Result<BTreeMap<String, u64>, Box<dyn DiagnosticMessage>> {
    let Value::Object(map) = value else {
        return Err(compile_error("fields must be an object literal".into()));
    };
    let mut seen = BTreeSet::new();
    map.into_iter()
        .map(|(name, number)| {
            let name = name.as_str().to_owned();
            if name.is_empty() {
                return Err(compile_error("a sub-table name must not be empty".into()));
            }
            let number = field_number(&format!("fields.{name}"), number, false)?;
            if number == generic_field {
                return Err(compile_error(format!(
                    "fields.{name} reuses the Any member's field number {number}"
                )));
            }
            if !seen.insert(number) {
                return Err(compile_error(format!(
                    "field number {number} is used by more than one sub-table"
                )));
            }
            Ok((name, number))
        })
        .collect()
}

#[derive(Clone, Debug)]
struct LpAnySubtableFn {
    message: Box<dyn Expression>,
    container: u64,
    generic_field: u64,
    subtable: Box<dyn Expression>,
    fields: BTreeMap<String, u64>,
}

impl FunctionExpression for LpAnySubtableFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let message = self.message.resolve(ctx)?;
        let bytes = message.try_bytes()?;
        let subtable = match self.subtable.resolve(ctx)? {
            Value::Null => return Ok(Value::Null),
            value => value.try_bytes()?,
        };
        let subtable = std::str::from_utf8(&subtable)
            .map_err(|_| "lp_any_subtable: subtable is not UTF-8".to_string())?;
        let Some(&number) = self.fields.get(subtable) else {
            return Ok(Value::Bytes(bytes));
        };
        let rewritten = rekey(&bytes, self.container, self.generic_field, number)
            .map_err(|e| format!("lp_any_subtable: {e}"))?;
        Ok(Value::Bytes(rewritten.into()))
    }

    fn type_def(&self, _: &state::TypeState) -> TypeDef {
        TypeDef::bytes().or_null().fallible()
    }
}

/// Replaces the payload container of `buf` (the last field numbered `container`, or `buf` itself
/// when `container` is 0) with one field numbered `number` that holds the Any member's `value`.
/// Other top-level fields of `buf` keep their bytes and order; the rewritten container takes the
/// place of its last occurrence and earlier occurrences are dropped. Several occurrences of the
/// Any member merge as a decode would: the last `value` on the wire wins.
fn rekey(buf: &[u8], container: u64, generic_field: u64, number: u64) -> Result<Vec<u8>, String> {
    if container == 0 {
        return Ok(subtable_field(any_value(buf, generic_field)?, number));
    }
    let outer = fields(buf)?;
    let last = outer
        .iter()
        .rposition(|field| field.number == container)
        .ok_or_else(|| format!("the record has no payload container (field {container})"))?;
    let payload = message_value(&outer[last])?;
    let rewritten = subtable_field(any_value(payload, generic_field)?, number);
    let mut out = Vec::with_capacity(buf.len() + 8);
    for (index, field) in outer.iter().enumerate() {
        if index == last {
            write_len_field(&mut out, container, &rewritten);
        } else if field.number != container {
            out.extend_from_slice(field.encoded);
        }
    }
    Ok(out)
}

/// The `value` of the Any member `generic_field` in `payload`, merged over every occurrence. Empty
/// when the Any carries no `value`, like a decode's default.
fn any_value(payload: &[u8], generic_field: u64) -> Result<&[u8], String> {
    let mut found = false;
    let mut value: &[u8] = &[];
    for field in fields(payload)? {
        if field.number != generic_field {
            continue;
        }
        found = true;
        for any_field in fields(message_value(&field)?)? {
            if any_field.number == ANY_VALUE_FIELD {
                value = message_value(&any_field)?;
            }
        }
    }
    if found {
        Ok(value)
    } else {
        Err(format!(
            "generic sub-table route but the payload has no Any member (field {generic_field})"
        ))
    }
}

/// The payload of a length-delimited field.
fn message_value<'a>(field: &WireField<'a>) -> Result<&'a [u8], String> {
    if field.wire_type == 2 {
        Ok(field.value)
    } else {
        Err(format!(
            "field {} is not length-delimited (wire type {})",
            field.number, field.wire_type
        ))
    }
}

fn subtable_field(value: &[u8], number: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len() + 8);
    write_len_field(&mut out, number, value);
    out
}

fn write_len_field(out: &mut Vec<u8>, number: u64, value: &[u8]) {
    write_varint(out, (number << 3) | 2);
    write_varint(out, u64::try_from(value.len()).unwrap_or(u64::MAX));
    out.extend_from_slice(value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lp_wire::test_fields::{bytes_field, varint_field};
    use vrl::compiler::runtime::Runtime;
    use vrl::compiler::{TargetValue, TimeZone, compile};
    use vrl::value;
    use vrl::value::Secrets;

    /// BackgroundActivityLog's `activity` (the union payload) and its Any member.
    const CONTAINER: u64 = 4;
    const ANY: u64 = 58;
    const OPENSEARCH: u64 = 10_001;

    fn any(type_url: &str, value: &[u8]) -> Vec<u8> {
        let mut out = bytes_field(1, type_url.as_bytes());
        out.extend(bytes_field(ANY_VALUE_FIELD, value));
        out
    }

    /// A subtable message: field 1 (string) and field 2 (varint).
    fn inner() -> Vec<u8> {
        let mut out = bytes_field(1, b"cluster-a");
        out.extend(varint_field(2, 7));
        out
    }

    fn record(activity: &[u8]) -> Vec<u8> {
        let mut out = varint_field(2, 12_345);
        out.extend(bytes_field(CONTAINER, activity));
        out.extend(varint_field(9, 1));
        out
    }

    /// Runs the VRL program against `.message` and `.subtable`, returning the result.
    fn run(message: Vec<u8>, subtable: Value) -> Result<Value, String> {
        let program = indoc! {r#"
            lp_any_subtable!(
                message: .message,
                container: 4,
                generic_field: 58,
                subtable: .subtable,
                fields: {"opensearch_dp_proxy": 10001, "cloud_sub": 10002},
            )
        "#};
        let program = compile(program, &crate::all())
            .map_err(|diagnostics| format!("{diagnostics:?}"))?
            .program;
        let mut target = TargetValue {
            value: value!({"message": (Value::Bytes(message.into())), "subtable": subtable}),
            metadata: value!({}),
            secrets: Secrets::default(),
        };
        Runtime::default()
            .resolve(&mut target, &program, &TimeZone::default())
            .map_err(|error| error.to_string())
    }

    fn compile_error_of(program: &str) -> String {
        match compile(program, &crate::all()) {
            Ok(_) => panic!("expected a compile error for {program}"),
            Err(diagnostics) => format!("{diagnostics:?}"),
        }
    }

    #[test]
    fn typed_member_passes_through_unchanged() {
        let message = record(&bytes_field(41, &inner()));
        let out = run(message.clone(), Value::from("network_event_log")).unwrap();
        assert_eq!(out, Value::Bytes(message.into()));
    }

    #[test]
    fn generic_any_is_keyed_by_the_subtable() {
        let url = "type.googleapis.com/logging.opensearchdpproxy.OpenSearchDpProxyActivityLog";
        let message = record(&bytes_field(ANY, &any(url, &inner())));
        let out = run(message, Value::from("opensearch_dp_proxy")).unwrap();
        // The container now holds only the sub-table member; the other record fields keep their
        // bytes and order.
        let expected = record(&bytes_field(OPENSEARCH, &inner()));
        assert_eq!(out, Value::Bytes(expected.into()));
    }

    #[test]
    fn null_route_returns_null() {
        // An Any whose type has no catalog route: lp_subtable_name returns null.
        let unknown = record(&bytes_field(
            ANY,
            &any("type.googleapis.com/x.Unknown", &inner()),
        ));
        assert_eq!(run(unknown, Value::Null).unwrap(), Value::Null);
        // A record with no oneof member at all.
        assert_eq!(run(record(&[]), Value::Null).unwrap(), Value::Null);
    }

    #[test]
    fn generic_route_without_an_any_member_is_an_error() {
        let message = record(&bytes_field(41, &inner()));
        let error = run(message, Value::from("opensearch_dp_proxy")).unwrap_err();
        assert!(error.contains("no Any member"), "{error}");
    }

    #[test]
    fn generic_route_without_a_container_is_an_error() {
        let error = run(varint_field(2, 1), Value::from("cloud_sub")).unwrap_err();
        assert!(error.contains("no payload container"), "{error}");
    }

    #[test]
    fn malformed_record_is_an_error() {
        assert!(run(vec![0xff; 11], Value::from("cloud_sub")).is_err());
        // The Any member is a varint, not a message.
        let message = record(&varint_field(ANY, 3));
        assert!(run(message, Value::from("cloud_sub")).is_err());
    }

    #[test]
    fn any_without_a_value_becomes_an_empty_member() {
        let message = record(&bytes_field(
            ANY,
            &bytes_field(1, b"type.googleapis.com/x.Sub"),
        ));
        let out = rekey(&message, CONTAINER, ANY, OPENSEARCH).unwrap();
        assert_eq!(out, record(&bytes_field(OPENSEARCH, &[])));
    }

    #[test]
    fn repeated_any_occurrences_merge_to_the_last_value() {
        let mut activity = bytes_field(ANY, &any("type.googleapis.com/x.Sub", b"first"));
        activity.extend(bytes_field(ANY, &bytes_field(ANY_VALUE_FIELD, b"second")));
        let out = rekey(&record(&activity), CONTAINER, ANY, OPENSEARCH).unwrap();
        assert_eq!(out, record(&bytes_field(OPENSEARCH, b"second")));
    }

    #[test]
    fn earlier_container_occurrences_are_dropped() {
        let mut message = bytes_field(CONTAINER, &bytes_field(41, b""));
        message.extend(bytes_field(CONTAINER, &bytes_field(ANY, &any("u", b"v"))));
        let out = rekey(&message, CONTAINER, ANY, OPENSEARCH).unwrap();
        assert_eq!(out, bytes_field(CONTAINER, &bytes_field(OPENSEARCH, b"v")));
    }

    #[test]
    fn container_zero_rewrites_the_whole_message() {
        // The record is the payload message (for example after lp_oneof_payload's nesting).
        let mut message = bytes_field(ANY, &any("type.googleapis.com/x.Sub", &inner()));
        message.extend(varint_field(9, 1));
        assert_eq!(
            rekey(&message, 0, ANY, OPENSEARCH).unwrap(),
            bytes_field(OPENSEARCH, &inner())
        );
    }

    #[test]
    fn invalid_fields_literals_are_compile_errors() {
        let call = |fields: &str| {
            format!(
                "lp_any_subtable!(message: .m, container: 4, generic_field: 58, subtable: .s, \
                 fields: {fields})"
            )
        };
        assert!(compile_error_of(&call(r#"{"a": "x"}"#)).contains("fields.a"));
        assert!(compile_error_of(&call(r#"{"a": 0}"#)).contains("fields.a"));
        assert!(compile_error_of(&call(r#"{"a": 58}"#)).contains("reuses"));
        assert!(compile_error_of(&call(r#"{"a": 10001, "b": 10001}"#)).contains("more than one"));
        assert!(compile_error_of(&call(r#"{"a": 536870912}"#)).contains("fields.a"));
    }

    #[test]
    fn invalid_field_number_literals_are_compile_errors() {
        let error = compile_error_of(
            "lp_any_subtable!(message: .m, container: 4, generic_field: 0, subtable: .s, \
             fields: {})",
        );
        assert!(error.contains("generic_field"), "{error}");
        let error = compile_error_of(
            "lp_any_subtable!(message: .m, container: -1, generic_field: 58, subtable: .s, \
             fields: {})",
        );
        assert!(error.contains("container"), "{error}");
    }
}
