//! The `lp_oneof_payload` VRL function: nests a message's oneof members under one payload field.
//!
//! A Lumberjack Prime union table carries a message's oneof as one `payload` column (a VARIANT,
//! keyed by the populated branch). Its descriptor (`make_self_contained_descriptor
//! --union-payload-from-oneof`) declares `payload` as a wrapper message that holds a copy of each
//! oneof member at the member's own field number. The wire bytes of the parent carry the members as
//! top-level fields, so this function rewrites a parent message into the union's shape: every
//! top-level field whose number is a member is copied, in order, into one length-delimited field
//! `payload`; every other top-level field is dropped, which is what the union table keeps.
//!
//! Opt-in by construction: the function is only called from a VRL program that names it.

use crate::lp_wire::{MAX_FIELD_NUMBER, fields, write_varint};
use vrl::prelude::*;

#[derive(Clone, Copy, Debug)]
pub struct LpOneofPayload;

impl Function for LpOneofPayload {
    fn identifier(&self) -> &'static str {
        "lp_oneof_payload"
    }

    fn summary(&self) -> &'static str {
        "nest a message's oneof member fields under one payload field"
    }

    fn usage(&self) -> &'static str {
        indoc::indoc! {"
            Rewrites a parent message's wire bytes (`message`) into a union envelope's shape: every
            top-level field whose number is in `members` is copied, in order, into one
            length-delimited field numbered `payload`. Other fields are dropped. Returns empty bytes
            when no member is present. Fallible: errors on malformed wire bytes (call as
            `lp_oneof_payload!(...)`).
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
                keyword: "members",
                kind: kind::ARRAY,
                required: true,
            },
            Parameter {
                keyword: "payload",
                kind: kind::INTEGER,
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
        let members = parse_members(arguments.required_literal("members", state)?)?;
        let payload = parse_field_number(arguments.required_literal("payload", state)?)?;
        Ok(LpOneofPayloadFn {
            message,
            members,
            payload,
        }
        .as_expr())
    }
}

fn compile_error(message: String) -> Box<dyn DiagnosticMessage> {
    Box::new(ExpressionError::Error {
        message: format!("lp_oneof_payload: {message}"),
        labels: vec![],
        notes: vec![],
    }) as Box<dyn DiagnosticMessage>
}

fn parse_field_number(value: Value) -> Result<u64, Box<dyn DiagnosticMessage>> {
    match value {
        Value::Integer(number) => u64::try_from(number)
            .ok()
            .filter(|number| (1..=MAX_FIELD_NUMBER).contains(number))
            .ok_or_else(|| compile_error(format!("field number {number} is out of range"))),
        _ => Err(compile_error("field numbers must be integers".into())),
    }
}

fn parse_members(value: Value) -> Result<Vec<u64>, Box<dyn DiagnosticMessage>> {
    let Value::Array(items) = value else {
        return Err(compile_error("members must be an array literal".into()));
    };
    items.into_iter().map(parse_field_number).collect()
}

#[derive(Clone, Debug)]
struct LpOneofPayloadFn {
    message: Box<dyn Expression>,
    members: Vec<u64>,
    payload: u64,
}

impl FunctionExpression for LpOneofPayloadFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let message = self.message.resolve(ctx)?;
        let bytes = message.try_bytes()?;
        let rewritten = rewrite(&bytes, &self.members, self.payload)
            .map_err(|e| format!("lp_oneof_payload: {e}"))?;
        Ok(Value::Bytes(rewritten.into()))
    }

    fn type_def(&self, _: &state::TypeState) -> TypeDef {
        TypeDef::bytes().fallible()
    }
}

/// Rewrites `buf`: top-level fields numbered in `members` are copied in order, raw, into one
/// length-delimited field `payload`; every other top-level field is dropped.
fn rewrite(buf: &[u8], members: &[u64], payload: u64) -> Result<Vec<u8>, String> {
    let mut nested = Vec::new();
    for field in fields(buf)? {
        if members.contains(&field.number) {
            nested.extend_from_slice(field.encoded);
        }
    }
    if nested.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    write_varint(&mut out, (payload << 3) | 2);
    write_varint(&mut out, u64::try_from(nested.len()).unwrap_or(u64::MAX));
    out.extend_from_slice(&nested);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lp_wire::read_varint;
    use crate::lp_wire::test_fields::{bytes_field, varint_field};

    fn concat(parts: &[Vec<u8>]) -> Vec<u8> {
        parts.concat()
    }

    #[test]
    fn members_are_nested_in_order_and_others_dropped() {
        let input = concat(&[
            bytes_field(1074, b"first"),
            varint_field(1, 7),
            bytes_field(1031, b"second"),
        ]);
        let out = rewrite(&input, &[1074, 1031], 1).unwrap();
        let nested = concat(&[bytes_field(1074, b"first"), bytes_field(1031, b"second")]);
        assert_eq!(out, bytes_field(1, &nested));
    }

    #[test]
    fn repeated_member_occurrences_are_all_kept_in_order() {
        let input = concat(&[bytes_field(1074, b"a"), bytes_field(1074, b"b")]);
        let out = rewrite(&input, &[1074], 1).unwrap();
        let expected = concat(&[bytes_field(1074, b"a"), bytes_field(1074, b"b")]);
        assert_eq!(out, bytes_field(1, &expected));
    }

    #[test]
    fn no_member_present_routes_to_empty_bytes() {
        let input = concat(&[varint_field(1, 7), bytes_field(2, b"x")]);
        assert_eq!(rewrite(&input, &[1074], 1).unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn empty_message_routes_to_empty_bytes() {
        assert_eq!(rewrite(&[], &[1074], 1).unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn fixed_width_fields_are_skipped_whole() {
        let mut fixed64 = Vec::new();
        write_varint(&mut fixed64, (3 << 3) | 1);
        fixed64.extend_from_slice(&[0u8; 8]);
        let mut fixed32 = Vec::new();
        write_varint(&mut fixed32, (1074 << 3) | 5);
        fixed32.extend_from_slice(&[9u8; 4]);
        let out = rewrite(&concat(&[fixed64, fixed32]), &[1074], 1).unwrap();
        assert!(!out.is_empty());
        assert_eq!(out[0], (1 << 3) | 2);
    }

    #[test]
    fn truncated_field_is_an_error() {
        let mut input = Vec::new();
        write_varint(&mut input, (1074 << 3) | 2);
        write_varint(&mut input, 10);
        input.extend_from_slice(b"short");
        assert!(rewrite(&input, &[1074], 1).is_err());
    }

    #[test]
    fn group_wire_types_are_rejected() {
        let mut input = Vec::new();
        write_varint(&mut input, (1074 << 3) | 3);
        assert!(rewrite(&input, &[1074], 1).is_err());
    }

    #[test]
    fn varint_round_trips_across_widths() {
        for value in [0u64, 1, 127, 128, 16_384, u64::MAX] {
            let mut buf = Vec::new();
            write_varint(&mut buf, value);
            let mut pos = 0;
            assert_eq!(read_varint(&buf, &mut pos).unwrap(), value);
            assert_eq!(pos, buf.len());
        }
    }
}
