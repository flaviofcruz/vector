//! The `lp_registry_variant` VRL function: moves a registry-only `service_extra_v2` payload into a
//! synthetic `ServiceExtra` member so that `apply_redaction!` can redact it.
//!
//! `ServiceHealthEvent.service_extra_v2` (field 16) is opaque `bytes`. Its proto type is only known
//! from `event_name` (field 1) through the event registry, and the redaction executor cannot see
//! inside it. Classic decodes it after redaction (`ServiceExtraDecoder.decodeFromParsed`), so for
//! LP the payload must become a typed field the executor redacts. This function rewrites the wire
//! bytes before redaction:
//!
//! - When `event_name` is in `variant_fields` and field 16 is non-empty, the ServiceExtra
//!   (field 7) is replaced by `ServiceExtra { <synthetic field N> = <field 16 bytes> }`. The
//!   synthetic member exists only in the augmented descriptor (Universe side), so the redaction
//!   plan covers its data labels. Field 7 is replaced because classic gives V2 precedence over V1.
//! - Field 16 is always removed from the output, so raw bytes never reach the sink, including for
//!   events that are not in `variant_fields`.
//! - Everything else is copied byte for byte.
//!
//! The function is only called from a VRL program that names it, so it is inert otherwise.

use vrl::prelude::*;
use vrl::value::ObjectMap;

const EVENT_NAME_FIELD: u64 = 1;
const SERVICE_EXTRA_FIELD: u64 = 7;
const SERVICE_EXTRA_V2_FIELD: u64 = 16;
/// Synthetic members must sit above the v1 ServiceExtra oneof (highest assigned number 9999).
const FIRST_SYNTHETIC_FIELD: u64 = 10_000;
const MAX_FIELD_NUMBER: u64 = (1 << 29) - 1;

#[derive(Clone, Copy, Debug)]
pub struct LpRegistryVariant;

/// Event name to synthetic ServiceExtra member field number, fixed at compile time.
#[derive(Clone, Debug)]
struct VariantFields(std::collections::BTreeMap<Vec<u8>, u64>);

impl Function for LpRegistryVariant {
    fn identifier(&self) -> &'static str {
        "lp_registry_variant"
    }

    fn summary(&self) -> &'static str {
        "move a registry-only service_extra_v2 payload into a redactable ServiceExtra member"
    }

    fn usage(&self) -> &'static str {
        indoc::indoc! {"
            Rewrites a ServiceHealthEvent's wire bytes (`message`) so that the registry-only
            `service_extra_v2` payload (field 16) becomes a synthetic ServiceExtra member (field 7)
            keyed by the event's `event_name` (field 1) through `variant_fields`, which maps event
            names to the synthetic member's field number (>= 10000). Field 16 is always removed.
            Run this before `apply_redaction!` so the payload is redacted. Fallible: errors on
            malformed wire bytes (call as `lp_registry_variant!(...)`).
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
                keyword: "variant_fields",
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
        let fields = arguments.required_literal("variant_fields", state)?;
        let Value::Object(map) = fields else {
            return Err(Box::new(ExpressionError::Error {
                message: "lp_registry_variant: variant_fields must be an object literal".into(),
                labels: vec![],
                notes: vec![],
            }) as Box<dyn DiagnosticMessage>);
        };
        let variant_fields = parse_variant_fields(&map).map_err(|message| {
            Box::new(ExpressionError::Error {
                message: format!("lp_registry_variant: {message}"),
                labels: vec![],
                notes: vec![],
            }) as Box<dyn DiagnosticMessage>
        })?;
        Ok(LpRegistryVariantFn {
            message,
            variant_fields,
        }
        .as_expr())
    }
}

fn parse_variant_fields(map: &ObjectMap) -> Result<VariantFields, String> {
    let mut out = std::collections::BTreeMap::new();
    for (event_name, value) in map {
        let Value::Integer(number) = value else {
            return Err(format!("field number for {event_name} must be an integer"));
        };
        let number = u64::try_from(*number)
            .ok()
            .filter(|n| (FIRST_SYNTHETIC_FIELD..=MAX_FIELD_NUMBER).contains(n))
            .ok_or_else(|| {
                format!("field number for {event_name} must be in {FIRST_SYNTHETIC_FIELD}..={MAX_FIELD_NUMBER}")
            })?;
        out.insert(event_name.as_bytes().to_vec(), number);
    }
    Ok(VariantFields(out))
}

#[derive(Clone, Debug)]
struct LpRegistryVariantFn {
    message: Box<dyn Expression>,
    variant_fields: VariantFields,
}

impl FunctionExpression for LpRegistryVariantFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let message = self.message.resolve(ctx)?;
        let bytes = message.try_bytes()?;
        let rewritten = rewrite(&bytes, &self.variant_fields.0)
            .map_err(|e| format!("lp_registry_variant: {e}"))?;
        Ok(Value::Bytes(rewritten.into()))
    }

    fn type_def(&self, _: &state::TypeState) -> TypeDef {
        TypeDef::bytes().fallible()
    }
}

/// One top-level protobuf field: its number, wire type, and full encoded span.
struct Span<'a> {
    number: u64,
    wire_type: u64,
    /// Raw encoded bytes of the whole field (tag included).
    raw: &'a [u8],
    /// Payload bytes of a length-delimited field (empty for other wire types).
    payload: &'a [u8],
}

fn read_varint(buf: &[u8], pos: &mut usize) -> Result<u64, String> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *buf.get(*pos).ok_or("truncated varint")?;
        *pos += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err("varint too long".into())
}

fn read_spans(buf: &[u8]) -> Result<Vec<Span<'_>>, String> {
    let mut spans = Vec::new();
    let mut pos = 0usize;
    while pos < buf.len() {
        let start = pos;
        let key = read_varint(buf, &mut pos)?;
        let number = key >> 3;
        let wire_type = key & 7;
        if number == 0 {
            return Err("field number 0".into());
        }
        let mut payload: &[u8] = &[];
        match wire_type {
            0 => {
                read_varint(buf, &mut pos)?;
            }
            1 => pos = pos.checked_add(8).ok_or("overflow")?,
            5 => pos = pos.checked_add(4).ok_or("overflow")?,
            2 => {
                let len = usize::try_from(read_varint(buf, &mut pos)?).map_err(|_| "length")?;
                let end = pos.checked_add(len).ok_or("overflow")?;
                payload = buf
                    .get(pos..end)
                    .ok_or("truncated length-delimited field")?;
                pos = end;
            }
            other => return Err(format!("unsupported wire type {other}")),
        }
        if pos > buf.len() {
            return Err("truncated field".into());
        }
        spans.push(Span {
            number,
            wire_type,
            raw: &buf[start..pos],
            payload,
        });
    }
    Ok(spans)
}

fn push_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn push_len_field(out: &mut Vec<u8>, number: u64, payload: &[u8]) {
    push_varint(out, (number << 3) | 2);
    push_varint(out, payload.len() as u64);
    out.extend_from_slice(payload);
}

/// Pure rewrite, separated from the VRL plumbing so it is unit-testable.
fn rewrite(
    message: &[u8],
    variant_fields: &std::collections::BTreeMap<Vec<u8>, u64>,
) -> Result<Vec<u8>, String> {
    let spans = read_spans(message)?;

    // Proto3 singular fields: the last occurrence wins.
    let event_name = spans
        .iter()
        .rev()
        .find(|s| s.number == EVENT_NAME_FIELD && s.wire_type == 2)
        .map(|s| s.payload);
    let v2 = spans
        .iter()
        .rev()
        .find(|s| s.number == SERVICE_EXTRA_V2_FIELD && s.wire_type == 2)
        .map(|s| s.payload);

    let synthetic_number = event_name.and_then(|name| variant_fields.get(name).copied());
    // Classic's ServiceExtraDecoder only uses V2 when it is non-empty; an empty V2 falls back to V1.
    let replacement = match (synthetic_number, v2) {
        (Some(number), Some(bytes)) if !bytes.is_empty() => {
            let mut member = Vec::new();
            push_len_field(&mut member, number, bytes);
            Some(member)
        }
        _ => None,
    };

    let mut out = Vec::with_capacity(message.len());
    for span in &spans {
        if span.number == SERVICE_EXTRA_V2_FIELD {
            continue;
        }
        if span.number == SERVICE_EXTRA_FIELD && replacement.is_some() {
            continue;
        }
        out.extend_from_slice(span.raw);
    }
    if let Some(member) = replacement {
        push_len_field(&mut out, SERVICE_EXTRA_FIELD, &member);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    const EVENT: &[u8] = b"ModelServingProxyEvent";
    const SYNTH: u64 = 10_001;

    fn map() -> BTreeMap<Vec<u8>, u64> {
        BTreeMap::from([(EVENT.to_vec(), SYNTH)])
    }

    fn field_bytes(number: u64, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        push_len_field(&mut out, number, payload);
        out
    }

    fn varint_field(number: u64, value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        push_varint(&mut out, number << 3);
        push_varint(&mut out, value);
        out
    }

    fn event(fields: &[Vec<u8>]) -> Vec<u8> {
        fields.concat()
    }

    #[test]
    fn moves_v2_into_synthetic_member_and_drops_field_16() {
        let payload = b"\x08\x2a\x12\x03abc";
        let input = event(&[
            field_bytes(1, EVENT),
            varint_field(2, 7),
            field_bytes(16, payload),
        ]);
        let out = rewrite(&input, &map()).unwrap();
        // Field 16 gone; the prefix fields are copied byte for byte.
        let expected = event(&[
            field_bytes(1, EVENT),
            varint_field(2, 7),
            field_bytes(7, &field_bytes(SYNTH, payload)),
        ]);
        assert_eq!(out, expected);
        assert!(read_spans(&out).unwrap().iter().all(|s| s.number != 16));
    }

    #[test]
    fn v2_replaces_v1_service_extra_for_a_mapped_event() {
        let v1 = field_bytes(7, &field_bytes(1, b"jobs-payload"));
        let input = event(&[field_bytes(1, EVENT), v1, field_bytes(16, b"\x08\x01")]);
        let out = rewrite(&input, &map()).unwrap();
        let spans = read_spans(&out).unwrap();
        let sevens: Vec<_> = spans.iter().filter(|s| s.number == 7).collect();
        assert_eq!(sevens.len(), 1, "exactly one ServiceExtra survives");
        assert_eq!(
            sevens[0].payload,
            field_bytes(SYNTH, b"\x08\x01").as_slice()
        );
    }

    #[test]
    fn unmapped_event_drops_field_16_and_keeps_service_extra() {
        let v1 = field_bytes(7, &field_bytes(1, b"jobs-payload"));
        let input = event(&[
            field_bytes(1, b"NotRegistered"),
            v1.clone(),
            field_bytes(16, b"\x08\x01"),
        ]);
        let out = rewrite(&input, &map()).unwrap();
        assert_eq!(out, event(&[field_bytes(1, b"NotRegistered"), v1]));
    }

    #[test]
    fn mapped_event_without_v2_is_unchanged() {
        let input = event(&[field_bytes(1, EVENT), field_bytes(7, &field_bytes(1, b"x"))]);
        assert_eq!(rewrite(&input, &map()).unwrap(), input);
    }

    #[test]
    fn empty_v2_falls_back_to_v1_like_classic() {
        let v1 = field_bytes(7, &field_bytes(1, b"x"));
        let input = event(&[field_bytes(1, EVENT), v1.clone(), field_bytes(16, b"")]);
        assert_eq!(
            rewrite(&input, &map()).unwrap(),
            event(&[field_bytes(1, EVENT), v1])
        );
    }

    #[test]
    fn last_occurrence_wins_for_singular_fields() {
        let input = event(&[
            field_bytes(1, b"Other"),
            field_bytes(16, b"\x01"),
            field_bytes(1, EVENT),
            field_bytes(16, b"\x02"),
        ]);
        let out = rewrite(&input, &map()).unwrap();
        let spans = read_spans(&out).unwrap();
        let member = spans
            .iter()
            .find(|s| s.number == 7)
            .expect("synthetic member");
        assert_eq!(member.payload, field_bytes(SYNTH, b"\x02").as_slice());
    }

    #[test]
    fn malformed_wire_bytes_fail_closed() {
        assert!(
            rewrite(&[0x0a, 0x09, 0x01], &map()).is_err(),
            "truncated length"
        );
        assert!(rewrite(&[0x0b], &map()).is_err(), "unsupported wire type 3");
        assert!(rewrite(&[0x80], &map()).is_err(), "truncated varint");
    }

    #[test]
    fn rejects_out_of_range_field_numbers() {
        let mut m = ObjectMap::new();
        m.insert("E".into(), Value::Integer(9999));
        assert!(
            parse_variant_fields(&m).is_err(),
            "must not collide with v1 oneof"
        );
        m.insert("E".into(), Value::Integer(10_000));
        assert!(parse_variant_fields(&m).is_ok());
    }
}
