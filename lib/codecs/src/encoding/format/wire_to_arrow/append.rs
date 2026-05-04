//! Wire-value → Arrow builder append dispatch and tagless packed-scalar readers.
//!
//! The outer scan in `mod.rs` walks proto bytes tag-by-tag and hands each
//! decoded `WireValue` to one of the `append_*` functions here. For packed
//! repeated scalars the tagless readers (`decode_varint`, `read_fixed32`,
//! `read_fixed64`) are used to walk the inner blob; `try_parse_field` expects
//! tag-prefixed fields and can't traverse that.

use zeroparser::wire::{WireValue, decode_zigzag32, decode_zigzag64};

use super::builders::TypedBuilder;
use super::errors::{Result, WireToArrowError};
use super::plan::{ScalarKind, WT_I32, WT_I64, WT_LEN, WT_VARINT};

/// Extract the inner bytes from a length-delimited `WireValue`, or error.
#[inline]
pub(super) fn expect_len<'a>(wv: &'a WireValue<'a>) -> Result<&'a [u8]> {
    match wv {
        WireValue::Len(b) => Ok(b),
        other => Err(WireToArrowError::WireTypeMismatch {
            expected: WT_LEN,
            actual: wire_type_byte(other),
        }),
    }
}

/// Proto wire type numeric code for a `WireValue`. Used for error reporting.
#[inline]
pub(super) fn wire_type_byte(wv: &WireValue) -> u8 {
    match wv {
        WireValue::Varint(_) => WT_VARINT,
        WireValue::I64(_) => WT_I64,
        WireValue::Len(_) => WT_LEN,
        WireValue::I32(_) => WT_I32,
    }
}

/// Append one scalar `WireValue` into the matching typed Arrow builder.
pub(super) fn append_scalar_from_wire(
    kind: ScalarKind,
    wv: &WireValue,
    tb: &mut TypedBuilder,
) -> Result<()> {
    match (kind, tb, wv) {
        (ScalarKind::Int32, TypedBuilder::Int32(b), WireValue::Varint(v)) => {
            b.append_value(*v as i32);
        }
        (ScalarKind::Int64, TypedBuilder::Int64(b), WireValue::Varint(v)) => {
            b.append_value(*v as i64);
        }
        // `int64` -> `Timestamp(Microsecond, _)` coercion. Proto carries the
        // value as a plain varint; the Arrow column interprets it as
        // microseconds since Unix epoch. Used primarily for `_event_time` on
        // LP tables (matching `proto_descriptor_to_arrow_schema`).
        (ScalarKind::Int64, TypedBuilder::TimestampMicros(b), WireValue::Varint(v)) => {
            b.append_value(*v as i64);
        }
        (ScalarKind::UInt32, TypedBuilder::UInt32(b), WireValue::Varint(v)) => {
            b.append_value(*v as u32);
        }
        (ScalarKind::UInt64, TypedBuilder::UInt64(b), WireValue::Varint(v)) => {
            b.append_value(*v);
        }
        (ScalarKind::SInt32, TypedBuilder::Int32(b), WireValue::Varint(v)) => {
            b.append_value(decode_zigzag32(*v as u32));
        }
        (ScalarKind::SInt64, TypedBuilder::Int64(b), WireValue::Varint(v)) => {
            b.append_value(decode_zigzag64(*v));
        }
        (ScalarKind::SInt64, TypedBuilder::TimestampMicros(b), WireValue::Varint(v)) => {
            b.append_value(decode_zigzag64(*v));
        }
        (ScalarKind::Fixed32, TypedBuilder::UInt32(b), WireValue::I32(v)) => {
            b.append_value(*v);
        }
        (ScalarKind::SFixed32, TypedBuilder::Int32(b), WireValue::I32(v)) => {
            b.append_value(*v as i32);
        }
        (ScalarKind::Float, TypedBuilder::Float32(b), WireValue::I32(v)) => {
            b.append_value(f32::from_bits(*v));
        }
        (ScalarKind::Fixed64, TypedBuilder::UInt64(b), WireValue::I64(v)) => {
            b.append_value(*v);
        }
        (ScalarKind::SFixed64, TypedBuilder::Int64(b), WireValue::I64(v)) => {
            b.append_value(*v as i64);
        }
        (ScalarKind::SFixed64, TypedBuilder::TimestampMicros(b), WireValue::I64(v)) => {
            b.append_value(*v as i64);
        }
        (ScalarKind::Double, TypedBuilder::Float64(b), WireValue::I64(v)) => {
            b.append_value(f64::from_bits(*v));
        }
        (ScalarKind::Bool, TypedBuilder::Boolean(b), WireValue::Varint(v)) => {
            b.append_value(*v != 0);
        }
        (ScalarKind::String, TypedBuilder::LargeUtf8(b), WireValue::Len(bytes)) => {
            let s = std::str::from_utf8(bytes).map_err(|_| WireToArrowError::InvalidUtf8)?;
            b.append_value(s);
        }
        (ScalarKind::Bytes, TypedBuilder::LargeBinary(b), WireValue::Len(bytes)) => {
            b.append_value(bytes);
        }
        // Any other combination is either a wire-type mismatch (wire bytes
        // don't match the declared schema) or — much less likely — a plan
        // that disagrees with its builder tree. Report as a wire-type
        // mismatch since that's the real-world failure mode.
        (_, _, wv) => {
            return Err(WireToArrowError::WireTypeMismatch {
                expected: kind.wire_type(),
                actual: wire_type_byte(wv),
            });
        }
    }
    Ok(())
}

/// Append a repeated-scalar occurrence (either a single unpacked value or a
/// full packed blob) into `values`.
///
/// `current_offset` is the running cumulative element count for the parent
/// Arrow `List<primitive>`: bumped by 1 per element appended here (1 for
/// unpacked, N for a packed blob). The owning `BuilderNode::RepeatedScalar`
/// later pushes it onto its `offsets` buffer at row finalization, which is
/// how list lengths are recorded in Arrow's offsets-buffer layout.
pub(super) fn append_repeated_scalar(
    kind: ScalarKind,
    wv: &WireValue,
    values: &mut TypedBuilder,
    current_offset: &mut i32,
) -> Result<()> {
    // Unpacked form: the `WireValue` variant matches the scalar's native
    // wire type. Single append, regardless of scalar kind.
    if wire_type_byte(wv) == kind.wire_type() {
        append_scalar_from_wire(kind, wv, values)?;
        *current_offset += 1;
        return Ok(());
    }

    // Packed form: a `Len` blob holding a run of raw scalar values. Only
    // valid when the scalar's native wire type is 0/1/5 (packable).
    let WireValue::Len(inner) = wv else {
        return Err(WireToArrowError::WireTypeMismatch {
            expected: kind.wire_type(),
            actual: wire_type_byte(wv),
        });
    };
    // Proto spec forbids packed encoding for length-delimited scalars
    // (string/bytes) — there's no length-prefix per element inside a packed
    // blob, so a `Len`-typed `string`/`bytes` must arrive as one unpacked
    // occurrence per value. Reject the combo here.
    if kind.wire_type() == WT_LEN {
        return Err(WireToArrowError::WireTypeMismatch {
            expected: kind.wire_type(),
            actual: wire_type_byte(wv),
        });
    }
    let mut pos = 0usize;
    while pos < inner.len() {
        let decoded = read_packed_element(kind, inner, &mut pos)?;
        append_scalar_from_wire(kind, &decoded, values)?;
        *current_offset += 1;
    }
    if pos != inner.len() {
        return Err(WireToArrowError::UnexpectedEof);
    }
    Ok(())
}

/// Read one raw scalar value from a packed blob and yield it as a
/// `WireValue` so we can reuse [`append_scalar_from_wire`] for the append.
#[inline]
fn read_packed_element<'a>(
    kind: ScalarKind,
    bytes: &'a [u8],
    pos: &mut usize,
) -> Result<WireValue<'a>> {
    match kind.wire_type() {
        WT_VARINT => Ok(WireValue::Varint(decode_varint(bytes, pos)?)),
        WT_I64 => Ok(WireValue::I64(read_fixed64(bytes, pos)?)),
        WT_I32 => Ok(WireValue::I32(read_fixed32(bytes, pos)?)),
        // `WT_LEN` would be string/bytes — unreachable per the caller's guard.
        // Any other value indicates a plan build bug.
        _ => Err(WireToArrowError::PlanBuilderMismatch),
    }
}

// ---------------------------------------------------------------------------
// Tagless readers for the packed-scalar inner loop.
//
// `try_parse_field` expects tag-prefixed fields. Packed repeated scalars live
// inside a single `WireValue::Len(inner)` blob whose contents are raw values
// with no tags. Proto-parser doesn't expose tagless readers, so we keep these
// here until a follow-up integration removes the packed inner loop entirely.
// ---------------------------------------------------------------------------

/// Read a single varint and advance `pos`. Caps at 10 bytes per proto spec.
#[inline]
pub(super) fn decode_varint(bytes: &[u8], pos: &mut usize) -> Result<u64> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    for _ in 0..10 {
        if *pos >= bytes.len() {
            return Err(WireToArrowError::UnexpectedEof);
        }
        let b = bytes[*pos];
        *pos += 1;
        value |= u64::from(b & 0x7f) << shift;
        if b < 0x80 {
            return Ok(value);
        }
        shift += 7;
    }
    Err(WireToArrowError::VarintOverflow)
}

/// Read 8 little-endian bytes and advance `pos`.
#[inline]
pub(super) fn read_fixed64(bytes: &[u8], pos: &mut usize) -> Result<u64> {
    if *pos + 8 > bytes.len() {
        return Err(WireToArrowError::UnexpectedEof);
    }
    let v = u64::from_le_bytes(bytes[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Ok(v)
}

/// Read 4 little-endian bytes and advance `pos`.
#[inline]
pub(super) fn read_fixed32(bytes: &[u8], pos: &mut usize) -> Result<u32> {
    if *pos + 4 > bytes.len() {
        return Err(WireToArrowError::UnexpectedEof);
    }
    let v = u32::from_le_bytes(bytes[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Ok(v)
}
