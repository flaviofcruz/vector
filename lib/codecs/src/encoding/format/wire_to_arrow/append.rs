//! Wire-value → Arrow builder append dispatch.
//!
//! The outer scan in `mod.rs` walks proto bytes tag-by-tag and hands each
//! decoded `WireValue` to one of the `append_*` functions here. For packed
//! repeated scalars the inner blob is walked tagless: `try_parse_field`
//! expects tag-prefixed fields and can't traverse it, so varints are read
//! via `zeroparser::wire::try_read_varint` and the two fixed-width wire
//! types are read inline (3-line LE chunk reads — not worth a helper).

use zeroparser::wire::{WireValue, decode_zigzag32, decode_zigzag64, try_read_varint};

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
    let mut remaining: &[u8] = inner;
    while !remaining.is_empty() {
        let decoded;
        (decoded, remaining) = read_packed_element(kind, remaining)?;
        append_scalar_from_wire(kind, &decoded, values)?;
        *current_offset += 1;
    }
    Ok(())
}

/// Mirror of [`append_scalar_from_wire`] that runs the wire-type / UTF-8
/// checks without touching a builder. Used by the encoder's pre-validate
/// pass so a malformed row can be detected and dropped before any column
/// builder is mutated (Arrow's `*Builder` types expose no rollback API, so
/// rejecting the row up front is how we keep per-row isolation).
///
/// Must stay in lock-step with [`append_scalar_from_wire`]: every (kind, wv)
/// combination that succeeds here must also succeed there, and vice versa.
pub(super) fn validate_scalar_from_wire(kind: ScalarKind, wv: &WireValue) -> Result<()> {
    match (kind, wv) {
        (ScalarKind::Int32, WireValue::Varint(_))
        | (ScalarKind::Int64, WireValue::Varint(_))
        | (ScalarKind::UInt32, WireValue::Varint(_))
        | (ScalarKind::UInt64, WireValue::Varint(_))
        | (ScalarKind::SInt32, WireValue::Varint(_))
        | (ScalarKind::SInt64, WireValue::Varint(_))
        | (ScalarKind::Bool, WireValue::Varint(_))
        | (ScalarKind::Fixed32, WireValue::I32(_))
        | (ScalarKind::SFixed32, WireValue::I32(_))
        | (ScalarKind::Float, WireValue::I32(_))
        | (ScalarKind::Fixed64, WireValue::I64(_))
        | (ScalarKind::SFixed64, WireValue::I64(_))
        | (ScalarKind::Double, WireValue::I64(_))
        | (ScalarKind::Bytes, WireValue::Len(_)) => Ok(()),
        (ScalarKind::String, WireValue::Len(bytes)) => std::str::from_utf8(bytes)
            .map(|_| ())
            .map_err(|_| WireToArrowError::InvalidUtf8),
        (_, wv) => Err(WireToArrowError::WireTypeMismatch {
            expected: kind.wire_type(),
            actual: wire_type_byte(wv),
        }),
    }
}

/// Mirror of [`append_repeated_scalar`] that walks the value (or packed
/// blob) without appending. Used by the pre-validate pass — the packed-blob
/// inner loop in [`append_repeated_scalar`] is the one site in the encoder
/// where a partial append is possible (an EOF on element N leaves N-1
/// values already in the builder), so dropping the row up front here is how
/// we keep per-row isolation for repeated scalars.
pub(super) fn validate_repeated_scalar(kind: ScalarKind, wv: &WireValue) -> Result<()> {
    if wire_type_byte(wv) == kind.wire_type() {
        return validate_scalar_from_wire(kind, wv);
    }
    let WireValue::Len(inner) = wv else {
        return Err(WireToArrowError::WireTypeMismatch {
            expected: kind.wire_type(),
            actual: wire_type_byte(wv),
        });
    };
    if kind.wire_type() == WT_LEN {
        return Err(WireToArrowError::WireTypeMismatch {
            expected: kind.wire_type(),
            actual: wire_type_byte(wv),
        });
    }
    let mut remaining: &[u8] = inner;
    while !remaining.is_empty() {
        // Packed scalars are always varint / fixed32 / fixed64; the decoded
        // `WireValue` is always shape-compatible with `kind`, so no further
        // per-element validation is needed.
        (_, remaining) = read_packed_element(kind, remaining)?;
    }
    Ok(())
}

/// Read one raw scalar value from a packed blob and yield it as a
/// `WireValue` alongside the remaining bytes. The caller reuses
/// [`append_scalar_from_wire`] for the actual append.
///
/// Tagless: the inner blob of a packed-repeated field has no per-element
/// tags, so we dispatch on the scalar's wire type directly into zeroparser's
/// tagless readers.
#[inline]
pub(super) fn read_packed_element<'a>(
    kind: ScalarKind,
    bytes: &'a [u8],
) -> Result<(WireValue<'a>, &'a [u8])> {
    match kind.wire_type() {
        WT_VARINT => {
            let (v, rest) = try_read_varint(bytes)?;
            Ok((WireValue::Varint(v), rest))
        }
        WT_I64 => {
            let Some((b, rest)) = bytes.split_first_chunk::<8>() else {
                return Err(WireToArrowError::UnexpectedEof);
            };
            Ok((WireValue::I64(u64::from_le_bytes(*b)), rest))
        }
        WT_I32 => {
            let Some((b, rest)) = bytes.split_first_chunk::<4>() else {
                return Err(WireToArrowError::UnexpectedEof);
            };
            Ok((WireValue::I32(u32::from_le_bytes(*b)), rest))
        }
        // `WT_LEN` would be string/bytes — unreachable per the caller's guard.
        // Any other value indicates a plan build bug.
        _ => Err(WireToArrowError::PlanBuilderMismatch {
            site: "read_packed_element:non_packable_wire_type",
        }),
    }
}

