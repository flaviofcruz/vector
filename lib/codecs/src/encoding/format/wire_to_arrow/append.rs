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
#[inline(always)]
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
/// The only runtime error is a wire-type disagreement; (kind, builder)
/// pairing is enforced when the plan is built.
#[inline(always)]
pub(super) fn append_scalar_from_wire(
    kind: ScalarKind,
    wv: &WireValue,
    tb: &mut TypedBuilder,
) -> Result<()> {
    match kind {
        ScalarKind::Int32 => {
            let v = expect_varint(kind, wv)?;
            if let TypedBuilder::Int32(b) = tb {
                b.append_value(v as i32);
                return Ok(());
            }
        }
        ScalarKind::Int64 => {
            let v = expect_varint(kind, wv)?;
            match tb {
                TypedBuilder::Int64(b) => {
                    b.append_value(v as i64);
                    return Ok(());
                }
                TypedBuilder::TimestampMicros(b) => {
                    b.append_value(v as i64);
                    return Ok(());
                }
                _ => {}
            }
        }
        ScalarKind::UInt32 => {
            let v = expect_varint(kind, wv)?;
            if let TypedBuilder::UInt32(b) = tb {
                b.append_value(v as u32);
                return Ok(());
            }
        }
        ScalarKind::UInt64 => {
            let v = expect_varint(kind, wv)?;
            if let TypedBuilder::UInt64(b) = tb {
                b.append_value(v);
                return Ok(());
            }
        }
        ScalarKind::SInt32 => {
            let v = expect_varint(kind, wv)?;
            if let TypedBuilder::Int32(b) = tb {
                b.append_value(decode_zigzag32(v as u32));
                return Ok(());
            }
        }
        ScalarKind::SInt64 => {
            let v = expect_varint(kind, wv)?;
            match tb {
                TypedBuilder::Int64(b) => {
                    b.append_value(decode_zigzag64(v));
                    return Ok(());
                }
                TypedBuilder::TimestampMicros(b) => {
                    b.append_value(decode_zigzag64(v));
                    return Ok(());
                }
                _ => {}
            }
        }
        ScalarKind::Fixed32 => {
            let v = expect_i32(kind, wv)?;
            if let TypedBuilder::UInt32(b) = tb {
                b.append_value(v);
                return Ok(());
            }
        }
        ScalarKind::SFixed32 => {
            let v = expect_i32(kind, wv)?;
            if let TypedBuilder::Int32(b) = tb {
                b.append_value(v as i32);
                return Ok(());
            }
        }
        ScalarKind::Float => {
            let v = expect_i32(kind, wv)?;
            if let TypedBuilder::Float32(b) = tb {
                b.append_value(f32::from_bits(v));
                return Ok(());
            }
        }
        ScalarKind::Fixed64 => {
            let v = expect_i64(kind, wv)?;
            if let TypedBuilder::UInt64(b) = tb {
                b.append_value(v);
                return Ok(());
            }
        }
        ScalarKind::SFixed64 => {
            let v = expect_i64(kind, wv)?;
            match tb {
                TypedBuilder::Int64(b) => {
                    b.append_value(v as i64);
                    return Ok(());
                }
                TypedBuilder::TimestampMicros(b) => {
                    b.append_value(v as i64);
                    return Ok(());
                }
                _ => {}
            }
        }
        ScalarKind::Double => {
            let v = expect_i64(kind, wv)?;
            if let TypedBuilder::Float64(b) = tb {
                b.append_value(f64::from_bits(v));
                return Ok(());
            }
        }
        ScalarKind::Bool => {
            let v = expect_varint(kind, wv)?;
            if let TypedBuilder::Boolean(b) = tb {
                b.append_value(v != 0);
                return Ok(());
            }
        }
        ScalarKind::String => {
            let bytes = expect_len(wv)?;
            if let TypedBuilder::LargeUtf8(b) = tb {
                let s = std::str::from_utf8(bytes).map_err(|_| WireToArrowError::InvalidUtf8)?;
                b.append_value(s);
                return Ok(());
            }
        }
        ScalarKind::Bytes => {
            let bytes = expect_len(wv)?;
            if let TypedBuilder::LargeBinary(b) = tb {
                b.append_value(bytes);
                return Ok(());
            }
        }
    }
    // Wire bytes don't match the schema, or (defensively) the plan builder
    // paired a scalar kind with a builder it can't write to.
    Err(WireToArrowError::WireTypeMismatch {
        expected: kind.wire_type(),
        actual: wire_type_byte(wv),
    })
}

/// Append the proto3 default for `kind` to `tb`. Used at `finalize_row` time
/// for absent scalar slots inside Map entry sub-plans, where the Arrow Map
/// type declares the key non-nullable but proto3 elides the key tag whenever
/// it carries its default value (`""`, `0`, `false`, `b""`). Writing a null
/// here would fail `StructArray::try_new` at finish; writing the proto3
/// default matches the semantics every standards-compliant proto consumer
/// applies.
///
/// Infallible: [`MessagePlan::build_at_depth`] rejects any (`ScalarKind`,
/// Arrow leaf) pairing this helper can't handle via
/// [`ScalarKind::matches_arrow_type`], so by the time a builder tree exists
/// every `BuilderNode::Scalar` paired with `inside_map_entry = true` is
/// guaranteed to land in one of the matched arms below. Keep the pairings
/// here in sync with that check and with [`append_scalar_from_wire`].
///
/// [`MessagePlan::build_at_depth`]: super::plan::MessagePlan
/// [`ScalarKind::matches_arrow_type`]: super::plan::ScalarKind::matches_arrow_type
#[inline]
pub(super) fn append_proto3_default(kind: ScalarKind, tb: &mut TypedBuilder) {
    match kind {
        ScalarKind::Int32 | ScalarKind::SInt32 | ScalarKind::SFixed32 => {
            if let TypedBuilder::Int32(b) = tb {
                b.append_value(0);
                return;
            }
        }
        ScalarKind::Int64 | ScalarKind::SInt64 | ScalarKind::SFixed64 => match tb {
            TypedBuilder::Int64(b) => {
                b.append_value(0);
                return;
            }
            TypedBuilder::TimestampMicros(b) => {
                b.append_value(0);
                return;
            }
            _ => {}
        },
        ScalarKind::UInt32 | ScalarKind::Fixed32 => {
            if let TypedBuilder::UInt32(b) = tb {
                b.append_value(0);
                return;
            }
        }
        ScalarKind::UInt64 | ScalarKind::Fixed64 => {
            if let TypedBuilder::UInt64(b) = tb {
                b.append_value(0);
                return;
            }
        }
        ScalarKind::Float => {
            if let TypedBuilder::Float32(b) = tb {
                b.append_value(0.0);
                return;
            }
        }
        ScalarKind::Double => {
            if let TypedBuilder::Float64(b) = tb {
                b.append_value(0.0);
                return;
            }
        }
        ScalarKind::Bool => {
            if let TypedBuilder::Boolean(b) = tb {
                b.append_value(false);
                return;
            }
        }
        ScalarKind::String => {
            if let TypedBuilder::LargeUtf8(b) = tb {
                b.append_value("");
                return;
            }
        }
        ScalarKind::Bytes => {
            if let TypedBuilder::LargeBinary(b) = tb {
                b.append_value(b"" as &[u8]);
                return;
            }
        }
    }
    unreachable!(
        "plan-build invariant: (ScalarKind, TypedBuilder) pairing is enforced \
         by ScalarKind::matches_arrow_type at MessagePlan::build_at_depth; \
         reaching this arm means the plan and builder tree diverged"
    );
}

#[inline(always)]
fn expect_varint(kind: ScalarKind, wv: &WireValue) -> Result<u64> {
    if let WireValue::Varint(v) = wv {
        Ok(*v)
    } else {
        Err(WireToArrowError::WireTypeMismatch {
            expected: kind.wire_type(),
            actual: wire_type_byte(wv),
        })
    }
}

#[inline(always)]
fn expect_i32(kind: ScalarKind, wv: &WireValue) -> Result<u32> {
    if let WireValue::I32(v) = wv {
        Ok(*v)
    } else {
        Err(WireToArrowError::WireTypeMismatch {
            expected: kind.wire_type(),
            actual: wire_type_byte(wv),
        })
    }
}

#[inline(always)]
fn expect_i64(kind: ScalarKind, wv: &WireValue) -> Result<u64> {
    if let WireValue::I64(v) = wv {
        Ok(*v)
    } else {
        Err(WireToArrowError::WireTypeMismatch {
            expected: kind.wire_type(),
            actual: wire_type_byte(wv),
        })
    }
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

