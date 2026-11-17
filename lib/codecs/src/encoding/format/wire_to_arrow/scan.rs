//! Per-message wire walks.
//!
//! [`scan_message`] is the hot path — it walks one proto message's wire
//! bytes and appends decoded values into the matching Arrow column
//! builders. [`validate_message`] is the side-effect-free mirror used by
//! `WireToArrowEncoder::encode_batch` to detect malformed rows before
//! any builder is mutated, so a single poison row can be dropped without
//! poisoning the whole batch.

use std::sync::Arc;

use bytes::Bytes;
use zeroparser::wire::{WireValue, try_parse_field};

use super::append::{
    append_enum_string_from_wire, append_repeated_enum_string, append_repeated_scalar,
    append_scalar_from_wire, encode_variant, expect_len, validate_enum_string_from_wire,
    validate_repeated_enum_string, validate_repeated_scalar, validate_scalar_from_wire,
    wire_type_byte,
};
use super::builders::{self, BuilderNodeList};
use super::errors::{Result, WireToArrowError};
use super::plan::{MessagePlan, PlanSlot, SLOT_UNKNOWN, VariantSource};
use super::serializer::OnWireTypeMismatch;
use super::variant_message::SkipReport;

/// Wire-type guard shared by the validate and scan passes: is this wire
/// value a legal encoding for the slot at `slot_idx`?
///
/// Hot path — runs on every known field occurrence of every row, in both
/// passes — so it reads the precomputed per-slot accept mask (bit n = wire
/// type n) instead of matching the `PlanSlot` enum.
///
/// Every mismatched occurrence is self-delimiting (`try_parse_field` has
/// already consumed its value), so skipping it is a pure `continue` at every
/// nesting depth: only that field's column is left unset for the occurrence;
/// sibling fields, other repeated elements and the row itself survive. This
/// is the protobuf-java behavior ("mismatched wire type is treated as an
/// unknown field"). The masks are derived from
/// [`PlanSlot::accept_wire_type_mask`], so both passes cannot diverge; any
/// *other* validate/scan disagreement (a code bug) still surfaces as a
/// batch-level error via `?` in `encode_batch`.
///
/// A *malformed* value that is wire-type-compatible (a truncated packed
/// blob, an overlong varint, invalid UTF-8 in a declared string) is NOT a
/// mismatch — it still fails the per-field validators and drops the row,
/// exactly as before.
#[inline(always)]
pub(super) fn wire_type_matches(accept_mask: u8, wv: &WireValue<'_>) -> bool {
    accept_mask & (1u8 << wire_type_byte(wv)) != 0
}

/// Per-batch accumulator for wire-type-mismatch skips committed from the
/// validate pass's per-row tallies (which run on every row, so the counts are
/// authoritative; the scan pass skips the same occurrences silently).
#[derive(Default)]
pub(super) struct SkipAccumulator {
    /// Total mismatched field occurrences skipped across the batch.
    pub(super) skipped: u64,
    /// First skip in the batch, kept for the (rate-limited-by-batch) warning.
    pub(super) sample: Option<SkipSample>,
}

/// One sampled skip: the root-to-leaf proto field path, the wire types
/// involved, and the row's wire bytes for producer attribution. `Bytes` is
/// refcounted, so keeping the row costs a reference count bump, not a copy.
pub(super) struct SkipSample {
    pub(super) field_numbers: Vec<u32>,
    pub(super) expected: u8,
    pub(super) actual: u8,
    pub(super) row: Bytes,
}

/// Per-row tally of wire-type-mismatch skips, recorded by the validate pass
/// (and by the VARIANT walker) and committed to the batch
/// [`SkipAccumulator`] only once the row is kept — a row that later drops (a
/// truncated sibling, a failed canonicalization) must not count its skips
/// nor feed the skip warning's sample.
#[derive(Default)]
pub(super) struct RowSkips {
    skipped: u64,
    /// Root-to-leaf path of this row's first skip, reused across rows. Only
    /// written while `capture` is set (until the batch has a sample), so the
    /// no-skip hot path stays allocation-free.
    first_path: Vec<u32>,
    first_expected: u8,
    first_actual: u8,
    has_first: bool,
    capture: bool,
}

impl RowSkips {
    pub(super) fn reset(&mut self, capture: bool) {
        self.skipped = 0;
        self.has_first = false;
        self.capture = capture;
    }

    fn record(&mut self, path: &[u32], field_number: u32, expected: u8, actual: u8) {
        self.skipped += 1;
        if self.capture && !self.has_first {
            self.first_path.clear();
            self.first_path.extend_from_slice(path);
            self.first_path.push(field_number);
            self.first_expected = expected;
            self.first_actual = actual;
            self.has_first = true;
        }
    }

    pub(super) fn commit(&mut self, acc: &mut SkipAccumulator, row: &Bytes) {
        if self.skipped == 0 {
            return;
        }
        acc.skipped += self.skipped;
        if acc.sample.is_none() && self.has_first {
            acc.sample = Some(SkipSample {
                field_numbers: std::mem::take(&mut self.first_path),
                expected: self.first_expected,
                actual: self.first_actual,
                row: row.clone(),
            });
        }
    }

    /// Captured tally state for rollback. A message-backed VARIANT walk that
    /// falls back to the sanitized reference path records skips twice — once
    /// during the abandoned walk, once during the sanitization re-walk — so
    /// the caller snapshots beforehand and restores in between.
    pub(super) fn snapshot(&self) -> RowSkipsSnapshot {
        RowSkipsSnapshot {
            skipped: self.skipped,
            first_path_len: self.first_path.len(),
            first_expected: self.first_expected,
            first_actual: self.first_actual,
            has_first: self.has_first,
        }
    }

    pub(super) fn restore(&mut self, snapshot: RowSkipsSnapshot) {
        self.skipped = snapshot.skipped;
        self.first_path.truncate(snapshot.first_path_len);
        self.first_expected = snapshot.first_expected;
        self.first_actual = snapshot.first_actual;
        self.has_first = snapshot.has_first;
    }
}

/// See [`RowSkips::snapshot`].
#[derive(Clone, Copy)]
pub(super) struct RowSkipsSnapshot {
    skipped: u64,
    first_path_len: usize,
    first_expected: u8,
    first_actual: u8,
    has_first: bool,
}

/// Context threaded through `validate_message`: the configured mismatch
/// policy, the full row's wire bytes (for skip attribution), the row's
/// pending skip tally, and the stack of enclosing proto field numbers used to
/// build root-to-leaf paths for skip samples.
pub(super) struct ValidateCtx<'a> {
    pub(super) on_mismatch: OnWireTypeMismatch,
    pub(super) row: &'a Bytes,
    pub(super) skips: &'a mut RowSkips,
    pub(super) path: &'a mut Vec<u32>,
}

/// Scan one proto message's wire bytes, appending values into `builders`.
///
/// Sets `builders.present[i] = true` for each slot `i` touched by any tag in
/// this message. The caller is responsible for resetting `present` (via
/// [`BuilderNodeList::reset_present`]) before invoking, and for calling
/// [`BuilderNodeList::finalize_row`] afterwards. Sub-messages reuse their own
/// level's `present` buffer, so no per-occurrence allocation happens on the
/// hot path.
pub(super) fn scan_message(
    plan: &MessagePlan,
    mut bytes: &[u8],
    builders: &mut BuilderNodeList,
    variants: &mut BuiltVariants,
) -> Result<()> {
    let dispatch_table = plan.slot_by_proto_field.as_slice();
    while !bytes.is_empty() {
        let (field, rest) = try_parse_field(bytes)?;
        bytes = rest;
        let field_number = field.field_num as usize;

        // Out-of-range or unknown field number — skip; `try_parse_field`
        // already consumed the value.
        let Some(&slot_idx) = dispatch_table.get(field_number) else {
            continue;
        };
        if slot_idx == SLOT_UNKNOWN {
            continue;
        }
        let slot_idx = slot_idx as usize;

        // Wire-type mismatch: `validate_message` already handled this
        // occurrence (skipped it under `SkipField`, or dropped the whole row
        // under `DropRow` before the scan ever ran), so it must be invisible
        // here too or builders would diverge from the pre-pass. Both passes
        // read the same precomputed accept mask, so their skip decisions
        // cannot diverge.
        if !wire_type_matches(plan.wt_accept_masks[slot_idx], &field.value) {
            continue;
        }

        let BuilderNodeList { nodes, present } = &mut *builders;
        match &mut nodes[slot_idx] {
            builders::BuilderNode::Scalar { kind, builder } => {
                append_scalar_from_wire(*kind, &field.value, builder)?;
                present[slot_idx] = true;
            }
            builders::BuilderNode::EnumString { desc, builder } => {
                append_enum_string_from_wire(desc, &field.value, builder)?;
                present[slot_idx] = true;
            }
            builders::BuilderNode::Variant {
                metadata, value, ..
            } => {
                let (m, v) = variants.take()?;
                metadata.append_value(&m);
                value.append_value(&v);
                present[slot_idx] = true;
            }
            builders::BuilderNode::RepeatedVariant {
                metadata,
                value,
                current_offset,
                ..
            } => {
                let (m, v) = variants.take()?;
                metadata.append_value(&m);
                value.append_value(&v);
                *current_offset =
                    current_offset
                        .checked_add(1)
                        .ok_or(WireToArrowError::OffsetOverflow {
                            site: "scan_message:repeated_variant",
                        })?;
                present[slot_idx] = true;
            }
            builders::BuilderNode::Struct {
                sub_plan, children, ..
            } => {
                let sub_bytes = expect_len(&field.value)?;
                children.reset_present();
                scan_message(sub_plan, sub_bytes, children, variants)?;
                children.finalize_row(sub_plan);
                present[slot_idx] = true;
            }
            builders::BuilderNode::RepeatedMessage {
                sub_plan,
                children,
                current_offset,
                ..
            }
            | builders::BuilderNode::Map {
                sub_plan,
                children,
                current_offset,
                ..
            } => {
                let sub_bytes = expect_len(&field.value)?;
                children.reset_present();
                scan_message(sub_plan, sub_bytes, children, variants)?;
                children.finalize_row(sub_plan);
                // Cumulative across the batch — checked_add converts what
                // would otherwise be a release-mode wrap + OffsetBuffer
                // assertion (process panic at finish) into a clean batch
                // failure. Per-row validate doesn't catch this case because
                // the overflow can be aggregate across many small rows.
                *current_offset =
                    current_offset
                        .checked_add(1)
                        .ok_or(WireToArrowError::OffsetOverflow {
                            site: "scan_message:repeated_message_or_map",
                        })?;
                present[slot_idx] = true;
            }
            builders::BuilderNode::RepeatedScalar {
                kind,
                values,
                current_offset,
                ..
            } => {
                append_repeated_scalar(*kind, &field.value, values, current_offset)?;
                present[slot_idx] = true;
            }
            builders::BuilderNode::RepeatedEnumString {
                desc,
                values,
                current_offset,
                ..
            } => {
                append_repeated_enum_string(desc, &field.value, values, current_offset)?;
                present[slot_idx] = true;
            }
        }
    }
    Ok(())
}

/// Walk one proto message's wire bytes without touching any builders, surfacing
/// every decode error that [`scan_message`] would produce for the same input.
/// [`WireToArrowEncoder::encode_batch`] runs this as a pre-pass per message so
/// rows that fail can be dropped from the batch cleanly — no half-appended
/// leaves, no finalized nested sub-rows — and replaced with a `dropped`
/// counter instead of failing the entire batch.
///
/// The two-pass cost is acceptable because (a) the parse walk is small
/// relative to value appends + buffer growth on the real scan, and (b) Arrow
/// `*Builder` types expose no public rollback API, so an in-place
/// "snapshot + truncate on error" alternative isn't viable.
///
/// Must stay in lock-step with [`scan_message`]: any wire byte sequence that
/// is accepted here must also be accepted there, and vice versa. If the two
/// diverge (validate accepts but scan errors), the real scan's `?` in
/// `encode_batch` will bubble it out as a batch-level failure — that's a
/// clear signal of a code bug rather than user input.
///
/// [`WireToArrowEncoder::encode_batch`]: super::encoder::WireToArrowEncoder::encode_batch
///
/// VARIANT values are fully built here and pushed onto `variants`, in wire
/// order, for the scan to append.
pub(super) fn validate_message(
    plan: &MessagePlan,
    mut bytes: &[u8],
    variants: &mut BuiltVariants,
    ctx: &mut ValidateCtx<'_>,
) -> Result<()> {
    // Track which singular slots (Scalar / Struct) have been seen in this
    // message so a duplicate tag drops the row instead of corrupting
    // column-length alignment downstream in scan_message. Proto3 parsers
    // are required to accept duplicate singular tags (last-wins for
    // scalars, merge for sub-messages), but Arrow `*Builder` types don't
    // expose retraction. The encoder canonicalizes these rows and validates
    // them again before scanning, leaving ordinary rows on the direct wire path.
    //
    // Slots and oneof groups share 128 inline bits; wider plans use a heap bitvec.
    // Common case is zero allocations on the hot path.
    let mut seen_singular = SeenSingular::with_capacity(plan.slots.len() + plan.oneof_count);

    while !bytes.is_empty() {
        let (field, rest) = try_parse_field(bytes)?;
        bytes = rest;
        let field_number = field.field_num as usize;

        let slot_idx_u = plan.slot_by_proto_field.get(field_number).copied();
        let slot_idx = match slot_idx_u {
            Some(idx) if idx != SLOT_UNKNOWN => idx as usize,
            // Not a projected field — but it may still be an unprojected
            // oneof member whose occurrence must clear a projected sibling,
            // which only normalization can do. A *wire-type-mismatched*
            // occurrence is an unknown field to protobuf-java: it must
            // neither trigger normalization nor clear the sibling, so the
            // accept-mask check runs before the oneof bookkeeping. Only
            // oneof members are checked here (a descriptor lookup away);
            // other unprojected fields stay unchecked, by design, for cost —
            // which also means their skips are never counted.
            _ => {
                if let Some(&oneof) = plan.oneof_by_proto_field.get(&(field.field_num as u32)) {
                    // `DropRow` deliberately skips this check: pre-skip
                    // behavior ignored a lone mismatched unprojected member
                    // and kept the row, and `drop_row` must match it exactly.
                    if ctx.on_mismatch == OnWireTypeMismatch::SkipField
                        && let Some(&accept_mask) = plan
                            .unprojected_oneof_wt_masks
                            .get(&(field.field_num as u32))
                        && !wire_type_matches(accept_mask, &field.value)
                    {
                        // Oneof members are never repeated, so the mask holds
                        // a single bit — its position is the wire type the
                        // projected-slot validators would report as `expected`.
                        let expected = accept_mask.trailing_zeros() as u8;
                        let actual = wire_type_byte(&field.value);
                        ctx.skips
                            .record(ctx.path, field.field_num as u32, expected, actual);
                        continue;
                    }
                    if seen_singular.test_and_set(plan.slots.len() + oneof) {
                        return Err(WireToArrowError::OneofNeedsNormalization {
                            field_number: field.field_num as u32,
                        });
                    }
                }
                continue;
            }
        };
        let slot = &plan.slots[slot_idx];

        // Wire-type mismatch on a known field, decided before the oneof /
        // singular bookkeeping below: a mismatched occurrence is invisible
        // to both, and `normalize_message` strips it from a replayed row for
        // the same reason — so a mismatched duplicate must not trigger
        // canonicalization on its own. The masks are derived from the
        // per-slot validators, so the reported wire types match what the
        // validators would produce.
        if !wire_type_matches(plan.wt_accept_masks[slot_idx], &field.value) {
            let expected = slot.singular_wire_type();
            let actual = wire_type_byte(&field.value);
            match ctx.on_mismatch {
                OnWireTypeMismatch::SkipField => {
                    ctx.skips
                        .record(ctx.path, field.field_num as u32, expected, actual);
                }
                OnWireTypeMismatch::DropRow => {
                    return Err(WireToArrowError::WireTypeMismatch { expected, actual }
                        .at_proto_field(field.field_num as u32));
                }
            }
            continue;
        }

        if let Some(&oneof) = plan.oneof_by_proto_field.get(&(field.field_num as u32))
            && seen_singular.test_and_set(plan.slots.len() + oneof)
        {
            return Err(WireToArrowError::OneofNeedsNormalization {
                field_number: field.field_num as u32,
            });
        }

        let validation = match slot {
            PlanSlot::Scalar(sk) => {
                if seen_singular.test_and_set(slot_idx) {
                    return Err(WireToArrowError::DuplicateSingularField {
                        field_number: field.field_num as u32,
                    });
                }
                validate_scalar_from_wire(*sk, &field.value)
            }
            PlanSlot::Struct(sub_plan) => {
                if seen_singular.test_and_set(slot_idx) {
                    return Err(WireToArrowError::DuplicateSingularField {
                        field_number: field.field_num as u32,
                    });
                }
                validate_sub_message(
                    sub_plan,
                    field.field_num as u32,
                    &field.value,
                    variants,
                    ctx,
                )
            }
            PlanSlot::RepeatedMessage(sub_plan) | PlanSlot::Map(sub_plan) => validate_sub_message(
                sub_plan,
                field.field_num as u32,
                &field.value,
                variants,
                ctx,
            ),
            PlanSlot::RepeatedScalar(sk) => validate_repeated_scalar(*sk, &field.value),
            PlanSlot::RepeatedEnumString(desc) => validate_repeated_enum_string(desc, &field.value),
            PlanSlot::EnumString(desc) => {
                if seen_singular.test_and_set(slot_idx) {
                    return Err(WireToArrowError::DuplicateSingularField {
                        field_number: field.field_num as u32,
                    });
                }
                validate_enum_string_from_wire(desc, &field.value)
            }
            // Singular field: dedup like Scalar/Struct, and run the full Variant
            // build so a malformed-JSON *or* build-failing row is dropped before
            // the scan appends it.
            PlanSlot::Variant(source) => {
                if seen_singular.test_and_set(slot_idx) {
                    return Err(WireToArrowError::DuplicateSingularField {
                        field_number: field.field_num as u32,
                    });
                }
                encode_variant_slot(source, &field.value, ctx, field.field_num as u32)
                    .map(|value| variants.push(value))
            }
            // Repeated: no dedup (repeated fields legitimately recur), each
            // occurrence fully built.
            PlanSlot::RepeatedVariant(source) => {
                encode_variant_slot(source, &field.value, ctx, field.field_num as u32)
                    .map(|value| variants.push(value))
            }
            // No proto field number ever points at an Absent slot (Absent
            // slots are Arrow columns the proto descriptor lacks), so this
            // arm is unreachable in practice. Mirror `scan_message`'s
            // fall-through and surface it as a code-bug signal.
            PlanSlot::Absent => {
                return Err(WireToArrowError::PlanBuilderMismatch {
                    site: "validate_message:absent_slot_unreachable",
                });
            }
        };
        // Wire-type mismatches were already decided by the accept mask above;
        // the per-slot validators now only reject wire-type-*compatible*
        // malformation (truncated packed blobs, invalid UTF-8). A plain
        // `WireTypeMismatch` here would mean the masks diverged from a
        // validator — handle it with the same skip policy as a safety net
        // rather than dropping the row. Other errors are attributed to this
        // field's root-to-leaf path.
        match validation {
            Ok(()) => {}
            Err(WireToArrowError::WireTypeMismatch { expected, actual }) => match ctx.on_mismatch {
                OnWireTypeMismatch::SkipField => {
                    ctx.skips
                        .record(ctx.path, field.field_num as u32, expected, actual);
                }
                OnWireTypeMismatch::DropRow => {
                    return Err(WireToArrowError::WireTypeMismatch { expected, actual }
                        .at_proto_field(field.field_num as u32));
                }
            },
            Err(error) => return Err(error.at_proto_field(field.field_num as u32)),
        }
    }
    Ok(())
}

/// Recurse `validate_message` into a length-delimited sub-message, extending
/// the root-to-leaf field-number path used for skip samples. A non-`Len`
/// value fails `expect_len` here and surfaces as a plain `WireTypeMismatch`
/// attributed to the caller's field.
fn validate_sub_message(
    sub_plan: &Arc<MessagePlan>,
    field_number: u32,
    value: &WireValue<'_>,
    variants: &mut BuiltVariants,
    ctx: &mut ValidateCtx<'_>,
) -> Result<()> {
    let sub_bytes = expect_len(value)?;
    ctx.path.push(field_number);
    let result = validate_message(
        sub_plan,
        sub_bytes,
        variants,
        &mut ValidateCtx {
            on_mismatch: ctx.on_mismatch,
            row: ctx.row,
            skips: ctx.skips,
            path: ctx.path,
        },
    );
    ctx.path.pop();
    result
}

/// Encode one VARIANT wire value during validation.
///
/// Under `SkipField`, a message-backed VARIANT whose sub-message carries
/// wire-type mismatches is walked directly with the mismatched occurrences
/// omitted (protobuf-java semantics) instead of falling back to the prost
/// decoder, which rejects them and would drop the row — the same treatment a
/// Struct column gets. If the walk still bails for a reference-only shape
/// (a map, a well-known type, a conflicting oneof), the bytes are sanitized —
/// every mismatched occurrence the reference decoder would reject is
/// stripped, recursively — and the sanitized bytes go to the reference
/// decoder, so the row survives with those fields omitted.
///
/// Skips are reported into the row's pending tally, so they count only if
/// the row is kept. The abandoned direct walk may have recorded skips before
/// bailing; sanitization re-walks and is the authoritative skip recorder for
/// the fallback, so the tally is rolled back to its pre-walk state first and
/// each mismatched occurrence is counted exactly once.
fn encode_variant_slot(
    source: &VariantSource,
    value: &WireValue<'_>,
    ctx: &mut ValidateCtx<'_>,
    field_number: u32,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let VariantSource::Message(plan) = source else {
        return encode_variant(source, value);
    };
    if ctx.on_mismatch == OnWireTypeMismatch::DropRow {
        return encode_variant(source, value);
    }
    let saved = ctx.path.len();
    ctx.path.push(field_number);
    let result = {
        let skips = &mut *ctx.skips;
        let path = &mut *ctx.path;
        // Snapshot before the walk: on fallback the sanitization re-walk is
        // the authoritative skip recorder, and the tally must be rolled back
        // so each mismatched occurrence counts exactly once.
        let snapshot = skips.snapshot();
        match value {
            WireValue::Len(bytes) => {
                // Block-scoped so the report's tally borrow ends before the
                // fallback restores the tally below.
                let direct = {
                    let mut report = Some(SkipReport {
                        path,
                        sink: &mut |path, number, expected, actual| {
                            skips.record(path, number, expected, actual)
                        },
                    });
                    plan.encode_direct(bytes, &mut report)
                };
                match direct {
                    Some(built) => Ok(built),
                    None => {
                        // The abandoned walk may already have recorded skips
                        // before it bailed. Roll them back and let the
                        // sanitized re-walk — with a fresh report, so the
                        // tally borrow is not held across the restore —
                        // record them once.
                        skips.restore(snapshot);
                        let sanitized = {
                            let mut report = Some(SkipReport {
                                path,
                                sink: &mut |path, number, expected, actual| {
                                    skips.record(path, number, expected, actual)
                                },
                            });
                            plan.sanitize_for_reference(value, &mut report)
                        };
                        match sanitized {
                            // A failed encode either drops the row or (a Spark
                            // `required` miss) retries it merged, and that
                            // revalidation re-records these skips, so they are
                            // discarded here.
                            Some(clean) => {
                                let result = plan.encode_reference(&WireValue::Len(&clean));
                                if result.is_err() {
                                    skips.restore(snapshot);
                                }
                                result
                            }
                            // Malformed input: the row drops below; discard
                            // the sanitized walk's partial skips too and let
                            // the reference decoder produce its error from
                            // the original bytes.
                            None => {
                                let result = plan.encode_reference(value);
                                skips.restore(snapshot);
                                result
                            }
                        }
                    }
                }
            }
            _ => plan.encode_reference(value),
        }
    };
    // The walker keeps the path balanced on its own error paths; truncating
    // to the pre-walk depth also covers any leak cheaply.
    ctx.path.truncate(saved);
    result
}

/// VARIANT values of one row, built by [`validate_message`] and consumed by
/// [`scan_message`]. Both walk the same wire bytes against the same plan, so
/// the scan meets VARIANT slots in exactly the order the pre-pass pushed them;
/// building once instead of in both passes halves the dominant cost of
/// VARIANT columns.
#[derive(Default)]
pub(super) struct BuiltVariants {
    values: Vec<(Vec<u8>, Vec<u8>)>,
    next: usize,
}

impl BuiltVariants {
    /// Reset for the next row, keeping the allocation.
    pub(super) fn clear(&mut self) {
        self.values.clear();
        self.next = 0;
    }

    fn push(&mut self, built: (Vec<u8>, Vec<u8>)) {
        self.values.push(built);
    }

    fn take(&mut self) -> Result<(Vec<u8>, Vec<u8>)> {
        let built = self.values.get_mut(self.next).map(std::mem::take).ok_or(
            WireToArrowError::PlanBuilderMismatch {
                site: "scan_message:variant_not_prebuilt",
            },
        )?;
        self.next += 1;
        Ok(built)
    }
}

/// Bitset for tracking which singular slots have already been seen in one
/// `validate_message` call. Inline `u128` covers plans with up to 128
/// singular Scalar/Struct slots per level — every realistic Arrow schema
/// for zerobus targets fits — so the common case is allocation-free on
/// the per-row hot path. Wider plans fall back to a heap `Vec<u64>`.
enum SeenSingular {
    Small(u128),
    Large(Vec<u64>),
}

impl SeenSingular {
    #[inline]
    fn with_capacity(slot_count: usize) -> Self {
        if slot_count <= 128 {
            Self::Small(0)
        } else {
            Self::Large(vec![0u64; slot_count.div_ceil(64)])
        }
    }

    /// Set the bit for `idx` and return whether it was already set.
    /// Used by `validate_message` to detect duplicate singular tags
    /// (`true` on the second occurrence of any Scalar/Struct slot).
    #[inline]
    fn test_and_set(&mut self, idx: usize) -> bool {
        match self {
            Self::Small(bits) => {
                let mask = 1u128 << idx;
                let already = (*bits & mask) != 0;
                *bits |= mask;
                already
            }
            Self::Large(words) => {
                let word = &mut words[idx / 64];
                let mask = 1u64 << (idx % 64);
                let already = (*word & mask) != 0;
                *word |= mask;
                already
            }
        }
    }
}
