//! Per-message wire walks.
//!
//! [`scan_message`] is the hot path — it walks one proto message's wire
//! bytes and appends decoded values into the matching Arrow column
//! builders. [`validate_message`] is the side-effect-free mirror used by
//! `WireToArrowEncoder::encode_batch` to detect malformed rows before
//! any builder is mutated, so a single poison row can be dropped without
//! poisoning the whole batch.

use zeroparser::wire::try_parse_field;

use super::append::{
    append_repeated_scalar, append_scalar_from_wire, expect_len, validate_repeated_scalar,
    validate_scalar_from_wire,
};
use super::builders::{self, BuilderNodeList};
use super::errors::{Result, WireToArrowError};
use super::plan::{MessagePlan, PlanSlot, SLOT_UNKNOWN};

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

        let BuilderNodeList { nodes, present } = &mut *builders;
        match &mut nodes[slot_idx] {
            builders::BuilderNode::Scalar { kind, builder } => {
                append_scalar_from_wire(*kind, &field.value, builder)?;
                present[slot_idx] = true;
            }
            builders::BuilderNode::Struct {
                sub_plan, children, ..
            } => {
                let sub_bytes = expect_len(&field.value)?;
                children.reset_present();
                scan_message(sub_plan, sub_bytes, children)?;
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
                scan_message(sub_plan, sub_bytes, children)?;
                children.finalize_row(sub_plan);
                *current_offset += 1;
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
pub(super) fn validate_message(plan: &MessagePlan, mut bytes: &[u8]) -> Result<()> {
    while !bytes.is_empty() {
        let (field, rest) = try_parse_field(bytes)?;
        bytes = rest;
        let field_number = field.field_num as usize;

        let Some(&slot_idx) = plan.slot_by_proto_field.get(field_number) else {
            continue;
        };
        if slot_idx == SLOT_UNKNOWN {
            continue;
        }
        let slot_idx = slot_idx as usize;
        let slot = &plan.slots[slot_idx];

        match slot {
            PlanSlot::Scalar(sk) => validate_scalar_from_wire(*sk, &field.value)?,
            PlanSlot::Struct(sub_plan)
            | PlanSlot::RepeatedMessage(sub_plan)
            | PlanSlot::Map(sub_plan) => {
                let sub_bytes = expect_len(&field.value)?;
                validate_message(sub_plan, sub_bytes)?;
            }
            PlanSlot::RepeatedScalar(sk) => validate_repeated_scalar(*sk, &field.value)?,
            // No proto field number ever points at an Absent slot (Absent
            // slots are Arrow columns the proto descriptor lacks), so this
            // arm is unreachable in practice. Mirror `scan_message`'s
            // fall-through and surface it as a code-bug signal.
            PlanSlot::Absent => {
                return Err(WireToArrowError::PlanBuilderMismatch {
                    site: "validate_message:absent_slot_unreachable",
                });
            }
        }
    }
    Ok(())
}
