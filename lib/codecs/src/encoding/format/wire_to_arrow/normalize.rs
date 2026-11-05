use std::collections::BTreeMap;

use prost_reflect::prost::encoding::{WireType, encode_key, encode_varint};
use zeroparser::wire::try_parse_field;

use super::append::expect_len;
use super::errors::Result;
use super::plan::{MessagePlan, PlanSlot, SLOT_UNKNOWN, VariantSource};

/// Resolve singular-field merges without losing explicitly encoded scalar
/// defaults, which a DynamicMessage roundtrip would omit for proto3 fields.
/// The caller validates the original bytes before overwritten values disappear.
pub(super) fn normalize_message(plan: &MessagePlan, mut bytes: &[u8]) -> Result<Vec<u8>> {
    let mut output = Vec::with_capacity(bytes.len());
    let mut singular: BTreeMap<usize, (u32, Vec<u8>)> = BTreeMap::new();
    let mut active_oneof = BTreeMap::new();

    while !bytes.is_empty() {
        let (field, rest) = try_parse_field(bytes)?;
        let raw = &bytes[..bytes.len() - rest.len()];
        bytes = rest;
        let field_number = field.field_num as u32;
        let oneof = plan.oneof_by_proto_field.get(&field_number);
        if let Some(&oneof) = oneof
            && let Some(previous) = active_oneof.insert(oneof, field_number)
            && previous != field_number
            && let Some(&previous_slot) = plan.slot_by_proto_field.get(previous as usize)
            && previous_slot != SLOT_UNKNOWN
        {
            singular.remove(&(previous_slot as usize));
        }
        let slot_idx = plan
            .slot_by_proto_field
            .get(field.field_num as usize)
            .copied()
            .unwrap_or(SLOT_UNKNOWN);
        if slot_idx == SLOT_UNKNOWN {
            // Hidden siblings have already cleared the old member. Replaying them would
            // recreate a conflict with the selected projected member during revalidation.
            if oneof.is_none() {
                output.extend_from_slice(raw);
            }
            continue;
        }
        let slot_idx = slot_idx as usize;
        match &plan.slots[slot_idx] {
            PlanSlot::Struct(_) | PlanSlot::Variant(VariantSource::Message(_)) => {
                singular
                    .entry(slot_idx)
                    .or_insert_with(|| (field_number, Vec::new()))
                    .1
                    .extend_from_slice(expect_len(&field.value)?);
            }
            PlanSlot::Scalar(_)
            | PlanSlot::EnumString(_)
            | PlanSlot::Variant(VariantSource::JsonText) => {
                singular.insert(slot_idx, (field_number, raw.to_vec()));
            }
            PlanSlot::RepeatedMessage(child) | PlanSlot::Map(child) => {
                let body = normalize_message(child, expect_len(&field.value)?)?;
                append_message(field_number, &body, &mut output);
            }
            _ => output.extend_from_slice(raw),
        }
    }

    for (slot_idx, (field_number, bytes)) in singular {
        match &plan.slots[slot_idx] {
            PlanSlot::Struct(child) => {
                let body = normalize_message(child, &bytes)?;
                append_message(field_number, &body, &mut output);
            }
            // Message-backed VARIANT already handles nested duplicates through
            // its reference decoder; concatenation supplies protobuf merge semantics.
            PlanSlot::Variant(VariantSource::Message(_)) => {
                append_message(field_number, &bytes, &mut output);
            }
            _ => output.extend_from_slice(&bytes),
        }
    }
    Ok(output)
}

fn append_message(field_number: u32, body: &[u8], output: &mut Vec<u8>) {
    encode_key(field_number, WireType::LengthDelimited, output);
    encode_varint(body.len() as u64, output);
    output.extend_from_slice(body);
}
