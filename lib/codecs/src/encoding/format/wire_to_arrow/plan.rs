//! Encoding plan: a tree describing how to decode a given proto message into
//! a set of Arrow column builders.
//!
//! Built once per (proto descriptor, Arrow schema) pair. Immutable after build.

use std::sync::Arc;

use arrow::datatypes::{DataType, Fields};
use prost_reflect::{Cardinality, Kind, MessageDescriptor};

use super::errors::{Result, WireToArrowError};

/// Proto wire-type codes (the low 3 bits of a tag).
///
/// The upstream `proto_parser::wire::WireType` enum is private to that crate,
/// so we redeclare the codes here for use across this module's public-API
/// surface (`ScalarKind::wire_type`, error fields, packed-scalar dispatch).
/// Keep these in sync with the proto spec: <https://protobuf.dev/programming-guides/encoding/#structure>
pub(super) const WT_VARINT: u8 = 0;
pub(super) const WT_I64: u8 = 1;
pub(super) const WT_LEN: u8 = 2;
pub(super) const WT_I32: u8 = 5;

/// Proto scalar kinds that this PoC can read off the wire and append to Arrow
/// primitive builders. Proto enums map to `Int32` (matching the zerobus sink's
/// `proto_descriptor_to_arrow_schema` convention).
#[derive(Clone, Copy, Debug)]
pub enum ScalarKind {
    Int32,
    Int64,
    UInt32,
    UInt64,
    SInt32,
    SInt64,
    Bool,
    Fixed32,
    SFixed32,
    Float,
    Fixed64,
    SFixed64,
    Double,
    String,
    Bytes,
}

impl ScalarKind {
    /// Proto wire type expected for values of this kind.
    pub fn wire_type(self) -> u8 {
        match self {
            ScalarKind::Int32
            | ScalarKind::Int64
            | ScalarKind::UInt32
            | ScalarKind::UInt64
            | ScalarKind::SInt32
            | ScalarKind::SInt64
            | ScalarKind::Bool => WT_VARINT,
            ScalarKind::Fixed32 | ScalarKind::SFixed32 | ScalarKind::Float => WT_I32,
            ScalarKind::Fixed64 | ScalarKind::SFixed64 | ScalarKind::Double => WT_I64,
            ScalarKind::String | ScalarKind::Bytes => WT_LEN,
        }
    }

    /// Map a `prost_reflect::Kind` to a `ScalarKind`. Returns `None` for Kinds
    /// that aren't scalars (Message types are handled at the plan level).
    pub fn from_proto_kind(kind: &Kind) -> Option<Self> {
        Some(match kind {
            Kind::Int32 => ScalarKind::Int32,
            Kind::Int64 => ScalarKind::Int64,
            Kind::Uint32 => ScalarKind::UInt32,
            Kind::Uint64 => ScalarKind::UInt64,
            Kind::Sint32 => ScalarKind::SInt32,
            Kind::Sint64 => ScalarKind::SInt64,
            Kind::Fixed32 => ScalarKind::Fixed32,
            Kind::Fixed64 => ScalarKind::Fixed64,
            Kind::Sfixed32 => ScalarKind::SFixed32,
            Kind::Sfixed64 => ScalarKind::SFixed64,
            Kind::Float => ScalarKind::Float,
            Kind::Double => ScalarKind::Double,
            Kind::Bool => ScalarKind::Bool,
            Kind::String => ScalarKind::String,
            Kind::Bytes => ScalarKind::Bytes,
            // Proto enums carry over as int32 on the Arrow side.
            Kind::Enum(_) => ScalarKind::Int32,
            Kind::Message(_) => return None,
        })
    }
}

/// One entry per Arrow field at this message level: describes how to route
/// wire-bytes values into the corresponding Arrow column builder.
#[derive(Debug)]
pub enum PlanSlot {
    Scalar(ScalarKind),
    Struct(Arc<MessagePlan>),
    RepeatedMessage(Arc<MessagePlan>),
    /// Repeated scalar field (e.g. `repeated int32`) -> Arrow `List<primitive>`.
    /// Handles both packed and unpacked wire encodings at scan time.
    RepeatedScalar(ScalarKind),
    /// Proto `map<K, V>` -> Arrow `Map<Struct(key, value)>`. On the wire, maps
    /// are encoded as `repeated MapEntry` where `MapEntry` is a generated
    /// message with field 1 = key and field 2 = value; we scan them the same
    /// way as `RepeatedMessage` and assemble a `MapArray` at finish time.
    Map(Arc<MessagePlan>),
    /// Arrow column has no matching proto field — always emits null (or empty
    /// list / all-null struct). Happens when the Arrow schema (from UC) has
    /// more columns than the producer's proto — typically because a field was
    /// deleted from the proto schema but UC hasn't been updated yet, or the
    /// producer is running an older version. The scanner never dispatches to
    /// these slots; `finalize_row` null-pads them for every row.
    Absent,
}

/// Plan for encoding one proto message type into a set of Arrow column builders.
#[derive(Debug)]
pub struct MessagePlan {
    /// One entry per Arrow field at this level, in schema order.
    pub(crate) slots: Vec<PlanSlot>,
    /// Reverse index: `slot_by_proto_field[proto_field_number]` is `Some(slot_idx)`
    /// for known fields, `None` for unknown fields (which get skipped). Dense
    /// vector indexed directly by proto field number — no hashing on the hot path.
    pub(crate) slot_by_proto_field: Vec<Option<u32>>,
    /// Arrow `Fields` at this level, kept for assembly of `StructArray` / `ListArray`.
    pub(crate) arrow_fields: Fields,
}

impl MessagePlan {
    /// Build a plan from a proto message descriptor and a matching Arrow `Fields`.
    ///
    /// Fields in the Arrow schema must exist (by name) in the proto descriptor.
    /// Proto fields absent from the Arrow schema are treated as unknown and will
    /// be skipped at scan time.
    ///
    /// # Self-referential proto types
    ///
    /// Proto schemas can reference themselves (e.g. `message Tree { Tree left
    /// = 1; }`), but Arrow schemas cannot carry a recursive type. Recursion
    /// in this builder terminates because we only descend into
    /// `Kind::Message(_)` fields when the Arrow target at that path is also
    /// a nested type (`Struct` / `List<Struct>` / `Map`). Arrow schemas are
    /// finite by construction (Unity Catalog schemas, for one, don't produce
    /// cyclic Arrow types), so each recursion step strictly reduces the
    /// remaining Arrow depth. Proto self-reference past the depth declared
    /// in the Arrow schema is treated as an unknown field and skipped at
    /// scan time.
    ///
    /// # Oneof
    ///
    /// Proto `oneof` is purely an annotation; on the wire each variant is a
    /// normal singular field with its own tag, and the receiver takes
    /// whichever variant appeared last in the bytes. No special handling is
    /// needed at the plan level — each variant becomes its own `PlanSlot`
    /// (Scalar / Struct / etc.) and the normal "absent slot => null"
    /// machinery produces the correct Arrow output.
    pub fn build(descriptor: &MessageDescriptor, fields: &Fields) -> Result<Self> {
        let mut slots = Vec::with_capacity(fields.len());
        let mut max_field_num = 0u32;
        // `slot_proto_numbers[i] = Some(n)` means slot i maps to proto field n;
        // `None` means slot i is `Absent` (no proto tag maps here) and is skipped
        // by the reverse-index build below.
        let mut slot_proto_numbers: Vec<Option<u32>> = Vec::with_capacity(fields.len());

        for arrow_field in fields.iter() {
            let Some(proto_field) = descriptor.get_field_by_name(arrow_field.name()) else {
                // Schema drift: the Arrow column exists but the proto doesn't
                // carry it. Log + metric + keep going — the column becomes
                // always-null. Typical cause: a field was removed from the
                // proto before the UC table schema was updated.
                tracing::warn!(
                    message = "proto descriptor is missing a field declared in the Arrow schema; \
                               the column will be emitted as all-null",
                    field = %arrow_field.name(),
                    descriptor = %descriptor.full_name(),
                );
                metrics::counter!(
                    "wire_to_arrow_missing_proto_field",
                    "field" => arrow_field.name().to_string(),
                    "descriptor" => descriptor.full_name().to_string(),
                )
                .increment(1);
                slots.push(PlanSlot::Absent);
                slot_proto_numbers.push(None);
                continue;
            };
            max_field_num = max_field_num.max(proto_field.number());
            slot_proto_numbers.push(Some(proto_field.number()));

            let is_repeated = proto_field.cardinality() == Cardinality::Repeated;
            let kind = proto_field.kind();

            // Maps take precedence: proto map fields have `is_map() == true` and
            // cardinality Repeated, but we dispatch differently from a bare
            // repeated-message field.
            let slot = if proto_field.is_map() {
                let entry_desc = match &kind {
                    Kind::Message(m) => m,
                    _ => {
                        return Err(WireToArrowError::UnsupportedCombination {
                            name: arrow_field.name().to_string(),
                            kind: format!("{kind:?}"),
                            arrow_type: format!("{:?}", arrow_field.data_type()),
                            repeated: is_repeated,
                        });
                    }
                };
                let entry_fields = match arrow_field.data_type() {
                    DataType::Map(entry_field, _keys_sorted) => match entry_field.data_type() {
                        DataType::Struct(fs) => fs,
                        other => {
                            return Err(WireToArrowError::UnsupportedCombination {
                                name: arrow_field.name().to_string(),
                                kind: format!("{kind:?}"),
                                arrow_type: format!("Map(entry_type = {other:?})"),
                                repeated: is_repeated,
                            });
                        }
                    },
                    other => {
                        return Err(WireToArrowError::UnsupportedCombination {
                            name: arrow_field.name().to_string(),
                            kind: format!("{kind:?}"),
                            arrow_type: format!("{other:?}"),
                            repeated: is_repeated,
                        });
                    }
                };
                let sub = MessagePlan::build(entry_desc, entry_fields)?;
                PlanSlot::Map(Arc::new(sub))
            } else {
                match (&kind, arrow_field.data_type(), is_repeated) {
                    // Singular scalar.
                    (_, dt, false) if !matches!(dt, DataType::Struct(_) | DataType::List(_)) => {
                        let sk = ScalarKind::from_proto_kind(&kind).ok_or_else(|| {
                            WireToArrowError::UnsupportedKind {
                                name: arrow_field.name().to_string(),
                                kind: format!("{kind:?}"),
                            }
                        })?;
                        PlanSlot::Scalar(sk)
                    }
                    // Singular nested message.
                    (Kind::Message(inner_desc), DataType::Struct(inner_fields), false) => {
                        let sub = MessagePlan::build(inner_desc, inner_fields)?;
                        PlanSlot::Struct(Arc::new(sub))
                    }
                    // Repeated nested message -> Arrow List<Struct>.
                    (Kind::Message(inner_desc), DataType::List(element_field), true) => {
                        let inner_fields = match element_field.data_type() {
                            DataType::Struct(fs) => fs,
                            other => {
                                return Err(WireToArrowError::RepeatedNonStructList {
                                    name: arrow_field.name().to_string(),
                                    element: format!("{other:?}"),
                                });
                            }
                        };
                        let sub = MessagePlan::build(inner_desc, inner_fields)?;
                        PlanSlot::RepeatedMessage(Arc::new(sub))
                    }
                    // Repeated scalar -> Arrow List<primitive>.
                    (_, DataType::List(_), true) => {
                        let sk = ScalarKind::from_proto_kind(&kind).ok_or_else(|| {
                            WireToArrowError::UnsupportedKind {
                                name: arrow_field.name().to_string(),
                                kind: format!("{kind:?}"),
                            }
                        })?;
                        PlanSlot::RepeatedScalar(sk)
                    }
                    (k, dt, r) => {
                        return Err(WireToArrowError::UnsupportedCombination {
                            name: arrow_field.name().to_string(),
                            kind: format!("{k:?}"),
                            arrow_type: format!("{dt:?}"),
                            repeated: r,
                        });
                    }
                }
            };
            slots.push(slot);
        }

        let mut slot_by_proto_field = vec![None; (max_field_num as usize) + 1];
        for (slot_idx, pn) in slot_proto_numbers.iter().enumerate() {
            if let Some(pn) = pn {
                slot_by_proto_field[*pn as usize] = Some(slot_idx as u32);
            }
        }

        // Opposite-direction drift: proto fields the Arrow schema doesn't
        // carry. These would be silently skipped at scan time (matching
        // proto's standard "ignore unknown fields" behavior), but if the
        // descriptor reflects the current producer schema, it signals
        // "producer emits this field but UC hasn't caught up." Log + count
        // once at plan build so operators notice.
        let arrow_field_names: std::collections::HashSet<&str> =
            fields.iter().map(|f| f.name().as_str()).collect();
        for proto_field in descriptor.fields() {
            if !arrow_field_names.contains(proto_field.name()) {
                tracing::warn!(
                    message = "proto descriptor has a field not declared in the Arrow schema; \
                               occurrences on the wire will be silently skipped",
                    field = %proto_field.name(),
                    descriptor = %descriptor.full_name(),
                );
                metrics::counter!(
                    "wire_to_arrow_extra_proto_field",
                    "field" => proto_field.name().to_string(),
                    "descriptor" => descriptor.full_name().to_string(),
                )
                .increment(1);
            }
        }

        Ok(MessagePlan {
            slots,
            slot_by_proto_field,
            arrow_fields: fields.clone(),
        })
    }

    /// Build a plan whose slots are all `Absent`. Used by the builder layer to
    /// shape a null-filled sub-tree when an outer Arrow Struct / List / Map
    /// column is itself `Absent` (so every nested child has to null-pad per row).
    pub(crate) fn all_absent(fields: &Fields) -> Self {
        let slots = (0..fields.len()).map(|_| PlanSlot::Absent).collect();
        MessagePlan {
            slots,
            slot_by_proto_field: Vec::new(),
            arrow_fields: fields.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use prost_reflect::DescriptorPool;
    use std::path::PathBuf;

    fn load_person_descriptor() -> MessageDescriptor {
        let desc_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data/protobuf/protos/test_protobuf.desc");
        let bytes = std::fs::read(&desc_path).expect("read desc");
        DescriptorPool::decode(bytes.as_slice())
            .expect("decode pool")
            .get_message_by_name("test_protobuf.Person")
            .expect("Person descriptor")
    }

    #[test]
    fn build_scalar_plan() {
        let desc = load_person_descriptor();
        let schema = Schema::new(vec![
            Field::new("name", DataType::LargeUtf8, true),
            Field::new("id", DataType::Int32, true),
            Field::new("email", DataType::LargeUtf8, true),
        ]);
        let plan = MessagePlan::build(&desc, &Fields::from(schema.fields().clone())).unwrap();
        assert_eq!(plan.slots.len(), 3);
        assert!(matches!(plan.slots[0], PlanSlot::Scalar(ScalarKind::String)));
        assert!(matches!(plan.slots[1], PlanSlot::Scalar(ScalarKind::Int32)));
        assert!(matches!(plan.slots[2], PlanSlot::Scalar(ScalarKind::String)));
    }

    #[test]
    fn missing_proto_field_yields_absent_slot() {
        // Schema-drift tolerance: if the Arrow schema declares a column the
        // proto descriptor doesn't carry, the plan builder logs + increments
        // a metric and emits a `PlanSlot::Absent` so the column comes out as
        // all-null rather than failing the batch. Typical cause: a field was
        // removed from the proto but the UC table still has the column.
        let desc = load_person_descriptor();
        let schema = Schema::new(vec![
            Field::new("name", DataType::LargeUtf8, true),
            Field::new("deleted_in_proto", DataType::Int32, true),
            Field::new("id", DataType::Int32, true),
        ]);
        let plan = MessagePlan::build(&desc, &Fields::from(schema.fields().clone())).unwrap();
        assert!(matches!(plan.slots[0], PlanSlot::Scalar(ScalarKind::String)));
        assert!(matches!(plan.slots[1], PlanSlot::Absent));
        assert!(matches!(plan.slots[2], PlanSlot::Scalar(ScalarKind::Int32)));
        // No proto tag for slot 1 — so the reverse index never points at it.
        assert!(
            plan.slot_by_proto_field
                .iter()
                .all(|entry| *entry != Some(1))
        );
    }

    #[test]
    fn unsupported_combination_flagged() {
        // Person.id is a scalar int32. If we claim it's a Struct in Arrow,
        // the plan builder should reject.
        let desc = load_person_descriptor();
        let schema = Schema::new(vec![Field::new(
            "id",
            DataType::Struct(Fields::from(vec![Field::new("x", DataType::Int32, true)])),
            true,
        )]);
        let err = MessagePlan::build(&desc, &Fields::from(schema.fields().clone()))
            .expect_err("should fail");
        assert!(matches!(err, WireToArrowError::UnsupportedCombination { .. }));
    }

    #[test]
    fn wire_type_for_scalars() {
        assert_eq!(ScalarKind::Int32.wire_type(), WT_VARINT);
        assert_eq!(ScalarKind::String.wire_type(), WT_LEN);
        assert_eq!(ScalarKind::Double.wire_type(), WT_I64);
        assert_eq!(ScalarKind::Float.wire_type(), WT_I32);
    }
}
