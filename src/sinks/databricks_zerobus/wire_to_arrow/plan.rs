//! Encoding plan: a tree describing how to decode a given proto message into
//! a set of Arrow column builders.
//!
//! Built once per (proto descriptor, Arrow schema) pair. Immutable after build.

use std::sync::Arc;

use arrow::datatypes::{DataType, Fields};
use prost_reflect::{Cardinality, Kind, MessageDescriptor};

use super::errors::{Result, WireToArrowError};

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
            | ScalarKind::Bool => 0,
            ScalarKind::Fixed32 | ScalarKind::SFixed32 | ScalarKind::Float => 5,
            ScalarKind::Fixed64 | ScalarKind::SFixed64 | ScalarKind::Double => 1,
            ScalarKind::String | ScalarKind::Bytes => 2,
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
    pub fn build(descriptor: &MessageDescriptor, fields: &Fields) -> Result<Self> {
        let mut slots = Vec::with_capacity(fields.len());
        let mut max_field_num = 0u32;
        let mut slot_proto_numbers: Vec<u32> = Vec::with_capacity(fields.len());

        for arrow_field in fields.iter() {
            let proto_field = descriptor.get_field_by_name(arrow_field.name()).ok_or_else(
                || WireToArrowError::MissingProtoField {
                    name: arrow_field.name().to_string(),
                },
            )?;
            max_field_num = max_field_num.max(proto_field.number());
            slot_proto_numbers.push(proto_field.number());

            let is_repeated = proto_field.cardinality() == Cardinality::Repeated;
            let kind = proto_field.kind();

            let slot = match (&kind, arrow_field.data_type(), is_repeated) {
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
                (k, dt, r) => {
                    return Err(WireToArrowError::UnsupportedCombination {
                        name: arrow_field.name().to_string(),
                        kind: format!("{k:?}"),
                        arrow_type: format!("{dt:?}"),
                        repeated: r,
                    });
                }
            };
            slots.push(slot);
        }

        let mut slot_by_proto_field = vec![None; (max_field_num as usize) + 1];
        for (slot_idx, pn) in slot_proto_numbers.iter().enumerate() {
            slot_by_proto_field[*pn as usize] = Some(slot_idx as u32);
        }

        Ok(MessagePlan {
            slots,
            slot_by_proto_field,
            arrow_fields: fields.clone(),
        })
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
            .join("lib/codecs/tests/data/protobuf/protos/test_protobuf.desc");
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
    fn missing_proto_field_errors() {
        let desc = load_person_descriptor();
        let schema = Schema::new(vec![Field::new("not_a_field", DataType::Int32, true)]);
        let err = MessagePlan::build(&desc, &Fields::from(schema.fields().clone()))
            .expect_err("should fail");
        assert!(matches!(
            err,
            WireToArrowError::MissingProtoField { name } if name == "not_a_field"
        ));
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
        assert_eq!(ScalarKind::Int32.wire_type(), 0);
        assert_eq!(ScalarKind::String.wire_type(), 2);
        assert_eq!(ScalarKind::Double.wire_type(), 1);
        assert_eq!(ScalarKind::Float.wire_type(), 5);
    }
}
