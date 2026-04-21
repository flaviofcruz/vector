//! Streaming wire-format to Arrow encoder for the `databricks_zerobus` sink.
//!
//! Parses proto wire bytes in a single pass and appends values directly into
//! Arrow `RecordBatch` column builders, skipping the `DynamicMessage` /
//! `LogEvent` intermediate representations used by the generic
//! `ProtobufDeserializer` + `ArrowStreamSerializer` path.
//!
//! ## Supported today
//!
//! - Scalar proto fields (int32/int64/uint32/uint64/sint32/sint64/fixed*/float/double/bool/string/bytes/enum).
//! - Singular nested messages -> Arrow `Struct`.
//! - Repeated nested messages -> Arrow `List<Struct>`.
//!
//! ## Not yet supported
//!
//! - Repeated scalars (packed or unpacked)
//! - Maps (proto `map<k, v>` / Arrow `Map`)
//! - Oneof
//! - Self-referential message types (plan building would stack-overflow)
//! - `unsafe from_utf8_unchecked` string fast-path
//!
//! ## Typical use
//!
//! ```ignore
//! let encoder = WireToArrowEncoder::new(&descriptor, arrow_schema)?;
//! let record_batch = encoder.encode_batch(&wire_bytes_per_row)?;
//! ```
//!
//! Benchmarks live at `benches/codecs/wire_to_arrow_bench.rs`; see the bench
//! for head-to-head comparison against the reference encoder chain.

mod builders;
mod errors;
mod plan;
mod scan;

use std::sync::Arc;

use arrow::datatypes::{Fields, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use prost_reflect::MessageDescriptor;

pub use errors::WireToArrowError;

use builders::{BuilderNodeList, TypedBuilder};
use errors::Result;
use plan::{MessagePlan, PlanSlot, ScalarKind};
use scan::{decode_varint, read_fixed32, read_fixed64, skip_field, zigzag32, zigzag64};

/// Streaming wire-format encoder. Build once per (proto message type,
/// Arrow schema) pair, then call [`WireToArrowEncoder::encode_batch`]
/// repeatedly.
pub struct WireToArrowEncoder {
    plan: Arc<MessagePlan>,
    schema: Arc<Schema>,
}

impl WireToArrowEncoder {
    /// Compile a plan for the given proto descriptor + Arrow schema.
    ///
    /// Every field in `schema` must exist (by name) in `descriptor`. Proto
    /// fields absent from `schema` are silently skipped at scan time.
    pub fn new(descriptor: &MessageDescriptor, schema: Schema) -> Result<Self> {
        let plan = MessagePlan::build(descriptor, &Fields::from(schema.fields().clone()))?;
        Ok(Self {
            plan: Arc::new(plan),
            schema: Arc::new(schema),
        })
    }

    /// Encode a batch of serialized proto messages into a single `RecordBatch`.
    pub fn encode_batch(&self, messages: &[Bytes]) -> Result<RecordBatch> {
        let capacity = messages.len();
        let mut builders = BuilderNodeList::with_capacity(&self.plan, capacity);
        let mut present = vec![false; self.plan.slots.len()];

        for msg_bytes in messages {
            present.iter_mut().for_each(|p| *p = false);
            scan_message(&self.plan, msg_bytes, &mut builders, &mut present)?;
            builders.finalize_row(&self.plan, &present);
        }

        let arrays = builders.finish(&self.plan)?;
        RecordBatch::try_new(Arc::clone(&self.schema), arrays)
            .map_err(|source| WireToArrowError::RecordBatchAssembly { source })
    }
}

/// Scan one proto message's wire bytes, appending values into `builders`.
/// `present[i]` is set to `true` if slot `i` was touched by any tag in this
/// message.
fn scan_message(
    plan: &MessagePlan,
    bytes: &[u8],
    builders: &mut BuilderNodeList,
    present: &mut [bool],
) -> Result<()> {
    let mut pos = 0usize;
    while pos < bytes.len() {
        let tag = decode_varint(bytes, &mut pos)?;
        let field_number = (tag >> 3) as usize;
        let wire_type = (tag & 0x7) as u8;

        let slot_idx = plan.slot_by_proto_field.get(field_number).and_then(|s| *s);
        let slot_idx = match slot_idx {
            None => {
                skip_field(wire_type, bytes, &mut pos)?;
                continue;
            }
            Some(idx) => idx as usize,
        };

        let slot = &plan.slots[slot_idx];
        let node = &mut builders.nodes[slot_idx];

        match (slot, node) {
            (PlanSlot::Scalar(sk), builders::BuilderNode::Scalar(tb)) => {
                if wire_type != sk.wire_type() {
                    return Err(WireToArrowError::WireTypeMismatch {
                        expected: sk.wire_type(),
                        actual: wire_type,
                    });
                }
                append_scalar(*sk, bytes, &mut pos, tb)?;
                present[slot_idx] = true;
            }
            (PlanSlot::Struct(sub_plan), builders::BuilderNode::Struct { children, .. }) => {
                if wire_type != 2 {
                    return Err(WireToArrowError::WireTypeMismatch {
                        expected: 2,
                        actual: wire_type,
                    });
                }
                let len = decode_varint(bytes, &mut pos)? as usize;
                if pos + len > bytes.len() {
                    return Err(WireToArrowError::UnexpectedEof);
                }
                let sub_bytes = &bytes[pos..pos + len];
                pos += len;
                let mut sub_present = vec![false; sub_plan.slots.len()];
                scan_message(sub_plan, sub_bytes, children, &mut sub_present)?;
                children.finalize_row(sub_plan, &sub_present);
                present[slot_idx] = true;
            }
            (
                PlanSlot::RepeatedMessage(sub_plan),
                builders::BuilderNode::RepeatedMessage {
                    children,
                    current_offset,
                    ..
                },
            ) => {
                if wire_type != 2 {
                    return Err(WireToArrowError::WireTypeMismatch {
                        expected: 2,
                        actual: wire_type,
                    });
                }
                let len = decode_varint(bytes, &mut pos)? as usize;
                if pos + len > bytes.len() {
                    return Err(WireToArrowError::UnexpectedEof);
                }
                let sub_bytes = &bytes[pos..pos + len];
                pos += len;
                let mut sub_present = vec![false; sub_plan.slots.len()];
                scan_message(sub_plan, sub_bytes, children, &mut sub_present)?;
                children.finalize_row(sub_plan, &sub_present);
                *current_offset += 1;
                present[slot_idx] = true;
            }
            _ => return Err(WireToArrowError::PlanBuilderMismatch),
        }
    }
    Ok(())
}

/// Append one scalar value from `bytes` (starting at `pos`) into `tb`.
fn append_scalar(
    kind: ScalarKind,
    bytes: &[u8],
    pos: &mut usize,
    tb: &mut TypedBuilder,
) -> Result<()> {
    match (kind, tb) {
        (ScalarKind::Int32, TypedBuilder::Int32(b)) => {
            b.append_value(decode_varint(bytes, pos)? as i32)
        }
        (ScalarKind::Int64, TypedBuilder::Int64(b)) => {
            b.append_value(decode_varint(bytes, pos)? as i64)
        }
        (ScalarKind::UInt32, TypedBuilder::UInt32(b)) => {
            b.append_value(decode_varint(bytes, pos)? as u32)
        }
        (ScalarKind::UInt64, TypedBuilder::UInt64(b)) => {
            b.append_value(decode_varint(bytes, pos)?)
        }
        (ScalarKind::SInt32, TypedBuilder::Int32(b)) => {
            b.append_value(zigzag32(decode_varint(bytes, pos)? as u32))
        }
        (ScalarKind::SInt64, TypedBuilder::Int64(b)) => {
            b.append_value(zigzag64(decode_varint(bytes, pos)?))
        }
        (ScalarKind::Fixed32, TypedBuilder::UInt32(b)) => b.append_value(read_fixed32(bytes, pos)?),
        (ScalarKind::SFixed32, TypedBuilder::Int32(b)) => {
            b.append_value(read_fixed32(bytes, pos)? as i32)
        }
        (ScalarKind::Float, TypedBuilder::Float32(b)) => {
            b.append_value(f32::from_bits(read_fixed32(bytes, pos)?))
        }
        (ScalarKind::Fixed64, TypedBuilder::UInt64(b)) => b.append_value(read_fixed64(bytes, pos)?),
        (ScalarKind::SFixed64, TypedBuilder::Int64(b)) => {
            b.append_value(read_fixed64(bytes, pos)? as i64)
        }
        (ScalarKind::Double, TypedBuilder::Float64(b)) => {
            b.append_value(f64::from_bits(read_fixed64(bytes, pos)?))
        }
        (ScalarKind::Bool, TypedBuilder::Boolean(b)) => {
            b.append_value(decode_varint(bytes, pos)? != 0)
        }
        (ScalarKind::String, TypedBuilder::LargeUtf8(b)) => {
            let len = decode_varint(bytes, pos)? as usize;
            if *pos + len > bytes.len() {
                return Err(WireToArrowError::UnexpectedEof);
            }
            let s = std::str::from_utf8(&bytes[*pos..*pos + len])
                .map_err(|_| WireToArrowError::InvalidUtf8)?;
            b.append_value(s);
            *pos += len;
        }
        (ScalarKind::Bytes, TypedBuilder::LargeBinary(b)) => {
            let len = decode_varint(bytes, pos)? as usize;
            if *pos + len > bytes.len() {
                return Err(WireToArrowError::UnexpectedEof);
            }
            b.append_value(&bytes[*pos..*pos + len]);
            *pos += len;
        }
        _ => return Err(WireToArrowError::PlanBuilderMismatch),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::{DataType, Field};
    use prost_reflect::prost::Message as _;
    use prost_reflect::{DescriptorPool, DynamicMessage, Value as ProtoValue};
    use std::path::PathBuf;

    fn descriptor_pool(file: &str) -> DescriptorPool {
        let desc_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("lib/codecs/tests/data/protobuf/protos")
            .join(file);
        let bytes = std::fs::read(&desc_path).unwrap();
        DescriptorPool::decode(bytes.as_slice()).unwrap()
    }

    fn scalar_descriptor() -> MessageDescriptor {
        descriptor_pool("test_protobuf.desc")
            .get_message_by_name("test_protobuf.Person")
            .unwrap()
    }

    #[test]
    fn scalar_roundtrip() {
        let desc = scalar_descriptor();
        let schema = Schema::new(vec![
            Field::new("name", DataType::LargeUtf8, true),
            Field::new("id", DataType::Int32, true),
            Field::new("email", DataType::LargeUtf8, true),
        ]);
        let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

        let mut msg = DynamicMessage::new(desc.clone());
        msg.set_field_by_name("name", ProtoValue::String("Alice".into()));
        msg.set_field_by_name("id", ProtoValue::I32(42));
        msg.set_field_by_name("email", ProtoValue::String("alice@x.com".into()));
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();

        let batch = enc.encode_batch(&[Bytes::from(buf)]).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(
            batch.column(0).as_string::<i64>().value(0),
            "Alice"
        );
        assert_eq!(
            batch.column(1).as_primitive::<arrow::datatypes::Int32Type>().value(0),
            42
        );
    }

    #[test]
    fn absent_fields_are_null() {
        let desc = scalar_descriptor();
        let schema = Schema::new(vec![
            Field::new("name", DataType::LargeUtf8, true),
            Field::new("id", DataType::Int32, true),
            Field::new("email", DataType::LargeUtf8, true),
        ]);
        let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

        // Populate only `name`; `id` and `email` should show up as null.
        let mut msg = DynamicMessage::new(desc.clone());
        msg.set_field_by_name("name", ProtoValue::String("only name".into()));
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();

        let batch = enc.encode_batch(&[Bytes::from(buf)]).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(!batch.column(0).is_null(0));
        assert!(batch.column(1).is_null(0), "id should be null");
        assert!(batch.column(2).is_null(0), "email should be null");
    }

    #[test]
    fn unknown_wire_fields_are_skipped() {
        // Person.phones (field 4 in proto2 test_protobuf.Person) is not in
        // our Arrow schema but will appear in wire bytes when populated. The
        // encoder should skip it cleanly.
        let desc = scalar_descriptor();
        let schema = Schema::new(vec![
            Field::new("name", DataType::LargeUtf8, true),
            Field::new("id", DataType::Int32, true),
            Field::new("email", DataType::LargeUtf8, true),
        ]);
        let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

        let phone_desc = desc
            .get_field_by_name("phones")
            .unwrap()
            .kind()
            .as_message()
            .unwrap()
            .clone();
        let mut phone = DynamicMessage::new(phone_desc);
        phone.set_field_by_name("number", ProtoValue::String("555-0000".into()));

        let mut msg = DynamicMessage::new(desc.clone());
        msg.set_field_by_name("name", ProtoValue::String("Alice".into()));
        msg.set_field_by_name("id", ProtoValue::I32(1));
        msg.set_field_by_name(
            "phones",
            ProtoValue::List(vec![ProtoValue::Message(phone)]),
        );
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();

        // Should not error — unknown fields get skipped.
        let batch = enc.encode_batch(&[Bytes::from(buf)]).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch.column(0).as_string::<i64>().value(0),
            "Alice"
        );
    }

    #[test]
    fn multiple_rows_preserve_order() {
        let desc = scalar_descriptor();
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, true)]);
        let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

        let mut messages = Vec::new();
        for i in 0..5 {
            let mut msg = DynamicMessage::new(desc.clone());
            msg.set_field_by_name("id", ProtoValue::I32(i * 10));
            let mut buf = Vec::new();
            msg.encode(&mut buf).unwrap();
            messages.push(Bytes::from(buf));
        }

        let batch = enc.encode_batch(&messages).unwrap();
        let ids = batch.column(0).as_primitive::<arrow::datatypes::Int32Type>();
        assert_eq!(ids.len(), 5);
        for i in 0..5 {
            assert_eq!(ids.value(i), (i as i32) * 10);
        }
    }
}
