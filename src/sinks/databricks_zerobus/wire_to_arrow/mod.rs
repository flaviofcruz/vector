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
use vector_lib::event::{Event, Value};

pub use errors::WireToArrowError;

use builders::{BuilderNodeList, TypedBuilder};
use errors::Result;
use plan::{MessagePlan, PlanSlot, ScalarKind};
use scan::{decode_varint, read_fixed32, read_fixed64, skip_field, zigzag32, zigzag64};

/// Log field name where upstream writers stash the original proto wire bytes.
///
/// Events that carry this field with a `Value::Bytes` value are eligible for
/// the [`WireToArrowEncoder`] fast path. The sink falls back to the generic
/// encoder chain when the field is absent or not `Bytes`-typed.
///
/// Upstream (VRL on VA) must preserve the original proto wire bytes in this
/// field for tables that want the wire-to-Arrow optimization. Details in the
/// sink-integration doc.
pub const WIRE_BYTES_FIELD: &str = "_proto_wire_bytes";

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

    /// Try to pull proto wire bytes from [`WIRE_BYTES_FIELD`] on each event.
    ///
    /// Returns `Some(Vec<Bytes>)` only when every event carries the field as
    /// a `Value::Bytes`. If any event is missing the field or has it under a
    /// non-bytes type, returns `None` — the caller should fall back to the
    /// generic encoder path.
    ///
    /// This is a stateless helper; it doesn't touch `self`. Provided on the
    /// encoder type for convenient grouping.
    pub fn try_extract_wire_bytes(events: &[Event]) -> Option<Vec<Bytes>> {
        let mut out = Vec::with_capacity(events.len());
        for event in events {
            let log = event.as_log();
            match log.get(WIRE_BYTES_FIELD) {
                Some(Value::Bytes(b)) => out.push(b.clone()),
                _ => return None,
            }
        }
        Some(out)
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
            )
            | (
                PlanSlot::Map(sub_plan),
                builders::BuilderNode::Map {
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
            (
                PlanSlot::RepeatedScalar(sk),
                builders::BuilderNode::RepeatedScalar {
                    values,
                    current_offset,
                    ..
                },
            ) => {
                // Two wire encodings are possible for repeated scalars:
                //
                // * Unpacked — wire_type matches the scalar's native type, one
                //   value per tag occurrence. Always used for strings/bytes
                //   (whose native wire type is already 2), optional for others.
                // * Packed   — wire_type 2 with a length-delimited blob holding
                //   a run of concatenated scalar values of the same kind. Only
                //   valid for scalars whose native wire type is 0/1/5.
                let native_wt = sk.wire_type();
                if wire_type == native_wt {
                    append_scalar(*sk, bytes, &mut pos, values)?;
                    *current_offset += 1;
                } else if wire_type == 2 && native_wt != 2 {
                    let len = decode_varint(bytes, &mut pos)? as usize;
                    if pos + len > bytes.len() {
                        return Err(WireToArrowError::UnexpectedEof);
                    }
                    let end = pos + len;
                    while pos < end {
                        append_scalar(*sk, bytes, &mut pos, values)?;
                        *current_offset += 1;
                    }
                    if pos != end {
                        return Err(WireToArrowError::UnexpectedEof);
                    }
                } else {
                    return Err(WireToArrowError::WireTypeMismatch {
                        expected: native_wt,
                        actual: wire_type,
                    });
                }
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
        // `int64` -> `Timestamp(Microsecond, _)` coercion. Proto carries the
        // value as a plain varint; the Arrow column interprets it as
        // microseconds since Unix epoch. Used primarily for `_event_time` on
        // LP tables (matching `proto_descriptor_to_arrow_schema`).
        (ScalarKind::Int64, TypedBuilder::TimestampMicros(b)) => {
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
        (ScalarKind::SInt64, TypedBuilder::TimestampMicros(b)) => {
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
        (ScalarKind::SFixed64, TypedBuilder::TimestampMicros(b)) => {
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
    use arrow::datatypes::{DataType, Field, Fields as ArrowFields};
    use prost_reflect::prost::Message as _;
    use prost_reflect::prost_types::field_descriptor_proto::{Label, Type as ProtoType};
    use prost_reflect::prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        OneofDescriptorProto,
    };
    use prost_reflect::{DescriptorPool, DynamicMessage, Value as ProtoValue};
    use std::path::PathBuf;
    use std::sync::Arc;

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

    fn rich_descriptor() -> MessageDescriptor {
        descriptor_pool("test_protobuf3.desc")
            .get_message_by_name("test_protobuf3.Person")
            .unwrap()
    }

    /// Build an ad-hoc `message Bag { repeated int32 numbers = 1; }` descriptor
    /// programmatically, since none of the checked-in test protos have a bare
    /// repeated scalar field.
    fn repeated_int32_descriptor() -> MessageDescriptor {
        let fd = FileDescriptorProto {
            name: Some("wire_to_arrow_poc_test.proto".into()),
            package: Some("wire_to_arrow_poc_test".into()),
            message_type: vec![DescriptorProto {
                name: Some("Bag".into()),
                field: vec![FieldDescriptorProto {
                    name: Some("numbers".into()),
                    number: Some(1),
                    label: Some(Label::Repeated as i32),
                    r#type: Some(ProtoType::Int32 as i32),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let set = FileDescriptorSet { file: vec![fd] };
        let mut bytes = Vec::new();
        set.encode(&mut bytes).unwrap();
        DescriptorPool::decode(bytes.as_slice())
            .unwrap()
            .get_message_by_name("wire_to_arrow_poc_test.Bag")
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
    fn extract_wire_bytes_all_present() {
        let mut e1 = Event::from(vector_lib::event::LogEvent::default());
        e1.as_mut_log()
            .insert(WIRE_BYTES_FIELD, Bytes::from_static(b"one"));
        let mut e2 = Event::from(vector_lib::event::LogEvent::default());
        e2.as_mut_log()
            .insert(WIRE_BYTES_FIELD, Bytes::from_static(b"two"));

        let extracted = WireToArrowEncoder::try_extract_wire_bytes(&[e1, e2]);
        assert!(extracted.is_some());
        let bytes = extracted.unwrap();
        assert_eq!(bytes.len(), 2);
        assert_eq!(&bytes[0][..], b"one");
        assert_eq!(&bytes[1][..], b"two");
    }

    #[test]
    fn extract_wire_bytes_missing_field_returns_none() {
        let mut e1 = Event::from(vector_lib::event::LogEvent::default());
        e1.as_mut_log()
            .insert(WIRE_BYTES_FIELD, Bytes::from_static(b"one"));
        // e2 has no _proto_wire_bytes field.
        let e2 = Event::from(vector_lib::event::LogEvent::default());

        assert!(WireToArrowEncoder::try_extract_wire_bytes(&[e1, e2]).is_none());
    }

    #[test]
    fn extract_wire_bytes_wrong_type_returns_none() {
        // Vector's `Value` represents plain strings as `Value::Bytes`, so a
        // string IS a bytes value here — use an integer to get a non-bytes
        // variant for the negative case.
        let mut e1 = Event::from(vector_lib::event::LogEvent::default());
        e1.as_mut_log().insert(WIRE_BYTES_FIELD, 42_i64);
        assert!(WireToArrowEncoder::try_extract_wire_bytes(&[e1]).is_none());
    }

    #[test]
    fn encode_from_extracted_matches_direct_encode() {
        // End-to-end: build events with wire bytes in the field, extract,
        // encode, compare against encoding the bytes directly.
        let desc = scalar_descriptor();
        let schema = Schema::new(vec![
            Field::new("name", DataType::LargeUtf8, true),
            Field::new("id", DataType::Int32, true),
            Field::new("email", DataType::LargeUtf8, true),
        ]);
        let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

        let mut wire_bytes_list = Vec::new();
        let mut events = Vec::new();
        for i in 0..5_i32 {
            let mut msg = DynamicMessage::new(desc.clone());
            msg.set_field_by_name("name", ProtoValue::String(format!("n-{i}")));
            msg.set_field_by_name("id", ProtoValue::I32(i));
            let mut buf = Vec::new();
            msg.encode(&mut buf).unwrap();
            let bytes = Bytes::from(buf);
            wire_bytes_list.push(bytes.clone());

            let mut e = Event::from(vector_lib::event::LogEvent::default());
            e.as_mut_log().insert(WIRE_BYTES_FIELD, bytes);
            events.push(e);
        }

        let extracted = WireToArrowEncoder::try_extract_wire_bytes(&events).unwrap();
        let via_events = enc.encode_batch(&extracted).unwrap();
        let via_bytes = enc.encode_batch(&wire_bytes_list).unwrap();

        assert_eq!(via_events.num_rows(), via_bytes.num_rows());
        assert_eq!(via_events.num_columns(), via_bytes.num_columns());
        for i in 0..via_events.num_columns() {
            assert_eq!(via_events.column(i).as_ref(), via_bytes.column(i).as_ref());
        }
    }

    #[test]
    fn repeated_scalar_unpacked_roundtrip() {
        // Unpacked: emit each element with its own tag. For proto3, this is
        // the default for non-packed repeated scalars when the writer chooses
        // not to pack (which can happen for proto2 as well).
        let desc = repeated_int32_descriptor();
        let schema = Schema::new(vec![Field::new(
            "numbers",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )]);
        let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

        // Hand-craft wire bytes for two rows:
        //   row 0: numbers = [1, 2, 3] (unpacked: 3 tag+value pairs)
        //   row 1: numbers = [42]      (single tag+value)
        let tag = (1u8 << 3) | 0; // field 1, wire type 0 (varint)
        let mut row0 = Vec::new();
        for v in [1i32, 2, 3] {
            row0.push(tag);
            encode_varint_into(&mut row0, v as u64);
        }
        let mut row1 = Vec::new();
        row1.push(tag);
        encode_varint_into(&mut row1, 42);

        let batch = enc
            .encode_batch(&[Bytes::from(row0), Bytes::from(row1)])
            .unwrap();
        assert_eq!(batch.num_rows(), 2);
        let list = batch.column(0).as_list::<i32>();
        assert_eq!(list.value_length(0), 3);
        assert_eq!(list.value_length(1), 1);
        let values = list
            .values()
            .as_primitive::<arrow::datatypes::Int32Type>();
        assert_eq!(values.len(), 4);
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), 2);
        assert_eq!(values.value(2), 3);
        assert_eq!(values.value(3), 42);
    }

    #[test]
    fn repeated_scalar_packed_roundtrip() {
        // Packed: one length-delimited blob with concatenated varints. proto3
        // repeated scalars default to this encoding.
        let desc = repeated_int32_descriptor();
        let schema = Schema::new(vec![Field::new(
            "numbers",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )]);
        let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

        // Tag for field 1 with wire_type 2 (length-delimited).
        let tag = (1u8 << 3) | 2;
        let mut payload = Vec::new();
        for v in [10i32, 20, 30, 40] {
            encode_varint_into(&mut payload, v as u64);
        }
        let mut row = Vec::new();
        row.push(tag);
        encode_varint_into(&mut row, payload.len() as u64);
        row.extend_from_slice(&payload);

        let batch = enc.encode_batch(&[Bytes::from(row)]).unwrap();
        assert_eq!(batch.num_rows(), 1);
        let list = batch.column(0).as_list::<i32>();
        assert_eq!(list.value_length(0), 4);
        let values = list
            .values()
            .as_primitive::<arrow::datatypes::Int32Type>();
        assert_eq!(
            (0..4).map(|i| values.value(i)).collect::<Vec<_>>(),
            vec![10, 20, 30, 40]
        );
    }

    #[test]
    fn repeated_scalar_empty_row_produces_empty_list() {
        // A row with no tag occurrences produces an empty list, not null.
        let desc = repeated_int32_descriptor();
        let schema = Schema::new(vec![Field::new(
            "numbers",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )]);
        let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

        let batch = enc.encode_batch(&[Bytes::new()]).unwrap();
        assert_eq!(batch.num_rows(), 1);
        let list = batch.column(0).as_list::<i32>();
        assert_eq!(list.value_length(0), 0);
        assert!(!list.is_null(0), "list column itself should never be null");
    }

    #[test]
    fn map_roundtrip() {
        // Proto: test_protobuf3.Person.data = map<string, PhoneType>
        // where PhoneType is an enum (int32-encoded on the wire).
        //
        // Arrow side: Map<Struct(key: LargeUtf8, value: Int32)> with entry
        // field named "key_value" per the sink's existing convention.
        let desc = rich_descriptor();
        let entry_fields = ArrowFields::from(vec![
            Field::new("key", DataType::LargeUtf8, false),
            Field::new("value", DataType::Int32, true),
        ]);
        let entry_field = Arc::new(Field::new("key_value", DataType::Struct(entry_fields), false));
        let schema = Schema::new(vec![Field::new(
            "data",
            DataType::Map(entry_field, false),
            true,
        )]);
        let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

        // Populate a row with 2 map entries.
        let mut msg = DynamicMessage::new(desc.clone());
        let mut entries: std::collections::HashMap<prost_reflect::MapKey, ProtoValue> =
            std::collections::HashMap::new();
        entries.insert(
            prost_reflect::MapKey::String("alpha".into()),
            ProtoValue::EnumNumber(1),
        );
        entries.insert(
            prost_reflect::MapKey::String("beta".into()),
            ProtoValue::EnumNumber(2),
        );
        msg.set_field_by_name("data", ProtoValue::Map(entries));
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();

        let batch = enc.encode_batch(&[Bytes::from(buf)]).unwrap();
        assert_eq!(batch.num_rows(), 1);
        let map = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::MapArray>()
            .expect("column should be MapArray");
        assert_eq!(map.value_length(0), 2, "expected 2 map entries");
        let keys = map.keys().as_string::<i64>();
        let values = map
            .values()
            .as_primitive::<arrow::datatypes::Int32Type>();
        // Map entry iteration order is not guaranteed — collect then compare.
        let pairs: std::collections::HashMap<String, i32> = (0..2)
            .map(|i| (keys.value(i).to_string(), values.value(i)))
            .collect();
        assert_eq!(pairs.get("alpha").copied(), Some(1));
        assert_eq!(pairs.get("beta").copied(), Some(2));
    }

    fn encode_varint_into(buf: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            buf.push((value as u8) | 0x80);
            value >>= 7;
        }
        buf.push(value as u8);
    }

    /// `message Ts { int64 event_time = 1; }` — for the timestamp coercion test.
    fn timestamp_descriptor() -> MessageDescriptor {
        let fd = FileDescriptorProto {
            name: Some("wire_to_arrow_poc_ts.proto".into()),
            package: Some("wire_to_arrow_poc_test".into()),
            message_type: vec![DescriptorProto {
                name: Some("Ts".into()),
                field: vec![FieldDescriptorProto {
                    name: Some("event_time".into()),
                    number: Some(1),
                    label: Some(Label::Optional as i32),
                    r#type: Some(ProtoType::Int64 as i32),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let set = FileDescriptorSet { file: vec![fd] };
        let mut bytes = Vec::new();
        set.encode(&mut bytes).unwrap();
        DescriptorPool::decode(bytes.as_slice())
            .unwrap()
            .get_message_by_name("wire_to_arrow_poc_test.Ts")
            .unwrap()
    }

    /// `message Choice { oneof x { int32 a = 1; string b = 2; } }`
    fn oneof_descriptor() -> MessageDescriptor {
        let fd = FileDescriptorProto {
            name: Some("wire_to_arrow_poc_oneof.proto".into()),
            package: Some("wire_to_arrow_poc_test".into()),
            message_type: vec![DescriptorProto {
                name: Some("Choice".into()),
                field: vec![
                    FieldDescriptorProto {
                        name: Some("a".into()),
                        number: Some(1),
                        label: Some(Label::Optional as i32),
                        r#type: Some(ProtoType::Int32 as i32),
                        oneof_index: Some(0),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("b".into()),
                        number: Some(2),
                        label: Some(Label::Optional as i32),
                        r#type: Some(ProtoType::String as i32),
                        oneof_index: Some(0),
                        ..Default::default()
                    },
                ],
                oneof_decl: vec![OneofDescriptorProto {
                    name: Some("x".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let set = FileDescriptorSet { file: vec![fd] };
        let mut bytes = Vec::new();
        set.encode(&mut bytes).unwrap();
        DescriptorPool::decode(bytes.as_slice())
            .unwrap()
            .get_message_by_name("wire_to_arrow_poc_test.Choice")
            .unwrap()
    }

    #[test]
    fn int64_to_timestamp_micros_coercion() {
        // proto int64 field with the Arrow column declared as Timestamp(Micro, UTC).
        let desc = timestamp_descriptor();
        let schema = Schema::new(vec![Field::new(
            "event_time",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )]);
        let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

        // Two rows, plus one with the field absent (should produce null).
        let mut row0 = Vec::new();
        row0.push((1u8 << 3) | 0); // tag 1, varint
        encode_varint_into(&mut row0, 1_700_000_000_000_000_u64);
        let mut row1 = Vec::new();
        row1.push((1u8 << 3) | 0);
        encode_varint_into(&mut row1, 1_800_000_000_000_000_u64);

        let batch = enc
            .encode_batch(&[
                Bytes::from(row0),
                Bytes::from(row1),
                Bytes::new(), // absent -> null
            ])
            .unwrap();

        assert_eq!(batch.num_rows(), 3);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .expect("TimestampMicrosecondArray");
        assert_eq!(col.value(0), 1_700_000_000_000_000);
        assert_eq!(col.value(1), 1_800_000_000_000_000);
        assert!(col.is_null(2));
    }

    #[test]
    fn oneof_variants_map_to_separate_columns() {
        // With the wire-format identity (oneof variants look like regular
        // singular fields), the encoder should populate whichever variant
        // appears in the bytes and leave the others null.
        let desc = oneof_descriptor();
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::LargeUtf8, true),
        ]);
        let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

        // Row 0: only `a = 42`.
        let mut row0 = Vec::new();
        row0.push((1u8 << 3) | 0); // field 1, varint
        encode_varint_into(&mut row0, 42);

        // Row 1: only `b = "hello"`.
        let mut row1 = Vec::new();
        row1.push((2u8 << 3) | 2); // field 2, length-delimited
        encode_varint_into(&mut row1, 5);
        row1.extend_from_slice(b"hello");

        let batch = enc
            .encode_batch(&[Bytes::from(row0), Bytes::from(row1)])
            .unwrap();
        assert_eq!(batch.num_rows(), 2);

        let a = batch.column(0).as_primitive::<arrow::datatypes::Int32Type>();
        assert_eq!(a.value(0), 42);
        assert!(a.is_null(1));

        let b = batch.column(1).as_string::<i64>();
        assert!(batch.column(1).is_null(0));
        assert_eq!(b.value(1), "hello");
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
