//! Streaming wire-format to Arrow encoder.
//!
//! Parses proto wire bytes in a single pass and appends values directly into
//! Arrow `RecordBatch` column builders, skipping the `DynamicMessage` /
//! `LogEvent` intermediate representations used by the generic
//! `ProtobufDeserializer` + `ArrowStreamSerializer` path.
//!
//! Used as a [`BatchSerializerConfig`] variant — upstream is expected to
//! stash original proto wire bytes in the event's `message` field (Vector
//! convention). The serializer is all-or-nothing: a batch fails if any event
//! lacks a `Bytes`-typed message or the wire decode errors.
//!
//! ## Supported today
//!
//! - Scalar proto fields (int32/int64/uint32/uint64/sint32/sint64/fixed*/float/double/bool/string/bytes/enum)
//! - Singular nested messages -> Arrow `Struct`
//! - Repeated nested messages -> Arrow `List<Struct>`
//! - Repeated scalars (packed and unpacked) -> Arrow `List<primitive>`
//! - Proto maps (`map<K, V>`) -> Arrow `Map<Struct(key, value)>`
//! - Oneof variants
//! - `int64 -> Timestamp(Microsecond, tz)` coercion
//!
//! Benchmarks live at `benches/codecs/wire_to_arrow_bench.rs`.
//!
//! [`BatchSerializerConfig`]: crate::encoding::BatchSerializerConfig

mod builders;
mod errors;
mod plan;

use std::sync::Arc;

use arrow::datatypes::{Fields, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use prost_reflect::MessageDescriptor;
use vector_config::configurable_component;
use vector_core::{
    config::DataType,
    event::{Event, Value},
    schema,
};

pub use errors::WireToArrowError;

use zeroparser::wire::{WireValue, decode_zigzag32, decode_zigzag64, try_parse_field};

use builders::{BuilderNodeList, TypedBuilder};
use errors::Result;
use plan::{MessagePlan, PlanSlot, ScalarKind};

/// Configuration for the wire-to-Arrow batch serializer.
///
/// Requires both a proto `MessageDescriptor` (to interpret the wire bytes)
/// and an Arrow `Schema` (to lay out the output `RecordBatch`). The sink is
/// responsible for resolving both from its own schema source and injecting
/// them into the config before calling [`Self::build`](BatchSerializerConfig::build).
#[configurable_component]
#[derive(Clone, Default)]
pub struct WireToArrowSerializerConfig {
    /// The proto message descriptor describing the wire bytes in `message`.
    #[serde(skip)]
    #[configurable(derived)]
    pub descriptor: Option<MessageDescriptor>,

    /// The Arrow schema of the output `RecordBatch`.
    #[serde(skip)]
    #[configurable(derived)]
    pub schema: Option<arrow::datatypes::Schema>,
}

impl std::fmt::Debug for WireToArrowSerializerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WireToArrowSerializerConfig")
            .field(
                "descriptor",
                &self.descriptor.as_ref().map(|d| d.full_name().to_string()),
            )
            .field(
                "schema",
                &self
                    .schema
                    .as_ref()
                    .map(|s| format!("{} fields", s.fields().len())),
            )
            .finish()
    }
}

impl WireToArrowSerializerConfig {
    /// Create a config with both descriptor and schema present.
    pub fn new(descriptor: MessageDescriptor, schema: arrow::datatypes::Schema) -> Self {
        Self {
            descriptor: Some(descriptor),
            schema: Some(schema),
        }
    }

    /// The data type of events accepted by this serializer.
    pub fn input_type(&self) -> DataType {
        DataType::Log
    }

    /// The schema required by the serializer.
    pub fn schema_requirement(&self) -> schema::Requirement {
        schema::Requirement::empty()
    }
}

/// Batch serializer that decodes proto wire bytes directly into an Arrow
/// `RecordBatch`, bypassing the generic `ProtobufDeserializer` chain.
#[derive(Clone, Debug)]
pub struct WireToArrowSerializer {
    encoder: Arc<WireToArrowEncoder>,
}

impl WireToArrowSerializer {
    /// Build a serializer from the given configuration.
    pub fn new(config: WireToArrowSerializerConfig) -> Result<Self> {
        let descriptor = config
            .descriptor
            .ok_or_else(|| WireToArrowError::ConfigurationMissing { field: "descriptor" })?;
        let schema = config
            .schema
            .ok_or_else(|| WireToArrowError::ConfigurationMissing { field: "schema" })?;
        let encoder = WireToArrowEncoder::new(&descriptor, schema)?;
        Ok(Self {
            encoder: Arc::new(encoder),
        })
    }

    /// Encode a batch of events into a single Arrow `RecordBatch`.
    ///
    /// Every event must carry a `Value::Bytes`-typed `message` field holding
    /// the original proto wire bytes; any miss rejects the batch.
    pub fn encode_to_record_batch(&self, events: &[Event]) -> Result<RecordBatch> {
        if events.is_empty() {
            return Err(WireToArrowError::NoEvents);
        }
        let mut wire_bytes = Vec::with_capacity(events.len());
        for event in events {
            match event.as_log().get_message() {
                Some(Value::Bytes(b)) => wire_bytes.push(b.clone()),
                Some(_) => return Err(WireToArrowError::MessageBytesWrongType),
                None => return Err(WireToArrowError::MessageBytesMissing),
            }
        }
        self.encoder.encode_batch(&wire_bytes)
    }
}

/// Streaming wire-format encoder. Build once per (proto message type,
/// Arrow schema) pair, then call [`WireToArrowEncoder::encode_batch`]
/// repeatedly.
pub struct WireToArrowEncoder {
    plan: Arc<MessagePlan>,
    schema: Arc<Schema>,
}

impl std::fmt::Debug for WireToArrowEncoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WireToArrowEncoder")
            .field("schema_fields", &self.schema.fields().len())
            .finish()
    }
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
    mut bytes: &[u8],
    builders: &mut BuilderNodeList,
    present: &mut [bool],
) -> Result<()> {
    while !bytes.is_empty() {
        let (field, rest) = try_parse_field(bytes)?;
        bytes = rest;
        let field_number = field.field_num as usize;

        let Some(Some(slot_idx)) = plan.slot_by_proto_field.get(field_number).copied() else {
            // Unknown field — `try_parse_field` already consumed it.
            continue;
        };
        let slot_idx = slot_idx as usize;

        let slot = &plan.slots[slot_idx];
        let node = &mut builders.nodes[slot_idx];

        match (slot, node) {
            (PlanSlot::Scalar(sk), builders::BuilderNode::Scalar(tb)) => {
                append_scalar_from_wire(*sk, &field.value, tb)?;
                present[slot_idx] = true;
            }
            (PlanSlot::Struct(sub_plan), builders::BuilderNode::Struct { children, .. }) => {
                let sub_bytes = expect_len(&field.value)?;
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
                let sub_bytes = expect_len(&field.value)?;
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
                append_repeated_scalar(*sk, &field.value, values, current_offset)?;
                present[slot_idx] = true;
            }
            _ => return Err(WireToArrowError::PlanBuilderMismatch),
        }
    }
    Ok(())
}

/// Extract the inner bytes from a length-delimited `WireValue`, or error.
#[inline]
fn expect_len<'a>(wv: &'a WireValue<'a>) -> Result<&'a [u8]> {
    match wv {
        WireValue::Len(b) => Ok(b),
        other => Err(WireToArrowError::WireTypeMismatch {
            expected: 2,
            actual: wire_type_byte(other),
        }),
    }
}

/// Proto wire type numeric code for a `WireValue`. Used for error reporting.
#[inline]
fn wire_type_byte(wv: &WireValue) -> u8 {
    match wv {
        WireValue::Varint(_) => 0,
        WireValue::I64(_) => 1,
        WireValue::Len(_) => 2,
        WireValue::I32(_) => 5,
    }
}

/// Append one scalar `WireValue` into the matching typed Arrow builder.
fn append_scalar_from_wire(
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
fn append_repeated_scalar(
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
    if kind.wire_type() == 2 {
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
        0 => Ok(WireValue::Varint(decode_varint(bytes, pos)?)),
        1 => Ok(WireValue::I64(read_fixed64(bytes, pos)?)),
        5 => Ok(WireValue::I32(read_fixed32(bytes, pos)?)),
        // Wire type 2 would be string/bytes — unreachable per the caller's
        // guard. Any other value indicates a plan build bug.
        _ => Err(WireToArrowError::PlanBuilderMismatch),
    }
}

// ---------------------------------------------------------------------------
// Tagless readers for the packed-scalar inner loop.
//
// The outer scan uses `try_parse_field`, which expects tag-prefixed fields.
// Packed repeated scalars live inside a single `WireValue::Len(inner)` blob
// whose contents are raw values with no tags. Proto-parser doesn't expose
// tagless readers, so we keep these here until a follow-up integration
// removes the packed inner loop entirely.
// ---------------------------------------------------------------------------

/// Read a single varint and advance `pos`. Caps at 10 bytes per proto spec.
#[inline]
fn decode_varint(bytes: &[u8], pos: &mut usize) -> Result<u64> {
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
fn read_fixed64(bytes: &[u8], pos: &mut usize) -> Result<u64> {
    if *pos + 8 > bytes.len() {
        return Err(WireToArrowError::UnexpectedEof);
    }
    let v = u64::from_le_bytes(bytes[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Ok(v)
}

/// Read 4 little-endian bytes and advance `pos`.
#[inline]
fn read_fixed32(bytes: &[u8], pos: &mut usize) -> Result<u32> {
    if *pos + 4 > bytes.len() {
        return Err(WireToArrowError::UnexpectedEof);
    }
    let v = u32::from_le_bytes(bytes[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Ok(v)
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

    fn encode_varint_for_test(mut value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        while value >= 0x80 {
            out.push((value as u8) | 0x80);
            value >>= 7;
        }
        out.push(value as u8);
        out
    }

    #[test]
    fn packed_readers_decode_varint_roundtrip() {
        for v in [0u64, 1, 127, 128, 255, 16384, u32::MAX as u64, u64::MAX] {
            let encoded = encode_varint_for_test(v);
            let mut pos = 0;
            let decoded = decode_varint(&encoded, &mut pos).unwrap();
            assert_eq!(v, decoded, "mismatch on {v}");
            assert_eq!(pos, encoded.len(), "position not advanced");
        }
    }

    #[test]
    fn packed_readers_decode_varint_eof() {
        let mut pos = 0;
        assert!(matches!(
            decode_varint(&[0x80u8], &mut pos),
            Err(WireToArrowError::UnexpectedEof)
        ));
    }

    #[test]
    fn packed_readers_decode_varint_overflow() {
        let mut pos = 0;
        assert!(matches!(
            decode_varint(&[0xffu8; 11], &mut pos),
            Err(WireToArrowError::VarintOverflow)
        ));
    }

    #[test]
    fn packed_readers_fixed32_roundtrip() {
        let bytes = 0x12345678u32.to_le_bytes();
        let mut pos = 0;
        assert_eq!(read_fixed32(&bytes, &mut pos).unwrap(), 0x12345678u32);
        assert_eq!(pos, 4);
    }

    #[test]
    fn packed_readers_fixed64_roundtrip() {
        let v: u64 = 0x0011_2233_4455_6677;
        let bytes = v.to_le_bytes();
        let mut pos = 0;
        assert_eq!(read_fixed64(&bytes, &mut pos).unwrap(), v);
        assert_eq!(pos, 8);
    }

    fn descriptor_pool(file: &str) -> DescriptorPool {
        let desc_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data/protobuf/protos")
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

    fn serializer_for(desc: &MessageDescriptor, schema: Schema) -> WireToArrowSerializer {
        WireToArrowSerializer::new(WireToArrowSerializerConfig::new(desc.clone(), schema))
            .expect("serializer build")
    }

    fn event_with_message_bytes(bytes: Bytes) -> Event {
        let mut e = Event::from(vector_core::event::LogEvent::default());
        e.as_mut_log().insert("message", bytes);
        e
    }

    #[test]
    fn serializer_requires_descriptor_and_schema() {
        let desc = scalar_descriptor();
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, true)]);

        let missing_desc = WireToArrowSerializer::new(WireToArrowSerializerConfig {
            descriptor: None,
            schema: Some(schema.clone()),
        });
        assert!(matches!(
            missing_desc,
            Err(WireToArrowError::ConfigurationMissing { field: "descriptor" })
        ));

        let missing_schema = WireToArrowSerializer::new(WireToArrowSerializerConfig {
            descriptor: Some(desc),
            schema: None,
        });
        assert!(matches!(
            missing_schema,
            Err(WireToArrowError::ConfigurationMissing { field: "schema" })
        ));
    }

    #[test]
    fn serializer_empty_batch_errors() {
        let desc = scalar_descriptor();
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, true)]);
        let serializer = serializer_for(&desc, schema);
        assert!(matches!(
            serializer.encode_to_record_batch(&[]),
            Err(WireToArrowError::NoEvents)
        ));
    }

    #[test]
    fn serializer_missing_message_field_errors() {
        let desc = scalar_descriptor();
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, true)]);
        let serializer = serializer_for(&desc, schema);

        let e1 = event_with_message_bytes(Bytes::from_static(b""));
        let e2 = Event::from(vector_core::event::LogEvent::default()); // no message

        assert!(matches!(
            serializer.encode_to_record_batch(&[e1, e2]),
            Err(WireToArrowError::MessageBytesMissing)
        ));
    }

    #[test]
    fn serializer_wrong_type_message_errors() {
        let desc = scalar_descriptor();
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, true)]);
        let serializer = serializer_for(&desc, schema);

        // Plain strings are represented as `Value::Bytes`, so use an integer
        // to get a non-bytes variant for this negative case.
        let mut e = Event::from(vector_core::event::LogEvent::default());
        e.as_mut_log().insert("message", 42_i64);
        assert!(matches!(
            serializer.encode_to_record_batch(&[e]),
            Err(WireToArrowError::MessageBytesWrongType)
        ));
    }

    #[test]
    fn serializer_end_to_end_matches_direct_encode() {
        // Build events with wire bytes on `message`, encode via the
        // serializer, and compare against calling the lower-level encoder
        // directly.
        let desc = scalar_descriptor();
        let schema = Schema::new(vec![
            Field::new("name", DataType::LargeUtf8, true),
            Field::new("id", DataType::Int32, true),
            Field::new("email", DataType::LargeUtf8, true),
        ]);
        let serializer = serializer_for(&desc, schema.clone());
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
            events.push(event_with_message_bytes(bytes));
        }

        let via_events = serializer.encode_to_record_batch(&events).unwrap();
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
