use super::append::read_packed_element;
use super::plan::ScalarKind;
use super::serializer::OnWireTypeMismatch;
use super::{
    WireToArrowEncoder, WireToArrowError, WireToArrowSerializer, WireToArrowSerializerConfig,
};

use proptest::prelude::*;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Field, Fields as ArrowFields, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use prost_reflect::MessageDescriptor;
use prost_reflect::prost::Message as _;
use prost_reflect::prost_types::field_descriptor_proto::{Label, Type as ProtoType};
use prost_reflect::prost_types::{
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FileDescriptorProto, FileDescriptorSet, OneofDescriptorProto,
};
use prost_reflect::{DescriptorPool, DynamicMessage, Value as ProtoValue};
use std::path::PathBuf;
use std::sync::Arc;
use tracing_test::traced_test;
use vector_core::event::Event;
use zeroparser::wire::WireValue;

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
fn packed_element_varint_roundtrip() {
    for v in [0u64, 1, 127, 128, 255, 16384, u32::MAX as u64, u64::MAX] {
        let encoded = encode_varint_for_test(v);
        let (decoded, rest) = read_packed_element(ScalarKind::Int64, &encoded).unwrap();
        assert!(
            matches!(decoded, WireValue::Varint(d) if d == v),
            "mismatch on {v}"
        );
        assert!(rest.is_empty(), "buffer not fully consumed for {v}");
    }
}

#[test]
fn packed_element_varint_eof_maps_to_unexpected_eof() {
    assert!(matches!(
        read_packed_element(ScalarKind::Int64, &[0x80u8]),
        Err(WireToArrowError::UnexpectedEof)
    ));
}

#[test]
fn packed_element_varint_overflow_maps_to_varint_overflow() {
    assert!(matches!(
        read_packed_element(ScalarKind::Int64, &[0xffu8; 11]),
        Err(WireToArrowError::VarintOverflow)
    ));
}

#[test]
fn packed_element_fixed32_roundtrip() {
    let bytes = 0x12345678u32.to_le_bytes();
    let (decoded, rest) = read_packed_element(ScalarKind::Fixed32, &bytes).unwrap();
    assert!(matches!(decoded, WireValue::I32(v) if v == 0x12345678));
    assert!(rest.is_empty());
}

#[test]
fn packed_element_fixed64_roundtrip() {
    let v: u64 = 0x0011_2233_4455_6677;
    let bytes = v.to_le_bytes();
    let (decoded, rest) = read_packed_element(ScalarKind::Fixed64, &bytes).unwrap();
    assert!(matches!(decoded, WireValue::I64(d) if d == v));
    assert!(rest.is_empty());
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

/// Build a `message Fixups { fixed32 f32 = 1; fixed64 f64 = 2; uint32 u32 = 3;
/// uint64 u64 = 4; }` descriptor. None of the checked-in test protos carry
/// fixed/unsigned scalar fields, which are the ones that land in *signed* Arrow
/// columns (Delta has no unsigned integer type — span's `fixed32 flags` and
/// `fixed64 *_unix_nano` are the real cases).
fn fixed_and_unsigned_descriptor() -> MessageDescriptor {
    let field = |name: &str, number: i32, ty: ProtoType| FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        label: Some(Label::Optional as i32),
        r#type: Some(ty as i32),
        ..Default::default()
    };
    let fd = FileDescriptorProto {
        name: Some("wire_to_arrow_fixups_test.proto".into()),
        package: Some("wire_to_arrow_fixups_test".into()),
        message_type: vec![DescriptorProto {
            name: Some("Fixups".into()),
            field: vec![
                field("f32", 1, ProtoType::Fixed32),
                field("f64", 2, ProtoType::Fixed64),
                field("u32", 3, ProtoType::Uint32),
                field("u64", 4, ProtoType::Uint64),
            ],
            ..Default::default()
        }],
        ..Default::default()
    };
    let set = FileDescriptorSet { file: vec![fd] };
    let mut bytes = Vec::new();
    set.encode(&mut bytes).unwrap();
    DescriptorPool::decode(bytes.as_slice())
        .unwrap()
        .get_message_by_name("wire_to_arrow_fixups_test.Fixups")
        .unwrap()
}

#[test]
fn fixed_and_unsigned_scalars_encode_into_signed_columns() {
    // Delta/Spark has no unsigned integer type, so `fixed32`/`uint32` columns are
    // declared `Int32` and `fixed64`/`uint64` columns `Int64` in the destination
    // table. The encoder must accept those pairings and write bit-preserving —
    // values above the signed max reinterpret as negatives. Regression test for
    // the span WTA build failure on `fixed32 flags` / `arrow type Int32`.
    let desc = fixed_and_unsigned_descriptor();
    let schema = Schema::new(vec![
        Field::new("f32", DataType::Int32, true),
        Field::new("f64", DataType::Int64, true),
        Field::new("u32", DataType::Int32, true),
        Field::new("u64", DataType::Int64, true),
    ]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    let mut msg = DynamicMessage::new(desc.clone());
    // All chosen above the signed max so the reinterpret is observable.
    msg.set_field_by_name("f32", ProtoValue::U32(0x8000_0001));
    msg.set_field_by_name("f64", ProtoValue::U64(0x8000_0000_0000_0001));
    msg.set_field_by_name("u32", ProtoValue::U32(u32::MAX));
    msg.set_field_by_name("u64", ProtoValue::U64(u64::MAX));
    let mut buf = Vec::new();
    msg.encode(&mut buf).unwrap();

    let batch = enc.encode_batch(&[Bytes::from(buf)]).unwrap();
    assert_eq!(batch.num_rows(), 1);
    let i32_col = |i: usize| {
        batch
            .column(i)
            .as_primitive::<arrow::datatypes::Int32Type>()
            .value(0)
    };
    let i64_col = |i: usize| {
        batch
            .column(i)
            .as_primitive::<arrow::datatypes::Int64Type>()
            .value(0)
    };
    assert_eq!(i32_col(0), 0x8000_0001u32 as i32);
    assert_eq!(i64_col(1), 0x8000_0000_0000_0001u64 as i64);
    assert_eq!(i32_col(2), -1); // u32::MAX
    assert_eq!(i64_col(3), -1); // u64::MAX
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
    assert_eq!(batch.column(0).as_string::<i64>().value(0), "Alice");
    assert_eq!(
        batch
            .column(1)
            .as_primitive::<arrow::datatypes::Int32Type>()
            .value(0),
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
    msg.set_field_by_name("phones", ProtoValue::List(vec![ProtoValue::Message(phone)]));
    let mut buf = Vec::new();
    msg.encode(&mut buf).unwrap();

    // Should not error — unknown fields get skipped.
    let batch = enc.encode_batch(&[Bytes::from(buf)]).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.column(0).as_string::<i64>().value(0), "Alice");
}

#[test]
fn absent_proto_field_becomes_all_null_column() {
    // Schema-drift path: the Arrow schema has a column `missing_col` that
    // the proto descriptor doesn't carry (simulates "UC has the column but
    // the producer's proto dropped it"). The encoder must emit an all-null
    // Arrow column for `missing_col` and otherwise populate normally.
    let desc = scalar_descriptor();
    let schema = Schema::new(vec![
        Field::new("name", DataType::LargeUtf8, true),
        Field::new("missing_col", DataType::Int64, true),
        Field::new("id", DataType::Int32, true),
    ]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    let mut msg1 = DynamicMessage::new(desc.clone());
    msg1.set_field_by_name("name", ProtoValue::String("Alice".into()));
    msg1.set_field_by_name("id", ProtoValue::I32(1));
    let mut buf1 = Vec::new();
    msg1.encode(&mut buf1).unwrap();

    let mut msg2 = DynamicMessage::new(desc.clone());
    msg2.set_field_by_name("name", ProtoValue::String("Bob".into()));
    msg2.set_field_by_name("id", ProtoValue::I32(2));
    let mut buf2 = Vec::new();
    msg2.encode(&mut buf2).unwrap();

    let batch = enc
        .encode_batch(&[Bytes::from(buf1), Bytes::from(buf2)])
        .unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 3);

    assert_eq!(batch.column(0).as_string::<i64>().value(0), "Alice");
    assert_eq!(batch.column(0).as_string::<i64>().value(1), "Bob");

    // The missing column must be declared null for every row.
    let missing = batch.column(1);
    assert_eq!(missing.len(), 2);
    assert!(missing.is_null(0));
    assert!(missing.is_null(1));

    let ids = batch
        .column(2)
        .as_primitive::<arrow::datatypes::Int32Type>();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
}

fn serializer_for(desc: &MessageDescriptor, schema: Schema) -> WireToArrowSerializer {
    WireToArrowSerializer::from_descriptor(desc.clone(), schema).expect("serializer build")
}

fn event_with_message_bytes(bytes: Bytes) -> Event {
    let mut e = Event::from(vector_core::event::LogEvent::default());
    e.as_mut_log().insert("message", bytes);
    e
}

#[test]
fn serializer_loads_descriptor_from_config() {
    // Happy path via the user-facing constructor: `desc_file` + `message_type`
    // point at a real descriptor set, `schema` is injected by the sink.
    let schema = Schema::new(vec![Field::new("id", DataType::Int32, true)]);
    let config = WireToArrowSerializerConfig {
        desc_file: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data/protobuf/protos/test_protobuf.desc"),
        message_type: "test_protobuf.Person".to_string(),
        schema: Some(schema),
        ..Default::default()
    };
    let serializer = WireToArrowSerializer::new(config).expect("build");
    assert!(matches!(
        serializer.encode_to_record_batch(&[]),
        Err(WireToArrowError::NoEvents)
    ));
}

#[test]
fn serializer_errors_on_bad_descriptor_path() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int32, true)]);
    let config = WireToArrowSerializerConfig {
        desc_file: PathBuf::from("/nonexistent/path/to/schema.desc"),
        message_type: "some.Message".to_string(),
        schema: Some(schema),
        ..Default::default()
    };
    assert!(matches!(
        WireToArrowSerializer::new(config),
        Err(WireToArrowError::DescriptorLoad { .. })
    ));
}

#[test]
fn serializer_errors_on_missing_schema() {
    let config = WireToArrowSerializerConfig {
        desc_file: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data/protobuf/protos/test_protobuf.desc"),
        message_type: "test_protobuf.Person".to_string(),
        schema: None,
        ..Default::default()
    };
    assert!(matches!(
        WireToArrowSerializer::new(config),
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
fn encode_batch_with_kept_reports_surviving_event_indices() {
    let desc = scalar_descriptor();
    let schema = Schema::new(vec![
        Field::new("name", DataType::LargeUtf8, true),
        Field::new("id", DataType::Int32, true),
        Field::new("email", DataType::LargeUtf8, true),
    ]);
    let serializer = serializer_for(&desc, schema.clone());
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    let mut messages = Vec::new();
    let mut events = Vec::new();
    for (i, valid) in [true, false, true, false, true].into_iter().enumerate() {
        let bytes = if valid {
            let mut msg = DynamicMessage::new(desc.clone());
            msg.set_field_by_name("name", ProtoValue::String(format!("n-{i}")));
            msg.set_field_by_name("id", ProtoValue::I32(i as i32));
            let mut buf = Vec::new();
            msg.encode(&mut buf).unwrap();
            Bytes::from(buf)
        } else {
            // 0xff starts an unterminated varint: the row drops per-row.
            Bytes::from_static(&[0xff, 0xff])
        };
        messages.push(bytes.clone());
        events.push(event_with_message_bytes(bytes));
    }

    let (batch, kept) = enc.encode_batch_with_kept(&messages).unwrap();
    assert_eq!(
        kept,
        vec![0, 2, 4],
        "indices are event indices, not row slots"
    );
    assert_eq!(batch.num_rows(), 3);

    let (batch, kept) = serializer
        .encode_to_record_batch_with_kept(&events)
        .unwrap();
    assert_eq!(kept, vec![0, 2, 4]);
    assert_eq!(batch.num_rows(), 3);
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
    let tag = 1u8 << 3; // field 1, wire type 0 (varint)
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
    let values = list.values().as_primitive::<arrow::datatypes::Int32Type>();
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
    let values = list.values().as_primitive::<arrow::datatypes::Int32Type>();
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

/// A `repeated KeyValue` message where `KeyValue { string key = 1; string value = 2; }` does NOT
/// carry the proto `map_entry` option. This models Databricks' `(databricks.json_map) = true`
/// convention (e.g. `dbr_log.DbrLog.context` -> `repeated SparkContext`), which the legacy Spark
/// pipeline renders as a Spark MapType even though it is not a native proto map. `is_map()` is
/// false for this field, so wire_to_arrow must recognize the "repeated key/value message targeting
/// an Arrow Map column" shape to build a MapArray.
fn json_map_descriptor() -> MessageDescriptor {
    let fd = FileDescriptorProto {
        name: Some("wire_to_arrow_json_map_test.proto".into()),
        package: Some("wire_to_arrow_json_map_test".into()),
        message_type: vec![
            DescriptorProto {
                name: Some("Log".into()),
                field: vec![FieldDescriptorProto {
                    name: Some("context".into()),
                    number: Some(3),
                    label: Some(Label::Repeated as i32),
                    r#type: Some(ProtoType::Message as i32),
                    type_name: Some(".wire_to_arrow_json_map_test.KeyValue".into()),
                    ..Default::default()
                }],
                ..Default::default()
            },
            // KeyValue: key=1, value=2, NO options.map_entry (unlike a native proto map entry).
            DescriptorProto {
                name: Some("KeyValue".into()),
                field: vec![
                    FieldDescriptorProto {
                        name: Some("key".into()),
                        number: Some(1),
                        label: Some(Label::Optional as i32),
                        r#type: Some(ProtoType::String as i32),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("value".into()),
                        number: Some(2),
                        label: Some(Label::Optional as i32),
                        r#type: Some(ProtoType::String as i32),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let set = FileDescriptorSet { file: vec![fd] };
    let mut bytes = Vec::new();
    set.encode(&mut bytes).unwrap();
    DescriptorPool::decode(bytes.as_slice())
        .unwrap()
        .get_message_by_name("wire_to_arrow_json_map_test.Log")
        .unwrap()
}

#[test]
fn json_map_repeated_keyvalue_roundtrips_to_map() {
    // The json_map field is `repeated KeyValue` (no map_entry option), and the UC schema declares
    // the column as Arrow Map<Struct(key, value)>. On the wire a repeated key/value message is
    // byte-identical to a native proto map, so wire_to_arrow should produce a MapArray keyed by the
    // Arrow Map column type — not fall through to List<Struct>.
    let desc = json_map_descriptor();
    let entry_fields = ArrowFields::from(vec![
        Field::new("key", DataType::LargeUtf8, false),
        Field::new("value", DataType::LargeUtf8, true),
    ]);
    let entry_field = Arc::new(Field::new(
        "key_value",
        DataType::Struct(entry_fields),
        false,
    ));
    let schema = Schema::new(vec![Field::new(
        "context",
        DataType::Map(entry_field, false),
        true,
    )]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    // Populate `context` with two KeyValue entries.
    let kv_desc = desc
        .get_field_by_name("context")
        .unwrap()
        .kind()
        .as_message()
        .unwrap()
        .clone();
    let mut kv1 = DynamicMessage::new(kv_desc.clone());
    kv1.set_field_by_name("key", ProtoValue::String("spark.app.id".into()));
    kv1.set_field_by_name("value", ProtoValue::String("app-123".into()));
    let mut kv2 = DynamicMessage::new(kv_desc);
    kv2.set_field_by_name("key", ProtoValue::String("spark.executor.id".into()));
    kv2.set_field_by_name("value", ProtoValue::String("driver".into()));

    let mut msg = DynamicMessage::new(desc.clone());
    msg.set_field_by_name(
        "context",
        ProtoValue::List(vec![ProtoValue::Message(kv1), ProtoValue::Message(kv2)]),
    );
    let mut buf = Vec::new();
    msg.encode(&mut buf).unwrap();

    let batch = enc.encode_batch(&[Bytes::from(buf)]).unwrap();
    assert_eq!(batch.num_rows(), 1);
    let map = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::MapArray>()
        .expect("json_map column should be MapArray");
    assert_eq!(map.value_length(0), 2, "expected 2 json_map entries");
    let keys = map.keys().as_string::<i64>();
    let values = map.values().as_string::<i64>();
    let pairs: std::collections::HashMap<String, String> = (0..2)
        .map(|i| (keys.value(i).to_string(), values.value(i).to_string()))
        .collect();
    assert_eq!(
        pairs.get("spark.app.id").map(String::as_str),
        Some("app-123")
    );
    assert_eq!(
        pairs.get("spark.executor.id").map(String::as_str),
        Some("driver")
    );
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
    let entry_field = Arc::new(Field::new(
        "key_value",
        DataType::Struct(entry_fields),
        false,
    ));
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
    let values = map.values().as_primitive::<arrow::datatypes::Int32Type>();
    // Map entry iteration order is not guaranteed — collect then compare.
    let pairs: std::collections::HashMap<String, i32> = (0..2)
        .map(|i| (keys.value(i).to_string(), values.value(i)))
        .collect();
    assert_eq!(pairs.get("alpha").copied(), Some(1));
    assert_eq!(pairs.get("beta").copied(), Some(2));
}

#[test]
fn map_entry_positional_bind_on_name_mismatch() {
    // Arrow's Map type doesn't constrain the inner Struct's field names, and
    // Delta/Spark-derived schemas surface proto `map<K, V>` columns with entry
    // fields named `keys`/`values` rather than proto MapEntry's `key`/`value`.
    // The encoder binds by ordinal position inside a map entry (slot 0 ->
    // key(1), slot 1 -> value(2)), so such a schema must build and round-trip
    // rather than failing serializer build over a cosmetic name difference.
    let desc = rich_descriptor();
    let entry_fields = ArrowFields::from(vec![
        Field::new("keys", DataType::LargeUtf8, false),
        Field::new("values", DataType::Int32, true),
    ]);
    let entry_field = Arc::new(Field::new("entries", DataType::Struct(entry_fields), false));
    let schema = Schema::new(vec![Field::new(
        "data",
        DataType::Map(entry_field, false),
        true,
    )]);
    let enc = WireToArrowEncoder::new(&desc, schema)
        .expect("Map entry named keys/values must bind positionally to proto key/value");

    // Populate a row with 2 map entries and confirm each proto key/value
    // landed in the correspondingly-positioned Arrow column.
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
    let values = map.values().as_primitive::<arrow::datatypes::Int32Type>();
    // Map entry iteration order is not guaranteed — collect then compare.
    let pairs: std::collections::HashMap<String, i32> = (0..2)
        .map(|i| (keys.value(i).to_string(), values.value(i)))
        .collect();
    assert_eq!(pairs.get("alpha").copied(), Some(1));
    assert_eq!(pairs.get("beta").copied(), Some(2));
}

#[test]
fn map_entry_extra_field_rejected_at_plan_build() {
    // Positional binding recovers `key`/`value` regardless of name, but a proto
    // MapEntry only declares fields 1 and 2. An entry struct with a third field
    // has no proto field to bind to at any position, so plan-build must still
    // reject it up front rather than let the absent-padding path panic.
    let desc = rich_descriptor();
    let entry_fields = ArrowFields::from(vec![
        Field::new("keys", DataType::LargeUtf8, false),
        Field::new("values", DataType::Int32, true),
        Field::new("extra", DataType::Int32, true),
    ]);
    let entry_field = Arc::new(Field::new("entries", DataType::Struct(entry_fields), false));
    let schema = Schema::new(vec![Field::new(
        "data",
        DataType::Map(entry_field, false),
        true,
    )]);
    let err = WireToArrowEncoder::new(&desc, schema)
        .expect_err("plan-build must reject a Map entry with more fields than proto MapEntry");
    assert!(
        matches!(
            err,
            WireToArrowError::MapEntryFieldNotInProto { ref name } if name == "extra"
        ),
        "expected MapEntryFieldNotInProto for the extra field, got {err:?}"
    );
}

/// Build a `data` field carrying one map entry, with raw bytes for the
/// MapEntry message (so we can elide the key tag, the value tag, or both —
/// proto3 default elision applies inside MapEntry messages just like every
/// other singular field). Schema: `test_protobuf3.Person.data` is field 4,
/// `map<string, PhoneType>`. Wire format: outer tag `(4 << 3) | 2 = 0x22`,
/// LEN-prefixed entry body.
fn person_with_raw_map_entry(entry_body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(entry_body.len() + 2);
    out.push(0x22); // tag 4, LEN
    out.push(entry_body.len() as u8); // body length (small for tests)
    out.extend_from_slice(entry_body);
    out
}

#[test]
fn map_entry_with_empty_string_key_encodes_as_empty_string_not_null() {
    // Proto3 elides default-valued singular fields *inside* MapEntry too.
    // Arrow's Map type declares the key field non-nullable; before the fix
    // an absent key tag produced a null, which fails `StructArray::try_new`
    // at batch finish — taking the whole batch down even though the wire
    // bytes are perfectly valid proto3. Producers in prost / Python /
    // protoc-gen-cpp emit exactly this shape when given a map with key "".
    let desc = rich_descriptor();
    let entry_fields = ArrowFields::from(vec![
        Field::new("key", DataType::LargeUtf8, false),
        Field::new("value", DataType::Int32, true),
    ]);
    let entry_field = Arc::new(Field::new(
        "key_value",
        DataType::Struct(entry_fields),
        false,
    ));
    let schema = Schema::new(vec![Field::new(
        "data",
        DataType::Map(entry_field, false),
        true,
    )]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    // MapEntry with only the value tag (key omitted, taking proto3 default "").
    //   0x10 = (2 << 3) | 0 = value tag, varint
    //   0x01 = enum value 1 (HOME)
    let bytes = person_with_raw_map_entry(&[0x10, 0x01]);

    let batch = enc
        .encode_batch(&[Bytes::from(bytes)])
        .expect("default-keyed map entry must not fail the batch");
    assert_eq!(batch.num_rows(), 1);
    let map = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::MapArray>()
        .expect("MapArray");
    assert_eq!(map.value_length(0), 1);
    let keys = map.keys().as_string::<i64>();
    assert_eq!(
        keys.value(0),
        "",
        "empty-default key must materialize as \"\""
    );
    assert!(!keys.is_null(0), "key column must contain no nulls");
    let values = map.values().as_primitive::<arrow::datatypes::Int32Type>();
    assert_eq!(values.value(0), 1);
}

#[test]
fn map_entry_with_default_int_value_encodes_as_zero_not_null() {
    // Mirror of the key case for the value side: proto3 elides value=0
    // (default int) inside MapEntry. Even with a nullable Arrow value field,
    // the proto semantics say "absent == 0", not "absent == null" — and
    // when the Arrow value is *non*-nullable, a null here would crash the
    // batch. Set value non-nullable to exercise both behaviors at once.
    let desc = rich_descriptor();
    let entry_fields = ArrowFields::from(vec![
        Field::new("key", DataType::LargeUtf8, false),
        Field::new("value", DataType::Int32, false),
    ]);
    let entry_field = Arc::new(Field::new(
        "key_value",
        DataType::Struct(entry_fields),
        false,
    ));
    let schema = Schema::new(vec![Field::new(
        "data",
        DataType::Map(entry_field, false),
        true,
    )]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    // MapEntry with only the key tag (value omitted, taking proto3 default 0).
    //   0x0a = (1 << 3) | 2 = key tag, LEN
    //   0x03 = length
    //   "foo"
    let bytes = person_with_raw_map_entry(&[0x0a, 0x03, b'f', b'o', b'o']);

    let batch = enc
        .encode_batch(&[Bytes::from(bytes)])
        .expect("default-valued map entry must not fail the batch");
    assert_eq!(batch.num_rows(), 1);
    let map = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::MapArray>()
        .unwrap();
    let keys = map.keys().as_string::<i64>();
    let values = map.values().as_primitive::<arrow::datatypes::Int32Type>();
    assert_eq!(keys.value(0), "foo");
    assert_eq!(
        values.value(0),
        0,
        "default-int value must materialize as 0"
    );
    assert!(!values.is_null(0));
}

#[test]
fn map_entry_with_all_defaults_encodes_as_empty_default_pair() {
    // An entirely-empty MapEntry on the wire (`0x22 0x00`) is what prost
    // emits when you serialize `HashMap::from([("".to_string(), 0)])`.
    // Both key and value tags are elided. Must produce a valid Arrow row
    // with `("", 0)`.
    let desc = rich_descriptor();
    let entry_fields = ArrowFields::from(vec![
        Field::new("key", DataType::LargeUtf8, false),
        Field::new("value", DataType::Int32, false),
    ]);
    let entry_field = Arc::new(Field::new(
        "key_value",
        DataType::Struct(entry_fields),
        false,
    ));
    let schema = Schema::new(vec![Field::new(
        "data",
        DataType::Map(entry_field, false),
        true,
    )]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    let bytes = person_with_raw_map_entry(&[]); // empty MapEntry

    let batch = enc
        .encode_batch(&[Bytes::from(bytes)])
        .expect("empty MapEntry must not fail the batch");
    assert_eq!(batch.num_rows(), 1);
    let map = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::MapArray>()
        .unwrap();
    let keys = map.keys().as_string::<i64>();
    let values = map.values().as_primitive::<arrow::datatypes::Int32Type>();
    assert_eq!(keys.value(0), "");
    assert_eq!(values.value(0), 0);
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
    // Inputs are epoch-milliseconds; the encoder must scale them ms->us (x1000) into the
    // micros Arrow column.
    let mut row0 = Vec::new();
    row0.push(1u8 << 3); // tag 1, varint
    encode_varint_into(&mut row0, 1_700_000_000_000_u64);
    let mut row1 = Vec::new();
    row1.push(1u8 << 3);
    encode_varint_into(&mut row1, 1_800_000_000_000_u64);

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
    // ms input scaled x1000 -> micros stored.
    assert_eq!(col.value(0), 1_700_000_000_000 * 1000);
    assert_eq!(col.value(1), 1_800_000_000_000 * 1000);
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
    row0.push(1u8 << 3); // field 1, varint
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

    let a = batch
        .column(0)
        .as_primitive::<arrow::datatypes::Int32Type>();
    assert_eq!(a.value(0), 42);
    assert!(a.is_null(1));

    let b = batch.column(1).as_string::<i64>();
    assert!(batch.column(1).is_null(0));
    assert_eq!(b.value(1), "hello");
}

#[test]
fn oneof_last_scalar_member_wins_and_preserves_defaults() {
    let enc = WireToArrowEncoder::new(
        &oneof_descriptor(),
        Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::LargeUtf8, true),
        ]),
    )
    .unwrap();
    let batch = enc
        .encode_batch(&[
            Bytes::from_static(&[0x08, 42, 0x12, 1, b'x']),
            Bytes::from_static(&[0x12, 1, b'x', 0x08, 42]),
            Bytes::from_static(&[0x12, 1, b'x', 0x08, 0]),
            Bytes::from_static(&[0x08, 42, 0x12, 0]),
        ])
        .unwrap();
    assert_eq!(batch.num_rows(), 4);
    let a = batch
        .column(0)
        .as_primitive::<arrow::datatypes::Int32Type>();
    let b = batch.column(1).as_string::<i64>();
    assert_eq!(
        a.iter().collect::<Vec<_>>(),
        vec![None, Some(42), Some(0), None]
    );
    assert_eq!(
        b.iter().collect::<Vec<_>>(),
        vec![Some("x"), None, None, Some("")]
    );
}

#[test]
fn proto3_optional_preserves_presence_without_oneof_switch_tracking() {
    let mut file = oneof_descriptor().parent_file_descriptor_proto().clone();
    file.syntax = Some("proto3".into());
    file.message_type[0].field.truncate(1);
    file.message_type[0].field[0].proto3_optional = Some(true);
    let pool = DescriptorPool::decode(
        FileDescriptorSet { file: vec![file] }
            .encode_to_vec()
            .as_slice(),
    )
    .unwrap();
    let desc = pool
        .get_message_by_name("wire_to_arrow_poc_test.Choice")
        .unwrap();
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let plan = super::plan::MessagePlan::build(&desc, schema.fields(), Default::default()).unwrap();
    assert!(plan.oneof_by_proto_field.is_empty());
    assert_eq!(plan.oneof_count, 0);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();
    let batch = enc
        .encode_batch(&[
            Bytes::new(),
            Bytes::from_static(&[0x08, 0]),
            Bytes::from_static(&[0x08, 42, 0x08, 0]),
        ])
        .unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow::datatypes::Int32Type>()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, Some(0), Some(0)]
    );
}

#[test]
fn oneof_hidden_members_clear_projected_values_in_wire_order() {
    for hidden_tag in [2, 536_870_911] {
        let mut file = oneof_descriptor().parent_file_descriptor_proto().clone();
        file.message_type[0].field[1].number = Some(hidden_tag);
        let pool = DescriptorPool::decode(
            FileDescriptorSet { file: vec![file] }
                .encode_to_vec()
                .as_slice(),
        )
        .unwrap();
        let desc = pool
            .get_message_by_name("wire_to_arrow_poc_test.Choice")
            .unwrap();
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let plan =
            super::plan::MessagePlan::build(&desc, schema.fields(), Default::default()).unwrap();
        assert_eq!(plan.slot_by_proto_field.len(), 2);
        let enc = WireToArrowEncoder::new(&desc, schema).unwrap();
        let mut hidden = encode_varint_for_test(((hidden_tag as u64) << 3) | 2);
        hidden.extend([1, b'x']);
        let a = [0x08, 42];
        let batch = enc
            .encode_batch(&[
                Bytes::from([a.as_slice(), hidden.as_slice()].concat()),
                Bytes::from([hidden.as_slice(), a.as_slice()].concat()),
                Bytes::from([a.as_slice(), hidden.as_slice(), &[0x08, 0]].concat()),
            ])
            .unwrap();
        assert_eq!(batch.num_rows(), 3, "hidden tag {hidden_tag}");
        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow::datatypes::Int32Type>()
                .iter()
                .collect::<Vec<_>>(),
            vec![None, Some(42), Some(0)],
            "hidden tag {hidden_tag}"
        );
    }
}

fn message_oneof_descriptor() -> MessageDescriptor {
    let mut file = message_variant_descriptor_with_syntax("proto3")
        .parent_file_descriptor_proto()
        .clone();
    let container = &mut file.message_type[0];
    container.field[0].oneof_index = Some(0);
    container.field.push(FieldDescriptorProto {
        name: Some("choice".into()),
        number: Some(3),
        label: Some(Label::Optional as i32),
        r#type: Some(ProtoType::Int32 as i32),
        oneof_index: Some(0),
        ..Default::default()
    });
    container.oneof_decl.push(OneofDescriptorProto {
        name: Some("selection".into()),
        ..Default::default()
    });
    DescriptorPool::decode(
        FileDescriptorSet { file: vec![file] }
            .encode_to_vec()
            .as_slice(),
    )
    .unwrap()
    .get_message_by_name("wire_to_arrow_msg_variant_test.Container")
    .unwrap()
}

#[test]
fn oneof_message_merges_until_another_member_resets_it() {
    let fields = ArrowFields::from(vec![
        Field::new("k", DataType::LargeUtf8, true),
        Field::new("n", DataType::Int64, true),
    ]);
    // Exercise both a mapped and a hidden switching member.
    for project_choice in [false, true] {
        let mut columns = vec![Field::new("single", DataType::Struct(fields.clone()), true)];
        if project_choice {
            columns.push(Field::new("choice", DataType::Int32, true));
        }
        let enc =
            WireToArrowEncoder::new(&message_oneof_descriptor(), Schema::new(columns)).unwrap();
        let first = [0x0a, 3, 0x0a, 1, b'x'];
        let second = [0x0a, 2, 0x10, 7];
        let batch = enc
            .encode_batch(&[
                Bytes::from([first.as_slice(), second.as_slice()].concat()),
                Bytes::from([first.as_slice(), &[0x18, 1], second.as_slice()].concat()),
                // An unrelated repeated-message field does not switch the oneof.
                Bytes::from([first.as_slice(), &[0x12, 0], second.as_slice()].concat()),
                Bytes::from([first.as_slice(), &[0x18, 0]].concat()),
            ])
            .unwrap();
        assert_eq!(batch.num_rows(), 4);
        let single = batch.column(0).as_struct();
        assert_eq!(
            single
                .column(0)
                .as_string::<i64>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("x"), None, Some("x"), None]
        );
        for row in 0..3 {
            assert!(!single.is_null(row));
            assert!(!single.column(1).is_null(row));
            assert_eq!(
                single
                    .column(1)
                    .as_primitive::<arrow::datatypes::Int64Type>()
                    .value(row),
                7
            );
        }
        assert!(single.is_null(3));
        if project_choice {
            assert_eq!(
                batch
                    .column(1)
                    .as_primitive::<arrow::datatypes::Int32Type>()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![None, None, None, Some(0)]
            );
        }
    }
}

#[test]
fn oneof_state_is_isolated_between_nested_and_repeated_messages() {
    let mut file = message_variant_descriptor_with_syntax("proto3")
        .parent_file_descriptor_proto()
        .clone();
    let kv = &mut file.message_type[1];
    kv.field[0].oneof_index = Some(0);
    kv.field[1].oneof_index = Some(0);
    kv.oneof_decl.push(OneofDescriptorProto {
        name: Some("selection".into()),
        ..Default::default()
    });
    let desc = DescriptorPool::decode(
        FileDescriptorSet { file: vec![file] }
            .encode_to_vec()
            .as_slice(),
    )
    .unwrap()
    .get_message_by_name("wire_to_arrow_msg_variant_test.Container")
    .unwrap();
    let fields = ArrowFields::from(vec![
        Field::new("k", DataType::LargeUtf8, true),
        Field::new("n", DataType::Int64, true),
    ]);
    let enc = WireToArrowEncoder::new(
        &desc,
        Schema::new(vec![
            Field::new("single", DataType::Struct(fields.clone()), true),
            Field::new(
                "many",
                DataType::List(Arc::new(Field::new("item", DataType::Struct(fields), true))),
                true,
            ),
        ]),
    )
    .unwrap();
    let batch = enc
        .encode_batch(&[
            Bytes::from_static(&[
                0x0a, 3, 0x0a, 1, b'x', 0x0a, 2, 0x10, 0, 0x12, 5, 0x0a, 1, b'a', 0x10, 1, 0x12, 5,
                0x10, 2, 0x0a, 1, b'b',
            ]),
            Bytes::from_static(&[0x0a, 3, 0x0a, 1, b'c']),
        ])
        .unwrap();
    assert_eq!(batch.num_rows(), 2);
    let single = batch.column(0).as_struct();
    assert_eq!(
        single
            .column(0)
            .as_string::<i64>()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, Some("c")]
    );
    assert_eq!(
        single
            .column(1)
            .as_primitive::<arrow::datatypes::Int64Type>()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(0), None]
    );
    let many = batch.column(1).as_list::<i32>().value(0);
    let many = many.as_struct();
    assert_eq!(
        many.column(0).as_string::<i64>().iter().collect::<Vec<_>>(),
        vec![None, Some("b")]
    );
    assert_eq!(
        many.column(1)
            .as_primitive::<arrow::datatypes::Int64Type>()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(1), None]
    );
}

#[test]
fn oneof_overwrites_do_not_hide_malformed_members_or_poison_neighbors() {
    for project_b in [false, true] {
        let mut columns = vec![Field::new("a", DataType::Int32, true)];
        if project_b {
            columns.push(Field::new("b", DataType::LargeUtf8, true));
        }
        let enc = WireToArrowEncoder::new(&oneof_descriptor(), Schema::new(columns)).unwrap();
        let batch = enc
            .encode_batch(&[
                Bytes::from_static(&[0x08, 7]),
                // Switching must validate the overwritten UTF-8, even when b is hidden.
                Bytes::from_static(&[0x08, 1, 0x12, 1, 0xff, 0x08, 2]),
                Bytes::from_static(&[0x08, 9]),
            ])
            .unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow::datatypes::Int32Type>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(7), Some(9)]
        );
    }
}

/// `message Choice { oneof x { int32 a = 1; int32 b = 2; } }` — same-typed
/// members so an unprojected member can carry a wire-type mismatch.
fn same_typed_oneof_descriptor() -> MessageDescriptor {
    let mut file = oneof_descriptor().parent_file_descriptor_proto().clone();
    file.message_type[0].field[1].r#type = Some(ProtoType::Int32 as i32);
    DescriptorPool::decode(
        FileDescriptorSet { file: vec![file] }
            .encode_to_vec()
            .as_slice(),
    )
    .unwrap()
    .get_message_by_name("wire_to_arrow_poc_test.Choice")
    .unwrap()
}

/// A wire-type-mismatched occurrence of an *unprojected* oneof member is an
/// unknown field to protobuf-java: it is skipped (and counted) without
/// clearing the well-typed projected sibling — in either wire order. A
/// correctly-typed unprojected member still clears it.
#[test]
fn unprojected_oneof_member_mismatch_does_not_clear_projected_sibling() {
    let desc = same_typed_oneof_descriptor();
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let plan = super::plan::MessagePlan::build(&desc, schema.fields(), Default::default()).unwrap();
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    // a=5 then b as LEN (0x12 0x01 0x01) — the mismatch; and the reverse
    // order; then a correctly-typed unprojected b (varint 7) clearing a.
    let wire_a_then_mismatch = [0x08, 0x05, 0x12, 0x01, 0x01];
    let wire_mismatch_then_a = [0x12, 0x01, 0x01, 0x08, 0x05];
    let wire_valid_hidden = [0x08, 0x05, 0x10, 0x07];
    let batch = enc
        .encode_batch(&[
            Bytes::from(wire_a_then_mismatch.to_vec()),
            Bytes::from(wire_mismatch_then_a.to_vec()),
            Bytes::from(wire_valid_hidden.to_vec()),
        ])
        .unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow::datatypes::Int32Type>()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(5), Some(5), None],
        "mismatched hidden member keeps the sibling; well-typed one clears it"
    );

    // Each mismatched occurrence is counted exactly once, with the field's
    // own path.
    for wire in [&wire_a_then_mismatch, &wire_mismatch_then_a] {
        let (result, skips) =
            validate_with_mode_and_skips(&plan, wire, OnWireTypeMismatch::SkipField);
        result.unwrap();
        assert_eq!(skips.skipped, 1, "wire {wire:02x?}");
        let sample = skips.sample.unwrap();
        assert_eq!(sample.field_numbers, [2]);
        assert_eq!(sample.expected, 0);
        assert_eq!(sample.actual, 2);
    }
    // The well-typed hidden member records no skip; it still requests
    // normalization, which is what clears the sibling (the encoder handles
    // that; the raw validate pass only signals it).
    let (result, skips) =
        validate_with_mode_and_skips(&plan, &wire_valid_hidden, OnWireTypeMismatch::SkipField);
    assert!(matches!(
        result,
        Err(WireToArrowError::OneofNeedsNormalization { field_number: 2 })
    ));
    assert_eq!(skips.skipped, 0);
}

/// Under `drop_row` a lone mismatched unprojected oneof member is ignored and
/// the row kept, exactly as before the skip policy existed.
#[test]
fn unprojected_oneof_member_mismatch_kept_under_drop_row_like_base() {
    let desc = same_typed_oneof_descriptor();
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let enc =
        WireToArrowEncoder::with_options(&desc, schema, OnWireTypeMismatch::DropRow, &[]).unwrap();
    // Lone mismatched member: ignored, row kept. Mismatched member alongside
    // the projected sibling: the base codec's oneof normalization hits
    // prost's strict wire-type check and drops the row.
    let batch = enc
        .encode_batch(&[
            Bytes::from_static(&[0x12, 0x01, 0x01]),
            Bytes::from_static(&[0x08, 0x05, 0x12, 0x01, 0x01]),
            Bytes::from_static(&[0x08, 0x05]),
        ])
        .unwrap();
    assert_eq!(batch.num_rows(), 2);
    let col = batch
        .column(0)
        .as_primitive::<arrow::datatypes::Int32Type>();
    assert!(col.is_null(0), "lone mismatch kept with null projection");
    assert_eq!(col.value(1), 5);
}

/// The same rule inside a nested message: a mismatched unprojected oneof
/// member in a sub-message keeps its sibling, with the skip attributed to the
/// root-to-leaf path.
#[test]
fn nested_unprojected_oneof_member_mismatch_keeps_sibling() {
    let mut file = same_typed_oneof_descriptor()
        .parent_file_descriptor_proto()
        .clone();
    file.message_type.push(DescriptorProto {
        name: Some("Holder".into()),
        field: vec![FieldDescriptorProto {
            name: Some("inner".into()),
            number: Some(1),
            label: Some(Label::Optional as i32),
            r#type: Some(ProtoType::Message as i32),
            type_name: Some(".wire_to_arrow_poc_test.Choice".into()),
            ..Default::default()
        }],
        ..Default::default()
    });
    let pool = DescriptorPool::decode(
        FileDescriptorSet { file: vec![file] }
            .encode_to_vec()
            .as_slice(),
    )
    .unwrap();
    let desc = pool
        .get_message_by_name("wire_to_arrow_poc_test.Holder")
        .unwrap();
    let inner = ArrowFields::from(vec![Field::new("a", DataType::Int32, true)]);
    let schema = Schema::new(vec![Field::new("inner", DataType::Struct(inner), true)]);
    let plan = super::plan::MessagePlan::build(&desc, schema.fields(), Default::default()).unwrap();
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    // inner = { a: 5, b as LEN }.
    let wire = [0x0a, 0x05, 0x08, 0x05, 0x12, 0x01, 0x01];
    let batch = enc.encode_batch(&[Bytes::from(wire.to_vec())]).unwrap();
    assert_eq!(batch.num_rows(), 1);
    let inner = batch.column(0).as_struct();
    assert_eq!(
        inner
            .column(0)
            .as_primitive::<arrow::datatypes::Int32Type>()
            .value(0),
        5
    );

    let (result, skips) = validate_with_mode_and_skips(&plan, &wire, OnWireTypeMismatch::SkipField);
    result.unwrap();
    assert_eq!(skips.skipped, 1);
    assert_eq!(skips.sample.unwrap().field_numbers, [1, 2]);
}

/// `message Tree { Tree next = 1; int32 leaf = 2; }` — a proto that's
/// self-referential by construction, used to drive deep plan/scan recursion.
fn self_referential_descriptor() -> MessageDescriptor {
    let fd = FileDescriptorProto {
        name: Some("wire_to_arrow_poc_tree.proto".into()),
        package: Some("wire_to_arrow_poc_test".into()),
        message_type: vec![DescriptorProto {
            name: Some("Tree".into()),
            field: vec![
                FieldDescriptorProto {
                    name: Some("next".into()),
                    number: Some(1),
                    label: Some(Label::Optional as i32),
                    r#type: Some(ProtoType::Message as i32),
                    type_name: Some(".wire_to_arrow_poc_test.Tree".into()),
                    ..Default::default()
                },
                FieldDescriptorProto {
                    name: Some("leaf".into()),
                    number: Some(2),
                    label: Some(Label::Optional as i32),
                    r#type: Some(ProtoType::Int32 as i32),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
        ..Default::default()
    };
    let set = FileDescriptorSet { file: vec![fd] };
    let mut bytes = Vec::new();
    set.encode(&mut bytes).unwrap();
    DescriptorPool::decode(bytes.as_slice())
        .unwrap()
        .get_message_by_name("wire_to_arrow_poc_test.Tree")
        .unwrap()
}

/// Build an Arrow Struct nested `levels` deep along a single `next` field,
/// with an `Int32` `leaf` at every level. Used to drive `MessagePlan::build`
/// recursion to a known depth.
fn nested_tree_struct(levels: usize) -> DataType {
    if levels == 0 {
        // Innermost level: just the leaf scalar, no `next`.
        return DataType::Struct(ArrowFields::from(vec![Field::new(
            "leaf",
            DataType::Int32,
            true,
        )]));
    }
    DataType::Struct(ArrowFields::from(vec![
        Field::new("next", nested_tree_struct(levels - 1), true),
        Field::new("leaf", DataType::Int32, true),
    ]))
}

#[test]
fn plan_build_rejects_schema_deeper_than_cap() {
    use super::plan::MAX_NESTING_DEPTH;

    let desc = self_referential_descriptor();
    // One level over the cap. The wrapper Schema counts as depth 0, so we
    // need MAX_NESTING_DEPTH levels of nested struct to step over the limit.
    let deep_struct = nested_tree_struct(MAX_NESTING_DEPTH);
    let schema = Schema::new(vec![Field::new("next", deep_struct, true)]);
    let err = WireToArrowEncoder::new(&desc, schema).expect_err("should reject");
    assert!(
        matches!(err, WireToArrowError::SchemaTooDeep { limit } if limit == MAX_NESTING_DEPTH),
        "expected SchemaTooDeep, got {err:?}"
    );
}

#[test]
fn plan_build_accepts_moderately_deep_schema() {
    // A reasonably deep but legal schema must build without error. Pick a
    // depth far below the cap so any reasonable real-world nesting is fine.
    let desc = self_referential_descriptor();
    let deep_struct = nested_tree_struct(8);
    let schema = Schema::new(vec![Field::new("next", deep_struct, true)]);
    let enc = WireToArrowEncoder::new(&desc, schema).expect("8-deep schema should build");
    // And it should be able to encode an empty payload (every level absent).
    let batch = enc.encode_batch(&[Bytes::new()]).unwrap();
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn plan_build_rejects_non_nullable_singular_scalar() {
    // proto3 omits default-valued singular scalars on the wire, so a
    // column declared non-nullable would fail RecordBatch::try_new with
    // a generic Arrow error deep in encode_batch — dropping the whole
    // batch. The plan builder should reject this at init.
    let desc = scalar_descriptor();
    let schema = Schema::new(vec![
        Field::new("name", DataType::LargeUtf8, true),
        Field::new("id", DataType::Int32, /* nullable */ false),
        Field::new("email", DataType::LargeUtf8, true),
    ]);
    let err = WireToArrowEncoder::new(&desc, schema).expect_err("should reject");
    assert!(
        matches!(
            &err,
            WireToArrowError::NonNullableNotGuaranteed { name, .. } if name == "id"
        ),
        "expected NonNullableNotGuaranteed for 'id', got {err:?}"
    );
}

#[test]
fn plan_build_allows_non_nullable_outer_list_and_map() {
    // Repeated and map outer columns are always-present (the encoder
    // emits an empty list / empty map for an absent occurrence), so a
    // non-nullable declaration on the outer column is safe and must
    // build without error.
    let desc = rich_descriptor();
    let phone_struct = DataType::Struct(ArrowFields::from(vec![Field::new(
        "number",
        DataType::LargeUtf8,
        true,
    )]));
    let phones_field = Field::new("item", phone_struct, true);
    let entry_fields = ArrowFields::from(vec![
        // Arrow Map keys are mandated non-nullable by the Map type
        // contract; the carve-out for Map entry sub-plans must let this
        // through.
        Field::new("key", DataType::LargeUtf8, false),
        Field::new("value", DataType::Int32, true),
    ]);
    let entry_field = Arc::new(Field::new(
        "key_value",
        DataType::Struct(entry_fields),
        false,
    ));
    let schema = Schema::new(vec![
        Field::new("name", DataType::LargeUtf8, true),
        // Outer list non-nullable: OK, the encoder writes empty-list, not null.
        Field::new(
            "phones",
            DataType::List(Arc::new(phones_field)),
            /* nullable */ false,
        ),
        // Outer map non-nullable: OK, same reason.
        Field::new(
            "data",
            DataType::Map(entry_field, false),
            /* nullable */ false,
        ),
    ]);
    WireToArrowEncoder::new(&desc, schema).expect("should build");
}

#[test]
fn plan_build_rejects_non_nullable_absent_column() {
    // Schema-drift case: Arrow schema has a column the proto descriptor
    // doesn't carry. The encoder fills it with all nulls; a non-nullable
    // declaration is a hard mismatch that must be rejected at init.
    let desc = scalar_descriptor();
    let schema = Schema::new(vec![
        Field::new("name", DataType::LargeUtf8, true),
        Field::new(
            "dropped_from_proto",
            DataType::Int64,
            /* nullable */ false,
        ),
    ]);
    let err = WireToArrowEncoder::new(&desc, schema).expect_err("should reject");
    assert!(
        matches!(
            &err,
            WireToArrowError::NonNullableNotGuaranteed { name, .. }
                if name == "dropped_from_proto"
        ),
        "expected NonNullableNotGuaranteed for absent column, got {err:?}"
    );
}

#[test]
fn plan_build_rejects_unsupported_arrow_leaf_in_scalar_slot() {
    // proto says `id: int32`, Arrow says `id: Date64`. Date64 isn't in
    // `TypedBuilder::supports`, so plan-build must reject up front
    // rather than letting the first batch panic inside `TypedBuilder::new`.
    let desc = scalar_descriptor();
    let schema = Schema::new(vec![
        Field::new("name", DataType::LargeUtf8, true),
        Field::new("id", DataType::Date64, true),
        Field::new("email", DataType::LargeUtf8, true),
    ]);
    let err = WireToArrowEncoder::new(&desc, schema).expect_err("should reject Date64");
    assert!(
        matches!(
            &err,
            WireToArrowError::UnsupportedArrowLeafType { name, .. } if name == "id"
        ),
        "expected UnsupportedArrowLeafType for 'id', got {err:?}"
    );
}

#[test]
fn plan_build_rejects_unsupported_arrow_leaf_in_absent_slot() {
    // `created_at` doesn't exist in test_protobuf.Person, so it becomes
    // PlanSlot::Absent. The Absent path builds via `build_absent_node`,
    // which also calls `TypedBuilder::new` for leaves. Plan-build must
    // validate the Arrow leaf type on absent slots too.
    let desc = scalar_descriptor();
    let schema = Schema::new(vec![
        Field::new("name", DataType::LargeUtf8, true),
        Field::new("created_at", DataType::Date64, true),
    ]);
    let err =
        WireToArrowEncoder::new(&desc, schema).expect_err("should reject Date64 on absent slot");
    assert!(
        matches!(
            &err,
            WireToArrowError::UnsupportedArrowLeafType { name, .. } if name == "created_at"
        ),
        "expected UnsupportedArrowLeafType for 'created_at', got {err:?}"
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
    let ids = batch
        .column(0)
        .as_primitive::<arrow::datatypes::Int32Type>();
    assert_eq!(ids.len(), 5);
    for i in 0..5 {
        assert_eq!(ids.value(i), (i as i32) * 10);
    }
}

#[test]
fn wire_descriptor_tags_drive_decode_regardless_of_arrow_schema_source() {
    // The two mappings the encoder relies on:
    //   (a) Arrow schema ↔ proto descriptor — by name, at plan-build time.
    //   (b) Proto descriptor ↔ wire bytes — by tag number, at scan time.
    // They are independent. An Arrow schema derived from any other
    // descriptor (e.g. the UC-synthesized one with position-based tags) must
    // not leak its tag numbers into the scan. Tags on the wire come from
    // whatever descriptor the *wire* side was encoded with, and that's the
    // only descriptor the serializer is told about.
    //
    // Here: wire descriptor uses tags 1001/1002/1003. If the scanner ever
    // fell back to a position-based or otherwise-synthesized tag space
    // (1/2/3), every wire tag would miss and the batch columns would be
    // all-null. Full population proves decode uses the wire descriptor's
    // tag numbers exclusively.
    let wire_fd = FileDescriptorProto {
        name: Some("wire_tag_divergence_test.proto".into()),
        package: Some("wire_tag_divergence_test".into()),
        message_type: vec![DescriptorProto {
            name: Some("Row".into()),
            field: vec![
                FieldDescriptorProto {
                    name: Some("name".into()),
                    number: Some(1001),
                    label: Some(Label::Optional as i32),
                    r#type: Some(ProtoType::String as i32),
                    ..Default::default()
                },
                FieldDescriptorProto {
                    name: Some("id".into()),
                    number: Some(1002),
                    label: Some(Label::Optional as i32),
                    r#type: Some(ProtoType::Int32 as i32),
                    ..Default::default()
                },
                FieldDescriptorProto {
                    name: Some("email".into()),
                    number: Some(1003),
                    label: Some(Label::Optional as i32),
                    r#type: Some(ProtoType::String as i32),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
        ..Default::default()
    };
    let set = FileDescriptorSet {
        file: vec![wire_fd],
    };
    let mut set_bytes = Vec::new();
    set.encode(&mut set_bytes).unwrap();
    let wire_desc = DescriptorPool::decode(set_bytes.as_slice())
        .unwrap()
        .get_message_by_name("wire_tag_divergence_test.Row")
        .unwrap();

    let arrow_schema = Schema::new(vec![
        Field::new("name", DataType::LargeUtf8, true),
        Field::new("id", DataType::Int32, true),
        Field::new("email", DataType::LargeUtf8, true),
    ]);
    let serializer = WireToArrowSerializer::from_descriptor(wire_desc.clone(), arrow_schema)
        .expect("serializer build");

    let mut msg = DynamicMessage::new(wire_desc);
    msg.set_field_by_name("name", ProtoValue::String("alice".into()));
    msg.set_field_by_name("id", ProtoValue::I32(42));
    msg.set_field_by_name("email", ProtoValue::String("alice@example.com".into()));
    let mut buf = Vec::new();
    msg.encode(&mut buf).unwrap();

    let batch = serializer
        .encode_to_record_batch(&[event_with_message_bytes(Bytes::from(buf))])
        .expect("encode");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 3);
    assert_eq!(batch.column(0).as_string::<i64>().value(0), "alice");
    assert_eq!(
        batch
            .column(1)
            .as_primitive::<arrow::datatypes::Int32Type>()
            .value(0),
        42
    );
    assert_eq!(
        batch.column(2).as_string::<i64>().value(0),
        "alice@example.com"
    );
}

// -------------------------------------------------------------------------
// Per-row isolation: a single malformed message in a batch must not poison
// the whole batch. The encoder pre-validates each message and drops bad
// rows from the output `RecordBatch`, counting them via the
// `wire_to_arrow_rows_dropped` metric.
// -------------------------------------------------------------------------

#[test]
fn encode_batch_drops_malformed_row_in_mixed_batch() {
    // A single malformed message in the middle of an otherwise-valid batch
    // must not fail the batch. The bad row is dropped; the valid rows still
    // appear in the output `RecordBatch` in original order.
    let desc = scalar_descriptor();
    let schema = Schema::new(vec![
        Field::new("name", DataType::LargeUtf8, true),
        Field::new("id", DataType::Int32, true),
    ]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    let mut a = DynamicMessage::new(desc.clone());
    a.set_field_by_name("name", ProtoValue::String("alice".into()));
    a.set_field_by_name("id", ProtoValue::I32(1));
    let mut buf_a = Vec::new();
    a.encode(&mut buf_a).unwrap();

    let mut b = DynamicMessage::new(desc.clone());
    b.set_field_by_name("name", ProtoValue::String("bob".into()));
    b.set_field_by_name("id", ProtoValue::I32(2));
    let mut buf_b = Vec::new();
    b.encode(&mut buf_b).unwrap();

    // Tag = (1 << 3) | 2 = 0x0a (`name`, LEN); declared length 5, only 2
    // payload bytes follow → BufferTooShort inside `try_parse_field`,
    // which the encoder maps to `UnexpectedEof`.
    let bad = vec![0x0a, 0x05, 0x01, 0x02];

    let batch = enc
        .encode_batch(&[Bytes::from(buf_a), Bytes::from(bad), Bytes::from(buf_b)])
        .expect("malformed row must not fail the batch");
    assert_eq!(batch.num_rows(), 2);
    let names = batch.column(0).as_string::<i64>();
    assert_eq!(names.value(0), "alice");
    assert_eq!(names.value(1), "bob");
    let ids = batch
        .column(1)
        .as_primitive::<arrow::datatypes::Int32Type>();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
}

#[test]
fn encode_batch_drops_invalid_utf8_row() {
    // String field with non-UTF-8 payload — the pre-validate pass catches
    // this via the `from_utf8` check in `validate_scalar_from_wire`.
    let desc = scalar_descriptor();
    let schema = Schema::new(vec![Field::new("name", DataType::LargeUtf8, true)]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    // Tag = (1 << 3) | 2 = 0x0a (`name`, LEN); length = 1; payload = 0xff
    // (lone continuation byte, not a valid UTF-8 sequence).
    let bad = vec![0x0a, 0x01, 0xff];

    let mut ok = DynamicMessage::new(desc.clone());
    ok.set_field_by_name("name", ProtoValue::String("ok".into()));
    let mut ok_buf = Vec::new();
    ok.encode(&mut ok_buf).unwrap();

    let batch = enc
        .encode_batch(&[Bytes::from(bad), Bytes::from(ok_buf)])
        .expect("invalid-UTF-8 row must not fail the batch");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.column(0).as_string::<i64>().value(0), "ok");
}

#[test]
fn encode_batch_all_malformed_returns_empty_batch() {
    // Every row malformed → empty `RecordBatch` (schema preserved, zero
    // rows). Surfacing the situation is the metric's job; the serializer
    // doesn't conflate "every row was bad" with "configuration broken".
    let desc = scalar_descriptor();
    let schema = Schema::new(vec![Field::new("name", DataType::LargeUtf8, true)]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    let bad1 = vec![0x0a, 0x05, 0x00, 0x01]; // truncated LEN value
    let bad2 = vec![0x0a, 0x01, 0xff]; // invalid UTF-8

    let batch = enc
        .encode_batch(&[Bytes::from(bad1), Bytes::from(bad2)])
        .expect("all-malformed batch must still return an empty RecordBatch");
    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.num_columns(), 1);
}

#[test]
fn encode_batch_drops_packed_scalar_eof_row() {
    // Packed repeated int32 with a truncated varint in the inner blob —
    // the one site (`append_repeated_scalar`'s packed loop) where the
    // real scan could otherwise leave half the elements committed before
    // erroring. Pre-validate sees the EOF and drops the row before any
    // append.
    let desc = repeated_int32_descriptor();
    let schema = Schema::new(vec![Field::new(
        "numbers",
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        true,
    )]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    // Tag = (1 << 3) | 2 = 0x0a (`numbers`, LEN — packed form); length 2;
    // inner = [0x80, 0x80] — a varint with continuation bits and no
    // terminator → EOF inside the packed walk.
    let bad = vec![0x0a, 0x02, 0x80, 0x80];
    // Packed encoding of `numbers = [7]`: one varint (7) inside a LEN blob.
    let good = vec![0x0a, 0x01, 0x07];

    let batch = enc
        .encode_batch(&[Bytes::from(bad), Bytes::from(good)])
        .expect("packed EOF row must not fail the batch");
    assert_eq!(batch.num_rows(), 1);
    let list = batch.column(0).as_list::<i32>();
    assert_eq!(list.value_length(0), 1);
}

#[test]
fn encode_batch_duplicate_singular_scalar_uses_last_value() {
    let desc = scalar_descriptor();
    let schema = Schema::new(vec![
        Field::new("name", DataType::LargeUtf8, true),
        Field::new("id", DataType::Int32, true),
    ]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    let mut a = DynamicMessage::new(desc.clone());
    a.set_field_by_name("name", ProtoValue::String("alice".into()));
    a.set_field_by_name("id", ProtoValue::I32(1));
    let mut buf_a = Vec::new();
    a.encode(&mut buf_a).unwrap();

    let mut b = DynamicMessage::new(desc.clone());
    b.set_field_by_name("name", ProtoValue::String("bob".into()));
    b.set_field_by_name("id", ProtoValue::I32(2));
    let mut buf_b = Vec::new();
    b.encode(&mut buf_b).unwrap();

    // Hand-rolled bytes: two occurrences of singular tag 1 (`name`, LEN).
    //   0x0a 0x03 "bob"     — first occurrence, value "bob"
    //   0x0a 0x05 "alice"   — second occurrence, value "alice"
    // Spec says last-wins ("alice").
    let dup = vec![
        0x0a, 0x03, b'b', b'o', b'b', 0x0a, 0x05, b'a', b'l', b'i', b'c', b'e',
    ];

    let batch = enc
        .encode_batch(&[Bytes::from(buf_a), Bytes::from(dup), Bytes::from(buf_b)])
        .expect("duplicate-tag row must not fail the batch");
    assert_eq!(batch.num_rows(), 3);
    let names = batch.column(0).as_string::<i64>();
    assert_eq!(names.value(0), "alice");
    assert_eq!(names.value(1), "alice");
    assert_eq!(names.value(2), "bob");
    let ids = batch
        .column(1)
        .as_primitive::<arrow::datatypes::Int32Type>();
    assert_eq!(ids.value(0), 1);
    assert!(ids.is_null(1));
    assert_eq!(ids.value(2), 2);
}

#[test]
fn append_repeated_scalar_unpacked_offset_overflow_is_clean_error() {
    // The Arrow `ListArray` uses i32 offsets, so `current_offset` can grow
    // at most to `i32::MAX` across a batch. Without bounds checking the
    // `+= 1` wraps in release mode and `OffsetBuffer::new` later asserts at
    // batch finish, panicking the process. The encoder uses `checked_add`
    // here so an overflow surfaces as a structured `OffsetOverflow` error.
    // This converts a process-panic surface (adversarial input → crash) into
    // a clean batch-level failure.
    use super::append::append_repeated_scalar;
    use super::builders::TypedBuilder;
    let mut values = TypedBuilder::new(&DataType::Int32, 1);
    let mut current_offset: i32 = i32::MAX;
    let wv = WireValue::Varint(42);
    let err = append_repeated_scalar(ScalarKind::Int32, &wv, &mut values, &mut current_offset)
        .expect_err("unpacked repeated-scalar increment at i32::MAX must error");
    assert!(
        matches!(err, WireToArrowError::OffsetOverflow { .. }),
        "expected OffsetOverflow, got {err:?}",
    );
}

#[test]
fn append_repeated_scalar_packed_offset_overflow_is_clean_error() {
    // Same property for the packed-blob inner loop. A two-element packed
    // varint starting from `current_offset == i32::MAX - 1` overflows on
    // the second element; the loop's `checked_add` must surface that as
    // `OffsetOverflow` rather than wrapping silently.
    use super::append::append_repeated_scalar;
    use super::builders::TypedBuilder;
    let mut values = TypedBuilder::new(&DataType::Int32, 4);
    // Two varint elements (7, 8) packed inline. We synthesize at the
    // WireValue layer, so the outer tag + length prefix have already been
    // stripped — only the inner packed payload appears here.
    let blob: Vec<u8> = vec![0x07, 0x08];
    let wv = WireValue::Len(&blob);
    let mut current_offset: i32 = i32::MAX - 1;
    let err = append_repeated_scalar(ScalarKind::Int32, &wv, &mut values, &mut current_offset)
        .expect_err("packed-scalar increment crossing i32::MAX must error");
    assert!(
        matches!(err, WireToArrowError::OffsetOverflow { .. }),
        "expected OffsetOverflow, got {err:?}",
    );
}

#[test]
fn map_preserves_user_entry_field_name_and_metadata() {
    // Arrow's Map spec doesn't pin a specific entry name — "entries" is
    // canonical in Arrow itself; "key_value" is the Spark/Delta convention
    // the zerobus sink uses. Before the fix the encoder hardcoded
    // "key_value" at finish time, so any caller whose schema declared a
    // different entry name (or attached metadata) got `RecordBatch::try_new`
    // rejection on every batch. The fix preserves the user-supplied entry
    // Field unchanged.
    let desc = rich_descriptor();
    let entry_fields = ArrowFields::from(vec![
        Field::new("key", DataType::LargeUtf8, false),
        Field::new("value", DataType::Int32, true),
    ]);
    let user_entry = Arc::new(
        Field::new("entries", DataType::Struct(entry_fields), false).with_metadata(
            [("source".to_string(), "unit_test".to_string())]
                .into_iter()
                .collect::<std::collections::HashMap<_, _>>(),
        ),
    );
    let outer_field = Field::new("data", DataType::Map(Arc::clone(&user_entry), false), true);
    let schema = Schema::new(vec![outer_field.clone()]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    let mut msg = DynamicMessage::new(desc.clone());
    let mut entries: std::collections::HashMap<prost_reflect::MapKey, ProtoValue> =
        std::collections::HashMap::new();
    entries.insert(
        prost_reflect::MapKey::String("k1".into()),
        ProtoValue::EnumNumber(1),
    );
    msg.set_field_by_name("data", ProtoValue::Map(entries));
    let mut buf = Vec::new();
    msg.encode(&mut buf).unwrap();

    let batch = enc
        .encode_batch(&[Bytes::from(buf)])
        .expect("batch must finish with the user-supplied entry name preserved");
    assert_eq!(batch.num_rows(), 1);
    // The schema the batch reports must match what we declared, including
    // the non-default entry name and the metadata we attached.
    let col_field = batch.schema().field(0).clone();
    let actual_entry = match col_field.data_type() {
        DataType::Map(f, _) => Arc::clone(f),
        other => panic!("expected Map, got {other:?}"),
    };
    assert_eq!(actual_entry.name(), "entries");
    assert_eq!(
        actual_entry.metadata().get("source").map(String::as_str),
        Some("unit_test"),
        "user-attached entry metadata must round-trip",
    );
}

// -------------------------------------------------------------------------
// Enum -> STRING column (PlanSlot::EnumString): render the proto enum varint
// as its value name, matching the arrow_stream / `proto_to_value` path.
// -------------------------------------------------------------------------

/// Build `message Msg { Outcome outcome = 1; }` with
/// `enum Outcome { UNSPECIFIED = 0; SUCCESS = 1; FAILURE = 2; }`.
fn singular_enum_descriptor() -> MessageDescriptor {
    use prost_reflect::prost_types::{EnumDescriptorProto, EnumValueDescriptorProto};
    let fd = FileDescriptorProto {
        name: Some("wire_to_arrow_enum_test.proto".into()),
        package: Some("wire_to_arrow_enum_test".into()),
        syntax: Some("proto3".into()),
        enum_type: vec![EnumDescriptorProto {
            name: Some("Outcome".into()),
            value: vec![
                EnumValueDescriptorProto {
                    name: Some("UNSPECIFIED".into()),
                    number: Some(0),
                    ..Default::default()
                },
                EnumValueDescriptorProto {
                    name: Some("SUCCESS".into()),
                    number: Some(1),
                    ..Default::default()
                },
                EnumValueDescriptorProto {
                    name: Some("FAILURE".into()),
                    number: Some(2),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
        message_type: vec![DescriptorProto {
            name: Some("Msg".into()),
            field: vec![
                FieldDescriptorProto {
                    name: Some("outcome".into()),
                    number: Some(1),
                    label: Some(Label::Optional as i32),
                    r#type: Some(ProtoType::Enum as i32),
                    type_name: Some(".wire_to_arrow_enum_test.Outcome".into()),
                    ..Default::default()
                },
                FieldDescriptorProto {
                    name: Some("outcomes".into()),
                    number: Some(2),
                    label: Some(Label::Repeated as i32),
                    r#type: Some(ProtoType::Enum as i32),
                    type_name: Some(".wire_to_arrow_enum_test.Outcome".into()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
        ..Default::default()
    };
    let set = FileDescriptorSet { file: vec![fd] };
    let mut bytes = Vec::new();
    set.encode(&mut bytes).unwrap();
    DescriptorPool::decode(bytes.as_slice())
        .unwrap()
        .get_message_by_name("wire_to_arrow_enum_test.Msg")
        .unwrap()
}

#[test]
fn enum_to_string_renders_value_name() {
    let desc = singular_enum_descriptor();
    let schema = Schema::new(vec![Field::new("outcome", DataType::LargeUtf8, true)]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    let mut msg = DynamicMessage::new(desc.clone());
    msg.set_field_by_name("outcome", ProtoValue::EnumNumber(1)); // SUCCESS
    let mut buf = Vec::new();
    msg.encode(&mut buf).unwrap();

    let batch = enc.encode_batch(&[Bytes::from(buf)]).unwrap();
    assert_eq!(batch.column(0).as_string::<i64>().value(0), "SUCCESS");
}

#[test]
fn enum_to_string_absent_is_null() {
    // proto3 elides an enum at its zero value; `proto_to_value` only walks
    // present fields, so an absent enum -> null (NOT the name of value 0).
    let desc = singular_enum_descriptor();
    let schema = Schema::new(vec![Field::new("outcome", DataType::LargeUtf8, true)]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    // Empty message: `outcome` absent.
    let batch = enc.encode_batch(&[Bytes::new()]).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(batch.column(0).is_null(0), "absent enum must be null");
}

#[test]
fn enum_to_string_unknown_value_renders_unrecognized() {
    // An out-of-range enum number (e.g. a value added upstream after this
    // binary was built) has no descriptor entry. The classic Lumberjack ETL
    // writes ScalaPB's `UNRECOGNIZED` rather than dropping the row.
    let desc = singular_enum_descriptor();
    let schema = Schema::new(vec![Field::new("outcome", DataType::LargeUtf8, true)]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    let mut good = DynamicMessage::new(desc.clone());
    good.set_field_by_name("outcome", ProtoValue::EnumNumber(2)); // FAILURE
    let mut good_buf = Vec::new();
    good.encode(&mut good_buf).unwrap();

    let mut unknown = DynamicMessage::new(desc.clone());
    unknown.set_field_by_name("outcome", ProtoValue::EnumNumber(99)); // undefined
    let mut unknown_buf = Vec::new();
    unknown.encode(&mut unknown_buf).unwrap();

    let batch = enc
        .encode_batch(&[Bytes::from(good_buf), Bytes::from(unknown_buf)])
        .expect("unknown-enum row must not fail the batch");
    assert_eq!(batch.num_rows(), 2, "the unknown-enum row must be kept");
    let col = batch.column(0).as_string::<i64>();
    assert_eq!(col.value(0), "FAILURE");
    assert_eq!(col.value(1), "UNRECOGNIZED");
}

#[test]
fn repeated_enum_to_string_renders_value_names() {
    // `repeated Outcome` -> List<LargeUtf8>: each element rendered by name.
    let desc = singular_enum_descriptor();
    let outcomes = Field::new("item", DataType::LargeUtf8, true);
    let schema = Schema::new(vec![Field::new(
        "outcomes",
        DataType::List(Arc::new(outcomes)),
        true,
    )]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    let mut msg = DynamicMessage::new(desc.clone());
    msg.set_field_by_name(
        "outcomes",
        ProtoValue::List(vec![
            ProtoValue::EnumNumber(1), // SUCCESS
            ProtoValue::EnumNumber(2), // FAILURE
            ProtoValue::EnumNumber(0), // UNSPECIFIED
        ]),
    );
    let mut buf = Vec::new();
    msg.encode(&mut buf).unwrap();

    let batch = enc.encode_batch(&[Bytes::from(buf)]).unwrap();
    let list = batch.column(0).as_list::<i32>();
    let vals = list.value(0);
    let strs = vals.as_string::<i64>();
    assert_eq!(strs.len(), 3);
    assert_eq!(strs.value(0), "SUCCESS");
    assert_eq!(strs.value(1), "FAILURE");
    assert_eq!(strs.value(2), "UNSPECIFIED");
}

#[test]
fn repeated_enum_to_string_empty_when_absent() {
    // An absent repeated field is an empty list (never null), like every
    // other repeated slot.
    let desc = singular_enum_descriptor();
    let outcomes = Field::new("item", DataType::LargeUtf8, true);
    let schema = Schema::new(vec![Field::new(
        "outcomes",
        DataType::List(Arc::new(outcomes)),
        true,
    )]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    let batch = enc.encode_batch(&[Bytes::new()]).unwrap();
    assert_eq!(batch.num_rows(), 1);
    let list = batch.column(0).as_list::<i32>();
    assert!(
        !list.is_null(0),
        "absent repeated enum must be empty list, not null"
    );
    assert_eq!(list.value(0).len(), 0);
}

#[test]
fn repeated_enum_to_string_unknown_value_renders_placeholder() {
    // An out-of-range element renders its placeholder in place (parity with the
    // singular case); the row is kept and known elements are unaffected.
    let desc = singular_enum_descriptor();
    let outcomes = Field::new("item", DataType::LargeUtf8, true);
    let schema = Schema::new(vec![Field::new(
        "outcomes",
        DataType::List(Arc::new(outcomes)),
        true,
    )]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    let mut msg = DynamicMessage::new(desc.clone());
    msg.set_field_by_name(
        "outcomes",
        ProtoValue::List(vec![ProtoValue::EnumNumber(1), ProtoValue::EnumNumber(99)]),
    );
    let mut buf = Vec::new();
    msg.encode(&mut buf).unwrap();

    let batch = enc
        .encode_batch(&[Bytes::from(buf)])
        .expect("unknown-enum element must not fail the batch");
    assert_eq!(
        batch.num_rows(),
        1,
        "the row with an unknown element must be kept"
    );
    let list = batch.column(0).as_list::<i32>();
    let strs = list.value(0);
    let strs = strs.as_string::<i64>();
    assert_eq!(strs.len(), 2);
    assert_eq!(strs.value(0), "SUCCESS");
    assert_eq!(strs.value(1), "UNRECOGNIZED");
}

// -------------------------------------------------------------------------
// VARIANT: a proto string carrying JSON text -> Arrow Struct<metadata, value>
// marked with the `arrow.parquet.variant` extension, encoded into the Parquet
// Variant binary form.
// -------------------------------------------------------------------------

/// A VARIANT Arrow field: marked `Struct<metadata, value>` (both non-nullable
/// LargeBinary), as the zerobus SDK emits with `annotate_variant_extension`.
fn variant_arrow_field(name: &str) -> Field {
    let children = ArrowFields::from(vec![
        Field::new("metadata", DataType::LargeBinary, false),
        Field::new("value", DataType::LargeBinary, false),
    ]);
    Field::new(name, DataType::Struct(children), true).with_metadata(
        std::collections::HashMap::from([(
            "ARROW:extension:name".to_string(),
            "arrow.parquet.variant".to_string(),
        )]),
    )
}

/// Decode a VARIANT cell back to a `serde_json::Value` for order-independent
/// comparison.
fn decode_variant_wire(col: &arrow::array::ArrayRef, row: usize) -> serde_json::Value {
    use arrow::array::LargeBinaryArray;
    use parquet_variant::Variant;
    use parquet_variant_json::VariantToJson;

    let s = col.as_struct();
    let metadata = s
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    let value = s
        .column(1)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    let variant = Variant::new(metadata.value(row), value.value(row));
    serde_json::from_str(&variant.to_json_string().unwrap()).unwrap()
}

#[test]
fn variant_column_from_json_string() {
    // Person.name is a proto string; declare its Arrow column as a VARIANT so
    // the JSON text on the wire is re-encoded into Parquet Variant binary.
    let desc = scalar_descriptor();
    let schema = Schema::new(vec![
        variant_arrow_field("name"),
        Field::new("id", DataType::Int32, true),
    ]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    let mut msg = DynamicMessage::new(desc.clone());
    msg.set_field_by_name("name", ProtoValue::String(r#"{"a":1,"b":"x"}"#.into()));
    msg.set_field_by_name("id", ProtoValue::I32(7));
    let mut buf = Vec::new();
    msg.encode(&mut buf).unwrap();

    let batch = enc.encode_batch(&[Bytes::from(buf)]).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(!batch.column(0).is_null(0));
    assert_eq!(
        decode_variant_wire(batch.column(0), 0),
        serde_json::json!({"a": 1, "b": "x"})
    );
}

#[test]
fn variant_column_absent_is_null() {
    // proto3 elides an unset (or empty) string field, so an absent `name`
    // yields a null VARIANT row.
    let desc = scalar_descriptor();
    let schema = Schema::new(vec![
        variant_arrow_field("name"),
        Field::new("id", DataType::Int32, true),
    ]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    let mut msg = DynamicMessage::new(desc.clone());
    msg.set_field_by_name("id", ProtoValue::I32(7)); // name omitted
    let mut buf = Vec::new();
    msg.encode(&mut buf).unwrap();

    let batch = enc.encode_batch(&[Bytes::from(buf)]).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(batch.column(0).is_null(0));
}

#[test]
fn variant_column_drops_malformed_json_row() {
    // Non-JSON text in a VARIANT wire field is caught by the pre-validate pass
    // and the row is dropped, matching the per-row isolation of other decode
    // failures. A valid row in the same batch survives.
    let desc = scalar_descriptor();
    let schema = Schema::new(vec![
        variant_arrow_field("name"),
        Field::new("id", DataType::Int32, true),
    ]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    let mut good = DynamicMessage::new(desc.clone());
    good.set_field_by_name("name", ProtoValue::String(r#"{"ok":true}"#.into()));
    good.set_field_by_name("id", ProtoValue::I32(1));
    let mut buf_good = Vec::new();
    good.encode(&mut buf_good).unwrap();

    let mut bad = DynamicMessage::new(desc.clone());
    bad.set_field_by_name("name", ProtoValue::String("not json".into()));
    bad.set_field_by_name("id", ProtoValue::I32(2));
    let mut buf_bad = Vec::new();
    bad.encode(&mut buf_bad).unwrap();

    let batch = enc
        .encode_batch(&[Bytes::from(buf_bad), Bytes::from(buf_good)])
        .expect("malformed variant row must not fail the batch");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        decode_variant_wire(batch.column(0), 0),
        serde_json::json!({"ok": true})
    );
}

/// `message Bag { repeated string items = 1; }` — a bare `repeated string`,
/// the wire shape of `ARRAY<VARIANT>` (VARIANT maps to proto string).
fn repeated_string_descriptor() -> MessageDescriptor {
    let fd = FileDescriptorProto {
        name: Some("wire_to_arrow_variant_test.proto".into()),
        package: Some("wire_to_arrow_variant_test".into()),
        message_type: vec![DescriptorProto {
            name: Some("Bag".into()),
            field: vec![FieldDescriptorProto {
                name: Some("items".into()),
                number: Some(1),
                label: Some(Label::Repeated as i32),
                r#type: Some(ProtoType::String as i32),
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
        .get_message_by_name("wire_to_arrow_variant_test.Bag")
        .unwrap()
}

#[test]
fn array_of_variant_from_repeated_json_strings() {
    use arrow::array::ListArray;

    let desc = repeated_string_descriptor();
    let schema = Schema::new(vec![Field::new(
        "items",
        DataType::List(Arc::new(variant_arrow_field("item"))),
        true,
    )]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    let mut msg = DynamicMessage::new(desc.clone());
    msg.set_field_by_name(
        "items",
        ProtoValue::List(vec![
            ProtoValue::String(r#"{"a":1}"#.into()),
            ProtoValue::String(r#""hi""#.into()),
        ]),
    );
    let mut buf = Vec::new();
    msg.encode(&mut buf).unwrap();

    let batch = enc.encode_batch(&[Bytes::from(buf)]).unwrap();
    assert_eq!(batch.num_rows(), 1);
    let list = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let items = list.values();
    assert_eq!(items.len(), 2);
    assert_eq!(decode_variant_wire(items, 0), serde_json::json!({"a": 1}));
    assert_eq!(decode_variant_wire(items, 1), serde_json::json!("hi"));
}

/// `message Container { KV single = 1; repeated KV many = 2; }` with
/// `message KV { string k = 1; int64 n = 2; repeated int32 nums = 3; }` — a singular and a repeated
/// nested-message field, for the message-backed VARIANT path.
fn message_variant_descriptor() -> MessageDescriptor {
    message_variant_descriptor_with_syntax("proto2")
}

fn message_variant_descriptor_with_syntax(syntax: &str) -> MessageDescriptor {
    let fd = FileDescriptorProto {
        syntax: Some(syntax.into()),
        name: Some("wire_to_arrow_msg_variant_test.proto".into()),
        package: Some("wire_to_arrow_msg_variant_test".into()),
        message_type: vec![
            DescriptorProto {
                name: Some("Container".into()),
                field: vec![
                    FieldDescriptorProto {
                        name: Some("single".into()),
                        number: Some(1),
                        label: Some(Label::Optional as i32),
                        r#type: Some(ProtoType::Message as i32),
                        type_name: Some(".wire_to_arrow_msg_variant_test.KV".into()),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("many".into()),
                        number: Some(2),
                        label: Some(Label::Repeated as i32),
                        r#type: Some(ProtoType::Message as i32),
                        type_name: Some(".wire_to_arrow_msg_variant_test.KV".into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
            DescriptorProto {
                name: Some("KV".into()),
                field: vec![
                    FieldDescriptorProto {
                        name: Some("k".into()),
                        number: Some(1),
                        label: Some(Label::Optional as i32),
                        r#type: Some(ProtoType::String as i32),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("n".into()),
                        number: Some(2),
                        label: Some(Label::Optional as i32),
                        r#type: Some(ProtoType::Int64 as i32),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("nums".into()),
                        number: Some(3),
                        label: Some(Label::Repeated as i32),
                        r#type: Some(ProtoType::Int32 as i32),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let set = FileDescriptorSet { file: vec![fd] };
    let mut bytes = Vec::new();
    set.encode(&mut bytes).unwrap();
    DescriptorPool::decode(bytes.as_slice())
        .unwrap()
        .get_message_by_name("wire_to_arrow_msg_variant_test.Container")
        .unwrap()
}

/// Fetch the `KV` sub-message descriptor from `Container`'s `single` field.
fn kv_descriptor(container: &MessageDescriptor) -> MessageDescriptor {
    container
        .get_field_by_name("single")
        .unwrap()
        .kind()
        .as_message()
        .unwrap()
        .clone()
}

#[test]
fn wire_type_mismatch_reports_root_to_leaf_proto_field_numbers() {
    let desc = message_variant_descriptor();
    let fields = ArrowFields::from(vec![
        Field::new("k", DataType::LargeUtf8, true),
        Field::new("n", DataType::Int64, true),
    ]);
    let schema = Schema::new(vec![
        Field::new("single", DataType::Struct(fields.clone()), true),
        Field::new(
            "many",
            DataType::List(Arc::new(Field::new("item", DataType::Struct(fields), true))),
            true,
        ),
    ]);
    let plan = super::plan::MessagePlan::build(&desc, schema.fields(), Default::default()).unwrap();
    let cases: &[(&[u8], &[u32], u8, u8)] = &[
        (&[0x08, 0x00], &[1], 2, 0),
        (&[0x0a, 0x02, 0x12, 0x00], &[1, 2], 0, 2),
        (&[0x12, 0x02, 0x08, 0x00], &[2, 1], 2, 0),
    ];
    // `DropRow` keeps the historical error with the root-to-leaf path.
    for &(wire, path, expected_type, actual_type) in cases {
        let error = validate_with_mode(&plan, wire, OnWireTypeMismatch::DropRow).unwrap_err();
        assert!(
            error
                .to_string()
                .contains(&format!("proto field path {path:?}")),
            "{error}"
        );
        match error {
            WireToArrowError::WireTypeMismatchAtField {
                field_numbers,
                expected,
                actual,
            } => {
                assert_eq!(field_numbers, path);
                assert_eq!(expected, expected_type);
                assert_eq!(actual, actual_type);
            }
            other => panic!("expected a field path for wire mismatch: {other}"),
        }
    }
    // `SkipField` accepts the same rows and records one skip each, with the
    // same root-to-leaf path in the sample.
    for &(wire, path, expected_type, actual_type) in cases {
        let (result, skips) =
            validate_with_mode_and_skips(&plan, wire, OnWireTypeMismatch::SkipField);
        result.unwrap();
        assert_eq!(skips.skipped, 1, "wire={wire:?}");
        let sample = skips.sample.unwrap();
        assert_eq!(sample.field_numbers, path);
        assert_eq!(sample.expected, expected_type);
        assert_eq!(sample.actual, actual_type);
    }
}

/// Run `validate_message` on one row with the given mismatch policy.
fn validate_with_mode(
    plan: &super::plan::MessagePlan,
    wire: &[u8],
    on_mismatch: OnWireTypeMismatch,
) -> super::errors::Result<()> {
    validate_with_mode_and_skips(plan, wire, on_mismatch).0
}

fn validate_with_mode_and_skips(
    plan: &super::plan::MessagePlan,
    wire: &[u8],
    on_mismatch: OnWireTypeMismatch,
) -> (super::errors::Result<()>, super::scan::SkipAccumulator) {
    let row = Bytes::from(wire.to_vec());
    let mut variants = super::scan::BuiltVariants::default();
    let mut skips = super::scan::SkipAccumulator::default();
    let mut row_skips = super::scan::RowSkips::default();
    let mut path = Vec::new();
    row_skips.reset(true);
    let mut needs_normalization = None;
    let mut ctx = super::scan::ValidateCtx {
        on_mismatch,
        row: &row,
        skips: &mut row_skips,
        path: &mut path,
        needs_normalization: Some(&mut needs_normalization),
    };
    let result = super::scan::validate_message(plan, wire, &mut variants, &mut ctx)
        .and_then(|()| needs_normalization.map_or(Ok(()), Err));
    if result.is_ok() {
        row_skips.commit(&mut skips, &row);
    }
    (result, skips)
}

#[test]
fn variant_column_from_singular_message() {
    // A singular nested proto message pointed at a variant-marked struct column
    // is decoded and serialized to JSON, then encoded into Parquet Variant.
    // int64 stays numeric; the unset `n` on the second field is skipped
    // (skip_default_fields), so only present fields appear.
    let desc = message_variant_descriptor();
    let kv = kv_descriptor(&desc);
    let schema = Schema::new(vec![variant_arrow_field("single")]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    let mut sub = DynamicMessage::new(kv);
    sub.set_field_by_name("k", ProtoValue::String("x".into()));
    sub.set_field_by_name("n", ProtoValue::I64(5));
    let mut msg = DynamicMessage::new(desc.clone());
    msg.set_field_by_name("single", ProtoValue::Message(sub));
    let mut buf = Vec::new();
    msg.encode(&mut buf).unwrap();

    let batch = enc.encode_batch(&[Bytes::from(buf)]).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(!batch.column(0).is_null(0));
    assert_eq!(
        decode_variant_wire(batch.column(0), 0),
        serde_json::json!({"k": "x", "n": 5})
    );
}

#[test]
fn array_of_variant_from_repeated_messages() {
    use arrow::array::ListArray;

    // A `repeated KV` field pointed at Arrow List<variant> — each element is a
    // message decoded to JSON then Variant. The default-valued `n` on the
    // second element is skipped.
    let desc = message_variant_descriptor();
    let kv = kv_descriptor(&desc);
    let schema = Schema::new(vec![Field::new(
        "many",
        DataType::List(Arc::new(variant_arrow_field("item"))),
        true,
    )]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    let mut kv1 = DynamicMessage::new(kv.clone());
    kv1.set_field_by_name("k", ProtoValue::String("a".into()));
    kv1.set_field_by_name("n", ProtoValue::I64(1));
    let mut kv2 = DynamicMessage::new(kv);
    kv2.set_field_by_name("k", ProtoValue::String("b".into())); // n unset -> skipped
    let mut msg = DynamicMessage::new(desc.clone());
    msg.set_field_by_name(
        "many",
        ProtoValue::List(vec![ProtoValue::Message(kv1), ProtoValue::Message(kv2)]),
    );
    let mut buf = Vec::new();
    msg.encode(&mut buf).unwrap();

    let batch = enc.encode_batch(&[Bytes::from(buf)]).unwrap();
    assert_eq!(batch.num_rows(), 1);
    let list = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let items = list.values();
    assert_eq!(items.len(), 2);
    assert_eq!(
        decode_variant_wire(items, 0),
        serde_json::json!({"k": "a", "n": 1})
    );
    assert_eq!(decode_variant_wire(items, 1), serde_json::json!({"k": "b"}));
}

#[test]
fn variant_message_drops_undecodable_row() {
    // A variant-message column whose sub-message bytes are a valid LEN blob at
    // the outer level but fail proto decode (truncated varint) drops via
    // per-row isolation — VariantMessageDecode caught in the pre-pass — while a
    // valid row in the same batch survives.
    let desc = message_variant_descriptor();
    let kv = kv_descriptor(&desc);
    let schema = Schema::new(vec![variant_arrow_field("single")]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    // Outer: field 1 (single, message) tag 0x0a, length 1, payload 0x08 — a KV
    // sub-message with a field-1 varint tag and no value byte => decode error.
    let bad = vec![0x0a, 0x01, 0x08];

    let mut sub = DynamicMessage::new(kv);
    sub.set_field_by_name("k", ProtoValue::String("ok".into()));
    let mut good = DynamicMessage::new(desc.clone());
    good.set_field_by_name("single", ProtoValue::Message(sub));
    let mut buf_good = Vec::new();
    good.encode(&mut buf_good).unwrap();

    let batch = enc
        .encode_batch(&[Bytes::from(bad), Bytes::from(buf_good)])
        .expect("undecodable variant-message row must not fail the batch");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        decode_variant_wire(batch.column(0), 0),
        serde_json::json!({"k": "ok"})
    );
}

/// Encode `msg` to wire bytes.
fn encode_message(msg: &DynamicMessage) -> Vec<u8> {
    let mut buf = Vec::new();
    msg.encode(&mut buf).unwrap();
    buf
}

/// A `KV` sub-message with `k` set and, when non-zero, `n`.
fn kv_message(kv: &MessageDescriptor, k: &str, n: i64) -> DynamicMessage {
    let mut sub = DynamicMessage::new(kv.clone());
    sub.set_field_by_name("k", ProtoValue::String(k.into()));
    if n != 0 {
        sub.set_field_by_name("n", ProtoValue::I64(n));
    }
    sub
}

#[test]
fn duplicate_proto3_scalar_preserves_explicit_defaults_in_all_columns() {
    let desc = kv_descriptor(&message_variant_descriptor_with_syntax("proto3"));
    let schema = Schema::new(vec![
        Field::new("k", DataType::LargeUtf8, true),
        Field::new("n", DataType::Int64, true),
    ]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();
    // Both rows explicitly carry empty k and zero n. The second also carries
    // an earlier k value; normalization must preserve presence in both columns.
    let batch = enc
        .encode_batch(&[
            Bytes::from_static(&[0x0a, 0x00, 0x10, 0x00]),
            Bytes::from_static(&[0x0a, 0x01, b'x', 0x0a, 0x00, 0x10, 0x00]),
        ])
        .unwrap();
    assert_eq!(batch.num_rows(), 2);
    for row in 0..2 {
        assert!(!batch.column(0).is_null(row));
        assert_eq!(batch.column(0).as_string::<i64>().value(row), "");
        assert!(!batch.column(1).is_null(row));
        assert_eq!(
            batch
                .column(1)
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(row),
            0
        );
    }
}

#[test]
fn duplicate_singular_message_preserves_nested_explicit_defaults() {
    let desc = message_variant_descriptor_with_syntax("proto3");
    let fields = ArrowFields::from(vec![
        Field::new("k", DataType::LargeUtf8, true),
        Field::new("n", DataType::Int64, true),
    ]);
    let enc = WireToArrowEncoder::new(
        &desc,
        Schema::new(vec![Field::new("single", DataType::Struct(fields), true)]),
    )
    .unwrap();
    // single { n: 11 }, single { k: "", n: 0 }.
    let batch = enc
        .encode_batch(&[Bytes::from_static(&[
            0x0a, 0x02, 0x10, 0x0b, 0x0a, 0x04, 0x0a, 0x00, 0x10, 0x00,
        ])])
        .unwrap();
    assert_eq!(batch.num_rows(), 1);
    let single = batch.column(0).as_struct();
    assert!(!single.column(0).is_null(0));
    assert_eq!(single.column(0).as_string::<i64>().value(0), "");
    assert!(!single.column(1).is_null(0));
    assert_eq!(
        single
            .column(1)
            .as_primitive::<arrow::datatypes::Int64Type>()
            .value(0),
        0
    );
}

#[test]
fn duplicate_singular_message_concatenates_nested_repeated_fields() {
    let desc = message_variant_descriptor();
    let fields = ArrowFields::from(vec![Field::new(
        "nums",
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        true,
    )]);
    let enc = WireToArrowEncoder::new(
        &desc,
        Schema::new(vec![Field::new("single", DataType::Struct(fields), true)]),
    )
    .unwrap();
    // single { nums: [1, 2] }, single { nums: [3] }.
    let batch = enc
        .encode_batch(&[Bytes::from_static(&[
            0x0a, 0x04, 0x18, 0x01, 0x18, 0x02, 0x0a, 0x02, 0x18, 0x03,
        ])])
        .unwrap();
    assert_eq!(batch.num_rows(), 1);
    let nums = batch
        .column(0)
        .as_struct()
        .column(0)
        .as_list::<i32>()
        .value(0);
    assert_eq!(
        nums.as_primitive::<arrow::datatypes::Int32Type>()
            .values()
            .as_ref(),
        &[1, 2, 3]
    );
}

#[test]
fn duplicate_scalar_does_not_hide_malformed_overwritten_values() {
    let desc = kv_descriptor(&message_variant_descriptor());
    let enc = WireToArrowEncoder::new(
        &desc,
        Schema::new(vec![Field::new("k", DataType::LargeUtf8, true)]),
    )
    .unwrap();
    let batch = enc
        .encode_batch(&[
            Bytes::from_static(&[0x0a, 0x01, b'a']),
            // The duplicate is detected before reading the invalid UTF-8.
            // Taking only the last value would hide this malformed occurrence.
            Bytes::from_static(&[0x0a, 0x01, b'a', 0x0a, 0x01, 0xff, 0x0a, 0x01, b'b']),
            Bytes::from_static(&[0x0a, 0x01, b'c']),
        ])
        .unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.column(0).as_string::<i64>().value(0), "a");
    assert_eq!(batch.column(0).as_string::<i64>().value(1), "c");
}

#[test]
fn duplicate_singular_message_merges_fields_and_preserves_repeated_order() {
    let desc = message_variant_descriptor();
    let kv = kv_descriptor(&desc);
    let fields = ArrowFields::from(vec![
        Field::new("k", DataType::LargeUtf8, true),
        Field::new("n", DataType::Int64, true),
    ]);
    let schema = Schema::new(vec![
        Field::new("single", DataType::Struct(fields.clone()), true),
        Field::new(
            "many",
            DataType::List(Arc::new(Field::new("item", DataType::Struct(fields), true))),
            true,
        ),
    ]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();
    let mut first = DynamicMessage::new(desc.clone());
    first.set_field_by_name("single", ProtoValue::Message(kv_message(&kv, "old", 11)));
    first.set_field_by_name(
        "many",
        ProtoValue::List(vec![ProtoValue::Message(kv_message(&kv, "first", 1))]),
    );
    let mut second = DynamicMessage::new(desc);
    second.set_field_by_name("single", ProtoValue::Message(kv_message(&kv, "new", 0)));
    second.set_field_by_name(
        "many",
        ProtoValue::List(vec![ProtoValue::Message(kv_message(&kv, "second", 2))]),
    );
    let mut wire = encode_message(&first);
    wire.extend(encode_message(&second));

    let batch = enc.encode_batch(&[Bytes::from(wire)]).unwrap();

    assert_eq!(batch.num_rows(), 1);
    let single = batch.column(0).as_struct();
    assert_eq!(single.column(0).as_string::<i64>().value(0), "new");
    assert_eq!(
        single
            .column(1)
            .as_primitive::<arrow::datatypes::Int64Type>()
            .value(0),
        11
    );
    let many = batch.column(1).as_list::<i32>().value(0);
    let many = many.as_struct();
    assert_eq!(many.len(), 2);
    assert_eq!(many.column(0).as_string::<i64>().value(0), "first");
    assert_eq!(many.column(0).as_string::<i64>().value(1), "second");
}

#[test]
fn duplicate_variant_message_merges_without_leaking_prevalidation_values() {
    let desc = message_variant_descriptor();
    let kv = kv_descriptor(&desc);
    let enc =
        WireToArrowEncoder::new(&desc, Schema::new(vec![variant_arrow_field("single")])).unwrap();
    let single = |k: &str, n: i64| {
        let mut message = DynamicMessage::new(desc.clone());
        message.set_field_by_name("single", ProtoValue::Message(kv_message(&kv, k, n)));
        encode_message(&message)
    };
    let mut duplicate = single("old", 11);
    duplicate.extend(single("new", 0));

    let batch = enc
        .encode_batch(&[
            Bytes::from(single("first", 1)),
            Bytes::from(duplicate),
            Bytes::from(single("last", 3)),
        ])
        .unwrap();

    assert_eq!(batch.num_rows(), 3);
    for (row, expected) in [
        serde_json::json!({"k": "first", "n": 1}),
        serde_json::json!({"k": "new", "n": 11}),
        serde_json::json!({"k": "last", "n": 3}),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(decode_variant_wire(batch.column(0), row), expected);
    }
}

#[test]
fn variant_row_dropped_after_its_variant_was_built_does_not_leak() {
    // The pre-pass builds a row's VARIANT values before the row can still be
    // dropped by a later field. The duplicate `single` has a truncated value,
    // so canonicalization fails. The next row must read its own value.
    let desc = message_variant_descriptor();
    let kv = kv_descriptor(&desc);
    let schema = Schema::new(vec![variant_arrow_field("single")]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    let single = |k: &str| {
        let mut msg = DynamicMessage::new(desc.clone());
        msg.set_field_by_name("single", ProtoValue::Message(kv_message(&kv, k, 0)));
        encode_message(&msg)
    };
    // Repeat `single` with a truncated nested varint.
    let mut bad = single("dropped");
    bad.extend([0x0a, 0x01, 0x08]);

    let batch = enc
        .encode_batch(&[
            Bytes::from(single("first")),
            Bytes::from(bad),
            Bytes::from(single("second")),
        ])
        .unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(
        decode_variant_wire(batch.column(0), 0),
        serde_json::json!({"k": "first"})
    );
    assert_eq!(
        decode_variant_wire(batch.column(0), 1),
        serde_json::json!({"k": "second"})
    );
}

/// `message Outer { KV a = 1; repeated KV b = 2; Inner inner = 3; KV c = 4; }`,
/// `message Inner { KV v = 1; string tag = 2; }`,
/// `message KV { string k = 1; int64 n = 2; }` — VARIANT slots at the top
/// level, in a repeated field and inside a nested struct.
fn multi_variant_descriptor() -> MessageDescriptor {
    let kv_field = |name: &str, number: i32, label: Label| FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        label: Some(label as i32),
        r#type: Some(ProtoType::Message as i32),
        type_name: Some(".wire_to_arrow_multi_variant_test.KV".into()),
        ..Default::default()
    };
    let scalar_field = |name: &str, number: i32, ty: ProtoType| FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        label: Some(Label::Optional as i32),
        r#type: Some(ty as i32),
        ..Default::default()
    };
    let fd = FileDescriptorProto {
        name: Some("wire_to_arrow_multi_variant_test.proto".into()),
        package: Some("wire_to_arrow_multi_variant_test".into()),
        message_type: vec![
            DescriptorProto {
                name: Some("Outer".into()),
                field: vec![
                    kv_field("a", 1, Label::Optional),
                    kv_field("b", 2, Label::Repeated),
                    FieldDescriptorProto {
                        name: Some("inner".into()),
                        number: Some(3),
                        label: Some(Label::Optional as i32),
                        r#type: Some(ProtoType::Message as i32),
                        type_name: Some(".wire_to_arrow_multi_variant_test.Inner".into()),
                        ..Default::default()
                    },
                    kv_field("c", 4, Label::Optional),
                ],
                ..Default::default()
            },
            DescriptorProto {
                name: Some("Inner".into()),
                field: vec![
                    kv_field("v", 1, Label::Optional),
                    scalar_field("tag", 2, ProtoType::String),
                ],
                ..Default::default()
            },
            DescriptorProto {
                name: Some("KV".into()),
                field: vec![
                    scalar_field("k", 1, ProtoType::String),
                    scalar_field("n", 2, ProtoType::Int64),
                ],
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let set = FileDescriptorSet { file: vec![fd] };
    let mut bytes = Vec::new();
    set.encode(&mut bytes).unwrap();
    DescriptorPool::decode(bytes.as_slice())
        .unwrap()
        .get_message_by_name("wire_to_arrow_multi_variant_test.Outer")
        .unwrap()
}

#[test]
fn variant_values_land_in_their_own_slots_across_columns_and_nesting() {
    use arrow::array::{LargeStringArray, ListArray};

    // The scan consumes the pre-pass's VARIANT values in order, so every slot
    // kind -- top-level, repeated and inside a nested struct -- must be met in
    // the same order by both passes. Fields are interleaved on the wire, out
    // of field-number order, and every value is distinct, so any misalignment
    // puts a value in the wrong column or row.
    let desc = multi_variant_descriptor();
    let kv = desc
        .get_field_by_name("a")
        .unwrap()
        .kind()
        .as_message()
        .unwrap()
        .clone();
    let inner_desc = desc
        .get_field_by_name("inner")
        .unwrap()
        .kind()
        .as_message()
        .unwrap()
        .clone();
    let schema = Schema::new(vec![
        variant_arrow_field("a"),
        Field::new(
            "b",
            DataType::List(Arc::new(variant_arrow_field("item"))),
            true,
        ),
        Field::new(
            "inner",
            DataType::Struct(ArrowFields::from(vec![
                variant_arrow_field("v"),
                Field::new("tag", DataType::LargeUtf8, true),
            ])),
            true,
        ),
        variant_arrow_field("c"),
    ]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    let field = |name: &str, value: ProtoValue| {
        let mut msg = DynamicMessage::new(desc.clone());
        msg.set_field_by_name(name, value);
        encode_message(&msg)
    };
    let kv_value = |k: &str, n: i64| ProtoValue::Message(kv_message(&kv, k, n));
    let b_value = |k: &str, n: i64| ProtoValue::List(vec![kv_value(k, n)]);
    let inner_value = |k: &str, n: i64, tag: &str| {
        let mut inner = DynamicMessage::new(inner_desc.clone());
        inner.set_field_by_name("tag", ProtoValue::String(tag.into()));
        inner.set_field_by_name("v", kv_value(k, n));
        ProtoValue::Message(inner)
    };
    // Wire order c, b[0], inner, a, b[1]; values encode (row, slot).
    let row = |r: i64| {
        [
            field("c", kv_value("c", r)),
            field("b", b_value("b0", r)),
            field("inner", inner_value("v", r, &format!("t{r}"))),
            field("a", kv_value("a", r)),
            field("b", b_value("b1", r)),
        ]
        .concat()
    };

    let batch = enc
        .encode_batch(&[Bytes::from(row(1)), Bytes::from(row(2))])
        .unwrap();
    assert_eq!(batch.num_rows(), 2);
    let b = batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let inner = batch.column(2).as_struct();
    let tags = inner
        .column(1)
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    for r in 0..2usize {
        let n = r as i64 + 1;
        assert_eq!(
            decode_variant_wire(batch.column(0), r),
            serde_json::json!({"k": "a", "n": n})
        );
        let items = b.value(r);
        assert_eq!(items.len(), 2);
        assert_eq!(
            decode_variant_wire(&items, 0),
            serde_json::json!({"k": "b0", "n": n})
        );
        assert_eq!(
            decode_variant_wire(&items, 1),
            serde_json::json!({"k": "b1", "n": n})
        );
        assert_eq!(
            decode_variant_wire(inner.column(0), r),
            serde_json::json!({"k": "v", "n": n})
        );
        assert_eq!(tags.value(r), format!("t{n}"));
        assert_eq!(
            decode_variant_wire(batch.column(3), r),
            serde_json::json!({"k": "c", "n": n})
        );
    }
}

// -------------------------------------------------------------------------
// Fuzz: random wire bytes through `encode_batch` must not panic. Any
// `Result` outcome is acceptable — we only care that bad input is reported
// as a normal error and that the scan-time recursion (which the depth cap
// also bounds) doesn't overflow the stack.
// -------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        // Per-case timeout in ms; bounds CI cost if a future change makes
        // the scan path much slower for some inputs.
        timeout: 2_000,
        ..ProptestConfig::default()
    })]

    /// Encoder must never panic on adversarial wire bytes against a scalar
    /// schema. Most random byte sequences will hit `UnexpectedEof`,
    /// `InvalidWireType`, or `WireTypeMismatch`; a few will parse but
    /// produce nonsense values. All paths are fine as long as no panic.
    #[test]
    fn encode_batch_does_not_panic_on_random_bytes_scalar(
        bytes in proptest::collection::vec(any::<u8>(), 0..256),
    ) {
        let desc = scalar_descriptor();
        let schema = Schema::new(vec![
            Field::new("name", DataType::LargeUtf8, true),
            Field::new("id", DataType::Int32, true),
            Field::new("email", DataType::LargeUtf8, true),
        ]);
        let enc = WireToArrowEncoder::new(&desc, schema).unwrap();
        // Any Result is acceptable — the assertion is "no panic".
        let _ = enc.encode_batch(&[Bytes::from(bytes)]);
    }

    /// Same property against a deeply-nestable self-referential descriptor.
    /// This is the wire-side stack-overflow surface Flavio called out:
    /// attacker-controlled bytes try to drive `scan_message` recursion
    /// down to the plan's maximum depth. With the depth cap in place,
    /// scanning bounded by the plan stays within the safe limit.
    #[test]
    fn encode_batch_does_not_panic_on_random_bytes_nested(
        bytes in proptest::collection::vec(any::<u8>(), 0..512),
    ) {
        let desc = self_referential_descriptor();
        // Modest nesting in the Arrow schema — well under the cap, but
        // enough that adversarial bytes have a real `next` field to chase.
        let deep_struct = nested_tree_struct(8);
        let schema = Schema::new(vec![Field::new("next", deep_struct, true)]);
        let enc = WireToArrowEncoder::new(&desc, schema).unwrap();
        let _ = enc.encode_batch(&[Bytes::from(bytes)]);
    }
}

// -------------------------------------------------------------------------
// Wire-type mismatch policy (`on_wire_type_mismatch`)
//
// A known field whose wire type disagrees with the descriptor is handled
// like protobuf-java handles it: the occurrence is skipped (self-delimiting
// by wire type), the column is null for that occurrence, and the row is
// KEPT. Genuinely malformed bytes still drop the row.
// -------------------------------------------------------------------------

/// `message Stage { int64 peak_netty = 1; string name = 2; }`,
/// `message StageReport { repeated Stage stages = 1; int64 top = 2; }` —
/// mirrors the QPL incident shape: `repeated StageData` carrying an int64
/// (`peak_netty_direct_memory`) that a diverged producer sent as `LEN`.
fn stage_report_descriptor() -> MessageDescriptor {
    let stage_fields = vec![
        FieldDescriptorProto {
            name: Some("peak_netty".into()),
            number: Some(1),
            label: Some(Label::Optional as i32),
            r#type: Some(ProtoType::Int64 as i32),
            ..Default::default()
        },
        FieldDescriptorProto {
            name: Some("name".into()),
            number: Some(2),
            label: Some(Label::Optional as i32),
            r#type: Some(ProtoType::String as i32),
            ..Default::default()
        },
    ];
    let fd = FileDescriptorProto {
        name: Some("wire_to_arrow_mismatch_test.proto".into()),
        package: Some("wire_to_arrow_mismatch_test".into()),
        message_type: vec![
            DescriptorProto {
                name: Some("Stage".into()),
                field: stage_fields,
                ..Default::default()
            },
            DescriptorProto {
                name: Some("StageReport".into()),
                field: vec![
                    FieldDescriptorProto {
                        name: Some("stages".into()),
                        number: Some(1),
                        label: Some(Label::Repeated as i32),
                        r#type: Some(ProtoType::Message as i32),
                        type_name: Some(".wire_to_arrow_mismatch_test.Stage".into()),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("top".into()),
                        number: Some(2),
                        label: Some(Label::Optional as i32),
                        r#type: Some(ProtoType::Int64 as i32),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let set = FileDescriptorSet { file: vec![fd] };
    let mut bytes = Vec::new();
    set.encode(&mut bytes).unwrap();
    DescriptorPool::decode(bytes.as_slice())
        .unwrap()
        .get_message_by_name("wire_to_arrow_mismatch_test.StageReport")
        .unwrap()
}

/// Person row with `id` (int32, field 2) sent as `LEN` — the exact incident
/// shape on a top-level scalar. The row must be kept, `id` null, the other
/// columns intact.
#[test]
fn wire_type_mismatch_skips_field_and_keeps_row() {
    let desc = scalar_descriptor();
    let schema = Schema::new(vec![
        Field::new("name", DataType::LargeUtf8, true),
        Field::new("id", DataType::Int32, true),
        Field::new("email", DataType::LargeUtf8, true),
    ]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    let mut wire = Vec::new();
    wire.extend([0x0a, 5]);
    wire.extend_from_slice(b"Alice");
    // id (field 2) as LEN: tag (2<<3)|2 = 0x12. A mismatched occurrence is
    // self-delimiting, so the decoder skips exactly this value.
    wire.extend([0x12, 1, 0x01]);
    wire.extend([0x1a, 11]);
    wire.extend_from_slice(b"alice@x.com");

    let batch = enc.encode_batch(&[Bytes::from(wire)]).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.column(0).as_string::<i64>().value(0), "Alice");
    assert!(
        batch.column(1).is_null(0),
        "mismatched scalar must leave the column null"
    );
    assert_eq!(batch.column(2).as_string::<i64>().value(0), "alice@x.com");
}

/// Inside a repeated message, one element's mismatched field nulls only that
/// element's column; siblings within the element, other elements and other
/// columns of the row all survive.
#[test]
fn nested_repeated_element_field_mismatch_nulls_only_that_element() {
    let desc = stage_report_descriptor();
    let stage_fields = ArrowFields::from(vec![
        Field::new("peak_netty", DataType::Int64, true),
        Field::new("name", DataType::LargeUtf8, true),
    ]);
    let schema = Schema::new(vec![
        Field::new(
            "stages",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(stage_fields),
                true,
            ))),
            true,
        ),
        Field::new("top", DataType::Int64, true),
    ]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    // Element 1: peak_netty as varint (well-typed), name "a".
    let mut elem1 = vec![0x08, 10];
    elem1.extend([0x12, 1, b'a']);
    // Element 2: peak_netty as LEN (mismatch — int64 expects varint), name "b".
    let mut elem2 = vec![0x0a, 1, 0x7f];
    elem2.extend([0x12, 1, b'b']);
    let mut wire = Vec::new();
    for elem in [&elem1, &elem2] {
        wire.push(0x0a);
        wire.push(elem.len() as u8);
        wire.extend_from_slice(elem);
    }
    wire.extend([0x10, 99]); // top = 99 (varint, well-typed)

    let batch = enc.encode_batch(&[Bytes::from(wire)]).unwrap();
    assert_eq!(batch.num_rows(), 1);
    let stages = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .unwrap();
    assert_eq!(stages.len(), 1);
    let items = stages.value(0);
    let inner = items.as_struct();
    let peaks = inner
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(peaks.value(0), 10);
    assert!(
        peaks.is_null(1),
        "only the mismatched element's field is null"
    );
    let names = inner
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::LargeStringArray>()
        .unwrap();
    assert_eq!(names.value(0), "a");
    assert_eq!(names.value(1), "b");
    assert_eq!(
        batch
            .column(1)
            .as_primitive::<arrow::datatypes::Int64Type>()
            .value(0),
        99
    );
}

/// A message-typed field receiving a varint is skipped (column null), the
/// rest of the row is kept.
#[test]
fn message_field_receiving_varint_is_skipped() {
    let desc = self_referential_descriptor();
    let schema = Schema::new(vec![
        Field::new("next", nested_tree_struct(2), true),
        Field::new("leaf", DataType::Int32, true),
    ]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    // next (field 1, message) as varint: tag 0x08 — skipped. leaf (field 2)
    // as varint: tag 0x10 — kept.
    let batch = enc
        .encode_batch(&[Bytes::from(vec![0x08, 0x05, 0x10, 0x07])])
        .unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(batch.column(0).is_null(0));
    assert_eq!(
        batch
            .column(1)
            .as_primitive::<arrow::datatypes::Int32Type>()
            .value(0),
        7
    );
}

/// Repeated numeric fields accept both unpacked and packed occurrences, mixed
/// within one field.
#[test]
fn repeated_scalar_accepts_mixed_packed_and_unpacked() {
    let desc = repeated_int32_descriptor();
    let schema = Schema::new(vec![Field::new(
        "numbers",
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        true,
    )]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    let mut wire = vec![0x08, 0x05]; // unpacked: 5
    wire.extend([0x0a, 0x03, 0x01, 0x02, 0x03]); // packed: [1, 2, 3]

    let batch = enc.encode_batch(&[Bytes::from(wire)]).unwrap();
    let list = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .unwrap();
    let items = list.value(0);
    let values = items
        .as_any()
        .downcast_ref::<arrow::array::Int32Array>()
        .unwrap();
    let got: Vec<i32> = values.iter().map(|v| v.unwrap()).collect();
    assert_eq!(got, vec![5, 1, 2, 3]);
}

/// A mismatched occurrence of a singular field neither clobbers a well-typed
/// occurrence before it nor gets clobbered by one after it — protobuf
/// last-wins applies only to well-typed occurrences.
#[test]
fn duplicate_singular_mismatched_occurrence_does_not_clobber_valid() {
    let desc = scalar_descriptor();
    let schema = Schema::new(vec![
        Field::new("name", DataType::LargeUtf8, true),
        Field::new("id", DataType::Int32, true),
    ]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    // Valid first, mismatched second.
    let valid_then_mismatch = [0x10, 7, 0x12, 1, 0x01];
    let batch = enc
        .encode_batch(&[Bytes::from(valid_then_mismatch.to_vec())])
        .unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(1)
            .as_primitive::<arrow::datatypes::Int32Type>()
            .value(0),
        7
    );

    // Mismatched first, valid second.
    let mismatch_then_valid = [0x12, 1, 0x01, 0x10, 7];
    let batch = enc
        .encode_batch(&[Bytes::from(mismatch_then_valid.to_vec())])
        .unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(1)
            .as_primitive::<arrow::datatypes::Int32Type>()
            .value(0),
        7
    );
}

/// Genuinely malformed bytes still drop the row: truncated `LEN`, overlong
/// varint, invalid wire type 6/7, invalid UTF-8 in a declared string field.
#[test]
fn genuinely_malformed_rows_still_drop() {
    let desc = scalar_descriptor();
    let schema = Schema::new(vec![
        Field::new("name", DataType::LargeUtf8, true),
        Field::new("id", DataType::Int32, true),
    ]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    let truncated_len = [0x0a, 0x05, 0x61]; // name declared 5 bytes, 1 present
    let overlong_varint = {
        let mut v = vec![0x10];
        v.extend([0xff_u8; 11]); // 11-byte varint for id
        v
    };
    let invalid_wire_type = [0x17, 0x01]; // id with wire type 7
    let invalid_utf8 = [0x0a, 0x01, 0xff]; // name with non-UTF-8 bytes
    for wire in [
        truncated_len.as_slice(),
        overlong_varint.as_slice(),
        invalid_wire_type.as_slice(),
        invalid_utf8.as_slice(),
    ] {
        let batch = enc.encode_batch(&[Bytes::from(wire.to_vec())]).unwrap();
        assert_eq!(batch.num_rows(), 0, "wire {wire:?} must still drop the row");
    }
}

/// `on_wire_type_mismatch: drop_row` preserves the pre-option behavior: the
/// whole row is dropped and counted via `wire_to_arrow_rows_dropped`.
#[test]
fn drop_row_mode_preserves_old_mismatch_behavior() {
    let desc = scalar_descriptor();
    let schema = Schema::new(vec![
        Field::new("name", DataType::LargeUtf8, true),
        Field::new("id", DataType::Int32, true),
    ]);
    let enc =
        WireToArrowEncoder::with_options(&desc, schema, OnWireTypeMismatch::DropRow, &[]).unwrap();

    let wire = [0x0a, 5, b'A', b'l', b'i', b'c', b'e', 0x12, 1, 0x01];
    let batch = enc
        .encode_batch(&[Bytes::from(wire.as_slice().to_vec())])
        .unwrap();
    assert_eq!(batch.num_rows(), 0);
}

/// `message M { oneof x { int32 a = 1; string b = 2; } map<string, int32> m = 3; }`
fn oneof_with_map_descriptor() -> MessageDescriptor {
    use prost_reflect::prost_types::MessageOptions;
    let entry_field = |name: &str, number: i32, ty: ProtoType| FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        label: Some(Label::Optional as i32),
        r#type: Some(ty as i32),
        ..Default::default()
    };
    let fd = FileDescriptorProto {
        syntax: Some("proto3".into()),
        name: Some("wire_to_arrow_oneof_map.proto".into()),
        package: Some("wire_to_arrow_oneof_map_test".into()),
        message_type: vec![DescriptorProto {
            name: Some("M".into()),
            field: vec![
                FieldDescriptorProto {
                    oneof_index: Some(0),
                    ..entry_field("a", 1, ProtoType::Int32)
                },
                FieldDescriptorProto {
                    oneof_index: Some(0),
                    ..entry_field("b", 2, ProtoType::String)
                },
                FieldDescriptorProto {
                    label: Some(Label::Repeated as i32),
                    type_name: Some(".wire_to_arrow_oneof_map_test.M.MEntry".into()),
                    ..entry_field("m", 3, ProtoType::Message)
                },
            ],
            nested_type: vec![DescriptorProto {
                name: Some("MEntry".into()),
                field: vec![
                    entry_field("key", 1, ProtoType::String),
                    entry_field("value", 2, ProtoType::Int32),
                ],
                options: Some(MessageOptions {
                    map_entry: Some(true),
                    ..Default::default()
                }),
                ..Default::default()
            }],
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
        .get_message_by_name("wire_to_arrow_oneof_map_test.M")
        .unwrap()
}

fn map_column(name: &str, value: DataType) -> Field {
    let entry = ArrowFields::from(vec![
        Field::new("key", DataType::LargeUtf8, false),
        Field::new("value", value, true),
    ]);
    Field::new(
        name,
        DataType::Map(
            Arc::new(Field::new("key_value", DataType::Struct(entry), false)),
            false,
        ),
        true,
    )
}

fn assert_map_empty_or_null(col: &arrow::array::ArrayRef) {
    let map = col
        .as_any()
        .downcast_ref::<arrow::array::MapArray>()
        .unwrap();
    assert!(
        map.is_null(0) || map.value_length(0) == 0,
        "map must be empty"
    );
}

/// The map field carried with each non-LEN wire type (field 4 on Person, 3 on M).
fn mismatched_map_occurrences(field: u8) -> Vec<Vec<u8>> {
    vec![
        vec![field << 3, 0x05],
        [vec![(field << 3) | 1], vec![0; 8]].concat(),
        [vec![(field << 3) | 5], vec![0; 4]].concat(),
    ]
}

fn skip_field_encoder(desc: &MessageDescriptor, schema: Schema) -> WireToArrowEncoder {
    WireToArrowEncoder::with_options(desc, schema, OnWireTypeMismatch::SkipField, &[]).unwrap()
}

fn drop_row_encoder(desc: &MessageDescriptor, schema: Schema) -> WireToArrowEncoder {
    WireToArrowEncoder::with_options(desc, schema, OnWireTypeMismatch::DropRow, &[]).unwrap()
}

/// A wrong-wire-type MAP field plus a duplicate singular scalar (normalization
/// path) keeps the row under `skip_field`, as classic ScalaPB `skipField` does:
/// last value wins, map empty. Both orders; `drop_row` still drops.
#[test]
fn map_mismatch_with_duplicate_singular_is_kept_under_skip_field() {
    let desc = rich_descriptor();
    let schema = || {
        Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            map_column("data", DataType::Int32),
        ])
    };
    let dup = [0x10, 0x01, 0x10, 0x02];
    for mismatch in mismatched_map_occurrences(4) {
        for mismatch_first in [true, false] {
            let wire = if mismatch_first {
                [mismatch.clone(), dup.to_vec()].concat()
            } else {
                [dup.to_vec(), mismatch.clone()].concat()
            };
            let batch = skip_field_encoder(&desc, schema())
                .encode_batch(&[Bytes::from(wire.clone())])
                .unwrap();
            assert_eq!(batch.num_rows(), 1, "kept: {wire:02x?}");
            assert_eq!(
                batch
                    .column(0)
                    .as_primitive::<arrow::datatypes::Int32Type>()
                    .value(0),
                2
            );
            assert_map_empty_or_null(batch.column(1));
            let dropped = drop_row_encoder(&desc, schema())
                .encode_batch(&[Bytes::from(wire)])
                .unwrap();
            assert_eq!(dropped.num_rows(), 0);
        }
    }
}

/// Same, with the normalization trigger being a oneof switch.
#[test]
fn map_mismatch_with_oneof_switch_is_kept_under_skip_field() {
    let desc = oneof_with_map_descriptor();
    let schema = || {
        Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::LargeUtf8, true),
            map_column("m", DataType::Int32),
        ])
    };
    let switch = [0x08, 0x01, 0x12, 0x01, b'z'];
    for mismatch in mismatched_map_occurrences(3) {
        for mismatch_first in [true, false] {
            let wire = if mismatch_first {
                [mismatch.clone(), switch.to_vec()].concat()
            } else {
                [switch.to_vec(), mismatch.clone()].concat()
            };
            let batch = skip_field_encoder(&desc, schema())
                .encode_batch(&[Bytes::from(wire.clone())])
                .unwrap();
            assert_eq!(batch.num_rows(), 1, "kept: {wire:02x?}");
            assert!(batch.column(0).is_null(0), "a cleared by the switch");
            assert_eq!(batch.column(1).as_string::<i64>().value(0), "z");
            assert_map_empty_or_null(batch.column(2));
            let dropped = drop_row_encoder(&desc, schema())
                .encode_batch(&[Bytes::from(wire)])
                .unwrap();
            assert_eq!(dropped.num_rows(), 0);
        }
    }
}

/// A mismatch inside a message-backed VARIANT slot combined with a repeated
/// (merged) singular occurrence of that slot keeps the row.
#[test]
fn variant_mismatch_with_duplicate_singular_is_kept_under_skip_field() {
    let desc = message_variant_descriptor();
    let schema = Schema::new(vec![variant_arrow_field("single")]);
    let enc = skip_field_encoder(&desc, schema);
    // single = KV { k: "x", n as LEN }, then single = KV { k: "y" }.
    let wire = vec![
        0x0a, 0x06, 0x0a, 0x01, b'x', 0x12, 0x01, 0x01, // first, mismatched n
        0x0a, 0x03, 0x0a, 0x01, b'y', // merges, k = "y"
    ];
    let batch = enc.encode_batch(&[Bytes::from(wire)]).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        decode_variant_wire(batch.column(0), 0),
        serde_json::json!({"k": "y"})
    );
}

/// A malformed overwritten string drops the row under `skip_field` no matter
/// where a wire-type mismatch sits: first, last, or absent.
#[test]
fn guard_drops_overwritten_malformed_string_regardless_of_mismatch_position() {
    let desc = rich_descriptor();
    let schema = || Schema::new(vec![Field::new("name", DataType::LargeUtf8, true)]);
    let dup = [0x0a, 0x01, b'a', 0x0a, 0x02, 0xff, 0xfe, 0x0a, 0x01, b'b'];
    let mismatch = [0x10, 0x01, 0x01]; // id (int32) as LEN
    for wire in [
        [mismatch.as_slice(), dup.as_slice()].concat(),
        dup.to_vec(),
        [dup.as_slice(), mismatch.as_slice()].concat(),
    ] {
        let batch = skip_field_encoder(&desc, schema())
            .encode_batch(&[Bytes::from(wire.clone())])
            .unwrap();
        assert_eq!(batch.num_rows(), 0, "must drop: {wire:02x?}");
    }
}

/// The metrics registry is process-global. These tests assert before/after
/// deltas and serialize against each other; counters bumped by unrelated
/// concurrent tests can still skew a delta under threaded `cargo test`, so CI
/// runs them under nextest (process per test).
pub(super) static METRICS_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Sum of a global counter, optionally restricted to a `reason` tag. Tests
/// compare before/after deltas instead of resetting the shared registry.
pub(super) fn counter_total(
    controller: &vector_core::metrics::Controller,
    name: &str,
    reason: Option<&str>,
) -> f64 {
    controller
        .capture_metrics()
        .iter()
        .filter(|m| {
            m.name() == name
                && reason.is_none_or(|r| m.tags().and_then(|t| t.get("reason")) == Some(r))
        })
        .map(|m| match m.value() {
            vector_core::event::MetricValue::Counter { value } => *value,
            other => panic!("not a counter: {other:?}"),
        })
        .sum()
}

/// The skip counter is emitted with the bounded `reason` label, and rows
/// still dropped keep their counter.
#[test]
fn skip_and_drop_counters_emitted() {
    use vector_core::metrics::{Controller, init_test};

    let _guard = METRICS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    init_test();
    let controller = Controller::get().unwrap();
    let skipped_before = counter_total(
        controller,
        "wire_to_arrow_fields_skipped_total",
        Some("wire_type_mismatch"),
    );
    let dropped_before = counter_total(controller, "wire_to_arrow_rows_dropped", None);

    let desc = scalar_descriptor();
    let schema = Schema::new(vec![
        Field::new("name", DataType::LargeUtf8, true),
        Field::new("id", DataType::Int32, true),
    ]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    let mut mismatched = vec![0x0a, 5];
    mismatched.extend_from_slice(b"Alice");
    mismatched.extend([0x12, 1, 0x01]); // id as LEN — skipped
    let malformed = [0x0a, 0x05, 0x61]; // truncated LEN — dropped

    let batch = enc
        .encode_batch(&[Bytes::from(mismatched), Bytes::from(malformed.to_vec())])
        .unwrap();
    assert_eq!(batch.num_rows(), 1);

    let skipped = counter_total(
        controller,
        "wire_to_arrow_fields_skipped_total",
        Some("wire_type_mismatch"),
    ) - skipped_before;
    assert_eq!(skipped, 1.0, "one skipped occurrence expected");
    let dropped = counter_total(controller, "wire_to_arrow_rows_dropped", None) - dropped_before;
    assert_eq!(dropped, 1.0, "one dropped row expected");
}

/// Attribution: configured top-level fields are sampled from the offending
/// row into the skip/drop warning payload. The extractor is tested directly;
/// it only reads the configured fields, truncates values, and never logs
/// payloads.
#[test]
fn attribution_samples_configured_fields_from_offending_row() {
    let desc = scalar_descriptor();
    let schema = Schema::new(vec![
        Field::new("name", DataType::LargeUtf8, true),
        Field::new("id", DataType::Int32, true),
    ]);
    let enc = WireToArrowEncoder::with_options(
        &desc,
        schema,
        OnWireTypeMismatch::SkipField,
        &["email".to_string(), "id".to_string()],
    )
    .unwrap();

    let mut wire = Vec::new();
    wire.extend([0x1a, 11]);
    wire.extend_from_slice(b"alice@x.com"); // email, well-typed
    wire.extend([0x12, 1, 0x31]); // id as LEN — the mismatch (payload "1")

    let attribution = enc.attribution_from_row(&wire);
    assert_eq!(attribution, "email=alice@x.com,id=1");

    // Long string values are truncated to the per-field cap.
    let mut long = Vec::new();
    long.extend([0x1a, 0x50]);
    long.extend(std::iter::repeat_n(b'x', 80));
    let attribution = enc.attribution_from_row(&long);
    assert!(attribution.len() <= "email=".len() + 64, "{attribution}");

    // Rows whose bytes don't parse yield an empty attribution string.
    assert_eq!(enc.attribution_from_row(&[0xff]), "");
}

/// A mismatched occurrence of a repeated numeric field is skipped per
/// occurrence: sibling elements, before and after, still land in the list.
#[test]
fn repeated_field_mismatch_skips_only_that_occurrence() {
    let desc = repeated_int32_descriptor();
    let schema = Schema::new(vec![Field::new(
        "numbers",
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        true,
    )]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    // Unpacked 5, one occurrence with fixed32 wire type (skipped), unpacked 7.
    let wire = [0x08, 0x05, 0x0d, 0x01, 0x02, 0x03, 0x04, 0x08, 0x07];
    let batch = enc.encode_batch(&[Bytes::from(wire.to_vec())]).unwrap();
    assert_eq!(batch.num_rows(), 1);
    let list = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .unwrap();
    let items = list.value(0);
    let values = items
        .as_any()
        .downcast_ref::<arrow::array::Int32Array>()
        .unwrap();
    let got: Vec<i32> = values.iter().map(|v| v.unwrap()).collect();
    assert_eq!(got, vec![5, 7], "only the mismatched occurrence is skipped");
}

/// A singular enum paired with a STRING column receiving a `LEN` occurrence
/// is skipped (column null, row kept) — enums are varints on the wire.
#[test]
fn enum_string_receiving_len_is_skipped() {
    let color_values: Vec<EnumValueDescriptorProto> = [("RED", 0), ("GREEN", 1)]
        .iter()
        .map(|(n, v)| EnumValueDescriptorProto {
            name: Some((*n).into()),
            number: Some(*v),
            ..Default::default()
        })
        .collect();
    let fd = FileDescriptorProto {
        name: Some("wire_to_arrow_enum_mismatch_test.proto".into()),
        package: Some("wire_to_arrow_enum_mismatch_test".into()),
        enum_type: vec![EnumDescriptorProto {
            name: Some("Color".into()),
            value: color_values,
            ..Default::default()
        }],
        message_type: vec![DescriptorProto {
            name: Some("Shaded".into()),
            field: vec![FieldDescriptorProto {
                name: Some("color".into()),
                number: Some(1),
                label: Some(Label::Optional as i32),
                r#type: Some(ProtoType::Enum as i32),
                type_name: Some(".wire_to_arrow_enum_mismatch_test.Color".into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let set = FileDescriptorSet { file: vec![fd] };
    let desc = DescriptorPool::decode(set.encode_to_vec().as_slice())
        .unwrap()
        .get_message_by_name("wire_to_arrow_enum_mismatch_test.Shaded")
        .unwrap();

    let schema = Schema::new(vec![Field::new("color", DataType::LargeUtf8, true)]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    // color as LEN (tag 0x0a): skipped; sibling row shows the enum name path
    // still works for well-typed occurrences.
    let wire = [0x0a, 0x01, 0x01];
    let batch = enc.encode_batch(&[Bytes::from(wire.to_vec())]).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(batch.column(0).is_null(0), "mismatched enum must be null");
}

/// A map entry whose key (or value) occurrence carries a mismatched wire type
/// keeps the entry with the proto3 default filled in — Arrow Map's key is
/// non-nullable, so a skipped key becomes `""`/`0`, never null.
#[test]
fn map_entry_mismatch_fills_proto3_defaults() {
    let desc = rich_descriptor();
    let entry_fields = ArrowFields::from(vec![
        Field::new("key", DataType::LargeUtf8, false),
        Field::new("value", DataType::Int32, true),
    ]);
    let entry_field = Arc::new(Field::new(
        "key_value",
        DataType::Struct(entry_fields),
        false,
    ));
    let schema = Schema::new(vec![Field::new(
        "data",
        DataType::Map(entry_field, false),
        true,
    )]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    // One map entry (field 4, tag 0x22): key (field 1) as varint — mismatch;
    // value (field 2) as LEN — mismatch.
    let wire = [0x22, 0x05, 0x08, 0x01, 0x12, 0x01, 0x01];
    let batch = enc.encode_batch(&[Bytes::from(wire.to_vec())]).unwrap();
    assert_eq!(batch.num_rows(), 1);
    let map = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::MapArray>()
        .unwrap();
    assert_eq!(map.value_length(0), 1, "entry kept, not dropped");
    assert_eq!(map.keys().as_string::<i64>().value(0), "");
    assert_eq!(
        map.values()
            .as_primitive::<arrow::datatypes::Int32Type>()
            .value(0),
        0
    );
}

/// A message-backed VARIANT column receiving a non-`LEN` occurrence is
/// skipped like any other message-typed field.
#[test]
fn variant_column_receiving_non_len_is_skipped() {
    let desc = message_variant_descriptor();
    let schema = Schema::new(vec![variant_arrow_field("single")]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    // single (field 1, message) as varint — the occurrence is skipped and the
    // row kept, like any other message-typed field.
    let batch = enc.encode_batch(&[Bytes::from(vec![0x08, 0x05])]).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(batch.column(0).is_null(0));
}

/// A wire-type mismatch *inside* a message-backed VARIANT sub-message omits
/// that field from the Variant object and keeps the row (the same treatment
/// a Struct column gets), instead of dropping the row via the prost fallback.
#[test]
fn variant_sub_message_mismatch_is_skipped_under_skip_field() {
    let desc = message_variant_descriptor();
    let schema = Schema::new(vec![variant_arrow_field("single")]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    // single = KV { k: "x", n as LEN (int64 expects varint) }.
    let mut sub = vec![0x0a, 0x01, b'x'];
    sub.extend([0x12, 0x01, 0x01]);
    let mut wire = vec![0x0a, sub.len() as u8];
    wire.extend_from_slice(&sub);

    let batch = enc.encode_batch(&[Bytes::from(wire)]).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(!batch.column(0).is_null(0));
    assert_eq!(
        decode_variant_wire(batch.column(0), 0),
        serde_json::json!({"k": "x"}),
        "mismatched field omitted from the Variant, well-typed sibling kept"
    );
}

/// Walking a message-backed VARIANT must leave the shared field-number path
/// at its prior depth, or a later skip in the same row reports a stale prefix.
#[test]
fn variant_walk_does_not_leak_into_later_skip_path() {
    let desc = message_variant_descriptor();
    let fields = ArrowFields::from(vec![
        Field::new("k", DataType::LargeUtf8, true),
        Field::new("n", DataType::Int64, true),
    ]);
    let schema = Schema::new(vec![
        variant_arrow_field("single"),
        Field::new(
            "many",
            DataType::List(Arc::new(Field::new("item", DataType::Struct(fields), true))),
            true,
        ),
    ]);
    let plan = super::plan::MessagePlan::build(&desc, schema.fields(), Default::default()).unwrap();

    // single = KV { k: "x" } (well-typed VARIANT), then many[0] = KV { k as varint }.
    let wire = [0x0a, 0x03, 0x0a, 0x01, b'x', 0x12, 0x02, 0x08, 0x00];
    let (result, skips) = validate_with_mode_and_skips(&plan, &wire, OnWireTypeMismatch::SkipField);
    result.unwrap();
    assert_eq!(skips.skipped, 1);
    assert_eq!(skips.sample.unwrap().field_numbers, [2, 1]);
}

/// Under `drop_row` the same mismatch inside a VARIANT sub-message preserves
/// the historical behavior: the whole row drops.
#[test]
fn variant_sub_message_mismatch_drops_row_under_drop_row() {
    let desc = message_variant_descriptor();
    let schema = Schema::new(vec![variant_arrow_field("single")]);
    let enc =
        WireToArrowEncoder::with_options(&desc, schema, OnWireTypeMismatch::DropRow, &[]).unwrap();

    let mut sub = vec![0x0a, 0x01, b'x'];
    sub.extend([0x12, 0x01, 0x01]); // n as LEN — mismatch
    let mut wire = vec![0x0a, sub.len() as u8];
    wire.extend_from_slice(&sub);

    let batch = enc.encode_batch(&[Bytes::from(wire)]).unwrap();
    assert_eq!(batch.num_rows(), 0);
}

/// `message KVMap { string k = 1; int64 n = 2; map<string,int32> labels = 3;
/// google.protobuf.Timestamp ts = 4; oneof choice { int32 a = 6; string b = 7; } }`
/// — a VARIANT sub-message that mixes plain fields with reference-only shapes
/// (map, well-known type), so the direct walk falls back to the reference
/// decoder, and with a resolvable-at-runtime oneof conflict (two members set).
fn message_variant_map_descriptor() -> MessageDescriptor {
    let timestamp = FileDescriptorProto {
        name: Some("google/protobuf/timestamp.proto".into()),
        package: Some("google.protobuf".into()),
        syntax: Some("proto3".into()),
        message_type: vec![DescriptorProto {
            name: Some("Timestamp".into()),
            field: vec![
                FieldDescriptorProto {
                    name: Some("seconds".into()),
                    number: Some(1),
                    label: Some(Label::Optional as i32),
                    r#type: Some(ProtoType::Int64 as i32),
                    ..Default::default()
                },
                FieldDescriptorProto {
                    name: Some("nanos".into()),
                    number: Some(2),
                    label: Some(Label::Optional as i32),
                    r#type: Some(ProtoType::Int32 as i32),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut fd = message_variant_descriptor()
        .parent_file_descriptor_proto()
        .clone();
    fd.dependency.push("google/protobuf/timestamp.proto".into());
    let kv = &mut fd.message_type[1];
    kv.field.push(FieldDescriptorProto {
        name: Some("labels".into()),
        number: Some(4),
        label: Some(Label::Repeated as i32),
        r#type: Some(ProtoType::Message as i32),
        type_name: Some(".wire_to_arrow_msg_variant_test.KV.LabelsEntry".into()),
        ..Default::default()
    });
    kv.field.push(FieldDescriptorProto {
        name: Some("ts".into()),
        number: Some(5),
        label: Some(Label::Optional as i32),
        r#type: Some(ProtoType::Message as i32),
        type_name: Some(".google.protobuf.Timestamp".into()),
        ..Default::default()
    });
    // Oneof with two members: setting both on the wire makes the direct walk
    // bail at runtime (last-wins depends on wire order).
    kv.oneof_decl.push(OneofDescriptorProto {
        name: Some("choice".into()),
        ..Default::default()
    });
    kv.field.push(FieldDescriptorProto {
        name: Some("a".into()),
        number: Some(6),
        label: Some(Label::Optional as i32),
        r#type: Some(ProtoType::Int32 as i32),
        oneof_index: Some(0),
        ..Default::default()
    });
    kv.field.push(FieldDescriptorProto {
        name: Some("b".into()),
        number: Some(7),
        label: Some(Label::Optional as i32),
        r#type: Some(ProtoType::String as i32),
        oneof_index: Some(0),
        ..Default::default()
    });
    kv.nested_type.push(DescriptorProto {
        name: Some("LabelsEntry".into()),
        field: vec![
            FieldDescriptorProto {
                name: Some("key".into()),
                number: Some(1),
                label: Some(Label::Optional as i32),
                r#type: Some(ProtoType::String as i32),
                ..Default::default()
            },
            FieldDescriptorProto {
                name: Some("value".into()),
                number: Some(2),
                label: Some(Label::Optional as i32),
                r#type: Some(ProtoType::Int32 as i32),
                ..Default::default()
            },
        ],
        options: Some(prost_reflect::prost_types::MessageOptions {
            map_entry: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    });
    let set = FileDescriptorSet {
        file: vec![timestamp, fd],
    };
    DescriptorPool::decode(set.encode_to_vec().as_slice())
        .unwrap()
        .get_message_by_name("wire_to_arrow_msg_variant_test.Container")
        .unwrap()
}

/// When a message-backed VARIANT falls back to the reference decoder (here:
/// because the sub-message carries a map, a well-known type, or a conflicting
/// oneof), the fallback path must not lose the skip policy: mismatched
/// occurrences — at the top level, inside a map entry, or inside a well-known
/// type — are stripped so the row is kept with those fields omitted, and each
/// is counted exactly once (the abandoned direct walk's partial tally is
/// rolled back).
#[test]
fn variant_fallback_shapes_are_sanitized_under_skip_field() {
    let desc = message_variant_map_descriptor();
    let schema = Schema::new(vec![variant_arrow_field("single")]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    // single = KVMap { k: "x", n as LEN (mismatch), labels {"a": 1} }.
    let mismatch_plus_map = {
        let mut sub = vec![0x0a, 0x01, b'x'];
        sub.extend([0x12, 0x01, 0x01]); // n as LEN — mismatch
        sub.extend([0x22, 0x05, 0x0a, 0x01, b'a', 0x10, 0x01]); // labels
        let mut wire = vec![0x0a, sub.len() as u8];
        wire.extend_from_slice(&sub);
        wire
    };
    // single = KVMap { labels { key as varint (mismatch), value: 5 } }.
    let mismatched_map_key = {
        let sub = [0x22, 0x04, 0x08, 0x01, 0x10, 0x05];
        let mut wire = vec![0x0a, sub.len() as u8];
        wire.extend_from_slice(&sub);
        wire
    };
    // single = KVMap { n as LEN (mismatch), ts { seconds: 5 } }.
    let mismatch_plus_wkt = {
        let mut sub = vec![0x12, 0x01, 0x01]; // n as LEN — mismatch
        sub.extend([0x2a, 0x02, 0x08, 0x05]); // ts { seconds: 5 }
        let mut wire = vec![0x0a, sub.len() as u8];
        wire.extend_from_slice(&sub);
        wire
    };
    // single = KVMap { n as LEN (mismatch), a: 1, b: "x" }: the oneof
    // conflict makes the direct walk bail mid-walk, after it recorded the
    // mismatch; the reference keeps only the last member set.
    let mismatch_plus_oneof_conflict = {
        let mut sub = vec![0x12, 0x01, 0x01]; // n as LEN — mismatch
        sub.extend([0x30, 0x01]); // a: 1
        sub.extend([0x3a, 0x01, b'x']); // b: "x"
        let mut wire = vec![0x0a, sub.len() as u8];
        wire.extend_from_slice(&sub);
        wire
    };

    let batch = enc
        .encode_batch(&[
            Bytes::from(mismatch_plus_map),
            Bytes::from(mismatched_map_key),
            Bytes::from(mismatch_plus_wkt),
            Bytes::from(mismatch_plus_oneof_conflict),
        ])
        .unwrap();
    assert_eq!(batch.num_rows(), 4);
    assert_eq!(
        decode_variant_wire(batch.column(0), 0),
        serde_json::json!({"k": "x", "labels": {"a": 1}}),
        "mismatched field stripped around the fallback-forcing map"
    );
    assert_eq!(
        decode_variant_wire(batch.column(0), 1),
        serde_json::json!({"labels": {"": 5}}),
        "mismatched map key stripped; entry kept with the proto default key"
    );
    assert_eq!(
        decode_variant_wire(batch.column(0), 2),
        serde_json::json!({"ts": "1970-01-01T00:00:05Z"}),
        "mismatched field stripped beside the well-known type \
         (the reference renders Timestamp in its JSON mapping)"
    );
    assert_eq!(
        decode_variant_wire(batch.column(0), 3),
        serde_json::json!({"b": "x"}),
        "mismatched field stripped around the oneof conflict; \
         the reference keeps only the last member set"
    );
}

/// The sanitized fallback rows above each count exactly one skip, with the
/// root-to-leaf path of the mismatched occurrence. The column is a real
/// message-backed VARIANT (`variant_arrow_field`, not a Struct), so this goes
/// through `encode_variant_slot`: the direct walk records the mismatch before
/// it bails mid-walk on the map field / the oneof conflict, so without the
/// fallback's tally rollback this counts two skips.
#[test]
fn variant_fallback_sanitization_counts_exactly_one_skip() {
    let desc = message_variant_map_descriptor();
    let schema = Schema::new(vec![variant_arrow_field("single")]);
    let plan = super::plan::MessagePlan::build(&desc, schema.fields(), Default::default()).unwrap();

    // single = KVMap { k: "x", n as LEN (mismatch), labels {"a": 1} }: the
    // map field aborts the direct walk after the mismatch was recorded.
    let map_wire = {
        let mut sub = vec![0x0a, 0x01, b'x'];
        sub.extend([0x12, 0x01, 0x01]);
        sub.extend([0x22, 0x05, 0x0a, 0x01, b'a', 0x10, 0x01]);
        let mut wire = vec![0x0a, sub.len() as u8];
        wire.extend_from_slice(&sub);
        wire
    };
    // single = KVMap { n as LEN (mismatch), a: 1, b: "x" }: the oneof
    // conflict aborts the direct walk after the mismatch was recorded.
    let oneof_wire = {
        let mut sub = vec![0x12, 0x01, 0x01];
        sub.extend([0x30, 0x01]); // a: 1
        sub.extend([0x3a, 0x01, b'x']); // b: "x"
        let mut wire = vec![0x0a, sub.len() as u8];
        wire.extend_from_slice(&sub);
        wire
    };

    for wire in [&map_wire, &oneof_wire] {
        let (result, skips) =
            validate_with_mode_and_skips(&plan, wire, OnWireTypeMismatch::SkipField);
        result.unwrap();
        assert_eq!(skips.skipped, 1, "wire={wire:?}");
        let sample = skips.sample.unwrap();
        assert_eq!(sample.field_numbers, [1, 2]);
        assert_eq!((sample.expected, sample.actual), (0, 2));
    }
}

/// Under `drop_row` a fallback-forcing shape plus a mismatch preserves the
/// historical behavior: the whole row drops.
#[test]
fn variant_fallback_with_mismatch_drops_row_under_drop_row() {
    let desc = message_variant_map_descriptor();
    let schema = Schema::new(vec![variant_arrow_field("single")]);
    let enc =
        WireToArrowEncoder::with_options(&desc, schema, OnWireTypeMismatch::DropRow, &[]).unwrap();

    let mut sub = vec![0x12, 0x01, 0x01]; // n as LEN — mismatch
    sub.extend([0x22, 0x05, 0x0a, 0x01, b'a', 0x10, 0x01]); // labels
    let mut wire = vec![0x0a, sub.len() as u8];
    wire.extend_from_slice(&sub);

    let batch = enc.encode_batch(&[Bytes::from(wire)]).unwrap();
    assert_eq!(batch.num_rows(), 0);
}

/// Malformed bytes are not repaired by sanitization: a truncated map entry
/// body inside the mismatch-bearing sub-message still drops the row.
#[test]
fn variant_fallback_malformed_with_mismatch_still_drops() {
    let desc = message_variant_map_descriptor();
    let schema = Schema::new(vec![variant_arrow_field("single")]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    // n as LEN (mismatch) + labels entry declaring 3 bytes of body but
    // carrying a key tag that promises more than remains.
    let mut sub = vec![0x12, 0x01, 0x01];
    sub.extend([0x22, 0x03, 0x0a, 0x05]);
    let mut wire = vec![0x0a, sub.len() as u8];
    wire.extend_from_slice(&sub);

    let batch = enc.encode_batch(&[Bytes::from(wire)]).unwrap();
    assert_eq!(batch.num_rows(), 0, "malformed entry body must still drop");
}

/// A truncated packed blob is wire-type-compatible but malformed, so the row
/// still drops.
#[test]
fn truncated_packed_blob_still_drops_row() {
    let desc = repeated_int32_descriptor();
    let schema = Schema::new(vec![Field::new(
        "numbers",
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        true,
    )]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    // Packed blob (tag 0x0a) declaring 1 byte holding a truncated varint.
    let batch = enc
        .encode_batch(&[Bytes::from(vec![0x0a, 0x01, 0x80])])
        .unwrap();
    assert_eq!(
        batch.num_rows(),
        0,
        "malformed packed blob must drop the row"
    );
}

/// A row carrying both a mismatched field and a truncated field drops, and
/// its recorded skip must NOT reach the `wire_to_arrow_fields_skipped_total`
/// metric — skips count only for kept rows.
#[test]
fn skip_not_counted_for_dropped_row() {
    use vector_core::metrics::{Controller, init_test};

    let _guard = METRICS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    init_test();
    let controller = Controller::get().unwrap();
    let skipped_before = counter_total(
        controller,
        "wire_to_arrow_fields_skipped_total",
        Some("wire_type_mismatch"),
    );
    let dropped_before = counter_total(controller, "wire_to_arrow_rows_dropped", None);

    let desc = scalar_descriptor();
    let schema = Schema::new(vec![
        Field::new("name", DataType::LargeUtf8, true),
        Field::new("id", DataType::Int32, true),
        Field::new("email", DataType::LargeUtf8, true),
    ]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    // id as LEN (mismatch — recorded into the row tally), then a truncated
    // email that drops the row.
    let mut wire = vec![0x12, 0x01, 0x01];
    wire.extend([0x1a, 0x05, 0x61]);

    let batch = enc.encode_batch(&[Bytes::from(wire)]).unwrap();
    assert_eq!(batch.num_rows(), 0);

    let skipped = counter_total(
        controller,
        "wire_to_arrow_fields_skipped_total",
        Some("wire_type_mismatch"),
    ) - skipped_before;
    assert_eq!(skipped, 0.0, "dropped row must not count skips");

    // >= rather than ==: unlocked tests elsewhere can also drop rows concurrently.
    let dropped = counter_total(controller, "wire_to_arrow_rows_dropped", None) - dropped_before;
    assert!(
        dropped >= 1.0,
        "the truncated row must be counted as dropped, got {dropped}"
    );
}

/// A row that needs canonicalization (duplicate singular field) counts its
/// skip exactly once: the first pass over the original row records it, and
/// the revalidation of the canonical bytes records nothing.
#[test]
fn skip_counted_once_through_normalize_and_revalidation() {
    use vector_core::metrics::{Controller, init_test};

    let _guard = METRICS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    init_test();
    let controller = Controller::get().unwrap();
    let skipped_before = counter_total(
        controller,
        "wire_to_arrow_fields_skipped_total",
        Some("wire_type_mismatch"),
    );

    let desc = scalar_descriptor();
    let schema = Schema::new(vec![
        Field::new("name", DataType::LargeUtf8, true),
        Field::new("id", DataType::Int32, true),
    ]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    // id varint 7, id as LEN (mismatch), id varint 8 — the duplicate
    // singular tag forces normalize + revalidation.
    let wire = [0x10, 0x07, 0x12, 0x01, 0x01, 0x10, 0x08];
    let batch = enc.encode_batch(&[Bytes::from(wire.to_vec())]).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(1)
            .as_primitive::<arrow::datatypes::Int32Type>()
            .value(0),
        8,
        "last well-typed occurrence wins"
    );

    let skipped = counter_total(
        controller,
        "wire_to_arrow_fields_skipped_total",
        Some("wire_type_mismatch"),
    ) - skipped_before;
    assert_eq!(skipped, 1.0, "exactly one skip across both passes");
}

/// Encode `row` as a one-row batch and return it with the row's
/// `wire_to_arrow_fields_skipped_total` delta. Callers hold
/// `METRICS_TEST_LOCK`.
fn encode_row_counting_skips(enc: &WireToArrowEncoder, row: &[u8]) -> (RecordBatch, f64) {
    use vector_core::metrics::{Controller, init_test};

    init_test();
    let controller = Controller::get().unwrap();
    let skipped = || {
        counter_total(
            controller,
            "wire_to_arrow_fields_skipped_total",
            Some("wire_type_mismatch"),
        )
    };
    let before = skipped();
    let batch = enc.encode_batch(&[Bytes::from(row.to_vec())]).unwrap();
    (batch, skipped() - before)
}

/// A mismatch *after* the duplicate that sends the row to normalization is
/// counted and attributed: the first pass no longer stops at the duplicate,
/// so the occurrence normalization strips silently is still recorded.
#[traced_test]
#[test]
fn skip_after_duplicate_singular_counted_and_attributed() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let schema = Schema::new(vec![
        Field::new("name", DataType::LargeUtf8, true),
        Field::new("id", DataType::Int32, true),
    ]);
    let enc = WireToArrowEncoder::with_options(
        &scalar_descriptor(),
        schema,
        OnWireTypeMismatch::SkipField,
        &["id".to_string()],
    )
    .unwrap();

    // id varint 7, id varint 8 (duplicate), then id as LEN (mismatch).
    let (batch, skipped) =
        encode_row_counting_skips(&enc, &[0x10, 0x07, 0x10, 0x08, 0x12, 0x01, 0x01]);
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(1)
            .as_primitive::<arrow::datatypes::Int32Type>()
            .value(0),
        8,
        "last well-typed occurrence wins"
    );
    assert_eq!(skipped, 1.0, "the stripped mismatch counts once");
    assert!(logs_contain("skipped wire-type-mismatched fields"));
    assert!(logs_contain("field_numbers=[2]"));
    assert!(logs_contain("attribution=id=7"));
}

/// The oneof counterpart: a mismatched member after a oneof switch is counted
/// and attributed even though normalization strips it.
#[traced_test]
#[test]
fn skip_after_oneof_switch_counted_and_attributed() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::LargeUtf8, true),
    ]);
    let enc = WireToArrowEncoder::with_options(
        &oneof_descriptor(),
        schema,
        OnWireTypeMismatch::SkipField,
        &["b".to_string()],
    )
    .unwrap();

    // a = 1, b = "x" (switches the oneof), then a as LEN (mismatch).
    let (batch, skipped) =
        encode_row_counting_skips(&enc, &[0x08, 0x01, 0x12, 0x01, b'x', 0x0a, 0x01, 0x01]);
    assert_eq!(batch.num_rows(), 1);
    assert!(
        batch.column(0).is_null(0),
        "the mismatch must not reselect a"
    );
    assert_eq!(batch.column(1).as_string::<i64>().value(0), "x");
    assert_eq!(skipped, 1.0, "the stripped mismatch counts once");
    assert!(logs_contain("skipped wire-type-mismatched fields"));
    assert!(logs_contain("field_numbers=[1]"));
    assert!(logs_contain("attribution=b=x"));
}

/// A mismatch inside a message-backed VARIANT payload of a row that needs
/// normalization counts exactly once wherever it sits: in the first
/// occurrence (once double-counted, as the canonical bytes still carry it),
/// in the duplicate after it, or in a repeated VARIANT element after it.
#[test]
fn variant_payload_skip_counted_once_through_normalization() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let schema = Schema::new(vec![
        variant_arrow_field("single"),
        Field::new(
            "many",
            DataType::List(Arc::new(variant_arrow_field("item"))),
            true,
        ),
    ]);
    let enc = skip_field_encoder(&message_variant_descriptor(), schema);

    let kv_x_mismatched = [0x0a, 0x06, 0x0a, 0x01, b'x', 0x12, 0x01, 0x01]; // KV { k: "x", n as LEN }
    let kv_y = [0x0a, 0x03, 0x0a, 0x01, b'y']; // KV { k: "y" }
    let many_mismatched = [0x12, 0x03, 0x12, 0x01, 0x01]; // many { n as LEN }
    for (row, single) in [
        (
            [kv_x_mismatched.as_slice(), &kv_y].concat(),
            serde_json::json!({"k": "y"}),
        ),
        (
            [kv_y.as_slice(), &kv_x_mismatched].concat(),
            serde_json::json!({"k": "x"}),
        ),
        (
            [kv_y.as_slice(), &kv_y, &many_mismatched].concat(),
            serde_json::json!({"k": "y"}),
        ),
    ] {
        let (batch, skipped) = encode_row_counting_skips(&enc, &row);
        assert_eq!(batch.num_rows(), 1, "row={row:02x?}");
        assert_eq!(
            decode_variant_wire(batch.column(0), 0),
            single,
            "row={row:02x?}"
        );
        assert_eq!(skipped, 1.0, "row={row:02x?}");
    }
}

/// `message Outer { Rec rec = 1; string name = 2; Kind kind = 3; }`,
/// `message Rec { Kind kind = 3; int64 ts = 4; }`, `enum Kind { A = 0; B = 1; }` in
/// proto2 — the shape of `DbrActivityLog.activity_log_type`, a top-level
/// optional enum, beside nested messages.
fn proto2_enum_descriptor() -> MessageDescriptor {
    let field =
        |name: &str, number: i32, ty: ProtoType, type_name: Option<&str>| FieldDescriptorProto {
            name: Some(name.into()),
            number: Some(number),
            label: Some(Label::Optional as i32),
            r#type: Some(ty as i32),
            type_name: type_name.map(Into::into),
            ..Default::default()
        };
    let kind = || {
        field(
            "kind",
            3,
            ProtoType::Enum,
            Some(".wire_to_arrow_proto2_enum.Kind"),
        )
    };
    let fd = FileDescriptorProto {
        name: Some("wire_to_arrow_proto2_enum.proto".into()),
        package: Some("wire_to_arrow_proto2_enum".into()),
        syntax: Some("proto2".into()),
        enum_type: vec![EnumDescriptorProto {
            name: Some("Kind".into()),
            value: ["A", "B"]
                .iter()
                .zip(0..)
                .map(|(name, number)| EnumValueDescriptorProto {
                    name: Some((*name).into()),
                    number: Some(number),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        message_type: vec![
            DescriptorProto {
                name: Some("Outer".into()),
                field: vec![
                    field(
                        "rec",
                        1,
                        ProtoType::Message,
                        Some(".wire_to_arrow_proto2_enum.Rec"),
                    ),
                    field("name", 2, ProtoType::String, None),
                    kind(),
                ],
                ..Default::default()
            },
            DescriptorProto {
                name: Some("Rec".into()),
                field: vec![kind(), field("ts", 4, ProtoType::Int64, None)],
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let set = FileDescriptorSet { file: vec![fd] };
    let mut bytes = Vec::new();
    set.encode(&mut bytes).unwrap();
    DescriptorPool::decode(bytes.as_slice())
        .unwrap()
        .get_message_by_name("wire_to_arrow_proto2_enum.Outer")
        .unwrap()
}

/// A single wire-type-mismatched (LEN) occurrence of an otherwise-absent enum
/// counts exactly once wherever it sits relative to a needs-normalization
/// verdict (a duplicate singular field), at the top level, in a Struct and in
/// a message-backed VARIANT under every encoding, and the row is the one the
/// same bytes without that occurrence produce. Before the first pass recorded
/// skips past the verdict, an occurrence after it was stripped by
/// normalization uncounted (seen as `activity_log_type` sent as LEN).
#[test]
fn lone_mismatched_enum_counted_once_around_normalization() {
    use super::serializer::VariantMessageEncoding;

    let _guard = METRICS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let desc = proto2_enum_descriptor();
    let rec_fields = ArrowFields::from(vec![
        Field::new("kind", DataType::LargeUtf8, true),
        Field::new("ts", DataType::Int64, true),
    ]);
    let variant_schema = Schema::new(vec![
        variant_arrow_field("rec"),
        Field::new("name", DataType::LargeUtf8, true),
        Field::new("kind", DataType::LargeUtf8, true),
    ]);
    let struct_schema = Schema::new(vec![
        Field::new("rec", DataType::Struct(rec_fields), true),
        Field::new("name", DataType::LargeUtf8, true),
        Field::new("kind", DataType::LargeUtf8, true),
    ]);

    const MISMATCHED_KIND: &[u8] = &[0x1a, 0x01, 0x01]; // kind as LEN
    const NAME: &[u8] = &[0x12, 0x01, b'n'];
    const TS: &[u8] = &[0x20, 0x05];
    fn rec(body: &[&[u8]]) -> Vec<u8> {
        let body = body.concat();
        [&[0x0a, body.len() as u8][..], &body].concat()
    }
    // Each case builds its row around `m`: the mismatched occurrence, or
    // nothing for the expected row.
    type Case = (&'static str, fn(&[u8]) -> Vec<u8>);
    let cases: [Case; 5] = [
        ("top level after verdict", |m| {
            [NAME, NAME, &rec(&[TS]), m].concat()
        }),
        ("top level before verdict", |m| {
            [m, NAME, NAME, &rec(&[TS])].concat()
        }),
        ("in a duplicated rec", |m| {
            [NAME, &rec(&[TS]), &rec(&[m])].concat()
        }),
        ("in rec after a duplicate name", |m| {
            [NAME, NAME, &rec(&[TS, m])].concat()
        }),
        ("in rec before its duplicate", |m| {
            [NAME, &rec(&[TS, m]), &rec(&[TS])].concat()
        }),
    ];

    let encoders = [
        (
            "struct",
            WireToArrowEncoder::new(&desc, struct_schema).unwrap(),
        ),
        (
            "variant proto_json",
            WireToArrowEncoder::with_variant_message_encoding(
                &desc,
                variant_schema.clone(),
                VariantMessageEncoding::ProtoJson,
            )
            .unwrap(),
        ),
        (
            "variant spark",
            WireToArrowEncoder::with_variant_message_encoding(
                &desc,
                variant_schema.clone(),
                VariantMessageEncoding::Spark,
            )
            .unwrap(),
        ),
        (
            "variant spark_strip_nulls",
            WireToArrowEncoder::with_variant_message_encoding(
                &desc,
                variant_schema,
                VariantMessageEncoding::SparkStripNulls,
            )
            .unwrap(),
        ),
    ];
    for (encoding, enc) in &encoders {
        for (case, build) in &cases {
            let (batch, skipped) = encode_row_counting_skips(enc, &build(MISMATCHED_KIND));
            let (expected, expected_skipped) = encode_row_counting_skips(enc, &build(&[]));
            assert_eq!(expected_skipped, 0.0, "{encoding}: {case}");
            assert_eq!(batch.num_rows(), 1, "{encoding}: {case}");
            assert_eq!(batch, expected, "{encoding}: {case}");
            assert!(batch.column(2).is_null(0), "{encoding}: {case}");
            assert_eq!(skipped, 1.0, "{encoding}: {case}");
        }
    }
}

// -------------------------------------------------------------------------
// The first pass stops validating once a row needs normalization: an
// occurrence normalization discards (a losing singular, a cleared oneof
// member) must not drop a row the canonical bytes accept. Every
// `encode_batch` call in this crate's tests also asserts, in
// `assert_rows_match_first_pass_stopping_at_verdict`, that rows match a first
// pass that returns at the verdict.
// -------------------------------------------------------------------------

/// Both mismatch policies' encoders for one fixture.
fn encoders_for_both_policies(
    desc: &MessageDescriptor,
    schema: &Schema,
) -> [(OnWireTypeMismatch, WireToArrowEncoder); 2] {
    [OnWireTypeMismatch::SkipField, OnWireTypeMismatch::DropRow].map(|policy| {
        let enc = WireToArrowEncoder::with_options(desc, schema.clone(), policy, &[]).unwrap();
        (policy, enc)
    })
}

/// A losing singular occurrence carrying invalid JSON for a JSON-text VARIANT
/// is discarded by last-wins, so the row keeps the final valid value under
/// either policy, with no wire-type mismatch involved.
#[test]
fn losing_singular_json_variant_occurrence_keeps_row() {
    let schema = Schema::new(vec![variant_arrow_field("name")]);
    // name = "{}", name = "!", name = "{}".
    let row = [
        0x0a, 0x02, b'{', b'}', 0x0a, 0x01, b'!', 0x0a, 0x02, b'{', b'}',
    ];
    for (policy, enc) in encoders_for_both_policies(&scalar_descriptor(), &schema) {
        let batch = enc.encode_batch(&[Bytes::from(row.to_vec())]).unwrap();
        assert_eq!(batch.num_rows(), 1, "{policy:?}");
        assert_eq!(
            decode_variant_wire(batch.column(0), 0),
            serde_json::json!({}),
            "{policy:?}"
        );
    }
}

/// The oneof counterpart: a member carrying invalid JSON that a later switch
/// clears never reaches the canonical bytes, so the row is kept; the same
/// member left selected still drops it.
#[test]
fn cleared_oneof_member_with_invalid_json_keeps_row() {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        variant_arrow_field("b"),
    ]);
    // a = 1, b = "!" (switches the oneof), a = 2 (switches it back).
    let cleared = [0x08, 0x01, 0x12, 0x01, b'!', 0x08, 0x02];
    // a = 1, b = "!": b stays selected.
    let selected = [0x08, 0x01, 0x12, 0x01, b'!'];
    for (policy, enc) in encoders_for_both_policies(&oneof_descriptor(), &schema) {
        let batch = enc
            .encode_batch(&[
                Bytes::from(cleared.to_vec()),
                Bytes::from(selected.to_vec()),
            ])
            .unwrap();
        assert_eq!(batch.num_rows(), 1, "{policy:?}");
        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow::datatypes::Int32Type>()
                .value(0),
            2,
            "{policy:?}"
        );
        assert!(batch.column(1).is_null(0), "{policy:?}");
    }
}

/// `message Mixed { string one = 1; repeated string many = 2; }` — a singular
/// and a repeated JSON-text VARIANT column side by side.
fn singular_and_repeated_string_descriptor() -> MessageDescriptor {
    let string_field = |name: &str, number: i32, label: Label| FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        label: Some(label as i32),
        r#type: Some(ProtoType::String as i32),
        ..Default::default()
    };
    let fd = FileDescriptorProto {
        name: Some("wire_to_arrow_mixed_variant_test.proto".into()),
        package: Some("wire_to_arrow_mixed_variant_test".into()),
        message_type: vec![DescriptorProto {
            name: Some("Mixed".into()),
            field: vec![
                string_field("one", 1, Label::Optional),
                string_field("many", 2, Label::Repeated),
            ],
            ..Default::default()
        }],
        ..Default::default()
    };
    let set = FileDescriptorSet { file: vec![fd] };
    let mut bytes = Vec::new();
    set.encode(&mut bytes).unwrap();
    DescriptorPool::decode(bytes.as_slice())
        .unwrap()
        .get_message_by_name("wire_to_arrow_mixed_variant_test.Mixed")
        .unwrap()
}

fn singular_and_repeated_variant_schema() -> Schema {
    Schema::new(vec![
        variant_arrow_field("one"),
        Field::new(
            "many",
            DataType::List(Arc::new(variant_arrow_field("item"))),
            true,
        ),
    ])
}

/// Normalization discards a losing singular occurrence but keeps every
/// repeated element, so invalid JSON after the duplicate is harmless in the
/// singular field and still drops the row in the repeated one.
#[test]
fn invalid_json_after_duplicate_drops_row_only_when_repeated() {
    use arrow::array::ListArray;

    let schema = singular_and_repeated_variant_schema();
    // one = "{}", one = "!", one = "1", many = "2".
    let losing_singular = [
        0x0a, 0x02, b'{', b'}', 0x0a, 0x01, b'!', 0x0a, 0x01, b'1', 0x12, 0x01, b'2',
    ];
    // one = "{}", one = "1", many = "!".
    let repeated_after_duplicate = [0x0a, 0x02, b'{', b'}', 0x0a, 0x01, b'1', 0x12, 0x01, b'!'];
    for (policy, enc) in
        encoders_for_both_policies(&singular_and_repeated_string_descriptor(), &schema)
    {
        let (batch, kept) = enc
            .encode_batch_with_kept(&[
                Bytes::from(losing_singular.to_vec()),
                Bytes::from(repeated_after_duplicate.to_vec()),
            ])
            .unwrap();
        assert_eq!(kept, [0], "{policy:?}");
        assert_eq!(
            decode_variant_wire(batch.column(0), 0),
            serde_json::json!(1),
            "{policy:?}"
        );
        let many = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(0);
        assert_eq!(many.len(), 1, "{policy:?}");
        assert_eq!(
            decode_variant_wire(&many, 0),
            serde_json::json!(2),
            "{policy:?}"
        );
    }
}

/// Occurrence pools for [`first_pass_past_verdict_never_changes_rows`]: one
/// fixture per verdict shape, each pool mixing valid values, values only
/// validation rejects (invalid JSON, invalid UTF-8), and wire-type mismatches.
fn verdict_fixture_pools() -> Vec<(MessageDescriptor, Schema, Vec<Vec<u8>>)> {
    let kv_fields = ArrowFields::from(vec![
        Field::new("k", DataType::LargeUtf8, true),
        Field::new("n", DataType::Int64, true),
    ]);
    vec![
        (
            scalar_descriptor(),
            Schema::new(vec![
                variant_arrow_field("name"),
                Field::new("id", DataType::Int32, true),
                Field::new("email", DataType::LargeUtf8, true),
            ]),
            vec![
                vec![0x0a, 0x02, b'{', b'}'],
                vec![0x0a, 0x01, b'!'],
                vec![0x0a, 0x01, 0xff],
                vec![0x08, 0x01],
                vec![0x10, 0x07],
                vec![0x12, 0x01, 0x01],
                vec![0x1a, 0x01, b'x'],
                vec![0x1a, 0x01, 0xff],
                vec![0xf8, 0x06, 0x01],
            ],
        ),
        (
            oneof_descriptor(),
            Schema::new(vec![
                Field::new("a", DataType::Int32, true),
                variant_arrow_field("b"),
            ]),
            vec![
                vec![0x08, 0x01],
                vec![0x08, 0x02],
                vec![0x12, 0x02, b'{', b'}'],
                vec![0x12, 0x01, b'!'],
                vec![0x0a, 0x01, 0x01],
                vec![0x10, 0x01],
            ],
        ),
        (
            singular_and_repeated_string_descriptor(),
            singular_and_repeated_variant_schema(),
            vec![
                vec![0x0a, 0x02, b'{', b'}'],
                vec![0x0a, 0x01, b'!'],
                vec![0x12, 0x01, b'1'],
                vec![0x12, 0x01, b'!'],
                vec![0x08, 0x01],
                vec![0x10, 0x01],
            ],
        ),
        (
            message_variant_descriptor(),
            Schema::new(vec![
                variant_arrow_field("single"),
                Field::new(
                    "many",
                    DataType::List(Arc::new(variant_arrow_field("item"))),
                    true,
                ),
            ]),
            vec![
                vec![0x0a, 0x03, 0x0a, 0x01, b'y'],
                vec![0x0a, 0x06, 0x0a, 0x01, b'x', 0x12, 0x01, 0x01],
                vec![0x0a, 0x03, 0x0a, 0x01, 0xff],
                vec![0x12, 0x03, 0x12, 0x01, 0x01],
                vec![0x12, 0x03, 0x0a, 0x01, 0xff],
                vec![0x08, 0x01],
            ],
        ),
        (
            message_variant_descriptor(),
            Schema::new(vec![
                Field::new("single", DataType::Struct(kv_fields.clone()), true),
                Field::new(
                    "many",
                    DataType::List(Arc::new(Field::new(
                        "item",
                        DataType::Struct(kv_fields),
                        true,
                    ))),
                    true,
                ),
            ]),
            vec![
                vec![0x0a, 0x03, 0x0a, 0x01, b'y'],
                vec![0x0a, 0x06, 0x0a, 0x01, b'x', 0x12, 0x01, 0x01],
                vec![0x0a, 0x03, 0x0a, 0x01, 0xff],
                vec![0x0a, 0x04, 0x10, 0x01, 0x10, 0x02],
                vec![0x12, 0x03, 0x0a, 0x01, 0xff],
                vec![0x08, 0x01],
            ],
        ),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        timeout: 10_000,
        ..ProptestConfig::default()
    })]

    /// Rows assembled from the fixture pools hit duplicates, oneof switches
    /// and invalid occurrences on both sides of the verdict; `encode_batch`
    /// asserts each batch matches the first pass that stops at the verdict.
    #[test]
    fn first_pass_past_verdict_never_changes_rows(
        rows in proptest::collection::vec(proptest::collection::vec(any::<usize>(), 0..7), 1..4),
    ) {
        for (desc, schema, pool) in verdict_fixture_pools() {
            let messages: Vec<Bytes> = rows
                .iter()
                .map(|picks| {
                    let row: Vec<u8> = picks
                        .iter()
                        .flat_map(|&i| pool[i % pool.len()].iter().copied())
                        .collect();
                    Bytes::from(row)
                })
                .collect();
            for (_, enc) in encoders_for_both_policies(&desc, &schema) {
                let _ = enc.encode_batch(&messages);
            }
        }
    }
}

/// Config deserialization: `on_wire_type_mismatch` accepts both policy names
/// and defaults to `skip_field` when omitted.
#[test]
fn config_deserializes_mismatch_policy() {
    let config: WireToArrowSerializerConfig = serde_json::from_str(
        r#"{"desc_file": "/tmp/x.desc", "message_type": "test_protobuf.Person"}"#,
    )
    .unwrap();
    assert_eq!(config.on_wire_type_mismatch, OnWireTypeMismatch::SkipField);
    assert!(config.attribution_fields.is_empty());

    let config: WireToArrowSerializerConfig = serde_json::from_str(
        r#"{"desc_file": "/tmp/x.desc", "message_type": "test_protobuf.Person",
            "on_wire_type_mismatch": "drop_row"}"#,
    )
    .unwrap();
    assert_eq!(config.on_wire_type_mismatch, OnWireTypeMismatch::DropRow);

    assert!(
        serde_json::from_str::<WireToArrowSerializerConfig>(
            r#"{"desc_file": "/tmp/x.desc", "message_type": "test_protobuf.Person",
                "on_wire_type_mismatch": "bogus"}"#,
        )
        .is_err(),
        "unknown policy name must be rejected"
    );
}

/// `attribution_fields` beyond the 8-field cap are ignored, and a name not
/// present in the descriptor warns and is ignored.
#[test]
fn attribution_fields_capped_and_unknown_ignored() {
    // message Wide { int32 f1 = 1; ... int32 f10 = 10; }
    let fields: Vec<FieldDescriptorProto> = (1..=10)
        .map(|i| FieldDescriptorProto {
            name: Some(format!("f{i}")),
            number: Some(i),
            label: Some(Label::Optional as i32),
            r#type: Some(ProtoType::Int32 as i32),
            ..Default::default()
        })
        .collect();
    let fd = FileDescriptorProto {
        name: Some("wire_to_arrow_attribution_test.proto".into()),
        package: Some("wire_to_arrow_attribution_test".into()),
        message_type: vec![DescriptorProto {
            name: Some("Wide".into()),
            field: fields,
            ..Default::default()
        }],
        ..Default::default()
    };
    let set = FileDescriptorSet { file: vec![fd] };
    let desc = DescriptorPool::decode(set.encode_to_vec().as_slice())
        .unwrap()
        .get_message_by_name("wire_to_arrow_attribution_test.Wide")
        .unwrap();

    let schema = Schema::new(
        (1..=10)
            .map(|i| Field::new(format!("f{i}"), DataType::Int32, true))
            .collect::<Vec<_>>(),
    );
    // "nope" occupies one of the first 8 slots (warned + ignored), so only
    // f1..f7 fit under the cap and f8+ are dropped by it.
    let names: Vec<String> = ["f1", "f2", "nope", "f3", "f4", "f5", "f6", "f7", "f8", "f9"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let enc =
        WireToArrowEncoder::with_options(&desc, schema, OnWireTypeMismatch::SkipField, &names)
            .unwrap();

    let mut wire = Vec::new();
    for i in 1u8..=9 {
        wire.push(i << 3);
        wire.push(i);
    }
    let attribution = enc.attribution_from_row(&wire);
    assert_eq!(
        attribution, "f1=1,f2=2,f3=3,f4=4,f5=5,f6=6,f7=7",
        "unknown name ignored, cap keeps the first 8 configured names"
    );
}

// -------------------------------------------------------------------------
// Message-typed fields stored as opaque binary columns.
// -------------------------------------------------------------------------

/// `Row { Inner opaque = 1; repeated Inner opaques = 2; string name = 3; }`,
/// `Inner { int32 a = 1; string b = 2; }`.
fn opaque_message_descriptor() -> MessageDescriptor {
    let field = |name: &str, number: i32, ty: ProtoType, label: Label, type_name: Option<&str>| {
        FieldDescriptorProto {
            name: Some(name.into()),
            number: Some(number),
            label: Some(label as i32),
            r#type: Some(ty as i32),
            type_name: type_name.map(str::to_owned),
            ..Default::default()
        }
    };
    let fd = FileDescriptorProto {
        name: Some("opaque_test.proto".into()),
        package: Some("opaque_test".into()),
        message_type: vec![
            DescriptorProto {
                name: Some("Inner".into()),
                field: vec![
                    field("a", 1, ProtoType::Int32, Label::Optional, None),
                    field("b", 2, ProtoType::String, Label::Optional, None),
                ],
                ..Default::default()
            },
            DescriptorProto {
                name: Some("Row".into()),
                field: vec![
                    field(
                        "opaque",
                        1,
                        ProtoType::Message,
                        Label::Optional,
                        Some(".opaque_test.Inner"),
                    ),
                    field(
                        "opaques",
                        2,
                        ProtoType::Message,
                        Label::Repeated,
                        Some(".opaque_test.Inner"),
                    ),
                    field("name", 3, ProtoType::String, Label::Optional, None),
                ],
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let mut bytes = Vec::new();
    FileDescriptorSet { file: vec![fd] }
        .encode(&mut bytes)
        .unwrap();
    DescriptorPool::decode(bytes.as_slice())
        .unwrap()
        .get_message_by_name("opaque_test.Row")
        .unwrap()
}

#[test]
fn opaque_message_fields_become_binary_columns() {
    let desc = opaque_message_descriptor();
    let schema = Schema::new(vec![
        Field::new("opaque", DataType::LargeBinary, true),
        Field::new(
            "opaques",
            DataType::List(Arc::new(Field::new("item", DataType::LargeBinary, true))),
            true,
        ),
        Field::new("name", DataType::LargeUtf8, true),
    ]);
    let enc = WireToArrowEncoder::new(&desc, schema).unwrap();

    // Inner { a: 7, b: "x" } = 08 07 12 01 78; Inner { a: 9 } = 08 09.
    let inner_one = [0x08u8, 0x07, 0x12, 0x01, b'x'];
    let inner_two = [0x08u8, 0x09];
    let mut row = vec![0x0a, inner_one.len() as u8];
    row.extend_from_slice(&inner_one);
    row.extend_from_slice(&[0x12, inner_two.len() as u8]);
    row.extend_from_slice(&inner_two);
    row.extend_from_slice(&[0x12, inner_one.len() as u8]);
    row.extend_from_slice(&inner_one);
    row.extend_from_slice(&[0x1a, 0x02, b'o', b'k']);

    let batch = enc.encode_batch(&[Bytes::from(row), Bytes::new()]).unwrap();
    assert_eq!(batch.num_rows(), 2);
    let opaque = batch.column(0).as_binary::<i64>();
    assert_eq!(opaque.value(0), inner_one);
    assert!(opaque.is_null(1));
    let list = batch.column(1).as_list::<i32>();
    let items = list.value(0);
    let items = items.as_binary::<i64>();
    assert_eq!(items.len(), 2);
    assert_eq!(items.value(0), inner_two);
    assert_eq!(items.value(1), inner_one);
    assert_eq!(batch.column(2).as_string::<i64>().value(0), "ok");
    // A message-typed field against a struct column keeps its struct handling.
    let struct_schema = Schema::new(vec![Field::new(
        "opaque",
        DataType::Struct(ArrowFields::from(vec![Field::new(
            "a",
            DataType::Int32,
            true,
        )])),
        true,
    )]);
    WireToArrowEncoder::new(&desc, struct_schema).unwrap();
}

#[test]
fn opaque_message_field_against_other_scalar_columns_still_errors() {
    let desc = opaque_message_descriptor();
    let schema = Schema::new(vec![Field::new("opaque", DataType::Int64, true)]);
    assert!(matches!(
        WireToArrowEncoder::new(&desc, schema),
        Err(WireToArrowError::UnsupportedKind { .. })
    ));
}

// -------------------------------------------------------------------------
// Classic-compatible date handling: int64 ms -> Date32, ISO-8601 string -> Timestamp.
// -------------------------------------------------------------------------

fn int64_row(value: i64) -> Bytes {
    let mut row = vec![1u8 << 3];
    encode_varint_into(&mut row, value as u64);
    Bytes::from(row)
}

fn date32_encoder() -> WireToArrowEncoder {
    let schema = Schema::new(vec![Field::new("event_time", DataType::Date32, true)]);
    WireToArrowEncoder::new(&timestamp_descriptor(), schema).unwrap()
}

#[test]
fn int64_ms_to_date32_matches_classic_utc_day() {
    let enc = date32_encoder();
    let ms_per_day = 86_400_000i64;
    let cases: Vec<(i64, i32)> = vec![
        (0, 0),                      // 1970-01-01, a real date and not null
        (ms_per_day - 1, 0),         // last millisecond of the first day
        (ms_per_day, 1),             // first millisecond of 1970-01-02
        (1_700_000_000_000, 19_675), // 2023-11-14
        (-1, -1),                    // 1969-12-31
        (-ms_per_day, -1),           // exactly one day before the epoch
        (-ms_per_day - 1, -2),       // floors, not truncates toward zero
        (i64::MAX, i32::MAX),        // beyond the i32 day range saturates, never wraps
        (i64::MIN, i32::MIN),
    ];
    let mut rows: Vec<Bytes> = cases.iter().map(|(ms, _)| int64_row(*ms)).collect();
    rows.push(Bytes::new()); // absent field -> null
    let batch = enc.encode_batch(&rows).unwrap();
    assert_eq!(batch.num_rows(), cases.len() + 1);
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Date32Array>()
        .expect("Date32Array");
    for (i, (ms, days)) in cases.iter().enumerate() {
        assert_eq!(col.value(i), *days, "ms={ms}");
    }
    assert!(col.is_null(cases.len()));
}

#[test]
fn zigzag_int64_to_date32_uses_the_decoded_value() {
    // sint64 field paired with a Date32 column: -1 ms is zigzag 1 and must land on 1969-12-31.
    let fd = FileDescriptorProto {
        name: Some("wire_to_arrow_sint_date.proto".into()),
        package: Some("wire_to_arrow_poc_test".into()),
        message_type: vec![DescriptorProto {
            name: Some("SintDate".into()),
            field: vec![FieldDescriptorProto {
                name: Some("d".into()),
                number: Some(1),
                label: Some(Label::Optional as i32),
                r#type: Some(ProtoType::Sint64 as i32),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut bytes = Vec::new();
    FileDescriptorSet { file: vec![fd] }
        .encode(&mut bytes)
        .unwrap();
    let desc = DescriptorPool::decode(bytes.as_slice())
        .unwrap()
        .get_message_by_name("wire_to_arrow_poc_test.SintDate")
        .unwrap();
    let enc = WireToArrowEncoder::new(
        &desc,
        Schema::new(vec![Field::new("d", DataType::Date32, true)]),
    )
    .unwrap();
    let batch = enc.encode_batch(&[Bytes::from(vec![0x08, 0x01])]).unwrap();
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Date32Array>()
        .unwrap();
    assert_eq!(col.value(0), -1);
}

fn string_timestamp_descriptor() -> MessageDescriptor {
    let fd = FileDescriptorProto {
        name: Some("wire_to_arrow_str_ts.proto".into()),
        package: Some("wire_to_arrow_poc_test".into()),
        message_type: vec![DescriptorProto {
            name: Some("StrTs".into()),
            field: vec![FieldDescriptorProto {
                name: Some("ts".into()),
                number: Some(1),
                label: Some(Label::Optional as i32),
                r#type: Some(ProtoType::String as i32),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut bytes = Vec::new();
    FileDescriptorSet { file: vec![fd] }
        .encode(&mut bytes)
        .unwrap();
    DescriptorPool::decode(bytes.as_slice())
        .unwrap()
        .get_message_by_name("wire_to_arrow_poc_test.StrTs")
        .unwrap()
}

fn string_row(text: &str) -> Bytes {
    let mut row = vec![(1u8 << 3) | 2, text.len() as u8];
    row.extend_from_slice(text.as_bytes());
    Bytes::from(row)
}

#[test]
fn iso_string_to_timestamp_matches_classic_instant_parse() {
    let ts_type = DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()));
    let enc = WireToArrowEncoder::new(
        &string_timestamp_descriptor(),
        Schema::new(vec![Field::new("ts", ts_type, true)]),
    )
    .unwrap();
    let seconds = 1_700_000_000i64 * 1_000_000;
    let cases: Vec<(&str, i64)> = vec![
        ("2023-11-14T22:13:20Z", seconds),
        ("2023-11-14T23:13:20+01:00", seconds), // offset is applied
        ("2023-11-14T22:13:20.123456789Z", seconds + 123_456), // sub-microsecond truncated
        ("2023-11-14T22:13:20.5Z", seconds + 500_000),
        ("1969-12-31T23:59:59.999999999Z", -1), // floors for pre-epoch instants
        ("1970-01-01T00:00:00Z", 0),
        ("not a date", 0),                      // unparseable -> epoch 0, row kept
        ("", 0),                                // present but empty -> epoch 0
        ("2023-11-14", 0),                      // date only is not an instant
        ("2023-11-14 22:13:20Z", 0),            // no 'T'
        ("2023-11-14T22:13:20.1234567890Z", 0), // >9 fraction digits fail like Instant.parse
    ];
    let mut rows: Vec<Bytes> = cases.iter().map(|(text, _)| string_row(text)).collect();
    rows.push(Bytes::new()); // absent -> null
    let batch = enc.encode_batch(&rows).unwrap();
    assert_eq!(batch.num_rows(), cases.len() + 1, "no row may be dropped");
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
        .expect("TimestampMicrosecondArray");
    for (i, (text, micros)) in cases.iter().enumerate() {
        assert_eq!(col.value(i), *micros, "input {text:?}");
    }
    assert!(col.is_null(cases.len()));
}

#[test]
fn existing_date_and_timestamp_pairings_are_unchanged() {
    // int64 -> Int64 and int64 -> Timestamp keep their behavior.
    let ts_type = DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()));
    let ts = WireToArrowEncoder::new(
        &timestamp_descriptor(),
        Schema::new(vec![Field::new("event_time", ts_type, true)]),
    )
    .unwrap();
    let batch = ts.encode_batch(&[int64_row(1_700_000_000_000)]).unwrap();
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(col.value(0), 1_700_000_000_000 * 1000);
    let plain = WireToArrowEncoder::new(
        &timestamp_descriptor(),
        Schema::new(vec![Field::new("event_time", DataType::Int64, true)]),
    )
    .unwrap();
    let batch = plain.encode_batch(&[int64_row(7)]).unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow::datatypes::Int64Type>()
            .value(0),
        7
    );

    // string -> LargeUtf8 stays text (not parsed).
    let text = WireToArrowEncoder::new(
        &string_timestamp_descriptor(),
        Schema::new(vec![Field::new("ts", DataType::LargeUtf8, true)]),
    )
    .unwrap();
    let batch = text
        .encode_batch(&[string_row("2023-11-14T22:13:20Z")])
        .unwrap();
    assert_eq!(
        batch.column(0).as_string::<i64>().value(0),
        "2023-11-14T22:13:20Z"
    );
}

#[test]
fn unsupported_date_pairings_still_fail_plan_build() {
    // string -> Date32, int32 -> Date32 and int64 -> Date64 were and remain plan-build errors.
    let string_to_date = WireToArrowEncoder::new(
        &string_timestamp_descriptor(),
        Schema::new(vec![Field::new("ts", DataType::Date32, true)]),
    );
    assert!(matches!(
        string_to_date,
        Err(WireToArrowError::UnsupportedCombination { .. })
    ));
    let int64_to_date64 = WireToArrowEncoder::new(
        &timestamp_descriptor(),
        Schema::new(vec![Field::new("event_time", DataType::Date64, true)]),
    );
    assert!(int64_to_date64.is_err());
}
