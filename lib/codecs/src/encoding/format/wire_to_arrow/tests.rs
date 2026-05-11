use super::append::{decode_varint, read_fixed32, read_fixed64};
use super::{
    WireToArrowEncoder, WireToArrowError, WireToArrowSerializer, WireToArrowSerializerConfig,
};

use proptest::prelude::*;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Field, Fields as ArrowFields, Schema};
use bytes::Bytes;
use prost_reflect::MessageDescriptor;
use prost_reflect::prost::Message as _;
use prost_reflect::prost_types::field_descriptor_proto::{Label, Type as ProtoType};
use prost_reflect::prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    OneofDescriptorProto,
};
use prost_reflect::{DescriptorPool, DynamicMessage, Value as ProtoValue};
use std::path::PathBuf;
use std::sync::Arc;
use vector_core::event::Event;

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

    let ids = batch.column(2).as_primitive::<arrow::datatypes::Int32Type>();
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
        Field::new("data", DataType::Map(entry_field, false), /* nullable */ false),
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
        Field::new("dropped_from_proto", DataType::Int64, /* nullable */ false),
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
    // proto says `id: int32`, Arrow says `id: Date32`. Date32 isn't in
    // `TypedBuilder::supports`, so plan-build must reject up front
    // rather than letting the first batch panic inside `TypedBuilder::new`.
    let desc = scalar_descriptor();
    let schema = Schema::new(vec![
        Field::new("name", DataType::LargeUtf8, true),
        Field::new("id", DataType::Date32, true),
        Field::new("email", DataType::LargeUtf8, true),
    ]);
    let err = WireToArrowEncoder::new(&desc, schema).expect_err("should reject Date32");
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
        Field::new("created_at", DataType::Date32, true),
    ]);
    let err = WireToArrowEncoder::new(&desc, schema)
        .expect_err("should reject Date32 on absent slot");
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
    let ids = batch.column(0).as_primitive::<arrow::datatypes::Int32Type>();
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
