//! Spark-parity VARIANT encoding: one test per classic behavior, against descriptors built here.

use std::collections::HashMap;

use bytes::Bytes;
use parquet_variant::Variant;
use prost_reflect::prost::Message as _;
use prost_reflect::prost_types::field_descriptor_proto::{Label, Type as ProtoType};
use prost_reflect::prost_types::{
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FieldOptions, FileDescriptorProto, FileDescriptorSet, MessageOptions, OneofDescriptorProto,
};
use prost_reflect::{
    DescriptorPool, DynamicMessage, MapKey, MessageDescriptor, ReflectMessage, Value,
};
use zeroparser::wire::WireValue;

use super::SparkVariantEncoder;
use crate::encoding::format::wire_to_arrow::errors::WireToArrowError;
use crate::encoding::format::wire_to_arrow::serializer::VariantMessageEncoding;
use crate::encoding::format::wire_to_arrow::variant_message::VariantMessagePlan;
use crate::encoding::format::wire_to_arrow::{WireToArrowEncoder, WireToArrowSerializerConfig};

fn field(name: &str, number: i32, label: Label, ty: ProtoType) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        label: Some(label as i32),
        r#type: Some(ty as i32),
        ..Default::default()
    }
}

fn typed(
    name: &str,
    number: i32,
    label: Label,
    ty: ProtoType,
    type_name: &str,
) -> FieldDescriptorProto {
    FieldDescriptorProto {
        type_name: Some(type_name.into()),
        ..field(name, number, label, ty)
    }
}

fn message(name: &str, fields: Vec<FieldDescriptorProto>) -> DescriptorProto {
    DescriptorProto {
        name: Some(name.into()),
        field: fields,
        ..Default::default()
    }
}

fn map_entry(name: &str, key: ProtoType, value: FieldDescriptorProto) -> DescriptorProto {
    DescriptorProto {
        options: Some(MessageOptions {
            map_entry: Some(true),
            ..Default::default()
        }),
        ..message(name, vec![field("key", 1, Label::Optional, key), value])
    }
}

fn enum_type(name: &str, values: &[(&str, i32)]) -> EnumDescriptorProto {
    EnumDescriptorProto {
        name: Some(name.into()),
        value: values
            .iter()
            .map(|(name, number)| EnumValueDescriptorProto {
                name: Some((*name).into()),
                number: Some(*number),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn well_known(file: &str, name: &str, fields: Vec<FieldDescriptorProto>) -> FileDescriptorProto {
    FileDescriptorProto {
        name: Some(format!("google/protobuf/{file}")),
        package: Some("google.protobuf".into()),
        syntax: Some("proto3".into()),
        message_type: vec![message(name, fields)],
        ..Default::default()
    }
}

/// proto2 `vs2.P2` (every scalar, an explicit default, a closed enum, nested and repeated messages,
/// maps, well-known types, empty message types and a oneof), its envelope `vs2.Envelope`, and
/// proto3 `vs3.P3` (implicit presence, `optional`, an open enum).
fn pool() -> DescriptorPool {
    use Label::{Optional, Repeated};
    use ProtoType as T;
    let seconds_nanos = || {
        vec![
            field("seconds", 1, Optional, T::Int64),
            field("nanos", 2, Optional, T::Int32),
        ]
    };
    let p2_fields = vec![
        field("o_int32", 1, Optional, T::Int32),
        field("o_int64", 2, Optional, T::Int64),
        field("o_uint32", 3, Optional, T::Uint32),
        field("o_uint64", 4, Optional, T::Uint64),
        field("o_fixed32", 5, Optional, T::Fixed32),
        field("o_fixed64", 6, Optional, T::Fixed64),
        field("o_float", 7, Optional, T::Float),
        field("o_double", 8, Optional, T::Double),
        field("o_bool", 9, Optional, T::Bool),
        field("o_string", 10, Optional, T::String),
        field("o_bytes", 11, Optional, T::Bytes),
        FieldDescriptorProto {
            default_value: Some("5".into()),
            ..field("o_default", 12, Optional, T::Int32)
        },
        typed("color", 13, Optional, T::Enum, ".vs2.Color"),
        typed("colors", 14, Repeated, T::Enum, ".vs2.Color"),
        typed("child", 15, Optional, T::Message, ".vs2.Leaf"),
        typed("children", 16, Repeated, T::Message, ".vs2.Leaf"),
        typed("labels", 17, Repeated, T::Message, ".vs2.P2.LabelsEntry"),
        typed("by_id", 18, Repeated, T::Message, ".vs2.P2.ByIdEntry"),
        typed("ts", 19, Optional, T::Message, ".google.protobuf.Timestamp"),
        typed("dur", 20, Optional, T::Message, ".google.protobuf.Duration"),
        typed("any", 21, Optional, T::Message, ".google.protobuf.Any"),
        typed("empty", 22, Optional, T::Message, ".vs2.Empty"),
        typed("empties", 23, Repeated, T::Message, ".vs2.Empty"),
        typed(
            "empty_map",
            24,
            Repeated,
            T::Message,
            ".vs2.P2.EmptyMapEntry",
        ),
        typed("hollow", 25, Optional, T::Message, ".vs2.Hollow"),
        field("r_int32", 26, Repeated, T::Int32),
        FieldDescriptorProto {
            oneof_index: Some(0),
            ..field("c_str", 27, Optional, T::String)
        },
        FieldDescriptorProto {
            oneof_index: Some(0),
            ..typed("c_leaf", 28, Optional, T::Message, ".vs2.Leaf")
        },
    ];
    let p2 = DescriptorProto {
        nested_type: vec![
            map_entry(
                "LabelsEntry",
                T::String,
                field("value", 2, Optional, T::Int64),
            ),
            map_entry(
                "ByIdEntry",
                T::Uint32,
                typed("value", 2, Optional, T::Message, ".vs2.Leaf"),
            ),
            map_entry(
                "EmptyMapEntry",
                T::String,
                typed("value", 2, Optional, T::Message, ".vs2.Empty"),
            ),
        ],
        oneof_decl: vec![OneofDescriptorProto {
            name: Some("choice".into()),
            ..Default::default()
        }],
        ..message("P2", p2_fields)
    };
    let vs2 = FileDescriptorProto {
        name: Some("vs2.proto".into()),
        package: Some("vs2".into()),
        syntax: Some("proto2".into()),
        dependency: vec![
            "google/protobuf/timestamp.proto".into(),
            "google/protobuf/duration.proto".into(),
            "google/protobuf/any.proto".into(),
        ],
        message_type: vec![
            p2,
            message(
                "Leaf",
                vec![
                    field("name", 1, Optional, T::String),
                    field("n", 2, Optional, T::Int32),
                ],
            ),
            message("Empty", vec![]),
            message(
                "Hollow",
                vec![typed("e", 1, Optional, T::Message, ".vs2.Empty")],
            ),
            message(
                "Envelope",
                vec![
                    field("id", 1, Optional, T::Int32),
                    typed("payload", 2, Optional, T::Message, ".vs2.P2"),
                ],
            ),
        ],
        enum_type: vec![enum_type("Color", &[("RED", 1), ("BLUE", 2)])],
        ..Default::default()
    };
    let vs3 = FileDescriptorProto {
        name: Some("vs3.proto".into()),
        package: Some("vs3".into()),
        syntax: Some("proto3".into()),
        message_type: vec![
            DescriptorProto {
                oneof_decl: vec![OneofDescriptorProto {
                    name: Some("_opt".into()),
                    ..Default::default()
                }],
                ..message(
                    "P3",
                    vec![
                        field("s", 1, Optional, T::String),
                        field("i", 2, Optional, T::Int64),
                        field("b", 3, Optional, T::Bool),
                        field("f", 4, Optional, T::Float),
                        typed("mode", 5, Optional, T::Enum, ".vs3.Mode"),
                        FieldDescriptorProto {
                            oneof_index: Some(0),
                            proto3_optional: Some(true),
                            ..field("opt", 6, Optional, T::Int32)
                        },
                        field("raw", 7, Optional, T::Bytes),
                        field("tags", 8, Repeated, T::String),
                        typed("leaf", 9, Optional, T::Message, ".vs3.Leaf3"),
                    ],
                )
            },
            message("Leaf3", vec![field("x", 1, Optional, T::Int32)]),
        ],
        enum_type: vec![enum_type("Mode", &[("MODE_UNSET", 0), ("MODE_ON", 1)])],
        ..Default::default()
    };
    let set = FileDescriptorSet {
        file: vec![
            well_known("timestamp.proto", "Timestamp", seconds_nanos()),
            well_known("duration.proto", "Duration", seconds_nanos()),
            well_known(
                "any.proto",
                "Any",
                vec![
                    field("type_url", 1, Optional, T::String),
                    field("value", 2, Optional, T::Bytes),
                ],
            ),
            vs2,
            vs3,
        ],
    };
    DescriptorPool::decode(set.encode_to_vec().as_slice()).expect("test descriptors")
}

fn descriptor(name: &str) -> MessageDescriptor {
    pool().get_message_by_name(name).expect(name)
}

fn build(name: &str, fields: &[(&str, Value)]) -> DynamicMessage {
    let mut message = DynamicMessage::new(descriptor(name));
    for (field, value) in fields {
        message.set_field_by_name(field, value.clone());
    }
    message
}

fn leaf(name: &str) -> Value {
    Value::Message(build("vs2.Leaf", &[("name", Value::String(name.into()))]))
}

/// A Variant rendered with its types: `i8:1`, `f32:NaN`, `bin:00ff`, `"s"`, `{k: v}`, `[a, b]`.
fn render(variant: &Variant) -> String {
    match variant {
        Variant::Null => "null".into(),
        Variant::BooleanTrue => "true".into(),
        Variant::BooleanFalse => "false".into(),
        Variant::Int8(i) => format!("i8:{i}"),
        Variant::Int16(i) => format!("i16:{i}"),
        Variant::Int32(i) => format!("i32:{i}"),
        Variant::Int64(i) => format!("i64:{i}"),
        Variant::Float(f) => format!("f32:{f}"),
        Variant::Double(f) => format!("f64:{f}"),
        Variant::Binary(b) => format!(
            "bin:{}",
            b.iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        ),
        Variant::String(_) | Variant::ShortString(_) => {
            format!("{:?}", variant.as_string().expect("string variant"))
        }
        Variant::Object(object) => {
            let fields: Vec<String> = object
                .iter()
                .map(|(key, value)| format!("{key}: {}", render(&value)))
                .collect();
            format!("{{{}}}", fields.join(", "))
        }
        Variant::List(list) => {
            let items: Vec<String> = list.iter().map(|item| render(&item)).collect();
            format!("[{}]", items.join(", "))
        }
        other => format!("{other:?}"),
    }
}

fn encode_with(message: &DynamicMessage, strip_nulls: bool) -> String {
    let encoder = SparkVariantEncoder::new(&message.descriptor(), strip_nulls);
    let (metadata, value) = encoder.encode(&message.encode_to_vec()).expect("encode");
    render(&Variant::new(&metadata, &value))
}

/// The encoding with null keys stripped, as the union stores it.
fn stripped(message: &DynamicMessage) -> String {
    encode_with(message, true)
}

/// The rendered value of one top-level key (`absent` when the key is not stored).
fn key(message: &DynamicMessage, strip_nulls: bool, name: &str) -> String {
    let encoder = SparkVariantEncoder::new(&message.descriptor(), strip_nulls);
    let (metadata, value) = encoder.encode(&message.encode_to_vec()).expect("encode");
    let variant = Variant::new(&metadata, &value);
    let object = variant.as_object().expect("object");
    object
        .get(name)
        .map_or_else(|| "absent".into(), |value| render(&value))
}

const ALWAYS_PRESENT: &str = "by_id: {}, children: [], colors: [], labels: {}, o_default: i8:5, \
                              r_int32: []";

#[test]
fn bytes_are_native_binary() {
    let message = build(
        "vs2.P2",
        &[("o_bytes", Value::Bytes(Bytes::from_static(b"\x00\xff")))],
    );
    assert_eq!(key(&message, true, "o_bytes"), "bin:00ff");
}

#[test]
fn timestamp_and_duration_are_seconds_nanos_objects() {
    let ts = build(
        "google.protobuf.Timestamp",
        &[("seconds", Value::I64(1_700_000_000))],
    );
    let dur = build(
        "google.protobuf.Duration",
        &[("seconds", Value::I64(3)), ("nanos", Value::I32(500))],
    );
    let message = build(
        "vs2.P2",
        &[("ts", Value::Message(ts)), ("dur", Value::Message(dur))],
    );
    // Both are proto3 messages, so a zero `nanos` is written out (emit.default.values).
    assert_eq!(
        key(&message, true, "ts"),
        "{nanos: i8:0, seconds: i32:1700000000}"
    );
    assert_eq!(
        key(&message, true, "dur"),
        "{nanos: i16:500, seconds: i8:3}"
    );
}

#[test]
fn integers_take_the_narrowest_width_of_their_java_value() {
    let message = build(
        "vs2.P2",
        &[
            ("o_int32", Value::I32(-200)),
            ("o_int64", Value::I64(1 << 40)),
            ("o_uint32", Value::U32(u32::MAX)),
            ("o_uint64", Value::U64(u64::MAX)),
            ("o_fixed32", Value::U32(3_000_000_000)),
            ("o_fixed64", Value::U64(70_000)),
        ],
    );
    assert_eq!(key(&message, true, "o_int32"), "i16:-200");
    assert_eq!(key(&message, true, "o_int64"), "i64:1099511627776");
    // Unsigned values past the signed range wrap, as a Java int/long does.
    assert_eq!(key(&message, true, "o_uint32"), "i8:-1");
    assert_eq!(key(&message, true, "o_uint64"), "i8:-1");
    assert_eq!(key(&message, true, "o_fixed32"), "i32:-1294967296");
    assert_eq!(key(&message, true, "o_fixed64"), "i32:70000");
}

#[test]
fn floats_keep_their_width_and_non_finite_values() {
    let message = build(
        "vs2.P2",
        &[
            ("o_float", Value::F32(f32::NAN)),
            ("o_double", Value::F64(f64::NEG_INFINITY)),
        ],
    );
    assert_eq!(key(&message, true, "o_float"), "f32:NaN");
    assert_eq!(key(&message, true, "o_double"), "f64:-inf");
    let message = build("vs2.P2", &[("o_float", Value::F32(1.5))]);
    assert_eq!(key(&message, true, "o_float"), "f32:1.5");
}

#[test]
fn proto2_unset_fields_are_null_keys_and_stripped() {
    let message = build("vs2.P2", &[("o_int32", Value::I32(0))]);
    // A present default is written; an absent field with an explicit default reads as that
    // default; every other absent singular field is a null key.
    assert_eq!(key(&message, false, "o_int32"), "i8:0");
    assert_eq!(key(&message, false, "o_default"), "i8:5");
    assert_eq!(key(&message, false, "o_string"), "null");
    assert_eq!(key(&message, false, "child"), "null");
    assert_eq!(key(&message, false, "c_str"), "null");
    assert_eq!(key(&message, true, "o_string"), "absent");
    assert_eq!(
        stripped(&message),
        "{by_id: {}, children: [], colors: [], labels: {}, o_default: i8:5, o_int32: i8:0, \
         r_int32: []}"
    );
}

#[test]
fn proto3_defaults_are_written_out() {
    let message = build("vs3.P3", &[]);
    // Implicit-presence scalars read as their zero; `optional` and message fields stay null.
    assert_eq!(
        encode_with(&message, false),
        r#"{b: false, f: f32:0, i: i8:0, leaf: null, mode: "MODE_UNSET", opt: null, raw: bin:, s: "", tags: []}"#
    );
    assert_eq!(
        stripped(&message),
        r#"{b: false, f: f32:0, i: i8:0, mode: "MODE_UNSET", raw: bin:, s: "", tags: []}"#
    );
}

#[test]
fn repeated_and_map_fields_are_always_written() {
    assert_eq!(
        stripped(&build("vs2.P2", &[])),
        format!("{{{ALWAYS_PRESENT}}}")
    );
}

#[test]
fn maps_are_objects_with_string_keys() {
    let mut labels = HashMap::new();
    labels.insert(MapKey::String("a".into()), Value::I64(1));
    let mut by_id = HashMap::new();
    by_id.insert(MapKey::U32(7), leaf("x"));
    by_id.insert(MapKey::U32(u32::MAX), leaf("y"));
    let message = build(
        "vs2.P2",
        &[("labels", Value::Map(labels)), ("by_id", Value::Map(by_id))],
    );
    assert_eq!(key(&message, true, "labels"), "{a: i8:1}");
    assert_eq!(
        key(&message, true, "by_id"),
        r#"{-1: {name: "y"}, 7: {name: "x"}}"#
    );
}

#[test]
fn enums_are_names_and_unknown_numbers_follow_protobuf_java() {
    let message = build(
        "vs2.P2",
        &[
            ("color", Value::EnumNumber(2)),
            (
                "colors",
                Value::List(vec![
                    Value::EnumNumber(1),
                    Value::EnumNumber(9),
                    Value::EnumNumber(2),
                ]),
            ),
        ],
    );
    assert_eq!(key(&message, true, "color"), r#""BLUE""#);
    // A closed (proto2) enum keeps unknown numbers as unknown fields.
    assert_eq!(key(&message, true, "colors"), r#"["RED", "BLUE"]"#);
    let unknown = build("vs2.P2", &[("color", Value::EnumNumber(9))]);
    assert_eq!(key(&unknown, false, "color"), "null");
    // An open (proto3) enum names them.
    let open = build("vs3.P3", &[("mode", Value::EnumNumber(7))]);
    assert_eq!(key(&open, true, "mode"), r#""UNKNOWN_ENUM_VALUE_Mode_7""#);
}

#[test]
fn messages_with_no_fields_are_dropped() {
    let empty = || Value::Message(build("vs2.Empty", &[]));
    let mut empty_map = HashMap::new();
    empty_map.insert(MapKey::String("k".into()), empty());
    let hollow = build("vs2.Hollow", &[("e", empty())]);
    let message = build(
        "vs2.P2",
        &[
            ("empty", empty()),
            ("empties", Value::List(vec![empty()])),
            ("empty_map", Value::Map(empty_map)),
            ("hollow", Value::Message(hollow)),
        ],
    );
    for name in ["empty", "empties", "empty_map", "hollow"] {
        assert_eq!(key(&message, false, name), "absent", "{name}");
    }
}

#[test]
fn any_is_a_type_url_and_value_object() {
    let any = build(
        "google.protobuf.Any",
        &[
            ("type_url", Value::String("type.googleapis.com/x.Y".into())),
            ("value", Value::Bytes(Bytes::from_static(b"\x08\x01"))),
        ],
    );
    let message = build("vs2.P2", &[("any", Value::Message(any))]);
    assert_eq!(
        key(&message, true, "any"),
        r#"{type_url: "type.googleapis.com/x.Y", value: bin:0801}"#
    );
}

#[test]
fn nested_null_keys_are_stripped_at_every_depth() {
    let message = build(
        "vs2.P2",
        &[
            ("c_leaf", leaf("x")),
            ("children", Value::List(vec![leaf("y")])),
        ],
    );
    assert_eq!(key(&message, false, "c_leaf"), r#"{n: null, name: "x"}"#);
    assert_eq!(key(&message, true, "c_leaf"), r#"{name: "x"}"#);
    assert_eq!(key(&message, true, "children"), r#"[{name: "y"}]"#);
    // The other oneof member is unset.
    assert_eq!(key(&message, false, "c_str"), "null");
}

#[test]
fn malformed_bytes_are_an_error() {
    let encoder = SparkVariantEncoder::new(&descriptor("vs2.P2"), true);
    // Field 10 (string) declares 5 bytes and carries 1.
    assert!(encoder.encode(&[(10 << 3) | 2, 5, b'a']).is_err());
}

#[test]
fn the_plan_routes_spark_encodings_and_keeps_proto_json_by_default() {
    let message = build(
        "vs2.P2",
        &[
            ("o_bytes", Value::Bytes(Bytes::from_static(b"\x01"))),
            ("o_int64", Value::I64(5)),
            ("c_leaf", leaf("x")),
        ],
    );
    let bytes = message.encode_to_vec();
    let wire = WireValue::Len(&bytes);
    let desc = descriptor("vs2.P2");
    let render_plan = |plan: VariantMessagePlan| {
        let (metadata, value) = plan.encode(&wire).expect("encode");
        render(&Variant::new(&metadata, &value))
    };
    let json = render_plan(VariantMessagePlan::new(&desc));
    assert_eq!(
        render_plan(VariantMessagePlan::with_encoding(
            &desc,
            VariantMessageEncoding::ProtoJson
        )),
        json
    );
    assert_eq!(
        json,
        r#"{c_leaf: {name: "x"}, o_bytes: "AQ==", o_int64: i8:5}"#
    );
    assert_eq!(
        render_plan(VariantMessagePlan::with_encoding(
            &desc,
            VariantMessageEncoding::SparkStripNulls
        )),
        stripped(&message)
    );
    assert_eq!(
        render_plan(VariantMessagePlan::with_encoding(
            &desc,
            VariantMessageEncoding::Spark
        )),
        encode_with(&message, false)
    );
}

/// An `id` INT column and a `payload` VARIANT column.
fn id_and_payload_schema() -> arrow::datatypes::Schema {
    use arrow::datatypes::{DataType, Field, Fields, Schema};

    let variant_field = Field::new(
        "payload",
        DataType::Struct(Fields::from(vec![
            Field::new("metadata", DataType::LargeBinary, false),
            Field::new("value", DataType::LargeBinary, false),
        ])),
        true,
    )
    .with_metadata(HashMap::from([(
        "ARROW:extension:name".to_string(),
        "arrow.parquet.variant".to_string(),
    )]));
    Schema::new(vec![Field::new("id", DataType::Int32, true), variant_field])
}

#[test]
fn the_encoder_applies_the_encoding_to_variant_columns() {
    use arrow::array::{Array, AsArray, LargeBinaryArray};

    let schema = id_and_payload_schema();
    let payload = build(
        "vs2.P2",
        &[("o_bytes", Value::Bytes(Bytes::from_static(b"\x01")))],
    );
    let envelope = build(
        "vs2.Envelope",
        &[("id", Value::I32(1)), ("payload", Value::Message(payload))],
    );
    let encoder = WireToArrowEncoder::with_variant_message_encoding(
        &descriptor("vs2.Envelope"),
        schema,
        VariantMessageEncoding::SparkStripNulls,
    )
    .expect("plan");
    let batch = encoder
        .encode_batch(&[Bytes::from(envelope.encode_to_vec())])
        .expect("batch");
    let column = batch.column(1).as_struct();
    assert!(!column.is_null(0));
    let part = |index: usize| {
        column
            .column(index)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .expect("binary")
            .value(0)
            .to_vec()
    };
    let (metadata, value) = (part(0), part(1));
    assert_eq!(
        render(&Variant::new(&metadata, &value)),
        "{by_id: {}, children: [], colors: [], labels: {}, o_bytes: bin:01, o_default: i8:5, \
         r_int32: []}"
    );
}

#[test]
fn the_config_defaults_to_proto_json() {
    let config: WireToArrowSerializerConfig =
        serde_json::from_str(r#"{"desc_file": "/x.desc", "message_type": "x.Y"}"#).expect("parse");
    assert_eq!(
        config.variant_message_encoding,
        VariantMessageEncoding::ProtoJson
    );
    for (text, expected) in [
        ("proto_json", VariantMessageEncoding::ProtoJson),
        ("spark", VariantMessageEncoding::Spark),
        ("spark_strip_nulls", VariantMessageEncoding::SparkStripNulls),
    ] {
        let config: WireToArrowSerializerConfig = serde_json::from_str(&format!(
            r#"{{"desc_file": "/x.desc", "message_type": "x.Y", "variant_message_encoding": "{text}"}}"#
        ))
        .expect("parse");
        assert_eq!(config.variant_message_encoding, expected);
    }
}

/// proto2 `ce2` importing proto3 `ce3`: closed and imported-open enums in singular, repeated,
/// packed, oneof, map, nested and group fields, plus `required` fields (`ce2.Req`) and an
/// envelope carrying `ce2.Req` payloads in each of those positions (`ce2.ReqEnvelope`).
fn closed_enum_pool() -> DescriptorPool {
    use Label::{Optional, Repeated, Required};
    use ProtoType as T;
    let packed = |field: FieldDescriptorProto| FieldDescriptorProto {
        options: Some(FieldOptions {
            packed: Some(true),
            ..Default::default()
        }),
        ..field
    };
    let in_oneof = |field: FieldDescriptorProto| FieldDescriptorProto {
        oneof_index: Some(0),
        ..field
    };
    let holder = DescriptorProto {
        nested_type: vec![
            map_entry(
                "ByNameEntry",
                T::String,
                typed("value", 2, Optional, T::Enum, ".ce2.Color"),
            ),
            message("G", vec![typed("c", 1, Optional, T::Enum, ".ce2.Color")]),
        ],
        oneof_decl: vec![OneofDescriptorProto {
            name: Some("pick".into()),
            ..Default::default()
        }],
        ..message(
            "Holder",
            vec![
                typed("color", 1, Optional, T::Enum, ".ce2.Color"),
                typed("colors", 2, Repeated, T::Enum, ".ce2.Color"),
                packed(typed("packed_colors", 3, Repeated, T::Enum, ".ce2.Color")),
                in_oneof(field("label", 4, Optional, T::String)),
                in_oneof(typed("pick_color", 5, Optional, T::Enum, ".ce2.Color")),
                typed("mode", 6, Optional, T::Enum, ".ce3.Mode"),
                typed("modes", 7, Repeated, T::Enum, ".ce3.Mode"),
                typed(
                    "by_name",
                    8,
                    Repeated,
                    T::Message,
                    ".ce2.Holder.ByNameEntry",
                ),
                typed("inner", 9, Optional, T::Message, ".ce2.Inner"),
                typed("g", 10, Optional, T::Group, ".ce2.Holder.G"),
                typed("open", 11, Optional, T::Message, ".ce3.Open"),
            ],
        )
    };
    let req = DescriptorProto {
        nested_type: vec![map_entry(
            "ByKeyEntry",
            T::String,
            typed("value", 2, Optional, T::Message, ".ce2.Child"),
        )],
        ..message(
            "Req",
            vec![
                field("id", 1, Required, T::Int32),
                typed("child", 2, Optional, T::Message, ".ce2.Child"),
                typed("children", 3, Repeated, T::Message, ".ce2.Child"),
                typed("by_key", 4, Repeated, T::Message, ".ce2.Req.ByKeyEntry"),
            ],
        )
    };
    let req_envelope = DescriptorProto {
        nested_type: vec![map_entry(
            "ByKeyEntry",
            T::String,
            typed("value", 2, Optional, T::Message, ".ce2.Req"),
        )],
        oneof_decl: vec![OneofDescriptorProto {
            name: Some("pick".into()),
            ..Default::default()
        }],
        ..message(
            "ReqEnvelope",
            vec![
                field("id", 1, Optional, T::Int32),
                typed("payload", 2, Optional, T::Message, ".ce2.Req"),
                typed("payloads", 3, Repeated, T::Message, ".ce2.Req"),
                in_oneof(typed("picked", 4, Optional, T::Message, ".ce2.Req")),
                in_oneof(field("label", 5, Optional, T::String)),
                typed("holder", 6, Optional, T::Message, ".ce2.ReqHolder"),
                typed(
                    "by_key",
                    7,
                    Repeated,
                    T::Message,
                    ".ce2.ReqEnvelope.ByKeyEntry",
                ),
            ],
        )
    };
    let ce2 = FileDescriptorProto {
        name: Some("ce2.proto".into()),
        package: Some("ce2".into()),
        syntax: Some("proto2".into()),
        dependency: vec!["ce3.proto".into()],
        message_type: vec![
            holder,
            message(
                "Inner",
                vec![typed("color", 1, Optional, T::Enum, ".ce2.Color")],
            ),
            req,
            message("Child", vec![field("name", 1, Required, T::String)]),
            req_envelope,
            message(
                "ReqHolder",
                vec![typed("payload", 1, Optional, T::Message, ".ce2.Req")],
            ),
        ],
        enum_type: vec![enum_type("Color", &[("RED", 1), ("BLUE", 2)])],
        ..Default::default()
    };
    let ce3 = FileDescriptorProto {
        name: Some("ce3.proto".into()),
        package: Some("ce3".into()),
        syntax: Some("proto3".into()),
        message_type: vec![message(
            "Open",
            vec![
                typed("mode", 1, Optional, T::Enum, ".ce3.Mode"),
                typed("modes", 2, Repeated, T::Enum, ".ce3.Mode"),
            ],
        )],
        enum_type: vec![enum_type("Mode", &[("MODE_UNSET", 0), ("MODE_ON", 1)])],
        ..Default::default()
    };
    let set = FileDescriptorSet {
        file: vec![ce3, ce2],
    };
    DescriptorPool::decode(set.encode_to_vec().as_slice()).expect("closed-enum descriptors")
}

/// Encodes raw wire `bytes` of `name` from [`closed_enum_pool`].
fn encode_raw(name: &str, bytes: &[u8], strip_nulls: bool) -> Result<String, WireToArrowError> {
    let descriptor = closed_enum_pool().get_message_by_name(name).expect(name);
    let (metadata, value) = SparkVariantEncoder::new(&descriptor, strip_nulls).encode(bytes)?;
    Ok(render(&Variant::new(&metadata, &value)))
}

/// The rendered value of one top-level key of a `ce2.Holder` (`absent` when not stored).
fn holder_key(bytes: &[u8], strip_nulls: bool, name: &str) -> String {
    let descriptor = closed_enum_pool()
        .get_message_by_name("ce2.Holder")
        .expect("holder");
    let (metadata, value) = SparkVariantEncoder::new(&descriptor, strip_nulls)
        .encode(bytes)
        .expect("encode");
    let variant = Variant::new(&metadata, &value);
    variant
        .as_object()
        .expect("object")
        .get(name)
        .map_or_else(|| "absent".into(), |value| render(&value))
}

/// A length-delimited field: tag, length and `body` (field numbers below 16, bodies below 128).
fn len_field(number: u8, body: &[u8]) -> Vec<u8> {
    let mut bytes = vec![
        (number << 3) | 2,
        u8::try_from(body.len()).expect("short body"),
    ];
    bytes.extend_from_slice(body);
    bytes
}

/// A varint field with a one-byte value (field numbers below 16).
fn varint_field(number: u8, value: u8) -> [u8; 2] {
    [number << 3, value]
}

#[test]
fn a_known_closed_enum_value_survives_a_later_unknown_number() {
    let bytes = [varint_field(1, 1), varint_field(1, 9)].concat();
    assert_eq!(holder_key(&bytes, true, "color"), r#""RED""#);
    let bytes = [varint_field(1, 2), varint_field(1, 9), varint_field(1, 1)].concat();
    assert_eq!(holder_key(&bytes, true, "color"), r#""RED""#);
    assert_eq!(holder_key(&varint_field(1, 9), false, "color"), "null");
    // In a nested message too, and a message left with no known fields is still present.
    let inner = len_field(9, &[varint_field(1, 2), varint_field(1, 9)].concat());
    assert_eq!(holder_key(&inner, true, "inner"), r#"{color: "BLUE"}"#);
    let inner = len_field(9, &varint_field(1, 9));
    assert_eq!(holder_key(&inner, false, "inner"), "{color: null}");
    // A group body is filtered the same way.
    let group = [
        &[(10 << 3) | 3][..],
        &varint_field(1, 1),
        &varint_field(1, 9),
        &[(10 << 3) | 4],
    ]
    .concat();
    assert_eq!(holder_key(&group, true, "g"), r#"{c: "RED"}"#);
}

#[test]
fn an_unknown_closed_enum_number_does_not_switch_the_oneof() {
    let bytes = [len_field(4, b"a"), varint_field(5, 9).to_vec()].concat();
    assert_eq!(holder_key(&bytes, true, "label"), r#""a""#);
    assert_eq!(holder_key(&bytes, false, "pick_color"), "null");
    let bytes = [varint_field(5, 2), varint_field(5, 9)].concat();
    assert_eq!(holder_key(&bytes, true, "pick_color"), r#""BLUE""#);
    assert_eq!(holder_key(&bytes, false, "label"), "null");
}

#[test]
fn repeated_closed_enums_drop_unknown_elements_packed_or_not() {
    let bytes = [varint_field(2, 1), varint_field(2, 9), varint_field(2, 2)].concat();
    assert_eq!(holder_key(&bytes, true, "colors"), r#"["RED", "BLUE"]"#);
    let packed = len_field(3, &[1, 9, 2]);
    assert_eq!(
        holder_key(&packed, true, "packed_colors"),
        r#"["RED", "BLUE"]"#
    );
    assert_eq!(holder_key(&len_field(3, &[9]), true, "packed_colors"), "[]");
    // A packed run of a field declared unpacked is read the same way.
    assert_eq!(
        holder_key(&len_field(2, &[9, 1]), true, "colors"),
        r#"["RED"]"#
    );
}

#[test]
fn a_map_entry_with_an_unknown_closed_enum_keeps_its_known_value() {
    let entry = |values: &[[u8; 2]]| {
        let mut body = len_field(1, b"k");
        for value in values {
            body.extend_from_slice(value);
        }
        len_field(8, &body)
    };
    assert_eq!(
        holder_key(
            &entry(&[varint_field(2, 2), varint_field(2, 9)]),
            true,
            "by_name"
        ),
        r#"{k: "BLUE"}"#
    );
    // An entry whose only value is unknown reads the value as unset: the enum's default.
    assert_eq!(
        holder_key(&entry(&[varint_field(2, 9)]), true, "by_name"),
        r#"{k: "RED"}"#
    );
}

#[test]
fn a_proto2_field_of_an_imported_proto3_enum_is_closed() {
    assert_eq!(holder_key(&varint_field(6, 7), false, "mode"), "null");
    let bytes = [varint_field(6, 1), varint_field(6, 7)].concat();
    assert_eq!(holder_key(&bytes, true, "mode"), r#""MODE_ON""#);
    let bytes = [varint_field(7, 1), varint_field(7, 7), varint_field(7, 0)].concat();
    assert_eq!(
        holder_key(&bytes, true, "modes"),
        r#"["MODE_ON", "MODE_UNSET"]"#
    );
    // The proto3 message's own fields of that enum stay open.
    let open = len_field(11, &[varint_field(1, 7), varint_field(2, 7)].concat());
    assert_eq!(
        holder_key(&open, true, "open"),
        r#"{mode: "UNKNOWN_ENUM_VALUE_Mode_7", modes: ["UNKNOWN_ENUM_VALUE_Mode_7"]}"#
    );
}

#[test]
fn a_missing_required_field_fails_the_decode() {
    let missing = |bytes: &[u8]| match encode_raw("ce2.Req", bytes, true) {
        Err(WireToArrowError::VariantMessageMissingRequiredField { field }) => field,
        other => panic!("expected a missing required field, got {other:?}"),
    };
    let id = varint_field(1, 1);
    assert_eq!(missing(&[]), "ce2.Req.id");
    assert_eq!(
        encode_raw("ce2.Req", &id, true).expect("initialized"),
        "{by_key: {}, children: [], id: i8:1}"
    );
    let named = len_field(1, b"a");
    let child = [id.to_vec(), len_field(2, &named)].concat();
    assert_eq!(
        encode_raw("ce2.Req", &child, true).expect("initialized"),
        r#"{by_key: {}, child: {name: "a"}, children: [], id: i8:1}"#
    );
    // Inside a present child, a repeated child, and a map value (an entry without a value reads
    // it as the default instance, which lacks it too).
    assert_eq!(
        missing(&[id.to_vec(), len_field(2, &[])].concat()),
        "ce2.Child.name"
    );
    assert_eq!(
        missing(&[id.to_vec(), len_field(3, &named), len_field(3, &[])].concat()),
        "ce2.Child.name"
    );
    let entry_without_name = [len_field(1, b"k"), len_field(2, &[])].concat();
    assert_eq!(
        missing(&[id.to_vec(), len_field(4, &entry_without_name)].concat()),
        "ce2.Child.name"
    );
    assert_eq!(
        missing(&[id.to_vec(), len_field(4, &len_field(1, b"k"))].concat()),
        "ce2.Child.name"
    );
}

#[test]
fn a_row_missing_a_required_payload_field_is_dropped() {
    use arrow::array::{Array, Int32Array};

    let schema = id_and_payload_schema();
    let descriptor = closed_enum_pool()
        .get_message_by_name("ce2.ReqEnvelope")
        .expect("envelope");
    let encoder = WireToArrowEncoder::with_variant_message_encoding(
        &descriptor,
        schema,
        VariantMessageEncoding::SparkStripNulls,
    )
    .expect("plan");
    let row = |id: u8, payload: &[u8]| {
        Bytes::from([varint_field(1, id).to_vec(), len_field(2, payload)].concat())
    };
    let batch = encoder
        .encode_batch(&[
            row(1, &varint_field(1, 7)),
            row(2, &[]),
            row(
                3,
                &[varint_field(1, 8).to_vec(), len_field(2, &[])].concat(),
            ),
        ])
        .expect("batch");
    assert_eq!(batch.num_rows(), 1);
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("ids");
    assert_eq!(ids.value(0), 1);
}

#[test]
fn a_nested_required_field_is_checked_after_merging_its_occurrences() {
    let id = varint_field(1, 1).to_vec();
    let named = len_field(1, b"a");
    // A singular child, and a map entry's value, merge their occurrences in either order.
    for children in [
        [len_field(2, &[]), len_field(2, &named)],
        [len_field(2, &named), len_field(2, &[])],
    ] {
        assert_eq!(
            encode_raw("ce2.Req", &[id.clone(), children.concat()].concat(), true)
                .expect("initialized"),
            r#"{by_key: {}, child: {name: "a"}, children: [], id: i8:1}"#
        );
    }
    let entry = [len_field(1, b"k"), len_field(2, &[]), len_field(2, &named)].concat();
    assert_eq!(
        encode_raw("ce2.Req", &[id, len_field(4, &entry)].concat(), true).expect("initialized"),
        r#"{by_key: {k: {name: "a"}}, children: [], id: i8:1}"#
    );
}

/// The `ce2.ReqEnvelope` columns: `ce2.Req` as a top-level, repeated, oneof, nested and map-value
/// VARIANT.
fn req_envelope_schema() -> arrow::datatypes::Schema {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Fields, Schema};

    let variant = |name: &str| {
        Field::new(
            name,
            DataType::Struct(Fields::from(vec![
                Field::new("metadata", DataType::LargeBinary, false),
                Field::new("value", DataType::LargeBinary, false),
            ])),
            true,
        )
        .with_metadata(HashMap::from([(
            "ARROW:extension:name".to_string(),
            "arrow.parquet.variant".to_string(),
        )]))
    };
    let entry = Field::new(
        "key_value",
        DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::LargeUtf8, false),
            variant("value"),
        ])),
        false,
    );
    Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        variant("payload"),
        Field::new("payloads", DataType::List(Arc::new(variant("item"))), true),
        variant("picked"),
        Field::new("label", DataType::LargeUtf8, true),
        Field::new(
            "holder",
            DataType::Struct(Fields::from(vec![variant("payload")])),
            true,
        ),
        Field::new("by_key", DataType::Map(Arc::new(entry), false), true),
    ])
}

#[test]
fn a_required_payload_field_is_checked_after_merging_the_rows_occurrences() {
    use arrow::array::{Array, AsArray, Int32Array, LargeBinaryArray, MapArray, StructArray};

    let descriptor = closed_enum_pool()
        .get_message_by_name("ce2.ReqEnvelope")
        .expect("envelope");
    let req7 = varint_field(1, 7);
    let row = |id: u8, fields: &[Vec<u8>]| {
        Bytes::from([varint_field(1, id).to_vec(), fields.concat()].concat())
    };
    let payload = |body: &[u8]| len_field(2, body);
    let picked = |body: &[u8]| len_field(4, body);
    // `ce2.ReqHolder.payload` and a map entry's `value` are fields 1 and 2.
    let holder = |bodies: &[&[u8]]| {
        let fields: Vec<_> = bodies.iter().map(|body| len_field(1, body)).collect();
        len_field(6, &fields.concat())
    };
    let entry = |bodies: &[&[u8]]| {
        let values: Vec<_> = bodies.iter().map(|body| len_field(2, body)).collect();
        len_field(7, &[len_field(1, b"k"), values.concat()].concat())
    };
    let rows = [
        // A singular payload, empty then complete (`08 01 12 00 12 02 08 07`) and the reverse.
        row(1, &[payload(&[]), payload(&req7)]),
        row(2, &[payload(&req7), payload(&[])]),
        // Genuinely incomplete: one empty payload, or two.
        row(3, &[payload(&[])]),
        row(4, &[payload(&[]), payload(&[])]),
        // Inside a nested message, across its occurrences or within one.
        row(5, &[holder(&[&[]]), holder(&[&req7])]),
        row(6, &[holder(&[&[], &req7])]),
        // A oneof member merges with itself, but a switch away and back starts it afresh
        // (`DynamicMessage.Builder.setField` clears the other member).
        row(7, &[picked(&[]), len_field(5, b"x"), picked(&req7)]),
        row(8, &[picked(&req7), len_field(5, b"x"), picked(&[])]),
        row(9, &[picked(&[]), picked(&req7)]),
        // Repeated elements never merge, so an empty one fails.
        row(10, &[len_field(3, &[]), len_field(3, &req7)]),
        // A map entry merges its own value occurrences, but separate entries of one key are
        // separate repeated elements, each checked.
        row(11, &[entry(&[&[], &req7])]),
        row(12, &[entry(&[&[]]), entry(&[&req7])]),
        // A valid row after the incomplete ones.
        row(13, &[payload(&req7)]),
    ];
    let render_at = |column: &StructArray, index: usize| {
        assert!(column.is_valid(index), "a null VARIANT at {index}");
        let part = |child: usize| {
            column
                .column(child)
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .expect("variant part")
                .value(index)
                .to_vec()
        };
        render(&Variant::new(&part(0), &part(1)))
    };
    let req7_value = "{by_key: {}, children: [], id: i8:7}";

    for encoding in [
        VariantMessageEncoding::Spark,
        VariantMessageEncoding::SparkStripNulls,
    ] {
        let encoder = WireToArrowEncoder::with_variant_message_encoding(
            &descriptor,
            req_envelope_schema(),
            encoding,
        )
        .expect("plan");
        let batch = encoder.encode_batch(&rows).expect("batch");
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("ids");
        assert_eq!(
            ids.values().to_vec(),
            [1, 2, 5, 6, 7, 9, 11, 13],
            "{encoding:?}"
        );
        if encoding != VariantMessageEncoding::SparkStripNulls {
            continue;
        }
        let top_level = batch.column(1).as_struct();
        for index in [0, 1, 7] {
            assert_eq!(render_at(top_level, index), req7_value);
        }
        let holders = batch.column(5).as_struct().column(0).as_struct();
        for index in [2, 3] {
            assert_eq!(render_at(holders, index), req7_value);
        }
        let picks = batch.column(3).as_struct();
        for index in [4, 5] {
            assert_eq!(render_at(picks, index), req7_value);
        }
        let maps = batch
            .column(6)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("map");
        assert_eq!(maps.value_length(6), 1);
        let entries = maps.value(6);
        assert_eq!(render_at(entries.column(1).as_struct(), 0), req7_value);
    }
}

/// `ce2.ReqEnvelope` rows: `id`, then each body as a `payload` occurrence.
fn req_envelope_row(id: u8, payloads: &[&[u8]]) -> Bytes {
    let payloads: Vec<_> = payloads.iter().map(|body| len_field(2, body)).collect();
    Bytes::from([varint_field(1, id).to_vec(), payloads.concat()].concat())
}

fn encoder_with_policy(
    on_wire_type_mismatch: crate::encoding::format::wire_to_arrow::OnWireTypeMismatch,
) -> WireToArrowEncoder {
    let descriptor = closed_enum_pool()
        .get_message_by_name("ce2.ReqEnvelope")
        .expect("envelope");
    WireToArrowEncoder::from_parts(
        &descriptor,
        id_and_payload_schema(),
        on_wire_type_mismatch,
        &[],
        VariantMessageEncoding::SparkStripNulls,
    )
    .expect("plan")
}

#[test]
fn a_wire_type_mismatch_inside_the_payload_follows_the_mismatch_policy() {
    use arrow::array::{Array, AsArray, Int32Array, LargeBinaryArray};

    use crate::encoding::format::wire_to_arrow::OnWireTypeMismatch;

    // `id` sent as LEN is an unknown field to protobuf-java, so a later varint `id` still
    // satisfies `required`; with no well-typed `id` the payload fails its decode.
    let mismatched_id = len_field(1, b"x");
    let rows = [
        req_envelope_row(
            1,
            &[&[mismatched_id.clone(), varint_field(1, 7).to_vec()].concat()],
        ),
        req_envelope_row(2, &[&mismatched_id]),
    ];

    let batch = encoder_with_policy(OnWireTypeMismatch::SkipField)
        .encode_batch(&rows)
        .expect("batch");
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("ids");
    assert_eq!(ids.values().to_vec(), [1]);
    let column = batch.column(1).as_struct();
    let part = |index: usize| {
        column
            .column(index)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .expect("binary")
            .value(0)
            .to_vec()
    };
    assert_eq!(
        render(&Variant::new(&part(0), &part(1))),
        "{by_key: {}, children: [], id: i8:7}"
    );

    let batch = encoder_with_policy(OnWireTypeMismatch::DropRow)
        .encode_batch(&rows)
        .expect("batch");
    assert_eq!(batch.num_rows(), 0);
}

/// A payload whose only `id` is mismatched fails `required`, and the row is retried with its
/// payload occurrences merged; the retry re-records the skip, so it counts once.
#[test]
fn a_skip_in_a_payload_retried_for_a_required_field_counts_once() {
    use vector_core::metrics::{Controller, init_test};

    use crate::encoding::format::wire_to_arrow::OnWireTypeMismatch;
    use crate::encoding::format::wire_to_arrow::tests::{METRICS_TEST_LOCK, counter_total};

    let _guard = METRICS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
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

    let row = req_envelope_row(1, &[&len_field(1, b"x"), &varint_field(1, 7)]);
    let batch = encoder_with_policy(OnWireTypeMismatch::SkipField)
        .encode_batch(&[row])
        .expect("batch");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(skipped() - before, 1.0);
}
