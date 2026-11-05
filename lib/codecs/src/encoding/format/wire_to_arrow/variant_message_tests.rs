//! Parity of the direct wire → Variant walk with the reference
//! `DynamicMessage` → JSON → Variant path.

use parquet_variant::Variant;
use proptest::prelude::*;
use prost_reflect::prost::Message as _;
use prost_reflect::prost_types::field_descriptor_proto::{Label, Type as ProtoType};
use prost_reflect::prost_types::{
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FieldOptions, FileDescriptorProto, FileDescriptorSet, MessageOptions, OneofDescriptorProto,
    descriptor_proto::ExtensionRange,
};
use prost_reflect::{DescriptorPool, DynamicMessage, Kind, MapKey, MessageDescriptor, Value};
use zeroparser::wire::WireValue;

use super::VariantMessagePlan;
use crate::encoding::format::wire_to_arrow::append::encode_variant_from_message;

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

fn packed(mut f: FieldDescriptorProto, packed: bool) -> FieldDescriptorProto {
    f.options = Some(FieldOptions {
        packed: Some(packed),
        ..Default::default()
    });
    f
}

const SCALARS: [(&str, ProtoType); 15] = [
    ("int32", ProtoType::Int32),
    ("int64", ProtoType::Int64),
    ("uint32", ProtoType::Uint32),
    ("uint64", ProtoType::Uint64),
    ("sint32", ProtoType::Sint32),
    ("sint64", ProtoType::Sint64),
    ("fixed32", ProtoType::Fixed32),
    ("fixed64", ProtoType::Fixed64),
    ("sfixed32", ProtoType::Sfixed32),
    ("sfixed64", ProtoType::Sfixed64),
    ("float", ProtoType::Float),
    ("double", ProtoType::Double),
    ("bool", ProtoType::Bool),
    ("string", ProtoType::String),
    ("bytes", ProtoType::Bytes),
];

/// proto2 `vm.Rich` (every scalar singular / repeated / packed, enums, nested
/// and self-referential messages, a oneof, a map, a well-known type and an
/// extension) and proto3 `vm3.Lean` (implicit presence, `optional`).
fn pool() -> DescriptorPool {
    let timestamp = FileDescriptorProto {
        name: Some("google/protobuf/timestamp.proto".into()),
        package: Some("google.protobuf".into()),
        syntax: Some("proto3".into()),
        message_type: vec![DescriptorProto {
            name: Some("Timestamp".into()),
            field: vec![
                field("seconds", 1, Label::Optional, ProtoType::Int64),
                field("nanos", 2, Label::Optional, ProtoType::Int32),
            ],
            ..Default::default()
        }],
        ..Default::default()
    };

    let mut rich_fields = Vec::new();
    for (i, (name, ty)) in SCALARS.iter().enumerate() {
        let i = i as i32;
        rich_fields.push(field(&format!("o_{name}"), 1 + i, Label::Optional, *ty));
        rich_fields.push(packed(
            field(&format!("r_{name}"), 21 + i, Label::Repeated, *ty),
            false,
        ));
        if !matches!(ty, ProtoType::String | ProtoType::Bytes) {
            rich_fields.push(packed(
                field(&format!("p_{name}"), 41 + i, Label::Repeated, *ty),
                true,
            ));
        }
    }
    rich_fields.extend([
        typed("color", 60, Label::Optional, ProtoType::Enum, ".vm.Color"),
        packed(
            typed("colors", 61, Label::Repeated, ProtoType::Enum, ".vm.Color"),
            false,
        ),
        packed(
            typed(
                "colors_packed",
                62,
                Label::Repeated,
                ProtoType::Enum,
                ".vm.Color",
            ),
            true,
        ),
        typed("child", 70, Label::Optional, ProtoType::Message, ".vm.Rich"),
        typed(
            "children",
            71,
            Label::Repeated,
            ProtoType::Message,
            ".vm.Rich",
        ),
        FieldDescriptorProto {
            oneof_index: Some(0),
            ..field("c_str", 80, Label::Optional, ProtoType::String)
        },
        FieldDescriptorProto {
            oneof_index: Some(0),
            ..field("c_int", 81, Label::Optional, ProtoType::Int32)
        },
        FieldDescriptorProto {
            oneof_index: Some(0),
            ..typed("c_msg", 82, Label::Optional, ProtoType::Message, ".vm.Leaf")
        },
        typed(
            "labels",
            90,
            Label::Repeated,
            ProtoType::Message,
            ".vm.Rich.LabelsEntry",
        ),
        typed(
            "ts",
            91,
            Label::Optional,
            ProtoType::Message,
            ".google.protobuf.Timestamp",
        ),
        typed("leaf", 92, Label::Optional, ProtoType::Message, ".vm.Leaf"),
    ]);
    let vm = FileDescriptorProto {
        name: Some("vm.proto".into()),
        package: Some("vm".into()),
        dependency: vec!["google/protobuf/timestamp.proto".into()],
        enum_type: vec![EnumDescriptorProto {
            name: Some("Color".into()),
            value: [("RED", 0), ("GREEN", 1), ("BLUE", 5)]
                .iter()
                .map(|(n, v)| EnumValueDescriptorProto {
                    name: Some((*n).into()),
                    number: Some(*v),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        message_type: vec![
            DescriptorProto {
                name: Some("Rich".into()),
                field: rich_fields,
                oneof_decl: vec![OneofDescriptorProto {
                    name: Some("choice".into()),
                    ..Default::default()
                }],
                nested_type: vec![DescriptorProto {
                    name: Some("LabelsEntry".into()),
                    field: vec![
                        field("key", 1, Label::Optional, ProtoType::String),
                        field("value", 2, Label::Optional, ProtoType::Int32),
                    ],
                    options: Some(MessageOptions {
                        map_entry: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                extension_range: vec![ExtensionRange {
                    start: Some(1000),
                    end: Some(1001),
                    ..Default::default()
                }],
                ..Default::default()
            },
            DescriptorProto {
                name: Some("Leaf".into()),
                field: vec![
                    field("s", 1, Label::Optional, ProtoType::String),
                    field("n", 2, Label::Optional, ProtoType::Int64),
                ],
                ..Default::default()
            },
        ],
        extension: vec![FieldDescriptorProto {
            extendee: Some(".vm.Rich".into()),
            ..field("ext", 1000, Label::Optional, ProtoType::Int32)
        }],
        ..Default::default()
    };

    let vm3 = FileDescriptorProto {
        name: Some("vm3.proto".into()),
        package: Some("vm3".into()),
        syntax: Some("proto3".into()),
        enum_type: vec![EnumDescriptorProto {
            name: Some("Mood".into()),
            value: [("CALM", 0), ("ANGRY", 3)]
                .iter()
                .map(|(n, v)| EnumValueDescriptorProto {
                    name: Some((*n).into()),
                    number: Some(*v),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        message_type: vec![DescriptorProto {
            name: Some("Lean".into()),
            field: vec![
                field("i", 1, Label::Optional, ProtoType::Int32),
                field("s", 2, Label::Optional, ProtoType::String),
                field("b", 3, Label::Optional, ProtoType::Bytes),
                field("d", 4, Label::Optional, ProtoType::Double),
                field("f", 5, Label::Optional, ProtoType::Float),
                field("t", 6, Label::Optional, ProtoType::Bool),
                typed("m", 7, Label::Optional, ProtoType::Enum, ".vm3.Mood"),
                field("r", 8, Label::Repeated, ProtoType::Int64),
                FieldDescriptorProto {
                    oneof_index: Some(0),
                    proto3_optional: Some(true),
                    ..field("oi", 9, Label::Optional, ProtoType::Int32)
                },
                typed("next", 10, Label::Optional, ProtoType::Message, ".vm3.Lean"),
                field("u", 11, Label::Optional, ProtoType::Uint64),
                typed("ms", 12, Label::Repeated, ProtoType::Enum, ".vm3.Mood"),
            ],
            oneof_decl: vec![OneofDescriptorProto {
                name: Some("_oi".into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let set = FileDescriptorSet {
        file: vec![timestamp, vm, vm3],
    };
    DescriptorPool::decode(set.encode_to_vec().as_slice()).unwrap()
}

/// Order-independent rendering that keeps Variant types (`Int8(5)` vs
/// `Int64(5)`, `Double`, short vs long strings), stricter than JSON equality.
fn render(v: &Variant) -> String {
    match v {
        Variant::Object(o) => {
            let mut fields: Vec<String> = o
                .iter()
                .map(|(k, x)| format!("{k:?}:{}", render(&x)))
                .collect();
            fields.sort();
            format!("{{{}}}", fields.join(","))
        }
        Variant::List(l) => {
            let items: Vec<String> = l.iter().map(|x| render(&x)).collect();
            format!("[{}]", items.join(","))
        }
        other => format!("{other:?}"),
    }
}

fn rendered((metadata, value): &(Vec<u8>, Vec<u8>)) -> String {
    render(&Variant::new(metadata, value))
}

/// Deterministic xorshift so failures reproduce from the printed seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn chance(&mut self, one_in: u64) -> bool {
        self.next().is_multiple_of(one_in)
    }
    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[(self.next() % items.len() as u64) as usize]
    }
}

fn random_scalar(kind: &Kind, rng: &mut Rng) -> Value {
    match kind {
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => {
            Value::I32(rng.pick(&[0, 1, -1, 127, 128, -129, 40_000, i32::MAX, i32::MIN]))
        }
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => {
            let random = rng.next() as i64;
            Value::I64(rng.pick(&[0, -1, 200, 1 << 40, i64::MAX, i64::MIN, random]))
        }
        Kind::Uint32 | Kind::Fixed32 => Value::U32(rng.pick(&[0, 1, 255, 65_536, u32::MAX])),
        Kind::Uint64 | Kind::Fixed64 => {
            let random = rng.next();
            Value::U64(rng.pick(&[0, 7, i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX, random]))
        }
        Kind::Float => Value::F32(rng.pick(&[
            0.0,
            -0.0,
            1.5,
            -3.25e10,
            f32::NAN,
            f32::INFINITY,
            f32::NEG_INFINITY,
            f32::MIN_POSITIVE,
            0.1,
        ])),
        Kind::Double => Value::F64(rng.pick(&[
            0.0,
            -0.0,
            2.0,
            1e300,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            0.1,
        ])),
        Kind::Bool => Value::Bool(rng.chance(2)),
        Kind::String => Value::String(
            rng.pick(&[
                "",
                "a",
                "héllo ✓",
                "a string that is comfortably longer than sixty-four bytes in total",
            ])
            .into(),
        ),
        Kind::Bytes => Value::Bytes(rng.pick(&[&b""[..], b"\x00\xff", b"abc"]).to_vec().into()),
        Kind::Enum(e) => {
            // Mostly named values, sometimes a number with no name.
            let numbers: Vec<i32> = e.values().map(|v| v.number()).collect();
            Value::EnumNumber(if rng.chance(5) {
                42
            } else {
                rng.pick(&numbers)
            })
        }
        Kind::Message(_) => unreachable!(),
    }
}

fn random_message(desc: &MessageDescriptor, rng: &mut Rng, depth: usize) -> DynamicMessage {
    let mut msg = DynamicMessage::new(desc.clone());
    for f in desc.fields() {
        if !rng.chance(3) {
            continue;
        }
        // Reference-only shapes stay rare (top level only) so most cases
        // exercise the direct walk; `reference_only_shapes_fall_back` covers them.
        let reference_only = f.is_map()
            || f.kind()
                .as_message()
                .is_some_and(|m| m.full_name().starts_with("google.protobuf."));
        if reference_only && (depth > 0 || !rng.chance(4)) {
            continue;
        }
        let value = |rng: &mut Rng| match f.kind() {
            Kind::Message(m) if depth < 3 => {
                Some(Value::Message(random_message(&m, rng, depth + 1)))
            }
            Kind::Message(_) => None,
            k => Some(random_scalar(&k, rng)),
        };
        if f.is_map() {
            let mut map = std::collections::HashMap::new();
            for _ in 0..(rng.next() % 3) {
                map.insert(
                    MapKey::String(format!("k{}", rng.next() % 5)),
                    Value::I32(rng.pick(&[0, 9])),
                );
            }
            msg.set_field(&f, Value::Map(map));
        } else if f.is_list() {
            let items: Vec<Value> = (0..(rng.next() % 4)).filter_map(|_| value(rng)).collect();
            msg.set_field(&f, Value::List(items));
        } else if let Some(v) = value(rng) {
            msg.set_field(&f, v);
        }
    }
    if let Some(ext) = desc.extensions().next()
        && rng.chance(8)
    {
        msg.set_extension(&ext, Value::I32(7));
    }
    msg
}

/// Wire shapes beyond a single canonical encoding: merged concatenations
/// (repeated singular fields, oneof conflicts) and unknown fields.
fn random_wire(desc: &MessageDescriptor, rng: &mut Rng) -> Vec<u8> {
    let mut wire = random_message(desc, rng, 0).encode_to_vec();
    if rng.chance(4) {
        wire.extend(random_message(desc, rng, 0).encode_to_vec());
    }
    if rng.chance(4) {
        // field 777 varint 300, field 778 length-delimited "zz"
        wire.extend([0xc8, 0x30, 0xac, 0x02, 0xd2, 0x30, 0x02, b'z', b'z']);
    }
    wire
}

fn assert_parity(desc: &MessageDescriptor, plan: &VariantMessagePlan, wire: &[u8]) -> bool {
    let reference = encode_variant_from_message(desc, &WireValue::Len(wire));
    if let Some(direct) = plan.encode_direct(wire) {
        let reference = reference.expect("direct walk accepted input the reference rejects");
        assert_eq!(rendered(&direct), rendered(&reference), "wire {wire:02x?}");
        return true;
    }
    match (plan.encode(&WireValue::Len(wire)), reference) {
        (Ok(a), Ok(b)) => assert_eq!(rendered(&a), rendered(&b)),
        (Err(_), Err(_)) => {}
        (a, b) => panic!("fallback diverged from reference: {a:?} vs {b:?}"),
    }
    false
}

#[test]
fn random_messages_match_reference() {
    let pool = pool();
    for name in ["vm.Rich", "vm3.Lean"] {
        let desc = pool.get_message_by_name(name).unwrap();
        let plan = VariantMessagePlan::new(&desc);
        let mut direct = 0;
        let cases = 4000;
        for seed in 1..=cases as u64 {
            let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
            if assert_parity(&desc, &plan, &random_wire(&desc, &mut rng)) {
                direct += 1;
            }
        }
        // The fallback must stay the exception, or the speedup is gone.
        assert!(
            direct * 3 > cases,
            "{name}: only {direct}/{cases} took the direct walk"
        );
    }
}

#[test]
fn single_messages_take_direct_walk() {
    let pool = pool();
    let desc = pool.get_message_by_name("vm.Leaf").unwrap();
    let plan = VariantMessagePlan::new(&desc);
    let mut msg = DynamicMessage::new(desc.clone());
    msg.set_field_by_name("s", Value::String("x".into()));
    msg.set_field_by_name("n", Value::I64(5));
    let wire = msg.encode_to_vec();
    assert!(assert_parity(&desc, &plan, &wire));
}

#[test]
fn reference_only_shapes_fall_back() {
    let pool = pool();
    let rich = pool.get_message_by_name("vm.Rich").unwrap();
    let plan = VariantMessagePlan::new(&rich);
    let leaf = pool.get_message_by_name("vm.Leaf").unwrap();
    let ts = pool
        .get_message_by_name("google.protobuf.Timestamp")
        .unwrap();

    let mut with_map = DynamicMessage::new(rich.clone());
    with_map.set_field_by_name(
        "labels",
        Value::Map([(MapKey::String("a".into()), Value::I32(1))].into()),
    );
    let mut with_wkt = DynamicMessage::new(rich.clone());
    with_wkt.set_field_by_name("ts", Value::Message(DynamicMessage::new(ts)));
    let mut with_ext = DynamicMessage::new(rich.clone());
    with_ext.set_extension(&rich.extensions().next().unwrap(), Value::I32(3));
    let mut c_str = DynamicMessage::new(rich.clone());
    c_str.set_field_by_name("c_str", Value::String("s".into()));
    let mut c_int = DynamicMessage::new(rich.clone());
    c_int.set_field_by_name("c_int", Value::I32(4));
    let oneof_conflict = [c_str.encode_to_vec(), c_int.encode_to_vec()].concat();
    let mut with_leaf = DynamicMessage::new(rich.clone());
    let mut l = DynamicMessage::new(leaf);
    l.set_field_by_name("s", Value::String("a".into()));
    with_leaf.set_field_by_name("leaf", Value::Message(l));
    let merged_singular = [with_leaf.encode_to_vec(), with_leaf.encode_to_vec()].concat();

    for wire in [
        with_map.encode_to_vec(),
        with_wkt.encode_to_vec(),
        with_ext.encode_to_vec(),
        oneof_conflict,
        merged_singular,
    ] {
        assert!(
            !assert_parity(&rich, &plan, &wire),
            "expected fallback for {wire:02x?}"
        );
    }
}

#[test]
fn malformed_wire_reports_reference_error() {
    let pool = pool();
    let desc = pool.get_message_by_name("vm.Rich").unwrap();
    let plan = VariantMessagePlan::new(&desc);
    for wire in [
        &[0x08][..],                     // truncated varint
        &[0x72, 0x02, 0xff, 0xfe],       // o_string (14): invalid UTF-8
        &[0x0d, 0x01, 0x02, 0x03, 0x04], // o_int32 (1) with fixed32 wire type
        &[0x0b, 0x0c],                   // unknown group
        // An oversized field number must not wrap into the known field 14.
        &[0xf1, 0x80, 0x80, 0x80, 0x80, 0x01, 0, 0, 0, 0, 0, 0, 0, 0],
    ] {
        assert!(plan.encode_direct(wire).is_none());
        let reference = encode_variant_from_message(&desc, &WireValue::Len(wire));
        assert_eq!(
            plan.encode(&WireValue::Len(wire)).is_ok(),
            reference.is_ok()
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, ..ProptestConfig::default() })]

    /// Arbitrary bytes: the direct walk never accepts what the reference
    /// rejects, and anything it accepts matches the reference.
    #[test]
    fn random_bytes_match_reference(bytes in proptest::collection::vec(any::<u8>(), 0..128)) {
        let pool = pool();
        for name in ["vm.Rich", "vm3.Lean"] {
            let desc = pool.get_message_by_name(name).unwrap();
            assert_parity(&desc, &VariantMessagePlan::new(&desc), &bytes);
        }
    }
}
