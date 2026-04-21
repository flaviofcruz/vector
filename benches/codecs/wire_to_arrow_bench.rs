//! Benchmarks for the `wire_to_arrow` module in the `databricks_zerobus` sink.
//!
//! Compares two paths over identical proto wire bytes:
//!
//! * `new_wire_to_arrow` — `WireToArrowEncoder` from
//!   `vector::sinks::databricks_zerobus::wire_to_arrow` (direct wire -> Arrow).
//! * `old_chain`         — reference pipeline used elsewhere in Vector:
//!   `ProtobufDeserializer::parse` -> `Vec<Event>` -> `ArrowStreamSerializer::encode_to_record_batch`.
//!
//! A correctness gate runs at the start of the bench and asserts column-by-column
//! equality between the two paths for each variant. The gate panics on mismatch
//! so we never measure diverged encoders.
//!
//! Two variants exercised today:
//!
//! * `scalar` — `test_protobuf.Person` (name, id, email).
//! * `rich`   — `test_protobuf3.Person` (scalars + `repeated PhoneNumber phones`),
//!              phones populated with `number` only (the `type` enum field is
//!              omitted to sidestep a reference-path quirk where enum names
//!              don't coerce to Int32 Arrow columns).
//!
//! Run:
//!
//!     cargo bench --bench codecs --features "codecs-benches sinks-databricks-zerobus codecs-arrow" -- wire_to_arrow

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Fields, Schema};
use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group};
use prost_reflect::prost::Message as _;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, Value as ProtoValue};
use vector::event::Event;
use vector::sinks::databricks_zerobus::wire_to_arrow::WireToArrowEncoder;
use vector_lib::codecs::{
    decoding::{ProtobufDeserializer, format::Deserializer},
    encoding::{ArrowStreamSerializer, ArrowStreamSerializerConfig},
};
use vector_lib::config::LogNamespace;

// -------------------------------------------------------------------------
// Fixture helpers
// -------------------------------------------------------------------------

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

fn scalar_arrow_schema() -> Schema {
    Schema::new(vec![
        Field::new("name", DataType::LargeUtf8, true),
        Field::new("id", DataType::Int32, true),
        Field::new("email", DataType::LargeUtf8, true),
    ])
}

/// NOTE: intentionally omits `PhoneNumber.type` (an enum). The reference
/// `ProtobufDeserializer` renders enums as names, which then appear as nulls
/// in an Int32 Arrow column — so including `type` in the schema makes the
/// reference path diverge from `WireToArrowEncoder`. `number` alone is enough
/// to exercise `List<Struct>`.
fn rich_arrow_schema() -> Schema {
    let phone_struct = DataType::Struct(Fields::from(vec![Field::new(
        "number",
        DataType::LargeUtf8,
        true,
    )]));
    let phones_field = Field::new("item", phone_struct, true);
    Schema::new(vec![
        Field::new("name", DataType::LargeUtf8, true),
        Field::new("id", DataType::Int32, true),
        Field::new("email", DataType::LargeUtf8, true),
        Field::new("job_description", DataType::LargeUtf8, true),
        Field::new("phones", DataType::List(Arc::new(phones_field)), true),
    ])
}

fn build_scalar_bytes(descriptor: &MessageDescriptor, i: usize) -> Bytes {
    let mut msg = DynamicMessage::new(descriptor.clone());
    msg.set_field_by_name("name", ProtoValue::String(format!("person-{i}")));
    msg.set_field_by_name("id", ProtoValue::I32(i as i32));
    msg.set_field_by_name(
        "email",
        ProtoValue::String(format!("user{i}@databricks.com")),
    );
    let mut buf = Vec::with_capacity(64);
    msg.encode(&mut buf).unwrap();
    Bytes::from(buf)
}

fn build_rich_bytes(descriptor: &MessageDescriptor, i: usize) -> Bytes {
    let mut msg = DynamicMessage::new(descriptor.clone());
    msg.set_field_by_name("name", ProtoValue::String(format!("person-{i}")));
    msg.set_field_by_name("id", ProtoValue::I32(i as i32));
    msg.set_field_by_name(
        "email",
        ProtoValue::String(format!("user{i}@databricks.com")),
    );
    msg.set_field_by_name(
        "job_description",
        ProtoValue::String(format!("engineer level {}", i % 7)),
    );

    let phone_desc = descriptor
        .get_field_by_name("phones")
        .unwrap()
        .kind()
        .as_message()
        .unwrap()
        .clone();
    let mut phone1 = DynamicMessage::new(phone_desc.clone());
    phone1.set_field_by_name("number", ProtoValue::String(format!("555-{:04}", i)));
    let mut phone2 = DynamicMessage::new(phone_desc);
    phone2.set_field_by_name("number", ProtoValue::String(format!("555-{:04}", i + 1)));
    msg.set_field_by_name(
        "phones",
        ProtoValue::List(vec![
            ProtoValue::Message(phone1),
            ProtoValue::Message(phone2),
        ]),
    );

    let mut buf = Vec::with_capacity(128);
    msg.encode(&mut buf).unwrap();
    Bytes::from(buf)
}

// -------------------------------------------------------------------------
// Correctness gate
// -------------------------------------------------------------------------

fn check_correctness(
    descriptor: &MessageDescriptor,
    schema: Schema,
    messages: &[Bytes],
    label: &str,
) {
    let new_encoder = WireToArrowEncoder::new(descriptor, schema.clone()).expect("plan build");
    let deserializer = ProtobufDeserializer::new(descriptor.clone());
    let old_encoder =
        ArrowStreamSerializer::new(ArrowStreamSerializerConfig::new(schema)).expect("old ser");

    let new_batch = new_encoder.encode_batch(messages).expect("new encode");
    let events: Vec<Event> = messages
        .iter()
        .flat_map(|b| deserializer.parse(b.clone(), LogNamespace::Vector).unwrap())
        .collect();
    let old_batch = old_encoder.encode_to_record_batch(&events).expect("old encode");

    assert_eq!(new_batch.num_rows(), old_batch.num_rows(), "{label}: row count");
    assert_eq!(
        new_batch.num_columns(),
        old_batch.num_columns(),
        "{label}: col count"
    );
    for i in 0..new_batch.num_columns() {
        assert_eq!(
            new_batch.column(i).as_ref(),
            old_batch.column(i).as_ref(),
            "{label}: column {} ({}) differs",
            i,
            new_batch.schema().field(i).name(),
        );
    }
    eprintln!("correctness gate [{label}]: {} messages match ✓", messages.len());
}

// -------------------------------------------------------------------------
// Bench
// -------------------------------------------------------------------------

struct Variant {
    name: &'static str,
    descriptor: MessageDescriptor,
    schema: Schema,
    build_bytes: fn(&MessageDescriptor, usize) -> Bytes,
}

fn bench_wire_to_arrow(c: &mut Criterion) {
    let variants = vec![
        Variant {
            name: "scalar",
            descriptor: scalar_descriptor(),
            schema: scalar_arrow_schema(),
            build_bytes: build_scalar_bytes,
        },
        Variant {
            name: "rich",
            descriptor: rich_descriptor(),
            schema: rich_arrow_schema(),
            build_bytes: build_rich_bytes,
        },
    ];

    for v in &variants {
        let messages: Vec<Bytes> = (0..100).map(|i| (v.build_bytes)(&v.descriptor, i)).collect();
        check_correctness(&v.descriptor, v.schema.clone(), &messages, v.name);
    }

    let mut group = c.benchmark_group("wire_to_arrow");
    group.measurement_time(Duration::from_secs(20));
    group.warm_up_time(Duration::from_secs(3));
    group.sample_size(50);

    for v in &variants {
        let new_encoder = Arc::new(
            WireToArrowEncoder::new(&v.descriptor, v.schema.clone()).expect("plan build"),
        );
        let deserializer = Arc::new(ProtobufDeserializer::new(v.descriptor.clone()));
        let reference_serializer = Arc::new(
            ArrowStreamSerializer::new(ArrowStreamSerializerConfig::new(v.schema.clone()))
                .unwrap(),
        );

        for &batch_size in &[100_usize, 1_000, 10_000] {
            let wire_bytes: Vec<Bytes> = (0..batch_size)
                .map(|i| (v.build_bytes)(&v.descriptor, i))
                .collect();
            let total_wire_bytes: u64 = wire_bytes.iter().map(|b| b.len() as u64).sum();

            group.throughput(Throughput::Bytes(total_wire_bytes));

            {
                let enc = Arc::clone(&new_encoder);
                let input = wire_bytes.clone();
                group.bench_with_input(
                    BenchmarkId::new(format!("{}/new_wire_to_arrow", v.name), batch_size),
                    &(enc, input),
                    |b, (enc, messages): &(Arc<WireToArrowEncoder>, Vec<Bytes>)| {
                        b.iter(|| {
                            let batch = enc.encode_batch(messages).unwrap();
                            std::hint::black_box(batch);
                        });
                    },
                );
            }

            {
                let deser = Arc::clone(&deserializer);
                let ser = Arc::clone(&reference_serializer);
                let input = wire_bytes;
                group.bench_with_input(
                    BenchmarkId::new(format!("{}/old_chain", v.name), batch_size),
                    &(deser, ser, input),
                    |b,
                     (deser, ser, messages): &(
                        Arc<ProtobufDeserializer>,
                        Arc<ArrowStreamSerializer>,
                        Vec<Bytes>,
                    )| {
                        b.iter(|| {
                            let events: Vec<Event> = messages
                                .iter()
                                .flat_map(|by| {
                                    deser.parse(by.clone(), LogNamespace::Vector).unwrap()
                                })
                                .collect();
                            let batch = ser.encode_to_record_batch(&events).unwrap();
                            std::hint::black_box(batch);
                        });
                    },
                );
            }
        }
    }
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(3))
        .measurement_time(Duration::from_secs(20))
        .noise_threshold(0.01)
        .significance_level(0.05)
        .confidence_level(0.95)
        .sample_size(50);
    targets = bench_wire_to_arrow
);
