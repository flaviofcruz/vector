//! One-shot byte-size comparison: ProtoBatch vs ArrowStream on sparse
//! `ServiceHealthEvent` payloads.
//!
//! Mirrors `LumberjackPrimeArchivedLogGenerator` in
//! `lamp-test-service/src/LogGenerator.scala`: each row always sets
//! `event_name` + `duration_ms`, then independently flips a coin for each
//! top-level optional field, and with 50% probability attaches one of three
//! `ServiceExtra` oneof variants (Jobs / Clusters / SqlGateway) filled with
//! random content.
//!
//! Run with:
//!
//! ```bash
//! cargo test -p vector --features 'sinks-databricks-zerobus codecs-arrow' \
//!   --release she_bytes_per_encoding -- --ignored --nocapture
//! ```

use std::fs;
use std::path::Path;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Schema};
use arrow::ipc::writer::StreamWriter;
use bytes::Bytes;
use prost_reflect::prost::Message as _;
use prost_reflect::{DynamicMessage, MessageDescriptor, Value as ProtoValue};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use vector_lib::codecs::encoding::WireToArrowEncoder;
use vrl::protobuf::descriptor::get_message_descriptor;

use super::proto_to_arrow::proto_descriptor_to_arrow_schema;

const SHE_DESC_PATH: &str = "/tmp/she_bench/she.desc";
const SHE_NAME: &str = "com.databricks.logging.proto.ServiceHealthEvent";

const ALPHANUM: &[u8] =
    b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

fn random_alphanum(rng: &mut SmallRng, len: usize) -> String {
    (0..len)
        .map(|_| ALPHANUM[rng.random_range(0..ALPHANUM.len())] as char)
        .collect()
}

fn count_leaf_columns(dt: &DataType) -> usize {
    match dt {
        DataType::Struct(fs) => fs.iter().map(|f| count_leaf_columns(f.data_type())).sum(),
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            count_leaf_columns(f.data_type())
        }
        DataType::Map(f, _) => count_leaf_columns(f.data_type()),
        _ => 1,
    }
}

fn count_schema_leaf_columns(schema: &Schema) -> usize {
    schema
        .fields()
        .iter()
        .map(|f| count_leaf_columns(f.data_type()))
        .sum()
}

fn build_service_extra(rng: &mut SmallRng, descriptor: &MessageDescriptor) -> DynamicMessage {
    // ServiceExtra is the message type of ServiceHealthEvent.service_extra (field 7).
    let se_desc = descriptor
        .get_field_by_name("service_extra")
        .unwrap()
        .kind()
        .as_message()
        .unwrap()
        .clone();
    let mut se = DynamicMessage::new(se_desc.clone());

    match rng.random_range(0..3u32) {
        0 => {
            let jobs_desc = se_desc
                .get_field_by_name("jobs")
                .unwrap()
                .kind()
                .as_message()
                .unwrap()
                .clone();
            let mut jobs = DynamicMessage::new(jobs_desc.clone());
            jobs.set_field_by_name("job_id", ProtoValue::I64(rng.random::<i64>().abs()));
            jobs.set_field_by_name("task_run_id", ProtoValue::I64(rng.random::<i64>().abs()));
            jobs.set_field_by_name("job_run_id", ProtoValue::I64(rng.random::<i64>().abs()));
            jobs.set_field_by_name(
                "run_termination_reason",
                ProtoValue::String(random_alphanum(rng, 40)),
            );
            jobs.set_field_by_name("is_serverless", ProtoValue::Bool(rng.random::<bool>()));
            jobs.set_field_by_name(
                "cluster_termination_reason",
                ProtoValue::String(random_alphanum(rng, 40)),
            );
            jobs.set_field_by_name(
                "customer_impacting_startup_delay",
                ProtoValue::Bool(rng.random::<bool>()),
            );
            jobs.set_field_by_name("managed_by", ProtoValue::String(random_alphanum(rng, 20)));

            let dbr_err_desc = jobs_desc
                .get_field_by_name("dbr_error_info")
                .unwrap()
                .kind()
                .as_message()
                .unwrap()
                .clone();
            let mut dbr_err = DynamicMessage::new(dbr_err_desc);
            dbr_err.set_field_by_name("sql_state", ProtoValue::String(random_alphanum(rng, 5)));
            dbr_err.set_field_by_name("error_class", ProtoValue::String(random_alphanum(rng, 20)));
            dbr_err.set_field_by_name(
                "is_internal_exception",
                ProtoValue::Bool(rng.random::<bool>()),
            );
            jobs.set_field_by_name("dbr_error_info", ProtoValue::Message(dbr_err));

            se.set_field_by_name("jobs", ProtoValue::Message(jobs));
        }
        1 => {
            let clusters_desc = se_desc
                .get_field_by_name("clusters")
                .unwrap()
                .kind()
                .as_message()
                .unwrap()
                .clone();
            let mut clusters = DynamicMessage::new(clusters_desc);
            clusters.set_field_by_name("cluster_id", ProtoValue::String(random_alphanum(rng, 20)));
            clusters.set_field_by_name(
                "cluster_creator",
                ProtoValue::String(random_alphanum(rng, 20)),
            );
            clusters.set_field_by_name("is_vcpu", ProtoValue::Bool(rng.random::<bool>()));
            clusters.set_field_by_name("is_autoscaling", ProtoValue::Bool(rng.random::<bool>()));
            clusters.set_field_by_name(
                "worker_env_id",
                ProtoValue::String(random_alphanum(rng, 20)),
            );
            clusters.set_field_by_name(
                "effective_spark_version",
                ProtoValue::String(random_alphanum(rng, 10)),
            );
            clusters.set_field_by_name(
                "driver_node_type_id",
                ProtoValue::String(random_alphanum(rng, 15)),
            );
            clusters.set_field_by_name(
                "worker_node_type_id",
                ProtoValue::String(random_alphanum(rng, 15)),
            );
            clusters.set_field_by_name(
                "data_plane_region",
                ProtoValue::String(random_alphanum(rng, 12)),
            );
            clusters.set_field_by_name("zone_id", ProtoValue::String(random_alphanum(rng, 10)));
            clusters.set_field_by_name(
                "error_rule_id",
                ProtoValue::String(random_alphanum(rng, 20)),
            );
            se.set_field_by_name("clusters", ProtoValue::Message(clusters));
        }
        _ => {
            let sql_desc = se_desc
                .get_field_by_name("sqlgateway")
                .unwrap()
                .kind()
                .as_message()
                .unwrap()
                .clone();
            let mut sql = DynamicMessage::new(sql_desc);
            sql.set_field_by_name("method_name", ProtoValue::String(random_alphanum(rng, 30)));
            sql.set_field_by_name("endpoint_id", ProtoValue::String(random_alphanum(rng, 30)));
            sql.set_field_by_name(
                "is_serverless_warehouse",
                ProtoValue::Bool(rng.random::<bool>()),
            );
            sql.set_field_by_name(
                "is_due_to_known_error",
                ProtoValue::Bool(rng.random::<bool>()),
            );
            sql.set_field_by_name(
                "is_asynchronous_execution",
                ProtoValue::Bool(rng.random::<bool>()),
            );
            se.set_field_by_name("sqlgateway", ProtoValue::Message(sql));
        }
    }

    se
}

fn build_she_wire_bytes(rng: &mut SmallRng, seq: i64, descriptor: &MessageDescriptor) -> Bytes {
    let mut msg = DynamicMessage::new(descriptor.clone());

    // Always-set identification fields.
    msg.set_field_by_name(
        "event_name",
        ProtoValue::String("LumberjackPrimeLoadTest".into()),
    );
    msg.set_field_by_name("duration_ms", ProtoValue::I64(seq));

    // Each top-level optional: ~50% probability of being populated.
    if rng.random::<bool>() {
        msg.set_field_by_name("outcome", ProtoValue::String(random_alphanum(rng, 50)));
    }
    if rng.random::<bool>() {
        msg.set_field_by_name(
            "outcome_details",
            ProtoValue::String(random_alphanum(rng, 250)),
        );
    }
    if rng.random::<bool>() {
        msg.set_field_by_name("dbr_version", ProtoValue::String(random_alphanum(rng, 20)));
    }
    if rng.random::<bool>() {
        msg.set_field_by_name(
            "engine_request_id",
            ProtoValue::String(random_alphanum(rng, 40)),
        );
    }
    if rng.random::<bool>() {
        msg.set_field_by_name(
            "workspace_id",
            ProtoValue::I64(rng.random::<i64>().abs()),
        );
    }
    if rng.random::<bool>() {
        msg.set_field_by_name(
            "classification_low_confidence",
            ProtoValue::Bool(rng.random::<bool>()),
        );
    }
    if rng.random::<bool>() {
        msg.set_field_by_name("is_suppressed", ProtoValue::Bool(rng.random::<bool>()));
    }
    if rng.random::<bool>() {
        let ri_desc = descriptor
            .get_field_by_name("request_info")
            .unwrap()
            .kind()
            .as_message()
            .unwrap()
            .clone();
        let mut ri = DynamicMessage::new(ri_desc);
        ri.set_field_by_name("handler", ProtoValue::String(random_alphanum(rng, 60)));
        ri.set_field_by_name("retry_count", ProtoValue::I32(rng.random_range(0..5i32)));
        // request_type / source_type are enums; leave at default to avoid per-proto-version churn.
        msg.set_field_by_name("request_info", ProtoValue::Message(ri));
    }
    if rng.random::<bool>() {
        let se = build_service_extra(rng, descriptor);
        msg.set_field_by_name("service_extra", ProtoValue::Message(se));
    }

    let mut buf = Vec::with_capacity(512);
    msg.encode(&mut buf).unwrap();
    Bytes::from(buf)
}

fn proto_batch_bytes(messages: &[Bytes]) -> usize {
    // Length-delimited framing: 4-byte LE length prefix + payload per row.
    // This is what ProtoBatchSerializer emits; sum is the wire size the sink would send.
    messages.iter().map(|m| 4 + m.len()).sum()
}

fn arrow_stream_bytes(schema: &Schema, batch: &RecordBatch) -> usize {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
    }
    buf.len()
}

#[test]
#[ignore = "standalone bench: run with --ignored --nocapture and requires /tmp/she_bench/she.desc"]
fn she_bytes_per_encoding() {
    if !Path::new(SHE_DESC_PATH).exists() {
        panic!(
            "Missing {SHE_DESC_PATH}. Build with:\n  \
             bazel build //proto/logs/sla:service_health_event_proto_desc\n  \
             cp bazel-bin/proto/logs/sla/service_health_event_proto_desc.pb {SHE_DESC_PATH}"
        );
    }

    let desc_bytes = fs::read(SHE_DESC_PATH).unwrap();
    let _ = desc_bytes; // load via helper below
    let descriptor = get_message_descriptor(Path::new(SHE_DESC_PATH), SHE_NAME)
        .expect("load ServiceHealthEvent descriptor");

    let schema = proto_descriptor_to_arrow_schema(&descriptor).expect("proto -> arrow schema");
    let top_cols = schema.fields().len();
    let leaf_cols = count_schema_leaf_columns(&schema);

    eprintln!(
        "ServiceHealthEvent Arrow schema: {top_cols} top-level fields, {leaf_cols} leaf columns"
    );

    let encoder = WireToArrowEncoder::new(&descriptor, schema.clone())
        .expect("build WireToArrowEncoder");

    eprintln!(
        "\n{:>8}  {:>14}  {:>12}  {:>14}  {:>12}  {:>10}",
        "N rows", "proto_batch B", "B/row", "arrow_stream B", "B/row", "arrow/proto"
    );
    eprintln!("{}", "-".repeat(82));

    for &n in &[1usize, 10, 100, 1_000, 10_000] {
        // Fixed seed so every batch size gets a deterministic, reproducible mix.
        let mut rng = SmallRng::seed_from_u64(0xDEADBEEF);
        let messages: Vec<Bytes> = (0..n)
            .map(|i| build_she_wire_bytes(&mut rng, i as i64, &descriptor))
            .collect();

        let proto_total = proto_batch_bytes(&messages);
        let batch = encoder.encode_batch(&messages).expect("wire -> arrow");
        assert_eq!(batch.num_rows(), n);
        let arrow_total = arrow_stream_bytes(&schema, &batch);

        eprintln!(
            "{:>8}  {:>14}  {:>12.1}  {:>14}  {:>12.1}  {:>9.2}x",
            n,
            proto_total,
            proto_total as f64 / n as f64,
            arrow_total,
            arrow_total as f64 / n as f64,
            arrow_total as f64 / proto_total as f64
        );
    }
}
