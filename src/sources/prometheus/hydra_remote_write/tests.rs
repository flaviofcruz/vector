//! Source equivalence tests against prost + the unchanged per-sample transform.
use super::*;
use crate::{
    config::TransformOutput, event::Event, transforms::metric_to_log_hydra::MetricToLogHydra,
};
use arrow::{array::RecordBatch, ipc::reader::StreamReader};
use rand::{Rng, SeedableRng, rngs::SmallRng};
use std::collections::BTreeMap;
use vector_lib::{
    codecs::encoding::{ArrowStreamSerializer, ArrowStreamSerializerConfig},
    transform::{SyncTransform, TransformOutputsBuf},
};

const CONFIG: &str = r#"
pod_routing:
  systems:
    - system: "*"
      pods: [pod0, pod_prime, pod_prime_copy, pod_prime_variant]
    - system: lakebase-storage
      pods: [pod0, pod_prime, pod_prime_copy, pod_prime_a, pod_prime_variant, poda]
    - system: reyden
      pods: [pod0, pod_prime, pod_prime_copy, pod_prime_a, pod_prime_variant, podb]
outputs:
  pod0: ndjson
  poda: ndjson
  podb: ndjson
  pod_prime: arrow_ipc
  pod_prime_copy: arrow_ipc
  pod_prime_a: arrow_ipc
  pod_prime_variant: arrow_ipc_variant
"#;
const PORTS: [&str; 7] = [
    "pod0",
    "poda",
    "podb",
    "pod_prime",
    "pod_prime_copy",
    "pod_prime_a",
    "pod_prime_variant",
];
#[derive(serde::Deserialize)]
struct Cfg {
    pod_routing: PodRoutingConfig,
    outputs: BTreeMap<String, OutputEncoding>,
}
fn cfg() -> Cfg {
    serde_yaml::from_str(CONFIG).unwrap()
}
fn source() -> HydraRemoteWriteSource {
    let c = cfg();
    HydraRemoteWriteSource {
        plan: Arc::new(OutputPlan::compile(&c.outputs, &c.pod_routing).unwrap()),
        port_field: "hydra_port".into(),
    }
}
fn message(e: &Event) -> Bytes {
    match e.as_log().get("message") {
        Some(Value::Bytes(bytes)) => bytes.clone(),
        other => panic!("missing encoded payload: {other:?}"),
    }
}
#[derive(Debug, PartialEq)]
enum Rows {
    Json(Bytes),
    Arrow(RecordBatch),
}
fn render(bytes: Bytes, encoding: OutputEncoding) -> Rows {
    if encoding == OutputEncoding::Ndjson {
        return Rows::Json(bytes);
    }
    let mut reader = StreamReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
    let record = reader.next().unwrap().unwrap();
    assert!(
        reader.next().is_none(),
        "one request record per destination"
    );
    Rows::Arrow(record)
}
fn reference(body: Bytes) -> Result<BTreeMap<String, Rows>, String> {
    let request = proto::WriteRequest::decode(body)
        .map_err(|e| format!("Could not decode write request: {e}"))?;
    let events = super::super::parser::parse_request_flat(request, false)
        .map_err(|e| format!("Could not decode write request: {e}"))?;
    let config = cfg();
    let mut transform = MetricToLogHydra::with_pod_routing(&config.pod_routing).unwrap();
    let outputs = PORTS
        .iter()
        .map(|p| TransformOutput::new(DataType::Log, Default::default()).with_port(*p))
        .collect();
    let mut output = TransformOutputsBuf::new_with_capacity(outputs, 32);
    for event in events {
        transform.transform(event, &mut output);
    }
    let mut result = BTreeMap::new();
    for port in PORTS {
        let events: Vec<Event> = output.drain_named(port).collect();
        if events.is_empty() {
            continue;
        }
        let encoding = config.outputs[port];
        let rows = if encoding == OutputEncoding::Ndjson {
            let json: Vec<Vec<u8>> = events
                .iter()
                .map(|e| serde_json::to_vec(e.as_log()).unwrap())
                .collect();
            Rows::Json(Bytes::from(json.join(&b'\n')))
        } else {
            let schema = if encoding == OutputEncoding::ArrowIpcVariant {
                encoding::variant_schema()
            } else {
                encoding::arrow_schema()
            };
            let serializer = ArrowStreamSerializer::new(ArrowStreamSerializerConfig::new(
                schema.as_ref().clone(),
            ))
            .unwrap();
            Rows::Arrow(serializer.encode_to_record_batch(&events).unwrap())
        };
        result.insert(port.to_string(), rows);
    }
    Ok(result)
}
fn fused(body: Bytes) -> Result<BTreeMap<String, Rows>, String> {
    let events = source()
        .decode_body(body)
        .map_err(|e| e.message().to_string())?;
    let config = cfg();
    let mut result = BTreeMap::new();
    for event in events {
        let port = event
            .as_log()
            .get("hydra_port")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let rows = render(message(&event), config.outputs[&port]);
        let count = match &rows {
            Rows::Json(bytes) => bytes.split(|b| *b == b'\n').count(),
            Rows::Arrow(record) => record.num_rows(),
        };
        assert_eq!(
            event.metadata().delivery_event_count(),
            count,
            "{port}: sample delivery count"
        );
        assert!(
            result.insert(port, rows).is_none(),
            "one payload per destination"
        );
    }
    Ok(result)
}
fn label(name: &str, value: &str) -> proto::Label {
    proto::Label {
        name: name.into(),
        value: value.into(),
    }
}
fn request(rng: &mut SmallRng, series: usize) -> proto::WriteRequest {
    let now = chrono::Utc::now().timestamp_millis();
    let systems = ["", "web", "lakebase-storage", "reyden"];
    let mut timeseries = Vec::new();
    for i in 0..series {
        let mut labels = vec![
            label(
                "__name__",
                ["a_total", "b_bucket", "c", "d_sum", "x_summary", "q_count"][i % 6],
            ),
            label("region_uri", "aws:us-west-2"),
            label("shardName", &format!("s{}", rng.random_range(0..3))),
            label("z\"q", "v\\1\né😀"),
            label("instance", &format!("10.0.0.{}", rng.random_range(0..255))),
        ];
        let system = systems[rng.random_range(0..systems.len())];
        if !system.is_empty() {
            labels.push(label("system", system));
        }
        if rng.random_bool(0.3) {
            labels.push(label("workspace_id", "123"));
        }
        if rng.random_bool(0.2) {
            labels.push(label("instance", "dup"));
        }
        if rng.random_bool(0.1) {
            labels.push(label("__name__", "renamed_count"));
        }
        if rng.random_bool(0.1) {
            labels.push(label("empty", ""));
        }
        if rng.random_bool(0.5) {
            labels.reverse();
        }
        let n = rng.random_range(0..3);
        let samples = (0..n)
            .map(|_| proto::Sample {
                value: match rng.random_range(0..6) {
                    0 => f64::NAN,
                    1 => -0.0,
                    2 => f64::INFINITY,
                    _ => rng.random::<f64>() * 1e6,
                },
                timestamp: match rng.random_range(0..10) {
                    0 => now - 3_600_000,
                    1 => 0,
                    _ => now - rng.random_range(0..100_000),
                },
            })
            .collect();
        timeseries.push(proto::TimeSeries { labels, samples });
    }
    proto::WriteRequest {
        timeseries,
        ..Default::default()
    }
}
#[test]
fn encoded_output_matches_prost_and_per_sample_transform() {
    let mut rng = SmallRng::seed_from_u64(7);
    for i in 0..200 {
        let mut req = request(&mut rng, 1 + i % 40);
        if i % 3 == 0 {
            req.metadata.push(proto::MetricMetadata {
                r#type: 2,
                metric_family_name: "a".into(),
                help: "h".into(),
                unit: "u".into(),
            });
        }
        let body = Bytes::from(req.encode_to_vec());
        assert_eq!(fused(body.clone()), reference(body), "request {i}");
    }
    let body = Bytes::from(
        proto::WriteRequest {
            timeseries: vec![proto::TimeSeries {
                labels: vec![label("a", "b")],
                samples: vec![],
            }],
            ..Default::default()
        }
        .encode_to_vec(),
    );
    let expected = reference(body.clone());
    assert!(expected.is_err());
    assert_eq!(fused(body), expected);
}
#[test]
fn outputs_require_every_routed_destination_and_no_others() {
    let mut c = cfg();
    c.outputs.remove("poda");
    assert!(OutputPlan::compile(&c.outputs, &c.pod_routing).is_err());
    c.outputs.insert("poda".into(), OutputEncoding::Ndjson);
    c.outputs.insert("unknown".into(), OutputEncoding::Ndjson);
    assert!(OutputPlan::compile(&c.outputs, &c.pod_routing).is_err());
}
#[test]
fn arrow_payload_columns_are_the_per_sample_log_fields() {
    let now = chrono::Utc::now().timestamp_millis();
    let req = proto::WriteRequest {
        timeseries: vec![proto::TimeSeries {
            labels: vec![
                label("__name__", "x_total"),
                label("cluster_id", "c"),
                label("region_uri", "aws:us-west-2"),
                label("shardName", "s"),
                label("system", "reyden"),
                label("tenant_id", "t"),
                label("workspace_id", "w"),
            ],
            samples: vec![proto::Sample {
                value: 1.5,
                timestamp: now,
            }],
        }],
        ..Default::default()
    };
    let body = Bytes::from(req.encode_to_vec());
    let events = super::super::parser::parse_request_flat(
        proto::WriteRequest::decode(body.clone()).unwrap(),
        false,
    )
    .unwrap();
    let config = cfg();
    let mut transform = MetricToLogHydra::with_pod_routing(&config.pod_routing).unwrap();
    let outputs = PORTS
        .iter()
        .map(|p| TransformOutput::new(DataType::Log, Default::default()).with_port(*p))
        .collect();
    let mut output = TransformOutputsBuf::new_with_capacity(outputs, 1);
    transform.transform(events.into_iter().next().unwrap(), &mut output);
    let sample = output.drain_named("pod_prime").next().unwrap();
    let mut log_fields: Vec<String> = sample
        .as_log()
        .as_map()
        .unwrap()
        .keys()
        .map(|k| k.to_string())
        .collect();
    log_fields.sort();
    for schema in [encoding::arrow_schema(), encoding::variant_schema()] {
        let mut columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        columns.sort();
        assert_eq!(columns, log_fields);
    }
    let fused = fused(body).unwrap();
    let Rows::Arrow(record) = &fused["pod_prime"] else {
        panic!("pod_prime is arrow_ipc")
    };
    assert_eq!(record.schema(), encoding::arrow_schema());
}

#[test]
fn received_samples_counts_every_sample_dropped_or_routed() {
    let now = chrono::Utc::now().timestamp_millis();
    let samples = [
        (1.0, now),
        (2.0, now),
        (f64::NAN, now),
        (3.0, now - 3_600_000),
        (f64::INFINITY, 0),
    ];
    let req = proto::WriteRequest {
        timeseries: vec![proto::TimeSeries {
            labels: vec![label("__name__", "x_total"), label("system", "web")],
            samples: samples
                .map(|(value, timestamp)| proto::Sample { value, timestamp })
                .into(),
        }],
        ..Default::default()
    };
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        source().decode_body(req.encode_to_vec().into())
    })
    .unwrap();
    let mut counters = BTreeMap::new();
    for (key, _, _, value) in snapshotter.snapshot().into_vec() {
        let metrics_util::debugging::DebugValue::Counter(n) = value else {
            continue;
        };
        let labels: Vec<_> = key.key().labels().map(|l| l.value().to_string()).collect();
        counters.insert(format!("{}{labels:?}", key.key().name()), n);
    }
    let dropped = counters["metric_to_log_hydra_dropped_total[\"non_finite_value\"]"]
        + counters["metric_to_log_hydra_dropped_total[\"timestamp_out_of_window\"]"];
    let routed = counters["hydra_remote_write_encoded_rows_total[\"pod0\"]"];
    assert_eq!(counters["hydra_remote_write_received_samples_total[]"], 5);
    assert_eq!((dropped, routed), (3, 2));
}
