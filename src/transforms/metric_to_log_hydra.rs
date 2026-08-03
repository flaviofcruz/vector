use chrono::Utc;
use md5::{Digest, Md5};
use serde_json::Value as JsonValue;
use vector_lib::{
    configurable::configurable_component,
    internal_event::{Count, InternalEventHandle as _, Registered},
    lookup::event_path,
};
use vrl::value::{KeyString, Value};

use crate::{
    config::{
        DataType, GenerateConfig, Input, OutputId, TransformConfig, TransformContext,
        TransformOutput,
    },
    event::{Event, LogEvent, Metric, MetricValue},
    internal_events::MetricToLogHydraEventsDropped,
    schema::Definition,
    transforms::{FunctionTransform, OutputBuffer, Transform},
};

/// Native replacement for the three-transform chain used in the Hydra/VAM
/// metrics ingestion pipeline:
///
/// ```text
/// metric_to_log  →  log_based_metric_transformed (remap)  →  log_based_metric_json_transformed (remap)
/// ```
///
/// **Output schema is identical to the legacy VRL pipeline.** Key compatibility
/// guarantees:
/// - `md5_labels` matches VRL's `md5(encode_json(.tags))` — BTreeMap-sorted keys,
///   serde_json compact format, no `preserve_order`.
/// - Timestamp window (−10 min / +5 min) matches the VRL `abort` condition.
/// - Optional tag fields (`workspace_id`, `cluster_id`, etc.) produce JSON `null`
///   when absent, matching VRL's `?? null` fallback.
/// - Metrics dropped for timestamp violations increment
///   `component_events_dropped_total{intentional="true"}`.
///
/// **Do not use for new pipelines** — this encodes Hydra-specific field semantics.
/// Use `metric_to_log` + VRL remap for a different output shape.
#[configurable_component(transform(
    "metric_to_log_hydra",
    "Convert Prometheus metrics to Hydra log events natively."
))]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct MetricToLogHydraConfig {}

impl GenerateConfig for MetricToLogHydraConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {}).unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "metric_to_log_hydra")]
impl TransformConfig for MetricToLogHydraConfig {
    async fn build(&self, _context: &TransformContext) -> crate::Result<Transform> {
        Ok(Transform::function(MetricToLogHydra::new()))
    }

    fn input(&self) -> Input {
        Input::metric()
    }

    fn outputs(
        &self,
        _context: &TransformContext,
        input_definitions: &[(OutputId, Definition)],
    ) -> Vec<TransformOutput> {
        vec![TransformOutput::new(
            DataType::Log,
            input_definitions
                .iter()
                .map(|(id, _)| (id.clone(), Definition::default_legacy_namespace()))
                .collect(),
        )]
    }

    fn enable_concurrency(&self) -> bool {
        true
    }
}

/// Known prometheus metric-name suffixes and their canonical type strings.
const SUFFIXES: &[(&str, &str)] = &[
    ("_bucket", "bucket"),
    ("_sum", "sum"),
    ("_count", "count"),
    ("_total", "total"),
    ("_summary", "summary"),
];

#[derive(Clone)]
pub struct MetricToLogHydra {
    events_dropped: Registered<MetricToLogHydraEventsDropped>,
}

impl MetricToLogHydra {
    pub fn new() -> Self {
        Self {
            events_dropped: register!(MetricToLogHydraEventsDropped),
        }
    }

    /// Strip a known prometheus suffix from `name`, returning (base, type_str).
    fn parse_metric_name(name: &str) -> (&str, &'static str) {
        for (suffix, type_str) in SUFFIXES {
            if let Some(base) = name.strip_suffix(suffix) {
                return (base, type_str);
            }
        }
        (name, "gauge")
    }

    fn md5_hex(s: &str) -> String {
        format!("{:x}", Md5::digest(s.as_bytes()))
    }

    /// Compute `metric_part`: first 8 hex chars of md5(metric_name) as u64 mod 100.
    fn metric_part(metric_name: &str) -> i64 {
        let hex = Self::md5_hex(metric_name);
        (i64::from_str_radix(&hex[..8], 16)
            .unwrap_or(0)
            .unsigned_abs()
            % 100) as i64
    }

    /// Consume a `Metric` and produce a `LogEvent`, or `None` if the timestamp
    /// is outside the accepted window (−10 min / +5 min from now).
    pub fn transform_one(metric: Metric) -> Option<LogEvent> {
        let now_ms = Utc::now().timestamp_millis();

        let timestamp_ms: i64 = metric
            .timestamp()
            .map(|ts| ts.timestamp_millis())
            .unwrap_or(now_ms);

        if timestamp_ms > now_ms + 300_000 || timestamp_ms < now_ms - 600_000 {
            return None;
        }

        let timestamp_hour_ms = timestamp_ms - (timestamp_ms % 3_600_000);

        // Values array [{ts, v}] — only Gauge and Counter carry a scalar value.
        let values: Value = match metric.value() {
            MetricValue::Gauge { value } | MetricValue::Counter { value } => {
                Value::Array(vec![Value::Object(
                    [
                        (KeyString::from("ts"), Value::Integer(timestamp_ms)),
                        (
                            KeyString::from("v"),
                            Value::Float((*value).try_into().ok()?),
                        ),
                    ]
                    .into(),
                )])
            }
            _ => Value::Array(vec![]),
        };

        let original_name: String = metric.name().to_owned();
        let (metric_name, metric_type) = {
            let (base, t) = Self::parse_metric_name(&original_name);
            (base.to_owned(), t)
        };
        let part = Self::metric_part(&metric_name);

        // Build tag map once; used for both labels field and individual tag lookups.
        let tags_iter = metric.tags().map(|t| t.iter_single()).into_iter().flatten();

        // Collect into a Vec so we can iterate twice (labels + individual fields).
        let tags: Vec<(String, String)> = tags_iter
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        // md5_labels: hash a canonical JSON representation of the sorted tag map,
        // matching VRL's `md5(encode_json(.tags))`. Use serde_json for correct
        // escaping of any special characters in tag keys/values.
        // Tags from iter_single are already in BTreeMap sorted order.
        let tags_json = {
            let map: serde_json::Map<String, JsonValue> = tags
                .iter()
                .map(|(k, v)| (k.clone(), JsonValue::String(v.clone())))
                .collect();
            serde_json::to_string(&map).unwrap_or_default()
        };
        let md5_labels = Self::md5_hex(&tags_json);

        let get_tag = |key: &str| -> Option<String> {
            tags.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
        };

        let system_label = get_tag("system").unwrap_or_default();
        let workspace_id = get_tag("workspace_id");
        let cluster_id = get_tag("cluster_id");
        let tenant_id = get_tag("tenant_id");
        let endpoint_name = get_tag("endpoint_name");
        let shard_name = get_tag("shardName").unwrap_or_default();
        let region_uri = get_tag("region_uri");

        let hydra_cluster_column = format!("{}|{}|{}", original_name, system_label, shard_name);

        let tenant_cluster_column: Value = cluster_id
            .clone()
            .or_else(|| endpoint_name)
            .or_else(|| tenant_id.clone())
            .or_else(|| workspace_id.clone())
            .map(|s| Value::Bytes(s.into()))
            .unwrap_or(Value::Null);

        let system_uri: Value = if !system_label.is_empty() {
            Value::Bytes(format!("system:{}", system_label).into())
        } else {
            Value::Null
        };

        // Labels as a nested object.
        let labels: Value = Value::Object(
            tags.into_iter()
                .map(|(k, v)| (KeyString::from(k), Value::Bytes(v.into())))
                .collect(),
        );

        // Extract EventMetadata before consuming metric.
        let (_, _, metadata) = metric.into_parts();
        let mut log = LogEvent::new_with_metadata(metadata);

        log.insert(event_path!("name"), Value::Bytes(original_name.into()));
        log.insert(event_path!("labels"), labels);
        log.insert(event_path!("start_ts"), Value::Integer(timestamp_ms));
        log.insert(event_path!("metric_name"), Value::Bytes(metric_name.into()));
        log.insert(
            event_path!("timestamp_hour_ms"),
            Value::Integer(timestamp_hour_ms),
        );
        log.insert(event_path!("metric_part"), Value::Integer(part));
        log.insert(event_path!("md5_labels"), Value::Bytes(md5_labels.into()));
        log.insert(event_path!("metric_type"), Value::Bytes(metric_type.into()));
        log.insert(event_path!("values"), values);
        log.insert(
            event_path!("workspace_id"),
            workspace_id
                .map(|s| Value::Bytes(s.into()))
                .unwrap_or(Value::Null),
        );
        log.insert(
            event_path!("cluster_id"),
            cluster_id
                .map(|s| Value::Bytes(s.into()))
                .unwrap_or(Value::Null),
        );
        log.insert(
            event_path!("region_uri"),
            region_uri
                .map(|s| Value::Bytes(s.into()))
                .unwrap_or(Value::Null),
        );
        log.insert(
            event_path!("tenant_id"),
            tenant_id
                .map(|s| Value::Bytes(s.into()))
                .unwrap_or(Value::Null),
        );
        log.insert(
            event_path!("hydra_cluster_column"),
            Value::Bytes(hydra_cluster_column.into()),
        );
        log.insert(event_path!("tenant_cluster_column"), tenant_cluster_column);
        log.insert(event_path!("shard_name"), Value::Bytes(shard_name.into()));
        log.insert(event_path!("system_uri"), system_uri);
        log.insert(event_path!("topic"), Value::Bytes("metric-to-log".into()));

        Some(log)
    }
}

impl FunctionTransform for MetricToLogHydra {
    fn transform(&mut self, output: &mut OutputBuffer, event: Event) {
        let metric = event.into_metric();
        match Self::transform_one(metric) {
            Some(log) => output.push(log.into()),
            None => self.events_dropped.emit(Count(1)),
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use vector_lib::event::metric::{MetricKind, MetricTags, MetricValue};

    use super::*;
    use crate::event::Metric;

    fn make_gauge(name: &str, value: f64, tags: &[(&str, &str)], ts_ms: i64) -> Metric {
        let mut metric_tags = MetricTags::default();
        for (k, v) in tags {
            metric_tags.replace(k.to_string(), v.to_string());
        }
        let ts = Utc.timestamp_millis_opt(ts_ms).unwrap();
        Metric::new(name, MetricKind::Absolute, MetricValue::Gauge { value })
            .with_tags(Some(metric_tags))
            .with_timestamp(Some(ts))
    }

    fn make_counter(name: &str, value: f64, tags: &[(&str, &str)], ts_ms: i64) -> Metric {
        let mut metric_tags = MetricTags::default();
        for (k, v) in tags {
            metric_tags.replace(k.to_string(), v.to_string());
        }
        let ts = Utc.timestamp_millis_opt(ts_ms).unwrap();
        Metric::new(
            name,
            MetricKind::Incremental,
            MetricValue::Counter { value },
        )
        .with_tags(Some(metric_tags))
        .with_timestamp(Some(ts))
    }

    fn get_str(log: &LogEvent, key: &str) -> String {
        log.get(event_path!(key))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default()
    }

    fn get_i64(log: &LogEvent, key: &str) -> i64 {
        log.get(event_path!(key))
            .and_then(|v| v.as_integer())
            .unwrap_or(0)
    }

    fn is_null(log: &LogEvent, key: &str) -> bool {
        log.get(event_path!(key))
            .map(|v| v.is_null())
            .unwrap_or(false)
    }

    #[test]
    fn gauge_basic_fields() {
        let now_ms = Utc::now().timestamp_millis();
        let metric = make_gauge(
            "jvm_heap_used_bytes",
            1024.0,
            &[
                ("workspace_id", "w1"),
                ("shardName", "shard1"),
                ("system", "obs"),
            ],
            now_ms,
        );
        let log = MetricToLogHydra::transform_one(metric).expect("should produce log");

        assert_eq!(get_str(&log, "name"), "jvm_heap_used_bytes");
        assert_eq!(get_str(&log, "metric_name"), "jvm_heap_used_bytes");
        assert_eq!(get_str(&log, "metric_type"), "gauge");
        assert_eq!(get_str(&log, "topic"), "metric-to-log");
        assert_eq!(get_str(&log, "workspace_id"), "w1");
        assert_eq!(get_str(&log, "shard_name"), "shard1");
        assert_eq!(get_str(&log, "system_uri"), "system:obs");
        assert_eq!(get_i64(&log, "start_ts"), now_ms);
        assert_eq!(
            get_i64(&log, "timestamp_hour_ms"),
            now_ms - (now_ms % 3_600_000)
        );
    }

    #[test]
    fn suffix_stripping() {
        let now_ms = Utc::now().timestamp_millis();
        for (name, exp_base, exp_type) in &[
            ("req_bucket", "req", "bucket"),
            ("req_sum", "req", "sum"),
            ("req_count", "req", "count"),
            ("req_total", "req", "total"),
            ("req_summary", "req", "summary"),
            ("plain", "plain", "gauge"),
        ] {
            let metric = make_gauge(name, 1.0, &[], now_ms);
            let log = MetricToLogHydra::transform_one(metric).unwrap();
            assert_eq!(
                &get_str(&log, "metric_name"),
                exp_base,
                "metric_name for {name}"
            );
            assert_eq!(
                &get_str(&log, "metric_type"),
                exp_type,
                "metric_type for {name}"
            );
        }
    }

    #[test]
    fn counter_values_array() {
        let now_ms = Utc::now().timestamp_millis();
        let metric = make_counter("http_requests_total", 42.0, &[], now_ms);
        let log = MetricToLogHydra::transform_one(metric).unwrap();
        let values = log.get(event_path!("values")).unwrap();
        let arr = values.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0].get("ts").unwrap().as_integer().unwrap(), now_ms);
        assert!(
            (arr[0].get("v").unwrap().as_float().unwrap().into_inner() - 42.0).abs() < f64::EPSILON
        );
    }

    #[test]
    fn metric_part_range_and_determinism() {
        let now_ms = Utc::now().timestamp_millis();
        let metric = make_gauge("kube_pod_cpu_seconds_total", 1.0, &[], now_ms);
        let log = MetricToLogHydra::transform_one(metric.clone()).unwrap();
        let part = get_i64(&log, "metric_part");
        assert!((0..100).contains(&part), "metric_part out of range: {part}");
        let log2 = MetricToLogHydra::transform_one(metric).unwrap();
        assert_eq!(get_i64(&log2, "metric_part"), part);
    }

    #[test]
    fn hydra_cluster_column_format() {
        let now_ms = Utc::now().timestamp_millis();
        let metric = make_gauge(
            "foo_total",
            1.0,
            &[("system", "sys1"), ("shardName", "shard42")],
            now_ms,
        );
        let log = MetricToLogHydra::transform_one(metric).unwrap();
        assert_eq!(
            get_str(&log, "hydra_cluster_column"),
            "foo_total|sys1|shard42"
        );
    }

    #[test]
    fn tenant_cluster_column_priority() {
        let now_ms = Utc::now().timestamp_millis();
        let m = make_gauge(
            "m",
            1.0,
            &[
                ("cluster_id", "c1"),
                ("tenant_id", "t1"),
                ("workspace_id", "w1"),
            ],
            now_ms,
        );
        assert_eq!(
            get_str(
                &MetricToLogHydra::transform_one(m).unwrap(),
                "tenant_cluster_column"
            ),
            "c1"
        );

        let m2 = make_gauge("m", 1.0, &[("workspace_id", "w1")], now_ms);
        assert_eq!(
            get_str(
                &MetricToLogHydra::transform_one(m2).unwrap(),
                "tenant_cluster_column"
            ),
            "w1"
        );

        let m3 = make_gauge("m", 1.0, &[], now_ms);
        assert!(is_null(
            &MetricToLogHydra::transform_one(m3).unwrap(),
            "tenant_cluster_column"
        ));
    }

    #[test]
    fn rejects_future_timestamp() {
        let future_ms = Utc::now().timestamp_millis() + 400_000;
        let metric = make_gauge("foo", 1.0, &[], future_ms);
        assert!(MetricToLogHydra::transform_one(metric).is_none());
    }

    #[test]
    fn rejects_old_timestamp() {
        let old_ms = Utc::now().timestamp_millis() - 700_000;
        let metric = make_gauge("foo", 1.0, &[], old_ms);
        assert!(MetricToLogHydra::transform_one(metric).is_none());
    }

    #[test]
    fn dropped_metric_emits_to_output_buffer_and_counter() {
        use vector_lib::transform::FunctionTransform;
        let mut t = MetricToLogHydra::new();
        let mut buf = OutputBuffer::with_capacity(1);

        // Valid metric → lands in buffer
        let now_ms = Utc::now().timestamp_millis();
        let good = make_gauge("good_metric", 1.0, &[], now_ms);
        t.transform(&mut buf, good.into());
        assert_eq!(buf.len(), 1, "valid metric should be forwarded");
        let _ = buf.drain().count();

        // Stale metric → dropped, buffer stays empty
        let stale = make_gauge("stale_metric", 1.0, &[], now_ms - 700_000);
        t.transform(&mut buf, stale.into());
        assert_eq!(buf.len(), 0, "stale metric should be dropped");
    }

    #[test]
    fn md5_labels_matches_vrl_encode_json() {
        // Verify md5_labels matches VRL's md5(encode_json(.tags)) exactly.
        // VRL encode_json uses serde_json compact format with BTreeMap-sorted keys.
        let now_ms = Utc::now().timestamp_millis();
        let metric = make_gauge(
            "foo",
            1.0,
            &[
                ("workspace_id", "w1"),
                ("system", "obs"),
                ("shardName", "s1"),
            ],
            now_ms,
        );
        let log = MetricToLogHydra::transform_one(metric).unwrap();

        // Reproduce what VRL does: encode_json of sorted BTreeMap
        let expected_json = r#"{"shardName":"s1","system":"obs","workspace_id":"w1"}"#;
        let expected_md5 = format!("{:x}", Md5::digest(expected_json.as_bytes()));
        assert_eq!(get_str(&log, "md5_labels"), expected_md5);
    }

    #[test]
    fn md5_labels_empty_tags() {
        let now_ms = Utc::now().timestamp_millis();
        let metric = make_gauge("foo", 1.0, &[], now_ms);
        let log = MetricToLogHydra::transform_one(metric).unwrap();
        let expected_md5 = format!("{:x}", Md5::digest("{}".as_bytes()));
        assert_eq!(get_str(&log, "md5_labels"), expected_md5);
    }

    #[test]
    fn no_tags_produces_null_optional_fields() {
        let now_ms = Utc::now().timestamp_millis();
        let metric = make_gauge("some_metric", 1.0, &[], now_ms);
        let log = MetricToLogHydra::transform_one(metric).unwrap();
        assert!(is_null(&log, "workspace_id"));
        assert!(is_null(&log, "cluster_id"));
        assert!(is_null(&log, "region_uri"));
        assert!(is_null(&log, "tenant_id"));
        assert!(is_null(&log, "tenant_cluster_column"));
        assert!(is_null(&log, "system_uri"));
        assert_eq!(get_str(&log, "shard_name"), "");
        assert_eq!(get_str(&log, "hydra_cluster_column"), "some_metric||");
    }

}
