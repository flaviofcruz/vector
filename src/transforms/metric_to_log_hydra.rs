use chrono::Utc;
use md5::{Digest, Md5};
use serde_json::Value as JsonValue;
use vector_lib::{
    configurable::configurable_component,
    internal_event::{InternalEventHandle as _, Registered},
    lookup::event_path,
};
use vrl::value::{KeyString, Value};

use crate::{
    config::{
        DataType, GenerateConfig, Input, OutputId, TransformConfig, TransformContext,
        TransformOutput,
    },
    event::{Event, LogEvent, Metric, MetricValue},
    internal_events::{MetricToLogHydraDrop, MetricToLogHydraEventsDropped},
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
/// - Every drop is reported as a [`DropReason`] and recorded on the per-reason breakdown metric
///   `metric_to_log_hydra_dropped_total{reason="..."}`. Reasons that count as loss
///   ([`DropReason::counts_as_loss`]) — an invalid counter value, or a timestamp outside the
///   window — also increment `component_discarded_events_total{intentional="true"}`. An invalid
///   (NaN/±Inf) *gauge* value is a legitimate staleness marker, not loss: it is recorded on the
///   breakdown metric but is kept off the discard/completeness metric.
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

/// Accepted timestamp window relative to now: +5 min into the future, −10 min into the past.
const TS_WINDOW_FUTURE_MS: i64 = 300_000;
const TS_WINDOW_PAST_MS: i64 = 600_000;

/// Why `transform_one` dropped a metric instead of producing a log event.
///
/// Extensible: add a variant here plus its arm in [`DropReason::as_str`] to introduce a new
/// reason. Every drop path in `transform_one` names one of these, so a drop can never be
/// unclassified. Each reason carries its loss policy ([`DropReason::counts_as_loss`]): every drop
/// is recorded on the per-reason breakdown metric, but only "loss" reasons also increment the
/// standard discard metric that the completeness/SLO dashboards consume.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropReason {
    /// Gauge value is non-finite (NaN or ±Inf) — a legitimate Prometheus staleness marker
    /// (absent scrape, recording-rule gap), not data loss.
    InvalidGaugeValue,
    /// Counter value is non-finite (NaN or ±Inf) — invalid for a monotonic counter.
    InvalidCounterValue,
    /// Metric timestamp is outside the accepted window (−10 min / +5 min from now).
    TimestampOutOfWindow,
}

impl DropReason {
    /// Stable, low-cardinality value for the `reason` metric label and the log line.
    pub const fn as_str(self) -> &'static str {
        match self {
            DropReason::InvalidGaugeValue => "invalid_gauge_value",
            DropReason::InvalidCounterValue => "invalid_counter_value",
            DropReason::TimestampOutOfWindow => "timestamp_out_of_window",
        }
    }

    /// Whether a drop for this reason counts as data loss — i.e. whether it increments the
    /// standard `component_discarded_events_total{intentional="true"}` metric the completeness/SLO
    /// dashboards consume. An invalid gauge value is a legitimate Prometheus staleness marker, not
    /// loss, so it does NOT (but it is still recorded on the per-reason breakdown metric so it
    /// stays observable). Every other reason counts as loss.
    pub const fn counts_as_loss(self) -> bool {
        !matches!(self, DropReason::InvalidGaugeValue)
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

    /// Consume a `Metric` and produce a `LogEvent`, or the [`DropReason`] it was dropped for.
    ///
    /// `transform_one` never decides how a drop is accounted — it always reports the reason and
    /// leaves the loss policy to the caller (via [`DropReason::counts_as_loss`]).
    pub fn transform_one(metric: Metric) -> Result<LogEvent, DropReason> {
        let now_ms = Utc::now().timestamp_millis();

        let timestamp_ms: i64 = metric
            .timestamp()
            .map(|ts| ts.timestamp_millis())
            .unwrap_or(now_ms);

        // Timestamp window is checked before the value: an out-of-window drop is reported as
        // TimestampOutOfWindow regardless of the value (so a stale gauge is never mistaken for a
        // InvalidGaugeValue drop).
        if timestamp_ms > now_ms + TS_WINDOW_FUTURE_MS || timestamp_ms < now_ms - TS_WINDOW_PAST_MS
        {
            return Err(DropReason::TimestampOutOfWindow);
        }

        let timestamp_hour_ms = timestamp_ms - (timestamp_ms % 3_600_000);

        // Values array [{ts, v}] — only Gauge and Counter carry a scalar value. Both families are
        // handled identically: pair the value with the reason to report if it is non-finite, then
        // one shared finite-check + build. A non-finite value (NaN or ±Inf) is invalid — `NotNan`
        // (which `Value::Float` wraps) can represent ±Inf but not NaN, so `is_finite()` is checked
        // explicitly to reject both uniformly. Other metric types carry no scalar → empty array.
        let scalar = match metric.value() {
            MetricValue::Gauge { value } => Some((*value, DropReason::InvalidGaugeValue)),
            MetricValue::Counter { value } => Some((*value, DropReason::InvalidCounterValue)),
            _ => None,
        };
        let values: Value = match scalar {
            Some((value, invalid_reason)) => {
                if !value.is_finite() {
                    return Err(invalid_reason);
                }
                let v = value
                    .try_into()
                    .expect("value checked finite above, NotNan cannot fail");
                Value::Array(vec![Value::Object(
                    [
                        (KeyString::from("ts"), Value::Integer(timestamp_ms)),
                        (KeyString::from("v"), Value::Float(v)),
                    ]
                    .into(),
                )])
            }
            None => Value::Array(vec![]),
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

        Ok(log)
    }
}

impl FunctionTransform for MetricToLogHydra {
    fn transform(&mut self, output: &mut OutputBuffer, event: Event) {
        match Self::transform_one(event.into_metric()) {
            Ok(log) => output.push(log.into()),
            // Every drop is recorded on the per-reason breakdown; whether it also counts as loss
            // (the standard discard metric) is decided by the reason. FunctionTransform handles one
            // event per call, so a drop here is a single event.
            Err(reason) => self.events_dropped.emit(MetricToLogHydraDrop {
                count: 1,
                reason: reason.as_str(),
                counts_as_loss: reason.counts_as_loss(),
            }),
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

    // Test helpers over the Result<LogEvent, DropReason> return.
    fn logof(m: Metric) -> LogEvent {
        MetricToLogHydra::transform_one(m).expect("expected a log event, got a drop")
    }
    fn drop_reason(m: Metric) -> DropReason {
        MetricToLogHydra::transform_one(m).expect_err("expected a drop, got a log event")
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
        let log = logof(metric);

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
            let log = logof(metric);
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
        let log = logof(metric);
        let values = log.get(event_path!("values")).unwrap();
        let arr = values.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0].get("ts").unwrap().as_integer().unwrap(), now_ms);
        assert!(
            (arr[0].get("v").unwrap().as_float().unwrap().into_inner() - 42.0).abs() < f64::EPSILON
        );
    }

    // Returns values[0].v as f64, asserting exactly one sample is present.
    fn single_value(log: &LogEvent) -> f64 {
        let arr = log
            .get(event_path!("values"))
            .and_then(|v| v.as_array())
            .expect("values array");
        assert_eq!(arr.len(), 1, "expected exactly one sample");
        arr[0].get("v").unwrap().as_float().unwrap().into_inner()
    }

    #[test]
    fn invalid_gauge_value_is_dropped_but_not_loss() {
        // A gauge with a non-finite value (NaN or ±Inf) is a Prometheus staleness marker: reported
        // as InvalidGaugeValue and dropped, but it does NOT count as loss (recorded only on the
        // per-reason breakdown metric, not the discard/completeness metric).
        let now_ms = Utc::now().timestamp_millis();
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let reason = drop_reason(make_gauge("g", value, &[], now_ms));
            assert_eq!(reason, DropReason::InvalidGaugeValue, "value {value}");
            assert!(
                !reason.counts_as_loss(),
                "invalid gauge must not count as loss"
            );
        }
    }

    #[test]
    fn invalid_counter_value_is_dropped_and_counts_as_loss() {
        // Counter is handled symmetrically with gauge: any non-finite value (NaN or ±Inf) is
        // invalid → dropped as InvalidCounterValue, which counts as loss.
        let now_ms = Utc::now().timestamp_millis();
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let reason = drop_reason(make_counter("c", value, &[], now_ms));
            assert_eq!(reason, DropReason::InvalidCounterValue, "value {value}");
            assert!(
                reason.counts_as_loss(),
                "invalid counter must count as loss"
            );
        }
    }

    #[test]
    fn drop_reason_labels_are_stable() {
        // The metric `reason` label values must stay stable (dashboards/alerts key on them).
        assert_eq!(
            DropReason::InvalidGaugeValue.as_str(),
            "invalid_gauge_value"
        );
        assert_eq!(
            DropReason::InvalidCounterValue.as_str(),
            "invalid_counter_value"
        );
        assert_eq!(
            DropReason::TimestampOutOfWindow.as_str(),
            "timestamp_out_of_window"
        );
    }

    #[test]
    fn finite_gauge_value_is_preserved() {
        let now_ms = Utc::now().timestamp_millis();
        let log = logof(make_gauge("g", 123.4, &[], now_ms));
        assert!((single_value(&log) - 123.4).abs() < f64::EPSILON);
    }

    #[test]
    fn metric_part_range_and_determinism() {
        let now_ms = Utc::now().timestamp_millis();
        let metric = make_gauge("kube_pod_cpu_seconds_total", 1.0, &[], now_ms);
        let log = logof(metric.clone());
        let part = get_i64(&log, "metric_part");
        assert!((0..100).contains(&part), "metric_part out of range: {part}");
        let log2 = logof(metric);
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
        let log = logof(metric);
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
        assert_eq!(get_str(&logof(m), "tenant_cluster_column"), "c1");

        let m2 = make_gauge("m", 1.0, &[("workspace_id", "w1")], now_ms);
        assert_eq!(get_str(&logof(m2), "tenant_cluster_column"), "w1");

        let m3 = make_gauge("m", 1.0, &[], now_ms);
        assert!(is_null(&logof(m3), "tenant_cluster_column"));
    }

    #[test]
    fn rejects_future_timestamp() {
        let future_ms = Utc::now().timestamp_millis() + 400_000;
        // Out-of-window even for a gauge (checked before the value); counts as loss.
        let reason = drop_reason(make_gauge("foo", 1.0, &[], future_ms));
        assert_eq!(reason, DropReason::TimestampOutOfWindow);
        assert!(reason.counts_as_loss());
    }

    #[test]
    fn rejects_old_timestamp() {
        let old_ms = Utc::now().timestamp_millis() - 700_000;
        assert_eq!(
            drop_reason(make_gauge("foo", 1.0, &[], old_ms)),
            DropReason::TimestampOutOfWindow
        );
    }

    #[test]
    fn stale_invalid_gauge_reports_timestamp_and_counts_as_loss() {
        // A gauge that is BOTH stale and non-finite must report TimestampOutOfWindow (which counts
        // as loss), not InvalidGaugeValue — guards against deciding the reason from the value alone.
        let old_ms = Utc::now().timestamp_millis() - 700_000;
        let reason = drop_reason(make_gauge("foo", f64::NAN, &[], old_ms));
        assert_eq!(reason, DropReason::TimestampOutOfWindow);
        assert!(
            reason.counts_as_loss(),
            "stale drop counts as loss, unlike a well-timed invalid gauge"
        );
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

        // Non-finite gauge and counter (NaN or ±Inf) → dropped, buffer stays empty.
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            t.transform(&mut buf, make_gauge("nf_gauge", value, &[], now_ms).into());
            assert_eq!(buf.len(), 0, "non-finite gauge {value} should be dropped");
            t.transform(
                &mut buf,
                make_counter("nf_counter", value, &[], now_ms).into(),
            );
            assert_eq!(buf.len(), 0, "non-finite counter {value} should be dropped");
        }
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
        let log = logof(metric);

        // Reproduce what VRL does: encode_json of sorted BTreeMap
        let expected_json = r#"{"shardName":"s1","system":"obs","workspace_id":"w1"}"#;
        let expected_md5 = format!("{:x}", Md5::digest(expected_json.as_bytes()));
        assert_eq!(get_str(&log, "md5_labels"), expected_md5);
    }

    #[test]
    fn md5_labels_empty_tags() {
        let now_ms = Utc::now().timestamp_millis();
        let metric = make_gauge("foo", 1.0, &[], now_ms);
        let log = logof(metric);
        let expected_md5 = format!("{:x}", Md5::digest("{}".as_bytes()));
        assert_eq!(get_str(&log, "md5_labels"), expected_md5);
    }

    #[test]
    fn no_tags_produces_null_optional_fields() {
        let now_ms = Utc::now().timestamp_millis();
        let metric = make_gauge("some_metric", 1.0, &[], now_ms);
        let log = logof(metric);
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
