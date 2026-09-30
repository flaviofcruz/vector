use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Utc};
use md5::{Digest, Md5};
use serde::{Serialize, Serializer};
use vector_lib::{
    configurable::configurable_component,
    lookup::event_path,
    transform::{SyncTransform, TransformOutputsBuf},
};
use vrl::value::{KeyString, Value};

use crate::{
    config::{
        DataType, GenerateConfig, Input, OutputId, TransformConfig, TransformContext,
        TransformOutput,
    },
    event::{Event, LogEvent, Metric, MetricTags, MetricValue},
    internal_events::MetricToLogHydraDropped,
    schema::Definition,
    transforms::Transform,
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
/// - Drops are counted on `metric_to_log_hydra_dropped_total{reason}`. Non-finite (NaN/±Inf)
///   values are staleness markers, not loss; only out-of-window timestamps count as loss (see
///   [`DropReason::counts_as_loss`]).
///
/// **Do not use for new pipelines** — this encodes Hydra-specific field semantics.
/// Use `metric_to_log` + VRL remap for a different output shape.
#[configurable_component(transform(
    "metric_to_log_hydra",
    "Convert Prometheus metrics to Hydra log events natively."
))]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct MetricToLogHydraConfig {
    /// Optional native pod routing. When set, the transform is multi-output: each event goes to the
    /// named port(s) of its `system`'s pod(s), replacing the downstream `route` transform.
    #[serde(default)]
    pod_routing: Option<PodRoutingConfig>,
}

/// One pod-routing rule: a `system` (or `"*"` catch-all) → the pod(s) it routes to.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct SystemPodRoute {
    /// System to match (e.g. `lakebase-storage`), or `"*"` for the catch-all. Exactly one `"*"` required.
    pub system: String,
    /// Pods to write to; each is an output port. List several to fan out (e.g. migration). Non-empty.
    pub pods: Vec<String>,
}

/// Native pod routing: a `system -> pods` table from `pod_config.jsonnet`'s `system_pod_mapping`.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct PodRoutingConfig {
    /// Routing rules; exactly one must use `system = "*"` (the catch-all).
    pub systems: Vec<SystemPodRoute>,
}

impl GenerateConfig for MetricToLogHydraConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self::default()).unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "metric_to_log_hydra")]
impl TransformConfig for MetricToLogHydraConfig {
    async fn build(&self, _context: &TransformContext) -> crate::Result<Transform> {
        let routing = match &self.pod_routing {
            Some(cfg) => Some(Arc::new(PodRouting::compile(cfg)?)),
            None => None,
        };
        Ok(Transform::synchronous(MetricToLogHydra::with_routing(routing)))
    }

    fn input(&self) -> Input {
        Input::metric()
    }

    /// Without `pod_routing`: one default output (byte-identical to the legacy single-sink path).
    /// With `pod_routing`: one named output per distinct pod across all rules (including the `"*"`
    /// catch-all's pods), so downstream sinks bind to `…hydra_transformed.<pod_id>` and no `route`
    /// transform is needed.
    fn outputs(
        &self,
        _context: &TransformContext,
        input_definitions: &[(OutputId, Definition)],
    ) -> Vec<TransformOutput> {
        let defs = || -> std::collections::HashMap<OutputId, Definition> {
            input_definitions
                .iter()
                .map(|(id, _)| (id.clone(), Definition::default_legacy_namespace()))
                .collect()
        };
        match &self.pod_routing {
            None => vec![TransformOutput::new(DataType::Log, defs())],
            Some(cfg) => {
                let mut pods: Vec<&str> = cfg
                    .systems
                    .iter()
                    .flat_map(|s| s.pods.iter().map(String::as_str))
                    .collect();
                pods.sort_unstable();
                pods.dedup();
                pods.into_iter()
                    .map(|p| TransformOutput::new(DataType::Log, defs()).with_port(p))
                    .collect()
            }
        }
    }

    fn enable_concurrency(&self) -> bool {
        true
    }
}

/// Accepted timestamp window relative to now: +5 min into the future, −10 min into the past.
const TS_WINDOW_FUTURE_MS: i64 = 300_000;
const TS_WINDOW_PAST_MS: i64 = 600_000;

/// Why `transform_one` dropped a metric. The `reason` label on `metric_to_log_hydra_dropped_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropReason {
    /// Non-finite (NaN/±Inf) value — a staleness marker; gauge or counter.
    NonFiniteValue,
    /// Timestamp outside the accepted window (−10 min / +5 min).
    TimestampOutOfWindow,
}

impl DropReason {
    /// Stable, low-cardinality `reason` label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            DropReason::NonFiniteValue => "non_finite_value",
            DropReason::TimestampOutOfWindow => "timestamp_out_of_window",
        }
    }

    /// Whether the drop also increments the standard `component_discarded_events_total` discard
    /// metric. Non-finite values are staleness markers (not loss); an out-of-window timestamp is a
    /// rejected sample (loss).
    pub const fn counts_as_loss(self) -> bool {
        matches!(self, DropReason::TimestampOutOfWindow)
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

/// The reserved `system` value marking the catch-all rule.
const CATCH_ALL_SYSTEM: &str = "*";

/// Compiled routing table: O(1) exact-match on the event's `system_uri`, with the `"*"` rule lifted
/// into `default`. Built once at config time, shared across instances via `Arc`. Keys are the
/// event-side `system:<name>` form, so config `system` values are prefixed at compile time.
struct PodRouting {
    /// Pods for any `system_uri` not in `by_system_uri` (the `"*"` rule). Always non-empty.
    default: Vec<String>,
    by_system_uri: HashMap<String, Vec<String>>,
}

impl PodRouting {
    fn compile(cfg: &PodRoutingConfig) -> crate::Result<Self> {
        let mut default: Option<Vec<String>> = None;
        let mut by_system_uri: HashMap<String, Vec<String>> = HashMap::new();
        for r in &cfg.systems {
            if r.pods.is_empty() {
                return Err(format!(
                    "metric_to_log_hydra pod_routing: rule `{}` lists no pods",
                    r.system
                )
                .into());
            }
            if r.system == CATCH_ALL_SYSTEM {
                if default.replace(r.pods.clone()).is_some() {
                    return Err("metric_to_log_hydra pod_routing: more than one `*` catch-all rule"
                        .into());
                }
            } else if by_system_uri
                .insert(format!("system:{}", r.system), r.pods.clone())
                .is_some()
            {
                return Err(format!(
                    "metric_to_log_hydra pod_routing: duplicate rule for `{}`",
                    r.system
                )
                .into());
            }
        }
        let default = default.ok_or(
            "metric_to_log_hydra pod_routing: a `*` catch-all rule is required",
        )?;
        Ok(Self {
            default,
            by_system_uri,
        })
    }

    /// Resolve the pods for an event's `system_uri`; unmatched (or absent) → the `"*"` default.
    /// Borrows only into `self`, never the event, so the log's read borrow can end before it moves.
    /// The returned slice is always non-empty.
    #[inline]
    fn decide(&self, system_uri: Option<&str>) -> &[String] {
        system_uri
            .and_then(|u| self.by_system_uri.get(u))
            .unwrap_or(&self.default)
    }
}

#[derive(Clone, Default)]
pub struct MetricToLogHydra {
    /// `None` = single default output (legacy; a downstream `route`/sink consumes it). `Some` =
    /// multi-output: emit each event to its pod's named port.
    routing: Option<Arc<PodRouting>>,
}

/// Serializes tags as a sorted JSON object from `iter_single()` (MetricTags is BTreeMap-backed, so
/// already key-sorted) with no intermediate map. Matches VRL's `encode_json(.tags)`; `None` → `{}`.
struct TagsJson<'a>(Option<&'a MetricTags>);

impl Serialize for TagsJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            Some(tags) => serializer.collect_map(tags.iter_single()),
            None => serializer.collect_map(std::iter::empty::<(&str, &str)>()),
        }
    }
}

/// `io::Write` that feeds bytes into an `Md5` hasher, so tag JSON is hashed as it serializes.
struct HashWriter<'a>(&'a mut Md5);

impl std::io::Write for HashWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl MetricToLogHydra {
    pub fn new() -> Self {
        Self::default()
    }

    fn with_routing(routing: Option<Arc<PodRouting>>) -> Self {
        Self { routing }
    }

    /// Construct with native pod routing (compiles + validates the config once). Public for
    /// benchmarks/tests; the topology builds via [`MetricToLogHydraConfig::build`].
    pub fn with_pod_routing(config: &PodRoutingConfig) -> crate::Result<Self> {
        Ok(Self::with_routing(Some(Arc::new(PodRouting::compile(
            config,
        )?))))
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

    /// `metric_part`: first 4 md5 bytes as big-endian u32, mod 100 (== first 8 hex chars, mod 100).
    fn metric_part(metric_name: &str) -> i64 {
        let digest = Md5::digest(metric_name.as_bytes());
        (u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]) as i64) % 100
    }

    /// Consume a `Metric` and produce a `LogEvent`, or the [`DropReason`] it was dropped for.
    pub fn transform_one(metric: Metric) -> Result<LogEvent, DropReason> {
        let now_ms = Utc::now().timestamp_millis();
        let (series, data, metadata) = metric.into_parts();

        let timestamp_ms: i64 = data
            .time
            .timestamp
            .map(|ts| ts.timestamp_millis())
            .unwrap_or(now_ms);

        // Checked before the value: out-of-window always reports TimestampOutOfWindow.
        if timestamp_ms > now_ms + TS_WINDOW_FUTURE_MS || timestamp_ms < now_ms - TS_WINDOW_PAST_MS
        {
            return Err(DropReason::TimestampOutOfWindow);
        }

        let timestamp_hour_ms = timestamp_ms - (timestamp_ms % 3_600_000);

        // Only Gauge/Counter carry a scalar; both handled the same. Non-finite (NaN/±Inf) → drop as
        // NonFiniteValue (explicit is_finite: NotNan accepts ±Inf but not NaN). Others → [].
        let values: Value = match data.value {
            MetricValue::Gauge { value } | MetricValue::Counter { value } => {
                if !value.is_finite() {
                    return Err(DropReason::NonFiniteValue);
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
            _ => Value::Array(vec![]),
        };

        // Own name + tags so their buffers move into the output.
        let tags = series.tags;
        let original_name: String = series.name.name;
        let (metric_name, metric_type) = {
            let (base, t) = Self::parse_metric_name(&original_name);
            (base.to_owned(), t)
        };
        let part = Self::metric_part(&metric_name);

        // md5_labels == md5(encode_json(.tags)); stream the sorted tag JSON into the hasher.
        let md5_labels = {
            let mut hasher = Md5::new();
            serde_json::to_writer(HashWriter(&mut hasher), &TagsJson(tags.as_ref())).ok();
            format!("{:x}", hasher.finalize())
        };

        // Promoted columns are also emitted as top-level fields; `get` is a keyed lookup, no scan.
        let get_tag = |key: &str| tags.as_ref().and_then(|t| t.get(key)).map(String::from);
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
            .or(endpoint_name)
            .or_else(|| tenant_id.clone())
            .or_else(|| workspace_id.clone())
            .map(|s| Value::Bytes(s.into()))
            .unwrap_or(Value::Null);

        let system_uri: Value = if !system_label.is_empty() {
            Value::Bytes(format!("system:{}", system_label).into())
        } else {
            Value::Null
        };

        // Nested labels object; tag buffers moved in, not copied.
        let labels: Value = Value::Object(
            tags.map(|t| {
                t.into_iter_single()
                    .map(|(k, v)| (KeyString::from(k), Value::Bytes(v.into())))
                    .collect()
            })
            .unwrap_or_default(),
        );

        // Delta/zerobus-shaped fields, so the pod_prime → databricks_zerobus path needs no
        // per-event remap. `_timestamp_hour` is a NEW Timestamp field (not a mutation of the
        // integer `timestamp_hour_ms`, which S3 consumers still read as-is). `_event_time` derives
        // from `start_ts`; `_partition_date` is its UTC date; `_system`/`_shard_name` mirror the
        // promoted `system`/`shardName` tags under the table's column names.
        let event_time = DateTime::<Utc>::from_timestamp_millis(timestamp_ms).unwrap_or_default();
        let timestamp_hour = DateTime::<Utc>::from_timestamp_millis(timestamp_hour_ms)
            .unwrap_or_default();
        let partition_date = event_time.format("%Y-%m-%d").to_string();

        let mut log = LogEvent::new_with_metadata(metadata);

        log.insert(event_path!("_event_time"), Value::Timestamp(event_time));
        log.insert(event_path!("_timestamp_hour"), Value::Timestamp(timestamp_hour));
        log.insert(
            event_path!("_partition_date"),
            Value::Bytes(partition_date.into()),
        );
        log.insert(event_path!("_system"), Value::Bytes(system_label.into()));
        log.insert(
            event_path!("_shard_name"),
            Value::Bytes(shard_name.clone().into()),
        );

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

impl SyncTransform for MetricToLogHydra {
    fn transform(&mut self, event: Event, output: &mut TransformOutputsBuf) {
        let log = match Self::transform_one(event.into_metric()) {
            Ok(log) => log,
            // Breakdown metric for every drop; standard discard only for loss reasons.
            Err(reason) => {
                emit!(MetricToLogHydraDropped {
                    reason: reason.as_str(),
                    counts_as_loss: reason.counts_as_loss(),
                });
                return;
            }
        };

        let Some(routing) = &self.routing else {
            // No routing: single default output. Downstream reads it with no port suffix, exactly
            // as before — a following `route`/`filter` transform (or single sink) consumes it.
            output.push(None, log.into());
            return;
        };

        // Route on `system_uri` (set by `transform_one`). `decide` borrows only into `routing`, not
        // the log, so the log's read borrow ends with this block — before the log is moved below.
        let pods = {
            let system_uri = log.get(event_path!("system_uri")).and_then(|v| v.as_str());
            routing.decide(system_uri.as_deref())
        };
        // A pod's name IS its output port. Fan out by cloning for every target but the last and
        // MOVING into the last — so the common single-pod case is zero clones and no VRL. `pods` is
        // guaranteed non-empty by `PodRouting::compile`.
        if let Some((last, rest)) = pods.split_last() {
            for pod in rest {
                output.push(Some(pod), log.clone().into());
            }
            output.push(Some(last), log.into());
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
    fn emits_delta_shaped_fields() {
        let now_ms = Utc::now().timestamp_millis();
        let hour_ms = now_ms - (now_ms % 3_600_000);
        let log = logof(make_gauge(
            "m",
            1.0,
            &[("system", "lakebase-storage"), ("shardName", "shard-7")],
            now_ms,
        ));

        // _system / _shard_name mirror the promoted tags under the Delta column names.
        assert_eq!(get_str(&log, "_system"), "lakebase-storage");
        assert_eq!(get_str(&log, "_shard_name"), "shard-7");

        // timestamp_hour_ms stays an integer — the S3 contract is unchanged.
        assert_eq!(get_i64(&log, "timestamp_hour_ms"), hour_ms);

        // _event_time / _timestamp_hour are Timestamp-typed (for the Delta/zerobus path) and equal
        // the source millis.
        match log.get(event_path!("_event_time")) {
            Some(Value::Timestamp(t)) => assert_eq!(t.timestamp_millis(), now_ms),
            other => panic!("_event_time must be a Timestamp, got {other:?}"),
        }
        match log.get(event_path!("_timestamp_hour")) {
            Some(Value::Timestamp(t)) => assert_eq!(t.timestamp_millis(), hour_ms),
            other => panic!("_timestamp_hour must be a Timestamp, got {other:?}"),
        }

        // _partition_date is the UTC date of start_ts.
        let expected_date = DateTime::<Utc>::from_timestamp_millis(now_ms)
            .unwrap()
            .format("%Y-%m-%d")
            .to_string();
        assert_eq!(get_str(&log, "_partition_date"), expected_date);

        // Absent system/shard → empty strings (matches the old remap's `?? ""`).
        let log2 = logof(make_gauge("m", 1.0, &[], now_ms));
        assert_eq!(get_str(&log2, "_system"), "");
        assert_eq!(get_str(&log2, "_shard_name"), "");
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
    fn non_finite_gauge_and_counter_dropped_as_same_reason_not_loss() {
        // Gauge and counter are not distinguished: any non-finite value (NaN or ±Inf) on either is
        // reported as NonFiniteValue (a staleness marker) and does NOT count as loss.
        let now_ms = Utc::now().timestamp_millis();
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            for reason in [
                drop_reason(make_gauge("g", value, &[], now_ms)),
                drop_reason(make_counter("c", value, &[], now_ms)),
            ] {
                assert_eq!(reason, DropReason::NonFiniteValue, "value {value}");
                assert!(
                    !reason.counts_as_loss(),
                    "non-finite value must not be loss"
                );
            }
        }
    }

    #[test]
    fn drop_reason_labels_are_stable() {
        // The metric `reason` label values must stay stable (dashboards/alerts key on them).
        assert_eq!(DropReason::NonFiniteValue.as_str(), "non_finite_value");
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
    fn stale_and_non_finite_reports_timestamp_and_counts_as_loss() {
        // A metric that is BOTH stale and non-finite must report TimestampOutOfWindow (which counts
        // as loss), not NonFiniteValue — the timestamp window is checked before the value.
        let old_ms = Utc::now().timestamp_millis() - 700_000;
        let reason = drop_reason(make_gauge("foo", f64::NAN, &[], old_ms));
        assert_eq!(reason, DropReason::TimestampOutOfWindow);
        assert!(
            reason.counts_as_loss(),
            "stale drop counts as loss, unlike a well-timed non-finite value"
        );
    }

    #[test]
    fn dropped_metric_emits_to_output_and_counter() {
        let mut t = MetricToLogHydra::new();
        let now_ms = Utc::now().timestamp_millis();

        // Valid metric → one event on the default output.
        assert_eq!(
            run_default(&mut t, make_gauge("good_metric", 1.0, &[], now_ms)).len(),
            1,
            "valid metric should be forwarded"
        );
        // Stale metric → dropped.
        assert_eq!(
            run_default(&mut t, make_gauge("stale_metric", 1.0, &[], now_ms - 700_000)).len(),
            0,
            "stale metric should be dropped"
        );
        // Non-finite gauge and counter (NaN or ±Inf) → dropped.
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(
                run_default(&mut t, make_gauge("nf_gauge", value, &[], now_ms)).len(),
                0,
                "non-finite gauge {value} should be dropped"
            );
            assert_eq!(
                run_default(&mut t, make_counter("nf_counter", value, &[], now_ms)).len(),
                0,
                "non-finite counter {value} should be dropped"
            );
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

    // ── pod routing (named outputs) ────────────────────────────────────────────

    // Build a router from a `default_pod` (becomes the `"*"` rule) plus `(system_uri, pods)` rules.
    // Returns the transform and its declared output ports (every distinct pod).
    fn routing(default_pod: &str, systems: &[(&str, &[&str])]) -> (MetricToLogHydra, Vec<String>) {
        let cfg = pod_cfg(default_pod, systems);
        let mut ports: Vec<String> = cfg
            .systems
            .iter()
            .flat_map(|s| s.pods.iter().cloned())
            .collect();
        ports.sort();
        ports.dedup();
        (MetricToLogHydra::with_pod_routing(&cfg).unwrap(), ports)
    }

    fn pod_cfg(default_pod: &str, systems: &[(&str, &[&str])]) -> PodRoutingConfig {
        let mut rules: Vec<SystemPodRoute> = systems
            .iter()
            .map(|(system, pods)| SystemPodRoute {
                system: system.to_string(),
                pods: pods.iter().map(|p| p.to_string()).collect(),
            })
            .collect();
        rules.push(SystemPodRoute {
            system: CATCH_ALL_SYSTEM.to_string(),
            pods: vec![default_pod.to_string()],
        });
        PodRoutingConfig { systems: rules }
    }

    // Drive the SyncTransform; return the per-port emitted-event counts.
    fn run_ports(
        t: &mut MetricToLogHydra,
        ports: &[String],
        metric: Metric,
    ) -> std::collections::HashMap<String, usize> {
        let outputs = ports
            .iter()
            .map(|p| TransformOutput::new(DataType::Log, Default::default()).with_port(p.clone()))
            .collect();
        let mut buf = TransformOutputsBuf::new_with_capacity(outputs, 8);
        t.transform(Event::Metric(metric), &mut buf);
        ports
            .iter()
            .map(|p| (p.clone(), buf.drain_named(p).count()))
            .collect()
    }

    // Drive the no-routing transform; return the events on the single default output.
    fn run_default(t: &mut MetricToLogHydra, metric: Metric) -> Vec<LogEvent> {
        let outputs = vec![TransformOutput::new(DataType::Log, Default::default())];
        let mut buf = TransformOutputsBuf::new_with_capacity(outputs, 8);
        t.transform(Event::Metric(metric), &mut buf);
        buf.drain().map(|e| e.into_log()).collect()
    }

    #[test]
    fn no_routing_uses_default_output() {
        let now = Utc::now().timestamp_millis();
        let mut t = MetricToLogHydra::new();
        let out = run_default(&mut t, make_gauge("m", 1.0, &[("system", "obs")], now));
        assert_eq!(out.len(), 1);
        assert_eq!(get_str(&out[0], "system_uri"), "system:obs");
    }

    #[test]
    fn routes_matched_system_to_pod_port() {
        let now = Utc::now().timestamp_millis();
        let (mut t, ports) = routing("pod0", &[("lakebase-storage", &["poda"])]);
        let counts = run_ports(
            &mut t,
            &ports,
            make_gauge("m", 1.0, &[("system", "lakebase-storage")], now),
        );
        assert_eq!(counts["poda"], 1);
        assert_eq!(counts["pod0"], 0);
    }

    #[test]
    fn unmatched_system_falls_back_to_default_port() {
        let now = Utc::now().timestamp_millis();
        let (mut t, ports) = routing("pod0", &[("lakebase-storage", &["poda"])]);
        let counts = run_ports(&mut t, &ports, make_gauge("m", 1.0, &[("system", "obs")], now));
        assert_eq!(counts["pod0"], 1);
        assert_eq!(counts["poda"], 0);
    }

    #[test]
    fn absent_system_uri_uses_default_port() {
        let now = Utc::now().timestamp_millis();
        let (mut t, ports) = routing("pod0", &[("lakebase-storage", &["poda"])]);
        // No `system` tag → system_uri is null → default pod's port.
        let counts = run_ports(&mut t, &ports, make_gauge("m", 1.0, &[], now));
        assert_eq!(counts["pod0"], 1);
        assert_eq!(counts["poda"], 0);
    }

    #[test]
    fn multi_pod_system_fans_out_to_every_pod() {
        let now = Utc::now().timestamp_millis();
        // Migration / fan-out: one system → three pods. Each event emitted once per pod.
        let (mut t, ports) = routing(
            "pod0",
            &[("lakebase-storage", &["poda", "podb", "pod0"])],
        );
        let counts = run_ports(
            &mut t,
            &ports,
            make_gauge("m", 1.0, &[("system", "lakebase-storage")], now),
        );
        assert_eq!(counts["poda"], 1);
        assert_eq!(counts["podb"], 1);
        assert_eq!(counts["pod0"], 1);
    }

    #[test]
    fn compile_requires_catch_all_rule() {
        let cfg = PodRoutingConfig {
            systems: vec![SystemPodRoute {
                system: "lakebase-storage".to_string(),
                pods: vec!["poda".to_string()],
            }],
        };
        assert!(MetricToLogHydra::with_pod_routing(&cfg).is_err());
    }

    #[test]
    fn compile_rejects_duplicate_and_empty_rules() {
        // Duplicate exact system_uri.
        let dup = pod_cfg("pod0", &[("x", &["poda"]), ("x", &["podb"])]);
        assert!(MetricToLogHydra::with_pod_routing(&dup).is_err());

        // Rule with no pods.
        let empty = pod_cfg("pod0", &[("x", &[])]);
        assert!(MetricToLogHydra::with_pod_routing(&empty).is_err());
    }
}
