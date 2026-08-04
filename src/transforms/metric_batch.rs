//! `metric_batch` — generic, sharded batch-reduce of Prometheus metrics.
//!
//! Emits a **minimal, generic** log per group — only the raw metric identity plus the batched
//! samples:
//!
//! ```json
//! { "name": <metric name>, "labels": {<tags>}, "start_ts": <min ts ms>, "end_ts": <max ts ms>,
//!   "values": [{"ts": <ms>, "v": <f64>}, …] }
//! ```
//!
//! `start_ts`/`end_ts` are the min/max sample timestamps in the window (NOT the flush time). With
//! `shard_outputs = true`, each log also carries `<output_shard_label> = hash % output_shards`,
//! where `hash` is the SAME fnv-1a used for worker routing (no extra hashing) — so a downstream
//! `route` can fan groups across parallel derive transforms.
//!
//! It computes NO derived columns (metric_name/type split, md5 of labels, tenant/cluster columns,
//! …): any downstream shaping (e.g. the Hydra `metric_to_log` schema) is done by a following
//! transform (a VRL `remap` or a native one). Keeping the reduce generic trades a little pipeline
//! overhead for reusability across output schemas.
//!
//! Grouping/sharding/flush semantics: metrics are grouped by `(name, non-excluded tags)` over
//! `batch_period_ms`, optionally also by hour bucket (`split_by_time_hour_truncation`); events are
//! hash-routed to `worker_shards` independent worker tasks; flushes can be staggered
//! (`worker_flush_offset_ms`); idle groups are evicted after `group_cache_ttl_ms`.

use std::{
    collections::{BTreeMap, HashMap, HashSet, hash_map::Entry},
    pin::Pin,
    time::Instant,
};

use chrono::Utc;
use futures::{Stream, StreamExt};
use metrics::gauge;
use serde_json::Value as JsonValue;
use tokio::{
    select,
    sync::mpsc,
    time::{Duration, Instant as TokioInstant, interval_at},
};
use tokio_stream::wrappers::ReceiverStream;
use vector_lib::{
    configurable::configurable_component,
    lookup::{OwnedTargetPath, event_path, lookup_v2::parse_target_path},
};

use crate::{
    config::{
        DataType, GenerateConfig, Input, OutputId, TransformConfig, TransformContext,
        TransformOutput,
    },
    event::{Event, EventMetadata, LogEvent, Metric, MetricValue},
    schema::Definition,
    transforms::{TaskTransform, Transform},
};

/// Configuration for the `metric_batch` transform.
#[configurable_component(transform(
    "metric_batch",
    "Sharded native batch-reduce of Prometheus metrics into a generic {name, labels, start_ts, values} log."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct MetricBatchConfig {
    /// Batch window in milliseconds — each worker flushes its accumulated groups this often.
    pub batch_period_ms: u64,

    /// Number of independent shard workers (each on its own task + timer). Optional; defaults to 1.
    #[serde(default)]
    pub worker_shards: Option<usize>,

    /// Flush-stagger base (ms): all workers flush in lockstep when unset (or 0); = `batch_period_ms`
    /// spreads the workers' flushes evenly across one period. Worker `w` is offset earlier by
    /// `(worker_flush_offset_ms / worker_shards) * (w + 1)`. Optional; unset disables staggering.
    #[serde(default)]
    pub worker_flush_offset_ms: Option<u64>,

    /// Evict a group if no sample arrives for it within this long (ms). Default 120000 (2 min).
    #[serde(default = "default_group_cache_ttl_ms")]
    pub group_cache_ttl_ms: u64,

    /// How often (ms) each worker scans for and evicts idle groups. Default 30000.
    #[serde(default = "default_group_cache_clean_interval_ms")]
    pub group_cache_clean_interval_ms: u64,

    /// Tag keys excluded from the group key and the shard hash (still emitted in `labels`).
    #[serde(default)]
    pub exclude_tags: Vec<String>,

    /// When true, the hour bucket (`ts` truncated to the hour) is part of the group key, so samples
    /// that cross an hour boundary flush as separate groups (one log per series *per hour*). When
    /// false (the default), the hour is excluded from the key and all samples for a series in the
    /// window batch into a single group regardless of hour.
    #[serde(default)]
    pub split_by_time_hour_truncation: bool,

    /// When true, stamp each output log with `output_shard_label = hash % output_shards`, where
    /// `hash` is the same fnv-1a over `name + sorted non-excluded tags` already used for worker
    /// routing (no extra hashing). Lets a downstream `route` fan the load across parallel derive
    /// transforms. Default false. When true, `output_shards` and `output_shard_label` are required.
    #[serde(default)]
    pub shard_outputs: bool,

    /// Number of output shards to spread groups across. Required (and only used) when
    /// `shard_outputs` is true; ignored otherwise.
    #[serde(default)]
    pub output_shards: Option<u64>,

    /// Log field to receive the output shard index (e.g. `__output_shard__`). Required (and only
    /// used) when `shard_outputs` is true; ignored otherwise.
    #[serde(default)]
    pub output_shard_label: Option<String>,
}

const fn default_group_cache_ttl_ms() -> u64 {
    120_000
}
const fn default_group_cache_clean_interval_ms() -> u64 {
    30_000
}

impl GenerateConfig for MetricBatchConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            batch_period_ms: 120_000,
            worker_shards: None,
            worker_flush_offset_ms: None,
            group_cache_ttl_ms: default_group_cache_ttl_ms(),
            group_cache_clean_interval_ms: default_group_cache_clean_interval_ms(),
            exclude_tags: Vec::new(),
            split_by_time_hour_truncation: false,
            shard_outputs: false,
            output_shards: None,
            output_shard_label: None,
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "metric_batch")]
impl TransformConfig for MetricBatchConfig {
    async fn build(&self, _context: &TransformContext) -> crate::Result<Transform> {
        Ok(Transform::event_task(MetricBatch::new(self)?))
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
}

#[derive(Clone)]
pub struct MetricBatch {
    batch_period_ms: u64,
    worker_shards: usize,
    worker_flush_offset_ms: u64,
    group_cache_ttl: Duration,
    clean_interval: Duration,
    exclude: HashSet<String>,
    /// When true, the hour bucket is part of the group key (one group per series per hour); when
    /// false the hour is dropped from the key so a series batches into a single group.
    split_by_time_hour_truncation: bool,
    /// `Some((shards, path))` only when `shard_outputs` is enabled; `None` disables output sharding
    /// entirely (no field stamped, no path parsed).
    output_sharding: Option<(u64, OwnedTargetPath)>,
}

impl MetricBatch {
    pub fn new(config: &MetricBatchConfig) -> crate::Result<Self> {
        let output_sharding = if config.shard_outputs {
            let shards = config
                .output_shards
                .ok_or("output_shards is required when shard_outputs is true")?;
            if shards == 0 {
                return Err("output_shards must be >= 1".into());
            }
            let label = config
                .output_shard_label
                .as_deref()
                .ok_or("output_shard_label is required when shard_outputs is true")?;
            let path = parse_target_path(label)
                .map_err(|e| format!("invalid output_shard_label {label:?}: {e}"))?;
            Some((shards, path))
        } else {
            None
        };
        Ok(Self {
            batch_period_ms: config.batch_period_ms,
            worker_shards: config.worker_shards.unwrap_or(1).max(1),
            worker_flush_offset_ms: config.worker_flush_offset_ms.unwrap_or(0),
            group_cache_ttl: Duration::from_millis(config.group_cache_ttl_ms),
            clean_interval: Duration::from_millis(config.group_cache_clean_interval_ms.max(1)),
            exclude: config.exclude_tags.iter().cloned().collect(),
            split_by_time_hour_truncation: config.split_by_time_hour_truncation,
            output_sharding,
        })
    }

    /// fnv-1a hash over `name + sorted non-excluded tags`. Reused for BOTH worker routing
    /// (`% worker_shards`) and, when enabled, output sharding (`% output_shards`) — computed once.
    fn identity_hash(&self, metric: &Metric) -> u64 {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        let mut feed = |bytes: &[u8]| {
            for &b in bytes {
                h ^= b as u64;
                h = h.wrapping_mul(0x0000_0100_0000_01b3);
            }
        };
        feed(metric.name().as_bytes());
        if let Some(tags) = metric.tags() {
            for (k, v) in tags.iter_single() {
                if self.exclude.contains(k) {
                    continue;
                }
                feed(k.as_bytes());
                feed(b"=");
                feed(v.as_bytes());
            }
        }
        h
    }
}

// --------------------------------------------------------------------------- reduce state

/// Per-group accumulator (per series, and per hour when `split_by_time_hour_truncation`). Holds ONLY
/// this window's samples; name+labels are recovered from the lossless group key at flush time so
/// nothing per-series is cached between windows.
struct Group {
    values: Vec<(i64, f64)>,
    start_ts: i64,
    end_ts: i64,
    /// Series identity hash (fnv-1a over name + non-excluded tags); constant for the group. Carried
    /// from the dispatcher so output sharding reuses the routing hash instead of recomputing it.
    hash: u64,
    last_seen: Instant,
}

/// Build the lossless group key. When `hour_ms` is `Some`, the hour bucket is appended after the
/// `\x1e` delimiter so series flush per-hour; when `None` the hour is omitted and a series batches
/// into a single group. `decode_key` recovers name/labels either way (it stops at `\x1e`).
fn group_key(metric: &Metric, exclude: &HashSet<String>, hour_ms: Option<i64>) -> String {
    let mut k = String::with_capacity(256);
    k.push_str(metric.name());
    k.push('\u{1f}');
    if let Some(tags) = metric.tags() {
        for (tk, tv) in tags.iter_single() {
            if exclude.contains(tk) {
                continue;
            }
            k.push_str(tk);
            k.push('=');
            k.push_str(tv);
            k.push('\u{1f}');
        }
    }
    if let Some(hour_ms) = hour_ms {
        k.push('\u{1e}');
        k.push_str(&hour_ms.to_string());
    }
    k
}

/// Decode the lossless group key (`name \x1f k=v\x1f… \x1e hour`) back into (name, labels). Assumes
/// label values don't contain the \x1f/\x1e control delimiters (true for Prometheus remote-write).
fn decode_key(key: &str) -> (String, BTreeMap<String, String>) {
    let head = key.split('\u{1e}').next().unwrap_or(key);
    let mut parts = head.split('\u{1f}');
    let name = parts.next().unwrap_or("").to_string();
    let mut labels = BTreeMap::new();
    for kv in parts {
        if kv.is_empty() {
            continue;
        }
        if let Some(eq) = kv.find('=') {
            labels.insert(kv[..eq].to_string(), kv[eq + 1..].to_string());
        }
    }
    (name, labels)
}

fn accumulate(
    groups: &mut HashMap<String, Group>,
    metric: Metric,
    hash: u64,
    exclude: &HashSet<String>,
    split_by_hour: bool,
) {
    let timestamp_ms: i64 = metric
        .timestamp()
        .map(|ts| ts.timestamp_millis())
        .unwrap_or_else(|| Utc::now().timestamp_millis());
    let now_ms = Utc::now().timestamp_millis();
    // Drop samples more than 5m in the future or 10m in the past (matches the Hydra pipeline).
    if timestamp_ms > now_ms + 300_000 || timestamp_ms < now_ms - 600_000 {
        return;
    }
    let value = match metric.value() {
        MetricValue::Gauge { value } => *value,
        MetricValue::Counter { value } => *value,
        _ => return,
    };
    let hour_ms = split_by_hour.then(|| timestamp_ms - (timestamp_ms % 3_600_000));
    let key = group_key(&metric, exclude, hour_ms);
    match groups.entry(key) {
        Entry::Vacant(e) => {
            e.insert(Group {
                values: vec![(timestamp_ms, value)],
                start_ts: timestamp_ms,
                end_ts: timestamp_ms,
                hash,
                last_seen: Instant::now(),
            });
        }
        Entry::Occupied(mut e) => {
            let g = e.get_mut();
            g.values.push((timestamp_ms, value));
            if timestamp_ms < g.start_ts {
                g.start_ts = timestamp_ms;
            }
            if timestamp_ms > g.end_ts {
                g.end_ts = timestamp_ms;
            }
            g.last_seen = Instant::now();
        }
    }
}

/// Build the generic batched log: `name`, `labels`, `start_ts`, `end_ts`, `values`, and (when output
/// sharding is on) the shard-index field.
#[allow(clippy::too_many_arguments)]
fn assemble_generic_log(
    name: &str,
    labels: &BTreeMap<String, String>,
    values: JsonValue,
    start_ts_ms: i64,
    end_ts_ms: i64,
    output_shard: Option<(&OwnedTargetPath, u64)>,
    metadata: EventMetadata,
) -> LogEvent {
    let labels_obj = JsonValue::Object(
        labels
            .iter()
            .map(|(k, v)| (k.clone(), JsonValue::String(v.clone())))
            .collect(),
    );
    let mut log = LogEvent::new_with_metadata(metadata);
    log.insert(event_path!("name"), name);
    log.insert(event_path!("labels"), labels_obj);
    log.insert(event_path!("start_ts"), JsonValue::Number(start_ts_ms.into()));
    log.insert(event_path!("end_ts"), JsonValue::Number(end_ts_ms.into()));
    log.insert(event_path!("values"), values);
    if let Some((path, shard)) = output_shard {
        log.insert(path, shard as i64);
    }
    log
}

// --------------------------------------------------------------------------- workers

/// One independent shard worker: own map, own flush + clean timers, no shared state.
async fn run_worker(
    worker: usize,
    mut rx: mpsc::UnboundedReceiver<(Metric, u64)>,
    out: mpsc::Sender<Event>,
    batch_period_ms: u64,
    offset_earlier_ms: u64,
    ttl: Duration,
    clean_interval: Duration,
    exclude: HashSet<String>,
    split_by_hour: bool,
    output_sharding: Option<(u64, OwnedTargetPath)>,
) {
    let mut groups: HashMap<String, Group> = HashMap::new();
    let worker_label = worker.to_string();

    let first_delay = batch_period_ms.saturating_sub(offset_earlier_ms);
    let mut flush_tick = interval_at(
        TokioInstant::now() + Duration::from_millis(first_delay),
        Duration::from_millis(batch_period_ms.max(1)),
    );
    let mut clean_tick = interval_at(TokioInstant::now() + clean_interval, clean_interval);

    loop {
        select! {
            maybe = rx.recv() => match maybe {
                Some((metric, hash)) => accumulate(&mut groups, metric, hash, &exclude, split_by_hour),
                None => { flush(&mut groups, &out, output_sharding.as_ref()).await; break; }
            },
            _ = flush_tick.tick() => {
                flush(&mut groups, &out, output_sharding.as_ref()).await;
                gauge!("metric_batch_groups", "worker" => worker_label.clone()).set(groups.len() as f64);
            }
            _ = clean_tick.tick() => {
                let now = Instant::now();
                groups.retain(|_, g| now.duration_since(g.last_seen) <= ttl);
                gauge!("metric_batch_groups", "worker" => worker_label.clone()).set(groups.len() as f64);
            }
        }
    }
}

/// Double-buffered flush: swap each group's samples out of the still-live map and hand the batch to a
/// spawned task that decodes the key and builds the generic logs — so it never blocks accumulation.
async fn flush(
    groups: &mut HashMap<String, Group>,
    out: &mpsc::Sender<Event>,
    output_sharding: Option<&(u64, OwnedTargetPath)>,
) {
    let mut batch: Vec<(String, Vec<(i64, f64)>, i64, i64, u64)> = Vec::new();
    for (key, g) in groups.iter_mut() {
        if !g.values.is_empty() {
            batch.push((key.clone(), std::mem::take(&mut g.values), g.start_ts, g.end_ts, g.hash));
            g.start_ts = i64::MAX; // reset; next sample re-seeds the window's min ts
            g.end_ts = i64::MIN; // reset; next sample re-seeds the window's max ts
        }
    }
    if batch.is_empty() {
        return;
    }
    let out = out.clone();
    let output_sharding = output_sharding.cloned();
    tokio::spawn(async move {
        for (key, vals, start_ts, end_ts, hash) in batch {
            let (name, labels) = decode_key(&key);
            let values = JsonValue::Array(
                vals.iter()
                    .map(|(ts, v)| serde_json::json!({"ts": ts, "v": v}))
                    .collect(),
            );
            let output_shard = output_sharding
                .as_ref()
                .map(|(n, path)| (path, hash % n));
            let log = assemble_generic_log(
                &name,
                &labels,
                values,
                start_ts,
                end_ts,
                output_shard,
                EventMetadata::default(),
            );
            if out.send(Event::from(log)).await.is_err() {
                break;
            }
        }
    });
}

impl TaskTransform<Event> for MetricBatch {
    fn transform(
        self: Box<Self>,
        mut input_rx: Pin<Box<dyn Stream<Item = Event> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Event> + Send>>
    where
        Self: 'static,
    {
        let n = self.worker_shards;
        let (out_tx, out_rx) = mpsc::channel::<Event>(100_000);

        // Unbounded per-worker queues so the dispatcher never blocks on a busy worker.
        let mut worker_txs: Vec<mpsc::UnboundedSender<(Metric, u64)>> = Vec::with_capacity(n);
        for w in 0..n {
            let (wtx, wrx) = mpsc::unbounded_channel::<(Metric, u64)>();
            worker_txs.push(wtx);
            let offset_earlier_ms =
                (self.worker_flush_offset_ms / n as u64).saturating_mul(w as u64 + 1);
            tokio::spawn(run_worker(
                w,
                wrx,
                out_tx.clone(),
                self.batch_period_ms,
                offset_earlier_ms,
                self.group_cache_ttl,
                self.clean_interval,
                self.exclude.clone(),
                self.split_by_time_hour_truncation,
                self.output_sharding.clone(),
            ));
        }
        drop(out_tx);

        // Dispatcher: compute the identity hash ONCE, route to a worker by `% worker_shards`, and
        // hand the raw hash along so the worker can reuse it for output sharding at flush time.
        let me = *self;
        let worker_shards = me.worker_shards as u64;
        tokio::spawn(async move {
            while let Some(event) = input_rx.next().await {
                let metric = event.into_metric();
                let hash = me.identity_hash(&metric);
                let shard = (hash % worker_shards) as usize;
                let _ = worker_txs[shard].send((metric, hash));
            }
        });

        Box::pin(ReceiverStream::new(out_rx))
    }
}
