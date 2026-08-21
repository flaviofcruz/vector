use chrono::Utc;
use metrics::counter;
use regex::Regex;
use std::collections::HashMap;
use std::env;
use std::ops::Add;
use std::sync::{
    Arc, LazyLock, Mutex, OnceLock,
    atomic::{AtomicBool, AtomicU32, Ordering},
};
use tokio::runtime::Handle;
use tokio::sync::Notify;
use tracing::Span;

// Cumulative delivery counters survive config reloads but reset with the process. Keep this value
// process-wide so remote consumers can distinguish real resets without splitting on config reload.
static PROCESS_GENERATION_ID: LazyLock<String> =
    LazyLock::new(|| uuid::Uuid::new_v4().to_string());

// Sentinel emitted when a Lumberjack source's topic cannot be resolved from
// either the source context or the filename. The spelling — including the
// "Infered" typo — matches the value used elsewhere in the pipeline.
const LUMBERJACK_TOPIC_INFERRED_SENTINEL: &str = "lumberjackTopicInfered";

// Fallback topic used by the woodchuck VRL for kubernetes_logs events that
// have neither an explicit source_context.topic nor a Lumberjack filename
// match. Mirrors `event_logs.libsonnet:155`.
const KUBERNETES_LOGS_FALLBACK_TOPIC: &str = "sawmill-service-log";

const UNKNOWN_SERVICE_SYSTEM: &str = "unknown";
const UNKNOWN_SOURCE_POD_ID: &str = "unknown";
// Kubernetes pod names cannot contain underscores, so this cannot collide with
// a real source identity. The export transform selects this aggregate series
// when source-pod cardinality is disabled.
const ALL_SOURCE_PODS_ID: &str = "__all_source_pods__";

/// Identifies which source emitted the event, used to align topic resolution
/// with the woodchuck VRL's per-source fallback behavior.
pub const SOURCE_TYPE_FILE: &str = "file";
pub const SOURCE_TYPE_KUBERNETES_LOGS: &str = "kubernetes_logs";

/// Bounded reason label for records rejected by a file-backed source before
/// they can enter the event pipeline.
pub const REJECTION_REASON_LINE_TOO_LONG: &str = "line_too_long";

/// Canonical `deliveryMethod` values written by the woodchuck VRL into
/// `logMetadata.deliveryMethod`. Used both on the read side (where the VRL
/// hasn't run yet, so we infer the value from `source_type`) and to keep the
/// metric label aligned with what downstream consumers already see in VEL.
/// Mirrors `woodchuck/configuration/components/transforms/`:
/// `*_log_daemon_wrapper.libsonnet`, `diskless.libsonnet`,
/// `application_heartbeats.libsonnet`, `file_based_raw_proto_streaming.libsonnet`.
pub const DELIVERY_METHOD_FILE: &str = "VECTOR_WOODCHUCK_V2_FILE";

/// Extracts a Lumberjack topic from a source filename of either the active
/// form (`<TableNameCamelCase>[LaMigration].pb[.base64][.gz]`) or the
/// archived/rotated form (`<prefix>.<TableNameCamelCase>[LaMigration].pb[.base64][.gz]`),
/// returning the dash-cased table name (e.g. `"service-request-log"`).
/// Returns `None` for filenames that do not match either form.
pub fn extract_topic_from_source_filename(path: &str) -> Option<String> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        // The CamelCase capture is allowed to start at the beginning of the
        // string, after a `/` (directory separator), or after a `.` (timestamp
        // / hostname separator) so both active and archived filenames match.
        // Lazy `+?` on the CamelCase capture lets the optional `LaMigration`
        // suffix match outside the capture group rather than being absorbed.
        Regex::new(r"(?:^|[/.])([A-Z][A-Za-z0-9]+?)(LaMigration)?\.pb(?:\.base64)?(?:\.gz)?$")
            .expect("topic extraction regex must compile")
    });
    let captures = re.captures(path)?;
    let camel = captures.get(1)?.as_str();
    Some(camel_to_dash_case(camel))
}

/// Converts a CamelCase identifier to dash-case
/// (e.g. `"ServiceRequestLog"` -> `"service-request-log"`).
fn camel_to_dash_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for (i, c) in s.chars().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            out.push('-');
        }
        out.push(c.to_ascii_lowercase());
    }
    out
}

/// Resolves the topic label for a source-read event. Mirrors the per-source
/// branching in `event_logs.libsonnet`'s `ConvertDeliveryInfoLogs`:
/// 1. `source_context.topic` if it is a concrete value (not the sentinel).
/// 2. Otherwise the topic inferred from the filename.
/// 3. Otherwise a source-specific fallback: `"sawmill-service-log"` for
///    `kubernetes_logs` sources, the inference sentinel for everything else.
fn resolve_received_topic(
    source_context: &HashMap<String, String>,
    path: &str,
    source_type: &str,
) -> String {
    let explicit = source_context.get("topic").map(String::as_str);
    match explicit {
        Some(t) if t != LUMBERJACK_TOPIC_INFERRED_SENTINEL => t.to_string(),
        _ => extract_topic_from_source_filename(path).unwrap_or_else(|| {
            if source_type == SOURCE_TYPE_KUBERNETES_LOGS {
                KUBERNETES_LOGS_FALLBACK_TOPIC.to_string()
            } else {
                LUMBERJACK_TOPIC_INFERRED_SENTINEL.to_string()
            }
        }),
    }
}

/// Hour-rounded Unix-milliseconds bucket for the current time, as an `i64`.
/// This is the discovery-time `timeParity`: it is stamped onto the event at
/// read time (see `file.rs` / `kubernetes_logs/mod.rs`) and carried through the
/// rest of the pipeline, so the read/staged/delivered legs all bucket on the
/// same instant rather than each recomputing a wall-clock hour at a different
/// pipeline stage. Matches the spec for `time_period_parity` ("the unix time
/// the log was discovered by logging-agent, truncated to the hour, consistent
/// across all stages").
pub fn current_hour_time_parity_ms_value() -> i64 {
    const HOUR_MS: i64 = 60 * 60 * 1000;
    let now_ms = Utc::now().timestamp_millis();
    now_ms - (now_ms % HOUR_MS)
}

/// String form of [`current_hour_time_parity_ms_value`], for use as a metric label
/// and as the fallback when an event carries no `timeParity`.
fn current_hour_time_parity_ms() -> String {
    current_hour_time_parity_ms_value().to_string()
}

/// Reads `timeParity` from `value_map`, falling back to the current hour.
fn time_parity_from_value_map(value_map: &HashMap<String, String>) -> String {
    value_map
        .get("timeParity")
        .cloned()
        .unwrap_or_else(current_hour_time_parity_ms)
}

/// Reads `topic` from `value_map`, falling back to `"unknown"`.
fn topic_from_value_map(value_map: &HashMap<String, String>) -> String {
    value_map
        .get("topic")
        .cloned()
        .unwrap_or_else(|| "unknown".to_string())
}

/// Reads `deliveryMethod` from `value_map`, falling back to `"unknown"`.
/// Mirrors `topic_from_value_map` / `time_parity_from_value_map`. Splitting
/// the completeness ratio by this label distinguishes file-source deliveries
/// (`VECTOR_WOODCHUCK_V2_FILE`) from diskless gRPC deliveries
/// (`VECTOR_WOODCHUCK_V2_DISKLESS`), which is necessary because diskless
/// events have no corresponding read-side counter.
fn delivery_method_from_value_map(value_map: &HashMap<String, String>) -> String {
    value_map
        .get("deliveryMethod")
        .cloned()
        .unwrap_or_else(|| "unknown".to_string())
}

/// Reads the source service identity from delivery-event granularity, falling
/// back to a stable label value so every delivery counter has the same shape.
fn service_system_from_value_map(value_map: &HashMap<String, String>) -> String {
    service_system_label(value_map.get("system").map(String::as_str))
}

fn service_system_label(service_system: Option<&str>) -> String {
    service_system
        .filter(|system| !system.is_empty())
        .unwrap_or(UNKNOWN_SERVICE_SYSTEM)
        .to_string()
}

/// Reads the originating workload identity from delivery-event granularity,
/// falling back to a stable label value so every delivery counter has the same shape.
fn source_pod_id_from_value_map(value_map: &HashMap<String, String>) -> String {
    source_pod_id_label(value_map.get("workloadInstanceId").map(String::as_str))
}

fn source_pod_id_label(source_pod_id: Option<&str>) -> String {
    source_pod_id
        .filter(|pod_id| !pod_id.is_empty())
        .unwrap_or(UNKNOWN_SOURCE_POD_ID)
        .to_string()
}

/// Extracts the source workload identity from file roots whose first child is
/// the pod UID or pod name. This mirrors the logging-agent wrapper transform so
/// file-source reads and sink deliveries use the same metric label.
pub fn source_pod_id_from_file_path(path: &str) -> Option<&str> {
    const SOURCE_POD_PATH_PREFIXES: [&str; 3] = [
        "/var/lib/kubelet/pods/",
        "/databricks/host-root/local_disk0/databricks/spark-logs/",
        "/databricks/host-root/local_disk0/serverless-logs/internal/",
    ];

    SOURCE_POD_PATH_PREFIXES.iter().find_map(|prefix| {
        path.strip_prefix(prefix)
            .and_then(|suffix| suffix.split('/').next())
            .filter(|source_pod_id| !source_pod_id.is_empty())
    })
}

/// Maps a read source's `source_type` to the `deliveryMethod` value the VRL
/// pipeline stamps downstream. `deliveryMethod` denotes *how the content was
/// read* — `VECTOR_WOODCHUCK_V2_FILE` for content read from files,
/// `VECTOR_WOODCHUCK_V2_DISKLESS` for diskless gRPC — it is not a topic-type
/// taxonomy. Both the `file` source and the Databricks fork's `kubernetes_logs`
/// source read from files (the latter reads Lumberjack proto files, and pod
/// stdout, from container directories), so both map to `DELIVERY_METHOD_FILE`.
/// The diskless gRPC source emits no read event, so reads are always
/// file-based; the `_` arm is a defensive fallback for any unexpected source.
fn delivery_method_for_source_type(source_type: &str) -> &'static str {
    match source_type {
        SOURCE_TYPE_FILE | SOURCE_TYPE_KUBERNETES_LOGS => DELIVERY_METHOD_FILE,
        _ => "unknown",
    }
}

// Env flag to gate when we should be emitting the read event (want to support both while we check the performance of the new spot)
pub static EMIT_READ_EVENT_AFTER_MULTILINE_AGG: OnceLock<bool> = OnceLock::new();
pub fn emit_read_event_after_multiline_agg() -> bool {
    *EMIT_READ_EVENT_AFTER_MULTILINE_AGG.get_or_init(|| {
        env::var("EMIT_READ_EVENT_AFTER_MULTILINE_AGG")
            .map(|v| v == "true")
            .unwrap_or(false)
    })
}

/// Shared gate for read-event emission: emit only once — either *before*
/// multiline aggregation with `EMIT_READ_EVENT_AFTER_MULTILINE_AGG` off, or
/// *after* it with the flag on. Both read spots (the pre-multiline spot in
/// `file_server.rs` and the per-line post-multiline spots in `file.rs` /
/// `kubernetes_logs`) route through the singleton; this keeps exactly one of
/// them active for a given config.
fn read_should_emit(emitted_after_multiline_agg: bool) -> bool {
    (emitted_after_multiline_agg && emit_read_event_after_multiline_agg())
        || (!emitted_after_multiline_agg && !emit_read_event_after_multiline_agg())
}

/// Per-`(source_type, path)` read accumulation. `source_context` is captured
/// once on first insert within a flush window (a given path is read by a single
/// source instance, so its context is stable), so the hot per-line path never
/// re-clones it.
#[derive(Debug)]
struct ReadAccum {
    source_context: HashMap<String, String>,
    bytes_read: usize,
    lines_read: usize,
    /// Component span captured at accumulate time. Re-entered around the flush
    /// `info!` so the trace `BroadcastLayer` copies `component_id`/`type`/`kind`
    /// onto the log under `.vector.*` — restoring the component context that the
    /// detached flush task would otherwise lack.
    span: Span,
}

/// A sink delivery count plus the component span it was accumulated under, so
/// the batched flush log carries `.vector.component_*` like the read leg.
#[derive(Debug)]
struct SinkAccum {
    value: MetadataValuesCount,
    span: Span,
}

/// Flush interval for the global delivery-event singleton. Configurable via
/// `VECTOR_DELIVERY_FLUSH_INTERVAL_SECS` (whole seconds, must be > 0); defaults
/// to 60s. Read once and cached.
pub fn delivery_flush_interval() -> std::time::Duration {
    const DEFAULT_SECS: u64 = 60;
    static INTERVAL_SECS: OnceLock<u64> = OnceLock::new();
    let secs = *INTERVAL_SECS.get_or_init(|| {
        env::var("VECTOR_DELIVERY_FLUSH_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|s| *s > 0)
            .unwrap_or(DEFAULT_SECS)
    });
    std::time::Duration::from_secs(secs)
}

/// Whether delivery-event VEL `info!` logs are batched through the process-global
/// [`DeliveryEventSingleton`] (one aggregated log per flush window) or emitted
/// inline at each event site (one log per line read / per sink request — the
/// original pre-singleton behavior).
///
/// Controlled by `VECTOR_BATCH_DELIVERY_EVENT_LOGS` (`true`/`1` to batch);
/// defaults to `false` (inline). Read once and cached. Regardless of this flag,
/// the `delivery_events_total` counters are always emitted inline at the event
/// sites, so the SLI metrics are unaffected by the choice.
pub fn delivery_event_batching_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        env::var("VECTOR_BATCH_DELIVERY_EVENT_LOGS")
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false)
    })
}

/// Which sink leg a count map belongs to.
#[derive(Clone, Copy)]
enum SinkLeg {
    Staged,
    Delivered,
}

/// Process-global accumulator for all VEL delivery events (source reads, sink
/// staged, sink delivered). Emit sites call `accumulate_*` (cheap per-leg map
/// merges); a single background task flushes every `delivery_flush_interval()`
/// and once more when `shutdown()` is invoked.
///
/// This replaces both the per-line `info!`/`counter!` emissions and the
/// per-source `DeliveryReadAccumulator` with one flush loop, one configurable
/// interval, and one shutdown flush. The shutdown flush is invoked from
/// `topology::running::stop` at the wave-1 -> wave-2 boundary — after every
/// data source/sink has emitted its final counts, but while `internal_logs` is
/// still alive to forward the resulting VEL logs (see the matching comment
/// around `VECTOR_PROCESS_COMPONENTS_CLOSED`).
pub struct DeliveryEventSingleton {
    // Separate locks per leg so the hot read path never contends with sink
    // response handling. Each lock is held only for a HashMap merge.
    reads: Mutex<HashMap<(&'static str, String, i64), ReadAccum>>,
    staged: Mutex<HashMap<String, SinkAccum>>,
    delivered: Mutex<HashMap<String, SinkAccum>>,
    /// Signals the background flush task to perform a final flush and exit.
    shutdown: Notify,
    /// Set once the background flush task has been spawned.
    spawned: AtomicBool,
}

static DELIVERY_SINGLETON: OnceLock<DeliveryEventSingleton> = OnceLock::new();

/// Returns the process-global delivery-event accumulator, initializing it on
/// first use. The background flush task is spawned lazily on the first
/// `accumulate_*` call that runs inside a Tokio runtime.
pub fn delivery_singleton() -> &'static DeliveryEventSingleton {
    DELIVERY_SINGLETON.get_or_init(|| DeliveryEventSingleton {
        reads: Mutex::new(HashMap::new()),
        staged: Mutex::new(HashMap::new()),
        delivered: Mutex::new(HashMap::new()),
        shutdown: Notify::new(),
        spawned: AtomicBool::new(false),
    })
}

impl DeliveryEventSingleton {
    /// Spawns the periodic flush task the first time it is called from within a
    /// Tokio runtime. A cheap relaxed load makes every subsequent call a no-op.
    fn ensure_flush_task(&'static self) {
        if self.spawned.load(Ordering::Relaxed) {
            return;
        }
        // Outside a runtime (e.g. unit tests, `vector generate`) there is no
        // task to drive periodic flushing; accumulation still works and can be
        // flushed explicitly. Retry on a later call once a runtime exists.
        if Handle::try_current().is_err() {
            return;
        }
        if self
            .spawned
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            tokio::spawn(self.run_flush_loop());
        }
    }

    async fn run_flush_loop(&'static self) {
        let mut ticker = tokio::time::interval(delivery_flush_interval());
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // The first tick resolves immediately; consume it so we don't flush an
        // empty registry right away.
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = ticker.tick() => self.flush(),
                _ = self.shutdown.notified() => {
                    self.flush();
                    break;
                }
            }
        }
    }

    /// Accumulates a source read. The `delivery_events_total` counter fires
    /// immediately (left as-is, so the metric stays exact); only the VEL
    /// `info!` log is batched. `source_context` is borrowed and only cloned on
    /// the first occurrence of a `(source_type, path, time_parity)` within a flush window.
    pub fn accumulate_read(
        &'static self,
        path: String,
        bytes_read: usize,
        lines_read: usize,
        source_context: &Option<HashMap<String, String>>,
        service_system: Option<&str>,
        source_pod_id: Option<&str>,
        source_type: &'static str,
        time_parity: i64,
        emitted_after_multiline_agg: bool,
    ) {
        if !read_should_emit(emitted_after_multiline_agg) {
            return;
        }

        // Counter emitted inline, bucketed on the read-time `time_parity` the
        // event is stamped with — not batched through the flush.
        let empty = HashMap::new();
        let ctx = source_context.as_ref().unwrap_or(&empty);
        counter!(
            "delivery_events_total",
            "delivery_event_type" => "VECTOR_SOURCE_READ",
            "time_parity" => time_parity.to_string(),
            "delivery_method" => delivery_method_for_source_type(source_type),
            "topic" => resolve_received_topic(ctx, &path, source_type),
            "service_system" => service_system_label(service_system),
            "source_pod_id" => source_pod_id_label(source_pod_id),
            "process_generation_id" => PROCESS_GENERATION_ID.as_str(),
        )
        .increment(lines_read as u64);

        counter!(
            "delivery_events_total",
            "delivery_event_type" => "VECTOR_SOURCE_READ",
            "time_parity" => time_parity.to_string(),
            "delivery_method" => delivery_method_for_source_type(source_type),
            "topic" => resolve_received_topic(ctx, &path, source_type),
            "service_system" => service_system_label(service_system),
            "source_pod_id" => ALL_SOURCE_PODS_ID,
            "process_generation_id" => PROCESS_GENERATION_ID.as_str(),
        )
        .increment(lines_read as u64);

        // Byte-count sibling of `delivery_events_total` with identical labels.
        counter!(
            "delivery_event_bytes_total",
            "delivery_event_type" => "VECTOR_SOURCE_READ",
            "time_parity" => time_parity.to_string(),
            "delivery_method" => delivery_method_for_source_type(source_type),
            "topic" => resolve_received_topic(ctx, &path, source_type),
            "service_system" => service_system_label(service_system),
            "source_pod_id" => source_pod_id_label(source_pod_id),
            "process_generation_id" => PROCESS_GENERATION_ID.as_str(),
        )
        .increment(bytes_read as u64);

        counter!(
            "delivery_event_bytes_total",
            "delivery_event_type" => "VECTOR_SOURCE_READ",
            "time_parity" => time_parity.to_string(),
            "delivery_method" => delivery_method_for_source_type(source_type),
            "topic" => resolve_received_topic(ctx, &path, source_type),
            "service_system" => service_system_label(service_system),
            "source_pod_id" => ALL_SOURCE_PODS_ID,
            "process_generation_id" => PROCESS_GENERATION_ID.as_str(),
        )
        .increment(bytes_read as u64);

        // Inline mode (default): emit the VEL log immediately in the current
        // source span, one per read — the original pre-singleton behavior. No
        // accumulation, so the background flush task is never spawned.
        if !delivery_event_batching_enabled() {
            emit_read_log(&path, bytes_read, lines_read, ctx, time_parity);
            return;
        }

        self.ensure_flush_task();
        self.record_read(path, bytes_read, lines_read, source_context, source_type, time_parity);
    }

    /// Records source events rejected before admission to the event pipeline.
    /// These use a distinct delivery-event stage so the existing
    /// delivered/read completeness ratio is unchanged, while callers can add
    /// rejected events to the denominator for an admission-adjusted view.
    pub fn emit_rejected(
        &self,
        path: &str,
        rejected_events: usize,
        source_context: &Option<HashMap<String, String>>,
        service_system: Option<&str>,
        source_pod_id: Option<&str>,
        source_type: &'static str,
        time_parity: i64,
        rejection_reason: &'static str,
    ) {
        if rejected_events == 0 {
            return;
        }

        let empty = HashMap::new();
        let ctx = source_context.as_ref().unwrap_or(&empty);
        let topic = resolve_received_topic(ctx, path, source_type);
        let service_system = service_system
            .filter(|system| !system.is_empty())
            .unwrap_or("unknown")
            .to_string();
        counter!(
            "delivery_events_total",
            "delivery_event_type" => "VECTOR_SOURCE_REJECTED",
            "rejection_reason" => rejection_reason,
            "time_parity" => time_parity.to_string(),
            "delivery_method" => delivery_method_for_source_type(source_type),
            "topic" => topic.clone(),
            "service_system" => service_system.clone(),
            "source_pod_id" => source_pod_id_label(source_pod_id),
            "process_generation_id" => PROCESS_GENERATION_ID.as_str(),
        )
        .increment(rejected_events as u64);

        counter!(
            "delivery_events_total",
            "delivery_event_type" => "VECTOR_SOURCE_REJECTED",
            "rejection_reason" => rejection_reason,
            "time_parity" => time_parity.to_string(),
            "delivery_method" => delivery_method_for_source_type(source_type),
            "topic" => topic.clone(),
            "service_system" => service_system.clone(),
            "source_pod_id" => ALL_SOURCE_PODS_ID,
            "process_generation_id" => PROCESS_GENERATION_ID.as_str(),
        )
        .increment(rejected_events as u64);

        // This counter omits delivery-only bucket and process labels so M3 can
        // aggregate recent rejections by the affected service and topic.
        counter!(
            "source_rejected_events_total",
            "rejection_reason" => rejection_reason,
            "topic" => topic,
            "service_system" => service_system,
        )
        .increment(rejected_events as u64);
    }

    /// Merges one read into the `(source_type, path, time_parity)` accumulator.
    /// Split out from [`accumulate_read`] so the merge/bucketing behavior can be
    /// unit-tested directly, independent of the `VECTOR_BATCH_DELIVERY_EVENT_LOGS`
    /// routing and the inline counter.
    fn record_read(
        &self,
        path: String,
        bytes_read: usize,
        lines_read: usize,
        source_context: &Option<HashMap<String, String>>,
        source_type: &'static str,
        time_parity: i64,
    ) {
        let mut reads = self.reads.lock().expect("delivery reads lock poisoned");
        let entry = reads
            .entry((source_type, path, time_parity))
            .or_insert_with(|| ReadAccum {
                source_context: source_context.clone().unwrap_or_default(),
                bytes_read: 0,
                lines_read: 0,
                // Captured once per key: the source component span is stable per path.
                span: Span::current(),
            });
        entry.bytes_read += bytes_read;
        entry.lines_read += lines_read;
    }

    /// Accumulates a sink "staged" count map (one per sink request).
    pub fn accumulate_staged(&'static self, count_map: &HashMap<String, MetadataValuesCount>) {
        self.merge_sink(count_map, SinkLeg::Staged);
    }

    /// Accumulates a sink "delivered" count map (one per successful request).
    pub fn accumulate_delivered(&'static self, count_map: &HashMap<String, MetadataValuesCount>) {
        self.merge_sink(count_map, SinkLeg::Delivered);
    }

    fn merge_sink(&'static self, count_map: &HashMap<String, MetadataValuesCount>, leg: SinkLeg) {
        if count_map.is_empty() {
            return;
        }
        let (message, delivery_event_type) = match leg {
            SinkLeg::Staged => (
                "Delivery event: SINK_STAGED_MESSAGES",
                "VECTOR_SINK_UPLOAD_STAGED",
            ),
            SinkLeg::Delivered => (
                "Delivery event: SINK_DELIVERED_MESSAGES",
                "VECTOR_SINK_UPLOAD_DELIVERED",
            ),
        };

        // Inline mode (default): emit one VEL log per count-map entry in the
        // current sink span, per request — the original pre-singleton behavior.
        if !delivery_event_batching_enabled() {
            for value in count_map.values() {
                emit_sink_delivery_log(value, message, delivery_event_type);
            }
            return;
        }

        self.ensure_flush_task();
        // One emit call comes from a single sink, so the component span is the
        // same for every key in this count map; captured once and stored per key.
        let span = Span::current();
        let lock = match leg {
            SinkLeg::Staged => &self.staged,
            SinkLeg::Delivered => &self.delivered,
        };
        let mut target = lock.lock().expect("delivery sink lock poisoned");
        for (key, value) in count_map {
            target
                .entry(key.clone())
                .and_modify(|existing| {
                    existing.value.count += value.count;
                    existing.value.size += value.size;
                })
                .or_insert_with(|| SinkAccum {
                    value: value.clone(),
                    span: span.clone(),
                });
        }
    }

    /// Drains all three legs and emits the aggregated VEL `info!` logs. Only the
    /// logs are batched here; the `delivery_events_total` counters are emitted
    /// inline at the event sites. Each leg lock is taken only to swap out its
    /// map, so `info!` never runs while a registry lock is held.
    pub fn flush(&self) {
        let reads = std::mem::take(&mut *self.reads.lock().expect("delivery reads lock poisoned"));
        let staged =
            std::mem::take(&mut *self.staged.lock().expect("delivery staged lock poisoned"));
        let delivered =
            std::mem::take(&mut *self.delivered.lock().expect("delivery delivered lock poisoned"));

        for ((_source_type, path, time_parity), accum) in reads {
            // Re-enter the source's component span so the trace BroadcastLayer
            // copies `.vector.component_{id,type,kind}` onto the log.
            let _entered = accum.span.enter();
            emit_read_log(
                &path,
                accum.bytes_read,
                accum.lines_read,
                &accum.source_context,
                time_parity,
            );
        }

        emit_sink_delivery_logs(
            staged.values(),
            "Delivery event: SINK_STAGED_MESSAGES",
            "VECTOR_SINK_UPLOAD_STAGED",
        );
        emit_sink_delivery_logs(
            delivered.values(),
            "Delivery event: SINK_DELIVERED_MESSAGES",
            "VECTOR_SINK_UPLOAD_DELIVERED",
        );
    }

    /// Final flush at shutdown: stops the background task and drains
    /// synchronously so the events are emitted in the wave-1 -> wave-2 window
    /// while `internal_logs` can still forward them.
    pub fn shutdown(&self) {
        self.shutdown.notify_one();
        self.flush();
    }
}

/// Emits a single source-read VEL `info!` log. Shared by the singleton flush
/// (which re-enters the captured component span first) and the inline path in
/// [`DeliveryEventSingleton::accumulate_read`] (which is already in the source
/// span), so both modes emit the identical log shape.
fn emit_read_log(
    path: &str,
    bytes_read: usize,
    lines_read: usize,
    source_context: &HashMap<String, String>,
    time_parity: i64,
) {
    info!(
        message = "Delivery event: READ_MESSAGES.",
        file = %path,
        num_bytes = bytes_read,
        num_events = lines_read,
        delivery_event_type = "VECTOR_SOURCE_READ",
        vector_event_type = "VECTOR_LOG_DELIVERY_EVENT",
        // Read-time hour bucket carried onto the log so the universe VRL buckets
        // the read leg on the same `timeParity` as staged/delivered, instead of
        // recomputing it from the (up-to-flush-interval-late) timestamp.
        time_parity = time_parity,
        internal_log_rate_limit = false,
        source_context = serde_json::to_string(source_context).unwrap(),
    );
}

/// Emits a single sink-delivery VEL `info!` log for one aggregated `value_map`.
/// Shared by the singleton flush (via [`emit_sink_delivery_logs`], which
/// re-enters the captured span first) and the inline path in
/// [`DeliveryEventSingleton::merge_sink`] (already in the sink span).
fn emit_sink_delivery_log(
    value: &MetadataValuesCount,
    message: &'static str,
    delivery_event_type: &'static str,
) {
    info!(
        message = message,
        keys = serde_json::to_string(&value.value_map).unwrap(),
        delivery_event_type = delivery_event_type,
        vector_event_type = "VECTOR_LOG_DELIVERY_EVENT",
        num_events = value.count,
        num_bytes = value.size,
        // Specifying this allows us to emit without rate limiting (needed for high throughput sinks)
        internal_log_rate_limit = false,
    );
}

/// Emits one VEL `info!` log per aggregated `value_map`. Used by the sink
/// delivery legs of the singleton flush. Each entry's component span is
/// re-entered so the trace BroadcastLayer copies `.vector.component_*` onto the
/// log. The matching counters are emitted separately and inline by
/// [`emit_delivery_counters`].
fn emit_sink_delivery_logs<'a>(
    accums: impl Iterator<Item = &'a SinkAccum>,
    message: &'static str,
    delivery_event_type: &'static str,
) {
    for accum in accums {
        let _entered = accum.span.enter();
        emit_sink_delivery_log(&accum.value, message, delivery_event_type);
    }
}

/// Emits the `delivery_events_total` counter per `value_map`. Called inline
/// (per sink request) so the metric is not affected by the log batching.
pub fn emit_delivery_counters<'a>(
    values: impl Iterator<Item = &'a MetadataValuesCount>,
    delivery_event_type: &'static str,
) {
    for value in values {
        counter!(
            "delivery_events_total",
            "delivery_event_type" => delivery_event_type.to_string(),
            "time_parity" => time_parity_from_value_map(&value.value_map),
            "topic" => topic_from_value_map(&value.value_map),
            "delivery_method" => delivery_method_from_value_map(&value.value_map),
            "service_system" => service_system_from_value_map(&value.value_map),
            "source_pod_id" => source_pod_id_from_value_map(&value.value_map),
            "process_generation_id" => PROCESS_GENERATION_ID.as_str(),
        )
        .increment(value.count as u64);

        counter!(
            "delivery_events_total",
            "delivery_event_type" => delivery_event_type.to_string(),
            "time_parity" => time_parity_from_value_map(&value.value_map),
            "topic" => topic_from_value_map(&value.value_map),
            "delivery_method" => delivery_method_from_value_map(&value.value_map),
            "service_system" => service_system_from_value_map(&value.value_map),
            "source_pod_id" => ALL_SOURCE_PODS_ID,
            "process_generation_id" => PROCESS_GENERATION_ID.as_str(),
        )
        .increment(value.count as u64);
    }
}

/*
* Struct to help track the count/size of events per unique combination of specified fields
*/
#[derive(Clone, Debug)]
pub struct MetadataValuesCount {
    pub value_map: HashMap<String, String>,
    pub count: usize,
    pub size: usize,
}

// Struct for vector sink delivery events (staged, delivered)
// This doesn't match the actual delivery event structure (the real events are exploded from the count map)
// But similar to send events, it helps us easily extend granularity without needing a vector code change
#[derive(Clone, Debug, Default)]
pub struct VectorSinkDeliveryEvent {
    pub count_map: HashMap<String, MetadataValuesCount>,
    // Field for testing purposes use Arc<AtomicU32> to ensure something that carries info even across cloning
    pub delivered_call_count: Arc<AtomicU32>,
}

impl VectorSinkDeliveryEvent {
    pub fn new() -> Self {
        Self {
            count_map: HashMap::new(),
            delivered_call_count: Arc::new(AtomicU32::new(0)),
        }
    }

    pub fn with_count_map(count_map: HashMap<String, MetadataValuesCount>) -> Self {
        Self {
            count_map,
            delivered_call_count: Arc::new(AtomicU32::new(0)),
        }
    }

    pub fn emit_delivered_event(&self) {
        // Mark for testing
        self.delivered_call_count.fetch_add(1, Ordering::SeqCst);

        // VECTOR_DELIVERED_MESSAGES_EVENT. The counter is emitted inline per
        // request (left as-is); only the VEL `info!` log is batched through the
        // process-global singleton.
        emit_delivery_counters(self.count_map.values(), "VECTOR_SINK_UPLOAD_DELIVERED");
        delivery_singleton().accumulate_delivered(&self.count_map);
    }

    pub fn emit_staged_event(&self) {
        // VECTOR_STAGED_MESSAGES_EVENT — see `emit_delivered_event`.
        emit_delivery_counters(self.count_map.values(), "VECTOR_SINK_UPLOAD_STAGED");
        delivery_singleton().accumulate_staged(&self.count_map);
    }
}

pub fn combine_sink_delivery_events(
    events: Vec<VectorSinkDeliveryEvent>,
) -> VectorSinkDeliveryEvent {
    let mut combined_map: HashMap<String, MetadataValuesCount> = HashMap::new();

    for event in events {
        for (key, value) in &event.count_map {
            combined_map
                .entry(key.clone())
                .and_modify(|existing| {
                    existing.count += value.count;
                    existing.size += value.size;
                })
                .or_insert_with(|| value.clone());
        }
    }

    VectorSinkDeliveryEvent::with_count_map(combined_map)
}

impl Add<VectorSinkDeliveryEvent> for VectorSinkDeliveryEvent {
    type Output = VectorSinkDeliveryEvent;

    fn add(mut self, other: VectorSinkDeliveryEvent) -> Self::Output {
        self.merge(other);
        self
    }
}

impl VectorSinkDeliveryEvent {
    /// Merge another event's count_map into self in-place, avoiding O(N²) cloning.
    pub fn merge(&mut self, other: VectorSinkDeliveryEvent) {
        for (key, value) in other.count_map {
            self.count_map
                .entry(key)
                .and_modify(|existing| {
                    existing.count += value.count;
                    existing.size += value.size;
                })
                .or_insert(value);
        }
    }
}

#[cfg(test)]
mod topic_inference_tests {
    use super::*;

    #[test]
    fn process_generation_id_is_stable_and_valid() {
        let first = PROCESS_GENERATION_ID.as_str();
        let second = PROCESS_GENERATION_ID.as_str();

        assert_eq!(first, second);
        assert!(uuid::Uuid::parse_str(first).is_ok());
    }

    #[test]
    fn camel_to_dash_case_basic() {
        assert_eq!(camel_to_dash_case("ServiceRequestLog"), "service-request-log");
        assert_eq!(camel_to_dash_case("Log"), "log");
        assert_eq!(camel_to_dash_case("ABC"), "a-b-c");
    }

    #[test]
    fn extract_topic_standard_pb_base64() {
        assert_eq!(
            extract_topic_from_source_filename(
                "/var/lib/kubelet/pods/abc/volumes/logs/12345.ServiceRequestLog.pb.base64"
            )
            .as_deref(),
            Some("service-request-log"),
        );
    }

    #[test]
    fn extract_topic_standard_pb_base64_gz() {
        assert_eq!(
            extract_topic_from_source_filename(
                "/var/lib/kubelet/pods/abc/volumes/logs/12345.ProductEventLog.pb.base64.gz"
            )
            .as_deref(),
            Some("product-event-log"),
        );
    }

    #[test]
    fn extract_topic_la_migration_archived() {
        // Archived/rotated form: timestamp prefix, ends in .gz.
        assert_eq!(
            extract_topic_from_source_filename(
                "/var/log/pods/sample/2026-05-22-01.ServiceRequestLogLaMigration.pb.base64.gz"
            )
            .as_deref(),
            Some("service-request-log"),
        );
    }

    #[test]
    fn extract_topic_la_migration_active() {
        // Active form: filename starts directly with the CamelCase, no prefix.
        assert_eq!(
            extract_topic_from_source_filename(
                "/var/log/pods/sample/ServiceRequestLogLaMigration.pb.base64"
            )
            .as_deref(),
            Some("service-request-log"),
        );
    }

    #[test]
    fn extract_topic_active_form_no_suffix() {
        // Active form without LaMigration suffix.
        assert_eq!(
            extract_topic_from_source_filename(
                "/var/log/pods/sample/ProductEventLog.pb.base64"
            )
            .as_deref(),
            Some("product-event-log"),
        );
    }

    #[test]
    fn extract_topic_bare_pb() {
        // Some files end at .pb without .base64.
        assert_eq!(
            extract_topic_from_source_filename(
                "/var/log/pods/sample/12345.BackgroundActivityLog.pb"
            )
            .as_deref(),
            Some("background-activity-log"),
        );
    }

    #[test]
    fn extract_topic_returns_none_for_non_lumberjack() {
        // HAL / nginx access logs don't follow the Lumberjack convention.
        assert!(
            extract_topic_from_source_filename(
                "/var/log/pods/sample/nginx/service-access.log"
            )
            .is_none()
        );
        assert!(
            extract_topic_from_source_filename("/var/log/pods/sample/audit.json").is_none()
        );
        assert!(extract_topic_from_source_filename("no-pattern-at-all").is_none());
    }

    #[test]
    fn resolve_received_topic_uses_explicit_context_when_present() {
        let mut ctx = HashMap::new();
        ctx.insert("topic".to_string(), "audit-log".to_string());
        assert_eq!(
            resolve_received_topic(&ctx, "/some/file.json", SOURCE_TYPE_FILE),
            "audit-log"
        );
    }

    #[test]
    fn resolve_received_topic_infers_when_context_is_sentinel() {
        let mut ctx = HashMap::new();
        ctx.insert("topic".to_string(), "lumberjackTopicInfered".to_string());
        assert_eq!(
            resolve_received_topic(
                &ctx,
                "/path/to/12345.ServiceRequestLog.pb.base64",
                SOURCE_TYPE_FILE,
            ),
            "service-request-log"
        );
    }

    #[test]
    fn resolve_received_topic_falls_back_to_sentinel_for_file_source() {
        let mut ctx = HashMap::new();
        ctx.insert("topic".to_string(), "lumberjackTopicInfered".to_string());
        assert_eq!(
            resolve_received_topic(&ctx, "/path/to/nothing-matching.log", SOURCE_TYPE_FILE),
            "lumberjackTopicInfered"
        );
    }

    #[test]
    fn resolve_received_topic_falls_back_to_sawmill_for_kubernetes_logs() {
        // kubernetes_logs source with non-Lumberjack filename and no useful
        // source_context falls back to "sawmill-service-log" to match the
        // woodchuck VRL's ConvertDeliveryInfoLogs branch for k8s logs.
        let ctx = HashMap::new();
        assert_eq!(
            resolve_received_topic(
                &ctx,
                "/var/log/pods/some-pod/container/0.log",
                SOURCE_TYPE_KUBERNETES_LOGS,
            ),
            "sawmill-service-log"
        );
    }

    #[test]
    fn resolve_received_topic_kubernetes_logs_still_prefers_filename_match() {
        // A kubernetes_logs source reading a Lumberjack proto file still
        // resolves via filename — the sawmill-service-log fallback only
        // applies when no other resolution is available.
        let ctx = HashMap::new();
        assert_eq!(
            resolve_received_topic(
                &ctx,
                "/var/log/pods/sample/12345.AuditLog.pb.base64",
                SOURCE_TYPE_KUBERNETES_LOGS,
            ),
            "audit-log"
        );
    }

    #[test]
    fn resolve_received_topic_infers_when_context_missing() {
        let ctx = HashMap::new();
        assert_eq!(
            resolve_received_topic(&ctx, "/path/to/12345.AuditLog.pb.base64.gz", SOURCE_TYPE_FILE),
            "audit-log"
        );
    }

    #[test]
    fn delivery_method_for_source_type_file_sources_are_file() {
        assert_eq!(
            delivery_method_for_source_type(SOURCE_TYPE_FILE),
            DELIVERY_METHOD_FILE
        );
        // The Databricks fork's kubernetes_logs source reads files, so it is
        // file-based too — not a distinct delivery method.
        assert_eq!(
            delivery_method_for_source_type(SOURCE_TYPE_KUBERNETES_LOGS),
            DELIVERY_METHOD_FILE
        );
    }

    #[test]
    fn delivery_method_for_source_type_unknown_falls_back() {
        assert_eq!(delivery_method_for_source_type("vector"), "unknown");
        assert_eq!(delivery_method_for_source_type(""), "unknown");
    }

    #[test]
    fn delivery_method_from_value_map_reads_existing_key() {
        let mut value_map = HashMap::new();
        value_map.insert("deliveryMethod".to_string(), "VECTOR_WOODCHUCK_V2_DISKLESS".to_string());
        assert_eq!(
            delivery_method_from_value_map(&value_map),
            "VECTOR_WOODCHUCK_V2_DISKLESS"
        );
    }

    #[test]
    fn delivery_method_from_value_map_falls_back_when_missing() {
        let value_map = HashMap::new();
        assert_eq!(delivery_method_from_value_map(&value_map), "unknown");
    }

    #[test]
    fn service_system_from_value_map_reads_existing_key() {
        let value_map = HashMap::from([("system".to_string(), "lakebase-proxy".to_string())]);
        assert_eq!(service_system_from_value_map(&value_map), "lakebase-proxy");
    }

    #[test]
    fn service_system_from_value_map_falls_back_when_missing_or_empty() {
        assert_eq!(service_system_from_value_map(&HashMap::new()), "unknown");
        assert_eq!(
            service_system_from_value_map(&HashMap::from([("system".to_string(), String::new())])),
            "unknown"
        );
    }

    #[test]
    fn source_pod_id_from_value_map_reads_workload_instance_id() {
        let value_map = HashMap::from([(
            "workloadInstanceId".to_string(),
            "apps-123-main-abcde".to_string(),
        )]);
        assert_eq!(
            source_pod_id_from_value_map(&value_map),
            "apps-123-main-abcde"
        );
    }

    #[test]
    fn source_pod_id_from_value_map_falls_back_when_missing_or_empty() {
        assert_eq!(source_pod_id_from_value_map(&HashMap::new()), "unknown");
        assert_eq!(
            source_pod_id_from_value_map(&HashMap::from([(
                "workloadInstanceId".to_string(),
                String::new(),
            )])),
            "unknown"
        );
    }

    #[test]
    fn source_pod_id_from_file_path_reads_supported_roots() {
        for (path, expected) in [
            (
                "/var/lib/kubelet/pods/pod-uid/volumes/kubernetes.io~empty-dir/logs/auth/app.log",
                "pod-uid",
            ),
            (
                "/databricks/host-root/local_disk0/databricks/spark-logs/spark-pod/databricks/driver/logs/stdout",
                "spark-pod",
            ),
            (
                "/databricks/host-root/local_disk0/serverless-logs/internal/serverless-pod/databricks/driver/logs/stdout",
                "serverless-pod",
            ),
        ] {
            assert_eq!(source_pod_id_from_file_path(path), Some(expected));
        }
    }

    #[test]
    fn source_pod_id_from_file_path_ignores_unknown_or_empty_roots() {
        assert_eq!(source_pod_id_from_file_path("/var/log/auth/app.log"), None);
        assert_eq!(source_pod_id_from_file_path("/var/lib/kubelet/pods/"), None);
    }
}

#[cfg(test)]
mod read_accumulation_tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    // A fresh, isolated singleton per test. Leaked to obtain the `&'static`
    // reference `accumulate_read` requires; under `#[test]` there is no Tokio
    // runtime, so `ensure_flush_task` is a no-op and nothing is spawned.
    fn empty_singleton() -> &'static DeliveryEventSingleton {
        Box::leak(Box::new(DeliveryEventSingleton {
            reads: Mutex::new(HashMap::new()),
            staged: Mutex::new(HashMap::new()),
            delivered: Mutex::new(HashMap::new()),
            shutdown: Notify::new(),
            spawned: AtomicBool::new(false),
        }))
    }

    // An arbitrary hour-floored timestamp (ms).
    const TP: i64 = 1_700_000_000_000 - (1_700_000_000_000 % (60 * 60 * 1000));

    // Reads for the same path within one hour bucket aggregate into a single
    // entry, so the flush emits one VEL read line carrying that bucket.
    #[test]
    fn same_bucket_aggregates() {
        let s = empty_singleton();
        s.record_read(
            "/f.log".to_string(),
            10,
            1,
            &None::<HashMap<String, String>>,
            SOURCE_TYPE_FILE,
            TP,
        );
        s.record_read(
            "/f.log".to_string(),
            25,
            2,
            &None::<HashMap<String, String>>,
            SOURCE_TYPE_FILE,
            TP,
        );

        let reads = s.reads.lock().unwrap();
        assert_eq!(
            reads.len(),
            1,
            "same (path, time_parity) must share one entry"
        );
        let accum = reads
            .get(&(SOURCE_TYPE_FILE, "/f.log".to_string(), TP))
            .expect("entry for the read bucket");
        assert_eq!(accum.bytes_read, 35);
        assert_eq!(accum.lines_read, 3);
    }

    // Reads for the same path that straddle an hour boundary bucket separately,
    // so the flush emits one correctly-bucketed VEL read line per hour instead of
    // collapsing both into the flush-time bucket. This is the gap the fix closes.
    #[test]
    fn hour_boundary_buckets_separately() {
        let s = empty_singleton();
        let tp_next = TP + 60 * 60 * 1000;
        s.record_read(
            "/f.log".to_string(),
            10,
            1,
            &None::<HashMap<String, String>>,
            SOURCE_TYPE_FILE,
            TP,
        );
        s.record_read(
            "/f.log".to_string(),
            10,
            1,
            &None::<HashMap<String, String>>,
            SOURCE_TYPE_FILE,
            tp_next,
        );

        let reads = s.reads.lock().unwrap();
        assert_eq!(
            reads.len(),
            2,
            "reads crossing an hour boundary must bucket separately"
        );
        assert!(reads.contains_key(&(SOURCE_TYPE_FILE, "/f.log".to_string(), TP)));
        assert!(reads.contains_key(&(SOURCE_TYPE_FILE, "/f.log".to_string(), tp_next)));
    }

    // With `VECTOR_BATCH_DELIVERY_EVENT_LOGS` unset (the default), the public
    // `accumulate_read` routes to the inline emit path and accumulates nothing,
    // so the registry stays empty. (CI does not set the env var; the flag is
    // cached on first read.)
    #[test]
    fn inline_mode_does_not_accumulate() {
        let s = empty_singleton();
        s.accumulate_read(
            "/f.log".to_string(),
            10,
            1,
            &None::<HashMap<String, String>>,
            None,
            None,
            SOURCE_TYPE_FILE,
            TP,
            false,
        );
        assert!(
            s.reads.lock().unwrap().is_empty(),
            "inline mode (default) must not accumulate reads"
        );
    }

    #[test]
    fn rejected_events_use_a_separate_stage_and_bounded_reason() {
        let s = empty_singleton();
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let source_context = Some(HashMap::from([(
            "topic".to_string(),
            "security-log".to_string(),
        )]));

        metrics::with_local_recorder(&recorder, || {
            s.emit_rejected(
                "/var/log/security.log",
                3,
                &source_context,
                Some("money-settings"),
                Some("money-settings-7d9f"),
                SOURCE_TYPE_FILE,
                TP,
                REJECTION_REASON_LINE_TOO_LONG,
            );
        });

        let metrics = snapshotter.snapshot().into_vec();
        assert_eq!(metrics.len(), 3);
        let (key, _, _, value) = metrics
            .iter()
            .find(|(key, _, _, _)| {
                key.key().name() == "delivery_events_total"
                    && key.key().labels().any(|label| {
                        label.key() == "source_pod_id" && label.value() != ALL_SOURCE_PODS_ID
                    })
            })
            .expect("delivery metric");
        assert_eq!(key.key().name(), "delivery_events_total");
        assert_eq!(*value, DebugValue::Counter(3));

        let labels = key
            .key()
            .labels()
            .map(|label| (label.key(), label.value()))
            .collect::<HashMap<_, _>>();
        assert_eq!(
            labels.get("delivery_event_type"),
            Some(&"VECTOR_SOURCE_REJECTED")
        );
        assert_eq!(
            labels.get("rejection_reason"),
            Some(&REJECTION_REASON_LINE_TOO_LONG)
        );
        assert_eq!(labels.get("topic"), Some(&"security-log"));
        assert_eq!(labels.get("service_system"), Some(&"money-settings"));
        assert_eq!(labels.get("source_pod_id"), Some(&"money-settings-7d9f"));
        assert_eq!(labels.get("delivery_method"), Some(&DELIVERY_METHOD_FILE));
        let time_parity = TP.to_string();
        assert_eq!(labels.get("time_parity"), Some(&time_parity.as_str()));

        let aggregate_labels = metrics
            .iter()
            .find(|(key, _, _, _)| {
                key.key().name() == "delivery_events_total"
                    && key.key().labels().any(|label| {
                        label.key() == "source_pod_id" && label.value() == ALL_SOURCE_PODS_ID
                    })
            })
            .expect("aggregate delivery metric")
            .0
            .key()
            .labels()
            .map(|label| (label.key(), label.value()))
            .collect::<HashMap<_, _>>();
        assert_eq!(
            aggregate_labels.get("source_pod_id"),
            Some(&ALL_SOURCE_PODS_ID)
        );

        let (key, _, _, value) = metrics
            .iter()
            .find(|(key, _, _, _)| key.key().name() == "source_rejected_events_total")
            .expect("alerting metric");
        assert_eq!(*value, DebugValue::Counter(3));
        let labels = key
            .key()
            .labels()
            .map(|label| (label.key(), label.value()))
            .collect::<HashMap<_, _>>();
        assert_eq!(labels.get("rejection_reason"), Some(&"line_too_long"));
        assert_eq!(labels.get("topic"), Some(&"security-log"));
        assert_eq!(labels.get("service_system"), Some(&"money-settings"));
        assert!(!labels.contains_key("time_parity"));
        assert!(!labels.contains_key("process_generation_id"));
    }

    // flush() drains the read registry so each bucket is emitted exactly once.
    #[test]
    fn flush_drains_reads() {
        let s = empty_singleton();
        s.record_read(
            "/f.log".to_string(),
            10,
            1,
            &None::<HashMap<String, String>>,
            SOURCE_TYPE_FILE,
            TP,
        );
        assert_eq!(s.reads.lock().unwrap().len(), 1);

        s.flush();
        assert!(s.reads.lock().unwrap().is_empty(), "flush must drain reads");
    }
}
