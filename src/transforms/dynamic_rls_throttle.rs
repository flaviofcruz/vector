use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use async_stream::stream;
use bytes::{Buf, Bytes, BytesMut};
use futures::{Stream, StreamExt};
use http::{Request, Uri};
use hyper::{Body, client::HttpConnector};
use prost_reflect::{
    DescriptorPool, DeserializeOptions, DynamicMessage, MethodDescriptor, Value, prost::Message,
};
use vector_lib::{
    config::clone_input_definitions,
    configurable::configurable_component,
    internal_event::{ComponentEventsDropped, Count, INTENTIONAL, InternalEventHandle, Registered},
};
use vrl::path::{OwnedTargetPath, parse_target_path};

use crate::{
    config::{DataType, Input, OutputId, TransformConfig, TransformContext, TransformOutput},
    event::{Event, EventStatus},
    internal_events::{DynamicRlsThrottleInflow, DynamicRlsThrottleOverLimit},
    schema,
    transforms::{TaskTransform, Transform},
};

/// Fully-qualified name of the sidecar's gRPC service, looked up in the runtime-loaded
/// `DescriptorPool` to resolve the request/response message types.
const REPORT_COUNTS_SERVICE: &str =
    "databricks.woodchuck.vectoraggregatorsidecar.ReportCountsService";
/// The unary gRPC method on `REPORT_COUNTS_SERVICE` the transform calls each reporting window.
const REPORT_COUNTS_METHOD: &str = "ReportCounts";

/// Plaintext HTTP/2 (h2c) client for the loopback gRPC call — no TLS (the sidecar is on pod
/// loopback), unlike the `token_manager` / `bricklens_ingest` mTLS clients.
type GrpcClient = hyper::Client<HttpConnector, Body>;

/// The key a log is counted and throttled under: its `(topic, system)` combination.
type Key = (String, String);

/// Whether the transform is actually dropping over-quota logs or only measuring them. Used as the
/// `mode` tag on the over-limit metric.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ThrottleMode {
    /// Forward over-quota logs and only emit the would-drop metric.
    Shadow,
    /// Drop over-quota logs.
    Enforce,
}

impl ThrottleMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            ThrottleMode::Shadow => "shadow",
            ThrottleMode::Enforce => "enforce",
        }
    }
}

/// Which over-quota logs actually get dropped. The three states are mutually exclusive, so invalid
/// combinations (e.g. "armed but no scope") can't be expressed.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EnforceMode {
    /// Measure only — every over-quota log is shadow-forwarded, nothing is dropped (full shadow).
    #[default]
    None,
    /// Drop over-quota logs only for the systems in `enforce.systems`; shadow the rest on the same
    /// pod (half-shadow).
    Selected,
    /// Drop every over-quota `(topic, system)`.
    All,
}

/// How the transform enforces over-quota drops. Replaces the earlier flat `enforce` bool +
/// `enforce_all` + `enforce_systems`: a single `mode` enum makes the valid states explicit, and
/// `systems` only applies when `mode = selected`.
#[configurable_component]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct EnforceConfig {
    /// Enforcement mode. Defaults to `none` (full shadow), so an existing config that omits
    /// `enforce` behaves exactly as before.
    #[serde(default)]
    pub mode: EnforceMode,

    /// With `mode = selected`, the allowlist of `system` values whose over-quota logs are dropped;
    /// every other system stays measure-only on the same pod (half-shadow). Ignored for any other
    /// mode.
    #[serde(default)]
    pub systems: Vec<String>,
}

const fn default_sidecar_endpoint() -> &'static str {
    // gRPC target only (scheme + host:port, no path); `http://` = plaintext h2c loopback.
    "http://127.0.0.1:8090"
}

fn default_sidecar_endpoint_string() -> String {
    default_sidecar_endpoint().to_string()
}

/// Default mount path of the sidecar's `ReportCounts` `FileDescriptorSet`. Produced and mounted
/// universe-side (separate PR); that mount MUST match this exact path.
const fn default_sidecar_proto_descriptor_path() -> &'static str {
    "/etc/proto/vector-aggregator-sidecar/report_counts_proto_descriptor.pb"
}

fn default_sidecar_proto_descriptor_path_buf() -> PathBuf {
    PathBuf::from(default_sidecar_proto_descriptor_path())
}

const fn default_report_interval_secs() -> u64 {
    30
}

const fn default_report_timeout_secs() -> u64 {
    5
}

const fn default_max_staleness_secs() -> u64 {
    60
}

fn default_topic_field() -> String {
    ".logMetadata.topic".to_string()
}

fn default_system_field() -> String {
    ".message.kubernetes.pod_labels.system".to_string()
}

const fn default_emit_inflow_metric() -> bool {
    true
}

/// Configuration for the `dynamic_rls_throttle` transform.
///
/// This transform enforces per-`(topic, system)` online quotas: it drops logs whose
/// `(topic, system)` is over quota according to `enforce.mode`, or shadow-forwards them (the
/// default). It does not decide quotas itself:
/// every `report_interval_secs` it POSTs the per-combo passed-through counts to a sidecar
/// (a Java service in the same pod) which consults the rate-limit service (RLSv2) and returns
/// the current over-limit set. The transform reads that set lock-free on the hot path and
/// never blocks on the sidecar.
///
/// Fail-open is the rule everywhere: on any sidecar/RLSv2 error, or if the over-limit set goes
/// stale for longer than `max_staleness_secs`, the transform stops dropping and lets every log
/// through. Dropping logs we should not is worse than briefly letting over-quota logs through.
#[configurable_component(transform(
    "dynamic_rls_throttle",
    "Throttle logs whose topic/system is over its online quota (as decided by the rate-limit service); drops per `enforce.mode`, otherwise shadow-forwards."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct DynamicRlsThrottleConfig {
    /// The sidecar's native gRPC target: scheme + `host:port` only (e.g. `http://127.0.0.1:8090`),
    /// no path. `http://` selects plaintext h2c over pod loopback; the method path comes from the
    /// mounted descriptor. Replaces the old HTTP/JSON `POST /api/reportCounts` (which 404'd wherever
    /// the sidecar's transcoding route was not registered).
    #[serde(default = "default_sidecar_endpoint_string")]
    pub sidecar_endpoint: String,

    /// Path to the sidecar's `ReportCounts` `FileDescriptorSet`, loaded at startup to drive gRPC
    /// (de)serialization (vector carries no compiled copy of the universe proto). If it is
    /// missing/unloadable the transform fails open: throttling disabled, all logs forwarded.
    #[serde(default = "default_sidecar_proto_descriptor_path_buf")]
    pub sidecar_proto_descriptor_path: PathBuf,

    /// How often, in seconds, to report counts to the sidecar and refresh the over-limit set.
    #[serde(default = "default_report_interval_secs")]
    pub report_interval_secs: u64,

    /// How long, in seconds, to wait for the sidecar's response before treating the report as
    /// failed (and falling open if the set is stale).
    #[serde(default = "default_report_timeout_secs")]
    pub report_timeout_secs: u64,

    /// How long, in seconds, the over-limit set may go without a successful refresh before the
    /// transform fails open (drops nothing). Guards against dropping on a stale decision when the
    /// sidecar is unreachable.
    #[serde(default = "default_max_staleness_secs")]
    pub max_staleness_secs: u64,

    /// Event field path holding the log topic, e.g. `background-activity-log`. Maps to the RLSv2
    /// rate-limit group.
    #[serde(default = "default_topic_field")]
    pub topic_field: String,

    /// Event field path holding the originating system, e.g. `auth-v2`. Sourced from the pod's
    /// `system` label (`kubernetes.pod_labels.system`).
    #[serde(default = "default_system_field")]
    pub system_field: String,

    /// How over-quota logs are enforced: `mode` (`none` = full shadow / `selected` = half-shadow /
    /// `all`) plus the `systems` allowlist used when `mode = selected`. Defaults to `none`, so an
    /// existing config that omits `enforce` measures only and drops nothing. Over-quota logs are
    /// excluded from the RLSv2 counts in every mode, so shadow predicts what enforce would drop.
    #[configurable(derived)]
    #[serde(default)]
    pub enforce: EnforceConfig,

    /// Whether to emit the per-`(topic, system)` `dynamic_rls_throttle_inflow_total` metric
    /// (default `true`). This is the highest-cardinality metric — one series per `(topic, system)`
    /// per pod, emitted for every keyed log. Set to `false` to suppress it on a shard where the
    /// series count is a concern; the `over_limit` metric (sparse — only over-quota combos) is
    /// unaffected.
    #[serde(default = "default_emit_inflow_metric")]
    pub emit_inflow_metric: bool,
}

impl Default for DynamicRlsThrottleConfig {
    fn default() -> Self {
        Self {
            sidecar_endpoint: default_sidecar_endpoint_string(),
            sidecar_proto_descriptor_path: default_sidecar_proto_descriptor_path_buf(),
            report_interval_secs: default_report_interval_secs(),
            report_timeout_secs: default_report_timeout_secs(),
            max_staleness_secs: default_max_staleness_secs(),
            topic_field: default_topic_field(),
            system_field: default_system_field(),
            enforce: EnforceConfig::default(),
            emit_inflow_metric: default_emit_inflow_metric(),
        }
    }
}

impl_generate_config_from_default!(DynamicRlsThrottleConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "dynamic_rls_throttle")]
impl TransformConfig for DynamicRlsThrottleConfig {
    async fn build(&self, _context: &TransformContext) -> crate::Result<Transform> {
        // Validate the timing config at build time rather than panicking/no-op-ing at runtime.
        // Zero timings: `report_interval_secs == 0` would panic `tokio::time::interval`, and a
        // zero timeout would fail every report immediately.
        if self.report_interval_secs == 0 {
            return Err("`report_interval_secs` must be greater than zero".into());
        }
        if self.report_timeout_secs == 0 {
            return Err("`report_timeout_secs` must be greater than zero".into());
        }
        // Cross-field relationships: a timeout longer than the reporting interval lets a slow
        // report overrun the next cycle, and a staleness budget shorter than the interval would
        // fail open before even one successful refresh could land.
        if self.report_timeout_secs > self.report_interval_secs {
            return Err(format!(
                "`report_timeout_secs` ({}) must be <= `report_interval_secs` ({})",
                self.report_timeout_secs, self.report_interval_secs
            )
            .into());
        }
        if self.max_staleness_secs < self.report_interval_secs {
            return Err(format!(
                "`max_staleness_secs` ({}) must be >= `report_interval_secs` ({})",
                self.max_staleness_secs, self.report_interval_secs
            )
            .into());
        }

        // Parse the key field paths once here so a malformed path fails the build loudly
        let topic_path = parse_target_path(&self.topic_field)
            .map_err(|e| format!("invalid `topic_field` path {:?}: {e}", self.topic_field))?;
        let system_path = parse_target_path(&self.system_field)
            .map_err(|e| format!("invalid `system_field` path {:?}: {e}", self.system_field))?;

        // Load the gRPC transport (client + endpoint + runtime descriptor). MUST FAIL OPEN: any
        // failure here (bad descriptor OR unparseable endpoint) disables throttling rather than
        // erroring the build, which would crashloop the VA config. The timing/field-path checks
        // above still fail loud — only the transport build fails open.
        let transport = match build_transport(&self.sidecar_endpoint, self.sidecar_proto_descriptor_path.as_path()) {
            Ok(transport) => Some(transport),
            Err(error) => {
                warn!(
                    message = "Failed to load sidecar gRPC descriptor; DISABLING throttling (failing open, forwarding all logs).",
                    %error,
                    descriptor_path = %self.sidecar_proto_descriptor_path.display(),
                );
                None
            }
        };

        Ok(Transform::event_task(DynamicRlsThrottle::new(
            self.clone(),
            transport,
            topic_path,
            system_path,
        )))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn outputs(
        &self,
        _: &TransformContext,
        input_definitions: &[(OutputId, schema::Definition)],
    ) -> Vec<TransformOutput> {
        // The event is not modified, so the definition is passed through as-is.
        vec![TransformOutput::new(
            DataType::Log,
            clone_input_definitions(input_definitions),
        )]
    }
}

// --- gRPC transport over pod-local loopback (native `ReportCounts`, runtime descriptor).
//
// Hand-frames a unary gRPC call over an h2c client using a `prost_reflect` `MethodDescriptor` from
// a runtime-mounted `FileDescriptorSet`, so vector carries no compiled copy of the universe proto
// (the LP-1615 / `token_manager` precedent).

/// Resolved gRPC transport for `ReportCounts`: h2c client, endpoint, and method descriptor. Built
/// once at startup and cheaply cloned into the reporter.
#[derive(Clone)]
struct GrpcTransport {
    client: GrpcClient,
    endpoint: Uri,
    method: MethodDescriptor,
}

/// Load the `ReportCounts` `MethodDescriptor` from a `FileDescriptorSet` file. Any error (missing
/// file, bad bytes, missing service/method) is returned so `build` can fail open.
fn load_report_counts_method(path: &std::path::Path) -> crate::Result<MethodDescriptor> {
    let bytes = std::fs::read(path)
        .map_err(|e| format!("read sidecar proto descriptor {}: {e}", path.display()))?;
    let fds = prost_reflect::prost_types::FileDescriptorSet::decode(&bytes[..])
        .map_err(|e| format!("decode sidecar FileDescriptorSet {}: {e}", path.display()))?;
    let pool = DescriptorPool::from_file_descriptor_set(fds)
        .map_err(|e| format!("build sidecar DescriptorPool {}: {e}", path.display()))?;
    let service = pool.get_service_by_name(REPORT_COUNTS_SERVICE).ok_or_else(|| {
        format!(
            "service `{REPORT_COUNTS_SERVICE}` not found in descriptor {}",
            path.display()
        )
    })?;
    service
        .methods()
        .find(|m| m.name() == REPORT_COUNTS_METHOD)
        .ok_or_else(|| {
            format!("method `{REPORT_COUNTS_METHOD}` not found in service `{REPORT_COUNTS_SERVICE}`")
                .into()
        })
}

/// Build the h2c client, parse the endpoint, and load the descriptor. Failures return to `build`
/// (which fails open).
fn build_transport(endpoint: &str, descriptor_path: &std::path::Path) -> crate::Result<GrpcTransport> {
    let method = load_report_counts_method(descriptor_path)?;
    let endpoint: Uri = endpoint
        .parse()
        .map_err(|e| format!("invalid `sidecar_endpoint` {endpoint:?}: {e}"))?;
    // Prior-knowledge h2c (no TLS, no HTTP/1.1 upgrade) — mirrors the `kafka_producer_proxy` client.
    let client = hyper::Client::builder()
        .http2_only(true)
        .build(HttpConnector::new());
    Ok(GrpcTransport {
        client,
        endpoint,
        method,
    })
}

/// The `POST /<fully-qualified-service>/<method>` URI for a unary gRPC call.
fn grpc_uri(endpoint: &Uri, method: &MethodDescriptor) -> crate::Result<Uri> {
    let path = format!(
        "/{}/{}",
        method.parent_service().full_name(),
        method.name()
    );
    let base = endpoint.to_string();
    let base = base.trim_end_matches('/');
    format!("{base}{path}")
        .parse::<Uri>()
        .map_err(|e| format!("invalid gRPC request URI `{base}{path}`: {e}").into())
}

/// Wrap a serialized protobuf message with the 5-byte gRPC length-prefix framing
/// (1 byte compression flag + 4 bytes big-endian length).
fn encode_grpc_message(message: Vec<u8>) -> Vec<u8> {
    let len = message.len() as u32;
    let mut framed = Vec::with_capacity(5 + message.len());
    framed.push(0); // 0 = uncompressed
    framed.extend_from_slice(&len.to_be_bytes());
    framed.extend_from_slice(&message);
    framed
}

/// Strip the 5-byte gRPC framing prefix and return the message body bytes. Errors on a short or
/// compressed frame (this client sends `grpc-encoding: identity` and does not decompress).
fn decode_grpc_frame(mut body: Bytes) -> crate::Result<Bytes> {
    if body.remaining() < 5 {
        return Err("gRPC response too short for 5-byte framing".into());
    }
    let compression_flag = body.get_u8();
    if compression_flag != 0 {
        return Err(format!("compressed gRPC responses not supported (flag {compression_flag})").into());
    }
    let message_len = body.get_u32() as usize;
    if body.remaining() < message_len {
        return Err("gRPC response advertised more bytes than were present".into());
    }
    Ok(body.copy_to_bytes(message_len))
}

/// Resolve the effective gRPC status code + message from the response headers, falling back to the
/// HTTP/2 trailers (a "Trailers-Only" fast error puts the status in the headers; a normal response
/// carries it in the trailers). Absent from both → OK (0).
fn resolve_grpc_status(
    headers: &http::HeaderMap,
    trailers: Option<&http::HeaderMap>,
) -> (i32, Option<String>) {
    let code = |h: &http::HeaderMap| {
        h.get("grpc-status")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<i32>().ok())
    };
    let msg = |h: &http::HeaderMap| {
        h.get("grpc-message")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    };
    let status = code(headers).or_else(|| trailers.and_then(code)).unwrap_or(0);
    let message = msg(headers).or_else(|| trailers.and_then(msg));
    (status, message)
}

/// Decode a `ReportCountsResponse` into the over-limit `(topic, system)` set, skipping entries with
/// an empty topic or system (they can't map to an RLS policy).
fn parse_over_limit(msg: &DynamicMessage) -> HashSet<Key> {
    let mut set = HashSet::new();
    let Some(Value::List(entries)) = msg.get_field_by_name("over_limit").map(|v| v.into_owned())
    else {
        return set;
    };
    for entry in entries {
        let Value::Message(m) = entry else { continue };
        let field = |name: &str| {
            m.get_field_by_name(name)
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_default()
        };
        let topic = field("topic");
        let system = field("system");
        if !topic.is_empty() && !system.is_empty() {
            set.insert((topic, system));
        }
    }
    set
}

/// State shared between the per-log hot path and the background reporter task.
struct SharedThrottleState {
    /// Passed-through log counts per `(topic, system)` since the last report. Drained (taken)
    /// each reporting window.
    counts: Mutex<HashMap<Key, u64>>,
    /// The `(topic, system)` combos currently over quota. Read lock-free on the hot path; swapped
    /// wholesale by the reporter each window.
    over_limit: ArcSwap<HashSet<Key>>,
}

pub struct DynamicRlsThrottle {
    config: DynamicRlsThrottleConfig,
    /// `None` if the descriptor failed to load at build (fail-open): the transform is disabled and
    /// passes every event through — no reporter, no counting, no drops.
    transport: Option<GrpcTransport>,
    shared: Arc<SharedThrottleState>,
    /// Pre-compiled key field paths, parsed once at build time (see `build`).
    topic_path: OwnedTargetPath,
    system_path: OwnedTargetPath,
    events_dropped: Registered<ComponentEventsDropped<'static, INTENTIONAL>>,
}

impl DynamicRlsThrottle {
    fn new(
        config: DynamicRlsThrottleConfig,
        transport: Option<GrpcTransport>,
        topic_path: OwnedTargetPath,
        system_path: OwnedTargetPath,
    ) -> Self {
        Self {
            config,
            transport,
            shared: Arc::new(SharedThrottleState {
                counts: Mutex::new(HashMap::new()),
                over_limit: ArcSwap::from_pointee(HashSet::new()),
            }),
            topic_path,
            system_path,
            events_dropped: register!(ComponentEventsDropped::<INTENTIONAL>::from(
                "Topic/system is over its online quota."
            )),
        }
    }
}

impl TaskTransform<Event> for DynamicRlsThrottle {
    fn transform(
        self: Box<Self>,
        mut input_rx: Pin<Box<dyn Stream<Item = Event> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Event> + Send>> {
        let Self {
            config,
            transport,
            shared,
            topic_path,
            system_path,
            events_dropped,
        } = *self;

        // Fail-open: with no transport (descriptor load failed at build), throttling is disabled.
        // Pass every event straight through — no reporter, no counting, nothing ever dropped.
        let Some(transport) = transport else {
            return input_rx;
        };

        // Enforcement is decided per event (`effective_mode`, below) so one pod can drop some
        // systems while shadowing the rest (half-shadow). Captured into locals before `config` is
        // moved into the reporter. `enforce.systems` is a small allowlist, so a linear `contains`
        // on the rare over-quota branch is fine.
        let enforce_mode = config.enforce.mode;
        let enforce_systems = config.enforce.systems.clone();
        let emit_inflow_metric = config.emit_inflow_metric;

        // The reporter runs concurrently so the hot path never blocks on the sidecar. It is
        // aborted when the input stream ends (the returned guard's `Drop`).
        let reporter = BackgroundReporter::spawn(config, transport, Arc::clone(&shared));

        Box::pin(stream! {
            // Keep the reporter alive for exactly as long as this stream; dropping it aborts the
            // background task.
            let _reporter = reporter;

            while let Some(event) = input_rx.next().await {
                match event_key(&topic_path, &system_path, &event) {
                    Some(key) => {
                        // Inflow: every keyed log, pre-decision (stable across shadow/enforce).
                        // Highest-cardinality metric, so it's suppressible via `emit_inflow_metric`.
                        if emit_inflow_metric {
                            emit!(DynamicRlsThrottleInflow {
                                topic: key.0.clone(),
                                system: key.1.clone(),
                            });
                        }

                        if shared.over_limit.load().contains(&key) {
                            // Over quota: never counted; only the action differs. Decide
                            // enforce-vs-shadow for this event's system (`key.1`) before the `emit!`
                            // moves `key`, so `over_limit_total{mode}` is a true per-emission signal.
                            let effective_mode = match enforce_mode {
                                EnforceMode::All => ThrottleMode::Enforce,
                                EnforceMode::Selected if enforce_systems.contains(&key.1) => {
                                    ThrottleMode::Enforce
                                }
                                _ => ThrottleMode::Shadow,
                            };
                            emit!(DynamicRlsThrottleOverLimit {
                                topic: key.0,
                                system: key.1,
                                mode: effective_mode,
                            });
                            match effective_mode {
                                ThrottleMode::Enforce => {
                                    event.metadata().update_status(EventStatus::Dropped);
                                    events_dropped.emit(Count(1));
                                }
                                ThrottleMode::Shadow => yield event,
                            }
                        } else {
                            // Under quota: count the passed-through log and forward it.
                            if let Ok(mut counts) = shared.counts.lock() {
                                *counts.entry(key).or_insert(0) += 1;
                            }
                            yield event;
                        }
                    }
                    // No usable key: forward without counting.
                    None => yield event,
                }
            }
        })
    }
}

/// Extract the `(topic, system)` key from an event using the pre-compiled field paths. Returns
/// `None` if the event is not a log or either field is missing/empty — such a log can't map to an
/// RLS policy, so it is forwarded and not counted.
fn event_key(
    topic_path: &OwnedTargetPath,
    system_path: &OwnedTargetPath,
    event: &Event,
) -> Option<Key> {
    let log = event.maybe_as_log()?;
    let topic = log
        .get(topic_path)
        .map(|v| v.to_string_lossy().into_owned())
        .filter(|s| !s.is_empty())?;
    let system = log
        .get(system_path)
        .map(|v| v.to_string_lossy().into_owned())
        .filter(|s| !s.is_empty())?;
    Some((topic, system))
}

/// Owns the background reporting task and aborts it on drop.
struct BackgroundReporter {
    handle: tokio::task::JoinHandle<()>,
}

impl BackgroundReporter {
    fn spawn(
        config: DynamicRlsThrottleConfig,
        transport: GrpcTransport,
        shared: Arc<SharedThrottleState>,
    ) -> Self {
        let handle = tokio::spawn(run_reporter(config, transport, shared));
        Self { handle }
    }
}

impl Drop for BackgroundReporter {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// The reporting loop: every `report_interval_secs`, drain the counts, report them to the sidecar
/// over gRPC, and swap in the returned over-limit set. On any failure, keep the current set until it
/// exceeds `max_staleness_secs`, then fail open by clearing it.
async fn run_reporter(
    config: DynamicRlsThrottleConfig,
    transport: GrpcTransport,
    shared: Arc<SharedThrottleState>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(config.report_interval_secs));
    // The first tick fires immediately; skip it so we report on a real window boundary.
    interval.tick().await;

    let mut last_ok = Instant::now();

    loop {
        interval.tick().await;

        // Drain + zero the counts atomically so counting for the next window starts fresh.
        let snapshot: HashMap<Key, u64> = match shared.counts.lock() {
            Ok(mut counts) => std::mem::take(&mut *counts),
            Err(_) => continue,
        };

        match report(&config, &transport, snapshot).await {
            Ok(over_limit) => {
                shared.over_limit.store(Arc::new(over_limit));
                last_ok = Instant::now();
            }
            Err(error) => {
                let staleness = last_ok.elapsed();
                if staleness > Duration::from_secs(config.max_staleness_secs) {
                    // Fail open: the decision is too old to trust, so stop dropping.
                    if !shared.over_limit.load().is_empty() {
                        shared.over_limit.store(Arc::new(HashSet::new()));
                    }
                    warn!(
                        message = "Failed to refresh over-limit set; failing open.",
                        %error,
                        staleness_secs = staleness.as_secs(),
                        internal_log_rate_limit = true,
                    );
                } else {
                    // Keep the current set; a transient failure inside the staleness budget.
                    debug!(
                        message = "Failed to refresh over-limit set; keeping current set.",
                        %error,
                        internal_log_rate_limit = true,
                    );
                }
            }
        }
    }
}

/// Report one window's counts to the sidecar's gRPC `ReportCounts` and return the over-limit set.
async fn report(
    config: &DynamicRlsThrottleConfig,
    transport: &GrpcTransport,
    snapshot: HashMap<Key, u64>,
) -> crate::Result<HashSet<Key>> {
    let request_desc = transport.method.input();

    // Build the request as proto3-JSON, then deserialize into a DynamicMessage against the
    // descriptor (int64 `count` as a string, per proto-JSON). An empty window still sends an empty
    // `counts` list so the sidecar's count-of-0 recheck runs and over-quota combos can recover.
    let counts: Vec<_> = snapshot
        .into_iter()
        .map(|((topic, system), count)| {
            serde_json::json!({ "topic": topic, "system": system, "count": count.to_string() })
        })
        .collect();
    let request_json = serde_json::json!({ "counts": counts });
    let opts = DeserializeOptions::new().deny_unknown_fields(true);
    let request_msg = DynamicMessage::deserialize_with_options(request_desc, &request_json, &opts)
        .map_err(|e| format!("build ReportCounts request message: {e}"))?;

    let uri = grpc_uri(&transport.endpoint, &transport.method)?;
    let http_req: Request<Body> = Request::builder()
        .uri(uri)
        .method("POST")
        .header("content-type", "application/grpc+proto")
        .header("te", "trailers")
        .header("grpc-encoding", "identity")
        .body(Body::from(encode_grpc_message(request_msg.encode_to_vec())))?;

    // Bound the whole call — request plus draining body + trailers — with the timeout. The client
    // resolves at response headers, so a stall mid-body would otherwise wedge the reporter loop and
    // the staleness fail-open could never run.
    let output_desc = transport.method.output();
    let over_limit = tokio::time::timeout(
        Duration::from_secs(config.report_timeout_secs),
        async {
            let response = transport.client.request(http_req).await?;
            let response_headers = response.headers().clone();

            // gRPC status is in the initial HEADERS (Trailers-Only error) or the HTTP/2 trailers —
            // drain data frames then read trailers explicitly (hyper 0.14 `to_bytes` drops them).
            use hyper::body::HttpBody as _;
            let mut body_stream = response.into_body();
            let mut body = BytesMut::new();
            while let Some(chunk) =
                std::future::poll_fn(|cx| std::pin::Pin::new(&mut body_stream).poll_data(cx)).await
            {
                body.extend_from_slice(&chunk?);
            }
            let trailers =
                std::future::poll_fn(|cx| std::pin::Pin::new(&mut body_stream).poll_trailers(cx))
                    .await?;

            let (status, message) = resolve_grpc_status(&response_headers, trailers.as_ref());
            if status != 0 {
                return Err(crate::Error::from(format!(
                    "sidecar ReportCounts returned gRPC status {status}: {}",
                    message.unwrap_or_else(|| "unknown error".to_string())
                )));
            }

            let message_bytes = decode_grpc_frame(body.freeze())?;
            let response_msg = DynamicMessage::decode(output_desc, &message_bytes[..])
                .map_err(|e| format!("decode ReportCounts response: {e}"))?;
            crate::Result::Ok(parse_over_limit(&response_msg))
        },
    )
    .await??;

    Ok(over_limit)
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::io::Write as _;
    use std::sync::Mutex as StdMutex;
    use std::task::Poll;

    use hyper::service::{make_service_fn, service_fn};
    use hyper::{Response, Server, server::conn::AddrStream};
    use prost_reflect::prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        MethodDescriptorProto, ServiceDescriptorProto, field_descriptor_proto,
    };
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    use super::*;
    use crate::{
        event::{Event, LogEvent},
        test_util::addr::next_addr,
        test_util::components::assert_transform_compliance,
        transforms::test::create_topology,
    };

    // --- In-memory descriptor fixture ------------------------------------------------------------
    //
    // The shape of `report_counts.proto`, built in memory (the `token_manager` test pattern) so the
    // tests carry no snapshot of the universe proto and need no mounted `.pb`. Loading the real
    // `--include_imports` protoset in `prost_reflect` was validated separately.

    fn optional_field(name: &str, number: i32, ty: field_descriptor_proto::Type) -> FieldDescriptorProto {
        FieldDescriptorProto {
            name: Some(name.into()),
            number: Some(number),
            label: Some(field_descriptor_proto::Label::Optional as i32),
            r#type: Some(ty as i32),
            ..Default::default()
        }
    }

    fn message_field(name: &str, number: i32, type_name: &str, repeated: bool) -> FieldDescriptorProto {
        let label = if repeated {
            field_descriptor_proto::Label::Repeated
        } else {
            field_descriptor_proto::Label::Optional
        };
        FieldDescriptorProto {
            name: Some(name.into()),
            number: Some(number),
            label: Some(label as i32),
            r#type: Some(field_descriptor_proto::Type::Message as i32),
            type_name: Some(type_name.into()),
            ..Default::default()
        }
    }

    /// Build the fixture `FileDescriptorSet` bytes for the `ReportCounts` service.
    fn report_counts_fds_bytes() -> Vec<u8> {
        use field_descriptor_proto::Type;

        let topic_system_count = DescriptorProto {
            name: Some("TopicSystemCount".into()),
            field: vec![
                optional_field("topic", 1, Type::String),
                optional_field("system", 2, Type::String),
                optional_field("count", 3, Type::Int64),
            ],
            ..Default::default()
        };
        let report_counts_request = DescriptorProto {
            name: Some("ReportCountsRequest".into()),
            field: vec![message_field(
                "counts",
                1,
                ".databricks.woodchuck.vectoraggregatorsidecar.TopicSystemCount",
                true,
            )],
            ..Default::default()
        };
        let topic_system = DescriptorProto {
            name: Some("TopicSystem".into()),
            field: vec![
                optional_field("topic", 1, Type::String),
                optional_field("system", 2, Type::String),
            ],
            ..Default::default()
        };
        let report_counts_response = DescriptorProto {
            name: Some("ReportCountsResponse".into()),
            field: vec![message_field(
                "over_limit",
                1,
                ".databricks.woodchuck.vectoraggregatorsidecar.TopicSystem",
                true,
            )],
            ..Default::default()
        };
        let service = ServiceDescriptorProto {
            name: Some("ReportCountsService".into()),
            method: vec![MethodDescriptorProto {
                name: Some("ReportCounts".into()),
                input_type: Some(
                    ".databricks.woodchuck.vectoraggregatorsidecar.ReportCountsRequest".into(),
                ),
                output_type: Some(
                    ".databricks.woodchuck.vectoraggregatorsidecar.ReportCountsResponse".into(),
                ),
                ..Default::default()
            }],
            ..Default::default()
        };
        let file = FileDescriptorProto {
            name: Some("report_counts.proto".into()),
            package: Some("databricks.woodchuck.vectoraggregatorsidecar".into()),
            // proto2 matches the real proto; the transport only depends on field numbers/types.
            syntax: Some("proto2".into()),
            message_type: vec![
                topic_system_count,
                report_counts_request,
                topic_system,
                report_counts_response,
            ],
            service: vec![service],
            ..Default::default()
        };
        FileDescriptorSet { file: vec![file] }.encode_to_vec()
    }

    fn report_counts_method() -> MethodDescriptor {
        let fds = prost_reflect::prost_types::FileDescriptorSet::decode(
            &report_counts_fds_bytes()[..],
        )
        .expect("decode fixture FDS");
        let pool = DescriptorPool::from_file_descriptor_set(fds).expect("fixture pool");
        pool.get_service_by_name(REPORT_COUNTS_SERVICE)
            .expect("fixture service")
            .methods()
            .find(|m| m.name() == REPORT_COUNTS_METHOD)
            .expect("fixture method")
    }

    /// A `GrpcTransport` wired to `endpoint`, using the in-memory fixture descriptor. No file, no
    /// network until the reporter actually fires.
    fn test_transport(endpoint: &str) -> GrpcTransport {
        GrpcTransport {
            client: hyper::Client::builder()
                .http2_only(true)
                .build(HttpConnector::new()),
            endpoint: endpoint.parse().expect("endpoint uri"),
            method: report_counts_method(),
        }
    }

    // --- Hermetic in-process h2c gRPC sidecar ----------------------------------------------------
    //
    // A minimal h2c server answering `ReportCounts`: decode the framed request, record its counts,
    // reply with a framed `ReportCountsResponse` + a `grpc-status: 0` trailer. Exercises the real
    // return path (framing → h2c → parse → over-limit swap) as a regression harness — not a
    // substitute for the real sidecar on a pod, which a mock can't stand in for.

    /// Records what the fake sidecar received across calls.
    #[derive(Default)]
    struct SidecarLog {
        /// The `(topic, system, count)` rows decoded from each request's `counts`.
        requests: Vec<Vec<(String, String, i64)>>,
    }

    /// Spawn the fake sidecar on `addr`, always answering with `over_limit`. Returns once bound.
    async fn spawn_fake_sidecar(
        addr: std::net::SocketAddr,
        over_limit: Vec<(String, String)>,
        log: Arc<StdMutex<SidecarLog>>,
    ) {
        let method = report_counts_method();
        let make_svc = make_service_fn(move |_conn: &AddrStream| {
            let method = method.clone();
            let over_limit = over_limit.clone();
            let log = Arc::clone(&log);
            async move {
                Ok::<_, Infallible>(service_fn(move |req: Request<Body>| {
                    let method = method.clone();
                    let over_limit = over_limit.clone();
                    let log = Arc::clone(&log);
                    async move {
                        // The client must target the fully-qualified gRPC path.
                        assert_eq!(
                            req.uri().path(),
                            "/databricks.woodchuck.vectoraggregatorsidecar.ReportCountsService/ReportCounts"
                        );

                        // Read + decode the framed request, record its counts. (Drain via the
                        // HttpBody trait — `hyper::body::to_bytes` is deprecated under this fork's
                        // `#[deny(warnings)]`; the request carries no trailers we need here.)
                        let body = http_body::Body::collect(req.into_body())
                            .await
                            .unwrap()
                            .to_bytes();
                        let msg_bytes = decode_grpc_frame(body).expect("frame");
                        let request_msg =
                            DynamicMessage::decode(method.input(), &msg_bytes[..]).expect("decode req");
                        let mut rows = Vec::new();
                        if let Some(Value::List(counts)) =
                            request_msg.get_field_by_name("counts").map(|v| v.into_owned())
                        {
                            for c in counts {
                                if let Value::Message(m) = c {
                                    let s = |n: &str| {
                                        m.get_field_by_name(n)
                                            .and_then(|v| v.as_str().map(str::to_owned))
                                            .unwrap_or_default()
                                    };
                                    let count = m
                                        .get_field_by_name("count")
                                        .and_then(|v| v.as_i64())
                                        .unwrap_or_default();
                                    rows.push((s("topic"), s("system"), count));
                                }
                            }
                        }
                        log.lock().unwrap().requests.push(rows);

                        // Build the framed ReportCountsResponse.
                        let over_json: Vec<_> = over_limit
                            .iter()
                            .map(|(t, s)| serde_json::json!({ "topic": t, "system": s }))
                            .collect();
                        let resp_json = serde_json::json!({ "over_limit": over_json });
                        let opts = DeserializeOptions::new();
                        let resp_msg = DynamicMessage::deserialize_with_options(
                            method.output(),
                            &resp_json,
                            &opts,
                        )
                        .expect("build resp");
                        let framed = encode_grpc_message(resp_msg.encode_to_vec());

                        // Deliver body then a gRPC-OK trailer (the real return path reads trailers).
                        let (mut sender, resp_body) = Body::channel();
                        tokio::spawn(async move {
                            let _ = sender.send_data(Bytes::from(framed)).await;
                            let mut trailers = http::HeaderMap::new();
                            trailers
                                .insert("grpc-status", http::HeaderValue::from_static("0"));
                            let _ = sender.send_trailers(trailers).await;
                        });
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(200)
                                .header("content-type", "application/grpc")
                                .body(resp_body)
                                .unwrap(),
                        )
                    }
                }))
            }
        });

        // h2c (prior-knowledge HTTP/2, no TLS) to match the client.
        tokio::spawn(async move {
            Server::bind(&addr)
                .http2_only(true)
                .serve(make_svc)
                .await
                .unwrap();
        });
        // Give the listener a moment to bind before the client connects.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // --- Unit tests: framing, URI, response parsing ----------------------------------------------

    #[test]
    fn grpc_framing_roundtrips() {
        let framed = encode_grpc_message(vec![1, 2, 3, 4]);
        // 1 flag byte + 4 length bytes + payload.
        assert_eq!(framed.len(), 9);
        assert_eq!(framed[0], 0); // uncompressed
        assert_eq!(&framed[1..5], &4u32.to_be_bytes());
        let body = decode_grpc_frame(Bytes::from(framed)).unwrap();
        assert_eq!(&body[..], &[1, 2, 3, 4]);
    }

    #[test]
    fn decode_grpc_frame_rejects_short_and_compressed() {
        // Fewer than 5 framing bytes.
        assert!(decode_grpc_frame(Bytes::from_static(&[0, 0, 0])).is_err());
        // Compression flag set — this client sends/expects identity only.
        let mut compressed = vec![1u8];
        compressed.extend_from_slice(&0u32.to_be_bytes());
        assert!(decode_grpc_frame(Bytes::from(compressed)).is_err());
        // Advertised length exceeds the actual payload.
        let mut truncated = vec![0u8];
        truncated.extend_from_slice(&10u32.to_be_bytes());
        truncated.extend_from_slice(&[1, 2]);
        assert!(decode_grpc_frame(Bytes::from(truncated)).is_err());
    }

    #[test]
    fn grpc_uri_appends_service_and_method_path() {
        let method = report_counts_method();
        let endpoint: Uri = "http://127.0.0.1:8090".parse().unwrap();
        let uri = grpc_uri(&endpoint, &method).unwrap();
        assert_eq!(
            uri.to_string(),
            "http://127.0.0.1:8090/databricks.woodchuck.vectoraggregatorsidecar.ReportCountsService/ReportCounts"
        );
        // A trailing slash on the endpoint must not double up.
        let endpoint: Uri = "http://127.0.0.1:8090/".parse().unwrap();
        let uri = grpc_uri(&endpoint, &method).unwrap();
        assert!(!uri.to_string().contains("8090//"));
    }

    #[test]
    fn parse_over_limit_filters_empty_entries() {
        let method = report_counts_method();
        let json = serde_json::json!({
            "over_limit": [
                {"topic": "spark-log", "system": "telemetry"},
                {"topic": "", "system": "telemetry"},      // empty topic → dropped
                {"topic": "spark-log", "system": ""},        // empty system → dropped
            ]
        });
        let msg = DynamicMessage::deserialize_with_options(
            method.output(),
            &json,
            &DeserializeOptions::new(),
        )
        .unwrap();
        let set = parse_over_limit(&msg);
        assert_eq!(set.len(), 1);
        assert!(set.contains(&("spark-log".to_string(), "telemetry".to_string())));
    }

    // --- Descriptor loading + fail-open ----------------------------------------------------------

    #[test]
    fn load_report_counts_method_from_file() {
        // A real descriptor file on disk resolves the service + method.
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(&report_counts_fds_bytes()).unwrap();
        let method = load_report_counts_method(f.path()).expect("load ok");
        assert_eq!(method.name(), REPORT_COUNTS_METHOD);
        assert_eq!(method.parent_service().full_name(), REPORT_COUNTS_SERVICE);
    }

    #[test]
    fn load_report_counts_method_errors_on_bad_descriptor() {
        // Missing file.
        assert!(load_report_counts_method(std::path::Path::new("/nonexistent/x.pb")).is_err());
        // Present but not a FileDescriptorSet.
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"not a descriptor set").unwrap();
        assert!(load_report_counts_method(f.path()).is_err());
    }

    #[tokio::test]
    async fn build_fails_open_when_descriptor_missing() {
        // A missing descriptor must NOT error the build — it must produce a disabled transform that
        // forwards everything, even a combo that is "over limit". Fail open, never closed or crash.
        let config = DynamicRlsThrottleConfig {
            sidecar_proto_descriptor_path: PathBuf::from("/nonexistent/report_counts.pb"),
            ..Default::default()
        };
        let transform = config.build(&TransformContext::default()).await;
        assert!(transform.is_ok(), "descriptor load failure must fail open, not error the build");

        // Disabled transform (no transport); seed an over-limit combo and confirm it's forwarded.
        let throttle = build_throttle_no_transport(&config);
        throttle
            .shared
            .over_limit
            .store(Arc::new(HashSet::from([(
                "spark-log".to_string(),
                "telemetry".to_string(),
            )])));

        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        let mut out = Box::new(throttle).transform(Box::pin(rx));
        tx.try_send(log_with("spark-log", "telemetry")).unwrap();
        // Disabled: the "over-limit" log is forwarded, not dropped.
        assert!(out.next().await.is_some());
        tx.disconnect();
        assert_eq!(out.next().await, None);
    }

    // --- Test config + construction helpers ------------------------------------------------------

    /// Build a log event with the default topic/system paths populated.
    fn log_with(topic: &str, system: &str) -> Event {
        let mut log = LogEvent::default();
        log.insert("logMetadata.topic", topic);
        log.insert("message.kubernetes.pod_labels.system", system);
        log.into()
    }

    fn test_config(endpoint: String) -> DynamicRlsThrottleConfig {
        DynamicRlsThrottleConfig {
            sidecar_endpoint: endpoint,
            // Fast interval so the reporter fires quickly in tests. Timeout must be <= interval
            // and staleness >= interval (enforced in `build`).
            report_interval_secs: 1,
            report_timeout_secs: 1,
            max_staleness_secs: 60,
            ..Default::default()
        }
    }

    /// Long interval so the reporter can't drain the counts mid-test (and staleness >= interval, per
    /// build validation), with an explicit enforce mode (and no system allowlist).
    fn hotpath_config(mode: EnforceMode) -> DynamicRlsThrottleConfig {
        DynamicRlsThrottleConfig {
            report_interval_secs: 3600,
            max_staleness_secs: 3600,
            enforce: EnforceConfig {
                mode,
                systems: Vec::new(),
            },
            ..test_config(default_sidecar_endpoint_string())
        }
    }

    /// An enabled transform (fixture descriptor + the config's endpoint), bypassing `build`'s file
    /// load — the hot-path/reporter tests drive this directly.
    fn build_throttle(config: &DynamicRlsThrottleConfig) -> DynamicRlsThrottle {
        let transport = test_transport(&config.sidecar_endpoint);
        let topic_path = parse_target_path(&config.topic_field).unwrap();
        let system_path = parse_target_path(&config.system_field).unwrap();
        DynamicRlsThrottle::new(config.clone(), Some(transport), topic_path, system_path)
    }

    /// Construct a DISABLED transform (no transport) — models the descriptor-load fail-open state.
    fn build_throttle_no_transport(config: &DynamicRlsThrottleConfig) -> DynamicRlsThrottle {
        let topic_path = parse_target_path(&config.topic_field).unwrap();
        let system_path = parse_target_path(&config.system_field).unwrap();
        DynamicRlsThrottle::new(config.clone(), None, topic_path, system_path)
    }

    fn throttle_with_over_limit(
        config: &DynamicRlsThrottleConfig,
        over_limit: HashSet<Key>,
    ) -> DynamicRlsThrottle {
        let throttle = build_throttle(config);
        throttle.shared.over_limit.store(Arc::new(over_limit));
        throttle
    }

    // --- Build-time validation -------------------------------------------------------------------

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<DynamicRlsThrottleConfig>();
    }

    #[tokio::test]
    async fn build_rejects_zero_timings() {
        let ctx = TransformContext::default();

        // Zero report interval would panic `tokio::time::interval`; must be rejected at build.
        let zero_interval = DynamicRlsThrottleConfig {
            report_interval_secs: 0,
            ..Default::default()
        };
        assert!(zero_interval.build(&ctx).await.is_err());

        // Zero timeout would fail every report immediately; must be rejected at build.
        let zero_timeout = DynamicRlsThrottleConfig {
            report_timeout_secs: 0,
            ..Default::default()
        };
        assert!(zero_timeout.build(&ctx).await.is_err());

        // The defaults build successfully (the default descriptor path won't exist in the test
        // env, so this also confirms build fails open rather than erroring).
        assert!(
            DynamicRlsThrottleConfig::default()
                .build(&ctx)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn build_rejects_inconsistent_timings() {
        let ctx = TransformContext::default();

        // timeout > interval: a slow report could overrun the next cycle.
        let timeout_gt_interval = DynamicRlsThrottleConfig {
            report_interval_secs: 5,
            report_timeout_secs: 10,
            ..Default::default()
        };
        assert!(timeout_gt_interval.build(&ctx).await.is_err());

        // staleness < interval: would fail open before even one refresh could land.
        let staleness_lt_interval = DynamicRlsThrottleConfig {
            report_interval_secs: 30,
            max_staleness_secs: 10,
            ..Default::default()
        };
        assert!(staleness_lt_interval.build(&ctx).await.is_err());

        // Equalities are allowed (timeout == interval, staleness == interval).
        let boundary = DynamicRlsThrottleConfig {
            report_interval_secs: 10,
            report_timeout_secs: 10,
            max_staleness_secs: 10,
            ..Default::default()
        };
        assert!(boundary.build(&ctx).await.is_ok());
    }

    // --- Hot-path: drop / shadow / counting / metrics (transport-independent) ---------------------

    #[tokio::test]
    async fn enforce_drops_over_limit_and_counts_passed_through() {
        // enforce.mode = all: over-limit logs are dropped for every system.
        let config = hotpath_config(EnforceMode::All);
        // (spark-log, telemetry) is over quota; (background-activity-log, auth-v2) is not.
        let mut over_limit = HashSet::new();
        over_limit.insert(("spark-log".to_string(), "telemetry".to_string()));

        let throttle = throttle_with_over_limit(&config, over_limit);
        let shared = Arc::clone(&throttle.shared);

        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        let mut out = Box::new(throttle).transform(Box::pin(rx));

        // Over-limit event is dropped.
        tx.try_send(log_with("spark-log", "telemetry")).unwrap();
        // Under-limit events pass through.
        tx.try_send(log_with("background-activity-log", "auth-v2"))
            .unwrap();
        tx.try_send(log_with("background-activity-log", "auth-v2"))
            .unwrap();
        // Event missing a system: forwarded, not counted.
        tx.try_send(log_with("background-activity-log", ""))
            .unwrap();

        // Exactly the three forwarded events come out (the over-limit one is dropped).
        for _ in 0..3 {
            assert!(out.next().await.is_some());
        }

        tx.disconnect();
        assert_eq!(out.next().await, None);

        // Only the two passed-through, keyed logs were counted; the dropped and keyless logs were not.
        let counts = shared.counts.lock().unwrap();
        assert_eq!(
            counts.get(&("background-activity-log".to_string(), "auth-v2".to_string())),
            Some(&2)
        );
        assert_eq!(counts.len(), 1);
    }

    #[tokio::test]
    async fn shadow_forwards_over_limit_but_still_excludes_from_counts() {
        // enforce.mode = none: over-limit logs are forwarded, but still excluded from the RLSv2
        // counts (same rule as enforce), so the shadow over-limit count predicts enforce's drops.
        let config = hotpath_config(EnforceMode::None);
        let mut over_limit = HashSet::new();
        over_limit.insert(("spark-log".to_string(), "telemetry".to_string()));

        let throttle = throttle_with_over_limit(&config, over_limit);
        let shared = Arc::clone(&throttle.shared);

        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        let mut out = Box::new(throttle).transform(Box::pin(rx));

        // Over-limit event — forwarded in shadow, not dropped.
        tx.try_send(log_with("spark-log", "telemetry")).unwrap();
        // Under-limit events pass through and are counted.
        tx.try_send(log_with("background-activity-log", "auth-v2"))
            .unwrap();
        tx.try_send(log_with("background-activity-log", "auth-v2"))
            .unwrap();

        // All THREE come out — including the over-limit one (shadow drops nothing).
        for _ in 0..3 {
            assert!(out.next().await.is_some());
        }

        tx.disconnect();
        assert_eq!(out.next().await, None);

        // Counting is identical to enforce: the over-limit combo is NOT counted (excluded from
        // the RLSv2 report), only the two under-limit logs are.
        let counts = shared.counts.lock().unwrap();
        assert_eq!(
            counts.get(&("background-activity-log".to_string(), "auth-v2".to_string())),
            Some(&2)
        );
        assert!(
            !counts.contains_key(&("spark-log".to_string(), "telemetry".to_string())),
            "over-limit combo must be excluded from counts even in shadow mode"
        );
        assert_eq!(counts.len(), 1);
    }

    // --- Half-shadow: per-event effective mode (LP-1814) -----------------------------------------

    #[tokio::test]
    async fn enforce_systems_drops_only_listed_systems() {
        // Half-shadow: mode = selected, allowlist = [reyden]. Over-limit logs for reyden are
        // dropped; over-limit logs for any other system are shadow-forwarded.
        let config = DynamicRlsThrottleConfig {
            enforce: EnforceConfig {
                mode: EnforceMode::Selected,
                systems: vec!["reyden".to_string()],
            },
            ..hotpath_config(EnforceMode::Selected)
        };
        let mut over_limit = HashSet::new();
        over_limit.insert(("spark-log".to_string(), "reyden".to_string()));
        over_limit.insert(("spark-log".to_string(), "auth-v2".to_string()));
        let throttle = throttle_with_over_limit(&config, over_limit);

        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        let mut out = Box::new(throttle).transform(Box::pin(rx));

        // reyden over-limit → dropped; auth-v2 over-limit → forwarded (shadowed).
        tx.try_send(log_with("spark-log", "reyden")).unwrap();
        tx.try_send(log_with("spark-log", "auth-v2")).unwrap();
        tx.disconnect();

        // Exactly one event (the shadowed auth-v2 one) comes out.
        let first = out.next().await.expect("one event forwarded");
        let log = first.as_log();
        assert_eq!(
            log.get(".message.kubernetes.pod_labels.system")
                .unwrap()
                .to_string_lossy(),
            "auth-v2",
            "only the non-enforced system's log should be forwarded"
        );
        assert_eq!(out.next().await, None, "the reyden log must have been dropped");
    }

    #[tokio::test]
    async fn enforce_all_drops_every_system() {
        // mode = all: every over-limit system is dropped regardless of the allowlist.
        let config = DynamicRlsThrottleConfig {
            enforce: EnforceConfig {
                mode: EnforceMode::All,
                systems: vec!["reyden".to_string()], // ignored when mode = all
            },
            ..hotpath_config(EnforceMode::All)
        };
        let mut over_limit = HashSet::new();
        over_limit.insert(("spark-log".to_string(), "reyden".to_string()));
        over_limit.insert(("spark-log".to_string(), "auth-v2".to_string()));
        let throttle = throttle_with_over_limit(&config, over_limit);

        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        let mut out = Box::new(throttle).transform(Box::pin(rx));

        tx.try_send(log_with("spark-log", "reyden")).unwrap();
        tx.try_send(log_with("spark-log", "auth-v2")).unwrap();
        tx.disconnect();

        // Both over-limit logs are dropped; nothing comes out.
        assert_eq!(out.next().await, None);
    }

    #[tokio::test]
    async fn enforce_selected_with_empty_allowlist_drops_nothing() {
        // mode = selected but an empty `systems` allowlist: no system is in scope, so every
        // over-limit log is shadow-forwarded. Guards against a rollout that selects enforcement
        // but forgets to list any systems.
        let config = DynamicRlsThrottleConfig {
            enforce: EnforceConfig {
                mode: EnforceMode::Selected,
                systems: vec![],
            },
            ..hotpath_config(EnforceMode::Selected)
        };
        let mut over_limit = HashSet::new();
        over_limit.insert(("spark-log".to_string(), "telemetry".to_string()));
        let throttle = throttle_with_over_limit(&config, over_limit);

        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        let mut out = Box::new(throttle).transform(Box::pin(rx));
        tx.try_send(log_with("spark-log", "telemetry")).unwrap();
        tx.disconnect();

        assert!(out.next().await.is_some(), "with no enforce scope, nothing is dropped");
        assert_eq!(out.next().await, None);
    }

    #[tokio::test]
    async fn emits_expected_metrics_per_mode() {
        // The recorder is a name set: `contains_name_once` checks presence/absence, not count.
        use vector_lib::event_test_util::{clear_recorded_events, contains_name_once};

        // Drive one log — either an over-limit combo or an under-limit one — through the transform
        // in the given mode, then drain the output.
        async fn run(mode: EnforceMode, over_limit_log: bool) {
            let config = hotpath_config(mode);
            let mut over_limit = HashSet::new();
            over_limit.insert(("spark-log".to_string(), "telemetry".to_string()));
            let throttle = throttle_with_over_limit(&config, over_limit);

            let (mut tx, rx) = futures::channel::mpsc::channel(10);
            let mut out = Box::new(throttle).transform(Box::pin(rx));

            let log = if over_limit_log {
                log_with("spark-log", "telemetry") // in the over-limit set
            } else {
                log_with("background-activity-log", "auth-v2") // under limit
            };
            tx.try_send(log).unwrap();
            tx.disconnect();
            while out.next().await.is_some() {}
        }

        // Over-limit log: inflow + over-limit counter fire in every mode. The `mode` tag value and
        // the drop-vs-forward action are covered by enforce_drops_* / shadow_forwards_*.
        for mode in [EnforceMode::None, EnforceMode::All] {
            clear_recorded_events();
            run(mode, true).await;
            assert!(contains_name_once("DynamicRlsThrottleInflow").is_ok());
            assert!(
                contains_name_once("DynamicRlsThrottleOverLimit").is_ok(),
                "over-limit log must emit the over-limit metric (mode={mode:?})"
            );
        }

        // Under-limit log: inflow fires, over-limit counter does NOT — in every mode.
        for mode in [EnforceMode::None, EnforceMode::All] {
            clear_recorded_events();
            run(mode, false).await;
            assert!(contains_name_once("DynamicRlsThrottleInflow").is_ok());
            assert!(
                contains_name_once("DynamicRlsThrottleOverLimit").is_err(),
                "under-limit log must not emit the over-limit metric (mode={mode:?})"
            );
        }
    }

    #[tokio::test]
    async fn emit_inflow_metric_false_suppresses_inflow_only() {
        use vector_lib::event_test_util::{clear_recorded_events, contains_name_once};

        // Over-quota log with inflow disabled: inflow is suppressed, over-limit still fires.
        let config = DynamicRlsThrottleConfig {
            emit_inflow_metric: false,
            ..hotpath_config(EnforceMode::None)
        };
        let mut over_limit = HashSet::new();
        over_limit.insert(("spark-log".to_string(), "telemetry".to_string()));
        let throttle = throttle_with_over_limit(&config, over_limit);

        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        let mut out = Box::new(throttle).transform(Box::pin(rx));

        clear_recorded_events();
        tx.try_send(log_with("spark-log", "telemetry")).unwrap();
        tx.disconnect();
        while out.next().await.is_some() {}

        assert!(
            contains_name_once("DynamicRlsThrottleInflow").is_err(),
            "inflow metric must be suppressed when emit_inflow_metric is false"
        );
        assert!(
            contains_name_once("DynamicRlsThrottleOverLimit").is_ok(),
            "over-limit metric is unaffected by emit_inflow_metric"
        );
    }

    #[tokio::test]
    async fn missing_topic_passes_through_uncounted() {
        // A log missing the topic field can't map to an RLS policy, so it is forwarded and never
        // counted — even if a same-system combo happens to be over quota.
        let config = DynamicRlsThrottleConfig {
            report_interval_secs: 3600,
            max_staleness_secs: 3600,
            ..test_config(default_sidecar_endpoint_string())
        };
        let mut over_limit = HashSet::new();
        over_limit.insert(("spark-log".to_string(), "telemetry".to_string()));
        let throttle = throttle_with_over_limit(&config, over_limit);
        let shared = Arc::clone(&throttle.shared);

        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        let mut out = Box::new(throttle).transform(Box::pin(rx));

        // Missing topic (empty string) — forwarded, not counted, never dropped.
        tx.try_send(log_with("", "telemetry")).unwrap();
        assert!(out.next().await.is_some());

        tx.disconnect();
        assert_eq!(out.next().await, None);
        assert!(shared.counts.lock().unwrap().is_empty());
    }

    // --- End-to-end gRPC transport against a hermetic h2c sidecar ---------------------------------

    #[tokio::test]
    async fn reporter_reports_counts_over_grpc_and_swaps_in_over_limit_set() {
        // The real return path end-to-end: the reporter frames the drained counts, POSTs them to a
        // real (in-process) h2c gRPC server, and swaps in the over-limit set the server returns.
        let (_guard, addr) = next_addr();
        let log = Arc::new(StdMutex::new(SidecarLog::default()));
        spawn_fake_sidecar(
            addr,
            vec![("spark-log".to_string(), "telemetry".to_string())],
            Arc::clone(&log),
        )
        .await;

        let config = test_config(format!("http://{addr}"));
        // Seed a stale entry so we also prove convergence (fail-open recovery): it must be replaced
        // by the server's response, not merged.
        let throttle = throttle_with_over_limit(
            &config,
            HashSet::from([("stale-topic".to_string(), "stale-system".to_string())]),
        );
        let shared = Arc::clone(&throttle.shared);
        shared.counts.lock().unwrap().insert(
            ("background-activity-log".to_string(), "auth-v2".to_string()),
            7,
        );

        let (tx, rx) = futures::channel::mpsc::channel::<Event>(1);
        let mut out = Box::new(throttle).transform(Box::pin(rx));
        assert_eq!(Poll::Pending, futures::poll!(out.next()));

        let expected = ("spark-log".to_string(), "telemetry".to_string());
        let stale = ("stale-topic".to_string(), "stale-system".to_string());
        let mut swapped = false;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = futures::poll!(out.next());
            let set = shared.over_limit.load();
            if set.contains(&expected) && !set.contains(&stale) {
                swapped = true;
                break;
            }
        }
        assert!(swapped, "reporter did not swap in the gRPC over-limit set");

        // The counts map was drained to zero by the report.
        assert!(shared.counts.lock().unwrap().is_empty());

        // The sidecar received the drained count over the wire.
        let requests = log.lock().unwrap();
        assert!(!requests.requests.is_empty(), "sidecar received no request");
        assert!(
            requests
                .requests
                .iter()
                .any(|rows| rows.contains(&(
                    "background-activity-log".to_string(),
                    "auth-v2".to_string(),
                    7
                ))),
            "sidecar did not receive the seeded count over gRPC"
        );

        drop(tx);
    }

    #[tokio::test]
    async fn reports_empty_window_over_grpc() {
        // Even with no passed-through logs, the reporter must still call the sidecar with an empty
        // counts list so the sidecar's count-of-0 recheck runs and over-quota combos can recover.
        let (_guard, addr) = next_addr();
        let log = Arc::new(StdMutex::new(SidecarLog::default()));
        spawn_fake_sidecar(addr, vec![], Arc::clone(&log)).await;

        let config = test_config(format!("http://{addr}"));
        // No counts seeded — this window is empty.
        let throttle = build_throttle(&config);

        let (tx, rx) = futures::channel::mpsc::channel::<Event>(1);
        let mut out = Box::new(throttle).transform(Box::pin(rx));
        assert_eq!(Poll::Pending, futures::poll!(out.next()));

        let mut called = false;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = futures::poll!(out.next());
            if !log.lock().unwrap().requests.is_empty() {
                called = true;
                break;
            }
        }
        assert!(called, "reporter did not call the sidecar for an empty window");
        // The empty window is reported as an empty counts list, not a skipped call.
        assert_eq!(log.lock().unwrap().requests[0].len(), 0);

        drop(tx);
    }

    #[tokio::test]
    async fn fails_open_after_staleness() {
        // The sidecar endpoint is unreachable (nothing listening), so the over-limit set is never
        // refreshed and must be cleared once it goes stale.
        let (_guard, addr) = next_addr(); // reserved but NO server started → connection refused
        let config = DynamicRlsThrottleConfig {
            report_interval_secs: 1,
            report_timeout_secs: 1,
            // Small staleness budget so fail-open triggers within a couple of report cycles.
            max_staleness_secs: 1,
            ..test_config(format!("http://{addr}"))
        };

        // Start already over quota so we can observe the fail-open clearing it.
        let throttle = throttle_with_over_limit(
            &config,
            HashSet::from([("spark-log".to_string(), "telemetry".to_string())]),
        );
        let shared = Arc::clone(&throttle.shared);

        let (tx, rx) = futures::channel::mpsc::channel::<Event>(1);
        let mut out = Box::new(throttle).transform(Box::pin(rx));
        assert_eq!(Poll::Pending, futures::poll!(out.next()));

        let mut failed_open = false;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = futures::poll!(out.next());
            if shared.over_limit.load().is_empty() {
                failed_open = true;
                break;
            }
        }
        assert!(failed_open, "over-limit set was not cleared after staleness");

        drop(tx);
    }

    #[tokio::test]
    async fn keeps_set_within_staleness_budget() {
        // The sidecar is unreachable, but the staleness budget is generous, so a transient failure
        // must NOT clear the set — the last-known decision is kept until it goes stale.
        let (_guard, addr) = next_addr(); // reserved but NO server started
        let config = DynamicRlsThrottleConfig {
            report_interval_secs: 1,
            report_timeout_secs: 1,
            // Large budget: several report cycles will fail without ever exceeding it.
            max_staleness_secs: 3600,
            ..test_config(format!("http://{addr}"))
        };

        let over_limit_key = ("spark-log".to_string(), "telemetry".to_string());
        let throttle =
            throttle_with_over_limit(&config, HashSet::from([over_limit_key.clone()]));
        let shared = Arc::clone(&throttle.shared);

        let (tx, rx) = futures::channel::mpsc::channel::<Event>(1);
        let mut out = Box::new(throttle).transform(Box::pin(rx));
        assert_eq!(Poll::Pending, futures::poll!(out.next()));

        // Let several failing report cycles elapse; the set must survive all of them.
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = futures::poll!(out.next());
            assert!(
                shared.over_limit.load().contains(&over_limit_key),
                "set was cleared despite being within the staleness budget"
            );
        }

        drop(tx);
    }

    #[tokio::test]
    async fn transform_compliance() {
        assert_transform_compliance(async move {
            // Use the fixture descriptor via a temp file so the transform is fully enabled and
            // exercises the real (enabled) event path.
            let mut f = tempfile::NamedTempFile::new().unwrap();
            f.write_all(&report_counts_fds_bytes()).unwrap();
            let config = DynamicRlsThrottleConfig {
                sidecar_proto_descriptor_path: f.path().to_path_buf(),
                ..test_config(default_sidecar_endpoint_string())
            };
            let (tx, rx) = mpsc::channel(1);
            let (topology, mut out) = create_topology(ReceiverStream::new(rx), config).await;

            let log = log_with("background-activity-log", "auth-v2");
            tx.send(log).await.unwrap();

            _ = out.recv().await;

            drop(tx);
            topology.stop().await;
            assert_eq!(out.recv().await, None);
        })
        .await
    }
}
