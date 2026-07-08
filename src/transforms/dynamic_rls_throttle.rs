use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use async_stream::stream;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use http::Request;
use serde::{Deserialize, Serialize};
use vector_lib::{
    config::clone_input_definitions,
    configurable::configurable_component,
    internal_event::{ComponentEventsDropped, Count, INTENTIONAL, InternalEventHandle, Registered},
};
use vrl::path::{OwnedTargetPath, parse_target_path};

use crate::{
    config::{DataType, Input, OutputId, TransformConfig, TransformContext, TransformOutput},
    event::{Event, EventStatus},
    http::HttpClient,
    schema,
    transforms::{TaskTransform, Transform},
};

/// The key a log is counted and throttled under: its `(topic, system)` combination.
type Key = (String, String);

const fn default_sidecar_endpoint() -> &'static str {
    "http://127.0.0.1:8090/api/reportCounts"
}

fn default_sidecar_endpoint_string() -> String {
    default_sidecar_endpoint().to_string()
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

/// Configuration for the `dynamic_rls_throttle` transform.
///
/// This transform enforces per-`(topic, system)` online quotas by dropping logs whose
/// `(topic, system)` combination is currently over quota. It does not decide quotas itself:
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
    "Drop logs whose topic/system is over its online quota, as decided by the rate-limit service."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct DynamicRlsThrottleConfig {
    /// The sidecar `reportCounts` endpoint the transform POSTs per-window counts to.
    ///
    /// The sidecar runs in the same pod and is reachable over loopback. The path is
    /// `/api/reportCounts` because the sidecar serves the gRPC method over HTTP/JSON
    /// transcoding under the framework's default `/api` prefix.
    #[serde(default = "default_sidecar_endpoint_string")]
    pub sidecar_endpoint: String,

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
}

impl Default for DynamicRlsThrottleConfig {
    fn default() -> Self {
        Self {
            sidecar_endpoint: default_sidecar_endpoint_string(),
            report_interval_secs: default_report_interval_secs(),
            report_timeout_secs: default_report_timeout_secs(),
            max_staleness_secs: default_max_staleness_secs(),
            topic_field: default_topic_field(),
            system_field: default_system_field(),
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

        let client = HttpClient::new(None, &Default::default())?;
        // Parse the key field paths once here so a malformed path fails the build loudly
        let topic_path = parse_target_path(&self.topic_field)
            .map_err(|e| format!("invalid `topic_field` path {:?}: {e}", self.topic_field))?;
        let system_path = parse_target_path(&self.system_field)
            .map_err(|e| format!("invalid `system_field` path {:?}: {e}", self.system_field))?;
        Ok(Transform::event_task(DynamicRlsThrottle::new(
            self.clone(),
            client,
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

// --- Wire structs: HTTP/JSON shape of the sidecar `reportCounts` proto (report_counts.proto).
// Field names and types match the proto exactly. `count` is an int64 serialized as a string,
// per the proto-JSON convention the sidecar transcoding uses.

#[derive(Debug, Serialize)]
struct TopicSystemCount {
    topic: String,
    system: String,
    #[serde(with = "count_as_string")]
    count: u64,
}

#[derive(Debug, Serialize)]
struct ReportCountsRequest {
    counts: Vec<TopicSystemCount>,
}

#[derive(Debug, Default, Deserialize)]
struct TopicSystem {
    #[serde(default)]
    topic: String,
    #[serde(default)]
    system: String,
}

#[derive(Debug, Default, Deserialize)]
struct ReportCountsResponse {
    #[serde(default)]
    over_limit: Vec<TopicSystem>,
}

/// (De)serialize an int64 count as a JSON string, matching proto-JSON int64 encoding.
mod count_as_string {
    use serde::Serializer;

    pub(super) fn serialize<S: Serializer>(count: &u64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&count.to_string())
    }
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
    client: HttpClient,
    shared: Arc<SharedThrottleState>,
    /// Pre-compiled key field paths, parsed once at build time (see `build`).
    topic_path: OwnedTargetPath,
    system_path: OwnedTargetPath,
    events_dropped: Registered<ComponentEventsDropped<'static, INTENTIONAL>>,
}

impl DynamicRlsThrottle {
    fn new(
        config: DynamicRlsThrottleConfig,
        client: HttpClient,
        topic_path: OwnedTargetPath,
        system_path: OwnedTargetPath,
    ) -> Self {
        Self {
            config,
            client,
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
            client,
            shared,
            topic_path,
            system_path,
            events_dropped,
        } = *self;

        // The reporter runs concurrently so the hot path never blocks on the sidecar. It is
        // aborted when the input stream ends (the returned guard's `Drop`).
        let reporter = BackgroundReporter::spawn(config, client, Arc::clone(&shared));

        Box::pin(stream! {
            // Keep the reporter alive for exactly as long as this stream; dropping it aborts the
            // background task.
            let _reporter = reporter;

            while let Some(event) = input_rx.next().await {
                match event_key(&topic_path, &system_path, &event) {
                    // Over quota: drop (and do not count it — only passed-through logs count).
                    Some(key) if shared.over_limit.load().contains(&key) => {
                        event.metadata().update_status(EventStatus::Dropped);
                        events_dropped.emit(Count(1));
                    }
                    // Under quota with a usable key: count the passed-through log and forward it.
                    Some(key) => {
                        if let Ok(mut counts) = shared.counts.lock() {
                            *counts.entry(key).or_insert(0) += 1;
                        }
                        yield event;
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
        client: HttpClient,
        shared: Arc<SharedThrottleState>,
    ) -> Self {
        let handle = tokio::spawn(run_reporter(config, client, shared));
        Self { handle }
    }
}

impl Drop for BackgroundReporter {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// The reporting loop: every `report_interval_secs`, drain the counts, POST them to the sidecar,
/// and swap in the returned over-limit set. On any failure, keep the current set until it exceeds
/// `max_staleness_secs`, then fail open by clearing it.
async fn run_reporter(
    config: DynamicRlsThrottleConfig,
    client: HttpClient,
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

        match report(&config, &client, snapshot).await {
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

/// POST one window's counts to the sidecar and parse the returned over-limit set.
async fn report(
    config: &DynamicRlsThrottleConfig,
    client: &HttpClient,
    snapshot: HashMap<Key, u64>,
) -> crate::Result<HashSet<Key>> {
    let counts = snapshot
        .into_iter()
        .map(|((topic, system), count)| TopicSystemCount {
            topic,
            system,
            count,
        })
        .collect();
    let body = crate::serde::json::to_bytes(&ReportCountsRequest { counts })?.freeze();

    let request: Request<Bytes> = Request::post(&config.sidecar_endpoint)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(body)?;

    // Bound the whole call — sending the request AND reading the response body — with the timeout.
    // `client.send` resolves as soon as the response headers arrive, so a sidecar that commits
    // headers then stalls before/mid-body would otherwise wedge the reporter loop here forever,
    // and the staleness fail-open below could never run.
    let (status, body) =
        tokio::time::timeout(Duration::from_secs(config.report_timeout_secs), async {
            let response = client.send(request.map(hyper::Body::from)).await?;
            let status = response.status();
            let body = http_body::Body::collect(response.into_body())
                .await?
                .to_bytes();
            crate::Result::Ok((status, body))
        })
        .await??;

    if !status.is_success() {
        return Err(format!("sidecar returned status {status}").into());
    }

    let parsed: ReportCountsResponse = serde_json::from_slice(&body)?;
    Ok(parsed
        .over_limit
        .into_iter()
        .filter(|ts| !ts.topic.is_empty() && !ts.system.is_empty())
        .map(|ts| (ts.topic, ts.system))
        .collect())
}

#[cfg(test)]
mod tests {
    use std::task::Poll;

    use serde_json::json;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;
    use crate::{
        event::{Event, LogEvent},
        test_util::components::assert_transform_compliance,
        transforms::test::create_topology,
    };

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

        // The defaults build successfully.
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

    /// Construct a transform from a config, parsing its key paths the same way `build` does.
    fn build_throttle(config: &DynamicRlsThrottleConfig) -> DynamicRlsThrottle {
        let client = HttpClient::new(None, &Default::default()).unwrap();
        let topic_path = parse_target_path(&config.topic_field).unwrap();
        let system_path = parse_target_path(&config.system_field).unwrap();
        DynamicRlsThrottle::new(config.clone(), client, topic_path, system_path)
    }

    fn throttle_with_over_limit(
        config: &DynamicRlsThrottleConfig,
        over_limit: HashSet<Key>,
    ) -> DynamicRlsThrottle {
        let throttle = build_throttle(config);
        throttle.shared.over_limit.store(Arc::new(over_limit));
        throttle
    }

    #[tokio::test]
    async fn drops_over_limit_and_counts_passed_through() {
        // Long report interval so the background reporter cannot drain the counts map between
        // processing the events and asserting on the counts below. Staleness must stay >= interval.
        let config = DynamicRlsThrottleConfig {
            report_interval_secs: 3600,
            max_staleness_secs: 3600,
            ..test_config(default_sidecar_endpoint_string())
        };
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
    async fn reporter_drains_counts_and_swaps_in_response_set() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/reportCounts"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "over_limit": [{"topic": "spark-log", "system": "telemetry"}]
            })))
            .mount(&server)
            .await;

        let config = test_config(format!("{}/api/reportCounts", server.uri()));
        let throttle = build_throttle(&config);
        let shared = Arc::clone(&throttle.shared);

        // Seed a count that the reporter should drain and report.
        shared.counts.lock().unwrap().insert(
            ("background-activity-log".to_string(), "auth-v2".to_string()),
            7,
        );

        let (tx, rx) = futures::channel::mpsc::channel::<Event>(1);
        let mut out = Box::new(throttle).transform(Box::pin(rx));
        // Drive the stream so the spawned reporter makes progress.
        assert_eq!(Poll::Pending, futures::poll!(out.next()));

        // Wait for the reporter to POST and swap in the response set.
        let over_limit = ("spark-log".to_string(), "telemetry".to_string());
        let mut swapped = false;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = futures::poll!(out.next());
            if shared.over_limit.load().contains(&over_limit) {
                swapped = true;
                break;
            }
        }
        assert!(swapped, "reporter did not swap in the over-limit set");

        // The counts map was drained to zero by the report.
        assert!(shared.counts.lock().unwrap().is_empty());

        // The sidecar received exactly one request carrying the drained count as a string.
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        let body: serde_json::Value = requests[0].body_json().unwrap();
        assert_eq!(
            body,
            json!({"counts": [{
                "topic": "background-activity-log",
                "system": "auth-v2",
                "count": "7"
            }]})
        );

        drop(tx);
    }

    #[tokio::test]
    async fn fails_open_after_staleness() {
        // The sidecar is reachable but errors on every report (HTTP 500), so the over-limit set is
        // never refreshed and must be cleared once it goes stale.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/reportCounts"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let config = DynamicRlsThrottleConfig {
            sidecar_endpoint: format!("{}/api/reportCounts", server.uri()),
            report_interval_secs: 1,
            report_timeout_secs: 1,
            // Small staleness budget so fail-open triggers within a couple of report cycles.
            max_staleness_secs: 1,
            ..Default::default()
        };

        // Start already over quota so we can observe the fail-open clearing it.
        let mut over_limit = HashSet::new();
        over_limit.insert(("spark-log".to_string(), "telemetry".to_string()));
        let throttle = throttle_with_over_limit(&config, over_limit);
        let shared = Arc::clone(&throttle.shared);

        let (tx, rx) = futures::channel::mpsc::channel::<Event>(1);
        let mut out = Box::new(throttle).transform(Box::pin(rx));
        // Drive the stream so the spawned reporter makes progress.
        assert_eq!(Poll::Pending, futures::poll!(out.next()));

        // Wait for the reporter to exhaust the staleness budget and clear the set.
        let mut failed_open = false;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = futures::poll!(out.next());
            if shared.over_limit.load().is_empty() {
                failed_open = true;
                break;
            }
        }
        assert!(
            failed_open,
            "over-limit set was not cleared after staleness"
        );

        drop(tx);
    }

    #[tokio::test]
    async fn keeps_set_within_staleness_budget() {
        // The sidecar errors, but the staleness budget is generous, so a transient failure must
        // NOT clear the set — the last-known decision is kept until it goes stale.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/reportCounts"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let config = DynamicRlsThrottleConfig {
            sidecar_endpoint: format!("{}/api/reportCounts", server.uri()),
            report_interval_secs: 1,
            report_timeout_secs: 1,
            // Large budget: several report cycles will fail without ever exceeding it.
            max_staleness_secs: 3600,
            ..Default::default()
        };

        let over_limit_key = ("spark-log".to_string(), "telemetry".to_string());
        let mut over_limit = HashSet::new();
        over_limit.insert(over_limit_key.clone());
        let throttle = throttle_with_over_limit(&config, over_limit);
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

        // At least one report was attempted (and failed), proving the set survived real failures.
        assert!(!server.received_requests().await.unwrap().is_empty());

        drop(tx);
    }

    #[tokio::test]
    async fn recovers_after_fail_open() {
        // After a fail-open clears the set, a subsequent successful report must repopulate it.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/reportCounts"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "over_limit": [{"topic": "spark-log", "system": "telemetry"}]
            })))
            .mount(&server)
            .await;

        // Start with an unrelated stale entry so we can watch it get replaced by the response set.
        let config = test_config(format!("{}/api/reportCounts", server.uri()));
        let mut seed = HashSet::new();
        seed.insert(("stale-topic".to_string(), "stale-system".to_string()));
        let throttle = throttle_with_over_limit(&config, seed);
        let shared = Arc::clone(&throttle.shared);

        let (tx, rx) = futures::channel::mpsc::channel::<Event>(1);
        let mut out = Box::new(throttle).transform(Box::pin(rx));
        assert_eq!(Poll::Pending, futures::poll!(out.next()));

        let expected = ("spark-log".to_string(), "telemetry".to_string());
        let stale = ("stale-topic".to_string(), "stale-system".to_string());
        let mut recovered = false;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = futures::poll!(out.next());
            let set = shared.over_limit.load();
            if set.contains(&expected) && !set.contains(&stale) {
                recovered = true;
                break;
            }
        }
        assert!(
            recovered,
            "over-limit set did not converge to the sidecar's response"
        );

        drop(tx);
    }

    #[tokio::test]
    async fn reports_empty_window() {
        // Even with no passed-through logs, the reporter must still POST an empty counts list so
        // the sidecar's count-of-0 re-check runs and over-quota combos can recover within ~N.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/reportCounts"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "over_limit": [] })))
            .mount(&server)
            .await;

        let config = test_config(format!("{}/api/reportCounts", server.uri()));
        // No counts seeded — this window is empty.
        let throttle = build_throttle(&config);

        let (tx, rx) = futures::channel::mpsc::channel::<Event>(1);
        let mut out = Box::new(throttle).transform(Box::pin(rx));
        assert_eq!(Poll::Pending, futures::poll!(out.next()));

        let mut posted = false;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = futures::poll!(out.next());
            if !server.received_requests().await.unwrap().is_empty() {
                posted = true;
                break;
            }
        }
        assert!(posted, "reporter did not POST for an empty window");

        // The empty window is reported as an empty counts list, not a skipped call.
        let requests = server.received_requests().await.unwrap();
        let body: serde_json::Value = requests[0].body_json().unwrap();
        assert_eq!(body, json!({ "counts": [] }));

        drop(tx);
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

    #[tokio::test]
    async fn transform_compliance() {
        assert_transform_compliance(async move {
            let config = test_config(default_sidecar_endpoint_string());
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
