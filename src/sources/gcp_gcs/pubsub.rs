use std::{
    collections::BTreeMap,
    num::{NonZeroU64, NonZeroUsize},
    panic,
    sync::Arc,
    time::Duration,
};

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use chrono::{DateTime, SecondsFormat, Utc};
use futures::FutureExt;
use http::Request;
use hyper::Body;
use serde::{Deserialize, Serialize, de::Error as SerdeDeError};
use serde_with::serde_as;
use snafu::Snafu;
use tokio::{pin, select};
use tracing::Instrument;
use vector_lib::{
    codecs::decoding::FramingError,
    configurable::configurable_component,
    internal_event::EventsReceived,
    internal_event::{BytesReceived, Protocol, Registered},
    source_sender::SendError,
};

use crate::{
    SourceSender,
    config::{SourceAcknowledgementsConfig, SourceContext},
    gcp::{GcpAuthenticator, PUBSUB_URL},
    http::{HttpClient, HttpError},
    internal_events::{
        PubSubMessageProcessingError, PubSubMessageProcessingSucceeded, PubSubMessageReceiveError,
        PubSubMessageReceiveSucceeded, QueueMessageCount, QueueNotificationProcessLag,
    },
    shutdown::ShutdownSignal,
    sources::ingestion_callback::IngestionCallbackClient,
    sources::util::object_storage_rate_limit::{ByteCounter, ObjectStorageRateLimiter},
};

use super::object::GcsDownloader;

// ============================================================================
// Pub/Sub configuration
// ============================================================================

/// Configuration options for the Pub/Sub subscription.
#[serde_as]
#[configurable_component]
#[derive(Clone, Debug, Derivative)]
#[derivative(Default)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    /// The Pub/Sub subscription name (short form, without the `projects/…` prefix).
    ///
    /// The source composes the full resource name as
    /// `projects/{project}/subscriptions/{subscription}`.
    #[configurable(metadata(docs::examples = "my-gcs-subscription"))]
    pub(super) subscription: String,

    /// Seconds to wait between polls when no messages are available.
    #[serde(default = "default_poll_secs")]
    #[derivative(Default(value = "default_poll_secs()"))]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    pub(super) poll_secs: u64,

    /// Maximum number of messages to pull per request.
    ///
    /// Valid range is 1–1000.
    #[serde(default = "default_max_messages")]
    #[derivative(Default(value = "default_max_messages()"))]
    #[configurable(metadata(docs::human_name = "Max Messages"))]
    #[configurable(metadata(docs::examples = 10))]
    pub(super) max_number_of_messages: u32,

    /// Whether to acknowledge (delete) successfully processed messages.
    ///
    /// Set to `false` for debugging or during initial setup.
    #[serde(default = "default_true")]
    #[derivative(Default(value = "default_true()"))]
    pub(super) acknowledge_message: bool,

    /// Whether to acknowledge messages that fail to process.
    ///
    /// When `true`, failed messages are acknowledged so they are not redelivered.
    /// When `false`, failed messages will be redelivered after the Pub/Sub
    /// acknowledgement deadline on the subscription expires.
    #[serde(default = "default_true")]
    #[derivative(Default(value = "default_true()"))]
    pub(super) acknowledge_failed_message: bool,

    /// Number of concurrent polling tasks.
    ///
    /// Defaults to the number of available CPUs.
    #[configurable(metadata(docs::type_unit = "tasks"))]
    #[configurable(metadata(docs::examples = 4))]
    pub(super) client_concurrency: Option<NonZeroUsize>,

    /// Overrides the Pub/Sub API endpoint. Defaults to the public Pub/Sub endpoint.
    #[serde(default)]
    #[configurable(metadata(docs::examples = "https://pubsub.googleapis.com"))]
    pub(super) endpoint: Option<String>,

    /// How often to sample the subscription's backlog for the `queue_message_count` gauge, in
    /// seconds. Unset (the default) disables sampling.
    ///
    /// The Pub/Sub API exposes no backlog count, so each sample reads the subscription's
    /// `num_undelivered_messages` metric from Cloud Monitoring (`timeSeries.list` in `project`),
    /// which requires `monitoring.timeSeries.list` on that project. Sampled on a dedicated timer,
    /// off the pull/acknowledge path. Cloud Monitoring publishes the metric with a delay of a few
    /// minutes, so sampling more often than every 60 seconds adds no freshness.
    #[configurable(metadata(docs::type_unit = "seconds"))]
    #[configurable(metadata(docs::examples = 60))]
    pub(super) depth_poll_secs: Option<NonZeroU64>,

    /// Overrides the Cloud Monitoring API endpoint used to sample the subscription's backlog.
    /// Defaults to the public Cloud Monitoring endpoint.
    #[serde(default)]
    #[configurable(metadata(docs::examples = "https://monitoring.googleapis.com"))]
    pub(super) monitoring_endpoint: Option<String>,
}

// ============================================================================
// Message types — must be placed after Config to satisfy file order requested.
// ============================================================================

/// The `kind` value that identifies a direct ingest message.
const DIRECT_INGEST_KIND: &str = "INGEST";

/// A custom message placed on the Pub/Sub subscription to trigger ingestion
/// of a specific GCS object without relying on native GCS notifications.
///
/// Example payload:
/// ```json
/// {"kind": "INGEST", "bucket": "my-bucket", "key": "path/to/file.log", "file_id": "f-abc-123", "log_type": "cp_logs"}
/// ```
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DirectIngestMessage {
    /// Must equal `"INGEST"`.
    pub kind: String,
    /// The GCS bucket containing the object.
    pub bucket: String,
    /// The GCS object key / path within the bucket.
    pub key: String,
    /// Unique file identifier assigned by the upstream ingestion service.
    ///
    /// Required. Used by the ingestion callback (added in stack/add-callback) to
    /// notify the upstream service of processing completion or failure.
    /// Messages without this field are rejected at deserialization time.
    pub file_id: String,
    /// Optional log type set by the upstream caller to demarcate the type of
    /// log file being processed. When present, it is stamped onto every emitted
    /// log event so downstream transforms can route on it. Optional: messages
    /// that omit it deserialize to `None` and are not rejected.
    #[serde(default)]
    pub log_type: Option<String>,
}

/// All message types recognised on the Pub/Sub subscription.
///
/// `DirectIngest` is listed first and its struct uses `deny_unknown_fields`,
/// so it is tried first during deserialization but will not false-match any
/// future message type that carries additional fields.
///
/// Add new variants here to extend the set of supported message formats.
#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum QueueEvent {
    DirectIngest(DirectIngestMessage),
    // Native GCS notifications are NOT parsed here: GCS carries the event kind and object
    // identity in the Pub/Sub message *attributes* (not the JSON body), so they are handled
    // out of band in `MessageFormat::GcsNative` via `parse_gcs_native_notification`.
}

/// How the source interprets messages on the Pub/Sub subscription. Selected by the source
/// `strategy`: on-demand ingestion pushes custom [`DirectIngestMessage`]s, whereas the LP
/// blob-ingest path drains the archive bucket's native GCS `OBJECT_FINALIZE` notifications
/// (the GCP analog of `aws_s3`'s SQS strategy and `azure_blob`'s Event Grid queue).
#[derive(Clone, Copy, Debug)]
pub(crate) enum MessageFormat {
    DirectIngest,
    GcsNative,
}

// GCS notification attribute keys (see the notification schema, `payloadFormat=JSON_API_V1`).
// GCS puts the event kind and object identity in the Pub/Sub message *attributes*, not the body.
const GCS_EVENT_TYPE_ATTR: &str = "eventType";
const GCS_BUCKET_ID_ATTR: &str = "bucketId";
const GCS_OBJECT_ID_ATTR: &str = "objectId";
/// The only event that means "a new object is fully written and readable". Other kinds
/// (`OBJECT_DELETE`, `OBJECT_ARCHIVE`, `OBJECT_METADATA_UPDATE`) carry no object to ingest.
const GCS_OBJECT_FINALIZE: &str = "OBJECT_FINALIZE";

/// What a native GCS notification resolves to once its attributes are interpreted.
enum GcsNativeOutcome {
    /// An `OBJECT_FINALIZE` for `gs://bucket/object` — download and ingest it.
    Ingest { bucket: String, object: String },
    /// A non-finalize event — acknowledge and move on, nothing to fetch.
    Skip,
}

/// Interpret a native GCS -> Pub/Sub notification from its message attributes.
///
/// `objectId` is the raw object name (not URL-encoded), so it is used as the GCS key as-is.
/// `Err` is a permanently-invalid message (missing required attributes): the caller acks it so
/// it does not poison the subscription.
fn parse_gcs_native_notification(
    attributes: &BTreeMap<String, String>,
) -> Result<GcsNativeOutcome, String> {
    let event_type = attributes
        .get(GCS_EVENT_TYPE_ATTR)
        .ok_or_else(|| format!("missing `{GCS_EVENT_TYPE_ATTR}` attribute"))?;
    if event_type != GCS_OBJECT_FINALIZE {
        return Ok(GcsNativeOutcome::Skip);
    }
    let non_empty = |k: &str| {
        attributes
            .get(k)
            .filter(|v| !v.is_empty())
            .cloned()
            .ok_or_else(|| format!("{GCS_OBJECT_FINALIZE} missing `{k}` attribute"))
    };
    Ok(GcsNativeOutcome::Ingest {
        bucket: non_empty(GCS_BUCKET_ID_ATTR)?,
        object: non_empty(GCS_OBJECT_ID_ATTR)?,
    })
}

// ============================================================================
// Error types
// ============================================================================

/// Errors that can occur while building the ingestor.
#[derive(Debug, Snafu)]
pub(super) enum IngestorNewError {
    #[snafu(display("Invalid max_number_of_messages {}: must be 1–1000", messages))]
    InvalidNumberOfMessages { messages: u32 },
}

/// Errors that can occur while processing a single Pub/Sub message.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Snafu)]
pub enum ProcessingError {
    /// The message body could not be parsed as a valid direct INGEST message.
    #[snafu(display(
        "Could not parse Pub/Sub message {} as a direct INGEST message: {}",
        message_id,
        source
    ))]
    InvalidPubSubMessage {
        source: serde_json::Error,
        message_id: String,
    },

    /// A native GCS notification was missing required attributes (`eventType`/`bucketId`/`objectId`).
    #[snafu(display("Invalid native GCS notification {}: {}", message_id, reason))]
    InvalidNativeNotification { message_id: String, reason: String },

    /// Failed to fetch the GCS object (HTTP-level error).
    #[snafu(display("HTTP error fetching gs://{}/{}: {}", bucket, key, source))]
    FetchObject {
        source: HttpError,
        bucket: String,
        key: String,
    },

    /// The GCS API returned a non-success status code.
    #[snafu(display("GCS returned HTTP {} for gs://{}/{}", status, bucket, key))]
    GetObject {
        status: u16,
        bucket: String,
        key: String,
    },

    /// The GCS object body could not be fully read.
    #[snafu(display("Failed to read gs://{}/{}: {}", bucket, key, source))]
    ReadObject {
        source: Box<dyn FramingError>,
        bucket: String,
        key: String,
    },

    /// Events could not be sent downstream.
    #[snafu(display("Failed to send events for gs://{}/{}: {}", bucket, key, source))]
    PipelineSend {
        source: SendError,
        bucket: String,
        key: String,
    },

    /// The GCS object was not found (HTTP 404).
    ///
    /// This typically means the object was deleted after the Pub/Sub notification was
    /// enqueued but before Vector processed it. The message should be acknowledged to
    /// prevent an infinite retry loop.
    #[snafu(display("GCS object not found gs://{}/{}: {}", bucket, key, reason))]
    ObjectNotFound {
        bucket: String,
        key: String,
        reason: String,
    },

    /// The downstream sink reported an error for this object.
    #[snafu(display("Sink reported an error for gs://{}/{}", bucket, key))]
    ErrorAcknowledgement { bucket: String, key: String },

    /// The direct ingest message has an empty `file_id`.
    #[snafu(display(
        "Direct ingest message for gs://{}/{} has an empty file_id, ignoring.",
        bucket,
        key
    ))]
    EmptyFileId { bucket: String, key: String },
}

impl ProcessingError {
    /// Returns `false` for errors where retrying would never succeed — the
    /// Pub/Sub message should be acknowledged immediately to prevent an
    /// infinite retry loop. Returns `true` for transient errors that may
    /// resolve on re-delivery.
    pub fn is_retriable(&self) -> bool {
        match self {
            // Permanently-invalid: message payload is broken, or the object is
            // gone for good. No amount of retrying will ever succeed.
            ProcessingError::InvalidPubSubMessage { .. }
            | ProcessingError::InvalidNativeNotification { .. }
            | ProcessingError::EmptyFileId { .. }
            | ProcessingError::ObjectNotFound { .. } => false,

            // Transient errors: network blips, temporary GCS unavailability (429,
            // 503), downstream pipeline pressure. Re-delivery may succeed.
            _ => true,
        }
    }
}

// ============================================================================
// Pub/Sub REST response types
// ============================================================================

/// Top-level response from the Pub/Sub `pull` REST endpoint.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PullResponse {
    #[serde(default)]
    received_messages: Vec<ReceivedMessage>,
}

/// A single message as returned by the Pub/Sub `pull` endpoint.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReceivedMessage {
    ack_id: String,
    message: PubSubMessage,
}

/// The inner message envelope from the Pub/Sub REST API.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PubSubMessage {
    /// Base64-encoded message payload.
    data: String,
    message_id: String,
    /// RFC 3339 publish timestamp.
    #[serde(default)]
    publish_time: Option<String>,
    /// Message attributes. Native GCS notifications carry the event kind + object identity here
    /// (`eventType`/`bucketId`/`objectId`); custom INGEST messages leave it empty.
    #[serde(default)]
    attributes: Option<BTreeMap<String, String>>,
}

/// Request body for the Pub/Sub `acknowledge` endpoint.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AcknowledgeRequest<'a> {
    ack_ids: &'a [String],
}

// ============================================================================
// Cloud Monitoring backlog sample (queue_message_count gauge)
// ============================================================================

/// Public Cloud Monitoring endpoint, used unless `monitoring_endpoint` overrides it.
const MONITORING_URL: &str = "https://monitoring.googleapis.com";

/// Pub/Sub's per-subscription backlog metric. The Pub/Sub API itself has no backlog count.
const NUM_UNDELIVERED_MESSAGES_METRIC: &str =
    "pubsub.googleapis.com/subscription/num_undelivered_messages";

/// How far back each backlog sample looks. Cloud Monitoring publishes Pub/Sub points with a delay
/// of a few minutes, so the window must reach past that delay to find the latest point; the
/// newest point in the window is the one reported.
const BACKLOG_LOOKBACK: Duration = Duration::from_secs(5 * 60);

/// Response from the Cloud Monitoring `timeSeries.list` REST endpoint, limited to the fields the
/// backlog sample reads.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListTimeSeriesResponse {
    #[serde(default)]
    time_series: Vec<TimeSeries>,
}

#[derive(Debug, Deserialize)]
struct TimeSeries {
    #[serde(default)]
    points: Vec<Point>,
}

#[derive(Debug, Deserialize)]
struct Point {
    interval: TimeInterval,
    value: TypedValue,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TimeInterval {
    end_time: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TypedValue {
    /// Proto3 JSON encodes int64 values as strings.
    #[serde(default)]
    int64_value: Option<String>,
}

/// Builds the `timeSeries.list` request for one subscription's `num_undelivered_messages` over the
/// lookback window ending at `now`.
fn backlog_request_url(
    endpoint: &str,
    project: &str,
    subscription: &str,
    now: DateTime<Utc>,
) -> Result<url::Url, url::ParseError> {
    let mut url = url::Url::parse(&format!("{endpoint}/v3/projects/{project}/timeSeries"))?;
    let filter = format!(
        r#"metric.type = "{NUM_UNDELIVERED_MESSAGES_METRIC}" AND resource.type = "pubsub_subscription" AND resource.labels.subscription_id = "{subscription}""#
    );
    let start = now
        - chrono::Duration::from_std(BACKLOG_LOOKBACK).expect("lookback fits in chrono::Duration");
    url.query_pairs_mut()
        .append_pair("filter", &filter)
        .append_pair(
            "interval.startTime",
            &start.to_rfc3339_opts(SecondsFormat::Secs, true),
        )
        .append_pair(
            "interval.endTime",
            &now.to_rfc3339_opts(SecondsFormat::Secs, true),
        );
    Ok(url)
}

/// The most recent backlog value in the response; `None` when the window holds no numeric point
/// (e.g. Cloud Monitoring has not published one yet).
fn latest_backlog(response: &ListTimeSeriesResponse) -> Option<u64> {
    response
        .time_series
        .iter()
        .flat_map(|series| series.points.iter())
        .filter_map(|point| {
            let value = point.value.int64_value.as_deref()?.parse::<u64>().ok()?;
            Some((point.interval.end_time, value))
        })
        .max_by_key(|(end_time, _)| *end_time)
        .map(|(_, value)| value)
}

// ============================================================================
// Ingestor — owns shared state and spawns per-concurrency IngestorProcesses.
// ============================================================================

struct State {
    client: HttpClient,
    auth: GcpAuthenticator,
    /// Base URL for the Pub/Sub REST API (defaults to [`PUBSUB_URL`]).
    endpoint: String,
    /// Full Pub/Sub resource name: `projects/{project}/subscriptions/{subscription}`.
    subscription_resource_name: String,
    /// Short subscription ID, as Cloud Monitoring labels it (`resource.labels.subscription_id`).
    subscription: String,
    project: String,
    /// Base URL for the Cloud Monitoring REST API (defaults to [`MONITORING_URL`]).
    monitoring_endpoint: String,
    /// Seconds between backlog samples for the `queue_message_count` gauge; `None` disables them.
    depth_poll_secs: Option<NonZeroU64>,
    poll_secs: u64,
    max_number_of_messages: u32,
    client_concurrency: usize,
    acknowledge_message: bool,
    acknowledge_failed_message: bool,
    callback_client: Option<IngestionCallbackClient>,
    message_format: MessageFormat,
    rate_limiter: ObjectStorageRateLimiter,
}

/// Resolves a Google API base endpoint: the configured override with any trailing
/// slash trimmed, or the public `default` ([`PUBSUB_URL`] / [`MONITORING_URL`]). Pub/Sub
/// and Cloud Monitoring request URLs are built with `format!`, so a trailing slash would
/// double the separator. The GCS side does not need this because it composes URLs with
/// `url::Url`, which normalizes.
fn resolve_endpoint(configured: Option<&str>, default: &str) -> String {
    configured
        .unwrap_or(default)
        .trim_end_matches('/')
        .to_string()
}

pub(super) struct Ingestor {
    state: Arc<State>,
    downloader: Arc<GcsDownloader>,
}

impl Ingestor {
    pub(super) async fn new(
        project: String,
        client: HttpClient,
        auth: GcpAuthenticator,
        config: Config,
        downloader: Arc<GcsDownloader>,
        callback_client: Option<IngestionCallbackClient>,
        message_format: MessageFormat,
        rate_limiter: ObjectStorageRateLimiter,
    ) -> Result<Self, IngestorNewError> {
        if config.max_number_of_messages < 1 || config.max_number_of_messages > 1000 {
            return Err(IngestorNewError::InvalidNumberOfMessages {
                messages: config.max_number_of_messages,
            });
        }

        let subscription_resource_name =
            format!("projects/{project}/subscriptions/{}", config.subscription);

        let endpoint = resolve_endpoint(config.endpoint.as_deref(), PUBSUB_URL);
        let monitoring_endpoint =
            resolve_endpoint(config.monitoring_endpoint.as_deref(), MONITORING_URL);

        let state = Arc::new(State {
            client,
            auth,
            endpoint,
            subscription_resource_name,
            subscription: config.subscription,
            project,
            monitoring_endpoint,
            depth_poll_secs: config.depth_poll_secs,
            poll_secs: config.poll_secs,
            max_number_of_messages: config.max_number_of_messages,
            client_concurrency: config
                .client_concurrency
                .map(|n| n.get())
                .unwrap_or_else(crate::num_threads),
            acknowledge_message: config.acknowledge_message,
            acknowledge_failed_message: config.acknowledge_failed_message,
            callback_client,
            message_format,
            rate_limiter,
        });

        Ok(Self { state, downloader })
    }

    pub(super) async fn run(
        self,
        cx: SourceContext,
        acknowledgements: SourceAcknowledgementsConfig,
        log_namespace: vector_lib::config::LogNamespace,
    ) -> Result<(), ()> {
        let acknowledgements = cx.do_acknowledgements(acknowledgements);
        let mut handles = Vec::new();

        for _ in 0..self.state.client_concurrency {
            let process = IngestorProcess::new(
                Arc::clone(&self.state),
                Arc::clone(&self.downloader),
                cx.out.clone(),
                cx.shutdown.clone(),
                log_namespace,
                acknowledgements,
            );
            let handle = tokio::spawn(process.run().in_current_span());
            handles.push(handle);
        }

        // When enabled, a single backlog sampler per subscription, independent of
        // client_concurrency. Spawned in the source's span so the gauge carries this source's
        // component_id.
        if let Some(period) = self.state.depth_poll_secs {
            let depth_handle = tokio::spawn(
                Self::run_depth_poller(Arc::clone(&self.state), cx.shutdown.clone(), period)
                    .in_current_span(),
            );
            handles.push(depth_handle);
        }

        for handle in handles.drain(..) {
            if let Err(e) = handle.await {
                if e.is_panic() {
                    panic::resume_unwind(e.into_panic());
                }
            }
        }

        Ok(())
    }

    /// Periodically samples the subscription's backlog and emits the `queue_message_count` gauge.
    /// Runs on its own timer, decoupled from pulling, so it adds no latency to draining. Each
    /// sample is a single Cloud Monitoring `timeSeries.list` call bounded by a timeout; on error,
    /// timeout, or no recent point it skips the sample (never reporting a false zero) and retries
    /// next tick.
    async fn run_depth_poller(state: Arc<State>, shutdown: ShutdownSignal, period: NonZeroU64) {
        // A backlog sample must never wedge the loop; keep this well under the tick interval.
        const LIST_TIME_SERIES_TIMEOUT: Duration = Duration::from_secs(10);

        let shutdown = shutdown.fuse();
        pin!(shutdown);

        let mut ticker = tokio::time::interval(Duration::from_secs(period.get()));
        // On a missed tick (e.g. a slow call), skip it rather than firing a catch-up burst.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            select! {
                _ = &mut shutdown => break,
                _ = ticker.tick() => {
                    let sample =
                        tokio::time::timeout(LIST_TIME_SERIES_TIMEOUT, fetch_backlog(&state)).await;
                    match sample {
                        Ok(Ok(Some(count))) => emit!(QueueMessageCount {
                            count,
                            cloud: "gcp",
                        }),
                        Ok(Ok(None)) => warn!(
                            message = "No recent subscription backlog point in Cloud Monitoring for depth metric.",
                            subscription = %state.subscription_resource_name,
                            internal_log_rate_limit = true,
                        ),
                        Ok(Err(error)) => warn!(
                            message = "Failed to fetch subscription backlog for depth metric.",
                            %error,
                            subscription = %state.subscription_resource_name,
                            internal_log_rate_limit = true,
                        ),
                        Err(_) => warn!(
                            message = "Timed out fetching subscription backlog for depth metric.",
                            subscription = %state.subscription_resource_name,
                            internal_log_rate_limit = true,
                        ),
                    }
                }
            }
        }
    }
}

/// Reads the subscription's latest `num_undelivered_messages` from Cloud Monitoring.
async fn fetch_backlog(state: &State) -> crate::Result<Option<u64>> {
    let url = backlog_request_url(
        &state.monitoring_endpoint,
        &state.project,
        &state.subscription,
        Utc::now(),
    )?;

    let mut request = Request::get(url.as_str())
        .body(Body::empty())
        .expect("timeSeries.list request must be valid");
    state.auth.apply(&mut request);

    let response = state.client.send(request).await?;

    let status = response.status();
    let body_bytes = http_body::Body::collect(response.into_body())
        .await
        .map_err(|source| HttpError::CallRequest { source })?
        .to_bytes();

    if !status.is_success() {
        return Err(format!(
            "Cloud Monitoring timeSeries.list returned HTTP {} — body: {}",
            status,
            String::from_utf8_lossy(&body_bytes)
                .chars()
                .take(512)
                .collect::<String>()
        )
        .into());
    }

    let response: ListTimeSeriesResponse = serde_json::from_slice(&body_bytes)?;
    Ok(latest_backlog(&response))
}

// ============================================================================
// IngestorProcess — one per concurrent polling task.
// ============================================================================

struct IngestorProcess {
    state: Arc<State>,
    downloader: Arc<GcsDownloader>,
    out: SourceSender,
    shutdown: ShutdownSignal,
    acknowledgements: bool,
    log_namespace: vector_lib::config::LogNamespace,
    bytes_received: Registered<BytesReceived>,
    events_received: Registered<EventsReceived>,
    // Decoded bytes processed in the current batch, drained after each pull to pace.
    byte_counter: ByteCounter,
}

impl IngestorProcess {
    fn new(
        state: Arc<State>,
        downloader: Arc<GcsDownloader>,
        out: SourceSender,
        shutdown: ShutdownSignal,
        log_namespace: vector_lib::config::LogNamespace,
        acknowledgements: bool,
    ) -> Self {
        Self {
            state,
            downloader,
            out,
            shutdown,
            acknowledgements,
            log_namespace,
            bytes_received: register!(BytesReceived::from(Protocol::HTTP)),
            events_received: register!(EventsReceived),
            byte_counter: ByteCounter::default(),
        }
    }

    async fn run(mut self) {
        let shutdown = self.shutdown.clone().fuse();
        pin!(shutdown);

        loop {
            select! {
                _ = &mut shutdown => break,
                _ = self.run_once() => {},
            }
        }
    }

    /// Single iteration: pull messages, process each, ack successes, sleep if empty.
    async fn run_once(&mut self) {
        let messages = match self.pull_messages().await {
            Ok(msgs) => {
                emit!(PubSubMessageReceiveSucceeded { count: msgs.len() });
                debug!(
                    message = "Received messages from Pub/Sub.",
                    count = %msgs.len(),
                    internal_log_rate_limit = true
                );
                msgs
            }
            Err(err) => {
                emit!(PubSubMessageReceiveError { error: &err });
                return;
            }
        };

        if messages.is_empty() {
            tokio::time::sleep(Duration::from_secs(self.state.poll_secs)).await;
            return;
        }

        let mut ack_ids: Vec<String> = Vec::new();

        for msg in messages {
            let message_id = msg.message.message_id.clone();
            let ack_id = msg.ack_id.clone();

            let publish_time = msg
                .message
                .publish_time
                .as_deref()
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&Utc));

            match self
                .handle_message(
                    &msg.message.data,
                    msg.message.attributes.as_ref(),
                    &message_id,
                    publish_time,
                )
                .await
            {
                Ok(()) => {
                    emit!(PubSubMessageProcessingSucceeded {
                        message_id: &message_id,
                    });
                    if self.state.acknowledge_message {
                        ack_ids.push(ack_id);
                    }
                }
                Err(ref err) => {
                    emit!(PubSubMessageProcessingError {
                        message_id: &message_id,
                        error: err,
                    });
                    // Non-retriable errors must always be acknowledged to prevent
                    // an infinite retry loop. Retriable errors respect the
                    // acknowledge_failed_message config flag.
                    if !err.is_retriable() || self.state.acknowledge_failed_message {
                        ack_ids.push(ack_id);
                    }
                }
            }
        }

        if !ack_ids.is_empty() {
            if let Err(err) = self.acknowledge_messages(&ack_ids).await {
                error!(
                    message = "Failed to acknowledge Pub/Sub messages.",
                    error = %err,
                    count = %ack_ids.len(),
                );
            }
        }

        // Pace between pulls: the batch is fully processed and its messages are
        // acked, so nothing is in flight while we wait to honor the byte rate.
        self.state
            .rate_limiter
            .pace(self.byte_counter.take())
            .await;
    }

    /// Decodes and dispatches a single Pub/Sub message according to the configured format.
    async fn handle_message(
        &mut self,
        data_b64: &str,
        attributes: Option<&BTreeMap<String, String>>,
        message_id: &str,
        publish_time: Option<DateTime<Utc>>,
    ) -> Result<(), ProcessingError> {
        match self.state.message_format {
            MessageFormat::DirectIngest => {
                self.handle_direct_ingest_message(data_b64, message_id, publish_time)
                    .await
            }
            MessageFormat::GcsNative => {
                self.handle_gcs_native(attributes, message_id, publish_time)
                    .await
            }
        }
    }

    /// Decodes and dispatches a custom INGEST message (on-demand ingestion path).
    async fn handle_direct_ingest_message(
        &mut self,
        data_b64: &str,
        message_id: &str,
        publish_time: Option<DateTime<Utc>>,
    ) -> Result<(), ProcessingError> {
        // Pub/Sub REST API always delivers message data as base64.
        // A decode failure means a corrupt/invalid payload — surface it as an error.
        let data = BASE64_STANDARD.decode(data_b64).map_err(|e| {
            error!(
                message = "Failed to base64-decode Pub/Sub message data.",
                error = ?e,
                message_id = %message_id,
            );
            ProcessingError::InvalidPubSubMessage {
                source: serde_json::Error::custom(format!("base64 decode error: {e}")),
                message_id: message_id.to_string(),
            }
        })?;

        let queue_event: QueueEvent = serde_json::from_slice(&data).map_err(|e| {
            error!(
                message = "Failed to parse Pub/Sub message as a direct INGEST message.",
                error = ?e,
                message_id = %message_id,
            );
            ProcessingError::InvalidPubSubMessage {
                source: e,
                message_id: message_id.to_string(),
            }
        })?;

        match queue_event {
            QueueEvent::DirectIngest(msg) => {
                self.handle_direct_ingest(msg, message_id, publish_time)
                    .await
            }
        }
    }

    /// Processes a native GCS `OBJECT_FINALIZE` notification (LP blob-ingest path). The object
    /// identity lives in the message attributes, not the body; non-finalize events are acked and
    /// skipped, and there is no ingestion callback (no upstream requester on this path).
    async fn handle_gcs_native(
        &mut self,
        attributes: Option<&BTreeMap<String, String>>,
        message_id: &str,
        publish_time: Option<DateTime<Utc>>,
    ) -> Result<(), ProcessingError> {
        let attributes = attributes.ok_or_else(|| ProcessingError::InvalidNativeNotification {
            message_id: message_id.to_string(),
            reason: "message has no attributes".to_string(),
        })?;

        let (bucket, object) = match parse_gcs_native_notification(attributes).map_err(|reason| {
            ProcessingError::InvalidNativeNotification {
                message_id: message_id.to_string(),
                reason,
            }
        })? {
            GcsNativeOutcome::Skip => {
                debug!(
                    message = "Skipping non-finalize GCS notification.",
                    message_id = %message_id,
                    internal_log_rate_limit = true,
                );
                return Ok(());
            }
            GcsNativeOutcome::Ingest { bucket, object } => (bucket, object),
        };

        if let Some(ts) = publish_time {
            let lag_secs = Utc::now().signed_duration_since(ts).num_milliseconds() as f64 / 1000.0;
            emit!(QueueNotificationProcessLag {
                lag_seconds: lag_secs,
                cloud: "gcp",
                bucket: &bucket,
            });
        }

        debug!(
            message = "Processing native GCS notification.",
            bucket = %bucket,
            key = %object,
            message_id = %message_id,
        );

        self.downloader
            .process_object(
                &bucket,
                &object,
                None,
                &mut self.out,
                self.log_namespace,
                self.acknowledgements,
                self.state.acknowledge_failed_message,
                &self.bytes_received,
                &self.events_received,
            )
            .await
    }

    /// Emits lag metrics and delegates to the GCS downloader.
    async fn handle_direct_ingest(
        &mut self,
        msg: DirectIngestMessage,
        message_id: &str,
        publish_time: Option<DateTime<Utc>>,
    ) -> Result<(), ProcessingError> {
        if msg.kind != DIRECT_INGEST_KIND {
            warn!(
                message = "Unknown direct ingest kind — ignoring.",
                kind = %msg.kind,
                expected = DIRECT_INGEST_KIND,
                message_id = %message_id,
            );
            return Ok(());
        }

        if msg.file_id.is_empty() {
            return Err(ProcessingError::EmptyFileId {
                bucket: msg.bucket,
                key: msg.key,
            });
        }

        if let Some(ts) = publish_time {
            let lag_secs = Utc::now().signed_duration_since(ts).num_milliseconds() as f64 / 1000.0;
            emit!(QueueNotificationProcessLag {
                lag_seconds: lag_secs,
                cloud: "gcp",
                bucket: &msg.bucket,
            });
        }

        debug!(
            message = "Processing direct ingest message.",
            bucket = %msg.bucket,
            key = %msg.key,
            file_id = %msg.file_id,
            message_id = %message_id,
        );

        let processing_start = std::time::Instant::now();
        let obj_start_bytes = self.byte_counter.get();
        let result = self
            .downloader
            .process_object(
                &msg.bucket,
                &msg.key,
                msg.log_type.as_deref(),
                &mut self.out,
                self.log_namespace,
                self.acknowledgements,
                self.state.acknowledge_failed_message,
                &self.bytes_received,
                &self.events_received,
                &self.byte_counter,
            )
            .await;

        // Observability-only: warn if this object's decoded size is large enough to
        // risk the Pub/Sub ack deadline even at full speed. Never drops data.
        self.state.rate_limiter.note_object(
            self.byte_counter.get().saturating_sub(obj_start_bytes),
            &msg.bucket,
            &msg.key,
        );

        // Fire ingestion callback if configured (non-blocking).
        if let Some(ref cb_client) = self.state.callback_client {
            let message_fields = std::collections::HashMap::from([
                ("file_id".to_string(), msg.file_id.clone()),
                ("bucket".to_string(), msg.bucket.clone()),
                ("key".to_string(), msg.key.clone()),
                ("project".to_string(), self.state.project.clone()),
            ]);
            let _ = cb_client.spawn_notify(&result, processing_start.elapsed(), message_fields);
        }

        result
    }

    // ========================================================================
    // Pub/Sub REST API calls
    // ========================================================================

    /// Pulls up to `max_number_of_messages` from the subscription.
    async fn pull_messages(&self) -> crate::Result<Vec<ReceivedMessage>> {
        let url = format!(
            "{}/v1/{}:pull",
            self.state.endpoint, self.state.subscription_resource_name
        );

        let body = serde_json::json!({
            "maxMessages": self.state.max_number_of_messages
        })
        .to_string();

        let mut request = Request::post(&url)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .expect("pull request must be valid");
        self.state.auth.apply(&mut request);

        let response = self.state.client.send(request).await?;

        let status = response.status();
        let body_bytes = http_body::Body::collect(response.into_body())
            .await
            .map_err(|e| HttpError::CallRequest {
                source: hyper::Error::from(e),
            })?
            .to_bytes();

        // Non-2xx: surface as an error with the response body for debugging.
        if !status.is_success() {
            return Err(format!(
                "Pub/Sub pull returned HTTP {} — body: {}",
                status,
                String::from_utf8_lossy(&body_bytes)
                    .chars()
                    .take(512)
                    .collect::<String>()
            )
            .into());
        }

        // Log a warning on malformed responses
        let pull_response: PullResponse = match serde_json::from_slice(&body_bytes) {
            Ok(r) => r,
            Err(error) => {
                warn!(
                    message = "Failed to parse Pub/Sub pull response as JSON.",
                    %error,
                    subscription = %self.state.subscription_resource_name,
                    body = %String::from_utf8_lossy(&body_bytes),
                );
                PullResponse {
                    received_messages: vec![],
                }
            }
        };

        Ok(pull_response.received_messages)
    }

    /// Acknowledges a batch of messages by their ack IDs.
    async fn acknowledge_messages(&self, ack_ids: &[String]) -> Result<(), HttpError> {
        let url = format!(
            "{}/v1/{}:acknowledge",
            self.state.endpoint, self.state.subscription_resource_name
        );

        let body = serde_json::to_string(&AcknowledgeRequest { ack_ids })
            .expect("AcknowledgeRequest must serialize");

        let mut request = Request::post(&url)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .expect("acknowledge request must be valid");
        self.state.auth.apply(&mut request);

        let response = self.state.client.send(request).await?;

        if !response.status().is_success() {
            // Log a warning but do not return an error. A failed ack means the
            // messages will be redelivered after the subscription's ack deadline,
            // which is safer than surfacing the error and potentially skipping
            // further processing. Callers already log failures at a higher level.
            warn!(
                message = "Pub/Sub acknowledge request returned a non-success status.",
                status = %response.status(),
                subscription = %self.state.subscription_resource_name,
            );
        }

        Ok(())
    }
}

const fn default_poll_secs() -> u64 {
    15
}

const fn default_max_messages() -> u32 {
    10
}

const fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------------
    // Pub/Sub Config: business-logic validation
    // -------------------------------------------------------------------------

    /// Verify the defaults match the documented values — these are part of the
    /// public interface and must not change without a migration.
    #[test]
    fn config_defaults_are_correct() {
        let config: Config = toml::from_str(r#"subscription = "s""#).unwrap();
        assert_eq!(config.poll_secs, 15, "default poll_secs must be 15s");
        assert_eq!(
            config.max_number_of_messages, 10,
            "default max_number_of_messages must be 10"
        );
        assert!(
            config.acknowledge_message,
            "messages must be acknowledged by default"
        );
        assert!(
            config.acknowledge_failed_message,
            "failed messages must be acknowledged by default to prevent infinite retries"
        );
    }

    /// Unknown fields must be rejected so that misconfigured deployments fail
    /// loudly at startup rather than silently ignoring unrecognised options.
    #[test]
    fn config_unknown_fields_rejected() {
        let result: Result<Config, _> = toml::from_str(
            r#"subscription = "s"
               unknown_field = "oops""#,
        );
        assert!(
            result.is_err(),
            "unknown fields must be rejected at parse time"
        );
    }

    /// The Pub/Sub endpoint is unset by default (the public endpoint is applied
    /// at ingestor construction) and can be overridden via config.
    #[test]
    fn config_endpoint_parses() {
        let default: Config = toml::from_str(r#"subscription = "s""#).unwrap();
        assert_eq!(default.endpoint, None, "endpoint must be unset by default");

        let overridden: Config =
            toml::from_str("subscription = \"s\"\nendpoint = \"https://pubsub.example.com\"")
                .unwrap();
        assert_eq!(
            overridden.endpoint.as_deref(),
            Some("https://pubsub.example.com")
        );
    }

    /// The resolved endpoint is the public default when unset, and an override is
    /// used verbatim with any trailing slash trimmed so request URLs never double
    /// the separator.
    #[test]
    fn resolve_endpoint_defaults_and_trims() {
        let cases: [(Option<&str>, &str, &str); 6] = [
            (None, PUBSUB_URL, PUBSUB_URL),
            (
                Some("https://pubsub.example.com"),
                PUBSUB_URL,
                "https://pubsub.example.com",
            ),
            (
                Some("https://pubsub.example.com/"),
                PUBSUB_URL,
                "https://pubsub.example.com",
            ),
            (None, MONITORING_URL, MONITORING_URL),
            (
                Some("https://monitoring.example.com"),
                MONITORING_URL,
                "https://monitoring.example.com",
            ),
            (
                Some("https://monitoring.example.com/"),
                MONITORING_URL,
                "https://monitoring.example.com",
            ),
        ];
        for (configured, default, expected) in cases {
            assert_eq!(
                resolve_endpoint(configured, default),
                expected,
                "configured {configured:?}, default {default}"
            );
        }
    }

    // -------------------------------------------------------------------------
    // Backlog sample (queue_message_count gauge)
    // -------------------------------------------------------------------------

    /// Backlog sampling is opt-in: unset by default, so sources that don't ask for it (and lack
    /// the Cloud Monitoring grant) never call the Monitoring API.
    #[test]
    fn config_depth_poll_defaults_and_parses() {
        let default: Config = toml::from_str(r#"subscription = "s""#).unwrap();
        assert_eq!(
            default.depth_poll_secs, None,
            "sampling must be off by default"
        );
        assert_eq!(default.monitoring_endpoint, None);

        let configured: Config = toml::from_str(
            "subscription = \"s\"\ndepth_poll_secs = 60\nmonitoring_endpoint = \"https://monitoring.example.com\"",
        )
        .unwrap();
        assert_eq!(configured.depth_poll_secs, NonZeroU64::new(60));
        assert_eq!(
            configured.monitoring_endpoint.as_deref(),
            Some("https://monitoring.example.com")
        );
    }

    /// Zero would panic the sampler's interval timer, so it must be rejected at parse time.
    #[test]
    fn config_depth_poll_rejects_zero() {
        let result: Result<Config, _> = toml::from_str("subscription = \"s\"\ndepth_poll_secs = 0");
        assert!(result.is_err(), "depth_poll_secs = 0 must be rejected");
    }

    #[test]
    fn backlog_request_url_targets_subscription_metric_over_lookback() {
        let now = DateTime::parse_from_rfc3339("2026-10-05T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let url = backlog_request_url(MONITORING_URL, "my-project", "my-sub", now).unwrap();

        assert_eq!(url.scheme(), "https");
        assert_eq!(url.host_str(), Some("monitoring.googleapis.com"));
        assert_eq!(url.path(), "/v3/projects/my-project/timeSeries");

        let query: BTreeMap<String, String> = url.query_pairs().into_owned().collect();
        assert_eq!(
            query.get("filter").map(String::as_str),
            Some(
                r#"metric.type = "pubsub.googleapis.com/subscription/num_undelivered_messages" AND resource.type = "pubsub_subscription" AND resource.labels.subscription_id = "my-sub""#
            )
        );
        assert_eq!(
            query.get("interval.startTime").map(String::as_str),
            Some("2026-10-05T11:55:00Z")
        );
        assert_eq!(
            query.get("interval.endTime").map(String::as_str),
            Some("2026-10-05T12:00:00Z")
        );
    }

    /// The newest point wins regardless of response order; points without a numeric int64Value
    /// are skipped rather than read as zero.
    #[test]
    fn latest_backlog_picks_newest_numeric_point() {
        let json = r#"{
            "timeSeries": [{
                "metric": {"type": "pubsub.googleapis.com/subscription/num_undelivered_messages"},
                "resource": {"type": "pubsub_subscription", "labels": {"subscription_id": "s"}},
                "points": [
                    {"interval": {"startTime": "2026-10-05T11:58:00Z", "endTime": "2026-10-05T11:58:00Z"}, "value": {"int64Value": "7"}},
                    {"interval": {"startTime": "2026-10-05T11:59:00.5Z", "endTime": "2026-10-05T11:59:00.5Z"}, "value": {"int64Value": "42"}},
                    {"interval": {"startTime": "2026-10-05T11:57:00Z", "endTime": "2026-10-05T11:57:00Z"}, "value": {"int64Value": "3"}}
                ]
            }]
        }"#;
        let response: ListTimeSeriesResponse = serde_json::from_str(json).unwrap();
        assert_eq!(latest_backlog(&response), Some(42));

        let json = r#"{
            "timeSeries": [{
                "points": [
                    {"interval": {"endTime": "2026-10-05T11:59:00Z"}, "value": {"doubleValue": 1.0}},
                    {"interval": {"endTime": "2026-10-05T11:58:00Z"}, "value": {"int64Value": "abc"}},
                    {"interval": {"endTime": "2026-10-05T11:57:00Z"}, "value": {"int64Value": "5"}}
                ]
            }]
        }"#;
        let response: ListTimeSeriesResponse = serde_json::from_str(json).unwrap();
        assert_eq!(latest_backlog(&response), Some(5));
    }

    /// An empty response (no published point in the window) yields no sample, not zero.
    #[test]
    fn latest_backlog_empty_response_is_none() {
        let response: ListTimeSeriesResponse = serde_json::from_str("{}").unwrap();
        assert_eq!(latest_backlog(&response), None);
    }

    // max_number_of_messages bounds are tested via Ingestor::new in mod.rs::tests.

    // -------------------------------------------------------------------------
    // DirectIngestMessage: schema constraints
    // -------------------------------------------------------------------------

    /// file_id is required — messages without it must be rejected so that the
    /// ingestion callback can always reference the upstream file identifier.
    #[test]
    fn message_without_file_id_is_rejected() {
        let json = r#"{"kind": "INGEST", "bucket": "b", "key": "k"}"#;
        let msg: Result<DirectIngestMessage, _> = serde_json::from_str(json);
        assert!(
            msg.is_err(),
            "missing file_id must fail — field is required"
        );
    }

    /// Extra fields must be rejected so that future message types don't
    /// silently match as DirectIngest when they shouldn't.
    #[test]
    fn message_with_unknown_field_is_rejected() {
        let json = r#"{"kind": "INGEST", "bucket": "b", "key": "k", "file_id": "f", "extra": "x"}"#;
        let msg: Result<DirectIngestMessage, _> = serde_json::from_str(json);
        assert!(
            msg.is_err(),
            "deny_unknown_fields must prevent unrecognised fields from silently passing"
        );
    }

    /// log_type is optional: messages that omit it must still parse (backward
    /// compatibility with producers that predate the field).
    #[test]
    fn message_without_log_type_parses_to_none() {
        let json = r#"{"kind": "INGEST", "bucket": "b", "key": "k", "file_id": "f"}"#;
        let msg: DirectIngestMessage =
            serde_json::from_str(json).expect("missing log_type must parse — field is optional");
        assert_eq!(msg.log_type, None, "absent log_type must deserialize to None");
    }

    /// log_type is carried through when the upstream Log Access service sets it,
    /// so the ingestion path can segregate CP / DP-spark / DP-service logs.
    #[test]
    fn message_with_log_type_is_parsed() {
        let json =
            r#"{"kind": "INGEST", "bucket": "b", "key": "k", "file_id": "f", "log_type": "cp_logs"}"#;
        let msg: DirectIngestMessage =
            serde_json::from_str(json).expect("log_type must parse when present");
        assert_eq!(msg.log_type.as_deref(), Some("cp_logs"));
    }

    /// QueueEvent must reject messages that match no known variant so that a
    /// misconfigured producer doesn't produce endlessly-retried dead letters.
    #[test]
    fn queue_event_rejects_unrecognised_message() {
        let json = r#"{"something": "completely_different"}"#;
        let event: Result<QueueEvent, _> = serde_json::from_str(json);
        assert!(
            event.is_err(),
            "unrecognised structure must fail to prevent silent data loss"
        );
    }

    /// An empty file_id must parse successfully (it's a valid String) but be
    /// rejected at processing time with EmptyFileId, matching aws_s3/azure_blob.
    #[test]
    fn message_with_empty_file_id_parses_but_is_rejected_by_is_retriable() {
        let json = r#"{"kind": "INGEST", "bucket": "my-logs-bucket", "key": "app/2026/03/21/events.log", "file_id": ""}"#;
        let msg: DirectIngestMessage = serde_json::from_str(json)
            .expect("empty file_id must parse — validation is at processing time, not parse time");
        assert!(
            msg.file_id.is_empty(),
            "empty file_id must be preserved after parsing"
        );
        // The EmptyFileId error that would be produced is non-retriable.
        let err = ProcessingError::EmptyFileId {
            bucket: msg.bucket,
            key: msg.key,
        };
        assert!(
            !err.is_retriable(),
            "EmptyFileId must not be retriable — empty file_id can never be fixed by retrying"
        );
    }

    #[test]
    fn is_retriable_classification() {
        let cases: &[(ProcessingError, bool, &str)] = &[
            // ── Permanent: never retry ──────────────────────────────────────
            (
                ProcessingError::InvalidPubSubMessage {
                    source: serde_json::from_str::<()>(r#"{"kind":"INGEST"}"#).unwrap_err(),
                    message_id: "projects/my-project/subscriptions/my-sub:1234".into(),
                },
                false,
                "malformed Pub/Sub message can never be fixed by re-delivery",
            ),
            (
                ProcessingError::EmptyFileId {
                    bucket: "my-logs-bucket".into(),
                    key: "app/2026/03/21/events.log".into(),
                },
                false,
                "empty file_id is a producer bug — re-delivering the same message won't fix it",
            ),
            (
                ProcessingError::ObjectNotFound {
                    bucket: "my-logs-bucket".into(),
                    key: "app/2026/03/21/events.log".into(),
                    reason:
                        "GCS returned HTTP 404 — object deleted after notification was enqueued"
                            .into(),
                },
                false,
                "deleted GCS object will never reappear — retrying would loop forever",
            ),
            // ── Transient: retry (unless acknowledge_failed_message=true) ───
            (
                ProcessingError::GetObject {
                    status: 503,
                    bucket: "my-logs-bucket".into(),
                    key: "app/2026/03/21/events.log".into(),
                },
                true,
                "GCS 503 is a transient server error that may resolve on re-delivery",
            ),
            (
                ProcessingError::GetObject {
                    status: 429,
                    bucket: "my-logs-bucket".into(),
                    key: "app/2026/03/21/events.log".into(),
                },
                true,
                "GCS 429 rate-limit is transient — backing off and retrying is correct",
            ),
            (
                ProcessingError::ReadObject {
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "connection reset while reading object body",
                    )),
                    bucket: "my-logs-bucket".into(),
                    key: "app/2026/03/21/events.log".into(),
                },
                true,
                "mid-stream connection drop is transient — object may be readable on retry",
            ),
            (
                ProcessingError::PipelineSend {
                    source: vector_lib::source_sender::SendError::Closed,
                    bucket: "my-logs-bucket".into(),
                    key: "app/2026/03/21/events.log".into(),
                },
                true,
                "downstream pipeline closed transiently — retry may succeed after recovery",
            ),
            (
                ProcessingError::ErrorAcknowledgement {
                    bucket: "my-logs-bucket".into(),
                    key: "app/2026/03/21/events.log".into(),
                },
                true,
                "sink error acknowledgement is transient — sink may recover on re-delivery",
            ),
        ];

        for (err, expected, reason) in cases {
            assert_eq!(err.is_retriable(), *expected, "{err:?}: {reason}");
        }
    }

    // -------------------------------------------------------------------------
    // Native GCS notification parsing (MessageFormat::GcsNative)
    // -------------------------------------------------------------------------

    fn attrs(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// An OBJECT_FINALIZE notification yields the bucket + object to ingest, read from attributes
    /// (not the JSON body).
    #[test]
    fn native_finalize_parses_to_ingest() {
        let a = attrs(&[
            ("eventType", "OBJECT_FINALIZE"),
            ("bucketId", "my-logs-bucket"),
            ("objectId", "archived-log/2026/03/21/events.log"),
            ("payloadFormat", "JSON_API_V1"),
        ]);
        match parse_gcs_native_notification(&a).expect("finalize must parse") {
            GcsNativeOutcome::Ingest { bucket, object } => {
                assert_eq!(bucket, "my-logs-bucket");
                assert_eq!(object, "archived-log/2026/03/21/events.log");
            }
            GcsNativeOutcome::Skip => panic!("OBJECT_FINALIZE must ingest, not skip"),
        }
    }

    /// Non-finalize events (delete/archive/metadata) carry no object to fetch and must be skipped
    /// (acked), not errored.
    #[test]
    fn native_non_finalize_is_skipped() {
        for event in ["OBJECT_DELETE", "OBJECT_ARCHIVE", "OBJECT_METADATA_UPDATE"] {
            let a = attrs(&[
                ("eventType", event),
                ("bucketId", "my-logs-bucket"),
                ("objectId", "archived-log/x.log"),
            ]);
            assert!(
                matches!(
                    parse_gcs_native_notification(&a),
                    Ok(GcsNativeOutcome::Skip)
                ),
                "{event} must be skipped"
            );
        }
    }

    /// A message with no `eventType` is malformed — a permanent error so the caller acks it rather
    /// than poisoning the subscription.
    #[test]
    fn native_missing_event_type_errors() {
        let a = attrs(&[("bucketId", "b"), ("objectId", "k")]);
        assert!(parse_gcs_native_notification(&a).is_err());
    }

    /// OBJECT_FINALIZE without a bucket or object (or with an empty one) is unusable.
    #[test]
    fn native_finalize_missing_or_empty_ids_error() {
        let cases = [
            attrs(&[("eventType", "OBJECT_FINALIZE"), ("objectId", "k")]),
            attrs(&[("eventType", "OBJECT_FINALIZE"), ("bucketId", "b")]),
            attrs(&[
                ("eventType", "OBJECT_FINALIZE"),
                ("bucketId", ""),
                ("objectId", "k"),
            ]),
            attrs(&[
                ("eventType", "OBJECT_FINALIZE"),
                ("bucketId", "b"),
                ("objectId", ""),
            ]),
        ];
        for a in &cases {
            assert!(
                parse_gcs_native_notification(a).is_err(),
                "missing/empty bucket or object must error: {a:?}"
            );
        }
    }

    /// A malformed native notification must be non-retriable so it is acked, not redelivered forever.
    #[test]
    fn invalid_native_notification_is_non_retriable() {
        let err = ProcessingError::InvalidNativeNotification {
            message_id: "m-1".into(),
            reason: "missing `eventType` attribute".into(),
        };
        assert!(!err.is_retriable());
    }
}
