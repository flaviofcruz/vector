use std::{num::NonZeroUsize, panic, sync::Arc, time::Duration};

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use chrono::{DateTime, Utc};
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
        PubSubMessageReceiveSucceeded, QueueNotificationProcessLag,
    },
    shutdown::ShutdownSignal,
    sources::ingestion_callback::IngestionCallbackClient,
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
    // Future variants (e.g. GcsNativeNotification) can be added here.
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
}

/// Request body for the Pub/Sub `acknowledge` endpoint.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AcknowledgeRequest<'a> {
    ack_ids: &'a [String],
}

// ============================================================================
// Ingestor — owns shared state and spawns per-concurrency IngestorProcesses.
// ============================================================================

struct State {
    client: HttpClient,
    auth: GcpAuthenticator,
    /// Full Pub/Sub resource name: `projects/{project}/subscriptions/{subscription}`.
    subscription_resource_name: String,
    project: String,
    poll_secs: u64,
    max_number_of_messages: u32,
    client_concurrency: usize,
    acknowledge_message: bool,
    acknowledge_failed_message: bool,
    callback_client: Option<IngestionCallbackClient>,
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
    ) -> Result<Self, IngestorNewError> {
        if config.max_number_of_messages < 1 || config.max_number_of_messages > 1000 {
            return Err(IngestorNewError::InvalidNumberOfMessages {
                messages: config.max_number_of_messages,
            });
        }

        let subscription_resource_name =
            format!("projects/{project}/subscriptions/{}", config.subscription);

        let state = Arc::new(State {
            client,
            auth,
            subscription_resource_name,
            project,
            poll_secs: config.poll_secs,
            max_number_of_messages: config.max_number_of_messages,
            client_concurrency: config
                .client_concurrency
                .map(|n| n.get())
                .unwrap_or_else(crate::num_threads),
            acknowledge_message: config.acknowledge_message,
            acknowledge_failed_message: config.acknowledge_failed_message,
            callback_client,
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

        for handle in handles.drain(..) {
            if let Err(e) = handle.await {
                if e.is_panic() {
                    panic::resume_unwind(e.into_panic());
                }
            }
        }

        Ok(())
    }
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
                .handle_message(&msg.message.data, &message_id, publish_time)
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
    }

    /// Decodes and dispatches a single Pub/Sub message.
    async fn handle_message(
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
            )
            .await;

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
            "{PUBSUB_URL}/v1/{}:pull",
            self.state.subscription_resource_name
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
            "{PUBSUB_URL}/v1/{}:acknowledge",
            self.state.subscription_resource_name
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
}
