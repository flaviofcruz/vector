// Standard library imports for core functionality
use std::collections::HashMap;
use std::time::Duration;
use std::{future::ready, num::NonZeroUsize, panic, sync::Arc};

// Azure SDK imports for blob and queue operations
// Note: Using azure_core_for_storage (v0.21) to match azure_storage_* crate versions
use azure_core_for_storage::error::ErrorKind;
use azure_storage_blobs::prelude::*;
use azure_storage_queues::operations::Message;
use azure_storage_queues::prelude::*;

// Utility and serialization imports
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use futures::{FutureExt, Stream, StreamExt};
use serde::de::Error as SerdeDeError;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use smallvec::SmallVec;
use snafu::{ResultExt, Snafu};
use tokio::{pin, select};
use tokio_util::codec::FramedRead;
use tracing::Instrument;

// Vector-specific imports
use vector_lib::codecs::decoding::FramingError;
use vector_lib::configurable::configurable_component;
use vector_lib::internal_event::{
    ByteSize, BytesReceived, CountByteSize, InternalEventHandle as _, Protocol, Registered,
};
use vector_lib::source_sender::SendError;

use crate::codecs::Decoder;
use crate::event::{Event, LogEvent};
use crate::internal_events::{
    EventsReceived, QueueNotificationProcessLag, StreamClosedError,
    emit_object_storage_ack_metrics, emit_object_storage_non_ack_metrics,
};
use crate::{
    SourceSender,
    config::{SourceAcknowledgementsConfig, SourceContext},
    event::{BatchNotifier, BatchStatus, EstimatedJsonEncodedSizeOf},
    line_agg::{self, LineAgg},
    shutdown::ShutdownSignal,
    sources::azure_blob::AzureBlobConfig,
    sources::ingestion_callback::IngestionCallbackClient,
    tls::TlsConfig,
};
use vector_lib::config::{LegacyKey, LogNamespace, log_schema};
use vector_lib::event::MaybeAsLogMut;
use vector_lib::lookup::{PathPrefix, metadata_path, path};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;

const CLOUD_PROVIDER: &str = "azure";

/// Azure Queue Storage configuration options.
/// This struct defines all configurable parameters for the Azure Queue source,
/// including queue settings, polling behavior, and message handling.
#[serde_as]
#[configurable_component]
#[derive(Clone, Debug, Derivative)]
#[derivative(Default)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    /// The name of the Azure Queue to poll for Event Grid notifications.
    /// This queue should receive notifications when blobs are created in the storage account.
    #[configurable(metadata(docs::examples = "my-storage-queue"))]
    pub(super) queue_name: String,

    /// How long to wait while polling the queue for new messages, in seconds.
    /// This is the idle time between polling attempts when no messages are available.
    /// Note: Messages are always consumed immediately when available, regardless of this value.
    #[serde(default = "default_poll_secs")]
    #[derivative(Default(value = "default_poll_secs()"))]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    pub(super) poll_secs: u64,

    /// The visibility timeout to use for messages, in seconds.
    /// When a message is received, it becomes invisible to other consumers for this duration.
    /// If processing takes longer than this timeout, the message becomes visible again
    /// and may be processed by another consumer. This helps prevent message loss if
    /// processing fails or takes too long.
    #[serde(default = "default_visibility_timeout_secs")]
    #[derivative(Default(value = "default_visibility_timeout_secs()"))]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    #[configurable(metadata(docs::human_name = "Visibility Timeout"))]
    pub(super) visibility_timeout_secs: u64,

    /// Whether to delete the message once it is processed successfully.
    /// Set to false for debugging or initial setup to prevent message loss.
    #[serde(default = "default_true")]
    #[derivative(Default(value = "default_true()"))]
    pub(super) delete_message: bool,

    /// Whether to delete messages that fail processing and are not retryable.
    /// If true, failed messages are removed from the queue to prevent blocking
    /// the processing of other messages.
    #[serde(default = "default_true")]
    #[derivative(Default(value = "default_true()"))]
    pub(super) delete_failed_message: bool,

    /// Number of concurrent tasks to create for polling the queue.
    /// Defaults to the number of available CPUs.
    /// Increasing this value can improve throughput when:
    /// 1. There's a high rate of messages
    /// 2. The blobs being fetched are small
    /// 3. System resources are underutilized
    #[configurable(metadata(docs::type_unit = "tasks"))]
    #[configurable(metadata(docs::examples = 5))]
    pub(super) client_concurrency: Option<NonZeroUsize>,

    /// Maximum number of messages to poll from the queue in a batch.
    /// Defaults to 10. Valid range is 1-32.
    /// Use smaller values when processing large files to prevent
    /// visibility timeout issues with other messages in the batch.
    #[serde(default = "default_max_number_of_messages")]
    #[derivative(Default(value = "default_max_number_of_messages()"))]
    #[configurable(metadata(docs::human_name = "Max Messages"))]
    #[configurable(metadata(docs::examples = 1))]
    pub(super) max_number_of_messages: u64,

    /// TLS configuration options for secure communication with Azure services.
    #[configurable(derived)]
    #[serde(default)]
    #[derivative(Default)]
    pub(super) tls_options: Option<TlsConfig>,

    /// Whether to process custom direct ingest messages from the queue.
    ///
    /// When enabled, the source will also handle messages with `{"kind": "INGEST", "container": "...", "blob": "..."}` format
    /// in addition to standard Event Grid notifications. This allows you to manually enqueue
    /// blobs for ingestion without relying on Event Grid notifications.
    #[serde(default)]
    #[derivative(Default)]
    pub(super) process_custom_message: bool,
}

// Default configuration values
const fn default_poll_secs() -> u64 {
    15
} // 15 seconds between polls when no messages
const fn default_visibility_timeout_secs() -> u64 {
    300
} // 5 minutes visibility timeout
const fn default_max_number_of_messages() -> u64 {
    10
} // Default batch size
const fn default_true() -> bool {
    true
} // Default for boolean flags

/// Errors that can occur during ingestor initialization
#[derive(Debug, Snafu)]
pub(super) enum IngestorNewError {
    /// The specified number of messages is outside the valid range (1-32)
    #[snafu(display("Invalid value for max_number_of_messages {}", messages))]
    InvalidNumberOfMessages { messages: u64 },
}

/// Errors that can occur during message processing
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Snafu)]
pub enum ProcessingError {
    /// Failed to parse a queue message as an Event Grid notification
    #[snafu(display(
        "Could not parse queue message with id {} as Event Grid notification: {}",
        message_id,
        source
    ))]
    InvalidQueueMessage {
        source: serde_json::Error,
        message_id: String,
    },

    /// Failed to fetch a blob from Azure Storage
    #[snafu(display("Failed to fetch blob {}/{}: {}", container, blob, source))]
    GetBlob {
        source: azure_core_for_storage::Error,
        container: String,
        blob: String,
    },

    /// Failed to read the contents of a blob
    #[snafu(display("Failed to read all of blob {}/{}: {}", container, blob, source))]
    ReadBlob {
        source: Box<dyn FramingError>,
        container: String,
        blob: String,
    },

    /// Failed to send processed events to the Vector pipeline
    #[snafu(display("Failed to flush all of blob {}/{}: {}", container, blob, source))]
    PipelineSend {
        source: SendError,
        container: String,
        blob: String,
    },

    /// Received a blob notification for a different storage account
    #[snafu(display(
        "Blob notification for {}/{} is for a different storage account: {}",
        container,
        blob,
        account
    ))]
    WrongStorageAccount {
        account: String,
        container: String,
        blob: String,
    },

    /// Received an unsupported Event Grid version
    #[snafu(display("Unsupported Event Grid version: {}.", version,))]
    UnsupportedEventGridVersion { version: semver::Version },

    /// Failed to process a blob due to error acknowledgement
    #[snafu(display(
        "Failed to process blob {}/{} due to error acknowledgement",
        container,
        blob
    ))]
    ErrorAcknowledgement { container: String, blob: String },

    /// Blob was not found in Azure Storage (404)
    #[snafu(display("Blob not found {}/{}: {}", container, blob, source))]
    BlobNotFound {
        source: azure_core_for_storage::Error,
        container: String,
        blob: String,
    },
    #[snafu(display(
        "Direct ingest message for blob {}/{} has an empty file_id, ignoring.",
        container,
        blob
    ))]
    EmptyFileId { container: String, blob: String },
}

/// Internal state maintained by the ingestor
pub struct State {
    /// Client for interacting with Azure Blob Storage
    blob_client: BlobServiceClient,
    /// Client for interacting with Azure Queue Storage
    queue_client: QueueServiceClient,

    /// Configuration for multiline log processing
    multiline: Option<line_agg::Config>,
    /// Compression settings for blob data
    compression: super::Compression,

    /// Name of the queue to poll
    queue_name: String,
    /// Seconds to wait between polls when no messages
    poll_secs: u64,
    /// Maximum messages to fetch in one batch
    max_number_of_messages: u64,
    /// Number of concurrent polling tasks
    client_concurrency: usize,
    /// How long messages are invisible after being received
    visibility_timeout_secs: u64,
    /// Whether to delete successfully processed messages
    delete_message: bool,
    /// Whether to delete failed messages
    delete_failed_message: bool,
    /// Decoder for processing blob contents
    decoder: Decoder,

    /// Name of the storage account being monitored
    storage_account_name: String,
    /// Whether to process custom direct ingest messages
    process_custom_message: bool,
    /// Optional callback client for notifying upstream on processing completion
    callback_client: Option<IngestionCallbackClient>,
}

/// Main ingestor implementation that handles Azure Queue message processing
pub(super) struct Ingestor {
    /// Shared state containing configuration and clients
    state: Arc<State>,
}

impl Ingestor {
    /// Creates a new ingestor instance with the provided configuration
    pub(super) async fn new(
        blob_client: BlobServiceClient,
        queue_client: QueueServiceClient,
        config: Config,
        compression: super::Compression,
        multiline: Option<line_agg::Config>,
        decoder: Decoder,
        callback_client: Option<IngestionCallbackClient>,
    ) -> Result<Ingestor, IngestorNewError> {
        // Validate message batch size
        if config.max_number_of_messages < 1 || config.max_number_of_messages > 32 {
            return Err(IngestorNewError::InvalidNumberOfMessages {
                messages: config.max_number_of_messages,
            });
        }

        // Extract storage account name from blob client URL
        let storage_account_name = blob_client
            .url()
            .map(|url| {
                url.host_str()
                    .and_then(|host| host.split('.').next())
                    .unwrap_or("unknown")
                    .to_string()
            })
            .unwrap_or_else(|_| "unknown".to_string());

        // Create shared state
        let state = Arc::new(State {
            blob_client,
            queue_client,
            compression,
            multiline,
            queue_name: config.queue_name,
            poll_secs: config.poll_secs,
            max_number_of_messages: config.max_number_of_messages,
            client_concurrency: config
                .client_concurrency
                .map(|n| n.get())
                .unwrap_or_else(crate::num_threads),
            visibility_timeout_secs: config.visibility_timeout_secs,
            delete_message: config.delete_message,
            delete_failed_message: config.delete_failed_message,
            decoder,
            storage_account_name,
            process_custom_message: config.process_custom_message,
            callback_client,
        });

        Ok(Ingestor { state })
    }

    /// Starts the ingestor process with the specified number of concurrent tasks
    pub(super) async fn run(
        self,
        cx: SourceContext,
        acknowledgements: SourceAcknowledgementsConfig,
        log_namespace: LogNamespace,
    ) -> Result<(), ()> {
        let acknowledgements = cx.do_acknowledgements(acknowledgements);
        let mut handles = Vec::new();

        // Spawn concurrent processing tasks
        for _ in 0..self.state.client_concurrency {
            let process = IngestorProcess::new(
                Arc::clone(&self.state),
                cx.out.clone(),
                cx.shutdown.clone(),
                log_namespace,
                acknowledgements,
            );
            let fut = process.run();
            let handle = tokio::spawn(fut.in_current_span());
            handles.push(handle);
        }

        // Wait for all tasks to complete, propagating any panics
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

/// Individual ingestor process that handles message polling and processing
pub struct IngestorProcess {
    /// Shared state containing configuration and clients
    state: Arc<State>,
    /// Sender for processed events
    out: SourceSender,
    /// Signal for graceful shutdown
    shutdown: ShutdownSignal,
    /// Whether to use acknowledgements
    acknowledgements: bool,
    /// Namespace for log event metadata
    log_namespace: LogNamespace,
    /// Counter for bytes received
    bytes_received: Registered<BytesReceived>,
    /// Counter for events received
    events_received: Registered<EventsReceived>,
}

impl IngestorProcess {
    /// Creates a new ingestor process
    pub fn new(
        state: Arc<State>,
        out: SourceSender,
        shutdown: ShutdownSignal,
        log_namespace: LogNamespace,
        acknowledgements: bool,
    ) -> Self {
        Self {
            state,
            out,
            shutdown,
            acknowledgements,
            log_namespace,
            bytes_received: register!(BytesReceived::from(Protocol::HTTP)),
            events_received: register!(EventsReceived),
        }
    }

    /// Main processing loop that polls for messages and processes them
    ///
    /// This function runs until the shutdown signal is received. For each iteration:
    /// 1. Polls for new messages
    /// 2. Processes each message
    /// 3. Deletes processed messages if configured
    /// 4. Waits for the configured poll interval
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

    /// Single iteration of the processing loop
    ///
    /// This function:
    /// 1. Receives messages from the queue
    /// 2. Processes each message
    /// 3. Deletes successfully processed messages if configured
    /// 4. Logs any errors that occur
    async fn run_once(&mut self) {
        // Receive messages from queue
        let messages = self.receive_messages().await;
        let messages = messages
            .inspect(|messages| {
                debug!(
                    message = "Received messages from queue.",
                    count = %messages.len(),
                    internal_log_rate_limit = true
                );
            })
            .inspect_err(|err| {
                error!(
                    message = "Failed to receive messages from queue.",
                    error = ?err,
                );
            })
            .unwrap_or_default();

        let mut delete_messages = Vec::new();

        // Process each message
        for message in messages {
            let message_id = message.message_id.clone();

            match self.handle_queue_message(&message).await {
                Ok(()) => {
                    debug!(
                        message = "Successfully processed queue message.",
                        message_id = %message_id,
                        internal_log_rate_limit = true
                    );
                    if self.state.delete_message {
                        trace!(message = "Queued message for deletion.", id = &message_id,);
                        delete_messages.push(message.clone());
                    }
                }
                Err(err) => {
                    match err {
                        ProcessingError::BlobNotFound {
                            ref source,
                            ref container,
                            ref blob,
                        } => {
                            warn!(
                                message = "Blob not found.",
                                message_id = &message_id,
                                container = %container,
                                blob = %blob,
                                error = %source,
                            );
                            // Blob no longer exists — retrying won't help.
                            // Delete the queue message to prevent infinite retries.
                            if self.state.delete_message {
                                delete_messages.push(message.clone());
                            }
                        }
                        ProcessingError::EmptyFileId {
                            ref container,
                            ref blob,
                        } => {
                            warn!(
                                message = "Direct ingest message has empty file_id, discarding.",
                                message_id = &message_id,
                                container = %container,
                                blob = %blob,
                            );
                            // Empty file_id is a permanent invalid state — retrying won't help.
                            // Delete the queue message to prevent infinite retries.
                            if self.state.delete_message {
                                delete_messages.push(message.clone());
                            }
                        }
                        _ => {
                            error!(
                                message = "Failed to process queue message.",
                                message_id = &message_id,
                                error = ?err,
                            );
                        }
                    }
                }
            }
        }

        // Delete processed messages
        for message in delete_messages {
            if let Err(err) = self.delete_message(&message).await {
                let message_id = message.message_id.clone();
                error!(
                    message = "Failed to delete message from queue.",
                    message_id = %message_id,
                    error = ?err,
                );
            }
        }

        // Wait for next poll interval
        tokio::time::sleep(Duration::from_secs(self.state.poll_secs)).await;
    }

    /// Dispatches a single queue message to the appropriate handler based on its content.
    ///
    /// The message body is base64-decoded, then deserialized as one of:
    /// - `EventGridEvent` — standard Event Grid blob notification
    /// - `DirectIngestMessage` — custom message requesting ingestion of a specific blob
    ///   (only processed when `process_custom_message` is enabled in the queue config)
    async fn handle_queue_message(&mut self, message: &Message) -> Result<(), ProcessingError> {
        let message_text = &message.message_text;

        // Decode base64 message content
        let decoded_message = STANDARD.decode(message_text).map_err(|e| {
            error!(
                message = "Failed to base64 decode queue message",
                error = ?e,
                message_id = &message.message_id
            );
            ProcessingError::InvalidQueueMessage {
                source: serde_json::Error::custom(format!("Failed to base64 decode: {}", e)),
                message_id: message.message_id.clone(),
            }
        })?;

        // Convert decoded bytes to UTF-8 string
        let decoded_str = String::from_utf8(decoded_message).map_err(|e| {
            error!(
                message = "Failed to convert decoded message to string",
                error = ?e,
                message_id = &message.message_id
            );
            ProcessingError::InvalidQueueMessage {
                source: serde_json::Error::custom(format!("Failed to convert to UTF-8: {}", e)),
                message_id: message.message_id.clone(),
            }
        })?;

        // Represents the time at which the message was inserted into the queue
        let queue_notification_create_timestamp = Utc
            .timestamp_opt(
                message.insertion_time.unix_timestamp(),
                message.insertion_time.nanosecond(),
            )
            .single();

        let message_id = &message.message_id;

        // Try to parse as a QueueEvent (DirectIngest is tried first due to deny_unknown_fields)
        let queue_event: QueueEvent = serde_json::from_str(&decoded_str).map_err(|e| {
            error!(
                message = "Failed to parse decoded message as JSON object",
                error = ?e,
                message_id = message_id,
                decoded_message = %decoded_str
            );
            ProcessingError::InvalidQueueMessage {
                source: e,
                message_id: message_id.clone(),
            }
        })?;

        match queue_event {
            QueueEvent::DirectIngest(msg) => {
                self.handle_direct_ingest(msg, message_id, queue_notification_create_timestamp)
                    .await
            }
            QueueEvent::EventGrid(event) => {
                self.handle_event_grid_event(event, queue_notification_create_timestamp)
                    .await
            }
        }
    }

    /// Handles a direct ingest message — a custom queue message that explicitly
    /// names a blob container and blob path to ingest, bypassing Event Grid notifications.
    ///
    /// Gated behind the `process_custom_message` config flag. When disabled,
    /// direct ingest messages are silently ignored. Also validates that the
    /// `kind` field equals [`DIRECT_INGEST_KIND`] and emits queue processing
    /// lag metrics from the queue insertion time.
    async fn handle_direct_ingest(
        &mut self,
        msg: DirectIngestMessage,
        message_id: &str,
        queue_notification_create_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), ProcessingError> {
        if !self.state.process_custom_message {
            debug!(
                message = "Ignoring direct ingest message because process_custom_message is not enabled.",
                message_id = %message_id,
            );
            return Ok(());
        }
        if msg.kind != DIRECT_INGEST_KIND {
            warn!(
                message = "Unknown direct ingest kind, ignoring.",
                kind = %msg.kind,
                expected = DIRECT_INGEST_KIND,
                message_id = %message_id,
            );
            return Ok(());
        }
        if msg.file_id.is_empty() {
            return Err(ProcessingError::EmptyFileId {
                container: msg.container,
                blob: msg.blob,
            });
        }
        // The blob client is bound to the source's configured storage account, so we
        // reject cross-account requests — consistent with the Event Grid notification path.
        if let Some(ref account) = msg.account {
            if self.state.storage_account_name != *account {
                return Err(ProcessingError::WrongStorageAccount {
                    account: account.clone(),
                    container: msg.container,
                    blob: msg.blob,
                });
            }
        }

        // Emit queue processing lag metric (insertion time → now).
        // This measures how long the message sat in the queue before being picked up.
        if let Some(notification_ts) = queue_notification_create_timestamp {
            let lag_duration = Utc::now().signed_duration_since(notification_ts);
            let lag_seconds = lag_duration.num_milliseconds() as f64 / 1000.0;
            // The shared metric struct uses `bucket` (S3 terminology); for Azure this is the container.
            emit!(QueueNotificationProcessLag {
                lag_seconds,
                cloud: CLOUD_PROVIDER,
                bucket: &msg.container,
            });
        }

        debug!(
            message = "Processing direct ingest message.",
            container = %msg.container,
            blob = %msg.blob,
            account = %self.state.storage_account_name,
            file_id = %msg.file_id,
            message_id = %message_id,
        );

        let processing_start = std::time::Instant::now();
        let result = self
            .process_blob_object(&msg.container, &msg.blob, msg.log_type.as_deref())
            .await;

        // Fire ingestion callback if configured (non-blocking).
        if let Some(ref cb_client) = self.state.callback_client {
            let message_fields = std::collections::HashMap::from([
                ("file_id".to_string(), msg.file_id.clone()),
                ("container".to_string(), msg.container.clone()),
                ("blob".to_string(), msg.blob.clone()),
                (
                    "account".to_string(),
                    self.state.storage_account_name.clone(),
                ),
            ]);
            let _ = cb_client.spawn_notify(&result, processing_start.elapsed(), message_fields);
        }

        result
    }

    /// Processes a single Event Grid event
    ///
    /// This function:
    /// 1. Validates the event type (must be BlobCreated)
    /// 2. Extracts container and blob information
    /// 3. Verifies the storage account
    /// 4. Downloads and processes the blob content
    ///
    /// # Arguments
    /// * `event` - The Event Grid event to process
    ///
    /// # Returns
    /// Ok(()) if processing succeeds, or a ProcessingError if any step fails
    async fn handle_event_grid_event(
        &mut self,
        event: EventGridEvent,
        queue_notification_create_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), ProcessingError> {
        // Capture the processing start time for metrics
        let processing_start_time = Utc::now();

        // Validate event type
        if event.event_type != "Microsoft.Storage.BlobCreated" {
            debug!(
                message = "Ignoring non-BlobCreated event.",
                event_type = %event.event_type,
                subject = %event.subject,
                internal_log_rate_limit = true
            );
            return Ok(());
        }

        // Extract container and blob names from subject
        let (container, blob) = parse_blob_subject(&event.subject).ok_or_else(|| {
            ProcessingError::InvalidQueueMessage {
                source: <serde_json::Error as serde::de::Error>::custom("Invalid subject format"),
                message_id: event.id.clone(),
            }
        })?;

        // Emit object storage non-acknowledgement metrics as we have all the required information
        emit_object_storage_non_ack_metrics(
            &event.event_time,
            queue_notification_create_timestamp,
            processing_start_time,
            CLOUD_PROVIDER,
            container,
        );

        // Verify storage account matches
        if !event.data.url.contains(&self.state.storage_account_name) {
            return Err(ProcessingError::WrongStorageAccount {
                account: self.state.storage_account_name.clone(),
                container: container.to_string(),
                blob: blob.to_string(),
            });
        }

        // Event Grid notifications carry no log type; only direct-ingest messages do.
        self.process_blob_object(container, blob, None).await
    }

    /// Downloads a blob, decompresses, frames, deserializes, enriches, and sends
    /// events downstream. This is the shared processing core used by both Event Grid
    /// notifications and direct ingest messages.
    ///
    async fn process_blob_object(
        &mut self,
        container: &str,
        blob: &str,
        log_type: Option<&str>,
    ) -> Result<(), ProcessingError> {
        let processing_start_time = Utc::now();

        // Own these once up front — they're needed by the blob SDK, error paths,
        // and log enrichment, so we avoid repeated to_owned() calls.
        let container = container.to_owned();
        let blob = blob.to_owned();
        let log_type = log_type.map(|s| s.to_owned());

        // Get blob client and download content
        let blob_client = self
            .state
            .blob_client
            .container_client(&container)
            .blob_client(&blob);

        let mut blob_stream = blob_client.get().into_stream();

        let response_result = blob_stream.next().await.transpose();
        let response_opt = match response_result {
            Ok(opt) => opt,
            Err(err) => {
                // Check if the error is a 404 (Blob not found)
                let is_not_found = matches!(
                    err.kind(),
                    ErrorKind::HttpResponse { status, .. } if *status == azure_core_for_storage::StatusCode::NotFound
                );
                if is_not_found {
                    return Err(ProcessingError::BlobNotFound {
                        source: err,
                        container: container.clone(),
                        blob: blob.clone(),
                    });
                }
                Err(err).context(GetBlobSnafu {
                    container: container.clone(),
                    blob: blob.clone(),
                })?
            }
        };

        let blob_response = response_opt.ok_or_else(|| ProcessingError::GetBlob {
            source: azure_core_for_storage::Error::message(
                ErrorKind::Other,
                "no blob response received",
            ),
            container: container.clone(),
            blob: blob.clone(),
        })?;

        debug!(
            message = "Got blob.",
            container = %container,
            blob = %blob,
            internal_log_rate_limit = true
        );

        // Extract blob metadata and properties
        let properties = blob_response.blob.properties;
        let metadata = blob_response.blob.metadata;

        let content_type = Some(properties.content_type.as_str());
        let content_encoding = properties.content_encoding.as_deref();
        let last_modified = properties.last_modified;

        let timestamp = last_modified;

        // Create batch for event processing
        let (batch, receiver) = BatchNotifier::maybe_new_with_receiver(self.acknowledgements);

        // Prepare blob content stream
        let body_stream = blob_response
            .data
            .map(|result| result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)));

        // Decode blob content based on compression settings
        let blob_reader = super::blob_object_decoder(
            self.state.compression,
            &blob,
            content_encoding,
            content_type,
            Box::new(body_stream),
        )
        .await;

        // Set up error tracking and metrics
        let mut read_error = None;
        let bytes_received = self.bytes_received.clone();
        let events_received = self.events_received.clone();

        // Create stream of decoded lines
        let lines: Box<dyn Stream<Item = Bytes> + Send + Unpin> = Box::new(
            FramedRead::new(blob_reader, self.state.decoder.framer.clone())
                .map(|res| {
                    res.inspect(|bytes| {
                        bytes_received.emit(ByteSize(bytes.len()));
                    })
                    .map_err(|err| {
                        read_error = Some(err);
                    })
                    .ok()
                })
                .take_while(|res| ready(res.is_some()))
                .map(|r| r.expect("validated by take_while")),
        );

        // Apply multiline processing if configured
        let lines: Box<dyn Stream<Item = Bytes> + Send + Unpin> = match &self.state.multiline {
            Some(config) => Box::new(
                LineAgg::new(
                    lines.map(|line| ((), line, ())),
                    line_agg::Logic::new(config.clone()),
                )
                .map(|(_src, line, _context, _lastline_context)| line),
            ),
            None => lines,
        };

        // Prepare metadata for event processing
        let account_name = self.state.storage_account_name.clone();

        // Process each line into events
        let mut stream = lines.flat_map(|line| {
            let events = match self.state.decoder.deserializer_parse(line) {
                Ok((events, _events_size)) => events,
                Err(_error) => {
                    // Error is handled by `codecs::Decoder`, no further handling needed
                    SmallVec::new()
                }
            };

            // Add metadata and process each event
            let events = events
                .into_iter()
                .map(|mut event: Event| {
                    event = event.with_batch_notifier_option(&batch);
                    if let Some(log_event) = event.maybe_as_log_mut() {
                        let metadata_map = metadata.clone().unwrap_or_default();
                        let timestamp_chrono = Utc
                            .timestamp_opt(timestamp.unix_timestamp(), timestamp.nanosecond())
                            .single();
                        handle_single_log(
                            log_event,
                            self.log_namespace,
                            &container,
                            &blob,
                            &account_name,
                            log_type.as_deref(),
                            &metadata_map,
                            timestamp_chrono,
                        );
                    }
                    events_received.emit(CountByteSize(1, event.estimated_json_encoded_size_of()));
                    event
                })
                .collect::<Vec<Event>>();
            futures::stream::iter(events)
        });

        // Send events to pipeline and handle results
        let send_error = match self.out.send_event_stream(&mut stream).await {
            Ok(_) => None,
            Err(_) => {
                let (count, _) = stream.size_hint();
                emit!(StreamClosedError { count });
                Some(SendError::Closed)
            }
        };

        drop(stream);
        drop(batch);

        // Handle any errors that occurred
        let processing_result = if let Some(error) = read_error {
            Err(ProcessingError::ReadBlob {
                source: error,
                container: container.clone(),
                blob: blob.clone(),
            })
        } else if let Some(error) = send_error {
            Err(ProcessingError::PipelineSend {
                source: error,
                container: container.clone(),
                blob: blob.clone(),
            })
        } else {
            // Handle batch acknowledgement
            match receiver {
                None => Ok(()),
                Some(receiver) => {
                    let result = receiver.await;
                    // Emit the acknowledgement metrics
                    emit_object_storage_ack_metrics(
                        processing_start_time,
                        CLOUD_PROVIDER,
                        &container,
                    );

                    match result {
                        BatchStatus::Delivered => {
                            debug!(
                                message = "Blob delivered.",
                                container = %container,
                                blob = %blob,
                                internal_log_rate_limit = true
                            );

                            Ok(())
                        }
                        BatchStatus::Errored => {
                            warn!(
                                message = "Blob processing errored.",
                                container = %container,
                                blob = %blob,
                            );
                            if self.state.delete_failed_message {
                                Ok(())
                            } else {
                                Err(ProcessingError::ErrorAcknowledgement {
                                    container: container.clone(),
                                    blob: blob.clone(),
                                })
                            }
                        }
                        BatchStatus::Rejected => {
                            warn!(
                                message = "Blob was rejected.",
                                container = %container,
                                blob = %blob,
                            );
                            if self.state.delete_failed_message {
                                Ok(())
                            } else {
                                Err(ProcessingError::ErrorAcknowledgement {
                                    container: container.clone(),
                                    blob: blob.clone(),
                                })
                            }
                        }
                    }
                }
            }
        };

        processing_result
    }

    /// Receives messages from the Azure Queue
    ///
    /// # Returns
    /// A Result containing a Vec of messages or an error if the receive operation fails
    async fn receive_messages(&mut self) -> Result<Vec<Message>, azure_core_for_storage::Error> {
        let queue_client = self.state.queue_client.queue_client(&self.state.queue_name);

        let response = queue_client
            .get_messages()
            .number_of_messages(self.state.max_number_of_messages as u8)
            .visibility_timeout(Duration::from_secs(self.state.visibility_timeout_secs))
            .await?;

        Ok(response.messages)
    }

    /// Deletes a message from the Azure Queue
    async fn delete_message(
        &mut self,
        message: &Message,
    ) -> Result<(), azure_core_for_storage::Error> {
        let queue_client = self.state.queue_client.queue_client(&self.state.queue_name);

        queue_client
            .pop_receipt_client(message.clone())
            .delete()
            .await?;

        Ok(())
    }
}

/// Processes a single log event by adding metadata and handling timestamps
fn handle_single_log(
    log: &mut LogEvent,
    log_namespace: LogNamespace,
    container: &str,
    blob: &str,
    account: &str,
    log_type: Option<&str>,
    metadata: &HashMap<String, String>,
    timestamp: Option<DateTime<Utc>>,
) {
    // Add container metadata
    log_namespace.insert_source_metadata(
        AzureBlobConfig::NAME,
        log,
        Some(LegacyKey::Overwrite(path!("container"))),
        path!("container"),
        Bytes::from(container.as_bytes().to_vec()),
    );

    // Add blob metadata
    log_namespace.insert_source_metadata(
        AzureBlobConfig::NAME,
        log,
        Some(LegacyKey::Overwrite(path!("blob"))),
        path!("blob"),
        Bytes::from(blob.as_bytes().to_vec()),
    );

    // Add account metadata
    log_namespace.insert_source_metadata(
        AzureBlobConfig::NAME,
        log,
        Some(LegacyKey::Overwrite(path!("account"))),
        path!("account"),
        Bytes::from(account.as_bytes().to_vec()),
    );

    // Only stamp log_type when the direct-ingest message carried it, so events
    // from plain Event Grid notifications are unchanged.
    if let Some(log_type) = log_type {
        log_namespace.insert_source_metadata(
            AzureBlobConfig::NAME,
            log,
            Some(LegacyKey::Overwrite(path!("log_type"))),
            path!("log_type"),
            Bytes::from(log_type.as_bytes().to_vec()),
        );
    }

    // Add any additional metadata
    if !metadata.is_empty() {
        for (key, value) in metadata {
            log_namespace.insert_source_metadata(
                AzureBlobConfig::NAME,
                log,
                Some(LegacyKey::Overwrite(path!(key))),
                path!("metadata", key.as_str()),
                value.clone(),
            );
        }
    }

    // Add source type metadata
    log_namespace.insert_vector_metadata(
        log,
        log_schema().source_type_key(),
        path!("source_type"),
        Bytes::from_static(AzureBlobConfig::NAME.as_bytes()),
    );

    // Handle timestamps based on namespace
    match log_namespace {
        LogNamespace::Vector => {
            if let Some(timestamp) = timestamp {
                log.insert(
                    metadata_path!(AzureBlobConfig::NAME, "timestamp"),
                    timestamp,
                );
            }
            log.insert(metadata_path!("vector", "ingest_timestamp"), Utc::now());
        }
        LogNamespace::Legacy => {
            if let Some(timestamp_key) = log_schema().timestamp_key() {
                log.try_insert(
                    (PathPrefix::Event, timestamp_key),
                    timestamp.unwrap_or_else(Utc::now),
                );
            }
        }
    };
}

/// Parses a blob subject string to extract container and blob names
fn parse_blob_subject(subject: &str) -> Option<(&str, &str)> {
    let parts: Vec<&str> = subject.split('/').collect();
    if parts.len() >= 7
        && parts[1] == "blobServices"
        && parts[3] == "containers"
        && parts[5] == "blobs"
    {
        Some((parts[4], &subject[subject.rfind("/blobs/").unwrap() + 7..]))
    } else {
        None
    }
}

/// The expected value of the `kind` field in a direct ingest message.
const DIRECT_INGEST_KIND: &str = "INGEST";

/// A direct ingest message that can be placed on the Azure Queue to trigger
/// ingestion of a specific blob without relying on Event Grid notifications.
///
/// Example message:
/// ```json
/// {"kind": "INGEST", "container": "my-container", "blob": "path/to/file.log", "file_id": "f-abc-123", "log_type": "cp_logs"}
/// ```
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DirectIngestMessage {
    /// File identifier assigned by the upstream service.
    /// Used by the ingestion callback component to notify the upstream
    /// service of processing completion.
    pub file_id: String,
    /// Must be "INGEST".
    pub kind: String,
    /// The blob container name.
    pub container: String,
    /// The blob path/key.
    pub blob: String,
    /// Optional storage account override. Falls back to the source's configured account.
    pub account: Option<String>,
    /// Optional log type set by the upstream caller to demarcate the type of
    /// log file being processed. When present, it is stamped onto every emitted
    /// log event so downstream transforms can route on it. Optional: messages
    /// that omit it deserialize to `None` and are not rejected.
    #[serde(default)]
    pub log_type: Option<String>,
}

/// Represents the possible message types that can arrive on the Azure Queue.
///
/// `DirectIngest` is listed first with `#[serde(deny_unknown_fields)]` on the struct,
/// so it is tried first during deserialization but will not false-match Event Grid
/// notifications (which carry additional fields that `deny_unknown_fields` rejects).
#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
enum QueueEvent {
    DirectIngest(DirectIngestMessage),
    EventGrid(EventGridEvent),
}

/// Event Grid event structure for blob notifications.
/// https://learn.microsoft.com/en-us/azure/event-grid/event-schema
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EventGridEvent {
    /// The topic of the event
    pub topic: String,
    /// The subject of the event (blob path)
    pub subject: String,
    /// The type of event (e.g., "Microsoft.Storage.BlobCreated")
    pub event_type: String,
    /// Unique identifier for the event
    pub id: String,
    /// The event data containing blob details
    pub data: EventGridData,
    /// Version of the event data schema
    pub data_version: String,
    /// Version of the event metadata schema
    pub metadata_version: String,

    /// Time when the event occurred in ISO-8601 format (eg. 1970-01-01T00:00:00.000Z).
    /// For the "Microsoft.Storage.BlobCreated" event, this is the
    /// time the blob was created.
    pub event_time: String,
}

/// Event Grid event data structure for blob events
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EventGridData {
    /// The API operation that triggered the event
    #[serde(default)]
    pub api: Option<String>,
    /// Client request ID for the operation
    #[serde(default)]
    pub client_request_id: Option<String>,
    /// Request ID for the operation
    #[serde(default)]
    pub request_id: Option<String>,
    /// ETag of the blob
    #[serde(default)]
    pub e_tag: Option<String>,
    /// Content type of the blob
    pub content_type: String,
    /// Size of the blob in bytes
    #[serde(default)]
    pub content_length: Option<u64>,
    /// Type of the blob (BlockBlob, PageBlob, etc.)
    #[serde(default)]
    pub blob_type: Option<String>,
    /// Access tier of the blob
    #[serde(default)]
    pub access_tier: Option<String>,
    /// URL of the blob
    pub url: String,
    /// Sequencer value for ordering events
    #[serde(default)]
    pub sequencer: Option<String>,
    /// Storage diagnostics information
    #[serde(default)]
    pub storage_diagnostics: Option<StorageDiagnostics>,
}

/// Storage diagnostics information for blob events
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageDiagnostics {
    /// Batch ID for the operation
    #[serde(default)]
    pub batch_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests the blob subject parser with various input formats
    #[test]
    fn test_parse_blob_subject() {
        // Test Case 1: Simple blob path
        // Tests parsing a basic blob path with no subdirectories
        // Expected format: /blobServices/default/containers/{container}/blobs/{blob}
        let subject = "/blobServices/default/containers/testcontainer/blobs/testfile.txt";
        let (container, blob) = parse_blob_subject(subject).unwrap();
        assert_eq!(
            container, "testcontainer",
            "Container name should be correctly extracted"
        );
        assert_eq!(
            blob, "testfile.txt",
            "Blob name should be correctly extracted"
        );

        // Test Case 2: Blob path with subdirectories
        // Tests parsing a blob path that includes subdirectories
        // Verifies the parser can handle nested paths in the blob name
        let subject_with_path =
            "/blobServices/default/containers/testcontainer/blobs/path/to/file.txt";
        let (container, blob) = parse_blob_subject(subject_with_path).unwrap();
        assert_eq!(
            container, "testcontainer",
            "Container name should be correctly extracted with nested paths"
        );
        assert_eq!(
            blob, "path/to/file.txt",
            "Full blob path including subdirectories should be preserved"
        );
    }

    /// Tests deserialization of a minimal valid Event Grid event
    /// Verifies that required fields are properly parsed and optional fields are handled correctly
    #[test]
    fn test_minimal_event_grid_deserialization() {
        // Test Case: Minimal valid Event Grid event
        // JSON contains only required fields (content_type and url) and basic event metadata
        let json = r#"[
            {
                "topic": "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/test/providers/Microsoft.Storage/storageAccounts/testaccount",
                "subject": "/blobServices/default/containers/testcontainer/blobs/testfile.txt",
                "eventType": "Microsoft.Storage.BlobCreated",
                "id": "00000000-0000-0000-0000-000000000000",
                "data": {
                    "contentType": "text/plain",
                    "url": "https://testaccount.blob.core.windows.net/testcontainer/testfile.txt"
                },
                "dataVersion": "",
                "metadataVersion": "1",
                "eventTime": "2024-12-06T03:32:15.7238874Z"
            }
        ]"#;

        // Deserialize and verify event structure
        let events: Vec<EventGridEvent> = serde_json::from_str(json).unwrap();
        assert_eq!(events.len(), 1, "Should parse exactly one event");

        let event = &events[0];
        assert_eq!(
            event.event_type, "Microsoft.Storage.BlobCreated",
            "Event type should match"
        );

        // Verify required fields are present and correct
        assert_eq!(
            event.data.content_type, "text/plain",
            "Content type should match"
        );
        assert_eq!(
            event.data.url, "https://testaccount.blob.core.windows.net/testcontainer/testfile.txt",
            "Blob URL should match"
        );

        // Verify optional fields are None
        assert!(event.data.api.is_none(), "API field should be None");
        assert!(
            event.data.client_request_id.is_none(),
            "Client request ID should be None"
        );
        assert!(event.data.request_id.is_none(), "Request ID should be None");
        assert!(event.data.e_tag.is_none(), "ETag should be None");
        assert!(
            event.data.content_length.is_none(),
            "Content length should be None"
        );
        assert!(event.data.blob_type.is_none(), "Blob type should be None");
        assert!(
            event.data.access_tier.is_none(),
            "Access tier should be None"
        );
        assert!(event.data.sequencer.is_none(), "Sequencer should be None");
        assert!(
            event.data.storage_diagnostics.is_none(),
            "Storage diagnostics should be None"
        );
    }

    /// Tests deserialization of an invalid Event Grid event
    /// Verifies that missing required fields are properly detected
    #[test]
    fn test_invalid_event_grid_deserialization() {
        // Test Case: Invalid Event Grid event
        // JSON is missing required fields (content_type and url)
        let json = r#"[
            {
                "topic": "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/test/providers/Microsoft.Storage/storageAccounts/testaccount",
                "subject": "/blobServices/default/containers/testcontainer/blobs/testfile.txt",
                "eventType": "Microsoft.Storage.BlobCreated",
                "id": "00000000-0000-0000-0000-000000000000",
                "data": {
                    "api": "PutBlob",
                    "clientRequestId": "00000000-0000-0000-0000-000000000000"
                },
                "dataVersion": "",
                "metadataVersion": "1",
                "eventTime": "2024-12-06T03:32:15.7238874Z"
            }
        ]"#;

        // Attempt deserialization and verify it fails
        let result: Result<Vec<EventGridEvent>, _> = serde_json::from_str(json);
        assert!(
            result.is_err(),
            "Deserialization should fail when required fields are missing"
        );

        // Verify error message indicates missing fields
        let err = result.unwrap_err();
        let err_str = err.to_string();
        assert!(
            err_str.contains("missing field")
                || err_str.contains("content_type")
                || err_str.contains("url"),
            "Error message should mention missing required fields, got: {}",
            err_str
        );
    }

    /// Tests parsing of queue configuration from TOML
    /// Verifies that basic configuration can be deserialized correctly
    #[test]
    fn parse_queue_config() {
        // Test Case: Basic Queue Configuration
        // Verifies that a minimal valid configuration can be parsed
        let config: Config = toml::from_str(
            r#"
            queue_name = "my-storage-queue"
            "#,
        )
        .unwrap();

        assert_eq!(
            config.queue_name, "my-storage-queue",
            "Queue name should be correctly parsed from TOML"
        );
    }

    #[test]
    fn test_direct_ingest_message() {
        // Basic direct ingest message
        let value: QueueEvent = serde_json::from_str(
            r#"{"kind": "INGEST", "container": "my-container", "blob": "path/to/file.log", "file_id": "f-123"}"#,
        )
        .unwrap();
        match value {
            QueueEvent::DirectIngest(msg) => {
                assert_eq!(msg.kind, DIRECT_INGEST_KIND);
                assert_eq!(msg.container, "my-container");
                assert_eq!(msg.blob, "path/to/file.log");
                assert!(msg.account.is_none());
                assert_eq!(msg.file_id, "f-123");
                // log_type is optional; a message that omits it parses to None.
                assert!(msg.log_type.is_none());
            }
            _ => panic!("Expected DirectIngest variant"),
        }

        // With optional account
        let value: QueueEvent = serde_json::from_str(
            r#"{"kind": "INGEST", "container": "my-container", "blob": "data.csv", "account": "mystorageaccount", "file_id": "f-456"}"#,
        )
        .unwrap();
        match value {
            QueueEvent::DirectIngest(msg) => {
                assert_eq!(msg.account.as_deref(), Some("mystorageaccount"));
                assert_eq!(msg.file_id, "f-456");
            }
            _ => panic!("Expected DirectIngest variant"),
        }

        // Event Grid notification still parses as EventGrid, not DirectIngest
        let event_grid_json = r#"{
            "topic": "/subscriptions/00000000/resourceGroups/test/providers/Microsoft.Storage/storageAccounts/testaccount",
            "subject": "/blobServices/default/containers/testcontainer/blobs/testfile.txt",
            "eventType": "Microsoft.Storage.BlobCreated",
            "id": "00000000-0000-0000-0000-000000000000",
            "data": {
                "contentType": "text/plain",
                "url": "https://testaccount.blob.core.windows.net/testcontainer/testfile.txt"
            },
            "dataVersion": "",
            "metadataVersion": "1",
            "eventTime": "2024-12-06T03:32:15.7238874Z"
        }"#;
        let value: QueueEvent = serde_json::from_str(event_grid_json).unwrap();
        assert!(matches!(value, QueueEvent::EventGrid(_)));
    }

    #[test]
    fn test_direct_ingest_message_with_log_type() {
        // log_type set by the upstream Log Access service is carried through so
        // the ingestion path can segregate CP / DP-spark / DP-service logs.
        let value: QueueEvent = serde_json::from_str(
            r#"{"kind": "INGEST", "container": "my-container", "blob": "path/to/file.log", "file_id": "f-123", "log_type": "cp_logs"}"#,
        )
        .unwrap();
        match value {
            QueueEvent::DirectIngest(msg) => {
                assert_eq!(msg.log_type.as_deref(), Some("cp_logs"));
            }
            _ => panic!("Expected DirectIngest variant"),
        }
    }

    #[test]
    fn handle_single_log_stamps_log_type_when_present() {
        let mut log = LogEvent::default();
        handle_single_log(
            &mut log,
            LogNamespace::Legacy,
            "my-container",
            "path/to/file.log",
            "myaccount",
            Some("dp_spark_logs"),
            &HashMap::new(),
            None,
        );
        assert_eq!(log["log_type"], "dp_spark_logs".into());
    }

    #[test]
    fn handle_single_log_omits_log_type_when_absent() {
        let mut log = LogEvent::default();
        handle_single_log(
            &mut log,
            LogNamespace::Legacy,
            "my-container",
            "path/to/file.log",
            "myaccount",
            None,
            &HashMap::new(),
            None,
        );
        assert!(
            log.get("log_type").is_none(),
            "log_type must be absent when the message did not carry it"
        );
    }

    #[test]
    fn test_direct_ingest_unknown_kind_still_parses() {
        // A message with unknown kind still deserializes into DirectIngest —
        // the runtime check in handle_direct_ingest rejects it, not serde.
        let value: QueueEvent = serde_json::from_str(
            r#"{"kind": "UNKNOWN", "container": "c", "blob": "b", "file_id": "f-1"}"#,
        )
        .unwrap();
        match value {
            QueueEvent::DirectIngest(msg) => {
                assert_eq!(msg.kind, "UNKNOWN");
            }
            _ => panic!("Expected DirectIngest variant"),
        }
    }

    #[test]
    fn test_direct_ingest_extra_fields_rejected() {
        // deny_unknown_fields on DirectIngestMessage means a message with extra
        // fields won't match DirectIngest. It should fall through to another
        // variant or fail to parse entirely.
        let msg_with_extra = r#"{"kind": "INGEST", "container": "c", "blob": "b", "file_id": "f-1", "unexpected_field": true}"#;
        let result: Result<QueueEvent, _> = serde_json::from_str(msg_with_extra);
        // Should not parse as DirectIngest (deny_unknown_fields), and won't match
        // EventGridEvent either, so parsing fails.
        assert!(result.is_err());
    }

    #[test]
    fn test_process_custom_message_config_parsing() {
        // Default: process_custom_message is false
        let config: Config = toml::from_str(
            r#"
            queue_name = "my-storage-queue"
            "#,
        )
        .unwrap();
        assert!(!config.process_custom_message);

        // Explicitly enabled
        let config: Config = toml::from_str(
            r#"
            queue_name = "my-storage-queue"
            process_custom_message = true
            "#,
        )
        .unwrap();
        assert!(config.process_custom_message);

        // Explicitly disabled
        let config: Config = toml::from_str(
            r#"
            queue_name = "my-storage-queue"
            process_custom_message = false
            "#,
        )
        .unwrap();
        assert!(!config.process_custom_message);
    }

    #[test]
    fn test_direct_ingest_missing_required_fields() {
        let cases = [
            (
                "missing blob",
                r#"{"kind": "INGEST", "container": "c", "file_id": "f-1"}"#,
            ),
            (
                "missing container",
                r#"{"kind": "INGEST", "blob": "b", "file_id": "f-1"}"#,
            ),
            (
                "missing kind",
                r#"{"container": "c", "blob": "b", "file_id": "f-1"}"#,
            ),
            (
                "missing file_id",
                r#"{"kind": "INGEST", "container": "c", "blob": "b"}"#,
            ),
        ];
        for (name, json) in cases {
            let result: Result<QueueEvent, _> = serde_json::from_str(json);
            assert!(result.is_err(), "expected error for case: {name}");
        }
    }

    #[test]
    fn test_direct_ingest_base64_round_trip() {
        // Simulate the actual message path: base64 encode then decode
        let raw_json = r#"{"kind": "INGEST", "container": "my-container", "blob": "path/to/file.log", "file_id": "f-b64"}"#;
        let encoded = STANDARD.encode(raw_json);
        let decoded_bytes = STANDARD.decode(&encoded).unwrap();
        let decoded_str = String::from_utf8(decoded_bytes).unwrap();
        let value: QueueEvent = serde_json::from_str(&decoded_str).unwrap();
        match value {
            QueueEvent::DirectIngest(msg) => {
                assert_eq!(msg.kind, DIRECT_INGEST_KIND);
                assert_eq!(msg.container, "my-container");
                assert_eq!(msg.blob, "path/to/file.log");
            }
            _ => panic!("Expected DirectIngest variant"),
        }
    }

    #[test]
    fn test_event_grid_base64_round_trip() {
        // Ensure Event Grid messages still work through the base64 round-trip
        let raw_json = r#"{
            "topic": "/subscriptions/00000000/resourceGroups/test/providers/Microsoft.Storage/storageAccounts/testaccount",
            "subject": "/blobServices/default/containers/testcontainer/blobs/testfile.txt",
            "eventType": "Microsoft.Storage.BlobCreated",
            "id": "00000000-0000-0000-0000-000000000000",
            "data": {
                "contentType": "text/plain",
                "url": "https://testaccount.blob.core.windows.net/testcontainer/testfile.txt"
            },
            "dataVersion": "",
            "metadataVersion": "1",
            "eventTime": "2024-12-06T03:32:15.7238874Z"
        }"#;
        let encoded = STANDARD.encode(raw_json);
        let decoded_bytes = STANDARD.decode(&encoded).unwrap();
        let decoded_str = String::from_utf8(decoded_bytes).unwrap();
        let value: QueueEvent = serde_json::from_str(&decoded_str).unwrap();
        assert!(matches!(value, QueueEvent::EventGrid(_)));
    }

    #[test]
    fn test_azure_blob_config_with_ingestion_callback() {
        let config: super::AzureBlobConfig = toml::from_str(
            r#"
                connection_string = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net"

                [queue]
                queue_name = "my-queue"
                process_custom_message = true

                [ingestion_callback]

                [ingestion_callback.on_success]
                uri = "/v2/files/{{message.file_id}}/mark-successful"

                [ingestion_callback.on_failure]
                uri = "/v2/files/{{message.file_id}}/mark-failed"

                [ingestion_callback.on_failure.body]
                file_id = "{{message.file_id}}"
                error_message = "{{error_message}}"

                [ingestion_callback.request]
                base_url = "https://log-access.example.com"

                [ingestion_callback.auth]
                strategy = "bearer"
                token = "my-token"
            "#,
        )
        .unwrap();

        let cb = config
            .ingestion_callback
            .expect("ingestion_callback should be present");
        assert!(cb.on_success.is_some());
        assert!(cb.on_failure.is_some());
        assert_eq!(cb.request.base_url, "https://log-access.example.com");
        assert!(cb.auth.is_some());

        let on_failure = cb.on_failure.unwrap();
        assert_eq!(on_failure.body.len(), 2);
        assert_eq!(on_failure.body["file_id"], "{{message.file_id}}");
    }
}
