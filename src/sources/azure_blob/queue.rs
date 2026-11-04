// Standard library imports for core functionality
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use std::{
    future::ready,
    num::{NonZeroU64, NonZeroUsize},
    panic,
    sync::Arc,
};

// Azure SDK imports for blob and queue operations
// Note: Using azure_core_for_storage (v0.21) to match azure_storage_* crate versions
use azure_core_for_storage::StatusCode;
use azure_core_for_storage::error::ErrorKind;
use azure_core_for_storage::request_options::IfMatchCondition;
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::*;
use azure_storage_queues::operations::Message;
use azure_storage_queues::prelude::*;

// Utility and serialization imports
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use futures::{FutureExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use smallvec::SmallVec;
use snafu::{ResultExt, Snafu};
use tokio::io::AsyncRead;
use tokio::{pin, select};
use tokio_util::codec::FramedRead;
use tracing::Instrument;

// Vector-specific imports
use vector_lib::codecs::decoding::FramingError;
use vector_lib::configurable::configurable_component;
use vector_lib::internal_event::{
    ByteSize, BytesReceived, CountByteSize, InternalEventHandle as _, Protocol, Registered,
    error_stage, error_type,
};
use vector_lib::source_sender::SendError;

use crate::codecs::Decoder;
use crate::event::{Event, LogEvent};
use crate::internal_events::{
    AzureBlobMessageProcessingError, AzureBlobQueueDeleteFailed, AzureBlobQueueMetadataFailed,
    AzureBlobQueueMetadataTimedOut, AzureBlobQueueReceiveFailed, EventsReceived,
    ObjectStorageBlobTruncated, QueueMessageCount, QueueMessageDeleteSucceeded,
    QueueMessageReceived, QueueNotificationProcessLag, StreamClosedError,
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
    sources::util::TolerateTrailingEof,
    sources::util::object_storage_drain_gate::{GateDecision, ObjectStorageGate},
    sources::util::object_storage_rate_limit::{ByteCounter, ObjectStorageRateLimiter},
    tls::TlsConfig,
};
use vector_lib::config::{LegacyKey, LogNamespace, log_schema};
use vector_lib::event::MaybeAsLogMut;
use vector_lib::lookup::{PathPrefix, metadata_path, path};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;

const CLOUD_PROVIDER: &str = "azure";

/// Default size of each ranged GET used to read a blob (the SDK's own default, made explicit).
const DEFAULT_BLOB_CHUNK_SIZE: u64 = 16 * 1024 * 1024;

/// Tracks how much of the blob the page chain has promised, so the caller can tell a complete
/// read from a short one. `GetBlobResponse::blob.properties.content_length` is only the length of
/// the first *page*, never the blob's size, so it can't serve as the expected total.
#[derive(Debug, Default)]
struct BlobReadProgress {
    /// Bytes the server has said the blob prefix up to the latest page contains (`Content-Range`
    /// end + 1). Pages are contiguous from offset 0, so this is also the cumulative expected size.
    expected_bytes: AtomicU64,
    /// Set once a page reported no remaining range, i.e. the final page was reached.
    last_page_reached: AtomicBool,
}

impl BlobReadProgress {
    /// Records a page whose bytes `0..expected` the server has promised so far.
    fn record_page(&self, expected: u64, last: bool) {
        self.expected_bytes.fetch_max(expected, Ordering::Relaxed);
        if last {
            self.last_page_reached.store(true, Ordering::Relaxed);
        }
    }

    fn final_page_reached(&self) -> bool {
        self.last_page_reached.load(Ordering::Relaxed)
    }

    /// Compressed bytes that must have been pulled for the read to count as complete: the blob's
    /// advertised size once its final page was reached. If the page chain was abandoned before
    /// the final page, part of the blob was never requested, so nothing short of `u64::MAX`
    /// qualifies.
    fn required_bytes(&self) -> u64 {
        if self.final_page_reached() {
            self.expected_bytes.load(Ordering::Relaxed)
        } else {
            u64::MAX
        }
    }
}

/// Chains the remaining pages of a blob after `first` into one lazily-fetched body stream.
///
/// `GetBlobBuilder::into_stream` is a pager of `chunk_size` ranged GETs; reading only its first
/// item silently drops everything past the first page. Pages after the first are requested with
/// `If-Match: <etag of the first page>`, so a blob overwritten mid-read fails with 412 instead of
/// splicing two versions. The next page is requested only after the previous body is drained, so
/// memory stays bounded by one in-flight chunk, as with a single streaming GET.
///
/// `next_start` is where the page after `first_body` begins, or `None` if it was the only page.
fn chain_blob_pages(
    blob_client: BlobClient,
    chunk_size: u64,
    etag: String,
    first_body: azure_core_for_storage::ResponseBody,
    next_start: Option<u64>,
    progress: Arc<BlobReadProgress>,
) -> std::pin::Pin<Box<dyn Stream<Item = Result<Bytes, azure_core_for_storage::Error>> + Send>> {
    use futures::TryStreamExt;

    let rest = futures::stream::try_unfold(next_start, move |next| {
        let blob_client = blob_client.clone();
        let etag = etag.clone();
        let progress = Arc::clone(&progress);
        async move {
            let Some(start) = next else {
                return Ok(None);
            };
            let page = blob_client
                .get()
                .range(start..)
                .chunk_size(chunk_size)
                .if_match(IfMatchCondition::Match(etag))
                .into_stream()
                .next()
                .await
                .ok_or_else(|| {
                    azure_core_for_storage::Error::message(
                        ErrorKind::Other,
                        "no blob response received for follow-up page",
                    )
                })??;
            let more = page.remaining_range.is_some();
            // A ranged GET always answers 206 with Content-Range; without it we can't tell where
            // the page ended, so fail rather than guess.
            let Some(azure_core_for_storage::request_options::Range::Range(r)) =
                page.content_range.as_ref()
            else {
                return Err(azure_core_for_storage::Error::message(
                    ErrorKind::Other,
                    "follow-up blob page has no Content-Range",
                ));
            };
            let following = more.then_some(r.end + 1);
            progress.record_page(r.end + 1, !more);
            Ok(Some((page.data, following)))
        }
    });

    Box::pin(
        futures::stream::once(futures::future::ready(Ok(first_body)))
            .chain(rest)
            .try_flatten(),
    )
}

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

    /// How often to sample the queue's approximate depth (backlog) for the `queue_message_count`
    /// gauge, in seconds. Sampled on a dedicated timer independent of `poll_secs`, off the message
    /// receive/delete path, so it never adds latency to draining and its request rate stays fixed
    /// even if `poll_secs` is lowered. Depth moves slowly; keep this coarse. Must be greater than
    /// zero.
    #[serde(default = "default_depth_poll_secs")]
    #[derivative(Default(value = "default_depth_poll_secs()"))]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    pub(super) depth_poll_secs: NonZeroU64,

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

    /// Whether to ingest the decodable prefix of a truncated compressed blob and ack it, instead
    /// of failing the read and redelivering the message forever.
    ///
    /// A gzip/zstd archive whose committed bytes end mid-member (a torn upload) decodes fine up to
    /// the tear, then errors with `UnexpectedEof`. The blob is immutable, so leaving the read to
    /// error redelivers the same un-decodable blob indefinitely, re-ingesting the decodable prefix
    /// as duplicates each cycle. When enabled, the source ingests the records that did decode, acks
    /// the message, and emits `ObjectStorageBlobTruncated` for the dropped tail.
    ///
    /// Defaults to `false`: tolerating truncation drops the torn tail (data loss), so it is
    /// opt-in per source. It only ever applies to a fully-downloaded but truncated archive — a
    /// short *transfer* (fewer bytes than `Content-Length`) still redelivers regardless of this
    /// setting.
    #[serde(default)]
    #[derivative(Default)]
    pub(super) tolerate_truncated_archives: bool,

    /// Size, in bytes, of each ranged GET used to read a blob.
    ///
    /// Blobs larger than this are read as a sequence of ranged requests, each pinned to the first
    /// request's ETag so a blob overwritten mid-read fails instead of mixing versions. Larger
    /// values mean fewer round trips; smaller values bound the bytes held in flight per blob.
    #[serde(default = "default_blob_read_chunk_size_bytes")]
    #[derivative(Default(value = "default_blob_read_chunk_size_bytes()"))]
    #[configurable(metadata(docs::type_unit = "bytes"))]
    #[configurable(metadata(docs::human_name = "Blob Read Chunk Size"))]
    pub(super) blob_read_chunk_size_bytes: NonZeroU64,
}

// Default configuration values
const fn default_poll_secs() -> u64 {
    15
} // 15 seconds between polls when no messages
const fn default_depth_poll_secs() -> NonZeroU64 {
    NonZeroU64::new(60).unwrap()
} // 1 minute between queue-depth (backlog) samples
const fn default_visibility_timeout_secs() -> u64 {
    300
} // 5 minutes visibility timeout
const fn default_blob_read_chunk_size_bytes() -> NonZeroU64 {
    NonZeroU64::new(DEFAULT_BLOB_CHUNK_SIZE).unwrap()
} // 16 MiB, the Azure SDK's own default
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
    /// The queue message body is not valid base64
    #[snafu(display("Could not base64-decode queue message: {}", source))]
    InvalidBase64 { source: base64::DecodeError },

    /// The base64-decoded queue message body is not valid UTF-8
    #[snafu(display("Could not convert decoded queue message to UTF-8: {}", source))]
    InvalidUtf8 { source: std::string::FromUtf8Error },

    /// The decoded queue message body is not a JSON object this source understands
    #[snafu(display(
        "Could not parse decoded queue message as an Event Grid notification or direct ingest message: {}",
        source
    ))]
    InvalidJson { source: serde_json::Error },

    /// An Event Grid notification's subject is not a blob path
    #[snafu(display("Invalid Event Grid blob subject: {}", subject))]
    InvalidBlobSubject { subject: String },

    /// Failed to fetch a blob from Azure Storage
    #[snafu(display("Failed to fetch blob {}/{}: {}", container, blob, source))]
    GetBlob {
        source: azure_core_for_storage::Error,
        container: String,
        blob: String,
    },

    /// The blob fetch completed without yielding a response
    #[snafu(display("No response received while fetching blob {}/{}", container, blob))]
    GetBlobEmptyResponse { container: String, blob: String },

    /// Failed to read the contents of a blob
    #[snafu(display("Failed to read all of blob {}/{}: {}", container, blob, source))]
    ReadBlob {
        source: Box<dyn FramingError>,
        container: String,
        blob: String,
    },

    /// The blob body ended before the advertised content length was received (short transfer)
    #[snafu(display(
        "Incomplete download of blob {}/{}: read {} bytes, {}",
        container,
        blob,
        bytes_read,
        expected_size_text(*content_length)
    ))]
    IncompleteBlobDownload {
        bytes_read: u64,
        /// Advertised blob size; `None` if the page chain stopped before the final page, so the
        /// size is not known.
        content_length: Option<u64>,
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

    /// A direct-ingest message named a storage account not permitted by the source's
    /// `allowed_storage_accounts` allowlist (or a cross-account read under account-bound auth).
    #[snafu(display(
        "Direct ingest for {}/{} names storage account {} which is not in the allowlist",
        container,
        blob,
        account
    ))]
    DisallowedStorageAccount {
        account: String,
        container: String,
        blob: String,
    },

    /// Received an unsupported Event Grid version
    #[snafu(display("Unsupported Event Grid version: {}.", version,))]
    UnsupportedEventGridVersion { version: semver::Version },

    /// The pipeline acknowledged the blob's events as errored
    #[snafu(display(
        "Blob {}/{} was acknowledged as errored by the pipeline",
        container,
        blob
    ))]
    BlobDeliveryErrored { container: String, blob: String },

    /// The pipeline rejected the blob's events
    #[snafu(display("Blob {}/{} was rejected by the pipeline", container, blob))]
    BlobDeliveryRejected { container: String, blob: String },

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

/// Classifies a failed blob fetch (`GetBlob`) into an `(error_type, error_code)` pair.
///
/// `error_code` is a bounded static string so it can label `component_errors_total`.
/// Status-specific codes distinguish the causes operators act on (forbidden, throttled,
/// server-side); everything else falls back to the generic code for its kind.
fn get_blob_failure(err: &azure_core_for_storage::Error) -> (&'static str, &'static str) {
    match err.kind() {
        ErrorKind::HttpResponse { status, .. } => {
            let code = if *status == StatusCode::Forbidden {
                "get_blob_forbidden"
            } else if *status == StatusCode::TooManyRequests
                || *status == StatusCode::ServiceUnavailable
            {
                "get_blob_throttled"
            } else if status.is_server_error() {
                "get_blob_server_error"
            } else {
                "get_blob_failed"
            };
            (error_type::REQUEST_FAILED, code)
        }
        ErrorKind::Io => (error_type::CONNECTION_FAILED, "get_blob_io_failed"),
        ErrorKind::Credential => (
            error_type::CONFIGURATION_FAILED,
            "get_blob_credential_failed",
        ),
        _ => (error_type::REQUEST_FAILED, "get_blob_failed"),
    }
}

fn expected_size_text(content_length: Option<u64>) -> String {
    match content_length {
        Some(total) => format!("expected {total}"),
        None => "stopped before the final page".to_string(),
    }
}

impl ProcessingError {
    /// The `error_type` label for `component_errors_total`. Exhaustive on purpose: a new variant
    /// must be classified before it compiles.
    pub(crate) fn error_type(&self) -> &'static str {
        match self {
            Self::InvalidBase64 { .. }
            | Self::InvalidUtf8 { .. }
            | Self::InvalidJson { .. }
            | Self::InvalidBlobSubject { .. }
            | Self::UnsupportedEventGridVersion { .. } => error_type::PARSER_FAILED,
            Self::EmptyFileId { .. }
            | Self::WrongStorageAccount { .. }
            | Self::DisallowedStorageAccount { .. } => error_type::CONDITION_FAILED,
            Self::GetBlob { source, .. } => get_blob_failure(source).0,
            Self::GetBlobEmptyResponse { .. } | Self::BlobNotFound { .. } => {
                error_type::REQUEST_FAILED
            }
            Self::ReadBlob { .. } | Self::IncompleteBlobDownload { .. } => {
                error_type::READER_FAILED
            }
            Self::PipelineSend { .. } => error_type::WRITER_FAILED,
            Self::BlobDeliveryErrored { .. } | Self::BlobDeliveryRejected { .. } => {
                error_type::ACKNOWLEDGMENT_FAILED
            }
        }
    }

    /// The bounded, static `error_code` label for `component_errors_total`.
    pub(crate) fn error_code(&self) -> &'static str {
        match self {
            Self::InvalidBase64 { .. } => "invalid_base64",
            Self::InvalidUtf8 { .. } => "invalid_utf8",
            Self::InvalidJson { .. } => "invalid_json",
            Self::InvalidBlobSubject { .. } => "invalid_blob_subject",
            Self::UnsupportedEventGridVersion { .. } => "unsupported_event_grid_version",
            Self::EmptyFileId { .. } => "empty_file_id",
            Self::WrongStorageAccount { .. } => "wrong_storage_account",
            Self::DisallowedStorageAccount { .. } => "disallowed_storage_account",
            Self::GetBlob { source, .. } => get_blob_failure(source).1,
            Self::GetBlobEmptyResponse { .. } => "get_blob_empty_response",
            Self::BlobNotFound { .. } => "blob_not_found",
            Self::ReadBlob { .. } => "read_blob_failed",
            Self::IncompleteBlobDownload { .. } => "incomplete_blob_download",
            Self::PipelineSend { .. } => "pipeline_send_failed",
            Self::BlobDeliveryErrored { .. } => "delivery_errored",
            Self::BlobDeliveryRejected { .. } => "delivery_rejected",
        }
    }

    /// The `stage` label for `component_errors_total`.
    pub(crate) const fn stage(&self) -> &'static str {
        match self {
            Self::InvalidBase64 { .. }
            | Self::InvalidUtf8 { .. }
            | Self::InvalidJson { .. }
            | Self::InvalidBlobSubject { .. }
            | Self::UnsupportedEventGridVersion { .. }
            | Self::EmptyFileId { .. }
            | Self::WrongStorageAccount { .. }
            | Self::DisallowedStorageAccount { .. } => error_stage::PROCESSING,
            Self::GetBlob { .. }
            | Self::GetBlobEmptyResponse { .. }
            | Self::BlobNotFound { .. }
            | Self::ReadBlob { .. }
            | Self::IncompleteBlobDownload { .. } => error_stage::RECEIVING,
            Self::PipelineSend { .. }
            | Self::BlobDeliveryErrored { .. }
            | Self::BlobDeliveryRejected { .. } => error_stage::SENDING,
        }
    }

    /// Whether retrying can never succeed, so the failure is logged at `warn` rather than `error`.
    pub(crate) const fn is_permanent(&self) -> bool {
        matches!(
            self,
            Self::EmptyFileId { .. }
                | Self::WrongStorageAccount { .. }
                | Self::DisallowedStorageAccount { .. }
                | Self::BlobNotFound { .. }
        )
    }

    /// Whether this failure is already counted by another event, so the per-message error event
    /// must not count it again.
    ///
    /// - `PipelineSend` is counted by `StreamClosedError` where the send fails.
    /// - Delivery failures are counted where the batch status is observed, because
    ///   `delete_failed_message` can turn them into `Ok` and they would otherwise go uncounted.
    pub(crate) const fn reported_elsewhere(&self) -> bool {
        matches!(
            self,
            Self::PipelineSend { .. }
                | Self::BlobDeliveryErrored { .. }
                | Self::BlobDeliveryRejected { .. }
        )
    }
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
    /// Seconds between queue-depth (backlog) samples for the `queue_message_count` gauge
    depth_poll_secs: NonZeroU64,
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
    /// Whether to tolerate a truncated trailing gzip/zstd member (ingest the decodable prefix and
    /// ack) rather than redelivering an un-decodable blob forever. See `Config`.
    tolerate_truncated_archives: bool,
    /// Optional callback client for notifying upstream on processing completion
    callback_client: Option<IngestionCallbackClient>,
    /// Blob clients for allowlisted cross-account read targets, keyed by account and built
    /// eagerly at startup. The configured account is served by `blob_client`, not listed here.
    extra_blob_clients: HashMap<String, BlobServiceClient>,
    /// Byte-rate pacer shared across polling workers
    rate_limiter: ObjectStorageRateLimiter,
    /// Drains the queue without fetching blobs while the table is disabled
    gate: Option<ObjectStorageGate>,
    /// Size of each ranged GET the blob is read in; see [`chain_blob_pages`] and
    /// `Config::blob_read_chunk_size_bytes`.
    blob_chunk_size: u64,
}

impl State {
    /// Returns the blob service client to read `account`: the configured account's own client, or
    /// an allowlisted cross-account client built at startup. `None` if `account` is not permitted.
    fn blob_client_for(&self, account: &str) -> Option<BlobServiceClient> {
        if account == self.storage_account_name {
            Some(self.blob_client.clone())
        } else {
            self.extra_blob_clients.get(account).cloned()
        }
    }
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
        cross_account_credentials: Option<StorageCredentials>,
        allowed_storage_accounts: Vec<String>,
        rate_limiter: ObjectStorageRateLimiter,
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

        // Build a blob client for each allowlisted cross-account target up front, so any transport
        // setup failure surfaces here rather than on the first cross-account message. Cross-account
        // reads reuse the account-agnostic token, so this is empty under connection-string auth.
        // The configured account is served by `blob_client`.
        let extra_blob_clients: HashMap<String, BlobServiceClient> = match cross_account_credentials
        {
            Some(credentials) => allowed_storage_accounts
                .into_iter()
                .filter(|account| !account.is_empty() && account != &storage_account_name)
                .map(|account| {
                    let client =
                        super::build_blob_service_client(account.clone(), credentials.clone());
                    (account, client)
                })
                .collect(),
            None => HashMap::new(),
        };

        // Create shared state
        let state = Arc::new(State {
            blob_client,
            queue_client,
            compression,
            multiline,
            queue_name: config.queue_name,
            poll_secs: config.poll_secs,
            depth_poll_secs: config.depth_poll_secs,
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
            tolerate_truncated_archives: config.tolerate_truncated_archives,
            callback_client,
            extra_blob_clients,
            rate_limiter,
            gate: None,
            blob_chunk_size: config.blob_read_chunk_size_bytes.get(),
        });

        Ok(Ingestor { state })
    }

    /// Applies `pause_when_listed` / `drain_when_disabled` to every receive.
    pub(super) fn with_gate(mut self, gate: Option<ObjectStorageGate>) -> Self {
        Arc::get_mut(&mut self.state)
            .expect("state is not shared before run")
            .gate = gate;
        self
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

        // Spawn a single queue-depth sampler, independent of client_concurrency: it polls the queue's
        // approximate backlog on a coarse timer, off the receive/delete path, and emits the
        // `queue_message_count` gauge. One metadata call per `depth_poll_secs` per queue regardless of
        // how many worker tasks drain it.
        let depth_handle = tokio::spawn(
            Self::run_depth_poller(Arc::clone(&self.state), cx.shutdown.clone()).in_current_span(),
        );
        handles.push(depth_handle);

        // Wait for all tasks to complete, propagating any panics
        for handle in handles.drain(..) {
            if let Err(e) = handle.await
                && e.is_panic()
            {
                panic::resume_unwind(e.into_panic());
            }
        }

        Ok(())
    }

    /// Periodically samples the queue's approximate depth (backlog) and emits the
    /// `queue_message_count` gauge. Runs on its own timer, decoupled from message polling, so it adds
    /// no latency to draining and its request rate is independent of `poll_secs`. Each sample is a
    /// single cheap Get-Queue-Metadata call bounded by a timeout, so a slow or hung call can neither
    /// stall this loop nor pile up; on error or timeout it skips the sample and retries next tick.
    async fn run_depth_poller(state: Arc<State>, shutdown: ShutdownSignal) {
        // A depth sample must never wedge the loop; keep this well under the tick interval.
        const METADATA_TIMEOUT: Duration = Duration::from_secs(10);

        let shutdown = shutdown.fuse();
        pin!(shutdown);

        let mut ticker = tokio::time::interval(Duration::from_secs(state.depth_poll_secs.get()));
        // On a missed tick (e.g. a slow metadata call), skip it rather than firing a catch-up burst.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            select! {
                _ = &mut shutdown => break,
                _ = ticker.tick() => {
                    let queue_client = state.queue_client.queue_client(&state.queue_name);
                    match tokio::time::timeout(
                        METADATA_TIMEOUT,
                        queue_client.get_metadata().into_future(),
                    )
                    .await
                    {
                        Ok(Ok(response)) => {
                            emit!(QueueMessageCount {
                                count: response.approximate_messages_count as u64,
                                cloud: CLOUD_PROVIDER,
                            });
                        }
                        Ok(Err(err)) => {
                            emit!(AzureBlobQueueMetadataFailed { error: &err });
                        }
                        Err(_) => {
                            emit!(AzureBlobQueueMetadataTimedOut);
                        }
                    }
                }
            }
        }
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
    /// Decoded bytes processed in the current batch, drained after each poll to pace.
    byte_counter: ByteCounter,
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
            byte_counter: ByteCounter::default(),
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
        if let Some(gate) = &self.state.gate
            && gate.skip_paused_poll(self.state.poll_secs).await
        {
            return;
        }

        // Receive messages from queue
        let messages = self.receive_messages().await;
        let messages = messages
            .inspect(|messages| {
                debug!(
                    message = "Received messages from queue.",
                    count = %messages.len(),
                    internal_log_rate_limit = true
                );
                // Count messages pulled from the Storage Queue (the received leg of received-vs-deleted
                // drain health; the delete leg is emitted after the delete loop below). Skip empty
                // receives so an idle source does not emit a zero-count event every poll interval,
                // mirroring the delete leg's `deleted_count > 0` guard.
                if !messages.is_empty() {
                    emit!(QueueMessageReceived {
                        count: messages.len() as u64,
                        cloud: CLOUD_PROVIDER,
                    });
                }
            })
            .inspect_err(|err| {
                emit!(AzureBlobQueueReceiveFailed { error: err });
            })
            .unwrap_or_default();

        // A disabled table drains without fetching blobs, and skips the post-poll sleep so a
        // backlog empties at the queue API's pace rather than `poll_secs`.
        let state = Arc::clone(&self.state);
        if !messages.is_empty()
            && let Some(gate) = &state.gate
        {
            match gate.decide_batch(messages.len()) {
                GateDecision::Process => {}
                GateDecision::Drain => {
                    self.drain_messages(gate, messages).await;
                    return;
                }
                GateDecision::Pause(_) => return,
            }
        }

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
                    // Count and log the failure once here. Failures already counted elsewhere are
                    // skipped (see `ProcessingError::reported_elsewhere`).
                    if !err.reported_elsewhere() {
                        emit!(AzureBlobMessageProcessingError {
                            message_id: &message_id,
                            error: &err,
                        });
                    }
                    match err {
                        // Blob no longer exists — retrying won't help.
                        // Delete the queue message to prevent infinite retries.
                        ProcessingError::BlobNotFound { .. } => {
                            if self.state.delete_message {
                                delete_messages.push(message.clone());
                            }
                        }
                        // Empty file_id is a permanent invalid state — retrying won't help.
                        // Delete the queue message to prevent infinite retries.
                        ProcessingError::EmptyFileId { .. } => {
                            if self.state.delete_message {
                                delete_messages.push(message.clone());
                            }
                        }
                        // Permanent: the allowlist is static config, so a retry can never
                        // succeed. Delete to avoid redelivery until queue TTL.
                        ProcessingError::DisallowedStorageAccount { .. }
                        | ProcessingError::WrongStorageAccount { .. } => {
                            if self.state.delete_message {
                                delete_messages.push(message.clone());
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        // Delete processed messages
        let mut deleted_count: u64 = 0;
        for message in delete_messages {
            if let Err(err) = self.delete_message(&message).await {
                emit!(AzureBlobQueueDeleteFailed {
                    message_id: &message.message_id,
                    error: &err,
                });
            } else {
                deleted_count += 1;
            }
        }
        // Count messages successfully deleted (the deleted leg of received-vs-deleted drain health).
        if deleted_count > 0 {
            emit!(QueueMessageDeleteSucceeded {
                count: deleted_count,
                cloud: CLOUD_PROVIDER,
            });
        }

        // Pace between polls: the batch is fully processed and its messages are
        // deleted, so nothing is in flight while we wait to honor the byte rate.
        self.state.rate_limiter.pace(self.byte_counter.take()).await;

        // Wait for next poll interval
        tokio::time::sleep(Duration::from_secs(self.state.poll_secs)).await;
    }

    /// Deletes a received batch without fetching any blobs (the table is disabled).
    async fn drain_messages(&mut self, gate: &ObjectStorageGate, messages: Vec<Message>) {
        if !self.state.delete_message {
            return;
        }
        let mut deleted_count: u64 = 0;
        for message in messages {
            match self.delete_message(&message).await {
                Ok(()) => deleted_count += 1,
                Err(err) => emit!(AzureBlobQueueDeleteFailed {
                    message_id: &message.message_id,
                    error: &err,
                }),
            }
        }
        if deleted_count > 0 {
            emit!(QueueMessageDeleteSucceeded {
                count: deleted_count,
                cloud: CLOUD_PROVIDER,
            });
            gate.record_drained(deleted_count as usize);
        }
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
        let decoded_message = STANDARD
            .decode(message_text)
            .map_err(|source| ProcessingError::InvalidBase64 { source })?;

        // Convert decoded bytes to UTF-8 string
        let decoded_str = String::from_utf8(decoded_message)
            .map_err(|source| ProcessingError::InvalidUtf8 { source })?;

        // Represents the time at which the message was inserted into the queue
        let queue_notification_create_timestamp = Utc
            .timestamp_opt(
                message.insertion_time.unix_timestamp(),
                message.insertion_time.nanosecond(),
            )
            .single();

        let message_id = &message.message_id;

        // Try to parse as a QueueEvent (DirectIngest is tried first due to deny_unknown_fields)
        let queue_event: QueueEvent = serde_json::from_str(&decoded_str)
            .map_err(|source| ProcessingError::InvalidJson { source })?;

        match queue_event {
            QueueEvent::DirectIngest(msg) => {
                self.handle_direct_ingest(msg, message_id, queue_notification_create_timestamp)
                    .await
            }
            QueueEvent::EventGrid(event) => {
                self.handle_event_grid_event(event, message_id, queue_notification_create_timestamp)
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
        // Resolve the storage account this message targets: its explicit `account` override,
        // else the source's configured account. The allowlist is enforced when the per-account
        // blob client is resolved during processing (State::blob_client_for).
        let target_account = msg
            .account
            .unwrap_or_else(|| self.state.storage_account_name.clone());

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
            account = %target_account,
            file_id = %msg.file_id,
            message_id = %message_id,
        );

        let processing_start = std::time::Instant::now();
        let result = self
            .process_blob_object(
                message_id,
                &target_account,
                &msg.container,
                &msg.blob,
                msg.log_type.as_deref(),
            )
            .await;

        // Fire ingestion callback if configured (non-blocking).
        if let Some(ref cb_client) = self.state.callback_client {
            let message_fields = std::collections::HashMap::from([
                ("file_id".to_string(), msg.file_id.clone()),
                ("container".to_string(), msg.container.clone()),
                ("blob".to_string(), msg.blob.clone()),
                ("account".to_string(), target_account),
            ]);
            cb_client.spawn_notify(&result, processing_start.elapsed(), message_fields);
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
        message_id: &str,
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
            ProcessingError::InvalidBlobSubject {
                subject: event.subject.clone(),
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

        // Event Grid notifications are always for the source's configured account.
        // They carry no log type, unlike direct-ingest messages.
        let account = self.state.storage_account_name.clone();
        self.process_blob_object(message_id, &account, container, blob, None)
            .await
    }

    /// Downloads a blob, decompresses, frames, deserializes, enriches, and sends
    /// events downstream. This is the shared processing core used by both Event Grid
    /// notifications and direct ingest messages.
    ///
    async fn process_blob_object(
        &mut self,
        message_id: &str,
        account: &str,
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

        // Resolve the blob client for the target account (allowlist enforced here), then
        // address the specific container and blob.
        let blob_client = self
            .state
            .blob_client_for(account)
            .ok_or_else(|| ProcessingError::DisallowedStorageAccount {
                account: account.to_owned(),
                container: container.clone(),
                blob: blob.clone(),
            })?
            .container_client(&container)
            .blob_client(&blob);

        let mut blob_stream = blob_client
            .get()
            .chunk_size(self.state.blob_chunk_size)
            .into_stream();

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

        let blob_response = response_opt.ok_or_else(|| ProcessingError::GetBlobEmptyResponse {
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
        // This is the length of the first page, not of the blob; see `BlobReadProgress`.
        let first_page_length = properties.content_length;
        let etag = properties.etag.to_string();

        let timestamp = last_modified;

        // Create batch for event processing
        let (batch, receiver) = BatchNotifier::maybe_new_with_receiver(self.acknowledgements);

        // Prepare blob content stream, counting the compressed bytes actually pulled so we can
        // later confirm the whole committed blob was read before tolerating a trailing EOF.
        let compressed_bytes = Arc::new(AtomicU64::new(0));
        let byte_counter = Arc::clone(&compressed_bytes);
        let progress = Arc::new(BlobReadProgress::default());
        let more_pages = blob_response.remaining_range.is_some();
        // A 200 (no Content-Range) is the whole blob, so its length is the total.
        let first_page_end = match blob_response.content_range.as_ref() {
            Some(azure_core_for_storage::request_options::Range::Range(r)) => r.end + 1,
            _ => first_page_length,
        };
        progress.record_page(first_page_end, !more_pages);
        let next_start = match blob_response.content_range.as_ref() {
            Some(azure_core_for_storage::request_options::Range::Range(r)) if more_pages => {
                Some(r.end + 1)
            }
            _ => None,
        };
        // `blob_response.data` is only the first `blob_chunk_size` page; chain the rest.
        let pages = chain_blob_pages(
            blob_client,
            self.state.blob_chunk_size,
            etag,
            blob_response.data,
            next_start,
            Arc::clone(&progress),
        );
        let body_stream = pages.map(move |result| {
            result
                .inspect(|chunk| {
                    byte_counter.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                })
                .map_err(std::io::Error::other)
        });

        // Decode blob content based on compression settings
        let blob_reader = super::blob_object_decoder(
            self.state.compression,
            &blob,
            content_encoding,
            content_type,
            Box::new(body_stream),
        )
        .await;

        // Optionally tolerate a truncated trailing gzip/zstd member: ingest the decodable prefix
        // and ack rather than redelivering an immutable, un-fully-decodable archive forever. Gated
        // by `tolerate_truncated_archives` (default off) because the dropped tail is data loss;
        // when off, the raw decoder is used and a truncated blob errors and redelivers as before.
        // See TolerateTrailingEof.
        let truncated_tail = Arc::new(AtomicBool::new(false));
        let blob_reader: Box<dyn AsyncRead + Send + Unpin> =
            if self.state.tolerate_truncated_archives {
                Box::new(TolerateTrailingEof::new(
                    blob_reader,
                    Arc::clone(&truncated_tail),
                ))
            } else {
                blob_reader
            };

        // Set up error tracking and metrics
        let mut read_error = None;
        let bytes_received = self.bytes_received.clone();
        let events_received = self.events_received.clone();
        // Count decoded bytes for this blob (for rate pacing and the large-object warning).
        let byte_counter = self.byte_counter.clone();
        let obj_start_bytes = byte_counter.get();

        // Create stream of decoded lines
        let lines: Box<dyn Stream<Item = Bytes> + Send + Unpin> = Box::new(
            FramedRead::new(blob_reader, self.state.decoder.framer.clone())
                .map(|res| {
                    res.inspect(|bytes| {
                        bytes_received.emit(ByteSize(bytes.len()));
                        byte_counter.add(bytes.len());
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

        // A framer error surfaced only at end-of-stream — `decode_eof` on the residual bytes —
        // after we tolerated a trailing archive truncation is the truncated tail's incomplete
        // final frame, not a real read failure: `length_delimited`/`octet_counting` framers
        // return `Err` on a partial trailing frame (newline/bytes framers instead emit the
        // residual as a final frame, so there is no error to clear). Left as an error it would
        // set `read_error` -> `ReadBlob`, so `truncated_tail` is set but `delivered` is not and
        // the whole committed blob was read, and neither reconcile branch below runs: the message
        // never acks and the immutable blob redelivers forever — the exact loop this feature
        // prevents. Drop that residual error so we take the normal ack path (and record the
        // tolerated drop). Gated on having read the whole committed blob, so a short transfer
        // still surfaces as a real error and redelivers rather than acking a partial download.
        let required_bytes = progress.required_bytes();
        if read_error.is_some()
            && tolerated_trailing_frame_error(
                truncated_tail.load(Ordering::Relaxed),
                required_bytes,
                compressed_bytes.load(Ordering::Relaxed),
            )
        {
            read_error = None;
        }

        // Observability-only: warn if this blob's decoded size is large enough to
        // risk the queue visibility timeout even at full speed. Never drops data.
        self.state.rate_limiter.note_object(
            byte_counter.get().saturating_sub(obj_start_bytes),
            &container,
            &blob,
        );

        // Handle any errors that occurred
        let mut delivered = false;
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
                // Acknowledgements disabled: best-effort send, treat as delivered.
                None => {
                    delivered = true;
                    Ok(())
                }
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

                            delivered = true;
                            Ok(())
                        }
                        // Counted here, not by the per-message arm: with `delete_failed_message`
                        // these become `Ok` (and are deleted), so they would otherwise go uncounted.
                        BatchStatus::Errored => {
                            let err = ProcessingError::BlobDeliveryErrored {
                                container: container.clone(),
                                blob: blob.clone(),
                            };
                            emit!(AzureBlobMessageProcessingError {
                                message_id,
                                error: &err,
                            });
                            if self.state.delete_failed_message {
                                Ok(())
                            } else {
                                Err(err)
                            }
                        }
                        BatchStatus::Rejected => {
                            let err = ProcessingError::BlobDeliveryRejected {
                                container: container.clone(),
                                blob: blob.clone(),
                            };
                            emit!(AzureBlobMessageProcessingError {
                                message_id,
                                error: &err,
                            });
                            if self.state.delete_failed_message {
                                Ok(())
                            } else {
                                Err(err)
                            }
                        }
                    }
                }
            }
        };

        // Reconcile with how much of the committed blob we actually read. A short read is a cut-short
        // or abandoned download, never a torn archive, so it must redeliver whether or not a
        // decoder noticed (an uncompressed blob or a page boundary EOF raises no error at all).
        let mut processing_result = processing_result;
        let pulled = compressed_bytes.load(Ordering::Relaxed);
        if processing_result.is_ok() && required_bytes > 0 && pulled < required_bytes {
            // The per-message arm counts and logs this via `AzureBlobMessageProcessingError`.
            processing_result = Err(ProcessingError::IncompleteBlobDownload {
                bytes_read: pulled,
                content_length: progress.final_page_reached().then_some(required_bytes),
                container: container.clone(),
                blob: blob.clone(),
            });
        } else if truncated_tail.load(Ordering::Relaxed) && delivered {
            // Whole committed blob read but its trailing member was truncated: we ingested the
            // decodable prefix and the batch was delivered. Record the tolerated data-drop.
            emit!(ObjectStorageBlobTruncated {
                cloud: CLOUD_PROVIDER,
                container: &container,
                blob: &blob,
            });
        }

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

/// Whether a framer error raised at end-of-stream should be tolerated as the truncated tail's
/// incomplete final frame rather than treated as a read failure.
///
/// Only meaningful once `tolerate_truncated_archives` turned a trailing decoder `UnexpectedEof`
/// into a clean EOF (`truncated_tail`). At that point the whole compressed body was consumed by
/// the decoder, so any framer error can only come from `decode_eof` on the leftover partial
/// frame — the truncated tail. It is tolerated only when at least `required_bytes` (the blob's
/// advertised size, or `u64::MAX` if the final page was never reached) were pulled; a short
/// transfer is a cut-short download and must still surface as an error so the message
/// redelivers instead of acking a partial read. `required_bytes == 0` (empty blob) is trivially
/// satisfied.
const fn tolerated_trailing_frame_error(
    truncated_tail: bool,
    required_bytes: u64,
    pulled: u64,
) -> bool {
    truncated_tail && pulled >= required_bytes
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
    use tokio::io::AsyncReadExt as _;

    /// End-to-end through the REAL gzip decoder (not a mock): a multi-member gzip whose final
    /// member is truncated must yield the fully-decoded prefix and set `tolerated`; a fully valid
    /// gzip must decode completely with `tolerated` unset. This guards the load-bearing assumption
    /// that async-compression reports mid-member truncation as `UnexpectedEof` — if a dependency
    /// bump changes that, this test fails instead of the tolerance silently no-opping.
    #[tokio::test]
    async fn real_truncated_gzip_yields_prefix_and_is_tolerated() {
        use std::io::Write as _;

        fn gz(bytes: &[u8]) -> Vec<u8> {
            let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            e.write_all(bytes).unwrap();
            e.finish().unwrap()
        }

        async fn decode(bytes: Vec<u8>) -> (String, bool) {
            let tolerated = Arc::new(AtomicBool::new(false));
            let inner = super::super::Compression::Gzip
                .build_decoder(futures::stream::iter(vec![Ok::<_, std::io::Error>(
                    Bytes::from(bytes),
                )]))
                .await;
            let mut reader = TolerateTrailingEof::new(inner, Arc::clone(&tolerated));
            let mut out = Vec::new();
            reader.read_to_end(&mut out).await.unwrap();
            (
                String::from_utf8(out).unwrap(),
                tolerated.load(Ordering::Relaxed),
            )
        }

        let m1 = gz(b"span1\nspan2\n");
        let m2 = gz(b"span3\nspan4\nspan5\n");

        // Full, valid two-member gzip: decodes completely, not tolerated.
        let mut full = m1.clone();
        full.extend_from_slice(&m2);
        let (out, tolerated) = decode(full).await;
        assert_eq!(out, "span1\nspan2\nspan3\nspan4\nspan5\n");
        assert!(!tolerated);

        // Second member truncated inside its DEFLATE payload: member 1 decodes (the prefix), then
        // the decoder hits a mid-member EOF -> tolerated.
        let mut truncated = m1.clone();
        truncated.extend_from_slice(&m2[..12]);
        let (out, tolerated) = decode(truncated).await;
        assert!(out.starts_with("span1\nspan2\n"), "got: {out:?}");
        assert!(out.len() < "span1\nspan2\nspan3\nspan4\nspan5\n".len());
        assert!(tolerated);
    }

    #[test]
    fn tolerated_trailing_frame_error_gates_on_whole_blob_read() {
        // No tolerated truncation: a framer error is a genuine read failure, never suppressed.
        assert!(!tolerated_trailing_frame_error(false, 100, 100));
        assert!(!tolerated_trailing_frame_error(false, 0, 0));
        // Trailing truncation tolerated AND the whole committed blob was read: the framer error is
        // the truncated tail's partial final frame -> tolerate so we ack the decodable prefix.
        assert!(tolerated_trailing_frame_error(true, 100, 100));
        assert!(tolerated_trailing_frame_error(true, 100, 120));
        // Empty blob (required_bytes == 0): trivially fully read.
        assert!(tolerated_trailing_frame_error(true, 0, 0));
        // Final page never reached (u64::MAX): never tolerated.
        assert!(!tolerated_trailing_frame_error(true, u64::MAX, 100));
        // Short transfer (cut-short download): keep the error so the message redelivers rather than
        // acking a partial read as if it were a truncated archive.
        assert!(!tolerated_trailing_frame_error(true, 100, 50));
    }

    /// Regression guard for the interaction between `tolerate_truncated_archives` and a framer
    /// whose `decode_eof` errors on a partial trailing frame (`length_delimited`/`octet_counting`).
    /// The tolerance turns the decoder's trailing `UnexpectedEof` into a clean EOF, but `FramedRead`
    /// then runs the framer's `decode_eof` on the residual bytes; for a length-delimited stream the
    /// truncated tail is an incomplete frame, so `decode_eof` returns `Err`. This reproduces that
    /// exact shape and asserts the decodable prefix frames still come through, a framer error is
    /// recorded, the truncation was tolerated, and `tolerated_trailing_frame_error` therefore
    /// clears that error (so `process_blob` acks instead of redelivering forever).
    #[tokio::test]
    async fn length_delimited_truncated_tail_frame_error_is_tolerated() {
        use std::io;
        use std::pin::Pin;
        use std::task::{Context, Poll};
        use tokio::io::ReadBuf;
        use vector_lib::codecs::LengthDelimitedDecoder;

        /// Yields `data` then a single `UnexpectedEof`, mimicking a truncated gzip decoder.
        struct MockRead {
            data: Vec<u8>,
            pos: usize,
            errored: bool,
        }

        impl AsyncRead for MockRead {
            fn poll_read(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                buf: &mut ReadBuf<'_>,
            ) -> Poll<io::Result<()>> {
                if self.pos < self.data.len() {
                    let n = std::cmp::min(buf.remaining(), self.data.len() - self.pos);
                    let start = self.pos;
                    buf.put_slice(&self.data[start..start + n]);
                    self.pos += n;
                    return Poll::Ready(Ok(()));
                }
                if !self.errored {
                    self.errored = true;
                    return Poll::Ready(Err(io::Error::new(io::ErrorKind::UnexpectedEof, "mock")));
                }
                Poll::Ready(Ok(()))
            }
        }

        // Two complete 4-byte-length-prefixed frames ("foo", "bar") followed by a frame whose
        // header declares 5 bytes but supplies only 2 — the truncated tail.
        let mut decoded = Vec::new();
        decoded.extend_from_slice(b"\x00\x00\x00\x03foo");
        decoded.extend_from_slice(b"\x00\x00\x00\x03bar");
        decoded.extend_from_slice(b"\x00\x00\x00\x05qu");

        let truncated_tail = Arc::new(AtomicBool::new(false));
        let reader = TolerateTrailingEof::new(
            MockRead {
                data: decoded,
                pos: 0,
                errored: false,
            },
            Arc::clone(&truncated_tail),
        );

        // Drive the framer exactly as `process_blob` does: capture the framer error, stop on it.
        let mut read_error = None;
        let mut frames = Vec::new();
        let mut framed = FramedRead::new(reader, LengthDelimitedDecoder::default());
        while let Some(res) = framed.next().await {
            match res {
                Ok(bytes) => frames.push(bytes),
                Err(err) => {
                    read_error = Some(err);
                    break;
                }
            }
        }

        // The decodable prefix is delivered, the truncation was tolerated, and the partial tail
        // surfaced as a framer error at EOF.
        assert_eq!(frames, vec![Bytes::from("foo"), Bytes::from("bar")]);
        assert!(truncated_tail.load(Ordering::Relaxed));
        assert!(read_error.is_some());

        // With the whole committed blob read, that residual error is tolerated (cleared), so the
        // message is acked rather than looping. A short transfer would keep it.
        assert!(tolerated_trailing_frame_error(
            truncated_tail.load(Ordering::Relaxed),
            10,
            10,
        ));
        assert!(!tolerated_trailing_frame_error(
            truncated_tail.load(Ordering::Relaxed),
            10,
            4,
        ));
    }

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
    fn incomplete_download_message_never_prints_unknown_size() {
        let err = |content_length| ProcessingError::IncompleteBlobDownload {
            bytes_read: 3,
            content_length,
            container: "c".into(),
            blob: "b".into(),
        };
        assert!(
            err(Some(10))
                .to_string()
                .contains("read 3 bytes, expected 10")
        );
        let unknown = err(None).to_string();
        assert!(
            unknown.contains("stopped before the final page"),
            "{unknown}"
        );
        assert!(!unknown.contains(&u64::MAX.to_string()));
    }

    #[test]
    fn test_blob_read_chunk_size_config_parsing() {
        let config: Config = toml::from_str(r#"queue_name = "q""#).unwrap();
        assert_eq!(config.blob_read_chunk_size_bytes.get(), 16 * 1024 * 1024);

        let config: Config = toml::from_str(
            r#"
            queue_name = "q"
            blob_read_chunk_size_bytes = 4096
            "#,
        )
        .unwrap();
        assert_eq!(config.blob_read_chunk_size_bytes.get(), 4096);

        assert!(
            toml::from_str::<Config>(
                r#"
                queue_name = "q"
                blob_read_chunk_size_bytes = 0
                "#
            )
            .is_err(),
            "a zero chunk size must be rejected at config load"
        );
    }

    #[test]
    fn test_tolerate_truncated_archives_config_parsing() {
        // Default: off — tolerating truncation drops the torn tail, so it must be opt-in.
        let config: Config = toml::from_str(
            r#"
            queue_name = "my-storage-queue"
            "#,
        )
        .unwrap();
        assert!(!config.tolerate_truncated_archives);

        // Explicitly enabled
        let config: Config = toml::from_str(
            r#"
            queue_name = "my-storage-queue"
            tolerate_truncated_archives = true
            "#,
        )
        .unwrap();
        assert!(config.tolerate_truncated_archives);
    }

    #[test]
    fn test_depth_poll_secs_config_parsing() {
        // Default: sample every 60 seconds.
        let config: Config = toml::from_str(
            r#"
            queue_name = "my-storage-queue"
            "#,
        )
        .unwrap();
        assert_eq!(config.depth_poll_secs, NonZeroU64::new(60).unwrap());
        assert_eq!(
            Config::default().depth_poll_secs,
            NonZeroU64::new(60).unwrap()
        );

        let config: Config = toml::from_str(
            r#"
            queue_name = "my-storage-queue"
            depth_poll_secs = 300
            "#,
        )
        .unwrap();
        assert_eq!(config.depth_poll_secs, NonZeroU64::new(300).unwrap());

        // Zero would panic the sampler's interval timer, so it must be rejected at parse time.
        let result: Result<Config, toml::de::Error> = toml::from_str(
            r#"
            queue_name = "my-storage-queue"
            depth_poll_secs = 0
            "#,
        );
        assert!(result.is_err());
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

    /// Some Azure Storage stamps (e.g. syd29prdstr04a, behind dblogprodausteast) omit `Server`
    /// on successful responses. The vendored azure_storage must still parse the common headers,
    /// or every queue receive/delete/metadata call fails with "header not found server".
    #[test]
    fn common_storage_headers_parse_without_server() {
        use azure_core_for_storage::headers::{DATE, Headers, REQUEST_ID, SERVER, VERSION};
        use azure_storage::headers::CommonStorageResponseHeaders;

        let mut headers = Headers::new();
        headers.insert(REQUEST_ID, "83dc881c-4003-007e-6527-5d5804000000");
        headers.insert(VERSION, "2019-12-12");
        headers.insert(DATE, "Fri, 16 Oct 2026 04:30:00 GMT");
        let parsed = CommonStorageResponseHeaders::try_from(&headers).unwrap();
        assert_eq!(parsed.server, "");

        headers.insert(SERVER, "Windows-Azure-Queue/1.0 Microsoft-HTTPAPI/2.0");
        let parsed = CommonStorageResponseHeaders::try_from(&headers).unwrap();
        assert_eq!(
            parsed.server,
            "Windows-Azure-Queue/1.0 Microsoft-HTTPAPI/2.0"
        );
    }
}

#[cfg(test)]
mod error_classification_tests {
    use super::*;
    use azure_core_for_storage::error::ErrorKind;
    use vector_lib::internal_event::{error_stage, error_type};

    fn io_err() -> azure_core_for_storage::Error {
        ErrorKind::Io.into_error()
    }

    fn http_err(status: StatusCode) -> azure_core_for_storage::Error {
        ErrorKind::http_response(status, None).into_error()
    }

    /// Every `GetBlob` failure kind maps to a bounded (error_type, error_code) pair.
    #[test]
    fn get_blob_failure_classifies_by_kind_and_status() {
        let cases = [
            (
                http_err(StatusCode::Forbidden),
                error_type::REQUEST_FAILED,
                "get_blob_forbidden",
            ),
            (
                http_err(StatusCode::TooManyRequests),
                error_type::REQUEST_FAILED,
                "get_blob_throttled",
            ),
            (
                http_err(StatusCode::ServiceUnavailable),
                error_type::REQUEST_FAILED,
                "get_blob_throttled",
            ),
            (
                http_err(StatusCode::InternalServerError),
                error_type::REQUEST_FAILED,
                "get_blob_server_error",
            ),
            (
                http_err(StatusCode::BadGateway),
                error_type::REQUEST_FAILED,
                "get_blob_server_error",
            ),
            (
                http_err(StatusCode::BadRequest),
                error_type::REQUEST_FAILED,
                "get_blob_failed",
            ),
            (
                http_err(StatusCode::Conflict),
                error_type::REQUEST_FAILED,
                "get_blob_failed",
            ),
            (
                io_err(),
                error_type::CONNECTION_FAILED,
                "get_blob_io_failed",
            ),
            (
                ErrorKind::Credential.into_error(),
                error_type::CONFIGURATION_FAILED,
                "get_blob_credential_failed",
            ),
            (
                ErrorKind::Other.into_error(),
                error_type::REQUEST_FAILED,
                "get_blob_failed",
            ),
            (
                ErrorKind::DataConversion.into_error(),
                error_type::REQUEST_FAILED,
                "get_blob_failed",
            ),
        ];
        for (err, expected_type, expected_code) in cases {
            let (ty, code) = get_blob_failure(&err);
            assert_eq!((ty, code), (expected_type, expected_code), "for {err:?}");

            let processing = ProcessingError::GetBlob {
                source: err,
                container: "c".into(),
                blob: "b".into(),
            };
            assert_eq!(processing.error_type(), expected_type);
            assert_eq!(processing.error_code(), expected_code);
            assert_eq!(processing.stage(), error_stage::RECEIVING);
        }
    }

    /// Every `ProcessingError` variant is classified, with the expected labels, permanence, and
    /// whether the per-message event must skip it. The match in the methods is exhaustive, so a
    /// new variant also fails to compile until it is added here.
    #[test]
    fn processing_error_labels_cover_every_variant() {
        let base64_err = STANDARD.decode("!!!").unwrap_err();
        let utf8_err = String::from_utf8(vec![0xff]).unwrap_err();
        let json_err = serde_json::from_str::<u8>("x").unwrap_err();
        let (c, b) = ("c".to_string(), "k.log".to_string());

        // (error, error_type, error_code, stage, permanent, reported_elsewhere)
        let cases: Vec<(ProcessingError, &str, &str, &str, bool, bool)> = vec![
            (
                ProcessingError::InvalidBase64 { source: base64_err },
                error_type::PARSER_FAILED,
                "invalid_base64",
                error_stage::PROCESSING,
                false,
                false,
            ),
            (
                ProcessingError::InvalidUtf8 { source: utf8_err },
                error_type::PARSER_FAILED,
                "invalid_utf8",
                error_stage::PROCESSING,
                false,
                false,
            ),
            (
                ProcessingError::InvalidJson { source: json_err },
                error_type::PARSER_FAILED,
                "invalid_json",
                error_stage::PROCESSING,
                false,
                false,
            ),
            (
                ProcessingError::InvalidBlobSubject {
                    subject: "/bad".into(),
                },
                error_type::PARSER_FAILED,
                "invalid_blob_subject",
                error_stage::PROCESSING,
                false,
                false,
            ),
            (
                ProcessingError::UnsupportedEventGridVersion {
                    version: semver::Version::new(2, 0, 0),
                },
                error_type::PARSER_FAILED,
                "unsupported_event_grid_version",
                error_stage::PROCESSING,
                false,
                false,
            ),
            (
                ProcessingError::EmptyFileId {
                    container: c.clone(),
                    blob: b.clone(),
                },
                error_type::CONDITION_FAILED,
                "empty_file_id",
                error_stage::PROCESSING,
                true,
                false,
            ),
            (
                ProcessingError::WrongStorageAccount {
                    account: "a".into(),
                    container: c.clone(),
                    blob: b.clone(),
                },
                error_type::CONDITION_FAILED,
                "wrong_storage_account",
                error_stage::PROCESSING,
                true,
                false,
            ),
            (
                ProcessingError::DisallowedStorageAccount {
                    account: "a".into(),
                    container: c.clone(),
                    blob: b.clone(),
                },
                error_type::CONDITION_FAILED,
                "disallowed_storage_account",
                error_stage::PROCESSING,
                true,
                false,
            ),
            (
                ProcessingError::BlobNotFound {
                    source: http_err(StatusCode::NotFound),
                    container: c.clone(),
                    blob: b.clone(),
                },
                error_type::REQUEST_FAILED,
                "blob_not_found",
                error_stage::RECEIVING,
                true,
                false,
            ),
            (
                ProcessingError::GetBlobEmptyResponse {
                    container: c.clone(),
                    blob: b.clone(),
                },
                error_type::REQUEST_FAILED,
                "get_blob_empty_response",
                error_stage::RECEIVING,
                false,
                false,
            ),
            (
                ProcessingError::ReadBlob {
                    source: Box::new(std::io::Error::other("boom")),
                    container: c.clone(),
                    blob: b.clone(),
                },
                error_type::READER_FAILED,
                "read_blob_failed",
                error_stage::RECEIVING,
                false,
                false,
            ),
            (
                ProcessingError::IncompleteBlobDownload {
                    bytes_read: 3,
                    content_length: Some(10),
                    container: c.clone(),
                    blob: b.clone(),
                },
                error_type::READER_FAILED,
                "incomplete_blob_download",
                error_stage::RECEIVING,
                false,
                false,
            ),
            (
                ProcessingError::PipelineSend {
                    source: SendError::Closed,
                    container: c.clone(),
                    blob: b.clone(),
                },
                error_type::WRITER_FAILED,
                "pipeline_send_failed",
                error_stage::SENDING,
                false,
                true,
            ),
            (
                ProcessingError::BlobDeliveryErrored {
                    container: c.clone(),
                    blob: b.clone(),
                },
                error_type::ACKNOWLEDGMENT_FAILED,
                "delivery_errored",
                error_stage::SENDING,
                false,
                true,
            ),
            (
                ProcessingError::BlobDeliveryRejected {
                    container: c.clone(),
                    blob: b.clone(),
                },
                error_type::ACKNOWLEDGMENT_FAILED,
                "delivery_rejected",
                error_stage::SENDING,
                false,
                true,
            ),
        ];

        for (err, ty, code, stage, permanent, elsewhere) in &cases {
            assert_eq!(err.error_type(), *ty, "error_type for {err}");
            assert_eq!(err.error_code(), *code, "error_code for {err}");
            assert_eq!(err.stage(), *stage, "stage for {err}");
            assert_eq!(err.is_permanent(), *permanent, "is_permanent for {err}");
            assert_eq!(
                err.reported_elsewhere(),
                *elsewhere,
                "reported_elsewhere for {err}"
            );
        }
    }
}

#[cfg(test)]
mod drain_tests {
    use std::sync::Arc;

    use azure_storage::CloudLocation;
    use azure_storage_queues::QueueServiceClientBuilder;
    use base64::Engine as _;
    use std::sync::atomic::Ordering;
    use vector_lib::codecs::{NewlineDelimitedDecoderConfig, decoding::FramingConfig};
    use vector_lib::event::EventStatus;

    use crate::test_util::trace_init;
    use vector_lib::config::LogNamespace;
    use vrl::value::Value;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path_regex},
    };

    use super::*;
    use crate::{
        codecs::DecodingConfig,
        serde::default_decoding,
        sources::util::object_storage_drain_gate::test_util::{
            ENABLED_LIST_KEY, PAUSED_LIST_KEY, gate, object_gate, pause_gate, pause_gate_sequence,
            with_drained_count, with_paused_count,
        },
        sources::util::object_storage_drain_gate::{DrainGate, PauseGate},
        sources::util::object_storage_rate_limit::ObjectStorageRateLimitConfig,
    };

    fn storage_headers(template: ResponseTemplate) -> ResponseTemplate {
        template
            .insert_header("x-ms-request-id", "abcdef01-2345-6789-abcd-ef0123456789")
            .insert_header("x-ms-version", "2021-08-06")
            .insert_header("date", "Fri, 17 Oct 2026 00:00:00 GMT")
    }

    /// The base64 body of a direct-ingest message for blob `c/k.log`.
    fn ingest_message_text() -> String {
        base64::engine::general_purpose::STANDARD
            .encode(r#"{"kind": "INGEST", "container": "c", "blob": "k.log", "file_id": "f1"}"#)
    }

    /// The queue returns one INGEST message; deletes succeed; any blob GET 404s.
    async fn mock_queue_and_blob() -> MockServer {
        mock_queue_and_blob_with(204).await
    }

    /// Like `mock_queue_and_blob`, with `delete_status` as the queue-message DELETE response
    /// (404 is not retried by the SDK, so it fails the delete immediately).
    async fn mock_queue_and_blob_with(delete_status: u16) -> MockServer {
        mock_queue(
            &ingest_message_text(),
            delete_status,
            ResponseTemplate::new(404),
        )
        .await
    }

    /// A queue returning one message with body `message_text`, DELETE answering `delete_status`,
    /// and blob GETs answering `blob_get`.
    async fn mock_queue(
        message_text: &str,
        delete_status: u16,
        blob_get: ResponseTemplate,
    ) -> MockServer {
        let server = MockServer::start().await;
        let text = message_text;
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?><QueueMessagesList><QueueMessage>\
             <MessageId>m1</MessageId>\
             <InsertionTime>Fri, 17 Oct 2026 00:00:00 GMT</InsertionTime>\
             <ExpirationTime>Fri, 24 Oct 2026 00:00:00 GMT</ExpirationTime>\
             <PopReceipt>pop-1</PopReceipt>\
             <TimeNextVisible>Fri, 17 Oct 2026 00:10:00 GMT</TimeNextVisible>\
             <DequeueCount>1</DequeueCount>\
             <MessageText>{text}</MessageText>\
             </QueueMessage></QueueMessagesList>"
        );
        Mock::given(method("GET"))
            .and(path_regex("^/q/messages$"))
            .respond_with(storage_headers(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "application/xml")
                    .set_body_string(body),
            ))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path_regex("^/q/messages/m1$"))
            .respond_with(storage_headers(ResponseTemplate::new(delete_status)))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path_regex("^/c/"))
            .respond_with(storage_headers(blob_get))
            .mount(&server)
            .await;
        server
    }

    async fn run_once_with(server: &MockServer, drain_gate: Option<DrainGate>) {
        run_once_configured(server, drain_gate, None, true).await;
    }

    async fn run_once_configured(
        server: &MockServer,
        drain_gate: Option<DrainGate>,
        pause_gate: Option<PauseGate>,
        delete_message: bool,
    ) {
        run_once_full(server, drain_gate, pause_gate, delete_message, false, None).await;
    }

    /// Runs one poll. `finalize` (when `Some`) enables acknowledgements and finalizes every event
    /// with that status, as a sink would; `None` disables acknowledgements.
    async fn run_once_full(
        server: &MockServer,
        drain_gate: Option<DrainGate>,
        pause_gate: Option<PauseGate>,
        delete_message: bool,
        delete_failed_message: bool,
        finalize: Option<EventStatus>,
    ) {
        let location = || CloudLocation::Custom {
            account: "devstoreaccount1".to_string(),
            uri: server.uri(),
        };
        let creds = StorageCredentials::anonymous();
        let blob_client =
            ClientBuilder::with_location(location(), creds.clone()).blob_service_client();
        let queue_client = QueueServiceClientBuilder::with_location(location(), creds).build();
        let decoder = DecodingConfig::new(
            FramingConfig::NewlineDelimited(NewlineDelimitedDecoderConfig::default()),
            default_decoding(),
            LogNamespace::Legacy,
        )
        .build()
        .expect("decoder must build");
        let config = Config {
            queue_name: "q".to_string(),
            poll_secs: 0,
            process_custom_message: true,
            delete_message,
            delete_failed_message,
            ..Default::default()
        };
        let ingestor = Ingestor::new(
            blob_client,
            queue_client,
            config,
            super::super::Compression::Auto,
            None,
            decoder,
            None,
            None,
            Vec::new(),
            ObjectStorageRateLimiter::new(&ObjectStorageRateLimitConfig::default()),
        )
        .await
        .expect("ingestor must build")
        .with_gate(object_gate(drain_gate, pause_gate));
        let acknowledgements = finalize.is_some();
        // The sink side: draining the stream is what finalizes each batch.
        let out = if let Some(status) = finalize {
            let (out, rx) = SourceSender::new_test_finalize(status);
            tokio::spawn(drain_stream(rx));
            out
        } else {
            let (out, rx) = SourceSender::new_test();
            tokio::spawn(drain_stream(rx));
            out
        };
        let mut process = IngestorProcess::new(
            Arc::clone(&ingestor.state),
            out,
            ShutdownSignal::noop(),
            LogNamespace::Legacy,
            acknowledgements,
        );
        process.run_once().await;
    }

    async fn drain_stream(mut rx: impl Stream<Item = Event> + Unpin) {
        while rx.next().await.is_some() {}
    }

    /// Current value of `component_errors_total` for the exact label set, or 0 if unseen.
    fn component_errors(error_type: &str, error_code: &str, stage: &str) -> f64 {
        use vector_lib::event::MetricValue;
        use vector_lib::metrics::Controller;
        Controller::get()
            .unwrap()
            .capture_metrics()
            .iter()
            .filter(|m| m.name() == "component_errors_total")
            .find(|m| {
                let tags = m.tags().expect("component_errors_total has tags");
                tags.get("error_code") == Some(error_code)
                    && tags.get("error_type") == Some(error_type)
                    && tags.get("stage") == Some(stage)
            })
            .map(|m| match m.value() {
                MetricValue::Counter { value } => *value,
                other => panic!("expected a counter, got {other:?}"),
            })
            .unwrap_or(0.0)
    }

    /// (blob GETs, queue-message DELETEs)
    async fn requests(server: &MockServer) -> (usize, usize) {
        let received = server.received_requests().await.expect("recording enabled");
        let blob_gets = received
            .iter()
            .filter(|r| r.method.as_str() == "GET" && r.url.path().starts_with("/c/"))
            .count();
        let deletes = received
            .iter()
            .filter(|r| {
                r.method.as_str() == "DELETE" && r.url.query().unwrap_or_default().contains("pop-1")
            })
            .count();
        (blob_gets, deletes)
    }

    fn disabled_gate() -> DrainGate {
        gate("span", false, &[(ENABLED_LIST_KEY, Value::from("qpl"))])
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn disabled_table_deletes_without_fetching() {
        let server = mock_queue_and_blob().await;
        let ((), drained) =
            with_drained_count("span", run_once_with(&server, Some(disabled_gate())));
        assert_eq!(requests(&server).await, (0, 1));
        assert_eq!(drained, 1, "the deleted message is counted as drained");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn disabled_table_without_delete_message_does_not_delete() {
        let server = mock_queue_and_blob().await;
        let ((), drained) = with_drained_count(
            "span",
            run_once_configured(&server, Some(disabled_gate()), None, false),
        );
        assert_eq!(
            requests(&server).await,
            (0, 0),
            "delete_message = false leaves the message queued"
        );
        assert_eq!(drained, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn failed_delete_is_not_counted_as_drained() {
        let server = mock_queue_and_blob_with(404).await;
        let ((), drained) =
            with_drained_count("span", run_once_with(&server, Some(disabled_gate())));
        assert_eq!(
            requests(&server).await,
            (0, 1),
            "the delete is still attempted"
        );
        assert_eq!(
            drained, 0,
            "a message the queue failed to delete is not drained"
        );
    }

    #[tokio::test]
    async fn enabled_table_fetches() {
        let server = mock_queue_and_blob().await;
        let gate = gate("span", false, &[(ENABLED_LIST_KEY, Value::from("span"))]);
        run_once_with(&server, Some(gate)).await;
        assert!(
            requests(&server).await.0 > 0,
            "an enabled table fetches its blob"
        );
    }

    #[tokio::test]
    async fn unknown_flags_fetch() {
        let server = mock_queue_and_blob().await;
        run_once_with(&server, Some(gate("span", false, &[]))).await;
        assert!(
            requests(&server).await.0 > 0,
            "unreadable flags must process normally"
        );
    }

    #[tokio::test]
    async fn no_gate_fetches() {
        let server = mock_queue_and_blob().await;
        run_once_with(&server, None).await;
        assert!(requests(&server).await.0 > 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn paused_table_leaves_queue_untouched() {
        let server = mock_queue_and_blob().await;
        // Pause wins over drain: the table is also disabled, yet nothing is received or deleted.
        let paused = pause_gate("span", &[(PAUSED_LIST_KEY, Value::from("span"))]);
        let ((), polls) = with_paused_count(
            "span",
            run_once_configured(&server, Some(disabled_gate()), Some(paused), true),
        );
        let received = server.received_requests().await.expect("recording enabled");
        assert!(
            received.is_empty(),
            "a paused source makes no queue or object requests"
        );
        assert_eq!(polls, 1, "the skipped poll is counted");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pause_set_during_receive_leaves_batch_on_queue() {
        let server = mock_queue_and_blob().await;
        // Unpaused when the poll starts, paused by the time the batch arrives. The table is
        // also disabled, which must not drain the in-flight batch.
        let paused = pause_gate_sequence(
            "span",
            &[(PAUSED_LIST_KEY, vec![Value::from(""), Value::from("span")])],
        );
        let ((), polls) = with_paused_count(
            "span",
            run_once_configured(&server, Some(disabled_gate()), Some(paused), true),
        );
        assert!(
            !server
                .received_requests()
                .await
                .expect("recording enabled")
                .is_empty(),
            "the poll received a batch"
        );
        let (gets, deletes) = requests(&server).await;
        assert_eq!(gets, 0, "a paused batch is not fetched");
        assert_eq!(deletes, 0, "a paused batch is not deleted");
        assert_eq!(polls, 0, "the poll itself was not skipped");
    }

    /// A blob GET that succeeds with one JSON log line (`{"a":"b"}` plus newline).
    fn blob_ok() -> ResponseTemplate {
        ResponseTemplate::new(200)
            .insert_header("content-type", "application/json")
            .insert_header("content-length", "10")
            .insert_header("last-modified", "Fri, 17 Oct 2026 00:00:00 GMT")
            .insert_header("etag", "\"0x1\"")
            .insert_header("x-ms-blob-type", "BlockBlob")
            .insert_header("x-ms-server-encrypted", "true")
            .set_body_string("{\"a\":\"b\"}\n")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn invalid_base64_message_is_counted_and_not_deleted() {
        trace_init();
        let server = mock_queue("!!!", 204, ResponseTemplate::new(404)).await;
        let before = component_errors("parser_failed", "invalid_base64", "processing");
        run_once_with(&server, None).await;
        let after = component_errors("parser_failed", "invalid_base64", "processing");
        assert!(
            after >= before + 1.0,
            "invalid base64 must increment component_errors_total ({before} -> {after})"
        );
        assert_eq!(
            requests(&server).await,
            (0, 0),
            "parse failures keep their existing behavior: no blob fetch and no delete"
        );
    }

    /// A batch the sink errors or rejects is counted exactly once. With `delete_failed_message`
    /// the message is still acknowledged and deleted, as before; without it the error propagates
    /// and the message stays queued. Both paths keep their behavior, and neither double-counts.
    #[tokio::test(flavor = "multi_thread")]
    async fn failed_delivery_is_counted_once_whether_or_not_failed_message_is_deleted() {
        trace_init();
        let cases = [
            (EventStatus::Errored, "delivery_errored"),
            (EventStatus::Rejected, "delivery_rejected"),
        ];
        for (status, code) in cases {
            // delete_failed_message = true: Ok, so the message is deleted.
            let server = mock_queue(&ingest_message_text(), 204, blob_ok()).await;
            let before = component_errors("acknowledgment_failed", code, "sending");
            run_once_full(&server, None, None, true, true, Some(status)).await;
            let after = component_errors("acknowledgment_failed", code, "sending");
            assert_eq!(
                after - before,
                1.0,
                "{code} must be counted exactly once when delete_failed_message = true"
            );
            assert_eq!(
                requests(&server).await,
                (1, 1),
                "{code}: the blob is fetched and the message deleted when delete_failed_message = true"
            );

            // delete_failed_message = false: the error propagates and the message stays queued.
            let server = mock_queue(&ingest_message_text(), 204, blob_ok()).await;
            let before = component_errors("acknowledgment_failed", code, "sending");
            run_once_full(&server, None, None, true, false, Some(status)).await;
            let after = component_errors("acknowledgment_failed", code, "sending");
            assert_eq!(
                after - before,
                1.0,
                "{code} must be counted exactly once when delete_failed_message = false"
            );
            assert_eq!(
                requests(&server).await,
                (1, 0),
                "{code}: the message is left on the queue when delete_failed_message = false"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unpaused_table_processes_normally() {
        let server = mock_queue_and_blob().await;
        let unpaused = pause_gate("span", &[(PAUSED_LIST_KEY, Value::from("qpl"))]);
        let ((), polls) = with_paused_count(
            "span",
            run_once_configured(&server, None, Some(unpaused), true),
        );
        assert!(
            requests(&server).await.0 > 0,
            "an unpaused table fetches its object"
        );
        assert_eq!(polls, 0);
    }

    /// Serves `body` for `c/k.log` as ranged GETs, like Azure Blob Storage: every GET is a 206
    /// with `Content-Range`/`ETag`, `If-Match` is enforced against the current ETag.
    struct RangedBlob {
        body: Vec<u8>,
        /// Withhold this many trailing bytes of the final page while still advertising them in
        /// `Content-Range`, simulating a connection cut mid-body.
        withhold_tail: usize,
        /// Switch to a new ETag after the first request, simulating an overwrite mid-read.
        overwrite_after_first: bool,
        requests: std::sync::atomic::AtomicUsize,
    }

    impl wiremock::Respond for RangedBlob {
        fn respond(&self, request: &wiremock::Request) -> ResponseTemplate {
            let nth = self.requests.fetch_add(1, Ordering::SeqCst);
            let etag = if self.overwrite_after_first && nth > 0 {
                "\"0x2\""
            } else {
                "\"0x1\""
            };
            if let Some(want) = request.headers.get("if-match")
                && want.to_str().unwrap() != etag
            {
                return storage_headers(ResponseTemplate::new(412));
            }
            let total = self.body.len();
            let range = request
                .headers
                .get("x-ms-range")
                .map(|v| v.to_str().unwrap().to_owned())
                .unwrap_or_default();
            let (start, end) = range
                .strip_prefix("bytes=")
                .and_then(|r| r.split_once('-'))
                .map(|(a, b)| {
                    (
                        a.parse::<usize>().unwrap(),
                        b.parse::<usize>().map_or(total - 1, |b| b.min(total - 1)),
                    )
                })
                .unwrap_or((0, total - 1));
            let served_end = if end + 1 == total {
                (end + 1).saturating_sub(self.withhold_tail).max(start)
            } else {
                end + 1
            };
            storage_headers(ResponseTemplate::new(206))
                .insert_header("content-type", "application/octet-stream")
                .insert_header("content-range", format!("bytes {start}-{end}/{total}"))
                .insert_header("etag", etag)
                .insert_header("last-modified", "Fri, 14 Aug 2026 12:00:00 GMT")
                .insert_header("x-ms-creation-time", "Fri, 14 Aug 2026 11:00:00 GMT")
                .insert_header("x-ms-blob-type", "BlockBlob")
                .insert_header("x-ms-server-encrypted", "true")
                .set_body_bytes(self.body[start..served_end].to_vec())
        }
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        use std::io::Write as _;
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        e.write_all(bytes).unwrap();
        e.finish().unwrap()
    }

    /// `rows` newline-delimited lines of pseudo-random hex, so gzip can't shrink them away.
    fn random_rows(rows: usize) -> String {
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        (0..rows)
            .map(|i| {
                let mut line = format!("row{i}-");
                for _ in 0..4 {
                    x = x
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    line.push_str(&format!("{:016x}", x));
                }
                line + "\n"
            })
            .collect()
    }

    /// Processes the one queued `c/k.log` blob with a tiny page size; returns (events decoded,
    /// blob GETs, queue-message DELETEs).
    async fn run_ranged_blob(
        blob: RangedBlob,
        compression: super::super::Compression,
        chunk_size: u64,
        tolerate_truncated_archives: bool,
    ) -> (usize, usize, usize) {
        use futures::StreamExt as _;

        let server = mock_queue_and_blob().await;
        Mock::given(method("GET"))
            .and(path_regex("^/c/k.log$"))
            .respond_with(blob)
            // Outranks the catch-all 404 blob route mounted by the queue mock.
            .with_priority(1)
            .mount(&server)
            .await;
        let location = || CloudLocation::Custom {
            account: "devstoreaccount1".to_string(),
            uri: server.uri(),
        };
        let creds = StorageCredentials::anonymous();
        let blob_client =
            ClientBuilder::with_location(location(), creds.clone()).blob_service_client();
        let queue_client = QueueServiceClientBuilder::with_location(location(), creds).build();
        let decoder = DecodingConfig::new(
            FramingConfig::NewlineDelimited(NewlineDelimitedDecoderConfig::default()),
            default_decoding(),
            LogNamespace::Legacy,
        )
        .build()
        .expect("decoder must build");
        let config = Config {
            queue_name: "q".to_string(),
            poll_secs: 0,
            process_custom_message: true,
            delete_message: true,
            tolerate_truncated_archives,
            blob_read_chunk_size_bytes: NonZeroU64::new(chunk_size).unwrap(),
            ..Default::default()
        };
        let ingestor = Ingestor::new(
            blob_client,
            queue_client,
            config,
            compression,
            None,
            decoder,
            None,
            None,
            Vec::new(),
            ObjectStorageRateLimiter::new(&ObjectStorageRateLimitConfig::default()),
        )
        .await
        .expect("ingestor must build");
        let (out, rx) = SourceSender::new_test();
        let collected = tokio::spawn(async move { rx.collect::<Vec<_>>().await.len() });
        let mut process = IngestorProcess::new(
            Arc::clone(&ingestor.state),
            out,
            ShutdownSignal::noop(),
            LogNamespace::Legacy,
            false,
        );
        process.run_once().await;
        drop(process);
        let events = collected.await.unwrap();
        let (gets, deletes) = requests(&server).await;
        (events, gets, deletes)
    }

    fn ranged(body: Vec<u8>) -> RangedBlob {
        RangedBlob {
            body,
            withhold_tail: 0,
            overwrite_after_first: false,
            requests: Default::default(),
        }
    }

    /// Regression: only the first page of a blob larger than the chunk size used to be read, and
    /// the rest was silently dropped.
    #[tokio::test(flavor = "multi_thread")]
    async fn multi_page_gzip_blob_decodes_every_row() {
        let rows = 300;
        let body = gzip(random_rows(rows).as_bytes());
        assert!(body.len() > 4 * 1000, "fixture must span several pages");
        let (events, gets, deletes) =
            run_ranged_blob(ranged(body), super::super::Compression::Gzip, 1000, false).await;
        assert_eq!(events, rows);
        assert!(gets > 4, "expected several page requests, got {gets}");
        assert_eq!(deletes, 1, "a fully read blob is acked");
    }

    /// A short read of a plain (undecoded) blob raises no decoder error at all; it must still
    /// redeliver rather than ack.
    #[tokio::test(flavor = "multi_thread")]
    async fn short_uncompressed_blob_is_not_acked() {
        let mut blob = ranged(random_rows(300).into_bytes());
        blob.withhold_tail = 100;
        let (_, _, deletes) =
            run_ranged_blob(blob, super::super::Compression::None, 1000, false).await;
        assert_eq!(deletes, 0, "short transfer must redeliver");
    }

    /// Even with truncation tolerance on, a page sequence that comes up short of the advertised
    /// size is a short transfer (redeliver), not a torn archive (ack).
    #[tokio::test(flavor = "multi_thread")]
    async fn short_gzip_blob_is_not_acked_even_when_tolerating_truncation() {
        let mut blob = ranged(gzip(random_rows(300).as_bytes()));
        blob.withhold_tail = 20;
        let (_, _, deletes) =
            run_ranged_blob(blob, super::super::Compression::Gzip, 1000, true).await;
        assert_eq!(deletes, 0, "short transfer must redeliver");
    }

    /// A committed blob whose final gzip member is genuinely torn (full advertised length read)
    /// keeps the tolerate behaviour: ingest the decodable prefix and ack; without the opt-in it
    /// redelivers.
    #[tokio::test(flavor = "multi_thread")]
    async fn torn_final_member_with_full_length_follows_tolerate_setting() {
        let first = random_rows(40);
        let second = gzip(random_rows(40).as_bytes());
        let mut body = gzip(first.as_bytes());
        body.extend_from_slice(&second[..second.len() / 2]);

        let (events, _, deletes) = run_ranged_blob(
            ranged(body.clone()),
            super::super::Compression::Gzip,
            1000,
            true,
        )
        .await;
        assert!(events >= 40, "decodable prefix is ingested, got {events}");
        assert_eq!(deletes, 1, "tolerated: acked");

        let (_, _, deletes) =
            run_ranged_blob(ranged(body), super::super::Compression::Gzip, 1000, false).await;
        assert_eq!(deletes, 0, "not tolerated: redelivered");
    }

    /// Pages after the first carry `If-Match`, so a blob overwritten mid-read fails (412) rather
    /// than mixing two versions.
    #[tokio::test(flavor = "multi_thread")]
    async fn blob_overwritten_mid_read_is_not_acked() {
        let mut blob = ranged(gzip(random_rows(300).as_bytes()));
        blob.overwrite_after_first = true;
        let (_, _, deletes) =
            run_ranged_blob(blob, super::super::Compression::Gzip, 1000, false).await;
        assert_eq!(deletes, 0);
    }
}
