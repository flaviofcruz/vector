use metrics::{counter, gauge, histogram};
use tracing::{debug, info, trace, warn};
use vector_lib::emit;
use vector_lib::internal_event::{InternalEvent, NamedInternalEvent};

#[derive(Debug)]
pub struct ObjectStorageAdaptiveConcurrency {
    pub limit: usize,
    pub active_objects: usize,
    pub memory_pressure: bool,
}

impl NamedInternalEvent for ObjectStorageAdaptiveConcurrency {
    fn name(&self) -> &'static str {
        "ObjectStorageAdaptiveConcurrency"
    }
}

impl InternalEvent for ObjectStorageAdaptiveConcurrency {
    fn emit(self) {
        gauge!("object_storage_adaptive_concurrency_limit").set(self.limit as f64);
        gauge!("object_storage_adaptive_active_objects").set(self.active_objects as f64);
        gauge!("object_storage_adaptive_memory_pressure")
            .set(u8::from(self.memory_pressure) as f64);
    }
}

/// The configured `client_concurrency` ceiling. The live adaptive limit is reported separately
/// as `object_storage_adaptive_concurrency_limit`.
#[derive(Debug)]
pub struct ObjectStorageClientConcurrency {
    pub client_concurrency: usize,
}

impl NamedInternalEvent for ObjectStorageClientConcurrency {
    fn name(&self) -> &'static str {
        "ObjectStorageClientConcurrency"
    }
}

impl InternalEvent for ObjectStorageClientConcurrency {
    fn emit(self) {
        gauge!("object_storage_client_concurrency").set(self.client_concurrency as f64);
    }
}

#[derive(Debug)]
pub struct ObjectStorageAdaptiveCompletion {
    pub elapsed: std::time::Duration,
    pub decoded_bytes: u64,
    pub delivered: bool,
}

impl NamedInternalEvent for ObjectStorageAdaptiveCompletion {
    fn name(&self) -> &'static str {
        "ObjectStorageAdaptiveCompletion"
    }
}

impl InternalEvent for ObjectStorageAdaptiveCompletion {
    fn emit(self) {
        if self.delivered {
            histogram!("object_storage_adaptive_processing_seconds")
                .record(self.elapsed.as_secs_f64());
            histogram!("object_storage_adaptive_decoded_bytes").record(self.decoded_bytes as f64);
        }
    }
}

/// This metrics the delay between when a blob is created in object storage
/// and when the notification appears in the SQS / Azure queue / GCP Pub/Sub topic.
/// This is controlled by the cloud services such as AWS SNS, Azure Event Grid and GCS Pub/Sub notifications.
#[derive(Debug)]
pub struct QueueNotificationCreationDelay<'a> {
    pub delay_seconds: f64,
    pub cloud: &'a str,
    pub bucket: &'a str,
}

impl NamedInternalEvent for QueueNotificationCreationDelay<'_> {
    fn name(&self) -> &'static str {
        "QueueNotificationCreationDelay"
    }
}

impl InternalEvent for QueueNotificationCreationDelay<'_> {
    fn emit(self) {
        debug!(
            message = "Queue notification creation delay measured.",
            delay_seconds = %self.delay_seconds,
            cloud = %self.cloud,
            bucket = %self.bucket
        );
        histogram!(
            "queue_notification_creation_delay_seconds",
            "cloud" => self.cloud.to_string(),
            "bucket" => self.bucket.to_string(),
        )
        .record(self.delay_seconds);
    }
}

/// Metric for tracking lag between when the queue notification was created
/// and when Vector started processing it.
/// This metric is affected by Vector's processing speed / polling rate /
/// current load or backlog on the current process.
#[derive(Debug)]
pub struct QueueNotificationProcessLag<'a> {
    pub lag_seconds: f64,
    pub cloud: &'a str,
    pub bucket: &'a str,
}

impl NamedInternalEvent for QueueNotificationProcessLag<'_> {
    fn name(&self) -> &'static str {
        "QueueNotificationProcessLag"
    }
}

impl InternalEvent for QueueNotificationProcessLag<'_> {
    fn emit(self) {
        debug!(
            message = "Queue notification processing lag measured.",
            lag_seconds = %self.lag_seconds,
            cloud = %self.cloud,
            bucket = ?self.bucket
        );
        histogram!(
            "queue_notification_process_lag_seconds",
            "cloud" => self.cloud.to_string(),
            "bucket" => self.bucket.to_string(),
        )
        .record(self.lag_seconds);
    }
}

/// Metric for tracking the total latency from start of notification processing
/// to when the data is completely ingested in the sink (e.g., ClickHouse).
/// This includes the time to download the object from the cloud storage,
/// parse it and for the sink to acknowledge receipt.
#[derive(Debug)]
pub struct ObjectStorageIngestionLatency<'a> {
    pub latency_seconds: f64,
    pub cloud: &'a str,
    pub bucket: &'a str,
}

impl NamedInternalEvent for ObjectStorageIngestionLatency<'_> {
    fn name(&self) -> &'static str {
        "ObjectStorageIngestionLatency"
    }
}

impl InternalEvent for ObjectStorageIngestionLatency<'_> {
    fn emit(self) {
        debug!(
            message = "Object storage ingestion latency measured.",
            latency_seconds = %self.latency_seconds,
            cloud = %self.cloud,
            bucket = %self.bucket
        );
        histogram!(
            "object_storage_ingestion_latency_seconds",
            "cloud" => self.cloud.to_string(),
            "bucket" => self.bucket.to_string(),
        )
        .record(self.latency_seconds);
    }
}

/// Emitted after a source pauses to honor its configured byte-rate limit.
#[derive(Debug)]
pub struct ObjectStoragePaced {
    pub wait_seconds: f64,
    pub bytes: u64,
}

impl NamedInternalEvent for ObjectStoragePaced {
    fn name(&self) -> &'static str {
        "ObjectStoragePaced"
    }
}

impl InternalEvent for ObjectStoragePaced {
    fn emit(self) {
        debug!(
            message = "Paced object-storage processing to honor byte-rate limit.",
            wait_seconds = %self.wait_seconds,
            bytes = self.bytes,
        );
        histogram!("object_storage_pacing_wait_seconds").record(self.wait_seconds);
        counter!("object_storage_paced_bytes_total").increment(self.bytes);
    }
}

/// Emitted when a single object's decoded size exceeds the configured warn
/// threshold. Observability only — no data is dropped or deferred.
#[derive(Debug)]
pub struct ObjectStorageLargeObject<'a> {
    pub bytes: u64,
    pub bucket: &'a str,
    pub key: &'a str,
}

impl NamedInternalEvent for ObjectStorageLargeObject<'_> {
    fn name(&self) -> &'static str {
        "ObjectStorageLargeObject"
    }
}

impl InternalEvent for ObjectStorageLargeObject<'_> {
    fn emit(self) {
        warn!(
            message = "Object decoded size exceeds warn threshold; may risk the queue visibility timeout even at full speed.",
            bytes = self.bytes,
            bucket = %self.bucket,
            key = %self.key,
            internal_log_rate_limit = true,
        );
        counter!("object_storage_large_object_total").increment(1);
    }
}

/// Helper function to safely parse RFC3339 timestamps and calculate second differences
pub fn calculate_duration_seconds(
    start_time: &str,
    end_time: chrono::DateTime<chrono::Utc>,
) -> Option<f64> {
    let start_utc = chrono::DateTime::parse_from_rfc3339(start_time)
        .ok()?
        .with_timezone(&chrono::Utc);
    let duration = end_time.signed_duration_since(start_utc);
    Some(duration.as_seconds_f64())
}

/// Emit object storage metrics that don't require acknowledgements.
/// This function emits QueueNotificationCreationDelay and QueueNotificationProcessLag metrics.
pub fn emit_object_storage_non_ack_metrics(
    blob_creation_time: &str,
    queue_notification_create_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    processing_start_time: chrono::DateTime<chrono::Utc>,
    cloud: &str,
    bucket: &str,
) {
    if let Some(notification_ts) = queue_notification_create_timestamp {
        let _ =
            calculate_duration_seconds(blob_creation_time, notification_ts).map(|delay_seconds| {
                emit!(QueueNotificationCreationDelay {
                    delay_seconds,
                    cloud,
                    bucket,
                });
                // Return Some(()) to continue the chain
            });

        let lag_duration = processing_start_time.signed_duration_since(notification_ts);
        let lag_seconds = lag_duration.num_milliseconds() as f64 / 1000.0;
        emit!(QueueNotificationProcessLag {
            lag_seconds,
            cloud,
            bucket,
        });
    }
}

/// Emit delivery metrics that requires acknowledgement from the sink.
/// This function emits the ObjectStorageIngestionLatency metric.
///
/// You should call this function only after the sink has acknowledged receipt of the data.
/// It uses the current time to calculate the ingestion latency.
pub fn emit_object_storage_ack_metrics(
    processing_start_time: chrono::DateTime<chrono::Utc>,
    cloud: &str,
    bucket: &str,
) {
    let latency_duration = chrono::Utc::now().signed_duration_since(processing_start_time);
    let latency_seconds = latency_duration.num_milliseconds() as f64 / 1000.0;

    emit!(ObjectStorageIngestionLatency {
        latency_seconds,
        cloud,
        bucket,
    });
}

/// Count of messages fetched from the object-storage notification queue in one receive.
/// Cross-cloud analog of the AWS-specific `sqs_message_received_messages_total`: the azure_blob
/// (Storage Queue) source emits it today, and any future queue-backed source can too. Pairs with
/// `QueueMessageDeleteSucceeded` to give the received-vs-deleted drain-health view the AWS SQS panel
/// has, on non-AWS clouds. The `cloud` label distinguishes provider; `component_id` (added by the
/// tracing-context metrics layer) distinguishes the source (one per table for Lumberjack Prime).
#[derive(Debug)]
pub struct QueueMessageReceived<'a> {
    pub count: u64,
    pub cloud: &'a str,
}

impl NamedInternalEvent for QueueMessageReceived<'_> {
    fn name(&self) -> &'static str {
        "QueueMessageReceived"
    }
}

impl InternalEvent for QueueMessageReceived<'_> {
    fn emit(self) {
        trace!(
            message = "Received messages from queue.",
            count = %self.count,
            cloud = %self.cloud,
        );
        counter!("queue_message_received_total", "cloud" => self.cloud.to_string())
            .increment(self.count);
    }
}

/// Count of queue messages deleted after being processed (and acked, when acknowledgements are on).
/// Cross-cloud analog of the AWS-specific `sqs_message_delete_succeeded_total`. Read together with
/// `QueueMessageReceived`: the two tracking each other is healthy; sustained received > deleted means
/// the source is falling behind or messages are being redelivered.
#[derive(Debug)]
pub struct QueueMessageDeleteSucceeded<'a> {
    pub count: u64,
    pub cloud: &'a str,
}

impl NamedInternalEvent for QueueMessageDeleteSucceeded<'_> {
    fn name(&self) -> &'static str {
        "QueueMessageDeleteSucceeded"
    }
}

impl InternalEvent for QueueMessageDeleteSucceeded<'_> {
    fn emit(self) {
        trace!(
            message = "Deleted messages from queue.",
            count = %self.count,
            cloud = %self.cloud,
        );
        counter!("queue_message_delete_succeeded_total", "cloud" => self.cloud.to_string())
            .increment(self.count);
    }
}

/// Approximate number of messages currently in the notification queue (its backlog / depth).
/// A gauge, not a rate: pairs with `QueueMessageReceived` / `QueueMessageDeleteSucceeded` (the drain
/// rates) to answer "is the backlog growing?" directly, which the rates only imply. Sourced from the
/// provider's cheap metadata call (Azure's `x-ms-approximate-messages-count`), sampled on a coarse
/// timer off the receive path. Azure Monitor only exposes this per storage account, not per queue, so
/// this source-side gauge is the only per-queue backlog signal; `component_id` (added by the
/// tracing-context metrics layer) distinguishes the queue (one source per table for Lumberjack Prime).
#[derive(Debug)]
pub struct QueueMessageCount<'a> {
    pub count: u64,
    pub cloud: &'a str,
}

impl NamedInternalEvent for QueueMessageCount<'_> {
    fn name(&self) -> &'static str {
        "QueueMessageCount"
    }
}

impl InternalEvent for QueueMessageCount<'_> {
    fn emit(self) {
        trace!(
            message = "Measured queue depth.",
            count = %self.count,
            cloud = %self.cloud,
        );
        gauge!("queue_message_count", "cloud" => self.cloud.to_string()).set(self.count as f64);
    }
}

/// A blob's gzip/zstd stream ended part-way through a member (`UnexpectedEof`) on a fully-read
/// (Content-Length-verified) committed blob: the archive is truncated. The source ingested the
/// records that did decode and acked the message rather than redelivering an immutable blob that
/// can never fully decode; the truncated tail is dropped. This is a tolerated data-drop, not a
/// component error, so it is counted on its own metric (`object_storage_blob_truncated_total`)
/// rather than `component_errors_total` — it must not inflate error-rate alerting. Boundary
/// framers can report the bytes in an unframed decoded tail; the compressed archive tail itself
/// is not measurable from the decoder error.
#[derive(Debug)]
pub struct ObjectStorageBlobTruncated<'a> {
    pub cloud: &'a str,
    pub container: &'a str,
    pub blob: &'a str,
    pub dropped_tail_bytes: Option<u64>,
}

impl NamedInternalEvent for ObjectStorageBlobTruncated<'_> {
    fn name(&self) -> &'static str {
        "ObjectStorageBlobTruncated"
    }
}

impl InternalEvent for ObjectStorageBlobTruncated<'_> {
    fn emit(self) {
        if self.cloud == "gcp" {
            warn!(
                message = "Blob stream truncated (UnexpectedEof); ingested the decodable prefix and \
                           acked the message to stop infinite redelivery. The truncated tail is dropped.",
                bucket = %self.container,
                key = %self.blob,
                cloud = %self.cloud,
                dropped_tail_bytes = ?self.dropped_tail_bytes,
            );
        } else {
            warn!(
                message = "Blob stream truncated (UnexpectedEof); ingested the decodable prefix and \
                           acked the message to stop infinite redelivery. The truncated tail is dropped.",
                container = %self.container,
                blob = %self.blob,
                cloud = %self.cloud,
                dropped_tail_bytes = ?self.dropped_tail_bytes,
            );
        }
        counter!("object_storage_blob_truncated_total", "cloud" => self.cloud.to_string())
            .increment(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[test]
    fn test_calculate_duration_seconds_positive() {
        let start_time = "2023-01-01T12:00:00.000Z";
        let end_time = Utc.with_ymd_and_hms(2023, 1, 1, 12, 0, 5).unwrap(); // 5 seconds later

        let duration = calculate_duration_seconds(start_time, end_time);
        assert_eq!(duration, Some(5.0));
    }

    #[test]
    fn test_calculate_duration_seconds_negative() {
        let start_time = "2023-01-01T12:00:05.000Z";
        let end_time = Utc.with_ymd_and_hms(2023, 1, 1, 12, 0, 0).unwrap(); // 5 seconds before

        let duration = calculate_duration_seconds(start_time, end_time);
        assert_eq!(duration, Some(-5.0));
    }
}

/// Queue messages deleted/acknowledged without fetching their objects because the
/// source's table is disabled (see `drain_when_disabled`). The objects are not ingested.
#[derive(Debug)]
pub struct ObjectStorageMessagesDrained<'a> {
    pub table: &'a str,
    pub count: usize,
}

impl NamedInternalEvent for ObjectStorageMessagesDrained<'_> {
    fn name(&self) -> &'static str {
        "ObjectStorageMessagesDrained"
    }
}

impl InternalEvent for ObjectStorageMessagesDrained<'_> {
    fn emit(self) {
        info!(
            message = "Table is disabled; draining queue messages without fetching objects.",
            table = %self.table,
            count = %self.count,
            internal_log_rate_limit = true,
        );
        counter!(
            "lp_blob_source_drained_messages_total",
            "table" => self.table.to_string(),
        )
        .increment(self.count as u64);
    }
}

/// One poll skipped because the source's table is paused (see `pause_when_listed`). Its
/// queue messages are left in place.
#[derive(Debug)]
pub struct ObjectStorageSourcePaused<'a> {
    pub table: &'a str,
    /// `listed`, or `flag_unavailable` when the pause list couldn't be read and the source
    /// failed closed.
    pub reason: &'static str,
}

impl NamedInternalEvent for ObjectStorageSourcePaused<'_> {
    fn name(&self) -> &'static str {
        "ObjectStorageSourcePaused"
    }
}

impl InternalEvent for ObjectStorageSourcePaused<'_> {
    fn emit(self) {
        info!(
            message = "Table is paused; leaving queue messages in place.",
            table = %self.table,
            reason = %self.reason,
            internal_log_rate_limit = true,
        );
        counter!(
            "lp_blob_source_paused_polls_total",
            "table" => self.table.to_string(),
            "reason" => self.reason,
        )
        .increment(1);
    }
}

/// Whether the source's table is paused (1) or polling (0), set on every pause check so
/// autoscaling can exclude a paused source's queue backlog.
#[derive(Debug)]
pub struct ObjectStorageSourcePauseState<'a> {
    pub table: &'a str,
    pub paused: bool,
}

impl NamedInternalEvent for ObjectStorageSourcePauseState<'_> {
    fn name(&self) -> &'static str {
        "ObjectStorageSourcePauseState"
    }
}

impl InternalEvent for ObjectStorageSourcePauseState<'_> {
    fn emit(self) {
        gauge!("lp_blob_source_paused", "table" => self.table.to_string()).set(if self.paused {
            1.0
        } else {
            0.0
        });
    }
}
