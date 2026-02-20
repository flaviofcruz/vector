use metrics::histogram;
use tracing::debug;
use vector_lib::emit;
use vector_lib::internal_event::{InternalEvent, NamedInternalEvent};

/// This metrics the delay between when a blob is created in object storage
/// and when the notification appears in the SQS / Azure queue.
/// This is controlled by the cloud services such as AWS SNS and Azure Event Grid.
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
        let _ = calculate_duration_seconds(blob_creation_time, notification_ts).and_then(
            |delay_seconds| {
                emit!(QueueNotificationCreationDelay {
                    delay_seconds,
                    cloud,
                    bucket,
                });
                Some(()) // Return Some(()) to continue the chain
            },
        );

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
