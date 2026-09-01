use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use aws_sdk_kinesis::operation::put_records::PutRecordsOutput;
use aws_smithy_runtime_api::client::{orchestrator::HttpResponse, result::SdkError};
use aws_smithy_types::Blob;
use bytes::Bytes;
use tracing::Instrument;

use super::{
    super::service::RecordResult, KinesisClient, KinesisError, KinesisRecord, KinesisResponse,
    Record, SendRecord,
};
use crate::sinks::prelude::*;

#[derive(Clone)]
pub struct KinesisStreamRecord {
    pub record: KinesisRecord,
}

impl Record for KinesisStreamRecord {
    type T = KinesisRecord;

    fn new(payload_bytes: &Bytes, partition_key: &str) -> Self {
        Self {
            record: KinesisRecord::builder()
                .data(Blob::new(&payload_bytes[..]))
                .partition_key(partition_key)
                .build()
                .expect("all required builder fields set"),
        }
    }

    fn encoded_length(&self) -> usize {
        let hash_key_size = self
            .record
            .explicit_hash_key
            .as_ref()
            .map(|s| s.len())
            .unwrap_or_default();

        // data is base64 encoded
        let data_len = self.record.data.as_ref().len();
        let key_len = self.record.partition_key.len();

        data_len.div_ceil(3) * 4 + hash_key_size + key_len + 10
    }

    fn get(self) -> Self::T {
        self.record
    }
}

#[derive(Clone)]
pub struct KinesisStreamClient {
    pub client: KinesisClient,
    /// Public-endpoint client used after a PrivateLink transport failure. `None` disables failover
    /// (the sink was not configured with PrivateLink failover).
    pub fallback_client: Option<KinesisClient>,
    /// Permanent latch: once a PrivateLink transport failure trips it, every subsequent request
    /// uses `fallback_client` for the remainder of the process. Shared across service clones so a
    /// latch set by one in-flight request is seen by all. Only a process restart re-attempts
    /// PrivateLink.
    pub private_link_disabled: Arc<AtomicBool>,
}

impl KinesisStreamClient {
    async fn put_records(
        client: &KinesisClient,
        records: Vec<KinesisRecord>,
        stream_name: String,
        rec_count: usize,
        total_size: usize,
    ) -> Result<KinesisResponse, SdkError<KinesisError, HttpResponse>> {
        client
            .put_records()
            .set_records(Some(records))
            .stream_name(stream_name)
            .send()
            .instrument(info_span!("request").or_current())
            .await
            .map(|output: PutRecordsOutput| KinesisResponse {
                failed_records: extract_failed_records(&output),
                failure_count: output.failed_record_count().unwrap_or(0) as usize,
                events_byte_size: CountByteSize(rec_count, JsonSize::new(total_size)).into(),
            })
    }
}

impl SendRecord for KinesisStreamClient {
    type T = KinesisRecord;
    type E = KinesisError;

    async fn send(
        &self,
        records: Vec<Self::T>,
        stream_name: String,
    ) -> Result<KinesisResponse, SdkError<Self::E, HttpResponse>> {
        let rec_count = records.len();
        let total_size = records
            .iter()
            .fold(0, |acc, record| acc + record.data().as_ref().len());

        match &self.fallback_client {
            Some(fallback) => {
                if self.private_link_disabled.load(Ordering::Relaxed) {
                    // Already latched off PrivateLink: use the public endpoint.
                    Self::put_records(fallback, records, stream_name, rec_count, total_size).await
                } else {
                    // Try PrivateLink. On a PrivateLink transport failure, latch to the public
                    // endpoint and return the error so the sink's retry layer re-drives this
                    // request through NAT; subsequent requests then use NAT directly. We do NOT
                    // resend inline: a transport failure is ambiguous (the request may have been
                    // accepted before the failure surfaced), so an immediate cross-endpoint resend
                    // could duplicate records.
                    let result = Self::put_records(
                        &self.client,
                        records,
                        stream_name,
                        rec_count,
                        total_size,
                    )
                    .await;
                    if let Err(err) = &result
                        && is_private_link_transport_failure(err)
                        && !self.private_link_disabled.swap(true, Ordering::Relaxed)
                    {
                        // Warn exactly once even if several in-flight requests trip the latch.
                        tracing::warn!(
                            message = "Kinesis PrivateLink endpoint unreachable; latching to the \
                                public endpoint for the remainder of the process.",
                            error = %err,
                        );
                    }
                    result
                }
            }
            // Failover disabled: original single-client behavior.
            None => {
                Self::put_records(&self.client, records, stream_name, rec_count, total_size).await
            }
        }
    }
}

/// A PrivateLink-specific *transport* failure — the endpoint could not be reached (DNS/NXDOMAIN,
/// connection refused/reset, TLS, or a connect/read timeout), which is exactly what happens when
/// the netvisor PrivateLink DNS override is absent. A `ServiceError` (a real Kinesis service or
/// throttling response) is deliberately excluded: the endpoint was reachable, so it is not a
/// PrivateLink problem and must not trip the failover.
const fn is_private_link_transport_failure(error: &SdkError<KinesisError, HttpResponse>) -> bool {
    matches!(
        error,
        SdkError::DispatchFailure(_) | SdkError::TimeoutError(_)
    )
}

fn extract_failed_records(output: &PutRecordsOutput) -> Vec<RecordResult> {
    output
        .records()
        .iter()
        .enumerate()
        .filter_map(|(index, record)| {
            record.error_code().map(|error_code| RecordResult {
                index,
                success: false,
                error_code: Some(error_code.to_string()),
                error_message: record.error_message().map(String::from),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use aws_smithy_runtime_api::client::result::ConnectorError;

    use super::*;
    use aws_sdk_kinesis::{operation::put_records::PutRecordsOutput, types::PutRecordsResultEntry};

    #[test]
    fn is_private_link_transport_failure_matches_only_transport_errors() {
        // Endpoint unreachable (DNS/NXDOMAIN, connection refused/reset, TLS) — the
        // netvisor-override-absent case that must fail over.
        let dispatch: SdkError<KinesisError, HttpResponse> =
            SdkError::dispatch_failure(ConnectorError::io("name or service not known".into()));
        assert!(is_private_link_transport_failure(&dispatch));

        // Connect/read timeout — also a reachability failure.
        let timeout: SdkError<KinesisError, HttpResponse> = SdkError::timeout_error("slow");
        assert!(is_private_link_transport_failure(&timeout));

        // A client-side construction error is not an endpoint-reachability problem; represents the
        // non-transport variants (including a real Kinesis `ServiceError`) that must not fail over.
        let construction: SdkError<KinesisError, HttpResponse> =
            SdkError::construction_failure("bad request");
        assert!(!is_private_link_transport_failure(&construction));
    }

    #[test]
    fn test_extract_failed_records_all_success() {
        // Create mock successful records
        let record1 = PutRecordsResultEntry::builder()
            .sequence_number("seq1")
            .shard_id("shard1")
            .build();

        let record2 = PutRecordsResultEntry::builder()
            .sequence_number("seq2")
            .shard_id("shard2")
            .build();

        let output = PutRecordsOutput::builder()
            .records(record1)
            .records(record2)
            .failed_record_count(0)
            .build()
            .unwrap();

        let results = extract_failed_records(&output);

        // Should return empty since no failures
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_extract_failed_records_mixed_success_failure() {
        // Create mock records with mixed success/failure
        let success_record = PutRecordsResultEntry::builder()
            .sequence_number("seq1")
            .shard_id("shard1")
            .build();

        let failure_record = PutRecordsResultEntry::builder()
            .error_code("ProvisionedThroughputExceededException")
            .error_message("Rate exceeded for shard")
            .build();

        let output = PutRecordsOutput::builder()
            .records(success_record)
            .records(failure_record)
            .failed_record_count(1)
            .build()
            .unwrap();

        let results = extract_failed_records(&output);

        // Should only return the failed record
        assert_eq!(results.len(), 1);

        // Only record should be the failed one (originally at index 1)
        assert!(!results[0].success);
        assert_eq!(
            results[0].error_code.as_ref().unwrap(),
            "ProvisionedThroughputExceededException"
        );
        assert_eq!(
            results[0].error_message.as_ref().unwrap(),
            "Rate exceeded for shard"
        );
        assert_eq!(results[0].index, 1);
    }

    #[test]
    fn test_extract_failed_records_all_failures() {
        // Create mock failed records
        let failure_record1 = PutRecordsResultEntry::builder()
            .error_code("ProvisionedThroughputExceededException")
            .error_message("Rate exceeded")
            .build();

        let failure_record2 = PutRecordsResultEntry::builder()
            .error_code("InternalFailure")
            .error_message("Internal server error")
            .build();

        let output = PutRecordsOutput::builder()
            .records(failure_record1)
            .records(failure_record2)
            .failed_record_count(2)
            .build()
            .unwrap();

        let results = extract_failed_records(&output);

        assert_eq!(results.len(), 2);
        assert!(!results[0].success);
        assert!(!results[1].success);
        assert_eq!(
            results[0].error_code.as_ref().unwrap(),
            "ProvisionedThroughputExceededException"
        );
        assert_eq!(results[1].error_code.as_ref().unwrap(), "InternalFailure");
    }
}
