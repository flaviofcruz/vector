use std::task::{Context, Poll};
use std::time::Duration;

use futures::future::BoxFuture;
use http::{Request, Uri};
use hyper::Body;
use prost_reflect::{prost::Message, MethodDescriptor};
use snafu::Snafu;
use tower::Service;
use tracing::{debug, warn};

use vector_lib::config::telemetry;
use vector_lib::finalization::{EventFinalizers, Finalizable};
use vector_lib::internal_event::{ComponentEventsDropped, INTENTIONAL, UNINTENTIONAL};
use vector_lib::request_metadata::{GroupedCountByteSize, MetaDescriptive, RequestMetadata};
use vector_lib::stream::DriverResponse;
use vector_lib::EstimatedJsonEncodedSizeOf;

use crate::sinks::util::retries::RetryLogic;

// gRPC status codes (https://grpc.io/docs/guides/status-codes/) that represent transient
// conditions worth retrying. Everything else is treated as a permanent failure.
const GRPC_STATUS_DEADLINE_EXCEEDED: i32 = 4;
const GRPC_STATUS_RESOURCE_EXHAUSTED: i32 = 8;
const GRPC_STATUS_UNAVAILABLE: i32 = 14;

/// Errors returned by [`BricklensIngestService::call`].
///
/// This is a concrete error type (rather than a boxed `crate::Error`) specifically so the Tower
/// retry middleware can downcast it back from the boxed error produced by the `Timeout` layer and
/// consult [`BricklensIngestError::is_retriable`] in [`BricklensRetryLogic::is_retriable_error`].
/// A stringly-typed error cannot be downcast and would force the retry policy into its
/// "retry everything" fallback, defeating the retriable/permanent distinction.
#[derive(Debug, Snafu)]
pub enum BricklensIngestError {
    /// The protobuf message could not be built or encoded. This is a deterministic client-side
    /// shaping error and will never succeed on retry.
    #[snafu(display("{}", message))]
    Encode { message: String },

    /// The HTTP/2 transport failed before a gRPC status was received (connection reset, DNS, TLS
    /// handshake, etc.). Transient — safe to retry.
    #[snafu(display("gRPC request failed: {}", message))]
    Transport { message: String },

    /// The server returned a non-OK gRPC status.
    #[snafu(display("gRPC error {}: {}", status, message))]
    Grpc { status: i32, message: String },

    /// The gRPC response framing or body could not be parsed. Deterministic — not retriable.
    #[snafu(display("{}", message))]
    ResponseParse { message: String },
}

impl BricklensIngestError {
    /// Whether this error is retriable — i.e. a transient condition where retrying the request
    /// could succeed.
    fn is_retriable(&self) -> bool {
        match self {
            // Transport-level failures are virtually always transient.
            Self::Transport { .. } => true,
            // Retry only the gRPC status codes that indicate a transient condition.
            Self::Grpc { status, .. } => matches!(
                *status,
                GRPC_STATUS_DEADLINE_EXCEEDED
                    | GRPC_STATUS_RESOURCE_EXHAUSTED
                    | GRPC_STATUS_UNAVAILABLE
            ),
            // Encoding and response-parse failures are deterministic; retrying cannot help.
            Self::Encode { .. } | Self::ResponseParse { .. } => false,
        }
    }
}

/// Retry policy for the `bricklens_ingest` sink. Drives the Tower `Retry` layer wired up in
/// `config.rs`, retrying transport failures and transient gRPC statuses while dropping permanent
/// failures immediately.
#[derive(Clone, Default)]
pub struct BricklensRetryLogic;

impl RetryLogic for BricklensRetryLogic {
    type Error = BricklensIngestError;
    type Request = BricklensIngestRequest;
    type Response = BricklensIngestResponse;

    fn is_retriable_error(&self, error: &Self::Error) -> bool {
        error.is_retriable()
    }

    // NOTE: `should_retry_response` is intentionally left at its default (`Successful`). A response
    // with `accepted_count == 0` is reported as `Rejected` by `event_status()` (so the loss is
    // accounted for) but is NOT retried: the current API does not distinguish a transient
    // server-side rejection from a permanent validation failure, and blindly retrying risks
    // duplicate writes on an ambiguous partial success.
}

/// Builds the gRPC request URI from the endpoint and RPC path.
/// The TCP connection always goes to `endpoint` (e.g. 127.0.0.3:443 for the s2s-proxy sidecar).
/// SNI is set separately via the TLS callback in sink.rs using `server_name`, which is how the
/// s2s-proxy sidecar determines which backend to route to. The URI authority is intentionally
/// kept as the endpoint address so that hyper connects to the sidecar rather than DNS-resolving
/// the privileged DBNS hostname directly.
fn build_request_uri(endpoint: &Uri, path: &str) -> crate::Result<Uri> {
    let endpoint_str = endpoint.to_string();
    let endpoint_base = endpoint_str.trim_end_matches('/');
    format!("{}{}", endpoint_base, path)
        .parse::<Uri>()
        .map_err(|e| format!("Invalid URI: {}", e).into())
}

/// Wraps a serialized protobuf message with the 5-byte gRPC framing prefix
/// (1 byte compression flag + 4 bytes big-endian message length).
fn encode_grpc_message(message: Vec<u8>) -> Vec<u8> {
    let len = message.len() as u32;
    let mut framed = Vec::with_capacity(5 + message.len());
    framed.push(0); // compression flag: 0 = uncompressed
    framed.extend_from_slice(&len.to_be_bytes());
    framed.extend_from_slice(&message);
    framed
}

/// Extracts the `grpc-status` code from a header or trailer map, if present and parseable.
fn grpc_status_code(headers: &http::HeaderMap) -> Option<i32> {
    headers
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i32>().ok())
}

/// Extracts the `grpc-message` text from a header or trailer map, if present.
fn grpc_status_message(headers: &http::HeaderMap) -> Option<String> {
    headers
        .get("grpc-message")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

/// Resolves the effective gRPC status and message for a response.
///
/// The status can arrive in the initial response HEADERS frame (a "Trailers-Only" response —
/// typical for fast errors) or in the HTTP/2 trailers sent after the body. A status in the headers
/// takes precedence; otherwise the trailer status is used; absent from both, the call is treated as
/// OK (status 0). Reading the trailers matters because an intermediary (e.g. the s2s-proxy/Envoy
/// hop) can deliver a transient status there — and missing it would misreport the error and skip
/// the retry.
pub(crate) fn resolve_grpc_status(
    headers: &http::HeaderMap,
    trailers: Option<&http::HeaderMap>,
) -> (i32, Option<String>) {
    let status = grpc_status_code(headers)
        .or_else(|| trailers.and_then(grpc_status_code))
        .unwrap_or(0);
    let message = grpc_status_message(headers).or_else(|| trailers.and_then(grpc_status_message));
    (status, message)
}

// `Clone` is required by the Tower retry layer (`Request: Clone`), which clones the request to
// replay it on a retry. This is safe: the driver calls `take_finalizers()` before the request
// enters the service stack (see vector-stream `Driver::run`), so the request being cloned for
// retries always carries an empty finalizer set — acknowledgement happens once, from the driver.
#[derive(Clone, Debug)]
pub struct BricklensIngestRequest {
    pub events: Vec<vector_lib::event::Event>,
    pub metadata: RequestMetadata,
    pub finalizers: EventFinalizers,
}

#[derive(Debug)]
pub struct BricklensIngestResponse {
    /// Number of records the server reported as durably accepted. Sourced from per-record status
    /// (`BatchCreateLogRecordsResponse.results[].success`) or per-batch counters
    /// (`WriteMetricsResponse.metrics_written`, modulo `status == FAILED`). Every supported
    /// bricklens response provides this count; a 0 here means the batch did not land.
    pub accepted_count: usize,
    /// Count + estimated byte size of the events in this request, for
    /// `component_sent_events_total` / `component_sent_event_bytes_total`. Set at construction in
    /// `parse_grpc_response` from the real events `call()` computes (it recomputes the grouped size
    /// rather than reading the always-zero request-metadata field — see `call()`).
    events_sent: GroupedCountByteSize,
    /// Actual protobuf wire bytes sent, for `component_sent_bytes_total`. Like `events_sent`, set at
    /// construction in `parse_grpc_response` from the encoded payload size `call()` computes.
    bytes_sent: usize,
}

impl Finalizable for BricklensIngestRequest {
    fn take_finalizers(&mut self) -> EventFinalizers {
        std::mem::take(&mut self.finalizers)
    }
}

impl MetaDescriptive for BricklensIngestRequest {
    fn get_metadata(&self) -> &RequestMetadata {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut RequestMetadata {
        &mut self.metadata
    }
}

impl DriverResponse for BricklensIngestResponse {
    fn event_status(&self) -> vector_lib::event::EventStatus {
        // The server reports how many records it durably accepted. Zero acceptance → Rejected so
        // events are not acked as delivered (which would silently lose data); partial acceptance is
        // still Delivered (per-record partial-failure accounting would require richer per-record
        // status than the response protos currently expose).
        if self.accepted_count > 0 {
            vector_lib::event::EventStatus::Delivered
        } else {
            vector_lib::event::EventStatus::Rejected
        }
    }

    fn events_sent(&self) -> &GroupedCountByteSize {
        &self.events_sent
    }

    fn bytes_sent(&self) -> Option<usize> {
        Some(self.bytes_sent)
    }
}

#[derive(Clone)]
pub struct BricklensIngestService {
    client: hyper::Client<hyper_openssl::HttpsConnector<hyper::client::HttpConnector>>,
    endpoint: Uri,
    method: MethodDescriptor,
    /// Per-request deadline, sent to the server as a `grpc-timeout` header. Mirrors the client-side
    /// Tower `Timeout` layer so the server can abandon work the client has already given up on.
    request_timeout: Duration,
}

impl BricklensIngestService {
    pub fn new(
        client: hyper::Client<hyper_openssl::HttpsConnector<hyper::client::HttpConnector>>,
        endpoint: Uri,
        method: MethodDescriptor,
        request_timeout: Duration,
    ) -> Self {
        Self {
            client,
            endpoint,
            method,
            request_timeout,
        }
    }

    /// Convert a Vector LogEvent to a VRL Value for dynamic protobuf encoding.
    /// This preserves the entire event structure as shaped by VRL transforms.
    fn log_event_to_vrl_value(
        log: &vector_lib::event::LogEvent,
    ) -> Result<vrl::value::Value, String> {
        // LogEvent internally stores a vrl::value::Value - just clone it
        // No JSON intermediate representation needed
        Ok(log.value().clone())
    }

    /// Validates the service configuration without making a network request.
    /// This ensures the gRPC request path can be constructed correctly.
    pub fn validate_configuration(&self) -> crate::Result<()> {
        let path = self.grpc_path();
        build_request_uri(&self.endpoint, &path)?;
        Ok(())
    }

    fn grpc_path(&self) -> String {
        format!(
            "/{}/{}",
            self.method.parent_service().full_name(),
            self.method.name()
        )
    }

    /// Builds the gRPC HTTP/2 request and returns it alongside the encoded protobuf payload size
    /// (used for `bytes_sent` telemetry).
    fn build_grpc_request(
        &self,
        request: BricklensIngestRequest,
    ) -> Result<(Request<Body>, usize), BricklensIngestError> {
        let input_desc = self.method.input();
        use vrl::protobuf::encode::encode_message;

        let encode_options = vrl::protobuf::encode::Options {
            use_json_names: false,
        };

        // Convert events to VRL values, tracking drops by type so we can emit metrics.
        let mut intentional_drops: usize = 0; // non-log events (Metric, Trace): sink is log-only
        let mut unintentional_drops: usize = 0; // conversion failures: should not happen
        let event_values: Vec<vrl::value::Value> = request
            .events
            .iter()
            .filter_map(|event| match event {
                vector_lib::event::Event::Log(log) => match Self::log_event_to_vrl_value(log) {
                    Ok(value) => Some(value),
                    Err(e) => {
                        tracing::error!("Failed to convert log event to VRL value: {}", e);
                        unintentional_drops += 1;
                        None
                    }
                },
                _ => {
                    intentional_drops += 1;
                    None
                }
            })
            .collect();

        if intentional_drops > 0 {
            emit!(ComponentEventsDropped::<INTENTIONAL> {
                count: intentional_drops,
                reason: "bricklens_ingest only supports log events",
            });
        }
        if unintentional_drops > 0 {
            emit!(ComponentEventsDropped::<UNINTENTIONAL> {
                count: unintentional_drops,
                reason: "failed to convert log event to VRL value",
            });
        }

        if event_values.is_empty() {
            return Err(BricklensIngestError::Encode {
                message: "No events to encode".to_string(),
            });
        }

        // The event is expected to already be the full proto message, shaped by a VRL
        // remap + reduce transform pipeline. The reduce transform aggregates per-record
        // events into one merged event containing the complete BatchCreateLogRecordsRequest
        // structure (batch-level fields + a 'requests' array). Use max_events = 1 on the
        // sink so exactly one merged event arrives per call.
        if event_values.len() > 1 {
            tracing::warn!(
                count = event_values.len(),
                "bricklens_ingest received multiple events; only the first will be encoded. \
                 Use max_events = 1 with a reduce transform for batching."
            );
        }
        let event_value = event_values.into_iter().next().unwrap();

        // Encode using VRL's dynamic protobuf encoder
        let dynamic_msg =
            encode_message(&input_desc, event_value, &encode_options).map_err(|e| {
                tracing::error!("Encode failed: {}", e);
                BricklensIngestError::Encode {
                    message: format!("Failed to encode message: {}", e),
                }
            })?;

        // Encode to bytes
        let buf = dynamic_msg.encode_to_vec();
        let byte_size = buf.len();

        // Build gRPC HTTP/2 request
        let path = self.grpc_path();
        let uri =
            build_request_uri(&self.endpoint, &path).map_err(|e| BricklensIngestError::Encode {
                message: format!("Failed to build request URI: {}", e),
            })?;

        // Tell the server the same deadline the client enforces via the Tower `Timeout` layer.
        // The timeout is configured in whole seconds (TowerRequestConfig::timeout_secs), so second
        // granularity is exact; gRPC caps this value at 8 digits, which seconds only exceed past
        // ~3.17 years.
        let grpc_timeout = format!("{}S", self.request_timeout.as_secs());

        let req = Request::builder()
            .uri(uri)
            .method("POST")
            .header("content-type", "application/grpc+proto")
            .header("te", "trailers")
            .header("grpc-encoding", "identity")
            .header("grpc-timeout", grpc_timeout)
            .body(Body::from(encode_grpc_message(buf)))
            .map_err(|e| BricklensIngestError::Encode {
                message: format!("Failed to build request: {}", e),
            })?;

        Ok((req, byte_size))
    }

    fn parse_grpc_response(
        &self,
        mut body: impl bytes::Buf,
        events_sent: GroupedCountByteSize,
        bytes_sent: usize,
    ) -> Result<BricklensIngestResponse, BricklensIngestError> {
        use prost_reflect::DynamicMessage;

        // Parse gRPC framing (5-byte prefix: 1 byte compression flag + 4 bytes message length)
        if body.remaining() < 5 {
            return Err(BricklensIngestError::ResponseParse {
                message: "Response too short".to_string(),
            });
        }

        let compression_flag = body.get_u8();

        // Validate compression - we only support uncompressed messages (flag = 0)
        if compression_flag != 0 {
            return Err(BricklensIngestError::ResponseParse {
                message: format!(
                    "Compressed responses not supported (compression flag: {})",
                    compression_flag
                ),
            });
        }

        let message_len = body.get_u32() as usize;

        if body.remaining() < message_len {
            return Err(BricklensIngestError::ResponseParse {
                message: "Incomplete message".to_string(),
            });
        }

        let message_bytes = body.copy_to_bytes(message_len);

        // Decode using dynamic message
        let output_desc = self.method.output();
        let dynamic_response = DynamicMessage::decode(output_desc, message_bytes.as_ref())
            .map_err(|e| BricklensIngestError::ResponseParse {
                message: format!("Failed to decode response: {}", e),
            })?;

        // Dispatch on the response message name so the supported RPCs are explicit in code rather
        // than implicit in field-name probing. Adding a new bricklens RPC requires extending this
        // match — the unknown branch will warn + Reject until that happens, which is louder than
        // a silent field-probe miss and easier to audit.
        let accepted_count = match self.method.output().name() {
            "BatchCreateLogRecordsResponse" => self.count_log_record_results(&dynamic_response),
            "WriteMetricsResponse" => self.count_write_metrics_result(&dynamic_response),
            // bricklens-ingest-external is an atomic (all-or-nothing) forwarder: its Export* RPCs
            // return an empty `ExportResponse` with no partial-success channel, and this code is
            // reached only on a gRPC-OK status, so a response means the destination accepted the
            // one message we sent. `build_grpc_request` encodes and sends exactly ONE event (the
            // merged event; it warns and drops the tail if handed more than one — see the
            // `event_values.len() > 1` guard), so the accepted count is 1. It is deliberately NOT
            // the request's input event count: acking a never-sent tail as delivered would silently
            // lose those events when misconfigured with `max_events > 1`.
            "ExportResponse" => 1,
            other => {
                // The sink is pointed at a method whose response type we don't know how to
                // interpret. Treat as Rejected (accepted_count = 0) rather than silently acking
                // events as delivered, and warn so the misconfiguration is visible.
                warn!(
                    response_type = other,
                    internal_log_rate_secs = 60,
                    "bricklens_ingest: response proto type is not supported (expected \
                     BatchCreateLogRecordsResponse, WriteMetricsResponse, or ExportResponse); \
                     treating batch as Rejected so events are not silently lost"
                );
                0
            }
        };

        Ok(BricklensIngestResponse {
            accepted_count,
            events_sent,
            bytes_sent,
        })
    }

    /// Returns the number of `success == true` entries in `BatchCreateLogRecordsResponse.results`
    /// and logs (rate-limited) a sample of per-record rejection reasons. Returning the list length —
    /// NOT just successes — would silently ack KM/encryption per-record failures (which surface as
    /// `success=false` with an `error_message` while the gRPC status stays 0/OK).
    fn count_log_record_results(&self, response: &prost_reflect::DynamicMessage) -> usize {
        let results = response.get_field_by_name("results");
        let Some(result_list) = results.as_ref().and_then(|f| f.as_list()) else {
            // The dispatcher already verified the response is BatchCreateLogRecordsResponse, so a
            // missing `results` field means the proto descriptor doesn't match the production
            // schema. Reject the batch and warn so the misconfiguration is visible.
            warn!(
                internal_log_rate_secs = 60,
                "bricklens_ingest: BatchCreateLogRecordsResponse missing `results` field; \
                 proto descriptor likely out of sync with server. Treating batch as Rejected."
            );
            return 0;
        };

        let total = result_list.len();
        // Keep only a small sample of per-record reasons for logging: a fully-rejected 100-record
        // batch would otherwise build and emit 100 strings on one line. The sample is enough to
        // diagnose the failure; `rejected_count` carries the true total.
        const MAX_REASON_SAMPLE: usize = 3;
        let mut accepted_count = 0usize;
        let mut rejected_count = 0usize;
        let mut reason_sample: Vec<String> = Vec::new();
        for item in result_list {
            let Some(record) = item.as_message() else {
                continue;
            };
            let success = record
                .get_field_by_name("success")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if success {
                accepted_count += 1;
            } else {
                rejected_count += 1;
                if reason_sample.len() < MAX_REASON_SAMPLE {
                    let record_id = record
                        .get_field_by_name("record_id")
                        .and_then(|v| v.as_str().map(str::to_string))
                        .unwrap_or_default();
                    let error_message = record
                        .get_field_by_name("error_message")
                        .and_then(|v| v.as_str().map(str::to_string))
                        .unwrap_or_default();
                    reason_sample.push(format!("record_id={record_id}: {error_message}"));
                }
            }
        }
        if rejected_count > 0 {
            // Rate-limited: tracing-limit is on by default (10s); widen to once/minute since a
            // systematic rejection would otherwise fire on every batch. The dropped records are
            // already counted via event_status -> component_discarded_events_total, so this log is
            // only for diagnosis — a throttled sample of reasons is enough.
            warn!(
                accepted_count,
                rejected_count,
                total,
                reason_sample = ?reason_sample,
                internal_log_rate_secs = 60,
                "bricklens_ingest: server rejected one or more records in the batch"
            );
        }

        accepted_count
    }

    /// Reads `metrics_written` / `metrics_failed` / `status` out of a `WriteMetricsResponse`-shaped
    /// message and returns the accepted count. The batch is reported as accepted only when
    /// `status != FAILED` AND `metrics_written > 0`; otherwise the returned count is 0 so
    /// `event_status()` reports Rejected and the loss is not silently acked. A rate-limited warning
    /// surfaces per-metric failures (`metrics_failed > 0`) without depending on the per-record
    /// detail that the `WriteMetrics` response does not carry.
    fn count_write_metrics_result(&self, response: &prost_reflect::DynamicMessage) -> usize {
        // WriteResult::FAILED — hardcoded to avoid importing a generated enum for one comparison.
        const WRITE_RESULT_FAILED: i32 = 3;

        let status_code = response
            .get_field_by_name("status")
            .and_then(|v| v.as_enum_number());
        let written = response
            .get_field_by_name("metrics_written")
            .and_then(|v| v.as_i32())
            .unwrap_or(0);
        let failed = response
            .get_field_by_name("metrics_failed")
            .and_then(|v| v.as_i32())
            .unwrap_or(0);

        let is_failed_status = status_code == Some(WRITE_RESULT_FAILED);
        // Clamp to non-negative: a negative `metrics_written` on the wire is junk we should not
        // propagate as a huge usize via `as` casting. Treat it as zero successes.
        let accepted_count = if is_failed_status || written <= 0 {
            0usize
        } else {
            written as usize
        };

        if accepted_count == 0 || failed > 0 {
            warn!(
                metrics_written = written,
                metrics_failed = failed,
                status = ?status_code,
                internal_log_rate_secs = 60,
                "bricklens_ingest: server reported a failed or partial metrics write"
            );
        }

        accepted_count
    }
}

impl Service<BricklensIngestRequest> for BricklensIngestService {
    type Response = BricklensIngestResponse;
    type Error = BricklensIngestError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: BricklensIngestRequest) -> Self::Future {
        let client = self.client.clone();
        let service = self.clone();

        Box::pin(async move {
            // Compute the events' count + estimated JSON byte size directly from the events here,
            // for `component_sent_events_total` / `component_sent_event_bytes_total`.
            //
            // We deliberately do NOT read
            // `req.get_metadata().events_estimated_json_encoded_byte_size()`: this sink's
            // `RequestBuilder::split_input` hands the payload encoder an empty `Vec` (the events are
            // carried in metadata and protobuf-encoded here, not re-encoded by the unused payload
            // encoder), so `RequestMetadataBuilder::build()` derives that field from the empty
            // encoder output and it is always `CountByteSize(0, 0)`. Reading it would leave
            // `component_sent_events_total` stuck at 0 even on fully-delivered batches. Instead we
            // recompute the grouped size from the real events with the same logic as
            // `RequestMetadataBuilder::from_events` (telemetry-aware tagging + estimated JSON size),
            // mirroring how `bytes_sent` is sourced from the real encoded payload at this call site.
            let mut events_sent = telemetry().create_request_count_byte_size();
            for event in &req.events {
                events_sent.add_event(event, event.estimated_json_encoded_size_of());
            }
            let (http_req, bytes_sent) = service.build_grpc_request(req)?;

            let response =
                client
                    .request(http_req)
                    .await
                    .map_err(|e| BricklensIngestError::Transport {
                        message: e.to_string(),
                    })?;

            // The gRPC status can arrive in the initial HEADERS frame (a "Trailers-Only" response,
            // typical for fast errors) or in the HTTP/2 trailers sent after the body. Snapshot the
            // initial headers, then drain the body and read the trailers, and let
            // resolve_grpc_status() prefer the header status over the trailer status.
            //
            // hyper::body::to_bytes() discards trailers, so we poll the data frames and the
            // trailers explicitly via the HttpBody trait (still hyper 0.14; no http-body-util).
            let response_headers = response.headers().clone();

            use hyper::body::HttpBody as _;
            let mut response_body = response.into_body();
            let mut body = bytes::BytesMut::new();
            while let Some(chunk) =
                std::future::poll_fn(|cx| std::pin::Pin::new(&mut response_body).poll_data(cx))
                    .await
            {
                let chunk = chunk.map_err(|e| BricklensIngestError::Transport {
                    message: format!("Failed to read response body: {}", e),
                })?;
                body.extend_from_slice(&chunk);
            }
            let trailers =
                std::future::poll_fn(|cx| std::pin::Pin::new(&mut response_body).poll_trailers(cx))
                    .await
                    .map_err(|e| BricklensIngestError::Transport {
                        message: format!("Failed to read response trailers: {}", e),
                    })?;

            let (status, message) = resolve_grpc_status(&response_headers, trailers.as_ref());
            if status != 0 {
                return Err(BricklensIngestError::Grpc {
                    status,
                    message: message.unwrap_or_else(|| "Unknown error".to_string()),
                });
            }

            // Parse the response for the accepted count and build it with the real telemetry values
            // computed above: the grouped size of the events actually sent and the encoded payload's
            // byte size. For the atomic `ExportResponse` forwarder this is the whole batch on a
            // gRPC-OK response.
            let response = service.parse_grpc_response(body.freeze(), events_sent, bytes_sent)?;

            // Log accepted count for observability.
            debug!(
                message = "Bricklens request completed",
                accepted_count = response.accepted_count
            );

            Ok(response)
        })
    }
}

#[cfg(test)]
mod tests {
    use prost_reflect::{
        prost_types::{
            field_descriptor_proto, DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto,
            FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet, MethodDescriptorProto,
            ServiceDescriptorProto,
        },
        DynamicMessage,
    };
    use vector_lib::event::{Event, LogEvent};

    use super::*;

    const GRPC_PATH: &str = "/databricks.bricklensingestinternal.api.v1.BricklensIngestInternalService/BatchCreateLogRecords";

    // --- build_request_uri ---

    #[test]
    fn test_build_request_uri_uses_endpoint_authority() {
        let endpoint: Uri = "https://127.0.0.3:443".parse().unwrap();
        let uri = build_request_uri(&endpoint, GRPC_PATH).unwrap();
        assert_eq!(uri.host(), Some("127.0.0.3"));
        assert_eq!(uri.port_u16(), Some(443));
        assert_eq!(uri.path(), GRPC_PATH);
        assert_eq!(uri.scheme_str(), Some("https"));
    }

    #[test]
    fn test_build_request_uri_preserves_scheme_and_port() {
        let endpoint: Uri = "https://127.0.0.3:9090".parse().unwrap();
        let uri = build_request_uri(&endpoint, "/some.Service/Method").unwrap();
        assert_eq!(uri.host(), Some("127.0.0.3"));
        assert_eq!(uri.port_u16(), Some(9090));
        assert_eq!(uri.scheme_str(), Some("https"));
        assert_eq!(uri.path(), "/some.Service/Method");
    }

    // --- encode_grpc_message ---

    #[test]
    fn test_encode_grpc_message_adds_5_byte_framing_prefix() {
        let message = b"hello world".to_vec();
        let framed = encode_grpc_message(message.clone());
        assert_eq!(framed.len(), 5 + message.len());
        assert_eq!(framed[0], 0, "compression flag must be 0 (uncompressed)");
        let length = u32::from_be_bytes([framed[1], framed[2], framed[3], framed[4]]);
        assert_eq!(length as usize, message.len());
        assert_eq!(&framed[5..], message.as_slice());
    }

    #[test]
    fn test_encode_grpc_message_empty_payload() {
        let framed = encode_grpc_message(vec![]);
        assert_eq!(framed.len(), 5);
        assert_eq!(framed[0], 0);
        let length = u32::from_be_bytes([framed[1], framed[2], framed[3], framed[4]]);
        assert_eq!(length, 0);
    }

    // ---------------------------------------------------------------------------
    // Test helpers for build_grpc_request / parse_grpc_response
    // ---------------------------------------------------------------------------

    /// Builds a minimal in-memory descriptor pool mirroring the production log-records shape:
    ///   test.Record                            { string message = 1; }
    ///   test.BatchCreateLogRecordsRequest      { repeated Record records = 1; }
    ///   test.LogRecordResult                   { bool success = 2; }
    ///   test.BatchCreateLogRecordsResponse     { repeated LogRecordResult results = 1; }
    /// Response message name matches production so `parse_grpc_response`'s name-based dispatch
    /// routes here.
    fn make_test_pool() -> prost_reflect::DescriptorPool {
        let file = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![
                DescriptorProto {
                    name: Some("Record".to_string()),
                    field: vec![FieldDescriptorProto {
                        name: Some("message".to_string()),
                        number: Some(1),
                        label: Some(field_descriptor_proto::Label::Optional as i32),
                        r#type: Some(field_descriptor_proto::Type::String as i32),
                        json_name: Some("message".to_string()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("BatchCreateLogRecordsRequest".to_string()),
                    field: vec![FieldDescriptorProto {
                        name: Some("records".to_string()),
                        number: Some(1),
                        label: Some(field_descriptor_proto::Label::Repeated as i32),
                        r#type: Some(field_descriptor_proto::Type::Message as i32),
                        type_name: Some(".test.Record".to_string()),
                        json_name: Some("records".to_string()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("LogRecordResult".to_string()),
                    // Mirrors the real LogRecordResult field number for `success` (2) so
                    // parse_grpc_response can distinguish accepted from rejected records.
                    field: vec![FieldDescriptorProto {
                        name: Some("success".to_string()),
                        number: Some(2),
                        label: Some(field_descriptor_proto::Label::Optional as i32),
                        r#type: Some(field_descriptor_proto::Type::Bool as i32),
                        json_name: Some("success".to_string()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("BatchCreateLogRecordsResponse".to_string()),
                    field: vec![FieldDescriptorProto {
                        name: Some("results".to_string()),
                        number: Some(1),
                        label: Some(field_descriptor_proto::Label::Repeated as i32),
                        r#type: Some(field_descriptor_proto::Type::Message as i32),
                        type_name: Some(".test.LogRecordResult".to_string()),
                        json_name: Some("results".to_string()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            service: vec![ServiceDescriptorProto {
                name: Some("TestService".to_string()),
                method: vec![MethodDescriptorProto {
                    name: Some("BatchCreateLogRecords".to_string()),
                    input_type: Some(".test.BatchCreateLogRecordsRequest".to_string()),
                    output_type: Some(".test.BatchCreateLogRecordsResponse".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        prost_reflect::DescriptorPool::from_file_descriptor_set(FileDescriptorSet {
            file: vec![file],
        })
        .expect("test descriptor pool")
    }

    fn make_test_service(endpoint: &str) -> BricklensIngestService {
        make_test_service_with_pool(endpoint, make_test_pool())
    }

    fn make_test_service_with_pool(
        endpoint: &str,
        pool: prost_reflect::DescriptorPool,
    ) -> BricklensIngestService {
        let svc = pool.get_service_by_name("test.TestService").unwrap();
        let method = svc.methods().next().unwrap();

        let mut http_connector = hyper::client::HttpConnector::new();
        http_connector.enforce_http(false);
        let ssl_builder =
            openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls()).unwrap();
        let https_connector =
            hyper_openssl::HttpsConnector::with_connector(http_connector, ssl_builder).unwrap();
        let client = hyper::Client::builder()
            .http2_only(true)
            .build(https_connector);

        BricklensIngestService {
            client,
            endpoint: endpoint.parse().unwrap(),
            method,
            request_timeout: Duration::from_secs(60),
        }
    }

    /// Builds a descriptor pool whose response message name is not one parse_grpc_response
    /// recognizes (neither `BatchCreateLogRecordsResponse` nor `WriteMetricsResponse`) — used to
    /// assert that the dispatcher's unknown-name fallback marks the batch as Rejected and warns.
    fn make_test_pool_without_results_field() -> prost_reflect::DescriptorPool {
        let file = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![
                DescriptorProto {
                    name: Some("UnknownRequest".to_string()),
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("UnknownResponse".to_string()),
                    field: vec![],
                    ..Default::default()
                },
            ],
            service: vec![ServiceDescriptorProto {
                name: Some("TestService".to_string()),
                method: vec![MethodDescriptorProto {
                    name: Some("Unknown".to_string()),
                    input_type: Some(".test.UnknownRequest".to_string()),
                    output_type: Some(".test.UnknownResponse".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        prost_reflect::DescriptorPool::from_file_descriptor_set(FileDescriptorSet {
            file: vec![file],
        })
        .expect("test descriptor pool with unrecognized response name")
    }

    /// Builds a descriptor pool that mirrors the real `WriteMetricsResponse` shape:
    ///   enum WriteResult { UNSPECIFIED=0; SUCCEEDED=1; PARTIAL_SUCCESS=2; FAILED=3; }
    ///   message Response {
    ///     WriteResult status = 1;
    ///     int32 metrics_written = 2;
    ///     int32 metrics_failed = 3;
    ///   }
    /// Field numbers match the production proto so the hand-encoded wire bytes in the test
    /// bodies below are valid against this descriptor.
    fn make_test_pool_with_write_metrics_shape() -> prost_reflect::DescriptorPool {
        let file = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            enum_type: vec![EnumDescriptorProto {
                name: Some("WriteResult".to_string()),
                value: vec![
                    EnumValueDescriptorProto {
                        name: Some("WRITE_RESULT_UNSPECIFIED".to_string()),
                        number: Some(0),
                        ..Default::default()
                    },
                    EnumValueDescriptorProto {
                        name: Some("SUCCEEDED".to_string()),
                        number: Some(1),
                        ..Default::default()
                    },
                    EnumValueDescriptorProto {
                        name: Some("PARTIAL_SUCCESS".to_string()),
                        number: Some(2),
                        ..Default::default()
                    },
                    EnumValueDescriptorProto {
                        name: Some("FAILED".to_string()),
                        number: Some(3),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            message_type: vec![
                DescriptorProto {
                    name: Some("WriteMetricsRequest".to_string()),
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("WriteMetricsResponse".to_string()),
                    field: vec![
                        FieldDescriptorProto {
                            name: Some("status".to_string()),
                            number: Some(1),
                            label: Some(field_descriptor_proto::Label::Optional as i32),
                            r#type: Some(field_descriptor_proto::Type::Enum as i32),
                            type_name: Some(".test.WriteResult".to_string()),
                            json_name: Some("status".to_string()),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("metrics_written".to_string()),
                            number: Some(2),
                            label: Some(field_descriptor_proto::Label::Optional as i32),
                            r#type: Some(field_descriptor_proto::Type::Int32 as i32),
                            json_name: Some("metricsWritten".to_string()),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("metrics_failed".to_string()),
                            number: Some(3),
                            label: Some(field_descriptor_proto::Label::Optional as i32),
                            r#type: Some(field_descriptor_proto::Type::Int32 as i32),
                            json_name: Some("metricsFailed".to_string()),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
            ],
            service: vec![ServiceDescriptorProto {
                name: Some("TestService".to_string()),
                method: vec![MethodDescriptorProto {
                    name: Some("WriteMetrics".to_string()),
                    input_type: Some(".test.WriteMetricsRequest".to_string()),
                    output_type: Some(".test.WriteMetricsResponse".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        prost_reflect::DescriptorPool::from_file_descriptor_set(FileDescriptorSet {
            file: vec![file],
        })
        .expect("test descriptor pool with WriteMetrics shape")
    }

    fn make_request(events: Vec<Event>) -> BricklensIngestRequest {
        BricklensIngestRequest {
            events,
            metadata: vector_lib::request_metadata::RequestMetadata::default(),
            finalizers: Default::default(),
        }
    }

    fn log_event(message: &str) -> Event {
        let mut log = LogEvent::default();
        log.insert("message", message);
        Event::Log(log)
    }

    // ---------------------------------------------------------------------------
    // build_grpc_request
    // ---------------------------------------------------------------------------

    #[test]
    fn test_build_grpc_request_sets_grpc_headers_and_path() {
        let svc = make_test_service("https://example.com:443");
        let (req, _) = svc
            .build_grpc_request(make_request(vec![log_event("hello")]))
            .unwrap();
        assert_eq!(req.method(), "POST");
        assert_eq!(req.headers()["content-type"], "application/grpc+proto");
        assert_eq!(req.headers()["te"], "trailers");
        assert_eq!(req.headers()["grpc-encoding"], "identity");
        assert_eq!(req.uri().path(), "/test.TestService/BatchCreateLogRecords");
    }

    #[test]
    fn test_build_grpc_request_uses_endpoint_host_regardless_of_server_name() {
        // server_name is used only for TLS SNI (via the TLS callback), not for URI authority.
        // The TCP connection always goes to the endpoint so the s2s-proxy sidecar is not bypassed.
        let svc = make_test_service("https://127.0.0.3:443");
        let (req, _) = svc
            .build_grpc_request(make_request(vec![log_event("hello")]))
            .unwrap();
        assert_eq!(req.uri().host(), Some("127.0.0.3"));
        assert_eq!(req.uri().port_u16(), Some(443));
    }

    #[test]
    fn test_build_grpc_request_empty_events_returns_error() {
        let svc = make_test_service("https://example.com:443");
        let err = svc.build_grpc_request(make_request(vec![])).unwrap_err();
        assert!(
            err.to_string().contains("No events"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_build_grpc_request_encodes_event_directly_as_proto_message() {
        // With the reduce approach the sink receives one merged event that already IS the
        // full proto message (e.g. BatchRequest { records: [...] }).  The sink encodes it
        // directly without any wrapping.
        let svc = make_test_service("https://example.com:443");

        // Build an event whose structure matches
        // test.BatchCreateLogRecordsRequest { repeated Record records }
        let mut log = LogEvent::default();
        log.insert("records[0].message", "hello");
        let (req, _) = svc
            .build_grpc_request(make_request(vec![Event::Log(log)]))
            .unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        #[allow(deprecated)]
        let body = rt.block_on(hyper::body::to_bytes(req.into_body())).unwrap();

        let msg_len = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
        let msg_bytes = &body[5..5 + msg_len];

        let pool = make_test_pool();
        let input_desc = pool
            .get_message_by_name("test.BatchCreateLogRecordsRequest")
            .unwrap();
        let decoded = DynamicMessage::decode(input_desc, msg_bytes).unwrap();
        let records = decoded.get_field_by_name("records").unwrap();
        match &*records {
            prost_reflect::Value::List(items) => {
                assert_eq!(items.len(), 1, "expected 1 record from direct encoding");
            }
            other => panic!("expected list for records field, got {:?}", other),
        }
    }

    // ---------------------------------------------------------------------------
    // parse_grpc_response
    // ---------------------------------------------------------------------------

    #[test]
    fn test_parse_grpc_response_counts_only_successful_records() {
        let svc = make_test_service("https://example.com:443");
        // proto3 hand-encoding of: Response { results: [ {success:true}, {success:true},
        // {success:false} ] }. Each results entry is field 1, wire type 2 (tag 0x0A) + length.
        // A Result with success=true encodes field 2 (bool, wire type 0): tag 0x10, value 0x01
        // (2 bytes). A success=false Result omits the proto3 default → empty message (length 0).
        let ok = [0x0A, 0x02, 0x10, 0x01]; // results[i] = { success: true }
        let fail = [0x0A, 0x00]; // results[i] = {} (success defaults to false)
        let mut msg_bytes = Vec::new();
        msg_bytes.extend_from_slice(&ok);
        msg_bytes.extend_from_slice(&ok);
        msg_bytes.extend_from_slice(&fail);
        let body = bytes::Bytes::from(encode_grpc_message(msg_bytes));
        let resp = svc
            .parse_grpc_response(body, GroupedCountByteSize::new_untagged(), 0)
            .unwrap();
        // Only the two success=true records are counted; the failed one is excluded so a
        // partially-failed batch isn't acked as fully delivered.
        assert_eq!(resp.accepted_count, 2);
    }

    #[test]
    fn test_parse_grpc_response_all_failed_yields_zero_accepted() {
        let svc = make_test_service("https://example.com:443");
        // Response { results: [ {} ] } — one record, success defaults to false. accepted_count
        // must be 0 so event_status() reports Rejected instead of silently acking the loss.
        let msg_bytes = vec![0x0A, 0x00];
        let body = bytes::Bytes::from(encode_grpc_message(msg_bytes));
        let resp = svc
            .parse_grpc_response(body, GroupedCountByteSize::new_untagged(), 0)
            .unwrap();
        assert_eq!(resp.accepted_count, 0);
        assert_eq!(
            resp.event_status(),
            vector_lib::event::EventStatus::Rejected
        );
    }

    /// Hand-encodes a proto3 `Response { status, metrics_written, metrics_failed }` body.
    /// All three fields are varint-encoded (enum and int32 both use wire type 0).
    fn encode_write_metrics_body(status: i32, written: i32, failed: i32) -> Vec<u8> {
        fn encode_varint(mut value: u64, out: &mut Vec<u8>) {
            while value >= 0x80 {
                out.push((value as u8) | 0x80);
                value >>= 7;
            }
            out.push(value as u8);
        }
        let mut buf = Vec::new();
        // Proto3 implicit-presence: only emit a field if it differs from the default. We always
        // emit because the production server always sets these, and the WriteMetrics-shape
        // detection in `parse_grpc_response` keys off field presence in the *descriptor* (which
        // make_test_pool_with_write_metrics_shape declares), not in the wire payload.
        if status != 0 {
            buf.push(0x08); // field 1, varint
            encode_varint(status as u64, &mut buf);
        }
        if written != 0 {
            buf.push(0x10); // field 2, varint
            encode_varint(written as u64, &mut buf);
        }
        if failed != 0 {
            buf.push(0x18); // field 3, varint
            encode_varint(failed as u64, &mut buf);
        }
        buf
    }

    #[test]
    fn test_parse_grpc_response_write_metrics_all_success_yields_delivered() {
        // status=SUCCEEDED(1), metrics_written=5, metrics_failed=0 — happy path.
        let svc = make_test_service_with_pool(
            "https://example.com:443",
            make_test_pool_with_write_metrics_shape(),
        );
        let body = bytes::Bytes::from(encode_grpc_message(encode_write_metrics_body(1, 5, 0)));
        let resp = svc
            .parse_grpc_response(body, GroupedCountByteSize::new_untagged(), 0)
            .unwrap();
        assert_eq!(resp.accepted_count, 5);
        assert_eq!(
            resp.event_status(),
            vector_lib::event::EventStatus::Delivered
        );
    }

    #[test]
    fn test_parse_grpc_response_write_metrics_zero_written_yields_rejected() {
        // metrics_written=0 means the server accepted nothing — must be Rejected so the events
        // are not silently acked. This is the user-requested rule: "mark as error when
        // metrics_written is 0". status=SUCCEEDED is contrived (servers shouldn't return this
        // combo in practice) but pins the behavior on metrics_written alone.
        let svc = make_test_service_with_pool(
            "https://example.com:443",
            make_test_pool_with_write_metrics_shape(),
        );
        let body = bytes::Bytes::from(encode_grpc_message(encode_write_metrics_body(1, 0, 0)));
        let resp = svc
            .parse_grpc_response(body, GroupedCountByteSize::new_untagged(), 0)
            .unwrap();
        assert_eq!(resp.accepted_count, 0);
        assert_eq!(
            resp.event_status(),
            vector_lib::event::EventStatus::Rejected
        );
    }

    #[test]
    fn test_parse_grpc_response_write_metrics_failed_status_yields_rejected() {
        // status=FAILED(3) overrides any metrics_written value — even if the server somehow
        // reports metrics_written=5 alongside FAILED, the explicit FAILED signal wins so the
        // batch is Rejected. Pins the "OR status == FAILED" half of the user's rule.
        let svc = make_test_service_with_pool(
            "https://example.com:443",
            make_test_pool_with_write_metrics_shape(),
        );
        let body = bytes::Bytes::from(encode_grpc_message(encode_write_metrics_body(3, 5, 0)));
        let resp = svc
            .parse_grpc_response(body, GroupedCountByteSize::new_untagged(), 0)
            .unwrap();
        assert_eq!(resp.accepted_count, 0);
        assert_eq!(
            resp.event_status(),
            vector_lib::event::EventStatus::Rejected
        );
    }

    #[test]
    fn test_parse_grpc_response_write_metrics_partial_success_yields_delivered() {
        // status=PARTIAL_SUCCESS(2), metrics_written=3, metrics_failed=2. Partial success is
        // still Delivered (mirrors the log-records path: any non-zero acceptance is Delivered);
        // the per-metric failure count is logged for visibility but doesn't reject the batch.
        let svc = make_test_service_with_pool(
            "https://example.com:443",
            make_test_pool_with_write_metrics_shape(),
        );
        let body = bytes::Bytes::from(encode_grpc_message(encode_write_metrics_body(2, 3, 2)));
        let resp = svc
            .parse_grpc_response(body, GroupedCountByteSize::new_untagged(), 0)
            .unwrap();
        assert_eq!(resp.accepted_count, 3);
        assert_eq!(
            resp.event_status(),
            vector_lib::event::EventStatus::Delivered
        );
    }

    #[test]
    fn test_parse_grpc_response_unknown_shape_rejects() {
        // Pins the misconfiguration fallback: when the response proto matches neither the log-
        // records shape (`results[].success`) nor the WriteMetrics shape
        // (`metrics_written`/`status`), `accepted_count` is 0 so the batch is Rejected rather
        // than silently acked. A rate-limited warn! is emitted so the misconfiguration is visible
        // in logs (asserted indirectly — we don't capture logs here, but the contract is that
        // adding a new RPC requires either matching one of these shapes or extending the parser).
        let svc = make_test_service_with_pool(
            "https://example.com:443",
            make_test_pool_without_results_field(),
        );
        // Empty Response message — the proto has no fields at all.
        let body = bytes::Bytes::from(encode_grpc_message(Vec::new()));
        let resp = svc
            .parse_grpc_response(body, GroupedCountByteSize::new_untagged(), 0)
            .unwrap();
        assert_eq!(resp.accepted_count, 0);
        assert_eq!(
            resp.event_status(),
            vector_lib::event::EventStatus::Rejected
        );
    }

    /// Builds a descriptor pool whose method response is the empty `ExportResponse` (the
    /// bricklens-ingest-external Export* RPCs), so `parse_grpc_response` exercises the
    /// `ExportResponse` branch.
    fn make_test_pool_with_export_response_shape() -> prost_reflect::DescriptorPool {
        let file = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![
                DescriptorProto {
                    name: Some("ExportLogsRequest".to_string()),
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("ExportResponse".to_string()),
                    field: vec![],
                    ..Default::default()
                },
            ],
            service: vec![ServiceDescriptorProto {
                name: Some("TestService".to_string()),
                method: vec![MethodDescriptorProto {
                    name: Some("ExportLogs".to_string()),
                    input_type: Some(".test.ExportLogsRequest".to_string()),
                    output_type: Some(".test.ExportResponse".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        prost_reflect::DescriptorPool::from_file_descriptor_set(FileDescriptorSet {
            file: vec![file],
        })
        .expect("test descriptor pool with ExportResponse")
    }

    #[test]
    fn test_parse_grpc_response_export_response_yields_delivered() {
        use vector_lib::internal_event::CountByteSize;
        use vector_lib::json_size::JsonSize;

        // bricklens-ingest-external is an atomic (all-or-nothing) forwarder: its Export* RPCs return
        // an empty ExportResponse, and a gRPC-OK response means the destination accepted the one
        // message we sent (there is no partial-success channel). accepted_count is 1 -- the single
        // merged message build_grpc_request encodes -- so event_status() reports Delivered (not the
        // Rejected the unknown-name fallback returned before the ExportResponse branch existed).
        // accepted_count is deliberately DECOUPLED from the request's input event count: the sink
        // sends exactly one message, and acking a never-sent tail would silently lose data under a
        // misconfigured max_events > 1. events_sent telemetry (below) is a separate axis and still
        // carries the real per-event grouped size.
        let svc = make_test_service_with_pool(
            "https://example.com:443",
            make_test_pool_with_export_response_shape(),
        );
        // Empty ExportResponse message body.
        let body = bytes::Bytes::from(encode_grpc_message(Vec::new()));
        // Pass a >1 event grouped size (CountByteSize(5, ..)) the way call() does, to prove
        // accepted_count is NOT derived from it (regression guard for the over-count fix) while
        // events_sent/bytes_sent are still threaded straight through (no placeholder, no overwrite).
        let events_sent: GroupedCountByteSize = CountByteSize(5, JsonSize::new(321)).into();
        let resp = svc.parse_grpc_response(body, events_sent, 654).unwrap();
        assert_eq!(
            resp.accepted_count, 1,
            "atomic forwarder sends exactly one merged message; accept count is 1, not the input count"
        );
        assert_eq!(
            resp.event_status(),
            vector_lib::event::EventStatus::Delivered
        );
        assert_eq!(
            resp.events_sent().size(),
            Some(CountByteSize(5, JsonSize::new(321))),
            "events_sent must be the real value threaded in, not a placeholder"
        );
        assert_eq!(
            resp.bytes_sent(),
            Some(654),
            "bytes_sent must be the real encoded payload size threaded in, not a placeholder"
        );
    }

    #[test]
    fn test_parse_grpc_response_rejects_short_body() {
        let svc = make_test_service("https://example.com:443");
        let body = bytes::Bytes::from(vec![0x00, 0x00, 0x00, 0x00]); // 4 bytes, need 5
        let err = svc
            .parse_grpc_response(body, GroupedCountByteSize::new_untagged(), 0)
            .unwrap_err();
        assert!(
            err.to_string().contains("too short"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_parse_grpc_response_rejects_compressed_flag() {
        let svc = make_test_service("https://example.com:443");
        // 5-byte gRPC frame with compression flag = 1 and empty body
        let body = bytes::Bytes::from(vec![0x01, 0x00, 0x00, 0x00, 0x00]);
        let err = svc
            .parse_grpc_response(body, GroupedCountByteSize::new_untagged(), 0)
            .unwrap_err();
        assert!(
            err.to_string().contains("Compressed"),
            "unexpected error: {}",
            err
        );
    }

    // ---------------------------------------------------------------------------
    // DriverResponse: event_status + sent-events telemetry
    // ---------------------------------------------------------------------------

    #[test]
    fn test_event_status_rejected_when_zero_records_accepted() {
        // A successful gRPC call that durably accepts zero records must NOT be acked as
        // Delivered, or the events are silently lost.
        let resp = BricklensIngestResponse {
            accepted_count: 0,
            events_sent: GroupedCountByteSize::new_untagged(),
            bytes_sent: 0,
        };
        assert_eq!(
            resp.event_status(),
            vector_lib::event::EventStatus::Rejected
        );
    }

    #[test]
    fn test_event_status_delivered_when_records_accepted() {
        let resp = BricklensIngestResponse {
            accepted_count: 3,
            events_sent: GroupedCountByteSize::new_untagged(),
            bytes_sent: 0,
        };
        assert_eq!(
            resp.event_status(),
            vector_lib::event::EventStatus::Delivered
        );
    }

    #[test]
    fn test_response_reports_bytes_sent() {
        use vector_lib::internal_event::CountByteSize;
        use vector_lib::json_size::JsonSize;

        let resp = BricklensIngestResponse {
            accepted_count: 1,
            events_sent: CountByteSize(2, JsonSize::new(42)).into(),
            bytes_sent: 128,
        };
        // bytes_sent feeds component_sent_bytes_total; the sink previously hard-coded None here,
        // making throughput invisible.
        assert_eq!(resp.bytes_sent(), Some(128));
    }

    #[test]
    fn test_response_reports_events_sent() {
        use vector_lib::internal_event::CountByteSize;
        use vector_lib::json_size::JsonSize;

        let resp = BricklensIngestResponse {
            accepted_count: 3,
            events_sent: CountByteSize(3, JsonSize::new(99)).into(),
            bytes_sent: 256,
        };
        // events_sent feeds component_sent_events_total on the Delivered path; the sink previously
        // sourced this from the request metadata's estimated-JSON size, which is always 0 here
        // because split_input hands the payload encoder an empty Vec. A zeroed count meant
        // component_sent_events_total never advanced despite successful delivery.
        assert_eq!(
            resp.events_sent().size(),
            Some(CountByteSize(3, JsonSize::new(99)))
        );
    }

    #[test]
    fn test_events_sent_computed_from_events_not_zeroed_metadata() {
        // Reproduces the metric bug at its source: a BricklensIngestRequest built through the real
        // RequestBuilder carries metadata whose events_estimated_json_encoded_byte_size is
        // CountByteSize(0, 0) (split_input encodes an empty payload), yet the request still holds
        // the real events. The call() path must derive events_sent from req.events, so the count is
        // the true number of delivered events rather than the zeroed metadata field.
        use crate::sinks::util::RequestBuilder;
        use vector_lib::config::telemetry;
        use vector_lib::EstimatedJsonEncodedSizeOf;

        let events = vec![log_event("a"), log_event("b"), log_event("c")];

        // Build request metadata exactly as the sink does: via the request builder, whose payload
        // encoder sees an empty event list.
        let builder =
            crate::sinks::bricklens_ingest::request_builder::BricklensIngestRequestBuilder::new(
                crate::sinks::util::Compression::None,
                {
                    use vector_lib::codecs::encoding::{
                        Framer, FramingConfig, JsonSerializerConfig, SerializerConfig,
                    };
                    let serializer = SerializerConfig::Json(JsonSerializerConfig::default())
                        .build()
                        .expect("serializer");
                    let framer = FramingConfig::NewlineDelimited.build();
                    (
                        crate::codecs::Transformer::default(),
                        crate::codecs::Encoder::<Framer>::new(framer, serializer),
                    )
                },
            );
        let (metadata, meta_builder, encoder_events) = builder.split_input(events.clone());
        let payload = builder
            .encode_events(encoder_events)
            .expect("encode empty payload");
        let request_metadata = meta_builder.build(&payload);
        let request = builder.build_request(metadata, request_metadata, payload);

        // The metadata field the old code read is zeroed...
        assert_eq!(
            request
                .get_metadata()
                .events_estimated_json_encoded_byte_size()
                .size(),
            Some(vector_lib::internal_event::CountByteSize(
                0,
                vector_lib::json_size::JsonSize::zero()
            )),
            "metadata estimated-JSON size is expected to be zero for this sink"
        );

        // The events handed to call() arrive with their JSON-size cache already warmed by
        // `RequestMetadataBuilder::from_events` inside `split_input`. That warming is what makes
        // the recompute below an atomic cache load rather than a second structural walk per event.
        // If split_input ever stops warming the cache (or starts giving call() cloned/mutated
        // events that drop it), this fails before the cost regression ships.
        for event in &request.events {
            assert!(
                event.as_log().estimated_json_encoded_size_is_cached(),
                "split_input must warm each event's JSON-size cache before the events_sent loop",
            );
        }

        // ...and recomputing from the request's real events (what call() now does) yields the true
        // count, so component_sent_events_total advances.
        let mut events_sent = telemetry().create_request_count_byte_size();
        for event in &request.events {
            events_sent.add_event(event, event.estimated_json_encoded_size_of());
        }
        let computed = events_sent.size().expect("untagged size");
        assert_eq!(computed.0, 3, "all delivered events must be counted");
        assert!(
            computed.1.get() > 0,
            "estimated JSON byte size must be non-zero"
        );
    }

    // ---------------------------------------------------------------------------
    // BricklensRetryLogic + grpc-timeout
    // ---------------------------------------------------------------------------

    #[test]
    fn test_retry_logic_retries_transient_drops_permanent() {
        let logic = BricklensRetryLogic;

        // Transport-level failures (connection reset, TLS, DNS) are always transient.
        assert!(logic.is_retriable_error(&BricklensIngestError::Transport {
            message: "connection reset".to_string(),
        }));

        // Retriable gRPC statuses: DEADLINE_EXCEEDED(4), RESOURCE_EXHAUSTED(8), UNAVAILABLE(14).
        for status in [4, 8, 14] {
            assert!(
                logic.is_retriable_error(&BricklensIngestError::Grpc {
                    status,
                    message: "x".to_string(),
                }),
                "gRPC status {status} should be retriable"
            );
        }

        // Permanent gRPC statuses must NOT retry: e.g. INVALID_ARGUMENT(3), NOT_FOUND(5),
        // PERMISSION_DENIED(7), INTERNAL(13).
        for status in [3, 5, 7, 13] {
            assert!(
                !logic.is_retriable_error(&BricklensIngestError::Grpc {
                    status,
                    message: "x".to_string(),
                }),
                "gRPC status {status} should not be retriable"
            );
        }

        // Deterministic client-side errors never retry.
        assert!(!logic.is_retriable_error(&BricklensIngestError::Encode {
            message: "x".to_string(),
        }));
        assert!(
            !logic.is_retriable_error(&BricklensIngestError::ResponseParse {
                message: "x".to_string(),
            })
        );
    }

    #[test]
    fn test_build_grpc_request_sets_grpc_timeout_header() {
        // make_test_service builds the service with a 60s request timeout.
        let svc = make_test_service("https://example.com:443");
        let (req, _) = svc
            .build_grpc_request(make_request(vec![log_event("hello")]))
            .unwrap();
        assert_eq!(req.headers()["grpc-timeout"], "60S");
    }

    #[test]
    fn test_resolve_grpc_status_prefers_header_then_trailer() {
        use http::HeaderMap;

        // Trailers-Only response: status + message in the initial headers.
        let mut headers = HeaderMap::new();
        headers.insert("grpc-status", "7".parse().unwrap());
        headers.insert("grpc-message", "denied".parse().unwrap());
        let (status, message) = resolve_grpc_status(&headers, None);
        assert_eq!(status, 7);
        assert_eq!(message.as_deref(), Some("denied"));

        // Status delivered only in the trailers (initial headers carry none). This is the case the
        // old header-only read silently missed — treating a real error as OK (status 0) and
        // skipping the retry.
        let empty = HeaderMap::new();
        let mut trailers = HeaderMap::new();
        trailers.insert("grpc-status", "14".parse().unwrap());
        trailers.insert("grpc-message", "unavailable".parse().unwrap());
        let (status, message) = resolve_grpc_status(&empty, Some(&trailers));
        assert_eq!(status, 14);
        assert_eq!(message.as_deref(), Some("unavailable"));

        // No status in either map → treated as success.
        let (status, _) = resolve_grpc_status(&empty, None);
        assert_eq!(status, 0);
    }
}
