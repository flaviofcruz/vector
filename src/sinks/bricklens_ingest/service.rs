use std::task::{Context, Poll};
use std::time::Duration;

use futures::future::BoxFuture;
use http::{Request, Uri};
use hyper::Body;
use prost_reflect::{MethodDescriptor, prost::Message};
use snafu::Snafu;
use tower::Service;
use tracing::debug;

use vector_lib::finalization::{EventFinalizers, Finalizable};
use vector_lib::internal_event::{ComponentEventsDropped, INTENTIONAL, UNINTENTIONAL};
use vector_lib::request_metadata::{GroupedCountByteSize, MetaDescriptive, RequestMetadata};
use vector_lib::stream::DriverResponse;

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
fn resolve_grpc_status(
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
    pub accepted_count: usize,
    /// Count + estimated byte size of the events in this request, for
    /// `component_sent_events_total` / `component_sent_event_bytes_total`. Populated in `call()`
    /// from request metadata.
    events_sent: GroupedCountByteSize,
    /// Actual protobuf wire bytes sent, for `component_sent_bytes_total`.
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
        // The server reports how many records it durably accepted. If it accepted none, treat the
        // whole request as rejected so the events are not acked as delivered (which would silently
        // lose data). Partial acceptance is still reported as Delivered; per-record partial-failure
        // accounting would require record-level status the current API does not expose.
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
        let uri = build_request_uri(&self.endpoint, &path).map_err(|e| {
            BricklensIngestError::Encode {
                message: format!("Failed to build request URI: {}", e),
            }
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

        // Count successfully accepted records from BatchCreateLogRecordsResponse.results
        let accepted_count = dynamic_response
            .get_field_by_name("results")
            .and_then(|f| f.as_list().map(|l| l.len()))
            .unwrap_or(0);

        // events_sent / bytes_sent are populated by `call()` from request metadata; this method
        // only knows the accepted count.
        Ok(BricklensIngestResponse {
            accepted_count,
            events_sent: GroupedCountByteSize::new_untagged(),
            bytes_sent: 0,
        })
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
            // Capture the events' count + estimated byte size before `req` is consumed, for
            // `component_sent_events_total` telemetry.
            let events_sent = req
                .get_metadata()
                .events_estimated_json_encoded_byte_size()
                .clone();

            let (http_req, bytes_sent) = service.build_grpc_request(req)?;

            let response = client.request(http_req).await.map_err(|e| {
                BricklensIngestError::Transport {
                    message: e.to_string(),
                }
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
                std::future::poll_fn(|cx| std::pin::Pin::new(&mut response_body).poll_data(cx)).await
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

            let mut response = service.parse_grpc_response(body.freeze())?;
            response.events_sent = events_sent;
            response.bytes_sent = bytes_sent;

            // Log accepted count for observability
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
        DynamicMessage,
        prost_types::{
            DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
            MethodDescriptorProto, ServiceDescriptorProto, field_descriptor_proto,
        },
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

    /// Builds a minimal in-memory descriptor pool containing:
    ///   test.Record         { string message = 1; }
    ///   test.BatchRequest   { repeated Record records = 1; }
    ///   test.Response       { int32 accepted_count = 1; }
    ///   test.TestService    { rpc Batch(BatchRequest) returns (Response); }
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
                    name: Some("BatchRequest".to_string()),
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
                    name: Some("Result".to_string()),
                    field: vec![],
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("Response".to_string()),
                    field: vec![FieldDescriptorProto {
                        name: Some("results".to_string()),
                        number: Some(1),
                        label: Some(field_descriptor_proto::Label::Repeated as i32),
                        r#type: Some(field_descriptor_proto::Type::Message as i32),
                        type_name: Some(".test.Result".to_string()),
                        json_name: Some("results".to_string()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            service: vec![ServiceDescriptorProto {
                name: Some("TestService".to_string()),
                method: vec![MethodDescriptorProto {
                    name: Some("Batch".to_string()),
                    input_type: Some(".test.BatchRequest".to_string()),
                    output_type: Some(".test.Response".to_string()),
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
        let pool = make_test_pool();
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
        assert_eq!(req.uri().path(), "/test.TestService/Batch");
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

        // Build an event whose structure matches test.BatchRequest { repeated Record records }
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
        let input_desc = pool.get_message_by_name("test.BatchRequest").unwrap();
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
    fn test_parse_grpc_response_counts_results() {
        let svc = make_test_service("https://example.com:443");
        // proto3 hand-encoding of: Response { results: [{}, {}, {}] } (3 empty Result messages)
        // field 1, wire type 2 (length-delimited): tag = (1 << 3) | 2 = 0x0A, length = 0x00
        let msg_bytes = vec![0x0A, 0x00, 0x0A, 0x00, 0x0A, 0x00];
        let body = bytes::Bytes::from(encode_grpc_message(msg_bytes));
        let resp = svc.parse_grpc_response(body).unwrap();
        assert_eq!(resp.accepted_count, 3);
    }

    #[test]
    fn test_parse_grpc_response_rejects_short_body() {
        let svc = make_test_service("https://example.com:443");
        let body = bytes::Bytes::from(vec![0x00, 0x00, 0x00, 0x00]); // 4 bytes, need 5
        let err = svc.parse_grpc_response(body).unwrap_err();
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
        let err = svc.parse_grpc_response(body).unwrap_err();
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
        assert_eq!(resp.event_status(), vector_lib::event::EventStatus::Rejected);
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
        assert!(!logic.is_retriable_error(&BricklensIngestError::ResponseParse {
            message: "x".to_string(),
        }));
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
