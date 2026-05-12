//! Zerobus service wrapper for Vector sink integration.

#[cfg(feature = "codecs-arrow")]
use databricks_zerobus_ingest_sdk::{ArrowTableProperties, ZerobusArrowStream};
use databricks_zerobus_ingest_sdk::{TableProperties, ZerobusSdk, ZerobusStream};
use futures::future::BoxFuture;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{Mutex, OnceCell};
use tower::Service;
use tracing::{info, warn};
use vector_lib::codecs::encoding::{BatchEncoder, BatchOutput, BatchSerializerConfig};
use vector_lib::finalization::{EventFinalizers, Finalizable};
use vector_lib::request_metadata::{GroupedCountByteSize, MetaDescriptive, RequestMetadata};
use vector_lib::stream::DriverResponse;
use vrl::protobuf::descriptor::get_message_descriptor;

use crate::event::Event;
use crate::sinks::util::retries::RetryLogic;

use super::{config::ZerobusSinkConfig, error::ZerobusSinkError, unity_catalog_schema};

/// The payload for a Zerobus request.
///
/// Proto payloads contain pre-encoded protobuf records, while Arrow payloads
/// carry a RecordBatch for Arrow Flight ingestion.
#[derive(Clone, Debug)]
pub enum ZerobusPayload {
    /// Pre-encoded protobuf records (one byte buffer per event).
    Records(Vec<Vec<u8>>),
    /// An Arrow RecordBatch for Arrow Flight ingestion.
    #[cfg(feature = "codecs-arrow")]
    Arrow(arrow::record_batch::RecordBatch),
}

/// Request type for the Zerobus service.
///
/// Carries the *unencoded* batch — encoding happens inside `Service::call` so
/// that schema-fetch failures flow through the Tower retry layer. Events live
/// behind an `Arc` because Tower's retry policy clones the request before
/// every call (not just on retry), and a deep clone of `Vec<Event>` per call
/// would be wasteful.
#[derive(Clone)]
pub struct ZerobusRequest {
    pub events: Arc<Vec<Event>>,
    pub metadata: RequestMetadata,
    pub finalizers: EventFinalizers,
}

/// Response type for the Zerobus service.
#[derive(Debug)]
pub struct ZerobusResponse {
    pub events_byte_size: GroupedCountByteSize,
}

impl DriverResponse for ZerobusResponse {
    fn event_status(&self) -> vector_lib::event::EventStatus {
        vector_lib::event::EventStatus::Delivered
    }

    fn events_sent(&self) -> &GroupedCountByteSize {
        &self.events_byte_size
    }
}

impl Finalizable for ZerobusRequest {
    fn take_finalizers(&mut self) -> EventFinalizers {
        std::mem::take(&mut self.finalizers)
    }
}

impl MetaDescriptive for ZerobusRequest {
    fn get_metadata(&self) -> &RequestMetadata {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut RequestMetadata {
        &mut self.metadata
    }
}

/// Determines what kind of stream the service creates and how payloads are ingested.
#[derive(Clone)]
pub enum StreamMode {
    /// Proto stream using `ZerobusStream::ingest_records_offset`.
    Proto {
        descriptor_proto: Arc<prost_reflect::prost_types::DescriptorProto>,
    },
    /// Arrow Flight stream using `ZerobusArrowStream::ingest_batch`.
    #[cfg(feature = "codecs-arrow")]
    Arrow {
        arrow_schema: Arc<arrow::datatypes::Schema>,
    },
}

/// The active stream, either proto or Arrow Flight.
enum ActiveStream {
    Proto(ZerobusStream),
    #[cfg(feature = "codecs-arrow")]
    Arrow(ZerobusArrowStream),
    /// Test-only variant that returns a pre-configured error on ingest.
    #[cfg(test)]
    Mock(MockStream),
}

impl ActiveStream {
    /// Gracefully flush and close the underlying SDK stream.
    ///
    /// Safe to call before the value is dropped — the SDK's own `Drop`
    /// implementation is a no-op on already-closed streams.
    async fn close(&mut self) {
        let result = match self {
            ActiveStream::Proto(s) => s.close().await,
            #[cfg(feature = "codecs-arrow")]
            ActiveStream::Arrow(s) => s.close().await,
            #[cfg(test)]
            ActiveStream::Mock(m) => {
                m.closed.store(true, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            }
        };
        if let Err(e) = result {
            warn!(message = "Failed to close Zerobus stream.", error = %e);
        }
    }
}

/// A mock stream that returns a configurable error on the next ingest call.
#[cfg(test)]
pub struct MockStream {
    /// When `Some`, the next ingest returns this error; when `None`, ingest succeeds.
    next_error: std::sync::Mutex<Option<databricks_zerobus_ingest_sdk::ZerobusError>>,
    /// Shared flag set to `true` when `ActiveStream::close()` is called.
    closed: Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(test)]
impl MockStream {
    pub fn succeeding() -> Self {
        Self {
            next_error: std::sync::Mutex::new(None),
            closed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub fn failing(error: databricks_zerobus_ingest_sdk::ZerobusError) -> Self {
        Self {
            next_error: std::sync::Mutex::new(Some(error)),
            closed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Returns a shared handle to the closed flag for test assertions.
    pub fn closed_flag(&self) -> Arc<std::sync::atomic::AtomicBool> {
        Arc::clone(&self.closed)
    }

    /// Set the error that will be returned on the next ingest call.
    pub fn set_next_error(&self, error: databricks_zerobus_ingest_sdk::ZerobusError) {
        *self.next_error.lock().unwrap() = Some(error);
    }

    fn try_ingest(&self) -> Result<(), databricks_zerobus_ingest_sdk::ZerobusError> {
        match self.next_error.lock().unwrap().take() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

/// Schema, encoder, and stream-mode derived from the Unity Catalog table or
/// the path-based descriptor. Resolved lazily on first use.
pub(super) struct ResolvedSchema {
    encoder: BatchEncoder,
    stream_mode: StreamMode,
}

/// Service for handling Zerobus requests.
pub struct ZerobusService {
    sdk: Arc<ZerobusSdk>,
    config: Arc<ZerobusSinkConfig>,
    stream: Arc<Mutex<Option<Arc<ActiveStream>>>>,
    schema: Arc<OnceCell<ResolvedSchema>>,
    /// When true, the service waits for server-side acknowledgment after each
    /// ingest call. Derived from `AcknowledgementsConfig`.
    require_acknowledgements: bool,
}

impl ZerobusService {
    pub async fn new(
        config: ZerobusSinkConfig,
        require_acknowledgements: bool,
    ) -> Result<Self, ZerobusSinkError> {
        // Validate configuration
        config.validate()?;

        // Create SDK instance
        let sdk = ZerobusSdk::builder()
            .endpoint(&config.ingestion_endpoint)
            .unity_catalog_url(&config.unity_catalog_endpoint)
            .build()
            .map_err(|e| ZerobusSinkError::ConfigError {
                message: format!("Failed to create Zerobus SDK: {}", e),
            })?;

        Ok(Self {
            sdk: Arc::new(sdk),
            config: Arc::new(config),
            stream: Arc::new(Mutex::new(None)),
            schema: Arc::new(OnceCell::new()),
            require_acknowledgements,
        })
    }

    /// Resolve the protobuf message descriptor from the schema configuration.
    pub async fn resolve_descriptor(
        config: &ZerobusSinkConfig,
    ) -> Result<prost_reflect::MessageDescriptor, ZerobusSinkError> {
        match &config.schema {
            super::config::SchemaSource::Path { path, message_type } => {
                let path = Path::new(path);
                get_message_descriptor(path, message_type).map_err(|e| {
                    ZerobusSinkError::ConfigError {
                        message: format!("Failed to get message descriptor: {}", e),
                    }
                })
            }
            super::config::SchemaSource::UnityCatalog => {
                let (client_id, client_secret) = match &config.auth {
                    super::config::DatabricksAuthentication::OAuth {
                        client_id,
                        client_secret,
                    } => (client_id.inner(), client_secret.inner()),
                };

                let table_schema = unity_catalog_schema::fetch_table_schema(
                    &config.unity_catalog_endpoint,
                    &config.table_name,
                    client_id,
                    client_secret,
                )
                .await?;

                unity_catalog_schema::generate_descriptor_from_schema(&table_schema)
            }
        }
    }

    /// Resolve the schema on first use; cache the result.
    pub(super) async fn ensure_schema(&self) -> Result<&ResolvedSchema, ZerobusSinkError> {
        self.schema
            .get_or_try_init(|| async {
                let descriptor = Self::resolve_descriptor(&self.config).await?;

                let mut batch_encoding = self.config.batch_encoding.clone();
                let stream_mode = match &mut batch_encoding {
                    BatchSerializerConfig::ProtoBatch(config) => {
                        config.descriptor = Some(descriptor.clone());
                        StreamMode::Proto {
                            descriptor_proto: Arc::new(descriptor.descriptor_proto().clone()),
                        }
                    }
                    #[cfg(feature = "codecs-arrow")]
                    BatchSerializerConfig::ArrowStream(arrow_config) => {
                        let arrow_schema =
                            super::proto_to_arrow::proto_descriptor_to_arrow_schema(&descriptor)?;
                        arrow_config.schema = Some(arrow_schema.clone());
                        StreamMode::Arrow {
                            arrow_schema: Arc::new(arrow_schema),
                        }
                    }
                    #[cfg(feature = "codecs-arrow")]
                    BatchSerializerConfig::WireToArrow(config) => {
                        // `descriptor` from `resolve_descriptor` describes the *output*
                        // table shape and is used solely to derive the Arrow schema.
                        // The wire descriptor (for decoding incoming bytes) is loaded
                        // separately by the encoder from `batch_encoding.desc_file` +
                        // `batch_encoding.message_type` — under `SchemaSource::UnityCatalog`
                        // the UC-synthesized descriptor's field numbers don't match
                        // real wire tags, so the two descriptors must be distinct.
                        let arrow_schema =
                            super::proto_to_arrow::proto_descriptor_to_arrow_schema(&descriptor)?;
                        config.schema = Some(arrow_schema.clone());
                        StreamMode::Arrow {
                            arrow_schema: Arc::new(arrow_schema),
                        }
                    }
                };
                let batch_serializer =
                    batch_encoding
                        .build()
                        .map_err(|e| ZerobusSinkError::ConfigError {
                            message: format!("Failed to build batch serializer: {}", e),
                        })?;

                Ok(ResolvedSchema {
                    encoder: BatchEncoder::new(batch_serializer),
                    stream_mode,
                })
            })
            .await
    }

    /// Encode a batch of events into a `ZerobusPayload` plus its byte size.
    pub(super) fn encode_batch(
        schema: &ResolvedSchema,
        events: &[Event],
    ) -> Result<(ZerobusPayload, usize), ZerobusSinkError> {
        match schema
            .encoder
            .encode_batch(events)
            .map_err(|e| ZerobusSinkError::EncodingError {
                message: format!("Failed to encode batch: {}", e),
            })? {
            BatchOutput::Records(records) => {
                let size = records.iter().map(|r| r.len()).sum::<usize>();
                Ok((ZerobusPayload::Records(records), size))
            }
            #[cfg(feature = "codecs-arrow")]
            BatchOutput::Arrow(record_batch) => {
                let size = record_batch.get_array_memory_size();
                Ok((ZerobusPayload::Arrow(record_batch), size))
            }
        }
    }

    /// Ensure we have an active stream, creating one if necessary.
    ///
    /// Also used as the healthcheck: resolving the schema verifies the table
    /// and credentials against Unity Catalog, and creating the stream verifies
    /// connectivity to the Zerobus endpoint.
    pub async fn ensure_stream(&self) -> Result<(), ZerobusSinkError> {
        let schema = self.ensure_schema().await?;
        self.get_or_create_stream(schema).await.map(|_| ())
    }

    /// Return an `Arc` handle to the active stream, creating one if needed.
    ///
    /// The lock is held only while checking/creating the stream; callers can
    /// then use the returned `Arc` without holding the lock.
    async fn get_or_create_stream(
        &self,
        schema: &ResolvedSchema,
    ) -> Result<Arc<ActiveStream>, ZerobusSinkError> {
        let mut stream_guard = self.stream.lock().await;

        if stream_guard.is_none() {
            let (client_id, client_secret) = match &self.config.auth {
                super::config::DatabricksAuthentication::OAuth {
                    client_id,
                    client_secret,
                } => (
                    client_id.inner().to_string(),
                    client_secret.inner().to_string(),
                ),
            };

            let active_stream = match &schema.stream_mode {
                StreamMode::Proto { descriptor_proto } => {
                    let table_properties = TableProperties {
                        table_name: self.config.table_name.clone(),
                        descriptor_proto: Some((**descriptor_proto).clone()),
                    };
                    let stream_options = Some(self.config.stream_options.clone().into());
                    let stream = self
                        .sdk
                        .create_stream(table_properties, client_id, client_secret, stream_options)
                        .await
                        .map_err(|e| ZerobusSinkError::StreamInitError { source: e })?;
                    ActiveStream::Proto(stream)
                }
                #[cfg(feature = "codecs-arrow")]
                StreamMode::Arrow { arrow_schema } => {
                    // Log Arrow IPC schema size to help diagnose large-schema issues.
                    {
                        use arrow::ipc::writer::StreamWriter;
                        let mut buf = Vec::new();
                        if let Ok(mut w) = StreamWriter::try_new(&mut buf, arrow_schema) {
                            let _ = w.finish();
                        }
                        info!(
                            schema_fields = arrow_schema.fields().len(),
                            ipc_bytes = buf.len(),
                            "Arrow schema IPC size for stream setup"
                        );
                    }
                    let table_properties = ArrowTableProperties {
                        table_name: self.config.table_name.clone(),
                        schema: Arc::clone(arrow_schema),
                    };
                    let stream_options = Some(self.config.stream_options.clone().into());
                    let stream = self
                        .sdk
                        .create_arrow_stream(
                            table_properties,
                            client_id,
                            client_secret,
                            stream_options,
                        )
                        .await
                        .map_err(|e| ZerobusSinkError::StreamInitError { source: e })?;
                    ActiveStream::Arrow(stream)
                }
            };

            *stream_guard = Some(Arc::new(active_stream));
        }

        Ok(Arc::clone(stream_guard.as_ref().unwrap()))
    }

    /// Gracefully close and remove the active stream.
    ///
    /// Should be called after all in-flight ingests have completed (e.g.,
    /// after the driver returns) so that the slot holds the sole `Arc`
    /// reference to the stream.
    pub async fn close_stream(&self) {
        if let Some(stream) = self.stream.lock().await.take() {
            match Arc::try_unwrap(stream) {
                Ok(mut stream) => stream.close().await,
                Err(_) => {
                    warn!(
                        message =
                            "Zerobus stream has outstanding references, skipping graceful close."
                    );
                }
            }
        }
    }

    /// Send an encoded payload to an already-resolved stream.
    ///
    /// On retryable errors the active stream is removed from the slot so that
    /// the next attempt (driven by Tower retry) creates a fresh one.
    async fn ingest(
        &self,
        mut stream: Arc<ActiveStream>,
        payload: ZerobusPayload,
        events_byte_size: GroupedCountByteSize,
    ) -> Result<ZerobusResponse, ZerobusSinkError> {
        // Lock is not held here — other tasks can ingest concurrently.
        let result = match (payload, stream.as_ref()) {
            (ZerobusPayload::Records(records), ActiveStream::Proto(stream)) => {
                match stream.ingest_records_offset(records).await {
                    Ok(Some(offset)) if self.require_acknowledgements => {
                        stream.wait_for_offset(offset).await.map(|_| ())
                    }
                    Ok(_) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            #[cfg(feature = "codecs-arrow")]
            (ZerobusPayload::Arrow(record_batch), ActiveStream::Arrow(stream)) => {
                match stream.ingest_batch(record_batch).await {
                    Ok(offset) if self.require_acknowledgements => {
                        stream.wait_for_offset(offset).await.map(|_| ())
                    }
                    Ok(_) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            #[cfg(test)]
            (ZerobusPayload::Records(_), ActiveStream::Mock(mock)) => mock.try_ingest(),
            #[allow(unreachable_patterns)]
            _ => {
                return Err(ZerobusSinkError::ConfigError {
                    message: "Payload type does not match stream type".to_string(),
                });
            }
        };

        match result {
            Ok(()) => Ok(ZerobusResponse { events_byte_size }),
            Err(e) => {
                if e.is_retryable() {
                    // Remove the stream from the slot so the next retry creates a fresh one,
                    // then try to close gracefully. Dropping the slot's Arc first means our
                    // local `stream` may be the sole owner, allowing `Arc::get_mut` to succeed.
                    self.stream.lock().await.take();
                    if let Some(active) = Arc::get_mut(&mut stream) {
                        active.close().await;
                    }
                }
                Err(ZerobusSinkError::IngestionError { source: e })
            }
        }
    }
}

impl Service<ZerobusRequest> for ZerobusService {
    type Response = ZerobusResponse;
    type Error = ZerobusSinkError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut request: ZerobusRequest) -> Self::Future {
        let service = self.clone();
        let events_byte_size =
            std::mem::take(request.metadata_mut()).into_events_estimated_json_encoded_byte_size();

        Box::pin(async move {
            let schema = service.ensure_schema().await?;
            let (payload, _) = Self::encode_batch(schema, &request.events)?;
            let stream = service.get_or_create_stream(schema).await?;
            service.ingest(stream, payload, events_byte_size).await
        })
    }
}

impl Clone for ZerobusService {
    fn clone(&self) -> Self {
        Self {
            sdk: Arc::clone(&self.sdk),
            config: Arc::clone(&self.config),
            stream: Arc::clone(&self.stream),
            schema: Arc::clone(&self.schema),
            require_acknowledgements: self.require_acknowledgements,
        }
    }
}

/// Retry logic for the Zerobus service.
///
/// For SDK errors (`ZerobusError`), delegates to the SDK's `is_retryable()` which
/// correctly marks transient errors (stream closed, channel issues) as retriable
/// and permanent errors (invalid table name, invalid argument, invalid endpoint)
/// as non-retriable.
#[derive(Debug, Default, Clone)]
pub struct ZerobusRetryLogic;

#[cfg(test)]
impl ZerobusService {
    /// Create a service with a mock stream already installed for testing.
    pub async fn new_with_mock(
        config: ZerobusSinkConfig,
        mock: MockStream,
        require_acknowledgements: bool,
    ) -> Result<Self, ZerobusSinkError> {
        config.validate()?;

        let sdk = ZerobusSdk::builder()
            .endpoint(&config.ingestion_endpoint)
            .unity_catalog_url(&config.unity_catalog_endpoint)
            .build()
            .map_err(|e| ZerobusSinkError::ConfigError {
                message: format!("Failed to create Zerobus SDK: {}", e),
            })?;

        Ok(Self {
            sdk: Arc::new(sdk),
            config: Arc::new(config),
            stream: Arc::new(Mutex::new(Some(Arc::new(ActiveStream::Mock(mock))))),
            schema: Arc::new(OnceCell::new()),
            require_acknowledgements,
        })
    }

    /// Returns true if the service currently has an active stream.
    pub async fn has_active_stream(&self) -> bool {
        self.stream.lock().await.is_some()
    }
}

impl RetryLogic for ZerobusRetryLogic {
    type Error = ZerobusSinkError;
    type Request = ZerobusRequest;
    type Response = ZerobusResponse;

    fn is_retriable_error(&self, error: &Self::Error) -> bool {
        match error {
            ZerobusSinkError::ZerobusError { source }
            | ZerobusSinkError::StreamInitError { source }
            | ZerobusSinkError::IngestionError { source } => source.is_retryable(),
            ZerobusSinkError::ConfigError { .. } | ZerobusSinkError::EncodingError { .. } => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinks::databricks_zerobus::config::{
        DatabricksAuthentication, SchemaSource, ZerobusStreamOptions,
    };
    use databricks_zerobus_ingest_sdk::ZerobusError;
    use vector_lib::sensitive_string::SensitiveString;

    fn test_config() -> ZerobusSinkConfig {
        ZerobusSinkConfig {
            ingestion_endpoint: "https://127.0.0.1:1".to_string(),
            table_name: "test.default.logs".to_string(),
            unity_catalog_endpoint: "https://127.0.0.1:1".to_string(),
            auth: DatabricksAuthentication::OAuth {
                client_id: SensitiveString::from("id".to_string()),
                client_secret: SensitiveString::from("secret".to_string()),
            },
            schema: SchemaSource::Path {
                path: "tests/data/protobuf/test_proto.desc".to_string(),
                message_type: "test_proto.User".to_string(),
            },
            stream_options: ZerobusStreamOptions::default(),
            batch_encoding: vector_lib::codecs::encoding::BatchSerializerConfig::ProtoBatch(
                Default::default(),
            ),
            batch: Default::default(),
            request: Default::default(),
            acknowledgements: Default::default(),
        }
    }

    fn dummy_payload() -> ZerobusPayload {
        ZerobusPayload::Records(vec![vec![1, 2, 3]])
    }

    async fn current_stream(service: &ZerobusService) -> Arc<ActiveStream> {
        service.stream.lock().await.as_ref().unwrap().clone()
    }

    #[tokio::test]
    async fn ingest_succeeds_with_mock_stream() {
        let service = ZerobusService::new_with_mock(test_config(), MockStream::succeeding(), false)
            .await
            .unwrap();

        let stream = current_stream(&service).await;
        let result = service
            .ingest(
                stream,
                dummy_payload(),
                GroupedCountByteSize::new_untagged(),
            )
            .await;

        assert!(result.is_ok());
        assert!(service.has_active_stream().await);
    }

    #[tokio::test]
    async fn retryable_error_clears_stream() {
        let mock = MockStream::failing(ZerobusError::ChannelCreationError(
            "connection reset".to_string(),
        ));
        let service = ZerobusService::new_with_mock(test_config(), mock, false)
            .await
            .unwrap();

        assert!(service.has_active_stream().await);

        let stream = current_stream(&service).await;
        let err = service
            .ingest(
                stream,
                dummy_payload(),
                GroupedCountByteSize::new_untagged(),
            )
            .await
            .unwrap_err();

        assert!(matches!(err, ZerobusSinkError::IngestionError { .. }));
        assert!(ZerobusRetryLogic.is_retriable_error(&err));
        // Stream must have been cleared for the next retry.
        assert!(!service.has_active_stream().await);
    }

    #[tokio::test]
    async fn non_retryable_error_keeps_stream() {
        let mock = MockStream::failing(ZerobusError::InvalidArgument("bad field".to_string()));
        let service = ZerobusService::new_with_mock(test_config(), mock, false)
            .await
            .unwrap();

        assert!(service.has_active_stream().await);

        let stream = current_stream(&service).await;
        let err = service
            .ingest(
                stream,
                dummy_payload(),
                GroupedCountByteSize::new_untagged(),
            )
            .await
            .unwrap_err();

        assert!(matches!(err, ZerobusSinkError::IngestionError { .. }));
        assert!(!ZerobusRetryLogic.is_retriable_error(&err));
        // Stream should NOT be cleared for non-retryable errors.
        assert!(service.has_active_stream().await);
    }

    #[tokio::test]
    async fn stream_recovers_after_retryable_failure() {
        // Simulate: success → retryable failure → success again.
        let mock = MockStream::succeeding();
        let service = ZerobusService::new_with_mock(test_config(), mock, false)
            .await
            .unwrap();

        // First ingest succeeds.
        let stream = current_stream(&service).await;
        assert!(
            service
                .ingest(
                    stream,
                    dummy_payload(),
                    GroupedCountByteSize::new_untagged()
                )
                .await
                .is_ok()
        );
        assert!(service.has_active_stream().await);

        // Inject a retryable error for the next call.
        {
            let guard = service.stream.lock().await;
            if let Some(arc) = guard.as_ref() {
                if let ActiveStream::Mock(mock) = arc.as_ref() {
                    mock.set_next_error(ZerobusError::ChannelCreationError("reset".to_string()));
                }
            }
        }

        // Second ingest fails and clears the stream.
        let stream = current_stream(&service).await;
        let err = service
            .ingest(
                stream,
                dummy_payload(),
                GroupedCountByteSize::new_untagged(),
            )
            .await
            .unwrap_err();
        assert!(ZerobusRetryLogic.is_retriable_error(&err));
        assert!(!service.has_active_stream().await);

        // Simulate Tower retry: re-inject a fresh mock stream
        // (in production, ensure_stream() would create a new real stream).
        *service.stream.lock().await = Some(Arc::new(ActiveStream::Mock(MockStream::succeeding())));

        // Third ingest succeeds on the new stream.
        let stream = current_stream(&service).await;
        assert!(
            service
                .ingest(
                    stream,
                    dummy_payload(),
                    GroupedCountByteSize::new_untagged()
                )
                .await
                .is_ok()
        );
        assert!(service.has_active_stream().await);
    }

    #[tokio::test]
    async fn close_stream_calls_close_on_active_stream() {
        let mock = MockStream::succeeding();
        let closed = mock.closed_flag();

        let service = ZerobusService::new_with_mock(test_config(), mock, false)
            .await
            .unwrap();

        assert!(service.has_active_stream().await);
        assert!(!closed.load(std::sync::atomic::Ordering::Relaxed));

        service.close_stream().await;

        assert!(!service.has_active_stream().await);
        assert!(closed.load(std::sync::atomic::Ordering::Relaxed));
    }
}
