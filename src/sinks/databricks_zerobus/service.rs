//! Zerobus service wrapper for Vector sink integration.

use databricks_zerobus_ingest_sdk::{ZerobusArrowStream, ZerobusSdk};
use futures::future::BoxFuture;
use std::sync::Arc;
use tokio::sync::{Mutex, OnceCell};
use tower::Service;
use tracing::{info, warn};
use vector_lib::codecs::encoding::{BatchEncoder, BatchOutput, BatchSerializerConfig};
use vector_lib::finalization::{EventFinalizers, Finalizable};
use vector_lib::request_metadata::{GroupedCountByteSize, MetaDescriptive, RequestMetadata};
use vector_lib::stream::DriverResponse;
use crate::event::Event;
use crate::sinks::util::retries::RetryLogic;

use super::{config::ZerobusSinkConfig, error::ZerobusSinkError, unity_catalog_schema};

/// The payload for a Zerobus request: an Arrow `RecordBatch` for Arrow Flight ingestion.
#[derive(Clone, Debug)]
pub struct ZerobusPayload(pub arrow::record_batch::RecordBatch);

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

/// The active Arrow Flight stream.
enum ActiveStream {
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

/// Arrow serializer + schema derived from the Unity Catalog table.
/// Resolved lazily on first use.
pub(super) struct ResolvedSchema {
    encoder: BatchEncoder,
    arrow_schema: Arc<arrow::datatypes::Schema>,
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

    /// Resolve the Arrow schema for the configured Unity Catalog table.
    pub async fn resolve_arrow_schema(
        config: &ZerobusSinkConfig,
    ) -> Result<arrow::datatypes::Schema, ZerobusSinkError> {
        match &config.schema {
            super::config::SchemaSource::Path { .. } => Err(ZerobusSinkError::ConfigError {
                message: "schema.type=\"path\" is no longer supported; use \"unity_catalog\""
                    .to_string(),
            }),
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

                databricks_zerobus_ingest_sdk::schema::arrow_schema_from_uc_schema(
                    &table_schema.to_sdk_uc_schema(),
                )
                .map_err(|e| ZerobusSinkError::ConfigError {
                    message: format!("Failed to convert UC schema to Arrow: {}", e),
                })
            }
        }
    }

    /// Resolve the schema on first use; cache the result.
    pub(super) async fn ensure_schema(&self) -> Result<&ResolvedSchema, ZerobusSinkError> {
        self.schema
            .get_or_try_init(|| async {
                let arrow_schema = Self::resolve_arrow_schema(&self.config).await?;
                let mut batch_encoding = self.config.batch_encoding.clone();
                match &mut batch_encoding {
                    BatchSerializerConfig::ArrowStream(config) => {
                        config.schema = Some(arrow_schema.clone());
                    }
                    BatchSerializerConfig::WireToArrow(config) => {
                        // The Arrow schema describes the *output* table shape.
                        // The wire descriptor (for decoding incoming bytes) is
                        // loaded separately by the encoder from
                        // `batch_encoding.desc_file` + `batch_encoding.message_type`.
                        config.schema = Some(arrow_schema.clone());
                    }
                }
                let arrow_schema = Arc::new(arrow_schema);

                let batch_serializer =
                    batch_encoding
                        .build()
                        .map_err(|e| ZerobusSinkError::ConfigError {
                            message: format!("Failed to build batch serializer: {}", e),
                        })?;

                Ok(ResolvedSchema {
                    encoder: BatchEncoder::new(batch_serializer),
                    arrow_schema,
                })
            })
            .await
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

            let stream_options = &self.config.stream_options;
            let arrow_schema = &schema.arrow_schema;
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
            let stream = self
                .sdk
                .stream_builder()
                .table(self.config.table_name.clone())
                .oauth(client_id, client_secret)
                .arrow(Arc::clone(arrow_schema))
                .recovery(true)
                .recovery_retries(4)
                .server_lack_of_ack_timeout_ms(stream_options.server_lack_of_ack_timeout_ms)
                .flush_timeout_ms(stream_options.flush_timeout_ms)
                .build_arrow()
                .await
                .map_err(|e| ZerobusSinkError::StreamInitError { source: e })?;

            *stream_guard = Some(Arc::new(ActiveStream::Arrow(stream)));
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
        let ZerobusPayload(record_batch) = payload;
        let result = match stream.as_ref() {
            ActiveStream::Arrow(stream) => match stream.ingest_batch(record_batch).await {
                Ok(offset) if self.require_acknowledgements => {
                    stream.wait_for_offset(offset).await.map(|_| ())
                }
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            },
            #[cfg(test)]
            ActiveStream::Mock(mock) => mock.try_ingest(),
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
            let BatchOutput::Arrow(record_batch) = schema
                .encoder
                .encode_batch(&request.events)
                .map_err(|e| ZerobusSinkError::EncodingError {
                    message: format!("Failed to encode batch: {}", e),
                })?;
            let payload = ZerobusPayload(record_batch);
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
            schema: SchemaSource::UnityCatalog,
            stream_options: ZerobusStreamOptions::default(),
            batch_encoding: vector_lib::codecs::encoding::BatchSerializerConfig::ArrowStream(
                Default::default(),
            ),
            batch: Default::default(),
            request: Default::default(),
            acknowledgements: Default::default(),
        }
    }

    fn dummy_payload() -> ZerobusPayload {
        use arrow::datatypes::Schema;
        ZerobusPayload(
            arrow::record_batch::RecordBatch::new_empty(Arc::new(Schema::empty())),
        )
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
