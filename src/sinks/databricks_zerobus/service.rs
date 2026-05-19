//! Zerobus service wrapper for Vector sink integration.

use databricks_zerobus_ingest_sdk::{ZerobusArrowStream, ZerobusSdk};
use futures::future::BoxFuture;
use std::sync::Arc;
use tokio::sync::{Mutex, OnceCell, RwLock};
use tower::Service;
use tracing::{info, warn};
use vector_lib::codecs::encoding::{ArrowStreamSerializer, BatchSerializerConfig};
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
///
/// The SDK's `ZerobusArrowStream::close()` requires `&mut self`, but ingests need
/// shared access to call `&self` methods concurrently. We resolve this with an
/// `RwLock`: ingests hold a read guard across `ingest_batch`, and `close()` takes
/// the write guard, pulls the stream out of the `Option`, and awaits its
/// SDK-level close on the owned value. Any holder of an `Arc` can invoke
/// `close()`, so the graceful path always runs — there is no
/// `try_unwrap`/`get_mut` race.
enum ActiveStream {
    Arrow(RwLock<Option<Box<ZerobusArrowStream>>>),
    /// Test-only variant that returns a pre-configured error on ingest.
    #[cfg(test)]
    Mock(MockStream),
}

impl ActiveStream {
    fn arrow(stream: ZerobusArrowStream) -> Self {
        ActiveStream::Arrow(RwLock::new(Some(Box::new(stream))))
    }

    /// Gracefully flush and close the underlying SDK stream.
    ///
    /// Waits for any in-flight ingests (read-lock holders) to complete, then
    /// pulls the stream out of the slot and runs the SDK's awaitable `close()`
    /// on the owned value (released-lock so further ingests fail fast with
    /// `StreamClosed` rather than blocking).
    ///
    /// Idempotent: a second call after the stream has been taken is a no-op.
    /// The SDK's own `Drop` is also a no-op once close has run.
    async fn close(&self) {
        let result = match self {
            ActiveStream::Arrow(lock) => {
                let taken = lock.write().await.take();
                match taken {
                    Some(mut stream) => stream.close().await,
                    None => return,
                }
            }
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
    /// Optional gate: when set, each ingest call signals `started` and then
    /// waits to acquire a `release` permit before returning. Lets tests
    /// deterministically force two ingests to overlap (each holding an `Arc`
    /// clone of the `ActiveStream`) before they fail.
    gate: Option<MockGate>,
}

#[cfg(test)]
struct MockGate {
    started: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
}

#[cfg(test)]
#[derive(Clone)]
pub struct MockGateHandle {
    started: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
}

#[cfg(test)]
impl MockGateHandle {
    /// Wait until `n` ingests have entered the gated region.
    pub async fn wait_for_started(&self, n: u32) {
        let permit = self.started.acquire_many(n).await.unwrap();
        permit.forget();
    }

    /// Release `n` queued ingests so they can return their result.
    pub fn release(&self, n: u32) {
        self.release.add_permits(n as usize);
    }
}

#[cfg(test)]
impl MockStream {
    pub fn succeeding() -> Self {
        Self {
            next_error: std::sync::Mutex::new(None),
            closed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            gate: None,
        }
    }

    pub fn failing(error: databricks_zerobus_ingest_sdk::ZerobusError) -> Self {
        Self {
            next_error: std::sync::Mutex::new(Some(error)),
            closed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            gate: None,
        }
    }

    /// Install a gate so ingests block until the test releases them.
    /// Returns a handle the test uses to coordinate.
    pub fn with_gate(mut self) -> (Self, MockGateHandle) {
        let started = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        self.gate = Some(MockGate {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
        });
        (self, MockGateHandle { started, release })
    }

    /// Returns a shared handle to the closed flag for test assertions.
    pub fn closed_flag(&self) -> Arc<std::sync::atomic::AtomicBool> {
        Arc::clone(&self.closed)
    }

    /// Set the error that will be returned on the next ingest call.
    pub fn set_next_error(&self, error: databricks_zerobus_ingest_sdk::ZerobusError) {
        *self.next_error.lock().unwrap() = Some(error);
    }

    async fn try_ingest(&self) -> Result<(), databricks_zerobus_ingest_sdk::ZerobusError> {
        if let Some(gate) = &self.gate {
            gate.started.add_permits(1);
            // Acquire and immediately forget — we don't need to release the
            // permit on drop, the test's `release()` call hands them out.
            gate.release.acquire().await.unwrap().forget();
        }
        match self.next_error.lock().unwrap().take() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

/// Arrow serializer + schema derived from the Unity Catalog table.
/// Resolved lazily on first use.
pub(super) struct ResolvedSchema {
    serializer: ArrowStreamSerializer,
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
                let BatchSerializerConfig::ArrowStream(mut arrow_config) =
                    self.config.batch_encoding.clone();
                let arrow_schema = Self::resolve_arrow_schema(&self.config).await?;
                arrow_config.schema = Some(arrow_schema.clone());
                let arrow_schema = Arc::new(arrow_schema);

                let serializer =
                    ArrowStreamSerializer::new(arrow_config).map_err(|e| {
                        ZerobusSinkError::ConfigError {
                            message: format!("Failed to build Arrow serializer: {}", e),
                        }
                    })?;

                Ok(ResolvedSchema {
                    serializer,
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
                .ipc_compression(stream_options.compression.map(Into::into))
                .build_arrow()
                .await
                .map_err(|e| ZerobusSinkError::StreamInitError { source: e })?;

            *stream_guard = Some(Arc::new(ActiveStream::arrow(stream)));
        }

        Ok(Arc::clone(stream_guard.as_ref().unwrap()))
    }

    /// Gracefully close and remove the active stream.
    ///
    /// `ActiveStream::close()` takes `&self`, so this works regardless of how
    /// many `Arc` clones are still in flight: the inner write lock waits for
    /// any concurrent ingests to release their read guards before the SDK
    /// flush + close runs. The slot lock is released before close starts so
    /// concurrent `get_or_create_stream` calls aren't blocked on the SDK
    /// shutdown path.
    pub async fn close_stream(&self) {
        let stream = self.stream.lock().await.take();
        if let Some(stream) = stream {
            stream.close().await;
        }
    }

    /// Send an encoded payload to an already-resolved stream.
    ///
    /// Holds a read guard on the inner `RwLock` for the duration of the SDK
    /// call so concurrent ingests can run in parallel; a concurrent `close()`
    /// will wait for the read guards to drain before flushing.
    ///
    /// On retryable errors the active stream is removed from the slot so that
    /// the next attempt (driven by Tower retry) creates a fresh one.
    async fn ingest(
        &self,
        stream: Arc<ActiveStream>,
        payload: ZerobusPayload,
        events_byte_size: GroupedCountByteSize,
    ) -> Result<ZerobusResponse, ZerobusSinkError> {
        let ZerobusPayload(record_batch) = payload;
        let result = match stream.as_ref() {
            ActiveStream::Arrow(lock) => {
                let guard = lock.read().await;
                let Some(s) = guard.as_ref() else {
                    return Err(ZerobusSinkError::StreamClosed);
                };
                match s.ingest_batch(record_batch).await {
                    Ok(offset) if self.require_acknowledgements => {
                        s.wait_for_offset(offset).await.map(|_| ())
                    }
                    Ok(_) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            #[cfg(test)]
            ActiveStream::Mock(mock) => mock.try_ingest().await,
        };

        match result {
            Ok(()) => Ok(ZerobusResponse { events_byte_size }),
            Err(e) => {
                if e.is_retryable() {
                    // Clear the slot so the next attempt creates a fresh stream,
                    // but only if it still points to the same stream that failed —
                    // a concurrent task may have already replaced it.
                    {
                        let mut guard = self.stream.lock().await;
                        if guard.as_ref().is_some_and(|s| Arc::ptr_eq(s, &stream)) {
                            guard.take();
                        }
                    }
                    // `close()` takes `&self`, so we can always run the graceful
                    // path here regardless of how many other `Arc` clones are in
                    // flight. The write lock will wait for any concurrent ingests
                    // holding read guards to drain before flushing.
                    stream.close().await;
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
            let record_batch = schema.serializer.encode_to_record_batch(&request.events)
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
            ZerobusSinkError::StreamClosed => true,
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

    /// Regression test for the "silent abort-only Drop" issue: when two
    /// ingests are in flight (each holding an `Arc<ActiveStream>`) and one
    /// fails retryably, the failing task must still run the graceful close
    /// path. Under the previous design `Arc::get_mut` returned `None` here
    /// because the second task held a clone, so close was skipped and the
    /// stream fell to abort-only Drop.
    #[tokio::test]
    async fn retryable_failure_with_concurrent_ingest_still_closes() {
        let (mock, gate) = MockStream::failing(ZerobusError::ChannelCreationError(
            "connection reset".to_string(),
        ))
        .with_gate();
        let closed = mock.closed_flag();

        let service = ZerobusService::new_with_mock(test_config(), mock, false)
            .await
            .unwrap();

        // Spawn two concurrent ingests. Each takes its own `Arc` clone of the
        // active stream, then blocks in the gate.
        let s1 = service.clone();
        let stream1 = current_stream(&service).await;
        let t1 = tokio::spawn(async move {
            s1.ingest(stream1, dummy_payload(), GroupedCountByteSize::new_untagged())
                .await
        });
        let s2 = service.clone();
        let stream2 = current_stream(&service).await;
        let t2 = tokio::spawn(async move {
            s2.ingest(stream2, dummy_payload(), GroupedCountByteSize::new_untagged())
                .await
        });

        // Wait until both ingests are inside the gate (both `Arc`s alive).
        gate.wait_for_started(2).await;

        // Release both. The failing one will go through the retry-cleanup
        // path while the other still holds an `Arc`. Under the old design
        // `Arc::get_mut` would return `None` and close would be skipped.
        gate.release(2);

        let r1 = t1.await.unwrap();
        let r2 = t2.await.unwrap();

        // At least one task observed the retryable error (the mock only
        // produces a single error, but ordering between tasks is undefined).
        assert!(r1.is_err() || r2.is_err());

        // The graceful close path must have run despite concurrent `Arc`s.
        assert!(
            closed.load(std::sync::atomic::Ordering::Relaxed),
            "graceful close did not run; stream would have leaked under old design"
        );
        // And the slot was cleared so the next ingest creates a fresh stream.
        assert!(!service.has_active_stream().await);
    }
}
