//! Zerobus service wrapper for Vector sink integration.

use crate::config::ProxyConfig;
use crate::databricks_auth::{LoginServiceHeadersProvider, TokenManager};
use crate::event::Event;
use crate::http::HttpClient;
use crate::internal_events::{
    SchemaReloadOutcome, ZerobusRetryableError, ZerobusSchemaRefetchFailed,
    ZerobusSchemaReloadOutcome,
};
use crate::sinks::util::retries::RetryLogic;
use crate::tls::TlsSettings;
use databricks_zerobus_ingest_sdk::{
    ConnectorFactory, HeadersProvider, MultiplexedArrowStream, OffsetId, ProxyConnector,
    StatsExporter, StreamStat, ZerobusArrowStream, ZerobusError, ZerobusSdk,
};
use futures::future::BoxFuture;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tower::{Layer, Service, timeout::error::Elapsed};
use tracing::{info, warn};
use vector_lib::codecs::encoding::{BatchEncoder, BatchOutput, BatchSerializerConfig};
use vector_lib::finalization::{EventFinalizers, Finalizable};
use vector_lib::internal_event::CodedError;
use vector_lib::request_metadata::{GroupedCountByteSize, MetaDescriptive, RequestMetadata};
use vector_lib::stream::DriverResponse;

use super::{
    config::ZerobusSinkConfig, error::ZerobusSinkError, freshness, headers,
    ingest_tls::ingestion_tls_settings, uc_client::UnityCatalogClient, unity_catalog_schema,
};

/// Build a connector factory that routes Zerobus gRPC traffic through
/// Vector's configured proxy, honoring `no_proxy` rules.
///
/// The Zerobus endpoint is always HTTPS gRPC, so the `https` proxy is
/// preferred; the `http` proxy is used as a fallback if only that is set.
/// The returned factory fully replaces the SDK's default env-var proxy
/// detection — Vector's `ProxyConfig` has already merged the process
/// environment at a higher layer and is the single source of truth.
///
/// When proxying is disabled or no proxy URL is configured, returns a
/// factory that unconditionally yields `None`, forcing direct connections.
/// Returns an error if the configured proxy URL is malformed, so the
/// problem surfaces at sink startup rather than per-connection.
fn build_connector_factory(proxy: &ProxyConfig) -> Result<ConnectorFactory, ZerobusSinkError> {
    let proxy_url = if proxy.enabled {
        proxy.https.clone().or_else(|| proxy.http.clone())
    } else {
        None
    };
    let Some(proxy_url) = proxy_url else {
        return Ok(Arc::new(|_host: &str| None));
    };
    // Validate the proxy URL once up-front so a malformed value surfaces at
    // sink startup rather than per-connection.
    ProxyConnector::new(&proxy_url).map_err(|e| ZerobusSinkError::ConfigError {
        message: format!("Invalid proxy URL '{}': {}", proxy_url, e),
    })?;
    let no_proxy = proxy.no_proxy.clone();
    Ok(Arc::new(move |host: &str| {
        if no_proxy.matches(host) {
            return None;
        }
        ProxyConnector::new(&proxy_url).ok()
    }))
}

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
///
/// Carries the final `EventStatus` so the driver can mark finalizers correctly:
/// `Delivered` on success, `Errored` when the retry budget was exhausted on a
/// transient failure (asking the source / disk buffer to replay), and `Err`
/// from `Service::call` reserved for permanent failures (driver maps to
/// `Rejected`).
#[derive(Debug)]
pub struct ZerobusResponse {
    pub events_byte_size: GroupedCountByteSize,
    pub status: vector_lib::event::EventStatus,
    /// Approximate wire bytes of the acknowledged batch, as reported by the
    /// SDK's stream stats. `None` when the SDK emitted no stats for the batch.
    pub bytes_sent: Option<usize>,
}

impl ZerobusResponse {
    const fn delivered(events_byte_size: GroupedCountByteSize, bytes_sent: Option<usize>) -> Self {
        Self {
            events_byte_size,
            status: vector_lib::event::EventStatus::Delivered,
            bytes_sent,
        }
    }

    /// Synthesize a response signalling a transient failure that exhausted the
    /// retry budget. Carries a telemetry-aware zero `events_byte_size` because
    /// the driver only consumes `events_sent()` on the `Delivered` path.
    fn errored() -> Self {
        Self {
            events_byte_size: vector_lib::config::telemetry().create_request_count_byte_size(),
            status: vector_lib::event::EventStatus::Errored,
            bytes_sent: None,
        }
    }
}

impl DriverResponse for ZerobusResponse {
    fn event_status(&self) -> vector_lib::event::EventStatus {
        self.status
    }

    fn events_sent(&self) -> &GroupedCountByteSize {
        &self.events_byte_size
    }

    fn bytes_sent(&self) -> Option<usize> {
        self.bytes_sent
    }
}

/// Collects each batch's approximate wire size from the SDK's stream stats so
/// `ingest` can report it once the batch is acknowledged. The driver only
/// emits `bytes_sent` for delivered responses, so failed batches aren't counted.
///
/// `record` runs inline on the SDK's IO tasks; it only does a map insert.
#[derive(Default)]
struct WireBytesExporter {
    bytes_by_offset: std::sync::Mutex<HashMap<OffsetId, u64>>,
}

impl WireBytesExporter {
    /// Remove and return the recorded wire bytes for `offset`.
    fn take(&self, offset: OffsetId) -> Option<usize> {
        self.bytes_by_offset
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&offset)
            .map(|bytes| bytes as usize)
    }
}

impl StatsExporter for WireBytesExporter {
    fn record(&self, stat: StreamStat) {
        if let StreamStat::BatchSent { offset, stats, .. } = stat {
            // Keep the first transmission so a recovery replay isn't counted twice.
            // If the first reported send is a partial retry (only the unacked
            // suffix), the batch is undercounted.
            self.bytes_by_offset
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(offset)
                .or_insert(stats.approximate_wire_bytes);
        }
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
    Arrow(
        RwLock<Option<Box<ZerobusArrowStream>>>,
        Arc<WireBytesExporter>,
    ),
    /// Several Arrow Flight lanes behind one handle, used when `multiplexed_streams > 1`.
    ///
    /// The SDK's multiplexed stream rejects a stats exporter, so batches sent through it
    /// report no wire size.
    ArrowMux(RwLock<Option<Box<MultiplexedArrowStream>>>),
    /// Test-only variant that returns a pre-configured error on ingest.
    #[cfg(test)]
    Mock(MockStream),
}

impl ActiveStream {
    fn arrow(stream: ZerobusArrowStream, stats: Arc<WireBytesExporter>) -> Self {
        ActiveStream::Arrow(RwLock::new(Some(Box::new(stream))), stats)
    }

    fn arrow_mux(stream: MultiplexedArrowStream) -> Self {
        ActiveStream::ArrowMux(RwLock::new(Some(Box::new(stream))))
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
            ActiveStream::Arrow(lock, _) => {
                let taken = lock.write().await.take();
                match taken {
                    Some(mut stream) => stream.close().await,
                    None => return,
                }
            }
            ActiveStream::ArrowMux(lock) => {
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

/// Arrow serializer + schema derived from the Unity Catalog table, resolved
/// lazily on first use.
///
/// Immutable: a reload installs a new value rather than mutating this one, so an
/// in-flight ingest keeps encoding against the generation it started with and
/// `Arc` identity serves as the generation marker.
pub(super) struct ResolvedSchema {
    encoder: BatchEncoder,
    arrow_schema: Arc<arrow::datatypes::Schema>,
}

/// The active stream and the schema generation it was created for, compared by
/// `Arc` identity.
///
/// A stream's shape is fixed at creation, and the SDK rejects a foreign-shaped
/// batch client-side with a non-retryable `InvalidArgument` that never reaches
/// the reload path. So a request still encoding an older generation must not be
/// handed a newer stream.
struct PinnedStream {
    schema: Arc<ResolvedSchema>,
    stream: Arc<ActiveStream>,
}

/// Service for handling Zerobus requests.
pub struct ZerobusService {
    sdk: Arc<ZerobusSdk>,
    config: Arc<ZerobusSinkConfig>,
    http_client: HttpClient,
    stream: Arc<Mutex<Option<PinnedStream>>>,
    /// Cached Arrow schema + encoder for the target table.
    ///
    /// A `RwLock` rather than a `OnceCell` so the cache can be *replaced*: the
    /// Unity Catalog table can be widened (or narrowed) while the sink runs,
    /// and a write-once cell can only be refreshed by restarting the process.
    /// Replaced by `reload_schema_after_rejection` when the server rejects the
    /// cached shape as stale.
    schema: Arc<RwLock<Option<Arc<ResolvedSchema>>>>,
    /// Serializes schema *resolution* (not reads), so a burst of concurrent
    /// cache misses — or a fleet-wide rejection failing every in-flight batch at
    /// once — issues a single Unity Catalog fetch instead of one per batch. Held
    /// across the fetch await deliberately; the losers re-check the cache and
    /// reuse the winner's result.
    schema_resolve: Arc<Mutex<()>>,
    /// Optional token manager for Login service auth.
    token_manager: Option<Arc<TokenManager>>,
    unity_catalog_client: Option<Arc<UnityCatalogClient>>,
    /// OAuth headers provider wrapping the SDK's, built once when `custom_headers`
    /// is set on the OAuth strategy. `None` otherwise, in which case `build_stream`
    /// uses the SDK's own `builder.oauth(...)` unchanged.
    oauth_provider: Option<Arc<dyn HeadersProvider>>,
    /// Configured static headers, leaked once (`&'static`) so both auth
    /// strategies' providers can hold a `Copy` reference with no clone. Merged
    /// into every request. Empty when none set.
    custom_headers: &'static HashMap<&'static str, String>,
    /// Test-only stand-in for the Unity Catalog fetch, which unit tests cannot
    /// perform. When set, `resolve_schema` pops the next schema from this queue
    /// instead of calling out to UC, letting a test script the sequence of
    /// shapes a reload observes.
    #[cfg(test)]
    schema_fetch_results:
        Arc<std::sync::Mutex<std::collections::VecDeque<arrow::datatypes::Schema>>>,
}

impl ZerobusService {
    pub async fn new(
        config: ZerobusSinkConfig,
        proxy: &ProxyConfig,
    ) -> Result<Self, ZerobusSinkError> {
        let mut builder = ZerobusSdk::builder()
            .endpoint(&config.ingestion_endpoint)
            .unity_catalog_url(&config.unity_catalog_endpoint)
            .application_name(config.user_agent_suffix());
        builder = builder.connector_factory(build_connector_factory(proxy)?);
        let tls = match &config.auth {
            super::config::DatabricksAuthentication::LoginService(auth) => Some(&auth.tls),
            super::config::DatabricksAuthentication::OAuth { .. } => None,
        };
        builder = builder.tls_config(Arc::new(ingestion_tls_settings(
            tls,
            config.ingestion_domain_name.as_deref(),
        )?));
        let sdk = builder.build().map_err(|e| ZerobusSinkError::ConfigError {
            message: format!("Failed to create Zerobus SDK: {}", e),
        })?;

        let http_client = HttpClient::new(TlsSettings::default(), proxy).map_err(|e| {
            ZerobusSinkError::ConfigError {
                message: format!("Failed to create HTTP client: {}", e),
            }
        })?;

        // Initialize token manager for Login service auth. The first call to
        // `LoginServiceHeadersProvider::get_headers` (during the healthcheck stream-create
        // below) triggers the initial bootstrap. `TokenManager` re-bootstraps on demand when
        // the cached principal contexts near expiry, so no background refresh loop is needed.
        //
        // `TokenManager::new` is async because it loads the principal-context proto
        // FileDescriptorSet from disk at startup (mounted into the container) — vector carries
        // no snapshot of the proto via build-time codegen. See databricks_auth::TokenManager.
        let token_manager = match &config.auth {
            super::config::DatabricksAuthentication::LoginService(auth_config) => {
                let tm = TokenManager::new(auth_config.clone()).await.map_err(|e| {
                    ZerobusSinkError::ConfigError {
                        message: format!("Failed to initialize TokenManager: {}", e),
                    }
                })?;
                Some(Arc::new(tm))
            }
            _ => None,
        };

        let unity_catalog_client = match (&config.auth, &token_manager) {
            (super::config::DatabricksAuthentication::LoginService(auth), Some(manager)) => {
                Some(Arc::new(
                    UnityCatalogClient::new(
                        &config.unity_catalog_endpoint,
                        auth,
                        Arc::clone(manager),
                    )
                    .await?,
                ))
            }
            _ => None,
        };

        let custom_headers = headers::leak_custom_headers(&config);
        let oauth_provider = headers::build_oauth_provider(&config, custom_headers);

        Ok(Self {
            sdk: Arc::new(sdk),
            config: Arc::new(config),
            http_client,
            stream: Arc::new(Mutex::new(None)),
            schema: Arc::new(RwLock::new(None)),
            schema_resolve: Arc::new(Mutex::new(())),
            token_manager,
            unity_catalog_client,
            oauth_provider,
            custom_headers,
            #[cfg(test)]
            schema_fetch_results: Arc::new(std::sync::Mutex::new(Default::default())),
        })
    }

    /// Resolve the Arrow schema for the configured Unity Catalog table.
    async fn resolve_arrow_schema(
        config: &ZerobusSinkConfig,
        http_client: &HttpClient,
        unity_catalog_client: Option<&Arc<UnityCatalogClient>>,
    ) -> Result<arrow::datatypes::Schema, ZerobusSinkError> {
        match &config.schema {
            super::config::SchemaSource::Path { .. } => Err(ZerobusSinkError::ConfigError {
                message: "schema.type=\"path\" is no longer supported; use \"unity_catalog\""
                    .to_string(),
            }),
            super::config::SchemaSource::UnityCatalog => {
                let uc_schema = match &config.auth {
                    super::config::DatabricksAuthentication::OAuth {
                        client_id,
                        client_secret,
                    } => unity_catalog_schema::fetch_table_schema(
                        &config.unity_catalog_endpoint,
                        &config.table_name,
                        client_id.inner(),
                        client_secret.inner(),
                        http_client,
                    )
                    .await?
                    .to_sdk_uc_schema(),
                    super::config::DatabricksAuthentication::LoginService(_) => {
                        unity_catalog_client
                            .ok_or_else(|| ZerobusSinkError::ConfigError {
                                message:
                                    "LoginService auth requires an internal Unity Catalog client"
                                        .to_string(),
                            })?
                            .get_table_schema(&config.table_name)
                            .await?
                    }
                };

                // Mark VARIANT columns with `arrow.parquet.variant` so the batch
                // encoders can detect them; without the marker they'd write all-null.
                let mut options =
                    databricks_zerobus_ingest_sdk::schema::ArrowSchemaOptions::default();
                options.annotate_variant_extension = true;
                databricks_zerobus_ingest_sdk::schema::arrow_schema_from_uc_schema_with_options(
                    &uc_schema, &options,
                )
                .map_err(|e| ZerobusSinkError::ConfigError {
                    message: format!("Failed to convert UC schema to Arrow: {}", e),
                })
            }
        }
    }

    /// Resolve the schema on first use; cache the result.
    ///
    /// Returns an `Arc` so the caller keeps a stable view for the whole request
    /// even if a concurrent reload replaces the cache mid-flight.
    pub(super) async fn ensure_schema(&self) -> Result<Arc<ResolvedSchema>, ZerobusSinkError> {
        // Fast path: a resolved schema is already cached.
        if let Some(schema) = self.schema.read().await.as_ref() {
            return Ok(Arc::clone(schema));
        }

        // Cache miss. Take the resolve lock so concurrent misses collapse into
        // one Unity Catalog fetch, then re-check: the winner of the race has
        // already populated the cache by the time the losers get here.
        let _resolve_guard = self.schema_resolve.lock().await;
        if let Some(schema) = self.schema.read().await.as_ref() {
            return Ok(Arc::clone(schema));
        }

        let resolved = Arc::new(self.resolve_schema().await?);
        *self.schema.write().await = Some(Arc::clone(&resolved));
        Ok(resolved)
    }

    /// Re-resolve the table schema after the server rejected the cached one as
    /// stale, and drop the rejected stream so the next attempt builds a fresh
    /// one against the new shape.
    ///
    /// `rejected` is the schema generation the failing request used. It is
    /// compared against the newly fetched one for two reasons:
    ///
    /// - If another task already reloaded (the cache no longer holds
    ///   `rejected`), this call piggybacks on that work rather than issuing a
    ///   second fetch. A fleet-wide envelope change makes every in-flight batch
    ///   fail at once, so the reload must collapse.
    /// - If the fetch returns the *same* shape the server just rejected, the
    ///   reload made no progress. Reporting success there would spin
    ///   (reload -> same schema -> reject -> reload) against a table Unity
    ///   Catalog and the ingestion server disagree about, so it is refused.
    ///
    /// Returns whether the cache now holds a schema different from `rejected`.
    async fn reload_schema_after_rejection(
        &self,
        rejected: &Arc<ResolvedSchema>,
    ) -> Result<bool, ZerobusSinkError> {
        let _resolve_guard = self.schema_resolve.lock().await;

        // Someone else reloaded while we waited for the lock. Counted as a
        // reload: this batch does get retried against a new shape, and the
        // fetch it piggybacked on reported its own outcome.
        if let Some(current) = self.schema.read().await.as_ref()
            && !Arc::ptr_eq(current, rejected)
        {
            emit!(ZerobusSchemaReloadOutcome {
                outcome: SchemaReloadOutcome::Reloaded,
            });
            return Ok(true);
        }

        let resolved = self.resolve_schema().await?;
        if resolved.arrow_schema == rejected.arrow_schema {
            emit!(ZerobusSchemaReloadOutcome {
                outcome: SchemaReloadOutcome::NoProgress,
            });
            return Ok(false);
        }

        *self.schema.write().await = Some(Arc::new(resolved));
        emit!(ZerobusSchemaReloadOutcome {
            outcome: SchemaReloadOutcome::Reloaded,
        });
        Ok(true)
    }

    /// Fetch the table schema from Unity Catalog and build the matching batch
    /// encoder. Does not touch the cache — callers decide where the result goes.
    async fn resolve_schema(&self) -> Result<ResolvedSchema, ZerobusSinkError> {
        // Bind the pop to a local first: holding the guard across the fetch
        // await below would make this future non-`Send`.
        #[cfg(test)]
        let scripted = self.schema_fetch_results.lock().unwrap().pop_front();
        #[cfg(test)]
        let arrow_schema = match scripted {
            Some(schema) => {
                // Yield so a scripted fetch suspends where a real Unity Catalog
                // fetch would. Without a suspension point here the resolve is
                // effectively atomic, concurrent callers can never interleave,
                // and the single-flight tests would pass even with the
                // `schema_resolve` lock removed.
                tokio::task::yield_now().await;
                schema
            }
            None => {
                Self::resolve_arrow_schema(
                    &self.config,
                    &self.http_client,
                    self.unity_catalog_client.as_ref(),
                )
                .await?
            }
        };
        #[cfg(not(test))]
        let arrow_schema = Self::resolve_arrow_schema(
            &self.config,
            &self.http_client,
            self.unity_catalog_client.as_ref(),
        )
        .await?;
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
    }

    /// Ensure we have an active stream, creating one if necessary.
    ///
    /// Also used as the healthcheck: resolving the schema verifies the table
    /// and credentials against Unity Catalog, and creating the stream verifies
    /// connectivity to the Zerobus endpoint.
    pub async fn ensure_stream(&self) -> Result<(), ZerobusSinkError> {
        let schema = self.ensure_schema().await?;
        self.get_or_create_stream(&schema).await.map(|_| ())
    }

    /// Return an `Arc` handle to a stream pinned to `schema`, creating one if the
    /// slot is empty or holds a stream built for a different generation.
    ///
    /// The lock is held only while checking/creating the stream; callers can
    /// then use the returned `Arc` without holding the lock.
    ///
    /// A caller the cache has moved past is refused with a retryable
    /// `SchemaReloaded`. Handing it the current stream would drop its batch (see
    /// `PinnedStream`), and letting it build a stale-shaped one would evict the
    /// healthy stream for everyone else. Its retry re-encodes against the current
    /// generation.
    async fn get_or_create_stream(
        &self,
        schema: &Arc<ResolvedSchema>,
    ) -> Result<Arc<ActiveStream>, ZerobusSinkError> {
        let mut stream_guard = self.stream.lock().await;

        // `schema` can disagree with either of the two things it must match, and
        // the answer differs:
        //
        //   vs the cache — the caller is behind (it encoded before a reload). Its
        //     batch fits no stream we would build, so refuse and let the retry
        //     re-encode. See `PinnedStream` for why handing it a stream drops data.
        //   vs the pin — the caller is current and the *stream* is behind, which
        //     the recovery path leaves open by installing the new schema before
        //     discarding the old stream. Evict it and build the right one.
        let cache_moved_on = self
            .schema
            .read()
            .await
            .as_ref()
            .is_some_and(|current| !Arc::ptr_eq(current, schema));
        if cache_moved_on {
            return Err(ZerobusSinkError::SchemaReloaded {
                message: "schema was reloaded while this batch was encoding".to_string(),
            });
        }

        let stream_is_behind = stream_guard
            .as_ref()
            .is_some_and(|pinned| !Arc::ptr_eq(&pinned.schema, schema));
        let superseded = if stream_is_behind {
            stream_guard.take().map(|pinned| pinned.stream)
        } else {
            None
        };

        let result = match stream_guard.as_ref() {
            Some(pinned) => Ok(Arc::clone(&pinned.stream)),
            None => self.build_stream(schema).await.map(|created| {
                let stream = Arc::new(created);
                *stream_guard = Some(PinnedStream {
                    schema: Arc::clone(schema),
                    stream: Arc::clone(&stream),
                });
                stream
            }),
        };

        // Close the evicted stream off the slot lock (as `close_stream` does, so
        // the SDK flush cannot block other requests), and on the error path too
        // so a failed create cannot leak it.
        drop(stream_guard);
        if let Some(superseded) = superseded {
            superseded.close().await;
        }
        result
    }

    /// Open a new Arrow Flight stream shaped to `schema`.
    ///
    /// Overrides only the two timeouts and the compression mode `stream_options`
    /// exposes, accepting the SDK's defaults otherwise — notably `recovery = true`,
    /// so the SDK reconnects and replays in-flight batches itself and only
    /// surfaces a retryable error once its own budget is spent. Both layers are
    /// at-least-once, so a reconnect may re-send unacknowledged batches.
    ///
    /// The two auth flows: `OAuth` exchanges long-lived credentials for a token on
    /// every call (the upstream path external customers use), while `LoginService`
    /// mints signed and unsigned principal contexts over mTLS from the Databricks Login
    /// service via the s2s-proxy, refreshed lazily by `TokenManager` (used by the
    /// logging-agent's SSP-acted-as flow).
    ///
    /// TODO(LP-1615): distinguish stream creation for native OTEL ingestion
    /// endpoints (e.g. emit OTLP-shaped streams). Tracked separately.
    async fn build_stream(
        &self,
        schema: &ResolvedSchema,
    ) -> Result<ActiveStream, ZerobusSinkError> {
        let options = &self.config.stream_options;
        let builder = self
            .sdk
            .stream_builder()
            .table(self.config.table_name.clone());

        let builder = match &self.config.auth {
            super::config::DatabricksAuthentication::LoginService(_) => {
                let tm = self
                    .token_manager
                    .as_ref()
                    .expect("token_manager initialized for LoginService auth");
                let headers_provider: Arc<dyn HeadersProvider> =
                    Arc::new(LoginServiceHeadersProvider::new(
                        Arc::clone(tm),
                        self.config.table_name.clone(),
                        self.custom_headers,
                    ));
                builder.headers_provider(headers_provider)
            }
            super::config::DatabricksAuthentication::OAuth { .. } => {
                let provider = self
                    .oauth_provider
                    .as_ref()
                    .expect("oauth_provider initialized for OAuth auth");
                builder.headers_provider(Arc::clone(provider))
            }
        };

        let builder = builder
            .arrow(Arc::clone(&schema.arrow_schema))
            .server_lack_of_ack_timeout_ms(options.server_lack_of_ack_timeout_ms)
            .flush_timeout_ms(options.flush_timeout_ms)
            .compression(options.arrow_compression());

        // One lane keeps the plain stream (and its wire-size stats) so the default
        // configuration is unchanged by the multiplexed option existing.
        if options.multiplexed_streams > 1 {
            builder
                .multiplexed(options.multiplexed_streams)
                .build_arrow()
                .await
                .map(ActiveStream::arrow_mux)
        } else {
            let stats = Arc::new(WireBytesExporter::default());
            builder
                .stats_exporter(Arc::clone(&stats))
                .build_arrow()
                .await
                .map(|stream| ActiveStream::arrow(stream, stats))
        }
        .map_err(|e| ZerobusSinkError::StreamInitError { source: e })
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
        let pinned = self.stream.lock().await.take();
        if let Some(pinned) = pinned {
            pinned.stream.close().await;
        }
    }

    /// Drop the active stream, gracefully closing it, if the slot still points
    /// at `stream`. A concurrent task may have already replaced it.
    async fn discard_stream(&self, stream: &Arc<ActiveStream>) {
        {
            let mut guard = self.stream.lock().await;
            if guard
                .as_ref()
                .is_some_and(|pinned| Arc::ptr_eq(&pinned.stream, stream))
            {
                guard.take();
            }
        }
        stream.close().await;
    }

    /// Recover from a server-reported schema rejection by re-resolving the table
    /// and discarding the rejected stream, so the retry rebuilds both.
    ///
    /// Returns `SchemaReloaded` (retryable) when the refetch produced a different
    /// schema, otherwise the original error unchanged, so an unfixable rejection
    /// stays permanent instead of looping.
    ///
    /// Fixability is decided from the refetch's *result*, not the server's cause
    /// tokens: a cause can look permanent and not be (`ALTER COLUMN TYPE` arrives
    /// as `TYPE_INCOMPATIBLE`), and predicting wrong drops data silently. The cost
    /// is that an unsatisfiable table refetches once per failing batch, since
    /// `NoProgress` installs nothing to piggyback on.
    ///
    /// A successful reload discards the stream too, since it was built for the
    /// rejected shape. No-progress leaves it: permanent either way, and rebuilding
    /// per batch would add a stream-create round trip to every failure.
    async fn recover_from_schema_rejection(
        &self,
        error: ZerobusSinkError,
        schema: &Arc<ResolvedSchema>,
        stream: Option<&Arc<ActiveStream>>,
    ) -> ZerobusSinkError {
        if !self.config.recover_from_schema_rejection {
            // Recovery disabled: leave the rejection as the SDK reported it
            // (non-retryable), the pre-feature behavior. With no reload the
            // cache holds a single generation for the sink's lifetime, so the
            // generation checks in `get_or_create_stream` never fire either.
            return error;
        }
        if !error.is_schema_rejection() {
            // Not a schema rejection at all; nothing here can help it.
            return error;
        }

        let reloaded = match self.reload_schema_after_rejection(schema).await {
            Ok(reloaded) => reloaded,
            Err(refetch_error) => {
                emit!(ZerobusSchemaRefetchFailed {
                    unity_catalog_failure: refetch_error.to_string(),
                    server_rejection: error.to_string(),
                });
                // Report the refetch failure: unlike the rejection it may be
                // transient (UC 5xx), and its own retryability is already known.
                return refetch_error;
            }
        };

        if !reloaded {
            // Keep the stream: it is about to be rejected again either way, and
            // rebuilding it per batch would add a stream-create round trip to
            // every permanent failure on a table UC and the server disagree
            // about. The outcome counter is emitted by the reload itself.
            warn!(
                message = "Zerobus rejected the sink's schema as stale, but Unity Catalog returned the same schema; not retrying.",
                error = %error,
                table = %self.config.table_name,
            );
            return error;
        }

        // The stream was created with the rejected shape, so it must be rebuilt
        // even when the rejection surfaced mid-stream rather than at setup.
        if let Some(stream) = stream {
            self.discard_stream(stream).await;
        }

        info!(
            message = "Zerobus rejected the sink's schema as stale; reloaded it from Unity Catalog and will retry on a fresh stream.",
            rejection = %error,
            table = %self.config.table_name,
        );
        ZerobusSinkError::SchemaReloaded {
            message: error.to_string(),
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
        let ack_times = freshness::AckTimes::from_batch(&record_batch);
        let result = match stream.as_ref() {
            ActiveStream::Arrow(lock, stats) => {
                let guard = lock.read().await;
                let Some(s) = guard.as_ref() else {
                    return Err(ZerobusSinkError::StreamClosed);
                };
                match s.ingest_batch(record_batch).await {
                    Ok(offset) => {
                        let acked = s.wait_for_offset(offset).await;
                        if acked.is_ok() {
                            ack_times.record_acked(&self.config.table_name);
                        }
                        // Take on every outcome so failed offsets don't linger in the map.
                        let bytes_sent = stats.take(offset);
                        acked.map(|_| bytes_sent)
                    }
                    Err(e) => Err(e),
                }
            }
            ActiveStream::ArrowMux(lock) => {
                let guard = lock.read().await;
                let Some(s) = guard.as_ref() else {
                    return Err(ZerobusSinkError::StreamClosed);
                };
                // Ack durability is per lane, so waiting on this batch's message id
                // (not `flush`, which would wait on every lane) keeps the ack semantics
                // of the single-stream path.
                let acked = match s.ingest_batch(record_batch).await {
                    Ok(id) => s.wait_for_message_id(id).await,
                    Err(e) => Err(e),
                };
                if acked.is_ok() {
                    ack_times.record_acked(&self.config.table_name);
                }
                if let Err(e) = &acked
                    && !mux_should_discard(e, s.is_closed())
                {
                    return Err(ZerobusSinkError::IngestionError { source: e.clone() });
                }
                acked.map(|_| None)
            }
            #[cfg(test)]
            ActiveStream::Mock(mock) => {
                let acked = mock.try_ingest().await;
                if acked.is_ok() {
                    ack_times.record_acked(&self.config.table_name);
                }
                acked.map(|_| None)
            }
        };

        match result {
            Ok(bytes_sent) => Ok(ZerobusResponse::delivered(events_byte_size, bytes_sent)),
            Err(e) => {
                if e.is_retryable() {
                    // Drop the stream so the next attempt creates a fresh one.
                    // `close()` takes `&self`, so the graceful path always runs
                    // regardless of how many other `Arc` clones are in flight:
                    // the write lock waits for concurrent ingests holding read
                    // guards to drain before flushing.
                    self.discard_stream(&stream).await;
                }
                Err(ZerobusSinkError::IngestionError { source: e })
            }
        }
    }
}

/// Whether a failed multiplexed ingest must drop the mux so the next attempt rebuilds it.
///
/// A capacity or ack-wait timeout leaves the lanes usable; only a poisoned (closed) mux
/// must be rebuilt, which recreates all lanes. Non-retryable errors never discard,
/// matching the single-stream path. After an ack-wait timeout the batch stays queued on
/// its lane and may still land, so the retry can duplicate it; delivery is at-least-once
/// either way, since the single-stream close also keeps acking until its flush deadline.
fn mux_should_discard(err: &ZerobusError, mux_closed: bool) -> bool {
    err.is_retryable() && mux_closed
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
                    schema_constraint: matches!(
                        e,
                        vector_lib::codecs::encoding::Error::SchemaConstraintViolation(_)
                    ),
                    message: format!("Failed to encode batch: {}", e),
                })?;
            let payload = ZerobusPayload(record_batch);

            // Both legs below can surface the server's schema rejection: stream
            // setup rejects the shape outright, and an established stream can
            // have the mismatch detected mid-flight during SDK recovery.
            let stream = match service.get_or_create_stream(&schema).await {
                Ok(stream) => stream,
                Err(e) => {
                    return Err(service
                        .recover_from_schema_rejection(e, &schema, None)
                        .await);
                }
            };
            match service
                .ingest(Arc::clone(&stream), payload, events_byte_size)
                .await
            {
                Ok(response) => Ok(response),
                Err(e) => Err(service
                    .recover_from_schema_rejection(e, &schema, Some(&stream))
                    .await),
            }
        })
    }
}

impl Clone for ZerobusService {
    fn clone(&self) -> Self {
        Self {
            sdk: Arc::clone(&self.sdk),
            config: Arc::clone(&self.config),
            http_client: self.http_client.clone(),
            stream: Arc::clone(&self.stream),
            schema: Arc::clone(&self.schema),
            schema_resolve: Arc::clone(&self.schema_resolve),
            token_manager: self.token_manager.clone(),
            unity_catalog_client: self.unity_catalog_client.clone(),
            oauth_provider: self.oauth_provider.clone(),
            custom_headers: self.custom_headers,
            #[cfg(test)]
            schema_fetch_results: Arc::clone(&self.schema_fetch_results),
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
    ///
    /// Pinned to a throwaway generation so `ingest`-only tests need not seed the
    /// cache; pin-sensitive tests use `seed_schema` + `pin_stream_to_cached_schema`.
    pub async fn new_with_mock(
        config: ZerobusSinkConfig,
        mock: MockStream,
    ) -> Result<Self, ZerobusSinkError> {
        config.validate()?;

        let sdk = ZerobusSdk::builder()
            .endpoint(&config.ingestion_endpoint)
            .unity_catalog_url(&config.unity_catalog_endpoint)
            .build()
            .map_err(|e| ZerobusSinkError::ConfigError {
                message: format!("Failed to create Zerobus SDK: {}", e),
            })?;

        let http_client = HttpClient::new(TlsSettings::default(), &ProxyConfig::default())
            .map_err(|e| ZerobusSinkError::ConfigError {
                message: format!("Failed to create HTTP client: {}", e),
            })?;

        let custom_headers = headers::leak_custom_headers(&config);
        let oauth_provider = headers::build_oauth_provider(&config, custom_headers);

        let config = Arc::new(config);
        let pinned = PinnedStream {
            schema: Self::build_test_schema(&config, arrow::datatypes::Schema::empty()),
            stream: Arc::new(ActiveStream::Mock(mock)),
        };

        Ok(Self {
            sdk: Arc::new(sdk),
            config,
            http_client,
            stream: Arc::new(Mutex::new(Some(pinned))),
            schema: Arc::new(RwLock::new(None)),
            schema_resolve: Arc::new(Mutex::new(())),
            token_manager: None,
            unity_catalog_client: None,
            oauth_provider,
            custom_headers,
            schema_fetch_results: Arc::new(std::sync::Mutex::new(Default::default())),
        })
    }

    /// Returns true if the service currently has an active stream.
    pub async fn has_active_stream(&self) -> bool {
        self.stream.lock().await.is_some()
    }

    /// Build a `ResolvedSchema` without touching Unity Catalog.
    fn build_test_schema(
        config: &ZerobusSinkConfig,
        arrow_schema: arrow::datatypes::Schema,
    ) -> Arc<ResolvedSchema> {
        let mut batch_encoding = config.batch_encoding.clone();
        if let BatchSerializerConfig::ArrowStream(config) = &mut batch_encoding {
            config.schema = Some(arrow_schema.clone());
        }
        Arc::new(ResolvedSchema {
            encoder: BatchEncoder::new(batch_encoding.build().unwrap()),
            arrow_schema: Arc::new(arrow_schema),
        })
    }

    /// Install a schema into the cache directly, standing in for a Unity
    /// Catalog fetch (which the unit tests cannot perform).
    pub(super) async fn seed_schema(&self, arrow_schema: arrow::datatypes::Schema) {
        let resolved = Self::build_test_schema(&self.config, arrow_schema);
        *self.schema.write().await = Some(resolved);
    }

    /// Re-pin the installed stream to whatever the cache currently holds, so a
    /// test can exercise `get_or_create_stream` from a consistent starting
    /// state after seeding a schema.
    pub(super) async fn pin_stream_to_cached_schema(&self) {
        let schema = self.cached_schema().await.expect("schema must be seeded");
        if let Some(pinned) = self.stream.lock().await.as_mut() {
            pinned.schema = schema;
        }
    }

    /// The schema generation the installed stream is pinned to, if any.
    pub(super) async fn pinned_stream_schema(&self) -> Option<Arc<ResolvedSchema>> {
        self.stream
            .lock()
            .await
            .as_ref()
            .map(|pinned| Arc::clone(&pinned.schema))
    }

    /// Returns true if a schema is currently cached.
    pub(super) async fn has_cached_schema(&self) -> bool {
        self.schema.read().await.is_some()
    }

    /// Script the schemas the next `resolve_schema` calls will "fetch", standing
    /// in for Unity Catalog.
    pub(super) fn script_schema_fetches(
        &self,
        schemas: impl IntoIterator<Item = arrow::datatypes::Schema>,
    ) {
        *self.schema_fetch_results.lock().unwrap() = schemas.into_iter().collect();
    }

    /// The currently cached schema, if any.
    pub(super) async fn cached_schema(&self) -> Option<Arc<ResolvedSchema>> {
        self.schema.read().await.as_ref().map(Arc::clone)
    }

    /// How many scripted schema fetches are still queued. A test asserts this is
    /// unchanged to prove no reload was attempted.
    pub(super) fn scripted_fetch_count(&self) -> usize {
        self.schema_fetch_results.lock().unwrap().len()
    }
}

impl RetryLogic for ZerobusRetryLogic {
    type Error = ZerobusSinkError;
    type Request = ZerobusRequest;
    type Response = ZerobusResponse;

    fn is_retriable_error(&self, error: &Self::Error) -> bool {
        error.is_retryable()
    }

    fn on_retriable_error(&self, error: &Self::Error) {
        emit!(ZerobusRetryableError {
            error_code: error.error_code(),
        });
    }

    fn on_retriable_timeout(&self) {
        emit!(ZerobusRetryableError {
            error_code: REQUEST_TIMEOUT_ERROR_CODE,
        });
    }
}

/// `error_code` for an attempt cut off by the Tower request timeout (`Elapsed`).
const REQUEST_TIMEOUT_ERROR_CODE: &str = "request_timeout";

/// Tower layer that converts retry-budget-exhausted retryable errors into a
/// successful `ZerobusResponse` carrying `EventStatus::Errored`.
///
/// Wraps the retry layer from the outside. When the retry layer returns:
/// - `Ok(resp)` — pass through unchanged.
/// - `Err(e)` where `e.is_retryable()` — convert to `Ok(ZerobusResponse::errored())`
///   so the driver marks finalizers `Errored` (transient — source / disk
///   buffer may replay) rather than `Rejected` (permanent drop).
/// - `Err(e)` permanent — propagate so the driver maps to `Rejected`, wrapped in a
///   `CodedError` so the driver's error and discard metrics carry its `error_code`.
///
/// Without this layer the driver maps every `Err` from `Service::call` to
/// `EventStatus::Rejected`, which would drop transient-but-exhausted failures
/// as if they were permanent.
#[derive(Clone, Debug, Default)]
pub struct RetryableErrorAsErroredLayer;

impl<S> Layer<S> for RetryableErrorAsErroredLayer {
    type Service = RetryableErrorAsErrored<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RetryableErrorAsErrored { inner }
    }
}

#[derive(Clone, Debug)]
pub struct RetryableErrorAsErrored<S> {
    inner: S,
}

impl<S> Service<ZerobusRequest> for RetryableErrorAsErrored<S>
where
    S: Service<ZerobusRequest, Response = ZerobusResponse, Error = crate::Error>,
    S::Future: Send + 'static,
{
    type Response = ZerobusResponse;
    type Error = crate::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: ZerobusRequest) -> Self::Future {
        let fut = self.inner.call(req);
        Box::pin(async move {
            match fut.await {
                Ok(resp) => Ok(resp),
                Err(e) => {
                    // The Tower stack boxes errors above us (retry, timeout,
                    // adaptive-concurrency). Downcast to inspect retryability;
                    // anything that isn't a `ZerobusSinkError` (e.g. a timeout
                    // `Elapsed`) is conservatively treated as transient.
                    let (retryable, error_code) = match e.downcast_ref::<ZerobusSinkError>() {
                        Some(zb) => (zb.is_retryable(), zb.error_code()),
                        None if e.is::<Elapsed>() => (true, REQUEST_TIMEOUT_ERROR_CODE),
                        None => (true, "other"),
                    };
                    if retryable {
                        warn!(
                            message = "Zerobus retry budget exhausted on transient error; signaling Errored so source or buffer may replay.",
                            error = %e,
                            error_code,
                        );
                        Ok(ZerobusResponse::errored())
                    } else {
                        Err(CodedError {
                            code: error_code,
                            error: e,
                        }
                        .into())
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinks::databricks_zerobus::config::{
        DatabricksAuthentication, SchemaSource, ZerobusStreamOptions,
    };
    use databricks_zerobus_ingest_sdk::ZerobusError;
    use vector_lib::event::MetricValue;
    use vector_lib::event_test_util::{clear_recorded_events, contains_name_once};
    use vector_lib::sensitive_string::SensitiveString;

    #[tokio::test]
    async fn oauth_always_builds_wrapped_provider() {
        // OAuth always routes through our wrapper (which merges custom headers,
        // empty or not), never the SDK's plain oauth path.
        let service = ZerobusService::new(test_config(), &ProxyConfig::default())
            .await
            .unwrap();
        assert!(service.oauth_provider.is_some());

        let mut config = test_config();
        config
            .custom_headers
            .insert("x-my-tenant".into(), "acme".into());
        let service = ZerobusService::new(config, &ProxyConfig::default())
            .await
            .unwrap();
        assert!(service.oauth_provider.is_some());
    }

    fn test_config() -> ZerobusSinkConfig {
        ZerobusSinkConfig {
            ingestion_endpoint: "https://127.0.0.1:1".to_string(),
            ingestion_domain_name: None,
            table_name: "test.default.logs".to_string(),
            unity_catalog_endpoint: "https://127.0.0.1:1".to_string(),
            auth: DatabricksAuthentication::OAuth {
                client_id: SensitiveString::from("id".to_string()),
                client_secret: SensitiveString::from("secret".to_string()),
            },
            user_agent: None,
            custom_headers: std::collections::BTreeMap::new(),
            schema: SchemaSource::UnityCatalog,
            stream_options: ZerobusStreamOptions::default(),
            batch_encoding: vector_lib::codecs::encoding::BatchSerializerConfig::ArrowStream(
                Default::default(),
            ),
            batch: Default::default(),
            request: Default::default(),
            // The reload/pin tests exercise the recovery path, so the gate is on
            // here; `gate_off_disables_schema_recovery` flips it back to assert
            // the default (disabled) behavior.
            recover_from_schema_rejection: true,
            acknowledgements: Default::default(),
        }
    }

    fn dummy_payload() -> ZerobusPayload {
        use arrow::datatypes::Schema;
        ZerobusPayload(arrow::record_batch::RecordBatch::new_empty(Arc::new(
            Schema::empty(),
        )))
    }

    async fn current_stream(service: &ZerobusService) -> Arc<ActiveStream> {
        Arc::clone(&service.stream.lock().await.as_ref().unwrap().stream)
    }

    fn batch_sent(offset: OffsetId, attempt: u32, wire_bytes: u64) -> StreamStat {
        StreamStat::BatchSent {
            offset,
            attempt,
            stats: databricks_zerobus_ingest_sdk::BatchStats::new(10, wire_bytes, wire_bytes * 2),
        }
    }

    #[test]
    fn wire_bytes_exporter_keeps_first_send_per_offset() {
        let exporter = WireBytesExporter::default();
        exporter.record(batch_sent(1, 0, 100));
        // A recovery replay of the same offset must not double count.
        exporter.record(batch_sent(1, 1, 40));
        exporter.record(batch_sent(2, 0, 7));
        exporter.record(StreamStat::BatchAcked { offset: 1 });

        assert_eq!(exporter.take(1), Some(100));
        assert_eq!(exporter.take(1), None);
        assert_eq!(exporter.take(2), Some(7));
        assert_eq!(exporter.take(3), None);
    }

    #[test]
    fn response_reports_bytes_sent() {
        let resp = ZerobusResponse::delivered(GroupedCountByteSize::new_untagged(), Some(42));
        assert_eq!(resp.bytes_sent(), Some(42));
        assert_eq!(ZerobusResponse::errored().bytes_sent(), None);
    }

    #[test]
    fn mux_discards_only_on_retryable_error_from_closed_mux() {
        let retryable = ZerobusError::ChannelCreationError("lane failed".to_string());
        let non_retryable = ZerobusError::InvalidArgument("bad field".to_string());
        assert!(retryable.is_retryable());
        assert!(!non_retryable.is_retryable());

        for (err, closed, expected) in [
            (&retryable, false, false),
            (&retryable, true, true),
            (&non_retryable, false, false),
            (&non_retryable, true, false),
        ] {
            assert_eq!(
                mux_should_discard(err, closed),
                expected,
                "{err:?}, closed = {closed}"
            );
        }
    }

    #[tokio::test]
    async fn ingest_succeeds_with_mock_stream() {
        let service = ZerobusService::new_with_mock(test_config(), MockStream::succeeding())
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

    /// Freshness metrics captured for one table label.
    fn freshness_metrics(table: &str) -> Vec<vector_lib::event::Metric> {
        vector_lib::metrics::Controller::get()
            .unwrap()
            .capture_metrics()
            .into_iter()
            .filter(|m| {
                m.name().starts_with("zerobus_ack_")
                    && m.tags()
                        .and_then(|t| t.get("table"))
                        .map(String::from)
                        .as_deref()
                        == Some(table)
            })
            .collect()
    }

    fn histogram_count(metrics: &[vector_lib::event::Metric], name: &str) -> u64 {
        let found: Vec<u64> = metrics
            .iter()
            .filter(|m| m.name() == name)
            .map(|m| match m.value() {
                MetricValue::AggregatedHistogram { count, .. } => *count,
                other => panic!("{name} is not a histogram: {other:?}"),
            })
            .collect();
        assert_eq!(found.len(), 1, "{name} recorded {} times", found.len());
        found[0]
    }

    fn counter_value(
        metrics: &[vector_lib::event::Metric],
        name: &str,
        series: Option<&str>,
    ) -> f64 {
        let matching: Vec<f64> = metrics
            .iter()
            .filter(|m| {
                m.name() == name
                    && series.is_none_or(|s| {
                        m.tags()
                            .and_then(|t| t.get("series"))
                            .map(String::from)
                            .as_deref()
                            == Some(s)
                    })
            })
            .map(|m| match m.value() {
                MetricValue::Counter { value } => *value,
                other => panic!("{name} is not a counter: {other:?}"),
            })
            .collect();
        assert_eq!(matching.len(), 1, "{name} series {series:?} missing");
        matching[0]
    }

    #[tokio::test]
    async fn freshness_is_recorded_only_for_acked_batches() {
        use crate::sinks::databricks_zerobus::freshness::test_support::batch_with_times;

        vector_lib::metrics::init_test();

        // Row 0: real born-at and ingest. Row 1: no born-at, so it is missing.
        let mut acked_config = test_config();
        acked_config.table_name = "freshness.acked.logs".to_string();
        let service = ZerobusService::new_with_mock(acked_config, MockStream::succeeding())
            .await
            .unwrap();
        let stream = current_stream(&service).await;
        let payload = ZerobusPayload(batch_with_times(
            vec![Some(1_000), None],
            vec![Some(2_000), Some(2_000)],
        ));
        service
            .ingest(stream, payload, GroupedCountByteSize::new_untagged())
            .await
            .unwrap();

        let acked = freshness_metrics("freshness.acked.logs");
        assert_eq!(
            histogram_count(&acked, "zerobus_ack_born_to_saved_seconds"),
            1
        );
        assert_eq!(
            histogram_count(&acked, "zerobus_ack_lp_ingest_to_saved_seconds"),
            2
        );
        assert_eq!(
            counter_value(&acked, "zerobus_ack_missing_born_at_total", None),
            1.0
        );
        // Row 0's born-at is 1_000 ms epoch, so its lag is far over the SLA.
        assert_eq!(
            counter_value(&acked, "zerobus_ack_over_sla_total", None),
            1.0
        );
        assert_eq!(
            counter_value(
                &acked,
                "zerobus_ack_negative_latency_total",
                Some("born_to_saved")
            ),
            0.0
        );
        assert_eq!(
            counter_value(
                &acked,
                "zerobus_ack_negative_latency_total",
                Some("lp_ingest_to_saved")
            ),
            0.0
        );

        let mut failed_config = test_config();
        failed_config.table_name = "freshness.failed.logs".to_string();
        let mock = MockStream::failing(ZerobusError::ChannelCreationError(
            "connection reset".to_string(),
        ));
        let service = ZerobusService::new_with_mock(failed_config, mock)
            .await
            .unwrap();
        let stream = current_stream(&service).await;
        let payload = ZerobusPayload(batch_with_times(vec![Some(1_000)], vec![Some(2_000)]));
        assert!(
            service
                .ingest(stream, payload, GroupedCountByteSize::new_untagged())
                .await
                .is_err()
        );
        assert!(freshness_metrics("freshness.failed.logs").is_empty());
    }

    fn freshness_perf_schema() -> arrow::datatypes::Schema {
        use arrow::datatypes::{DataType, Field, Fields, Schema};
        let service_metadata = DataType::Struct(Fields::from(vec![Field::new(
            "log_timestamp_ms",
            DataType::Int64,
            true,
        )]));
        let vector_metadata = DataType::Struct(Fields::from(vec![Field::new(
            "timestamp_ms",
            DataType::Int64,
            true,
        )]));
        let log_metadata = DataType::Struct(Fields::from(vec![
            Field::new("service_metadata", service_metadata, true),
            Field::new("vector_metadata", vector_metadata, true),
        ]));
        Schema::new(vec![
            Field::new("_log_metadata", log_metadata, true),
            Field::new("message", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
            Field::new("count", DataType::Int64, true),
        ])
    }

    /// Timing proof: the freshness step against `encode_batch` on the same 5,000-row batch.
    ///
    /// Run in release mode:
    /// `cargo test --release --lib freshness_step_vs_encode -- --ignored --nocapture`
    #[test]
    #[ignore]
    #[allow(clippy::print_stderr)]
    fn freshness_step_vs_encode() {
        use std::time::{Duration, Instant};

        vector_lib::metrics::init_test();
        const ROWS: usize = 5_000;
        const ITERS: usize = 300;

        let schema = ZerobusService::build_test_schema(&test_config(), freshness_perf_schema());
        let events: Vec<Event> = (0..ROWS)
            .map(|i| {
                let mut log = vector_lib::event::LogEvent::default();
                log.insert(
                    "_log_metadata.service_metadata.log_timestamp_ms",
                    1_700_000_000_000_i64 + i as i64,
                );
                log.insert(
                    "_log_metadata.vector_metadata.timestamp_ms",
                    1_700_000_001_000_i64 + i as i64,
                );
                log.insert("message", format!("row {i} payload bytes"));
                log.insert("level", "INFO");
                log.insert("count", i as i64);
                Event::Log(log)
            })
            .collect();

        let mut encode = Vec::with_capacity(ITERS);
        let mut freshness = Vec::with_capacity(ITERS);
        for _ in 0..ITERS {
            let start = Instant::now();
            let BatchOutput::Arrow(batch) = schema.encoder.encode_batch(&events).unwrap();
            encode.push(start.elapsed());
            let start = Instant::now();
            freshness::AckTimes::from_batch(&batch).record_acked("perf.table");
            freshness.push(start.elapsed());
            std::hint::black_box(&batch);
        }

        let median = |mut v: Vec<Duration>| {
            v.sort();
            v[v.len() / 2]
        };
        let encode_ns = median(encode).as_nanos() as f64 / ROWS as f64;
        let freshness_ns = median(freshness).as_nanos() as f64 / ROWS as f64;
        eprintln!(
            "rows={ROWS} iters={ITERS} (median)\n\
             encode_batch: {encode_ns:.1} ns/row\n\
             freshness:    {freshness_ns:.1} ns/row\n\
             overhead:     {:.2}% of encode_batch",
            freshness_ns / encode_ns * 100.0
        );
    }

    #[tokio::test]
    async fn retryable_error_clears_stream() {
        let mock = MockStream::failing(ZerobusError::ChannelCreationError(
            "connection reset".to_string(),
        ));
        let service = ZerobusService::new_with_mock(test_config(), mock)
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
        let service = ZerobusService::new_with_mock(test_config(), mock)
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
        let service = ZerobusService::new_with_mock(test_config(), mock)
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
            if let Some(pinned) = guard.as_ref()
                && let ActiveStream::Mock(mock) = pinned.stream.as_ref()
            {
                mock.set_next_error(ZerobusError::ChannelCreationError("reset".to_string()));
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
        *service.stream.lock().await = Some(PinnedStream {
            schema: ZerobusService::build_test_schema(
                &service.config,
                arrow::datatypes::Schema::empty(),
            ),
            stream: Arc::new(ActiveStream::Mock(MockStream::succeeding())),
        });

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

        let service = ZerobusService::new_with_mock(test_config(), mock)
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

        let service = ZerobusService::new_with_mock(test_config(), mock)
            .await
            .unwrap();

        // Spawn two concurrent ingests. Each takes its own `Arc` clone of the
        // active stream, then blocks in the gate.
        let s1 = service.clone();
        let stream1 = current_stream(&service).await;
        let t1 = tokio::spawn(async move {
            s1.ingest(
                stream1,
                dummy_payload(),
                GroupedCountByteSize::new_untagged(),
            )
            .await
        });
        let s2 = service.clone();
        let stream2 = current_stream(&service).await;
        let t2 = tokio::spawn(async move {
            s2.ingest(
                stream2,
                dummy_payload(),
                GroupedCountByteSize::new_untagged(),
            )
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

    /// The schema cache must be *replaceable*. Under the previous write-once
    /// `OnceCell` a widened Unity Catalog table could only be picked up by
    /// restarting the process; a second install silently kept the stale value.
    #[tokio::test]
    async fn cached_schema_can_be_replaced() {
        use arrow::datatypes::{DataType, Field, Schema};

        let service = ZerobusService::new_with_mock(test_config(), MockStream::succeeding())
            .await
            .unwrap();
        assert!(!service.has_cached_schema().await);

        let narrow = Schema::new(vec![Field::new("status", DataType::Int32, true)]);
        service.seed_schema(narrow).await;
        let first = service.ensure_schema().await.unwrap();
        assert_eq!(first.arrow_schema.fields().len(), 1);

        // Simulate the table being widened, then reloaded.
        let wide = Schema::new(vec![
            Field::new("status", DataType::Int32, true),
            Field::new("referer", DataType::LargeUtf8, true),
        ]);
        service.seed_schema(wide).await;
        let second = service.ensure_schema().await.unwrap();

        assert_eq!(second.arrow_schema.fields().len(), 2);
        // The reload installs a new value rather than mutating the old one, so
        // the handle taken before the swap still sees the narrow schema.
        assert_eq!(first.arrow_schema.fields().len(), 1);
        assert!(!Arc::ptr_eq(&first, &second));
    }

    /// `ensure_schema` hands out an `Arc`, so an in-flight request keeps
    /// encoding against the generation it started with even if the cache is
    /// replaced underneath it.
    #[tokio::test]
    async fn in_flight_schema_handle_survives_replacement() {
        use arrow::datatypes::{DataType, Field, Schema};

        let service = ZerobusService::new_with_mock(test_config(), MockStream::succeeding())
            .await
            .unwrap();
        service
            .seed_schema(Schema::new(vec![Field::new(
                "status",
                DataType::Int32,
                true,
            )]))
            .await;

        let in_flight = service.ensure_schema().await.unwrap();
        service.seed_schema(Schema::empty()).await;

        // The cache moved on, but the handle is still usable and unchanged.
        assert_eq!(in_flight.arrow_schema.fields().len(), 1);
        assert_eq!(
            service
                .ensure_schema()
                .await
                .unwrap()
                .arrow_schema
                .fields()
                .len(),
            0
        );
    }

    /// Concurrent cache *misses* must collapse into a single resolve. The
    /// single-flight guarantee is what keeps a burst of batches from issuing
    /// one Unity Catalog fetch each.
    ///
    /// Starts with an empty cache so every caller takes the miss path — seeding
    /// it first would let all eight return from `ensure_schema`'s fast path,
    /// leaving `schema_resolve` untouched and the test unable to fail. Exactly
    /// one scripted fetch is queued: a second resolve finds the queue empty and
    /// falls through to a real Unity Catalog call against the unroutable
    /// endpoint in `test_config`, which fails the test.
    #[tokio::test]
    async fn concurrent_ensure_schema_resolves_once() {
        let service = ZerobusService::new_with_mock(test_config(), MockStream::succeeding())
            .await
            .unwrap();
        assert!(!service.has_cached_schema().await);
        service.script_schema_fetches([narrow_schema()]);

        let mut handles = Vec::new();
        for _ in 0..8 {
            let svc = service.clone();
            handles.push(tokio::spawn(async move { svc.ensure_schema().await }));
        }

        let first = handles
            .pop()
            .unwrap()
            .await
            .unwrap()
            .expect("resolve should succeed from the single scripted fetch");
        for handle in handles {
            let got = handle
                .await
                .unwrap()
                .expect("a second resolve would hit Unity Catalog and fail");
            assert!(
                Arc::ptr_eq(&first, &got),
                "concurrent callers observed different schema generations"
            );
        }
    }

    /// `get_or_create_stream`'s error, discarding the `Ok` stream (which holds
    /// SDK internals and is not `Debug`, so `unwrap_err` is unavailable).
    async fn stream_error(
        service: &ZerobusService,
        schema: &Arc<ResolvedSchema>,
    ) -> ZerobusSinkError {
        match service.get_or_create_stream(schema).await {
            Ok(_) => panic!("expected get_or_create_stream to fail"),
            Err(e) => e,
        }
    }

    fn narrow_schema() -> arrow::datatypes::Schema {
        use arrow::datatypes::{DataType, Field, Schema};
        Schema::new(vec![Field::new("status", DataType::Int32, true)])
    }

    fn wide_schema() -> arrow::datatypes::Schema {
        use arrow::datatypes::{DataType, Field, Schema};
        Schema::new(vec![
            Field::new("status", DataType::Int32, true),
            Field::new("referer", DataType::LargeUtf8, true),
        ])
    }

    /// The recovery path for a stale rejection: re-resolve, observe the widened
    /// table, and install it. This is what makes a pod bounce unnecessary.
    #[tokio::test]
    async fn reload_installs_a_widened_schema() {
        let service = ZerobusService::new_with_mock(test_config(), MockStream::succeeding())
            .await
            .unwrap();
        service.seed_schema(narrow_schema()).await;
        let rejected = service.ensure_schema().await.unwrap();

        service.script_schema_fetches([wide_schema()]);
        clear_recorded_events();
        let reloaded = service
            .reload_schema_after_rejection(&rejected)
            .await
            .unwrap();

        assert!(reloaded);
        let current = service.cached_schema().await.unwrap();
        assert_eq!(current.arrow_schema.fields().len(), 2);
        assert!(!Arc::ptr_eq(&current, &rejected));
        // A recovered rejection must be countable: aggregate throughput and
        // error rates read as half-healthy during a widening, so this counter is
        // the only signal that drift happened at all.
        assert!(
            contains_name_once("ZerobusSchemaReloadOutcome").is_ok(),
            "a successful reload must report its outcome"
        );
    }

    /// The loop guard. If Unity Catalog hands back the very schema the server
    /// just rejected, the reload made no progress; reporting success would spin
    /// reload -> reject -> reload forever.
    #[tokio::test]
    async fn reload_refuses_when_schema_is_unchanged() {
        let service = ZerobusService::new_with_mock(test_config(), MockStream::succeeding())
            .await
            .unwrap();
        service.seed_schema(narrow_schema()).await;
        let rejected = service.ensure_schema().await.unwrap();

        service.script_schema_fetches([narrow_schema()]);
        clear_recorded_events();
        let reloaded = service
            .reload_schema_after_rejection(&rejected)
            .await
            .unwrap();

        assert!(!reloaded, "an unchanged schema must not report progress");
        // The alertable case: UC and the ingestion server disagree, so every
        // batch fails permanently until someone intervenes. It must be visible.
        assert!(
            contains_name_once("ZerobusSchemaReloadOutcome").is_ok(),
            "a stuck reload must report its outcome"
        );
        // The cache is left on the original generation rather than churned.
        assert!(Arc::ptr_eq(
            &service.cached_schema().await.unwrap(),
            &rejected
        ));
    }

    /// A fleet-wide envelope change fails every in-flight batch at once. Those
    /// reloads must collapse into one Unity Catalog fetch, not one per batch.
    #[tokio::test]
    async fn concurrent_reloads_fetch_once() {
        let service = ZerobusService::new_with_mock(test_config(), MockStream::succeeding())
            .await
            .unwrap();
        service.seed_schema(narrow_schema()).await;
        let rejected = service.ensure_schema().await.unwrap();

        // Exactly one scripted fetch for eight concurrent rejections: a second
        // fetch would fall through to a real UC call and fail the test.
        service.script_schema_fetches([wide_schema()]);

        let mut handles = Vec::new();
        for _ in 0..8 {
            let svc = service.clone();
            let rejected = Arc::clone(&rejected);
            handles.push(tokio::spawn(async move {
                svc.reload_schema_after_rejection(&rejected).await
            }));
        }
        for handle in handles {
            assert!(
                handle.await.unwrap().unwrap(),
                "every caller should observe the reload, whether it did the work or piggybacked"
            );
        }

        assert_eq!(
            service
                .cached_schema()
                .await
                .unwrap()
                .arrow_schema
                .fields()
                .len(),
            2
        );
    }

    /// The generation-skew case, and the reason the stream slot is pinned.
    ///
    /// A request can finish encoding an older generation after another request's
    /// reload installed a stream for the new one. Handing it that stream trips the
    /// SDK's client-side check, which returns `InvalidArgument` rather than
    /// `InvalidSchema` — so `is_schema_rejection()` is false, recovery passes it
    /// through, and the batch is dropped though a plain retry would have
    /// succeeded. It must be refused up front as retryable instead.
    #[tokio::test]
    async fn superseded_request_is_refused_as_retryable() {
        let service = ZerobusService::new_with_mock(test_config(), MockStream::succeeding())
            .await
            .unwrap();
        service.seed_schema(narrow_schema()).await;
        service.pin_stream_to_cached_schema().await;

        // The generation this request resolved and encoded against.
        let stale = service.ensure_schema().await.unwrap();

        // Another request reloads: the cache moves to the widened shape.
        service.script_schema_fetches([wide_schema()]);
        assert!(service.reload_schema_after_rejection(&stale).await.unwrap());

        let err = stream_error(&service, &stale).await;

        assert!(
            matches!(err, ZerobusSinkError::SchemaReloaded { .. }),
            "a superseded caller must be refused, not handed a foreign-shaped stream: {err:?}"
        );
        // Retryable is the whole point: `call` re-runs `ensure_schema`, picks up
        // the widened shape, re-encodes, and succeeds.
        assert!(
            err.is_retryable() && ZerobusRetryLogic.is_retriable_error(&err),
            "the refusal must be retryable, otherwise the batch is still dropped"
        );
    }

    /// The current generation must not be starved by a leftover stream. A reload
    /// installs the new schema before its caller discards the old stream, so a
    /// request arriving in that window must replace it rather than be blamed for
    /// it.
    #[tokio::test]
    async fn stale_pinned_stream_is_replaced_for_current_generation() {
        let service = ZerobusService::new_with_mock(test_config(), MockStream::succeeding())
            .await
            .unwrap();
        service.seed_schema(narrow_schema()).await;
        service.pin_stream_to_cached_schema().await;

        let stale = service.ensure_schema().await.unwrap();
        let closed = match service
            .stream
            .lock()
            .await
            .as_ref()
            .unwrap()
            .stream
            .as_ref()
        {
            ActiveStream::Mock(mock) => mock.closed_flag(),
            _ => unreachable!("mock stream installed by new_with_mock"),
        };

        // Reload without discarding the stream — exactly the interleaving the
        // recovery path leaves open between installing the schema and closing
        // the rejected stream.
        service.script_schema_fetches([wide_schema()]);
        assert!(service.reload_schema_after_rejection(&stale).await.unwrap());
        let current = service.cached_schema().await.unwrap();
        assert!(Arc::ptr_eq(
            &service.pinned_stream_schema().await.unwrap(),
            &stale
        ));

        // Creating a real stream needs the network, so assert on the failure
        // *mode*: reaching stream creation (StreamInitError against the
        // unroutable test endpoint) rather than being refused as superseded.
        let err = stream_error(&service, &current).await;
        assert!(
            matches!(err, ZerobusSinkError::StreamInitError { .. }),
            "the current generation must proceed to build its own stream: {err:?}"
        );

        // The superseded stream was evicted and gracefully closed, not leaked.
        assert!(
            service.pinned_stream_schema().await.is_none(),
            "the stale-generation stream must be removed from the slot"
        );
        assert!(
            closed.load(std::sync::atomic::Ordering::Relaxed),
            "the evicted stream must be closed gracefully, not dropped"
        );
    }

    /// The ordinary case must stay a plain cache hit: same generation, same
    /// stream, no rebuild.
    #[tokio::test]
    async fn matching_generation_reuses_the_pinned_stream() {
        let service = ZerobusService::new_with_mock(test_config(), MockStream::succeeding())
            .await
            .unwrap();
        service.seed_schema(narrow_schema()).await;
        service.pin_stream_to_cached_schema().await;

        let schema = service.ensure_schema().await.unwrap();
        let expected = current_stream(&service).await;

        let stream = service.get_or_create_stream(&schema).await.unwrap();

        assert!(
            Arc::ptr_eq(&stream, &expected),
            "an unchanged generation must reuse the existing stream"
        );
    }

    /// An error that is not a schema rejection must not trigger a refetch. Now
    /// that every `InvalidSchema` is refetched, this gate is the only thing
    /// keeping unrelated failures off the Unity Catalog path — including the
    /// client-side `InvalidArgument` from a generation-skewed batch, which is
    /// what this uses (the SDK's `InvalidSchema` is `#[non_exhaustive]` with a
    /// crate-private constructor, so no external crate can build one).
    #[tokio::test]
    async fn non_schema_error_passes_through_without_refetching() {
        let service = ZerobusService::new_with_mock(test_config(), MockStream::succeeding())
            .await
            .unwrap();
        service.seed_schema(narrow_schema()).await;
        let schema = service.ensure_schema().await.unwrap();

        // No scripted fetches: a reload attempt would hit the network and fail.
        let original = ZerobusSinkError::IngestionError {
            source: ZerobusError::InvalidArgument("bad field".to_string()),
        };
        let returned = service
            .recover_from_schema_rejection(original, &schema, None)
            .await;

        assert!(matches!(returned, ZerobusSinkError::IngestionError { .. }));
        assert!(!returned.is_retryable());
        // Cache untouched.
        assert!(Arc::ptr_eq(
            &service.cached_schema().await.unwrap(),
            &schema
        ));
    }

    /// With `recover_from_schema_rejection` off (the default), recovery is a
    /// pure pass-through: the rejection is returned exactly as the SDK reported
    /// it and no Unity Catalog refetch is attempted, so behavior matches the
    /// pre-feature sink. The gate is checked before `is_schema_rejection`, so it
    /// holds even for what would otherwise be a stale rejection.
    #[tokio::test]
    async fn gate_off_disables_schema_recovery() {
        let mut config = test_config();
        config.recover_from_schema_rejection = false;
        let service = ZerobusService::new_with_mock(config, MockStream::succeeding())
            .await
            .unwrap();
        service.seed_schema(narrow_schema()).await;
        let schema = service.ensure_schema().await.unwrap();

        // Queue a widened schema. If the gate leaked, recovery would consume
        // this fetch and reload; with the gate off it must stay untouched.
        service.script_schema_fetches([wide_schema()]);
        let original = ZerobusSinkError::IngestionError {
            source: ZerobusError::InvalidArgument("bad field".to_string()),
        };
        let returned = service
            .recover_from_schema_rejection(original, &schema, None)
            .await;

        assert!(matches!(returned, ZerobusSinkError::IngestionError { .. }));
        assert!(!returned.is_retryable());
        // Cache still on the original generation, and the scripted fetch was
        // never consumed — no reload was attempted.
        assert!(Arc::ptr_eq(
            &service.cached_schema().await.unwrap(),
            &schema
        ));
        assert_eq!(service.scripted_fetch_count(), 1);
    }

    /// `SchemaReloaded` must be retryable even though the rejection it wraps is
    /// not: the schema changed between attempts, so the retry is not a repeat of
    /// the same request.
    #[test]
    fn schema_reloaded_is_retryable() {
        let error = ZerobusSinkError::SchemaReloaded {
            message: "stale".to_string(),
        };
        assert!(error.is_retryable());
        assert!(ZerobusRetryLogic.is_retriable_error(&error));
        assert_eq!(
            vector_lib::event::EventStatus::from(error),
            vector_lib::event::EventStatus::Errored
        );
    }

    fn dummy_request() -> ZerobusRequest {
        ZerobusRequest {
            events: Arc::new(vec![]),
            metadata: RequestMetadata::default(),
            finalizers: EventFinalizers::default(),
        }
    }

    #[tokio::test]
    async fn retryable_err_after_exhaustion_becomes_ok_errored() {
        use tower::ServiceExt;
        let inner = tower::service_fn(|_req: ZerobusRequest| async move {
            let err: crate::Error = Box::new(ZerobusSinkError::SchemaError {
                message: "UC 503".to_string(),
                retryable: true,
                status: Some(503),
            });
            Err::<ZerobusResponse, _>(err)
        });
        let mut svc = RetryableErrorAsErrored { inner };
        let resp = svc
            .ready()
            .await
            .unwrap()
            .call(dummy_request())
            .await
            .unwrap();
        assert_eq!(resp.status, vector_lib::event::EventStatus::Errored);
    }

    fn retryable_error_counts() -> Vec<(Option<String>, vector_lib::event::MetricValue)> {
        let mut counts: Vec<_> = vector_lib::metrics::Controller::get()
            .unwrap()
            .capture_metrics()
            .into_iter()
            .filter(|m| m.name() == "zerobus_retryable_errors_total")
            .map(|m| {
                let code = m.tags().and_then(|t| t.get("error_code")).map(String::from);
                (code, m.value().clone())
            })
            .collect();
        counts.sort_by(|a, b| a.0.cmp(&b.0));
        counts
    }

    #[tokio::test(start_paused = true)]
    async fn retried_attempts_count_by_error_code() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tower::ServiceExt;

        vector_lib::metrics::init_test();
        vector_lib::metrics::Controller::get().unwrap().reset();

        // Fails with a retryable UC error, then the request timeout, then succeeds.
        let attempts = Arc::new(AtomicUsize::new(0));
        let inner = tower::service_fn({
            let attempts = Arc::clone(&attempts);
            move |_req: ZerobusRequest| {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                async move {
                    match attempt {
                        0 => Err::<ZerobusResponse, crate::Error>(Box::new(
                            ZerobusSinkError::SchemaError {
                                message: "UC 503".to_string(),
                                retryable: true,
                                status: Some(503),
                            },
                        )),
                        1 => Err::<ZerobusResponse, crate::Error>(Box::new(Elapsed::new())),
                        _ => Ok(ZerobusResponse::delivered(
                            vector_lib::config::telemetry().create_request_count_byte_size(),
                            None,
                        )),
                    }
                }
            }
        });
        let policy = crate::sinks::util::retries::FibonacciRetryPolicy::new(
            5,
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(10),
            ZerobusRetryLogic,
            crate::sinks::util::retries::JitterMode::None,
        );
        let svc = tower::ServiceBuilder::new().retry(policy).service(inner);

        let resp = svc.oneshot(dummy_request()).await.unwrap();
        assert_eq!(resp.status, vector_lib::event::EventStatus::Delivered);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert_eq!(
            retryable_error_counts(),
            vec![
                (
                    Some(REQUEST_TIMEOUT_ERROR_CODE.to_string()),
                    vector_lib::event::MetricValue::Counter { value: 1.0 }
                ),
                (
                    Some("uc_server_error".to_string()),
                    vector_lib::event::MetricValue::Counter { value: 1.0 }
                ),
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn permanent_errors_are_not_counted_as_retryable() {
        use tower::ServiceExt;

        vector_lib::metrics::init_test();
        vector_lib::metrics::Controller::get().unwrap().reset();

        let inner = tower::service_fn(|_req: ZerobusRequest| async move {
            Err::<ZerobusResponse, crate::Error>(Box::new(ZerobusSinkError::EncodingError {
                message: "bad".to_string(),
                schema_constraint: false,
            }))
        });
        let policy = crate::sinks::util::retries::FibonacciRetryPolicy::new(
            5,
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(10),
            ZerobusRetryLogic,
            crate::sinks::util::retries::JitterMode::None,
        );
        let svc = tower::ServiceBuilder::new().retry(policy).service(inner);

        assert!(svc.oneshot(dummy_request()).await.is_err());
        assert!(retryable_error_counts().is_empty());
    }

    #[tokio::test]
    async fn non_retryable_err_propagates() {
        use tower::ServiceExt;
        let inner = tower::service_fn(|_req: ZerobusRequest| async move {
            let err: crate::Error = Box::new(ZerobusSinkError::EncodingError {
                message: "bad".to_string(),
                schema_constraint: true,
            });
            Err::<ZerobusResponse, _>(err)
        });
        let mut svc = RetryableErrorAsErrored { inner };
        let err = svc
            .ready()
            .await
            .unwrap()
            .call(dummy_request())
            .await
            .unwrap_err();
        // Wrapped so the driver labels its metrics with the error's code.
        assert_eq!(
            CodedError::code_of(&err),
            Some("encoding_schema_constraint")
        );
        let coded = err.downcast_ref::<CodedError>().unwrap();
        let zb = coded.error.downcast_ref::<ZerobusSinkError>().unwrap();
        assert!(matches!(zb, ZerobusSinkError::EncodingError { .. }));
    }

    #[tokio::test]
    async fn unknown_err_treated_as_transient() {
        use tower::ServiceExt;
        // Simulate a Tower-layer error that isn't a ZerobusSinkError (e.g.
        // timeout `Elapsed`): conservatively becomes Errored, not Rejected.
        #[derive(Debug)]
        struct Other;
        impl std::fmt::Display for Other {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "other")
            }
        }
        impl std::error::Error for Other {}

        let inner = tower::service_fn(|_req: ZerobusRequest| async move {
            let err: crate::Error = Box::new(Other);
            Err::<ZerobusResponse, _>(err)
        });
        let mut svc = RetryableErrorAsErrored { inner };
        let resp = svc
            .ready()
            .await
            .unwrap()
            .call(dummy_request())
            .await
            .unwrap();
        assert_eq!(resp.status, vector_lib::event::EventStatus::Errored);
    }

    #[tokio::test]
    async fn ok_response_passes_through() {
        use tower::ServiceExt;
        let inner = tower::service_fn(|_req: ZerobusRequest| async move {
            Ok::<_, crate::Error>(ZerobusResponse::delivered(
                GroupedCountByteSize::new_untagged(),
                None,
            ))
        });
        let mut svc = RetryableErrorAsErrored { inner };
        let resp = svc
            .ready()
            .await
            .unwrap()
            .call(dummy_request())
            .await
            .unwrap();
        assert_eq!(resp.status, vector_lib::event::EventStatus::Delivered);
    }
}
