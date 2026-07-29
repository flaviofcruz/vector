//! Configuration for the `Clickhouse` sink.

use std::fmt;
use std::time::Duration;

use http::{Request, StatusCode, Uri};
use hyper::Body;
use vector_lib::codecs::encoding::format::SchemaProvider;
use vector_lib::codecs::encoding::{ArrowStreamSerializerConfig, BatchSerializerConfig};

use super::{
    direct_fallback::{
        DirectFallbackService, FALLBACK_MAX_ATTEMPTS, PRIMARY_MAX_ATTEMPTS, RetrySettings,
    },
    headless::{EndpointServiceConfig, HeadlessService},
    request_builder::ClickhouseRequestBuilder,
    service::{ClickhouseRetryLogic, ClickhouseServiceRequestBuilder},
    sink::{ClickhouseSink, PartitionKey},
};
use crate::{
    http::{Auth, HttpClient, MaybeAuth},
    sinks::{
        prelude::*,
        util::{
            RealtimeSizeBasedDefaultBatchSettings, TowerRequestSettings, UriSerde,
            adaptive_concurrency::AdaptiveConcurrencyLimitLayer,
            http::{HttpRequest, HttpResponse, HttpService},
        },
    },
};

/// Data format.
///
/// The format used to parse input/output data.
///
/// [formats]: https://clickhouse.com/docs/en/interfaces/formats
#[configurable_component]
#[derive(Clone, Copy, Debug, Derivative, Eq, PartialEq, Hash)]
#[serde(rename_all = "snake_case")]
#[derivative(Default)]
#[allow(clippy::enum_variant_names)]
pub enum Format {
    #[derivative(Default)]
    /// JSONEachRow.
    JsonEachRow,

    /// JSONAsObject.
    JsonAsObject,

    /// JSONAsString.
    JsonAsString,

    /// ArrowStream (beta).
    #[configurable(metadata(status = "beta"))]
    ArrowStream,
}

impl fmt::Display for Format {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Format::JsonEachRow => write!(f, "JSONEachRow"),
            Format::JsonAsObject => write!(f, "JSONAsObject"),
            Format::JsonAsString => write!(f, "JSONAsString"),
            Format::ArrowStream => write!(f, "ArrowStream"),
        }
    }
}

/// Configuration for the `clickhouse` sink.
#[configurable_component(sink("clickhouse", "Deliver log data to a ClickHouse database."))]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct ClickhouseConfig {
    /// The endpoint of the ClickHouse server.
    #[serde(alias = "host")]
    #[configurable(metadata(docs::examples = "http://localhost:8123"))]
    pub endpoint: UriSerde,

    /// The table that data is inserted into.
    #[configurable(metadata(docs::examples = "mytable"))]
    pub table: Template,

    /// The database that contains the table that data is inserted into.
    #[configurable(metadata(docs::examples = "mydatabase"))]
    pub database: Option<Template>,

    /// The format to parse input data.
    #[serde(default)]
    pub format: Format,

    /// Sets `input_format_skip_unknown_fields`, allowing ClickHouse to discard fields not present in the table schema.
    ///
    /// If left unspecified, use the default provided by the `ClickHouse` server.
    #[serde(default)]
    pub skip_unknown_fields: Option<bool>,

    /// Sets `date_time_input_format` to `best_effort`, allowing ClickHouse to properly parse RFC3339/ISO 8601.
    #[serde(default)]
    pub date_time_best_effort: bool,

    /// Sets `insert_distributed_one_random_shard`, allowing ClickHouse to insert data into a random shard when using Distributed Table Engine.
    #[serde(default)]
    pub insert_random_shard: bool,

    #[configurable(derived)]
    #[serde(default = "Compression::gzip_default")]
    pub compression: Compression,

    #[configurable(derived)]
    #[serde(default, skip_serializing_if = "crate::serde::is_default")]
    pub encoding: Transformer,

    /// The batch encoding configuration for encoding events in batches.
    ///
    /// When specified, events are encoded together as a single batch.
    /// This is mutually exclusive with per-event encoding based on the `format` field.
    #[configurable(derived)]
    #[serde(default)]
    pub batch_encoding: Option<BatchSerializerConfig>,

    #[configurable(derived)]
    #[serde(default)]
    pub batch: BatchConfig<RealtimeSizeBasedDefaultBatchSettings>,

    #[configurable(derived)]
    pub auth: Option<Auth>,

    #[configurable(derived)]
    #[serde(default)]
    pub request: TowerRequestConfig,

    #[configurable(derived)]
    pub tls: Option<TlsConfig>,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,

    #[configurable(derived)]
    #[serde(default)]
    pub query_settings: QuerySettingsConfig,

    /// When true, treat the endpoint as a headless Kubernetes service DNS name.
    ///
    /// The hostname is resolved to individual pod IPs and requests are
    /// load-balanced across all resolved endpoints using the Power of Two
    /// Choices (P2C) algorithm. Failed endpoints are automatically removed
    /// and re-discovered on the next DNS refresh.
    #[serde(default)]
    pub use_headless_service: bool,

    /// DNS re-resolution interval in seconds when `use_headless_service` is enabled.
    ///
    /// Defaults to 30 seconds.
    #[serde(default)]
    pub dns_refresh_interval_secs: Option<u64>,

    /// Fallback endpoint used when the primary endpoint is unreachable.
    ///
    /// Behaves differently depending on `use_headless_service`:
    ///
    /// - **Headless mode** (`use_headless_service: true`): required. Must point to
    ///   a normal ClusterIP Kubernetes service (not a headless service). Activated
    ///   when all resolved pod IPs have been removed due to connection errors;
    ///   traffic returns to P2C once the headless DNS refresh re-discovers healthy
    ///   pods.
    ///
    /// - **Direct mode** (`use_headless_service: false`): optional. Enables a
    ///   per-request two-phase retry — each request is first sent to `endpoint`
    ///   (e.g. `clickhouse-proxy`) and retried there per the `request` settings
    ///   (attempts, Fibonacci backoff, jitter); if the primary still fails, the
    ///   same request fails over to this endpoint (e.g. the direct ClusterIP write
    ///   service) and is retried there the same way. This is the `clickhouse-proxy`
    ///   dual-write path: keep the proxy as the primary, fall back to writing SMK
    ///   directly when the proxy is down. Failover fires after the primary
    ///   exhausts *any* retriable failure — connection-level errors, per-attempt
    ///   timeouts, and retriable HTTP responses (408/429/5xx) alike. A
    ///   non-retriable ClickHouse error (a `5xx` whose body carries a `Code: N` in
    ///   the non-retriable set — see `non_retriable_error_codes`) does **not** fail
    ///   over and is returned as-is, since the same rows would fail identically on
    ///   the fallback.
    ///
    /// Can be HTTP or HTTPS. If HTTPS, the `tls` block must also be configured
    /// with the appropriate CA certificate — the same `HttpClient` is shared
    /// between the primary connections and this fallback. Without a `tls`
    /// block, HTTPS will fail for self-signed or custom-CA certificates.
    #[configurable(metadata(
        docs::examples = "http://cluster-service-write.logging-clickhouse.svc.cluster.local:8123"
    ))]
    #[serde(default)]
    pub fallback_endpoint: Option<UriSerde>,

    /// Maximum number of idle connections to keep per ClickHouse pod IP.
    ///
    /// When `use_headless_service` is true, Vector maintains a separate Hyper
    /// connection pool per pod IP. Setting this to `1` bounds idle connections
    /// to N (one per pod) instead of N × concurrency. Defaults to `1`.
    #[serde(default)]
    pub pool_max_idle_per_host: Option<usize>,

    /// ClickHouse error codes that must not be retried.
    ///
    /// ClickHouse returns most deterministic data errors (e.g. `VIOLATED_CONSTRAINT`,
    /// `CANNOT_CONVERT_TYPE`) over HTTP as status 500 with a body beginning
    /// `Code: {n}. DB::Exception: ...`. Such rows fail identically on every retry,
    /// so retrying only wastes the retry budget (and, in headless mode, fans the
    /// doomed request across pods) before the request is dropped anyway. Codes
    /// listed here are dropped immediately instead.
    ///
    /// Left unset, a built-in default set is used (469, 70, 69, 407, 131, 53, 117).
    /// Set explicitly to override per shard without a Vector binary roll; an empty
    /// list retries every 500. Transient 500s (e.g. `MEMORY_LIMIT_EXCEEDED`) and
    /// bodies without a parseable `Code:` prefix are always retried.
    #[serde(default)]
    #[configurable(metadata(docs::examples = "example_non_retriable_error_codes()"))]
    pub non_retriable_error_codes: Option<Vec<u32>>,
}

fn example_non_retriable_error_codes() -> Vec<u32> {
    vec![469, 70, 69, 407, 131]
}

/// Query settings for the `clickhouse` sink.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct QuerySettingsConfig {
    /// Async insert-related settings.
    #[serde(default)]
    pub async_insert_settings: AsyncInsertSettingsConfig,
}

/// Async insert related settings for the `clickhouse` sink.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct AsyncInsertSettingsConfig {
    /// Sets `async_insert`, allowing ClickHouse to queue the inserted data and later flush to table in the background.
    ///
    /// If left unspecified, use the default provided by the `ClickHouse` server.
    #[serde(default)]
    pub enabled: Option<bool>,

    /// Sets `wait_for`, allowing ClickHouse to wait for processing of asynchronous insertion.
    ///
    /// If left unspecified, use the default provided by the `ClickHouse` server.
    #[serde(default)]
    pub wait_for_processing: Option<bool>,

    /// Sets 'wait_for_processing_timeout`, to control the timeout for waiting for processing asynchronous insertion.
    ///
    /// If left unspecified, use the default provided by the `ClickHouse` server.
    #[serde(default)]
    pub wait_for_processing_timeout: Option<u64>,

    /// Sets `async_insert_deduplicate`, allowing ClickHouse to perform deduplication when inserting blocks in the replicated table.
    ///
    /// If left unspecified, use the default provided by the `ClickHouse` server.
    #[serde(default)]
    pub deduplicate: Option<bool>,

    /// Sets `async_insert_max_data_size`, the maximum size in bytes of unparsed data collected per query before being inserted.
    ///
    /// If left unspecified, use the default provided by the `ClickHouse` server.
    #[serde(default)]
    pub max_data_size: Option<u64>,

    /// Sets `async_insert_max_query_number`, the maximum number of insert queries before being inserted
    ///
    /// If left unspecified, use the default provided by the `ClickHouse` server.
    #[serde(default)]
    pub max_query_number: Option<u64>,
}

/// Maximum retries for headless mode regardless of the global `request.retry_attempts` setting.
///
/// In headless mode each retry may be routed to a different pod IP; with many pods and the
/// default Fibonacci back-off the cumulative wait before reaching the fallback endpoint can
/// exceed two minutes. Capping at 3 keeps the fall-through to the ClusterIP fallback fast.
/// Users can lower this further via `request.retry_attempts`.
const DEFAULT_HEADLESS_MAX_RETRIES: usize = 3;

/// Common parameters needed to build the ClickHouse sink.
struct ClickhouseBuildParams {
    client: HttpClient,
    endpoint: Uri,
    auth: Option<Auth>,
    request_limits: TowerRequestSettings,
    batch_settings: BatcherSettings,
    database: Template,
    format: Format,
    request_builder: ClickhouseRequestBuilder,
    svc_config: EndpointServiceConfig,
    non_retriable_error_codes: Option<Vec<u32>>,
}

impl_generate_config_from_default!(ClickhouseConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "clickhouse")]
impl SinkConfig for ClickhouseConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let endpoint = self.endpoint.with_default_parts().uri;
        let auth = self.auth.choose_one(&self.endpoint.auth)?;
        let tls_settings = TlsSettings::from_options(self.tls.as_ref())?;
        let mut http_client_builder = hyper::Client::builder();
        http_client_builder.pool_max_idle_per_host(self.pool_max_idle_per_host.unwrap_or(1));
        let client =
            HttpClient::new_with_custom_client(tls_settings, &cx.proxy, &mut http_client_builder)?;
        let request_limits = self.request.into_settings();
        let batch_settings = self.batch.into_batcher_settings()?;

        let database = self.database.clone().unwrap_or_else(|| {
            "default"
                .try_into()
                .expect("'default' should be a valid template")
        });

        if self.use_headless_service {
            self.validate_headless_config(&endpoint)?;
        } else if self.dns_refresh_interval_secs.is_some() {
            warn!(
                message = "'dns_refresh_interval_secs' is set but 'use_headless_service' is false; this setting will be ignored.",
            );
        }

        let (format, encoder_kind) = self
            .resolve_strategy(&client, &endpoint, &database, auth.as_ref())
            .await?;

        let request_builder = ClickhouseRequestBuilder {
            compression: self.compression,
            encoder: (self.encoding.clone(), encoder_kind),
        };

        let svc_config = EndpointServiceConfig {
            auth: auth.clone(),
            skip_unknown_fields: self.skip_unknown_fields,
            date_time_best_effort: self.date_time_best_effort,
            insert_random_shard: self.insert_random_shard,
            compression: self.compression,
            query_settings: self.query_settings,
        };

        let params = ClickhouseBuildParams {
            client,
            endpoint,
            auth,
            request_limits,
            batch_settings,
            database,
            format,
            request_builder,
            svc_config,
            non_retriable_error_codes: self.non_retriable_error_codes.clone(),
        };

        if self.use_headless_service {
            self.build_headless(params).await
        } else {
            self.build_direct(params)
        }
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

impl ClickhouseConfig {
    /// Validates configuration fields that are only relevant when `use_headless_service` is true.
    fn validate_headless_config(&self, _endpoint: &Uri) -> crate::Result<()> {
        if self.fallback_endpoint.is_none() {
            return Err(
                "'fallback_endpoint' is required when 'use_headless_service' is true".into(),
            );
        }
        if self.dns_refresh_interval_secs == Some(0) {
            return Err("'dns_refresh_interval_secs' must be greater than 0".into());
        }
        Ok(())
    }

    /// Builds the direct single-endpoint sink (no headless routing).
    ///
    /// When `fallback_endpoint` is set, wraps the primary endpoint in a
    /// [`DirectFallbackService`] that retries the primary (proxy), then fails over
    /// to the fallback (direct SMK) and retries that — the `clickhouse-proxy`
    /// dual-write path. That service owns its retry loop, so the outer Tower retry
    /// layer is disabled and the outer timeout widened to bound the two phases.
    /// Without a `fallback_endpoint`, builds a plain single-endpoint sink.
    fn build_direct(
        &self,
        params: ClickhouseBuildParams,
    ) -> crate::Result<(VectorSink, Healthcheck)> {
        if let Some(fallback) = self.fallback_endpoint.as_ref() {
            let fallback_uri = fallback.with_default_parts().uri;

            // Attempt counts are fixed per endpoint (proxy fails fast, direct SMK
            // gets more tries); only backoff/jitter/timeout come from the request
            // settings. Each endpoint's own retry policy applies these.
            let retry_settings = RetrySettings {
                initial_backoff: params.request_limits.retry_initial_backoff,
                max_backoff: params.request_limits.retry_max_duration,
                jitter_mode: params.request_limits.retry_jitter_mode,
                per_call_timeout: params.request_limits.timeout,
            };

            info!(
                message = "ClickHouse direct sink configured with a retrying fallback endpoint.",
                primary_endpoint = %params.endpoint,
                fallback_endpoint = %fallback_uri,
                primary_max_attempts = PRIMARY_MAX_ATTEMPTS,
                fallback_max_attempts = FALLBACK_MAX_ATTEMPTS,
            );

            let service = DirectFallbackService::new(
                &params.client,
                params.endpoint.clone(),
                fallback_uri.clone(),
                &params.svc_config,
                params.non_retriable_error_codes.clone(),
                retry_settings,
            );

            // Outer timeout bounds both phases: `hops` HTTP calls plus a backoff
            // sleep between the attempts of each endpoint (`hops - 2`), with margin.
            // Saturating throughout so an extreme config can't wrap to a tiny value.
            let hops = u32::try_from(PRIMARY_MAX_ATTEMPTS.saturating_add(FALLBACK_MAX_ATTEMPTS))
                .unwrap_or(u32::MAX);
            let sleeps = hops.saturating_sub(2);
            let hop_budget = retry_settings
                .per_call_timeout
                .checked_mul(hops)
                .unwrap_or(Duration::MAX);
            let sleep_budget = retry_settings
                .max_backoff
                .checked_mul(sleeps)
                .unwrap_or(Duration::MAX);
            let outer_timeout = hop_budget
                .checked_add(sleep_budget)
                .and_then(|d| d.checked_add(Duration::from_secs(5)))
                .unwrap_or(Duration::MAX);

            // Each endpoint owns its retry policy already, so the outer stack omits
            // the retry layer — a second policy would double-count the standard
            // sink retry metrics and log spurious drops on primary exhaustion. The
            // rest of the standard stack (rate limit, adaptive concurrency, outer
            // timeout) is kept.
            let service = ServiceBuilder::new()
                .rate_limit(
                    params.request_limits.rate_limit_num,
                    params.request_limits.rate_limit_duration,
                )
                .layer(AdaptiveConcurrencyLimitLayer::new(
                    params.request_limits.concurrency,
                    params.request_limits.adaptive_concurrency,
                    ClickhouseRetryLogic::new(params.non_retriable_error_codes.clone()),
                ))
                .timeout(outer_timeout)
                .service(service);

            // Probe both endpoints, mirroring the runtime failover rule (see
            // `healthcheck_either`).
            let healthcheck = Box::pin(healthcheck_either(
                params.client.clone(),
                params.endpoint.clone(),
                fallback_uri.clone(),
                params.non_retriable_error_codes.clone(),
                params.auth.clone(),
            )) as Healthcheck;

            return self.build_sink_and_healthcheck(params, service, Some(healthcheck));
        }

        let service_request_builder = ClickhouseServiceRequestBuilder {
            auth: params.svc_config.auth.clone(),
            endpoint: params.endpoint.clone(),
            skip_unknown_fields: params.svc_config.skip_unknown_fields,
            date_time_best_effort: params.svc_config.date_time_best_effort,
            insert_random_shard: params.svc_config.insert_random_shard,
            compression: params.svc_config.compression,
            query_settings: params.svc_config.query_settings,
        };

        let inner = HttpService::new(params.client.clone(), service_request_builder);
        let service = Self::wrap_standard_service(
            params.request_limits.clone(),
            params.non_retriable_error_codes.clone(),
            inner,
        );
        self.build_sink_and_healthcheck(params, service, None)
    }

    /// Builds the headless-service sink with P2C load-balanced dispatch across
    /// dynamically resolved pod IPs.
    async fn build_headless(
        &self,
        mut params: ClickhouseBuildParams,
    ) -> crate::Result<(VectorSink, Healthcheck)> {
        // Cap retries so we fall through to the ClusterIP fallback quickly. Without this,
        // Fibonacci back-off across N pod IPs can take >2 min before active_count hits 0.
        params.request_limits.retry_attempts = params
            .request_limits
            .retry_attempts
            .min(DEFAULT_HEADLESS_MAX_RETRIES);

        let fallback_uri = self
            .fallback_endpoint
            .as_ref()
            .expect("fallback_endpoint validated above")
            .with_default_parts()
            .uri;

        let headless = HeadlessService::new(
            params.client.clone(),
            params.endpoint.clone(),
            params.svc_config.clone(),
            self.dns_refresh_interval_secs,
            fallback_uri.clone(),
            params.request_limits.concurrency,
            params.request_limits.retry_attempts,
        )
        .await?;

        // Healthcheck should target the stable ClusterIP, not the headless DNS name.
        // The headless name resolves to individual pod IPs which may come and go.
        params.endpoint = fallback_uri;
        let service = Self::wrap_standard_service(
            params.request_limits.clone(),
            params.non_retriable_error_codes.clone(),
            headless,
        );
        self.build_sink_and_healthcheck(params, service, None)
    }

    /// Constructs the sink + healthcheck pair from an already-wrapped `service`
    /// (the caller applies the outer Tower middleware). Shared by the
    /// single-endpoint, headless, and direct-fallback code paths.
    ///
    /// Callers wrap the service themselves because the direct-fallback path uses
    /// a different outer stack (no retry layer — each endpoint owns its retry
    /// policy) than the standard `.settings()` stack used by the other paths.
    ///
    /// `healthcheck_override` lets a caller supply its own healthcheck future;
    /// when `None`, a single-endpoint probe of `params.endpoint` is used. The
    /// direct-fallback path passes an override that probes both endpoints so a
    /// healthy fallback isn't blocked by a down primary.
    fn build_sink_and_healthcheck<S>(
        &self,
        params: ClickhouseBuildParams,
        service: S,
        healthcheck_override: Option<Healthcheck>,
    ) -> crate::Result<(VectorSink, Healthcheck)>
    where
        S: Service<HttpRequest<PartitionKey>> + Send + 'static,
        S::Future: Send + 'static,
        S::Response: DriverResponse + Send + 'static,
        S::Error: std::fmt::Debug + Into<crate::Error> + Send,
    {
        let sink = ClickhouseSink::new(
            params.batch_settings,
            service,
            params.database,
            self.table.clone(),
            params.format,
            params.request_builder,
        );

        let healthcheck = healthcheck_override.unwrap_or_else(|| {
            Box::pin(healthcheck(params.client, params.endpoint, params.auth))
        });
        Ok((VectorSink::from_event_streamsink(sink), healthcheck))
    }

    /// Wraps an inner service in the standard Tower request stack (rate limit,
    /// adaptive concurrency, Fibonacci retry, per-request timeout) built from the
    /// sink's request settings. Used by the single-endpoint and headless paths;
    /// the direct-fallback path builds its own retry-less stack instead.
    fn wrap_standard_service<S>(
        request_limits: TowerRequestSettings,
        non_retriable_error_codes: Option<Vec<u32>>,
        inner_service: S,
    ) -> Svc<S, ClickhouseRetryLogic>
    where
        S: Service<HttpRequest<PartitionKey>, Response = HttpResponse, Error = crate::Error>
            + Send
            + 'static,
        S::Future: Send + 'static,
    {
        ServiceBuilder::new()
            .settings(
                request_limits,
                ClickhouseRetryLogic::new(non_retriable_error_codes),
            )
            .service(inner_service)
    }

    /// Resolves the encoding strategy (format + encoder) based on configuration.
    ///
    /// This method determines the appropriate ClickHouse format and Vector encoder
    /// based on the user's configuration, ensuring they are consistent.
    async fn resolve_strategy(
        &self,
        client: &HttpClient,
        endpoint: &Uri,
        database: &Template,
        auth: Option<&Auth>,
    ) -> crate::Result<(Format, vector_lib::codecs::EncoderKind)> {
        use vector_lib::codecs::EncoderKind;
        use vector_lib::codecs::{
            JsonSerializerConfig, NewlineDelimitedEncoderConfig, encoding::Framer,
        };

        if let Some(batch_encoding) = &self.batch_encoding {
            use vector_lib::codecs::BatchEncoder;

            // Validate that batch_encoding is only compatible with ArrowStream format
            if self.format != Format::ArrowStream {
                return Err(format!(
                    "'batch_encoding' is only compatible with 'format: arrow_stream'. Found 'format: {}'.",
                    self.format
                )
                .into());
            }

            let BatchSerializerConfig::ArrowStream(arrow_config) = batch_encoding else {
                return Err(format!(
                    "'batch_encoding.codec' must be 'arrow_stream' for the clickhouse sink. Found '{:?}'.",
                    batch_encoding
                )
                .into());
            };
            let mut arrow_config = arrow_config.clone();

            let arrow_encoder = match self
                .resolve_arrow_schema(
                    client,
                    endpoint.to_string(),
                    database,
                    auth,
                    &mut arrow_config,
                )
                .await
            {
                Ok(()) => BatchSerializerConfig::ArrowStream(arrow_config)
                    .build()
                    .map(|s| EncoderKind::Batch(BatchEncoder::new(s))),
                Err(e) => Err(e),
            };

            match arrow_encoder {
                Ok(encoder) => return Ok((Format::ArrowStream, encoder)),
                // Arrow setup failed: fall back to JSONEachRow. Return JsonEachRow, not self.format
                // (ArrowStream here), so the FORMAT clause matches the JSON encoder.
                Err(error) => {
                    metrics::counter!("clickhouse_arrow_setup_failed_fallback_json").increment(1);
                    warn!(
                        message = "ClickHouse Arrow setup failed; falling back to JSONEachRow.",
                        %error,
                    );
                    let encoder = EncoderKind::Framed(Box::new(Encoder::<Framer>::new(
                        NewlineDelimitedEncoderConfig.build().into(),
                        JsonSerializerConfig::default().build().into(),
                    )));
                    return Ok((Format::JsonEachRow, encoder));
                }
            }
        }

        let encoder = EncoderKind::Framed(Box::new(Encoder::<Framer>::new(
            NewlineDelimitedEncoderConfig.build().into(),
            JsonSerializerConfig::default().build().into(),
        )));

        Ok((self.format, encoder))
    }

    async fn resolve_arrow_schema(
        &self,
        client: &HttpClient,
        endpoint: String,
        database: &Template,
        auth: Option<&Auth>,
        config: &mut ArrowStreamSerializerConfig,
    ) -> crate::Result<()> {
        use super::arrow;

        if self.table.is_dynamic() || database.is_dynamic() {
            return Err(
                "Arrow codec requires a static table and database. Dynamic schema inference is not supported."
                    .into(),
            );
        }

        let table_str = self.table.get_ref();
        let database_str = database.get_ref();

        debug!(
            "Fetching schema for table {}.{} at startup.",
            database_str, table_str
        );

        let provider = arrow::ClickHouseSchemaProvider::new(
            client.clone(),
            endpoint,
            database_str.to_string(),
            table_str.to_string(),
            auth.cloned(),
        );

        let schema = provider.get_schema().await.map_err(|e| {
            format!(
                "Failed to fetch schema for {}.{}: {}.",
                database_str, table_str, e
            )
        })?;

        config.schema = Some(schema);

        // Enable coercion: without it, a missing/null or string-typed value would fail the batch
        // on a non-nullable column.
        config.coerce_missing_to_default = true;

        debug!(
            "Successfully fetched Arrow schema with {} fields.",
            config
                .schema
                .as_ref()
                .map(|s| s.fields().len())
                .unwrap_or(0)
        );

        Ok(())
    }
}

fn get_healthcheck_uri(endpoint: &Uri) -> String {
    let mut uri = endpoint.to_string();
    if !uri.ends_with('/') {
        uri.push('/');
    }
    uri.push_str("?query=SELECT%201");
    uri
}

async fn healthcheck(client: HttpClient, endpoint: Uri, auth: Option<Auth>) -> crate::Result<()> {
    let uri = get_healthcheck_uri(&endpoint);
    let mut request = Request::get(uri).body(Body::empty()).unwrap();

    if let Some(auth) = auth {
        auth.apply(&mut request);
    }

    let response = client.send(request).await?;

    match response.status() {
        StatusCode::OK => Ok(()),
        status => Err(HealthcheckError::UnexpectedStatus { status }.into()),
    }
}

/// Outcome of probing a single endpoint for the direct-fallback healthcheck,
/// classified with the same `ClickhouseRetryLogic` the runtime uses.
enum ProbeOutcome {
    /// Endpoint answered `200` — healthy.
    Healthy,
    /// Endpoint failed in a way the runtime would retry / fail over on
    /// (connection error, timeout, retriable status). A healthy fallback can
    /// legitimately cover this.
    Retriable(crate::Error),
    /// Endpoint answered with a non-retriable status (e.g. 401/404/501). The
    /// runtime returns this without failing over, so a healthy fallback must
    /// NOT mask it.
    NonRetriable(crate::Error),
}

/// Probes one endpoint (`GET /?query=SELECT 1`) and classifies the result with
/// `retry_logic`, mirroring the runtime failover decision in
/// `DirectFallbackService`.
async fn probe_endpoint(
    client: &HttpClient,
    endpoint: Uri,
    auth: Option<Auth>,
    retry_logic: &ClickhouseRetryLogic,
) -> ProbeOutcome {
    let uri = get_healthcheck_uri(&endpoint);
    let mut request = Request::get(uri).body(Body::empty()).unwrap();
    if let Some(auth) = auth {
        auth.apply(&mut request);
    }

    // A transport-level error (connection refused/reset) is retriable at runtime.
    let response = match client.send(request).await {
        Ok(response) => response,
        Err(err) => return ProbeOutcome::Retriable(err.into()),
    };

    let status = response.status();
    if status == StatusCode::OK {
        return ProbeOutcome::Healthy;
    }

    // Read the body so `ClickhouseRetryLogic` can inspect any `Code: N` payload,
    // then classify the response exactly as the runtime would. (Drain via the
    // `HttpBody` trait — `hyper::body::to_bytes` is deprecated under this fork's
    // `#[deny(warnings)]`; the probe response carries no trailers we need.)
    let (parts, body) = response.into_parts();
    let body = http_body::Body::collect(body)
        .await
        .map(|collected| collected.to_bytes())
        .unwrap_or_default();
    let http_response = http::Response::from_parts(parts, body);
    let probe = HttpResponse {
        http_response,
        events_byte_size: Default::default(),
        raw_byte_size: 0,
    };

    let err: crate::Error = HealthcheckError::UnexpectedStatus { status }.into();
    if retry_logic.should_retry_response(&probe).is_retryable() {
        ProbeOutcome::Retriable(err)
    } else {
        ProbeOutcome::NonRetriable(err)
    }
}

/// Healthcheck for the direct-fallback path. Mirrors the runtime failover
/// semantics of `DirectFallbackService`, so a green healthcheck means writes can
/// actually succeed:
///
/// - Both endpoints are probed **concurrently**, so a hanging primary cannot
///   consume the whole healthcheck deadline before the fallback is checked.
/// - A healthy primary passes immediately.
/// - A healthy fallback rescues the healthcheck **only** when the primary's
///   failure is retriable (connection error, timeout, retriable status) — i.e.
///   the cases where the runtime would actually fail over. A non-retriable
///   primary failure (e.g. 401/404/501) fails the healthcheck even if the
///   fallback is healthy, because the runtime returns it without touching the
///   fallback.
async fn healthcheck_either(
    client: HttpClient,
    primary: Uri,
    fallback: Uri,
    non_retriable_error_codes: Option<Vec<u32>>,
    auth: Option<Auth>,
) -> crate::Result<()> {
    // Per-probe cap (under the 10s default outer deadline) so one hanging endpoint
    // can't consume the whole budget. A timed-out probe is treated as retriable,
    // so a healthy fallback still rescues the healthcheck.
    const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

    let retry_logic = ClickhouseRetryLogic::new(non_retriable_error_codes);
    let (primary_res, fallback_res) = future::join(
        tokio::time::timeout(
            PROBE_TIMEOUT,
            probe_endpoint(&client, primary, auth.clone(), &retry_logic),
        ),
        tokio::time::timeout(PROBE_TIMEOUT, healthcheck(client.clone(), fallback, auth)),
    )
    .await;

    // A timed-out probe: retriable primary failure / unhealthy fallback.
    let primary_outcome = primary_res.unwrap_or_else(|_elapsed| {
        ProbeOutcome::Retriable("primary healthcheck probe timed out".into())
    });
    let fallback_result =
        fallback_res.unwrap_or_else(|_elapsed| Err("fallback healthcheck probe timed out".into()));

    match primary_outcome {
        ProbeOutcome::Healthy => Ok(()),
        ProbeOutcome::NonRetriable(primary_err) => Err(format!(
            "ClickHouse primary endpoint healthcheck failed with a non-retriable error; \
             the fallback is not used for this failure at runtime: {primary_err}"
        )
        .into()),
        ProbeOutcome::Retriable(primary_err) => match fallback_result {
            Ok(()) => {
                warn!(
                    message = "ClickHouse primary endpoint healthcheck failed; fallback is healthy.",
                    primary_error = %primary_err,
                );
                Ok(())
            }
            Err(fallback_err) => Err(format!(
                "both ClickHouse endpoints failed healthcheck: primary: {primary_err}; fallback: {fallback_err}"
            )
            .into()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vector_lib::codecs::encoding::ArrowStreamSerializerConfig;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<ClickhouseConfig>();
    }

    #[test]
    fn test_get_healthcheck_uri() {
        assert_eq!(
            get_healthcheck_uri(&"http://localhost:8123".parse().unwrap()),
            "http://localhost:8123/?query=SELECT%201"
        );
        assert_eq!(
            get_healthcheck_uri(&"http://localhost:8123/".parse().unwrap()),
            "http://localhost:8123/?query=SELECT%201"
        );
        assert_eq!(
            get_healthcheck_uri(&"http://localhost:8123/path/".parse().unwrap()),
            "http://localhost:8123/path/?query=SELECT%201"
        );
    }

    /// Helper to create a minimal ClickhouseConfig for testing
    fn create_test_config(
        format: Format,
        batch_encoding: Option<BatchSerializerConfig>,
    ) -> ClickhouseConfig {
        ClickhouseConfig {
            endpoint: "http://localhost:8123".parse::<http::Uri>().unwrap().into(),
            table: "test_table".try_into().unwrap(),
            database: Some("test_db".try_into().unwrap()),
            format,
            batch_encoding,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_format_selection_with_batch_encoding() {
        use crate::http::HttpClient;
        use crate::tls::TlsSettings;

        // Create minimal dependencies for resolve_strategy
        let tls = TlsSettings::default();
        let client = HttpClient::new(tls, &Default::default()).unwrap();
        let endpoint: http::Uri = "http://localhost:8123".parse().unwrap();
        let database: Template = "test_db".try_into().unwrap();

        // Test incompatible formats - should all return errors
        let incompatible_formats = vec![
            (Format::JsonEachRow, "json_each_row"),
            (Format::JsonAsObject, "json_as_object"),
            (Format::JsonAsString, "json_as_string"),
        ];

        for (format, format_name) in incompatible_formats {
            let config = create_test_config(
                format,
                Some(BatchSerializerConfig::ArrowStream(
                    ArrowStreamSerializerConfig::default(),
                )),
            );

            let result = config
                .resolve_strategy(&client, &endpoint, &database, None)
                .await;

            assert!(
                result.is_err(),
                "Expected error for format {} with batch_encoding, but got success",
                format_name
            );
        }
    }

    #[test]
    fn test_format_selection_without_batch_encoding() {
        // When batch_encoding is None, the configured format should be used
        let configs = vec![
            Format::JsonEachRow,
            Format::JsonAsObject,
            Format::JsonAsString,
            Format::ArrowStream,
        ];

        for format in configs {
            let config = create_test_config(format, None);

            assert!(
                config.batch_encoding.is_none(),
                "batch_encoding should be None for format {:?}",
                format
            );
            assert_eq!(
                config.format, format,
                "format should match configured value"
            );
        }
    }

    #[tokio::test]
    async fn test_arrow_schema_failure_falls_back_to_json() {
        use crate::http::HttpClient;
        use crate::tls::TlsSettings;

        let tls = TlsSettings::default();
        let client = HttpClient::new(tls, &Default::default()).unwrap();
        // Unroutable port so the schema fetch fails.
        let endpoint: http::Uri = "http://127.0.0.1:1".parse().unwrap();
        let database: Template = "test_db".try_into().unwrap();

        let config = create_test_config(
            Format::ArrowStream,
            Some(BatchSerializerConfig::ArrowStream(
                ArrowStreamSerializerConfig::default(),
            )),
        );

        let (format, _encoder) = config
            .resolve_strategy(&client, &endpoint, &database, None)
            .await
            .expect("Arrow setup failure should fall back, not error");

        assert_eq!(
            format,
            Format::JsonEachRow,
            "on Arrow setup failure the sink must fall back to JSONEachRow"
        );
    }

    mod healthcheck_either {
        use std::convert::Infallible;
        use std::net::SocketAddr;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        use hyper::service::{make_service_fn, service_fn};
        use hyper::{Body, Response as HyperResponse, Server};

        use super::super::{Auth, HttpClient, Uri, healthcheck_either};
        use crate::config::ProxyConfig;
        use crate::test_util::addr::next_addr;

        /// Spawns a server that answers every request with `status` and `body`,
        /// returning a hit counter so tests can assert whether an endpoint was
        /// probed. If `hang` is true, the handler never responds — used to model
        /// a blackholing endpoint.
        fn spawn_server(
            addr: SocketAddr,
            status: u16,
            body: &'static str,
            hang: bool,
        ) -> Arc<AtomicUsize> {
            let hits = Arc::new(AtomicUsize::new(0));
            let hits_srv = hits.clone();
            let make = make_service_fn(move |_| {
                let hits = hits_srv.clone();
                async move {
                    Ok::<_, Infallible>(service_fn(move |_req| {
                        let hits = hits.clone();
                        async move {
                            hits.fetch_add(1, Ordering::SeqCst);
                            if hang {
                                // Never respond: model a proxy that accepts the
                                // connection but blackholes the request.
                                std::future::pending::<()>().await;
                            }
                            Ok::<_, Infallible>(
                                HyperResponse::builder()
                                    .status(status)
                                    .body(Body::from(body))
                                    .unwrap(),
                            )
                        }
                    }))
                }
            });
            tokio::spawn(async move {
                let _ = Server::bind(&addr).serve(make).await;
            });
            hits
        }

        fn uri(addr: SocketAddr) -> Uri {
            format!("http://{}:{}/", addr.ip(), addr.port())
                .parse()
                .unwrap()
        }

        fn client() -> HttpClient {
            HttpClient::new(None, &ProxyConfig::default()).unwrap()
        }

        /// Endpoint spec for a probe: status, body, and whether it hangs.
        /// `None` leaves the port unbound (connection refused).
        type Endpoint = Option<(u16, &'static str, bool)>;

        async fn run_ext(
            primary: Endpoint,
            fallback: Endpoint,
            non_retriable_codes: Option<Vec<u32>>,
        ) -> (usize, usize, crate::Result<()>) {
            let (_pg, p_addr) = next_addr();
            let (_fg, fb_addr) = next_addr();
            let no_hits = || Arc::new(AtomicUsize::new(0));
            let p_hits = primary.map_or_else(no_hits, |(s, b, h)| spawn_server(p_addr, s, b, h));
            let fb_hits = fallback.map_or_else(no_hits, |(s, b, h)| spawn_server(fb_addr, s, b, h));
            // Give the spawned servers a moment to bind.
            tokio::time::sleep(Duration::from_millis(50)).await;
            let res = healthcheck_either(
                client(),
                uri(p_addr),
                uri(fb_addr),
                non_retriable_codes,
                None::<Auth>,
            )
            .await;
            (
                p_hits.load(Ordering::SeqCst),
                fb_hits.load(Ordering::SeqCst),
                res,
            )
        }

        /// Convenience: healthy-vs-status endpoints, no hang, no custom codes.
        async fn run(primary: Option<u16>, fallback: Option<u16>) -> (usize, usize, crate::Result<()>) {
            run_ext(
                primary.map(|s| (s, "", false)),
                fallback.map(|s| (s, "", false)),
                None,
            )
            .await
        }

        #[tokio::test]
        async fn primary_healthy_passes() {
            let (_p, _fb, res) = run(Some(200), Some(200)).await;
            assert!(res.is_ok());
        }

        #[tokio::test]
        async fn primary_down_falls_back_to_healthy_fallback() {
            // Primary port unbound (connection refused, retriable); fallback answers 200.
            let (p, fb, res) = run(None, Some(200)).await;
            assert!(res.is_ok(), "a healthy fallback must satisfy the healthcheck");
            assert_eq!((p, fb), (0, 1));
        }

        #[tokio::test]
        async fn retriable_primary_status_falls_back() {
            // Primary returns 503 (retriable); fallback answers 200 → green.
            let (_p, _fb, res) = run(Some(503), Some(200)).await;
            assert!(res.is_ok());
        }

        #[tokio::test]
        async fn non_retriable_primary_fails_even_with_healthy_fallback() {
            // Primary returns 401 (non-retriable): at runtime every write returns
            // this without touching the fallback, so a healthy fallback must NOT
            // make the healthcheck green.
            let (_p, _fb, res) =
                run_ext(Some((401, "", false)), Some((200, "", false)), None).await;
            assert!(
                res.is_err(),
                "a non-retriable primary must fail the healthcheck even with a healthy fallback"
            );
        }

        #[tokio::test]
        async fn non_retriable_clickhouse_code_fails_even_with_healthy_fallback() {
            // Primary returns 500 with a non-retriable ClickHouse Code: 70 body;
            // runtime does not fail over, so the healthcheck must be red.
            let (_p, _fb, res) = run_ext(
                Some((500, "Code: 70. DB::Exception", false)),
                Some((200, "", false)),
                Some(vec![70]),
            )
            .await;
            assert!(res.is_err());
        }

        #[tokio::test]
        async fn hanging_primary_still_probes_fallback_within_deadline() {
            // Primary accepts the connection but never responds. Because both
            // endpoints are probed concurrently with a per-probe timeout, the
            // hanging primary surfaces as a (retriable) timeout while the healthy
            // fallback satisfies the healthcheck — rather than the hang consuming
            // the whole budget. Resolves by the ~5s per-probe cap, well under the
            // real 10s outer deadline.
            let res = tokio::time::timeout(
                Duration::from_secs(8),
                run_ext(Some((200, "", true)), Some((200, "", false)), None),
            )
            .await
            .expect("healthcheck must resolve within the outer deadline despite a hanging primary");
            assert!(res.2.is_ok(), "healthy fallback should satisfy the healthcheck");
        }

        #[tokio::test]
        async fn both_endpoints_down_errors() {
            let (p, fb, res) = run(None, None).await;
            assert!(res.is_err(), "healthcheck must fail when neither endpoint is healthy");
            assert_eq!((p, fb), (0, 0));
        }
    }
}
