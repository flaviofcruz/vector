//! Configuration for the `Clickhouse` sink.

use std::fmt;

use http::{Request, StatusCode, Uri};
use hyper::Body;
use vector_lib::codecs::encoding::format::SchemaProvider;
use vector_lib::codecs::encoding::{ArrowStreamSerializerConfig, BatchSerializerConfig};

use super::{
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

    /// Fallback endpoint used when all headless pod IPs are unreachable.
    ///
    /// Required when `use_headless_service` is true. Must point to a normal
    /// ClusterIP Kubernetes service (not a headless service) so Kubernetes
    /// handles routing internally. Activated when all resolved pod IPs have
    /// been removed due to connection errors; traffic returns to P2C once
    /// the headless DNS refresh re-discovers healthy pods.
    ///
    /// Can be HTTP or HTTPS. If HTTPS, the `tls` block must also be configured
    /// with the appropriate CA certificate — the same `HttpClient` is shared
    /// between the headless pod connections and this fallback. Without a `tls`
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

    /// Builds the direct single-endpoint sink (default behavior, no headless routing).
    fn build_direct(
        &self,
        params: ClickhouseBuildParams,
    ) -> crate::Result<(VectorSink, Healthcheck)> {
        let service_request_builder = ClickhouseServiceRequestBuilder {
            auth: params.svc_config.auth.clone(),
            endpoint: params.endpoint.clone(),
            skip_unknown_fields: params.svc_config.skip_unknown_fields,
            date_time_best_effort: params.svc_config.date_time_best_effort,
            insert_random_shard: params.svc_config.insert_random_shard,
            compression: params.svc_config.compression,
            query_settings: params.svc_config.query_settings,
        };

        let service = HttpService::new(params.client.clone(), service_request_builder);
        self.build_sink_and_healthcheck(params, service)
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
        self.build_sink_and_healthcheck(params, headless)
    }

    /// Wraps an inner service with Tower middleware and constructs the
    /// sink + healthcheck pair. Shared by both single-endpoint and headless
    /// code paths.
    fn build_sink_and_healthcheck<S>(
        &self,
        params: ClickhouseBuildParams,
        inner_service: S,
    ) -> crate::Result<(VectorSink, Healthcheck)>
    where
        S: Service<HttpRequest<PartitionKey>, Response = HttpResponse, Error = crate::Error>
            + Send
            + Clone
            + 'static,
        S::Future: Send + 'static,
    {
        let service = ServiceBuilder::new()
            .settings(params.request_limits, ClickhouseRetryLogic::default())
            .service(inner_service);

        let sink = ClickhouseSink::new(
            params.batch_settings,
            service,
            params.database,
            self.table.clone(),
            params.format,
            params.request_builder,
        );

        let healthcheck = Box::pin(healthcheck(params.client, params.endpoint, params.auth));
        Ok((VectorSink::from_event_streamsink(sink), healthcheck))
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

            self.resolve_arrow_schema(
                client,
                endpoint.to_string(),
                database,
                auth,
                &mut arrow_config,
            )
            .await?;

            let resolved_batch_config = BatchSerializerConfig::ArrowStream(arrow_config);
            let batch_serializer = resolved_batch_config.build()?;
            let encoder = EncoderKind::Batch(BatchEncoder::new(batch_serializer));

            return Ok((Format::ArrowStream, encoder));
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

        // Match the JSONEachRow path's leniency (omitted-column defaults + quoted-integer parsing)
        // so a missing/null or string-typed value doesn't fail the batch on a non-nullable column.
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
}
