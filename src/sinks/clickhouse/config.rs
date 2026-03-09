//! Configuration for the `Clickhouse` sink.

use std::fmt;

use http::{Request, StatusCode, Uri};
use hyper::Body;
use vector_lib::codecs::encoding::format::SchemaProvider;
use vector_lib::codecs::encoding::{ArrowStreamSerializerConfig, BatchSerializerConfig};

use super::{
    headless::HeadlessService,
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
            http::HttpService,
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
    /// dispatched round-robin across all resolved endpoints. Failed endpoints
    /// are automatically removed and re-discovered on the next DNS refresh.
    #[serde(default)]
    pub use_headless_service: bool,

    /// DNS re-resolution interval in seconds when `use_headless_service` is enabled.
    ///
    /// Defaults to 30 seconds.
    #[serde(default)]
    pub dns_refresh_interval_secs: Option<u64>,
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

impl_generate_config_from_default!(ClickhouseConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "clickhouse")]
impl SinkConfig for ClickhouseConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        info!(
            message = "ClickHouse sink build() starting.",
            endpoint = %self.endpoint.uri,
            table = %self.table,
            database = ?self.database,
            format = ?self.format,
            use_headless_service = %self.use_headless_service,
            dns_refresh_interval_secs = ?self.dns_refresh_interval_secs,
            compression = ?self.compression,
            skip_unknown_fields = ?self.skip_unknown_fields,
            date_time_best_effort = %self.date_time_best_effort,
            insert_random_shard = %self.insert_random_shard,
        );

        let endpoint = self.endpoint.with_default_parts().uri;
        info!(
            message = "ClickHouse sink: parsed endpoint URI with default parts.",
            endpoint = %endpoint,
            scheme = ?endpoint.scheme_str(),
            host = ?endpoint.host(),
            port = ?endpoint.port_u16(),
        );

        let auth = self.auth.choose_one(&self.endpoint.auth)?;
        info!(
            message = "ClickHouse sink: authentication configured.",
            has_auth = %auth.is_some(),
        );

        let tls_settings = TlsSettings::from_options(self.tls.as_ref())?;
        info!(
            message = "ClickHouse sink: TLS settings configured.",
            tls_enabled = %self.tls.is_some(),
        );

        let client = HttpClient::new(tls_settings, &cx.proxy)?;
        info!(message = "ClickHouse sink: HTTP client created successfully.");

        let request_limits = self.request.into_settings();
        info!(
            message = "ClickHouse sink: request limits configured.",
            concurrency = ?request_limits.concurrency,
            rate_limit_num = ?request_limits.rate_limit_num,
        );

        let batch_settings = self.batch.into_batcher_settings()?;
        info!(
            message = "ClickHouse sink: batch settings configured.",
            max_bytes = ?batch_settings.size.bytes,
            max_events = ?batch_settings.size.events,
        );

        let database = self.database.clone().unwrap_or_else(|| {
            "default"
                .try_into()
                .expect("'default' should be a valid template")
        });
        info!(
            message = "ClickHouse sink: database resolved.",
            database = %database,
        );

        info!(message = "ClickHouse sink: resolving encoding strategy...");
        let (format, encoder_kind) = self
            .resolve_strategy(&client, &endpoint, &database, auth.as_ref())
            .await?;
        info!(
            message = "ClickHouse sink: encoding strategy resolved.",
            format = %format,
        );

        let request_builder = ClickhouseRequestBuilder {
            compression: self.compression,
            encoder: (self.encoding.clone(), encoder_kind),
        };
        info!(message = "ClickHouse sink: request builder created.");

        if self.use_headless_service {
            info!(
                message = "ClickHouse sink: HEADLESS SERVICE MODE ENABLED - will resolve DNS to individual pod IPs.",
                endpoint = %endpoint,
                dns_refresh_interval_secs = ?self.dns_refresh_interval_secs,
            );
            self.build_headless(
                client,
                endpoint,
                auth,
                request_limits,
                batch_settings,
                database,
                format,
                request_builder,
            )
            .await
        } else {
            info!(
                message = "ClickHouse sink: SINGLE ENDPOINT MODE - using direct endpoint connection.",
                endpoint = %endpoint,
            );
            self.build_single(
                client,
                endpoint,
                auth,
                request_limits,
                batch_settings,
                database,
                format,
                request_builder,
            )
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
    /// Builds the single-endpoint sink (default behavior).
    fn build_single(
        &self,
        client: HttpClient,
        endpoint: Uri,
        auth: Option<Auth>,
        request_limits: TowerRequestSettings,
        batch_settings: BatcherSettings,
        database: Template,
        format: Format,
        request_builder: ClickhouseRequestBuilder,
    ) -> crate::Result<(VectorSink, Healthcheck)> {
        info!(
            message = "ClickHouse sink build_single(): constructing single-endpoint sink.",
            endpoint = %endpoint,
            database = %database,
            table = %self.table,
            format = %format,
        );

        let service_request_builder = ClickhouseServiceRequestBuilder {
            auth: auth.clone(),
            endpoint: endpoint.clone(),
            skip_unknown_fields: self.skip_unknown_fields,
            date_time_best_effort: self.date_time_best_effort,
            insert_random_shard: self.insert_random_shard,
            compression: self.compression,
            query_settings: self.query_settings,
        };
        info!(
            message = "ClickHouse sink build_single(): service request builder created.",
            skip_unknown_fields = ?self.skip_unknown_fields,
            date_time_best_effort = %self.date_time_best_effort,
            insert_random_shard = %self.insert_random_shard,
        );

        let service: HttpService<ClickhouseServiceRequestBuilder, PartitionKey> =
            HttpService::new(client.clone(), service_request_builder);
        info!(message = "ClickHouse sink build_single(): HTTP service created.");

        let service = ServiceBuilder::new()
            .settings(request_limits, ClickhouseRetryLogic::default())
            .service(service);
        info!(message = "ClickHouse sink build_single(): service with retry logic configured.");

        let sink = ClickhouseSink::new(
            batch_settings,
            service,
            database.clone(),
            self.table.clone(),
            format,
            request_builder,
        );
        info!(
            message = "ClickHouse sink build_single(): sink created successfully.",
            database = %database,
            table = %self.table,
        );

        let healthcheck = Box::pin(healthcheck(client, endpoint.clone(), auth));
        info!(
            message = "ClickHouse sink build_single(): healthcheck configured.",
            healthcheck_endpoint = %endpoint,
        );

        Ok((VectorSink::from_event_streamsink(sink), healthcheck))
    }

    /// Builds the headless-service sink with round-robin dispatch across
    /// dynamically resolved pod IPs.
    async fn build_headless(
        &self,
        client: HttpClient,
        endpoint: Uri,
        auth: Option<Auth>,
        request_limits: TowerRequestSettings,
        batch_settings: BatcherSettings,
        database: Template,
        format: Format,
        request_builder: ClickhouseRequestBuilder,
    ) -> crate::Result<(VectorSink, Healthcheck)> {
        info!(
            message = "ClickHouse sink build_headless(): constructing headless service sink.",
            endpoint = %endpoint,
            host = ?endpoint.host(),
            database = %database,
            table = %self.table,
            format = %format,
            dns_refresh_interval_secs = ?self.dns_refresh_interval_secs,
        );

        info!(
            message = "ClickHouse sink build_headless(): initializing HeadlessService with DNS resolution.",
            endpoint = %endpoint,
        );
        let headless = HeadlessService::new(
            client.clone(),
            endpoint.clone(),
            auth.clone(),
            self.skip_unknown_fields,
            self.date_time_best_effort,
            self.insert_random_shard,
            self.compression,
            self.query_settings,
            self.dns_refresh_interval_secs,
        )
        .await?;
        info!(message = "ClickHouse sink build_headless(): HeadlessService created successfully.");

        let service = ServiceBuilder::new()
            .settings(request_limits, ClickhouseRetryLogic::default())
            .service(headless);
        info!(message = "ClickHouse sink build_headless(): service with retry logic configured.");

        let sink = ClickhouseSink::new(
            batch_settings,
            service,
            database.clone(),
            self.table.clone(),
            format,
            request_builder,
        );
        info!(
            message = "ClickHouse sink build_headless(): sink created successfully.",
            database = %database,
            table = %self.table,
        );

        let healthcheck = Box::pin(healthcheck(client, endpoint.clone(), auth));
        info!(
            message = "ClickHouse sink build_headless(): healthcheck configured.",
            healthcheck_endpoint = %endpoint,
        );

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

            let mut arrow_config = match batch_encoding {
                BatchSerializerConfig::ArrowStream(config) => config.clone(),
                _ => {
                    return Err(
                        "'batch_encoding' for ClickHouse must use 'arrow_stream' codec.".into(),
                    );
                }
            };

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
    info!(
        message = "ClickHouse healthcheck: initiating health check.",
        endpoint = %endpoint,
        healthcheck_uri = %uri,
        has_auth = %auth.is_some(),
    );

    let mut request = Request::get(&uri).body(Body::empty()).unwrap();

    if let Some(auth) = auth {
        info!(message = "ClickHouse healthcheck: applying authentication to request.");
        auth.apply(&mut request);
    }

    info!(message = "ClickHouse healthcheck: sending request...");
    let response = client.send(request).await?;
    let status = response.status();

    info!(
        message = "ClickHouse healthcheck: received response.",
        status_code = %status,
        status_canonical = ?status.canonical_reason(),
    );

    match status {
        StatusCode::OK => {
            info!(message = "ClickHouse healthcheck: SUCCESS - endpoint is healthy.");
            Ok(())
        }
        status => {
            info!(
                message = "ClickHouse healthcheck: FAILED - unexpected status code.",
                status = %status,
            );
            Err(HealthcheckError::UnexpectedStatus { status }.into())
        }
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
