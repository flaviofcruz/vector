use std::convert::TryInto;

use snafu::Snafu;
use vector_lib::{
    codecs::{
        NewlineDelimitedDecoderConfig,
        decoding::{DeserializerConfig, FramingConfig, NewlineDelimitedDecoderOptions},
    },
    config::{LegacyKey, LogNamespace},
    configurable::configurable_component,
    lookup::owned_value_path,
};
use vrl::value::Kind;

use super::util::MultilineConfig;
pub use super::util::object_storage_compression::Compression;
use crate::{
    codecs::DecodingConfig,
    config::{SourceAcknowledgementsConfig, SourceConfig, SourceContext, SourceOutput},
    gcp::{GcpAuthConfig, Scope},
    http::HttpClient,
    line_agg,
    serde::{bool_or_struct, default_decoding},
    sources::ingestion_callback::IngestionCallbackConfig,
    tls::{TlsConfig, TlsSettings},
};

pub mod object;
pub mod pubsub;

/// Strategies for consuming objects from GCS.
#[configurable_component]
#[derive(Clone, Copy, Debug, Derivative)]
#[serde(rename_all = "snake_case")]
#[derivative(Default)]
enum Strategy {
    /// Consumes objects by polling a GCP Pub/Sub subscription for custom INGEST messages.
    ///
    /// Each message must use the direct-ingest format:
    /// `{"kind": "INGEST", "bucket": "...", "key": "...", "file_id": "..."}`.
    #[derivative(Default)]
    PubSubCustomPoll,

    /// Consumes objects by polling a GCP Pub/Sub subscription bound to a bucket's native GCS
    /// notifications, ingesting each `OBJECT_FINALIZE` event. The GCP analog of `aws_s3`'s SQS
    /// strategy and `azure_blob`'s Event Grid queue.
    GcsNativeNotifications,
}

/// Configuration for the `gcp_gcs` source.
#[configurable_component(source("gcp_gcs", "Collect logs from GCP Cloud Storage."))]
#[derive(Clone, Debug, Derivative)]
#[derivative(Default)]
#[serde(default, deny_unknown_fields)]
pub struct GcpGcsConfig {
    /// The GCP project ID.
    #[configurable(metadata(docs::examples = "my-project"))]
    pub project: String,

    /// The compression scheme used for decompressing objects retrieved from GCS.
    compression: Compression,

    /// The strategy to use to consume objects from GCS.
    #[configurable(metadata(docs::hidden))]
    strategy: Strategy,

    /// Configuration options for Pub/Sub.
    pubsub: Option<pubsub::Config>,

    /// Authentication configuration for GCP services.
    ///
    /// Supported mechanisms (in priority order):
    /// - `token`: a short-lived bearer token (e.g. from a secret backend)
    /// - `credentials_path`: path to a service account JSON key file
    /// - `api_key`: a GCP API key
    /// - Implicit: GCE/GKE metadata server when running on Google infrastructure,
    ///   or `GOOGLE_APPLICATION_CREDENTIALS` environment variable
    #[configurable(derived)]
    #[serde(flatten)]
    pub auth: GcpAuthConfig,

    /// Multiline aggregation configuration.
    ///
    /// If not specified, multiline aggregation is disabled.
    #[configurable(derived)]
    multiline: Option<MultilineConfig>,

    #[configurable(derived)]
    #[serde(default, deserialize_with = "bool_or_struct")]
    acknowledgements: SourceAcknowledgementsConfig,

    /// TLS configuration.
    #[configurable(derived)]
    #[serde(default)]
    tls: Option<TlsConfig>,

    /// The namespace to use for logs. This overrides the global setting.
    #[configurable(metadata(docs::hidden))]
    #[serde(default)]
    log_namespace: Option<bool>,

    #[configurable(derived)]
    #[serde(default = "default_framing")]
    #[derivative(Default(value = "default_framing()"))]
    pub framing: FramingConfig,

    #[configurable(derived)]
    #[serde(default = "default_decoding")]
    #[derivative(Default(value = "default_decoding()"))]
    pub decoding: DeserializerConfig,

    /// Optional ingestion callback configuration.
    ///
    /// When present, the source fires HTTP callbacks to notify an upstream
    /// service after a direct-ingest file finishes processing.
    #[configurable(derived)]
    pub ingestion_callback: Option<IngestionCallbackConfig>,
}

const fn default_framing() -> FramingConfig {
    FramingConfig::NewlineDelimited(NewlineDelimitedDecoderConfig {
        newline_delimited: NewlineDelimitedDecoderOptions { max_length: None },
    })
}

impl_generate_config_from_default!(GcpGcsConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "gcp_gcs")]
impl SourceConfig for GcpGcsConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        let log_namespace = cx.log_namespace(self.log_namespace);

        let multiline_config: Option<line_agg::Config> = self
            .multiline
            .as_ref()
            .map(|config| config.try_into())
            .transpose()?;

        let message_format = match self.strategy {
            Strategy::PubSubCustomPoll => pubsub::MessageFormat::DirectIngest,
            Strategy::GcsNativeNotifications => pubsub::MessageFormat::GcsNative,
        };
        Ok(Box::pin(
            self.create_pubsub_ingestor(multiline_config, log_namespace, &cx.proxy, message_format)
                .await?
                .run(cx, self.acknowledgements, log_namespace),
        ))
    }

    fn outputs(&self, global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        let log_namespace = global_log_namespace.merge(self.log_namespace);
        let mut schema_definition = self
            .decoding
            .schema_definition(log_namespace)
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::Overwrite(owned_value_path!("bucket"))),
                &owned_value_path!("bucket"),
                Kind::bytes(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::Overwrite(owned_value_path!("object"))),
                &owned_value_path!("object"),
                Kind::bytes(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::Overwrite(owned_value_path!("project"))),
                &owned_value_path!("project"),
                Kind::bytes(),
                None,
            )
            .with_standard_vector_source_metadata();

        if log_namespace == LogNamespace::Legacy {
            schema_definition = schema_definition.unknown_fields(Kind::bytes());
        }

        vec![SourceOutput::new_maybe_logs(
            self.decoding.output_type(),
            schema_definition,
        )]
    }

    fn can_acknowledge(&self) -> bool {
        true
    }
}

impl GcpGcsConfig {
    async fn create_pubsub_ingestor(
        &self,
        multiline: Option<line_agg::Config>,
        log_namespace: LogNamespace,
        proxy: &crate::config::ProxyConfig,
        message_format: pubsub::MessageFormat,
    ) -> crate::Result<pubsub::Ingestor> {
        if self.project.is_empty() {
            return Err(CreateIngestorError::EmptyProject.into());
        }

        let pubsub_config = self
            .pubsub
            .as_ref()
            .ok_or(CreateIngestorError::ConfigMissing)?;

        let decoder =
            DecodingConfig::new(self.framing.clone(), self.decoding.clone(), log_namespace)
                .build()?;

        // Build a shared GCP authenticator covering both GCS and Pub/Sub.
        // CloudPlatform scope grants access to both services.
        let auth = self.auth.build(Scope::CloudPlatform).await?;

        let tls =
            TlsSettings::from_options(self.tls.as_ref()).map_err(|e| format!("TLS error: {e}"))?;
        let client = HttpClient::new(tls, proxy).map_err(|e| format!("{e}"))?;

        let downloader = std::sync::Arc::new(object::GcsDownloader::new(
            client.clone(),
            auth.clone(),
            self.compression,
            decoder,
            multiline,
            self.project.clone(),
        ));

        let callback_client = self
            .ingestion_callback
            .as_ref()
            .map(|cb_config| {
                crate::sources::ingestion_callback::IngestionCallbackClient::new(cb_config, proxy)
            })
            .transpose()
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let ingestor = pubsub::Ingestor::new(
            self.project.clone(),
            client,
            auth,
            pubsub_config.clone(),
            downloader,
            callback_client,
            message_format,
        )
        .await?;

        Ok(ingestor)
    }
}

#[derive(Debug, Snafu)]
enum CreateIngestorError {
    #[snafu(display("Configuration for `pubsub` required for a Pub/Sub-based strategy"))]
    ConfigMissing,
    #[snafu(display("`project` must not be empty"))]
    EmptyProject,
}

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------------
    // GcpGcsConfig TOML parsing
    // -------------------------------------------------------------------------

    #[test]
    fn config_minimal_valid() {
        let config: Result<GcpGcsConfig, _> = toml::from_str(
            r#"
            project = "my-project"
            [pubsub]
            subscription = "my-sub"
            "#,
        );
        assert!(config.is_ok(), "minimal config should parse: {config:?}");
        let config = config.unwrap();
        assert_eq!(config.project, "my-project");
        assert_eq!(config.pubsub.unwrap().subscription, "my-sub");
        assert_eq!(config.compression, Compression::Auto);
    }

    #[test]
    fn config_with_compression() {
        for (value, expected) in [
            ("auto", Compression::Auto),
            ("none", Compression::None),
            ("gzip", Compression::Gzip),
            ("zstd", Compression::Zstd),
        ] {
            let toml = format!(
                r#"
                project = "p"
                compression = "{value}"
                [pubsub]
                subscription = "s"
                "#
            );
            let config: GcpGcsConfig = toml::from_str(&toml)
                .unwrap_or_else(|e| panic!("compression={value} should parse: {e}"));
            assert_eq!(config.compression, expected, "compression={value}");
        }
    }

    #[test]
    fn config_invalid_compression_rejected() {
        let config: Result<GcpGcsConfig, _> = toml::from_str(
            r#"
            project = "p"
            compression = "lz4"
            [pubsub]
            subscription = "s"
            "#,
        );
        assert!(config.is_err(), "unknown compression value should fail");
    }

    #[test]
    fn config_unknown_fields_rejected() {
        let config: Result<GcpGcsConfig, _> = toml::from_str(
            r#"
            project = "p"
            unknown_field = "oops"
            [pubsub]
            subscription = "s"
            "#,
        );
        assert!(
            config.is_err(),
            "unknown fields should be rejected by deny_unknown_fields"
        );
    }

    #[test]
    fn config_without_pubsub_block_is_valid_toml() {
        // Missing pubsub is valid TOML (optional field) but fails at build() time.
        let config: Result<GcpGcsConfig, _> = toml::from_str(
            r#"
            project = "p"
            "#,
        );
        assert!(
            config.is_ok(),
            "pubsub block is optional at parse time (enforced at build)"
        );
        assert!(config.unwrap().pubsub.is_none());
    }

    #[test]
    fn config_missing_project_uses_default_empty_string() {
        let config: Result<GcpGcsConfig, _> = toml::from_str(
            r#"
            [pubsub]
            subscription = "s"
            "#,
        );
        assert!(config.is_ok());
        assert_eq!(config.unwrap().project, "");
    }

    #[test]
    fn config_with_auth_credentials_path() {
        let config: Result<GcpGcsConfig, _> = toml::from_str(
            r#"
            project = "p"
            credentials_path = "/path/to/key.json"
            [pubsub]
            subscription = "s"
            "#,
        );
        assert!(config.is_ok(), "credentials_path should parse: {config:?}");
        let config = config.unwrap();
        assert_eq!(
            config.auth.credentials_path.as_deref(),
            Some("/path/to/key.json")
        );
    }

    #[test]
    fn config_with_all_pubsub_options() {
        let config: Result<GcpGcsConfig, _> = toml::from_str(
            r#"
            project = "my-project"
            compression = "gzip"

            [pubsub]
            subscription = "my-sub"
            poll_secs = 30
            max_number_of_messages = 100
            acknowledge_message = true
            acknowledge_failed_message = false
            client_concurrency = 8
            "#,
        );
        assert!(config.is_ok(), "full config should parse: {config:?}");
        let config = config.unwrap();
        let pubsub = config.pubsub.unwrap();
        assert_eq!(pubsub.poll_secs, 30);
        assert_eq!(pubsub.max_number_of_messages, 100);
        assert!(pubsub.acknowledge_message);
        assert!(!pubsub.acknowledge_failed_message);
        assert_eq!(
            pubsub.client_concurrency,
            Some(std::num::NonZeroUsize::new(8).unwrap())
        );
    }

    #[test]
    fn config_with_ingestion_callback() {
        let config: Result<GcpGcsConfig, _> = toml::from_str(
            r#"
            project = "my-project"

            [pubsub]
            subscription = "my-sub"

            [ingestion_callback]

            [ingestion_callback.on_success]
            uri = "/v2/files/{{message.file_id}}/mark-successful"

            [ingestion_callback.on_failure]
            uri = "/v2/files/{{message.file_id}}/mark-failed"

            [ingestion_callback.on_failure.body]
            file_id = "{{message.file_id}}"
            error_message = "{{error_message}}"

            [ingestion_callback.request]
            base_url = "https://log-access.example.com"
            timeout_secs = 5
            retry_max_attempts = 2

            [ingestion_callback.auth]
            strategy = "bearer"
            token = "my-token"
            "#,
        );
        assert!(
            config.is_ok(),
            "ingestion_callback config must parse: {config:?}"
        );
        let cb = config
            .unwrap()
            .ingestion_callback
            .expect("ingestion_callback must be present");

        assert!(cb.on_success.is_some());
        assert!(cb.on_failure.is_some());
        assert_eq!(cb.request.base_url, "https://log-access.example.com");
        assert_eq!(cb.request.timeout_secs, 5);
        assert_eq!(cb.request.retry_max_attempts, 2);
        assert!(cb.auth.is_some());

        let on_failure = cb.on_failure.unwrap();
        assert_eq!(on_failure.body.len(), 2);
        assert_eq!(on_failure.body["file_id"], "{{message.file_id}}");
        assert_eq!(on_failure.body["error_message"], "{{error_message}}");
    }
}

#[cfg(test)]
mod ingestor_tests {
    use std::sync::Arc;

    use vector_lib::codecs::{
        NewlineDelimitedDecoderConfig,
        decoding::{FramingConfig, NewlineDelimitedDecoderOptions},
    };
    use vector_lib::config::LogNamespace;

    use crate::codecs::DecodingConfig;
    use crate::config::ProxyConfig;
    use crate::gcp::GcpAuthenticator;
    use crate::http::HttpClient;
    use crate::serde::default_decoding;
    use crate::tls::TlsSettings;

    use super::Compression;
    use super::object::GcsDownloader;
    use super::pubsub::{Config as PubSubConfig, Ingestor, IngestorNewError};

    fn make_test_downloader() -> Arc<GcsDownloader> {
        let framing = FramingConfig::NewlineDelimited(NewlineDelimitedDecoderConfig {
            newline_delimited: NewlineDelimitedDecoderOptions { max_length: None },
        });
        let client = HttpClient::new(TlsSettings::default(), &ProxyConfig::default())
            .expect("HttpClient must build");
        let decoder = DecodingConfig::new(framing, default_decoding(), LogNamespace::Legacy)
            .build()
            .expect("Decoder must build");
        Arc::new(GcsDownloader::new(
            client,
            GcpAuthenticator::None,
            Compression::Auto,
            decoder,
            None,
            "test-project".into(),
        ))
    }

    async fn build_ingestor(max_number_of_messages: u32) -> Result<Ingestor, IngestorNewError> {
        let client = HttpClient::new(TlsSettings::default(), &ProxyConfig::default())
            .expect("HttpClient must build");
        let config = PubSubConfig {
            subscription: "test-sub".into(),
            max_number_of_messages,
            ..Default::default()
        };
        Ingestor::new(
            "test-project".into(),
            client,
            GcpAuthenticator::None,
            config,
            make_test_downloader(),
            None,
            super::pubsub::MessageFormat::DirectIngest,
        )
        .await
    }

    /// The Pub/Sub pull API requires at least 1 message per request.
    /// Verify Ingestor::new rejects 0 before doing any network calls.
    #[tokio::test]
    async fn ingestor_rejects_zero_max_messages() {
        let result = build_ingestor(0).await;
        assert!(
            matches!(
                result,
                Err(IngestorNewError::InvalidNumberOfMessages { messages: 0 })
            ),
            "0 must be rejected (Pub/Sub requires at least 1)"
        );
    }

    /// The Pub/Sub pull API caps a single request at 1000 messages.
    /// Verify Ingestor::new rejects values above 1000.
    #[tokio::test]
    async fn ingestor_rejects_max_messages_over_1000() {
        let result = build_ingestor(1001).await;
        assert!(
            matches!(
                result,
                Err(IngestorNewError::InvalidNumberOfMessages { messages: 1001 })
            ),
            "1001 must be rejected (Pub/Sub caps at 1000)"
        );
    }

    /// 1000 is the upper boundary and must be accepted.
    #[tokio::test]
    async fn ingestor_accepts_max_messages_at_1000() {
        let result = build_ingestor(1000).await;
        assert!(result.is_ok(), "1000 (upper bound) must be accepted");
    }

    /// 1 is the lower boundary and must be accepted.
    #[tokio::test]
    async fn ingestor_accepts_max_messages_at_1() {
        let result = build_ingestor(1).await;
        assert!(result.is_ok(), "1 (lower bound) must be accepted");
    }
}
