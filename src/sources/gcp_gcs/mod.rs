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

        match self.strategy {
            Strategy::PubSubCustomPoll => Ok(Box::pin(
                self.create_pubsub_ingestor(multiline_config, log_namespace, &cx.proxy)
                    .await?
                    .run(cx, self.acknowledgements, log_namespace),
            )),
        }
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

        let ingestor = pubsub::Ingestor::new(
            self.project.clone(),
            client,
            auth,
            pubsub_config.clone(),
            downloader,
        )
        .await?;

        Ok(ingestor)
    }
}

#[derive(Debug, Snafu)]
enum CreateIngestorError {
    #[snafu(display("Configuration for `pubsub` required when strategy=pub_sub_custom_poll"))]
    ConfigMissing,
    #[snafu(display("`project` must not be empty"))]
    EmptyProject,
}
