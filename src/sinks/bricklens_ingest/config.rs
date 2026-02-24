use std::path::PathBuf;

use vector_lib::configurable::configurable_component;

use crate::{
    config::{AcknowledgementsConfig, GenerateConfig, Input, SinkConfig, SinkContext},
    sinks::{Healthcheck, VectorSink},
    tls::TlsEnableableConfig,
};

use super::sink::BricklensIngestSink;

/// Configuration for the `bricklens_ingest` sink.
#[configurable_component(sink(
    "bricklens_ingest",
    "Send logs to Databricks Bricklens Ingest service via gRPC with dynamic protobuf."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct BricklensIngestConfig {
    /// The gRPC endpoint address (e.g., "bricklens-ingest-internal.staging.dbns.databricks.com:443").
    #[configurable(metadata(
        docs::examples = "bricklens-ingest-internal.staging.dbns.databricks.com:443"
    ))]
    pub endpoint: String,

    /// Path to the protobuf FileDescriptorSet file.
    ///
    /// This file contains the compiled protobuf definitions for the service.
    /// Generate with: protoc --descriptor_set_out=service.desc --include_imports service.proto
    #[configurable(metadata(docs::examples = "/etc/vector/bricklens.desc"))]
    pub proto_descriptor_path: PathBuf,

    /// The fully qualified service name (e.g., "databricks.bricklensingestinternal.api.v1.BricklensIngestInternalService").
    #[configurable(metadata(
        docs::examples = "databricks.bricklensingestinternal.api.v1.BricklensIngestInternalService"
    ))]
    pub service_name: String,

    /// The gRPC method name (e.g., "BatchCreateLogRecords").
    #[configurable(metadata(docs::examples = "BatchCreateLogRecords"))]
    pub method_name: String,

    /// TLS configuration for the gRPC connection.
    #[configurable(derived)]
    #[serde(default)]
    pub tls: Option<TlsEnableableConfig>,

    /// Event batching behavior.
    ///
    /// Defaults to one event per request (`max_events = 1`) because each gRPC request encodes a
    /// single event. Set a higher `max_events` only if your transform shapes multiple source
    /// events into one event (e.g. as an array field) so that one request carries the whole batch.
    #[configurable(derived)]
    #[serde(default)]
    pub batch: crate::sinks::util::BatchConfig<crate::sinks::util::OneEventPerBatchSettings>,

    /// Request configuration.
    #[configurable(derived)]
    #[serde(default)]
    pub request: crate::sinks::util::TowerRequestConfig,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,
}

impl GenerateConfig for BricklensIngestConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(
            r#"
            endpoint = "http://localhost:9090"
            proto_descriptor_path = "/path/to/service_proto_descriptor.pb"
            service_name = "databricks.bricklensingestinternal.api.v1.BricklensIngestInternalService"
            method_name = "BatchCreateLogRecords"
            "#,
        )
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "bricklens_ingest")]
impl SinkConfig for BricklensIngestConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let sink = BricklensIngestSink::new(self.clone(), cx).await?;
        let healthcheck = sink.healthcheck();

        Ok((VectorSink::from_event_streamsink(sink), healthcheck))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}
