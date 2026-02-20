// Azure Delta table sink configuration and schema management
// Defines connection parameters, table schemas, and processing options for Delta table operations.

use std::sync::Arc;

use tower::ServiceBuilder;
use vector_lib::configurable::configurable_component;
use vector_lib::sensitive_string::SensitiveString;

use crate::config::{
    AcknowledgementsConfig, DataType, GenerateConfig, Input, SinkConfig, SinkContext,
};
use crate::sinks::util::service::TowerRequestConfigDefaults;
use crate::sinks::util::{
    BatchConfig, BulkSizeBasedDefaultBatchSettings, ServiceBuilderExt, TowerRequestConfig,
};
use crate::sinks::{Healthcheck, VectorSink};

use super::request_builder::AzureDeltaRequestOptions;
use super::service::{AzureDeltaRetryLogic, AzureDeltaService, DeltaTable};
use super::sink::AzureDeltaSink;

/// Rate limiting defaults for Azure Delta table operations.
/// Configures maximum requests per second to avoid overwhelming Azure Storage.
#[derive(Clone, Copy, Debug, Default)]
pub struct AzureDeltaTowerRequestConfigDefaults;

/// Partition column configuration for Delta table optimization.
/// Defines column name and its data type for partitioning.
#[configurable_component]
#[derive(Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PartitionColumn {
    /// Name of the column to partition by
    #[configurable(metadata(docs::examples = "date"))]
    pub column_name: String,

    /// Data type of the partition column
    #[configurable(metadata(docs::examples = "string"))]
    pub column_type: String,
}

impl TowerRequestConfigDefaults for AzureDeltaTowerRequestConfigDefaults {
    const RATE_LIMIT_NUM: u64 = 250;
}

/// Configuration for Azure Delta table sink operations.
/// Handles connection parameters, table location, schema, and processing options.
#[configurable_component(sink("azure_delta"))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct AzureDeltaSinkConfig {
    /// Azure Storage connection string with SharedAccessSignature token
    /// Must include the SAS token for authentication to Azure Storage
    #[configurable(metadata(
        docs::examples = "BlobEndpoint=https://account.blob.core.windows.net/;SharedAccessSignature=sv=..."
    ))]
    pub connection_string: SensitiveString,

    /// Azure Storage account name for the target storage account
    #[configurable(metadata(docs::examples = "mystorageaccount"))]
    pub storage_account: String,

    /// Full path to the Delta table in Azure Data Lake Storage
    /// Must use abfss:// protocol and include container, account, and table path
    #[configurable(metadata(
        docs::examples = "abfss://container@account.dfs.core.windows.net/tables/db/table_name"
    ))]
    pub table_path: String,

    /// Azure Blob Storage container name where the Delta table is stored
    #[configurable(metadata(docs::examples = "my-container-name"))]
    pub container: String,

    /// Partition columns for Delta table optimization
    /// List of columns with their data types for partitioning the Delta table
    #[configurable(metadata(
        docs::examples = "[{column_name = 'date', column_type = 'string'}, {column_name = 'int_field', column_type = 'int'}]"
    ))]
    #[serde(default)]
    pub partition_columns: Vec<PartitionColumn>,

    /// Batch processing configuration for grouping events before writing
    #[configurable(derived)]
    #[serde(default)]
    pub batch: BatchConfig<BulkSizeBasedDefaultBatchSettings>,

    /// Request processing configuration including rate limiting and retries
    #[configurable(derived)]
    #[serde(default)]
    pub request: TowerRequestConfig<AzureDeltaTowerRequestConfigDefaults>,

    /// Event acknowledgment configuration for delivery guarantees
    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,
}

/// Generates example configuration for the Azure Delta sink.
/// Provides a complete working example with all required fields.
impl GenerateConfig for AzureDeltaSinkConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            connection_string: SensitiveString::from(
                "BlobEndpoint=https://account.blob.core.windows.net/;SharedAccessSignature=sv=..."
                    .to_string(),
            ),
            storage_account: "mystorageaccount".to_string(),
            table_path:
                "abfss://container@account.dfs.core.windows.net/tables/events/kubernetes_events"
                    .to_string(),
            container: "my-container".to_string(),
            batch: BatchConfig::default(),
            request: TowerRequestConfig::default(),
            acknowledgements: Default::default(),
            partition_columns: vec![
                PartitionColumn {
                    column_name: "date".to_string(),
                    column_type: "string".to_string(),
                },
                PartitionColumn {
                    column_name: "int_field".to_string(),
                    column_type: "int".to_string(),
                },
            ],
        })
        .unwrap()
    }
}

/// Implements Vector's SinkConfig trait for Azure Delta sink.
/// Handles sink initialization, health checks, and input type validation.
#[async_trait::async_trait]
#[typetag::serde(name = "azure_delta")]
impl SinkConfig for AzureDeltaSinkConfig {
    /// Builds the complete sink pipeline including table connection and event processing.
    /// Creates DeltaTable instance, health check function, and event processor.
    async fn build(&self, _cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        // Validate partition column types before building
        self.validate_partition_column_types()?;

        // Create DeltaTable instance with connection and schema
        let table = Arc::new(
            DeltaTable::try_new(
                self.table_path.clone(),
                self.connection_string.inner().to_string(),
                self.storage_account.clone(),
                self.container.clone(),
                self.partition_columns.clone(),
            )
            .await?,
        );

        // Build the event processing sink
        let sink = self.build_processor(table)?;
        Ok((sink, Box::pin(async { Ok(()) })))
    }

    /// Returns the input data type this sink accepts.
    /// Currently supports Log events only.
    fn input(&self) -> Input {
        Input::new(DataType::Log)
    }

    /// Returns acknowledgment configuration for delivery guarantees.
    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

impl AzureDeltaSinkConfig {
    /// Validates partition column types using the service layer validation
    fn validate_partition_column_types(&self) -> crate::Result<()> {
        crate::sinks::delta_table::service::DeltaTable::validate_partition_column_types(
            &self.partition_columns,
        )
        .map_err(|e| e.into())
    }
    /// Builds the event processing pipeline with batching and service layers.
    /// Creates the complete sink that converts Vector events to Delta table writes.
    fn build_processor(&self, table: Arc<DeltaTable>) -> crate::Result<VectorSink> {
        // Configure request processing with rate limiting and retry logic
        let request_limits = self.request.into_settings();
        let service = ServiceBuilder::new()
            .settings(request_limits, AzureDeltaRetryLogic)
            .service(AzureDeltaService::new(Arc::clone(&table)));

        // Configure event batching for efficient processing
        let batcher_settings = self.batch.into_batcher_settings()?;

        // Create request builder with table configuration
        let request_options = AzureDeltaRequestOptions { table };

        // Assemble the complete sink with all components
        let sink = AzureDeltaSink::new(service, request_options, batcher_settings);

        Ok(VectorSink::from_event_streamsink(sink))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_partition_column_types_valid() {
        let config = AzureDeltaSinkConfig {
            connection_string: SensitiveString::from("test".to_string()),
            storage_account: "test".to_string(),
            table_path: "test".to_string(),
            container: "test".to_string(),
            partition_columns: vec![
                PartitionColumn {
                    column_name: "date".to_string(),
                    column_type: "string".to_string(),
                },
                PartitionColumn {
                    column_name: "int_field".to_string(),
                    column_type: "int".to_string(),
                },
            ],
            batch: BatchConfig::default(),
            request: TowerRequestConfig::default(),
            acknowledgements: Default::default(),
        };

        // Should pass with valid types
        assert!(config.validate_partition_column_types().is_ok());
    }

    #[test]
    fn test_validate_partition_column_types_invalid() {
        let config = AzureDeltaSinkConfig {
            connection_string: SensitiveString::from("test".to_string()),
            storage_account: "test".to_string(),
            table_path: "test".to_string(),
            container: "test".to_string(),
            partition_columns: vec![
                PartitionColumn {
                    column_name: "date".to_string(),
                    column_type: "string".to_string(),
                },
                PartitionColumn {
                    column_name: "int_field".to_string(),
                    column_type: "invalid_type".to_string(),
                },
            ],
            batch: BatchConfig::default(),
            request: TowerRequestConfig::default(),
            acknowledgements: Default::default(),
        };

        // Should fail with invalid type
        let result = config.validate_partition_column_types();
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Invalid partition column type"));
        assert!(error_msg.contains("invalid_type"));
        assert!(error_msg.contains("int_field"));
    }

    #[test]
    fn test_validate_partition_column_types_empty() {
        let config = AzureDeltaSinkConfig {
            connection_string: SensitiveString::from("test".to_string()),
            storage_account: "test".to_string(),
            table_path: "test".to_string(),
            container: "test".to_string(),
            partition_columns: Vec::new(), // Empty Vec - default case
            batch: BatchConfig::default(),
            request: TowerRequestConfig::default(),
            acknowledgements: Default::default(),
        };

        // Should pass with empty partition columns
        assert!(config.validate_partition_column_types().is_ok());
    }
}
