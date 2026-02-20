use vector_lib::configurable::configurable_component;

use crate::{http::Auth, sinks::util::UriSerde, tls::TlsConfig};

/// Configuration for ClickHouse endpoints.
/// You can either specify a single endpoint for both read and write operations,
/// or specify separate endpoints for read and write operations.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClickHouseEndpoints {
    /// Single endpoint configuration for both read and write operations.
    Single {
        /// The ClickHouse server endpoint used for both read and write operations.
        #[serde(alias = "host")]
        #[configurable(metadata(docs::examples = "http://localhost:8123"))]
        endpoint: UriSerde,
    },
    /// Separate endpoints configuration for read and write operations.
    Separate {
        /// Dedicated endpoint for read operations.
        #[configurable(metadata(docs::examples = "http://readonly-replica:8123"))]
        read_endpoint: UriSerde,

        /// Dedicated endpoint for write operations.
        #[configurable(metadata(docs::examples = "http://write-node:8123"))]
        write_endpoint: UriSerde,
    },
}

/// Default value for the `replicated` field.
fn default_replicated() -> bool {
    true
}

/// Configuration for ClickHouse-based deduplication functionality.
///
/// This configuration allows sources to deduplicate files based on metadata stored in ClickHouse.
/// When enabled, the source will query the specified ClickHouse table to check if a file has
/// already been processed before attempting to ingest it.
///
/// To use this deduplication functionality, you need to provide connection details to a
/// ClickHouse database which has a table with the following required columns:
///
/// ```sql
/// CREATE TABLE logs_metadata (
///     log_path String,                            -- Primary key to identify the file
///     file_size Int64,                            -- Size of the log file in bytes
///     source_creation_time DateTime,              -- When the original file was created in cloud storage
///     ingestion_start_time DateTime,              -- When Vector started processing the file
///     ingestion_completion_time DateTime,         -- When Vector finished processing the file
///     status Enum8(                               -- Current processing status
///         'IN_PROGRESS' = 1,
///         'COMPLETED' = 2,
///         'ERROR' = 3
///     )                                       
/// )
/// ```
///
/// The deduplication process will:
/// - Query existing records by `log_path` to check processing status
/// - Insert new records with `IN_PROGRESS` status when starting file processing
/// - Update records with `COMPLETED` or `FAILED` status when finishing
/// - Use `status` to determine whether to retry failed files
///
/// Additional columns in the table are allowed but will not be populated by the deduplication process.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseDeduplicator {
    /// ClickHouse endpoint configuration.
    #[configurable(derived)]
    pub endpoints: ClickHouseEndpoints,

    /// The table name used for deduplication.
    #[configurable(derived)]
    #[configurable(metadata(docs::examples = "dedup_table"))]
    pub table: String,

    /// The database that contains the deduplication table.
    #[configurable(derived)]
    #[configurable(metadata(docs::examples = "mydatabase"))]
    pub database: String,

    /// Is the table replicated across multiple nodes?
    /// If true, before inserting a new record for deduplication, a `SYSTEM SYNC REPLICA` command
    /// will be executed to ensure the replica is up-to-date.
    /// The default value of the `replicated` field is `true`.
    #[configurable(derived)]
    #[configurable(metadata(docs::examples = "true"))]
    #[serde(default = "default_replicated")]
    pub replicated: bool,

    /// Authentication configuration for ClickHouse.
    #[configurable(derived)]
    pub auth: Option<Auth>,

    /// TLS configuration for ClickHouse connection.
    #[configurable(derived)]
    pub tls: Option<TlsConfig>,
}

impl ClickHouseEndpoints {
    /// Gets the endpoint to use for read operations.
    /// Returns the dedicated read endpoint if specified, otherwise returns the single endpoint.
    pub fn read_endpoint(&self) -> &UriSerde {
        match &self {
            ClickHouseEndpoints::Single { endpoint } => endpoint,
            ClickHouseEndpoints::Separate { read_endpoint, .. } => read_endpoint,
        }
    }

    /// Gets the endpoint to use for write operations.
    /// Returns the dedicated write endpoint if specified, otherwise returns the single endpoint.
    pub fn write_endpoint(&self) -> &UriSerde {
        match &self {
            ClickHouseEndpoints::Single { endpoint } => endpoint,
            ClickHouseEndpoints::Separate { write_endpoint, .. } => write_endpoint,
        }
    }
}
