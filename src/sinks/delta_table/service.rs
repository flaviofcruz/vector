// Delta Table Service for Azure Storage Integration
//
// This module handles all Delta table operations including table creation, schema management,
// event conversion to Arrow format, and writing to Delta tables. It provides the core
// integration between Vector events and Azure Delta Lake storage.
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use deltalake::DeltaTable as DeltaTableCore;
use deltalake::kernel::schema::{
    DataType as SchemaDataType, PrimitiveType, StructField as DeltaSchemaField,
};
use deltalake::operations::create::CreateBuilder;
use deltalake::writer::DeltaWriter;
use futures::future::BoxFuture;
use tower::Service;
use tracing::{debug, info};
use vector_lib::event::Event;
use vector_lib::finalization::{EventFinalizers, Finalizable};
use vector_lib::request_metadata::GroupedCountByteSize;
use vector_lib::request_metadata::{MetaDescriptive, RequestMetadata};
use vector_lib::stream::DriverResponse;

use crate::sinks::util::retries::{RetryAction, RetryLogic};

use super::config::PartitionColumn;

use deltalake::writer::WriteMode;

/// Error types for Azure Delta table operations
/// Covers Delta table errors and other operational failures
#[derive(Debug)]
pub enum AzureDeltaError {
    /// Errors from Delta table operations (schema, write, etc.)
    DeltaTableError { message: String },
    /// Other errors wrapped in a generic error type
    #[allow(dead_code)]
    Other {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Implements Display trait for error formatting
impl std::fmt::Display for AzureDeltaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AzureDeltaError::DeltaTableError { message } => {
                write!(f, "Delta table error: {}", message)
            }
            AzureDeltaError::Other { source } => write!(f, "Other error: {}", source),
        }
    }
}

/// Implements Error trait for error handling
impl std::error::Error for AzureDeltaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            AzureDeltaError::DeltaTableError { .. } => None,
            AzureDeltaError::Other { source } => Some(source.as_ref()),
        }
    }
}

/// Conversion implementations for common error types to AzureDeltaError
impl From<String> for AzureDeltaError {
    fn from(message: String) -> Self {
        AzureDeltaError::DeltaTableError { message }
    }
}

impl From<deltalake::DeltaTableError> for AzureDeltaError {
    fn from(error: deltalake::DeltaTableError) -> Self {
        AzureDeltaError::DeltaTableError {
            message: error.to_string(),
        }
    }
}

/// Request structure for Delta table write operations
/// Contains events to write, finalizers, and table reference
#[derive(Clone)]
pub struct AzureDeltaRequest {
    /// Vector events to be written to the Delta table
    pub events: Vec<Event>,
    /// Event finalizers for delivery acknowledgment
    pub finalizers: EventFinalizers,
    /// Request metadata for tracking and monitoring
    pub request_metadata: RequestMetadata,
    /// Reference to the target Delta table
    pub table: Arc<DeltaTable>,
}

/// Response from Delta table write operations
/// Contains information about successful writes and event counts
#[derive(Debug, Default, Clone)]
pub struct AzureDeltaResponse {
    /// Byte size information for the written events
    pub events_byte_size: GroupedCountByteSize,
}

/// Implements Vector's DriverResponse trait for Delta table responses
/// Provides event status and byte size information for monitoring
impl DriverResponse for AzureDeltaResponse {
    fn event_status(&self) -> vector_lib::event::EventStatus {
        vector_lib::event::EventStatus::Delivered
    }

    fn events_sent(&self) -> &GroupedCountByteSize {
        &self.events_byte_size
    }
}

/// Implements Vector's Finalizable trait for request finalization
/// Handles event finalizers for delivery acknowledgment
impl Finalizable for AzureDeltaRequest {
    fn take_finalizers(&mut self) -> EventFinalizers {
        std::mem::take(&mut self.finalizers)
    }
}

/// Implements Vector's MetaDescriptive trait for request metadata
/// Provides access to request metadata for monitoring and tracking
impl MetaDescriptive for AzureDeltaRequest {
    fn get_metadata(&self) -> &RequestMetadata {
        &self.request_metadata
    }

    fn metadata_mut(&mut self) -> &mut RequestMetadata {
        &mut self.request_metadata
    }
}

/// Wrapper for Delta table operations with schema management
/// Provides thread-safe access to Delta table operations and schema information
pub struct DeltaTable {
    /// Thread-safe reference to the underlying Delta table
    table: Arc<tokio::sync::RwLock<DeltaTableCore>>,
    /// Path to the Delta table in Azure Storage
    table_path: String,
    /// Partition columns for the table
    #[allow(dead_code)]
    partition_columns: Vec<PartitionColumn>,
}

impl DeltaTable {
    /// Creates a new Delta table connection or opens an existing one
    /// Handles schema loading, Azure connection setup, and table creation if needed
    pub async fn try_new(
        table_path: String,
        connection_string: String,
        storage_account: String,
        container: String,
        partition_columns: Vec<PartitionColumn>,
    ) -> Result<Self, AzureDeltaError> {
        // Register Azure storage handlers for Delta Lake
        deltalake::azure::register_handlers(None);

        // Build Azure storage options from connection string
        let mut storage_options = HashMap::new();

        // Extract SAS token from connection string for authentication
        if let Some(sas_start) = connection_string.find("SharedAccessSignature=") {
            let sas_token = &connection_string[sas_start + "SharedAccessSignature=".len()..];

            // Set all possible Azure storage options for compatibility
            storage_options.insert("azure_storage_sas_token".to_string(), sas_token.to_string());
            storage_options.insert(
                "azure_storage_account_name".to_string(),
                storage_account.clone(),
            );
            storage_options.insert("container_name".to_string(), container.clone());
        }

        // Try to open existing table or create new one
        let (table, final_partition_columns) =
            match deltalake::open_table_with_storage_options(&table_path, storage_options.clone())
                .await
            {
                Ok(table) => {
                    let metadata = table.metadata()?.clone();
                    let existing_partitions = metadata.partition_columns().clone();

                    info!(
                        message = "Existing partition columns loaded from Delta table",
                        existing_partitions = ?existing_partitions,
                    );
                    let existing_schema = table.get_schema()?;

                    // Use table's partition columns instead of config
                    let mut actual_partition_columns = Vec::new();
                    for column_name in &existing_partitions {
                        let existing_field = existing_schema
                            .fields()
                            .find(|field| field.name() == column_name)
                            .ok_or_else(|| AzureDeltaError::DeltaTableError {
                                message: format!(
                                    "Partition column '{}' not found in existing table schema",
                                    column_name
                                ),
                            })?;

                        let existing_type_str =
                            Self::partition_column_type_to_string(existing_field.data_type())
                                .map_err(|e| AzureDeltaError::DeltaTableError {
                                    message: format!(
                                        "Failed to convert existing table type for column '{}': {}",
                                        column_name, e
                                    ),
                                })?;

                        actual_partition_columns.push(PartitionColumn {
                            column_name: column_name.clone(),
                            column_type: existing_type_str,
                        });
                    }
                    (table, actual_partition_columns)
                }
                Err(_e) => {
                    info!(
                        message = "New Delta table will be created",
                        table_path = %table_path,
                        partition_columns = ?partition_columns
                    );
                    // Build schema for new table creation with partition columns and their types
                    let fields: Vec<DeltaSchemaField> = partition_columns
                        .iter()
                        .map(
                            |partition_column| -> Result<DeltaSchemaField, AzureDeltaError> {
                                let data_type = Self::parse_partition_column_type_to_delta(
                                &partition_column.column_type,
                            )
                            .map_err(|e| AzureDeltaError::DeltaTableError {
                                message: format!(
                                    "Invalid partition column type '{}' for column '{}': {}",
                                    partition_column.column_type,
                                    partition_column.column_name,
                                    e,
                                ),
                            })?;
                                Ok(DeltaSchemaField::new(
                                    partition_column.column_name.clone(),
                                    data_type,
                                    true,
                                ))
                            },
                        )
                        .collect::<Result<Vec<_>, _>>()?;

                    // Create new Delta table with the specified schema
                    let table = CreateBuilder::new()
                        .with_location(&table_path)
                        .with_storage_options(storage_options.clone())
                        .with_columns(fields)
                        .with_partition_columns(
                            partition_columns
                                .iter()
                                .map(|partition_column| partition_column.column_name.clone())
                                .collect::<Vec<String>>(),
                        )
                        .await
                        .map_err(|e| format!("Failed to create table: {}", e))?;

                    (table, partition_columns)
                }
            };

        let result = Self {
            table: Arc::new(tokio::sync::RwLock::new(table)),
            table_path,
            partition_columns: final_partition_columns,
        };

        Ok(result)
    }

    pub async fn write_batch(
        &self,
        events: Vec<Event>,
    ) -> Result<AzureDeltaResponse, AzureDeltaError> {
        // Extract just the log data from events, not the entire Event struct
        let json_values: Vec<serde_json::Value> = events
            .into_iter()
            .filter_map(|event| {
                // Convert Event to LogEvent first
                if let Some(log) = event.try_into_log() {
                    // Then convert the LogEvent to JSON
                    serde_json::to_value(&log)
                        .map_err(|e| AzureDeltaError::DeltaTableError {
                            message: format!("Failed to convert log: {}", e),
                        })
                        .ok()
                } else {
                    None
                }
            })
            .collect();

        if json_values.is_empty() {
            return Err(AzureDeltaError::DeltaTableError {
                message: "No valid log events found in batch".to_string(),
            });
        }

        const MAX_RETRIES_FOR_NON_CONFLICT: u32 = 3;
        let mut non_conflict_retries = 0;

        loop {
            let mut table_guard = self.table.write().await;
            let table_ref = &mut *table_guard;

            // If this is a retry, update the table state first to get latest metadata (someone else might have written).
            if non_conflict_retries > 0 {
                table_ref
                    .update()
                    .await
                    .map_err(|e| AzureDeltaError::DeltaTableError {
                        message: format!("Failed to update table state: {}", e),
                    })?;
            }

            let mut writer =
                deltalake::writer::json::JsonWriter::for_table(&*table_ref).map_err(|e| {
                    AzureDeltaError::DeltaTableError {
                        message: format!("Failed to create JsonWriter: {}", e),
                    }
                })?;

            // Pass the write mode here instead of a builder method
            writer
                .write_with_mode(json_values.clone(), WriteMode::Default)
                .await
                .map_err(|e| AzureDeltaError::DeltaTableError {
                    message: format!("Failed to write batch: {}", e),
                })?;

            match writer.flush_and_commit(table_ref).await {
                Ok(..) => {
                    return Ok(AzureDeltaResponse {
                        events_byte_size: GroupedCountByteSize::default(),
                    });
                }
                Err(e) => {
                    let error_str = e.to_string();

                    if error_str.contains("conflict") {
                        // For write conflicts, retry infinitely (no counter needed)
                        drop(table_guard);

                        // Sleep before the next attempt with uniform random delay between 1-10 seconds
                        let delay_ms = rand::rng().random_range(1000..=10000); // 1000-10000ms (1-10 seconds)
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    } else if non_conflict_retries < MAX_RETRIES_FOR_NON_CONFLICT {
                        // For non-conflict errors, retry up to MAX_RETRIES_FOR_NON_CONFLICT times
                        non_conflict_retries += 1;

                        drop(table_guard);

                        // Sleep before the next attempt
                        tokio::time::sleep(std::time::Duration::from_millis(
                            100 * 2_u64.pow(non_conflict_retries),
                        ))
                        .await;
                    } else {
                        // Max retries exceeded for non-conflict errors
                        return Err(AzureDeltaError::DeltaTableError {
                            message: format!(
                                "Failed to commit transaction after {} non-conflict retries: {}",
                                non_conflict_retries, error_str
                            ),
                        });
                    }
                }
            }
        }
    }

    /// Validates partition column types for config validation
    pub fn validate_partition_column_types(
        partition_columns: &[PartitionColumn],
    ) -> Result<(), String> {
        for partition_column in partition_columns {
            let column_name = &partition_column.column_name;
            let column_type = &partition_column.column_type;
            Self::parse_partition_column_type_to_delta(column_type).map_err(|e| {
                format!(
                    "Invalid partition column type '{}' for column '{}': {}",
                    column_type, column_name, e
                )
            })?;
        }
        Ok(())
    }

    /// Parse partition column type string to Delta Lake schema type (primitive types only)
    fn parse_partition_column_type_to_delta(type_str: &str) -> Result<SchemaDataType, String> {
        let primitive_type = match type_str.to_lowercase().as_str() {
            "string" => PrimitiveType::String,
            "integer" | "int" => PrimitiveType::Integer,
            "long" | "bigint" => PrimitiveType::Long,
            "float" => PrimitiveType::Float,
            "double" => PrimitiveType::Double,
            "boolean" | "bool" => PrimitiveType::Boolean,
            "timestamp" => PrimitiveType::Timestamp,
            "date" => PrimitiveType::Date,
            other => return Err(format!("Unsupported field type: {}", other)),
        };

        Ok(SchemaDataType::Primitive(primitive_type))
    }

    /// Converts partition column Delta Lake schema types back to string representations for comparison
    /// Errors if encountering unsupported types (which should never happen for partition columns)
    fn partition_column_type_to_string(data_type: &SchemaDataType) -> Result<String, String> {
        match data_type {
            SchemaDataType::Primitive(primitive_type) => match primitive_type {
                PrimitiveType::String => Ok("string".to_string()),
                PrimitiveType::Integer => Ok("integer".to_string()),
                PrimitiveType::Long => Ok("long".to_string()),
                PrimitiveType::Float => Ok("float".to_string()),
                PrimitiveType::Double => Ok("double".to_string()),
                PrimitiveType::Boolean => Ok("boolean".to_string()),
                PrimitiveType::Timestamp => Ok("timestamp".to_string()),
                PrimitiveType::Date => Ok("date".to_string()),
                other => Err(format!(
                    "Unsupported primitive type for partition column: {:?}",
                    other
                )),
            },
            other => Err(format!(
                "Partition columns must be primitive types, found: {:?}",
                other
            )),
        }
    }
}

#[derive(Clone)]
/// Service layer for Delta table operations
/// Implements Tower Service trait for request/response handling with retry logic
pub struct AzureDeltaService {
    /// Reference to the Delta table for write operations
    table: Arc<DeltaTable>,
    /// Counter for in-flight requests to implement backpressure
    in_flight: Arc<AtomicUsize>,
}

impl AzureDeltaService {
    /// Creates a new Azure Delta service with the specified table
    pub fn new(table: Arc<DeltaTable>) -> Self {
        Self {
            table,
            in_flight: Arc::new(AtomicUsize::new(0)),
        }
    }
}

/// Implements Tower Service trait for Delta table request processing
/// Handles async request/response pattern with proper error handling
impl Service<AzureDeltaRequest> for AzureDeltaService {
    type Response = AzureDeltaResponse;
    type Error = AzureDeltaError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    /// Checks if the service is ready to process requests
    /// Implements backpressure by limiting concurrent operations
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Limit concurrent operations to 10
        const MAX_CONCURRENT: usize = 10;

        let current = self.in_flight.load(Ordering::Relaxed);
        if current >= MAX_CONCURRENT {
            // Too many in-flight requests, apply backpressure
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    /// Processes Delta table write requests asynchronously
    /// Converts request to table write operation and returns response
    fn call(&mut self, mut request: AzureDeltaRequest) -> Self::Future {
        let table = Arc::clone(&self.table);
        let in_flight = Arc::clone(&self.in_flight);

        Box::pin(async move {
            // Increment in-flight counter
            in_flight.fetch_add(1, Ordering::Relaxed);

            debug!(
                message = "Writing events to Delta table.",
                events = request.events.len(),
                table = %request.table.table_path,
            );

            let events_byte_size = std::mem::take(request.metadata_mut())
                .into_events_estimated_json_encoded_byte_size();

            let result = table.write_batch(request.events).await;

            // Decrement in-flight counter
            in_flight.fetch_sub(1, Ordering::Relaxed);

            match result {
                Ok(mut response) => {
                    response.events_byte_size = events_byte_size;
                    Ok(response)
                }
                Err(e) => Err(e),
            }
        })
    }
}

#[derive(Debug, Default, Clone)]
/// Retry logic for Delta table operations
/// Determines which errors are retriable and when to retry operations
pub struct AzureDeltaRetryLogic;

impl RetryLogic for AzureDeltaRetryLogic {
    type Error = AzureDeltaError;
    type Request = AzureDeltaRequest;
    type Response = AzureDeltaResponse;

    /// Determines if an error should trigger a retry
    /// Retries on network errors, temporary failures, and rate limiting
    fn is_retriable_error(&self, error: &Self::Error) -> bool {
        let err = error.to_string();
        err.contains("conflict") || err.contains("timeout") || err.contains("connection")
    }

    /// Determines if a response should trigger a retry
    /// Currently no retry on successful responses
    fn should_retry_response(&self, _response: &Self::Response) -> RetryAction<Self::Request> {
        RetryAction::Successful
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_partition_column_types_valid() {
        let partition_columns = vec![
            PartitionColumn {
                column_name: "date".to_string(),
                column_type: "string".to_string(),
            },
            PartitionColumn {
                column_name: "int_field".to_string(),
                column_type: "int".to_string(),
            },
        ];

        // Should pass with all valid types
        assert!(DeltaTable::validate_partition_column_types(&partition_columns).is_ok());
    }

    #[test]
    fn test_validate_partition_column_types_invalid() {
        let partition_columns = vec![
            PartitionColumn {
                column_name: "date".to_string(),
                column_type: "string".to_string(),
            },
            PartitionColumn {
                column_name: "int_field".to_string(),
                column_type: "invalid_type".to_string(),
            },
        ];

        // Should fail with invalid type
        let result = DeltaTable::validate_partition_column_types(&partition_columns);
        assert!(result.is_err());
        let error_msg = result.unwrap_err();
        assert!(error_msg.contains("Invalid partition column type"));
        assert!(error_msg.contains("invalid_type"));
        assert!(error_msg.contains("int_field"));
    }

    #[test]
    fn test_parse_partition_column_type_to_delta() {
        // Test valid types
        assert!(DeltaTable::parse_partition_column_type_to_delta("string").is_ok());
        assert!(DeltaTable::parse_partition_column_type_to_delta("integer").is_ok());
        assert!(DeltaTable::parse_partition_column_type_to_delta("int").is_ok()); // alias
        assert!(DeltaTable::parse_partition_column_type_to_delta("long").is_ok());
        assert!(DeltaTable::parse_partition_column_type_to_delta("bigint").is_ok()); // alias
        assert!(DeltaTable::parse_partition_column_type_to_delta("float").is_ok());
        assert!(DeltaTable::parse_partition_column_type_to_delta("double").is_ok());
        assert!(DeltaTable::parse_partition_column_type_to_delta("boolean").is_ok());
        assert!(DeltaTable::parse_partition_column_type_to_delta("bool").is_ok()); // alias
        assert!(DeltaTable::parse_partition_column_type_to_delta("timestamp").is_ok());
        assert!(DeltaTable::parse_partition_column_type_to_delta("date").is_ok());

        // Test case insensitive
        assert!(DeltaTable::parse_partition_column_type_to_delta("STRING").is_ok());
        assert!(DeltaTable::parse_partition_column_type_to_delta("Integer").is_ok());

        // Test invalid type
        let result = DeltaTable::parse_partition_column_type_to_delta("invalid_type");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("Unsupported field type: invalid_type")
        );
    }
}
