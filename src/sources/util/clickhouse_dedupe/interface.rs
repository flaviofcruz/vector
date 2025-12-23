use chrono::Utc;
use clickhouse::Client;
use snafu::{ResultExt, Snafu};
use tracing::{debug, info};

use super::{
    config::ClickHouseDeduplicator,
    types::{CheckStatusLogMetadata, InsertLogMetadata, LogStatus},
};

/// Errors that can occur during deduplication operations.
#[derive(Debug, Snafu)]
pub enum DeduplicatorError {
    #[snafu(display("Failed to create ClickHouse client: endpoint={}", endpoint))]
    ClientCreationFailed { endpoint: String },

    #[snafu(display(
        "Failed to execute ClickHouse query: query={}, error={}",
        query,
        source
    ))]
    QueryExecutionFailed {
        query: String,
        source: clickhouse::error::Error,
    },

    #[snafu(display(
        "Failed to fetch data from ClickHouse cursor: table={}, error={}",
        table,
        source
    ))]
    CursorFetchFailed {
        table: String,
        source: clickhouse::error::Error,
    },

    #[snafu(display(
        "Failed to insert data into ClickHouse: table={}, error={}",
        table,
        source
    ))]
    InsertFailed {
        table: String,
        source: clickhouse::error::Error,
    },

    #[snafu(display(
        "Failed to update record status in ClickHouse: table={}, log_path={}, error={}",
        table,
        log_path,
        source
    ))]
    StatusUpdateFailed {
        table: String,
        log_path: String,
        source: clickhouse::error::Error,
    },

    #[snafu(display(
        "Failed to execute replica sync query: query={}, error={}",
        query,
        source
    ))]
    ReplicaSyncQueryFailed {
        query: String,
        source: clickhouse::error::Error,
    },
}

impl ClickHouseDeduplicator {
    /// Helper to get the full table reference for queries
    pub fn table_ref(&self) -> String {
        format!("{}.{}", self.database, self.table)
    }

    /// Creates a ClickHouse client for read operations
    fn create_read_client(&self) -> Result<Client, DeduplicatorError> {
        let endpoint = self.endpoints.read_endpoint();
        self.create_client_for_endpoint(endpoint)
    }

    /// Creates a ClickHouse client for write operations
    fn create_write_client(&self) -> Result<Client, DeduplicatorError> {
        let endpoint = self.endpoints.write_endpoint();
        self.create_client_for_endpoint(endpoint)
    }

    /// Creates both read and write ClickHouse clients
    /// Returns a tuple of (read_client, write_client) or an error if either fails
    pub fn create_clients(&self) -> Result<(Client, Client), DeduplicatorError> {
        let read_client = self.create_read_client()?;
        let write_client = self.create_write_client()?;

        info!(
            message = "Successfully created ClickHouse clients for deduplication",
            table = %self.table_ref()
        );

        Ok((read_client, write_client))
    }

    /// Helper method to create a ClickHouse client for a specific endpoint
    fn create_client_for_endpoint(
        &self,
        endpoint: &crate::sinks::util::UriSerde,
    ) -> Result<Client, DeduplicatorError> {
        let url = endpoint.with_default_parts().uri.to_string();

        debug!(message = "Creating ClickHouse client", endpoint = &url);

        let mut client = Client::default().with_url(&url);

        if let Some(ref auth) = self.auth {
            match auth {
                crate::http::Auth::Basic { user, password } => {
                    client = client
                        .with_user(user)
                        .with_password(password.inner().to_string());
                }
                crate::http::Auth::Bearer { token } => {
                    client = client.with_access_token(token.inner().to_string());
                }
                _ => {
                    // ClickHouse client doesn't support other auth types (e.g., AWS)
                    // If unsupported auth is configured, proceed without authentication
                }
            }
        }

        client = client.with_database(&self.database);

        info!(
            message = "Successfully created ClickHouse client",
            endpoint = &url,
            database = &self.database
        );

        Ok(client)
    }

    /// Checks if a file needs to be ingested. It queries the underlying Clickhouse table,
    /// and based on the status and file size, decides whether to ingest or drop the file.
    /// It is also responsible for updating the status, as well emitting the relevant metrics
    /// for observability.
    ///
    /// Returns: Ok(true) if should ingest, Ok(false) if should drop, Err if error.
    pub async fn should_ingest(
        &self,
        log_path: &str,
        file_size: u64,
        client: &Client,
    ) -> Result<bool, DeduplicatorError> {
        let table_ref = self.table_ref();

        debug!(
            message = "Checking if file should be ingested",
            log_path = &log_path,
            file_size = &file_size,
            table = &table_ref
        );

        if let Some(existing_record) = self.get_existing_record(log_path, client).await? {
            self.handle_existing_record(log_path, existing_record, client)
                .await
        } else {
            self.create_new_record(log_path, file_size, client).await
        }
    }

    /// Executes a SYSTEM SYNC REPLICA LIGHTWEIGHT query to ensure sequential consistency while
    /// reading. This query forces the replica to synchronize its state by fetching metadata
    /// from ClickHouse ZooKeeper nodes.
    /// This query is only executed if the `replicated` configuration option is set to true.
    /// Since this is a non-critical operation (that only mitigates stale reads), any errors that may
    /// occur during the execution of this query are logged and ignored.
    ///
    /// Clickhouse Documentation Reference:
    /// https://clickhouse.com/docs/sql-reference/statements/system#sync-replica
    /// https://clickhouse.com/docs/cloud/reference/shared-merge-tree#consistency
    async fn execute_replica_sync_query(&self, client: &Client) {
        if !self.replicated {
            return;
        }

        let table_ref = self.table_ref();
        let query = format!("SYSTEM SYNC REPLICA {} LIGHTWEIGHT", table_ref);

        debug!(message = "Executing replica sync query", query = &query);

        if let Err(e) = client
            .query(&query)
            .execute()
            .await
            .context(ReplicaSyncQueryFailedSnafu { query })
        {
            error!(
                message = "Failed to execute replica sync query, proceeding without synchronization",
                table = &table_ref,
                error = ?e,
            );
        }
    }

    /// Retrieves existing record for the given log path from ClickHouse
    /// Returns None if no record exists, Some(record) if found
    async fn get_existing_record(
        &self,
        log_path: &str,
        client: &Client,
    ) -> Result<Option<CheckStatusLogMetadata>, DeduplicatorError> {
        self.execute_replica_sync_query(client).await;

        let table_ref = self.table_ref();
        let query = format!(
            "SELECT file_size, status FROM {} WHERE log_path = ?",
            table_ref
        );

        debug!(
            message = "Executing query to check existing record",
            log_path = &log_path,
            table = &table_ref,
            query = &query
        );

        let mut cursor = client
            .query(&query)
            .bind(log_path)
            .fetch::<CheckStatusLogMetadata>()
            .context(QueryExecutionFailedSnafu { query })?;

        cursor
            .next()
            .await
            .context(CursorFetchFailedSnafu { table: table_ref })
    }

    /// Handles the logic for existing records based on their current status
    /// Returns true if should ingest, false if should skip
    async fn handle_existing_record(
        &self,
        log_path: &str,
        record: CheckStatusLogMetadata,
        client: &Client,
    ) -> Result<bool, DeduplicatorError> {
        let table_ref = self.table_ref();

        debug!(
            message = "Found existing record",
            log_path = &log_path,
            db_file_size = &record.file_size,
            status = ?record.status
        );

        match record.status {
            LogStatus::Error => {
                info!(
                    message = "File previously failed, retrying ingestion",
                    log_path = &log_path
                );
                // emit metric: failed, will retry
                // TODO: emit_metric("dedup_failed_retry")

                // Update status to IN_PROGRESS (1)
                // Reference: https://clickhouse.com/docs/en/sql-reference/statements/update
                let update_query = format!(
                    "ALTER TABLE {} UPDATE status = 1 WHERE log_path = ?",
                    table_ref
                );
                debug!(
                    message = "Updating status to IN_PROGRESS",
                    log_path = &log_path,
                    query = &update_query
                );

                client
                    .query(&update_query)
                    .bind(log_path)
                    // Wait for all active replicas to acknowledge the mutation
                    // https://clickhouse.com/docs/operations/settings/settings#mutations_sync
                    .with_option("mutations_sync", "2")
                    .execute()
                    .await
                    .context(StatusUpdateFailedSnafu {
                        table: table_ref,
                        log_path: log_path.to_string(),
                    })?;

                // Trying to ingest a failed record, so return true
                Ok(true)
            }
            LogStatus::Completed => {
                info!(
                    message = "File already completed, skipping",
                    log_path = &log_path
                );
                // emit metric: completed, drop
                // TODO: emit_metric("dedup_completed_drop")
                Ok(false)
            }
            LogStatus::InProgress => {
                info!(
                    message = "File currently in progress, skipping",
                    log_path = &log_path
                );
                // emit metric: in progress, maybe skip
                // TODO: emit_metric("dedup_in_progress")
                Ok(false)
            }
        }
    }

    /// Creates a new record for a file that doesn't exist in the database
    /// Returns true as the file should be ingested
    async fn create_new_record(
        &self,
        log_path: &str,
        file_size: u64,
        client: &Client,
    ) -> Result<bool, DeduplicatorError> {
        let table_ref = self.table_ref();

        debug!(
            message = "No existing log metadata record found, creating new entry",
            log_path = &log_path
        );

        let insert_row = InsertLogMetadata {
            log_path: log_path.to_string(),
            file_size,
            ingestion_start_time: chrono::Utc::now(),
            status: LogStatus::InProgress,
        };

        debug!(
            message = "Inserting new record for log metadata",
            record = ?insert_row
        );

        // Create insert and handle all operations with shared error context
        async {
            let mut insert = client.insert(&table_ref)?;
            insert.write(&insert_row).await?;
            insert.end().await?;
            Ok::<(), clickhouse::error::Error>(())
        }
        .await
        .context(InsertFailedSnafu { table: table_ref })?;

        info!(
            message = "Successfully inserted new record for log metadata",
            record = ?insert_row,
        );

        // TODO: emit_metric("dedup_new_row")

        Ok(true)
    }

    /// Marks a file as completed or failed in the deduplication table. It also
    /// updates the metadata table with additional information such as the
    /// ingestion completion time, the file size, and the file creation timestamp.
    pub async fn mark_file_completion(
        &self,
        log_path: &str,
        file_creation_timestamp: &str,
        file_size: u64,
        success: bool,
        client: &Client,
    ) -> Result<(), DeduplicatorError> {
        let table_ref = self.table_ref();
        let status = if success {
            LogStatus::Completed
        } else {
            LogStatus::Error
        };

        debug!(
            message = "Marking file completion",
            log_path = &log_path,
            file_size = &file_size,
            success = &success,
            status = ?status
        );

        // TODO: Consider using a lightweight UPDATE when it becomes generally available
        // Reference: https://clickhouse.com/docs/en/sql-reference/statements/update
        let update_query = format!(
            "
            ALTER TABLE {} 
            UPDATE 
                status = ?, 
                ingestion_completion_time = parseDateTime32BestEffortOrNull(?), 
                source_creation_time = parseDateTime32BestEffortOrNull(?) 
            WHERE log_path = ?
        ",
            table_ref
        );

        debug!(
            message = "Executing completion status update",
            log_path = &log_path,
            query = &update_query,
            status = ?status,
            file_creation_timestamp = &file_creation_timestamp
        );

        let completion_time = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();

        client
            .query(&update_query)
            .bind(status as u8)
            .bind(completion_time)
            .bind(file_creation_timestamp)
            .bind(log_path)
            // Wait for all active replicas to acknowledge the mutation
            // https://clickhouse.com/docs/operations/settings/settings#mutations_sync
            .with_option("mutations_sync", "2")
            .execute()
            .await
            .context(StatusUpdateFailedSnafu {
                table: table_ref,
                log_path: log_path.to_string(),
            })?;

        debug!(
            message = "Successfully updated file completion status",
            log_path = &log_path,
            status = ?status
        );

        // emit metric: completion marked
        // TODO: emit_metric("dedup_completion_marked")

        Ok(())
    }
}
