use clickhouse::Client;
use tracing::{error, info};

use super::config::ClickHouseDeduplicator;

/// A high-level client for ClickHouse deduplication that handles both read and write operations.
/// This struct encapsulates the configuration, clients, and provides simple methods for deduplication.
///
/// All methods are designed to be resilient - they will log errors internally and return safe
/// boolean values rather than propagating errors to the caller. This ensures that deduplication
/// failures don't break the main data processing pipeline.
pub struct DeduplicationClient {
    config: ClickHouseDeduplicator,
    read_client: Client,
    write_client: Client,
}

impl DeduplicationClient {
    /// Creates a new DeduplicationClient from the given configuration.
    /// Returns None if client creation fails (with error logging).
    pub fn new(config: ClickHouseDeduplicator) -> Option<Self> {
        match config.create_clients() {
            Ok((read_client, write_client)) => {
                info!(
                    message = "Successfully created DeduplicationClient",
                    table = %config.table_ref()
                );
                Some(Self {
                    config,
                    read_client,
                    write_client,
                })
            }
            Err(e) => {
                error!(
                    message = "Failed to create ClickHouse clients for deduplication. Please check the configuration. No deduplication would happen for all the requests.",
                    error = ?e,
                    table = %config.table_ref()
                );
                None
            }
        }
    }

    /// Checks if a file should be ingested based on deduplication logic.
    ///
    /// Returns:
    /// - `true` if the file should be processed (either new file or failed file to retry)
    /// - `false` if the file should be skipped (already completed or in progress)
    ///
    /// If any errors occur during the deduplication check, this method will log the
    /// error and return `true` to ensure data processing continues. This fail-safe
    /// approach prevents deduplication issues from blocking data ingestion.
    pub async fn should_ingest(&self, log_path: &str, file_size: u64) -> bool {
        match self
            .config
            .should_ingest(log_path, file_size, &self.read_client)
            .await
        {
            Ok(result) => result,
            Err(e) => {
                error!(
                    message = "Failed to check deduplication status, proceeding with ingestion to avoid data loss",
                    log_path = %log_path,
                    file_size = %file_size,
                    table = %self.config.table_ref(),
                    error = ?e,
                );
                // Return true to ensure data is processed if the deduplication check fails
                true
            }
        }
    }

    /// Marks a file's processing completion status in the deduplication table. The
    /// file creation timestamp is expected to be in the ISO 8601 format.
    ///
    /// If any errors occur while marking completion, this method will log the error.
    /// The failure to mark completion won't affect the data processing pipeline,
    /// but it may result in the same file being reprocessed in future runs.
    pub async fn mark_completion(
        &self,
        log_path: &str,
        file_creation_timestamp: &str,
        file_size: u64,
        success: bool,
    ) -> () {
        match self
            .config
            .mark_file_completion(
                log_path,
                file_creation_timestamp,
                file_size,
                success,
                &self.write_client,
            )
            .await
        {
            Ok(()) => {
                info!(
                    message = "Successfully marked file completion status",
                    log_path = %log_path,
                    success = %success,
                    table = %self.config.table_ref(),
                );
            }
            Err(e) => {
                error!(
                    message = "Failed to mark file completion status in ClickHouse",
                    log_path = %log_path,
                    success = %success,
                    table = %self.config.table_ref(),
                    error = ?e,
                );
            }
        }
    }
}
