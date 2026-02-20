use chrono::{DateTime, Utc};
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

/// Represents the status of a log file being processed.
/// It is represent by the type `Enum8` in the Clickhouse schema.
#[derive(Debug, Clone, Copy, PartialEq, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum LogStatus {
    InProgress = 1,
    Completed = 2,
    Error = 3,
}

/// [Internal] Used to check the status of log files in the
/// "should_ingest" process.
#[derive(Debug, Clone, PartialEq, Deserialize, Row)]
pub struct CheckStatusLogMetadata {
    pub file_size: u64,
    pub status: LogStatus,
}

/// [Internal] Used to insert new log metadata rows
#[derive(Debug, Clone, Serialize, Row)]
pub struct InsertLogMetadata {
    pub log_path: String,
    pub file_size: u64,
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    pub ingestion_start_time: DateTime<Utc>,
    pub status: LogStatus,
}
