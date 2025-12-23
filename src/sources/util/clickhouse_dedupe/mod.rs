mod client;
mod config;
mod interface;
mod types;

pub use client::DeduplicationClient;
pub use config::ClickHouseDeduplicator;
pub use interface::DeduplicatorError;
pub use types::LogStatus;
