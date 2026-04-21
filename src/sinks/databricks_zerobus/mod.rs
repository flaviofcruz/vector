//! The Zerobus sink.
//!
//! This sink streams observability data to Databricks Unity Catalog tables
//! via the Zerobus/Shinkansen ingestion service.

mod config;
mod error;
#[cfg(feature = "codecs-arrow")]
mod proto_to_arrow;
mod service;
mod sink;
mod unity_catalog_schema;
#[cfg(feature = "codecs-arrow")]
pub mod wire_to_arrow;

pub use config::ZerobusSinkConfig;
