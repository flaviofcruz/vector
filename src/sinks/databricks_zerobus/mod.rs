//! The Zerobus sink.
//!
//! This sink streams observability data to Databricks Unity Catalog tables
//! via the Zerobus/Shinkansen ingestion service.

mod config;
mod error;
mod headers;
mod service;
mod sink;
mod uc_client;
mod unity_catalog_schema;

pub use config::ZerobusSinkConfig;
