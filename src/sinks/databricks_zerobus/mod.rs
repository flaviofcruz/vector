//! The Zerobus sink.
//!
//! This sink streams observability data to Databricks Unity Catalog tables
//! via the Zerobus/Shinkansen ingestion service.

mod config;
mod error;
mod freshness;
mod headers;
mod ingest_tls;
mod service;
mod sink;
mod uc_client;
mod unity_catalog_schema;

pub use config::ZerobusSinkConfig;
