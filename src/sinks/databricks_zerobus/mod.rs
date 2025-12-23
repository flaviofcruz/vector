//! The Zerobus sink.
//!
//! This sink streams observability data to Databricks Unity Catalog tables
//! via the Zerobus/Shinkansen ingestion service.

mod config;
mod error;
mod request_builder;
mod service;
mod sink;

pub use config::ZerobusSinkConfig;
