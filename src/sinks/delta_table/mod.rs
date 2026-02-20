// Delta Table Sink for Azure Storage
//
// This module implements a Vector sink that writes events to Delta Lake tables
// stored in Azure Blob Storage. The sink supports both inline schema definitions
// and protobuf-based schema extraction from descriptor files.
//
// Architecture:
// - config.rs: Configuration parsing and sink setup
// - request_builder.rs: Converts events to Delta table write requests
// - service.rs: Delta table operations and Azure storage integration
// - sink.rs: Main event processing pipeline and batching

mod config;
mod request_builder;
mod service;
mod sink;

pub use self::config::AzureDeltaSinkConfig;
