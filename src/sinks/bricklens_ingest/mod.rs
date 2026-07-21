mod config;
mod request_builder;
mod service;
mod sink;

pub use config::BricklensIngestConfig;
// Reused by the `bricklens_config_enricher` transform so gRPC status parsing (header + trailer,
// header-wins) has a single source of truth shared with this sink.
pub(crate) use service::resolve_grpc_status;
