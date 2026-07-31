//! Enrichment table backed by a generic HTTP service.
//!
//! Periodically fetches a dataset from an HTTP endpoint (with optional pagination), decodes
//! it into rows, indexes it, and serves lookups from an in-memory snapshot — refreshing in
//! the background so lookups never block on the network. Best suited to small, relatively
//! static reference datasets served over a REST-style API.

pub mod config;
pub mod table;

pub use config::HttpConfig;
pub use table::HttpTable;
