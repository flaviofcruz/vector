#![allow(
    missing_docs,
    clippy::missing_errors_doc,
    clippy::doc_markdown,
    clippy::missing_panics_doc
)]

mod builder;
mod errors;
mod output;
mod sender;
#[cfg(test)]
mod tests;

pub use builder::Builder;
pub use errors::SendError;
use output::Output;
pub use sender::{SourceSender, SourceSenderItem};

pub static CHUNK_SIZE: std::sync::LazyLock<usize> = std::sync::LazyLock::new(|| {
    std::env::var("VECTOR_CHUNK_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(1000)
});

#[cfg(any(test, feature = "test"))]
const TEST_BUFFER_SIZE: usize = 100;

const LAG_TIME_NAME: &str = "source_lag_time_seconds";
