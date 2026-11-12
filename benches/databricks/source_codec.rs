//! The source side of a pipeline: how tapped events become corpus items, and how items are sent.
//!
//! A new source plugs in by implementing [`SourceCodec`] and listing it in [`CODECS`]. Everything
//! else in the harness works on items without knowing what they hold: capture's tap and histogram,
//! corpus I/O, and gen's pacing, concurrency, retries and sent log.

use async_trait::async_trait;
use std::net::SocketAddr;

use bytes::Bytes;

use super::err;
use super::generator::{Client, Target};
use super::prom_remote_write::PromRemoteWrite;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// One corpus item: an event in the codec's own JSON form. The core stores, cuts and shuffles
/// items, but never reads inside them.
pub type Item = serde_json::Value;

/// What a response to one attempt says about the request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The source took the request, so it is not sent again.
    Accepted,
    /// The source may not have taken it, and sending it again can help.
    Retryable,
    /// The source refused the request, and sending it again cannot help.
    Rejected,
}

/// A request body and the headers it is sent with.
pub struct Encoded {
    pub body: Bytes,
    pub headers: Vec<(&'static str, &'static str)>,
}

#[async_trait]
pub trait SourceCodec: Sync {
    /// The name on the command line (`--source-type`) and in the corpus header.
    fn name(&self) -> &'static str;

    /// Decodes one line of `vector tap` output into an item. On failure the reason is a short
    /// fixed name: capture counts reasons and refuses a capture that has any.
    fn decode_tap(&self, line: &str) -> Result<Item, &'static str>;

    /// Checks an item read from a corpus, so that `encode` can rely on its shape.
    fn validate(&self, item: &Item) -> Result<(), String>;

    /// Encodes items as one request. Each item is restamped with `sent_ms`, since a captured
    /// timestamp would be too old for the source to accept.
    fn encode(&self, items: &[Item], sent_ms: i64) -> Encoded;

    /// Classifies an attempt's HTTP status. 0 means no response was received.
    fn classify(&self, status: u16) -> Outcome;

    /// The URL check gives gen for the source listening at `addr`. The default is plain HTTP at
    /// `/`, which the source's HTTP endpoint serves.
    fn target_url(&self, addr: SocketAddr) -> String {
        format!("http://{addr}/")
    }

    /// Whether the source at `addr` is up and taking connections, so check can start gen. The
    /// default is a TCP connect.
    async fn ready(&self, addr: SocketAddr) -> bool {
        tokio::net::TcpStream::connect(addr).await.is_ok()
    }

    /// Sends one attempt of `encoded` to `target` and returns the status it got. The caller bounds
    /// the attempt's time and counts a failure as 0. The default is an HTTP POST of the body and
    /// headers to the target's URI; a source that is not HTTP replaces this method, and nothing
    /// else in gen changes.
    async fn send(
        &self,
        client: &Client,
        target: &Target,
        encoded: &Encoded,
    ) -> Result<u16, BoxError> {
        let mut request = hyper::Request::post(target.uri.clone());
        for (name, value) in &encoded.headers {
            request = request.header(*name, *value);
        }
        let request = request.body(hyper::Body::from(encoded.body.clone()))?;
        let response = client.request(request).await?;
        let status = response.status().as_u16();
        // Drained, so the connection can be reused.
        let _ = hyper::body::to_bytes(response.into_body()).await;
        Ok(status)
    }

    /// The name of the source's events-per-request histogram in its exporter, from which capture
    /// takes the mean request size. Its `_sum` and `_count` series are read under this name.
    fn request_size_metric(&self) -> &'static str {
        "vector_component_received_events_count"
    }
}

/// Every codec this build knows, by the name a corpus records.
pub const CODECS: &[&dyn SourceCodec] = &[&PromRemoteWrite];

/// The codec with this name, or an error that lists the known names.
pub fn codec_named(name: &str) -> Result<&'static dyn SourceCodec, BoxError> {
    CODECS
        .iter()
        .copied()
        .find(|codec| codec.name() == name)
        .ok_or_else(|| {
            let known: Vec<_> = CODECS.iter().map(|codec| codec.name()).collect();
            err(format!(
                "unknown source type `{name}`; known: {}",
                known.join(", ")
            ))
        })
}

#[cfg(test)]
#[allow(unused_imports, dead_code)] // Bench builds compile this module but never run it.
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_default_target_is_plain_http_and_ready_is_a_tcp_connect() {
        let codec = codec_named("prom_remote_write").unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        assert_eq!(codec.target_url(addr), format!("http://{addr}/"));
        assert!(codec.ready(addr).await);
        drop(listener);
        assert!(!codec.ready(addr).await);
    }
}
