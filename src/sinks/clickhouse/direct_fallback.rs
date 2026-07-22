//! Direct-endpoint ClickHouse sink with a single-hop fallback.
//!
//! Used when `fallback_endpoint` is set while `use_headless_service` is false —
//! the configuration the `clickhouse-proxy` dual-write path uses. Requests are
//! sent to the primary endpoint (the proxy); if the primary returns a
//! connection-level error (proxy pods gone, connection refused/reset, connect
//! timeout), the *same* request is immediately re-dispatched to the fallback
//! endpoint (the direct SMK ClusterIP write service) within the same call.
//!
//! Only connection-level failures trigger the fallback. A `5xx` *response* from
//! the proxy is a delivered HTTP response, not a connection failure, and is left
//! to the sink's retry logic — matching the semantics of the headless service's
//! ClusterIP fallback (`headless::is_connection_error`).

use std::task::{Context, Poll};

use futures_util::future::BoxFuture;
use http::Uri;
use tower::Service;
use tracing::warn;

use super::headless::{EndpointServiceConfig, build_endpoint_service, is_connection_error};
use super::service::ClickhouseServiceRequestBuilder;
use super::sink::PartitionKey;
use crate::http::HttpClient;
use crate::sinks::util::http::{HttpRequest, HttpResponse, HttpService};

type Inner = HttpService<ClickhouseServiceRequestBuilder, PartitionKey>;

/// A Tower service that sends each request to a primary endpoint and, on a
/// connection-level failure, re-dispatches the same request to a fallback
/// endpoint. Failover is per-request and immediate — there is no sticky state,
/// so every request independently prefers the primary and only falls through
/// when the primary is unreachable.
#[derive(Clone)]
pub(super) struct DirectFallbackService {
    primary: Inner,
    fallback: Inner,
}

impl DirectFallbackService {
    pub(super) fn new(
        client: &HttpClient,
        primary_endpoint: Uri,
        fallback_endpoint: Uri,
        svc_config: &EndpointServiceConfig,
    ) -> Self {
        Self {
            primary: build_endpoint_service(client, primary_endpoint, svc_config),
            fallback: build_endpoint_service(client, fallback_endpoint, svc_config),
        }
    }
}

impl Service<HttpRequest<PartitionKey>> for DirectFallbackService {
    type Response = HttpResponse;
    type Error = crate::Error;
    type Future = BoxFuture<'static, Result<HttpResponse, crate::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // `HttpService` is stateless and always ready; the fallback (also an
        // `HttpService`) is therefore ready too, so polling the primary is
        // sufficient to satisfy the Tower readiness contract.
        self.primary.poll_ready(cx)
    }

    fn call(&mut self, request: HttpRequest<PartitionKey>) -> Self::Future {
        // Clone the request so the fallback can re-send it if the primary hits a
        // connection error. The Tower retry layer already clones requests between
        // attempts, so this does not change finalizer/acknowledgement semantics.
        let fallback_request = request.clone();
        let primary_fut = self.primary.call(request);
        let mut fallback = self.fallback.clone();

        Box::pin(async move {
            match primary_fut.await {
                Err(e) if is_connection_error(&e) => {
                    metrics::counter!("clickhouse_direct_fallback_routed_total").increment(1);
                    warn!(
                        message = "ClickHouse primary endpoint unreachable; routing request to fallback endpoint.",
                        error = %e,
                    );
                    fallback.call(fallback_request).await
                }
                other => other,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use bytes::Bytes;
    use hyper::service::{make_service_fn, service_fn};
    use hyper::{Body, Response, Server};
    use tower::ServiceExt;
    use vector_lib::finalization::EventFinalizers;
    use vector_lib::request_metadata::RequestMetadata;

    use super::*;
    use crate::config::ProxyConfig;
    use crate::sinks::clickhouse::config::Format;
    use crate::test_util::addr::next_addr;

    fn make_test_client() -> HttpClient {
        let proxy = ProxyConfig::default();
        HttpClient::new(None, &proxy).unwrap()
    }

    fn make_test_config() -> EndpointServiceConfig {
        EndpointServiceConfig {
            auth: None,
            skip_unknown_fields: None,
            date_time_best_effort: false,
            insert_random_shard: false,
            compression: Default::default(),
            query_settings: Default::default(),
        }
    }

    fn test_request() -> HttpRequest<PartitionKey> {
        HttpRequest::new(
            Bytes::from_static(b"{}\n"),
            EventFinalizers::default(),
            RequestMetadata::default(),
            PartitionKey {
                database: "db".to_string(),
                table: "t".to_string(),
                format: Format::JsonEachRow,
            },
        )
    }

    /// Spawns a hyper server on `addr` that counts requests and returns 200.
    fn spawn_counting_server(addr: std::net::SocketAddr) -> Arc<AtomicUsize> {
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_srv = hits.clone();
        let make = make_service_fn(move |_| {
            let hits = hits_srv.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |_req| {
                    let hits = hits.clone();
                    async move {
                        hits.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, Infallible>(Response::new(Body::from("")))
                    }
                }))
            }
        });
        tokio::spawn(async move {
            let _ = Server::bind(&addr).serve(make).await;
        });
        hits
    }

    fn uri_for(addr: std::net::SocketAddr) -> Uri {
        format!("http://{}:{}/", addr.ip(), addr.port())
            .parse()
            .unwrap()
    }

    #[tokio::test]
    async fn routes_to_fallback_when_primary_connection_refused() {
        // Fallback: a live server. Primary: a reserved-but-unbound port, so the
        // connect is refused immediately (a connection-level error).
        let (_fb_guard, fb_addr) = next_addr();
        let fb_hits = spawn_counting_server(fb_addr);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let (_p_guard, primary_addr) = next_addr();

        let client = make_test_client();
        let svc = DirectFallbackService::new(
            &client,
            uri_for(primary_addr),
            uri_for(fb_addr),
            &make_test_config(),
        );

        let resp = svc.oneshot(test_request()).await;
        assert!(
            resp.is_ok(),
            "fallback should have served the request, got error: {}",
            resp.err().map(|e| e.to_string()).unwrap_or_default(),
        );
        assert_eq!(
            fb_hits.load(Ordering::SeqCst),
            1,
            "the fallback endpoint should have received exactly one request"
        );
    }

    #[tokio::test]
    async fn uses_primary_and_skips_fallback_when_primary_healthy() {
        // Primary: a live server. Fallback: a reserved-but-unbound port that would
        // fail if we ever hit it — proving the fallback is not exercised.
        let (_p_guard, primary_addr) = next_addr();
        let primary_hits = spawn_counting_server(primary_addr);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let (_fb_guard, fb_addr) = next_addr();

        let client = make_test_client();
        let svc = DirectFallbackService::new(
            &client,
            uri_for(primary_addr),
            uri_for(fb_addr),
            &make_test_config(),
        );

        let resp = svc.oneshot(test_request()).await;
        assert!(
            resp.is_ok(),
            "primary should have served the request, got error: {}",
            resp.err().map(|e| e.to_string()).unwrap_or_default(),
        );
        assert_eq!(
            primary_hits.load(Ordering::SeqCst),
            1,
            "the primary endpoint should have received exactly one request"
        );
    }
}
