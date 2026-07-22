//! Direct-endpoint ClickHouse sink that retries a primary endpoint, then fails
//! over to a fallback endpoint and retries that.
//!
//! Used when `fallback_endpoint` is set with `use_headless_service = false` — the
//! `clickhouse-proxy` dual-write path (primary = proxy, fallback = direct SMK).
//! Retriability uses the normal sink rules (`ClickhouseRetryLogic`): connection
//! errors and transient 5xx retry; a non-retriable ClickHouse error (bad
//! schema/type) stops without failing over, since it would fail identically on
//! the fallback.
//!
//! This service owns its retry loop, so `build_direct` disables the outer Tower
//! retry layer and widens the outer timeout for this path; each hop is bounded by
//! `per_call_timeout`. Finalizers are taken by the driver before `call`, so
//! cloning the request per attempt does not affect acknowledgements.

use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::future::BoxFuture;
use http::Uri;
use tokio::time::{sleep, timeout};
use tower::Service;
use tracing::warn;

use super::headless::{EndpointServiceConfig, build_endpoint_service};
use super::service::{ClickhouseRetryLogic, ClickhouseServiceRequestBuilder};
use super::sink::PartitionKey;
use crate::http::HttpClient;
use crate::sinks::util::http::{HttpRequest, HttpResponse, HttpService};
use crate::sinks::util::retries::RetryLogic;

type Inner = HttpService<ClickhouseServiceRequestBuilder, PartitionKey>;

/// Result of retrying one endpoint.
enum EndpointOutcome {
    /// Succeeded — return it, don't try the other endpoint.
    Success(HttpResponse),
    /// Non-retriable failure — return as-is; failing over would fail identically.
    NonRetriable(Result<HttpResponse, crate::Error>),
    /// All attempts failed with retriable errors; carries the last result.
    Exhausted(Result<HttpResponse, crate::Error>),
}

/// Retries `primary`, then fails over to `fallback`. See module docs.
#[derive(Clone)]
pub(super) struct DirectFallbackService {
    primary: Inner,
    fallback: Inner,
    retry_logic: ClickhouseRetryLogic,
    max_attempts_per_endpoint: usize,
    initial_backoff: Duration,
    max_backoff: Duration,
    per_call_timeout: Duration,
}

impl DirectFallbackService {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        client: &HttpClient,
        primary_endpoint: Uri,
        fallback_endpoint: Uri,
        svc_config: &EndpointServiceConfig,
        non_retriable_codes: Option<Vec<u32>>,
        max_attempts_per_endpoint: usize,
        initial_backoff: Duration,
        max_backoff: Duration,
        per_call_timeout: Duration,
    ) -> Self {
        Self {
            primary: build_endpoint_service(client, primary_endpoint, svc_config),
            fallback: build_endpoint_service(client, fallback_endpoint, svc_config),
            retry_logic: ClickhouseRetryLogic::new(non_retriable_codes),
            max_attempts_per_endpoint: max_attempts_per_endpoint.max(1),
            initial_backoff,
            max_backoff,
            per_call_timeout,
        }
    }
}

/// Retries one endpoint up to `max_attempts` times with Fibonacci backoff,
/// each hop bounded by `per_call_timeout`.
#[allow(clippy::too_many_arguments)]
async fn run_endpoint(
    svc: &mut Inner,
    request: &HttpRequest<PartitionKey>,
    logic: &ClickhouseRetryLogic,
    max_attempts: usize,
    initial_backoff: Duration,
    max_backoff: Duration,
    per_call_timeout: Duration,
    label: &'static str,
) -> EndpointOutcome {
    let mut last: Result<HttpResponse, crate::Error> = Err(format!("{label}: not attempted").into());
    let (mut prev, mut cur) = (Duration::ZERO, initial_backoff);

    for attempt in 1..=max_attempts {
        // `HttpService::poll_ready` is always `Ready(Ok(()))`, so we dispatch
        // directly (matching the sink driver's use of the inner service).
        match timeout(per_call_timeout, svc.call(request.clone())).await {
            // Per-hop timeout: retriable failure.
            Err(_elapsed) => {
                warn!(message = "ClickHouse endpoint attempt timed out.", endpoint = label);
                last = Err(format!("{label}: timed out after {per_call_timeout:?}").into());
            }
            Ok(Ok(resp)) => {
                let action = logic.should_retry_response(&resp);
                if action.is_successful() {
                    return EndpointOutcome::Success(resp);
                }
                // Deterministic failure (e.g. bad schema): don't retry or fail over.
                if action.is_not_retryable() {
                    return EndpointOutcome::NonRetriable(Ok(resp));
                }
                last = Ok(resp); // retriable 5xx
            }
            // Transport/connection error: retriable.
            Ok(Err(e)) => last = Err(e),
        }

        if attempt < max_attempts {
            sleep(cur.min(max_backoff)).await;
            let next = prev.checked_add(cur).unwrap_or(max_backoff).min(max_backoff);
            prev = cur;
            cur = next;
        }
    }

    EndpointOutcome::Exhausted(last)
}

impl Service<HttpRequest<PartitionKey>> for DirectFallbackService {
    type Response = HttpResponse;
    type Error = crate::Error;
    type Future = BoxFuture<'static, Result<HttpResponse, crate::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Both inners are stateless `HttpService`s (always ready).
        self.primary.poll_ready(cx)
    }

    fn call(&mut self, request: HttpRequest<PartitionKey>) -> Self::Future {
        let mut primary = self.primary.clone();
        let mut fallback = self.fallback.clone();
        let logic = self.retry_logic.clone();
        let attempts = self.max_attempts_per_endpoint;
        let initial = self.initial_backoff;
        let max_backoff = self.max_backoff;
        let per_call = self.per_call_timeout;

        Box::pin(async move {
            // Phase 1: primary (proxy).
            match run_endpoint(
                &mut primary, &request, &logic, attempts, initial, max_backoff, per_call, "primary",
            )
            .await
            {
                EndpointOutcome::Success(resp) => return Ok(resp),
                EndpointOutcome::NonRetriable(res) => return res,
                EndpointOutcome::Exhausted(_) => {
                    metrics::counter!("clickhouse_direct_fallback_routed_total").increment(1);
                    warn!(message = "ClickHouse primary exhausted retries; failing over to fallback.");
                }
            }

            // Phase 2: fallback (direct SMK). Surface its last result if it also fails.
            match run_endpoint(
                &mut fallback, &request, &logic, attempts, initial, max_backoff, per_call, "fallback",
            )
            .await
            {
                EndpointOutcome::Success(resp) => Ok(resp),
                EndpointOutcome::NonRetriable(res) | EndpointOutcome::Exhausted(res) => res,
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
        HttpClient::new(None, &ProxyConfig::default()).unwrap()
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

    fn test_service(
        client: &HttpClient,
        primary: std::net::SocketAddr,
        fallback: std::net::SocketAddr,
        non_retriable_codes: Option<Vec<u32>>,
    ) -> DirectFallbackService {
        DirectFallbackService::new(
            client,
            uri_for(primary),
            uri_for(fallback),
            &make_test_config(),
            non_retriable_codes,
            2,                          // max_attempts_per_endpoint
            Duration::from_millis(1),   // initial_backoff
            Duration::from_millis(1),   // max_backoff
            Duration::from_secs(2),     // per_call_timeout
        )
    }

    /// Spawns a hyper server that counts requests and returns `status`/`body`.
    fn spawn_server(addr: std::net::SocketAddr, status: u16, body: &'static str) -> Arc<AtomicUsize> {
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_srv = hits.clone();
        let make = make_service_fn(move |_| {
            let hits = hits_srv.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |_req| {
                    let hits = hits.clone();
                    async move {
                        hits.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, Infallible>(
                            Response::builder().status(status).body(Body::from(body)).unwrap(),
                        )
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
        format!("http://{}:{}/", addr.ip(), addr.port()).parse().unwrap()
    }

    #[tokio::test]
    async fn routes_to_fallback_when_primary_connection_refused() {
        // Primary: unbound port (connect refused). Fallback: live server.
        let (_fb_guard, fb_addr) = next_addr();
        let fb_hits = spawn_server(fb_addr, 200, "");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let (_p_guard, primary_addr) = next_addr();

        let client = make_test_client();
        let svc = test_service(&client, primary_addr, fb_addr, None);

        let resp = svc.oneshot(test_request()).await;
        assert!(resp.is_ok(), "fallback should serve the request");
        assert_eq!(fb_hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn uses_primary_and_skips_fallback_when_primary_healthy() {
        // Primary: live server. Fallback: unbound port (would fail if hit).
        let (_p_guard, primary_addr) = next_addr();
        let primary_hits = spawn_server(primary_addr, 200, "");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let (_fb_guard, fb_addr) = next_addr();

        let client = make_test_client();
        let svc = test_service(&client, primary_addr, fb_addr, None);

        let resp = svc.oneshot(test_request()).await;
        assert!(resp.is_ok());
        assert_eq!(primary_hits.load(Ordering::SeqCst), 1, "primary serves on first attempt");
    }

    #[tokio::test]
    async fn retries_primary_then_falls_back_on_transient_5xx() {
        // Primary: always 503 (retriable). Fallback: 200.
        let (_p_guard, primary_addr) = next_addr();
        let primary_hits = spawn_server(primary_addr, 503, "overloaded");
        let (_fb_guard, fb_addr) = next_addr();
        let fb_hits = spawn_server(fb_addr, 200, "");
        tokio::time::sleep(Duration::from_millis(50)).await;

        let client = make_test_client();
        let svc = test_service(&client, primary_addr, fb_addr, None);

        let resp = svc.oneshot(test_request()).await;
        assert!(resp.is_ok());
        assert_eq!(primary_hits.load(Ordering::SeqCst), 2, "primary retried to max_attempts");
        assert_eq!(fb_hits.load(Ordering::SeqCst), 1, "fallback serves after primary exhausts");
    }

    #[tokio::test]
    async fn does_not_fall_back_on_non_retriable_clickhouse_error() {
        // Primary: 500 with non-retriable code 70. No retry, no failover.
        let (_p_guard, primary_addr) = next_addr();
        let primary_hits = spawn_server(primary_addr, 500, "Code: 70. DB::Exception: cannot convert type");
        let (_fb_guard, fb_addr) = next_addr();
        let fb_hits = spawn_server(fb_addr, 200, "");
        tokio::time::sleep(Duration::from_millis(50)).await;

        let client = make_test_client();
        let svc = test_service(&client, primary_addr, fb_addr, Some(vec![70]));

        let resp = svc.oneshot(test_request()).await.expect("returns Ok even for a 5xx");
        assert_eq!(resp.http_response.status().as_u16(), 500);
        assert_eq!(primary_hits.load(Ordering::SeqCst), 1, "non-retriable is not retried");
        assert_eq!(fb_hits.load(Ordering::SeqCst), 0, "non-retriable does not fail over");
    }
}
