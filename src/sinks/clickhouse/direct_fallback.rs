//! Direct-sink ClickHouse fallback: retry the primary (proxy), then fail over to
//! the fallback (direct SMK) and retry that. Used when `fallback_endpoint` is set
//! with `use_headless_service = false` — the `clickhouse-proxy` dual-write path.
//!
//! Retriability reuses `ClickhouseRetryLogic` (connection errors + transient 5xx
//! retry; non-retriable ClickHouse errors stop without failing over). The service
//! owns its retry loop, so `build_direct` disables the outer retry layer and
//! widens the outer timeout. Finalizers are taken by the driver before `call`, so
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
    /// Succeeded — return it, skip the other endpoint.
    Success(HttpResponse),
    /// Non-retriable failure — return as-is; failover would fail identically.
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

/// Retries one endpoint up to `max_attempts` times with Fibonacci backoff; each
/// hop is bounded by `per_call_timeout`.
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
        // `HttpService::poll_ready` is always ready, so dispatch directly.
        match timeout(per_call_timeout, svc.call(request.clone())).await {
            Err(_elapsed) => {
                warn!(message = "ClickHouse endpoint attempt timed out.", endpoint = label);
                last = Err(format!("{label}: timed out after {per_call_timeout:?}").into());
            }
            Ok(Ok(resp)) => {
                let action = logic.should_retry_response(&resp);
                if action.is_successful() {
                    return EndpointOutcome::Success(resp);
                }
                if action.is_not_retryable() {
                    // Deterministic failure (e.g. bad schema): don't retry or fail over.
                    return EndpointOutcome::NonRetriable(Ok(resp));
                }
                last = Ok(resp); // retriable 5xx
            }
            Ok(Err(e)) => last = Err(e), // connection error: retriable
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

    fn test_config() -> EndpointServiceConfig {
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

    /// Runs one request through the service. `Some((status, body))` spawns a live
    /// server; `None` leaves the port unbound (connection refused). Returns
    /// `(primary_hits, fallback_hits, result)`. Uses 2 attempts/endpoint and 1ms
    /// backoff so tests are fast.
    async fn run(
        primary: Option<(u16, &'static str)>,
        fallback: Option<(u16, &'static str)>,
        non_retriable_codes: Option<Vec<u32>>,
    ) -> (usize, usize, Result<HttpResponse, crate::Error>) {
        let (_pg, p_addr) = next_addr();
        let (_fg, fb_addr) = next_addr();
        let p_hits = primary.map_or_else(|| Arc::new(AtomicUsize::new(0)), |(s, b)| spawn_server(p_addr, s, b));
        let fb_hits = fallback.map_or_else(|| Arc::new(AtomicUsize::new(0)), |(s, b)| spawn_server(fb_addr, s, b));
        tokio::time::sleep(Duration::from_millis(50)).await;

        let client = HttpClient::new(None, &ProxyConfig::default()).unwrap();
        let uri = |a: std::net::SocketAddr| format!("http://{}:{}/", a.ip(), a.port()).parse().unwrap();
        let svc = DirectFallbackService::new(
            &client, uri(p_addr), uri(fb_addr), &test_config(), non_retriable_codes,
            2, Duration::from_millis(1), Duration::from_millis(1), Duration::from_secs(2),
        );

        let result = svc.oneshot(test_request()).await;
        (p_hits.load(Ordering::SeqCst), fb_hits.load(Ordering::SeqCst), result)
    }

    #[tokio::test]
    async fn falls_back_when_primary_connection_refused() {
        let (p, fb, res) = run(None, Some((200, "")), None).await;
        assert!(res.is_ok(), "fallback should serve the request");
        assert_eq!((p, fb), (0, 1));
    }

    #[tokio::test]
    async fn uses_primary_and_skips_fallback_when_healthy() {
        let (p, fb, res) = run(Some((200, "")), None, None).await;
        assert!(res.is_ok());
        assert_eq!((p, fb), (1, 0), "primary serves on first attempt, fallback untouched");
    }

    #[tokio::test]
    async fn retries_primary_then_falls_back_on_transient_5xx() {
        let (p, fb, res) = run(Some((503, "overloaded")), Some((200, "")), None).await;
        assert!(res.is_ok());
        assert_eq!((p, fb), (2, 1), "primary retried to max_attempts, then fallback serves");
    }

    #[tokio::test]
    async fn does_not_fall_back_on_non_retriable_clickhouse_error() {
        let (p, fb, res) =
            run(Some((500, "Code: 70. DB::Exception")), Some((200, "")), Some(vec![70])).await;
        assert_eq!(res.unwrap().http_response.status().as_u16(), 500);
        assert_eq!((p, fb), (1, 0), "non-retriable: not retried, no failover");
    }
}
