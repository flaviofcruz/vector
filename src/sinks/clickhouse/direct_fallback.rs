//! Direct-sink ClickHouse fallback: retry the primary (proxy), then fail over to
//! the fallback (direct SMK) and retry that — the `clickhouse-proxy` dual-write
//! path (`fallback_endpoint` set, `use_headless_service = false`). Retriability
//! reuses `ClickhouseRetryLogic`. Finalizers are taken by the driver before
//! `call`, so cloning the request per attempt does not affect acknowledgements.

use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::future::BoxFuture;
use http::Uri;
use tokio::time::{sleep, timeout};
use tower::Service;
use tracing::warn;

use vector_lib::emit;

use super::headless::{EndpointServiceConfig, build_endpoint_service};
use super::service::{ClickhouseRetryLogic, ClickhouseServiceRequestBuilder};
use super::sink::PartitionKey;
use crate::http::HttpClient;
use crate::internal_events::{ClickhouseDirectFallbackRouted, ClickhouseDirectRetry};
use crate::sinks::util::http::{HttpRequest, HttpResponse, HttpService};
use crate::sinks::util::retries::RetryLogic;

type Inner = HttpService<ClickhouseServiceRequestBuilder, PartitionKey>;
type SinkResult = Result<HttpResponse, crate::Error>;

enum EndpointOutcome {
    Success(HttpResponse),
    /// Non-retriable — return as-is; failover would fail identically.
    NonRetriable(SinkResult),
    /// All attempts failed with retriable errors; carries the last result.
    Exhausted(SinkResult),
}

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

    /// Retries one endpoint up to `max_attempts` times with Fibonacci backoff;
    /// each hop is bounded by `per_call_timeout`.
    async fn run_endpoint(
        &self,
        svc: &mut Inner,
        request: &HttpRequest<PartitionKey>,
        label: &'static str,
    ) -> EndpointOutcome {
        let mut last: SinkResult = Err(format!("{label}: not attempted").into());
        let (mut prev, mut cur) = (Duration::ZERO, self.initial_backoff);

        for attempt in 1..=self.max_attempts_per_endpoint {
            match timeout(self.per_call_timeout, svc.call(request.clone())).await {
                Err(_elapsed) => {
                    warn!(message = "ClickHouse endpoint attempt timed out.", endpoint = label);
                    last = Err(format!("{label}: timed out").into());
                }
                Ok(Ok(resp)) => {
                    let action = self.retry_logic.should_retry_response(&resp);
                    if action.is_successful() {
                        return EndpointOutcome::Success(resp);
                    }
                    if action.is_not_retryable() {
                        return EndpointOutcome::NonRetriable(Ok(resp));
                    }
                    last = Ok(resp);
                }
                Ok(Err(e)) => last = Err(e),
            }

            if attempt < self.max_attempts_per_endpoint {
                emit!(ClickhouseDirectRetry { endpoint: label });
                sleep(cur.min(self.max_backoff)).await;
                let next = prev.checked_add(cur).unwrap_or(self.max_backoff).min(self.max_backoff);
                prev = cur;
                cur = next;
            }
        }

        EndpointOutcome::Exhausted(last)
    }
}

impl Service<HttpRequest<PartitionKey>> for DirectFallbackService {
    type Response = HttpResponse;
    type Error = crate::Error;
    type Future = BoxFuture<'static, SinkResult>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.primary.poll_ready(cx)
    }

    fn call(&mut self, request: HttpRequest<PartitionKey>) -> Self::Future {
        let this = self.clone();
        let (mut primary, mut fallback) = (this.primary.clone(), this.fallback.clone());

        Box::pin(async move {
            match this.run_endpoint(&mut primary, &request, "primary").await {
                EndpointOutcome::Success(resp) => return Ok(resp),
                EndpointOutcome::NonRetriable(res) => return res,
                EndpointOutcome::Exhausted(_) => {
                    emit!(ClickhouseDirectFallbackRouted);
                }
            }
            match this.run_endpoint(&mut fallback, &request, "fallback").await {
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
    use hyper::{Body, Response as HyperResponse, Server};
    use tower::ServiceExt;
    use vector_lib::finalization::EventFinalizers;
    use vector_lib::request_metadata::RequestMetadata;

    use super::*;
    use crate::config::ProxyConfig;
    use crate::sinks::clickhouse::config::Format;
    use crate::test_util::addr::next_addr;

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

    fn spawn_server(addr: std::net::SocketAddr, status: u16, body: &'static str) -> Arc<AtomicUsize> {
        spawn_flaky_server(addr, 0, status, status, body)
    }

    /// Spawns a server that returns `fail_status` for the first `fail_times`
    /// requests, then `ok_status` for the rest. `fail_times = 0` means always
    /// `ok_status`. Returns the hit counter.
    fn spawn_flaky_server(
        addr: std::net::SocketAddr,
        fail_times: usize,
        fail_status: u16,
        ok_status: u16,
        body: &'static str,
    ) -> Arc<AtomicUsize> {
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_srv = hits.clone();
        let make = make_service_fn(move |_| {
            let hits = hits_srv.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |_req| {
                    let hits = hits.clone();
                    async move {
                        let n = hits.fetch_add(1, Ordering::SeqCst);
                        let status = if n < fail_times { fail_status } else { ok_status };
                        Ok::<_, Infallible>(HyperResponse::builder().status(status).body(Body::from(body)).unwrap())
                    }
                }))
            }
        });
        tokio::spawn(async move {
            let _ = Server::bind(&addr).serve(make).await;
        });
        hits
    }

    /// Sends one request through a service pointed at `p_addr`/`fb_addr`, with
    /// `attempts` per endpoint and 1ms backoff. Servers must already be spawned.
    async fn drive(
        p_addr: std::net::SocketAddr,
        fb_addr: std::net::SocketAddr,
        non_retriable_codes: Option<Vec<u32>>,
        attempts: usize,
    ) -> SinkResult {
        let cfg = EndpointServiceConfig {
            auth: None,
            skip_unknown_fields: None,
            date_time_best_effort: false,
            insert_random_shard: false,
            compression: Default::default(),
            query_settings: Default::default(),
        };
        let uri = |a: std::net::SocketAddr| format!("http://{}:{}/", a.ip(), a.port()).parse().unwrap();
        let client = HttpClient::new(None, &ProxyConfig::default()).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        DirectFallbackService::new(
            &client, uri(p_addr), uri(fb_addr), &cfg, non_retriable_codes,
            attempts, Duration::from_millis(1), Duration::from_millis(1), Duration::from_secs(2),
        )
        .oneshot(test_request())
        .await
    }

    /// `run` for the common case: `Some((status, body))` spawns a live server,
    /// `None` leaves the port unbound (connection refused). 2 attempts/endpoint.
    /// Returns `(primary_hits, fallback_hits, result)`.
    async fn run(
        primary: Option<(u16, &'static str)>,
        fallback: Option<(u16, &'static str)>,
        non_retriable_codes: Option<Vec<u32>>,
    ) -> (usize, usize, SinkResult) {
        let (_pg, p_addr) = next_addr();
        let (_fg, fb_addr) = next_addr();
        let no_hits = || Arc::new(AtomicUsize::new(0));
        let p_hits = primary.map_or_else(no_hits, |(s, b)| spawn_server(p_addr, s, b));
        let fb_hits = fallback.map_or_else(no_hits, |(s, b)| spawn_server(fb_addr, s, b));
        let result = drive(p_addr, fb_addr, non_retriable_codes, 2).await;
        (p_hits.load(Ordering::SeqCst), fb_hits.load(Ordering::SeqCst), result)
    }

    // --- Primary succeeds (no failover) ---

    #[tokio::test]
    async fn uses_primary_and_skips_fallback_when_healthy() {
        let (p, fb, res) = run(Some((200, "")), None, None).await;
        assert!(res.is_ok());
        assert_eq!((p, fb), (1, 0));
    }

    #[tokio::test]
    async fn primary_retries_then_succeeds_without_failover() {
        // Primary fails once (503) then succeeds on the 2nd attempt; fallback untouched.
        let (_pg, p_addr) = next_addr();
        let (_fg, fb_addr) = next_addr();
        let p_hits = spawn_flaky_server(p_addr, 1, 503, 200, "");
        let fb_hits = spawn_server(fb_addr, 200, "");
        let res = drive(p_addr, fb_addr, None, 2).await;
        assert!(res.is_ok());
        assert_eq!(p_hits.load(Ordering::SeqCst), 2);
        assert_eq!(fb_hits.load(Ordering::SeqCst), 0);
    }

    // --- Primary fails, fallback succeeds ---

    #[tokio::test]
    async fn falls_back_when_primary_connection_refused() {
        let (p, fb, res) = run(None, Some((200, "")), None).await;
        assert!(res.is_ok());
        assert_eq!((p, fb), (0, 1));
    }

    #[tokio::test]
    async fn retries_primary_then_falls_back_on_transient_5xx() {
        let (p, fb, res) = run(Some((503, "overloaded")), Some((200, "")), None).await;
        assert!(res.is_ok());
        assert_eq!((p, fb), (2, 1));
    }

    #[tokio::test]
    async fn fallback_retries_then_succeeds() {
        // Primary exhausts on 503; fallback fails once then succeeds on its 2nd attempt.
        let (_pg, p_addr) = next_addr();
        let (_fg, fb_addr) = next_addr();
        let p_hits = spawn_server(p_addr, 503, "overloaded");
        let fb_hits = spawn_flaky_server(fb_addr, 1, 503, 200, "");
        let res = drive(p_addr, fb_addr, None, 2).await;
        assert!(res.is_ok());
        assert_eq!(p_hits.load(Ordering::SeqCst), 2);
        assert_eq!(fb_hits.load(Ordering::SeqCst), 2);
    }

    // --- Non-retriable: stop, never fail over ---

    #[tokio::test]
    async fn does_not_fall_back_on_non_retriable_clickhouse_error() {
        let (p, fb, res) = run(Some((500, "Code: 70. DB::Exception")), Some((200, "")), Some(vec![70])).await;
        assert_eq!(res.unwrap().http_response.status().as_u16(), 500);
        assert_eq!((p, fb), (1, 0));
    }

    #[tokio::test]
    async fn non_retriable_on_fallback_is_returned_as_is() {
        // Primary exhausts on 503, fails over; fallback returns a non-retriable 500 (code 70).
        let (p, fb, res) =
            run(Some((503, "overloaded")), Some((500, "Code: 70. DB::Exception")), Some(vec![70])).await;
        assert_eq!(res.unwrap().http_response.status().as_u16(), 500);
        assert_eq!((p, fb), (2, 1));
    }

    // --- Both endpoints fail ---

    #[tokio::test]
    async fn both_endpoints_unreachable_returns_error() {
        // Both ports unbound: primary and fallback each exhaust on connection refused.
        let (p, fb, res) = run(None, None, None).await;
        assert!(res.is_err());
        assert_eq!((p, fb), (0, 0));
    }

    #[tokio::test]
    async fn both_endpoints_exhaust_on_transient_5xx() {
        // Both return 503: each retried to max_attempts; the fallback's last 5xx is surfaced.
        let (p, fb, res) = run(Some((503, "overloaded")), Some((503, "still down")), None).await;
        assert_eq!(res.unwrap().http_response.status().as_u16(), 503);
        assert_eq!((p, fb), (2, 2));
    }

    #[tokio::test]
    async fn primary_unreachable_fallback_exhausts_5xx() {
        // Primary unreachable (connection refused), fallback keeps returning 503.
        let (p, fb, res) = run(None, Some((503, "still down")), None).await;
        assert_eq!(res.unwrap().http_response.status().as_u16(), 503);
        assert_eq!((p, fb), (0, 2));
    }

    // --- Config edge: no retries still fails over ---

    #[tokio::test]
    async fn single_attempt_still_fails_over() {
        // attempts = 1 (no retries): one primary hit, then fail over.
        let (_pg, p_addr) = next_addr();
        let (_fg, fb_addr) = next_addr();
        let p_hits = spawn_server(p_addr, 503, "overloaded");
        let fb_hits = spawn_server(fb_addr, 200, "");
        let res = drive(p_addr, fb_addr, None, 1).await;
        assert!(res.is_ok());
        assert_eq!(p_hits.load(Ordering::SeqCst), 1, "no retries when attempts = 1");
        assert_eq!(fb_hits.load(Ordering::SeqCst), 1);
    }
}
