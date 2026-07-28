//! Direct-sink ClickHouse fallback: retry the primary (proxy), then fail over to
//! the fallback (direct SMK) and retry that — the `clickhouse-proxy` dual-write
//! path (`fallback_endpoint` set, `use_headless_service = false`).
//!
//! Each endpoint is wrapped in Vector's standard retry stack
//! (`Retry<FibonacciRetryPolicy<ClickhouseRetryLogic>, Timeout<HttpService>>`),
//! so per-endpoint retries inherit the configured jitter (`retry_jitter_mode`,
//! full jitter by default) and emit the standard `sink_requests_completed_total`
//! / `sink_attempts_per_request` metrics — identical to every other Vector sink.
//! `DirectFallbackService` itself only sequences the two endpoints: run the
//! primary, and if its final result is a retriable-but-exhausted failure, fail
//! over to the fallback. A non-retriable ClickHouse error (bad schema / type
//! mismatch / constraint violation) is returned as-is without failing over,
//! since the same rows would fail identically on the fallback.
//!
//! Retriability reuses `ClickhouseRetryLogic`. Finalizers are taken by the
//! driver before `call`, so cloning the request per attempt does not affect
//! acknowledgements.

use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::future::BoxFuture;
use http::Uri;
use tower::retry::Retry;
use tower::timeout::Timeout;
use tower::{Service, ServiceBuilder, ServiceExt};

use vector_lib::emit;

use super::headless::{EndpointServiceConfig, build_endpoint_service};
use super::service::{ClickhouseRetryLogic, ClickhouseServiceRequestBuilder};
use super::sink::PartitionKey;
use crate::http::HttpClient;
use crate::internal_events::ClickhouseDirectFallbackRouted;
use crate::sinks::util::http::{HttpRequest, HttpResponse, HttpService};
use crate::sinks::util::retries::{FibonacciRetryPolicy, JitterMode, RetryLogic};

type Inner = HttpService<ClickhouseServiceRequestBuilder, PartitionKey>;

/// One endpoint's full request path: the standard Fibonacci retry policy (with
/// jitter and standard metrics) wrapping a per-attempt-timed `HttpService`.
type RetryingEndpoint = Retry<FibonacciRetryPolicy<ClickhouseRetryLogic>, Timeout<Inner>>;

type SinkResult = Result<HttpResponse, crate::Error>;

/// Backoff/jitter/attempt parameters for one endpoint's retry policy.
///
/// Mirrors the sink's `TowerRequestSettings`, captured in `build_direct` before
/// the outer retry layer is disabled.
#[derive(Clone, Copy)]
pub(super) struct RetrySettings {
    /// Total attempts per endpoint (`retry_attempts + 1`).
    pub max_attempts_per_endpoint: usize,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub jitter_mode: JitterMode,
    pub per_call_timeout: Duration,
}

#[derive(Clone)]
pub(super) struct DirectFallbackService {
    primary: RetryingEndpoint,
    fallback: RetryingEndpoint,
    /// Classifies the final per-endpoint result to decide whether to fail over.
    retry_logic: ClickhouseRetryLogic,
}

impl DirectFallbackService {
    pub(super) fn new(
        client: &HttpClient,
        primary_endpoint: Uri,
        fallback_endpoint: Uri,
        svc_config: &EndpointServiceConfig,
        non_retriable_codes: Option<Vec<u32>>,
        settings: RetrySettings,
    ) -> Self {
        let retry_logic = ClickhouseRetryLogic::new(non_retriable_codes);
        Self {
            primary: Self::build_endpoint(client, primary_endpoint, svc_config, &retry_logic, &settings),
            fallback: Self::build_endpoint(client, fallback_endpoint, svc_config, &retry_logic, &settings),
            retry_logic,
        }
    }

    /// Wraps a single endpoint's `HttpService` in a per-attempt timeout and the
    /// standard Fibonacci retry policy, so retries honor `jitter_mode` and emit
    /// the standard sink retry metrics.
    fn build_endpoint(
        client: &HttpClient,
        endpoint: Uri,
        svc_config: &EndpointServiceConfig,
        retry_logic: &ClickhouseRetryLogic,
        settings: &RetrySettings,
    ) -> RetryingEndpoint {
        let inner = build_endpoint_service(client, endpoint, svc_config);
        // `retry_attempts` in the policy is the number of *retries*, so subtract
        // the initial attempt from the total-attempts budget (min 0).
        let retries = settings.max_attempts_per_endpoint.saturating_sub(1);
        let policy = FibonacciRetryPolicy::new(
            retries,
            settings.initial_backoff,
            settings.max_backoff,
            retry_logic.clone(),
            settings.jitter_mode,
        );
        ServiceBuilder::new()
            .retry(policy)
            .timeout(settings.per_call_timeout)
            .service(inner)
    }

    /// Drives one endpoint's retry stack to completion and returns its final
    /// result. `should_fail_over` decides what that result means for the caller.
    async fn run_endpoint(svc: &mut RetryingEndpoint, request: &HttpRequest<PartitionKey>) -> SinkResult {
        svc.ready().await?.call(request.clone()).await
    }

    /// A final per-endpoint result is worth failing over only when it is a
    /// retriable failure the endpoint's own retries could not clear. A success or
    /// a non-retriable error (e.g. a deterministic ClickHouse data error) is not.
    fn should_fail_over(&self, result: &SinkResult) -> bool {
        match result {
            Ok(resp) => self.retry_logic.should_retry_response(resp).is_retryable(),
            // The endpoint's retry policy only surfaces an error after exhausting
            // retriable ones (or on a non-retriable transport error). Failing over
            // on any error is safe: the fallback gets a chance, and a fallback that
            // fails identically returns its own error anyway.
            Err(_) => true,
        }
    }
}

impl Service<HttpRequest<PartitionKey>> for DirectFallbackService {
    type Response = HttpResponse;
    type Error = crate::Error;
    type Future = BoxFuture<'static, SinkResult>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Readiness is driven per-call via `ready()` inside `run_endpoint`, so the
        // driver's poll_ready is always satisfied here (matching how the outer
        // Tower stack polls a cloned service per request).
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: HttpRequest<PartitionKey>) -> Self::Future {
        let this = self.clone();
        let (mut primary, mut fallback) = (this.primary.clone(), this.fallback.clone());

        Box::pin(async move {
            let primary_result = Self::run_endpoint(&mut primary, &request).await;
            if !this.should_fail_over(&primary_result) {
                return primary_result;
            }

            emit!(ClickhouseDirectFallbackRouted);
            Self::run_endpoint(&mut fallback, &request).await
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
    /// `attempts` per endpoint and 1ms backoff. Jitter is disabled so retry
    /// timing stays deterministic in tests. Servers must already be spawned.
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
            &client,
            uri(p_addr),
            uri(fb_addr),
            &cfg,
            non_retriable_codes,
            RetrySettings {
                max_attempts_per_endpoint: attempts,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(1),
                jitter_mode: JitterMode::None,
                per_call_timeout: Duration::from_secs(2),
            },
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
