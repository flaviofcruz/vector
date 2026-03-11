//! Headless Kubernetes service support for the ClickHouse sink.
//!
//! When `use_headless_service` is enabled, the configured endpoint hostname is
//! resolved via DNS to discover individual pod IPs. Requests are dispatched
//! using Tower's Power of Two Choices (P2C) load balancer across all resolved
//! endpoints, routing each request to the endpoint with fewer in-flight requests.
//!
//! Failed endpoints (connection errors) are removed from the active set.
//! A background task periodically re-resolves DNS to discover new pods and
//! re-add recovered ones.

use std::collections::HashSet;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::future::BoxFuture;
use http::Uri;
use tokio::sync::{Notify, mpsc, watch};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tower::balance::p2c::Balance;
use tower::buffer::Buffer;
use tower::discover::Change;
use tower::load::Load;

use super::config::QuerySettingsConfig;
use super::dns;
use super::service::ClickhouseServiceRequestBuilder;
use super::sink::PartitionKey;
use crate::http::{Auth, HttpClient, HttpError};
use crate::sinks::prelude::*;
use crate::sinks::util::http::{HttpRequest, HttpResponse, HttpService};

/// Default DNS refresh interval.
const DEFAULT_DNS_REFRESH_SECS: u64 = 30;

// --- Type aliases ---

type DiscoverEvent = Result<Change<IpAddr, TrackedHttpService>, crate::Error>;
type DiscoverStream = Pin<Box<UnboundedReceiverStream<DiscoverEvent>>>;
type P2cBalance = Balance<DiscoverStream, HttpRequest<PartitionKey>>;
type P2cFuture = <P2cBalance as tower::Service<HttpRequest<PartitionKey>>>::Future;
type BufferedBalance = Buffer<HttpRequest<PartitionKey>, P2cFuture>;

/// Configuration parameters needed to construct per-endpoint services.
#[derive(Clone)]
pub(super) struct EndpointServiceConfig {
    pub auth: Option<Auth>,
    pub skip_unknown_fields: Option<bool>,
    pub date_time_best_effort: bool,
    pub insert_random_shard: bool,
    pub compression: Compression,
    pub query_settings: QuerySettingsConfig,
}

/// Shared state between TrackedHttpService error handlers and the DNS refresh task.
struct SharedDiscoveryState {
    known_ips: Mutex<HashSet<IpAddr>>,
    active_count: AtomicUsize,
    refresh_notify: Notify,
}

/// Wraps an `HttpService` with load tracking and error-based endpoint removal.
///
/// Implements `Load` (returns in-flight request count) so that Tower's P2C
/// balancer can route to the least-loaded endpoint. On connection errors,
/// removes this endpoint from the discover channel so Balance stops routing
/// to it.
struct TrackedHttpService {
    inner: HttpService<ClickhouseServiceRequestBuilder, PartitionKey>,
    ip: IpAddr,
    pending: Arc<AtomicUsize>,
    discover_tx: mpsc::UnboundedSender<DiscoverEvent>,
    shared: Arc<SharedDiscoveryState>,
}

impl Load for TrackedHttpService {
    type Metric = usize;

    fn load(&self) -> Self::Metric {
        self.pending.load(Ordering::Relaxed)
    }
}

/// Guard that decrements the pending counter and removes the endpoint from the
/// active set when the request future is dropped without completing (e.g., when
/// Tower's timeout layer cancels the request).
///
/// When a pod is deleted, the TCP connection hangs until the OS-level SYN retry
/// timeout (~60-127s on Linux). Tower's request timeout fires first and drops
/// the future. Without this guard, the endpoint would never be removed and the
/// pending counter would leak.
struct RequestGuard {
    ip: IpAddr,
    pending: Arc<AtomicUsize>,
    discover_tx: mpsc::UnboundedSender<DiscoverEvent>,
    shared: Arc<SharedDiscoveryState>,
    completed: bool,
}

impl RequestGuard {
    /// Marks the request as completed so `Drop` won't remove the endpoint.
    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        self.pending.fetch_sub(1, Ordering::Relaxed);

        if !self.completed {
            // Future was cancelled (e.g., by Tower timeout) before HttpService
            // returned. Treat as endpoint failure — remove it so Balance stops
            // routing to this pod. DNS refresh will re-add it if it recovers.
            remove_endpoint(
                &self.shared,
                &self.discover_tx,
                self.ip,
                "request cancelled (likely timeout)",
            );
        }
    }
}

/// Removes an endpoint IP from the active set and notifies Balance via the
/// discover channel. Triggers an immediate DNS refresh if this was the last
/// active endpoint.
fn remove_endpoint(
    shared: &Arc<SharedDiscoveryState>,
    discover_tx: &mpsc::UnboundedSender<DiscoverEvent>,
    ip: IpAddr,
    reason: &str,
) {
    warn!(
        message = "Removing failed ClickHouse endpoint.",
        ip = %ip,
        reason = %reason,
    );

    let trigger_refresh = {
        let mut known = shared.known_ips.lock().unwrap_or_else(|e| e.into_inner());
        let removed = known.remove(&ip);
        if removed {
            let prev = shared.active_count.fetch_sub(1, Ordering::Relaxed);
            let _ = discover_tx.send(Ok(Change::Remove(ip)));
            prev == 1 // was the last endpoint
        } else {
            false
        }
    };

    if trigger_refresh {
        warn!(message = "All ClickHouse endpoints failed, triggering immediate DNS refresh.");
        shared.refresh_notify.notify_one();
    }
}

impl tower::Service<HttpRequest<PartitionKey>> for TrackedHttpService {
    type Response = HttpResponse;
    type Error = crate::Error;
    type Future = BoxFuture<'static, Result<HttpResponse, crate::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: HttpRequest<PartitionKey>) -> Self::Future {
        self.pending.fetch_add(1, Ordering::Relaxed);

        let mut guard = RequestGuard {
            ip: self.ip,
            pending: self.pending.clone(),
            discover_tx: self.discover_tx.clone(),
            shared: self.shared.clone(),
            completed: false,
        };

        let fut = self.inner.call(request);

        Box::pin(async move {
            let result = fut.await;

            // Mark as completed so the guard won't remove the endpoint on drop.
            guard.complete();

            if let Err(ref e) = result {
                if is_connection_error(e) {
                    remove_endpoint(&guard.shared, &guard.discover_tx, guard.ip, &e.to_string());
                }
            }

            result
        })
    }
}

/// A Tower service that load-balances HTTP requests across dynamically
/// resolved ClickHouse pod IPs using Tower's P2C (Power of Two Choices)
/// load balancer.
///
/// - Resolves a headless K8s service DNS name to pod IPs at startup
/// - Dispatches requests via P2C: picks two random endpoints and routes to
///   the one with fewer in-flight requests
/// - Removes endpoints on connection failures (connect/closed errors)
/// - Periodically re-resolves DNS in the background
///
/// The background DNS refresh task shuts down automatically when all clones
/// of this service are dropped (i.e., when the sink is torn down).
#[derive(Clone)]
pub struct HeadlessService {
    inner: BufferedBalance,
    fallback: HttpService<ClickhouseServiceRequestBuilder, PartitionKey>,
    shared: Arc<SharedDiscoveryState>,
    /// Tracks which service was polled in `poll_ready` so the matching one is
    /// used in `call`. `true` means the fallback was polled ready.
    using_fallback: bool,
    // Held to keep the background DNS refresh task alive. When all clones of
    // HeadlessService are dropped, this sender is dropped, signaling the
    // background task to exit.
    _shutdown_tx: Arc<watch::Sender<()>>,
}

impl HeadlessService {
    /// Creates a new `HeadlessService` by resolving the endpoint DNS name and
    /// spawning a background refresh task.
    pub async fn new(
        client: HttpClient,
        endpoint: Uri,
        svc_config: EndpointServiceConfig,
        dns_refresh_interval_secs: Option<u64>,
        fallback_uri: Uri,
    ) -> crate::Result<Self> {
        let initial_uris = dns::resolve_endpoints(&endpoint).await?;

        let (discover_tx, discover_rx) = mpsc::unbounded_channel::<DiscoverEvent>();

        let initial_ips: HashSet<IpAddr> = initial_uris
            .iter()
            .filter_map(|uri| dns::ip_from_uri(uri))
            .collect();

        if initial_ips.is_empty() {
            return Err("DNS resolution returned no usable IP addresses for ClickHouse".into());
        }

        let shared = Arc::new(SharedDiscoveryState {
            known_ips: Mutex::new(initial_ips.clone()),
            active_count: AtomicUsize::new(initial_ips.len()),
            refresh_notify: Notify::new(),
        });

        // Build TrackedHttpService for each initial IP and send Insert events.
        for uri in &initial_uris {
            if let Some(ip) = dns::ip_from_uri(uri) {
                if initial_ips.contains(&ip) {
                    let service = TrackedHttpService {
                        inner: build_endpoint_service(&client, uri.clone(), &svc_config),
                        ip,
                        pending: Arc::new(AtomicUsize::new(0)),
                        discover_tx: discover_tx.clone(),
                        shared: shared.clone(),
                    };
                    let _ = discover_tx.send(Ok(Change::Insert(ip, service)));
                }
            }
        }

        info!(
            message = "HeadlessService initialized for ClickHouse.",
            endpoint = %endpoint,
            active_endpoints = initial_ips.len(),
        );

        let fallback = build_endpoint_service(&client, fallback_uri.clone(), &svc_config);
        info!(
            message = "HeadlessService fallback endpoint configured.",
            fallback_endpoint = %fallback_uri,
        );

        let discover_stream: DiscoverStream = Box::pin(UnboundedReceiverStream::new(discover_rx));
        let balance = Balance::new(discover_stream);
        let buffer = Buffer::new(balance, 1);

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let refresh_secs = dns_refresh_interval_secs.unwrap_or(DEFAULT_DNS_REFRESH_SECS);

        spawn_dns_refresh_task(
            shared.clone(),
            discover_tx,
            client,
            endpoint,
            svc_config,
            Duration::from_secs(refresh_secs),
            shutdown_rx,
        );

        Ok(Self {
            inner: buffer,
            fallback,
            shared,
            using_fallback: false,
            _shutdown_tx: Arc::new(shutdown_tx),
        })
    }
}

impl tower::Service<HttpRequest<PartitionKey>> for HeadlessService {
    type Response = HttpResponse;
    type Error = crate::Error;
    type Future = BoxFuture<'static, Result<HttpResponse, crate::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.shared.active_count.load(Ordering::Relaxed) == 0 {
            self.using_fallback = true;
            self.fallback.poll_ready(cx)
        } else {
            self.using_fallback = false;
            self.inner.poll_ready(cx).map_err(Into::into)
        }
    }

    fn call(&mut self, request: HttpRequest<PartitionKey>) -> Self::Future {
        if self.using_fallback {
            warn!(
                message =
                    "All headless endpoints unavailable, routing to fallback ClusterIP service."
            );
            self.fallback.call(request)
        } else {
            let fut = self.inner.call(request);
            Box::pin(async move { fut.await.map_err(Into::into) })
        }
    }
}

/// Returns true if the error is a connection-level failure, as opposed to an
/// HTTP-level or parse error. Covers:
/// - `is_connect()`: TCP handshake failures
/// - `is_closed()`: connection pool channel closed
/// - `is_incomplete_message()`: connection dropped mid-message
/// - `is_timeout()`: connection timed out
/// - IO errors with connection-related kinds (ConnectionReset, BrokenPipe, etc.)
///
/// Note: hyper 0.14 wraps mid-stream IO errors (like "Connection reset by peer")
/// as `Kind::Io` which has no public checker method, so we walk the source chain
/// to find the underlying `std::io::Error`.
fn is_connection_error(error: &crate::Error) -> bool {
    error.downcast_ref::<HttpError>().is_some_and(|e| match e {
        HttpError::CallRequest { source } => {
            source.is_connect()
                || source.is_closed()
                || source.is_incomplete_message()
                || source.is_timeout()
                || has_io_connection_error(source as &dyn std::error::Error)
        }
        _ => false,
    })
}

/// Walks the error and its source chain looking for an `io::Error` with a
/// connection-related kind. This catches hyper's `Kind::Io` errors that have
/// no public checker method (e.g., "Connection reset by peer").
fn has_io_connection_error(error: &(dyn std::error::Error + 'static)) -> bool {
    // Check the error itself first, then walk the source chain.
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(err) = current {
        if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
            return matches!(
                io_err.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::BrokenPipe
            );
        }
        current = err.source();
    }
    false
}

/// Builds an `HttpService` targeting a single resolved endpoint URI.
fn build_endpoint_service(
    client: &HttpClient,
    uri: Uri,
    config: &EndpointServiceConfig,
) -> HttpService<ClickhouseServiceRequestBuilder, PartitionKey> {
    let request_builder = ClickhouseServiceRequestBuilder {
        auth: config.auth.clone(),
        endpoint: uri,
        skip_unknown_fields: config.skip_unknown_fields,
        date_time_best_effort: config.date_time_best_effort,
        insert_random_shard: config.insert_random_shard,
        compression: config.compression,
        query_settings: config.query_settings,
    };
    HttpService::new(client.clone(), request_builder)
}

/// Spawns a background task that periodically re-resolves DNS and reconciles
/// the active endpoint set via the discover channel.
///
/// The task exits when `shutdown_rx` detects that the sender has been dropped
/// (i.e., when all `HeadlessService` clones are dropped during sink teardown).
fn spawn_dns_refresh_task(
    shared: Arc<SharedDiscoveryState>,
    discover_tx: mpsc::UnboundedSender<DiscoverEvent>,
    client: HttpClient,
    endpoint: Uri,
    svc_config: EndpointServiceConfig,
    refresh_interval: Duration,
    mut shutdown_rx: watch::Receiver<()>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(refresh_interval);
        // Skip the first tick which fires immediately.
        interval.tick().await;

        loop {
            tokio::select! {
                _ = interval.tick() => {},
                _ = shared.refresh_notify.notified() => {
                    interval.reset();
                },
                _ = shutdown_rx.changed() => break,
            }

            match dns::resolve_endpoints(&endpoint).await {
                Ok(new_uris) => {
                    reconcile_endpoints(&shared, &discover_tx, &client, &svc_config, &new_uris);
                }
                Err(e) => {
                    warn!(
                        message = "DNS refresh failed for ClickHouse headless service.",
                        error = %e,
                    );
                }
            }
        }

        info!(message = "ClickHouse headless DNS refresh task shutting down.");
    });
}

/// Reconciles the active endpoint set with freshly resolved URIs by sending
/// `Change::Insert` and `Change::Remove` events to the discover channel.
fn reconcile_endpoints(
    shared: &Arc<SharedDiscoveryState>,
    discover_tx: &mpsc::UnboundedSender<DiscoverEvent>,
    client: &HttpClient,
    svc_config: &EndpointServiceConfig,
    new_uris: &[Uri],
) {
    let new_ips: HashSet<IpAddr> = new_uris
        .iter()
        .filter_map(|u| dns::ip_from_uri(u))
        .collect();

    let mut known = shared.known_ips.lock().unwrap_or_else(|e| e.into_inner());

    // Remove endpoints no longer in DNS.
    let stale_ips: Vec<IpAddr> = known.difference(&new_ips).copied().collect();
    let removed_count = stale_ips.len();
    for ip in &stale_ips {
        known.remove(ip);
        shared.active_count.fetch_sub(1, Ordering::Relaxed);
        let _ = discover_tx.send(Ok(Change::Remove(*ip)));
    }

    // Add new endpoints.
    let mut added_count = 0;
    for uri in new_uris {
        if let Some(ip) = dns::ip_from_uri(uri) {
            if !known.contains(&ip) {
                let service = TrackedHttpService {
                    inner: build_endpoint_service(client, uri.clone(), svc_config),
                    ip,
                    pending: Arc::new(AtomicUsize::new(0)),
                    discover_tx: discover_tx.clone(),
                    shared: shared.clone(),
                };
                let _ = discover_tx.send(Ok(Change::Insert(ip, service)));
                known.insert(ip);
                shared.active_count.fetch_add(1, Ordering::Relaxed);
                added_count += 1;
            }
        }
    }

    if removed_count > 0 || added_count > 0 {
        info!(
            message = "ClickHouse headless service endpoints reconciled.",
            added = added_count,
            removed = removed_count,
            active = known.len(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_connection_error_with_string_error() {
        let err: crate::Error = "some application error".into();
        assert!(!is_connection_error(&err));
    }

    #[test]
    fn test_is_connection_error_with_io_error() {
        // A bare io::Error (not wrapped in HttpError) should NOT match.
        let err: crate::Error = Box::new(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "connection refused",
        ));
        assert!(!is_connection_error(&err));
    }

    #[test]
    fn test_has_io_connection_error_connection_reset() {
        let io_err = std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "Connection reset by peer (os error 104)",
        );
        assert!(has_io_connection_error(&io_err));
    }

    #[test]
    fn test_has_io_connection_error_broken_pipe() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken pipe");
        assert!(has_io_connection_error(&io_err));
    }

    #[test]
    fn test_has_io_connection_error_other_kind() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "permission denied");
        assert!(!has_io_connection_error(&io_err));
    }

    #[test]
    fn test_reconcile_adds_new_endpoints() {
        let client = make_test_client();
        let config = make_test_config();
        let (discover_tx, mut discover_rx) = mpsc::unbounded_channel::<DiscoverEvent>();

        let shared = Arc::new(SharedDiscoveryState {
            known_ips: Mutex::new(HashSet::new()),
            active_count: AtomicUsize::new(0),
            refresh_notify: Notify::new(),
        });

        let uris: Vec<Uri> = vec![
            "http://10.0.0.1:8123/".parse().unwrap(),
            "http://10.0.0.2:8123/".parse().unwrap(),
        ];

        reconcile_endpoints(&shared, &discover_tx, &client, &config, &uris);

        // Verify shared state.
        let known = shared.known_ips.lock().unwrap();
        assert_eq!(known.len(), 2);
        assert!(known.contains(&"10.0.0.1".parse::<IpAddr>().unwrap()));
        assert!(known.contains(&"10.0.0.2".parse::<IpAddr>().unwrap()));
        drop(known);
        assert_eq!(shared.active_count.load(Ordering::Relaxed), 2);

        // Verify discover channel events.
        let mut inserted_ips = HashSet::new();
        while let Ok(event) = discover_rx.try_recv() {
            if let Ok(Change::Insert(ip, _)) = event {
                inserted_ips.insert(ip);
            }
        }
        assert_eq!(inserted_ips.len(), 2);
    }

    #[test]
    fn test_reconcile_removes_stale_endpoints() {
        let client = make_test_client();
        let config = make_test_config();
        let (discover_tx, mut discover_rx) = mpsc::unbounded_channel::<DiscoverEvent>();

        let initial_ips: HashSet<IpAddr> = vec![
            "10.0.0.1".parse().unwrap(),
            "10.0.0.2".parse().unwrap(),
            "10.0.0.3".parse().unwrap(),
        ]
        .into_iter()
        .collect();

        let shared = Arc::new(SharedDiscoveryState {
            known_ips: Mutex::new(initial_ips),
            active_count: AtomicUsize::new(3),
            refresh_notify: Notify::new(),
        });

        // DNS now only returns 2 of the 3 original IPs.
        let new_uris: Vec<Uri> = vec![
            "http://10.0.0.1:8123/".parse().unwrap(),
            "http://10.0.0.3:8123/".parse().unwrap(),
        ];

        reconcile_endpoints(&shared, &discover_tx, &client, &config, &new_uris);

        let known = shared.known_ips.lock().unwrap();
        assert_eq!(known.len(), 2);
        assert!(known.contains(&"10.0.0.1".parse::<IpAddr>().unwrap()));
        assert!(!known.contains(&"10.0.0.2".parse::<IpAddr>().unwrap()));
        assert!(known.contains(&"10.0.0.3".parse::<IpAddr>().unwrap()));
        drop(known);
        assert_eq!(shared.active_count.load(Ordering::Relaxed), 2);

        // Verify a Remove event was sent for 10.0.0.2.
        let mut removed_ips = HashSet::new();
        while let Ok(event) = discover_rx.try_recv() {
            if let Ok(Change::Remove(ip)) = event {
                removed_ips.insert(ip);
            }
        }
        assert!(removed_ips.contains(&"10.0.0.2".parse::<IpAddr>().unwrap()));
    }

    #[test]
    fn test_reconcile_preserves_existing() {
        let client = make_test_client();
        let config = make_test_config();
        let (discover_tx, mut discover_rx) = mpsc::unbounded_channel::<DiscoverEvent>();

        let initial_ips: HashSet<IpAddr> =
            vec!["10.0.0.1".parse().unwrap(), "10.0.0.2".parse().unwrap()]
                .into_iter()
                .collect();

        let shared = Arc::new(SharedDiscoveryState {
            known_ips: Mutex::new(initial_ips),
            active_count: AtomicUsize::new(2),
            refresh_notify: Notify::new(),
        });

        // DNS returns the same IPs plus a new one.
        let new_uris: Vec<Uri> = vec![
            "http://10.0.0.1:8123/".parse().unwrap(),
            "http://10.0.0.2:8123/".parse().unwrap(),
            "http://10.0.0.3:8123/".parse().unwrap(),
        ];

        reconcile_endpoints(&shared, &discover_tx, &client, &config, &new_uris);

        let known = shared.known_ips.lock().unwrap();
        assert_eq!(known.len(), 3);
        drop(known);
        assert_eq!(shared.active_count.load(Ordering::Relaxed), 3);

        // Only one Insert event (for 10.0.0.3), no Remove events.
        let mut inserted_ips = HashSet::new();
        let mut removed_ips = HashSet::new();
        while let Ok(event) = discover_rx.try_recv() {
            match event {
                Ok(Change::Insert(ip, _)) => {
                    inserted_ips.insert(ip);
                }
                Ok(Change::Remove(ip)) => {
                    removed_ips.insert(ip);
                }
                _ => {}
            }
        }
        assert_eq!(inserted_ips.len(), 1);
        assert!(inserted_ips.contains(&"10.0.0.3".parse::<IpAddr>().unwrap()));
        assert!(removed_ips.is_empty());
    }

    fn make_test_client() -> HttpClient {
        use crate::tls::TlsSettings;
        HttpClient::new(TlsSettings::default(), &Default::default()).unwrap()
    }

    fn make_test_config() -> EndpointServiceConfig {
        EndpointServiceConfig {
            auth: None,
            skip_unknown_fields: None,
            date_time_best_effort: false,
            insert_random_shard: false,
            compression: Compression::default(),
            query_settings: QuerySettingsConfig::default(),
        }
    }
}
