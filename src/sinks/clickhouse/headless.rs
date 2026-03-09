//! Headless Kubernetes service support for the ClickHouse sink.
//!
//! When `use_headless_service` is enabled, the configured endpoint hostname is
//! resolved via DNS to discover individual pod IPs. Requests are dispatched
//! round-robin across all resolved endpoints.
//!
//! Failed endpoints (connection errors) are removed from the active set.
//! A background task periodically re-resolves DNS to discover new pods and
//! re-add recovered ones. If all endpoints are removed, an immediate DNS
//! refresh is triggered.

use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use http::Uri;
use tokio::sync::Notify;

use super::config::QuerySettingsConfig;
use super::dns;
use super::service::ClickhouseServiceRequestBuilder;
use super::sink::PartitionKey;
use crate::http::{Auth, HttpClient, HttpError};
use crate::sinks::prelude::*;
use crate::sinks::util::http::{HttpRequest, HttpResponse, HttpService};

/// Default DNS refresh interval.
const DEFAULT_DNS_REFRESH_SECS: u64 = 30;

/// Configuration parameters needed to construct per-endpoint services.
#[derive(Clone)]
struct EndpointServiceConfig {
    auth: Option<Auth>,
    skip_unknown_fields: Option<bool>,
    date_time_best_effort: bool,
    insert_random_shard: bool,
    compression: Compression,
    query_settings: QuerySettingsConfig,
}

struct EndpointEntry {
    ip: IpAddr,
    service: HttpService<ClickhouseServiceRequestBuilder, PartitionKey>,
}

struct SharedState {
    entries: Vec<EndpointEntry>,
}

/// A Tower service that round-robins HTTP requests across dynamically
/// resolved ClickHouse pod IPs.
///
/// - Resolves a headless K8s service DNS name to pod IPs at startup
/// - Dispatches requests round-robin across active endpoints
/// - Removes endpoints on connection failures
/// - Triggers immediate DNS re-resolution when all endpoints are exhausted
/// - Periodically re-resolves DNS in the background
#[derive(Clone)]
pub struct HeadlessService {
    state: Arc<Mutex<SharedState>>,
    next: Arc<AtomicUsize>,
    refresh_notify: Arc<Notify>,
}

impl HeadlessService {
    /// Creates a new `HeadlessService` by resolving the endpoint DNS name and
    /// spawning a background refresh task.
    pub async fn new(
        client: HttpClient,
        endpoint: Uri,
        auth: Option<Auth>,
        skip_unknown_fields: Option<bool>,
        date_time_best_effort: bool,
        insert_random_shard: bool,
        compression: Compression,
        query_settings: QuerySettingsConfig,
        dns_refresh_interval_secs: Option<u64>,
    ) -> crate::Result<Self> {
        info!(
            message = "HeadlessService::new() starting initialization.",
            endpoint = %endpoint,
            host = ?endpoint.host(),
            has_auth = %auth.is_some(),
            skip_unknown_fields = ?skip_unknown_fields,
            date_time_best_effort = %date_time_best_effort,
            insert_random_shard = %insert_random_shard,
            compression = ?compression,
            dns_refresh_interval_secs = ?dns_refresh_interval_secs,
        );

        let svc_config = EndpointServiceConfig {
            auth,
            skip_unknown_fields,
            date_time_best_effort,
            insert_random_shard,
            compression,
            query_settings,
        };
        info!(message = "HeadlessService::new(): endpoint service config created.");

        info!(
            message = "HeadlessService::new(): performing initial DNS resolution.",
            endpoint = %endpoint,
        );
        let initial_uris = dns::resolve_endpoints(&endpoint).await?;
        info!(
            message = "HeadlessService::new(): initial DNS resolution completed.",
            resolved_uri_count = %initial_uris.len(),
            resolved_uris = ?initial_uris.iter().map(|u| u.to_string()).collect::<Vec<_>>(),
        );

        info!(message = "HeadlessService::new(): building endpoint entries from resolved URIs.");
        let entries: Vec<EndpointEntry> = initial_uris
            .iter()
            .filter_map(|uri| {
                let ip = dns::ip_from_uri(uri)?;
                info!(
                    message = "HeadlessService::new(): creating endpoint entry.",
                    uri = %uri,
                    ip = %ip,
                );
                let service = build_endpoint_service(&client, uri.clone(), &svc_config);
                Some(EndpointEntry { ip, service })
            })
            .collect();

        if entries.is_empty() {
            info!(
                message = "HeadlessService::new(): FAILED - no usable IP addresses from DNS.",
                endpoint = %endpoint,
            );
            return Err("DNS resolution returned no usable IP addresses for ClickHouse".into());
        }

        info!(
            message = "HeadlessService::new(): endpoint entries created successfully.",
            endpoint = %endpoint,
            active_endpoints = %entries.len(),
            endpoint_ips = ?entries.iter().map(|e| e.ip.to_string()).collect::<Vec<_>>(),
        );

        let state = Arc::new(Mutex::new(SharedState { entries }));
        let refresh_notify = Arc::new(Notify::new());

        let refresh_secs = dns_refresh_interval_secs.unwrap_or(DEFAULT_DNS_REFRESH_SECS);
        info!(
            message = "HeadlessService::new(): spawning background DNS refresh task.",
            refresh_interval_secs = %refresh_secs,
        );
        spawn_dns_refresh_task(
            state.clone(),
            refresh_notify.clone(),
            client,
            endpoint.clone(),
            svc_config,
            Duration::from_secs(refresh_secs),
        );

        info!(
            message = "HeadlessService::new(): INITIALIZATION COMPLETE.",
            endpoint = %endpoint,
        );

        Ok(Self {
            state,
            next: Arc::new(AtomicUsize::new(0)),
            refresh_notify,
        })
    }
}

impl Service<HttpRequest<PartitionKey>> for HeadlessService {
    type Response = HttpResponse;
    type Error = crate::Error;
    type Future = BoxFuture<'static, Result<HttpResponse, crate::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: HttpRequest<PartitionKey>) -> Self::Future {
        let state = self.state.clone();
        let notify = self.refresh_notify.clone();
        let idx = self.next.fetch_add(1, Ordering::Relaxed);

        info!(
            message = "HeadlessService::call(): dispatching request via round-robin.",
            request_index = %idx,
        );

        // Pick the next service via round-robin.
        let pick = {
            let guard = state.lock().unwrap_or_else(|e| e.into_inner());
            let endpoint_count = guard.entries.len();
            info!(
                message = "HeadlessService::call(): selecting endpoint.",
                available_endpoints = %endpoint_count,
                round_robin_index = %idx,
            );
            if guard.entries.is_empty() {
                info!(message = "HeadlessService::call(): NO ENDPOINTS AVAILABLE.");
                None
            } else {
                let selected_idx = idx % endpoint_count;
                let entry = &guard.entries[selected_idx];
                info!(
                    message = "HeadlessService::call(): endpoint selected.",
                    selected_endpoint_index = %selected_idx,
                    selected_ip = %entry.ip,
                    all_available_ips = ?guard.entries.iter().map(|e| e.ip.to_string()).collect::<Vec<_>>(),
                );
                Some((entry.service.clone(), entry.ip))
            }
        };

        let Some((mut service, ip)) = pick else {
            // No endpoints available — the background task should be resolving.
            info!(
                message = "HeadlessService::call(): no endpoints, triggering DNS refresh.",
            );
            notify.notify_one();
            return Box::pin(async {
                Err("No available ClickHouse endpoints (DNS refresh pending)".into())
            });
        };

        let request_ip = ip;
        Box::pin(async move {
            info!(
                message = "HeadlessService::call(): sending request to endpoint.",
                target_ip = %request_ip,
            );

            let result = service.call(request).await;

            match &result {
                Ok(response) => {
                    info!(
                        message = "HeadlessService::call(): request SUCCESS.",
                        target_ip = %request_ip,
                        status = %response.http_response.status(),
                    );
                }
                Err(ref e) => {
                    info!(
                        message = "HeadlessService::call(): request FAILED.",
                        target_ip = %request_ip,
                        error = %e,
                        is_connection_error = %is_connection_error(e),
                    );

                    if is_connection_error(e) {
                        warn!(
                            message = "HeadlessService::call(): REMOVING failed ClickHouse endpoint due to connection error.",
                            ip = %request_ip,
                            error = %e,
                        );
                        let trigger_refresh = {
                            let mut guard = state.lock().unwrap_or_else(|e| e.into_inner());
                            let before_count = guard.entries.len();
                            guard.entries.retain(|entry| entry.ip != request_ip);
                            let after_count = guard.entries.len();
                            info!(
                                message = "HeadlessService::call(): endpoint removed from pool.",
                                removed_ip = %request_ip,
                                endpoints_before = %before_count,
                                endpoints_after = %after_count,
                                remaining_ips = ?guard.entries.iter().map(|e| e.ip.to_string()).collect::<Vec<_>>(),
                            );
                            guard.entries.is_empty()
                        };
                        if trigger_refresh {
                            warn!(
                                message = "HeadlessService::call(): ALL ClickHouse endpoints failed, triggering IMMEDIATE DNS refresh."
                            );
                            notify.notify_one();
                        }
                    }
                }
            }

            result
        })
    }
}

/// Returns true if the error is a connection-level failure (as opposed to
/// an application-level HTTP error).
fn is_connection_error(error: &crate::Error) -> bool {
    error
        .downcast_ref::<HttpError>()
        .is_some_and(|e| matches!(e, HttpError::CallRequest { .. }))
}

/// Builds an `HttpService` targeting a single resolved endpoint URI.
fn build_endpoint_service(
    client: &HttpClient,
    uri: Uri,
    config: &EndpointServiceConfig,
) -> HttpService<ClickhouseServiceRequestBuilder, PartitionKey> {
    info!(
        message = "build_endpoint_service(): creating HTTP service for endpoint.",
        uri = %uri,
        host = ?uri.host(),
        port = ?uri.port_u16(),
        has_auth = %config.auth.is_some(),
        skip_unknown_fields = ?config.skip_unknown_fields,
        date_time_best_effort = %config.date_time_best_effort,
        insert_random_shard = %config.insert_random_shard,
    );

    let request_builder = ClickhouseServiceRequestBuilder {
        auth: config.auth.clone(),
        endpoint: uri.clone(),
        skip_unknown_fields: config.skip_unknown_fields,
        date_time_best_effort: config.date_time_best_effort,
        insert_random_shard: config.insert_random_shard,
        compression: config.compression,
        query_settings: config.query_settings,
    };

    info!(
        message = "build_endpoint_service(): HTTP service created successfully.",
        uri = %uri,
    );

    HttpService::new(client.clone(), request_builder)
}

/// Spawns a background task that periodically re-resolves DNS and reconciles
/// the active endpoint set. Also listens for immediate-refresh notifications.
fn spawn_dns_refresh_task(
    state: Arc<Mutex<SharedState>>,
    notify: Arc<Notify>,
    client: HttpClient,
    endpoint: Uri,
    svc_config: EndpointServiceConfig,
    refresh_interval: Duration,
) {
    info!(
        message = "spawn_dns_refresh_task(): spawning background DNS refresh task.",
        endpoint = %endpoint,
        refresh_interval_secs = %refresh_interval.as_secs(),
    );

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(refresh_interval);
        // Skip the first tick which fires immediately.
        interval.tick().await;

        info!(
            message = "DNS refresh task: started and waiting for first interval.",
            endpoint = %endpoint,
            refresh_interval_secs = %refresh_interval.as_secs(),
        );

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    info!(
                        message = "DNS refresh task: periodic refresh triggered.",
                        endpoint = %endpoint,
                    );
                },
                _ = notify.notified() => {
                    info!(
                        message = "DNS refresh task: IMMEDIATE refresh triggered (notified).",
                        endpoint = %endpoint,
                    );
                    interval.reset();
                },
            }

            info!(
                message = "DNS refresh task: performing DNS resolution.",
                endpoint = %endpoint,
            );

            match dns::resolve_endpoints(&endpoint).await {
                Ok(new_uris) => {
                    info!(
                        message = "DNS refresh task: DNS resolution succeeded.",
                        endpoint = %endpoint,
                        resolved_count = %new_uris.len(),
                        resolved_uris = ?new_uris.iter().map(|u| u.to_string()).collect::<Vec<_>>(),
                    );
                    reconcile_endpoints(&state, &client, &svc_config, &new_uris);
                }
                Err(e) => {
                    warn!(
                        message = "DNS refresh task: DNS resolution FAILED.",
                        endpoint = %endpoint,
                        error = %e,
                    );
                }
            }
        }
    });
}

/// Reconciles the active endpoint set with freshly resolved URIs.
/// - Removes entries whose IPs are no longer in DNS
/// - Adds new entries for IPs that appeared in DNS
/// - Preserves existing entries (and their connection state) for unchanged IPs
fn reconcile_endpoints(
    state: &Arc<Mutex<SharedState>>,
    client: &HttpClient,
    svc_config: &EndpointServiceConfig,
    new_uris: &[Uri],
) {
    info!(
        message = "reconcile_endpoints(): starting endpoint reconciliation.",
        new_uri_count = %new_uris.len(),
        new_uris = ?new_uris.iter().map(|u| u.to_string()).collect::<Vec<_>>(),
    );

    let new_ips: HashSet<IpAddr> = new_uris
        .iter()
        .filter_map(|u| dns::ip_from_uri(u))
        .collect();

    info!(
        message = "reconcile_endpoints(): extracted IPs from new URIs.",
        new_ip_count = %new_ips.len(),
        new_ips = ?new_ips.iter().map(|ip| ip.to_string()).collect::<Vec<_>>(),
    );

    let mut guard = state.lock().unwrap_or_else(|e| e.into_inner());
    let current_ips: HashSet<IpAddr> = guard.entries.iter().map(|e| e.ip).collect();

    info!(
        message = "reconcile_endpoints(): current endpoint state.",
        current_endpoint_count = %guard.entries.len(),
        current_ips = ?current_ips.iter().map(|ip| ip.to_string()).collect::<Vec<_>>(),
    );

    // Remove endpoints no longer in DNS.
    let before_remove = guard.entries.len();
    let ips_to_remove: Vec<IpAddr> = current_ips
        .iter()
        .filter(|ip| !new_ips.contains(ip))
        .copied()
        .collect();
    if !ips_to_remove.is_empty() {
        info!(
            message = "reconcile_endpoints(): removing stale endpoints.",
            ips_to_remove = ?ips_to_remove.iter().map(|ip| ip.to_string()).collect::<Vec<_>>(),
        );
    }
    guard.entries.retain(|entry| new_ips.contains(&entry.ip));
    let removed_count = before_remove - guard.entries.len();

    // Add new endpoints.
    let mut added_count = 0;
    let ips_to_add: Vec<IpAddr> = new_ips
        .iter()
        .filter(|ip| !current_ips.contains(ip))
        .copied()
        .collect();
    if !ips_to_add.is_empty() {
        info!(
            message = "reconcile_endpoints(): adding new endpoints.",
            ips_to_add = ?ips_to_add.iter().map(|ip| ip.to_string()).collect::<Vec<_>>(),
        );
    }

    for uri in new_uris {
        if let Some(ip) = dns::ip_from_uri(uri) {
            if !current_ips.contains(&ip) {
                info!(
                    message = "reconcile_endpoints(): creating service for new endpoint.",
                    ip = %ip,
                    uri = %uri,
                );
                let service = build_endpoint_service(client, uri.clone(), svc_config);
                guard.entries.push(EndpointEntry { ip, service });
                added_count += 1;
            }
        }
    }

    info!(
        message = "reconcile_endpoints(): RECONCILIATION COMPLETE.",
        added = %added_count,
        removed = %removed_count,
        active_endpoints = %guard.entries.len(),
        active_ips = ?guard.entries.iter().map(|e| e.ip.to_string()).collect::<Vec<_>>(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_connection_error_with_hyper_error() {
        // We can't easily construct a real HttpError::CallRequest in tests,
        // so we test the negative case — non-connection errors.
        let err: crate::Error = "some application error".into();
        assert!(!is_connection_error(&err));
    }

    #[test]
    fn test_is_connection_error_with_string_error() {
        let err: crate::Error = Box::new(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "connection refused",
        ));
        assert!(!is_connection_error(&err));
    }

    #[test]
    fn test_reconcile_adds_new_endpoints() {
        let client = make_test_client();
        let config = make_test_config();
        let state = Arc::new(Mutex::new(SharedState {
            entries: Vec::new(),
        }));

        let uris: Vec<Uri> = vec![
            "http://10.0.0.1:8123/".parse().unwrap(),
            "http://10.0.0.2:8123/".parse().unwrap(),
        ];

        reconcile_endpoints(&state, &client, &config, &uris);

        let guard = state.lock().unwrap();
        assert_eq!(guard.entries.len(), 2);
        let ips: HashSet<IpAddr> = guard.entries.iter().map(|e| e.ip).collect();
        assert!(ips.contains(&"10.0.0.1".parse::<IpAddr>().unwrap()));
        assert!(ips.contains(&"10.0.0.2".parse::<IpAddr>().unwrap()));
    }

    #[test]
    fn test_reconcile_removes_stale_endpoints() {
        let client = make_test_client();
        let config = make_test_config();

        let initial_uris: Vec<Uri> = vec![
            "http://10.0.0.1:8123/".parse().unwrap(),
            "http://10.0.0.2:8123/".parse().unwrap(),
            "http://10.0.0.3:8123/".parse().unwrap(),
        ];

        let entries: Vec<EndpointEntry> = initial_uris
            .iter()
            .filter_map(|uri| {
                let ip = dns::ip_from_uri(uri)?;
                let service = build_endpoint_service(&client, uri.clone(), &config);
                Some(EndpointEntry { ip, service })
            })
            .collect();

        let state = Arc::new(Mutex::new(SharedState { entries }));

        // DNS now only returns 2 of the 3 original IPs.
        let new_uris: Vec<Uri> = vec![
            "http://10.0.0.1:8123/".parse().unwrap(),
            "http://10.0.0.3:8123/".parse().unwrap(),
        ];

        reconcile_endpoints(&state, &client, &config, &new_uris);

        let guard = state.lock().unwrap();
        assert_eq!(guard.entries.len(), 2);
        let ips: HashSet<IpAddr> = guard.entries.iter().map(|e| e.ip).collect();
        assert!(ips.contains(&"10.0.0.1".parse::<IpAddr>().unwrap()));
        assert!(!ips.contains(&"10.0.0.2".parse::<IpAddr>().unwrap()));
        assert!(ips.contains(&"10.0.0.3".parse::<IpAddr>().unwrap()));
    }

    #[test]
    fn test_reconcile_preserves_existing() {
        let client = make_test_client();
        let config = make_test_config();

        let initial_uris: Vec<Uri> = vec![
            "http://10.0.0.1:8123/".parse().unwrap(),
            "http://10.0.0.2:8123/".parse().unwrap(),
        ];

        let entries: Vec<EndpointEntry> = initial_uris
            .iter()
            .filter_map(|uri| {
                let ip = dns::ip_from_uri(uri)?;
                let service = build_endpoint_service(&client, uri.clone(), &config);
                Some(EndpointEntry { ip, service })
            })
            .collect();

        let state = Arc::new(Mutex::new(SharedState { entries }));

        // DNS returns the same IPs plus a new one.
        let new_uris: Vec<Uri> = vec![
            "http://10.0.0.1:8123/".parse().unwrap(),
            "http://10.0.0.2:8123/".parse().unwrap(),
            "http://10.0.0.3:8123/".parse().unwrap(),
        ];

        reconcile_endpoints(&state, &client, &config, &new_uris);

        let guard = state.lock().unwrap();
        assert_eq!(guard.entries.len(), 3);
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
