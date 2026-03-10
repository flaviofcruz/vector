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
        let svc_config = EndpointServiceConfig {
            auth,
            skip_unknown_fields,
            date_time_best_effort,
            insert_random_shard,
            compression,
            query_settings,
        };

        let initial_uris = dns::resolve_endpoints(&endpoint).await?;

        let entries: Vec<EndpointEntry> = initial_uris
            .iter()
            .filter_map(|uri| {
                let ip = dns::ip_from_uri(uri)?;
                let service = build_endpoint_service(&client, uri.clone(), &svc_config);
                Some(EndpointEntry { ip, service })
            })
            .collect();

        if entries.is_empty() {
            return Err("DNS resolution returned no usable IP addresses for ClickHouse".into());
        }

        info!(
            message = "HeadlessService initialized for ClickHouse.",
            endpoint = %endpoint,
            active_endpoints = entries.len(),
        );

        let state = Arc::new(Mutex::new(SharedState { entries }));
        let refresh_notify = Arc::new(Notify::new());

        let refresh_secs = dns_refresh_interval_secs.unwrap_or(DEFAULT_DNS_REFRESH_SECS);
        spawn_dns_refresh_task(
            state.clone(),
            refresh_notify.clone(),
            client,
            endpoint,
            svc_config,
            Duration::from_secs(refresh_secs),
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

        // Pick the next service via round-robin.
        let pick = {
            let guard = state.lock().unwrap_or_else(|e| e.into_inner());
            if guard.entries.is_empty() {
                None
            } else {
                let entry = &guard.entries[idx % guard.entries.len()];
                Some((entry.service.clone(), entry.ip))
            }
        };

        let Some((mut service, ip)) = pick else {
            // No endpoints available — the background task should be resolving.
            notify.notify_one();
            return Box::pin(async {
                Err("No available ClickHouse endpoints (DNS refresh pending)".into())
            });
        };

        Box::pin(async move {
            let result = service.call(request).await;

            if let Err(ref e) = result {
                if is_connection_error(e) {
                    warn!(
                        message = "Removing failed ClickHouse endpoint.",
                        ip = %ip,
                        error = %e,
                    );
                    let trigger_refresh = {
                        let mut guard = state.lock().unwrap_or_else(|e| e.into_inner());
                        guard.entries.retain(|entry| entry.ip != ip);
                        guard.entries.is_empty()
                    };
                    if trigger_refresh {
                        warn!(
                            message = "All ClickHouse endpoints failed, triggering immediate DNS refresh."
                        );
                        notify.notify_one();
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
/// the active endpoint set. Also listens for immediate-refresh notifications.
fn spawn_dns_refresh_task(
    state: Arc<Mutex<SharedState>>,
    notify: Arc<Notify>,
    client: HttpClient,
    endpoint: Uri,
    svc_config: EndpointServiceConfig,
    refresh_interval: Duration,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(refresh_interval);
        // Skip the first tick which fires immediately.
        interval.tick().await;

        loop {
            tokio::select! {
                _ = interval.tick() => {},
                _ = notify.notified() => {
                    interval.reset();
                },
            }

            match dns::resolve_endpoints(&endpoint).await {
                Ok(new_uris) => {
                    reconcile_endpoints(&state, &client, &svc_config, &new_uris);
                }
                Err(e) => {
                    warn!(
                        message = "DNS refresh failed for ClickHouse headless service.",
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
    let new_ips: HashSet<IpAddr> = new_uris
        .iter()
        .filter_map(|u| dns::ip_from_uri(u))
        .collect();

    let mut guard = state.lock().unwrap_or_else(|e| e.into_inner());
    let current_ips: HashSet<IpAddr> = guard.entries.iter().map(|e| e.ip).collect();

    // Remove endpoints no longer in DNS.
    let removed_count = guard.entries.len();
    guard.entries.retain(|entry| new_ips.contains(&entry.ip));
    let removed_count = removed_count - guard.entries.len();

    // Add new endpoints.
    let mut added_count = 0;
    for uri in new_uris {
        if let Some(ip) = dns::ip_from_uri(uri) {
            if !current_ips.contains(&ip) {
                let service = build_endpoint_service(client, uri.clone(), svc_config);
                guard.entries.push(EndpointEntry { ip, service });
                added_count += 1;
            }
        }
    }

    if removed_count > 0 || added_count > 0 {
        info!(
            message = "ClickHouse headless service DNS refresh completed.",
            added = added_count,
            removed = removed_count,
            active = guard.entries.len(),
        );
    }
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
