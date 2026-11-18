//! DNS resolution for headless Kubernetes service discovery.
//!
//! Resolves a headless K8s service DNS name to individual pod IPs,
//! supporting direct dispatch to pods while retaining the ClickHouse URI API.

#[cfg(any(feature = "sinks-clickhouse", feature = "sinks-vector", test))]
use std::net::IpAddr;

#[cfg(any(feature = "sinks-clickhouse", feature = "sinks-vector", test))]
use http::Uri;
use tokio::net::lookup_host;

/// Resolves a host through the system resolver; empty answers remain distinguishable.
pub async fn resolve_addresses(host: &str, port: u16) -> crate::Result<Vec<std::net::SocketAddr>> {
    Ok(lookup_host((host, port)).await?.collect())
}

/// Resolves the hostname to deduplicated A/AAAA-record IP URIs, preserving scheme,
/// port and path. Empty answers are errors for the ClickHouse compatibility API.
#[cfg(any(feature = "sinks-clickhouse", feature = "sinks-vector", test))]
pub async fn resolve_endpoints(endpoint: &Uri) -> crate::Result<Vec<Uri>> {
    let host = endpoint.host().ok_or("Endpoint URI has no host")?;
    let port = endpoint.port_u16().unwrap_or(match endpoint.scheme_str() {
        Some("https") => 443,
        _ => 80,
    });
    let scheme = endpoint.scheme_str().unwrap_or("http");
    let path_and_query = endpoint
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");

    let addrs = resolve_addresses(host, port)
        .await
        .map_err(|e| format!("DNS resolution failed for '{}': {}", host, e))?;

    if addrs.is_empty() {
        return Err(format!("DNS resolution for '{}' returned no addresses", host).into());
    }

    // Deduplicate by IP using a HashMap, then build per-IP URIs.
    let uris = addrs
        .into_iter()
        .map(|addr| (addr.ip(), addr))
        .collect::<std::collections::HashMap<_, _>>()
        .into_values()
        .map(|addr| {
            // SocketAddr::to_string() formats as "ip:port" for IPv4 and
            // "[ip]:port" for IPv6, which is valid URI authority syntax.
            Uri::builder()
                .scheme(scheme)
                .authority(addr.to_string().as_str())
                .path_and_query(path_and_query)
                .build()
                .map_err(|e| format!("Failed to build resolved URI for '{}': {}", addr, e).into())
        })
        .collect::<crate::Result<Vec<Uri>>>()?;

    info!(
        message = "Resolved headless DNS endpoints.",
        host = %host,
        count = uris.len(),
    );
    debug!(
        message = "Resolved headless DNS endpoints (detailed).",
        host = %host,
        endpoints = ?uris.iter().map(|u| u.to_string()).collect::<Vec<_>>(),
    );

    Ok(uris)
}

/// Extracts the IP address from a resolved URI's host component.
///
/// Strips IPv6 brackets before parsing, since `http::Uri::host()` returns
/// `"[::1]"` with brackets intact for IPv6 addresses.
#[cfg(any(feature = "sinks-clickhouse", feature = "sinks-vector", test))]
pub fn ip_from_uri(uri: &Uri) -> Option<IpAddr> {
    uri.host().and_then(|h| {
        // Strip exactly one leading '[' and trailing ']' for IPv6 addresses.
        // Falls back to the original string for IPv4 or hostnames.
        let h = h
            .strip_prefix('[')
            .and_then(|h| h.strip_suffix(']'))
            .unwrap_or(h);
        h.parse().ok()
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn test_ip_from_uri_v4() {
        let uri: Uri = "http://10.0.1.5:8123/".parse().unwrap();
        assert_eq!(ip_from_uri(&uri), Some("10.0.1.5".parse().unwrap()));
    }

    #[test]
    fn test_ip_from_uri_v6() {
        let uri: Uri = "http://[::1]:8123/".parse().unwrap();
        assert_eq!(ip_from_uri(&uri), Some("::1".parse().unwrap()));
    }

    #[test]
    fn test_ip_from_uri_hostname() {
        let uri: Uri = "http://clickhouse.ns.svc:8123/".parse().unwrap();
        assert_eq!(ip_from_uri(&uri), None);
    }

    #[tokio::test]
    async fn test_resolve_endpoints_localhost() {
        let uri: Uri = "http://localhost:8123/".parse().unwrap();
        let result = resolve_endpoints(&uri).await;
        assert!(result.is_ok());
        let endpoints = result.unwrap();
        assert!(!endpoints.is_empty());
        for ep in &endpoints {
            assert_eq!(ep.port_u16(), Some(8123));
            assert_eq!(ep.scheme_str(), Some("http"));
        }
    }

    #[tokio::test]
    async fn test_resolve_endpoints_no_host() {
        let uri: Uri = "/just/a/path".parse().unwrap();
        let result = resolve_endpoints(&uri).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_resolve_endpoints_preserves_path() {
        let uri: Uri = "http://localhost:8123/custom/path".parse().unwrap();
        let result = resolve_endpoints(&uri).await;
        if let Ok(endpoints) = result {
            for ep in &endpoints {
                assert!(ep.path().starts_with("/custom/path"));
            }
        }
    }

    #[tokio::test]
    async fn test_resolve_endpoints_deduplicates() {
        let uri: Uri = "http://localhost:8123/".parse().unwrap();
        let result = resolve_endpoints(&uri).await.unwrap();
        let ips: Vec<_> = result.iter().filter_map(ip_from_uri).collect();
        let unique: HashSet<_> = ips.iter().collect();
        assert_eq!(ips.len(), unique.len(), "IPs should be deduplicated");
    }
}
