//! DNS resolution for headless Kubernetes service discovery.
//!
//! Resolves a headless K8s service DNS name to individual pod IPs,
//! enabling direct P2C load-balanced dispatch to ClickHouse shard pods.

use std::collections::HashSet;
use std::net::IpAddr;

use http::Uri;
use tokio::net::lookup_host;

/// Resolves the hostname in the given URI to all A/AAAA-record IPs and returns
/// a `Vec<Uri>` with each resolved IP substituted into the original URI.
///
/// The scheme, port, and path of the original URI are preserved.
/// Duplicate IPs are deduplicated.
///
/// Note: `lookup_host` requires a `(host, port)` tuple — `Uri` does not
/// implement `ToSocketAddrs`, so we extract those components from the URI.
/// The resolved URIs are built using `Uri::builder()` with `SocketAddr`'s
/// `Display` impl (which handles IPv6 bracket formatting per RFC 3986).
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

    let addrs: Vec<std::net::SocketAddr> = lookup_host((host, port))
        .await
        .map_err(|e| format!("DNS resolution failed for '{}': {}", host, e))?
        .collect();

    if addrs.is_empty() {
        return Err(format!("DNS resolution for '{}' returned no addresses", host).into());
    }

    let mut seen = HashSet::new();
    let uris = addrs
        .into_iter()
        .filter(|addr| seen.insert(addr.ip()))
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
        message = "Resolved headless DNS endpoints for ClickHouse.",
        host = %host,
        count = uris.len(),
        endpoints = ?uris.iter().map(|u| u.to_string()).collect::<Vec<_>>(),
    );

    Ok(uris)
}

/// Extracts the IP address from a resolved URI's host component.
///
/// `http::Uri::host()` includes brackets for IPv6 (e.g., `[::1]`), but
/// `IpAddr::parse` does not accept brackets, so we strip them before parsing.
pub fn ip_from_uri(uri: &Uri) -> Option<IpAddr> {
    uri.host().and_then(|h| {
        // Strip IPv6 brackets: http::Uri::host() returns "[::1]" for IPv6,
        // but IpAddr expects "::1".
        let h = h
            .strip_prefix('[')
            .and_then(|h| h.strip_suffix(']'))
            .unwrap_or(h);
        h.parse().ok()
    })
}

#[cfg(test)]
mod tests {
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
        let ips: Vec<_> = result.iter().filter_map(|u| ip_from_uri(u)).collect();
        let unique: HashSet<_> = ips.iter().collect();
        assert_eq!(ips.len(), unique.len(), "IPs should be deduplicated");
    }
}
