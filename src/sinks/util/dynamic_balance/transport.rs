use std::error::Error as _;

use hyper::client::HttpConnector;
use hyper_openssl::HttpsConnector;
use hyper_proxy::ProxyConnector;
use tonic::body::BoxBody;

use crate::{
    config::ProxyConfig,
    http::build_proxy_connector,
    tls::{MaybeTlsSettings, TlsEnableableConfig, tls_connector_builder},
};

/// The opt-in path verifies the configured DNS identity while dialing IP URIs.
/// OpenSSL's `into_ssl(ip)` would otherwise overwrite the verification target
/// after the connector's SNI callback runs.
pub fn new_client(
    tls: &MaybeTlsSettings,
    options: Option<&TlsEnableableConfig>,
    proxy: &ProxyConfig,
) -> crate::Result<hyper::Client<ProxyConnector<HttpsConnector<HttpConnector>>, BoxBody>> {
    if tls.is_tls()
        && options
            .and_then(|options| options.options.server_name.as_ref())
            .is_none()
    {
        return Err("load_balancing with TLS requires tls.server_name set to the certificate DNS identity (pod IPs are used for connections)".into());
    }
    let mut connector = build_proxy_connector(tls.clone(), proxy)?;
    if let Some(options) = options.filter(|_| tls.is_tls())
        && let Some(server_name) = options.options.server_name.clone()
    {
        // A CONNECT proxy performs its own TLS setup without the HTTPS
        // callback. Reject that combination rather than weaken verification.
        if !connector.proxies().is_empty() {
            return Err("load_balancing with tls.server_name requires direct pod connections (disable the sink proxy)".into());
        }
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        let mut https = HttpsConnector::with_connector(http, tls_connector_builder(tls)?)?;
        let settings = tls.tls().cloned();
        let verify = options.options.verify_hostname.unwrap_or(true);
        https.set_callback(move |connection, _| {
            if let Some(settings) = &settings {
                settings.apply_connect_configuration(connection)?;
            }
            connection.set_use_server_name_indication(false);
            connection.set_hostname(&server_name)?;
            // Chain verification remains enabled. Set the hostname check
            // explicitly so OpenSSL cannot replace it with the pod IP.
            connection.set_verify_hostname(false);
            if verify {
                connection.param_mut().set_host(&server_name)?;
            }
            Ok(())
        });
        connector = connector.with_connector(https);
    }
    Ok(hyper::Client::builder().http2_only(true).build(connector))
}

/// Backpressure trains ARC alone; transport and availability errors train health.
pub(crate) fn classify_grpc_error(status: &tonic::Status) -> Option<bool> {
    if status.code() == tonic::Code::ResourceExhausted {
        return None;
    }
    Some(
        matches!(
            status.code(),
            tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
        ) || status.source().is_some(),
    )
}
