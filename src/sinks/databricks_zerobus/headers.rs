//! OAuth headers provider that merges user-configured custom headers.

use std::collections::HashMap;
use std::sync::Arc;

use databricks_zerobus_ingest_sdk::{HeadersProvider, OAuthHeadersProvider, ZerobusResult};

use super::config::{DatabricksAuthentication, ZerobusSinkConfig};

/// Wraps the SDK's OAuth headers provider and merges user-configured static
/// headers into every gRPC request. Built once per `ZerobusService` so the
/// inner provider's token cache persists across stream rebuilds — the same
/// sharing the SDK gives `builder.oauth(...)`.
pub(super) struct OAuthWithCustomHeaders {
    pub(super) inner: OAuthHeadersProvider,
    /// Leaked once at service construction (`&'static`), so the provider — which
    /// the SDK owns beyond `build_stream` — needs no clone or lifetime. Bounded
    /// by the config's header count (validated, reserved names rejected). Empty
    /// when no custom headers are configured, in which case the merge is a no-op.
    pub(super) extra: &'static HashMap<&'static str, String>,
}

#[async_trait::async_trait]
impl HeadersProvider for OAuthWithCustomHeaders {
    async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
        let mut headers = self.inner.get_headers().await?;
        // Custom headers cannot clobber reserved names: rejected at config validate.
        headers.extend(self.extra.iter().map(|(k, v)| (*k, v.clone())));
        Ok(headers)
    }

    async fn invalidate(&self) {
        self.inner.invalidate().await;
    }
}

/// Leak the configured header map so both auth providers can hold a `&'static`
/// reference (the SDK owns the provider past `build_stream`) with no clone or
/// lifetime. Leaks a tiny map per service construction — a config change that
/// rebuilds the sink re-leaks it; bounded by the validated header count.
pub(super) fn leak_custom_headers(
    config: &ZerobusSinkConfig,
) -> &'static HashMap<&'static str, String> {
    Box::leak(Box::new(
        config
            .custom_headers
            .iter()
            .map(|(k, v)| {
                (
                    &*Box::leak(k.clone().into_boxed_str()) as &'static str,
                    v.clone(),
                )
            })
            .collect(),
    ))
}

/// For OAuth, build the SDK's `OAuthHeadersProvider` ourselves once (so its token
/// cache lives as long as the service, matching the SDK's shared cache for
/// `builder.oauth`) and wrap it to merge custom headers — an empty header map
/// makes the merge a no-op, so this is unconditional. `None` for LoginService,
/// which merges the headers itself in `build_stream`.
pub(super) fn build_oauth_provider(
    config: &ZerobusSinkConfig,
    custom_headers: &'static HashMap<&'static str, String>,
) -> Option<Arc<dyn HeadersProvider>> {
    match &config.auth {
        DatabricksAuthentication::OAuth {
            client_id,
            client_secret,
        } => {
            let inner = OAuthHeadersProvider::new(
                client_id.inner().to_string(),
                client_secret.inner().to_string(),
                config.table_name.clone(),
                workspace_id_from_endpoint(&config.ingestion_endpoint),
                config.unity_catalog_endpoint.clone(),
            );
            Some(Arc::new(OAuthWithCustomHeaders {
                inner,
                extra: custom_headers,
            }))
        }
        DatabricksAuthentication::LoginService(_) => None,
    }
}

/// Replicate the SDK's workspace-id derivation (`ZerobusSdkBuilder::build`):
/// scheme-normalize the endpoint, strip the scheme, and take the first dotted
/// segment. Needed because we construct `OAuthHeadersProvider` ourselves and the
/// SDK's own copy is `pub(crate)`. Infallible and byte-for-byte identical to the
/// SDK, so it never diverges from the workspace id the SDK already derived.
pub(super) fn workspace_id_from_endpoint(endpoint: &str) -> String {
    let normalized = if endpoint.starts_with("https://") || endpoint.starts_with("http://") {
        endpoint.to_string()
    } else {
        format!("https://{endpoint}")
    };
    normalized
        .strip_prefix("https://")
        .or_else(|| normalized.strip_prefix("http://"))
        .and_then(|s| s.split('.').next())
        .unwrap_or("")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_id_derivation() {
        assert_eq!(
            workspace_id_from_endpoint("https://dbc-abc123.cloud.databricks.com"),
            "dbc-abc123"
        );
        // Scheme-less endpoints are normalized like the SDK does.
        assert_eq!(
            workspace_id_from_endpoint("ingest.dev.databricks.com"),
            "ingest"
        );
    }
}
