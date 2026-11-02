use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use databricks_zerobus_ingest_sdk::{HeadersProvider, ZerobusError, ZerobusResult};

use super::TokenManager;
use super::token_manager::SIGNED_PRINCIPAL_CONTEXT_HEADER;

/// A `HeadersProvider` implementation that uses the Login service TokenManager
/// to provide the signed principal context for Zerobus gRPC requests. No
/// `authorization` header is sent.
pub struct LoginServiceHeadersProvider {
    token_manager: Arc<TokenManager>,
    table_name: String,
    /// User-configured static headers merged into every request. Leaked once at
    /// service construction (`&'static`). Reserved names are rejected at config
    /// validation, so these never clobber auth headers.
    custom_headers: &'static HashMap<&'static str, String>,
}

impl LoginServiceHeadersProvider {
    /// Create a new LoginServiceHeadersProvider.
    pub const fn new(
        token_manager: Arc<TokenManager>,
        table_name: String,
        custom_headers: &'static HashMap<&'static str, String>,
    ) -> Self {
        Self {
            token_manager,
            table_name,
            custom_headers,
        }
    }
}

#[async_trait]
impl HeadersProvider for LoginServiceHeadersProvider {
    async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
        let signed_principal_context = self
            .token_manager
            .get_signed_principal_context()
            .await
            .map_err(|e| {
                ZerobusError::InvalidUCTokenError(format!(
                    "Failed to get principal context from Login service: {}",
                    e
                ))
            })?;

        let mut headers = HashMap::new();
        // Base64, per the gRPC `-bin` convention; the SDK must send it as binary metadata.
        headers.insert(SIGNED_PRINCIPAL_CONTEXT_HEADER, signed_principal_context);
        headers.insert("x-databricks-zerobus-table-name", self.table_name.clone());
        if let Ok(app_instance) = std::env::var("ZEROBUS_APP_INSTANCE_NAME")
            && !app_instance.is_empty()
        {
            headers.insert("x-databricks-app-instance-name", app_instance);
        }
        headers.extend(self.custom_headers.iter().map(|(k, v)| (*k, v.clone())));
        Ok(headers)
    }
}
