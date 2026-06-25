use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use databricks_zerobus_ingest_sdk::{HeadersProvider, ZerobusError, ZerobusResult};

use super::TokenManager;

/// A `HeadersProvider` implementation that uses the Login service TokenManager
/// to provide OAuth Bearer tokens for Zerobus gRPC requests.
pub struct LoginServiceHeadersProvider {
    token_manager: Arc<TokenManager>,
    table_name: String,
}

impl LoginServiceHeadersProvider {
    /// Create a new LoginServiceHeadersProvider.
    pub fn new(token_manager: Arc<TokenManager>, table_name: String) -> Self {
        Self {
            token_manager,
            table_name,
        }
    }
}

#[async_trait]
impl HeadersProvider for LoginServiceHeadersProvider {
    async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
        let token = self.token_manager.get_token().await.map_err(|e| {
            ZerobusError::InvalidUCTokenError(format!(
                "Failed to get OAuth token from Login service: {}",
                e
            ))
        })?;

        let mut headers = HashMap::new();
        headers.insert("authorization", format!("Bearer {}", token));
        headers.insert("x-databricks-zerobus-table-name", self.table_name.clone());
        if let Ok(app_instance) = std::env::var("ZEROBUS_APP_INSTANCE_NAME") {
            if !app_instance.is_empty() {
                headers.insert("x-databricks-app-instance-name", app_instance);
            }
        }
        Ok(headers)
    }
}
