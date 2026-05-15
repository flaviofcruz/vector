//! Unity Catalog schema fetching.

use bytes::Buf;
use http::{Request, Uri};
use http_body::Body as HttpBody;
use hyper::Body;
use percent_encoding::{NON_ALPHANUMERIC, percent_encode};
use serde::Deserialize;

use super::error::ZerobusSinkError;
use crate::config::ProxyConfig;
use crate::http::HttpClient;
use crate::tls::TlsSettings;

/// Unity Catalog table column information
#[derive(Debug, Deserialize, Clone)]
pub struct UnityCatalogColumn {
    pub name: String,
    #[allow(dead_code)] // Will be used for complex type parsing
    pub type_text: String,
    pub type_name: String,
    #[serde(default)]
    pub position: i32,
    pub nullable: bool,
    #[allow(dead_code)] // Will be used for complex type parsing
    #[serde(default)]
    pub type_json: String,
}

/// Unity Catalog table schema response
#[derive(Debug, Deserialize)]
pub struct UnityCatalogTableSchema {
    pub name: String,
    pub catalog_name: String,
    pub schema_name: String,
    pub columns: Vec<UnityCatalogColumn>,
}

impl UnityCatalogTableSchema {
    /// Convert to the SDK's `UcTableSchema` so we can call the SDK's
    /// schema-conversion helpers (`arrow_schema_from_uc_schema`, etc.).
    pub fn to_sdk_uc_schema(&self) -> databricks_zerobus_ingest_sdk::schema::UcTableSchema {
        use databricks_zerobus_ingest_sdk::schema::{UcColumn, UcTableSchema};
        UcTableSchema {
            name: self.name.clone(),
            catalog_name: self.catalog_name.clone(),
            schema_name: self.schema_name.clone(),
            columns: self
                .columns
                .iter()
                .map(|c| UcColumn {
                    name: c.name.clone(),
                    type_name: c.type_name.clone(),
                    type_text: c.type_text.clone(),
                    type_json: c.type_json.clone(),
                    nullable: c.nullable,
                    position: c.position,
                })
                .collect(),
        }
    }
}

/// OAuth token response from Databricks
#[derive(Debug, Deserialize)]
struct OAuthTokenResponse {
    access_token: String,
}

/// Fetch table schema from Unity Catalog API
pub async fn fetch_table_schema(
    unity_catalog_endpoint: &str,
    table_name: &str,
    client_id: &str,
    client_secret: &str,
) -> Result<UnityCatalogTableSchema, ZerobusSinkError> {
    // First, get OAuth token
    let token = get_oauth_token(unity_catalog_endpoint, client_id, client_secret).await?;

    // Fetch table schema
    let url = format!(
        "{}/api/2.0/unity-catalog/tables/{}",
        unity_catalog_endpoint.trim_end_matches('/'),
        table_name
    );

    let uri: Uri = url.parse().map_err(|e| ZerobusSinkError::ConfigError {
        message: format!("Invalid Unity Catalog endpoint URL: {}", e),
    })?;

    let http_client =
        HttpClient::new(TlsSettings::default(), &ProxyConfig::default()).map_err(|e| {
            ZerobusSinkError::ConfigError {
                message: format!("Failed to create HTTP client: {}", e),
            }
        })?;

    let request = Request::get(uri)
        .header("Authorization", format!("Bearer {}", token))
        .header("Content-Type", "application/json")
        .body(Body::empty())
        .map_err(|e| ZerobusSinkError::ConfigError {
            message: format!("Failed to build request: {}", e),
        })?;

    let response = http_client
        .send(request)
        .await
        .map_err(|e| ZerobusSinkError::ConfigError {
            message: format!("Failed to fetch table schema: {}", e),
        })?;

    let status = response.status();
    if !status.is_success() {
        let body_bytes = response
            .into_body()
            .collect()
            .await
            .map(|c| c.to_bytes())
            .unwrap_or_default();
        let error_text = String::from_utf8_lossy(&body_bytes);
        return Err(ZerobusSinkError::ConfigError {
            message: format!(
                "Unity Catalog API returned error {}: {}",
                status, error_text
            ),
        });
    }

    let body_bytes = response
        .into_body()
        .collect()
        .await
        .map(|c| c.to_bytes())
        .map_err(|e| ZerobusSinkError::ConfigError {
            message: format!("Failed to read response body: {}", e),
        })?;

    let schema: UnityCatalogTableSchema =
        serde_json::from_reader(body_bytes.reader()).map_err(|e| {
            ZerobusSinkError::ConfigError {
                message: format!("Failed to parse table schema response: {}", e),
            }
        })?;

    Ok(schema)
}

/// Get OAuth token from Databricks
async fn get_oauth_token(
    unity_catalog_endpoint: &str,
    client_id: &str,
    client_secret: &str,
) -> Result<String, ZerobusSinkError> {
    let token_url = format!(
        "{}/oidc/v1/token",
        unity_catalog_endpoint.trim_end_matches('/')
    );

    let uri: Uri = token_url
        .parse()
        .map_err(|e| ZerobusSinkError::ConfigError {
            message: format!("Invalid token endpoint URL: {}", e),
        })?;

    // Build form-encoded body
    let form_body = format!(
        "grant_type=client_credentials&client_id={}&client_secret={}&scope=all-apis",
        percent_encode(client_id.as_bytes(), NON_ALPHANUMERIC),
        percent_encode(client_secret.as_bytes(), NON_ALPHANUMERIC)
    );

    let http_client =
        HttpClient::new(TlsSettings::default(), &ProxyConfig::default()).map_err(|e| {
            ZerobusSinkError::ConfigError {
                message: format!("Failed to create HTTP client: {}", e),
            }
        })?;

    let request = Request::post(uri)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(Body::from(form_body))
        .map_err(|e| ZerobusSinkError::ConfigError {
            message: format!("Failed to build OAuth request: {}", e),
        })?;

    let response = http_client
        .send(request)
        .await
        .map_err(|e| ZerobusSinkError::ConfigError {
            message: format!("Failed to get OAuth token: {}", e),
        })?;

    let status = response.status();
    if !status.is_success() {
        let body_bytes = response
            .into_body()
            .collect()
            .await
            .map(|c| c.to_bytes())
            .unwrap_or_default();
        let error_text = String::from_utf8_lossy(&body_bytes);
        return Err(ZerobusSinkError::ConfigError {
            message: format!("OAuth token request failed {}: {}", status, error_text),
        });
    }

    let body_bytes = response
        .into_body()
        .collect()
        .await
        .map(|c| c.to_bytes())
        .map_err(|e| ZerobusSinkError::ConfigError {
            message: format!("Failed to read OAuth response body: {}", e),
        })?;

    let token_response: OAuthTokenResponse =
        serde_json::from_reader(body_bytes.reader()).map_err(|e| {
            ZerobusSinkError::ConfigError {
                message: format!("Failed to parse OAuth token response: {}", e),
            }
        })?;

    Ok(token_response.access_token)
}
