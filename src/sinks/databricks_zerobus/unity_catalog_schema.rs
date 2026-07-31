//! Unity Catalog schema fetching.

use bytes::Buf;
use http::{Request, StatusCode, Uri};
use http_body::Body as HttpBody;
use hyper::Body;
use percent_encoding::{NON_ALPHANUMERIC, percent_encode};
use serde::Deserialize;

use super::error::ZerobusSinkError;
use crate::http::HttpClient;

/// Whether a Unity Catalog HTTP response status should be retried.
///
/// Mirrors the canonical Vector HTTP retry policy used by other HTTP-based
/// sinks (`crate::sinks::util::http::HttpRetryLogic`) so this sink stays in
/// lock-step with them: 5xx (except 501 Not Implemented), 408 (Request
/// Timeout), and 429 (Too Many Requests) are transient; 4xx otherwise (404,
/// 401, 403, ...) and 501 are permanent.
fn status_is_retryable(status: StatusCode) -> bool {
    match status {
        StatusCode::TOO_MANY_REQUESTS | StatusCode::REQUEST_TIMEOUT => true,
        StatusCode::NOT_IMPLEMENTED => false,
        s => s.is_server_error(),
    }
}

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
    http_client: &HttpClient,
) -> Result<UnityCatalogTableSchema, ZerobusSinkError> {
    // First, get OAuth token
    let token = get_oauth_token(
        http_client,
        unity_catalog_endpoint,
        client_id,
        client_secret,
    )
    .await?;

    // Fetch table schema.
    // Encode each segment of the fully-qualified table name (catalog.schema.table)
    // so that reserved URI characters in quoted Unity Catalog identifiers (spaces,
    // #, /, etc.) don't break URI parsing or hit the wrong endpoint.
    let encoded_table_name: String = table_name
        .split('.')
        .map(|seg| percent_encode(seg.as_bytes(), NON_ALPHANUMERIC).to_string())
        .collect::<Vec<_>>()
        .join(".");
    let url = format!(
        "{}/api/2.1/unity-catalog/tables/{}",
        unity_catalog_endpoint.trim_end_matches('/'),
        encoded_table_name
    );

    let uri: Uri = url.parse().map_err(|e| ZerobusSinkError::ConfigError {
        message: format!("Invalid Unity Catalog endpoint URL: {}", e),
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
        .map_err(|e| ZerobusSinkError::SchemaError {
            message: format!("Failed to fetch table schema: {}", e),
            retryable: true,
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
        return Err(ZerobusSinkError::SchemaError {
            message: format!(
                "Unity Catalog API returned error {}: {}",
                status, error_text
            ),
            retryable: status_is_retryable(status),
        });
    }

    let body_bytes = response
        .into_body()
        .collect()
        .await
        .map(|c| c.to_bytes())
        .map_err(|e| ZerobusSinkError::SchemaError {
            message: format!("Failed to read response body: {}", e),
            retryable: true,
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
    http_client: &HttpClient,
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

    let request = Request::post(uri)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(Body::from(form_body))
        .map_err(|e| ZerobusSinkError::ConfigError {
            message: format!("Failed to build OAuth request: {}", e),
        })?;

    let response = http_client
        .send(request)
        .await
        .map_err(|e| ZerobusSinkError::SchemaError {
            message: format!("Failed to get OAuth token: {}", e),
            retryable: true,
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
        return Err(ZerobusSinkError::SchemaError {
            message: format!("OAuth token request failed {}: {}", status, error_text),
            retryable: status_is_retryable(status),
        });
    }

    let body_bytes = response
        .into_body()
        .collect()
        .await
        .map(|c| c.to_bytes())
        .map_err(|e| ZerobusSinkError::SchemaError {
            message: format!("Failed to read OAuth response body: {}", e),
            retryable: true,
        })?;

    let token_response: OAuthTokenResponse =
        serde_json::from_reader(body_bytes.reader()).map_err(|e| {
            ZerobusSinkError::ConfigError {
                message: format!("Failed to parse OAuth token response: {}", e),
            }
        })?;

    Ok(token_response.access_token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_is_retryable_matches_canonical_policy() {
        // Transient — must retry.
        assert!(status_is_retryable(StatusCode::INTERNAL_SERVER_ERROR));
        assert!(status_is_retryable(StatusCode::BAD_GATEWAY));
        assert!(status_is_retryable(StatusCode::SERVICE_UNAVAILABLE));
        assert!(status_is_retryable(StatusCode::GATEWAY_TIMEOUT));
        assert!(status_is_retryable(StatusCode::REQUEST_TIMEOUT));
        assert!(status_is_retryable(StatusCode::TOO_MANY_REQUESTS));
        // Permanent — must not retry. 501 in particular: the server doesn't
        // support the requested functionality; retry won't change that.
        assert!(!status_is_retryable(StatusCode::NOT_IMPLEMENTED));
        assert!(!status_is_retryable(StatusCode::NOT_FOUND));
        assert!(!status_is_retryable(StatusCode::UNAUTHORIZED));
        assert!(!status_is_retryable(StatusCode::FORBIDDEN));
        assert!(!status_is_retryable(StatusCode::BAD_REQUEST));
    }
}
