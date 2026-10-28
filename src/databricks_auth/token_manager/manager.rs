use std::sync::Arc;
use std::time::Instant;

use base64::Engine;
use http::Uri;
use prost_reflect::{DynamicMessage, MethodDescriptor, Value};
use snafu::ResultExt;
use tokio::sync::RwLock;
use tracing::info;

use super::cache::CachedToken;
use super::config::{
    DEFAULT_TOKEN_LIFETIME, LoginServiceAuthConfig, MIN_TOKEN_VALIDITY, OAUTH_METHOD_NAME,
    OAUTH_SERVICE_NAME, PRINCIPAL_CONTEXT_HEADER, UC_GET_TABLE_METHOD_NAME, UC_SERVICE_NAME,
    UnityCatalogServiceConfig,
};
use super::error::{
    InvalidLoginEndpointSnafu, InvalidUcEndpointSnafu, OauthFailedSnafu, TokenManagerError,
    TokenTooShortLivedSnafu,
};
use super::jwt::{extract_jwt_expiry, principal_context_from_jwt};
use super::request::build_request_message;
use super::transport::{
    GrpcHttpClient, build_hyper_openssl_client, grpc_method_uri, grpc_unary_call,
    load_descriptor_pool,
};
use super::uc::uc_table_schema_from_response;
use databricks_zerobus_ingest_sdk::schema::UcTableSchema;

/// Manages internal OAuth token lifecycle: bootstrap and re-bootstrap on expiry.
///
/// Built on `prost_reflect::DynamicMessage` against a runtime-loaded `FileDescriptorSet`
/// — there's no compile-time proto codegen for the OAuth service, so vector doesn't carry
/// a snapshot of universe protos. The wire format is identical to a tonic-generated
/// client; only the message types are dynamic.
///
/// The merged Login service API (`GenerateReauthenticatedAndReauthorizedInternalOAuthToken`)
/// returns only an access token (no refresh token), so the manager re-bootstraps
/// before each expiry rather than refreshing.
pub struct TokenManager {
    config: LoginServiceAuthConfig,
    method: MethodDescriptor,
    client: GrpcHttpClient,
    endpoint: Uri,
    token: Arc<RwLock<Option<CachedToken>>>,
    unity_catalog: Option<UnityCatalogClient>,
}

/// gRPC client for the internal Unity Catalog `getTable` RPC.
struct UnityCatalogClient {
    method: MethodDescriptor,
    client: GrpcHttpClient,
    endpoint: Uri,
}

impl TokenManager {
    /// Create a new TokenManager. Loads the OAuth proto descriptor from disk and
    /// builds the hyper-openssl client used for every bootstrap call.
    ///
    /// One manager (holding one cached token) exists per LoginService `databricks_zerobus`
    /// sink, shared across that sink's Tower service clones. The token is not reusable across
    /// sinks: it is minted from `(workspace_id, service principal, UC permissions)`, all baked
    /// into the JWT, so distinct sinks (distinct table / workspace / UC) inherently need
    /// distinct tokens. The descriptor pool + mTLS client built here are identity-independent
    /// and could in principle be shared if a process ever ran many LoginService sinks, but that
    /// is a one-time startup cost (not per-request) and not worth optimizing while only a single
    /// sink is configured.
    pub async fn new(config: LoginServiceAuthConfig) -> Result<Self, TokenManagerError> {
        let pool = load_descriptor_pool(&config.login_service.oauth_proto_descriptor_path).await?;
        let svc = pool
            .get_service_by_name(OAUTH_SERVICE_NAME)
            .ok_or_else(|| TokenManagerError::OauthServiceNotFound {
                service: OAUTH_SERVICE_NAME.into(),
                path: config.login_service.oauth_proto_descriptor_path.clone(),
            })?;
        let method = svc
            .methods()
            .find(|m| m.name() == OAUTH_METHOD_NAME)
            .ok_or_else(|| TokenManagerError::OauthMethodNotFound {
                method: OAUTH_METHOD_NAME.into(),
                service: OAUTH_SERVICE_NAME.into(),
            })?;

        let endpoint: Uri =
            config
                .login_service
                .login_endpoint
                .parse()
                .context(InvalidLoginEndpointSnafu {
                    uri: config.login_service.login_endpoint.clone(),
                })?;

        let client = build_hyper_openssl_client(
            &config,
            &config.login_service.login_server_name,
            config.login_service.verify_hostname,
        )?;
        let unity_catalog = match &config.unity_catalog {
            Some(uc) => Some(UnityCatalogClient::new(&config, uc).await?),
            None => None,
        };

        Ok(Self {
            config,
            method,
            client,
            endpoint,
            token: Arc::new(RwLock::new(None)),
            unity_catalog,
        })
    }

    /// Bootstrap: call GenerateReauthenticatedAndReauthorizedInternalOAuthToken
    /// over hyper-openssl, decode the response, and return the cached token.
    async fn bootstrap(&self) -> Result<CachedToken, TokenManagerError> {
        let request_msg = build_request_message(&self.config, &self.method.input())?;

        info!(
            message = "Bootstrapping OAuth token from Login service.",
            workspace_id = self.config.user.workspace_id,
            service_principal_resource = %self.config.user.service_principal_resource,
            service_principal_type = self.config.user.service_principal_type,
        );

        let response_msg = grpc_unary_call(
            &self.client,
            grpc_method_uri(&self.endpoint, &self.method)?,
            &[],
            &request_msg,
            &self.method.output(),
        )
        .await?;

        // Defend against an empty / non-Success state. The proto enum value 1 is
        // SUCCESS; all others are failures. Use field-by-name access so we don't have
        // to mirror the full enum type at runtime.
        let state_value = response_msg
            .get_field_by_name("state")
            .map(|v| v.into_owned());
        let state_int = match state_value {
            Some(Value::EnumNumber(n)) => n,
            Some(Value::I32(n)) => n,
            _ => 0,
        };
        if state_int != 1 {
            let message = response_msg
                .get_field_by_name("error_message")
                .map(|v| v.into_owned())
                .and_then(|v| match v {
                    Value::String(s) => Some(s),
                    _ => None,
                })
                .unwrap_or_default();
            return OauthFailedSnafu {
                state: state_int,
                message,
            }
            .fail();
        }

        let access_token = response_msg
            .get_field_by_name("access_token")
            .map(|v| v.into_owned())
            .and_then(|v| match v {
                Value::String(s) => Some(s),
                _ => None,
            })
            .ok_or(TokenManagerError::AccessTokenMissing)?;

        let expires_at = extract_jwt_expiry(&access_token)
            .unwrap_or_else(|| Instant::now() + DEFAULT_TOKEN_LIFETIME);
        let cached = CachedToken {
            access_token,
            expires_at,
        };

        // Reject a token too short-lived to use: serving it risks the token expiring
        // mid-write to Zerobus, which is costlier than failing the bootstrap and retrying.
        // The 50-min DEFAULT_TOKEN_LIFETIME fallback always clears this floor, so an
        // unparseable `exp` claim is unaffected.
        let now = Instant::now();
        if !cached.has_remaining_lifetime(now, MIN_TOKEN_VALIDITY) {
            return TokenTooShortLivedSnafu {
                ttl_secs: cached.expires_at.saturating_duration_since(now).as_secs(),
                min_secs: MIN_TOKEN_VALIDITY.as_secs(),
            }
            .fail();
        }

        info!(
            message = "OAuth token bootstrap successful.",
            expires_in_secs = cached.expires_at.saturating_duration_since(now).as_secs(),
        );

        Ok(cached)
    }

    /// Get a valid access token, re-bootstrapping if near expiry.
    pub async fn get_token(&self) -> Result<String, TokenManagerError> {
        // Fast path: check if we have a valid token.
        {
            let guard = self.token.read().await;
            if let Some(ref cached) = *guard
                && cached.is_valid_at(Instant::now())
            {
                return Ok(cached.access_token.clone());
            }
        }

        // Slow path: need to re-bootstrap.
        let mut guard = self.token.write().await;

        // Double-check after acquiring write lock.
        if let Some(ref cached) = *guard
            && cached.is_valid_at(Instant::now())
        {
            return Ok(cached.access_token.clone());
        }

        let new_token = self.bootstrap().await?;
        let token = new_token.access_token.clone();
        *guard = Some(new_token);
        Ok(token)
    }

    /// Fetch a table's schema from the internal Unity Catalog service, authenticating with
    /// the Login-minted token and the principal context embedded in it.
    pub async fn get_uc_table_schema(
        &self,
        full_name: &str,
    ) -> Result<UcTableSchema, TokenManagerError> {
        let uc = self
            .unity_catalog
            .as_ref()
            .ok_or(TokenManagerError::UnityCatalogNotConfigured)?;
        let token = self.get_token().await?;
        let principal_context = principal_context_from_jwt(&token)?;
        uc.get_table_schema(&token, &principal_context, full_name)
            .await
    }
}

impl UnityCatalogClient {
    async fn new(
        config: &LoginServiceAuthConfig,
        uc: &UnityCatalogServiceConfig,
    ) -> Result<Self, TokenManagerError> {
        let pool = load_descriptor_pool(&uc.proto_descriptor_path).await?;
        let method = pool
            .get_service_by_name(UC_SERVICE_NAME)
            .and_then(|svc| svc.methods().find(|m| m.name() == UC_GET_TABLE_METHOD_NAME))
            .ok_or_else(|| TokenManagerError::UcMethodNotFound {
                method: UC_GET_TABLE_METHOD_NAME.into(),
                service: UC_SERVICE_NAME.into(),
                path: uc.proto_descriptor_path.clone(),
            })?;
        let endpoint: Uri = uc.endpoint.parse().context(InvalidUcEndpointSnafu {
            uri: uc.endpoint.clone(),
        })?;
        let client = build_hyper_openssl_client(config, &uc.server_name, uc.verify_hostname)?;
        Ok(Self {
            method,
            client,
            endpoint,
        })
    }

    async fn get_table_schema(
        &self,
        token: &str,
        principal_context: &[u8],
        full_name: &str,
    ) -> Result<UcTableSchema, TokenManagerError> {
        let mut request = DynamicMessage::new(self.method.input());
        request
            .try_set_field_by_name("full_name_arg", Value::String(full_name.to_string()))
            .map_err(|e| TokenManagerError::UcResponse {
                message: format!("set full_name_arg on getTable request: {e}"),
            })?;
        let headers = [
            ("authorization", format!("Bearer {token}")),
            (
                PRINCIPAL_CONTEXT_HEADER,
                base64::engine::general_purpose::STANDARD_NO_PAD.encode(principal_context),
            ),
        ];
        let response = grpc_unary_call(
            &self.client,
            grpc_method_uri(&self.endpoint, &self.method)?,
            &headers,
            &request,
            &self.method.output(),
        )
        .await?;
        uc_table_schema_from_response(&response)
    }
}
