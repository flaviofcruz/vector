use std::sync::Arc;
use std::time::Instant;

use http::{Request as HttpRequest, Uri};
use hyper::Body;
use prost_reflect::{DescriptorPool, MethodDescriptor, Value, prost::Message};
use snafu::ResultExt;
use tokio::sync::RwLock;
use tracing::info;

use super::cache::CachedToken;
use super::config::{
    DEFAULT_TOKEN_LIFETIME, LoginServiceAuthConfig, MIN_TOKEN_VALIDITY, OAUTH_METHOD_NAME,
    OAUTH_SERVICE_NAME,
};
use super::error::{
    BuildDescriptorPoolSnafu, BuildGrpcHttpRequestSnafu, DecodeDescriptorSetSnafu,
    InvalidGrpcUriSnafu, InvalidLoginEndpointSnafu, OauthFailedSnafu, ReadDescriptorSnafu,
    ReadResponseBodySnafu, TokenManagerError, TokenTooShortLivedSnafu, TransportSnafu,
};
use super::jwt::extract_jwt_expiry;
use super::request::build_request_message;
use super::transport::{
    build_hyper_openssl_client, encode_grpc_message, parse_grpc_response_message,
};

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
    client: hyper::Client<hyper_openssl::HttpsConnector<hyper::client::HttpConnector>>,
    endpoint: Uri,
    token: Arc<RwLock<Option<CachedToken>>>,
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
        // Load the runtime-mounted FileDescriptorSet covering the OAuth service.
        let descriptor_bytes = tokio::fs::read(&config.login_service.oauth_proto_descriptor_path)
            .await
            .context(ReadDescriptorSnafu {
                path: config.login_service.oauth_proto_descriptor_path.clone(),
            })?;
        let fds = prost_reflect::prost_types::FileDescriptorSet::decode(&descriptor_bytes[..])
            .context(DecodeDescriptorSetSnafu)?;
        let pool =
            DescriptorPool::from_file_descriptor_set(fds).context(BuildDescriptorPoolSnafu)?;

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

        let endpoint: Uri = config
            .login_service
            .login_endpoint
            .parse()
            .context(InvalidLoginEndpointSnafu {
                uri: config.login_service.login_endpoint.clone(),
            })?;

        let client = build_hyper_openssl_client(&config)?;

        Ok(Self {
            config,
            method,
            client,
            endpoint,
            token: Arc::new(RwLock::new(None)),
        })
    }

    /// Build the gRPC URI for the OAuth bootstrap RPC.
    fn grpc_uri(&self) -> Result<Uri, TokenManagerError> {
        let path = format!(
            "/{}/{}",
            self.method.parent_service().full_name(),
            self.method.name()
        );
        let endpoint_str = self.endpoint.to_string();
        let endpoint_base = endpoint_str.trim_end_matches('/');
        let uri_str = format!("{}{}", endpoint_base, path);
        uri_str
            .parse::<Uri>()
            .context(InvalidGrpcUriSnafu { uri: uri_str })
    }

    /// Bootstrap: call GenerateReauthenticatedAndReauthorizedInternalOAuthToken
    /// over hyper-openssl, decode the response, and return the cached token.
    async fn bootstrap(&self) -> Result<CachedToken, TokenManagerError> {
        let request_msg = build_request_message(&self.config, &self.method.input())?;
        let request_bytes = request_msg.encode_to_vec();

        let uri = self.grpc_uri()?;
        let http_req = HttpRequest::builder()
            .uri(uri)
            .method("POST")
            .header("content-type", "application/grpc+proto")
            .header("te", "trailers")
            .header("grpc-encoding", "identity")
            .body(Body::from(encode_grpc_message(request_bytes)))
            .context(BuildGrpcHttpRequestSnafu)?;

        info!(
            message = "Bootstrapping OAuth token from Login service.",
            workspace_id = self.config.user.workspace_id,
            service_principal_resource = %self.config.user.service_principal_resource,
            service_principal_type = self.config.user.service_principal_type,
        );

        let response = self
            .client
            .request(http_req)
            .await
            .context(TransportSnafu)?;

        // Inspect gRPC status (header-trailers; for unary calls the status is in the
        // initial response or in trailers). Treat non-zero as error.
        let status = response
            .headers()
            .get("grpc-status")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(0);
        if status != 0 {
            let message = response
                .headers()
                .get("grpc-message")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("unknown grpc error")
                .to_string();
            return Err(TokenManagerError::GrpcStatus { status, message });
        }

        // Using hyper::body::to_bytes which is deprecated in favor of http_body_util.
        // Vector's ecosystem is currently on hyper 0.14, mirroring bricklens_ingest sink.
        #[allow(deprecated)]
        let body = hyper::body::to_bytes(response.into_body())
            .await
            .context(ReadResponseBodySnafu)?;

        let response_msg = parse_grpc_response_message(body, &self.method.output())?;

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
}
