use std::sync::Arc;
use std::time::Instant;

use base64::Engine;
use http::Uri;
use prost_reflect::{DynamicMessage, MethodDescriptor};
use snafu::ResultExt;
use tokio::sync::RwLock;
use tracing::info;

use super::cache::CachedPrincipalContext;
use super::config::{
    DEFAULT_TOKEN_LIFETIME, LoginServiceAuthConfig, MIN_TOKEN_VALIDITY, PC_METHOD_NAME,
    PC_SERVICE_NAME,
};
use super::error::{InvalidLoginEndpointSnafu, TokenManagerError, TokenTooShortLivedSnafu};
use super::request::build_request_message;
use super::response::{message_field_bytes, principal_context_expiry};
use super::transport::{
    GrpcHttpClient, build_hyper_openssl_client, grpc_method_uri, grpc_unary_call,
    load_descriptor_pool,
};

/// Manages the principal-context lifecycle: bootstrap and re-bootstrap on expiry.
///
/// Built on `prost_reflect::DynamicMessage` against a runtime-loaded `FileDescriptorSet`
/// — there's no compile-time proto codegen for the principal-context service, so vector
/// doesn't carry a snapshot of universe protos. The wire format is identical to a
/// tonic-generated client; only the message types are dynamic.
///
/// Each bootstrap issues two `CreatePrincipalContext` calls: an unsigned one whose
/// `principal_context` UC reads, and a signed one whose `signed_principal_context` wrapper
/// Zerobus verifies. Neither call returns a bearer token.
pub struct TokenManager {
    config: LoginServiceAuthConfig,
    method: MethodDescriptor,
    client: GrpcHttpClient,
    endpoint: Uri,
    context: Arc<RwLock<Option<Arc<CachedPrincipalContext>>>>,
}

impl TokenManager {
    /// Create a new TokenManager. Loads the principal-context proto descriptor from disk and
    /// builds the hyper-openssl client used for every bootstrap call.
    ///
    /// One manager (holding one cached context pair) exists per LoginService
    /// `databricks_zerobus` sink, shared across that sink's Tower service clones. The contexts
    /// are not reusable across sinks: they are minted from `(workspace_id, service principal,
    /// UC permissions, metastore)`, so distinct sinks (distinct table / workspace / UC)
    /// inherently need distinct contexts. The descriptor pool + mTLS client built here are
    /// identity-independent and could in principle be shared if a process ever ran many
    /// LoginService sinks, but that is a one-time startup cost (not per-request) and not worth
    /// optimizing while only a single sink is configured.
    pub async fn new(config: LoginServiceAuthConfig) -> Result<Self, TokenManagerError> {
        let pool = load_descriptor_pool(&config.login_service.proto_descriptor_path).await?;
        let svc = pool.get_service_by_name(PC_SERVICE_NAME).ok_or_else(|| {
            TokenManagerError::ServiceNotFound {
                service: PC_SERVICE_NAME.into(),
                path: config.login_service.proto_descriptor_path.clone(),
            }
        })?;
        let method = svc
            .methods()
            .find(|m| m.name() == PC_METHOD_NAME)
            .ok_or_else(|| TokenManagerError::MethodNotFound {
                method: PC_METHOD_NAME.into(),
                service: PC_SERVICE_NAME.into(),
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

        Ok(Self {
            config,
            method,
            client,
            endpoint,
            context: Arc::new(RwLock::new(None)),
        })
    }

    /// Bootstrap: call `CreatePrincipalContext` unsigned (for UC) and signed (for Zerobus)
    /// over hyper-openssl and return the cached pair.
    async fn bootstrap(&self) -> Result<CachedPrincipalContext, TokenManagerError> {
        info!(
            message = "Bootstrapping principal context from Login service.",
            workspace_id = self.config.user.workspace_id,
            service_principal_resource = %self.config.user.service_principal_resource,
            service_principal_type = self.config.user.service_principal_type,
        );

        let unsigned = self.create_principal_context(false).await?;
        let signed = self.create_principal_context(true).await?;

        // Both contexts are minted together; serve the pair until the earlier one expires.
        let expires_at = [&unsigned, &signed]
            .into_iter()
            .filter_map(principal_context_expiry)
            .min()
            .unwrap_or_else(|| Instant::now() + DEFAULT_TOKEN_LIFETIME);
        let cached = CachedPrincipalContext {
            principal_context: message_field_bytes(&unsigned, "principal_context")?,
            signed_principal_context: message_field_bytes(&signed, "signed_principal_context")?,
            expires_at,
        };

        // Reject contexts too short-lived to use: serving them risks expiry mid-write to
        // Zerobus, which is costlier than failing the bootstrap and retrying. The 50-min
        // DEFAULT_TOKEN_LIFETIME fallback always clears this floor, so an unset expiration
        // is unaffected.
        let now = Instant::now();
        if !cached.has_remaining_lifetime(now, MIN_TOKEN_VALIDITY) {
            return TokenTooShortLivedSnafu {
                ttl_secs: cached.expires_at.saturating_duration_since(now).as_secs(),
                min_secs: MIN_TOKEN_VALIDITY.as_secs(),
            }
            .fail();
        }

        info!(
            message = "Principal context bootstrap successful.",
            expires_in_secs = cached.expires_at.saturating_duration_since(now).as_secs(),
        );

        Ok(cached)
    }

    async fn create_principal_context(
        &self,
        signed: bool,
    ) -> Result<DynamicMessage, TokenManagerError> {
        let request_msg = build_request_message(&self.config, &self.method.input(), signed)?;
        grpc_unary_call(
            &self.client,
            grpc_method_uri(&self.endpoint, &self.method)?,
            &[],
            &request_msg,
            &self.method.output(),
        )
        .await
    }

    /// Get valid principal contexts, re-bootstrapping if near expiry.
    async fn get_principal_context(
        &self,
    ) -> Result<Arc<CachedPrincipalContext>, TokenManagerError> {
        // Fast path: check if we have valid contexts.
        {
            let guard = self.context.read().await;
            if let Some(ref cached) = *guard
                && cached.is_valid_at(Instant::now())
            {
                return Ok(Arc::clone(cached));
            }
        }

        // Slow path: need to re-bootstrap.
        let mut guard = self.context.write().await;

        // Double-check after acquiring write lock.
        if let Some(ref cached) = *guard
            && cached.is_valid_at(Instant::now())
        {
            return Ok(Arc::clone(cached));
        }

        let cached = Arc::new(self.bootstrap().await?);
        *guard = Some(Arc::clone(&cached));
        Ok(cached)
    }

    /// Base64 of the signed principal context, for Zerobus's `s2s-principal-context-sig-bin`.
    pub async fn get_signed_principal_context(&self) -> Result<String, TokenManagerError> {
        let cached = self.get_principal_context().await?;
        Ok(base64::engine::general_purpose::STANDARD_NO_PAD
            .encode(&cached.signed_principal_context))
    }

    /// Serialized unsigned principal context for internal Unity Catalog requests.
    pub async fn get_unsigned_principal_context(&self) -> Result<Vec<u8>, TokenManagerError> {
        let cached = self.get_principal_context().await?;
        Ok(cached.principal_context.clone())
    }
}
