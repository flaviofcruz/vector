#![allow(missing_docs)]
use std::{
    fs,
    path::PathBuf,
    sync::{Arc, LazyLock, RwLock},
    time::Duration,
};

use base64::prelude::{BASE64_URL_SAFE, Engine as _};
use chrono::{DateTime, Utc};
pub use goauth::scopes::Scope;
use goauth::{
    GoErr,
    auth::{JwtClaims, Token, TokenErr},
    credentials::Credentials,
};
use http::{StatusCode, Uri, uri::PathAndQuery};
use http_body::{Body as _, Collected};
use hyper::{Body, header::AUTHORIZATION};
use serde::Deserialize;
use smpl_jwt::Jwt;
use snafu::{ResultExt, Snafu};
use tokio::sync::watch;
use vector_lib::{configurable::configurable_component, sensitive_string::SensitiveString};

use crate::{
    config::ProxyConfig,
    http::{HttpClient, HttpError},
};

const SERVICE_ACCOUNT_TOKEN_URL: &str =
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";

// See https://cloud.google.com/compute/docs/access/authenticate-workloads#applications
const METADATA_TOKEN_EXPIRY_MARGIN_SECS: u64 = 200;

const METADATA_TOKEN_ERROR_RETRY_SECS: u64 = 2;

const EXTERNAL_ACCOUNT_TOKEN_EXCHANGE_GRANT_TYPE: &str =
    "urn:ietf:params:oauth:grant-type:token-exchange";
const EXTERNAL_ACCOUNT_REQUESTED_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:access_token";
const EXTERNAL_ACCOUNT_IMPERSONATED_TOKEN_LIFETIME_SECS: u32 = 3600;

pub const PUBSUB_URL: &str = "https://pubsub.googleapis.com";

pub static PUBSUB_ADDRESS: LazyLock<String> = LazyLock::new(|| {
    std::env::var("EMULATOR_ADDRESS").unwrap_or_else(|_| "http://localhost:8681".into())
});

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum GcpError {
    #[snafu(display("This requires one of api_key or credentials_path to be defined"))]
    MissingAuth,
    #[snafu(display("Invalid GCP credentials: {}", source))]
    InvalidCredentials { source: GoErr },
    #[snafu(display("Invalid GCP API key: {}", source))]
    InvalidApiKey { source: base64::DecodeError },
    #[snafu(display("Healthcheck endpoint forbidden"))]
    HealthcheckForbidden,
    #[snafu(display("Invalid RSA key in GCP credentials: {}", source))]
    InvalidRsaKey { source: GoErr },
    #[snafu(display("Failed to get OAuth token: {}", source))]
    GetToken { source: GoErr },
    #[snafu(display("Failed to get OAuth token text: {}", source))]
    GetTokenBytes { source: hyper::Error },
    #[snafu(display("Failed to get implicit GCP token: {}", source))]
    GetImplicitToken { source: HttpError },
    #[snafu(display("Failed to parse OAuth token JSON: {}", source))]
    TokenFromJson { source: TokenErr },
    #[snafu(display("Failed to parse OAuth token JSON text: {}", source))]
    TokenJsonFromStr { source: serde_json::Error },
    #[snafu(display("Failed to build HTTP client: {}", source))]
    BuildHttpClient { source: HttpError },
    #[snafu(display("Failed to read external account subject token from {path:?}: {source}"))]
    ReadExternalAccountSubjectToken {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("External account subject token file {path:?} was empty"))]
    EmptyExternalAccountSubjectToken { path: PathBuf },
    #[snafu(display("Invalid external account credentials: {source}"))]
    InvalidExternalAccountCredentials { source: serde_json::Error },
    #[snafu(display("Failed to build external account {operation} request: {source}"))]
    BuildExternalAccountRequest {
        operation: &'static str,
        source: http::Error,
    },
    #[snafu(display("Failed to send external account {operation} request: {source}"))]
    SendExternalAccountRequest {
        operation: &'static str,
        source: HttpError,
    },
    #[snafu(display("Failed to read external account {operation} response: {source}"))]
    ReadExternalAccountResponse {
        operation: &'static str,
        source: hyper::Error,
    },
    #[snafu(display("External account {operation} request failed with HTTP status {status}"))]
    ExternalAccountResponseStatus {
        operation: &'static str,
        status: StatusCode,
    },
    #[snafu(display("Invalid external account {operation} response: {source}"))]
    InvalidExternalAccountResponse {
        operation: &'static str,
        source: serde_json::Error,
    },
}

/// Credentials in Google's external account format.
///
/// This is the format emitted by `gcloud iam workload-identity-pools create-cred-config` and
/// consumed by Application Default Credentials for workload identity federation. The `file`
/// credential source covers the Kubernetes projected-token flow while deliberately excluding
/// executable and URL credential sources, which have materially different security properties.
#[derive(Clone, Debug, Deserialize)]
struct ExternalAccountCredentials {
    audience: String,
    subject_token_type: String,
    token_url: String,
    #[serde(default)]
    service_account_impersonation_url: Option<String>,
    #[serde(default)]
    service_account_impersonation: Option<ExternalAccountServiceAccountImpersonation>,
    credential_source: ExternalAccountCredentialSource,
}

#[derive(Clone, Debug, Deserialize)]
struct ExternalAccountServiceAccountImpersonation {
    token_lifetime_seconds: Option<u32>,
}

#[derive(Deserialize)]
struct CredentialFileType {
    #[serde(rename = "type")]
    credential_type: String,
}

#[derive(Clone, Debug, Deserialize)]
struct ExternalAccountCredentialSource {
    file: PathBuf,
}

#[derive(Clone, Deserialize)]
struct ExternalAccountToken {
    access_token: String,
    token_type: String,
    expires_in: u32,
}

impl std::fmt::Debug for ExternalAccountToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExternalAccountToken")
            .field("token_type", &self.token_type)
            .field("expires_in", &self.expires_in)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
struct ImpersonatedServiceAccountToken {
    #[serde(rename = "accessToken")]
    access_token: String,
    #[serde(rename = "expireTime")]
    expires_at: DateTime<Utc>,
}

pub struct ExternalAccountAuthenticator {
    credentials: ExternalAccountCredentials,
    scope: Scope,
    token: RwLock<ExternalAccountToken>,
}

impl std::fmt::Debug for ExternalAccountAuthenticator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExternalAccountAuthenticator")
            .field("credentials", &self.credentials)
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

/// Configuration of the authentication strategy for interacting with GCP services.
// TODO: We're duplicating the "either this or that" verbiage for each field because this struct gets flattened into the
// component config types, which means all that's carried over are the fields, not the type itself.
//
// Seems like we really really have it as a nested field -- i.e. `auth.api_key` -- which is a closer fit to how we do
// similar things in configuration (TLS, framing, decoding, etc.). Doing so would let us embed the type itself, and
// hoist up the common documentation bits to the docs for the type rather than the fields.
#[configurable_component]
#[derive(Clone, Debug, Default)]
pub struct GcpAuthConfig {
    /// An [API key][gcp_api_key].
    ///
    /// Either an API key or a path to a service-account or external-account credentials JSON file
    /// can be specified.
    ///
    /// If both are unset, the `GOOGLE_APPLICATION_CREDENTIALS` environment variable is checked for a filename. If no
    /// filename is named, an attempt is made to fetch an instance service account for the compute instance the program is
    /// running on. If this is not on a GCE instance, then you must define it with an API key or
    /// credentials JSON file.
    ///
    /// [gcp_api_key]: https://cloud.google.com/docs/authentication/api-keys
    pub api_key: Option<SensitiveString>,

    /// Path to a [service account][gcp_service_account_credentials] or
    /// [external account][gcp_external_account_credentials] credentials JSON file.
    ///
    /// Either an API key or a path to a service-account or external-account credentials JSON file
    /// can be specified.
    ///
    /// If both are unset, the `GOOGLE_APPLICATION_CREDENTIALS` environment variable is checked for a filename. If no
    /// filename is named, an attempt is made to fetch an instance service account for the compute instance the program is
    /// running on. If this is not on a GCE instance, then you must define it with an API key or
    /// credentials JSON file.
    ///
    /// [gcp_service_account_credentials]: https://cloud.google.com/docs/authentication/production#manually
    /// [gcp_external_account_credentials]: https://cloud.google.com/iam/docs/workload-identity-federation-with-deployment-pipelines#file-sourced-credentials
    pub credentials_path: Option<String>,

    /// A short-lived OAuth2 bearer token to use for authentication.
    ///
    /// Intended for use with Vector's file or exec secret backend, where the token is read
    /// from a file on disk and refreshed automatically via `files_to_watch` and `--watch-config`.
    /// The token value should be the raw access token string (without the "Bearer " prefix).
    ///
    /// When set, takes precedence over `credentials_path` and `api_key`.
    pub token: Option<SensitiveString>,

    /// Skip all authentication handling. For use with integration tests only.
    #[serde(default, skip_serializing)]
    #[configurable(metadata(docs::hidden))]
    pub skip_authentication: bool,
}

impl GcpAuthConfig {
    pub async fn build(&self, scope: Scope) -> crate::Result<GcpAuthenticator> {
        Ok(if self.skip_authentication {
            GcpAuthenticator::None
        } else if let Some(token) = &self.token {
            GcpAuthenticator::StaticToken(token.inner().to_string())
        } else {
            let gap = std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok();
            let creds_path = self.credentials_path.as_ref().or(gap.as_ref());
            match (&creds_path, &self.api_key) {
                (Some(path), _) => GcpAuthenticator::from_file(path, scope).await?,
                (None, Some(api_key)) => GcpAuthenticator::from_api_key(api_key.inner())?,
                (None, None) => GcpAuthenticator::new_implicit().await?,
            }
        })
    }
}

#[derive(Clone, Debug)]
pub enum GcpAuthenticator {
    Credentials(Arc<InnerCreds>),
    ExternalAccount(Arc<ExternalAccountAuthenticator>),
    ApiKey(Box<str>),
    /// A static bearer token supplied directly in config (e.g. via a secret backend).
    /// Token refresh is handled externally via Vector's `--watch-config` + `files_to_watch`.
    StaticToken(String),
    None,
}

#[derive(Debug)]
pub struct InnerCreds {
    creds: Option<(Credentials, Scope)>,
    token: RwLock<Token>,
}

impl GcpAuthenticator {
    async fn from_file(path: &str, scope: Scope) -> crate::Result<Self> {
        if let Ok(credential_contents) = fs::read(path)
            && matches!(
                serde_json::from_slice::<CredentialFileType>(&credential_contents),
                Ok(CredentialFileType { credential_type }) if credential_type == "external_account"
            )
        {
            let credentials =
                serde_json::from_slice::<ExternalAccountCredentials>(&credential_contents)
                    .context(InvalidExternalAccountCredentialsSnafu)?;
            return Self::from_external_account(credentials, scope).await;
        }

        let creds = Credentials::from_file(path).context(InvalidCredentialsSnafu)?;
        let token = RwLock::new(fetch_token(&creds, &scope).await?);
        let creds = Some((creds, scope));
        Ok(Self::Credentials(Arc::new(InnerCreds { creds, token })))
    }

    async fn from_external_account(
        credentials: ExternalAccountCredentials,
        scope: Scope,
    ) -> crate::Result<Self> {
        let token = RwLock::new(fetch_external_account_token(&credentials, &scope).await?);
        Ok(Self::ExternalAccount(Arc::new(
            ExternalAccountAuthenticator {
                credentials,
                scope,
                token,
            },
        )))
    }

    async fn new_implicit() -> crate::Result<Self> {
        let token = RwLock::new(get_token_implicit().await?);
        let creds = None;
        Ok(Self::Credentials(Arc::new(InnerCreds { creds, token })))
    }

    fn from_api_key(api_key: &str) -> crate::Result<Self> {
        BASE64_URL_SAFE
            .decode(api_key)
            .context(InvalidApiKeySnafu)?;
        Ok(Self::ApiKey(api_key.into()))
    }

    pub fn make_token(&self) -> Option<String> {
        match self {
            Self::Credentials(inner) => Some(inner.make_token()),
            Self::ExternalAccount(inner) => Some(inner.make_token()),
            Self::StaticToken(token) => Some(format!("Bearer {token}")),
            Self::ApiKey(_) | Self::None => None,
        }
    }

    pub fn apply<T>(&self, request: &mut http::Request<T>) {
        if let Some(token) = self.make_token() {
            request
                .headers_mut()
                .insert(AUTHORIZATION, token.parse().unwrap());
        }
        self.apply_uri(request.uri_mut());
    }

    pub fn apply_uri(&self, uri: &mut Uri) {
        match self {
            Self::Credentials(_) | Self::ExternalAccount(_) | Self::StaticToken(_) | Self::None => {
            }
            Self::ApiKey(api_key) => {
                let mut parts = uri.clone().into_parts();
                let path = parts
                    .path_and_query
                    .as_ref()
                    .map_or("/", PathAndQuery::path);
                let paq = format!("{path}?key={api_key}");
                // The API key is verified above to only contain
                // URL-safe characters. That key is added to a path
                // that came from a successfully parsed URI. As such,
                // re-parsing the string cannot fail.
                parts.path_and_query =
                    Some(paq.parse().expect("Could not re-parse path and query"));
                *uri = Uri::from_parts(parts).expect("Could not re-parse URL");
            }
        }
    }

    pub fn spawn_regenerate_token(&self) -> watch::Receiver<()> {
        let (sender, receiver) = watch::channel(());
        tokio::spawn(self.clone().token_regenerator(sender));
        receiver
    }

    async fn token_regenerator(self, sender: watch::Sender<()>) {
        match self {
            Self::Credentials(inner) => {
                let mut expires_in = inner.token.read().unwrap().expires_in() as u64;
                loop {
                    let deadline = Duration::from_secs(
                        expires_in
                            .saturating_sub(METADATA_TOKEN_EXPIRY_MARGIN_SECS)
                            .max(METADATA_TOKEN_ERROR_RETRY_SECS),
                    );
                    debug!(
                        deadline = deadline.as_secs(),
                        "Sleeping before refreshing GCP authentication token.",
                    );
                    tokio::time::sleep(deadline).await;
                    match inner.regenerate_token().await {
                        Ok(()) => {
                            sender.send_replace(());
                            debug!("GCP authentication token renewed.");
                            // Rather than an expected fresh token, the Metadata Server may return
                            // the same (cached) token during the last 300 seconds of its lifetime.
                            // This scenario is handled by retrying the token refresh after the
                            // METADATA_TOKEN_ERROR_RETRY_SECS period when a fresh token is expected
                            expires_in = inner.token.read().unwrap().expires_in() as u64;
                        }
                        Err(error) => {
                            error!(
                                message = "Failed to update GCP authentication token.",
                                %error
                            );
                            expires_in = METADATA_TOKEN_EXPIRY_MARGIN_SECS;
                        }
                    }
                }
            }
            Self::ExternalAccount(inner) => {
                let mut expires_in = inner.token.read().unwrap().expires_in as u64;
                loop {
                    let deadline = Duration::from_secs(
                        expires_in
                            .saturating_sub(METADATA_TOKEN_EXPIRY_MARGIN_SECS)
                            .max(METADATA_TOKEN_ERROR_RETRY_SECS),
                    );
                    debug!(
                        deadline = deadline.as_secs(),
                        "Sleeping before refreshing GCP external account authentication token.",
                    );
                    tokio::time::sleep(deadline).await;
                    match inner.regenerate_token().await {
                        Ok(()) => {
                            sender.send_replace(());
                            expires_in = inner.token.read().unwrap().expires_in as u64;
                        }
                        Err(error) => {
                            error!(message = "Failed to refresh GCP external account authentication token.", %error);
                            expires_in = METADATA_TOKEN_ERROR_RETRY_SECS;
                        }
                    }
                }
            }
            // Static token and API key don't need internal refresh — token rotation is
            // handled externally via Vector's --watch-config + files_to_watch mechanism.
            Self::StaticToken(_) | Self::ApiKey(_) | Self::None => {
                // This keeps the sender end of the watch open without
                // actually sending anything, effectively creating an
                // empty watch stream.
                sender.closed().await
            }
        }
    }
}

impl InnerCreds {
    async fn regenerate_token(&self) -> crate::Result<()> {
        let token = match &self.creds {
            Some((creds, scope)) => fetch_token(creds, scope).await?,
            None => get_token_implicit().await?,
        };
        *self.token.write().unwrap() = token;
        Ok(())
    }

    fn make_token(&self) -> String {
        let token = self.token.read().unwrap();
        format!("{} {}", token.token_type(), token.access_token())
    }
}

impl ExternalAccountAuthenticator {
    async fn regenerate_token(&self) -> crate::Result<()> {
        let token = fetch_external_account_token(&self.credentials, &self.scope).await?;
        *self.token.write().unwrap() = token;
        Ok(())
    }

    fn make_token(&self) -> String {
        let token = self.token.read().unwrap();
        format!("{} {}", token.token_type, token.access_token)
    }
}

async fn fetch_external_account_token(
    credentials: &ExternalAccountCredentials,
    scope: &Scope,
) -> Result<ExternalAccountToken, GcpError> {
    let subject_token_path = &credentials.credential_source.file;
    let subject_token = fs::read_to_string(subject_token_path).map_err(|source| {
        GcpError::ReadExternalAccountSubjectToken {
            path: subject_token_path.clone(),
            source,
        }
    })?;
    let subject_token = subject_token.trim();
    if subject_token.is_empty() {
        return Err(GcpError::EmptyExternalAccountSubjectToken {
            path: subject_token_path.clone(),
        });
    }

    // The federated token must be authorized to invoke IAM Credentials when it is used to
    // impersonate a service account. Google requires the cloud-platform scope for that call;
    // the requested, narrower scope belongs on the impersonated token instead.
    let token_exchange_scope = if credentials.service_account_impersonation_url.is_some() {
        Scope::CloudPlatform.url()
    } else {
        scope.url()
    };

    let request_body = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("audience", &credentials.audience)
        .append_pair("grant_type", EXTERNAL_ACCOUNT_TOKEN_EXCHANGE_GRANT_TYPE)
        .append_pair(
            "requested_token_type",
            EXTERNAL_ACCOUNT_REQUESTED_TOKEN_TYPE,
        )
        .append_pair("scope", &token_exchange_scope)
        .append_pair("subject_token", subject_token)
        .append_pair("subject_token_type", &credentials.subject_token_type)
        .finish();
    let request = http::Request::post(&credentials.token_url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(Body::from(request_body))
        .map_err(|source| GcpError::BuildExternalAccountRequest {
            operation: "token exchange",
            source,
        })?;
    let response = external_account_http_client()?
        .send(request)
        .await
        .map_err(|source| GcpError::SendExternalAccountRequest {
            operation: "token exchange",
            source,
        })?;
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .map(Collected::to_bytes)
        .map_err(|source| GcpError::ReadExternalAccountResponse {
            operation: "token exchange",
            source,
        })?;
    if !status.is_success() {
        return Err(GcpError::ExternalAccountResponseStatus {
            operation: "token exchange",
            status,
        });
    }
    let token = serde_json::from_slice::<ExternalAccountToken>(&body).map_err(|source| {
        GcpError::InvalidExternalAccountResponse {
            operation: "token exchange",
            source,
        }
    })?;

    match &credentials.service_account_impersonation_url {
        Some(url) => {
            let lifetime_secs = credentials
                .service_account_impersonation
                .as_ref()
                .and_then(|config| config.token_lifetime_seconds)
                .unwrap_or(EXTERNAL_ACCOUNT_IMPERSONATED_TOKEN_LIFETIME_SECS);
            impersonate_service_account(url, token, scope, lifetime_secs).await
        }
        None => Ok(token),
    }
}

fn external_account_http_client() -> Result<HttpClient, GcpError> {
    HttpClient::new(None, &ProxyConfig::from_env())
        .map_err(|source| GcpError::BuildHttpClient { source })
}

async fn impersonate_service_account(
    url: &str,
    token: ExternalAccountToken,
    scope: &Scope,
    lifetime_secs: u32,
) -> Result<ExternalAccountToken, GcpError> {
    let request_body = serde_json::json!({
        "lifetime": format!("{lifetime_secs}s"),
        "scope": [scope.url()],
    });
    let request = http::Request::post(url)
        .header(
            "Authorization",
            format!("{} {}", token.token_type, token.access_token),
        )
        .header("Content-Type", "application/json")
        .body(Body::from(request_body.to_string()))
        .map_err(|source| GcpError::BuildExternalAccountRequest {
            operation: "service account impersonation",
            source,
        })?;
    let response = external_account_http_client()?
        .send(request)
        .await
        .map_err(|source| GcpError::SendExternalAccountRequest {
            operation: "service account impersonation",
            source,
        })?;
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .map(Collected::to_bytes)
        .map_err(|source| GcpError::ReadExternalAccountResponse {
            operation: "service account impersonation",
            source,
        })?;
    if !status.is_success() {
        return Err(GcpError::ExternalAccountResponseStatus {
            operation: "service account impersonation",
            status,
        });
    }
    let token =
        serde_json::from_slice::<ImpersonatedServiceAccountToken>(&body).map_err(|source| {
            GcpError::InvalidExternalAccountResponse {
                operation: "service account impersonation",
                source,
            }
        })?;
    Ok(ExternalAccountToken {
        access_token: token.access_token,
        token_type: "Bearer".to_string(),
        expires_in: remaining_token_lifetime_secs(token.expires_at, Utc::now()),
    })
}

fn remaining_token_lifetime_secs(expires_at: DateTime<Utc>, now: DateTime<Utc>) -> u32 {
    let remaining_secs = expires_at.signed_duration_since(now).num_seconds().max(0);
    u32::try_from(remaining_secs).unwrap_or(u32::MAX)
}

async fn fetch_token(creds: &Credentials, scope: &Scope) -> crate::Result<Token> {
    let claims = JwtClaims::new(
        creds.iss(),
        std::slice::from_ref(scope),
        creds.token_uri(),
        None,
        None,
    );
    let rsa_key = creds.rsa_key().context(InvalidRsaKeySnafu)?;
    let jwt = Jwt::new(claims, rsa_key, None);

    debug!(
        message = "Fetching GCP authentication token.",
        project = ?creds.project(),
        iss = ?creds.iss(),
        token_uri = ?creds.token_uri(),
    );
    goauth::get_token(&jwt, creds)
        .await
        .context(GetTokenSnafu)
        .map_err(Into::into)
}

async fn get_token_implicit() -> Result<Token, GcpError> {
    debug!("Fetching implicit GCP authentication token.");
    let req = http::Request::get(SERVICE_ACCOUNT_TOKEN_URL)
        .header("Metadata-Flavor", "Google")
        .body(hyper::Body::empty())
        .unwrap();

    let proxy = ProxyConfig::from_env();
    let res = HttpClient::new(None, &proxy)
        .context(BuildHttpClientSnafu)?
        .send(req)
        .await
        .context(GetImplicitTokenSnafu)?;

    let body = res.into_body();
    let bytes = body
        .collect()
        .await
        .map(Collected::to_bytes)
        .context(GetTokenBytesSnafu)?;

    // Token::from_str is irresponsible and may panic!
    match serde_json::from_slice::<Token>(&bytes) {
        Ok(token) => Ok(token),
        Err(error) => Err(match serde_json::from_slice::<TokenErr>(&bytes) {
            Ok(error) => GcpError::TokenFromJson { source: error },
            Err(_) => GcpError::TokenJsonFromStr { source: error },
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::assert_downcast_matches;
    use tempfile::tempdir;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_string_contains, header, method, path},
    };

    #[tokio::test]
    async fn fails_missing_creds() {
        let error = build_auth("").await.expect_err("build failed to error");
        assert_downcast_matches!(error, GcpError, GcpError::GetImplicitToken { .. });
        // This should be a more relevant error
    }

    #[tokio::test]
    async fn skip_authentication() {
        let auth = build_auth(
            r#"
                skip_authentication = true
                api_key = "testing"
            "#,
        )
        .await
        .expect("build_auth failed");
        assert!(matches!(auth, GcpAuthenticator::None));
    }

    #[tokio::test]
    async fn uses_api_key() {
        let key = crate::test_util::random_string(16);

        let auth = build_auth(&format!(r#"api_key = "{key}""#))
            .await
            .expect("build_auth failed");
        assert!(matches!(auth, GcpAuthenticator::ApiKey(..)));

        assert_eq!(
            apply_uri(&auth, "http://example.com"),
            format!("http://example.com/?key={key}")
        );
        assert_eq!(
            apply_uri(&auth, "http://example.com/"),
            format!("http://example.com/?key={key}")
        );
        assert_eq!(
            apply_uri(&auth, "http://example.com/path"),
            format!("http://example.com/path?key={key}")
        );
        assert_eq!(
            apply_uri(&auth, "http://example.com/path1/"),
            format!("http://example.com/path1/?key={key}")
        );
    }

    #[tokio::test]
    async fn fails_bad_api_key() {
        let error = build_auth(r#"api_key = "abc%xyz""#)
            .await
            .expect_err("build failed to error");
        assert_downcast_matches!(error, GcpError, GcpError::InvalidApiKey { .. });
    }

    #[tokio::test]
    async fn uses_static_token() {
        let auth = build_auth(r#"token = "ya29.test-token-value""#)
            .await
            .expect("build_auth failed");
        assert!(matches!(auth, GcpAuthenticator::StaticToken(..)));

        // Token is prefixed with "Bearer " in the Authorization header value.
        assert_eq!(
            auth.make_token(),
            Some("Bearer ya29.test-token-value".to_string())
        );

        // apply_uri must not modify the URI (unlike ApiKey which appends ?key=...).
        assert_eq!(
            apply_uri(&auth, "http://example.com/path"),
            "http://example.com/path"
        );
    }

    #[tokio::test]
    async fn static_token_takes_precedence_over_api_key() {
        // When both token and api_key are set, token wins.
        let key = crate::test_util::random_string(16);
        let auth = build_auth(&format!(
            r#"
                token = "ya29.test-token-value"
                api_key = "{key}"
            "#
        ))
        .await
        .expect("build_auth failed");
        assert!(matches!(auth, GcpAuthenticator::StaticToken(..)));
    }

    #[tokio::test]
    async fn external_account_credentials_exchange_and_impersonate() {
        let server = MockServer::start().await;
        let granted_token_lifetime_secs = 300;
        let expires_at =
            (Utc::now() + chrono::Duration::seconds(granted_token_lifetime_secs)).to_rfc3339();
        Mock::given(method("POST"))
            .and(path("/v1/token"))
            .and(header("content-type", "application/x-www-form-urlencoded"))
            .and(body_string_contains(
                "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange",
            ))
            .and(body_string_contains(
                "scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fcloud-platform",
            ))
            .and(body_string_contains("subject_token=projected-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "federated-token",
                "token_type": "Bearer",
                "expires_in": 600,
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/projects/-/serviceAccounts/log-collection-sa@example.iam.gserviceaccount.com:generateAccessToken"))
            .and(header("authorization", "Bearer federated-token"))
            .and(header("content-type", "application/json"))
            .and(body_string_contains("https://www.googleapis.com/auth/devstorage.read_write"))
            .and(body_string_contains(r#""lifetime":"600s""#))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "accessToken": "impersonated-token",
                "expireTime": expires_at,
            })))
            .expect(1)
            .mount(&server)
            .await;

        let directory = tempdir().expect("temporary directory");
        let subject_token_path = directory.path().join("subject-token");
        fs::write(&subject_token_path, "projected-token\n").expect("write subject token");
        let credentials_path = directory.path().join("external-account.json");
        fs::write(
            &credentials_path,
            serde_json::json!({
                "type": "external_account",
                "audience": "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
                "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
                "token_url": format!("{}/v1/token", server.uri()),
                "service_account_impersonation_url": format!(
                    "{}/v1/projects/-/serviceAccounts/log-collection-sa@example.iam.gserviceaccount.com:generateAccessToken",
                    server.uri(),
                ),
                "service_account_impersonation": { "token_lifetime_seconds": 600 },
                "credential_source": { "file": subject_token_path },
            })
            .to_string(),
        )
        .expect("write external account credentials");

        let auth = GcpAuthenticator::from_file(
            credentials_path.to_str().expect("UTF-8 path"),
            Scope::DevStorageReadWrite,
        )
        .await
        .expect("external account authentication should build");

        assert!(matches!(auth, GcpAuthenticator::ExternalAccount(..)));
        assert_eq!(
            auth.make_token(),
            Some("Bearer impersonated-token".to_string())
        );
        let GcpAuthenticator::ExternalAccount(inner) = &auth else {
            panic!("expected external-account authenticator");
        };
        assert!(
            inner.token.read().unwrap().expires_in <= granted_token_lifetime_secs as u32,
            "the refresh deadline must use Google's returned expireTime, not the requested lifetime"
        );
        assert!(
            inner.token.read().unwrap().expires_in > granted_token_lifetime_secs as u32 - 20,
            "the granted lifetime should remain close to the returned expiration"
        );
        server.verify().await;
    }

    #[test]
    fn remaining_token_lifetime_clamps_expired_tokens_to_zero() {
        let now = DateTime::parse_from_rfc3339("2026-08-26T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let expires_at = now - chrono::Duration::seconds(1);

        assert_eq!(remaining_token_lifetime_secs(expires_at, now), 0);
    }

    #[test]
    fn remaining_token_lifetime_uses_returned_expiration() {
        let now = DateTime::parse_from_rfc3339("2026-08-26T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let expires_at = now + chrono::Duration::seconds(300);

        assert_eq!(remaining_token_lifetime_secs(expires_at, now), 300);
    }

    #[tokio::test]
    async fn external_account_credentials_without_impersonation_use_sts_token() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/token"))
            .and(body_string_contains(
                "scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fdevstorage.read_write",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "sts-token",
                "token_type": "Bearer",
                "expires_in": 600,
            })))
            .expect(1)
            .mount(&server)
            .await;

        let directory = tempdir().expect("temporary directory");
        let subject_token_path = directory.path().join("subject-token");
        fs::write(&subject_token_path, "projected-token").expect("write subject token");
        let credentials_path = directory.path().join("external-account.json");
        fs::write(
            &credentials_path,
            serde_json::json!({
                "type": "external_account",
                "audience": "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
                "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
                "token_url": format!("{}/v1/token", server.uri()),
                "credential_source": { "file": subject_token_path },
            })
            .to_string(),
        )
        .expect("write external account credentials");

        let auth = GcpAuthenticator::from_file(
            credentials_path.to_str().expect("UTF-8 path"),
            Scope::DevStorageReadWrite,
        )
        .await
        .expect("external account authentication should build");

        assert_eq!(auth.make_token(), Some("Bearer sts-token".to_string()));
        server.verify().await;
    }

    fn apply_uri(auth: &GcpAuthenticator, uri: &str) -> String {
        let mut uri: Uri = uri.parse().unwrap();
        auth.apply_uri(&mut uri);
        uri.to_string()
    }

    async fn build_auth(toml: &str) -> crate::Result<GcpAuthenticator> {
        let config: GcpAuthConfig = toml::from_str(toml).expect("Invalid TOML");
        config.build(Scope::Compute).await
    }
}
