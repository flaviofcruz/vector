//! Configuration types for the Login OAuth `TokenManager`.
//!
//! # Duplication note
//!
//! A parallel implementation of this auth module exists in
//! `bricklens/bricklens-ingest-external/src/databricks_auth/` in universe.
//! The struct shapes (`UserAuthInfo`, `LoginServiceConfig`, `UcPermissionEntry`)
//! are intentionally kept in sync, but some fields differ for architectural
//! reasons — vector is config-file driven so `LoginServiceConfig` carries
//! `login_endpoint`, `login_server_name`, and `oauth_proto_descriptor_path`,
//! while the universe side derives those from DB_CONF and compile-time generated
//! protos.
//!
//! Once vector is vendored into universe the two implementations should be
//! consolidated into a single shared crate. At that point there may be conflicts
//! to resolve between vector's needs (config-file deployment, runtime proto
//! loading, hyper transport) and universe's needs (DB_CONF, generated protos,
//! raf-client transport). The duplication is intentional for now to keep both
//! services unblocked.

use std::path::PathBuf;
use std::time::Duration;

use vector_lib::configurable::configurable_component;

/// Fully-qualified name of the Login OAuth service. Looked up in the runtime-loaded
/// `DescriptorPool` to resolve the request/response message types.
pub(super) const OAUTH_SERVICE_NAME: &str =
    "databricks.common.authentication.oauth.InternalOAuthTokenService";
pub(super) const OAUTH_METHOD_NAME: &str =
    "GenerateReauthenticatedAndReauthorizedInternalOAuthToken";

/// Default token lifetime when the JWT `exp` claim cannot be parsed.
pub(super) const DEFAULT_TOKEN_LIFETIME: Duration = Duration::from_secs(50 * 60);

/// Re-bootstrap a cached token once its remaining TTL drops below this threshold.
/// With a 50-minute default lifetime, tokens are refreshed at the 45-minute mark.
pub(super) const TOKEN_REFRESH_THRESHOLD: Duration = Duration::from_secs(5 * 60);

/// A freshly bootstrapped token with less than this much remaining lifetime is rejected
/// rather than served: serving it would risk the token expiring mid-write to Zerobus.
/// Distinct from `TOKEN_REFRESH_THRESHOLD` (proactive refresh of an already-cached token) —
/// this is a backstop against Login minting a token too short-lived to use at all.
pub(super) const MIN_TOKEN_VALIDITY: Duration = Duration::from_secs(60);

/// `databricks.common.authentication.identity.ServicePrincipalType::SYSTEM` enum value.
/// Hardcoded because the descriptor pool is loaded at runtime and the enum is not
/// generated locally.
///
/// TODO(universe port): replace with the generated
/// `universe_central_api_users_service_principal_type_proto::users::ServicePrincipalType::System`
/// enum and its `as i32` cast, matching `bricklens-ingest-external/src/databricks_auth/config.rs`.
pub(super) const SERVICE_PRINCIPAL_TYPE_SYSTEM: i32 = 3;

/// One UC permission entry: a set of privileges on a single securable.
///
/// The bricklens-agent flow currently sends 5 entries (USE_CATALOG on the catalog,
/// USE_SCHEMA on the schema, SELECT+MODIFY on each of the 3 otel tables) per the
/// apps SSP `ManageOtelTables` policy.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct UcPermissionEntry {
    /// UC privilege names (e.g., `USE_CATALOG`, `USE_SCHEMA`, `SELECT`, `MODIFY`).
    pub privileges: Vec<String>,

    /// Securable type: `CATALOG` | `SCHEMA` | `TABLE`.
    pub securable_type: String,

    /// Full name of the securable (e.g., `main.default.otel_logs`).
    pub full_name: String,
}

/// Per-customer identity for the Login service OAuth request.
///
/// Mirrors `bricklens-ingest-external/src/databricks_auth/config.rs::UserAuthInfo`
/// in universe. In universe, `service_principal_type` is the typed proto enum
/// `ServicePrincipalType`; here it's `i32` because the enum is not generated locally
/// (see the TODO on `SERVICE_PRINCIPAL_TYPE_SYSTEM`).
#[configurable_component]
#[derive(Clone, Debug)]
pub struct UserAuthInfo {
    /// Workspace ID this agent's UWI cert is bound to. Used as `Authentication.scope.workspace`
    /// when minting the JWT. For nimbus pods, this matches the workspace ID encoded in the
    /// cert SAN OID `1.3.6.1.4.1.42.113.1`.
    ///
    /// Eventually this should come from `bricklens-config-service`; for now the deploy
    /// template renders it per-pod.
    pub workspace_id: i64,

    /// SSP resource name (e.g., `accounts/<account-uuid>/apps`).
    pub service_principal_resource: String,

    /// Service principal type (default: SYSTEM = 3).
    #[serde(default = "default_sp_type")]
    pub service_principal_type: i32,

    /// UC permission entries to include in `AuthorizationConstraint`.
    #[serde(default)]
    pub uc_permissions: Vec<UcPermissionEntry>,
}

/// Connection parameters for the Login service.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct LoginServiceConfig {
    /// Login service endpoint via s2s-proxy (e.g., `https://login-service-bricklens.privileged.dev.dbns.databricks.com:443`).
    pub login_endpoint: String,

    /// TLS server name / SNI for Login service DBNS hostname.
    pub login_server_name: String,

    /// Path to TLS client certificate file (mTLS).
    pub tls_crt_file: String,

    /// Path to TLS client key file (mTLS).
    pub tls_key_file: String,

    /// Optional path to CA certificate for verifying the server.
    #[serde(default)]
    pub tls_ca_file: Option<String>,

    /// Path to a protobuf `FileDescriptorSet` covering Login's
    /// `InternalOAuthTokenService.GenerateReauthenticatedAndReauthorizedInternalOAuthToken`
    /// (and its transitively-referenced messages from `databricks.identity`).
    ///
    /// Generated with:
    /// ```sh
    /// protoc --descriptor_set_out=oauth_service.pb --include_imports oauth_service.proto
    /// ```
    ///
    /// Mounted into the agent container alongside the bricklens_ingest service descriptor.
    /// Loading the descriptor at runtime keeps vector decoupled from a snapshot of
    /// universe protos. The same precedent is in use by the `bricklens_ingest` sink.
    pub oauth_proto_descriptor_path: PathBuf,
}

/// Configuration for Login service authentication.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct LoginServiceAuthConfig {
    /// Per-customer identity used when minting the JWT.
    pub user: UserAuthInfo,

    /// Login service connection parameters.
    pub login_service: LoginServiceConfig,
}

fn default_sp_type() -> i32 {
    SERVICE_PRINCIPAL_TYPE_SYSTEM
}
