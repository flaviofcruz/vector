use std::path::PathBuf;

use snafu::Snafu;

/// gRPC status codes treated as transient by [`TokenManagerError::is_retryable`].
const GRPC_UNKNOWN: i32 = 2;
const GRPC_DEADLINE_EXCEEDED: i32 = 4;
const GRPC_RESOURCE_EXHAUSTED: i32 = 8;
const GRPC_ABORTED: i32 = 10;
const GRPC_INTERNAL: i32 = 13;
const GRPC_UNAVAILABLE: i32 = 14;

/// Errors produced while bootstrapping or refreshing an OAuth token through the
/// Databricks Login service.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(super)))]
pub enum TokenManagerError {
    /// Reading a proto descriptor file from disk failed.
    #[snafu(display("read proto descriptor at {}: {}", path.display(), source))]
    ReadDescriptor {
        /// Path to the descriptor file that could not be read.
        path: PathBuf,
        /// Underlying I/O error.
        source: std::io::Error,
    },

    /// The bytes loaded from the descriptor file did not parse as a `FileDescriptorSet`.
    #[snafu(display("decode proto FileDescriptorSet: {}", source))]
    DecodeDescriptorSet {
        /// Underlying protobuf decode error.
        source: prost_reflect::prost::DecodeError,
    },

    /// The decoded `FileDescriptorSet` was structurally invalid (e.g. dangling type refs).
    #[snafu(display("build proto DescriptorPool: {}", source))]
    BuildDescriptorPool {
        /// Underlying descriptor-pool error.
        source: prost_reflect::DescriptorError,
    },

    /// The configured OAuth service name was not present in the descriptor pool.
    #[snafu(display(
        "OAuth service `{}` not found in descriptor at {}",
        service,
        path.display()
    ))]
    OauthServiceNotFound {
        /// Fully-qualified service name that was looked up.
        service: String,
        /// Descriptor file the service was expected to live in.
        path: PathBuf,
    },

    /// The configured OAuth method name was not present on the resolved service.
    #[snafu(display("OAuth method `{}` not found in service `{}`", method, service))]
    OauthMethodNotFound {
        /// Method name that was looked up.
        method: String,
        /// Service the method was expected to live in.
        service: String,
    },

    /// `login_endpoint` from the config did not parse as an HTTP URI.
    #[snafu(display("invalid login_endpoint `{}`: {}", uri, source))]
    InvalidLoginEndpoint {
        /// The endpoint string that failed to parse.
        uri: String,
        /// Underlying URI parse error.
        source: http::uri::InvalidUri,
    },

    /// The derived gRPC request URI (endpoint + path) did not parse as an HTTP URI.
    #[snafu(display("invalid gRPC URI `{}`: {}", uri, source))]
    InvalidGrpcUri {
        /// The constructed URI string that failed to parse.
        uri: String,
        /// Underlying URI parse error.
        source: http::uri::InvalidUri,
    },

    /// Serializing the JSON request body into a `DynamicMessage` failed.
    /// Typically indicates a config/descriptor mismatch.
    #[snafu(display("build OAuth request from descriptor: {}", source))]
    BuildRequest {
        /// Underlying serde-JSON error.
        source: serde_json::Error,
    },

    /// `http::Request::builder()` rejected the built request.
    #[snafu(display("build gRPC HTTP request: {}", source))]
    BuildGrpcHttpRequest {
        /// Underlying `http` builder error.
        source: http::Error,
    },

    /// The hyper transport failed while issuing or reading the request.
    #[snafu(display("gRPC request failed: {}", source))]
    Transport {
        /// Underlying hyper error.
        source: hyper::Error,
    },

    /// The response carried a non-zero gRPC status.
    #[snafu(display("gRPC error {}: {}", status, message))]
    GrpcStatus {
        /// gRPC status code.
        status: i32,
        /// Server-provided status message.
        message: String,
    },

    /// Reading the response body bytes from hyper failed mid-stream.
    #[snafu(display("read gRPC response body: {}", source))]
    ReadResponseBody {
        /// Underlying hyper error.
        source: hyper::Error,
    },

    /// The response body was shorter than the 5-byte gRPC framing header.
    #[snafu(display("gRPC response too short for framing"))]
    ResponseTooShort,

    /// The gRPC framing indicated a compressed payload, which this client does not support.
    #[snafu(display("compressed gRPC responses not supported (flag = {})", flag))]
    UnexpectedCompression {
        /// Compression flag from the gRPC framing prefix.
        flag: u8,
    },

    /// The response advertised more message bytes than were actually present.
    #[snafu(display("incomplete gRPC response message"))]
    IncompleteResponseMessage,

    /// The framed message bytes failed to decode as the expected response type.
    #[snafu(display("decode gRPC response: {}", source))]
    DecodeResponse {
        /// Underlying protobuf decode error.
        source: prost_reflect::prost::DecodeError,
    },

    /// The response decoded but had no `access_token` field set.
    #[snafu(display("OAuth response missing access_token field"))]
    AccessTokenMissing,

    /// The Login service returned a non-success `state` enum value.
    #[snafu(display("Login service returned state {}: {}", state, message))]
    OauthFailed {
        /// State value reported by Login.
        state: i32,
        /// Optional `error_message` accompanying the failure.
        message: String,
    },

    /// The freshly bootstrapped token expires too soon to use safely — serving it would
    /// risk the token expiring mid-write to Zerobus.
    #[snafu(display(
        "Login minted a token valid for only {}s (minimum {}s)",
        ttl_secs,
        min_secs
    ))]
    TokenTooShortLived {
        /// Remaining lifetime of the minted token, in seconds.
        ttl_secs: u64,
        /// Minimum acceptable lifetime, in seconds.
        min_secs: u64,
    },

    /// Constructing the OpenSSL `SslConnectorBuilder` failed.
    #[snafu(display("create SSL builder: {}", source))]
    SslBuilder {
        /// Underlying OpenSSL error stack.
        source: openssl::error::ErrorStack,
    },

    /// Loading the TLS client certificate from disk failed.
    #[snafu(display("load TLS client cert at {}: {}", path, source))]
    LoadClientCert {
        /// Path that was attempted.
        path: String,
        /// Underlying OpenSSL error stack.
        source: openssl::error::ErrorStack,
    },

    /// Loading the TLS client private key from disk failed.
    #[snafu(display("load TLS client key at {}: {}", path, source))]
    LoadClientKey {
        /// Path that was attempted.
        path: String,
        /// Underlying OpenSSL error stack.
        source: openssl::error::ErrorStack,
    },

    /// Loading the TLS CA bundle from disk failed.
    #[snafu(display("load TLS CA file at {}: {}", path, source))]
    LoadCaFile {
        /// Path that was attempted.
        path: String,
        /// Underlying OpenSSL error stack.
        source: openssl::error::ErrorStack,
    },

    /// Wrapping the HTTP connector with the configured SSL context failed.
    #[snafu(display("create HTTPS connector: {}", source))]
    HttpsConnector {
        /// Underlying OpenSSL error stack.
        source: openssl::error::ErrorStack,
    },

    /// A Unity Catalog schema fetch was requested but `unity_catalog` is not configured.
    #[snafu(display("login_service auth has no `unity_catalog` endpoint configured"))]
    UnityCatalogNotConfigured,

    /// The Unity Catalog `getTable` method was not present in the descriptor.
    #[snafu(display(
        "Unity Catalog method `{}` not found in service `{}` in descriptor at {}",
        method,
        service,
        path.display()
    ))]
    UcMethodNotFound {
        /// Method name that was looked up.
        method: String,
        /// Service the method was expected to live in.
        service: String,
        /// Descriptor file the service was expected to live in.
        path: PathBuf,
    },

    /// `unity_catalog.endpoint` from the config did not parse as an HTTP URI.
    #[snafu(display("invalid unity_catalog endpoint `{}`: {}", uri, source))]
    InvalidUcEndpoint {
        /// The endpoint string that failed to parse.
        uri: String,
        /// Underlying URI parse error.
        source: http::uri::InvalidUri,
    },

    /// The Unity Catalog `getTable` request or response did not have the expected shape.
    #[snafu(display("Unity Catalog getTable: {}", message))]
    UcResponse {
        /// Description of the mismatch.
        message: String,
    },

    /// The Login-minted token did not carry a usable `pctx` principal-context claim.
    #[snafu(display("extract principal context from Login token: {}", message))]
    PrincipalContext {
        /// Why the claim could not be extracted.
        message: String,
    },
}

impl TokenManagerError {
    /// Whether retrying the call that produced this error could succeed.
    ///
    /// Mirrors the HTTP policy of the OAuth schema fetch: transport failures and transient gRPC
    /// statuses retry; configuration, authorization, and malformed-response errors do not, so a
    /// missing table or a denied permission fails fast instead of retrying forever.
    pub const fn is_retryable(&self) -> bool {
        match self {
            Self::GrpcStatus { status, .. } => matches!(
                *status,
                GRPC_UNKNOWN
                    | GRPC_DEADLINE_EXCEEDED
                    | GRPC_RESOURCE_EXHAUSTED
                    | GRPC_ABORTED
                    | GRPC_INTERNAL
                    | GRPC_UNAVAILABLE
            ),
            Self::Transport { .. }
            | Self::ReadResponseBody { .. }
            | Self::ResponseTooShort
            | Self::IncompleteResponseMessage
            | Self::OauthFailed { .. }
            | Self::TokenTooShortLived { .. } => true,
            _ => false,
        }
    }
}
