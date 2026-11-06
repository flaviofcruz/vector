//! Databricks Login service authentication for logging-agent.
//!
//! Mints and refreshes signed and unsigned principal contexts via the Login service's
//! `PrincipalContextService.CreatePrincipalContext` RPC.

mod headers_provider;
mod token_manager;

pub use headers_provider::LoginServiceHeadersProvider;
pub use token_manager::{
    LoginServiceAuthConfig, LoginServiceConfig, ServiceTlsConfig, TokenManager, TokenManagerError,
    UnityCatalogServiceConfig, UserAuthInfo,
};

pub(crate) use token_manager::{
    GrpcHttpClient, build_hyper_openssl_client, grpc_method_uri, grpc_unary_call,
    load_descriptor_pool,
};

pub(crate) use token_manager::PrincipalContextMetadata;
