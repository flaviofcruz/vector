//! Principal-context manager for the bricklens-agent zerobus sink.
//!
//! Mints signed and unsigned principal contexts from Databricks Login via mTLS and
//! the `databricks.identity.PrincipalContextService.CreatePrincipalContext` RPC, then
//! re-bootstraps before expiry. The proto schema is loaded from a runtime
//! `FileDescriptorSet` so vector never carries a snapshot of universe protos.

mod cache;
mod config;
mod error;
mod manager;
mod request;
mod response;
mod transport;

pub(crate) use config::SIGNED_PRINCIPAL_CONTEXT_HEADER;
pub use config::{
    LoginServiceAuthConfig, LoginServiceConfig, ServiceTlsConfig, UnityCatalogServiceConfig,
    UserAuthInfo,
};
pub use error::TokenManagerError;
pub use manager::TokenManager;
pub(crate) use transport::{
    GrpcHttpClient, build_hyper_openssl_client, grpc_method_uri, grpc_unary_call,
    load_descriptor_pool,
};

#[cfg(test)]
mod tests;

pub(crate) use manager::PrincipalContextMetadata;
