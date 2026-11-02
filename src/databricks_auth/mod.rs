//! Databricks Login service authentication for logging-agent.
//!
//! Mints and refreshes signed and unsigned principal contexts via the Login service's
//! `PrincipalContextService.CreatePrincipalContext` RPC.

mod headers_provider;
mod token_manager;

pub use headers_provider::LoginServiceHeadersProvider;
pub use token_manager::{
    LoginServiceAuthConfig, LoginServiceConfig, TokenManager, TokenManagerError, UcPermissionEntry,
    UnityCatalogServiceConfig, UserAuthInfo,
};
