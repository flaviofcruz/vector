//! Databricks Login service authentication for logging-agent.
//!
//! Provides token management for bootstrapping and refreshing internal OAuth tokens
//! via the Login service's `GenerateReauthenticatedAndReauthorizedInternalOAuthTokenPair`
//! and `RefreshInternalOAuthTokenPair` RPCs.

mod headers_provider;
mod token_manager;

pub use headers_provider::LoginServiceHeadersProvider;
pub use token_manager::{
    LoginServiceAuthConfig, LoginServiceConfig, TokenManager, TokenManagerError, UcPermissionEntry,
    UserAuthInfo,
};
