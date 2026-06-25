//! OAuth token manager for the bricklens-agent zerobus sink.
//!
//! Bootstraps an internal-OAuth access token from Databricks Login via mTLS and
//! the `databricks.common.authentication.oauth.InternalOAuthTokenService.GenerateReauthenticatedAndReauthorizedInternalOAuthToken`
//! RPC, then re-bootstraps before expiry. The OAuth proto schema is loaded from
//! a runtime `FileDescriptorSet` so vector never carries a snapshot of universe
//! protos.

mod cache;
mod config;
mod error;
mod jwt;
mod manager;
mod request;
mod transport;

pub use config::{LoginServiceAuthConfig, LoginServiceConfig, UcPermissionEntry, UserAuthInfo};
pub use error::TokenManagerError;
pub use manager::TokenManager;

#[cfg(test)]
mod tests;
