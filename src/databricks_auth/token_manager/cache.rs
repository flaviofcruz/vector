use std::time::{Duration, Instant};

use super::config::TOKEN_REFRESH_THRESHOLD;

/// Principal contexts minted by Login's `CreatePrincipalContext`, together with the instant
/// at which the `TokenManager` should re-bootstrap rather than reuse them.
#[derive(Debug, Clone)]
pub(super) struct CachedPrincipalContext {
    /// Serialized unsigned `PrincipalContext`, sent to UC as `s2s-principal-context-bin`.
    pub(super) principal_context: Vec<u8>,
    /// Serialized signed `ProtectedMetadataWrapper`, sent to Zerobus as
    /// `s2s-principal-context-sig-bin`.
    pub(super) signed_principal_context: Vec<u8>,
    pub(super) expires_at: Instant,
}

impl CachedPrincipalContext {
    /// Returns true if the cached contexts still have more than `TOKEN_REFRESH_THRESHOLD`
    /// of remaining lifetime at `now`.
    pub(super) fn is_valid_at(&self, now: Instant) -> bool {
        self.has_remaining_lifetime(now, TOKEN_REFRESH_THRESHOLD)
    }

    /// Returns true if the contexts have more than `margin` of remaining lifetime at `now`.
    pub(super) fn has_remaining_lifetime(&self, now: Instant, margin: Duration) -> bool {
        self.expires_at > now + margin
    }
}
