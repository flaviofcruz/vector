use std::time::{Duration, Instant};

use super::config::TOKEN_REFRESH_THRESHOLD;

/// A bootstrapped access token together with the instant at which the
/// `TokenManager` should re-bootstrap rather than reuse it.
#[derive(Debug, Clone)]
pub(super) struct CachedToken {
    pub(super) access_token: String,
    pub(super) expires_at: Instant,
}

impl CachedToken {
    /// Returns true if the cached token still has more than `TOKEN_REFRESH_THRESHOLD`
    /// of remaining lifetime at `now`.
    pub(super) fn is_valid_at(&self, now: Instant) -> bool {
        self.has_remaining_lifetime(now, TOKEN_REFRESH_THRESHOLD)
    }

    /// Returns true if the token has more than `margin` of remaining lifetime at `now`.
    pub(super) fn has_remaining_lifetime(&self, now: Instant, margin: Duration) -> bool {
        self.expires_at > now + margin
    }
}
