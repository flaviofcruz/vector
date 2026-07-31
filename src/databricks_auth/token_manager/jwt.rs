use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine;

/// Extract the `exp` claim from a JWT access token and return the `Instant` at
/// which it expires.
///
/// JWT format: `header.payload.signature` (base64url encoded). The payload is a
/// JSON object containing at least `{"exp": <unix_timestamp>}`. Returns `None`
/// if the JWT is malformed, the payload is not JSON, or `exp` is already in the
/// past — callers fall back to a default TTL.
pub(super) fn extract_jwt_expiry(jwt: &str) -> Option<Instant> {
    #[derive(serde::Deserialize)]
    struct ExpClaim {
        exp: u64,
    }

    let parts: Vec<&str> = jwt.split('.').collect();
    if parts.len() != 3 {
        return None;
    }

    let payload_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[1])
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(parts[1]))
        .ok()?;

    let claim: ExpClaim = serde_json::from_slice(&payload_bytes).ok()?;
    let now_timestamp = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
    if claim.exp <= now_timestamp {
        return None;
    }
    Some(Instant::now() + Duration::from_secs(claim.exp - now_timestamp))
}
