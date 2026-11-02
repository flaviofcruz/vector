use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use prost_reflect::{DynamicMessage, Value, prost::Message};

use super::error::TokenManagerError;

/// Return the serialized bytes of message field `name` on a `PrincipalContextResponse`
/// (`principal_context` or `signed_principal_context`), failing if Login left it unset.
pub(super) fn message_field_bytes(
    response: &DynamicMessage,
    name: &str,
) -> Result<Vec<u8>, TokenManagerError> {
    match response.get_field_by_name(name).as_deref() {
        Some(Value::Message(m)) if response.has_field_by_name(name) => Ok(m.encode_to_vec()),
        _ => Err(TokenManagerError::PrincipalContext {
            message: format!("CreatePrincipalContext response has no `{name}`"),
        }),
    }
}

/// Read `principal_context.authentication_details.token_expiration` from a
/// `PrincipalContextResponse` and return the `Instant` at which it expires.
///
/// Returns `None` if any level is unset (Login treats an unset expiration as non-expiring) or
/// the expiration is already in the past — callers fall back to a default TTL.
pub(super) fn principal_context_expiry(response: &DynamicMessage) -> Option<Instant> {
    let mut message = response.clone();
    for name in [
        "principal_context",
        "authentication_details",
        "token_expiration",
    ] {
        if !message.has_field_by_name(name) {
            return None;
        }
        message = match message.get_field_by_name(name).as_deref() {
            Some(Value::Message(m)) => m.clone(),
            _ => return None,
        };
    }
    let seconds = match message.get_field_by_name("seconds").as_deref() {
        Some(Value::I64(s)) => u64::try_from(*s).ok()?,
        _ => return None,
    };
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
    (seconds > now).then(|| Instant::now() + Duration::from_secs(seconds - now))
}
