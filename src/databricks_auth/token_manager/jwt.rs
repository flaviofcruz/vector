use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bytes::Buf;
use prost_reflect::prost::encoding::{self, DecodeContext, WireType};

use super::error::TokenManagerError;

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

/// Extract the serialized `PrincipalContext` that Login embeds in the minted JWT's `pctx` claim.
///
/// The claim is base64url of a `PrincipalContextClaim` whose field 1 is a
/// `ProtectedMetadataWrapper`; the wrapper's field 2 (`payload`) is the serialized
/// `PrincipalContext`, which UC reads from the unsigned principal-context header.
/// Mirrors `principal_context_from_jwt` in universe's `bricklens-ingest-external/src/uc_client.rs`.
/// Only for tokens obtained directly from Login over mTLS, never inbound JWTs.
pub(super) fn principal_context_from_jwt(jwt: &str) -> Result<Vec<u8>, TokenManagerError> {
    let fail = |message: &str| TokenManagerError::PrincipalContext {
        message: message.to_string(),
    };

    let payload_b64 = jwt
        .split('.')
        .nth(1)
        .ok_or_else(|| fail("JWT has no payload segment"))?;
    let payload_bytes = URL_SAFE_NO_PAD
        .decode(payload_b64.trim_end_matches('='))
        .map_err(|_| fail("JWT payload is not valid base64url"))?;
    let claims: serde_json::Value = serde_json::from_slice(&payload_bytes)
        .map_err(|_| fail("JWT payload is not valid JSON"))?;
    let pctx_b64 = claims
        .get("pctx")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| fail("JWT has no `pctx` claim"))?;
    let claim_bytes = URL_SAFE_NO_PAD
        .decode(pctx_b64.trim_end_matches('='))
        .map_err(|_| fail("`pctx` claim is not valid base64url"))?;

    let wrapper = length_delimited_field(&claim_bytes, 1)
        .ok_or_else(|| fail("`pctx` claim carried no principal-context wrapper"))?;
    let payload = length_delimited_field(&wrapper, 2)
        .ok_or_else(|| fail("principal-context wrapper carried no payload"))?;
    Ok(payload)
}

/// Return the last occurrence of length-delimited field `field_number` in a serialized message,
/// or `None` if it is absent or the message is malformed.
fn length_delimited_field(message: &[u8], field_number: u32) -> Option<Vec<u8>> {
    let mut buf = message;
    let mut found = None;
    while buf.has_remaining() {
        let (tag, wire_type) = encoding::decode_key(&mut buf).ok()?;
        if tag == field_number && wire_type == WireType::LengthDelimited {
            let len = usize::try_from(encoding::decode_varint(&mut buf).ok()?).ok()?;
            if buf.remaining() < len {
                return None;
            }
            found = Some(buf[..len].to_vec());
            buf.advance(len);
        } else {
            encoding::skip_field(wire_type, tag, &mut buf, DecodeContext::default()).ok()?;
        }
    }
    found
}
