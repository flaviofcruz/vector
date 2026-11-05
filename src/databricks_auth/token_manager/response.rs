use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use prost_reflect::{DynamicMessage, Value, prost::Message};

use super::error::TokenManagerError;

/// Return the serialized bytes of message field `name` on a `PrincipalContextResponse`
/// (`principal_context` or `signed_principal_context`), failing if Login left it unset.
pub(super) fn message_field_bytes(
    response: &DynamicMessage,
    name: &str,
) -> Result<Vec<u8>, TokenManagerError> {
    optional_message_field_bytes(response, name).ok_or_else(|| {
        TokenManagerError::PrincipalContext {
            message: format!("CreatePrincipalContext response has no `{name}`"),
        }
    })
}

/// Like [`message_field_bytes`], but `None` when Login left the field unset.
pub(super) fn optional_message_field_bytes(
    response: &DynamicMessage,
    name: &str,
) -> Option<Vec<u8>> {
    match response.get_field_by_name(name).as_deref() {
        Some(Value::Message(m)) if response.has_field_by_name(name) => Some(m.encode_to_vec()),
        _ => None,
    }
}

/// Redaction prefix universe prepends to every `s2s-principal-context-v2-sig-bin` value
/// (`MetadataKey.SIGNED_PRINCIPAL_CONTEXT_V2`).
const PRINCIPAL_CONTEXT_V2_WIRE_PREFIX: &[u8] = b"pctx_v2_";

/// `databricks.s2s.metadata.CompressedPayload.CompressionAlgorithm.NONE`.
const COMPRESSION_ALGORITHM_NONE: u64 = 1;

/// Encode a serialized `PrincipalContextWrapper` the way universe's `MetadataWireCodec` expects
/// for `s2s-principal-context-v2-sig-bin`: the `pctx_v2_` prefix followed by a `CompressedPayload`
/// envelope. Universe itself sends `NONE` for payloads under its compression threshold, so
/// receivers accept it and vector needs no zstd here.
pub(super) fn principal_context_v2_wire_bytes(wrapper: &[u8]) -> Vec<u8> {
    use prost_reflect::prost::encoding::encode_varint;

    let mut out = Vec::with_capacity(PRINCIPAL_CONTEXT_V2_WIRE_PREFIX.len() + wrapper.len() + 8);
    out.extend_from_slice(PRINCIPAL_CONTEXT_V2_WIRE_PREFIX);
    // `CompressedPayload` field 1 `algorithm` (varint), then field 2 `data` (length-delimited).
    out.push(0x08);
    encode_varint(COMPRESSION_ALGORITHM_NONE, &mut out);
    out.push(0x12);
    encode_varint(wrapper.len() as u64, &mut out);
    out.extend_from_slice(wrapper);
    out
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
