use std::time::{Duration, Instant};

use secrecy::SecretSlice;

use super::config::TOKEN_REFRESH_THRESHOLD;

/// Principal contexts minted by Login's `CreatePrincipalContext`, together with the instant
/// at which the `TokenManager` should re-bootstrap rather than reuse them.
#[derive(Debug)]
pub(super) struct CachedPrincipalContext {
    /// Serialized unsigned `PrincipalContext`, sent to UC as `s2s-principal-context-bin`.
    pub(super) principal_context: SecretSlice<u8>,
    /// Serialized signed `ProtectedMetadataWrapper`, sent to Zerobus as
    /// `s2s-principal-context-sig-bin`.
    pub(super) signed_principal_context: SecretSlice<u8>,
    /// Optional signed PCv2 wrapper returned alongside the unsigned context.
    pub(super) signed_principal_context_v2: Option<SecretSlice<u8>>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_output_redacts_principal_context_bytes() {
        // ASCII markers so a leak is visible in both `{:?}` byte-list and string renderings.
        let build = |v2: Option<&[u8]>| CachedPrincipalContext {
            principal_context: b"UNSIGNED-SECRET".to_vec().into(),
            signed_principal_context: b"SIGNED-SECRET".to_vec().into(),
            signed_principal_context_v2: v2.map(|bytes| bytes.to_vec().into()),
            expires_at: Instant::now(),
        };

        for ctx in [build(Some(b"V2-SECRET")), build(None)] {
            for rendered in [format!("{ctx:?}"), format!("{ctx:#?}")] {
                for secret in [&b"UNSIGNED-SECRET"[..], b"SIGNED-SECRET", b"V2-SECRET"] {
                    let as_list = format!("{:?}", secret.to_vec());
                    let as_list = as_list.trim_matches(|c: char| c == '[' || c == ']');
                    assert!(
                        !rendered.contains(std::str::from_utf8(secret).unwrap())
                            && !rendered.contains(as_list),
                        "Debug output leaked principal context bytes: {rendered}"
                    );
                }
                assert!(rendered.contains("expires_at"));
                assert!(rendered.contains("REDACTED"));
            }
        }
    }
}
