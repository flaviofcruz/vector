/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#[cfg(feature = "openssl")]
mod openssl;
#[cfg(feature = "openssl")]
pub(crate) use openssl::{hmac_sha256, sha256};

/// Failure to perform a signing operation in the selected crypto backend.
#[cfg(feature = "openssl")]
pub(crate) type CryptoError = ::openssl::error::ErrorStack;
/// The default backend's SHA-256 and HMAC operations are infallible.
#[cfg(not(feature = "openssl"))]
pub(crate) type CryptoError = std::convert::Infallible;

#[cfg(not(feature = "openssl"))]
pub(crate) fn sha256(bytes: &[u8]) -> Result<[u8; 32], CryptoError> {
    use sha2::{Digest, Sha256};
    Ok(Sha256::digest(bytes).into())
}

#[cfg(not(feature = "openssl"))]
pub(crate) fn hmac_sha256(key: &[u8], bytes: &[u8]) -> Result<[u8; 32], CryptoError> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(bytes);
    Ok(mac.finalize().into_bytes().into())
}
