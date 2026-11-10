use openssl::{
    asn1::Asn1Time,
    ssl::{SslRef, SslVerifyMode},
    x509::X509VerifyResult,
};
use std::time::Duration;

pub(super) fn verified_peer_certificate_lifetime(ssl: &SslRef) -> Option<Duration> {
    if !ssl.verify_mode().contains(SslVerifyMode::PEER)
        || ssl.verify_result() != X509VerifyResult::OK
    {
        return None;
    }
    let cert = ssl.peer_certificate()?;
    let now = Asn1Time::days_from_now(0).ok()?;
    if cert.not_before() > now.as_ref() || cert.not_after() <= now.as_ref() {
        return None;
    }
    let remaining = now.diff(cert.not_after()).ok()?;
    Some(Duration::from_secs(
        u64::try_from(i64::from(remaining.days) * 86400 + i64::from(remaining.secs)).ok()?,
    ))
}

/// A parsed peer certificate is authoritative only when TLS required and verified it.
pub(super) fn verified_peer_certificate_der(ssl: &SslRef) -> Option<Vec<u8>> {
    if verified_peer_certificate_lifetime(ssl).is_some() {
        // On the server, peer_cert_chain excludes the client leaf.
        ssl.peer_certificate().and_then(|cert| cert.to_der().ok())
    } else {
        None
    }
}
