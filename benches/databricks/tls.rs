//! Throwaway CA and server certificate for the fake Zerobus endpoint.
//!
//! The Zerobus SDK always verifies the ingestion server (OAuth mode trusts the native root
//! store), so the fake must present a certificate the candidate trusts. The harness points the
//! candidate's `SSL_CERT_FILE` at this CA rather than switching the sink to plaintext, so the TLS
//! work the sink does in production stays on the measured path.

use std::path::{Path, PathBuf};

use openssl::asn1::Asn1Time;
use openssl::bn::{BigNum, MsbOption};
use openssl::ec::{EcGroup, EcKey};
use openssl::hash::MessageDigest;
use openssl::nid::Nid;
use openssl::pkey::{PKey, Private};
use openssl::x509::extension::{
    BasicConstraints, ExtendedKeyUsage, KeyUsage, SubjectAlternativeName,
};
use openssl::x509::{X509, X509NameBuilder};

use super::io_at;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub struct TlsFiles {
    pub ca_cert: PathBuf,
    pub server_cert: PathBuf,
    pub server_key: PathBuf,
}

impl TlsFiles {
    pub fn in_dir(dir: &Path) -> Self {
        Self {
            ca_cert: dir.join("ca.pem"),
            server_cert: dir.join("server.pem"),
            server_key: dir.join("server.key"),
        }
    }

    /// Writes a fresh CA and a `localhost`/`127.0.0.1` server certificate into `dir`.
    pub fn generate(dir: &Path) -> Result<Self, BoxError> {
        io_at(std::fs::create_dir_all(dir), "creating --tls-dir", dir)?;
        let files = Self::in_dir(dir);

        let ca_key = new_key()?;
        let ca = {
            let mut b = X509::builder()?;
            b.set_version(2)?;
            let serial = serial()?;
            b.set_serial_number(&serial)?;
            let name = name("databricks-bench test CA")?;
            b.set_subject_name(&name)?;
            b.set_issuer_name(&name)?;
            b.set_pubkey(&ca_key)?;
            b.set_not_before(&*Asn1Time::days_from_now(0)?)?;
            b.set_not_after(&*Asn1Time::days_from_now(30)?)?;
            b.append_extension(BasicConstraints::new().critical().ca().build()?)?;
            b.append_extension(
                KeyUsage::new()
                    .critical()
                    .key_cert_sign()
                    .crl_sign()
                    .build()?,
            )?;
            b.sign(&ca_key, MessageDigest::sha256())?;
            b.build()
        };

        let server_key = new_key()?;
        let server = {
            let mut b = X509::builder()?;
            b.set_version(2)?;
            let serial = serial()?;
            b.set_serial_number(&serial)?;
            let subject = name("localhost")?;
            b.set_subject_name(&subject)?;
            b.set_issuer_name(ca.subject_name())?;
            b.set_pubkey(&server_key)?;
            b.set_not_before(&*Asn1Time::days_from_now(0)?)?;
            b.set_not_after(&*Asn1Time::days_from_now(30)?)?;
            b.append_extension(BasicConstraints::new().build()?)?;
            b.append_extension(KeyUsage::new().critical().digital_signature().build()?)?;
            b.append_extension(ExtendedKeyUsage::new().server_auth().build()?)?;
            let san = SubjectAlternativeName::new()
                .dns("localhost")
                .ip("127.0.0.1")
                .build(&b.x509v3_context(Some(&ca), None))?;
            b.append_extension(san)?;
            b.sign(&ca_key, MessageDigest::sha256())?;
            b.build()
        };

        let ca_pem = ca.to_pem()?;
        io_at(
            std::fs::write(&files.ca_cert, ca_pem),
            "writing TLS file",
            &files.ca_cert,
        )?;
        let server_pem = server.to_pem()?;
        io_at(
            std::fs::write(&files.server_cert, server_pem),
            "writing TLS file",
            &files.server_cert,
        )?;
        let key_pem = server_key.private_key_to_pem_pkcs8()?;
        io_at(
            std::fs::write(&files.server_key, key_pem),
            "writing TLS file",
            &files.server_key,
        )?;
        Ok(files)
    }
}

fn new_key() -> Result<PKey<Private>, BoxError> {
    let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1)?;
    Ok(PKey::from_ec_key(EcKey::generate(&group)?)?)
}

fn serial() -> Result<openssl::asn1::Asn1Integer, BoxError> {
    let mut bn = BigNum::new()?;
    bn.rand(64, MsbOption::MAYBE_ZERO, false)?;
    Ok(bn.to_asn1_integer()?)
}

fn name(cn: &str) -> Result<openssl::x509::X509Name, BoxError> {
    let mut b = X509NameBuilder::new()?;
    b.append_entry_by_nid(Nid::COMMONNAME, cn)?;
    Ok(b.build())
}
