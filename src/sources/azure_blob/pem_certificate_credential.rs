// Note: Using v0.21 Azure SDKs to match azure_storage_* crate versions
use azure_core_for_storage::{Error as AzureError, auth::Secret};
use azure_identity_for_storage::{ClientCertificateCredential, ClientCertificateCredentialOptions};
use base64::prelude::*;
use openssl::pkcs12::Pkcs12;
use openssl::pkey::PKey;
use openssl::x509::X509;
use pem::Pem;
use snafu::{ResultExt, Snafu};
use std::fs;
use std::path::Path;
use uuid::Uuid;
use vector_lib::configurable::configurable_component;

/// Errors that can occur during PEM certificate credential operations
#[derive(Debug, Snafu)]
pub enum PemCertificateError {
    #[snafu(display("Failed to read PEM certificate file at {}: {}", path.display(), source))]
    FileRead {
        path: std::path::PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to parse private key from PEM file: {}", source))]
    PrivateKeyParse { source: openssl::error::ErrorStack },

    #[snafu(display("Failed to parse certificate from PEM file: {}", source))]
    CertificateParse { source: openssl::error::ErrorStack },

    #[snafu(display("Failed to create PKCS12 structure: {}", source))]
    Pkcs12Creation { source: openssl::error::ErrorStack },

    #[snafu(display("Failed to create Azure ClientCertificateCredential: {}", source))]
    CredentialCreation { source: AzureError },

    #[snafu(display(
        "Invalid PEM file format: expected both private key and certificate to be present in the file"
    ))]
    InvalidPemFormat,
}

type Result<T> = std::result::Result<T, PemCertificateError>;

/// Default value for send_certificate_chain field.
const fn default_send_certificate_chain() -> bool {
    true
}

/// Configuration that must be provided by the client if they want to
/// use service principal based SNI authentication.
///
/// [Internal] We use this struct as a wrapper around the azure-sdk-for-rust's
/// ClientCertificateCredential as the same only supports PKCS12 format certificates,
/// while SEAR mounted certificates use the PEM format.
#[configurable_component]
#[derive(Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct PemCertificateCredential {
    /// Azure Active Directory tenant ID (also known as directory ID)
    #[configurable(metadata(docs::examples = "12345678-1234-1234-1234-123456789012"))]
    pub tenant_id: String,

    /// Azure application (client) ID
    #[configurable(metadata(docs::examples = "87654321-4321-4321-4321-210987654321"))]
    pub client_id: String,

    /// Azure Storage account name
    #[configurable(metadata(docs::examples = "mystorageaccount"))]
    pub storage_account: String,

    /// Path to the PEM certificate file containing both private key and certificate.
    /// The PEM file should contain:
    /// - One RSA private key (-----BEGIN PRIVATE KEY-----)
    /// - One X.509 certificate (-----BEGIN CERTIFICATE-----)
    #[configurable(metadata(docs::examples = "/path/to/certificate.pem"))]
    pub client_certificate_path: String,

    /// Whether to send the certificate chain in the x5c header of token requests.
    /// This is required for Subject Name/Issuer (SNI) authentication.
    /// Defaults to true.
    #[serde(default = "default_send_certificate_chain")]
    pub send_certificate_chain: bool,
}

impl PemCertificateCredential {
    /// Creates a new PemCertificateCredential instance.
    pub fn new(
        tenant_id: String,
        client_id: String,
        storage_account: String,
        client_certificate_path: String,
        send_certificate_chain: Option<bool>,
    ) -> Self {
        Self {
            tenant_id,
            client_id,
            storage_account,
            client_certificate_path,
            send_certificate_chain: send_certificate_chain.unwrap_or(true),
        }
    }

    /// Converts PEM certificate to PKCS12 format and creates Azure ClientCertificateCredential.
    pub fn create_client_certificate_credential(&self) -> Result<ClientCertificateCredential> {
        let pem_path = Path::new(&self.client_certificate_path);
        let pem_content = fs::read_to_string(pem_path).context(FileReadSnafu {
            path: pem_path.to_path_buf(),
        })?;

        let (private_key, certificate) = self.parse_pem_content(&pem_content)?;

        // Generate a random temporary password for PKCS12 encryption
        let temp_random_password = self.generate_temp_random_password();
        let pkcs12_data = self.create_pkcs12(&private_key, &certificate, &temp_random_password)?;

        let mut credential_options = ClientCertificateCredentialOptions::default();
        credential_options.set_send_certificate_chain(self.send_certificate_chain);

        let client_certificate_secret =
            Secret::new(base64::prelude::BASE64_STANDARD.encode(&pkcs12_data));
        let password_secret = Secret::new(temp_random_password);

        let client_credential = ClientCertificateCredential::new(
            self.tenant_id.clone(),
            self.client_id.clone(),
            client_certificate_secret,
            password_secret,
            credential_options,
        )
        .context(CredentialCreationSnafu)?;

        Ok(client_credential)
    }

    /// Generates a random temporary password for PKCS12 encryption.
    /// The password is used internally when converting PEM certificates to PKCS12 format.
    /// This would be used by the azure_identity crate to decode the PKCS12 data provided
    /// and extract the certificate and the private key.
    /// We make use a constant prefix and a UUID as the password.
    fn generate_temp_random_password(&self) -> String {
        format!("vector-azure-temp-{}", Uuid::new_v4())
    }

    /// Parses PEM content to extract private key and certificate.
    /// Returns error if the PEM content doesn't contain both a private key and certificate,
    /// or if OpenSSL fails to parse the components. The function handles both cases
    /// gracefully: where the ordering of the private key and certificate is swapped.
    ///
    /// [Implementation Note] We can't use direct methods like PKey::private_key_from_pem
    /// or X509::from_pem to parse as we are not sure which will come first in the PEM bytes.
    /// Thus we make use of pem crate to read the objects one by one and extract the
    /// private key and certificate.
    fn parse_pem_content(&self, pem_content: &str) -> Result<(PKey<openssl::pkey::Private>, X509)> {
        // Parse all PEM objects from the content
        let pems =
            pem::parse_many(pem_content).map_err(|_| PemCertificateError::InvalidPemFormat)?;
        if pems.is_empty() {
            return Err(PemCertificateError::InvalidPemFormat);
        }

        // Find private key and certificate
        let mut private_key_pem: Option<&Pem> = None;
        let mut certificate_pem: Option<&Pem> = None;
        for pem_obj in &pems {
            match pem_obj.tag() {
                "PRIVATE KEY" | "RSA PRIVATE KEY" => {
                    private_key_pem = Some(pem_obj);
                }
                "CERTIFICATE" => {
                    certificate_pem = Some(pem_obj);
                }
                _ => {} // Ignore other PEM types
            }
        }

        // Ensure we have both components
        let private_key_pem = private_key_pem.ok_or(PemCertificateError::InvalidPemFormat)?;
        let certificate_pem = certificate_pem.ok_or(PemCertificateError::InvalidPemFormat)?;

        // Convert to OpenSSL objects directly using PEM crate data
        let private_key =
            PKey::private_key_from_der(private_key_pem.contents()).context(PrivateKeyParseSnafu)?;

        let certificate =
            X509::from_der(certificate_pem.contents()).context(CertificateParseSnafu)?;

        Ok((private_key, certificate))
    }

    /// Creates PKCS12 structure from private key and certificate.
    fn create_pkcs12(
        &self,
        private_key: &PKey<openssl::pkey::Private>,
        certificate: &X509,
        temp_random_password: &str,
    ) -> Result<Vec<u8>> {
        let pkcs12 = Pkcs12::builder()
            .pkey(private_key)
            .cert(certificate)
            .build2(temp_random_password)
            .context(Pkcs12CreationSnafu)?;

        let pkcs12_der = pkcs12.to_der().context(Pkcs12CreationSnafu)?;
        Ok(pkcs12_der)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    // Test PEM content with only private key
    const PEM_PRIVATE_KEY_ONLY: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEwAIBADANBgkqhkiG9w0BAQEFAASCBKowggSmAgEAAoIBAQCkhe1/WPpMQjpp
Cjf3A84zity988ZY27WDp5yHd+2PMPttCN9DUVRTNWYSHAVHayxsJ+T6R5lOHw+f
NwCbxJGakXpJ0RRrxO8/y7O5yYzlwtyCEvydlXGBNvytTdbDshkWxgruNxBuCVsu
bKSfLc1tozfoTAZn0tNSfO+0Jbcxd/U7b72qAcayQOTJ4TFXcrgRDM//HjYglKOn
T76AooxlvjYC9cp7Wb9Ee7QhnRIqYkaaRimAK/upnIrk37D1rHiwRN3QkuvvEqqe
HuDz8MP0NuCg9Ckxx1QI5qySzpNGCNoLoq7PJO+eMdJiDnjuypgFcejkIBJAXjdk
PyOGA5M3AgMBAAECggEBAJOK+9sp24YKGsHahSgEBKcakC3VcXp8xsKjzO98MNIq
VkCJJVgElr/scnYpJN7QkU0JVgLRtP1nQ6ZAOE55MS7R6j5Kv6qKORkYQDyMMMxY
PVJ1XpCf1ePQgTeWR9TGYlOXFXRec2CLCXePvO53r/Pz3Q55J4Fyg0tFed4vuKMm
Bcp2CtWQIM6+lMgK0bpb9NVf3xVU6ZGntOgynAqBG13nZlORvkTQDoQgengtQMXw
69DQVgKPgb8mbNNPOpPTV/Xb1gFhCnzXyEg981smiaMtGX6LVCGQv6KDgMfi7WGy
yN3Z2NbDbqeiCkrE7K/qX00g5rzSCHbNVKKcknULCMECgYEA0zLaDId0+KovlugR
oMpkyaI6m5pwdzTT3rwVdyXCMVzfXB9IEfAP/OJAdm0j/BSnR34kzx09jygVtsN2
mnehmVWuV7wSTm66ugX975FKpzMiHoppMAtTO51u1SqIZkyM8hoiiwiMJnkXKKr6
Pq5KbP030VpxYtE0wmHijwyslNcCgYEAx2xcyGsJ1tAwwTcMz83mVADEEF21o4nK
ioI/Z4anwfX1emucvi6vAu4/KtVChsaZ3Qt27T692OeJkqbciYtvzOA8cFtoyRpS
eQrHjUFPfHtKSYx/Qxbh2tdu7IfttrjexkilqVVNhEWMCxEaP+J22+LZc6HqExgk
0hZahPgeyKECgYEAkarXPiEHiqNHI5x43B/8mB3usng47d9f6pZrb7x5TjayUAW7
XbPoMxGSSJxKX4mXPvZASSHv3ZdWMrJqUWwF545zK0wqjDJPVBLh7KSXiu73r3zj
xCFrjQiu8xPc9EIETM+914tTrw2B7ajP5P+tkbKtFxZ8ch29d/yvmN6zAg0CgYEA
j6QAzKc0phK9G16wjrlrDtSiZHtrCsmEJvIcA1CdYvrrfusmMmJj0sOSoiKL0ZIZ
X3sThV0s16Ammogvz66sr7BQOEnPFxMrll3qUFdbjnkrkABv5f4EXmHQVvSth3Bv
nfjTwj1cIUsKzSnbc2qGXGlwYXadqHU6iExrlN03JyECgYEA0LK2E0b/HImnpsg0
PyJHLOpxhrGd0Jie6fz7nfoAQwHnQdWjVO9ZdQpVMvtG2AuBNbDgO3r1Ny0pzHVd
NErYxdPIeYvHMX+wPOckCJtAMu7UPqH1GHNIhNf5YPk5UnBQGN6k5kwDN3BgCTLL
YBiBeXQ0Q+ovW72U2aGNFbpyxz4=
-----END PRIVATE KEY-----"#;

    // Test PEM content with only certificate
    const PEM_CERTIFICATE_ONLY: &str = r#"-----BEGIN CERTIFICATE-----
MIICmjCCAYICCQDDZFmcSEfaNTANBgkqhkiG9w0BAQsFADAPMQ0wCwYDVQQDDAR0
ZXN0MB4XDTI1MDgwNzEyMTIyNloXDTI2MDgwNzEyMTIyNlowDzENMAsGA1UEAwwE
dGVzdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAKSF7X9Y+kxCOmkK
N/cDzjOK3L3zxljbtYOnnId37Y8w+20I30NRVFM1ZhIcBUdrLGwn5PpHmU4fD583
AJvEkZqReknRFGvE7z/Ls7nJjOXC3IIS/J2VcYE2/K1N1sOyGRbGCu43EG4JWy5s
pJ8tzW2jN+hMBmfS01J877QltzF39TtvvaoBxrJA5MnhMVdyuBEMz/8eNiCUo6dP
voCijGW+NgL1yntZv0R7tCGdEipiRppGKYAr+6mciuTfsPWseLBE3dCS6+8Sqp4e
4PPww/Q24KD0KTHHVAjmrJLOk0YI2guirs8k754x0mIOeO7KmAVx6OQgEkBeN2Q/
I4YDkzcCAwEAATANBgkqhkiG9w0BAQsFAAOCAQEAhwXa9LrHAwveNgIrG9nU33Zz
k2M5+QIlcO6yEZKSGwSzSZro4+Y5J+q/GUb++9Wj/mHP8iO1cmPsYi5PvBl+6rFC
EGtlFHsEPwkPCfgNhHzWD1dGIeaRltsT0NYlZfU6F8Q9sJLZ+FvFR2nm0pAIshmM
FprKH4Y8mzNT8LyYCWguj85zfQ2AiYztHUnwl3IFge6KSPlMFRd5idhGlPNeaMUd
5eoooNdI+tSKO1ycMioZ+ChWw+sD5Ml5RD5e/N1Ntk8muwuFqrLAHLiRZf4hN//R
FLgLKNR10xCWvrfoLa2uhwFmt1pjfBCV+GRkscoq3DxIpTxzESL5jIX5VRUSug==
-----END CERTIFICATE-----
"#;

    fn create_temp_pem_file(content: &str) -> NamedTempFile {
        let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
        temp_file
            .write_all(content.as_bytes())
            .expect("Failed to write PEM content");
        temp_file.flush().expect("Failed to flush temp file");
        temp_file
    }

    #[test]
    fn test_pem_certificate_credential_new() {
        let cred = PemCertificateCredential::new(
            "tenant-123".to_string(),
            "client-456".to_string(),
            "storageaccount".to_string(),
            "/path/to/cert.pem".to_string(),
            Some(false),
        );

        assert_eq!(cred.tenant_id, "tenant-123");
        assert_eq!(cred.client_id, "client-456");
        assert_eq!(cred.storage_account, "storageaccount");
        assert_eq!(cred.client_certificate_path, "/path/to/cert.pem");
        assert!(!cred.send_certificate_chain);
    }

    #[test]
    fn test_pem_certificate_credential_new_default_send_chain() {
        let cred = PemCertificateCredential::new(
            "tenant-123".to_string(),
            "client-456".to_string(),
            "storageaccount".to_string(),
            "/path/to/cert.pem".to_string(),
            None,
        );

        assert!(cred.send_certificate_chain); // Default should be true
    }

    #[test]
    fn test_parse_pem_content_valid() {
        let cred = PemCertificateCredential::new(
            "tenant-123".to_string(),
            "client-456".to_string(),
            "storageaccount".to_string(),
            "/path/to/cert.pem".to_string(),
            None,
        );

        let test_pem_content: String = PEM_PRIVATE_KEY_ONLY.to_string() + PEM_CERTIFICATE_ONLY;
        let result = cred.parse_pem_content(test_pem_content.as_str());
        assert!(
            result.is_ok(),
            "Valid PEM content should parse successfully"
        );

        let (private_key, certificate) = result.unwrap();
        assert!(!private_key.private_key_to_pem_pkcs8().unwrap().is_empty());
        assert!(!certificate.to_pem().unwrap().is_empty());
    }

    #[test]
    fn test_parse_pem_content_private_key_only() {
        let cred = PemCertificateCredential::new(
            "tenant-123".to_string(),
            "client-456".to_string(),
            "storageaccount".to_string(),
            "/path/to/cert.pem".to_string(),
            None,
        );

        let result = cred.parse_pem_content(PEM_PRIVATE_KEY_ONLY);
        assert!(result.is_err(), "PEM with only private key should fail");

        match result.unwrap_err() {
            PemCertificateError::InvalidPemFormat => {} // Expected error - missing certificate
            other => panic!("Expected InvalidPemFormat error, got: {:?}", other),
        }
    }

    #[test]
    fn test_parse_pem_content_certificate_only() {
        let cred = PemCertificateCredential::new(
            "tenant-123".to_string(),
            "client-456".to_string(),
            "storageaccount".to_string(),
            "/path/to/cert.pem".to_string(),
            None,
        );

        let result = cred.parse_pem_content(PEM_CERTIFICATE_ONLY);
        assert!(result.is_err(), "PEM with only certificate should fail");

        match result.unwrap_err() {
            PemCertificateError::InvalidPemFormat => {} // Expected error - missing private key
            other => panic!("Expected InvalidPemFormat error, got: {:?}", other),
        }
    }

    #[test]
    fn test_parse_pem_content_invalid_format() {
        let cred = PemCertificateCredential::new(
            "tenant-123".to_string(),
            "client-456".to_string(),
            "storageaccount".to_string(),
            "/path/to/cert.pem".to_string(),
            None,
        );

        let result = cred.parse_pem_content("invalid pem content");
        assert!(result.is_err(), "Invalid PEM content should fail");
    }

    #[test]
    fn test_create_pkcs12_valid() {
        let cred = PemCertificateCredential::new(
            "tenant-123".to_string(),
            "client-456".to_string(),
            "storageaccount".to_string(),
            "/path/to/cert.pem".to_string(),
            None,
        );

        // Contains the private key, and then the certificate
        let test_pem_content: String = PEM_PRIVATE_KEY_ONLY.to_string() + PEM_CERTIFICATE_ONLY;
        let (private_key, certificate) = cred.parse_pem_content(test_pem_content.as_str()).unwrap();
        let temp_password = cred.generate_temp_random_password();
        let result = cred.create_pkcs12(&private_key, &certificate, &temp_password);

        assert!(
            result.is_ok(),
            "PKCS12 creation should succeed with valid key and cert"
        );
        let pkcs12_data = result.unwrap();
        assert!(!pkcs12_data.is_empty(), "PKCS12 data should not be empty");
    }

    #[test]
    fn test_create_pkcs12_valid_reverse() {
        let cred = PemCertificateCredential::new(
            "tenant-123".to_string(),
            "client-456".to_string(),
            "storageaccount".to_string(),
            "/path/to/cert.pem".to_string(),
            None,
        );

        // Contains the certificate, and then the private key
        let test_pem_content: String = PEM_CERTIFICATE_ONLY.to_string() + PEM_PRIVATE_KEY_ONLY;
        let (private_key, certificate) = cred.parse_pem_content(test_pem_content.as_str()).unwrap();
        let temp_password = cred.generate_temp_random_password();
        let result = cred.create_pkcs12(&private_key, &certificate, &temp_password);

        assert!(
            result.is_ok(),
            "PKCS12 creation should succeed with valid key and cert"
        );
        let pkcs12_data = result.unwrap();
        assert!(!pkcs12_data.is_empty(), "PKCS12 data should not be empty");
    }

    #[test]
    fn test_file_read_error() {
        let cred = PemCertificateCredential::new(
            "tenant-123".to_string(),
            "client-456".to_string(),
            "storageaccount".to_string(),
            "/nonexistent/path/cert.pem".to_string(),
            None,
        );

        let result = cred.create_client_certificate_credential();
        assert!(result.is_err(), "Should fail when PEM file doesn't exist");

        match result.unwrap_err() {
            PemCertificateError::FileRead { .. } => {} // Expected error
            other => panic!("Expected FileRead error, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_create_client_certificate_credential_invalid_pem() {
        let temp_file = create_temp_pem_file(PEM_PRIVATE_KEY_ONLY);
        let temp_path = temp_file.path().to_string_lossy().to_string();

        let cred = PemCertificateCredential::new(
            "tenant-123".to_string(),
            "client-456".to_string(),
            "storageaccount".to_string(),
            temp_path,
            None,
        );

        let result = cred.create_client_certificate_credential();
        assert!(result.is_err(), "Should fail with invalid PEM content");

        match result.unwrap_err() {
            PemCertificateError::InvalidPemFormat => {} // Expected error - missing certificate
            other => panic!("Expected InvalidPemFormat error, got: {:?}", other),
        }
    }
}
