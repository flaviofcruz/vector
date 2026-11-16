// Note: Using v0.21 Azure SDKs to match azure_storage_* crate versions
use azure_core_for_storage::{
    Error as AzureError, Url,
    auth::{AccessToken, TokenCredential},
    error::ErrorKind,
};
use azure_identity_for_storage::{TokenCredentialOptions, federated_credentials_flow};
use base64::prelude::*;
use openssl::hash::MessageDigest;
use openssl::pkey::{Id, PKey, Private};
use openssl::rsa::Padding;
use openssl::sign::Signer;
use openssl::x509::X509;
use pem::Pem;
use snafu::{ResultExt, Snafu};
use std::{
    collections::HashMap,
    fmt, fs,
    path::Path,
    time::{Duration, SystemTime},
};
use tokio::sync::Mutex;
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

    #[snafu(display("Client certificate authentication requires an RSA private key"))]
    UnsupportedPrivateKey,

    #[snafu(display("Client certificate does not match the private key"))]
    CertificateKeyMismatch,

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

/// Azure cloud hosting the storage account and service principal.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AzureCloud {
    /// Public Azure endpoints.
    #[default]
    Public,
    /// Azure US Government endpoints.
    UsGovernment,
}

impl AzureCloud {
    pub(super) const fn storage_endpoint_suffix(self) -> &'static str {
        match self {
            Self::Public => "core.windows.net",
            Self::UsGovernment => "core.usgovcloudapi.net",
        }
    }
}

/// Configuration that must be provided by the client if they want to
/// use service principal based SNI authentication.
///
/// SEAR mounts PEM credentials. Sign assertions directly with that key: converting
/// through the SDK's PKCS12 credential requires PKCS12KDF, unavailable in FIPS mode.
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

    /// Azure cloud to authenticate against. Defaults to public Azure.
    #[serde(default)]
    pub cloud: AzureCloud,
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
            cloud: AzureCloud::Public,
        }
    }

    /// Loads the PEM key and certificate for signing Azure client assertions.
    pub fn create_client_certificate_credential(&self) -> Result<PemTokenCredential> {
        let mut options = TokenCredentialOptions::default();
        if self.cloud == AzureCloud::UsGovernment {
            options.set_authority_host("https://login.microsoftonline.us".to_owned());
        }
        self.create_credential_with_options(options)
    }

    fn create_credential_with_options(
        &self,
        options: TokenCredentialOptions,
    ) -> Result<PemTokenCredential> {
        let pem_path = Path::new(&self.client_certificate_path);
        let pem_content = fs::read_to_string(pem_path).context(FileReadSnafu {
            path: pem_path.to_path_buf(),
        })?;
        let (private_key, certificate) = self.parse_pem_content(&pem_content)?;
        if private_key.id() != Id::RSA {
            return Err(PemCertificateError::UnsupportedPrivateKey);
        }
        let public_key = certificate.public_key().context(CertificateParseSnafu)?;
        if !private_key.public_eq(&public_key) {
            return Err(PemCertificateError::CertificateKeyMismatch);
        }
        PemTokenCredential::new(self, private_key, &certificate, options)
            .context(CredentialCreationSnafu)
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
}

/// An Azure token credential that signs directly with a PEM private key.
///
/// The key is held for the lifetime of the source, matching the previous SDK
/// credential's behavior. Rebuilding the source reloads the mounted certificate.
pub struct PemTokenCredential {
    tenant_id: String,
    client_id: String,
    private_key: PKey<Private>,
    header: String,
    authority_host: Url,
    audience: Url,
    options: TokenCredentialOptions,
    cache: Mutex<HashMap<String, AccessToken>>,
}

impl fmt::Debug for PemTokenCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PemTokenCredential")
            .field("tenant_id", &self.tenant_id)
            .field("client_id", &self.client_id)
            .finish_non_exhaustive()
    }
}

impl PemTokenCredential {
    fn new(
        config: &PemCertificateCredential,
        private_key: PKey<Private>,
        certificate: &X509,
        options: TokenCredentialOptions,
    ) -> azure_core_for_storage::Result<Self> {
        let authority_host = options.authority_host()?;
        let audience = authority_host.join(&format!("/{}/oauth2/v2.0/token", config.tenant_id))?;
        // x5t identifies the certificate with SHA-1; the assertion uses RSA/SHA-256.
        let thumbprint = certificate
            .digest(MessageDigest::sha1())
            .map_err(Self::openssl_error)?;
        let mut header = serde_json::json!({
            "alg": "RS256",
            "typ": "JWT",
            "x5t": BASE64_STANDARD.encode(thumbprint),
        });
        if config.send_certificate_chain {
            // Preserve azure_identity 0.21's SNI header representation.
            header["x5c"] = serde_json::json!([
                BASE64_STANDARD.encode(certificate.to_pem().map_err(Self::openssl_error)?)
            ]);
        }
        Ok(Self {
            tenant_id: config.tenant_id.clone(),
            client_id: config.client_id.clone(),
            private_key,
            header: BASE64_URL_SAFE_NO_PAD.encode(header.to_string()),
            authority_host,
            audience,
            options,
            cache: Mutex::new(HashMap::new()),
        })
    }

    fn client_assertion(&self) -> azure_core_for_storage::Result<String> {
        let now = chrono::Utc::now().timestamp();
        let payload = serde_json::json!({
            "aud": self.audience.as_str(),
            "exp": now + 300,
            "iss": self.client_id,
            "jti": Uuid::new_v4().to_string(),
            "nbf": now,
            "sub": self.client_id,
        });
        let jwt = format!(
            "{}.{}",
            self.header,
            BASE64_URL_SAFE_NO_PAD.encode(payload.to_string())
        );
        let mut signer =
            Signer::new(MessageDigest::sha256(), &self.private_key).map_err(Self::openssl_error)?;
        signer
            .set_rsa_padding(Padding::PKCS1)
            .map_err(Self::openssl_error)?;
        signer.update(jwt.as_bytes()).map_err(Self::openssl_error)?;
        let signature = signer.sign_to_vec().map_err(Self::openssl_error)?;
        Ok(format!(
            "{}.{}",
            jwt,
            BASE64_URL_SAFE_NO_PAD.encode(signature)
        ))
    }

    fn openssl_error(error: openssl::error::ErrorStack) -> AzureError {
        AzureError::new(ErrorKind::Credential, error)
    }
}

#[async_trait::async_trait]
impl TokenCredential for PemTokenCredential {
    async fn get_token(&self, scopes: &[&str]) -> azure_core_for_storage::Result<AccessToken> {
        let [scope] = scopes else {
            return Err(AzureError::message(
                ErrorKind::Credential,
                "Exactly one Azure storage scope is required",
            ));
        };
        // Serialize refreshes so the blob and queue clients share one token request.
        let mut cache = self.cache.lock().await;
        if let Some(token) = cache.get(*scope)
            && SystemTime::from(token.expires_on) > SystemTime::now() + Duration::from_secs(20)
        {
            return Ok(token.clone());
        }
        let assertion = self.client_assertion()?;
        // Certificate and federated assertions use the same OAuth token exchange.
        let response = federated_credentials_flow::perform(
            self.options.http_client(),
            &self.client_id,
            &assertion,
            scopes,
            &self.tenant_id,
            &self.authority_host,
        )
        .await?;
        let token = AccessToken::new(
            response.access_token,
            (SystemTime::now() + Duration::from_secs(response.expires_in)).into(),
        );
        cache.insert((*scope).to_owned(), token.clone());
        Ok(token)
    }

    async fn clear_cache(&self) -> azure_core_for_storage::Result<()> {
        self.cache.lock().await.clear();
        Ok(())
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
    fn certificate_cloud_selects_token_authority() {
        let file = create_temp_pem_file(&format!("{PEM_PRIVATE_KEY_ONLY}{PEM_CERTIFICATE_ONLY}"));
        for (cloud, authority) in [
            (AzureCloud::Public, "https://login.microsoftonline.com"),
            (AzureCloud::UsGovernment, "https://login.microsoftonline.us"),
        ] {
            let mut config = test_config(&file, true);
            config.cloud = cloud;
            let credential = config.create_client_certificate_credential().unwrap();
            assert_eq!(
                credential.audience.as_str(),
                format!("{authority}/tenant-123/oauth2/v2.0/token")
            );
        }
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
    fn test_create_credential_valid() {
        for content in [
            format!("{PEM_PRIVATE_KEY_ONLY}{PEM_CERTIFICATE_ONLY}"),
            format!("{PEM_CERTIFICATE_ONLY}{PEM_PRIVATE_KEY_ONLY}"),
        ] {
            let file = create_temp_pem_file(&content);
            let config = test_config(&file, true);
            config.create_client_certificate_credential().unwrap();
        }
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
    fn test_config(file: &NamedTempFile, send_chain: bool) -> PemCertificateCredential {
        PemCertificateCredential::new(
            "tenant-123".to_owned(),
            "client-456".to_owned(),
            "storageaccount".to_owned(),
            file.path().to_string_lossy().into_owned(),
            Some(send_chain),
        )
    }

    // Only the Azure HTTP boundary is simulated; PEM parsing, signing, token exchange,
    // response handling, and the credential cache all use the production implementation.
    #[derive(Debug)]
    struct TokenEndpoint {
        send_chain: bool,
        fail_first: bool,
        requests: std::sync::Mutex<Vec<(String, String)>>,
    }

    #[async_trait::async_trait]
    impl azure_core_for_storage::HttpClient for TokenEndpoint {
        async fn execute_request(
            &self,
            request: &azure_core_for_storage::Request,
        ) -> azure_core_for_storage::Result<azure_core_for_storage::Response> {
            use azure_core_for_storage::{Body, Method, Response, StatusCode, headers};
            use openssl::sign::Verifier;

            tokio::task::yield_now().await;
            assert_eq!(request.method(), &Method::Post);
            assert_eq!(
                request.url().as_str(),
                "https://login.microsoftonline.us/tenant-123/oauth2/v2.0/token"
            );
            assert_eq!(
                request.headers().get_str(&headers::CONTENT_TYPE).unwrap(),
                "application/x-www-form-urlencoded"
            );
            let Body::Bytes(body) = request.body() else {
                panic!("expected token request body")
            };
            let form: HashMap<_, _> = url::form_urlencoded::parse(body).into_owned().collect();
            assert_eq!(form["client_id"], "client-456");
            assert_eq!(form["grant_type"], "client_credentials");
            assert_eq!(
                form["client_assertion_type"],
                "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
            );
            let parts: Vec<_> = form["client_assertion"].split('.').collect();
            assert_eq!(parts.len(), 3);
            let decode = |part| {
                serde_json::from_slice::<serde_json::Value>(
                    &BASE64_URL_SAFE_NO_PAD.decode(part).unwrap(),
                )
                .unwrap()
            };
            let header = decode(parts[0]);
            let claims = decode(parts[1]);
            assert_eq!(header["alg"], "RS256");
            assert_eq!(header["typ"], "JWT");
            let certificate = X509::from_pem(PEM_CERTIFICATE_ONLY.as_bytes()).unwrap();
            assert_eq!(
                BASE64_STANDARD
                    .decode(header["x5t"].as_str().unwrap())
                    .unwrap(),
                certificate.digest(MessageDigest::sha1()).unwrap().as_ref()
            );
            if self.send_chain {
                let chain = header["x5c"].as_array().unwrap();
                assert_eq!(chain.len(), 1);
                assert_eq!(
                    BASE64_STANDARD.decode(chain[0].as_str().unwrap()).unwrap(),
                    certificate.to_pem().unwrap()
                );
            } else {
                assert!(header.get("x5c").is_none());
            }
            assert_eq!(claims["aud"], request.url().as_str());
            assert_eq!(claims["iss"], "client-456");
            assert_eq!(claims["sub"], "client-456");
            let nbf = claims["nbf"].as_i64().unwrap();
            assert!((chrono::Utc::now().timestamp() - nbf).abs() < 30);
            assert_eq!(claims["exp"].as_i64().unwrap() - nbf, 300);
            let jti = claims["jti"].as_str().unwrap();
            assert_eq!(Uuid::parse_str(jti).unwrap().get_version_num(), 4);
            let public_key = certificate.public_key().unwrap();
            let mut verifier = Verifier::new(MessageDigest::sha256(), &public_key).unwrap();
            verifier.set_rsa_padding(Padding::PKCS1).unwrap();
            verifier
                .update(format!("{}.{}", parts[0], parts[1]).as_bytes())
                .unwrap();
            assert!(
                verifier
                    .verify(&BASE64_URL_SAFE_NO_PAD.decode(parts[2]).unwrap())
                    .unwrap()
            );
            let mut requests = self.requests.lock().unwrap();
            assert!(requests.iter().all(|(_, previous_jti)| previous_jti != jti));
            requests.push((form["scope"].clone(), jti.to_owned()));
            let (status, response) = if self.fail_first && requests.len() == 1 {
                (
                    StatusCode::Unauthorized,
                    serde_json::json!({"error": "invalid_client", "error_description": "test rejection"}),
                )
            } else {
                (
                    StatusCode::Ok,
                    serde_json::json!({
                        "token_type": "Bearer", "expires_in": 3600, "ext_expires_in": 3600,
                        "access_token": format!("test-token-{}", requests.len()),
                    }),
                )
            };
            Ok(Response::new(
                status,
                headers::Headers::new(),
                Box::pin(futures::stream::once(async move {
                    Ok(bytes::Bytes::from(response.to_string()))
                })),
            ))
        }
    }

    fn token_test_credential(
        send_chain: bool,
        fail_first: bool,
    ) -> (PemTokenCredential, std::sync::Arc<TokenEndpoint>) {
        let file = create_temp_pem_file(&format!("{PEM_PRIVATE_KEY_ONLY}{PEM_CERTIFICATE_ONLY}"));
        let endpoint = std::sync::Arc::new(TokenEndpoint {
            send_chain,
            fail_first,
            requests: std::sync::Mutex::new(Vec::new()),
        });
        let http = std::sync::Arc::clone(&endpoint);
        let mut options = TokenCredentialOptions::from(
            http as std::sync::Arc<dyn azure_core_for_storage::HttpClient>,
        );
        options.set_authority_host("https://login.microsoftonline.us".to_owned());
        let credential = test_config(&file, send_chain)
            .create_credential_with_options(options)
            .unwrap();
        (credential, endpoint)
    }

    #[tokio::test]
    async fn test_token_acquisition_cache_refresh_and_clear() {
        let (credential, endpoint) = token_test_credential(true, false);
        let scopes = ["https://storage.azure.com/.default"];
        let (first, concurrent) =
            tokio::join!(credential.get_token(&scopes), credential.get_token(&scopes));
        let first = first.unwrap();
        assert_eq!(first.token.secret(), "test-token-1");
        assert_eq!(concurrent.unwrap().token.secret(), first.token.secret());
        assert_eq!(endpoint.requests.lock().unwrap().len(), 1);
        let seconds_remaining = SystemTime::from(first.expires_on)
            .duration_since(SystemTime::now())
            .unwrap()
            .as_secs();
        assert!((3590..=3600).contains(&seconds_remaining));

        // A different scope must never reuse the storage token.
        assert_eq!(
            credential
                .get_token(&["https://management.azure.com/.default"])
                .await
                .unwrap()
                .token
                .secret(),
            "test-token-2"
        );
        credential
            .cache
            .lock()
            .await
            .get_mut(scopes[0])
            .unwrap()
            .expires_on = (SystemTime::now() + Duration::from_secs(10)).into();
        assert_eq!(
            credential.get_token(&scopes).await.unwrap().token.secret(),
            "test-token-3"
        );
        credential.clear_cache().await.unwrap();
        assert_eq!(
            credential.get_token(&scopes).await.unwrap().token.secret(),
            "test-token-4"
        );
        assert_eq!(endpoint.requests.lock().unwrap().len(), 4);
    }

    #[tokio::test]
    async fn test_token_acquisition_without_certificate_chain() {
        let (credential, endpoint) = token_test_credential(false, false);
        credential
            .get_token(&["https://storage.azure.com/.default"])
            .await
            .unwrap();
        assert_eq!(endpoint.requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_failed_token_request_is_not_cached() {
        let (credential, endpoint) = token_test_credential(true, true);
        let scopes = ["https://storage.azure.com/.default"];
        let error = credential.get_token(&scopes).await.unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::HttpResponse {
                status: azure_core_for_storage::StatusCode::Unauthorized,
                ..
            }
        ));
        assert!(credential.cache.lock().await.is_empty());
        assert_eq!(
            credential.get_token(&scopes).await.unwrap().token.secret(),
            "test-token-2"
        );
        assert_eq!(endpoint.requests.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_invalid_scopes_do_not_send_a_request() {
        let (credential, endpoint) = token_test_credential(true, false);
        assert!(credential.get_token(&[]).await.is_err());
        assert!(credential.get_token(&["first", "second"]).await.is_err());
        assert!(endpoint.requests.lock().unwrap().is_empty());
    }

    #[test]
    fn test_mismatched_certificate_is_rejected() {
        let key = PKey::from_rsa(openssl::rsa::Rsa::generate(2048).unwrap()).unwrap();
        let content = format!(
            "{}{PEM_CERTIFICATE_ONLY}",
            String::from_utf8(key.private_key_to_pem_pkcs8().unwrap()).unwrap()
        );
        let file = create_temp_pem_file(&content);
        assert!(matches!(
            test_config(&file, true).create_client_certificate_credential(),
            Err(PemCertificateError::CertificateKeyMismatch)
        ));
    }

    #[test]
    fn test_non_rsa_key_is_rejected() {
        let group =
            openssl::ec::EcGroup::from_curve_name(openssl::nid::Nid::X9_62_PRIME256V1).unwrap();
        let key = PKey::from_ec_key(openssl::ec::EcKey::generate(&group).unwrap()).unwrap();
        let content = format!(
            "{}{PEM_CERTIFICATE_ONLY}",
            String::from_utf8(key.private_key_to_pem_pkcs8().unwrap()).unwrap()
        );
        let file = create_temp_pem_file(&content);
        assert!(matches!(
            test_config(&file, true).create_client_certificate_credential(),
            Err(PemCertificateError::UnsupportedPrivateKey)
        ));
    }

    #[test]
    fn test_fips_policy_when_requested() {
        if std::env::var("VECTOR_TEST_EXPECT_FIPS").as_deref() != Ok("true") {
            return;
        }
        // Run the credential suite in a separate process with the FIPS config;
        // never change OpenSSL's process-wide defaults from a unit test.
        assert!(openssl::hash::hash(MessageDigest::md5(), b"policy probe").is_err());
        let key = PKey::private_key_from_pem(PEM_PRIVATE_KEY_ONLY.as_bytes()).unwrap();
        let cert = X509::from_pem(PEM_CERTIFICATE_ONLY.as_bytes()).unwrap();
        assert!(
            openssl::pkcs12::Pkcs12::builder()
                .pkey(&key)
                .cert(&cert)
                .build2("vector-test-password-long-enough")
                .is_err()
        );
    }
}
