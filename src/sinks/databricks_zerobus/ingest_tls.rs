//! TLS settings passed to the Zerobus SDK.
//!
//! Login auth supplies the shared client identity and CA bundle. Ingestion always
//! verifies the server name; auth.tls.verify_hostname only controls Login and UC.

use databricks_zerobus_ingest_sdk::TlsSettings;
use rustls::pki_types::{CertificateDer, pem::PemObject};
use tonic_014::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};

use super::error::ZerobusSinkError;
use crate::databricks_auth::ServiceTlsConfig;

/// Read and validate mounted TLS material before starting the sink.
pub(super) fn ingestion_tls_settings(
    identity: Option<&ServiceTlsConfig>,
    domain_name: Option<&str>,
) -> Result<TlsSettings, ZerobusSinkError> {
    let fail = |message: String| ZerobusSinkError::ConfigError {
        message: format!("Zerobus ingest tls: {message}"),
    };
    let mut settings = TlsSettings::new();
    let mut validation = ClientTlsConfig::new();
    if let Some(identity) = identity {
        let read = |path: &str| {
            std::fs::read(path).map_err(|error| fail(format!("failed to read `{path}`: {error}")))
        };
        let certificate = read(&identity.crt_file)?;
        let key = read(&identity.key_file)?;
        validation = validation.identity(Identity::from_pem(&certificate, &key));
        settings = settings.with_client_identity_pem(certificate, key);
        if let Some(ca_file) = &identity.ca_file {
            let ca = read(ca_file)?;
            if !CertificateDer::pem_slice_iter(&ca).any(|certificate| certificate.is_ok()) {
                return Err(fail("ca_file contains no certificates".to_string()));
            }
            validation = validation.ca_certificate(Certificate::from_pem(&ca));
            settings = settings.without_native_roots().with_ca_certificate_pem(ca);
        } else {
            validation = validation.with_native_roots();
        }
    } else {
        validation = validation.with_native_roots();
    }
    if let Some(domain_name) = domain_name {
        settings = settings.with_domain_name(domain_name);
        validation = validation.domain_name(domain_name);
    }
    Endpoint::from_static("https://localhost")
        .tls_config(validation)
        .map_err(|error| fail(format!("invalid TLS settings: {error}")))?;
    Ok(settings)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::pin::Pin;
    use std::sync::Arc;

    use databricks_zerobus_ingest_sdk::{HeadersProvider, ZerobusResult, ZerobusSdk};
    use hyper::{Body, Response, service::service_fn};
    use openssl::{
        ssl::{AlpnError, Ssl, SslAcceptor, SslFiletype, SslMethod, SslVerifyMode},
        x509::X509,
    };
    use tokio::net::TcpListener;
    use tokio_openssl::SslStream;

    use super::*;
    use crate::databricks_auth::LoginServiceAuthConfig;

    fn path(relative: &str) -> String {
        format!("{}/tests/data/ca/{relative}", env!("CARGO_MANIFEST_DIR"))
    }

    fn auth() -> LoginServiceAuthConfig {
        toml::from_str(&format!(
            r#"
            metastore_id = "m"
            user.workspace_id = 1
            user.service_principal_resource = "accounts/a/apps"
            login_service.login_endpoint = "https://login.example"
            login_service.login_server_name = "login.example"
            login_service.proto_descriptor_path = "/unused/login.pb"
            tls.crt_file = {:?}
            tls.key_file = {:?}
            tls.ca_file = {:?}
        "#,
            path("intermediate_client/certs/localhost-chain.cert.pem"),
            path("intermediate_client/private/localhost.key.pem"),
            path("intermediate_server/certs/ca-chain.cert.pem")
        ))
        .unwrap()
    }

    #[test]
    fn missing_certificate_and_invalid_key_fail_at_startup() {
        let mut config = auth();
        config.tls.crt_file = path("missing.pem");
        assert!(ingestion_tls_settings(Some(&config.tls), None).is_err());
        let mut config = auth();
        config.tls.key_file = config.tls.crt_file.clone();
        assert!(ingestion_tls_settings(Some(&config.tls), None).is_err());
    }

    #[test]
    fn empty_ca_bundle_fails_at_startup() {
        let empty = tempfile::NamedTempFile::new().unwrap();
        let mut config = auth();
        config.tls.ca_file = Some(empty.path().to_string_lossy().into_owned());
        assert!(ingestion_tls_settings(Some(&config.tls), None).is_err());
    }

    #[tokio::test]
    async fn sdk_ingestion_domain_override_verifies_the_peer_and_preserves_the_destination() {
        let mut config = auth();
        config.tls.verify_hostname = false;
        let request = sdk_request(Some(&config.tls), Some("localhost"))
            .await
            .expect("verified SDK request");
        assert_eq!(request.server_name.as_deref(), Some("localhost"));
        let expected = X509::from_pem(&std::fs::read(&config.tls.crt_file).unwrap())
            .unwrap()
            .to_der()
            .unwrap();
        assert_eq!(request.peer.as_ref(), Some(&expected));

        assert!(
            sdk_request(Some(&config.tls), Some("wrong.example"))
                .await
                .is_none(),
            "mismatched DNS SAN"
        );
        config.tls.ca_file = Some(path("../Multi_CA.crt"));
        assert!(
            sdk_request(Some(&config.tls), Some("localhost"))
                .await
                .is_none(),
            "untrusted certificate chain"
        );
    }

    #[tokio::test]
    async fn ingestion_verifies_hostnames_independently_of_login_and_uc_policy() {
        let mut config = auth();
        // The endpoint is an IP address absent from the certificate. A Login/UC
        // policy change must never disable the SDK's ingestion hostname check.
        for verify_hostname in [true, false] {
            config.tls.verify_hostname = verify_hostname;
            assert!(sdk_request(Some(&config.tls), None).await.is_none());
        }
        // OAuth has no client identity, but supports the same name override.
        assert!(sdk_request(None, None).await.is_none());
        let request = sdk_request(None, Some("localhost")).await.unwrap();
        assert_eq!(request.server_name.as_deref(), Some("localhost"));
        assert!(request.peer.is_none());
        assert!(sdk_request(None, Some("wrong.example")).await.is_none());
    }

    struct NoHeaders;

    #[async_trait::async_trait]
    impl HeadersProvider for NoHeaders {
        async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
            Ok(HashMap::new())
        }
    }

    #[derive(Debug)]
    struct ObservedRequest {
        peer: Option<Vec<u8>>,
        server_name: Option<String>,
        authority: String,
    }

    // A terminal gRPC response stops the real SDK before any records are ingested.
    #[allow(deprecated)] // Hyper 0.14 supplies the Tokio executor for this test server.
    async fn sdk_request(
        identity: Option<&ServiceTlsConfig>,
        domain_name: Option<&str>,
    ) -> Option<ObservedRequest> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let acceptor = ingest_acceptor(identity.is_some());
        let (request_tx, mut request_rx) = tokio::sync::mpsc::unbounded_channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ssl = Ssl::new(acceptor.context()).unwrap();
            let mut stream = SslStream::new(ssl, stream).unwrap();
            if Pin::new(&mut stream).accept().await.is_err() {
                return;
            }
            let peer = stream
                .ssl()
                .peer_certificate()
                .map(|cert| cert.to_der().unwrap());
            let server_name = stream
                .ssl()
                .servername(openssl::ssl::NameType::HOST_NAME)
                .map(str::to_owned);
            let service = service_fn(move |request: hyper::Request<Body>| {
                request_tx
                    .send(ObservedRequest {
                        peer: peer.clone(),
                        server_name: server_name.clone(),
                        authority: request.uri().authority().unwrap().to_string(),
                    })
                    .unwrap();
                async {
                    Ok::<_, std::convert::Infallible>(
                        Response::builder()
                            .header("content-type", "application/grpc")
                            .header("grpc-status", "12")
                            .body(Body::empty())
                            .unwrap(),
                    )
                }
            });
            let _ = hyper::server::conn::Http::new()
                .http2_only(true)
                .serve_connection(stream, service)
                .await;
        });
        let mut settings = ingestion_tls_settings(identity, domain_name).unwrap();
        if identity.is_none() {
            // Trust the loopback fixture without changing the process trust store.
            settings = settings.without_native_roots().with_ca_certificate_pem(
                std::fs::read(path("intermediate_server/certs/ca-chain.cert.pem")).unwrap(),
            );
        }
        let sdk = ZerobusSdk::builder()
            .endpoint(format!("https://{address}"))
            .tls_config(Arc::new(settings))
            .connector_factory(Arc::new(|_| None))
            .build()
            .unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            sdk.stream_builder()
                .table("test.schema.table")
                .headers_provider(Arc::new(NoHeaders))
                .arrow(Arc::new(arrow::datatypes::Schema::empty()))
                .connection_timeout_ms(500)
                .recovery_timeout_ms(1000)
                .recovery(false)
                .build_arrow(),
        )
        .await
        .expect("SDK TLS attempt timed out");
        assert!(
            result.is_err(),
            "test server never accepts an ingestion stream"
        );
        server.abort();
        let request = request_rx.try_recv().ok();
        if let Some(request) = &request {
            assert_eq!(request.authority, address.to_string());
        }
        request
    }

    fn ingest_acceptor(require_client_identity: bool) -> SslAcceptor {
        let mut acceptor = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
        acceptor
            .set_certificate_chain_file(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/data/databricks/ingest-server.cert.pem"
            ))
            .unwrap();
        acceptor
            .set_private_key_file(
                path("intermediate_server/private/localhost.key.pem"),
                SslFiletype::PEM,
            )
            .unwrap();
        acceptor
            .set_ca_file(path("intermediate_client/certs/ca-chain.cert.pem"))
            .unwrap();
        let verify = if require_client_identity {
            SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT
        } else {
            SslVerifyMode::PEER
        };
        acceptor.set_verify(verify);
        acceptor.set_alpn_select_callback(|_, protocols| {
            openssl::ssl::select_next_proto(b"\x02h2", protocols).ok_or(AlpnError::NOACK)
        });
        acceptor.build()
    }
}
