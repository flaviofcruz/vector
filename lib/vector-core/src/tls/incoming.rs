use std::{
    collections::HashMap,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{FutureExt, Stream, future::BoxFuture, stream};
use ipnet::IpNet;
use openssl::{
    ssl::{Ssl, SslAcceptor, SslMethod, SslVerifyMode},
    x509::X509,
};
use snafu::ResultExt;
use tokio::{
    io::{self, AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
};
use tokio_openssl::SslStream;
use tonic::transport::{Certificate, server::Connected};

use super::{
    CreateAcceptorSnafu, HandshakeSnafu, IncomingListenerSnafu, MaybeTlsSettings, MaybeTlsStream,
    SslBuildSnafu, TcpBindSnafu, TlsError, TlsSettings,
};
use crate::tcp::{self, TcpKeepaliveConfig};

impl TlsSettings {
    pub fn acceptor(&self) -> crate::tls::Result<SslAcceptor> {
        if self.identity.is_none() {
            Err(TlsError::MissingRequiredIdentity)
        } else {
            let mut acceptor =
                SslAcceptor::mozilla_intermediate(SslMethod::tls()).context(CreateAcceptorSnafu)?;
            self.apply_context_base(&mut acceptor, true)?;
            Ok(acceptor.build())
        }
    }
}

impl MaybeTlsSettings {
    pub async fn bind(&self, addr: &SocketAddr) -> crate::tls::Result<MaybeTlsListener> {
        let listener = TcpListener::bind(addr).await.context(TcpBindSnafu)?;

        let acceptor = match self {
            Self::Tls(tls) => Some(tls.acceptor()?),
            Self::Raw(()) => None,
        };

        Ok(MaybeTlsListener {
            listener,
            acceptor,
            origin_filter: None,
        })
    }

    pub async fn bind_with_allowlist(
        &self,
        addr: &SocketAddr,
        allow_origin: Vec<IpNet>,
    ) -> crate::tls::Result<MaybeTlsListener> {
        let listener = TcpListener::bind(addr).await.context(TcpBindSnafu)?;

        let acceptor = match self {
            Self::Tls(tls) => Some(tls.acceptor()?),
            Self::Raw(()) => None,
        };

        Ok(MaybeTlsListener {
            listener,
            acceptor,
            origin_filter: Some(allow_origin),
        })
    }
}

pub struct MaybeTlsListener {
    listener: TcpListener,
    acceptor: Option<SslAcceptor>,
    origin_filter: Option<Vec<IpNet>>,
}

impl MaybeTlsListener {
    pub async fn accept(&mut self) -> crate::tls::Result<MaybeTlsIncomingStream<TcpStream>> {
        let listener = self
            .listener
            .accept()
            .await
            .map(|(stream, peer_addr)| {
                MaybeTlsIncomingStream::new(stream, peer_addr, self.acceptor.clone())
            })
            .context(IncomingListenerSnafu)?;

        if let Some(origin_filter) = &self.origin_filter {
            if origin_filter
                .iter()
                .any(|net| net.contains(&listener.peer_addr().ip()))
            {
                Ok(listener)
            } else {
                Err(TlsError::Connect {
                    source: std::io::ErrorKind::ConnectionRefused.into(),
                })
            }
        } else {
            Ok(listener)
        }
    }

    async fn into_accept(
        mut self,
    ) -> (crate::tls::Result<MaybeTlsIncomingStream<TcpStream>>, Self) {
        (self.accept().await, self)
    }

    pub fn accept_stream(
        self,
    ) -> impl Stream<Item = crate::tls::Result<MaybeTlsIncomingStream<TcpStream>>> {
        let mut accept = Box::pin(self.into_accept());
        stream::poll_fn(move |context| match accept.as_mut().poll(context) {
            Poll::Ready((item, this)) => {
                accept.set(this.into_accept());
                Poll::Ready(Some(item))
            }
            Poll::Pending => Poll::Pending,
        })
    }

    pub fn accept_stream_limited(
        self,
        max_connections: Option<u32>,
    ) -> impl Stream<
        Item = (
            crate::tls::Result<MaybeTlsIncomingStream<TcpStream>>,
            Option<OwnedSemaphorePermit>,
        ),
    > {
        let mut connection_semaphore_future = max_connections.map(|max| {
            let semaphore = Arc::new(Semaphore::new(max as usize));
            let future = Box::pin(semaphore.clone().acquire_owned());
            (semaphore, future)
        });

        let mut accept = Box::pin(self.into_accept());
        stream::poll_fn(move |context| {
            let permit = match connection_semaphore_future.as_mut() {
                Some((semaphore, future)) => match future.as_mut().poll(context) {
                    Poll::Ready(permit) => {
                        future.set(semaphore.clone().acquire_owned());
                        permit.ok()
                    }
                    Poll::Pending => return Poll::Pending,
                },
                None => None,
            };
            match accept.as_mut().poll(context) {
                Poll::Ready((item, this)) => {
                    accept.set(this.into_accept());
                    Poll::Ready(Some((item, permit)))
                }
                Poll::Pending => Poll::Pending,
            }
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.listener.local_addr()
    }

    #[must_use]
    pub fn with_allowlist(mut self, allowlist: Option<Vec<IpNet>>) -> Self {
        self.origin_filter = allowlist;
        self
    }
}

impl From<TcpListener> for MaybeTlsListener {
    fn from(listener: TcpListener) -> Self {
        Self {
            listener,
            acceptor: None,
            origin_filter: None,
        }
    }
}

pub struct MaybeTlsIncomingStream<S> {
    state: StreamState<S>,
    client_certificate_deadline: Option<Pin<Box<tokio::time::Sleep>>>,
    // BoxFuture doesn't allow access to the inner stream, but users
    // of MaybeTlsIncomingStream want access to the peer address while
    // still handshaking, so we have to cache it here.
    peer_addr: SocketAddr,
}

enum StreamState<S> {
    Accepted(MaybeTlsStream<S>),
    Accepting(BoxFuture<'static, Result<SslStream<S>, TlsError>>),
    AcceptError(String),
    Closed,
}

impl<S> MaybeTlsIncomingStream<S> {
    pub const fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    /// None if connection still hasn't been established.
    pub fn get_ref(&self) -> Option<&S> {
        use super::MaybeTls;

        match &self.state {
            StreamState::Accepted(stream) => Some(match stream {
                MaybeTls::Raw(s) => s,
                MaybeTls::Tls(s) => s.get_ref(),
            }),
            StreamState::Accepting(_) | StreamState::AcceptError(_) | StreamState::Closed => None,
        }
    }

    pub const fn ssl_stream(&self) -> Option<&SslStream<S>> {
        use super::MaybeTls;

        match &self.state {
            StreamState::Accepted(stream) => match stream {
                MaybeTls::Raw(_) => None,
                MaybeTls::Tls(s) => Some(s),
            },
            StreamState::Accepting(_) | StreamState::AcceptError(_) | StreamState::Closed => None,
        }
    }

    pub fn get_mut(&mut self) -> Option<&mut S> {
        use super::MaybeTls;

        match &mut self.state {
            StreamState::Accepted(stream) => Some(match stream {
                MaybeTls::Raw(s) => s,
                MaybeTls::Tls(s) => s.get_mut(),
            }),
            StreamState::Accepting(_) | StreamState::AcceptError(_) | StreamState::Closed => None,
        }
    }
}

impl MaybeTlsIncomingStream<TcpStream> {
    pub(super) fn new(
        stream: TcpStream,
        peer_addr: SocketAddr,
        acceptor: Option<SslAcceptor>,
    ) -> Self {
        let state = match acceptor {
            Some(acceptor) => StreamState::Accepting(
                async move {
                    let ssl = Ssl::new(acceptor.context()).context(SslBuildSnafu)?;
                    let mut stream = SslStream::new(ssl, stream).context(SslBuildSnafu)?;
                    Pin::new(&mut stream)
                        .accept()
                        .await
                        .context(HandshakeSnafu)?;
                    Ok(stream)
                }
                .boxed(),
            ),
            None => StreamState::Accepted(MaybeTlsStream::Raw(stream)),
        };
        Self {
            state,
            peer_addr,
            client_certificate_deadline: None,
        }
    }

    fn accept_tls(&mut self, stream: SslStream<TcpStream>) -> crate::tls::Result<()> {
        if stream.ssl().verify_mode().contains(SslVerifyMode::PEER) {
            // Resumption restores OpenSSL's old verify_result without checking expiry.
            let lifetime = super::peer_identity::verified_peer_certificate_lifetime(stream.ssl())
                .filter(|lifetime| *lifetime > std::time::Duration::from_secs(1))
                .and_then(|lifetime| lifetime.checked_sub(std::time::Duration::from_secs(1)))
                .ok_or(TlsError::InvalidPeerCertificate)?;
            // Close before expiry so multiplexed clients reconnect with rotated credentials
            // instead of permanently receiving non-retryable authentication failures.
            self.client_certificate_deadline = Some(Box::pin(tokio::time::sleep(lifetime)));
        }
        self.state = StreamState::Accepted(MaybeTlsStream::Tls(stream));
        Ok(())
    }

    // Explicit handshake method
    pub async fn handshake(&mut self) -> crate::tls::Result<()> {
        if let StreamState::Accepting(fut) = &mut self.state {
            let stream = fut.await?;
            self.accept_tls(stream)?;
        }

        Ok(())
    }

    pub fn set_keepalive(&mut self, keepalive: TcpKeepaliveConfig) -> io::Result<()> {
        let stream = self.get_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "Can't set keepalive on connection that has not been accepted yet.",
            )
        })?;

        if let Some(time_secs) = keepalive.time_secs {
            let config =
                socket2::TcpKeepalive::new().with_time(std::time::Duration::from_secs(time_secs));

            tcp::set_keepalive(stream, &config)?;
        }

        Ok(())
    }

    pub fn set_receive_buffer_bytes(&mut self, bytes: usize) -> std::io::Result<()> {
        let stream = self.get_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "Can't set receive buffer size on connection that has not been accepted yet.",
            )
        })?;

        tcp::set_receive_buffer_size(stream, bytes)
    }

    fn poll_io<T, F>(self: Pin<&mut Self>, cx: &mut Context, poll_fn: F) -> Poll<io::Result<T>>
    where
        F: FnOnce(Pin<&mut MaybeTlsStream<TcpStream>>, &mut Context) -> Poll<io::Result<T>>,
    {
        let this = self.get_mut();
        loop {
            if this
                .client_certificate_deadline
                .as_mut()
                .is_some_and(|deadline| deadline.as_mut().poll(cx).is_ready())
            {
                this.state = StreamState::Closed;
                return Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()));
            }
            return match &mut this.state {
                StreamState::Accepted(stream) => poll_fn(Pin::new(stream), cx),
                StreamState::Accepting(fut) => match std::task::ready!(fut.as_mut().poll(cx)) {
                    Ok(stream) => {
                        if let Err(error) = this.accept_tls(stream) {
                            this.state = StreamState::AcceptError(error.to_string());
                            return Poll::Ready(Err(io::Error::other(error)));
                        }
                        continue;
                    }
                    Err(error) => {
                        let error = io::Error::other(error);
                        this.state = StreamState::AcceptError(error.to_string());
                        Poll::Ready(Err(error))
                    }
                },
                StreamState::AcceptError(error) => {
                    Poll::Ready(Err(io::Error::other(error.clone())))
                }
                StreamState::Closed => Poll::Ready(Err(io::ErrorKind::BrokenPipe.into())),
            };
        }
    }
}

impl AsyncRead for MaybeTlsIncomingStream<TcpStream> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.poll_io(cx, |s, cx| s.poll_read(cx, buf))
    }
}

impl AsyncWrite for MaybeTlsIncomingStream<TcpStream> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.poll_io(cx, |s, cx| s.poll_write(cx, buf))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        self.poll_io(cx, AsyncWrite::poll_flush)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match &mut this.state {
            StreamState::Accepted(stream) => match Pin::new(stream).poll_shutdown(cx) {
                Poll::Ready(Ok(())) => {
                    this.state = StreamState::Closed;
                    Poll::Ready(Ok(()))
                }
                poll_result => poll_result,
            },
            StreamState::Accepting(fut) => match std::task::ready!(fut.as_mut().poll(cx)) {
                Ok(stream) => {
                    if let Err(error) = this.accept_tls(stream) {
                        return Poll::Ready(Err(io::Error::other(error)));
                    }
                    Pin::new(this).poll_shutdown(cx)
                }
                Err(error) => {
                    let error = io::Error::other(error);
                    this.state = StreamState::AcceptError(error.to_string());
                    Poll::Ready(Err(error))
                }
            },
            StreamState::AcceptError(error) => Poll::Ready(Err(io::Error::other(error.clone()))),
            StreamState::Closed => Poll::Ready(Ok(())),
        }
    }
}

#[derive(Debug)]
pub struct CertificateMetadata {
    pub country_name: Option<String>,
    pub state_or_province_name: Option<String>,
    pub locality_name: Option<String>,
    pub organization_name: Option<String>,
    pub organizational_unit_name: Option<String>,
    pub common_name: Option<String>,
}

impl CertificateMetadata {
    pub fn subject(&self) -> String {
        let mut components = Vec::<String>::with_capacity(6);
        if let Some(cn) = &self.common_name {
            components.push(format!("CN={cn}"));
        }
        if let Some(ou) = &self.organizational_unit_name {
            components.push(format!("OU={ou}"));
        }
        if let Some(o) = &self.organization_name {
            components.push(format!("O={o}"));
        }
        if let Some(l) = &self.locality_name {
            components.push(format!("L={l}"));
        }
        if let Some(st) = &self.state_or_province_name {
            components.push(format!("ST={st}"));
        }
        if let Some(c) = &self.country_name {
            components.push(format!("C={c}"));
        }
        components.join(",")
    }
}

impl From<X509> for CertificateMetadata {
    fn from(cert: X509) -> Self {
        let mut subject_metadata: HashMap<String, String> = HashMap::new();
        for entry in cert.subject_name().entries() {
            let data_string = match entry.data().as_utf8() {
                Ok(data) => data.to_string(),
                Err(_) => String::new(),
            };
            subject_metadata.insert(entry.object().to_string(), data_string);
        }
        Self {
            country_name: subject_metadata.get("countryName").cloned(),
            state_or_province_name: subject_metadata.get("stateOrProvinceName").cloned(),
            locality_name: subject_metadata.get("localityName").cloned(),
            organization_name: subject_metadata.get("organizationName").cloned(),
            organizational_unit_name: subject_metadata.get("organizationalUnitName").cloned(),
            common_name: subject_metadata.get("commonName").cloned(),
        }
    }
}

#[derive(Clone)]
pub struct MaybeTlsConnectInfo {
    pub remote_addr: SocketAddr,
    pub peer_certs: Option<Vec<Certificate>>,
    /// The client leaf, available only after a handshake that required peer verification.
    /// Callers must finish `handshake()` before taking this connection snapshot.
    pub verified_peer_certificate_der: Option<Vec<u8>>,
}

impl Connected for MaybeTlsIncomingStream<TcpStream> {
    type ConnectInfo = MaybeTlsConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        MaybeTlsConnectInfo {
            remote_addr: self.peer_addr(),
            verified_peer_certificate_der: self.ssl_stream().and_then(|stream| {
                super::peer_identity::verified_peer_certificate_der(stream.ssl())
            }),
            peer_certs: self
                .ssl_stream()
                .and_then(|s| s.ssl().peer_cert_chain())
                .map(|s| {
                    s.into_iter()
                        .filter_map(|c| c.to_pem().ok())
                        .map(Certificate::from_pem)
                        .collect()
                }),
        }
    }
}

#[cfg(test)]
mod test {
    use super::super::{
        TEST_PEM_CA_PATH, TEST_PEM_CLIENT_CRT_PATH, TEST_PEM_CLIENT_KEY_PATH, TEST_PEM_CRT_PATH,
        TEST_PEM_KEY_PATH, TlsConfig, peer_identity::verified_peer_certificate_der,
    };
    use super::*;
    use openssl::{
        asn1::Asn1Time,
        hash::MessageDigest,
        pkey::PKey,
        ssl::{SslConnector, SslConnectorBuilder, SslFiletype, SslSession, SslVersion},
    };
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn server_settings(verify: bool, trusted: bool) -> TlsSettings {
        let config = TlsConfig {
            verify_certificate: Some(verify),
            ca_file: Some(
                if trusted {
                    TEST_PEM_CA_PATH
                } else {
                    "tests/data/ca/intermediate_server/certs/intermediate_server.cert.pem"
                }
                .into(),
            ),
            crt_file: Some(TEST_PEM_CRT_PATH.into()),
            key_file: Some(TEST_PEM_KEY_PATH.into()),
            ..Default::default()
        };
        TlsSettings::from_options_base(Some(&config), true).unwrap()
    }

    fn client_connector(present: bool) -> SslConnectorBuilder {
        let mut connector = SslConnector::builder(SslMethod::tls()).unwrap();
        connector.set_ca_file(TEST_PEM_CA_PATH).unwrap();
        if present {
            connector
                .set_certificate_chain_file(TEST_PEM_CLIENT_CRT_PATH)
                .unwrap();
            connector
                .set_private_key_file(TEST_PEM_CLIENT_KEY_PATH, SslFiletype::PEM)
                .unwrap();
        }
        connector
    }

    async fn handshake(
        listener: &mut MaybeTlsListener,
        connector: &SslConnector,
        session: Option<&SslSession>,
    ) -> (
        MaybeTlsIncomingStream<TcpStream>,
        crate::tls::Result<()>,
        Result<SslStream<TcpStream>, openssl::ssl::Error>,
    ) {
        let addr = listener.local_addr().unwrap();
        let client = async {
            let mut config = connector.configure().unwrap();
            if let Some(session) = session {
                // SAFETY: callers reuse sessions only with the connector that created them.
                unsafe { config.set_session(session).unwrap() };
            }
            let socket = TcpStream::connect(addr).await.unwrap();
            let mut stream = SslStream::new(config.into_ssl("localhost").unwrap(), socket).unwrap();
            Pin::new(&mut stream).connect().await?;
            Ok(stream)
        };
        let server = async {
            let mut stream = listener.accept().await.unwrap();
            assert!(
                stream
                    .connect_info()
                    .verified_peer_certificate_der
                    .is_none()
            );
            let result = stream.handshake().await;
            (stream, result)
        };
        let ((server, result), client) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(server, client)
        })
        .await
        .unwrap();
        (server, result, client)
    }

    #[tokio::test]
    async fn connection_context_requires_a_verified_client_leaf() {
        // Verification enabled, client certificate present, trusted issuer, permissive callback.
        for (verify, present, trusted, permissive) in [
            (true, true, true, false),
            (false, true, true, false),
            (true, false, true, false),
            (true, true, false, false),
            (true, true, false, true),
        ] {
            let settings = server_settings(verify, trusted);
            let mut listener = MaybeTlsSettings::Tls(settings.clone())
                .bind(&"127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
            if permissive {
                let mut acceptor = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
                settings.apply_context_base(&mut acceptor, true).unwrap();
                acceptor.set_verify_callback(
                    SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT,
                    |_, _| true,
                );
                listener.acceptor = Some(acceptor.build());
            }
            let mut connector = client_connector(present);
            if !verify {
                connector.set_verify(SslVerifyMode::NONE);
            }
            let (stream, result, client) = handshake(&mut listener, &connector.build(), None).await;
            if verify && (!present || !trusted) {
                assert!(result.is_err());
                assert!(
                    stream
                        .connect_info()
                        .verified_peer_certificate_der
                        .is_none()
                );
                if permissive {
                    assert!(matches!(result, Err(TlsError::InvalidPeerCertificate)));
                }
            } else {
                result.unwrap();
                let client = client.unwrap();
                if !verify {
                    assert!(client.ssl().peer_certificate().is_some());
                    assert_eq!(
                        client.ssl().verify_result(),
                        openssl::x509::X509VerifyResult::OK
                    );
                    assert!(verified_peer_certificate_der(client.ssl()).is_none());
                }
                let leaf = X509::from_pem(&std::fs::read(TEST_PEM_CLIENT_CRT_PATH).unwrap())
                    .unwrap()
                    .to_der()
                    .unwrap();
                assert_eq!(
                    stream.connect_info().verified_peer_certificate_der,
                    verify.then_some(leaf)
                );
            }
        }
        let mut listener = MaybeTlsSettings::Raw(())
            .bind(&"127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let _client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        assert!(
            listener
                .accept()
                .await
                .unwrap()
                .connect_info()
                .verified_peer_certificate_der
                .is_none()
        );
    }

    #[tokio::test]
    async fn session_resumption_is_isolated_to_each_acceptor() {
        for replace_acceptor in [false, true] {
            let settings = server_settings(true, true);
            let mut listener = MaybeTlsSettings::Tls(settings.clone())
                .bind(&"127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
            let mut connector = client_connector(true);
            connector
                .set_max_proto_version(Some(SslVersion::TLS1_2))
                .unwrap();
            let connector = connector.build();
            let (first, result, client) = handshake(&mut listener, &connector, None).await;
            result.unwrap();
            let client = client.unwrap();
            assert!(!client.ssl().session_reused());
            let session = client.ssl().session().unwrap().to_owned();
            if replace_acceptor {
                listener.acceptor = Some(settings.acceptor().unwrap());
            }
            let (second, result, resumed) =
                handshake(&mut listener, &connector, Some(&session)).await;
            result.unwrap();
            assert_eq!(resumed.unwrap().ssl().session_reused(), !replace_acceptor);
            let expected = X509::from_pem(&std::fs::read(TEST_PEM_CLIENT_CRT_PATH).unwrap())
                .unwrap()
                .to_der()
                .unwrap();
            assert_eq!(
                first.connect_info().verified_peer_certificate_der,
                Some(expected.clone())
            );
            assert_eq!(
                second.connect_info().verified_peer_certificate_der,
                Some(expected)
            );
        }
    }

    #[tokio::test]
    async fn verified_connections_expire_and_expired_sessions_cannot_resume() {
        let expiry = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 6;
        // Reissue the existing client fixture so only this test needs a short validity window.
        let template = X509::from_pem(&std::fs::read(TEST_PEM_CLIENT_CRT_PATH).unwrap()).unwrap();
        let issuer = X509::from_pem(&std::fs::read(TEST_PEM_CA_PATH).unwrap()).unwrap();
        let key =
            PKey::private_key_from_pem(&std::fs::read("tests/data/ca/private/ca.key.pem").unwrap())
                .unwrap();
        let mut cert = X509::builder().unwrap();
        cert.set_version(2).unwrap();
        cert.set_serial_number(template.serial_number()).unwrap();
        cert.set_subject_name(template.subject_name()).unwrap();
        cert.set_issuer_name(issuer.subject_name()).unwrap();
        cert.set_pubkey(&template.public_key().unwrap()).unwrap();
        cert.set_not_before(&Asn1Time::days_from_now(0).unwrap())
            .unwrap();
        cert.set_not_after(&Asn1Time::from_unix(expiry as i64).unwrap())
            .unwrap();
        cert.sign(&key, MessageDigest::sha256()).unwrap();
        let mut connector = client_connector(true);
        connector.set_certificate(&cert.build()).unwrap();
        connector
            .set_max_proto_version(Some(SslVersion::TLS1_2))
            .unwrap();
        let connector = connector.build();
        let mut listener = MaybeTlsSettings::Tls(server_settings(true, true))
            .bind(&"127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let (mut stream, result, client) = handshake(&mut listener, &connector, None).await;
        result.unwrap();
        let client = client.unwrap();
        let session = client.ssl().session().unwrap().to_owned();
        let error = tokio::time::timeout(
            Duration::from_secs(10),
            tokio::io::AsyncReadExt::read(&mut stream, &mut [0]),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionAborted);
        let until_expired = Duration::from_secs(expiry + 1)
            .saturating_sub(SystemTime::now().duration_since(UNIX_EPOCH).unwrap());
        tokio::time::sleep(until_expired).await;
        let (_stream, result, resumed) = handshake(&mut listener, &connector, Some(&session)).await;
        assert!(resumed.unwrap().ssl().session_reused());
        assert!(matches!(result, Err(TlsError::InvalidPeerCertificate)));
    }

    #[test]
    fn certificate_metadata_full() {
        let example_meta = CertificateMetadata {
            common_name: Some("common".to_owned()),
            country_name: Some("country".to_owned()),
            locality_name: Some("locality".to_owned()),
            organization_name: Some("organization".to_owned()),
            organizational_unit_name: Some("org_unit".to_owned()),
            state_or_province_name: Some("state".to_owned()),
        };

        let expected = format!(
            "CN={},OU={},O={},L={},ST={},C={}",
            example_meta.common_name.as_ref().unwrap(),
            example_meta.organizational_unit_name.as_ref().unwrap(),
            example_meta.organization_name.as_ref().unwrap(),
            example_meta.locality_name.as_ref().unwrap(),
            example_meta.state_or_province_name.as_ref().unwrap(),
            example_meta.country_name.as_ref().unwrap()
        );
        assert_eq!(expected, example_meta.subject());
    }

    #[test]
    fn certificate_metadata_partial() {
        let example_meta = CertificateMetadata {
            common_name: Some("common".to_owned()),
            country_name: Some("country".to_owned()),
            locality_name: None,
            organization_name: Some("organization".to_owned()),
            organizational_unit_name: Some("org_unit".to_owned()),
            state_or_province_name: None,
        };

        let expected = format!(
            "CN={},OU={},O={},C={}",
            example_meta.common_name.as_ref().unwrap(),
            example_meta.organizational_unit_name.as_ref().unwrap(),
            example_meta.organization_name.as_ref().unwrap(),
            example_meta.country_name.as_ref().unwrap()
        );
        assert_eq!(expected, example_meta.subject());
    }
}
