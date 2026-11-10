use std::{convert::Infallible, net::SocketAddr, time::Duration};

use futures::FutureExt;
use http::{Request, Response};
use hyper::Body;
use tonic::{
    body::BoxBody,
    server::NamedService,
    transport::server::{Routes, Server},
};
use tower::Service;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::{
    classify::{GrpcErrorsAsFailures, SharedClassifier},
    trace::TraceLayer,
};
use tracing::Span;

use crate::{
    internal_events::{GrpcServerRequestReceived, GrpcServerResponseSent},
    shutdown::{ShutdownSignal, ShutdownSignalToken},
    tls::{MaybeTlsListener, MaybeTlsSettings},
};

mod decompression;
pub use self::decompression::{DecompressionAndMetrics, DecompressionAndMetricsLayer};

pub async fn run_grpc_server<S>(
    address: SocketAddr,
    tls_settings: MaybeTlsSettings,
    service: S,
    shutdown: ShutdownSignal,
    max_concurrent_requests: Option<usize>,
) -> crate::Result<()>
where
    S: Service<Request<Body>, Response = Response<BoxBody>, Error = Infallible>
        + NamedService
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    let span = Span::current();
    let (tx, rx) = tokio::sync::oneshot::channel::<ShutdownSignalToken>();
    let listener = tls_settings.bind(&address).await?;
    let stream = listener.accept_stream();

    info!(%address, "Building gRPC server.");
    if let Some(max_concurrent_requests) = max_concurrent_requests {
        Server::builder()
            // Ok to use ConcurrencyLimitLayer because service will be crated once in Server::serve_with_shutdown using all these layers.
            .layer(ConcurrencyLimitLayer::new(max_concurrent_requests))
            .layer(build_grpc_trace_layer(span.clone()))
            // This layer explicitly decompresses payloads, if compressed, and reports the number of message bytes we've
            // received if the message is processed successfully, aka `BytesReceived`. We do this because otherwise the only
            // access we have is either the event-specific bytes (the in-memory representation) or the raw bytes over the
            // wire prior to decompression... and if that case, any bytes at all, not just the ones we successfully process.
            //
            // The weaving of `tonic`, `axum`, `tower`, and `hyper` is fairly complex and there currently exists no way to
            // use independent `tower` layers when the request body itself (the body type, not the actual bytes) must be
            // modified or wrapped.. so instead of a cleaner design, we're opting here to bake it all together until the
            // crates are sufficiently flexible for us to craft a better design.
            .layer(DecompressionAndMetricsLayer)
            .add_service(service)
            .serve_with_incoming_shutdown(stream, shutdown.map(|token| tx.send(token).unwrap()))
            .await?;
    } else {
        Server::builder()
            .layer(build_grpc_trace_layer(span.clone()))
            // This layer explicitly decompresses payloads, if compressed, and reports the number of message bytes we've
            // received if the message is processed successfully, aka `BytesReceived`. We do this because otherwise the only
            // access we have is either the event-specific bytes (the in-memory representation) or the raw bytes over the
            // wire prior to decompression... and if that case, any bytes at all, not just the ones we successfully process.
            //
            // The weaving of `tonic`, `axum`, `tower`, and `hyper` is fairly complex and there currently exists no way to
            // use independent `tower` layers when the request body itself (the body type, not the actual bytes) must be
            // modified or wrapped.. so instead of a cleaner design, we're opting here to bake it all together until the
            // crates are sufficiently flexible for us to craft a better design.
            .layer(DecompressionAndMetricsLayer)
            .add_service(service)
            .serve_with_incoming_shutdown(stream, shutdown.map(|token| tx.send(token).unwrap()))
            .await?;
    }

    drop(rx.await);

    Ok(())
}

// This is a bit of a ugly hack to allow us to run two services on the same port.
// I just don't know how to convert the generic type with associated types into a Vec<Box<trait object>>.
pub async fn run_grpc_server_with_routes(
    address: SocketAddr,
    listener: MaybeTlsListener,
    routes: Routes,
    authenticate_peer: bool,
    shutdown: ShutdownSignal,
) -> crate::Result<()> {
    let span = Span::current();
    let (tx, rx) = tokio::sync::oneshot::channel::<ShutdownSignalToken>();
    // Tonic snapshots Connected before reading, so authenticate before yielding.
    // Keep accepting while handshakes run; saturated peers are closed rather than
    // holding the listener's backlog until the handshake timeout.
    let stream = authenticated_incoming(
        listener,
        authenticate_peer,
        std::sync::Arc::new(tokio::sync::Semaphore::new(64)),
        Duration::from_secs(10),
    );

    info!(%address, "Building gRPC server.");

    Server::builder()
        .layer(build_grpc_trace_layer(span.clone()))
        .layer(DecompressionAndMetricsLayer)
        .add_routes(routes)
        .serve_with_incoming_shutdown(stream, shutdown.map(|token| tx.send(token).unwrap()))
        .await?;

    drop(rx.await);

    Ok(())
}

fn authenticated_incoming(
    mut listener: MaybeTlsListener,
    authenticate_peer: bool,
    permits: std::sync::Arc<tokio::sync::Semaphore>,
    handshake_timeout: Duration,
) -> impl futures::Stream<
    Item = Result<crate::tls::MaybeTlsIncomingStream<tokio::net::TcpStream>, crate::tls::TlsError>,
> {
    async_stream::stream! {
        let mut handshakes = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                result = listener.accept() => {
                    let mut stream = match result {
                        Ok(stream) => stream,
                        Err(error) => { yield Err(error); continue; }
                    };
                    if !authenticate_peer {
                        yield Ok(stream);
                        continue;
                    }
                    let peer_addr = stream.peer_addr();
                    let Ok(permit) = std::sync::Arc::clone(&permits).try_acquire_owned() else {
                        warn!(message = "gRPC TLS handshake capacity reached.", %peer_addr, internal_log_rate_limit = true);
                        continue;
                    };
                    handshakes.spawn(async move {
                        let result = match tokio::time::timeout(handshake_timeout, stream.handshake()).await {
                            Ok(Ok(())) => Some(stream),
                            Ok(Err(error)) => {
                                warn!(message = "gRPC TLS handshake failed.", %peer_addr, %error, internal_log_rate_limit = true);
                                None
                            }
                            Err(error) => {
                                warn!(message = "gRPC TLS handshake timed out.", %peer_addr, %error, internal_log_rate_limit = true);
                                None
                            }
                        };
                        // Bound completed handshakes waiting to be yielded as well as active ones.
                        (result, permit)
                    });
                }
                Some(result) = handshakes.join_next(), if !handshakes.is_empty() => {
                    match result {
                        Ok((Some(stream), _permit)) => yield Ok(stream),
                        Ok((None, _permit)) => {}
                        Err(error) => error!(message = "gRPC TLS handshake task failed.", %error),
                    }
                }
            }
        }
    }
}

/// Builds a [TraceLayer] configured for a gRPC server.
///
/// This layer emits gPRC specific telemetry for messages received/sent and handler duration.
pub fn build_grpc_trace_layer(
    span: Span,
) -> TraceLayer<
    SharedClassifier<GrpcErrorsAsFailures>,
    impl Fn(&Request<Body>) -> Span + Clone,
    impl Fn(&Request<Body>, &Span) + Clone,
    impl Fn(&Response<BoxBody>, Duration, &Span) + Clone,
    (),
    (),
    (),
> {
    TraceLayer::new_for_grpc()
        .make_span_with(move |request: &Request<Body>| {
            // The path is defined as “/” {service name} “/” {method name}.
            let mut path = request.uri().path().split('/');
            let service = path.nth(1).unwrap_or("_unknown");
            let method = path.next().unwrap_or("_unknown");

            // This is an error span so that the labels are always present for metrics.
            error_span!(
               parent: &span,
               "grpc-request",
               grpc_service = service,
               grpc_method = method,
            )
        })
        .on_request(Box::new(|_request: &Request<Body>, _span: &Span| {
            emit!(GrpcServerRequestReceived);
        }))
        .on_response(
            |response: &Response<BoxBody>, latency: Duration, _span: &Span| {
                emit!(GrpcServerResponseSent { response, latency });
            },
        )
        .on_failure(())
        .on_body_chunk(())
        .on_eos(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::sync::Arc;
    use tokio::{io::AsyncReadExt, sync::Semaphore};

    async fn wait_for_permits(permits: &Semaphore, expected: usize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while permits.available_permits() != expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn saturated_handshakes_close_excess_peers_and_recover_after_timeout() {
        let settings = MaybeTlsSettings::from_config(
            Some(&crate::tls::TlsEnableableConfig::test_config()),
            true,
        )
        .unwrap();
        let listener = settings
            .bind(&"127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let permits = Arc::new(Semaphore::new(1));
        let stream = authenticated_incoming(
            listener,
            true,
            Arc::clone(&permits),
            Duration::from_millis(500),
        );
        let server = tokio::spawn(async move {
            futures::pin_mut!(stream);
            stream.next().await.unwrap().unwrap()
        });
        let idle = tokio::net::TcpStream::connect(address).await.unwrap();
        wait_for_permits(&permits, 0).await;
        let mut excess = tokio::net::TcpStream::connect(address).await.unwrap();
        let read = tokio::time::timeout(Duration::from_millis(300), excess.read(&mut [0]))
            .await
            .expect("accept must drain excess connections before the handshake timeout");
        assert!(matches!(read, Ok(0) | Err(_)));
        wait_for_permits(&permits, 1).await;
        drop(idle);
        let client = tokio::task::spawn_blocking(move || {
            let mut connector =
                openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls()).unwrap();
            connector
                .cert_store_mut()
                .add_cert(
                    openssl::x509::X509::from_pem(
                        &std::fs::read(crate::tls::TEST_PEM_CA_PATH).unwrap(),
                    )
                    .unwrap(),
                )
                .unwrap();
            connector
                .build()
                .connect("localhost", std::net::TcpStream::connect(address).unwrap())
                .unwrap()
        });
        let _accepted = tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
        let _client = client.await.unwrap();
    }
}
