use std::path::Path;

use bytes::{Buf, BytesMut};
use http::{HeaderMap, Request as HttpRequest, Uri};
use hyper::Body;
use hyper::body::HttpBody;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, prost::Message};
use snafu::ResultExt;

use super::config::ServiceTlsConfig;
use super::error::{
    BuildDescriptorPoolSnafu, BuildGrpcHttpRequestSnafu, DecodeDescriptorSetSnafu,
    DecodeResponseSnafu, HttpsConnectorSnafu, IncompleteResponseMessageSnafu, InvalidGrpcUriSnafu,
    LoadCaFileSnafu, LoadClientCertSnafu, LoadClientKeySnafu, ReadDescriptorSnafu,
    ReadResponseBodySnafu, ResponseTooShortSnafu, SslBuilderSnafu, TokenManagerError,
    TransportSnafu, UnexpectedCompressionSnafu,
};

/// HTTP client used for every gRPC call issued by the token manager.
pub(crate) type GrpcHttpClient =
    hyper::Client<hyper_openssl::HttpsConnector<hyper::client::HttpConnector>>;

/// Load a runtime-mounted `FileDescriptorSet` into a `DescriptorPool`.
pub(crate) async fn load_descriptor_pool(path: &Path) -> Result<DescriptorPool, TokenManagerError> {
    let descriptor_bytes = tokio::fs::read(path).await.context(ReadDescriptorSnafu {
        path: path.to_path_buf(),
    })?;
    let fds = prost_reflect::prost_types::FileDescriptorSet::decode(&descriptor_bytes[..])
        .context(DecodeDescriptorSetSnafu)?;
    DescriptorPool::from_file_descriptor_set(fds).context(BuildDescriptorPoolSnafu)
}

/// Build a hyper-openssl client configured with the agent's mTLS cert + an SNI
/// override (matching the bricklens_ingest sink pattern). The TCP connection goes
/// to the request URI; SNI is set to `server_name` for the s2s-proxy sidecar to
/// route correctly. Hostname verification checks the request URI's hostname,
/// independently of SNI. With `verify_hostname` false the certificate chain is
/// still verified, but not its hostname. The connection requires TLS 1.3.
pub(crate) fn build_hyper_openssl_client(
    tls: &ServiceTlsConfig,
    server_name: &str,
) -> Result<
    hyper::Client<hyper_openssl::HttpsConnector<hyper::client::HttpConnector>>,
    TokenManagerError,
> {
    let mut http_connector = hyper::client::HttpConnector::new();
    http_connector.enforce_http(false);

    let mut ssl_builder = openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls())
        .context(SslBuilderSnafu)?;

    // Login and UC are internal hops, so refuse to negotiate anything below TLS 1.3.
    ssl_builder
        .set_min_proto_version(Some(openssl::ssl::SslVersion::TLS1_3))
        .context(SslBuilderSnafu)?;

    ssl_builder
        .set_certificate_file(&tls.crt_file, openssl::ssl::SslFiletype::PEM)
        .context(LoadClientCertSnafu {
            path: tls.crt_file.clone(),
        })?;
    ssl_builder
        .set_private_key_file(&tls.key_file, openssl::ssl::SslFiletype::PEM)
        .context(LoadClientKeySnafu {
            path: tls.key_file.clone(),
        })?;
    if let Some(ref ca_file) = tls.ca_file {
        ssl_builder.set_ca_file(ca_file).context(LoadCaFileSnafu {
            path: ca_file.clone(),
        })?;
    }

    // The client speaks HTTP/2 only; advertise it before the server selects a protocol.
    ssl_builder
        .set_alpn_protos(b"\x02h2")
        .context(SslBuilderSnafu)?;

    let mut https_connector =
        hyper_openssl::HttpsConnector::with_connector(http_connector, ssl_builder)
            .context(HttpsConnectorSnafu)?;

    let server_name = server_name.to_string();
    let verify_hostname = tls.verify_hostname;
    https_connector.set_callback(move |connection, _uri| {
        // Endpoint is typically the s2s-proxy sidecar (e.g. 127.0.0.3); the SNI must
        // be set explicitly to the privileged DBNS hostname so the sidecar routes to
        // the right backend. This mirrors bricklens_ingest sink.rs.
        connection.set_use_server_name_indication(false);
        connection.set_hostname(&server_name)?;
        connection.set_verify_hostname(verify_hostname);
        Ok(())
    });

    Ok(hyper::Client::builder()
        .http2_only(true)
        .build(https_connector))
}

/// Issue a unary gRPC call and return the decoded response message.
///
/// `extra_headers` are added verbatim; binary (`-bin`) header values must already be
/// base64-encoded.
pub(crate) async fn grpc_unary_call(
    client: &GrpcHttpClient,
    uri: Uri,
    extra_headers: &[(&str, String)],
    request: &DynamicMessage,
    output_desc: &MessageDescriptor,
) -> Result<DynamicMessage, TokenManagerError> {
    let mut builder = HttpRequest::builder()
        .uri(uri)
        .method("POST")
        .header("content-type", "application/grpc+proto")
        .header("te", "trailers")
        .header("grpc-encoding", "identity");
    for (name, value) in extra_headers {
        builder = builder.header(*name, value);
    }
    let http_req = builder
        .body(Body::from(encode_grpc_message(request.encode_to_vec())))
        .context(BuildGrpcHttpRequestSnafu)?;

    let response = client.request(http_req).await.context(TransportSnafu)?;
    let (parts, mut body) = response.into_parts();

    // A trailers-only error response carries its status in the headers.
    if let Some(error) = grpc_status_error(&parts.headers) {
        return Err(error);
    }

    let mut data = BytesMut::new();
    while let Some(chunk) = body.data().await {
        data.extend_from_slice(&chunk.context(ReadResponseBodySnafu)?);
    }

    // Otherwise the status arrives in the trailers. Check it before parsing: a failed call
    // has no message, which would otherwise surface as a misleading framing error.
    let trailers = body.trailers().await.context(ReadResponseBodySnafu)?;
    if let Some(error) = trailers.as_ref().and_then(grpc_status_error) {
        return Err(error);
    }

    parse_grpc_response_message(data.freeze(), output_desc)
}

/// Return the error described by a non-zero `grpc-status` in `headers`, if any.
pub(super) fn grpc_status_error(headers: &HeaderMap) -> Option<TokenManagerError> {
    let status = headers
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i32>().ok())
        .filter(|status| *status != 0)?;
    let message = headers
        .get("grpc-message")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown grpc error")
        .to_string();
    Some(TokenManagerError::GrpcStatus { status, message })
}

/// Build the request URI for `method` on `endpoint`.
pub(crate) fn grpc_method_uri(
    endpoint: &Uri,
    method: &prost_reflect::MethodDescriptor,
) -> Result<Uri, TokenManagerError> {
    let uri_str = format!(
        "{}/{}/{}",
        endpoint.to_string().trim_end_matches('/'),
        method.parent_service().full_name(),
        method.name()
    );
    uri_str
        .parse::<Uri>()
        .context(InvalidGrpcUriSnafu { uri: uri_str })
}

/// Wrap a serialized protobuf message with the 5-byte gRPC framing prefix
/// (1 byte compression flag + 4 bytes big-endian length).
pub(super) fn encode_grpc_message(message: Vec<u8>) -> Vec<u8> {
    let len = message.len() as u32;
    let mut framed = Vec::with_capacity(5 + message.len());
    framed.push(0); // compression flag: 0 = uncompressed
    framed.extend_from_slice(&len.to_be_bytes());
    framed.extend_from_slice(&message);
    framed
}

/// Decode a gRPC unary response body into a `DynamicMessage` typed by `output_desc`.
pub(super) fn parse_grpc_response_message(
    body: bytes::Bytes,
    output_desc: &MessageDescriptor,
) -> Result<DynamicMessage, TokenManagerError> {
    let mut buf = body;
    if buf.remaining() < 5 {
        return ResponseTooShortSnafu.fail();
    }
    let compression_flag = buf.get_u8();
    if compression_flag != 0 {
        return UnexpectedCompressionSnafu {
            flag: compression_flag,
        }
        .fail();
    }
    let message_len = buf.get_u32() as usize;
    if buf.remaining() < message_len {
        return IncompleteResponseMessageSnafu.fail();
    }
    let message_bytes = buf.copy_to_bytes(message_len);
    DynamicMessage::decode(output_desc.clone(), message_bytes.as_ref()).context(DecodeResponseSnafu)
}
