use bytes::Buf;
use prost_reflect::{DynamicMessage, MessageDescriptor};
use snafu::ResultExt;

use super::config::LoginServiceAuthConfig;
use super::error::{
    DecodeResponseSnafu, HttpsConnectorSnafu, IncompleteResponseMessageSnafu, LoadCaFileSnafu,
    LoadClientCertSnafu, LoadClientKeySnafu, ResponseTooShortSnafu, SslBuilderSnafu,
    TokenManagerError, UnexpectedCompressionSnafu,
};

/// Build a hyper-openssl client configured with the agent's mTLS cert + Login's
/// SNI override (matching the bricklens_ingest sink pattern). The TCP connection
/// goes to `endpoint`; SNI is set to `login_server_name` for the s2s-proxy sidecar
/// to route correctly.
pub(super) fn build_hyper_openssl_client(
    config: &LoginServiceAuthConfig,
) -> Result<
    hyper::Client<hyper_openssl::HttpsConnector<hyper::client::HttpConnector>>,
    TokenManagerError,
> {
    let mut http_connector = hyper::client::HttpConnector::new();
    http_connector.enforce_http(false);

    let mut ssl_builder = openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls())
        .context(SslBuilderSnafu)?;

    ssl_builder
        .set_certificate_file(&config.login_service.tls_crt_file, openssl::ssl::SslFiletype::PEM)
        .context(LoadClientCertSnafu {
            path: config.login_service.tls_crt_file.clone(),
        })?;
    ssl_builder
        .set_private_key_file(&config.login_service.tls_key_file, openssl::ssl::SslFiletype::PEM)
        .context(LoadClientKeySnafu {
            path: config.login_service.tls_key_file.clone(),
        })?;
    if let Some(ref ca_file) = config.login_service.tls_ca_file {
        ssl_builder.set_ca_file(ca_file).context(LoadCaFileSnafu {
            path: ca_file.clone(),
        })?;
    }

    let mut https_connector =
        hyper_openssl::HttpsConnector::with_connector(http_connector, ssl_builder)
            .context(HttpsConnectorSnafu)?;

    let server_name = config.login_service.login_server_name.clone();
    https_connector.set_callback(move |connection, _uri| {
        // Endpoint is typically the s2s-proxy sidecar (e.g. 127.0.0.3); the SNI must
        // be set explicitly to the privileged DBNS hostname so the sidecar routes to
        // the right backend. This mirrors bricklens_ingest sink.rs.
        connection.set_use_server_name_indication(false);
        connection.set_hostname(&server_name)?;
        Ok(())
    });

    Ok(hyper::Client::builder()
        .http2_only(true)
        .build(https_connector))
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
