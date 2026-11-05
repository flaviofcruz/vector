use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use prost_reflect::{
    DescriptorPool, DynamicMessage, MessageDescriptor, ReflectMessage, Value, prost::Message,
};

use super::cache::CachedPrincipalContext;
use super::config::{
    LoginServiceAuthConfig, LoginServiceConfig, MIN_TOKEN_VALIDITY, ServiceTlsConfig,
    TOKEN_REFRESH_THRESHOLD, UcPermissionEntry, UserAuthInfo,
};
use super::error::TokenManagerError;
use super::request::{PrincipalContextRequestKind, build_request_message};
use super::response::{
    message_field_bytes, optional_message_field_bytes, principal_context_expiry,
    principal_context_v2_wire_bytes,
};
use super::transport::{encode_grpc_message, grpc_status_error, parse_grpc_response_message};

// ---- Descriptor pool ----
//
// Login's real `PrincipalContextService` descriptor set, the same file the vector image mounts
// at `/etc/proto/principal-context/service_proto_descriptor.pb`. See
// `tests/data/databricks/README.md` to regenerate it.

const PRINCIPAL_CONTEXT_DESCRIPTOR_SET: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/data/databricks/principal_context_service_proto_descriptor.pb"
));

fn make_test_pool() -> &'static DescriptorPool {
    static POOL: OnceLock<DescriptorPool> = OnceLock::new();
    POOL.get_or_init(|| {
        DescriptorPool::decode(PRINCIPAL_CONTEXT_DESCRIPTOR_SET)
            .expect("principal-context descriptor set")
    })
}

fn get_message(name: &str) -> MessageDescriptor {
    make_test_pool()
        .get_message_by_name(name)
        .unwrap_or_else(|| panic!("{name} in principal-context descriptor set"))
}

fn cfg(workspace_id: i64, perms: Vec<UcPermissionEntry>) -> LoginServiceAuthConfig {
    LoginServiceAuthConfig {
        user: UserAuthInfo {
            workspace_id,
            service_principal_resource: "accounts/u/apps".into(),
            service_principal_type: 3,
            uc_permissions: perms,
        },
        login_service: LoginServiceConfig {
            login_endpoint: "https://login:443".into(),
            login_server_name: "login".into(),
            proto_descriptor_path: PathBuf::from("/dev/null"),
        },
        tls: ServiceTlsConfig {
            crt_file: "/dev/null".into(),
            key_file: "/dev/null".into(),
            ca_file: None,
            verify_hostname: true,
        },
        metastore_id: "metastore-1".into(),
        zerobus_ssp_resource_path: None,
        unity_catalog: None,
    }
}

fn request_descriptor() -> MessageDescriptor {
    get_message("databricks.identity.PrincipalContextRequest")
}

fn response_descriptor() -> MessageDescriptor {
    get_message("databricks.identity.PrincipalContextResponse")
}

// ---- request-shape tests against the dynamic builder ----

#[test]
fn build_request_sets_workspace_scope_oneof() {
    let request_desc = request_descriptor();
    let msg = build_request_message(
        &cfg(848_677_326_595_393, vec![]),
        &request_desc,
        PrincipalContextRequestKind::ServicePrincipal,
    )
    .unwrap();

    let auth = msg.get_field_by_name("authentication").unwrap();
    let auth_msg = match &*auth {
        Value::Message(m) => m.clone(),
        other => panic!("expected authentication to be a message, got {:?}", other),
    };
    let workspace = auth_msg.get_field_by_name("workspace").unwrap();
    match &*workspace {
        Value::I64(w) => assert_eq!(*w, 848_677_326_595_393),
        other => panic!("expected workspace I64, got {:?}", other),
    }
}

#[test]
fn build_request_sets_service_method_oneof() {
    let request_desc = request_descriptor();
    let msg = build_request_message(
        &cfg(1, vec![]),
        &request_desc,
        PrincipalContextRequestKind::ServicePrincipal,
    )
    .unwrap();

    let auth = msg.get_field_by_name("authentication").unwrap();
    let auth_msg = match &*auth {
        Value::Message(m) => m.clone(),
        _ => panic!(),
    };
    let svc = auth_msg.get_field_by_name("service").unwrap();
    let svc_msg = match &*svc {
        Value::Message(m) => m.clone(),
        other => panic!("expected service to be a message, got {:?}", other),
    };
    let sp_type = svc_msg.get_field_by_name("service_principal_type").unwrap();
    match &*sp_type {
        Value::EnumNumber(v) => assert_eq!(*v, 3),
        other => panic!("expected service_principal_type enum, got {:?}", other),
    }
    let sp_resource = svc_msg
        .get_field_by_name("service_principal_resource")
        .unwrap();
    match &*sp_resource {
        Value::String(s) => assert_eq!(s, "accounts/u/apps"),
        other => panic!(
            "expected service_principal_resource String, got {:?}",
            other
        ),
    }
}

#[test]
fn build_request_emits_one_uc_permission_per_entry() {
    let perms = vec![
        UcPermissionEntry {
            privileges: vec!["USE_CATALOG".into()],
            securable_type: "CATALOG".into(),
            full_name: "main".into(),
        },
        UcPermissionEntry {
            privileges: vec!["USE_SCHEMA".into()],
            securable_type: "SCHEMA".into(),
            full_name: "main.default".into(),
        },
        UcPermissionEntry {
            privileges: vec!["SELECT".into(), "MODIFY".into()],
            securable_type: "TABLE".into(),
            full_name: "main.default.otel_logs".into(),
        },
    ];
    let request_desc = request_descriptor();
    let msg = build_request_message(
        &cfg(1, perms),
        &request_desc,
        PrincipalContextRequestKind::ServicePrincipal,
    )
    .unwrap();

    let authz = msg.get_field_by_name("authorization").unwrap();
    let authz_msg = match &*authz {
        Value::Message(m) => m.clone(),
        _ => panic!(),
    };
    let constraint = authz_msg
        .get_field_by_name("authorization_constraint")
        .unwrap();
    let constraint_msg = match &*constraint {
        Value::Message(m) => m.clone(),
        _ => panic!(),
    };
    let uc_pc = constraint_msg
        .get_field_by_name("uc_permission_constraint")
        .unwrap();
    let uc_pc_msg = match &*uc_pc {
        Value::Message(m) => m.clone(),
        _ => panic!(),
    };
    let perms_field = uc_pc_msg
        .get_field_by_name("uc_securable_permission")
        .unwrap();
    let perm_list = match &*perms_field {
        Value::List(l) => l.clone(),
        other => panic!("expected list, got {:?}", other),
    };
    assert_eq!(perm_list.len(), 3);

    // first entry
    let first = match &perm_list[0] {
        Value::Message(m) => m.clone(),
        _ => panic!(),
    };
    let privileges = first.get_field_by_name("privileges").unwrap();
    match &*privileges {
        Value::List(l) => {
            assert_eq!(l.len(), 1);
            match &l[0] {
                Value::String(s) => assert_eq!(s, "USE_CATALOG"),
                _ => panic!(),
            }
        }
        _ => panic!(),
    }

    // last entry: SELECT + MODIFY on the otel_logs table
    let last = match &perm_list[2] {
        Value::Message(m) => m.clone(),
        _ => panic!(),
    };
    let privileges = last.get_field_by_name("privileges").unwrap();
    match &*privileges {
        Value::List(l) => {
            assert_eq!(l.len(), 2);
            let names: Vec<String> = l
                .iter()
                .map(|v| match v {
                    Value::String(s) => s.clone(),
                    _ => String::new(),
                })
                .collect();
            assert_eq!(names, vec!["SELECT".to_string(), "MODIFY".to_string()]);
        }
        _ => panic!(),
    }
    let sec_ref = last.get_field_by_name("securable_reference").unwrap();
    let sec_ref_msg = match &*sec_ref {
        Value::Message(m) => m.clone(),
        _ => panic!(),
    };
    let full_name = sec_ref_msg.get_field_by_name("full_name").unwrap();
    match &*full_name {
        Value::String(s) => assert_eq!(s, "main.default.otel_logs"),
        _ => panic!(),
    }
}

#[test]
fn build_request_with_no_uc_permissions_omits_uc_permission_constraint() {
    let request_desc = request_descriptor();
    let msg = build_request_message(
        &cfg(1, vec![]),
        &request_desc,
        PrincipalContextRequestKind::ServicePrincipal,
    )
    .unwrap();

    // authorization is set, with an (empty) authorization_constraint, but no
    // uc_permission_constraint nested inside.
    let authz = msg.get_field_by_name("authorization").unwrap();
    let authz_msg = match &*authz {
        Value::Message(m) => m.clone(),
        _ => panic!(),
    };
    let constraint = authz_msg
        .get_field_by_name("authorization_constraint")
        .unwrap();
    let constraint_msg = match &*constraint {
        Value::Message(m) => m.clone(),
        _ => panic!(),
    };
    // Field not set on the underlying message → has_field returns false.
    let uc_pc_field = constraint_msg
        .descriptor()
        .get_field_by_name("uc_permission_constraint")
        .expect("uc_permission_constraint field");
    assert!(
        !constraint_msg.has_field(&uc_pc_field),
        "uc_permission_constraint should not be set when uc_permissions is empty"
    );
}

// ---- gRPC framing tests ----

#[test]
fn encode_grpc_message_adds_5_byte_framing() {
    let payload = b"hello".to_vec();
    let framed = encode_grpc_message(payload.clone());
    assert_eq!(framed.len(), 5 + payload.len());
    assert_eq!(framed[0], 0, "compression flag must be 0 (uncompressed)");
    let len = u32::from_be_bytes([framed[1], framed[2], framed[3], framed[4]]);
    assert_eq!(len as usize, payload.len());
    assert_eq!(&framed[5..], payload.as_slice());
}

#[test]
fn parse_grpc_response_message_round_trip() {
    let response_desc = response_descriptor();

    let response = response_with(Some(b"pc"), Some(4_000_000_000));
    let framed = encode_grpc_message(response.encode_to_vec());

    let parsed = parse_grpc_response_message(bytes::Bytes::from(framed), &response_desc).unwrap();
    assert_eq!(
        message_field_bytes(&parsed, "principal_context").unwrap(),
        message_field_bytes(&response, "principal_context").unwrap()
    );
}

#[test]
fn parse_grpc_response_message_rejects_short_body() {
    let response_desc = response_descriptor();
    let body = bytes::Bytes::from(vec![0x00, 0x00]); // 2 bytes, need 5
    let err = parse_grpc_response_message(body, &response_desc).unwrap_err();
    assert!(
        matches!(err, TokenManagerError::ResponseTooShort),
        "unexpected error: {:?}",
        err
    );
}

#[test]
fn parse_grpc_response_message_rejects_compressed_flag() {
    let response_desc = response_descriptor();
    let body = bytes::Bytes::from(vec![0x01, 0x00, 0x00, 0x00, 0x00]);
    let err = parse_grpc_response_message(body, &response_desc).unwrap_err();
    assert!(
        matches!(err, TokenManagerError::UnexpectedCompression { flag: 1 }),
        "unexpected error: {:?}",
        err
    );
}

// ---- Cache TTL tests ----

#[test]
fn cached_token_is_valid_far_in_future() {
    let now = Instant::now();
    let cached = CachedPrincipalContext {
        principal_context: vec![],
        signed_principal_context: vec![],
        signed_principal_context_v2: None,
        expires_at: now + Duration::from_secs(3600),
    };
    assert!(cached.is_valid_at(now));
}

#[test]
fn cached_token_not_valid_when_already_expired() {
    let now = Instant::now();
    let cached = CachedPrincipalContext {
        principal_context: vec![],
        signed_principal_context: vec![],
        signed_principal_context_v2: None,
        // Saturating-sub keeps this on the past side of `now` without panicking
        // on monotonic clocks that can't represent times before the process start.
        expires_at: now - Duration::from_secs(1),
    };
    assert!(!cached.is_valid_at(now));
}

#[test]
fn cached_token_not_valid_within_refresh_threshold() {
    let now = Instant::now();
    // Expires just inside the refresh threshold → should refresh.
    let cached = CachedPrincipalContext {
        principal_context: vec![],
        signed_principal_context: vec![],
        signed_principal_context_v2: None,
        expires_at: now + TOKEN_REFRESH_THRESHOLD - Duration::from_secs(1),
    };
    assert!(!cached.is_valid_at(now));
}

#[test]
fn cached_token_valid_just_past_refresh_threshold() {
    let now = Instant::now();
    let cached = CachedPrincipalContext {
        principal_context: vec![],
        signed_principal_context: vec![],
        signed_principal_context_v2: None,
        expires_at: now + TOKEN_REFRESH_THRESHOLD + Duration::from_secs(1),
    };
    assert!(cached.is_valid_at(now));
}

#[test]
fn cached_token_below_min_validity_is_rejected() {
    let now = Instant::now();
    // A freshly minted token with less than MIN_TOKEN_VALIDITY remaining must not be served:
    // bootstrap() rejects it as TokenTooShortLived rather than risk a mid-write expiry.
    let cached = CachedPrincipalContext {
        principal_context: vec![],
        signed_principal_context: vec![],
        signed_principal_context_v2: None,
        expires_at: now + MIN_TOKEN_VALIDITY - Duration::from_secs(1),
    };
    assert!(!cached.has_remaining_lifetime(now, MIN_TOKEN_VALIDITY));
}

#[test]
fn cached_token_above_min_validity_is_accepted() {
    let now = Instant::now();
    let cached = CachedPrincipalContext {
        principal_context: vec![],
        signed_principal_context: vec![],
        signed_principal_context_v2: None,
        expires_at: now + MIN_TOKEN_VALIDITY + Duration::from_secs(1),
    };
    assert!(cached.has_remaining_lifetime(now, MIN_TOKEN_VALIDITY));
}

// ---- CreatePrincipalContext request/response ----

fn message(desc: MessageDescriptor, fields: &[(&str, Value)]) -> DynamicMessage {
    let mut msg = DynamicMessage::new(desc);
    for (name, value) in fields {
        msg.set_field_by_name(name, value.clone());
    }
    msg
}

/// A `PrincipalContextResponse` with an unsigned context expiring at `exp` (unix seconds),
/// and a signed wrapper whose payload is `signed_payload`.
fn response_with(signed_payload: Option<&[u8]>, exp: Option<i64>) -> DynamicMessage {
    let mut details = Vec::new();
    if let Some(exp) = exp {
        let ts = message(
            get_message("google.protobuf.Timestamp"),
            &[("seconds", Value::I64(exp))],
        );
        details.push(("token_expiration", Value::Message(ts)));
    }
    let details = message(
        get_message("databricks.identity.AuthenticationDetails"),
        &details,
    );
    let pc = message(
        get_message("databricks.identity.PrincipalContext"),
        &[("authentication_details", Value::Message(details))],
    );
    let mut fields = vec![("principal_context", Value::Message(pc))];
    if let Some(payload) = signed_payload {
        let wrapper = message(
            get_message("databricks.s2s.metadata.ProtectedMetadataWrapper"),
            &[
                ("header", Value::Bytes(b"header".to_vec().into())),
                ("payload", Value::Bytes(payload.to_vec().into())),
            ],
        );
        fields.push(("signed_principal_context", Value::Message(wrapper)));
    }
    message(response_descriptor(), &fields)
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

#[test]
fn service_principal_request_omits_dp_auth_token_config_even_with_ssp_override() {
    let mut config = cfg(1, vec![]);
    config.zerobus_ssp_resource_path = Some("workspaces/1/custom".into());
    let msg = build_request_message(
        &config,
        &request_descriptor(),
        PrincipalContextRequestKind::ServicePrincipal,
    )
    .unwrap();
    assert!(!msg.has_field_by_name("signed_principal_context_config"));
}

#[test]
fn dp_auth_token_request_sets_legacy_type_metastore_and_default_ssp_path() {
    let config = cfg(42, vec![]);
    let msg = build_request_message(
        &config,
        &request_descriptor(),
        PrincipalContextRequestKind::dp_auth_token(&config),
    )
    .unwrap();
    let config = match &*msg
        .get_field_by_name("signed_principal_context_config")
        .unwrap()
    {
        Value::Message(m) => m.clone(),
        other => panic!("expected signed config message, got {other:?}"),
    };
    assert_eq!(
        *config
            .get_field_by_name("signed_principal_context_type")
            .unwrap(),
        Value::EnumNumber(1)
    );
    assert_eq!(
        *config
            .get_field_by_name("allowed_service_principal_resource_path")
            .unwrap(),
        Value::String("workspaces/42/zerobusDirectWriteApi".into())
    );
    assert_eq!(
        *config.get_field_by_name("metastore_id").unwrap(),
        Value::String("metastore-1".into())
    );
}

#[test]
fn dp_auth_token_request_honors_ssp_path_override() {
    let mut config = cfg(42, vec![]);
    config.zerobus_ssp_resource_path = Some("workspaces/42/custom".into());
    let msg = build_request_message(
        &config,
        &request_descriptor(),
        PrincipalContextRequestKind::dp_auth_token(&config),
    )
    .unwrap();
    let Value::Message(signed) = &*msg
        .get_field_by_name("signed_principal_context_config")
        .unwrap()
    else {
        panic!("expected signed config message");
    };
    assert_eq!(
        *signed
            .get_field_by_name("allowed_service_principal_resource_path")
            .unwrap(),
        Value::String("workspaces/42/custom".into())
    );
}

#[test]
fn message_field_bytes_returns_serialized_signed_wrapper() {
    let response = response_with(Some(b"signed-pc"), None);
    let bytes = message_field_bytes(&response, "signed_principal_context").unwrap();
    let wrapper = DynamicMessage::decode(
        get_message("databricks.s2s.metadata.ProtectedMetadataWrapper"),
        bytes.as_slice(),
    )
    .unwrap();
    assert_eq!(
        *wrapper.get_field_by_name("payload").unwrap(),
        Value::Bytes(b"signed-pc".to_vec().into())
    );
}

#[test]
fn message_field_bytes_rejects_missing_field() {
    let response = response_with(None, None);
    assert!(matches!(
        message_field_bytes(&response, "signed_principal_context"),
        Err(TokenManagerError::PrincipalContext { .. })
    ));
}

#[test]
fn principal_context_expiry_reads_token_expiration() {
    let response = response_with(None, Some(unix_now() + 3600));
    let remaining = principal_context_expiry(&response)
        .expect("expected Some")
        .saturating_duration_since(Instant::now());
    assert!(remaining.as_secs() > 3500 && remaining.as_secs() <= 3600);
}

#[test]
fn principal_context_expiry_none_when_unset_or_past() {
    assert!(principal_context_expiry(&response_with(None, None)).is_none());
    assert!(principal_context_expiry(&response_with(None, Some(1))).is_none());
    assert!(principal_context_expiry(&message(response_descriptor(), &[])).is_none());
}

/// A `PrincipalContextResponse` with an unsigned context and, if `v2_payload` is set, a
/// dual-sent signed PCv2 wrapper whose inner signed payload is `v2_payload`.
fn response_with_v2(v2_payload: Option<&[u8]>) -> DynamicMessage {
    let mut response = response_with(None, None);
    if let Some(payload) = v2_payload {
        let signed = message(
            get_message("databricks.s2s.metadata.ProtectedMetadataWrapper"),
            &[("payload", Value::Bytes(payload.to_vec().into()))],
        );
        let wrapper = message(
            get_message("databricks.s2s.metadata.PrincipalContextWrapper"),
            &[("signed_principal_context", Value::Message(signed))],
        );
        response.set_field_by_name("signed_principal_context_v2", Value::Message(wrapper));
    }
    response
}

#[test]
fn optional_message_field_bytes_returns_dual_sent_v2_wrapper() {
    let response = response_with_v2(Some(b"signed-pc-v2"));
    let bytes = optional_message_field_bytes(&response, "signed_principal_context_v2")
        .expect("v2 wrapper present");
    let wrapper = DynamicMessage::decode(
        get_message("databricks.s2s.metadata.PrincipalContextWrapper"),
        bytes.as_slice(),
    )
    .unwrap();
    let Value::Message(signed) = &*wrapper
        .get_field_by_name("signed_principal_context")
        .unwrap()
    else {
        panic!("expected signed_principal_context message");
    };
    assert_eq!(
        *signed.get_field_by_name("payload").unwrap(),
        Value::Bytes(b"signed-pc-v2".to_vec().into())
    );
}

#[test]
fn optional_message_field_bytes_none_when_v2_not_sent() {
    let response = response_with_v2(None);
    assert!(optional_message_field_bytes(&response, "signed_principal_context_v2").is_none());
}

#[test]
fn principal_context_v2_wire_bytes_prefixes_uncompressed_envelope() {
    let wrapper = vec![0xAB; 200];
    let wire = principal_context_v2_wire_bytes(&wrapper);
    let envelope = wire
        .strip_prefix(b"pctx_v2_".as_slice())
        .expect("pctx_v2_ prefix");
    // CompressedPayload { algorithm: NONE (1), data: wrapper }; 200 encodes as varint [0xC8, 0x01].
    assert_eq!(&envelope[..5], &[0x08, 0x01, 0x12, 0xC8, 0x01]);
    assert_eq!(&envelope[5..], wrapper.as_slice());
}

// ---- gRPC status and retryability ----

fn grpc_headers(status: &str, message: Option<&str>) -> http::HeaderMap {
    let mut headers = http::HeaderMap::new();
    headers.insert("grpc-status", status.parse().unwrap());
    if let Some(message) = message {
        headers.insert("grpc-message", message.parse().unwrap());
    }
    headers
}

#[test]
fn grpc_status_error_ignores_ok_and_missing_status() {
    assert!(grpc_status_error(&http::HeaderMap::new()).is_none());
    assert!(grpc_status_error(&grpc_headers("0", None)).is_none());
}

#[test]
fn grpc_status_error_reports_status_and_message() {
    let error = grpc_status_error(&grpc_headers("7", Some("no USE CATALOG"))).unwrap();
    assert!(matches!(
        error,
        TokenManagerError::GrpcStatus { status: 7, ref message } if message == "no USE CATALOG"
    ));
}

#[test]
fn grpc_status_retryability_matches_transient_codes() {
    let retryable = |status| {
        TokenManagerError::GrpcStatus {
            status,
            message: String::new(),
        }
        .is_retryable()
    };
    // UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, INTERNAL retry.
    for status in [14, 4, 8, 13] {
        assert!(retryable(status), "status {status} should retry");
    }
    // INVALID_ARGUMENT, NOT_FOUND, PERMISSION_DENIED, UNAUTHENTICATED fail fast.
    for status in [3, 5, 7, 16] {
        assert!(!retryable(status), "status {status} should not retry");
    }
}

#[test]
fn malformed_principal_context_error_is_not_retryable() {
    assert!(
        !TokenManagerError::PrincipalContext {
            message: String::new()
        }
        .is_retryable()
    );
    assert!(TokenManagerError::ResponseTooShort.is_retryable());
}

#[tokio::test]
#[allow(deprecated)] // Hyper 0.14 supplies the Tokio executor for this test server.
async fn login_and_uc_transport_uses_shared_tls_and_negotiates_h2() {
    use std::pin::Pin;

    use hyper::{Body, Response, service::service_fn};
    use openssl::ssl::Ssl;
    use tokio::net::TcpListener;
    use tokio_openssl::SslStream;

    use super::transport::build_hyper_openssl_client;
    use crate::tls::{TlsConfig, TlsSettings};

    for (verify_hostname, server_name, endpoint_host, should_succeed) in [
        (true, "localhost", "localhost", true),
        (true, "different.example", "localhost", true),
        (true, "localhost", "127.0.0.1", false),
        (false, "different.example", "127.0.0.1", true),
    ] {
        let mut server_tls = TlsConfig::test_config();
        server_tls.ca_file =
            Some("tests/data/ca/intermediate_client/certs/ca-chain.cert.pem".into());
        server_tls.alpn_protocols = Some(vec!["h2".to_string()]);
        let settings = TlsSettings::from_options(Some(&server_tls)).unwrap();
        let acceptor = settings.acceptor().unwrap().into_context();
        // Use the configured acceptor's ALPN list; a client without ALPN would leave this unset.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = SslStream::new(Ssl::new(&acceptor).unwrap(), stream).unwrap();
            if let Err(error) = Pin::new(&mut stream).accept().await {
                assert!(!should_succeed, "unexpected TLS failure: {error}");
                return;
            }
            assert_eq!(
                stream.ssl().servername(openssl::ssl::NameType::HOST_NAME),
                Some(server_name)
            );
            assert_eq!(
                stream.ssl().selected_alpn_protocol(),
                Some(b"h2".as_slice())
            );
            let service = service_fn(|_| async {
                Ok::<_, std::convert::Infallible>(Response::new(Body::empty()))
            });
            hyper::server::conn::Http::new()
                .http2_only(true)
                .serve_connection(stream, service)
                .await
                .unwrap();
        });
        let mut auth = cfg(1, vec![]);
        auth.tls.crt_file = format!(
            "{}/tests/data/ca/intermediate_client/certs/localhost-chain.cert.pem",
            env!("CARGO_MANIFEST_DIR")
        );
        auth.tls.key_file = format!(
            "{}/tests/data/ca/intermediate_client/private/localhost.key.pem",
            env!("CARGO_MANIFEST_DIR")
        );
        auth.tls.ca_file = Some(format!(
            "{}/tests/data/ca/intermediate_server/certs/ca-chain.cert.pem",
            env!("CARGO_MANIFEST_DIR")
        ));
        auth.tls.verify_hostname = verify_hostname;
        let client = build_hyper_openssl_client(&auth.tls, server_name).unwrap();
        let response = tokio::time::timeout(
            Duration::from_secs(5),
            client.get(
                format!("https://{endpoint_host}:{}/", address.port())
                    .parse()
                    .unwrap(),
            ),
        )
        .await
        .unwrap();
        if should_succeed {
            assert!(response.as_ref().unwrap().status().is_success());
        } else {
            assert!(response.is_err(), "endpoint hostname must be verified");
        }
        drop(response);
        drop(client);
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
    }
}
