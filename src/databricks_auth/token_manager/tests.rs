use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine;
use prost_reflect::prost_types::{
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FileDescriptorProto, FileDescriptorSet, MethodDescriptorProto, OneofDescriptorProto,
    ServiceDescriptorProto, field_descriptor_proto,
};
use prost_reflect::{
    DescriptorPool, DynamicMessage, MessageDescriptor, ReflectMessage, Value, prost::Message,
};

use super::cache::CachedToken;
use super::config::{
    LoginServiceAuthConfig, LoginServiceConfig, MIN_TOKEN_VALIDITY, TOKEN_REFRESH_THRESHOLD,
    UcPermissionEntry, UserAuthInfo,
};
use super::error::TokenManagerError;
use super::jwt::extract_jwt_expiry;
use super::request::build_request_message;
use super::transport::{encode_grpc_message, parse_grpc_response_message};

// ---- JWT expiry tests ----

fn jwt_with_exp(exp: u64) -> String {
    let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(b"{\"typ\":\"JWT\",\"alg\":\"RS256\"}");
    let payload =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(format!("{{\"exp\":{exp}}}"));
    format!("{header}.{payload}.sig")
}

#[test]
fn extract_jwt_expiry_valid() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let jwt = jwt_with_exp(now + 3600);
    let expiry = extract_jwt_expiry(&jwt).expect("expected Some");
    let remaining = expiry.saturating_duration_since(Instant::now());
    assert!(remaining.as_secs() > 3500 && remaining.as_secs() <= 3600);
}

#[test]
fn extract_jwt_expiry_already_expired() {
    let jwt = jwt_with_exp(1);
    assert!(extract_jwt_expiry(&jwt).is_none());
}

#[test]
fn extract_jwt_expiry_malformed_not_three_parts() {
    assert!(extract_jwt_expiry("not.a.valid.jwt").is_none());
    assert!(extract_jwt_expiry("only-one-part").is_none());
}

#[test]
fn extract_jwt_expiry_payload_not_json() {
    let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(b"{\"typ\":\"JWT\",\"alg\":\"RS256\"}");
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"not-json");
    let jwt = format!("{header}.{payload}.sig");
    assert!(extract_jwt_expiry(&jwt).is_none());
}

#[test]
fn extract_jwt_expiry_payload_missing_exp() {
    let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(b"{\"typ\":\"JWT\",\"alg\":\"RS256\"}");
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"{\"sub\":\"x\"}");
    let jwt = format!("{header}.{payload}.sig");
    assert!(extract_jwt_expiry(&jwt).is_none());
}

// ---- Fixture descriptor pool ----
//
// Builds an in-memory `FileDescriptorSet` matching the relevant subset of the
// OAuth + identity protos so we can exercise `build_request_message` without
// mounting a real `oauth_service.pb`. Mirrors the test pool pattern in
// `src/sinks/bricklens_ingest/service.rs`.

const TEST_REQUEST_TYPE: &str =
    ".test.GenerateReauthenticatedAndReauthorizedInternalOAuthTokenRequest";
const TEST_RESPONSE_TYPE: &str =
    ".test.GenerateReauthenticatedAndReauthorizedInternalOAuthTokenResponse";

fn make_test_pool() -> DescriptorPool {
    // -- identity.proto (subset) --
    let authenticate_as_service = DescriptorProto {
        name: Some("AuthenticateAsService".into()),
        field: vec![
            FieldDescriptorProto {
                name: Some("service_principal_type".into()),
                number: Some(1),
                label: Some(field_descriptor_proto::Label::Optional as i32),
                r#type: Some(field_descriptor_proto::Type::Int32 as i32),
                json_name: Some("servicePrincipalType".into()),
                ..Default::default()
            },
            FieldDescriptorProto {
                name: Some("service_principal_resource".into()),
                number: Some(2),
                label: Some(field_descriptor_proto::Label::Optional as i32),
                r#type: Some(field_descriptor_proto::Type::String as i32),
                json_name: Some("servicePrincipalResource".into()),
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let authentication = DescriptorProto {
        name: Some("Authentication".into()),
        field: vec![
            FieldDescriptorProto {
                name: Some("workspace".into()),
                number: Some(1),
                label: Some(field_descriptor_proto::Label::Optional as i32),
                r#type: Some(field_descriptor_proto::Type::Int64 as i32),
                oneof_index: Some(0),
                json_name: Some("workspace".into()),
                ..Default::default()
            },
            FieldDescriptorProto {
                name: Some("service".into()),
                number: Some(2),
                label: Some(field_descriptor_proto::Label::Optional as i32),
                r#type: Some(field_descriptor_proto::Type::Message as i32),
                type_name: Some(".test.AuthenticateAsService".into()),
                oneof_index: Some(1),
                json_name: Some("service".into()),
                ..Default::default()
            },
        ],
        oneof_decl: vec![
            OneofDescriptorProto {
                name: Some("scope".into()),
                ..Default::default()
            },
            OneofDescriptorProto {
                name: Some("method".into()),
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let uc_securable_reference = DescriptorProto {
        name: Some("UcSecurableReference".into()),
        field: vec![
            FieldDescriptorProto {
                name: Some("type".into()),
                number: Some(1),
                label: Some(field_descriptor_proto::Label::Optional as i32),
                r#type: Some(field_descriptor_proto::Type::String as i32),
                json_name: Some("type".into()),
                ..Default::default()
            },
            FieldDescriptorProto {
                name: Some("full_name".into()),
                number: Some(2),
                label: Some(field_descriptor_proto::Label::Optional as i32),
                r#type: Some(field_descriptor_proto::Type::String as i32),
                json_name: Some("fullName".into()),
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let uc_securable_permission = DescriptorProto {
        name: Some("UcSecurablePermission".into()),
        field: vec![
            FieldDescriptorProto {
                name: Some("privileges".into()),
                number: Some(1),
                label: Some(field_descriptor_proto::Label::Repeated as i32),
                r#type: Some(field_descriptor_proto::Type::String as i32),
                json_name: Some("privileges".into()),
                ..Default::default()
            },
            FieldDescriptorProto {
                name: Some("securable_reference".into()),
                number: Some(2),
                label: Some(field_descriptor_proto::Label::Optional as i32),
                r#type: Some(field_descriptor_proto::Type::Message as i32),
                type_name: Some(".test.UcSecurableReference".into()),
                json_name: Some("securableReference".into()),
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let uc_permission_constraint = DescriptorProto {
        name: Some("UcPermissionConstraint".into()),
        field: vec![FieldDescriptorProto {
            name: Some("uc_securable_permission".into()),
            number: Some(1),
            label: Some(field_descriptor_proto::Label::Repeated as i32),
            r#type: Some(field_descriptor_proto::Type::Message as i32),
            type_name: Some(".test.UcSecurablePermission".into()),
            json_name: Some("ucSecurablePermission".into()),
            ..Default::default()
        }],
        ..Default::default()
    };

    let authorization_constraint = DescriptorProto {
        name: Some("AuthorizationConstraint".into()),
        field: vec![FieldDescriptorProto {
            name: Some("uc_permission_constraint".into()),
            number: Some(1),
            label: Some(field_descriptor_proto::Label::Optional as i32),
            r#type: Some(field_descriptor_proto::Type::Message as i32),
            type_name: Some(".test.UcPermissionConstraint".into()),
            json_name: Some("ucPermissionConstraint".into()),
            ..Default::default()
        }],
        ..Default::default()
    };

    let authorization = DescriptorProto {
        name: Some("Authorization".into()),
        field: vec![FieldDescriptorProto {
            name: Some("authorization_constraint".into()),
            number: Some(1),
            label: Some(field_descriptor_proto::Label::Optional as i32),
            r#type: Some(field_descriptor_proto::Type::Message as i32),
            type_name: Some(".test.AuthorizationConstraint".into()),
            json_name: Some("authorizationConstraint".into()),
            ..Default::default()
        }],
        ..Default::default()
    };

    // -- oauth_service.proto (subset) --
    let request = DescriptorProto {
        name: Some("GenerateReauthenticatedAndReauthorizedInternalOAuthTokenRequest".into()),
        field: vec![
            FieldDescriptorProto {
                name: Some("authentication".into()),
                number: Some(1),
                label: Some(field_descriptor_proto::Label::Optional as i32),
                r#type: Some(field_descriptor_proto::Type::Message as i32),
                type_name: Some(".test.Authentication".into()),
                json_name: Some("authentication".into()),
                ..Default::default()
            },
            FieldDescriptorProto {
                name: Some("authorization".into()),
                number: Some(2),
                label: Some(field_descriptor_proto::Label::Optional as i32),
                r#type: Some(field_descriptor_proto::Type::Message as i32),
                type_name: Some(".test.Authorization".into()),
                json_name: Some("authorization".into()),
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let state_enum = EnumDescriptorProto {
        name: Some("State".into()),
        value: vec![
            EnumValueDescriptorProto {
                name: Some("STATE_UNSPECIFIED".into()),
                number: Some(0),
                ..Default::default()
            },
            EnumValueDescriptorProto {
                name: Some("SUCCESS".into()),
                number: Some(1),
                ..Default::default()
            },
            EnumValueDescriptorProto {
                name: Some("FAILURE".into()),
                number: Some(2),
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let response = DescriptorProto {
        name: Some("GenerateReauthenticatedAndReauthorizedInternalOAuthTokenResponse".into()),
        field: vec![
            FieldDescriptorProto {
                name: Some("state".into()),
                number: Some(1),
                label: Some(field_descriptor_proto::Label::Optional as i32),
                r#type: Some(field_descriptor_proto::Type::Enum as i32),
                type_name: Some(
                    ".test.GenerateReauthenticatedAndReauthorizedInternalOAuthTokenResponse.State"
                        .into(),
                ),
                json_name: Some("state".into()),
                ..Default::default()
            },
            FieldDescriptorProto {
                name: Some("error_message".into()),
                number: Some(2),
                label: Some(field_descriptor_proto::Label::Optional as i32),
                r#type: Some(field_descriptor_proto::Type::String as i32),
                json_name: Some("errorMessage".into()),
                ..Default::default()
            },
            FieldDescriptorProto {
                name: Some("access_token".into()),
                number: Some(3),
                label: Some(field_descriptor_proto::Label::Optional as i32),
                r#type: Some(field_descriptor_proto::Type::String as i32),
                json_name: Some("accessToken".into()),
                ..Default::default()
            },
        ],
        enum_type: vec![state_enum],
        ..Default::default()
    };

    let service = ServiceDescriptorProto {
        name: Some("InternalOAuthTokenService".into()),
        method: vec![MethodDescriptorProto {
            name: Some("GenerateReauthenticatedAndReauthorizedInternalOAuthToken".into()),
            input_type: Some(TEST_REQUEST_TYPE.into()),
            output_type: Some(TEST_RESPONSE_TYPE.into()),
            ..Default::default()
        }],
        ..Default::default()
    };

    let file = FileDescriptorProto {
        name: Some("test.proto".into()),
        package: Some("test".into()),
        syntax: Some("proto3".into()),
        message_type: vec![
            authenticate_as_service,
            authentication,
            uc_securable_reference,
            uc_securable_permission,
            uc_permission_constraint,
            authorization_constraint,
            authorization,
            request,
            response,
        ],
        service: vec![service],
        ..Default::default()
    };

    DescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: vec![file] })
        .expect("test descriptor pool")
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
            tls_crt_file: "/dev/null".into(),
            tls_key_file: "/dev/null".into(),
            tls_ca_file: None,
            oauth_proto_descriptor_path: PathBuf::from("/dev/null"),
        },
    }
}

fn request_descriptor() -> MessageDescriptor {
    let pool = make_test_pool();
    pool.get_message_by_name("test.GenerateReauthenticatedAndReauthorizedInternalOAuthTokenRequest")
        .expect("request message in test pool")
}

fn response_descriptor() -> MessageDescriptor {
    let pool = make_test_pool();
    pool.get_message_by_name(
        "test.GenerateReauthenticatedAndReauthorizedInternalOAuthTokenResponse",
    )
    .expect("response message in test pool")
}

// ---- request-shape tests against the dynamic builder ----

#[test]
fn build_request_sets_workspace_scope_oneof() {
    let request_desc = request_descriptor();
    let msg = build_request_message(&cfg(848_677_326_595_393, vec![]), &request_desc).unwrap();

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
    let msg = build_request_message(&cfg(1, vec![]), &request_desc).unwrap();

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
        Value::I32(v) => assert_eq!(*v, 3),
        other => panic!("expected service_principal_type I32, got {:?}", other),
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
    let msg = build_request_message(&cfg(1, perms), &request_desc).unwrap();

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
    let msg = build_request_message(&cfg(1, vec![]), &request_desc).unwrap();

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

    // Build a SUCCESS response with an access_token.
    let mut response = DynamicMessage::new(response_desc.clone());
    let state_field = response_desc.get_field_by_name("state").unwrap();
    response.set_field(&state_field, Value::EnumNumber(1));
    let token_field = response_desc.get_field_by_name("access_token").unwrap();
    response.set_field(
        &token_field,
        Value::String("eyJhbGciOiJSUzI1NiJ9.payload.sig".into()),
    );

    let body_bytes = response.encode_to_vec();
    let framed = encode_grpc_message(body_bytes);

    let parsed = parse_grpc_response_message(bytes::Bytes::from(framed), &response_desc).unwrap();
    let token = parsed.get_field_by_name("access_token").unwrap();
    match &*token {
        Value::String(s) => assert_eq!(s, "eyJhbGciOiJSUzI1NiJ9.payload.sig"),
        _ => panic!(),
    }
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
    let cached = CachedToken {
        access_token: "tok".into(),
        expires_at: now + Duration::from_secs(3600),
    };
    assert!(cached.is_valid_at(now));
}

#[test]
fn cached_token_not_valid_when_already_expired() {
    let now = Instant::now();
    let cached = CachedToken {
        access_token: "tok".into(),
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
    let cached = CachedToken {
        access_token: "tok".into(),
        expires_at: now + TOKEN_REFRESH_THRESHOLD - Duration::from_secs(1),
    };
    assert!(!cached.is_valid_at(now));
}

#[test]
fn cached_token_valid_just_past_refresh_threshold() {
    let now = Instant::now();
    let cached = CachedToken {
        access_token: "tok".into(),
        expires_at: now + TOKEN_REFRESH_THRESHOLD + Duration::from_secs(1),
    };
    assert!(cached.is_valid_at(now));
}

#[test]
fn cached_token_below_min_validity_is_rejected() {
    let now = Instant::now();
    // A freshly minted token with less than MIN_TOKEN_VALIDITY remaining must not be served:
    // bootstrap() rejects it as TokenTooShortLived rather than risk a mid-write expiry.
    let cached = CachedToken {
        access_token: "tok".into(),
        expires_at: now + MIN_TOKEN_VALIDITY - Duration::from_secs(1),
    };
    assert!(!cached.has_remaining_lifetime(now, MIN_TOKEN_VALIDITY));
}

#[test]
fn cached_token_above_min_validity_is_accepted() {
    let now = Instant::now();
    let cached = CachedToken {
        access_token: "tok".into(),
        expires_at: now + MIN_TOKEN_VALIDITY + Duration::from_secs(1),
    };
    assert!(cached.has_remaining_lifetime(now, MIN_TOKEN_VALIDITY));
}
