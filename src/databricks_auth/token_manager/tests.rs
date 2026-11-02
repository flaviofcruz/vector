use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use prost_reflect::prost_types::{
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FileDescriptorProto, FileDescriptorSet, MethodDescriptorProto, OneofDescriptorProto,
    ServiceDescriptorProto, field_descriptor_proto,
};
use prost_reflect::{
    DescriptorPool, DynamicMessage, MessageDescriptor, ReflectMessage, Value, prost::Message,
};

use super::cache::CachedPrincipalContext;
use super::config::{
    LoginServiceAuthConfig, LoginServiceConfig, MIN_TOKEN_VALIDITY, TOKEN_REFRESH_THRESHOLD,
    UcPermissionEntry, UserAuthInfo,
};
use super::error::TokenManagerError;
use super::request::build_request_message;
use super::response::{message_field_bytes, principal_context_expiry};
use super::transport::{encode_grpc_message, grpc_status_error, parse_grpc_response_message};
use super::uc::uc_table_schema_from_response;

// ---- Fixture descriptor pool ----
//
// Builds an in-memory `FileDescriptorSet` matching the relevant subset of the
// principal-context protos so we can exercise `build_request_message` without
// mounting a real `principal_context_service.pb`. Mirrors the test pool pattern in
// `src/sinks/bricklens_ingest/service.rs`.

const TEST_REQUEST_TYPE: &str = ".test.PrincipalContextRequest";
const TEST_RESPONSE_TYPE: &str = ".test.PrincipalContextResponse";

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

    // -- signed_principal_context_config.proto (subset) --
    let signed_type_enum = EnumDescriptorProto {
        name: Some("SignedPrincipalContextType".into()),
        value: vec![
            EnumValueDescriptorProto {
                name: Some("SIGNED_PRINCIPAL_CONTEXT_TYPE_UNSPECIFIED".into()),
                number: Some(0),
                ..Default::default()
            },
            EnumValueDescriptorProto {
                name: Some("PCTX_DP_AUTH_TOKEN".into()),
                number: Some(1),
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let signed_config = DescriptorProto {
        name: Some("SignedPrincipalContextConfig".into()),
        field: vec![
            FieldDescriptorProto {
                name: Some("signed_principal_context_type".into()),
                number: Some(1),
                label: Some(field_descriptor_proto::Label::Optional as i32),
                r#type: Some(field_descriptor_proto::Type::Enum as i32),
                type_name: Some(".test.SignedPrincipalContextType".into()),
                json_name: Some("signedPrincipalContextType".into()),
                ..Default::default()
            },
            FieldDescriptorProto {
                name: Some("allowed_service_principal_resource_path".into()),
                number: Some(3),
                label: Some(field_descriptor_proto::Label::Optional as i32),
                r#type: Some(field_descriptor_proto::Type::String as i32),
                json_name: Some("allowedServicePrincipalResourcePath".into()),
                ..Default::default()
            },
            FieldDescriptorProto {
                name: Some("metastore_id".into()),
                number: Some(4),
                label: Some(field_descriptor_proto::Label::Optional as i32),
                r#type: Some(field_descriptor_proto::Type::String as i32),
                json_name: Some("metastoreId".into()),
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    // -- principal_context_request.proto (subset) --
    let request = DescriptorProto {
        name: Some("PrincipalContextRequest".into()),
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
            FieldDescriptorProto {
                name: Some("signed_principal_context_config".into()),
                number: Some(3),
                label: Some(field_descriptor_proto::Label::Optional as i32),
                r#type: Some(field_descriptor_proto::Type::Message as i32),
                type_name: Some(".test.SignedPrincipalContextConfig".into()),
                json_name: Some("signedPrincipalContextConfig".into()),
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    // -- principal_context_response.proto and its referenced messages (subset) --
    let timestamp = DescriptorProto {
        name: Some("Timestamp".into()),
        field: vec![
            pc_field("seconds", 1, field_descriptor_proto::Type::Int64, None),
            pc_field("nanos", 2, field_descriptor_proto::Type::Int32, None),
        ],
        ..Default::default()
    };
    let authentication_details = DescriptorProto {
        name: Some("AuthenticationDetails".into()),
        field: vec![pc_field(
            "token_expiration",
            6,
            field_descriptor_proto::Type::Message,
            Some(".test.Timestamp"),
        )],
        ..Default::default()
    };
    let principal_context = DescriptorProto {
        name: Some("PrincipalContext".into()),
        field: vec![pc_field(
            "authentication_details",
            11,
            field_descriptor_proto::Type::Message,
            Some(".test.AuthenticationDetails"),
        )],
        ..Default::default()
    };
    let protected_metadata_wrapper = DescriptorProto {
        name: Some("ProtectedMetadataWrapper".into()),
        field: vec![
            pc_field("header", 1, field_descriptor_proto::Type::Bytes, None),
            pc_field("payload", 2, field_descriptor_proto::Type::Bytes, None),
        ],
        ..Default::default()
    };
    let response = DescriptorProto {
        name: Some("PrincipalContextResponse".into()),
        field: vec![
            pc_field(
                "principal_context",
                1,
                field_descriptor_proto::Type::Message,
                Some(".test.PrincipalContext"),
            ),
            pc_field(
                "signed_principal_context",
                2,
                field_descriptor_proto::Type::Message,
                Some(".test.ProtectedMetadataWrapper"),
            ),
        ],
        ..Default::default()
    };

    let service = ServiceDescriptorProto {
        name: Some("PrincipalContextService".into()),
        method: vec![MethodDescriptorProto {
            name: Some("CreatePrincipalContext".into()),
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
            signed_config,
            request,
            timestamp,
            authentication_details,
            principal_context,
            protected_metadata_wrapper,
            response,
        ],
        enum_type: vec![signed_type_enum],
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
            verify_hostname: true,
            proto_descriptor_path: PathBuf::from("/dev/null"),
        },
        metastore_id: "metastore-1".into(),
        zerobus_ssp_resource_path: None,
        unity_catalog: None,
    }
}

fn request_descriptor() -> MessageDescriptor {
    let pool = make_test_pool();
    pool.get_message_by_name("test.PrincipalContextRequest")
        .expect("request message in test pool")
}

fn response_descriptor() -> MessageDescriptor {
    let pool = make_test_pool();
    pool.get_message_by_name("test.PrincipalContextResponse")
        .expect("response message in test pool")
}

// ---- request-shape tests against the dynamic builder ----

#[test]
fn build_request_sets_workspace_scope_oneof() {
    let request_desc = request_descriptor();
    let msg =
        build_request_message(&cfg(848_677_326_595_393, vec![]), &request_desc, false).unwrap();

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
    let msg = build_request_message(&cfg(1, vec![]), &request_desc, false).unwrap();

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
    let msg = build_request_message(&cfg(1, perms), &request_desc, false).unwrap();

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
    let msg = build_request_message(&cfg(1, vec![]), &request_desc, false).unwrap();

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
        expires_at: now + MIN_TOKEN_VALIDITY + Duration::from_secs(1),
    };
    assert!(cached.has_remaining_lifetime(now, MIN_TOKEN_VALIDITY));
}

// ---- CreatePrincipalContext request/response ----

fn pc_field(
    name: &str,
    number: i32,
    ty: field_descriptor_proto::Type,
    type_name: Option<&str>,
) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        label: Some(field_descriptor_proto::Label::Optional as i32),
        r#type: Some(ty as i32),
        type_name: type_name.map(Into::into),
        ..Default::default()
    }
}

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
    let pool = make_test_pool();
    let get = |name: &str| pool.get_message_by_name(name).unwrap();
    let mut details = Vec::new();
    if let Some(exp) = exp {
        let ts = message(get("test.Timestamp"), &[("seconds", Value::I64(exp))]);
        details.push(("token_expiration", Value::Message(ts)));
    }
    let details = message(get("test.AuthenticationDetails"), &details);
    let pc = message(
        get("test.PrincipalContext"),
        &[("authentication_details", Value::Message(details))],
    );
    let mut fields = vec![("principal_context", Value::Message(pc))];
    if let Some(payload) = signed_payload {
        let wrapper = message(
            get("test.ProtectedMetadataWrapper"),
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
fn unsigned_request_omits_signed_config() {
    let msg = build_request_message(&cfg(1, vec![]), &request_descriptor(), false).unwrap();
    assert!(!msg.has_field_by_name("signed_principal_context_config"));
}

#[test]
fn signed_request_sets_signed_config_with_default_ssp_path() {
    let msg = build_request_message(&cfg(42, vec![]), &request_descriptor(), true).unwrap();
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
fn signed_request_honors_ssp_path_override() {
    let mut config = cfg(42, vec![]);
    config.zerobus_ssp_resource_path = Some("workspaces/42/custom".into());
    let msg = build_request_message(&config, &request_descriptor(), true).unwrap();
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
    let pool = make_test_pool();
    let wrapper = DynamicMessage::decode(
        pool.get_message_by_name("test.ProtectedMetadataWrapper")
            .unwrap(),
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

// ---- Unity Catalog getTable response decoding ----

fn uc_field(name: &str, number: i32, ty: field_descriptor_proto::Type) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        label: Some(field_descriptor_proto::Label::Optional as i32),
        r#type: Some(ty as i32),
        ..Default::default()
    }
}

fn uc_message_field(name: &str, number: i32, type_name: &str) -> FieldDescriptorProto {
    FieldDescriptorProto {
        type_name: Some(type_name.into()),
        ..uc_field(name, number, field_descriptor_proto::Type::Message)
    }
}

/// Subset of `managed-catalog/api/messages/{table,column}.proto` (proto2) with the real
/// field numbers, including `nullable`'s `[default = true]`.
fn uc_get_table_response_descriptor() -> MessageDescriptor {
    use field_descriptor_proto::Type;
    let column_info = DescriptorProto {
        name: Some("ColumnInfo".into()),
        field: vec![
            uc_field("name", 1, Type::String),
            uc_field("type_text", 2, Type::String),
            uc_field("type_json", 3, Type::String),
            FieldDescriptorProto {
                type_name: Some(".managedcatalog.ColumnTypeName".into()),
                ..uc_field("type_name", 4, Type::Enum)
            },
            uc_field("position", 8, Type::Int32),
            FieldDescriptorProto {
                default_value: Some("true".into()),
                ..uc_field("nullable", 10, Type::Bool)
            },
        ],
        ..Default::default()
    };
    let column_infos = DescriptorProto {
        name: Some("ColumnInfos".into()),
        field: vec![FieldDescriptorProto {
            label: Some(field_descriptor_proto::Label::Repeated as i32),
            ..uc_message_field("columns", 1, ".managedcatalog.ColumnInfo")
        }],
        ..Default::default()
    };
    let table_info = DescriptorProto {
        name: Some("TableInfo".into()),
        field: vec![
            uc_field("name", 1, Type::String),
            uc_field("catalog_name", 2, Type::String),
            uc_field("schema_name", 3, Type::String),
            uc_message_field("column_infos", 7, ".managedcatalog.ColumnInfos"),
        ],
        ..Default::default()
    };
    let get_table = DescriptorProto {
        name: Some("GetTable".into()),
        nested_type: vec![DescriptorProto {
            name: Some("Response".into()),
            field: vec![uc_message_field(
                "table_info",
                1,
                ".managedcatalog.TableInfo",
            )],
            ..Default::default()
        }],
        ..Default::default()
    };
    let column_type_name = EnumDescriptorProto {
        name: Some("ColumnTypeName".into()),
        value: vec![
            EnumValueDescriptorProto {
                name: Some("UNKNOWN_COLUMN_TYPE_NAME".into()),
                number: Some(0),
                ..Default::default()
            },
            EnumValueDescriptorProto {
                name: Some("LONG".into()),
                number: Some(5),
                ..Default::default()
            },
            EnumValueDescriptorProto {
                name: Some("STRING".into()),
                number: Some(10),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let file = FileDescriptorProto {
        name: Some("uc_test.proto".into()),
        package: Some("managedcatalog".into()),
        syntax: Some("proto2".into()),
        message_type: vec![column_info, column_infos, table_info, get_table],
        enum_type: vec![column_type_name],
        ..Default::default()
    };
    DescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: vec![file] })
        .expect("uc test descriptor pool")
        .get_message_by_name("managedcatalog.GetTable.Response")
        .expect("GetTable.Response")
}

fn uc_column(
    desc: &MessageDescriptor,
    name: &str,
    type_name: i32,
    nullable: Option<bool>,
) -> Value {
    let mut column = DynamicMessage::new(desc.clone());
    column.set_field_by_name("name", Value::String(name.into()));
    column.set_field_by_name("type_text", Value::String(name.to_lowercase()));
    column.set_field_by_name("type_name", Value::EnumNumber(type_name));
    if let Some(nullable) = nullable {
        column.set_field_by_name("nullable", Value::Bool(nullable));
    }
    Value::Message(column)
}

#[test]
fn uc_table_schema_from_response_maps_columns() {
    let response_desc = uc_get_table_response_descriptor();
    let pool = response_desc.parent_pool();
    let column_desc = pool
        .get_message_by_name("managedcatalog.ColumnInfo")
        .unwrap();

    let mut column_infos = DynamicMessage::new(
        pool.get_message_by_name("managedcatalog.ColumnInfos")
            .unwrap(),
    );
    column_infos.set_field_by_name(
        "columns",
        Value::List(vec![
            uc_column(&column_desc, "ID", 5, Some(false)),
            uc_column(&column_desc, "MSG", 10, None),
        ]),
    );
    let mut table_info = DynamicMessage::new(
        pool.get_message_by_name("managedcatalog.TableInfo")
            .unwrap(),
    );
    table_info.set_field_by_name("name", Value::String("t".into()));
    table_info.set_field_by_name("catalog_name", Value::String("c".into()));
    table_info.set_field_by_name("schema_name", Value::String("s".into()));
    table_info.set_field_by_name("column_infos", Value::Message(column_infos));
    let mut response = DynamicMessage::new(response_desc.clone());
    response.set_field_by_name("table_info", Value::Message(table_info));

    // Round-trip through the wire so this exercises what grpc_unary_call hands back.
    let decoded = parse_grpc_response_message(
        bytes::Bytes::from(encode_grpc_message(response.encode_to_vec())),
        &response_desc,
    )
    .unwrap();
    let schema = uc_table_schema_from_response(&decoded).unwrap();

    assert_eq!(
        (
            schema.name.as_str(),
            schema.catalog_name.as_str(),
            schema.schema_name.as_str()
        ),
        ("t", "c", "s")
    );
    let columns: Vec<_> = schema
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c.type_name.as_str(), c.nullable))
        .collect();
    // An unset proto2 `nullable` reads back as its declared default (true).
    assert_eq!(
        columns,
        vec![("ID", "LONG", false), ("MSG", "STRING", true)]
    );
}

#[test]
fn uc_table_schema_from_response_rejects_missing_table_info() {
    let response = DynamicMessage::new(uc_get_table_response_descriptor());
    assert!(matches!(
        uc_table_schema_from_response(&response),
        Err(TokenManagerError::UcResponse { .. })
    ));
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
fn malformed_token_and_response_errors_are_not_retryable() {
    assert!(
        !TokenManagerError::PrincipalContext {
            message: String::new()
        }
        .is_retryable()
    );
    assert!(
        !TokenManagerError::UcResponse {
            message: String::new()
        }
        .is_retryable()
    );
    assert!(!TokenManagerError::UnityCatalogNotConfigured.is_retryable());
    assert!(TokenManagerError::ResponseTooShort.is_retryable());
}
