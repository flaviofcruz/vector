//! The `lp_unwrap` VRL function: Lumberjack Prime's stage-1 unwrap, natively.
//!
//! Replaces the VRL sequence `decode_base64!` -> `parse_proto!(LogDaemonWrapper)` ->
//! `decode_zstd` -> `parse_proto(LogEntry)` -> five field assignments, which was the single
//! largest CPU consumer on the LP pods. The cost was not the protobuf decoding itself (~5 µs per
//! event) but everything around it: six interpreted VRL statements, a `decode_zstd` attempt that
//! allocates a decoder context on every uncompressed event (~3 µs), and the stash assignments that
//! clone each decoded object (~1.4 µs). Doing it once, natively, removes those.
//!
//! The protobuf decoding is deliberately the same code `parse_proto` uses (prost-reflect
//! `DynamicMessage` + `proto_to_value`), so the decoded objects are identical by construction on
//! every input, well-formed or not. A hand-written wire-format walker was measured at only ~1.3 µs
//! per event faster and is not worth owning (branch `nikhilesh.payyavuala/lp-unwrap-walker`).
//!
//! Writes, exactly as the VRL did:
//! - `._lp_inner_bytes`: `LogDaemonWrapper.message`, zstd-decoded when it carries a zstd frame.
//! - `._lp_log_entry`: the parsed `logging.LogEntry` (`{}` when the inner bytes are not one).
//! - `._lp_log_entry_parsed`: whether the LogEntry parse succeeded.
//! - `._lp_log_daemon_wrapper`: the parsed wrapper.
//! - `._workspace_id`: `LogEntry.workspace_id`, else `LogDaemonWrapper.workspace_id`.
use std::path::Path;

use bytes::Bytes;
use prost_reflect::{DynamicMessage, MessageDescriptor};
use vrl::path::{OwnedTargetPath, parse_target_path};
use vrl::prelude::*;
use vrl::protobuf::descriptor::get_message_descriptor;
use vrl::protobuf::parse::{Options as ProtoOptions, proto_to_value};
use vrl::value::kind::Collection;

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Whether `zstd::decode_all` can succeed on these bytes: a zstd frame, or a skippable frame
/// (magic 0x184D2A5?), which decodes to nothing. The old chain called `decode_zstd` on every
/// message and fell back on error; checking the magic first gives the same result without paying
/// for a decoder context on the (common) uncompressed path.
fn zstd_decodable(bytes: &[u8]) -> bool {
    bytes.len() >= 4
        && (bytes[..4] == ZSTD_MAGIC
            || (bytes[0] & 0xF0 == 0x50
                && bytes[1] == 0x2A
                && bytes[2] == 0x4D
                && bytes[3] == 0x18))
}

/// Protobuf wire types. Hand-rolled (with the varint/key/skip helpers below) because the
/// workspace `prost` and the one `prost_reflect` is built on are different major versions.
const WRAPPER_MESSAGE_FIELD: &str = "message";

#[derive(Clone, Copy, Debug)]
pub struct LpUnwrap;

impl Function for LpUnwrap {
    fn identifier(&self) -> &'static str {
        "lp_unwrap"
    }

    fn summary(&self) -> &'static str {
        "decode a base64 LogDaemonWrapper line onto the event for the Lumberjack Prime pipeline"
    }

    fn usage(&self) -> &'static str {
        indoc::indoc! {"
            Decodes `value` (a base64 `LogDaemonWrapper`) and writes `._lp_inner_bytes`,
            `._lp_log_entry`, `._lp_log_entry_parsed`, `._lp_log_daemon_wrapper` and
            `._workspace_id` onto the event, byte for byte what the Lumberjack Prime unwrap VRL
            produced with `decode_base64!` + `parse_proto!` + `decode_zstd`. Returns null.
            Fallible (call as `lp_unwrap!(...)`): errors when the base64 or the wrapper does not
            decode.
        "}
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[
            Parameter {
                keyword: "value",
                kind: kind::BYTES,
                required: true,
            },
            Parameter {
                keyword: "wrapper_descriptor_file",
                kind: kind::BYTES,
                required: true,
            },
            Parameter {
                keyword: "log_entry_descriptor_file",
                kind: kind::BYTES,
                required: true,
            },
        ]
    }

    fn examples(&self) -> &'static [Example] {
        &[]
    }

    fn compile(
        &self,
        state: &state::TypeState,
        _ctx: &mut FunctionCompileContext,
        arguments: ArgumentList,
    ) -> Compiled {
        let compile_error = |message: String| {
            Box::new(ExpressionError::Error {
                message: format!("lp_unwrap: {message}"),
                labels: vec![],
                notes: vec![],
            }) as Box<dyn DiagnosticMessage>
        };
        let descriptor = |name: &'static str, message_type: &str| {
            let path = arguments
                .required_literal(name, state)?
                .try_bytes_utf8_lossy()
                .map(|s| s.into_owned())
                .map_err(|_| compile_error(format!("{name} must be a string")))?;
            get_message_descriptor(Path::new(&path), message_type).map_err(compile_error)
        };
        let wrapper = descriptor("wrapper_descriptor_file", "LogDaemonWrapper")?;
        let log_entry = descriptor("log_entry_descriptor_file", "logging.LogEntry")?;
        let path = |p: &str| parse_target_path(p).expect("static path");
        Ok(Box::new(LpUnwrapFn {
            value: arguments.required("value"),
            wrapper,
            log_entry,
            inner_bytes_path: path("._lp_inner_bytes"),
            log_entry_path: path("._lp_log_entry"),
            log_entry_parsed_path: path("._lp_log_entry_parsed"),
            wrapper_path: path("._lp_log_daemon_wrapper"),
            workspace_id_path: path("._workspace_id"),
        }))
    }
}

#[derive(Clone, Debug)]
struct LpUnwrapFn {
    value: Box<dyn Expression>,
    wrapper: MessageDescriptor,
    log_entry: MessageDescriptor,
    inner_bytes_path: OwnedTargetPath,
    log_entry_path: OwnedTargetPath,
    log_entry_parsed_path: OwnedTargetPath,
    wrapper_path: OwnedTargetPath,
    workspace_id_path: OwnedTargetPath,
}

impl LpUnwrapFn {
    /// The paths this function writes and their kinds, for the VRL type checker: later statements
    /// and downstream transforms see them exactly as they saw the old chain's assignments.
    fn written_paths(&self) -> [(&OwnedTargetPath, Kind); 5] {
        [
            (&self.inner_bytes_path, Kind::bytes()),
            (&self.log_entry_path, Kind::object(Collection::any())),
            (&self.log_entry_parsed_path, Kind::boolean()),
            (&self.wrapper_path, Kind::object(Collection::any())),
            (&self.workspace_id_path, Kind::integer().or_null()),
        ]
    }
}

impl Expression for LpUnwrapFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let encoded = self.value.resolve(ctx)?.try_bytes()?;
        let decoded = decode_base64(&encoded)?;

        let wrapper = parse_message(&self.wrapper, decoded)
            .map_err(|e| format!("lp_unwrap: LogDaemonWrapper: {e}"))?;
        // A refcount bump: prost-reflect decodes from `Bytes`, so the field is a slice of the buffer.
        let message = match wrapper.get(WRAPPER_MESSAGE_FIELD) {
            Some(Value::Bytes(b)) => Some(b.clone()),
            _ => None,
        };
        // Opportunistic parse: raw target-proto bytes (file path) usually fail here; the proto
        // stage uses the flag, not the contents, to tell the two cases apart. A wrapper with no
        // `message` at all is what the VRL chain saw as `null`: nothing to decode, parse fails.
        let (inner, log_entry, parsed) = match message {
            Some(m) => {
                let inner = if zstd_decodable(&m) {
                    zstd::decode_all(&m[..]).map(Bytes::from).unwrap_or(m)
                } else {
                    m
                };
                match parse_message(&self.log_entry, inner.clone()) {
                    Ok(entry) => (Value::Bytes(inner), entry, true),
                    Err(_) => (Value::Bytes(inner), ObjectMap::new(), false),
                }
            }
            None => (Value::Null, ObjectMap::new(), false),
        };

        let workspace_id = match log_entry.get("workspace_id") {
            Some(v) if !v.is_null() => v.clone(),
            _ => wrapper.get("workspace_id").cloned().unwrap_or(Value::Null),
        };

        let target = ctx.target_mut();
        target.target_insert(&self.inner_bytes_path, inner)?;
        target.target_insert(&self.log_entry_path, Value::Object(log_entry))?;
        target.target_insert(&self.log_entry_parsed_path, Value::Boolean(parsed))?;
        target.target_insert(&self.wrapper_path, Value::Object(wrapper))?;
        target.target_insert(&self.workspace_id_path, workspace_id)?;
        Ok(Value::Null)
    }

    fn type_info(&self, state: &state::TypeState) -> TypeInfo {
        let mut state = state.clone();
        self.value.apply_type_info(&mut state);
        let mut kind = state.external.target_kind().clone();
        for (path, path_kind) in self.written_paths() {
            kind.insert(&path.path, path_kind);
        }
        state.external =
            state::ExternalEnv::new_with_kind(kind, state.external.metadata_kind().clone());
        TypeInfo::new(state, TypeDef::null().fallible().impure())
    }
}

impl std::fmt::Display for LpUnwrapFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("lp_unwrap")
    }
}

fn decode_base64(encoded: &[u8]) -> Result<Bytes, ExpressionError> {
    // Exactly VRL `decode_base64` with the default charset: drop trailing '=' and decode as
    // unpadded standard base64 with the same crate.
    let end = encoded
        .iter()
        .rev()
        .position(|c| *c != b'=')
        .map_or(encoded.len(), |p| encoded.len() - p);
    base64_simd::STANDARD_NO_PAD
        .decode_to_vec(&encoded[..end])
        .map(Bytes::from)
        .map_err(|_| "lp_unwrap: unable to decode value from base64".into())
}

/// Exactly VRL `parse_proto`: prost-reflect decodes (zero-copy for `bytes`/`string` fields when
/// given `Bytes`), `proto_to_value` builds the VRL object.
fn parse_message(descriptor: &MessageDescriptor, bytes: Bytes) -> Result<ObjectMap, String> {
    let message = DynamicMessage::decode(descriptor.clone(), bytes)
        .map_err(|error| format!("Error parsing protobuf: {error:?}"))?;
    match proto_to_value(
        &prost_reflect::Value::Message(message),
        None,
        &ProtoOptions::default(),
    )? {
        Value::Object(object) => Ok(object),
        other => Err(format!("protobuf message decoded to {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost_reflect::prost::Message;
    use prost_reflect::prost_types::field_descriptor_proto::{Label, Type};
    use prost_reflect::prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    };
    use prost_reflect::{DescriptorPool, DynamicMessage};
    use vrl::compiler::CompileConfig;

    fn at<'a>(value: &'a Value, key: &str) -> &'a Value {
        match value {
            Value::Object(map) => &map[key],
            other => panic!("{key}: not an object: {other:?}"),
        }
    }

    fn field(
        name: &str,
        number: i32,
        ty: Type,
        type_name: Option<&str>,
        repeated: bool,
    ) -> FieldDescriptorProto {
        FieldDescriptorProto {
            name: Some(name.to_owned()),
            number: Some(number),
            label: Some(if repeated {
                Label::Repeated
            } else {
                Label::Optional
            } as i32),
            r#type: Some(ty as i32),
            type_name: type_name.map(str::to_owned),
            ..Default::default()
        }
    }

    /// LogDaemonWrapper + logging.LogEntry as `lp_unwrap!` compiles them, written to a .desc file.
    fn lp_pool() -> (DescriptorPool, String) {
        let wrapper = FileDescriptorProto {
            name: Some("w.proto".into()),
            syntax: Some("proto3".into()),
            message_type: vec![DescriptorProto {
                name: Some("LogDaemonWrapper".into()),
                field: vec![
                    field("message", 1, Type::Bytes, None, false),
                    field("workspace_id", 2, Type::Int64, None, false),
                    field("host", 3, Type::String, None, false),
                ],
                ..Default::default()
            }],
            ..Default::default()
        };
        let entry = FileDescriptorProto {
            name: Some("e.proto".into()),
            package: Some("logging".into()),
            syntax: Some("proto3".into()),
            message_type: vec![DescriptorProto {
                name: Some("LogEntry".into()),
                field: vec![
                    field("workspace_id", 1, Type::Int64, None, false),
                    field("system_uri", 2, Type::String, None, false),
                ],
                ..Default::default()
            }],
            ..Default::default()
        };
        let set = FileDescriptorSet {
            file: vec![wrapper, entry],
        };
        // One file per call: tests run in parallel and a concurrent rewrite of a shared path
        // would expose a truncated descriptor set to another test's compile.
        static N: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "lp_unwrap_{}_{}.desc",
            std::process::id(),
            N.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::write(&path, set.encode_to_vec()).unwrap();
        (
            DescriptorPool::from_file_descriptor_set(set).unwrap(),
            path.to_string_lossy().into_owned(),
        )
    }

    fn wrapper_line(pool: &DescriptorPool, message: &[u8], workspace_id: i64) -> String {
        let desc = pool.get_message_by_name("LogDaemonWrapper").unwrap();
        let mut m = DynamicMessage::new(desc);
        m.set_field_by_name(
            "message",
            prost_reflect::Value::Bytes(Bytes::copy_from_slice(message)),
        );
        m.set_field_by_name("workspace_id", prost_reflect::Value::I64(workspace_id));
        m.set_field_by_name("host", prost_reflect::Value::String("h1".into()));
        base64_simd::STANDARD.encode_to_string(m.encode_to_vec())
    }

    fn log_entry(pool: &DescriptorPool, workspace_id: i64) -> Vec<u8> {
        let desc = pool.get_message_by_name("logging.LogEntry").unwrap();
        let mut m = DynamicMessage::new(desc);
        m.set_field_by_name("workspace_id", prost_reflect::Value::I64(workspace_id));
        m.set_field_by_name(
            "system_uri",
            prost_reflect::Value::String("system:a".into()),
        );
        m.encode_to_vec()
    }

    /// Compiles `lp_unwrap!(.v, desc, desc)` and resolves it against `{v: line}`; returns the event.
    fn unwrap_event(desc_path: &str, line: &str) -> ObjectMap {
        let type_state = state::TypeState::default();
        let mut cctx = FunctionCompileContext::new(Span::new(0, 0), CompileConfig::default());
        let args = func_args![
            value: Bytes::from(line.to_owned()),
            wrapper_descriptor_file: desc_path.to_owned(),
            log_entry_descriptor_file: desc_path.to_owned(),
        ];
        let expr = LpUnwrap
            .compile(&type_state, &mut cctx, args.into())
            .expect("compile");
        let mut runtime_state = state::RuntimeState::default();
        let mut object = Value::Object(ObjectMap::new());
        let tz = TimeZone::default();
        let mut ctx = Context::new(&mut object, &mut runtime_state, &tz);
        assert_eq!(expr.resolve(&mut ctx).expect("resolve"), Value::Null);
        match object {
            Value::Object(map) => map,
            other => panic!("event is not an object: {other:?}"),
        }
    }

    #[test]
    fn base64_accepts_padded_and_unpadded() {
        assert_eq!(decode_base64(b"aGk=").unwrap(), Bytes::from_static(b"hi"));
        assert_eq!(decode_base64(b"aGk").unwrap(), Bytes::from_static(b"hi"));
        assert!(decode_base64(b"!!").is_err());
    }

    #[test]
    fn resolve_writes_the_five_fields_for_a_zstd_log_entry() {
        let (pool, desc) = lp_pool();
        let entry = log_entry(&pool, 42);
        let compressed = zstd::encode_all(&entry[..], 3).unwrap();
        assert!(compressed.starts_with(&ZSTD_MAGIC));
        let event = unwrap_event(&desc, &wrapper_line(&pool, &compressed, 7));

        assert_eq!(event["_lp_inner_bytes"], Value::Bytes(Bytes::from(entry)));
        assert_eq!(event["_lp_log_entry_parsed"], Value::Boolean(true));
        assert_eq!(
            at(&event["_lp_log_entry"], "workspace_id"),
            &Value::Integer(42)
        );
        assert_eq!(
            at(&event["_lp_log_entry"], "system_uri"),
            &Value::from("system:a")
        );
        // LogEntry's workspace_id wins over the wrapper's
        assert_eq!(event["_workspace_id"], Value::Integer(42));
        let wrapper = &event["_lp_log_daemon_wrapper"];
        assert_eq!(at(wrapper, "workspace_id"), &Value::Integer(7));
        assert_eq!(at(wrapper, "host"), &Value::from("h1"));
        // the old chain stashed the whole wrapper, message included
        assert_eq!(
            at(wrapper, "message"),
            &Value::Bytes(Bytes::from(compressed))
        );
        assert_eq!(event.len(), 5);
    }

    #[test]
    fn resolve_keeps_raw_bytes_when_inner_is_not_a_log_entry() {
        let (pool, desc) = lp_pool();
        // tag 1 as a 64-bit field: not how LogEntry encodes workspace_id, and truncated.
        let raw = [0x09, 0x01, 0x02];
        let event = unwrap_event(&desc, &wrapper_line(&pool, &raw, 7));

        assert_eq!(
            event["_lp_inner_bytes"],
            Value::Bytes(Bytes::copy_from_slice(&raw))
        );
        assert_eq!(event["_lp_log_entry_parsed"], Value::Boolean(false));
        assert_eq!(event["_lp_log_entry"], Value::Object(ObjectMap::new()));
        // falls back to the wrapper's workspace_id
        assert_eq!(event["_workspace_id"], Value::Integer(7));
    }

    /// `decode_zstd` in the old chain accepted a skippable frame (and returned nothing); so do we.
    #[test]
    fn resolve_treats_a_zstd_skippable_frame_like_decode_zstd() {
        let (pool, desc) = lp_pool();
        let skippable = [0x50, 0x2A, 0x4D, 0x18, 0, 0, 0, 0];
        assert_eq!(zstd::decode_all(&skippable[..]).unwrap(), Vec::<u8>::new());
        let event = unwrap_event(&desc, &wrapper_line(&pool, &skippable, 7));
        assert_eq!(event["_lp_inner_bytes"], Value::Bytes(Bytes::new()));
        assert_eq!(event["_lp_log_entry_parsed"], Value::Boolean(true));

        // a skippable frame in front of a real frame decompresses to the real frame's content
        let entry = log_entry(&pool, 42);
        let mut prefixed = skippable.to_vec();
        prefixed.extend(zstd::encode_all(&entry[..], 3).unwrap());
        assert_eq!(zstd::decode_all(&prefixed[..]).unwrap(), entry);
        let event = unwrap_event(&desc, &wrapper_line(&pool, &prefixed, 7));
        assert_eq!(event["_lp_inner_bytes"], Value::Bytes(Bytes::from(entry)));
        assert_eq!(event["_workspace_id"], Value::Integer(42));

        // and anything else without a frame magic is left alone, as decode_zstd would have failed
        for raw in [&b"\x51\x2a\x4d\x19"[..], b"\x28\xb5\x2f", b""] {
            assert!(zstd::decode_all(raw).is_err());
            assert!(!zstd_decodable(raw));
        }
    }

    #[test]
    fn resolve_keeps_original_bytes_on_a_corrupt_zstd_frame() {
        let (pool, desc) = lp_pool();
        let mut corrupt = ZSTD_MAGIC.to_vec();
        corrupt.extend([0xFF; 6]);
        let event = unwrap_event(&desc, &wrapper_line(&pool, &corrupt, 7));

        assert_eq!(event["_lp_inner_bytes"], Value::Bytes(Bytes::from(corrupt)));
        assert_eq!(event["_lp_log_entry_parsed"], Value::Boolean(false));
        assert_eq!(event["_workspace_id"], Value::Integer(7));
    }

    #[test]
    fn resolve_with_no_message_field_matches_the_vrl_chain() {
        let (pool, desc) = lp_pool();
        let wrapper = pool.get_message_by_name("LogDaemonWrapper").unwrap();
        let mut m = DynamicMessage::new(wrapper);
        m.set_field_by_name("workspace_id", prost_reflect::Value::I64(7));
        let line = base64_simd::STANDARD.encode_to_string(m.encode_to_vec());
        let event = unwrap_event(&desc, &line);

        // The VRL chain read a missing `message` as null: decode_zstd and parse_proto both fail.
        assert_eq!(event["_lp_inner_bytes"], Value::Null);
        assert_eq!(event["_lp_log_entry_parsed"], Value::Boolean(false));
        assert_eq!(event["_lp_log_entry"], Value::Object(ObjectMap::new()));
        assert_eq!(event["_workspace_id"], Value::Integer(7));
    }

    #[test]
    fn resolve_falls_back_to_wrapper_workspace_id_when_entry_has_none() {
        let (pool, desc) = lp_pool();
        let desc_entry = pool.get_message_by_name("logging.LogEntry").unwrap();
        let mut m = DynamicMessage::new(desc_entry);
        m.set_field_by_name(
            "system_uri",
            prost_reflect::Value::String("system:a".into()),
        );
        let event = unwrap_event(&desc, &wrapper_line(&pool, &m.encode_to_vec(), 7));

        assert_eq!(event["_lp_log_entry_parsed"], Value::Boolean(true));
        assert_eq!(
            at(&event["_lp_log_entry"], "system_uri"),
            &Value::from("system:a")
        );
        assert_eq!(event["_workspace_id"], Value::Integer(7));
    }

    /// The type checker must see the five written paths, so `if ._lp_log_entry_parsed { .. }`
    /// in a later statement or transform compiles exactly as it did after the VRL assignments.
    #[test]
    fn type_info_declares_the_written_paths() {
        let (_, desc) = lp_pool();
        let type_state = state::TypeState::default();
        let mut cctx = FunctionCompileContext::new(Span::new(0, 0), CompileConfig::default());
        let args = func_args![
            value: Bytes::from_static(b"x"),
            wrapper_descriptor_file: desc.clone(),
            log_entry_descriptor_file: desc,
        ];
        let expr = LpUnwrap
            .compile(&type_state, &mut cctx, args.into())
            .unwrap();
        let info = expr.type_info(&type_state);
        assert_eq!(info.result, TypeDef::null().fallible().impure());
        let target = info.state.external.target_kind();
        assert_eq!(
            target.at_path(&vrl::owned_value_path!("_lp_log_entry_parsed")),
            Kind::boolean()
        );
        assert_eq!(
            target.at_path(&vrl::owned_value_path!("_lp_inner_bytes")),
            Kind::bytes()
        );
        assert_eq!(
            target.at_path(&vrl::owned_value_path!("_workspace_id")),
            Kind::integer().or_null()
        );
        assert!(
            target
                .at_path(&vrl::owned_value_path!("_lp_log_entry"))
                .is_object()
        );
        assert!(
            target
                .at_path(&vrl::owned_value_path!("_lp_log_daemon_wrapper"))
                .is_object()
        );
    }

    #[test]
    fn resolve_errors_on_bad_base64_or_bad_wrapper() {
        let (_, desc) = lp_pool();
        let type_state = state::TypeState::default();
        for bad in [
            "!!!!",
            &base64_simd::STANDARD.encode_to_string([0x09, 0x01]),
        ] {
            let mut cctx = FunctionCompileContext::new(Span::new(0, 0), CompileConfig::default());
            let args = func_args![
                value: Bytes::from(bad.to_owned()),
                wrapper_descriptor_file: desc.clone(),
                log_entry_descriptor_file: desc.clone(),
            ];
            let expr = LpUnwrap
                .compile(&type_state, &mut cctx, args.into())
                .unwrap();
            let mut runtime_state = state::RuntimeState::default();
            let mut object = Value::Object(ObjectMap::new());
            let tz = TimeZone::default();
            let mut ctx = Context::new(&mut object, &mut runtime_state, &tz);
            assert!(expr.resolve(&mut ctx).is_err(), "{bad}");
        }
    }

    // ---- Parity with the VRL chain this function replaces -------------------------------------
    // Both programs run on the same event through the real VRL compiler and runtime; the whole
    // resulting event must be equal (or both must fail).

    const REFERENCE_VRL: &str = include_str!("../tests/data/lp_unwrap_reference.vrl");
    const REAL_WRAPPER_DESC: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/data/lp_log_daemon_wrapper_descriptor.pb"
    );
    const REAL_LOG_ENTRY_DESC: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/data/lp_log_entry_descriptor.pb"
    );

    /// Compiles and runs `source` with the crate's functions on `{message: line}`; returns the event.
    fn run_program(source: &str, line: &str) -> Result<Value, String> {
        use vrl::compiler::runtime::Runtime;
        use vrl::compiler::{TargetValue, compile};
        let program = compile(source, &crate::all())
            .map_err(|diagnostics| format!("{diagnostics:?}"))?
            .program;
        let mut event = ObjectMap::new();
        event.insert("message".into(), Value::Bytes(Bytes::from(line.to_owned())));
        let mut target = TargetValue {
            value: Value::Object(event),
            metadata: Value::Object(Default::default()),
            secrets: vrl::value::Secrets::default(),
        };
        Runtime::default()
            .resolve(&mut target, &program, &TimeZone::default())
            .map_err(|error| error.to_string())?;
        Ok(target.value)
    }

    fn assert_parity_with(wrapper_desc: &str, log_entry_desc: &str, line: &str, label: &str) {
        let reference = run_program(
            &REFERENCE_VRL
                .replace("{WRAPPER_DESCRIPTOR}", wrapper_desc)
                .replace("{LOG_ENTRY_DESCRIPTOR}", log_entry_desc),
            line,
        );
        let native = run_program(
            &format!(r#"lp_unwrap!(.message, "{wrapper_desc}", "{log_entry_desc}")"#),
            line,
        );
        match (&reference, &native) {
            (Ok(r), Ok(n)) => assert_eq!(n, r, "{label}: events differ"),
            (Err(_), Err(_)) => {}
            _ => panic!("{label}: reference={reference:?} native={native:?}"),
        }
    }

    /// Parity on the production LogDaemonWrapper / logging.LogEntry descriptors.
    fn assert_parity(line: &str, label: &str) {
        assert_parity_with(REAL_WRAPPER_DESC, REAL_LOG_ENTRY_DESC, line, label);
    }

    fn real_pool() -> DescriptorPool {
        let mut pool =
            DescriptorPool::decode(&std::fs::read(REAL_WRAPPER_DESC).unwrap()[..]).unwrap();
        pool.decode_file_descriptor_set(&std::fs::read(REAL_LOG_ENTRY_DESC).unwrap()[..])
            .unwrap();
        pool
    }

    fn sub(m: &DynamicMessage, field: &str) -> DynamicMessage {
        use prost_reflect::ReflectMessage;
        DynamicMessage::new(
            m.descriptor()
                .get_field_by_name(field)
                .unwrap()
                .kind()
                .as_message()
                .unwrap()
                .clone(),
        )
    }

    /// A LogEntry the way the HTTP path emits it: nested messages (log_time, event_id, whereami),
    /// enums, bytes and strings, plus the opaque inner row.
    fn real_log_entry(pool: &DescriptorPool, workspace_id: Option<i64>) -> Vec<u8> {
        use prost_reflect::Value as P;
        let mut e = DynamicMessage::new(pool.get_message_by_name("logging.LogEntry").unwrap());
        e.set_field_by_name("message", P::Bytes(Bytes::from(vec![0x42u8; 900])));
        e.set_field_by_name("system_uri", P::String("system:apiproxy".into()));
        e.set_field_by_name("project_name", P::String("apiproxy".into()));
        e.set_field_by_name("shard_name", P::String("aws-us-west-2-0".into()));
        e.set_field_by_name(
            "request_id",
            P::String("3f1c9a2e-7d44-4f1b-9e2a-1234567890ab".into()),
        );
        e.set_field_by_name("trace_id", P::String("a1b2c3d4e5f60718".into()));
        e.set_field_by_name("pod_name", P::String("apiproxy-7d9f8c6b5-xk2lq".into()));
        e.set_field_by_name(
            "account_id",
            P::String("1e2f3a4b-5c6d-7e8f-9a0b-c1d2e3f4a5b6".into()),
        );
        e.set_field_by_name("is_log_entry", P::Bool(true));
        e.set_field_by_name("logging_client_version", P::String("1.2.3".into()));
        e.set_field_by_name("workload_class", P::EnumNumber(2));
        e.set_field_by_name("emit_path", P::EnumNumber(3));
        if let Some(ws) = workspace_id {
            e.set_field_by_name("workspace_id", P::I64(ws));
        }
        let mut t = sub(&e, "log_time");
        t.set_field_by_name("seconds", P::I64(1_761_800_000));
        t.set_field_by_name("nanos", P::I32(123_456_000));
        e.set_field_by_name("log_time", P::Message(t));
        let mut id = sub(&e, "event_id");
        let mut ts = sub(&id, "timestamp");
        ts.set_field_by_name("seconds", P::I64(1_761_800_000));
        id.set_field_by_name("timestamp", P::Message(ts));
        id.set_field_by_name("process_id", P::I32(4242));
        id.set_field_by_name("thread_id", P::I64(777));
        id.set_field_by_name("pod_uid", P::Bytes(Bytes::from_static(b"\x01\x02\x03")));
        e.set_field_by_name("event_id", P::Message(id));
        let mut w = sub(&e, "whereami");
        w.set_field_by_name(
            "kubernetes_cluster_uri",
            P::String("kube://aws-us-west-2-0".into()),
        );
        w.set_field_by_name("region_uri", P::String("region:aws/us-west-2".into()));
        w.set_field_by_name("kubernetes_cluster_short_name", P::String("uw2-0".into()));
        w.set_field_by_name("cloud_provider", P::EnumNumber(1));
        e.set_field_by_name("whereami", P::Message(w));
        e.encode_to_vec()
    }

    fn real_wrapper_line(
        pool: &DescriptorPool,
        message: Option<&[u8]>,
        workspace_id: i64,
    ) -> String {
        use prost_reflect::Value as P;
        let mut w = DynamicMessage::new(pool.get_message_by_name("LogDaemonWrapper").unwrap());
        if let Some(m) = message {
            w.set_field_by_name("message", P::Bytes(Bytes::copy_from_slice(m)));
            w.set_field_by_name("length", P::I64(m.len() as i64));
        }
        w.set_field_by_name("shard_name", P::String("aws-us-west-2-0".into()));
        w.set_field_by_name("instance_id", P::String("i-0abc123def4567890".into()));
        w.set_field_by_name("stream_offset", P::I64(99_123_456));
        w.set_field_by_name("upload_time", P::I64(1_761_800_001_000));
        w.set_field_by_name("plane", P::EnumNumber(1));
        w.set_field_by_name(
            "stream_GUID",
            P::String("9d2c0c4e-0f1b-4a8e-9b1a-6c7d8e9f0a1b".into()),
        );
        w.set_field_by_name("workspace_id", P::I64(workspace_id));
        w.set_field_by_name("crc32", P::I32(-12345));
        w.set_field_by_name("pod_id", P::String("log-daemon-abc12".into()));
        w.set_field_by_name("is_delivered_by_logging_agent", P::Bool(true));
        w.set_field_by_name("is_diskless", P::Bool(false));
        w.set_field_by_name("region", P::String("us-west-2".into()));
        base64_simd::STANDARD.encode_to_string(w.encode_to_vec())
    }

    #[test]
    fn parity_with_the_vrl_chain_on_production_descriptors() {
        let pool = real_pool();
        let entry = real_log_entry(&pool, Some(42));
        let raw_row = sample(&pool_for_sample()); // a valid proto that is not a LogEntry (file path)
        let compressed = zstd::encode_all(&entry[..], 3).unwrap();
        let mut skippable_then_frame = vec![0x50, 0x2A, 0x4D, 0x18, 0, 0, 0, 0];
        skippable_then_frame.extend(&compressed);
        let mut corrupt = ZSTD_MAGIC.to_vec();
        corrupt.extend([0xFF; 6]);
        let mut bad_enum = real_log_entry(&pool, Some(42));
        // workload_class (tag 22, varint) = 99, a number the enum does not define
        bad_enum.extend([22 << 3, 99]);
        let bad_utf8 = [0x12, 0x02, 0xFF, 0xFE]; // system_uri = invalid UTF-8
        let cases: Vec<(&str, Option<Vec<u8>>)> = vec![
            ("http path: LogEntry", Some(entry.clone())),
            ("file path: zstd LogEntry", Some(compressed.clone())),
            ("file path: raw non-LogEntry proto", Some(raw_row)),
            (
                "zstd of a raw proto",
                Some(zstd::encode_all(&sample(&pool_for_sample())[..], 3).unwrap()),
            ),
            (
                "skippable frame alone",
                Some(vec![0x50, 0x2A, 0x4D, 0x18, 0, 0, 0, 0]),
            ),
            (
                "skippable frame then compressed frame",
                Some(skippable_then_frame),
            ),
            ("corrupt zstd frame", Some(corrupt)),
            (
                "LogEntry without workspace_id",
                Some(real_log_entry(&pool, None)),
            ),
            ("LogEntry with unknown enum number", Some(bad_enum)),
            ("LogEntry with invalid UTF-8", Some(bad_utf8.to_vec())),
            ("empty message", Some(Vec::new())),
            ("wrapper without message", None),
        ];
        for (label, message) in &cases {
            let line = real_wrapper_line(&pool, message.as_deref(), 7);
            assert_parity(&line, label);
        }
        // and failures fail on both sides
        for (label, line) in [
            ("bad base64", "!!!!".to_owned()),
            (
                "bad wrapper",
                base64_simd::STANDARD.encode_to_string([0x09, 0x01]),
            ),
        ] {
            assert_parity(&line, label);
        }
    }

    #[test]
    fn parity_with_the_vrl_chain_on_the_test_schema() {
        let (pool, desc) = lp_pool();
        let entry = log_entry(&pool, 42);
        for (label, message) in [
            ("zstd LogEntry", zstd::encode_all(&entry[..], 3).unwrap()),
            ("plain LogEntry", entry),
            ("raw bytes", vec![0x09, 0x01, 0x02]),
            ("empty", Vec::new()),
        ] {
            assert_parity_with(&desc, &desc, &wrapper_line(&pool, &message, 7), label);
        }
    }

    /// The 3-field schema from the earlier tests, used as "a proto that is not a LogEntry".
    fn pool_for_sample() -> DescriptorPool {
        let (pool, _) = lp_pool();
        pool
    }

    fn sample(pool: &DescriptorPool) -> Vec<u8> {
        // a LogDaemonWrapper from the test schema is a fine stand-in for an arbitrary target proto
        let desc = pool.get_message_by_name("LogDaemonWrapper").unwrap();
        let mut m = DynamicMessage::new(desc);
        m.set_field_by_name("host", prost_reflect::Value::String("raw-row".into()));
        m.set_field_by_name("workspace_id", prost_reflect::Value::I64(5));
        m.encode_to_vec()
    }

    /// The LogEntry parse is opportunistic; anything prost-reflect rejects (here: invalid UTF-8 in
    /// a `string` field) takes the raw path, exactly as `parse_proto` made it do before.
    #[test]
    fn resolve_treats_a_rejected_log_entry_as_raw_bytes() {
        let (pool, desc) = lp_pool();
        let bad = [0x12, 0x02, 0xFF, 0xFE]; // system_uri (tag 2, string) = invalid UTF-8
        let event = unwrap_event(&desc, &wrapper_line(&pool, &bad, 7));
        assert_eq!(event["_lp_log_entry_parsed"], Value::Boolean(false));
        assert_eq!(event["_lp_log_entry"], Value::Object(ObjectMap::new()));
        assert_eq!(
            event["_lp_inner_bytes"],
            Value::Bytes(Bytes::copy_from_slice(&bad))
        );
        assert_eq!(event["_workspace_id"], Value::Integer(7));
    }
}
