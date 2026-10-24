//! The `lp_splice_metadata` VRL function: builds the Lumberjack Prime wire_to_arrow splice's
//! injected-columns message (`_event_time`, `_partition_*`, `_shard_name`, `_project_name`,
//! `_system`, `_log_metadata.*`) natively, replacing the ~100-statement VRL `meta` build plus
//! `encode_proto!` in the universe splice template (`TransformLumberjackPrimeSpliceCodeFile`).
//!
//! The column mapping, its derivations, the metadata shape policy and the encoding live in Universe
//! (`//woodchuck/lp-splice-metadata`) behind `vector_redaction_executor`. At compile time the
//! executor resolves the mapping against the fat descriptor and names the event paths it reads; per
//! record this function reads exactly those off the event by borrow, applying the VRL conversions
//! (`encode_json`, `to_int`) the executor asks for, and hands them over.
//!
//! Fallible: errors where the VRL build errors — a present `log_time` whose `seconds` / `nanos`
//! `to_int` cannot coerce — or when the executor rejects the record.

use std::sync::Arc;

use vector_redaction_executor::{LpSplice, SpliceInput, SpliceInputMode};
use vrl::compiler::conversion::Conversion;
use vrl::path::{OwnedTargetPath, parse_target_path};
use vrl::prelude::*;

#[derive(Clone, Copy, Debug)]
pub struct LpSpliceMetadata;

impl Function for LpSpliceMetadata {
    fn identifier(&self) -> &'static str {
        "lp_splice_metadata"
    }

    fn summary(&self) -> &'static str {
        "build the Lumberjack Prime splice's injected-columns proto bytes"
    }

    fn usage(&self) -> &'static str {
        indoc! {"
            Encodes the Lumberjack Prime injected columns (`_event_time`, `_partition_*`,
            `_shard_name`, `_project_name`, `_system`, `_log_metadata.*`) for the fat
            `message_type` in `descriptor_file`, reading `._lp_log_entry`,
            `._lp_log_daemon_wrapper` and `.metadata` from the event. The result is concatenated
            with the redacted target-proto bytes by the splice. `enforce_shape` applies the
            `enforce_metadata_shape` policy to the encoded `_log_metadata` strings when it is
            `true`; any other value, including null, leaves it off, so a per-table marker that is
            absent on most events can be passed as-is.

            Fallible (call as `lp_splice_metadata!(...)`): errors if `log_time` is present and its
            `seconds` / `nanos` do not coerce like `to_int`, or if the executor rejects the record.
        "}
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[
            Parameter {
                keyword: "descriptor_file",
                kind: kind::BYTES,
                required: true,
            },
            Parameter {
                keyword: "message_type",
                kind: kind::BYTES,
                required: true,
            },
            Parameter {
                keyword: "enforce_shape",
                kind: kind::ANY,
                required: false,
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
                message: format!("lp_splice_metadata: {message}"),
                labels: vec![],
                notes: vec![],
            }) as Box<dyn DiagnosticMessage>
        };
        let literal_string = |name: &str, value: Value| {
            value
                .try_bytes_utf8_lossy()
                .map(|s| s.into_owned())
                .map_err(|_| compile_error(format!("{name} must be a string")))
        };
        let descriptor_file = literal_string(
            "descriptor_file",
            arguments.required_literal("descriptor_file", state)?,
        )?;
        let message_type = literal_string(
            "message_type",
            arguments.required_literal("message_type", state)?,
        )?;
        let splice = LpSplice::register(&descriptor_file, &message_type)
            .map_err(|error| compile_error(error.to_string()))?;
        let inputs = splice
            .inputs()
            .iter()
            .map(|(mode, path)| {
                parse_target_path(&format!(".{path}"))
                    .map(|path| (*mode, path))
                    .map_err(|error| compile_error(format!("invalid input path {path}: {error}")))
            })
            .collect::<Result<_, _>>()?;
        let enforce_shape = arguments.optional("enforce_shape");
        Ok(LpSpliceMetadataFn {
            splice: Arc::new(splice),
            inputs,
            enforce_shape,
        }
        .as_expr())
    }
}

#[derive(Clone, Debug)]
struct LpSpliceMetadataFn {
    splice: Arc<LpSplice>,
    inputs: Vec<(SpliceInputMode, OwnedTargetPath)>,
    enforce_shape: Option<Box<dyn Expression>>,
}

/// An input read off the event: borrowed, or converted for this record.
enum Read<'a> {
    Value(Option<&'a Value>),
    Json(Vec<u8>),
    Integer(i64),
}

impl FunctionExpression for LpSpliceMetadataFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let enforce_shape = match &self.enforce_shape {
            Some(expr) => expr.resolve(ctx)? == Value::Boolean(true),
            None => false,
        };
        let target = ctx.target();
        let reads = self
            .inputs
            .iter()
            .map(|(mode, path)| read(*mode, target.target_get(path).ok().flatten()))
            .collect::<Result<Vec<_>, ExpressionError>>()?;
        let inputs: Vec<SpliceInput> = reads.iter().map(splice_input).collect();
        let bytes = self
            .splice
            .encode(&inputs, enforce_shape)
            .map_err(|error| format!("lp_splice_metadata: {error}"))?;
        Ok(Value::Bytes(bytes))
    }

    fn type_def(&self, _: &state::TypeState) -> TypeDef {
        TypeDef::bytes().fallible()
    }
}

fn read(mode: SpliceInputMode, value: Option<&Value>) -> Result<Read<'_>, ExpressionError> {
    Ok(match mode {
        SpliceInputMode::Value => Read::Value(value),
        // VRL `encode_json`, applied only where the template calls it on a non-null value; missing
        // and null pass through as MISSING / NULL per the ABI.
        SpliceInputMode::Json => match value {
            None | Some(Value::Null) => Read::Value(value),
            Some(value) => Read::Json(
                serde_json::to_vec(value)
                    .map_err(|error| format!("lp_splice_metadata: {error}"))?,
            ),
        },
        SpliceInputMode::ToInt => Read::Integer(to_int(value)?),
    })
}

fn splice_input<'a>(read: &'a Read<'a>) -> SpliceInput<'a> {
    match read {
        Read::Value(None) => SpliceInput::Missing,
        Read::Value(Some(value)) => match value {
            Value::Null => SpliceInput::Null,
            Value::Bytes(bytes) => SpliceInput::Bytes(bytes),
            Value::Integer(i) => SpliceInput::Integer(*i),
            Value::Float(f) => SpliceInput::Float(f.into_inner()),
            Value::Boolean(b) => SpliceInput::Boolean(*b),
            Value::Timestamp(t) => SpliceInput::Timestamp(t.timestamp_micros()),
            _ => SpliceInput::Other,
        },
        Read::Json(json) => SpliceInput::Bytes(json),
        Read::Integer(i) => SpliceInput::Integer(*i),
    }
}

/// VRL's `to_int` coercion (missing reads as null): null -> 0, floats truncate, booleans -> 0 / 1,
/// numeric strings parse, timestamps -> epoch seconds; anything else errors.
fn to_int(value: Option<&Value>) -> Result<i64, ExpressionError> {
    Ok(match value.unwrap_or(&Value::Null) {
        Value::Integer(i) => *i,
        Value::Float(f) => f.into_inner() as i64,
        Value::Boolean(b) => i64::from(*b),
        Value::Null => 0,
        Value::Bytes(b) => match Conversion::Integer.convert::<Value>(b.clone()) {
            Ok(Value::Integer(i)) => i,
            Ok(_) => unreachable!("Conversion::Integer yields an integer"),
            Err(error) => return Err(format!("lp_splice_metadata: {error}").into()),
        },
        Value::Timestamp(t) => t.timestamp(),
        v => {
            return Err(format!(
                "lp_splice_metadata: unable to coerce {} into integer",
                v.kind()
            )
            .into());
        }
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use prost_reflect::DynamicMessage;
    use prost_reflect::prost::Message as _;
    use prost_reflect::prost_types::{DescriptorProto, FileDescriptorSet};
    use vrl::compiler::runtime::Runtime;
    use vrl::compiler::{TargetValue, TimeZone, compile};
    use vrl::protobuf::descriptor::get_message_descriptor;
    use vrl::value;
    use vrl::value::Secrets;

    use super::*;

    const DESCRIPTOR: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/data/lp_span_fat_descriptor.pb"
    );
    const MESSAGE_TYPE: &str = "com.databricks.logging.proto.Span";
    /// Every parity case runs against each of these fat descriptors, which declare different
    /// `_log_metadata` subsets.
    const TABLES: [(&str, &str); 3] = [
        (DESCRIPTOR, MESSAGE_TYPE),
        (
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/data/lp_service_health_event_fat_descriptor.pb"
            ),
            "com.databricks.logging.proto.ServiceHealthEvent",
        ),
        (
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/data/lp_query_profile_log_fat_descriptor.pb"
            ),
            "logging.QueryProfileLog",
        ),
    ];
    const REFERENCE_VRL: &str = include_str!("../tests/data/lp_splice_metadata_reference.vrl");

    fn run(source: &str, event: &Value) -> Result<Value, String> {
        let program = compile(source, &crate::all())
            .map_err(|diagnostics| format!("{diagnostics:?}"))?
            .program;
        let mut target = TargetValue {
            value: event.clone(),
            metadata: Value::Object(Default::default()),
            secrets: Secrets::default(),
        };
        Runtime::default()
            .resolve(&mut target, &program, &TimeZone::default())
            .map_err(|error| error.to_string())
    }

    fn reference_with(
        descriptor: &str,
        message_type: &str,
        event: &Value,
    ) -> Result<Value, String> {
        run(
            &REFERENCE_VRL
                .replace("{DESCRIPTOR}", descriptor)
                .replace("{MESSAGE_TYPE}", message_type),
            event,
        )
    }

    fn reference(event: &Value) -> Result<Value, String> {
        reference_with(DESCRIPTOR, MESSAGE_TYPE, event)
    }

    fn native_with(descriptor: &str, message_type: &str, event: &Value) -> Result<Value, String> {
        run(
            &format!(
                r#"lp_splice_metadata!("{descriptor}", "{message_type}", enforce_shape: ._lp_enforce_metadata_shape == true)"#
            ),
            event,
        )
    }

    fn native(event: &Value) -> Result<Value, String> {
        native_with(DESCRIPTOR, MESSAGE_TYPE, event)
    }

    /// Decodes and re-encodes canonically with the `now()`-stamped fields cleared, so the reference
    /// and native outputs compare deterministically. `event_time_from_now` also clears the columns
    /// derived from `_event_time`, which falls back to `now()` when the LogEntry has no `log_time`.
    fn normalized(
        descriptor: &str,
        message_type: &str,
        bytes: &Value,
        event_time_from_now: bool,
    ) -> Vec<u8> {
        let descriptor = get_message_descriptor(Path::new(descriptor), message_type).unwrap();
        let Value::Bytes(bytes) = bytes else {
            panic!("expected bytes, got {bytes:?}");
        };
        let mut message = DynamicMessage::decode(descriptor, bytes.clone()).unwrap();
        if event_time_from_now {
            for field in ["_event_time", "_partition_date", "_partition_hour"] {
                message.clear_field_by_name(field);
            }
        }
        let log_metadata = message
            .get_field_by_name_mut("_log_metadata")
            .unwrap()
            .as_message_mut()
            .unwrap();
        for (child, field) in [
            ("vector_metadata", "timestamp_ms"),
            ("etl_metadata", "ingestion_timestamp_ms"),
        ] {
            log_metadata
                .get_field_by_name_mut(child)
                .unwrap()
                .as_message_mut()
                .unwrap()
                .clear_field_by_name(field);
        }
        if event_time_from_now {
            log_metadata
                .get_field_by_name_mut("service_metadata")
                .unwrap()
                .as_message_mut()
                .unwrap()
                .clear_field_by_name("log_timestamp_ms");
        }
        message.encode_to_vec()
    }

    fn assert_parity_with(descriptor: &str, message_type: &str, event: Value) {
        let event_time_from_now = event.get(vrl::path!("_lp_log_entry", "log_time")).is_none();
        let reference = reference_with(descriptor, message_type, &event).expect("reference VRL");
        let native = native_with(descriptor, message_type, &event).expect("lp_splice_metadata");
        assert_eq!(
            normalized(descriptor, message_type, &native, event_time_from_now),
            normalized(descriptor, message_type, &reference, event_time_from_now),
            "{message_type}"
        );
    }

    fn assert_parity(event: Value) {
        for (descriptor, message_type) in TABLES {
            assert_parity_with(descriptor, message_type, event.clone());
        }
    }

    fn service_metadata(message: &DynamicMessage) -> DynamicMessage {
        let log_metadata = message.get_field_by_name("_log_metadata").unwrap();
        let service_metadata = log_metadata
            .as_message()
            .unwrap()
            .get_field_by_name("service_metadata")
            .unwrap();
        service_metadata.as_message().unwrap().clone()
    }

    /// Writes a copy of the Span fat descriptor with `field` removed from `service_metadata`'s
    /// message, standing in for a table whose descriptor omits an optional mapped column.
    fn descriptor_without_service_metadata_field(field: &str) -> String {
        fn remove(
            messages: &mut [DescriptorProto],
            prefix: &str,
            target: &str,
            field: &str,
        ) -> bool {
            messages.iter_mut().any(|message| {
                let name = if prefix.is_empty() {
                    message.name().to_owned()
                } else {
                    format!("{prefix}.{}", message.name())
                };
                if name == target {
                    message.field.retain(|f| f.name() != field);
                    return true;
                }
                remove(&mut message.nested_type, &name, target, field)
            })
        }

        let span = get_message_descriptor(Path::new(DESCRIPTOR), MESSAGE_TYPE).unwrap();
        let log_metadata = span.get_field_by_name("_log_metadata").unwrap().kind();
        let service_metadata = log_metadata
            .as_message()
            .unwrap()
            .get_field_by_name("service_metadata")
            .unwrap()
            .kind();
        let target = service_metadata
            .as_message()
            .unwrap()
            .full_name()
            .to_owned();
        let mut set =
            FileDescriptorSet::decode(std::fs::read(DESCRIPTOR).unwrap().as_slice()).unwrap();
        let removed = set.file.iter_mut().any(|file| {
            let package = file.package().to_owned();
            remove(&mut file.message_type, &package, &target, field)
        });
        assert!(removed, "{target} not found in {DESCRIPTOR}");
        let path = std::env::temp_dir().join(format!(
            "lp_splice_metadata_without_{field}_{}.pb",
            std::process::id()
        ));
        std::fs::write(&path, set.encode_to_vec()).unwrap();
        path.to_str().unwrap().to_owned()
    }

    fn http_path_event(enforce_shape: bool, request_id: &str) -> Value {
        value!({
            "_lp_enforce_metadata_shape": enforce_shape,
            "_lp_log_entry": {
                "log_time": { "seconds": 1792244700, "nanos": 250000000 },
                "shard_name": "prod-aws-us-west-2",
                "project_name": "le-project",
                "branch_name": "le-branch",
                "request_id": request_id,
                "event_id_uuid": "le-event-uuid",
                "parent_event_id": "le-parent",
                "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                "trace_id": "0af7651916cd43dd8448eb211c80319c",
                "server_uuid": "server-uuid-bytes",
                "message_sequence_number": 42,
                "pod_name": "le-pod",
                "pod_uid": "not-a-uuid",
                "system_uri": "system:le-system",
                "workspace_id": 1234567,
                "account_id": "01ef1742-bfef-1d54-8e8f-0d79b856b0f3",
                "workload_id": "le-workload",
                "sampling_rate": 7,
                "policy": { "mode": "le-policy" },
                "logging_client_version": "le-client",
                "workload_class": "le-workload-class",
                "data_plane_metadata": {
                    "cluster_id": "not-a-cluster-id",
                    "spark_version": "le-spark",
                    "internal_cluster_id": "le-internal",
                },
                "whereami": { "kubernetes_location": {
                    "cloud_provider": "AWS",
                    "cloud_provider_region": "AWS_US_WEST_2",
                    "kubernetes_cluster_uri": "kubernetes-cluster:le",
                } },
            },
            "_lp_log_daemon_wrapper": {
                "shard_name": "ldw-shard",
                "instance_id": "ldw-instance",
                "stream_offset": 11,
                "upload_time": 22,
                "length": 33,
                "stream_GUID": "ldw-guid",
                "workspace_id": 7654321,
                "is_diskless": true,
                "plane": "CONTROL",
                "metadata": {
                    "pod_id": "ldw-pod",
                    "cluster_type": "ldw-cluster-type",
                    "vm_id": "ldw-vm",
                    "k8s_context": "kubernetes-cluster:ldw",
                },
            },
            "metadata": {
                "agent_host_name": "agent-host",
                "agent_timestamp_ms": 99,
                "agent_cloud_provider": "AWS",
            },
        })
    }

    #[test]
    fn http_path_matches_reference() {
        assert_parity(http_path_event(false, "le-request-id"));
    }

    #[test]
    fn shape_enforcement_matches_reference() {
        let event = http_path_event(true, "not-a-uuid");
        assert_parity(event.clone());
        // The non-conforming request_id must actually be dropped, not just match a dropped reference.
        let descriptor = get_message_descriptor(Path::new(DESCRIPTOR), MESSAGE_TYPE).unwrap();
        let Value::Bytes(bytes) = native(&event).unwrap() else {
            unreachable!()
        };
        let message = DynamicMessage::decode(descriptor, bytes).unwrap();
        let service_metadata = service_metadata(&message);
        assert!(!service_metadata.has_field_by_name("request_id"));
        assert!(service_metadata.has_field_by_name("account_id"));
    }

    #[test]
    fn json_inputs_keep_missing_and_null_apart() {
        let object = value!({"id": "legacy"});
        for (value, expected) in [
            (None, SpliceInput::Missing),
            (Some(&Value::Null), SpliceInput::Null),
            (Some(&object), SpliceInput::Bytes(br#"{"id":"legacy"}"#)),
        ] {
            let read = read(SpliceInputMode::Json, value).unwrap();
            assert_eq!(splice_input(&read), expected, "{value:?}");
        }
    }

    #[test]
    fn environment_cloud_type_follows_vector_cloud_type() {
        // Both sides read the process environment; scripts/test-databricks-redaction.sh sets it.
        let Ok(cloud_type) = std::env::var("VECTOR_CLOUD_TYPE") else {
            return;
        };
        let event = http_path_event(false, "le-request-id");
        assert_parity(event.clone());
        let descriptor = get_message_descriptor(Path::new(DESCRIPTOR), MESSAGE_TYPE).unwrap();
        let Value::Bytes(bytes) = native(&event).unwrap() else {
            unreachable!()
        };
        let message = DynamicMessage::decode(descriptor, bytes).unwrap();
        let environment = message
            .get_field_by_name("_environment")
            .unwrap()
            .as_message()
            .unwrap()
            .clone();
        assert_eq!(
            environment
                .get_field_by_name("cloud_type")
                .unwrap()
                .as_str(),
            Some(cloud_type.as_str())
        );
    }

    #[test]
    fn non_true_enforce_shape_is_off() {
        let mut event = http_path_event(false, "not-a-uuid");
        let encode = |marker: &str, event: &Value| {
            let source = format!(
                r#"lp_splice_metadata!("{DESCRIPTOR}", "{MESSAGE_TYPE}", enforce_shape: {marker})"#
            );
            normalized(
                DESCRIPTOR,
                MESSAGE_TYPE,
                &run(&source, event).unwrap(),
                false,
            )
        };
        let off = encode("false", &event);
        assert_ne!(
            encode("true", &event),
            off,
            "enforcement must drop the non-conforming request_id"
        );
        for marker in ["null", "\"true\"", "1"] {
            assert_eq!(encode(marker, &event), off, "{marker}");
        }
        // The template passes the marker field directly; most tables never set it.
        let Value::Object(fields) = &mut event else {
            unreachable!()
        };
        fields.remove("_lp_enforce_metadata_shape");
        assert_eq!(encode("._lp_enforce_metadata_shape", &event), off);
    }

    #[test]
    fn descriptor_missing_mapped_field_is_skipped() {
        let descriptor = descriptor_without_service_metadata_field("workload_class");
        let event = http_path_event(false, "le-request-id");
        assert_parity_with(&descriptor, MESSAGE_TYPE, event.clone());
        // Decoding against the full descriptor catches a stale field number: the dropped column
        // must be absent and its neighbours must still land on their own numbers.
        let full = get_message_descriptor(Path::new(DESCRIPTOR), MESSAGE_TYPE).unwrap();
        let Value::Bytes(bytes) = native_with(&descriptor, MESSAGE_TYPE, &event).unwrap() else {
            unreachable!()
        };
        let service_metadata = service_metadata(&DynamicMessage::decode(full, bytes).unwrap());
        assert!(!service_metadata.has_field_by_name("workload_class"));
        assert_eq!(
            service_metadata
                .get_field_by_name("client_version")
                .unwrap()
                .as_str(),
            Some("le-client")
        );
        assert_eq!(
            service_metadata
                .get_field_by_name("request_id")
                .unwrap()
                .as_str(),
            Some("le-request-id")
        );
    }

    #[test]
    fn log_time_coercion_matches_reference() {
        for log_time in [
            value!(null),
            value!({ "seconds": "1792244700", "nanos": "250000000" }),
            value!({ "seconds": 1792244700.0, "nanos": 250000000.0 }),
            value!({ "seconds": 1792244700 }),
            value!({ "seconds": 1792244700, "nanos": null }),
        ] {
            assert_parity(value!({
                "_lp_log_entry": { "log_time": log_time },
                "_lp_log_daemon_wrapper": {},
            }));
        }
    }

    #[test]
    fn file_path_data_plane_matches_reference() {
        assert_parity(value!({
            "_lp_log_entry": {},
            "_lp_log_daemon_wrapper": {
                "shard_name": "dp-shard",
                "plane": "DATA",
                "metadata": { "vm_id": "dp-vm" },
            },
        }));
    }

    #[test]
    fn edge_cases_match_reference() {
        assert_parity(value!({
            "_lp_log_entry": {
                "log_time": { "seconds": 1792244700, "nanos": 0 },
                "event_id": { "id": "legacy" },
                "sampling_rate": 0,
                "system_uri": "not-a-system-uri",
                "workspace_id": "1234",
            },
            "_lp_log_daemon_wrapper": { "plane": "UNKNOWN", "is_diskless": false },
        }));
    }

    #[test]
    fn out_of_range_log_time_partitions_like_reference() {
        // 10^13 s is past chrono's range: the template's `from_unix_timestamp(...) ?? now()`
        // partitions the row by now(), so compare with the now()-derived columns cleared, then
        // check the partitions are present and agree.
        let event = value!({
            "_lp_log_entry": { "log_time": { "seconds": 10_000_000_000_000_i64, "nanos": 0 } },
            "_lp_log_daemon_wrapper": {},
        });
        let reference = reference(&event).expect("reference VRL");
        let native = native(&event).expect("lp_splice_metadata");
        assert_eq!(
            normalized(DESCRIPTOR, MESSAGE_TYPE, &native, true),
            normalized(DESCRIPTOR, MESSAGE_TYPE, &reference, true)
        );
        let descriptor = get_message_descriptor(Path::new(DESCRIPTOR), MESSAGE_TYPE).unwrap();
        let decode = |bytes: &Value| {
            let Value::Bytes(bytes) = bytes else {
                unreachable!()
            };
            DynamicMessage::decode(descriptor.clone(), bytes.clone()).unwrap()
        };
        let (native, reference) = (decode(&native), decode(&reference));
        for field in ["_event_time", "_partition_date"] {
            assert!(native.has_field_by_name(field), "{field} missing");
            assert_eq!(
                native.get_field_by_name(field),
                reference.get_field_by_name(field)
            );
        }
    }

    #[test]
    fn timestamp_upload_time_matches_reference() {
        let mut event = value!({
            "_lp_log_entry": { "log_time": { "seconds": 1792244700, "nanos": 0 } },
            "_lp_log_daemon_wrapper": { "instance_id": "ldw-instance" },
        });
        let upload_time = chrono::DateTime::from_timestamp_micros(1_792_244_700_250_000).unwrap();
        event.insert(
            vrl::path!("_lp_log_daemon_wrapper", "upload_time"),
            Value::Timestamp(upload_time),
        );
        assert_parity(event);
    }

    #[test]
    fn non_integer_log_time_errors_like_reference() {
        let event = value!({
            "_lp_log_entry": { "log_time": { "seconds": "soon", "nanos": 0 } },
            "_lp_log_daemon_wrapper": {},
        });
        assert!(reference(&event).is_err());
        assert!(native(&event).is_err());
    }

    #[test]
    fn missing_descriptor_fails_to_compile() {
        let source = format!(r#"lp_splice_metadata!("/nonexistent.pb", "{MESSAGE_TYPE}")"#);
        assert!(compile(&source, &crate::all()).is_err());
    }
}
