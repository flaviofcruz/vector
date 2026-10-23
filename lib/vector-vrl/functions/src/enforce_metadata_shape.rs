//! The `enforce_metadata_shape` VRL function: shape-enforces the Lumberjack `_log_metadata` envelope
//! on the Lumberjack Prime write path. It is the LP port of Scala `LogMetadataShapeEnforcer`, which
//! classic Lumberjack applies as a Spark UDF in `CentralizationBase`. The field -> shape policy and the
//! validators both live in Universe and are called through `vector_redaction_executor`.
//!
//! Given the `_log_metadata` object, each declared string field is kept iff it is empty, the literal
//! `"unknown"`, or matches one of its declared `DataShape`s; otherwise it is set to `null`. Absent
//! fields and non-string values are left untouched, and rows are never dropped (fields are only
//! nulled), so the stage cannot reduce completeness. Fallible: errors if `metadata` is not an object.

use vector_redaction_executor as executor;
use vrl::prelude::*;

#[derive(Clone, Copy, Debug)]
pub struct EnforceMetadataShape;

impl Function for EnforceMetadataShape {
    fn identifier(&self) -> &'static str {
        "enforce_metadata_shape"
    }

    fn summary(&self) -> &'static str {
        "shape-enforce the Lumberjack _log_metadata envelope"
    }

    fn usage(&self) -> &'static str {
        indoc! {"
            Shape-enforces the Lumberjack `_log_metadata` object: each declared string field in
            `service_metadata`, `service_metadata.data_plane_metadata`, and `log_daemon_metadata` is
            kept iff it is empty, the literal \"unknown\", or matches one of its declared DataShapes;
            otherwise it is set to null. Absent fields and non-string values are left untouched.

            Mirrors classic Lumberjack's LogMetadataShapeEnforcer so the LP write path and the Spark
            centralization ETL null the same values on the shared tables.

            Fallible (call as `enforce_metadata_shape!(...)`): errors if `metadata` is not an object.
        "}
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[Parameter {
            keyword: "metadata",
            kind: kind::OBJECT,
            required: true,
        }]
    }

    fn examples(&self) -> &'static [Example] {
        &[]
    }

    fn compile(
        &self,
        _state: &state::TypeState,
        _ctx: &mut FunctionCompileContext,
        arguments: ArgumentList,
    ) -> Compiled {
        let metadata = arguments.required("metadata");
        Ok(EnforceMetadataShapeFn { metadata }.as_expr())
    }
}

#[derive(Clone, Debug)]
struct EnforceMetadataShapeFn {
    metadata: Box<dyn Expression>,
}

/// Nulls every string field of `value` (when it is an object) that the policy for `section` rejects.
/// Two passes to satisfy the borrow checker: collect the non-conforming keys, then null them.
fn enforce_fields(value: &mut Value, section: &str) -> Result<(), ExpressionError> {
    let obj = match value {
        Value::Object(obj) => obj,
        _ => return Ok(()),
    };
    let mut to_null = Vec::new();
    for (field, value) in obj.iter() {
        if let Value::Bytes(bytes) = value
            && !executor::metadata_field_conforms(section, field, bytes)
                .map_err(|error| format!("enforce_metadata_shape: {error}"))?
        {
            to_null.push(field.clone());
        }
    }
    for field in to_null {
        obj.insert(field, Value::Null);
    }
    Ok(())
}

impl FunctionExpression for EnforceMetadataShapeFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let mut value = self.metadata.resolve(ctx)?;
        let root = match &mut value {
            Value::Object(root) => root,
            _ => return Err("enforce_metadata_shape: metadata must be an object".into()),
        };

        if let Some(service) = root.get_mut("service_metadata") {
            enforce_fields(service, "service_metadata")?;
            // data_plane_metadata is nested inside service_metadata (matches the Scala tree).
            if let Value::Object(service_obj) = service
                && let Some(data_plane) = service_obj.get_mut("data_plane_metadata")
            {
                enforce_fields(data_plane, "service_metadata.data_plane_metadata")?;
            }
        }
        if let Some(daemon) = root.get_mut("log_daemon_metadata") {
            enforce_fields(daemon, "log_daemon_metadata")?;
        }

        Ok(value)
    }

    fn type_def(&self, _: &state::TypeState) -> TypeDef {
        // Returns the (possibly field-nulled) metadata object; fallible on a non-object argument.
        TypeDef::object(Collection::any()).fallible()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use vrl::compiler::CompileConfig;
    use vrl::compiler::function::FunctionCompileContext;
    use vrl::compiler::state::{self, TypeState};
    use vrl::diagnostic::Span;

    const A_UUID: &str = "01ef1742-bfef-1d54-8e8f-0d79b856b0f3";
    const DBR_IMAGE_LABEL: &str = "release__18.2.x-photon-scala2.13__dbr__18.2.2__8020d77__099e4fa__jenkins__sdp-20260524-rc0-fbcacd4__format-4";

    fn object(pairs: Vec<(&str, Value)>) -> Value {
        let mut map = BTreeMap::new();
        for (key, value) in pairs {
            map.insert(key.into(), value);
        }
        Value::Object(map)
    }

    /// Compiles and runs `enforce_metadata_shape(metadata)`, returning the result.
    fn enforce(metadata: Value) -> Value {
        let type_state = TypeState::default();
        let mut cctx = FunctionCompileContext::new(Span::new(0, 0), CompileConfig::default());
        let args = func_args![metadata: metadata];
        let expr = EnforceMetadataShape
            .compile(&type_state, &mut cctx, args.into())
            .expect("compile should succeed");

        let mut runtime_state = state::RuntimeState::default();
        let mut root_object: Value = Value::Object(BTreeMap::new());
        let tz = TimeZone::default();
        let mut ctx = Context::new(&mut root_object, &mut runtime_state, &tz);
        expr.resolve(&mut ctx).expect("resolve should succeed")
    }

    fn at<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
        let mut cur = value;
        for key in path {
            cur = match cur {
                Value::Object(map) => map.get(*key)?,
                _ => return None,
            };
        }
        Some(cur)
    }

    /// The full happy-path tree from LogMetadataShapeEnforcerSuite — every enforced field carries a
    /// conforming value, so nothing is wiped and the output equals the input.
    fn happy_path_metadata() -> Value {
        let data_plane = object(vec![
            ("cluster_id", Value::from("0506-133027-8y3ep6tp-v2n")),
            ("spark_version", Value::from(DBR_IMAGE_LABEL)),
            ("cluster_creator", Value::from("JobLauncher")),
            ("spark_image_label", Value::from(DBR_IMAGE_LABEL)),
            ("cluster_node_type", Value::from("r8gd.8xlarge")),
            ("cluster_size_type", Value::from("VM_CONTAINER")),
            ("cluster_unity_catalog_mode", Value::from("SINGLE_USER")),
            ("dbsql_channel", Value::from("CHANNEL_NAME_CURRENT")),
            (
                "internal_cluster_id",
                Value::from("qeaj2n--i0508122019-c6vlrq5a"),
            ),
        ]);
        let service = object(vec![
            ("branch_name", Value::from("master")),
            ("system_uri", Value::from("system:notebook")),
            ("shard_name", Value::from("oregon-prod")),
            ("request_id", Value::from(A_UUID)),
            (
                "traceparent",
                Value::from("00-fc0296e891538bed49b0b1d6087ce59b-5f4344be67cae67a-00"),
            ),
            ("trace_id", Value::from("01f0c57fcad41b81aee48604eb996db7")),
            ("account_id", Value::from(A_UUID)),
            ("pod_uid", Value::from(A_UUID)),
            ("data_plane_metadata", data_plane),
        ]);
        let log_daemon = object(vec![
            ("shard_name", Value::from("oregon-prod")),
            ("stream_guid", Value::from(A_UUID)),
            ("plane", Value::from("PLANE_DATA")),
            ("cluster_type", Value::from("CLUSTER_TYPE_NEPHOS")),
            ("spark_image_label", Value::from(DBR_IMAGE_LABEL)),
            ("org_id", Value::from("12345")),
            ("region", Value::from("us-east-1")),
            ("unp_node_id", Value::from("i-059d5bb24debad6e7")),
        ]);
        object(vec![
            ("service_metadata", service),
            ("log_daemon_metadata", log_daemon),
        ])
    }

    #[test]
    fn happy_path_leaves_every_field_intact() {
        let input = happy_path_metadata();
        assert_eq!(enforce(input.clone()), input);
    }

    #[test]
    fn non_conforming_fields_are_nulled_including_nested_data_plane() {
        // Start from the happy path, then corrupt one field in each of the three sub-structs.
        let mut input = happy_path_metadata();
        if let Value::Object(root) = &mut input {
            if let Some(Value::Object(service)) = root.get_mut("service_metadata") {
                service.insert("request_id".into(), Value::from("not-a-uuid"));
                if let Some(Value::Object(dp)) = service.get_mut("data_plane_metadata") {
                    dp.insert("cluster_id".into(), Value::from("bogus"));
                }
            }
            if let Some(Value::Object(daemon)) = root.get_mut("log_daemon_metadata") {
                daemon.insert("stream_guid".into(), Value::from("not-a-guid"));
            }
        }
        let out = enforce(input);

        // The three corrupted fields are nulled.
        assert_eq!(
            at(&out, &["service_metadata", "request_id"]),
            Some(&Value::Null)
        );
        assert_eq!(
            at(
                &out,
                &["service_metadata", "data_plane_metadata", "cluster_id"]
            ),
            Some(&Value::Null)
        );
        assert_eq!(
            at(&out, &["log_daemon_metadata", "stream_guid"]),
            Some(&Value::Null)
        );
        // A conforming sibling is untouched.
        assert_eq!(
            at(&out, &["service_metadata", "system_uri"]),
            Some(&Value::from("system:notebook"))
        );
    }

    #[test]
    fn skipped_placeholders_are_kept() {
        let input = object(vec![(
            "service_metadata",
            object(vec![
                ("request_id", Value::from("")),
                ("account_id", Value::from("unknown")),
            ]),
        )]);
        let out = enforce(input);
        assert_eq!(
            at(&out, &["service_metadata", "request_id"]),
            Some(&Value::from(""))
        );
        assert_eq!(
            at(&out, &["service_metadata", "account_id"]),
            Some(&Value::from("unknown"))
        );
    }

    #[test]
    fn absent_log_daemon_metadata_is_handled() {
        let input = object(vec![(
            "service_metadata",
            object(vec![("request_id", Value::from("not-a-uuid"))]),
        )]);
        let out = enforce(input);
        // service_metadata still enforced, and the absent log_daemon_metadata causes no error.
        assert_eq!(
            at(&out, &["service_metadata", "request_id"]),
            Some(&Value::Null)
        );
        assert_eq!(at(&out, &["log_daemon_metadata"]), None);
    }
}
