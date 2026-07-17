//! The `apply_redaction` VRL function: redacts a serialized proto record using a compiled plan.
//!
//! `plan_file` is a compile-time literal, so the plan is read and registered once at compile time
//! and only the redaction walk runs per record. Fallible: a bad plan fails compilation, a malformed
//! record fails at runtime.

use vector_redaction_executor as executor;

use vrl::prelude::*;

use crate::redaction::resolver::{PlanKey, PlanResolver};

#[derive(Clone, Copy, Debug)]
pub struct ApplyRedaction;

impl Function for ApplyRedaction {
    fn identifier(&self) -> &'static str {
        "apply_redaction"
    }

    fn summary(&self) -> &'static str {
        "redact a serialized proto record using a compiled redaction plan"
    }

    fn usage(&self) -> &'static str {
        indoc! {"
            Redacts the serialized protobuf `message` using the compiled redaction plan at
            `plan_file`, returning the redacted protobuf bytes.

            `plan_file` must be a literal path to a serialized `RedactionPlanSet`. It is read and
            registered once when the VRL program is compiled.

            The function is fallible (call it as `apply_redaction!(...)`): it fails if the plan
            cannot be read or registered at compile time, or if the record is not valid protobuf at
            runtime.
        "}
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[
            Parameter {
                keyword: "message",
                kind: kind::BYTES,
                required: true,
            },
            Parameter {
                keyword: "plan_file",
                kind: kind::BYTES,
                required: true,
            },
        ]
    }

    fn examples(&self) -> &'static [Example] {
        // No runnable example: the plan path is machine-specific and the input/output are binary
        // protobuf. The tests below cover the behaviour.
        &[]
    }

    fn compile(
        &self,
        state: &state::TypeState,
        _ctx: &mut FunctionCompileContext,
        arguments: ArgumentList,
    ) -> Compiled {
        let message = arguments.required("message");

        // A literal path lets the plan be read and registered once, here at compile time.
        let plan_file = arguments.required_literal("plan_file", state)?;
        let plan_path = plan_file
            .try_bytes_utf8_lossy()
            .map_err(|_| {
                Box::new(ExpressionError::Error {
                    message: "apply_redaction: plan_file must be a string".to_owned(),
                    labels: vec![],
                    notes: vec![],
                }) as Box<dyn DiagnosticMessage>
            })?
            .into_owned();

        // Register the plan now; a bad or missing plan fails compilation with a diagnostic.
        let resolver = PlanResolver::from_plan_file(&plan_path).map_err(|e| {
            Box::new(ExpressionError::Error {
                message: format!("apply_redaction: {e}"),
                labels: vec![],
                notes: vec![],
            }) as Box<dyn DiagnosticMessage>
        })?;

        Ok(ApplyRedactionFn { message, resolver }.as_expr())
    }
}

#[derive(Clone, Debug)]
struct ApplyRedactionFn {
    message: Box<dyn Expression>,
    resolver: PlanResolver,
}

impl FunctionExpression for ApplyRedactionFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let message = self.message.resolve(ctx)?;
        let bytes = message.try_bytes()?;

        // PlanKey::default() selects the single plan registered at compile time. When the resolver
        // is populated with multiple plans (keyed by log type, policy group, or other record
        // attributes), a non-default key derived from the record would be passed here instead.
        let handle = self
            .resolver
            .handle_for(&PlanKey::default())
            .ok_or("apply_redaction: no redaction plan registered")?;

        let redacted =
            executor::redact(handle, &bytes).map_err(|e| format!("apply_redaction: {e}"))?;

        Ok(Value::Bytes(Bytes::from(redacted)))
    }

    fn type_def(&self, _: &state::TypeState) -> TypeDef {
        // Returns redacted proto bytes; fallible on a malformed record.
        TypeDef::bytes().fallible()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Write;
    use std::path::PathBuf;

    use prost::Message;
    use vector_redaction_executor::plan_proto;

    use super::*;
    use vrl::compiler::CompileConfig;
    use vrl::compiler::function::FunctionCompileContext;
    use vrl::compiler::state::{self, TypeState};
    use vrl::diagnostic::Span;

    /// Encodes a two-field proto record on the wire: field 1 and field 2, both varints.
    fn record(field1: u8, field2: u8) -> Vec<u8> {
        // tag = (field_number << 3) | wire_type(0 = varint)
        vec![(1 << 3), field1, (2 << 3), field2]
    }

    /// Writes a plan keeping field 1 and dropping field 2 to a temp `.pb`, returning its path.
    fn write_pass1_drop2_plan() -> String {
        let plan = plan_proto::RedactionPlanSet {
            plans: vec![plan_proto::MessageRedactionPlan {
                actions: vec![
                    plan_proto::FieldActionEntry {
                        field_number: Some(1),
                        action: Some(plan_proto::FieldAction {
                            action: Some(plan_proto::field_action::Action::PassThrough(
                                plan_proto::PassThrough {},
                            )),
                        }),
                    },
                    plan_proto::FieldActionEntry {
                        field_number: Some(2),
                        action: Some(plan_proto::FieldAction {
                            action: Some(plan_proto::field_action::Action::Remove(
                                plan_proto::Remove {},
                            )),
                        }),
                    },
                ],
                ..Default::default()
            }],
        };
        let path: PathBuf = std::env::temp_dir().join("apply_redaction_pass1_drop2.pb");
        let mut f = std::fs::File::create(&path).expect("create temp plan");
        f.write_all(&plan.encode_to_vec()).expect("write temp plan");
        path.to_str().expect("utf-8 temp path").to_owned()
    }

    /// The plan keeps field 1 and removes field 2, so `{1: 7, 2: 9}` redacts to `{1: 7}`.
    ///
    /// Plain `#[test]` (rather than vrl's `test_function!` macro) because `vector-vrl-functions`
    /// does not depend on the macro's `paste`/`anyhow`/`chrono_tz` support crates.
    #[test]
    fn redacts_per_plan() {
        let type_state = TypeState::default();
        let mut cctx = FunctionCompileContext::new(Span::new(0, 0), CompileConfig::default());
        let args = func_args![
            message: Bytes::from(record(7, 9)),
            plan_file: write_pass1_drop2_plan(),
        ];
        let expr = ApplyRedaction
            .compile(&type_state, &mut cctx, args.into())
            .expect("compile should succeed with a valid plan file");

        let mut runtime_state = state::RuntimeState::default();
        let mut object: Value = Value::Object(BTreeMap::new());
        let tz = TimeZone::default();
        let mut ctx = Context::new(&mut object, &mut runtime_state, &tz);

        let value = expr.resolve(&mut ctx).expect("resolve should succeed");
        assert_eq!(value, Value::Bytes(Bytes::from(vec![(1u8 << 3), 7u8])));
    }

    /// A missing plan file fails at compile time. Matched on a substring since the full message
    /// includes a platform-dependent OS error string.
    #[test]
    fn missing_plan_file_fails_to_compile() {
        let type_state = TypeState::default();
        let mut cctx = FunctionCompileContext::new(Span::new(0, 0), CompileConfig::default());
        let args = func_args![
            message: Bytes::from_static(b"anything"),
            plan_file: "/nonexistent/redaction_plan.pb",
        ];
        let err = ApplyRedaction
            .compile(&type_state, &mut cctx, args.into())
            .expect_err("compile should fail on a missing plan file")
            .message();
        assert!(
            err.contains("could not read redaction plan file"),
            "unexpected diagnostic: {err}"
        );
    }
}
