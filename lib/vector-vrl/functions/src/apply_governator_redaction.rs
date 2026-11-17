use vector_redaction_executor as executor;

use vrl::prelude::*;

use crate::redaction::resolver::{PlanKey, PlanResolver};

#[derive(Clone, Copy, Debug)]
pub struct ApplyGovernatorRedaction;

impl Function for ApplyGovernatorRedaction {
    fn identifier(&self) -> &'static str {
        "apply_governator_redaction"
    }

    fn summary(&self) -> &'static str {
        "redact a serialized proto record with the Governator plan for its log topic and policy"
    }

    fn usage(&self) -> &'static str {
        indoc! {"
            Redacts the serialized protobuf `message` with the plan baked for (`log_topic`,
            `policy`), returning the redacted protobuf bytes.

            `plan_dir` must be a literal path. When the VRL program is compiled, every regular file
            in it named `<POLICY>__<log-topic>.pb` is read and registered: the policy is upper snake
            case and the log topic is dash case. Other files are ignored. A `.pb` file with any other
            name, a plan the executor rejects, or a plan that needs a PUID lookup fails compilation.
            A directory with no plan files compiles.

            `log_topic` and `policy` are evaluated per record. Shape enforcement is off and
            non-centralizable fields are wiped.

            The function is fallible (call it as `apply_governator_redaction!(...)`): it fails if
            no plan is registered for (`log_topic`, `policy`) or if the record is not valid
            protobuf.
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
                keyword: "plan_dir",
                kind: kind::BYTES,
                required: true,
            },
            Parameter {
                keyword: "log_topic",
                kind: kind::BYTES,
                required: true,
            },
            Parameter {
                keyword: "policy",
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
                message: format!("apply_governator_redaction: {message}"),
                labels: vec![],
                notes: vec![],
            }) as Box<dyn DiagnosticMessage>
        };
        let message = arguments.required("message");
        let log_topic = arguments.required("log_topic");
        let policy = arguments.required("policy");
        let plan_dir = arguments
            .required_literal("plan_dir", state)?
            .try_bytes_utf8_lossy()
            .map_err(|_| compile_error("plan_dir must be a string".to_owned()))?
            .into_owned();
        let resolver =
            PlanResolver::from_plan_dir(&plan_dir).map_err(|e| compile_error(e.to_string()))?;

        Ok(ApplyGovernatorRedactionFn {
            message,
            log_topic,
            policy,
            resolver,
        }
        .as_expr())
    }
}

#[derive(Clone, Debug)]
struct ApplyGovernatorRedactionFn {
    message: Box<dyn Expression>,
    log_topic: Box<dyn Expression>,
    policy: Box<dyn Expression>,
    resolver: PlanResolver,
}

impl FunctionExpression for ApplyGovernatorRedactionFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let message = self.message.resolve(ctx)?;
        let bytes = message.try_bytes()?;
        let log_topic = self.log_topic.resolve(ctx)?;
        let policy = self.policy.resolve(ctx)?;
        let key = PlanKey {
            log_topic: log_topic.try_bytes_utf8_lossy()?.into_owned(),
            policy: policy.try_bytes_utf8_lossy()?.into_owned(),
        };
        let handle = self.resolver.handle_for(&key).ok_or_else(|| {
            format!(
                "apply_governator_redaction: no Governator plan for ({}, {})",
                key.log_topic, key.policy
            )
        })?;
        let redacted = executor::redact(handle, &bytes, executor::ShapeEnforcement::Off)
            .map_err(|e| format!("apply_governator_redaction: {e}"))?;
        Ok(Value::Bytes(redacted))
    }

    fn type_def(&self, _: &state::TypeState) -> TypeDef {
        TypeDef::bytes().fallible()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use prost::Message;
    use vector_redaction_executor::plan_proto;
    use vector_redaction_executor::plan_proto::field_action::Action;

    use super::*;
    use vrl::compiler::CompileConfig;
    use vrl::compiler::function::FunctionCompileContext;
    use vrl::compiler::state::{self, TypeState};
    use vrl::diagnostic::Span;

    static PLAN_DIR_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    const TOPIC: &str = "ai-products-event-log";
    const KEEP_FIRST: &str = "GOVERNATOR_REDACTION_PLAN_KEY_USER_GENERATED_CONTENT";
    const KEEP_SECOND: &str = "GOVERNATOR_REDACTION_PLAN_KEY_GENAI_MODEL_IO";

    fn plan_dir(name: &str) -> PathBuf {
        let sequence = PLAN_DIR_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "apply_governator_redaction_{name}_{}_{}",
            std::process::id(),
            sequence
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create plan dir");
        dir
    }

    fn field_action(field_number: i32, action: Action) -> plan_proto::FieldActionEntry {
        plan_proto::FieldActionEntry {
            field_number: Some(field_number),
            action: Some(plan_proto::FieldAction {
                action: Some(action),
            }),
        }
    }

    fn keep_one_field_plan(kept: i32, removed: i32) -> Vec<u8> {
        plan_proto::RedactionPlanSet {
            plans: vec![plan_proto::MessageRedactionPlan {
                actions: vec![
                    field_action(kept, Action::PassThrough(plan_proto::PassThrough {})),
                    field_action(removed, Action::Remove(plan_proto::Remove {})),
                ],
                ..Default::default()
            }],
        }
        .encode_to_vec()
    }

    fn puid_plan() -> Vec<u8> {
        plan_proto::RedactionPlanSet {
            plans: vec![plan_proto::MessageRedactionPlan {
                actions: vec![
                    field_action(1, Action::Remove(plan_proto::Remove {})),
                    field_action(
                        2,
                        Action::RedactFrom(plan_proto::RedactFrom {
                            source_field_number: Some(1),
                            transform: Some(plan_proto::RedactFromTransform::GeneratePuid as i32),
                            is_repeated: Some(false),
                            puid_target_type: Some(plan_proto::PuidTargetType::Int64 as i32),
                            ..Default::default()
                        }),
                    ),
                ],
                ..Default::default()
            }],
        }
        .encode_to_vec()
    }

    fn write_plan(dir: &Path, policy: &str, topic: &str, plan: Vec<u8>) {
        std::fs::write(dir.join(format!("{policy}__{topic}.pb")), plan).expect("write plan");
    }

    fn governator_plan_dir() -> PathBuf {
        let dir = plan_dir("governator");
        write_plan(&dir, KEEP_FIRST, TOPIC, keep_one_field_plan(1, 2));
        write_plan(&dir, KEEP_SECOND, TOPIC, keep_one_field_plan(2, 1));
        dir
    }

    fn record(field1: u8, field2: u8) -> Vec<u8> {
        vec![(1 << 3), field1, (2 << 3), field2]
    }

    fn compile(dir: &Path, log_topic: &str, policy: &str) -> Compiled {
        let mut cctx = FunctionCompileContext::new(Span::new(0, 0), CompileConfig::default());
        let args = func_args![
            message: Bytes::from(record(7, 9)),
            plan_dir: dir.to_str().expect("utf-8 plan dir"),
            log_topic: log_topic,
            policy: policy,
        ];
        ApplyGovernatorRedaction.compile(&TypeState::default(), &mut cctx, args.into())
    }

    fn resolve(expr: Box<dyn Expression>) -> Resolved {
        let mut runtime_state = state::RuntimeState::default();
        let mut object: Value = Value::Object(BTreeMap::new());
        let tz = TimeZone::default();
        let mut ctx = Context::new(&mut object, &mut runtime_state, &tz);
        expr.resolve(&mut ctx)
    }

    fn runtime_error(result: Resolved) -> String {
        match result {
            Err(ExpressionError::Error { message, .. }) => message,
            other => panic!("expected a runtime error, got {other:?}"),
        }
    }

    fn compile_error(dir: &Path) -> String {
        compile(dir, TOPIC, KEEP_FIRST)
            .expect_err("compile should fail")
            .message()
    }

    #[test]
    fn selects_plan_by_topic_and_policy() {
        let dir = governator_plan_dir();
        let first = compile(&dir, TOPIC, KEEP_FIRST).expect("compile");
        let second = compile(&dir, TOPIC, KEEP_SECOND).expect("compile");
        assert_eq!(
            resolve(first).expect("resolve"),
            Value::Bytes(Bytes::from(vec![(1u8 << 3), 7u8]))
        );
        assert_eq!(
            resolve(second).expect("resolve"),
            Value::Bytes(Bytes::from(vec![(2u8 << 3), 9u8]))
        );
    }

    #[test]
    fn unknown_pair_errors() {
        let dir = governator_plan_dir();
        for (topic, policy) in [
            (TOPIC, "GOVERNATOR_REDACTION_PLAN_KEY_DOES_NOT_EXIST"),
            ("query-profile-log", KEEP_FIRST),
        ] {
            let err = runtime_error(resolve(compile(&dir, topic, policy).expect("compile")));
            assert!(
                err.contains(&format!("no Governator plan for ({topic}, {policy})")),
                "unexpected error: {err}"
            );
        }
    }

    #[test]
    fn ignores_non_pb_files() {
        let dir = governator_plan_dir();
        std::fs::write(dir.join("topics.csv"), "log_topic\nai-products-event-log\n")
            .expect("write topics csv");
        std::fs::write(dir.join("assignment__dev.csv"), "workspace_id,policy\n")
            .expect("write assignment csv");
        std::fs::create_dir(dir.join("nested.pb")).expect("create nested dir");
        let expr = compile(&dir, TOPIC, KEEP_FIRST).expect("compile");
        assert_eq!(
            resolve(expr).expect("resolve"),
            Value::Bytes(Bytes::from(vec![(1u8 << 3), 7u8]))
        );
    }

    #[test]
    fn compile_accepts_dir_without_plan_files() {
        let dir = plan_dir("empty");
        std::fs::write(dir.join("topics.csv"), "log_topic\n").expect("write topics csv");
        let err = runtime_error(resolve(compile(&dir, TOPIC, KEEP_FIRST).expect("compile")));
        assert!(
            err.contains("no Governator plan"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn compile_rejects_missing_plan_dir() {
        let err = compile_error(Path::new("/nonexistent/governator"));
        assert!(
            err.contains("could not list redaction plan directory"),
            "unexpected diagnostic: {err}"
        );
    }

    #[test]
    fn compile_rejects_malformed_plan_file_name() {
        for name in [
            "redaction_plan__ai-products-event-log__pipeline.pb",
            "GOVERNATOR_REDACTION_PLAN_KEY_GENAI_MODEL_IO__ai_products_event_log.pb",
            "governator_redaction_plan_key_genai_model_io__ai-products-event-log.pb",
            "GOVERNATOR_REDACTION_PLAN_KEY_GENAI_MODEL_IO.pb",
            "__ai-products-event-log.pb",
        ] {
            let dir = governator_plan_dir();
            std::fs::write(dir.join(name), keep_one_field_plan(1, 2)).expect("write plan");
            let err = compile_error(&dir);
            assert!(
                err.contains(&format!("{name:?} is not named <POLICY>__<log-topic>.pb")),
                "unexpected diagnostic for {name}: {err}"
            );
        }
    }

    #[test]
    fn compile_rejects_plan_the_executor_rejects() {
        let dir = governator_plan_dir();
        write_plan(
            &dir,
            "GOVERNATOR_REDACTION_PLAN_KEY_BROKEN",
            TOPIC,
            vec![0xff, 0xff],
        );
        let err = compile_error(&dir);
        assert!(
            err.contains("invalid redaction plan"),
            "unexpected diagnostic: {err}"
        );
    }

    #[test]
    fn compile_rejects_plan_that_needs_puid_lookup() {
        let dir = governator_plan_dir();
        write_plan(
            &dir,
            "GOVERNATOR_REDACTION_PLAN_KEY_PUID",
            TOPIC,
            puid_plan(),
        );
        let err = compile_error(&dir);
        assert!(
            err.contains("needs a PUID lookup"),
            "unexpected diagnostic: {err}"
        );
    }
}
