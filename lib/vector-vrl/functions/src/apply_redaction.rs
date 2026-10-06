//! The `apply_redaction` VRL function: redacts a serialized proto record using a compiled plan.
//!
//! `plan_file` is a compile-time literal, so the plan is read and registered once at compile time
//! and only the redaction walk runs per record. Fallible: a bad plan fails compilation, a malformed
//! record fails at runtime.

use vector_redaction_executor as executor;

use enrichment::{Case, Condition, IndexHandle, TableRegistry, TableSearch};
use vrl::prelude::*;

use crate::redaction::resolver::{PlanKey, PlanResolver};

const PUID_MAPPING_TABLE: &str = "puid_mapping";
const USER_ID_FIELD: &str = "user_id";
const PUID_FIELD: &str = "puid";

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

            `enforce_shapes` is an optional literal boolean (default false) that gates data-shape
            enforcement: when true, `PassThroughString` fields are kept only if they match a declared
            shape; when false or absent, they pass through verbatim. It must be a literal so the
            verdict is a static property of the VRL program, resolved once at compile time and NOT
            influenceable by record content (this gate is the fail-closed boundary). Set to true only
            for LP pipeline plans; the LP splice sets this true, while Governator omits it (defaults
            false) to scope enforcement to the LP write path.

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
            Parameter {
                keyword: "enforce_shapes",
                kind: kind::BOOLEAN,
                required: false,
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
        ctx: &mut FunctionCompileContext,
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

        // A literal (not a per-record expression) so the enforcement verdict is a static property
        // of the VRL program: record content cannot flip the fail-closed gate off. Absent => Off.
        let enforcement = match arguments.optional_literal("enforce_shapes", state)? {
            Some(value) => {
                let enforce = value.try_boolean().map_err(|_| {
                    Box::new(ExpressionError::Error {
                        message: "apply_redaction: enforce_shapes must be a boolean".to_owned(),
                        labels: vec![],
                        notes: vec![],
                    }) as Box<dyn DiagnosticMessage>
                })?;
                if enforce {
                    executor::ShapeEnforcement::On
                } else {
                    executor::ShapeEnforcement::Off
                }
            }
            None => executor::ShapeEnforcement::Off,
        };

        // Enrichment tables are optional for this function because a Vector instance can compile
        // the same plan before the regional table is configured. A PUID plan still uses the
        // callback ABI in that case; the unavailable lookup returns Error and the executor applies
        // the plan's nonblocking miss behavior. When the table is present, an index is mandatory:
        // falling back to an unindexed scan would put the full regional map on the per-record path.
        let puid_lookup = if resolver.requires_puid_lookup() {
            match ctx.get_external_context_mut::<TableRegistry>() {
                Some(registry)
                    if registry
                        .table_ids()
                        .iter()
                        .any(|table| table == PUID_MAPPING_TABLE) =>
                {
                    let index = registry
                        .add_index(PUID_MAPPING_TABLE, Case::Sensitive, &[USER_ID_FIELD])
                        .map_err(|error| {
                            Box::new(ExpressionError::Error {
                                message: format!(
                                    "apply_redaction: could not index {PUID_MAPPING_TABLE}.{USER_ID_FIELD}: {error}"
                                ),
                                labels: vec![],
                                notes: vec![],
                            }) as Box<dyn DiagnosticMessage>
                        })?;
                    Some(PuidLookup {
                        tables: registry.as_readonly(),
                        index,
                    })
                }
                _ => None,
            }
        } else {
            None
        };

        Ok(ApplyRedactionFn {
            message,
            resolver,
            enforcement,
            puid_lookup,
        }
        .as_expr())
    }
}

#[derive(Clone, Debug)]
struct PuidLookup {
    tables: TableSearch,
    index: IndexHandle,
}

impl PuidLookup {
    fn find(&self, user_id: i64) -> executor::PuidLookupResult {
        let condition = [Condition::Equals {
            field: USER_ID_FIELD,
            value: Value::Integer(user_id),
        }];
        let rows = match self.tables.find_table_rows(
            PUID_MAPPING_TABLE,
            Case::Sensitive,
            &condition,
            None,
            None,
            Some(self.index),
        ) {
            Ok(rows) => rows,
            Err(_) => return executor::PuidLookupResult::Error,
        };

        match rows.as_slice() {
            [] => executor::PuidLookupResult::Missing,
            [row] => match row.get(PUID_FIELD) {
                Some(Value::Integer(puid)) => executor::PuidLookupResult::Found(*puid),
                _ => executor::PuidLookupResult::Error,
            },
            _ => executor::PuidLookupResult::Error,
        }
    }
}

#[derive(Clone, Debug)]
struct ApplyRedactionFn {
    message: Box<dyn Expression>,
    resolver: PlanResolver,
    // Resolved once at compile time from the literal `enforce_shapes` arg (see `compile`); a static
    // property of the program, so record content cannot disable enforcement.
    enforcement: executor::ShapeEnforcement,
    puid_lookup: Option<PuidLookup>,
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

        let redacted = if self.resolver.requires_puid_lookup() {
            executor::redact_with_puid_lookup(
                handle,
                &bytes,
                self.enforcement,
                executor::KeepMode::Wipe,
                &|user_id| match &self.puid_lookup {
                    Some(lookup) => lookup.find(user_id),
                    None => executor::PuidLookupResult::Error,
                },
            )
        } else {
            executor::redact(handle, &bytes, self.enforcement)
        }
        .map_err(|e| format!("apply_redaction: {e}"))?;

        Ok(Value::Bytes(redacted))
    }

    fn type_def(&self, _: &state::TypeState) -> TypeDef {
        // Returns redacted proto bytes; fallible on a malformed record.
        TypeDef::bytes().fallible()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    use enrichment::{Table, TableRegistry};
    use prost::Message;
    use vector_redaction_executor::plan_proto;

    use super::*;
    use vrl::compiler::CompileConfig;
    use vrl::compiler::function::FunctionCompileContext;
    use vrl::compiler::state::{self, TypeState};
    use vrl::diagnostic::Span;

    static PLAN_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
    const TEST_USER_ID: i64 = 7;
    type RecordedIndexes = Arc<Mutex<Vec<(Case, Vec<String>)>>>;

    fn plan_path(name: &str) -> PathBuf {
        let sequence = PLAN_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "apply_redaction_{name}_{}_{}.pb",
            std::process::id(),
            sequence
        ))
    }

    #[derive(Clone)]
    struct TestPuidTable {
        rows: Result<Vec<ObjectMap>, String>,
        indexes: RecordedIndexes,
    }

    impl TestPuidTable {
        fn new(rows: Vec<ObjectMap>) -> Self {
            Self {
                rows: Ok(rows),
                indexes: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl Table for TestPuidTable {
        fn find_table_row<'a>(
            &self,
            _case: Case,
            _condition: &'a [Condition<'a>],
            _select: Option<&[String]>,
            _wildcard: Option<&Value>,
            _index: Option<IndexHandle>,
        ) -> Result<ObjectMap, String> {
            Err("single-row lookup is not used by apply_redaction".to_owned())
        }

        fn find_table_rows<'a>(
            &self,
            case: Case,
            condition: &'a [Condition<'a>],
            _select: Option<&[String]>,
            _wildcard: Option<&Value>,
            index: Option<IndexHandle>,
        ) -> Result<Vec<ObjectMap>, String> {
            if case != Case::Sensitive || index != Some(IndexHandle(0)) {
                return Err("lookup did not use the registered case-sensitive index".to_owned());
            }
            let queried_user_id = match condition {
                [
                    Condition::Equals {
                        field: USER_ID_FIELD,
                        value: Value::Integer(user_id),
                    },
                ] => *user_id,
                _ => return Err("lookup did not use an exact integer user_id condition".to_owned()),
            };
            let rows = self.rows.clone()?;
            Ok(rows
                .into_iter()
                .filter(|row| {
                    matches!(
                        row.get(USER_ID_FIELD),
                        Some(Value::Integer(user_id)) if *user_id == queried_user_id
                    )
                })
                .collect())
        }

        fn add_index(&mut self, case: Case, fields: &[&str]) -> Result<IndexHandle, String> {
            self.indexes.lock().expect("indexes lock").push((
                case,
                fields.iter().map(|field| (*field).to_owned()).collect(),
            ));
            Ok(IndexHandle(0))
        }

        fn index_fields(&self) -> Vec<(Case, Vec<String>)> {
            self.indexes.lock().expect("indexes lock").clone()
        }

        fn needs_reload(&self) -> bool {
            false
        }
    }

    /// Encodes a two-field proto record on the wire: field 1 and field 2, both varints.
    fn record(field1: u8, field2: u8) -> Vec<u8> {
        // tag = (field_number << 3) | wire_type(0 = varint)
        vec![(1 << 3), field1, (2 << 3), field2]
    }

    fn record_with_unrelated_field(field1: u8, field2: u8, field3: u8) -> Vec<u8> {
        let mut record = record(field1, field2);
        record.extend_from_slice(&[(3u8 << 3), field3]);
        record
    }

    fn encode_int64_field(field_number: u64, value: i64) -> Vec<u8> {
        let mut output = Vec::new();
        encode_varint(field_number << 3, &mut output);
        encode_varint(value as u64, &mut output);
        output
    }

    fn encode_varint(mut value: u64, output: &mut Vec<u8>) {
        while value >= 0x80 {
            output.push((value as u8) | 0x80);
            value >>= 7;
        }
        output.push(value as u8);
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
        let path = plan_path("pass1_drop2");
        let mut f = std::fs::File::create(&path).expect("create temp plan");
        f.write_all(&plan.encode_to_vec()).expect("write temp plan");
        path.to_str().expect("utf-8 temp path").to_owned()
    }

    fn write_puid_plan(target_type: plan_proto::PuidTargetType) -> String {
        use plan_proto::field_action::Action;

        let derived_shapes = (target_type == plan_proto::PuidTargetType::String).then(|| {
            plan_proto::redact_from::DerivedShapes {
                shapes: vec![executor::DataShape::Numeric as i32],
            }
        });
        let plan = plan_proto::RedactionPlanSet {
            plans: vec![plan_proto::MessageRedactionPlan {
                actions: vec![
                    plan_proto::FieldActionEntry {
                        field_number: Some(1),
                        action: Some(plan_proto::FieldAction {
                            action: Some(Action::Remove(plan_proto::Remove {})),
                        }),
                    },
                    plan_proto::FieldActionEntry {
                        field_number: Some(2),
                        action: Some(plan_proto::FieldAction {
                            action: Some(Action::RedactFrom(plan_proto::RedactFrom {
                                source_field_number: Some(1),
                                transform: Some(
                                    plan_proto::RedactFromTransform::GeneratePuid as i32,
                                ),
                                derived_shapes,
                                is_repeated: Some(false),
                                puid_target_type: Some(target_type as i32),
                                ..Default::default()
                            })),
                        }),
                    },
                    plan_proto::FieldActionEntry {
                        field_number: Some(3),
                        action: Some(plan_proto::FieldAction {
                            action: Some(Action::PassThrough(plan_proto::PassThrough {})),
                        }),
                    },
                ],
                ..Default::default()
            }],
        };
        let path = plan_path(&format!("puid_{}", target_type as i32));
        std::fs::write(&path, plan.encode_to_vec()).expect("write PUID plan");
        path.to_str().expect("utf-8 temp path").to_owned()
    }

    fn puid_row_for_user(user_id: i64, puid: Value) -> ObjectMap {
        BTreeMap::from([
            (USER_ID_FIELD.into(), Value::Integer(user_id)),
            (PUID_FIELD.into(), puid),
        ])
    }

    fn puid_row(puid: Value) -> ObjectMap {
        puid_row_for_user(TEST_USER_ID, puid)
    }

    fn registry_with_puid_table(table: TestPuidTable) -> TableRegistry {
        let registry = TableRegistry::default();
        let mut tables: HashMap<String, Box<dyn Table + Send + Sync>> = HashMap::new();
        tables.insert(PUID_MAPPING_TABLE.to_owned(), Box::new(table));
        registry.load(tables);
        registry
    }

    fn lookup_with_rows(rows: Vec<ObjectMap>) -> PuidLookup {
        let mut registry = registry_with_puid_table(TestPuidTable::new(rows));
        let index = registry
            .add_index(PUID_MAPPING_TABLE, Case::Sensitive, &[USER_ID_FIELD])
            .expect("register user_id index");
        let tables = registry.as_readonly();
        registry.finish_load();
        PuidLookup { tables, index }
    }

    fn resolve_expression(expr: Box<dyn Expression>) -> Resolved {
        let mut runtime_state = state::RuntimeState::default();
        let mut object: Value = Value::Object(BTreeMap::new());
        let tz = TimeZone::default();
        let mut ctx = Context::new(&mut object, &mut runtime_state, &tz);
        expr.resolve(&mut ctx)
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

    /// A literal `enforce_shapes: true` compiles through the compile-time literal path and resolves.
    /// The plan here carries no PassThroughString, so On vs Off produce identical output; this only
    /// exercises the literal-arg binding (the enforcement semantics are covered by the executor's
    /// run_enforced tests and the E2E). A non-literal arg would be rejected by optional_literal.
    #[test]
    fn enforce_shapes_literal_compiles_and_resolves() {
        let type_state = TypeState::default();
        let mut cctx = FunctionCompileContext::new(Span::new(0, 0), CompileConfig::default());
        let args = func_args![
            message: Bytes::from(record(7, 9)),
            plan_file: write_pass1_drop2_plan(),
            enforce_shapes: true,
        ];
        let expr = ApplyRedaction
            .compile(&type_state, &mut cctx, args.into())
            .expect("compile should succeed with a literal enforce_shapes");

        let mut runtime_state = state::RuntimeState::default();
        let mut object: Value = Value::Object(BTreeMap::new());
        let tz = TimeZone::default();
        let mut ctx = Context::new(&mut object, &mut runtime_state, &tz);

        let value = expr.resolve(&mut ctx).expect("resolve should succeed");
        assert_eq!(value, Value::Bytes(Bytes::from(vec![(1u8 << 3), 7u8])));
    }

    #[test]
    fn string_puid_plan_fails_to_compile() {
        let mut cctx = FunctionCompileContext::new(Span::new(0, 0), CompileConfig::default());
        let args = func_args![
            message: Bytes::from(record(7, 9)),
            plan_file: write_puid_plan(plan_proto::PuidTargetType::String),
        ];
        let error = ApplyRedaction
            .compile(&TypeState::default(), &mut cctx, args.into())
            .expect_err("Vector must reject string PUID targets")
            .message();

        assert!(
            error.contains("must be INT64"),
            "unexpected diagnostic: {error}"
        );
    }

    #[test]
    fn puid_plan_uses_registered_index_and_preserves_large_integer() {
        let mapped_puid = 9_007_199_254_740_993_i64;
        let table = TestPuidTable::new(vec![puid_row(Value::Integer(mapped_puid))]);
        let indexes = table.indexes.clone();
        let registry = registry_with_puid_table(table);
        let mut config = CompileConfig::default();
        config.set_custom(registry.clone());
        let mut cctx = FunctionCompileContext::new(Span::new(0, 0), config);
        let args = func_args![
            message: Bytes::from(record(7, 9)),
            plan_file: write_puid_plan(plan_proto::PuidTargetType::Int64),
        ];
        let expr = ApplyRedaction
            .compile(&TypeState::default(), &mut cctx, args.into())
            .expect("PUID plan should compile with the table present");

        assert_eq!(
            *indexes.lock().expect("indexes lock"),
            vec![(Case::Sensitive, vec![USER_ID_FIELD.to_owned()])]
        );
        registry.finish_load();

        let value = resolve_expression(expr).expect("PUID redaction should resolve");
        assert_eq!(
            value,
            Value::Bytes(Bytes::from(encode_int64_field(2, mapped_puid)))
        );
    }

    #[test]
    fn puid_plan_without_table_is_nonblocking() {
        let mut cctx = FunctionCompileContext::new(Span::new(0, 0), CompileConfig::default());
        let args = func_args![
            message: Bytes::from(record_with_unrelated_field(7, 9, 11)),
            plan_file: write_puid_plan(plan_proto::PuidTargetType::Int64),
        ];
        let expr = ApplyRedaction
            .compile(&TypeState::default(), &mut cctx, args.into())
            .expect("PUID plan should compile without enrichment tables");

        assert_eq!(
            resolve_expression(expr).expect("missing table should retain the record"),
            Value::Bytes(Bytes::from(vec![(3u8 << 3), 11]))
        );
    }

    #[test]
    fn puid_lookup_classifies_rows_without_coercion() {
        assert_eq!(
            lookup_with_rows(Vec::new()).find(7),
            executor::PuidLookupResult::Missing
        );
        assert_eq!(
            lookup_with_rows(vec![puid_row(Value::Integer(42))]).find(8),
            executor::PuidLookupResult::Missing
        );
        assert_eq!(
            lookup_with_rows(vec![puid_row(Value::Integer(i64::MIN))]).find(7),
            executor::PuidLookupResult::Found(i64::MIN)
        );
        assert_eq!(
            lookup_with_rows(vec![
                puid_row(Value::Integer(1)),
                puid_row(Value::Integer(2)),
            ])
            .find(7),
            executor::PuidLookupResult::Error
        );
        assert_eq!(
            lookup_with_rows(vec![puid_row(Value::Bytes(Bytes::from_static(b"42")))]).find(7),
            executor::PuidLookupResult::Error
        );
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
