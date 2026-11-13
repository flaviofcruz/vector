//! The `apply_redaction` VRL function: redacts a serialized proto record using a compiled plan.
//!
//! `plan_file` is a compile-time literal, so the plan is read and registered once at compile time
//! and only the redaction walk runs per record. Fallible: a bad plan fails compilation, a malformed
//! record fails at runtime.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tracing::warn;
use vector_redaction_executor as executor;

use enrichment::{Case, Condition, IndexHandle, TableRegistry, TableSearch};
use vrl::prelude::*;

use crate::redaction::resolver::{PlanKey, PlanResolver};

const PUID_MAPPING_TABLE: &str = "puid_mapping";
const FEDRAMP_ALLOWLIST_TABLE: &str = "fedramp_allowlist";
const WORKSPACE_ID_FIELD: &str = "workspace_id";
const ACCOUNT_ID_FIELD: &str = "account_id";
const METASTORE_ID_FIELD: &str = "metastore_id";
const DEFAULT_ALLOWLIST_REFRESH_SECS: i64 = 60;
const ALLOWLIST_RETRY_AFTER: Duration = Duration::from_secs(5);
const USER_ID_FIELD: &str = "user_id";
const PUID_FIELD: &str = "puid";

/// Set once a non-empty allowlist snapshot has been pushed. Process-wide like the executor's
/// allowlist it mirrors, so an empty table only clears a snapshot that really existed.
static ALLOWLIST_LOADED: AtomicBool = AtomicBool::new(false);

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

            GENERATE_PUID actions use the indexed `puid_mapping` enrichment table. INT64 targets
            receive native protobuf int64 values; STRING targets receive signed decimal text.
            String targets must declare a shape that accepts decimal text when shape enforcement
            is enabled. A missing mapping removes the source ID and omits the derived PUID.

            `enforce_shapes` is an optional literal boolean (default false) that gates data-shape
            enforcement: when true, `PassThroughString` fields are kept only if they match a declared
            shape; when false or absent, they pass through verbatim. It must be a literal so the
            verdict is a static property of the VRL program, resolved once at compile time and NOT
            influenceable by record content (this gate is the fail-closed boundary). Set to true only
            for LP pipeline plans; the LP splice sets this true, while Governator omits it (defaults
            false) to scope enforcement to the LP write path.

            Opt-in extensions (every one defaults to the behavior above):

            `shape_mode` is an optional literal string, mutually exclusive with `enforce_shapes`:
            \"off\" and \"enforce_all\" equal `enforce_shapes: false` / `true`; \"match_attribution\"
            resolves enforcement per record like classic Lumberjack: the record's in-proto
            workspace/metastore/account id decides, falling back to the `workspace_id`,
            `metastore_id` and `account_id` arguments (envelope ids; integers or strings, null or
            absent = none). The allowlists are a snapshot of the `fedramp_allowlist` enrichment
            table (`workspace_id` integer column; optional `account_id` / `metastore_id` string
            columns), re-read at most every `allowlist_refresh_secs` (literal, default 60). Where
            that table is not registered the mode is inert (no enforcement), like an empty classic
            allowlist, and compiling logs a warning. Until a non-empty snapshot has loaded, a table
            that cannot be read or has no rows leaves the executor enforcing (fail closed).

            `keep_original` is an optional literal boolean (default false = wipe). True keeps the
            non-centralizable fields, like classic Lumberjack outside prod.

            `shard_name` is an optional per-record string passed to the executor for PUID derivation.

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
            Parameter {
                keyword: "shape_mode",
                kind: kind::BYTES,
                required: false,
            },
            Parameter {
                keyword: "keep_original",
                kind: kind::BOOLEAN,
                required: false,
            },
            Parameter {
                keyword: "allowlist_refresh_secs",
                kind: kind::INTEGER,
                required: false,
            },
            Parameter {
                keyword: "workspace_id",
                kind: kind::ANY,
                required: false,
            },
            Parameter {
                keyword: "account_id",
                kind: kind::ANY,
                required: false,
            },
            Parameter {
                keyword: "metastore_id",
                kind: kind::ANY,
                required: false,
            },
            Parameter {
                keyword: "shard_name",
                kind: kind::ANY,
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

        // A literal (not a per-record expression) so the mode is a static property of the VRL
        // program. Under match_attribution the record's attribution ids decide per record; without
        // the allowlist table the mode is Off.
        let compile_error = |message: &str| {
            Box::new(ExpressionError::Error {
                message: format!("apply_redaction: {message}"),
                labels: vec![],
                notes: vec![],
            }) as Box<dyn DiagnosticMessage>
        };
        let enforce_shapes = match arguments.optional_literal("enforce_shapes", state)? {
            Some(value) => Some(
                value
                    .try_boolean()
                    .map_err(|_| compile_error("enforce_shapes must be a boolean"))?,
            ),
            None => None,
        };
        let shape_mode = match arguments.optional_literal("shape_mode", state)? {
            Some(value) => Some(
                value
                    .try_bytes_utf8_lossy()
                    .map_err(|_| compile_error("shape_mode must be a string"))?
                    .into_owned(),
            ),
            None => None,
        };
        let enforcement = match (enforce_shapes, shape_mode.as_deref()) {
            (Some(_), Some(_)) => {
                return Err(compile_error(
                    "enforce_shapes and shape_mode are mutually exclusive",
                ));
            }
            (Some(true), None) | (None, Some("enforce_all")) => executor::ShapeEnforcement::On,
            (Some(false), None) | (None, Some("off")) | (None, None) => {
                executor::ShapeEnforcement::Off
            }
            (None, Some("match_attribution")) => executor::ShapeEnforcement::MatchAttribution,
            (None, Some(other)) => {
                return Err(compile_error(&format!(
                    "shape_mode must be \"off\", \"enforce_all\" or \"match_attribution\", got \"{other}\""
                )));
            }
        };
        let keep_mode = match arguments.optional_literal("keep_original", state)? {
            Some(value) => {
                let keep = value
                    .try_boolean()
                    .map_err(|_| compile_error("keep_original must be a boolean"))?;
                if keep {
                    executor::KeepMode::Keep
                } else {
                    executor::KeepMode::Wipe
                }
            }
            None => executor::KeepMode::Wipe,
        };
        let refresh_interval = match arguments.optional_literal("allowlist_refresh_secs", state)? {
            Some(value) => {
                let secs = value
                    .try_integer()
                    .map_err(|_| compile_error("allowlist_refresh_secs must be an integer"))?;
                if secs < 1 {
                    return Err(compile_error("allowlist_refresh_secs must be at least 1"));
                }
                secs
            }
            None => DEFAULT_ALLOWLIST_REFRESH_SECS,
        };
        let workspace_id = arguments.optional("workspace_id");
        let account_id = arguments.optional("account_id");
        let metastore_id = arguments.optional("metastore_id");
        let shard_name = arguments.optional("shard_name");

        // Only match_attribution consults the allowlist. Without the table the mode is inert.
        let allowlist = if enforcement == executor::ShapeEnforcement::MatchAttribution {
            match ctx.get_external_context_mut::<TableRegistry>() {
                Some(registry)
                    if registry
                        .table_ids()
                        .iter()
                        .any(|table| table == FEDRAMP_ALLOWLIST_TABLE) =>
                {
                    Some(AllowlistRefresher::new(
                        registry.as_readonly(),
                        Duration::from_secs(refresh_interval.unsigned_abs()),
                    ))
                }
                _ => {
                    warn!(
                        message = "apply_redaction: match_attribution has no fedramp_allowlist \
                                   table, so shape enforcement is off for this program.",
                        table = FEDRAMP_ALLOWLIST_TABLE,
                    );
                    None
                }
            }
        } else {
            None
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
            keep_mode,
            allowlist,
            workspace_id,
            account_id,
            metastore_id,
            shard_name,
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

/// Keeps the executor's process-wide attribution allowlists current from the `fedramp_allowlist`
/// enrichment table. Clones share one refresh deadline, so concurrent records trigger at most one
/// scan per interval.
#[derive(Clone, Debug)]
struct AllowlistRefresher {
    tables: TableSearch,
    interval: Duration,
    started: Instant,
    next_refresh_ms: Arc<AtomicU64>,
}

impl AllowlistRefresher {
    fn new(tables: TableSearch, interval: Duration) -> Self {
        Self {
            tables,
            interval,
            started: Instant::now(),
            next_refresh_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    fn refresh_if_due(&self) {
        let now = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let due = self.next_refresh_ms.load(Ordering::Acquire);
        if now < due {
            return;
        }
        // Exactly one caller wins the deadline and performs the scan.
        let provisional = now.saturating_add(ALLOWLIST_RETRY_AFTER.as_millis() as u64);
        if self
            .next_refresh_ms
            .compare_exchange(due, provisional, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        match self.push_snapshot() {
            Ok(()) => {
                let next = now.saturating_add(self.interval.as_millis() as u64);
                self.next_refresh_ms.store(next, Ordering::Release);
            }
            Err(error) => {
                // Keep the previous snapshot (or the executor's fail-closed default) and retry soon.
                warn!(message = "apply_redaction: could not refresh the shape-enforcement allowlist.", %error);
            }
        }
    }

    /// Pushes the table as the executor's allowlist. Zero rows before a non-empty snapshot has
    /// loaded are an unloaded table (an HTTP table whose first fetch failed starts empty), so they
    /// fail the push and the executor keeps its fail-closed default. Rows with no usable id are
    /// skipped and counted; if no row has one, the push fails for the same reason.
    fn push_snapshot(&self) -> Result<(), String> {
        let rows = self
            .tables
            .find_table_rows(
                FEDRAMP_ALLOWLIST_TABLE,
                Case::Sensitive,
                &[],
                None,
                None,
                None,
            )
            .map_err(|error| error.to_string())?;
        let mut workspaces = Vec::with_capacity(rows.len());
        let mut accounts = Vec::new();
        let mut metastores = Vec::new();
        let mut unusable = 0_usize;
        for row in &rows {
            let workspace = match row.get(WORKSPACE_ID_FIELD) {
                Some(Value::Integer(id)) => Some(*id),
                Some(Value::Bytes(text)) => String::from_utf8_lossy(text).parse::<i64>().ok(),
                _ => None,
            };
            let account = row.get(ACCOUNT_ID_FIELD).and_then(text_id);
            let metastore = row.get(METASTORE_ID_FIELD).and_then(text_id);
            if workspace.is_none() && account.is_none() && metastore.is_none() {
                unusable += 1;
            }
            // The executor keys workspaces by the two's-complement bit pattern.
            workspaces.extend(workspace.map(|id| id as u64));
            accounts.extend(account);
            metastores.extend(metastore);
        }
        if unusable > 0 {
            warn!(
                message = "apply_redaction: skipped fedramp_allowlist rows with no usable id.",
                skipped = unusable,
                rows = rows.len(),
            );
        }
        let usable = rows.len() - unusable;
        if rows.is_empty() {
            if !ALLOWLIST_LOADED.load(Ordering::Acquire) {
                return Err(format!(
                    "{FEDRAMP_ALLOWLIST_TABLE} has no rows yet; keeping the fail-closed default"
                ));
            }
            warn!(
                message = "apply_redaction: fedramp_allowlist is empty; match_attribution \
                           enforces no workspace, account or metastore until it has rows.",
                table = FEDRAMP_ALLOWLIST_TABLE,
            );
        } else if usable == 0 {
            return Err(format!(
                "{FEDRAMP_ALLOWLIST_TABLE} has {} rows but none has a usable id",
                rows.len()
            ));
        }
        executor::update_shape_enforcement_allowlists(&workspaces, &accounts, &metastores)
            .map_err(|error| error.to_string())?;
        if usable > 0 {
            ALLOWLIST_LOADED.store(true, Ordering::Release);
        }
        Ok(())
    }
}

/// An allowlist id column as text; empty text is no id.
fn text_id(value: &Value) -> Option<String> {
    match value {
        Value::Bytes(bytes) => {
            let text = String::from_utf8_lossy(bytes).into_owned();
            (!text.is_empty()).then_some(text)
        }
        _ => None,
    }
}

/// Reads an optional per-record id expression as text: integers render in decimal, bytes as UTF-8,
/// anything else (including null) is absent.
fn optional_text(
    expression: Option<&dyn Expression>,
    ctx: &mut Context,
) -> Result<Option<String>, ExpressionError> {
    let Some(expression) = expression else {
        return Ok(None);
    };
    Ok(match expression.resolve(ctx)? {
        Value::Integer(id) => Some(id.to_string()),
        Value::Bytes(text) => {
            let text = String::from_utf8_lossy(&text).into_owned();
            (!text.is_empty()).then_some(text)
        }
        _ => None,
    })
}

#[derive(Clone, Debug)]
struct ApplyRedactionFn {
    message: Box<dyn Expression>,
    resolver: PlanResolver,
    // Resolved once at compile time from the literal `enforce_shapes` / `shape_mode` args (see
    // `compile`). Under match_attribution the record's attribution ids decide per record.
    enforcement: executor::ShapeEnforcement,
    keep_mode: executor::KeepMode,
    // Some only for match_attribution where the allowlist table is registered.
    allowlist: Option<AllowlistRefresher>,
    workspace_id: Option<Box<dyn Expression>>,
    account_id: Option<Box<dyn Expression>>,
    metastore_id: Option<Box<dyn Expression>>,
    shard_name: Option<Box<dyn Expression>>,
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

        // match_attribution without the allowlist table is inert, like an empty classic allowlist.
        let enforcement = match (&self.allowlist, self.enforcement) {
            (Some(allowlist), executor::ShapeEnforcement::MatchAttribution) => {
                allowlist.refresh_if_due();
                executor::ShapeEnforcement::MatchAttribution
            }
            (None, executor::ShapeEnforcement::MatchAttribution) => executor::ShapeEnforcement::Off,
            (_, fixed) => fixed,
        };

        let workspace = optional_text(self.workspace_id.as_deref(), ctx)?;
        let account = optional_text(self.account_id.as_deref(), ctx)?;
        let metastore = optional_text(self.metastore_id.as_deref(), ctx)?;
        let shard = optional_text(self.shard_name.as_deref(), ctx)?;
        let context = executor::RedactContext {
            shard_name: shard.as_deref(),
            workspace_fallback: workspace.as_deref(),
            account_fallback: account.as_deref(),
            metastore_fallback: metastore.as_deref(),
        };

        let redacted = if self.resolver.requires_puid_lookup() {
            executor::redact_with_puid_lookup_and_context(
                handle,
                &bytes,
                enforcement,
                self.keep_mode,
                &context,
                &|user_id| match &self.puid_lookup {
                    Some(lookup) => lookup.find(user_id),
                    None => executor::PuidLookupResult::Error,
                },
            )
        } else {
            executor::redact_with_context(handle, &bytes, enforcement, self.keep_mode, &context)
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
        write_puid_plan_with_string_shapes(target_type, vec![executor::DataShape::Numeric as i32])
    }

    fn write_puid_plan_with_string_shapes(
        target_type: plan_proto::PuidTargetType,
        string_shapes: Vec<i32>,
    ) -> String {
        use plan_proto::field_action::Action;

        let derived_shapes = (target_type == plan_proto::PuidTargetType::String).then_some(
            plan_proto::redact_from::DerivedShapes {
                shapes: string_shapes,
            },
        );
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
        resolve_ref(expr.as_ref())
    }

    /// Resolves without consuming the expression, so one compiled program can run several records.
    fn resolve_ref(expr: &dyn Expression) -> Resolved {
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
    fn string_puid_plan_uses_index_and_preserves_signed_decimal_values() {
        for mapped_puid in [0, 9_007_199_254_740_993, i64::MIN, i64::MAX] {
            let table = TestPuidTable::new(vec![puid_row(Value::Integer(mapped_puid))]);
            let indexes = table.indexes.clone();
            let registry = registry_with_puid_table(table);
            let mut config = CompileConfig::default();
            config.set_custom(registry.clone());
            let mut cctx = FunctionCompileContext::new(Span::new(0, 0), config);
            let args = func_args![
                message: Bytes::from(record(7, 9)),
                plan_file: write_puid_plan(plan_proto::PuidTargetType::String),
                enforce_shapes: true,
            ];
            let expr = ApplyRedaction
                .compile(&TypeState::default(), &mut cctx, args.into())
                .expect("string PUID plan should compile");

            assert_eq!(
                *indexes.lock().expect("indexes lock"),
                vec![(Case::Sensitive, vec![USER_ID_FIELD.to_owned()])]
            );
            registry.finish_load();

            let expected = mapped_puid.to_string();
            let mut wire = vec![(2u8 << 3) | 2, expected.len() as u8];
            wire.extend_from_slice(expected.as_bytes());
            assert_eq!(
                resolve_expression(expr).expect("string PUID redaction should resolve"),
                Value::Bytes(Bytes::from(wire))
            );
        }
    }

    #[test]
    fn string_puid_plan_preserves_shape_enforcement() {
        let mapped_puid = i64::MIN;
        let hex = "0000000000000000000000000000000000000000000000008000000000000000";
        let numeric = "-9223372036854775808";
        for (shapes, expected) in [
            (vec![executor::DataShape::Hex64 as i32], Some(hex)),
            (vec![executor::DataShape::Numeric as i32], Some(numeric)),
            (
                vec![
                    executor::DataShape::Hex64 as i32,
                    executor::DataShape::Numeric as i32,
                ],
                Some(numeric),
            ),
            (
                vec![
                    executor::DataShape::Numeric as i32,
                    executor::DataShape::Hex64 as i32,
                ],
                Some(numeric),
            ),
            (vec![executor::DataShape::Uuid as i32], None),
            (vec![], None),
        ] {
            let registry = registry_with_puid_table(TestPuidTable::new(vec![puid_row(
                Value::Integer(mapped_puid),
            )]));
            let mut config = CompileConfig::default();
            config.set_custom(registry.clone());
            let mut cctx = FunctionCompileContext::new(Span::new(0, 0), config);
            let args = func_args![
                message: Bytes::from(record_with_unrelated_field(7, 9, 11)),
                plan_file: write_puid_plan_with_string_shapes(
                    plan_proto::PuidTargetType::String, shapes
                ),
                enforce_shapes: true,
            ];
            let expr = ApplyRedaction
                .compile(&TypeState::default(), &mut cctx, args.into())
                .expect("string plan should compile regardless of shape compatibility");
            registry.finish_load();

            let mut wire = Vec::new();
            if let Some(expected) = expected {
                wire.extend_from_slice(&[(2u8 << 3) | 2, expected.len() as u8]);
                wire.extend_from_slice(expected.as_bytes());
            }
            wire.extend_from_slice(&[(3u8 << 3), 11]);
            assert_eq!(
                resolve_expression(expr).expect("shape mismatch should not drop the record"),
                Value::Bytes(Bytes::from(wire))
            );
        }
    }

    #[test]
    fn string_puid_miss_and_lookup_error_preserve_unrelated_fields() {
        for rows in [Ok(Vec::new()), Err("lookup unavailable".to_owned())] {
            let mut table = TestPuidTable::new(Vec::new());
            table.rows = rows;
            let registry = registry_with_puid_table(table);
            let mut config = CompileConfig::default();
            config.set_custom(registry.clone());
            let mut cctx = FunctionCompileContext::new(Span::new(0, 0), config);
            let args = func_args![
                message: Bytes::from(record_with_unrelated_field(7, 9, 11)),
                plan_file: write_puid_plan(plan_proto::PuidTargetType::String),
                enforce_shapes: true,
            ];
            let expr = ApplyRedaction
                .compile(&TypeState::default(), &mut cctx, args.into())
                .expect("string PUID plan should compile");
            registry.finish_load();
            assert_eq!(
                resolve_expression(expr).expect("lookup failure should retain the record"),
                Value::Bytes(Bytes::from(vec![(3u8 << 3), 11]))
            );
        }
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

    #[derive(Clone)]
    struct TestAllowlistTable {
        rows: Arc<Mutex<Vec<ObjectMap>>>,
    }

    impl Table for TestAllowlistTable {
        fn find_table_row<'a>(
            &self,
            _case: Case,
            _condition: &'a [Condition<'a>],
            _select: Option<&[String]>,
            _wildcard: Option<&Value>,
            _index: Option<IndexHandle>,
        ) -> Result<ObjectMap, String> {
            Err("single-row lookup is not used".to_owned())
        }

        fn find_table_rows<'a>(
            &self,
            _case: Case,
            condition: &'a [Condition<'a>],
            _select: Option<&[String]>,
            _wildcard: Option<&Value>,
            _index: Option<IndexHandle>,
        ) -> Result<Vec<ObjectMap>, String> {
            assert!(
                condition.is_empty(),
                "the snapshot scan must be unconditional"
            );
            Ok(self.rows.lock().expect("rows lock").clone())
        }

        fn add_index(&mut self, _case: Case, _fields: &[&str]) -> Result<IndexHandle, String> {
            Ok(IndexHandle(0))
        }

        fn index_fields(&self) -> Vec<(Case, Vec<String>)> {
            Vec::new()
        }

        fn needs_reload(&self) -> bool {
            false
        }
    }

    fn workspace_rows(workspaces: &[i64]) -> Vec<ObjectMap> {
        workspaces
            .iter()
            .map(|id| BTreeMap::from([(WORKSPACE_ID_FIELD.into(), Value::Integer(*id))]))
            .collect()
    }

    fn registry_with_rows(rows: Vec<ObjectMap>) -> TableRegistry {
        shared_registry(Arc::new(Mutex::new(rows)))
    }

    /// Shares the rows so a test can change what the table returns between refreshes.
    fn shared_registry(rows: Arc<Mutex<Vec<ObjectMap>>>) -> TableRegistry {
        let registry = TableRegistry::default();
        let mut tables: HashMap<String, Box<dyn Table + Send + Sync>> = HashMap::new();
        tables.insert(
            FEDRAMP_ALLOWLIST_TABLE.to_owned(),
            Box::new(TestAllowlistTable { rows }),
        );
        registry.load(tables);
        registry
    }

    /// Plan with one shape-gated `PassThroughString` (field 4, no allowed shapes: dropped when the
    /// record is enforced, kept otherwise).
    fn write_shape_gated_plan() -> String {
        let plan = plan_proto::RedactionPlanSet {
            plans: vec![plan_proto::MessageRedactionPlan {
                actions: vec![plan_proto::FieldActionEntry {
                    field_number: Some(4),
                    action: Some(plan_proto::FieldAction {
                        action: Some(plan_proto::field_action::Action::PassThroughString(
                            plan_proto::PassThroughString::default(),
                        )),
                    }),
                }],
                ..Default::default()
            }],
        };
        let path = plan_path("shape_gated");
        std::fs::write(&path, plan.encode_to_vec()).expect("write plan");
        path.to_str().expect("utf-8 temp path").to_owned()
    }

    fn shaped_record() -> Vec<u8> {
        vec![(4u8 << 3) | 2, 4, b't', b'e', b'x', b't']
    }

    fn compile_with_registry(
        registry: Option<&TableRegistry>,
        args: ArgumentList,
    ) -> Box<dyn Expression> {
        let mut config = CompileConfig::default();
        if let Some(registry) = registry {
            config.set_custom(registry.clone());
        }
        let mut cctx = FunctionCompileContext::new(Span::new(0, 0), config);
        let expr = ApplyRedaction
            .compile(&TypeState::default(), &mut cctx, args)
            .expect("compile should succeed");
        if let Some(registry) = registry {
            registry.finish_load();
        }
        expr
    }

    /// One test (not several) because the executor's allowlist is process-wide: parallel
    /// match_attribution tests would overwrite each other's snapshot. Order matters: the executor
    /// starts unset, so the first push must be an empty or unusable table, not a real snapshot.
    #[test]
    fn match_attribution_resolves_envelope_ids_against_the_table_snapshot() {
        // A registry can be finished (loaded) only once, so each run gets a fresh one.
        let run = |rows: Option<Vec<ObjectMap>>, workspace: Value| {
            let registry = rows.map(registry_with_rows);
            let args = func_args![
                message: Bytes::from(shaped_record()),
                plan_file: write_shape_gated_plan(),
                shape_mode: "match_attribution",
                workspace_id: workspace,
            ];
            resolve_expression(compile_with_registry(registry.as_ref(), args.into()))
                .expect("redaction should resolve")
        };
        let enforced = Value::Bytes(Bytes::new());
        let kept = Value::Bytes(Bytes::from(shaped_record()));
        let unusable_row = || -> Vec<ObjectMap> {
            vec![BTreeMap::from([(
                WORKSPACE_ID_FIELD.into(),
                Value::from("not-an-id"),
            )])]
        };

        // Without the table the mode is inert.
        assert_eq!(run(None, Value::Integer(4242)), kept);

        // Until a non-empty snapshot loads, an empty table fails closed, as does a table
        // whose rows have no usable id.
        assert_eq!(run(Some(vec![]), Value::Integer(4242)), enforced);
        assert_eq!(run(Some(unusable_row()), Value::Integer(4242)), enforced);

        let listed = || workspace_rows(&[4242, -5]);
        assert_eq!(run(Some(listed()), Value::Integer(4242)), enforced);
        assert_eq!(run(Some(listed()), Value::from("4242")), enforced);
        assert_eq!(run(Some(listed()), Value::Integer(-5)), enforced);
        assert_eq!(run(Some(listed()), Value::Integer(7)), kept);
        assert_eq!(run(Some(listed()), Value::Null), kept);

        // Once a snapshot has loaded, a table with only unusable rows keeps it (the push fails).
        assert_eq!(run(Some(unusable_row()), Value::Integer(4242)), enforced);

        // Then an empty table clears it, like an empty classic allowlist.
        assert_eq!(run(Some(vec![]), Value::Integer(4242)), kept);

        // A non-positive refresh interval is rejected at compile time.
        let zero_refresh = func_args![
            message: Bytes::from(shaped_record()),
            plan_file: write_shape_gated_plan(),
            shape_mode: "match_attribution",
            allowlist_refresh_secs: 0_i64,
        ];
        let mut cctx = FunctionCompileContext::new(Span::new(0, 0), CompileConfig::default());
        let compiled =
            ApplyRedaction.compile(&TypeState::default(), &mut cctx, zero_refresh.into());
        assert!(
            compiled.is_err(),
            "allowlist_refresh_secs: 0 must not compile"
        );

        // Refresh is throttled to allowlist_refresh_secs: a table change is seen only after the
        // interval elapses. Kept last in this test because it leaves a populated allowlist.
        let rows = Arc::new(Mutex::new(workspace_rows(&[4242])));
        let registry = shared_registry(rows.clone());
        let args = func_args![
            message: Bytes::from(shaped_record()),
            plan_file: write_shape_gated_plan(),
            shape_mode: "match_attribution",
            workspace_id: Value::Integer(4242),
            allowlist_refresh_secs: 1_i64,
        ];
        let expr = compile_with_registry(Some(&registry), args.into());
        assert_eq!(resolve_ref(expr.as_ref()).expect("redact"), enforced);
        *rows.lock().expect("rows lock") = workspace_rows(&[7]);
        assert_eq!(resolve_ref(expr.as_ref()).expect("redact"), enforced);
        std::thread::sleep(Duration::from_millis(1100));
        assert_eq!(resolve_ref(expr.as_ref()).expect("redact"), kept);
    }

    #[test]
    fn shape_mode_is_exclusive_with_enforce_shapes_and_validated() {
        let compile = |args: ArgumentList| {
            let mut cctx = FunctionCompileContext::new(Span::new(0, 0), CompileConfig::default());
            ApplyRedaction
                .compile(&TypeState::default(), &mut cctx, args)
                .map(|_| ())
                .map_err(|e| e.message())
        };
        let both = func_args![
            message: Bytes::from(record(7, 9)),
            plan_file: write_pass1_drop2_plan(),
            enforce_shapes: true,
            shape_mode: "enforce_all",
        ];
        assert!(
            compile(both.into())
                .expect_err("both args")
                .contains("mutually exclusive")
        );
        let bad = func_args![
            message: Bytes::from(record(7, 9)),
            plan_file: write_pass1_drop2_plan(),
            shape_mode: "sometimes",
        ];
        assert!(
            compile(bad.into())
                .expect_err("bad mode")
                .contains("shape_mode must be")
        );
        for mode in ["off", "enforce_all"] {
            let ok = func_args![
                message: Bytes::from(record(7, 9)),
                plan_file: write_pass1_drop2_plan(),
                shape_mode: mode,
            ];
            compile(ok.into()).expect("fixed modes compile");
        }
    }

    #[test]
    fn enforce_all_mode_matches_enforce_shapes_true() {
        for args in [
            func_args![
                message: Bytes::from(shaped_record()),
                plan_file: write_shape_gated_plan(),
                enforce_shapes: true,
            ],
            func_args![
                message: Bytes::from(shaped_record()),
                plan_file: write_shape_gated_plan(),
                shape_mode: "enforce_all",
            ],
        ] {
            assert_eq!(
                resolve_expression(compile_with_registry(None, args.into())).expect("resolve"),
                Value::Bytes(Bytes::new())
            );
        }
    }

    /// Field 1 is `mode_switch { wipe: Remove, keep: PassThrough }`.
    fn write_mode_switch_plan() -> String {
        use plan_proto::field_action::Action;

        let wipe = plan_proto::FieldAction {
            action: Some(Action::Remove(plan_proto::Remove {})),
        };
        let keep = plan_proto::FieldAction {
            action: Some(Action::PassThrough(plan_proto::PassThrough {})),
        };
        let mode_switch = plan_proto::ModeSwitch {
            wipe: Some(Box::new(wipe)),
            keep: Some(Box::new(keep)),
        };
        let plan = plan_proto::RedactionPlanSet {
            plans: vec![plan_proto::MessageRedactionPlan {
                actions: vec![plan_proto::FieldActionEntry {
                    field_number: Some(1),
                    action: Some(plan_proto::FieldAction {
                        action: Some(Action::ModeSwitch(Box::new(mode_switch))),
                    }),
                }],
                ..Default::default()
            }],
        };
        let path = plan_path("mode_switch");
        std::fs::write(&path, plan.encode_to_vec()).expect("write plan");
        path.to_str().expect("utf-8 temp path").to_owned()
    }

    #[test]
    fn keep_original_defaults_to_wipe_and_opts_in_to_keep() {
        let resolve = |keep: Option<bool>| {
            let mut args = func_args![
                message: Bytes::from(record(7, 9)),
                plan_file: write_mode_switch_plan(),
            ];
            if let Some(keep) = keep {
                args.insert("keep_original", keep.into());
            }
            resolve_expression(compile_with_registry(None, args.into())).expect("resolve")
        };
        let wiped = Value::Bytes(Bytes::new());
        let kept = Value::Bytes(Bytes::from(vec![(1u8 << 3), 7u8]));
        assert_eq!(resolve(None), wiped);
        assert_eq!(resolve(Some(false)), wiped);
        assert_eq!(resolve(Some(true)), kept);
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
