//! Shared, opt-in "drain when disabled" gate for the object-storage sources
//! (`aws_s3`, `azure_blob`, `gcp_gcs`).
//!
//! A source that drains a queue dedicated to one logical table can be told to
//! stop processing that table at runtime through a flag enrichment table (for
//! example Lumberjack Prime's `safe_flags`, served by a sidecar). While the
//! table is explicitly disabled, the source still receives notifications but
//! deletes/acknowledges them without fetching or decoding the objects, so a
//! disabled table neither wastes CPU nor builds a backlog.
//!
//! The gate only drains on an explicit "off". Any lookup failure (table not
//! loaded, missing row, non-string value) evaluates to [`GateState::Unknown`],
//! which the sources treat like [`GateState::Enabled`]: process normally.
//!
//! The lookup mirrors the VRL used by the downstream filter
//! (`get_enrichment_table_record(<table>, {"key": <key>}).value`, a
//! comma-separated list matched exactly with `split`/`includes`; booleans are
//! the strings `"true"`/`"false"`), so the gate and the filter agree on which
//! tables are enabled.

use vector_lib::configurable::configurable_component;
use vector_lib::emit;
use vector_lib::enrichment::{Case, Condition, IndexHandle, TableRegistry, TableSearch};
use vrl::value::Value;

use crate::internal_events::ObjectStorageMessagesDrained;

/// Drain this source's queue without fetching objects while its table is disabled
/// in a flag enrichment table.
#[configurable_component]
#[derive(Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DrainWhenDisabledConfig {
    /// Name of the enrichment table holding the flags. Rows are looked up by a `key`
    /// column and must carry a string `value` column.
    #[configurable(metadata(docs::examples = "safe_flags"))]
    pub enrichment_table: String,

    /// Key of the row whose `value` is a comma-separated list of enabled tables.
    #[configurable(metadata(docs::examples = "databricks.logging.lumberjackPrimeEnabledTables"))]
    pub enabled_list_key: String,

    /// The table this source ingests. The source drains while this name is absent
    /// from the enabled list.
    #[configurable(metadata(docs::examples = "span"))]
    pub table: String,

    /// Optional key of a boolean master switch. The source also drains while its
    /// value is exactly `"false"`.
    #[configurable(metadata(docs::examples = "databricks.logging.enableLumberjackPrime"))]
    #[serde(default)]
    pub master_switch_key: Option<String>,
}

/// Outcome of evaluating the gate for one receive batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GateState {
    /// The table is enabled: process normally.
    Enabled,
    /// The table is explicitly disabled: delete/acknowledge without fetching.
    Disabled,
    /// The flags could not be read: process normally.
    Unknown,
}

/// Runtime handle for [`DrainWhenDisabledConfig`].
#[derive(Clone)]
pub struct DrainGate {
    config: DrainWhenDisabledConfig,
    search: TableSearch,
    index: Option<IndexHandle>,
}

impl std::fmt::Debug for DrainGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DrainGate")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl DrainGate {
    /// Builds the gate during source build, while the registry still accepts indexes.
    /// Never fails: if the table is not loaded (or no longer accepts indexes) the gate
    /// falls back to an unindexed lookup, and lookups that fail evaluate to `Unknown`.
    pub fn new(config: &DrainWhenDisabledConfig, registry: &TableRegistry) -> Self {
        let mut registry_for_index = registry.clone();
        let index = registry_for_index
            .add_index(&config.enrichment_table, Case::Sensitive, &["key"])
            .ok();
        Self {
            config: config.clone(),
            search: registry.as_readonly(),
            index,
        }
    }

    /// The table this gate guards.
    pub fn table(&self) -> &str {
        &self.config.table
    }

    /// Evaluates the flags. Called once per receive batch, before fetching objects.
    pub fn state(&self) -> GateState {
        if let Some(key) = &self.config.master_switch_key {
            match self.lookup(key).as_deref() {
                Some("false") => return GateState::Disabled,
                Some("true") => {}
                _ => return GateState::Unknown,
            }
        }
        match self.lookup(&self.config.enabled_list_key) {
            Some(list) if list.split(',').any(|t| t == self.config.table) => GateState::Enabled,
            Some(_) => GateState::Disabled,
            None => GateState::Unknown,
        }
    }

    /// Records `count` messages deleted/acknowledged without fetching their objects.
    pub fn record_drained(&self, count: usize) {
        if count > 0 {
            emit!(ObjectStorageMessagesDrained {
                table: &self.config.table,
                count,
            });
        }
    }

    fn lookup(&self, key: &str) -> Option<String> {
        let condition = [Condition::Equals {
            field: "key",
            value: Value::from(key),
        }];
        let row = self
            .search
            .find_table_row(
                &self.config.enrichment_table,
                Case::Sensitive,
                &condition,
                None,
                None,
                self.index,
            )
            .ok()?;
        match row.get("value")? {
            Value::Bytes(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
            _ => None,
        }
    }
}

/// Builds the optional gate for a source.
pub fn build_drain_gate(
    config: Option<&DrainWhenDisabledConfig>,
    registry: &TableRegistry,
) -> Option<DrainGate> {
    config.map(|config| DrainGate::new(config, registry))
}

#[cfg(test)]
pub(crate) mod test_util {
    use std::collections::{BTreeMap, HashMap};

    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use vector_lib::enrichment::{Case, Condition, IndexHandle, Table, TableRegistry};
    use vrl::value::{ObjectMap, Value};

    /// A minimal in-memory flag table keyed by `key` with a `value` column.
    #[derive(Clone)]
    pub(crate) struct FlagTable(pub BTreeMap<String, Value>);

    impl Table for FlagTable {
        fn find_table_row<'a>(
            &self,
            _case: Case,
            condition: &'a [Condition<'a>],
            _select: Option<&[String]>,
            _wildcard: Option<&Value>,
            _index: Option<IndexHandle>,
        ) -> Result<ObjectMap, String> {
            let key = match condition {
                [
                    Condition::Equals {
                        field: "key",
                        value,
                    },
                ] => value
                    .as_str()
                    .map(|s| s.into_owned())
                    .ok_or_else(|| "non-string key".to_string())?,
                _ => return Err("unsupported condition".to_string()),
            };
            let value = self
                .0
                .get(&key)
                .ok_or_else(|| "no rows found".to_string())?;
            let mut row = ObjectMap::new();
            row.insert("key".into(), Value::from(key));
            row.insert("value".into(), value.clone());
            Ok(row)
        }

        fn find_table_rows<'a>(
            &self,
            case: Case,
            condition: &'a [Condition<'a>],
            select: Option<&[String]>,
            wildcard: Option<&Value>,
            index: Option<IndexHandle>,
        ) -> Result<Vec<ObjectMap>, String> {
            self.find_table_row(case, condition, select, wildcard, index)
                .map(|row| vec![row])
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

    /// A registry with a `safe_flags` table holding `rows`, already past `finish_load`.
    pub(crate) fn registry_with_flags(rows: &[(&str, Value)]) -> TableRegistry {
        let registry = TableRegistry::default();
        let table = FlagTable(
            rows.iter()
                .map(|(k, v)| ((*k).to_string(), v.clone()))
                .collect(),
        );
        let mut tables: HashMap<String, Box<dyn Table + Send + Sync>> = HashMap::new();
        tables.insert("safe_flags".to_string(), Box::new(table));
        registry.load(tables);
        registry
    }

    pub(crate) const ENABLED_LIST_KEY: &str = "databricks.logging.lumberjackPrimeEnabledTables";
    pub(crate) const MASTER_SWITCH_KEY: &str = "databricks.logging.enableLumberjackPrime";

    pub(crate) fn config(table: &str, master: bool) -> super::DrainWhenDisabledConfig {
        super::DrainWhenDisabledConfig {
            enrichment_table: "safe_flags".to_string(),
            enabled_list_key: ENABLED_LIST_KEY.to_string(),
            table: table.to_string(),
            master_switch_key: master.then(|| MASTER_SWITCH_KEY.to_string()),
        }
    }

    /// Builds a gate the way a source does (indexes added before `finish_load`).
    pub(crate) fn gate(table: &str, master: bool, rows: &[(&str, Value)]) -> super::DrainGate {
        let registry = registry_with_flags(rows);
        let gate = super::DrainGate::new(&config(table, master), &registry);
        registry.finish_load();
        gate
    }

    /// Runs `fut` and returns its output with the `lp_blob_source_drained_messages_total`
    /// count it recorded for `table`. The recorder is thread-local, so the future is driven
    /// on this thread; callers need a multi-thread runtime for `block_in_place`.
    pub(crate) fn with_drained_count<F: std::future::Future>(
        table: &str,
        fut: F,
    ) -> (F::Output, u64) {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let output = tokio::task::block_in_place(|| {
            metrics::with_local_recorder(&recorder, || {
                tokio::runtime::Handle::current().block_on(fut)
            })
        });
        let count = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, _, _, _)| {
                key.key().name() == "lp_blob_source_drained_messages_total"
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "table" && label.value() == table)
            })
            .map(|(_, _, _, value)| match value {
                DebugValue::Counter(count) => count,
                _ => 0,
            })
            .sum();
        (output, count)
    }
}

#[cfg(test)]
mod tests {
    use vrl::value::Value;

    use super::GateState;
    use super::test_util::{ENABLED_LIST_KEY, MASTER_SWITCH_KEY, gate};

    #[test]
    fn config_parses_and_rejects_unknown_fields() {
        let config: super::DrainWhenDisabledConfig = toml::from_str(
            r#"
            enrichment_table = "safe_flags"
            enabled_list_key = "databricks.logging.lumberjackPrimeEnabledTables"
            table = "span"
            master_switch_key = "databricks.logging.enableLumberjackPrime"
            "#,
        )
        .expect("config must parse");
        assert_eq!(config, super::test_util::config("span", true));
        assert!(
            toml::from_str::<super::DrainWhenDisabledConfig>(
                "enrichment_table = \"safe_flags\"\nenabled_list_key = \"k\"\ntable = \"span\"\nbogus = 1"
            )
            .is_err()
        );
    }

    #[test]
    fn listed_table_is_enabled() {
        let gate = gate(
            "span",
            false,
            &[(ENABLED_LIST_KEY, Value::from("qpl,span,she"))],
        );
        assert_eq!(gate.state(), GateState::Enabled);
    }

    #[test]
    fn unlisted_table_is_disabled() {
        let gate = gate("span", false, &[(ENABLED_LIST_KEY, Value::from("qpl,she"))]);
        assert_eq!(gate.state(), GateState::Disabled);
    }

    #[test]
    fn empty_list_is_disabled() {
        let gate = gate("span", false, &[(ENABLED_LIST_KEY, Value::from(""))]);
        assert_eq!(gate.state(), GateState::Disabled);
    }

    #[test]
    fn membership_is_exact_like_the_vrl_filter() {
        // `split`/`includes` in the downstream filter neither trims nor prefix-matches.
        let gate = gate(
            "span",
            false,
            &[(ENABLED_LIST_KEY, Value::from("qpl, span,spans"))],
        );
        assert_eq!(gate.state(), GateState::Disabled);
    }

    #[test]
    fn missing_row_is_unknown() {
        let gate = gate("span", false, &[]);
        assert_eq!(gate.state(), GateState::Unknown);
    }

    #[test]
    fn non_string_value_is_unknown() {
        let gate = gate("span", false, &[(ENABLED_LIST_KEY, Value::from(1_i64))]);
        assert_eq!(gate.state(), GateState::Unknown);
    }

    #[test]
    fn table_not_loaded_is_unknown() {
        let registry = vector_lib::enrichment::TableRegistry::default();
        let gate = super::DrainGate::new(&super::test_util::config("span", true), &registry);
        registry.finish_load();
        assert_eq!(gate.state(), GateState::Unknown);
    }

    #[test]
    fn master_switch_false_disables_even_listed_table() {
        let gate = gate(
            "span",
            true,
            &[
                (MASTER_SWITCH_KEY, Value::from("false")),
                (ENABLED_LIST_KEY, Value::from("span")),
            ],
        );
        assert_eq!(gate.state(), GateState::Disabled);
    }

    #[test]
    fn master_switch_true_defers_to_list() {
        let rows = [
            (MASTER_SWITCH_KEY, Value::from("true")),
            (ENABLED_LIST_KEY, Value::from("qpl")),
        ];
        assert_eq!(gate("span", true, &rows).state(), GateState::Disabled);
        assert_eq!(gate("qpl", true, &rows).state(), GateState::Enabled);
    }

    #[test]
    fn master_switch_missing_or_garbage_is_unknown() {
        // Even though the list explicitly omits the table, an unreadable master switch
        // means the flags can't be trusted: process normally.
        let list = (ENABLED_LIST_KEY, Value::from("qpl"));
        assert_eq!(
            gate("span", true, std::slice::from_ref(&list)).state(),
            GateState::Unknown
        );
        assert_eq!(
            gate(
                "span",
                true,
                &[(MASTER_SWITCH_KEY, Value::from("nope")), list]
            )
            .state(),
            GateState::Unknown
        );
    }
}
