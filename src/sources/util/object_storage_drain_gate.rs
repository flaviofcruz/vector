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
//! A source can also be paused through [`PauseWhenListedConfig`]: while its table is listed
//! in a pause flag, it stops receiving altogether, so queued messages are neither fetched
//! nor deleted and wait for the table to be unpaused (or for the queue's retention to
//! expire). [`ObjectStorageGate`] applies both options with one precedence (pause, then
//! drain) and checks pause again on each received batch, so a pause set while a receive is
//! in flight leaves that batch on the queue. Pause fails closed when the flag is served but
//! unreadable (non-string value, empty or unreadable table), so a bad value or a flag
//! outage can only build a backlog, never delete. A flag missing from a table that serves
//! other flags lists nothing and pauses nothing; it is logged as an error. A pause gate
//! whose enrichment table isn't configured fails the source build.
//!
//! The lookup mirrors the VRL used by the downstream filter
//! (`get_enrichment_table_record(<table>, {"key": <key>}).value`, a
//! comma-separated list matched exactly with `split`/`includes`; booleans are
//! the strings `"true"`/`"false"`), so the gate and the filter agree on which
//! tables are enabled.

use vector_lib::configurable::configurable_component;
use vector_lib::emit;
use vector_lib::enrichment::{Case, Condition, IndexHandle, TableRegistry, TableSearch};
use vrl::value::{ObjectMap, Value};

use std::time::Duration;

use crate::internal_events::{
    ObjectStorageMessagesDrained, ObjectStorageSourcePauseState, ObjectStorageSourcePaused,
};

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

/// Stop receiving from this source's queue while its table is listed in a pause flag.
#[configurable_component]
#[derive(Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PauseWhenListedConfig {
    /// Name of the enrichment table holding the flags. Rows are looked up by a `key`
    /// column and must carry a string `value` column.
    #[configurable(metadata(docs::examples = "safe_flags"))]
    pub enrichment_table: String,

    /// Key of the row whose `value` is a comma-separated list of paused tables.
    #[configurable(metadata(
        docs::examples = "databricks.logging.lumberjackPrimeBlobIngestPausedTables"
    ))]
    pub paused_list_key: String,

    /// The table this source ingests. The source pauses while this name is in the
    /// paused list, or while the served list can't be read.
    #[configurable(metadata(docs::examples = "span"))]
    pub table: String,
}

/// Outcome of evaluating the drain flags for one receive batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GateState {
    /// The table is enabled: process normally.
    Enabled,
    /// The table is explicitly disabled: delete/acknowledge without fetching.
    Disabled,
    /// The flags could not be read: process normally.
    Unknown,
}

/// One flag lookup.
#[derive(Debug, PartialEq, Eq)]
enum FlagRead {
    /// The row's string `value`.
    Value(String),
    /// The row exists but its `value` is not a string.
    NonString,
    /// The table serves other rows but none for the key: the flag is not being served.
    Missing,
    /// The table can't be read, is empty, or has several rows for the key.
    Unavailable,
}

/// Reads string values by `key` from a flag enrichment table.
#[derive(Clone)]
struct FlagLookup {
    enrichment_table: String,
    search: TableSearch,
    index: Option<IndexHandle>,
}

impl FlagLookup {
    /// Built during source build, while the registry still accepts indexes. Never fails:
    /// if the table is not loaded (or no longer accepts indexes) it falls back to an
    /// unindexed lookup, and lookups that fail return `None`.
    fn new(enrichment_table: &str, registry: &TableRegistry) -> Self {
        let mut registry_for_index = registry.clone();
        let index = registry_for_index
            .add_index(enrichment_table, Case::Sensitive, &["key"])
            .ok();
        Self {
            enrichment_table: enrichment_table.to_string(),
            search: registry.as_readonly(),
            index,
        }
    }

    /// Rows for `key`, or `None` when the table can't be read.
    fn rows(&self, key: &str) -> Option<Vec<ObjectMap>> {
        let condition = [Condition::Equals {
            field: "key",
            value: Value::from(key),
        }];
        self.search
            .find_table_rows(
                &self.enrichment_table,
                Case::Sensitive,
                &condition,
                None,
                None,
                self.index,
            )
            .ok()
    }

    /// Classifies the lookup of `key`, telling a key the table doesn't serve apart from a
    /// table that serves nothing.
    fn read(&self, key: &str) -> FlagRead {
        let Some(mut rows) = self.rows(key) else {
            return FlagRead::Unavailable;
        };
        if rows.is_empty() {
            // An empty table is not serving anything yet (the http table starts empty when its
            // first fetch fails), so only a table that serves other rows proves the key absent.
            // The key is read again after that check, because a refresh may have filled the
            // table in between.
            if !self.has_rows() {
                return FlagRead::Unavailable;
            }
            match self.rows(key) {
                Some(again) if again.is_empty() => return FlagRead::Missing,
                Some(again) => rows = again,
                None => return FlagRead::Unavailable,
            }
        }
        match rows.as_slice() {
            [row] => string_value(row).map_or(FlagRead::NonString, FlagRead::Value),
            _ => FlagRead::Unavailable,
        }
    }

    fn has_rows(&self) -> bool {
        // Selecting no columns keeps the scan from copying any values.
        self.search
            .find_table_rows(
                &self.enrichment_table,
                Case::Sensitive,
                &[],
                Some(&[]),
                None,
                None,
            )
            .is_ok_and(|rows| !rows.is_empty())
    }

    /// The string `value` for `key`, or `None` for any lookup that doesn't yield exactly one.
    fn get(&self, key: &str) -> Option<String> {
        match self.rows(key)?.as_slice() {
            [row] => string_value(row),
            _ => None,
        }
    }
}

fn string_value(row: &ObjectMap) -> Option<String> {
    match row.get("value")? {
        Value::Bytes(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
        _ => None,
    }
}

/// Runtime handle for [`DrainWhenDisabledConfig`].
#[derive(Clone)]
pub struct DrainGate {
    config: DrainWhenDisabledConfig,
    flags: FlagLookup,
}

impl std::fmt::Debug for DrainGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DrainGate")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl DrainGate {
    /// Builds the gate during source build. Lookups that fail evaluate to `Unknown`.
    pub fn new(config: &DrainWhenDisabledConfig, registry: &TableRegistry) -> Self {
        Self {
            config: config.clone(),
            flags: FlagLookup::new(&config.enrichment_table, registry),
        }
    }

    /// Evaluates the flags.
    pub fn state(&self) -> GateState {
        if let Some(key) = &self.config.master_switch_key {
            match self.flags.get(key).as_deref() {
                Some("false") => return GateState::Disabled,
                Some("true") => {}
                _ => return GateState::Unknown,
            }
        }
        match self.flags.get(&self.config.enabled_list_key) {
            Some(list) if list.split(',').any(|t| t == self.config.table) => GateState::Enabled,
            Some(_) => GateState::Disabled,
            None => GateState::Unknown,
        }
    }

    /// Records `count` messages deleted/acknowledged without fetching their objects.
    fn record_drained(&self, count: usize) {
        if count > 0 {
            emit!(ObjectStorageMessagesDrained {
                table: &self.config.table,
                count,
            });
        }
    }
}

/// Why a [`PauseGate`] is skipping polls.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PauseReason {
    /// The table is in the pause list.
    Listed,
    /// The pause list is served but couldn't be read, so the source fails closed.
    FlagUnavailable,
}

impl PauseReason {
    /// The `reason` label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Listed => "listed",
            Self::FlagUnavailable => "flag_unavailable",
        }
    }
}

/// Runtime handle for [`PauseWhenListedConfig`].
#[derive(Clone)]
pub struct PauseGate {
    config: PauseWhenListedConfig,
    flags: FlagLookup,
    /// Floor on the wait between skipped polls, so a paused source never spins.
    min_wait: Duration,
}

impl std::fmt::Debug for PauseGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PauseGate")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl PauseGate {
    /// Builds the gate during source build. Errors if `enrichment_table` isn't configured,
    /// since every lookup would then fail and keep the source paused for good.
    pub fn new(config: &PauseWhenListedConfig, registry: &TableRegistry) -> crate::Result<Self> {
        if !registry.table_ids().contains(&config.enrichment_table) {
            return Err(format!(
                "pause_when_listed: enrichment table {:?} is not configured",
                config.enrichment_table
            )
            .into());
        }
        Ok(Self {
            config: config.clone(),
            flags: FlagLookup::new(&config.enrichment_table, registry),
            min_wait: Duration::from_secs(1),
        })
    }

    /// Why the source should skip this poll, or `None` to poll. Also publishes the result
    /// as the `lp_blob_source_paused` gauge.
    pub fn pause_reason(&self) -> Option<PauseReason> {
        let reason = match self.flags.read(&self.config.paused_list_key) {
            FlagRead::Value(list) => list
                .split(',')
                .any(|t| t == self.config.table)
                .then_some(PauseReason::Listed),
            // The table serves other flags but not this one (for example the sidecar's flag
            // list doesn't carry it yet), so no table can be listed. Pausing would stall
            // ingestion with nothing to undo it.
            FlagRead::Missing => {
                error!(
                    message = "Pause flag is not served by the enrichment table; not pausing.",
                    table = %self.config.table,
                    enrichment_table = %self.config.enrichment_table,
                    key = %self.config.paused_list_key,
                    internal_log_rate_limit = true,
                );
                None
            }
            FlagRead::NonString | FlagRead::Unavailable => {
                error!(
                    message = "Pause flag can't be read; pausing until it can.",
                    table = %self.config.table,
                    enrichment_table = %self.config.enrichment_table,
                    key = %self.config.paused_list_key,
                    internal_log_rate_limit = true,
                );
                Some(PauseReason::FlagUnavailable)
            }
        };
        emit!(ObjectStorageSourcePauseState {
            table: &self.config.table,
            paused: reason.is_some(),
        });
        reason
    }

    /// Records one skipped poll and waits out the poll interval.
    async fn wait(&self, reason: PauseReason, poll_secs: u64) {
        emit!(ObjectStorageSourcePaused {
            table: &self.config.table,
            reason: reason.as_str(),
        });
        tokio::time::sleep(Duration::from_secs(poll_secs).max(self.min_wait)).await;
    }
}

/// What a source does with a received batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GateDecision {
    /// Fetch and process the batch.
    Process,
    /// Delete/acknowledge the batch without fetching its objects: the table is disabled.
    Drain,
    /// Leave the batch on the queue: the table is paused.
    Pause(PauseReason),
}

/// A source's `pause_when_listed` and `drain_when_disabled`, evaluated together so every
/// source applies one precedence: pause, then drain.
#[derive(Clone, Debug)]
pub struct ObjectStorageGate {
    drain: Option<DrainGate>,
    pause: Option<PauseGate>,
}

impl ObjectStorageGate {
    /// Builds the gate during source build; `None` when neither option is set.
    pub fn build(
        drain: Option<&DrainWhenDisabledConfig>,
        pause: Option<&PauseWhenListedConfig>,
        registry: &TableRegistry,
    ) -> crate::Result<Option<Self>> {
        let pause = pause
            .map(|config| PauseGate::new(config, registry))
            .transpose()?;
        let drain = drain.map(|config| DrainGate::new(config, registry));
        Ok(Self::new(drain, pause))
    }

    fn new(drain: Option<DrainGate>, pause: Option<PauseGate>) -> Option<Self> {
        (drain.is_some() || pause.is_some()).then_some(Self { drain, pause })
    }

    /// Checked before every receive. While the table is paused, records the skipped poll,
    /// waits out the poll interval and returns `true`, so the caller skips the receive.
    pub async fn skip_paused_poll(&self, poll_secs: u64) -> bool {
        let Some(pause) = &self.pause else {
            return false;
        };
        match pause.pause_reason() {
            Some(reason) => {
                pause.wait(reason, poll_secs).await;
                true
            }
            None => false,
        }
    }

    /// Decides a received batch of `count` messages. Pause is checked again because it may
    /// have been set while the receive was in flight. A paused batch is neither deleted nor
    /// acknowledged, so the queue redelivers it once its visibility timeout or ack deadline
    /// expires.
    pub fn decide_batch(&self, count: usize) -> GateDecision {
        if let Some(pause) = &self.pause
            && let Some(reason) = pause.pause_reason()
        {
            info!(
                message = "Table was paused while receiving; leaving the batch on the queue.",
                table = %pause.config.table,
                count,
                reason = reason.as_str(),
                internal_log_rate_limit = true,
            );
            return GateDecision::Pause(reason);
        }
        match &self.drain {
            Some(drain) if drain.state() == GateState::Disabled => GateDecision::Drain,
            _ => GateDecision::Process,
        }
    }

    /// Records `count` messages deleted/acknowledged without fetching their objects.
    pub fn record_drained(&self, count: usize) {
        if let Some(drain) = &self.drain {
            drain.record_drained(count);
        }
    }
}

#[cfg(test)]
pub(crate) mod test_util {
    use std::collections::{BTreeMap, HashMap, VecDeque};
    use std::sync::{Arc, Mutex};

    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use vector_lib::enrichment::{Case, Condition, IndexHandle, Table, TableRegistry};
    use vrl::value::{ObjectMap, Value};

    /// A minimal in-memory flag table keyed by `key` with a `value` column. A key with
    /// several queued values answers them in order, repeating the last one, so a test can
    /// change a flag between two reads; a queued `None` answers as no row.
    #[derive(Clone)]
    pub(crate) struct FlagTable(Arc<Mutex<BTreeMap<String, VecDeque<Option<Value>>>>>);

    impl FlagTable {
        fn value(&self, key: &str) -> Option<Value> {
            let mut rows = self.0.lock().unwrap();
            let values = rows.get_mut(key)?;
            if values.len() > 1 {
                values.pop_front().flatten()
            } else {
                values.front().cloned().flatten()
            }
        }
    }

    fn equals_key<'a>(condition: &'a [Condition<'a>]) -> Result<String, String> {
        match condition {
            [
                Condition::Equals {
                    field: "key",
                    value,
                },
            ] => value
                .as_str()
                .map(|s| s.into_owned())
                .ok_or_else(|| "non-string key".to_string()),
            _ => Err("unsupported condition".to_string()),
        }
    }

    fn row(key: String, value: Value) -> ObjectMap {
        let mut row = ObjectMap::new();
        row.insert("key".into(), Value::from(key));
        row.insert("value".into(), value);
        row
    }

    impl Table for FlagTable {
        fn find_table_row<'a>(
            &self,
            _case: Case,
            condition: &'a [Condition<'a>],
            _select: Option<&[String]>,
            _wildcard: Option<&Value>,
            _index: Option<IndexHandle>,
        ) -> Result<ObjectMap, String> {
            let key = equals_key(condition)?;
            let value = self
                .value(&key)
                .ok_or_else(|| "no rows found".to_string())?;
            Ok(row(key, value))
        }

        /// Like the file and http tables, an unmatched condition is an empty result.
        fn find_table_rows<'a>(
            &self,
            _case: Case,
            condition: &'a [Condition<'a>],
            _select: Option<&[String]>,
            _wildcard: Option<&Value>,
            _index: Option<IndexHandle>,
        ) -> Result<Vec<ObjectMap>, String> {
            if condition.is_empty() {
                let rows = self.0.lock().unwrap();
                return Ok(rows
                    .iter()
                    .filter_map(|(key, values)| Some(row(key.clone(), values.front()?.clone()?)))
                    .collect());
            }
            let key = equals_key(condition)?;
            Ok(self
                .value(&key)
                .map(|value| row(key, value))
                .into_iter()
                .collect())
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

    /// A registry with a `safe_flags` table holding `rows`, still accepting indexes.
    fn loading_registry(rows: &[(&str, Vec<Option<Value>>)]) -> TableRegistry {
        let registry = TableRegistry::default();
        let table = FlagTable(Arc::new(Mutex::new(
            rows.iter()
                .map(|(k, v)| ((*k).to_string(), v.iter().cloned().collect()))
                .collect(),
        )));
        let mut tables: HashMap<String, Box<dyn Table + Send + Sync>> = HashMap::new();
        tables.insert("safe_flags".to_string(), Box::new(table));
        registry.load(tables);
        registry
    }

    fn single<'a>(rows: &[(&'a str, Value)]) -> Vec<(&'a str, Vec<Option<Value>>)> {
        rows.iter()
            .map(|(k, v)| (*k, vec![Some(v.clone())]))
            .collect()
    }

    pub(crate) const ENABLED_LIST_KEY: &str = "databricks.logging.lumberjackPrimeEnabledTables";
    pub(crate) const MASTER_SWITCH_KEY: &str = "databricks.logging.enableLumberjackPrime";
    pub(crate) const PAUSED_LIST_KEY: &str =
        "databricks.logging.lumberjackPrimeBlobIngestPausedTables";

    pub(crate) fn config(table: &str, master: bool) -> super::DrainWhenDisabledConfig {
        super::DrainWhenDisabledConfig {
            enrichment_table: "safe_flags".to_string(),
            enabled_list_key: ENABLED_LIST_KEY.to_string(),
            table: table.to_string(),
            master_switch_key: master.then(|| MASTER_SWITCH_KEY.to_string()),
        }
    }

    pub(crate) fn pause_config(table: &str) -> super::PauseWhenListedConfig {
        super::PauseWhenListedConfig {
            enrichment_table: "safe_flags".to_string(),
            paused_list_key: PAUSED_LIST_KEY.to_string(),
            table: table.to_string(),
        }
    }

    /// Builds a gate the way a source does (indexes added before `finish_load`).
    pub(crate) fn gate(table: &str, master: bool, rows: &[(&str, Value)]) -> super::DrainGate {
        let registry = loading_registry(&single(rows));
        let gate = super::DrainGate::new(&config(table, master), &registry);
        registry.finish_load();
        gate
    }

    /// Builds a pause gate the way a source does, without the one-second wait floor.
    pub(crate) fn pause_gate(table: &str, rows: &[(&str, Value)]) -> super::PauseGate {
        pause_gate_with_rows(table, &single(rows))
    }

    /// [`pause_gate`] over keys whose values change on each read (see [`FlagTable`]).
    pub(crate) fn pause_gate_sequence(
        table: &str,
        rows: &[(&str, Vec<Value>)],
    ) -> super::PauseGate {
        let rows: Vec<_> = rows
            .iter()
            .map(|(k, values)| (*k, values.iter().cloned().map(Some).collect()))
            .collect();
        pause_gate_with_rows(table, &rows)
    }

    /// [`pause_gate`] over raw queued rows, where `None` answers as no row.
    pub(crate) fn pause_gate_with_rows(
        table: &str,
        rows: &[(&str, Vec<Option<Value>>)],
    ) -> super::PauseGate {
        let registry = loading_registry(rows);
        let mut gate =
            super::PauseGate::new(&pause_config(table), &registry).expect("safe_flags is loaded");
        registry.finish_load();
        gate.min_wait = std::time::Duration::ZERO;
        gate
    }

    /// The gate a source holds for these parts.
    pub(crate) fn object_gate(
        drain: Option<super::DrainGate>,
        pause: Option<super::PauseGate>,
    ) -> Option<super::ObjectStorageGate> {
        super::ObjectStorageGate::new(drain, pause)
    }

    /// Runs `f` and returns its output with every `name` metric it recorded, as (labels,
    /// value) pairs. The recorder is thread-local, so `f` must run on this thread.
    pub(crate) fn record<T>(
        name: &str,
        f: impl FnOnce() -> T,
    ) -> (T, Vec<(Vec<(String, String)>, DebugValue)>) {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let output = metrics::with_local_recorder(&recorder, f);
        let recorded = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, _, _, _)| key.key().name() == name)
            .map(|(key, _, _, value)| {
                let labels = key
                    .key()
                    .labels()
                    .map(|l| (l.key().to_string(), l.value().to_string()))
                    .collect();
                (labels, value)
            })
            .collect();
        (output, recorded)
    }

    /// [`with_counter`] for `lp_blob_source_drained_messages_total`.
    pub(crate) fn with_drained_count<F: std::future::Future>(
        table: &str,
        fut: F,
    ) -> (F::Output, u64) {
        with_counter("lp_blob_source_drained_messages_total", table, fut)
    }

    /// [`with_counter`] for `lp_blob_source_paused_polls_total`.
    pub(crate) fn with_paused_count<F: std::future::Future>(
        table: &str,
        fut: F,
    ) -> (F::Output, u64) {
        with_counter("lp_blob_source_paused_polls_total", table, fut)
    }

    /// Drives `fut` on this thread under [`record`] and sums the `name` counter for `table`.
    /// Callers need a multi-thread runtime for `block_in_place`.
    fn with_counter<F: std::future::Future>(name: &str, table: &str, fut: F) -> (F::Output, u64) {
        let (output, recorded) = record(name, || {
            tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(fut))
        });
        let count = recorded
            .into_iter()
            .filter(|(labels, _)| labels.iter().any(|(k, v)| k == "table" && v == table))
            .map(|(_, value)| match value {
                DebugValue::Counter(count) => count,
                _ => 0,
            })
            .sum();
        (output, count)
    }
}

#[cfg(test)]
mod tests {
    use metrics_util::debugging::DebugValue;
    use vrl::value::Value;

    use super::test_util::{
        ENABLED_LIST_KEY, MASTER_SWITCH_KEY, PAUSED_LIST_KEY, gate, object_gate, pause_config,
        pause_gate, pause_gate_sequence, record,
    };
    use super::{GateDecision, GateState, PauseReason};

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

    #[test]
    fn listed_table_is_paused() {
        let rows = [(PAUSED_LIST_KEY, Value::from("qpl,span"))];
        assert_eq!(
            pause_gate("span", &rows).pause_reason(),
            Some(PauseReason::Listed)
        );
        assert_eq!(pause_gate("dbr", &rows).pause_reason(), None);
    }

    #[test]
    fn empty_paused_list_pauses_nothing() {
        assert_eq!(
            pause_gate("span", &[(PAUSED_LIST_KEY, Value::from(""))]).pause_reason(),
            None
        );
    }

    #[test]
    fn paused_membership_is_exact() {
        let rows = [(PAUSED_LIST_KEY, Value::from("span_v2, span"))];
        assert_eq!(pause_gate("span", &rows).pause_reason(), None);
    }

    #[test]
    fn unserved_pause_flag_does_not_pause() {
        // The table serves other flags but not this one, so nothing can be listed: pausing
        // would stall ingestion until someone noticed.
        let (reason, gauge) = record("lp_blob_source_paused", || {
            pause_gate("span", &[(ENABLED_LIST_KEY, Value::from("span"))]).pause_reason()
        });
        assert_eq!(reason, None);
        assert_eq!(
            gauge,
            vec![(
                vec![("table".to_string(), "span".to_string())],
                DebugValue::Gauge(0.0.into())
            )]
        );
    }

    #[test]
    fn unreadable_pause_flag_fails_closed() {
        // A served flag that can't be parsed may hide a pause, so the source stays paused: a
        // bad value may build a backlog but must never let the source delete.
        assert_eq!(
            pause_gate("span", &[(PAUSED_LIST_KEY, Value::from(1))]).pause_reason(),
            Some(PauseReason::FlagUnavailable)
        );
    }

    #[test]
    fn empty_flag_table_fails_closed() {
        // The http table starts empty when its first fetch fails: that is an outage, not
        // proof that the flag is unset.
        assert_eq!(
            pause_gate("span", &[]).pause_reason(),
            Some(PauseReason::FlagUnavailable)
        );
    }

    #[test]
    fn key_filled_in_between_reads_is_read_again() {
        // The first read misses the key (as on an empty table mid-refresh); by the emptiness
        // check the table has rows, and the key must be read again rather than called unset.
        let gate = super::test_util::pause_gate_with_rows(
            "span",
            &[
                (ENABLED_LIST_KEY, vec![Some(Value::from("span"))]),
                (PAUSED_LIST_KEY, vec![None, Some(Value::from("span"))]),
            ],
        );
        assert_eq!(gate.pause_reason(), Some(PauseReason::Listed));
    }

    #[test]
    fn unconfigured_pause_table_fails_the_build() {
        let registry = vector_lib::enrichment::TableRegistry::default();
        let error = super::ObjectStorageGate::build(None, Some(&pause_config("span")), &registry)
            .expect_err("a misnamed table would keep the source paused for good");
        assert!(error.to_string().contains("\"safe_flags\""), "{error}");
        // The drain gate fails open, so it still builds without the table.
        let drain = super::test_util::config("span", false);
        assert!(
            super::ObjectStorageGate::build(Some(&drain), None, &registry)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn neither_option_builds_no_gate() {
        let registry = vector_lib::enrichment::TableRegistry::default();
        assert!(
            super::ObjectStorageGate::build(None, None, &registry)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn batch_decision_prefers_pause_then_drain() {
        let disabled = || gate("span", false, &[(ENABLED_LIST_KEY, Value::from("qpl"))]);
        let paused = |list: &str| pause_gate("span", &[(PAUSED_LIST_KEY, Value::from(list))]);
        let decide = |gate: Option<super::ObjectStorageGate>| gate.unwrap().decide_batch(1);
        assert_eq!(
            decide(object_gate(Some(disabled()), Some(paused("span")))),
            GateDecision::Pause(PauseReason::Listed)
        );
        assert_eq!(
            decide(object_gate(Some(disabled()), Some(paused("qpl")))),
            GateDecision::Drain
        );
        assert_eq!(
            decide(object_gate(None, Some(paused("qpl")))),
            GateDecision::Process
        );
    }

    #[tokio::test]
    async fn pause_set_during_receive_holds_the_batch() {
        // Unpaused when the poll starts, paused by the time the batch arrives; the table is
        // also disabled, which must not drain the batch.
        let pause = pause_gate_sequence(
            "span",
            &[(PAUSED_LIST_KEY, vec![Value::from(""), Value::from("span")])],
        );
        let disabled = gate("span", false, &[(ENABLED_LIST_KEY, Value::from("qpl"))]);
        let gate = object_gate(Some(disabled), Some(pause)).unwrap();
        assert!(!gate.skip_paused_poll(0).await);
        assert_eq!(
            gate.decide_batch(3),
            GateDecision::Pause(PauseReason::Listed)
        );
    }

    #[test]
    fn pause_state_gauge_tracks_each_check() {
        let table = vec![("table".to_string(), "span".to_string())];
        let gauge = |rows: &[(&str, Value)]| {
            let gate = pause_gate("span", rows);
            record("lp_blob_source_paused", || gate.pause_reason()).1
        };
        let set_to = |value: f64| vec![(table.clone(), DebugValue::Gauge(value.into()))];
        assert_eq!(
            gauge(&[(PAUSED_LIST_KEY, Value::from("span"))]),
            set_to(1.0)
        );
        assert_eq!(gauge(&[(PAUSED_LIST_KEY, Value::from("qpl"))]), set_to(0.0));
        // Failing closed is still paused, so autoscaling ignores the backlog it builds.
        assert_eq!(gauge(&[(PAUSED_LIST_KEY, Value::from(1))]), set_to(1.0));
    }

    #[test]
    fn paused_poll_counter_carries_reason() {
        for (reason, label) in [
            (PauseReason::Listed, "listed"),
            (PauseReason::FlagUnavailable, "flag_unavailable"),
        ] {
            let ((), counted) = record("lp_blob_source_paused_polls_total", || {
                vector_lib::emit!(crate::internal_events::ObjectStorageSourcePaused {
                    table: "span",
                    reason: reason.as_str(),
                });
            });
            assert_eq!(
                counted,
                vec![(
                    vec![
                        ("table".to_string(), "span".to_string()),
                        ("reason".to_string(), label.to_string()),
                    ],
                    DebugValue::Counter(1),
                )]
            );
        }
    }

    #[test]
    fn pause_config_parses_and_rejects_unknown_fields() {
        let config: super::PauseWhenListedConfig = toml::from_str(
            r#"
            enrichment_table = "safe_flags"
            paused_list_key = "databricks.logging.lumberjackPrimeBlobIngestPausedTables"
            table = "span"
            "#,
        )
        .expect("valid config parses");
        assert_eq!(config.table, "span");
        assert!(
            toml::from_str::<super::PauseWhenListedConfig>(
                r#"
                enrichment_table = "safe_flags"
                paused_list_key = "k"
                table = "span"
                bogus = true
                "#,
            )
            .is_err()
        );
    }
}
