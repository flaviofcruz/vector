//! One encoded payload per destination for each remote-write request.
//! Samples remain individual rows; this does not merge rows or change sink batching/buffers.

use std::{collections::BTreeMap, fmt::Write as _, sync::Arc};

use super::variant::{map_array_to_variant, variant_struct_fields};
use arrow::{
    array::{
        ArrayRef, AsArray, Float64Builder, Int64Builder, LargeStringBuilder, ListBuilder,
        MapBuilder, MapFieldNames, RecordBatch, StructBuilder, TimestampMicrosecondBuilder,
        UInt32Array,
    },
    compute::take_record_batch,
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    ipc::writer::StreamWriter,
};
use bytes::Bytes;
use vector_lib::configurable::configurable_component;
use vrl::value::Value;

use md5::Digest as _;

use super::{CATCH_ALL_SYSTEM, DropReason, MetricToLogHydra, PodRoutingConfig, TOPIC, accept};
use crate::event::LogEvent;

/// How one pod port's rows are encoded in encoded output.
#[configurable_component]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OutputEncoding {
    /// Newline-separated JSON rows, for an S3 sink with `encoding.codec: text` and
    /// `framing.method: newline_delimited`.
    Ndjson,
    /// An Arrow IPC stream, for a Zerobus sink with `batch_encoding.codec: arrow_ipc`. `labels`
    /// is a string map.
    ArrowIpc,
    /// As `arrow_ipc`, with `labels` already encoded as a VARIANT column (for a table whose
    /// `labels` is VARIANT). Encoded once per array for all such ports, instead of once per sink.
    ArrowIpcVariant,
}

impl OutputEncoding {
    const fn is_arrow(self) -> bool {
        matches!(self, Self::ArrowIpc | Self::ArrowIpcVariant)
    }
}

/// One output port of a compiled output plan.
struct Port {
    name: String,
    format: OutputEncoding,
}

/// `outputs` compiled against the routing table: ports by index, and each routing rule's
/// pods as port indices.
pub(super) struct OutputPlan {
    ports: Vec<Port>,
    /// Port indices for a `system` label with no rule of its own (the `"*"` rule).
    default: Vec<usize>,
    /// Port indices per `system` label (the config's `system`, without the `system:` prefix).
    by_system: std::collections::HashMap<String, Vec<usize>>,
    schema: SchemaRef,
    /// [`arrow_schema`] with `labels` as VARIANT.
    variant_schema: SchemaRef,
}

impl OutputPlan {
    pub(super) fn compile(
        cfg: &BTreeMap<String, OutputEncoding>,
        routing: &PodRoutingConfig,
    ) -> crate::Result<Self> {
        // Reuse the stable transform's routing validation without exposing its implementation.
        MetricToLogHydra::with_pod_routing(routing)?;
        let mut pods: Vec<&str> = routing
            .systems
            .iter()
            .flat_map(|r| r.pods.iter().map(String::as_str))
            .collect();
        pods.sort_unstable();
        pods.dedup();
        for pod in &pods {
            if !cfg.contains_key(*pod) {
                return Err(
                    format!("hydra_remote_write outputs: pod `{pod}` has no encoding").into(),
                );
            }
        }
        for name in cfg.keys() {
            if !pods.contains(&name.as_str()) {
                return Err(
                    format!("hydra_remote_write outputs: `{name}` is not in pod_routing").into(),
                );
            }
        }
        let ports: Vec<Port> = cfg
            .iter()
            .map(|(name, format)| Port {
                name: name.clone(),
                format: *format,
            })
            .collect();
        let index = |pods: &[String]| -> Vec<usize> {
            pods.iter()
                .map(|p| {
                    ports
                        .iter()
                        .position(|port| &port.name == p)
                        .expect("checked above")
                })
                .collect()
        };
        let default = index(
            &routing
                .systems
                .iter()
                .find(|r| r.system == CATCH_ALL_SYSTEM)
                .expect("validated catch-all")
                .pods,
        );
        let by_system = routing
            .systems
            .iter()
            .filter(|r| r.system != CATCH_ALL_SYSTEM)
            .map(|r| (r.system.clone(), index(&r.pods)))
            .collect();
        Ok(Self {
            ports,
            default,
            by_system,
            schema: arrow_schema(),
            variant_schema: variant_schema(),
        })
    }

    /// Port indices for a row's `system` label (empty = no label = the `"*"` rule).
    #[inline]
    fn decide(&self, system_label: &str) -> &[usize] {
        if system_label.is_empty() {
            return &self.default;
        }
        self.by_system
            .get(system_label)
            .map(Vec::as_slice)
            .unwrap_or(&self.default)
    }
}

/// The Arrow schema of `arrow_ipc` records: one column per field of the per-sample event, typed
/// as the Zerobus SDK types the matching Unity Catalog column (STRING -> LargeUtf8, BIGINT ->
/// Int64, TIMESTAMP -> Timestamp(us, UTC)). The sink's codec conforms names and nullability.
pub(super) fn arrow_schema() -> SchemaRef {
    let ts = || DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let s = |name: &str, nullable: bool| Field::new(name, DataType::LargeUtf8, nullable);
    let i = |name: &str| Field::new(name, DataType::Int64, false);
    Arc::new(Schema::new(vec![
        Field::new("_event_time", ts(), false),
        Field::new("_timestamp_hour", ts(), false),
        s("_partition_date", false),
        s("_system", false),
        s("_shard_name", false),
        s("name", false),
        Field::new("labels", labels_type(), false),
        i("start_ts"),
        s("metric_name", false),
        i("timestamp_hour_ms"),
        i("metric_part"),
        s("md5_labels", false),
        s("metric_type", false),
        Field::new("values", values_type(), false),
        s("workspace_id", true),
        s("cluster_id", true),
        s("region_uri", true),
        s("tenant_id", true),
        s("hydra_cluster_column", false),
        s("tenant_cluster_column", true),
        s("shard_name", false),
        s("system_uri", true),
        s("topic", false),
    ]))
}

/// Index of `labels` in [`arrow_schema`].
const LABELS_COLUMN: usize = 6;

/// [`arrow_schema`] with `labels` as a VARIANT struct carrying the `arrow.parquet.variant` marker.
pub(super) fn variant_schema() -> SchemaRef {
    let base = arrow_schema();
    let mut fields: Vec<Field> = base.fields().iter().map(|f| f.as_ref().clone()).collect();
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(
        "ARROW:extension:name".to_string(),
        "arrow.parquet.variant".to_string(),
    );
    fields[LABELS_COLUMN] = Field::new("labels", DataType::Struct(variant_struct_fields()), false)
        .with_metadata(metadata);
    Arc::new(Schema::new(fields))
}

fn map_field_names() -> MapFieldNames {
    MapFieldNames {
        entry: "entries".into(),
        key: "keys".into(),
        value: "values".into(),
    }
}

fn labels_type() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("keys", DataType::LargeUtf8, false),
                    Field::new("values", DataType::LargeUtf8, true),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

fn value_fields() -> Vec<Field> {
    vec![
        Field::new("ts", DataType::Int64, true),
        Field::new("v", DataType::Float64, true),
    ]
}

fn values_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(value_fields().into()),
        true,
    )))
}

/// Column builders for the rows bound for any `arrow_ipc` port, in [`arrow_schema`] order.
struct ArrowRows {
    event_time: TimestampMicrosecondBuilder,
    timestamp_hour: TimestampMicrosecondBuilder,
    partition_date: LargeStringBuilder,
    system: LargeStringBuilder,
    shard_name_: LargeStringBuilder,
    name: LargeStringBuilder,
    labels: MapBuilder<LargeStringBuilder, LargeStringBuilder>,
    start_ts: Int64Builder,
    metric_name: LargeStringBuilder,
    timestamp_hour_ms: Int64Builder,
    metric_part: Int64Builder,
    md5_labels: LargeStringBuilder,
    metric_type: LargeStringBuilder,
    values: ListBuilder<StructBuilder>,
    workspace_id: LargeStringBuilder,
    cluster_id: LargeStringBuilder,
    region_uri: LargeStringBuilder,
    tenant_id: LargeStringBuilder,
    hydra_cluster_column: LargeStringBuilder,
    tenant_cluster_column: LargeStringBuilder,
    shard_name: LargeStringBuilder,
    system_uri: LargeStringBuilder,
    topic: LargeStringBuilder,
    rows: usize,
}

impl ArrowRows {
    fn new() -> Self {
        let s = LargeStringBuilder::new;
        let values_item = Arc::new(Field::new(
            "item",
            DataType::Struct(value_fields().into()),
            true,
        ));
        Self {
            event_time: TimestampMicrosecondBuilder::new(),
            timestamp_hour: TimestampMicrosecondBuilder::new(),
            partition_date: s(),
            system: s(),
            shard_name_: s(),
            name: s(),
            labels: MapBuilder::new(
                Some(map_field_names()),
                LargeStringBuilder::new(),
                LargeStringBuilder::new(),
            )
            .with_keys_field(Field::new("keys", DataType::LargeUtf8, false))
            .with_values_field(Field::new("values", DataType::LargeUtf8, true)),
            start_ts: Int64Builder::new(),
            metric_name: s(),
            timestamp_hour_ms: Int64Builder::new(),
            metric_part: Int64Builder::new(),
            md5_labels: s(),
            metric_type: s(),
            values: ListBuilder::new(StructBuilder::from_fields(value_fields(), 0))
                .with_field(values_item),
            workspace_id: s(),
            cluster_id: s(),
            region_uri: s(),
            tenant_id: s(),
            hydra_cluster_column: s(),
            tenant_cluster_column: s(),
            shard_name: s(),
            system_uri: s(),
            topic: s(),
            rows: 0,
        }
    }

    fn push(&mut self, row: &RowRef<'_>, p: &Promoted<'_>, partition_date: &str) {
        self.event_time.append_value(row.timestamp_ms * 1000);
        self.timestamp_hour
            .append_value(row.timestamp_hour_ms * 1000);
        self.partition_date.append_value(partition_date);
        self.system.append_value(p.system);
        self.shard_name_.append_value(p.shard_name);
        self.name.append_value(row.name);
        for (k, v) in row.labels {
            self.labels.keys().append_value(k);
            self.labels.values().append_value(v);
        }
        self.labels
            .append(true)
            .expect("keys and values appended in pairs");
        self.start_ts.append_value(row.timestamp_ms);
        self.metric_name.append_value(row.metric_name());
        self.timestamp_hour_ms.append_value(row.timestamp_hour_ms);
        self.metric_part.append_value(row.part);
        self.md5_labels.append_value(row.md5_labels);
        self.metric_type.append_value(row.metric_type);
        let item = self.values.values();
        item.field_builder::<Int64Builder>(0)
            .expect("ts")
            .append_value(row.timestamp_ms);
        item.field_builder::<Float64Builder>(1)
            .expect("v")
            .append_value(row.value);
        item.append(true);
        self.values.append(true);
        self.workspace_id.append_option(p.workspace_id);
        self.cluster_id.append_option(p.cluster_id);
        self.region_uri.append_option(p.region_uri);
        self.tenant_id.append_option(p.tenant_id);
        // `{name}|{system}|{shardName}`, written straight into the builder.
        let _ = write!(
            self.hydra_cluster_column,
            "{}|{}|{}",
            row.name, p.system, p.shard_name
        );
        self.hydra_cluster_column.append_value("");
        self.tenant_cluster_column
            .append_option(p.tenant_cluster_column);
        self.shard_name.append_value(p.shard_name);
        match p.system {
            "" => self.system_uri.append_null(),
            system => {
                let _ = write!(self.system_uri, "system:{system}");
                self.system_uri.append_value("");
            }
        }
        self.topic.append_value(TOPIC);
        self.rows += 1;
    }

    fn finish(mut self, schema: &SchemaRef) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.event_time.finish().with_timezone("UTC")),
            Arc::new(self.timestamp_hour.finish().with_timezone("UTC")),
            Arc::new(self.partition_date.finish()),
            Arc::new(self.system.finish()),
            Arc::new(self.shard_name_.finish()),
            Arc::new(self.name.finish()),
            Arc::new(self.labels.finish()),
            Arc::new(self.start_ts.finish()),
            Arc::new(self.metric_name.finish()),
            Arc::new(self.timestamp_hour_ms.finish()),
            Arc::new(self.metric_part.finish()),
            Arc::new(self.md5_labels.finish()),
            Arc::new(self.metric_type.finish()),
            Arc::new(self.values.finish()),
            Arc::new(self.workspace_id.finish()),
            Arc::new(self.cluster_id.finish()),
            Arc::new(self.region_uri.finish()),
            Arc::new(self.tenant_id.finish()),
            Arc::new(self.hydra_cluster_column.finish()),
            Arc::new(self.tenant_cluster_column.finish()),
            Arc::new(self.shard_name.finish()),
            Arc::new(self.system_uri.finish()),
            Arc::new(self.topic.finish()),
        ];
        RecordBatch::try_new(Arc::clone(schema), columns)
            .expect("builders match arrow_schema by construction")
    }
}

fn ipc_bytes(batch: &RecordBatch) -> Bytes {
    let mut writer = StreamWriter::try_new(
        Vec::with_capacity(batch.get_array_memory_size() + 4096),
        &batch.schema(),
    )
    .expect("in-memory IPC writer");
    writer.write(batch).expect("in-memory IPC write");
    writer.finish().expect("in-memory IPC finish");
    Bytes::from(writer.into_inner().expect("in-memory IPC writer"))
}

/// One accepted sample and its derived values, borrowed from the decoded request.
pub(super) struct RowRef<'a> {
    name: &'a str,
    /// The tags, key-sorted and unique, as `MetricTags::iter_single()` yields them.
    labels: &'a [(&'a str, &'a str)],
    timestamp_ms: i64,
    timestamp_hour_ms: i64,
    /// The finite scalar.
    value: f64,
    metric_name_len: usize,
    metric_type: &'static str,
    part: i64,
    md5_labels: &'a str,
}

impl<'a> RowRef<'a> {
    /// A tag's value: `MetricTags::get` on the key-sorted pairs.
    #[inline]
    fn tag(&self, key: &str) -> Option<&'a str> {
        self.labels
            .binary_search_by(|(k, _)| (*k).cmp(key))
            .ok()
            .map(|i| self.labels[i].1)
    }

    #[inline]
    fn metric_name(&self) -> &'a str {
        &self.name[..self.metric_name_len]
    }

    fn event_time(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp_millis(self.timestamp_ms).unwrap_or_default()
    }

    fn timestamp_hour(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp_millis(self.timestamp_hour_ms).unwrap_or_default()
    }
}

/// A row's promoted tags, looked up once.
pub(super) struct Promoted<'a> {
    system: &'a str,
    shard_name: &'a str,
    workspace_id: Option<&'a str>,
    cluster_id: Option<&'a str>,
    tenant_id: Option<&'a str>,
    region_uri: Option<&'a str>,
    tenant_cluster_column: Option<&'a str>,
}

impl<'a> Promoted<'a> {
    fn of(row: &RowRef<'a>) -> Self {
        let cluster_id = row.tag("cluster_id");
        let tenant_id = row.tag("tenant_id");
        let workspace_id = row.tag("workspace_id");
        Self {
            system: row.tag("system").unwrap_or_default(),
            shard_name: row.tag("shardName").unwrap_or_default(),
            workspace_id,
            cluster_id,
            tenant_id,
            region_uri: row.tag("region_uri"),
            // First of cluster_id, endpoint_name, tenant_id, workspace_id (as the per-sample transform).
            tenant_cluster_column: cluster_id
                .or_else(|| row.tag("endpoint_name"))
                .or(tenant_id)
                .or(workspace_id),
        }
    }
}

#[derive(serde::Serialize)]
struct SerdeRow<'a> {
    _event_time: &'a str,
    _partition_date: &'a str,
    _shard_name: &'a str,
    _system: &'a str,
    _timestamp_hour: &'a str,
    cluster_id: Option<&'a str>,
    hydra_cluster_column: ClusterText<'a>,
    labels: JsonLabels<'a>,
    md5_labels: &'a str,
    metric_name: &'a str,
    metric_part: i64,
    metric_type: &'a str,
    name: &'a str,
    region_uri: Option<&'a str>,
    shard_name: &'a str,
    start_ts: i64,
    system_uri: SystemText<'a>,
    tenant_cluster_column: Option<&'a str>,
    tenant_id: Option<&'a str>,
    timestamp_hour_ms: i64,
    topic: &'a str,
    values: [Sample; 1],
    workspace_id: Option<&'a str>,
}
struct JsonLabels<'a>(&'a [(&'a str, &'a str)]);
impl serde::Serialize for JsonLabels<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(self.0.iter().copied())
    }
}
struct ClusterText<'a>(&'a str, &'a str, &'a str);
impl serde::Serialize for ClusterText<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&format_args!("{}|{}|{}", self.0, self.1, self.2))
    }
}
struct SystemText<'a>(&'a str);
impl serde::Serialize for SystemText<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.0.is_empty() {
            serializer.serialize_none()
        } else {
            serializer.collect_str(&format_args!("system:{}", self.0))
        }
    }
}
#[derive(serde::Serialize)]
struct Sample {
    ts: i64,
    v: f64,
}
fn write_row_json(
    out: &mut Vec<u8>,
    row: &RowRef<'_>,
    p: &Promoted<'_>,
    event_time: &str,
    timestamp_hour: &str,
    partition_date: &str,
) {
    serde_json::to_writer(
        out,
        &SerdeRow {
            _event_time: event_time,
            _partition_date: partition_date,
            _shard_name: p.shard_name,
            _system: p.system,
            _timestamp_hour: timestamp_hour,
            cluster_id: p.cluster_id,
            hydra_cluster_column: ClusterText(row.name, p.system, p.shard_name),
            labels: JsonLabels(row.labels),
            md5_labels: row.md5_labels,
            metric_name: row.metric_name(),
            metric_part: row.part,
            metric_type: row.metric_type,
            name: row.name,
            region_uri: p.region_uri,
            shard_name: p.shard_name,
            start_ts: row.timestamp_ms,
            system_uri: SystemText(p.system),
            tenant_cluster_column: p.tenant_cluster_column,
            tenant_id: p.tenant_id,
            timestamp_hour_ms: row.timestamp_hour_ms,
            topic: TOPIC,
            values: [Sample {
                ts: row.timestamp_ms,
                v: row.value,
            }],
            workspace_id: p.workspace_id,
        },
    )
    .expect("serializing to reused memory buffer");
}

/// Assembles one request's rows into one pre-encoded event per port.
pub(super) struct RequestEncoder<'a> {
    plan: &'a OutputPlan,
    /// NDJSON rows per port (only `ndjson` ports), separated by `\n`.
    json: Vec<Vec<u8>>,
    /// Row indices into `arrow` per port (only `arrow_ipc` ports).
    arrow_rows: Vec<Vec<u32>>,
    arrow: Option<ArrowRows>,
    rows: Vec<usize>,
    scratch: Vec<u8>,
    labels_json: Vec<u8>,
}

impl<'a> RequestEncoder<'a> {
    pub(super) fn new(plan: &'a OutputPlan) -> Self {
        let n = plan.ports.len();
        let has_arrow = plan.ports.iter().any(|p| p.format.is_arrow());
        Self {
            plan,
            json: vec![Vec::new(); n],
            arrow_rows: vec![Vec::new(); n],
            arrow: has_arrow.then(ArrowRows::new),
            rows: vec![0; n],
            scratch: Vec::with_capacity(4096),
            labels_json: Vec::with_capacity(2048),
        }
    }

    /// Validate and add one sample (see [`super::accept`]), or return why it is dropped.
    /// `labels` must be key-sorted and unique, as `MetricTags::iter_single()` yields them.
    pub(super) fn push_sample(
        &mut self,
        name: &str,
        labels: &[(&str, &str)],
        timestamp_ms: i64,
        value: f64,
    ) -> Result<(), DropReason> {
        let (timestamp_ms, timestamp_hour_ms) =
            accept(timestamp_ms, value, chrono::Utc::now().timestamp_millis())?;
        let (base, metric_type) = MetricToLogHydra::parse_metric_name(name);
        let part = MetricToLogHydra::metric_part(base);
        // md5_labels == md5(encode_json(.tags)), as the transform computes it.
        self.labels_json.clear();
        serde_json::to_writer(&mut self.labels_json, &JsonLabels(labels))
            .expect("serialize labels into reused buffer");
        let md5_labels = format!("{:x}", md5::Md5::digest(self.labels_json.as_slice()));
        let row = RowRef {
            name,
            labels,
            timestamp_ms,
            timestamp_hour_ms,
            value,
            metric_name_len: base.len(),
            metric_type,
            part,
            md5_labels: &md5_labels,
        };
        self.push_row(&row);
        Ok(())
    }

    fn push_row(&mut self, row: &RowRef<'_>) {
        let plan = self.plan;
        let promoted = Promoted::of(row);
        let ports = plan.decide(promoted.system);

        let partition_date = row.event_time().format("%Y-%m-%d").to_string();
        let mut wants_json = false;
        let mut wants_arrow = false;
        for &p in ports {
            match plan.ports[p].format {
                OutputEncoding::Ndjson => wants_json = true,
                OutputEncoding::ArrowIpc | OutputEncoding::ArrowIpcVariant => wants_arrow = true,
            }
        }

        if wants_json {
            let event_time = vrl::value::value::timestamp_to_string(&row.event_time());
            let timestamp_hour = vrl::value::value::timestamp_to_string(&row.timestamp_hour());
            self.scratch.clear();
            write_row_json(
                &mut self.scratch,
                row,
                &promoted,
                &event_time,
                &timestamp_hour,
                &partition_date,
            );
        }
        let arrow_index = if wants_arrow {
            let arrow = self.arrow.as_mut().expect("plan has an arrow port");
            let index = arrow.rows as u32;
            arrow.push(row, &promoted, &partition_date);
            Some(index)
        } else {
            None
        };

        for &p in ports {
            match plan.ports[p].format {
                OutputEncoding::Ndjson => {
                    let buf = &mut self.json[p];
                    if !buf.is_empty() {
                        buf.push(b'\n');
                    }
                    buf.extend_from_slice(&self.scratch);
                }
                OutputEncoding::ArrowIpc | OutputEncoding::ArrowIpcVariant => {
                    self.arrow_rows[p].push(arrow_index.expect("arrow row appended"));
                }
            }
            self.rows[p] += 1;
        }
    }

    /// One pre-encoded event per port with rows, with that port's name.
    pub(super) fn finish_events(mut self) -> Vec<(&'a str, LogEvent)> {
        let mut out = Vec::with_capacity(self.plan.ports.len());
        let plan = self.plan;
        let full = self.arrow.take().map(|a| a.finish(&plan.schema));
        // `labels` as VARIANT, once for every `arrow_ipc_variant` port.
        let full_variant = plan
            .ports
            .iter()
            .enumerate()
            .any(|(p, port)| port.format == OutputEncoding::ArrowIpcVariant && self.rows[p] > 0)
            .then(|| {
                let full = full.as_ref().expect("arrow rows were built");
                let labels = full.column(LABELS_COLUMN).as_map();
                let variant = map_array_to_variant(labels).expect("string map from the builder");
                let mut columns = full.columns().to_vec();
                columns[LABELS_COLUMN] = variant;
                RecordBatch::try_new(Arc::clone(&plan.variant_schema), columns)
                    .expect("variant column matches variant_schema")
            });
        for (p, port) in plan.ports.iter().enumerate() {
            if self.rows[p] == 0 {
                continue;
            }
            let message = match port.format {
                OutputEncoding::Ndjson => Bytes::from(std::mem::take(&mut self.json[p])),
                format => {
                    let batch = match format {
                        OutputEncoding::ArrowIpcVariant => full_variant.as_ref(),
                        _ => full.as_ref(),
                    }
                    .expect("arrow rows were built");
                    let selection = std::mem::take(&mut self.arrow_rows[p]);
                    if selection.len() == batch.num_rows() {
                        ipc_bytes(batch)
                    } else {
                        let indices = UInt32Array::from(selection);
                        let subset =
                            take_record_batch(batch, &indices).expect("indices are in range");
                        ipc_bytes(&subset)
                    }
                }
            };
            // Delivery accounting counts the payload's rows, not one event.
            let mut log = LogEvent::default();
            log.metadata_mut().set_delivery_event_count(self.rows[p]);
            log.insert(
                vector_lib::lookup::event_path!("message"),
                Value::Bytes(message),
            );
            metrics::counter!("hydra_remote_write_encoded_rows_total", "output" => port.name.clone()).increment(self.rows[p] as u64);
            out.push((port.name.as_str(), log));
        }
        out
    }
}
