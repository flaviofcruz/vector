//! Arrow IPC streaming format codec for batched event encoding
//!
//! Provides Apache Arrow IPC stream format encoding with static schema support.
//! This implements the streaming variant of the Arrow IPC protocol, which writes
//! a continuous stream of record batches without a file footer.

use arrow::{
    array::{
        ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Date64Builder, Decimal128Builder,
        Decimal256Builder, Float32Builder, Float64Builder, Int8Builder, Int16Builder, Int32Builder,
        Int64Builder, LargeBinaryBuilder, LargeStringBuilder, ListArray, MapArray, StringBuilder,
        StringDictionaryBuilder, StructArray, TimestampMicrosecondBuilder,
        TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder,
        UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
    },
    buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
    datatypes::{DataType, Field, Fields, Int32Type, Schema, TimeUnit, i256},
    ipc::writer::StreamWriter,
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use rust_decimal::Decimal;
use snafu::Snafu;
use std::collections::HashMap;
use std::sync::Arc;
use vector_config::configurable_component;

use vector_core::event::{Event, Value};

/// Field-metadata key holding a column's coercion default. Consumed when
/// `coerce_missing_to_default` is set; always stripped from the schema before the wire.
pub const COERCE_DEFAULT_METADATA_KEY: &str = "databricks.arrow.coerce_default";

/// Provides Arrow schema for encoding.
///
/// Sinks can implement this trait to provide custom schema fetching logic.
#[async_trait]
pub trait SchemaProvider: Send + Sync + std::fmt::Debug {
    /// Fetch the Arrow schema from the data store.
    ///
    /// This is called during sink configuration build phase to fetch
    /// the schema once at startup, rather than at runtime.
    async fn get_schema(&self) -> Result<Schema, ArrowEncodingError>;
}

/// Configuration for Arrow IPC stream serialization
#[configurable_component]
#[derive(Clone, Default)]
pub struct ArrowStreamSerializerConfig {
    /// The Arrow schema to use for encoding
    #[serde(skip)]
    #[configurable(derived)]
    pub schema: Option<arrow::datatypes::Schema>,

    /// Allow null values for non-nullable fields in the schema.
    ///
    /// When enabled, missing or incompatible values will be encoded as null even for fields
    /// marked as non-nullable in the Arrow schema. This is useful when working with downstream
    /// systems that can handle null values through defaults, computed columns, or other mechanisms.
    ///
    /// When disabled (default), missing values for non-nullable fields will cause encoding errors,
    /// ensuring all required data is present before sending to the sink.
    #[serde(default)]
    #[configurable(derived)]
    pub allow_nullable_fields: bool,

    /// Coerce a missing/null/incompatible value for a non-nullable column to its default (from
    /// `COERCE_DEFAULT_METADATA_KEY` metadata) and parse numeric strings into `Int64` columns. Off by default.
    #[serde(default)]
    #[configurable(derived)]
    pub coerce_missing_to_default: bool,
}

impl std::fmt::Debug for ArrowStreamSerializerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrowStreamSerializerConfig")
            .field(
                "schema",
                &self
                    .schema
                    .as_ref()
                    .map(|s| format!("{} fields", s.fields().len())),
            )
            .field("allow_nullable_fields", &self.allow_nullable_fields)
            .field("coerce_missing_to_default", &self.coerce_missing_to_default)
            .finish()
    }
}

impl ArrowStreamSerializerConfig {
    /// Create a new ArrowStreamSerializerConfig with a schema
    pub fn new(schema: arrow::datatypes::Schema) -> Self {
        Self {
            schema: Some(schema),
            allow_nullable_fields: false,
            coerce_missing_to_default: false,
        }
    }

    /// The data type of events that are accepted by `ArrowStreamEncoder`.
    pub fn input_type(&self) -> vector_core::config::DataType {
        vector_core::config::DataType::Log
    }

    /// The schema required by the serializer.
    pub fn schema_requirement(&self) -> vector_core::schema::Requirement {
        vector_core::schema::Requirement::empty()
    }
}

/// Arrow IPC stream batch serializer that holds the schema
#[derive(Clone, Debug)]
pub struct ArrowStreamSerializer {
    schema: Arc<Schema>,
    /// Per-column coercion defaults (field -> default string); `None` when coercion is off.
    coerce_defaults: Option<Arc<HashMap<String, String>>>,
}

impl ArrowStreamSerializer {
    /// Create a new ArrowStreamSerializer with the given configuration
    pub fn new(config: ArrowStreamSerializerConfig) -> Result<Self, vector_common::Error> {
        let schema = config
            .schema
            .ok_or_else(|| vector_common::Error::from("Arrow serializer requires a schema."))?;

        // Read the per-column coerce defaults from field metadata before it is stripped below.
        let coerce_defaults = config.coerce_missing_to_default.then(|| {
            Arc::new(
                schema
                    .fields()
                    .iter()
                    .filter_map(|f| {
                        f.metadata()
                            .get(COERCE_DEFAULT_METADATA_KEY)
                            .map(|d| (f.name().clone(), d.clone()))
                    })
                    .collect::<HashMap<String, String>>(),
            )
        });

        // Strip the internal coerce marker (it must not reach the wire) and, if enabled, relax
        // nullability. Done once here, not per batch.
        let needs_strip = schema
            .fields()
            .iter()
            .any(|f| f.metadata().contains_key(COERCE_DEFAULT_METADATA_KEY));
        let needs_nullable = config.allow_nullable_fields;
        let schema = if needs_strip || needs_nullable {
            let fields = schema
                .fields()
                .iter()
                .map(|f| {
                    let field = if needs_strip {
                        strip_coerce_metadata(f)
                    } else {
                        f.as_ref().clone()
                    };
                    let field = if needs_nullable {
                        make_field_nullable(&field)
                    } else {
                        field
                    };
                    Arc::new(field)
                })
                .collect::<Vec<_>>();
            Schema::new_with_metadata(fields, schema.metadata().clone())
        } else {
            schema
        };

        Ok(Self {
            schema: Arc::new(schema),
            coerce_defaults,
        })
    }

    /// Encode a batch of events into an Arrow RecordBatch.
    pub fn encode_to_record_batch(
        &self,
        events: &[Event],
    ) -> Result<RecordBatch, ArrowEncodingError> {
        if events.is_empty() {
            return Err(ArrowEncodingError::NoEvents);
        }
        build_record_batch_inner(
            Arc::clone(&self.schema),
            events,
            self.coerce_defaults.as_deref(),
        )
    }
}

impl tokio_util::codec::Encoder<Vec<Event>> for ArrowStreamSerializer {
    type Error = ArrowEncodingError;

    fn encode(&mut self, events: Vec<Event>, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        if events.is_empty() {
            return Err(ArrowEncodingError::NoEvents);
        }

        // With coercion, build the batch with per-column defaults; otherwise take the existing path.
        let bytes = match self.coerce_defaults.as_deref() {
            None => encode_events_to_arrow_ipc_stream(&events, Some(Arc::clone(&self.schema)))?,
            Some(defaults) => {
                let record_batch =
                    build_record_batch_inner(Arc::clone(&self.schema), &events, Some(defaults))?;
                record_batch_to_arrow_ipc_stream(&record_batch)?
            }
        };

        buffer.extend_from_slice(&bytes);
        Ok(())
    }
}

/// Errors that can occur during Arrow encoding
#[derive(Debug, Snafu)]
pub enum ArrowEncodingError {
    /// Failed to create Arrow record batch
    #[snafu(display("Failed to create Arrow record batch: {}", source))]
    RecordBatchCreation {
        /// The underlying Arrow error
        source: arrow::error::ArrowError,
    },

    /// Failed to write Arrow IPC data
    #[snafu(display("Failed to write Arrow IPC data: {}", source))]
    IpcWrite {
        /// The underlying Arrow error
        source: arrow::error::ArrowError,
    },

    /// No events provided for encoding
    #[snafu(display("No events provided for encoding"))]
    NoEvents,

    /// Schema must be provided before encoding
    #[snafu(display("Schema must be provided before encoding"))]
    NoSchemaProvided,

    /// Failed to fetch schema from provider
    #[snafu(display("Failed to fetch schema from provider: {}", message))]
    SchemaFetchError {
        /// Error message from the provider
        message: String,
    },

    /// Unsupported Arrow data type for field
    #[snafu(display(
        "Unsupported Arrow data type for field '{}': {:?}",
        field_name,
        data_type
    ))]
    UnsupportedType {
        /// The field name
        field_name: String,
        /// The unsupported data type
        data_type: DataType,
    },

    /// Null value encountered for non-nullable field
    #[snafu(display("Null value for non-nullable field '{}'", field_name))]
    NullConstraint {
        /// The field name
        field_name: String,
    },

    /// A present value cannot be represented as the column's type
    #[snafu(display(
        "Field '{}': present value cannot be encoded as the target column type",
        field_name
    ))]
    InvalidValue {
        /// The field name
        field_name: String,
    },

    /// IO error during encoding
    #[snafu(display("IO error: {}", source))]
    Io {
        /// The underlying IO error
        source: std::io::Error,
    },
}

impl From<std::io::Error> for ArrowEncodingError {
    fn from(error: std::io::Error) -> Self {
        Self::Io { source: error }
    }
}

/// Encodes a batch of events into Arrow IPC streaming format
pub fn encode_events_to_arrow_ipc_stream(
    events: &[Event],
    schema: Option<Arc<Schema>>,
) -> Result<Bytes, ArrowEncodingError> {
    if events.is_empty() {
        return Err(ArrowEncodingError::NoEvents);
    }

    let schema_ref = schema.ok_or(ArrowEncodingError::NoSchemaProvided)?;

    let record_batch = build_record_batch(schema_ref, events)?;

    let ipc_err = |source| ArrowEncodingError::IpcWrite { source };

    let mut buffer = BytesMut::new().writer();
    let mut writer =
        StreamWriter::try_new(&mut buffer, record_batch.schema_ref()).map_err(ipc_err)?;
    writer.write(&record_batch).map_err(ipc_err)?;
    writer.finish().map_err(ipc_err)?;

    Ok(buffer.into_inner().freeze())
}

/// Recursively makes a Field and all its nested fields nullable
fn make_field_nullable(field: &arrow::datatypes::Field) -> arrow::datatypes::Field {
    let new_data_type = match field.data_type() {
        DataType::List(inner_field) => DataType::List(Arc::new(make_field_nullable(inner_field))),
        DataType::Struct(fields) => {
            DataType::Struct(fields.iter().map(|f| make_field_nullable(f)).collect())
        }
        DataType::Map(inner_field, sorted) => {
            DataType::Map(Arc::new(make_field_nullable(inner_field)), *sorted)
        }
        other => other.clone(),
    };

    field
        .clone()
        .with_data_type(new_data_type)
        .with_nullable(true)
}

/// Returns a copy of `field` with the internal coerce-default marker removed (other metadata kept).
fn strip_coerce_metadata(field: &arrow::datatypes::Field) -> arrow::datatypes::Field {
    let mut metadata = field.metadata().clone();
    metadata.remove(COERCE_DEFAULT_METADATA_KEY);
    field.clone().with_metadata(metadata)
}

/// Serializes a RecordBatch into Arrow IPC streaming format bytes.
pub fn record_batch_to_arrow_ipc_stream(
    record_batch: &RecordBatch,
) -> Result<Bytes, ArrowEncodingError> {
    let ipc_err = |source| ArrowEncodingError::IpcWrite { source };

    let mut buffer = BytesMut::new().writer();
    let mut writer =
        StreamWriter::try_new(&mut buffer, record_batch.schema_ref()).map_err(ipc_err)?;
    writer.write(record_batch).map_err(ipc_err)?;
    writer.finish().map_err(ipc_err)?;

    Ok(buffer.into_inner().freeze())
}

/// Builds an Arrow RecordBatch from events
pub fn build_record_batch(
    schema: Arc<Schema>,
    events: &[Event],
) -> Result<RecordBatch, ArrowEncodingError> {
    build_record_batch_inner(schema, events, None)
}

/// Builds a RecordBatch, optionally coercing missing/null/incompatible values to a per-column
/// default instead of erroring. See `ArrowStreamSerializerConfig::coerce_missing_to_default`.
fn build_record_batch_inner(
    schema: Arc<Schema>,
    events: &[Event],
    coerce_defaults: Option<&HashMap<String, String>>,
) -> Result<RecordBatch, ArrowEncodingError> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let missing_default = coerce_defaults
            .and_then(|m| m.get(field.name()))
            .map(String::as_str);
        columns.push(build_column_for_path(
            events,
            field.name(),
            field,
            missing_default,
        )?);
    }

    RecordBatch::try_new(schema, columns)
        .map_err(|source| ArrowEncodingError::RecordBatchCreation { source })
}

/// Macro to handle appending null or returning an error for non-nullable fields.
macro_rules! handle_null_constraints {
    ($builder:expr, $nullable:expr, $field_name:expr) => {{
        if !$nullable {
            return Err(ArrowEncodingError::NullConstraint {
                field_name: $field_name.into(),
            });
        }
        $builder.append_null();
    }};
}

/// Handles a present value that cannot be represented as the target type: errors when coercion
/// is enabled, otherwise applies the null/non-nullable handling.
macro_rules! present_value_invalid {
    ($builder:expr, $nullable:expr, $field_name:expr, $missing_default:expr) => {{
        match $missing_default {
            Some(_) => {
                return Err(ArrowEncodingError::InvalidValue {
                    field_name: $field_name.into(),
                });
            }
            None => handle_null_constraints!($builder, $nullable, $field_name),
        }
    }};
}

/// Macro to generate a `build_*_array` function for primitive types.
macro_rules! define_build_primitive_array_fn {
    (
        $fn_name:ident, // The function name (e.g., build_int8_array)
        $builder_ty:ty, // The builder type (e.g., Int8Builder)
        // One or more match arms for valid Value types
        $( $value_pat:pat $(if $guard:expr)? => $append_expr:expr ),+
    ) => {
        fn $fn_name(
            events: &[Event],
            field_name: &str,
            nullable: bool,
            missing_default: Option<&str>,
        ) -> Result<ArrayRef, ArrowEncodingError> {
            let mut builder = <$builder_ty>::with_capacity(events.len());

            for event in events {
                if let Event::Log(log) = event {
                    match log.get(field_name) {
                        $(
                            $value_pat $(if $guard)? => builder.append_value($append_expr),
                        )+
                        // Absent or null: default when coercing, else null/error.
                        None | Some(Value::Null) => match missing_default {
                            Some(_) => builder.append_value(Default::default()),
                            None => handle_null_constraints!(builder, nullable, field_name),
                        },
                        // Present but invalid for this type.
                        Some(_) => {
                            present_value_invalid!(builder, nullable, field_name, missing_default)
                        }
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    };
}

fn extract_timestamp(value: &Value) -> Option<DateTime<Utc>> {
    match value {
        Value::Timestamp(ts) => Some(*ts),
        Value::Bytes(bytes) => std::str::from_utf8(bytes)
            .ok()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc)),
        _ => None,
    }
}

fn build_timestamp_array(
    events: &[Event],
    field_name: &str,
    time_unit: TimeUnit,
    timezone: Option<Arc<str>>,
    nullable: bool,
    missing_default: Option<&str>,
) -> Result<ArrayRef, ArrowEncodingError> {
    macro_rules! build_array {
        ($builder:ty, $converter:expr) => {{
            let mut builder = <$builder>::with_capacity(events.len());
            for event in events {
                if let Event::Log(log) = event {
                    match log.get(field_name) {
                        // Absent or null: epoch when coercing, else null/error.
                        None | Some(Value::Null) => match missing_default {
                            Some(_) => builder.append_value(0), // epoch
                            None => handle_null_constraints!(builder, nullable, field_name),
                        },
                        Some(value) => {
                            // Try a native or string timestamp, else a raw integer.
                            let parsed = if let Some(ts) = extract_timestamp(value) {
                                $converter(&ts)
                            } else if let Value::Integer(i) = value {
                                Some(*i)
                            } else {
                                None
                            };
                            match parsed {
                                Some(v) => builder.append_value(v),
                                None => present_value_invalid!(
                                    builder,
                                    nullable,
                                    field_name,
                                    missing_default
                                ),
                            }
                        }
                    }
                }
            }
            let array = builder.finish();
            if let Some(tz) = timezone {
                Ok(Arc::new(array.with_timezone(tz)))
            } else {
                Ok(Arc::new(array))
            }
        }};
    }

    match time_unit {
        TimeUnit::Second => {
            build_array!(TimestampSecondBuilder, |ts: &DateTime<Utc>| Some(
                ts.timestamp()
            ))
        }
        TimeUnit::Millisecond => {
            build_array!(TimestampMillisecondBuilder, |ts: &DateTime<Utc>| Some(
                ts.timestamp_millis()
            ))
        }
        TimeUnit::Microsecond => {
            build_array!(TimestampMicrosecondBuilder, |ts: &DateTime<Utc>| Some(
                ts.timestamp_micros()
            ))
        }
        TimeUnit::Nanosecond => {
            build_array!(TimestampNanosecondBuilder, |ts: &DateTime<Utc>| ts
                .timestamp_nanos_opt())
        }
    }
}

// Arrow's Date32/Date64 epoch is 1970-01-01 UTC. `chrono::NaiveDate::num_days_from_ce`
// counts from year 1 CE, so we subtract the epoch's CE-day count.
fn days_since_epoch(date: NaiveDate) -> i32 {
    const UNIX_EPOCH_DAYS_FROM_CE: i32 = 719_163;
    date.num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE
}

fn build_date32_array(
    events: &[Event],
    field_name: &str,
    nullable: bool,
    missing_default: Option<&str>,
) -> Result<ArrayRef, ArrowEncodingError> {
    let mut builder = Date32Builder::with_capacity(events.len());
    for event in events {
        if let Event::Log(log) = event {
            match log.get(field_name) {
                // Absent or null: epoch when coercing, else null/error.
                None | Some(Value::Null) => match missing_default {
                    Some(_) => builder.append_value(0), // epoch (1970-01-01)
                    None => handle_null_constraints!(builder, nullable, field_name),
                },
                Some(value) => {
                    let parsed = if let Some(ts) = extract_timestamp(value) {
                        Some(days_since_epoch(ts.date_naive()))
                    } else if let Value::Integer(i) = value {
                        i32::try_from(*i).ok()
                    } else {
                        None
                    };
                    match parsed {
                        Some(days) => builder.append_value(days),
                        None => {
                            present_value_invalid!(builder, nullable, field_name, missing_default)
                        }
                    }
                }
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_date64_array(
    events: &[Event],
    field_name: &str,
    nullable: bool,
    missing_default: Option<&str>,
) -> Result<ArrayRef, ArrowEncodingError> {
    let mut builder = Date64Builder::with_capacity(events.len());
    for event in events {
        if let Event::Log(log) = event {
            match log.get(field_name) {
                // Absent or null: epoch when coercing, else null/error.
                None | Some(Value::Null) => match missing_default {
                    Some(_) => builder.append_value(0), // epoch (1970-01-01)
                    None => handle_null_constraints!(builder, nullable, field_name),
                },
                Some(value) => {
                    // Date64 is midnight-aligned millis since epoch; timestamps are truncated to
                    // the date, and integers pass through as-is.
                    let parsed = if let Some(ts) = extract_timestamp(value) {
                        Some(i64::from(days_since_epoch(ts.date_naive())) * 86_400_000)
                    } else if let Value::Integer(i) = value {
                        Some(*i)
                    } else {
                        None
                    };
                    match parsed {
                        Some(ms) => builder.append_value(ms),
                        None => {
                            present_value_invalid!(builder, nullable, field_name, missing_default)
                        }
                    }
                }
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_string_array(
    events: &[Event],
    field_name: &str,
    nullable: bool,
    missing_default: Option<&str>,
) -> Result<ArrayRef, ArrowEncodingError> {
    let mut builder = StringBuilder::with_capacity(events.len(), 0);

    for event in events {
        if let Event::Log(log) = event {
            let mut appended = false;
            if let Some(value) = log.get(field_name) {
                match value {
                    Value::Bytes(bytes) => {
                        // Attempt direct UTF-8 conversion first, fallback to lossy
                        match std::str::from_utf8(bytes) {
                            Ok(s) => builder.append_value(s),
                            Err(_) => builder.append_value(&String::from_utf8_lossy(bytes)),
                        }
                        appended = true;
                    }
                    Value::Object(obj) => {
                        if let Ok(s) = serde_json::to_string(&obj) {
                            builder.append_value(s);
                            appended = true;
                        }
                    }
                    Value::Array(arr) => {
                        if let Ok(s) = serde_json::to_string(&arr) {
                            builder.append_value(s);
                            appended = true;
                        }
                    }
                    Value::Null => {}
                    _ => {
                        builder.append_value(&value.to_string_lossy());
                        appended = true;
                    }
                }
            }

            if !appended {
                match missing_default {
                    Some(default) => builder.append_value(default),
                    None => handle_null_constraints!(builder, nullable, field_name),
                }
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Builds a dictionary-encoded string array (`Dictionary(Int32, Utf8)`) for a
/// `LowCardinality(String)` column, so ClickHouse stores it as `LowCardinality` without rebuilding.
fn build_string_dictionary_array(
    events: &[Event],
    field_name: &str,
    nullable: bool,
    missing_default: Option<&str>,
) -> Result<ArrayRef, ArrowEncodingError> {
    // One dictionary key per row; pre-size the keys buffer. Distinct values grow on demand.
    let mut builder = StringDictionaryBuilder::<Int32Type>::with_capacity(events.len(), 0, 0);

    for event in events {
        if let Event::Log(log) = event {
            let mut appended = false;
            if let Some(value) = log.get(field_name) {
                match value {
                    Value::Bytes(bytes) => {
                        match std::str::from_utf8(bytes) {
                            Ok(s) => builder.append_value(s),
                            Err(_) => builder.append_value(&String::from_utf8_lossy(bytes)),
                        }
                        appended = true;
                    }
                    Value::Object(obj) => {
                        if let Ok(s) = serde_json::to_string(&obj) {
                            builder.append_value(s);
                            appended = true;
                        }
                    }
                    Value::Array(arr) => {
                        if let Ok(s) = serde_json::to_string(&arr) {
                            builder.append_value(s);
                            appended = true;
                        }
                    }
                    Value::Null => {}
                    _ => {
                        builder.append_value(&value.to_string_lossy());
                        appended = true;
                    }
                }
            }

            if !appended {
                match missing_default {
                    Some(default) => builder.append_value(default),
                    None => handle_null_constraints!(builder, nullable, field_name),
                }
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

define_build_primitive_array_fn!(
    build_int8_array,
    Int8Builder,
    Some(Value::Integer(i)) if *i >= i8::MIN as i64 && *i <= i8::MAX as i64 => *i as i8
);

define_build_primitive_array_fn!(
    build_int16_array,
    Int16Builder,
    Some(Value::Integer(i)) if *i >= i16::MIN as i64 && *i <= i16::MAX as i64 => *i as i16
);

define_build_primitive_array_fn!(
    build_int32_array,
    Int32Builder,
    Some(Value::Integer(i)) if *i >= i32::MIN as i64 && *i <= i32::MAX as i64 => *i as i32
);

/// Builds an `Int64` array. With `missing_default` set, numeric strings are parsed and any
/// missing/null/non-integer value falls back to the default; with `None`, only `Value::Integer`
/// is accepted (anything else hits the nullable/non-nullable handling).
fn build_int64_array(
    events: &[Event],
    field_name: &str,
    nullable: bool,
    missing_default: Option<&str>,
) -> Result<ArrayRef, ArrowEncodingError> {
    let mut builder = Int64Builder::with_capacity(events.len());

    // Coercion on: an unparseable default (the "" a collection column forwards to its children)
    // coerces to 0, like the other numeric builders. Off (`None`): a missing value stays unset.
    let default_value: Option<i64> = missing_default.map(|d| d.trim().parse::<i64>().unwrap_or(0));

    for event in events {
        if let Event::Log(log) = event {
            match log.get(field_name) {
                Some(Value::Integer(i)) => builder.append_value(*i),
                Some(Value::Bytes(bytes)) if missing_default.is_some() => {
                    match std::str::from_utf8(bytes)
                        .ok()
                        .and_then(|s| s.trim().parse::<i64>().ok())
                    {
                        Some(parsed) => builder.append_value(parsed),
                        // Present string that does not parse as an integer.
                        None => {
                            present_value_invalid!(builder, nullable, field_name, missing_default)
                        }
                    }
                }
                // Absent or null: parsed default when coercing, else null/error.
                None | Some(Value::Null) => match default_value {
                    Some(d) => builder.append_value(d),
                    None => handle_null_constraints!(builder, nullable, field_name),
                },
                // Present, non-integer value.
                Some(_) => {
                    present_value_invalid!(builder, nullable, field_name, missing_default)
                }
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

define_build_primitive_array_fn!(
    build_uint8_array,
    UInt8Builder,
    Some(Value::Integer(i)) if *i >= 0 && *i <= u8::MAX as i64 => *i as u8
);

define_build_primitive_array_fn!(
    build_uint16_array,
    UInt16Builder,
    Some(Value::Integer(i)) if *i >= 0 && *i <= u16::MAX as i64 => *i as u16
);

define_build_primitive_array_fn!(
    build_uint32_array,
    UInt32Builder,
    Some(Value::Integer(i)) if *i >= 0 && *i <= u32::MAX as i64 => *i as u32
);

define_build_primitive_array_fn!(
    build_uint64_array,
    UInt64Builder,
    Some(Value::Integer(i)) if *i >= 0 => *i as u64
);

define_build_primitive_array_fn!(
    build_float32_array,
    Float32Builder,
    Some(Value::Float(f)) => f.into_inner() as f32,
    Some(Value::Integer(i)) => *i as f32
);

define_build_primitive_array_fn!(
    build_float64_array,
    Float64Builder,
    Some(Value::Float(f)) => f.into_inner(),
    Some(Value::Integer(i)) => *i as f64
);

define_build_primitive_array_fn!(
    build_boolean_array,
    BooleanBuilder,
    Some(Value::Boolean(b)) => *b
);

fn build_binary_array(
    events: &[Event],
    field_name: &str,
    nullable: bool,
    missing_default: Option<&str>,
) -> Result<ArrayRef, ArrowEncodingError> {
    let mut builder = BinaryBuilder::with_capacity(events.len(), 0);

    for event in events {
        if let Event::Log(log) = event {
            match log.get(field_name) {
                Some(Value::Bytes(bytes)) => builder.append_value(bytes),
                _ => match missing_default {
                    Some(default) => builder.append_value(default),
                    None => handle_null_constraints!(builder, nullable, field_name),
                },
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn build_large_string_array(
    events: &[Event],
    field_name: &str,
    nullable: bool,
    missing_default: Option<&str>,
) -> Result<ArrayRef, ArrowEncodingError> {
    let mut builder = LargeStringBuilder::with_capacity(events.len(), 0);

    for event in events {
        if let Event::Log(log) = event {
            let mut appended = false;
            if let Some(value) = log.get(field_name) {
                match value {
                    Value::Bytes(bytes) => {
                        match std::str::from_utf8(bytes) {
                            Ok(s) => builder.append_value(s),
                            Err(_) => builder.append_value(&String::from_utf8_lossy(bytes)),
                        }
                        appended = true;
                    }
                    Value::Object(obj) => {
                        if let Ok(s) = serde_json::to_string(&obj) {
                            builder.append_value(s);
                            appended = true;
                        }
                    }
                    Value::Array(arr) => {
                        if let Ok(s) = serde_json::to_string(&arr) {
                            builder.append_value(s);
                            appended = true;
                        }
                    }
                    Value::Null => {}
                    _ => {
                        builder.append_value(&value.to_string_lossy());
                        appended = true;
                    }
                }
            }

            if !appended {
                match missing_default {
                    Some(default) => builder.append_value(default),
                    None => handle_null_constraints!(builder, nullable, field_name),
                }
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn build_large_binary_array(
    events: &[Event],
    field_name: &str,
    nullable: bool,
    missing_default: Option<&str>,
) -> Result<ArrayRef, ArrowEncodingError> {
    let mut builder = LargeBinaryBuilder::with_capacity(events.len(), 0);

    for event in events {
        if let Event::Log(log) = event {
            match log.get(field_name) {
                Some(Value::Bytes(bytes)) => builder.append_value(bytes),
                _ => match missing_default {
                    Some(default) => builder.append_value(default),
                    None => handle_null_constraints!(builder, nullable, field_name),
                },
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn build_decimal128_array(
    events: &[Event],
    field_name: &str,
    precision: u8,
    scale: i8,
    nullable: bool,
    missing_default: Option<&str>,
) -> Result<ArrayRef, ArrowEncodingError> {
    let mut builder = Decimal128Builder::with_capacity(events.len())
        .with_precision_and_scale(precision, scale)
        .map_err(|_| ArrowEncodingError::UnsupportedType {
            field_name: field_name.into(),
            data_type: DataType::Decimal128(precision, scale),
        })?;

    let target_scale = scale.unsigned_abs() as u32;

    for event in events {
        if let Event::Log(log) = event {
            match log.get(field_name) {
                Some(Value::Float(f)) => match Decimal::try_from(f.into_inner()) {
                    Ok(mut decimal) => {
                        decimal.rescale(target_scale);
                        builder.append_value(decimal.mantissa());
                    }
                    // Present but not a finite decimal (NaN/Inf).
                    Err(_) => {
                        present_value_invalid!(builder, nullable, field_name, missing_default)
                    }
                },
                Some(Value::Integer(i)) => {
                    let mut decimal = Decimal::from(*i);
                    decimal.rescale(target_scale);
                    builder.append_value(decimal.mantissa());
                }
                // Absent or null: 0 when coercing, else null/error.
                None | Some(Value::Null) => match missing_default {
                    Some(_) => builder.append_value(0),
                    None => handle_null_constraints!(builder, nullable, field_name),
                },
                // Present but not numeric.
                Some(_) => {
                    present_value_invalid!(builder, nullable, field_name, missing_default)
                }
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn build_decimal256_array(
    events: &[Event],
    field_name: &str,
    precision: u8,
    scale: i8,
    nullable: bool,
    missing_default: Option<&str>,
) -> Result<ArrayRef, ArrowEncodingError> {
    let mut builder = Decimal256Builder::with_capacity(events.len())
        .with_precision_and_scale(precision, scale)
        .map_err(|_| ArrowEncodingError::UnsupportedType {
            field_name: field_name.into(),
            data_type: DataType::Decimal256(precision, scale),
        })?;

    let target_scale = scale.unsigned_abs() as u32;

    for event in events {
        if let Event::Log(log) = event {
            match log.get(field_name) {
                Some(Value::Float(f)) => match Decimal::try_from(f.into_inner()) {
                    Ok(mut decimal) => {
                        decimal.rescale(target_scale);
                        // rust_decimal does not support i256 natively so we upcast here
                        builder.append_value(i256::from_i128(decimal.mantissa()));
                    }
                    // Present but not a finite decimal (NaN/Inf).
                    Err(_) => {
                        present_value_invalid!(builder, nullable, field_name, missing_default)
                    }
                },
                Some(Value::Integer(i)) => {
                    let mut decimal = Decimal::from(*i);
                    decimal.rescale(target_scale);
                    builder.append_value(i256::from_i128(decimal.mantissa()));
                }
                // Absent or null: 0 when coercing, else null/error.
                None | Some(Value::Null) => match missing_default {
                    Some(_) => builder.append_value(i256::from_i128(0)),
                    None => handle_null_constraints!(builder, nullable, field_name),
                },
                // Present but not numeric.
                Some(_) => {
                    present_value_invalid!(builder, nullable, field_name, missing_default)
                }
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Dispatches building an `ArrayRef` for a field at a dot-separated path in the events.
///
/// Used by `build_record_batch` for top-level fields and recursively by
/// `build_struct_array` for nested child fields.
fn build_column_for_path(
    events: &[Event],
    path: &str,
    field: &Field,
    missing_default: Option<&str>,
) -> Result<ArrayRef, ArrowEncodingError> {
    let nullable = field.is_nullable();
    match field.data_type() {
        DataType::Struct(fields) => {
            build_struct_array(events, path, fields, nullable, missing_default)
        }
        DataType::Map(entries_field, _sorted) => {
            build_map_array(events, path, entries_field, nullable, missing_default)
        }
        DataType::List(item_field) => {
            build_list_array(events, path, item_field, nullable, missing_default)
        }
        DataType::Timestamp(time_unit, tz) => build_timestamp_array(
            events,
            path,
            *time_unit,
            tz.clone(),
            nullable,
            missing_default,
        ),
        DataType::Date32 => build_date32_array(events, path, nullable, missing_default),
        DataType::Date64 => build_date64_array(events, path, nullable, missing_default),
        DataType::Utf8 => build_string_array(events, path, nullable, missing_default),
        DataType::LargeUtf8 => build_large_string_array(events, path, nullable, missing_default),
        DataType::Int8 => build_int8_array(events, path, nullable, missing_default),
        DataType::Int16 => build_int16_array(events, path, nullable, missing_default),
        DataType::Int32 => build_int32_array(events, path, nullable, missing_default),
        DataType::Int64 => build_int64_array(events, path, nullable, missing_default),
        DataType::UInt8 => build_uint8_array(events, path, nullable, missing_default),
        DataType::UInt16 => build_uint16_array(events, path, nullable, missing_default),
        DataType::UInt32 => build_uint32_array(events, path, nullable, missing_default),
        DataType::UInt64 => build_uint64_array(events, path, nullable, missing_default),
        DataType::Float32 => build_float32_array(events, path, nullable, missing_default),
        DataType::Float64 => build_float64_array(events, path, nullable, missing_default),
        DataType::Boolean => build_boolean_array(events, path, nullable, missing_default),
        DataType::Binary => build_binary_array(events, path, nullable, missing_default),
        DataType::LargeBinary => build_large_binary_array(events, path, nullable, missing_default),
        DataType::Decimal128(precision, scale) => {
            build_decimal128_array(events, path, *precision, *scale, nullable, missing_default)
        }
        DataType::Decimal256(precision, scale) => {
            build_decimal256_array(events, path, *precision, *scale, nullable, missing_default)
        }
        DataType::Dictionary(key, value)
            if matches!(key.as_ref(), DataType::Int32)
                && matches!(value.as_ref(), DataType::Utf8) =>
        {
            build_string_dictionary_array(events, path, nullable, missing_default)
        }
        other_type => Err(ArrowEncodingError::UnsupportedType {
            field_name: path.into(),
            data_type: other_type.clone(),
        }),
    }
}

/// Emits a metric and warning for `count` present but wrong-typed values at `path` that were
/// coerced to an empty collection. Called once per column per batch.
fn emit_malformed_collection_coerced(path: &str, count: u64, batch_size: usize) {
    metrics::counter!("arrow_malformed_collection_coerced", "field" => path.to_string())
        .increment(count);
    tracing::warn!(
        message = "Arrow encoder coerced present but wrong-typed value(s) to an empty collection",
        field = %path,
        coerced = count,
        batch_size,
    );
}

/// Emits a metric and warning for `count` map keys at `path` collapsed to 0/false because they did
/// not convert to a non-string key type. Collapsing can produce duplicate keys in a row.
fn emit_lossy_map_keys(path: &str, count: u64) {
    metrics::counter!("arrow_map_key_coercion_lossy", "field" => path.to_string()).increment(count);
    tracing::warn!(
        message = "Arrow encoder collapsed map key(s) that did not parse to the key type, \
                   so distinct keys may have merged",
        field = %path,
        lossy_keys = count,
    );
}

/// Disposition of a map/list cell whose value is not a well-formed collection.
enum CollectionCell {
    /// Emit an empty collection. `malformed` is set for a present-but-wrong-shape value, which the
    /// caller counts for the malformed-coercion metric.
    Coerced { malformed: bool },
    /// Emit a null row.
    Null,
}

/// Shared missing/null/wrong-type cascade for `build_map_array` and `build_list_array`.
/// `present_wrong_type` marks a present-but-wrong-shape value. A non-nullable column with coercion
/// off errors, matching the scalar builders.
fn classify_collection_presence(
    present_wrong_type: bool,
    nullable: bool,
    missing_default: Option<&str>,
    path: &str,
) -> Result<CollectionCell, ArrowEncodingError> {
    if missing_default.is_some() {
        Ok(CollectionCell::Coerced {
            malformed: present_wrong_type,
        })
    } else if nullable {
        Ok(CollectionCell::Null)
    } else {
        Err(ArrowEncodingError::NullConstraint {
            field_name: path.into(),
        })
    }
}

/// Builds an Arrow `StructArray` for a struct field at the given dot-separated path.
///
/// Child fields are accessed via `path.child_name` using Vector's path lookup,
/// and built recursively — supporting arbitrarily nested structs.
fn build_struct_array(
    events: &[Event],
    path: &str,
    fields: &Fields,
    nullable: bool,
    missing_default: Option<&str>,
) -> Result<ArrayRef, ArrowEncodingError> {
    // Children are built for every event; under coercion a missing child takes its default.
    let child_arrays: Vec<ArrayRef> = fields
        .iter()
        .map(|child_field| {
            let child_path = format!("{}.{}", path, child_field.name());
            build_column_for_path(events, &child_path, child_field, missing_default)
        })
        .collect::<Result<_, _>>()?;

    let mut has_null = false;
    let mut coerced_malformed: u64 = 0;
    let mut validity: Vec<bool> = Vec::with_capacity(events.len());
    for event in events {
        if let Event::Log(log) = event {
            let valid = if let Some(value) = log.get(path) {
                // Present non-object under coercion: valid row of child defaults, counted.
                // Without coercion, any present value yields a valid row.
                if missing_default.is_some() && !matches!(value, Value::Object(_) | Value::Null) {
                    coerced_malformed += 1;
                }
                true
            } else if missing_default.is_some() {
                // Coerced absent struct -> valid row of child defaults.
                true
            } else if nullable {
                has_null = true;
                false
            } else {
                return Err(ArrowEncodingError::NullConstraint {
                    field_name: path.into(),
                });
            };
            validity.push(valid);
        } else {
            validity.push(false);
            has_null = true;
        }
    }
    if coerced_malformed > 0 {
        emit_malformed_collection_coerced(path, coerced_malformed, events.len());
    }
    let null_buffer = has_null.then(|| NullBuffer::from(validity));

    let struct_array = StructArray::try_new(fields.clone(), child_arrays, null_buffer)
        .map_err(|source| ArrowEncodingError::RecordBatchCreation { source })?;
    Ok(Arc::new(struct_array))
}

/// Converts a string map key to the Arrow key type required by the schema.
///
/// Protobuf map keys may be integer types (e.g. `map<int64, bool>` uses `Int64`).
/// JSON object keys are always strings, so we parse them into the target type.
/// Protobuf allows bool, int32/64, uint32/64, and string as map key types.
///
/// Returns the converted `Value` and a `lossy` flag. `lossy` is true when the input did not
/// represent the key type (a non-numeric string for an integer key, a non-boolean string for a bool
/// key), so it collapses to 0/false and distinct keys may merge. The caller counts and reports it.
fn coerce_string_key(k: &str, key_type: &DataType) -> (Value, bool) {
    match key_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => match k.parse::<i64>() {
            Ok(i) => (Value::Integer(i), false),
            Err(_) => (Value::Integer(0), true),
        },
        DataType::Boolean => match k {
            "true" | "1" => (Value::Boolean(true), false),
            "false" | "0" => (Value::Boolean(false), false),
            _ => (Value::Boolean(false), true),
        },
        _ => (Value::Bytes(k.to_owned().into()), false),
    }
}

/// Builds an Arrow `MapArray` for a map field at the given path.
///
/// The Vector event value at `path` must be a `Value::Object` (string-keyed map). Values may be any
/// type `build_map_value_array` handles: the scalar types plus nested `Struct`, `List`, and `Map`.
fn build_map_array(
    events: &[Event],
    path: &str,
    entries_field: &Field,
    nullable: bool,
    missing_default: Option<&str>,
) -> Result<ArrayRef, ArrowEncodingError> {
    let DataType::Struct(kv_fields) = entries_field.data_type() else {
        return Err(ArrowEncodingError::UnsupportedType {
            field_name: path.into(),
            data_type: entries_field.data_type().clone(),
        });
    };
    if kv_fields.len() < 2 {
        return Err(ArrowEncodingError::UnsupportedType {
            field_name: path.into(),
            data_type: entries_field.data_type().clone(),
        });
    }
    // Arrow Map convention: entries struct has key as field 0, value as field 1.
    let key_field = &kv_fields[0];
    let value_field = &kv_fields[1];

    let mut flat_keys: Vec<Value> = Vec::new();
    let mut flat_values: Vec<Value> = Vec::new();
    let mut offsets: Vec<i32> = Vec::with_capacity(events.len() + 1);
    let mut validity: Vec<bool> = Vec::with_capacity(events.len());
    let mut current_offset: i32 = 0;
    let mut coerced_malformed: u64 = 0;
    let mut lossy_keys: u64 = 0;
    offsets.push(0);

    for event in events {
        if let Event::Log(log) = event {
            match log.get(path) {
                Some(Value::Object(obj)) => {
                    validity.push(true);
                    for (k, v) in obj.iter() {
                        // JSON object keys are always strings. Convert to the
                        // schema's key type (e.g. parse "123" → Int64 for map<int64, *>).
                        let (key_val, lossy) = coerce_string_key(k.as_str(), key_field.data_type());
                        if lossy {
                            lossy_keys += 1;
                        }
                        flat_keys.push(key_val);
                        flat_values.push(v.clone());
                        current_offset += 1;
                    }
                }
                // Not a well-formed object: empty map, null, or error per classify_collection_presence.
                // An explicit null counts as absent, not malformed.
                other => {
                    let present_wrong_type = !matches!(other, None | Some(Value::Null));
                    match classify_collection_presence(
                        present_wrong_type,
                        nullable,
                        missing_default,
                        path,
                    )? {
                        CollectionCell::Coerced { malformed } => {
                            validity.push(true);
                            if malformed {
                                coerced_malformed += 1;
                            }
                        }
                        CollectionCell::Null => validity.push(false),
                    }
                }
            }
        } else {
            validity.push(false);
        }
        offsets.push(current_offset);
    }
    if coerced_malformed > 0 {
        emit_malformed_collection_coerced(path, coerced_malformed, events.len());
    }
    if lossy_keys > 0 {
        emit_lossy_map_keys(path, lossy_keys);
    }

    let key_array = build_map_value_array(&flat_keys, key_field)?;
    let value_array = build_map_value_array(&flat_values, value_field)?;

    let entries_array = StructArray::try_new(kv_fields.clone(), vec![key_array, value_array], None)
        .map_err(|source| ArrowEncodingError::RecordBatchCreation { source })?;

    let null_buffer = validity
        .iter()
        .any(|v| !v)
        .then(|| NullBuffer::from(validity));

    let map_array = MapArray::try_new(
        Arc::new(entries_field.clone()),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        entries_array,
        null_buffer,
        false,
    )
    .map_err(|source| ArrowEncodingError::RecordBatchCreation { source })?;

    Ok(Arc::new(map_array))
}

/// Builds an Arrow `ListArray` for a repeated field at the given path.
///
/// The Vector event value at `path` must be a `Value::Array`. Each element
/// is encoded according to `item_field`, supporting all scalar types, `Struct`,
/// and nested `List`. An absent or non-array value becomes an empty list under coercion, a null list
/// row when the field is nullable, or a `NullConstraint` error when non-nullable.
fn build_list_array(
    events: &[Event],
    path: &str,
    item_field: &Field,
    nullable: bool,
    missing_default: Option<&str>,
) -> Result<ArrayRef, ArrowEncodingError> {
    let mut flat_items: Vec<Value> = Vec::new();
    let mut offsets: Vec<i32> = vec![0];
    let mut validity: Vec<bool> = Vec::with_capacity(events.len());
    let mut has_null = false;
    let mut coerced_malformed: u64 = 0;

    for event in events {
        let value = if let Event::Log(log) = event {
            log.get(path)
        } else {
            None
        };

        match value {
            Some(Value::Array(arr)) => {
                flat_items.extend(arr.iter().cloned());
                offsets.push(flat_items.len() as i32);
                validity.push(true);
            }
            // Not a well-formed array: empty list, null, or error per classify_collection_presence.
            // An explicit null counts as absent, not malformed.
            other => {
                offsets.push(*offsets.last().unwrap_or(&0));
                let present_wrong_type = !matches!(other, None | Some(Value::Null));
                match classify_collection_presence(
                    present_wrong_type,
                    nullable,
                    missing_default,
                    path,
                )? {
                    CollectionCell::Coerced { malformed } => {
                        validity.push(true);
                        if malformed {
                            coerced_malformed += 1;
                        }
                    }
                    CollectionCell::Null => {
                        validity.push(false);
                        has_null = true;
                    }
                }
            }
        }
    }
    if coerced_malformed > 0 {
        emit_malformed_collection_coerced(path, coerced_malformed, events.len());
    }

    let child_array = build_list_item_array(&flat_items, item_field)?;
    let offset_buffer = OffsetBuffer::new(ScalarBuffer::from(offsets));
    let null_buffer = has_null.then(|| NullBuffer::from(validity));

    let list_array = ListArray::try_new(
        Arc::new(item_field.clone()),
        offset_buffer,
        child_array,
        null_buffer,
    )
    .map_err(|source| ArrowEncodingError::RecordBatchCreation { source })?;

    Ok(Arc::new(list_array))
}

/// Builds the flat child array for a `ListArray` from pre-collected item values.
///
/// Delegates to `build_map_value_array` for scalar types.  For `Struct`, each
/// item must be a `Value::Object`; child fields are extracted by name.
/// For nested `List`, each item must be a `Value::Array`.
fn build_list_item_array(items: &[Value], field: &Field) -> Result<ArrayRef, ArrowEncodingError> {
    match field.data_type() {
        DataType::Struct(fields) => {
            let child_arrays: Vec<ArrayRef> = fields
                .iter()
                .map(|child_field| {
                    let child_values: Vec<Value> = items
                        .iter()
                        .map(|item| match item {
                            Value::Object(map) => map
                                .get(child_field.name().as_str())
                                .cloned()
                                .unwrap_or(Value::Null),
                            _ => Value::Null,
                        })
                        .collect();
                    build_list_item_array(&child_values, child_field)
                })
                .collect::<Result<_, _>>()?;

            // Null buffer: items that were not Value::Object are null struct rows.
            let mut has_null = false;
            let validity: Vec<bool> = items
                .iter()
                .map(|item| {
                    let valid = matches!(item, Value::Object(_));
                    if !valid {
                        has_null = true;
                    }
                    valid
                })
                .collect();
            let null_buffer = has_null.then(|| NullBuffer::from(validity));

            let struct_array = StructArray::try_new(fields.clone(), child_arrays, null_buffer)
                .map_err(|source| ArrowEncodingError::RecordBatchCreation { source })?;
            Ok(Arc::new(struct_array))
        }
        DataType::List(item_field) => {
            // Nested list: each element of `items` should be a Value::Array.
            // Flatten them and build offsets, then recurse.
            let mut flat_items: Vec<Value> = Vec::new();
            let mut offsets: Vec<i32> = vec![0];
            let mut validity: Vec<bool> = Vec::with_capacity(items.len());
            let mut has_null = false;

            for item in items {
                match item {
                    Value::Array(arr) => {
                        flat_items.extend(arr.iter().cloned());
                        offsets.push(flat_items.len() as i32);
                        validity.push(true);
                    }
                    _ => {
                        offsets.push(*offsets.last().unwrap_or(&0));
                        validity.push(false);
                        has_null = true;
                    }
                }
            }

            let child_array = build_list_item_array(&flat_items, item_field)?;
            let offset_buffer = OffsetBuffer::new(ScalarBuffer::from(offsets));
            let null_buffer = has_null.then(|| NullBuffer::from(validity));

            let list_array = ListArray::try_new(
                Arc::new(item_field.as_ref().clone()),
                offset_buffer,
                child_array,
                null_buffer,
            )
            .map_err(|source| ArrowEncodingError::RecordBatchCreation { source })?;
            Ok(Arc::new(list_array))
        }
        DataType::Map(entries_field, _sorted) => {
            // Each item is a Value::Object representing one row's map.
            // Build offsets + flat key/value arrays across all rows.
            let DataType::Struct(kv_fields) = entries_field.data_type() else {
                return Err(ArrowEncodingError::UnsupportedType {
                    field_name: field.name().clone(),
                    data_type: field.data_type().clone(),
                });
            };
            let key_field = &kv_fields[0];
            let value_field = &kv_fields[1];

            let mut flat_keys: Vec<Value> = Vec::new();
            let mut flat_values: Vec<Value> = Vec::new();
            let mut offsets: Vec<i32> = vec![0];
            let mut validity: Vec<bool> = Vec::with_capacity(items.len());
            let mut has_null = false;
            let mut lossy_keys: u64 = 0;

            for item in items {
                match item {
                    Value::Object(obj) => {
                        for (k, v) in obj.iter() {
                            let (key_val, lossy) =
                                coerce_string_key(k.as_str(), key_field.data_type());
                            if lossy {
                                lossy_keys += 1;
                            }
                            flat_keys.push(key_val);
                            flat_values.push(v.clone());
                        }
                        offsets.push(flat_keys.len() as i32);
                        validity.push(true);
                    }
                    _ => {
                        offsets.push(*offsets.last().unwrap_or(&0));
                        validity.push(false);
                        has_null = true;
                    }
                }
            }
            if lossy_keys > 0 {
                emit_lossy_map_keys(field.name(), lossy_keys);
            }

            let key_array = build_map_value_array(&flat_keys, key_field)?;
            let value_array = build_map_value_array(&flat_values, value_field)?;

            let entries_array =
                StructArray::try_new(kv_fields.clone(), vec![key_array, value_array], None)
                    .map_err(|source| ArrowEncodingError::RecordBatchCreation { source })?;

            let null_buffer = has_null.then(|| NullBuffer::from(validity));
            let map_array = MapArray::try_new(
                Arc::new(entries_field.as_ref().clone()),
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                entries_array,
                null_buffer,
                false,
            )
            .map_err(|source| ArrowEncodingError::RecordBatchCreation { source })?;
            Ok(Arc::new(map_array))
        }
        _ => build_map_value_array(items, field),
    }
}

/// Builds a flat value array for Map entries from pre-collected Vector values.
fn build_map_value_array(values: &[Value], field: &Field) -> Result<ArrayRef, ArrowEncodingError> {
    let nullable = field.is_nullable();
    match field.data_type() {
        DataType::LargeUtf8 => {
            let mut builder = LargeStringBuilder::with_capacity(values.len(), 0);
            for v in values {
                match v {
                    Value::Bytes(b) => match std::str::from_utf8(b) {
                        Ok(s) => builder.append_value(s),
                        Err(_) => builder.append_value(&String::from_utf8_lossy(b)),
                    },
                    _ => builder.append_value(&v.to_string_lossy()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(values.len(), 0);
            for v in values {
                match v {
                    Value::Bytes(b) => match std::str::from_utf8(b) {
                        Ok(s) => builder.append_value(s),
                        Err(_) => builder.append_value(&String::from_utf8_lossy(b)),
                    },
                    _ => builder.append_value(&v.to_string_lossy()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::LargeBinary => {
            let mut builder = LargeBinaryBuilder::with_capacity(values.len(), 0);
            for v in values {
                match v {
                    Value::Bytes(b) => builder.append_value(b),
                    _ => handle_null_constraints!(builder, nullable, field.name()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Binary => {
            let mut builder = BinaryBuilder::with_capacity(values.len(), 0);
            for v in values {
                match v {
                    Value::Bytes(b) => builder.append_value(b),
                    _ => handle_null_constraints!(builder, nullable, field.name()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Boolean(b) => builder.append_value(*b),
                    _ => handle_null_constraints!(builder, nullable, field.name()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Integer(i) if *i >= i32::MIN as i64 && *i <= i32::MAX as i64 => {
                        builder.append_value(*i as i32)
                    }
                    _ => handle_null_constraints!(builder, nullable, field.name()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Integer(i) => builder.append_value(*i),
                    _ => handle_null_constraints!(builder, nullable, field.name()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::UInt32 => {
            let mut builder = UInt32Builder::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Integer(i) if *i >= 0 && *i <= u32::MAX as i64 => {
                        builder.append_value(*i as u32)
                    }
                    _ => handle_null_constraints!(builder, nullable, field.name()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::UInt64 => {
            let mut builder = UInt64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Integer(i) if *i >= 0 => builder.append_value(*i as u64),
                    _ => handle_null_constraints!(builder, nullable, field.name()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float32 => {
            let mut builder = Float32Builder::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Float(f) => builder.append_value(f.into_inner() as f32),
                    Value::Integer(i) => builder.append_value(*i as f32),
                    _ => handle_null_constraints!(builder, nullable, field.name()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Float(f) => builder.append_value(f.into_inner()),
                    Value::Integer(i) => builder.append_value(*i as f64),
                    _ => handle_null_constraints!(builder, nullable, field.name()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Struct(_) | DataType::List(_) | DataType::Map(_, _) => {
            // build_list_item_array handles all three. Map is included so a Map-valued Map builds
            // here instead of erroring, keeping this set equal to what
            // parser::reject_unsupported_nested_type admits.
            build_list_item_array(values, field)
        }
        other => Err(ArrowEncodingError::UnsupportedType {
            field_name: field.name().clone(),
            data_type: other.clone(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{
            Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Float64Array, Int64Array,
            StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
            TimestampNanosecondArray, TimestampSecondArray,
        },
        datatypes::Field,
        ipc::reader::StreamReader,
    };
    use chrono::{TimeZone, Utc};
    use rstest::rstest;
    use std::io::Cursor;
    use vector_core::event::LogEvent;

    #[test]
    fn test_encode_all_types() {
        let mut log = LogEvent::default();
        log.insert("string_field", "test");
        log.insert("int8_field", 127);
        log.insert("int16_field", 32000);
        log.insert("int32_field", 1000000);
        log.insert("int64_field", 42);
        log.insert("float32_field", 3.15);
        log.insert("float64_field", 3.15);
        log.insert("bool_field", true);
        log.insert("bytes_field", bytes::Bytes::from("binary"));
        log.insert("timestamp_field", Utc::now());

        let events = vec![Event::Log(log)];

        let schema = Arc::new(Schema::new(vec![
            Field::new("string_field", DataType::Utf8, true),
            Field::new("int8_field", DataType::Int8, true),
            Field::new("int16_field", DataType::Int16, true),
            Field::new("int32_field", DataType::Int32, true),
            Field::new("int64_field", DataType::Int64, true),
            Field::new("float32_field", DataType::Float32, true),
            Field::new("float64_field", DataType::Float64, true),
            Field::new("bool_field", DataType::Boolean, true),
            Field::new("bytes_field", DataType::Binary, true),
            Field::new(
                "timestamp_field",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
        ]));

        let result = encode_events_to_arrow_ipc_stream(&events, Some(Arc::clone(&schema)));
        assert!(result.is_ok());

        let bytes = result.unwrap();
        let cursor = Cursor::new(bytes);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 10);

        // Verify string field
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "test"
        );

        // Verify int8 field
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .unwrap()
                .value(0),
            127
        );

        // Verify int16 field
        assert_eq!(
            batch
                .column(2)
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .unwrap()
                .value(0),
            32000
        );

        // Verify int32 field
        assert_eq!(
            batch
                .column(3)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap()
                .value(0),
            1000000
        );

        // Verify int64 field
        assert_eq!(
            batch
                .column(4)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            42
        );

        // Verify float32 field
        assert!(
            (batch
                .column(5)
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .unwrap()
                .value(0)
                - 3.15)
                .abs()
                < 0.001
        );

        // Verify float64 field
        assert!(
            (batch
                .column(6)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
                - 3.15)
                .abs()
                < 0.001
        );

        // Verify boolean field
        assert!(
            batch
                .column(7)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0),
            "{}",
            true
        );

        // Verify binary field
        assert_eq!(
            batch
                .column(8)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            b"binary"
        );

        // Verify timestamp field
        assert!(
            !batch
                .column(9)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .is_null(0)
        );
    }

    #[test]
    fn test_encode_low_cardinality_dictionary() {
        use arrow::array::{DictionaryArray, StringArray};

        // Three rows, two distinct values -> dictionary of 2 entries, keys [0,0,1].
        let events: Vec<Event> = ["a", "a", "b"]
            .iter()
            .map(|v| {
                let mut log = LogEvent::default();
                log.insert("lc", *v);
                Event::Log(log)
            })
            .collect();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "lc",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));

        let batch = build_record_batch(schema, &events).expect("dictionary batch builds");
        assert_eq!(batch.num_rows(), 3);
        let dict = batch
            .column(0)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("column is a dictionary array");
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("dictionary values are strings");
        assert_eq!(values.len(), 2); // deduped: "a","b"
        let keys = dict.keys();
        assert_eq!(keys.value(0), keys.value(1)); // both "a"
        assert_ne!(keys.value(0), keys.value(2)); // "a" != "b"
    }

    #[test]
    fn test_coerce_missing_string_to_default() {
        // Missing non-nullable columns coerce to their default ("" for String, "{}" for JSON).
        let mut present = LogEvent::default();
        present.insert("s", "hello");
        let events = vec![Event::Log(present), Event::Log(LogEvent::default())];

        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, false),
            Field::new("j", DataType::Utf8, false),
        ]));
        let defaults = std::collections::HashMap::from([
            ("s".to_string(), String::new()),
            ("j".to_string(), "{}".to_string()),
        ]);

        let batch = build_record_batch_inner(schema, &events, Some(&defaults))
            .expect("coerced batch builds");
        let s = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(s.value(0), "hello");
        assert_eq!(s.value(1), ""); // missing -> String default, not null
        assert!(!s.is_null(1));
        let j = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(j.value(0), "{}"); // JSON column default for a missing value
        assert_eq!(j.value(1), "{}");
    }

    #[test]
    fn test_coerce_lowcardinality_missing_to_default() {
        use arrow::array::{DictionaryArray, StringArray};

        let mut present = LogEvent::default();
        present.insert("lc", "x");
        let events = vec![Event::Log(present), Event::Log(LogEvent::default())];
        let schema = Arc::new(Schema::new(vec![Field::new(
            "lc",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let defaults = std::collections::HashMap::from([("lc".to_string(), String::new())]);

        let batch = build_record_batch_inner(schema, &events, Some(&defaults)).unwrap();
        let dict = batch
            .column(0)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let resolved: Vec<&str> = (0..dict.len())
            .map(|i| values.value(dict.keys().value(i) as usize))
            .collect();
        assert_eq!(resolved, vec!["x", ""]); // missing -> "" through the dictionary
    }

    #[test]
    fn test_coerce_int64_from_string_and_missing() {
        // Numeric string is parsed, integer passes through, missing becomes the default ("0").
        let mut as_string = LogEvent::default();
        as_string.insert("ws", "12345");
        let mut as_int = LogEvent::default();
        as_int.insert("ws", 7);
        let events = vec![
            Event::Log(as_string),
            Event::Log(as_int),
            Event::Log(LogEvent::default()),
        ];
        let schema = Arc::new(Schema::new(vec![Field::new("ws", DataType::Int64, false)]));
        let defaults = std::collections::HashMap::from([("ws".to_string(), "0".to_string())]);

        let batch = build_record_batch_inner(schema, &events, Some(&defaults)).unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(arr.value(0), 12345); // parsed from string
        assert_eq!(arr.value(1), 7); // already an integer
        assert_eq!(arr.value(2), 0); // missing -> default
        assert!(!arr.is_null(2));
    }

    #[test]
    fn test_no_coerce_missing_nonnullable_still_errors() {
        // Without coercion, a missing non-nullable value still errors (unchanged opt-out path).
        let events = vec![Event::Log(LogEvent::default())];
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let result = build_record_batch_inner(schema, &events, None);
        assert!(matches!(
            result,
            Err(ArrowEncodingError::NullConstraint { .. })
        ));
    }

    #[test]
    fn test_coerce_fills_zero_for_all_scalar_types() {
        // With coercion on, a missing value on a non-nullable column of any covered scalar type
        // is filled with the type's zero default instead of failing the whole batch.
        let schema = Arc::new(Schema::new(vec![
            Field::new("i32", DataType::Int32, false),
            Field::new("u64", DataType::UInt64, false),
            Field::new("f64", DataType::Float64, false),
            Field::new("b", DataType::Boolean, false),
            Field::new("d", DataType::Date32, false),
            Field::new("dec", DataType::Decimal128(18, 2), false),
        ]));
        let coerce: HashMap<String, String> = schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), "0".to_string()))
            .collect();

        let events = vec![Event::Log(LogEvent::default())]; // every field missing
        let batch = build_record_batch_inner(Arc::clone(&schema), &events, Some(&coerce))
            .expect("coercion fills defaults instead of erroring");

        assert_eq!(batch.num_rows(), 1);
        for i in 0..schema.fields().len() {
            assert!(
                !batch.column(i).is_null(0),
                "column {i} should be a zero default, not null"
            );
        }
        assert_eq!(
            batch
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            0.0
        );
        assert!(
            !batch
                .column(3)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        );
    }

    #[test]
    fn test_coerce_present_unparseable_int64_errors() {
        // Coercion errors on a present unparseable value but still fills the default when absent.
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let coerce: HashMap<String, String> = HashMap::from([("n".to_string(), "0".to_string())]);

        let mut bad = LogEvent::default();
        bad.insert("n", "abc");
        let result =
            build_record_batch_inner(Arc::clone(&schema), &[Event::Log(bad)], Some(&coerce));
        assert!(matches!(
            result,
            Err(ArrowEncodingError::InvalidValue { .. })
        ));

        // An absent field still fills the default.
        let batch =
            build_record_batch_inner(schema, &[Event::Log(LogEvent::default())], Some(&coerce))
                .expect("absent field fills the default");
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            0
        );
    }

    #[test]
    fn test_coerce_present_out_of_range_errors() {
        // Coercion errors on a present out-of-range value but still fills the default when absent.
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int8, false)]));
        let coerce: HashMap<String, String> = HashMap::from([("n".to_string(), "0".to_string())]);

        let mut big = LogEvent::default();
        big.insert("n", 999); // out of Int8 range
        let result =
            build_record_batch_inner(Arc::clone(&schema), &[Event::Log(big)], Some(&coerce));
        assert!(matches!(
            result,
            Err(ArrowEncodingError::InvalidValue { .. })
        ));

        // An absent field still fills the default (unchanged).
        let batch =
            build_record_batch_inner(schema, &[Event::Log(LogEvent::default())], Some(&coerce))
                .expect("absent field fills the default");
        assert!(!batch.column(0).is_null(0));
    }

    #[test]
    fn test_metadata_stripping_is_wire_invariant() {
        // With coercion off, a schema carrying the coerce marker encodes to the same bytes as a
        // clean schema. The marker is stripped from the wire, not shipped.
        use tokio_util::codec::Encoder;

        let mut log = LogEvent::default();
        log.insert("s", "hi");
        log.insert("n", 7);
        let events = vec![Event::Log(log)];

        let fields = || {
            vec![
                Field::new("s", DataType::Utf8, false),
                Field::new("n", DataType::Int64, false),
            ]
        };

        // Golden: a clean schema with no metadata.
        let golden =
            encode_events_to_arrow_ipc_stream(&events, Some(Arc::new(Schema::new(fields()))))
                .expect("clean schema encodes");

        // The same schema with the coerce marker on every field.
        let marked_schema = Schema::new(
            fields()
                .into_iter()
                .map(|f| {
                    f.with_metadata(HashMap::from([(
                        COERCE_DEFAULT_METADATA_KEY.to_string(),
                        "0".to_string(),
                    )]))
                })
                .collect::<Vec<_>>(),
        );

        // Without stripping, the marker is part of the wire schema, so the bytes differ.
        let unstripped =
            encode_events_to_arrow_ipc_stream(&events, Some(Arc::new(marked_schema.clone())))
                .expect("marked schema encodes");
        assert_ne!(
            unstripped.as_ref(),
            golden.as_ref(),
            "marker should be on the wire when not stripped"
        );

        // The serializer (coercion off by default) strips it and reproduces the clean wire.
        let config = ArrowStreamSerializerConfig::new(marked_schema);
        let mut serializer = ArrowStreamSerializer::new(config).expect("serializer builds");
        let mut buffer = BytesMut::new();
        serializer
            .encode(events.clone(), &mut buffer)
            .expect("serializer encodes");
        assert_eq!(
            buffer.as_ref(),
            golden.as_ref(),
            "stripping the marker must yield the clean wire"
        );
    }

    #[test]
    fn test_serializer_builds_coerce_map_and_strips_metadata() {
        // new() reads the default from field metadata, strips the metadata, and applies it.
        let field =
            Field::new("j", DataType::Utf8, false).with_metadata(std::collections::HashMap::from(
                [(COERCE_DEFAULT_METADATA_KEY.to_string(), "{}".to_string())],
            ));
        let mut config = ArrowStreamSerializerConfig::new(Schema::new(vec![field]));
        config.coerce_missing_to_default = true;

        let serializer = ArrowStreamSerializer::new(config).expect("serializer builds");
        let batch = serializer
            .encode_to_record_batch(&[Event::Log(LogEvent::default())])
            .expect("encodes with coercion");

        // Internal marker metadata is not written to the wire schema.
        assert!(batch.schema().field(0).metadata().is_empty());
        // The missing JSON column is coerced to its "{}" default.
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "{}");
    }

    #[test]
    fn test_encode_null_values() {
        let mut log1 = LogEvent::default();
        log1.insert("field_a", 1);
        // field_b is missing

        let mut log2 = LogEvent::default();
        log2.insert("field_b", 2);
        // field_a is missing

        let events = vec![Event::Log(log1), Event::Log(log2)];

        let schema = Arc::new(Schema::new(vec![
            Field::new("field_a", DataType::Int64, true),
            Field::new("field_b", DataType::Int64, true),
        ]));

        let result = encode_events_to_arrow_ipc_stream(&events, Some(Arc::clone(&schema)));
        assert!(result.is_ok());

        let bytes = result.unwrap();
        let cursor = Cursor::new(bytes);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 2);

        let field_a = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(field_a.value(0), 1);
        assert!(field_a.is_null(1));

        let field_b = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(field_b.is_null(0));
        assert_eq!(field_b.value(1), 2);
    }

    #[test]
    fn test_encode_type_mismatches() {
        let mut log1 = LogEvent::default();
        log1.insert("field", 42); // Integer

        let mut log2 = LogEvent::default();
        log2.insert("field", 3.15); // Float - type mismatch!

        let events = vec![Event::Log(log1), Event::Log(log2)];

        // Schema expects Int64
        let schema = Arc::new(Schema::new(vec![Field::new(
            "field",
            DataType::Int64,
            true,
        )]));

        let result = encode_events_to_arrow_ipc_stream(&events, Some(Arc::clone(&schema)));
        assert!(result.is_ok());

        let bytes = result.unwrap();
        let cursor = Cursor::new(bytes);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 2);

        let field_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(field_array.value(0), 42);
        assert!(field_array.is_null(1)); // Type mismatch becomes null
    }

    #[test]
    fn test_encode_complex_json_values() {
        use serde_json::json;

        let mut log = LogEvent::default();
        log.insert(
            "object_field",
            json!({"key": "value", "nested": {"count": 42}}),
        );
        log.insert("array_field", json!([1, 2, 3]));

        let events = vec![Event::Log(log)];

        let schema = Arc::new(Schema::new(vec![
            Field::new("object_field", DataType::Utf8, true),
            Field::new("array_field", DataType::Utf8, true),
        ]));

        let result = encode_events_to_arrow_ipc_stream(&events, Some(Arc::clone(&schema)));
        assert!(result.is_ok());

        let bytes = result.unwrap();
        let cursor = Cursor::new(bytes);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 1);

        let object_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let object_str = object_array.value(0);
        assert!(object_str.contains("key"));
        assert!(object_str.contains("value"));

        let array_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let array_str = array_array.value(0);
        assert_eq!(array_str, "[1,2,3]");
    }

    #[test]
    fn test_encode_unsupported_type() {
        let mut log = LogEvent::default();
        log.insert("field", "value");

        let events = vec![Event::Log(log)];

        // Use an unsupported type
        let schema = Arc::new(Schema::new(vec![Field::new(
            "field",
            DataType::Duration(TimeUnit::Millisecond),
            true,
        )]));

        let result = encode_events_to_arrow_ipc_stream(&events, Some(schema));
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ArrowEncodingError::UnsupportedType { .. }
        ));
    }

    #[test]
    fn test_encode_without_schema_fails() {
        let mut log1 = LogEvent::default();
        log1.insert("message", "hello");

        let events = vec![Event::Log(log1)];

        let result = encode_events_to_arrow_ipc_stream(&events, None);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ArrowEncodingError::NoSchemaProvided
        ));
    }

    #[test]
    fn test_encode_empty_events() {
        let events: Vec<Event> = vec![];
        let result = encode_events_to_arrow_ipc_stream(&events, None);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ArrowEncodingError::NoEvents));
    }

    #[test]
    fn test_encode_timestamp_precisions() {
        let now = Utc::now();
        let mut log = LogEvent::default();
        log.insert("ts_second", now);
        log.insert("ts_milli", now);
        log.insert("ts_micro", now);
        log.insert("ts_nano", now);

        let events = vec![Event::Log(log)];

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts_second",
                DataType::Timestamp(TimeUnit::Second, None),
                true,
            ),
            Field::new(
                "ts_milli",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new(
                "ts_micro",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(
                "ts_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ]));

        let result = encode_events_to_arrow_ipc_stream(&events, Some(Arc::clone(&schema)));
        assert!(result.is_ok());

        let bytes = result.unwrap();
        let cursor = Cursor::new(bytes);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 4);

        let ts_second = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .unwrap();
        assert!(!ts_second.is_null(0));
        assert_eq!(ts_second.value(0), now.timestamp());

        let ts_milli = batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert!(!ts_milli.is_null(0));
        assert_eq!(ts_milli.value(0), now.timestamp_millis());

        let ts_micro = batch
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert!(!ts_micro.is_null(0));
        assert_eq!(ts_micro.value(0), now.timestamp_micros());

        let ts_nano = batch
            .column(3)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert!(!ts_nano.is_null(0));
        assert_eq!(ts_nano.value(0), now.timestamp_nanos_opt().unwrap());
    }

    // 2026-05-19 is 20_592 days since the 1970-01-01 UTC epoch: used as the
    // single fixed reference point for the date-encoding tests below.
    const EXPECTED_DAYS_SINCE_EPOCH: i32 = 20_592;
    const EXPECTED_MS_SINCE_EPOCH: i64 = 20_592 * 86_400_000;

    // Materializes the test date as one of the supported VRL input forms,
    // then inserts it into a log event under "d". The "integer" form is
    // expressed in the target Arrow date type's native unit: days for
    // Date32, milliseconds for Date64.
    fn insert_test_date(log: &mut LogEvent, kind: &str, target: &DataType) {
        let date = Utc.with_ymd_and_hms(2026, 5, 19, 13, 27, 52).unwrap();
        match (kind, target) {
            ("timestamp", _) => log.insert("d", date),
            ("string", _) => log.insert("d", "2026-05-19T13:27:52Z"),
            ("integer", DataType::Date32) => log.insert("d", i64::from(EXPECTED_DAYS_SINCE_EPOCH)),
            ("integer", DataType::Date64) => log.insert("d", EXPECTED_MS_SINCE_EPOCH),
            _ => unreachable!("unknown input kind {kind} for {target:?}"),
        };
    }

    // Round-trips a single-event batch through encode + StreamReader and returns
    // the decoded batch. Used by both date32 and date64 tests.
    fn encode_single_event(field: Field, log: LogEvent) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![field]));
        let bytes = encode_events_to_arrow_ipc_stream(&[Event::Log(log)], Some(schema))
            .expect("encoding should succeed");
        StreamReader::try_new(Cursor::new(bytes), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
    }

    // Test plan: verify that build_date32_array converts every supported VRL
    // input form (native timestamp, RFC 3339 string, integer days-since-epoch)
    // into the same Arrow Date32 value — 20_588 days since 1970-01-01 UTC.
    #[rstest]
    #[case::timestamp("timestamp")]
    #[case::rfc3339_string("string")]
    #[case::integer_days("integer")]
    fn test_encode_date32_from_input(#[case] kind: &str) {
        let mut log = LogEvent::default();
        insert_test_date(&mut log, kind, &DataType::Date32);

        let batch = encode_single_event(Field::new("d", DataType::Date32, true), log);
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("column should be a Date32Array");
        assert_eq!(array.value(0), EXPECTED_DAYS_SINCE_EPOCH);
    }

    // Test plan: verify that build_date64_array converts every supported VRL
    // input form (native timestamp, RFC 3339 string, integer millis-since-epoch)
    // into the same Arrow Date64 value: 20_588 * 86_400_000 ms.
    #[rstest]
    #[case::timestamp("timestamp")]
    #[case::rfc3339_string("string")]
    #[case::integer_millis("integer")]
    fn test_encode_date64_from_input(#[case] kind: &str) {
        let mut log = LogEvent::default();
        insert_test_date(&mut log, kind, &DataType::Date64);

        let batch = encode_single_event(Field::new("d", DataType::Date64, true), log);
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Date64Array>()
            .expect("column should be a Date64Array");
        assert_eq!(array.value(0), EXPECTED_MS_SINCE_EPOCH);
    }

    // Test plan: verify that a missing value on a nullable Date32/Date64 column
    // encodes as an explicit null in the resulting array, and on a non-nullable
    // column raises NullConstraint naming the offending field.
    #[rstest]
    #[case::date32_nullable(DataType::Date32)]
    #[case::date64_nullable(DataType::Date64)]
    fn test_encode_date_nullable_missing_value_is_null(#[case] data_type: DataType) {
        let batch = encode_single_event(
            Field::new("d", data_type, /* nullable */ true),
            LogEvent::default(),
        );
        assert_eq!(batch.num_rows(), 1);
        assert!(batch.column(0).is_null(0));
    }

    #[rstest]
    #[case::date32_non_nullable(DataType::Date32)]
    #[case::date64_non_nullable(DataType::Date64)]
    fn test_encode_date_non_nullable_missing_value_errors(#[case] data_type: DataType) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "d", data_type, /* nullable */ false,
        )]));
        let result =
            encode_events_to_arrow_ipc_stream(&[Event::Log(LogEvent::default())], Some(schema));
        match result {
            Err(ArrowEncodingError::NullConstraint { field_name }) => {
                assert_eq!(field_name, "d");
            }
            other => panic!("expected NullConstraint error, got {other:?}"),
        }
    }

    #[rstest]
    #[case::date32(
        DataType::Date32,
        i64::from(EXPECTED_DAYS_SINCE_EPOCH - 1),
        i64::from(EXPECTED_DAYS_SINCE_EPOCH),
    )]
    #[case::date64(
        DataType::Date64,
        (EXPECTED_DAYS_SINCE_EPOCH as i64 - 1) * 86_400_000,
        EXPECTED_MS_SINCE_EPOCH,
    )]
    fn test_encode_date_multi_event_batch(
        #[case] data_type: DataType,
        #[case] expected_prev_day: i64,
        #[case] expected_target_day: i64,
    ) {
        let mut log_ts = LogEvent::default();
        insert_test_date(&mut log_ts, "timestamp", &data_type);

        let mut log_string = LogEvent::default();
        log_string.insert("d", "2026-05-18T00:00:00Z");

        let mut log_integer = LogEvent::default();
        insert_test_date(&mut log_integer, "integer", &data_type);

        let log_missing = LogEvent::default();

        let events = vec![
            Event::Log(log_ts),
            Event::Log(log_string),
            Event::Log(log_integer),
            Event::Log(log_missing),
        ];
        let schema = Arc::new(Schema::new(vec![Field::new("d", data_type.clone(), true)]));
        let bytes = encode_events_to_arrow_ipc_stream(&events, Some(schema))
            .expect("encoding should succeed");
        let batch = StreamReader::try_new(Cursor::new(bytes), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(batch.num_rows(), 4);
        let column = batch.column(0);

        let row_values: Vec<Option<i64>> = match data_type {
            DataType::Date32 => {
                let array = column.as_any().downcast_ref::<Date32Array>().unwrap();
                (0..array.len())
                    .map(|i| (!array.is_null(i)).then(|| i64::from(array.value(i))))
                    .collect()
            }
            DataType::Date64 => {
                let array = column.as_any().downcast_ref::<Date64Array>().unwrap();
                (0..array.len())
                    .map(|i| (!array.is_null(i)).then(|| array.value(i)))
                    .collect()
            }
            _ => unreachable!(),
        };

        assert_eq!(
            row_values,
            vec![
                Some(expected_target_day),
                Some(expected_prev_day),
                Some(expected_target_day),
                None,
            ]
        );
    }

    #[test]
    fn test_encode_mixed_timestamp_string_and_native() {
        // Test mixing string timestamps with native Timestamp values
        let mut log1 = LogEvent::default();
        log1.insert("ts", "2025-10-22T10:18:44.256Z"); // String

        let mut log2 = LogEvent::default();
        log2.insert("ts", Utc::now()); // Native Timestamp

        let mut log3 = LogEvent::default();
        log3.insert("ts", 1729594724256000000_i64); // Integer (nanoseconds)

        let events = vec![Event::Log(log1), Event::Log(log2), Event::Log(log3)];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));

        let result = encode_events_to_arrow_ipc_stream(&events, Some(Arc::clone(&schema)));
        assert!(result.is_ok());

        let bytes = result.unwrap();
        let cursor = Cursor::new(bytes);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 3);

        let ts_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();

        // All three should be non-null
        assert!(!ts_array.is_null(0));
        assert!(!ts_array.is_null(1));
        assert!(!ts_array.is_null(2));

        // First one should match the parsed string
        let expected = chrono::DateTime::parse_from_rfc3339("2025-10-22T10:18:44.256Z")
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap();
        assert_eq!(ts_array.value(0), expected);

        // Third one should match the integer
        assert_eq!(ts_array.value(2), 1729594724256000000_i64);
    }

    #[test]
    fn test_encode_invalid_string_timestamp() {
        // Test that invalid timestamp strings become null
        let mut log1 = LogEvent::default();
        log1.insert("timestamp", "not-a-timestamp");

        let mut log2 = LogEvent::default();
        log2.insert("timestamp", "2025-10-22T10:18:44.256Z"); // Valid

        let mut log3 = LogEvent::default();
        log3.insert("timestamp", "2025-99-99T99:99:99Z"); // Invalid

        let events = vec![Event::Log(log1), Event::Log(log2), Event::Log(log3)];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));

        let result = encode_events_to_arrow_ipc_stream(&events, Some(Arc::clone(&schema)));
        assert!(result.is_ok());

        let bytes = result.unwrap();
        let cursor = Cursor::new(bytes);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 3);

        let ts_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();

        // Invalid timestamps should be null
        assert!(ts_array.is_null(0));
        assert!(!ts_array.is_null(1)); // Valid one
        assert!(ts_array.is_null(2));
    }

    #[test]
    fn test_encode_decimal128_from_integer() {
        use arrow::array::Decimal128Array;

        let mut log = LogEvent::default();
        // Store quantity as integer: 1000
        log.insert("quantity", 1000_i64);

        let events = vec![Event::Log(log)];

        // Decimal(10, 3) - will represent 1000 as 1000.000
        let schema = Arc::new(Schema::new(vec![Field::new(
            "quantity",
            DataType::Decimal128(10, 3),
            true,
        )]));

        let result = encode_events_to_arrow_ipc_stream(&events, Some(Arc::clone(&schema)));
        assert!(result.is_ok());

        let bytes = result.unwrap();
        let cursor = Cursor::new(bytes);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 1);

        let decimal_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();

        assert!(!decimal_array.is_null(0));
        // 1000 with scale 3 = 1000 * 10^3 = 1000000
        assert_eq!(decimal_array.value(0), 1000000_i128);
    }

    #[test]
    fn test_encode_decimal256() {
        use arrow::array::Decimal256Array;

        let mut log = LogEvent::default();
        // Very large precision number
        log.insert("big_value", 123456789.123456_f64);

        let events = vec![Event::Log(log)];

        // Decimal256(50, 6) - high precision decimal
        let schema = Arc::new(Schema::new(vec![Field::new(
            "big_value",
            DataType::Decimal256(50, 6),
            true,
        )]));

        let result = encode_events_to_arrow_ipc_stream(&events, Some(Arc::clone(&schema)));
        assert!(result.is_ok());

        let bytes = result.unwrap();
        let cursor = Cursor::new(bytes);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 1);

        let decimal_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal256Array>()
            .unwrap();

        assert!(!decimal_array.is_null(0));
        // Value should be non-null and encoded
        let value = decimal_array.value(0);
        assert!(value.to_i128().is_some());
    }

    #[test]
    fn test_encode_decimal_null_values() {
        use arrow::array::Decimal128Array;

        let mut log1 = LogEvent::default();
        log1.insert("price", 99.99_f64);

        let log2 = LogEvent::default();
        // No price field - should be null

        let mut log3 = LogEvent::default();
        log3.insert("price", 50.00_f64);

        let events = vec![Event::Log(log1), Event::Log(log2), Event::Log(log3)];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Decimal128(10, 2),
            true,
        )]));

        let result = encode_events_to_arrow_ipc_stream(&events, Some(Arc::clone(&schema)));
        assert!(result.is_ok());

        let bytes = result.unwrap();
        let cursor = Cursor::new(bytes);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 3);

        let decimal_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();

        // First row: 99.99
        assert!(!decimal_array.is_null(0));
        assert_eq!(decimal_array.value(0), 9999_i128);

        // Second row: null
        assert!(decimal_array.is_null(1));

        // Third row: 50.00
        assert!(!decimal_array.is_null(2));
        assert_eq!(decimal_array.value(2), 5000_i128);
    }

    #[test]
    fn test_encode_unsigned_integer_types() {
        use arrow::array::{UInt8Array, UInt16Array, UInt32Array, UInt64Array};

        let mut log = LogEvent::default();
        log.insert("uint8_field", 255_i64);
        log.insert("uint16_field", 65535_i64);
        log.insert("uint32_field", 4294967295_i64);
        log.insert("uint64_field", 9223372036854775807_i64);

        let events = vec![Event::Log(log)];

        let schema = Arc::new(Schema::new(vec![
            Field::new("uint8_field", DataType::UInt8, true),
            Field::new("uint16_field", DataType::UInt16, true),
            Field::new("uint32_field", DataType::UInt32, true),
            Field::new("uint64_field", DataType::UInt64, true),
        ]));

        let result = encode_events_to_arrow_ipc_stream(&events, Some(Arc::clone(&schema)));
        assert!(result.is_ok());

        let bytes = result.unwrap();
        let cursor = Cursor::new(bytes);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 4);

        // Verify uint8
        let uint8_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        assert_eq!(uint8_array.value(0), 255_u8);

        // Verify uint16
        let uint16_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap();
        assert_eq!(uint16_array.value(0), 65535_u16);

        // Verify uint32
        let uint32_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(uint32_array.value(0), 4294967295_u32);

        // Verify uint64
        let uint64_array = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(uint64_array.value(0), 9223372036854775807_u64);
    }

    #[test]
    fn test_encode_unsigned_integers_with_null_and_overflow() {
        use arrow::array::{UInt8Array, UInt32Array};

        let mut log1 = LogEvent::default();
        log1.insert("uint8_field", 100_i64);
        log1.insert("uint32_field", 1000_i64);

        let mut log2 = LogEvent::default();
        log2.insert("uint8_field", 300_i64); // Overflow - should be null
        log2.insert("uint32_field", -1_i64); // Negative - should be null

        let log3 = LogEvent::default();
        // Missing fields - should be null

        let events = vec![Event::Log(log1), Event::Log(log2), Event::Log(log3)];

        let schema = Arc::new(Schema::new(vec![
            Field::new("uint8_field", DataType::UInt8, true),
            Field::new("uint32_field", DataType::UInt32, true),
        ]));

        let result = encode_events_to_arrow_ipc_stream(&events, Some(Arc::clone(&schema)));
        assert!(result.is_ok());

        let bytes = result.unwrap();
        let cursor = Cursor::new(bytes);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 3);

        // Check uint8 column
        let uint8_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        assert_eq!(uint8_array.value(0), 100_u8); // Valid
        assert!(uint8_array.is_null(1)); // Overflow
        assert!(uint8_array.is_null(2)); // Missing

        // Check uint32 column
        let uint32_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(uint32_array.value(0), 1000_u32); // Valid
        assert!(uint32_array.is_null(1)); // Negative
        assert!(uint32_array.is_null(2)); // Missing
    }

    #[test]
    fn test_encode_non_nullable_field_with_null_value() {
        // Test that encoding fails when a non-nullable field encounters a null value
        let mut log1 = LogEvent::default();
        log1.insert("required_field", 42);

        let log2 = LogEvent::default();
        // log2 is missing required_field - should cause an error

        let events = vec![Event::Log(log1), Event::Log(log2)];

        // Create schema with non-nullable field
        let schema = Arc::new(Schema::new(vec![Field::new(
            "required_field",
            DataType::Int64,
            false, // Not nullable
        )]));

        let result = encode_events_to_arrow_ipc_stream(&events, Some(schema));
        assert!(result.is_err());

        match result.unwrap_err() {
            ArrowEncodingError::NullConstraint { field_name } => {
                assert_eq!(field_name, "required_field");
            }
            other => panic!("Expected NullConstraint error, got: {:?}", other),
        }
    }

    #[test]
    fn test_encode_non_nullable_string_field_with_missing_value() {
        // Test that encoding fails for non-nullable string field
        let mut log1 = LogEvent::default();
        log1.insert("name", "Alice");

        let mut log2 = LogEvent::default();
        log2.insert("name", "Bob");

        let log3 = LogEvent::default();
        // log3 is missing name field

        let events = vec![Event::Log(log1), Event::Log(log2), Event::Log(log3)];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "name",
            DataType::Utf8,
            false, // Not nullable
        )]));

        let result = encode_events_to_arrow_ipc_stream(&events, Some(schema));
        assert!(result.is_err());

        match result.unwrap_err() {
            ArrowEncodingError::NullConstraint { field_name } => {
                assert_eq!(field_name, "name");
            }
            other => panic!("Expected NullConstraint error, got: {:?}", other),
        }
    }

    #[test]
    fn test_encode_nullable_string_field_with_explicit_null() {
        // Regression: a present-but-null VRL value used to be stringified to
        // the literal "<null>" via Value::to_string_lossy. It must become a
        // real Arrow null instead.
        let mut log1 = LogEvent::default();
        log1.insert("name", "Alice");

        let mut log2 = LogEvent::default();
        log2.insert("name", Value::Null);

        let events = vec![Event::Log(log1), Event::Log(log2)];

        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));

        let bytes = encode_events_to_arrow_ipc_stream(&events, Some(schema)).unwrap();
        let cursor = Cursor::new(bytes);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let batch = reader.next().unwrap().unwrap();

        let name_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_array.value(0), "Alice");
        assert!(name_array.is_null(1));
    }

    #[test]
    fn test_encode_nullable_large_string_field_with_explicit_null() {
        use arrow::array::LargeStringArray;

        let mut log1 = LogEvent::default();
        log1.insert("name", "Alice");

        let mut log2 = LogEvent::default();
        log2.insert("name", Value::Null);

        let events = vec![Event::Log(log1), Event::Log(log2)];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "name",
            DataType::LargeUtf8,
            true,
        )]));

        let bytes = encode_events_to_arrow_ipc_stream(&events, Some(schema)).unwrap();
        let cursor = Cursor::new(bytes);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let batch = reader.next().unwrap().unwrap();

        let name_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        assert_eq!(name_array.value(0), "Alice");
        assert!(name_array.is_null(1));
    }

    #[test]
    fn test_encode_non_nullable_string_field_with_explicit_null_errors() {
        let mut log1 = LogEvent::default();
        log1.insert("name", "Alice");

        let mut log2 = LogEvent::default();
        log2.insert("name", Value::Null);

        let events = vec![Event::Log(log1), Event::Log(log2)];

        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));

        let result = encode_events_to_arrow_ipc_stream(&events, Some(schema));
        match result.unwrap_err() {
            ArrowEncodingError::NullConstraint { field_name } => {
                assert_eq!(field_name, "name");
            }
            other => panic!("Expected NullConstraint error, got: {:?}", other),
        }
    }

    #[test]
    fn test_encode_non_nullable_field_all_values_present() {
        // Test that encoding succeeds when all values are present for non-nullable field
        let mut log1 = LogEvent::default();
        log1.insert("id", 1);

        let mut log2 = LogEvent::default();
        log2.insert("id", 2);

        let mut log3 = LogEvent::default();
        log3.insert("id", 3);

        let events = vec![Event::Log(log1), Event::Log(log2), Event::Log(log3)];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false, // Not nullable
        )]));

        let result = encode_events_to_arrow_ipc_stream(&events, Some(Arc::clone(&schema)));
        assert!(result.is_ok());

        let bytes = result.unwrap();
        let cursor = Cursor::new(bytes);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 3);

        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);
        assert_eq!(id_array.value(2), 3);
        assert!(!id_array.is_null(0));
        assert!(!id_array.is_null(1));
        assert!(!id_array.is_null(2));
    }

    #[test]
    fn test_config_allow_nullable_fields_overrides_schema() {
        use tokio_util::codec::Encoder;

        // Create events: One valid, one missing the "required" field
        let mut log1 = LogEvent::default();
        log1.insert("strict_field", 42);
        let log2 = LogEvent::default();
        let events = vec![Event::Log(log1), Event::Log(log2)];

        let schema = Schema::new(vec![Field::new("strict_field", DataType::Int64, false)]);

        let mut config = ArrowStreamSerializerConfig::new(schema);
        config.allow_nullable_fields = true;

        let mut serializer =
            ArrowStreamSerializer::new(config).expect("Failed to create serializer");

        let mut buffer = BytesMut::new();
        serializer
            .encode(events, &mut buffer)
            .expect("Encoding should succeed when allow_nullable_fields is true");

        let cursor = Cursor::new(buffer);
        let mut reader = StreamReader::try_new(cursor, None).expect("Failed to create reader");
        let batch = reader.next().unwrap().expect("Failed to read batch");

        assert_eq!(batch.num_rows(), 2);

        let binding = batch.schema();
        let output_field = binding.field(0);
        assert!(
            output_field.is_nullable(),
            "The output schema field should have been transformed to nullable=true"
        );

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(array.value(0), 42);
        assert!(!array.is_null(0));
        assert!(
            array.is_null(1),
            "The missing value should be encoded as null"
        );
    }

    #[test]
    fn test_make_field_nullable_with_nested_types() {
        // Test that make_field_nullable recursively handles List and Struct types

        // Create a nested structure: Struct containing a List of Structs
        // struct { inner_list: [{ nested_field: Int64 }] }
        let inner_struct_field = Field::new("nested_field", DataType::Int64, false);
        let inner_struct =
            DataType::Struct(arrow::datatypes::Fields::from(vec![inner_struct_field]));
        let list_field = Field::new("item", inner_struct, false);
        let list_type = DataType::List(Arc::new(list_field));
        let outer_field = Field::new("inner_list", list_type, false);
        let outer_struct = DataType::Struct(arrow::datatypes::Fields::from(vec![outer_field]));

        let original_field = Field::new("root", outer_struct, false);

        // Apply make_field_nullable
        let nullable_field = make_field_nullable(&original_field);

        // Verify root field is nullable
        assert!(
            nullable_field.is_nullable(),
            "Root field should be nullable"
        );

        // Verify nested struct is nullable
        if let DataType::Struct(root_fields) = nullable_field.data_type() {
            let inner_list_field = &root_fields[0];
            assert!(
                inner_list_field.is_nullable(),
                "inner_list field should be nullable"
            );

            // Verify list element is nullable
            if let DataType::List(list_item_field) = inner_list_field.data_type() {
                assert!(
                    list_item_field.is_nullable(),
                    "List item field should be nullable"
                );

                // Verify inner struct fields are nullable
                if let DataType::Struct(inner_struct_fields) = list_item_field.data_type() {
                    let nested_field = &inner_struct_fields[0];
                    assert!(
                        nested_field.is_nullable(),
                        "nested_field should be nullable"
                    );
                } else {
                    panic!("Expected Struct type for list items");
                }
            } else {
                panic!("Expected List type for inner_list");
            }
        } else {
            panic!("Expected Struct type for root field");
        }
    }

    #[test]
    fn test_make_field_nullable_with_map_type() {
        // Test that make_field_nullable handles Map types
        // Map is internally represented as List<Struct<key, value>>

        // Create a map: Map<Utf8, Int64>
        // Internally: List<Struct<entries: {key: Utf8, value: Int64}>>
        let key_field = Field::new("key", DataType::Utf8, false);
        let value_field = Field::new("value", DataType::Int64, false);
        let entries_struct =
            DataType::Struct(arrow::datatypes::Fields::from(vec![key_field, value_field]));
        let entries_field = Field::new("entries", entries_struct, false);
        let map_type = DataType::Map(Arc::new(entries_field), false);

        let original_field = Field::new("my_map", map_type, false);

        // Apply make_field_nullable
        let nullable_field = make_field_nullable(&original_field);

        // Verify root field is nullable
        assert!(
            nullable_field.is_nullable(),
            "Root map field should be nullable"
        );

        // Verify map entries are nullable
        if let DataType::Map(entries_field, _sorted) = nullable_field.data_type() {
            assert!(
                entries_field.is_nullable(),
                "Map entries field should be nullable"
            );

            // Verify the struct inside the map is nullable
            if let DataType::Struct(struct_fields) = entries_field.data_type() {
                let key_field = &struct_fields[0];
                let value_field = &struct_fields[1];
                assert!(key_field.is_nullable(), "Map key field should be nullable");
                assert!(
                    value_field.is_nullable(),
                    "Map value field should be nullable"
                );
            } else {
                panic!("Expected Struct type for map entries");
            }
        } else {
            panic!("Expected Map type for my_map field");
        }
    }

    // -------------------------------------------------------------------------
    // Struct encoding tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_encode_struct_flat() {
        use arrow::array::{LargeStringArray, StructArray};

        let mut log = LogEvent::default();
        log.insert("person.name", "Alice");
        log.insert("person.age", 30_i64);

        let events = vec![Event::Log(log)];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "person",
            DataType::Struct(Fields::from(vec![
                Field::new("name", DataType::LargeUtf8, true),
                Field::new("age", DataType::Int64, true),
            ])),
            true,
        )]));

        let result = build_record_batch(Arc::clone(&schema), &events);
        assert!(result.is_ok(), "build_record_batch failed: {:?}", result);
        let batch = result.unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);

        let struct_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(!struct_col.is_null(0));

        let name_col = struct_col
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "Alice");

        let age_col = struct_col
            .column_by_name("age")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(age_col.value(0), 30);
    }

    #[test]
    fn test_encode_struct_nested() {
        use arrow::array::StructArray;

        let mut log = LogEvent::default();
        log.insert("outer.inner.value", 42_i64);
        log.insert("outer.label", "top");

        let events = vec![Event::Log(log)];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "outer",
            DataType::Struct(Fields::from(vec![
                Field::new(
                    "inner",
                    DataType::Struct(Fields::from(vec![Field::new(
                        "value",
                        DataType::Int64,
                        true,
                    )])),
                    true,
                ),
                Field::new("label", DataType::LargeUtf8, true),
            ])),
            true,
        )]));

        let result = build_record_batch(Arc::clone(&schema), &events);
        assert!(result.is_ok(), "build_record_batch failed: {:?}", result);
        let batch = result.unwrap();

        let outer = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(!outer.is_null(0));

        let inner = outer
            .column_by_name("inner")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(!inner.is_null(0));

        let value_col = inner
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(value_col.value(0), 42);
    }

    #[test]
    fn test_encode_struct_null_rows() {
        use arrow::array::StructArray;

        // Row 0: struct present; row 1: struct absent (null).
        let mut log1 = LogEvent::default();
        log1.insert("meta.key", "v1");

        let log2 = LogEvent::default(); // meta absent

        let events = vec![Event::Log(log1), Event::Log(log2)];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "meta",
            DataType::Struct(Fields::from(vec![Field::new(
                "key",
                DataType::LargeUtf8,
                true,
            )])),
            true, // nullable
        )]));

        let result = build_record_batch(Arc::clone(&schema), &events);
        assert!(result.is_ok(), "build_record_batch failed: {:?}", result);
        let batch = result.unwrap();

        let struct_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(!struct_col.is_null(0), "row 0 should be non-null");
        assert!(struct_col.is_null(1), "row 1 should be null");
    }

    #[test]
    fn test_encode_struct_non_nullable_missing_fails() {
        let log = LogEvent::default(); // struct field absent
        let events = vec![Event::Log(log)];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "required",
            DataType::Struct(Fields::from(vec![Field::new("x", DataType::Int64, true)])),
            false, // non-nullable
        )]));

        let result = build_record_batch(Arc::clone(&schema), &events);
        assert!(matches!(
            result,
            Err(ArrowEncodingError::NullConstraint { .. })
        ));
    }

    #[test]
    fn test_encode_struct_present_non_object_coerces_to_defaults() {
        use arrow::array::StructArray;
        // A present non-object value coerces to a struct of child defaults.
        let mut log = LogEvent::default();
        log.insert("required", "not-a-struct");
        let events = vec![Event::Log(log)];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "required",
            DataType::Struct(Fields::from(vec![Field::new("x", DataType::Int64, true)])),
            false,
        )]));
        let coerce = std::collections::HashMap::from([("required".to_string(), String::new())]);

        let batch = build_record_batch_inner(Arc::clone(&schema), &events, Some(&coerce)).expect(
            "present non-object struct should coerce to child defaults, not fail the batch",
        );
        let s = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(
            !s.is_null(0),
            "coerced row should be a valid struct of child defaults"
        );
    }

    #[test]
    fn test_encode_struct_non_nullable_int_child_missing_coerces_to_zero() {
        use arrow::array::{Int64Array, StructArray};
        // An absent non-nullable Int64 struct child must coerce to 0, not fail the batch: the "" the
        // parent forwards as its default must reach build_int64_array as 0, not NullConstraint.
        let mut log = LogEvent::default();
        log.insert("required.name", "alice"); // present child
        // "required.count" (the non-nullable Int64 child) is intentionally absent.
        let events = vec![Event::Log(log)];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "required",
            DataType::Struct(Fields::from(vec![
                Field::new("name", DataType::Utf8, true),
                Field::new("count", DataType::Int64, false),
            ])),
            false,
        )]));
        let coerce = std::collections::HashMap::from([("required".to_string(), String::new())]);

        let batch = build_record_batch_inner(Arc::clone(&schema), &events, Some(&coerce))
            .expect("absent non-nullable int child should coerce to 0, not fail the batch");
        let s = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(!s.is_null(0));
        let count = s
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            count.value(0),
            0,
            "missing non-nullable int child should be 0"
        );
    }

    #[test]
    fn test_encode_map_of_list_roundtrip() {
        use arrow::array::MapArray;
        use serde_json::json;
        // Map<Utf8, List<Int64>> round-trip across multiple rows, including an empty inner list.
        // Exercises the recursive collection-value path that scalar-only map tests do not.
        let item = Field::new("item", DataType::Int64, true);
        let entries = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::List(Arc::new(item)), true),
            ])),
            false,
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "buckets",
            DataType::Map(Arc::new(entries), false),
            true,
        )]));

        let mut log0 = LogEvent::default();
        log0.insert("buckets", json!({"a": [1, 2], "b": []})); // "b" is an empty inner list
        let mut log1 = LogEvent::default();
        log1.insert("buckets", json!({"c": [3]}));
        let events = vec![Event::Log(log0), Event::Log(log1)];

        let batch = build_record_batch(Arc::clone(&schema), &events)
            .expect("Map<Utf8,List<Int64>> should encode");
        let map = batch.column(0).as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map.value_length(0), 2, "row 0 has two keys (a, b)");
        assert_eq!(map.value_length(1), 1, "row 1 has one key (c)");

        // IPC round-trip: the whole batch must serialize and read back with the same shape.
        let ipc = encode_events_to_arrow_ipc_stream(&events, Some(Arc::clone(&schema)))
            .expect("IPC serialize");
        let mut reader = StreamReader::try_new(Cursor::new(ipc), None).expect("IPC reader");
        let read_back = reader.next().expect("one batch").expect("valid batch");
        assert_eq!(read_back.num_rows(), 2);
    }

    #[test]
    fn test_encode_map_of_map_roundtrip() {
        use arrow::array::MapArray;
        use serde_json::json;
        // A Map used as a map value: build_map_value_array must build the inner Map, so the Map that
        // parser::reject_unsupported_nested_type admits is backed by a real builder path.
        let inner_entries = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, true),
            ])),
            false,
        );
        let outer_entries = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Map(Arc::new(inner_entries), false), true),
            ])),
            false,
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "outer",
            DataType::Map(Arc::new(outer_entries), false),
            true,
        )]));

        let mut log = LogEvent::default();
        log.insert("outer", json!({"a": {"x": "1"}, "b": {}}));
        let events = vec![Event::Log(log)];

        let batch = build_record_batch(Arc::clone(&schema), &events)
            .expect("Map<Utf8,Map<Utf8,Utf8>> should encode");
        let map = batch.column(0).as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map.value_length(0), 2, "row 0 has two outer keys (a, b)");

        let ipc = encode_events_to_arrow_ipc_stream(&events, Some(Arc::clone(&schema)))
            .expect("IPC serialize");
        let mut reader = StreamReader::try_new(Cursor::new(ipc), None).expect("IPC reader");
        let read_back = reader.next().expect("one batch").expect("valid batch");
        assert_eq!(read_back.num_rows(), 1);
    }

    #[test]
    fn test_encode_map_int_key_unparseable_is_signaled_not_dropped() {
        use arrow::array::MapArray;
        use serde_json::json;
        // A non-numeric key for an Int64-keyed map collapses to 0 (lossy) but the entry is retained
        // and the batch still encodes, rather than the whole batch failing.
        let entries = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Int64, false),
                Field::new("value", DataType::Utf8, true),
            ])),
            false,
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "m",
            DataType::Map(Arc::new(entries), false),
            true,
        )]));

        let mut log = LogEvent::default();
        log.insert("m", json!({"not_a_number": "x"}));
        let events = vec![Event::Log(log)];

        let batch = build_record_batch(Arc::clone(&schema), &events)
            .expect("lossy int key should not fail");
        let map = batch.column(0).as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(
            map.value_length(0),
            1,
            "the entry is retained (key collapsed to 0)"
        );
    }

    // -------------------------------------------------------------------------
    // List encoding tests
    // -------------------------------------------------------------------------

    /// `repeated int64` — encodes a List&lt;Int64&gt; field.
    #[test]
    fn test_encode_list_int64() {
        use arrow::array::{Int64Array, ListArray};

        let mut log0 = LogEvent::default();
        log0.insert(
            "hashes",
            Value::Array(vec![
                Value::Integer(111),
                Value::Integer(222),
                Value::Integer(333),
            ]),
        );

        let mut log1 = LogEvent::default();
        log1.insert("hashes", Value::Array(vec![Value::Integer(999)]));

        let events = vec![Event::Log(log0), Event::Log(log1)];
        let item_field = Field::new("item", DataType::Int64, true);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "hashes",
            DataType::List(Arc::new(item_field)),
            true,
        )]));

        let result = build_record_batch(Arc::clone(&schema), &events);
        assert!(result.is_ok(), "build_record_batch failed: {:?}", result);
        let batch = result.unwrap();

        let list_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        // Row 0: [111, 222, 333]
        assert!(!list_col.is_null(0));
        let row0 = list_col
            .value(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(row0, vec![111, 222, 333]);

        // Row 1: [999]
        assert!(!list_col.is_null(1));
        let row1 = list_col
            .value(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(row1, vec![999]);
    }

    /// `repeated string` — encodes a List&lt;LargeUtf8&gt; field.
    #[test]
    fn test_encode_list_string() {
        use arrow::array::{LargeStringArray, ListArray};

        let mut log0 = LogEvent::default();
        log0.insert(
            "ids",
            Value::Array(vec![
                Value::Bytes("qpl-abc".into()),
                Value::Bytes("qpl-def".into()),
            ]),
        );

        let events = vec![Event::Log(log0)];
        let item_field = Field::new("item", DataType::LargeUtf8, true);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ids",
            DataType::List(Arc::new(item_field)),
            true,
        )]));

        let result = build_record_batch(Arc::clone(&schema), &events);
        assert!(result.is_ok(), "build_record_batch failed: {:?}", result);
        let batch = result.unwrap();

        let list_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let row0_val = list_col.value(0);
        let row0 = row0_val
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        assert_eq!(row0.value(0), "qpl-abc");
        assert_eq!(row0.value(1), "qpl-def");
    }

    /// Null rows: absent list field → null list; non-nullable missing → error.
    #[test]
    fn test_encode_list_null_rows() {
        use arrow::array::ListArray;

        let mut log0 = LogEvent::default();
        log0.insert(
            "hashes",
            Value::Array(vec![Value::Integer(1), Value::Integer(2)]),
        );
        let log1 = LogEvent::default(); // absent → null

        let events = vec![Event::Log(log0), Event::Log(log1)];
        let item_field = Field::new("item", DataType::Int64, true);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "hashes",
            DataType::List(Arc::new(item_field)),
            true, // nullable
        )]));

        let batch = build_record_batch(Arc::clone(&schema), &events).unwrap();
        let list_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(!list_col.is_null(0), "row 0 should be non-null");
        assert!(list_col.is_null(1), "row 1 should be null");
        assert_eq!(list_col.value(0).len(), 2);
    }

    #[test]
    fn test_encode_list_non_nullable_missing_fails() {
        let log = LogEvent::default();
        let events = vec![Event::Log(log)];
        let item_field = Field::new("item", DataType::Int64, true);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "hashes",
            DataType::List(Arc::new(item_field)),
            false, // non-nullable
        )]));

        let result = build_record_batch(Arc::clone(&schema), &events);
        assert!(matches!(
            result,
            Err(ArrowEncodingError::NullConstraint { .. })
        ));
    }

    #[test]
    fn test_encode_list_non_nullable_missing_coerces_to_empty() {
        use arrow::array::ListArray;
        // Absent non-nullable list under coercion -> empty list.
        let events = vec![Event::Log(LogEvent::default())];
        let item_field = Field::new("item", DataType::Int64, true);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "hashes",
            DataType::List(Arc::new(item_field)),
            false,
        )]));
        let coerce = std::collections::HashMap::from([("hashes".to_string(), String::new())]);

        let batch = build_record_batch_inner(Arc::clone(&schema), &events, Some(&coerce))
            .expect("missing non-nullable list should coerce to empty, not fail");
        let list = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(
            !list.is_null(0),
            "coerced row should be a valid (empty) list"
        );
        assert_eq!(
            list.value_length(0),
            0,
            "coerced missing list should be empty"
        );
    }

    #[test]
    fn test_encode_list_present_non_array_coerces_to_empty() {
        use arrow::array::ListArray;
        // A present non-array value coerces to an empty list.
        let mut log = LogEvent::default();
        log.insert("hashes", "not-an-array");
        let events = vec![Event::Log(log)];
        let item_field = Field::new("item", DataType::Int64, true);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "hashes",
            DataType::List(Arc::new(item_field)),
            false,
        )]));
        let coerce = std::collections::HashMap::from([("hashes".to_string(), String::new())]);

        let batch = build_record_batch_inner(Arc::clone(&schema), &events, Some(&coerce))
            .expect("present non-array list should coerce to empty, not fail the batch");
        let list = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(
            !list.is_null(0),
            "coerced row should be a valid (empty) list"
        );
        assert_eq!(
            list.value_length(0),
            0,
            "malformed list value should coerce to zero items"
        );
    }

    /// `List<Struct>` — encodes a repeated nested message field.
    #[test]
    fn test_encode_list_struct() {
        use arrow::array::{Int64Array, ListArray, StructArray};
        use vrl::value::ObjectMap;

        let item_struct_fields = Fields::from(vec![
            Field::new("rule_id", DataType::Int64, true),
            Field::new("total_time_ns", DataType::Int64, true),
            Field::new("phase_id", DataType::Int64, true),
        ]);

        // Row 0: two rule summaries.
        let make_rule = |rule_id: i64, total: i64, phase: i64| {
            let mut m = ObjectMap::new();
            m.insert("rule_id".into(), Value::Integer(rule_id));
            m.insert("total_time_ns".into(), Value::Integer(total));
            m.insert("phase_id".into(), Value::Integer(phase));
            Value::Object(m)
        };

        let mut log0 = LogEvent::default();
        log0.insert(
            "rule_stats",
            Value::Array(vec![make_rule(1, 5_000, 3), make_rule(2, 8_000, 4)]),
        );

        // Row 1: empty list.
        let mut log1 = LogEvent::default();
        log1.insert("rule_stats", Value::Array(vec![]));

        let events = vec![Event::Log(log0), Event::Log(log1)];
        let item_field = Field::new("item", DataType::Struct(item_struct_fields.clone()), true);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "rule_stats",
            DataType::List(Arc::new(item_field)),
            true,
        )]));

        let result = build_record_batch(Arc::clone(&schema), &events);
        assert!(result.is_ok(), "build_record_batch failed: {:?}", result);
        let batch = result.unwrap();

        let list_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        // Row 0: 2 struct items.
        assert!(!list_col.is_null(0));
        let row0_val = list_col.value(0);
        let row0_structs = row0_val.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(row0_structs.len(), 2);

        let rule_ids = row0_structs
            .column_by_name("rule_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(rule_ids.value(0), 1);
        assert_eq!(rule_ids.value(1), 2);

        let total_ns = row0_structs
            .column_by_name("total_time_ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(total_ns.value(0), 5_000);
        assert_eq!(total_ns.value(1), 8_000);

        // Row 1: empty list.
        assert!(!list_col.is_null(1));
        assert_eq!(list_col.value(1).len(), 0);
    }

    /// IPC round-trip for List<Int64>.
    #[test]
    fn test_encode_list_ipc_roundtrip() {
        use arrow::ipc::reader::StreamReader;
        use std::io::Cursor;

        let mut log = LogEvent::default();
        log.insert(
            "hashes",
            Value::Array(vec![
                Value::Integer(10),
                Value::Integer(20),
                Value::Integer(30),
            ]),
        );

        let events = vec![Event::Log(log)];
        let item_field = Field::new("item", DataType::Int64, true);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "hashes",
            DataType::List(Arc::new(item_field)),
            true,
        )]));

        let bytes = encode_events_to_arrow_ipc_stream(&events, Some(Arc::clone(&schema)));
        assert!(bytes.is_ok(), "IPC encoding failed: {:?}", bytes);

        let cursor = Cursor::new(bytes.unwrap());
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
    }

    // -------------------------------------------------------------------------
    // Map encoding tests
    // -------------------------------------------------------------------------

    fn map_field(value_type: DataType, nullable: bool) -> Field {
        Field::new(
            "flags",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::LargeUtf8, false),
                        Field::new("value", value_type, true),
                    ])),
                    false,
                )),
                false,
            ),
            nullable,
        )
    }

    #[test]
    fn test_encode_map_string_to_bool() {
        use arrow::array::MapArray;
        use serde_json::json;

        let mut log = LogEvent::default();
        log.insert("flags", json!({"enabled": true, "debug": false}));

        let events = vec![Event::Log(log)];
        let schema = Arc::new(Schema::new(vec![map_field(DataType::Boolean, true)]));

        let result = build_record_batch(Arc::clone(&schema), &events);
        assert!(result.is_ok(), "build_record_batch failed: {:?}", result);
        let batch = result.unwrap();

        let map_col = batch.column(0).as_any().downcast_ref::<MapArray>().unwrap();
        assert!(!map_col.is_null(0));

        // The map for row 0 has 2 entries.
        let entries = map_col.value(0);
        assert_eq!(entries.len(), 2);

        let keys = entries
            .column_by_name("key")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::LargeStringArray>()
            .unwrap();
        let vals = entries
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();

        // BTreeMap iteration order is alphabetical: "debug" < "enabled".
        assert_eq!(keys.value(0), "debug");
        assert!(!vals.value(0));
        assert_eq!(keys.value(1), "enabled");
        assert!(vals.value(1));
    }

    #[test]
    fn test_encode_map_string_to_int64() {
        use arrow::array::MapArray;
        use serde_json::json;

        let mut log = LogEvent::default();
        log.insert("flags", json!({"hits": 10, "misses": 3}));

        let events = vec![Event::Log(log)];
        let schema = Arc::new(Schema::new(vec![map_field(DataType::Int64, true)]));

        let result = build_record_batch(Arc::clone(&schema), &events);
        assert!(result.is_ok(), "build_record_batch failed: {:?}", result);
        let batch = result.unwrap();

        let map_col = batch.column(0).as_any().downcast_ref::<MapArray>().unwrap();

        let entries = map_col.value(0);
        assert_eq!(entries.len(), 2);

        let keys = entries
            .column_by_name("key")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::LargeStringArray>()
            .unwrap();
        let vals = entries
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Alphabetical: "hits" < "misses"
        assert_eq!(keys.value(0), "hits");
        assert_eq!(vals.value(0), 10);
        assert_eq!(keys.value(1), "misses");
        assert_eq!(vals.value(1), 3);
    }

    #[test]
    fn test_encode_map_null_rows() {
        use arrow::array::MapArray;
        use serde_json::json;

        let mut log1 = LogEvent::default();
        log1.insert("flags", json!({"a": true}));

        let log2 = LogEvent::default(); // flags absent → null map

        let events = vec![Event::Log(log1), Event::Log(log2)];
        let schema = Arc::new(Schema::new(vec![map_field(DataType::Boolean, true)]));

        let result = build_record_batch(Arc::clone(&schema), &events);
        assert!(result.is_ok(), "build_record_batch failed: {:?}", result);
        let batch = result.unwrap();

        let map_col = batch.column(0).as_any().downcast_ref::<MapArray>().unwrap();
        assert!(!map_col.is_null(0), "row 0 should be non-null");
        assert!(map_col.is_null(1), "row 1 should be null");
        assert_eq!(map_col.value(0).len(), 1);
    }

    #[test]
    fn test_encode_map_non_nullable_missing_fails() {
        let log = LogEvent::default(); // map field absent
        let events = vec![Event::Log(log)];
        let schema = Arc::new(Schema::new(vec![map_field(DataType::Boolean, false)]));

        let result = build_record_batch(Arc::clone(&schema), &events);
        assert!(matches!(
            result,
            Err(ArrowEncodingError::NullConstraint { .. })
        ));
    }

    #[test]
    fn test_encode_map_non_nullable_missing_coerces_to_empty() {
        use arrow::array::MapArray;
        // Under coercion, a missing non-nullable Map becomes an empty map; without coercion it
        // errors (test_encode_map_non_nullable_missing_fails).
        let events = vec![Event::Log(LogEvent::default())]; // "flags" map field absent
        let schema = Arc::new(Schema::new(vec![map_field(DataType::Boolean, false)]));
        let coerce = std::collections::HashMap::from([("flags".to_string(), String::new())]);

        let batch = build_record_batch_inner(Arc::clone(&schema), &events, Some(&coerce))
            .expect("missing non-nullable map should coerce to empty, not fail");

        assert_eq!(batch.num_rows(), 1);
        let map = batch.column(0).as_any().downcast_ref::<MapArray>().unwrap();
        assert!(
            !map.is_null(0),
            "coerced row should be a valid (empty) map, not null"
        );
        assert_eq!(
            map.value_length(0),
            0,
            "coerced missing map should have zero entries"
        );
    }

    #[test]
    fn test_encode_map_present_non_object_coerces_to_empty() {
        use arrow::array::MapArray;
        // A present non-object value coerces to an empty map.
        let mut log = LogEvent::default();
        log.insert("flags", "not-a-map");
        let events = vec![Event::Log(log)];
        let schema = Arc::new(Schema::new(vec![map_field(DataType::Boolean, false)]));
        let coerce = std::collections::HashMap::from([("flags".to_string(), String::new())]);

        let batch = build_record_batch_inner(Arc::clone(&schema), &events, Some(&coerce))
            .expect("present non-object map should coerce to empty, not fail the batch");
        let map = batch.column(0).as_any().downcast_ref::<MapArray>().unwrap();
        assert!(
            !map.is_null(0),
            "coerced row should be a valid (empty) map, not null"
        );
        assert_eq!(
            map.value_length(0),
            0,
            "malformed map value should coerce to zero entries"
        );
    }

    #[test]
    fn test_encode_map_ipc_roundtrip() {
        use arrow::ipc::reader::StreamReader;
        use serde_json::json;
        use std::io::Cursor;

        let mut log = LogEvent::default();
        log.insert("settings", json!({"timeout": 30, "retries": 3}));

        let events = vec![Event::Log(log)];
        let schema = Arc::new(Schema::new(vec![map_field(DataType::Int64, true)]));

        let bytes = encode_events_to_arrow_ipc_stream(&events, Some(Arc::clone(&schema)));
        assert!(bytes.is_ok(), "IPC encoding failed: {:?}", bytes);

        let cursor = Cursor::new(bytes.unwrap());
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
    }

    /// Helper to build a Map field with an Int64 key type (mirrors proto map<int64, V>).
    /// Uses "key_value" as the entries field name to match Spark/Delta/UC convention.
    fn map_field_int64_key(value_type: DataType, nullable: bool) -> Field {
        Field::new(
            "config_access",
            DataType::Map(
                Arc::new(Field::new(
                    "key_value",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Int64, false),
                        Field::new("value", value_type, true),
                    ])),
                    false,
                )),
                false,
            ),
            nullable,
        )
    }

    #[test]
    fn test_encode_map_int64_key_to_bool() {
        use arrow::array::{Int64Array, MapArray};
        use serde_json::json;

        let mut log = LogEvent::default();
        // Keys are string representations of int64 values (how proto map<int64,bool>
        // arrives in Vector events after JSON decode).
        log.insert(
            "config_access",
            json!({"1234567890": true, "9876543210": false}),
        );

        let events = vec![Event::Log(log)];
        let schema = Arc::new(Schema::new(vec![map_field_int64_key(
            DataType::Boolean,
            true,
        )]));

        let result = build_record_batch(Arc::clone(&schema), &events);
        assert!(result.is_ok(), "build_record_batch failed: {:?}", result);
        let batch = result.unwrap();

        let map_col = batch.column(0).as_any().downcast_ref::<MapArray>().unwrap();
        assert!(!map_col.is_null(0));

        let entries = map_col.value(0);
        assert_eq!(entries.len(), 2);

        let keys = entries
            .column_by_name("key")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let vals = entries
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();

        // BTreeMap iteration order is lexicographic on the string keys.
        // "1234567890" < "9876543210" lexicographically.
        assert_eq!(keys.value(0), 1234567890_i64);
        assert!(vals.value(0));
        assert_eq!(keys.value(1), 9876543210_i64);
        assert!(!vals.value(1));
    }

    #[test]
    fn test_encode_map_int64_key_to_float64() {
        use arrow::array::{Float64Array, Int64Array, MapArray};
        use serde_json::json;

        let mut log = LogEvent::default();
        log.insert(
            "config_access",
            json!({"1111111111": 200.0, "2222222222": 0.5}),
        );

        let events = vec![Event::Log(log)];
        let schema = Arc::new(Schema::new(vec![map_field_int64_key(
            DataType::Float64,
            true,
        )]));

        let result = build_record_batch(Arc::clone(&schema), &events);
        assert!(result.is_ok(), "build_record_batch failed: {:?}", result);
        let batch = result.unwrap();

        let map_col = batch.column(0).as_any().downcast_ref::<MapArray>().unwrap();
        let entries = map_col.value(0);
        assert_eq!(entries.len(), 2);

        let keys = entries
            .column_by_name("key")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let vals = entries
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(keys.value(0), 1111111111_i64);
        assert_eq!(vals.value(0), 200.0_f64);
        assert_eq!(keys.value(1), 2222222222_i64);
        assert_eq!(vals.value(1), 0.5_f64);
    }

    #[test]
    fn test_encode_map_key_value_naming_ipc_roundtrip() {
        // Verify that a map schema using "key_value" (Spark/Delta/UC convention)
        // round-trips through Arrow IPC correctly.
        use arrow::ipc::reader::StreamReader;
        use serde_json::json;
        use std::io::Cursor;

        let mut log = LogEvent::default();
        log.insert(
            "config_access",
            json!({"1234567890": true, "9876543210": false}),
        );

        let events = vec![Event::Log(log)];
        let schema = Arc::new(Schema::new(vec![map_field_int64_key(
            DataType::Boolean,
            true,
        )]));

        let bytes = encode_events_to_arrow_ipc_stream(&events, Some(Arc::clone(&schema)));
        assert!(bytes.is_ok(), "IPC encoding failed: {:?}", bytes);

        let cursor = Cursor::new(bytes.unwrap());
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);

        // Verify the inner field is named "key_value" (UC/Spark convention).
        let schema = batch.schema();
        let map_field = schema.field(0);
        if let DataType::Map(entries_field, _) = map_field.data_type() {
            assert_eq!(
                entries_field.name(),
                "key_value",
                "Map entries field must be named 'key_value' for UC compatibility"
            );
        } else {
            panic!("Expected Map type");
        }
    }

    /// Struct field set to an empty object `{}` — all child columns are null.
    ///
    /// The struct validity bit should be **true** (the field exists) but every
    /// child column should be **null** (no sub-keys are present in the empty map).
    #[test]
    fn test_encode_struct_empty_object_is_valid_with_null_children() {
        use arrow::array::{Int64Array, LargeStringArray, StructArray};
        use vrl::value::ObjectMap;

        // Row 0: struct is an empty object — present but has no children.
        let mut log0 = LogEvent::default();
        log0.insert("meta", Value::Object(ObjectMap::new()));

        // Row 1: struct is absent altogether — should be null.
        let log1 = LogEvent::default();

        // Row 2: struct has actual child values.
        let mut log2 = LogEvent::default();
        log2.insert("meta.id", 42_i64);
        log2.insert("meta.label", "hello");

        let events = vec![Event::Log(log0), Event::Log(log1), Event::Log(log2)];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "meta",
            DataType::Struct(Fields::from(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("label", DataType::LargeUtf8, true),
            ])),
            true,
        )]));

        let result = build_record_batch(Arc::clone(&schema), &events);
        assert!(result.is_ok(), "build_record_batch failed: {:?}", result);
        let batch = result.unwrap();

        let struct_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        // Row 0: empty object → non-null struct, but null children.
        assert!(
            !struct_col.is_null(0),
            "empty-object struct row should be non-null"
        );
        let id_col = struct_col
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let label_col = struct_col
            .column_by_name("label")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        assert!(id_col.is_null(0), "id should be null for empty-object row");
        assert!(
            label_col.is_null(0),
            "label should be null for empty-object row"
        );

        // Row 1: absent struct → null struct.
        assert!(struct_col.is_null(1), "absent struct row should be null");

        // Row 2: fully populated struct.
        assert!(
            !struct_col.is_null(2),
            "populated struct row should be non-null"
        );
        assert_eq!(id_col.value(2), 42);
        assert_eq!(label_col.value(2), "hello");
    }

    // -------------------------------------------------------------------------
    // Realistic proto-schema tests
    // -------------------------------------------------------------------------

    /// Schema with a mix of scalar, nested struct, list, bool, and binary types.
    ///
    /// Field names are intentionally generic (field_N / sub_N).
    ///
    /// Field layout (field number → Arrow type):
    ///   field_1  (1)  LargeUtf8         — optional string
    ///   field_3  (3)  LargeUtf8         — optional string
    ///   field_4  (4)  Int32             — optional enum
    ///   field_5  (5)  Int64             — optional int64
    ///   field_6  (6)  Int64             — optional int64
    ///   field_8  (8)  Struct            — nested message (2 string children)
    ///     sub_1 LargeUtf8, sub_2 LargeUtf8
    ///   field_9  (9)  Struct            — nested message (6 children)
    ///     sub_1 Int32, sub_2 LargeUtf8, sub_3 LargeUtf8,
    ///     sub_4 LargeUtf8, sub_5 Int32, sub_6 Int32
    ///   field_10 (10) List<Int64>       — repeated int64
    ///   field_11 (11) Boolean           — optional bool
    ///   field_12 (12) LargeUtf8         — optional string
    ///   field_13 (13) LargeUtf8         — optional string
    ///   field_14 (14) Boolean           — optional bool
    ///   field_15 (15) LargeUtf8         — optional string
    ///   field_16 (16) LargeBinary       — optional bytes
    #[test]
    fn test_encode_proto_schema_mixed_types() {
        use arrow::array::{BooleanArray, Int32Array, Int64Array, LargeStringArray, StructArray};

        // field_8: Struct(sub_1 LargeUtf8, sub_2 LargeUtf8)
        let field_8_fields = Fields::from(vec![
            Field::new("sub_1", DataType::LargeUtf8, true),
            Field::new("sub_2", DataType::LargeUtf8, true),
        ]);
        // field_9: Struct(sub_1 Int32, sub_2..sub_4 LargeUtf8, sub_5..sub_6 Int32)
        let field_9_fields = Fields::from(vec![
            Field::new("sub_1", DataType::Int32, true),
            Field::new("sub_2", DataType::LargeUtf8, true),
            Field::new("sub_3", DataType::LargeUtf8, true),
            Field::new("sub_4", DataType::LargeUtf8, true),
            Field::new("sub_5", DataType::Int32, true),
            Field::new("sub_6", DataType::Int32, true),
        ]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("field_1", DataType::LargeUtf8, true),
            Field::new("field_3", DataType::LargeUtf8, true),
            Field::new("field_4", DataType::Int32, true),
            Field::new("field_5", DataType::Int64, true),
            Field::new("field_6", DataType::Int64, true),
            Field::new("field_8", DataType::Struct(field_8_fields.clone()), true),
            Field::new("field_9", DataType::Struct(field_9_fields.clone()), true),
            Field::new(
                "field_10",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ),
            Field::new("field_11", DataType::Boolean, true),
            Field::new("field_12", DataType::LargeUtf8, true),
            Field::new("field_13", DataType::LargeUtf8, true),
            Field::new("field_14", DataType::Boolean, true),
            Field::new("field_15", DataType::LargeUtf8, true),
            Field::new("field_16", DataType::LargeBinary, true),
        ]));

        // Row 0: all fields populated; nullable sub-fields omitted → null in Arrow.
        let mut log0 = LogEvent::default();
        log0.insert("field_1", "val_a");
        log0.insert("field_3", "val_b");
        log0.insert("field_4", 1i64);
        log0.insert("field_5", 42_000i64);
        log0.insert("field_6", 100i64);
        log0.insert("field_8.sub_1", "sub_val_a");
        // field_8.sub_2 absent → null
        log0.insert("field_9.sub_1", 1i64);
        log0.insert("field_9.sub_2", "sub_val_b");
        // field_9.sub_3 absent → null
        // field_9.sub_4 absent → null
        log0.insert("field_9.sub_5", 2i64);
        log0.insert("field_9.sub_6", 0i64);
        log0.insert(
            "field_10",
            Value::Array(vec![Value::Integer(111), Value::Integer(222)]),
        );
        log0.insert("field_11", false);
        log0.insert("field_12", "val_c");
        log0.insert("field_13", "val_d");
        log0.insert("field_14", false);
        log0.insert("field_15", "val_e");
        log0.insert("field_16", Value::Bytes(b"\x0a\x05hello".to_vec().into()));

        // Row 1: field_8 and field_9 absent → null structs; several fields absent → null.
        let mut log1 = LogEvent::default();
        log1.insert("field_1", "val_f");
        log1.insert("field_3", "val_g");
        log1.insert("field_4", 2i64);
        log1.insert("field_5", 5_000i64);
        log1.insert("field_6", 200i64);
        // field_8 absent → null struct
        // field_9 absent → null struct
        // field_10 absent → null list
        log1.insert("field_11", true);
        log1.insert("field_12", "val_h");
        // field_13 absent → null
        log1.insert("field_14", true);
        // field_15 absent → null
        // field_16 absent → null

        let events = vec![Event::Log(log0), Event::Log(log1)];

        let result = build_record_batch(Arc::clone(&schema), &events);
        assert!(result.is_ok(), "build_record_batch failed: {:?}", result);
        let batch = result.unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 14);

        // Scalar fields.
        let f1 = batch
            .column_by_name("field_1")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        assert_eq!(f1.value(0), "val_a");
        assert_eq!(f1.value(1), "val_f");

        let f4 = batch
            .column_by_name("field_4")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(f4.value(0), 1);
        assert_eq!(f4.value(1), 2);

        let f6 = batch
            .column_by_name("field_6")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(f6.value(0), 100);
        assert_eq!(f6.value(1), 200);

        // field_8 (Struct): row 0 non-null, row 1 null.
        let f8 = batch
            .column_by_name("field_8")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(!f8.is_null(0));
        assert!(f8.is_null(1));
        let f8_sub1 = f8
            .column_by_name("sub_1")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        assert_eq!(f8_sub1.value(0), "sub_val_a");

        // field_9 (Struct): row 0 non-null, row 1 null.
        let f9 = batch
            .column_by_name("field_9")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(!f9.is_null(0));
        assert!(f9.is_null(1));
        let f9_sub6 = f9
            .column_by_name("sub_6")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(f9_sub6.value(0), 0);

        // field_10 (List<Int64>): row 0 has [111, 222], row 1 null.
        use arrow::array::ListArray;
        let f10 = batch
            .column_by_name("field_10")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(!f10.is_null(0));
        assert!(f10.is_null(1));
        let f10_vals: Vec<i64> = f10
            .value(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .flatten()
            .collect();
        assert_eq!(f10_vals, vec![111, 222]);

        // Boolean fields.
        let f14 = batch
            .column_by_name("field_14")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(!f14.value(0));
        assert!(f14.value(1));

        // field_16 (LargeBinary): row 0 non-null, row 1 null.
        use arrow::array::LargeBinaryArray;
        let f16 = batch
            .column_by_name("field_16")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert!(!f16.is_null(0));
        assert!(f16.is_null(1));
        assert_eq!(f16.value(0), b"\x0a\x05hello");
    }

    // -------------------------------------------------------------------------
    // complex nested type patterns
    // -------------------------------------------------------------------------

    /// `List<Struct>` containing its own inner `List<Struct>`.
    ///
    /// Schema:
    ///   field_a: List<Struct {
    ///     sub_1: Int32,
    ///     sub_2: Int32,
    ///     sub_3: List<Struct { inner_1: Int64, inner_2: Int64 }>,
    ///   }>
    #[test]
    fn test_encode_list_struct_with_inner_list_struct() {
        use arrow::array::{Int32Array, Int64Array, ListArray, StructArray};
        use vrl::value::ObjectMap;

        // Inner struct builder helper.
        let make_inner = |inner_1: i64, inner_2: i64| {
            let mut m = ObjectMap::new();
            m.insert("inner_1".into(), Value::Integer(inner_1));
            m.insert("inner_2".into(), Value::Integer(inner_2));
            Value::Object(m)
        };
        let make_outer = |sub_1: i64, sub_2: i64, inners: Vec<Value>| {
            let mut m = ObjectMap::new();
            m.insert("sub_1".into(), Value::Integer(sub_1));
            m.insert("sub_2".into(), Value::Integer(sub_2));
            m.insert("sub_3".into(), Value::Array(inners));
            Value::Object(m)
        };

        // Row 0: two outer elements, first with two inner elements, second with none.
        let mut log0 = LogEvent::default();
        log0.insert(
            "field_a",
            Value::Array(vec![
                make_outer(
                    1,
                    10,
                    vec![make_inner(101, 500_000), make_inner(102, 300_000)],
                ),
                make_outer(2, 5, vec![]),
            ]),
        );

        // Row 1: single outer element, one inner element.
        let mut log1 = LogEvent::default();
        log1.insert(
            "field_a",
            Value::Array(vec![make_outer(3, 8, vec![make_inner(201, 1_000_000)])]),
        );

        // Row 2: absent field_a → null list.
        let log2 = LogEvent::default();

        let events = vec![Event::Log(log0), Event::Log(log1), Event::Log(log2)];

        let inner_struct_fields = Fields::from(vec![
            Field::new("inner_1", DataType::Int64, true),
            Field::new("inner_2", DataType::Int64, true),
        ]);
        let outer_struct_fields = Fields::from(vec![
            Field::new("sub_1", DataType::Int32, true),
            Field::new("sub_2", DataType::Int32, true),
            Field::new(
                "sub_3",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(inner_struct_fields),
                    true,
                ))),
                true,
            ),
        ]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "field_a",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(outer_struct_fields),
                true,
            ))),
            true,
        )]));

        let result = build_record_batch(Arc::clone(&schema), &events);
        assert!(result.is_ok(), "build_record_batch failed: {:?}", result);
        let batch = result.unwrap();
        assert_eq!(batch.num_rows(), 3);

        let outer_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        // Row 0: 2 outer elements.
        assert!(!outer_col.is_null(0));
        let row0_outer_arr = outer_col.value(0);
        let row0_outer = row0_outer_arr
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(row0_outer.len(), 2);

        // sub_1 values of the two outer elements.
        let sub1_col = row0_outer
            .column_by_name("sub_1")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(sub1_col.value(0), 1);
        assert_eq!(sub1_col.value(1), 2);

        let sub3_col = row0_outer
            .column_by_name("sub_3")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        // First outer element's sub_3: [inner_1=101, inner_1=102].
        let first_inner_arr = sub3_col.value(0);
        let first_inner = first_inner_arr
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(first_inner.len(), 2);
        let inner1_col = first_inner
            .column_by_name("inner_1")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(inner1_col.value(0), 101);
        assert_eq!(inner1_col.value(1), 102);

        // Second outer element's sub_3: empty.
        assert_eq!(sub3_col.value(1).len(), 0);

        // Row 1: 1 outer element.
        assert!(!outer_col.is_null(1));
        assert_eq!(outer_col.value(1).len(), 1);

        // Row 2: absent → null list.
        assert!(outer_col.is_null(2));
    }

    /// Map fields nested inside a struct.
    ///
    /// Schema:
    ///   field_a: Struct {
    ///     sub_1: Int64,
    ///     sub_2: Map("key_value", Struct(key: Int64, value: Boolean)),
    ///     sub_3: Map("key_value", Struct(key: Int64, value: Float64)),
    ///   }
    #[test]
    fn test_encode_struct_with_nested_map_fields() {
        use arrow::array::{Float64Array, Int64Array, MapArray, StructArray};
        use serde_json::json;

        let sub2_field = Field::new(
            "sub_2",
            DataType::Map(
                Arc::new(Field::new(
                    "key_value",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Int64, false),
                        Field::new("value", DataType::Boolean, true),
                    ])),
                    false,
                )),
                false,
            ),
            true,
        );
        let sub3_field = Field::new(
            "sub_3",
            DataType::Map(
                Arc::new(Field::new(
                    "key_value",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Int64, false),
                        Field::new("value", DataType::Float64, true),
                    ])),
                    false,
                )),
                false,
            ),
            true,
        );
        let struct_fields = Fields::from(vec![
            Field::new("sub_1", DataType::Int64, true),
            sub2_field,
            sub3_field,
        ]);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "field_a",
            DataType::Struct(struct_fields),
            true,
        )]));

        // Row 0: fully populated field_a.
        let mut log0 = LogEvent::default();
        log0.insert("field_a.sub_1", 42_000_i64);
        log0.insert("field_a.sub_2", json!({"100": true, "200": false}));
        log0.insert("field_a.sub_3", json!({"300": 1.5, "400": 2.0}));

        // Row 1: field_a absent → null struct.
        let log1 = LogEvent::default();

        let events = vec![Event::Log(log0), Event::Log(log1)];

        let result = build_record_batch(Arc::clone(&schema), &events);
        assert!(result.is_ok(), "build_record_batch failed: {:?}", result);
        let batch = result.unwrap();
        assert_eq!(batch.num_rows(), 2);

        let struct_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        // Row 0: non-null struct.
        assert!(!struct_col.is_null(0), "row 0 struct should be non-null");
        // Row 1: null struct.
        assert!(struct_col.is_null(1), "row 1 struct should be null");

        // Check sub_1.
        let sub1_col = struct_col
            .column_by_name("sub_1")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(sub1_col.value(0), 42_000);

        // Check sub_2 map: 2 entries with int64 keys.
        let sub2_col = struct_col
            .column_by_name("sub_2")
            .unwrap()
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
        assert!(!sub2_col.is_null(0));
        let sub2_entries = sub2_col.value(0);
        assert_eq!(sub2_entries.len(), 2);
        let sub2_keys = sub2_entries
            .column_by_name("key")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // Keys coerced from string "100"/"200" to Int64.
        assert_eq!(sub2_keys.value(0), 100_i64);
        assert_eq!(sub2_keys.value(1), 200_i64);

        // Check sub_3 map: 2 entries with float64 values.
        let sub3_col = struct_col
            .column_by_name("sub_3")
            .unwrap()
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
        assert!(!sub3_col.is_null(0));
        let sub3_entries = sub3_col.value(0);
        assert_eq!(sub3_entries.len(), 2);
        let sub3_vals = sub3_entries
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(sub3_vals.value(0), 1.5_f64);
        assert_eq!(sub3_vals.value(1), 2.0_f64);
    }

    /// IPC round-trip for a schema combining scalar, Struct, List<Struct>, Map,
    /// and List<string> fields — verifies the full Arrow IPC serialization path.
    #[test]
    fn test_encode_complex_schema_ipc_roundtrip() {
        use arrow::ipc::reader::StreamReader;
        use serde_json::json;
        use std::io::Cursor;
        use vrl::value::ObjectMap;

        let field3_fields = Fields::from(vec![
            Field::new("sub_1", DataType::LargeUtf8, true),
            Field::new("sub_2", DataType::LargeUtf8, true),
            Field::new("sub_3", DataType::LargeUtf8, true),
        ]);
        let field4_struct_fields = Fields::from(vec![
            Field::new("sub_1", DataType::Int32, true),
            Field::new("sub_2", DataType::Int32, true),
            Field::new("sub_3", DataType::LargeUtf8, true),
        ]);
        let field5_map = Field::new(
            "sub_2",
            DataType::Map(
                Arc::new(Field::new(
                    "key_value",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Int64, false),
                        Field::new("value", DataType::Boolean, true),
                    ])),
                    false,
                )),
                false,
            ),
            true,
        );
        let field5_fields =
            Fields::from(vec![Field::new("sub_1", DataType::Int64, true), field5_map]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("field_1", DataType::LargeUtf8, true),
            Field::new("field_2", DataType::Boolean, true),
            Field::new("field_3", DataType::Struct(field3_fields), true),
            Field::new(
                "field_4",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(field4_struct_fields),
                    true,
                ))),
                true,
            ),
            Field::new("field_5", DataType::Struct(field5_fields), true),
            Field::new(
                "field_6",
                DataType::List(Arc::new(Field::new("item", DataType::LargeUtf8, true))),
                true,
            ),
            Field::new(
                "field_7",
                DataType::List(Arc::new(Field::new("item", DataType::LargeUtf8, true))),
                true,
            ),
        ]));

        // Row 0: field_3 absent, field_4 and field_5 populated.
        let mut log0 = LogEvent::default();
        log0.insert("field_1", "rec-a");
        log0.insert("field_2", true);
        log0.insert(
            "field_4",
            Value::Array({
                let mut m = ObjectMap::new();
                m.insert("sub_1".into(), Value::Integer(1));
                m.insert("sub_2".into(), Value::Integer(10));
                m.insert("sub_3".into(), Value::Bytes("".into()));
                vec![Value::Object(m)]
            }),
        );
        log0.insert("field_5.sub_1", 5_000_i64);
        log0.insert("field_5.sub_2", json!({"12345": true}));
        log0.insert("field_6", Value::Array(vec![]));
        log0.insert(
            "field_7",
            Value::Array(vec![
                Value::Bytes("tag-a".into()),
                Value::Bytes("tag-b".into()),
            ]),
        );

        // Row 1: field_3 populated, field_4 absent.
        let mut log1 = LogEvent::default();
        log1.insert("field_1", "rec-b");
        log1.insert("field_2", false);
        log1.insert("field_3.sub_1", "err-a");
        log1.insert("field_3.sub_2", "err-sub-a");
        log1.insert("field_3.sub_3", "code-a");
        log1.insert("field_6", Value::Array(vec![Value::Bytes("rec-a".into())]));
        log1.insert("field_7", Value::Array(vec![Value::Bytes("tag-c".into())]));

        let events = vec![Event::Log(log0), Event::Log(log1)];

        let bytes = encode_events_to_arrow_ipc_stream(&events, Some(Arc::clone(&schema)));
        assert!(bytes.is_ok(), "IPC encoding failed: {:?}", bytes);

        let cursor = Cursor::new(bytes.unwrap());
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 7);
    }

    /// Full schema IPC round-trip: 20 columns covering multiple scalar types,
    /// booleans, two List<string> fields, an optional string, and an optional
    /// Struct with 5 string children.
    ///
    /// Row 0: Struct absent (null), List fields populated with 0 and 2 entries.
    /// Row 1: Struct populated, List fields populated with 1 entry each.
    #[test]
    fn test_encode_wide_schema_with_optional_struct_ipc_roundtrip() {
        use arrow::array::{BooleanArray, Int64Array, LargeStringArray, ListArray, StructArray};
        use arrow::ipc::reader::StreamReader;
        use std::io::Cursor;

        let field18_fields = Fields::from(vec![
            Field::new("sub_1", DataType::LargeUtf8, true),
            Field::new("sub_2", DataType::LargeUtf8, true),
            Field::new("sub_3", DataType::LargeUtf8, true),
            Field::new("sub_4", DataType::LargeUtf8, true),
            Field::new("sub_5", DataType::LargeUtf8, true),
        ]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("field_1", DataType::LargeUtf8, true),
            Field::new("field_2", DataType::LargeUtf8, true),
            Field::new("field_3", DataType::LargeUtf8, true),
            Field::new("field_4", DataType::Int64, true),
            Field::new("field_5", DataType::Int64, true),
            Field::new("field_6", DataType::Boolean, true),
            Field::new("field_7", DataType::Boolean, true),
            Field::new("field_8", DataType::LargeUtf8, true),
            Field::new("field_9", DataType::Boolean, true),
            Field::new("field_10", DataType::Boolean, true),
            Field::new("field_11", DataType::LargeUtf8, true),
            Field::new("field_12", DataType::LargeUtf8, true),
            Field::new("field_13", DataType::LargeUtf8, true),
            Field::new("field_14", DataType::LargeUtf8, true),
            Field::new(
                "field_15",
                DataType::List(Arc::new(Field::new("item", DataType::LargeUtf8, true))),
                true,
            ),
            Field::new(
                "field_16",
                DataType::List(Arc::new(Field::new("item", DataType::LargeUtf8, true))),
                true,
            ),
            Field::new("field_17", DataType::LargeUtf8, true),
            Field::new("field_18", DataType::Struct(field18_fields), true),
            Field::new("field_19", DataType::Int64, true),
            Field::new("field_20", DataType::LargeUtf8, true),
        ]));

        // Row 0: field_17 and field_18 absent → null.
        let mut log0 = LogEvent::default();
        log0.insert("field_1", "rec-a");
        log0.insert("field_2", "app-test");
        log0.insert("field_3", "exec-a");
        log0.insert("field_4", 1700000000000_i64);
        log0.insert("field_5", 1700000001523_i64);
        log0.insert("field_6", false);
        log0.insert("field_7", true);
        log0.insert("field_8", "entry-a");
        log0.insert("field_9", true);
        log0.insert("field_10", false);
        log0.insert("field_11", "wh-a");
        log0.insert("field_12", "rec-a-date");
        log0.insert("field_13", "op-a");
        log0.insert("field_14", "query-a");
        log0.insert("field_15", Value::Array(vec![]));
        log0.insert(
            "field_16",
            Value::Array(vec![
                Value::Bytes("tag-a".into()),
                Value::Bytes("tag-b".into()),
            ]),
        );
        // field_17 and field_18 absent → null
        log0.insert("field_19", 1700000000000000_i64);
        log0.insert("field_20", "2026-02-20");

        // Row 1: field_18 struct populated, field_17 present.
        let mut log1 = LogEvent::default();
        log1.insert("field_1", "rec-b");
        log1.insert("field_2", "app-test");
        log1.insert("field_3", "exec-b");
        log1.insert("field_4", 1700000010000_i64);
        log1.insert("field_5", 1700000055230_i64);
        log1.insert("field_6", true);
        log1.insert("field_7", false);
        log1.insert("field_8", "entry-b");
        log1.insert("field_9", false);
        log1.insert("field_10", true);
        log1.insert("field_11", "wh-b");
        log1.insert("field_12", "rec-b-date");
        log1.insert("field_13", "op-b");
        log1.insert("field_14", "query-b");
        log1.insert("field_15", Value::Array(vec![Value::Bytes("rec-a".into())]));
        log1.insert("field_16", Value::Array(vec![Value::Bytes("tag-c".into())]));
        log1.insert("field_17", "err-type-a");
        log1.insert("field_18.sub_1", "err-a");
        log1.insert("field_18.sub_2", "err-sub-a");
        log1.insert("field_18.sub_3", "code-a");
        log1.insert("field_18.sub_4", "trace-a");
        log1.insert("field_18.sub_5", "err-type-a: detail");
        log1.insert("field_19", 1700000010000000_i64);
        log1.insert("field_20", "2026-02-20");

        let events = vec![Event::Log(log0), Event::Log(log1)];

        let bytes = encode_events_to_arrow_ipc_stream(&events, Some(Arc::clone(&schema)));
        assert!(bytes.is_ok(), "IPC encoding failed: {:?}", bytes);

        let cursor = Cursor::new(bytes.unwrap());
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 20);

        // Spot-check scalar fields.
        let f1 = batch
            .column_by_name("field_1")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        assert_eq!(f1.value(0), "rec-a");
        assert_eq!(f1.value(1), "rec-b");

        let f4 = batch
            .column_by_name("field_4")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(f4.value(0), 1700000000000_i64);
        assert_eq!(f4.value(1), 1700000010000_i64);

        let f7 = batch
            .column_by_name("field_7")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(f7.value(0));
        assert!(!f7.value(1));

        // field_15: row 0 empty list, row 1 has one entry.
        let f15 = batch
            .column_by_name("field_15")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(!f15.is_null(0));
        assert_eq!(f15.value(0).len(), 0);
        assert!(!f15.is_null(1));
        let f15_row1 = f15.value(1);
        let f15_row1_str = f15_row1
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        assert_eq!(f15_row1_str.value(0), "rec-a");

        // field_16: row 0 has ["tag-a","tag-b"], row 1 has ["tag-c"].
        let f16 = batch
            .column_by_name("field_16")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let f16_row0 = f16.value(0);
        let f16_row0_str = f16_row0
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        assert_eq!(f16_row0_str.value(0), "tag-a");
        assert_eq!(f16_row0_str.value(1), "tag-b");

        // field_17: row 0 null, row 1 present.
        let f17 = batch
            .column_by_name("field_17")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        assert!(f17.is_null(0));
        assert_eq!(f17.value(1), "err-type-a");

        // field_18 struct: row 0 null, row 1 non-null.
        let f18 = batch
            .column_by_name("field_18")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(f18.is_null(0));
        assert!(!f18.is_null(1));
        let f18_sub1 = f18
            .column_by_name("sub_1")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        assert_eq!(f18_sub1.value(1), "err-a");
        let f18_sub3 = f18
            .column_by_name("sub_3")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        assert_eq!(f18_sub3.value(1), "code-a");
    }

    /// Regression: `LargeBinary` columns inside a `List<Struct{…}>` must encode
    /// via `LargeBinaryBuilder` in the nested value-builder path, not error out
    /// as `UnsupportedType`. This shape arises from a proto `repeated message`
    /// whose message has a `bytes` field.
    #[test]
    fn test_encode_list_struct_with_large_binary_child() {
        use arrow::array::{LargeBinaryArray, ListArray, StructArray};
        use vrl::value::ObjectMap;

        let item_fields = Fields::from(vec![
            Field::new("driver_id", DataType::LargeBinary, true),
            Field::new("label", DataType::LargeUtf8, true),
        ]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "items",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(item_fields),
                true,
            ))),
            true,
        )]));

        let make_item = |driver_id: &[u8], label: &str| {
            let mut m = ObjectMap::new();
            m.insert("driver_id".into(), Value::Bytes(driver_id.to_vec().into()));
            m.insert("label".into(), Value::from(label));
            Value::Object(m)
        };

        let mut log = LogEvent::default();
        log.insert(
            "items",
            Value::Array(vec![
                make_item(b"abc\x00\xff", "first"),
                make_item(b"", "empty"),
            ]),
        );
        let events = vec![Event::Log(log)];

        let batch = build_record_batch(Arc::clone(&schema), &events)
            .expect("nested LargeBinary must encode");
        let list = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let inner_arr = list.value(0);
        let inner = inner_arr.as_any().downcast_ref::<StructArray>().unwrap();
        let driver_ids = inner
            .column_by_name("driver_id")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert_eq!(driver_ids.value(0), b"abc\x00\xff");
        assert_eq!(driver_ids.value(1), b"");
    }
}
