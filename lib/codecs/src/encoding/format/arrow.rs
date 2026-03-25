//! Arrow IPC streaming format codec for batched event encoding
//!
//! Provides Apache Arrow IPC stream format encoding with static schema support.
//! This implements the streaming variant of the Arrow IPC protocol, which writes
//! a continuous stream of record batches without a file footer.

use arrow::{
    array::{
        ArrayRef, BinaryBuilder, BooleanBuilder, Decimal128Builder, Decimal256Builder,
        Float32Builder, Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder,
        LargeBinaryBuilder, LargeStringBuilder, ListArray, MapArray, StringBuilder, StructArray,
        TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
        TimestampSecondBuilder, UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
    },
    buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
    datatypes::{DataType, Field, Fields, Schema, TimeUnit, i256},
    ipc::writer::StreamWriter,
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use snafu::Snafu;
use std::sync::Arc;
use vector_config::configurable_component;

use vector_core::event::{Event, Value};

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
            .finish()
    }
}

impl ArrowStreamSerializerConfig {
    /// Create a new ArrowStreamSerializerConfig with a schema
    pub fn new(schema: arrow::datatypes::Schema) -> Self {
        Self {
            schema: Some(schema),
            allow_nullable_fields: false,
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
}

impl ArrowStreamSerializer {
    /// Create a new ArrowStreamSerializer with the given configuration
    pub fn new(config: ArrowStreamSerializerConfig) -> Result<Self, vector_common::Error> {
        let schema = config
            .schema
            .ok_or_else(|| vector_common::Error::from("Arrow serializer requires a schema."))?;

        // If allow_nullable_fields is enabled, transform the schema once here
        // instead of on every batch encoding
        let schema = if config.allow_nullable_fields {
            Schema::new_with_metadata(
                schema
                    .fields()
                    .iter()
                    .map(|f| Arc::new(make_field_nullable(f)))
                    .collect::<Vec<_>>(),
                schema.metadata().clone(),
            )
        } else {
            schema
        };

        Ok(Self {
            schema: Arc::new(schema),
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
        build_record_batch(Arc::clone(&self.schema), events)
    }
}

impl tokio_util::codec::Encoder<Vec<Event>> for ArrowStreamSerializer {
    type Error = ArrowEncodingError;

    fn encode(&mut self, events: Vec<Event>, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        if events.is_empty() {
            return Err(ArrowEncodingError::NoEvents);
        }

        let bytes = encode_events_to_arrow_ipc_stream(&events, Some(Arc::clone(&self.schema)))?;

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
    let num_fields = schema.fields().len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_fields);

    for field in schema.fields() {
        let field_name = field.name();
        let nullable = field.is_nullable();
        let array: ArrayRef = match field.data_type() {
            DataType::Timestamp(time_unit, _) => {
                build_timestamp_array(events, field_name, *time_unit, nullable)?
            }
            DataType::Utf8 => build_string_array(events, field_name, nullable)?,
            DataType::LargeUtf8 => build_large_string_array(events, field_name, nullable)?,
            DataType::Int8 => build_int8_array(events, field_name, nullable)?,
            DataType::Int16 => build_int16_array(events, field_name, nullable)?,
            DataType::Int32 => build_int32_array(events, field_name, nullable)?,
            DataType::Int64 => build_int64_array(events, field_name, nullable)?,
            DataType::UInt8 => build_uint8_array(events, field_name, nullable)?,
            DataType::UInt16 => build_uint16_array(events, field_name, nullable)?,
            DataType::UInt32 => build_uint32_array(events, field_name, nullable)?,
            DataType::UInt64 => build_uint64_array(events, field_name, nullable)?,
            DataType::Float32 => build_float32_array(events, field_name, nullable)?,
            DataType::Float64 => build_float64_array(events, field_name, nullable)?,
            DataType::Boolean => build_boolean_array(events, field_name, nullable)?,
            DataType::Binary => build_binary_array(events, field_name, nullable)?,
            DataType::LargeBinary => build_large_binary_array(events, field_name, nullable)?,
            DataType::Decimal128(precision, scale) => {
                build_decimal128_array(events, field_name, *precision, *scale, nullable)?
            }
            DataType::Decimal256(precision, scale) => {
                build_decimal256_array(events, field_name, *precision, *scale, nullable)?
            }
            DataType::Struct(fields) => build_struct_array(events, field_name, fields, nullable)?,
            DataType::Map(entries_field, _sorted) => {
                build_map_array(events, field_name, entries_field, nullable)?
            }
            DataType::List(item_field) => {
                build_list_array(events, field_name, item_field, nullable)?
            }
            other_type => {
                return Err(ArrowEncodingError::UnsupportedType {
                    field_name: field_name.into(),
                    data_type: other_type.clone(),
                });
            }
        };

        columns.push(array);
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
        ) -> Result<ArrayRef, ArrowEncodingError> {
            let mut builder = <$builder_ty>::with_capacity(events.len());

            for event in events {
                if let Event::Log(log) = event {
                    match log.get(field_name) {
                        $(
                            $value_pat $(if $guard)? => builder.append_value($append_expr),
                        )+
                        // All other patterns are treated as null/invalid
                        _ => handle_null_constraints!(builder, nullable, field_name),
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
    nullable: bool,
) -> Result<ArrayRef, ArrowEncodingError> {
    macro_rules! build_array {
        ($builder:ty, $converter:expr) => {{
            let mut builder = <$builder>::with_capacity(events.len());
            for event in events {
                if let Event::Log(log) = event {
                    let value_to_append = log.get(field_name).and_then(|value| {
                        // First, try to extract it as a native or string timestamp
                        if let Some(ts) = extract_timestamp(value) {
                            $converter(&ts)
                        }
                        // Else, fall back to a raw integer
                        else if let Value::Integer(i) = value {
                            Some(*i)
                        }
                        // Else, it's an unsupported type (e.g., Bool, Float)
                        else {
                            None
                        }
                    });

                    if value_to_append.is_none() && !nullable {
                        return Err(ArrowEncodingError::NullConstraint {
                            field_name: field_name.into(),
                        });
                    }

                    builder.append_option(value_to_append);
                }
            }
            Ok(Arc::new(builder.finish()))
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

fn build_string_array(
    events: &[Event],
    field_name: &str,
    nullable: bool,
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
                    _ => {
                        builder.append_value(&value.to_string_lossy());
                        appended = true;
                    }
                }
            }

            if !appended {
                handle_null_constraints!(builder, nullable, field_name);
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

define_build_primitive_array_fn!(
    build_int64_array,
    Int64Builder,
    Some(Value::Integer(i)) => *i
);

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
) -> Result<ArrayRef, ArrowEncodingError> {
    let mut builder = BinaryBuilder::with_capacity(events.len(), 0);

    for event in events {
        if let Event::Log(log) = event {
            match log.get(field_name) {
                Some(Value::Bytes(bytes)) => builder.append_value(bytes),
                _ => handle_null_constraints!(builder, nullable, field_name),
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn build_large_string_array(
    events: &[Event],
    field_name: &str,
    nullable: bool,
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
                    _ => {
                        builder.append_value(&value.to_string_lossy());
                        appended = true;
                    }
                }
            }

            if !appended {
                handle_null_constraints!(builder, nullable, field_name);
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn build_large_binary_array(
    events: &[Event],
    field_name: &str,
    nullable: bool,
) -> Result<ArrayRef, ArrowEncodingError> {
    let mut builder = LargeBinaryBuilder::with_capacity(events.len(), 0);

    for event in events {
        if let Event::Log(log) = event {
            match log.get(field_name) {
                Some(Value::Bytes(bytes)) => builder.append_value(bytes),
                _ => handle_null_constraints!(builder, nullable, field_name),
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
            let mut appended = false;
            match log.get(field_name) {
                Some(Value::Float(f)) => {
                    if let Ok(mut decimal) = Decimal::try_from(f.into_inner()) {
                        decimal.rescale(target_scale);
                        let mantissa = decimal.mantissa();
                        builder.append_value(mantissa);
                        appended = true;
                    }
                }
                Some(Value::Integer(i)) => {
                    let mut decimal = Decimal::from(*i);
                    decimal.rescale(target_scale);
                    let mantissa = decimal.mantissa();
                    builder.append_value(mantissa);
                    appended = true;
                }
                _ => {}
            }

            if !appended {
                handle_null_constraints!(builder, nullable, field_name);
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
            let mut appended = false;
            match log.get(field_name) {
                Some(Value::Float(f)) => {
                    if let Ok(mut decimal) = Decimal::try_from(f.into_inner()) {
                        decimal.rescale(target_scale);
                        let mantissa = decimal.mantissa();
                        // rust_decimal does not support i256 natively so we upcast here
                        builder.append_value(i256::from_i128(mantissa));
                        appended = true;
                    }
                }
                Some(Value::Integer(i)) => {
                    let mut decimal = Decimal::from(*i);
                    decimal.rescale(target_scale);
                    let mantissa = decimal.mantissa();
                    builder.append_value(i256::from_i128(mantissa));
                    appended = true;
                }
                _ => {}
            }

            if !appended {
                handle_null_constraints!(builder, nullable, field_name);
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
) -> Result<ArrayRef, ArrowEncodingError> {
    let nullable = field.is_nullable();
    match field.data_type() {
        DataType::Struct(fields) => build_struct_array(events, path, fields, nullable),
        DataType::Map(entries_field, _sorted) => {
            build_map_array(events, path, entries_field, nullable)
        }
        DataType::List(item_field) => build_list_array(events, path, item_field, nullable),
        DataType::Timestamp(time_unit, _) => {
            build_timestamp_array(events, path, *time_unit, nullable)
        }
        DataType::Utf8 => build_string_array(events, path, nullable),
        DataType::LargeUtf8 => build_large_string_array(events, path, nullable),
        DataType::Int8 => build_int8_array(events, path, nullable),
        DataType::Int16 => build_int16_array(events, path, nullable),
        DataType::Int32 => build_int32_array(events, path, nullable),
        DataType::Int64 => build_int64_array(events, path, nullable),
        DataType::UInt8 => build_uint8_array(events, path, nullable),
        DataType::UInt16 => build_uint16_array(events, path, nullable),
        DataType::UInt32 => build_uint32_array(events, path, nullable),
        DataType::UInt64 => build_uint64_array(events, path, nullable),
        DataType::Float32 => build_float32_array(events, path, nullable),
        DataType::Float64 => build_float64_array(events, path, nullable),
        DataType::Boolean => build_boolean_array(events, path, nullable),
        DataType::Binary => build_binary_array(events, path, nullable),
        DataType::LargeBinary => build_large_binary_array(events, path, nullable),
        DataType::Decimal128(precision, scale) => {
            build_decimal128_array(events, path, *precision, *scale, nullable)
        }
        DataType::Decimal256(precision, scale) => {
            build_decimal256_array(events, path, *precision, *scale, nullable)
        }
        other_type => Err(ArrowEncodingError::UnsupportedType {
            field_name: path.into(),
            data_type: other_type.clone(),
        }),
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
) -> Result<ArrayRef, ArrowEncodingError> {
    let child_arrays: Vec<ArrayRef> = fields
        .iter()
        .map(|child_field| {
            let child_path = format!("{}.{}", path, child_field.name());
            build_column_for_path(events, &child_path, child_field)
        })
        .collect::<Result<_, _>>()?;

    let mut has_null = false;
    let mut validity: Vec<bool> = Vec::with_capacity(events.len());
    for event in events {
        if let Event::Log(log) = event {
            let valid = log.get(path).is_some();
            if !valid {
                if !nullable {
                    return Err(ArrowEncodingError::NullConstraint {
                        field_name: path.into(),
                    });
                }
                has_null = true;
            }
            validity.push(valid);
        } else {
            validity.push(false);
            has_null = true;
        }
    }
    let null_buffer = has_null.then(|| NullBuffer::from(validity));

    let struct_array = StructArray::try_new(fields.clone(), child_arrays, null_buffer)
        .map_err(|source| ArrowEncodingError::RecordBatchCreation { source })?;
    Ok(Arc::new(struct_array))
}

/// Builds an Arrow `MapArray` for a map field at the given path.
///
/// The Vector event value at `path` must be a `Value::Object` (string-keyed map).
/// Supported map value types: `LargeUtf8`, `Utf8`, `Boolean`, `Int32`, `Int64`,
/// `UInt32`, `UInt64`, `Float32`, `Float64`.
fn build_map_array(
    events: &[Event],
    path: &str,
    entries_field: &Field,
    nullable: bool,
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
    let value_field = &kv_fields[1];

    let mut key_builder = LargeStringBuilder::new();
    let mut flat_values: Vec<Value> = Vec::new();
    let mut offsets: Vec<i32> = Vec::with_capacity(events.len() + 1);
    let mut validity: Vec<bool> = Vec::with_capacity(events.len());
    let mut current_offset: i32 = 0;
    offsets.push(0);

    for event in events {
        if let Event::Log(log) = event {
            match log.get(path) {
                Some(Value::Object(obj)) => {
                    validity.push(true);
                    for (k, v) in obj.iter() {
                        key_builder.append_value(k.as_str());
                        flat_values.push(v.clone());
                        current_offset += 1;
                    }
                }
                _ => {
                    if !nullable {
                        return Err(ArrowEncodingError::NullConstraint {
                            field_name: path.into(),
                        });
                    }
                    validity.push(false);
                }
            }
        } else {
            validity.push(false);
        }
        offsets.push(current_offset);
    }

    let key_array: ArrayRef = Arc::new(key_builder.finish());
    let value_array = build_map_value_array(&flat_values, value_field)?;

    let entries_array =
        StructArray::try_new(kv_fields.clone(), vec![key_array, value_array], None)
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
/// and nested `List`.  Absent or non-array values produce a null list row when
/// the field is nullable; non-nullable missing values return `NullConstraint`.
fn build_list_array(
    events: &[Event],
    path: &str,
    item_field: &Field,
    nullable: bool,
) -> Result<ArrayRef, ArrowEncodingError> {
    let mut flat_items: Vec<Value> = Vec::new();
    let mut offsets: Vec<i32> = vec![0];
    let mut validity: Vec<bool> = Vec::with_capacity(events.len());
    let mut has_null = false;

    for event in events {
        let arr_opt = if let Event::Log(log) = event {
            log.get(path).and_then(|v| {
                if let Value::Array(a) = v {
                    Some(a.clone())
                } else {
                    None
                }
            })
        } else {
            None
        };

        match arr_opt {
            Some(arr) => {
                flat_items.extend(arr.into_iter());
                offsets.push(flat_items.len() as i32);
                validity.push(true);
            }
            None => {
                if !nullable {
                    return Err(ArrowEncodingError::NullConstraint {
                        field_name: path.into(),
                    });
                }
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
fn build_list_item_array(
    items: &[Value],
    field: &Field,
) -> Result<ArrayRef, ArrowEncodingError> {
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
        _ => build_map_value_array(items, field),
    }
}

/// Builds a flat value array for Map entries from pre-collected Vector values.
fn build_map_value_array(
    values: &[Value],
    field: &Field,
) -> Result<ArrayRef, ArrowEncodingError> {
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
            Array, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray,
            TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
            TimestampSecondArray,
        },
        datatypes::Field,
        ipc::reader::StreamReader,
    };
    use chrono::Utc;
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
            DataType::Struct(Fields::from(vec![Field::new("key", DataType::LargeUtf8, true)])),
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

    // -------------------------------------------------------------------------
    // List encoding tests
    // -------------------------------------------------------------------------

    /// `repeated int64` — mirrors `flag_evaluation_hashes` in service_health_event.proto
    /// and `contributing_query_ids` (repeated string) pattern.
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
        log1.insert(
            "hashes",
            Value::Array(vec![Value::Integer(999)]),
        );

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

    /// `repeated string` — mirrors `contributing_query_ids` / `redacted_task_retryable_errors`.
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
        let row0 = row0_val.as_any().downcast_ref::<LargeStringArray>().unwrap();
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

    /// `List<Struct>` — mirrors repeated message fields like QPL's `stage_data`.
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
        let item_field = Field::new(
            "item",
            DataType::Struct(item_struct_fields.clone()),
            true,
        );
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

        let map_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
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

        let map_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();

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

        let map_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
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

    // -------------------------------------------------------------------------
    // Realistic proto-schema tests
    // -------------------------------------------------------------------------

    /// Schema mirroring proto/logs/sla/service_health_event.proto.
    ///
    /// Covers:
    ///   - top-level scalar fields (LargeUtf8, Int32, Int64, Boolean)
    ///   - deprecated `rpc_info` Struct with two string sub-fields
    ///   - `request_info` Struct with string and Int32 sub-fields
    ///   - null handling: rows where optional struct fields are absent
    #[test]
    fn test_encode_service_health_event_schema() {
        use arrow::array::{BooleanArray, Int32Array, Int64Array, LargeStringArray, StructArray};

        let rpc_info_fields = Fields::from(vec![
            Field::new("rpc_handler", DataType::LargeUtf8, true),
            Field::new("exception_class", DataType::LargeUtf8, true),
        ]);
        let request_info_fields = Fields::from(vec![
            Field::new("request_type", DataType::Int32, true),
            Field::new("handler", DataType::LargeUtf8, true),
            Field::new("exception_class", DataType::LargeUtf8, true),
            Field::new("http_method", DataType::LargeUtf8, true),
            Field::new("source_type", DataType::Int32, true),
            Field::new("retry_count", DataType::Int32, true),
        ]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("event_name", DataType::LargeUtf8, true),
            Field::new("outcome", DataType::LargeUtf8, true),
            Field::new("outcome_type", DataType::Int32, true),
            Field::new("duration_ms", DataType::Int64, true),
            Field::new("workspace_id", DataType::Int64, true),
            Field::new("rpc_info", DataType::Struct(rpc_info_fields.clone()), true),
            Field::new(
                "request_info",
                DataType::Struct(request_info_fields.clone()),
                true,
            ),
            Field::new("classification_low_confidence", DataType::Boolean, true),
            Field::new("dbr_version", DataType::LargeUtf8, true),
            Field::new("outcome_details", DataType::LargeUtf8, true),
            Field::new("is_suppressed", DataType::Boolean, true),
            Field::new("engine_request_id", DataType::LargeUtf8, true),
            Field::new("service_extra_v2", DataType::LargeBinary, true),
            Field::new(
                "flag_evaluation_hashes",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ),
        ]));

        // Row 0: all fields populated (nullable sub-fields simply omitted → null in Arrow).
        let mut log0 = LogEvent::default();
        log0.insert("event_name", "JobRunTermination");
        log0.insert("outcome", "Success");
        log0.insert("outcome_type", 1i64); // SUCCESS enum → Int32
        log0.insert("duration_ms", 42_000i64);
        log0.insert("workspace_id", 123_456_789i64);
        log0.insert("rpc_info.rpc_handler", "com.databricks.api.Jobs");
        // rpc_info.exception_class absent → null
        log0.insert("request_info.request_type", 1i64); // RPC
        log0.insert("request_info.handler", "com.databricks.api.Jobs");
        // request_info.exception_class absent → null
        // request_info.http_method absent → null
        log0.insert("request_info.source_type", 2i64); // SERVER_SIDE
        log0.insert("request_info.retry_count", 0i64);
        log0.insert("classification_low_confidence", false);
        log0.insert("dbr_version", "16.4.7");
        log0.insert("outcome_details", "JobRunTermination succeeded after 42s");
        log0.insert("is_suppressed", false);
        log0.insert("engine_request_id", "abc-def-123");
        log0.insert(
            "service_extra_v2",
            Value::Bytes(b"\x0a\x05hello".to_vec().into()),
        );
        log0.insert(
            "flag_evaluation_hashes",
            Value::Array(vec![
                Value::Integer(111_222_333),
                Value::Integer(444_555_666),
            ]),
        );

        // Row 1: rpc_info and request_info absent → null structs; several fields absent → null.
        let mut log1 = LogEvent::default();
        log1.insert("event_name", "ClusterTermination");
        log1.insert("outcome", "CloudFailure");
        log1.insert("outcome_type", 2i64);
        log1.insert("duration_ms", 5_000i64);
        log1.insert("workspace_id", 987_654_321i64);
        log1.insert("classification_low_confidence", true);
        log1.insert("dbr_version", "14.3.0");
        // outcome_details absent → null
        log1.insert("is_suppressed", true);
        // engine_request_id absent → null
        // service_extra_v2 absent → null
        // flag_evaluation_hashes absent → null

        let events = vec![Event::Log(log0), Event::Log(log1)];

        let result = build_record_batch(Arc::clone(&schema), &events);
        assert!(result.is_ok(), "build_record_batch failed: {:?}", result);
        let batch = result.unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 14);

        // Top-level scalar checks.
        let event_name_col = batch
            .column_by_name("event_name")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        assert_eq!(event_name_col.value(0), "JobRunTermination");
        assert_eq!(event_name_col.value(1), "ClusterTermination");

        let outcome_type_col = batch
            .column_by_name("outcome_type")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(outcome_type_col.value(0), 1);
        assert_eq!(outcome_type_col.value(1), 2);

        let workspace_id_col = batch
            .column_by_name("workspace_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(workspace_id_col.value(0), 123_456_789);
        assert_eq!(workspace_id_col.value(1), 987_654_321);

        // rpc_info: row 0 non-null, row 1 null.
        let rpc_info_col = batch
            .column_by_name("rpc_info")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(!rpc_info_col.is_null(0), "row 0 rpc_info should be non-null");
        assert!(rpc_info_col.is_null(1), "row 1 rpc_info should be null");

        let rpc_handler = rpc_info_col
            .column_by_name("rpc_handler")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        assert_eq!(rpc_handler.value(0), "com.databricks.api.Jobs");

        // request_info: row 0 non-null, row 1 null.
        let request_info_col = batch
            .column_by_name("request_info")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(
            !request_info_col.is_null(0),
            "row 0 request_info should be non-null"
        );
        assert!(
            request_info_col.is_null(1),
            "row 1 request_info should be null"
        );

        let retry_count = request_info_col
            .column_by_name("retry_count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(retry_count.value(0), 0);

        // Boolean fields.
        let is_suppressed_col = batch
            .column_by_name("is_suppressed")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(!is_suppressed_col.value(0));
        assert!(is_suppressed_col.value(1));

        // service_extra_v2 (LargeBinary): row 0 non-null, row 1 null.
        use arrow::array::LargeBinaryArray;
        let binary_col = batch
            .column_by_name("service_extra_v2")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert!(!binary_col.is_null(0));
        assert!(binary_col.is_null(1));
        assert_eq!(binary_col.value(0), b"\x0a\x05hello");

        // flag_evaluation_hashes (List<Int64>): row 0 has [111_222_333, 444_555_666], row 1 null.
        use arrow::array::ListArray;
        let list_col = batch
            .column_by_name("flag_evaluation_hashes")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(!list_col.is_null(0));
        assert!(list_col.is_null(1));
        let hashes: Vec<i64> = list_col
            .value(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .flatten()
            .collect();
        assert_eq!(hashes, vec![111_222_333, 444_555_666]);
    }

    /// Schema mirroring the core fields of proto/logs/qpl/query_profile.proto.
    ///
    /// Covers:
    ///   - top-level scalar and enum (Int32/Int64/Boolean/LargeUtf8) fields
    ///   - `failure` Struct (QueryFailure: error_class, sub_error_class, sql_state,
    ///     redacted_exception)
    ///   - `query_metrics` Struct (parsing_time_ns, analysis_time_ns,
    ///     optimization_time_ns, physical_planning_time_ns — all Int64)
    ///   - `query_profile_debug` Struct (logging_overhead_ns Int64, error_class
    ///     LargeUtf8, sequence_number Int64)
    ///   - null handling: row with no failure struct
    #[test]
    fn test_encode_query_profile_schema() {
        use arrow::array::{BooleanArray, Int32Array, Int64Array, LargeStringArray, StructArray};

        let failure_fields = Fields::from(vec![
            Field::new("error_class", DataType::LargeUtf8, true),
            Field::new("sub_error_class", DataType::LargeUtf8, true),
            Field::new("sql_state", DataType::LargeUtf8, true),
            Field::new("redacted_exception", DataType::LargeUtf8, true),
        ]);
        let query_metrics_fields = Fields::from(vec![
            Field::new("parsing_time_ns", DataType::Int64, true),
            Field::new("analysis_time_ns", DataType::Int64, true),
            Field::new("optimization_time_ns", DataType::Int64, true),
            Field::new("physical_planning_time_ns", DataType::Int64, true),
        ]);
        let debug_fields = Fields::from(vec![
            Field::new("logging_overhead_ns", DataType::Int64, true),
            Field::new("error_class", DataType::LargeUtf8, true),
            Field::new("sequence_number", DataType::Int64, true),
        ]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::LargeUtf8, true),
            Field::new(
                "contributing_query_ids",
                DataType::List(Arc::new(Field::new("item", DataType::LargeUtf8, true))),
                true,
            ),
            Field::new("app_id", DataType::LargeUtf8, true),
            Field::new("execution_id", DataType::LargeUtf8, true),
            Field::new("time_submitted_unix_ms", DataType::Int64, true),
            Field::new("time_completed_unix_ms", DataType::Int64, true),
            Field::new("is_streaming", DataType::Boolean, true),
            Field::new("is_success", DataType::Boolean, true),
            Field::new("entry_point", DataType::Int32, true),
            Field::new("exception", DataType::LargeUtf8, true),
            Field::new("failure", DataType::Struct(failure_fields.clone()), true),
            Field::new(
                "query_metrics",
                DataType::Struct(query_metrics_fields.clone()),
                true,
            ),
            Field::new(
                "query_profile_debug",
                DataType::Struct(debug_fields.clone()),
                true,
            ),
        ]));

        // Row 0: successful query — exception and failure absent → null.
        let mut log0 = LogEvent::default();
        log0.insert("id", "qpl-2024-01-15-00-00-abc123");
        log0.insert(
            "contributing_query_ids",
            Value::Array(vec![
                Value::Bytes("qpl-prev-1".into()),
                Value::Bytes("qpl-prev-2".into()),
            ]),
        );
        log0.insert("app_id", "application_1234_0001");
        log0.insert("execution_id", "exec-42");
        log0.insert("time_submitted_unix_ms", 1_705_276_800_000i64);
        log0.insert("time_completed_unix_ms", 1_705_276_802_500i64);
        log0.insert("is_streaming", false);
        log0.insert("is_success", true);
        log0.insert("entry_point", 2i64); // THRIFT_SERVER
        // exception absent → null
        // failure fields absent → null struct
        log0.insert("query_metrics.parsing_time_ns", 50_000i64);
        log0.insert("query_metrics.analysis_time_ns", 120_000i64);
        log0.insert("query_metrics.optimization_time_ns", 80_000i64);
        log0.insert("query_metrics.physical_planning_time_ns", 30_000i64);
        log0.insert("query_profile_debug.logging_overhead_ns", 1_500i64);
        // query_profile_debug.error_class absent → null
        log0.insert("query_profile_debug.sequence_number", 7i64);

        // Row 1: failed query — contributing_query_ids absent → null list.
        let mut log1 = LogEvent::default();
        log1.insert("id", "qpl-2024-01-15-00-01-xyz789");
        // contributing_query_ids absent → null
        log1.insert("app_id", "application_1234_0002");
        log1.insert("execution_id", "exec-43");
        log1.insert("time_submitted_unix_ms", 1_705_276_810_000i64);
        log1.insert("time_completed_unix_ms", 1_705_276_810_200i64);
        log1.insert("is_streaming", false);
        log1.insert("is_success", false);
        log1.insert("entry_point", 2i64); // THRIFT_SERVER
        log1.insert("exception", "AnalysisException");
        log1.insert("failure.error_class", "TABLE_OR_VIEW_NOT_FOUND");
        // failure.sub_error_class absent → null
        log1.insert("failure.sql_state", "42P01");
        log1.insert("failure.redacted_exception", "Table or view not found: foo");
        log1.insert("query_metrics.parsing_time_ns", 10_000i64);
        log1.insert("query_metrics.analysis_time_ns", 5_000i64);
        // query_metrics.optimization_time_ns absent → null
        // query_metrics.physical_planning_time_ns absent → null
        log1.insert("query_profile_debug.logging_overhead_ns", 900i64);
        log1.insert("query_profile_debug.error_class", "SerializationError");
        log1.insert("query_profile_debug.sequence_number", 8i64);

        let events = vec![Event::Log(log0), Event::Log(log1)];

        let result = build_record_batch(Arc::clone(&schema), &events);
        assert!(result.is_ok(), "build_record_batch failed: {:?}", result);
        let batch = result.unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 13);

        // Top-level scalar checks.
        let id_col = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        assert_eq!(id_col.value(0), "qpl-2024-01-15-00-00-abc123");
        assert_eq!(id_col.value(1), "qpl-2024-01-15-00-01-xyz789");

        let is_success_col = batch
            .column_by_name("is_success")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(is_success_col.value(0));
        assert!(!is_success_col.value(1));

        let entry_point_col = batch
            .column_by_name("entry_point")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(entry_point_col.value(0), 2);
        assert_eq!(entry_point_col.value(1), 2);

        // failure struct: row 0 null, row 1 non-null.
        let failure_col = batch
            .column_by_name("failure")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(failure_col.is_null(0), "row 0 failure should be null");
        assert!(!failure_col.is_null(1), "row 1 failure should be non-null");

        let error_class = failure_col
            .column_by_name("error_class")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        assert_eq!(error_class.value(1), "TABLE_OR_VIEW_NOT_FOUND");

        let sql_state = failure_col
            .column_by_name("sql_state")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        assert_eq!(sql_state.value(1), "42P01");

        // query_metrics struct: both rows non-null.
        let metrics_col = batch
            .column_by_name("query_metrics")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(!metrics_col.is_null(0));
        assert!(!metrics_col.is_null(1));

        let parsing_ns = metrics_col
            .column_by_name("parsing_time_ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(parsing_ns.value(0), 50_000);
        assert_eq!(parsing_ns.value(1), 10_000);

        // query_profile_debug struct: both rows non-null.
        let debug_col = batch
            .column_by_name("query_profile_debug")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(!debug_col.is_null(0));
        assert!(!debug_col.is_null(1));

        let seq_num = debug_col
            .column_by_name("sequence_number")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(seq_num.value(0), 7);
        assert_eq!(seq_num.value(1), 8);

        let debug_error_class = debug_col
            .column_by_name("error_class")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        assert!(debug_error_class.is_null(0));
        assert_eq!(debug_error_class.value(1), "SerializationError");

        // contributing_query_ids (List<LargeUtf8>): row 0 has 2 ids, row 1 null.
        use arrow::array::ListArray;
        let cq_col = batch
            .column_by_name("contributing_query_ids")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(!cq_col.is_null(0), "row 0 contributing_query_ids non-null");
        assert!(cq_col.is_null(1), "row 1 contributing_query_ids null");
        let cq_val = cq_col.value(0);
        let ids: Vec<&str> = cq_val
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .iter()
            .flatten()
            .collect();
        assert_eq!(ids, vec!["qpl-prev-1", "qpl-prev-2"]);
    }
}
