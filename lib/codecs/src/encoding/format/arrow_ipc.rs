//! `arrow_ipc` batch codec: Arrow record batches that an upstream component already built.
//!
//! Each event's `message` field holds an Arrow IPC *stream* (schema, one or more record batches,
//! end-of-stream). The serializer
//! decodes every event's batches, conforms each one to the sink's target schema, and concatenates
//! them into one `RecordBatch`. Nothing is re-derived per row from event fields, so a batch that
//! several sinks receive is built once upstream instead of once per sink.
//!
//! Conforming a source batch to the target schema, per target column (matched by name):
//! - same data type: the column is used as is (zero-copy);
//! - same physical layout but different nested field names, nullability or metadata (for example
//!   map `entries`/`keys`/`values` naming): the column is re-wrapped with the target's fields,
//!   without copying its buffers;
//! - `Utf8` <-> `LargeUtf8`: cast;
//! - `Timestamp` with the same unit and a different time zone: re-tagged (same instants);
//! - column absent from the source batch: all-null column when the target is nullable, else a
//!   null-constraint error (as `arrow_stream` does for an absent field);
//! - anything else (for example a table column retyped to INT or DATE): the column's values are
//!   turned back into event values and the column is built by `arrow_stream`'s own builder, so it
//!   gets exactly the coercion `arrow_stream` applies to the per-sample events; only values that
//!   cannot be turned back (none that `hydra_remote_write` produces) remain an `UnsupportedType`
//!   error.

use std::sync::Arc;

#[cfg(test)]
use arrow::datatypes::Fields;
use arrow::{
    array::{Array, ArrayRef, AsArray, RecordBatch, new_null_array},
    buffer::Buffer,
    compute::concat_batches,
    datatypes::{DataType, Field, Schema, SchemaRef},
    ipc::reader::StreamDecoder,
};
use vector_config::configurable_component;
use vector_core::{config::DataType as EventDataType, event::Event, schema};

use super::arrow::{ArrowEncodingError, build_column_for_path};

/// Config for the `arrow_ipc` batch codec.
#[configurable_component]
#[derive(Clone, Default)]
pub struct ArrowIpcSerializerConfig {
    /// The target Arrow schema. Injected by the sink (for example from the Unity Catalog table),
    /// never read from the config file.
    #[serde(skip)]
    #[configurable(derived)]
    pub schema: Option<Schema>,
}

impl std::fmt::Debug for ArrowIpcSerializerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrowIpcSerializerConfig")
            .field("schema", &self.schema.as_ref().map(|s| s.fields().len()))
            .finish()
    }
}

impl ArrowIpcSerializerConfig {
    /// The data type of events accepted by this codec.
    pub fn input_type(&self) -> EventDataType {
        EventDataType::Log
    }

    /// The schema required by this codec.
    pub fn schema_requirement(&self) -> schema::Requirement {
        schema::Requirement::empty()
    }
}

/// Decodes Arrow IPC streams carried in events and conforms them to a target schema.
#[derive(Clone, Debug)]
pub struct ArrowIpcSerializer {
    schema: SchemaRef,
}

impl ArrowIpcSerializer {
    /// Creates the serializer. Requires the target schema.
    pub fn new(config: ArrowIpcSerializerConfig) -> Result<Self, vector_common::Error> {
        let schema = config
            .schema
            .ok_or_else(|| vector_common::Error::from("arrow_ipc codec requires a schema."))?;
        Ok(Self {
            schema: Arc::new(schema),
        })
    }

    /// The target schema.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Decode every event's batches, conform them to the target schema, and concatenate them.
    pub fn encode_to_record_batch(
        &self,
        events: &[Event],
    ) -> Result<RecordBatch, ArrowEncodingError> {
        if events.is_empty() {
            return Err(ArrowEncodingError::NoEvents);
        }
        let mut batches = Vec::with_capacity(events.len());
        for event in events {
            let bytes = match event {
                Event::Log(log) => match log.get_message() {
                    Some(vector_core::event::Value::Bytes(bytes)) => bytes.clone(),
                    _ => return Err(invalid("message")),
                },
                _ => return Err(invalid("message")),
            };
            // Zero-copy: the decoded arrays point into the event's buffer (copied only where a
            // buffer is misaligned for its type).
            let mut buffer = Buffer::from(bytes);
            let mut decoder = StreamDecoder::new();
            let ipc_error = |source| ArrowEncodingError::RecordBatchCreation { source };
            while !buffer.is_empty() {
                if let Some(batch) = decoder.decode(&mut buffer).map_err(ipc_error)? {
                    batches.push(conform_batch(&self.schema, &batch)?);
                }
            }
            decoder.finish().map_err(ipc_error)?;
        }
        concat_batches(&self.schema, &batches)
            .map_err(|source| ArrowEncodingError::RecordBatchCreation { source })
    }
}

fn invalid(field_name: &str) -> ArrowEncodingError {
    ArrowEncodingError::InvalidValue {
        field_name: field_name.into(),
    }
}

fn unsupported(field_name: &str, data_type: &DataType) -> ArrowEncodingError {
    ArrowEncodingError::UnsupportedType {
        field_name: field_name.into(),
        data_type: data_type.clone(),
    }
}

/// Conform one source batch to `schema`, column by column (see the module docs).
pub fn conform_batch(
    schema: &SchemaRef,
    batch: &RecordBatch,
) -> Result<RecordBatch, ArrowEncodingError> {
    let rows = batch.num_rows();
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let column = match batch.column_by_name(field.name()) {
            Some(column) => match conform_array(column, field) {
                Ok(column) => column,
                Err(error @ ArrowEncodingError::UnsupportedType { .. }) => {
                    rebuild_as_arrow_stream(column, field).ok_or(error)??
                }
                Err(error) => return Err(error),
            },
            None if field.is_nullable() => new_null_array(field.data_type(), rows),
            None => {
                return Err(ArrowEncodingError::NullConstraint {
                    field_name: field.name().clone(),
                });
            }
        };
        if !field.is_nullable() && column.null_count() > 0 {
            return Err(ArrowEncodingError::NullConstraint {
                field_name: field.name().clone(),
            });
        }
        columns.push(column);
    }
    RecordBatch::try_new(Arc::clone(schema), columns)
        .map_err(|source| ArrowEncodingError::RecordBatchCreation { source })
}

/// Build `field`'s column as `arrow_stream` would from the per-sample events: each row's value is
/// turned back into the event value it was built from, and `arrow_stream`'s column builder does
/// the rest (coercion, defaults, null constraints). `None` when some value cannot be turned back.
fn rebuild_as_arrow_stream(
    column: &ArrayRef,
    field: &Field,
) -> Option<Result<ArrayRef, ArrowEncodingError>> {
    use vector_core::event::LogEvent;
    use vrl::value::{KeyString, ObjectMap};
    let mut events = Vec::with_capacity(column.len());
    for row in 0..column.len() {
        let value = event_value(column.as_ref(), row)?;
        let map = ObjectMap::from([(KeyString::from(field.name().as_str()), value)]);
        events.push(Event::Log(LogEvent::from_map(map, Default::default())));
    }
    Some(build_column_for_path(&events, field.name(), field, None))
}

/// The event value an upstream encoded column holds at `row`: strings as bytes,
/// integers, finite floats, timestamps, lists, structs and string maps as objects, and a VARIANT
/// struct as the JSON value it encodes. A null is `Value::Null`, as the per-sample event holds it.
fn event_value(array: &dyn Array, row: usize) -> Option<vrl::value::Value> {
    use arrow::datatypes::{
        Float64Type, Int32Type, Int64Type, TimeUnit, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
    };
    use chrono::DateTime;
    use vrl::value::{KeyString, ObjectMap, Value};
    if array.is_null(row) {
        return Some(Value::Null);
    }
    Some(match array.data_type() {
        DataType::Utf8 => Value::from(array.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => Value::from(array.as_string::<i64>().value(row)),
        DataType::Int64 => Value::Integer(array.as_primitive::<Int64Type>().value(row)),
        DataType::Int32 => Value::Integer(i64::from(array.as_primitive::<Int32Type>().value(row))),
        DataType::Float64 => Value::Float(
            ordered_float::NotNan::new(array.as_primitive::<Float64Type>().value(row)).ok()?,
        ),
        DataType::Boolean => Value::Boolean(array.as_boolean().value(row)),
        DataType::Timestamp(unit, _) => {
            let t = match unit {
                TimeUnit::Second => DateTime::from_timestamp(
                    array.as_primitive::<TimestampSecondType>().value(row),
                    0,
                ),
                TimeUnit::Millisecond => DateTime::from_timestamp_millis(
                    array.as_primitive::<TimestampMillisecondType>().value(row),
                ),
                TimeUnit::Microsecond => DateTime::from_timestamp_micros(
                    array.as_primitive::<TimestampMicrosecondType>().value(row),
                ),
                TimeUnit::Nanosecond => Some(DateTime::from_timestamp_nanos(
                    array.as_primitive::<TimestampNanosecondType>().value(row),
                )),
            }?;
            Value::Timestamp(t)
        }
        DataType::List(_) => {
            let items = array.as_list::<i32>().value(row);
            Value::Array(
                (0..items.len())
                    .map(|i| event_value(items.as_ref(), i))
                    .collect::<Option<Vec<_>>>()?,
            )
        }
        DataType::Struct(fields)
            if fields.len() == 2
                && fields[0].name() == "metadata"
                && fields[1].name() == "value"
                && matches!(fields[0].data_type(), DataType::LargeBinary)
                && matches!(fields[1].data_type(), DataType::LargeBinary) =>
        {
            use parquet_variant_json::VariantToJson;
            let s = array.as_struct();
            let metadata = s.column(0).as_binary::<i64>().value(row);
            let value = s.column(1).as_binary::<i64>().value(row);
            let json = parquet_variant::Variant::try_new(metadata, value)
                .ok()?
                .to_json_value()
                .ok()?;
            Value::from(json)
        }
        DataType::Struct(fields) => {
            let s = array.as_struct();
            let mut map = ObjectMap::new();
            for (i, f) in fields.iter().enumerate() {
                map.insert(
                    KeyString::from(f.name().as_str()),
                    event_value(s.column(i).as_ref(), row)?,
                );
            }
            Value::Object(map)
        }
        DataType::Map(..) => {
            let entries = array.as_map().value(row);
            let (keys, values) = (entries.column(0), entries.column(1));
            let mut map = ObjectMap::new();
            for i in 0..entries.len() {
                let Value::Bytes(k) = event_value(keys.as_ref(), i)? else {
                    return None;
                };
                map.insert(
                    KeyString::from(String::from_utf8_lossy(&k).into_owned()),
                    event_value(values.as_ref(), i)?,
                );
            }
            Value::Object(map)
        }
        _ => return None,
    })
}

/// Conform one array to the target field's type.
fn conform_array(array: &ArrayRef, target: &Field) -> Result<ArrayRef, ArrowEncodingError> {
    if array.data_type() == target.data_type() {
        Ok(Arc::clone(array))
    } else {
        Err(unsupported(target.name(), target.data_type()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(bytes: bytes::Bytes) -> Event {
        let mut log = vector_core::event::LogEvent::default();
        log.insert("message", bytes);
        Event::Log(log)
    }

    #[test]
    fn generic_streams_are_concatenated_and_conformed_by_name() {
        use arrow::array::{Int64Array, StringArray};

        let upstream = RecordBatch::try_from_iter(vec![
            ("count", Arc::new(Int64Array::from(vec![7, 9])) as ArrayRef),
            (
                "label",
                Arc::new(StringArray::from(vec!["plain", "é\n"])) as ArrayRef,
            ),
        ])
        .unwrap();
        let schema = Schema::new(vec![
            Field::new("label", DataType::LargeUtf8, false),
            Field::new("absent", DataType::Int64, true),
            Field::new("count", DataType::Int64, false),
        ]);
        let serializer = ArrowIpcSerializer::new(ArrowIpcSerializerConfig {
            schema: Some(schema.clone()),
        })
        .unwrap();
        let bytes = super::super::arrow::record_batch_to_arrow_ipc_stream(&upstream).unwrap();
        let got = serializer
            .encode_to_record_batch(&[envelope(bytes.clone()), envelope(bytes)])
            .unwrap();
        let conformed = conform_batch(&Arc::new(schema), &upstream).unwrap();
        let expected =
            concat_batches(&conformed.schema(), &[conformed.clone(), conformed]).unwrap();
        assert_eq!(got, expected);
        assert_eq!(got.num_rows(), 4);
        assert_eq!(got.column(1).null_count(), 4);
    }

    #[test]
    fn invalid_envelopes_and_required_nulls_are_rejected() {
        use arrow::array::Int64Array;

        assert!(ArrowIpcSerializer::new(ArrowIpcSerializerConfig::default()).is_err());
        let schema = Schema::new(vec![Field::new("count", DataType::Int64, false)]);
        let serializer = ArrowIpcSerializer::new(ArrowIpcSerializerConfig {
            schema: Some(schema.clone()),
        })
        .unwrap();
        assert!(matches!(
            serializer.encode_to_record_batch(&[]),
            Err(ArrowEncodingError::NoEvents)
        ));
        assert!(
            serializer
                .encode_to_record_batch(&[Event::Log(Default::default())])
                .is_err()
        );
        assert!(
            serializer
                .encode_to_record_batch(&[envelope(bytes::Bytes::from_static(b"not ipc"))])
                .is_err()
        );
        let upstream = RecordBatch::try_from_iter(vec![(
            "count",
            Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef,
        )])
        .unwrap();
        assert!(matches!(
            conform_batch(&Arc::new(schema), &upstream),
            Err(ArrowEncodingError::NullConstraint { .. })
        ));
        let bytes = super::super::arrow::record_batch_to_arrow_ipc_stream(&upstream).unwrap();
        // Remove part of the final record batch, not just the optional end marker.
        assert!(
            serializer
                .encode_to_record_batch(&[envelope(bytes.slice(..bytes.len() / 2))])
                .is_err()
        );
    }

    /// A table column retyped from what the upstream batch holds (INT for BIGINT, DATE for STRING,
    /// a struct child retyped, a map for a pre-encoded VARIANT) is built exactly as `arrow_stream`
    /// builds it from the per-sample events.
    #[test]
    fn retyped_columns_match_arrow_stream() {
        use arrow::array::{
            Float64Builder, Int64Builder, LargeStringBuilder, ListBuilder, MapBuilder,
            StructBuilder, TimestampMicrosecondBuilder,
        };
        use arrow::datatypes::TimeUnit;
        use chrono::{TimeZone, Utc};
        use vector_core::event::LogEvent;
        use vrl::value::{KeyString, ObjectMap, Value};

        let rows = vec![
            (
                5,
                "2026-11-13",
                Some("w1"),
                1_794_537_808_550,
                1.5,
                vec![("a", "x"), ("b", "")],
            ),
            (99, "1999-12-31", None, 1_794_535_200_000, -0.0, vec![]),
            (0, "not-a-date", Some(""), 0, 1e300, vec![("é", "😀\"")]),
        ];
        let ts_type = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let mut part = Int64Builder::new();
        let mut date = LargeStringBuilder::new();
        let mut ws = LargeStringBuilder::new();
        let mut ts = TimestampMicrosecondBuilder::new().with_data_type(ts_type.clone());
        let value_fields: Fields = vec![
            Field::new("ts", DataType::Int64, true),
            Field::new("v", DataType::Float64, true),
        ]
        .into();
        let mut values = ListBuilder::new(StructBuilder::new(
            value_fields.clone(),
            vec![
                Box::new(Int64Builder::new()),
                Box::new(Float64Builder::new()),
            ],
        ));
        let mut labels =
            MapBuilder::new(None, LargeStringBuilder::new(), LargeStringBuilder::new());
        let mut events = Vec::new();
        for (p, d, w, t, v, l) in &rows {
            part.append_value(*p);
            date.append_value(d);
            ws.append_option(*w);
            ts.append_value(t * 1000);
            let item = values.values();
            item.field_builder::<Int64Builder>(0)
                .unwrap()
                .append_value(*t);
            item.field_builder::<Float64Builder>(1)
                .unwrap()
                .append_value(*v);
            item.append(true);
            values.append(true);
            for (k, val) in l {
                labels.keys().append_value(k);
                labels.values().append_value(val);
            }
            labels.append(true).unwrap();
            let mut log = ObjectMap::new();
            log.insert("metric_part".into(), Value::Integer(*p));
            log.insert("_partition_date".into(), Value::from(*d));
            log.insert(
                "workspace_id".into(),
                w.map(Value::from).unwrap_or(Value::Null),
            );
            log.insert(
                "_event_time".into(),
                Value::Timestamp(Utc.timestamp_millis_opt(*t).unwrap()),
            );
            let mut item = ObjectMap::new();
            item.insert("ts".into(), Value::Integer(*t));
            item.insert(
                "v".into(),
                Value::Float(ordered_float::NotNan::new(*v).unwrap()),
            );
            log.insert("values".into(), Value::Array(vec![Value::Object(item)]));
            log.insert(
                "labels".into(),
                Value::Object(
                    l.iter()
                        .map(|(k, v)| (KeyString::from(*k), Value::from(*v)))
                        .collect(),
                ),
            );
            events.push(Event::Log(LogEvent::from_map(log, Default::default())));
        }
        let upstream = RecordBatch::try_from_iter(vec![
            ("metric_part", Arc::new(part.finish()) as ArrayRef),
            ("_partition_date", Arc::new(date.finish()) as ArrayRef),
            ("workspace_id", Arc::new(ws.finish()) as ArrayRef),
            ("_event_time", Arc::new(ts.finish()) as ArrayRef),
            ("values", Arc::new(values.finish()) as ArrayRef),
            ("labels", Arc::new(labels.finish()) as ArrayRef),
        ])
        .unwrap();

        let retyped_values = DataType::List(Arc::new(Field::new(
            "element",
            DataType::Struct(
                vec![
                    Field::new("ts", DataType::Int32, true),
                    Field::new("v", DataType::Float32, true),
                ]
                .into(),
            ),
            true,
        )));
        let variant_field = Field::new(
            "labels",
            DataType::Struct(
                vec![
                    Field::new("metadata", DataType::LargeBinary, false),
                    Field::new("value", DataType::LargeBinary, false),
                ]
                .into(),
            ),
            true,
        )
        .with_metadata(std::collections::HashMap::from([(
            "ARROW:extension:name".to_string(),
            "arrow.parquet.variant".to_string(),
        )]));
        for target in [
            Field::new("metric_part", DataType::Int32, true),
            Field::new("metric_part", DataType::Utf8, true),
            Field::new("metric_part", DataType::Float64, true),
            Field::new("_partition_date", DataType::Date32, true),
            Field::new("_partition_date", DataType::Int64, true),
            Field::new("workspace_id", DataType::Int64, true),
            Field::new("workspace_id", DataType::Binary, true),
            Field::new(
                "_event_time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("_event_time", DataType::Utf8, true),
            Field::new("values", retyped_values.clone(), true),
            Field::new("labels", DataType::Utf8, true),
            variant_field.clone(),
        ] {
            let schema: SchemaRef = Arc::new(Schema::new(vec![target.clone()]));
            let expected = super::super::arrow::build_record_batch(Arc::clone(&schema), &events);
            let got = conform_batch(&schema, &upstream);
            match (expected, got) {
                (Ok(e), Ok(g)) => assert_eq!(e, g, "{target:?}"),
                (Err(e), Err(g)) => assert_eq!(e.to_string(), g.to_string(), "{target:?}"),
                (e, g) => panic!("{target:?}: arrow_stream {e:?} vs arrow_ipc {g:?}"),
            }
        }

        // A pre-encoded VARIANT labels column into a MAP table column.
        let variant_record = super::super::arrow::build_record_batch(
            Arc::new(Schema::new(vec![variant_field])),
            &events,
        )
        .unwrap();
        let variant = Arc::clone(variant_record.column(0));
        let upstream_variant = RecordBatch::try_from_iter(vec![("labels", variant)]).unwrap();
        let map_target = Field::new(
            "labels",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        );
        let schema: SchemaRef = Arc::new(Schema::new(vec![map_target]));
        assert_eq!(
            super::super::arrow::build_record_batch(Arc::clone(&schema), &events).unwrap(),
            conform_batch(&schema, &upstream_variant).unwrap()
        );
    }
}
