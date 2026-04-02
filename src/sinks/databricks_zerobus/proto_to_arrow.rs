//! Conversion from protobuf `MessageDescriptor` to Arrow `Schema`.
//!
//! Maps protobuf field types to their Arrow equivalents so that the
//! `ArrowStreamSerializer` can encode Vector events using the table's
//! protobuf schema definition.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema};
use prost_reflect::{Cardinality, FieldDescriptor, Kind, MessageDescriptor};

use super::error::ZerobusSinkError;

/// Convert a protobuf `MessageDescriptor` into an Arrow `Schema`.
pub fn proto_descriptor_to_arrow_schema(
    descriptor: &MessageDescriptor,
) -> Result<Schema, ZerobusSinkError> {
    let fields: Result<Vec<Field>, _> = descriptor
        .fields()
        .map(|field| proto_field_to_arrow_field(&field))
        .collect();

    Ok(Schema::new(fields?))
}

/// Convert a single protobuf field descriptor into an Arrow `Field`.
fn proto_field_to_arrow_field(field: &FieldDescriptor) -> Result<Field, ZerobusSinkError> {
    let name = field.name().to_string();
    let nullable = field.cardinality() != Cardinality::Required;

    if field.is_map() {
        // Protobuf maps are represented as repeated messages with `key` (field 1)
        // and `value` (field 2). Convert to Arrow Map<K, V>.
        let map_msg = match field.kind() {
            Kind::Message(m) => m,
            _ => unreachable!("map field must have a message kind"),
        };
        let key_field = map_msg
            .get_field_by_name("key")
            .expect("map entry message must have a 'key' field");
        let val_field = map_msg
            .get_field_by_name("value")
            .expect("map entry message must have a 'value' field");

        let key_type = proto_kind_to_arrow_type(&key_field.kind(), "key")?;
        let val_type = proto_kind_to_arrow_type(&val_field.kind(), "value")?;

        // DEBUG: VECTOR_ARROW_MAP_DEBUG=skip  → replace all maps with Utf8 (tests server acceptance)
        //        VECTOR_ARROW_MAP_DEBUG=int64  → only emit Map when key is Int64
        //        VECTOR_ARROW_MAP_DEBUG=int32  → only emit Map when key is Int32
        //        VECTOR_ARROW_MAP_DEBUG=string → only emit Map when key is Utf8
        //        unset / other                 → normal behaviour (emit Arrow Map for all)
        let debug_mode = std::env::var("VECTOR_ARROW_MAP_DEBUG")
            .unwrap_or_default()
            .to_lowercase();
        let use_arrow_map = match debug_mode.as_str() {
            "skip" => false,
            "int64" => matches!(key_type, DataType::Int64),
            "int32" => matches!(key_type, DataType::Int32),
            "string" => matches!(key_type, DataType::LargeUtf8),
            _ => true,
        };

        if !use_arrow_map {
            // Fall back to LargeUtf8 so the field is still present but as a plain
            // string column — lets us confirm the server accepts the schema without Map.
            tracing::debug!(
                field = %name,
                key_type = ?key_type,
                val_type = ?val_type,
                mode = %debug_mode,
                "VECTOR_ARROW_MAP_DEBUG: replacing map column with LargeUtf8"
            );
            return Ok(Field::new(name, DataType::LargeUtf8, true));
        }

        tracing::debug!(
            field = %name,
            key_type = ?key_type,
            val_type = ?val_type,
            "emitting Arrow Map column"
        );

        let entries = DataType::Struct(Fields::from(vec![
            Field::new("key", key_type, false),
            Field::new("value", val_type, true),
        ]));
        // Use "key_value" as the entries field name — this matches Spark/Delta
        // Arrow IPC convention and is required by the Databricks Arrow Flight server.
        return Ok(Field::new(
            name,
            DataType::Map(Arc::new(Field::new("key_value", entries, false)), false),
            nullable,
        ));
    }

    let data_type = proto_kind_to_arrow_type(&field.kind(), &name)?;

    if field.is_list() {
        Ok(Field::new(
            name,
            DataType::List(Arc::new(Field::new("item", data_type, true))),
            nullable,
        ))
    } else {
        Ok(Field::new(name, data_type, nullable))
    }
}

/// Map a protobuf `Kind` to an Arrow `DataType`.
fn proto_kind_to_arrow_type(kind: &Kind, field_name: &str) -> Result<DataType, ZerobusSinkError> {
    match kind {
        Kind::Double => Ok(DataType::Float64),
        Kind::Float => Ok(DataType::Float32),
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => Ok(DataType::Int32),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 if field_name == "_event_time" => Ok(
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
        ),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => Ok(DataType::Int64),
        Kind::Uint32 | Kind::Fixed32 => Ok(DataType::UInt32),
        Kind::Uint64 | Kind::Fixed64 => Ok(DataType::UInt64),
        Kind::Bool => Ok(DataType::Boolean),
        Kind::String => Ok(DataType::LargeUtf8),
        Kind::Bytes => Ok(DataType::LargeBinary),
        Kind::Enum(_) => Ok(DataType::Int32),
        Kind::Message(msg_descriptor) => {
            let fields: Result<Vec<Field>, _> = msg_descriptor
                .fields()
                .map(|f| proto_field_to_arrow_field(&f))
                .collect();
            Ok(DataType::Struct(Fields::from(fields?)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use vrl::protobuf::descriptor::get_message_descriptor;

    /// Load the test User descriptor from the test .desc file.
    /// Message: test_proto.User { string id, string name, int32 age, repeated string emails }
    fn load_test_user_descriptor() -> MessageDescriptor {
        let path = Path::new("tests/data/protobuf/test_proto.desc");
        get_message_descriptor(path, "test_proto.User")
            .expect("Failed to load test_proto.User descriptor")
    }

    #[test]
    fn test_user_schema_fields() {
        let descriptor = load_test_user_descriptor();
        let schema = proto_descriptor_to_arrow_schema(&descriptor).unwrap();

        assert_eq!(schema.fields().len(), 4);

        let id_field = schema.field_with_name("id").unwrap();
        assert_eq!(id_field.data_type(), &DataType::LargeUtf8);

        let name_field = schema.field_with_name("name").unwrap();
        assert_eq!(name_field.data_type(), &DataType::LargeUtf8);

        let age_field = schema.field_with_name("age").unwrap();
        assert_eq!(age_field.data_type(), &DataType::Int32);
    }

    #[test]
    fn test_repeated_field_becomes_list() {
        let descriptor = load_test_user_descriptor();
        let schema = proto_descriptor_to_arrow_schema(&descriptor).unwrap();

        let emails_field = schema.field_with_name("emails").unwrap();
        match emails_field.data_type() {
            DataType::List(inner) => {
                assert_eq!(inner.data_type(), &DataType::LargeUtf8);
            }
            other => panic!("Expected List, got {:?}", other),
        }
    }

    #[test]
    fn test_proto3_fields_are_nullable() {
        let descriptor = load_test_user_descriptor();
        let schema = proto_descriptor_to_arrow_schema(&descriptor).unwrap();

        for field in schema.fields() {
            assert!(
                field.is_nullable(),
                "Field '{}' should be nullable in proto3",
                field.name()
            );
        }
    }

    #[test]
    fn test_map_field_produces_key_value_naming() {
        // A proto map<int64, bool> field must become Arrow Map("key_value", Struct(key: Int64,
        // value: Bool)) — the inner entries field must be named "key_value" to match
        // the Spark / Delta / Unity Catalog Arrow IPC convention.
        use super::super::unity_catalog_schema::{
            UnityCatalogColumn, UnityCatalogTableSchema, generate_descriptor_from_schema,
        };

        let schema = UnityCatalogTableSchema {
            name: "test_table".to_string(),
            catalog_name: "test_catalog".to_string(),
            schema_name: "test_schema".to_string(),
            columns: vec![UnityCatalogColumn {
                name: "boolean_config_access".to_string(),
                type_text: "map<bigint,boolean>".to_string(),
                type_name: "MAP".to_string(),
                position: 0,
                nullable: true,
                type_json: r#"{"type":"map","keyType":"long","valueType":"boolean","valueContainsNull":true}"#.to_string(),
            }],
        };

        let descriptor =
            generate_descriptor_from_schema(&schema).expect("Failed to generate descriptor");
        let arrow_schema =
            proto_descriptor_to_arrow_schema(&descriptor).expect("Failed to build Arrow schema");

        assert_eq!(arrow_schema.fields().len(), 1);
        let field = arrow_schema.field(0);
        assert_eq!(field.name(), "boolean_config_access");

        match field.data_type() {
            DataType::Map(entries_field, _sorted) => {
                assert_eq!(
                    entries_field.name(),
                    "key_value",
                    "Map entries field must be 'key_value' for UC/Spark compatibility"
                );
                match entries_field.data_type() {
                    DataType::Struct(kv_fields) => {
                        assert_eq!(kv_fields.len(), 2);
                        assert_eq!(kv_fields[0].name(), "key");
                        assert_eq!(kv_fields[0].data_type(), &DataType::Int64);
                        assert!(!kv_fields[0].is_nullable(), "map key must not be nullable");
                        assert_eq!(kv_fields[1].name(), "value");
                        assert_eq!(kv_fields[1].data_type(), &DataType::Boolean);
                    }
                    other => panic!("Expected Struct inside Map, got {:?}", other),
                }
            }
            other => panic!("Expected Map type for boolean_config_access, got {:?}", other),
        }
    }

    #[test]
    fn test_map_field_string_to_string() {
        // map<string, string> → Arrow Map("key_value", Struct(key: Utf8, value: Utf8))
        use super::super::unity_catalog_schema::{
            UnityCatalogColumn, UnityCatalogTableSchema, generate_descriptor_from_schema,
        };

        let schema = UnityCatalogTableSchema {
            name: "test_table".to_string(),
            catalog_name: "test_catalog".to_string(),
            schema_name: "test_schema".to_string(),
            columns: vec![UnityCatalogColumn {
                name: "sql_confs".to_string(),
                type_text: "map<string,string>".to_string(),
                type_name: "MAP".to_string(),
                position: 0,
                nullable: true,
                type_json: r#"{"type":"map","keyType":"string","valueType":"string","valueContainsNull":true}"#.to_string(),
            }],
        };

        let descriptor =
            generate_descriptor_from_schema(&schema).expect("Failed to generate descriptor");
        let arrow_schema =
            proto_descriptor_to_arrow_schema(&descriptor).expect("Failed to build Arrow schema");

        let field = arrow_schema.field(0);
        assert_eq!(field.name(), "sql_confs");

        match field.data_type() {
            DataType::Map(entries_field, _) => {
                assert_eq!(entries_field.name(), "key_value");
                match entries_field.data_type() {
                    DataType::Struct(kv_fields) => {
                        assert_eq!(kv_fields[0].data_type(), &DataType::LargeUtf8);
                        assert_eq!(kv_fields[1].data_type(), &DataType::LargeUtf8);
                    }
                    other => panic!("Expected Struct, got {:?}", other),
                }
            }
            other => panic!("Expected Map, got {:?}", other),
        }
    }

    #[test]
    fn test_nested_message_from_unity_catalog() {
        use super::super::unity_catalog_schema::{
            UnityCatalogColumn, UnityCatalogTableSchema, generate_descriptor_from_schema,
        };

        let schema = UnityCatalogTableSchema {
            name: "test_table".to_string(),
            catalog_name: "test_catalog".to_string(),
            schema_name: "test_schema".to_string(),
            columns: vec![
                UnityCatalogColumn {
                    name: "id".to_string(),
                    type_text: "LONG".to_string(),
                    type_name: "LONG".to_string(),
                    position: 0,
                    nullable: false,
                    type_json: String::new(),
                },
                UnityCatalogColumn {
                    name: "name".to_string(),
                    type_text: "STRING".to_string(),
                    type_name: "STRING".to_string(),
                    position: 1,
                    nullable: true,
                    type_json: String::new(),
                },
                UnityCatalogColumn {
                    name: "score".to_string(),
                    type_text: "DOUBLE".to_string(),
                    type_name: "DOUBLE".to_string(),
                    position: 2,
                    nullable: true,
                    type_json: String::new(),
                },
                UnityCatalogColumn {
                    name: "active".to_string(),
                    type_text: "BOOLEAN".to_string(),
                    type_name: "BOOLEAN".to_string(),
                    position: 3,
                    nullable: false,
                    type_json: String::new(),
                },
            ],
        };

        let descriptor =
            generate_descriptor_from_schema(&schema).expect("Failed to generate descriptor");
        let arrow_schema = proto_descriptor_to_arrow_schema(&descriptor).unwrap();

        assert_eq!(arrow_schema.fields().len(), 4);

        let id_field = arrow_schema.field_with_name("id").unwrap();
        assert_eq!(id_field.data_type(), &DataType::Int64);

        let name_field = arrow_schema.field_with_name("name").unwrap();
        assert_eq!(name_field.data_type(), &DataType::LargeUtf8);

        let score_field = arrow_schema.field_with_name("score").unwrap();
        assert_eq!(score_field.data_type(), &DataType::Float64);

        let active_field = arrow_schema.field_with_name("active").unwrap();
        assert_eq!(active_field.data_type(), &DataType::Boolean);
    }

    #[test]
    fn test_string_maps_to_large_utf8_and_bytes_maps_to_large_binary() {
        // Verifies that proto `string` → Arrow `LargeUtf8` and proto `bytes` → Arrow `LargeBinary`.
        // This is required for compatibility with the Databricks Arrow Flight server, which
        // expects LargeUtf8/LargeBinary rather than the narrow Utf8/Binary variants.
        use super::super::unity_catalog_schema::{
            UnityCatalogColumn, UnityCatalogTableSchema, generate_descriptor_from_schema,
        };

        let schema = UnityCatalogTableSchema {
            name: "test_table".to_string(),
            catalog_name: "cat".to_string(),
            schema_name: "sch".to_string(),
            columns: vec![
                UnityCatalogColumn {
                    name: "name".to_string(),
                    type_text: "STRING".to_string(),
                    type_name: "STRING".to_string(),
                    position: 0,
                    nullable: true,
                    type_json: String::new(),
                },
                UnityCatalogColumn {
                    name: "data".to_string(),
                    type_text: "BINARY".to_string(),
                    type_name: "BINARY".to_string(),
                    position: 1,
                    nullable: true,
                    type_json: String::new(),
                },
            ],
        };

        let descriptor =
            generate_descriptor_from_schema(&schema).expect("Failed to generate descriptor");
        let arrow_schema =
            proto_descriptor_to_arrow_schema(&descriptor).expect("Failed to build Arrow schema");

        let name_field = arrow_schema.field_with_name("name").unwrap();
        assert_eq!(
            name_field.data_type(),
            &DataType::LargeUtf8,
            "proto string must map to LargeUtf8 for Databricks Flight compatibility"
        );

        let data_field = arrow_schema.field_with_name("data").unwrap();
        assert_eq!(
            data_field.data_type(),
            &DataType::LargeBinary,
            "proto bytes must map to LargeBinary for Databricks Flight compatibility"
        );
    }
}
