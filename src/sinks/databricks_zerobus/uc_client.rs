//! Internal Unity Catalog (managed-catalog) gRPC client for `login_service` schema reads.
//!
//! Reads a table's schema over Unity Catalog's east-west gRPC API, authenticating with the
//! unsigned principal context minted by the sink's Login
//! [`TokenManager`].
//! The Unity Catalog proto schema is loaded from a runtime `FileDescriptorSet`. The `getTable`
//! response is read field by field rather than via proto JSON: the REST shape the OAuth read
//! parses flattens `table_info` and `column_infos` (`json_inline`), which proto JSON does not.

use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::STANDARD_NO_PAD;
use databricks_zerobus_ingest_sdk::schema::{UcColumn, UcTableSchema};
use http::Uri;
use prost_reflect::{DynamicMessage, Kind, MethodDescriptor, ReflectMessage, Value};

use super::error::ZerobusSinkError;
use crate::databricks_auth::{
    GrpcHttpClient, LoginServiceAuthConfig, TokenManager, TokenManagerError,
    build_hyper_openssl_client, grpc_method_uri, grpc_unary_call, load_descriptor_pool,
};

const UC_SERVICE_NAME: &str = "managedcatalog.UnityCatalogService";
const UC_GET_TABLE_METHOD_NAME: &str = "getTable";

/// Unsigned principal-context header. UC's east-west ingress authorizes internal callers from
/// this header (it rejects requests without one).
const PRINCIPAL_CONTEXT_HEADER: &str = "s2s-principal-context-bin";

/// gRPC client for the internal Unity Catalog `getTable` RPC, authenticated with the principal
/// context from the sink's shared Login [`TokenManager`].
pub(super) struct UnityCatalogClient {
    token_manager: Arc<TokenManager>,
    method: MethodDescriptor,
    client: GrpcHttpClient,
    uri: Uri,
}

impl UnityCatalogClient {
    pub(super) async fn new(
        auth: &LoginServiceAuthConfig,
        token_manager: Arc<TokenManager>,
    ) -> Result<Self, ZerobusSinkError> {
        let setup = |message: String| ZerobusSinkError::ConfigError {
            message: format!("login_service Unity Catalog client: {message}"),
        };
        let uc = auth.unity_catalog.as_ref().ok_or_else(|| {
            setup("auth.unity_catalog is required to fetch the table schema".to_string())
        })?;
        let path = &uc.proto_descriptor_path;
        let pool = load_descriptor_pool(path)
            .await
            .map_err(|e| setup(e.to_string()))?;
        let method = pool
            .get_service_by_name(UC_SERVICE_NAME)
            .and_then(|svc| svc.methods().find(|m| m.name() == UC_GET_TABLE_METHOD_NAME))
            .ok_or_else(|| {
                setup(format!(
                    "{UC_SERVICE_NAME}/{UC_GET_TABLE_METHOD_NAME} not found in {}",
                    path.display()
                ))
            })?;
        let endpoint = &uc.endpoint;
        let uri = endpoint
            .parse()
            .map_err(|e| setup(format!("invalid unity_catalog_endpoint `{endpoint}`: {e}")))
            .and_then(|endpoint| {
                grpc_method_uri(&endpoint, &method).map_err(|e| setup(e.to_string()))
            })?;
        let client = build_hyper_openssl_client(auth, &uc.server_name, uc.verify_hostname)
            .map_err(|e| setup(e.to_string()))?;
        Ok(Self {
            token_manager,
            method,
            client,
            uri,
        })
    }

    /// Fetch a table's schema with the Login-minted principal contexts.
    pub(super) async fn get_table_schema(
        &self,
        full_name: &str,
    ) -> Result<UcTableSchema, ZerobusSinkError> {
        let principal_context = self
            .token_manager
            .get_unsigned_principal_context()
            .await
            .map_err(grpc_error)?;
        let mut request = DynamicMessage::new(self.method.input());
        request
            .try_set_field_by_name("full_name_arg", Value::String(full_name.to_string()))
            .map_err(|e| uc_error(&format!("set full_name_arg on getTable request: {e}")))?;
        let headers = principal_context_headers(&principal_context);
        let response = grpc_unary_call(
            &self.client,
            self.uri.clone(),
            &headers,
            &request,
            &self.method.output(),
        )
        .await
        .map_err(grpc_error)?;
        uc_table_schema_from_response(&response)
    }
}

/// The internal UC endpoint authorizes schema reads from the unsigned context.
fn principal_context_headers(principal_context: &[u8]) -> Vec<(&'static str, String)> {
    vec![(
        PRINCIPAL_CONTEXT_HEADER,
        STANDARD_NO_PAD.encode(principal_context),
    )]
}

/// Transport, status, and token failures; retryable when transient.
fn grpc_error(error: TokenManagerError) -> ZerobusSinkError {
    ZerobusSinkError::SchemaError {
        retryable: error.is_retryable(),
        status: None,
        message: format!("Unity Catalog getTable: {error}"),
    }
}

/// Malformed requests and responses; never retryable.
fn uc_error(message: &str) -> ZerobusSinkError {
    ZerobusSinkError::SchemaError {
        retryable: false,
        status: None,
        message: format!("Unity Catalog getTable: {message}"),
    }
}

/// Convert a `managedcatalog.GetTable.Response` into the SDK's table schema.
fn uc_table_schema_from_response(
    response: &DynamicMessage,
) -> Result<UcTableSchema, ZerobusSinkError> {
    let table_info = message_field(response, "table_info")?;
    let column_infos = message_field(&table_info, "column_infos")?;
    let columns = match column_infos.get_field_by_name("columns").as_deref() {
        Some(Value::List(columns)) => columns
            .iter()
            .map(|column| match column {
                Value::Message(column) => column_from_proto(column),
                _ => Err(uc_error("`columns` entry is not a message")),
            })
            .collect::<Result<Vec<_>, _>>()?,
        _ => return Err(uc_error("`column_infos.columns` is missing")),
    };

    Ok(UcTableSchema::new(
        string_field(&table_info, "catalog_name")?,
        string_field(&table_info, "schema_name")?,
        string_field(&table_info, "name")?,
        columns,
    ))
}

fn column_from_proto(column: &DynamicMessage) -> Result<UcColumn, ZerobusSinkError> {
    // Unset proto2 fields read back as their declared defaults (e.g. `nullable` = true).
    let nullable = match column.get_field_by_name("nullable").as_deref() {
        Some(Value::Bool(b)) => *b,
        _ => return Err(uc_error("column `nullable` is missing")),
    };
    let position = match column.get_field_by_name("position").as_deref() {
        Some(Value::I32(n)) => *n,
        _ => return Err(uc_error("column `position` is missing")),
    };
    Ok(UcColumn::new(
        string_field(column, "name")?,
        enum_name_field(column, "type_name")?,
    )
    .with_type_text(string_field(column, "type_text")?)
    .with_type_json(string_field(column, "type_json")?)
    .with_nullable(nullable)
    .with_position(position))
}

fn message_field(message: &DynamicMessage, name: &str) -> Result<DynamicMessage, ZerobusSinkError> {
    // An unset message field reads back as an empty default message, so check presence first.
    if !message.has_field_by_name(name) {
        return Err(uc_error(&format!("`{name}` is missing")));
    }
    match message.get_field_by_name(name).as_deref() {
        Some(Value::Message(m)) => Ok(m.clone()),
        _ => Err(uc_error(&format!("`{name}` is missing"))),
    }
}

fn string_field(message: &DynamicMessage, name: &str) -> Result<String, ZerobusSinkError> {
    match message.get_field_by_name(name).as_deref() {
        Some(Value::String(s)) => Ok(s.clone()),
        _ => Err(uc_error(&format!("`{name}` is not a string"))),
    }
}

/// Resolve an enum field to its value name (e.g. `STRING`), matching the REST API's encoding.
fn enum_name_field(message: &DynamicMessage, name: &str) -> Result<String, ZerobusSinkError> {
    let field = message
        .descriptor()
        .get_field_by_name(name)
        .ok_or_else(|| uc_error(&format!("`{name}` is not in the descriptor")))?;
    let (Kind::Enum(enum_desc), Value::EnumNumber(number)) =
        (field.kind(), message.get_field(&field).into_owned())
    else {
        return Err(uc_error(&format!("`{name}` is not an enum")));
    };
    enum_desc
        .get_value(number)
        .map(|v| v.name().to_string())
        .ok_or_else(|| uc_error(&format!("`{name}` has unknown enum value {number}")))
}

#[cfg(test)]
mod tests {
    use prost_reflect::prost::Message;
    use prost_reflect::prost_types::{
        DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
        FileDescriptorProto, FileDescriptorSet, field_descriptor_proto,
    };

    use prost_reflect::{DescriptorPool, MessageDescriptor};

    use super::*;

    // ---- Unity Catalog getTable response decoding ----

    fn uc_field(name: &str, number: i32, ty: field_descriptor_proto::Type) -> FieldDescriptorProto {
        FieldDescriptorProto {
            name: Some(name.into()),
            number: Some(number),
            label: Some(field_descriptor_proto::Label::Optional as i32),
            r#type: Some(ty as i32),
            ..Default::default()
        }
    }

    fn uc_message_field(name: &str, number: i32, type_name: &str) -> FieldDescriptorProto {
        FieldDescriptorProto {
            type_name: Some(type_name.into()),
            ..uc_field(name, number, field_descriptor_proto::Type::Message)
        }
    }

    /// Subset of `managed-catalog/api/messages/{table,column}.proto` (proto2) with the real
    /// field numbers, including `nullable`'s `[default = true]`.
    fn uc_get_table_response_descriptor() -> MessageDescriptor {
        use field_descriptor_proto::Type;
        let column_info = DescriptorProto {
            name: Some("ColumnInfo".into()),
            field: vec![
                uc_field("name", 1, Type::String),
                uc_field("type_text", 2, Type::String),
                uc_field("type_json", 3, Type::String),
                FieldDescriptorProto {
                    type_name: Some(".managedcatalog.ColumnTypeName".into()),
                    ..uc_field("type_name", 4, Type::Enum)
                },
                uc_field("position", 8, Type::Int32),
                FieldDescriptorProto {
                    default_value: Some("true".into()),
                    ..uc_field("nullable", 10, Type::Bool)
                },
            ],
            ..Default::default()
        };
        let column_infos = DescriptorProto {
            name: Some("ColumnInfos".into()),
            field: vec![FieldDescriptorProto {
                label: Some(field_descriptor_proto::Label::Repeated as i32),
                ..uc_message_field("columns", 1, ".managedcatalog.ColumnInfo")
            }],
            ..Default::default()
        };
        let table_info = DescriptorProto {
            name: Some("TableInfo".into()),
            field: vec![
                uc_field("name", 1, Type::String),
                uc_field("catalog_name", 2, Type::String),
                uc_field("schema_name", 3, Type::String),
                uc_message_field("column_infos", 7, ".managedcatalog.ColumnInfos"),
            ],
            ..Default::default()
        };
        let get_table = DescriptorProto {
            name: Some("GetTable".into()),
            nested_type: vec![DescriptorProto {
                name: Some("Response".into()),
                field: vec![uc_message_field(
                    "table_info",
                    1,
                    ".managedcatalog.TableInfo",
                )],
                ..Default::default()
            }],
            ..Default::default()
        };
        let column_type_name = EnumDescriptorProto {
            name: Some("ColumnTypeName".into()),
            value: vec![
                EnumValueDescriptorProto {
                    name: Some("UNKNOWN_COLUMN_TYPE_NAME".into()),
                    number: Some(0),
                    ..Default::default()
                },
                EnumValueDescriptorProto {
                    name: Some("LONG".into()),
                    number: Some(5),
                    ..Default::default()
                },
                EnumValueDescriptorProto {
                    name: Some("STRING".into()),
                    number: Some(10),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let file = FileDescriptorProto {
            name: Some("uc_test.proto".into()),
            package: Some("managedcatalog".into()),
            syntax: Some("proto2".into()),
            message_type: vec![column_info, column_infos, table_info, get_table],
            enum_type: vec![column_type_name],
            ..Default::default()
        };
        DescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: vec![file] })
            .expect("uc test descriptor pool")
            .get_message_by_name("managedcatalog.GetTable.Response")
            .expect("GetTable.Response")
    }

    fn uc_column(
        desc: &MessageDescriptor,
        name: &str,
        type_name: i32,
        nullable: Option<bool>,
    ) -> Value {
        let mut column = DynamicMessage::new(desc.clone());
        column.set_field_by_name("name", Value::String(name.into()));
        column.set_field_by_name("type_text", Value::String(name.to_lowercase()));
        column.set_field_by_name("type_name", Value::EnumNumber(type_name));
        if let Some(nullable) = nullable {
            column.set_field_by_name("nullable", Value::Bool(nullable));
        }
        Value::Message(column)
    }

    #[test]
    fn uc_table_schema_from_response_maps_columns() {
        let response_desc = uc_get_table_response_descriptor();
        let pool = response_desc.parent_pool();
        let column_desc = pool
            .get_message_by_name("managedcatalog.ColumnInfo")
            .unwrap();

        let mut column_infos = DynamicMessage::new(
            pool.get_message_by_name("managedcatalog.ColumnInfos")
                .unwrap(),
        );
        column_infos.set_field_by_name(
            "columns",
            Value::List(vec![
                uc_column(&column_desc, "ID", 5, Some(false)),
                uc_column(&column_desc, "MSG", 10, None),
            ]),
        );
        let mut table_info = DynamicMessage::new(
            pool.get_message_by_name("managedcatalog.TableInfo")
                .unwrap(),
        );
        table_info.set_field_by_name("name", Value::String("t".into()));
        table_info.set_field_by_name("catalog_name", Value::String("c".into()));
        table_info.set_field_by_name("schema_name", Value::String("s".into()));
        table_info.set_field_by_name("column_infos", Value::Message(column_infos));
        let mut response = DynamicMessage::new(response_desc.clone());
        response.set_field_by_name("table_info", Value::Message(table_info));

        let decoded =
            DynamicMessage::decode(response_desc, response.encode_to_vec().as_slice()).unwrap();
        let schema = uc_table_schema_from_response(&decoded).unwrap();

        assert_eq!(
            (
                schema.name.as_str(),
                schema.catalog_name.as_str(),
                schema.schema_name.as_str()
            ),
            ("t", "c", "s")
        );
        let columns: Vec<_> = schema
            .columns
            .iter()
            .map(|c| (c.name.as_str(), c.type_name.as_str(), c.nullable))
            .collect();
        // An unset proto2 `nullable` reads back as its declared default (true).
        assert_eq!(
            columns,
            vec![("ID", "LONG", false), ("MSG", "STRING", true)]
        );
    }

    #[test]
    fn uc_table_schema_from_response_rejects_missing_table_info() {
        let response = DynamicMessage::new(uc_get_table_response_descriptor());
        assert!(matches!(
            uc_table_schema_from_response(&response),
            Err(ZerobusSinkError::SchemaError {
                retryable: false,
                ..
            })
        ));
    }

    // ---- Principal-context headers ----

    #[test]
    fn principal_context_headers_sends_unsigned_without_authorization() {
        let headers = principal_context_headers(b"unsigned");
        assert_eq!(
            headers,
            vec![(
                PRINCIPAL_CONTEXT_HEADER,
                STANDARD_NO_PAD.encode(b"unsigned")
            )]
        );
    }
}
