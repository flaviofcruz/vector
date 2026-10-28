//! Decoding of the internal Unity Catalog `getTable` response.
//!
//! Read field-by-field rather than via proto JSON: the REST shape the OAuth path parses
//! flattens `table_info` and `column_infos` (`json_inline`), which proto JSON does not.

use databricks_zerobus_ingest_sdk::schema::{UcColumn, UcTableSchema};
use prost_reflect::{DynamicMessage, Kind, ReflectMessage, Value};

use super::error::TokenManagerError;

/// Convert a `managedcatalog.GetTable.Response` into the SDK's table schema.
pub(super) fn uc_table_schema_from_response(
    response: &DynamicMessage,
) -> Result<UcTableSchema, TokenManagerError> {
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

    Ok(UcTableSchema {
        name: string_field(&table_info, "name")?,
        catalog_name: string_field(&table_info, "catalog_name")?,
        schema_name: string_field(&table_info, "schema_name")?,
        columns,
    })
}

fn column_from_proto(column: &DynamicMessage) -> Result<UcColumn, TokenManagerError> {
    // Unset proto2 fields read back as their declared defaults (e.g. `nullable` = true).
    let nullable = match column.get_field_by_name("nullable").as_deref() {
        Some(Value::Bool(b)) => *b,
        _ => return Err(uc_error("column `nullable` is missing")),
    };
    let position = match column.get_field_by_name("position").as_deref() {
        Some(Value::I32(n)) => *n,
        _ => return Err(uc_error("column `position` is missing")),
    };
    Ok(UcColumn {
        name: string_field(column, "name")?,
        type_name: enum_name_field(column, "type_name")?,
        type_text: string_field(column, "type_text")?,
        type_json: string_field(column, "type_json")?,
        nullable,
        position,
    })
}

fn message_field(
    message: &DynamicMessage,
    name: &str,
) -> Result<DynamicMessage, TokenManagerError> {
    // An unset message field reads back as an empty default message, so check presence first.
    if !message.has_field_by_name(name) {
        return Err(uc_error(&format!("`{name}` is missing")));
    }
    match message.get_field_by_name(name).as_deref() {
        Some(Value::Message(m)) => Ok(m.clone()),
        _ => Err(uc_error(&format!("`{name}` is missing"))),
    }
}

fn string_field(message: &DynamicMessage, name: &str) -> Result<String, TokenManagerError> {
    match message.get_field_by_name(name).as_deref() {
        Some(Value::String(s)) => Ok(s.clone()),
        _ => Err(uc_error(&format!("`{name}` is not a string"))),
    }
}

/// Resolve an enum field to its value name (e.g. `STRING`), matching the REST API's encoding.
fn enum_name_field(message: &DynamicMessage, name: &str) -> Result<String, TokenManagerError> {
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

fn uc_error(message: &str) -> TokenManagerError {
    TokenManagerError::UcResponse {
        message: message.to_string(),
    }
}
