//! Unity Catalog schema fetching and protobuf descriptor generation.

use bytes::Buf;
use http::{Request, Uri};
use http_body::Body as HttpBody;
use hyper::Body;
use percent_encoding::{percent_encode, NON_ALPHANUMERIC};
use serde::Deserialize;
use zerobus_prost_types as prost_types;

use crate::config::ProxyConfig;
use crate::http::HttpClient;
use crate::tls::TlsSettings;
use super::error::ZerobusSinkError;

/// Unity Catalog table column information
#[derive(Debug, Deserialize, Clone)]
pub struct UnityCatalogColumn {
    pub name: String,
    #[allow(dead_code)] // Will be used for complex type parsing
    pub type_text: String,
    pub type_name: String,
    #[serde(default)]
    pub position: i32,
    pub nullable: bool,
    #[allow(dead_code)] // Will be used for complex type parsing
    #[serde(default)]
    pub type_json: String,
}

/// Unity Catalog table schema response
#[derive(Debug, Deserialize)]
pub struct UnityCatalogTableSchema {
    pub name: String,
    pub catalog_name: String,
    pub schema_name: String,
    pub columns: Vec<UnityCatalogColumn>,
}

/// OAuth token response from Databricks
#[derive(Debug, Deserialize)]
struct OAuthTokenResponse {
    access_token: String,
}

/// Fetch table schema from Unity Catalog API
pub async fn fetch_table_schema(
    unity_catalog_endpoint: &str,
    table_name: &str,
    client_id: &str,
    client_secret: &str,
) -> Result<UnityCatalogTableSchema, ZerobusSinkError> {
    // First, get OAuth token
    let token = get_oauth_token(unity_catalog_endpoint, client_id, client_secret).await?;

    // Fetch table schema
    let url = format!(
        "{}/api/2.0/unity-catalog/tables/{}",
        unity_catalog_endpoint.trim_end_matches('/'),
        table_name
    );

    let uri: Uri = url.parse().map_err(|e| ZerobusSinkError::ConfigError {
        message: format!("Invalid Unity Catalog endpoint URL: {}", e),
    })?;

    let http_client = HttpClient::new(TlsSettings::default(), &ProxyConfig::default())
        .map_err(|e| ZerobusSinkError::ConfigError {
            message: format!("Failed to create HTTP client: {}", e),
        })?;

    let request = Request::get(uri)
        .header("Authorization", format!("Bearer {}", token))
        .header("Content-Type", "application/json")
        .body(Body::empty())
        .map_err(|e| ZerobusSinkError::ConfigError {
            message: format!("Failed to build request: {}", e),
        })?;

    let response = http_client.send(request).await.map_err(|e| {
        ZerobusSinkError::ConfigError {
            message: format!("Failed to fetch table schema: {}", e),
        }
    })?;

    let status = response.status();
    if !status.is_success() {
        let body_bytes = response
            .into_body()
            .collect()
            .await
            .map(|c| c.to_bytes())
            .unwrap_or_default();
        let error_text = String::from_utf8_lossy(&body_bytes);
        return Err(ZerobusSinkError::ConfigError {
            message: format!(
                "Unity Catalog API returned error {}: {}",
                status, error_text
            ),
        });
    }

    let body_bytes = response
        .into_body()
        .collect()
        .await
        .map(|c| c.to_bytes())
        .map_err(|e| ZerobusSinkError::ConfigError {
            message: format!("Failed to read response body: {}", e),
        })?;

    let schema: UnityCatalogTableSchema =
        serde_json::from_reader(body_bytes.reader()).map_err(|e| {
            ZerobusSinkError::ConfigError {
                message: format!("Failed to parse table schema response: {}", e),
            }
        })?;

    Ok(schema)
}

/// Get OAuth token from Databricks
async fn get_oauth_token(
    unity_catalog_endpoint: &str,
    client_id: &str,
    client_secret: &str,
) -> Result<String, ZerobusSinkError> {
    let token_url = format!(
        "{}/oidc/v1/token",
        unity_catalog_endpoint.trim_end_matches('/')
    );

    let uri: Uri = token_url.parse().map_err(|e| ZerobusSinkError::ConfigError {
        message: format!("Invalid token endpoint URL: {}", e),
    })?;

    // Build form-encoded body
    let form_body = format!(
        "grant_type=client_credentials&client_id={}&client_secret={}&scope=all-apis",
        percent_encode(client_id.as_bytes(), NON_ALPHANUMERIC),
        percent_encode(client_secret.as_bytes(), NON_ALPHANUMERIC)
    );

    let http_client = HttpClient::new(TlsSettings::default(), &ProxyConfig::default())
        .map_err(|e| ZerobusSinkError::ConfigError {
            message: format!("Failed to create HTTP client: {}", e),
        })?;

    let request = Request::post(uri)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(Body::from(form_body))
        .map_err(|e| ZerobusSinkError::ConfigError {
            message: format!("Failed to build OAuth request: {}", e),
        })?;

    let response = http_client.send(request).await.map_err(|e| {
        ZerobusSinkError::ConfigError {
            message: format!("Failed to get OAuth token: {}", e),
        }
    })?;

    let status = response.status();
    if !status.is_success() {
        let body_bytes = response
            .into_body()
            .collect()
            .await
            .map(|c| c.to_bytes())
            .unwrap_or_default();
        let error_text = String::from_utf8_lossy(&body_bytes);
        return Err(ZerobusSinkError::ConfigError {
            message: format!("OAuth token request failed {}: {}", status, error_text),
        });
    }

    let body_bytes = response
        .into_body()
        .collect()
        .await
        .map(|c| c.to_bytes())
        .map_err(|e| ZerobusSinkError::ConfigError {
            message: format!("Failed to read OAuth response body: {}", e),
        })?;

    let token_response: OAuthTokenResponse =
        serde_json::from_reader(body_bytes.reader()).map_err(|e| {
            ZerobusSinkError::ConfigError {
                message: format!("Failed to parse OAuth token response: {}", e),
            }
        })?;

    Ok(token_response.access_token)
}

/// Generate protobuf descriptor from Unity Catalog table schema
pub fn generate_descriptor_from_schema(
    schema: &UnityCatalogTableSchema,
) -> Result<prost_reflect::MessageDescriptor, ZerobusSinkError> {
    let mut proto_fields = Vec::new();

    // Sort columns by position to maintain stable field numbers
    let mut columns = schema.columns.clone();
    columns.sort_by_key(|c| c.position);

    for column in columns {
        // Skip columns with invalid positions
        if column.position < 1 {
            continue;
        }

        let field_type = map_databricks_type_to_protobuf(&column)?;

        proto_fields.push(prost_types::FieldDescriptorProto {
            name: Some(column.name.clone()),
            number: Some(column.position),
            label: Some(if column.nullable {
                prost_types::field_descriptor_proto::Label::Optional as i32
            } else {
                prost_types::field_descriptor_proto::Label::Required as i32
            }),
            r#type: Some(field_type as i32),
            type_name: None,
            extendee: None,
            default_value: None,
            oneof_index: None,
            json_name: Some(column.name.clone()),
            options: None,
            proto3_optional: Some(column.nullable),
        });
    }

    // Create the message descriptor
    let message_name = format!("{}_{}", schema.schema_name, schema.name);
    let message_proto = prost_types::DescriptorProto {
        name: Some(message_name.clone()),
        field: proto_fields,
        extension: vec![],
        nested_type: vec![],
        enum_type: vec![],
        extension_range: vec![],
        oneof_decl: vec![],
        options: None,
        reserved_range: vec![],
        reserved_name: vec![],
    };

    let file_proto = prost_types::FileDescriptorProto {
        name: Some(format!("{}.proto", message_name)),
        package: Some(schema.catalog_name.clone()),
        message_type: vec![message_proto.clone()],
        ..Default::default()
    };

    let file_descriptor_set = prost_types::FileDescriptorSet {
        file: vec![file_proto],
    };

    // Build a FileDescriptor
    let pool = prost_reflect::DescriptorPool::from_file_descriptor_set(file_descriptor_set)
        .map_err(|e| ZerobusSinkError::ConfigError {
            message: format!("Failed to build descriptor pool: {}", e),
        })?;

    let full_message_name = format!("{}.{}", schema.catalog_name, message_name);
    let message_descriptor = pool
        .get_message_by_name(&full_message_name)
        .ok_or_else(|| ZerobusSinkError::ConfigError {
            message: format!("Failed to get message descriptor for {}", full_message_name),
        })?;

    Ok(message_descriptor)
}

/// Map Databricks type to protobuf type
/// Starting with simple types, will expand to complex types later
fn map_databricks_type_to_protobuf(
    column: &UnityCatalogColumn,
) -> Result<prost_types::field_descriptor_proto::Type, ZerobusSinkError> {
    match column.type_name.as_str() {
        "STRING" => Ok(prost_types::field_descriptor_proto::Type::String),
        "INT" => Ok(prost_types::field_descriptor_proto::Type::Int32),
        "BIGINT" => Ok(prost_types::field_descriptor_proto::Type::Int64),
        "BOOLEAN" | "BOOL" => Ok(prost_types::field_descriptor_proto::Type::Bool),
        "DOUBLE" | "FLOAT" => Ok(prost_types::field_descriptor_proto::Type::Double),
        "TIMESTAMP" => Ok(prost_types::field_descriptor_proto::Type::String),
        "BINARY" => Ok(prost_types::field_descriptor_proto::Type::Bytes),

        // Complex types - for now, serialize as string
        // TODO: Implement proper struct/array handling
        "STRUCT" => {
            eprintln!(
                "Warning: Column '{}' has complex STRUCT type, treating as string. \
                 Use explicit schema file for full complex type support.",
                column.name
            );
            Ok(prost_types::field_descriptor_proto::Type::String)
        }
        "ARRAY" => {
            eprintln!(
                "Warning: Column '{}' has ARRAY type, treating as string. \
                 Use explicit schema file for full array support.",
                column.name
            );
            Ok(prost_types::field_descriptor_proto::Type::String)
        }

        unknown => Err(ZerobusSinkError::ConfigError {
            message: format!(
                "Unsupported Databricks type '{}' for column '{}'. \
                 Consider using an explicit schema file for complex types.",
                unknown, column.name
            ),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_simple_types() {
        let test_cases = vec![
            ("STRING", prost_types::field_descriptor_proto::Type::String),
            ("INT", prost_types::field_descriptor_proto::Type::Int32),
            ("BIGINT", prost_types::field_descriptor_proto::Type::Int64),
            ("BOOLEAN", prost_types::field_descriptor_proto::Type::Bool),
            ("DOUBLE", prost_types::field_descriptor_proto::Type::Double),
            ("TIMESTAMP", prost_types::field_descriptor_proto::Type::String),
            ("BINARY", prost_types::field_descriptor_proto::Type::Bytes),
        ];

        for (databricks_type, expected_proto_type) in test_cases {
            let column = UnityCatalogColumn {
                name: "test_column".to_string(),
                type_text: databricks_type.to_lowercase(),
                type_name: databricks_type.to_string(),
                position: 1,
                nullable: true,
                type_json: "{}".to_string(),
            };

            let result = map_databricks_type_to_protobuf(&column);
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), expected_proto_type);
        }
    }

    #[test]
    fn test_generate_descriptor_simple_schema() {
        let schema = UnityCatalogTableSchema {
            name: "test_table".to_string(),
            catalog_name: "test_catalog".to_string(),
            schema_name: "test_schema".to_string(),
            columns: vec![
                UnityCatalogColumn {
                    name: "id".to_string(),
                    type_text: "bigint".to_string(),
                    type_name: "BIGINT".to_string(),
                    position: 1,
                    nullable: false,
                    type_json: "{}".to_string(),
                },
                UnityCatalogColumn {
                    name: "message".to_string(),
                    type_text: "string".to_string(),
                    type_name: "STRING".to_string(),
                    position: 2,
                    nullable: true,
                    type_json: "{}".to_string(),
                },
            ],
        };

        let result = generate_descriptor_from_schema(&schema);
        assert!(result.is_ok());

        let descriptor = result.unwrap();
        assert_eq!(descriptor.fields().len(), 2);

        let id_field = descriptor.get_field_by_name("id");
        assert!(id_field.is_some());

        let message_field = descriptor.get_field_by_name("message");
        assert!(message_field.is_some());
    }
}
