//! Zerobus service wrapper for Vector sink integration.

use databricks_zerobus_ingest_sdk::{TableProperties, ZerobusSdk, ZerobusStream};
use futures::future::BoxFuture;
use std::sync::Arc;
use tokio::sync::Mutex;
use tower::Service;
use vector_lib::event::Event;
use vector_lib::finalization::{EventFinalizers, Finalizable};
use vector_lib::request_metadata::{GroupedCountByteSize, MetaDescriptive, RequestMetadata};
use vector_lib::stream::DriverResponse;
// Use prost 0.13 to match the SDK (aliased as zerobus-prost* in Cargo.toml)
use prost_reflect::prost::Message as ProstMessage;
use std::path::Path;
use vrl::protobuf::descriptor::get_message_descriptor;
use vrl::protobuf::encode::encode_message;
use zerobus_prost_types as prost_types;

use super::{config::ZerobusSinkConfig, error::ZerobusSinkError};

/// Request type for the Zerobus service.
#[derive(Debug)]
pub struct ZerobusRequest {
    pub events: Vec<Event>,
    pub metadata: RequestMetadata,
    pub finalizers: EventFinalizers,
}

/// Response type for the Zerobus service.
#[derive(Debug)]
#[allow(dead_code)]
pub struct ZerobusResponse {
    pub count: usize,
}

impl DriverResponse for ZerobusResponse {
    fn event_status(&self) -> vector_lib::event::EventStatus {
        vector_lib::event::EventStatus::Delivered
    }

    fn events_sent(&self) -> &GroupedCountByteSize {
        // For now, return a simple grouped count
        use std::sync::LazyLock;
        static ZERO_SIZE: LazyLock<GroupedCountByteSize> =
            LazyLock::new(|| GroupedCountByteSize::new_untagged());
        &ZERO_SIZE
    }

    fn bytes_sent(&self) -> Option<usize> {
        None
    }
}

impl Finalizable for ZerobusRequest {
    fn take_finalizers(&mut self) -> EventFinalizers {
        std::mem::take(&mut self.finalizers)
    }
}

impl MetaDescriptive for ZerobusRequest {
    fn get_metadata(&self) -> &RequestMetadata {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut RequestMetadata {
        &mut self.metadata
    }
}

/// Service for handling Zerobus requests.
pub struct ZerobusService {
    sdk: ZerobusSdk,
    pub config: ZerobusSinkConfig,
    stream: Arc<Mutex<Option<ZerobusStream>>>,
    descriptor: Arc<Mutex<Option<prost_reflect::MessageDescriptor>>>,
}

impl ZerobusService {
    pub fn new(config: ZerobusSinkConfig) -> Result<Self, ZerobusSinkError> {
        // Validate configuration
        config.validate()?;

        // Create SDK instance
        let sdk = ZerobusSdk::new(
            config.ingestion_endpoint.clone(),
            config.unity_catalog_endpoint.clone(),
        )
        .map_err(|e| ZerobusSinkError::ConfigError {
            message: format!("Failed to create Zerobus SDK: {}", e),
        })?;

        let descriptor_opt = if let Some(ref schema) = config.schema {
            let message_descriptor = Self::build_descriptor_from_config(schema).map_err(|e| {
                ZerobusSinkError::ConfigError {
                    message: format!("Failed to load descriptor: {}", e),
                }
            })?;
            Some(message_descriptor.clone())
        } else {
            None
        };

        Ok(Self {
            sdk,
            config,
            stream: Arc::new(Mutex::new(None)),
            descriptor: Arc::new(Mutex::new(descriptor_opt)),
        })
    }

    /// Build protobuf descriptor from explicit schema configuration.
    ///
    /// This is the preferred approach when the table schema is known, as it ensures
    /// exact type compatibility with the Unity Catalog table.
    fn build_descriptor_from_config(
        schema_source: &super::config::SchemaSource,
    ) -> Result<prost_reflect::MessageDescriptor, ZerobusSinkError> {
        // Get the descriptor bytes from the source
        match schema_source {
            super::config::SchemaSource::Path { path, message_type } => {
                let path = Path::new(&path);
                let message_descriptor =
                    get_message_descriptor(&path, &message_type).map_err(|e| {
                        ZerobusSinkError::ConfigError {
                            message: format!("Failed to get message descriptor: {}", e),
                        }
                    })?;
                Ok(message_descriptor)
            }
        }
    }

    /// Infer protobuf schema from a Vector event.
    ///
    /// This is a fallback approach when no explicit schema is provided.
    /// Note: Type inference may not always match the Unity Catalog table schema exactly.
    /// Schemas should be provided explicitly but this makes testing a bit easier
    /// especially for non-OTEL events
    fn infer_schema_from_event(
        event: &Event,
    ) -> Result<prost_reflect::MessageDescriptor, ZerobusSinkError> {
        match event {
            Event::Log(log_event) => {
                let fields = log_event.all_event_fields().ok_or_else(|| {
                    ZerobusSinkError::EncodingError {
                        message: "Failed to get event fields".to_string(),
                    }
                })?;

                let mut proto_fields = Vec::new();
                let mut field_number = 1;

                for (key, value) in fields {
                    // Map Vector value types to protobuf types
                    let field_type = match value {
                        vrl::value::Value::Integer(_) => {
                            prost_types::field_descriptor_proto::Type::Int64
                        }
                        vrl::value::Value::Float(_) => {
                            prost_types::field_descriptor_proto::Type::Double
                        }
                        vrl::value::Value::Boolean(_) => {
                            prost_types::field_descriptor_proto::Type::Bool
                        }
                        vrl::value::Value::Bytes(_) => {
                            prost_types::field_descriptor_proto::Type::String
                        }
                        vrl::value::Value::Timestamp(_) => {
                            prost_types::field_descriptor_proto::Type::String
                        }
                        _ => prost_types::field_descriptor_proto::Type::String, // Default to string for complex types
                    };

                    proto_fields.push(prost_types::FieldDescriptorProto {
                        name: Some(key.to_string()),
                        number: Some(field_number),
                        label: Some(prost_types::field_descriptor_proto::Label::Optional as i32),
                        r#type: Some(field_type as i32),
                        type_name: None,
                        extendee: None,
                        default_value: None,
                        oneof_index: None,
                        json_name: Some(key.to_string()),
                        options: None,
                        proto3_optional: None,
                    });

                    field_number += 1;
                }

                let message_proto = prost_types::DescriptorProto {
                    name: Some("LogRecord".to_string()),
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
                    name: Some("dynamic_file.proto".to_string()),
                    message_type: vec![message_proto.clone()],
                    ..Default::default()
                };

                let file_descriptor_set = prost_types::FileDescriptorSet {
                    file: vec![file_proto],
                };

                // Build a FileDescriptor
                let pool =
                    prost_reflect::DescriptorPool::from_file_descriptor_set(file_descriptor_set)
                        .unwrap();

                let message_descriptor: prost_reflect::MessageDescriptor =
                    pool.get_message_by_name("LogRecord").unwrap();

                Ok(message_descriptor)
            }
            _ => Err(ZerobusSinkError::EncodingError {
                message: "Unsupported event type for schema inference".to_string(),
            }),
        }
    }

    async fn get_descriptor_or_infer(
        sample_event: &Event,
        descriptor: &Arc<Mutex<Option<prost_reflect::MessageDescriptor>>>,
    ) -> Result<prost_reflect::MessageDescriptor, ZerobusSinkError> {
        let mut guard = descriptor.lock().await;

        if let Some(existing) = &*guard {
            return Ok(existing.clone());
        }

        // Use configured schema if available, otherwise infer from event
        let new_value = Self::infer_schema_from_event(sample_event)?;

        *guard = Some(new_value.clone());
        Ok(new_value)
    }

    /// Ensure we have an active stream, creating one if necessary.
    async fn ensure_stream(&self, sample_event: &Event) -> Result<(), ZerobusSinkError> {
        let mut stream_guard = self.stream.lock().await;

        if stream_guard.is_none() {
            // Store the descriptor for encoding

            let descriptor = Self::get_descriptor_or_infer(sample_event, &self.descriptor).await?;
            let table_properties = TableProperties {
                table_name: self.config.table_name.clone(),
                descriptor_proto: descriptor.descriptor_proto().clone(),
            };

            let stream_options = Some(self.config.stream_options.clone().into());

            // Create stream using OAuth authentication
            // The SDK handles OAuth token exchange and UC endpoint automatically
            let stream = match &self.config.auth {
                super::config::DatabricksAuthentication::OAuth {
                    client_id,
                    client_secret,
                } => self
                    .sdk
                    .create_stream(
                        table_properties,
                        client_id.inner().to_string(),
                        client_secret.inner().to_string(),
                        stream_options,
                    )
                    .await
                    .map_err(|e| ZerobusSinkError::StreamInitError {
                        message: format!("Failed to create Zerobus stream: {}", e),
                    })?,
            };

            *stream_guard = Some(stream);
        }

        Ok(())
    }

    /// Process a batch of events.
    pub async fn process_events(
        &self,
        events: Vec<Event>,
    ) -> Result<ZerobusResponse, ZerobusSinkError> {
        if events.is_empty() {
            return Ok(ZerobusResponse { count: 0 });
        }

        // Ensure we have an active stream
        self.ensure_stream(&events[0]).await?;

        let stream_guard = self.stream.lock().await;
        let stream = stream_guard.as_ref().unwrap();

        // Get the descriptor for encoding
        let descriptor_guard = self.descriptor.lock().await;
        let descriptor =
            descriptor_guard
                .as_ref()
                .ok_or_else(|| ZerobusSinkError::EncodingError {
                    message: "Descriptor not initialized".to_string(),
                })?;

        let mut ack_futures = Vec::new();
        let encode_options = vrl::protobuf::encode::Options {
            use_json_names: false,
        };

        // Process each event
        for event in events.iter() {
            // Encode event to protobuf bytes
            let encoded_data = if let Event::Log(log_event) = event {
                let dynamic_message = encode_message(
                    descriptor,
                    log_event.clone().into_parts().0,
                    &encode_options,
                )
                .map_err(|e| ZerobusSinkError::EncodingError {
                    message: format!("Failed to encode event to protobuf: {}", e),
                })?;
                dynamic_message.encode_to_vec()
            } else {
                return Err(ZerobusSinkError::EncodingError {
                    message: "Unsupported event type".to_string(),
                });
            };

            // Ingest the record and collect the acknowledgment future
            let ack_future = stream.ingest_record(encoded_data).await.map_err(|e| {
                ZerobusSinkError::IngestionError {
                    message: format!("Failed to ingest record: {}", e),
                }
            })?;

            ack_futures.push(ack_future);
        }

        // Wait for all acknowledgments
        let mut success_count = 0;
        for ack_future in ack_futures {
            match ack_future.await {
                Ok(_offset) => {
                    success_count += 1;
                }
                Err(e) => {
                    return Err(ZerobusSinkError::IngestionError {
                        message: format!("Record acknowledgment failed: {}", e),
                    });
                }
            }
        }

        Ok(ZerobusResponse {
            count: success_count,
        })
    }
}

impl Service<ZerobusRequest> for ZerobusService {
    type Response = ZerobusResponse;
    type Error = ZerobusSinkError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        // Always ready to accept requests
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: ZerobusRequest) -> Self::Future {
        let service = self.clone();

        Box::pin(async move { service.process_events(request.events).await })
    }
}

impl Clone for ZerobusService {
    fn clone(&self) -> Self {
        // Note: ZerobusSdk::new can fail, but Clone trait doesn't support Result.
        // This should be safe since we already validated the config in the constructor.
        let sdk = ZerobusSdk::new(
            self.config.ingestion_endpoint.clone(),
            self.config.unity_catalog_endpoint.clone(),
        )
        .expect("ZerobusSdk creation should not fail for validated config");

        Self {
            sdk,
            config: self.config.clone(),
            stream: Arc::clone(&self.stream),
            descriptor: Arc::clone(&self.descriptor),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinks::databricks_zerobus::config::ZerobusStreamOptions;
    use vector_lib::sensitive_string::SensitiveString;

    fn create_test_config() -> ZerobusSinkConfig {
        ZerobusSinkConfig {
            ingestion_endpoint: "https://test.databricks.com".to_string(),
            table_name: "test.default.logs".to_string(),
            unity_catalog_endpoint: "https://test-workspace.databricks.com".to_string(),
            auth: crate::sinks::databricks_zerobus::config::DatabricksAuthentication::OAuth {
                client_id: SensitiveString::from("test-client-id".to_string()),
                client_secret: SensitiveString::from("test-client-secret".to_string()),
            },
            use_tls: true,
            schema: None,
            stream_options: ZerobusStreamOptions::default(),
            custom_headers: None,
            batch: Default::default(),
            request: Default::default(),
            acknowledgements: Default::default(),
        }
    }

    #[test]
    fn test_service_with_single_custom_header() {
        use std::collections::HashMap;

        let mut config = create_test_config();
        let mut headers = HashMap::new();
        headers.insert(
            "s2s-principal-context-sig-bin".to_string(),
            SensitiveString::from("base64encodedvalue".to_string()),
        );
        config.custom_headers = Some(headers);

        let result = ZerobusService::new(config);
        assert!(result.is_ok());

        let service = result.unwrap();
        let custom_headers = service.config.custom_headers.as_ref().unwrap();
        assert_eq!(custom_headers.len(), 1);
        assert!(custom_headers.contains_key("s2s-principal-context-sig-bin"));
    }

    #[test]
    fn test_sensitive_string_not_exposed_in_debug() {
        use std::collections::HashMap;

        let mut config = create_test_config();
        let mut headers = HashMap::new();
        headers.insert(
            "X-Secret-Token".to_string(),
            SensitiveString::from("super-secret-value".to_string()),
        );
        config.custom_headers = Some(headers);

        let service = ZerobusService::new(config).unwrap();
        let debug_output = format!("{:?}", service.config.custom_headers);

        // SensitiveString should not expose the actual value in debug output
        // The exact format depends on SensitiveString's Debug impl, but it shouldn't contain the secret
        assert!(!debug_output.contains("super-secret-value"));
    }

    // Tests for core encoding/decoding logic

    #[test]
    fn test_infer_schema_from_event_simple() {
        use vector_lib::event::{Event, LogEvent};

        let mut log_event = LogEvent::default();
        log_event.insert("message", "test message");
        log_event.insert("level", "info");
        let event = Event::Log(log_event);

        let result = ZerobusService::infer_schema_from_event(&event);
        assert!(result.is_ok());

        let descriptor = result.unwrap();
        assert_eq!(descriptor.name(), "LogRecord");
        assert!(descriptor.fields().len() >= 2); // At least our two fields
    }

    #[test]
    fn test_infer_schema_from_event_various_types() {
        use vector_lib::event::{Event, LogEvent};

        let mut log_event = LogEvent::default();
        log_event.insert("string_field", "text");
        log_event.insert("int_field", 42i64);
        log_event.insert("float_field", 3.14f64);
        log_event.insert("bool_field", true);
        let event = Event::Log(log_event);

        let result = ZerobusService::infer_schema_from_event(&event);
        assert!(result.is_ok());

        let descriptor = result.unwrap();
        assert!(descriptor.fields().len() >= 4);

        // Find and verify field types
        let fields: std::collections::HashMap<_, _> = descriptor
            .fields()
            .map(|f| {
                let t = f.field_descriptor_proto().r#type.unwrap();
                (String::from(f.name()), t)
            })
            .collect();

        assert_eq!(
            fields.get("string_field"),
            Some(&(prost_types::field_descriptor_proto::Type::String as i32))
        );
        assert_eq!(
            fields.get("int_field"),
            Some(&(prost_types::field_descriptor_proto::Type::Int64 as i32))
        );
        assert_eq!(
            fields.get("float_field"),
            Some(&(prost_types::field_descriptor_proto::Type::Double as i32))
        );
        assert_eq!(
            fields.get("bool_field"),
            Some(&(prost_types::field_descriptor_proto::Type::Bool as i32))
        );
    }

    #[test]
    fn test_infer_schema_from_event_with_timestamp() {
        use chrono::Utc;
        use vector_lib::event::{Event, LogEvent};

        let mut log_event = LogEvent::default();
        log_event.insert("message", "test");
        log_event.insert("timestamp", Utc::now());
        let event = Event::Log(log_event);

        let result = ZerobusService::infer_schema_from_event(&event);
        assert!(result.is_ok());

        let descriptor = result.unwrap();
        // Timestamp should be inferred as String type
        let timestamp_field = descriptor.get_field_by_name("timestamp");
        assert!(timestamp_field.is_some());
    }
}
