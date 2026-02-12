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

use super::{
    config::ZerobusSinkConfig, error::ZerobusSinkError, unity_catalog_schema,
};

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
    encode_options: vrl::protobuf::encode::Options,
}

impl ZerobusService {
    pub async fn new(config: ZerobusSinkConfig) -> Result<Self, ZerobusSinkError> {
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

        // Load schema based on configuration (always required)
        let descriptor = match &config.schema {
            super::config::SchemaSource::Path { .. } => {
                // Load from file synchronously
                Self::build_descriptor_from_config(&config.schema).map_err(|e| {
                    ZerobusSinkError::ConfigError {
                        message: format!("Failed to load descriptor: {}", e),
                    }
                })?
            }
            super::config::SchemaSource::UnityCatalog => {
                // Fetch from Unity Catalog API asynchronously
                let (client_id, client_secret) = match &config.auth {
                    super::config::DatabricksAuthentication::OAuth {
                        client_id,
                        client_secret,
                    } => (client_id.inner(), client_secret.inner()),
                };

                let table_schema = unity_catalog_schema::fetch_table_schema(
                    &config.unity_catalog_endpoint,
                    &config.table_name,
                    client_id,
                    client_secret,
                )
                .await?;

                unity_catalog_schema::generate_descriptor_from_schema(&table_schema)?
            }
        };

        let encode_options = vrl::protobuf::encode::Options {
            use_json_names: false,
        };

        Ok(Self {
            sdk,
            config,
            stream: Arc::new(Mutex::new(None)),
            descriptor: Arc::new(Mutex::new(Some(descriptor))),
            encode_options,
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
            super::config::SchemaSource::UnityCatalog => {
                // This variant should not reach here - it's handled asynchronously
                Err(ZerobusSinkError::ConfigError {
                    message: "UnityCatalog schema should be fetched asynchronously".to_string(),
                })
            }
        }
    }

    async fn get_descriptor(&self) -> Result<prost_reflect::MessageDescriptor, ZerobusSinkError> {
        let guard = self.descriptor.lock().await;

        guard
            .as_ref()
            .cloned()
            .ok_or_else(|| ZerobusSinkError::ConfigError {
                message: "Schema should have been loaded during initialization".to_string(),
            })
    }

    /// Ensure we have an active stream, creating one if necessary.
    async fn ensure_stream(&self, _sample_event: &Event) -> Result<(), ZerobusSinkError> {
        let mut stream_guard = self.stream.lock().await;

        if stream_guard.is_none() {
            // Get the descriptor loaded during initialization

            let descriptor = self.get_descriptor().await?;
            let table_properties = TableProperties {
                table_name: self.config.table_name.clone(),
                descriptor_proto: Some(descriptor.descriptor_proto().clone()),
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

        let num_events = events.len();

        let mut batch: Vec<Vec<u8>> = Vec::with_capacity(num_events);

        // Process each event and collect the last acknowledgment future
        for event in events.into_iter() {
            let encoded_data = if let Event::Log(log_event) = event {
                let dynamic_message =
                    encode_message(descriptor, log_event.into_parts().0, &self.encode_options)
                        .map_err(|e| ZerobusSinkError::EncodingError {
                            message: format!("Failed to encode event to protobuf: {}", e),
                        })?;
                dynamic_message.encode_to_vec()
            } else {
                return Err(ZerobusSinkError::EncodingError {
                    message: "Unsupported event type".to_string(),
                });
            };
            batch.push(encoded_data);
        }

        let ack_future =
            stream
                .ingest_records(batch)
                .await
                .map_err(|e| ZerobusSinkError::IngestionError {
                    message: format!("Failed to ingest batch: {}", e),
                })?;

        // Wait for it.
        if let Err(e) = ack_future.await {
            return Err(ZerobusSinkError::IngestionError {
                message: format!("Batch acknowledgment failed: {}", e),
            });
        }

        Ok(ZerobusResponse { count: num_events })
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
            encode_options: self.encode_options.clone(),
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
            schema: crate::sinks::databricks_zerobus::config::SchemaSource::UnityCatalog,
            stream_options: ZerobusStreamOptions::default(),
            custom_headers: None,
            batch: Default::default(),
            request: Default::default(),
            acknowledgements: Default::default(),
        }
    }

    #[tokio::test]
    async fn test_service_with_single_custom_header() {
        use std::collections::HashMap;

        let mut config = create_test_config();
        let mut headers = HashMap::new();
        headers.insert(
            "s2s-principal-context-sig-bin".to_string(),
            SensitiveString::from("base64encodedvalue".to_string()),
        );
        config.custom_headers = Some(headers);

        let result = ZerobusService::new(config).await;
        assert!(result.is_ok());

        let service = result.unwrap();
        let custom_headers = service.config.custom_headers.as_ref().unwrap();
        assert_eq!(custom_headers.len(), 1);
        assert!(custom_headers.contains_key("s2s-principal-context-sig-bin"));
    }

    #[tokio::test]
    async fn test_sensitive_string_not_exposed_in_debug() {
        use std::collections::HashMap;

        let mut config = create_test_config();
        let mut headers = HashMap::new();
        headers.insert(
            "X-Secret-Token".to_string(),
            SensitiveString::from("super-secret-value".to_string()),
        );
        config.custom_headers = Some(headers);

        let service = ZerobusService::new(config).await.unwrap();
        let debug_output = format!("{:?}", service.config.custom_headers);

        // SensitiveString should not expose the actual value in debug output
        // The exact format depends on SensitiveString's Debug impl, but it shouldn't contain the secret
        assert!(!debug_output.contains("super-secret-value"));
    }

}
