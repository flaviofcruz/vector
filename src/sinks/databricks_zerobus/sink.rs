//! The main Zerobus sink implementation.

use futures::stream::BoxStream;

use crate::codecs::Transformer;
use crate::sinks::prelude::*;
use crate::sinks::util::RealtimeSizeBasedDefaultBatchSettings;

use super::request_builder::ZerobusRequestBuilder;
use super::service::ZerobusService;

/// The main Zerobus sink.
pub struct ZerobusSink {
    service: ZerobusService,
    batch_settings: BatcherSettings,
}

impl ZerobusSink {
    pub fn new(
        service: ZerobusService,
        batch_config: BatchConfig<RealtimeSizeBasedDefaultBatchSettings>,
    ) -> Result<Self, crate::Error> {
        let batch_settings = batch_config.into_batcher_settings()?;

        Ok(Self {
            service,
            batch_settings,
        })
    }

    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        // Note: The encoder is required by the RequestBuilder trait but not actually used.
        // Zerobus encoding happens in the service layer using protobuf via prost-reflect.
        // The RequestBuilder ignores the encoded payload and passes raw events instead.
        use vector_lib::codecs::encoding::{
            Framer, FramingConfig, JsonSerializerConfig, SerializerConfig,
        };

        let serializer = SerializerConfig::Json(JsonSerializerConfig::default())
            .build()
            .expect("Failed to build serializer");
        let framer = FramingConfig::NewlineDelimited.build();
        let encoder_inner = Encoder::<Framer>::new(framer, serializer);
        let encoder = (Transformer::default(), encoder_inner);

        input
            .batched(self.batch_settings.as_byte_size_config())
            .request_builder(
                // Limit concurrency to what the SDK allows.
                self.service.config.stream_options.max_inflight_requests,
                ZerobusRequestBuilder::new(Compression::None, encoder),
            )
            .filter_map(|request| async move {
                match request {
                    Err(error) => {
                        emit!(SinkRequestBuildError { error });
                        None
                    }
                    Ok(req) => Some(req),
                }
            })
            .into_driver(self.service)
            .run()
            .await
    }
}

#[async_trait::async_trait]
impl StreamSink<Event> for ZerobusSink {
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}

/// Partitioner for Zerobus events.
/// For simplicity, we use a single partition (all events go to the same table).
#[allow(dead_code)]
struct ZerobusPartitioner {
    table_key: Option<String>,
}

#[allow(dead_code)]
impl ZerobusPartitioner {
    fn new() -> Self {
        Self { table_key: None }
    }
}

impl Partitioner for ZerobusPartitioner {
    type Item = Event;
    type Key = Option<String>;

    fn partition(&self, _item: &Self::Item) -> Self::Key {
        // For now, all events go to the same partition (same table)
        // In the future, we could partition by table name if supporting multi-table sinks
        self.table_key.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinks::databricks_zerobus::config::{ZerobusSinkConfig, ZerobusStreamOptions};
    use vector_lib::event::LogEvent;
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
    async fn test_sink_creation() {
        let config = create_test_config();
        let service = ZerobusService::new(config.clone()).await.unwrap();
        let sink = ZerobusSink::new(service, config.batch).unwrap();

        // Just verify the sink was created successfully
        assert!(sink.batch_settings.item_limit > 0);
    }

    #[test]
    fn test_partitioner() {
        let partitioner = ZerobusPartitioner::new();

        let mut log_event = LogEvent::default();
        log_event.insert("message", "test");
        let event = Event::Log(log_event);

        let key1 = partitioner.partition(&event);
        let key2 = partitioner.partition(&event);

        // All events should get the same partition key
        assert_eq!(key1, key2);
    }
}
