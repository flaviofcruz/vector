use std::{fmt, num::NonZeroUsize};

use async_trait::async_trait;
use futures::{StreamExt, future, stream::BoxStream};
use futures_util::Stream;
use prost::Message;
use tower::Service;
use vector_lib::internal_event::{ComponentEventsDropped, UNINTENTIONAL};
use vector_lib::request_metadata::GroupedCountByteSize;
use vector_lib::stream::{BatcherSettings, DriverResponse, batcher::data::BatchReduce};
use vector_lib::{ByteSizeOf, EstimatedJsonEncodedSizeOf, config::telemetry};
use vrl::path::OwnedTargetPath;

use crate::{
    event::{Event, EventFinalizers, Finalizable},
    sinks::util::{SinkBuilderExt, StreamSink, metadata::RequestMetadataBuilder},
};

use super::service::KPPRequest;
use kafka_producer_proxy::proto::{self as proto_kpp, KafkaMessage};

/// Based on the KPP Protobuf definition, we expect incoming messages to have a
/// field to specify the key, message and log.
/// The `key_field`, `message_field`, and `log_entry` are all used to find the
/// name of that field in the incoming message itself
pub struct KafkaProducerProxySink<S> {
    pub topic: String,
    pub key_field: OwnedTargetPath,
    pub message_field: OwnedTargetPath,
    pub log_entry: OwnedTargetPath,
    pub batch_settings: BatcherSettings,
    pub service: S,
}

/// Temporary struct to collect events during batching.
#[derive(Clone, Default)]
struct EventCollection {
    pub finalizers: EventFinalizers,
    pub events: Vec<KafkaMessage>,
    pub events_byte_size: usize,
    pub events_json_byte_size: GroupedCountByteSize,
}

/// Event with additional metadata
struct KafkaEvent {
    finalizers: EventFinalizers,
    byte_size: usize,
    json_byte_size: GroupedCountByteSize,
    message: proto_kpp::KafkaMessage,
}

impl<S> KafkaProducerProxySink<S>
where
    S: Service<KPPRequest>,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: fmt::Debug + Into<crate::Error> + Send + 'static,
{
    fn transform_stream<'a>(
        &self,
        input: BoxStream<'a, Event>,
    ) -> impl Stream<Item = KPPRequest> + Send + 'a {
        let topic = self.topic.clone();
        let message_field = self.message_field.clone();
        let key_field = self.key_field.clone();
        let log_entry_field = self.log_entry.clone();
        let batch_settings = self.batch_settings;

        input
            .filter_map(move |event| {
                let message = event
                    .as_log()
                    .get(&message_field)
                    .and_then(|v| v.as_bytes())
                    .map(|bytes| bytes.to_vec());

                if message.is_some() {
                    future::ready(Some((event, message)))
                } else {
                    emit!(ComponentEventsDropped::<UNINTENTIONAL> {
                        count: 1,
                        reason: "Missing required message field",
                    });
                    future::ready(None)
                }
            })
            .map(move |(mut event, message)| {
                let log = event.as_log();

                let key = log
                    .get(&key_field)
                    .and_then(|v| v.as_str())
                    .map(String::from);

                let log_entry = log
                    .get(&log_entry_field)
                    .and_then(|v| v.as_bytes())
                    .map(|bytes| bytes.to_vec());

                let mut byte_size = telemetry().create_request_count_byte_size();
                byte_size.add_event(&event, event.estimated_json_encoded_size_of());
                KafkaEvent {
                    byte_size: event.size_of(),
                    json_byte_size: byte_size,
                    finalizers: event.take_finalizers(),
                    message: proto_kpp::KafkaMessage {
                        topic_name: Some(topic.clone()),
                        key: key.clone(),
                        data: message,
                        log_entry: log_entry.clone(),
                    },
                }
            })
            .batched(batch_settings.as_reducer_config(
                |data: &KafkaEvent| data.message.encoded_len(),
                BatchReduce::new(|event_collection: &mut EventCollection, item: KafkaEvent| {
                    event_collection.finalizers.merge(item.finalizers);
                    event_collection.events.push(item.message);
                    event_collection.events_byte_size += item.byte_size;
                    event_collection.events_json_byte_size += item.json_byte_size;
                }),
            ))
            // This logic is similar to RequestBuilder. The building of the request is split into two parts:
            // RequestMetadataBuilder and EventCollection
            // This can be separated into a RequestBuilder if additional logic is required in the future
            .map(|event_collection| {
                let builder = RequestMetadataBuilder::new(
                    event_collection.events.len(),
                    event_collection.events_byte_size,
                    event_collection.events_json_byte_size,
                );

                let encoded_events = proto_kpp::KafkaMessages {
                    messages: event_collection.events,
                };

                let byte_size = encoded_events.encoded_len();
                let bytes_len =
                    NonZeroUsize::new(byte_size).expect("payload should never be zero length");

                KPPRequest {
                    finalizers: event_collection.finalizers,
                    metadata: builder.with_request_size(bytes_len),
                    request: encoded_events,
                }
            })
    }

    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.transform_stream(input)
            .into_driver(self.service)
            .run()
            .await
    }
}

#[async_trait]
impl<S> StreamSink<Event> for KafkaProducerProxySink<S>
where
    S: Service<KPPRequest> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: fmt::Debug + Into<crate::Error> + Send + 'static,
{
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use crate::sinks::kafka_producer_proxy::service::KPPResponse;
    use crate::sinks::util::{BatchConfig, RealtimeEventBasedDefaultBatchSettings};
    use futures::{Future, stream};
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tower::Service;
    use vector_lib::event::LogEvent;
    use vrl::owned_value_path;

    #[derive(Debug, Clone)]
    pub struct MockKPPService;

    impl Service<KPPRequest> for MockKPPService {
        type Response = KPPResponse;
        type Error = Error;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: KPPRequest) -> Self::Future {
            Box::pin(async move {
                Ok(KPPResponse {
                    all_succeeded: true,
                    results: Vec::new(),
                    event_byte_size: GroupedCountByteSize::default(),
                })
            })
        }
    }

    fn create_test_event(key: &str, message: &str, log: &str) -> Event {
        let mut event = Event::Log(LogEvent::from("Test Message"));
        event.as_mut_log().insert("key", key);
        event.as_mut_log().insert("message", message);
        event.as_mut_log().insert("log", log);
        event
    }

    fn create_test_sink() -> KafkaProducerProxySink<MockKPPService> {
        let mut batch_config = BatchConfig::<RealtimeEventBasedDefaultBatchSettings>::default();
        batch_config.max_events = Some(10);

        KafkaProducerProxySink {
            topic: "test".to_string(),
            key_field: OwnedTargetPath::event(owned_value_path!("key")),
            message_field: OwnedTargetPath::event(owned_value_path!("message")),
            log_entry: OwnedTargetPath::event(owned_value_path!("log")),
            batch_settings: batch_config.into_batcher_settings().unwrap(),
            service: MockKPPService,
        }
    }

    #[test]
    fn test_sink_creation() {
        let sink = create_test_sink();
        assert_eq!(sink.topic, "test");
    }

    #[tokio::test]
    async fn test_successful_event_processing() {
        let expected_messages = vec![
            KafkaMessage {
                topic_name: Some("test".to_string()),
                key: Some("key1".to_string()),
                data: Some("test-message1".as_bytes().to_vec()),
                log_entry: Some("test-log1".as_bytes().to_vec()),
            },
            KafkaMessage {
                topic_name: Some("test".to_string()),
                key: Some("key2".to_string()),
                data: Some("test-message2".as_bytes().to_vec()),
                log_entry: Some("test-log2".as_bytes().to_vec()),
            },
        ];

        let sink = create_test_sink();
        let events = vec![
            create_test_event("key1", "test-message1", "test-log1"),
            create_test_event("key2", "test-message2", "test-log2"),
        ];
        let input = Box::pin(stream::iter(events));
        let results: Vec<KPPRequest> = sink.transform_stream(input).collect().await;

        assert_eq!(results.len(), 1);
        let kafka_request = &results[0];
        assert_eq!(kafka_request.request.messages.len(), 2);
        assert_eq!(kafka_request.request.messages, expected_messages);
    }

    // test message with message field not present and see if gets dropped
    #[tokio::test]
    async fn test_missing_fields() {
        let expected_messages = vec![KafkaMessage {
            topic_name: Some("test".to_string()),
            key: None,
            data: Some("test-message1".as_bytes().to_vec()),
            log_entry: None,
        }];

        let sink = create_test_sink();
        let mut valid_event = Event::Log(LogEvent::from("Test Message"));
        valid_event.as_mut_log().insert("message", "test-message1");

        let mut invalid_event = Event::Log(LogEvent::default());
        invalid_event.as_mut_log().insert("key", "key1");

        let input = Box::pin(stream::iter(vec![valid_event, invalid_event]));
        let results: Vec<KPPRequest> = sink.transform_stream(input).collect().await;

        assert_eq!(results.len(), 1);
        let kafka_request = &results[0];
        assert_eq!(kafka_request.request.messages.len(), 1);
        assert_eq!(kafka_request.request.messages, expected_messages);
    }

    #[tokio::test]
    async fn test_batch_event_processing() {
        let events: Vec<Event> = (0..50)
            .map(|i| {
                create_test_event(
                    &format!("key{}", i),
                    &format!("message{}", i),
                    &format!("log{}", i),
                )
            })
            .collect();

        let expected_messages: Vec<KafkaMessage> = (0..50)
            .map(|i| KafkaMessage {
                topic_name: Some("test".to_string()),
                key: Some(format!("key{}", i)),
                data: Some(format!("message{}", i).as_bytes().to_vec()),
                log_entry: Some(format!("log{}", i).as_bytes().to_vec()),
            })
            .collect();

        let sink = create_test_sink();

        let input = Box::pin(stream::iter(events));
        let results: Vec<KPPRequest> = sink.transform_stream(input).collect().await;

        assert_eq!(results.len(), 5);
        let mut counter = 0;

        for result in results {
            assert_eq!(result.request.messages.len(), 10);
            assert_eq!(
                &result.request.messages,
                &expected_messages[counter..counter + 10]
            );
            counter += 10;
        }
    }
}
