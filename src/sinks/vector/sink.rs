use std::{fmt, num::NonZeroUsize};

use async_trait::async_trait;
use futures::{StreamExt, stream::BoxStream};
use prost::Message;
use tower::Service;
use vector_common::internal_event::vector_event::{
    VectorSinkEventMetadata, delivery_event::VectorSinkDeliveryEvent,
};
use vector_lib::event::event_log::generate_count_map_event_wrapper;
use vector_lib::{
    ByteSizeOf, EstimatedJsonEncodedSizeOf,
    config::telemetry,
    request_metadata::GroupedCountByteSize,
    stream::{BatcherSettings, DriverResponse, batcher::data::BatchReduce},
};

use super::service::VectorRequest;
use crate::{
    event::{Event, EventFinalizers, Finalizable, proto::EventWrapper},
    proto::vector as proto_vector,
    sinks::util::{SinkBuilderExt, StreamSink, metadata::RequestMetadataBuilder},
};

/// Data for a single event.
struct EventData {
    byte_size: usize,
    json_byte_size: GroupedCountByteSize,
    finalizers: EventFinalizers,
    wrapper: EventWrapper,
}

/// Temporary struct to collect events during batching.
#[derive(Clone)]
struct EventCollection {
    pub finalizers: EventFinalizers,
    pub events: Vec<EventWrapper>,
    pub events_byte_size: usize,
    pub events_json_byte_size: GroupedCountByteSize,
}

impl Default for EventCollection {
    fn default() -> Self {
        Self {
            finalizers: Default::default(),
            events: Default::default(),
            events_byte_size: Default::default(),
            events_json_byte_size: telemetry().create_request_count_byte_size(),
        }
    }
}

pub struct VectorSink<S> {
    pub batch_settings: BatcherSettings,
    pub service: S,
}

impl<S> VectorSink<S>
where
    S: Service<VectorRequest> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: fmt::Debug + Into<crate::Error> + Send,
{
    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        input
            .map(|mut event| {
                let mut byte_size = telemetry().create_request_count_byte_size();
                byte_size.add_event(&event, event.estimated_json_encoded_size_of());

                EventData {
                    byte_size: event.size_of(),
                    json_byte_size: byte_size,
                    finalizers: event.take_finalizers(),
                    wrapper: EventWrapper::from(event),
                }
            })
            .batched(self.batch_settings.as_reducer_config(
                |data: &EventData| data.wrapper.encoded_len(),
                BatchReduce::new(|event_collection: &mut EventCollection, item: EventData| {
                    event_collection.finalizers.merge(item.finalizers);
                    event_collection.events.push(item.wrapper);
                    event_collection.events_byte_size += item.byte_size;
                    event_collection.events_json_byte_size += item.json_byte_size;
                }),
            ))
            .map(|mut event_collection| {
                // This sink is exceptional in that it doesn't use other util classes / functions to build request
                // So we need to generate the delivery event log needed for the request
                let delivery_event_log = VectorSinkDeliveryEvent::with_count_map(
                    generate_count_map_event_wrapper(&event_collection.events, true),
                );
                let builder = RequestMetadataBuilder::new_with_event_log(
                    event_collection.events.len(),
                    event_collection.events_byte_size,
                    event_collection.events_json_byte_size,
                    Some(VectorSinkEventMetadata {
                        delivery_event: delivery_event_log,
                        file_send_event: None,
                    }),
                );

                // LA accounting ends at this outbound send. Capture it above before stripping
                // the wire metadata so VA cannot credit these LA requirements again.
                for event in &mut event_collection.events {
                    event.clear_delivery_path_accounting();
                }

                let encoded_events = proto_vector::PushEventsRequest {
                    events: event_collection.events,
                };

                let byte_size = encoded_events.encoded_len();
                let bytes_len =
                    NonZeroUsize::new(byte_size).expect("payload should never be zero length");

                VectorRequest {
                    finalizers: event_collection.finalizers,
                    metadata: builder.with_request_size(bytes_len),
                    request: encoded_events,
                }
            })
            .into_driver(self.service)
            .run()
            .await
    }
}

#[async_trait]
impl<S> StreamSink<Event> for VectorSink<S>
where
    S: Service<VectorRequest> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: fmt::Debug + Into<crate::Error> + Send,
{
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc, time::Duration};

    use vector_common::internal_event::vector_event::delivery_path::{
        DeliveryPathAccounting, DeliverySourceLabels,
    };

    use super::*;
    use crate::event::{EventStatus, LogEvent};

    struct SentResponse(GroupedCountByteSize);

    impl DriverResponse for SentResponse {
        fn event_status(&self) -> EventStatus {
            EventStatus::Delivered
        }

        fn events_sent(&self) -> &GroupedCountByteSize {
            &self.0
        }
    }

    #[tokio::test]
    async fn delivery_path_counts_stay_in_la_request_metadata_and_are_removed_from_wire() {
        let events = ["old-process", "new-process"].map(|process| {
            let mut event = LogEvent::from("service message");
            let mut accounting = DeliveryPathAccounting::at_read(
                DeliverySourceLabels {
                    process_generation_id: process.to_owned(),
                    time_parity: 3_600_000,
                    topic: "service-log".to_owned(),
                    delivery_method: "file".to_owned(),
                    service_system: "service".to_owned(),
                    source_pod_id: "pod".to_owned(),
                },
                ["archive".to_owned(), "online".to_owned()],
            );
            accounting.select_path("online");
            event
                .metadata_mut()
                .set_delivery_path_accounting(Some(accounting));
            event.metadata_mut().value_mut().insert(
                vrl::path!("delivery_path_accounting"),
                vrl::value!({"paths": ["archive", "online"]}),
            );
            event
                .metadata_mut()
                .value_mut()
                .insert(vrl::path!("unrelated"), vrl::value!("preserved"));
            Event::Log(event)
        });
        // Only the external receiver is replaced; the real sink builds and sends the request.
        let (tx, rx) = mpsc::sync_channel(1);
        let service = tower::service_fn(move |request: VectorRequest| {
            let response = SentResponse(
                request
                    .metadata
                    .events_estimated_json_encoded_byte_size()
                    .clone(),
            );
            tx.send(request).expect("capture LA request");
            async move { Ok::<_, crate::Error>(response) }
        });
        Box::new(VectorSink {
            batch_settings: BatcherSettings {
                timeout: Duration::from_secs(1),
                size_limit: 1_000_000,
                item_limit: 2,
            },
            service,
        })
        .run_inner(futures::stream::iter(events).boxed())
        .await
        .expect("successful send");

        let request = rx.try_recv().expect("one batched request");
        let paths = request
            .metadata
            .event_log_metadata()
            .delivery_event
            .count_map
            .values()
            .filter_map(|value| value.delivery_path_count.as_ref())
            .collect::<Vec<_>>();
        assert_eq!(paths.len(), 2);
        let mut processes = Vec::new();
        for count in paths {
            assert_eq!(count.path, "online");
            assert_eq!(count.count, 1);
            processes.push(count.source.process_generation_id.as_str());
        }
        processes.sort_unstable();
        assert_eq!(processes, ["new-process", "old-process"]);
        assert_eq!(request.request.events.len(), 2);
        for wrapper in request.request.events {
            #[allow(deprecated)]
            if let Some(crate::event::proto::Event::Log(log)) = &wrapper.event {
                let legacy_metadata = log.metadata.as_ref().expect("legacy metadata");
                let crate::event::proto::value::Kind::Map(map) =
                    legacy_metadata.kind.as_ref().unwrap()
                else {
                    panic!("metadata must be an object")
                };
                assert!(!map.fields.contains_key("delivery_path_accounting"));
                assert!(map.fields.contains_key("unrelated"));
            }
            let event = Event::from(wrapper);
            assert!(event.metadata().delivery_path_accounting().is_none());
            assert!(
                event
                    .metadata()
                    .value()
                    .get(vrl::path!("delivery_path_accounting"))
                    .is_none()
            );
            assert_eq!(
                event.metadata().value().get(vrl::path!("unrelated")),
                Some(&vrl::value!("preserved"))
            );
            assert_eq!(event.as_log()["message"], "service message".into());
        }
    }
}
