use std::io;

use bytes::Bytes;
use vector_lib::event::Event;
use vector_lib::finalization::EventFinalizers;
use vector_lib::request_metadata::RequestMetadata;

use crate::sinks::util::Compression;
use crate::{
    codecs::Transformer,
    sinks::util::{
        RequestBuilder, metadata::RequestMetadataBuilder, request_builder::EncodeResult,
    },
};

use super::service::BricklensIngestRequest;

#[derive(Clone)]
pub struct BricklensIngestRequestBuilder {
    compression: Compression,
    encoder: (
        Transformer,
        crate::codecs::Encoder<vector_lib::codecs::encoding::Framer>,
    ),
}

/// Metadata that includes the original events for Bricklens processing
pub struct BricklensIngestMetadata {
    finalizers: EventFinalizers,
    events: Vec<Event>,
}

impl BricklensIngestRequestBuilder {
    pub const fn new(
        compression: Compression,
        encoder: (
            Transformer,
            crate::codecs::Encoder<vector_lib::codecs::encoding::Framer>,
        ),
    ) -> Self {
        Self {
            compression,
            encoder,
        }
    }
}

impl RequestBuilder<Vec<Event>> for BricklensIngestRequestBuilder {
    type Metadata = BricklensIngestMetadata;
    type Events = Vec<Event>;
    type Encoder = (
        Transformer,
        crate::codecs::Encoder<vector_lib::codecs::encoding::Framer>,
    );
    type Payload = Bytes;
    type Request = BricklensIngestRequest;
    type Error = io::Error;

    fn compression(&self) -> Compression {
        self.compression
    }

    fn encoder(&self) -> &Self::Encoder {
        &self.encoder
    }

    fn split_input(
        &self,
        input: Vec<Event>,
    ) -> (Self::Metadata, RequestMetadataBuilder, Self::Events) {
        let finalizers = input
            .iter()
            .filter_map(|event| match event {
                Event::Log(log) => Some(log.metadata().finalizers().clone()),
                Event::Metric(metric) => Some(metric.metadata().finalizers().clone()),
                Event::Trace(trace) => Some(trace.metadata().finalizers().clone()),
            })
            .fold(EventFinalizers::default(), |mut acc, finalizers| {
                acc.merge(finalizers);
                acc
            });
        let metadata_builder = RequestMetadataBuilder::from_events(&input);

        // Encoding to protobuf happens in the service layer (via prost-reflect) from the events
        // carried in metadata, NOT from the RequestBuilder payload. Move the batch into metadata
        // and hand the builder an empty event list: the (unused) payload encoder then does no real
        // work and we avoid cloning the entire batch on every request. `build_request` ignores the
        // payload, and the request metadata's event count / estimated size already came from
        // `from_events(&input)` above.
        let metadata = BricklensIngestMetadata {
            finalizers,
            events: input,
        };

        (metadata, metadata_builder, Vec::new())
    }

    fn build_request(
        &self,
        metadata: Self::Metadata,
        request_metadata: RequestMetadata,
        _payload: EncodeResult<Self::Payload>,
    ) -> Self::Request {
        // Pass events directly to service layer for protobuf encoding.
        // No JSON intermediate representation - work directly with Vector's Event type.
        BricklensIngestRequest {
            events: metadata.events,
            metadata: request_metadata,
            finalizers: metadata.finalizers,
        }
    }
}

#[cfg(test)]
mod tests {
    use vector_lib::codecs::encoding::{
        Framer, FramingConfig, JsonSerializerConfig, SerializerConfig,
    };
    use vector_lib::event::{Event, LogEvent};

    use crate::codecs::{Encoder, Transformer};
    use crate::sinks::util::{Compression, RequestBuilder};

    use super::BricklensIngestRequestBuilder;

    fn make_builder() -> BricklensIngestRequestBuilder {
        let serializer = SerializerConfig::Json(JsonSerializerConfig::default())
            .build()
            .expect("serializer");
        let framer = FramingConfig::NewlineDelimited.build();
        let encoder = (
            Transformer::default(),
            Encoder::<Framer>::new(framer, serializer),
        );
        BricklensIngestRequestBuilder::new(Compression::None, encoder)
    }

    fn log_event(message: &str) -> Event {
        let mut log = LogEvent::default();
        log.insert("message", message);
        Event::Log(log)
    }

    #[test]
    fn split_input_skips_payload_encode_and_preserves_events() {
        let builder = make_builder();
        let (metadata, _meta_builder, events) =
            builder.split_input(vec![log_event("a"), log_event("b"), log_event("c")]);

        // The events handed to the (unused) RequestBuilder payload encoder must be empty so the
        // batch is not needlessly re-encoded; the real events are carried in metadata for the
        // service layer to protobuf-encode.
        assert!(
            events.is_empty(),
            "split_input must return an empty event list for the payload encoder"
        );
        assert_eq!(metadata.events.len(), 3, "all events must be kept in metadata");
    }

    #[test]
    fn build_request_carries_all_events_through_empty_payload() {
        let builder = make_builder();
        let (metadata, meta_builder, events) =
            builder.split_input(vec![log_event("a"), log_event("b")]);

        // Encoding the empty event list must succeed (and produce an empty payload)...
        let payload = builder
            .encode_events(events)
            .expect("encoding an empty event list should succeed");
        let request_metadata = meta_builder.build(&payload);

        // ...and the built request must still carry every original event.
        let request = builder.build_request(metadata, request_metadata, payload);
        assert_eq!(request.events.len(), 2);
    }
}
