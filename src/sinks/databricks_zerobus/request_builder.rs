//! Request builder for Zerobus requests.

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

use super::service::ZerobusRequest;

#[derive(Clone)]
pub struct ZerobusRequestBuilder {
    compression: Compression,
    encoder: (
        Transformer,
        crate::codecs::Encoder<vector_lib::codecs::encoding::Framer>,
    ),
}

/// Metadata that includes the original events for Zerobus processing
pub struct ZerobusMetadata {
    finalizers: EventFinalizers,
    events: Vec<Event>,
}

impl ZerobusRequestBuilder {
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

impl RequestBuilder<Vec<Event>> for ZerobusRequestBuilder {
    type Metadata = ZerobusMetadata;
    type Events = Vec<Event>;
    type Encoder = (
        Transformer,
        crate::codecs::Encoder<vector_lib::codecs::encoding::Framer>,
    );
    type Payload = Bytes;
    type Request = ZerobusRequest;
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

        // Clone events to store them in metadata - we need them in build_request
        let events_clone = input.clone();

        let metadata = ZerobusMetadata {
            finalizers,
            events: events_clone,
        };

        (metadata, metadata_builder, input)
    }

    fn build_request(
        &self,
        metadata: Self::Metadata,
        request_metadata: RequestMetadata,
        _payload: EncodeResult<Self::Payload>,
    ) -> Self::Request {
        // Use the events we stored in metadata
        ZerobusRequest {
            events: metadata.events,
            metadata: request_metadata,
            finalizers: metadata.finalizers,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::Encoder;
    use crate::codecs::Transformer;
    use vector_lib::codecs::encoding::{FramingConfig, JsonSerializerConfig, SerializerConfig};
    use vector_lib::event::{Event, LogEvent};
    use vector_lib::request_metadata::GroupedCountByteSize;

    fn create_test_encoder() -> (Transformer, Encoder<vector_lib::codecs::encoding::Framer>) {
        let serializer = SerializerConfig::Json(JsonSerializerConfig::default())
            .build()
            .expect("Failed to build serializer");
        let framer = FramingConfig::NewlineDelimited.build();
        let encoder = Encoder::<vector_lib::codecs::encoding::Framer>::new(framer, serializer);
        (Transformer::default(), encoder)
    }

    #[test]
    fn test_build_request_preserves_all_events() {
        let encoder = create_test_encoder();
        let builder = ZerobusRequestBuilder::new(Compression::None, encoder);

        let mut events = Vec::new();
        for i in 0..10 {
            let mut log_event = LogEvent::default();
            log_event.insert("message", format!("test {}", i));
            log_event.insert("id", i);
            events.push(Event::Log(log_event));
        }

        let (metadata, request_metadata_builder, _output_events) = builder.split_input(events);

        let payload =
            EncodeResult::uncompressed(Bytes::from("dummy"), GroupedCountByteSize::new_untagged());
        let request_metadata = request_metadata_builder.build(&payload);
        let request = builder.build_request(metadata, request_metadata, payload);

        assert_eq!(request.events.len(), 10);

        // Verify event content is preserved
        for (i, event) in request.events.iter().enumerate() {
            if let Event::Log(log) = event {
                let id = log.get("id").unwrap().as_integer().unwrap();
                assert_eq!(id, i as i64);
            }
        }
    }
}
