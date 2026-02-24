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

        // Clone events to preserve them for build_request() where they're encoded to protobuf.
        // The RequestBuilder trait requires splitting input into metadata and events, but we
        // need the original Event objects for dynamic protobuf encoding via prost-reflect
        // rather than working with the pre-encoded payload.
        let events_clone = input.clone();

        let metadata = BricklensIngestMetadata {
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
        // Pass events directly to service layer for protobuf encoding.
        // No JSON intermediate representation - work directly with Vector's Event type.
        BricklensIngestRequest {
            events: metadata.events,
            metadata: request_metadata,
            finalizers: metadata.finalizers,
        }
    }
}
