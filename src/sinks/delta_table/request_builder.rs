// Converts Vector event batches into Delta table write requests by extracting metadata
// and assembling request structures for the service layer.

use std::sync::Arc;

use bytes::Bytes;
use vector_lib::codecs::JsonSerializer;
use vector_lib::codecs::encoding::{Framer, FramingConfig, Serializer};
use vector_lib::finalization::EventFinalizers;
use vector_lib::request_metadata::RequestMetadata;

use crate::{
    codecs::{Encoder, Transformer},
    event::{Event, Finalizable},
    sinks::util::{
        Compression, RequestBuilder, metadata::RequestMetadataBuilder,
        request_builder::EncodeResult,
    },
};

use super::service::{AzureDeltaRequest, DeltaTable};

/// Request builder configuration for Delta table write operations.
/// Implements Vector's RequestBuilder trait to transform event batches into write requests.
#[derive(Clone)]
pub struct AzureDeltaRequestOptions {
    /// Target Delta table that will receive the events
    pub table: Arc<DeltaTable>,
}

impl RequestBuilder<Vec<Event>> for AzureDeltaRequestOptions {
    type Metadata = (EventFinalizers, Vec<Event>);
    type Events = Vec<Event>;
    type Encoder = (Transformer, Encoder<Framer>);
    type Payload = Bytes;
    type Request = AzureDeltaRequest;
    type Error = std::io::Error;

    /// Returns compression configuration. Delta table operations process raw events without compression to maintain data structure for Arrow conversion.
    fn compression(&self) -> Compression {
        Compression::None
    }

    /// Returns the encoder instance. Although we process raw events for Delta tables,
    /// we need to satisfy Vector's trait requirements.
    fn encoder(&self) -> &Self::Encoder {
        // This is never actually used since we pass raw events in metadata
        static DEFAULT_ENCODER: std::sync::OnceLock<(Transformer, Encoder<Framer>)> =
            std::sync::OnceLock::new();
        DEFAULT_ENCODER.get_or_init(|| {
            let framer = FramingConfig::NewlineDelimited.build();
            let serializer = Serializer::Json(JsonSerializer::new(
                Default::default(), // MetricTagValues::Single
                Default::default(), // JsonSerializerOptions { pretty: false }
            ));
            (
                Transformer::default(),
                Encoder::<Framer>::new(framer, serializer),
            )
        })
    }

    /// Processes incoming event batch to extract finalizers and prepare for request creation.
    /// Returns finalizers along with the events.
    fn split_input(
        &self,
        mut input: Vec<Event>,
    ) -> (Self::Metadata, RequestMetadataBuilder, Self::Events) {
        // Extract event finalizers for delivery acknowledgment tracking
        let finalizers = input.take_finalizers();

        // Build request tracking metadata from the event batch
        let builder = RequestMetadataBuilder::from_events(&input);

        // Return finalizers and events together to maintain ownership through the pipeline
        // The events in the third position are ignored since we pass them through metadata
        ((finalizers, input.clone()), builder, input)
    }

    /// Constructs the final Delta table write request from processed components.
    /// Combines events, finalizers, and table reference into a single request structure.
    fn build_request(
        &self,
        metadata: Self::Metadata,
        request_metadata: RequestMetadata,
        _payload: EncodeResult<Self::Payload>,
    ) -> Self::Request {
        let (finalizers, events) = metadata;

        AzureDeltaRequest {
            events,
            finalizers,
            request_metadata,
            table: Arc::clone(&self.table),
        }
    }
}
