use std::num::NonZeroUsize;

use super::request_builder::EncodeResult;
use vector_common::internal_event::delivery_event::{
    EventWithEventLog, VectorSinkDeliveryEvent, combine_sink_delivery_events,
};
use vector_lib::{
    ByteSizeOf, EstimatedJsonEncodedSizeOf, config,
    request_metadata::{GetEventCountTags, GroupedCountByteSize, RequestMetadata},
};

#[derive(Clone, Default)]
pub struct RequestMetadataBuilder {
    event_count: usize,
    events_byte_size: usize,
    grouped_events_byte_size: GroupedCountByteSize,
    // Keep it an option since we can't default expect metadata to work for each sink
    event_log_metadata: Option<VectorSinkDeliveryEvent>,
}

impl RequestMetadataBuilder {
    pub fn from_events<E>(events: &[E]) -> Self
    where
        E: ByteSizeOf + GetEventCountTags + EstimatedJsonEncodedSizeOf,
    {
        let mut size = config::telemetry().create_request_count_byte_size();

        let mut events_byte_size = 0;

        for event in events {
            events_byte_size += event.size_of();
            size.add_event(event, event.estimated_json_encoded_size_of());
        }

        Self {
            event_count: events.len(),
            events_byte_size,
            grouped_events_byte_size: size,
            event_log_metadata: None,
        }
    }

    pub fn from_events_with_event_log<E>(events: &[E]) -> Self
    where
        // Events is defined to take a generic type that can compute certain metadata
        // To fit this setup, we also require that it can generate event log metadata too
        // This will require a bit more additional code, mostly in the event library to implement this trait
        E: ByteSizeOf + GetEventCountTags + EstimatedJsonEncodedSizeOf + EventWithEventLog,
    {
        let mut builder = Self::from_events(events);
        builder.event_log_metadata = Some(combine_sink_delivery_events(
            events
                .iter()
                .map(|event| event.compute_event_log())
                .collect::<Vec<_>>(),
        ));
        builder
    }

    pub fn from_event<E>(event: &E) -> Self
    where
        E: ByteSizeOf + GetEventCountTags + EstimatedJsonEncodedSizeOf,
    {
        let mut size = config::telemetry().create_request_count_byte_size();
        size.add_event(event, event.estimated_json_encoded_size_of());

        Self {
            event_count: 1,
            events_byte_size: event.size_of(),
            grouped_events_byte_size: size,
            event_log_metadata: None,
        }
    }

    pub fn from_event_with_event_log<E>(event: &E) -> Self
    where
        E: ByteSizeOf + GetEventCountTags + EstimatedJsonEncodedSizeOf + EventWithEventLog,
    {
        let mut builder = Self::from_event(event);
        builder.event_log_metadata = Some(event.compute_event_log());
        builder
    }

    pub fn new_with_event_log(
        event_count: usize,
        events_byte_size: usize,
        grouped_events_byte_size: GroupedCountByteSize,
        event_log_metadata: Option<VectorSinkDeliveryEvent>,
    ) -> Self {
        Self {
            event_count,
            events_byte_size,
            grouped_events_byte_size,
            event_log_metadata,
        }
    }

    pub fn new(
        event_count: usize,
        events_byte_size: usize,
        grouped_events_byte_size: GroupedCountByteSize,
    ) -> Self {
        Self {
            event_count,
            events_byte_size,
            grouped_events_byte_size,
            event_log_metadata: None,
        }
    }

    pub fn track_event<E>(&mut self, event: E)
    where
        E: ByteSizeOf + GetEventCountTags + EstimatedJsonEncodedSizeOf,
    {
        self.event_count += 1;
        self.events_byte_size += event.size_of();
        let json_size = event.estimated_json_encoded_size_of();
        self.grouped_events_byte_size.add_event(&event, json_size);
    }

    /// Builds the [`RequestMetadata`] with the given size.
    /// This is used when there is no encoder in the process to provide an `EncodeResult`
    pub fn with_request_size(&self, size: NonZeroUsize) -> RequestMetadata {
        let size = size.get();

        RequestMetadata::new_with_event_log(
            self.event_count,
            self.events_byte_size,
            size,
            size,
            self.grouped_events_byte_size.clone(),
            self.event_log_metadata
                .clone()
                .unwrap_or(VectorSinkDeliveryEvent::new()),
        )
    }

    /// Builds the [`RequestMetadata`] from the results of encoding.
    /// `EncodeResult` provides us with the byte size before and after compression
    /// and the json size of the events after transforming (dropping unwanted fields) but
    /// before encoding.
    pub fn build<T>(&self, result: &EncodeResult<T>) -> RequestMetadata {
        RequestMetadata::new_with_event_log(
            self.event_count,
            self.events_byte_size,
            result.uncompressed_byte_size,
            result
                .compressed_byte_size
                .unwrap_or(result.uncompressed_byte_size),
            // Building from an encoded result, we take the json size from the encoded since that has the size
            // after transforming the event.
            result.transformed_json_size.clone(),
            self.event_log_metadata.clone().unwrap_or_default(),
        )
    }
}
