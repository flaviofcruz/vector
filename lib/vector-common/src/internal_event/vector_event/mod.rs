pub mod delivery_event;
pub mod file_send_event;

use delivery_event::{VectorSinkDeliveryEvent, combine_sink_delivery_events};
use file_send_event::{FileEventMetadata, VectorFileSendEvent, combine_file_send_events};
use std::ops::Add;

#[derive(Clone, Debug, Default)]
pub struct VectorSinkEventMetadata {
    pub delivery_event: VectorSinkDeliveryEvent,
    // Optional as not all sinks will need this
    pub file_send_event: Option<VectorFileSendEvent>,
}

impl VectorSinkEventMetadata {
    pub fn new() -> Self {
        Self {
            delivery_event: VectorSinkDeliveryEvent::new(),
            file_send_event: None,
        }
    }

    // Pipe in file metadata into event if file event exists
    pub fn update_file_metadata(&mut self, file_metadata: FileEventMetadata) {
        if let Some(file_send_event) = &mut self.file_send_event {
            file_send_event.file_metadata = file_metadata;
        }
    }
}

// Combines both delivery and file send events
// NOTE: This assumes that all the extra file send metadata is the same (filename, etc.)
pub fn combine_sink_event_metadata(
    events: Vec<VectorSinkEventMetadata>,
) -> VectorSinkEventMetadata {
    let mut combined = VectorSinkEventMetadata::new();
    combined.delivery_event = combine_sink_delivery_events(
        events
            .iter()
            .map(|event| event.delivery_event.clone())
            .collect(),
    );
    combined.file_send_event = Some(combine_file_send_events(
        events
            .iter()
            .map(|event| {
                event
                    .file_send_event
                    .clone()
                    .unwrap_or(VectorFileSendEvent::new())
            })
            .collect(),
    ));
    combined
}

impl Add<VectorSinkEventMetadata> for VectorSinkEventMetadata {
    type Output = VectorSinkEventMetadata;

    fn add(self, other: VectorSinkEventMetadata) -> Self::Output {
        combine_sink_event_metadata(vec![self, other])
    }
}

pub trait EventWithEventLog {
    fn compute_event_log(&self) -> VectorSinkEventMetadata;
    fn compute_event_log_with_file_event(
        &self,
        file_metadata: FileEventMetadata,
    ) -> VectorSinkEventMetadata;
}
