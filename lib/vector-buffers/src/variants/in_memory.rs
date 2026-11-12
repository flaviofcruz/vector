use std::error::Error;

use async_trait::async_trait;

use crate::{
    Bufferable,
    buffer_usage_data::BufferUsageHandle,
    config::MemoryBufferSize,
    topology::{
        builder::IntoBuffer,
        channel::{ReceiverAdapter, SenderAdapter, limited},
    },
};

pub struct MemoryBuffer {
    capacity: MemoryBufferSize,
}

impl MemoryBuffer {
    pub fn new(capacity: MemoryBufferSize) -> Self {
        MemoryBuffer { capacity }
    }

    #[cfg(test)]
    pub fn with_max_events(n: std::num::NonZeroUsize) -> Self {
        Self {
            capacity: MemoryBufferSize::MaxEvents(n),
        }
    }
}

#[async_trait]
impl<T> IntoBuffer<T> for MemoryBuffer
where
    T: Bufferable,
{
    async fn into_buffer_parts(
        self: Box<Self>,
        usage_handle: BufferUsageHandle,
    ) -> Result<(SenderAdapter<T>, ReceiverAdapter<T>), Box<dyn Error + Send + Sync>> {
        let (max_bytes, max_events) = match self.capacity {
            MemoryBufferSize::MaxEvents(max_events) => (None, Some(max_events.get())),
            MemoryBufferSize::MaxSize(max_size) => (Some(max_size.get() as u64), None),
        };

        usage_handle.set_buffer_limits(max_bytes, max_events);

        let (tx, rx) = limited(self.capacity, None, None);
        Ok((tx.into(), rx.into()))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use tracing::Span;
    use vector_common::internal_event::InternalEvent;

    use super::MemoryBuffer;
    use crate::{
        buffer_usage_data::{BufferUsage, BufferUsageHandle},
        config::MemoryBufferSize,
        internal_events::BufferCreated,
        topology::{builder::IntoBuffer, test_util::Sample},
    };

    async fn limits_for(capacity: MemoryBufferSize) -> BufferUsageHandle {
        let handle = BufferUsage::from_span(Span::none()).add_stage(0);
        let buffer: Box<dyn IntoBuffer<Sample>> = Box::new(MemoryBuffer::new(capacity));
        buffer
            .into_buffer_parts(handle.clone())
            .await
            .expect("memory buffer should build");
        handle
    }

    /// Emits `BufferCreated` exactly as the usage reporter does and returns the gauges it set.
    fn emitted_limit_gauges(handle: &BufferUsageHandle) -> Vec<(String, f64)> {
        let snapshot = handle.snapshot();
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            BufferCreated {
                buffer_id: "sink".to_string(),
                idx: 0,
                max_size_events: snapshot.max_size_events,
                max_size_bytes: snapshot.max_size_bytes,
            }
            .emit();
        });
        let mut gauges: Vec<_> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(key, _, _, value)| match value {
                DebugValue::Gauge(v) => (key.key().name().to_string(), v.into_inner()),
                other => panic!("unexpected metric value {other:?}"),
            })
            .collect();
        gauges.sort_by(|a, b| a.0.cmp(&b.0));
        gauges
    }

    #[tokio::test]
    async fn max_size_memory_buffer_reports_byte_limit() {
        let handle = limits_for(MemoryBufferSize::MaxSize(
            NonZeroUsize::new(33_554_432).unwrap(),
        ))
        .await;

        let snapshot = handle.snapshot();
        assert_eq!(snapshot.max_size_bytes, 33_554_432);
        assert_eq!(snapshot.max_size_events, 0);
        assert_eq!(
            emitted_limit_gauges(&handle),
            vec![
                ("buffer_max_byte_size".to_string(), 33_554_432.0),
                ("buffer_max_size_bytes".to_string(), 33_554_432.0),
            ]
        );
    }

    #[tokio::test]
    async fn max_events_memory_buffer_reports_event_limit() {
        let handle = limits_for(MemoryBufferSize::MaxEvents(NonZeroUsize::new(500).unwrap())).await;

        let snapshot = handle.snapshot();
        assert_eq!(snapshot.max_size_bytes, 0);
        assert_eq!(snapshot.max_size_events, 500);
        assert_eq!(
            emitted_limit_gauges(&handle),
            vec![
                ("buffer_max_event_size".to_string(), 500.0),
                ("buffer_max_size_events".to_string(), 500.0),
            ]
        );
    }
}
