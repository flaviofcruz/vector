use std::time::Duration;

use metrics::{counter, histogram};
use vector_lib::NamedInternalEvent;
use vector_lib::internal_event::InternalEvent;

/// Emitted when a ClickHouse batch is assembled and ready to be sent.
#[derive(Debug, NamedInternalEvent)]
pub struct ClickhouseBatchFlushed {
    pub event_count: usize,
    /// In-memory allocation size (`ByteSizeOf::size_of`), the same metric the
    /// batcher compares against `max_bytes` to decide when to flush.
    pub in_memory_byte_size: usize,
}

impl InternalEvent for ClickhouseBatchFlushed {
    fn emit(self) {
        trace!(
            message = "ClickHouse batch flushed.",
            event_count = %self.event_count,
            in_memory_byte_size = %self.in_memory_byte_size,
        );
        counter!("clickhouse_batches_flushed_total").increment(1);
        histogram!("clickhouse_batch_event_count").record(self.event_count as f64);
        // This is the size the batcher uses to enforce its max_bytes limit.
        histogram!("clickhouse_batch_in_memory_byte_size").record(self.in_memory_byte_size as f64);
    }
}

/// Emitted after a ClickHouse insert HTTP request completes.
#[derive(Debug, NamedInternalEvent)]
pub struct ClickhouseInsertCompleted {
    pub latency: Duration,
    pub compressed_byte_size: usize,
    pub status: &'static str,
}

impl InternalEvent for ClickhouseInsertCompleted {
    fn emit(self) {
        trace!(
            message = "ClickHouse insert completed.",
            latency_ms = %self.latency.as_millis(),
            compressed_byte_size = %self.compressed_byte_size,
            status = %self.status,
        );
        histogram!(
            "clickhouse_insert_duration_seconds",
            "status" => self.status.to_owned(),
        )
        .record(self.latency.as_secs_f64());
        histogram!("clickhouse_batch_compressed_byte_size")
            .record(self.compressed_byte_size as f64);
    }
}

/// Emitted to record the interval between consecutive batch flushes for a partition.
#[derive(Debug, NamedInternalEvent)]
pub struct ClickhouseBatchInterval {
    pub interval: Duration,
}

impl InternalEvent for ClickhouseBatchInterval {
    fn emit(self) {
        trace!(
            message = "Time since last ClickHouse batch flush.",
            interval_secs = %self.interval.as_secs_f64(),
        );
        histogram!("clickhouse_batch_interval_seconds").record(self.interval.as_secs_f64());
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use vector_lib::event::MetricValue;
    use vector_lib::metrics::Controller;

    use crate::test_util::trace_init;

    use super::*;

    #[test]
    fn batch_flushed_emits_counter_and_histograms() {
        trace_init();

        emit!(ClickhouseBatchFlushed {
            event_count: 42,
            in_memory_byte_size: 1024,
        });

        let metrics = Controller::get().unwrap().capture_metrics();

        let counter = metrics
            .iter()
            .find(|m| m.name() == "clickhouse_batches_flushed_total")
            .expect("clickhouse_batches_flushed_total not found");
        assert!(matches!(counter.value(), MetricValue::Counter { value } if *value >= 1.0));

        let event_count = metrics
            .iter()
            .find(|m| m.name() == "clickhouse_batch_event_count")
            .expect("clickhouse_batch_event_count not found");
        assert!(matches!(
            event_count.value(),
            MetricValue::AggregatedHistogram { .. }
        ));

        let byte_size = metrics
            .iter()
            .find(|m| m.name() == "clickhouse_batch_in_memory_byte_size")
            .expect("clickhouse_batch_in_memory_byte_size not found");
        assert!(matches!(
            byte_size.value(),
            MetricValue::AggregatedHistogram { .. }
        ));
    }

    #[test]
    fn insert_completed_emits_latency_and_compressed_size() {
        trace_init();

        emit!(ClickhouseInsertCompleted {
            latency: Duration::from_millis(150),
            compressed_byte_size: 512,
            status: "success",
        });

        let metrics = Controller::get().unwrap().capture_metrics();

        let latency = metrics
            .iter()
            .find(|m| m.name() == "clickhouse_insert_duration_seconds")
            .expect("clickhouse_insert_duration_seconds not found");
        assert!(matches!(
            latency.value(),
            MetricValue::AggregatedHistogram { .. }
        ));

        let compressed = metrics
            .iter()
            .find(|m| m.name() == "clickhouse_batch_compressed_byte_size")
            .expect("clickhouse_batch_compressed_byte_size not found");
        assert!(matches!(
            compressed.value(),
            MetricValue::AggregatedHistogram { .. }
        ));
    }

    #[test]
    fn batch_interval_emits_histogram() {
        trace_init();

        emit!(ClickhouseBatchInterval {
            interval: Duration::from_millis(980),
        });

        let metrics = Controller::get().unwrap().capture_metrics();

        let interval = metrics
            .iter()
            .find(|m| m.name() == "clickhouse_batch_interval_seconds")
            .expect("clickhouse_batch_interval_seconds not found");
        assert!(matches!(
            interval.value(),
            MetricValue::AggregatedHistogram { .. }
        ));
    }
}
