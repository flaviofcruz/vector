use std::net::IpAddr;
use std::time::Duration;

use metrics::{counter, gauge, histogram};
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

/// Emitted each time a request is routed to the fallback ClusterIP endpoint
/// because all headless pod IPs have been removed from the active set.
///
/// A non-zero rate of this metric means the headless pool is fully exhausted.
/// Alert on `clickhouse_headless_fallback_total` increasing to detect outages.
#[derive(Debug, NamedInternalEvent)]
pub struct ClickhouseHeadlessFallbackRouted {
    pub active_endpoints: usize,
}

impl InternalEvent for ClickhouseHeadlessFallbackRouted {
    fn emit(self) {
        warn!(
            message = "All headless endpoints unavailable, routing to fallback ClusterIP service.",
            active_endpoints = self.active_endpoints,
        );
        counter!("clickhouse_headless_fallback_total").increment(1);
    }
}

/// Emitted each time Tower's P2C buffer returns `Pending` during `poll_ready`,
/// meaning all buffer slots are occupied and the next dispatch must wait.
///
/// Rising `clickhouse_headless_p2c_buffer_full_total` indicates the buffer
/// bound is too small for the current concurrency + retry load.
#[derive(Debug, NamedInternalEvent)]
pub struct ClickhouseHeadlessP2cBufferFull;

impl InternalEvent for ClickhouseHeadlessP2cBufferFull {
    fn emit(self) {
        counter!("clickhouse_headless_p2c_buffer_full_total").increment(1);
    }
}

/// Emitted when a ClickHouse pod IP is removed from the active P2C pool due to
/// a connection failure or request timeout.
///
/// `clickhouse_headless_endpoint_removed_total` is a diagnostic counter for
/// tracking pod-level churn. Spikes indicate instability in the ClickHouse cluster.
#[derive(Debug, NamedInternalEvent)]
pub struct ClickhouseHeadlessEndpointRemoved {
    pub ip: IpAddr,
    pub reason: String,
    pub active_endpoints: usize,
}

impl InternalEvent for ClickhouseHeadlessEndpointRemoved {
    fn emit(self) {
        warn!(
            message = "Removing failed ClickHouse endpoint.",
            ip = %self.ip,
            reason = %self.reason,
            active_endpoints = self.active_endpoints,
        );
        counter!("clickhouse_headless_endpoint_removed_total").increment(1);
        gauge!("clickhouse_headless_active_endpoints").set(self.active_endpoints as f64);
    }
}

/// Emitted after each DNS reconciliation cycle (scheduled or immediate).
///
/// `clickhouse_headless_active_endpoints` gauge shows how many pod IPs are
/// currently in the P2C pool — use this to detect pool shrinkage or recovery.
/// `clickhouse_headless_dns_refresh_total{status="failure"}` rising indicates
/// DNS is unhealthy.
#[derive(Debug, NamedInternalEvent)]
pub struct ClickhouseHeadlessDnsRefreshed {
    pub success: bool,
    pub active_endpoints: usize,
}

impl InternalEvent for ClickhouseHeadlessDnsRefreshed {
    fn emit(self) {
        let status = if self.success { "success" } else { "failure" };
        counter!(
            "clickhouse_headless_dns_refresh_total",
            "status" => status,
        )
        .increment(1);
        if self.success {
            gauge!("clickhouse_headless_active_endpoints").set(self.active_endpoints as f64);
        }
    }
}

/// Emitted when the direct-sink primary endpoint (the `clickhouse-proxy`)
/// exhausts its retries and the request is failed over to the fallback endpoint
/// (the direct write service).
///
/// `clickhouse_direct_fallback_routed_total` rising means the proxy is degraded
/// and writes are being served directly — alert on it so proxy issues are
/// visible rather than silently absorbed.
#[derive(Debug, NamedInternalEvent)]
pub struct ClickhouseDirectFallbackRouted;

impl InternalEvent for ClickhouseDirectFallbackRouted {
    fn emit(self) {
        warn!(message = "ClickHouse primary exhausted retries; failing over to fallback.");
        counter!("clickhouse_direct_fallback_routed_total").increment(1);
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

    #[test]
    fn direct_fallback_routed_emits_counter() {
        trace_init();

        emit!(ClickhouseDirectFallbackRouted);

        let metrics = Controller::get().unwrap().capture_metrics();
        let counter = metrics
            .iter()
            .find(|m| m.name() == "clickhouse_direct_fallback_routed_total")
            .expect("clickhouse_direct_fallback_routed_total not found");
        assert!(matches!(counter.value(), MetricValue::Counter { value } if *value >= 1.0));
    }
}
