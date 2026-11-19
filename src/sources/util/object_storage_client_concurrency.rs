//! Periodic refresh of `object_storage_client_concurrency`, the configured worker ceiling.

use std::time::Duration;

use futures::FutureExt;
use tokio::{pin, select};

use crate::{internal_events::ObjectStorageClientConcurrency, shutdown::ShutdownSignal};

// Well under Prometheus' five-minute staleness window, so an idle source keeps a fresh value.
const REPORT_INTERVAL: Duration = Duration::from_secs(15);

/// Re-emits the gauge on a timer until shutdown, independent of receives and the depth poller.
pub async fn report_client_concurrency(client_concurrency: usize, shutdown: ShutdownSignal) {
    let shutdown = shutdown.fuse();
    pin!(shutdown);

    let mut ticker = tokio::time::interval(REPORT_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        select! {
            _ = &mut shutdown => break,
            _ = ticker.tick() => {
                emit!(ObjectStorageClientConcurrency { client_concurrency });
            }
        }
    }
}

/// Latest `object_storage_client_concurrency` value recorded through `snapshotter`.
#[cfg(test)]
pub(crate) fn client_concurrency_gauge(
    snapshotter: &metrics_util::debugging::Snapshotter,
) -> Option<f64> {
    use metrics_util::debugging::DebugValue;

    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .find_map(|(key, _, _, value)| match (key.key().name(), value) {
            ("object_storage_client_concurrency", DebugValue::Gauge(limit)) => Some(limit.0),
            _ => None,
        })
}

#[cfg(test)]
mod tests {
    use metrics_util::debugging::DebuggingRecorder;
    use tokio::time::advance;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn reporter_emits_ceiling_and_refreshes_each_interval() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _local = metrics::set_default_local_recorder(&recorder);

        let reporter = tokio::spawn(report_client_concurrency(4, ShutdownSignal::noop()));
        tokio::task::yield_now().await;
        assert_eq!(client_concurrency_gauge(&snapshotter), Some(4.0));

        // Overwrite the gauge, then let the next tick restore it without any receive activity.
        metrics::gauge!("object_storage_client_concurrency").set(0.0);
        assert_eq!(client_concurrency_gauge(&snapshotter), Some(0.0));
        advance(REPORT_INTERVAL).await;
        tokio::task::yield_now().await;
        assert_eq!(client_concurrency_gauge(&snapshotter), Some(4.0));
        reporter.abort();
    }
}
