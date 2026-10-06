use std::{hash::Hash, num::NonZeroU32, pin::Pin, time::Duration};

use async_stream::stream;
use futures::{Stream, StreamExt};
use governor::{Quota, clock};
use metrics::Counter;
use snafu::Snafu;
use vector_common::finalization::EventStatus;
use vector_common::internal_event::vector_event::delivery_event::DeliveryEventStage;

use super::{
    config::{ThrottleConfig, ThrottleInternalMetricsConfig},
    rate_limiter::RateLimiterRunner,
};
use crate::{
    conditions::Condition,
    config::TransformContext,
    event::Event,
    internal_events::{TemplateRenderingError, ThrottleEventDiscarded},
    template::Template,
    transforms::TaskTransform,
};

#[derive(Clone)]
pub struct Throttle<C: clock::Clock<Instant = I>, I: clock::Reference> {
    pub quota: Quota,
    pub flush_keys_interval: Duration,
    key_field: Option<Template>,
    exclude: Option<Condition>,
    pub clock: C,
    internal_metrics: ThrottleInternalMetricsConfig,
    throttled_event_status: EventStatus,
    delivery_path_intentional_drop: Option<(Vec<String>, String)>,
    pub cpu_ns: Option<Counter>,
}

impl<C, I> Throttle<C, I>
where
    C: clock::Clock<Instant = I> + Clone + Send + Sync + 'static,
    I: clock::Reference,
{
    pub fn new(
        config: &ThrottleConfig,
        context: &TransformContext,
        clock: C,
    ) -> crate::Result<Self> {
        let flush_keys_interval = config.window_secs;

        let threshold = match NonZeroU32::new(config.threshold) {
            Some(threshold) => threshold,
            None => return Err(Box::new(ConfigError::NonZero)),
        };

        let quota = match Quota::with_period(Duration::from_secs_f64(
            flush_keys_interval.as_secs_f64() / f64::from(threshold.get()),
        )) {
            Some(quota) => quota.allow_burst(threshold),
            None => return Err(Box::new(ConfigError::NonZero)),
        };
        let exclude = config
            .exclude
            .as_ref()
            .map(|condition| condition.build(&context.enrichment_tables, &context.metrics_storage))
            .transpose()?;

        let throttled_event_status = match config.throttled_event_status.as_str() {
            "dropped" => EventStatus::Dropped,
            "errored" => EventStatus::Errored,
            "rejected" => EventStatus::Rejected,
            _ => {
                return Err(Box::new(ConfigError::NonZero));
            }
        };
        let delivery_path_intentional_drop = config
            .delivery_path_intentional_drop
            .as_ref()
            .map(|intentional_drop| {
                if intentional_drop.paths.is_empty()
                    || intentional_drop.paths.iter().any(String::is_empty)
                    || intentional_drop.reason.is_empty()
                {
                    Err(Box::new(ConfigError::InvalidDeliveryPathIntentionalDrop))
                } else {
                    Ok((
                        intentional_drop.paths.clone(),
                        intentional_drop.reason.clone(),
                    ))
                }
            })
            .transpose()?;

        Ok(Self {
            quota,
            clock,
            flush_keys_interval,
            key_field: config.key_field.clone(),
            exclude,
            internal_metrics: config.internal_metrics.clone(),
            throttled_event_status,
            delivery_path_intentional_drop,
            cpu_ns: context.cpu_ns.clone(),
        })
    }

    #[must_use]
    pub fn start_rate_limiter<K>(&self) -> RateLimiterRunner<K, C>
    where
        K: Hash + Eq + Clone + Send + Sync + 'static,
    {
        RateLimiterRunner::start(self)
    }

    pub fn emit_event_discarded(&self, key: String) {
        emit!(ThrottleEventDiscarded {
            key,
            emit_events_discarded_per_key: self.internal_metrics.emit_events_discarded_per_key
        });
    }

    fn emit_delivery_path_intentional_drop(&self, event: &mut Event) {
        let Some((paths, reason)) = &self.delivery_path_intentional_drop else {
            return;
        };
        if let Some(accounting) = event.metadata_mut().delivery_path_accounting_mut() {
            for path in paths {
                for count in accounting.take_path(path) {
                    count.emit(DeliveryEventStage::IntentionalDrop { reason });
                }
            }
        }
    }
}

impl<C, I> TaskTransform<Event> for Throttle<C, I>
where
    C: clock::Clock<Instant = I> + Clone + Send + Sync + 'static,
    I: clock::Reference + Send + 'static,
{
    fn transform(
        self: Box<Self>,
        mut input_rx: Pin<Box<dyn Stream<Item = Event> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Event> + Send>>
    where
        Self: 'static,
    {
        let limiter = self.start_rate_limiter();

        Box::pin(stream! {
            while let Some(event) = input_rx.next().await {
                let (throttle, mut event) = match self.exclude.as_ref() {
                    Some(condition) => {
                        let (result, event) = condition.check(event);
                        (!result, event)
                    },
                    _ => (true, event)
                };
                let output = if throttle {
                    let key = self.key_field.as_ref().and_then(|t| {
                        t.render_string(&event)
                            .map_err(|error| {
                                emit!(TemplateRenderingError {
                                    error,
                                    field: Some("key_field"),
                                    drop_event: false,
                                })
                            })
                            .ok()
                    });

                    if limiter.check_key(&key) {
                        Some(event)
                    } else {
                        self.emit_event_discarded(key.unwrap_or_else(|| "None".to_string()));
                        self.emit_delivery_path_intentional_drop(&mut event);
                        event.metadata().update_status(self.throttled_event_status);
                        None
                    }
                } else {
                    Some(event)
                };
                if let Some(event) = output {
                    yield event;
                }
            }
        })
    }
}

#[derive(Debug, Snafu)]
pub enum ConfigError {
    #[snafu(display("`threshold`, and `window_secs` must be non-zero"))]
    NonZero,
    #[snafu(display("`delivery_path_intentional_drop` requires non-empty paths and reason"))]
    InvalidDeliveryPathIntentionalDrop,
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, task::Poll};

    use futures::SinkExt;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use vector_common::internal_event::vector_event::delivery_path::{
        DeliveryPathAccounting, DeliverySourceLabels,
    };

    use super::*;
    use crate::transforms::Transform;
    use crate::{
        event::{Event, LogEvent},
        test_util::components::assert_transform_compliance,
        transforms::test::create_topology,
        transforms::throttle::config::{
            ThrottleDeliveryPathIntentionalDropConfig, default_throttled_event_status,
        },
    };
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    #[tokio::test]
    async fn delivery_path_intentional_drop_counts_only_actual_throttle_drops() {
        for paths in [
            vec!["online"],
            vec!["blob", "online", "online", "not-required"],
            vec![],
        ] {
            assert_throttle_delivery_paths(paths);
        }
    }

    fn assert_throttle_delivery_paths(paths: Vec<&str>) {
        let config = ThrottleConfig {
            threshold: 1,
            window_secs: Duration::from_secs(60),
            throttled_event_status: default_throttled_event_status(),
            delivery_path_intentional_drop: (!paths.is_empty()).then(|| {
                ThrottleDeliveryPathIntentionalDropConfig {
                    paths: paths.iter().map(|path| (*path).to_owned()).collect(),
                    reason: "policy_rate_limit".to_owned(),
                }
            }),
            ..Default::default()
        };
        let throttle = Throttle::new(
            &config,
            &TransformContext::default(),
            clock::FakeRelativeClock::default(),
        )
        .expect("valid throttle");
        let mut event = Event::Log(LogEvent::from("message"));
        event
            .metadata_mut()
            .set_delivery_path_accounting(Some(DeliveryPathAccounting::at_read(
                DeliverySourceLabels {
                    process_generation_id: "source-process".to_owned(),
                    time_parity: 3_600_000,
                    topic: "service-log".to_owned(),
                    delivery_method: "file".to_owned(),
                    service_system: "service".to_owned(),
                    source_pod_id: "pod".to_owned(),
                    workspace_id: None,
                },
                ["blob".to_owned(), "online".to_owned()],
            )));

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let expected_metadata = event.metadata().delivery_path_accounting().cloned();
        let output = metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(
                Box::new(throttle)
                    .transform(Box::pin(futures::stream::iter(vec![event.clone(), event])))
                    .collect::<Vec<_>>(),
            )
        });
        assert_eq!(
            output.len(),
            1,
            "the second event must actually be throttled"
        );
        assert_eq!(
            output[0].metadata().delivery_path_accounting(),
            expected_metadata.as_ref()
        );
        let counters = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, _, _, _)| key.key().name() == "delivery_events_total")
            .collect::<Vec<_>>();
        let expected_paths = ["blob", "online"]
            .into_iter()
            .filter(|path| paths.contains(path))
            .collect::<Vec<_>>();
        assert_eq!(counters.len(), 2 * expected_paths.len());
        for (key, _, _, value) in counters {
            let labels = key
                .key()
                .labels()
                .map(|label| (label.key(), label.value()))
                .collect::<HashMap<_, _>>();
            assert_eq!(value, DebugValue::Counter(1));
            assert!(expected_paths.contains(&labels["delivery_path"]));
            assert_eq!(
                labels["delivery_event_type"],
                "VECTOR_DELIVERY_INTENTIONAL_DROP"
            );
            assert_eq!(labels["reason"], "policy_rate_limit");
        }
    }

    #[tokio::test]
    async fn throttle_events() {
        let clock = clock::FakeRelativeClock::default();
        let config = toml::from_str::<ThrottleConfig>(
            r"
threshold = 2
window_secs = 5
",
        )
        .unwrap();

        let throttle = Throttle::new(&config, &TransformContext::default(), clock.clone())
            .map(Transform::event_task)
            .unwrap();

        let throttle = throttle.into_task();

        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        let mut out_stream = throttle.transform_events(Box::pin(rx));

        // tokio interval is always immediately ready, so we poll once to make sure
        // we trip it/set the interval in the future
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        tx.send(LogEvent::default().into()).await.unwrap();
        tx.send(LogEvent::default().into()).await.unwrap();

        let mut count = 0_u8;
        while count < 2 {
            match out_stream.next().await {
                Some(_event) => {
                    count += 1;
                }
                _ => {
                    panic!("Unexpectedly received None in output stream");
                }
            }
        }
        assert_eq!(2, count);

        clock.advance(Duration::from_secs(2));

        tx.send(LogEvent::default().into()).await.unwrap();

        // We should be back to pending, having the second event dropped
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        clock.advance(Duration::from_secs(3));

        tx.send(LogEvent::default().into()).await.unwrap();

        // The rate limiter should now be refreshed and allow an additional event through
        match out_stream.next().await {
            Some(_event) => {}
            _ => {
                panic!("Unexpectedly received None in output stream");
            }
        }

        // We should be back to pending, having nothing waiting for us
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        tx.disconnect();

        // And still nothing there
        assert_eq!(Poll::Ready(None), futures::poll!(out_stream.next()));
    }

    #[tokio::test]
    async fn throttle_exclude() {
        let clock = clock::FakeRelativeClock::default();
        let config = toml::from_str::<ThrottleConfig>(
            r#"
threshold = 2
window_secs = 5
exclude = """
exists(.special)
"""
"#,
        )
        .unwrap();

        let throttle = Throttle::new(&config, &TransformContext::default(), clock.clone())
            .map(Transform::event_task)
            .unwrap();

        let throttle = throttle.into_task();

        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        let mut out_stream = throttle.transform_events(Box::pin(rx));

        // tokio interval is always immediately ready, so we poll once to make sure
        // we trip it/set the interval in the future
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        tx.send(LogEvent::default().into()).await.unwrap();
        tx.send(LogEvent::default().into()).await.unwrap();

        let mut count = 0_u8;
        while count < 2 {
            match out_stream.next().await {
                Some(_event) => {
                    count += 1;
                }
                _ => {
                    panic!("Unexpectedly received None in output stream");
                }
            }
        }
        assert_eq!(2, count);

        clock.advance(Duration::from_secs(2));

        tx.send(LogEvent::default().into()).await.unwrap();

        // We should be back to pending, having the second event dropped
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        let mut special_log = LogEvent::default();
        special_log.insert("special", "true");
        tx.send(special_log.into()).await.unwrap();
        // The rate limiter should allow this log through regardless of current limit
        match out_stream.next().await {
            Some(_event) => {}
            _ => {
                panic!("Unexpectedly received None in output stream");
            }
        }

        clock.advance(Duration::from_secs(3));

        tx.send(LogEvent::default().into()).await.unwrap();

        // The rate limiter should now be refreshed and allow an additional event through
        match out_stream.next().await {
            Some(_event) => {}
            _ => {
                panic!("Unexpectedly received None in output stream");
            }
        }

        // We should be back to pending, having nothing waiting for us
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        tx.disconnect();

        // And still nothing there
        assert_eq!(Poll::Ready(None), futures::poll!(out_stream.next()));
    }

    #[tokio::test]
    async fn throttle_buckets() {
        let clock = clock::FakeRelativeClock::default();
        let config = toml::from_str::<ThrottleConfig>(
            r#"
threshold = 1
window_secs = 5
key_field = "{{ bucket }}"
"#,
        )
        .unwrap();

        let throttle = Throttle::new(&config, &TransformContext::default(), clock.clone())
            .map(Transform::event_task)
            .unwrap();

        let throttle = throttle.into_task();

        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        let mut out_stream = throttle.transform_events(Box::pin(rx));

        // tokio interval is always immediately ready, so we poll once to make sure
        // we trip it/set the interval in the future
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        let mut log_a = LogEvent::default();
        log_a.insert("bucket", "a");
        let mut log_b = LogEvent::default();
        log_b.insert("bucket", "b");
        tx.send(log_a.into()).await.unwrap();
        tx.send(log_b.into()).await.unwrap();

        let mut count = 0_u8;
        while count < 2 {
            match out_stream.next().await {
                Some(_event) => {
                    count += 1;
                }
                _ => {
                    panic!("Unexpectedly received None in output stream");
                }
            }
        }
        assert_eq!(2, count);

        // We should be back to pending, having nothing waiting for us
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        tx.disconnect();

        // And still nothing there
        assert_eq!(Poll::Ready(None), futures::poll!(out_stream.next()));
    }

    #[tokio::test]
    async fn emits_internal_events() {
        assert_transform_compliance(async move {
            let config = ThrottleConfig {
                threshold: 1,
                window_secs: Duration::from_secs_f64(1.0),
                key_field: None,
                exclude: None,
                internal_metrics: Default::default(),
                throttled_event_status: default_throttled_event_status(),
                delivery_path_intentional_drop: None,
            };
            let (tx, rx) = mpsc::channel(1);
            let (topology, mut out) = create_topology(ReceiverStream::new(rx), config).await;

            let log = LogEvent::from("hello world");
            tx.send(log.into()).await.unwrap();

            _ = out.recv().await;

            drop(tx);
            topology.stop().await;
            assert_eq!(out.recv().await, None);
        })
        .await
    }
}
