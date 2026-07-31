use chrono::Utc;
use futures::{StreamExt, stream};
use vector_lib::{
    codecs::BytesDeserializerConfig,
    config::{LegacyKey, LogNamespace, log_schema},
    configurable::configurable_component,
    lookup::{OwnedValuePath, lookup_v2::OptionalValuePath, owned_value_path, path},
    schema::Definition,
};
use vrl::value::Kind;

use crate::{
    SourceSender,
    config::{DataType, SourceConfig, SourceContext, SourceOutput},
    event::{EstimatedJsonEncodedSizeOf, Event},
    internal_events::{InternalLogsBytesReceived, InternalLogsEventsReceived, StreamClosedError},
    shutdown::ShutdownSignal,
    trace::TraceSubscription,
};

/// Idle window the shutdown drain waits for a late event before concluding it is
/// done. Small relative to the wave-2 shutdown budget, so it does not slow shutdown.
const DRAIN_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(250);

/// Configuration for the `internal_logs` source.
#[configurable_component(source(
    "internal_logs",
    "Expose internal log messages emitted by the running Vector instance."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct InternalLogsConfig {
    /// Overrides the name of the log field used to add the current hostname to each event.
    ///
    /// By default, the [global `log_schema.host_key` option][global_host_key] is used.
    ///
    /// Set to `""` to suppress this key.
    ///
    /// [global_host_key]: https://vector.dev/docs/reference/configuration/global-options/#log_schema.host_key
    host_key: Option<OptionalValuePath>,

    /// Overrides the name of the log field used to add the current process ID to each event.
    ///
    /// By default, `"pid"` is used.
    ///
    /// Set to `""` to suppress this key.
    #[serde(default = "default_pid_key")]
    pid_key: OptionalValuePath,

    /// The namespace to use for logs. This overrides the global setting.
    #[configurable(metadata(docs::hidden))]
    #[serde(default)]
    log_namespace: Option<bool>,
}

fn default_pid_key() -> OptionalValuePath {
    OptionalValuePath::from(owned_value_path!("pid"))
}

impl_generate_config_from_default!(InternalLogsConfig);

impl Default for InternalLogsConfig {
    fn default() -> InternalLogsConfig {
        InternalLogsConfig {
            host_key: None,
            pid_key: default_pid_key(),
            log_namespace: None,
        }
    }
}

impl InternalLogsConfig {
    /// Generates the `schema::Definition` for this component.
    fn schema_definition(&self, log_namespace: LogNamespace) -> Definition {
        let host_key = self
            .host_key
            .clone()
            .unwrap_or(log_schema().host_key().cloned().into())
            .path
            .map(LegacyKey::Overwrite);
        let pid_key = self.pid_key.clone().path.map(LegacyKey::Overwrite);

        // There is a global and per-source `log_namespace` config.
        // The source config overrides the global setting and is merged here.
        BytesDeserializerConfig
            .schema_definition(log_namespace)
            .with_standard_vector_source_metadata()
            .with_source_metadata(
                InternalLogsConfig::NAME,
                host_key,
                &owned_value_path!("host"),
                Kind::bytes().or_undefined(),
                Some("host"),
            )
            .with_source_metadata(
                InternalLogsConfig::NAME,
                pid_key,
                &owned_value_path!("pid"),
                Kind::integer(),
                None,
            )
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "internal_logs")]
impl SourceConfig for InternalLogsConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        let host_key = self
            .host_key
            .clone()
            .unwrap_or(log_schema().host_key().cloned().into())
            .path;
        let pid_key = self.pid_key.clone().path;

        let subscription = TraceSubscription::subscribe();

        let log_namespace = cx.log_namespace(self.log_namespace);

        Ok(Box::pin(run(
            host_key,
            pid_key,
            subscription,
            cx.out,
            cx.shutdown,
            log_namespace,
        )))
    }

    fn outputs(&self, global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        let schema_definition =
            self.schema_definition(global_log_namespace.merge(self.log_namespace));

        vec![SourceOutput::new_maybe_logs(
            DataType::Log,
            schema_definition,
        )]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }

    fn has_deferred_shutdown(&self) -> bool {
        true
    }
}

async fn run(
    host_key: Option<OwnedValuePath>,
    pid_key: Option<OwnedValuePath>,
    mut subscription: TraceSubscription,
    mut out: SourceSender,
    mut shutdown: ShutdownSignal,
    log_namespace: LogNamespace,
) -> Result<(), ()> {
    let hostname = crate::get_hostname();
    let pid = std::process::id();

    // Chain any log events that were captured during early buffering to the front,
    // and then continue with the normal stream of internal log events.
    let buffered_events = subscription.buffered_events().await;
    let mut rx =
        stream::iter(buffered_events.into_iter().flatten()).chain(subscription.into_stream());
    let mut draining = false;

    // Note: This loop, or anything called within it, MUST NOT generate
    // any logs that don't break the loop, as that could cause an
    // infinite loop since it receives all such logs.
    //
    // After `shutdown` fires, drain remaining events before exiting so
    // `VECTOR_PROCESS_COMPONENTS_CLOSED` isn't lost: it's emitted right before
    // wave 2 cancels this source, and reaches `rx` asynchronously (`info!` ->
    // tracing layer -> broadcast channel), so it can arrive just after the drain
    // begins. Await late arrivals with a short idle timeout rather than a single
    // non-blocking poll, which would exit the instant the channel is momentarily
    // empty and drop the event.
    loop {
        let next = if draining {
            match tokio::time::timeout(DRAIN_IDLE_TIMEOUT, rx.next()).await {
                Ok(item) => item,
                // Idle for the whole window: drain complete.
                Err(_) => break,
            }
        } else {
            tokio::select! {
                item = rx.next() => item,
                _ = &mut shutdown => {
                    draining = true;
                    continue;
                }
            }
        };
        let Some(mut log) = next else { break };

        // TODO: Should this actually be in memory size?
        let byte_size = log.estimated_json_encoded_size_of().get();
        let json_byte_size = log.estimated_json_encoded_size_of();
        // This event doesn't emit any log
        emit!(InternalLogsBytesReceived { byte_size });
        emit!(InternalLogsEventsReceived {
            count: 1,
            byte_size: json_byte_size,
        });

        if let Ok(hostname) = &hostname {
            let legacy_host_key = host_key.as_ref().map(LegacyKey::Overwrite);
            log_namespace.insert_source_metadata(
                InternalLogsConfig::NAME,
                &mut log,
                legacy_host_key,
                path!("host"),
                hostname.to_owned(),
            );
        }

        let legacy_pid_key = pid_key.as_ref().map(LegacyKey::Overwrite);
        log_namespace.insert_source_metadata(
            InternalLogsConfig::NAME,
            &mut log,
            legacy_pid_key,
            path!("pid"),
            pid,
        );

        log_namespace.insert_standard_vector_source_metadata(
            &mut log,
            InternalLogsConfig::NAME,
            Utc::now(),
        );

        if (out.send_event(Event::from(log)).await).is_err() {
            // this wont trigger any infinite loop considering it stops the component
            emit!(StreamClosedError { count: 1 });
            return Err(());
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use futures::Stream;
    use tokio::time::{Duration, sleep};
    use vector_lib::{event::Value, lookup::OwnedTargetPath};
    use vrl::value::kind::Collection;

    use serial_test::serial;

    use super::*;
    use crate::{
        config::ComponentKey,
        event::Event,
        test_util::{
            collect_ready,
            components::{SOURCE_TAGS, assert_source_compliance},
        },
        trace,
    };

    #[test]
    fn generates_config() {
        crate::test_util::test_generate_config::<InternalLogsConfig>();
    }

    // This test is fairly overloaded with different cases.
    //
    // Unfortunately, this can't be easily split out into separate test
    // cases because `consume_early_buffer` (called within the
    // `start_source` helper) panics when called more than once.
    #[tokio::test]
    #[serial]
    async fn receives_logs() {
        trace::init(false, false, "debug", 10);
        trace::reset_early_buffer();

        assert_source_compliance(&SOURCE_TAGS, run_test()).await;
    }

    async fn run_test() {
        let test_id: u8 = rand::random();
        let start = chrono::Utc::now();

        error!(message = "Before source started without span.", %test_id);

        let span = error_span!(
            "source",
            component_kind = "source",
            component_id = "foo",
            component_type = "internal_logs",
        );
        let _enter = span.enter();

        error!(message = "Before source started.", %test_id);

        let rx = start_source().await;

        error!(message = "After source started.", %test_id);

        {
            let nested_span = error_span!(
                "nested span",
                component_kind = "bar",
                component_new_field = "baz",
                component_numerical_field = 1,
                ignored_field = "foobarbaz",
            );
            let _enter = nested_span.enter();
            error!(message = "In a nested span.", %test_id);
        }

        sleep(Duration::from_millis(1)).await;
        let mut events = collect_ready(rx).await;
        let test_id = Value::from(test_id.to_string());
        events.retain(|event| event.as_log().get("test_id") == Some(&test_id));

        let end = chrono::Utc::now();

        assert_eq!(events.len(), 4);

        assert_eq!(
            events[0].as_log()["message"],
            "Before source started without span.".into()
        );
        assert_eq!(
            events[1].as_log()["message"],
            "Before source started.".into()
        );
        assert_eq!(
            events[2].as_log()["message"],
            "After source started.".into()
        );
        assert_eq!(events[3].as_log()["message"], "In a nested span.".into());

        for (i, event) in events.iter().enumerate() {
            let log = event.as_log();
            let timestamp = *log["timestamp"]
                .as_timestamp()
                .expect("timestamp isn't a timestamp");
            assert!(timestamp >= start);
            assert!(timestamp <= end);
            assert_eq!(log["metadata.kind"], "event".into());
            assert_eq!(log["metadata.level"], "ERROR".into());
            // The first log event occurs outside our custom span
            if i == 0 {
                assert!(log.get("vector.component_id").is_none());
                assert!(log.get("vector.component_kind").is_none());
                assert!(log.get("vector.component_type").is_none());
            } else if i < 3 {
                assert_eq!(log["vector.component_id"], "foo".into());
                assert_eq!(log["vector.component_kind"], "source".into());
                assert_eq!(log["vector.component_type"], "internal_logs".into());
            } else {
                // The last event occurs in a nested span. Here, we expect
                // parent fields to be preserved (unless overwritten), new
                // fields to be added, and filtered fields to not exist.
                assert_eq!(log["vector.component_id"], "foo".into());
                assert_eq!(log["vector.component_kind"], "bar".into());
                assert_eq!(log["vector.component_type"], "internal_logs".into());
                assert_eq!(log["vector.component_new_field"], "baz".into());
                assert_eq!(log["vector.component_numerical_field"], 1.into());
                assert!(log.get("vector.ignored_field").is_none());
            }
        }
    }

    async fn start_source() -> impl Stream<Item = Event> + Unpin {
        let (tx, rx) = SourceSender::new_test();

        let source = InternalLogsConfig::default()
            .build(SourceContext::new_test(tx, None))
            .await
            .unwrap();
        tokio::spawn(source);
        sleep(Duration::from_millis(1)).await;
        trace::stop_early_buffering();
        rx
    }

    /// Regression test for the wave 1 → wave 2 VEL drop.
    ///
    /// Emits several tracing events and then synchronously fires shutdown in
    /// the same scheduler tick, with no `.await` in between. That is the same
    /// shape as the `info!` → `deferred_shutdowns.shutdown_all()` sequence in
    /// `topology::running::shutdown`: the events are queued in the tracing
    /// broadcast channel and the source's `ShutdownSignal` fires before the
    /// source task has a chance to wake up and consume them. The drain loop
    /// must yield those queued events before exiting; the previous
    /// `take_until(shutdown)` implementation would drop them.
    #[tokio::test]
    #[serial]
    async fn drains_buffered_events_on_shutdown() {
        trace::init(false, false, "error", 10);
        trace::reset_early_buffer();

        let (tx, rx) = SourceSender::new_test();
        let key = ComponentKey::from("internal_logs_drain_test");
        let (cx, coordinator) = SourceContext::new_shutdown(&key, tx);
        let source = InternalLogsConfig::default().build(cx).await.unwrap();
        tokio::spawn(source);
        sleep(Duration::from_millis(1)).await;
        trace::stop_early_buffering();

        // Events and shutdown in the same tick — no `.await` between them.
        for i in 0..5u8 {
            error!(drain_test_id = i, "event before shutdown");
        }
        coordinator.shutdown_all(None).await;

        let events = collect_ready(rx).await;
        let drain_events: Vec<_> = events
            .iter()
            .filter(|e| e.as_log().get("drain_test_id").is_some())
            .collect();
        assert_eq!(
            drain_events.len(),
            5,
            "expected all 5 buffered events to be drained on shutdown, got {}",
            drain_events.len(),
        );
    }

    /// The drain must catch an event that arrives *after* shutdown resolves and the
    /// drain has begun (the `VECTOR_PROCESS_COMPONENTS_CLOSED` race), not only events
    /// already buffered when shutdown fires. Emit the event after awaiting shutdown so
    /// the drain loop is already polling an empty channel.
    #[tokio::test]
    #[serial]
    async fn drains_event_arriving_after_shutdown_begins() {
        trace::init(false, false, "error", 10);
        trace::reset_early_buffer();

        let (tx, rx) = SourceSender::new_test();
        let key = ComponentKey::from("internal_logs_late_drain_test");
        let (cx, coordinator) = SourceContext::new_shutdown(&key, tx);
        let source = InternalLogsConfig::default().build(cx).await.unwrap();
        tokio::spawn(source);
        sleep(Duration::from_millis(1)).await;
        trace::stop_early_buffering();

        coordinator.shutdown_all(None).await;
        sleep(Duration::from_millis(1)).await;
        error!(late_drain_id = 1, "event after shutdown begins");

        // Let the drain forward the late event before its idle timeout fires.
        sleep(Duration::from_millis(50)).await;
        let events = collect_ready(rx).await;
        let late_events: Vec<_> = events
            .iter()
            .filter(|e| e.as_log().get("late_drain_id").is_some())
            .collect();
        assert_eq!(
            late_events.len(),
            1,
            "expected the post-shutdown event to be drained, got {}",
            late_events.len(),
        );
    }

    // NOTE: This test requires #[serial] because it directly interacts with global tracing state.
    // This is a pre-existing limitation around tracing initialization in tests.
    // We use error!() level logs because another test may have initialized tracing first
    // with a filter that excludes info-level logs.
    #[tokio::test]
    #[serial]
    async fn repeated_logs_are_not_rate_limited() {
        trace::init(false, false, "error", 10);
        trace::reset_early_buffer();

        let rx = start_source().await;

        // Generate 20 identical log messages with the same component_id
        for _ in 0..20 {
            error!(component_id = "test", "Repeated test message.");
        }

        sleep(Duration::from_millis(50)).await;
        let events = collect_ready(rx).await;

        // Filter to only our test messages
        let test_events: Vec<_> = events
            .iter()
            .filter(|e| {
                e.as_log()
                    .get("message")
                    .map(|m| m.to_string_lossy() == "Repeated test message.")
                    .unwrap_or(false)
            })
            .collect();

        // We should receive all 20 messages, no rate limiting.
        assert_eq!(
            test_events.len(),
            20,
            "internal_logs source should capture all repeated messages without rate limiting"
        );
    }

    #[test]
    fn output_schema_definition_vector_namespace() {
        let config = InternalLogsConfig::default();

        let definitions = config
            .outputs(LogNamespace::Vector)
            .remove(0)
            .schema_definition(true);

        let expected_definition =
            Definition::new_with_default_metadata(Kind::bytes(), [LogNamespace::Vector])
                .with_meaning(OwnedTargetPath::event_root(), "message")
                .with_metadata_field(
                    &owned_value_path!("vector", "source_type"),
                    Kind::bytes(),
                    None,
                )
                .with_metadata_field(
                    &owned_value_path!(InternalLogsConfig::NAME, "pid"),
                    Kind::integer(),
                    None,
                )
                .with_metadata_field(
                    &owned_value_path!("vector", "ingest_timestamp"),
                    Kind::timestamp(),
                    None,
                )
                .with_metadata_field(
                    &owned_value_path!(InternalLogsConfig::NAME, "host"),
                    Kind::bytes().or_undefined(),
                    Some("host"),
                );

        assert_eq!(definitions, Some(expected_definition))
    }

    #[test]
    fn output_schema_definition_legacy_namespace() {
        let mut config = InternalLogsConfig::default();

        let pid_key = "pid_a_pid_a_pid_pid_pid";

        config.pid_key = OptionalValuePath::from(owned_value_path!(pid_key));

        let definitions = config
            .outputs(LogNamespace::Legacy)
            .remove(0)
            .schema_definition(true);

        let expected_definition = Definition::new_with_default_metadata(
            Kind::object(Collection::empty()),
            [LogNamespace::Legacy],
        )
        .with_event_field(
            &owned_value_path!("message"),
            Kind::bytes(),
            Some("message"),
        )
        .with_event_field(&owned_value_path!("source_type"), Kind::bytes(), None)
        .with_event_field(&owned_value_path!(pid_key), Kind::integer(), None)
        .with_event_field(&owned_value_path!("timestamp"), Kind::timestamp(), None)
        .with_event_field(
            &owned_value_path!("host"),
            Kind::bytes().or_undefined(),
            Some("host"),
        );

        assert_eq!(definitions, Some(expected_definition))
    }
}
