//! The `vector` source. See [VectorConfig].
use std::{borrow::Cow, collections::HashMap, net::SocketAddr};

use chrono::Utc;
use futures::TryFutureExt;
use indexmap::{Equivalent, IndexMap};
use tonic::{Request, Response, Status};
use vector_lib::{
    EstimatedJsonEncodedSizeOf,
    codecs::NativeDeserializerConfig,
    config::LogNamespace,
    configurable::configurable_component,
    event::{BatchNotifier, BatchStatus, BatchStatusReceiver, Event, LogEvent, Value},
    internal_event::{
        CountByteSize, InternalEventHandle as _,
        vector_event::delivery_event::{
            SOURCE_TYPE_VECTOR, current_hour_time_parity_ms_value, delivery_singleton,
        },
    },
    lookup::{event_path, path},
};

use crate::{
    SourceSender,
    config::{
        DataType, GenerateConfig, Resource, SourceAcknowledgementsConfig, SourceConfig,
        SourceContext, SourceOutput, tls_snapshot::TlsConfigSnapshot,
    },
    internal_events::{EventsReceived, StreamClosedError},
    proto::vector as proto,
    serde::bool_or_struct,
    sources::{Source, util::grpc::run_grpc_server},
    tls::TlsEnableableConfig,
};

/// Marker type for version two of the configuration for the `vector` source.
#[configurable_component]
#[derive(Clone, Debug)]
enum VectorConfigVersion {
    /// Marker value for version two.
    #[serde(rename = "2")]
    V2,
}

#[derive(Debug, Clone)]
struct Service {
    pipeline: SourceSender,
    acknowledgements: bool,
    log_namespace: LogNamespace,
    emit_read_delivery_events: bool,
}

/// `message_tag` of a diskless structured-log batch, the only batch shape that
/// is counted as a source read.
const STRUCTURED_MESSAGE_TAG: &str = "MESSAGE_TAG_STRUCTURED_MESSAGE";

/// Labels one diskless read accumulation: the same topic / system / workload
/// identity the file and `kubernetes_logs` sources attach to their reads.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DisklessReadKey {
    topic: String,
    service_system: Option<String>,
    source_pod_id: Option<String>,
}

/// Borrowed form of [`DisklessReadKey`], so a message whose labels are already
/// in the map is counted without allocating. Its derived `Hash` matches
/// `DisklessReadKey`'s (`&str` hashes like `String`, field for field).
#[derive(Hash)]
struct DisklessReadKeyRef<'a> {
    topic: &'a str,
    service_system: Option<&'a str>,
    source_pod_id: Option<&'a str>,
}

impl Equivalent<DisklessReadKey> for DisklessReadKeyRef<'_> {
    fn equivalent(&self, key: &DisklessReadKey) -> bool {
        self.topic == key.topic
            && self.service_system == key.service_system.as_deref()
            && self.source_pod_id == key.source_pod_id.as_deref()
    }
}

impl DisklessReadKeyRef<'_> {
    fn to_owned_key(&self) -> DisklessReadKey {
        DisklessReadKey {
            topic: self.topic.to_owned(),
            service_system: self.service_system.map(str::to_owned),
            source_pod_id: self.source_pod_id.map(str::to_owned),
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct DisklessReadCount {
    lines: usize,
    bytes: usize,
}

type DisklessReads = IndexMap<DisklessReadKey, DisklessReadCount>;

fn non_empty_str(value: Option<&Value>) -> Option<Cow<'_, str>> {
    value.and_then(Value::as_str).filter(|s| !s.is_empty())
}

/// Dash-cases a topic the way file-source reads and the woodchuck diskless
/// `logMetadata.topic` do, so every delivery leg keys on the same value.
fn dash_case_topic(topic: &str) -> Cow<'_, str> {
    if topic.contains('_') {
        Cow::Owned(topic.replace('_', "-"))
    } else {
        Cow::Borrowed(topic)
    }
}

/// Counts the diskless structured-log messages in a request, grouped by read
/// labels, and stamps the read-time `time_parity` onto each counted batch so the
/// staged/delivered legs bucket on the same hour (as `kubernetes_logs` does).
///
/// Each event carries `structured_log_messages.structured_log_messages[]`; one
/// array element is one log line. Per message: topic is the message `topic`,
/// else the batch `topic`, dash-cased; system is `log_entry.system_uri` without its
/// `system:` prefix; workload is `log_entry.pod_name`, else `log_entry.pod_uid`.
/// Bytes are the length of the message payload.
fn collect_diskless_reads(events: &mut [Event], time_parity: i64) -> DisklessReads {
    let mut reads = DisklessReads::default();
    for event in events {
        let Event::Log(log) = event else { continue };
        if !accumulate_diskless_batch(log, &mut reads) {
            continue;
        }
        log.insert(event_path!("time_parity"), time_parity);
    }
    reads
}

/// Adds one batch's messages to `reads`. Returns whether the event was a
/// diskless structured-log batch.
fn accumulate_diskless_batch(log: &LogEvent, reads: &mut DisklessReads) -> bool {
    if log
        .get(event_path!("message_tag"))
        .and_then(Value::as_str)
        .as_deref()
        != Some(STRUCTURED_MESSAGE_TAG)
    {
        return false;
    }
    let Some(Value::Array(messages)) = log.get(event_path!(
        "structured_log_messages",
        "structured_log_messages"
    )) else {
        return false;
    };
    let batch_topic = non_empty_str(log.get(event_path!("topic")));
    for message in messages {
        let message_topic = non_empty_str(message.get(path!("topic")));
        let topic = dash_case_topic(
            message_topic
                .as_deref()
                .or(batch_topic.as_deref())
                .unwrap_or("unknown"),
        );
        let system_uri = message
            .get(path!("log_entry", "system_uri"))
            .and_then(Value::as_str);
        let pod_name = non_empty_str(message.get(path!("log_entry", "pod_name")));
        let pod_uid = non_empty_str(message.get(path!("log_entry", "pod_uid")));
        let key = DisklessReadKeyRef {
            topic: &topic,
            service_system: system_uri
                .as_deref()
                .and_then(|uri| uri.strip_prefix("system:"))
                .filter(|system| !system.is_empty()),
            source_pod_id: pod_name.as_deref().or(pod_uid.as_deref()),
        };
        let bytes = match message.get(path!("message")) {
            Some(Value::Bytes(bytes)) => bytes.len(),
            _ => 0,
        };
        let count = match reads.get_mut(&key) {
            Some(count) => count,
            None => reads.entry(key.to_owned_key()).or_default(),
        };
        count.lines += 1;
        count.bytes += bytes;
    }
    true
}

/// Emits `VECTOR_SOURCE_READ` for the counted diskless reads. The topic doubles
/// as the read "path" (the VEL accumulation key) and is passed as the explicit
/// `source_context.topic`, which the read labels and the universe VRL prefer.
fn emit_diskless_reads(reads: DisklessReads, time_parity: i64) {
    for (key, count) in reads {
        let source_context = Some(HashMap::from([("topic".to_owned(), key.topic.clone())]));
        delivery_singleton().accumulate_read(
            key.topic,
            count.bytes,
            count.lines,
            &source_context,
            key.service_system.as_deref(),
            key.source_pod_id.as_deref(),
            SOURCE_TYPE_VECTOR,
            time_parity,
            false,
            None,
            None,
        );
    }
}

#[tonic::async_trait]
impl proto::Service for Service {
    async fn push_events(
        &self,
        request: Request<proto::PushEventsRequest>,
    ) -> Result<Response<proto::PushEventsResponse>, Status> {
        let mut events: Vec<Event> = request
            .into_inner()
            .events
            .into_iter()
            .map(Event::from)
            .collect();

        let now = Utc::now();
        for event in &mut events {
            if let Event::Log(log) = event {
                self.log_namespace.insert_standard_vector_source_metadata(
                    log,
                    VectorConfig::NAME,
                    now,
                );
            }
        }

        let diskless_reads = self.emit_read_delivery_events.then(|| {
            let time_parity = current_hour_time_parity_ms_value();
            (
                collect_diskless_reads(&mut events, time_parity),
                time_parity,
            )
        });

        let count = events.len();
        let byte_size = events.estimated_json_encoded_size_of();
        let events_received = register!(EventsReceived);
        events_received.emit(CountByteSize(count, byte_size));

        let receiver = BatchNotifier::maybe_apply_to(self.acknowledgements, &mut events);

        self.pipeline
            .clone()
            .send_batch(events)
            .map_err(|error| {
                let message = error.to_string();
                emit!(StreamClosedError { count });
                Status::unavailable(message)
            })
            .await?;

        // Count reads once the batch is admitted to the pipeline, matching the
        // file sources counting at read time. A failed send is retried by the
        // client, so counting before admission would read it twice. Retries
        // after a rejected/errored acknowledgement *are* counted again, on
        // purpose: counting only acknowledged batches would hide the drops the
        // read/delivered ratio exists to show.
        if let Some((reads, time_parity)) = diskless_reads {
            emit_diskless_reads(reads, time_parity);
        }

        handle_batch_status(receiver).await?;

        Ok(Response::new(proto::PushEventsResponse {}))
    }

    // TODO: figure out a way to determine if the current Vector instance is "healthy".
    async fn health_check(
        &self,
        _: Request<proto::HealthCheckRequest>,
    ) -> Result<Response<proto::HealthCheckResponse>, Status> {
        let message = proto::HealthCheckResponse {
            status: proto::ServingStatus::Serving.into(),
        };

        Ok(Response::new(message))
    }
}

async fn handle_batch_status(receiver: Option<BatchStatusReceiver>) -> Result<(), Status> {
    let status = match receiver {
        Some(receiver) => receiver.await,
        None => BatchStatus::Delivered,
    };

    match status {
        BatchStatus::Errored => Err(Status::internal("Delivery error")),
        BatchStatus::Rejected => Err(Status::data_loss("Delivery failed")),
        BatchStatus::Delivered => Ok(()),
    }
}

/// Configuration for the `vector` source.
#[configurable_component(source("vector", "Collect observability data from a Vector instance."))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct VectorConfig {
    /// Version of the configuration.
    version: Option<VectorConfigVersion>,

    /// The socket address to listen for connections on.
    ///
    /// It _must_ include a port.
    pub address: SocketAddr,

    #[configurable(derived)]
    #[serde(default)]
    tls: Option<TlsEnableableConfig>,

    #[configurable(derived)]
    #[serde(default, deserialize_with = "bool_or_struct")]
    acknowledgements: SourceAcknowledgementsConfig,

    /// The namespace to use for logs. This overrides the global setting.
    #[serde(default)]
    #[configurable(metadata(docs::hidden))]
    pub log_namespace: Option<bool>,

    /// The maximum number of request source will handle concurrently.
    #[configurable(derived)]
    #[serde(default)]
    max_concurrent_requests: Option<usize>,

    /// Emit `VECTOR_SOURCE_READ` delivery events for diskless structured-log
    /// batches (`message_tag = MESSAGE_TAG_STRUCTURED_MESSAGE`), counting each
    /// `structured_log_messages.structured_log_messages[]` element as one line,
    /// and stamp the read-time `time_parity` onto each counted batch (the
    /// woodchuck diskless transform copies it into `logMetadata.timeParity`, as
    /// the file wrappers do for file-source reads).
    #[serde(default)]
    #[configurable(metadata(docs::hidden))]
    pub emit_read_delivery_events: bool,

    #[serde(skip)]
    tls_snapshot: TlsConfigSnapshot,
}

impl VectorConfig {
    /// Creates a `VectorConfig` with the given address.
    pub fn from_address(addr: SocketAddr) -> Self {
        Self {
            address: addr,
            ..Default::default()
        }
    }
}

impl Default for VectorConfig {
    fn default() -> Self {
        Self {
            version: None,
            address: "0.0.0.0:6000".parse().unwrap(),
            tls: None,
            acknowledgements: Default::default(),
            log_namespace: None,
            max_concurrent_requests: None,
            emit_read_delivery_events: false,
            tls_snapshot: Default::default(),
        }
    }
}

impl GenerateConfig for VectorConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(VectorConfig::default()).unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "vector")]
impl SourceConfig for VectorConfig {
    fn prepare_reload(&mut self) -> crate::Result<()> {
        self.tls_snapshot.prepare(self.tls.as_ref(), true)
    }

    fn reload_snapshot(&self) -> Option<crate::config::BoxedSource> {
        Some(Box::new(self.clone()))
    }

    async fn build(&self, cx: SourceContext) -> crate::Result<Source> {
        let tls_settings = self.tls_snapshot.get_or_load(self.tls.as_ref(), true)?;
        let acknowledgements = cx.do_acknowledgements(self.acknowledgements);
        let log_namespace = cx.log_namespace(self.log_namespace);

        let service = proto::Server::new(Service {
            pipeline: cx.out,
            acknowledgements,
            log_namespace,
            emit_read_delivery_events: self.emit_read_delivery_events,
        })
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        // Tonic added a default of 4MB in 0.9. This replaces the old behavior.
        .max_decoding_message_size(usize::MAX);

        let source = run_grpc_server(
            self.address,
            tls_settings,
            service,
            cx.shutdown,
            self.max_concurrent_requests,
        )
        .map_err(|error| {
            error!(message = "Source future failed.", %error);
        });

        Ok(Box::pin(source))
    }

    fn outputs(&self, global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        let log_namespace = global_log_namespace.merge(self.log_namespace);

        let schema_definition = NativeDeserializerConfig
            .schema_definition(log_namespace)
            .with_standard_vector_source_metadata();

        vec![SourceOutput::new_maybe_logs(
            DataType::all_bits(),
            schema_definition,
        )]
    }

    fn resources(&self) -> Vec<Resource> {
        vec![Resource::tcp(self.address)]
    }

    fn can_acknowledge(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod test {
    use vector_lib::{config::LogNamespace, lookup::owned_value_path, schema::Definition};
    use vrl::value::{Kind, kind::Collection};

    use super::VectorConfig;
    use crate::config::SourceConfig;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<super::VectorConfig>();
    }

    mod diskless_reads {
        use std::collections::HashMap;

        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        use tonic::Request;
        use vector_lib::{
            config::LogNamespace,
            event::{Event, LogEvent, Value, proto::EventWrapper},
            internal_event::vector_event::delivery_event::DELIVERY_METHOD_DISKLESS,
        };
        use vrl::btreemap;

        use super::super::{
            DisklessReadCount, DisklessReadKey, STRUCTURED_MESSAGE_TAG, Service,
            collect_diskless_reads,
        };
        use crate::{SourceSender, proto::vector as proto, proto::vector::Service as _};

        const TP: i64 = 1_700_000_000_000 - (1_700_000_000_000 % (60 * 60 * 1000));

        fn message(topic: Option<&str>, payload: &str, log_entry: Value) -> Value {
            let mut message = btreemap! {
                "message" => Value::from(payload),
                "log_entry" => log_entry,
            };
            if let Some(topic) = topic {
                message.insert("topic".into(), Value::from(topic));
            }
            Value::Object(message)
        }

        fn batch(tag: &str, topic: &str, messages: Vec<Value>) -> Event {
            let mut log = LogEvent::default();
            log.insert("message_tag", tag);
            log.insert("topic", topic);
            log.insert(
                "structured_log_messages.structured_log_messages",
                Value::Array(messages),
            );
            Event::Log(log)
        }

        fn key(topic: &str, system: Option<&str>, pod: Option<&str>) -> DisklessReadKey {
            DisklessReadKey {
                topic: topic.to_owned(),
                service_system: system.map(str::to_owned),
                source_pod_id: pod.map(str::to_owned),
            }
        }

        #[test]
        fn groups_messages_by_topic_system_and_pod() {
            let entry = Value::Object(btreemap! {
                "system_uri" => "system:quercus-cp",
                "pod_name" => "quercus-cp-abc",
                "pod_uid" => "uid-1",
            });
            let uid_only = Value::Object(btreemap! {
                "system_uri" => "quercus-cp",
                "pod_name" => "",
                "pod_uid" => "uid-2",
            });
            let mut events = vec![
                batch(
                    STRUCTURED_MESSAGE_TAG,
                    "batch-topic",
                    vec![
                        message(Some("service-request-log"), "abcd", entry.clone()),
                        message(Some("service-request-log"), "ef", entry.clone()),
                        // No per-message topic: falls back to the batch topic.
                        message(None, "g", entry),
                        // No `system:` prefix → no system; empty pod_name → pod_uid.
                        message(Some("service-request-log"), "hij", uid_only),
                        // Dash-cased like file-source reads.
                        message(Some("audit_log"), "k", Value::Null),
                    ],
                ),
                // Not a structured-log batch: neither counted nor stamped.
                batch(
                    "MESSAGE_TAG_INTERNAL_EVENT_LOG",
                    "t",
                    vec![message(None, "x", Value::Null)],
                ),
            ];

            let reads: HashMap<_, _> = collect_diskless_reads(&mut events, TP)
                .into_iter()
                .collect();

            assert_eq!(
                reads,
                HashMap::from([
                    (
                        key(
                            "service-request-log",
                            Some("quercus-cp"),
                            Some("quercus-cp-abc")
                        ),
                        DisklessReadCount { lines: 2, bytes: 6 },
                    ),
                    (
                        key("batch-topic", Some("quercus-cp"), Some("quercus-cp-abc")),
                        DisklessReadCount { lines: 1, bytes: 1 },
                    ),
                    (
                        key("service-request-log", None, Some("uid-2")),
                        DisklessReadCount { lines: 1, bytes: 3 },
                    ),
                    (
                        key("audit-log", None, None),
                        DisklessReadCount { lines: 1, bytes: 1 }
                    ),
                ])
            );
            assert_eq!(
                events[0].as_log().get("time_parity"),
                Some(&Value::from(TP))
            );
            assert_eq!(events[1].as_log().get("time_parity"), None);
        }

        fn service(pipeline: SourceSender, emit_read_delivery_events: bool) -> Service {
            Service {
                pipeline,
                acknowledgements: false,
                log_namespace: LogNamespace::Legacy,
                emit_read_delivery_events,
            }
        }

        fn request() -> Request<proto::PushEventsRequest> {
            let entry = Value::Object(btreemap! {
                "system_uri" => "system:quercus-cp",
                "pod_name" => "quercus-cp-abc",
            });
            let event = batch(
                STRUCTURED_MESSAGE_TAG,
                "service-request-log",
                vec![
                    message(None, "abcd", entry.clone()),
                    message(None, "ef", entry),
                ],
            );
            Request::new(proto::PushEventsRequest {
                events: vec![EventWrapper::from(event)],
            })
        }

        /// Runs `push_events` under a local metrics recorder and returns the
        /// `delivery_events_total` series as (labels, value) pairs.
        fn push_and_collect_read_counters(
            service: &Service,
        ) -> (bool, Vec<(HashMap<String, String>, u64)>) {
            let recorder = DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let ok = metrics::with_local_recorder(&recorder, || {
                runtime.block_on(service.push_events(request())).is_ok()
            });
            let counters = snapshotter
                .snapshot()
                .into_vec()
                .into_iter()
                .filter(|(key, ..)| key.key().name() == "delivery_events_total")
                .map(|(key, _, _, value)| {
                    let labels = key
                        .key()
                        .labels()
                        .map(|label| (label.key().to_owned(), label.value().to_owned()))
                        .collect();
                    let DebugValue::Counter(value) = value else {
                        panic!("delivery_events_total must be a counter");
                    };
                    (labels, value)
                })
                .collect();
            (ok, counters)
        }

        #[test]
        fn admitted_batch_emits_source_read_counters() {
            let (tx, _rx) = SourceSender::new_test();
            let (ok, counters) = push_and_collect_read_counters(&service(tx, true));

            assert!(ok);
            // One series for the real pod plus the all-source-pods companion.
            assert_eq!(counters.len(), 2);
            for (labels, value) in counters {
                assert_eq!(value, 2);
                assert_eq!(labels["delivery_event_type"], "VECTOR_SOURCE_READ");
                assert_eq!(labels["delivery_method"], DELIVERY_METHOD_DISKLESS);
                assert_eq!(labels["topic"], "service-request-log");
                assert_eq!(labels["service_system"], "quercus-cp");
                assert!(
                    ["quercus-cp-abc", "__all_source_pods__"]
                        .contains(&labels["source_pod_id"].as_str())
                );
            }
        }

        #[test]
        fn disabled_flag_emits_no_read_counters() {
            let (tx, _rx) = SourceSender::new_test();
            let (ok, counters) = push_and_collect_read_counters(&service(tx, false));

            assert!(ok);
            assert!(counters.is_empty());
        }

        #[test]
        fn failed_send_emits_no_read_counters() {
            let (tx, rx) = SourceSender::new_test();
            drop(rx);
            let (ok, counters) = push_and_collect_read_counters(&service(tx, true));

            assert!(!ok, "send to a closed pipeline must fail");
            assert!(counters.is_empty());
        }
    }

    #[test]
    fn output_schema_definition_vector_namespace() {
        let config = VectorConfig::default();

        let definitions = config
            .outputs(LogNamespace::Vector)
            .remove(0)
            .schema_definition(true);

        let expected_definition =
            Definition::new_with_default_metadata(Kind::any(), [LogNamespace::Vector])
                .with_metadata_field(
                    &owned_value_path!("vector", "source_type"),
                    Kind::bytes(),
                    None,
                )
                .with_metadata_field(
                    &owned_value_path!("vector", "ingest_timestamp"),
                    Kind::timestamp(),
                    None,
                );

        assert_eq!(definitions, Some(expected_definition))
    }

    #[test]
    fn output_schema_definition_legacy_namespace() {
        let config = VectorConfig::default();

        let definitions = config
            .outputs(LogNamespace::Legacy)
            .remove(0)
            .schema_definition(true);

        let expected_definition = Definition::new_with_default_metadata(
            Kind::object(Collection::empty()),
            [LogNamespace::Legacy],
        )
        .with_event_field(&owned_value_path!("source_type"), Kind::bytes(), None)
        .with_event_field(&owned_value_path!("timestamp"), Kind::timestamp(), None);

        assert_eq!(definitions, Some(expected_definition))
    }
}

#[cfg(feature = "sinks-vector")]
#[cfg(test)]
mod tests {
    use vector_lib::{assert_event_data_eq, config::log_schema};

    use super::*;
    use crate::{
        SourceSender,
        config::{SinkConfig as _, SinkContext},
        sinks::vector::VectorConfig as SinkConfig,
        test_util,
    };

    async fn run_test(vector_source_config_str: &str, addr: SocketAddr) {
        let config = format!(r#"address = "{addr}""#);
        let source: VectorConfig = toml::from_str(&config).unwrap();

        let (tx, rx) = SourceSender::new_test();
        let server = source
            .build(SourceContext::new_test(tx, None))
            .await
            .unwrap();
        tokio::spawn(server);
        test_util::wait_for_tcp(addr).await;

        // Ideally, this would be a fully custom agent to send the data,
        // but the sink side already does such a test and this is good
        // to ensure interoperability.
        let sink: SinkConfig = toml::from_str(vector_source_config_str).unwrap();
        let cx = SinkContext::default();
        let (sink, _) = sink.build(cx).await.unwrap();

        let (mut events, stream) = test_util::random_events_with_stream(100, 100, None);
        sink.run(stream).await.unwrap();

        for event in &mut events {
            event.as_mut_log().insert(
                log_schema().source_type_key_target_path().unwrap(),
                "vector",
            );
        }

        let output = test_util::collect_ready(rx).await;
        assert_event_data_eq!(events, output);
    }

    #[tokio::test]
    async fn receive_message() {
        let (_guard, addr) = test_util::addr::next_addr();

        let config = format!(r#"address = "{addr}""#);
        run_test(&config, addr).await;
    }

    #[tokio::test]
    async fn receive_compressed_message() {
        let (_guard, addr) = test_util::addr::next_addr();

        let config = format!(
            r#"address = "{addr}"
            compression=true"#
        );
        run_test(&config, addr).await;
    }
}
