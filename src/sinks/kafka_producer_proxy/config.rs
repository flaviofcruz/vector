use std::collections::HashSet;
use std::time::Duration;

use http::Uri;
use hyper::client::HttpConnector;
use hyper_openssl::HttpsConnector;
use hyper_proxy::ProxyConnector;
use serde_with::serde_as;
use tonic::body::BoxBody;
use tower::{Layer, ServiceBuilder};
use vector_lib::configurable::configurable_component;

use super::{
    KafkaProducerProxySinkError,
    blacklist::TopicBlacklistLayer,
    service::{KPPRequest, KPPResponse, KPPService},
    sink::KafkaProducerProxySink,
};
use crate::sinks::util::dynamic_balance::{self, LoadBalancingConfig};
use crate::sinks::util::vector_event_log::EventLoggingService;
use crate::{
    config::{AcknowledgementsConfig, GenerateConfig, Input, ProxyConfig, SinkConfig, SinkContext},
    http::build_proxy_connector,
    sinks::{
        Healthcheck, VectorSink,
        kafka_producer_proxy::ResponseCodes,
        util::{
            BatchConfig, RealtimeEventBasedDefaultBatchSettings, ServiceBuilderExt,
            TowerRequestConfig,
            retries::{RetryAction, RetryLogic},
        },
    },
    tls::{MaybeTlsSettings, TlsEnableableConfig},
};

/// Configuration for the KafkaProducerProxy sink.
#[serde_as]
#[configurable_component(sink(
    "kafka_producer_proxy",
    "Publish observability event data to internal Databricks Kafka server."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct KafkaProducerProxyConfig {
    /// The downstream Kafka Server address to which to connect
    address: String,

    /// The topic to which the sink should publish events to
    #[configurable(metadata(docs::advanced))]
    topic: String,

    /// Batch Settings
    #[configurable(derived)]
    #[serde(default)]
    pub batch: BatchConfig<RealtimeEventBasedDefaultBatchSettings>,

    /// The key used to send the message to a specific partition
    /// This will be the key field that will be searched for every incoming event
    /// to determine the key
    /// This can be left unspecified and is optional
    /// Defaults to "key"
    #[configurable(metadata(docs::advanced))]
    #[configurable(metadata(docs::human_name = "Kafka Message Key Field"))]
    #[serde(default = "default_key_field")]
    pub key_field: String,

    /// The message field used to determine where the message is in the incoming event
    /// This can be left unspecified and is optional **however**, the message must be
    /// set in the incoming message and if this field is left unspecified, the message
    /// must be in a field called "message" in the incoming event. If it does not
    /// follow this format, the message will be dropped.
    /// Defaults to "message"
    #[configurable(metadata(docs::advanced))]
    #[configurable(metadata(docs::human_name = "Kafka Message Message Field"))]
    #[serde(default = "default_message_field")]
    pub message_field: String,

    /// The key used for finding log in the message.
    /// This can be left unspecified and is optional.
    /// Defaults to "log_entry"
    #[configurable(metadata(docs::advanced))]
    #[configurable(metadata(docs::human_name = "Kafka Message Log Entry"))]
    #[serde(default = "default_log_entry")]
    pub log_entry: String,

    /// The duration (in seconds) at which the blacklisting of the topic should last
    /// Defaults to 30 seconds
    #[configurable(metadata(docs::advanced))]
    #[configurable(metadata(docs::human_name = "Topic Blacklist Duration"))]
    #[serde(default = "default_blacklist_duration")]
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    pub blacklist_duration: Duration,

    #[configurable(derived)]
    #[serde(default)]
    pub request: TowerRequestConfig,

    /// Opt-in DNS-driven per-pod client-side load balancing.
    #[configurable(derived)]
    #[serde(default, skip_serializing_if = "crate::serde::is_default")]
    pub(crate) load_balancing: LoadBalancingConfig,

    #[configurable(derived)]
    #[serde(default)]
    tls: Option<TlsEnableableConfig>,

    #[serde(skip)]
    tls_snapshot: crate::config::tls_snapshot::TlsConfigSnapshot,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,
}

impl Default for KafkaProducerProxyConfig {
    fn default() -> Self {
        Self {
            address: String::new(),
            topic: String::new(),
            batch: BatchConfig::default(),
            key_field: default_key_field(),
            message_field: default_message_field(),
            log_entry: default_log_entry(),
            blacklist_duration: default_blacklist_duration(),
            request: TowerRequestConfig::default(),
            load_balancing: LoadBalancingConfig::default(),
            tls: None,
            tls_snapshot: Default::default(),
            acknowledgements: AcknowledgementsConfig::default(),
        }
    }
}

fn default_key_field() -> String {
    "key".to_string()
}

fn default_message_field() -> String {
    "message".to_string()
}

fn default_log_entry() -> String {
    "log_entry".to_string()
}

const fn default_blacklist_duration() -> Duration {
    Duration::from_secs(30)
}

impl KafkaProducerProxyConfig {
    /// Creates a `VectorConfig` with the given address.
    pub fn from_address(addr: Uri) -> Self {
        let addr = addr.to_string();
        KafkaProducerProxyConfig {
            address: addr,
            ..Default::default()
        }
    }
}

impl GenerateConfig for KafkaProducerProxyConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self::from_address("127.0.0.1:6000".parse().unwrap())).unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "kafka_producer_proxy")]
impl SinkConfig for KafkaProducerProxyConfig {
    fn prepare_reload(&mut self) -> crate::Result<()> {
        self.tls_snapshot.prepare(self.tls.as_ref(), false)
    }

    fn reload_snapshot(&self) -> Option<crate::config::BoxedSink> {
        Some(Box::new(self.clone()))
    }

    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        if self.load_balancing.enabled {
            return self.build_balanced(cx).await;
        }

        let tls = self.tls_snapshot.get_or_load(self.tls.as_ref(), false)?;
        let uri = with_default_scheme(&self.address, tls.is_tls())?;

        let client = new_client(&tls, cx.proxy())?;

        let service = KPPService::new(client, uri);
        let request_settings = self.request.into_settings();
        let batch_settings = self.batch.into_batcher_settings()?;

        let blacklist_layer = TopicBlacklistLayer::new(self.blacklist_duration);

        let service = ServiceBuilder::new()
            .settings(request_settings, KafkaProducerProxyGrpcRetryLogic)
            .layer(blacklist_layer)
            .service(service);

        let event_log_service = EventLoggingService::new(service);

        let sink = KafkaProducerProxySink {
            topic: self.topic.clone(),
            key_field: self.key_field.parse().unwrap(),
            message_field: self.message_field.parse().unwrap(),
            log_entry: self.log_entry.parse().unwrap(),
            batch_settings,
            service: event_log_service,
        };

        Ok((
            VectorSink::from_event_streamsink(sink),
            Box::pin(futures::future::ok(())),
        ))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

impl KafkaProducerProxyConfig {
    async fn build_balanced(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let tls = self.tls_snapshot.get_or_load(self.tls.as_ref(), false)?;
        let uri = with_default_scheme(&self.address, tls.is_tls())?;
        let target = self.load_balancing.discovery_uri(&uri)?;
        let client = dynamic_balance::new_client(&tls, self.tls.as_ref(), cx.proxy())?;
        let fallback = KPPService::new(client.clone(), uri.clone());
        let settings = self.request.into_settings();
        let scheme = uri.scheme_str().unwrap_or("http").to_owned();
        let balanced = dynamic_balance::balanced_service(
            settings.clone(),
            KafkaProducerProxyGrpcRetryLogic,
            self.load_balancing.clone(),
            target.into(),
            fallback,
            move |addr| {
                let endpoint = format!("{scheme}://{addr}/")
                    .parse()
                    .expect("valid pod URI");
                KPPService::new(client.clone(), endpoint)
            },
            classify_endpoint_error,
        );
        let service = balanced_request_service(settings, balanced, self.blacklist_duration);
        let sink = KafkaProducerProxySink {
            topic: self.topic.clone(),
            key_field: self.key_field.parse()?,
            message_field: self.message_field.parse()?,
            log_entry: self.log_entry.parse()?,
            batch_settings: self.batch.into_batcher_settings()?,
            service: EventLoggingService::new(service),
        };
        Ok((
            VectorSink::from_event_streamsink(sink),
            Box::pin(futures::future::ok(())),
        ))
    }
}

fn balanced_request_service(
    settings: crate::sinks::util::TowerRequestSettings,
    balanced: dynamic_balance::BalancedService<KPPRequest, KPPResponse>,
    blacklist_duration: Duration,
) -> impl tower::Service<
    KPPRequest,
    Response = KPPResponse,
    Error = crate::Error,
    Future: Send + 'static,
> + Send {
    dynamic_balance::request_service(
        settings,
        KafkaProducerProxyGrpcRetryLogic,
        TopicBlacklistLayer::new(blacklist_duration).layer(balanced),
    )
}

fn classify_endpoint_error(error: &crate::Error) -> Option<bool> {
    match error.downcast_ref::<KafkaProducerProxySinkError>() {
        Some(KafkaProducerProxySinkError::Request { source }) => {
            dynamic_balance::classify_grpc_error(source)
        }
        None => Some(true),
    }
}

fn new_client(
    tls_settings: &MaybeTlsSettings,
    proxy_config: &ProxyConfig,
) -> crate::Result<hyper::Client<ProxyConnector<HttpsConnector<HttpConnector>>, BoxBody>> {
    let proxy = build_proxy_connector(tls_settings.clone(), proxy_config)?;

    Ok(hyper::Client::builder().http2_only(true).build(proxy))
}

pub fn with_default_scheme(address: &str, tls: bool) -> crate::Result<Uri> {
    let uri: Uri = address.parse()?;
    if uri.scheme().is_none() {
        // Default the scheme to http or https.
        let mut parts = uri.into_parts();

        parts.scheme = if tls {
            Some(
                "https"
                    .parse()
                    .unwrap_or_else(|_| unreachable!("https should be valid")),
            )
        } else {
            Some(
                "http"
                    .parse()
                    .unwrap_or_else(|_| unreachable!("http should be valid")),
            )
        };

        if parts.path_and_query.is_none() {
            parts.path_and_query = Some(
                "/".parse()
                    .unwrap_or_else(|_| unreachable!("root should be valid")),
            );
        }
        Ok(Uri::from_parts(parts)?)
    } else {
        Ok(uri)
    }
}

#[derive(Debug, Clone)]
struct KafkaProducerProxyGrpcRetryLogic;

impl RetryLogic for KafkaProducerProxyGrpcRetryLogic {
    type Error = KafkaProducerProxySinkError;
    type Request = KPPRequest;
    type Response = KPPResponse;

    fn is_retriable_error(&self, _err: &Self::Error) -> bool {
        // All gRPC Response codes are to be treated as retriable
        true
    }

    fn should_retry_response(&self, response: &KPPResponse) -> RetryAction<Self::Request> {
        if response.all_succeeded {
            return RetryAction::Successful;
        }

        // Getting all unique error codes
        let unique_error_codes: HashSet<ResponseCodes> = response
            .results
            .iter()
            .filter_map({
                |status| {
                    if status.is_success() {
                        None
                    } else {
                        Some(*status)
                    }
                }
            })
            .collect();

        // Getting all of the retriable errors
        let retriable_errors: HashSet<ResponseCodes> = unique_error_codes
            .iter()
            .copied()
            .filter(|res_code| res_code.is_retriable_error())
            .collect();

        // This means that it only sent retryable errors
        if unique_error_codes == retriable_errors {
            return RetryAction::Retry(
                format!("Kafka Server sent Retryable Errors {:?}", retriable_errors).into(),
            );
        }

        // Getting all non-retriable errors
        let non_retriable_errors = unique_error_codes.difference(&retriable_errors);
        RetryAction::DontRetry(
            format!(
                "Kafka Server sent Non-Retryable Errors {:?}. Dropping the request",
                non_retriable_errors
            )
            .into(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinks::kafka_producer_proxy::{ResponseCodes, service::KPPResponse};
    use vector_lib::request_metadata::GroupedCountByteSize;

    #[test]
    fn test_retry_all_success() {
        let kpp_response = KPPResponse {
            event_byte_size: GroupedCountByteSize::default(),
            all_succeeded: true,
            results: vec![ResponseCodes::Success, ResponseCodes::Success],
        };
        assert!(matches!(
            KafkaProducerProxyGrpcRetryLogic.should_retry_response(&kpp_response),
            RetryAction::Successful
        ));
    }

    #[test]
    fn test_retry_partial_retry() {
        let kpp_response = KPPResponse {
            event_byte_size: GroupedCountByteSize::default(),
            all_succeeded: false,
            results: vec![
                ResponseCodes::KafkaTopicExceedsRateLimitErrorCode,
                ResponseCodes::KafkaBlacklistTopicErrorCode,
            ],
        };
        assert!(matches!(
            KafkaProducerProxyGrpcRetryLogic.should_retry_response(&kpp_response),
            RetryAction::Retry(_)
        ));
    }

    #[test]
    fn test_retry_not_retriable() {
        let kpp_response = KPPResponse {
            event_byte_size: GroupedCountByteSize::default(),
            all_succeeded: false,
            results: vec![
                ResponseCodes::KafkaErrorErrorCode,
                ResponseCodes::KafkaUnexpectedErrorErrorCode,
            ],
        };
        assert!(matches!(
            KafkaProducerProxyGrpcRetryLogic.should_retry_response(&kpp_response),
            RetryAction::DontRetry(_)
        ));
    }

    #[test]
    fn test_retry_all_types() {
        let kpp_response = KPPResponse {
            event_byte_size: GroupedCountByteSize::default(),
            all_succeeded: false,
            results: vec![
                ResponseCodes::KafkaErrorErrorCode,
                ResponseCodes::Success,
                ResponseCodes::KafkaTopicExceedsRateLimitErrorCode,
            ],
        };
        assert!(matches!(
            KafkaProducerProxyGrpcRetryLogic.should_retry_response(&kpp_response),
            RetryAction::DontRetry(_)
        ));
    }
}

#[cfg(test)]
mod balancing_tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn retriable_failures_with_long_backoff_stay_within_admission_bound() {
        for kind in 0..3 {
            dynamic_balance::tests::retry_lifecycle_with_driver::<
                KPPRequest,
                KPPResponse,
                _,
                _,
                _,
                _,
                _,
                _,
            >(
                KafkaProducerProxyGrpcRetryLogic,
                |settings, _logic, balanced| {
                    balanced_request_service(settings, balanced, Duration::from_secs(10))
                },
                || KPPResponse {
                    event_byte_size: Default::default(),
                    all_succeeded: true,
                    results: vec![ResponseCodes::Success],
                },
                move || {
                    KafkaProducerProxySinkError::Request {
                        source: match kind {
                            0 => tonic::Status::unavailable("immediate failure"),
                            1 => tonic::Status::resource_exhausted("immediate failure"),
                            _ => tonic::Status::from_error(Box::new(std::io::Error::from(
                                std::io::ErrorKind::ConnectionRefused,
                            ))),
                        },
                    }
                    .into()
                },
                classify_endpoint_error,
            )
            .await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn blacklist_above_balancer_blocks_all_pods_and_fallback_until_expiry() {
        use futures::FutureExt;
        use std::sync::{Arc, Mutex};
        use tower::{Service, ServiceExt};

        fn backend(
            id: u8,
            calls: Arc<Mutex<Vec<u8>>>,
        ) -> tower::util::BoxCloneService<KPPRequest, KPPResponse, crate::Error> {
            tower::util::BoxCloneService::new(tower::service_fn(move |_: KPPRequest| {
                let mut calls = calls.lock().unwrap();
                let blocked = calls.is_empty();
                calls.push(id);
                async move {
                    Ok(KPPResponse {
                        event_byte_size: Default::default(),
                        all_succeeded: !blocked,
                        results: vec![if blocked {
                            ResponseCodes::KafkaBlacklistTopicErrorCode
                        } else {
                            ResponseCodes::Success
                        }],
                    })
                }
            }))
        }

        for start_with_pods in [true, false] {
            let first = "127.0.0.1:80".parse().unwrap();
            let next = "127.0.0.2:80".parse().unwrap();
            let calls = Arc::new(Mutex::new(Vec::new()));
            let (tx, rx) =
                tokio::sync::watch::channel(if start_with_pods { vec![first] } else { vec![] });
            let target = dynamic_balance::DnsTarget::with_resolver(
                "http://test:80".parse().unwrap(),
                move |_| {
                    let answer = rx.borrow().clone();
                    async move { Ok(answer) }.boxed()
                },
            );
            let request: TowerRequestConfig = Default::default();
            let mut settings = request.into_settings();
            settings.concurrency = Some(1);
            settings.retry_attempts = 0;
            let pod_calls = Arc::clone(&calls);
            let balanced = dynamic_balance::balanced_service(
                settings.clone(),
                KafkaProducerProxyGrpcRetryLogic,
                LoadBalancingConfig {
                    refresh_interval_secs: 1,
                    fallback_to_address: !start_with_pods,
                    ..Default::default()
                },
                target,
                backend(0, Arc::clone(&calls)),
                move |addr| backend(if addr == first { 1 } else { 2 }, Arc::clone(&pod_calls)),
                classify_endpoint_error,
            );
            let mut service = balanced_request_service(settings, balanced, Duration::from_secs(10));
            let response = service
                .ready()
                .await
                .unwrap()
                .call(KPPRequest::default())
                .await
                .unwrap();
            assert_eq!(
                response.results,
                vec![ResponseCodes::KafkaBlacklistTopicErrorCode]
            );
            tx.send(vec![next]).unwrap();
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
            let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
            assert!(service.poll_ready(&mut cx).is_pending());
            assert_eq!(
                *calls.lock().unwrap(),
                vec![if start_with_pods { 1 } else { 0 }]
            );
            tokio::time::advance(Duration::from_secs(10)).await;
            tokio::task::yield_now().await;
            assert!(
                service
                    .ready()
                    .await
                    .unwrap()
                    .call(KPPRequest::default())
                    .await
                    .unwrap()
                    .all_succeeded
            );
            assert_eq!(
                *calls.lock().unwrap(),
                vec![if start_with_pods { 1 } else { 0 }, 2]
            );
        }
    }

    #[tokio::test]
    async fn enabled_tls_without_server_name_rejects_config() {
        let mut config =
            KafkaProducerProxyConfig::from_address("https://relay.example:6000".parse().unwrap());
        config.load_balancing.enabled = true;
        config.tls = Some(TlsEnableableConfig {
            enabled: Some(true),
            ..Default::default()
        });
        let error = match config.build(SinkContext::default()).await {
            Ok(_) => panic!("TLS pod connections require a certificate identity"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("load_balancing with TLS requires tls.server_name"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn disabled_block_preserves_serialized_config_and_bypasses_dns_validation() {
        let input = r#"address = "http://127.0.0.1:6000"
topic = "test-topic"
"#;
        let legacy: KafkaProducerProxyConfig = toml::from_str(input).unwrap();
        let omitted = toml::to_string(&legacy).unwrap();
        assert!(!omitted.contains("load_balancing"));
        let explicit: KafkaProducerProxyConfig =
            toml::from_str(&format!("{input}\n[load_balancing]\nenabled = false\n")).unwrap();
        assert_eq!(
            toml::to_string(&explicit).unwrap().as_bytes(),
            omitted.as_bytes()
        );
        let mut config = explicit;
        config.load_balancing.refresh_interval_secs = 0;
        config.load_balancing.dns_name = Some("not a DNS name ://".into());
        let (_, health) = config.build(SinkContext::default()).await.unwrap();
        health.await.unwrap();
    }
    #[test]
    fn resource_exhausted_is_backpressure_without_health_penalty() {
        for (status, expected) in [
            (tonic::Status::resource_exhausted("busy"), None),
            (tonic::Status::unavailable("down"), Some(true)),
            (tonic::Status::deadline_exceeded("late"), Some(true)),
            (tonic::Status::invalid_argument("bad request"), Some(false)),
        ] {
            let error: crate::Error =
                Box::new(KafkaProducerProxySinkError::Request { source: status });
            assert_eq!(classify_endpoint_error(&error), expected);
        }
    }
}

#[cfg(test)]
mod balanced_component_test {
    use super::*;
    use kafka_producer_proxy::proto::{
        self as proto, kafka_message, kafka_messages,
        kafka_producer_proxy_service_server::{
            KafkaProducerProxyService, KafkaProducerProxyServiceServer,
        },
    };
    use std::sync::{Arc, Mutex};
    use tokio_stream::wrappers::TcpListenerStream;

    #[derive(Clone)]
    struct Peer(Arc<Mutex<Vec<proto::KafkaMessage>>>);
    #[tonic::async_trait]
    impl KafkaProducerProxyService for Peer {
        async fn produce_message(
            &self,
            _: tonic::Request<proto::KafkaMessage>,
        ) -> Result<tonic::Response<kafka_message::Response>, tonic::Status> {
            Ok(tonic::Response::new(kafka_message::Response {
                error_code: Some(0),
                error_msg: None,
            }))
        }
        async fn produce_messages(
            &self,
            request: tonic::Request<proto::KafkaMessages>,
        ) -> Result<tonic::Response<kafka_messages::Response>, tonic::Status> {
            let messages = request.into_inner().messages;
            let responses = messages
                .iter()
                .map(|_| kafka_message::Response {
                    error_code: Some(0),
                    error_msg: None,
                })
                .collect();
            self.0.lock().unwrap().extend(messages);
            Ok(tonic::Response::new(kafka_messages::Response {
                responses,
                all_succeed: Some(true),
            }))
        }
    }
    #[tokio::test]
    async fn enabled_sink_delivers_to_dns_override_without_static_address_fallback() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let received = Arc::new(Mutex::new(Vec::new()));
        let peer = Peer(Arc::clone(&received));
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(KafkaProducerProxyServiceServer::new(peer))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        let config: KafkaProducerProxyConfig = toml::from_str(&format!(
            r#"
address = "http://no-static-endpoint.invalid:{port}"
topic = "test-topic"
[request]
retry_attempts = 0
[load_balancing]
enabled = true
dns_name = "127.0.0.1"
fallback_to_address = false
"#
        ))
        .unwrap();
        let (sink, health) = config.build(SinkContext::default()).await.unwrap();
        health.await.unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            sink.run_events(vec![crate::event::Event::Log(
                crate::event::LogEvent::from("test-message"),
            )]),
        )
        .await
        .unwrap()
        .unwrap();
        let messages = received.lock().unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].topic_name.as_deref(), Some("test-topic"));
        assert_eq!(
            messages[0].data.as_deref(),
            Some(b"test-message".as_slice())
        );
        server.abort();
    }
}
