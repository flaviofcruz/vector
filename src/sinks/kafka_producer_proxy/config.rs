use std::collections::HashSet;
use std::time::Duration;

use http::Uri;
use hyper::client::HttpConnector;
use hyper_openssl::HttpsConnector;
use hyper_proxy::ProxyConnector;
use serde_with::serde_as;
use tonic::body::BoxBody;
use tower::ServiceBuilder;
use vector_lib::configurable::configurable_component;

use super::{
    KafkaProducerProxySinkError,
    blacklist::TopicBlacklistLayer,
    service::{KPPRequest, KPPResponse, KPPService},
    sink::KafkaProducerProxySink,
};
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

    #[configurable(derived)]
    #[serde(default)]
    tls: Option<TlsEnableableConfig>,

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
            tls: None,
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
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let tls = MaybeTlsSettings::from_config(self.tls.as_ref(), false)?;
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
