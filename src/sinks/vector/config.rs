use std::time::Duration;

use http::Uri;
use hyper::client::HttpConnector;
use hyper_openssl::HttpsConnector;
use hyper_proxy::ProxyConnector;
use serde::{Deserialize, Deserializer, de};
use tonic::body::BoxBody;
use tower::ServiceBuilder;
use vector_lib::configurable::configurable_component;

use super::{
    VectorSinkError,
    service::{VectorRequest, VectorResponse, VectorService},
    sink::VectorSink,
};
use crate::sinks::util::dynamic_balance::{self, LoadBalancingConfig};
use crate::sinks::util::vector_event_log::EventLoggingService;
use crate::{
    config::{
        AcknowledgementsConfig, GenerateConfig, Input, ProxyConfig, SinkConfig, SinkContext,
        SinkHealthcheckOptions,
    },
    http::build_proxy_connector_with_tcp_keepalive,
    proto::vector as proto,
    sinks::{
        Healthcheck, VectorSink as VectorSinkType,
        util::{
            BatchConfig, RealtimeEventBasedDefaultBatchSettings, ServiceBuilderExt,
            TowerRequestConfig, retries::RetryLogic,
        },
    },
    tls::{MaybeTlsSettings, TlsEnableableConfig},
};

/// Keepalive settings for the connection pool the `vector` sink uses.
///
/// All fields are unset (disabled) by default, which preserves Hyper's and the
/// OS's defaults.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct KeepAliveConfig {
    /// Send HTTP/2 PING frames at this interval to detect dead pooled connections.
    ///
    /// Unset by default.
    #[configurable(metadata(docs::type_unit = "seconds"))]
    #[configurable(metadata(docs::examples = 30))]
    #[configurable(validation(range(min = 1)))]
    http2_keep_alive_interval_secs: Option<u64>,

    /// How long to wait for an HTTP/2 PING acknowledgement before closing the connection.
    ///
    /// Used only when `http2_keep_alive_interval_secs` is set; Hyper defaults to 20 seconds.
    /// Unset by default.
    #[configurable(metadata(docs::type_unit = "seconds"))]
    #[configurable(metadata(docs::examples = 10))]
    #[configurable(validation(range(min = 1)))]
    http2_keep_alive_timeout_secs: Option<u64>,

    /// Send HTTP/2 PING frames when no streams are active.
    ///
    /// Used only when `http2_keep_alive_interval_secs` is set. Unset by default, which
    /// preserves Hyper's default (`false`).
    #[configurable(metadata(docs::examples = true))]
    http2_keep_alive_while_idle: Option<bool>,

    /// TCP keepalive idle time for the connection to the next hop, such as a local proxy.
    ///
    /// Does not configure keepalive on downstream proxy hops. Unset by default.
    #[configurable(metadata(docs::type_unit = "seconds"))]
    #[configurable(metadata(docs::examples = 60))]
    #[configurable(validation(range(min = 1)))]
    tcp_keepalive_secs: Option<u64>,
}

impl KeepAliveConfig {
    fn tcp_keepalive(&self) -> Option<Duration> {
        self.tcp_keepalive_secs.map(Duration::from_secs)
    }

    /// Applies the HTTP/2 keepalive settings to a Hyper client builder.
    ///
    /// Hyper treats the PING-ack timeout and the while-idle option as no-ops unless a
    /// PING interval is set, so unset fields always leave Hyper's defaults in place.
    fn apply_http2_keepalive(&self, builder: &mut hyper::client::Builder) {
        if let Some(interval) = self.http2_keep_alive_interval_secs {
            builder.http2_keep_alive_interval(Some(Duration::from_secs(interval)));
        }
        if let Some(timeout) = self.http2_keep_alive_timeout_secs {
            builder.http2_keep_alive_timeout(Duration::from_secs(timeout));
        }
        if let Some(while_idle) = self.http2_keep_alive_while_idle {
            builder.http2_keep_alive_while_idle(while_idle);
        }
    }
}

/// Configuration for the `vector` sink.
#[configurable_component(sink("vector", "Relay observability data to a Vector instance."))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct VectorConfig {
    /// Version of the configuration.
    // NOTE: this option is deprecated and has already been removed from the "old" docs.
    // At some point in the future we will remove it entirely as a breaking change.
    #[configurable(metadata(docs::hidden))]
    version: Option<super::VectorConfigVersion>,

    /// The downstream Vector address to which to connect.
    ///
    /// Both IP address and hostname are accepted formats.
    ///
    /// The address _must_ include a port.
    #[configurable(validation(format = "uri"))]
    #[configurable(metadata(docs::examples = "92.12.333.224:6000"))]
    #[configurable(metadata(docs::examples = "https://somehost:6000"))]
    address: String,

    /// Whether or not to compress requests.
    ///
    /// If set to `true`, requests are compressed with [`gzip`][gzip_docs].
    ///
    /// [gzip_docs]: https://www.gzip.org/
    #[configurable(metadata(docs::advanced))]
    #[serde(default, deserialize_with = "deserialize_compression")]
    compression: bool,

    /// Keepalive settings for the pooled connection to the downstream Vector instance.
    #[configurable(derived)]
    #[serde(default)]
    keepalive: KeepAliveConfig,

    #[configurable(derived)]
    #[serde(default)]
    pub batch: BatchConfig<RealtimeEventBasedDefaultBatchSettings>,

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

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub(in crate::sinks::vector) acknowledgements: AcknowledgementsConfig,
}

impl VectorConfig {
    /// Creates a `VectorConfig` with the given address.
    pub fn from_address(addr: Uri) -> Self {
        let addr = addr.to_string();
        default_config(addr.as_str())
    }
}

impl GenerateConfig for VectorConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(default_config("127.0.0.1:6000")).unwrap()
    }
}

fn default_config(address: &str) -> VectorConfig {
    VectorConfig {
        version: None,
        address: address.to_owned(),
        compression: false,
        keepalive: KeepAliveConfig::default(),
        batch: BatchConfig::default(),
        request: TowerRequestConfig::default(),
        load_balancing: LoadBalancingConfig::default(),
        tls: None,
        acknowledgements: Default::default(),
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "vector")]
impl SinkConfig for VectorConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSinkType, Healthcheck)> {
        if self.load_balancing.enabled {
            return self.build_balanced(cx).await;
        }

        let tls = MaybeTlsSettings::from_config(self.tls.as_ref(), false)?;
        let uri = with_default_scheme(&self.address, tls.is_tls())?;

        let client = new_client(&tls, cx.proxy(), self)?;

        let healthcheck_uri = cx
            .healthcheck
            .uri
            .clone()
            .map(|uri| uri.uri)
            .unwrap_or_else(|| uri.clone());
        let healthcheck_client = VectorService::new(client.clone(), healthcheck_uri, false);
        let healthcheck = healthcheck(healthcheck_client, cx.healthcheck);
        let service = VectorService::new(client, uri, self.compression);
        let request_settings = self.request.into_settings();
        let batch_settings = self.batch.into_batcher_settings()?;

        let service = ServiceBuilder::new()
            .settings(request_settings, VectorGrpcRetryLogic)
            .service(service);
        let event_log_service = EventLoggingService::new(service);

        let sink = VectorSink {
            batch_settings,
            service: event_log_service,
        };

        Ok((
            VectorSinkType::from_event_streamsink(sink),
            Box::pin(healthcheck),
        ))
    }

    fn input(&self) -> Input {
        Input::all()
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

impl VectorConfig {
    async fn build_balanced(
        &self,
        cx: SinkContext,
    ) -> crate::Result<(VectorSinkType, Healthcheck)> {
        let tls = MaybeTlsSettings::from_config(self.tls.as_ref(), false)?;
        let uri = with_default_scheme(&self.address, tls.is_tls())?;
        let target = self.load_balancing.discovery_uri(&uri)?;
        let client = dynamic_balance::new_client(&tls, self.tls.as_ref(), cx.proxy())?;
        let healthcheck = balanced_healthcheck(
            target.clone(),
            uri.clone(),
            client.clone(),
            self.load_balancing.clone(),
            cx.healthcheck,
        );
        let fallback = VectorService::new(client.clone(), uri.clone(), self.compression);
        let settings = self.request.into_settings();
        let compression = self.compression;
        let scheme = uri.scheme_str().unwrap_or("http").to_owned();
        let balanced = dynamic_balance::balanced_service(
            settings.clone(),
            VectorGrpcRetryLogic,
            self.load_balancing.clone(),
            target.into(),
            fallback,
            move |addr| {
                let endpoint = format!("{scheme}://{addr}/")
                    .parse()
                    .expect("valid pod URI");
                VectorService::new(client.clone(), endpoint, compression)
            },
            classify_endpoint_error,
        );
        let service = balanced_request_service(settings, balanced);
        let sink = VectorSink {
            batch_settings: self.batch.into_batcher_settings()?,
            service: EventLoggingService::new(service),
        };
        Ok((
            VectorSinkType::from_event_streamsink(sink),
            Box::pin(healthcheck),
        ))
    }
}

fn balanced_request_service(
    settings: crate::sinks::util::TowerRequestSettings,
    balanced: dynamic_balance::BalancedService<VectorRequest, VectorResponse>,
) -> impl tower::Service<
    VectorRequest,
    Response = VectorResponse,
    Error = crate::Error,
    Future: Send + 'static,
> + Send {
    dynamic_balance::request_service(settings, VectorGrpcRetryLogic, balanced)
}

fn classify_endpoint_error(error: &crate::Error) -> Option<bool> {
    match error.downcast_ref::<VectorSinkError>() {
        Some(VectorSinkError::Request { source }) => dynamic_balance::classify_grpc_error(source),
        Some(_) => Some(false),
        None => Some(true),
    }
}

async fn balanced_healthcheck(
    target: Uri,
    fallback: Uri,
    client: hyper::Client<ProxyConnector<HttpsConnector<HttpConnector>>, BoxBody>,
    config: LoadBalancingConfig,
    options: SinkHealthcheckOptions,
) -> crate::Result<()> {
    if !options.enabled {
        return Ok(());
    }
    let timeout = std::time::Duration::from_secs(config.refresh_interval_secs);
    let mut endpoints =
        match tokio::time::timeout(timeout, crate::sinks::util::dns::resolve_endpoints(&target))
            .await
        {
            Ok(Ok(endpoints)) => endpoints
                .into_iter()
                .filter(|uri| {
                    crate::sinks::util::dns::ip_from_uri(uri).is_some_and(|ip| ip.is_ipv4())
                })
                .collect(),
            _ => Vec::new(),
        };
    if endpoints.is_empty() && config.fallback_to_address {
        endpoints.push(fallback);
    }
    healthcheck_any(endpoints, client, options, timeout).await
}

async fn healthcheck_any(
    endpoints: Vec<Uri>,
    client: hyper::Client<ProxyConnector<HttpsConnector<HttpConnector>>, BoxBody>,
    options: SinkHealthcheckOptions,
    timeout: std::time::Duration,
) -> crate::Result<()> {
    let mut checks = futures::stream::FuturesUnordered::new();
    for endpoint in endpoints {
        let service = VectorService::new(client.clone(), endpoint, false);
        checks.push(tokio::time::timeout(
            timeout,
            healthcheck(service, options.clone()),
        ));
    }
    use futures::StreamExt;
    while let Some(result) = checks.next().await {
        if matches!(result, Ok(Ok(()))) {
            return Ok(());
        }
    }
    Err("No discovered Vector pod is serving".into())
}

/// Accepts the boolean form (`true` = gzip, `false` = none) and the string forms `"gzip"` and
/// `"none"`. The string forms match the `compression` setting in newer Vector releases, so one
/// rendered config can be loaded by either version. `"zstd"` is rejected because this build has no
/// zstd support for the `vector` sink.
fn deserialize_compression<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum CompressionSetting {
        Enabled(bool),
        Algorithm(String),
    }

    match CompressionSetting::deserialize(deserializer)? {
        CompressionSetting::Enabled(enabled) => Ok(enabled),
        CompressionSetting::Algorithm(name) => match name.as_str() {
            "gzip" => Ok(true),
            "none" => Ok(false),
            "zstd" => Err(de::Error::custom(
                "compression \"zstd\" is not supported by the vector sink in this Vector build; use \"gzip\", \"none\", or a boolean",
            )),
            other => Err(de::Error::unknown_variant(other, &["gzip", "none"])),
        },
    }
}

/// Check to see if the remote service accepts new events.
async fn healthcheck(
    mut service: VectorService,
    options: SinkHealthcheckOptions,
) -> crate::Result<()> {
    if !options.enabled {
        return Ok(());
    }

    let request = service.client.health_check(proto::HealthCheckRequest {});
    match request.await {
        Ok(response) => match proto::ServingStatus::try_from(response.into_inner().status) {
            Ok(proto::ServingStatus::Serving) => Ok(()),
            Ok(status) => Err(Box::new(VectorSinkError::Health {
                status: Some(status.as_str_name()),
            })),
            Err(_) => Err(Box::new(VectorSinkError::Health { status: None })),
        },
        Err(source) => Err(Box::new(VectorSinkError::Request { source })),
    }
}

/// grpc doesn't like an address without a scheme, so we default to http or https if one isn't
/// specified in the address.
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

fn new_client(
    tls_settings: &MaybeTlsSettings,
    proxy_config: &ProxyConfig,
    config: &VectorConfig,
) -> crate::Result<hyper::Client<ProxyConnector<HttpsConnector<HttpConnector>>, BoxBody>> {
    let proxy = build_proxy_connector_with_tcp_keepalive(
        tls_settings.clone(),
        proxy_config,
        config.keepalive.tcp_keepalive(),
    )?;

    let mut builder = hyper::Client::builder();
    builder.http2_only(true);
    config.keepalive.apply_http2_keepalive(&mut builder);

    Ok(builder.build(proxy))
}

#[derive(Debug, Clone)]
struct VectorGrpcRetryLogic;

impl RetryLogic for VectorGrpcRetryLogic {
    type Error = VectorSinkError;
    type Request = VectorRequest;
    type Response = VectorResponse;

    fn is_retriable_error(&self, err: &Self::Error) -> bool {
        use tonic::Code::*;

        match err {
            VectorSinkError::Request { source } => !matches!(
                source.code(),
                // List taken from
                //
                // <https://github.com/grpc/grpc/blob/ed1b20777c69bd47e730a63271eafc1b299f6ca0/doc/statuscodes.md>
                NotFound
                    | InvalidArgument
                    | AlreadyExists
                    | PermissionDenied
                    | OutOfRange
                    | Unimplemented
                    | Unauthenticated
                    | DataLoss
            ),
            _ => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{KeepAliveConfig, VectorConfig};

    fn compression_from(value: &str) -> Result<bool, toml::de::Error> {
        let config = format!("address = \"127.0.0.1:6000\"\ncompression = {value}\n");
        toml::from_str::<VectorConfig>(&config).map(|config| config.compression)
    }

    #[test]
    fn compression_accepts_boolean_and_string_forms() {
        assert!(compression_from("true").unwrap());
        assert!(!compression_from("false").unwrap());
        assert!(compression_from("\"gzip\"").unwrap());
        assert!(!compression_from("\"none\"").unwrap());
    }

    #[test]
    fn compression_defaults_to_disabled_when_absent() {
        let config: VectorConfig = toml::from_str("address = \"127.0.0.1:6000\"\n").unwrap();
        assert!(!config.compression);
    }

    #[test]
    fn compression_rejects_zstd_and_unknown_values() {
        let zstd = compression_from("\"zstd\"").unwrap_err().to_string();
        assert!(zstd.contains("zstd"), "{zstd}");
        let unknown = compression_from("\"snappy\"").unwrap_err().to_string();
        assert!(unknown.contains("gzip"), "{unknown}");
    }

    #[test]
    fn keepalive_settings_default_to_disabled_and_parse_optional_values() {
        let defaults: VectorConfig = toml::from_str("address = \"127.0.0.1:6000\"\n").unwrap();
        assert_eq!(defaults.keepalive, KeepAliveConfig::default());

        let configured: VectorConfig = toml::from_str(
            r#"
            address = "127.0.0.1:6000"
            [keepalive]
            http2_keep_alive_interval_secs = 30
            http2_keep_alive_timeout_secs = 10
            http2_keep_alive_while_idle = true
            tcp_keepalive_secs = 60
            "#,
        )
        .expect("optional keepalive settings should be accepted");
        let configured = toml::Value::try_from(configured).unwrap();
        assert_eq!(
            configured["keepalive"]["http2_keep_alive_interval_secs"].as_integer(),
            Some(30)
        );
        assert_eq!(
            configured["keepalive"]["http2_keep_alive_timeout_secs"].as_integer(),
            Some(10)
        );
        assert_eq!(
            configured["keepalive"]["http2_keep_alive_while_idle"].as_bool(),
            Some(true)
        );
        assert_eq!(
            configured["keepalive"]["tcp_keepalive_secs"].as_integer(),
            Some(60)
        );
    }
}

#[cfg(test)]
mod balancing_tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn retriable_failures_with_long_backoff_stay_within_admission_bound() {
        for kind in 0..3 {
            dynamic_balance::tests::retry_lifecycle_with_driver::<
                VectorRequest,
                VectorResponse,
                _,
                _,
                _,
                _,
                _,
                _,
            >(
                VectorGrpcRetryLogic,
                |settings, _logic, balanced| balanced_request_service(settings, balanced),
                VectorResponse::default,
                move || {
                    VectorSinkError::Request {
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

    use futures::{FutureExt, StreamExt};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use tokio_stream::wrappers::TcpListenerStream;
    use tower::{Service, ServiceExt};

    // Local gRPC peers are external dependencies of the balancer. The real
    // Vector client, timeout, health, ARC, discovery, Buffer and P2C all run.
    #[derive(Clone)]
    struct Peer {
        failing: Arc<AtomicBool>,
        requests: Arc<AtomicUsize>,
    }
    #[tonic::async_trait]
    impl proto::Service for Peer {
        async fn push_events(
            &self,
            _: tonic::Request<proto::PushEventsRequest>,
        ) -> Result<tonic::Response<proto::PushEventsResponse>, tonic::Status> {
            self.requests.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_millis(3)).await;
            if self.failing.load(Ordering::Relaxed) {
                Err(tonic::Status::unavailable("unhealthy pod"))
            } else {
                Ok(tonic::Response::new(proto::PushEventsResponse {}))
            }
        }
        async fn health_check(
            &self,
            _: tonic::Request<proto::HealthCheckRequest>,
        ) -> Result<tonic::Response<proto::HealthCheckResponse>, tonic::Status> {
            if self.failing.load(Ordering::Relaxed) {
                Err(tonic::Status::unavailable("unhealthy pod"))
            } else {
                Ok(tonic::Response::new(proto::HealthCheckResponse {
                    status: proto::ServingStatus::Serving.into(),
                }))
            }
        }
    }

    #[tokio::test]
    async fn unavailable_peer_receives_progressively_less_traffic_and_recovers() {
        let mut addresses = Vec::new();
        let mut peers = Vec::new();
        let mut servers = Vec::new();
        for _ in 0..3 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            addresses.push(listener.local_addr().unwrap());
            let peer = Peer {
                failing: Arc::new(AtomicBool::new(false)),
                requests: Arc::new(AtomicUsize::new(0)),
            };
            peers.push(peer.clone());
            servers.push(tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(proto::Server::new(peer))
                    .serve_with_incoming(TcpListenerStream::new(listener)),
            ));
        }
        let config = LoadBalancingConfig {
            enabled: true,
            slow_start_secs: 0,
            max_ejection_percent: 0,
            fallback_to_address: false,
            error_ewma_secs: 1,
            ..Default::default()
        };
        let uri: Uri = format!("http://{}", addresses[0]).parse().unwrap();
        let client = dynamic_balance::new_client(
            &MaybeTlsSettings::from_config(None, false).unwrap(),
            None,
            &ProxyConfig::default(),
        )
        .unwrap();
        let discovery = dynamic_balance::DnsTarget::with_resolver(uri.clone(), move |_| {
            let addresses = addresses.clone();
            async move { Ok(addresses) }.boxed()
        });
        let request: TowerRequestConfig = TowerRequestConfig::default();
        let mut settings = request.into_settings();
        settings.retry_attempts = 0;
        let client_for_pods = client.clone();
        let service = dynamic_balance::balanced_service(
            settings,
            VectorGrpcRetryLogic,
            config,
            discovery,
            VectorService::new(client, uri, false),
            move |addr| {
                VectorService::new(
                    client_for_pods.clone(),
                    format!("http://{addr}").parse().unwrap(),
                    false,
                )
            },
            classify_endpoint_error,
        );
        async fn traffic(
            service: &dynamic_balance::BalancedService<VectorRequest, VectorResponse>,
            n: usize,
        ) {
            futures::stream::iter(0..n)
                .for_each_concurrent(24, |_| {
                    let mut service = service.clone();
                    async move {
                        let _ = service
                            .ready()
                            .await
                            .unwrap()
                            .call(VectorRequest::default())
                            .await;
                    }
                })
                .await;
        }
        traffic(&service, 600).await;
        let before = peers[0].requests.load(Ordering::Relaxed);
        peers[0].failing.store(true, Ordering::Relaxed);
        traffic(&service, 600).await;
        let early = peers[0].requests.load(Ordering::Relaxed) - before;
        let before = peers[0].requests.load(Ordering::Relaxed);
        traffic(&service, 600).await;
        let late = peers[0].requests.load(Ordering::Relaxed) - before;
        assert!(early > 0, "the unavailable pod must have been exercised");
        assert!(
            late < early,
            "bad pod traffic must decrease as health degrades: early={early}, late={late}"
        );
        let good =
            peers[1].requests.load(Ordering::Relaxed) + peers[2].requests.load(Ordering::Relaxed);
        assert!(
            good > 1400,
            "healthy peers should serve most requests: {good}"
        );
        peers[0].failing.store(false, Ordering::Relaxed);
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        // Recovery must retrain ARC as well as decay the health penalty.
        traffic(&service, 600).await;
        let before = peers[0].requests.load(Ordering::Relaxed);
        traffic(&service, 600).await;
        let recovered = peers[0].requests.load(Ordering::Relaxed) - before;
        assert!(
            recovered > late,
            "idle EWMA decay must let a healthy peer recover: {recovered} <= {late}"
        );
        drop(service);
        for server in servers {
            server.abort();
        }
    }

    #[tokio::test]
    async fn healthcheck_succeeds_when_at_least_one_pod_serves() {
        let mut endpoints = Vec::new();
        let mut servers = Vec::new();
        for failing in [true, false] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            endpoints.push(
                format!("http://{}", listener.local_addr().unwrap())
                    .parse()
                    .unwrap(),
            );
            let peer = Peer {
                failing: Arc::new(AtomicBool::new(failing)),
                requests: Arc::new(AtomicUsize::new(0)),
            };
            servers.push(tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(proto::Server::new(peer))
                    .serve_with_incoming(TcpListenerStream::new(listener)),
            ));
        }
        let client = dynamic_balance::new_client(
            &MaybeTlsSettings::from_config(None, false).unwrap(),
            None,
            &ProxyConfig::default(),
        )
        .unwrap();
        let timeout = std::time::Duration::from_secs(2);
        healthcheck_any(
            endpoints.clone(),
            client.clone(),
            SinkHealthcheckOptions::default(),
            timeout,
        )
        .await
        .unwrap();
        assert!(
            healthcheck_any(
                vec![endpoints[0].clone()],
                client,
                SinkHealthcheckOptions::default(),
                timeout
            )
            .await
            .is_err()
        );
        for server in servers {
            server.abort();
        }
    }

    #[tokio::test]
    async fn tls_server_name_verifies_dns_identity_while_dialing_pod_ip() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let uri: Uri = format!("https://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let identity = tonic::transport::Identity::from_pem(
            include_bytes!(
                "../../../tests/data/ca/intermediate_server/certs/localhost-chain.cert.pem"
            ),
            include_bytes!("../../../tests/data/ca/intermediate_server/private/localhost.key.pem"),
        );
        let peer = Peer {
            failing: Arc::new(AtomicBool::new(false)),
            requests: Arc::new(AtomicUsize::new(0)),
        };
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))
                .unwrap()
                .add_service(proto::Server::new(peer))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        for (server_name, succeeds) in [("localhost", true), ("wrong.example", false)] {
            let options = TlsEnableableConfig {
                enabled: Some(true),
                options: crate::tls::TlsConfig {
                    ca_file: Some("tests/data/ca/certs/ca.cert.pem".into()),
                    server_name: Some(server_name.into()),
                    ..Default::default()
                },
            };
            let tls = MaybeTlsSettings::from_config(Some(&options), false).unwrap();
            let client =
                dynamic_balance::new_client(&tls, Some(&options), &ProxyConfig::default()).unwrap();
            let result = healthcheck(
                VectorService::new(client, uri.clone(), false),
                SinkHealthcheckOptions::default(),
            )
            .await;
            assert_eq!(
                result.is_ok(),
                succeeds,
                "identity={server_name}: {result:?}"
            );
        }
        server.abort();
    }

    #[tokio::test]
    async fn enabled_tls_without_server_name_rejects_config() {
        let mut config = VectorConfig::from_address("https://relay.example:6000".parse().unwrap());
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
        let input = r#"address = "http://127.0.0.1:6000""#;
        let legacy: VectorConfig = toml::from_str(input).unwrap();
        let omitted = toml::to_string(&legacy).unwrap();
        assert!(!omitted.contains("load_balancing"));
        let explicit: VectorConfig =
            toml::from_str(&format!("{input}\n[load_balancing]\nenabled = false\n")).unwrap();
        assert_eq!(
            toml::to_string(&explicit).unwrap().as_bytes(),
            omitted.as_bytes()
        );
        let mut config = explicit;
        config.load_balancing.refresh_interval_secs = 0;
        config.load_balancing.dns_name = Some("not a DNS name ://".into());
        let (_, health) = config.build(SinkContext::default()).await.unwrap();
        drop(health);
    }

    #[test]
    fn resource_exhausted_is_backpressure_without_health_penalty() {
        for (status, expected) in [
            (tonic::Status::resource_exhausted("busy"), None),
            (tonic::Status::unavailable("down"), Some(true)),
            (tonic::Status::deadline_exceeded("late"), Some(true)),
            (tonic::Status::invalid_argument("bad request"), Some(false)),
        ] {
            let error: crate::Error = Box::new(VectorSinkError::Request { source: status });
            assert_eq!(classify_endpoint_error(&error), expected);
        }
    }
}
