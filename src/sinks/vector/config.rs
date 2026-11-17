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
        tls: None,
        acknowledgements: Default::default(),
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "vector")]
impl SinkConfig for VectorConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSinkType, Healthcheck)> {
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
