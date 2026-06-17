use futures::{StreamExt, stream::BoxStream};
use prost_reflect::{DescriptorPool, prost::Message};
use tower::ServiceBuilder;
use vector_lib::stream::BatcherSettings;

use crate::{
    codecs::{Encoder, Transformer},
    config::SinkContext,
    event::Event,
    sinks::util::{Compression, ServiceBuilderExt, StreamSink, builder::SinkBuilderExt},
};

use super::{
    config::BricklensIngestConfig,
    request_builder::BricklensIngestRequestBuilder,
    service::{BricklensIngestService, BricklensRetryLogic},
};

pub struct BricklensIngestSink {
    service: BricklensIngestService,
    batch_settings: BatcherSettings,
    request_limits: crate::sinks::util::TowerRequestSettings,
    compression: Compression,
}

impl BricklensIngestSink {
    pub async fn new(config: BricklensIngestConfig, _cx: SinkContext) -> crate::Result<Self> {
        // Load protobuf descriptor file
        let descriptor_bytes = std::fs::read(&config.proto_descriptor_path)
            .map_err(|e| format!("Failed to read proto descriptor file: {}", e))?;

        // Parse FileDescriptorSet and create DescriptorPool
        let file_descriptor_set =
            prost_reflect::prost_types::FileDescriptorSet::decode(&descriptor_bytes[..])
                .map_err(|e| format!("Failed to decode FileDescriptorSet: {}", e))?;

        let descriptor_pool = DescriptorPool::from_file_descriptor_set(file_descriptor_set)
            .map_err(|e| format!("Failed to build descriptor pool: {}", e))?;

        // Find the service and method
        let service_desc = descriptor_pool
            .get_service_by_name(&config.service_name)
            .ok_or_else(|| format!("Service '{}' not found in descriptor", config.service_name))?;

        let method = service_desc
            .methods()
            .find(|m| m.name() == config.method_name)
            .ok_or_else(|| format!("Method '{}' not found in service", config.method_name))?;

        // Build HTTP client with TLS configuration
        let mut http_connector = hyper::client::HttpConnector::new();
        http_connector.enforce_http(false);

        // Read TLS configuration
        let verify_cert = config
            .tls
            .as_ref()
            .and_then(|t| t.options.verify_certificate)
            .unwrap_or(true);
        let verify_hostname = config
            .tls
            .as_ref()
            .and_then(|t| t.options.verify_hostname)
            .unwrap_or(true);
        // When routing via the local s2s-proxy sidecar (endpoint = 127.0.0.3), server_name
        // overrides TLS SNI so the sidecar presents the correct backend certificate.
        let server_name: Option<String> = config
            .tls
            .as_ref()
            .and_then(|t| t.options.server_name.clone());
        let key_file: Option<std::path::PathBuf> =
            config.tls.as_ref().and_then(|t| t.options.key_file.clone());
        let crt_file: Option<std::path::PathBuf> =
            config.tls.as_ref().and_then(|t| t.options.crt_file.clone());

        // SslConnector::builder() calls set_default_verify_paths() internally, so the system CA
        // bundle is loaded automatically. When verify_certificate=true, the s2s-proxy's DigiCert
        // server cert will be verified against those system CAs.
        let mut ssl_builder = openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls())
            .map_err(|e| format!("Failed to create SSL builder: {}", e))?;

        if !verify_cert {
            ssl_builder.set_verify(openssl::ssl::SslVerifyMode::NONE);
        }

        // Load client certificate and key for mTLS. The s2s-proxy sidecar requires a client cert
        // to identify the pod when server_name routes to bricklens-ingest-internal.
        if let (Some(crt), Some(key)) = (crt_file, key_file) {
            ssl_builder
                .set_certificate_file(&crt, openssl::ssl::SslFiletype::PEM)
                .map_err(|e| format!("Failed to load client certificate {:?}: {}", crt, e))?;
            ssl_builder
                .set_private_key_file(&key, openssl::ssl::SslFiletype::PEM)
                .map_err(|e| format!("Failed to load client key {:?}: {}", key, e))?;
        }

        // Create HTTPS connector and configure hostname verification
        let mut https_connector =
            hyper_openssl::HttpsConnector::with_connector(http_connector, ssl_builder)
                .map_err(|e| format!("Failed to create HTTPS connector: {}", e))?;

        // Apply hostname verification and SNI override via callback. The callback must return
        // Result<(), openssl::error::ErrorStack> — same pattern as
        // TlsSettings::apply_connect_configuration() in vector-core/src/tls/settings.rs.
        let sni_override = server_name.clone();
        https_connector.set_callback(move |connection, _uri| {
            connection.set_verify_hostname(verify_hostname);
            if let Some(ref name) = sni_override {
                // Prevent the TLS library from inferring SNI from the endpoint URL (127.0.0.3);
                // set it explicitly to the privileged DBNS hostname for s2s-proxy routing.
                connection.set_use_server_name_indication(false);
                connection.set_hostname(name)?;
            }
            Ok(())
        });

        // Configure client for HTTP/2 (required for gRPC)
        let client = hyper::Client::builder()
            .http2_only(true)
            .build(https_connector);

        let endpoint = config
            .endpoint
            .parse()
            .map_err(|e| format!("Invalid endpoint: {}", e))?;

        let request_limits = config.request.into_settings();

        // No field extraction or enum lookups here - the VRL transform shapes the data
        // to match the proto structure. The service just blindly encodes whatever it receives.
        // The per-request timeout is forwarded to the server as a grpc-timeout header so it
        // matches the client-side Tower `Timeout` applied in run_inner().
        let service =
            BricklensIngestService::new(client, endpoint, method.clone(), request_limits.timeout);

        let batch_settings = config
            .batch
            .into_batcher_settings()
            .map_err(|e| format!("Invalid batch settings: {}", e))?;

        let compression = Compression::None; // gRPC doesn't use transport-level compression

        Ok(Self {
            service,
            batch_settings,
            request_limits,
            compression,
        })
    }

    pub fn healthcheck(&self) -> crate::sinks::Healthcheck {
        let service = self.service.clone();
        Box::pin(async move {
            // Validate that we can construct the gRPC request path.
            // We don't perform an actual network request during healthcheck because:
            // 1. The service may not be available during Vector startup
            // 2. Network connectivity issues should be handled by retries during operation
            // 3. Configuration validation (descriptor loading, service/method lookup) already
            //    happened in new(), so we've validated the most critical aspects
            service.validate_configuration()
        })
    }

    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        use vector_lib::codecs::encoding::{
            Framer, FramingConfig, JsonSerializerConfig, SerializerConfig,
        };

        // Note: The encoder is required by the RequestBuilder trait but not actually used.
        // Bricklens encoding happens in the service layer using protobuf via prost-reflect/VRL.
        let serializer = SerializerConfig::Json(JsonSerializerConfig::default())
            .build()
            .expect("Failed to build serializer");
        let framer = FramingConfig::NewlineDelimited.build();
        let encoder_inner = Encoder::<Framer>::new(framer, serializer);
        let encoder = (Transformer::default(), encoder_inner);

        let request_builder_concurrency = self
            .request_limits
            .concurrency
            .and_then(|n| std::num::NonZeroUsize::new(n))
            .unwrap_or_else(|| std::num::NonZeroUsize::new(10).unwrap());

        // Wrap the gRPC service in the Tower request-middleware stack so the configured
        // retry / timeout / rate-limit / adaptive-concurrency settings actually take effect.
        // Transient gRPC statuses (UNAVAILABLE, RESOURCE_EXHAUSTED, DEADLINE_EXCEEDED) and
        // transport errors are retried with Fibonacci backoff + jitter; permanent failures are
        // dropped. Without this wrapping the `request` settings are inert and every transient
        // failure becomes a permanent drop.
        let service = ServiceBuilder::new()
            .settings(self.request_limits, BricklensRetryLogic)
            .service(self.service);

        input
            .batched(self.batch_settings.as_byte_size_config())
            .request_builder(
                request_builder_concurrency,
                BricklensIngestRequestBuilder::new(self.compression, encoder),
            )
            .filter_map(|request| async move {
                match request {
                    Err(error) => {
                        emit!(crate::internal_events::SinkRequestBuildError { error });
                        None
                    }
                    Ok(req) => Some(req),
                }
            })
            .into_driver(service)
            .run()
            .await
    }
}

#[async_trait::async_trait]
impl StreamSink<Event> for BricklensIngestSink {
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}
