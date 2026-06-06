//! Kubernetes-aware Prometheus scrape source.
//!
//! This source watches Kubernetes pods and automatically discovers Prometheus metrics
//! endpoints by looking for pods with the named port matching the configured value.

use std::{collections::HashMap, path::PathBuf, time::Duration};

use http_1::{HeaderName, HeaderValue};
use k8s_openapi::api::core::v1::Pod;
use kube::{
    Api, Client, Config as ClientConfig,
    config::{self, KubeConfigOptions},
    runtime::{WatchStreamExt, reflector, watcher},
};
use serde_with::serde_as;
use tokio::time;
use tracing::{debug, info, warn};
use vector_lib::{config::LogNamespace, configurable::configurable_component, event::Event};

use super::{
    k8s_endpoint_provider::{
        Endpoint, EndpointProvider, K8sEndpointProvider, NamespaceAnnotationLabels,
    },
    parser,
};
use crate::kubernetes::reflector::custom_reflector;
use crate::{
    Result, SourceSender,
    built_info::{PKG_NAME, PKG_VERSION},
    config::{GenerateConfig, ProxyConfig, SourceConfig, SourceContext, SourceOutput},
    http::Auth,
    internal_events::PrometheusParseError,
    kubernetes::meta_cache::MetaCache,
    shutdown::ShutdownSignal,
    sources,
    tls::{TlsConfig, TlsSettings},
};

const DEFAULT_SCRAPE_INTERVAL_SECS: u64 = 30;
const DEFAULT_SCRAPE_TIMEOUT_SECS: f64 = 10.0;
const SELF_NODE_NAME_ENV_KEY: &str = "VECTOR_SELF_NODE_NAME";

/// Configuration for the `prometheus_k8s_scrape` source.
#[serde_as]
#[configurable_component(source(
    "prometheus_k8s_scrape",
    "Automatically discover and scrape Prometheus metrics from Kubernetes pods."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields, default)]
pub struct PrometheusK8sScrapeConfig {
    /// The name of the Kubernetes Node that is running.
    ///
    /// Configured to use an environment variable by default, to be evaluated to a value provided by
    /// Kubernetes at Pod creation.
    #[configurable(metadata(docs::examples = "${VECTOR_SELF_NODE_NAME}"))]
    #[serde(default = "default_self_node_name_env_template")]
    self_node_name: String,

    /// Specifies the field selector to filter Pods with, to be used in addition
    /// to the built-in Node filter.
    #[configurable(metadata(docs::examples = "metadata.name!=pod-name-to-exclude"))]
    extra_field_selector: String,

    /// Specifies the label selector to filter Pods with, to be used in addition to the built-in exclude filter.
    #[configurable(metadata(docs::examples = "app=myapp"))]
    extra_label_selector: String,

    /// The interval between scrapes.
    #[serde(default = "default_scrape_interval")]
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(rename = "scrape_interval_secs")]
    #[configurable(metadata(docs::human_name = "Scrape Interval"))]
    scrape_interval: Duration,

    /// The timeout for each scrape request.
    #[serde(default = "default_scrape_timeout")]
    #[serde_as(as = "serde_with::DurationSecondsWithFrac<f64>")]
    #[serde(rename = "scrape_timeout_secs")]
    #[configurable(metadata(docs::human_name = "Scrape Timeout"))]
    scrape_timeout: Duration,

    /// Optional path to a readable kubeconfig file.
    ///
    /// If not set, a connection to Kubernetes is made using the in-cluster configuration.
    #[configurable(metadata(docs::examples = "/path/to/.kube/config"))]
    kube_config_file: Option<PathBuf>,

    /// Determines if requests to the kube-apiserver can be served by a cache.
    #[serde(default = "default_use_apiserver_cache")]
    use_apiserver_cache: bool,

    /// The tag name added to each metric representing the scraped Pod's name.
    #[configurable(metadata(docs::advanced))]
    pod_name_tag: Option<String>,

    /// The tag name added to each metric representing the scraped Pod's namespace.
    #[configurable(metadata(docs::advanced))]
    pod_namespace_tag: Option<String>,

    /// Controls how tag conflicts are handled if the scraped source has tags to be added.
    #[serde(default = "crate::serde::default_false")]
    #[configurable(metadata(docs::advanced))]
    honor_labels: bool,

    /// The name of the container port to look for when discovering pods.
    ///
    /// Only pods with a container port matching this name will be scraped.
    /// Set to `null` to disable named-port-based discovery entirely.
    #[serde(default = "default_named_port")]
    #[configurable(metadata(docs::examples = "metrics", docs::examples = "prometheus"))]
    named_port: Option<String>,

    /// The pod annotation key used for annotation-based endpoint discovery.
    ///
    /// Pods carrying this annotation will be scraped on the port number(s) given as
    /// the annotation value. The value may be a single port (e.g. `"9091"`) or a
    /// comma-separated list (e.g. `"9091,9092"`), in which case one endpoint is
    /// produced per unique valid port; whitespace around each entry is ignored,
    /// duplicate ports are collapsed (first-seen wins), and unparseable entries
    /// are silently skipped. Set to `null` to disable annotation-based discovery.
    #[serde(default = "default_annotation_name")]
    #[configurable(metadata(docs::examples = "system_metrics_enabled"))]
    annotation_name: Option<String>,

    /// The maximum number of scrape endpoints produced per pod.
    ///
    /// Caps how many endpoints a single pod can contribute via annotation-based
    /// discovery (the named-port path already produces at most one endpoint per
    /// pod, so the effective per-pod ceiling is `max_endpoints_per_pod + 1`
    /// when both paths fire on distinct ports). Successfully parsed annotation
    /// ports beyond this limit are silently dropped (a warning is logged at
    /// most once per minute per scrape). This guards against accidental or
    /// malicious annotations that would otherwise generate a large number of
    /// scrape targets.
    #[serde(default = "default_max_endpoints_per_pod")]
    #[configurable(metadata(docs::advanced))]
    max_endpoints_per_pod: usize,

    /// Per-namespace mapping of pod annotation keys to label names.
    ///
    /// Outer key is the pod's namespace (matched exactly). Inner map is
    /// annotation key → label name: each listed annotation that is present on
    /// the pod is added to the scraped metric as `label_name=<annotation
    /// value>` (subject to `honor_labels` and `emit_pod_metadata`).
    /// Annotations that are absent contribute no label. When the map is empty
    /// (the default), no annotation-sourced labels are added.
    #[serde(default)]
    namespace_annotation_labels: HashMap<String, HashMap<String, String>>,

    /// Controls whether to add pod metadata (pod_name, pod_namespace, endpoint) to scraped metrics.
    ///
    /// When false, no Kubernetes metadata tags will be added to the metrics.
    #[serde(default = "crate::serde::default_true")]
    #[configurable(metadata(docs::advanced))]
    emit_pod_metadata: bool,

    #[configurable(derived)]
    tls: Option<TlsConfig>,

    #[configurable(derived)]
    #[configurable(metadata(docs::advanced))]
    auth: Option<Auth>,
}

impl Default for PrometheusK8sScrapeConfig {
    fn default() -> Self {
        Self {
            self_node_name: default_self_node_name_env_template(),
            extra_field_selector: String::new(),
            extra_label_selector: String::new(),
            scrape_interval: default_scrape_interval(),
            scrape_timeout: default_scrape_timeout(),
            kube_config_file: None,
            use_apiserver_cache: default_use_apiserver_cache(),
            pod_name_tag: Some("pod_name".to_string()),
            pod_namespace_tag: Some("pod_namespace".to_string()),
            honor_labels: false,
            named_port: default_named_port(),
            annotation_name: default_annotation_name(),
            max_endpoints_per_pod: default_max_endpoints_per_pod(),
            namespace_annotation_labels: HashMap::new(),
            emit_pod_metadata: true,
            tls: None,
            auth: None,
        }
    }
}

impl GenerateConfig for PrometheusK8sScrapeConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self::default()).unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "prometheus_k8s_scrape")]
impl SourceConfig for PrometheusK8sScrapeConfig {
    async fn build(&self, cx: SourceContext) -> Result<sources::Source> {
        let self_node_name = resolve_self_node_name(&self.self_node_name)?;

        let field_selector = prepare_field_selector(&self.extra_field_selector, &self_node_name)?;
        let label_selector = prepare_label_selector(&self.extra_label_selector);

        // Setup Kubernetes client
        let mut client_config = match &self.kube_config_file {
            Some(kc) => {
                ClientConfig::from_custom_kubeconfig(
                    config::Kubeconfig::read_from(kc)?,
                    &KubeConfigOptions::default(),
                )
                .await?
            }
            None => ClientConfig::infer().await?,
        };

        if let Ok(user_agent) = HeaderValue::from_str(&format!("{PKG_NAME}/{PKG_VERSION}")) {
            client_config
                .headers
                .push((HeaderName::from_static("user-agent"), user_agent));
        }

        let client = Client::try_from(client_config)?;
        let tls = TlsSettings::from_options(self.tls.as_ref())?;

        let config = self.clone();

        Ok(Box::pin(async move {
            run_source(
                client,
                field_selector,
                label_selector,
                config.use_apiserver_cache,
                config.scrape_interval,
                config.scrape_timeout,
                config.pod_name_tag,
                config.pod_namespace_tag,
                config.honor_labels,
                config.named_port,
                config.annotation_name,
                config.max_endpoints_per_pod,
                config.namespace_annotation_labels,
                config.emit_pod_metadata,
                config.auth,
                tls,
                cx.proxy,
                cx.out,
                cx.shutdown,
            )
            .await
            .map_err(|e| {
                error!(message = "Identifying pods with named port failed", error = ?e);
            })
        }))
    }

    fn outputs(&self, _global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        vec![SourceOutput::new_metrics()]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

async fn run_source(
    client: Client,
    field_selector: String,
    label_selector: String,
    use_apiserver_cache: bool,
    scrape_interval: Duration,
    scrape_timeout: Duration,
    pod_name_tag: Option<String>,
    pod_namespace_tag: Option<String>,
    honor_labels: bool,
    named_port: Option<String>,
    annotation_name: Option<String>,
    max_endpoints_per_pod: usize,
    namespace_annotation_labels: NamespaceAnnotationLabels,
    emit_pod_metadata: bool,
    auth: Option<Auth>,
    tls: TlsSettings,
    proxy: ProxyConfig,
    out: SourceSender,
    shutdown: ShutdownSignal,
) -> Result<()> {
    info!(message = "Starting Kubernetes Prometheus scrape source.");

    // Setup pod watcher
    let pods = Api::<Pod>::all(client);

    let list_semantic = if use_apiserver_cache {
        watcher::ListSemantic::Any
    } else {
        watcher::ListSemantic::MostRecent
    };

    let pod_watcher = watcher(
        pods,
        watcher::Config {
            field_selector: Some(field_selector),
            label_selector: Some(label_selector),
            list_semantic,
            ..Default::default()
        },
    )
    .backoff(watcher::DefaultBackoff::default());

    let pod_store_w = reflector::store::Writer::default();
    let pod_state = pod_store_w.as_reader();
    let pod_cacher = MetaCache::new();

    // Spawn the reflector to keep pod state updated
    let reflector_handle = tokio::spawn(custom_reflector(
        pod_store_w,
        pod_cacher,
        pod_watcher,
        Duration::from_secs(60), // delay deletion
    ));

    // Create endpoint provider
    let endpoint_provider = K8sEndpointProvider::new(
        pod_state,
        named_port,
        annotation_name,
        max_endpoints_per_pod,
        namespace_annotation_labels,
    );

    // Run the scraping loop
    let scrape_result = scrape_loop(
        endpoint_provider,
        scrape_interval,
        scrape_timeout,
        pod_name_tag,
        pod_namespace_tag,
        honor_labels,
        emit_pod_metadata,
        auth,
        tls,
        proxy,
        out,
        shutdown,
    )
    .await;

    // Cleanup
    reflector_handle.abort();

    info!(message = "Kubernetes Prometheus scrape source stopped.");
    scrape_result
}

async fn scrape_loop(
    endpoint_provider: K8sEndpointProvider,
    scrape_interval: Duration,
    scrape_timeout: Duration,
    pod_name_tag: Option<String>,
    pod_namespace_tag: Option<String>,
    honor_labels: bool,
    emit_pod_metadata: bool,
    auth: Option<Auth>,
    tls: TlsSettings,
    proxy: ProxyConfig,
    mut out: SourceSender,
    mut shutdown: ShutdownSignal,
) -> Result<()> {
    let mut interval = time::interval(scrape_interval);
    let client = http_client::build_client(&tls, &proxy)?;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let endpoints = endpoint_provider.endpoints();

                if endpoints.is_empty() {
                    info!(message = "No endpoints discovered from Kubernetes pods as there are no pods with configured named port", internal_log_rate_secs = 300);
                    continue;
                }

                info!(message = "Scraping endpoints from pods.", count = endpoints.len(), ?endpoints, internal_log_rate_secs = 300);

                // Scrape all endpoints concurrently
                let scrape_futures: Vec<_> = endpoints
                    .into_iter()
                    .map(|endpoint| {
                        let client = client.clone();
                        let auth = auth.clone();
                        let pod_name_tag = pod_name_tag.clone();
                        let pod_namespace_tag = pod_namespace_tag.clone();

                        async move {
                            scrape_endpoint(
                                endpoint,
                                client,
                                scrape_timeout,
                                auth,
                                pod_name_tag,
                                pod_namespace_tag,
                                honor_labels,
                                emit_pod_metadata,
                            )
                            .await
                        }
                    })
                    .collect();

                let results = futures::future::join_all(scrape_futures).await;

                // Send all scraped metrics
                for result in results {
                    match result {
                        Ok(events) => {
                            if let Err(e) = out.send_batch(events).await {
                                warn!(
                                    message = "Error sending metrics to remote endpoint (prometheus cluster), check the remote endpoint configuration",
                                    error = ?e,
                                    internal_log_rate_secs = 60
                                );
                            }
                        }
                        Err(e) => {
                            warn!(
                                message = "Error scraping endpoint, likely the application pod's named port is not accessible",
                                error = ?e,
                                internal_log_rate_secs = 60
                            );
                        }
                    }
                }
            }
            _ = &mut shutdown => {
                info!(message = "Shutdown signal received.");
                break;
            }
        }
    }

    Ok(())
}

async fn scrape_endpoint(
    endpoint: Endpoint,
    client: http_client::HttpClient,
    timeout: Duration,
    auth: Option<Auth>,
    pod_name_tag: Option<String>,
    pod_namespace_tag: Option<String>,
    honor_labels: bool,
    emit_pod_metadata: bool,
) -> Result<Vec<Event>> {
    use http_body::Body as _;
    use hyper::{Body, Request};

    debug!(message = "Scraping endpoint.", endpoint = %endpoint.url, pod = %endpoint.name, namespace = %endpoint.namespace);

    let uri: hyper::Uri = endpoint.url.parse()?;
    let mut request_builder = Request::get(uri);

    // Apply authentication if configured
    if let Some(ref auth_config) = auth {
        request_builder = apply_auth_headers(request_builder, auth_config)?;
    }

    let request = request_builder.body(Body::empty())?;

    let response = tokio::time::timeout(timeout, client.request(request))
        .await
        .map_err(|_| format!("Request timeout: {}", endpoint.url))?
        .map_err(|e| {
            warn!(
                message = "Failed to scrape endpoint.",
                endpoint = %endpoint.url,
                pod = %endpoint.name,
                namespace = %endpoint.namespace,
                error = ?e,
                internal_log_rate_secs = 60
            );
            e
        })?;

    let body_bytes = response.into_body().collect().await?.to_bytes();
    let body_str = std::str::from_utf8(body_bytes.as_ref())
        .map_err(|e| format!("Invalid UTF-8 in response body: {}", e))?;

    // Parse Prometheus metrics
    let events = parser::parse_text(body_str)
        .map_err(|error| {
            emit!(PrometheusParseError {
                error,
                url: endpoint
                    .url
                    .parse()
                    .unwrap_or_else(|_| http::Uri::default()),
                body: String::from_utf8_lossy(body_bytes.as_ref())
            });
        })
        .unwrap_or_default();

    // Add pod metadata tags if configured
    let events: Vec<Event> = if emit_pod_metadata {
        events
            .into_iter()
            .map(|mut event| {
                if let Event::Metric(ref mut metric) = event {
                    // Use pod name and namespace from the Endpoint struct
                    if let Some(ref tag) = pod_name_tag {
                        if honor_labels && metric.tags().and_then(|t| t.get(tag)).is_some() {
                            // Skip if honor_labels is true and tag already exists
                        } else {
                            metric.replace_tag(tag.clone(), endpoint.name.clone());
                        }
                    }

                    if let Some(ref tag) = pod_namespace_tag {
                        if honor_labels && metric.tags().and_then(|t| t.get(tag)).is_some() {
                            // Skip if honor_labels is true and tag already exists
                        } else {
                            metric.replace_tag(tag.clone(), endpoint.namespace.clone());
                        }
                    }

                    for (label_name, value) in &endpoint.extra_labels {
                        if honor_labels
                            && metric.tags().and_then(|t| t.get(label_name.as_str())).is_some()
                        {
                            // honor_labels: scrape-target label wins
                        } else {
                            metric.replace_tag(label_name.clone(), value.clone());
                        }
                    }

                    // Add endpoint URL as a tag
                    metric.replace_tag("scrape_endpoint".to_string(), endpoint.url.clone());
                }
                event
            })
            .collect()
    } else {
        // Return events without adding any metadata
        events
    };

    Ok(events)
}

fn apply_auth_headers(
    builder: hyper::http::request::Builder,
    auth: &Auth,
) -> Result<hyper::http::request::Builder> {
    use base64::prelude::{BASE64_STANDARD, Engine as _};
    use hyper::header::{AUTHORIZATION, HeaderValue};

    match auth {
        Auth::Basic { user, password } => {
            let credentials = format!("{}:{}", user, password.inner());
            let encoded = BASE64_STANDARD.encode(credentials);
            let header_value = HeaderValue::from_str(&format!("Basic {}", encoded))?;
            Ok(builder.header(AUTHORIZATION, header_value))
        }
        Auth::Bearer { token } => {
            let header_value = HeaderValue::from_str(&format!("Bearer {}", token))?;
            Ok(builder.header(AUTHORIZATION, header_value))
        }
        Auth::Aws { .. } => {
            // AWS auth is not supported for direct HTTP scraping
            // This would require AWS SigV4 signing which is complex
            warn!(message = "AWS authentication is not supported for Kubernetes pod scraping.");
            Ok(builder)
        }
        Auth::Custom { value } => {
            let header_value = HeaderValue::from_str(value)?;
            Ok(builder.header(AUTHORIZATION, header_value))
        }
    }
}

// Helper functions

fn default_self_node_name_env_template() -> String {
    format!("${{{SELF_NODE_NAME_ENV_KEY}}}")
}

fn resolve_self_node_name(configured: &str) -> Result<String> {
    if configured.is_empty() || configured == default_self_node_name_env_template() {
        std::env::var(SELF_NODE_NAME_ENV_KEY).map_err(|_| {
            format!("self_node_name config value or {SELF_NODE_NAME_ENV_KEY} env var is not set")
                .into()
        })
    } else {
        Ok(configured.to_string())
    }
}

fn prepare_field_selector(extra: &str, self_node_name: &str) -> Result<String> {
    let field_selector = format!("spec.nodeName={self_node_name}");

    if extra.is_empty() {
        Ok(field_selector)
    } else {
        Ok(format!("{field_selector},{extra}"))
    }
}

fn prepare_label_selector(extra: &str) -> String {
    const BUILT_IN: &str = "";

    if BUILT_IN.is_empty() {
        extra.to_string()
    } else if extra.is_empty() {
        BUILT_IN.to_string()
    } else {
        format!("{BUILT_IN},{extra}")
    }
}

fn default_scrape_interval() -> Duration {
    Duration::from_secs(DEFAULT_SCRAPE_INTERVAL_SECS)
}

fn default_scrape_timeout() -> Duration {
    Duration::from_secs_f64(DEFAULT_SCRAPE_TIMEOUT_SECS)
}

fn default_use_apiserver_cache() -> bool {
    false
}

fn default_named_port() -> Option<String> {
    Some("user-metrics".to_string())
}

fn default_annotation_name() -> Option<String> {
    Some("system_metrics_enabled".to_string())
}

fn default_max_endpoints_per_pod() -> usize {
    2
}

mod http_client {
    use crate::{
        Result,
        config::ProxyConfig,
        http::build_tls_connector,
        tls::{MaybeTlsSettings, TlsSettings},
    };
    use hyper::{Body, Client, client::HttpConnector};
    use hyper_openssl::HttpsConnector;

    pub type HttpClient = Client<HttpsConnector<HttpConnector>, Body>;

    pub fn build_client(tls: &TlsSettings, _proxy: &ProxyConfig) -> Result<HttpClient> {
        let tls_settings = MaybeTlsSettings::Tls(tls.clone());
        let connector = build_tls_connector(tls_settings)?;
        Ok(Client::builder().build(connector))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::{
        Body, Response, Server,
        service::{make_service_fn, service_fn},
    };
    use std::collections::BTreeMap;
    use std::convert::Infallible;
    use std::net::SocketAddr;
    use tokio::sync::oneshot;

    #[test]
    fn test_generate_config() {
        crate::test_util::test_generate_config::<PrometheusK8sScrapeConfig>();
    }

    #[test]
    fn test_prepare_field_selector() {
        assert_eq!(
            prepare_field_selector("", "my-node").unwrap(),
            "spec.nodeName=my-node"
        );
        assert_eq!(
            prepare_field_selector("metadata.name=my-pod", "my-node").unwrap(),
            "spec.nodeName=my-node,metadata.name=my-pod"
        );
    }

    #[test]
    fn test_prepare_label_selector() {
        assert_eq!(prepare_label_selector(""), "");
        assert_eq!(prepare_label_selector("app=myapp"), "app=myapp");
    }

    #[tokio::test]
    async fn test_scrape_endpoint_success() {
        // Start a test HTTP server that returns Prometheus metrics
        let (tx, rx) = oneshot::channel();

        let make_svc = make_service_fn(|_conn| async {
            Ok::<_, Infallible>(service_fn(|_req| async {
                let body = r#"# HELP test_counter A test counter
# TYPE test_counter counter
test_counter{label="value"} 42.0
# HELP test_gauge A test gauge
# TYPE test_gauge gauge
test_gauge 3.14
"#;
                Ok::<_, Infallible>(Response::new(Body::from(body)))
            }))
        });

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = Server::bind(&addr).serve(make_svc);
        let addr = server.local_addr();

        // Spawn server in background
        tokio::spawn(async move {
            let server = server.with_graceful_shutdown(async {
                rx.await.ok();
            });
            server.await.ok();
        });

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create HTTP client
        let tls = TlsSettings::default();
        let proxy = ProxyConfig::default();
        let client = http_client::build_client(&tls, &proxy).unwrap();

        // Test scraping
        let endpoint = Endpoint {
            url: format!("http://{}/metrics", addr),
            name: "test-pod".to_string(),
            namespace: "test-namespace".to_string(),
            extra_labels: BTreeMap::new(),
        };
        let result = scrape_endpoint(
            endpoint.clone(),
            client,
            Duration::from_secs(5),
            None,
            Some("pod_name".to_string()),
            Some("pod_namespace".to_string()),
            false,
            true,
        )
        .await;

        // Shutdown server
        tx.send(()).ok();

        assert!(result.is_ok());
        let events = result.unwrap();
        assert!(!events.is_empty());

        // Verify that events are metrics
        for event in &events {
            assert!(matches!(event, Event::Metric(_)));
            if let Event::Metric(metric) = event {
                // Verify endpoint tag was added
                assert!(metric.tags().is_some());
                let tags = metric.tags().unwrap();
                assert!(tags.contains_key("scrape_endpoint"));
                assert_eq!(tags.get("scrape_endpoint").unwrap(), &endpoint.url);
            }
        }
    }

    #[tokio::test]
    async fn test_scrape_endpoint_with_timeout() {
        // Start a test HTTP server that delays response
        let (tx, rx) = oneshot::channel();

        let make_svc = make_service_fn(|_conn| async {
            Ok::<_, Infallible>(service_fn(|_req| async {
                // Delay longer than the timeout
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok::<_, Infallible>(Response::new(Body::from("too late")))
            }))
        });

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = Server::bind(&addr).serve(make_svc);
        let addr = server.local_addr();

        tokio::spawn(async move {
            let server = server.with_graceful_shutdown(async {
                rx.await.ok();
            });
            server.await.ok();
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let tls = TlsSettings::default();
        let proxy = ProxyConfig::default();
        let client = http_client::build_client(&tls, &proxy).unwrap();

        let endpoint = Endpoint {
            url: format!("http://{}/metrics", addr),
            name: "test-pod".to_string(),
            namespace: "test-namespace".to_string(),
            extra_labels: BTreeMap::new(),
        };
        let result = scrape_endpoint(
            endpoint,
            client,
            Duration::from_millis(100), // Very short timeout
            None,
            None,
            None,
            false,
            false,
        )
        .await;

        tx.send(()).ok();

        // Should fail with timeout error
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_scrape_endpoint_without_metadata() {
        let (tx, rx) = oneshot::channel();

        let make_svc = make_service_fn(|_conn| async {
            Ok::<_, Infallible>(service_fn(|_req| async {
                let body = "test_metric 100\n";
                Ok::<_, Infallible>(Response::new(Body::from(body)))
            }))
        });

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = Server::bind(&addr).serve(make_svc);
        let addr = server.local_addr();

        tokio::spawn(async move {
            let server = server.with_graceful_shutdown(async {
                rx.await.ok();
            });
            server.await.ok();
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let tls = TlsSettings::default();
        let proxy = ProxyConfig::default();
        let client = http_client::build_client(&tls, &proxy).unwrap();

        let endpoint = Endpoint {
            url: format!("http://{}/metrics", addr),
            name: "test-pod".to_string(),
            namespace: "test-namespace".to_string(),
            extra_labels: BTreeMap::new(),
        };
        let result = scrape_endpoint(
            endpoint,
            client,
            Duration::from_secs(5),
            None,
            Some("pod_name".to_string()),
            Some("pod_namespace".to_string()),
            false,
            false, // emit_pod_metadata = false
        )
        .await;

        tx.send(()).ok();

        assert!(result.is_ok());
        let events = result.unwrap();

        // Verify no pod metadata tags were added (only inherent metric tags)
        for event in &events {
            if let Event::Metric(metric) = event {
                let tags = metric.tags();
                // When emit_pod_metadata is false, endpoint/pod tags should not be added
                // unless they were part of the original metric
                if let Some(tags) = tags {
                    assert!(!tags.contains_key("scrape_endpoint"));
                }
            }
        }
    }

    #[tokio::test]
    async fn test_scrape_endpoint_honor_labels() {
        let (tx, rx) = oneshot::channel();

        let make_svc = make_service_fn(|_conn| async {
            Ok::<_, Infallible>(service_fn(|_req| async {
                // Metric already has pod_name label
                let body = r#"test_metric{pod_name="original_name"} 100"#;
                Ok::<_, Infallible>(Response::new(Body::from(body)))
            }))
        });

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = Server::bind(&addr).serve(make_svc);
        let addr = server.local_addr();

        tokio::spawn(async move {
            let server = server.with_graceful_shutdown(async {
                rx.await.ok();
            });
            server.await.ok();
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let tls = TlsSettings::default();
        let proxy = ProxyConfig::default();
        let client = http_client::build_client(&tls, &proxy).unwrap();

        let endpoint = Endpoint {
            url: format!("http://{}/metrics", addr),
            name: "test-pod".to_string(),
            namespace: "test-namespace".to_string(),
            extra_labels: BTreeMap::new(),
        };
        let result = scrape_endpoint(
            endpoint,
            client,
            Duration::from_secs(5),
            None,
            Some("pod_name".to_string()),
            None,
            true, // honor_labels = true
            true,
        )
        .await;

        tx.send(()).ok();

        assert!(result.is_ok());
        let events = result.unwrap();

        // Verify original label was preserved
        for event in &events {
            if let Event::Metric(metric) = event {
                if let Some(tags) = metric.tags() {
                    if let Some(pod_name) = tags.get("pod_name") {
                        // Original label should be preserved when honor_labels is true
                        assert_eq!(pod_name, "original_name");
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn test_emit_pod_metadata_enabled() {
        // Test that when emit_pod_metadata is true, all metadata tags are added
        let (tx, rx) = oneshot::channel();

        let make_svc = make_service_fn(|_conn| async {
            Ok::<_, Infallible>(service_fn(|_req| async {
                let body = r#"# TYPE http_requests_total counter
http_requests_total{method="GET",status="200"} 1234
"#;
                Ok::<_, Infallible>(Response::new(Body::from(body)))
            }))
        });

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = Server::bind(&addr).serve(make_svc);
        let addr = server.local_addr();

        tokio::spawn(async move {
            let server = server.with_graceful_shutdown(async {
                rx.await.ok();
            });
            server.await.ok();
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let tls = TlsSettings::default();
        let proxy = ProxyConfig::default();
        let client = http_client::build_client(&tls, &proxy).unwrap();

        let endpoint = Endpoint {
            url: format!("http://{}/metrics", addr),
            name: "test-pod".to_string(),
            namespace: "test-namespace".to_string(),
            extra_labels: BTreeMap::new(),
        };
        let result = scrape_endpoint(
            endpoint.clone(),
            client,
            Duration::from_secs(5),
            None,
            Some("pod_name".to_string()),
            Some("pod_namespace".to_string()),
            false,
            true, // emit_pod_metadata = true
        )
        .await;

        tx.send(()).ok();

        assert!(result.is_ok());
        let events = result.unwrap();
        assert!(!events.is_empty());

        // Verify all pod metadata tags are present
        for event in &events {
            if let Event::Metric(metric) = event {
                let tags = metric.tags();
                assert!(tags.is_some(), "Metric should have tags");

                let tags = tags.unwrap();

                // Should have endpoint tag
                assert!(
                    tags.contains_key("scrape_endpoint"),
                    "Metric should have 'endpoint_url' tag when emit_pod_metadata is true"
                );
                assert_eq!(tags.get("scrape_endpoint").unwrap(), &endpoint.url);

                // Should have pod_name tag
                assert!(
                    tags.contains_key("pod_name"),
                    "Metric should have 'pod_name' tag when emit_pod_metadata is true"
                );
                assert_eq!(tags.get("pod_name").unwrap(), &endpoint.name);

                // Should have pod_namespace tag
                assert!(
                    tags.contains_key("pod_namespace"),
                    "Metric should have 'pod_namespace' tag when emit_pod_metadata is true"
                );
                assert_eq!(tags.get("pod_namespace").unwrap(), &endpoint.namespace);

                // Original metric tags should still be present
                assert_eq!(tags.get("method").unwrap(), "GET");
                assert_eq!(tags.get("status").unwrap(), "200");
            }
        }
    }

    #[tokio::test]
    async fn test_emit_pod_metadata_disabled() {
        // Test that when emit_pod_metadata is false, no metadata tags are added
        let (tx, rx) = oneshot::channel();

        let make_svc = make_service_fn(|_conn| async {
            Ok::<_, Infallible>(service_fn(|_req| async {
                let body = r#"# TYPE http_requests_total counter
http_requests_total{method="POST",status="201"} 5678
"#;
                Ok::<_, Infallible>(Response::new(Body::from(body)))
            }))
        });

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = Server::bind(&addr).serve(make_svc);
        let addr = server.local_addr();

        tokio::spawn(async move {
            let server = server.with_graceful_shutdown(async {
                rx.await.ok();
            });
            server.await.ok();
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let tls = TlsSettings::default();
        let proxy = ProxyConfig::default();
        let client = http_client::build_client(&tls, &proxy).unwrap();

        let endpoint = Endpoint {
            url: format!("http://{}/metrics", addr),
            name: "test-pod".to_string(),
            namespace: "test-namespace".to_string(),
            extra_labels: BTreeMap::new(),
        };
        let result = scrape_endpoint(
            endpoint,
            client,
            Duration::from_secs(5),
            None,
            Some("pod_name".to_string()),
            Some("pod_namespace".to_string()),
            false,
            false, // emit_pod_metadata = false
        )
        .await;

        tx.send(()).ok();

        assert!(result.is_ok());
        let events = result.unwrap();
        assert!(!events.is_empty());

        // Verify NO pod metadata tags are added
        for event in &events {
            if let Event::Metric(metric) = event {
                let tags = metric.tags();

                if let Some(tags) = tags {
                    // Should NOT have endpoint tag
                    assert!(
                        !tags.contains_key("scrape_endpoint"),
                        "Metric should NOT have 'endpoint_url' tag when emit_pod_metadata is false"
                    );

                    // Should NOT have pod_name tag (unless it was in the original metric)
                    // Since our test metric doesn't have pod_name originally, it shouldn't be there
                    assert!(
                        !tags.contains_key("pod_name"),
                        "Metric should NOT have 'pod_name' tag when emit_pod_metadata is false"
                    );

                    // Should NOT have pod_namespace tag
                    assert!(
                        !tags.contains_key("pod_namespace"),
                        "Metric should NOT have 'pod_namespace' tag when emit_pod_metadata is false"
                    );

                    // Original metric tags should still be present
                    assert_eq!(tags.get("method").unwrap(), "POST");
                    assert_eq!(tags.get("status").unwrap(), "201");
                }
            }
        }
    }

    /// Helper to spin up a one-shot HTTP server returning a fixed body and
    /// return its listening address and shutdown trigger.
    async fn spawn_metrics_server(body: &'static str) -> (SocketAddr, oneshot::Sender<()>) {
        let (tx, rx) = oneshot::channel();

        let make_svc = make_service_fn(move |_conn| async move {
            Ok::<_, Infallible>(service_fn(move |_req| async move {
                Ok::<_, Infallible>(Response::new(Body::from(body)))
            }))
        });

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = Server::bind(&addr).serve(make_svc);
        let addr = server.local_addr();

        tokio::spawn(async move {
            let server = server.with_graceful_shutdown(async {
                rx.await.ok();
            });
            server.await.ok();
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
        (addr, tx)
    }

    fn endpoint_with_labels(addr: SocketAddr, labels: &[(&str, &str)]) -> Endpoint {
        Endpoint {
            url: format!("http://{}/metrics", addr),
            name: "test-pod".to_string(),
            namespace: "test-namespace".to_string(),
            extra_labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    /// When the endpoint carries `extra_labels`, scrape_endpoint emits each as
    /// a tag on every metric (subject to emit_pod_metadata being true).
    #[tokio::test]
    async fn test_scrape_endpoint_emits_extra_label_tag_when_present() {
        let (addr, tx) = spawn_metrics_server(
            "# TYPE http_requests_total counter\nhttp_requests_total{method=\"GET\"} 1\n",
        )
        .await;

        let tls = TlsSettings::default();
        let proxy = ProxyConfig::default();
        let client = http_client::build_client(&tls, &proxy).unwrap();

        let endpoint = endpoint_with_labels(addr, &[("tenant", "alpha")]);
        let result = scrape_endpoint(
            endpoint,
            client,
            Duration::from_secs(5),
            None,
            Some("pod_name".to_string()),
            Some("pod_namespace".to_string()),
            false,
            true,
        )
        .await;

        tx.send(()).ok();

        let events = result.unwrap();
        assert!(!events.is_empty());
        for event in &events {
            if let Event::Metric(metric) = event {
                let tags = metric.tags().expect("metric should have tags");
                assert_eq!(
                    tags.get("tenant"),
                    Some("alpha"),
                    "configured label should be present and match the annotation value"
                );
            }
        }
    }

    /// When `extra_labels` is empty, scrape_endpoint must not add any of the
    /// label names the user might have configured elsewhere.
    #[tokio::test]
    async fn test_scrape_endpoint_no_extra_label_tag_when_absent() {
        let (addr, tx) = spawn_metrics_server(
            "# TYPE http_requests_total counter\nhttp_requests_total{method=\"GET\"} 1\n",
        )
        .await;

        let tls = TlsSettings::default();
        let proxy = ProxyConfig::default();
        let client = http_client::build_client(&tls, &proxy).unwrap();

        let endpoint = endpoint_with_labels(addr, &[]);
        let result = scrape_endpoint(
            endpoint,
            client,
            Duration::from_secs(5),
            None,
            Some("pod_name".to_string()),
            Some("pod_namespace".to_string()),
            false,
            true,
        )
        .await;

        tx.send(()).ok();

        let events = result.unwrap();
        assert!(!events.is_empty());
        for event in &events {
            if let Event::Metric(metric) = event {
                if let Some(tags) = metric.tags() {
                    assert!(
                        !tags.contains_key("tenant"),
                        "no label should appear when extra_labels is empty"
                    );
                }
            }
        }
    }

    /// honor_labels = true preserves a label already present on the scraped
    /// metric and does not overwrite it with the configured `extra_labels`
    /// value.
    #[tokio::test]
    async fn test_scrape_endpoint_extra_labels_honor_labels() {
        let (addr, tx) = spawn_metrics_server(
            "test_metric{tenant=\"original_tenant\"} 1\n",
        )
        .await;

        let tls = TlsSettings::default();
        let proxy = ProxyConfig::default();
        let client = http_client::build_client(&tls, &proxy).unwrap();

        let endpoint = endpoint_with_labels(addr, &[("tenant", "override_tenant")]);
        let result = scrape_endpoint(
            endpoint,
            client,
            Duration::from_secs(5),
            None,
            None,
            None,
            true, // honor_labels
            true,
        )
        .await;

        tx.send(()).ok();

        let events = result.unwrap();
        assert!(!events.is_empty());
        for event in &events {
            if let Event::Metric(metric) = event {
                let tags = metric.tags().expect("metric should have tags");
                assert_eq!(
                    tags.get("tenant"),
                    Some("original_tenant"),
                    "honor_labels must preserve the scrape-target's existing label"
                );
            }
        }
    }

    /// emit_pod_metadata = false suppresses `extra_labels` too — they live in
    /// the same metadata block as pod_name/pod_namespace.
    #[tokio::test]
    async fn test_scrape_endpoint_extra_labels_suppressed_when_metadata_disabled() {
        let (addr, tx) = spawn_metrics_server(
            "# TYPE http_requests_total counter\nhttp_requests_total{method=\"GET\"} 1\n",
        )
        .await;

        let tls = TlsSettings::default();
        let proxy = ProxyConfig::default();
        let client = http_client::build_client(&tls, &proxy).unwrap();

        let endpoint = endpoint_with_labels(addr, &[("tenant", "alpha")]);
        let result = scrape_endpoint(
            endpoint,
            client,
            Duration::from_secs(5),
            None,
            Some("pod_name".to_string()),
            Some("pod_namespace".to_string()),
            false,
            false, // emit_pod_metadata = false
        )
        .await;

        tx.send(()).ok();

        let events = result.unwrap();
        assert!(!events.is_empty());
        for event in &events {
            if let Event::Metric(metric) = event {
                if let Some(tags) = metric.tags() {
                    assert!(
                        !tags.contains_key("tenant"),
                        "extra_labels must not be applied when emit_pod_metadata is false"
                    );
                }
            }
        }
    }

    /// Multi-entry `extra_labels` — when a pod's namespace rule matches
    /// several annotations, every resulting label lands on every scraped
    /// metric with its configured name and the annotation's value.
    #[tokio::test]
    async fn test_scrape_endpoint_emits_multiple_extra_label_tags() {
        let (addr, tx) = spawn_metrics_server(
            "# TYPE http_requests_total counter\nhttp_requests_total{method=\"GET\"} 1\n",
        )
        .await;

        let tls = TlsSettings::default();
        let proxy = ProxyConfig::default();
        let client = http_client::build_client(&tls, &proxy).unwrap();

        let endpoint = endpoint_with_labels(
            addr,
            &[
                ("tenant", "alpha"),
                ("svc", "checkout"),
                ("ver", "1.2.3"),
            ],
        );
        let result = scrape_endpoint(
            endpoint,
            client,
            Duration::from_secs(5),
            None,
            Some("pod_name".to_string()),
            Some("pod_namespace".to_string()),
            false,
            true,
        )
        .await;

        tx.send(()).ok();

        let events = result.unwrap();
        assert!(!events.is_empty());
        for event in &events {
            if let Event::Metric(metric) = event {
                let tags = metric.tags().expect("metric should have tags");
                assert_eq!(tags.get("tenant"), Some("alpha"));
                assert_eq!(tags.get("svc"), Some("checkout"));
                assert_eq!(tags.get("ver"), Some("1.2.3"));
            }
        }
    }
}
