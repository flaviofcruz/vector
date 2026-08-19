pub(crate) mod parser;

#[cfg(feature = "sources-prometheus-scrape")]
pub(crate) mod decompress;
#[cfg(feature = "sources-prometheus-scrape")]
mod k8s_endpoint_provider;
#[cfg(feature = "sources-prometheus-scrape")]
mod k8s_scrape;
#[cfg(feature = "sources-prometheus-pushgateway")]
mod pushgateway;
#[cfg(feature = "sources-prometheus-remote-write")]
mod remote_write;
#[cfg(feature = "sources-prometheus-scrape")]
mod scrape;

#[cfg(feature = "sources-prometheus-scrape")]
pub use k8s_scrape::PrometheusK8sScrapeConfig;
#[cfg(feature = "sources-prometheus-pushgateway")]
pub use pushgateway::PrometheusPushgatewayConfig;
#[cfg(feature = "sources-prometheus-remote-write")]
pub use remote_write::PrometheusRemoteWriteConfig;
#[cfg(feature = "sources-prometheus-scrape")]
pub use scrape::PrometheusScrapeConfig;
