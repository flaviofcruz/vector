//! Prometheus remote write directly to per-destination encoded output.
//! Each request produces one payload per destination, retaining one row per sample.
//! This is unrelated to row-merging batching and does not change sink batching or buffers.
//! Decoding (prost), timestamp conversion and acceptance match the ordinary
//! remote-write source followed by the stable per-sample Hydra transform.

mod encoding;
#[cfg(test)]
mod tests;
mod variant;

use encoding::{OutputEncoding, OutputPlan, RequestEncoder};
use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    sync::Arc,
};

use bytes::Bytes;
use prost::Message;
use vector_lib::{
    config::LogNamespace,
    configurable::configurable_component,
    lookup::event_path,
    prometheus::parser::{METRIC_NAME_LABEL, ParserError, proto},
};
use vrl::value::Value;
use warp::http::{HeaderMap, StatusCode};

use crate::{
    common::http::{ErrorMessage, server_auth::HttpServerAuthConfig},
    config::{
        DataType, GenerateConfig, SourceAcknowledgementsConfig, SourceConfig, SourceContext,
        SourceOutput,
    },
    event::Event,
    http::KeepaliveConfig,
    internal_events::{MetricToLogHydraDropped, PrometheusRemoteWriteParseError},
    schema::Definition,
    serde::bool_or_struct,
    sources::{
        self,
        util::{HttpSource, decompress_body, http::HttpMethod},
    },
    tls::TlsEnableableConfig,
    transforms::metric_to_log_hydra::{
        CATCH_ALL_SYSTEM, DropReason, MetricToLogHydra, PodRoutingConfig, TOPIC, accept_timestamp,
    },
};

/// Configuration for the `hydra_remote_write` source.
#[configurable_component(source(
    "hydra_remote_write",
    "Receive Prometheus remote write and emit per-destination encoded payloads."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct HydraRemoteWriteConfig {
    /// The socket address to accept connections on. The address _must_ include a port.
    #[configurable(metadata(docs::examples = "0.0.0.0:9090"))]
    address: SocketAddr,

    /// The URL path on which metric POST requests are accepted.
    #[serde(default = "default_path")]
    path: String,

    #[configurable(derived)]
    tls: Option<TlsEnableableConfig>,

    #[configurable(derived)]
    #[configurable(metadata(docs::advanced))]
    auth: Option<HttpServerAuthConfig>,

    #[configurable(derived)]
    #[serde(default, deserialize_with = "bool_or_struct")]
    acknowledgements: SourceAcknowledgementsConfig,

    #[configurable(derived)]
    #[serde(default)]
    keepalive: KeepaliveConfig,

    /// Routing of systems to pods, as `metric_to_log_hydra`'s `pod_routing`.
    pod_routing: PodRoutingConfig,

    /// Encoding of each destination's payload. Every pod in `pod_routing` must be listed.
    outputs: BTreeMap<String, OutputEncoding>,

    /// The field set to each emitted event's pod port, for a downstream `route` transform.
    #[serde(default = "default_port_field")]
    port_field: String,
}

fn default_path() -> String {
    "/".to_string()
}

fn default_port_field() -> String {
    "hydra_port".to_string()
}

impl GenerateConfig for HydraRemoteWriteConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(
            r#"address = "127.0.0.1:9090"
[pod_routing]
systems = [{ system = "*", pods = ["pod0"] }]
[outputs]
pod0 = "ndjson"
"#,
        )
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "hydra_remote_write")]
impl SourceConfig for HydraRemoteWriteConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<sources::Source> {
        let plan = OutputPlan::compile(&self.outputs, &self.pod_routing)?;
        let source = HydraRemoteWriteSource {
            plan: Arc::new(plan),
            port_field: self.port_field.clone().into(),
        };
        source.run(
            self.address,
            self.path.as_str(),
            HttpMethod::Post,
            StatusCode::OK,
            true,
            self.tls.as_ref(),
            self.auth.as_ref(),
            cx,
            self.acknowledgements,
            self.keepalive.clone(),
        )
    }

    fn outputs(&self, _global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        vec![SourceOutput::new_maybe_logs(
            DataType::Log,
            Definition::default_legacy_namespace(),
        )]
    }

    fn can_acknowledge(&self) -> bool {
        true
    }
}

#[derive(Clone)]
struct HydraRemoteWriteSource {
    plan: Arc<OutputPlan>,
    port_field: Arc<str>,
}

impl HydraRemoteWriteSource {
    /// Decode one request body into pre-encoded payloads, one per pod port with rows.
    fn decode_body(&self, body: Bytes) -> Result<Vec<Event>, ErrorMessage> {
        let request = proto::WriteRequest::decode(body).map_err(|error| {
            emit!(PrometheusRemoteWriteParseError {
                error: error.clone()
            });
            ErrorMessage::new(
                StatusCode::BAD_REQUEST,
                format!("Could not decode write request: {error}"),
            )
        })?;
        let start = chrono::Utc::now();
        let mut assembler = RequestEncoder::new(&self.plan);
        let mut dropped = DropTally::default();
        let mut received = 0;
        // `parse_request_flat`'s label handling: the series' labels without `__name__`, key-sorted
        // and unique (last value wins); a series without `__name__` fails the request.
        for series in &request.timeseries {
            let mut name = None;
            let mut labels: Vec<(&str, &str)> = Vec::with_capacity(series.labels.len());
            for label in &series.labels {
                if label.name == METRIC_NAME_LABEL {
                    name = Some(label.value.as_str());
                } else {
                    labels.push((&label.name, &label.value));
                }
            }
            let name = name.ok_or_else(no_name_error)?;
            sort_dedup_last(&mut labels);
            received += series.samples.len();
            for sample in &series.samples {
                let ts =
                    super::parser::utc_timestamp(Some(sample.timestamp), start).timestamp_millis();
                if let Err(reason) = assembler.push_sample(name, &labels, ts, sample.value) {
                    dropped.add(reason);
                }
            }
        }
        dropped.emit();
        metrics::counter!("hydra_remote_write_received_samples_total").increment(received as u64);
        Ok(assembler
            .finish_events()
            .into_iter()
            .map(|(port, mut log)| {
                log.insert(event_path!(self.port_field.as_ref()), Value::from(port));
                Event::from(log)
            })
            .collect())
    }
}

fn no_name_error() -> ErrorMessage {
    // As `prometheus_remote_write` reports a `parse_request_flat` error.
    ErrorMessage::new(
        StatusCode::BAD_REQUEST,
        format!(
            "Could not decode write request: {}",
            ParserError::RequestNoNameLabel
        ),
    )
}

impl HttpSource for HydraRemoteWriteSource {
    fn decode(&self, encoding_header: Option<&str>, body: Bytes) -> Result<Bytes, ErrorMessage> {
        // Default to snappy decoding the request body, as `prometheus_remote_write`.
        decompress_body(encoding_header.or(Some("snappy")), body)
    }

    fn build_events(
        &self,
        body: Bytes,
        _header_map: &HeaderMap,
        _query_parameters: &HashMap<String, String>,
        _full_path: &str,
    ) -> Result<Vec<Event>, ErrorMessage> {
        self.decode_body(body)
    }
}

/// Stable sort by key, then keep the last of each run of equal keys: the order and values of a
/// `MetricTags` built by `replace` in label order.
fn sort_dedup_last(labels: &mut Vec<(&str, &str)>) {
    if !labels.windows(2).all(|w| w[0].0 < w[1].0) {
        labels.sort_by(|a, b| a.0.cmp(b.0));
        let mut out = 0;
        for i in 0..labels.len() {
            if i + 1 < labels.len() && labels[i + 1].0 == labels[i].0 {
                continue;
            }
            labels[out] = labels[i];
            out += 1;
        }
        labels.truncate(out);
    }
}

/// The transform's checks, in its order: timestamp window, then a finite value.
fn accept(timestamp_ms: i64, value: f64, now_ms: i64) -> Result<(i64, i64), DropReason> {
    let timestamps = accept_timestamp(timestamp_ms, now_ms)?;
    if !value.is_finite() {
        return Err(DropReason::NonFiniteValue);
    }
    Ok(timestamps)
}

/// Drops tallied per input array and emitted once per reason, not once per sample.
#[derive(Default)]
struct DropTally([usize; 2]);

impl DropTally {
    const fn add(&mut self, reason: DropReason) {
        self.0[reason as usize] += 1;
    }

    fn emit(&self) {
        for (reason, count) in [DropReason::NonFiniteValue, DropReason::TimestampOutOfWindow]
            .into_iter()
            .zip(self.0)
        {
            if count > 0 {
                emit!(MetricToLogHydraDropped {
                    reason: reason.as_str(),
                    counts_as_loss: reason.counts_as_loss(),
                    count,
                });
            }
        }
    }
}
