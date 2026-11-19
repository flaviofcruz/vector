//! Real TLS/native transport around reloads; only the downstream RPC service is a test peer.
use std::{
    collections::HashSet,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use futures::{StreamExt, stream};
use kafka_producer_proxy::proto::{
    KafkaMessage, KafkaMessages, kafka_message, kafka_messages,
    kafka_producer_proxy_service_server::{
        KafkaProducerProxyService, KafkaProducerProxyServiceServer,
    },
};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use crate::{
    config::{ComponentKey, Config, ConfigBuilder, SinkConfig, SinkContext},
    event::{Event, EventContainer, LogEvent},
    proto::vector as proto,
    test_util::{addr::next_addr, start_topology, wait_for_tcp},
    tls::{
        MaybeTlsConnectInfo, MaybeTlsSettings, TEST_PEM_CA_PATH, TEST_PEM_CLIENT_CRT_PATH,
        TEST_PEM_CLIENT_KEY_PATH, TEST_PEM_CRT_PATH, TEST_PEM_KEY_PATH, TlsEnableableConfig,
    },
    topology::ReloadError,
};

/// Simulates a publisher changing files after preflight while another component fails.
#[vector_lib::configurable::configurable_component(sink(
    "test_tls_reload_error",
    "Test TLS reload rollback."
))]
#[derive(Clone, Debug, Default)]
struct RejectReload {
    /// Synthetic identity path to invalidate during the replacement build.
    key: PathBuf,
}

impl_generate_config_from_default!(RejectReload);

#[async_trait::async_trait]
#[typetag::serde(name = "test_tls_reload_error")]
impl SinkConfig for RejectReload {
    async fn build(
        &self,
        _: SinkContext,
    ) -> crate::Result<(crate::sinks::VectorSink, crate::sinks::Healthcheck)> {
        std::fs::write(&self.key, b"replacement in progress")?;
        Err("test component rejected the reload".into())
    }

    fn input(&self) -> crate::config::Input {
        crate::config::Input::all()
    }

    fn acknowledgements(&self) -> &crate::config::AcknowledgementsConfig {
        &crate::config::AcknowledgementsConfig::DEFAULT
    }
}

#[derive(Clone)]
struct Peer(mpsc::UnboundedSender<(String, Vec<u8>)>);

impl Peer {
    fn certificate<T>(request: &Request<T>) -> Vec<u8> {
        request
            .extensions()
            .get::<MaybeTlsConnectInfo>()
            .unwrap()
            .verified_peer_certificate_der
            .clone()
            .expect("verified client identity")
    }
}

#[tonic::async_trait]
impl proto::Service for Peer {
    async fn push_events(
        &self,
        request: Request<proto::PushEventsRequest>,
    ) -> Result<Response<proto::PushEventsResponse>, Status> {
        let cert = Self::certificate(&request);
        for event in request.into_inner().events {
            let event = Event::from(event);
            self.0
                .send((super::into_message(event), cert.clone()))
                .unwrap();
        }
        Ok(Response::new(proto::PushEventsResponse {}))
    }

    async fn health_check(
        &self,
        _: Request<proto::HealthCheckRequest>,
    ) -> Result<Response<proto::HealthCheckResponse>, Status> {
        Ok(Response::new(proto::HealthCheckResponse {
            status: proto::ServingStatus::Serving.into(),
        }))
    }
}

#[tonic::async_trait]
impl KafkaProducerProxyService for Peer {
    async fn produce_message(
        &self,
        request: Request<KafkaMessage>,
    ) -> Result<Response<kafka_message::Response>, Status> {
        let cert = Self::certificate(&request);
        self.0
            .send((
                String::from_utf8(request.into_inner().data.unwrap()).unwrap(),
                cert,
            ))
            .unwrap();
        Ok(Response::new(kafka_message::Response {
            error_code: Some(0),
            error_msg: None,
        }))
    }

    async fn produce_messages(
        &self,
        request: Request<KafkaMessages>,
    ) -> Result<Response<kafka_messages::Response>, Status> {
        let cert = Self::certificate(&request);
        let mut responses = Vec::new();
        for message in request.into_inner().messages {
            self.0
                .send((
                    String::from_utf8(message.data.unwrap()).unwrap(),
                    cert.clone(),
                ))
                .unwrap();
            responses.push(kafka_message::Response {
                error_code: Some(0),
                error_msg: None,
            });
        }
        Ok(Response::new(kafka_messages::Response {
            responses,
            all_succeed: Some(true),
        }))
    }
}

async fn peer(
    addr: SocketAddr,
) -> (
    tokio::task::JoinHandle<()>,
    mpsc::UnboundedReceiver<(String, Vec<u8>)>,
) {
    let mut tls = TlsEnableableConfig::test_config();
    tls.options.verify_certificate = Some(true);
    let listener = MaybeTlsSettings::from_config(Some(&tls), true)
        .unwrap()
        .bind(&addr)
        .await
        .unwrap();
    let incoming = listener.accept_stream().then(|stream| async {
        let mut stream = stream?;
        stream.handshake().await?;
        Ok::<_, crate::tls::TlsError>(stream)
    });
    let (tx, rx) = mpsc::unbounded_channel();
    let service = Peer(tx);
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(proto::Server::new(service.clone()))
            .add_service(KafkaProducerProxyServiceServer::new(service))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    (task, rx)
}

async fn send(addr: SocketAddr, message: &str, tls: Option<serde_json::Value>) {
    let mut value =
        serde_json::json!({"address": format!("http://{addr}"), "batch": {"max_events": 1}});
    if let Some(tls) = tls {
        value["address"] = format!("https://localhost:{}", addr.port()).into();
        value["tls"] = tls;
    }
    let sink: crate::sinks::vector::VectorConfig = serde_json::from_value(value).unwrap();
    let (sink, _) = sink.build(SinkContext::default()).await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        sink.run(stream::iter([Event::Log(LogEvent::from(message))]).map(Into::into)),
    )
    .await
    .unwrap()
    .unwrap();
}

fn fresh(builder: &ConfigBuilder) -> Config {
    let fresh: ConfigBuilder =
        serde_json::from_value(serde_json::to_value(builder).unwrap()).unwrap();
    fresh.build().unwrap()
}

fn identity(cert: &Path, key: &Path) -> serde_json::Value {
    serde_json::json!({"enabled": true, "verify_certificate": true, "verify_hostname": true,
        "crt_file": cert, "key_file": key, "ca_file": TEST_PEM_CA_PATH})
}

async fn sink_reload(sink_type: &str, load_balancing: bool) {
    crate::test_util::trace_init();
    let directory = tempfile::tempdir().unwrap();
    let cert = directory.path().join("cert.pem");
    let key = directory.path().join("key.pem");
    std::fs::copy(TEST_PEM_CLIENT_CRT_PATH, &cert).unwrap();
    std::fs::copy(TEST_PEM_CLIENT_KEY_PATH, &key).unwrap();
    let (_input_guard, input) = next_addr();
    let (_peer_guard, output) = next_addr();
    let (server, mut received) = peer(output).await;
    let mut sink = serde_json::json!({"type": sink_type, "inputs": ["in"],
        "address": format!("https://localhost:{}", output.port()),
        "tls": identity(&cert, &key), "healthcheck": {"enabled": false},
        "batch": {"max_events": 1, "timeout_secs": 0.1}});
    if sink_type == "kafka_producer_proxy" {
        sink["topic"] = "test-tls-renewal".into();
    }
    if load_balancing {
        sink["load_balancing"] = serde_json::json!({"enabled": true});
        sink["tls"]["server_name"] = "localhost".into();
    }
    let builder: ConfigBuilder = serde_json::from_value(serde_json::json!({
        "sources": {"in": {"type": "vector", "address": input.to_string()}},
        "sinks": {"out": sink}
    }))
    .unwrap();
    let (mut topology, _crash) = start_topology(fresh(&builder), false).await;
    wait_for_tcp(input).await;
    let expected_cert =
        openssl::x509::X509::from_pem(&std::fs::read(TEST_PEM_CLIENT_CRT_PATH).unwrap())
            .unwrap()
            .to_der()
            .unwrap();
    for stage in ["initial", "mismatch", "missing", "repaired", "rolled-back"] {
        match stage {
            "mismatch" | "missing" => {
                if stage == "mismatch" {
                    std::fs::copy(TEST_PEM_KEY_PATH, &key).unwrap();
                } else {
                    std::fs::remove_file(&key).unwrap();
                }
                // An unchanged reload must retain the running sink's last-good snapshot.
                if stage == "mismatch" {
                    topology
                        .reload_config_and_respawn(fresh(&builder), Default::default())
                        .await
                        .unwrap();
                }
                topology.extend_reload_set(HashSet::from([ComponentKey::from("out")]));
                assert!(matches!(
                    topology
                        .reload_config_and_respawn(fresh(&builder), Default::default())
                        .await,
                    Err(ReloadError::TopologyBuildFailed)
                ));
            }
            "repaired" => {
                std::fs::copy(TEST_PEM_CLIENT_KEY_PATH, &key).unwrap();
                topology.extend_reload_set(HashSet::from([ComponentKey::from("out")]));
                topology
                    .reload_config_and_respawn(fresh(&builder), Default::default())
                    .await
                    .unwrap();
            }
            "rolled-back" => {
                let mut candidate = builder.clone();
                candidate.add_sink("broken", &["in"], RejectReload { key: key.clone() });
                topology.extend_reload_set(HashSet::from([ComponentKey::from("out")]));
                assert!(matches!(
                    topology
                        .reload_config_and_respawn(fresh(&candidate), Default::default())
                        .await,
                    Err(ReloadError::TopologyBuildFailed)
                ));
            }
            _ => {}
        }
        send(input, stage, None).await;
        let (actual, actual_cert) = tokio::time::timeout(Duration::from_secs(10), received.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(actual, stage, "{sink_type}");
        assert_eq!(actual_cert, expected_cert, "{sink_type}, {stage}");
    }
    topology.stop().await;
    assert!(received.try_recv().is_err(), "duplicate delivery");
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn vector_sink_tls_reload_retains_identity_and_delivery() {
    sink_reload("vector", false).await;
}

#[tokio::test]
async fn kpp_sink_tls_reload_retains_identity_and_delivery() {
    sink_reload("kafka_producer_proxy", false).await;
}

#[tokio::test]
async fn balanced_vector_sink_tls_reload_retains_identity_and_delivery() {
    sink_reload("vector", true).await;
}

#[tokio::test]
async fn balanced_kpp_sink_tls_reload_retains_identity_and_delivery() {
    sink_reload("kafka_producer_proxy", true).await;
}

#[tokio::test]
async fn native_source_tls_reload_keeps_serving_after_mismatch_and_recovers() {
    crate::test_util::trace_init();
    let directory = tempfile::tempdir().unwrap();
    let cert = directory.path().join("cert.pem");
    let key = directory.path().join("key.pem");
    std::fs::copy(TEST_PEM_CRT_PATH, &cert).unwrap();
    std::fs::copy(TEST_PEM_KEY_PATH, &key).unwrap();
    let (_guard, input) = next_addr();
    let source: crate::sources::vector::VectorConfig = serde_json::from_value(serde_json::json!({
        "address": input.to_string(), "tls": identity(&cert, &key)
    }))
    .unwrap();
    let (mut output, sink) = crate::test_util::mock::basic_sink(10);
    let mut builder = Config::builder();
    builder.add_source("in", source);
    builder.add_sink("out", &["in"], sink);
    let (mut topology, _crash) = start_topology(builder.clone().build().unwrap(), false).await;
    wait_for_tcp(input).await;
    for stage in ["initial", "mismatch", "repaired", "rolled-back"] {
        if stage == "mismatch" {
            std::fs::copy(TEST_PEM_CLIENT_KEY_PATH, &key).unwrap();
            topology.extend_reload_set(HashSet::from([ComponentKey::from("in")]));
            assert!(matches!(
                topology
                    .reload_config_and_respawn(builder.clone().build().unwrap(), Default::default())
                    .await,
                Err(ReloadError::TopologyBuildFailed)
            ));
        } else if stage == "repaired" {
            std::fs::copy(TEST_PEM_KEY_PATH, &key).unwrap();
            topology
                .reload_config_and_respawn(builder.clone().build().unwrap(), Default::default())
                .await
                .unwrap();
            wait_for_tcp(input).await;
        } else if stage == "rolled-back" {
            let mut candidate = builder.clone();
            candidate.add_sink("broken", &["in"], RejectReload { key: key.clone() });
            assert!(matches!(
                topology
                    .reload_config_and_respawn(candidate.build().unwrap(), Default::default())
                    .await,
                Err(ReloadError::TopologyBuildFailed)
            ));
            wait_for_tcp(input).await;
        }
        send(
            input,
            stage,
            Some(identity(
                Path::new(TEST_PEM_CLIENT_CRT_PATH),
                Path::new(TEST_PEM_CLIENT_KEY_PATH),
            )),
        )
        .await;
        let batch = tokio::time::timeout(Duration::from_secs(10), output.next())
            .await
            .unwrap()
            .unwrap();
        let events: Vec<_> = batch
            .events
            .into_events()
            .map(super::into_message)
            .collect();
        assert_eq!(events, [stage]);
    }
    drop(builder);
    topology.stop().await;
    assert!(
        tokio::time::timeout(Duration::from_secs(10), output.next())
            .await
            .unwrap()
            .is_none()
    );
}
