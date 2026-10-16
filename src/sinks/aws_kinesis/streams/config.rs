use std::{
    num::NonZeroU64,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use aws_sdk_kinesis::operation::{
    describe_stream::DescribeStreamError, put_records::PutRecordsError,
};
use aws_smithy_runtime_api::client::{orchestrator::HttpResponse, result::SdkError};
use futures::FutureExt;
use snafu::Snafu;
use vector_lib::configurable::{component::GenerateConfig, configurable_component};

use super::{
    KinesisClient, KinesisError, KinesisRecord, KinesisResponse, KinesisSinkBaseConfig, build_sink,
    record::{KinesisStreamClient, KinesisStreamRecord},
    sink::BatchKinesisRequest,
};
use crate::{
    aws::{ClientBuilder, create_client, is_retriable_error},
    config::{AcknowledgementsConfig, Input, ProxyConfig, SinkConfig, SinkContext},
    sinks::{
        Healthcheck, VectorSink,
        prelude::*,
        util::{
            BatchConfig, SinkBatchSettings,
            retries::{RetryAction, RetryLogic},
        },
    },
};

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Snafu)]
enum HealthcheckError {
    #[snafu(display("DescribeStream failed: {}", source))]
    DescribeStreamFailed {
        source: SdkError<DescribeStreamError, HttpResponse>,
    },
    #[snafu(display("Stream names do not match, got {}, expected {}", name, stream_name))]
    StreamNamesMismatch { name: String, stream_name: String },
}

#[derive(Default)]
pub struct KinesisClientBuilder {
    pool_idle_timeout: Option<Duration>,
}

impl ClientBuilder for KinesisClientBuilder {
    type Client = KinesisClient;

    fn pool_idle_timeout(&self) -> Option<Duration> {
        self.pool_idle_timeout
    }

    fn build(&self, config: &aws_types::SdkConfig) -> Self::Client {
        let mut builder = aws_sdk_kinesis::config::Builder::from(config);
        // The SDK rejects FIPS resolution with a custom endpoint.
        builder.set_use_fips(Some(
            config.endpoint_url().is_none() && config.use_fips().unwrap_or(false),
        ));
        KinesisClient::from_conf(builder.build())
    }
}

pub const MAX_PAYLOAD_SIZE: usize = 5_000_000;
pub const MAX_PAYLOAD_EVENTS: usize = 500;

#[derive(Clone, Copy, Debug, Default)]
pub struct KinesisDefaultBatchSettings;

impl SinkBatchSettings for KinesisDefaultBatchSettings {
    const MAX_EVENTS: Option<usize> = Some(MAX_PAYLOAD_EVENTS);
    const MAX_BYTES: Option<usize> = Some(MAX_PAYLOAD_SIZE);
    const TIMEOUT_SECS: f64 = 1.0;
}

/// Configuration for the `aws_kinesis_streams` sink.
#[configurable_component(sink(
    "aws_kinesis_streams",
    "Publish logs to AWS Kinesis Streams topics."
))]
#[derive(Clone, Debug)]
pub struct KinesisStreamsSinkConfig {
    #[serde(flatten)]
    pub base: KinesisSinkBaseConfig,

    #[configurable(derived)]
    #[serde(default)]
    pub batch: BatchConfig<KinesisDefaultBatchSettings>,

    /// Fail over to the public AWS endpoint when the configured PrivateLink `endpoint` is
    /// unreachable.
    ///
    /// When set, the sink builds a second client that ignores the `endpoint` override (targeting
    /// the default public regional endpoint) and, on a transport failure to the override endpoint
    /// (DNS/NXDOMAIN, connection refused/reset, TLS, or a connect/read timeout), latches to it for
    /// the remainder of the process; a process restart re-attempts the override endpoint. Has no
    /// effect unless an `endpoint` override is configured.
    #[serde(default)]
    #[configurable(metadata(docs::advanced))]
    pub private_link_failover: bool,

    /// Maximum time a connection remains idle in the Kinesis HTTP pool.
    ///
    /// Set below the server's idle timeout to avoid reusing a connection as the server closes it.
    /// Applies to both the primary and fallback Kinesis clients, but not credential requests.
    /// Must be greater than zero. When unset, the HTTP client's default of 90 seconds is retained.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[configurable(metadata(docs::advanced))]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    pub pool_idle_timeout_secs: Option<NonZeroU64>,
}

impl KinesisStreamsSinkConfig {
    async fn healthcheck(self, client: KinesisClient) -> crate::Result<()> {
        let stream_name = self.base.stream_name;

        let describe_result = client
            .describe_stream()
            .stream_name(stream_name.clone())
            .set_exclusive_start_shard_id(None)
            .limit(1)
            .send()
            .await;

        match describe_result {
            Ok(resp) => {
                let name = resp
                    .stream_description
                    .map(|x| x.stream_name)
                    .unwrap_or_default();
                if name == stream_name {
                    Ok(())
                } else {
                    Err(HealthcheckError::StreamNamesMismatch { name, stream_name }.into())
                }
            }
            Err(source) => Err(HealthcheckError::DescribeStreamFailed { source }.into()),
        }
    }

    pub async fn create_client(&self, proxy: &ProxyConfig) -> crate::Result<KinesisClient> {
        create_client::<KinesisClientBuilder>(
            &KinesisClientBuilder {
                pool_idle_timeout: self
                    .pool_idle_timeout_secs
                    .map(|timeout| Duration::from_secs(timeout.get())),
            },
            &self.base.auth,
            self.base.region.region(),
            self.base.region.endpoint(),
            proxy,
            self.base.tls.as_ref(),
            None,
        )
        .await
    }

    /// Build a client that ignores the `endpoint` override, targeting the default public regional
    /// endpoint. Used as the PrivateLink failover target.
    pub async fn create_public_client(&self, proxy: &ProxyConfig) -> crate::Result<KinesisClient> {
        create_client::<KinesisClientBuilder>(
            &KinesisClientBuilder {
                pool_idle_timeout: self
                    .pool_idle_timeout_secs
                    .map(|timeout| Duration::from_secs(timeout.get())),
            },
            &self.base.auth,
            self.base.region.region(),
            None,
            proxy,
            self.base.tls.as_ref(),
            None,
        )
        .await
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "aws_kinesis_streams")]
impl SinkConfig for KinesisStreamsSinkConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let client = self.create_client(&cx.proxy).await?;
        let healthcheck = self.clone().healthcheck(client.clone()).boxed();

        // When failover is enabled and an `endpoint` override is configured, build a public/NAT
        // client to fall back to if the override endpoint is unreachable. With no override there is
        // nothing to fail over from.
        let fallback_client = if self.private_link_failover && self.base.region.endpoint().is_some()
        {
            Some(self.create_public_client(&cx.proxy).await?)
        } else {
            None
        };

        let batch_settings = self
            .batch
            .validate()?
            .limit_max_bytes(MAX_PAYLOAD_SIZE)?
            .limit_max_events(MAX_PAYLOAD_EVENTS)?
            .into_batcher_settings()?;

        let sink = build_sink::<
            KinesisStreamClient,
            KinesisRecord,
            KinesisStreamRecord,
            KinesisError,
            KinesisRetryLogic,
        >(
            &self.base,
            self.base.partition_key_field.clone(),
            batch_settings,
            KinesisStreamClient {
                client,
                fallback_client,
                private_link_disabled: Arc::new(AtomicBool::new(false)),
            },
            KinesisRetryLogic {
                retry_partial: self.base.request_retry_partial,
            },
        )?;

        Ok((sink, healthcheck))
    }

    fn input(&self) -> Input {
        self.base.input()
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        self.base.acknowledgements()
    }
}

impl GenerateConfig for KinesisStreamsSinkConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(
            r#"partition_key_field = "foo"
            stream_name = "my-stream"
            encoding.codec = "json""#,
        )
        .unwrap()
    }
}
#[derive(Default, Clone)]
struct KinesisRetryLogic {
    retry_partial: bool,
}

impl RetryLogic for KinesisRetryLogic {
    type Error = SdkError<KinesisError, HttpResponse>;
    type Request = BatchKinesisRequest<KinesisStreamRecord>;
    type Response = KinesisResponse;

    fn is_retriable_error(&self, error: &Self::Error) -> bool {
        if let SdkError::ServiceError(inner) = error {
            // Note that if the request partially fails (records sent to one
            // partition fail but the others do not, for example), Vector
            // does not retry. This line only covers a failure for the entire
            // request.
            //
            // https://github.com/vectordotdev/vector/issues/359

            // if we do not want to retry partial failure due to ProvisionedThroughputExceededException
            // we should also not retry when whole request is failed due to ProvisionedThroughputExceededException
            // using retry_partial to gate not retrying due to quota failure.
            if matches!(
                inner.err(),
                PutRecordsError::ProvisionedThroughputExceededException(_)
            ) && self.retry_partial
            {
                return true;
            }
        }
        is_retriable_error(error)
    }

    fn should_retry_response(&self, response: &Self::Response) -> RetryAction<Self::Request> {
        if response.failure_count > 0 && self.retry_partial && !response.failed_records.is_empty() {
            let failed_records = response.failed_records.clone();
            RetryAction::RetryPartial(Box::new(move |original_request| {
                let failed_events: Vec<_> = failed_records
                    .iter()
                    .filter_map(|r| original_request.events.get(r.index).cloned())
                    .collect();

                let metadata = RequestMetadata::from_batch(
                    failed_events.iter().map(|req| req.get_metadata().clone()),
                );

                BatchKinesisRequest {
                    events: failed_events,
                    metadata,
                }
            }))
        } else {
            RetryAction::Successful
        }
    }
}

#[cfg(test)]
mod tests {
    use super::KinesisStreamsSinkConfig;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<KinesisStreamsSinkConfig>();
    }

    #[test]
    fn pool_idle_timeout_rejects_zero() {
        let error = serde_json::from_value::<KinesisStreamsSinkConfig>(serde_json::json!({
            "stream_name": "test-stream",
            "region": "us-west-2",
            "encoding": { "codec": "json" },
            "pool_idle_timeout_secs": 0
        }))
        .unwrap_err();
        assert!(error.to_string().contains("nonzero"), "{error}");
    }

    // Count real local HTTP connections without contacting AWS.
    async fn assert_connection_reuse(timeout_secs: Option<u64>, expected_connections: usize) {
        use crate::config::ProxyConfig;
        use hyper::{
            Body, Response, Server,
            body::HttpBody,
            service::{make_service_fn, service_fn},
        };
        use std::{
            convert::Infallible,
            sync::{
                Arc,
                atomic::{AtomicUsize, Ordering},
            },
            time::Duration,
        };

        let connections = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&connections);
        let service = make_service_fn(move |_| {
            count.fetch_add(1, Ordering::SeqCst);
            async {
                Ok::<_, Infallible>(service_fn(|request: hyper::Request<Body>| async {
                    request.into_body().collect().await.unwrap();
                    Ok::<_, Infallible>(Response::new(Body::from(
                        r#"{"StreamNames":[],"HasMoreStreams":false}"#,
                    )))
                }))
            }
        });
        let server = Server::bind(&([127, 0, 0, 1], 0).into()).serve(service);
        let endpoint = format!("http://{}", server.local_addr());
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(server.with_graceful_shutdown(async {
            let _ = stopped.await;
        }));
        let mut config = serde_json::json!({
            "stream_name": "test-stream",
            "region": "us-west-2",
            "endpoint": endpoint,
            "encoding": { "codec": "json" },
            "auth": { "access_key_id": "test-access", "secret_access_key": "test-secret" }
        });
        if let Some(timeout) = timeout_secs {
            config["pool_idle_timeout_secs"] = timeout.into();
        }
        let config: KinesisStreamsSinkConfig = serde_json::from_value(config).unwrap();
        let client = config
            .create_client(&ProxyConfig {
                enabled: false,
                ..Default::default()
            })
            .await
            .unwrap();
        client.list_streams().send().await.unwrap();
        tokio::time::sleep(Duration::from_millis(1250)).await;
        client.list_streams().send().await.unwrap();
        assert_eq!(connections.load(Ordering::SeqCst), expected_connections);
        drop(client);
        let _ = stop.send(());
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn pool_idle_timeout_reconnects_after_expiry() {
        assert_connection_reuse(Some(1), 2).await;
    }

    #[tokio::test]
    async fn pool_idle_timeout_keeps_connections_before_expiry() {
        assert_connection_reuse(Some(5), 1).await;
    }

    #[tokio::test]
    async fn pool_idle_timeout_unset_preserves_default_reuse() {
        assert_connection_reuse(None, 1).await;
    }
}

#[cfg(test)]
mod fips_endpoint_tests {
    use super::*;
    use crate::aws::test_util::CaptureClient;

    const LIST_STREAMS_RESPONSE: &str = r#"{"StreamNames":[],"HasMoreStreams":false}"#;

    #[tokio::test]
    async fn fips_kinesis_requests_use_regional_endpoints() {
        for region in [
            "us-east-1",
            "us-east-2",
            "us-west-1",
            "us-west-2",
            "us-gov-east-1",
            "us-gov-west-1",
        ] {
            for requested in [None, Some(false), Some(true)] {
                let http = CaptureClient::new(LIST_STREAMS_RESPONSE);
                let client = KinesisClientBuilder::default().build(&http.config(region, requested));
                client.list_streams().send().await.unwrap();
                let service = if requested == Some(true) && !region.starts_with("us-gov-") {
                    "kinesis-fips"
                } else {
                    "kinesis"
                };
                let requests = http.requests.lock().unwrap();
                assert_eq!(
                    requests[0].uri(),
                    format!("https://{service}.{region}.amazonaws.com/")
                );
                assert!(
                    requests[0]
                        .headers()
                        .get("authorization")
                        .unwrap()
                        .contains(&format!("/{region}/kinesis/aws4_request"))
                );
            }
        }
    }

    #[tokio::test]
    async fn fips_kinesis_preserves_custom_endpoints() {
        for requested in [None, Some(false), Some(true)] {
            for region in ["us-east-1", "ca-central-1", "cn-north-1"] {
                let http = CaptureClient::new(LIST_STREAMS_RESPONSE);
                let config = http
                    .config(region, requested)
                    .into_builder()
                    .endpoint_url("https://private-kinesis.example.invalid")
                    .build();
                let client = KinesisClientBuilder::default().build(&config);
                client.list_streams().send().await.unwrap();
                let requests = http.requests.lock().unwrap();
                assert_eq!(requests.len(), 1);
                assert_eq!(
                    requests[0].uri(),
                    "https://private-kinesis.example.invalid/"
                );
            }
        }
    }

    #[tokio::test]
    async fn fips_kinesis_does_not_fallback_on_endpoint_resolution_failure() {
        for (region, endpoint) in [(None, None), (Some("us-east-1"), Some("not a URL"))] {
            let http = CaptureClient::new(LIST_STREAMS_RESPONSE);
            let mut config = http.config("us-east-1", Some(true)).into_builder();
            config.set_region(region.map(aws_types::region::Region::new));
            config.set_endpoint_url(endpoint.map(str::to_owned));
            let client = KinesisClientBuilder::default().build(&config.build());
            let error = client.list_streams().send().await.unwrap_err();
            if endpoint.is_none() {
                assert!(format!("{error:?}").contains("Missing Region"), "{error:?}");
            }
            assert!(http.requests.lock().unwrap().is_empty());
        }
    }
}
