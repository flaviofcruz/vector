use std::{collections::HashMap, convert::TryFrom, io};

use bytes::Bytes;
use chrono::{FixedOffset, Utc};
use http::{
    StatusCode, Uri,
    header::{HeaderName, HeaderValue},
};
use indoc::indoc;
use rand::Rng;
use snafu::{ResultExt, Snafu};
use tower::{Service, ServiceBuilder};
use uuid::Uuid;
use vector_common::internal_event::vector_event::file_send_event::FileEventMetadata;
use vector_lib::event::event_log::generate_count_map;
use vector_lib::{
    TimeZone, codecs::encoding::Framer, configurable::configurable_component, event::Finalizable,
    request_metadata::RequestMetadata,
};

use crate::{
    codecs::{Encoder, EncodingConfigWithFraming, SinkType, Transformer},
    config::{AcknowledgementsConfig, DataType, GenerateConfig, Input, SinkConfig, SinkContext},
    event::Event,
    gcp::{GcpAuthConfig, GcpAuthenticator, Scope},
    http::{HttpClient, get_http_scheme_from_uri},
    internal_events::vector_event::{VectorEventLogSendMetadata, emit_blob_file_upload_attempt},
    serde::json::to_string,
    sinks::{
        Healthcheck, VectorSink,
        gcs_common::{
            config::{
                GcsPredefinedAcl, GcsRetryLogic, GcsStorageClass, build_healthcheck,
                default_endpoint,
            },
            service::{GcsMetadata, GcsRequest, GcsRequestSettings, GcsResponse, GcsService},
            sink::GcsSink,
        },
        util::{
            BulkSizeBasedDefaultBatchSettings, Compression, RequestBuilder, ServiceBuilderExt,
            TowerRequestConfig, TowerRequestSettings, batch::BatchConfig,
            metadata::RequestMetadataBuilder, partitioner::KeyPartitioner,
            request_builder::EncodeResult, service::TowerRequestConfigDefaults, timezone_to_offset,
            vector_event_log::EventLoggingService,
        },
    },
    template::{Template, TemplateParseError},
    tls::{TlsConfig, TlsSettings},
};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum GcsHealthcheckError {
    #[snafu(display("key_prefix template parse error: {}", source))]
    KeyPrefixTemplate { source: TemplateParseError },
}

#[derive(Clone, Copy, Debug)]
pub struct GcsTowerRequestConfigDefaults;

impl TowerRequestConfigDefaults for GcsTowerRequestConfigDefaults {
    const RATE_LIMIT_NUM: u64 = 1_000;
}

/// Configuration for the `gcp_cloud_storage` sink.
#[configurable_component(sink(
    "gcp_cloud_storage",
    "Store observability events in GCP Cloud Storage."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct GcsSinkConfig {
    /// The GCS bucket name.
    #[configurable(metadata(docs::examples = "my-bucket"))]
    bucket: String,

    /// The Predefined ACL to apply to created objects.
    ///
    /// For more information, see [Predefined ACLs][predefined_acls].
    ///
    /// [predefined_acls]: https://cloud.google.com/storage/docs/access-control/lists#predefined-acl
    acl: Option<GcsPredefinedAcl>,

    /// The storage class for created objects.
    ///
    /// For more information, see the [storage classes][storage_classes] documentation.
    ///
    /// [storage_classes]: https://cloud.google.com/storage/docs/storage-classes
    storage_class: Option<GcsStorageClass>,

    /// The set of metadata `key:value` pairs for the created objects.
    ///
    /// For more information, see the [custom metadata][custom_metadata] documentation.
    ///
    /// [custom_metadata]: https://cloud.google.com/storage/docs/metadata#custom-metadata
    #[configurable(metadata(docs::additional_props_description = "A key/value pair."))]
    #[configurable(metadata(docs::advanced))]
    metadata: Option<HashMap<String, String>>,

    /// A prefix to apply to all object keys.
    ///
    /// Prefixes are useful for partitioning objects, such as by creating an object key that
    /// stores objects under a particular directory. If using a prefix for this purpose, it must end
    /// in `/` in order to act as a directory path. A trailing `/` is **not** automatically added.
    #[configurable(metadata(docs::templateable))]
    #[configurable(metadata(
        docs::examples = "date=%F/",
        docs::examples = "date=%F/hour=%H/",
        docs::examples = "year=%Y/month=%m/day=%d/",
        docs::examples = "application_id={{ application_id }}/date=%F/"
    ))]
    #[configurable(metadata(docs::advanced))]
    key_prefix: Option<String>,

    /// The timestamp format for the time component of the object key.
    ///
    /// By default, object keys are appended with a timestamp that reflects when the objects are
    /// sent to S3, such that the resulting object key is functionally equivalent to joining the key
    /// prefix with the formatted timestamp, such as `date=2022-07-18/1658176486`.
    ///
    /// This would represent a `key_prefix` set to `date=%F/` and the timestamp of Mon Jul 18 2022
    /// 20:34:44 GMT+0000, with the `filename_time_format` being set to `%s`, which renders
    /// timestamps in seconds since the Unix epoch.
    ///
    /// Supports the common [`strftime`][chrono_strftime_specifiers] specifiers found in most
    /// languages.
    ///
    /// When set to an empty string, no timestamp is appended to the key prefix.
    ///
    /// [chrono_strftime_specifiers]: https://docs.rs/chrono/latest/chrono/format/strftime/index.html#specifiers
    #[serde(default = "default_time_format")]
    #[configurable(metadata(docs::advanced))]
    filename_time_format: String,

    /// Whether or not to append a UUID v4 token to the end of the object key.
    ///
    /// The UUID is appended to the timestamp portion of the object key, such that if the object key
    /// generated is `date=2022-07-18/1658176486`, setting this field to `true` results
    /// in an object key that looks like `date=2022-07-18/1658176486-30f6652c-71da-4f9f-800d-a1189c47c547`.
    ///
    /// This ensures there are no name collisions, and can be useful in high-volume workloads where
    /// object keys must be unique.
    #[serde(default = "crate::serde::default_true")]
    #[configurable(metadata(docs::advanced))]
    filename_append_uuid: bool,

    /// Whether or not to prepend a random cryptographic nonce to the beginning of the object key's filename.
    ///
    /// The nonce is a truncated 8-character random hexadecimal string, prepended to the filename
    /// with a `-` delimiter. For example, if the filename would normally be `1658176486`, setting
    /// this field to `true` results in a filename like `a3b1f29c-1658176486`.
    ///
    /// This can be useful for GCS performance optimization by increasing key randomness.
    #[serde(default)]
    #[configurable(metadata(docs::advanced))]
    #[configurable(metadata(docs::human_name = "Prepend Crypto Nonce to Filename"))]
    filename_prepend_crypto_nonce: bool,

    /// The filename extension to use in the object key.
    ///
    /// If not specified, the extension is determined by the compression scheme used.
    #[configurable(metadata(docs::advanced))]
    filename_extension: Option<String>,

    #[serde(flatten)]
    encoding: EncodingConfigWithFraming,

    /// Overrides what content encoding has been applied to the object.
    ///
    /// Directly comparable to the `Content-Encoding` HTTP header.
    ///
    /// If not specified, the compression scheme used dictates this value.
    #[configurable(metadata(docs::examples = "gzip"))]
    content_encoding: Option<String>,

    /// Compression configuration.
    ///
    /// All compression algorithms use the default compression level unless otherwise specified.
    ///
    /// Some cloud storage API clients and browsers handle decompression transparently, so
    /// depending on how they are accessed, files may not always appear to be compressed.
    #[configurable(derived)]
    #[serde(default)]
    compression: Compression,

    /// Overrides the MIME type of the created objects.
    ///
    /// Directly comparable to the `Content-Type` HTTP header.
    ///
    /// If not specified, defaults to the encoder's content type.
    #[configurable(metadata(
        docs::examples = "text/plain; charset=utf-8",
        docs::examples = "application/gzip"
    ))]
    content_type: Option<String>,

    #[configurable(derived)]
    #[serde(default)]
    batch: BatchConfig<BulkSizeBasedDefaultBatchSettings>,

    /// API endpoint for Google Cloud Storage
    #[configurable(metadata(docs::examples = "http://localhost:9000"))]
    #[configurable(validation(format = "uri"))]
    #[serde(default = "default_endpoint")]
    endpoint: String,

    #[configurable(derived)]
    #[serde(default)]
    request: TowerRequestConfig<GcsTowerRequestConfigDefaults>,

    #[serde(flatten)]
    auth: GcpAuthConfig,

    #[configurable(derived)]
    tls: Option<TlsConfig>,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    acknowledgements: AcknowledgementsConfig,

    #[configurable(derived)]
    #[serde(default)]
    pub timezone: Option<TimeZone>,
}

fn default_time_format() -> String {
    "%s".to_string()
}

#[cfg(test)]
fn default_config(encoding: EncodingConfigWithFraming) -> GcsSinkConfig {
    GcsSinkConfig {
        bucket: Default::default(),
        acl: Default::default(),
        storage_class: Default::default(),
        metadata: Default::default(),
        key_prefix: Default::default(),
        filename_time_format: default_time_format(),
        filename_append_uuid: true,
        filename_prepend_crypto_nonce: false,
        filename_extension: Default::default(),
        content_type: Default::default(),
        encoding,
        content_encoding: None,
        compression: Compression::gzip_default(),
        batch: Default::default(),
        endpoint: Default::default(),
        request: Default::default(),
        auth: Default::default(),
        tls: Default::default(),
        acknowledgements: Default::default(),
        timezone: Default::default(),
    }
}

impl GenerateConfig for GcsSinkConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(indoc! {r#"
            bucket = "my-bucket"
            credentials_path = "/path/to/credentials.json"
            framing.method = "newline_delimited"
            encoding.codec = "json"
        "#})
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "gcp_cloud_storage")]
impl SinkConfig for GcsSinkConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let auth = self.auth.build(Scope::DevStorageReadWrite).await?;
        let base_url = format!("{}/{}/", self.endpoint, self.bucket);
        let tls = TlsSettings::from_options(self.tls.as_ref())?;
        let client = HttpClient::new(tls, cx.proxy())?;
        let healthcheck = build_healthcheck(
            self.bucket.clone(),
            client.clone(),
            base_url.clone(),
            auth.clone(),
        )?;
        auth.spawn_regenerate_token();
        let sink = self.build_sink(client, base_url, auth, cx)?;

        Ok((sink, healthcheck))
    }

    fn input(&self) -> Input {
        Input::new(self.encoding.config().1.input_type() & DataType::Log)
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

impl GcsSinkConfig {
    fn build_sink(
        &self,
        client: HttpClient,
        base_url: String,
        auth: GcpAuthenticator,
        cx: SinkContext,
    ) -> crate::Result<VectorSink> {
        let request = self.request.into_settings();

        let batch_settings = self.batch.into_batcher_settings()?;

        let partitioner = self.key_partitioner()?;

        let protocol = get_http_scheme_from_uri(&base_url.parse::<Uri>().unwrap());

        let svc = build_gcs_inner_service(client, base_url, auth, request, self.bucket.clone());
        // This wrapper is outside the retry layer and therefore emits one terminal
        // delivery/file-send result for the whole request, after retries settle.
        let svc = EventLoggingService::new(svc);

        let request_settings = RequestSettings::new(self, cx)?;

        let sink = GcsSink::new(svc, request_settings, partitioner, batch_settings, protocol);

        Ok(VectorSink::from_event_streamsink(sink))
    }

    fn key_partitioner(&self) -> crate::Result<KeyPartitioner> {
        Ok(KeyPartitioner::new(
            Template::try_from(self.key_prefix.as_deref().unwrap_or("date=%F/"))
                .context(KeyPrefixTemplateSnafu)?,
            None,
        ))
    }
}

#[derive(Debug, Snafu)]
#[snafu(display("GCS upload failed with HTTP status {status} after retries settled"))]
struct GcsUploadError {
    status: StatusCode,
}

/// Builds the GCS upload service, including retries and terminal failure classification.
/// Non-2xx responses become errors only after retries settle so the Driver rejects and
/// counts discarded events, matching terminal SDK errors in S3 and Azure Blob.
/// This policy is independent of the request's memory or disk batching implementation.
fn build_gcs_inner_service(
    client: HttpClient,
    base_url: String,
    auth: GcpAuthenticator,
    request: TowerRequestSettings,
    bucket: String,
) -> impl Service<GcsRequest, Response = GcsResponse, Error = crate::Error, Future: Send + 'static>
+ Send
+ 'static {
    ServiceBuilder::new()
        // Keep this outside retries: GcsRetryLogic must see raw HTTP responses to
        // distinguish retryable statuses from permanent failures such as 403 and 501.
        .map_result(|result: Result<GcsResponse, crate::Error>| {
            result.and_then(|response| {
                let status = response.inner.status();
                if status.is_success() {
                    Ok(response)
                } else {
                    Err(GcsUploadError { status }.into())
                }
            })
        })
        .settings(request, GcsRetryLogic::default())
        // This mapper is inside the retry layer, so it runs once per HTTP attempt.
        // Return the same result so retry decisions still use the raw response.
        .map_result(move |result: Result<GcsResponse, _>| {
            if let Ok(response) = &result
                && response.inner.status().is_success()
            {
                response.event_log_metadata.emit_upload_event();
            }
            // Unlike the S3/Azure SDKs, a GCS HTTP error still arrives as an `Ok`
            // response here — `GcsRetryLogic` maps a non-retriable 4xx to
            // `DontRetry`, not `Err` — so the status has to be inspected, otherwise
            // failed uploads would be labelled `success`.
            let success = result
                .as_ref()
                .is_ok_and(|response| response.inner.status().is_success());
            emit_blob_file_upload_attempt(success, &bucket);
            result
        })
        .service(GcsService::new(client, base_url, auth))
}

// Settings required to produce a request that do not change per
// request. All possible values are pre-computed for direct use in
// producing a request.
#[derive(Clone, Debug)]
struct RequestSettings {
    acl: Option<HeaderValue>,
    content_type: HeaderValue,
    content_encoding: Option<HeaderValue>,
    storage_class: HeaderValue,
    headers: Vec<(HeaderName, HeaderValue)>,
    extension: String,
    time_format: String,
    append_uuid: bool,
    prepend_crypto_nonce: bool,
    encoder: (Transformer, Encoder<Framer>),
    compression: Compression,
    tz_offset: Option<FixedOffset>,
    bucket: String,
}

impl RequestBuilder<(String, Vec<Event>)> for RequestSettings {
    // type Metadata = (String, EventFinalizers);
    type Metadata = GcsMetadata;
    type Events = Vec<Event>;
    type Encoder = (Transformer, Encoder<Framer>);
    type Payload = Bytes;
    type Request = GcsRequest;
    type Error = io::Error;

    fn compression(&self) -> Compression {
        self.compression
    }

    fn encoder(&self) -> &Self::Encoder {
        &self.encoder
    }

    fn split_input(
        &self,
        input: (String, Vec<Event>),
    ) -> (Self::Metadata, RequestMetadataBuilder, Self::Events) {
        let (partition_key, mut events) = input;
        let finalizers = events.take_finalizers();
        // Attach delivery + file-send event metadata so the EventLoggingService wrapper
        // can emit VECTOR_SINK_UPLOAD_{STAGED,DELIVERED} delivery events and
        // VECTOR_FILE_SEND_EVENT for each upload, matching the S3 and Azure blob sinks.
        // The concrete blob name and byte count are filled in below in build_request().
        let builder = RequestMetadataBuilder::from_events_with_event_log(
            &events,
            Some(FileEventMetadata::default()),
        );

        // Create event metadata here as this is where the list of events are available pre-encoding
        // And we want to access this list to process the raw events to see specific field values
        let event_log_metadata = VectorEventLogSendMetadata {
            // Events are not encoded here yet, so byte size is not yet known
            // Setting as 0 here and updating when it is set in build_request()
            bytes: 0,
            events_len: events.len(),
            // Similarly the exact blob isn't determined here yet
            blob: "".to_string(),
            container: self.bucket.clone(),
            bucket: Some(self.bucket.clone()),
            count_map: generate_count_map(&events, false),
        };

        let gcp_metadata = GcsMetadata {
            partition_key,
            finalizers,
            event_log_metadata,
        };

        (gcp_metadata, builder, events)
    }

    fn build_request(
        &self,
        mut gcs_metadata: Self::Metadata,
        metadata: RequestMetadata,
        payload: EncodeResult<Self::Payload>,
    ) -> Self::Request {
        let key = gcs_metadata.partition_key;
        let finalizers = gcs_metadata.finalizers;
        // TODO: pull the seconds from the last event
        let filename = {
            let seconds = match self.tz_offset {
                Some(offset) => Utc::now().with_timezone(&offset).format(&self.time_format),
                None => Utc::now()
                    .with_timezone(&chrono::Utc)
                    .format(&self.time_format),
            };

            let base = if self.append_uuid {
                let uuid = Uuid::new_v4();
                format!("{}-{}", seconds, uuid.hyphenated())
            } else {
                seconds.to_string()
            };

            if self.prepend_crypto_nonce {
                let nonce = rand::rng().random::<u32>();
                format!("{:08x}-{}", nonce, base)
            } else {
                base
            }
        };

        let key = format!("{}{}.{}", key, filename, self.extension);
        let body = payload.into_payload();

        let mut metadata = metadata;
        // Fill in the now-known blob name and byte count so the file-send events
        // emitted by the EventLoggingService wrapper carry them.
        metadata.update_file_metadata(
            body.len(),
            gcs_metadata.event_log_metadata.events_len,
            key.clone(),
            self.bucket.clone(),
            Some(self.bucket.clone()),
        );
        gcs_metadata.event_log_metadata.bytes = body.len();
        gcs_metadata.event_log_metadata.blob = key.clone();
        gcs_metadata.event_log_metadata.emit_sending_event();

        GcsRequest {
            key,
            body,
            finalizers,
            settings: GcsRequestSettings {
                acl: self.acl.clone(),
                content_type: self.content_type.clone(),
                content_encoding: self.content_encoding.clone(),
                storage_class: self.storage_class.clone(),
                headers: self.headers.clone(),
            },
            metadata,
            event_log_metadata: gcs_metadata.event_log_metadata,
        }
    }
}

impl RequestSettings {
    fn new(config: &GcsSinkConfig, cx: SinkContext) -> crate::Result<Self> {
        let transformer = config.encoding.transformer();
        let (framer, serializer) = config.encoding.build(SinkType::MessageBased)?;
        let encoder = Encoder::<Framer>::new(framer, serializer);
        let acl = config
            .acl
            .map(|acl| HeaderValue::from_str(&to_string(acl)).unwrap());
        let content_type_str = config
            .content_type
            .as_deref()
            .unwrap_or_else(|| encoder.content_type());
        let content_type = HeaderValue::from_str(content_type_str)?;
        let content_encoding = config
            .content_encoding
            .as_deref()
            .or(config.compression.content_encoding());
        let content_encoding =
            content_encoding.map(|ce| HeaderValue::from_str(&to_string(ce)).unwrap());
        let storage_class = config.storage_class.unwrap_or_default();
        let storage_class = HeaderValue::from_str(&to_string(storage_class)).unwrap();
        let metadata = config
            .metadata
            .as_ref()
            .map(|metadata| {
                metadata
                    .iter()
                    .map(make_header)
                    .collect::<Result<Vec<_>, _>>()
            })
            .unwrap_or_else(|| Ok(vec![]))?;
        let extension = config
            .filename_extension
            .clone()
            .unwrap_or_else(|| config.compression.extension().into());
        let time_format = config.filename_time_format.clone();
        let append_uuid = config.filename_append_uuid;
        let prepend_crypto_nonce = config.filename_prepend_crypto_nonce;
        let offset = config
            .timezone
            .or(cx.globals.timezone)
            .and_then(timezone_to_offset);

        Ok(Self {
            acl,
            content_type,
            content_encoding,
            storage_class,
            headers: metadata,
            extension,
            time_format,
            append_uuid,
            prepend_crypto_nonce,
            compression: config.compression,
            encoder: (transformer, encoder),
            tz_offset: offset,
            bucket: config.bucket.clone(),
        })
    }
}

// Make a header pair from a key-value string pair
fn make_header((name, value): (&String, &String)) -> crate::Result<(HeaderName, HeaderValue)> {
    Ok((
        HeaderName::from_bytes(name.as_bytes())?,
        HeaderValue::from_str(value)?,
    ))
}

#[cfg(test)]
mod tests {
    use futures_util::{future::ready, stream};
    use http::StatusCode;
    use hyper::{Body, Response};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
    use std::{
        convert::Infallible,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use tokio::sync::{mpsc, oneshot};
    use tower::ServiceExt;
    use vector_common::internal_event::vector_event::VectorSinkEventMetadata;
    use vector_lib::{
        EstimatedJsonEncodedSizeOf,
        codecs::{
            JsonSerializerConfig, NewlineDelimitedEncoderConfig, TextSerializerConfig,
            encoding::FramingConfig,
        },
        partition::Partitioner,
        request_metadata::GroupedCountByteSize,
        stream::{Driver, DriverResponse},
    };

    use super::*;
    use crate::{
        event::{BatchNotifier, BatchStatus, LogEvent},
        sinks::util::vector_event_log::test_util::VelEventCapture,
        test_util::{
            addr::next_addr,
            components::{SINK_TAGS, run_and_assert_sink_compliance},
            http::{always_200_response, spawn_blackhole_http_server},
        },
    };

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<GcsSinkConfig>();
    }

    #[tokio::test]
    async fn component_spec_compliance() {
        let mock_endpoint = spawn_blackhole_http_server(always_200_response).await;

        let context = SinkContext::default();

        let tls = TlsSettings::default();
        let client =
            HttpClient::new(tls, context.proxy()).expect("should not fail to create HTTP client");

        let config =
            default_config((None::<FramingConfig>, JsonSerializerConfig::default()).into());
        let sink = config
            .build_sink(
                client,
                mock_endpoint.to_string(),
                GcpAuthenticator::None,
                context,
            )
            .expect("failed to build sink");

        let event = Event::Log(LogEvent::from("simple message"));
        run_and_assert_sink_compliance(sink, stream::once(ready(event)), &SINK_TAGS).await;
    }

    async fn spawn_status_server(statuses: Vec<StatusCode>) -> (Uri, Arc<AtomicUsize>) {
        spawn_status_server_with_gate(statuses, None).await
    }

    async fn spawn_status_server_with_gate(
        statuses: Vec<StatusCode>,
        response_gate: Option<mpsc::UnboundedSender<oneshot::Sender<()>>>,
    ) -> (Uri, Arc<AtomicUsize>) {
        assert!(!statuses.is_empty());
        let statuses = Arc::new(statuses);
        let request_count = Arc::new(AtomicUsize::new(0));
        let handler_count = Arc::clone(&request_count);
        let endpoint = spawn_blackhole_http_server(move |_| {
            let statuses = Arc::clone(&statuses);
            let attempt = handler_count.fetch_add(1, Ordering::SeqCst);
            let response_gate = response_gate.clone();
            async move {
                if let Some(response_gate) = response_gate {
                    let (release, held) = oneshot::channel();
                    response_gate.send(release).unwrap();
                    held.await.unwrap();
                }
                let status = statuses
                    .get(attempt)
                    .copied()
                    .unwrap_or_else(|| *statuses.last().unwrap());
                Ok::<_, Infallible>(
                    Response::builder()
                        .status(status)
                        .body(Body::empty())
                        .unwrap(),
                )
            }
        })
        .await;
        (endpoint, request_count)
    }

    fn gcs_test_request() -> (GcsRequest, VectorSinkEventMetadata) {
        let request = build_request(None, false, Compression::None, None);
        let terminal_event_log = request.metadata.event_log_metadata().clone();
        assert!(
            !terminal_event_log
                .file_send_event
                .as_ref()
                .unwrap()
                .count_map
                .is_empty()
        );
        (request, terminal_event_log)
    }

    fn gcs_test_tower_settings(retry_attempts: usize) -> TowerRequestSettings {
        let mut settings =
            TowerRequestConfig::<GcsTowerRequestConfigDefaults>::default().into_settings();
        settings.retry_attempts = retry_attempts;
        settings.retry_initial_backoff = Duration::from_millis(1);
        settings.retry_max_duration = Duration::from_millis(1);
        settings.retry_jitter_mode = crate::sinks::util::retries::JitterMode::None;
        settings.concurrency = Some(1);
        settings
    }

    fn gcs_test_client() -> HttpClient {
        let context = SinkContext::default();
        HttpClient::new(TlsSettings::default(), context.proxy())
            .expect("should create a test HTTP client")
    }

    async fn call_gcs_service(
        endpoint: String,
        retry_attempts: usize,
        event_logging_enabled: bool,
    ) -> (
        Result<GcsResponse, crate::Error>,
        VectorSinkEventMetadata,
        VelEventCapture,
    ) {
        let (request, terminal_event_log) = gcs_test_request();
        let service = EventLoggingService {
            inner: build_gcs_inner_service(
                gcs_test_client(),
                endpoint,
                GcpAuthenticator::None,
                gcs_test_tower_settings(retry_attempts),
                "test-bucket".to_owned(),
            ),
            enable_logs: event_logging_enabled,
        };
        let capture = VelEventCapture::default();
        let response = capture.capture(service.oneshot(request)).await;
        (response, terminal_event_log, capture)
    }

    fn assert_vel_terminal_counts(
        terminal_event_log: &VectorSinkEventMetadata,
        capture: &VelEventCapture,
        legacy_uploads: usize,
        delivered: u32,
        file_starts: usize,
        file_uploads: usize,
        file_errors: usize,
    ) {
        assert_eq!(capture.legacy_upload_count(), legacy_uploads);
        assert_eq!(
            terminal_event_log
                .delivery_event
                .delivered_call_count
                .load(Ordering::SeqCst),
            delivered
        );
        assert_eq!(
            capture.file_send_count("VECTOR_FILE_SEND_START"),
            file_starts
        );
        assert_eq!(
            capture.file_send_count("VECTOR_FILE_SEND_COMPLETE"),
            file_uploads
        );
        assert_eq!(
            capture.file_send_count("VECTOR_FILE_SEND_WARN"),
            file_errors
        );
    }

    #[tokio::test]
    async fn gcs_vel_emits_success_only_for_successful_http_response() {
        let (endpoint, request_count) = spawn_status_server(vec![StatusCode::OK]).await;
        let (response, terminal_event_log, capture) =
            call_gcs_service(endpoint.to_string(), 2, true).await;

        let response = response.unwrap();
        assert_eq!(response.inner.status(), StatusCode::OK);
        assert_eq!(
            response.event_status(),
            crate::event::EventStatus::Delivered
        );
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
        assert_vel_terminal_counts(&terminal_event_log, &capture, 1, 1, 1, 1, 0);
    }

    #[tokio::test]
    async fn gcs_vel_does_not_emit_success_for_terminal_403() {
        let (endpoint, request_count) = spawn_status_server(vec![StatusCode::FORBIDDEN]).await;
        let (response, terminal_event_log, capture) =
            call_gcs_service(endpoint.to_string(), 2, true).await;

        assert_eq!(
            response
                .unwrap_err()
                .downcast_ref::<GcsUploadError>()
                .unwrap()
                .status,
            StatusCode::FORBIDDEN
        );
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
        assert_vel_terminal_counts(&terminal_event_log, &capture, 0, 0, 1, 0, 1);
    }

    #[tokio::test]
    async fn gcs_vel_does_not_emit_success_when_retries_are_exhausted() {
        for status in [
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::TOO_MANY_REQUESTS,
        ] {
            let (endpoint, request_count) = spawn_status_server(vec![status]).await;
            let (response, terminal_event_log, capture) =
                call_gcs_service(endpoint.to_string(), 2, true).await;

            assert_eq!(
                response
                    .unwrap_err()
                    .downcast_ref::<GcsUploadError>()
                    .unwrap()
                    .status,
                status
            );
            assert_eq!(request_count.load(Ordering::SeqCst), 3);
            assert_vel_terminal_counts(&terminal_event_log, &capture, 0, 0, 1, 0, 1);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gcs_vel_separates_per_attempt_upload_from_terminal_delivery() {
        let (endpoint, request_count) = spawn_status_server(vec![
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::OK,
        ])
        .await;
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let local_recorder = metrics::set_default_local_recorder(&recorder);
        let (response, terminal_event_log, capture) =
            call_gcs_service(endpoint.to_string(), 2, true).await;
        drop(local_recorder);

        let response = response.unwrap();
        assert_eq!(response.inner.status(), StatusCode::OK);
        assert_eq!(request_count.load(Ordering::SeqCst), 3);
        assert_vel_terminal_counts(&terminal_event_log, &capture, 1, 1, 1, 1, 0);

        let mut attempts_by_status = HashMap::new();
        for (key, _, _, value) in snapshotter.snapshot().into_vec() {
            if key.key().name() != "blob_sink_file_uploads_total" {
                continue;
            }
            let labels: HashMap<_, _> = key
                .key()
                .labels()
                .map(|label| (label.key(), label.value()))
                .collect();
            if labels.get("bucket") == Some(&"test-bucket")
                && let DebugValue::Counter(count) = value
            {
                attempts_by_status.insert(labels["status"].to_owned(), count);
            }
        }
        assert_eq!(attempts_by_status.get("failure"), Some(&2));
        assert_eq!(attempts_by_status.get("success"), Some(&1));
    }

    #[tokio::test]
    async fn gcs_vel_emits_error_for_transport_failure() {
        let (_guard, address) = next_addr();
        let endpoint = format!("http://{address}");
        let (response, terminal_event_log, capture) = call_gcs_service(endpoint, 0, true).await;

        assert!(response.is_err());
        assert_vel_terminal_counts(&terminal_event_log, &capture, 0, 0, 1, 0, 1);
    }

    #[tokio::test]
    async fn gcs_vel_wrapper_is_transparent_when_disabled() {
        let (endpoint, request_count) = spawn_status_server(vec![StatusCode::OK]).await;
        let (response, terminal_event_log, capture) =
            call_gcs_service(endpoint.to_string(), 2, false).await;

        let response = response.unwrap();
        assert_eq!(response.inner.status(), StatusCode::OK);
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
        // The legacy per-attempt upload event is independent of the terminal wrapper flag.
        assert_vel_terminal_counts(&terminal_event_log, &capture, 1, 0, 0, 0, 0);
    }

    #[tokio::test]
    async fn gcs_vel_rejected_response_is_transparent_when_wrapper_disabled() {
        let (endpoint, request_count) = spawn_status_server(vec![StatusCode::FORBIDDEN]).await;
        let (response, terminal_event_log, capture) =
            call_gcs_service(endpoint.to_string(), 2, false).await;

        assert_eq!(
            response
                .unwrap_err()
                .downcast_ref::<GcsUploadError>()
                .unwrap()
                .status,
            StatusCode::FORBIDDEN
        );
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
        assert_vel_terminal_counts(&terminal_event_log, &capture, 0, 0, 0, 0, 0);
    }

    #[test]
    fn gcs_raw_response_and_retry_semantics_are_unchanged() {
        use crate::{event::EventStatus, sinks::util::retries::RetryLogic};

        let (request, _) = gcs_test_request();
        let retry = GcsRetryLogic::<GcsRequest>::default();
        for (status, event_status, should_retry) in [
            (StatusCode::OK, EventStatus::Delivered, false),
            (StatusCode::FORBIDDEN, EventStatus::Rejected, false),
            (StatusCode::TOO_MANY_REQUESTS, EventStatus::Rejected, true),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                EventStatus::Errored,
                true,
            ),
            (StatusCode::NOT_IMPLEMENTED, EventStatus::Errored, false),
        ] {
            let response = GcsResponse {
                inner: Response::builder()
                    .status(status)
                    .body(Body::empty())
                    .unwrap(),
                // Deliberately distinct sizes: this PR must not switch the raw
                // response's byte-count convention used by other consumers.
                metadata: RequestMetadata::new(1, 23, 17, 9, GroupedCountByteSize::default()),
                event_log_metadata: request.event_log_metadata.clone(),
            };
            assert_eq!(response.event_status(), event_status);
            assert_eq!(response.bytes_sent(), Some(17));
            assert_eq!(
                retry.should_retry_response(&response).is_retryable(),
                should_retry
            );
            assert_eq!(
                retry.should_retry_response(&response).is_successful(),
                status.is_success()
            );
        }
    }

    fn counter_value(snapshotter: &Snapshotter, name: &str, labels: &[(&str, &str)]) -> u64 {
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter_map(|(key, _, _, value)| {
                if key.key().name() == name
                    && labels.iter().all(|(name, value)| {
                        key.key()
                            .labels()
                            .any(|label| label.key() == *name && label.value() == *value)
                    })
                    && let DebugValue::Counter(count) = value
                {
                    return Some(count);
                }
                None
            })
            .sum()
    }

    // Exercise the production request builder, HTTP service, retry stack, VEL wrapper,
    // and Driver together. Holding every response also checks that retries cannot
    // prematurely finalize or count a batch as sent/discarded.
    async fn assert_gcs_terminal_upload(
        statuses: Vec<StatusCode>,
        expected_attempts: usize,
        expected_status: BatchStatus,
        enable_logs: bool,
    ) {
        let (gates_tx, mut gates_rx) = mpsc::unbounded_channel();
        let (endpoint, request_count) =
            spawn_status_server_with_gate(statuses, Some(gates_tx)).await;
        let config = default_config(
            (
                Some(NewlineDelimitedEncoderConfig::new()),
                JsonSerializerConfig::default(),
            )
                .into(),
        );
        let builder = request_settings(&config, SinkContext::default());
        let (notifier, mut receiver) = BatchNotifier::new_with_receiver();
        let events = ["first event", "second event", "third event"]
            .into_iter()
            .map(|message| Event::Log(LogEvent::from(message).with_batch_notifier(&notifier)))
            .collect();
        drop(notifier);
        let (metadata, metadata_builder, events) = builder.split_input(("test/".into(), events));
        let payload = builder.encode_events(events).unwrap();
        let request_metadata = metadata_builder.build(&payload);
        let event_count = request_metadata.event_count() as u64;
        let encoded_bytes = request_metadata.request_encoded_size() as u64;
        let event_bytes = request_metadata
            .events_estimated_json_encoded_byte_size()
            .size()
            .unwrap()
            .1
            .get() as u64;
        assert_eq!(event_count, 3);
        assert!(encoded_bytes > 0);
        let request = builder.build_request(metadata, request_metadata, payload);
        assert_eq!(request.finalizers.len(), 3);
        let terminal_event_log = request.metadata.event_log_metadata().clone();
        let service = EventLoggingService {
            inner: build_gcs_inner_service(
                gcs_test_client(),
                endpoint.to_string(),
                GcpAuthenticator::None,
                gcs_test_tower_settings(2),
                "test-bucket".to_owned(),
            ),
            enable_logs,
        };
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _local_recorder = metrics::set_default_local_recorder(&recorder);
        let capture = VelEventCapture::default();
        let succeeded = expected_status == BatchStatus::Delivered;

        capture.capture(async {
            let driver = Driver::new(stream::iter([request]), service).protocol("http").run();
            tokio::pin!(driver);
            for attempt in 1..=expected_attempts {
                let gate = tokio::select! {
                    result = &mut driver => panic!("Driver finished before attempt {attempt}: {result:?}"),
                    gate = gates_rx.recv() => gate.expect("HTTP server closed before expected attempt"),
                };
                assert_eq!(request_count.load(Ordering::SeqCst), attempt);
                assert!(matches!(receiver.try_recv(), Err(oneshot::error::TryRecvError::Empty)));
                for metric in [
                    "component_discarded_events_total",
                    "component_errors_total",
                    "component_sent_events_total",
                    "component_sent_event_bytes_total",
                    "component_sent_bytes_total",
                ] {
                    assert_eq!(counter_value(&snapshotter, metric, &[]), 0, "{metric} before response {attempt}");
                }
                assert_vel_terminal_counts(&terminal_event_log, &capture, 0, 0, usize::from(enable_logs), 0, 0);
                gate.send(()).unwrap();
            }
            tokio::select! {
                result = &mut driver => result.unwrap(),
                gate = gates_rx.recv() => panic!("Unexpected HTTP retry after the terminal response: {}", gate.is_some()),
            }
            assert_eq!(receiver.await, expected_status);
        }).await;

        assert_eq!(request_count.load(Ordering::SeqCst), expected_attempts);
        assert_eq!(
            counter_value(
                &snapshotter,
                "component_discarded_events_total",
                &[("intentional", "false")]
            ),
            if succeeded { 0 } else { event_count }
        );
        assert_eq!(
            counter_value(
                &snapshotter,
                "component_errors_total",
                &[("error_type", "request_failed"), ("stage", "sending")]
            ),
            u64::from(!succeeded)
        );
        for (metric, expected) in [
            ("component_sent_events_total", event_count),
            ("component_sent_event_bytes_total", event_bytes),
            ("component_sent_bytes_total", encoded_bytes),
        ] {
            assert_eq!(
                counter_value(&snapshotter, metric, &[]),
                if succeeded { expected } else { 0 },
                "{metric}"
            );
        }
        for (metric, failure_label) in [
            ("blob_sink_file_uploads_total", "failure"),
            ("sink_requests_completed_total", "failed"),
        ] {
            assert_eq!(
                counter_value(&snapshotter, metric, &[("status", "success")]),
                u64::from(succeeded)
            );
            assert_eq!(
                counter_value(&snapshotter, metric, &[("status", failure_label)]),
                expected_attempts as u64 - u64::from(succeeded)
            );
        }
        assert_vel_terminal_counts(
            &terminal_event_log,
            &capture,
            usize::from(succeeded),
            u32::from(succeeded && enable_logs),
            usize::from(enable_logs),
            usize::from(succeeded && enable_logs),
            usize::from(!succeeded && enable_logs),
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gcs_terminal_driver_delivers_2xx() {
        for status in [StatusCode::OK, StatusCode::CREATED, StatusCode::NO_CONTENT] {
            tokio::time::timeout(
                Duration::from_secs(10),
                assert_gcs_terminal_upload(vec![status], 1, BatchStatus::Delivered, true),
            )
            .await
            .unwrap();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gcs_terminal_driver_retries_429_and_500_before_delivery() {
        tokio::time::timeout(
            Duration::from_secs(10),
            assert_gcs_terminal_upload(
                vec![
                    StatusCode::TOO_MANY_REQUESTS,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    StatusCode::OK,
                ],
                3,
                BatchStatus::Delivered,
                true,
            ),
        )
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gcs_terminal_driver_rejects_403_and_501_without_retry() {
        for status in [StatusCode::FORBIDDEN, StatusCode::NOT_IMPLEMENTED] {
            tokio::time::timeout(
                Duration::from_secs(10),
                assert_gcs_terminal_upload(vec![status], 1, BatchStatus::Rejected, true),
            )
            .await
            .unwrap();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gcs_terminal_driver_rejects_exhausted_429_and_500() {
        for status in [
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            tokio::time::timeout(
                Duration::from_secs(10),
                assert_gcs_terminal_upload(vec![status], 3, BatchStatus::Rejected, true),
            )
            .await
            .unwrap();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gcs_terminal_driver_rejection_does_not_depend_on_vel() {
        tokio::time::timeout(
            Duration::from_secs(10),
            assert_gcs_terminal_upload(
                vec![StatusCode::INTERNAL_SERVER_ERROR],
                3,
                BatchStatus::Rejected,
                false,
            ),
        )
        .await
        .unwrap();
    }

    #[test]
    fn gcs_encode_event_apply_rules() {
        crate::test_util::trace_init();

        let message = "hello world".to_string();
        let mut event = LogEvent::from(message);
        event.insert("key", "value");

        let sink_config = GcsSinkConfig {
            key_prefix: Some("key: {{ key }}".into()),
            ..default_config((None::<FramingConfig>, TextSerializerConfig::default()).into())
        };
        let key = sink_config
            .key_partitioner()
            .unwrap()
            .partition(&Event::Log(event))
            .expect("key wasn't provided");

        assert_eq!(key, "key: value");
    }

    fn request_settings(sink_config: &GcsSinkConfig, context: SinkContext) -> RequestSettings {
        RequestSettings::new(sink_config, context).expect("Could not create request settings")
    }

    fn build_request(
        extension: Option<&str>,
        uuid: bool,
        compression: Compression,
        content_encoding: Option<String>,
    ) -> GcsRequest {
        build_request_opts(extension, uuid, false, compression, content_encoding)
    }

    fn build_request_opts(
        extension: Option<&str>,
        uuid: bool,
        prepend_crypto_nonce: bool,
        compression: Compression,
        content_encoding: Option<String>,
    ) -> GcsRequest {
        let context = SinkContext::default();
        let sink_config = GcsSinkConfig {
            key_prefix: Some("key/".into()),
            filename_time_format: "date".into(),
            filename_extension: extension.map(Into::into),
            filename_append_uuid: uuid,
            filename_prepend_crypto_nonce: prepend_crypto_nonce,
            content_encoding,
            compression,
            ..default_config(
                (
                    Some(NewlineDelimitedEncoderConfig::new()),
                    JsonSerializerConfig::default(),
                )
                    .into(),
            )
        };
        let log = LogEvent::default().into();
        let key = sink_config
            .key_partitioner()
            .unwrap()
            .partition(&log)
            .expect("key wasn't provided");

        let mut byte_size = GroupedCountByteSize::new_untagged();
        byte_size.add_event(&log, log.estimated_json_encoded_size_of());

        let request_settings = request_settings(&sink_config, context);
        let (metadata, metadata_request_builder, _events) =
            request_settings.split_input((key, vec![log]));
        let payload = EncodeResult::uncompressed(Bytes::new(), byte_size);
        let request_metadata = metadata_request_builder.build(&payload);

        request_settings.build_request(metadata, request_metadata, payload)
    }

    #[test]
    fn gcs_build_request() {
        let req = build_request(Some("ext"), false, Compression::None, None);
        assert_eq!(req.key, "key/date.ext".to_string());

        let req = build_request(None, false, Compression::None, None);
        assert_eq!(req.key, "key/date.log".to_string());

        let req = build_request(None, false, Compression::gzip_default(), None);
        assert_eq!(req.key, "key/date.log.gz".to_string());

        let req = build_request(None, true, Compression::gzip_default(), None);
        assert_ne!(req.key, "key/date.log.gz".to_string());
    }

    #[test]
    fn gcs_content_type_default() {
        let context = SinkContext::default();
        let sink_config = GcsSinkConfig {
            content_type: None,
            ..default_config((None::<FramingConfig>, TextSerializerConfig::default()).into())
        };

        let request_settings = request_settings(&sink_config, context);
        // Should default to encoder's content type which is "text/plain" for text codec
        assert_eq!(
            request_settings.content_type.to_str().unwrap(),
            "text/plain"
        );
    }

    #[test]
    fn gcs_content_type_custom() {
        let context = SinkContext::default();
        let sink_config = GcsSinkConfig {
            content_type: Some("text/plain; charset=utf-8".to_string()),
            ..default_config((None::<FramingConfig>, TextSerializerConfig::default()).into())
        };

        let request_settings = request_settings(&sink_config, context);
        // Should use custom content type
        assert_eq!(
            request_settings.content_type.to_str().unwrap(),
            "text/plain; charset=utf-8"
        );
    }

    #[test]
    fn gcs_content_type_invalid() {
        let context = SinkContext::default();
        let sink_config = GcsSinkConfig {
            // Invalid header value with newline character
            content_type: Some("text/plain\nInvalid".to_string()),
            ..default_config((None::<FramingConfig>, TextSerializerConfig::default()).into())
        };

        let result = RequestSettings::new(&sink_config, context);
        // Should return an error, not panic
        assert!(result.is_err());
    }

    #[test]
    fn gcs_build_request_with_crypto_nonce() {
        let req = build_request_opts(None, false, true, Compression::None, None);
        let filename = req
            .key
            .strip_prefix("key/")
            .unwrap()
            .strip_suffix(".log")
            .unwrap();
        // Should be "<8-hex-chars>-date"
        let (nonce, rest) = filename.split_at(9);
        assert!(nonce[..8].chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(&nonce[8..], "-");
        assert_eq!(rest, "date");
    }

    #[test]
    fn gcs_build_request_respect_content_encoding_option() {
        let compression_scheme = Compression::gzip_default();
        let request = build_request(None, false, compression_scheme, None);
        assert_eq!(
            request.settings.content_encoding,
            HeaderValue::from_str(compression_scheme.content_encoding().unwrap()).ok()
        );

        let custom_encoding = "test_encoding";
        let request = build_request(
            None,
            false,
            compression_scheme,
            Some(custom_encoding.to_string()),
        );
        assert_eq!(
            request.settings.content_encoding,
            HeaderValue::from_str(custom_encoding).ok()
        );
    }
}
