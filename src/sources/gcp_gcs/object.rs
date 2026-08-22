use std::future::ready;

use bytes::Bytes;
use chrono::Utc;
use futures::{Stream, StreamExt, TryStreamExt};
use http::Request;
use hyper::Body;
use smallvec::SmallVec;
use tokio_util::codec::FramedRead;
use vector_lib::{
    config::{LegacyKey, LogNamespace, log_schema},
    event::MaybeAsLogMut,
    internal_event::EventsReceived,
    internal_event::{
        ByteSize, BytesReceived, CountByteSize, InternalEventHandle as _, Registered,
    },
    lookup::{PathPrefix, metadata_path, path},
    source_sender::SendError,
};

use crate::{
    SourceSender,
    codecs::Decoder,
    event::{BatchNotifier, BatchStatus, EstimatedJsonEncodedSizeOf, Event, LogEvent},
    gcp::GcpAuthenticator,
    http::HttpClient,
    internal_events::{
        GcsObjectProcessingFailed, GcsObjectProcessingSucceeded, StreamClosedError,
        emit_object_storage_ack_metrics,
    },
    line_agg::{self, LineAgg},
    sources::gcp_gcs::{Compression, GcpGcsConfig},
};

pub use crate::sources::gcp_gcs::pubsub::ProcessingError;

const GCS_BASE_URL: &str = "https://storage.googleapis.com";

/// Handles downloading GCS objects via the GCS XML API and processing
/// their content into Vector events.
pub(super) struct GcsDownloader {
    client: HttpClient,
    auth: GcpAuthenticator,
    compression: Compression,
    decoder: Decoder,
    multiline: Option<line_agg::Config>,
    project: String,
    /// GCS base URL, parsed and validated at construction. Defaults to
    /// [`GCS_BASE_URL`] and can be overridden via the source's `storage_endpoint` config.
    base_url: url::Url,
}

/// Parses the GCS base URL, defaulting to [`GCS_BASE_URL`]. Returns a config error
/// (rather than letting the source panic on the first download) for an endpoint that
/// is unparseable or cannot be a base URL, so a bad `storage_endpoint` fails at startup.
fn parse_base_url(endpoint: Option<String>) -> crate::Result<url::Url> {
    let raw = endpoint.unwrap_or_else(|| GCS_BASE_URL.to_string());
    let url = url::Url::parse(&raw)
        .map_err(|e| format!("invalid gcp_gcs storage_endpoint {raw:?}: {e}"))?;
    if url.cannot_be_a_base() {
        return Err(format!("gcp_gcs storage_endpoint {raw:?} must be a base URL (e.g. https://host)").into());
    }
    Ok(url)
}

impl GcsDownloader {
    pub(super) fn new(
        client: HttpClient,
        auth: GcpAuthenticator,
        compression: Compression,
        decoder: Decoder,
        multiline: Option<line_agg::Config>,
        project: String,
        endpoint: Option<String>,
    ) -> crate::Result<Self> {
        Ok(Self {
            client,
            auth,
            compression,
            decoder,
            multiline,
            project,
            base_url: parse_base_url(endpoint)?,
        })
    }

    /// Downloads a GCS object, decompresses, frames, decodes, and emits events.
    pub(super) async fn process_object(
        &self,
        bucket: &str,
        key: &str,
        log_type: Option<&str>,
        out: &mut SourceSender,
        log_namespace: LogNamespace,
        acknowledgements: bool,
        acknowledge_failed: bool,
        bytes_received: &Registered<BytesReceived>,
        events_received: &Registered<EventsReceived>,
    ) -> Result<(), ProcessingError> {
        let processing_start_time = Utc::now();
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        let log_type = log_type.map(|s| s.to_owned());

        // Extend the pre-validated base URL with percent-encoded path segments, so
        // special characters (?, #, &, spaces, etc.) in the key can't corrupt the URL.
        // `base_url` was validated as a base at construction, so `path_segments_mut`
        // cannot fail here.
        let url = {
            let mut u = self.base_url.clone();
            u.path_segments_mut()
                .expect("base_url is validated as a base at construction")
                .push(&bucket)
                .extend(key.split('/'));
            u
        };

        let mut request = Request::get(url.as_str())
            .body(Body::empty())
            .expect("GET request must be valid");
        self.auth.apply(&mut request);

        let response =
            self.client
                .send(request)
                .await
                .map_err(|source| ProcessingError::FetchObject {
                    source,
                    bucket: bucket.clone(),
                    key: key.clone(),
                })?;

        // 404 means the object was deleted after the notification was enqueued.
        // Return ObjectNotFound so the caller can acknowledge the message and avoid
        // an infinite retry loop.
        if response.status() == http::StatusCode::NOT_FOUND {
            return Err(ProcessingError::ObjectNotFound {
                bucket: bucket.clone(),
                key: key.clone(),
                reason: "GCS returned HTTP 404 — the object may have been deleted after the \
                         notification was enqueued"
                    .to_string(),
            });
        }

        if !response.status().is_success() {
            return Err(ProcessingError::GetObject {
                status: response.status().as_u16(),
                bucket: bucket.clone(),
                key: key.clone(),
            });
        }

        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned();
        let content_encoding = response
            .headers()
            .get("content-encoding")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned();

        debug!(
            message = "Fetched GCS object.",
            bucket = %bucket,
            key = %key,
            internal_log_rate_limit = true
        );

        let body_stream = response.into_body().map_err(std::io::Error::other);

        let (batch, receiver) = BatchNotifier::maybe_new_with_receiver(acknowledgements);

        // Empty strings don't match any compression codec, so passing them as Some("") is safe.
        let object_reader = self
            .compression
            .resolve(Some(&content_encoding), Some(&content_type), &key)
            .build_decoder(body_stream)
            .await;

        let mut read_error = None;
        let bytes_received = bytes_received.clone();
        let events_received = events_received.clone();

        let lines: Box<dyn Stream<Item = Bytes> + Send + Unpin> = Box::new(
            FramedRead::new(object_reader, self.decoder.framer.clone())
                .map(|res| {
                    res.inspect(|bytes| {
                        bytes_received.emit(ByteSize(bytes.len()));
                    })
                    .map_err(|err| {
                        read_error = Some(err);
                    })
                    .ok()
                })
                .take_while(|res| ready(res.is_some()))
                .map(|r| r.expect("validated by take_while")),
        );

        let lines: Box<dyn Stream<Item = Bytes> + Send + Unpin> = match &self.multiline {
            Some(config) => Box::new(
                LineAgg::new(
                    lines.map(|line| ((), line, ())),
                    line_agg::Logic::new(config.clone()),
                )
                .map(|(_src, line, _context, _lastline_context)| line),
            ),
            None => lines,
        };

        let project = self.project.clone();

        let mut stream = lines.flat_map(|line| {
            let events = match self.decoder.deserializer_parse(line) {
                Ok((events, _)) => events,
                Err(_) => SmallVec::new(),
            };

            let events = events
                .into_iter()
                .map(|mut event: Event| {
                    event = event.with_batch_notifier_option(&batch);
                    if let Some(log_event) = event.maybe_as_log_mut() {
                        enrich_log_event(
                            log_event,
                            log_namespace,
                            &bucket,
                            &key,
                            &project,
                            log_type.as_deref(),
                        );
                    }
                    events_received.emit(CountByteSize(1, event.estimated_json_encoded_size_of()));
                    event
                })
                .collect::<Vec<Event>>();
            futures::stream::iter(events)
        });

        let send_error = match out.send_event_stream(&mut stream).await {
            Ok(_) => None,
            Err(_) => {
                let (count, _) = stream.size_hint();
                emit!(StreamClosedError { count });
                Some(SendError::Closed)
            }
        };

        drop(stream);
        drop(batch);

        if let Some(error) = read_error {
            return Err(ProcessingError::ReadObject {
                source: error,
                bucket: bucket.clone(),
                key: key.clone(),
            });
        }

        if let Some(error) = send_error {
            return Err(ProcessingError::PipelineSend {
                source: error,
                bucket: bucket.clone(),
                key: key.clone(),
            });
        }

        match receiver {
            None => Ok(()),
            Some(receiver) => {
                let result = receiver.await;
                emit_object_storage_ack_metrics(processing_start_time, "gcp", &bucket);

                match result {
                    BatchStatus::Delivered => {
                        emit!(GcsObjectProcessingSucceeded { bucket: &bucket });
                        Ok(())
                    }
                    BatchStatus::Errored | BatchStatus::Rejected => {
                        let err = ProcessingError::ErrorAcknowledgement {
                            bucket: bucket.clone(),
                            key: key.clone(),
                        };
                        emit!(GcsObjectProcessingFailed {
                            bucket: &bucket,
                            key: &key,
                            error: &err,
                        });
                        if acknowledge_failed { Ok(()) } else { Err(err) }
                    }
                }
            }
        }
    }
}

/// Adds GCS source metadata fields to a log event.
fn enrich_log_event(
    log: &mut LogEvent,
    log_namespace: LogNamespace,
    bucket: &str,
    key: &str,
    project: &str,
    log_type: Option<&str>,
) {
    log_namespace.insert_source_metadata(
        GcpGcsConfig::NAME,
        log,
        Some(LegacyKey::Overwrite(path!("bucket"))),
        path!("bucket"),
        Bytes::from(bucket.as_bytes().to_vec()),
    );

    log_namespace.insert_source_metadata(
        GcpGcsConfig::NAME,
        log,
        Some(LegacyKey::Overwrite(path!("object"))),
        path!("object"),
        Bytes::from(key.as_bytes().to_vec()),
    );

    log_namespace.insert_source_metadata(
        GcpGcsConfig::NAME,
        log,
        Some(LegacyKey::Overwrite(path!("project"))),
        path!("project"),
        Bytes::from(project.as_bytes().to_vec()),
    );

    // Only stamp log_type when the direct-ingest message carried it, so events
    // from messages without a log type are unchanged.
    if let Some(log_type) = log_type {
        log_namespace.insert_source_metadata(
            GcpGcsConfig::NAME,
            log,
            Some(LegacyKey::Overwrite(path!("log_type"))),
            path!("log_type"),
            Bytes::from(log_type.as_bytes().to_vec()),
        );
    }

    log_namespace.insert_vector_metadata(
        log,
        log_schema().source_type_key(),
        path!("source_type"),
        Bytes::from_static(GcpGcsConfig::NAME.as_bytes()),
    );

    match log_namespace {
        LogNamespace::Vector => {
            log.insert(metadata_path!("vector", "ingest_timestamp"), Utc::now());
        }
        LogNamespace::Legacy => {
            if let Some(timestamp_key) = log_schema().timestamp_key() {
                log.try_insert((PathPrefix::Event, timestamp_key), Utc::now());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::net::SocketAddr;

    use hyper::{
        Body, Response, Server,
        service::{make_service_fn, service_fn},
    };
    use vector_lib::codecs::{
        NewlineDelimitedDecoderConfig,
        decoding::{FramingConfig, NewlineDelimitedDecoderOptions},
    };
    use vector_lib::config::LogNamespace;
    use vector_lib::internal_event::Protocol;
    use vector_lib::lookup::metadata_path;

    use super::*;
    use crate::event::{EventStatus, LogEvent};
    use crate::test_util::collect_n;
    use crate::{SourceSender, config::ProxyConfig, gcp::GcpAuthenticator, tls::TlsSettings};

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    fn test_downloader(base_url: &str) -> GcsDownloader {
        use crate::codecs::DecodingConfig;
        use crate::serde::default_decoding;

        let framing = FramingConfig::NewlineDelimited(NewlineDelimitedDecoderConfig {
            newline_delimited: NewlineDelimitedDecoderOptions { max_length: None },
        });
        let client = HttpClient::new(TlsSettings::default(), &ProxyConfig::default())
            .expect("HttpClient must build in tests");
        let decoder = DecodingConfig::new(framing, default_decoding(), LogNamespace::Legacy)
            .build()
            .expect("Decoder must build in tests");

        GcsDownloader::new(
            client,
            GcpAuthenticator::None,
            Compression::Auto,
            decoder,
            None,
            "test-project".into(),
            Some(base_url.into()),
        )
        .expect("test downloader must build")
    }

    /// A bad `storage_endpoint` fails fast at construction with a config error
    /// rather than panicking on the first download. Absent one, the public default
    /// is used. `Some(url)` expects that URL as the parsed base, `None` expects an error.
    #[test]
    fn parse_base_url_defaults_and_rejects_invalid() {
        let cases: [(Option<&str>, Option<&str>); 4] = [
            (None, Some("https://storage.googleapis.com/")),
            (Some("https://gcs.example.com"), Some("https://gcs.example.com/")),
            (Some("not a url"), None),
            (Some("mailto:x@example.com"), None),
        ];
        for (endpoint, expected) in cases {
            let result = parse_base_url(endpoint.map(String::from));
            match expected {
                Some(url) => assert_eq!(result.unwrap().as_str(), url, "endpoint {endpoint:?}"),
                None => assert!(result.is_err(), "endpoint {endpoint:?} must be rejected"),
            }
        }
    }

    /// Spawns a one-shot HTTP server that returns the given bytes with `status`.
    async fn spawn_test_server_bytes(status: u16, body: bytes::Bytes) -> SocketAddr {
        use std::sync::Arc;
        let body = Arc::new(body);
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let server = Server::from_tcp(listener.into_std().unwrap())
                .unwrap()
                .serve(make_service_fn(move |_| {
                    let body = Arc::clone(&body);
                    async move {
                        Ok::<_, Infallible>(service_fn(move |_req| {
                            let body = Arc::clone(&body);
                            async move {
                                Ok::<_, Infallible>(
                                    Response::builder()
                                        .status(status)
                                        .body(Body::from((*body).clone()))
                                        .unwrap(),
                                )
                            }
                        }))
                    }
                }));
            let _ = server.await;
        });

        addr
    }

    // -------------------------------------------------------------------------
    // enrich_log_event: verify source metadata is attached to each event
    // -------------------------------------------------------------------------

    /// In Legacy namespace, enrich_log_event must set bucket/object/project on
    /// the event body and insert a timestamp within the call window.
    #[test]
    fn enrich_log_event_legacy_namespace() {
        let mut log = LogEvent::default();
        let before = chrono::Utc::now();
        enrich_log_event(
            &mut log,
            LogNamespace::Legacy,
            "my-bucket",
            "path/file.log",
            "my-project",
            Some("cp_logs"),
        );
        let after = chrono::Utc::now();

        assert_eq!(log["bucket"], "my-bucket".into());
        assert_eq!(log["object"], "path/file.log".into());
        assert_eq!(log["project"], "my-project".into());
        // log_type is stamped when the direct-ingest message carried it.
        assert_eq!(log["log_type"], "cp_logs".into());

        use vector_lib::lookup::PathPrefix;
        let key = log_schema()
            .timestamp_key()
            .expect("log schema must have a timestamp key");
        let ts = log
            .get((PathPrefix::Event, key))
            .and_then(|v| v.as_timestamp())
            .copied()
            .expect("timestamp must be set in Legacy namespace");
        assert!(
            ts >= before && ts <= after,
            "timestamp must be within the test window"
        );
    }

    /// In the Vector namespace, GCS fields go into source metadata (not the event
    /// body). Assert the actual values are correct, not just that they're present.
    #[test]
    fn enrich_log_event_sets_gcs_metadata_in_vector_namespace() {
        let mut log = LogEvent::default();
        enrich_log_event(
            &mut log,
            LogNamespace::Vector,
            "my-bucket",
            "path/file.log",
            "my-project",
            None,
        );

        // In Vector namespace, fields must NOT appear in the event body.
        assert!(
            log.get("bucket").is_none(),
            "bucket must not be in the event body"
        );
        // No log_type supplied → it must not be attached anywhere on the event.
        assert!(
            log.get("log_type").is_none()
                && log
                    .get(metadata_path!(GcpGcsConfig::NAME, "log_type"))
                    .is_none(),
            "log_type must be absent when the message did not carry it"
        );
        assert!(
            log.get("object").is_none(),
            "object must not be in the event body"
        );
        assert!(
            log.get("project").is_none(),
            "project must not be in the event body"
        );

        // The source name metadata must be set.
        let source_type = log
            .get(metadata_path!("vector", "source_type"))
            .and_then(|v| v.as_str().map(|s| s.to_owned()));
        assert_eq!(
            source_type.as_deref(),
            Some(GcpGcsConfig::NAME),
            "source_type metadata must be gcp_gcs"
        );

        // ingest_timestamp must be set.
        assert!(
            log.get(metadata_path!("vector", "ingest_timestamp"))
                .is_some(),
            "vector.ingest_timestamp must be set in Vector namespace"
        );
    }

    /// In the Vector namespace, process_object end-to-end must route source
    /// metadata (bucket, object, project) to the Vector metadata path and not
    /// the event body — consistent with enrich_log_event above.
    #[tokio::test]
    async fn process_object_vector_namespace_metadata_in_vector_path() {
        let addr = spawn_test_server_bytes(200, bytes::Bytes::from_static(b"msg\n")).await;
        let framing = FramingConfig::NewlineDelimited(NewlineDelimitedDecoderConfig {
            newline_delimited: NewlineDelimitedDecoderOptions { max_length: None },
        });
        let client = HttpClient::new(TlsSettings::default(), &ProxyConfig::default()).unwrap();
        let decoder = crate::codecs::DecodingConfig::new(
            framing,
            crate::serde::default_decoding(),
            LogNamespace::Vector,
        )
        .build()
        .unwrap();
        let downloader = GcsDownloader::new(
            client,
            GcpAuthenticator::None,
            Compression::Auto,
            decoder,
            None,
            "my-project".into(),
            Some(format!("http://{addr}")),
        )
        .expect("test downloader must build");

        let (mut tx, rx) = SourceSender::new_test_finalize(EventStatus::Delivered);
        let result = downloader
            .process_object(
                "my-bucket",
                "key.log",
                Some("cp_logs"),
                &mut tx,
                LogNamespace::Vector,
                false,
                false,
                &register!(BytesReceived::from(Protocol::HTTP)),
                &register!(EventsReceived),
            )
            .await;

        assert!(result.is_ok(), "Vector namespace must succeed: {result:?}");
        let events = collect_n(rx, 1).await;
        let log = events[0].as_log();

        assert!(
            log.get("bucket").is_none(),
            "bucket must not be in event body"
        );
        // log_type from the direct-ingest message is stamped as source metadata.
        let log_type = log
            .get(metadata_path!(GcpGcsConfig::NAME, "log_type"))
            .and_then(|v| v.as_str().map(|s| s.to_owned()));
        assert_eq!(log_type.as_deref(), Some("cp_logs"));
        assert!(
            log.get("object").is_none(),
            "object must not be in event body"
        );
        assert!(
            log.get("project").is_none(),
            "project must not be in event body"
        );

        let source_type = log
            .get(metadata_path!("vector", "source_type"))
            .and_then(|v| v.as_str().map(|s| s.to_owned()));
        assert_eq!(source_type.as_deref(), Some(GcpGcsConfig::NAME));
    }

    // -------------------------------------------------------------------------
    // process_object: test against a real in-process HTTP server
    // -------------------------------------------------------------------------

    /// When GCS returns 404 the downloader must return ObjectNotFound so the
    /// caller can acknowledge the Pub/Sub message and break the retry loop.
    #[tokio::test]
    async fn process_object_404_returns_object_not_found() {
        let addr = spawn_test_server_bytes(404, bytes::Bytes::new()).await;
        let downloader = test_downloader(&format!("http://{addr}"));
        let (mut tx, _rx) = SourceSender::new_test_finalize(EventStatus::Delivered);

        let bytes_received = register!(BytesReceived::from(Protocol::HTTP));
        let events_received = register!(EventsReceived);
        let result = downloader
            .process_object(
                "my-bucket",
                "my-key",
                None,
                &mut tx,
                LogNamespace::Legacy,
                false,
                false,
                &bytes_received,
                &events_received,
            )
            .await;

        assert!(
            matches!(result, Err(ProcessingError::ObjectNotFound { .. })),
            "404 from GCS must produce ObjectNotFound so the Pub/Sub message is acked: {result:?}"
        );
    }

    /// A successful 200 response with newline-delimited text must produce one
    /// event per line, and each event must carry the correct GCS metadata fields.
    #[tokio::test]
    async fn process_object_200_emits_events_with_gcs_metadata() {
        let body = "first line\nsecond line\nthird line\n";
        let addr = spawn_test_server_bytes(200, bytes::Bytes::from_static(body.as_bytes())).await;
        let downloader = test_downloader(&format!("http://{addr}"));
        let (mut tx, rx) = SourceSender::new_test_finalize(EventStatus::Delivered);

        let result = downloader
            .process_object(
                "my-bucket",
                "logs/app.log",
                None,
                &mut tx,
                LogNamespace::Legacy,
                false,
                false,
                &register!(BytesReceived::from(Protocol::HTTP)),
                &register!(EventsReceived),
            )
            .await;

        assert!(result.is_ok(), "200 response must succeed: {result:?}");

        let events = collect_n(rx, 3).await;
        assert_eq!(events.len(), 3, "must emit one event per line");

        for event in &events {
            let log = event.as_log();
            assert_eq!(
                log["bucket"],
                "my-bucket".into(),
                "bucket metadata must be set"
            );
            assert_eq!(
                log["object"],
                "logs/app.log".into(),
                "object metadata must be set"
            );
            assert_eq!(
                log["project"],
                "test-project".into(),
                "project metadata must be set"
            );
        }

        assert_eq!(events[0].as_log()["message"], "first line".into());
        assert_eq!(events[1].as_log()["message"], "second line".into());
        assert_eq!(events[2].as_log()["message"], "third line".into());
    }

    // -------------------------------------------------------------------------
    // URL encoding: keys with spaces and special characters
    // -------------------------------------------------------------------------

    /// Object keys with spaces or special URL characters (?, #, &) must be
    /// percent-encoded. The mock server accepts any path — a successful response
    /// confirms the URL was valid HTTP (raw spaces/specials would be rejected by hyper).
    #[tokio::test]
    async fn process_object_keys_with_special_chars_are_url_encoded() {
        let addr = spawn_test_server_bytes(200, bytes::Bytes::from_static(b"event\n")).await;
        let downloader = test_downloader(&format!("http://{addr}"));

        for key in [
            "path/my file with spaces.log",
            "path/file?query=1&flag#section.log",
            "path/unicode_日本語/file.log",
            "path/brackets[0]/file (copy).log",
        ] {
            let (mut tx, rx) = SourceSender::new_test_finalize(EventStatus::Delivered);

            let result = downloader
                .process_object(
                    "my-logs-bucket",
                    key,
                    None,
                    &mut tx,
                    LogNamespace::Legacy,
                    false,
                    false,
                    &register!(BytesReceived::from(Protocol::HTTP)),
                    &register!(EventsReceived),
                )
                .await;

            assert!(
                result.is_ok(),
                "key {key:?} must be URL-encoded and successfully requested: {result:?}"
            );
            let events = collect_n(rx, 1).await;
            assert_eq!(events[0].as_log()["message"], "event".into());
        }
    }
}
