use std::{
    future::ready,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt};
use http::Request;
use hyper::Body;
use smallvec::SmallVec;
use tokio::io::AsyncRead;
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
        GcsObjectProcessingFailed, GcsObjectProcessingSucceeded, ObjectStorageBlobTruncated,
        StreamClosedError, emit_object_storage_ack_metrics,
    },
    line_agg::{self, LineAgg},
    sources::gcp_gcs::{Compression, GcpGcsConfig},
    sources::util::object_storage_adaptive_concurrency::AdaptiveObjectStorageConcurrency,
    sources::util::object_storage_rate_limit::ByteCounter,
    sources::util::{TolerateTrailingEof, TruncatedArchiveFramer},
};

pub use crate::sources::gcp_gcs::pubsub::ProcessingError;

const GCS_BASE_URL: &str = "https://storage.googleapis.com";

/// Handles downloading GCS objects via the GCS XML API and processing
/// their content into Vector events.
#[derive(Clone)]
pub(super) struct GcsDownloader {
    client: HttpClient,
    auth: GcpAuthenticator,
    compression: Compression,
    decoder: Decoder,
    multiline: Option<line_agg::Config>,
    tolerate_truncated_archives: bool,
    project: String,
    /// GCS base URL, parsed and validated at construction. Defaults to
    /// [`GCS_BASE_URL`] and can be overridden via the source's `storage_endpoint` config.
    base_url: url::Url,
    adaptive: Option<AdaptiveObjectStorageConcurrency>,
}

/// Parses the GCS base URL, defaulting to [`GCS_BASE_URL`]. Returns a config error
/// (rather than letting the source panic on the first download) for an endpoint that
/// is unparseable or cannot be a base URL, so a bad `storage_endpoint` fails at startup.
fn parse_base_url(endpoint: Option<String>) -> crate::Result<url::Url> {
    let raw = endpoint.unwrap_or_else(|| GCS_BASE_URL.to_string());
    let url = url::Url::parse(&raw)
        .map_err(|e| format!("invalid gcp_gcs storage_endpoint {raw:?}: {e}"))?;
    if url.cannot_be_a_base() {
        return Err(format!(
            "gcp_gcs storage_endpoint {raw:?} must be a base URL (e.g. https://host)"
        )
        .into());
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
            tolerate_truncated_archives: false,
            project,
            base_url: parse_base_url(endpoint)?,
            adaptive: None,
        })
    }

    pub(super) const fn with_tolerate_truncated_archives(mut self, tolerate: bool) -> Self {
        self.tolerate_truncated_archives = tolerate;
        self
    }

    pub(super) fn with_adaptive_concurrency(
        mut self,
        adaptive: Option<AdaptiveObjectStorageConcurrency>,
    ) -> Self {
        self.adaptive = adaptive;
        self
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
        byte_counter: &ByteCounter,
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
        // Object write time (the JSON API's `updated`), the GCS analogue of S3/Azure
        // `Last-Modified`. Absent or unparseable means no stamp rather than a wrong lag.
        let last_modified = response
            .headers()
            .get("last-modified")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| DateTime::parse_from_rfc2822(v).ok())
            .map(|dt| dt.with_timezone(&Utc));
        let content_length = response
            .headers()
            .get(http::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());
        // A truncated archive can only be told apart from a short transfer when the object size
        // is known, so without Content-Length this object is read without tolerance.
        let tolerate_truncated_archives =
            self.tolerate_truncated_archives && content_length.is_some();
        if self.tolerate_truncated_archives && content_length.is_none() {
            warn!(
                message = "GCS response has no Content-Length; reading without truncated-archive tolerance.",
                bucket = %bucket,
                key = %key,
                internal_log_rate_limit = true
            );
        }

        debug!(
            message = "Fetched GCS object.",
            bucket = %bucket,
            key = %key,
            internal_log_rate_limit = true
        );

        let compressed_bytes = Arc::new(AtomicU64::new(0));
        // Set when the body itself fails (e.g. the connection drops), as opposed to the decoder
        // or the pipeline stopping the read early.
        let transport_failed = Arc::new(AtomicBool::new(false));
        let count_compressed_bytes = tolerate_truncated_archives;
        let body_stream = response.into_body().map({
            let compressed_bytes = Arc::clone(&compressed_bytes);
            let transport_failed = Arc::clone(&transport_failed);
            move |result| {
                result
                    .inspect(|chunk| {
                        if count_compressed_bytes {
                            compressed_bytes.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                        }
                    })
                    .map_err(|error| {
                        transport_failed.store(true, Ordering::Relaxed);
                        std::io::Error::other(error)
                    })
            }
        });

        let (batch, receiver) = BatchNotifier::maybe_new_with_receiver(acknowledgements);

        // Empty strings don't match any compression codec, so passing them as Some("") is safe.
        let object_reader = self
            .compression
            .resolve(Some(&content_encoding), Some(&content_type), &key)
            .build_decoder(body_stream)
            .await;
        let truncated_tail = Arc::new(AtomicBool::new(false));
        let dropped_tail_bytes = Arc::new(AtomicU64::new(0));
        let object_reader: Box<dyn AsyncRead + Send + Unpin> = if tolerate_truncated_archives {
            Box::new(TolerateTrailingEof::new(
                object_reader,
                Arc::clone(&truncated_tail),
            ))
        } else {
            object_reader
        };

        let mut read_error = None;
        let bytes_received = bytes_received.clone();
        let events_received = events_received.clone();
        // Count decoded bytes so the caller can pace on the batch total.
        let byte_counter = byte_counter.clone();
        let framer = if tolerate_truncated_archives {
            TruncatedArchiveFramer::with_dropped_tail_bytes(
                self.decoder.framer.clone(),
                Arc::clone(&truncated_tail),
                Arc::clone(&dropped_tail_bytes),
            )
        } else {
            TruncatedArchiveFramer::new(self.decoder.framer.clone(), Arc::clone(&truncated_tail))
        };

        let lines: Box<dyn Stream<Item = Bytes> + Send + Unpin> = Box::new(
            FramedRead::new(object_reader, framer)
                .map(|res| {
                    res.inspect(|bytes| {
                        bytes_received.emit(ByteSize(bytes.len()));
                        byte_counter.add(bytes.len());
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
                        // Upload-time stamp for the Zerobus sink's object-uploaded-to-saved
                        // freshness.
                        crate::sources::util::stamp_object_uploaded_ms(log_event, last_modified);
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

        // A short read is an incomplete download only when the transfer itself ended early. A
        // decode or pipeline failure also stops the read before the end of the body, and must
        // keep its own error so it is classified (and acknowledged) as before.
        if tolerate_truncated_archives
            && let Some(content_length) = content_length
            && (transport_failed.load(Ordering::Relaxed)
                || (read_error.is_none() && send_error.is_none()))
        {
            let pulled = compressed_bytes.load(Ordering::Relaxed);
            if pulled < content_length {
                return Err(ProcessingError::IncompleteObjectDownload {
                    bytes_read: pulled,
                    content_length,
                    bucket: bucket.clone(),
                    key: key.clone(),
                });
            }
        }

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

        let batch_status = match receiver {
            None => None,
            Some(receiver) => Some(receiver.await),
        };
        let delivered = batch_status
            .as_ref()
            .is_none_or(|status| *status == BatchStatus::Delivered);

        if truncated_tail.load(Ordering::Relaxed) && delivered {
            emit!(ObjectStorageBlobTruncated {
                cloud: "gcp",
                container: &bucket,
                blob: &key,
                dropped_tail_bytes: Some(dropped_tail_bytes.load(Ordering::Relaxed)),
            });
        }

        match batch_status {
            None => Ok(()),
            Some(result) => {
                if result != BatchStatus::Delivered
                    && let Some(adaptive) = &self.adaptive
                {
                    adaptive.note_failed_delivery();
                }
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
        test_downloader_with_options(base_url, Compression::Auto, false)
    }

    fn test_downloader_with_options(
        base_url: &str,
        compression: Compression,
        tolerate_truncated_archives: bool,
    ) -> GcsDownloader {
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
            compression,
            decoder,
            None,
            "test-project".into(),
            Some(base_url.into()),
        )
        .expect("test downloader must build")
        .with_tolerate_truncated_archives(tolerate_truncated_archives)
    }

    /// A bad `storage_endpoint` fails fast at construction with a config error
    /// rather than panicking on the first download. Absent one, the public default
    /// is used. `Some(url)` expects that URL as the parsed base, `None` expects an error.
    #[test]
    fn parse_base_url_defaults_and_rejects_invalid() {
        let cases: [(Option<&str>, Option<&str>); 4] = [
            (None, Some("https://storage.googleapis.com/")),
            (
                Some("https://gcs.example.com"),
                Some("https://gcs.example.com/"),
            ),
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
        spawn_test_server_bytes_with_length(status, body, None, false).await
    }

    /// Like `spawn_test_server_bytes`, but the response carries extra headers, so tests
    /// can exercise header-derived behavior (e.g. the `Last-Modified` upload stamp).
    async fn spawn_test_server_bytes_with_headers(
        status: u16,
        body: bytes::Bytes,
        headers: &[(&str, String)],
    ) -> SocketAddr {
        spawn_test_server_bytes_full(status, body, None, false, headers).await
    }

    async fn spawn_test_server_bytes_with_length(
        status: u16,
        body: bytes::Bytes,
        content_length: Option<u64>,
        chunked: bool,
    ) -> SocketAddr {
        spawn_test_server_bytes_full(status, body, content_length, chunked, &[]).await
    }

    async fn spawn_test_server_bytes_full(
        status: u16,
        body: bytes::Bytes,
        content_length: Option<u64>,
        chunked: bool,
        headers: &[(&str, String)],
    ) -> SocketAddr {
        use std::sync::Arc;
        let body = Arc::new(body);
        let headers: Vec<(String, String)> = headers
            .iter()
            .map(|(name, value)| (name.to_string(), value.clone()))
            .collect();
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let server = Server::from_tcp(listener.into_std().unwrap())
                .unwrap()
                .serve(make_service_fn(move |_| {
                    let body = Arc::clone(&body);
                    let headers = headers.clone();
                    async move {
                        Ok::<_, Infallible>(service_fn(move |_req| {
                            let body = Arc::clone(&body);
                            let headers = headers.clone();
                            async move {
                                let mut builder = Response::builder().status(status);
                                for (name, value) in &headers {
                                    builder = builder.header(name.as_str(), value.as_str());
                                }
                                if let Some(content_length) = content_length {
                                    builder = builder.header("content-length", content_length);
                                }
                                let response_body = if chunked {
                                    builder = builder.header("transfer-encoding", "chunked");
                                    Body::wrap_stream(futures::stream::iter([Ok::<
                                        _,
                                        std::io::Error,
                                    >(
                                        (*body).clone()
                                    )]))
                                } else {
                                    Body::from((*body).clone())
                                };
                                Ok::<_, Infallible>(builder.body(response_body).unwrap())
                            }
                        }))
                    }
                }));
            let _ = server.await;
        });

        addr
    }

    async fn spawn_test_server_short_content_length(
        body: bytes::Bytes,
        content_length: u64,
    ) -> SocketAddr {
        use tokio::io::AsyncWriteExt as _;

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {content_length}\r\nConnection: close\r\n\r\n"
            )
            .into_bytes();
            response.extend_from_slice(&body);
            stream.write_all(&response).await.unwrap();
            stream.shutdown().await.unwrap();
        });
        addr
    }

    fn gzip_member(bytes: &[u8]) -> Vec<u8> {
        use std::io::Write as _;

        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    fn truncated_gzip_body() -> Vec<u8> {
        let mut body = gzip_member(b"complete-1\ncomplete-2\n");
        let mut last_member = gzip_member(b"complete-3\npartial-tail");
        last_member.truncate(last_member.len() - 3);
        body.extend_from_slice(&last_member);
        body
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
            .and_then(|v| v.as_str());
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
                &ByteCounter::default(),
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
            .and_then(|v| v.as_str());
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
            .and_then(|v| v.as_str());
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
                &ByteCounter::default(),
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
                &ByteCounter::default(),
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

    #[tokio::test]
    async fn truncated_gzip_with_newline_framing_ingests_complete_lines() {
        let body = truncated_gzip_body();
        let addr = spawn_test_server_bytes(200, bytes::Bytes::from(body)).await;
        let downloader =
            test_downloader_with_options(&format!("http://{addr}"), Compression::Gzip, true);
        let (mut tx, rx) = SourceSender::new_test_finalize(EventStatus::Delivered);
        let process = async move {
            let result = downloader
                .process_object(
                    "my-bucket",
                    "logs/archive.gz",
                    None,
                    &mut tx,
                    LogNamespace::Legacy,
                    true,
                    false,
                    &register!(BytesReceived::from(Protocol::HTTP)),
                    &register!(EventsReceived),
                    &ByteCounter::default(),
                )
                .await;
            drop(tx);
            result
        };
        let (result, events) = tokio::join!(process, rx.collect::<Vec<_>>());

        assert!(
            result.is_ok(),
            "truncated archive should be tolerated: {result:?}"
        );
        assert_eq!(events.len(), 3, "the partial final line must be dropped");
        assert_eq!(events[0].as_log()["message"], "complete-1".into());
        assert_eq!(events[1].as_log()["message"], "complete-2".into());
        assert_eq!(events[2].as_log()["message"], "complete-3".into());
    }

    #[tokio::test]
    async fn short_gcs_download_is_not_acknowledged() {
        let body = gzip_member(b"event\n");
        let expected_size = body.len() as u64 + 10;
        let addr =
            spawn_test_server_short_content_length(bytes::Bytes::from(body), expected_size).await;
        let downloader =
            test_downloader_with_options(&format!("http://{addr}"), Compression::Gzip, true);
        let (mut tx, _rx) = SourceSender::new_test_finalize(EventStatus::Delivered);

        let result = downloader
            .process_object(
                "my-bucket",
                "logs/short.gz",
                None,
                &mut tx,
                LogNamespace::Legacy,
                false,
                true,
                &register!(BytesReceived::from(Protocol::HTTP)),
                &register!(EventsReceived),
                &ByteCounter::default(),
            )
            .await;

        let error = result.expect_err("short transfer must fail before acknowledging");
        assert!(matches!(
            error,
            ProcessingError::IncompleteObjectDownload { .. }
        ));
        assert!(error.is_retriable());
        assert!(!error.should_acknowledge(true));
    }

    /// Runs `process_object` with truncation tolerance on and acknowledgements off, returning the
    /// result and the events that reached the pipeline.
    async fn process_tolerant(
        addr: SocketAddr,
        key: &str,
    ) -> (Result<(), ProcessingError>, Vec<Event>) {
        let downloader =
            test_downloader_with_options(&format!("http://{addr}"), Compression::Gzip, true);
        let (mut tx, rx) = SourceSender::new_test_finalize(EventStatus::Delivered);
        let key = key.to_owned();
        let process = async move {
            let result = downloader
                .process_object(
                    "my-bucket",
                    &key,
                    None,
                    &mut tx,
                    LogNamespace::Legacy,
                    false,
                    true,
                    &register!(BytesReceived::from(Protocol::HTTP)),
                    &register!(EventsReceived),
                    &ByteCounter::default(),
                )
                .await;
            drop(tx);
            result
        };
        tokio::join!(process, rx.collect::<Vec<_>>())
    }

    /// Without Content-Length a short transfer cannot be told apart from a truncated archive,
    /// so the object is read without tolerance: a healthy object is ingested normally.
    #[tokio::test]
    async fn unknown_gcs_object_size_reads_without_tolerance() {
        let body = gzip_member(b"line-1\nline-2\n");
        let addr =
            spawn_test_server_bytes_with_length(200, bytes::Bytes::from(body), None, true).await;

        let (result, events) = process_tolerant(addr, "logs/unknown-size.gz").await;

        assert!(
            result.is_ok(),
            "healthy object must be ingested: {result:?}"
        );
        assert_eq!(events.len(), 2);
    }

    /// A truncated archive without Content-Length keeps the non-tolerant read error, which the
    /// configured failure policy may acknowledge.
    #[tokio::test]
    async fn unknown_gcs_object_size_keeps_truncation_read_error() {
        let addr = spawn_test_server_bytes_with_length(
            200,
            bytes::Bytes::from(truncated_gzip_body()),
            None,
            true,
        )
        .await;

        let (result, _events) = process_tolerant(addr, "logs/unknown-size.gz").await;

        let error = result.expect_err("truncation without a known size must not be tolerated");
        assert!(
            matches!(error, ProcessingError::ReadObject { .. }),
            "unexpected error: {error:?}"
        );
        assert!(error.should_acknowledge(true));
    }

    /// A gzip header followed by an invalid deflate block, padded well past what a socket buffers,
    /// so the decoder fails before most of the body has been pulled.
    fn corrupt_gzip_body() -> Vec<u8> {
        let mut body = vec![0x1f, 0x8b, 0x08, 0x00, 0, 0, 0, 0, 0x00, 0xff];
        // BFINAL=1, BTYPE=11 (reserved): an invalid block type.
        body.push(0x07);
        body.resize(4 * 1024 * 1024, 0);
        body
    }

    /// A decode failure stops the read early, but it is not a short transfer: it must stay a
    /// `ReadObject` error that `acknowledge_failed_message` can acknowledge, not redeliver forever.
    #[tokio::test]
    async fn corrupt_gzip_with_tolerance_keeps_read_error() {
        let body = corrupt_gzip_body();
        let length = body.len() as u64;
        let addr =
            spawn_test_server_bytes_with_length(200, bytes::Bytes::from(body), Some(length), false)
                .await;

        let (result, _events) = process_tolerant(addr, "logs/corrupt.gz").await;

        let error = result.expect_err("corrupt archive must fail");
        assert!(
            matches!(error, ProcessingError::ReadObject { .. }),
            "unexpected error: {error:?}"
        );
        assert!(!error.must_redeliver());
        assert!(error.should_acknowledge(true));
    }

    /// A gzip object of incompressible lines, large enough that most of it is still unread when
    /// the pipeline stops accepting events.
    fn large_gzip_body() -> Vec<u8> {
        use std::fmt::Write as _;

        let mut lines = String::new();
        let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
        while lines.len() < 8 * 1024 * 1024 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            writeln!(lines, "{state:016x}{:016x}", state.rotate_left(17)).unwrap();
        }
        gzip_member(lines.as_bytes())
    }

    /// A downstream send failure also leaves the body unread; it must stay `PipelineSend` so the
    /// caller still sees back-pressure.
    #[tokio::test]
    async fn pipeline_send_failure_with_tolerance_keeps_send_error() {
        let body = large_gzip_body();
        let length = body.len() as u64;
        let addr =
            spawn_test_server_bytes_with_length(200, bytes::Bytes::from(body), Some(length), false)
                .await;
        let downloader =
            test_downloader_with_options(&format!("http://{addr}"), Compression::Gzip, true);
        let (mut tx, rx) = SourceSender::new_test_finalize(EventStatus::Delivered);
        drop(rx);

        let result = downloader
            .process_object(
                "my-bucket",
                "logs/large.gz",
                None,
                &mut tx,
                LogNamespace::Legacy,
                false,
                true,
                &register!(BytesReceived::from(Protocol::HTTP)),
                &register!(EventsReceived),
                &ByteCounter::default(),
            )
            .await;

        let error = result.expect_err("closed pipeline must fail the send");
        assert!(
            matches!(error, ProcessingError::PipelineSend { .. }),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn truncated_gzip_option_off_keeps_read_error() {
        let addr = spawn_test_server_bytes(200, bytes::Bytes::from(truncated_gzip_body())).await;
        let downloader =
            test_downloader_with_options(&format!("http://{addr}"), Compression::Gzip, false);
        let (mut tx, _rx) = SourceSender::new_test_finalize(EventStatus::Delivered);

        let result = downloader
            .process_object(
                "my-bucket",
                "logs/archive.gz",
                None,
                &mut tx,
                LogNamespace::Legacy,
                false,
                false,
                &register!(BytesReceived::from(Protocol::HTTP)),
                &register!(EventsReceived),
                &ByteCounter::default(),
            )
            .await;

        assert!(matches!(result, Err(ProcessingError::ReadObject { .. })));
    }

    // -------------------------------------------------------------------------
    // Object upload stamp: the response's Last-Modified (object write time)
    // -------------------------------------------------------------------------

    /// The GET response's `Last-Modified` header is the object's write time, so it is
    /// stamped as `%databricks.object_uploaded_ms` on every event — the GCS analogue of
    /// the S3/Azure `last_modified` stamp. The stamp must not flip a Legacy event's
    /// runtime namespace.
    #[tokio::test]
    async fn process_object_stamps_object_uploaded_ms_from_last_modified() {
        let last_modified = chrono::DateTime::parse_from_rfc2822("Mon, 16 Nov 2026 10:00:00 GMT")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let addr = spawn_test_server_bytes_with_headers(
            200,
            bytes::Bytes::from_static(b"first\nsecond\n"),
            &[("Last-Modified", "Mon, 16 Nov 2026 10:00:00 GMT".to_string())],
        )
        .await;
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
                &ByteCounter::default(),
            )
            .await;

        assert!(result.is_ok(), "200 response must succeed: {result:?}");
        let events = collect_n(rx, 2).await;
        assert_eq!(events.len(), 2);
        for event in &events {
            let log = event.as_log();
            assert_eq!(
                log.get(metadata_path!("databricks", "object_uploaded_ms")),
                Some(&vrl::value::Value::from(last_modified.timestamp_millis())),
                "every event must carry the object write time"
            );
            assert_eq!(log.namespace(), LogNamespace::Legacy);
        }
    }

    /// No `Last-Modified` header (or an unparseable one) must leave the stamp absent —
    /// never a `Utc::now()` stand-in, which would record a wrong lag.
    #[tokio::test]
    async fn process_object_omits_object_uploaded_ms_without_last_modified() {
        for headers in [
            &[],
            &[("Last-Modified", "not an rfc2822 date".to_string())][..],
        ] {
            let addr = spawn_test_server_bytes_with_headers(
                200,
                bytes::Bytes::from_static(b"line\n"),
                headers,
            )
            .await;
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
                    &ByteCounter::default(),
                )
                .await;

            assert!(result.is_ok(), "200 response must succeed: {result:?}");
            let events = collect_n(rx, 1).await;
            assert!(
                events[0]
                    .as_log()
                    .get(metadata_path!("databricks", "object_uploaded_ms"))
                    .is_none(),
                "stamp must be absent without a parseable Last-Modified ({headers:?})"
            );
        }
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
                    &ByteCounter::default(),
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
