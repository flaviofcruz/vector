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
}

impl GcsDownloader {
    pub(super) fn new(
        client: HttpClient,
        auth: GcpAuthenticator,
        compression: Compression,
        decoder: Decoder,
        multiline: Option<line_agg::Config>,
        project: String,
    ) -> Self {
        Self {
            client,
            auth,
            compression,
            decoder,
            multiline,
            project,
        }
    }

    /// Downloads a GCS object, decompresses, frames, decodes, and emits events.
    pub(super) async fn process_object(
        &self,
        bucket: &str,
        key: &str,
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

        // Build the GCS XML API URL using the `url` crate so that the bucket
        // name and each segment of the object key are correctly percent-encoded.
        // This handles spaces, `?`, `#`, `&`, and other URL-special characters
        // automatically without hardcoding character sets.
        let url = {
            let mut u = url::Url::parse(GCS_BASE_URL).expect("GCS base URL is valid");
            u.path_segments_mut()
                .expect("GCS base URL has a path")
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
                        enrich_log_event(log_event, log_namespace, &bucket, &key, &project);
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
