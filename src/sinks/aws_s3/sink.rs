use std::io;

use bytes::Bytes;
use chrono::{FixedOffset, Utc};
use rand::Rng;
use uuid::Uuid;
use vector_common::internal_event::vector_event::file_send_event::FileEventMetadata;
use vector_lib::event::event_log::generate_count_map;
use vector_lib::{codecs::encoding::Framer, event::Finalizable, request_metadata::RequestMetadata};

use crate::{
    codecs::{Encoder, Transformer},
    event::Event,
    internal_events::vector_event::VectorEventLogSendMetadata,
    sinks::{
        s3_common::{
            config::S3Options,
            partitioner::S3PartitionKey,
            service::{S3Metadata, S3Request},
        },
        util::{
            Compression, RequestBuilder, metadata::RequestMetadataBuilder,
            request_builder::EncodeResult, vector_event_log::use_event_log_sink_wrapping,
        },
    },
};

#[derive(Clone)]
pub struct S3RequestOptions {
    pub bucket: String,
    pub filename_time_format: String,
    pub filename_append_uuid: bool,
    pub filename_prepend_crypto_nonce: bool,
    pub filename_extension: Option<String>,
    pub api_options: S3Options,
    pub encoder: (Transformer, Encoder<Framer>),
    pub compression: Compression,
    pub filename_tz_offset: Option<FixedOffset>,
}

impl RequestBuilder<(S3PartitionKey, Vec<Event>)> for S3RequestOptions {
    type Metadata = S3Metadata;
    type Events = Vec<Event>;
    type Encoder = (Transformer, Encoder<Framer>);
    type Payload = Bytes;
    type Request = S3Request;
    type Error = io::Error; // TODO: this is ugly.

    fn compression(&self) -> Compression {
        self.compression
    }

    fn encoder(&self) -> &Self::Encoder {
        &self.encoder
    }

    fn split_input(
        &self,
        input: (S3PartitionKey, Vec<Event>),
    ) -> (Self::Metadata, RequestMetadataBuilder, Self::Events) {
        let (partition_key, mut events) = input;
        // We don't need to pass file metadata here especially since it isn't fully populated yet
        // Gate the expensive per-event computation (event cloning + field lookups) on whether
        // sink event logging is enabled. When disabled the count_map is empty and all emit_*
        // calls become no-ops, so skipping the build is safe.
        let vel_enabled = use_event_log_sink_wrapping();
        let builder = if vel_enabled {
            RequestMetadataBuilder::from_events_with_event_log(
                &events,
                Some(FileEventMetadata::default()),
            )
        } else {
            RequestMetadataBuilder::from_events(&events)
        };

        let finalizers = events.take_finalizers();
        let s3_key_prefix = partition_key.key_prefix.clone();

        // TODO: There's a good amount of overlapping code for event logs and we will continue to
        // need to update more sinks with this functionality. Might be tricky but might be good to
        // refactor/update the base request builder class to minimize this duplication

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
            count_map: if vel_enabled {
                generate_count_map(&events, false)
            } else {
                Default::default()
            },
        };

        let metadata = S3Metadata {
            partition_key,
            s3_key: s3_key_prefix,
            count: events.len(),
            finalizers,
            event_log_metadata,
        };

        (metadata, builder, events)
    }

    fn build_request(
        &self,
        mut s3metadata: Self::Metadata,
        request_metadata: RequestMetadata,
        payload: EncodeResult<Self::Payload>,
    ) -> Self::Request {
        let filename = {
            let formatted_ts = match self.filename_tz_offset {
                Some(offset) => Utc::now()
                    .with_timezone(&offset)
                    .format(self.filename_time_format.as_str()),
                None => Utc::now()
                    .with_timezone(&Utc)
                    .format(self.filename_time_format.as_str()),
            };

            let base = if self.filename_append_uuid {
                format!("{formatted_ts}-{}", Uuid::new_v4().hyphenated())
            } else {
                formatted_ts.to_string()
            };

            if self.filename_prepend_crypto_nonce {
                let nonce = rand::rng().random::<u32>();
                format!("{:08x}-{}", nonce, base)
            } else {
                base
            }
        };

        let ssekms_key_id = s3metadata.partition_key.ssekms_key_id.clone();
        let mut s3_options = self.api_options.clone();
        s3_options.ssekms_key_id = ssekms_key_id;

        let extension = self
            .filename_extension
            .as_ref()
            .cloned()
            .unwrap_or_else(|| self.compression.extension().into());

        s3metadata.s3_key = format_s3_key(&s3metadata.s3_key, &filename, &extension);

        let body = payload.into_payload();

        let mut request_metadata = request_metadata;
        // Update some components of the metadata since they've been computed now
        request_metadata.update_file_metadata(
            body.len(),
            s3metadata.count,
            s3metadata.s3_key.clone(),
            self.bucket.clone(),
            Some(self.bucket.clone()),
        );
        s3metadata.event_log_metadata.bytes = body.len();
        s3metadata.event_log_metadata.blob = s3metadata.s3_key.clone();
        s3metadata.event_log_metadata.emit_sending_event();

        S3Request {
            body,
            bucket: self.bucket.clone(),
            metadata: s3metadata,
            request_metadata,
            content_encoding: self.compression.content_encoding(),
            options: s3_options,
        }
    }
}

fn format_s3_key(s3_key: &str, filename: &str, extension: &str) -> String {
    if extension.is_empty() {
        format!("{s3_key}{filename}")
    } else {
        format!("{s3_key}{filename}.{extension}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::Encoder;
    use crate::sinks::s3_common::partitioner::S3PartitionKey;
    use vector_lib::codecs::{NewlineDelimitedEncoder, TextSerializerConfig, encoding::Framer};
    use vector_lib::request_metadata::GroupedCountByteSize;

    #[test]
    fn test_format_s3_key() {
        assert_eq!(
            "s3_key_filename.txt",
            format_s3_key("s3_key_", "filename", "txt")
        );
        assert_eq!("s3_key_filename", format_s3_key("s3_key_", "filename", ""));
    }

    fn make_request_options(prepend_crypto_nonce: bool, append_uuid: bool) -> S3RequestOptions {
        let encoder = Encoder::<Framer>::new(
            NewlineDelimitedEncoder::default().into(),
            TextSerializerConfig::default().build().into(),
        );

        S3RequestOptions {
            bucket: "test-bucket".to_string(),
            filename_time_format: "%s".to_string(),
            filename_append_uuid: append_uuid,
            filename_prepend_crypto_nonce: prepend_crypto_nonce,
            filename_extension: Some("log".to_string()),
            api_options: S3Options::default(),
            encoder: (Transformer::default(), encoder),
            compression: Compression::None,
            filename_tz_offset: None,
        }
    }

    fn build_request_with_options(options: &S3RequestOptions) -> S3Request {
        let partition_key = S3PartitionKey {
            key_prefix: "test-prefix/".to_string(),
            ssekms_key_id: None,
        };
        let events: Vec<Event> = vec![];
        let (metadata, metadata_builder, _events) =
            <S3RequestOptions as RequestBuilder<(S3PartitionKey, Vec<Event>)>>::split_input(
                options,
                (partition_key, events),
            );
        let byte_size = GroupedCountByteSize::new_untagged();
        let payload = EncodeResult::uncompressed(Bytes::new(), byte_size);
        let request_metadata = metadata_builder.build(&payload);
        options.build_request(metadata, request_metadata, payload)
    }

    #[test]
    fn s3_build_request_with_crypto_nonce() {
        let options = make_request_options(true, false);
        let request = build_request_with_options(&options);
        let filename = request
            .metadata
            .s3_key
            .strip_prefix("test-prefix/")
            .unwrap()
            .strip_suffix(".log")
            .unwrap();
        // Should be "<8-hex-chars>-<timestamp>"
        let (nonce, rest) = filename.split_at(9);
        assert_eq!(nonce.len(), 9);
        assert!(nonce[..8].chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(&nonce[8..], "-");
        assert!(rest.chars().all(|c| c.is_ascii_digit()));
    }
}
