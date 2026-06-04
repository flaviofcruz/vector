use bytes::Bytes;
use chrono::Utc;
use vector_lib::{
    EstimatedJsonEncodedSizeOf,
    codecs::{
        NewlineDelimitedEncoder, TextSerializerConfig,
        encoding::{Framer, FramingConfig},
    },
    partition::Partitioner,
    request_metadata::GroupedCountByteSize,
};

use super::{config::AzureBlobSinkConfig, request_builder::AzureBlobRequestOptions};
use crate::{
    codecs::{Encoder, EncodingConfigWithFraming},
    event::{Event, LogEvent},
    sinks::util::{
        Compression,
        request_builder::{EncodeResult, RequestBuilder},
    },
};

fn default_config(encoding: EncodingConfigWithFraming) -> AzureBlobSinkConfig {
    AzureBlobSinkConfig {
        connection_string: Default::default(),
        container_name: Default::default(),
        storage_account: Default::default(),
        blob_prefix: Default::default(),
        blob_time_format: Default::default(),
        blob_append_uuid: Default::default(),
        blob_prepend_crypto_nonce: false,
        encoding,
        compression: Compression::gzip_default(),
        batch: Default::default(),
        request: Default::default(),
        acknowledgements: Default::default(),
    }
}

#[test]
fn generate_config() {
    crate::test_util::test_generate_config::<AzureBlobSinkConfig>();
}

#[test]
fn azure_blob_build_request_without_compression() {
    let log = Event::Log(LogEvent::from("test message"));
    let compression = Compression::None;
    let container_name = String::from("logs");
    let sink_config = AzureBlobSinkConfig {
        blob_prefix: "blob".try_into().unwrap(),
        container_name: container_name.clone(),
        ..default_config((None::<FramingConfig>, TextSerializerConfig::default()).into())
    };
    let blob_time_format = String::from("");
    let blob_append_uuid = false;

    let key = sink_config
        .key_partitioner()
        .unwrap()
        .partition(&log)
        .expect("key wasn't provided");

    let request_options = AzureBlobRequestOptions {
        container_name,
        storage_account: Some(String::from("some-account")),
        blob_time_format,
        blob_append_uuid,
        blob_prepend_crypto_nonce: false,
        encoder: (
            Default::default(),
            Encoder::<Framer>::new(
                NewlineDelimitedEncoder::default().into(),
                TextSerializerConfig::default().build().into(),
            ),
        ),
        compression,
    };

    let mut byte_size = GroupedCountByteSize::new_untagged();
    byte_size.add_event(&log, log.estimated_json_encoded_size_of());

    let (metadata, request_metadata_builder, _events) =
        request_options.split_input((key, vec![log]));

    let payload = EncodeResult::uncompressed(Bytes::new(), byte_size);
    let request_metadata = request_metadata_builder.build(&payload);
    let request = request_options.build_request(metadata, request_metadata, payload);

    assert_eq!(request.metadata.partition_key, "blob.log".to_string());
    assert_eq!(request.content_encoding, None);
    assert_eq!(request.content_type, "text/plain");
}

#[test]
fn azure_blob_build_request_with_compression() {
    let log = Event::Log(LogEvent::from("test message"));
    let compression = Compression::gzip_default();
    let container_name = String::from("logs");
    let sink_config = AzureBlobSinkConfig {
        blob_prefix: "blob".try_into().unwrap(),
        container_name: container_name.clone(),
        ..default_config((None::<FramingConfig>, TextSerializerConfig::default()).into())
    };
    let blob_time_format = String::from("");
    let blob_append_uuid = false;

    let key = sink_config
        .key_partitioner()
        .unwrap()
        .partition(&log)
        .expect("key wasn't provided");

    let request_options = AzureBlobRequestOptions {
        container_name,
        storage_account: Some(String::from("some-account")),
        blob_time_format,
        blob_append_uuid,
        blob_prepend_crypto_nonce: false,
        encoder: (
            Default::default(),
            Encoder::<Framer>::new(
                NewlineDelimitedEncoder::default().into(),
                TextSerializerConfig::default().build().into(),
            ),
        ),
        compression,
    };

    let mut byte_size = GroupedCountByteSize::new_untagged();
    byte_size.add_event(&log, log.estimated_json_encoded_size_of());

    let (metadata, request_metadata_builder, _events) =
        request_options.split_input((key, vec![log]));

    let payload = EncodeResult::uncompressed(Bytes::new(), byte_size);
    let request_metadata = request_metadata_builder.build(&payload);
    let request = request_options.build_request(metadata, request_metadata, payload);

    assert_eq!(request.metadata.partition_key, "blob.log.gz".to_string());
    assert_eq!(request.content_encoding, Some("gzip"));
    assert_eq!(request.content_type, "text/plain");
}

#[test]
fn azure_blob_build_request_with_time_format() {
    let log = Event::Log(LogEvent::from("test message"));
    let compression = Compression::None;
    let container_name = String::from("logs");
    let sink_config = AzureBlobSinkConfig {
        blob_prefix: "blob".try_into().unwrap(),
        container_name: container_name.clone(),
        ..default_config((None::<FramingConfig>, TextSerializerConfig::default()).into())
    };
    let blob_time_format = String::from("%F");
    let blob_append_uuid = false;

    let key = sink_config
        .key_partitioner()
        .unwrap()
        .partition(&log)
        .expect("key wasn't provided");

    let request_options = AzureBlobRequestOptions {
        container_name,
        storage_account: Some(String::from("some-account")),
        blob_time_format,
        blob_append_uuid,
        blob_prepend_crypto_nonce: false,
        encoder: (
            Default::default(),
            Encoder::<Framer>::new(
                NewlineDelimitedEncoder::default().into(),
                TextSerializerConfig::default().build().into(),
            ),
        ),
        compression,
    };

    let mut byte_size = GroupedCountByteSize::new_untagged();
    byte_size.add_event(&log, log.estimated_json_encoded_size_of());

    let (metadata, request_metadata_builder, _events) =
        request_options.split_input((key, vec![log]));

    let payload = EncodeResult::uncompressed(Bytes::new(), byte_size);
    let request_metadata = request_metadata_builder.build(&payload);
    let request = request_options.build_request(metadata, request_metadata, payload);

    assert_eq!(
        request.metadata.partition_key,
        format!("blob{}.log", Utc::now().format("%F"))
    );
    assert_eq!(request.content_encoding, None);
    assert_eq!(request.content_type, "text/plain");
}

#[test]
fn azure_blob_build_request_with_uuid() {
    let log = Event::Log(LogEvent::from("test message"));
    let compression = Compression::None;
    let container_name = String::from("logs");
    let sink_config = AzureBlobSinkConfig {
        blob_prefix: "blob".try_into().unwrap(),
        container_name: container_name.clone(),
        ..default_config((None::<FramingConfig>, TextSerializerConfig::default()).into())
    };
    let blob_time_format = String::from("");
    let blob_append_uuid = true;

    let key = sink_config
        .key_partitioner()
        .unwrap()
        .partition(&log)
        .expect("key wasn't provided");

    let request_options = AzureBlobRequestOptions {
        container_name,
        storage_account: Some(String::from("some-account")),
        blob_time_format,
        blob_append_uuid,
        blob_prepend_crypto_nonce: false,
        encoder: (
            Default::default(),
            Encoder::<Framer>::new(
                NewlineDelimitedEncoder::default().into(),
                TextSerializerConfig::default().build().into(),
            ),
        ),
        compression,
    };

    let mut byte_size = GroupedCountByteSize::new_untagged();
    byte_size.add_event(&log, log.estimated_json_encoded_size_of());

    let (metadata, request_metadata_builder, _events) =
        request_options.split_input((key, vec![log]));

    let payload = EncodeResult::uncompressed(Bytes::new(), byte_size);
    let request_metadata = request_metadata_builder.build(&payload);
    let request = request_options.build_request(metadata, request_metadata, payload);

    assert_ne!(request.metadata.partition_key, "blob.log".to_string());
    assert_eq!(request.content_encoding, None);
    assert_eq!(request.content_type, "text/plain");
}

#[test]
fn azure_blob_build_request_with_crypto_nonce() {
    let log = Event::Log(LogEvent::from("test message"));
    let container_name = String::from("logs");
    let sink_config = AzureBlobSinkConfig {
        blob_prefix: "blob/".try_into().unwrap(),
        container_name: container_name.clone(),
        ..default_config((None::<FramingConfig>, TextSerializerConfig::default()).into())
    };
    let key = sink_config
        .key_partitioner()
        .unwrap()
        .partition(&log)
        .expect("key wasn't provided");

    let request_options = AzureBlobRequestOptions {
        container_name,
        storage_account: Some(String::from("some-account")),
        blob_time_format: String::new(),
        blob_append_uuid: false,
        blob_prepend_crypto_nonce: true,
        encoder: (
            Default::default(),
            Encoder::<Framer>::new(
                NewlineDelimitedEncoder::default().into(),
                TextSerializerConfig::default().build().into(),
            ),
        ),
        compression: Compression::None,
    };

    let mut byte_size = GroupedCountByteSize::new_untagged();
    byte_size.add_event(&log, log.estimated_json_encoded_size_of());

    let (metadata, request_metadata_builder, _events) =
        request_options.split_input((key, vec![log]));
    let payload = EncodeResult::uncompressed(Bytes::new(), byte_size);
    let request_metadata = request_metadata_builder.build(&payload);
    let request = request_options.build_request(metadata, request_metadata, payload);

    // Should be "blob/<8-hex-chars>-.log"
    let filename = request
        .metadata
        .partition_key
        .strip_prefix("blob/")
        .unwrap()
        .strip_suffix(".log")
        .unwrap();
    let (nonce, rest) = filename.split_at(9);
    assert!(nonce[..8].chars().all(|c| c.is_ascii_hexdigit()));
    assert_eq!(&nonce[8..], "-");
    assert!(rest.is_empty());
}

#[test]
fn azure_blob_event_log_metadata_bucket_and_container_split() {
    // The `container` field powers the URL (wasbs://<container>/...) while
    // the new `bucket` field carries the storage account name for parity
    // with log-daemon's LogSyncEvent.destination_bucket.
    let log = Event::Log(LogEvent::from("test message"));
    let container_name = String::from("my-container");
    let storage_account = String::from("mylogstorage");
    let sink_config = AzureBlobSinkConfig {
        blob_prefix: "blob".try_into().unwrap(),
        container_name: container_name.clone(),
        storage_account: Some(storage_account.clone()),
        ..default_config((None::<FramingConfig>, TextSerializerConfig::default()).into())
    };
    let key = sink_config
        .key_partitioner()
        .unwrap()
        .partition(&log)
        .expect("key wasn't provided");

    let request_options = AzureBlobRequestOptions {
        container_name: container_name.clone(),
        storage_account: Some(storage_account.clone()),
        blob_time_format: String::new(),
        blob_append_uuid: false,
        blob_prepend_crypto_nonce: false,
        encoder: (
            Default::default(),
            Encoder::<Framer>::new(
                NewlineDelimitedEncoder::default().into(),
                TextSerializerConfig::default().build().into(),
            ),
        ),
        compression: Compression::None,
    };

    let (metadata, _request_metadata_builder, _events) =
        request_options.split_input((key, vec![log]));

    assert_eq!(metadata.event_log_metadata.container, container_name);
    assert_eq!(metadata.event_log_metadata.bucket, Some(storage_account));
}

#[test]
fn azure_blob_event_log_metadata_bucket_none_when_storage_account_unset() {
    // If jsonnet forgets to populate storage_account, the daemon must not
    // crashloop. The bucket field shows up as None so the downstream VRL
    // emits an empty bucket (loud-fail in vector-event-log).
    let log = Event::Log(LogEvent::from("test message"));
    let container_name = String::from("my-container");
    let sink_config = AzureBlobSinkConfig {
        blob_prefix: "blob".try_into().unwrap(),
        container_name: container_name.clone(),
        ..default_config((None::<FramingConfig>, TextSerializerConfig::default()).into())
    };
    let key = sink_config
        .key_partitioner()
        .unwrap()
        .partition(&log)
        .expect("key wasn't provided");

    let request_options = AzureBlobRequestOptions {
        container_name: container_name.clone(),
        storage_account: None,
        blob_time_format: String::new(),
        blob_append_uuid: false,
        blob_prepend_crypto_nonce: false,
        encoder: (
            Default::default(),
            Encoder::<Framer>::new(
                NewlineDelimitedEncoder::default().into(),
                TextSerializerConfig::default().build().into(),
            ),
        ),
        compression: Compression::None,
    };

    let (metadata, _request_metadata_builder, _events) =
        request_options.split_input((key, vec![log]));

    assert_eq!(metadata.event_log_metadata.container, container_name);
    assert_eq!(metadata.event_log_metadata.bucket, None);
}
