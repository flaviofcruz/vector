mod config;
mod integration_tests;
mod record;

use aws_sdk_kinesis::{
    Client, operation::put_records::PutRecordsError, types::PutRecordsRequestEntry,
};

#[cfg(all(test, feature = "sinks-aws_s3"))]
pub(crate) use self::config::KinesisClientBuilder;
pub use self::config::KinesisStreamsSinkConfig;
pub use super::{
    config::{KinesisSinkBaseConfig, build_sink},
    record::{Record, SendRecord},
    request_builder,
    service::{KinesisResponse, KinesisService},
    sink,
};

pub type KinesisError = PutRecordsError;
pub type KinesisRecord = PutRecordsRequestEntry;
pub type KinesisClient = Client;
