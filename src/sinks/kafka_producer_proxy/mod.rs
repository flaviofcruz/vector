use std::convert::TryFrom;

use snafu::Snafu;

mod blacklist;
mod config;
mod service;
mod sink;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
#[repr(i32)]
pub enum ResponseCodes {
    Success = 0,
    KafkaUnexpectedErrorErrorCode = 1,
    KafkaRetriableErrorErrorCode = 2,
    KafkaErrorErrorCode = 3,
    KafkaInvalidArgumentsErrorCode = 4,
    KafkaBlacklistTopicErrorCode = 5,
    KafkaAuthzDenyErrorCode = 6,
    KafkaTopicExceedsRateLimitErrorCode = 7,
    Unknown(i32),
}

impl TryFrom<i32> for ResponseCodes {
    type Error = ();

    fn try_from(v: i32) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(ResponseCodes::Success),
            1 => Ok(ResponseCodes::KafkaUnexpectedErrorErrorCode),
            2 => Ok(ResponseCodes::KafkaRetriableErrorErrorCode),
            3 => Ok(ResponseCodes::KafkaErrorErrorCode),
            4 => Ok(ResponseCodes::KafkaInvalidArgumentsErrorCode),
            5 => Ok(ResponseCodes::KafkaBlacklistTopicErrorCode),
            6 => Ok(ResponseCodes::KafkaAuthzDenyErrorCode),
            7 => Ok(ResponseCodes::KafkaTopicExceedsRateLimitErrorCode),
            other => Ok(ResponseCodes::Unknown(other)),
        }
    }
}

impl ResponseCodes {
    const fn is_retriable_error(self) -> bool {
        matches!(
            self,
            ResponseCodes::KafkaBlacklistTopicErrorCode
                | ResponseCodes::KafkaTopicExceedsRateLimitErrorCode
                | ResponseCodes::KafkaRetriableErrorErrorCode
        )
    }

    const fn is_success(self) -> bool {
        matches!(self, ResponseCodes::Success)
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum KafkaProducerProxySinkError {
    #[snafu(display("Request failed: {}", source))]
    Request { source: tonic::Status },
}
