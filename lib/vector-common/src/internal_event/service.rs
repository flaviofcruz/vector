use std::fmt;

use metrics::counter;

use super::{
    ComponentEventsDropped, ComponentEventsDroppedWithCode, InternalEvent, UNINTENTIONAL, emit,
    error_stage, error_type,
};
use crate::NamedInternalEvent;

#[derive(Debug, NamedInternalEvent)]
pub struct PollReadyError<E> {
    pub error: E,
}

impl<E: std::fmt::Debug> InternalEvent for PollReadyError<E> {
    fn emit(self) {
        error!(
            message = "Service poll ready failed.",
            error = ?self.error,
            error_type = error_type::REQUEST_FAILED,
            stage = error_stage::SENDING,
        );
        counter!(
            "component_errors_total",
            "error_type" => error_type::REQUEST_FAILED,
            "stage" => error_stage::SENDING,
        )
        .increment(1);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct CallError<E> {
    pub error: E,
    pub request_id: usize,
    pub count: usize,
    /// Fixed classification of `error`, labeled as `error_code` on `component_errors_total`
    /// and `component_discarded_events_total`. `None` keeps both series unlabeled.
    pub error_code: Option<&'static str>,
}

impl<E: std::fmt::Debug> InternalEvent for CallError<E> {
    fn emit(self) {
        let reason = "Service call failed. No retries or retries exhausted.";
        error!(
            message = reason,
            error = ?self.error,
            request_id = self.request_id,
            error_code = self.error_code,
            error_type = error_type::REQUEST_FAILED,
            stage = error_stage::SENDING,
        );
        if let Some(error_code) = self.error_code {
            counter!(
                "component_errors_total",
                "error_code" => error_code,
                "error_type" => error_type::REQUEST_FAILED,
                "stage" => error_stage::SENDING,
            )
            .increment(1);
            emit(ComponentEventsDroppedWithCode::<UNINTENTIONAL> {
                reason,
                count: self.count,
                error_code,
            });
        } else {
            counter!(
                "component_errors_total",
                "error_type" => error_type::REQUEST_FAILED,
                "stage" => error_stage::SENDING,
            )
            .increment(1);
            emit(ComponentEventsDropped::<UNINTENTIONAL> {
                reason,
                count: self.count,
            });
        }
    }
}

/// A service error tagged with a fixed `error_code`, which the stream driver reads to label its
/// `CallError` metrics.
///
/// Opt-in: a service wraps its final (post-retry) error in this, boxed as [`crate::Error`].
/// Unwrapped errors keep today's unlabeled series. `code` must come from a fixed set (a
/// `match` on the error), never from error text, so the label stays bounded.
///
/// `Debug` and `Display` delegate to the wrapped error, so logs read the same as unwrapped.
pub struct CodedError {
    pub code: &'static str,
    pub error: crate::Error,
}

impl CodedError {
    /// The `error_code` of `error`, if it is a boxed [`CodedError`].
    pub fn code_of(error: &(dyn std::any::Any + 'static)) -> Option<&'static str> {
        error
            .downcast_ref::<crate::Error>()?
            .downcast_ref::<Self>()
            .map(|coded| coded.code)
    }
}

impl fmt::Debug for CodedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.error, f)
    }
}

impl fmt::Display for CodedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.error, f)
    }
}

impl std::error::Error for CodedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.error.source()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_of_reads_a_boxed_coded_error() {
        let error: crate::Error = Box::new(CodedError {
            code: "some_code",
            error: "boom".into(),
        });
        assert_eq!(CodedError::code_of(&error), Some("some_code"));
        // Logs keep the wrapped error's text.
        assert_eq!(error.to_string(), "boom");
    }

    #[test]
    fn code_of_ignores_other_errors() {
        let error: crate::Error = "boom".into();
        assert_eq!(CodedError::code_of(&error), None);
        // A non-boxed error type has no code either.
        assert_eq!(CodedError::code_of(&()), None);
    }
}
