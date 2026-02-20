use metrics::counter;
use vector_lib::internal_event::{ComponentEventsDropped, UNINTENTIONAL, error_stage, error_type};
use vector_lib::internal_event::{InternalEvent, NamedInternalEvent};

#[derive(Debug)]
#[allow(dead_code)]
pub struct TraceToLogConversionError {
    pub error: &'static str,
}

impl NamedInternalEvent for TraceToLogConversionError {
    fn name(&self) -> &'static str {
        "TraceToLogConversionError"
    }
}

impl InternalEvent for TraceToLogConversionError {
    fn emit(self) {
        let reason = "Failed to convert trace to log event.";
        error!(
            message = reason,
            error = ?self.error,
            error_type = error_type::CONVERSION_FAILED,
            stage = error_stage::PROCESSING,
            internal_log_rate_limit = true
        );
        counter!(
            "component_errors_total",
            "error_type" => error_type::CONVERSION_FAILED,
            "stage" => error_stage::PROCESSING,
        )
        .increment(1);

        emit!(ComponentEventsDropped::<UNINTENTIONAL> { count: 1, reason })
    }
}
