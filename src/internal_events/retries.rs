use metrics::counter;
use vector_lib::internal_event::{InternalEvent, NamedInternalEvent};

#[derive(Debug)]
pub struct CheckRetryEvent<'a> {
    pub status_code: &'a str,
    pub retry: bool,
}

impl NamedInternalEvent for CheckRetryEvent<'_> {
    fn name(&self) -> &'static str {
        "CheckRetryEvent"
    }
}

impl InternalEvent for CheckRetryEvent<'_> {
    fn emit(self) {
        debug!(
            message = "Considering retry on error.",
            status_code = self.status_code,
            retry = self.retry,
        );
        counter!("sink_retries_total",
            "status_code" => self.status_code.to_string(),
            "retry" => self.retry.to_string(),
        )
        .increment(1);
    }
}
