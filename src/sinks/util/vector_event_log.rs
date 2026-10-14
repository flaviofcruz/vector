use futures::future::BoxFuture;
use std::env;
use std::sync::OnceLock;
use std::task::{Context, Poll};
use tower::Service;

use crate::event::Finalizable;
use vector_lib::request_metadata::MetaDescriptive;
use vector_lib::stream::DriverResponse;

pub static ENABLE_SINK_EVENT_LOGGING: OnceLock<bool> = OnceLock::new();
// Use an env var to determine whether we should be wrapping services with event loggging
pub fn use_event_log_sink_wrapping() -> bool {
    // Initialize the static variable once, or return the value if it's already initialized/computed
    *ENABLE_SINK_EVENT_LOGGING.get_or_init(|| {
        env::var("ENABLE_SINK_EVENT_LOGGING")
            .map(|v| v == "true")
            .unwrap_or(false)
    })
}

#[derive(Clone)]
pub struct EventLoggingService<S> {
    pub inner: S,
    pub enable_logs: bool,
}

impl<S> EventLoggingService<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            enable_logs: use_event_log_sink_wrapping(),
        }
    }
}

// EventLogginService intends to act as a wrapper around the services that most sinks already build
// Effectively, it should be a transparent layer that just adds event logging before / after but doesn't affect other behavior
// This will help centralize the event logging logic so each sink doesn't need a custom implementation
impl<S, Req> Service<Req> for EventLoggingService<S>
where
    S: Service<Req> + Send + 'static,
    S::Future: Send + 'static,
    // This is the base request, which will have a wrapped layer around it to help with delivery event logs
    Req: Finalizable + MetaDescriptive + Send + 'static,
    S::Response: DriverResponse,
    S::Error: std::fmt::Debug,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        if self.enable_logs {
            let event_log_metadata = req.get_metadata().event_log_metadata().clone();
            let delivery_event_log = event_log_metadata.delivery_event;
            // let file_send_event_log = event_log_metadata.file_send_event;

            delivery_event_log.emit_staged_event();
            // Only emit if specified (for many sinks it won't be)
            if let Some(file_send_event_log) = event_log_metadata.file_send_event.as_ref() {
                file_send_event_log.emit_staged_event();
            }
            let response = self.inner.call(req);
            // Give back the response from the inner service, but first log the delivery event
            Box::pin(async move {
                let response = response.await;
                match &response {
                    Ok(response)
                        if response.event_status() == crate::event::EventStatus::Delivered =>
                    {
                        delivery_event_log.emit_delivered_event();
                        if let Some(file_send_event_log) =
                            event_log_metadata.file_send_event.as_ref()
                        {
                            file_send_event_log.emit_uploaded_event();
                        }
                    }
                    Ok(response) => {
                        if let Some(file_send_event_log) =
                            event_log_metadata.file_send_event.as_ref()
                        {
                            file_send_event_log.emit_error_event(&format!(
                                "request completed with {:?}",
                                response.event_status()
                            ));
                        }
                    }
                    Err(error) => {
                        if let Some(file_send_event_log) =
                            event_log_metadata.file_send_event.as_ref()
                        {
                            file_send_event_log.emit_error_event(&format!("{:?}", error));
                        }
                    }
                }
                response
            })
        } else {
            // We want a way to disable event logging for safety purposes
            // Ideally we could just choose to use the normal service vs. the wrapped one
            // But Rust limitations prevent us from having something be either one or the other (needs a concrete type)
            // So instead we just replicate the almost exact behavior with an extra Box pin
            Box::pin(self.inner.call(req))
        }
    }
}

#[cfg(test)]
pub(crate) mod test_util {
    use std::{
        future::Future,
        sync::{Arc, Mutex},
    };

    use tracing::{Event, Subscriber, field::Visit};
    use tracing_futures::WithSubscriber;
    use tracing_subscriber::{Layer, layer::Context, layer::SubscriberExt};
    use vector_common::internal_event::vector_event::file_send_event::{
        ENABLE_FILE_SEND_EVENTS, enable_file_send_events,
    };

    #[derive(Clone, Default)]
    pub(crate) struct VelEventCapture {
        events: Arc<Mutex<Vec<CapturedVelEvent>>>,
    }

    #[derive(Default)]
    struct CapturedVelEvent {
        file_send_event_type: Option<String>,
        vector_event_type: Option<String>,
    }

    impl VelEventCapture {
        pub(crate) async fn capture<F>(&self, future: F) -> F::Output
        where
            F: Future,
        {
            let _ = ENABLE_FILE_SEND_EVENTS.set(true);
            assert!(enable_file_send_events());
            let subscriber = tracing_subscriber::registry().with(self.clone());
            future.with_subscriber(subscriber).await
        }

        pub(crate) fn file_send_count(&self, event_type: &str) -> usize {
            self.events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| event.file_send_event_type.as_deref() == Some(event_type))
                .count()
        }

        pub(crate) fn legacy_upload_count(&self) -> usize {
            self.events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| event.vector_event_type.as_deref() == Some("4"))
                .count()
        }
    }

    impl<S> Layer<S> for VelEventCapture
    where
        S: Subscriber,
    {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            let mut captured = CapturedVelEvent::default();
            event.record(&mut captured);
            if captured.file_send_event_type.is_some() || captured.vector_event_type.is_some() {
                self.events.lock().unwrap().push(captured);
            }
        }
    }

    impl Visit for CapturedVelEvent {
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            match field.name() {
                "file_send_event_type" => self.file_send_event_type = Some(value.to_owned()),
                "vector_event_type" => self.vector_event_type = Some(value.to_owned()),
                _ => {}
            }
        }

        fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
            if field.name() == "vector_event_type" {
                self.vector_event_type = Some(value.to_string());
            }
        }

        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            if field.name() == "vector_event_type" {
                self.vector_event_type = Some(value.to_string());
            }
        }

        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "vector_event_type" {
                self.vector_event_type = Some(format!("{value:?}"));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Event, EventFinalizers, EventStatus, LogEvent};
    use crate::sinks::util::vector_event_log::test_util::VelEventCapture;
    use std::pin::Pin;
    use std::sync::atomic::Ordering;
    use vector_common::internal_event::vector_event::{
        EventWithEventLog, VectorSinkEventMetadata, file_send_event::FileEventMetadata,
    };
    use vector_lib::event::{BatchNotifier, EventFinalizer};
    use vector_lib::request_metadata::GroupedCountByteSize;
    use vector_lib::request_metadata::RequestMetadata;

    #[derive(Debug)]
    struct DoubleError;

    struct DoubleResponse {
        value: u32,
        status: EventStatus,
        events_byte_size: GroupedCountByteSize,
    }

    impl DriverResponse for DoubleResponse {
        fn event_status(&self) -> EventStatus {
            self.status
        }

        fn events_sent(&self) -> &GroupedCountByteSize {
            &self.events_byte_size
        }
    }

    struct DoubleRequest {
        pub value: u32,
        pub finalizers: EventFinalizers,
        pub metadata: RequestMetadata,
    }

    impl DoubleRequest {
        pub fn new(value: u32, event_log: VectorSinkEventMetadata) -> Self {
            Self {
                value,
                finalizers: EventFinalizers::new(EventFinalizer::new(
                    BatchNotifier::new_with_receiver().0,
                )),
                metadata: RequestMetadata::new_with_event_log(
                    0,
                    0,
                    0,
                    0,
                    GroupedCountByteSize::default(),
                    event_log,
                ),
            }
        }
    }

    impl Finalizable for DoubleRequest {
        fn take_finalizers(&mut self) -> EventFinalizers {
            self.finalizers.take_finalizers()
        }
    }

    impl MetaDescriptive for DoubleRequest {
        fn get_metadata(&self) -> &RequestMetadata {
            &self.metadata
        }

        fn metadata_mut(&mut self) -> &mut RequestMetadata {
            &mut self.metadata
        }
    }

    struct DoubleService;
    impl Service<DoubleRequest> for DoubleService {
        type Response = DoubleResponse;
        type Error = DoubleError;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            // Always ready to accept requests
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: DoubleRequest) -> Self::Future {
            // Return a boxed future that doubles the input
            let fut = async move {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                let value = req.value;
                // Only succeed if the value is not 0 (to help us test errors)
                if value != 0 {
                    Ok(DoubleResponse {
                        value: value * 2,
                        status: if value == 1 {
                            EventStatus::Delivered
                        } else if value == 2 {
                            EventStatus::Rejected
                        } else {
                            EventStatus::Errored
                        },
                        events_byte_size: GroupedCountByteSize::default(),
                    })
                } else {
                    Err(DoubleError)
                }
            };
            Box::pin(fut)
        }
    }

    fn event_log_with_file_event() -> VectorSinkEventMetadata {
        Event::Log(LogEvent::from("test event")).compute_event_log_with_file_event(
            FileEventMetadata::new(
                10,
                1,
                "test.log".to_owned(),
                "test-container".to_owned(),
                Some("test-bucket".to_owned()),
            ),
        )
    }

    fn assert_delivered_count(event_log: &VectorSinkEventMetadata, delivered: u32) {
        assert_eq!(
            event_log
                .delivery_event
                .delivered_call_count
                .load(Ordering::SeqCst),
            delivered
        );
    }

    fn assert_file_send_counts(
        capture: &VelEventCapture,
        staged: usize,
        uploaded: usize,
        errors: usize,
    ) {
        assert_eq!(capture.file_send_count("VECTOR_FILE_SEND_START"), staged);
        assert_eq!(
            capture.file_send_count("VECTOR_FILE_SEND_COMPLETE"),
            uploaded
        );
        assert_eq!(capture.file_send_count("VECTOR_FILE_SEND_WARN"), errors);
    }

    #[tokio::test]
    // Check that the event logging layer successfully emits and doesn't interfere with result
    async fn test_event_logging_success() {
        let inner_service = DoubleService;
        let mut event_logging_service = EventLoggingService {
            inner: inner_service,
            enable_logs: true,
        };
        let event_log = event_log_with_file_event();
        let request = DoubleRequest::new(1, event_log.clone());
        let capture = VelEventCapture::default();
        let response = capture
            .capture(async { event_logging_service.call(request).await })
            .await;

        assert_eq!(response.unwrap().value, 2);
        assert_delivered_count(&event_log, 1);
        assert_file_send_counts(&capture, 1, 1, 0);
    }

    #[tokio::test]
    // When failed, we should not emit a delivery event
    async fn test_event_logging_on_failure() {
        let inner_service = DoubleService;
        let mut event_logging_service = EventLoggingService {
            inner: inner_service,
            enable_logs: true,
        };
        let event_log = event_log_with_file_event();
        let request = DoubleRequest::new(0, event_log.clone());
        let capture = VelEventCapture::default();
        let response = capture
            .capture(async { event_logging_service.call(request).await })
            .await;

        assert!(response.is_err());
        assert_delivered_count(&event_log, 0);
        assert_file_send_counts(&capture, 1, 0, 1);
    }

    #[tokio::test]
    async fn test_event_logging_on_logical_failure() {
        for value in [2, 3] {
            let mut event_logging_service = EventLoggingService {
                inner: DoubleService,
                enable_logs: true,
            };
            let event_log = event_log_with_file_event();
            let capture = VelEventCapture::default();
            let response = capture
                .capture(async {
                    event_logging_service
                        .call(DoubleRequest::new(value, event_log.clone()))
                        .await
                })
                .await
                .unwrap();

            assert_ne!(response.event_status(), EventStatus::Delivered);
            assert_delivered_count(&event_log, 0);
            assert_file_send_counts(&capture, 1, 0, 1);
        }
    }

    #[tokio::test]
    async fn test_event_logging_disabled() {
        let mut event_logging_service = EventLoggingService {
            inner: DoubleService,
            enable_logs: false,
        };
        let event_log = event_log_with_file_event();
        let capture = VelEventCapture::default();
        let response = capture
            .capture(async {
                event_logging_service
                    .call(DoubleRequest::new(1, event_log.clone()))
                    .await
            })
            .await
            .unwrap();

        assert_eq!(response.event_status(), EventStatus::Delivered);
        assert_delivered_count(&event_log, 0);
        assert_file_send_counts(&capture, 0, 0, 0);
    }
}
