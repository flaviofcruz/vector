use futures::future::BoxFuture;
use std::env;
use std::sync::OnceLock;
use std::task::{Context, Poll};
use tower::Service;

use crate::event::Finalizable;
use vector_lib::request_metadata::MetaDescriptive;

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
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        if self.enable_logs {
            let delivery_event_log = req.get_metadata().event_log_metadata().clone();
            delivery_event_log.emit_staged_event();
            let response = self.inner.call(req);
            // Give back the response from the inner service, but first log the delivery event
            Box::pin(async move {
                let response = response.await;
                // Only log if the request succeeded (gives back OK)
                if response.is_ok() {
                    delivery_event_log.emit_delivered_event();
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
mod tests {
    use super::*;
    use crate::event::EventFinalizers;
    use std::pin::Pin;
    use std::sync::atomic::Ordering;
    use vector_common::internal_event::delivery_event::VectorSinkDeliveryEvent;
    use vector_lib::event::{BatchNotifier, EventFinalizer};
    use vector_lib::request_metadata::GroupedCountByteSize;
    use vector_lib::request_metadata::RequestMetadata;

    #[derive(Debug)]
    struct DoubleError;

    struct DoubleRequest {
        pub value: u32,
        pub finalizers: EventFinalizers,
        pub metadata: RequestMetadata,
    }

    impl DoubleRequest {
        pub fn new(value: u32, event_log: VectorSinkDeliveryEvent) -> Self {
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
        type Response = u32;
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
                    Ok(value * 2)
                } else {
                    Err(DoubleError)
                }
            };
            Box::pin(fut)
        }
    }

    #[tokio::test]
    // Check that the event logging layer successfully emits and doesn't interfere with result
    async fn test_event_logging_success() {
        let inner_service = DoubleService;
        let mut event_logging_service = EventLoggingService {
            inner: inner_service,
            enable_logs: true,
        };
        let event_log = VectorSinkDeliveryEvent::new();
        let request = DoubleRequest::new(1, event_log.clone());
        let response = event_logging_service.call(request).await;

        assert_eq!(response.unwrap(), 2);
        assert_eq!(event_log.delivered_call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    // When failed, we should not emit a delivery event
    async fn test_event_logging_on_failure() {
        let inner_service = DoubleService;
        let mut event_logging_service = EventLoggingService {
            inner: inner_service,
            enable_logs: true,
        };
        let event_log = VectorSinkDeliveryEvent::new();
        let request = DoubleRequest::new(0, event_log.clone());
        let response = event_logging_service.call(request).await;

        assert!(response.is_err());
        assert_eq!(event_log.delivered_call_count.load(Ordering::SeqCst), 0);
    }
}
