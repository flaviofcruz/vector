use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use arc_swap::ArcSwap;
use futures::future::BoxFuture;
use futures_util::Future;
use tokio::time::{self, Instant, Sleep};
use tower::{Layer, Service};

use super::service::{KPPRequest, KPPResponse};
use crate::{Error, sinks::kafka_producer_proxy::ResponseCodes};

#[derive(Clone, Debug, PartialEq, Eq)]
enum State {
    Allowed,
    // The Instant refers to the moment in time where the blacklist was enabled
    Blocked(Instant),
}

#[derive(Clone)]
pub struct TopicBlacklistLayer {
    // Wrapped in Arc so ArcSwap can be cloned and shared safely across concurrent tasks
    blacklist_state: Arc<ArcSwap<State>>,
    // blacklist_duration refers to the amount of time that needs to elapse before events can be sent via the service
    blacklist_duration: Duration,
}

impl TopicBlacklistLayer {
    pub fn new(blacklist_duration: Duration) -> Self {
        TopicBlacklistLayer {
            blacklist_state: Arc::new(ArcSwap::new(Arc::new(State::Allowed))),
            blacklist_duration,
        }
    }

    #[cfg(test)]
    const fn with_state(state: Arc<ArcSwap<State>>, blacklist_duration: Duration) -> Self {
        TopicBlacklistLayer {
            blacklist_state: state,
            blacklist_duration,
        }
    }
}

impl<S> Layer<S> for TopicBlacklistLayer
where
    S: Service<KPPRequest, Response = KPPResponse, Error = Error> + Send + 'static,
    S::Future: Send + 'static,
{
    type Service = TopicBlacklistService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TopicBlacklistService {
            inner,
            blacklist_state: Arc::clone(&self.blacklist_state),
            blacklist_duration: self.blacklist_duration,
            sleep: Arc::new(Mutex::new(None)),
        }
    }
}

#[derive(Clone)]
pub struct TopicBlacklistService<S>
where
    S: Service<KPPRequest, Response = KPPResponse, Error = Error> + Send + 'static,
    S::Future: Send + 'static,
{
    inner: S,
    blacklist_state: Arc<ArcSwap<State>>,
    blacklist_duration: Duration,
    // sleep represents a shared sleep timer that tracks when the blacklist period expires.
    // It ensures that the service remains blacklisted until the blacklist_duration has elapsed
    sleep: Arc<Mutex<Option<Pin<Box<Sleep>>>>>,
}

impl<S> Service<KPPRequest> for TopicBlacklistService<S>
where
    S: Service<KPPRequest, Response = KPPResponse, Error = Error> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = KPPResponse;
    type Error = Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let duration = self.blacklist_duration;
        let current_state = self.blacklist_state.load();

        if let State::Blocked(since) = &**current_state {
            if since.elapsed() > duration {
                let mut lock = self.sleep.lock().unwrap();
                *lock = None;
                // compare_and_swap will check if the current_state has changed since we last read it. If it has, it will not do the swap
                // If it hasn't it will swap the current_state with the Allowed State, thereby retaining atomicity while switching States
                // Note that compare_and_swap returns the previous value.
                let previous_state = self
                    .blacklist_state
                    .compare_and_swap(&current_state, Arc::new(State::Allowed));
                if !Arc::ptr_eq(&previous_state, &current_state) {
                    // This means the swap was unsuccessful because it was updated since we last read it
                    // The state has not been changed to Allowed.
                    // Since we do not know what state it is in, we return pending and retry immediately
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                return self.inner.poll_ready(cx);
            }

            // If the blacklist duration has not elapsed yet, we need to wait until it expires.
            // We create a sleep timer that will wake up when the blacklist period ends.
            let mut sleep_lock = self.sleep.lock().unwrap();
            let wakeup_time = *since + duration;
            if sleep_lock.is_none() {
                *sleep_lock = Some(Box::pin(time::sleep_until(wakeup_time)));
            }

            let sleep = sleep_lock.as_mut().unwrap();

            // Polls the sleep timer to check if the blacklist duration has elapsed.
            // If the timer is ready, we'll wake up and retry the state check.
            // If it's still pending, we'll wait for the timer to complete.
            match sleep.as_mut().poll(cx) {
                Poll::Ready(_) => {
                    // Since it is ready, we release the lock and return pending in the case
                    // that the state was changed since we last read it.
                    // We then retry immediately since it is likely that the blacklist duration has expired
                    *sleep_lock = None;
                    // We use cx.waker().wake_by_ref() so that we retry immediately
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                // This will be retried again when it is woken up
                Poll::Pending => return Poll::Pending,
            }
        }
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, kpp_request: KPPRequest) -> Self::Future {
        let inner = self.inner.call(kpp_request);
        let blacklist_state = Arc::clone(&self.blacklist_state);
        // let duration = self.blacklist_duration;

        Box::pin(async move {
            // Call lower layer service for KPP Response
            let kpp_response = inner.await?;

            // Check for blacklist responses
            let received_blacklist = kpp_response
                .results
                .iter()
                .any(|status| *status == ResponseCodes::KafkaBlacklistTopicErrorCode);

            if received_blacklist {
                blacklist_state.store(Arc::new(State::Blocked(Instant::now())));
            }

            Ok(kpp_response)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::EventFinalizers;
    use futures::Future;
    use futures::task::noop_waker;
    use kafka_producer_proxy::proto as proto_kpp;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tokio::sync::{Barrier, Semaphore};
    use tower::ServiceExt;
    use vector_lib::request_metadata::{GroupedCountByteSize, RequestMetadata};

    const DURATION: Duration = Duration::from_secs(2);

    /// This function returns a KPP Response with either a Success Error code or a blacklist error code
    pub fn mock_response(should_blacklist: bool) -> KPPResponse {
        let msg_status: ResponseCodes = if should_blacklist {
            ResponseCodes::KafkaBlacklistTopicErrorCode
        } else {
            ResponseCodes::Success
        };

        KPPResponse {
            event_byte_size: GroupedCountByteSize::default(),
            // All succeeded should be false if blacklist is enabled since the response code is non-zero.
            all_succeeded: !should_blacklist,
            results: vec![msg_status],
        }
    }

    /// A function that returns a KPPRequest formatted to trigger either a success or blacklist response.
    /// Based on the type of request that is made, the Mock Service will return a response of this type.
    /// For instance, if a blacklist request is made, the mock service will return a blacklist response.
    pub fn mock_request(should_blacklist: bool) -> KPPRequest {
        let message_content = if should_blacklist {
            "blacklist"
        } else {
            "success"
        };

        KPPRequest {
            finalizers: EventFinalizers::default(),
            metadata: RequestMetadata::default(),
            request: proto_kpp::KafkaMessages {
                messages: vec![proto_kpp::KafkaMessage {
                    topic_name: Some("test-topic".to_string()),
                    key: Some("test-key".to_string()),
                    data: Some(message_content.as_bytes().to_vec()),
                    log_entry: Some(message_content.as_bytes().to_vec()),
                }],
            },
        }
    }

    /// MockService to return response to blacklist layer
    #[derive(Debug, Clone)]
    pub struct MockKPPService;

    impl Service<KPPRequest> for MockKPPService {
        type Response = KPPResponse;
        type Error = Error;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        /// Call will return a blacklist response if the KPP Request is formatted to have
        /// the data field set to blacklist, otherwise it will return a successful response.
        fn call(&mut self, request: KPPRequest) -> Self::Future {
            let mut response = mock_response(false);

            if let Some(first) = request.request.messages.first() {
                if String::from_utf8(first.data().to_vec()).unwrap() == "blacklist" {
                    response = mock_response(true);
                }
            }

            Box::pin(async move { Ok(response) })
        }
    }

    fn blacklist_layer(state: Arc<ArcSwap<State>>) -> TopicBlacklistService<MockKPPService> {
        let blacklist_layer: TopicBlacklistLayer = TopicBlacklistLayer::with_state(state, DURATION);
        blacklist_layer.layer(MockKPPService {})
    }

    /// Test to see if the state in the service changes from allowed to blocked
    /// after a blacklist response is sent from service.
    #[tokio::test]
    async fn test_change_state() {
        let state = Arc::new(ArcSwap::new(Arc::new(State::Allowed)));
        let svc = blacklist_layer(Arc::clone(&state));
        let state_clone = Arc::clone(&state);

        let _ = svc.oneshot(mock_request(true)).await;

        assert!(matches!(**state_clone.load(), State::Blocked(_)));
    }

    /// Test to see if the state does not change after receive a successful response.
    #[tokio::test]
    async fn test_same_state_success() {
        let state = Arc::new(ArcSwap::new(Arc::new(State::Allowed)));
        let svc = blacklist_layer(Arc::clone(&state));
        let state_clone = Arc::clone(&state);

        let _ = svc.oneshot(mock_request(false)).await;

        assert!(matches!(**state_clone.load(), State::Allowed,));
    }

    /// Test to validate that after a blacklist request, poll_ready() returns pending
    #[tokio::test]
    async fn test_pending_poll() {
        let state = Arc::new(ArcSwap::new(Arc::new(State::Allowed)));
        let svc = blacklist_layer(Arc::clone(&state));
        let mut clone_svc = svc.clone();

        // This request should initiate a blacklist
        let _ = svc.oneshot(mock_request(true)).await;

        // Asserting that when we poll, we get that the poll is pending
        let waker = noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        let poll = clone_svc.poll_ready(&mut cx);
        assert!(matches!(poll, Poll::Pending));
    }

    /// Test to validate that the sleeping functionality of the poll_ready() is working successfully and that
    /// it returns ok only after the blacklist duration has elapsed
    #[tokio::test(start_paused = true)]
    async fn test_poll_ready_sleep() {
        let state = Arc::new(ArcSwap::new(Arc::new(State::Blocked(Instant::now()))));
        let svc = blacklist_layer(Arc::clone(&state));
        let mut svc_clone = svc.clone();

        let start_time = tokio::time::Instant::now();
        let ready = tokio::spawn(async move {
            let _ = svc_clone.ready().await;
        });

        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;

        // Ensuring that poll_ready() does not return Ok when blacklist duration has not elapsed
        assert!(!ready.is_finished());

        tokio::time::advance(Duration::new(1, 10000)).await;
        tokio::task::yield_now().await;

        // Waiting until it returns Ready
        ready.await.unwrap();

        // If elapsed >= DURATION, that means it returned ready only after blacklist finished
        let elapsed = start_time.elapsed();
        assert!(elapsed >= DURATION);
    }

    /// Test to validate that the state is Blocked after receiving a blacklist response and then
    /// a success response. The reset duration for the state has not elapsed.
    /// This test is concurrent but the blacklist response task runs first to allow for the state to
    /// be changed
    #[tokio::test(start_paused = true)]
    async fn test_simple_concurrent_access() {
        let state = Arc::new(ArcSwap::new(Arc::new(State::Allowed)));
        let svc = blacklist_layer(Arc::clone(&state));
        let state_clone = Arc::clone(&state);
        let mut clone_svc = svc.clone();

        // We introduce barriers to ensure that both calls are sent concurrently
        let barrier = Arc::new(Barrier::new(2));
        let b1 = Arc::clone(&barrier);
        let b2 = Arc::clone(&barrier);

        // Start a blacklist call and then success call
        let blacklist_call = tokio::spawn(async move {
            b1.wait().await;
            let _ = svc.oneshot(mock_request(true)).await;
        });

        let success_call = tokio::spawn(async move {
            b2.wait().await;
            // Yield so that blacklist call can start slightly earlier so state becomes blocked
            tokio::task::yield_now().await;
            // We use call here to ensure that the state remains Blocked
            let _ = clone_svc.call(mock_request(false)).await;
        });

        // Asserting state is blocked after both calls
        _ = blacklist_call.await;
        _ = success_call.await;

        assert!(matches!(**state_clone.load(), State::Blocked(_)));
    }

    /// A test to check that after the state is blocked, future successful requests that occur after
    /// the reset duration are successful.
    #[tokio::test(start_paused = true)]
    async fn test_access_after_timeout() {
        let state = Arc::new(ArcSwap::new(Arc::new(State::Allowed)));
        let svc = blacklist_layer(Arc::clone(&state));
        let state_clone = Arc::clone(&state);
        let clone_svc = svc.clone();

        // Run the call to return a blacklist response
        let _blacklist_call = tokio::spawn(async move {
            let _ = svc.oneshot(mock_request(true)).await;
        })
        .await;

        assert!(matches!(**state_clone.load(), State::Blocked(_),));

        // Advance time past the duration amount and then run call to return successful response
        tokio::time::advance(DURATION + Duration::from_millis(1)).await;
        let _success_call = tokio::spawn(async move {
            tokio::task::yield_now().await;
            let _ = clone_svc.oneshot(mock_request(false)).await;
        })
        .await;

        assert!(matches!(**state_clone.load(), State::Allowed,));
    }

    /// This test starts by sending a blacklist request, then making 10 success requests
    /// which should all encounter that the service is Blocked due to blacklisting.
    /// We then call a request after the blacklist duration has elapsed which should succeed.
    #[tokio::test(start_paused = true)]
    async fn test_many_concurrent() {
        let mut tasks = vec![];
        let state = Arc::new(ArcSwap::new(Arc::new(State::Allowed)));
        let svc = blacklist_layer(Arc::clone(&state));
        let state_clone = Arc::clone(&state);
        const CALLS: usize = 10;

        // Adding a semaphore such that we can allow for future requests to run once our main test runs
        let semaphore = Arc::new(Semaphore::new(0));

        // We make a blacklist request
        let blacklist_svc = svc.clone();
        let sem_clone = Arc::clone(&semaphore);

        let _blacklist_call = tokio::spawn(async move {
            let _ = blacklist_svc.oneshot(mock_request(true)).await;
            // Allowing future calls to run
            sem_clone.add_permits(CALLS);
        })
        .await;

        // Spawning successful calls which should all be blocked
        let sem_clone = Arc::clone(&semaphore);
        for _ in 0..CALLS {
            let _ = sem_clone.acquire().await.unwrap();
            let mut success_req_svc = svc.clone();
            tasks.push(tokio::spawn(async move {
                // We use call here to ensure that the state remains Blocked
                let _ = success_req_svc.call(mock_request(false)).await;
            }));
        }

        // Ensuring that all successfull calls are blocked
        for task in tasks {
            let _ = task.await;
            let resulting_state = state_clone.load().clone();
            assert!(matches!(*resulting_state, State::Blocked(_),));
        }

        // Moving the time forward past the duration and checking to see if the
        // blacklist state has been changed to Allowed
        tokio::time::advance(DURATION + Duration::from_millis(1)).await;
        let success_req_svc = svc.clone();
        let _after_timeout_call = tokio::spawn(async move {
            tokio::task::yield_now().await;
            let _ = success_req_svc.oneshot(mock_request(false)).await;
        })
        .await;

        assert!(matches!(**state_clone.load(), State::Allowed,));
    }
}
