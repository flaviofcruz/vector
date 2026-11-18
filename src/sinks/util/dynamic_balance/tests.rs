use super::*;

#[test]
fn config_defaults_and_buffer_bound_is_independent_of_retries() {
    let config: LoadBalancingConfig = toml::from_str("").unwrap();
    assert_eq!(config, LoadBalancingConfig::default());
    assert!(!config.enabled);
    config.validate().unwrap();
    let request: super::super::TowerRequestConfig = Default::default();
    let mut settings = request.into_settings();
    settings.concurrency = Some(4);
    settings.retry_attempts = 9;
    assert_eq!(buffer_bound(&settings), 32);
    settings.retry_attempts = usize::MAX;
    assert_eq!(buffer_bound(&settings), 32);
    settings.concurrency = Some(64);
    assert_eq!(buffer_bound(&settings), 64);
    settings.concurrency = Some(usize::MAX);
    assert_eq!(buffer_bound(&settings), tokio::sync::Semaphore::MAX_PERMITS);
}

#[tokio::test(start_paused = true)]
async fn default_retries_backpressure_slow_backends_with_bounded_in_flight() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    for concurrency in [None, Some(2)] {
        let request: super::super::TowerRequestConfig = Default::default();
        let mut settings = request.into_settings();
        assert_eq!(settings.retry_attempts, isize::MAX as usize);
        settings.concurrency = concurrency;
        let bound = buffer_bound(&settings);
        let limit = 2 * concurrency.unwrap_or(1);
        let calls = Arc::new(AtomicUsize::new(0));
        // Only the external backend is replaced; its response never completes.
        let backend = {
            let calls = Arc::clone(&calls);
            tower::service_fn(move |_: u8| {
                calls.fetch_add(1, Ordering::Relaxed);
                futures::future::pending::<crate::Result<u8>>()
            })
        };
        let target = DnsTarget::with_resolver("http://slow:80".parse().unwrap(), |_| {
            async {
                Ok(vec![
                    "127.0.0.1:80".parse().unwrap(),
                    "127.0.0.2:80".parse().unwrap(),
                ])
            }
            .boxed()
        });
        let mut service = balanced_service(
            settings,
            Retry,
            LoadBalancingConfig {
                fallback_to_address: false,
                ..Default::default()
            },
            target,
            backend.clone(),
            move |_| backend.clone(),
            |_| Some(true),
        );
        let mut requests = Vec::new();
        for _ in 0..bound + limit + 2 {
            tokio::task::yield_now().await;
            match service.poll_ready(&mut Context::from_waker(futures::task::noop_waker_ref())) {
                Poll::Ready(Ok(())) => requests.push(service.call(0)),
                Poll::Pending => break,
                Poll::Ready(Err(error)) => panic!("unexpected error: {error}"),
            }
        }
        tokio::task::yield_now().await;
        assert!(
            service
                .poll_ready(&mut Context::from_waker(futures::task::noop_waker_ref()))
                .is_pending()
        );
        assert_eq!(calls.load(Ordering::Relaxed), limit);
        // The worker may dequeue one additional request while waiting for ARC.
        assert!(requests.len() <= bound + limit + 1);
    }
}

#[derive(Clone)]
struct Retry;
impl RetryLogic for Retry {
    type Error = std::io::Error;
    type Request = u8;
    type Response = u8;
    fn is_retriable_error(&self, _: &std::io::Error) -> bool {
        true
    }
}

#[tokio::test(start_paused = true)]
async fn fallback_serves_empty_discovery_and_stops_after_dns_members_appear() {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    use tokio::sync::watch;
    // External transport returns its backend ID; the routing stack is real.
    fn backend(
        id: u8,
        calls: Arc<AtomicUsize>,
    ) -> tower::util::BoxCloneService<u8, u8, crate::Error> {
        tower::util::BoxCloneService::new(tower::service_fn(move |_| {
            calls.fetch_add(1, Ordering::Relaxed);
            async move { Ok(id) }
        }))
    }
    let fallback_calls = Arc::new(AtomicUsize::new(0));
    let pod_calls = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = watch::channel(Vec::<SocketAddr>::new());
    let target = DnsTarget::with_resolver("http://test:80".parse().unwrap(), move |_| {
        let addresses = rx.borrow().clone();
        async move { Ok(addresses) }.boxed()
    });
    let config = LoadBalancingConfig {
        enabled: true,
        refresh_interval_secs: 1,
        slow_start_secs: 0,
        ..Default::default()
    };
    let request: super::super::TowerRequestConfig = Default::default();
    let calls = Arc::clone(&pod_calls);
    let mut service = balanced_service(
        request.into_settings(),
        Retry,
        config,
        target,
        backend(1, Arc::clone(&fallback_calls)),
        move |_| backend(2, Arc::clone(&calls)),
        |_| Some(true),
    );
    assert_eq!(service.ready().await.unwrap().call(0).await.unwrap(), 1);
    tx.send(vec!["127.0.0.1:80".parse().unwrap()]).unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(service.ready().await.unwrap().call(0).await.unwrap(), 2);
    assert_eq!(fallback_calls.load(Ordering::Relaxed), 1);
    assert_eq!(pod_calls.load(Ordering::Relaxed), 1);
}

#[test]
fn invalid_settings_are_rejected_and_dns_name_preserves_address_scheme_and_port() {
    for config in [
        LoadBalancingConfig {
            refresh_interval_secs: 0,
            ..Default::default()
        },
        LoadBalancingConfig {
            max_penalty: f64::NAN,
            ..Default::default()
        },
        LoadBalancingConfig {
            max_ejection_percent: 101,
            ..Default::default()
        },
        LoadBalancingConfig {
            ejection_threshold: 0.0,
            ..Default::default()
        },
    ] {
        assert!(config.validate().is_err());
    }
    let config = LoadBalancingConfig {
        dns_name: Some("pods.example".into()),
        ..Default::default()
    };
    assert_eq!(
        config
            .discovery_uri(&"https://static.example:1234".parse().unwrap())
            .unwrap(),
        "https://pods.example:1234/".parse::<Uri>().unwrap()
    );
}

#[tokio::test]
async fn idle_members_have_equal_load_despite_different_concurrency_limits() {
    use super::health::EndpointHealth;
    let config = LoadBalancingConfig {
        slow_start_secs: 0,
        ..Default::default()
    };
    let addresses: Vec<SocketAddr> = ["127.0.0.1:80", "127.0.0.2:80"]
        .into_iter()
        .map(|s| s.parse().unwrap())
        .collect();
    let mut group = Group::new(config.clone(), "test-idle".into());
    for &addr in &addresses {
        group.members.insert(
            addr,
            EndpointHealth::new(1, tokio::time::Instant::now(), &config),
        );
    }
    let group = Arc::new(Mutex::new(group));
    let request: super::super::TowerRequestConfig = Default::default();
    let settings = request.into_settings();
    let idle_load = 1.0 / settings.adaptive_concurrency.max_concurrency_limit() as f64;
    // Fixed limits reproduce two learned ARC limits while both pods are idle.
    let member = |addr, limit| Member {
        inner: AdaptiveConcurrencyLimit::new(
            tower::service_fn(|_: u8| async { Ok::<_, crate::Error>(0u8) }),
            Retry,
            Some(limit),
            settings.adaptive_concurrency,
        ),
        token: EndpointToken {
            group: Arc::clone(&group),
            addr,
            generation: 1,
        },
        idle_load,
    };
    let recovering = member(addresses[0], 1);
    let healthy = member(addresses[1], 128);
    assert_eq!(recovering.load(), healthy.load());
    assert_eq!(recovering.load(), idle_load);
}

#[tokio::test]
async fn routing_to_pods_releases_reserved_fallback_concurrency() {
    let request: super::super::TowerRequestConfig = Default::default();
    let settings = request.into_settings();
    let config = LoadBalancingConfig::default();
    let group = Arc::new(Mutex::new(Group::new(
        config.clone(),
        "test-reservations".into(),
    )));
    let backend = tower::service_fn(|_: u8| async { Ok::<_, crate::Error>(0u8) });
    let fallback =
        AdaptiveConcurrencyLimit::new(backend, Retry, Some(1), settings.adaptive_concurrency);
    let mut observer = fallback.clone();
    let mut routing = Fallback {
        balance: backend,
        fallback,
        group: Arc::clone(&group),
        selected_fallback: false,
    };
    let mut cx = Context::from_waker(futures::task::noop_waker_ref());
    assert!(matches!(routing.poll_ready(&mut cx), Poll::Ready(Ok(()))));
    assert!(routing.selected_fallback);
    assert!(observer.poll_ready(&mut cx).is_pending());
    group.lock().unwrap().members.insert(
        "127.0.0.1:80".parse().unwrap(),
        health::EndpointHealth::new(1, tokio::time::Instant::now(), &config),
    );
    assert!(matches!(routing.poll_ready(&mut cx), Poll::Ready(Ok(()))));
    assert!(!routing.selected_fallback);
    assert!(matches!(observer.poll_ready(&mut cx), Poll::Ready(Ok(()))));
}

#[tokio::test]
#[serial_test::serial]
async fn pod_snapshot_reads_live_arc_counters_without_holding_group_lock() {
    let (_guard, logs) = super::diagnostics::tests::capture(tracing::Level::TRACE);
    let config = LoadBalancingConfig {
        slow_start_secs: 0,
        ..Default::default()
    };
    let addr = "127.0.0.1:80".parse().unwrap();
    let mut state = Group::new(config.clone(), "test-snapshot".into());
    state.members.insert(
        addr,
        health::EndpointHealth::new(7, tokio::time::Instant::now(), &config),
    );
    state.idle_load = 0.125;
    let group = Arc::new(Mutex::new(state));
    let request: super::super::TowerRequestConfig = Default::default();
    let token = EndpointToken {
        group: Arc::clone(&group),
        addr,
        generation: 7,
    };
    let mut member = Member {
        inner: AdaptiveConcurrencyLimit::new(
            HealthService::new(
                tower::service_fn(|_: u8| async { Ok::<_, crate::Error>(0u8) }),
                token.clone(),
                |_: &crate::Error| Some(true),
            ),
            Retry,
            Some(4),
            request.into_settings().adaptive_concurrency,
        ),
        token,
        idle_load: 0.125,
    };
    group
        .lock()
        .unwrap()
        .members
        .get_mut(&addr)
        .unwrap()
        .concurrency = Some(member.inner.diagnostics());
    let future = member.ready().await.unwrap().call(0);
    let pending = {
        let state = group.lock().unwrap();
        state.pod_snapshot(addr, &state.members[&addr], tokio::time::Instant::now())
    };
    assert!(group.try_lock().is_ok());
    let pod = pending.finish();
    assert_eq!(pod.generation, 7);
    assert_eq!(pod.current_limit, 4);
    assert_eq!(pod.in_flight, 1);
    assert_eq!(pod.normalized_load, 0.25);
    assert_eq!(pod.effective_load, member.load());
    assert_eq!(future.await.unwrap(), 0);
    let pending = {
        let state = group.lock().unwrap();
        state.pod_snapshot(addr, &state.members[&addr], tokio::time::Instant::now())
    };
    let pod = pending.finish();
    assert_eq!(pod.in_flight, 0);
    assert_eq!(pod.effective_load, member.load());
    let selections = logs.events("Dynamic balancer request selected.");
    assert_eq!(
        selections.len(),
        1,
        "load comparisons must not emit request logs"
    );
    assert_eq!(
        selections[0]["target"],
        "vector::sinks::util::dynamic_balance"
    );
    assert!(
        selections[0]["fields"]["pod"]
            .as_str()
            .unwrap()
            .contains("127.0.0.1:80")
    );
}

#[tokio::test]
#[serial_test::serial]
async fn fallback_logs_only_when_routing_changes() {
    let (_guard, logs) = super::diagnostics::tests::capture(tracing::Level::DEBUG);
    let config = LoadBalancingConfig::default();
    let group = Arc::new(Mutex::new(Group::new(
        config.clone(),
        "test-fallback".into(),
    )));
    let backend = tower::service_fn(|_: u8| async { Ok::<_, crate::Error>(0u8) });
    let request: super::super::TowerRequestConfig = Default::default();
    let mut routing = Fallback {
        balance: backend,
        fallback: AdaptiveConcurrencyLimit::new(
            backend,
            Retry,
            Some(1),
            request.into_settings().adaptive_concurrency,
        ),
        group: Arc::clone(&group),
        selected_fallback: false,
    };
    let mut cx = Context::from_waker(futures::task::noop_waker_ref());
    for _ in 0..2 {
        assert!(matches!(routing.poll_ready(&mut cx), Poll::Ready(Ok(()))));
    }
    group.lock().unwrap().members.insert(
        "127.0.0.1:80".parse().unwrap(),
        health::EndpointHealth::new(1, tokio::time::Instant::now(), &config),
    );
    for _ in 0..2 {
        assert!(matches!(routing.poll_ready(&mut cx), Poll::Ready(Ok(()))));
    }
    let events = logs.events("Dynamic balancer fallback changed.");
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["fields"]["reason"], "no_members");
    assert_eq!(events[0]["fields"]["fallback"], true);
    assert_eq!(events[0]["fields"]["members"], 0);
    assert_eq!(events[1]["fields"]["reason"], "members_available");
    assert_eq!(events[1]["fields"]["fallback"], false);
    assert_eq!(events[1]["fields"]["members"], 1);
    for event in events {
        assert_eq!(event["fields"]["ejected"], 0);
        assert_eq!(event["fields"]["ready_probes"], 0);
    }
}

// Count service admissions, rather than Driver input prefetch, to measure the
// batches retained by Retry. Only DNS and the external transport are replaced.
#[derive(Default)]
struct Admissions {
    total: std::sync::atomic::AtomicUsize,
    held: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
    completed: std::sync::atomic::AtomicUsize,
}

struct Count<S> {
    inner: S,
    counts: Arc<Admissions>,
}

struct Held(Arc<Admissions>);
impl Drop for Held {
    fn drop(&mut self) {
        self.0
            .held
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

impl<Req, S> Service<Req> for Count<S>
where
    S: Service<Req, Error = crate::Error>,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
{
    type Response = S::Response;
    type Error = crate::Error;
    type Future = BoxFuture<'static, crate::Result<Self::Response>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<crate::Result<()>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        use std::sync::atomic::Ordering::Relaxed;
        self.counts.total.fetch_add(1, Relaxed);
        let held = self.counts.held.fetch_add(1, Relaxed) + 1;
        self.counts.peak.fetch_max(held, Relaxed);
        let guard = Held(Arc::clone(&self.counts));
        let future = self.inner.call(req);
        async move {
            let result = future.await;
            guard.0.completed.fetch_add(1, Relaxed);
            drop(guard);
            result
        }
        .boxed()
    }
}

async fn settle() {
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
}

pub(crate) async fn retry_lifecycle_with_driver<Req, Resp, RL, S, Wrap, Reply, Error, Classify>(
    logic: RL,
    wrap: Wrap,
    reply: Reply,
    error: Error,
    classify: Classify,
) where
    Req: Default
        + Clone
        + Send
        + Unpin
        + crate::event::Finalizable
        + vector_lib::request_metadata::MetaDescriptive
        + 'static,
    Resp: vector_lib::stream::DriverResponse + Send + 'static,
    RL: RetryLogic<Request = Req, Response = Resp>,
    S: Service<Req, Response = Resp, Error = crate::Error> + Send + 'static,
    S::Future: Send + 'static,
    Wrap: Fn(TowerRequestSettings, RL, BalancedService<Req, Resp>) -> S,
    Reply: Fn() -> Resp + Clone + Send + Unpin + 'static,
    Error: Fn() -> crate::Error + Clone + Send + Unpin + 'static,
    Classify: Fn(&crate::Error) -> Option<bool> + Clone + Send + Sync + Unpin + 'static,
{
    use std::{
        sync::atomic::{AtomicBool, Ordering::Relaxed},
        time::Duration,
    };
    use tokio_stream::wrappers::UnboundedReceiverStream;

    for adaptive in [false, true] {
        for (fallback, cancel) in [(false, false), (true, false), (false, true), (true, true)] {
            let request: super::super::TowerRequestConfig = Default::default();
            let mut settings = request.into_settings();
            settings.concurrency = if adaptive { None } else { Some(5) };
            settings.adaptive_concurrency.initial_concurrency = 5;
            settings.adaptive_concurrency.max_concurrency_limit = 5;
            settings.retry_initial_backoff = Duration::from_secs(3600);
            settings.retry_max_duration = Duration::from_secs(3600);
            settings.retry_jitter_mode = super::super::retries::JitterMode::None;
            let addresses: Vec<SocketAddr> = (1..=3)
                .map(|i| format!("127.0.0.{i}:80").parse().unwrap())
                .collect();
            let replacement = "127.0.0.4:80".parse().unwrap();
            let (tx, rx) =
                tokio::sync::watch::channel(if fallback { vec![] } else { addresses.clone() });
            let target =
                DnsTarget::with_resolver("http://retry-test:80".parse().unwrap(), move |_| {
                    let answer = rx.borrow().clone();
                    async move { Ok(answer) }.boxed()
                });
            let failing = Arc::new(AtomicBool::new(true));
            let attempts = Arc::new(Mutex::new(Vec::new()));
            let backend = |addr| {
                let failing = Arc::clone(&failing);
                let attempts = Arc::clone(&attempts);
                let reply = reply.clone();
                let error = error.clone();
                tower::util::BoxCloneService::new(tower::service_fn(move |_: Req| {
                    attempts.lock().unwrap().push(addr);
                    let result = if failing.load(Relaxed) {
                        Err(error())
                    } else {
                        Ok(reply())
                    };
                    async move { result }
                }))
            };
            let fallback_service = backend("127.0.0.9:80".parse().unwrap());
            // The factory owns the captured transport state, not the stack.
            let failing_factory = Arc::clone(&failing);
            let attempts_factory = Arc::clone(&attempts);
            let reply_factory = reply.clone();
            let error_factory = error.clone();
            let balanced = balanced_service(
                settings.clone(),
                logic.clone(),
                LoadBalancingConfig {
                    refresh_interval_secs: 1,
                    slow_start_secs: 0,
                    fallback_to_address: fallback,
                    ..Default::default()
                },
                target,
                fallback_service,
                move |addr| {
                    let failing = Arc::clone(&failing_factory);
                    let attempts = Arc::clone(&attempts_factory);
                    let reply = reply_factory.clone();
                    let error = error_factory.clone();
                    tower::util::BoxCloneService::new(tower::service_fn(move |_: Req| {
                        attempts.lock().unwrap().push(addr);
                        let result = if failing.load(Relaxed) {
                            Err(error())
                        } else {
                            Ok(reply())
                        };
                        async move { result }
                    }))
                },
                classify.clone(),
            );
            let service = wrap(settings, logic.clone(), balanced);
            let counts = Arc::new(Admissions::default());
            let service = Count {
                inner: service,
                counts: Arc::clone(&counts),
            };
            let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
            let driver = tokio::spawn(
                vector_lib::stream::Driver::new(UnboundedReceiverStream::new(receiver), service)
                    .run(),
            );
            for _ in 0..10 {
                for _ in 0..100 {
                    sender.send(Req::default()).unwrap();
                }
                settle().await;
                tokio::time::advance(Duration::from_secs(1)).await;
                settle().await;
                assert_eq!(
                    counts.total.load(Relaxed),
                    5,
                    "adaptive={adaptive}, fallback={fallback}"
                );
                assert_eq!(counts.held.load(Relaxed), 5);
                assert_eq!(counts.completed.load(Relaxed), 0);
                assert_eq!(attempts.lock().unwrap().len(), 5);
            }
            if cancel {
                driver.abort();
                assert!(driver.await.unwrap_err().is_cancelled());
                settle().await;
                assert_eq!(counts.held.load(Relaxed), 0);
                assert_eq!(counts.peak.load(Relaxed), 5);
                continue;
            }
            // Remove all old pods and introduce a new generation while requests
            // hold outer permits in backoff. Retries must use the new member.
            tx.send(vec![replacement]).unwrap();
            tokio::time::advance(Duration::from_secs(1)).await;
            settle().await;
            failing.store(false, Relaxed);
            drop(sender);
            tokio::time::advance(Duration::from_secs(3600)).await;
            for _ in 0..2000 {
                if driver.is_finished() {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert!(
                driver.is_finished(),
                "requests must recover after membership changes"
            );
            driver.await.unwrap().unwrap();
            assert_eq!(counts.total.load(Relaxed), 1000);
            assert_eq!(counts.completed.load(Relaxed), 1000);
            assert_eq!(counts.held.load(Relaxed), 0);
            assert_eq!(counts.peak.load(Relaxed), 5);
            assert!(
                attempts.lock().unwrap()[5..]
                    .iter()
                    .all(|addr| *addr == replacement)
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn per_pod_overrides_cap_each_pod_and_fallback_while_unset_inherits() {
    use super::super::adaptive_concurrency::AdaptiveConcurrencySettings;
    use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
    for (concurrency, config, expected) in [
        (Some(8), LoadBalancingConfig::default(), 8),
        (None, LoadBalancingConfig::default(), 3),
        (
            Some(8),
            LoadBalancingConfig {
                per_pod_concurrency: Some(2),
                ..Default::default()
            },
            2,
        ),
        (
            Some(8),
            LoadBalancingConfig {
                per_pod_adaptive_concurrency: Some(AdaptiveConcurrencySettings {
                    initial_concurrency: 2,
                    max_concurrency_limit: 4,
                    ..Default::default()
                }),
                ..Default::default()
            },
            2,
        ),
    ] {
        for fallback in [false, true] {
            let calls = Arc::new(AtomicUsize::new(0));
            let transport_calls = Arc::clone(&calls);
            let backend = tower::service_fn(move |_: u8| {
                transport_calls.fetch_add(1, Relaxed);
                futures::future::pending::<crate::Result<u8>>()
            });
            let request: super::super::TowerRequestConfig = Default::default();
            let mut settings = request.into_settings();
            settings.concurrency = concurrency;
            settings.adaptive_concurrency.initial_concurrency = 3;
            let target = DnsTarget::with_resolver("http://caps:80".parse().unwrap(), move |_| {
                async move {
                    Ok(if fallback {
                        vec![]
                    } else {
                        vec![
                            "127.0.0.1:80".parse().unwrap(),
                            "127.0.0.2:80".parse().unwrap(),
                        ]
                    })
                }
                .boxed()
            });
            let mut service = balanced_service(
                settings,
                Retry,
                config.clone(),
                target,
                backend.clone(),
                move |_| backend.clone(),
                |_| Some(true),
            );
            let limit = if fallback { expected } else { 2 * expected };
            let mut pending = Vec::new();
            for _ in 0..32 + limit + 2 {
                settle().await;
                if service
                    .poll_ready(&mut Context::from_waker(futures::task::noop_waker_ref()))
                    .is_pending()
                {
                    break;
                }
                pending.push(service.call(0));
            }
            settle().await;
            assert_eq!(
                calls.load(Relaxed),
                limit,
                "config={config:?}, fallback={fallback}"
            );
        }
    }
}

#[test]
fn per_pod_override_validation_and_serialization() {
    let request: super::super::TowerRequestConfig = Default::default();
    let mut settings = request.into_settings();
    settings.concurrency = Some(7);
    settings.adaptive_concurrency.initial_concurrency = 3;
    let inherited = LoadBalancingConfig::default().per_pod_settings(settings.clone());
    assert_eq!(inherited.concurrency, settings.concurrency);
    assert_eq!(
        inherited.adaptive_concurrency,
        settings.adaptive_concurrency
    );
    for input in [
        "per_pod_concurrency = 0",
        "per_pod_concurrency = 2\n[per_pod_adaptive_concurrency]",
        "[per_pod_adaptive_concurrency]\ninitial_concurrency = 0",
        "[per_pod_adaptive_concurrency]\ninitial_concurrency = 3\nmax_concurrency_limit = 2",
        "[per_pod_adaptive_concurrency]\ndecrease_ratio = 1.0",
        "[per_pod_adaptive_concurrency]\newma_alpha = nan",
        "[per_pod_adaptive_concurrency]\nrtt_deviation_scale = -1.0",
        "[per_pod_adaptive_concurrency]\nmax_concurrency_limit = 0",
    ] {
        let config: LoadBalancingConfig = toml::from_str(input).unwrap();
        assert!(config.validate().is_err(), "{input}");
    }
    for input in [
        "per_pod_concurrency = 2",
        "[per_pod_adaptive_concurrency]\ninitial_concurrency = 2\nmax_concurrency_limit = 4",
    ] {
        let config: LoadBalancingConfig = toml::from_str(input).unwrap();
        config.validate().unwrap();
        let roundtrip: LoadBalancingConfig =
            toml::from_str(&toml::to_string(&config).unwrap()).unwrap();
        assert_eq!(roundtrip, config);
    }
}

#[tokio::test(start_paused = true)]
async fn one_failing_pod_shifts_traffic_without_collapsing_sink_wide_arc() {
    failing_pod_with_driver_backoff(std::time::Duration::from_secs(3600)).await;
}

#[tokio::test(start_paused = true)]
async fn short_backoff_retries_succeed_on_other_pods_without_collapsing_sink_wide_arc() {
    failing_pod_with_driver_backoff(std::time::Duration::from_millis(10)).await;
}

async fn failing_pod_with_driver_backoff(backoff: std::time::Duration) {
    use std::{
        sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed},
        time::Duration,
    };
    use vector_lib::{
        event::{EventFinalizers, Finalizable},
        request_metadata::{MetaDescriptive, RequestMetadata},
        stream::{Driver, DriverResponse},
    };
    #[derive(Clone, Default)]
    struct Batch(RequestMetadata);
    impl Finalizable for Batch {
        fn take_finalizers(&mut self) -> EventFinalizers {
            EventFinalizers::default()
        }
    }
    impl MetaDescriptive for Batch {
        fn get_metadata(&self) -> &RequestMetadata {
            &self.0
        }
        fn metadata_mut(&mut self) -> &mut RequestMetadata {
            &mut self.0
        }
    }
    struct Response(vector_lib::request_metadata::GroupedCountByteSize);
    impl DriverResponse for Response {
        fn event_status(&self) -> crate::event::EventStatus {
            crate::event::EventStatus::Delivered
        }
        fn events_sent(&self) -> &vector_lib::request_metadata::GroupedCountByteSize {
            &self.0
        }
    }
    #[derive(Clone)]
    struct Logic;
    impl RetryLogic for Logic {
        type Error = std::io::Error;
        type Request = Batch;
        type Response = Response;
        fn is_retriable_error(&self, _: &Self::Error) -> bool {
            true
        }
    }
    let request: super::super::TowerRequestConfig = Default::default();
    let mut settings = request.into_settings();
    settings.adaptive_concurrency.initial_concurrency = 16;
    settings.adaptive_concurrency.max_concurrency_limit = 16;
    settings.retry_initial_backoff = backoff;
    settings.retry_max_duration = Duration::from_secs(3600);
    settings.retry_jitter_mode = super::super::retries::JitterMode::None;
    let successes = Arc::new(AtomicUsize::new(0));
    let success_factory = Arc::clone(&successes);
    let bad = Arc::new(AtomicUsize::new(0));
    let failing = Arc::new(AtomicBool::new(true));
    let bad_factory = Arc::clone(&bad);
    let failing_factory = Arc::clone(&failing);
    let backend = move |id| {
        let successes = Arc::clone(&success_factory);
        let bad = Arc::clone(&bad_factory);
        let failing = Arc::clone(&failing_factory);
        tower::service_fn(move |_: Batch| {
            let failure = id == 1 && failing.load(Relaxed);
            if failure {
                bad.fetch_add(1, Relaxed);
            }
            let successes = Arc::clone(&successes);
            async move {
                if failure {
                    Err::<Response, crate::Error>(
                        std::io::Error::from(std::io::ErrorKind::ConnectionRefused).into(),
                    )
                } else {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    successes.fetch_add(1, Relaxed);
                    Ok(Response(Default::default()))
                }
            }
        })
    };
    let target = DnsTarget::with_resolver("http://many:80".parse().unwrap(), |_| {
        async {
            Ok((1..=8)
                .map(|i| format!("127.0.0.{i}:80").parse().unwrap())
                .collect())
        }
        .boxed()
    });
    let fallback = backend(0);
    let balanced = balanced_service(
        settings.clone(),
        Logic,
        LoadBalancingConfig {
            slow_start_secs: 0,
            fallback_to_address: false,
            ..Default::default()
        },
        target,
        fallback,
        move |addr| {
            backend(match addr.ip() {
                std::net::IpAddr::V4(ip) => ip.octets()[3],
                _ => unreachable!(),
            })
        },
        |_| Some(true),
    );
    let service = request_service(settings, Logic, balanced);
    let outer = service.get_ref().diagnostics();
    let counts = Arc::new(Admissions::default());
    let driver = tokio::spawn(
        Driver::new(
            futures::stream::iter((0..1000).map(|_| Batch::default())),
            Count {
                inner: service,
                counts: Arc::clone(&counts),
            },
        )
        .run(),
    );
    for _ in 0..1000 {
        tokio::time::advance(Duration::from_millis(1)).await;
        settle().await;
        assert!(counts.held.load(Relaxed) <= 16);
        assert!(
            outer.snapshot().0 >= 8,
            "outer limit collapsed: {:?}",
            outer.snapshot()
        );
    }
    assert!(bad.load(Relaxed) > 0);
    if backoff == Duration::from_secs(3600) {
        assert!(bad.load(Relaxed) < 16);
    } else {
        assert!(
            driver.is_finished(),
            "short retries must heal on other pods"
        );
        assert_eq!(successes.load(Relaxed), 1000);
        assert_eq!(counts.completed.load(Relaxed), 1000);
    }
    assert!(
        counts.completed.load(Relaxed) >= 980,
        "healthy pods must keep making progress"
    );
    assert!(
        outer.snapshot().0 >= 8,
        "healthy capacity must not collapse from per-attempt failures: {:?}",
        outer.snapshot()
    );
    failing.store(false, Relaxed);
    tokio::time::advance(Duration::from_secs(3600)).await;
    for _ in 0..100 {
        tokio::time::advance(Duration::from_millis(1)).await;
        settle().await;
        if driver.is_finished() {
            break;
        }
    }
    assert!(driver.is_finished());
    driver.await.unwrap().unwrap();
    assert_eq!(counts.completed.load(Relaxed), 1000);
}

#[tokio::test]
async fn idle_bad_pod_has_higher_load_than_idle_healthy_pod() {
    let config = LoadBalancingConfig {
        slow_start_secs: 0,
        ..Default::default()
    };
    let mut state = Group::new(config.clone(), "test-idle-health".into());
    let addresses: Vec<SocketAddr> = ["127.0.0.1:80", "127.0.0.2:80"]
        .into_iter()
        .map(|s| s.parse().unwrap())
        .collect();
    for &addr in &addresses {
        state.members.insert(
            addr,
            health::EndpointHealth::new(1, tokio::time::Instant::now(), &config),
        );
    }
    let group = Arc::new(Mutex::new(state));
    let request: super::super::TowerRequestConfig = Default::default();
    let settings = request.into_settings();
    let member = |addr| Member {
        inner: AdaptiveConcurrencyLimit::new(
            tower::service_fn(|_: u8| async { Ok::<_, crate::Error>(0u8) }),
            Retry,
            Some(5),
            settings.adaptive_concurrency,
        ),
        token: EndpointToken {
            group: Arc::clone(&group),
            addr,
            generation: 1,
        },
        idle_load: 0.2,
    };
    let bad = member(addresses[0]);
    let healthy = member(addresses[1]);
    let mut health = HealthService::new(
        tower::service_fn(|_: u8| async { Err::<u8, crate::Error>("unavailable".into()) }),
        bad.token.clone(),
        |_: &crate::Error| Some(true),
    );
    assert!(health.ready().await.unwrap().call(0).await.is_err());
    assert!(bad.load() > healthy.load());
    assert_eq!(healthy.load(), 0.2);
}

#[tokio::test(start_paused = true)]
async fn cancelling_retry_backoff_releases_shared_outer_permits() {
    use std::time::Duration;
    let request: super::super::TowerRequestConfig = Default::default();
    let mut settings = request.into_settings();
    settings.concurrency = Some(2);
    settings.retry_initial_backoff = Duration::from_secs(3600);
    settings.retry_max_duration = Duration::from_secs(3600);
    settings.retry_jitter_mode = super::super::retries::JitterMode::None;
    let backend = tower::service_fn(|_: u8| async {
        Err::<u8, crate::Error>(std::io::Error::from(std::io::ErrorKind::ConnectionRefused).into())
    });
    let target = DnsTarget::with_resolver("http://cancel:80".parse().unwrap(), |_| {
        async { Ok(vec!["127.0.0.1:80".parse().unwrap()]) }.boxed()
    });
    let balanced = balanced_service(
        settings.clone(),
        Retry,
        LoadBalancingConfig::default(),
        target,
        backend,
        move |_| backend,
        |_| Some(true),
    );
    let mut service = request_service(settings, Retry, balanced);
    let mut observer = service.get_ref().clone();
    let mut pending = Vec::new();
    for _ in 0..2 {
        let mut future = Box::pin(service.ready().await.unwrap().call(0));
        assert!(futures::poll!(future.as_mut()).is_pending());
        settle().await;
        assert!(futures::poll!(future.as_mut()).is_pending());
        pending.push(future);
    }
    let mut cx = Context::from_waker(futures::task::noop_waker_ref());
    assert!(observer.poll_ready(&mut cx).is_pending());
    pending.clear();
    assert!(matches!(observer.poll_ready(&mut cx), Poll::Ready(Ok(()))));
    assert!(futures::poll!(Box::pin(observer.call(0)).as_mut()).is_pending());
}
