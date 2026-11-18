//! DNS membership and error-aware P2C for the opt-in gRPC sink path.
//!
//! Health changes selection/readiness, never discovery membership. The decaying
//! health and cold-start signals follow RAF's error-aware endpoint model without
//! introducing a dependency on RAF.

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures::{FutureExt, future::BoxFuture};
use http::Uri;
use tower::{
    Service, ServiceExt, balance::p2c::Balance, buffer::Buffer, limit::RateLimit, load::Load,
    retry::Retry, timeout::Timeout,
};
use vector_lib::configurable::configurable_component;

use super::{
    adaptive_concurrency::{AdaptiveConcurrencyLimit, AdaptiveConcurrencySettings},
    retries::{FibonacciRetryPolicy, RetryLogic},
    service::TowerRequestSettings,
};

mod diagnostics;
mod discovery;
mod health;
#[cfg(test)]
pub(crate) mod tests;
mod transport;

use discovery::DnsDiscover;
pub(crate) use discovery::DnsTarget;
use health::{EndpointToken, Group, HealthService};
pub(crate) use transport::{classify_grpc_error, new_client};

/// Opt-in DNS-driven client-side balancing for gRPC sinks.
#[configurable_component]
#[derive(Clone, Debug, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct LoadBalancingConfig {
    /// Enables DNS discovery, per-pod concurrency, and error-aware P2C.
    pub enabled: bool,
    /// Fixed concurrency cap for each pod; unset inherits request concurrency.
    pub per_pod_concurrency: Option<usize>,
    /// Adaptive concurrency for each pod; unset inherits request adaptive settings.
    /// Setting this selects adaptive per-pod concurrency independently of request concurrency.
    #[configurable(derived)]
    pub per_pod_adaptive_concurrency: Option<AdaptiveConcurrencySettings>,
    /// DNS hostname to resolve; defaults to the host in `address`.
    pub dns_name: Option<String>,
    /// Seconds between DNS refreshes (also the DNS lookup timeout).
    pub refresh_interval_secs: u64,
    /// Decay time constant of endpoint error history, in seconds.
    pub error_ewma_secs: u64,
    /// Maximum multiplier applied to an endpoint's normalized concurrency load.
    pub max_penalty: f64,
    /// Decay time constant for newly discovered pods, in seconds; zero disables it.
    pub slow_start_secs: u64,
    /// Initial load multiplier for a new pod.
    pub slow_start_penalty: f64,
    /// Enables hard ejection and probes; disabled pods only receive a health penalty.
    pub ejection_enabled: bool,
    /// Error rate at which sustained failures may trigger hard ejection.
    pub ejection_threshold: f64,
    /// Seconds of near-total failure required before hard ejection.
    pub ejection_duration_secs: u64,
    /// Maximum percentage of DNS members that may be ejected (rounded down).
    pub max_ejection_percent: u8,
    /// Idle delay before a degraded-pod trickle or an initial ejection probe.
    pub probe_backoff_secs: u64,
    /// Maximum delay between probes after repeated probe failures.
    pub max_probe_backoff_secs: u64,
    /// Routes to the original address when no DNS members are available.
    pub fallback_to_address: bool,
}

impl Default for LoadBalancingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            per_pod_concurrency: None,
            per_pod_adaptive_concurrency: None,
            dns_name: None,
            refresh_interval_secs: 5,
            error_ewma_secs: 30,
            max_penalty: 50.0,
            slow_start_secs: 60,
            slow_start_penalty: 10.0,
            ejection_enabled: false,
            ejection_threshold: 0.98,
            ejection_duration_secs: 30,
            max_ejection_percent: 30,
            probe_backoff_secs: 5,
            max_probe_backoff_secs: 60,
            fallback_to_address: true,
        }
    }
}

impl LoadBalancingConfig {
    pub fn validate(&self) -> crate::Result<()> {
        if self
            .per_pod_concurrency
            .is_some_and(|limit| limit == 0 || limit > tokio::sync::Semaphore::MAX_PERMITS)
        {
            return Err("load_balancing.per_pod_concurrency must be positive and fit within Tokio semaphore capacity".into());
        }
        if self.per_pod_concurrency.is_some() && self.per_pod_adaptive_concurrency.is_some() {
            return Err("load_balancing.per_pod_concurrency and per_pod_adaptive_concurrency are mutually exclusive".into());
        }
        if let Some(options) = self.per_pod_adaptive_concurrency
            && (options.initial_concurrency == 0
                || options.initial_concurrency > options.max_concurrency_limit()
                || options.max_concurrency_limit() > tokio::sync::Semaphore::MAX_PERMITS
                || !options.decrease_ratio.is_finite()
                || !(0.0 < options.decrease_ratio && options.decrease_ratio < 1.0)
                || !options.ewma_alpha.is_finite()
                || !(0.0 < options.ewma_alpha && options.ewma_alpha < 1.0)
                || !options.rtt_deviation_scale.is_finite()
                || options.rtt_deviation_scale < 0.0)
        {
            return Err("Invalid load_balancing.per_pod_adaptive_concurrency settings".into());
        }
        if self.refresh_interval_secs == 0
            || self.error_ewma_secs == 0
            || self.ejection_duration_secs == 0
            || self.probe_backoff_secs == 0
            || self.max_probe_backoff_secs < self.probe_backoff_secs
            || self.max_ejection_percent > 100
            || !self.max_penalty.is_finite()
            || self.max_penalty < 1.0
            || !self.slow_start_penalty.is_finite()
            || self.slow_start_penalty < 1.0
            || !self.ejection_threshold.is_finite()
            || !(0.0 < self.ejection_threshold && self.ejection_threshold <= 1.0)
        {
            return Err(
                "Invalid load_balancing limits, decay intervals, or ejection settings".into(),
            );
        }
        Ok(())
    }

    const fn per_pod_settings(&self, mut settings: TowerRequestSettings) -> TowerRequestSettings {
        if let Some(limit) = self.per_pod_concurrency {
            settings.concurrency = Some(limit);
        } else if self.per_pod_adaptive_concurrency.is_some() {
            settings.concurrency = None;
        }
        if let Some(options) = self.per_pod_adaptive_concurrency {
            settings.adaptive_concurrency = options;
        }
        settings
    }

    pub fn discovery_uri(&self, address: &Uri) -> crate::Result<Uri> {
        self.validate()?;
        let host = self
            .dns_name
            .as_deref()
            .or_else(|| address.host())
            .ok_or("load_balancing requires an address host")?;
        if host.is_empty() {
            return Err("load_balancing dns_name must not be empty".into());
        }
        let port = address
            .port_u16()
            .ok_or("load_balancing requires an address port")?;
        Ok(Uri::builder()
            .scheme(address.scheme_str().unwrap_or("http"))
            .authority(format!("{host}:{port}"))
            .path_and_query("/")
            .build()?)
    }
}

pub(crate) type BalancedService<Req, Resp> = Buffer<Req, BoxFuture<'static, crate::Result<Resp>>>;

/// Holds the sink-wide permit across the entire retry future, including backoff.
/// KPP passes its shared topic blacklist around the balancer as `service`.
pub(crate) fn request_service<S, RL: RetryLogic>(
    settings: TowerRequestSettings,
    logic: RL,
    service: S,
) -> RateLimit<AdaptiveConcurrencyLimit<Retry<FibonacciRetryPolicy<RL>, S>, RL>> {
    let retry = Retry::new(settings.retry_policy(logic.clone()), service);
    tower::ServiceBuilder::new()
        .rate_limit(settings.rate_limit_num, settings.rate_limit_duration)
        .service(AdaptiveConcurrencyLimit::new(
            retry,
            logic,
            settings.concurrency,
            settings.adaptive_concurrency,
        ))
}

/// Builds Buffer -> (optional fallback ->) Balance -> per-pod ARC -> health -> timeout.
/// Each sink installs `request_service` outside this stack.
pub(crate) fn balanced_service<Req, RL, S, F, C>(
    settings: TowerRequestSettings,
    logic: RL,
    config: LoadBalancingConfig,
    target: DnsTarget,
    fallback: S,
    make_service: F,
    classify: C,
) -> BalancedService<Req, S::Response>
where
    Req: Clone + Send + 'static,
    RL: RetryLogic<Response = S::Response>,
    S: Service<Req, Error = crate::Error> + Clone + Send + Unpin + 'static,
    S::Response: Send + 'static,
    S::Future: Send + 'static,
    F: Fn(SocketAddr) -> S + Send + Unpin + 'static,
    C: Fn(&crate::Error) -> Option<bool> + Clone + Send + Sync + Unpin + 'static,
{
    let bound = buffer_bound(&settings);
    let settings = config.per_pod_settings(settings);
    let idle_load = 1.0
        / settings
            .concurrency
            .unwrap_or(settings.adaptive_concurrency.max_concurrency_limit())
            .max(1) as f64;
    let mut state = Group::new(config.clone(), target.uri.to_string());
    state.initial_limit = settings
        .concurrency
        .unwrap_or(settings.adaptive_concurrency.initial_concurrency);
    state.idle_load = idle_load;
    let group = Arc::new(Mutex::new(state));
    let fallback = AdaptiveConcurrencyLimit::new(
        Timeout::new(fallback, settings.timeout),
        logic.clone(),
        settings.concurrency,
        settings.adaptive_concurrency,
    );
    let discover = DnsDiscover::new(
        target,
        config,
        Arc::clone(&group),
        move |addr, generation, state| {
            let token = EndpointToken {
                group: Arc::clone(&group),
                addr,
                generation,
            };
            let health = HealthService::new(
                Timeout::new(make_service(addr), settings.timeout),
                token.clone(),
                classify.clone(),
            );
            let inner = AdaptiveConcurrencyLimit::new(
                health,
                logic.clone(),
                settings.concurrency,
                settings.adaptive_concurrency,
            );
            state.concurrency = Some(inner.diagnostics());
            Member {
                inner,
                token,
                idle_load,
            }
        },
    );
    let state = Arc::clone(&discover.group);
    let balance = Balance::new(discover);
    let routing = Fallback {
        balance,
        fallback,
        group: state,
        selected_fallback: false,
    };
    Buffer::new(routing.boxed(), bound)
}

fn buffer_bound(settings: &TowerRequestSettings) -> usize {
    settings
        .concurrency
        .unwrap_or(settings.adaptive_concurrency.max_concurrency_limit())
        .clamp(32, tokio::sync::Semaphore::MAX_PERMITS)
}

struct Member<S, L> {
    inner: AdaptiveConcurrencyLimit<S, L>,
    token: EndpointToken,
    idle_load: f64,
}

impl<S, L> Load for Member<S, L> {
    type Metric = f64;
    fn load(&self) -> f64 {
        // A common floor keeps health visible at zero in-flight requests without
        // starving idle pods whose adaptive limits have shrunk or are still cold.
        let limit = self.inner.diagnostics().snapshot().0;
        let (in_flight, penalty) = self.token.load_state();
        (in_flight as f64 / limit.max(1) as f64 + self.idle_load) * penalty
    }
}

impl<Req, S, L> Service<Req> for Member<S, L>
where
    S: Service<Req, Error = crate::Error>,
    L: RetryLogic<Response = S::Response>,
{
    type Response = S::Response;
    type Error = crate::Error;
    type Future = <AdaptiveConcurrencyLimit<S, L> as Service<Req>>::Future;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<crate::Result<()>> {
        if !self.token.poll_available(cx) {
            self.inner.release_ready();
            return Poll::Pending;
        }
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, req: Req) -> Self::Future {
        if tracing::enabled!(tracing::Level::TRACE) {
            let group = self.token.group.lock().expect("balancer state poisoned");
            let snapshot = group
                .members
                .get(&self.token.addr)
                .filter(|state| state.generation == self.token.generation)
                .map(|state| {
                    group.pod_snapshot(self.token.addr, state, tokio::time::Instant::now())
                });
            let target = group.target.clone();
            drop(group);
            if let Some(snapshot) = snapshot {
                let pod = snapshot.finish();
                trace!(target = %target, pod = ?pod, "Dynamic balancer request selected.");
            }
        }
        self.inner.call(req)
    }
}

struct Fallback<B, S, L> {
    balance: B,
    fallback: AdaptiveConcurrencyLimit<S, L>,
    group: Arc<Mutex<Group>>,
    selected_fallback: bool,
}

impl<Req, B, S, L> Service<Req> for Fallback<B, S, L>
where
    Req: Send + 'static,
    B: Service<Req, Response = S::Response, Error = crate::Error>,
    S: Service<Req, Error = crate::Error>,
    B::Future: Send + 'static,
    S::Future: Send + 'static,
    L: RetryLogic<Response = S::Response>,
{
    type Response = S::Response;
    type Error = crate::Error;
    type Future = BoxFuture<'static, crate::Result<S::Response>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<crate::Result<()>> {
        // Always poll discovery, including while fallback is serving requests.
        let ready = self.balance.poll_ready(cx);
        let group = self.group.lock().expect("balancer state poisoned");
        let selected = group.use_fallback();
        let transition = (selected != self.selected_fallback
            && tracing::enabled!(tracing::Level::DEBUG))
        .then(|| {
            (
                group.target.clone(),
                group.members.len(),
                group.ejected_count(),
                group.ready_probe_count(),
            )
        });
        self.selected_fallback = selected;
        drop(group);
        if let Some((target, members, ejected, ready_probes)) = transition {
            let reason = if selected {
                if members == 0 {
                    "no_members"
                } else {
                    "all_members_ejected"
                }
            } else if ready_probes > 0 {
                "probe_ready"
            } else {
                "members_available"
            };
            debug!(target = %target, fallback = selected, reason, members, ejected, ready_probes,
                "Dynamic balancer fallback changed.");
        }
        if self.selected_fallback {
            self.fallback.poll_ready(cx)
        } else {
            self.fallback.release_ready();
            ready
        }
    }
    fn call(&mut self, req: Req) -> Self::Future {
        if self.selected_fallback {
            let group = self.group.lock().expect("balancer state poisoned");
            let _entered = group.span.clone().entered();
            metrics::counter!("sink_lb_fallback_total", "target" => group.target.clone())
                .increment(1);
            drop(group);
            self.fallback.call(req).boxed()
        } else {
            self.balance.call(req).boxed()
        }
    }
}
