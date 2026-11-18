use std::{
    collections::HashMap,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures::{FutureExt, future::BoxFuture};
use tokio::time::{Instant, Sleep};
use tower::Service;

use super::super::adaptive_concurrency::ConcurrencyDiagnostics;
use super::{
    LoadBalancingConfig,
    diagnostics::{HealthTransition, PendingPodSnapshot, PodSnapshot},
};

// A small healthy prior heals idle pods without masking sustained low-rate failures.
const IDLE_HEALTHY_SAMPLE_WEIGHT: f64 = 0.1;

pub(super) struct EndpointHealth {
    pub generation: u64,
    pub concurrency: Option<ConcurrencyDiagnostics>,
    born: Instant,
    last_selected: Instant,
    trickle_in_flight: bool,
    // Health futures release this count on cancellation as well as completion.
    active_requests: usize,
    updated: Instant,
    errors: f64,
    samples: f64,
    bad_since: Option<Instant>,
    ejected_until: Option<Instant>,
    probe_in_flight: bool,
    ejection_epoch: u64,
    probe_ready: bool,
    degraded: bool,
    backoff: Duration,
    sleep: Option<Pin<Box<Sleep>>>,
    waker: Option<Waker>,
}

impl EndpointHealth {
    pub const fn new(generation: u64, now: Instant, config: &LoadBalancingConfig) -> Self {
        Self {
            generation,
            concurrency: None,
            born: now,
            last_selected: now,
            trickle_in_flight: false,
            active_requests: 0,
            updated: now,
            errors: 0.0,
            samples: 1.0,
            bad_since: None,
            ejected_until: None,
            probe_in_flight: false,
            ejection_epoch: 0,
            probe_ready: false,
            degraded: false,
            backoff: Duration::from_secs(config.probe_backoff_secs),
            sleep: None,
            waker: None,
        }
    }

    fn decay(&self, now: Instant, config: &LoadBalancingConfig) -> f64 {
        (-now.saturating_duration_since(self.updated).as_secs_f64() / config.error_ewma_secs as f64)
            .exp()
    }

    fn decayed_history(&self, now: Instant, config: &LoadBalancingConfig) -> (f64, f64) {
        let decay = self.decay(now, config);
        (
            self.errors * decay,
            self.samples * decay + IDLE_HEALTHY_SAMPLE_WEIGHT * (1.0 - decay),
        )
    }

    pub fn error_rate(&self, now: Instant, config: &LoadBalancingConfig) -> f64 {
        let (errors, samples) = self.decayed_history(now, config);
        errors / samples
    }

    pub fn penalty(&self, now: Instant, config: &LoadBalancingConfig) -> f64 {
        let error = self.health_multiplier(now, config);
        let cold = self.slow_start_multiplier(now, config);
        (error * cold).min(config.max_penalty)
    }

    fn health_multiplier(&self, now: Instant, config: &LoadBalancingConfig) -> f64 {
        1.0 + (config.max_penalty - 1.0) * self.error_rate(now, config).powi(2)
    }

    fn slow_start_multiplier(&self, now: Instant, config: &LoadBalancingConfig) -> f64 {
        if config.slow_start_secs == 0 {
            1.0
        } else {
            1.0 + (config.slow_start_penalty - 1.0)
                * (-now.saturating_duration_since(self.born).as_secs_f64()
                    / config.slow_start_secs as f64)
                    .exp()
        }
    }

    fn trickle_due(&self, now: Instant, config: &LoadBalancingConfig) -> bool {
        self.ejected_until.is_none()
            && !self.trickle_in_flight
            && self.error_rate(now, config) >= 0.5
            && now.saturating_duration_since(self.last_selected)
                >= Duration::from_secs(config.probe_backoff_secs)
    }

    pub(super) const fn state(&self) -> &'static str {
        if self.probe_in_flight {
            "probe_in_flight"
        } else if self.probe_ready {
            "probe_ready"
        } else if self.ejected_until.is_some() {
            "ejected"
        } else if self.degraded {
            "degraded"
        } else {
            "healthy"
        }
    }

    fn transition(
        &self,
        pod: SocketAddr,
        now: Instant,
        config: &LoadBalancingConfig,
        prev_state: &'static str,
        new_state: &'static str,
        reason: &'static str,
    ) -> Option<HealthTransition> {
        let enabled = match reason {
            "request_outcome" | "idle_decay" => tracing::enabled!(tracing::Level::TRACE),
            _ => tracing::enabled!(tracing::Level::DEBUG),
        };
        enabled.then(|| HealthTransition {
            pod,
            generation: self.generation,
            epoch: self.ejection_epoch,
            prev_state,
            new_state,
            reason,
            error_rate: self.error_rate(now, config),
            backoff: self.backoff,
        })
    }

    fn record(&mut self, failed: bool, now: Instant, config: &LoadBalancingConfig) {
        let (errors, samples) = self.decayed_history(now, config);
        // Resumed traffic must not inherit a timer that expired while idle.
        if errors / samples < config.ejection_threshold {
            self.bad_since = None;
        }
        // Cap both weights together, preserving their ratio and leaving room
        // for exactly one outcome. Successes must keep healing busy pods.
        let retained_samples = samples.min(199.0);
        let retained_errors = errors * (retained_samples / samples);
        self.samples = retained_samples + 1.0;
        self.errors = retained_errors + f64::from(failed);
        self.updated = now;
        if self.error_rate(now, config) >= config.ejection_threshold {
            self.bad_since.get_or_insert(now);
        } else {
            self.bad_since = None;
        }
    }

    fn eject(&mut self, now: Instant) {
        self.ejection_epoch += 1;
        self.ejected_until = Some(now + self.backoff);
        self.probe_ready = false;
        self.probe_in_flight = false;
        self.sleep = None;
    }

    fn recover(&mut self, now: Instant, config: &LoadBalancingConfig) {
        self.ejection_epoch += 1;
        self.ejected_until = None;
        self.probe_ready = false;
        self.probe_in_flight = false;
        self.bad_since = None;
        self.born = now;
        self.backoff = Duration::from_secs(config.probe_backoff_secs);
        self.sleep = None;
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    fn metrics(
        &mut self,
        addr: SocketAddr,
        target: &str,
        now: Instant,
        config: &LoadBalancingConfig,
    ) {
        let rate = self.error_rate(now, config);
        self.degraded = rate >= 0.5;
        metrics::gauge!("sink_lb_health_score", "target" => target.to_owned(),
            "pod" => addr.to_string())
        .set(1.0 - rate);
        metrics::gauge!("sink_lb_ejected", "target" => target.to_owned(),
            "pod" => addr.to_string())
        .set(f64::from(self.ejected_until.is_some()));
    }
}

pub(super) struct Group {
    pub config: LoadBalancingConfig,
    pub target: String,
    pub members: HashMap<SocketAddr, EndpointHealth>,
    pub generation: u64,
    pub active: bool,
    pub span: tracing::Span,
    pub initial_limit: usize,
    pub idle_load: f64,
    transitions: Vec<HealthTransition>,
    ejected: usize,
    ready_probes: usize,
    degraded_members: usize,
}

impl Group {
    pub fn new(config: LoadBalancingConfig, target: String) -> Self {
        Self {
            config,
            target,
            members: HashMap::new(),
            generation: 0,
            active: true,
            span: tracing::Span::current(),
            initial_limit: 1,
            idle_load: 1.0,
            transitions: Vec::new(),
            ejected: 0,
            ready_probes: 0,
            degraded_members: 0,
        }
    }

    pub fn pod_snapshot(
        &self,
        addr: SocketAddr,
        state: &EndpointHealth,
        now: Instant,
    ) -> PendingPodSnapshot {
        let capped_penalty = state.penalty(now, &self.config);
        let penalty =
            if state.ejected_until.is_some_and(|until| until <= now) && !state.probe_in_flight {
                0.0
            } else {
                capped_penalty
            };
        PendingPodSnapshot {
            pod: PodSnapshot {
                addr,
                generation: state.generation,
                epoch: state.ejection_epoch,
                error_rate: state.error_rate(now, &self.config),
                health_multiplier: state.health_multiplier(now, &self.config),
                slow_start_multiplier: state.slow_start_multiplier(now, &self.config),
                capped_penalty,
                penalty,
                current_limit: self.initial_limit,
                in_flight: state.active_requests,
                normalized_load: 0.0,
                idle_floor: self.idle_load,
                effective_load: 0.0,
                ejected: state.ejected_until.is_some(),
                probe_ready: state.probe_ready,
                probe_in_flight: state.probe_in_flight,
            },
            concurrency: state.concurrency.clone(),
        }
    }

    pub const fn ejected_count(&self) -> usize {
        self.ejected
    }
    pub const fn ready_probe_count(&self) -> usize {
        self.ready_probes
    }

    pub fn transition(
        &mut self,
        addr: SocketAddr,
        prev: &'static str,
        new: &'static str,
        reason: &'static str,
        now: Instant,
    ) {
        if let Some(event) = self
            .members
            .get(&addr)
            .and_then(|state| state.transition(addr, now, &self.config, prev, new, reason))
        {
            self.transitions.push(event);
        }
    }

    /// Formatting and console output must never hold the balancer lock.
    pub fn emit_transitions(mut group: MutexGuard<'_, Self>) {
        let batch = (!group.transitions.is_empty()).then(|| {
            (
                group.target.clone(),
                group.span.clone(),
                group.ejected,
                group.ejection_cap(),
                std::mem::take(&mut group.transitions),
            )
        });
        drop(group);
        if let Some((target, span, ejected, cap, events)) = batch {
            let _entered = span.enter();
            for event in events {
                event.emit(&target, ejected, cap);
            }
        }
    }

    fn trickle_target(&self, now: Instant) -> Option<SocketAddr> {
        self.members
            .iter()
            .filter(|(_, state)| state.trickle_due(now, &self.config) && state.active_requests == 0)
            .min_by_key(|(addr, state)| (state.last_selected, **addr))
            .map(|(addr, _)| *addr)
    }

    pub fn wake_members(&mut self) {
        for state in self.members.values_mut() {
            if let Some(waker) = state.waker.take() {
                waker.wake();
            }
        }
    }

    pub fn ejection_cap(&self) -> usize {
        if self.config.ejection_enabled {
            self.members.len() * usize::from(self.config.max_ejection_percent) / 100
        } else {
            0
        }
    }

    pub fn enforce_cap(&mut self) {
        // A DNS shrink can reduce the budget while ejections are in progress.
        let cap = self.ejection_cap();
        self.ejected = 0;
        self.ready_probes = 0;
        for (&addr, state) in self
            .members
            .iter_mut()
            .filter(|(_, s)| s.ejected_until.is_some())
        {
            if self.ejected < cap {
                self.ejected += 1;
                self.ready_probes += usize::from(state.probe_ready);
            } else {
                let prev = state.state();
                state.recover(Instant::now(), &self.config);
                if let Some(event) = state.transition(
                    addr,
                    Instant::now(),
                    &self.config,
                    prev,
                    state.state(),
                    "cap_enforcement",
                ) {
                    self.transitions.push(event);
                }
            }
        }
    }

    pub fn use_fallback(&self) -> bool {
        self.config.fallback_to_address
            && self.ejected == self.members.len()
            && self.ready_probes == 0
    }

    pub fn metrics(&mut self, now: Instant) {
        let _entered = self.span.enter();
        if !self.active {
            return;
        }
        metrics::gauge!("sink_lb_members", "target" => self.target.clone())
            .set(self.members.len() as f64);
        self.degraded_members = 0;
        for (&addr, state) in &mut self.members {
            let prev = state.state();
            state.metrics(addr, &self.target, now, &self.config);
            let new = state.state();
            if prev != new
                && let Some(event) =
                    state.transition(addr, now, &self.config, prev, new, "idle_decay")
            {
                self.transitions.push(event);
            }
            self.degraded_members += usize::from(state.degraded);
        }
        metrics::gauge!("sink_lb_degraded_members", "target" => self.target.clone())
            .set(self.degraded_members as f64);
    }

    #[cfg(test)]
    fn complete(&mut self, addr: SocketAddr, generation: u64, failed: bool, probe: Option<u64>) {
        self.complete_with_outcome(addr, generation, Some(failed), probe);
    }

    fn complete_with_outcome(
        &mut self,
        addr: SocketAddr,
        generation: u64,
        outcome: Option<bool>,
        probe: Option<u64>,
    ) {
        let _entered = self.span.clone().entered();
        let cap = self.ejection_cap();
        let Some(state) = self
            .members
            .get_mut(&addr)
            .filter(|s| s.generation == generation)
        else {
            return;
        };
        if probe.is_some_and(|epoch| {
            state.ejected_until.is_none() || !state.probe_in_flight || state.ejection_epoch != epoch
        }) {
            return;
        }
        let was_ejected = state.ejected_until.is_some();
        let was_probe_ready = state.probe_ready;
        let was_degraded = state.degraded;
        let now = Instant::now();
        let prev_state = state.state();
        let mut reason = None;
        if let Some(failed) = outcome {
            state.record(failed, now, &self.config);
        }
        if probe.is_some() {
            if outcome == Some(true) {
                state.backoff = state
                    .backoff
                    .saturating_mul(2)
                    .min(Duration::from_secs(self.config.max_probe_backoff_secs));
                state.eject(now);
                reason = Some("probe_failure");
            } else {
                state.recover(now, &self.config);
                reason = Some("probe_success");
            }
        } else if outcome.is_some()
            && self.config.ejection_enabled
            && state.ejected_until.is_none()
            && self.ejected < cap
            && state.bad_since.is_some_and(|since| {
                now.duration_since(since) >= Duration::from_secs(self.config.ejection_duration_secs)
            })
        {
            state.eject(now);
            reason = Some("sustained_failures");
            metrics::counter!("sink_lb_ejections_total", "target" => self.target.clone(),
                "pod" => addr.to_string())
            .increment(1);
        }
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
        self.ejected =
            self.ejected - usize::from(was_ejected) + usize::from(state.ejected_until.is_some());
        self.ready_probes =
            self.ready_probes - usize::from(was_probe_ready) + usize::from(state.probe_ready);
        state.metrics(addr, &self.target, now, &self.config);
        let new_state = state.state();
        if reason.is_none() && prev_state != new_state {
            reason = Some("request_outcome");
        }
        if let Some(reason) = reason
            && let Some(event) =
                state.transition(addr, now, &self.config, prev_state, new_state, reason)
        {
            self.transitions.push(event);
        }
        self.degraded_members =
            self.degraded_members - usize::from(was_degraded) + usize::from(state.degraded);
        metrics::gauge!("sink_lb_degraded_members", "target" => self.target.clone())
            .set(self.degraded_members as f64);
    }
}

#[derive(Clone)]
pub(super) struct EndpointToken {
    pub group: Arc<Mutex<Group>>,
    pub addr: SocketAddr,
    pub generation: u64,
}

impl EndpointToken {
    #[cfg(test)]
    pub fn penalty(&self) -> f64 {
        self.load_state().1
    }

    pub fn load_state(&self) -> (usize, f64) {
        let group = self.group.lock().expect("balancer state poisoned");
        group
            .members
            .get(&self.addr)
            .filter(|s| s.generation == self.generation)
            .map_or((0, f64::MAX), |s| {
                let now = Instant::now();
                let penalty =
                    if s.ejected_until.is_some_and(|until| until <= now) && !s.probe_in_flight {
                        // A due probe must win selection even when healthy pods are idle.
                        0.0
                    } else {
                        s.penalty(now, &group.config)
                    };
                (s.active_requests, penalty)
            })
    }

    pub fn poll_available(&self, cx: &mut Context<'_>) -> bool {
        let mut group = self.group.lock().expect("balancer state poisoned");
        let trickle = group.trickle_target(Instant::now());
        let Some(state) = group
            .members
            .get_mut(&self.addr)
            .filter(|s| s.generation == self.generation)
        else {
            return false;
        };
        // An overdue soft trickle must not starve a due hard-ejection probe.
        let due_probe = state
            .ejected_until
            .is_some_and(|until| until <= Instant::now())
            && !state.probe_in_flight;
        if state.trickle_in_flight || (trickle.is_some_and(|addr| addr != self.addr) && !due_probe)
        {
            state.waker = Some(cx.waker().clone());
            return false;
        }
        let Some(until) = state.ejected_until else {
            return true;
        };
        state.waker = Some(cx.waker().clone());
        if state.probe_in_flight {
            return false;
        }
        let sleep = state
            .sleep
            .get_or_insert_with(|| Box::pin(tokio::time::sleep_until(until)));
        if sleep.as_mut().poll(cx).is_pending() {
            return false;
        }
        if !state.probe_ready {
            state.probe_ready = true;
            group.ready_probes += 1;
            group.transition(
                self.addr,
                "ejected",
                "probe_ready",
                "backoff_elapsed",
                Instant::now(),
            );
        }
        Group::emit_transitions(group);
        true
    }

    fn start(&self) -> Completion {
        let mut group = self.group.lock().expect("balancer state poisoned");
        let now = Instant::now();
        // Readiness chose the overdue member; reserve its trickle until completion.
        let trickle = group
            .members
            .get(&self.addr)
            .filter(|state| state.generation == self.generation)
            .is_some_and(|state| state.trickle_due(now, &group.config));
        let mut prev_state = "ejected";
        let probe = group
            .members
            .get_mut(&self.addr)
            .filter(|s| s.generation == self.generation)
            .and_then(|state| {
                state.last_selected = now;
                state.trickle_in_flight = trickle;
                state.active_requests += 1;
                if state.ejected_until.is_some() && !state.probe_in_flight {
                    prev_state = state.state();
                    state.probe_in_flight = true;
                    Some(state.ejection_epoch)
                } else {
                    None
                }
            });
        if probe.is_some() {
            let state = group.members.get_mut(&self.addr).expect("present endpoint");
            if state.probe_ready {
                state.probe_ready = false;
                group.ready_probes -= 1;
            }
        }
        if probe.is_some() {
            group.transition(
                self.addr,
                prev_state,
                "probe_in_flight",
                "probe_start",
                Instant::now(),
            );
        }
        if trickle {
            let state = group.members[&self.addr].state();
            group.transition(self.addr, state, state, "trickle_dispatch", now);
            metrics::counter!("sink_lb_trickle_total", "target" => group.target.clone(),
                "pod" => self.addr.to_string())
            .increment(1);
            group.wake_members();
        }
        Group::emit_transitions(group);
        Completion {
            token: self.clone(),
            probe,
            trickle,
            completed: false,
        }
    }
}

struct Completion {
    token: EndpointToken,
    probe: Option<u64>,
    trickle: bool,
    completed: bool,
}
impl Completion {
    fn release_request(&mut self, group: &mut Group) {
        if let Some(state) = group
            .members
            .get_mut(&self.token.addr)
            .filter(|s| s.generation == self.token.generation)
        {
            state.active_requests -= 1;
            if self.trickle {
                state.trickle_in_flight = false;
                if let Some(waker) = state.waker.take() {
                    waker.wake();
                }
            }
        }
        self.trickle = false;
    }

    fn finish(&mut self, failed: bool) {
        self.finish_with_outcome(Some(failed));
    }

    fn finish_with_outcome(&mut self, outcome: Option<bool>) {
        if self.completed {
            return;
        }
        let shared = Arc::clone(&self.token.group);
        let mut group = shared.lock().expect("balancer state poisoned");
        self.release_request(&mut group);
        self.completed = true;
        group.complete_with_outcome(self.token.addr, self.token.generation, outcome, self.probe);
        Group::emit_transitions(group);
    }
}
impl Drop for Completion {
    fn drop(&mut self) {
        if !self.completed {
            let shared = Arc::clone(&self.token.group);
            self.release_request(&mut shared.lock().expect("balancer state poisoned"));
        }
        // Caller cancellation is not an endpoint failure. Release a cancelled
        // probe so the next readiness poll can try it again after backoff.
        if !self.completed
            && let Some(epoch) = self.probe
        {
            let mut group = self.token.group.lock().expect("balancer state poisoned");
            if let Some(state) = group.members.get_mut(&self.token.addr).filter(|s| {
                s.generation == self.token.generation
                    && s.ejected_until.is_some()
                    && s.probe_in_flight
                    && s.ejection_epoch == epoch
            }) {
                let now = Instant::now();
                state.eject(now);
                if let Some(waker) = state.waker.take() {
                    waker.wake();
                }
                group.transition(
                    self.token.addr,
                    "probe_in_flight",
                    "ejected",
                    "probe_cancel",
                    now,
                );
            }
            Group::emit_transitions(group);
        }
    }
}

#[derive(Clone)]
pub(super) struct HealthService<S, C> {
    inner: S,
    token: EndpointToken,
    classify: C,
}
impl<S, C> HealthService<S, C> {
    pub const fn new(inner: S, token: EndpointToken, classify: C) -> Self {
        Self {
            inner,
            token,
            classify,
        }
    }
}
impl<Req, S, C> Service<Req> for HealthService<S, C>
where
    S: Service<Req>,
    S::Error: Into<crate::Error>,
    S::Future: Send + 'static,
    C: Fn(&crate::Error) -> Option<bool> + Clone + Send + 'static,
{
    type Response = S::Response;
    type Error = crate::Error;
    type Future = BoxFuture<'static, crate::Result<S::Response>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<crate::Result<()>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }
    fn call(&mut self, req: Req) -> Self::Future {
        let mut completion = self.token.start();
        let classify = self.classify.clone();
        let future = self.inner.call(req);
        async move {
            let result = future.await.map_err(Into::into);
            match &result {
                Ok(_) => completion.finish(false),
                Err(error) => completion.finish_with_outcome(classify(error)),
            }
            result
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn group(n: usize, percent: u8) -> Group {
        let config = LoadBalancingConfig {
            ejection_enabled: true,
            max_ejection_percent: percent,
            slow_start_secs: 0,
            ejection_threshold: 0.8,
            ejection_duration_secs: 2,
            ..Default::default()
        };
        let mut group = Group::new(config, "test-health".into());
        for i in 1..=n {
            let addr = format!("127.0.0.{i}:80").parse().unwrap();
            group
                .members
                .insert(addr, EndpointHealth::new(1, Instant::now(), &group.config));
        }
        group
    }
    #[tokio::test(start_paused = true)]
    async fn idle_gap_failure_counts_as_one_sample_and_cannot_start_ejection_timer() {
        for idle_secs in [5, 30, 100, 150, 1800] {
            let mut group = group(1, 100);
            group.config.ejection_threshold = 0.98;
            let addr = *group.members.keys().next().unwrap();
            for _ in 0..199 {
                group.complete(addr, 1, false, None);
            }
            assert_eq!(group.members[&addr].samples, 200.0);
            tokio::time::advance(Duration::from_secs(idle_secs)).await;
            let decay = (-(idle_secs as f64) / 30.0).exp();
            let retained = (200.0 * decay + IDLE_HEALTHY_SAMPLE_WEIGHT * (1.0 - decay)).min(199.0);
            group.complete(addr, 1, true, None);
            let state = &group.members[&addr];
            assert_eq!(state.errors, 1.0, "idle={idle_secs}s");
            assert!((state.samples - (retained + 1.0)).abs() < 1e-12);
            let rate = state.error_rate(Instant::now(), &group.config);
            assert!((rate - 1.0 / (retained + 1.0)).abs() < 1e-12);
            assert!(rate < group.config.ejection_threshold);
            assert!(state.bad_since.is_none());
            assert!(state.ejected_until.is_none());
        }
    }

    #[test]
    fn sustained_failures_at_default_decay_reach_ejection_threshold_at_all_request_rates() {
        let config = LoadBalancingConfig {
            slow_start_secs: 0,
            ..Default::default()
        };
        for requests_per_second in [0.2, 1.0, 10.0, 100.0] {
            let interval = Duration::from_secs_f64(1.0 / requests_per_second);
            let mut now = Instant::now();
            let mut state = EndpointHealth::new(1, now, &config);
            for _ in 0..(1800.0 * requests_per_second) as usize {
                now += interval;
                state.record(true, now, &config);
            }
            // Even immediately before the next request, failures remain above
            // threshold so low-rate traffic can earn the sustained timer.
            let pre_rate = state.error_rate(now + interval, &config);
            let post_rate = state.error_rate(now, &config);
            let penalty = state.penalty(now, &config);
            assert!(
                pre_rate >= config.ejection_threshold,
                "rate={requests_per_second}"
            );
            assert!(
                post_rate >= config.ejection_threshold,
                "rate={requests_per_second}"
            );
            assert!(
                penalty > 0.96 * config.max_penalty,
                "rate={requests_per_second}"
            );
            assert!(now.duration_since(state.bad_since.unwrap()) >= Duration::from_secs(30));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn trickles_require_a_current_meaningfully_degraded_error_rate() {
        let config = LoadBalancingConfig {
            slow_start_secs: 0,
            ..Default::default()
        };
        let mut state = EndpointHealth::new(1, Instant::now(), &config);
        for _ in 0..100 {
            state.record(true, Instant::now(), &config);
        }
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(state.trickle_due(Instant::now(), &config));
        tokio::time::advance(Duration::from_secs(300)).await;
        assert!(state.errors > 0.0);
        assert!(!state.trickle_due(Instant::now(), &config));
        for _ in 0..100 {
            state.record(false, Instant::now(), &config);
        }
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(state.errors > 0.0);
        assert!(!state.trickle_due(Instant::now(), &config));
    }

    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn trickle_dispatch_emits_debug_transition_and_ignores_stale_generation() {
        let (_guard, logs) = super::super::diagnostics::tests::capture(tracing::Level::DEBUG);
        let mut group = group(1, 0);
        let addr = *group.members.keys().next().unwrap();
        for _ in 0..100 {
            group.complete(addr, 1, true, None);
        }
        let group = Arc::new(Mutex::new(group));
        let token = EndpointToken {
            group: Arc::clone(&group),
            addr,
            generation: 1,
        };
        tokio::time::advance(Duration::from_secs(5)).await;
        let stale = EndpointToken {
            generation: 0,
            ..token.clone()
        };
        let completion = stale.start();
        assert!(!completion.trickle);
        drop(completion);
        assert!(logs.events("Dynamic balancer health changed.").is_empty());
        token.start().finish(false);
        let events = logs.events("Dynamic balancer health changed.");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["level"], "DEBUG");
        assert_eq!(events[0]["fields"]["reason"], "trickle_dispatch");
        assert_eq!(events[0]["fields"]["pod"], addr.to_string());
    }

    #[tokio::test(start_paused = true)]
    async fn default_ejection_is_disabled_but_sustained_errors_still_degrade() {
        let mut group = group(1, 100);
        group.config.ejection_enabled = LoadBalancingConfig::default().ejection_enabled;
        let addr = *group.members.keys().next().unwrap();
        for _ in 0..60 {
            for _ in 0..99 {
                group.complete(addr, 1, true, None);
            }
            group.complete(addr, 1, false, None);
            tokio::time::advance(Duration::from_secs(1)).await;
        }
        assert_eq!(group.ejection_cap(), 0);
        assert_eq!(group.ejected_count(), 0);
        assert!(group.members[&addr].ejected_until.is_none());
        assert!(!group.members[&addr].probe_ready);
        assert!(!group.members[&addr].probe_in_flight);
        assert!(group.members[&addr].penalty(Instant::now(), &group.config) > 1.0);
        let token = EndpointToken {
            group: Arc::new(Mutex::new(group)),
            addr,
            generation: 1,
        };
        assert!(token.poll_available(&mut Context::from_waker(futures::task::noop_waker_ref())));
        assert!(token.start().probe.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn sustained_threshold_with_occasional_successes_ejects_when_ejection_enabled() {
        let mut group = group(1, 100);
        group.config.ejection_threshold = 0.98;
        group.config.ejection_duration_secs = 30;
        let addr = *group.members.keys().next().unwrap();
        for sample in 1..=3500 {
            tokio::time::advance(Duration::from_millis(10)).await;
            group.complete(addr, 1, sample % 100 != 0, None);
            if group.members[&addr].ejected_until.is_some() {
                break;
            }
        }
        assert!(group.members[&addr].ejected_until.is_some());
        assert_eq!(group.members.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn idle_decayed_rate_clears_stale_ejection_age_before_recording() {
        for idle_secs in [180, 300] {
            let mut group = group(1, 100);
            let addr = *group.members.keys().next().unwrap();
            for _ in 0..100 {
                group.complete(addr, 1, true, None);
            }
            assert!(group.members[&addr].bad_since.is_some());
            tokio::time::advance(Duration::from_secs(idle_secs)).await;
            assert!(
                group.members[&addr].error_rate(Instant::now(), &group.config)
                    < group.config.ejection_threshold
            );
            group.complete(addr, 1, true, None);
            // A new failure may cross the threshold, but cannot inherit the
            // age of the run that healed below threshold during the idle gap.
            assert!(group.members[&addr].ejected_until.is_none());
            assert_eq!(group.members[&addr].bad_since, Some(Instant::now()));
            tokio::time::advance(Duration::from_secs(1)).await;
            group.complete(addr, 1, true, None);
            assert!(group.members[&addr].ejected_until.is_none());
            tokio::time::advance(Duration::from_secs(1)).await;
            group.complete(addr, 1, true, None);
            assert!(group.members[&addr].ejected_until.is_some());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn above_threshold_idle_gap_preserves_age_and_below_threshold_outcome_clears_it() {
        let mut group = group(1, 100);
        let addr = *group.members.keys().next().unwrap();
        for _ in 0..100 {
            group.complete(addr, 1, true, None);
        }
        let since = group.members[&addr].bad_since;
        tokio::time::advance(Duration::from_secs(1)).await;
        group.complete(addr, 1, false, None);
        assert_eq!(group.members[&addr].bad_since, since);
        for _ in 0..100 {
            group.complete(addr, 1, false, None);
        }
        assert!(group.members[&addr].bad_since.is_none());
        tokio::time::advance(Duration::from_secs(2)).await;
        group.complete(addr, 1, true, None);
        assert!(group.members[&addr].ejected_until.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn successful_probe_retains_ewma_and_restarts_slow_start_at_recovery() {
        let mut group = group(1, 100);
        group.config.slow_start_secs = 60;
        let addr = *group.members.keys().next().unwrap();
        tokio::time::advance(Duration::from_secs(600)).await;
        for _ in 0..100 {
            group.complete(addr, 1, true, None);
        }
        tokio::time::advance(Duration::from_secs(2)).await;
        group.complete(addr, 1, true, None);
        assert!(group.members[&addr].ejected_until.is_some());
        let group = Arc::new(Mutex::new(group));
        let token = EndpointToken {
            group: Arc::clone(&group),
            addr,
            generation: 1,
        };
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(token.poll_available(&mut Context::from_waker(futures::task::noop_waker_ref())));
        let (errors, samples, decay) = {
            let group = group.lock().unwrap();
            let state = &group.members[&addr];
            (
                state.errors,
                state.samples,
                state.decay(Instant::now(), &group.config),
            )
        };
        token.start().finish(false);
        {
            let group = group.lock().unwrap();
            let state = &group.members[&addr];
            let decayed_samples = samples * decay + IDLE_HEALTHY_SAMPLE_WEIGHT * (1.0 - decay);
            let retained_samples = decayed_samples.min(199.0);
            let next_samples = retained_samples + 1.0;
            let expected_rate =
                errors * decay * (retained_samples / decayed_samples) / next_samples;
            assert!(
                (state.error_rate(Instant::now(), &group.config) - expected_rate).abs() < 1e-12
            );
            assert_eq!(state.samples, next_samples);
            assert_eq!(state.born, Instant::now());
            assert_eq!(
                state.slow_start_multiplier(Instant::now(), &group.config),
                10.0
            );
            assert!(state.ejected_until.is_none());
            assert!(state.bad_since.is_none());
        }
        let penalty = token.penalty();
        let health = {
            let group = group.lock().unwrap();
            group.members[&addr].health_multiplier(Instant::now(), &group.config)
        };
        tokio::time::advance(Duration::from_secs(60)).await;
        {
            let group = group.lock().unwrap();
            let state = &group.members[&addr];
            assert!(state.health_multiplier(Instant::now(), &group.config) < health);
            assert!(state.slow_start_multiplier(Instant::now(), &group.config) < 10.0);
        }
        // The combined health/cold penalty can stay capped as each component heals.
        assert!(token.penalty() <= penalty);
        tokio::time::advance(Duration::from_secs(240)).await;
        assert!(token.penalty() > 1.0 && token.penalty() < penalty);
        tokio::time::advance(Duration::from_secs(300)).await;
        assert!(token.penalty() < 1.001);
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_is_gradual_and_one_success_after_idle_never_resurrects_errors() {
        let config = LoadBalancingConfig {
            slow_start_secs: 0,
            ..Default::default()
        };
        let mut state = EndpointHealth::new(1, Instant::now(), &config);
        for _ in 0..3000 {
            state.record(true, Instant::now(), &config);
        }
        tokio::time::advance(Duration::from_secs(29)).await;
        let rate = state.error_rate(Instant::now(), &config);
        assert!(rate > 0.98 && rate < 1.0);
        let mut previous = state.penalty(Instant::now(), &config);
        for _ in 0..20 {
            let before = state.error_rate(Instant::now(), &config);
            state.record(false, Instant::now(), &config);
            assert!(state.error_rate(Instant::now(), &config) < before);
            let penalty = state.penalty(Instant::now(), &config);
            assert!(penalty > 1.0 && penalty < previous);
            previous = penalty;
            tokio::time::advance(Duration::from_millis(100)).await;
        }
        assert!(state.error_rate(Instant::now(), &config) < rate);
    }

    #[tokio::test(start_paused = true)]
    async fn ejected_dns_member_is_probed_indefinitely_with_capped_backoff() {
        let mut group = group(1, 100);
        let addr = *group.members.keys().next().unwrap();
        for _ in 0..100 {
            group.complete(addr, 1, true, None);
        }
        tokio::time::advance(Duration::from_secs(2)).await;
        group.complete(addr, 1, true, None);
        let group = Arc::new(Mutex::new(group));
        let token = EndpointToken {
            group: Arc::clone(&group),
            addr,
            generation: 1,
        };
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        for attempt in 0..30 {
            let expected = (5u64 * 2u64.pow(attempt.min(4))).min(60);
            assert_eq!(
                group.lock().unwrap().members[&addr].backoff,
                Duration::from_secs(expected)
            );
            assert!(!token.poll_available(&mut cx));
            tokio::time::advance(Duration::from_secs(expected)).await;
            assert!(token.poll_available(&mut cx));
            let mut probe = token.start();
            assert!(!token.poll_available(&mut cx));
            probe.finish(true);
            assert!(group.lock().unwrap().members[&addr].ejected_until.is_some());
            assert_eq!(group.lock().unwrap().members.len(), 1);
        }
    }

    #[derive(Clone)]
    struct TrickleRetry;
    impl crate::sinks::util::retries::RetryLogic for TrickleRetry {
        type Error = std::io::Error;
        type Request = u8;
        type Response = u8;
        fn is_retriable_error(&self, _: &Self::Error) -> bool {
            true
        }
    }

    #[tokio::test(start_paused = true)]
    async fn degrade_only_trickle_reaches_worst_idle_pod_every_backoff_under_light_load() {
        exercise_degrade_only_trickles(false, 20).await;
    }

    #[tokio::test(start_paused = true)]
    async fn degrade_only_dead_pod_receives_trickles_every_backoff_for_thirty_minutes() {
        exercise_degrade_only_trickles(true, 1800).await;
    }

    async fn exercise_degrade_only_trickles(permanently_failing: bool, seconds: usize) {
        use crate::sinks::util::{
            TowerRequestConfig, adaptive_concurrency::AdaptiveConcurrencyLimit,
        };
        use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
        use tower::{ServiceExt, discover::Change};
        let mut group = group(5, 100);
        group.config.ejection_enabled = false;
        let bad: SocketAddr = "127.0.0.1:80".parse().unwrap();
        for _ in 0..100 {
            group.complete(bad, 1, true, None);
        }
        let group = Arc::new(Mutex::new(group));
        let calls = Arc::new(AtomicUsize::new(0));
        let addresses: Vec<_> = group.lock().unwrap().members.keys().copied().collect();
        let request: TowerRequestConfig = Default::default();
        let mut settings = request.into_settings();
        settings.concurrency = Some(2);
        settings.retry_attempts = 0;
        let members: Vec<_> = addresses
            .into_iter()
            .map(|addr| {
                let calls = Arc::clone(&calls);
                let backend = tower::service_fn(move |_: u8| {
                    if addr == bad {
                        calls.fetch_add(1, Relaxed);
                    }
                    async move {
                        if permanently_failing && addr == bad {
                            Err(std::io::Error::new(
                                std::io::ErrorKind::ConnectionRefused,
                                "dead pod",
                            )
                            .into())
                        } else {
                            Ok::<_, crate::Error>(0u8)
                        }
                    }
                });
                let token = EndpointToken {
                    group: Arc::clone(&group),
                    addr,
                    generation: 1,
                };
                let inner = AdaptiveConcurrencyLimit::new(
                    HealthService::new(backend, token.clone(), |_: &crate::Error| Some(true)),
                    TrickleRetry,
                    Some(2),
                    settings.adaptive_concurrency,
                );
                group
                    .lock()
                    .unwrap()
                    .members
                    .get_mut(&addr)
                    .unwrap()
                    .concurrency = Some(inner.diagnostics());
                Ok::<_, crate::Error>(Change::Insert(
                    addr,
                    super::super::Member {
                        inner,
                        token,
                        idle_load: 0.5,
                    },
                ))
            })
            .collect();
        let balance = tower::balance::p2c::Balance::new(futures::stream::iter(members));
        let mut service = super::super::request_service(
            settings,
            TrickleRetry,
            tower::buffer::Buffer::new(balance, 32),
        );
        let mut previous = {
            let group = group.lock().unwrap();
            group.members[&bad].penalty(Instant::now(), &group.config)
        };
        // One live request per second, with four idle healthy competitors. A
        // random P2C pair alone cannot guarantee selection of the worst pod.
        for second in 1..=seconds {
            tokio::time::advance(Duration::from_secs(1)).await;
            let result = service.ready().await.unwrap().call(0).await;
            assert_eq!(result.is_err(), permanently_failing && second % 5 == 0);
            assert_eq!(calls.load(Relaxed), second / 5, "second={second}");
            if second % 5 == 0 {
                let group = group.lock().unwrap();
                let penalty = group.members[&bad].penalty(Instant::now(), &group.config);
                if permanently_failing {
                    assert!(penalty > 0.96 * group.config.max_penalty);
                    assert!(
                        group.members[&bad]
                            .trickle_due(Instant::now() + Duration::from_secs(5), &group.config)
                    );
                } else {
                    assert!(penalty > 1.0 && penalty < previous);
                }
                previous = penalty;
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn trickle_is_exclusive_and_cancellation_does_not_train_health() {
        let mut group = group(2, 100);
        group.config.ejection_enabled = false;
        let addr = *group.members.keys().next().unwrap();
        for _ in 0..100 {
            group.complete(addr, 1, true, None);
        }
        let group = Arc::new(Mutex::new(group));
        let token = EndpointToken {
            group: Arc::clone(&group),
            addr,
            generation: 1,
        };
        tokio::time::advance(Duration::from_secs(5)).await;
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        assert!(token.poll_available(&mut cx));
        let before = group.lock().unwrap().members[&addr].errors;
        let completion = token.start();
        assert!(completion.trickle);
        assert!(!token.poll_available(&mut cx));
        drop(completion);
        assert!(token.poll_available(&mut cx));
        assert_eq!(group.lock().unwrap().members[&addr].errors, before);
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_request_releases_idle_load_and_does_not_block_later_trickle() {
        use crate::sinks::util::{
            TowerRequestConfig, adaptive_concurrency::AdaptiveConcurrencyLimit,
        };
        use tower::{ServiceExt, load::Load};
        for degraded in [false, true] {
            let mut group = group(1, 100);
            group.config.ejection_enabled = false;
            let addr = *group.members.keys().next().unwrap();
            if degraded {
                for _ in 0..100 {
                    group.complete(addr, 1, true, None);
                }
            }
            let group = Arc::new(Mutex::new(group));
            let token = EndpointToken {
                group: Arc::clone(&group),
                addr,
                generation: 1,
            };
            let transport =
                tower::service_fn(|_: u8| futures::future::pending::<crate::Result<u8>>());
            let health =
                HealthService::new(transport, token.clone(), |_: &crate::Error| Some(true));
            let request: TowerRequestConfig = Default::default();
            let mut member = super::super::Member {
                inner: AdaptiveConcurrencyLimit::new(
                    health,
                    TrickleRetry,
                    Some(1),
                    request.into_settings().adaptive_concurrency,
                ),
                token: token.clone(),
                idle_load: 1.0,
            };
            let idle_load = member.load();
            let future = member.ready().await.unwrap().call(0);
            assert!(member.load() > idle_load);
            assert_eq!(group.lock().unwrap().members[&addr].active_requests, 1);
            drop(future);
            assert_eq!(group.lock().unwrap().members[&addr].active_requests, 0);
            assert_eq!(member.load(), idle_load);
            tokio::time::advance(Duration::from_secs(5)).await;
            assert_eq!(
                group.lock().unwrap().trickle_target(Instant::now()),
                degraded.then_some(addr)
            );
            let future = member.ready().await.unwrap().call(0);
            assert_eq!(
                group.lock().unwrap().members[&addr].trickle_in_flight,
                degraded
            );
            drop(future);
            assert!(!group.lock().unwrap().members[&addr].trickle_in_flight);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn due_ejection_probe_is_not_starved_by_overdue_soft_trickle() {
        let mut group = group(2, 50);
        let soft: SocketAddr = "127.0.0.1:80".parse().unwrap();
        let hard: SocketAddr = "127.0.0.2:80".parse().unwrap();
        for _ in 0..100 {
            group.complete(soft, 1, true, None);
        }
        group.members.get_mut(&hard).unwrap().eject(Instant::now());
        group.enforce_cap();
        let group = Arc::new(Mutex::new(group));
        let hard_token = EndpointToken {
            group: Arc::clone(&group),
            addr: hard,
            generation: 1,
        };
        let soft_token = EndpointToken {
            group: Arc::clone(&group),
            addr: soft,
            generation: 1,
        };
        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(
            group.lock().unwrap().trickle_target(Instant::now()),
            Some(soft)
        );
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        assert!(hard_token.poll_available(&mut cx));
        assert!(soft_token.poll_available(&mut cx));
        assert_eq!(hard_token.penalty(), 0.0);
        assert!(soft_token.penalty() > 1.0);
        let mut probe = hard_token.start();
        assert!(probe.probe.is_some());
        assert!(!hard_token.poll_available(&mut cx));
        assert!(soft_token.poll_available(&mut cx));
        probe.finish(true);
    }

    #[tokio::test(start_paused = true)]
    async fn pod_snapshot_includes_capped_components_idle_decay_and_probe_priority() {
        let mut group = group(1, 100);
        group.config.slow_start_secs = 60;
        group.initial_limit = 8;
        group.idle_load = 0.125;
        let addr = *group.members.keys().next().unwrap();
        let now = Instant::now();
        let state = group.members.get_mut(&addr).unwrap();
        state.record(true, now, &group.config);
        let pending = group.pod_snapshot(addr, &group.members[&addr], now);
        let pod = pending.finish();
        assert_eq!(pod.addr, addr);
        assert_eq!(pod.generation, 1);
        assert_eq!(pod.error_rate, 0.5);
        assert_eq!(pod.health_multiplier, 13.25);
        assert_eq!(pod.slow_start_multiplier, 10.0);
        assert_eq!(pod.capped_penalty, 50.0);
        assert_eq!(pod.current_limit, 8);
        assert_eq!(pod.in_flight, 0);
        assert_eq!(pod.normalized_load, 0.0);
        assert_eq!(pod.idle_floor, 0.125);
        assert_eq!(pod.effective_load, 6.25);
        assert!(!pod.ejected && !pod.probe_ready && !pod.probe_in_flight);
        group.members.get_mut(&addr).unwrap().eject(now);
        tokio::time::advance(Duration::from_secs(5)).await;
        let pod = group
            .pod_snapshot(addr, &group.members[&addr], Instant::now())
            .finish();
        assert_eq!(pod.epoch, 1);
        assert!(pod.ejected);
        assert!(pod.capped_penalty >= 1.0);
        assert_eq!(pod.penalty, 0.0);
        assert_eq!(pod.effective_load, 0.0);
        group
            .members
            .get_mut(&addr)
            .unwrap()
            .recover(Instant::now(), &group.config);
        tokio::time::advance(Duration::from_secs(600)).await;
        let pod = group
            .pod_snapshot(addr, &group.members[&addr], Instant::now())
            .finish();
        assert!(pod.capped_penalty < 1.001);
        assert!(pod.effective_load < 0.1251);
        assert!(!pod.ejected);
    }

    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn degraded_flips_are_collected_and_emitted_only_at_trace() {
        for level in [tracing::Level::DEBUG, tracing::Level::TRACE] {
            let (_guard, logs) = super::super::diagnostics::tests::capture(level);
            let group = Arc::new(Mutex::new(group(1, 0)));
            let addr = *group.lock().unwrap().members.keys().next().unwrap();
            for _ in 0..1_000 {
                let mut state = group.lock().unwrap();
                state.complete(addr, 1, true, None);
                assert!(state.members[&addr].degraded);
                state.complete(addr, 1, false, None);
                assert!(!state.members[&addr].degraded);
                if level == tracing::Level::DEBUG {
                    assert!(state.transitions.is_empty());
                }
                Group::emit_transitions(state);
            }
            {
                let mut state = group.lock().unwrap();
                state.complete(addr, 1, true, None);
                Group::emit_transitions(state);
            }
            tokio::time::advance(Duration::from_secs(60)).await;
            let mut state = group.lock().unwrap();
            state.metrics(Instant::now());
            assert!(!state.members[&addr].degraded);
            assert_eq!(state.degraded_members, 0);
            if level == tracing::Level::DEBUG {
                assert!(state.transitions.is_empty());
            }
            Group::emit_transitions(state);
            let events = logs.events("Dynamic balancer health changed.");
            if level == tracing::Level::DEBUG {
                assert!(events.is_empty());
            } else {
                assert_eq!(events.len(), 2_002);
                for event in &events {
                    assert_eq!(event["level"], "TRACE");
                    let fields = &event["fields"];
                    assert!(
                        (fields["prev_state"] == "healthy" && fields["new_state"] == "degraded")
                            || (fields["prev_state"] == "degraded"
                                && fields["new_state"] == "healthy")
                    );
                }
                assert_eq!(events[0]["fields"]["reason"], "request_outcome");
                assert_eq!(events.last().unwrap()["fields"]["reason"], "idle_decay");
            }
        }
    }

    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn degradation_changes_do_not_log_an_ejected_pod_as_healthy() {
        let (_guard, logs) = super::super::diagnostics::tests::capture(tracing::Level::TRACE);
        let group = Arc::new(Mutex::new(group(1, 100)));
        let addr = *group.lock().unwrap().members.keys().next().unwrap();
        {
            let mut state = group.lock().unwrap();
            state.members.get_mut(&addr).unwrap().eject(Instant::now());
            state.enforce_cap();
            state.complete(addr, 1, true, None);
            assert!(state.members[&addr].degraded);
            assert_eq!(state.members[&addr].state(), "ejected");
            Group::emit_transitions(state);
        }
        tokio::time::advance(Duration::from_secs(10)).await;
        let mut state = group.lock().unwrap();
        state.metrics(Instant::now());
        assert!(!state.members[&addr].degraded);
        assert_eq!(state.members[&addr].state(), "ejected");
        Group::emit_transitions(state);
        assert!(logs.events("Dynamic balancer health changed.").is_empty());
    }

    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn disabled_debug_does_not_queue_health_transitions() {
        crate::test_util::trace_init();
        let _guard = tracing::subscriber::set_default(tracing::subscriber::NoSubscriber::default());
        assert!(!tracing::enabled!(tracing::Level::DEBUG));
        let mut group = group(1, 100);
        let addr = *group.members.keys().next().unwrap();
        for _ in 0..20 {
            group.complete(addr, 1, true, None);
        }
        tokio::time::advance(Duration::from_secs(2)).await;
        group.complete(addr, 1, true, None);
        assert!(group.members[&addr].ejected_until.is_some());
        group.config.max_ejection_percent = 0;
        group.enforce_cap();
        assert!(group.members[&addr].ejected_until.is_none());
        group.metrics(Instant::now());
        assert!(group.members[&addr].degraded);
        assert!(group.transitions.is_empty());
    }

    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn health_logs_cover_degradation_probes_recovery_and_cap_enforcement() {
        let (_guard, logs) = super::super::diagnostics::tests::capture(tracing::Level::TRACE);
        let group = Arc::new(Mutex::new(group(1, 100)));
        let addr = *group.lock().unwrap().members.keys().next().unwrap();
        let token = EndpointToken {
            group: Arc::clone(&group),
            addr,
            generation: 1,
        };
        for _ in 0..20 {
            group.lock().unwrap().complete(addr, 1, true, None);
        }
        tokio::time::advance(Duration::from_secs(2)).await;
        group.lock().unwrap().complete(addr, 1, true, None);
        Group::emit_transitions(group.lock().unwrap());
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        for (backoff, outcome) in [(5, Some(true)), (10, None), (10, Some(false))] {
            tokio::time::advance(Duration::from_secs(backoff)).await;
            assert!(token.poll_available(&mut cx));
            let mut probe = token.start();
            if let Some(failed) = outcome {
                let before = logs.events("Dynamic balancer health changed.").len();
                probe.finish(failed);
                if !failed {
                    let events = logs.events("Dynamic balancer health changed.");
                    assert_eq!(events.len(), before + 1);
                    let fields = &events.last().unwrap()["fields"];
                    assert_eq!(fields["prev_state"], "probe_in_flight");
                    assert_eq!(fields["new_state"], "degraded");
                    assert_eq!(fields["reason"], "probe_success");
                }
            }
        }
        {
            let mut state = group.lock().unwrap();
            state.members.get_mut(&addr).unwrap().eject(Instant::now());
            state.config.max_ejection_percent = 0;
            state.enforce_cap();
            Group::emit_transitions(state);
        }
        let events = logs.events("Dynamic balancer health changed.");
        for reason in [
            "request_outcome",
            "sustained_failures",
            "backoff_elapsed",
            "probe_start",
            "probe_failure",
            "probe_cancel",
            "probe_success",
            "cap_enforcement",
        ] {
            assert!(
                events
                    .iter()
                    .any(|event| event["fields"]["reason"] == reason),
                "missing {reason}: {events:?}"
            );
        }
        assert!(
            events
                .iter()
                .any(|event| event["fields"]["prev_state"] == "healthy"
                    && event["fields"]["new_state"] == "degraded")
        );
        assert!(
            events
                .iter()
                .any(|event| event["fields"]["prev_state"] == "probe_in_flight"
                    && event["fields"]["new_state"] == "degraded")
        );
        for event in events {
            let fields = &event["fields"];
            let expected_level =
                if fields["reason"] == "request_outcome" || fields["reason"] == "idle_decay" {
                    "TRACE"
                } else {
                    "DEBUG"
                };
            assert_eq!(event["level"], expected_level);
            assert_eq!(fields["target"], "test-health");
            assert_eq!(fields["pod"], addr.to_string());
            assert_eq!(fields["generation"], 1);
            for name in [
                "epoch",
                "error_rate",
                "backoff",
                "ejected",
                "ejection_budget",
            ] {
                assert!(!fields[name].is_null(), "missing {name}: {fields:?}");
            }
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn health_events_are_emitted_after_unlocking_group() {
        crate::test_util::trace_init();
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tracing_subscriber::{Layer, prelude::*};
        struct CheckUnlocked {
            group: Arc<Mutex<Group>>,
            events: Arc<AtomicUsize>,
        }
        impl<S: tracing::Subscriber> Layer<S> for CheckUnlocked {
            fn on_event(
                &self,
                _: &tracing::Event<'_>,
                _: tracing_subscriber::layer::Context<'_, S>,
            ) {
                assert!(
                    self.group.try_lock().is_ok(),
                    "event emitted under balancer lock"
                );
                self.events.fetch_add(1, Ordering::Relaxed);
            }
        }
        let group = Arc::new(Mutex::new(group(1, 100)));
        let events = Arc::new(AtomicUsize::new(0));
        let subscriber = tracing_subscriber::registry()
            .with(CheckUnlocked {
                group: Arc::clone(&group),
                events: Arc::clone(&events),
            })
            .with(tracing_subscriber::filter::LevelFilter::TRACE);
        let _guard = tracing::subscriber::set_default(subscriber);
        let addr = *group.lock().unwrap().members.keys().next().unwrap();
        group.lock().unwrap().complete(addr, 1, true, None);
        Group::emit_transitions(group.lock().unwrap());
        assert_eq!(events.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn health_penalty_grows_smoothly_is_capped_and_decays_when_idle() {
        let config = LoadBalancingConfig {
            slow_start_secs: 0,
            ..Default::default()
        };
        let now = Instant::now();
        let mut state = EndpointHealth::new(1, now, &config);
        let mut previous = state.penalty(now, &config);
        assert_eq!(previous, 1.0);
        for _ in 0..100 {
            state.record(true, now, &config);
            let penalty = state.penalty(now, &config);
            assert!(penalty > previous && penalty <= config.max_penalty);
            previous = penalty;
        }
        for _ in 0..10 {
            tokio::time::advance(Duration::from_secs(30)).await;
            let penalty = state.penalty(Instant::now(), &config);
            assert!(penalty > 1.0 && penalty < previous);
            previous = penalty;
        }
        tokio::time::advance(Duration::from_secs(300)).await;
        assert!(state.penalty(Instant::now(), &config) < 1.000001);
    }
    #[test]
    fn slow_start_penalty_decays_and_can_be_disabled() {
        let mut config = LoadBalancingConfig::default();
        let now = Instant::now();
        let state = EndpointHealth::new(1, now, &config);
        assert_eq!(state.penalty(now, &config), 10.0);
        let later = state.penalty(now + Duration::from_secs(60), &config);
        assert!((later - (1.0 + 9.0 / std::f64::consts::E)).abs() < 0.001);
        assert!(state.penalty(now + Duration::from_secs(600), &config) < 1.001);
        config.slow_start_secs = 0;
        assert_eq!(state.penalty(now, &config), 1.0);
    }
    #[tokio::test(start_paused = true)]
    async fn sustained_failures_eject_only_up_to_cap_and_dns_shrink_reduces_budget() {
        let mut group = group(10, 30);
        let addresses: Vec<_> = group.members.keys().copied().collect();
        for &addr in &addresses {
            for _ in 0..20 {
                group.complete(addr, 1, true, None);
            }
        }
        assert!(group.members.values().all(|s| s.ejected_until.is_none()));
        tokio::time::advance(Duration::from_secs(2)).await;
        for &addr in &addresses {
            group.complete(addr, 1, true, None);
        }
        assert_eq!(
            group
                .members
                .values()
                .filter(|s| s.ejected_until.is_some())
                .count(),
            3
        );
        assert_eq!(
            group.members.len(),
            10,
            "health must not change DNS membership"
        );
        struct WakeCounter(std::sync::atomic::AtomicUsize);
        impl futures::task::ArcWake for WakeCounter {
            fn wake_by_ref(counter: &Arc<Self>) {
                counter.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        let wakes = Arc::new(WakeCounter(std::sync::atomic::AtomicUsize::new(0)));
        for state in group
            .members
            .values_mut()
            .filter(|s| s.ejected_until.is_some())
        {
            state.waker = Some(futures::task::waker(Arc::clone(&wakes)));
        }
        let ejected: Vec<_> = group
            .members
            .iter()
            .filter_map(|(a, s)| s.ejected_until.map(|_| *a))
            .collect();
        group.members.retain(|addr, _| ejected.contains(addr));
        group.enforce_cap();
        assert_eq!(group.ejected, 0);
        assert_eq!(group.ready_probes, 0);
        assert!(!group.use_fallback());
        assert!(group.members.values().all(|s| s.ejected_until.is_none()));
        assert_eq!(wakes.0.load(std::sync::atomic::Ordering::Relaxed), 3);
    }
    #[tokio::test(start_paused = true)]
    async fn ejected_endpoint_probes_with_backoff_and_recovers_on_success() {
        let group = Arc::new(Mutex::new(group(1, 100)));
        let addr = *group.lock().unwrap().members.keys().next().unwrap();
        let token = EndpointToken {
            group: Arc::clone(&group),
            addr,
            generation: 1,
        };
        {
            let mut group = group.lock().unwrap();
            for _ in 0..20 {
                group.complete(addr, 1, true, None);
            }
        }
        tokio::time::advance(Duration::from_secs(2)).await;
        group.lock().unwrap().complete(addr, 1, true, None);
        assert!(group.lock().unwrap().use_fallback());
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        assert!(!token.poll_available(&mut cx));
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(token.poll_available(&mut cx));
        assert!(
            !group.lock().unwrap().use_fallback(),
            "a due probe must be dispatched"
        );
        assert_eq!(token.penalty(), 0.0);
        let mut probe = token.start();
        assert!(!token.poll_available(&mut cx), "one probe in flight");
        probe.finish(true);
        assert!(!token.poll_available(&mut cx));
        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(token.poll_available(&mut cx));
        token.start().finish(false);
        assert!(token.poll_available(&mut cx));
        assert!(
            token.penalty() > 1.0,
            "probe success retains health history"
        );
        assert!(group.lock().unwrap().members[&addr].ejected_until.is_none());
    }
    #[tokio::test]
    async fn cancelled_probe_cannot_reeject_after_dns_cap_recovery() {
        let group = Arc::new(Mutex::new(group(1, 100)));
        let addr = *group.lock().unwrap().members.keys().next().unwrap();
        group
            .lock()
            .unwrap()
            .members
            .get_mut(&addr)
            .unwrap()
            .eject(Instant::now());
        group.lock().unwrap().enforce_cap();
        let token = EndpointToken {
            group: Arc::clone(&group),
            addr,
            generation: 1,
        };
        let probe = token.start();
        {
            let mut group = group.lock().unwrap();
            group.config.max_ejection_percent = 0;
            group.enforce_cap();
        }
        drop(probe);
        assert!(group.lock().unwrap().members[&addr].ejected_until.is_none());
    }
    #[tokio::test(start_paused = true)]
    async fn stale_probe_cannot_change_a_new_ejection_or_release_its_probe() {
        for outcome in [Some(false), Some(true), None] {
            let group = Arc::new(Mutex::new(group(1, 100)));
            let addr = *group.lock().unwrap().members.keys().next().unwrap();
            let token = EndpointToken {
                group: Arc::clone(&group),
                addr,
                generation: 1,
            };
            {
                let mut group = group.lock().unwrap();
                group.members.get_mut(&addr).unwrap().eject(Instant::now());
                group.enforce_cap();
            }
            tokio::time::advance(Duration::from_secs(5)).await;
            let mut cx = Context::from_waker(futures::task::noop_waker_ref());
            assert!(token.poll_available(&mut cx));
            let mut old_probe = token.start();
            {
                let mut group = group.lock().unwrap();
                group.config.max_ejection_percent = 0;
                group.enforce_cap();
                group.config.max_ejection_percent = 100;
                group.members.get_mut(&addr).unwrap().eject(Instant::now());
                group.enforce_cap();
            }
            tokio::time::advance(Duration::from_secs(5)).await;
            assert!(token.poll_available(&mut cx));
            let mut new_probe = token.start();
            let epoch = new_probe.probe;
            let until = group.lock().unwrap().members[&addr].ejected_until;
            if let Some(failed) = outcome {
                old_probe.finish(failed);
            }
            drop(old_probe);
            assert_eq!(group.lock().unwrap().members[&addr].ejected_until, until);
            assert_eq!(
                Some(group.lock().unwrap().members[&addr].ejection_epoch),
                epoch
            );
            assert!(
                !token.poll_available(&mut cx),
                "the newer probe must remain exclusive"
            );
            assert!(group.lock().unwrap().use_fallback());
            new_probe.finish(false);
            assert!(token.poll_available(&mut cx));
            assert!(!group.lock().unwrap().use_fallback());
        }
    }

    #[tokio::test]
    async fn ejected_member_releases_reserved_concurrency_before_polling_inner() {
        use crate::sinks::util::{
            TowerRequestConfig, adaptive_concurrency::AdaptiveConcurrencyLimit, retries::RetryLogic,
        };
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
        let group = Arc::new(Mutex::new(group(1, 100)));
        let addr = *group.lock().unwrap().members.keys().next().unwrap();
        let request: TowerRequestConfig = Default::default();
        let settings = request.into_settings();
        let inner = AdaptiveConcurrencyLimit::new(
            tower::service_fn(|_: u8| async { Ok::<_, crate::Error>(0u8) }),
            Retry,
            Some(1),
            settings.adaptive_concurrency,
        );
        let mut observer = inner.clone();
        let mut member = super::super::Member {
            inner,
            token: EndpointToken {
                group: Arc::clone(&group),
                addr,
                generation: 1,
            },
            idle_load: 1.0,
        };
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        assert!(matches!(member.poll_ready(&mut cx), Poll::Ready(Ok(()))));
        assert!(observer.poll_ready(&mut cx).is_pending());
        {
            let mut group = group.lock().unwrap();
            group.members.get_mut(&addr).unwrap().eject(Instant::now());
            group.enforce_cap();
        }
        assert!(member.poll_ready(&mut cx).is_pending());
        assert!(matches!(observer.poll_ready(&mut cx), Poll::Ready(Ok(()))));
    }

    #[tokio::test(start_paused = true)]
    async fn completions_update_only_affected_metrics_and_tick_refreshes_idle_members() {
        let mut group = group(2, 100);
        let addresses: Vec<_> = group.members.keys().copied().collect();
        group.complete(addresses[0], 1, true, None);
        group.complete(addresses[0], 1, true, None);
        assert_eq!(group.degraded_members, 1);
        tokio::time::advance(Duration::from_secs(300)).await;
        group.complete(addresses[1], 1, false, None);
        assert!(
            group.members[&addresses[0]].degraded,
            "idle pod awaits DNS tick"
        );
        assert_eq!(group.degraded_members, 1);
        group.metrics(Instant::now());
        assert!(!group.members[&addresses[0]].degraded);
        assert_eq!(group.degraded_members, 0);
    }

    #[test]
    fn stale_completion_cannot_poison_a_readded_endpoint() {
        let mut group = group(1, 100);
        let addr = *group.members.keys().next().unwrap();
        group.members.get_mut(&addr).unwrap().generation = 2;
        group.complete(addr, 1, true, None);
        assert_eq!(
            group.members[&addr].error_rate(Instant::now(), &group.config),
            0.0
        );
    }
}

#[cfg(test)]
mod outcome_tests {
    use super::*;
    use tower::{ServiceExt, timeout::Timeout};
    fn state() -> (Arc<Mutex<Group>>, EndpointToken) {
        let config = LoadBalancingConfig::default();
        let addr = "127.0.0.1:80".parse().unwrap();
        let mut group = Group::new(config.clone(), "test-outcomes".into());
        group
            .members
            .insert(addr, EndpointHealth::new(1, Instant::now(), &config));
        let group = Arc::new(Mutex::new(group));
        let token = EndpointToken {
            group: Arc::clone(&group),
            addr,
            generation: 1,
        };
        (group, token)
    }
    #[tokio::test]
    async fn ignored_backpressure_does_not_train_health() {
        let (group, token) = state();
        let addr = token.addr;
        // Transport outcomes are external to the real health tracker.
        let transport =
            tower::service_fn(|()| async { Err::<(), crate::Error>("backpressure".into()) });
        let mut health = HealthService::new(transport, token, |_: &crate::Error| None);
        assert!(health.ready().await.unwrap().call(()).await.is_err());
        let group = group.lock().unwrap();
        assert_eq!(group.members[&addr].samples, 1.0);
        assert_eq!(group.members[&addr].errors, 0.0);
    }
    #[tokio::test(start_paused = true)]
    async fn resource_exhausted_probe_recovers_gradually_without_training_health() {
        let (group, token) = state();
        let addr = token.addr;
        let (errors, samples) = {
            let mut group = group.lock().unwrap();
            group.config.ejection_enabled = true;
            group.config.max_ejection_percent = 100;
            for _ in 0..100 {
                group.complete(addr, 1, true, None);
            }
            let state = group.members.get_mut(&addr).unwrap();
            state.eject(Instant::now());
            let history = (state.errors, state.samples);
            group.enforce_cap();
            history
        };
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(token.poll_available(&mut Context::from_waker(futures::task::noop_waker_ref())));
        let transport = tower::service_fn(|()| async {
            Err::<(), crate::Error>(tonic::Status::resource_exhausted("backpressure").into())
        });
        let mut health = HealthService::new(transport, token.clone(), |error: &crate::Error| {
            super::super::classify_grpc_error(error.downcast_ref::<tonic::Status>().unwrap())
        });
        assert!(health.ready().await.unwrap().call(()).await.is_err());
        {
            let group = group.lock().unwrap();
            let state = &group.members[&addr];
            assert!(state.ejected_until.is_none());
            assert!(!state.probe_in_flight);
            assert_eq!((state.errors, state.samples), (errors, samples));
            assert_eq!(state.born, Instant::now());
            assert_eq!(group.ejected_count(), 0);
        }
        let penalty = token.penalty();
        assert!(penalty > 1.0);
        let rate = {
            let group = group.lock().unwrap();
            group.members[&addr].error_rate(Instant::now(), &group.config)
        };
        tokio::time::advance(Duration::from_secs(5)).await;
        {
            let group = group.lock().unwrap();
            assert!(group.members[&addr].error_rate(Instant::now(), &group.config) < rate);
        }
        // The combined cold/health penalty initially saturates its configured cap.
        tokio::time::advance(Duration::from_secs(295)).await;
        assert!(token.penalty() > 1.0 && token.penalty() < penalty);
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_is_observed_inside_health_without_removing_dns_membership() {
        let (group, token) = state();
        let addr = token.addr;
        let transport = tower::service_fn(|()| futures::future::pending::<crate::Result<()>>());
        let mut health = HealthService::new(
            Timeout::new(transport, Duration::from_secs(1)),
            token,
            |error: &crate::Error| Some(error.is::<tower::timeout::error::Elapsed>()),
        );
        let error = health.ready().await.unwrap().call(()).await.unwrap_err();
        assert!(error.is::<tower::timeout::error::Elapsed>());
        let group = group.lock().unwrap();
        assert_eq!(group.members.len(), 1);
        // One timeout adds one failed sample to the idle-decayed healthy prior.
        let decay = (-1.0f64 / 30.0).exp();
        let expected_rate = 1.0 / (decay + IDLE_HEALTHY_SAMPLE_WEIGHT * (1.0 - decay) + 1.0);
        assert!(
            (group.members[&addr].error_rate(Instant::now(), &group.config) - expected_rate).abs()
                < 1e-12
        );
    }
}
