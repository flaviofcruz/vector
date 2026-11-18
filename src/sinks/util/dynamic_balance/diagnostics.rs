use std::{fmt, net::SocketAddr, time::Duration};

use super::super::adaptive_concurrency::ConcurrencyDiagnostics;

pub(super) struct PendingPodSnapshot {
    pub pod: PodSnapshot,
    pub concurrency: Option<ConcurrencyDiagnostics>,
}

impl PendingPodSnapshot {
    pub fn finish(mut self) -> PodSnapshot {
        if let Some(concurrency) = self.concurrency {
            self.pod.current_limit = concurrency.snapshot().0;
        }
        self.pod.normalized_load = if self.pod.current_limit > 0 {
            self.pod.in_flight as f64 / self.pod.current_limit as f64
        } else {
            1.0
        };
        self.pod.effective_load =
            (self.pod.normalized_load + self.pod.idle_floor) * self.pod.penalty;
        self.pod
    }
}

pub(super) struct PodSnapshot {
    pub addr: SocketAddr,
    pub generation: u64,
    pub epoch: u64,
    pub error_rate: f64,
    pub health_multiplier: f64,
    pub slow_start_multiplier: f64,
    pub capped_penalty: f64,
    pub penalty: f64,
    pub current_limit: usize,
    pub in_flight: usize,
    pub normalized_load: f64,
    pub idle_floor: f64,
    pub effective_load: f64,
    pub ejected: bool,
    pub probe_ready: bool,
    pub probe_in_flight: bool,
}

impl fmt::Debug for PodSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pod")
            .field("addr", &self.addr)
            .field("generation", &self.generation)
            .field("epoch", &self.epoch)
            .field("error_rate", &self.error_rate)
            .field("health_multiplier", &self.health_multiplier)
            .field("slow_start_multiplier", &self.slow_start_multiplier)
            .field("capped_penalty", &self.capped_penalty)
            .field("penalty", &self.penalty)
            .field("current_limit", &self.current_limit)
            .field("in_flight", &self.in_flight)
            .field("normalized_load", &self.normalized_load)
            .field("idle_floor", &self.idle_floor)
            .field("effective_load", &self.effective_load)
            .field("ejected", &self.ejected)
            .field("probe_ready", &self.probe_ready)
            .field("probe_in_flight", &self.probe_in_flight)
            .finish()
    }
}

pub(super) struct HealthTransition {
    pub pod: SocketAddr,
    pub generation: u64,
    pub epoch: u64,
    pub prev_state: &'static str,
    pub new_state: &'static str,
    pub reason: &'static str,
    pub error_rate: f64,
    pub backoff: Duration,
}

impl HealthTransition {
    pub fn emit(self, target: &str, ejected: usize, ejection_budget: usize) {
        match self.reason {
            "request_outcome" | "idle_decay" => trace!(
                target = %target, pod = %self.pod, generation = self.generation,
                epoch = self.epoch, prev_state = self.prev_state, new_state = self.new_state,
                reason = self.reason, error_rate = self.error_rate, backoff = ?self.backoff,
                ejected, ejection_budget,
                "Dynamic balancer health changed."
            ),
            _ => debug!(
                target = %target, pod = %self.pod, generation = self.generation,
                epoch = self.epoch, prev_state = self.prev_state, new_state = self.new_state,
                reason = self.reason, error_rate = self.error_rate, backoff = ?self.backoff,
                ejected, ejection_budget,
                "Dynamic balancer health changed."
            ),
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::{
        io,
        sync::{Arc, Mutex},
    };

    #[derive(Clone, Default)]
    pub struct Logs(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Logs {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Logs {
        pub fn events(&self, message: &str) -> Vec<serde_json::Value> {
            String::from_utf8(self.0.lock().unwrap().clone())
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .filter(|event| event["fields"]["message"] == message)
                .collect()
        }
    }

    pub fn capture(level: tracing::Level) -> (tracing::subscriber::DefaultGuard, Logs) {
        crate::test_util::trace_init();
        let logs = Logs::default();
        let writer = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_max_level(level)
            .with_writer(move || writer.clone())
            .finish();
        (tracing::subscriber::set_default(subscriber), logs)
    }
}
