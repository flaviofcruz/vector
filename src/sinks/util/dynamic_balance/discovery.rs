use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use futures::{FutureExt, Stream, StreamExt, future::BoxFuture};
use http::Uri;
use tokio::{
    sync::watch,
    task::AbortHandle,
    time::{Instant, MissedTickBehavior},
};
use tokio_stream::wrappers::WatchStream;
use tower::discover::Change;
use tracing::Instrument;

use super::{
    LoadBalancingConfig,
    health::{EndpointHealth, Group},
};

pub struct DnsTarget {
    pub(super) uri: Uri,
    resolve: Box<dyn Fn(Uri) -> BoxFuture<'static, crate::Result<Vec<SocketAddr>>> + Send>,
}

impl From<Uri> for DnsTarget {
    fn from(uri: Uri) -> Self {
        Self {
            uri,
            resolve: Box::new(|target| {
                async move {
                    super::super::dns::resolve_addresses(
                        target.host().expect("validated DNS hostname"),
                        target.port_u16().expect("validated port"),
                    )
                    .await
                }
                .boxed()
            }),
        }
    }
}

impl DnsTarget {
    #[cfg(test)]
    pub(crate) fn with_resolver<F>(uri: Uri, resolve: F) -> Self
    where
        F: Fn(Uri) -> BoxFuture<'static, crate::Result<Vec<SocketAddr>>> + Send + 'static,
    {
        Self {
            uri,
            resolve: Box::new(resolve),
        }
    }
}

#[derive(Clone, Default)]
struct Snapshot {
    generation: u64,
    addresses: HashSet<SocketAddr>,
}

#[derive(Default, Debug, PartialEq)]
struct MembershipDiff {
    added: Vec<SocketAddr>,
    removed: Vec<SocketAddr>,
}

impl MembershipDiff {
    fn between(group: &Group, addresses: &HashSet<SocketAddr>) -> Self {
        let mut added: Vec<_> = addresses
            .iter()
            .filter(|addr| !group.members.contains_key(addr))
            .copied()
            .collect();
        let mut removed: Vec<_> = group
            .members
            .keys()
            .filter(|addr| !addresses.contains(addr))
            .copied()
            .collect();
        added.sort_unstable();
        removed.sort_unstable();
        Self { added, removed }
    }
}

/// Serialized lookups plus snapshot generations reject late/out-of-order results.
/// Empty/error answers deliberately do not revoke the last-good membership.
fn reconcile(group: &mut Group, snapshot: &Snapshot, diff: Option<&MembershipDiff>) -> bool {
    let _entered = group.span.clone().entered();
    if !group.active || snapshot.generation <= group.generation || snapshot.addresses.is_empty() {
        return false;
    }
    if let Some(diff) = diff {
        for &addr in &diff.removed {
            let prev = group.members[&addr].state();
            group.transition(addr, prev, "removed", "dns_removal", Instant::now());
        }
    }
    group.generation = snapshot.generation;
    group.members.retain(|addr, _| {
        if snapshot.addresses.contains(addr) {
            true
        } else {
            metrics::gauge!("sink_lb_health_score", "target" => group.target.clone(),
                "pod" => addr.to_string())
            .set(0.0);
            metrics::gauge!("sink_lb_ejected", "target" => group.target.clone(),
                "pod" => addr.to_string())
            .set(0.0);
            false
        }
    });
    let now = Instant::now();
    for &addr in &snapshot.addresses {
        group
            .members
            .entry(addr)
            .or_insert_with(|| EndpointHealth::new(snapshot.generation, now, &group.config));
    }
    group.enforce_cap();
    group.metrics(now);
    // Membership changes can remove a trickle target that held other pods pending.
    group.wake_members();
    true
}

pub(super) struct DnsDiscover<S> {
    pub group: Arc<Mutex<Group>>,
    snapshots: WatchStream<Snapshot>,
    known: HashMap<SocketAddr, u64>,
    pending: VecDeque<Change<SocketAddr, S>>,
    factory: Box<dyn Fn(SocketAddr, u64, &mut EndpointHealth) -> S + Send>,
    task: AbortHandle,
}

impl<S> DnsDiscover<S> {
    pub fn new<F>(
        target: DnsTarget,
        config: LoadBalancingConfig,
        group: Arc<Mutex<Group>>,
        factory: F,
    ) -> Self
    where
        F: Fn(SocketAddr, u64, &mut EndpointHealth) -> S + Send + 'static,
    {
        let DnsTarget {
            uri: target,
            resolve,
        } = target;
        let (tx, rx) = watch::channel(Snapshot::default());
        let task_group = Arc::clone(&group);
        let task = tokio::spawn(async move {
            let period = Duration::from_secs(config.refresh_interval_secs);
            let mut timer = tokio::time::interval(period);
            timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut generation = 0;
            let mut refresh_generation = 0;
            loop {
                timer.tick().await;
                refresh_generation += 1;
                let started = Instant::now();
                let result = tokio::time::timeout(period, resolve(target.clone())).await;
                let lookup_duration = started.elapsed();
                let diagnostics = tracing::enabled!(tracing::Level::DEBUG);
                let (addresses, raw_record_count, status, error) = match result {
                    Ok(Ok(addresses)) => {
                        let raw_count = addresses.len();
                        let addresses: HashSet<_> = addresses.into_iter().filter(SocketAddr::is_ipv4).collect();
                        let status = if addresses.is_empty() { "empty" } else { "success" };
                        (addresses, Some(raw_count), status, None)
                    }
                    Ok(Err(error)) => (HashSet::new(), None, "error", diagnostics.then(|| error.to_string())),
                    Err(error) => (HashSet::new(), None, "timeout", diagnostics.then(|| error.to_string())),
                };
                // Preserve the existing metric's error label for lookup timeouts.
                let metric_status = if status == "timeout" { "error" } else { status };
                metrics::counter!("sink_lb_dns_refresh_total", "target" => target.to_string(), "status" => metric_status).increment(1);
                let mut group = task_group.lock().expect("balancer state poisoned");
                let mut diff = diagnostics.then(|| if status == "success" {
                    MembershipDiff::between(&group, &addresses)
                } else { MembershipDiff::default() });
                let snapshot = if status == "success" {
                    generation += 1;
                    let snapshot = Snapshot { generation, addresses };
                    if !reconcile(&mut group, &snapshot, diff.as_ref())
                        && let Some(diff) = &mut diff {
                            *diff = MembershipDiff::default();
                        }
                    Some(snapshot)
                } else {
                    group.metrics(Instant::now());
                    None
                };
                let member_count = group.members.len();
                let pods = diagnostics.then(|| {
                    let now = Instant::now();
                    group.members.iter().map(|(&addr, state)| group.pod_snapshot(addr, state, now)).collect::<Vec<_>>()
                });
                Group::emit_transitions(group);
                if let (Some(diff), Some(pods)) = (diff, pods) {
                    let dedup_ipv4_count = snapshot.as_ref().map_or(0, |s| s.addresses.len());
                    debug!(
                        target = %target, refresh_generation, status, raw_record_count,
                        dedup_ipv4_count, member_count, added_ips = ?diff.added, removed_ips = ?diff.removed,
                        retained_last_good = status != "success" && member_count > 0,
                        lookup_duration = ?lookup_duration, error = error.as_deref(),
                        "Dynamic balancer DNS refresh."
                    );
                    let mut pods: Vec<_> = pods.into_iter().map(|pod| pod.finish()).collect();
                    pods.sort_unstable_by_key(|pod| pod.addr);
                    debug!(target = %target, refresh_generation, pods = ?pods,
                        "Dynamic balancer pod snapshot.");
                }
                if let Some(snapshot) = snapshot
                    && tx.send(snapshot).is_err() { break; }
            }
        }.instrument(tracing::Span::current())).abort_handle();
        Self {
            group,
            snapshots: WatchStream::new(rx),
            known: HashMap::new(),
            pending: VecDeque::new(),
            factory: Box::new(factory),
            task,
        }
    }
}

impl<S: Unpin> Stream for DnsDiscover<S> {
    type Item = crate::Result<Change<SocketAddr, S>>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(change) = this.pending.pop_front() {
            return Poll::Ready(Some(Ok(change)));
        }
        while let Poll::Ready(snapshot) = this.snapshots.poll_next_unpin(cx) {
            if snapshot.is_none() {
                return Poll::Ready(None);
            }
            let mut group = this.group.lock().expect("balancer state poisoned");
            this.known.retain(|addr, generation| {
                if group
                    .members
                    .get(addr)
                    .is_some_and(|state| state.generation == *generation)
                {
                    true
                } else {
                    this.pending.push_back(Change::Remove(*addr));
                    false
                }
            });
            for (&addr, state) in &mut group.members {
                if let std::collections::hash_map::Entry::Vacant(entry) = this.known.entry(addr) {
                    this.pending.push_back(Change::Insert(
                        addr,
                        (this.factory)(addr, state.generation, state),
                    ));
                    entry.insert(state.generation);
                }
            }
            if let Some(change) = this.pending.pop_front() {
                return Poll::Ready(Some(Ok(change)));
            }
        }
        Poll::Pending
    }
}

impl<S> Drop for DnsDiscover<S> {
    fn drop(&mut self) {
        self.task.abort();
        let mut group = self.group.lock().expect("balancer state poisoned");
        let _entered = group.span.clone().entered();
        metrics::gauge!("sink_lb_members", "target" => group.target.clone()).set(0.0);
        metrics::gauge!("sink_lb_degraded_members", "target" => group.target.clone()).set(0.0);
        for addr in group.members.keys() {
            metrics::gauge!("sink_lb_health_score", "target" => group.target.clone(), "pod" => addr.to_string()).set(0.0);
            metrics::gauge!("sink_lb_ejected", "target" => group.target.clone(), "pod" => addr.to_string()).set(0.0);
        }
        group.active = false;
        group.members.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn snapshot(generation: u64, addresses: &[&str]) -> Snapshot {
        Snapshot {
            generation,
            addresses: addresses.iter().map(|a| a.parse().unwrap()).collect(),
        }
    }
    #[test]
    fn membership_diff_reports_sorted_changes_before_reconciliation() {
        let mut group = Group::new(LoadBalancingConfig::default(), "test".into());
        let original = snapshot(1, &["127.0.0.3:80", "127.0.0.1:80", "127.0.0.2:80"]);
        assert!(reconcile(&mut group, &original, None));
        let next = snapshot(2, &["127.0.0.4:80", "127.0.0.2:80", "127.0.0.4:80"]);
        let diff = MembershipDiff::between(&group, &next.addresses);
        assert_eq!(
            diff.added,
            vec!["127.0.0.4:80".parse::<SocketAddr>().unwrap()]
        );
        assert_eq!(
            diff.removed,
            vec![
                "127.0.0.1:80".parse::<SocketAddr>().unwrap(),
                "127.0.0.3:80".parse().unwrap()
            ]
        );
        assert_eq!(group.generation, 1);
        assert_eq!(group.members.len(), 3);
        assert!(reconcile(&mut group, &next, None));
        assert_eq!(
            MembershipDiff::between(&group, &next.addresses),
            MembershipDiff::default()
        );
        assert_eq!(
            MembershipDiff::between(&group, &HashSet::new())
                .removed
                .len(),
            2
        );
    }

    #[test]
    fn discovery_deduplicates_retains_last_good_and_rejects_old_generations() {
        let mut group = Group::new(LoadBalancingConfig::default(), "test".into());
        assert!(reconcile(
            &mut group,
            &snapshot(1, &["127.0.0.1:80", "127.0.0.1:80"]),
            None
        ));
        assert_eq!(group.members.len(), 1);
        assert!(!reconcile(&mut group, &snapshot(2, &[]), None));
        assert_eq!(group.members.len(), 1);
        assert!(reconcile(&mut group, &snapshot(3, &["127.0.0.2:80"]), None));
        assert!(!reconcile(
            &mut group,
            &snapshot(1, &["127.0.0.1:80"]),
            None
        ));
        assert!(group.members.contains_key(&"127.0.0.2:80".parse().unwrap()));
    }
}

#[cfg(test)]
mod stream_tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn dns_diff_emits_only_membership_changes_and_keeps_last_good_on_error_or_empty() {
        let a: SocketAddr = "127.0.0.1:80".parse().unwrap();
        let b: SocketAddr = "127.0.0.2:80".parse().unwrap();
        // DNS is the external dependency; all reconciliation and stream logic is real.
        let (tx, rx) = watch::channel(Some(vec![a, a]));
        let target = DnsTarget::with_resolver("http://test:80".parse().unwrap(), move |_| {
            let answer = rx.borrow().clone();
            async move { answer.ok_or_else(|| "DNS lookup failed".into()) }.boxed()
        });
        let config = LoadBalancingConfig {
            refresh_interval_secs: 1,
            ..Default::default()
        };
        let group = Arc::new(Mutex::new(Group::new(config.clone(), "test".into())));
        let mut discover = DnsDiscover::new(target, config, Arc::clone(&group), |_, _, _| ());
        assert!(
            matches!(discover.next().await.unwrap().unwrap(), Change::Insert(addr, ()) if addr == a)
        );
        let first_generation = group.lock().unwrap().members[&a].generation;
        assert_eq!(discover.known.len(), 1);
        for answer in [Some(vec![]), None, Some(vec![a])] {
            tx.send(answer).unwrap();
            tokio::time::advance(Duration::from_secs(1)).await;
            assert!(
                tokio::time::timeout(Duration::from_millis(100), discover.next())
                    .await
                    .is_err()
            );
            assert_eq!(group.lock().unwrap().members.len(), 1);
        }
        tx.send(Some(vec![b])).unwrap();
        assert!(
            matches!(discover.next().await.unwrap().unwrap(), Change::Remove(addr) if addr == a)
        );
        assert!(
            matches!(discover.next().await.unwrap().unwrap(), Change::Insert(addr, ()) if addr == b)
        );
        tx.send(Some(vec![a])).unwrap();
        assert!(
            matches!(discover.next().await.unwrap().unwrap(), Change::Remove(addr) if addr == b)
        );
        assert!(
            matches!(discover.next().await.unwrap().unwrap(), Change::Insert(addr, ()) if addr == a)
        );
        assert!(group.lock().unwrap().members[&a].generation > first_generation);
    }
    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn refresh_logs_each_outcome_and_all_pods_in_one_snapshot() {
        let (_guard, logs) = super::super::diagnostics::tests::capture(tracing::Level::DEBUG);
        let a: SocketAddr = "127.0.0.1:80".parse().unwrap();
        let b: SocketAddr = "127.0.0.2:80".parse().unwrap();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = Arc::clone(&calls);
        let target = DnsTarget::with_resolver("http://test:80".parse().unwrap(), move |_| {
            use std::sync::atomic::Ordering;
            let attempt = count.fetch_add(1, Ordering::Relaxed);
            async move {
                match attempt {
                    0 => Ok(vec![a, b, a, "[::1]:80".parse().unwrap()]),
                    1 => Ok(vec![]),
                    2 => Err("DNS lookup failed".into()),
                    3 => futures::future::pending().await,
                    _ => Ok(vec![b]),
                }
            }
            .boxed()
        });
        let config = LoadBalancingConfig {
            refresh_interval_secs: 1,
            ..Default::default()
        };
        let group = Arc::new(Mutex::new(Group::new(config.clone(), "test".into())));
        let mut discover = DnsDiscover::new(target, config, Arc::clone(&group), |_, _, _| ());
        discover.next().await.unwrap().unwrap();
        for _ in 0..5 {
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }
        drop(discover);
        let events = logs.events("Dynamic balancer DNS refresh.");
        let statuses: Vec<_> = events
            .iter()
            .map(|event| event["fields"]["status"].as_str().unwrap())
            .collect();
        assert!(
            statuses.starts_with(&["success", "empty", "error", "timeout", "success"]),
            "{events:?}"
        );
        let fields = &events[0]["fields"];
        assert_eq!(fields["raw_record_count"], 4);
        assert_eq!(fields["dedup_ipv4_count"], 2);
        assert_eq!(fields["member_count"], 2);
        assert_eq!(fields["added_ips"], "[127.0.0.1:80, 127.0.0.2:80]");
        for (index, event) in events.iter().enumerate() {
            assert_eq!(event["fields"]["refresh_generation"], index + 1);
        }
        for event in &events[1..4] {
            assert_eq!(event["fields"]["retained_last_good"], true);
            assert_eq!(event["fields"]["member_count"], 2);
            assert_eq!(event["fields"]["removed_ips"], "[]");
        }
        assert_eq!(events[4]["fields"]["removed_ips"], "[127.0.0.1:80]");
        assert_eq!(events[2]["fields"]["error"], "DNS lookup failed");
        let snapshots = logs.events("Dynamic balancer pod snapshot.");
        assert_eq!(snapshots.len(), events.len());
        for snapshot in &snapshots[..4] {
            let pods = snapshot["fields"]["pods"].as_str().unwrap();
            assert!(pods.contains("127.0.0.1:80") && pods.contains("127.0.0.2:80"));
            assert_eq!(
                snapshot["target"],
                "vector::sinks::util::dynamic_balance::discovery"
            );
        }
        let removed = logs.events("Dynamic balancer health changed.");
        assert!(
            removed
                .iter()
                .any(|event| event["fields"]["reason"] == "dns_removal"
                    && event["fields"]["pod"] == "127.0.0.1:80")
        );
    }

    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn rejected_refresh_does_not_log_membership_changes() {
        let (_guard, logs) = super::super::diagnostics::tests::capture(tracing::Level::DEBUG);
        let a: SocketAddr = "127.0.0.1:80".parse().unwrap();
        let b: SocketAddr = "127.0.0.2:80".parse().unwrap();
        for active in [false, true] {
            let config = LoadBalancingConfig::default();
            let mut state = Group::new(config.clone(), "test".into());
            state
                .members
                .insert(a, EndpointHealth::new(1, Instant::now(), &config));
            state.active = active;
            // The active case rejects the lookup's stale generation instead.
            state.generation = u64::from(active);
            let group = Arc::new(Mutex::new(state));
            let target = DnsTarget::with_resolver("http://test:80".parse().unwrap(), move |_| {
                async move { Ok(vec![b]) }.boxed()
            });
            let discover = DnsDiscover::new(target, config, Arc::clone(&group), |_, _, _| ());
            tokio::task::yield_now().await;
            assert!(group.lock().unwrap().members.contains_key(&a));
            assert!(!group.lock().unwrap().members.contains_key(&b));
            drop(discover);
        }
        let events = logs.events("Dynamic balancer DNS refresh.");
        assert_eq!(events.len(), 2);
        for event in events {
            assert_eq!(event["fields"]["status"], "success");
            assert_eq!(event["fields"]["member_count"], 1);
            assert_eq!(event["fields"]["added_ips"], "[]");
            assert_eq!(event["fields"]["removed_ips"], "[]");
        }
        assert!(logs.events("Dynamic balancer health changed.").is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_discovery_stops_refreshes() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&calls);
        let target = DnsTarget::with_resolver("http://test:80".parse().unwrap(), move |_| {
            count.fetch_add(1, Ordering::Relaxed);
            async { Ok(vec!["127.0.0.1:80".parse().unwrap()]) }.boxed()
        });
        let config = LoadBalancingConfig::default();
        let group = Arc::new(Mutex::new(Group::new(config.clone(), "test-drop".into())));
        let mut discover = DnsDiscover::new(target, config, group, |_, _, _| ());
        discover.next().await.unwrap().unwrap();
        let before = calls.load(Ordering::Relaxed);
        drop(discover);
        tokio::time::advance(Duration::from_secs(60)).await;
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::Relaxed), before);
    }
}
