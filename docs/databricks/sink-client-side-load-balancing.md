# DNS client-side balancing for Vector and Kafka Producer Proxy sinks

Both sinks retain their original single-address client and middleware when
`load_balancing` is omitted or `enabled = false`. Enabling it discovers pod
IPv4 addresses through the system resolver (including DBNS-backed headless
service names). `address` still supplies the scheme and port, and is the
optional fallback. TLS requires `tls.server_name` set to the DNS identity on the
server certificate; the enabled sink rejects TLS configs that omit it, since
pod IPs generally do not appear in the certificate. Certificate-chain and hostname
verification remain enabled. Direct pod connections are required when using
this TLS override; explicitly disable the sink proxy in that case.

```toml
[sinks.downstream]
type = "vector" # also supports "kafka_producer_proxy"
inputs = ["upstream"]
address = "https://relay.namespace.svc:6000"

[sinks.downstream.tls]
enabled = true
server_name = "relay.example.internal"

[sinks.downstream.load_balancing]
enabled = true
# dns_name = "relay-headless.namespace.svc"
refresh_interval_secs = 5
```

| Key under `load_balancing` | Default | Meaning |
| --- | --- | --- |
| `enabled` | `false` | Selects the new client and middleware path. |
| `per_pod_concurrency` | unset: inherit `request.concurrency` | Optional positive fixed cap per pod (also fallback); mutually exclusive with `per_pod_adaptive_concurrency`. |
| `per_pod_adaptive_concurrency` | unset: inherit `request.adaptive_concurrency` | Optional adaptive settings, with the same fields/defaults as `request.adaptive_concurrency`; selects adaptive per-pod concurrency even if the sink-wide limit is fixed. Initial and maximum limits must be positive, initial must not exceed maximum, ratios must be finite and strictly between 0 and 1, and RTT deviation scale must be finite and nonnegative. |
| `dns_name` | host from `address` | DNS name whose A records identify pods. |
| `refresh_interval_secs` | `5` | Refresh period and lookup deadline in seconds. |
| `error_ewma_secs` | `30` | Error-history decay time constant in seconds. |
| `max_penalty` | `50.0` | Maximum effective-load multiplier. |
| `slow_start_secs` | `60` | Cold-pod penalty decay time constant; `0` disables. |
| `slow_start_penalty` | `10.0` | Initial cold-pod load multiplier. |
| `ejection_enabled` | `false` | Enables capped hard ejection and backoff probes; default behavior only degrades selection. |
| `ejection_threshold` | `0.98` | Error rate eligible for hard ejection. |
| `ejection_duration_secs` | `30` | How long that failure rate must persist. |
| `max_ejection_percent` | `30` | Maximum fraction ejected, rounded down. `0` disables. |
| `probe_backoff_secs` | `5` | Idle selection interval before a pod with live error rate at least 0.5 gets a trickle opportunity; also the initial ejection probe delay. |
| `max_probe_backoff_secs` | `60` | Maximum exponentially increasing probe delay. |
| `fallback_to_address` | `true` | Uses `address` if no members can serve, with backoff probes still enabled. |

Every successful nonempty lookup is deduplicated and reconciled by socket
address. Empty answers, lookup errors, and lookup timeouts keep the last-good
set. Snapshot and endpoint generations protect against stale updates and
late request completions. Health failures never insert or remove members.

Requests traverse RateLimit -> sink-wide AdaptiveConcurrencyLimit -> Retry ->
Buffer -> P2C Balance -> per-pod AdaptiveConcurrencyLimit -> health tracking ->
Timeout -> gRPC client.
KPP keeps one TopicBlacklist layer inside Retry and above Buffer/Balance,
so the blacklist applies to the whole destination, including fallback.
The sink-wide limiter uses `request.concurrency` / `request.adaptive_concurrency`,
exactly as in the single-address path. It holds its permit for the entire retry
future, including every attempt, buffer wait and backoff. Fast retriable errors
therefore cannot admit an unbounded number of retained batches. Cancellation
releases the permit and the endpoint's active request count, so it cannot leave
an idle pod with a permanently inflated selection load. The bound also covers static-address fallback and DNS
membership changes.

The buffer bound remains `max(c, 32)`, using the **sink-wide** fixed concurrency
or adaptive maximum and capped at Tokio's maximum semaphore capacity. It does
not grow with pod count, per-pod overrides or the retry budget. A freed queue
slot is not an admission permit: the outer limiter bounds all admitted batches,
including those asleep in Retry. Driver input prefetch and upstream buffers are
separate from this admission bound.

By default each pod inherits `request.concurrency` and
`request.adaptive_concurrency` but learns its own ARC limit. Optional
`per_pod_concurrency` or `per_pod_adaptive_concurrency` overrides only cap
individual pods; they never raise the sink-wide total. Fallback uses the same
per-pod settings under the same outer limit. For example, a sink-wide fixed
limit of 20 with `per_pod_concurrency = 3` admits at most 20 batches in total and
sends at most three concurrent attempts to any one pod. A per-pod adaptive
block uses the same shape as `request.adaptive_concurrency`:

```toml
[sinks.downstream.request]
concurrency = 20

[sinks.downstream.load_balancing.per_pod_adaptive_concurrency]
initial_concurrency = 2
max_concurrency_limit = 5
```

P2C compares `(in_flight / limit + idle_floor) * penalty`, using the current
per-pod ARC limit. The common `idle_floor` is `1 / c`, where `c` is the
per-pod fixed concurrency or adaptive maximum. This keeps health visible
at zero in-flight requests without starving recovered or new idle pods whose
learned limits differ. The smooth penalty is `1 + (max_penalty - 1) * error_rate²`,
combined with the exponentially decaying cold-pod penalty and capped at `max_penalty`.
Health remains visible even when both a bad pod and a healthy pod have zero
in-flight requests. The error rate uses time-decayed error and sample weights
`E` and `S`, starting with one healthy sample (`E = 0`, `S = 1`). With elapsed
time `t`, `d = exp(-t / error_ewma_secs)`, and healthy prior weight `w0 = 0.1`,
idle observations and recorded outcomes use the same history:

```text
Ed = E * d
Sd = S * d + w0 * (1 - d)
error_rate = Ed / Sd
c = min(1, 199 / Sd)
E_next = c * Ed + failed
S_next = c * Sd + 1
```

Here `failed` is `1` for a classified failure and `0` for success. Capping both
weights together bounds the history at 200 samples without changing its rate.
Every outcome contributes exactly one sample. The healthy prior makes idle
healing continuous while preserving sustained failures: with default settings,
a pod failing once every five seconds stays above the `0.98` ejection threshold
even between requests. The prior is one tenth of a sample so an isolated failure
after arbitrarily long idle has rate at most `1 / 1.1`, below that threshold.
A success can only lower the current rate, including after an idle gap.
Successes and idle time lower the penalty gradually, without resetting health
history or snapping small penalties to the healthy baseline.
Transport failures, UNAVAILABLE, DEADLINE_EXCEEDED and request timeouts
count as failures. RESOURCE_EXHAUSTED affects ARC without penalizing health;
other application status errors do not mark the pod unhealthy.

With default `ejection_enabled = false`, health only changes selection, and no
pod is hard-ejected or put through the ejection probe lifecycle. To prevent P2C
from permanently starving the worst idle pod, a pod whose current idle-decayed
error rate is at least 0.5 and which has not been selected for
`probe_backoff_secs` gets a trickle opportunity. A continuously failing pod
keeps receiving these opportunities indefinitely with the default settings;
its rate remains above 0.5 throughout each backoff interval. Once its rate falls
below 0.5, it returns to ordinary P2C selection even if a small penalty remains. The oldest
overdue eligible pod wins the next admitted request, even if healthy competitors
are idle. Each
trickle runs one request at a time, consumes an ordinary outer permit, and
trains health with its outcome. Pending requests and downstream capacity still
apply; no synthetic traffic is generated when there are no input batches.
Cancellation releases trickle eligibility without counting an endpoint failure.
Non-ejected degraded pods retain this guarantee when hard ejection is enabled,
including pods excluded by the ejection cap. Finding the oldest eligible pod
scans DNS membership under the group lock on each member readiness poll;
readiness work grows with the membership size.

With `ejection_enabled = true`, a rate at or above `ejection_threshold` sustained
for `ejection_duration_secs` can make a pod temporarily unready, without
removing it from DNS membership. Occasional successes do not reset the timer
while the EWMA stays above threshold. A below-threshold rate clears it,
including when idle decay takes it below threshold before resumed traffic.
Ejection obeys the group cap, including after a DNS shrink (fewer than four pods
cannot be ejected with the default 30% cap). Once backoff expires, one live
request probes the pod; failures increase backoff up to
`max_probe_backoff_secs`. Probes continue indefinitely at that capped interval
while the pod remains in DNS and live traffic is available. A successful probe
keeps the decaying error history and restarts slow start at recovery time; it
does not immediately restore a full healthy traffic share. RESOURCE_EXHAUSTED
also ends ejection and restarts slow start, without adding a health sample;
backpressure is an observed probe response rather than caller cancellation.
Probe cancellation and late results retain the existing membership-generation and ejection-epoch
checks. Probes receive selection priority and can run while fallback is enabled.
Retry resubmits through the balancer and may select a different pod. As with
existing retries, downstream operations must tolerate duplicate delivery after
ambiguous failures.

The Vector sink healthcheck concurrently checks discovered pods and passes
when at least one reports SERVING. A lookup with no usable result checks the
static address when fallback is enabled. KPP retains its existing no-op
startup healthcheck.

Metrics use the sink's component labels plus `target` (discovery URI):

| Metric | Additional labels | Meaning |
| --- | --- | --- |
| `sink_lb_dns_refresh_total` | `status=success\|empty\|error` | Lookup outcomes. |
| `sink_lb_members` | | Last-good DNS membership size. |
| `sink_lb_health_score` | `pod=IP:port` | Decayed health in `[0,1]`, where 1 is healthy. |
| `sink_lb_degraded_members` | | Members with decayed error rate at least 0.5. |
| `sink_lb_ejected` | `pod` | Whether a member is temporarily ejected. |
| `sink_lb_ejections_total` | `pod` | New hard ejections. |
| `sink_lb_trickle_total` | `pod` | Ordinary requests dispatched as recovery trickles. |
| `sink_lb_fallback_total` | | Attempts dispatched to the static address. |

Completions refresh only the affected pod's gauges and the cached degraded
count. DNS ticks refresh all pods and recompute that count, including on failed
lookups; idle health decay is reflected at the next tick. Removed members and
dropped discovery streams reset their gauges to zero and stop updating them.
Their per-pod series (including `sink_lb_ejections_total`) then expire through
Vector's internal metrics expiry: 300 seconds idle by default, removed on the
next metrics capture after expiry. Configure the top-level `expire_metrics_secs`
or `expire_metrics_per_metric_set` to change that interval. A negative global
expiry disables cleanup; keep a finite expiry for these series to bound
cardinality during pod churn. Choose an interval longer than DNS refresh and
metrics scrape intervals.

## Debugging

DEBUG is the safe level for routine balancer debugging in both sinks:

```sh
VECTOR_LOG='info,vector::sinks::util::dynamic_balance=debug' \
vector --config /path/to/vector.toml
```

Use `vector::sinks::util::dynamic_balance=trace` in the same filter to also
log each selected pod request and degraded/healthy flips. Those flips can occur
per request when error rates hover around 0.5. Use the parent prefix
`vector::sinks::util::dynamic_balance` for health logs: their level checks run
in `::health`, while emission runs in `::diagnostics`. The prefix also includes
`::discovery`; request and fallback events originate in the parent module.
TRACE takes the group and ARC controller locks per selected request, adding
overhead. All new events are DEBUG or TRACE.

For a short diagnostic capture, `VECTOR_INTERNAL_LOG_RATE_LIMIT=0` disables
console suppression globally. This can produce substantial output, especially
at TRACE; keep the default limiter for normal operation.

| Event | Level and cadence | Fields |
| --- | --- | --- |
| `Dynamic balancer DNS refresh.` | DEBUG, once per completed refresh attempt, including errors and timeouts. | `target`, `refresh_generation` (all attempts), `status` (`success`, `empty`, `error`, `timeout`), `raw_record_count` (before IPv4 filtering/deduplication; absent on failure), `dedup_ipv4_count`, `member_count` after reconciliation, `added_ips`, `removed_ips`, `retained_last_good`, `lookup_duration`, `error`. Diffs are computed before membership changes; retained answers have empty diffs. |
| `Dynamic balancer pod snapshot.` | DEBUG, once per refresh, containing **all** members in one event. | `target`, `refresh_generation`, `pods`. Each pod has `addr`, membership `generation`, ejection `epoch`, `error_rate`, `health_multiplier`, `slow_start_multiplier`, `capped_penalty`, selection `penalty`, `current_limit`, `in_flight`, `normalized_load`, `idle_floor`, `effective_load`, `ejected`, `probe_ready`, `probe_in_flight`. |
| `Dynamic balancer health changed.` | TRACE for degraded/healthy flips (potentially per request); DEBUG for all other transitions. | `target`, `pod`, membership `generation`, ejection `epoch`, `prev_state`, `new_state`, `reason`, `error_rate`, `backoff`, `ejected` count and `ejection_budget`. TRACE reasons: `request_outcome` or `idle_decay` for flips at error rate 0.5, matching the unchanged metric threshold. DEBUG reasons: `sustained_failures` for ejection; `backoff_elapsed` for probe readiness; `probe_start`, `probe_failure`, `probe_cancel`; `probe_success` for success/recovery; `cap_enforcement` for recovery when DNS shrink reduces the budget; `dns_removal`; `trickle_dispatch` for an ordinary recovery trickle (state may stay degraded). Ejection/probe state takes precedence over degradation labels, and a probe outcome emits one combined transition. |
| `Dynamic balancer fallback changed.` | DEBUG, entering or leaving fallback as routing observes the group. | `target`, `fallback`, `reason` (`no_members`, `all_members_ejected`, `probe_ready`, `members_available`), `members`, `ejected`, `ready_probes`. |
| `Dynamic balancer request selected.` | TRACE, once per selected pod request inside `Member::call`. | `target`, `pod` with the same load components and state as a pod snapshot. P2C load comparisons do not log. |

Snapshots use `normalized_load = in_flight / current_limit` and
`effective_load = (normalized_load + idle_floor) * penalty`. A due probe's
selection `penalty` is zero; `capped_penalty` still reports its health/slow-start
penalty. Health state and cancellation-safe active request counts are copied under the
group lock; ARC limits are copied under the controller lock;
formatting and emission happen after unlocking. Newly discovered pods report
the configured initial limit and zero in-flight requests until their ARC service
is created. DEBUG snapshot construction and TRACE request snapshots are guarded
by `tracing::enabled!`.

Focused validation (the usual sibling VRL checkout is required):

```sh
export DATABRICKS_VECTOR_REDACTOR_LIB_DIR="$PWD/lib/vector-redaction-executor/tests/fixtures/linux-x86_64"
cargo fmt --check
cargo test --locked --lib --features sinks-kafka_producer_proxy,databricks-redaction sinks::util::dynamic_balance
cargo test --locked --lib --features sinks-kafka_producer_proxy,databricks-redaction sinks::vector
cargo test --locked --lib --features sinks-kafka_producer_proxy,databricks-redaction sinks::kafka_producer_proxy
cargo test --locked --lib --features sinks-kafka_producer_proxy,databricks-redaction sinks::util::adaptive_concurrency
cargo test --locked --lib --features sinks-kafka_producer_proxy,databricks-redaction sinks::util::dns
cargo test --locked --lib --features sinks-kafka_producer_proxy,databricks-redaction sinks::clickhouse::headless
scripts/clippy-grandfathered-exceptions.sh
git diff --exit-code -- .github/clippy-grandfathered-exceptions.txt
scripts/ci-local.sh
```
