# Adaptive blob-source concurrency (experimental, default off)

For the rationale, control loop, tradeoffs, and rollout plan, see the
[mini design doc](adaptive-blob-concurrency-design.md).

The [reusable source-controller lifecycle](source-adaptive-concurrency.md) is
reviewed separately in https://github.com/databricks-eng/vector/pull/843.
The dependent https://github.com/databricks-eng/vector/pull/841 enables it for
blob sources and adds the admission, memory, and cloud-specific policies below.

AWS S3/SQS, Azure Blob/Storage Queue, and GCS/Pub/Sub support an optional
`adaptive_concurrency` block inside their `sqs`, `queue`, and `pubsub` configuration.
Omitting it preserves the existing fixed-worker behavior.

Example queue settings for a fixed-replica development canary:

```yaml
client_concurrency: 4
max_number_of_messages: 1
adaptive_concurrency:
  initial_concurrency: 2
  max_active_objects: 8
  memory_high_watermark_bytes: 6442450944
  memory_low_watermark_bytes: 4294967296
  memory_usage_path: /sys/fs/cgroup/memory.current
```

These are example bounds, not measured production recommendations. Choose
watermarks below the container's memory limit and leave headroom for transforms,
buffers, sinks, and one already-admitted large object. End-to-end acknowledgements
must be enabled. `client_concurrency` remains the per-source hard ceiling.

Each source reuses Vector's sink adaptive-concurrency controller with its own
latency history. It acquires a source permit before polling and holds it through
processing, downstream acknowledgement, and notification deletion/acknowledgement.
Empty polls, invalid notifications, missing objects, and cancelled requests do not
train the latency estimator. Explicit pipeline/acknowledgement failures clamp the
source to one, even if the cloud configuration deletes failed notifications.
Whole-notification processing latency includes object size and can reduce
concurrency for large healthy objects; this remains a canary acceptance concern.

All adaptive sources in the process share one active-object cap and memory
watermarks; inconsistent settings fail configuration. In this version, changing
these shared limits requires a process restart; a reload may reuse identical limits
but cannot create a second independent guard while the old topology is alive.
Adaptive sources reserve shared capacity before receiving a notification. Shared
contention therefore waits without consuming cloud delivery attempts. Polls reserve
one slot; the active-object metric counts only notifications being processed.
After a receive, the reservation stays held through processing and acknowledgement.
Disabled-table draining releases shared capacity before deleting notifications,
while retaining the source-local permit.
Memory pressure arising during receipt blocks later admission; it does not reject
work already reserved. One notification can contain multiple sequential objects.

AWS uses `WaitTimeSeconds=0`, Pub/Sub uses `returnImmediately=true`, and Azure uses
its existing short-poll API. The entire adaptive receive, including its response
body, is bounded to five seconds. Empty polls and failed receives release capacity
before backoff; empty adaptive polls wait one second. Fixed-mode polling is unchanged.
Pub/Sub's `returnImmediately` option is supported but deprecated for performance
reasons; short polls can return empty despite backlog. Measure API cost and throughput
before promotion. Fair semaphore admission prevents later polls overtaking queued
sources, but many idle sources can still increase admission latency. Network failures
and cancelled receives can still cause ordinary cloud redelivery.

Cgroup memory usage is sampled at most every 250 ms. At the high watermark,
new polling waits and concurrency is clamped to one. Polling resumes below the
low watermark. Unreadable or invalid samples fail closed; an unreadable initial
sample fails configuration. For cgroup v1 configure the appropriate
`memory.usage_in_bytes` path. This is admission protection, not a byte reservation
or an OOM guarantee: active objects are not preempted.

Observe these per-component metrics:

- `object_storage_client_concurrency`: the configured `client_concurrency` ceiling, reported
  even when adaptive is on. It does not follow the adaptive limit; use
  `object_storage_adaptive_concurrency_limit` for that. Refreshed on a timer while the source
  runs, including when idle.
- `object_storage_adaptive_concurrency_limit`: actual per-source limit.
- `object_storage_adaptive_active_objects`: shared process active count as
  observed by this source; use a maximum, not a sum across sources.
- `object_storage_adaptive_memory_pressure`: whether memory admission is blocked.
- `object_storage_adaptive_processing_seconds` and
  `object_storage_adaptive_decoded_bytes`: successfully processed notifications.

## Development acceptance

Pin replicas; compare fixed 2, fixed 4, and adaptive (initial 2, ceiling 4, pressure
floor 1) using equivalent isolated backlogs, representative alternating small/large
objects, and downstream acknowledgements. Measure throughput, oldest-message age,
CPU, RSS/cgroup usage,
redeliveries, poll timeouts, admission latency, API request rate, and actual adaptive
limits. Include an idle queue and a deliberately slow downstream consumer. Reject
promotion if adaptive is persistently worse than fixed 2 or oscillates.

Do not enable this against Lumberjack Prime's current production queue HPA:
its capacity denominator assumes static `clientConcurrency`. Production adoption
requires an HPA design using actual adaptive capacity and separate validation of
the two control loops. No production HPA or configuration is changed here.

## Local validation

The reduced feature set includes existing GCP test helpers, an Arrow serializer,
and JSON insertion-order support needed by the broader library tests.

```sh
adaptive_blob_test_features=sources-aws_s3,sources-azure_blob,sources-gcp_gcs,sources-http_client,sources-demo_logs,sinks-gcp,codecs-arrow,serde_json/preserve_order
cargo test --offline --locked --no-default-features --features "$adaptive_blob_test_features" --lib adaptive
cargo test --offline --locked --no-default-features --features "$adaptive_blob_test_features" --lib
```

These tests use in-process cloud API mocks and real source processing/downstream
acknowledgement code. They do not establish live-cloud throughput or validate the
production replica-autoscaling interaction.
