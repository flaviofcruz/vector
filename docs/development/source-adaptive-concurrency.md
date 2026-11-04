# Source-side adaptive concurrency

This preparatory change adapts Vector's existing request-concurrency controller
to source lifecycles. It is new integration code, not an upstream sync. The sink
algorithm and production behavior are unchanged.

The adapter compiles only in tests until a source integration enables its runtime
feature gates. It has no cloud dependencies, queue configuration, shared memory
guard, or object-storage metrics.

## Lifecycle

- Acquire a permit before polling; only start RTT accounting when processing begins.
- Complete successful work after downstream acknowledgement. Empty receives and
  cancelled or unprocessed work release capacity without training successful RTT.
- Clamp to one on explicit delivery failure, including before the first RTT sample.
- Serialize failure feedback with successful completion. A success overlapping a
  failure must not undo the failure clamp.
- Keep each source's latency history independent and respect its configured ceiling.

The follow-up blob-source integration owns polling deadlines, shared process-wide
admission, memory pressure, cloud acknowledgement policy, and operational metrics.

## Validation

The source adapter tests cover cancellation, bounded growth and recovery, variable
processing times, failure clamping, and interleaved failure/success feedback.
Run them with the existing sink ARC regression suite:

```sh
cargo test --offline --locked --no-default-features \
  --features sources-http_client,sources-demo_logs,sinks-gcp,codecs-arrow,serde_json/preserve_order \
  --lib sinks::util::adaptive_concurrency
```

This command does not enable AWS S3, Azure Blob, or GCS source features.

## Upstream candidate

The source permit lifecycle and cancellation-neutral feedback can apply to other
acknowledged sources. An upstream proposal should agree on module placement,
source-facing API and metrics, and failure-clamping semantics. No upstream
submission or acceptance is implied by this preparatory change.
