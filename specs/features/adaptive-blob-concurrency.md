---
service: vector
created: 2026-10-31
---

# Add bounded adaptive blob-ingestion concurrency

## Intent

Allow blob-ingestion sources to adjust useful concurrency to available capacity
without manually choosing one fixed worker count for every workload. Preserve the
existing source behavior until the new mode is explicitly enabled.

## Context

AWS S3/SQS, Azure Blob/Storage Queue, and GCP GCS/Pub/Sub sources currently use fixed
worker concurrency. Vector already implements adaptive request concurrency for
sinks. Blob processing must account for downstream acknowledgement, variable object
sizes, cloud-message visibility leases, and memory shared by all tables in a pod.

## Requirements

- R1: Adaptive blob concurrency is opt-in and defaults off.
- R2: Reuse existing adaptive-concurrency mechanisms where applicable.
- R3: Reserve per-source and shared capacity before acquiring visibility leases.
  Use bounded short polls and release empty/error reservations before backoff.
  Account for processing through downstream acknowledgement.
- R4: Apply explicit concurrency bounds and pod-wide memory/backpressure protection
  rather than permitting independent table controllers to exhaust pod memory.
- R5: Shutdown, cancellation, failures, and empty receives cannot leak admission capacity.
- R6: Expose enough bounded-cardinality telemetry to explain admission and adaptation.
- R7: Validate fixed-2, fixed-4, and adaptive modes on equivalent workloads with fixed
  replicas before considering any production rollout.

## Clarifications

- Q1: Does approval authorize production changes?
  A1: No. Implementation, local verification, and scoped dev validation only.
- Q2: Is a globally optimal concurrency value already known?
  A2: No. Bounds and canary measurements are required; CPU scaling is not assumed linear.

## Acceptance Criteria

- AC1: GIVEN the feature is absent WHEN a source starts THEN its historical fixed-worker
  behavior and configuration remain unchanged.
- AC2: GIVEN pending work and healthy capacity WHEN successful processing feedback is
  collected THEN adaptive concurrency can increase without exceeding its hard bound.
- AC3: GIVEN slow delivery, failed processing, or memory pressure WHEN admission is
  reconsidered THEN concurrency is reduced or new admission is blocked safely.
- AC4: GIVEN multiple enabled sources WHEN they compete for memory/admission THEN their
  aggregate work respects the process-wide protection.
- AC5: GIVEN empty receives, cancelled work, or shutdown WHEN workers terminate THEN
  permits are released and shutdown does not deadlock.
- AC6: GIVEN mixed object sizes and downstream acknowledgements WHEN work completes THEN
  feedback measures processing/delivery rather than queue receive latency alone.
- AC7: GIVEN an identical dev backlog and fixed replica count WHEN the three modes run
  THEN recorded throughput, queue age, CPU, memory, and redelivery evidence permits a
  comparison without claiming an unmeasured improvement.

## Constraints

- Do not alter existing dirty checkouts or unrelated source changes.
- Do not introduce new external dependencies or access bare API tokens.
- Keep cloud-message failure/redelivery and downstream-delivery semantics unchanged.
- Keep the adaptive worker controller distinct from replica autoscaling.

## Out of Scope

- Production rollout, production flag flips, or changes to HPA policy.
- Tuning all production regions from a single observation window.
- Replacing Vector's sink ARC algorithm or changing sink defaults.
- Writing or deleting production queue messages during tests.

## Plan

### Approach

Inspect the existing ARC and blob-source lifecycle, select the smallest shared
admission mechanism, and connect it to opt-in source behavior with process-wide
protection. Add hermetic adaptation/lifecycle coverage before scoped dev testing.

Reuse sink ARC with independent per-source histories. `client_concurrency` bounds
worker count; require one notification per receive and end-to-end acknowledgements.
Share only active-object capacity and high/low cgroup memory watermarks across
sources. Adaptive short polls reserve capacity before receiving, with a five-second
request deadline and one-second empty-poll backoff outside the reservation. Queue
contention cannot consume delivery attempts; there is no rejected-admission lease
release path. The processing gauge excludes poll reservations. Sample cgroup usage
every 250 ms and fail closed. Do not preempt active objects or claim byte reservations.
Changes to
shared pod limits require a process restart to preserve a single hard cap.
Whole-object RTT remains size-sensitive; dev workload comparison is a promotion
gate, not an assumed benefit. Pin replicas because existing HPA capacity math is static.

### Affected Components

- `src/sources/` — shared admission and cloud-source integration.
- Existing adaptive concurrency utilities — reuse or narrowly extract reusable pieces.
- Source configuration documentation and hermetic tests.

### Data Model Changes

None.

### API Changes

Optional, default-off configuration; exact shape follows inspection of existing APIs.

### Risks

Unfairness between tables, blob-size-driven feedback bias, leases acquired before
admission, permits retained across shutdown, and compounded HPA/controller reactions.

## Tasks

### Phase 1 — Implementation

- [x] T1: Select and document shared-controller/configuration boundaries.
- [x] T2: Implement bounded opt-in admission and process-wide pressure protection.
- [x] T3: Integrate cloud-source lifecycle and adaptation telemetry.

### Phase 2 — Verification

- [x] T4: Add and run default-off, adaptation, pressure, failure, and shutdown tests.
- [x] T5: Run focused and broader feature-gated compilation/lint checks.
- [ ] T6: Run the fixed-2/fixed-4/adaptive dev comparison if prerequisites are available.
- [x] T-val: Run acceptance criteria verification and record remaining validation gaps.

## Validation Record

- Focused all-cloud test run: 54 adaptive tests passed, including shared ARC tests,
  default-off configuration, memory pressure, delivery feedback, capacity waits,
  poll deadlines, cancellation, and stalled drains.
- Broader feature-unified library suite: 750 passed, zero failed, one ignored.
  Explicit `serde_json/preserve_order` supplies the insertion-order behavior
  expected by an existing JSON enrichment test; no test expectations were changed.
- Feature-gated root library/test Clippy completed successfully with `--no-deps`
  and the repository's `--cap-lints warn` convention. All nine diagnostic entries
  matched the committed grandfathered baseline; the baseline file is unchanged.
- Compile-only tests with `aws-s3-integration-tests` passed. A cloud-source-free
  library check with `codecs-arrow,sinks-gcp` also passed. Formatting passed.
  External-service integration tests were not run.
- Full Code Review Squad review identified delivery-attempt churn under shared
  contention, a feedback synchronization race, an unbounded Pub/Sub lease-release
  request, and configuration tests that could pass for the wrong error. Follow-up
  changes reserve capacity before receiving, remove the lease-release path,
  serialize feedback, and assert specific validation errors. Targeted follow-up
  operations, language, and test reviews also prompted stalled-drain isolation and
  stronger concurrency/test controls. The final drain assertion was corrected to
  release the sibling reservation before testing the source-local permit; its
  targeted regression passed after that correction. No review findings remain open.
- Live dev inspection located three running LJP pods in namespace
  `vector-aggregator`, context `dev-aws-us-west-2-0`. Its HPA remains min 3 / max 15.
  No image, deployment, HPA, queue, or production changes have been made.
- AC7 remains unverified. The fixed-2/fixed-4/adaptive comparison needs an isolated
  queue/backlog, a custom image, and fixed replicas. The shared dev HPA has not
  been modified to approximate an isolated canary.
- AC1–AC6 have hermetic coverage. This does not replace live-cloud validation or
  demonstrate an optimal concurrency setting or throughput improvement.
