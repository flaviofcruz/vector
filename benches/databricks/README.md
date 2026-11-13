# Databricks bench

Checks the correctness of a Databricks Vector pipeline, for any implementation of its source,
transform or sink. A candidate is a Vector binary plus a config file. One candidate is the
baseline, assumed correct; every other candidate is compared back to it.

The harness is generic: the core taps, counts, cuts, stores, paces, sends and logs *items* without
reading inside them. Everything that knows a protocol sits behind an adapter:

| Adapter | Where | Knows |
| --- | --- | --- |
| `SourceCodec` (`source_codec.rs`) | source side | how a tapped event becomes an item, how items become requests, how responses are classified |
| `SinkKind` (`sink.rs`; kinds in `s3_ndjson.rs` and `zerobus_flight.rs`) | sink side: `verify` and `check` | how a fake's captured output reads back as rows, with their destinations; which fake to start for a candidate and which environment points the candidate's config at it |

The first pipeline is Hydra, the metrics path: Prometheus remote write in, S3 or Zerobus out,
through `metric_to_log_hydra`. Its source codec is `prom_remote_write` (`prom_remote_write.rs`).

`check` runs the whole correctness flow in one command. The building blocks it drives are
`capture` (records real traffic from a pod into a corpus), the fake sink endpoints (record what a
sink writes), `gen` (replays a corpus into targets) and `verify` (compares the outputs).

## Build

The fork needs `../vrl` (`databricks-eng/vrl`, branch `v0.30.0-custom`) next to it. With the
universe toolchain:

```bash
export UNIVERSE_ROOT=~/universe PATH=~/universe/bin/rust/db-toolchain/bin:$PATH
# Universe pins a newer rustc than the fork (1.92); its new lints trip `deny(warnings)` in vrl.
export RUSTFLAGS=--cap-lints=warn
# codecs-arrow: lib/codecs does not compile without it in slim feature sets.
export FEATURES=databricks-benches,codecs-arrow,sources-prometheus-remote-write,transforms-metric_to_log_hydra,sinks-aws_s3,sinks-databricks-zerobus,unix

# Shorthand for the harness; everything after `--` is a harness subcommand.
hb() { cargo bench --profile dev --bench databricks --no-default-features --features $FEATURES -- "$@"; }
hb --help
cargo test --test databricks_bench --no-default-features --features $FEATURES   # harness unit tests
```

Run `hb` from the repository root: `cargo bench` starts the harness there, so relative paths
resolve against it. Drop `--profile dev` (the bench profile is release) for performance work.

## check: the correctness run

**`check` is THE way to run a correctness check.** The other subcommands are its building blocks;
you rarely run them by hand.

```bash
hb check --corpus corpus.ndjson --sink s3 --sink zerobus \
  --schema <catalog.schema.table>=table.json \
  --baseline a=<vector-bin>:<config> --candidate b=<vector-bin>:<config> \
  --work-dir /tmp/check-run
```

**Build profiles: real runs need optimized builds of both sides.** Run the bench with plain
`cargo bench` (no `--profile dev`), and build each candidate with `cargo build --release`. Dev
builds are only for playing around: they are slow enough to distort timing and drain. The `hb`
shorthand above is the dev profile.

What it does, in order:

1. Checks the flags and the files they name, then creates `--work-dir` (it must be missing or
   empty; `check` refuses otherwise, names it, and never deletes anything).
2. Starts the fakes in its own process, on OS-assigned `127.0.0.1` ports: one fake per candidate
   per `--sink`, and one shared fake-uc when a sink reads a table schema.
3. Starts each candidate as a Vector process, with a fresh data dir and a free source port, and
   waits up to 60 s for it to listen. A candidate that exits first, or never listens, fails the run
   with its exit status and the tail of its log.
4. Replays the corpus to every candidate, as `gen` does, into `<work-dir>/sent.ndjson`.
5. Stops each candidate with SIGTERM. Vector stops its sources and drains its sinks, which write
   their open batches, so no batch timeout has to elapse. A candidate that has not exited within
   60 s is killed and reported as an error, since its outputs may be incomplete. A candidate that
   exits 0 is still checked: its log must not report lost data. Vector exits 0 after it gives up
   on components, after a component fails, and after a component drops events by mistake, so the
   log is the only evidence. A drop is caught here because it would not show in verify when the
   baseline and a candidate drop the same rows (a table with no `--schema`, for example, or a batch
   every candidate's sink rejects). The lines checked for, in `process.rs`, are `Events dropped
   intentional=false`, `Failed to gracefully shut down in time`, `Overall shutdown deadline
   exceeded` and `An error occurred that Vector couldn't handle`. A filter's deliberate drops log
   `intentional=true` and are not checked. `gracefully_closed = false` is not one of them either: on
   this build's shutdown path Vector logs it on every run.
6. Stops the fakes and verifies each sink's outputs against that sink's baseline (the baseline is
   the first candidate, always).

Exit codes: `0` IDENTICAL; `1` DIFFERENT, INCONCLUSIVE, or a request that was not accepted on every
candidate; `2` an error, including a refused sent log, a candidate that had to be killed, and a
candidate whose log reports lost data (a failed shutdown or dropped events). Ctrl-C
(SIGINT) and any other error stop the candidates with SIGTERM and the fakes with the process; the
work dir is kept.

Avoid runs that cross midnight UTC: `check` warns when one does. A sink that puts a date in where
it writes (`date=%F` in the Hydra S3 `key_prefix`) renders it from the event's `timestamp`, or from
the clock when the event has none (Hydra's S3 rows carry no `timestamp` field). Rows written either side of midnight
can then land under different dates for different candidates, and differ by destination alone.

Flags:

- `--sink KIND` (repeatable; `s3` or `zerobus`). The candidate's config must use exactly these
  sinks: a variable the config reads that `check` did not set fails the config load.
- `--baseline NAME=BIN:CONFIG` and `--candidate NAME=BIN:CONFIG` (at least one candidate). The
  config is taken after the last colon, so it may not contain one. Names are letters, digits and
  underscores, and must differ.
- `--schema TABLE=PATH` (repeatable, once per table): TABLE is the full `<catalog>.<schema>.<table>`
  the config writes, and PATH the table schema saved by `fetch-schema`. The shared fake-uc serves
  each table its own file, and answers 404 for a table no `--schema` names, so the sink fails and
  `check` reports it. Required when a sink reads a table schema (`zerobus`), and refused otherwise.
  A repeated TABLE, a spec that is not `TABLE=PATH`, and a missing file are refused when the
  arguments are parsed, before anything starts.
- `--iterations`, `--items-per-sec`, `--max-concurrency`: as for `gen`.
- `--work-dir DIR`: everything the run writes goes under it.

### Fan-out: several tables and S3 prefixes

A candidate's config may write several Zerobus tables and several S3 key prefixes. The harness sets
one `DATABRICKS_BENCH_ZEROBUS_TABLE` for all of them, so a fan-out config writes each Zerobus sink's
`table_name` itself, and gives every table its own `--schema TABLE=PATH`. Each `--sink` kind gets
one fake per candidate, which every sink of that kind in the config writes to:

- `fake-zerobus` writes each table's frames to `<zerobus-out>/<last segment of the table>/`. Two
  tables whose names differ only by catalog or schema have the same last segment, so their rows land
  in one directory and `verify` cannot tell them apart. Give fan-out tables distinct last segments.
- `fake-s3` writes each object to `<s3-out>/<bucket>/<key>`, so sinks with different key
  prefixes land in different directories, and a sink with the same prefix as another would merge.

A table or prefix the candidate writes and the baseline does not shows as a destination `verify`
reports as extra, so the verdict is DIFFERENT.

What lands under `--work-dir`, and why:

- `sent.ndjson`: the sent log `gen` wrote. `verify` reads it, and it records every request sent.
- `<name>/vector.log`: the candidate's stdout and stderr. Start here when a candidate misbehaves.
- `<name>/data/`: the candidate's Vector data dir (disk buffers, checkpoints). Fresh per run.
- `<name>/tls/`: the throwaway CA and server certificate of the fake Zerobus endpoint. The
  candidate trusts `ca.pem` through `SSL_CERT_FILE`, so its TLS path is the production one.
- `<name>/<sink>-out/`: what the candidate's sink wrote, as its fake recorded it (`s3-out`,
  `zerobus-out`). `verify` compares these.

The candidate's config reads its endpoints from environment variables that `check` sets:

| Variable | Set for | Value |
| --- | --- | --- |
| `DATABRICKS_BENCH_SOURCE_ADDR` | every candidate | `127.0.0.1:<port>`, where its source must listen |
| `DATABRICKS_BENCH_DATA_DIR` | every candidate | its fresh data dir |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_EC2_METADATA_DISABLED` | `s3` | dummy credentials; the fake does not check them |
| `DATABRICKS_BENCH_S3_ENDPOINT` | `s3` | `http://127.0.0.1:<port>` of its fake-s3 |
| `DATABRICKS_BENCH_ZEROBUS_INGEST_ENDPOINT` | `zerobus` | `https://127.0.0.1:<port>` of its fake-zerobus |
| `DATABRICKS_BENCH_ZEROBUS_UC_ENDPOINT` | `zerobus` | `http://127.0.0.1:<port>` of the shared fake-uc |
| `DATABRICKS_BENCH_ZEROBUS_TABLE` | `zerobus` | `main.databricks_bench.output`, the same for every candidate |
| `SSL_CERT_FILE` | `zerobus` | `<work-dir>/<name>/tls/ca.pem` |

The committed config, `configs/hydra_dual_write.yaml`, uses both `s3` and `zerobus` and reads all
of these. Its S3 batch timeout is 5 s, and buffers block rather than drop. A config for another
sink set is written the same way, with the variables for the sinks it uses.

Example, with the committed config for both sides and the Hydra corpus (for a real run, replace
`hb` with `cargo bench --bench databricks --no-default-features --features $FEATURES --`, and use
`target/release/vector`):

```bash
CFG=benches/databricks/configs/hydra_dual_write.yaml
hb check --corpus ~/scratch/hb/pr-capture.v4.ndjson --sink s3 --sink zerobus \
  --schema main.databricks_bench.output=$HOME/scratch/hb/fetched/pod_prime_raw.json \
  --baseline a=target/release/vector:$CFG --candidate b=target/release/vector:$CFG \
  --work-dir /tmp/check-$(date +%s)
```

## bench: the performance run

**`bench` is THE way to run a performance comparison**, beside `check` for correctness. It runs its
candidates one at a time, so no two of them compete for the box, and it measures each candidate's
CPU and memory over the replay. With `--repeat N` it runs N rounds of every candidate, alternating
their order, and summarizes each candidate over its runs. Use `--repeat 3` or more to compare
candidates: a single run per candidate cannot tell a difference from noise.

```bash
hb_perf() { cargo bench --bench databricks --no-default-features --features $FEATURES -- "$@"; }
hb_perf bench --corpus ~/scratch/hb/pr-capture.v4.ndjson --sink s3 --sink zerobus \
  --schema main.databricks_bench.output=$HOME/scratch/hb/fetched/pod_prime_raw.json \
  --candidate a=/tmp/vector-a:benches/databricks/configs/hydra_dual_write.yaml \
  --candidate b=/tmp/vector-a:benches/databricks/configs/hydra_dual_write.yaml \
  --repeat 3 --work-dir /tmp/bench-$(date +%s)
```

**Builds.** `bench` refuses to run from a debug build, so run it with plain `cargo bench` (no
`--profile dev`). Build each candidate with `cargo build --release`, then copy it out of `target/`
(`cp target/release/vector /tmp/vector-a`) and pass the copy. The bench profile shares
`target/release/`, and every `cargo bench` re-links `target/release/vector` to its own build of
Vector (optimized, with debug info), so a candidate at that path is never your release build;
`bench` refuses it. The version line is recorded with each run. A release build's names no commit
(`vector 0.53.0-databricks (x86_64-unknown-linux-gnu)`), while a build with debug info, such as
the bench or dev profile, says `-custom-<sha>` and `debug=full`
(`vector 0.53.0-databricks-custom-8447822b1 (x86_64-unknown-linux-gnu debug=full)`). A debug
Vector tops out around 17k items/s, so a debug candidate's numbers say nothing about production.

**Isolation.** The box's physical cores are split in two, derived from its topology (sysfs
`thread_siblings_list`), never typed in: the first half of the cores, rounded down, serve the
harness, and the rest serve the candidate. Each core stays whole on one side, so the two never share
a core. A box with fewer than two cores is refused. The chosen sets are printed at startup and
recorded in `results.json`.

- The **harness** runs the load generator (`gen`) and the fakes, both in the `bench` process, as
  for `check`. Nothing is pinned: while the fence holds the candidate CPUs, the kernel keeps the
  `bench` process, like every process outside the fence, on the harness CPUs. `--gen-threads` sets
  the threads of the runtime gen and the fakes share, and defaults to the harness CPU count.
- The **candidate** runs alone on its CPUs, behind a **fence**: a cgroup-v2 cpuset partition,
  `/sys/fs/cgroup/databricks-bench`, of type `root` on the candidate CPUs. The kernel takes a
  partition's CPUs away from every other cgroup, so system services, your session and the harness
  all run on the harness CPUs while a candidate runs, and a process moved into it runs on the
  candidate CPUs only. Type `root`, not `isolated`, keeps the
  scheduler balancing the candidate's threads across its CPUs. The fence also sets `memory.max`
  from `--candidate-memory` (default `8G`) and `memory.swap.max` to 0. There is no CPU quota: the
  fence fixes the candidate's share, and quota throttling would distort latency.
- Before each candidate, `bench` checks that the fence took: it reads exactly `root` (an invalid
  partition reads `root invalid (...)`), owns exactly the candidate CPUs, and no other top-level
  cgroup can still run on them. It refuses to run otherwise. After the candidate exits, it reads
  the partition again, and a fence that is no longer exactly `root` fails the run.
- The candidate starts held at a gate (a shell waiting on its stdin) and is moved into the fence
  before the shell execs Vector, so every byte Vector uses is charged to the fence.
- **Passwordless sudo is required.** A partition must sit under the root cgroup, so making it,
  moving the candidate into it and removing it are root writes, done with `sudo -n`. The fence is
  made fresh for each candidate (which also gives each its own `memory.peak`) and removed after it,
  on errors and Ctrl-C too. It lives in the kernel only, so a reboot removes it as well. If a run
  could not remove it, `bench` prints the commands, and refuses to start while it exists:
  `echo member | sudo tee /sys/fs/cgroup/databricks-bench/cpuset.cpus.partition`, then
  `sudo rmdir /sys/fs/cgroup/databricks-bench`.
- Per-CPU kernel threads and interrupts still run on the candidate CPUs (only boot options such as
  `isolcpus` move them), and on a VM the host's other tenants share the physical cores.

**Rounds.** `--repeat N` (default 1) runs N rounds, and each round runs every candidate once: in
the order given in odd rounds and reversed in even ones (a, b, then b, a, then a, b ...), so over
each pair of rounds a drift that grows linearly with time, such as the box warming up, weighs on
every candidate alike. Every run gets fresh fakes and a fresh fence. After the per-run table,
`bench` prints each candidate's median and min-max of `ms/1k-item`, `cand-cpu`, `mem-peak` and
`anon-max` over its runs (failed runs included), and `results.json` holds the same summary.
Compare medians, and treat a difference smaller than the ranges as noise.

For each run, `bench`:

1. makes `<work-dir>/<name>/<round>/` (data dir, TLS dir, sink out dirs, logs), and reads the
   candidate's `--version` line;
2. starts the fakes, makes the fence, starts the candidate in it, and waits until the candidate
   listens (60 s, as `check`);
3. replays the corpus with `gen` to the candidate, sampling the fence about once a second for the
   candidate's anonymous memory;
4. stops the candidate with SIGTERM and times its flush, reads the fence's counters once it has
   exited (the cgroup outlives its process), checks the fence is still valid, removes it, and
   stops the fakes;
5. counts the rows each sink's fake recorded, read back as `verify` reads them, once the fakes
   have stopped, so the outputs are complete and the count is outside every timing.

**How many iterations.** One pass of the v4 corpus is 283,544 items, about 3 s uncapped, which is
too short to compare candidates. Pick the length of the replay and set `--iterations` to about
`seconds × items/s ÷ items per pass`: for 60 s, about 20 uncapped (near 90k items/s) or 2 at
`--items-per-sec 10000`.

**Which rate.** Compare candidates at a fixed `--items-per-sec`, ideally production's. On
2026-11-13 a staging AWS us-west-2 pod (`vector-aggregator-metrics`, context
`staging-aws-us-west-2-0`) received about 30k items/s on its `prom_remote_write` source, measured
from the growth of `vector_component_received_events_total` over 75 s. At `--items-per-sec 30000`
one v4 pass is about 9.5 s, so `--iterations 10` replays for about 95 s. Unset, gen sends as fast
as the candidate accepts, and a candidate can accept faster than its sinks drain: the backlog grows
in memory until the fence's `memory.max`, and the candidate may be OOM-killed (in our uncapped
20-iteration runs, peak memory reached the 8G limit and one run was killed). Use an unset rate only
to probe capacity, with a short run.

**What is measured.** The candidate from the start of the replay until it exits, so the flush of
what it accepted but had not yet written counts: with acknowledgements off and deep buffers, a
candidate can accept quickly and do the work after the last request, and stopping at the end of
the replay would favour the one that buffers more. The harness (gen and the fakes) is measured over
the replay only, where it works.

| Column | Meaning |
| --- | --- |
| `cand-cpu` | `cpu-s` per wall second of replay and flush: the candidate's busy CPUs, out of its CPU set. |
| `cpu-s` | CPU seconds the fence used from replay start until the candidate exited. |
| `ms/1k-item` | `cpu-s` per 1,000 items sent, in milliseconds. The number to compare between candidates: it does not depend on how a candidate splits its work between the replay and the flush. |
| `mem-peak` | The fence's `memory.peak` after the candidate exited: the most it used since it started. |
| `anon-max` | The highest anonymous memory sampled, about every second, until the candidate exited. |
| `oom` | `oom_kill` events in the fence from replay start until the candidate exited. |
| `items/s` | Items gen sent per second. |
| `target` | `--items-per-sec`, or `-` when unset. |
| `lag-ms` | The largest time a request was sent after its due time. It counts waiting for a free slot of `--max-concurrency`, so with a target set, large lag usually means the candidate was slow to answer. |
| `harness-cpu` | Busy CPUs of the `bench` process (gen and the fakes) over the replay, from its `getrusage`, out of the harness CPU set. |
| `accepted` | Whether every request was accepted (2xx) on the candidate. |
| `rows` | Rows every sink's fake recorded, summed; `results.json` and the lines under the table give each sink's. |
| `flush-s` | Time from SIGTERM to the candidate's exit. |

Also recorded: the fence (its cgroup, partition type, CPUs, `memory.max` and `memory.swap.max` as
read back), the `--version` line, and the binary and config paths (canonical).

**Pass rule.** A run fails when a request was not accepted, when the candidate exits non-zero after
SIGTERM or is killed, when its log reports lost data, a failed shutdown or dropped events (the
same lines `check` reads), or when the fence is no longer a valid `root` partition once the
candidate has exited (its CPUs stopped being exclusive, so the numbers cannot be trusted). With
`--items-per-sec`, it also fails when gen achieved more than 1% below the target or dispatched a
request more than 3 s after its due time: the candidate then set the rate, so its numbers are not
at the target. It also fails when a sink recorded no rows, or a different number of rows from the
first run of the invocation. A run that fails still gets its numbers. Exit `0` when every run
passed, `1` when any failed, `2` on error.

**Equivalent candidates.** `bench` assumes the candidates it compares are semantically equivalent:
given the same corpus, each sink writes the same rows for every one of them. Run `check` on them
first; that is what proves it. The row count is a cheap guard against a run that lost or
duplicated rows, not a comparison of the rows. Retried requests can legitimately deliver a row
twice, so when either run retried, the failure says so.
A candidate that never listens, a fake that cannot start or stop cleanly, or a fence that does not
take or cannot be removed is an error, not a failed run. Nothing here verifies outputs: that is `check`.

Limits:

- Linux only: cgroup v2 with the cpuset controller, plus passwordless sudo.
- The box may be too small near production rates (a staging pod receives about 30k items/s, see
  "Which rate"). The harness and the candidate each get half of this box's cores, which is not a
  pod's CPU allotment.
- The load is what `--items-per-sec` sets, so a higher rate tests the candidate at that rate. Unset,
  the run measures how fast the candidate accepts, and may end in an OOM kill (see "Which rate").
- Timing is for comparing candidates on the same box, in the same run conditions. It is not an
  absolute production number.
- Compare candidates in one invocation: the fence keeps other processes off the candidate's CPUs,
  but not the VM's neighbours off the host.

Flags, as for `check` unless noted:

- `--corpus`, `--sink` (repeatable), `--schema`, `--iterations`, `--items-per-sec`,
  `--max-concurrency`, `--work-dir` (missing or empty, never overwritten).
- `--sink-latency-ms KIND=MS` (repeatable, once per selected `--sink` kind), e.g.
  `--sink-latency-ms s3=100 --sink-latency-ms zerobus=50`. Omitted kinds use zero. One fake
  endpoint per kind gets that delay, identical for every candidate and round, not per table or
  pod. S3 delays each PUT before storage/response (`fake-s3 --latency-ms`); Zerobus delays each
  batch acknowledgement (`fake-zerobus --ack-delay-ms`). This does not simulate bandwidth, TLS
  or network behaviour. Malformed, negative/non-integer, unknown, duplicate and unselected kinds
  are refused before anything starts. `check` stays zero-delay and has no such flag.
- `--candidate NAME=BIN:CONFIG` (repeatable, at least one; names as for `check`).
- `--repeat N` (default 1, at least 1): rounds of every candidate, alternating their order. Use
  at least 3 to compare candidates.
- `--candidate-memory LIMIT` (default `8G`): the fence's `memory.max`, in bytes or with a K, M or G
  suffix.
- `--gen-threads N` (default: the harness CPU count, at least 1).

What lands under `--work-dir`:

- `results.json`: the settings (including `sink_latencies_ms` for every selected kind, even zero),
  the harness and candidate CPU sets, each run's candidate, round
  (from 1) and numbers (with the rows each sink recorded and gen's retried attempts), binary,
  config, `--version` line and failures, and each candidate's summary over its runs.
- `<name>/<round>/vector.log`: the candidate's stdout and stderr in that round. Start here when a
  run fails.
- `<name>/<round>/sent.ndjson`: the sent log `gen` wrote for that run.
- `<name>/<round>/data/`, `<name>/<round>/tls/`, `<name>/<round>/<sink>-out/`: as for `check`,
  for that run.

Ctrl-C (SIGINT) and any error stop the candidate with SIGTERM, remove the fence, stop the fakes,
and keep the work dir.

## fetch-schema: save a table's schema for the fakes (step 0)

`fake-uc` answers the sink's Unity Catalog lookups with a table schema. Rather than keep a copy in
the repo, fetch the one the workspace reports, with the databricks CLI:

```bash
hb fetch-schema --profile <profile> --table <catalog.schema.table> --out table.json
```

It runs `databricks tables get <table> -p <profile> -o json`, checks that the response has a
non-empty `columns` array, and writes the response to `--out`. The CLI's stderr is shown but kept
out of the file. If the CLI is missing or auth fails, it stops and prints the CLI's error with the
`databricks auth login --profile <profile>` hint. Then start `fake-uc --schema TABLE=<file>`, once per
table, which serves each table its own file.

## capture: record a corpus from a live pod

A corpus is real traffic recorded from a live pod, rebuilt into requests of production's mean size.
`capture` taps the source's outputs inside the pod (`kubectl exec … vector tap`), and reads the
pod's exporter (port 7777 unless `--exporter-port` says otherwise) before and after through the API
server's pod proxy (`kubectl get --raw /api/v1/namespaces/<namespace>/pods/<pod>:7777/proxy/metrics`), so the image
needs neither curl nor wget. The kube user needs `get` on `pods/proxy`. Nothing has to be installed
or port-forwarded locally.

```bash
hb capture --context <kube-context> --namespace <namespace> --pod <pod> --container <container> \
  --component prom_remote_write --source-type prom_remote_write \
  --duration-secs 30 --out corpus.ndjson
```

`--source-type` names the source's codec (see [Source codecs](#source-codecs)). Tap's events are
decoded by that codec; an event it cannot decode fails the capture.

Every request is cut to one size, the source's mean request size over the capture:

- **Sizes follow production's mean.** The source counts the events of each HTTP request in
  `vector_component_received_events_count` (the codec's `request_size_metric`). `capture` takes the
  growth of that histogram's `_sum` and `_count` between the two scrapes, and cuts every request
  to `round(sum / count)` items. `--items-per-request N` gives the size instead; the scrape still
  runs for the coverage figure. A capture with no requests is refused, and so is one whose mean
  rounds to 0, unless the flag is given: no size can be derived from it. A mean from a few requests
  is still a mean, so a short capture gives a rough size. The log line prints the request count.
- **The spread is not reproduced.** Production's requests vary in size and this corpus's do not:
  each is the mean, except the last, which may be smaller. The follow-up PR that captures exact
  request sizes is what closes this.
- **Coverage is measured.** The events tap returned, over the source's received-events delta for
  the same tap run. Below 95% `capture` warns that tap returned fewer events than the source
  received, so the corpus is a sample of the traffic, not all of it. Two things cause the gap: tap
  keeps at most `--limit` events from each interval, at random, and it drops events when its buffer
  is full. The capture cannot tell them apart.
- **The mix is random.** Items are shuffled with a seed and cut in that order. Production's batches
  mix items from several senders, so a request holds a random mix of them. `--seed` sets the seed;
  unset, it is random. The corpus records the seed.

Not reproduced (the TODO below):

- The spread of request sizes: every request is the mean (see above).
- Request boundaries and grouping. Production's are set by telegraf-router's buffering, and here
  they are random.
- Arrival timing. Tap has no arrival times, so `gen` paces by items per second instead.
- Completeness beyond the coverage figure, since tap samples events and drops them when full.

> **TODO (follow-up PR): request sizes, boundaries, grouping and arrival times are not production's.**
> Their mean size is, and coverage is measured, but tap cannot recover the rest: it sees decoded
> events after the source has chunked each request, with no arrival times, and it drops events
> when full. Fix candidate: a config-gated raw-request tee in the `prometheus_remote_write` source,
> which gives exact bodies, boundaries and arrival times.

What tap can and cannot give back:

- It samples. Tap runs for `--duration-secs` and, every `--interval-ms` (default 100), emits a
  uniform random sample of at most `--limit` of that interval's events (default and maximum
  10,000). A capture holds at most `duration / interval × limit` events (1,200 intervals and
  12M events for 120 s at the defaults). Tapping also costs the pod CPU; prefer staging.
- A pod that restarts during the capture is caught only when a counter disappears or goes
  down. If its counters have grown back past their earlier values by the second scrape, the restart
  goes unseen, and the coverage figure is wrong.
- Tap's JSON is decoded by the codec, which says what a usable event is. Any other event fails
  the capture instead of being dropped from it.

The corpus is NDJSON: a header line, then one line per rebuilt request holding its items. The
header records how the capture was made: where it came from, the codec's name, how tap sampled,
the events tap returned, the seed, the source's received-events delta, the coverage, the growth of
the request-size histogram's `sum` and `count` (`request_sizes`), and the size every request was cut
to (`items_per_request`: `{"size", "source"}`, where the source is `flag` or `mean`). Corpora
captured before the size was recorded have no `items_per_request`, and their bucket counts are
ignored, so they still load. A corpus in another format, such as `databricks-bench-corpus/2`, is
refused: re-run `capture` on it.

## Fakes: capturing sink output without changing the sink

Each sink is pointed at a local fake of its remote endpoint. The fake speaks the sink's own
protocol and records what arrives, so nothing inside the sink is hooked and the work that matters
for performance (encoding, compression, batching, transport, retries) still runs.

| Output | Fake | `--out` holds |
| --- | --- | --- |
| `aws_s3` (and any S3-compatible sink) | `fake-s3` | `<bucket>/<key>`: each object as uploaded, with aws-chunked framing removed and the sink's compression kept |
| `databricks_zerobus` (`oauth` auth) | `fake-zerobus` | `<bucket>/conn-NNNNNN.flight`: the frames of each connection, as received (still compressed) |

The `databricks_zerobus` sink in `oauth` mode calls two endpoints, so it needs two fakes. Point its
`unity_catalog_endpoint` at the address `fake-uc` listens on, and its `ingestion_endpoint` at the
one `fake-zerobus` listens on:

```yaml
unity_catalog_endpoint: "http://127.0.0.1:9080"  # fake-uc --listen
ingestion_endpoint: "https://127.0.0.1:9443"     # fake-zerobus --listen
```

`fake-uc` serves the token and each `--schema` table's schema, and 404s any other table. The sink
fetches them the first time it needs a stream, and again after Zerobus rejects its schema, when
`recover_from_schema_rejection` is set. It has no `--out` and records nothing. Start order: `fetch-schema`, then `fake-uc`, then
`fake-zerobus`, then the sink.

Both write `index.ndjson`, one line per accepted PUT or batch, in arrival order:

- `bucket`, `key`, `bytes` (the stored size; for Zerobus, the frame's header and body bytes).
- `recv_ms`: when the request or batch was fully read, before any injected delay.
- `ack_ms`: when the success response or ack was sent, after injected latency or ack delay and
  after storage. For Zerobus the batch is recorded just before its ack is sent, and a batch whose
  metadata does not parse is neither recorded nor acked.

Requests answered with an injected 503 are not indexed. Nothing in this harness reads the index
yet; later steps will.

Injected behaviour:

- `fake-s3 --latency-ms N`: delays every PUT before it is stored or failed.
- `fake-s3 --fail-every N`: answers every Nth PUT that passes validation with 503 SlowDown,
  counting from 1.
- `fake-zerobus --ack-delay-ms N`: delays each batch's ack, after its frame is read.
- `--discard` (`fake-s3` and `fake-zerobus`): stores no objects or frames. The index is still
  written.

So with `--latency-ms 200`, `ack_ms - recv_ms` is at least 200 for every indexed PUT.

Rejections:

- A request with a query string other than the SDK's `?x-id=PutObject` tag gets 501
  NotImplemented. This covers multipart uploads and other subresources, which would otherwise be
  stored as plain objects.
- An aws-chunked PUT whose body is malformed, or whose decoded length differs from
  `x-amz-decoded-content-length`, gets 400 and is not stored.

`--out` must be missing or empty. Both capture fakes refuse to start on a non-empty directory and
name it in the error, because the index is append-only and capture files are numbered per process: a
reused directory would mix two runs. The fakes never delete anything; pick a fresh `--out`, or
remove the old one yourself. Capture files and objects are created new, so a clash fails instead
of replacing data. A repeated S3 key is answered with a 500, where S3 would overwrite the object: the
fake keeps `create_new`, since a capture must never be replaced, so a sink that re-PUTs a key sees an
error here that S3 would not return. The index is written even with `--discard`, so the same rule
applies.

**Checksums are not verified.** `fake-s3` stores a PUT's body whatever its `Content-MD5` or
`x-amz-checksum-*` headers say. A candidate that sends a wrong checksum therefore passes here, where
S3 would answer 400 BadDigest. The fake records what was sent; it does not check the sink's integrity
headers, and `check` does not cover them.

**Zerobus specifics.** The ingestion fake follows the SDK's own mock protocol (schema message →
ready ack; each batch acked with its `offset_id` and the connection's cumulative row count). A frame
reaches the capture only once its ack is queued, so `zerobus-out` holds acked batches only. The
table schema the sink builds its Arrow buffers from comes from `fake-uc` (see above), so the sink
fills the same buffers it would in production. Ingestion stays on TLS: the fake uses a throwaway CA (`--tls-dir`,
generated if missing) and the candidate trusts it through `SSL_CERT_FILE`, instead of the SDK's
plaintext `testing` mode, which would change the build under test. The index's `bytes` is IPC
bytes as the fake received them, i.e. after tonic undoes any `grpc_compression`; it is not the
wire size.

## gen: replay a corpus

`gen` sends the corpus's requests to one or more targets (`--target name=url`), in captured order,
encoded by the corpus's codec. `--iterations N` (default 1) repeats the pass, so a run sends N
times the corpus's requests. Each request goes to every target in parallel with the same bytes; up
to `--max-concurrency` requests (default 16) are in flight at once.

```bash
hb gen --corpus corpus.ndjson --target a=http://127.0.0.1:9100/ \
  --sent-log sent.ndjson --iterations 2 --items-per-sec 20000
```

- `--items-per-sec R` paces by items, not requests, because captured requests vary in size. An
  item is one unit of the codec's input (Prometheus samples for `prom_remote_write`). A request is
  due once the items sent before it have been paced out at R, so the rate holds whatever the
  request sizes. Unset, the run goes as fast as the targets accept.
- The codec restamps every item with its request's send time, not its captured time. A sink may
  drop items outside a window around now, so a corpus captured earlier would be dropped whole. The
  send time also gives end-to-end latency without extra instrumentation.
- Responses are classified by the codec. Retryable ones are retried with the same bytes and
  timestamp, up to 5 attempts; the rest are not. An attempt that runs longer than
  `--request-timeout-secs` (default 30) counts as no response, status 0, and is retried like a
  connection error. A target that stored a request before failing it
  stores it again.
- `--sent-log` must not exist; a sent log is never replaced. It is NDJSON. The first line is the
  header, `{"corpus": {path, header}, "iterations": N, "requests_per_iteration": M,
  "items_per_sec": R or null}`, where `M` is the corpus's request count. Each later line is one request: `seq` (send order, from 0), `iteration`, `request` (index in the
  corpus), `items`, `ts` (send time), and `attempts` (per target, the HTTP status of every
  attempt; 0 for a connection-level failure). Lines are written as requests complete, so they can
  be out of order; `seq` restores the send order.
- The summary reports the achieved items/s per target, against the target rate when one is set,
  and the max dispatch lag: how far behind its due time a request was sent. A large lag means
  `--max-concurrency` or a slow target limits the rate, not the pacing.
- `gen` exits 1 when any request's last attempt was not accepted on every target.

## verify: compare candidates

`verify` compares what each candidate's sinks wrote with the baseline of the same sink. Each sink
reads its own output back as rows (see [Sinks](#sinks)). The baseline is the first output of each
sink; every other output of that sink is a candidate. It needs the sent log that `gen` wrote, and one
`--output <target>=<sink>:<dir>` per capture: the `gen --target` name, the sink kind (`s3` or
`zerobus`) and the fake's `--out`.

```bash
hb verify --sent-log sent.ndjson \
  --output a=s3:out-a/s3 --output a=zerobus:out-a/zerobus \
  --output b=s3:out-b/s3 --output b=zerobus:out-b/zerobus
```

- **Inputs.** The sent log must hold every request of its run, once each: `iterations` times the
  header's `requests_per_iteration`, with `seq` 0 to N−1. A log that is truncated or has a gap is
  refused (exit 2), since its verdict would cover only part of the run. The header's corpus header
  names the codec, so verify needs no corpus file. Each sink that appears needs at least two
  outputs, one baseline and one candidate, or verify refuses and names the sink.
- **Rows.** A row is its destination (the directory it was written under, without its file name:
  `<bucket>/<key prefix>` for S3, the table's last name segment for Zerobus), a metadata fingerprint,
  and its original bytes. Each candidate is compared with its baseline as a multiset of these
  triples; all three parts must match. Nothing is parsed or canonicalized: key order, whitespace
  and number spelling are part of the row. So rows that hold the same data but are serialized
  differently are different rows. A pipeline that needs semantic equality normalizes its rows in
  its sink's `read_back`, which then
  applies the same normalization to every output.
- **Metadata.** Fingerprinted once per S3 object or decoded Zerobus schema, then reused for
  each associated row. Object, batch and connection boundaries do not count, but changing which
  rows use which metadata does, even when the row set and schema set stay the same.
  - Zerobus: Arrow's native schema hash includes column order, names, nested types/nullability,
    and field/schema metadata (sorted by key). It is computed before timezone retagging, so
    `LargeUtf8` versus `Utf8`, timestamp units, and `UTC` versus `+00:00` remain distinct even
    when the rendered rows match. Column order matters because Zerobus validates it.
  - S3: the existing normalized filename (UUID masked as `<uuid>`, other ASCII digits as `#`)
    and the codec detected from content (`zstd`, `gzip`, `none`). This retains the prefix tail,
    time format shape and extension. A zero-padded format keeps its shape across seconds/days;
    an unpadded field can change width. A `date=%F` directory still changes destination at
    midnight. Prefix tails differing only in same-width digits remain indistinguishable;
    crypto nonces and month/weekday names are not masked.
  Empty objects/streams have no rows with which to compare metadata. Fingerprints are 64-bit
  hashes, so collisions are possible; they are not persistent identifiers.
- **Damaged records.** A cut-off object or frame keeps its complete rows and counts as a damaged
  record. Any damaged output makes the verdict DIFFERENT, even when its rows match.
- **Report.** Per candidate: counts of rows only in the baseline/candidate, then the first two
  differing rows on each side, in full with their destinations and metadata hashes. verify
  re-reads the outputs to fetch examples; it does not describe individual metadata fields.
- **Retries.** A compared target whose requests were retried, or whose last attempt was not
  accepted, can legitimately deliver duplicates that differ across candidates. The verdict is then
  INCONCLUSIVE, though the differences are still printed.
- **Exit.** 0 when IDENTICAL; 1 when DIFFERENT or INCONCLUSIVE; 2 on error (a bad flag, an
  unreadable directory, an incomplete sent log, an output with no rows, or a sink with fewer than
  two outputs).

What it does not prove: verify compares outputs with each other. It has no expected rows, so it
cannot tell a correct baseline from an incorrect one. IDENTICAL means the candidates agree with
the baseline; DIFFERENT names the candidate that disagrees, not the one that is wrong. Pair it
with a known-good baseline, or read the differing rows, before concluding which side is right.

## Sinks

A sink kind implements `SinkKind` (`sink.rs`), and is listed in `SINKS`. It names the output kind,
and `read_back` reads what that kind's fake recorded under `--out` as rows, each with its
destination and metadata fingerprint (see "verify").
Rows are flattened across objects, batches and connections, since batching depends on
timing. It has no knowledge of what the rows should hold. For `check` the same entry also says
whether the sink reads a table schema (`needs_schema`), starts its fake for a candidate
(`start_fake`), and names the environment variables that point the candidate at the fake
(`candidate_env`).

| Kind | Fake | Captured files | Read back as |
| --- | --- | --- | --- |
| `s3` (`s3_ndjson.rs`) | `fake-s3` | every object except the fake's top-level `index.ndjson` | NDJSON, one row per line; zstd or gzip detected by content. A stream cut short keeps the rows before the cut and counts one damaged record. Metadata: normalized object name and detected codec. |
| `zerobus` (`zerobus_flight.rs`) | `fake-zerobus` | every `.flight` file | Arrow Flight frames in order: a schema, then record batches, rendered with arrow's JSON writer and explicit nulls. A schema frame later in the stream applies to the batches after it. A cut-off frame keeps the complete frames before it and counts one damaged record. Metadata: decoded schema as sent, applied to subsequent rows. |

The Zerobus reader retags `UTC` timestamps as `+00:00` before rendering, since arrow's JSON
reader needs the `chrono-tz` feature for a named zone, which the bench does not enable. The
instants are unchanged, and every output is rendered the same way. The schema fingerprints are computed
before the retag, so a candidate that sends `+00:00` where the baseline sends `UTC` differs.

## Source codecs

A codec implements `SourceCodec` (`source_codec.rs`). Its jobs, which nothing else in the harness
knows:

- `decode_tap`: one tap line into an item, or a short reason it is not usable;
- `validate`: checks an item read from a corpus, so encoding can rely on it;
- `encode`: a request's items into one body and its headers, restamped with the send time;
- `classify`: an attempt's HTTP status as accepted, retryable (0, or whatever the source says to
  retry) or rejected;
- `send`: one attempt to a target, returning its status. The default is an HTTP POST; a source
  that is not HTTP replaces it, and gen's pacing, retries and sent log are unchanged;
- `request_size_metric`: the name of the histogram whose `_sum` and `_count` give capture its mean
  request size (default `vector_component_received_events_count`);
- `target_url`: the URL check gives gen for a source listening at an address (default
  `http://<addr>/`);
- `ready`: whether a source at an address is up, for check to wait on (default a TCP connect).

Items are `serde_json::Value`s in the codec's own form. Numbers are stored as strings holding
Rust's shortest round-trip text, and read back with std's parser: serde_json's own number parser is
not correctly rounded, and would change some values by one ulp. Header statistics (counts, sums,
coverage) are plain JSON numbers read by serde_json, since nothing replays them.

### `prom_remote_write`

- **Decode.** Only gauges are usable, since the source runs with `group_metrics: false` and emits
  every sample as one absolute gauge (`parse_request_flat` in `src/sources/prometheus/parser.rs`).
  A counter or histogram means the source groups metrics, and fails the capture. Tap prints a
  metric bare, as `{"name", "tags", "kind", "gauge"}`. Non-finite values print as `null`; they
  are kept as NaN, because staleness markers are common in production and the pipeline drops every
  non-finite value. Capture reads them as NaN like any other NaN, so the corpus cannot tell them
  apart, and replays do not reproduce the markers.
- **Encode.** One snappy-compressed `WriteRequest` per request, with one series per sample holding
  that one sample, restamped with the send time. Labels are sorted by name, as Prometheus writes
  them, so `__name__` is not always first.
- **Classify.** 2xx is accepted; 0 (connection error), 429 and 5xx are retryable; other statuses
  are rejected, since resending the request cannot help.

## Adding a pipeline

A pipeline is a source (its codec), a sink (its reader and fake), and the Vector config that
connects them. The core needs no change for either.

1. **Source codec.** Write `<name>.rs` with a unit struct that implements `SourceCodec`, and add
   it to `CODECS` in `source_codec.rs`. Its name is what `--source-type` and the corpus header use.
   Its unit tests cover decode (including every reason it can reject), encode (what the source
   accepts, with the send time) and classify.
2. **Sink, if it is new.** Add a fake that speaks the sink's protocol and records what arrives in
   `--out`, following the fakes' rules in [Fakes](#fakes-capturing-sink-output-without-changing-the-sink).
   Write a reader in `<name>.rs` that implements `SinkKind`, and add it to `SINKS` in
   `sink.rs`. Its kind is what `--output` and `check --sink` use. Its `start_fake` starts the
   fake in-process (each fake exposes `start` beside its `serve`), and its `candidate_env` names
   the variables the config reads, under `DATABRICKS_BENCH_*`. Its unit tests cover its rows and
   destinations,
   including a damaged or cut-off capture.
3. **Modules and features.** Add each new module to `main.rs`. Add every Vector feature the
   pipeline needs (the source's, the transform's, the sink's) to the feature list in
   [Build](#build), and to `hb`.
4. **Config.** Write the candidate's config under `configs/`, the way `hydra_dual_write.yaml` does,
   reading the sink's `DATABRICKS_BENCH_*` variables, and add the sink's kind to `check`'s
   `--sink` values.
5. **Docs.** Update this README and `CLAUDE.md` for the new codec, sink and commands.

