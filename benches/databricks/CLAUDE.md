# Databricks bench: agent setup and workflow

`benches/databricks` is a harness for checking implementations of Databricks Vector pipelines
against a baseline. The first supported pipeline is Hydra: Prometheus remote write in, then
`metric_to_log_hydra`, then the `aws_s3` or `databricks_zerobus` sink. The core (`capture`,
`corpus`, `gen`, `verify`) handles items and rows and never reads inside them. A source's protocol
lives behind the `SourceCodec` trait in `source_codec.rs` (first codec: `prom_remote_write.rs`), and
a sink's output behind `SinkKind` in `sink.rs` (kinds `s3` and `zerobus`), whose `read_back` reads
it back as rows, and which `check` also uses to start the sink's fake and point a candidate at it.
A new pipeline adds a codec, and a reader if its sink is new; see `README.md` ("Adding a pipeline").
This file covers what an agent on a fresh Databricks dev box (Arca/Arclet) needs in order to
build, test and run the harness without guessing.

## Machine setup (once per box)

1. **Checkouts.** This repo's `Cargo.toml` takes `vrl` from `path = "../vrl"`, so clone
   `databricks-eng/vrl`, branch `v0.30.0-custom`, as a sibling of this checkout. A universe
   checkout at `~/universe` provides the Rust toolchain. On an Arclet only `~/universe` survives
   stop/start, so clones elsewhere, including this one, are lost.
2. **Crates proxy.** crates.io and other public registries are blocked; every crate must come
   through the internal proxy. Create `~/.cargo/config.toml` if it is missing. It is not
   persisted on an Arclet.
   ```toml
   [source.crates-io]
   replace-with = "databricks-proxy"

   [source.databricks-proxy]
   registry = "sparse+https://crates-proxy.cloud.databricks.com/"

   [net]
   git-fetch-with-cli = true
   retry = 5
   ```
   The git dependencies (e.g. `databricks/zerobus-sdk`) are fetched with the `git` CLI. Never
   point Cargo at crates.io or a mirror. If the proxy is unreachable, stop and ask.
3. **Rust toolchain.** Use universe's wrapper, not rustup or a downloaded toolchain:
   ```bash
   export UNIVERSE_ROOT=~/universe
   export PATH=~/universe/bin/rust/db-toolchain/bin:$PATH
   export RUSTFLAGS=--cap-lints=warn
   ```
   The wrapper takes the Databricks rustc from Bazel and caches it in
   `~/.cache/databricks-rust-toolchain/`, so the first call is slow and needs a working Bazel in
   universe. That rustc is newer than the fork's pinned `1.92` (`rust-toolchain.toml`), and new
   lints would fail the fork's `#![deny(warnings)]` crates; `--cap-lints=warn` keeps them
   warnings. Expect a wall of dependency warnings on every build. They are not ours.

## Build and run

The bench needs `--no-default-features` plus this exact feature set. Pass it as a single
comma-separated value:

```bash
FEATURES=databricks-benches,codecs-arrow,sources-prometheus-remote-write
FEATURES=$FEATURES,transforms-metric_to_log_hydra,sinks-aws_s3
FEATURES=$FEATURES,sinks-databricks-zerobus,unix
cargo bench --profile dev --bench databricks --no-default-features \
  --features $FEATURES -- <subcommand> [args]
```

- `--profile dev` avoids a release build. Use `--no-run` to just compile.
- `-- --help` lists the subcommands; `-- <subcommand> --help` gives each one's flags.
- `cargo bench` appends `--bench` to the binary's arguments; `main.rs` ignores it.
- On failure, cargo adds `error: bench failed … Caused by: process didn't exit successfully`.
  The real error is the line above that.
- Many cargo builds of this repo take minutes, and cargo holds a lock on `target/`. Never run two
  cargo commands at once.

## Test and lint (run before every commit)

Use the same env and features as above:

```bash
cargo test --profile dev --test databricks_bench --no-default-features \
  --features $FEATURES
cargo clippy --profile dev --bench databricks --test databricks_bench \
  --no-default-features --features $FEATURES
rustfmt --edition 2024 --check benches/databricks/main.rs
```

`databricks_bench` (`unit_tests.rs`) runs the modules' unit tests. Clippy must report nothing under
`benches/databricks`; warnings elsewhere in the repo predate this harness. `rustfmt` on `main.rs`
also covers every module it declares. CI's Clippy, Build and unit-test jobs use default features,
so **CI never compiles this bench**: local checks are the only gate.

**Do not run `scripts/ci-local.sh` for bench-only changes.** It builds and tests the whole
workspace with default features, which never compiles this bench (`required-features =
["databricks-benches"]`), and it takes a very long time. For a change under `benches/databricks`,
run these instead:

- `cargo metadata --locked`;
- `cargo fmt --all -- --check` (only `benches/databricks` matters);
- `python3 scripts/check_databricks_changelog.py --base-ref origin/v0.53-custom --head-ref HEAD
  --repository databricks-eng/vector`;
- the bench tests and clippy above.

`scripts/ci-local.sh` is needed only when a change touches code outside the bench.

## Running a correctness check (`check`)

`check` is the one command for a correctness run. Use it instead of driving `gen` and `verify` by
hand. It starts the fakes and the candidates, replays the corpus, flushes the candidates with
SIGTERM, and verifies. Everything in `README.md` ("check") applies; the points an agent needs are:

- **Build profiles.** For a real result, build the bench with plain `cargo bench` (no
  `--profile dev`) and each candidate with `cargo build --release`. Dev builds are only for trying
  things out. A dev-profile E2E against the small corpus is fine for a check of the harness itself.
- **Command.** The committed config is `configs/hydra_dual_write.yaml`, and it needs both sinks:

  ```bash
  cargo bench --profile dev --bench databricks --no-default-features --features $FEATURES -- \
    check --corpus ~/scratch/hb/pr-capture.v4.ndjson --sink s3 --sink zerobus \
    --schema main.databricks_bench.output=$HOME/scratch/hb/fetched/pod_prime_raw.json \
    --baseline a=target/debug/vector:benches/databricks/configs/hydra_dual_write.yaml \
    --candidate b=target/debug/vector:benches/databricks/configs/hydra_dual_write.yaml \
    --work-dir /tmp/check-e2e/<run>
  ```

  `--work-dir` must not exist or must be empty; use a fresh path per run. The run takes minutes
  (about 3.5 min for the 270,608-row corpus in the dev profile, most of it verify).
- **Exit codes.** 0 IDENTICAL; 1 DIFFERENT or INCONCLUSIVE, or a request not accepted; 2 error,
  including a candidate whose log reports lost data (a failed shutdown, or events dropped by
  mistake, which verify cannot see when every candidate drops the same rows). For 1 or 2, read the report on stderr,
  then `<work-dir>/<name>/vector.log` for the candidate in question. For a DIFFERENT verdict, the
  report shows the rows that differ, with their destinations.
- **Ports.** `check` binds only OS-assigned `127.0.0.1` ports, so it does not clash with the user's
  services. Do not start fakes or candidates on fixed ports for a `check` run.
- **Processes.** A run leaves no process behind. After a failed or interrupted run, confirm with
  `ps` that no `vector --config` line naming the work dir is left, and stop any by PID.
- **Changing the candidate.** The config must use exactly the `--sink` values it is run with; the
  variables it reads are listed in `README.md` ("check").

## Running a performance run (`bench`)

`bench` is the one command for performance, beside `check`. It runs each candidate alone on half
of the box's cores, fenced off from every other process by a cgroup cpuset partition with a memory
limit; the fakes, the load generator and the rest of the box run on the other half. Everything in `README.md` ("bench") applies. The points an agent
needs are:

- **Builds.** The bench refuses a debug build, so run it with plain `cargo bench` (no
  `--profile dev`), and build each candidate with `cargo build --release`, then copy it out of
  `target/` (`cp target/release/vector /tmp/vector-a`) and pass the copy. Every `cargo bench`
  re-links `target/release/vector` to the bench profile's build, so bench refuses that path. A
  release build's `--version` names no commit and has no `debug=`; one with debug info (bench or
  dev profile) says `-custom-<sha>` and `debug=full`. The version line is recorded in
  `results.json`.
- **Command.** The same sinks, schema and corpus as `check`, with `--candidate NAME=BIN:CONFIG`
  (one or more, run in order) and optional `--items-per-sec`, `--iterations`, `--repeat`,
  `--candidate-memory`, `--gen-threads`. Use `--repeat 3` or more to compare candidates, and
  compare the summary's medians:

  ```bash
  cargo bench --bench databricks --no-default-features --features $FEATURES -- \
    bench --corpus ~/scratch/hb/pr-capture.v4.ndjson --sink s3 --sink zerobus \
    --schema main.databricks_bench.output=$HOME/scratch/hb/fetched/pod_prime_raw.json \
    --candidate a=/tmp/vector-a:$CFG --candidate b=/tmp/vector-a:$CFG \
    --items-per-sec 30000 --iterations 10 --repeat 3 --work-dir /tmp/bench-$(date +%s)
  ```

- **Rate.** Pass `--items-per-sec`. A staging AWS us-west-2 pod received about 30k items/s on
  2026-11-13 (README, "Which rate"), so 30000 is the default choice. Unset, a candidate accepts
  faster than its sinks drain, its memory climbs to `--candidate-memory`, and it may be OOM-killed.

- **Exit codes.** 0 when every run passed; 1 when a run failed (a request not accepted, the
  candidate exited non-zero after SIGTERM or logged lost data: a failed shutdown or dropped
  events, the fence was no longer a valid partition at the end, a paced run missed its rate, or a
  sink recorded no rows or a different count from the first run's); 2 on error. Nothing here
  verifies outputs: use `check` for that.
- **Equivalence.** bench assumes its candidates are semantically equivalent, so every run's sinks
  record the same number of rows. Run `check` on the candidates first.
- **Sudo.** The fence (`/sys/fs/cgroup/databricks-bench`) is made and removed with `sudo -n`, so
  bench needs passwordless sudo. It changes which CPUs the rest of the box may use while a
  candidate runs; ask the user before running bench.
- **Duration.** Set `--iterations` to about `seconds × items/s ÷ items per pass` (283,544 for v4):
  10 at 30k items/s replays for about 95 s.
- **Leftovers.** A bench run stops its candidates and fakes and removes the fence, and leaves none
  behind. After an interrupted run, check `ps` for `vector --config` lines naming the work dir and
  stop any by PID, and if `/sys/fs/cgroup/databricks-bench` still exists, run the two commands
  bench printed (README, "bench").

## Running against real systems

- **`capture`** runs `vector tap` inside a live pod via `kubectl exec`, and scrapes the pod's
  exporter on port 7777 (`--exporter-port`), through the API server's pod proxy (`get pods/proxy`
  RBAC), for the source's request-size histogram's `_sum` and `_count`, so it needs kube access to
  the target cluster. Staging VAM: context
  `staging-aws-us-west-2-0`, namespace `vector-aggregator`, container
  `vector-aggregator-metrics`, component and source type `prom_remote_write`. Pick a pod that is
  Running with all containers ready. A 30 s capture is about 300 MB.
- **`fetch-schema`** shells out to the `databricks` CLI, e.g. with profile
  `logfood-us-west-2-mt-staging` and table `main.eng_hydra.pod_prime_raw`. On an auth error run
  `databricks auth login --profile <p>` (interactive; ask the user to do it).
- **Fakes** (`fake-s3`, `fake-uc`, `fake-zerobus`) stand in for the sinks' remote services
  locally, and print `DATABRICKS_BENCH_LISTENING <addr>` once bound. A Zerobus candidate also needs
  `SSL_CERT_FILE` pointing at `<tls-dir>/ca.pem`.
- **`gen`** replays a corpus to one or more `--target name=http://…` (plain HTTP only),
  `--iterations` passes in captured order, paced by `--items-per-sec` and capped at
  `--max-concurrency` requests in flight. It loads the whole corpus into memory first (a 300 MB
  corpus takes several seconds, and that time is excluded from its timings). It exits 1 if any
  request's last attempt was not accepted (2xx) on every target; the codec classifies statuses.
  The `--sent-log` header records the corpus path and header, and the request count, so verify
  needs only the sent log.
- **`verify`** compares each candidate's sink output with its sink's baseline (the first output of
  that sink), as a multiset of rows, each compared by its destination and exact bytes. Nothing is
  parsed, so rows serialized differently are different rows. Run it once the sinks have flushed (the S3 batch
  timeout, then a few seconds). A sent log that is truncated or has a gap is refused (exit 2). Each `--output <target>=<s3|zerobus>:<dir>` names the sent-log target, the sink
  kind and that fake's `--out`; each sink needs at least two outputs. Any retried or not-accepted
  request on a compared target makes the verdict INCONCLUSIVE. It proves only that outputs agree
  with the baseline, not that the baseline is right. Exit 0 when IDENTICAL, 1 when DIFFERENT or
  INCONCLUSIVE, 2 on error. The report and its limits are in `README.md`.
- **A vector candidate under test** needs, besides its config: a fresh `data_dir`, dummy AWS
  credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_EC2_METADATA_DISABLED=true`)
  for an `aws_s3` sink pointed at fake-s3 (`endpoint` + `force_path_style: true`), and a
  restart after fake-uc is up, since the Zerobus sink fetches its schema lazily. Build it with
  `cargo build --profile dev --bin vector --no-default-features --features` and the bench list
  minus `databricks-benches`.

### Manual flow (debugging only)

`check` does all of this. Use the manual flow only to debug one part, such as a fake or a sink
config, that `check` does not expose. Start fake-uc (`--schema TABLE=PATH` per table), fake-s3 and
fake-zerobus on fresh `--out` dirs, export the candidate's `DATABRICKS_BENCH_*` variables (the table in `README.md`,
"check") pointing at them, and start the candidate. Then:

1. `hb gen --corpus c.ndjson --target a=http://127.0.0.1:<port>/ --sent-log sent.ndjson`
2. Stop the candidate with SIGTERM and wait for it to exit. Its sinks flush on shutdown.
3. `hb verify --sent-log sent.ndjson \
     --output a=s3:<fake-s3 --out> --output a=zerobus:<fake-zerobus --out>`

`hb` is the shorthand defined in `README.md`. Two candidates: give `gen` both targets and `verify`
all four outputs.

## Conventions

- Output paths (`--out`, `--sent-log`) must be new (or, for `--out`, empty). The tools refuse rather than
  overwrite, and never delete. Use a fresh path per run.
- Errors name the flag and path. Doc comments say what is written to disk and why. Keep both
  true when changing code.
- The user may have fakes or vector running on 9000, 9080, 9443 and 9100. Agents use other
  ports.
- The harness is built one layer per commit. Update this file in the same commit when a layer
  changes setup, commands or conventions.
