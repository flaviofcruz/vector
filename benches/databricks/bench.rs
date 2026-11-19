//! `bench`: the one command for performance, beside `check` for correctness.
//!
//! It runs its candidates one at a time, so no two of them compete, in `--repeat` rounds: each
//! round runs every candidate once, in the order given in odd rounds and reversed in even ones, so
//! that a drift over the invocation weighs on every candidate alike. For each run:
//! 1. starts the fakes in this process, as `check` does;
//! 2. fences the candidate CPUs off from every other process (see `fence`), with a memory limit;
//! 3. starts the candidate, moves it into the fence, which confines it to the candidate CPUs, and
//!    waits until it listens;
//! 4. replays the corpus with `gen` in this process, sampling the fence's cgroup;
//! 5. stops the candidate with SIGTERM and times its flush, reads the fence's counters once the
//!    candidate has exited (so its flush is counted), checks the fence is still valid, removes it,
//!    then stops the fakes and counts the rows each sink's fake recorded.
//!
//! The CPUs are split from the box's physical cores (see `cpus`): the first half of the cores,
//! rounded down, serve the harness, and the rest serve the candidate. Nothing is pinned: while the
//! fence holds the candidate CPUs, this process, like every other, runs on the harness CPUs. The
//! split is printed at startup and recorded in the results. Fencing needs passwordless sudo.
//!
//! A run passes when every request was accepted, the candidate exits 0 after SIGTERM with no lost
//! data in its log (a failed shutdown or dropped events), the fence is still a valid partition at
//! the end, a paced run held its rate (`RATE_TOLERANCE`, `MAX_DISPATCH_LAG`), and every sink
//! recorded rows, as many as in the invocation's first run, of whichever candidate. Candidates are
//! meant to be equivalent, so a different count means one of them lost or duplicated rows. Outputs
//! are not compared row by row: that is `check`. A harness failure (the fakes or gen cannot run,
//! or a candidate never listens) is an error. Exit 0 when every run passed, 1 when any failed, 2 on
//! error.
//!
//! Everything written goes under `--work-dir`, which must be missing or empty and is never deleted:
//! - `results.json`: the settings, every run's numbers with its candidate, round, binary, config
//!   and `--version` line, and each candidate's median and range over its runs.
//! - `<candidate>/<round>/vector.log`: the candidate's stdout and stderr in that round.
//! - `<candidate>/<round>/sent.ndjson`: the sent log `gen` wrote for that run.
//! - `<candidate>/<round>/data/`, `tls/`, `<sink>-out/`: as for `check`, per run.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::process::Child;

use super::cgroup::{AnonSampler, Cgroup};
use super::check::{
    Candidate, FLUSH_TIMEOUT, GRACEFUL_SHUTDOWN_SECS, READY_TIMEOUT, REQUEST_TIMEOUT_SECS,
    check_candidates, common_env, parse_candidate, parse_sinks, schema_for, sink_out_dir,
    start_fake_uc, start_sink_fakes,
};
use super::corpus::Corpus;
use super::cpus::{self, Split, self_cpu_secs};
use super::fake_uc::{self, TableSchema};
use super::fakes::{BoxError, RunningFake};
use super::fence::{Fence, FenceSettings, check_partition};
use super::generator::{
    CorpusIdentity, GenSummary, RunSettings, check_run_args, generate, parse_targets,
};
use super::out_dir::claim_empty_dir;
use super::process::{
    Stopped, free_loopback_addr, log_tail, logged_failure, open_gate, spawn_gated_candidate,
    terminate, wait_for_listener,
};
use super::sink::{SinkKind, sink_named};
use super::source_codec::codec_named;
use super::{CmdResult, err};

/// The memory limit of a candidate's fence when `--candidate-memory` is not given.
const DEFAULT_CANDIDATE_MEMORY: &str = "8G";
/// With `--items-per-sec`, a run fails when gen achieved more than this fraction below it: the
/// candidate, not the target, then set the rate it was measured at.
const RATE_TOLERANCE: f64 = 0.01;
/// With `--items-per-sec`, a run fails when a request was dispatched later than this after its
/// due time, which means the candidate answered too slowly for the pacing to hold. Background jobs
/// on the harness CPUs delayed a dispatch by up to 0.8 s in runs that otherwise held their rate.
const MAX_DISPATCH_LAG: Duration = Duration::from_secs(3);

#[derive(clap::Args)]
pub struct BenchArgs {
    /// Corpus to replay, as written by `capture`.
    #[arg(long)]
    corpus: PathBuf,
    /// A sink kind to run, by its kind name. Repeatable. The config must use exactly these sinks.
    #[arg(long = "sink", required = true, value_name = "KIND")]
    sinks: Vec<String>,
    /// Fake delay per selected sink kind, as KIND=MS. Repeatable, once per kind; omitted kinds
    /// use zero. Identical for every candidate and round. Object stores (`s3`, `azure_blob`,
    /// `gcs`) delay PUT storage/responses; Zerobus delays batch acknowledgements. No bandwidth or network simulation.
    #[arg(long = "sink-latency-ms", value_name = "KIND=MS", value_parser = parse_sink_latency)]
    sink_latencies: Vec<(String, u64)>,
    /// A candidate, as NAME=VECTOR_BIN:CONFIG. Repeatable. Candidates run one at a time, in the
    /// order given, reversed every other round. Use a release build (`cargo build --release`)
    /// copied out of `target/`: `cargo bench` replaces `target/release/vector`, which is refused.
    #[arg(long = "candidate", required = true, value_name = "NAME=BIN:CONFIG")]
    candidates: Vec<String>,
    /// A table schema for fake-uc, as `TABLE=PATH`, as for `check`. Repeatable, once per table.
    /// Required when a `--sink` reads a table schema.
    #[arg(
        long = "schema",
        value_name = "TABLE=PATH",
        value_parser = fake_uc::parse_table_schema
    )]
    schemas: Vec<TableSchema>,
    /// Passes over the corpus, as for `gen`.
    #[arg(long, default_value_t = 1)]
    iterations: u64,
    /// Rounds, each of which runs every candidate once. The order is reversed every other round,
    /// so that a drift over the invocation weighs on every candidate alike. Use at least 3 to
    /// compare candidates.
    #[arg(long, default_value_t = 1)]
    repeat: u64,
    /// Items per second, as for `gen`. Unset: as fast as the candidate accepts, which measures
    /// its capacity.
    #[arg(long)]
    items_per_sec: Option<f64>,
    /// Requests in flight at once, as for `gen`.
    #[arg(long, default_value_t = 16)]
    max_concurrency: usize,
    /// Memory limit of each candidate's fence, as cgroup `memory.max` takes it (bytes, or with a
    /// K, M or G suffix, such as `8G`). Swap is off.
    #[arg(long, default_value = DEFAULT_CANDIDATE_MEMORY)]
    candidate_memory: String,
    /// Threads of the runtime the load generator and the fakes share. Defaults to the number of
    /// harness CPUs.
    #[arg(long)]
    gen_threads: Option<usize>,
    /// Must be missing or empty; created if missing. Everything the run writes goes under it.
    #[arg(long)]
    work_dir: PathBuf,
}

/// Parses one fake delay using the sink registry's kind names.
fn parse_sink_latency(spec: &str) -> Result<(String, u64), String> {
    let invalid =
        || format!("--sink-latency-ms `{spec}`: expected KIND=MS with non-negative integer MS");
    let (kind, ms) = spec.split_once('=').ok_or_else(invalid)?;
    sink_named(kind).map_err(|e| format!("--sink-latency-ms: {e}"))?;
    if ms.is_empty() || !ms.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid());
    }
    Ok((kind.to_owned(), ms.parse().map_err(|_| invalid())?))
}

/// Effective delays for every selected kind, including zero defaults. One map is shared by all
/// candidates and rounds and saved with the results.
fn sink_latencies(
    sinks: &[&dyn SinkKind],
    specs: &[(String, u64)],
) -> Result<BTreeMap<String, u64>, BoxError> {
    let mut delays: BTreeMap<String, u64> = sinks
        .iter()
        .map(|sink| (sink.name().to_owned(), 0))
        .collect();
    for (i, (kind, ms)) in specs.iter().enumerate() {
        if specs[..i].iter().any(|(seen, _)| seen == kind) {
            return Err(err(format!(
                "--sink-latency-ms `{kind}` is given more than once"
            )));
        }
        let delay = delays
            .get_mut(kind)
            .ok_or_else(|| err(format!("--sink-latency-ms `{kind}` requires --sink {kind}")))?;
        *delay = *ms;
    }
    Ok(delays)
}

/// The run as the command line describes it. Building one reads the binaries, configs, schema and
/// CPU topology it names, and starts and writes nothing.
struct Plan {
    corpus: PathBuf,
    sinks: Vec<&'static dyn SinkKind>,
    sink_latencies_ms: BTreeMap<String, u64>,
    candidates: Vec<Candidate>,
    /// Empty when no `--sink` reads a table schema.
    schemas: Vec<TableSchema>,
    iterations: u64,
    repeat: u64,
    items_per_sec: Option<f64>,
    max_concurrency: usize,
    candidate_memory: String,
    gen_threads: usize,
    split: Split,
    work_dir: PathBuf,
}

impl Plan {
    fn from_args(args: &BenchArgs) -> Result<Self, BoxError> {
        let sinks = parse_sinks(&args.sinks)?;
        let sink_latencies_ms = sink_latencies(&sinks, &args.sink_latencies)?;
        let candidates = args
            .candidates
            .iter()
            .map(|spec| parse_candidate("--candidate", spec))
            .collect::<Result<Vec<_>, _>>()?;
        check_candidates(&candidates)?;
        if let Some(relinked) = relinked_vector() {
            refuse_relinked(&candidates, &relinked)?;
        }
        let schemas = schema_for(&sinks, &args.schemas)?;
        check_run_args(
            args.iterations,
            args.items_per_sec,
            args.max_concurrency,
            REQUEST_TIMEOUT_SECS,
        )?;
        if args.repeat == 0 {
            return Err(err("--repeat must be at least 1"));
        }
        let split = cpus::split_by_core()?;
        let gen_threads = args.gen_threads.unwrap_or(split.harness.len());
        if gen_threads == 0 {
            return Err(err("--gen-threads must be at least 1"));
        }
        Ok(Self {
            corpus: args.corpus.clone(),
            sinks,
            sink_latencies_ms,
            candidates,
            schemas,
            iterations: args.iterations,
            repeat: args.repeat,
            items_per_sec: args.items_per_sec,
            max_concurrency: args.max_concurrency,
            candidate_memory: args.candidate_memory.clone(),
            gen_threads,
            split,
            work_dir: args.work_dir.clone(),
        })
    }
}

/// Runs `bench`. It refuses a debug build first, since its numbers would describe the debug build.
pub fn run_cli(args: BenchArgs) -> CmdResult {
    if cfg!(debug_assertions) {
        return Err(err(
            "bench needs an optimized build, and this one is a debug build. Run it with \
             `cargo bench --bench databricks --no-default-features --features <FEATURES>` and no \
             `--profile dev`; FEATURES is the list in benches/databricks/README.md",
        ));
    }
    let plan = Plan::from_args(&args)?;
    eprintln!(
        "bench: harness CPUs {} run gen, the fakes and everything else on the box; candidate CPUs \
         {} are fenced off for each candidate alone, with {} of memory and no swap",
        list(&plan.split.harness),
        list(&plan.split.candidate),
        plan.candidate_memory
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(plan.gen_threads)
        .enable_all()
        .build()?;
    runtime.block_on(run(plan))
}

/// What a run has started and not yet stopped, so that an error or Ctrl-C stops it.
#[derive(Default)]
struct Session {
    candidate: Option<Child>,
    fence: Option<Fence>,
    fakes: Vec<RunningFake>,
}

async fn run(plan: Plan) -> CmdResult {
    claim_empty_dir("--work-dir", &plan.work_dir)?;
    let corpus = Corpus::load(&plan.corpus).map_err(|e| err(format!("--corpus: {e}")))?;

    let mut session = Session::default();
    let outcome = tokio::select! {
        outcome = measure_all(&plan, &corpus, &mut session) => outcome,
        _ = tokio::signal::ctrl_c() => Err(err(
            "interrupted: the candidate was sent SIGTERM and the fakes stopped; the work dir is kept",
        )),
    };
    teardown(&mut session).await;
    let runs = outcome?;

    let summary = summarize(&plan.candidates, &runs);
    write_results(&plan, &runs, &summary)?;
    print_table(&runs);
    print_summary(&summary);
    let passed = runs.iter().filter(|run| run.failures.is_empty()).count();
    eprintln!(
        "bench: {passed} of {} runs passed; results {}",
        runs.len(),
        plan.work_dir.join("results.json").display()
    );
    Ok(passed == runs.len())
}

/// Stops whatever is still running after an error or Ctrl-C. A normal run finds nothing left.
async fn teardown(session: &mut Session) {
    if let Some(child) = session.candidate.as_mut()
        && let Err(e) = terminate(child, FLUSH_TIMEOUT).await
    {
        eprintln!("bench: stopping candidate: {e}");
    }
    if let Some(fence) = session.fence.take()
        && let Err(e) = fence.remove()
    {
        eprintln!("bench: {e}");
    }
    for fake in session.fakes.drain(..) {
        if let Err(e) = fake.stop().await {
            eprintln!("bench: stopping fakes: {e}");
        }
    }
}

async fn measure_all(
    plan: &Plan,
    corpus: &Corpus,
    session: &mut Session,
) -> Result<Vec<Run>, BoxError> {
    let mut runs = Vec::new();
    let mut first: Option<FirstRun> = None;
    for round in 1..=plan.repeat {
        for index in round_order(plan.candidates.len(), round) {
            let candidate = &plan.candidates[index];
            eprintln!(
                "bench: round {round} of {}: candidate {} ({})",
                plan.repeat,
                candidate.name,
                candidate.bin.display()
            );
            let run = measure(plan, corpus, candidate, round, first.as_ref(), session).await?;
            first.get_or_insert_with(|| FirstRun {
                delivered: run.delivered.clone(),
                retried: run.loadgen.retried_attempts > 0,
            });
            runs.push(run);
        }
    }
    Ok(runs)
}

/// The candidates' indices in the order they run in `round`, counting from 1: as given in odd
/// rounds and reversed in even ones, so over each pair of rounds a drift that grows linearly with
/// time weighs on every candidate alike.
fn round_order(candidates: usize, round: u64) -> Vec<usize> {
    let mut order: Vec<usize> = (0..candidates).collect();
    if round.is_multiple_of(2) {
        order.reverse();
    }
    order
}

/// One candidate's run in `round`: the fakes, the candidate, the timed replay, the flush, then
/// what the fakes recorded. `first` is the invocation's first run, `None` while this is it.
async fn measure(
    plan: &Plan,
    corpus: &Corpus,
    candidate: &Candidate,
    round: u64,
    first: Option<&FirstRun>,
    session: &mut Session,
) -> Result<Run, BoxError> {
    let dir = plan.work_dir.join(&candidate.name).join(round.to_string());
    let data_dir = dir.join("data");
    std::fs::create_dir_all(&data_dir)
        .map_err(|e| err(format!("creating {}: {e}", data_dir.display())))?;
    let bin = candidate
        .bin
        .canonicalize()
        .map_err(|e| err(format!("{}: {e}", candidate.bin.display())))?;
    let config = candidate
        .config
        .canonicalize()
        .map_err(|e| err(format!("{}: {e}", candidate.config.display())))?;
    let version = vector_version(&bin)?;

    let schema_endpoint = start_fake_uc(&plan.schemas, &mut session.fakes)?;
    let fakes_env = start_sink_fakes(
        &dir,
        &plan.sinks,
        &plan.sink_latencies_ms,
        schema_endpoint,
        &mut session.fakes,
    )?;

    let source = free_loopback_addr()?;
    let mut env = common_env(&data_dir, source);
    env.extend(fakes_env);

    let log = dir.join("vector.log");
    let fence = session.fence.insert(Fence::create(
        &plan.split.candidate,
        &plan.candidate_memory,
    )?);
    let fence_settings = fence.describe()?;
    let cgroup = fence.cgroup();
    let child = spawn_gated_candidate(&bin, &config, &env, &log, GRACEFUL_SHUTDOWN_SECS)?;
    let child = session.candidate.insert(child);
    let pid = child.id().expect("a child that is still running has a pid");
    // Into the fence before Vector starts, so that all its memory is charged there and every
    // thread it starts runs on the fence's CPUs.
    fence.admit(pid)?;
    open_gate(child).await?;
    let codec = codec_named(&corpus.header.source_type)?;
    if let Err(e) = wait_for_listener(codec, source, child, READY_TIMEOUT).await {
        return Err(err(format!(
            "candidate `{}` did not start listening: {e}\nlog {}:\n{}",
            candidate.name,
            log.display(),
            log_tail(&log)
        )));
    }

    let targets = parse_targets(&[format!("{}={}", candidate.name, codec.target_url(source))])?;
    let identity = CorpusIdentity {
        path: plan.corpus.clone(),
        header: corpus.header.clone(),
    };
    let settings = RunSettings {
        iterations: plan.iterations,
        items_per_sec: plan.items_per_sec,
        max_concurrency: plan.max_concurrency,
        request_timeout: Duration::from_secs(REQUEST_TIMEOUT_SECS),
    };
    let sent_log = dir.join("sent.ndjson");

    let before = Counters::read(&cgroup)?;
    let sampler = AnonSampler::start(cgroup.clone());
    let started = Instant::now();
    let summary = generate(corpus, identity, &targets, &sent_log, settings).await?;
    let replay = started.elapsed();
    summary.print(&sent_log);
    // The harness is measured over the replay, where it works.
    let harness_cpu_secs = self_cpu_secs()? - before.harness_cpu_secs;

    // The candidate is measured until it exits: what it accepted but had not yet written is
    // written during the flush, and counting stops otherwise would favour a candidate that
    // buffers more. The fence outlives its process, so its counters still hold the exited
    // candidate's CPU time and memory peak.
    let mut candidate_child = session.candidate.take().expect("set above");
    let flush_started = Instant::now();
    let stopped = terminate(&mut candidate_child, FLUSH_TIMEOUT).await?;
    let flush = flush_started.elapsed();
    let anon_max = sampler.stop().await?;
    let candidate_cpu_secs = (cgroup.cpu_usage_usec()? - before.candidate_cpu_usec) as f64 / 1e6;
    let oom_kills = cgroup.oom_kills()? - before.oom_kills;
    let memory_peak = cgroup.memory_peak()?;
    let exit = match stopped {
        Stopped::Exited(status) => Some(status),
        Stopped::Killed => None,
    };
    let invalid_fence = check_partition(&fence.partition()?).err();
    session.fence.take().expect("set above").remove()?;

    let mut problems = Vec::new();
    for fake in session.fakes.drain(..) {
        if let Err(e) = fake.stop().await {
            problems.push(e.to_string());
        }
    }
    if !problems.is_empty() {
        return Err(err(format!("stopping the fakes: {}", problems.join("\n"))));
    }
    // Read once the fakes have stopped, so the outputs are complete, and outside any timing.
    let delivered = plan
        .sinks
        .iter()
        .map(|sink| {
            let mut rows = 0;
            sink.read_back(&sink_out_dir(&dir, *sink), &mut |_, _, _| rows += 1)?;
            Ok(Delivered {
                sink: sink.name(),
                rows,
            })
        })
        .collect::<Result<Vec<_>, BoxError>>()?;
    let failures = failures(&Evidence {
        exit,
        lost_data: logged_failure(&log)?,
        invalid_fence,
        summary: &summary,
        delivered: &delivered,
        first,
    });

    let secs = replay.as_secs_f64();
    let candidate_wall = (replay + flush).as_secs_f64();
    Ok(Run {
        name: candidate.name.clone(),
        round,
        bin,
        config,
        version,
        candidate: CandidateNumbers {
            fence: fence_settings,
            cpu_secs: candidate_cpu_secs,
            cpus_used: candidate_cpu_secs / candidate_wall,
            cpu_ms_per_1k_items: candidate_cpu_secs * 1e6 / summary.items.max(1) as f64,
            memory_peak_bytes: memory_peak,
            anon_max_bytes: anon_max,
            oom_kills,
        },
        harness: HarnessNumbers {
            cpus: plan.split.harness.clone(),
            cpus_used: harness_cpu_secs / secs,
        },
        loadgen: LoadgenNumbers {
            requests: summary.requests,
            items: summary.items,
            elapsed_secs: summary.elapsed.as_secs_f64(),
            items_per_sec: summary.items as f64 / summary.elapsed.as_secs_f64().max(1e-9),
            target_items_per_sec: plan.items_per_sec,
            max_dispatch_lag_ms: summary.max_lag.as_secs_f64() * 1000.0,
            all_accepted: summary.all_delivered(),
            retried_attempts: summary.retries.values().sum(),
        },
        delivered,
        flush_secs: flush.as_secs_f64(),
        failures,
    })
}

/// Rows one sink's fake recorded in a run.
#[derive(Clone, Debug, Serialize)]
struct Delivered {
    sink: &'static str,
    rows: u64,
}

/// The invocation's first run, which later runs' deliveries are compared with.
struct FirstRun {
    delivered: Vec<Delivered>,
    retried: bool,
}

/// What decides whether a run passed, gathered once its candidate and fakes have stopped.
struct Evidence<'a> {
    /// `None` when the candidate had to be killed.
    exit: Option<ExitStatus>,
    /// The first line of the candidate's log that reports lost data.
    lost_data: Option<String>,
    /// Why the fence was no longer a valid partition at the end of the run.
    invalid_fence: Option<String>,
    summary: &'a GenSummary,
    delivered: &'a [Delivered],
    /// `None` when this is the first run.
    first: Option<&'a FirstRun>,
}

/// Why a candidate's run failed, one reason each. Empty when it passed.
fn failures(evidence: &Evidence) -> Vec<String> {
    let mut reasons = Vec::new();
    match evidence.exit {
        Some(status) if status.success() => {}
        Some(status) => reasons.push(format!("exited with {status} after SIGTERM")),
        None => reasons.push(format!(
            "did not exit within {FLUSH_TIMEOUT:?} of SIGTERM and was killed"
        )),
    }
    if let Some(line) = &evidence.lost_data {
        reasons.push(format!("logged lost data: `{line}`"));
    }
    let summary = evidence.summary;
    if !summary.all_delivered() {
        reasons.push("some requests were not accepted on every target".to_owned());
    }
    if let Some(problem) = &evidence.invalid_fence {
        reasons.push(format!(
            "the fence was no longer valid at the end of the run, so its numbers cannot be \
             trusted: {problem}"
        ));
    }
    if let Some(target) = summary.items_per_sec {
        let achieved = summary.items as f64 / summary.elapsed.as_secs_f64().max(1e-9);
        if achieved < target * (1.0 - RATE_TOLERANCE) {
            reasons.push(format!(
                "gen achieved {achieved:.0} items/s, more than {:.0}% below the target {target:.0}, \
                 so the candidate set the rate",
                RATE_TOLERANCE * 100.0
            ));
        }
        if summary.max_lag > MAX_DISPATCH_LAG {
            reasons.push(format!(
                "a request was dispatched {:.0} ms after its due time (limit {MAX_DISPATCH_LAG:?}), \
                 so the pacing did not hold",
                summary.max_lag.as_secs_f64() * 1000.0
            ));
        }
    }
    let retried = summary.retries.values().any(|&n| n > 0)
        || evidence.first.is_some_and(|first| first.retried);
    for delivered in evidence.delivered {
        if delivered.rows == 0 {
            reasons.push(format!("sink {} recorded no rows", delivered.sink));
            continue;
        }
        // A first run with no rows has already failed for it, and says nothing about this one.
        let Some(first_rows) = evidence
            .first
            .and_then(|first| first.delivered.iter().find(|d| d.sink == delivered.sink))
            .map(|d| d.rows)
            .filter(|&rows| rows > 0)
        else {
            continue;
        };
        if delivered.rows != first_rows {
            let hint = if retried {
                "; requests were retried, which can deliver duplicates and may explain it"
            } else {
                ""
            };
            reasons.push(format!(
                "sink {} recorded {} rows and the first run's recorded {first_rows}, though \
                 benchmarked candidates are meant to be equivalent (`check` verifies that){hint}",
                delivered.sink, delivered.rows
            ));
        }
    }
    reasons
}

/// The counters read at the start of the replay. The candidate's are read again after it exits;
/// the harness's (this process: gen and the fakes) at the end of the replay.
struct Counters {
    candidate_cpu_usec: u64,
    oom_kills: u64,
    harness_cpu_secs: f64,
}

impl Counters {
    fn read(cgroup: &Cgroup) -> Result<Self, BoxError> {
        Ok(Self {
            candidate_cpu_usec: cgroup.cpu_usage_usec()?,
            oom_kills: cgroup.oom_kills()?,
            harness_cpu_secs: self_cpu_secs()?,
        })
    }
}

/// The `vector` that `cargo bench` re-links to its own bench-profile build every time it runs: the
/// one in this executable's profile directory, the parent of its `deps/`. `None` when this
/// executable is not in a `deps/` directory.
fn relinked_vector() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let deps = exe.parent()?;
    if deps.file_name()? != "deps" {
        return None;
    }
    Some(deps.parent()?.join("vector"))
}

/// Refuses a candidate whose binary is the same file as `relinked` (same device and inode, with
/// symlinks followed): whatever was built there, `cargo bench` has replaced it with the bench
/// profile's build of Vector by the time this runs.
fn refuse_relinked(candidates: &[Candidate], relinked: &Path) -> Result<(), BoxError> {
    use std::os::unix::fs::MetadataExt;
    let Ok(theirs) = std::fs::metadata(relinked) else {
        return Ok(());
    };
    for candidate in candidates {
        let bin = std::fs::metadata(&candidate.bin)
            .map_err(|e| err(format!("{}: {e}", candidate.bin.display())))?;
        if (bin.dev(), bin.ino()) == (theirs.dev(), theirs.ino()) {
            return Err(err(format!(
                "{} `{}`: {} is {}, which `cargo bench` re-links to its own bench-profile build \
                 of Vector every time it runs, so it is not your release build. Build the \
                 candidate with `cargo build --release`, copy it out of target/ (for example \
                 `cp target/release/vector /tmp/vector-{}`), and pass the copy",
                candidate.flag,
                candidate.name,
                candidate.bin.display(),
                relinked.display(),
                candidate.name
            )));
        }
    }
    Ok(())
}

/// The first line of `bin --version`, which names the build. Only a build with debug info names
/// its git SHA (`-custom-<sha>`), so a release build's does not.
fn vector_version(bin: &Path) -> Result<String, BoxError> {
    let output = std::process::Command::new(bin)
        .arg("--version")
        .output()
        .map_err(|e| err(format!("running {} --version: {e}", bin.display())))?;
    if !output.status.success() {
        return Err(err(format!(
            "{} --version exited with {}",
            bin.display(),
            output.status
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .next()
        .unwrap_or_default()
        .trim()
        .to_owned())
}

fn list(cpus: &[usize]) -> String {
    cpus.iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

#[derive(Serialize)]
struct Run {
    name: String,
    /// The round this run was in, counting from 1.
    round: u64,
    bin: PathBuf,
    config: PathBuf,
    version: String,
    candidate: CandidateNumbers,
    harness: HarnessNumbers,
    loadgen: LoadgenNumbers,
    /// Rows each sink's fake recorded, read back after the fakes stopped.
    delivered: Vec<Delivered>,
    flush_secs: f64,
    /// Why the run failed; empty when it passed.
    failures: Vec<String>,
}

#[derive(Serialize)]
struct CandidateNumbers {
    /// The fence the candidate ran in: its cgroup, partition type, CPUs and memory limits.
    fence: FenceSettings,
    /// CPU seconds the fence used from the start of the replay until the candidate exited, so the
    /// flush of what it had buffered is counted.
    cpu_secs: f64,
    /// `cpu_secs` per wall second of replay and flush: the average number of its CPUs kept busy.
    cpus_used: f64,
    /// `cpu_secs` per 1,000 items sent, in milliseconds: the cost of the work, however a candidate
    /// splits it between the replay and the flush.
    cpu_ms_per_1k_items: f64,
    /// `memory.peak` after the candidate exited.
    memory_peak_bytes: u64,
    anon_max_bytes: u64,
    oom_kills: u64,
}

#[derive(Serialize)]
struct HarnessNumbers {
    /// The harness CPUs, which run the load generator and the fakes.
    cpus: Vec<usize>,
    /// CPU seconds of this process, gen and the fakes together, per wall second of replay: the
    /// average number of the harness CPUs it kept busy.
    cpus_used: f64,
}

#[derive(Serialize)]
struct LoadgenNumbers {
    requests: u64,
    items: u64,
    elapsed_secs: f64,
    items_per_sec: f64,
    target_items_per_sec: Option<f64>,
    max_dispatch_lag_ms: f64,
    all_accepted: bool,
    /// Attempts beyond each request's first, which can deliver a row twice.
    retried_attempts: u64,
}

#[derive(Serialize)]
struct Settings<'a> {
    corpus: &'a Path,
    sinks: Vec<&'static str>,
    sink_latencies_ms: &'a BTreeMap<String, u64>,
    iterations: u64,
    repeat: u64,
    items_per_sec: Option<f64>,
    max_concurrency: usize,
    candidate_memory: &'a str,
    gen_threads: usize,
    harness_cpus: &'a [usize],
    candidate_cpus: &'a [usize],
}

#[derive(Serialize)]
struct Results<'a> {
    settings: Settings<'a>,
    runs: &'a [Run],
    summary: &'a [CandidateSummary],
}

/// Writes `results.json` in the work dir. It is new, so an earlier run's results are never replaced.
fn write_results(plan: &Plan, runs: &[Run], summary: &[CandidateSummary]) -> Result<(), BoxError> {
    let path = plan.work_dir.join("results.json");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|e| err(format!("creating {}: {e}", path.display())))?;
    let results = Results {
        settings: Settings {
            corpus: &plan.corpus,
            sinks: plan.sinks.iter().map(|sink| sink.name()).collect(),
            sink_latencies_ms: &plan.sink_latencies_ms,
            iterations: plan.iterations,
            repeat: plan.repeat,
            items_per_sec: plan.items_per_sec,
            max_concurrency: plan.max_concurrency,
            candidate_memory: &plan.candidate_memory,
            gen_threads: plan.gen_threads,
            harness_cpus: &plan.split.harness,
            candidate_cpus: &plan.split.candidate,
        },
        runs,
        summary,
    };
    serde_json::to_writer_pretty(file, &results)
        .map_err(|e| err(format!("writing {}: {e}", path.display())))
}

/// The compact table of every run, then each run's binary, config, version and failures.
fn print_table(runs: &[Run]) {
    println!(
        "{:<12} {:>5} {:>9} {:>9} {:>10} {:>9} {:>9} {:>4} {:>10} {:>10} {:>8} {:>11} {:>9} {:>10} {:>8} {:>6}",
        "candidate",
        "round",
        "cand-cpu",
        "cpu-s",
        "ms/1k-item",
        "mem-peak",
        "anon-max",
        "oom",
        "items/s",
        "target",
        "lag-ms",
        "harness-cpu",
        "accepted",
        "rows",
        "flush-s",
        "result"
    );
    for run in runs {
        let target = run
            .loadgen
            .target_items_per_sec
            .map_or("-".to_owned(), |rate| format!("{rate:.0}"));
        println!(
            "{:<12} {:>5} {:>9.2} {:>9.1} {:>10.2} {:>9} {:>9} {:>4} {:>10.0} {:>10} {:>8.1} {:>11.2} {:>9} {:>10} {:>8.2} {:>6}",
            run.name,
            run.round,
            run.candidate.cpus_used,
            run.candidate.cpu_secs,
            run.candidate.cpu_ms_per_1k_items,
            mib(run.candidate.memory_peak_bytes),
            mib(run.candidate.anon_max_bytes),
            run.candidate.oom_kills,
            run.loadgen.items_per_sec,
            target,
            run.loadgen.max_dispatch_lag_ms,
            run.harness.cpus_used,
            if run.loadgen.all_accepted {
                "yes"
            } else {
                "no"
            },
            run.delivered.iter().map(|d| d.rows).sum::<u64>(),
            run.flush_secs,
            if run.failures.is_empty() {
                "PASS"
            } else {
                "FAIL"
            },
        );
    }
    println!(
        "cand-cpu, cpu-s, ms/1k-item: the candidate from replay start until it exited, flush \
         included; cand-cpu is busy CPUs out of {}. harness-cpu: busy CPUs of the bench process \
         (gen and the fakes) over the replay, out of {} harness CPUs. rows: every sink's, read \
         back after the run.",
        runs.first().map_or(0, |run| run.candidate.fence.cpus.len()),
        runs.first().map_or(0, |run| run.harness.cpus.len()),
    );
    for run in runs {
        println!(
            "{} round {}: {} ({})",
            run.name,
            run.round,
            run.bin.display(),
            run.version
        );
        println!("  config {}", run.config.display());
        let rows: Vec<String> = run
            .delivered
            .iter()
            .map(|d| format!("{} {}", d.sink, d.rows))
            .collect();
        println!("  rows {}", rows.join(", "));
        for reason in &run.failures {
            println!("  failed: {reason}");
        }
    }
}

/// The median and range of one number over a candidate's runs.
#[derive(Debug, PartialEq, Serialize)]
struct Spread {
    median: f64,
    min: f64,
    max: f64,
}

impl Spread {
    /// `None` for no values.
    fn of(mut values: Vec<f64>) -> Option<Self> {
        values.sort_by(f64::total_cmp);
        let (&min, &max) = (values.first()?, values.last()?);
        let mid = values.len() / 2;
        let median = if values.len().is_multiple_of(2) {
            (values[mid - 1] + values[mid]) / 2.0
        } else {
            values[mid]
        };
        Some(Self { median, min, max })
    }

    fn show(&self, format: impl Fn(f64) -> String) -> String {
        format!(
            "{} [{}-{}]",
            format(self.median),
            format(self.min),
            format(self.max)
        )
    }
}

/// One candidate's numbers over all its runs, failed runs included.
#[derive(Serialize)]
struct CandidateSummary {
    name: String,
    runs: usize,
    failed: usize,
    cpu_ms_per_1k_items: Spread,
    cpus_used: Spread,
    memory_peak_bytes: Spread,
    anon_max_bytes: Spread,
}

/// A summary per candidate, in the order given, of every run it had.
fn summarize(candidates: &[Candidate], runs: &[Run]) -> Vec<CandidateSummary> {
    candidates
        .iter()
        .filter_map(|candidate| {
            let mine: Vec<&Run> = runs.iter().filter(|r| r.name == candidate.name).collect();
            let spread =
                |number: fn(&Run) -> f64| Spread::of(mine.iter().map(|r| number(r)).collect());
            Some(CandidateSummary {
                name: candidate.name.clone(),
                runs: mine.len(),
                failed: mine.iter().filter(|r| !r.failures.is_empty()).count(),
                cpu_ms_per_1k_items: spread(|r| r.candidate.cpu_ms_per_1k_items)?,
                cpus_used: spread(|r| r.candidate.cpus_used)?,
                memory_peak_bytes: spread(|r| r.candidate.memory_peak_bytes as f64)?,
                anon_max_bytes: spread(|r| r.candidate.anon_max_bytes as f64)?,
            })
        })
        .collect()
}

/// Each candidate's median and range over its runs.
fn print_summary(summary: &[CandidateSummary]) {
    println!(
        "{:<12} {:>4} {:>6} {:>22} {:>18} {:>20} {:>20}",
        "candidate", "runs", "failed", "ms/1k-item", "cand-cpu", "mem-peak MiB", "anon-max MiB"
    );
    let two = |v: f64| format!("{v:.2}");
    let in_mib = |v: f64| format!("{:.0}", v / (1024.0 * 1024.0));
    for candidate in summary {
        println!(
            "{:<12} {:>4} {:>6} {:>22} {:>18} {:>20} {:>20}",
            candidate.name,
            candidate.runs,
            candidate.failed,
            candidate.cpu_ms_per_1k_items.show(two),
            candidate.cpus_used.show(two),
            candidate.memory_peak_bytes.show(in_mib),
            candidate.anon_max_bytes.show(in_mib),
        );
    }
    println!("median [min-max] over each candidate's runs, failed runs included.");
}

fn mib(bytes: u64) -> String {
    format!("{:.0} MiB", bytes as f64 / (1024.0 * 1024.0))
}

#[cfg(test)]
#[allow(unused_imports, dead_code)] // Bench builds compile this module but never run it.
mod tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;

    #[derive(clap::Parser)]
    struct TestCli {
        #[command(flatten)]
        args: BenchArgs,
    }

    fn latency_args(specs: &[&str]) -> Result<BenchArgs, clap::Error> {
        use clap::Parser;
        let mut argv = vec![
            "bench",
            "--corpus",
            "unused",
            "--sink",
            "s3",
            "--sink",
            "zerobus",
            "--candidate",
            "a=unused:unused",
            "--work-dir",
            "unused",
        ];
        for spec in specs {
            argv.extend(["--sink-latency-ms", spec]);
        }
        TestCli::try_parse_from(argv).map(|cli| cli.args)
    }

    #[test]
    fn sink_latency_cli_defaults_and_overrides() {
        let sinks = parse_sinks(&["s3".into(), "zerobus".into()]).unwrap();
        for (specs, s3, zerobus) in [
            (vec![], 0, 0),
            (vec!["s3=100"], 100, 0),
            (vec!["zerobus=50"], 0, 50),
            (vec!["zerobus=50", "s3=100"], 100, 50),
            (vec!["s3=0", "zerobus=0"], 0, 0),
        ] {
            let args = latency_args(&specs).unwrap();
            let effective = sink_latencies(&sinks, &args.sink_latencies).unwrap();
            assert_eq!(
                effective,
                BTreeMap::from([("s3".into(), s3), ("zerobus".into(), zerobus)])
            );
        }
    }

    #[test]
    fn sink_latency_cli_refuses_bad_specs() {
        for spec in [
            "s3",
            "s3=",
            "=10",
            "s3=-1",
            "s3=1.5",
            "s3=NaN",
            "s3=+1",
            "s3=1=2",
            "s3= 1",
            "kafka=10",
            "s3=18446744073709551616",
        ] {
            let error = latency_args(&[spec]).err().unwrap().to_string();
            assert!(error.contains("--sink-latency-ms"), "{error}");
        }
        // Validation precedes candidate file checks and any runtime or output creation.
        let args = latency_args(&["s3=0", "s3=1"]).unwrap();
        let error = Plan::from_args(&args).err().unwrap().to_string();
        assert!(error.contains("given more than once"), "{error}");
        let mut args = latency_args(&["zerobus=0"]).unwrap();
        args.sinks = vec!["s3".into()];
        let error = Plan::from_args(&args).err().unwrap().to_string();
        assert!(error.contains("requires --sink zerobus"), "{error}");
    }

    fn exit(code: i32) -> ExitStatus {
        // A wait status: the exit code sits in the high byte.
        ExitStatus::from_raw(code << 8)
    }

    /// One sink's rows.
    fn rows(sink: &'static str, rows: u64) -> Vec<Delivered> {
        vec![Delivered { sink, rows }]
    }

    /// A first run, or a later one when `first` is given, with nothing wrong.
    fn clean<'a>(
        summary: &'a GenSummary,
        delivered: &'a [Delivered],
        first: Option<&'a FirstRun>,
    ) -> Evidence<'a> {
        Evidence {
            exit: Some(exit(0)),
            lost_data: None,
            invalid_fence: None,
            summary,
            delivered,
            first,
        }
    }

    #[test]
    fn a_run_passes_only_on_a_clean_exit_no_lost_data_all_accepted_and_a_valid_fence() {
        let summary = GenSummary::default();
        let delivered = rows("s3", 10);
        assert!(failures(&clean(&summary, &delivered, None)).is_empty());

        let bad_exit = failures(&Evidence {
            exit: Some(exit(1)),
            ..clean(&summary, &delivered, None)
        });
        assert_eq!(bad_exit.len(), 1, "{bad_exit:?}");
        assert!(bad_exit[0].contains("exited with"), "{bad_exit:?}");

        let killed = failures(&Evidence {
            exit: None,
            ..clean(&summary, &delivered, None)
        });
        assert!(killed[0].contains("was killed"), "{killed:?}");

        let shutdown = failures(&Evidence {
            lost_data: Some("ERROR: Overall shutdown deadline exceeded".into()),
            ..clean(&summary, &delivered, None)
        });
        assert!(shutdown[0].contains("logged lost data"), "{shutdown:?}");

        let mut rejected_gen = GenSummary::default();
        rejected_gen
            .final_status
            .insert("a".into(), [(503, 1)].into_iter().collect());
        let rejected = failures(&clean(&rejected_gen, &delivered, None));
        assert!(rejected[0].contains("not accepted"), "{rejected:?}");

        let fence = failures(&Evidence {
            invalid_fence: check_partition("root invalid (Cpu list in cpuset.cpus not exclusive)")
                .err(),
            ..clean(&summary, &delivered, None)
        });
        assert_eq!(fence.len(), 1, "{fence:?}");
        assert!(fence[0].contains("fence was no longer valid"), "{fence:?}");
        assert!(fence[0].contains("not exclusive"), "{fence:?}");
    }

    #[test]
    fn a_paced_run_fails_below_its_rate_or_with_a_late_dispatch() {
        let delivered = rows("s3", 10);
        let paced = |items, lag_ms| GenSummary {
            items,
            elapsed: Duration::from_secs(1),
            items_per_sec: Some(1000.0),
            max_lag: Duration::from_millis(lag_ms),
            ..Default::default()
        };
        assert!(failures(&clean(&paced(995, 2999), &delivered, None)).is_empty());

        let slow = failures(&clean(&paced(980, 0), &delivered, None));
        assert_eq!(slow.len(), 1, "{slow:?}");
        assert!(slow[0].contains("achieved 980 items/s"), "{slow:?}");

        let late = failures(&clean(&paced(1000, 3500), &delivered, None));
        assert_eq!(late.len(), 1, "{late:?}");
        assert!(late[0].contains("dispatched 3500 ms"), "{late:?}");

        let unpaced = GenSummary {
            items: 10,
            elapsed: Duration::from_secs(1),
            max_lag: Duration::from_secs(5),
            ..Default::default()
        };
        assert!(failures(&clean(&unpaced, &delivered, None)).is_empty());
    }

    #[test]
    fn a_sink_must_record_rows_and_as_many_as_in_the_first_run() {
        let summary = GenSummary::default();
        let empty = failures(&clean(&summary, &rows("zerobus", 0), None));
        assert_eq!(empty, ["sink zerobus recorded no rows"]);

        let first = FirstRun {
            delivered: rows("s3", 10),
            retried: false,
        };
        assert!(failures(&clean(&summary, &rows("s3", 10), Some(&first))).is_empty());

        let fewer = failures(&clean(&summary, &rows("s3", 9), Some(&first)));
        assert_eq!(fewer.len(), 1, "{fewer:?}");
        assert!(
            fewer[0].contains("recorded 9 rows and the first run's recorded 10"),
            "{fewer:?}"
        );
        assert!(!fewer[0].contains("retried"), "{fewer:?}");

        let mut retried_gen = GenSummary::default();
        retried_gen.retries.insert("a".into(), 1);
        let retried = failures(&clean(&retried_gen, &rows("s3", 11), Some(&first)));
        assert!(retried[0].contains("may explain it"), "{retried:?}");

        let first_retried = FirstRun {
            retried: true,
            ..first
        };
        let earlier = failures(&clean(&summary, &rows("s3", 9), Some(&first_retried)));
        assert!(earlier[0].contains("may explain it"), "{earlier:?}");

        let empty_first = FirstRun {
            delivered: rows("s3", 0),
            retried: false,
        };
        assert!(failures(&clean(&summary, &rows("s3", 10), Some(&empty_first))).is_empty());
    }

    #[test]
    fn a_candidate_that_is_the_relinked_vector_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let relinked = dir.path().join("vector");
        let other = dir.path().join("vector-a");
        let linked = dir.path().join("linked");
        std::fs::write(&relinked, b"bench build").unwrap();
        std::fs::write(&other, b"release build").unwrap();
        std::fs::hard_link(&relinked, &linked).unwrap();
        let candidate = |name: &str, bin: &Path| Candidate {
            flag: "--candidate",
            name: name.to_owned(),
            bin: bin.to_owned(),
            config: dir.path().join("config.yaml"),
        };

        assert!(refuse_relinked(&[candidate("a", &other)], &relinked).is_ok());
        for bin in [&relinked, &linked] {
            let error = refuse_relinked(&[candidate("a", &other), candidate("b", bin)], &relinked)
                .unwrap_err()
                .to_string();
            assert!(error.starts_with("--candidate `b`"), "{error}");
            assert!(error.contains("cp target/release/vector"), "{error}");
        }
        // Nothing has been built there yet.
        let missing = dir.path().join("none");
        assert!(refuse_relinked(&[candidate("a", &relinked)], &missing).is_ok());
    }

    #[test]
    fn rounds_alternate_the_order_of_the_candidates() {
        assert_eq!(round_order(2, 1), [0, 1]);
        assert_eq!(round_order(2, 2), [1, 0]);
        assert_eq!(round_order(2, 3), [0, 1]);
        assert_eq!(round_order(3, 2), [2, 1, 0]);
        assert_eq!(round_order(1, 2), [0]);
    }

    #[test]
    fn a_spread_is_the_median_and_range() {
        let odd = Spread::of(vec![3.0, 1.0, 2.0]).unwrap();
        assert_eq!(
            odd,
            Spread {
                median: 2.0,
                min: 1.0,
                max: 3.0
            }
        );
        assert_eq!(Spread::of(vec![4.0, 1.0, 2.0, 3.0]).unwrap().median, 2.5);
        assert_eq!(Spread::of(vec![]), None);
        assert_eq!(odd.show(|v| format!("{v:.1}")), "2.0 [1.0-3.0]");
    }

    #[test]
    fn cpu_lists_print_with_commas() {
        assert_eq!(list(&[2, 3, 6, 7]), "2,3,6,7");
        assert_eq!(list(&[]), "");
    }
}
