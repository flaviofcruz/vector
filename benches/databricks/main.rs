//! Databricks bench: checks Databricks Vector pipelines against a baseline.
//!
//! Drives a real Vector process (any source/transform/sink combination, supplied as a config
//! file) with traffic in a source's own protocol, replayed from a corpus of captured events. The
//! source's codec and the pipeline's sink are the only parts that know their protocols; see
//! `source_codec` and `benches/databricks/README.md`.
#![allow(deprecated)] // hyper 0.14 server/client APIs, matching the rest of the crate.

mod bench;
mod capture;
mod cgroup;
mod check;
mod corpus;
mod cpus;
mod fake_s3;
mod fake_uc;
mod fake_zerobus;
mod fakes;
mod fence;
mod fetch_schema;
mod generator;
mod out_dir;
mod process;
mod profile;
mod prom_remote_write;
mod s3_ndjson;
mod sink;
mod source_codec;
mod tls;
mod verify;
mod zerobus_flight;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "databricks-bench",
    about = "Correctness/performance harness for Databricks Vector pipelines (first: Hydra)"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
    /// `cargo bench` appends `--bench` to a bench binary's arguments; accept and ignore it.
    #[arg(long = "bench", global = true, hide = true)]
    _cargo_bench: bool,
}

#[derive(Subcommand)]
enum Command {
    /// Runs the whole correctness check: the one command to run, the others are its building blocks.
    ///
    /// Starts a fake for each candidate's sinks, runs each candidate as a Vector process, replays the
    /// corpus to all of them, stops them with SIGTERM so their sinks flush, then verifies each sink's
    /// outputs against the baseline. Exits 0 when IDENTICAL, 1 when DIFFERENT or INCONCLUSIVE (or when
    /// a request was not accepted), and 2 on error. Everything written goes under --work-dir, which
    /// must be missing or empty (see benches/databricks/README.md for what each file is).
    ///
    /// Real runs need optimized builds of both sides: the bench (plain `cargo bench`, without
    /// `--profile dev`) and each candidate (`cargo build --release`). Dev builds are only for trying
    /// things out; they are slow enough to distort timing and drain.
    Check(check::CheckArgs),
    /// Tap a live pod's source and reconstruct its input requests into a corpus.
    Capture(capture::CaptureArgs),
    /// Measure where a live pod's Vector process spends CPU, or summarize an existing profile
    /// (folded stacks or a flamegraph SVG) with --folded.
    ///
    /// Same kube arguments as `capture`. `--method platform` (default) runs the platform's Rust
    /// profiler (`//common/profiling/rust/cpu:profiler`, perf with DWARF stacks) on the Vector
    /// process. That tool changes the node's perf sysctls for the run and restores them, and
    /// stages its helpers in the pod. `--method perf-user` runs the image's perf on user space only
    /// (no node change; kernel CPU is not in the stacks). `--method counters` is read-only: it
    /// attributes the process's CPU from /proc to each sink's encode counters. Writes profile.folded,
    /// summary.json and summary.md (top frames, and buckets by module path) into --out, which must
    /// be missing or empty.
    Profile(profile::ProfileArgs),
    /// Save a Unity Catalog table's schema (via the databricks CLI) for fake-uc to serve.
    FetchSchema(fetch_schema::FetchSchemaArgs),
    /// Serve a minimal S3 PutObject endpoint that records every object it receives.
    FakeS3(fake_s3::FakeS3Args),
    /// Serve the Unity Catalog and OAuth endpoints the sink calls to get its table schemas, one
    /// `--schema TABLE=PATH` per table.
    FakeUc(fake_uc::FakeUcArgs),
    /// Serve a fake Zerobus ingestion endpoint (Arrow Flight over TLS) that records batches.
    FakeZerobus(fake_zerobus::FakeZerobusArgs),
    /// Replay a corpus to one or more targets, recording what was sent.
    Gen(generator::GenArgs),
    /// Compare what candidates' sinks wrote, each with its sink's baseline.
    Verify(verify::VerifyArgs),
    /// Runs the candidates one at a time for performance: the one command for perf, beside `check`.
    ///
    /// It runs in --repeat rounds, each of which runs every candidate once, in the order given and
    /// then reversed, alternately. For each run it starts the fakes in this process, fences the
    /// candidate CPUs off from every other process (a cpuset partition with a memory limit, made
    /// with passwordless sudo), starts the candidate inside the fence, replays the corpus with the
    /// load generator, stops the candidate with SIGTERM (timing its flush), removes the fence, then
    /// stops the fakes. The CPUs are split from the box's cores: the first half serves the harness
    /// (the load generator and the fakes) and the rest the candidate.
    ///
    /// Real runs need optimized builds: the bench (plain `cargo bench`, without `--profile dev`,
    /// which is refused) and each candidate (`cargo build --release`, copied out of `target/`, since
    /// `cargo bench` replaces `target/release/vector` with its own build, which is refused too).
    /// The candidate's `--version` line is recorded with its binary and config.
    ///
    /// A run fails when a request was not accepted, the candidate exits non-zero after SIGTERM or
    /// logs a failed shutdown, its fence is no longer valid at the end, a paced run missed its
    /// rate, or a sink recorded no rows or a different count from the first run's. Outputs are not
    /// verified: that is `check`. Exits 0 when every run passed, 1 when any failed, and 2 on error.
    /// Everything written goes under --work-dir, which must be missing or empty; `results.json`
    /// holds every run and each candidate's median and range over its runs.
    Bench(bench::BenchArgs),
}

fn main() {
    // hyper, h2 and tonic log through `tracing` but print nothing without a subscriber; this
    // lets RUST_LOG (e.g. `debug`, or `h2=info,debug`) show them. Unset, only errors print.
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Check(args) => runtime().block_on(check::run(args)),
        Command::Capture(args) => capture::capture(args),
        Command::Profile(args) => profile::profile(args),
        Command::FetchSchema(args) => fetch_schema::fetch_schema(args),
        Command::FakeS3(args) => runtime().block_on(fake_s3::serve(args)),
        Command::FakeUc(args) => runtime().block_on(fake_uc::serve(args)),
        Command::FakeZerobus(args) => runtime().block_on(fake_zerobus::serve(args)),
        Command::Gen(args) => runtime().block_on(generator::run_cli(args)),
        Command::Verify(args) => verify::run_cli(args),
        Command::Bench(args) => bench::run_cli(args),
    };
    match result {
        Ok(true) => {}
        Ok(false) => std::process::exit(1),
        Err(error) => {
            eprintln!("error: {error:#}");
            std::process::exit(2);
        }
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

/// Result type for subcommands: `Ok(false)` means the command ran but found failures.
pub type CmdResult = Result<bool, Box<dyn std::error::Error + Send + Sync>>;

pub fn err(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    message.into().into()
}

/// Names the operation and path in an I/O error: `reading --schema x.json: No such file ...`.
pub fn io_at<T>(
    result: std::io::Result<T>,
    action: &str,
    path: &std::path::Path,
) -> Result<T, Box<dyn std::error::Error + Send + Sync>> {
    result.map_err(|e| err(format!("{action} {}: {e}", path.display())))
}

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
