//! `check`: the whole correctness flow in one command.
//!
//! It starts the fakes in this process, starts each candidate as a Vector process, replays the
//! corpus to all of them with `gen`, stops each candidate with SIGTERM so its sinks flush, stops
//! the fakes, and runs `verify` on each sink's outputs with the baseline first. It exits with
//! verify's verdict: 0 IDENTICAL, 1 DIFFERENT or INCONCLUSIVE (or requests gen could not deliver),
//! 2 on error.
//!
//! Everything it writes goes under `--work-dir`, which must be missing or empty and is never
//! deleted:
//! - `sent.ndjson`: the sent log `gen` wrote. `verify` reads it, and it is the record of every
//!   request sent.
//! - `<candidate>/vector.log`: the candidate's stdout and stderr, the first place to look.
//! - `<candidate>/data/`: the candidate's data dir (disk buffers and checkpoints), fresh per run.
//! - `<candidate>/tls/`: the throwaway CA and server certificate of the fake Zerobus endpoint. The
//!   candidate trusts `ca.pem` through `SSL_CERT_FILE`.
//! - `<candidate>/<sink>-out/`: what the candidate's sink wrote, as its fake recorded it. `verify`
//!   compares these.
//!
//! The candidate's config reads its endpoints from environment variables. Every candidate gets:
//! - `DATABRICKS_BENCH_SOURCE_ADDR`: the `127.0.0.1` port its source must listen on;
//! - `DATABRICKS_BENCH_DATA_DIR`: its fresh data dir.
//!
//! Each `--sink` kind adds its own variables (see `sink.rs`, `zerobus_flight.rs`). The config
//! must use exactly the sinks given, since a variable it reads but that is not set fails its load.
//!
//! The flush relies on Vector's shutdown: on SIGTERM it stops the sources, and each sink sees its
//! input end and writes its open batch, so no batch timeout has to elapse. `terminate` in
//! `process.rs` bounds that wait and kills a candidate that does not exit in time.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::process::Child;

use super::corpus::Corpus;
use super::fake_uc::{self, FakeUcArgs, TableSchema};
use super::fakes::{BoxError, EPHEMERAL_LOOPBACK, RunningFake};
use super::generator::{
    CorpusIdentity, RunSettings, Target, check_run_args, generate, parse_targets,
};
use super::out_dir::claim_empty_dir;
use super::process::{
    Stopped, free_loopback_addr, log_tail, logged_failure, spawn_candidate, terminate,
    wait_for_listener,
};
use super::sink::{FakeSetup, SinkKind, sink_named};
use super::source_codec::codec_named;
use super::verify::verify;
use super::{CmdResult, err};

/// Set in every candidate's environment: the address its source must listen on.
pub const SOURCE_ADDR_VAR: &str = "DATABRICKS_BENCH_SOURCE_ADDR";
/// Set in every candidate's environment: its fresh data dir.
pub const DATA_DIR_VAR: &str = "DATABRICKS_BENCH_DATA_DIR";
/// How long a candidate may take to listen on its source after it starts.
const READY_TIMEOUT: Duration = Duration::from_secs(60);
/// How long a candidate may take to flush and exit after SIGTERM, before it is killed.
const FLUSH_TIMEOUT: Duration = Duration::from_secs(60);
/// Vector's own graceful-shutdown budget. It is below `FLUSH_TIMEOUT`, so a slow flush is ended
/// by Vector, which logs why, before the harness kills the process.
const GRACEFUL_SHUTDOWN_SECS: u64 = 50;
/// Per-attempt timeout for `gen`, as its default.
const REQUEST_TIMEOUT_SECS: u64 = 30;

#[derive(clap::Args)]
pub struct CheckArgs {
    /// Corpus to replay, as written by `capture`.
    #[arg(long)]
    corpus: PathBuf,
    /// A sink kind to check, by its kind name. Repeatable. The config must use exactly these sinks.
    #[arg(long = "sink", required = true, value_name = "KIND")]
    sinks: Vec<String>,
    /// The baseline, assumed correct, as NAME=VECTOR_BIN:CONFIG. Its config is split at the last
    /// colon, so the config path may not contain one.
    #[arg(long, value_name = "NAME=BIN:CONFIG")]
    baseline: String,
    /// A candidate to compare with the baseline, as NAME=VECTOR_BIN:CONFIG. Repeatable. Names are
    /// letters, digits and underscores, and must differ.
    #[arg(long = "candidate", required = true, value_name = "NAME=BIN:CONFIG")]
    candidates: Vec<String>,
    /// Table schema the shared fake-uc serves, as `TABLE=PATH`: TABLE is the full
    /// `<catalog>.<schema>.<table>` the config writes, and PATH the file `fetch-schema` saved for
    /// it. Repeatable, once per table; a table not given gets 404 from fake-uc. Required when a
    /// `--sink` reads a table schema, and refused otherwise.
    #[arg(
        long = "schema",
        value_name = "TABLE=PATH",
        value_parser = fake_uc::parse_table_schema
    )]
    schemas: Vec<TableSchema>,
    /// Passes over the corpus, as for `gen`.
    #[arg(long, default_value_t = 1)]
    iterations: u64,
    /// Items per second, as for `gen`. Unset: as fast as the candidates accept.
    #[arg(long)]
    items_per_sec: Option<f64>,
    /// Requests in flight at once, as for `gen`.
    #[arg(long, default_value_t = 16)]
    max_concurrency: usize,
    /// Must be missing or empty; created if missing. Everything the run writes goes under it, as
    /// `benches/databricks/README.md` ("check") lists.
    #[arg(long)]
    work_dir: PathBuf,
}

/// One candidate: a name, a Vector binary and its config.
#[derive(Debug, PartialEq)]
struct Candidate {
    /// The flag it came from, for error messages.
    flag: &'static str,
    name: String,
    bin: PathBuf,
    config: PathBuf,
}

/// Parses `NAME=BIN:CONFIG`. The config is taken after the last colon.
fn parse_candidate(flag: &'static str, spec: &str) -> Result<Candidate, BoxError> {
    let usage = || err(format!("{flag} `{spec}`: expected NAME=VECTOR_BIN:CONFIG"));
    let (name, rest) = spec.split_once('=').ok_or_else(usage)?;
    let (bin, config) = rest.rsplit_once(':').ok_or_else(usage)?;
    if bin.is_empty() || config.is_empty() {
        return Err(usage());
    }
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(err(format!(
            "{flag} `{spec}`: the name `{name}` must be letters, digits or underscores; \
             it names a directory and, for some sinks, a table"
        )));
    }
    Ok(Candidate {
        flag,
        name: name.to_owned(),
        bin: bin.into(),
        config: config.into(),
    })
}

fn parse_sinks(kinds: &[String]) -> Result<Vec<&'static dyn SinkKind>, BoxError> {
    let mut sinks: Vec<&'static dyn SinkKind> = Vec::new();
    for kind in kinds {
        let sink = sink_named(kind).map_err(|e| err(format!("--sink: {e}")))?;
        if sinks.iter().any(|seen| seen.name() == sink.name()) {
            return Err(err(format!("--sink `{kind}` is given more than once")));
        }
        sinks.push(sink);
    }
    Ok(sinks)
}

/// The run as the command line describes it. Building one reads nothing but the binaries, configs
/// and schema it names, and starts and writes nothing.
struct Plan {
    corpus: PathBuf,
    sinks: Vec<&'static dyn SinkKind>,
    /// The baseline first, then the candidates in the order given.
    candidates: Vec<Candidate>,
    /// Empty when no `--sink` reads a table schema.
    schemas: Vec<TableSchema>,
    iterations: u64,
    items_per_sec: Option<f64>,
    max_concurrency: usize,
    work_dir: PathBuf,
}

impl Plan {
    fn from_args(args: &CheckArgs) -> Result<Self, BoxError> {
        let sinks = parse_sinks(&args.sinks)?;
        let mut candidates = vec![parse_candidate("--baseline", &args.baseline)?];
        for spec in &args.candidates {
            candidates.push(parse_candidate("--candidate", spec)?);
        }
        for (i, candidate) in candidates.iter().enumerate() {
            if candidates[..i].iter().any(|c| c.name == candidate.name) {
                return Err(err(format!(
                    "{} `{}`: the name `{}` is already used; candidate names must differ",
                    candidate.flag, candidate.name, candidate.name
                )));
            }
            if !candidate.bin.is_file() {
                return Err(err(format!(
                    "{}: no vector binary at {}",
                    candidate.flag,
                    candidate.bin.display()
                )));
            }
            if !candidate.config.is_file() {
                return Err(err(format!(
                    "{}: no config at {}",
                    candidate.flag,
                    candidate.config.display()
                )));
            }
        }

        let needing_schema: Vec<&str> = sinks
            .iter()
            .filter(|sink| sink.needs_schema())
            .map(|sink| sink.name())
            .collect();
        fake_uc::check_unique(&args.schemas).map_err(err)?;
        match (args.schemas.is_empty(), needing_schema.is_empty()) {
            (false, true) => {
                return Err(err(
                    "--schema is given, but no --sink reads a table schema; drop it",
                ));
            }
            (true, false) => {
                return Err(err(format!(
                    "--schema is required: sink(s) {} read a table schema from fake-uc",
                    needing_schema.join(", ")
                )));
            }
            _ => {}
        }

        check_run_args(
            args.iterations,
            args.items_per_sec,
            args.max_concurrency,
            REQUEST_TIMEOUT_SECS,
        )?;
        Ok(Self {
            corpus: args.corpus.clone(),
            sinks,
            candidates,
            schemas: args.schemas.clone(),
            iterations: args.iterations,
            items_per_sec: args.items_per_sec,
            max_concurrency: args.max_concurrency,
            work_dir: args.work_dir.clone(),
        })
    }
}

/// Where a sink's fake for a candidate records its output.
fn out_dir(work_dir: &Path, candidate: &str, sink: &dyn SinkKind) -> PathBuf {
    work_dir
        .join(candidate)
        .join(format!("{}-out", sink.name()))
}

/// The environment every candidate gets, whatever its sinks. Each sink adds its own.
fn common_env(data_dir: &Path, source: SocketAddr) -> Vec<(String, String)> {
    vec![
        (DATA_DIR_VAR.to_owned(), data_dir.display().to_string()),
        (SOURCE_ADDR_VAR.to_owned(), source.to_string()),
    ]
}

/// A candidate process this run started.
struct Running {
    name: String,
    /// `None` once the process has been waited for.
    child: Option<Child>,
    log: PathBuf,
    source: SocketAddr,
}

/// Everything a run has started, so that every exit path can stop it.
#[derive(Default)]
struct Session {
    candidates: Vec<Running>,
    fakes: Vec<RunningFake>,
}

pub async fn run(args: CheckArgs) -> CmdResult {
    let plan = Plan::from_args(&args)?;
    claim_empty_dir("--work-dir", &plan.work_dir)?;
    let corpus = Corpus::load(&plan.corpus).map_err(|e| err(format!("--corpus: {e}")))?;

    let mut session = Session::default();
    let outcome = tokio::select! {
        outcome = execute(&plan, &corpus, &mut session) => outcome,
        _ = tokio::signal::ctrl_c() => Err(err(
            "interrupted: candidates were sent SIGTERM and the fakes stopped; the work dir is kept",
        )),
    };
    teardown(&mut session).await;
    outcome
}

async fn execute(plan: &Plan, corpus: &Corpus, session: &mut Session) -> CmdResult {
    let codec = codec_named(&corpus.header.source_type)?;
    let schema_endpoint = if plan.schemas.is_empty() {
        None
    } else {
        let uc = fake_uc::start(FakeUcArgs {
            listen: EPHEMERAL_LOOPBACK,
            schemas: plan.schemas.clone(),
        })?;
        let addr = uc.addr;
        session.fakes.push(uc);
        Some(addr)
    };

    for candidate in &plan.candidates {
        let dir = plan.work_dir.join(&candidate.name);
        let data_dir = dir.join("data");
        std::fs::create_dir_all(&data_dir)
            .map_err(|e| err(format!("creating {}: {e}", data_dir.display())))?;
        let source = free_loopback_addr()?;
        let mut env = common_env(&data_dir, source);
        for sink in &plan.sinks {
            let setup = FakeSetup {
                out: out_dir(&plan.work_dir, &candidate.name, *sink),
                tls_dir: dir.join("tls"),
                schema_endpoint,
            };
            let fake = sink.start_fake(&setup)?;
            let fake_addr = fake.addr;
            session.fakes.push(fake);
            env.extend(
                sink.candidate_env(&setup, fake_addr)?
                    .into_iter()
                    .map(|(k, v)| (k.to_owned(), v)),
            );
        }

        let log = dir.join("vector.log");
        let mut child = spawn_candidate(
            &candidate.bin,
            &candidate.config,
            &env,
            &log,
            GRACEFUL_SHUTDOWN_SECS,
        )?;
        let ready = wait_for_listener(codec, source, &mut child, READY_TIMEOUT).await;
        session.candidates.push(Running {
            name: candidate.name.clone(),
            child: Some(child),
            log: log.clone(),
            source,
        });
        if let Err(e) = ready {
            return Err(err(format!(
                "candidate `{}` did not start listening: {e}\nlog {}:\n{}",
                candidate.name,
                log.display(),
                log_tail(&log)
            )));
        }
        eprintln!("check: candidate {} listening on {source}", candidate.name);
    }

    let specs: Vec<String> = session
        .candidates
        .iter()
        .map(|r| format!("{}={}", r.name, codec.target_url(r.source)))
        .collect();
    let targets: Vec<Target> = parse_targets(&specs)?;
    let sent_log = plan.work_dir.join("sent.ndjson");
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
    let first_day = chrono::Utc::now().date_naive();
    let summary = generate(corpus, identity, &targets, &sent_log, settings).await?;
    summary.print(&sent_log);

    // Each candidate flushes when SIGTERM stops its sources; a failure here means the outputs may
    // be incomplete, so it is an error, reported after verify has run on what there is.
    let mut problems = Vec::new();
    for running in &mut session.candidates {
        let Some(child) = running.child.as_mut() else {
            continue;
        };
        match terminate(child, FLUSH_TIMEOUT).await? {
            Stopped::Exited(status) if status.success() => {
                eprintln!("check: candidate {} exited: {status}", running.name);
            }
            Stopped::Exited(status) => problems.push(format!(
                "candidate `{}` exited with {status} after SIGTERM; log {}",
                running.name,
                running.log.display()
            )),
            Stopped::Killed => problems.push(format!(
                "candidate `{}` did not exit within {FLUSH_TIMEOUT:?} of SIGTERM and was killed, \
                 so its sinks may not have flushed; log {}",
                running.name,
                running.log.display()
            )),
        }
        // Vector exits 0 even when its shutdown gave up on components or a component dropped
        // events, so the log is the evidence. A drop shared by every candidate would not show
        // in verify.
        match logged_failure(&running.log) {
            Ok(Some(line)) => problems.push(format!(
                "candidate `{}` logged lost data, so its outputs may be incomplete: `{line}`; \
                 log {}",
                running.name,
                running.log.display()
            )),
            Ok(None) => {}
            Err(e) => problems.push(e.to_string()),
        }
    }
    for fake in session.fakes.drain(..) {
        if let Err(e) = fake.stop().await {
            problems.push(e.to_string());
        }
    }
    // A sink that renders a date into where it writes (e.g. `date=%F` in an S3 key prefix) takes
    // it from the event's timestamp, or from the clock when the event has none. Then rows the
    // candidates wrote either side of midnight can land under different dates, and differ by
    // destination only.
    let last_day = chrono::Utc::now().date_naive();
    if last_day != first_day {
        eprintln!(
            "check: warning: the run crossed midnight UTC ({first_day} to {last_day}); a sink that \
             puts the clock's date in its destination can make rows differ by destination alone"
        );
    }

    let mut outputs = Vec::new();
    for sink in &plan.sinks {
        for candidate in &plan.candidates {
            let dir = out_dir(&plan.work_dir, &candidate.name, *sink);
            outputs.push(format!(
                "{}={}:{}",
                candidate.name,
                sink.name(),
                dir.display()
            ));
        }
    }
    let verdict = verify(&sent_log, &outputs);

    eprintln!("check: work dir {}", plan.work_dir.display());
    for candidate in &plan.candidates {
        let dir = plan.work_dir.join(&candidate.name);
        eprintln!("check: {} ({}):", candidate.name, candidate.flag);
        eprintln!("check:   log      {}", dir.join("vector.log").display());
        eprintln!("check:   data     {}", dir.join("data").display());
        for sink in &plan.sinks {
            eprintln!(
                "check:   {:<8} {}",
                format!("{}:", sink.name()),
                out_dir(&plan.work_dir, &candidate.name, *sink).display()
            );
        }
    }

    let identical = verdict?;
    if !problems.is_empty() {
        return Err(err(problems.join("\n")));
    }
    if !summary.all_delivered() {
        eprintln!(
            "check: some requests were not accepted on every candidate (see gen above); exit 1"
        );
        return Ok(false);
    }
    Ok(identical)
}

/// Stops whatever is still running, after an error or Ctrl-C. Processes that already exited are
/// skipped, so a normal run finds nothing to do here.
async fn teardown(session: &mut Session) {
    for running in &mut session.candidates {
        if let Some(child) = running.child.as_mut()
            && let Err(e) = terminate(child, FLUSH_TIMEOUT).await
        {
            eprintln!("check: stopping candidate {}: {e}", running.name);
        }
    }
    for fake in session.fakes.drain(..) {
        if let Err(e) = fake.stop().await {
            eprintln!("check: {e}");
        }
    }
}

#[cfg(test)]
#[allow(unused_imports, dead_code)] // Bench builds compile this module but never run it.
mod tests {
    use super::*;
    use clap::Parser;
    use std::net::SocketAddr;

    #[derive(clap::Parser)]
    struct TestCli {
        #[command(flatten)]
        check: CheckArgs,
    }

    /// Creates a vector stand-in and a config under `dir` and returns their paths.
    fn files(dir: &Path) -> (PathBuf, PathBuf) {
        let bin = dir.join("vector");
        let config = dir.join("config.yaml");
        std::fs::write(&bin, b"").unwrap();
        std::fs::write(&config, b"").unwrap();
        (bin, config)
    }

    fn argv(dir: &Path, extra: &[&str]) -> Vec<String> {
        let (bin, config) = files(dir);
        let mut args = vec![
            "check".to_owned(),
            "--corpus".into(),
            "corpus.ndjson".into(),
            "--baseline".into(),
            format!("a={}:{}", bin.display(), config.display()),
            "--candidate".into(),
            format!("b={}:{}", bin.display(), config.display()),
            "--work-dir".into(),
            dir.join("work").display().to_string(),
        ];
        args.extend(extra.iter().map(|s| s.to_string()));
        args
    }

    fn plan_error(args: &[String]) -> String {
        let parsed = TestCli::try_parse_from(args).expect("parses");
        Plan::from_args(&parsed.check)
            .err()
            .expect("plan is refused")
            .to_string()
    }

    fn plan_ok(args: &[String]) {
        let parsed = TestCli::try_parse_from(args).expect("parses");
        if let Err(e) = Plan::from_args(&parsed.check) {
            panic!("plan refused: {e}");
        }
    }

    #[test]
    fn candidate_specs_split_name_binary_and_config() {
        let c = parse_candidate("--candidate", "b=/opt/v:x/vector:/cfg/b.yaml").unwrap();
        assert_eq!(c.name, "b");
        assert_eq!(c.bin, PathBuf::from("/opt/v:x/vector"));
        assert_eq!(c.config, PathBuf::from("/cfg/b.yaml"));
    }

    #[test]
    fn candidate_specs_name_their_flag_when_malformed() {
        for bad in [
            "nobinary",
            "a=/bin/vector",
            "a=:cfg",
            "a-b=/bin/v:/cfg",
            "=/bin/v:/cfg",
        ] {
            let error = parse_candidate("--baseline", bad).unwrap_err().to_string();
            assert!(error.contains("--baseline"), "{bad}: {error}");
        }
    }

    #[test]
    fn sinks_are_known_and_not_repeated() {
        let error = parse_sinks(&["gcs".into()]).err().unwrap().to_string();
        assert!(error.contains("--sink"), "{error}");
        assert!(error.contains("unknown sink `gcs`"), "{error}");
        let error = parse_sinks(&["s3".into(), "s3".into()])
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("`s3` is given more than once"), "{error}");
    }

    #[test]
    fn a_plan_accepts_a_well_formed_s3_run() {
        let dir = tempfile::tempdir().unwrap();
        plan_ok(&argv(dir.path(), &["--sink", "s3"]));
    }

    #[test]
    fn zerobus_needs_a_schema_and_only_zerobus_takes_one() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("table.json");
        std::fs::write(&file, b"{}").unwrap();
        let schema = format!("main.databricks_bench.output={}", file.display());

        let error = plan_error(&argv(dir.path(), &["--sink", "zerobus"]));
        assert!(error.contains("--schema is required"), "{error}");
        assert!(error.contains("zerobus"), "{error}");

        plan_ok(&argv(
            dir.path(),
            &["--sink", "zerobus", "--schema", &schema],
        ));

        let error = plan_error(&argv(dir.path(), &["--sink", "s3", "--schema", &schema]));
        assert!(error.contains("--schema is given"), "{error}");
    }

    #[test]
    fn each_schema_names_one_table_and_a_repeated_table_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("table.json");
        std::fs::write(&file, b"{}").unwrap();
        let first = format!("main.databricks_bench.output={}", file.display());
        let second = format!("main.databricks_bench.output2={}", file.display());
        plan_ok(&argv(
            dir.path(),
            &["--sink", "zerobus", "--schema", &first, "--schema", &second],
        ));

        let error = plan_error(&argv(
            dir.path(),
            &["--sink", "zerobus", "--schema", &first, "--schema", &first],
        ));
        assert!(
            error.contains("--schema main.databricks_bench.output="),
            "{error}"
        );
        assert!(error.contains("given more than once"), "{error}");
    }

    #[test]
    fn a_schema_file_that_is_missing_is_refused_at_parse_time() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("no-such.json");
        let spec = format!("main.databricks_bench.output={}", missing.display());
        let args = argv(dir.path(), &["--sink", "zerobus", "--schema", &spec]);
        let error = TestCli::try_parse_from(&args).err().unwrap().to_string();
        assert!(error.contains("--schema"), "{error}");
        assert!(error.contains(&missing.display().to_string()), "{error}");
    }

    #[test]
    fn candidate_names_must_differ() {
        let dir = tempfile::tempdir().unwrap();
        let (bin, config) = files(dir.path());
        let spec = format!("a={}:{}", bin.display(), config.display());
        let args = argv(dir.path(), &["--sink", "s3", "--candidate", &spec]);
        let error = plan_error(&args);
        assert!(error.contains("the name `a` is already used"), "{error}");
    }

    #[test]
    fn missing_binaries_and_configs_name_the_flag_and_path() {
        let dir = tempfile::tempdir().unwrap();
        let (_, config) = files(dir.path());
        let missing = dir.path().join("no-such-vector");
        let mut args = argv(dir.path(), &["--sink", "s3"]);
        let i = args.iter().position(|a| a == "--candidate").unwrap() + 1;
        args[i] = format!("b={}:{}", missing.display(), config.display());
        let error = plan_error(&args);
        assert!(
            error.starts_with("--candidate: no vector binary at"),
            "{error}"
        );
        assert!(error.contains(&missing.display().to_string()), "{error}");
    }

    #[test]
    fn iterations_are_checked_before_anything_starts() {
        let dir = tempfile::tempdir().unwrap();
        let error = plan_error(&argv(dir.path(), &["--sink", "s3", "--iterations", "0"]));
        assert!(error.contains("--iterations"), "{error}");
    }

    #[test]
    fn a_non_empty_work_dir_is_refused_and_kept() {
        let dir = tempfile::tempdir().unwrap();
        let work = dir.path().join("work");
        std::fs::create_dir(&work).unwrap();
        std::fs::write(work.join("keep.txt"), b"x").unwrap();
        let error = claim_empty_dir("--work-dir", &work)
            .unwrap_err()
            .to_string();
        assert!(error.contains("--work-dir"), "{error}");
        assert!(error.contains(&work.display().to_string()), "{error}");
        assert!(work.join("keep.txt").exists());
    }

    #[test]
    fn common_env_names_the_source_and_data_dir() {
        let source: SocketAddr = "127.0.0.1:17001".parse().unwrap();
        let env = common_env(Path::new("/w/a/data"), source);
        assert_eq!(
            env,
            vec![
                (DATA_DIR_VAR.to_owned(), "/w/a/data".to_owned()),
                (SOURCE_ADDR_VAR.to_owned(), "127.0.0.1:17001".to_owned()),
            ]
        );
    }

    fn lookup(env: &[(String, String)], name: &str) -> Option<String> {
        env.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone())
    }

    #[test]
    fn s3_sets_its_endpoint_and_dummy_credentials_only() {
        let addr: SocketAddr = "127.0.0.1:17002".parse().unwrap();
        let setup = FakeSetup {
            out: PathBuf::from("/w/a/s3-out"),
            tls_dir: PathBuf::from("/w/a/tls"),
            schema_endpoint: None,
        };
        let env: Vec<(String, String)> = sink_named("s3")
            .unwrap()
            .candidate_env(&setup, addr)
            .unwrap()
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v))
            .collect();
        assert_eq!(
            lookup(&env, "DATABRICKS_BENCH_S3_ENDPOINT").as_deref(),
            Some("http://127.0.0.1:17002")
        );
        assert_eq!(
            lookup(&env, "AWS_EC2_METADATA_DISABLED").as_deref(),
            Some("true")
        );
        assert!(lookup(&env, "AWS_ACCESS_KEY_ID").is_some());
        assert_eq!(lookup(&env, "DATABRICKS_BENCH_ZEROBUS_TABLE"), None);
    }

    #[test]
    fn zerobus_sets_its_endpoints_table_and_ca_and_needs_fake_uc() {
        let addr: SocketAddr = "127.0.0.1:17002".parse().unwrap();
        let mut setup = FakeSetup {
            out: PathBuf::from("/w/a/zerobus-out"),
            tls_dir: PathBuf::from("/w/a/tls"),
            schema_endpoint: None,
        };
        let zerobus = sink_named("zerobus").unwrap();
        assert!(
            zerobus.candidate_env(&setup, addr).is_err(),
            "needs fake-uc"
        );

        setup.schema_endpoint = Some("127.0.0.1:17003".parse().unwrap());
        let env: Vec<(String, String)> = zerobus
            .candidate_env(&setup, addr)
            .unwrap()
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v))
            .collect();
        assert_eq!(
            lookup(&env, "DATABRICKS_BENCH_ZEROBUS_INGEST_ENDPOINT").as_deref(),
            Some("https://127.0.0.1:17002")
        );
        assert_eq!(
            lookup(&env, "DATABRICKS_BENCH_ZEROBUS_UC_ENDPOINT").as_deref(),
            Some("http://127.0.0.1:17003")
        );
        assert_eq!(
            lookup(&env, "DATABRICKS_BENCH_ZEROBUS_TABLE").as_deref(),
            Some("main.databricks_bench.output")
        );
        assert_eq!(
            lookup(&env, "SSL_CERT_FILE").as_deref(),
            Some("/w/a/tls/ca.pem")
        );
        assert_eq!(lookup(&env, "DATABRICKS_BENCH_S3_ENDPOINT"), None);
    }
}
