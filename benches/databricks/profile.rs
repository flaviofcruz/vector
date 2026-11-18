//! `profile`: where a live Vector pod's CPU goes, as a short summary for choosing optimizations.
//!
//! Takes the same kube arguments as `capture`. It checks the pod (`kubectl get`, `kubectl exec`
//! running `cat`/`getconf`/`vector --version`) and profiles it with one of three methods.
//!
//! `--method platform` (the default) runs the platform's ad-hoc Rust profiler,
//! `//common/profiling/rust/cpu:profiler` in a Universe checkout, against the Vector process. That
//! tool is not read-only: it sets three perf sysctls on the pod's node over kssh and restores them
//! on exit, copies its helpers into a run directory in the container and removes it afterwards,
//! and needs perf in the image (without it, it builds perf inside the pod). It is run with
//! `--format flamegraph`, the one format that unwinds with DWARF: Vector is built without frame
//! pointers, so the default pprof format (frame-pointer stacks) would be useless. That format
//! returns an SVG, so the folded stacks are rebuilt from its frame boxes (`fg:x`/`fg:w`, in perf
//! sample weight); frames narrower than the SVG's minimum width are folded into their parent.
//! The tool needs an SSH agent and node SSH (kssh), which an Arca box lacks today (PLAT-150971).
//!
//! `--method perf-user` needs no node access: it runs the image's own perf on the Vector process
//! with `cpu-clock:u` (user space only, which the node's default `perf_event_paranoid=2` allows)
//! and DWARF stacks, in a `/tmp` directory it removes afterwards, then folds `perf script` locally
//! and demangles it with binutils' `c++filt`. Kernel CPU is not in its stacks, so its header gives
//! the process's user and kernel CPU over the run from `/proc/<pid>/stat`.
//!
//! `--method counters` only reads: no stack sampler is involved. It attributes the process's CPU
//! with Vector's own counters over `--duration-secs`:
//! - the process's total CPU, from `/proc/<pid>/stat` of the Vector process;
//! - each S3 sink's `component_encode_cpu_seconds` (thread CPU of `encode_events`: JSON
//!   serialization plus compression, which it cannot split);
//! - each Zerobus sink's `zerobus_encoding_seconds` (time of its CPU-bound Arrow/VARIANT encode on a
//!   blocking thread; it counts preemption too, so it can exceed CPU on a saturated pod);
//! - the rest is unattributed: source decode, `Metric` construction, `metric_to_log_hydra`,
//!   fan-out, network and TLS, the runtime, and allocation outside the encoders.
//!
//! That attribution is written as folded stacks, one frame path per component, and summarized by
//! the same code as a real profile. `--folded` summarizes an existing folded-stacks file or a
//! flamegraph SVG (for example the platform profiler's), with buckets matched by module path.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use serde::Serialize;

use super::capture::{exporter_path, label, parse_exposition};
use super::out_dir::claim_empty_dir;
use super::{CmdResult, err, io_at, now_ms};

/// Thread CPU of each S3-style sink's `encode_events` (`sinks::util::builder`).
const ENCODE_CPU_SUM: &str = "vector_component_encode_cpu_seconds_sum";
/// Time of each Zerobus sink's Arrow/VARIANT encode (`sinks::databricks_zerobus::encoding`).
const ZEROBUS_ENCODING_SUM: &str = "vector_zerobus_encoding_seconds_sum";
const RECEIVED_EVENTS: &str = "vector_component_received_events_total";
const UTILIZATION: &str = "vector_utilization";
/// The source type whose received events are the pipeline's items.
const ITEM_SOURCE_TYPE: &str = "prometheus_remote_write";
const UNATTRIBUTED_FRAME: &str = "[unattributed: source decode, Metric construction, metric_to_log_hydra, fan-out, network/TLS, runtime]";

/// How a live pod is profiled.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Method {
    /// The platform's ad-hoc Rust profiler (perf, DWARF stacks): function-level.
    Platform,
    /// Vector's own counters and /proc: component-level, read-only.
    Counters,
    /// The pod's own perf, user-space DWARF stacks only: function-level, no node change.
    PerfUser,
}

/// The platform profiler records for at most this long.
const PLATFORM_MAX_SECS: u64 = 60;
const PLATFORM_DEFAULT_SECS: u64 = 30;
/// `:u` samples user space only, which the node's default `perf_event_paranoid=2` allows, so no
/// sysctl changes. 99 Hz and 64 KB DWARF stacks, as the platform profiler records.
const PERF_USER_RECORD_ARGS: &str = "-e cpu-clock:u -F 99 --call-graph dwarf,64000";
const PERF_USER_DEFAULT_SECS: u64 = 30;
/// DWARF samples are about 64 KB each, so a long run on a busy pod can fill its `/tmp`.
const PERF_USER_MAX_SECS: u64 = 60;
const COUNTERS_DEFAULT_SECS: u64 = 60;
const PLATFORM_PROFILER: &str = "//common/profiling/rust/cpu:profiler";

#[derive(clap::Args)]
pub struct ProfileArgs {
    /// kubectl context of the pod's cluster.
    #[arg(long, required_unless_present = "folded")]
    context: Option<String>,
    #[arg(long, required_unless_present = "folded")]
    namespace: Option<String>,
    /// Pod to profile. It must be Running with every container ready.
    #[arg(long, required_unless_present = "folded")]
    pod: Option<String>,
    /// Container running Vector.
    #[arg(long, required_unless_present = "folded")]
    container: Option<String>,
    /// The pod's exporter port, scraped through the API server's pod proxy.
    #[arg(long, default_value_t = 7777)]
    exporter_port: u16,
    /// Process name (`/proc/<pid>/comm`) of Vector inside the container.
    #[arg(long, default_value = "vector")]
    process: String,
    /// How to profile the pod: `platform` runs the platform's Rust profiler (perf; it changes the
    /// node's perf sysctls for the run), `counters` reads Vector's own counters, `perf-user` runs
    /// the pod's perf on user space only (no node change; kernel CPU is not in the stacks).
    #[arg(long, value_enum, default_value_t = Method::Platform)]
    method: Method,
    /// How long to measure: default 30 s for `platform` and `perf-user` (at most 60), 60 s for
    /// `counters`.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..=3600))]
    duration_secs: Option<u64>,
    /// Universe checkout to run the platform profiler from (`bazel run`). Defaults to
    /// `$UNIVERSE_ROOT`, else `~/universe`.
    #[arg(long)]
    universe: Option<PathBuf>,
    /// Summarize this file instead of profiling a pod: folded stacks (`frame;frame;leaf count` per
    /// line) or a flamegraph SVG (`.svg`, e.g. the platform profiler's). Takes no kube arguments.
    #[arg(long, conflicts_with_all = ["context", "namespace", "pod", "container", "method"])]
    folded: Option<PathBuf>,
    /// Rows in each top-functions table.
    #[arg(long, default_value_t = 20)]
    top: usize,
    /// Directory to write into; it must be missing or empty. Holds `profile.folded`,
    /// `summary.json` and `summary.md`; a `platform` run adds the tool's `platform.svg` and
    /// `platform-profiler.log`, a `perf-user` run `perf-record.log`, a `counters` run the two
    /// exporter scrapes.
    #[arg(long)]
    out: PathBuf,
}

pub fn profile(args: ProfileArgs) -> CmdResult {
    claim_empty_dir("--out", &args.out)?;
    let (header, stacks, live) = match &args.folded {
        Some(path) => {
            let text = io_at(std::fs::read_to_string(path), "reading --folded", path)?;
            let svg = is_svg(path, &text);
            let stacks = if svg {
                folded_from_flamegraph_svg(&text)
            } else {
                parse_folded(&text)
            }
            .map_err(|e| err(format!("--folded {}: {e}", path.display())))?;
            let header = Header {
                method: "folded".into(),
                description: format!(
                    "summary of the {} {}",
                    if svg {
                        "flamegraph SVG"
                    } else {
                        "folded-stacks file"
                    },
                    path.display()
                ),
                unit: if svg { "perf sample weight" } else { "samples" }.into(),
                samples: stacks.values().sum(),
                ..Header::empty()
            };
            (header, stacks, None)
        }
        None if args.method == Method::Platform => {
            let pod = PodRef::from_args(&args)?;
            let (header, stacks) = profile_platform(&pod, &args)?;
            (header, stacks, None)
        }
        None if args.method == Method::PerfUser => {
            let pod = PodRef::from_args(&args)?;
            let (header, stacks) = profile_perf_user(&pod, &args)?;
            (header, stacks, None)
        }
        None => {
            let pod = PodRef::from_args(&args)?;
            let (header, live) = profile_live(&pod, &args)?;
            let stacks = live_stacks(&live);
            (
                Header {
                    samples: stacks.values().sum(),
                    ..header
                },
                stacks,
                Some(live),
            )
        }
    };
    if stacks.is_empty() {
        return Err(err("the profile has no samples"));
    }
    let summary = Summary {
        header,
        total: stacks.values().sum(),
        top_self: top_frames(&stacks, args.top, false),
        top_inclusive: top_frames(&stacks, args.top, true),
        buckets: buckets(&stacks),
        live,
    };
    write_out(&args.out, "profile.folded", &folded_text(&stacks))?;
    write_out(
        &args.out,
        "summary.json",
        &(serde_json::to_string_pretty(&summary)? + "\n"),
    )?;
    write_out(&args.out, "summary.md", &summary_markdown(&summary))?;
    eprintln!(
        "profile: {} ({} {}) -> {}",
        summary.header.description,
        summary.total,
        summary.header.unit,
        args.out.display()
    );
    Ok(true)
}

fn write_out(dir: &Path, name: &str, contents: &str) -> CmdResult {
    let path = dir.join(name);
    io_at(std::fs::write(&path, contents), "writing", &path)?;
    Ok(true)
}

/// How the profile was taken, like `capture`'s corpus header.
#[derive(Debug, Serialize)]
struct Header {
    /// `platform` or `internal-metrics` (a live pod), or `folded` (a file).
    method: String,
    description: String,
    /// Unit of every count: `perf sample weight` (platform: perf's `cpu-clock` period, in ns, per
    /// sample), `cpu-microseconds` (counters) or `samples` (folded).
    unit: String,
    /// `<context>/<namespace>/<pod>/<container>`.
    origin: Option<String>,
    image: Option<String>,
    /// Vector's `--version` line.
    version: Option<String>,
    pid: Option<u32>,
    duration_secs: Option<f64>,
    /// Sum of every stack's count.
    samples: u64,
    /// The samples perf recorded, as it reported them (platform only).
    perf_samples: Option<u64>,
    /// The command that took the profile (platform and perf-user).
    command: Option<String>,
    /// The process's user and kernel CPU over the run, from `/proc/<pid>/stat` utime and stime
    /// (perf-user only: its stacks show the user part only).
    user_cpu_secs: Option<f64>,
    kernel_cpu_secs: Option<f64>,
    taken_at_ms: i64,
}

impl Header {
    fn empty() -> Self {
        Self {
            method: String::new(),
            description: String::new(),
            unit: String::new(),
            origin: None,
            image: None,
            version: None,
            pid: None,
            duration_secs: None,
            samples: 0,
            perf_samples: None,
            command: None,
            user_cpu_secs: None,
            kernel_cpu_secs: None,
            taken_at_ms: now_ms(),
        }
    }
}

#[derive(Debug, Serialize)]
struct Summary {
    header: Header,
    total: u64,
    top_self: Vec<FrameCost>,
    top_inclusive: Vec<FrameCost>,
    buckets: Vec<BucketCost>,
    /// The counters behind a live run's stacks.
    live: Option<Live>,
}

#[derive(Debug, PartialEq, Serialize)]
struct FrameCost {
    frame: String,
    count: u64,
    share: f64,
}

#[derive(Debug, PartialEq, Serialize)]
struct BucketCost {
    bucket: &'static str,
    count: u64,
    share: f64,
}

// ---------------------------------------------------------------------------------------------
// Folded stacks and the summary.

/// Count per frame path (root first, `;`-joined, as in the folded format).
type Stacks = BTreeMap<String, u64>;

/// Parses `root;...;leaf count` lines, merging repeated paths. Blank lines are skipped.
fn parse_folded(text: &str) -> Result<Stacks, String> {
    let mut stacks = Stacks::new();
    for (n, line) in text.lines().enumerate() {
        let line = line.trim_end();
        if line.trim().is_empty() {
            continue;
        }
        let (path, count) = line
            .rsplit_once(' ')
            .ok_or_else(|| format!("line {}: no count after the stack", n + 1))?;
        let count: u64 = count
            .parse()
            .map_err(|_| format!("line {}: bad count `{count}`", n + 1))?;
        if path.is_empty() {
            return Err(format!("line {}: empty stack", n + 1));
        }
        *stacks.entry(path.to_owned()).or_default() += count;
    }
    Ok(stacks)
}

fn folded_text(stacks: &Stacks) -> String {
    stacks
        .iter()
        .filter(|(_, count)| **count > 0)
        .map(|(path, count)| format!("{path} {count}\n"))
        .collect()
}

fn share(count: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        count as f64 / total as f64
    }
}

/// The `top` frames by self cost (as the leaf) or inclusive cost (anywhere in the stack, counted
/// once per stack even when a recursive frame repeats). Ties sort by name.
fn top_frames(stacks: &Stacks, top: usize, inclusive: bool) -> Vec<FrameCost> {
    let total: u64 = stacks.values().sum();
    let mut costs: HashMap<&str, u64> = HashMap::new();
    for (path, count) in stacks {
        if inclusive {
            let unique: HashSet<&str> = path.split(';').collect();
            for frame in unique {
                *costs.entry(frame).or_default() += count;
            }
        } else if let Some(leaf) = path.rsplit(';').next() {
            *costs.entry(leaf).or_default() += count;
        }
    }
    let mut costs: Vec<_> = costs.into_iter().collect();
    costs.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
    costs
        .into_iter()
        .take(top)
        .map(|(frame, count)| FrameCost {
            frame: frame.to_owned(),
            count,
            share: share(count, total),
        })
        .collect()
}

/// Where a bucket's patterns are looked for.
#[derive(Clone, Copy)]
enum Scope {
    /// Only the leaf frame: work done right there, whoever called it.
    Leaf,
    /// Any frame of the stack: everything done on behalf of that code.
    Stack,
}

struct Bucket {
    name: &'static str,
    scope: Scope,
    /// Substrings of a frame (a demangled symbol, e.g. `vector::sinks::aws_s3::...`).
    patterns: &'static [&'static str],
}

/// The buckets, tried in order; a stack goes to the first that matches. Leaf buckets come first
/// so that compression and allocation are counted wherever they are called from; then the
/// encoders before the sinks that call them, VARIANT before Arrow (VARIANT labels are encoded
/// inside the Arrow batch, with `serde_json`), and the transform before `Metric` code and the
/// `serde_json` hashing it calls. Network, TLS and syscalls, and the async runtime, go by the leaf:
/// a server's tasks (the source runs inside hyper's) and the runtime's workers are at the root of
/// whole stacks of other work. Field lookups come before the encoders that do them per column.
const BUCKETS: &[Bucket] = &[
    Bucket {
        name: "zstd compression",
        scope: Scope::Leaf,
        patterns: &["zstd", "ZSTD_"],
    },
    Bucket {
        name: "allocation",
        scope: Scope::Leaf,
        patterns: &[
            "malloc",
            "calloc",
            "realloc",
            "free",
            "_rjem_",
            "jemalloc",
            "__rust_alloc",
            "__rust_dealloc",
            "__rust_realloc",
            "alloc::alloc::",
            "sdallocx",
            "mallocx",
            "rallocx",
        ],
    },
    Bucket {
        name: "network and TLS",
        scope: Scope::Leaf,
        patterns: &[
            "rustls::",
            "aws_lc",
            "ring::",
            "hyper::",
            "h2::",
            "tonic::",
            "tokio::net",
            "mio::",
            "std::net",
            // Kernel frames (perf shows them with kptr_restrict=0).
            "tcp_",
            "sock_",
            "__sys_sendto",
            "__sys_recvfrom",
            "__x64_sys_sendto",
            "__x64_sys_recvfrom",
            "__x64_sys_write",
            "__x64_sys_read",
        ],
    },
    Bucket {
        name: "field lookup",
        scope: Scope::Stack,
        patterns: &[
            "vrl::path::",
            "lookup_v2",
            "event::util::log::get",
            "LogEvent::get",
        ],
    },
    Bucket {
        name: "VARIANT encoding",
        scope: Scope::Stack,
        patterns: &["parquet_variant", "::variant::"],
    },
    Bucket {
        name: "Arrow arrow_stream encoding",
        scope: Scope::Stack,
        patterns: &[
            "codecs::encoding::format::arrow",
            "arrow_stream",
            "arrow_ipc::",
            "arrow_array::",
            "arrow_buffer::",
            "arrow_schema::",
            "arrow_data::",
            "arrow_select::",
            "arrow::",
        ],
    },
    Bucket {
        name: "Zerobus sink, not split further",
        scope: Scope::Stack,
        patterns: &["sinks::databricks_zerobus", "zerobus"],
    },
    Bucket {
        name: "metric_to_log_hydra",
        scope: Scope::Stack,
        patterns: &["transforms::metric_to_log_hydra"],
    },
    Bucket {
        name: "S3 JSON serialization",
        scope: Scope::Stack,
        patterns: &["codecs::encoding::format::json", "serde_json::"],
    },
    Bucket {
        name: "S3 sink, not split further",
        scope: Scope::Stack,
        patterns: &["sinks::aws_s3", "sinks::s3_common"],
    },
    Bucket {
        name: "Metric construction",
        scope: Scope::Stack,
        patterns: &["event::metric::"],
    },
    Bucket {
        name: "remote-write decode",
        scope: Scope::Stack,
        patterns: &[
            "sources::prometheus",
            "prometheus_parser",
            "prost::",
            "snap::",
        ],
    },
    Bucket {
        name: "event cloning and fan-out",
        scope: Scope::Stack,
        patterns: &["fanout::", "core::clone::Clone>::clone", "EventArray"],
    },
    Bucket {
        name: "async runtime",
        scope: Scope::Leaf,
        patterns: &[
            "tokio::runtime",
            "parking_lot",
            "std::thread",
            "futex",
            "epoll_wait",
        ],
    },
];
const OTHER_BUCKET: &str = "other / unattributed";

/// Whether `pattern` is in `frame`. A module path (`a::b`) must start a path segment, so that
/// `ring::` does not match `alloc::string::String`.
fn frame_has(frame: &str, pattern: &str) -> bool {
    if pattern.starts_with(':') || !pattern.contains("::") {
        return frame.contains(pattern);
    }
    frame
        .match_indices(pattern)
        .any(|(i, _)| !frame[..i].ends_with(|c: char| c.is_alphanumeric() || c == '_'))
}

fn bucket_of(path: &str) -> &'static str {
    let leaf = path.rsplit(';').next().unwrap_or(path);
    let matches = |frame: &str, b: &Bucket| b.patterns.iter().any(|p| frame_has(frame, p));
    BUCKETS
        .iter()
        .find(|b| match b.scope {
            Scope::Leaf => matches(leaf, b),
            Scope::Stack => path.split(';').any(|frame| matches(frame, b)),
        })
        .map_or(OTHER_BUCKET, |b| b.name)
}

/// Every bucket's cost, in `BUCKETS` order, then the rest; empty buckets are kept so that a
/// missing cost is visible.
fn buckets(stacks: &Stacks) -> Vec<BucketCost> {
    let total: u64 = stacks.values().sum();
    let mut costs: HashMap<&str, u64> = HashMap::new();
    for (path, count) in stacks {
        *costs.entry(bucket_of(path)).or_default() += count;
    }
    BUCKETS
        .iter()
        .map(|b| b.name)
        .chain([OTHER_BUCKET])
        .map(|bucket| {
            let count = costs.get(bucket).copied().unwrap_or(0);
            BucketCost {
                bucket,
                count,
                share: share(count, total),
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// Live run.

struct PodRef {
    context: String,
    namespace: String,
    pod: String,
    container: String,
}

impl PodRef {
    fn from_args(args: &ProfileArgs) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let need = |v: &Option<String>, flag: &str| {
            v.clone()
                .ok_or_else(|| err(format!("{flag} is required without --folded")))
        };
        Ok(Self {
            context: need(&args.context, "--context")?,
            namespace: need(&args.namespace, "--namespace")?,
            pod: need(&args.pod, "--pod")?,
            container: need(&args.container, "--container")?,
        })
    }

    fn origin(&self) -> String {
        format!(
            "{}/{}/{}/{}",
            self.context, self.namespace, self.pod, self.container
        )
    }

    fn kubectl(&self) -> Command {
        let mut cmd = Command::new("kubectl");
        cmd.args(["--context", &self.context, "--namespace", &self.namespace]);
        cmd
    }

    /// Runs a command in the container and returns its stdout. Only perf-user writes (its own
    /// `/tmp` directory); every other command reads.
    fn exec(&self, argv: &[&str]) -> Result<String, String> {
        let out = self
            .kubectl()
            .args(["exec", &self.pod, "--container", &self.container, "--"])
            .args(argv)
            .output()
            .map_err(|e| format!("running kubectl: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "kubectl exec … {} failed ({}): {}",
                argv.join(" "),
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }
}

/// The pod's state that must not change during a run.
#[derive(Debug, PartialEq)]
struct PodState {
    image: String,
    restarts: u64,
}

/// Fails unless the pod is Running with every container ready, and returns the container's image
/// and restart count.
fn pod_state(pod: &PodRef) -> Result<PodState, String> {
    let out = pod
        .kubectl()
        .args(["get", "pod", &pod.pod, "--output", "json"])
        .output()
        .map_err(|e| format!("running kubectl: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "kubectl get pod {} failed ({}): {}",
            pod.pod,
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    let json: serde_json::Value = serde_json::from_slice(&out.stdout)
        .map_err(|e| format!("kubectl get pod {}: bad JSON: {e}", pod.pod))?;
    parse_pod_state(&json, &pod.container).map_err(|e| format!("pod {}: {e}", pod.pod))
}

fn parse_pod_state(pod: &serde_json::Value, container: &str) -> Result<PodState, String> {
    let phase = pod["status"]["phase"].as_str().unwrap_or("unknown");
    if phase != "Running" {
        return Err(format!(
            "is {phase}, not Running; pick a Running, ready pod"
        ));
    }
    let statuses = pod["status"]["containerStatuses"]
        .as_array()
        .ok_or("has no container statuses")?;
    if let Some(unready) = statuses.iter().find(|c| c["ready"] != true) {
        return Err(format!(
            "container {} is not ready; pick a Running, ready pod",
            unready["name"]
        ));
    }
    let status = statuses
        .iter()
        .find(|c| c["name"] == container)
        .ok_or_else(|| format!("has no container `{container}`"))?;
    Ok(PodState {
        image: status["image"].as_str().unwrap_or_default().to_owned(),
        restarts: status["restartCount"].as_u64().unwrap_or(0),
    })
}

/// The one process named `name`, from `/proc/<pid>/comm` in the container.
fn find_pid(pod: &PodRef, name: &str) -> Result<u32, String> {
    let script = format!(
        "for p in /proc/[0-9]*; do [ \"$(cat $p/comm 2>/dev/null)\" = '{name}' ] && echo ${{p#/proc/}}; done; true"
    );
    let out = pod.exec(&["sh", "-c", &script])?;
    let pids: Vec<u32> = out.lines().filter_map(|l| l.trim().parse().ok()).collect();
    match pids.as_slice() {
        [pid] => Ok(*pid),
        [] => Err(format!(
            "no process named `{name}` in the container; check --container and --process"
        )),
        many => Err(format!(
            "{} processes named `{name}` ({many:?}); profile needs exactly one",
            many.len()
        )),
    }
}

/// One reading of `/proc/<pid>/stat`.
#[derive(Debug, PartialEq)]
struct ProcStat {
    /// User plus system time, in clock ticks.
    cpu_ticks: u64,
    /// User time alone, in clock ticks.
    user_ticks: u64,
    /// Start time since boot, in clock ticks: a restarted process has a new one.
    start_ticks: u64,
    threads: u64,
}

/// Parses `/proc/<pid>/stat`. The command name (field 2) may hold spaces and parentheses, so the
/// fields are counted from its last `)`.
fn parse_proc_stat(text: &str) -> Result<ProcStat, String> {
    let rest = text
        .rsplit_once(')')
        .ok_or("no `)` after the command name")?
        .1;
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // `rest` starts at field 3 (state), so field N is fields[N - 3].
    let field = |n: usize| -> Result<u64, String> {
        fields
            .get(n - 3)
            .ok_or_else(|| format!("no field {n}"))?
            .parse()
            .map_err(|_| format!("field {n} is not a number"))
    };
    Ok(ProcStat {
        cpu_ticks: field(14)? + field(15)?,
        user_ticks: field(14)?,
        start_ticks: field(22)?,
        threads: field(20)?,
    })
}

/// `/proc/<pid>/stat`'s clock ticks per second in the container.
fn clock_ticks_per_sec(pod: &PodRef) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
    pod.exec(&["getconf", "CLK_TCK"])
        .map_err(err)?
        .trim()
        .parse()
        .map_err(|_| err("getconf CLK_TCK did not print a number"))
}

fn read_proc_stat(pod: &PodRef, pid: u32) -> Result<ProcStat, String> {
    let text = pod.exec(&["cat", &format!("/proc/{pid}/stat")])?;
    parse_proc_stat(&text).map_err(|e| format!("/proc/{pid}/stat: {e}"))
}

/// What the live counters showed over the run.
#[derive(Debug, Default, Serialize)]
struct Live {
    /// Process CPU over the run, in seconds.
    process_cpu_secs: f64,
    /// Process CPU over wall time: the cores used on average.
    cores: f64,
    threads: u64,
    /// Events received by the `prometheus_remote_write` sources over the run.
    items: f64,
    /// Process CPU per 1,000 items, in milliseconds.
    cpu_ms_per_1k_items: Option<f64>,
    components: Vec<ComponentCost>,
    /// Each component's `utilization` gauge at the end: a busy fraction for context, not CPU (a
    /// concurrent transform such as metric_to_log_hydra runs on several workers at once).
    utilization: Vec<Utilization>,
}

#[derive(Debug, Serialize)]
struct Utilization {
    component_id: String,
    component_type: String,
    value: f64,
}

#[derive(Debug, Default, Serialize)]
struct ComponentCost {
    component_id: String,
    component_type: String,
    /// The counter the cost comes from.
    metric: String,
    /// Growth of that counter over the run, in seconds.
    secs: f64,
    /// Over the process's CPU.
    share_of_process: f64,
    ms_per_1k_items: Option<f64>,
}

/// The counters this module reads from one scrape: (metric, component_id, component_type) to
/// value, summed over the other labels. Utilization is kept apart since it is a gauge.
#[derive(Debug, Default)]
struct Counters {
    values: BTreeMap<(String, String, String), f64>,
    source_types: BTreeMap<String, String>,
}

fn parse_counters(text: &str) -> Result<Counters, String> {
    let mut counters = Counters::default();
    for r in parse_exposition(text)? {
        let wanted = [ENCODE_CPU_SUM, ZEROBUS_ENCODING_SUM, RECEIVED_EVENTS];
        if !wanted.contains(&r.name.as_str()) {
            continue;
        }
        let id = label(&r.labels, "component_id")
            .unwrap_or_default()
            .to_owned();
        let ty = label(&r.labels, "component_type")
            .unwrap_or_default()
            .to_owned();
        if r.name == RECEIVED_EVENTS && label(&r.labels, "component_kind") == Some("source") {
            counters.source_types.insert(id.clone(), ty.clone());
        }
        *counters.values.entry((r.name, id, ty)).or_default() += r.value;
    }
    Ok(counters)
}

/// The run's costs from the scrapes and process readings before and after. A counter that went
/// down or vanished means the process restarted, which is an error.
fn live_costs(
    before: &Counters,
    after: &Counters,
    cpu_secs: f64,
    wall_secs: f64,
    threads: u64,
) -> Result<Live, String> {
    for (key, was) in &before.values {
        match after.values.get(key) {
            Some(now) if now >= was => {}
            Some(now) => {
                return Err(format!(
                    "{}{{component_id=\"{}\"}} went from {was} to {now}: the process restarted; run it again",
                    key.0, key.1
                ));
            }
            None => {
                return Err(format!(
                    "{}{{component_id=\"{}\"}} disappeared: did the process restart?",
                    key.0, key.1
                ));
            }
        }
    }
    let grown = |key: &(String, String, String)| {
        after.values[key] - before.values.get(key).copied().unwrap_or(0.0)
    };
    let items: f64 = after
        .values
        .keys()
        .filter(|(name, _, ty)| name == RECEIVED_EVENTS && ty == ITEM_SOURCE_TYPE)
        .map(grown)
        .sum();
    let per_1k = |secs: f64| (items > 0.0).then(|| secs * 1000.0 / (items / 1000.0));
    let mut components: Vec<ComponentCost> = after
        .values
        .keys()
        .filter(|(name, _, _)| name == ENCODE_CPU_SUM || name == ZEROBUS_ENCODING_SUM)
        .map(|key| {
            let secs = grown(key);
            ComponentCost {
                component_id: key.1.clone(),
                component_type: key.2.clone(),
                metric: key.0.trim_start_matches("vector_").to_owned(),
                secs,
                share_of_process: if cpu_secs > 0.0 { secs / cpu_secs } else { 0.0 },
                ms_per_1k_items: per_1k(secs),
            }
        })
        .collect();
    components.sort_by(|a, b| b.secs.total_cmp(&a.secs));
    Ok(Live {
        process_cpu_secs: cpu_secs,
        cores: cpu_secs / wall_secs,
        threads,
        items,
        cpu_ms_per_1k_items: per_1k(cpu_secs),
        components,
        utilization: Vec::new(),
    })
}

/// The live costs as folded stacks in CPU microseconds: one stack per sink, under its module
/// path so the buckets match it, and the rest as one unattributed stack.
fn live_stacks(live: &Live) -> Stacks {
    let micros = |secs: f64| (secs * 1e6).round().max(0.0) as u64;
    let mut stacks = Stacks::new();
    let mut attributed = 0;
    for c in &live.components {
        let frames = match c.component_type.as_str() {
            "databricks_zerobus" => {
                "vector::sinks::databricks_zerobus;{id};vector::sinks::databricks_zerobus::encoding::timed_encoding"
            }
            "aws_s3" => "vector::sinks::aws_s3;{id};vector::sinks::util::builder::encode_events",
            _ => "vector::sinks::{type};{id};vector::sinks::util::builder::encode_events",
        };
        let path = format!(
            "vector;{}",
            frames
                .replace("{id}", &c.component_id)
                .replace("{type}", &c.component_type)
        );
        let us = micros(c.secs);
        attributed += us;
        *stacks.entry(path).or_default() += us;
    }
    // The Zerobus figure is wall time, so on a saturated pod the sum can pass the CPU total.
    let rest = micros(live.process_cpu_secs).saturating_sub(attributed);
    stacks.insert(format!("vector;{UNATTRIBUTED_FRAME}"), rest);
    stacks.retain(|_, us| *us > 0);
    stacks
}

// ---------------------------------------------------------------------------------------------
// Platform profiler.

/// The `bazel run` arguments for one platform-profiler run against `pid`, writing its SVG to `svg`.
/// `--format flamegraph` is the format that unwinds with DWARF (Vector has no frame pointers), and
/// `--pid` is needed because the tool defaults to pid 1, which in a VAM pod is the `sh` wrapper.
fn platform_command(pod: &PodRef, pid: u32, duration_secs: u64, svg: &Path) -> Vec<String> {
    let mut argv: Vec<String> = ["run", PLATFORM_PROFILER, "--"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    for (flag, value) in [
        ("--context", pod.context.clone()),
        ("--namespace", pod.namespace.clone()),
        ("--pod-name", pod.pod.clone()),
        ("--container", pod.container.clone()),
        ("--pid", pid.to_string()),
        ("--duration", duration_secs.to_string()),
        ("--format", "flamegraph".to_owned()),
        ("--output", svg.display().to_string()),
    ] {
        argv.push(flag.to_owned());
        argv.push(value);
    }
    argv
}

fn universe_dir(flag: &Option<PathBuf>) -> PathBuf {
    flag.clone()
        .or_else(|| std::env::var_os("UNIVERSE_ROOT").map(PathBuf::from))
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join("universe")))
        .unwrap_or_else(|| PathBuf::from("universe"))
}

/// The sample count perf printed (`[ perf record: Captured and wrote … (N samples) ]`), the last
/// one in the tool's log.
fn perf_samples_in_log(log: &str) -> Option<u64> {
    log.lines()
        .rev()
        .filter(|line| line.contains("perf record: Captured"))
        .filter_map(|line| {
            let before = line.rsplit_once(" samples)")?.0;
            let digits = before.rsplit_once('(')?.1;
            digits.replace(',', "").parse().ok()
        })
        .next()
}

fn profile_platform(
    pod: &PodRef,
    args: &ProfileArgs,
) -> Result<(Header, Stacks), Box<dyn std::error::Error + Send + Sync>> {
    let duration_secs = args.duration_secs.unwrap_or(PLATFORM_DEFAULT_SECS);
    if duration_secs > PLATFORM_MAX_SECS {
        return Err(err(format!(
            "--duration-secs {duration_secs}: the platform profiler records for at most {PLATFORM_MAX_SECS} s"
        )));
    }
    let universe = universe_dir(&args.universe);
    if !universe.join("common/profiling/rust/cpu").is_dir() {
        return Err(err(format!(
            "--universe {}: no common/profiling/rust/cpu here; point --universe (or UNIVERSE_ROOT) at a Universe checkout",
            universe.display()
        )));
    }
    let state = pod_state(pod).map_err(err)?;
    let pid = find_pid(pod, &args.process).map_err(err)?;
    let version = pod.exec(&["vector", "--version"]).map_err(err)?;
    let stat_before = read_proc_stat(pod, pid).map_err(err)?;

    let out = io_at(
        std::fs::canonicalize(&args.out),
        "resolving --out",
        &args.out,
    )?;
    let svg = out.join("platform.svg");
    let log_path = out.join("platform-profiler.log");
    let argv = platform_command(pod, pid, duration_secs, &svg);
    let command = format!("bazel {}", argv.join(" "));
    eprintln!(
        "profile: running the platform profiler on {} (pid {pid}, {}) for {duration_secs}s; it sets the node's perf sysctls for the run and restores them on exit. Log: {}",
        pod.origin(),
        version.trim(),
        log_path.display()
    );
    let log = io_at(std::fs::File::create(&log_path), "creating", &log_path)?;
    let log_err = io_at(log.try_clone(), "opening", &log_path)?;
    let status = Command::new("bazel")
        .args(&argv)
        .current_dir(&universe)
        .stdin(Stdio::null())
        .stdout(log)
        .stderr(log_err)
        .status()
        .map_err(|e| err(format!("running bazel in {}: {e}", universe.display())))?;
    let log_text = io_at(std::fs::read_to_string(&log_path), "reading", &log_path)?;
    if !status.success() {
        let tail: Vec<&str> = log_text.lines().rev().take(15).collect();
        return Err(err(format!(
            "the platform profiler failed ({status}); last lines of {}:\n{}",
            log_path.display(),
            tail.into_iter().rev().collect::<Vec<_>>().join("\n")
        )));
    }

    let stat_after = read_proc_stat(pod, pid).map_err(err)?;
    if stat_after.start_ticks != stat_before.start_ticks {
        return Err(err(format!(
            "process {pid} restarted during the run; run it again"
        )));
    }
    let state_after = pod_state(pod).map_err(err)?;
    if state_after != state {
        return Err(err(format!(
            "the pod changed during the run ({state:?} -> {state_after:?}); run it again"
        )));
    }
    let text = io_at(
        std::fs::read_to_string(&svg),
        "reading the profiler's",
        &svg,
    )?;
    let stacks =
        folded_from_flamegraph_svg(&text).map_err(|e| err(format!("{}: {e}", svg.display())))?;
    let header = Header {
        method: "platform".into(),
        description: format!(
            "perf CPU profile (cpu-clock, DWARF stacks) of {} by {PLATFORM_PROFILER}",
            pod.origin()
        ),
        unit: "perf sample weight".into(),
        origin: Some(pod.origin()),
        image: Some(state.image),
        version: Some(version.trim().to_owned()),
        pid: Some(pid),
        duration_secs: Some(duration_secs as f64),
        samples: stacks.values().sum(),
        perf_samples: perf_samples_in_log(&log_text),
        command: Some(command),
        ..Header::empty()
    };
    Ok((header, stacks))
}

// ---------------------------------------------------------------------------------------------
// The pod's perf, user space only.

/// Records the Vector process with the pod's own perf, user-space DWARF stacks only, in a fresh
/// `/tmp` directory that it removes again, and folds the stacks. The node's default
/// `perf_event_paranoid=2` allows that for the container's own process, so nothing on the node
/// changes; the price is that kernel CPU (syscalls, TCP, page faults) is not in the stacks, so the
/// header gives the process's user/kernel split over the run.
fn profile_perf_user(
    pod: &PodRef,
    args: &ProfileArgs,
) -> Result<(Header, Stacks), Box<dyn std::error::Error + Send + Sync>> {
    let duration_secs = args.duration_secs.unwrap_or(PERF_USER_DEFAULT_SECS);
    if duration_secs > PERF_USER_MAX_SECS {
        return Err(err(format!(
            "--duration-secs {duration_secs}: perf-user records for at most {PERF_USER_MAX_SECS} s"
        )));
    }
    let state = pod_state(pod).map_err(err)?;
    let pid = find_pid(pod, &args.process).map_err(err)?;
    let version = pod.exec(&["vector", "--version"]).map_err(err)?;
    let ticks_per_sec = clock_ticks_per_sec(pod)?;

    let dir = pod
        .exec(&["mktemp", "-d", "/tmp/databricks-bench-profile.XXXXXX"])
        .map_err(err)?
        .trim()
        .to_owned();
    let data = format!("{dir}/perf.data");
    let record =
        format!("perf record {PERF_USER_RECORD_ARGS} -p {pid} -o {data} -- sleep {duration_secs}");
    let script_argv = [
        "perf",
        "script",
        "-i",
        &data,
        "-F",
        "comm,ip,sym",
        "--no-inline",
    ];
    eprintln!(
        "profile: recording {} (pid {pid}, {}) for {duration_secs}s with the pod's perf, user space only, in {dir}",
        pod.origin(),
        version.trim()
    );
    let recorded = (|| {
        check_tmp_space(pod, &dir, duration_secs)?;
        let before = read_proc_stat(pod, pid)?;
        let started = Instant::now();
        // perf reports on stderr; keep it on success, and pass it on as the error on failure.
        let log = pod.exec(&[
            "sh",
            "-c",
            &format!(
                "{record} > {dir}/record.log 2>&1; s=$?; cat {dir}/record.log; [ $s = 0 ] || {{ cat {dir}/record.log >&2; exit $s; }}"
            ),
        ])?;
        let after = read_proc_stat(pod, pid)?;
        let wall_secs = started.elapsed().as_secs_f64();
        let script = pod.exec(&script_argv)?;
        Ok::<_, String>((before, after, wall_secs, log, script))
    })();
    let removed = pod.exec(&["rm", "-rf", &dir]);
    if let Err(e) = &removed {
        eprintln!(
            "profile: warning: could not remove {dir} from the pod ({e}); run `rm -rf {dir}` in the container"
        );
    }
    let (before, after, wall_secs, log, script) = recorded.map_err(err)?;
    removed.map_err(err)?;
    write_out(&args.out, "perf-record.log", &log)?;

    if after.start_ticks != before.start_ticks {
        return Err(err(format!(
            "process {pid} restarted during the run; run it again"
        )));
    }
    let state_after = pod_state(pod).map_err(err)?;
    if state_after != state {
        return Err(err(format!(
            "the pod changed during the run ({state:?} -> {state_after:?}); run it again"
        )));
    }
    let stacks = demangle(fold_perf_script(&script)).map_err(err)?;
    let user_ticks = after.user_ticks.saturating_sub(before.user_ticks);
    let kernel_ticks =
        (after.cpu_ticks - after.user_ticks).saturating_sub(before.cpu_ticks - before.user_ticks);
    let header = Header {
        method: "perf-user".into(),
        description: format!(
            "perf CPU profile (cpu-clock:u, user space only, DWARF stacks) of {} by the pod's perf",
            pod.origin()
        ),
        unit: "samples".into(),
        origin: Some(pod.origin()),
        image: Some(state.image),
        version: Some(version.trim().to_owned()),
        pid: Some(pid),
        duration_secs: Some(wall_secs),
        samples: stacks.values().sum(),
        perf_samples: perf_samples_in_log(&log),
        command: Some(format!("{record}; {}", script_argv.join(" "))),
        user_cpu_secs: Some(user_ticks as f64 / ticks_per_sec),
        kernel_cpu_secs: Some(kernel_ticks as f64 / ticks_per_sec),
        ..Header::empty()
    };
    Ok((header, stacks))
}

/// Fails unless `dir` has room for the worst case: every CPU busy for the whole run, at 99 samples
/// per second of about 64 KB each.
fn check_tmp_space(pod: &PodRef, dir: &str, duration_secs: u64) -> Result<(), String> {
    let out = pod.exec(&["sh", "-c", &format!("nproc; df -Pk {dir} | tail -n 1")])?;
    let mut lines = out.lines();
    let cpus: u64 = lines
        .next()
        .and_then(|l| l.trim().parse().ok())
        .ok_or("nproc did not print a number")?;
    let free_kb: u64 = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(3)?.parse().ok())
        .ok_or_else(|| format!("df -Pk {dir} printed no free size"))?;
    let need_kb = cpus * 99 * 64 * duration_secs;
    if free_kb < need_kb {
        return Err(format!(
            "{dir} has {} MiB free, and {duration_secs} s on {cpus} CPUs can need {} MiB; use a shorter --duration-secs",
            free_kb / 1024,
            need_kb / 1024
        ));
    }
    Ok(())
}

/// Folds `perf script -F comm,ip,sym` output: per sample a line with the thread name, one indented
/// `<ip> <symbol>` line per frame, leaf first, and a blank line. The thread name is the root
/// frame, as in inferno's `collapse-perf`; each sample counts 1.
fn fold_perf_script(text: &str) -> Stacks {
    let mut stacks = Stacks::new();
    let mut comm: Option<&str> = None;
    let mut frames: Vec<&str> = Vec::new();
    for line in text.lines().chain([""]) {
        if line.trim().is_empty() {
            if let Some(comm) = comm.take() {
                let path: Vec<&str> = std::iter::once(comm)
                    .chain(frames.iter().rev().copied())
                    .collect();
                *stacks.entry(path.join(";")).or_default() += 1;
            }
            frames.clear();
        } else if line.starts_with(char::is_whitespace) {
            let symbol = line
                .trim()
                .split_once(' ')
                .map_or("[unknown]", |(_, s)| s.trim());
            frames.push(symbol);
        } else {
            comm = Some(line.trim());
        }
    }
    stacks
}

/// Demangles every frame with binutils' `c++filt` (perf in the image prints Rust symbols mangled)
/// and drops the crate hashes it keeps (`vector_core[e020ed14ce12883b]::`), so that the buckets'
/// module paths match.
fn demangle(stacks: Stacks) -> Result<Stacks, String> {
    use std::io::Write;
    let frames: Vec<&str> = stacks
        .keys()
        .flat_map(|path| path.split(';'))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let mut child = Command::new("c++filt")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("running c++filt (binutils) to demangle the symbols: {e}"))?;
    let mut stdin = child.stdin.take().expect("piped stdin");
    let input = frames.join("\n") + "\n";
    let writer = std::thread::spawn(move || stdin.write_all(input.as_bytes()));
    let out = child
        .wait_with_output()
        .map_err(|e| format!("running c++filt: {e}"))?;
    writer
        .join()
        .expect("c++filt writer panicked")
        .map_err(|e| format!("writing to c++filt: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "c++filt failed ({}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    let text = String::from_utf8_lossy(&out.stdout);
    let names: Vec<&str> = text.lines().collect();
    if names.len() != frames.len() {
        return Err(format!(
            "c++filt returned {} lines for {} symbols",
            names.len(),
            frames.len()
        ));
    }
    let names: HashMap<&str, String> = frames
        .into_iter()
        .zip(names.into_iter().map(without_crate_hashes))
        .collect();
    let mut demangled = Stacks::new();
    for (path, count) in &stacks {
        let path: Vec<&str> = path.split(';').map(|f| names[f].as_str()).collect();
        *demangled.entry(path.join(";")).or_default() += count;
    }
    Ok(demangled)
}

/// Drops `[<hex>]` crate hashes from a demangled symbol. `;` (in array types such as `[u8; 4]`)
/// would split a folded frame, so it becomes `,`.
fn without_crate_hashes(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut rest = name;
    while let Some(open) = rest.find('[') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        match after.find(']') {
            Some(close) if close >= 8 && after[..close].bytes().all(|b| b.is_ascii_hexdigit()) => {
                rest = &after[close + 1..];
            }
            _ => {
                out.push('[');
                rest = after;
            }
        }
    }
    out.push_str(rest);
    out.replace(';', ",")
}

// ---------------------------------------------------------------------------------------------
// Flamegraph SVG -> folded stacks.

/// Stands for top-level frames a flamegraph left out for being narrower than its minimum width.
const PRUNED_FRAME: &str = "[frames narrower than the flamegraph's minimum width]";

fn is_svg(path: &Path, text: &str) -> bool {
    path.extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("svg"))
        || text.trim_start().starts_with("<?xml")
        || text.trim_start().starts_with("<svg")
}

/// One frame box of an inferno flamegraph: `<g><title>NAME (N samples, P%)</title><rect … y=…
/// fg:x=… fg:w=…/>`. `fg:x`/`fg:w` are the box's start and width in sample weight.
struct FrameBox {
    name: String,
    y: i64,
    x: u64,
    w: u64,
}

fn xml_unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(amp) = rest.find('&') {
        out.push_str(&rest[..amp]);
        rest = &rest[amp..];
        let Some(semi) = rest.find(';') else { break };
        let entity = &rest[1..semi];
        let decoded = match entity {
            "lt" => Some('<'),
            "gt" => Some('>'),
            "amp" => Some('&'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            _ => entity
                .strip_prefix("#x")
                .and_then(|h| u32::from_str_radix(h, 16).ok())
                .or_else(|| entity.strip_prefix('#').and_then(|d| d.parse().ok()))
                .and_then(char::from_u32),
        };
        match decoded {
            Some(c) => {
                out.push(c);
                rest = &rest[semi + 1..];
            }
            None => {
                out.push('&');
                rest = &rest[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

fn attr<'a>(tag: &'a str, name: &str) -> Option<&'a str> {
    let key = format!(" {name}=\"");
    let start = tag.find(&key)? + key.len();
    let len = tag[start..].find('"')?;
    Some(&tag[start..start + len])
}

fn frame_boxes(svg: &str) -> Result<Vec<FrameBox>, String> {
    let mut boxes = Vec::new();
    for chunk in svg.split("<g><title>").skip(1) {
        let (title, rest) = chunk
            .split_once("</title>")
            .ok_or("a frame's <title> is not closed")?;
        let rect_start = rest.find("<rect").ok_or("a frame has no <rect>")?;
        let rect = &rest[rect_start
            ..rest[rect_start..]
                .find('>')
                .map_or(rest.len(), |e| rect_start + e)];
        let number = |name: &str| -> Result<u64, String> {
            attr(rect, name)
                .ok_or_else(|| {
                    format!("frame `{title}` has no {name}; the SVG needs inferno's fg:x/fg:w attributes")
                })?
                .parse()
                .map_err(|_| format!("frame `{title}` has a bad {name}"))
        };
        let title = xml_unescape(title);
        // `NAME (1,234 samples, 5.6%)`: the name may itself hold ` (`.
        let name = title
            .rsplit_once(" (")
            .map_or(title.as_str(), |(n, _)| n)
            .to_owned();
        let y: f64 = attr(rect, "y")
            .ok_or_else(|| format!("frame `{name}` has no y"))?
            .parse()
            .map_err(|_| format!("frame `{name}` has a bad y"))?;
        boxes.push(FrameBox {
            name,
            y: y.round() as i64,
            x: number("fg:x")?,
            w: number("fg:w")?,
        });
    }
    if boxes.is_empty() {
        return Err("no flamegraph frames (<g><title>…</title><rect …/>) found".into());
    }
    Ok(boxes)
}

/// Rebuilds folded stacks from a flamegraph's frame boxes. A box's depth is its row (roots at the
/// bottom, the largest y), its parent the box one row down whose span holds its start, and its self
/// weight its width less its children's. The synthetic `all` root is dropped. Frames the SVG left
/// out for being too narrow show up as self weight of their parent.
fn folded_from_flamegraph_svg(svg: &str) -> Result<Stacks, String> {
    let boxes = frame_boxes(svg)?;
    let mut rows: Vec<i64> = boxes.iter().map(|b| b.y).collect();
    rows.sort_unstable_by(|a, b| b.cmp(a));
    rows.dedup();
    let depth_of: HashMap<i64, usize> = rows.iter().enumerate().map(|(d, y)| (*y, d)).collect();
    // Per depth, box indices sorted by start.
    let mut by_depth: Vec<Vec<usize>> = vec![Vec::new(); rows.len()];
    for (i, b) in boxes.iter().enumerate() {
        by_depth[depth_of[&b.y]].push(i);
    }
    for row in &mut by_depth {
        row.sort_by_key(|&i| boxes[i].x);
    }
    let mut parent: Vec<Option<usize>> = vec![None; boxes.len()];
    let mut children_weight = vec![0u64; boxes.len()];
    for depth in 1..by_depth.len() {
        for &i in &by_depth[depth] {
            let below = &by_depth[depth - 1];
            let pos = below.partition_point(|&j| boxes[j].x <= boxes[i].x);
            let candidate = pos.checked_sub(1).map(|p| below[p]);
            let Some(p) = candidate.filter(|&p| boxes[i].x < boxes[p].x + boxes[p].w) else {
                return Err(format!(
                    "frame `{}` at row {depth} has no parent below it",
                    boxes[i].name
                ));
            };
            parent[i] = Some(p);
            children_weight[p] += boxes[i].w;
        }
    }
    let synthetic_root = |i: usize| parent[i].is_none() && boxes[i].name == "all";
    let mut stacks = Stacks::new();
    for i in 0..boxes.len() {
        let self_weight = boxes[i].w.saturating_sub(children_weight[i]);
        if synthetic_root(i) {
            // Weight of top-level frames too narrow to draw: keep it so the total stays exact.
            if self_weight > 0 {
                *stacks.entry(PRUNED_FRAME.to_owned()).or_default() += self_weight;
            }
            continue;
        }
        if self_weight == 0 {
            continue;
        }
        let mut frames = vec![boxes[i].name.as_str()];
        let mut at = parent[i];
        while let Some(p) = at {
            if !synthetic_root(p) {
                frames.push(boxes[p].name.as_str());
            }
            at = parent[p];
        }
        frames.reverse();
        *stacks.entry(frames.join(";")).or_default() += self_weight;
    }
    Ok(stacks)
}

fn scrape(pod: &PodRef, port: u16) -> Result<String, String> {
    let path = exporter_path(&pod.namespace, &pod.pod, port);
    let out = Command::new("kubectl")
        .args(["--context", &pod.context, "get", "--raw", &path])
        .output()
        .map_err(|e| format!("running kubectl: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "scraping {path} through the API server failed ({}): {}; the pod proxy needs `get pods/proxy` RBAC on the pod",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

fn profile_live(
    pod: &PodRef,
    args: &ProfileArgs,
) -> Result<(Header, Live), Box<dyn std::error::Error + Send + Sync>> {
    let duration_secs = args.duration_secs.unwrap_or(COUNTERS_DEFAULT_SECS);
    let state = pod_state(pod).map_err(err)?;
    let pid = find_pid(pod, &args.process).map_err(err)?;
    let version = pod.exec(&["vector", "--version"]).map_err(err)?;
    let ticks_per_sec = clock_ticks_per_sec(pod)?;
    eprintln!(
        "profile: measuring {} (pid {pid}, {}) for {}s",
        pod.origin(),
        version.trim(),
        duration_secs
    );

    let text_before = scrape(pod, args.exporter_port).map_err(err)?;
    let stat_before = read_proc_stat(pod, pid).map_err(err)?;
    let started = Instant::now();
    std::thread::sleep(Duration::from_secs(duration_secs));
    let stat_after = read_proc_stat(pod, pid).map_err(err)?;
    let wall_secs = started.elapsed().as_secs_f64();
    let text_after = scrape(pod, args.exporter_port).map_err(err)?;

    if stat_after.start_ticks != stat_before.start_ticks {
        return Err(err(format!(
            "process {pid} restarted during the run; run it again"
        )));
    }
    let state_after = pod_state(pod).map_err(err)?;
    if state_after != state {
        return Err(err(format!(
            "the pod changed during the run ({state:?} -> {state_after:?}); run it again"
        )));
    }
    write_out(&args.out, "metrics-before.prom", &text_before)?;
    write_out(&args.out, "metrics-after.prom", &text_after)?;

    let before = parse_counters(&text_before).map_err(err)?;
    let after = parse_counters(&text_after).map_err(err)?;
    let cpu_secs = (stat_after.cpu_ticks - stat_before.cpu_ticks) as f64 / ticks_per_sec;
    let mut live =
        live_costs(&before, &after, cpu_secs, wall_secs, stat_after.threads).map_err(err)?;
    live.utilization = utilization(&text_after)?;
    if live.items == 0.0 {
        eprintln!(
            "profile: warning: no {ITEM_SOURCE_TYPE} source received events during the run; is the pod receiving traffic?"
        );
    }
    let header = Header {
        method: "internal-metrics".into(),
        description: format!(
            "component-level CPU attribution of {} from /proc and Vector's own counters",
            pod.origin()
        ),
        unit: "cpu-microseconds".into(),
        origin: Some(pod.origin()),
        image: Some(state.image),
        version: Some(version.trim().to_owned()),
        pid: Some(pid),
        duration_secs: Some(wall_secs),
        ..Header::empty()
    };
    Ok((header, live))
}

/// The non-zero `utilization` gauges of one scrape, busiest first.
fn utilization(text: &str) -> Result<Vec<Utilization>, Box<dyn std::error::Error + Send + Sync>> {
    let mut rows: Vec<Utilization> = parse_exposition(text)
        .map_err(err)?
        .into_iter()
        .filter(|r| r.name == UTILIZATION && r.value > 0.0)
        .map(|r| Utilization {
            component_id: label(&r.labels, "component_id")
                .unwrap_or_default()
                .to_owned(),
            component_type: label(&r.labels, "component_type")
                .unwrap_or_default()
                .to_owned(),
            value: r.value,
        })
        .collect();
    rows.sort_by(|a, b| b.value.total_cmp(&a.value));
    Ok(rows)
}

// ---------------------------------------------------------------------------------------------
// summary.md

fn pct(share: f64) -> String {
    format!("{:.1}%", share * 100.0)
}

fn summary_markdown(s: &Summary) -> String {
    let h = &s.header;
    let mut md = String::from("# Vector CPU profile\n\n| | |\n| --- | --- |\n");
    let mut row = |k: &str, v: String| md.push_str(&format!("| {k} | {v} |\n"));
    row("method", h.method.clone());
    row("what", h.description.clone());
    if let Some(v) = &h.origin {
        row("pod", format!("`{v}`"));
    }
    if let Some(v) = &h.image {
        row("image", format!("`{v}`"));
    }
    if let Some(v) = &h.version {
        row("version", format!("`{v}`"));
    }
    if let Some(v) = h.pid {
        row("pid", v.to_string());
    }
    if let Some(v) = h.duration_secs {
        row("duration", format!("{v:.1} s"));
    }
    row("total", format!("{} {}", s.total, h.unit));
    if let Some(v) = h.perf_samples {
        row("perf samples", v.to_string());
    }
    if let Some(v) = &h.command {
        row("command", format!("`{v}`"));
    }
    if let (Some(user), Some(kernel)) = (h.user_cpu_secs, h.kernel_cpu_secs) {
        let cpu = user + kernel;
        let cores = h.duration_secs.map_or(0.0, |d| cpu / d);
        row(
            "process CPU",
            format!(
                "{cpu:.1} s ({cores:.2} cores): user {user:.1} s, kernel {kernel:.1} s. The stacks are user space only, so they miss the kernel's {}",
                pct(if cpu > 0.0 { kernel / cpu } else { 0.0 })
            ),
        );
    }

    if let Some(live) = &s.live {
        md.push_str(&format!(
            "\n## Live counters\n\nProcess CPU {:.1} s over the run ({:.2} cores, {} threads); {:.0} items received{}.\n\n",
            live.process_cpu_secs,
            live.cores,
            live.threads,
            live.items,
            live.cpu_ms_per_1k_items
                .map(|ms| format!(", {ms:.1} ms CPU per 1k items"))
                .unwrap_or_default()
        ));
        md.push_str("| component | type | metric | seconds | share of process CPU | ms per 1k items |\n| --- | --- | --- | --- | --- | --- |\n");
        for c in &live.components {
            md.push_str(&format!(
                "| `{}` | {} | {} | {:.2} | {} | {} |\n",
                c.component_id,
                c.component_type,
                c.metric,
                c.secs,
                pct(c.share_of_process),
                c.ms_per_1k_items
                    .map(|v| format!("{v:.2}"))
                    .unwrap_or_default(),
            ));
        }
        if !live.utilization.is_empty() {
            md.push_str("\nUtilization at the end of the run (a busy fraction, not CPU):\n\n| component | type | utilization |\n| --- | --- | --- |\n");
            for u in &live.utilization {
                md.push_str(&format!(
                    "| `{}` | {} | {:.3} |\n",
                    u.component_id, u.component_type, u.value
                ));
            }
        }
        md.push_str(
            "\nS3 encode CPU covers JSON serialization and compression together; Zerobus encoding \
             time covers Arrow and VARIANT together and is wall time on a blocking thread. Source \
             decode, Metric construction, metric_to_log_hydra, fan-out, network/TLS and the \
             runtime are not separated: they are the unattributed rest.\n",
        );
    }

    md.push_str("\n## Buckets\n\n| bucket | count | share |\n| --- | --- | --- |\n");
    for b in &s.buckets {
        md.push_str(&format!(
            "| {} | {} | {} |\n",
            b.bucket,
            b.count,
            pct(b.share)
        ));
    }
    for (title, rows) in [
        ("Top frames by self", &s.top_self),
        ("Top frames by inclusive", &s.top_inclusive),
    ] {
        md.push_str(&format!(
            "\n## {title}\n\n| # | frame | count | share |\n| --- | --- | --- | --- |\n"
        ));
        for (i, f) in rows.iter().enumerate() {
            md.push_str(&format!(
                "| {} | `{}` | {} | {} |\n",
                i + 1,
                f.frame.replace('|', "\\|"),
                f.count,
                pct(f.share)
            ));
        }
    }
    md
}

#[cfg(test)]
#[allow(unused_imports, dead_code)] // Bench builds compile this module but never run it.
mod tests {
    use super::*;
    use serde_json::json;

    /// A small perf-like profile of the Hydra fan-out, 100 samples.
    const FIXTURE: &str = "\
vector;tokio::runtime::task::harness::poll;vector::sinks::databricks_zerobus::encoding::timed_encoding;codecs::encoding::format::arrow::encode;arrow_array::builder::map_builder::MapBuilder::append 30
vector;tokio::runtime::task::harness::poll;vector::sinks::databricks_zerobus::encoding::timed_encoding;codecs::encoding::format::arrow::encode;parquet_variant::builder::VariantBuilder::append_value;serde_json::value::to_value 15
vector;tokio::runtime::task::harness::poll;vector::sinks::databricks_zerobus::encoding::timed_encoding;codecs::encoding::format::arrow::encode;_rjem_malloc 5
vector;tokio::runtime::task::harness::poll;vector::sinks::aws_s3::sink::encode;codecs::encoding::format::json::JsonSerializer::encode;serde_json::ser::format_escaped_str 6
vector;tokio::runtime::task::harness::poll;vector::sinks::aws_s3::sink::encode;vector::sinks::util::compressor::Compressor::write;ZSTD_compressBlock_doubleFast 12
vector;tokio::runtime::task::harness::poll;vector::transforms::metric_to_log_hydra::MetricToLogHydra::transform;vector_core::event::metric::Metric::tags 4
vector;tokio::runtime::task::harness::poll;vector::sources::prometheus::remote_write::decode;vector_core::event::metric::Metric::new 3
vector;tokio::runtime::task::harness::poll;vector::sources::prometheus::remote_write::decode;prost::encoding::decode_varint 5
vector;tokio::runtime::task::harness::poll;vector_core::fanout::Fanout::send;<vector_core::event::EventArray as core::clone::Clone>::clone 7
vector;tokio::runtime::task::harness::poll;hyper::proto::h1::io::Buffered::poll_read;rustls::conn::ConnectionCommon::read_tls 8
vector;tokio::runtime::scheduler::multi_thread::worker::park 3
vector;std::rt::lang_start;main 2
";

    fn bucket(buckets: &[BucketCost], name: &str) -> u64 {
        buckets.iter().find(|b| b.bucket == name).unwrap().count
    }

    #[test]
    fn folded_lines_merge_repeats_and_reject_bad_counts() {
        let stacks = parse_folded("a;b 2\n\na;b 3\na;c 1\n").unwrap();
        assert_eq!(stacks["a;b"], 5);
        assert_eq!(stacks["a;c"], 1);
        // Frames may hold spaces; the count is after the last one.
        let stacks = parse_folded("a;<T as core::clone::Clone>::clone 4\n").unwrap();
        assert_eq!(stacks["a;<T as core::clone::Clone>::clone"], 4);
        assert!(parse_folded("a;b\n").unwrap_err().contains("line 1"));
        assert!(
            parse_folded("ok 1\na;b x\n")
                .unwrap_err()
                .contains("line 2")
        );
        assert_eq!(
            folded_text(&parse_folded("b 1\na 2\n").unwrap()),
            "a 2\nb 1\n"
        );
    }

    #[test]
    fn fixture_buckets_follow_module_paths() {
        let stacks = parse_folded(FIXTURE).unwrap();
        let b = buckets(&stacks);
        assert_eq!(b.iter().map(|b| b.count).sum::<u64>(), 100);
        assert_eq!(bucket(&b, "Arrow arrow_stream encoding"), 30);
        // VARIANT wins over Arrow and over serde_json, which it calls.
        assert_eq!(bucket(&b, "VARIANT encoding"), 15);
        // Allocation and zstd are counted by the leaf, wherever they are called from.
        assert_eq!(bucket(&b, "allocation"), 5);
        assert_eq!(bucket(&b, "zstd compression"), 12);
        assert_eq!(bucket(&b, "S3 JSON serialization"), 6);
        assert_eq!(bucket(&b, "metric_to_log_hydra"), 4);
        assert_eq!(bucket(&b, "Metric construction"), 3);
        assert_eq!(bucket(&b, "remote-write decode"), 5);
        assert_eq!(bucket(&b, "event cloning and fan-out"), 7);
        assert_eq!(bucket(&b, "network and TLS"), 8);
        assert_eq!(bucket(&b, "async runtime"), 3);
        assert_eq!(bucket(&b, OTHER_BUCKET), 2);
        assert_eq!(bucket(&b, "S3 sink, not split further"), 0);
        // Every bucket is listed, empty or not, in order.
        assert_eq!(b.len(), BUCKETS.len() + 1);
        assert!((b[0].share - 0.12).abs() < 1e-9);
    }

    #[test]
    fn top_frames_by_self_and_inclusive() {
        let stacks = parse_folded(FIXTURE).unwrap();
        let by_self = top_frames(&stacks, 3, false);
        assert_eq!(
            by_self[0].frame,
            "arrow_array::builder::map_builder::MapBuilder::append"
        );
        assert_eq!(by_self[0].count, 30);
        assert_eq!(by_self[1].frame, "serde_json::value::to_value");
        assert_eq!(by_self.len(), 3);
        let inclusive = top_frames(&stacks, 4, true);
        assert_eq!(
            (inclusive[0].frame.as_str(), inclusive[0].count),
            ("vector", 100)
        );
        // tokio's harness poll: every stack but `park` (3) and `main` (2).
        assert_eq!(inclusive[1].count, 95);
        assert_eq!(
            (inclusive[2].frame.as_str(), inclusive[2].count),
            // Tied with the Zerobus encode frame that calls it; ties sort by name.
            ("codecs::encoding::format::arrow::encode", 50)
        );
        assert!((inclusive[0].share - 1.0).abs() < 1e-9);
        // A recursive frame counts once per stack.
        let recursive = parse_folded("a;f;f;f 5\n").unwrap();
        assert_eq!(top_frames(&recursive, 5, true)[1].count, 5);
    }

    #[test]
    fn proc_stat_fields_count_from_the_last_paren() {
        // Fields 14/15 (utime, stime), 20 (threads), 22 (starttime); the name holds `) (`.
        let line = "10 (vec) (tor) S 1 10 10 0 -1 4194560 1 2 3 4 1500 250 0 0 20 0 65 0 777 9 9";
        let stat = parse_proc_stat(line).unwrap();
        assert_eq!(
            stat,
            ProcStat {
                cpu_ticks: 1750,
                user_ticks: 1500,
                start_ticks: 777,
                threads: 65
            }
        );
        assert!(parse_proc_stat("10 vector S 1").is_err());
        assert!(
            parse_proc_stat("10 (vector) S 1 2")
                .unwrap_err()
                .contains("no field")
        );
    }

    #[test]
    fn pod_must_be_running_and_ready() {
        let pod = |phase: &str, ready: bool| {
            json!({"status": {"phase": phase, "containerStatuses": [
                {"name": "vector-aggregator-metrics", "ready": ready, "image": "img@sha", "restartCount": 2}
            ]}})
        };
        let state = parse_pod_state(&pod("Running", true), "vector-aggregator-metrics").unwrap();
        assert_eq!(
            state,
            PodState {
                image: "img@sha".into(),
                restarts: 2
            }
        );
        assert!(
            parse_pod_state(&pod("Pending", true), "vector-aggregator-metrics")
                .unwrap_err()
                .contains("not Running")
        );
        assert!(
            parse_pod_state(&pod("Running", false), "vector-aggregator-metrics")
                .unwrap_err()
                .contains("not ready")
        );
        assert!(
            parse_pod_state(&pod("Running", true), "other")
                .unwrap_err()
                .contains("no container")
        );
    }

    const BEFORE: &str = "\
vector_component_received_events_total{component_id=\"prom\",component_kind=\"source\",component_type=\"prometheus_remote_write\"} 1000\n\
vector_component_received_events_total{component_id=\"hydra\",component_kind=\"transform\",component_type=\"metric_to_log_hydra\"} 1000\n\
vector_component_encode_cpu_seconds_sum{component_id=\"s3\",component_kind=\"sink\",component_type=\"aws_s3\"} 10\n\
vector_component_encode_cpu_seconds_bucket{component_id=\"s3\",le=\"+Inf\"} 9\n\
vector_zerobus_encoding_seconds_sum{component_id=\"zb\",component_kind=\"sink\",component_type=\"databricks_zerobus\"} 20\n";

    fn after_text(prom: f64, s3: f64, zb: f64) -> String {
        format!(
            "vector_component_received_events_total{{component_id=\"prom\",component_kind=\"source\",component_type=\"prometheus_remote_write\"}} {prom}\n\
             vector_component_received_events_total{{component_id=\"hydra\",component_kind=\"transform\",component_type=\"metric_to_log_hydra\"}} 99999\n\
             vector_component_encode_cpu_seconds_sum{{component_id=\"s3\",component_kind=\"sink\",component_type=\"aws_s3\"}} {s3}\n\
             vector_zerobus_encoding_seconds_sum{{component_id=\"zb\",component_kind=\"sink\",component_type=\"databricks_zerobus\"}} {zb}\n\
             vector_utilization{{component_id=\"hydra\",component_type=\"metric_to_log_hydra\"}} 0.25\n"
        )
    }

    #[test]
    fn live_costs_are_counter_growth_over_process_cpu() {
        let before = parse_counters(BEFORE).unwrap();
        let after = parse_counters(&after_text(101_000.0, 12.0, 26.0)).unwrap();
        // 100k items, 20 s of process CPU over 10 s.
        let live = live_costs(&before, &after, 20.0, 10.0, 65).unwrap();
        assert_eq!(live.items, 100_000.0);
        assert_eq!(live.cores, 2.0);
        assert_eq!(live.cpu_ms_per_1k_items, Some(200.0));
        let zb = &live.components[0];
        assert_eq!((zb.component_id.as_str(), zb.secs), ("zb", 6.0));
        assert_eq!(zb.share_of_process, 0.3);
        assert_eq!(zb.ms_per_1k_items, Some(60.0));
        let s3 = &live.components[1];
        assert_eq!((s3.component_type.as_str(), s3.secs), ("aws_s3", 2.0));

        let stacks = live_stacks(&live);
        assert_eq!(stacks.values().sum::<u64>(), 20_000_000);
        assert_eq!(stacks[&format!("vector;{UNATTRIBUTED_FRAME}")], 12_000_000);
        let b = buckets(&stacks);
        assert_eq!(bucket(&b, "Zerobus sink, not split further"), 6_000_000);
        assert_eq!(bucket(&b, "S3 sink, not split further"), 2_000_000);
        assert_eq!(bucket(&b, OTHER_BUCKET), 12_000_000);

        let util = utilization(&after_text(0.0, 0.0, 0.0)).unwrap();
        assert_eq!(
            (util[0].component_id.as_str(), util[0].value),
            ("hydra", 0.25)
        );
    }

    #[test]
    fn a_counter_that_goes_down_is_a_restart() {
        let before = parse_counters(BEFORE).unwrap();
        let after = parse_counters(&after_text(1_500.0, 5.0, 26.0)).unwrap();
        let error = live_costs(&before, &after, 1.0, 1.0, 1).unwrap_err();
        assert!(error.contains("restarted"), "{error}");
        let empty = Counters::default();
        assert!(
            live_costs(&before, &empty, 1.0, 1.0, 1)
                .unwrap_err()
                .contains("disappeared")
        );
    }

    #[test]
    fn attributed_wall_time_above_cpu_leaves_no_negative_rest() {
        let before = parse_counters(BEFORE).unwrap();
        let after = parse_counters(&after_text(2_000.0, 10.0, 50.0)).unwrap();
        let live = live_costs(&before, &after, 5.0, 10.0, 1).unwrap();
        let stacks = live_stacks(&live);
        assert!(!stacks.contains_key(&format!("vector;{UNATTRIBUTED_FRAME}")));
        assert_eq!(stacks.values().sum::<u64>(), 30_000_000);
    }

    #[test]
    fn kube_arguments_are_required_unless_folded() {
        use clap::Parser;
        #[derive(Parser)]
        struct Cli {
            #[command(flatten)]
            args: ProfileArgs,
        }
        let parse =
            |argv: &[&str]| Cli::try_parse_from([&["profile"], argv].concat()).map(|c| c.args);
        let live = parse(&[
            "--context",
            "c",
            "--namespace",
            "n",
            "--pod",
            "p",
            "--container",
            "v",
            "--out",
            "o",
        ])
        .unwrap();
        // The duration's default depends on the method, so it is resolved later.
        assert_eq!(live.duration_secs, None);
        assert_eq!(live.method, Method::Platform);
        assert_eq!(live.process, "vector");
        let counters = parse(&[
            "--context",
            "c",
            "--namespace",
            "n",
            "--pod",
            "p",
            "--container",
            "v",
            "--out",
            "o",
            "--method",
            "counters",
        ])
        .unwrap();
        assert_eq!(counters.method, Method::Counters);
        let perf_user = parse(&[
            "--context",
            "c",
            "--namespace",
            "n",
            "--pod",
            "p",
            "--container",
            "v",
            "--out",
            "o",
            "--method",
            "perf-user",
        ])
        .unwrap();
        assert_eq!(perf_user.method, Method::PerfUser);
        assert!(parse(&["--folded", "f", "--method", "counters", "--out", "o"]).is_err());
        assert!(parse(&["--context", "c", "--out", "o"]).is_err());
        let folded = parse(&["--folded", "f.folded", "--out", "o"]).unwrap();
        assert_eq!(folded.folded, Some(PathBuf::from("f.folded")));
        assert!(parse(&["--folded", "f", "--pod", "p", "--out", "o"]).is_err());
        assert!(parse(&["--folded", "f", "--out", "o", "--duration-secs", "0"]).is_err());
    }

    #[test]
    fn perf_script_samples_fold_root_first_under_the_thread_name() {
        // `perf script -F comm,ip,sym` as perf 6.8 prints it; the third sample repeats the first.
        let script = "\
vector-worker \n\
\t    5604c41ff44a _RNvleaf\n\
\t    5604ce1db86c _RNvroot\n\
\n\
vector-worker \n\
\t    5604c41ff44b [unknown]\n\
\t    5604ce1db86d _RNvroot\n\
\n\
vector-worker \n\
\t    5604c41ff44a _RNvleaf\n\
\t    5604ce1db86c _RNvroot\n";
        let stacks = fold_perf_script(script);
        assert_eq!(
            stacks,
            Stacks::from([
                ("vector-worker;_RNvroot;_RNvleaf".to_owned(), 2),
                ("vector-worker;_RNvroot;[unknown]".to_owned(), 1),
            ])
        );
    }

    #[test]
    fn crate_hashes_are_dropped_from_demangled_names() {
        assert_eq!(
            without_crate_hashes(
                "<alloc[12e961f32105355]::string::String as vector_common[bafa5a743ffdf86a]::byte_size_of::ByteSizeOf>::allocated_bytes"
            ),
            "<alloc::string::String as vector_common::byte_size_of::ByteSizeOf>::allocated_bytes"
        );
        // Slices and arrays stay, and `;` cannot split the folded frame.
        assert_eq!(
            without_crate_hashes("core[0123456789abcdef]::f::<[f32], [u8; 4]>"),
            "core::f::<[f32], [u8, 4]>"
        );
    }

    #[test]
    fn demangling_turns_v0_symbols_into_module_paths() {
        // Needs binutils' c++filt on the PATH, as `--method perf-user` does.
        let mangled = "_RNvMs_NtNtNtNtCsk8wKEr9MtiO_5tokio7runtime9scheduler12multi_thread6workerNtB4_7Context3run";
        let stacks = Stacks::from([(format!("vector-worker;[unknown];{mangled}"), 3)]);
        let demangled = demangle(stacks).unwrap();
        assert_eq!(
            demangled,
            Stacks::from([(
                "vector-worker;[unknown];<tokio::runtime::scheduler::multi_thread::worker::Context>::run"
                    .to_owned(),
                3
            )])
        );
        assert_eq!(bucket_of(demangled.keys().next().unwrap()), "async runtime");
    }

    /// The shape of an inferno flamegraph (as cargo-flamegraph 0.6 writes it), cut to six frames.
    /// `all` is 105 wide but its one drawn child is 100: 5 belongs to frames too narrow to draw.
    const SVG_FIXTURE: &str = r#"<?xml version="1.0" standalone="no"?><svg version="1.1" width="1200" height="118" total_samples="105" xmlns="http://www.w3.org/2000/svg" xmlns:fg="http://github.com/jonhoo/inferno"><g id="frames" total_samples="105">
<g><title>arrow_array::builder::map_builder::MapBuilder::append (40 samples, 38.10%)</title><rect x="0.0000%" y="53" width="38.0952%" height="15" fill="rgb(227,0,7)" fg:x="0" fg:w="40"/><text x="0.2500%" y="63.50">arrow_array::..</text></g>
<g><title>parquet_variant::builder::&lt;T as core::clone::Clone&gt;::clone (15 samples, 14.29%)</title><rect x="38.0952%" y="53" width="14.2857%" height="15" fill="rgb(217,0,24)" fg:x="40" fg:w="15"/><text x="38.3452%" y="63.50">parquet_..</text></g>
<g><title>codecs::encoding::format::arrow::encode (60 samples, 57.14%)</title><rect x="0.0000%" y="69" width="57.1429%" height="15" fill="rgb(221,193,54)" fg:x="0" fg:w="60"/><text x="0.2500%" y="79.50">codecs::encoding::format::arrow::encode</text></g>
<g><title>hyper::proto::h1::dispatch::poll (30 samples, 28.57%)</title><rect x="57.1429%" y="69" width="28.5714%" height="15" fill="rgb(250,10,9)" fg:x="60" fg:w="30"/><text x="57.3929%" y="79.50">hyper::..</text></g>
<g><title>vector-worker (100 samples, 95.24%)</title><rect x="0.0000%" y="85" width="95.2381%" height="15" fill="rgb(200,100,10)" fg:x="0" fg:w="100"/><text x="0.2500%" y="95.50">vector-worker</text></g>
<g><title>all (105 samples, 100%)</title><rect x="0.0000%" y="101" width="100.0000%" height="15" fill="rgb(229,107,41)" fg:x="0" fg:w="105"/><text x="0.2500%" y="111.50"></text></g>
</g></svg>"#;

    #[test]
    fn flamegraph_svg_rebuilds_the_folded_stacks() {
        let stacks = folded_from_flamegraph_svg(SVG_FIXTURE).unwrap();
        let expected: Stacks = [
            ("vector-worker", 10),
            ("vector-worker;codecs::encoding::format::arrow::encode", 5),
            (
                "vector-worker;codecs::encoding::format::arrow::encode;arrow_array::builder::map_builder::MapBuilder::append",
                40,
            ),
            (
                "vector-worker;codecs::encoding::format::arrow::encode;parquet_variant::builder::<T as core::clone::Clone>::clone",
                15,
            ),
            ("vector-worker;hyper::proto::h1::dispatch::poll", 30),
            (PRUNED_FRAME, 5),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v))
        .collect();
        assert_eq!(stacks, expected);
        // Every sample is kept, pruned ones included.
        assert_eq!(stacks.values().sum::<u64>(), 105);
        let b = buckets(&stacks);
        assert_eq!(bucket(&b, "Arrow arrow_stream encoding"), 45);
        assert_eq!(bucket(&b, "VARIANT encoding"), 15);
        assert_eq!(bucket(&b, "network and TLS"), 30);
    }

    #[test]
    fn flamegraph_svg_errors_name_the_problem() {
        assert!(
            folded_from_flamegraph_svg("<svg></svg>")
                .unwrap_err()
                .contains("no flamegraph frames")
        );
        let no_fg = r#"<svg><g><title>f (1 samples, 100%)</title><rect x="0%" y="5" width="100%"/></g></svg>"#;
        assert!(
            folded_from_flamegraph_svg(no_fg)
                .unwrap_err()
                .contains("fg:x")
        );
        // A box with nothing under it (here at x=50 above a 10-wide root) is a broken SVG.
        let orphan = r#"<svg><g><title>a (10 samples, 100%)</title><rect y="20" fg:x="0" fg:w="10"/></g><g><title>b (1 samples, 10%)</title><rect y="4" fg:x="50" fg:w="1"/></g></svg>"#;
        assert!(
            folded_from_flamegraph_svg(orphan)
                .unwrap_err()
                .contains("no parent")
        );
        assert!(is_svg(Path::new("x.SVG"), ""));
        assert!(is_svg(Path::new("x"), "  <?xml version"));
        assert!(!is_svg(Path::new("x.folded"), "a;b 1"));
    }

    #[test]
    fn xml_entities_are_decoded() {
        assert_eq!(
            xml_unescape("&lt;T as Clone&gt;::f &amp; &quot;q&quot; &#39;&#x41;"),
            "<T as Clone>::f & \"q\" 'A"
        );
        // An unknown or unterminated entity is kept as text.
        assert_eq!(xml_unescape("a &bogus; b & c"), "a &bogus; b & c");
    }

    #[test]
    fn perf_sample_count_comes_from_the_tool_log() {
        let log = "INFO Profiling process 10 for 30 seconds...\n\
                   [ perf record: Woken up 175 times to write data ]\n\
                   [ perf record: Captured and wrote 43.816 MB perf.data (12,715 samples) ]\n";
        assert_eq!(perf_samples_in_log(log), Some(12_715));
        assert_eq!(perf_samples_in_log("nothing here"), None);
    }

    #[test]
    fn platform_command_targets_the_vector_pid_with_dwarf_stacks() {
        let pod = PodRef {
            context: "staging-aws-us-west-2-0".into(),
            namespace: "vector-aggregator".into(),
            pod: "vam-1".into(),
            container: "vector-aggregator-metrics".into(),
        };
        let argv = platform_command(&pod, 10, 30, Path::new("/out/platform.svg"));
        assert_eq!(
            argv.join(" "),
            "run //common/profiling/rust/cpu:profiler -- --context staging-aws-us-west-2-0 \
             --namespace vector-aggregator --pod-name vam-1 --container vector-aggregator-metrics \
             --pid 10 --duration 30 --format flamegraph --output /out/platform.svg"
        );
        assert_eq!(
            universe_dir(&Some(PathBuf::from("/u"))),
            PathBuf::from("/u")
        );
    }

    #[test]
    fn folded_accepts_a_flamegraph_svg() {
        use clap::Parser;
        #[derive(Parser)]
        struct Cli {
            #[command(flatten)]
            args: ProfileArgs,
        }
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("platform.svg");
        std::fs::write(&input, SVG_FIXTURE).unwrap();
        let out = dir.path().join("out");
        let args = Cli::try_parse_from([
            "profile",
            "--folded",
            input.to_str().unwrap(),
            "--out",
            out.to_str().unwrap(),
        ])
        .unwrap()
        .args;
        assert!(profile(args).unwrap());
        let summary: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(out.join("summary.json")).unwrap())
                .unwrap();
        assert_eq!(summary["header"]["unit"], "perf sample weight");
        assert_eq!(summary["total"], 105);
    }

    #[test]
    fn network_frames_win_over_the_sink_that_polls_them() {
        let stacks = parse_folded(
            "vector::sinks::aws_s3::sink::run;hyper::client::conn::poll;rustls::conn::write_tls 7\n\
             vector::sinks::aws_s3::sink::run;tokio::net::tcp::write;tcp_sendmsg 3\n\
             vector::sinks::databricks_zerobus::encode;vrl::path::jit::lookup 4\n",
        )
        .unwrap();
        let b = buckets(&stacks);
        assert_eq!(bucket(&b, "network and TLS"), 10);
        assert_eq!(bucket(&b, "S3 sink, not split further"), 0);
        assert_eq!(bucket(&b, "field lookup"), 4);
    }

    #[test]
    fn real_stack_roots_do_not_claim_the_work_under_them() {
        // Shapes from a live VAM pod: the source runs inside hyper's server task, every worker
        // stack starts in the runtime, and metric_to_log_hydra hashes with serde_json.
        let root = "tokio::runtime::scheduler::multi_thread::worker::run";
        let server = "hyper::server::server::new_svc::NewSvcTask::poll";
        let stacks = parse_folded(&format!(
            "{root};{server};vector::sources::prometheus::remote_write::decode;prost::encoding::merge 5\n\
             {root};vector::transforms::metric_to_log_hydra::hash;<&mut serde_json::ser::Serializer<HashWriter>>::collect_map 4\n\
             {root};<BTreeMap<alloc::string::String, KeyString> as Drop>::drop 3\n\
             {root};{server};rustls::conn::ConnectionCommon::read_tls 2\n\
             {root};tokio::runtime::task::harness::poll 1\n"
        ))
        .unwrap();
        let b = buckets(&stacks);
        assert_eq!(bucket(&b, "remote-write decode"), 5);
        assert_eq!(bucket(&b, "metric_to_log_hydra"), 4);
        assert_eq!(bucket(&b, "S3 JSON serialization"), 0);
        // `ring::` is not in `alloc::string::`, and the runtime root claims nothing.
        assert_eq!(bucket(&b, OTHER_BUCKET), 3);
        assert_eq!(bucket(&b, "network and TLS"), 2);
        assert_eq!(bucket(&b, "async runtime"), 1);
    }

    #[test]
    fn folded_run_writes_the_three_files() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.folded");
        std::fs::write(&input, FIXTURE).unwrap();
        let out = dir.path().join("out");
        use clap::Parser;
        #[derive(Parser)]
        struct Cli {
            #[command(flatten)]
            args: ProfileArgs,
        }
        let args = Cli::try_parse_from([
            "profile",
            "--folded",
            input.to_str().unwrap(),
            "--out",
            out.to_str().unwrap(),
        ])
        .unwrap()
        .args;
        assert!(profile(args).unwrap());
        let summary: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(out.join("summary.json")).unwrap())
                .unwrap();
        assert_eq!(summary["header"]["method"], "folded");
        assert_eq!(summary["header"]["samples"], 100);
        assert_eq!(summary["total"], 100);
        let md = std::fs::read_to_string(out.join("summary.md")).unwrap();
        assert!(
            md.contains("| Arrow arrow_stream encoding | 30 | 30.0% |"),
            "{md}"
        );
        assert_eq!(
            parse_folded(&std::fs::read_to_string(out.join("profile.folded")).unwrap()).unwrap(),
            parse_folded(FIXTURE).unwrap()
        );
        // A second run into the same directory is refused.
        let again = Cli::try_parse_from([
            "profile",
            "--folded",
            input.to_str().unwrap(),
            "--out",
            out.to_str().unwrap(),
        ])
        .unwrap()
        .args;
        assert!(profile(again).is_err());
    }
}
