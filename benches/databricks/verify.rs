//! `verify`: compares what candidates' sinks wrote, with the baseline of each sink.
//!
//! Each `--output <target>=<sink>:<dir>` names a `gen --target`, a sink (see `sink.rs`) and a
//! fake's `--out`. The sink reads its output back as rows, each with the destination it was written
//! under (`SinkKind::read_back`). The first output of each sink is the baseline, and every other
//! output of that sink is a candidate, compared with it as a multiset of
//! `(destination, metadata fingerprint, original bytes)`.
//!
//! Rows are compared byte for byte, so no JSON is parsed and nothing is canonicalized: a row equals
//! another only when its destination, metadata and bytes are equal. Rows that hold the same data but
//! are serialized differently (key order, whitespace, number spelling) are different rows. A
//! pipeline that needs semantic equality normalizes its rows in its sink's `read_back`.
//!
//! Outputs of different sinks are never compared with each other.
//!
//! What it proves: that the outputs agree with each other. It does not prove they are right. It
//! has no expected rows, so a baseline that is wrong passes unchallenged.
//!
//! On DIFFERENT, verify prints how many rows are only in the baseline and only in the candidate,
//! and the first few of each side with their destinations (re-read from the outputs).
//!
//! Retries: a target whose requests were retried, or whose last attempt was not accepted, can
//! legitimately deliver duplicates that differ across candidates. The verdict is then
//! INCONCLUSIVE, though the differences are still printed.
//!
//! Refusals are errors (exit 2), not verdicts: a sent log that does not hold every request of its
//! run, an output with no rows, and a sink with fewer than two outputs.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use super::generator::{SentLogHeader, SentRequest};
use super::sink::{SinkKind, fingerprint, sink_named};
use super::source_codec::{Outcome, SourceCodec, codec_named};
use super::{CmdResult, err, io_at};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Rows shown per side of a difference, each in full; the counts are always complete.
const EXAMPLES: usize = 2;

#[derive(clap::Args)]
pub struct VerifyArgs {
    /// Sent log written by `gen`. It names the corpus's codec and records every request of the run.
    #[arg(long)]
    sent_log: PathBuf,
    /// `<target>=<sink>:<dir>`: the `gen --target` name, the sink kind (`s3` or `zerobus`) and the
    /// fake's `--out`. Repeatable. Each sink needs at least two outputs: the first is the
    /// baseline, and the others are compared with it.
    #[arg(long = "output", required = true)]
    outputs: Vec<String>,
}

struct Output {
    spec: String,
    target: String,
    sink: &'static dyn SinkKind,
    dir: PathBuf,
}

impl Output {
    fn label(&self) -> String {
        format!("{}/{}", self.target, self.sink.name())
    }
}

fn parse_outputs(specs: &[String]) -> Result<Vec<Output>, BoxError> {
    specs
        .iter()
        .map(|spec| {
            let usage = || {
                err(format!(
                    "--output `{spec}`: expected <target>=<sink>:<dir>, sink `s3` or `zerobus`"
                ))
            };
            let (target, rest) = spec.split_once('=').ok_or_else(usage)?;
            let (kind, dir) = rest.split_once(':').ok_or_else(usage)?;
            if target.is_empty() || dir.is_empty() {
                return Err(usage());
            }
            let sink = sink_named(kind).map_err(|e| err(format!("--output `{spec}`: {e}")))?;
            Ok(Output {
                spec: spec.clone(),
                target: target.to_owned(),
                sink,
                dir: PathBuf::from(dir),
            })
        })
        .collect()
}

/// The verdict and its exit status: only IDENTICAL exits 0.
#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    Identical,
    Different,
    Inconclusive,
}

/// Retries and failed requests per compared target, as the sent log records them.
#[derive(Debug, Default, PartialEq)]
struct Retries {
    /// Requests with more than one attempt, per target.
    retried: BTreeMap<String, u64>,
    /// Requests whose last attempt was not accepted, per target.
    not_accepted: BTreeMap<String, u64>,
}

impl Retries {
    fn of(codec: &dyn SourceCodec, sent: &[SentRequest], targets: &[&str]) -> Self {
        let mut found = Self::default();
        for req in sent {
            for &target in targets {
                let Some(attempts) = req.attempts.get(target) else {
                    continue;
                };
                if attempts.len() > 1 {
                    *found.retried.entry(target.to_owned()).or_default() += 1;
                }
                let accepted = attempts
                    .last()
                    .is_some_and(|&status| codec.classify(status) == Outcome::Accepted);
                if !accepted {
                    *found.not_accepted.entry(target.to_owned()).or_default() += 1;
                }
            }
        }
        found
    }

    fn is_empty(&self) -> bool {
        self.retried.is_empty() && self.not_accepted.is_empty()
    }

    fn describe(&self) -> String {
        fn list(counts: &BTreeMap<String, u64>) -> String {
            counts
                .iter()
                .map(|(target, n)| format!("{target} {n}"))
                .collect::<Vec<_>>()
                .join(", ")
        }
        let mut parts = Vec::new();
        if !self.retried.is_empty() {
            parts.push(format!("requests retried on {}", list(&self.retried)));
        }
        if !self.not_accepted.is_empty() {
            parts.push(format!(
                "requests not accepted on {}",
                list(&self.not_accepted)
            ));
        }
        parts.join("; ")
    }
}

/// The verdict: retries make any outcome inconclusive, otherwise any difference fails.
fn verdict(differs: bool, retries: &Retries) -> Verdict {
    if !retries.is_empty() {
        Verdict::Inconclusive
    } else if differs {
        Verdict::Different
    } else {
        Verdict::Identical
    }
}

pub fn run_cli(args: VerifyArgs) -> CmdResult {
    verify(&args.sent_log, &args.outputs)
}

/// Compares each sink's outputs with that sink's baseline, as described in the module docs.
/// `outputs` holds `<target>=<sink>:<dir>` specs, with each sink's baseline first, and prints the
/// report. Returns `Ok(true)` only when IDENTICAL.
pub fn verify(sent_log: &Path, outputs: &[String]) -> CmdResult {
    let (sent_header, sent) = read_sent_log(sent_log)?;
    let codec = codec_named(&sent_header.corpus.header.source_type)?;
    check_complete(
        sent_log,
        sent_header.iterations,
        sent_header.requests_per_iteration,
        &sent,
    )?;
    let outputs = parse_outputs(outputs)?;
    check_targets(&outputs, &sent)?;
    check_sink_counts(&outputs)?;

    let mut targets: Vec<&str> = outputs.iter().map(|o| o.target.as_str()).collect();
    targets.sort_unstable();
    targets.dedup();
    let retries = Retries::of(codec, &sent, &targets);

    let scans = outputs.iter().map(scan).collect::<Result<Vec<_>, _>>()?;
    for scan in &scans {
        print_scan(scan);
    }
    check_rows(&scans)?;
    let mut differs = scans.iter().any(|s| s.damaged > 0);
    for group in by_sink(&scans) {
        // The first output of each sink is its baseline.
        let (baseline, candidates) = group
            .split_first()
            .expect("check_sink_counts: every sink has two or more outputs");
        for candidate in candidates {
            let diff = diff(baseline, candidate)?;
            print_diff(&diff);
            differs |= !diff.identical();
        }
    }

    let verdict = verdict(differs, &retries);
    match verdict {
        Verdict::Identical => {
            eprintln!("verify: IDENTICAL: every output of each sink holds the same rows")
        }
        Verdict::Different => {
            eprintln!(
                "verify: DIFFERENT: a candidate's rows or metadata differ from its sink's baseline"
            )
        }
        Verdict::Inconclusive => eprintln!(
            "verify: INCONCLUSIVE: {}; retried duplicates can legitimately differ across candidates",
            retries.describe()
        ),
    }
    Ok(verdict == Verdict::Identical)
}

fn read_sent_log(path: &Path) -> Result<(SentLogHeader, Vec<SentRequest>), BoxError> {
    let text = io_at(std::fs::read_to_string(path), "reading --sent-log", path)?;
    let mut lines = text.lines();
    let header = lines
        .next()
        .ok_or_else(|| err(format!("--sent-log {} is empty", path.display())))?;
    let header = serde_json::from_str(header)
        .map_err(|e| err(format!("--sent-log {}: not a header: {e}", path.display())))?;
    let requests = lines
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            serde_json::from_str(l).map_err(|e| err(format!("--sent-log {}: {e}", path.display())))
        })
        .collect::<Result<_, BoxError>>()?;
    Ok((header, requests))
}

/// The sent log must hold every request of every pass, once each. A run that was killed or failed
/// part way leaves a log whose verdict would cover only some of the corpus.
fn check_complete(
    sent_log: &Path,
    iterations: u64,
    requests_per_iteration: u64,
    sent: &[SentRequest],
) -> Result<(), BoxError> {
    let expected = iterations.checked_mul(requests_per_iteration).ok_or_else(|| {
        err(format!(
            "--sent-log {}: {iterations} iterations of {requests_per_iteration} requests is too many to check",
            sent_log.display()
        ))
    })?;
    let mut seqs: Vec<u64> = sent.iter().map(|r| r.seq).collect();
    seqs.sort_unstable();
    let numbered_from_zero = seqs.iter().enumerate().all(|(i, &seq)| seq == i as u64);
    if sent.len() as u64 != expected || !numbered_from_zero {
        return Err(err(format!(
            "--sent-log {} is incomplete: it holds {} requests, expected {expected} ({iterations} iteration(s) of {requests_per_iteration} corpus requests), with sequence numbers 0 to {} once each; the gen run may have stopped early",
            sent_log.display(),
            sent.len(),
            expected.saturating_sub(1)
        )));
    }
    Ok(())
}

/// Each output's target must appear in the sent log, and its directory must exist.
fn check_targets(outputs: &[Output], sent: &[SentRequest]) -> Result<(), BoxError> {
    for output in outputs {
        if !sent.iter().any(|r| r.attempts.contains_key(&output.target)) {
            return Err(err(format!(
                "--output `{}`: the sent log has no requests for target `{}`",
                output.spec, output.target
            )));
        }
        if !output.dir.is_dir() {
            return Err(err(format!(
                "--output `{}`: {} is not a directory",
                output.spec,
                output.dir.display()
            )));
        }
    }
    Ok(())
}

/// An output with no rows cannot be compared, so a run with one refuses rather than pass on the
/// rest. The empty output is usually a wrong --out directory.
fn check_rows(scans: &[Scan]) -> Result<(), BoxError> {
    match scans.iter().find(|s| s.keys.is_empty()) {
        None => Ok(()),
        Some(scan) => Err(err(format!(
            "--output `{}` has no rows, so it cannot be compared; check that it names the fake's --out directory the candidate wrote to",
            scan.output.spec
        ))),
    }
}

/// Every sink that appears needs a baseline and at least one candidate, or there is nothing to
/// compare it with.
fn check_sink_counts(outputs: &[Output]) -> Result<(), BoxError> {
    let mut kinds: Vec<&str> = outputs.iter().map(|o| o.sink.name()).collect();
    kinds.sort_unstable();
    kinds.dedup();
    for kind in kinds {
        let n = outputs.iter().filter(|o| o.sink.name() == kind).count();
        if n < 2 {
            return Err(err(format!(
                "sink `{kind}` has {n} output; verify compares each candidate with the first `{kind}` \
                 output, so it needs at least 2 (e.g. --output a={kind}:<dir> --output b={kind}:<dir>)"
            )));
        }
    }
    Ok(())
}

/// One output after its read-back: the key of every row, so the diff needs no second pass unless
/// it finds a difference.
struct Scan<'a> {
    output: &'a Output,
    files: u64,
    /// Records the reader could not read to their end. Any damaged output is a difference.
    damaged: u64,
    keys: Vec<u64>,
}

/// The scans of each sink, in the order the sinks first appear, each group in `--output` order.
fn by_sink<'s, 'a>(scans: &'s [Scan<'a>]) -> Vec<Vec<&'s Scan<'a>>> {
    let mut groups: Vec<Vec<&Scan>> = Vec::new();
    for scan in scans {
        let kind = scan.output.sink.name();
        match groups.iter_mut().find(|g| g[0].output.sink.name() == kind) {
            Some(group) => group.push(scan),
            None => groups.push(vec![scan]),
        }
    }
    groups
}

fn scan(output: &Output) -> Result<Scan<'_>, BoxError> {
    let mut keys = Vec::new();
    let found = output
        .sink
        .read_back(&output.dir, &mut |destination, metadata, bytes| {
            keys.push(row_key(destination, metadata, bytes));
        })?;
    Ok(Scan {
        output,
        files: found.files,
        damaged: found.damaged,
        keys,
    })
}

/// A row's identity without buffering its bytes. Hash's tuple/string/slice framing delimits
/// fields unambiguously. Both this key and the metadata fingerprint have a 64-bit collision risk;
/// for a million distinct rows, the row-key collision probability is around 10⁻⁸.
fn row_key(destination: &str, metadata: u64, bytes: &[u8]) -> u64 {
    fingerprint((destination, metadata, bytes))
}

/// The comparison of one candidate with its sink's baseline.
struct Diff {
    baseline: String,
    candidate: String,
    /// Rows the baseline holds more often than the candidate, and the reverse.
    only_baseline: u64,
    only_candidate: u64,
    /// The first rows of each side that are not in the other, as (destination, metadata hash, bytes).
    examples_baseline: Vec<(String, u64, Vec<u8>)>,
    examples_candidate: Vec<(String, u64, Vec<u8>)>,
}

impl Diff {
    fn identical(&self) -> bool {
        self.only_baseline == 0 && self.only_candidate == 0
    }
}

fn diff(baseline: &Scan, candidate: &Scan) -> Result<Diff, BoxError> {
    let (only_baseline, only_candidate, mut wanted_baseline, mut wanted_candidate) =
        multiset_diff(&baseline.keys, &candidate.keys);
    let (examples_baseline, examples_candidate) = if only_baseline + only_candidate == 0 {
        (Vec::new(), Vec::new())
    } else {
        // Only the differing rows are read again, so the cost follows the differences.
        (
            examples(baseline.output, &mut wanted_baseline)?,
            examples(candidate.output, &mut wanted_candidate)?,
        )
    };
    Ok(Diff {
        baseline: baseline.output.label(),
        candidate: candidate.output.label(),
        only_baseline,
        only_candidate,
        examples_baseline,
        examples_candidate,
    })
}

/// The rows each side holds more often than the other: their counts, and the keys with counts.
type Surplus = HashMap<u64, u64>;

fn multiset_diff(baseline: &[u64], candidate: &[u64]) -> (u64, u64, Surplus, Surplus) {
    // Net count per row: positive where the baseline holds more, negative where the candidate does.
    let mut balance: HashMap<u64, i64> = HashMap::new();
    for key in baseline {
        *balance.entry(*key).or_default() += 1;
    }
    for key in candidate {
        *balance.entry(*key).or_default() -= 1;
    }
    let (mut only_baseline, mut only_candidate) = (0, 0);
    let (mut wanted_baseline, mut wanted_candidate) = (HashMap::new(), HashMap::new());
    for (key, net) in balance {
        match net.cmp(&0) {
            std::cmp::Ordering::Greater => {
                only_baseline += net as u64;
                wanted_baseline.insert(key, net as u64);
            }
            std::cmp::Ordering::Less => {
                only_candidate += net.unsigned_abs();
                wanted_candidate.insert(key, net.unsigned_abs());
            }
            std::cmp::Ordering::Equal => {}
        }
    }
    (
        only_baseline,
        only_candidate,
        wanted_baseline,
        wanted_candidate,
    )
}

/// The first rows of `output` that `wanted` still counts, as (destination, metadata hash, bytes), at most
/// `EXAMPLES` of them. Each one found uses up one count.
fn examples(
    output: &Output,
    wanted: &mut Surplus,
) -> Result<Vec<(String, u64, Vec<u8>)>, BoxError> {
    let mut found = Vec::new();
    output
        .sink
        .read_back(&output.dir, &mut |destination, metadata, bytes| {
            if found.len() >= EXAMPLES {
                return;
            }
            if let Some(n) = wanted.get_mut(&row_key(destination, metadata, bytes))
                && *n > 0
            {
                *n -= 1;
                found.push((destination.to_owned(), metadata, bytes.to_vec()));
            }
        })?;
    Ok(found)
}

fn print_scan(scan: &Scan) {
    eprintln!(
        "verify[{}]: {} files, {} rows, {} damaged records",
        scan.output.label(),
        scan.files,
        scan.keys.len(),
        scan.damaged
    );
}

fn print_diff(d: &Diff) {
    let label = format!("{} vs {}", d.baseline, d.candidate);
    if d.identical() {
        eprintln!("diff[{label}]: identical");
        return;
    }
    eprintln!(
        "diff[{label}]: {} rows only in baseline, {} only in candidate -> DIFFERENT",
        d.only_baseline, d.only_candidate
    );
    let show = |destination: &str, metadata: u64, bytes: &[u8]| {
        let destination = if destination.is_empty() {
            "<root>"
        } else {
            destination
        };
        format!(
            "at {destination} (metadata {metadata:016x}): {}",
            text(bytes)
        )
    };
    for (destination, metadata, bytes) in &d.examples_baseline {
        eprintln!(
            "diff[{label}]:   only in baseline, {}",
            show(destination, *metadata, bytes)
        );
    }
    for (destination, metadata, bytes) in &d.examples_candidate {
        eprintln!(
            "diff[{label}]:   only in candidate, {}",
            show(destination, *metadata, bytes)
        );
    }
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).trim_end().to_owned()
}

#[cfg(test)]
#[allow(unused_imports, dead_code)] // Bench builds compile this module but never run it.
mod tests {
    use super::*;
    use std::io::Write;

    fn output(sink: &str, dir: &Path) -> Output {
        Output {
            spec: format!("a={sink}:{}", dir.display()),
            target: "a".into(),
            sink: sink_named(sink).unwrap(),
            dir: dir.to_owned(),
        }
    }

    /// Writes `files` (name, contents) under a fresh `out` directory of `dir` and returns it.
    fn write_output(dir: &Path, out: &str, files: &[(&str, &str)]) -> PathBuf {
        let out = dir.join(out);
        for (name, contents) in files {
            let path = out.join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, contents).unwrap();
        }
        out
    }

    fn sent(attempts: &[(&str, Vec<u16>)]) -> SentRequest {
        SentRequest {
            seq: 0,
            iteration: 0,
            request: 0,
            items: 1,
            ts: 0,
            attempts: attempts
                .iter()
                .map(|(target, statuses)| (target.to_string(), statuses.clone()))
                .collect(),
        }
    }

    #[test]
    fn each_sinks_first_output_leads_its_group() {
        let outputs = parse_outputs(&[
            "a=s3:/a/s3".into(),
            "a=zerobus:/a/zb".into(),
            "b=s3:/b/s3".into(),
            "b=zerobus:/b/zb".into(),
            "c=s3:/c/s3".into(),
        ])
        .unwrap();
        let scans: Vec<Scan> = outputs
            .iter()
            .map(|output| Scan {
                output,
                files: 0,
                damaged: 0,
                keys: Vec::new(),
            })
            .collect();
        let groups: Vec<Vec<&str>> = by_sink(&scans)
            .iter()
            .map(|g| g.iter().map(|s| s.output.spec.as_str()).collect())
            .collect();
        assert_eq!(
            groups,
            [
                vec!["a=s3:/a/s3", "b=s3:/b/s3", "c=s3:/c/s3"],
                vec!["a=zerobus:/a/zb", "b=zerobus:/b/zb"],
            ]
        );
    }

    #[test]
    fn output_specs_split_at_the_first_separators() {
        let outputs = parse_outputs(&["a=zerobus:/tmp/x:y".into()]).unwrap();
        assert_eq!(outputs[0].label(), "a/zerobus");
        assert_eq!(outputs[0].dir, Path::new("/tmp/x:y"));
        for bad in ["a", "a=gcs:/x", "=s3:/x", "a=s3:"] {
            let error = parse_outputs(&[bad.into()]).err().unwrap().to_string();
            assert!(error.contains("--output"), "{error}");
        }
    }

    #[test]
    fn each_sink_needs_a_baseline_and_a_candidate() {
        let s3 = |target: &str| Output {
            spec: String::new(),
            target: target.into(),
            sink: sink_named("s3").unwrap(),
            dir: PathBuf::from("/"),
        };
        assert!(check_sink_counts(&[s3("a"), s3("b")]).is_ok());
        let error = check_sink_counts(&[s3("a")]).err().unwrap().to_string();
        assert!(error.contains("sink `s3` has 1 output"), "{error}");
        assert!(error.contains("at least 2"), "{error}");
    }

    #[test]
    fn rows_are_equal_only_when_their_destination_and_bytes_are_equal() {
        let dir = tempfile::tempdir().unwrap();
        let base_files = [(
            "bucket/a/x.json",
            "{\"name\":\"up\",\"v\":1}\n{\"name\":\"down\"}\n",
        )];
        let base = write_output(dir.path(), "a", &base_files);
        let (base_out, same) = (
            output("s3", &base),
            output("s3", &write_output(dir.path(), "same", &base_files)),
        );
        assert!(
            diff(&scan(&base_out).unwrap(), &scan(&same).unwrap())
                .unwrap()
                .identical()
        );

        // The same data with a key moved, a space added, or a number respelled: different rows.
        for rewritten in [
            "{\"v\":1,\"name\":\"up\"}\n{\"name\":\"down\"}\n",
            "{\"name\":\"up\", \"v\":1}\n{\"name\":\"down\"}\n",
            "{\"name\":\"up\",\"v\":1.0}\n{\"name\":\"down\"}\n",
        ] {
            let cand = write_output(dir.path(), "cand", &[("bucket/a/x.json", rewritten)]);
            let d = diff(
                &scan(&base_out).unwrap(),
                &scan(&output("s3", &cand)).unwrap(),
            )
            .unwrap();
            assert_eq!((d.only_baseline, d.only_candidate), (1, 1), "{rewritten}");
            std::fs::remove_dir_all(&cand).unwrap();
        }
    }

    #[test]
    fn the_same_row_under_another_destination_is_a_difference() {
        let dir = tempfile::tempdir().unwrap();
        let base = write_output(dir.path(), "a", &[("bucket/date=1/x.json", "{\"v\":1}\n")]);
        let cand = write_output(dir.path(), "b", &[("bucket/date=2/x.json", "{\"v\":1}\n")]);
        let d = diff(
            &scan(&output("s3", &base)).unwrap(),
            &scan(&output("s3", &cand)).unwrap(),
        )
        .unwrap();
        assert_eq!((d.only_baseline, d.only_candidate), (1, 1));
        assert_eq!(
            d.examples_baseline,
            [(
                "bucket/date=1".into(),
                fingerprint(("x.json".to_owned(), "none")),
                b"{\"v\":1}".to_vec()
            )]
        );
        assert_eq!(
            d.examples_candidate,
            [(
                "bucket/date=2".into(),
                fingerprint(("x.json".to_owned(), "none")),
                b"{\"v\":1}".to_vec()
            )]
        );
    }

    #[test]
    fn a_difference_shows_the_rows_of_each_side_with_their_destinations() {
        let dir = tempfile::tempdir().unwrap();
        let base = write_output(
            dir.path(),
            "a",
            &[(
                "b/x.json",
                "{\"v\":1}\n{\"v\":2}\n{\"v\":3}\n{\"v\":4}\n{\"v\":5}\n",
            )],
        );
        let cand = write_output(
            dir.path(),
            "b",
            &[(
                "b/x.json",
                "{\"v\":1}\n{\"v\":7}\n{\"v\":8}\n{\"v\":9}\n{\"v\":5}\n",
            )],
        );
        let d = diff(
            &scan(&output("s3", &base)).unwrap(),
            &scan(&output("s3", &cand)).unwrap(),
        )
        .unwrap();
        assert_eq!((d.only_baseline, d.only_candidate), (3, 3));
        // At most EXAMPLES rows per side, each once.
        assert_eq!(d.examples_baseline.len(), EXAMPLES);
        assert_eq!(d.examples_candidate.len(), EXAMPLES);
        assert!(d.examples_baseline.iter().all(|(dest, _, _)| dest == "b"));
        assert_eq!(d.examples_baseline[0].2, b"{\"v\":2}");
        assert_eq!(d.examples_candidate[0].2, b"{\"v\":7}");
    }

    #[test]
    fn the_multiset_counts_each_side_separately() {
        let (only_a, only_b, wanted_a, wanted_b) = multiset_diff(&[1, 1, 2, 3], &[1, 2, 2, 4]);
        // The baseline has one more 1 and one more 3; the candidate has one more 2 and one 4.
        assert_eq!((only_a, only_b), (2, 2));
        assert_eq!(wanted_a, HashMap::from([(1, 1), (3, 1)]));
        assert_eq!(wanted_b, HashMap::from([(2, 1), (4, 1)]));
    }

    #[test]
    fn retries_and_failures_make_the_verdict_inconclusive() {
        let codec = codec_named("prom_remote_write").unwrap();
        let clean = [sent(&[("a", vec![200]), ("b", vec![200])])];
        let retries = Retries::of(codec, &clean, &["a", "b"]);
        assert!(retries.is_empty());
        assert_eq!(verdict(false, &retries), Verdict::Identical);
        assert_eq!(verdict(true, &retries), Verdict::Different);

        // A retried attempt on `a`, which then succeeded, still makes the run inconclusive.
        let retried = [sent(&[("a", vec![503, 200]), ("b", vec![200])])];
        let retries = Retries::of(codec, &retried, &["a", "b"]);
        assert_eq!(retries.retried.get("a"), Some(&1));
        assert_eq!(verdict(false, &retries), Verdict::Inconclusive);
        assert_eq!(verdict(true, &retries), Verdict::Inconclusive);

        // A final non-success counts, and only the compared targets are looked at.
        let rejected = [sent(&[("a", vec![400]), ("z", vec![503, 503])])];
        let retries = Retries::of(codec, &rejected, &["a"]);
        assert_eq!(retries.not_accepted.get("a"), Some(&1));
        assert!(!retries.retried.contains_key("z"));
        assert_eq!(verdict(false, &retries), Verdict::Inconclusive);
    }

    #[test]
    fn an_empty_output_has_no_rows_to_compare() {
        let dir = tempfile::tempdir().unwrap();
        let empty = output("s3", dir.path());
        let scan = scan(&empty).unwrap();
        assert_eq!((scan.files, scan.keys.len()), (0, 0));
    }

    #[test]
    fn one_empty_output_refuses_the_run_even_when_the_other_sink_has_rows() {
        let dir = tempfile::tempdir().unwrap();
        let s3_full = write_output(dir.path(), "s3-full", &[("bucket/object", "{\"a\":1}\n")]);
        let zerobus_empty = dir.path().join("zerobus-empty");
        std::fs::create_dir(&zerobus_empty).unwrap();
        let outputs = [
            output("s3", &s3_full),
            Output {
                target: "b".into(),
                ..output("s3", &s3_full)
            },
            output("zerobus", &zerobus_empty),
            Output {
                target: "b".into(),
                ..output("zerobus", &zerobus_empty)
            },
        ];
        let scans: Vec<Scan> = outputs.iter().map(|o| scan(o).unwrap()).collect();
        let error = check_rows(&scans).unwrap_err().to_string();
        assert!(error.contains("has no rows"), "{error}");
        assert!(check_rows(&scans[..2]).is_ok());
    }

    #[test]
    fn a_truncated_or_gapped_sent_log_is_incomplete() {
        let at = |seq: u64| SentRequest {
            seq,
            iteration: 0,
            request: 0,
            items: 1,
            ts: 0,
            attempts: BTreeMap::new(),
        };
        let path = Path::new("sent.ndjson");
        // Two passes of three requests, written in any order.
        let whole: Vec<SentRequest> = [3, 0, 5, 1, 4, 2].into_iter().map(at).collect();
        assert!(check_complete(path, 2, 3, &whole).is_ok());
        let error = check_complete(path, 2, 3, &whole[..5])
            .unwrap_err()
            .to_string();
        assert!(error.contains("incomplete"), "{error}");
        let gapped: Vec<SentRequest> = [0, 1, 2, 3, 4, 6].into_iter().map(at).collect();
        assert!(check_complete(path, 2, 3, &gapped).is_err());
    }

    #[test]
    fn a_damaged_output_is_a_difference_even_when_its_rows_match() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out");
        std::fs::create_dir_all(out.join("bucket")).unwrap();
        let long = zstd::encode_all("{\"v\":1}\n".repeat(20_000).as_bytes(), 0).unwrap();
        std::fs::write(out.join("bucket/cut.json.zst"), &long[..long.len() - 4]).unwrap();
        let s3 = output("s3", &out);
        let scan = scan(&s3).unwrap();
        assert_eq!(scan.damaged, 1);
        assert!(!scan.keys.is_empty());
    }

    #[test]
    fn row_identity_delimits_fields_and_associates_metadata_with_rows() {
        assert_ne!(row_key("ab", 1, b"c"), row_key("a", 1, b"bc"));
        let base = [row_key("t", 1, b"a"), row_key("t", 2, b"b")];
        let swapped = [row_key("t", 2, b"a"), row_key("t", 1, b"b")];
        let (only_a, only_b, _, _) = multiset_diff(&base, &swapped);
        assert_eq!((only_a, only_b), (2, 2));
    }

    #[test]
    fn object_boundaries_do_not_count_but_stem_extension_and_codec_do() {
        let dir = tempfile::tempdir().unwrap();
        let base = write_output(dir.path(), "base", &[("b/x-1.json", "a\nb\n")]);
        let same = write_output(
            dir.path(),
            "same",
            &[("b/x-2.json", "b\n"), ("b/x-3.json", "a\n")],
        );
        let base_out = output("s3", &base);
        let base = scan(&base_out).unwrap();
        assert!(
            diff(&base, &scan(&output("s3", &same)).unwrap())
                .unwrap()
                .identical()
        );
        for (i, name) in ["b/wrong-1.json", "b/x-1.txt", "b/x-1.json"]
            .iter()
            .enumerate()
        {
            let wrong = write_output(dir.path(), &format!("wrong-{i}"), &[(name, "a\nb\n")]);
            if i == 2 {
                let mut gz =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                gz.write_all(b"a\nb\n").unwrap();
                std::fs::write(wrong.join(name), gz.finish().unwrap()).unwrap();
            }
            let d = diff(&base, &scan(&output("s3", &wrong)).unwrap()).unwrap();
            assert_eq!((d.only_baseline, d.only_candidate), (2, 2));
            assert_eq!(d.examples_baseline[0].2, b"a");
            assert_ne!(d.examples_baseline[0].1, d.examples_candidate[0].1);
        }
    }
}
