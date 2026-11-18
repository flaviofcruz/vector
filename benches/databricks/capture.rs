//! `capture`: record real traffic from a live pod and rebuild it into requests of production's
//! mean size.
//!
//! Runs `vector tap` inside the pod (`kubectl exec`) on the outputs of the source to capture, so
//! nothing has to be installed or port-forwarded locally. Every Vector pod woodchuck deploys has
//! the API that tap needs enabled. The source's codec (see `source_codec`) decides what a tapped
//! event is; this module only taps, counts and cuts.
//!
//! Tap returns events, not requests, so the requests are rebuilt:
//! - Sizes follow production's mean. The source counts the events of each HTTP request it receives
//!   in a histogram, which the pod's exporter serves. `capture` scrapes its `_sum` and `_count`
//!   before and after tapping, and cuts every request to their mean over the capture (or to
//!   `--items-per-request`). Production's sender batches about 5,000 items, so grouping by
//!   timestamp, which this replaced, gave requests about 25x too small.
//! - Coverage is measured. The events tap returned, against the source's received-events total
//!   over the same tap run. Below `MIN_COVERAGE` the corpus is a sample of the traffic.
//! - Mix is random. Items are shuffled with a recorded seed and cut in that order, because
//!   production's batches mix items from several senders, and tap's order would keep one sender's
//!   items together.
//!
//! Still not reproduced (TODO in README.md):
//! - the spread of request sizes: every request is the mean, except the last, which may be smaller;
//! - request boundaries and grouping, which production gets from telegraf-router's buffering;
//! - arrival timing, since tap has none;
//! - completeness beyond the coverage figure, since tap samples events and drops them when its
//!   buffer is full.
//!
//! TODO(follow-up PR): a config-gated raw-request tee in the source would give exact bodies,
//! boundaries and arrival times.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command, Stdio};

use rand::SeedableRng;
use rand::seq::SliceRandom;
use rand_chacha::ChaCha8Rng;

use super::corpus::{
    CORPUS_FORMAT, Corpus, CorpusHeader, ItemsPerRequest, RequestSizes, SizeSource, TapSettings,
};
use super::source_codec::{Item, SourceCodec, codec_named};
use super::{CmdResult, err, now_ms};

const EVENTS_TOTAL: &str = "vector_component_received_events_total";
/// The request-size histogram's `_sum` and `_count` series, under the codec's metric name (see
/// `SourceCodec`). Its buckets are not read.
const REQUEST_SUM: &str = "vector_component_received_events_count_sum";
const REQUEST_COUNT: &str = "vector_component_received_events_count_count";
/// Below this share of the source's events, the corpus is a sample of the traffic, not all of it.
const MIN_COVERAGE: f64 = 0.95;

#[derive(clap::Args)]
pub struct CaptureArgs {
    /// kubectl context of the pod's cluster.
    #[arg(long)]
    context: String,
    #[arg(long)]
    namespace: String,
    /// Pod to tap.
    #[arg(long)]
    pod: String,
    /// Container running Vector.
    #[arg(long)]
    container: String,
    /// The pod's exporter port, which serves the source's internal metrics. It is scraped through
    /// the API server's pod proxy, so the image needs neither curl nor wget.
    #[arg(long, default_value_t = 7777)]
    exporter_port: u16,
    /// Component whose outputs to tap: the source, e.g. `prom_remote_write`.
    #[arg(long)]
    component: String,
    /// The source's codec, by name (e.g. `prom_remote_write`). Decides how its events are decoded
    /// and how they are sent.
    #[arg(long)]
    source_type: String,
    /// Corpus to write. An existing file is never replaced.
    #[arg(long)]
    out: PathBuf,
    /// How long to tap.
    #[arg(long, default_value_t = 60)]
    duration_secs: u64,
    /// Tap emits one sample of events per interval; shorter intervals capture more of a busy
    /// stream.
    #[arg(long, default_value_t = 100)]
    interval_ms: u64,
    /// Events tap keeps from each interval (its API allows at most 10,000). A capture holds at
    /// most `duration / interval × limit` events.
    #[arg(long, default_value_t = 10_000)]
    limit: u64,
    /// Items per request, instead of the source's mean over the capture. The scrape still runs,
    /// for the coverage figure.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    items_per_request: Option<u64>,
    /// Seed for shuffling the tapped items into requests. Random when unset. It is recorded in the
    /// corpus.
    #[arg(long)]
    seed: Option<u64>,
}

pub fn capture(args: CaptureArgs) -> CmdResult {
    let codec = codec_named(&args.source_type)?;
    let tap = TapSettings {
        duration_secs: args.duration_secs,
        interval_ms: args.interval_ms,
        limit: args.limit,
    };
    let seed = args.seed.unwrap_or_else(rand::random);
    let before = scrape(&args, codec).map_err(err)?;
    let mut cmd = kubectl_exec(&args);
    cmd.args(["vector", "tap", "--outputs-of", &args.component])
        .args(["--format", "json", "--quiet"])
        .args(["--interval", &tap.interval_ms.to_string()])
        .args(["--limit", &tap.limit.to_string()])
        .args(["--duration-ms", &(tap.duration_secs * 1000).to_string()])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let origin = format!(
        "{}/{}/{}/{} {}",
        args.context, args.namespace, args.pod, args.container, args.component
    );
    eprintln!(
        "capture: tapping {origin} for {}s ({} intervals of up to {} events)",
        tap.duration_secs,
        tap.intervals(),
        tap.limit
    );

    let mut decoder = Decoder::new(codec);
    let mut child = cmd
        .spawn()
        .map_err(|e| err(format!("running kubectl: {e}")))?;
    let stdout = child.stdout.take().expect("piped stdout");
    for line in BufReader::new(stdout).lines() {
        decoder.line(&line?);
    }
    let status = child.wait()?;
    if !status.success() {
        if decoder.items.is_empty() {
            return Err(err(format!("kubectl exec … vector tap failed: {status}")));
        }
        eprintln!("capture: warning: kubectl exec … vector tap exited with {status}");
    }

    if !decoder.rejected.is_empty() {
        return Err(err(format!(
            "{} of {} events could not be decoded by codec `{}` {:?}; check --component and --source-type",
            decoder.rejected.values().sum::<u64>(),
            decoder.events,
            codec.name(),
            decoder.rejected,
        )));
    }
    if decoder.items.is_empty() {
        return Err(err(
            "no events captured; check --component and that the pod is receiving traffic",
        ));
    }

    let after = scrape(&args, codec).map_err(err)?;
    let received = received_between(&before, &after).map_err(err)?;
    let events = decoder.events;
    let coverage = coverage(events, received.events);
    eprintln!(
        "capture: tap returned {events} events (at most {} possible); the source received {:.0}, so coverage is {:.1}%",
        tap.max_events(),
        received.events,
        coverage * 100.0
    );
    if coverage < MIN_COVERAGE {
        eprintln!(
            "capture: warning: coverage is below {:.0}%: tap returned fewer events than the source received (it keeps at most --limit per interval, and drops events when its buffer is full), so the corpus is a sample of the source's traffic, not all of it",
            MIN_COVERAGE * 100.0
        );
    }

    let used = items_per_request(args.items_per_request, &received.requests).map_err(err)?;
    let requests = rebuild_requests(decoder.items, used.size as usize, seed);
    let corpus = Corpus {
        header: CorpusHeader {
            format: CORPUS_FORMAT.into(),
            source_type: codec.name().into(),
            origin,
            captured_at_ms: now_ms(),
            tap,
            events,
            seed,
            received_events: received.events,
            coverage,
            request_sizes: received.requests,
            items_per_request: Some(used),
        },
        requests,
    };
    corpus.write(&args.out)?;
    // Read it back the way replays will, so a bad corpus fails here rather than mid-run.
    let written = Corpus::load(&args.out)?;
    if written.items() != corpus.items() || written.requests.len() != corpus.requests.len() {
        return Err(err(format!(
            "{}: corpus did not read back intact",
            args.out.display()
        )));
    }
    let basis = match used.source {
        SizeSource::Flag => "from --items-per-request".to_owned(),
        SizeSource::Mean => format!(
            "the source's mean of {:.1} events per request",
            received.requests.sum / received.requests.count
        ),
    };
    eprintln!(
        "capture: {} requests of up to {} items ({basis}), seed {seed} -> {}",
        corpus.requests.len(),
        used.size,
        args.out.display()
    );
    Ok(true)
}

fn kubectl_exec(args: &CaptureArgs) -> Command {
    let mut cmd = Command::new("kubectl");
    cmd.args(["--context", &args.context, "--namespace", &args.namespace])
        .args(["exec", &args.pod, "--container", &args.container, "--"]);
    cmd
}

/// A label set, sorted by key.
pub(super) type Labels = Vec<(String, String)>;

/// A counter's name and full label set.
type ReadingKey = (String, Labels);

/// The source's received-event counters at one scrape. Keyed by name and full label set, so a
/// counter that reset is caught even though the deltas are summed across label sets.
struct Scrape {
    readings: BTreeMap<ReadingKey, f64>,
}

fn scrape(args: &CaptureArgs, codec: &dyn SourceCodec) -> Result<Scrape, String> {
    let path = exporter_path(&args.namespace, &args.pod, args.exporter_port);
    let out = Command::new("kubectl")
        .args(["--context", &args.context, "get", "--raw", &path])
        .output()
        .map_err(|e| format!("running kubectl: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "scraping {path} through the API server failed ({}): {}; the pod proxy needs `get pods/proxy` RBAC on the pod",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    let readings = received_readings(
        &String::from_utf8_lossy(&out.stdout),
        &args.component,
        codec.request_size_metric(),
    )?;
    if readings.is_empty() {
        return Err(format!(
            "{path} has no vector_component_received_events counters for component_id=\"{}\"",
            args.component
        ));
    }
    Ok(Scrape { readings })
}

/// The API server's path to the pod's exporter through its proxy subresource. `kubectl get --raw`
/// sends it unchanged.
pub(super) fn exporter_path(namespace: &str, pod: &str, port: u16) -> String {
    format!("/api/v1/namespaces/{namespace}/pods/{pod}:{port}/proxy/metrics")
}

/// The received-event counters of `component`, from one exporter response. The request-size
/// histogram's `_sum` and `_count` are read under its codec's name `histogram` and keyed by the
/// constants above; its buckets are skipped.
fn received_readings(
    text: &str,
    component: &str,
    histogram: &str,
) -> Result<BTreeMap<ReadingKey, f64>, String> {
    let canonical = |name: &str| -> Option<&'static str> {
        if name == EVENTS_TOTAL {
            return Some(EVENTS_TOTAL);
        }
        match name.strip_prefix(histogram)? {
            "_sum" => Some(REQUEST_SUM),
            "_count" => Some(REQUEST_COUNT),
            _ => None,
        }
    };
    Ok(parse_exposition(text)?
        .into_iter()
        .filter(|s| {
            canonical(&s.name).is_some() && label(&s.labels, "component_id") == Some(component)
        })
        .map(|s| {
            (
                (canonical(&s.name).unwrap_or_default().to_owned(), s.labels),
                s.value,
            )
        })
        .collect())
}

/// One line of the Prometheus text exposition format.
pub(super) struct Reading {
    pub(super) name: String,
    /// Sorted by key.
    pub(super) labels: Vec<(String, String)>,
    pub(super) value: f64,
}

/// Parses `name{key="value",...} value [timestamp]` lines, skipping comments and blank lines.
pub(super) fn parse_exposition(text: &str) -> Result<Vec<Reading>, String> {
    let mut readings = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parsed = parse_line(line).map_err(|e| format!("metrics line `{line}`: {e}"))?;
        readings.push(parsed);
    }
    Ok(readings)
}

fn parse_line(line: &str) -> Result<Reading, String> {
    let name_end = line
        .find(|c: char| c == '{' || c.is_whitespace())
        .unwrap_or(line.len());
    let name = &line[..name_end];
    if name.is_empty() {
        return Err("no metric name".into());
    }
    let mut rest = &line[name_end..];
    let mut labels = Vec::new();
    if let Some(after_brace) = rest.strip_prefix('{') {
        let (parsed, after) = parse_labels(after_brace)?;
        labels = parsed;
        rest = after;
    }
    let value = rest.split_whitespace().next().ok_or("no value")?;
    let value = value
        .parse::<f64>()
        .map_err(|_| format!("bad value `{value}`"))?;
    labels.sort();
    Ok(Reading {
        name: name.to_owned(),
        labels,
        value,
    })
}

/// Parses `key="value",...}` (the text after `{`) into labels, returning the text after `}`.
/// Values may hold `\\`, `\"` and `\n` escapes, and commas.
fn parse_labels(mut s: &str) -> Result<(Labels, &str), String> {
    let mut labels = Vec::new();
    loop {
        s = s.trim_start();
        if let Some(rest) = s.strip_prefix('}') {
            return Ok((labels, rest));
        }
        let eq = s.find('=').ok_or("label without `=`")?;
        let key = s[..eq].trim().to_owned();
        s = s[eq + 1..]
            .trim_start()
            .strip_prefix('"')
            .ok_or("label value not quoted")?;
        let mut value = String::new();
        let mut escaped = false;
        let mut end = None;
        for (i, c) in s.char_indices() {
            match (escaped, c) {
                (true, 'n') => value.push('\n'),
                (true, c) => value.push(c),
                (false, '\\') => {
                    escaped = true;
                    continue;
                }
                (false, '"') => {
                    end = Some(i);
                    break;
                }
                (false, c) => value.push(c),
            }
            escaped = false;
        }
        let end = end.ok_or("unterminated label value")?;
        labels.push((key, value));
        s = s[end + 1..].trim_start();
        if let Some(rest) = s.strip_prefix(',') {
            s = rest;
        }
    }
}

pub(super) fn label<'a>(labels: &'a [(String, String)], key: &str) -> Option<&'a str> {
    labels
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

/// What the source received between two scrapes, as its exporter counted it.
struct Received {
    /// Growth of the received-events total.
    events: f64,
    /// Growth of the events-per-request histogram's sum and count.
    requests: RequestSizes,
}

/// Deltas between two scrapes. A restart is caught only when a counter vanished or went down; a
/// pod that restarted and whose counters grew back past their earlier values is not detected.
fn received_between(before: &Scrape, after: &Scrape) -> Result<Received, String> {
    for (key, was) in &before.readings {
        match after.readings.get(key) {
            None => {
                return Err(format!(
                    "{} disappeared during the capture; did the pod restart?",
                    key.0
                ));
            }
            Some(now) if now < was => {
                return Err(format!(
                    "{} went from {was} to {now}: a counter reset, so the pod restarted during the capture; run it again",
                    key.0
                ));
            }
            Some(_) => {}
        }
    }
    let (mut events, mut sum, mut count) = (0.0, 0.0, 0.0);
    for (key, now) in &after.readings {
        let grown = now - before.readings.get(key).copied().unwrap_or(0.0);
        match key.0.as_str() {
            EVENTS_TOTAL => events += grown,
            REQUEST_SUM => sum += grown,
            REQUEST_COUNT => count += grown,
            _ => {}
        }
    }
    Ok(Received {
        events,
        requests: RequestSizes { sum, count },
    })
}

/// Share of the source's events that tap returned.
fn coverage(tapped: u64, received: f64) -> f64 {
    tapped as f64 / received
}

/// The size every request is cut to: the flag if one is given, else the capture's mean events per
/// request, rounded. The mean is the only figure of the source's sizes a corpus carries.
fn items_per_request(flag: Option<u64>, sizes: &RequestSizes) -> Result<ItemsPerRequest, String> {
    if let Some(size) = flag {
        return Ok(ItemsPerRequest {
            size,
            source: SizeSource::Flag,
        });
    }
    if sizes.count <= 0.0 {
        return Err(
            "the source received no requests during the capture, so there is no mean request size; pass --items-per-request N"
                .into(),
        );
    }
    let mean = (sizes.sum / sizes.count).round();
    if mean < 1.0 {
        return Err(format!(
            "the source's mean is {:.2} events per request, which rounds to 0; pass --items-per-request N",
            sizes.sum / sizes.count
        ));
    }
    Ok(ItemsPerRequest {
        size: mean as u64,
        source: SizeSource::Mean,
    })
}

/// Shuffles the items with `seed`, then cuts them in order into requests of `size` items. Every
/// request has `size` items except possibly the last. `size` is at least 1.
fn rebuild_requests(mut items: Vec<Item>, size: usize, seed: u64) -> Vec<Vec<Item>> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    items.shuffle(&mut rng);
    let mut items = items.into_iter();
    let mut requests = Vec::new();
    loop {
        let request: Vec<Item> = items.by_ref().take(size).collect();
        if request.is_empty() {
            return requests;
        }
        requests.push(request);
    }
}

/// Decodes tap's output lines into items with the codec, counting what it could not decode.
struct Decoder {
    codec: &'static dyn SourceCodec,
    events: u64,
    /// Events that could not be turned into an item, by reason.
    rejected: BTreeMap<&'static str, u64>,
    items: Vec<Item>,
}

impl Decoder {
    fn new(codec: &'static dyn SourceCodec) -> Self {
        Self {
            codec,
            events: 0,
            rejected: BTreeMap::new(),
            items: Vec::new(),
        }
    }

    fn line(&mut self, line: &str) {
        if line.trim().is_empty() {
            return;
        }
        self.events += 1;
        match self.codec.decode_tap(line) {
            Ok(item) => self.items.push(item),
            Err(reason) => *self.rejected.entry(reason).or_default() += 1,
        }
    }
}

#[cfg(test)]
#[allow(unused_imports, dead_code)] // Bench builds compile this module but never run it.
mod tests {
    use super::*;
    use serde_json::json;

    /// The request-size histogram's name in the exposition: the default of `SourceCodec`.
    const HIST: &str = "vector_component_received_events_count";

    /// Items named `s0`, `s1`, ... so tests can tell them apart.
    fn items(n: usize) -> Vec<Item> {
        (0..n).map(|i| json!({"name": format!("s{i}")})).collect()
    }

    fn names(requests: &[Vec<Item>]) -> Vec<Vec<String>> {
        requests
            .iter()
            .map(|r| {
                r.iter()
                    .map(|s| s["name"].as_str().unwrap().to_owned())
                    .collect()
            })
            .collect()
    }

    fn sizes(sum: f64, count: f64) -> RequestSizes {
        RequestSizes { sum, count }
    }

    #[test]
    fn the_exporter_is_scraped_through_the_pod_proxy() {
        assert_eq!(
            exporter_path("vector-aggregator", "pod-1", 7777),
            "/api/v1/namespaces/vector-aggregator/pods/pod-1:7777/proxy/metrics"
        );
    }

    #[test]
    fn tap_lines_are_decoded_by_the_codec_and_counted() {
        let codec = codec_named("prom_remote_write").unwrap();
        let mut d = Decoder::new(codec);
        d.line(r#"{"name":"up","tags":{},"kind":"absolute","gauge":{"value":1.0}}"#);
        d.line(r#"{"name":"c","tags":{},"kind":"absolute","counter":{"value":1.0}}"#);
        d.line("");
        assert_eq!(d.events, 2);
        assert_eq!(d.items.len(), 1);
        assert_eq!(d.rejected.get("not_a_gauge"), Some(&1));
    }

    #[test]
    fn parses_labels_in_any_order_with_escapes() {
        let readings = parse_exposition(
            "# HELP x help text\n\
             m{b=\"x,y\",a=\"q\\\"r\\\\s\\nt\",le=\"+Inf\"} 1.5 1700000000000\n\
             \n\
             n 2\n\
             k{x=\"NaN\"} NaN\n",
        )
        .unwrap();
        assert_eq!(readings.len(), 3);
        assert_eq!(readings[0].name, "m");
        assert_eq!(
            readings[0].labels,
            vec![
                ("a".into(), "q\"r\\s\nt".into()),
                ("b".into(), "x,y".into()),
                ("le".into(), "+Inf".into()),
            ]
        );
        assert_eq!(readings[0].value, 1.5);
        assert!(readings[1].labels.is_empty());
        assert_eq!(readings[1].value, 2.0);
        assert!(readings[2].value.is_nan());
        assert!(parse_exposition("m{a=\"x} 1\n").is_err());
        assert!(parse_exposition("m{a=x} 1\n").is_err());
    }

    #[test]
    fn selects_the_component_and_skips_the_buckets() {
        let text = "\
            vector_component_received_events_total{component_kind=\"source\",component_id=\"prom\"} 7\n\
            vector_component_received_events_total{component_id=\"other\"} 9\n\
            vector_component_received_events_count_bucket{le=\"+Inf\",component_id=\"prom\"} 2\n\
            vector_component_received_events_count_sum{component_id=\"prom\"} 3\n\
            vector_component_received_events_count_count{component_id=\"prom\"} 2\n\
            vector_http_server_requests_received_total 5\n";
        let readings = received_readings(text, "prom", HIST).unwrap();
        // The total, the sum and the count; the bucket is not read.
        assert_eq!(readings.len(), 3);
        assert!(readings.keys().all(|(name, _)| !name.contains("bucket")));
        assert!(received_readings(text, "absent", HIST).unwrap().is_empty());
    }

    #[test]
    fn request_sizes_are_the_growth_of_sum_and_count_across_label_sets() {
        let before = "\
            vector_component_received_events_total{component_id=\"c\"} 100\n\
            vector_component_received_events_count_bucket{component_id=\"c\",le=\"10\"} 3\n\
            vector_component_received_events_count_sum{component_id=\"c\"} 90\n\
            vector_component_received_events_count_count{component_id=\"c\"} 5\n\
            vector_component_received_events_count_sum{component_id=\"c\",host=\"a\"} 10\n\
            vector_component_received_events_count_count{component_id=\"c\",host=\"a\"} 1\n";
        let after = "\
            vector_component_received_events_total{component_id=\"c\"} 400\n\
            vector_component_received_events_count_bucket{component_id=\"c\",le=\"10\"} 7\n\
            vector_component_received_events_count_sum{component_id=\"c\"} 390\n\
            vector_component_received_events_count_count{component_id=\"c\"} 12\n\
            vector_component_received_events_count_sum{component_id=\"c\",host=\"a\"} 30\n\
            vector_component_received_events_count_count{component_id=\"c\",host=\"a\"} 3\n";
        let scrape = |text| Scrape {
            readings: received_readings(text, "c", HIST).unwrap(),
        };
        let r = received_between(&scrape(before), &scrape(after)).unwrap();
        assert_eq!(r.events, 300.0);
        // Sum: (390 − 90) + (30 − 10) = 320. Count: (12 − 5) + (3 − 1) = 9.
        assert_eq!((r.requests.sum, r.requests.count), (320.0, 9.0));
    }

    #[test]
    fn a_counter_reset_is_an_error() {
        let text = |total: f64| {
            format!(
                "vector_component_received_events_total{{component_id=\"c\"}} {total}\n\
                 vector_component_received_events_count_count{{component_id=\"c\"}} 1\n"
            )
        };
        let scrape = |t: f64| Scrape {
            readings: received_readings(&text(t), "c", HIST).unwrap(),
        };
        let error = received_between(&scrape(50.0), &scrape(10.0))
            .err()
            .unwrap();
        assert!(error.contains("went from 50 to 10"), "{error}");
        // A counter that vanished (its pod restarted) is also an error.
        let gone = Scrape {
            readings: BTreeMap::new(),
        };
        assert!(received_between(&scrape(50.0), &gone).is_err());
    }

    #[test]
    fn the_size_is_the_rounded_mean_of_the_requests() {
        // 11,564,281 events in 5,479 requests is 2,110.7 a request.
        let mean = items_per_request(None, &sizes(11_564_281.0, 5479.0)).unwrap();
        assert_eq!((mean.size, mean.source), (2111, SizeSource::Mean));
        // 320 events in 9 requests is 35.6, which rounds to 36.
        let mean = items_per_request(None, &sizes(320.0, 9.0)).unwrap();
        assert_eq!((mean.size, mean.source), (36, SizeSource::Mean));
    }

    #[test]
    fn the_flag_overrides_the_mean_and_needs_no_requests() {
        let flagged = items_per_request(Some(5000), &sizes(320.0, 9.0)).unwrap();
        assert_eq!((flagged.size, flagged.source), (5000, SizeSource::Flag));
        let flagged = items_per_request(Some(7), &sizes(0.0, 0.0)).unwrap();
        assert_eq!((flagged.size, flagged.source), (7, SizeSource::Flag));
    }

    #[test]
    fn no_requests_or_a_mean_under_half_is_refused_without_the_flag() {
        let error = items_per_request(None, &sizes(0.0, 0.0)).unwrap_err();
        assert!(error.contains("no requests"), "{error}");
        assert!(error.contains("--items-per-request"), "{error}");
        // 4 events in 10 requests is a mean of 0.4, which rounds to 0.
        let error = items_per_request(None, &sizes(4.0, 10.0)).unwrap_err();
        assert!(error.contains("rounds to 0"), "{error}");
        // 5 events in 10 requests is 0.5, which rounds to 1.
        assert_eq!(items_per_request(None, &sizes(5.0, 10.0)).unwrap().size, 1);
    }

    #[test]
    fn cutting_is_deterministic_for_a_seed() {
        let a = rebuild_requests(items(500), 18, 7);
        let b = rebuild_requests(items(500), 18, 7);
        assert_eq!(names(&a), names(&b));
        let c = rebuild_requests(items(500), 18, 8);
        assert_ne!(names(&a), names(&c));
    }

    #[test]
    fn cutting_puts_every_item_in_one_request_of_the_size_but_the_last() {
        let requests = rebuild_requests(items(500), 7, 3);
        // 500 = 71 × 7 + 3.
        assert_eq!(requests.len(), 72);
        assert!(requests[..71].iter().all(|r| r.len() == 7));
        assert_eq!(requests[71].len(), 3);
        let mut all: Vec<String> = names(&requests).concat();
        assert_eq!(all.len(), 500);
        all.sort();
        all.dedup();
        assert_eq!(all.len(), 500);
    }

    #[test]
    fn coverage_is_tapped_over_received() {
        assert_eq!(coverage(950, 1000.0), 0.95);
        assert!(coverage(949, 1000.0) < MIN_COVERAGE);
    }
}
