//! `gen`: replays a corpus to one or more targets, in the corpus's source protocol.
//!
//! Sends the corpus's requests in captured order, and repeats that pass `--iterations` times. Each
//! request goes to every target in parallel with identical bytes. Up to `--max-concurrency`
//! requests are in flight at once, so a slow target delays the run instead of piling up requests.
//! Targets therefore see the same items, and their outputs can be compared row for row.
//!
//! The codec encodes each request and classifies each response (see `source_codec`). This module
//! only paces, sends over HTTP, retries what the codec calls retryable, and writes the sent log.
//!
//! Items are restamped with their request's send time by the codec, not their captured time:
//! a sink may drop items outside a window around now, so a corpus captured earlier would be
//! dropped whole.
//!
//! `--items-per-sec` paces by items, not requests, because captured requests vary in size. A
//! request is due once every item sent before it has been paced out, so the rate holds whatever
//! the request sizes are.
//!
//! Connection errors and retryable statuses are retried with the same bytes and timestamp. A
//! target that stored a request before failing it stores it again, and the per-attempt statuses
//! show that.
//!
//! `--sent-log` is a new NDJSON file that is never replaced: it records one run, and it is the
//! only record of which items were sent. Its first line is the [`SentLogHeader`]: the corpus
//! (path, SHA-256 and header), the iteration count and the pacing. The header is written before
//! any traffic, so a bad path fails first. Each later line is one [`SentRequest`], written when
//! its request completes. Requests can finish out of order, so `seq` gives the send order.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use futures::StreamExt;
use serde::{Deserialize, Serialize};

use super::corpus::{Corpus, CorpusHeader};
use super::source_codec::{Outcome, SourceCodec, codec_named};
use super::{CmdResult, err, now_ms};

type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub type Client = hyper::Client<hyper::client::HttpConnector>;

/// The first retry waits `BACKOFF_BASE`; each later retry doubles the wait, up to `BACKOFF_MAX`.
const BACKOFF_BASE: Duration = Duration::from_millis(100);
const BACKOFF_MAX: Duration = Duration::from_secs(2);
/// Attempts per request per target. A retry resends the same bytes and timestamp.
const MAX_ATTEMPTS: u32 = 5;

#[derive(clap::Args)]
pub struct GenArgs {
    /// Corpus to replay, as written by `capture`.
    #[arg(long)]
    corpus: PathBuf,
    /// `name=url` of an endpoint, e.g. `a=http://127.0.0.1:9090/`. Repeatable; names must be
    /// unique because they key the sent log.
    #[arg(long = "target", required = true)]
    targets: Vec<String>,
    /// Sent log to write. Must not exist.
    #[arg(long)]
    sent_log: PathBuf,
    /// Passes over the corpus; each pass sends every request in captured order.
    #[arg(long, default_value_t = 1)]
    iterations: u64,
    /// Items per second, counted across the corpus's requests. An item is what the codec calls
    /// one unit of input (Prometheus samples for prom_remote_write). Unset means as fast as the
    /// targets accept.
    #[arg(long)]
    items_per_sec: Option<f64>,
    /// Requests in flight at once. Each one is sent to every target.
    #[arg(long, default_value_t = 16)]
    max_concurrency: usize,
    /// Seconds an attempt may take, from sending it to the end of its response. An attempt that
    /// runs longer counts as no response, and is retried like a connection error.
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
}

/// First line of the sent log: the corpus that was replayed and how the run was paced.
#[derive(Serialize, Deserialize)]
pub struct SentLogHeader {
    pub corpus: CorpusIdentity,
    pub iterations: u64,
    /// Requests in the corpus, so a reader can tell how many requests a complete log holds.
    pub requests_per_iteration: u64,
    /// Items per second the run was paced to; null when unpaced.
    pub items_per_sec: Option<f64>,
}

/// The corpus a run replayed: where it was, and its header, which names the codec.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CorpusIdentity {
    pub path: PathBuf,
    pub header: CorpusHeader,
}

/// One line of the sent log after the header.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct SentRequest {
    /// Send order, counting from 0 across all iterations.
    pub seq: u64,
    /// The pass over the corpus this request belongs to, counting from 0.
    pub iteration: u64,
    /// Index of the request in the corpus.
    pub request: u64,
    /// Items in the request.
    pub items: u64,
    /// Send time, stamped into every item of the request.
    pub ts: i64,
    /// HTTP status of each attempt, per target; 0 means a connection-level failure.
    pub attempts: BTreeMap<String, Vec<u16>>,
}

#[derive(Clone)]
pub struct Target {
    pub name: String,
    pub uri: hyper::Uri,
}

/// Parses `name=url` specs. Names must be unique, since they key the sent log. Only plain http is
/// accepted: the client has no TLS connector, so an https target would fail every attempt.
pub fn parse_targets(specs: &[String]) -> Result<Vec<Target>, BoxError> {
    let mut names = BTreeSet::new();
    let mut targets = Vec::with_capacity(specs.len());
    for spec in specs {
        let (name, url) = spec
            .split_once('=')
            .ok_or_else(|| err(format!("--target `{spec}`: expected name=url")))?;
        if name.is_empty() {
            return Err(err(format!(
                "--target `{spec}`: the name before `=` is empty"
            )));
        }
        if !names.insert(name) {
            return Err(err(format!(
                "--target `{name}`: more than one target has this name; names key the sent log"
            )));
        }
        let uri: hyper::Uri = url
            .parse()
            .map_err(|e| err(format!("--target `{spec}`: {e}")))?;
        if uri.scheme_str() != Some("http") {
            return Err(err(format!(
                "--target `{spec}`: only http:// URLs are supported"
            )));
        }
        targets.push(Target {
            name: name.to_owned(),
            uri,
        });
    }
    Ok(targets)
}

pub async fn run_cli(args: GenArgs) -> CmdResult {
    check_run_args(
        args.iterations,
        args.items_per_sec,
        args.max_concurrency,
        args.request_timeout_secs,
    )?;
    let targets = parse_targets(&args.targets)?;
    let corpus = Corpus::load(&args.corpus).map_err(|e| err(format!("--corpus: {e}")))?;
    let identity = CorpusIdentity {
        path: args.corpus.clone(),
        header: corpus.header.clone(),
    };
    let settings = RunSettings {
        iterations: args.iterations,
        items_per_sec: args.items_per_sec,
        max_concurrency: args.max_concurrency,
        request_timeout: Duration::from_secs(args.request_timeout_secs),
    };
    let summary = generate(&corpus, identity, &targets, &args.sent_log, settings).await?;
    summary.print(&args.sent_log);
    Ok(summary.all_delivered())
}

/// Rejects settings the run cannot honour, before the corpus is loaded or any file is created.
pub fn check_run_args(
    iterations: u64,
    items_per_sec: Option<f64>,
    max_concurrency: usize,
    request_timeout_secs: u64,
) -> Result<(), BoxError> {
    if iterations == 0 {
        return Err(err("--iterations must be at least 1"));
    }
    if let Some(rate) = items_per_sec
        && !(rate.is_finite() && rate > 0.0)
    {
        return Err(err(format!(
            "--items-per-sec must be a positive, finite number, got {rate}"
        )));
    }
    if max_concurrency == 0 {
        return Err(err("--max-concurrency must be at least 1"));
    }
    if request_timeout_secs == 0 {
        return Err(err("--request-timeout-secs must be at least 1"));
    }
    Ok(())
}

/// The run settings `generate` needs beyond the corpus and targets.
pub struct RunSettings {
    pub iterations: u64,
    pub items_per_sec: Option<f64>,
    pub max_concurrency: usize,
    pub request_timeout: Duration,
}

/// When a request is due, as an offset from the run's start: once the items sent before it have
/// been paced out at `items_per_sec`.
fn due_offset(items_before: u64, items_per_sec: f64) -> Duration {
    Duration::from_secs_f64(items_before as f64 / items_per_sec)
}

#[derive(Default)]
pub struct GenSummary {
    pub requests: u64,
    pub items: u64,
    pub elapsed: Duration,
    pub items_per_sec: Option<f64>,
    /// How far behind its due time a request was dispatched, at worst. Zero when unpaced.
    /// Large lag means the concurrency cap or the targets, not the pacing, set the rate.
    pub max_lag: Duration,
    /// Attempts beyond each request's first, per target.
    pub retries: BTreeMap<String, u64>,
    /// Status of each request's last attempt, per target.
    pub final_status: BTreeMap<String, BTreeMap<u16, u64>>,
}

impl GenSummary {
    fn add(&mut self, req: &SentRequest) {
        self.requests += 1;
        self.items += req.items;
        for (target, attempts) in &req.attempts {
            *self.retries.entry(target.clone()).or_default() +=
                attempts.len().saturating_sub(1) as u64;
            // Every request has at least one attempt, so the fallback is never reported.
            let last = attempts.last().copied().unwrap_or(0);
            *self
                .final_status
                .entry(target.clone())
                .or_default()
                .entry(last)
                .or_default() += 1;
        }
    }

    /// Whether every request's last attempt was accepted on every target. The codec decides what
    /// accepted means.
    pub fn all_delivered(&self) -> bool {
        self.final_status
            .values()
            .all(|statuses| statuses.keys().all(|status| (200..300).contains(status)))
    }

    pub fn print(&self, sent_log: &Path) {
        let secs = self.elapsed.as_secs_f64().max(1e-9);
        eprintln!(
            "gen: {} requests, {} items in {secs:.1}s",
            self.requests, self.items
        );
        let target = match self.items_per_sec {
            Some(rate) => format!(" (target {rate} items/s)"),
            None => " (unpaced)".to_owned(),
        };
        eprintln!(
            "gen: achieved {:.0} items/s per target{target}, max dispatch lag {:.0} ms",
            self.items as f64 / secs,
            self.max_lag.as_secs_f64() * 1000.0
        );
        for (target, statuses) in &self.final_status {
            let retries = self.retries.get(target).copied().unwrap_or(0);
            eprintln!(
                "gen: target {target} final statuses {statuses:?}, {retries} retried attempts"
            );
        }
        eprintln!("gen: sent log -> {}", sent_log.display());
        if !self.all_delivered() {
            eprintln!("gen: some requests were not accepted on every target; see the sent log");
        }
    }
}

pub async fn generate(
    corpus: &Corpus,
    identity: CorpusIdentity,
    targets: &[Target],
    sent_log: &Path,
    settings: RunSettings,
) -> Result<GenSummary, BoxError> {
    let header = SentLogHeader {
        corpus: identity,
        iterations: settings.iterations,
        requests_per_iteration: corpus.requests.len() as u64,
        items_per_sec: settings.items_per_sec,
    };
    let mut out = create_sent_log(sent_log, &header)?;
    let client: Client = hyper::Client::new();
    let codec = codec_named(&corpus.header.source_type)?;
    let timeout = settings.request_timeout;
    let start = Instant::now();

    // Every request of every pass in send order, with the time it is due when paced.
    let requests = (0..settings.iterations)
        .flat_map(|iteration| {
            corpus
                .requests
                .iter()
                .enumerate()
                .map(move |(request, items)| (iteration, request as u64, items))
        })
        .scan(0u64, |items_before, (iteration, request, items)| {
            let due = settings
                .items_per_sec
                .map(|rate| start + due_offset(*items_before, rate));
            *items_before += items.len() as u64;
            Some((iteration, request, items, due))
        })
        .zip(0u64..);

    // `buffer_unordered` pulls the next request only when fewer than `max_concurrency` are in
    // flight, so a request's lag counts both the pacing and the wait for a free slot.
    let client = &client;
    let sends = futures::stream::iter(requests)
        .then(|((iteration, request, items, due), seq)| async move {
            if let Some(due) = due {
                tokio::time::sleep_until(due.into()).await;
            }
            let lag = due.map_or(Duration::ZERO, |d| {
                Instant::now().saturating_duration_since(d)
            });
            (seq, iteration, request, items, lag)
        })
        .map(|(seq, iteration, request, items, lag)| async move {
            let ts = now_ms();
            let encoded = codec.encode(items, ts);
            let sends = targets
                .iter()
                .map(|t| send_with_retries(client, t, &encoded, codec, timeout));
            let statuses = futures::future::join_all(sends).await;
            let record = SentRequest {
                seq,
                iteration,
                request,
                items: items.len() as u64,
                ts,
                attempts: targets
                    .iter()
                    .map(|t| t.name.clone())
                    .zip(statuses)
                    .collect(),
            };
            (record, lag)
        })
        .buffer_unordered(settings.max_concurrency);
    let mut sends = std::pin::pin!(sends);

    let mut summary = GenSummary {
        items_per_sec: settings.items_per_sec,
        ..Default::default()
    };
    while let Some((record, lag)) = sends.next().await {
        summary.max_lag = summary.max_lag.max(lag);
        summary.add(&record);
        write_line(&mut out, sent_log, &record)?;
    }
    out.flush()
        .map_err(|e| err(format!("writing --sent-log {}: {e}", sent_log.display())))?;
    summary.elapsed = start.elapsed();
    Ok(summary)
}

/// Creates the sent log and writes its header. Fails if the path exists, so an earlier run's log
/// is never replaced. The header is written before any request is sent, so a bad path fails
/// before traffic starts.
fn create_sent_log(path: &Path, header: &SentLogHeader) -> Result<BufWriter<File>, BoxError> {
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| match e.kind() {
            ErrorKind::AlreadyExists => err(format!(
                "--sent-log {} already exists; pick a new path, a sent log is never replaced",
                path.display()
            )),
            _ => err(format!("creating --sent-log {}: {e}", path.display())),
        })?;
    let mut out = BufWriter::new(file);
    write_line(&mut out, path, header)?;
    Ok(out)
}

/// One NDJSON line: the record, then a newline.
fn write_line(out: &mut impl Write, path: &Path, value: &impl Serialize) -> Result<(), BoxError> {
    let wrap =
        |e: &dyn std::fmt::Display| err(format!("writing --sent-log {}: {e}", path.display()));
    serde_json::to_writer(&mut *out, value).map_err(|e| wrap(&e))?;
    out.write_all(b"\n").map_err(|e| wrap(&e))
}

/// Sends `encoded` to `target` until an attempt succeeds, fails without retry, or `MAX_ATTEMPTS`
/// is spent. Returns the status of every attempt, 0 for no response: a connection error or a
/// timeout.
async fn send_with_retries(
    client: &Client,
    target: &Target,
    encoded: &super::source_codec::Encoded,
    codec: &dyn SourceCodec,
    timeout: Duration,
) -> Vec<u16> {
    let mut statuses = Vec::new();
    for attempt in 0..MAX_ATTEMPTS {
        let status = match tokio::time::timeout(timeout, codec.send(client, target, encoded)).await
        {
            Ok(Ok(status)) => status,
            Ok(Err(_)) | Err(_) => 0,
        };
        statuses.push(status);
        if codec.classify(status) != Outcome::Retryable || attempt + 1 == MAX_ATTEMPTS {
            break;
        }
        tokio::time::sleep(backoff(attempt)).await;
    }
    statuses
}

/// Delay before retry `retry`, counting retries from 0.
fn backoff(retry: u32) -> Duration {
    (BACKOFF_BASE * 2u32.pow(retry.min(10))).min(BACKOFF_MAX)
}

#[cfg(test)]
#[allow(unused_imports, dead_code)] // Bench builds compile this module but never run it.
mod tests {
    use super::super::corpus::{RequestSizes, TapSettings};
    use super::super::source_codec::codec_named;
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn header() -> CorpusHeader {
        CorpusHeader {
            format: super::super::corpus::CORPUS_FORMAT.into(),
            source_type: "prom_remote_write".into(),
            origin: "test".into(),
            captured_at_ms: 0,
            tap: TapSettings {
                duration_secs: 1,
                interval_ms: 100,
                limit: 10,
            },
            events: 0,
            seed: 0,
            received_events: 0.0,
            coverage: 1.0,
            request_sizes: RequestSizes {
                sum: 0.0,
                count: 0.0,
            },
            items_per_request: None,
        }
    }

    fn identity() -> CorpusIdentity {
        CorpusIdentity {
            path: "c.ndjson".into(),
            header: header(),
        }
    }

    fn item(name: &str) -> super::super::source_codec::Item {
        serde_json::json!({"name": name, "labels": {}, "value": "1"})
    }

    #[test]
    fn pacing_is_by_cumulative_items_and_args_are_checked() {
        // Requests of 3, 1 and 2 items at 2 items/s: each is due once the items before it have
        // been paced out.
        assert_eq!(due_offset(0, 2.0), Duration::ZERO);
        assert_eq!(due_offset(3, 2.0), Duration::from_millis(1500));
        assert_eq!(due_offset(4, 2.0), Duration::from_secs(2));

        assert!(check_run_args(1, Some(0.5), 1, 30).is_ok());
        assert!(check_run_args(1, None, 16, 30).is_ok());
        for rate in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            let error = check_run_args(1, Some(rate), 16, 30)
                .unwrap_err()
                .to_string();
            assert!(error.contains("--items-per-sec"), "{error}");
        }
        let error = check_run_args(0, None, 16, 30).unwrap_err().to_string();
        assert!(error.contains("--iterations"), "{error}");
        let error = check_run_args(1, None, 0, 30).unwrap_err().to_string();
        assert!(error.contains("--max-concurrency"), "{error}");
        let error = check_run_args(1, None, 16, 0).unwrap_err().to_string();
        assert!(error.contains("--request-timeout-secs"), "{error}");
    }

    #[test]
    fn no_more_than_max_concurrency_requests_are_in_flight() {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let make = {
            let (in_flight, peak) = (Arc::clone(&in_flight), Arc::clone(&peak));
            hyper::service::make_service_fn(move |_| {
                let (in_flight, peak) = (Arc::clone(&in_flight), Arc::clone(&peak));
                async move {
                    Ok::<_, std::convert::Infallible>(hyper::service::service_fn(
                        move |_req: hyper::Request<hyper::Body>| {
                            let (in_flight, peak) = (Arc::clone(&in_flight), Arc::clone(&peak));
                            async move {
                                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                                peak.fetch_max(now, Ordering::SeqCst);
                                tokio::time::sleep(Duration::from_millis(20)).await;
                                in_flight.fetch_sub(1, Ordering::SeqCst);
                                Ok::<_, std::convert::Infallible>(hyper::Response::new(
                                    hyper::Body::empty(),
                                ))
                            }
                        },
                    ))
                }
            })
        };

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let server = hyper::Server::bind(&"127.0.0.1:0".parse().unwrap()).serve(make);
            let addr = server.local_addr();
            tokio::spawn(server);
            let targets = [Target {
                name: "a".into(),
                uri: format!("http://{addr}/").parse().unwrap(),
            }];
            let corpus = Corpus {
                header: header(),
                requests: (0..12).map(|_| vec![item("up")]).collect(),
            };
            let dir = tempfile::tempdir().unwrap();
            let sent_log = dir.path().join("sent.ndjson");
            let settings = RunSettings {
                iterations: 1,
                items_per_sec: None,
                max_concurrency: 3,
                request_timeout: Duration::from_secs(30),
            };
            let summary = generate(&corpus, identity(), &targets, &sent_log, settings)
                .await
                .unwrap();
            assert_eq!(summary.requests, 12);
            assert!(summary.all_delivered());
            let lines = std::fs::read_to_string(&sent_log).unwrap();
            assert_eq!(lines.lines().count(), 13, "header and one line per request");
        });
        let peak = peak.load(Ordering::SeqCst);
        assert!((1..=3).contains(&peak), "peak in flight was {peak}");
    }

    #[tokio::test]
    async fn an_attempt_past_the_timeout_counts_as_no_response() {
        // The listener accepts connections and never answers, so every attempt times out.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                held.push(stream);
            }
        });
        let target = Target {
            name: "a".into(),
            uri: format!("http://{addr}/").parse().unwrap(),
        };
        let codec = codec_named("prom_remote_write").unwrap();
        let encoded = codec.encode(&[], 0);
        let statuses = send_with_retries(
            &Client::new(),
            &target,
            &encoded,
            codec,
            Duration::from_millis(50),
        )
        .await;
        assert_eq!(statuses, vec![0; MAX_ATTEMPTS as usize]);
    }

    #[test]
    fn targets_are_unique_named_http_urls() {
        let targets = parse_targets(&[
            "a=http://127.0.0.1:9100/".into(),
            "b=http://127.0.0.1:9101/".into(),
        ])
        .unwrap();
        let names: Vec<&str> = targets.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["a", "b"]);

        let bad = [
            ("127.0.0.1:9100", "name=url"),
            ("=http://127.0.0.1:9100/", "name before"),
            ("a=https://127.0.0.1:9100/", "only http://"),
        ];
        for (spec, expected) in bad {
            let error = parse_targets(&[spec.into()]).err().unwrap().to_string();
            assert!(error.contains("--target"), "{error}");
            assert!(error.contains(expected), "{error}");
        }
        let error = parse_targets(&[
            "a=http://127.0.0.1:9100/".into(),
            "a=http://127.0.0.1:9101/".into(),
        ])
        .err()
        .unwrap()
        .to_string();
        assert!(error.contains("`a`"), "{error}");
    }

    #[test]
    fn sent_log_lines_round_trip() {
        let header = SentLogHeader {
            corpus: identity(),
            iterations: 2,
            requests_per_iteration: 3,
            items_per_sec: None,
        };
        let line = serde_json::to_string(&header).unwrap();
        assert!(
            line.starts_with(r#"{"corpus":{"path":"c.ndjson","header":"#),
            "{line}"
        );
        assert!(
            line.contains(r#""iterations":2,"requests_per_iteration":3,"items_per_sec":null}"#),
            "{line}"
        );
        let back: SentLogHeader = serde_json::from_str(&line).unwrap();
        assert_eq!(serde_json::to_string(&back).unwrap(), line);

        let record = SentRequest {
            seq: 7,
            iteration: 0,
            request: 2,
            items: 5,
            ts: 1_794_000_000_000,
            attempts: BTreeMap::from([("a".to_owned(), vec![503, 200])]),
        };
        let line = serde_json::to_string(&record).unwrap();
        assert!(
            line.starts_with(r#"{"seq":7,"iteration":0,"request":2,"items":5,"#),
            "{line}"
        );
        let back: SentRequest = serde_json::from_str(&line).unwrap();
        assert_eq!(back, record);
    }

    #[test]
    fn sent_log_starts_with_the_header_and_is_never_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sent.ndjson");
        let header = SentLogHeader {
            corpus: identity(),
            iterations: 1,
            requests_per_iteration: 1,
            items_per_sec: Some(20000.0),
        };
        let mut out = create_sent_log(&path, &header).unwrap();
        out.flush().unwrap();
        drop(out);
        let written = std::fs::read_to_string(&path).unwrap();
        assert_eq!(written.lines().count(), 1);
        assert!(
            written.trim_end().ends_with(r#""items_per_sec":20000.0}"#),
            "{written}"
        );

        let error = create_sent_log(&path, &header).err().unwrap().to_string();
        assert!(error.contains("--sent-log"), "{error}");
        assert!(error.contains(&path.display().to_string()), "{error}");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), written);
    }
}
