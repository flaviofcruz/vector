//! Corpus: input requests rebuilt from traffic captured on a live pod (see `capture`).
//!
//! On disk it is NDJSON: a header line, then one line per request holding that request's items:
//!
//! ```text
//! {"format":"databricks-bench-corpus/3","source_type":"prom_remote_write",...}
//! {"items":[{"name":"up","labels":{"job":"x"},"value":"1"}, ...]}   (a prom_remote_write request)
//! ```
//!
//! The header records how the capture was made: the mean request size of the source, the size the
//! items were cut to, the seed that shuffled them, and how much of the source's traffic tap
//! returned. Items are the codec's JSON (see `source_codec`), and requests carry no timestamp: a
//! replay stamps each item with its send time.

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};

use super::source_codec::{Item, codec_named};
use super::{err, io_at};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub const CORPUS_FORMAT: &str = "databricks-bench-corpus/3";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CorpusHeader {
    pub format: String,
    /// The codec's name: it decides how the items are decoded and sent.
    pub source_type: String,
    /// Where the capture came from: `context/namespace/pod/container component`.
    pub origin: String,
    pub captured_at_ms: i64,
    pub tap: TapSettings,
    /// Events tap returned; every one became an item.
    pub events: u64,
    /// Seed of the shuffle that cut the items into requests.
    pub seed: u64,
    /// Growth of the source's received-events total over the capture.
    pub received_events: f64,
    /// `events / received_events`: the share of the source's events tap returned.
    pub coverage: f64,
    /// Events per received HTTP request, as the source counted them over the capture.
    pub request_sizes: RequestSizes,
    /// The size every request was cut to. Absent in corpora captured before it was recorded.
    #[serde(default)]
    pub items_per_request: Option<ItemsPerRequest>,
}

/// The source's events-per-request histogram, reduced to its totals: the growth of its `_sum` and
/// `_count` over the capture. Only the mean is kept, so the spread of sizes is not in a corpus.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct RequestSizes {
    pub sum: f64,
    pub count: f64,
}

/// The size every request of a corpus was cut to, and where the size came from.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct ItemsPerRequest {
    pub size: u64,
    pub source: SizeSource,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SizeSource {
    /// `capture --items-per-request`.
    Flag,
    /// The capture's mean events per request, rounded.
    Mean,
}

/// How tap sampled the source, which bounds what a capture can contain.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct TapSettings {
    pub duration_secs: u64,
    pub interval_ms: u64,
    /// Events kept per interval.
    pub limit: u64,
}

impl TapSettings {
    pub fn intervals(&self) -> u64 {
        (self.duration_secs * 1000).div_ceil(self.interval_ms.max(1))
    }

    /// The most events a capture with these settings can hold.
    pub fn max_events(&self) -> u64 {
        self.intervals() * self.limit
    }
}

pub struct Corpus {
    pub header: CorpusHeader,
    /// Rebuilt requests, in replay order.
    pub requests: Vec<Vec<Item>>,
}

#[derive(Deserialize)]
struct RequestLine {
    items: Vec<Item>,
}

/// Borrowing form of [`RequestLine`], so writing does not clone the items.
#[derive(Serialize)]
struct RequestLineRef<'a> {
    items: &'a [Item],
}

/// Parses the header line, refusing any other format before the rest of the header is parsed.
fn parse_header(line: &str, path: &Path) -> Result<CorpusHeader, BoxError> {
    let value: serde_json::Value = serde_json::from_str(line)
        .map_err(|e| err(format!("{}: not a corpus header: {e}", path.display())))?;
    let format = value
        .get("format")
        .and_then(|f| f.as_str())
        .unwrap_or_default();
    if format != CORPUS_FORMAT {
        return Err(err(format!(
            "{}: unsupported corpus format `{format}` (expected `{CORPUS_FORMAT}`; re-run `capture`)",
            path.display(),
        )));
    }
    serde_json::from_value(value)
        .map_err(|e| err(format!("{}: not a corpus header: {e}", path.display())))
}

impl Corpus {
    pub fn load(path: &Path) -> Result<Self, BoxError> {
        let file = io_at(File::open(path), "opening corpus", path)?;
        Self::read(BufReader::new(file), path)
    }

    /// Parses a corpus read from `reader`, which holds the file at `path` (named in errors).
    pub fn read(reader: impl BufRead, path: &Path) -> Result<Self, BoxError> {
        let mut lines = reader.lines().enumerate();
        let header = match lines.next() {
            Some((_, line)) => parse_header(&io_at(line, "reading corpus", path)?, path)?,
            None => return Err(err(format!("{} is empty", path.display()))),
        };
        let codec = codec_named(&header.source_type)
            .map_err(|e| err(format!("{}: {e}", path.display())))?;
        let mut requests = Vec::new();
        for (index, line) in lines {
            let line = io_at(line, "reading corpus", path)?;
            if line.trim().is_empty() {
                continue;
            }
            let at = || format!("{}:{}", path.display(), index + 1);
            let request: RequestLine =
                serde_json::from_str(&line).map_err(|e| err(format!("{}: {e}", at())))?;
            for item in &request.items {
                codec
                    .validate(item)
                    .map_err(|e| err(format!("{}: {e}", at())))?;
            }
            requests.push(request.items);
        }
        if requests.iter().all(Vec::is_empty) {
            return Err(err(format!("{}: corpus has no items", path.display())));
        }
        Ok(Self { header, requests })
    }

    /// Creates the file, never replacing one: a capture takes minutes, and a clash must not lose it.
    pub fn write(&self, path: &Path) -> Result<(), BoxError> {
        let file = io_at(File::create_new(path), "creating corpus", path)?;
        let mut out = BufWriter::new(file);
        serde_json::to_writer(&mut out, &self.header)?;
        out.write_all(b"\n")?;
        for items in &self.requests {
            serde_json::to_writer(&mut out, &RequestLineRef { items })?;
            out.write_all(b"\n")?;
        }
        out.flush()?;
        Ok(())
    }

    pub fn items(&self) -> usize {
        self.requests.iter().map(Vec::len).sum()
    }
}

#[cfg(test)]
#[allow(unused_imports, dead_code)] // Bench builds compile this module but never run it.
mod tests {
    use super::*;

    fn header(source_type: &str) -> CorpusHeader {
        CorpusHeader {
            format: CORPUS_FORMAT.into(),
            source_type: source_type.into(),
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

    /// A prom_remote_write item as tap would decode it; `value` is tap's JSON number text.
    fn item(name: &str, value: &str) -> Item {
        let line = format!(
            r#"{{"name":"{name}","tags":{{"k":"v"}},"kind":"absolute","gauge":{{"value":{value}}}}}"#
        );
        codec_named("prom_remote_write")
            .unwrap()
            .decode_tap(&line)
            .unwrap()
    }

    pub fn corpus(requests: Vec<Vec<(&str, &str)>>) -> Corpus {
        Corpus {
            header: header("prom_remote_write"),
            requests: requests
                .into_iter()
                .map(|r| r.into_iter().map(|(name, v)| item(name, v)).collect())
                .collect(),
        }
    }

    #[test]
    fn tap_settings_bound_the_capture() {
        let tap = TapSettings {
            duration_secs: 120,
            interval_ms: 100,
            limit: 10_000,
        };
        assert_eq!(tap.intervals(), 1200);
        assert_eq!(tap.max_events(), 12_000_000);
    }

    #[test]
    fn corpus_round_trips_items() {
        let c = corpus(vec![vec![("a", "1.5"), ("b", "null")], vec![("c", "2")]]);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.ndjson");
        c.write(&path).unwrap();
        let back = Corpus::load(&path).unwrap();
        assert_eq!(back.requests.len(), 2);
        assert_eq!(back.requests[0][0], c.requests[0][0]);
        assert_eq!(back.requests[0][1]["value"], "NaN");
        assert_eq!(back.requests[1][0]["labels"], serde_json::json!({"k": "v"}));
        assert_eq!(back.items(), 3);
    }

    #[test]
    fn header_records_the_mean_and_the_size_used() {
        let mut c = corpus(vec![vec![("a", "1")]]);
        c.header.request_sizes = RequestSizes {
            sum: 320.0,
            count: 9.0,
        };
        c.header.items_per_request = Some(ItemsPerRequest {
            size: 36,
            source: SizeSource::Mean,
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.ndjson");
        c.write(&path).unwrap();
        let back = Corpus::load(&path).unwrap();
        assert_eq!(back.header.request_sizes.sum, 320.0);
        assert_eq!(back.header.request_sizes.count, 9.0);
        let used = back.header.items_per_request.unwrap();
        assert_eq!((used.size, used.source), (36, SizeSource::Mean));
        let header: serde_json::Value = serde_json::from_str(
            std::fs::read_to_string(&path)
                .unwrap()
                .lines()
                .next()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            header["request_sizes"],
            serde_json::json!({"sum": 320.0, "count": 9.0})
        );
        assert_eq!(
            header["items_per_request"],
            serde_json::json!({"size": 36, "source": "mean"})
        );
    }

    #[test]
    fn corpora_with_bucketed_request_sizes_still_load() {
        // A header as corpus/3 wrote it before the sizes were cut to the mean: the buckets are
        // ignored and the header has no items_per_request.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("old.ndjson");
        std::fs::write(
            &path,
            "{\"format\":\"databricks-bench-corpus/3\",\"source_type\":\"prom_remote_write\",\
             \"origin\":\"o\",\"captured_at_ms\":0,\"tap\":{\"duration_secs\":1,\
             \"interval_ms\":100,\"limit\":10},\"events\":1,\"seed\":0,\"window_ms\":5,\
             \"received_events\":1.0,\"coverage\":1.0,\"items_per_sec\":2.0,\
             \"request_sizes\":{\"buckets\":[{\"le\":10.0,\"count\":1.0},{\"le\":null,\"count\":1.0}],\
             \"sum\":7.0,\"count\":1.0}}\n\
             {\"items\":[{\"name\":\"a\",\"labels\":{},\"value\":\"1\"}]}\n",
        )
        .unwrap();
        let back = Corpus::load(&path).unwrap();
        assert_eq!(
            (
                back.header.request_sizes.sum,
                back.header.request_sizes.count
            ),
            (7.0, 1.0)
        );
        assert!(back.header.items_per_request.is_none());
        assert_eq!(back.items(), 1);
    }

    #[test]
    fn values_round_trip_bit_for_bit() {
        // serde_json's default number parser is off by one ulp for some decimals such as this
        // one. Items hold values as strings, so the corpus carries the exact value.
        let value: f64 = 2.2800000000000003e-07;
        let c = corpus(vec![vec![("a", "2.2800000000000003e-07")]]);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.ndjson");
        c.write(&path).unwrap();
        let back = Corpus::load(&path).unwrap();
        let read: f64 = back.requests[0][0]["value"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(read.to_bits(), value.to_bits());
    }

    #[test]
    fn corpus_is_never_overwritten() {
        let c = corpus(vec![vec![("a", "1")]]);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.ndjson");
        c.write(&path).unwrap();
        assert!(c.write(&path).is_err());
    }

    #[test]
    fn items_the_codec_rejects_are_refused_at_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.ndjson");
        let mut c = corpus(vec![vec![("a", "1")]]);
        c.requests[0][0] = serde_json::json!({"name": 3});
        c.write(&path).unwrap();
        let error = Corpus::load(&path).err().unwrap().to_string();
        assert!(error.contains(":2:"), "{error}");
        assert!(error.contains("prom_remote_write sample"), "{error}");
    }

    #[test]
    fn older_corpus_formats_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("old.ndjson");
        std::fs::write(
            &path,
            "{\"format\":\"databricks-bench-corpus/2\",\"source_type\":\"prom_remote_write\"}\n",
        )
        .unwrap();
        let error = Corpus::load(&path).err().unwrap().to_string();
        assert!(error.contains("re-run `capture`"), "{error}");
        assert!(error.contains("databricks-bench-corpus/2"), "{error}");
    }

    #[test]
    fn unknown_source_types_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.ndjson");
        let mut c = corpus(vec![vec![("a", "1")]]);
        c.header.source_type = "gcs_logs".into();
        c.write(&path).unwrap();
        let error = Corpus::load(&path).err().unwrap().to_string();
        assert!(error.contains("unknown source type `gcs_logs`"), "{error}");
    }
}
