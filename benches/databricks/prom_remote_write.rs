//! The `prom_remote_write` codec: Prometheus remote write, as `prometheus_remote_write` receives it.
//!
//! An item is one sample: its name, labels and value. Values are stored as JSON strings holding
//! Rust's shortest round-trip decimal, because serde_json's number parser is not correctly rounded
//! and would change some values by one ulp. Non-finite values are kept as `NaN`, `inf` and `-inf`:
//! staleness markers are common in production, and the pipeline must drop them.

use std::collections::BTreeMap;

use bytes::Bytes;
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use vector_lib::prometheus::parser::{METRIC_NAME_LABEL, proto};

use super::source_codec::{Encoded, Item, Outcome, SourceCodec};

pub struct PromRemoteWrite;

/// One sample, the item's JSON form.
#[derive(Serialize, Deserialize)]
struct Sample {
    name: String,
    /// Sorted by key; excludes `__name__`.
    labels: BTreeMap<String, String>,
    #[serde(with = "exact_f64")]
    value: f64,
}

/// Writes an f64 as its shortest round-trip decimal and reads it back with std's parser, which is
/// correctly rounded.
mod exact_f64 {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &f64, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<f64, D::Error> {
        let text = String::deserialize(d)?;
        text.parse()
            .map_err(|_| serde::de::Error::custom(format!("not a number: {text}")))
    }
}

/// Tap's JSON for one metric, as `vector tap` prints it. The gauge value is kept as its raw text,
/// so it can be parsed exactly.
#[derive(Deserialize)]
struct TapMetric {
    name: Option<String>,
    /// Absent or null when the metric has no tags; the source only sends string tags.
    tags: Option<BTreeMap<String, String>>,
    gauge: Option<TapGauge>,
}

#[derive(Deserialize)]
struct TapGauge {
    value: Box<RawValue>,
}

impl SourceCodec for PromRemoteWrite {
    fn name(&self) -> &'static str {
        "prom_remote_write"
    }

    fn decode_tap(&self, line: &str) -> Result<Item, &'static str> {
        let metric: TapMetric = serde_json::from_str(line).map_err(|e| {
            if e.is_syntax() || e.is_eof() {
                "not_json"
            } else {
                "not_a_metric"
            }
        })?;
        let name = metric.name.ok_or("not_a_metric")?;
        // With group_metrics: false the source only emits gauges; anything else means it groups.
        let gauge = metric.gauge.ok_or("not_a_gauge")?;
        let value = match gauge.value.get().trim() {
            // serde_json prints non-finite floats as null.
            "null" => f64::NAN,
            text => text.parse::<f64>().map_err(|_| "bad_value")?,
        };
        let labels = metric.tags.unwrap_or_default();
        Ok(serde_json::to_value(Sample {
            name,
            labels,
            value,
        })
        .expect("a sample serializes"))
    }

    fn validate(&self, item: &Item) -> Result<(), String> {
        serde_json::from_value::<Sample>(item.clone())
            .map(|_| ())
            .map_err(|e| format!("not a prom_remote_write sample: {e}"))
    }

    fn encode(&self, items: &[Item], sent_ms: i64) -> Encoded {
        let timeseries = items
            .iter()
            .map(|item| {
                // Corpus items were validated when the corpus was loaded.
                let sample: Sample = serde_json::from_value(item.clone())
                    .expect("items are validated when the corpus loads");
                let mut labels = Vec::with_capacity(sample.labels.len() + 1);
                labels.push(proto::Label {
                    name: METRIC_NAME_LABEL.into(),
                    value: sample.name,
                });
                labels.extend(
                    sample
                        .labels
                        .into_iter()
                        .map(|(name, value)| proto::Label { name, value }),
                );
                // Sorted by name, as Prometheus writes them, so `__name__` is not always first.
                labels.sort_by(|a, b| a.name.cmp(&b.name));
                proto::TimeSeries {
                    labels,
                    samples: vec![proto::Sample {
                        value: sample.value,
                        timestamp: sent_ms,
                    }],
                }
            })
            .collect();
        let request = proto::WriteRequest {
            timeseries,
            ..Default::default()
        };
        let body = Bytes::from(
            snap::raw::Encoder::new()
                .compress_vec(&request.encode_to_vec())
                .expect("snappy encode"),
        );
        Encoded {
            body,
            headers: vec![
                ("content-encoding", "snappy"),
                ("content-type", "application/x-protobuf"),
                ("x-prometheus-remote-write-version", "0.1.0"),
            ],
        }
    }

    fn classify(&self, status: u16) -> Outcome {
        match status {
            200..=299 => Outcome::Accepted,
            // 0 is a connection-level failure. 429 is rate limiting, and 5xx a server error.
            0 | 429 | 500..=599 => Outcome::Retryable,
            _ => Outcome::Rejected,
        }
    }
}

#[cfg(test)]
#[allow(unused_imports, dead_code)] // Bench builds compile this module but never run it.
mod tests {
    use serde_json::Value;

    use super::super::source_codec::codec_named;
    use super::*;

    fn codec() -> &'static dyn SourceCodec {
        codec_named("prom_remote_write").unwrap()
    }

    fn decode(line: &str) -> Result<Value, &'static str> {
        PromRemoteWrite.decode_tap(line)
    }

    #[test]
    fn decodes_tap_gauges() {
        let bare = decode(
            r#"{"name":"up","tags":{"job":"b","instance":"a"},"timestamp":"2026-11-10T12:00:00.123Z","kind":"absolute","gauge":{"value":1.0}}"#,
        )
        .unwrap();
        assert_eq!(
            bare,
            serde_json::json!({"name": "up", "labels": {"instance": "a", "job": "b"}, "value": "1"})
        );
        // Non-finite values print as null and must be kept.
        let stale = decode(
            r#"{"name":"stale","tags":{},"timestamp":"2026-11-10T12:00:00.123Z","kind":"absolute","gauge":{"value":null}}"#,
        )
        .unwrap();
        assert_eq!(stale["value"], "NaN");
        let no_tags = decode(
            r#"{"name":"untagged","timestamp":"2026-11-10T12:00:01Z","kind":"absolute","gauge":{"value":5}}"#,
        )
        .unwrap();
        assert_eq!(no_tags["value"], "5");
        assert_eq!(no_tags["labels"], serde_json::json!({}));
        let null_tags =
            decode(r#"{"name":"nulltags","tags":null,"kind":"absolute","gauge":{"value":1}}"#)
                .unwrap();
        assert_eq!(null_tags["labels"], serde_json::json!({}));
    }

    #[test]
    fn rejects_events_the_flat_source_never_emits() {
        // A counter or histogram means the source groups metrics (group_metrics: true).
        let counter = r#"{"name":"c_total","timestamp":"2026-11-10T12:00:01Z","kind":"absolute","counter":{"value":5}}"#;
        assert_eq!(decode(counter).unwrap_err(), "not_a_gauge");
        let distribution = r#"{"name":"h","timestamp":"2026-11-10T12:00:00Z","kind":"absolute","distribution":{}}"#;
        assert_eq!(decode(distribution).unwrap_err(), "not_a_gauge");
        assert_eq!(
            decode(r#"{"message":"a log event"}"#).unwrap_err(),
            "not_a_metric"
        );
        assert_eq!(decode("not json").unwrap_err(), "not_json");
        assert_eq!(decode(r#"{"name":"cut"#).unwrap_err(), "not_json");
        // A wrapper is not a bare metric: tap never prints one without --meta.
        let wrapped = r#"{"metric":{"name":"w","tags":{},"kind":"absolute","gauge":{"value":1}}}"#;
        assert_eq!(decode(wrapped).unwrap_err(), "not_a_metric");
    }

    #[test]
    fn decimal_values_round_trip_bit_for_bit() {
        // serde_json's default number parser is off by one ulp for this decimal. The codec reads
        // the gauge's text with std's parser, so the item holds the exact value.
        let line =
            r#"{"name":"a","tags":{},"kind":"absolute","gauge":{"value":2.2800000000000003e-07}}"#;
        let item = decode(line).unwrap();
        let value: f64 = item["value"].as_str().unwrap().parse().unwrap();
        assert_eq!(value.to_bits(), 2.2800000000000003e-07f64.to_bits());
        // Shortest round-trip text, so the value survives a write and read of the corpus.
        assert_eq!(
            value.to_string().parse::<f64>().unwrap().to_bits(),
            value.to_bits()
        );
        assert!(PromRemoteWrite.validate(&item).is_ok());
        assert!(
            PromRemoteWrite
                .validate(&serde_json::json!({"name": 1}))
                .is_err()
        );
    }

    #[test]
    fn requests_restamp_every_sample_and_keep_nan() {
        let up = serde_json::json!({"name": "up", "labels": {"job": "x"}, "value": "1"});
        let stale = serde_json::json!({"name": "stale", "labels": {}, "value": "NaN"});
        let encoded = codec().encode(&[up, stale], 1_794_000_000_123);
        assert!(encoded.headers.contains(&("content-encoding", "snappy")));

        let raw = snap::raw::Decoder::new()
            .decompress_vec(&encoded.body)
            .unwrap();
        let request = proto::WriteRequest::decode(raw.as_slice()).unwrap();
        assert_eq!(request.timeseries.len(), 2, "one series per sample");
        let first = &request.timeseries[0];
        let labels: Vec<&str> = first.labels.iter().map(|l| l.name.as_str()).collect();
        assert_eq!(labels, ["__name__", "job"]);
        assert_eq!(first.samples[0].value, 1.0);
        assert_eq!(first.samples[0].timestamp, 1_794_000_000_123);
        let second = &request.timeseries[1];
        assert!(second.samples[0].value.is_nan());
        assert_eq!(second.samples[0].timestamp, 1_794_000_000_123);
    }

    #[test]
    fn statuses_are_classified_for_retries() {
        for status in [0, 429, 500, 503] {
            assert_eq!(
                PromRemoteWrite.classify(status),
                Outcome::Retryable,
                "{status}"
            );
        }
        for status in [200, 204] {
            assert_eq!(
                PromRemoteWrite.classify(status),
                Outcome::Accepted,
                "{status}"
            );
        }
        for status in [400, 401, 404] {
            assert_eq!(
                PromRemoteWrite.classify(status),
                Outcome::Rejected,
                "{status}"
            );
        }
    }

    #[test]
    fn codecs_are_found_by_name() {
        assert_eq!(codec().name(), "prom_remote_write");
        let error = codec_named("gcs_logs").err().unwrap().to_string();
        assert!(error.contains("prom_remote_write"), "{error}");
    }
}
