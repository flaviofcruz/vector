//! Per-row freshness of acked batches: how old each row was when Zerobus acked its batch.
//!
//! Times come from the Arrow batch, not the Vector event, because `wire_to_arrow` tables
//! carry only `.message` bytes on the event. Metrics are best effort: a batch without the
//! expected columns records nothing, and nothing here can fail or slow ingest.

use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{Array, Int64Array, StructArray};
use arrow::record_batch::RecordBatch;
use metrics::{counter, histogram};
use vector_lib::lookup::metadata_path;

use crate::{event::Event, internal_events::ZerobusAckFreshness};

/// LP freshness SLA. The alert divides `zerobus_ack_over_sla_total` by the born_to_saved `_count`.
const FRESHNESS_SLA_MS: i64 = 30 * 60 * 1000;

/// Born-at and LP ingest timestamps (epoch ms), read once per batch.
///
/// LP ingest is stamped by LP's splice transform (`now()`), so its lag includes the sink
/// buffer wait, batching, encode, and Zerobus.
///
/// Each is `None` when the table lacks the column or it is not BIGINT. Holding the
/// `Int64Array` clones only bumps buffer refcounts; no column data is copied.
pub(super) struct AckTimes {
    born: Option<Int64Array>,
    lp_ingest: Option<Int64Array>,
}

/// Counts for one batch. Summed per batch so the counters are bumped once.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct FreshnessCounts {
    pub negative_born_to_saved: u64,
    pub negative_lp_ingest_to_saved: u64,
    pub missing_born_at: u64,
    /// Born-to-saved rows over `FRESHNESS_SLA_MS`. Only rows that also reach the histogram.
    pub over_sla_born_to_saved: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Series {
    BornToSaved,
    LpIngestToSaved,
}

impl AckTimes {
    pub(super) fn from_batch(batch: &RecordBatch) -> Self {
        Self {
            born: int64_at(
                batch,
                ["_log_metadata", "service_metadata", "log_timestamp_ms"],
            ),
            lp_ingest: int64_at(batch, ["_log_metadata", "vector_metadata", "timestamp_ms"]),
        }
    }
    /// Records the lags of an acked batch. Call only after Zerobus acks the batch.
    pub(super) fn record_acked(&self, table: &str) {
        if self.born.is_none() && self.lp_ingest.is_none() {
            return;
        }
        // Handles are resolved once per batch; the per-row loop only calls `record`.
        let born_seconds =
            histogram!("zerobus_ack_born_to_saved_seconds", "table" => table.to_owned());
        let lp_seconds =
            histogram!("zerobus_ack_lp_ingest_to_saved_seconds", "table" => table.to_owned());
        let counts = walk(
            self.born.as_ref(),
            self.lp_ingest.as_ref(),
            now_epoch_ms(),
            |series, seconds| match series {
                Series::BornToSaved => born_seconds.record(seconds),
                Series::LpIngestToSaved => lp_seconds.record(seconds),
            },
        );
        emit!(ZerobusAckFreshness {
            table: table.to_owned(),
            negative_born_to_saved: counts.negative_born_to_saved,
            negative_lp_ingest_to_saved: counts.negative_lp_ingest_to_saved,
            missing_born_at: counts.missing_born_at,
            over_sla_born_to_saved: counts.over_sla_born_to_saved,
        });
    }
}

/// Object-upload stamps (epoch ms) for the events of one batch.
///
/// The queue sources already know when the blob was written — S3 `last_modified`, Azure
/// blob `Last-Modified`, GCS object `Last-Modified` response header — and stamp it as
/// `%databricks.object_uploaded_ms` event metadata, so the freshness clock starts at the
/// object upload rather than at the LP splice. Unlike `born`/`lp_ingest` this needs no Arrow
/// column, so it works for wire_to_arrow tables without a table schema change. Events without
/// the stamp are skipped; a batch where no event has it records nothing.
///
/// The vector is aligned with the request's events (one slot each, `None` when absent), so
/// `keep_rows` can drop the slots of rows that `wire_to_arrow` dropped during encoding and
/// that therefore never reach a Zerobus ack.
#[derive(Clone, Debug, Default)]
pub(super) struct ObjectUploadTimes(Vec<Option<i64>>);

impl ObjectUploadTimes {
    pub(super) fn from_events(events: &[Event]) -> Self {
        Self(
            events
                .iter()
                .map(|event| {
                    event
                        .maybe_as_log()
                        .and_then(|log| log.get(metadata_path!("databricks", "object_uploaded_ms")))
                        .and_then(|value| value.as_integer())
                })
                .collect(),
        )
    }

    /// Keeps only the slots of `kept` row indices (from the encoder, in event order), so
    /// rows dropped by `wire_to_arrow` never enter the histogram.
    pub(super) fn keep_rows(&mut self, kept: &[u32]) {
        self.0 = kept.iter().map(|&row| self.0[row as usize]).collect();
    }

    /// Records the upload-to-ack lags of an acked batch. Call only after Zerobus acks.
    pub(super) fn record_acked(&self, table: &str) {
        if self.0.iter().all(Option::is_none) {
            return;
        }
        // Handle resolved once per batch; the loop only calls `record`.
        let seconds =
            histogram!("zerobus_ack_object_uploaded_to_saved_seconds", "table" => table.to_owned());
        let now_ms = now_epoch_ms();
        let mut negative = 0;
        for stamp in self.0.iter().flatten() {
            let lag = now_ms.saturating_sub(*stamp);
            if lag < 0 {
                negative += 1;
            } else {
                seconds.record(lag as f64 / 1000.0);
            }
        }
        if negative > 0 {
            counter!(
                "zerobus_ack_negative_latency_total",
                "table" => table.to_owned(),
                "series" => "object_uploaded_to_saved",
            )
            .increment(negative);
        }
    }
}

/// Walks each row once. `observe` gets every non-negative lag, in seconds.
///
/// A born-at that is null, 0, or equal to LP ingest means the producer had no log time and
/// LJP stamped LP's "now" (or left 0). Those rows are counted as missing and kept out of the
/// born-to-saved series; their LP-ingest lag is still real and recorded.
pub(super) fn walk(
    born: Option<&Int64Array>,
    lp_ingest: Option<&Int64Array>,
    now_ms: i64,
    mut observe: impl FnMut(Series, f64),
) -> FreshnessCounts {
    let mut counts = FreshnessCounts::default();
    let rows = born.or(lp_ingest).map_or(0, |a| a.len());
    for row in 0..rows {
        let ingest = lp_ingest.and_then(|a| present(a, row));
        if let Some(born) = born {
            match present(born, row) {
                Some(born_ms) if Some(born_ms) != ingest => sample(
                    Series::BornToSaved,
                    now_ms.saturating_sub(born_ms),
                    &mut counts,
                    &mut observe,
                ),
                _ => counts.missing_born_at += 1,
            }
        }
        if let Some(ingest_ms) = ingest {
            sample(
                Series::LpIngestToSaved,
                now_ms.saturating_sub(ingest_ms),
                &mut counts,
                &mut observe,
            );
        }
    }
    counts
}

fn sample(
    series: Series,
    lag_ms: i64,
    counts: &mut FreshnessCounts,
    observe: &mut impl FnMut(Series, f64),
) {
    if lag_ms < 0 {
        match series {
            Series::BornToSaved => counts.negative_born_to_saved += 1,
            Series::LpIngestToSaved => counts.negative_lp_ingest_to_saved += 1,
        }
    } else {
        if series == Series::BornToSaved && lag_ms > FRESHNESS_SLA_MS {
            counts.over_sla_born_to_saved += 1;
        }
        observe(series, lag_ms as f64 / 1000.0);
    }
}

/// The value at `row`, or `None` for null or 0. LJP's VRL path writes 0 for "no log time".
fn present(column: &Int64Array, row: usize) -> Option<i64> {
    if column.is_null(row) {
        return None;
    }
    let value = column.value(row);
    (value != 0).then_some(value)
}

fn int64_at(batch: &RecordBatch, path: [&str; 3]) -> Option<Int64Array> {
    let [top, middle, leaf] = path;
    let top = batch
        .column_by_name(top)?
        .as_any()
        .downcast_ref::<StructArray>()?;
    let middle = top
        .column_by_name(middle)?
        .as_any()
        .downcast_ref::<StructArray>()?;
    middle
        .column_by_name(leaf)?
        .as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
}

fn now_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok())
        .unwrap_or(0)
}

#[cfg(test)]
pub(super) mod test_support {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Int64Array, StructArray};
    use arrow::datatypes::{DataType, Field};
    use arrow::record_batch::RecordBatch;

    /// A batch shaped like an LP table: `_log_metadata` with the two freshness leaves.
    pub(in crate::sinks::databricks_zerobus) fn batch_with_times(
        born: Vec<Option<i64>>,
        ingest: Vec<Option<i64>>,
    ) -> RecordBatch {
        let service_metadata = StructArray::from(vec![(
            Arc::new(Field::new("log_timestamp_ms", DataType::Int64, true)),
            Arc::new(Int64Array::from(born)) as ArrayRef,
        )]);
        let vector_metadata = StructArray::from(vec![(
            Arc::new(Field::new("timestamp_ms", DataType::Int64, true)),
            Arc::new(Int64Array::from(ingest)) as ArrayRef,
        )]);
        let log_metadata = StructArray::from(vec![
            (
                Arc::new(Field::new(
                    "service_metadata",
                    service_metadata.data_type().clone(),
                    true,
                )),
                Arc::new(service_metadata) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "vector_metadata",
                    vector_metadata.data_type().clone(),
                    true,
                )),
                Arc::new(vector_metadata) as ArrayRef,
            ),
        ]);
        RecordBatch::try_from_iter(vec![("_log_metadata", Arc::new(log_metadata) as ArrayRef)])
            .expect("test batch columns have equal lengths")
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::batch_with_times;
    use super::*;

    const NOW: i64 = 1_000_000;

    fn run(
        born: Vec<Option<i64>>,
        ingest: Vec<Option<i64>>,
    ) -> (FreshnessCounts, Vec<(Series, f64)>) {
        let batch = batch_with_times(born, ingest);
        let times = AckTimes::from_batch(&batch);
        let mut seen = Vec::new();
        let counts = walk(
            times.born.as_ref(),
            times.lp_ingest.as_ref(),
            NOW,
            |s, v| seen.push((s, v)),
        );
        (counts, seen)
    }

    #[test]
    fn normal_rows_record_both_lags_in_seconds() {
        let (counts, seen) = run(vec![Some(NOW - 2_000)], vec![Some(NOW - 500)]);
        assert_eq!(counts, FreshnessCounts::default());
        assert_eq!(
            seen,
            vec![(Series::BornToSaved, 2.0), (Series::LpIngestToSaved, 0.5)]
        );
    }

    #[test]
    fn negative_lag_is_counted_and_not_observed() {
        let (counts, seen) = run(vec![Some(NOW + 1_000)], vec![Some(NOW + 1_000)]);
        assert_eq!(counts.negative_born_to_saved, 0);
        assert_eq!(counts.missing_born_at, 1);
        assert_eq!(counts.negative_lp_ingest_to_saved, 1);
        assert!(seen.is_empty());

        let (counts, seen) = run(vec![Some(NOW + 1_000)], vec![None]);
        assert_eq!(counts.negative_born_to_saved, 1);
        assert_eq!(counts.missing_born_at, 0);
        assert!(seen.is_empty());
    }

    #[test]
    fn missing_born_at_is_null_zero_or_equal_to_ingest() {
        let (counts, seen) = run(
            vec![None, Some(0), Some(NOW - 300)],
            vec![Some(NOW - 100), Some(NOW - 100), Some(NOW - 300)],
        );
        assert_eq!(counts.missing_born_at, 3);
        assert_eq!(counts.negative_born_to_saved, 0);
        // Only the ingest lags are real; the born series gets nothing.
        assert_eq!(
            seen,
            vec![
                (Series::LpIngestToSaved, 0.1),
                (Series::LpIngestToSaved, 0.1),
                (Series::LpIngestToSaved, 0.3),
            ]
        );
    }

    #[test]
    fn null_or_zero_ingest_skips_only_the_ingest_lag() {
        let (counts, seen) = run(
            vec![Some(NOW - 2_000), Some(NOW - 2_000)],
            vec![None, Some(0)],
        );
        assert_eq!(counts, FreshnessCounts::default());
        assert_eq!(
            seen,
            vec![(Series::BornToSaved, 2.0), (Series::BornToSaved, 2.0)]
        );
    }

    #[test]
    fn batch_without_columns_or_with_other_shape_records_nothing() {
        use arrow::array::{ArrayRef, Int64Array};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let plain = RecordBatch::try_from_iter(vec![(
            "_log_metadata",
            Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef,
        )])
        .unwrap();
        let times = AckTimes::from_batch(&plain);
        assert!(times.born.is_none());
        assert!(times.lp_ingest.is_none());

        let mut seen = Vec::new();
        let counts = walk(None, None, NOW, |s, v| seen.push((s, v)));
        assert_eq!(counts, FreshnessCounts::default());
        assert!(seen.is_empty());
    }

    #[test]
    fn over_sla_counts_born_rows_strictly_above_threshold() {
        // `run` uses NOW, which is smaller than the SLA, so walk directly with a later clock.
        const LATER: i64 = 10_000_000;
        let batch = batch_with_times(
            vec![
                Some(LATER - FRESHNESS_SLA_MS),
                Some(LATER - FRESHNESS_SLA_MS - 1),
                Some(LATER + 1_000),
                None,
            ],
            vec![
                Some(LATER - 100),
                Some(LATER - 100),
                None,
                Some(LATER - 100),
            ],
        );
        let times = AckTimes::from_batch(&batch);
        let mut born = Vec::new();
        let counts = walk(
            times.born.as_ref(),
            times.lp_ingest.as_ref(),
            LATER,
            |s, v| {
                if s == Series::BornToSaved {
                    born.push(v);
                }
            },
        );
        // Exactly at the SLA is not over; one ms past is. The negative row and the
        // missing-born row never reach the born histogram, so they are not counted here.
        assert_eq!(counts.over_sla_born_to_saved, 1);
        assert_eq!(counts.negative_born_to_saved, 1);
        assert_eq!(counts.missing_born_at, 1);
        assert_eq!(born, vec![1800.0, 1800.001]);
    }

    #[test]
    fn columns_share_the_batch_buffers_instead_of_copying() {
        let batch = batch_with_times(vec![Some(1), Some(2)], vec![Some(3), None]);
        let times = AckTimes::from_batch(&batch);
        let top = batch
            .column_by_name("_log_metadata")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let middle = top
            .column_by_name("service_metadata")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let source = middle
            .column_by_name("log_timestamp_ms")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let born = times.born.as_ref().unwrap();
        assert_eq!(born.values().as_ptr(), source.values().as_ptr());
    }
}
