//! Zerobus sink output: the Arrow Flight frames that `fake-zerobus` records, decoded to JSON rows.
//!
//! Each `.flight` file is one connection's frames in order: a schema frame, then record batches.
//! Batches are rendered with arrow's JSON writer, so every column is present in every row.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use arrow::array::make_array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_flight::FlightData;
use arrow_flight::utils::flight_data_to_arrow_batch;
use bytes::Bytes;

use super::err;
use super::fake_zerobus::{self, FakeZerobusArgs};
use super::fakes::{EPHEMERAL_LOOPBACK, RunningFake};
use super::sink::{FakeSetup, ReadBack, SinkKind, destination_of, emit, files_under};
use super::tls::TlsFiles;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Environment variables that point a candidate's `databricks_zerobus` sink at the fakes.
const INGEST_ENDPOINT_VAR: &str = "DATABRICKS_BENCH_ZEROBUS_INGEST_ENDPOINT";
const UC_ENDPOINT_VAR: &str = "DATABRICKS_BENCH_ZEROBUS_UC_ENDPOINT";
const TABLE_VAR: &str = "DATABRICKS_BENCH_ZEROBUS_TABLE";
/// The table every candidate writes to. Each candidate has its own fake, so the names need not
/// differ, and a common one makes the table part of a row's destination comparable: a candidate
/// that writes another table then differs.
const TABLE: &str = "main.databricks_bench.output";

pub struct ZerobusFlight;

impl SinkKind for ZerobusFlight {
    fn name(&self) -> &'static str {
        "zerobus"
    }

    fn needs_schema(&self) -> bool {
        true
    }

    fn start_fake(&self, setup: &FakeSetup) -> Result<RunningFake, BoxError> {
        fake_zerobus::start(FakeZerobusArgs {
            out: setup.out.clone(),
            tls_dir: setup.tls_dir.clone(),
            listen: EPHEMERAL_LOOPBACK,
            discard: false,
            ack_delay_ms: 0,
        })
    }

    fn candidate_env(
        &self,
        setup: &FakeSetup,
        fake: SocketAddr,
    ) -> Result<Vec<(&'static str, String)>, BoxError> {
        let uc = setup
            .schema_endpoint
            .ok_or_else(|| err("zerobus needs fake-uc, but check did not start it"))?;
        let ca = TlsFiles::in_dir(&setup.tls_dir).ca_cert;
        Ok(vec![
            (INGEST_ENDPOINT_VAR, format!("https://{fake}")),
            (UC_ENDPOINT_VAR, format!("http://{uc}")),
            (TABLE_VAR, TABLE.to_owned()),
            // The candidate trusts the fake's throwaway CA, and keeps its TLS path otherwise.
            ("SSL_CERT_FILE", ca.display().to_string()),
        ])
    }

    fn read_back(
        &self,
        out: &Path,
        row: &mut dyn FnMut(&str, &[u8]),
    ) -> Result<ReadBack, BoxError> {
        let mut found = ReadBack::default();
        for relative in files_under(out)? {
            if relative.extension().is_none_or(|e| e != "flight") {
                continue;
            }
            let destination = destination_of(&relative);
            found.files += 1;
            found.damaged +=
                decode_capture(&out.join(&relative), &mut |line| row(&destination, line))?;
        }
        Ok(found)
    }
}

/// Calls `row` with each rendered row of one `.flight` file, and returns its damaged records.
fn decode_capture(file: &Path, row: &mut dyn FnMut(&[u8])) -> Result<u64, BoxError> {
    let raw = super::io_at(std::fs::read(file), "reading", file)?;
    let (frames, truncated) = read_frames(&raw);
    let mut damaged = 0;
    if truncated {
        eprintln!(
            "verify: {}: cut off mid-frame; keeping its {} complete frames",
            file.display(),
            frames.len()
        );
        damaged += 1;
    }
    let Some(((header, body), batches)) = frames.split_first() else {
        return Ok(damaged);
    };
    let schema = Schema::try_from(&flight_data(header, body)).map_err(|e| {
        err(format!(
            "{}: first frame is not a schema: {e}",
            file.display()
        ))
    })?;
    let schema = Arc::new(schema);
    for (seq, (header, body)) in batches.iter().enumerate() {
        let data = flight_data(header, body);
        let batch = match flight_data_to_arrow_batch(&data, Arc::clone(&schema), &HashMap::new()) {
            Ok(batch) => batch,
            Err(e) => {
                eprintln!("verify: {}#{seq}: {e}", file.display());
                damaged += 1;
                continue;
            }
        };
        let json = batch_to_json(&retag_timezone(batch, "UTC", "+00:00")?)?;
        for line in json.split(|b| *b == b'\n') {
            emit(line, row);
        }
    }
    Ok(damaged)
}

/// One captured Flight message: `(header, body)`.
type Frame<'a> = (&'a [u8], &'a [u8]);

/// The frames of a capture, and whether a partial frame followed them.
fn read_frames(mut bytes: &[u8]) -> (Vec<Frame<'_>>, bool) {
    let mut frames = Vec::new();
    while !bytes.is_empty() {
        let Some((header, body, rest)) = take_frame(bytes) else {
            return (frames, true);
        };
        frames.push((header, body));
        bytes = rest;
    }
    (frames, false)
}

/// The first frame of `bytes` as `(header, body, rest)`, or `None` if it is cut off.
fn take_frame(bytes: &[u8]) -> Option<(&[u8], &[u8], &[u8])> {
    let header_len = u32::from_le_bytes(bytes.get(..4)?.try_into().ok()?) as usize;
    let rest = &bytes[4..];
    let header = rest.get(..header_len)?;
    let rest = &rest[header_len..];
    let body_len = u64::from_le_bytes(rest.get(..8)?.try_into().ok()?) as usize;
    let rest = &rest[8..];
    let body = rest.get(..body_len)?;
    Some((header, body, &rest[body_len..]))
}

fn flight_data(header: &[u8], body: &[u8]) -> FlightData {
    FlightData {
        data_header: Bytes::copy_from_slice(header),
        data_body: Bytes::copy_from_slice(body),
        ..Default::default()
    }
}

/// Renders a batch as NDJSON with explicit nulls, so every column is present in every row.
fn batch_to_json(batch: &RecordBatch) -> Result<Vec<u8>, BoxError> {
    let mut json = Vec::new();
    let mut writer = arrow::json::WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, arrow::json::writer::LineDelimited>(&mut json);
    writer.write(batch)?;
    writer.finish()?;
    Ok(json)
}

/// arrow's JSON reader and writer handle a named timezone only with the `chrono-tz` feature,
/// which the bench does not enable. So columns tagged `from` are retagged `to`, an offset. The
/// instants are unchanged, and every output renders them the same way.
fn retag_timezone(batch: RecordBatch, from: &str, to: &str) -> Result<RecordBatch, BoxError> {
    let schema = batch.schema();
    let mut fields = Vec::with_capacity(schema.fields().len());
    let mut columns = Vec::with_capacity(schema.fields().len());
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        let mut field = field.as_ref().clone();
        let mut column = Arc::clone(column);
        if let DataType::Timestamp(unit, Some(tz)) = field.data_type()
            && tz.as_ref() == from
        {
            let data_type = DataType::Timestamp(*unit, Some(to.into()));
            let data = column
                .to_data()
                .into_builder()
                .data_type(data_type.clone())
                .build()?;
            column = make_array(data);
            field = Field::new(field.name(), data_type, field.is_nullable());
        }
        fields.push(field);
        columns.push(column);
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

#[cfg(test)]
#[allow(unused_imports, dead_code)] // Bench builds compile this module but never run it.
mod tests {
    use super::super::fake_zerobus::write_frame;
    use super::*;
    use arrow::datatypes::{Fields, TimeUnit};
    use serde_json::{Value, json};

    #[test]
    fn flight_capture_decodes_to_rows_and_keeps_complete_frames() {
        let schema = |tz: &str| {
            Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, true),
                Field::new_map(
                    "labels",
                    "entries",
                    Field::new("keys", DataType::Utf8, false),
                    Field::new("values", DataType::Utf8, true),
                    false,
                    true,
                ),
                Field::new(
                    "_event_time",
                    DataType::Timestamp(TimeUnit::Millisecond, Some(tz.into())),
                    true,
                ),
                Field::new(
                    "values",
                    DataType::List(Arc::new(Field::new(
                        "item",
                        DataType::Struct(Fields::from(vec![
                            Field::new("ts", DataType::Int64, true),
                            Field::new("v", DataType::Float64, true),
                        ])),
                        true,
                    ))),
                    true,
                ),
            ]))
        };
        // The JSON reader needs an offset, so the batch is read as "+00:00" and retagged to the
        // UTC the sink writes.
        let batch = |ts: i64| {
            let lines: String = (0..2)
                .map(|i| {
                    format!(
                        "{{\"name\":\"m\",\"labels\":{{\"a\":\"{i}\"}},\
                         \"_event_time\":\"2026-11-11T00:00:00Z\",\
                         \"values\":[{{\"ts\":{ts},\"v\":{i}.5}}]}}\n"
                    )
                })
                .collect();
            let mut reader = arrow::json::ReaderBuilder::new(schema("+00:00"))
                .build(std::io::Cursor::new(lines))
                .unwrap();
            let batch = reader.next().unwrap().unwrap();
            retag_timezone(batch, "+00:00", "UTC").unwrap()
        };
        let (first, second) = (batch(1), batch(2));
        let frames =
            arrow_flight::utils::batches_to_flight_data(&first.schema(), [&first, &second])
                .unwrap();
        let mut file = Vec::new();
        for frame in &frames {
            write_frame(&mut file, &frame.data_header, &frame.data_body);
        }
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("cand-a")).unwrap();
        let path = dir.path().join("cand-a/conn-000000.flight");
        std::fs::write(&path, &file).unwrap();

        std::fs::write(dir.path().join("index.ndjson"), "{}\n").unwrap();
        let mut rows: Vec<Value> = Vec::new();
        let found = ZerobusFlight
            .read_back(dir.path(), &mut |destination, line| {
                assert_eq!(destination, "cand-a");
                rows.push(serde_json::from_slice(line).unwrap())
            })
            .unwrap();
        assert_eq!((rows.len(), found.files, found.damaged), (4, 1, 0));
        assert_eq!(
            rows[0]["labels"],
            json!({"a": "0"}),
            "maps render as objects"
        );

        // Cut into the second batch: the first survives and the cut counts as damage.
        std::fs::write(&path, &file[..file.len() - 3]).unwrap();
        let mut count = 0;
        let found = ZerobusFlight
            .read_back(dir.path(), &mut |_, _| count += 1)
            .unwrap();
        assert_eq!((count, found.damaged), (2, 1));
    }
}
