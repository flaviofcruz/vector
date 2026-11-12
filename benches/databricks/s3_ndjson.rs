//! S3 sink output: the objects that `fake-s3` records, read back as NDJSON rows.
//!
//! Each object is stored as `<bucket>/<key>` under the fake's `--out`, with the sink's compression
//! kept. A row is one line of an object, and its destination is the object's `<bucket>/<key
//! prefix>`, without the file name.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::path::Path;

use super::fake_s3::{self, FakeS3Args};
use super::fakes::{EPHEMERAL_LOOPBACK, INDEX_FILE, RunningFake};
use super::io_at;
use super::sink::{FakeSetup, ReadBack, SinkKind, destination_of, emit, files_under};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Environment variable that gives a candidate's config the S3 fake's endpoint.
const S3_ENDPOINT_VAR: &str = "DATABRICKS_BENCH_S3_ENDPOINT";

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];
const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];

/// S3 objects written by `aws_s3` through the fake: NDJSON, one row per line.
pub struct S3Ndjson;

impl SinkKind for S3Ndjson {
    fn name(&self) -> &'static str {
        "s3"
    }

    fn start_fake(&self, setup: &FakeSetup) -> Result<RunningFake, BoxError> {
        fake_s3::start(FakeS3Args {
            listen: EPHEMERAL_LOOPBACK,
            out: setup.out.clone(),
            discard: false,
            latency_ms: 0,
            fail_every: 0,
        })
    }

    fn candidate_env(
        &self,
        _: &FakeSetup,
        fake: SocketAddr,
    ) -> Result<Vec<(&'static str, String)>, BoxError> {
        Ok(vec![
            (S3_ENDPOINT_VAR, format!("http://{fake}")),
            // Dummy credentials: the fake does not check them. The metadata service is off so
            // the SDK does not wait on it.
            ("AWS_ACCESS_KEY_ID", "databricks-bench".to_owned()),
            ("AWS_SECRET_ACCESS_KEY", "databricks-bench".to_owned()),
            ("AWS_EC2_METADATA_DISABLED", "true".to_owned()),
        ])
    }

    fn read_back(
        &self,
        out: &Path,
        row: &mut dyn FnMut(&str, &[u8]),
    ) -> Result<ReadBack, BoxError> {
        let mut found = ReadBack::default();
        for relative in files_under(out)? {
            // The fake's top-level index is not an object.
            if relative == Path::new(INDEX_FILE) {
                continue;
            }
            let destination = destination_of(&relative);
            found.files += 1;
            found.damaged +=
                decode_object(&out.join(&relative), &mut |line| row(&destination, line))?;
        }
        Ok(found)
    }
}

/// Calls `row` with each line of one object, without its line break. The object is decompressed by
/// its content, so `.json.zst` and plain `.json` objects both work. Returns 1 if the object was cut
/// short, after its complete lines.
fn decode_object(file: &Path, row: &mut dyn FnMut(&[u8])) -> Result<u64, BoxError> {
    let mut raw = BufReader::new(io_at(File::open(file), "opening", file)?);
    let head = io_at(raw.fill_buf(), "reading", file)?;
    let mut lines: Box<dyn BufRead> = if head.starts_with(&ZSTD_MAGIC) {
        let decoder = io_at(zstd::Decoder::new(raw), "decoding", file)?;
        Box::new(BufReader::new(decoder))
    } else if head.starts_with(&GZIP_MAGIC) {
        Box::new(BufReader::new(flate2::read::MultiGzDecoder::new(raw)))
    } else {
        Box::new(raw)
    };
    let mut damaged = 0;
    let mut line = Vec::new();
    loop {
        line.clear();
        match lines.read_until(b'\n', &mut line) {
            Ok(0) => break,
            Ok(_) => emit(line.strip_suffix(b"\n").unwrap_or(&line), row),
            Err(e) => {
                // A truncated zstd or gzip stream ends in an error; the rows before it count.
                eprintln!("verify: {}: {e}", file.display());
                damaged += 1;
                break;
            }
        }
    }
    Ok(damaged)
}

#[cfg(test)]
#[allow(unused_imports, dead_code)] // Bench builds compile this module but never run it.
mod tests {
    use super::*;
    use std::io::Write;

    const ROW: &str = "{\"name\":\"up\",\"labels\":{},\"values\":[{\"ts\":1,\"v\":1.0}]}\n";

    /// Reads back `root` and collects `(destination, row)` pairs, in read order.
    fn read(root: &Path) -> (ReadBack, Vec<(String, Vec<u8>)>) {
        let mut rows = Vec::new();
        let found = S3Ndjson
            .read_back(root, &mut |destination, bytes| {
                rows.push((destination.to_owned(), bytes.to_vec()))
            })
            .unwrap();
        (found, rows)
    }

    #[test]
    fn s3_objects_read_back_plain_zstd_and_gzip_with_their_destinations() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        // Objects are `<bucket>/<key>`; the destination drops the final file name.
        std::fs::create_dir_all(root.join("bucket/date=1")).unwrap();
        std::fs::write(root.join("bucket/date=1/plain.json"), ROW.repeat(2)).unwrap();
        let zst = zstd::encode_all(ROW.repeat(3).as_bytes(), 0).unwrap();
        std::fs::write(root.join("bucket/date=1/zstd.json.zst"), &zst).unwrap();
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(ROW.as_bytes()).unwrap();
        std::fs::write(root.join("bucket/gzip.json.gz"), gz.finish().unwrap()).unwrap();
        std::fs::write(root.join(INDEX_FILE), "{}\n").unwrap();

        let (found, rows) = read(root);
        assert_eq!(
            found,
            ReadBack {
                files: 3,
                damaged: 0
            },
            "the index is not an object"
        );
        assert_eq!(rows.len(), 6);
        let in_date = rows.iter().filter(|(d, _)| d == "bucket/date=1").count();
        assert_eq!(in_date, 5, "the two plain and three zstd rows");
        assert_eq!(rows.iter().filter(|(d, _)| d == "bucket").count(), 1);
        // Bytes are the line as written, without its line break.
        assert!(rows.iter().all(|(_, bytes)| bytes.last() != Some(&b'\n')));
        assert_eq!(rows[0].1, ROW.trim_end().as_bytes());
    }

    #[test]
    fn a_truncated_s3_object_keeps_its_rows_and_counts_as_damaged() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join("bucket")).unwrap();
        let long = zstd::encode_all(ROW.repeat(20_000).as_bytes(), 0).unwrap();
        std::fs::write(root.join("bucket/cut.json.zst"), &long[..long.len() - 4]).unwrap();

        let (found, rows) = read(root);
        assert_eq!(found.damaged, 1);
        assert!(rows.len() > 1, "rows before the cut are kept");
    }
}
