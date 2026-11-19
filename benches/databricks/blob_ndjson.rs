//! Object-store sink output: the objects that `fake-s3` (`aws_s3`), `fake-azure-blob`
//! (`azure_blob`) and `fake-gcs` (`gcp_cloud_storage`) record, read back as NDJSON rows. The three
//! stores are one [`BlobNdjson`] kind each, which differ only in the fake's protocol and the
//! environment that points a config at it.
//!
//! Each object is stored as `<bucket>/<key>` under the fake's `--out` (for Azure, the bucket is
//! `<account>/<container>`), with the sink's compression kept. A row is one line of an object, and
//! its destination is the object's directory, `<bucket>/<key prefix>`, without the file name. All
//! three sinks name objects `<prefix><time>-<uuid>.<extension>`.
//!
//! Each row includes a fingerprint of its object metadata: its file name with the parts
//! that differ per run masked (see [`object_name_shape`]), and the compression its content is in.
//! So a candidate whose key prefix differs only in the part that lands in the file name, or that
//! names or compresses its objects differently, is a difference even when its rows match.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::path::Path;

use super::fake_blob::{self, BlobApi, FakeBlobArgs};
use super::fakes::{EPHEMERAL_LOOPBACK, INDEX_FILE, RunningFake};
use super::io_at;
use super::sink::{FakeSetup, ReadBack, SinkKind, destination_of, emit, files_under, fingerprint};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];
const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];

/// Objects written by an object-store sink through its fake: NDJSON, one row per line.
pub struct BlobNdjson {
    name: &'static str,
    api: BlobApi,
    /// Gives the config the fake's `http://<addr>`: the S3 or GCS sink's `endpoint`, or the Azure
    /// connection string's `BlobEndpoint` before `/<account>`.
    endpoint_var: &'static str,
    /// Set as well, for an SDK that needs more than the endpoint.
    extra_env: &'static [(&'static str, &'static str)],
}

pub const S3_NDJSON: BlobNdjson = BlobNdjson {
    name: "s3",
    api: BlobApi::S3,
    endpoint_var: "DATABRICKS_BENCH_S3_ENDPOINT",
    // The AWS SDK signs every request, so it needs credentials: dummy ones, which the fake does
    // not check. The metadata service is off so the SDK does not wait on it.
    extra_env: &[
        ("AWS_ACCESS_KEY_ID", "databricks-bench"),
        ("AWS_SECRET_ACCESS_KEY", "databricks-bench"),
        ("AWS_EC2_METADATA_DISABLED", "true"),
    ],
};

pub const AZURE_BLOB_NDJSON: BlobNdjson = BlobNdjson {
    name: "azure_blob",
    api: BlobApi::AzureBlob,
    endpoint_var: "DATABRICKS_BENCH_AZURE_BLOB_ENDPOINT",
    extra_env: &[],
};

pub const GCS_NDJSON: BlobNdjson = BlobNdjson {
    name: "gcs",
    api: BlobApi::Gcs,
    endpoint_var: "DATABRICKS_BENCH_GCS_ENDPOINT",
    extra_env: &[],
};

impl SinkKind for BlobNdjson {
    fn name(&self) -> &'static str {
        self.name
    }

    fn start_fake(&self, setup: &FakeSetup) -> Result<RunningFake, BoxError> {
        fake_blob::start(
            self.api,
            FakeBlobArgs {
                listen: EPHEMERAL_LOOPBACK,
                out: setup.out.clone(),
                discard: false,
                latency_ms: setup.latency_ms,
                fail_every: 0,
            },
        )
    }

    fn candidate_env(
        &self,
        _: &FakeSetup,
        fake: SocketAddr,
    ) -> Result<Vec<(&'static str, String)>, BoxError> {
        let endpoint = (self.endpoint_var, format!("http://{fake}"));
        let extra = self.extra_env.iter().map(|(k, v)| (*k, (*v).to_owned()));
        Ok(std::iter::once(endpoint).chain(extra).collect())
    }

    fn read_back(
        &self,
        out: &Path,
        row: &mut dyn FnMut(&str, u64, &[u8]),
    ) -> Result<ReadBack, BoxError> {
        let mut found = ReadBack::default();
        for relative in files_under(out)? {
            // The fake's top-level index is not an object.
            if relative == Path::new(INDEX_FILE) {
                continue;
            }
            let destination = destination_of(&relative);
            found.files += 1;
            let name = relative.file_name().unwrap_or_default().to_string_lossy();
            found.damaged += decode_object(&out.join(&relative), &name, &mut |metadata, line| {
                row(&destination, metadata, line)
            })?;
        }
        Ok(found)
    }
}

/// An object's file name with the parts that differ per run masked, so names that differ only
/// there are equal. `aws_s3` names an object `<key prefix tail><time>-<uuid>.<extension>`, where
/// the time is `filename_time_format` rendered at upload. A UUID (8-4-4-4-12 hex digits) becomes
/// `<uuid>`, then every other ASCII digit becomes `#`. Everything else is kept: the key prefix's
/// tail, the time format's separators and letters, and the extension.
///
/// Digits are masked one for one, so a time rendered either side of a second, or of midnight, has
/// the same shape with a zero-padded format such as `%F-%s` (`####-##-##-##########`); one that is
/// not padded, such as `%-d`, changes shape on the day its width does. Month or weekday names
/// (`%b`, `%a`) are letters and so are kept, which makes a run that spans the change differ, as
/// does a `date=%F` key prefix (`check` warns about a run that crosses midnight). The cost of
/// masking every digit: a key prefix tail that differs only in its digits, with as many of them,
/// is not seen. A `filename_prepend_crypto_nonce` (8 hex digits) is not masked, so it differs.
pub fn object_name_shape(name: &str) -> String {
    let bytes = name.as_bytes();
    let mut shape = String::with_capacity(name.len());
    let mut i = 0;
    while i < bytes.len() {
        if is_uuid(&bytes[i..]) {
            shape.push_str("<uuid>");
            i += UUID_LEN;
            continue;
        }
        // Copy one whole character, so a non-ASCII name stays intact.
        let c = name[i..].chars().next().expect("i is on a char boundary");
        shape.push(if c.is_ascii_digit() { '#' } else { c });
        i += c.len_utf8();
    }
    shape
}

/// The length of a hyphenated UUID, `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`.
const UUID_LEN: usize = 36;

/// Whether `bytes` starts with a hyphenated UUID of hex digits, in either case.
fn is_uuid(bytes: &[u8]) -> bool {
    bytes.len() >= UUID_LEN
        && bytes[..UUID_LEN].iter().enumerate().all(|(i, b)| match i {
            8 | 13 | 18 | 23 => *b == b'-',
            _ => b.is_ascii_hexdigit(),
        })
}

/// Calls `row` with each line of one object, without its line break. The object is decompressed by
/// its content, so `.json.zst` and plain `.json` objects both work. Returns 1 if the object was cut
/// short, after its complete lines. The metadata fingerprint is computed once per object.
fn decode_object(
    file: &Path,
    name: &str,
    row: &mut dyn FnMut(u64, &[u8]),
) -> Result<u64, BoxError> {
    let mut raw = BufReader::new(io_at(File::open(file), "opening", file)?);
    let head = io_at(raw.fill_buf(), "reading", file)?;
    let (compression, mut lines): (_, Box<dyn BufRead>) = if head.starts_with(&ZSTD_MAGIC) {
        let decoder = io_at(zstd::Decoder::new(raw), "decoding", file)?;
        ("zstd", Box::new(BufReader::new(decoder)))
    } else if head.starts_with(&GZIP_MAGIC) {
        (
            "gzip",
            Box::new(BufReader::new(flate2::read::MultiGzDecoder::new(raw))),
        )
    } else {
        ("none", Box::new(raw))
    };
    let metadata = fingerprint((object_name_shape(name), compression));
    let mut damaged = 0;
    let mut line = Vec::new();
    loop {
        line.clear();
        match lines.read_until(b'\n', &mut line) {
            Ok(0) => break,
            Ok(_) => emit(line.strip_suffix(b"\n").unwrap_or(&line), &mut |bytes| {
                row(metadata, bytes)
            }),
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

    const STORES: [&BlobNdjson; 3] = [&S3_NDJSON, &AZURE_BLOB_NDJSON, &GCS_NDJSON];

    /// Reads back `root` as `(destination, metadata hash, original row)` triples, in read order,
    /// the same for every store.
    fn read(root: &Path) -> (ReadBack, Vec<(String, u64, Vec<u8>)>) {
        let [first, rest @ ..] = STORES.map(|store| {
            let mut rows = Vec::new();
            let found = store
                .read_back(root, &mut |destination, metadata, bytes| {
                    rows.push((destination.to_owned(), metadata, bytes.to_vec()))
                })
                .unwrap();
            (found, rows)
        });
        assert!(rest.iter().all(|other| *other == first));
        first
    }

    #[test]
    fn objects_read_back_plain_zstd_and_gzip_with_their_destinations() {
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
            (found.files, found.damaged),
            (3, 0),
            "the index is not an object"
        );
        assert_eq!(rows.len(), 6);
        let in_date = rows.iter().filter(|(d, _, _)| d == "bucket/date=1").count();
        assert_eq!(in_date, 5, "the two plain and three zstd rows");
        assert_eq!(rows.iter().filter(|(d, _, _)| d == "bucket").count(), 1);
        // Bytes are the line as written, without its line break.
        assert!(
            rows.iter()
                .all(|(_, _, bytes)| bytes.last() != Some(&b'\n'))
        );
        assert_eq!(rows[0].2, ROW.trim_end().as_bytes());
    }

    #[test]
    fn a_truncated_object_keeps_its_rows_and_counts_as_damaged() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join("bucket")).unwrap();
        let long = zstd::encode_all(ROW.repeat(20_000).as_bytes(), 0).unwrap();
        std::fs::write(root.join("bucket/cut.json.zst"), &long[..long.len() - 4]).unwrap();

        let (found, rows) = read(root);
        assert_eq!(found.damaged, 1);
        assert!(rows.len() > 1, "rows before the cut are kept");
    }

    #[test]
    fn object_names_mask_the_time_and_uuid_and_keep_the_rest() {
        let shape = object_name_shape(
            "vector-aggregator-lumberjack-2026-11-13-1794551152-0b8f6c3e-5d1a-4e2b-9c7d-1a2b3c4d5e6f.json.zst",
        );
        assert_eq!(
            shape,
            "vector-aggregator-lumberjack-####-##-##-##########-<uuid>.json.zst"
        );
        // Another second, another day, another UUID (upper case too): the same shape.
        for other in [
            "vector-aggregator-lumberjack-2026-11-13-1794551153-ffffffff-ffff-ffff-ffff-ffffffffffff.json.zst",
            "vector-aggregator-lumberjack-2026-11-14-1794585600-ABCDEF01-2345-6789-ABCD-EF0123456789.json.zst",
        ] {
            assert_eq!(object_name_shape(other), shape, "{other}");
        }
        // The fixed parts are kept: the key prefix's tail, the time format's shape, the extension.
        for differs in [
            "vector-aggregator-WRONG-2026-11-13-1794551152-0b8f6c3e-5d1a-4e2b-9c7d-1a2b3c4d5e6f.json.zst",
            "vector-aggregator-lumberjack-1794551152-0b8f6c3e-5d1a-4e2b-9c7d-1a2b3c4d5e6f.json.zst",
            "vector-aggregator-lumberjack-2026-11-13-1794551152-0b8f6c3e-5d1a-4e2b-9c7d-1a2b3c4d5e6f.json.gz",
            "vector-aggregator-lumberjack-2026-11-13-1794551152.json.zst",
        ] {
            assert_ne!(object_name_shape(differs), shape, "{differs}");
        }
        // Not a UUID: one hex digit short, or a separator moved.
        assert_eq!(
            object_name_shape("0b8f6c3e-5d1a-4e2b-9c7d-1a2b3c4d5e6"),
            "#b#f#c#e-#d#a-#e#b-#c#d-#a#b#c#d#e#"
        );
        assert_eq!(object_name_shape("é1.json"), "é#.json");
    }

    #[test]
    fn metadata_is_reused_per_object_and_includes_the_codec() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join("b/p")).unwrap();
        let zst = zstd::encode_all(ROW.as_bytes(), 0).unwrap();
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(ROW.as_bytes()).unwrap();
        let gz = gz.finish().unwrap();
        // Two zstd objects a second apart match; gzip with the same extension does not.
        std::fs::write(root.join("b/p/x-1794551152.json.zst"), &zst).unwrap();
        std::fs::write(root.join("b/p/x-1794551153.json.zst"), &zst).unwrap();
        std::fs::write(root.join("b/p/x-1794551154.json.zst"), &gz).unwrap();
        let (_, rows) = read(root);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].1, rows[1].1);
        assert_ne!(rows[0].1, rows[2].1);
    }

    fn setup(dir: &Path) -> FakeSetup {
        FakeSetup {
            out: dir.join("out"),
            tls_dir: dir.join("tls"),
            schema_endpoint: None,
            latency_ms: 0,
        }
    }

    /// A PUT of `body` to `path` on the fake, which must answer with a success status.
    async fn put(fake: &RunningFake, path: &str, headers: &[(&str, &str)], body: Vec<u8>) {
        let mut req = hyper::Request::put(format!("http://{}{path}", fake.addr));
        for (name, value) in headers {
            req = req.header(*name, *value);
        }
        let response = hyper::Client::new()
            .request(req.body(hyper::Body::from(body)).unwrap())
            .await
            .unwrap();
        assert!(response.status().is_success(), "{}", response.status());
    }

    #[tokio::test]
    async fn each_store_starts_its_fake_points_the_config_at_it_and_reads_its_objects() {
        // Every store's fake accepts this header; only Azure's needs it.
        let blob_type = [("x-ms-blob-type", "BlockBlob")];
        let aws = [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_EC2_METADATA_DISABLED",
        ];
        for (store, container, extra_env) in [
            (&S3_NDJSON, "bkt", &aws[..]),
            (&AZURE_BLOB_NDJSON, "acct/logs", &[]),
            (&GCS_NDJSON, "bkt", &[]),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let setup = setup(dir.path());
            let fake = store.start_fake(&setup).unwrap();
            let env = store.candidate_env(&setup, fake.addr).unwrap();
            let names: Vec<&str> = env.iter().map(|(k, _)| *k).collect();
            assert_eq!(names[0], store.endpoint_var);
            assert_eq!(names[1..], *extra_env, "{}", store.name);
            assert_eq!(env[0].1, format!("http://{}", fake.addr));
            let zst = zstd::encode_all(ROW.repeat(2).as_bytes(), 0).unwrap();
            let path = format!("/{container}/date=1/x-1.json.zst");
            put(&fake, &path, &blob_type, zst).await;
            let path = format!("/{container}/other/x-2.json");
            put(&fake, &path, &blob_type, ROW.into()).await;
            fake.stop().await.unwrap();

            let (found, rows) = read(&setup.out);
            assert_eq!((found.files, found.damaged), (2, 0), "{}", store.name);
            let destinations: Vec<&str> = rows.iter().map(|(d, _, _)| d.as_str()).collect();
            let (dated, other) = (format!("{container}/date=1"), format!("{container}/other"));
            assert_eq!(destinations, [&dated, &dated, &other], "{}", store.name);
            assert!(setup.out.join(INDEX_FILE).is_file());
        }
    }
}
