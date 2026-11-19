//! Minimal object-store endpoints, plus answers to the healthchecks' HEAD/GET probes (see
//! `probe`). Each sink uploads a batch as one PUT of the object:
//! - `fake-s3` (`aws_s3`): path-style PutObject (plain or aws-chunked) at `/<bucket>/<key>`.
//! - `fake-azure-blob` (`azure_blob`): Put Blob of a block blob at `/<account>/<container>/<blob>`,
//!   the path-style URL of a connection string whose `BlobEndpoint` is `http://<fake>/<account>`.
//! - `fake-gcs` (`gcp_cloud_storage`): the XML API's object PUT at `/<bucket>/<key>`.
//!
//! Each PUT is acked as soon as its body is stored (or discarded under `--discard`), so in perf
//! runs the sink, not the endpoint, is the bottleneck. `--latency-ms` delays every PUT before it
//! is stored or failed, to emulate a remote store. `--fail-every N` answers every Nth PUT that
//! passes validation (counting from 1) with 503 SlowDown, to exercise the sink's retries.
//! Errors carry the XML `<Error>` body that all three stores answer with.
//!
//! `--out` stands in for the bucket store:
//! - `<out>/<bucket>/<key>`: each object as uploaded, with aws-chunked framing removed and the
//!   sink's own compression kept, i.e. the bytes the store would hold. Azure's `<bucket>` is
//!   `<account>/<container>`. Neither the bucket nor the key can leave `<out>`.
//! - `<out>/index.ndjson`: not part of the store. One line per accepted PUT in arrival order:
//!   bucket, key, bytes, recv_ms and ack_ms. It is written even under `--discard`. PUTs answered
//!   with an injected 503, or rejected, are not recorded. Nothing in this harness reads it yet;
//!   later steps will.
//!
//! PUTs the fake does not model are refused with 501 NotImplemented (see `request_allowed`).
//! Multipart uploads, staged blocks and other subresources would otherwise be stored as plain
//! objects, which would be the wrong bytes.

use std::convert::Infallible;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, HeaderMap, Method, Request, Response, StatusCode};

use super::fakes::{
    BoxError, INDEX_FILE, IndexEntry, RunningFake, percent_decode, serve_until_signal,
};
use super::out_dir::claim_empty_out_dir;
use super::{CmdResult, err, io_at, now_ms};

/// Which store's upload protocol the fake speaks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobApi {
    S3,
    AzureBlob,
    Gcs,
}

impl BlobApi {
    /// Path segments before the object name: account and container, or bucket.
    fn container_segments(self) -> usize {
        match self {
            Self::AzureBlob => 2,
            Self::S3 | Self::Gcs => 1,
        }
    }
}

#[derive(clap::Args, Clone)]
pub struct FakeBlobArgs {
    #[arg(long, default_value = "127.0.0.1:0")]
    pub listen: SocketAddr,
    /// Objects land in `<out>/<bucket>/<key>`. Must be missing or empty.
    #[arg(long)]
    pub out: PathBuf,
    /// Do not store objects (perf runs that skip correctness). The index is still written.
    #[arg(long)]
    pub discard: bool,
    /// Delay added to every PUT before it is stored or failed, to emulate a remote store.
    #[arg(long, default_value_t = 0)]
    pub latency_ms: u64,
    /// Answer every Nth PUT that passes validation with 503 SlowDown (counting from 1; 0
    /// disables), to exercise sink retries.
    #[arg(long, default_value_t = 0)]
    pub fail_every: u64,
}

struct State {
    api: BlobApi,
    args: FakeBlobArgs,
    puts: AtomicU64,
    index: std::sync::Mutex<std::fs::File>,
    index_path: PathBuf,
}

/// Starts the fake in this process, on the caller's runtime. `--out` must be missing or empty.
pub fn start(api: BlobApi, args: FakeBlobArgs) -> Result<RunningFake, BoxError> {
    claim_empty_out_dir(&args.out)?;
    // Bind before creating the index: a failed bind must not leave an index that makes the
    // next start refuse the directory.
    let bound = hyper::Server::try_bind(&args.listen)
        .map_err(|e| err(format!("binding --listen {}: {e}", args.listen)))?;
    let index_path = args.out.join(INDEX_FILE);
    let index = io_at(
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&index_path),
        "creating index",
        &index_path,
    )?;
    let state = Arc::new(State {
        api,
        args: args.clone(),
        puts: AtomicU64::new(0),
        index: index.into(),
        index_path,
    });
    let make = make_service_fn(move |_| {
        let state = Arc::clone(&state);
        async move { Ok::<_, Infallible>(service_fn(move |req| handle(Arc::clone(&state), req))) }
    });
    let server = bound.serve(make);
    let addr = server.local_addr();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        server
            .with_graceful_shutdown(async move {
                let _ = stopped.await;
            })
            .await
            .map_err(|e| -> BoxError { e.into() })
    });
    Ok(RunningFake::new(addr, stop, task))
}

pub async fn serve(api: BlobApi, args: FakeBlobArgs) -> CmdResult {
    serve_until_signal(start(api, args)?).await
}

async fn handle(state: Arc<State>, req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let api = state.api;
    if let Err(reason) = request_allowed(api, &req) {
        eprintln!(
            "fake-blob {api:?}: refusing {} {}?{}: {reason}",
            req.method(),
            req.uri().path(),
            req.uri().query().unwrap_or_default()
        );
        return Ok(error(StatusCode::NOT_IMPLEMENTED, "NotImplemented"));
    }
    Ok(match *req.method() {
        Method::PUT => put_object(&state, req).await.unwrap_or_else(|e| {
            eprintln!("fake-blob {api:?}: {e}");
            error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError")
        }),
        // The healthchecks' bucket or container probes.
        Method::HEAD | Method::GET => probe(api),
        _ => error(StatusCode::NOT_IMPLEMENTED, "NotImplemented"),
    })
}

/// S3 takes no query but the SDK's `x-id=PutObject` tag, and GCS none at all. Azure's queries carry
/// the SAS token, which is not checked: a PUT must not name a `comp=` or `restype=` subresource
/// (staged blocks, container operations) and must be of a `BlockBlob`, and a GET or HEAD must be
/// the healthcheck's `restype=container` without a `comp=` (such as `comp=list`).
fn request_allowed(api: BlobApi, req: &Request<Body>) -> Result<(), &'static str> {
    let query = req.uri().query();
    let put = req.method() == Method::PUT;
    let params: Vec<&str> = query.into_iter().flat_map(|q| q.split('&')).collect();
    let has = |key| {
        params
            .iter()
            .any(|pair| pair.split('=').next() == Some(key))
    };
    let block_blob = req
        .headers()
        .get("x-ms-blob-type")
        .is_some_and(|v| v == "BlockBlob");
    match api {
        BlobApi::S3 if !query_allowed(query) => Err("only PutObject is implemented"),
        BlobApi::AzureBlob if put && (has("comp") || has("restype")) => {
            Err("only Put Blob is implemented")
        }
        BlobApi::AzureBlob if put && !block_blob => {
            Err("only block blobs (x-ms-blob-type: BlockBlob) are implemented")
        }
        BlobApi::AzureBlob if !put && (has("comp") || !params.contains(&"restype=container")) => {
            Err("only Put Blob and Get Container Properties are implemented")
        }
        BlobApi::Gcs if query.is_some() => Err("only the XML API's object PUT is implemented"),
        _ => Ok(()),
    }
}

/// The SDK tags PutObject with `?x-id=PutObject`; any other query is a subresource this fake
/// does not model.
fn query_allowed(query: Option<&str>) -> bool {
    matches!(query, None | Some("x-id=PutObject"))
}

async fn put_object(
    state: &State,
    req: Request<Body>,
) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>> {
    let api = state.api;
    let (parts, body) = req.into_parts();
    let (bucket, key) = split_path(parts.uri.path(), api.container_segments())?;
    // Only the S3 SDK frames bodies as aws-chunked.
    let chunked = api == BlobApi::S3 && is_aws_chunked(&parts.headers);
    let raw = hyper::body::to_bytes(body).await?;
    let recv_ms = now_ms();

    let body = if chunked {
        let decoded = match decode_aws_chunked(&raw) {
            Ok(decoded) => decoded,
            Err(e) => return Ok(reject(api, &bucket, &key, "InvalidRequest", &e.to_string())),
        };
        if let Err(reason) = check_decoded_length(&parts.headers, decoded.len()) {
            return Ok(reject(api, &bucket, &key, "IncompleteBody", &reason));
        }
        decoded
    } else {
        raw
    };

    let n = state.puts.fetch_add(1, Ordering::Relaxed) + 1;
    if state.args.latency_ms > 0 {
        tokio::time::sleep(Duration::from_millis(state.args.latency_ms)).await;
    }
    if state.args.fail_every > 0 && n.is_multiple_of(state.args.fail_every) {
        return Ok(error(StatusCode::SERVICE_UNAVAILABLE, "SlowDown"));
    }

    if !state.args.discard {
        // A second container segment (Azure's container) is checked as part of the key.
        let path = match bucket.split_once('/') {
            Some((account, container)) => {
                object_path(&state.args.out, account, &format!("{container}/{key}"))?
            }
            None => object_path(&state.args.out, &bucket, &key)?,
        };
        let body = body.clone();
        tokio::task::spawn_blocking(
            move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                let parent = path.parent().expect("object path has a parent");
                io_at(
                    std::fs::create_dir_all(parent),
                    "creating directory",
                    parent,
                )?;
                // create_new: a repeated key fails the PUT instead of replacing the stored object.
                let mut file = io_at(
                    std::fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&path),
                    "creating object",
                    &path,
                )?;
                io_at(file.write_all(&body), "writing object", &path)
            },
        )
        .await??;
    }
    let entry = IndexEntry {
        bucket,
        key,
        bytes: body.len() as u64,
        recv_ms,
        ack_ms: now_ms(),
        rows: None,
    };
    {
        let mut line = serde_json::to_vec(&entry)?;
        line.push(b'\n');
        let mut index = state.index.lock().expect("index lock");
        io_at(index.write_all(&line), "writing index", &state.index_path)?;
    }
    Ok(success(api, n)?)
}

/// Every store answers with an ETag. Azure's SDK also fails an upload whose response is not a 201
/// with the headers of `azure_headers` and `x-ms-request-server-encrypted`.
fn success(api: BlobApi, n: u64) -> Result<Response<Body>, hyper::http::Error> {
    let response = Response::builder().header("etag", format!("\"{n:032x}\""));
    match api {
        BlobApi::S3 => response.header("x-amz-request-id", n.to_string()),
        BlobApi::AzureBlob => azure_headers(response.status(StatusCode::CREATED), n)
            .header("x-ms-request-server-encrypted", "true"),
        BlobApi::Gcs => response,
    }
    .body(Body::empty())
}

/// The healthchecks of S3 (HeadBucket) and GCS (a HEAD of the bucket) read only the status, so they
/// get an empty 200. Azure's (Get Container Properties) fails without the container's ETag, the
/// headers of `azure_headers`, and its lease, immutability and legal-hold headers.
fn probe(api: BlobApi) -> Response<Body> {
    let response = Response::builder();
    match api {
        BlobApi::AzureBlob => azure_headers(response.header("etag", "\"0\""), 0)
            .header("x-ms-lease-status", "unlocked")
            .header("x-ms-lease-state", "available")
            .header("x-ms-has-immutability-policy", "false")
            .header("x-ms-has-legal-hold", "false"),
        BlobApi::S3 | BlobApi::Gcs => response,
    }
    .body(Body::empty())
    .expect("valid response")
}

/// Headers on every Azure response: Last-Modified, Date and a UUID `x-ms-request-id`, which the SDK
/// parses, and `x-ms-version`.
fn azure_headers(
    response: hyper::http::response::Builder,
    n: u64,
) -> hyper::http::response::Builder {
    let now = chrono::Utc::now()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string();
    response
        .header("last-modified", &now)
        .header("date", &now)
        .header(
            "x-ms-request-id",
            format!("00000000-0000-0000-0000-{n:012x}"),
        )
        .header("x-ms-version", "2022-11-02")
}

/// Answers a PUT the fake will not store, and says why on stderr.
fn reject(api: BlobApi, bucket: &str, key: &str, code: &str, reason: &str) -> Response<Body> {
    eprintln!("fake-blob {api:?}: rejected PUT {bucket}/{key}: {reason}");
    error(StatusCode::BAD_REQUEST, code)
}

/// Splits `/<container segments>/<name>`: `/<bucket>/<key>`, or `/<account>/<container>/<blob>`.
fn split_path(path: &str, segments: usize) -> Result<(String, String), BoxError> {
    let parts: Vec<&str> = path
        .trim_start_matches('/')
        .splitn(segments + 1, '/')
        .collect();
    match parts.split_last() {
        Some((key, bucket)) if bucket.len() == segments && !parts.contains(&"") => {
            Ok((bucket.join("/"), percent_decode(key)))
        }
        _ => Err(err(format!(
            "expected path-style /bucket/key, got `{path}`"
        ))),
    }
}

/// Keeps every object inside `<out>/<bucket>/`, whatever the bucket and key contain.
fn object_path(
    out: &Path,
    bucket: &str,
    key: &str,
) -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    let refuse = || {
        err(format!(
            "refusing object `{bucket}/{key}`: it would leave the bucket"
        ))
    };
    let mut bucket_parts = Path::new(bucket).components();
    let bucket_name = match (bucket_parts.next(), bucket_parts.next()) {
        (Some(Component::Normal(name)), None) => name,
        _ => return Err(refuse()),
    };
    let mut path = out.join(bucket_name);
    for component in Path::new(key).components() {
        match component {
            Component::Normal(part) => path.push(part),
            _ => return Err(refuse()),
        }
    }
    Ok(path)
}

fn is_aws_chunked(headers: &HeaderMap) -> bool {
    let has = |name: &str, needle: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.contains(needle))
    };
    has("content-encoding", "aws-chunked") || has("x-amz-content-sha256", "STREAMING-")
}

/// An aws-chunked upload declares its decoded size; a body that decodes to another length was
/// cut short or corrupted, so it must not be stored.
fn check_decoded_length(headers: &HeaderMap, decoded: usize) -> Result<(), String> {
    let Some(declared) = headers.get("x-amz-decoded-content-length") else {
        return Ok(());
    };
    let declared = declared
        .to_str()
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .ok_or_else(|| "x-amz-decoded-content-length is not a number".to_owned())?;
    if declared == decoded {
        Ok(())
    } else {
        Err(format!("declared {declared} decoded bytes, got {decoded}"))
    }
}

/// Strips `aws-chunked` framing (`<hex-size>[;ext]\r\n<data>\r\n` … `0\r\n<trailers>`).
fn decode_aws_chunked(body: &[u8]) -> Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
    let mut out = Vec::with_capacity(body.len());
    let mut pos = 0;
    loop {
        let line_end =
            find_crlf(&body[pos..]).ok_or_else(|| err("aws-chunked: missing chunk header"))? + pos;
        let header = std::str::from_utf8(&body[pos..line_end])?;
        let size_hex = header.split(';').next().unwrap_or_default().trim();
        let size = usize::from_str_radix(size_hex, 16)?;
        pos = line_end + 2;
        if size == 0 {
            return Ok(out.into());
        }
        let end = pos + size;
        if end > body.len() {
            return Err(err("aws-chunked: chunk overruns body"));
        }
        out.extend_from_slice(&body[pos..end]);
        pos = end + 2;
    }
}

fn find_crlf(bytes: &[u8]) -> Option<usize> {
    bytes.windows(2).position(|w| w == b"\r\n")
}

fn error(status: StatusCode, code: &str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "application/xml")
        .body(Body::from(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{code}</Code><Message>{code}</Message></Error>"
        )))
        .expect("valid response")
}

#[cfg(test)]
#[allow(unused_imports, dead_code)] // Bench builds compile this module but never run it.
mod tests {
    use super::super::fakes::EPHEMERAL_LOOPBACK;
    use super::*;
    use hyper::header::HeaderValue;

    #[test]
    fn aws_chunked_round_trip() {
        let body = b"5;chunk-signature=abc\r\nhello\r\n6\r\n world\r\n0\r\nx-amz-checksum-crc32:AAAA\r\n\r\n";
        assert_eq!(&decode_aws_chunked(body).unwrap()[..], b"hello world");
    }

    #[test]
    fn keys_and_buckets_cannot_escape_the_out_dir() {
        assert!(object_path(Path::new("/o"), "b", "../x").is_err());
        assert!(object_path(Path::new("/o"), "..", "x").is_err());
        assert!(object_path(Path::new("/o"), "a/b", "x").is_err());
        assert_eq!(
            object_path(Path::new("/o"), "b", "a/b.json").unwrap(),
            PathBuf::from("/o/b/a/b.json")
        );
    }

    #[test]
    fn only_the_sdk_putobject_query_is_allowed() {
        assert!(query_allowed(None));
        assert!(query_allowed(Some("x-id=PutObject")));
        assert!(!query_allowed(Some("uploads")));
        assert!(!query_allowed(Some("uploadId=abc&partNumber=1")));
        assert!(!query_allowed(Some("x-id=CreateMultipartUpload")));
        assert!(!query_allowed(Some("")));
    }

    #[test]
    fn decoded_length_must_match_the_declared_length() {
        let mut headers = HeaderMap::new();
        assert!(
            check_decoded_length(&headers, 5).is_ok(),
            "no header, no check"
        );
        headers.insert(
            "x-amz-decoded-content-length",
            HeaderValue::from_static("11"),
        );
        assert!(check_decoded_length(&headers, 11).is_ok());
        assert!(check_decoded_length(&headers, 5).is_err());
        headers.insert(
            "x-amz-decoded-content-length",
            HeaderValue::from_static("eleven"),
        );
        assert!(check_decoded_length(&headers, 11).is_err());
    }

    /// Sends `row\n` with an `x-ms-blob-type` header.
    async fn send(
        fake: &RunningFake,
        method: Method,
        path: &str,
        blob_type: &str,
    ) -> Response<Body> {
        let req = Request::builder()
            .method(method)
            .uri(format!("http://{}{path}", fake.addr))
            .header("x-ms-blob-type", blob_type);
        let req = req.body(Body::from("row\n")).unwrap();
        hyper::Client::new().request(req).await.unwrap()
    }

    fn args(out: &Path) -> FakeBlobArgs {
        let (listen, out) = (EPHEMERAL_LOOPBACK, out.to_owned());
        FakeBlobArgs {
            listen,
            out,
            discard: false,
            latency_ms: 0,
            fail_every: 0,
        }
    }

    fn index_lines(out: &Path) -> Vec<serde_json::Value> {
        let index = std::fs::read_to_string(out.join(INDEX_FILE)).unwrap();
        index
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    use BlobApi::{AzureBlob, Gcs, S3};

    /// A PUT of `row\n` that every store's fake models.
    async fn put(fake: &RunningFake, path: String) -> Response<Body> {
        send(fake, Method::PUT, &path, "BlockBlob").await
    }

    #[tokio::test]
    async fn every_store_stores_indexes_delays_fails_and_discards_puts_alike() {
        // Per store: the container path segments, the query its SDK adds to an upload (Azure: the
        // SAS token, not checked), and the success status and headers its SDK needs.
        for (api, container, query, created, headers) in [
            (S3, "bkt", "?x-id=PutObject", 200, "etag x-amz-request-id"),
            (
                AzureBlob,
                "acct/logs",
                "?sv=1&sig=s",
                201,
                "etag last-modified date x-ms-request-id x-ms-request-server-encrypted",
            ),
            (Gcs, "bkt", "", 200, "etag"),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let out = dir.path().join("out");
            let mut slow = args(&out);
            (slow.latency_ms, slow.fail_every) = (50, 3);
            let fake = start(api, slow).unwrap();
            let ok = put(&fake, format!("/{container}/date%3D1/x-1.json.zst{query}")).await;
            assert_eq!(ok.status(), created, "{api:?}");
            for h in headers.split(' ') {
                assert!(ok.headers().contains_key(h), "{api:?} {h}");
            }
            if let Some(id) = ok.headers().get("x-ms-request-id") {
                assert_eq!(id, "00000000-0000-0000-0000-000000000001", "a UUID");
            }
            // No container or key, or a key that would leave the store: 500. Only the last counts
            // toward --fail-every (PUT 2), as it is refused when stored.
            let escape = format!("/{container}/a/../../../x");
            for path in ["/x".to_owned(), format!("/{container}/"), escape] {
                let status = put(&fake, format!("{path}{query}")).await.status();
                assert_eq!(status, 500, "{api:?} {path}");
            }
            let failed = put(&fake, format!("/{container}/failed{query}")).await;
            assert_eq!(failed.status(), 503, "{api:?}");
            let body = hyper::body::to_bytes(failed.into_body()).await.unwrap();
            assert!(String::from_utf8_lossy(&body).contains("<Error><Code>SlowDown</Code>"));
            let third = put(&fake, format!("/{container}/x-3{query}")).await;
            assert_eq!(third.status(), created, "{api:?}");
            fake.stop().await.unwrap();

            let stored = out.join(container).join("date=1/x-1.json.zst");
            assert_eq!(std::fs::read_to_string(stored).unwrap(), "row\n");
            assert!(!out.join(container).join("failed").exists());
            let index = index_lines(&out);
            assert_eq!(
                index.len(),
                2,
                "{api:?}: nothing refused or failed is indexed"
            );
            assert_eq!(index[0]["bucket"], container);
            assert_eq!(index[0]["key"], "date=1/x-1.json.zst");
            let waited =
                index[0]["ack_ms"].as_i64().unwrap() - index[0]["recv_ms"].as_i64().unwrap();
            assert!(
                waited >= 50,
                "{api:?}: --latency-ms delays the ack: {waited}"
            );

            // --discard stores nothing but still indexes.
            let out = dir.path().join("discard");
            let fake = start(
                api,
                FakeBlobArgs {
                    discard: true,
                    ..args(&out)
                },
            )
            .unwrap();
            let status = put(&fake, format!("/{container}/x{query}")).await.status();
            assert_eq!(status, created, "{api:?}");
            fake.stop().await.unwrap();
            let files = super::super::sink::files_under(&out).unwrap();
            assert_eq!(files, [Path::new(INDEX_FILE)], "{api:?}");
        }
    }

    #[tokio::test]
    async fn each_store_answers_its_healthcheck_probe_and_refuses_other_queries() {
        let azure_probe = "/acct/logs?sv=1&restype=container&sig=s";
        for (api, path, status) in [
            // HeadBucket; ListObjectsV2 and GetObject are not modeled.
            (S3, "/bkt", 200),
            (S3, "/bkt?list-type=2", 501),
            (S3, "/bkt/x?x-id=GetObject", 501),
            // Get Container Properties, with the SAS token; listing, or a blob read, is not.
            (AzureBlob, azure_probe, 200),
            (AzureBlob, "/acct/logs?restype=container&comp=list", 501),
            (AzureBlob, "/acct/logs/x?sig=s", 501),
            // The bucket HEAD; a JSON API query is not modeled.
            (Gcs, "/bkt/", 200),
            (Gcs, "/bkt?alt=json", 501),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let fake = start(api, args(&dir.path().join("out"))).unwrap();
            for method in [Method::HEAD, Method::GET] {
                let response = send(&fake, method, path, "").await;
                assert_eq!(response.status(), status, "{api:?} {path}");
                if path == azure_probe {
                    for h in [
                        "etag",
                        "last-modified",
                        "date",
                        "x-ms-request-id",
                        "x-ms-lease-status",
                        "x-ms-lease-state",
                        "x-ms-has-immutability-policy",
                        "x-ms-has-legal-hold",
                    ] {
                        assert!(response.headers().contains_key(h), "{h}");
                    }
                }
            }
            fake.stop().await.unwrap();
        }
    }

    #[tokio::test]
    async fn each_store_refuses_the_uploads_it_does_not_model() {
        for (api, path, blob_type) in [
            // Multipart uploads.
            (S3, "/bkt/x?uploads", ""),
            (S3, "/bkt/x?uploadId=abc&partNumber=1", ""),
            // A staged block, a container operation, another blob type.
            (AzureBlob, "/acct/logs/x?comp=block&blockid=AA", "BlockBlob"),
            (AzureBlob, "/acct/logs?restype=container", "BlockBlob"),
            (AzureBlob, "/acct/logs/x", "AppendBlob"),
            // The API-key query, a resumable upload.
            (Gcs, "/bkt/x?key=abc", ""),
            (Gcs, "/bkt/x?uploadType=resumable", ""),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let out = dir.path().join("out");
            let fake = start(api, args(&out)).unwrap();
            let status = send(&fake, Method::PUT, path, blob_type).await.status();
            assert_eq!(status, 501, "{api:?} {path}");
            fake.stop().await.unwrap();
            assert!(index_lines(&out).is_empty(), "{api:?} {path}");
        }
    }
}
