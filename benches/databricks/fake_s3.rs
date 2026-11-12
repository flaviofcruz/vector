//! Minimal S3 endpoint for the `aws_s3` sink: path-style PutObject (plain or aws-chunked), plus
//! empty 200s for the HEAD/GET probes the SDK sends.
//!
//! Each PUT is acked as soon as its body is stored (or discarded under `--discard`), so in perf
//! runs the sink, not the endpoint, is the bottleneck. `--latency-ms` delays every PUT before it
//! is stored or failed, to emulate a remote store. `--fail-every N` answers every Nth PUT that
//! passes validation (counting from 1) with 503 SlowDown, to exercise the sink's retries.
//!
//! `--out` stands in for the bucket store:
//! - `<out>/<bucket>/<key>`: each object as uploaded, with aws-chunked framing removed and the
//!   sink's own compression kept, i.e. the bytes S3 would hold. Neither the bucket nor the key
//!   can leave `<out>`.
//! - `<out>/index.ndjson`: not part of S3. One line per accepted PUT in arrival order:
//!   bucket, key, bytes, recv_ms and ack_ms. It is written even under `--discard`. PUTs answered
//!   with an injected 503, or rejected, are not recorded. Nothing in this harness reads it yet;
//!   later steps will.
//!
//! Requests with a query string are refused with 501 NotImplemented, except the `x-id=PutObject`
//! tag the SDK adds to PutObject. Multipart uploads and other subresources would otherwise be
//! stored as plain objects, which would be the wrong bytes.

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

#[derive(clap::Args, Clone)]
pub struct FakeS3Args {
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
    args: FakeS3Args,
    puts: AtomicU64,
    index: std::sync::Mutex<std::fs::File>,
    index_path: PathBuf,
}

/// Starts the fake in this process, on the caller's runtime. `--out` must be missing or empty.
pub fn start(args: FakeS3Args) -> Result<RunningFake, BoxError> {
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

pub async fn serve(args: FakeS3Args) -> CmdResult {
    serve_until_signal(start(args)?).await
}

async fn handle(state: Arc<State>, req: Request<Body>) -> Result<Response<Body>, Infallible> {
    if !query_allowed(req.uri().query()) {
        eprintln!(
            "fake-s3: refusing {} {}?{}: only PutObject is implemented",
            req.method(),
            req.uri().path(),
            req.uri().query().unwrap_or_default()
        );
        return Ok(s3_error(StatusCode::NOT_IMPLEMENTED, "NotImplemented"));
    }
    Ok(match *req.method() {
        Method::PUT => put_object(&state, req).await.unwrap_or_else(|e| {
            eprintln!("fake-s3: {e}");
            s3_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError")
        }),
        // Healthcheck (HeadBucket) and anything else the SDK might probe with.
        Method::HEAD | Method::GET => Response::new(Body::empty()),
        _ => s3_error(StatusCode::NOT_IMPLEMENTED, "NotImplemented"),
    })
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
    let (parts, body) = req.into_parts();
    let (bucket, key) = split_path(parts.uri.path())?;
    let chunked = is_aws_chunked(&parts.headers);
    let raw = hyper::body::to_bytes(body).await?;
    let recv_ms = now_ms();

    let body = if chunked {
        let decoded = match decode_aws_chunked(&raw) {
            Ok(decoded) => decoded,
            Err(e) => return Ok(reject(&bucket, &key, "InvalidRequest", &e.to_string())),
        };
        if let Err(reason) = check_decoded_length(&parts.headers, decoded.len()) {
            return Ok(reject(&bucket, &key, "IncompleteBody", &reason));
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
        return Ok(s3_error(StatusCode::SERVICE_UNAVAILABLE, "SlowDown"));
    }

    if !state.args.discard {
        let path = object_path(&state.args.out, &bucket, &key)?;
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
    Ok(Response::builder()
        .header("etag", format!("\"{n:032x}\""))
        .header("x-amz-request-id", n.to_string())
        .body(Body::empty())?)
}

/// Answers a PUT the fake will not store, and says why on stderr.
fn reject(bucket: &str, key: &str, code: &str, reason: &str) -> Response<Body> {
    eprintln!("fake-s3: rejected PUT {bucket}/{key}: {reason}");
    s3_error(StatusCode::BAD_REQUEST, code)
}

fn split_path(path: &str) -> Result<(String, String), Box<dyn std::error::Error + Send + Sync>> {
    let trimmed = path.trim_start_matches('/');
    let (bucket, key) = trimmed
        .split_once('/')
        .ok_or_else(|| err(format!("expected path-style /bucket/key, got `{path}`")))?;
    let key = percent_decode(key);
    Ok((bucket.to_owned(), key))
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

fn s3_error(status: StatusCode, code: &str) -> Response<Body> {
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
}
