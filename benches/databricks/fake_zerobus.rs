//! Fake Zerobus ingestion: Arrow Flight `DoPut` over TLS with HTTP/2 (zstd gRPC compression
//! accepted), on `--listen`. Every other Flight RPC answers `Unimplemented`.
//!
//! The sink reads the table's columns from Unity Catalog, not from here (see `fake_uc.rs`). This
//! fake only records what the sink sends.
//!
//! The ingestion protocol follows the SDK's own mock (`rust/tests/src/mock_arrow_flight.rs` in
//! databricks/zerobus-sdk). A `DoPut`'s first message, when it has no app metadata, carries the
//! schema and is answered with a ready signal (`ack_up_to_offset = -1`). Each later message
//! carries app metadata `{"offset_id": n}` and is acked with that offset and the connection's
//! cumulative row count.
//!
//! On disk, under `--out` (`index.ndjson` is described in `fakes.rs`):
//! - `<out>/<bucket>/conn-NNNNNN.flight`, one per `DoPut`. The bucket is the table name's last
//!   dot-separated segment, so `main.databricks_bench.cand-a` lands in `cand-a/`; a segment that is
//!   empty, `.`, `..` or contains a slash or backslash is refused. Numbering starts at 0 in each
//!   process. Files are created new, so a clash fails rather than replacing a capture.
//! - Each file holds the frames as received, still compressed: the schema frame first, then one
//!   frame per batch, each laid out as `[u32 LE header length][header][u64 LE body length][body]`.
//!   The header and body are the Flight message's `data_header` and `data_body`.
//! - `index.ndjson` has one line per batch, keyed `conn-NNNNNN.flight#<seq>`. `seq` counts batches
//!   from 0 and skips the schema frame.
//!
//! `--tls-dir` holds `ca.pem`, `server.pem` and `server.key`, generated when `server.pem` is
//! missing. `ca.pem` is the throwaway CA the candidate must trust (`SSL_CERT_FILE`). The server
//! certificate is for `localhost` and `127.0.0.1`.
//!
//! A frame is written, and its index line made, only once its ack is certain to be sent. A batch
//! whose app metadata does not parse is neither written nor acked, and fails the stream.
//!
//! Injection: `--ack-delay-ms` delays each batch's ack after its frame is read, so the delay is
//! in `ack_ms` but not in `recv_ms`. `--discard` keeps frames out of `--out`; the index is still
//! written.

use std::io::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::Stream;
use openssl::ssl::{AlpnError, Ssl, SslAcceptor, SslFiletype, SslMethod};
use serde::Deserialize;
use serde_json::json;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic_014::codec::CompressionEncoding;
use tonic_014::{Request, Response, Status, Streaming};

use super::fakes::{BoxError, INDEX_FILE, IndexEntry, RunningFake, serve_until_signal};
use super::out_dir::claim_empty_out_dir;
use super::tls::TlsFiles;
use super::{CmdResult, err, io_at, now_ms};

pub const TABLE_HEADER: &str = "x-databricks-zerobus-table-name";
/// Large enough for any batch the sink can be configured to send.
const MAX_MESSAGE_BYTES: usize = 1 << 30;

#[derive(clap::Args, Clone)]
pub struct FakeZerobusArgs {
    /// Captured frames land in `<out>/<bucket>/conn-N.flight`. Must be missing or empty.
    #[arg(long)]
    pub out: PathBuf,
    /// Holds `ca.pem`, `server.pem`, `server.key`; generated if missing.
    #[arg(long)]
    pub tls_dir: PathBuf,
    #[arg(long, default_value = "127.0.0.1:0")]
    pub listen: SocketAddr,
    /// Count rows but do not store frames (perf runs that skip correctness). The index is still
    /// written.
    #[arg(long)]
    pub discard: bool,
    /// Delay added before each batch's ack, to emulate server-side commit latency. It falls
    /// after the frame is read, so it shows in the index's `ack_ms`, not `recv_ms`.
    #[arg(long, default_value_t = 0)]
    pub ack_delay_ms: u64,
}

struct State {
    args: FakeZerobusArgs,
    connections: AtomicU64,
    index: std::sync::Mutex<std::fs::File>,
    index_path: PathBuf,
}

/// Starts the fake in this process, on the caller's runtime. `--out` must be missing or empty.
pub fn start(args: FakeZerobusArgs) -> Result<RunningFake, BoxError> {
    claim_empty_out_dir(&args.out)?;
    let tls = TlsFiles::in_dir(&args.tls_dir);
    let tls = if tls.server_cert.exists() {
        tls
    } else {
        TlsFiles::generate(&args.tls_dir)?
    };
    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);

    // TLS terminated with OpenSSL, HTTP/2 + gRPC served by tonic. The listener is bound with std
    // so this stays synchronous: the caller may be a plain function inside the runtime.
    let listener = std::net::TcpListener::bind(args.listen)
        .map_err(|e| err(format!("binding --listen {}: {e}", args.listen)))?;
    listener.set_nonblocking(true)?;
    let listener = tokio::net::TcpListener::from_std(listener)?;
    let flight_addr = listener.local_addr()?;
    let acceptor = Arc::new(tls_acceptor(&tls)?);
    let (conn_tx, conn_rx) = mpsc::channel::<Result<TlsConn, std::io::Error>>(64);
    let mut stop_accept = shutdown_tx.subscribe();
    tokio::spawn(async move {
        loop {
            let (tcp, _) = tokio::select! {
                accepted = listener.accept() => match accepted {
                    Ok(pair) => pair,
                    Err(e) => { eprintln!("fake-zerobus: accept: {e}"); continue; }
                },
                _ = stop_accept.recv() => return,
            };
            let (acceptor, conn_tx) = (Arc::clone(&acceptor), conn_tx.clone());
            tokio::spawn(async move {
                let handshake = async {
                    let ssl = Ssl::new(acceptor.context())?;
                    let mut stream = tokio_openssl::SslStream::new(ssl, tcp)?;
                    Pin::new(&mut stream).accept().await?;
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(stream)
                };
                match handshake.await {
                    Ok(stream) => {
                        let _ = conn_tx.send(Ok(TlsConn(stream))).await;
                    }
                    Err(e) => eprintln!("fake-zerobus: TLS handshake failed: {e}"),
                }
            });
        }
    });
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
        connections: AtomicU64::new(0),
        index: index.into(),
        index_path,
    });
    let flight = FlightServiceServer::new(Flight {
        state: Arc::clone(&state),
    })
    .accept_compressed(CompressionEncoding::Zstd)
    .max_decoding_message_size(MAX_MESSAGE_BYTES)
    .max_encoding_message_size(MAX_MESSAGE_BYTES);
    let mut stop_flight = shutdown_tx.subscribe();
    let flight_server = tokio::spawn(
        tonic_014::transport::Server::builder()
            .add_service(flight)
            .serve_with_incoming_shutdown(ReceiverStream::new(conn_rx), async move {
                let _ = stop_flight.recv().await;
            }),
    );

    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let _ = stopped.await;
        let _ = shutdown_tx.send(());
        // Bounded: a client holding a stream open must not stall the harness.
        let _ = tokio::time::timeout(Duration::from_secs(10), flight_server).await;
        Ok::<(), BoxError>(())
    });
    Ok(RunningFake::new(flight_addr, stop, task))
}

pub async fn serve(args: FakeZerobusArgs) -> CmdResult {
    serve_until_signal(start(args)?).await
}

fn tls_acceptor(tls: &TlsFiles) -> Result<SslAcceptor, Box<dyn std::error::Error + Send + Sync>> {
    let mut b = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls_server())?;
    b.set_certificate_chain_file(&tls.server_cert)
        .map_err(|e| {
            err(format!(
                "loading TLS certificate {}: {e}",
                tls.server_cert.display()
            ))
        })?;
    b.set_private_key_file(&tls.server_key, SslFiletype::PEM)
        .map_err(|e| err(format!("loading TLS key {}: {e}", tls.server_key.display())))?;
    b.check_private_key()?;
    b.set_alpn_select_callback(|_, protocols| {
        openssl::ssl::select_next_proto(b"\x02h2", protocols).ok_or(AlpnError::NOACK)
    });
    Ok(b.build())
}

/// A TLS connection tonic can serve; tonic needs `Connected` on its incoming IO type.
struct TlsConn(tokio_openssl::SslStream<tokio::net::TcpStream>);

impl tonic_014::transport::server::Connected for TlsConn {
    type ConnectInfo = ();
    fn connect_info(&self) -> Self::ConnectInfo {}
}

impl AsyncRead for TlsConn {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for TlsConn {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

/// Bucket a table's frames are stored under: the table name's last segment.
pub fn bucket_for_table(table: &str) -> String {
    table.rsplit('.').next().unwrap_or(table).to_owned()
}

/// A bucket is used as one directory name under `--out`, so it must not be empty, `.`, `..`, or
/// contain a path separator.
fn plain_bucket(bucket: &str) -> bool {
    !bucket.is_empty() && bucket != "." && bucket != ".." && !bucket.contains(['/', '\\'])
}

#[derive(Deserialize)]
struct BatchMetadata {
    offset_id: i64,
}

/// Appends one record of a `.flight` capture file: `[u32 LE header len][header][u64 LE body len][body]`.
pub fn write_frame(out: &mut Vec<u8>, header: &[u8], body: &[u8]) {
    out.extend_from_slice(&(header.len() as u32).to_le_bytes());
    out.extend_from_slice(header);
    out.extend_from_slice(&(body.len() as u64).to_le_bytes());
    out.extend_from_slice(body);
}

struct Flight {
    state: Arc<State>,
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic_014::async_trait]
impl FlightService for Flight {
    type HandshakeStream = BoxStream<HandshakeResponse>;
    type ListFlightsStream = BoxStream<FlightInfo>;
    type DoGetStream = BoxStream<FlightData>;
    type DoPutStream = BoxStream<PutResult>;
    type DoExchangeStream = BoxStream<FlightData>;
    type DoActionStream = BoxStream<arrow_flight::Result>;
    type ListActionsStream = BoxStream<ActionType>;

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let table = request
            .metadata()
            .get(TABLE_HEADER)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| Status::invalid_argument(format!("missing {TABLE_HEADER}")))?
            .to_owned();
        let bucket = bucket_for_table(&table);
        if !plain_bucket(&bucket) {
            return Err(Status::invalid_argument(format!(
                "table {table} does not name a bucket directory"
            )));
        }
        let conn = self.state.connections.fetch_add(1, Ordering::Relaxed);
        let file_name = format!("conn-{conn:06}.flight");
        let path = self.state.args.out.join(&bucket).join(&file_name);
        let (tx, rx) = mpsc::channel(1024);
        let state = Arc::clone(&self.state);
        tokio::spawn(async move {
            if let Err(e) =
                ingest(state, request.into_inner(), &tx, &bucket, &file_name, &path).await
            {
                eprintln!("fake-zerobus: {table} {file_name}: {e}");
                let _ = tx.send(Err(Status::internal(e.to_string()))).await;
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn handshake(
        &self,
        _: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }
    async fn list_flights(
        &self,
        _: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }
    async fn get_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }
    async fn poll_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }
    async fn get_schema(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }
    async fn do_get(&self, _: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get"))
    }
    async fn do_exchange(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }
    async fn do_action(
        &self,
        _: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }
    async fn list_actions(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }
}

async fn ingest(
    state: Arc<State>,
    mut stream: Streaming<FlightData>,
    tx: &mpsc::Sender<Result<PutResult, Status>>,
    bucket: &str,
    file_name: &str,
    path: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut file = if state.args.discard {
        None
    } else {
        let parent = path.parent().expect("capture path has a parent");
        io_at(
            tokio::fs::create_dir_all(parent).await,
            "creating directory",
            parent,
        )?;
        let opened = io_at(
            tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .await,
            "creating capture file",
            path,
        )?;
        Some(tokio::io::BufWriter::with_capacity(1 << 20, opened))
    };
    let mut first = true;
    let mut records: u64 = 0;
    let mut seq: u64 = 0;
    let mut frame = Vec::new();
    while let Some(data) = stream.message().await? {
        let recv_ms = now_ms();
        // A message without app metadata is the schema, and is acked with no offset. Any other
        // message must carry its batch metadata; if it does not, the stream fails here, before
        // anything is recorded.
        let schema = std::mem::take(&mut first) && data.app_metadata.is_empty();
        let (offset, rows) = if schema {
            (-1, 0)
        } else {
            let metadata: BatchMetadata = serde_json::from_slice(&data.app_metadata)
                .map_err(|e| err(format!("bad batch app_metadata: {e}")))?;
            let rows = arrow::ipc::root_as_message(&data.data_header)
                .ok()
                .and_then(|m| m.header_as_record_batch())
                .map_or(0, |b| b.length() as u64);
            records += rows;
            if state.args.ack_delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(state.args.ack_delay_ms)).await;
            }
            (metadata.offset_id, rows)
        };
        // A frame is recorded only once its ack is certain to go out: the slot is reserved first,
        // and the send after the recording cannot fail. So the capture holds acked batches only.
        let ack = ack_result(offset, records)?;
        let slot = tx
            .reserve()
            .await
            .map_err(|_| err("client went away before the ack"))?;
        if let Some(file) = &mut file {
            frame.clear();
            write_frame(&mut frame, &data.data_header, &data.data_body);
            // Flush per frame: the stream stays open across batches, and the index already
            // counts each batch as received, so the file should not lag behind it.
            io_at(file.write_all(&frame).await, "writing capture file", path)?;
            io_at(file.flush().await, "writing capture file", path)?;
        }
        if !schema {
            let entry = IndexEntry {
                bucket: bucket.to_owned(),
                key: format!("{file_name}#{seq}"),
                bytes: (data.data_header.len() + data.data_body.len()) as u64,
                recv_ms,
                ack_ms: now_ms(),
                rows: Some(rows),
            };
            seq += 1;
            let mut line = serde_json::to_vec(&entry)?;
            line.push(b'\n');
            let mut index = state.index.lock().expect("index lock");
            io_at(index.write_all(&line), "writing index", &state.index_path)?;
        }
        slot.send(Ok(ack));
    }
    if let Some(mut file) = file {
        io_at(file.flush().await, "writing capture file", path)?;
    }
    Ok(())
}

/// The ack for a batch: its offset, and the connection's cumulative row count.
fn ack_result(offset: i64, records: u64) -> Result<PutResult, BoxError> {
    let metadata = json!({ "ack_up_to_offset": offset, "ack_up_to_records": records });
    Ok(PutResult {
        app_metadata: serde_json::to_vec(&metadata)?.into(),
    })
}

#[cfg(test)]
#[allow(unused_imports, dead_code)] // Bench builds compile this module but never run it.
mod tests {
    use super::*;

    #[test]
    fn bucket_is_last_table_segment() {
        assert_eq!(bucket_for_table("main.databricks_bench.cand-a"), "cand-a");
    }

    #[test]
    fn buckets_cannot_leave_the_out_dir() {
        assert!(plain_bucket("cand-a"));
        assert!(!plain_bucket(""));
        assert!(!plain_bucket(".."));
        assert!(!plain_bucket("."));
        assert!(!plain_bucket("a/../../x"));
        assert!(!plain_bucket("a\\b"));
    }
}
