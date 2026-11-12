//! What the fakes share: how they run (in this process, or as `serve` until a signal), the ready
//! line, the index format, and percent-decoding of names in paths.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::{CmdResult, err};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Where a fake listens when the caller does not care which port: the OS picks a free one, which
/// `RunningFake::addr` then reports.
pub const EPHEMERAL_LOOPBACK: SocketAddr =
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));

/// How long `RunningFake::stop` waits for a fake to finish open requests before giving up.
const STOP_TIMEOUT: Duration = Duration::from_secs(20);

/// A fake running as tasks in this process, bound to `addr`. Each fake's `start` returns one.
pub struct RunningFake {
    pub addr: SocketAddr,
    stop: oneshot::Sender<()>,
    task: JoinHandle<Result<(), BoxError>>,
}

impl RunningFake {
    pub fn new(
        addr: SocketAddr,
        stop: oneshot::Sender<()>,
        task: JoinHandle<Result<(), BoxError>>,
    ) -> Self {
        Self { addr, stop, task }
    }

    /// Stops accepting, lets open requests finish (each fake bounds its own waits), and reports
    /// any error the fake hit while running.
    pub async fn stop(self) -> Result<(), BoxError> {
        let _ = self.stop.send(());
        match tokio::time::timeout(STOP_TIMEOUT, self.task).await {
            Ok(Ok(result)) => result,
            Ok(Err(join)) => Err(join.into()),
            Err(_) => Err(err(format!(
                "fake on {} did not stop within {STOP_TIMEOUT:?}",
                self.addr
            ))),
        }
    }
}

/// The `serve` subcommand of a fake: prints the ready line, then runs until SIGTERM or Ctrl-C.
pub async fn serve_until_signal(fake: RunningFake) -> CmdResult {
    println!("{READY_PREFIX}{}", fake.addr);
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
    fake.stop().await?;
    Ok(true)
}

/// Starts the line each fake prints on stdout once its listener is bound, followed by the bound
/// address, so a parent that passed a `:0` port can learn which port it got.
pub const READY_PREFIX: &str = "DATABRICKS_BENCH_LISTENING ";

/// Index file in `--out` (S3 and Zerobus only): one JSON line per accepted PUT or record batch,
/// in arrival order.
pub const INDEX_FILE: &str = "index.ndjson";

#[derive(Serialize, Deserialize)]
pub struct IndexEntry {
    pub bucket: String,
    pub key: String,
    /// S3: the object's bytes as stored. Zerobus: the frame's header and body bytes.
    pub bytes: u64,
    /// When the request or batch was fully read, before any injected delay.
    pub recv_ms: i64,
    /// When the success response or ack was sent: after injected latency or ack delay, and after
    /// the object or frame was stored.
    pub ack_ms: i64,
    /// Rows in the batch, when the fake can tell without decoding it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<u64>,
}

pub fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = |b: u8| (b as char).to_digit(16);
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(hi), Some(lo)) = (hex(bytes[i + 1]), hex(bytes[i + 2]))
        {
            out.push((hi * 16 + lo) as u8);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}
