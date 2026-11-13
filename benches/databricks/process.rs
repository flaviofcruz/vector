//! Child processes for `check`: candidate Vector processes, their logs, readiness and shutdown.
//!
//! Fakes are not here: they run as tasks in the harness's own process (see `fakes`). Candidates
//! run as children so that each one is a real Vector process, with its own signals and exit.

use std::fs::OpenOptions;
use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::process::{ExitStatus, Stdio};
use std::time::{Duration, Instant};

use tokio::process::{Child, Command};

use super::err;
use super::fakes::BoxError;
use super::source_codec::SourceCodec;

/// Lines of a candidate's log shown in an error.
const TAIL_LINES: usize = 25;

/// Messages Vector logs when it lost data: a shutdown that did not finish cleanly, or events a
/// component dropped by mistake. It exits 0 after each of them, so the exit status cannot show
/// that data was dropped, and a drop that hits the baseline and a candidate alike leaves their
/// outputs equal. `gracefully_closed = false` is not one: on this build's shutdown path it is
/// always false, and a clean run logs it.
const LOGGED_FAILURES: &[&str] = &[
    // internal_event/component_events_dropped.rs: a component dropped events it did not mean to
    // (a filter's drops log `intentional=true`). The first is always logged before any are
    // rate-limited.
    "Events dropped intentional=false",
    // topology/running.rs: the wave-1 deadline passed, and the remaining components were killed.
    "Failed to gracefully shut down in time",
    // topology/running.rs: the overall shutdown deadline aborted the components still running.
    "Overall shutdown deadline exceeded",
    // topology/mod.rs: a component failed or panicked, and Vector shut down around it.
    "An error occurred that Vector couldn't handle",
];

/// Starts `bin --config <config> --graceful-shutdown-limit-secs <n>` with `env` added to the
/// environment. Stdout and stderr both go to `log`, which must not exist yet. The child is killed
/// if this process drops it, so no candidate outlives the harness.
pub fn spawn_candidate(
    bin: &Path,
    config: &Path,
    env: &[(String, String)],
    log: &Path,
    graceful_shutdown_secs: u64,
) -> Result<Child, BoxError> {
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(log)
        .map_err(|e| err(format!("creating {}: {e}", log.display())))?;
    let stderr = file.try_clone()?;
    Command::new(bin)
        .arg("--config")
        .arg(config)
        .arg("--graceful-shutdown-limit-secs")
        .arg(graceful_shutdown_secs.to_string())
        .envs(env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        .stdin(Stdio::null())
        .stdout(file)
        .stderr(stderr)
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| err(format!("starting {}: {e}", bin.display())))
}

/// A free loopback port: bound, read and released. Another process may take it before the
/// candidate binds it, which then shows up as the candidate failing to start.
pub fn free_loopback_addr() -> Result<SocketAddr, BoxError> {
    Ok(TcpListener::bind("127.0.0.1:0")?.local_addr()?)
}

/// Waits until `addr` accepts TCP connections, failing as soon as the child exits or after
/// `timeout`, whichever comes first.
pub async fn wait_for_listener(
    codec: &dyn SourceCodec,
    addr: SocketAddr,
    child: &mut Child,
    timeout: Duration,
) -> Result<(), BoxError> {
    let deadline = Instant::now() + timeout;
    loop {
        if codec.ready(addr).await {
            return Ok(());
        }
        if let Some(status) = child.try_wait()? {
            return Err(err(format!("exited before listening on {addr} ({status})")));
        }
        if Instant::now() >= deadline {
            return Err(err(format!(
                "nothing listening on {addr} after {timeout:?}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub enum Stopped {
    Exited(ExitStatus),
    /// Did not exit within the timeout, and was killed with SIGKILL.
    Killed,
}

/// Sends SIGTERM, which makes Vector drain its sinks, then waits up to `timeout` for the exit.
/// A process still running after that is killed.
pub async fn terminate(child: &mut Child, timeout: Duration) -> Result<Stopped, BoxError> {
    if let Some(status) = child.try_wait()? {
        return Ok(Stopped::Exited(status));
    }
    if let Some(pid) = child.id() {
        // SAFETY: plain signal delivery to a child this process spawned and has not reaped.
        unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
    }
    match tokio::time::timeout(timeout, child.wait()).await {
        Ok(status) => Ok(Stopped::Exited(status?)),
        Err(_) => {
            child.kill().await?;
            Ok(Stopped::Killed)
        }
    }
}

/// The last lines of a log, for an error message. Says so when the log cannot be read or is empty.
pub fn log_tail(log: &Path) -> String {
    match std::fs::read(log) {
        Ok(bytes) => {
            let text = String::from_utf8_lossy(&bytes);
            let lines: Vec<&str> = text.lines().collect();
            if lines.is_empty() {
                return format!("({} is empty)", log.display());
            }
            lines[lines.len().saturating_sub(TAIL_LINES)..].join("\n")
        }
        Err(e) => format!("(could not read {}: {e})", log.display()),
    }
}

/// The first line of a candidate's log that reports lost data (see `LOGGED_FAILURES`).
pub fn logged_failure(log: &Path) -> Result<Option<String>, BoxError> {
    let bytes = std::fs::read(log).map_err(|e| err(format!("reading {}: {e}", log.display())))?;
    let text = String::from_utf8_lossy(&bytes);
    Ok(text
        .lines()
        .find(|line| LOGGED_FAILURES.iter().any(|marker| line.contains(marker)))
        .map(|line| line.trim().to_owned()))
}

#[cfg(test)]
#[allow(unused_imports, dead_code)] // Bench builds compile this module but never run it.
mod tests {
    use super::*;

    #[test]
    fn a_logged_failure_is_found_and_a_clean_log_is_not() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("vector.log");
        // A clean run logs `gracefully_closed = false` on this build's shutdown path.
        let clean =
            "INFO vector: Vector has stopped.\nINFO vector::topology: gracefully_closed = false\n";
        std::fs::write(&log, clean).unwrap();
        assert_eq!(logged_failure(&log).unwrap(), None);

        for marker in LOGGED_FAILURES {
            let line = format!("ERROR vector::topology::running: {marker}. Killing components.");
            std::fs::write(&log, format!("{clean}{line}\n")).unwrap();
            assert_eq!(
                logged_failure(&log).unwrap().as_deref(),
                Some(line.as_str())
            );
        }
    }

    #[test]
    fn a_missing_log_is_an_error_naming_it() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("absent.log");
        let error = logged_failure(&log).unwrap_err().to_string();
        assert!(error.contains("absent.log"), "{error}");
    }
}
