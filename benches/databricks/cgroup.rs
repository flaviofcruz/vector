//! The counters `bench` reads from the cgroup a candidate runs in (its fence, see `fence`).
//!
//! `bench` reads the cgroup from outside it, so the reads cost the candidate nothing it would
//! count. The cgroup-v2 files are strict: each line is `name value`, a name may not repeat,
//! and a counter that is absent is an error, never a zero.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::err;
use super::fakes::BoxError;

/// How often the anonymous-memory sampler reads the cgroup.
const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// A cgroup's directory under the cgroup filesystem.
#[derive(Clone, Debug)]
pub struct Cgroup {
    dir: PathBuf,
}

impl Cgroup {
    pub fn at(dir: PathBuf) -> Self {
        Self { dir }
    }

    /// Total CPU time of the cgroup's tasks, in microseconds: `cpu.stat` `usage_usec`.
    pub fn cpu_usage_usec(&self) -> Result<u64, BoxError> {
        self.counter("cpu.stat", "usage_usec")
    }

    /// Anonymous memory now, in bytes: `memory.stat` `anon`.
    pub fn anon_bytes(&self) -> Result<u64, BoxError> {
        self.counter("memory.stat", "anon")
    }

    /// The highest memory use since the cgroup was made, in bytes: `memory.peak`.
    pub fn memory_peak(&self) -> Result<u64, BoxError> {
        let path = self.dir.join("memory.peak");
        let text = read(&path)?;
        text.trim().parse().map_err(|_| {
            err(format!(
                "{}: `{}` is not a byte count",
                path.display(),
                text.trim()
            ))
        })
    }

    /// Times the memory limit killed a task: `memory.events` `oom_kill`.
    pub fn oom_kills(&self) -> Result<u64, BoxError> {
        self.counter("memory.events", "oom_kill")
    }

    fn counter(&self, file: &str, name: &str) -> Result<u64, BoxError> {
        let path = self.dir.join(file);
        let text = read(&path)?;
        let fields = parse_fields(&text).map_err(|e| err(format!("{}: {e}", path.display())))?;
        fields
            .get(name)
            .copied()
            .ok_or_else(|| err(format!("{} has no `{name}` counter", path.display())))
    }
}

fn read(path: &Path) -> Result<String, BoxError> {
    std::fs::read_to_string(path).map_err(|e| err(format!("reading {}: {e}", path.display())))
}

/// Parses a cgroup-v2 keyed file: one `name value` per line, the value a counter. A name may not
/// repeat.
pub fn parse_fields(text: &str) -> Result<BTreeMap<&str, u64>, String> {
    let mut fields = BTreeMap::new();
    for line in text.lines() {
        let mut parts = line.split_whitespace();
        let (Some(name), Some(value), None) = (parts.next(), parts.next(), parts.next()) else {
            return Err(format!("`{line}` is not `name value`"));
        };
        let value: u64 = value
            .parse()
            .map_err(|_| format!("`{line}`: the value is not a counter"))?;
        if fields.insert(name, value).is_some() {
            return Err(format!("`{name}` appears twice"));
        }
    }
    Ok(fields)
}

/// Reads a cgroup's anonymous memory about once a second until stopped, and reports the highest
/// reading. Dropping it without `stop` ends the sampling too.
pub struct AnonSampler {
    stop: oneshot::Sender<()>,
    task: JoinHandle<Result<u64, BoxError>>,
}

impl AnonSampler {
    pub fn start(cgroup: Cgroup) -> Self {
        let (stop, mut stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let mut highest = 0;
            let mut tick = tokio::time::interval(SAMPLE_INTERVAL);
            loop {
                tokio::select! {
                    _ = &mut stopped => return Ok(highest),
                    _ = tick.tick() => highest = highest.max(cgroup.anon_bytes()?),
                }
            }
        });
        Self { stop, task }
    }

    /// Stops sampling and returns the highest anonymous memory read, in bytes.
    pub async fn stop(self) -> Result<u64, BoxError> {
        let _ = self.stop.send(());
        self.task.await?
    }
}

#[cfg(test)]
#[allow(unused_imports, dead_code)] // Bench builds compile this module but never run it.
mod tests {
    use super::*;

    #[test]
    fn counters_are_read_strictly() {
        let stat = "usage_usec 12\nuser_usec 3\nthrottled_usec 0\n";
        let fields = parse_fields(stat).unwrap();
        assert_eq!(fields.get("usage_usec"), Some(&12));
        assert_eq!(fields.get("throttled_usec"), Some(&0));

        let twice = parse_fields("anon 1\nanon 2\n").unwrap_err();
        assert!(twice.contains("`anon` appears twice"), "{twice}");
        assert!(parse_fields("anon\n").is_err());
        assert!(parse_fields("anon 1 2\n").is_err());
        assert!(parse_fields("anon many\n").is_err());
    }
}
