//! The sink side of a pipeline: how a sink's output is read back, as rows.
//!
//! A sink kind is one entry in [`SINKS`]. Its [`SinkKind`] reads back what its own fake recorded
//! under the fake's `--out`: every row, with the destination it was written under, flattened across
//! objects, batches and connections, since batching is timing-dependent. `verify` compares the rows;
//! nothing here knows what they should hold.
//!
//! A new sink plugs in by implementing [`SinkKind`] and listing it in [`SINKS`]. Nothing else in
//! the harness names a kind.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use super::fakes::RunningFake;
use super::s3_ndjson::S3Ndjson;
use super::zerobus_flight::ZerobusFlight;
use super::{err, io_at};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// What a sink's fake is given for one candidate. Every path is under the run's `--work-dir`.
pub struct FakeSetup {
    /// `<run dir>/<sink>-out`: where the fake records what arrives. Not yet created. The run dir
    /// is `<work-dir>/<candidate>` for `check` and `<work-dir>/<candidate>/<round>` for `bench`.
    pub out: PathBuf,
    /// `<run dir>/tls`: throwaway TLS files for an HTTPS fake, created on demand.
    pub tls_dir: PathBuf,
    /// The shared fake-uc, when a requested sink `needs_schema`; `None` otherwise.
    pub schema_endpoint: Option<SocketAddr>,
}

/// What reading back one output found besides its rows.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReadBack {
    /// Capture files read.
    pub files: u64,
    /// Records that could not be read to their end, such as a cut-off object or frame. The rows
    /// before the cut are still returned, but a damaged output never counts as identical.
    pub damaged: u64,
}

/// A comparison fingerprint, not a persistent identifier. `Hash` delimits compound values.
pub fn fingerprint(value: impl Hash) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

pub trait SinkKind: Sync {
    /// The kind in `--output <target>=<kind>:<dir>`.
    fn name(&self) -> &'static str;

    /// Calls `row` with each row's destination, metadata fingerprint and original bytes, in no
    /// particular order. The destination is the capture's directory. Metadata is fingerprinted
    /// once per object or schema and reused for its rows: the normalized file name and codec for
    /// S3, or the decoded schema as sent for Zerobus. Bytes exclude the line break.
    fn read_back(
        &self,
        out: &Path,
        row: &mut dyn FnMut(&str, u64, &[u8]),
    ) -> Result<ReadBack, BoxError>;

    /// Whether the sink reads its table schema from the shared fake-uc, which `check` starts once
    /// (from `--schema`) when any requested sink says so.
    fn needs_schema(&self) -> bool {
        false
    }

    /// Starts this kind's fake for one candidate, in this process, on the caller's runtime. The
    /// fake records what the candidate's sink writes under `setup.out`.
    fn start_fake(&self, setup: &FakeSetup) -> Result<RunningFake, BoxError>;

    /// The environment that points one candidate's config at the fake listening on `fake`.
    /// The config must interpolate exactly these names; see `check.rs` for the common ones.
    fn candidate_env(
        &self,
        setup: &FakeSetup,
        fake: SocketAddr,
    ) -> Result<Vec<(&'static str, String)>, BoxError>;
}

/// Every sink this build can read, by the kind its `--output` names.
pub const SINKS: &[&dyn SinkKind] = &[&S3Ndjson, &ZerobusFlight];

/// The sink with this kind, or an error that lists the known kinds.
pub fn sink_named(kind: &str) -> Result<&'static dyn SinkKind, BoxError> {
    SINKS
        .iter()
        .copied()
        .find(|sink| sink.name() == kind)
        .ok_or_else(|| {
            let known: Vec<_> = SINKS.iter().map(|sink| sink.name()).collect();
            err(format!(
                "unknown sink `{kind}`; known: {}",
                known.join(", ")
            ))
        })
}

/// The files under `root`, as paths relative to it, sorted.
pub fn files_under(root: &Path) -> Result<Vec<PathBuf>, BoxError> {
    fn walk(root: &Path, dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), BoxError> {
        for entry in io_at(std::fs::read_dir(dir), "reading --output dir", dir)? {
            let path = io_at(entry, "reading --output dir", dir)?.path();
            if path.is_dir() {
                walk(root, &path, out)?;
            } else {
                out.push(path.strip_prefix(root).unwrap_or(&path).to_owned());
            }
        }
        Ok(())
    }
    let mut files = Vec::new();
    walk(root, root, &mut files)?;
    files.sort();
    Ok(files)
}

/// The destination of a capture file: its directory under the fake's `--out`, which for S3 is
/// `<bucket>/<key prefix>` and for Zerobus the table's bucket.
pub fn destination_of(relative: &Path) -> String {
    relative
        .parent()
        .unwrap_or(Path::new(""))
        .to_string_lossy()
        .into_owned()
}

/// Hands one row to `row`, skipping blank lines.
pub fn emit(line: &[u8], row: &mut dyn FnMut(&[u8])) {
    if !line.iter().all(u8::is_ascii_whitespace) {
        row(line);
    }
}

#[cfg(test)]
#[allow(unused_imports, dead_code)] // Bench builds compile this module but never run it.
mod tests {
    use super::*;
    #[test]
    fn sinks_are_found_by_kind() {
        assert_eq!(sink_named("s3").unwrap().name(), "s3");
        assert_eq!(sink_named("zerobus").unwrap().name(), "zerobus");
        let error = sink_named("gcs").err().unwrap().to_string();
        assert!(error.contains("unknown sink `gcs`"), "{error}");
        assert!(error.contains("s3, zerobus"), "{error}");
    }
}
