use std::{
    collections::BTreeSet,
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, BufReader},
    sync::Mutex,
};
use tracing::{error, info, warn};

use super::{FilePosition, fingerprinter::FileFingerprint};

const TMP_FILE_NAME: &str = "checkpoints.new.json";
pub const CHECKPOINT_FILE_NAME: &str = "checkpoints.json";

/// How long a checkpoint is kept after its watcher is marked dead (`set_dead`,
/// e.g. the file went unfindable and was reaped from `fp_map`) before
/// `remove_expired` deletes it. Note this is keyed on watcher death, not file
/// deletion. 60s matches the historical hardcoded value; raise via
/// `Checkpointer::with_dead_retention` to bridge the gap until a rotated `.gz`
/// (same fingerprint) appears.
pub const DEFAULT_DEAD_RETENTION: chrono::Duration = chrono::Duration::seconds(60);

/// This enum represents the file format of checkpoints persisted to disk. Right
/// now there is only one variant, but any incompatible changes will require and
/// additional variant to be added here and handled anywhere that we transit
/// this format.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "version", rename_all = "snake_case")]
enum State {
    #[serde(rename = "1")]
    V1 { checkpoints: BTreeSet<Checkpoint> },
}

/// A simple JSON-friendly struct of the fingerprint/position pair, since
/// fingerprints as objects cannot be keys in a plain JSON map.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
#[serde(rename_all = "snake_case")]
struct Checkpoint {
    fingerprint: FileFingerprint,
    position: FilePosition,
    modified: DateTime<Utc>,
    #[serde(default)]
    is_done: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    /// Time the watcher was marked dead (`set_dead`), or `None` while watched.
    /// Persisted so the retention clock survives a restart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    removed_ts: Option<DateTime<Utc>>,
}

pub struct Checkpointer {
    tmp_file_path: PathBuf,
    stable_file_path: PathBuf,
    checkpoints: Arc<CheckpointsView>,
    last: Mutex<Option<State>>,
    /// How long a checkpoint for a reaped file is retained before cleanup.
    /// Defaults to [`DEFAULT_DEAD_RETENTION`]; override with
    /// [`Checkpointer::with_dead_retention`].
    dead_retention: chrono::Duration,
}

/// A thread-safe handle for reading and writing checkpoints in-memory across
/// multiple threads.
#[derive(Debug, Default)]
pub struct CheckpointsView {
    checkpoints: DashMap<FileFingerprint, FilePosition>,
    modified_times: DashMap<FileFingerprint, DateTime<Utc>>,
    removed_times: DashMap<FileFingerprint, DateTime<Utc>>,
    done: DashMap<FileFingerprint, bool>,
    /// Reverse map from file path to fingerprint for archived (`.gz`) files.
    /// Since archived files are immutable, their fingerprint never changes,
    /// so we can skip expensive fingerprinting (gzip decompression + CRC64)
    /// on every glob cycle by looking up the path here instead.
    archive_paths: DashMap<PathBuf, FileFingerprint>,
}

impl CheckpointsView {
    pub fn update(&self, fng: FileFingerprint, pos: FilePosition) {
        self.checkpoints.insert(fng, pos);
        self.clear_dead(fng);
    }

    /// Marks a fingerprint's checkpoint as live: clears the death mark and
    /// refreshes `modified`. Refreshing `modified` (not just on a read) keeps a
    /// still-present file — e.g. a rotated log compressed to `.gz` with a bumped
    /// mtime — from being evicted by the `ignore_before` load filter.
    pub fn clear_dead(&self, fng: FileFingerprint) {
        self.removed_times.remove(&fng);
        self.modified_times.insert(fng, Utc::now());
    }

    pub fn get(&self, fng: FileFingerprint) -> Option<FilePosition> {
        self.checkpoints.get(&fng).map(|r| *r.value())
    }

    pub fn set_done(&self, fng: FileFingerprint, path: &Path) {
        self.done.insert(fng, true);
        // Also ensure the archive path mapping exists for done files.
        self.archive_paths.insert(path.to_path_buf(), fng);
    }

    pub fn get_done(&self, fng: FileFingerprint) -> bool {
        self.done.get(&fng).map(|r| *r.value()).unwrap_or(false)
    }

    /// Records the path→fingerprint mapping for an archived (`.gz`) file.
    /// Call this when a `.gz` file is first fingerprinted so that subsequent
    /// glob cycles can skip the expensive fingerprinting step.
    pub fn set_archive_path(&self, fng: FileFingerprint, path: &Path) {
        self.archive_paths.insert(path.to_path_buf(), fng);
    }

    /// Returns the cached fingerprint for an archived file path, if known.
    /// This allows skipping gzip decompression + CRC64 during glob discovery.
    pub fn get_archive_fingerprint(&self, path: &Path) -> Option<FileFingerprint> {
        self.archive_paths.get(path).map(|r| *r.value())
    }

    pub fn set_dead(&self, fng: FileFingerprint) {
        self.removed_times.insert(fng, Utc::now());
    }

    pub fn update_key(&self, old: FileFingerprint, new: FileFingerprint) {
        if let Some((_, value)) = self.checkpoints.remove(&old) {
            self.checkpoints.insert(new, value);
        }

        if let Some((_, value)) = self.modified_times.remove(&old) {
            self.modified_times.insert(new, value);
        }

        if let Some((_, value)) = self.removed_times.remove(&old) {
            self.removed_times.insert(new, value);
        }

        if let Some((_, value)) = self.done.remove(&old) {
            self.done.insert(new, value);
        }

        // Update archive_paths entries that pointed to the old fingerprint.
        for mut entry in self.archive_paths.iter_mut() {
            if *entry.value() == old {
                *entry.value_mut() = new;
            }
        }
    }

    pub fn remove_expired(&self, retention: chrono::Duration) {
        let now = Utc::now();

        // Collect all of the expired keys. Removing them while iterating can
        // lead to deadlocks, the set should be small, and this is not a
        // performance-sensitive path.
        let to_remove = self
            .removed_times
            .iter()
            .filter(|entry| {
                let ts = entry.value();
                let duration = now - *ts;
                duration >= retention
            })
            .map(|entry| *entry.key())
            .collect::<Vec<FileFingerprint>>();

        for fng in to_remove {
            self.checkpoints.remove(&fng);
            self.modified_times.remove(&fng);
            self.removed_times.remove(&fng);
            self.done.remove(&fng);
            self.archive_paths.retain(|_, v| *v != fng);
        }
    }

    fn load(&self, checkpoint: Checkpoint) {
        self.checkpoints
            .insert(checkpoint.fingerprint, checkpoint.position);
        self.modified_times
            .insert(checkpoint.fingerprint, checkpoint.modified);
        if checkpoint.is_done {
            self.done.insert(checkpoint.fingerprint, true);
        }
        // Restore the death mark so the dead-retention clock resumes from where
        // it was rather than restarting on load. A still-present file clears
        // this again on its first rediscovery (see `clear_dead`).
        if let Some(removed_ts) = checkpoint.removed_ts {
            self.removed_times.insert(checkpoint.fingerprint, removed_ts);
        }
        // Restore the archive path mapping for any checkpoint that has one,
        // regardless of is_done status. This allows skipping fingerprinting
        // for in-progress archived files too.
        if let Some(path) = checkpoint.path {
            self.archive_paths
                .insert(PathBuf::from(path), checkpoint.fingerprint);
        }
    }

    fn set_state(&self, state: State, ignore_before: Option<DateTime<Utc>>) {
        match state {
            State::V1 { checkpoints } => {
                for checkpoint in checkpoints {
                    if let Some(ignore_before) = ignore_before
                        && checkpoint.modified < ignore_before
                    {
                        continue;
                    }
                    self.load(checkpoint);
                }
            }
        }
    }

    fn get_state(&self) -> State {
        // Build a reverse map (fingerprint → path) for serialization.
        let fng_to_path: std::collections::HashMap<FileFingerprint, String> = self
            .archive_paths
            .iter()
            .map(|entry| (*entry.value(), entry.key().to_string_lossy().into_owned()))
            .collect();

        State::V1 {
            checkpoints: self
                .checkpoints
                .iter()
                .map(|entry| {
                    let fingerprint = entry.key();
                    let position = entry.value();
                    Checkpoint {
                        fingerprint: *fingerprint,
                        position: *position,
                        modified: self
                            .modified_times
                            .get(fingerprint)
                            .map(|r| *r.value())
                            .unwrap_or_else(Utc::now),
                        is_done: self
                            .done
                            .get(fingerprint)
                            .map(|r| *r.value())
                            .unwrap_or(false),
                        path: fng_to_path.get(fingerprint).cloned(),
                        removed_ts: self.removed_times.get(fingerprint).map(|r| *r.value()),
                    }
                })
                .collect(),
        }
    }
}

impl Checkpointer {
    pub fn new(data_dir: &Path) -> Checkpointer {
        let tmp_file_path = data_dir.join(TMP_FILE_NAME);
        let stable_file_path = data_dir.join(CHECKPOINT_FILE_NAME);

        Checkpointer {
            tmp_file_path,
            stable_file_path,
            checkpoints: Arc::new(CheckpointsView::default()),
            last: Mutex::new(None),
            dead_retention: DEFAULT_DEAD_RETENTION,
        }
    }

    /// Override how long a reaped file's checkpoint is retained before cleanup.
    #[must_use]
    pub fn with_dead_retention(mut self, dead_retention: chrono::Duration) -> Self {
        self.dead_retention = dead_retention;
        self
    }

    pub fn view(&self) -> Arc<CheckpointsView> {
        Arc::clone(&self.checkpoints)
    }

    #[cfg(test)]
    pub fn update_checkpoint(&mut self, fng: FileFingerprint, pos: FilePosition) {
        self.checkpoints.update(fng, pos);
    }

    #[cfg(test)]
    pub fn get_checkpoint(&self, fng: FileFingerprint) -> Option<FilePosition> {
        self.checkpoints.get(fng)
    }

    /// Persist the current checkpoints state to disk, making our best effort to
    /// do so in an atomic way that allow for recovering the previous state in
    /// the event of a crash.
    pub async fn write_checkpoints(&self) -> Result<usize, io::Error> {
        // First drop any checkpoints for files that were removed longer than the
        // dead-retention window ago. This keeps our working set as small as
        // possible and makes sure we don't spend time and IO writing checkpoints
        // that don't matter anymore.
        self.checkpoints.remove_expired(self.dead_retention);

        let current = self.checkpoints.get_state();

        // Fetch last written state.
        let mut last = self.last.lock().await;
        if last.as_ref() != Some(&current) {
            // Write the new checkpoints to a tmp file and flush it fully to
            // disk. If vector dies anywhere during this section, the existing
            // stable file will still be in its current valid state and we'll be
            // able to recover.
            let tmp_file_path = self.tmp_file_path.clone();

            // spawn_blocking shouldn't be needed: https://github.com/vectordotdev/vector/issues/23743
            let current = tokio::task::spawn_blocking(move || -> Result<State, io::Error> {
                let mut f = std::io::BufWriter::new(std::fs::File::create(tmp_file_path)?);
                serde_json::to_writer(&mut f, &current)?;
                f.into_inner()?.sync_all()?;
                Ok(current)
            })
            .await
            .map_err(io::Error::other)??;

            // Once the temp file is fully flushed, rename the tmp file to replace
            // the previous stable file. This is an atomic operation on POSIX
            // systems (and the stdlib claims to provide equivalent behavior on
            // Windows), which should prevent scenarios where we don't have at least
            // one full valid file to recover from.
            fs::rename(&self.tmp_file_path, &self.stable_file_path).await?;

            *last = Some(current);
        }

        Ok(self.checkpoints.checkpoints.len())
    }

    /// Read persisted checkpoints from disk, preferring the new JSON file
    /// format but falling back to the legacy system when those files are found
    /// instead.
    pub async fn read_checkpoints(&mut self, ignore_before: Option<DateTime<Utc>>) {
        // First try reading from the tmp file location. If this works, it means
        // that the previous process was interrupted in the process of
        // checkpointing and the tmp file should contain more recent data that
        // should be preferred.
        match self.read_checkpoints_file(&self.tmp_file_path).await {
            Ok(state) => {
                warn!(message = "Recovered checkpoint data from interrupted process.");
                self.checkpoints.set_state(state, ignore_before);

                // Try to move this tmp file to the stable location so we don't
                // immediately overwrite it when we next persist checkpoints.
                if let Err(error) = fs::rename(&self.tmp_file_path, &self.stable_file_path).await {
                    warn!(message = "Error persisting recovered checkpoint file.", %error);
                }
                return;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                // This is expected, so no warning needed
            }
            Err(error) => {
                error!(message = "Unable to recover checkpoint data from interrupted process.", %error);
            }
        }

        // Next, attempt to read checkpoints from the stable file location. This
        // is the expected location, so warn more aggressively if something goes
        // wrong.
        match self.read_checkpoints_file(&self.stable_file_path).await {
            Ok(state) => {
                info!(message = "Loaded checkpoint data.");
                self.checkpoints.set_state(state, ignore_before);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                // This is expected, so no warning needed
            }
            Err(error) => {
                warn!(message = "Unable to load checkpoint data.", %error);
            }
        }
    }

    async fn read_checkpoints_file(&self, path: &Path) -> Result<State, io::Error> {
        // Possible optimization: mmap the file into a slice and pass it into serde_json instead of
        // calling read_to_end. Need to investigate if this would work with tokio::fs::File

        let mut reader = BufReader::new(File::open(path).await?);
        let mut output = Vec::new();
        reader.read_to_end(&mut output).await?;

        serde_json::from_slice(&output[..])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use chrono::{Duration, Utc};
    use similar_asserts::assert_eq;
    use tempfile::tempdir;
    use tokio::fs;

    use super::{
        CHECKPOINT_FILE_NAME, Checkpoint, Checkpointer, FileFingerprint, FilePosition,
        TMP_FILE_NAME,
    };

    #[test]
    fn test_checkpointer_basics() {
        let fingerprints = vec![
            FileFingerprint::DevInode(1, 2),
            FileFingerprint::FirstLinesChecksum(78910),
        ];
        for fingerprint in fingerprints {
            let position: FilePosition = 1234;
            let data_dir = tempdir().unwrap();
            let mut chkptr = Checkpointer::new(data_dir.path());
            chkptr.update_checkpoint(fingerprint, position);
            assert_eq!(chkptr.get_checkpoint(fingerprint), Some(position));
        }
    }

    #[tokio::test]
    async fn test_checkpointer_ignore_before() {
        let now = Utc::now();
        let newer = (FileFingerprint::DevInode(1, 2), now - Duration::seconds(5));
        let oldish = (
            FileFingerprint::FirstLinesChecksum(78910),
            now - Duration::seconds(15),
        );
        let older = (FileFingerprint::DevInode(3, 4), now - Duration::seconds(20));
        let ignore_before = Some(now - Duration::seconds(12));

        let position: FilePosition = 1234;
        let data_dir = tempdir().unwrap();

        // load and persist the checkpoints
        {
            let chkptr = Checkpointer::new(data_dir.path());

            for (fingerprint, modified) in &[&newer, &oldish, &older] {
                chkptr.checkpoints.load(Checkpoint {
                    fingerprint: *fingerprint,
                    position,
                    modified: *modified,
                    is_done: false,
                    path: None,
                    removed_ts: None,
                });
                assert_eq!(chkptr.get_checkpoint(*fingerprint), Some(position));
                chkptr.write_checkpoints().await.unwrap();
            }
        }

        // read them back and assert old are removed
        {
            let mut chkptr = Checkpointer::new(data_dir.path());
            chkptr.read_checkpoints(ignore_before).await;

            assert_eq!(chkptr.get_checkpoint(newer.0), Some(position));
            assert_eq!(chkptr.get_checkpoint(oldish.0), None);
            assert_eq!(chkptr.get_checkpoint(older.0), None);
        }
    }

    #[tokio::test]
    async fn test_checkpointer_restart() {
        let fingerprints = vec![
            FileFingerprint::DevInode(1, 2),
            FileFingerprint::FirstLinesChecksum(78910),
        ];
        for fingerprint in fingerprints {
            let position: FilePosition = 1234;
            let data_dir = tempdir().unwrap();
            {
                let mut chkptr = Checkpointer::new(data_dir.path());
                chkptr.update_checkpoint(fingerprint, position);
                assert_eq!(chkptr.get_checkpoint(fingerprint), Some(position));
                chkptr.write_checkpoints().await.unwrap();
            }
            {
                let mut chkptr = Checkpointer::new(data_dir.path());
                assert_eq!(chkptr.get_checkpoint(fingerprint), None);
                chkptr.read_checkpoints(None).await;
                assert_eq!(chkptr.get_checkpoint(fingerprint), Some(position));
            }
        }
    }

    #[tokio::test]
    async fn test_checkpointer_file_upgrades() {
        let fingerprint = FileFingerprint::DevInode(1, 2);
        let position: FilePosition = 1234;

        let data_dir = tempdir().unwrap();

        {
            let mut chkptr = Checkpointer::new(data_dir.path());
            chkptr.update_checkpoint(fingerprint, position);
            assert_eq!(chkptr.get_checkpoint(fingerprint), Some(position));

            // Ensure that the new files were not written but the old style of files were
            assert!(!data_dir.path().join(TMP_FILE_NAME).exists());
            assert!(!data_dir.path().join(CHECKPOINT_FILE_NAME).exists());
            assert!(!data_dir.path().join("checkpoints").is_dir());

            chkptr.write_checkpoints().await.unwrap();

            assert!(!data_dir.path().join(TMP_FILE_NAME).exists());
            assert!(data_dir.path().join(CHECKPOINT_FILE_NAME).exists());
            assert!(!data_dir.path().join("checkpoints").is_dir());
        }

        // Read from those old files, ensure the checkpoints were loaded properly, and then write
        // them normally (i.e. in the new format)
        {
            let mut chkptr = Checkpointer::new(data_dir.path());
            chkptr.read_checkpoints(None).await;
            assert_eq!(chkptr.get_checkpoint(fingerprint), Some(position));
            chkptr.write_checkpoints().await.unwrap();
        }

        // Ensure that the stable file is present, the tmp file is not, and the legacy files have
        // been cleaned up
        assert!(!data_dir.path().join(TMP_FILE_NAME).exists());
        assert!(data_dir.path().join(CHECKPOINT_FILE_NAME).exists());
        assert!(!data_dir.path().join("checkpoints").is_dir());

        // Ensure one last time that we can reread from the new files and get the same result
        {
            let mut chkptr = Checkpointer::new(data_dir.path());
            chkptr.read_checkpoints(None).await;
            assert_eq!(chkptr.get_checkpoint(fingerprint), Some(position));
        }
    }

    #[tokio::test]
    async fn test_checkpointer_expiration() {
        let cases = vec![
            // (checkpoint, position, seconds since removed)
            (FileFingerprint::FirstLinesChecksum(123), 0, 30),
            (FileFingerprint::FirstLinesChecksum(456), 1, 60),
            (FileFingerprint::FirstLinesChecksum(789), 2, 90),
            (FileFingerprint::FirstLinesChecksum(101112), 3, 120),
        ];

        let data_dir = tempdir().unwrap();
        // Pin a 60s retention so this boundary test is independent of the
        // (much longer) production default.
        let mut chkptr =
            Checkpointer::new(data_dir.path()).with_dead_retention(chrono::Duration::seconds(60));

        for (fingerprint, position, removed) in cases.clone() {
            chkptr.update_checkpoint(fingerprint, position);

            // slide these in manually so we don't have to sleep for a long time
            chkptr
                .checkpoints
                .removed_times
                .insert(fingerprint, Utc::now() - chrono::Duration::seconds(removed));

            assert_eq!(chkptr.get_checkpoint(fingerprint), Some(position));
        }

        // Update one that would otherwise be expired to ensure it sticks around
        chkptr.update_checkpoint(cases[2].0, 42);

        // Expiration is piggybacked on the persistence interval, so do a write to trigger it
        chkptr.write_checkpoints().await.unwrap();

        assert_eq!(chkptr.get_checkpoint(cases[0].0), Some(0));
        assert_eq!(chkptr.get_checkpoint(cases[1].0), None);
        assert_eq!(chkptr.get_checkpoint(cases[2].0), Some(42));
        assert_eq!(chkptr.get_checkpoint(cases[3].0), None);
    }

    #[tokio::test]
    async fn test_checkpointer_strategy_checksum_happy_path() {
        let data_dir = tempdir().unwrap();

        let mut fingerprinter = crate::Fingerprinter::new(
            crate::FingerprintStrategy::FirstLinesChecksum {
                ignored_header_bytes: 0,
                lines: 1,
            },
            1024,
            false,
        );

        let log_path = data_dir.path().join("test.log");
        let contents = "hello i am a test log line that is just long enough but not super long\n";
        fs::write(&log_path, contents)
            .await
            .expect("writing test data");

        let new = fingerprinter
            .fingerprint(&log_path)
            .await
            .expect("getting new checksum");

        assert!(matches!(new, FileFingerprint::FirstLinesChecksum(_)));

        let mut chkptr = Checkpointer::new(data_dir.path());
        chkptr.update_checkpoint(new, 1234);
        assert_eq!(Some(1234), chkptr.get_checkpoint(new));
    }

    // guards against accidental changes to the checkpoint serialization
    #[tokio::test]
    async fn test_checkpointer_serialization() {
        let fingerprints = vec![
            (
                FileFingerprint::DevInode(1, 2),
                r#"{"version":"1","checkpoints":[{"fingerprint":{"dev_inode":[1,2]},"is_done":false,"position":1234}]}"#,
            ),
            (
                FileFingerprint::FirstLinesChecksum(78910),
                r#"{"version":"1","checkpoints":[{"fingerprint":{"first_lines_checksum":78910},"is_done":false,"position":1234}]}"#,
            ),
        ];
        for (fingerprint, expected) in fingerprints {
            let expected: serde_json::Value = serde_json::from_str(expected).unwrap();

            let position: FilePosition = 1234;
            let data_dir = tempdir().unwrap();
            let mut chkptr = Checkpointer::new(data_dir.path());

            chkptr.update_checkpoint(fingerprint, position);
            chkptr.write_checkpoints().await.unwrap();

            let got: serde_json::Value = {
                let s = fs::read_to_string(data_dir.path().join(CHECKPOINT_FILE_NAME))
                    .await
                    .unwrap();
                let mut checkpoints: serde_json::Value = serde_json::from_str(&s).unwrap();
                for checkpoint in checkpoints["checkpoints"].as_array_mut().unwrap() {
                    checkpoint.as_object_mut().unwrap().remove("modified");
                }
                checkpoints
            };

            assert_eq!(expected, got);
        }
    }

    // guards against accidental changes to the checkpoint deserialization and tests deserializing
    // old checkpoint versions
    #[tokio::test]
    async fn test_checkpointer_deserialization() {
        let serialized_checkpoints = r#"
{
  "version": "1",
  "checkpoints": [
    {
      "fingerprint": { "dev_inode": [ 1, 2 ] },
      "position": 1234,
      "modified": "2021-07-12T18:19:11.769003Z"
    },
    {
      "fingerprint": { "first_line_checksum": 1234 },
      "position": 1234,
      "modified": "2021-07-12T18:19:11.769003Z"
    },
    {
      "fingerprint": { "first_lines_checksum": 78910 },
      "position": 1234,
      "modified": "2021-07-12T18:19:11.769003Z"
    }
  ]
}
        "#;
        let fingerprints = vec![
            FileFingerprint::DevInode(1, 2),
            FileFingerprint::FirstLinesChecksum(1234),
            FileFingerprint::FirstLinesChecksum(78910),
        ];

        let data_dir = tempdir().unwrap();

        let mut chkptr = Checkpointer::new(data_dir.path());

        fs::write(
            data_dir.path().join(CHECKPOINT_FILE_NAME),
            serialized_checkpoints,
        )
        .await
        .unwrap();

        chkptr.read_checkpoints(None).await;

        for fingerprint in fingerprints {
            assert_eq!(chkptr.get_checkpoint(fingerprint), Some(1234))
        }
    }

    #[test]
    fn test_checkpoints_view_set_done_and_get_done() {
        let view = super::CheckpointsView::default();
        let fng = FileFingerprint::DevInode(1, 2);

        // Not done by default
        assert!(!view.get_done(fng));

        // Mark as done
        view.set_done(fng, Path::new("/tmp/test.gz"));
        assert!(view.get_done(fng));
        assert_eq!(
            view.get_archive_fingerprint(Path::new("/tmp/test.gz")),
            Some(fng)
        );

        // Different fingerprint is still not done
        let other = FileFingerprint::FirstLinesChecksum(999);
        assert!(!view.get_done(other));
    }

    #[test]
    fn test_checkpoints_view_update_key_transfers_done() {
        let view = super::CheckpointsView::default();
        let old = FileFingerprint::DevInode(1, 2);
        let new = FileFingerprint::DevInode(3, 4);
        let path = Path::new("/tmp/test.gz");

        view.set_done(old, path);
        assert!(view.get_done(old));

        view.update_key(old, new);
        assert!(!view.get_done(old));
        assert!(view.get_done(new));
        // archive_paths entry now points to the new fingerprint
        assert_eq!(view.get_archive_fingerprint(path), Some(new));
    }

    #[test]
    fn test_checkpoints_view_update_key_without_done() {
        let view = super::CheckpointsView::default();
        let old = FileFingerprint::DevInode(1, 2);
        let new = FileFingerprint::DevInode(3, 4);

        // update_key when old has no done entry should not create one for new
        view.update_key(old, new);
        assert!(!view.get_done(new));
    }

    #[test]
    fn test_checkpoints_view_remove_dead_clears_done() {
        let view = super::CheckpointsView::default();
        let fng = FileFingerprint::DevInode(1, 2);
        let path = Path::new("/tmp/test.gz");

        view.checkpoints.insert(fng, 100);
        view.set_done(fng, path);
        view.removed_times
            .insert(fng, Utc::now() - Duration::seconds(120));

        view.remove_expired(Duration::seconds(60));

        assert!(view.get(fng).is_none());
        assert!(!view.get_done(fng));
        assert!(view.get_archive_fingerprint(path).is_none());
    }

    #[test]
    fn test_checkpoints_view_load_with_is_done() {
        let view = super::CheckpointsView::default();
        let fng = FileFingerprint::DevInode(5, 6);

        // Load a checkpoint with is_done = true and a path
        view.load(Checkpoint {
            fingerprint: fng,
            position: 42,
            modified: Utc::now(),
            is_done: true,
            path: Some("/tmp/archive.gz".to_string()),
            removed_ts: None,
        });
        assert!(view.get_done(fng));
        assert_eq!(
            view.get_archive_fingerprint(Path::new("/tmp/archive.gz")),
            Some(fng)
        );

        // Load a checkpoint with is_done = false
        let fng2 = FileFingerprint::FirstLinesChecksum(111);
        view.load(Checkpoint {
            fingerprint: fng2,
            position: 99,
            modified: Utc::now(),
            is_done: false,
            path: None,
            removed_ts: None,
        });
        assert!(!view.get_done(fng2));
    }

    #[test]
    fn test_checkpoints_view_get_state_includes_is_done() {
        let view = super::CheckpointsView::default();
        let fng = FileFingerprint::DevInode(1, 2);
        let path = Path::new("/tmp/test.gz");

        view.checkpoints.insert(fng, 100);
        view.modified_times.insert(fng, Utc::now());
        view.set_done(fng, path);

        let state = view.get_state();
        match state {
            super::State::V1 { checkpoints } => {
                assert_eq!(checkpoints.len(), 1);
                let checkpoint = checkpoints.into_iter().next().unwrap();
                assert!(checkpoint.is_done);
                assert_eq!(checkpoint.path.as_deref(), Some("/tmp/test.gz"));
            }
        }
    }

    #[tokio::test]
    async fn test_checkpointer_done_persists_across_restart() {
        let fng = FileFingerprint::DevInode(10, 20);
        let position: FilePosition = 5678;
        let data_dir = tempdir().unwrap();

        // Write a checkpoint with done=true and a path
        {
            let chkptr = Checkpointer::new(data_dir.path());
            chkptr.checkpoints.load(Checkpoint {
                fingerprint: fng,
                position,
                modified: Utc::now(),
                is_done: true,
                path: Some("/var/log/app.gz".to_string()),
                removed_ts: None,
            });
            assert!(chkptr.checkpoints.get_done(fng));
            assert_eq!(
                chkptr
                    .checkpoints
                    .get_archive_fingerprint(Path::new("/var/log/app.gz")),
                Some(fng)
            );
            chkptr.write_checkpoints().await.unwrap();
        }

        // Read it back and verify done and path are preserved
        {
            let mut chkptr = Checkpointer::new(data_dir.path());
            chkptr.read_checkpoints(None).await;
            assert_eq!(chkptr.get_checkpoint(fng), Some(position));
            assert!(chkptr.checkpoints.get_done(fng));
            assert_eq!(
                chkptr
                    .checkpoints
                    .get_archive_fingerprint(Path::new("/var/log/app.gz")),
                Some(fng)
            );
        }
    }

    #[tokio::test]
    async fn test_checkpointer_deserialization_without_is_done() {
        // Verify backward compat: old checkpoints without is_done field default to false
        let serialized = r#"
{
  "version": "1",
  "checkpoints": [
    {
      "fingerprint": { "dev_inode": [ 7, 8 ] },
      "position": 999,
      "modified": "2021-07-12T18:19:11.769003Z"
    }
  ]
}
        "#;
        let data_dir = tempdir().unwrap();
        let mut chkptr = Checkpointer::new(data_dir.path());

        fs::write(data_dir.path().join(CHECKPOINT_FILE_NAME), serialized)
            .await
            .unwrap();

        chkptr.read_checkpoints(None).await;

        let fng = FileFingerprint::DevInode(7, 8);
        assert_eq!(chkptr.get_checkpoint(fng), Some(999));
        assert!(!chkptr.checkpoints.get_done(fng));
    }

    // ----- Dead-retention (L2a / L2a′ / L2b) -----

    #[test]
    fn test_remove_expired_respects_configurable_retention() {
        let view = super::CheckpointsView::default();
        let fng = FileFingerprint::DevInode(1, 2);
        view.checkpoints.insert(fng, 100);
        // Died 30 minutes ago.
        view.removed_times
            .insert(fng, Utc::now() - Duration::minutes(30));

        // Under a 1h retention it survives...
        view.remove_expired(Duration::hours(1));
        assert_eq!(view.get(fng), Some(100));

        // ...but under a 10m retention it is cleaned up.
        view.remove_expired(Duration::minutes(10));
        assert_eq!(view.get(fng), None);
    }

    #[test]
    fn test_clear_dead_prevents_expiry() {
        let view = super::CheckpointsView::default();
        let fng = FileFingerprint::DevInode(3, 4);
        view.checkpoints.insert(fng, 100);
        view.set_dead(fng);

        // Rediscovery clears the death mark (as watch_new_file does on watcher
        // creation), so even a very short retention won't drop it — this is the
        // caught-up-.gz case where no `update` ever fires.
        view.clear_dead(fng);
        view.remove_expired(Duration::zero());
        assert_eq!(view.get(fng), Some(100));
    }

    #[test]
    fn test_clear_dead_refreshes_modified_for_ignore_before() {
        // BUG-7: a rotated file compressed to `.gz` hours after its last read
        // keeps a fresh on-disk mtime but a stale checkpoint `modified` (frozen
        // at last read). On re-watch, `clear_dead` must refresh `modified` so the
        // `ignore_before` load filter does not evict a still-present file.
        let view = super::CheckpointsView::default();
        let fng = FileFingerprint::DevInode(7, 8);
        view.checkpoints.insert(fng, 100);
        // Simulate a stale last-read time (24h ago).
        view.modified_times
            .insert(fng, Utc::now() - Duration::hours(24));

        // Re-watch (rediscovered on disk) refreshes modified to ~now.
        view.clear_dead(fng);

        let modified = *view.modified_times.get(&fng).unwrap().value();
        assert!(
            Utc::now() - modified < Duration::minutes(1),
            "clear_dead should refresh modified to now, got {modified}"
        );
    }

    #[tokio::test]
    async fn test_dead_timestamp_persisted_and_enforced_across_restart() {
        let position: FilePosition = 1234;
        let data_dir = tempdir().unwrap();

        // Alive (never dead), dead-recently, dead-long-ago.
        let alive = FileFingerprint::DevInode(1, 1);
        let dead_recent = FileFingerprint::DevInode(2, 2);
        let dead_old = FileFingerprint::DevInode(3, 3);

        {
            // Long retention on the writer so all three entries persist.
            let chkptr = Checkpointer::new(data_dir.path()).with_dead_retention(Duration::hours(2));
            for fng in [alive, dead_recent, dead_old] {
                chkptr.checkpoints.update(fng, position);
            }
            // Stamp deaths directly to control the clock.
            chkptr
                .checkpoints
                .removed_times
                .insert(dead_recent, Utc::now() - Duration::minutes(5));
            chkptr
                .checkpoints
                .removed_times
                .insert(dead_old, Utc::now() - Duration::minutes(90));
            chkptr.write_checkpoints().await.unwrap();
        }

        // Restart: all entries load (no load-time death filter). The persisted
        // `removed` timestamp is restored so post-restart `remove_expired` still
        // enforces retention on the write tick — dead_old (90m) is dropped under
        // a 1h retention, dead_recent (5m) and alive survive.
        {
            let mut chkptr =
                Checkpointer::new(data_dir.path()).with_dead_retention(Duration::hours(1));
            chkptr.read_checkpoints(None).await;
            assert_eq!(chkptr.get_checkpoint(alive), Some(position));
            assert_eq!(chkptr.get_checkpoint(dead_recent), Some(position));
            assert_eq!(chkptr.get_checkpoint(dead_old), Some(position));

            // remove_expired (fired via the periodic write) enforces retention
            // using the restored `removed` timestamps.
            chkptr.write_checkpoints().await.unwrap();
            assert_eq!(chkptr.get_checkpoint(alive), Some(position));
            assert_eq!(chkptr.get_checkpoint(dead_recent), Some(position));
            assert_eq!(
                chkptr.get_checkpoint(dead_old),
                None,
                "dead_old past retention should be cleaned by remove_expired after restart"
            );
        }
    }

    #[test]
    fn test_checkpoint_deserialization_without_removed_defaults_none() {
        // Old on-disk files have no `removed` field; it must default to None.
        let json = r#"{
            "fingerprint": { "dev_inode": [ 9, 9 ] },
            "position": 5,
            "modified": "2021-07-12T18:19:11.769003Z"
        }"#;
        let cp: Checkpoint = serde_json::from_str(json).unwrap();
        assert!(cp.removed_ts.is_none());
    }
}
