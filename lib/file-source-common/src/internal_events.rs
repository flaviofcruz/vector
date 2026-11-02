use std::{io::Error, path::Path, time::Duration};

use bytes::BytesMut;

use crate::FileFingerprint;

/// Why the file server stopped watching a file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileStopReason {
    /// A compressed archive was read to a clean EOF.
    ArchiveDone,
    /// The file is no longer found by path discovery (rotated away or deleted).
    FileGone,
}

impl FileStopReason {
    /// The `VectorFileStopReason` proto enum name for this reason.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ArchiveDone => "VECTOR_FILE_STOP_REASON_ARCHIVE_DONE",
            Self::FileGone => "VECTOR_FILE_STOP_REASON_FILE_GONE",
        }
    }
}

/// On-disk state of a watched file, as of the last time the file server looked it up
/// (watcher creation or the most recent discovery pass that found the file).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FileStat {
    /// Modification time in ms (unix time); `None` if the platform doesn't report it.
    pub mtime_ms: Option<i64>,
    pub size_bytes: Option<u64>,
}

impl FileStat {
    pub fn from_metadata(metadata: &std::fs::Metadata) -> Self {
        Self {
            mtime_ms: metadata
                .modified()
                .ok()
                .and_then(|mtime| mtime.duration_since(std::time::UNIX_EPOCH).ok())
                .and_then(|since_epoch| i64::try_from(since_epoch.as_millis()).ok()),
            size_bytes: Some(metadata.len()),
        }
    }
}

/// Details of a file the file server started reading from offset 0.
#[derive(Clone, Copy, Debug)]
pub struct FileReadStartedInfo {
    pub fingerprint: FileFingerprint,
    /// Lines read in the first read pass.
    pub lines_read: u64,
    /// Line bytes read in the first read pass.
    pub bytes_read: u64,
    pub stat: FileStat,
}

/// Details of a file the file server stopped watching.
#[derive(Clone, Copy, Debug)]
pub struct FileUnwatchedInfo {
    pub fingerprint: FileFingerprint,
    pub reason: FileStopReason,
    pub reached_eof: bool,
    /// Read position when the file was stopped (decompressed offset for archives).
    pub end_offset: u64,
    /// Lines read over the lifetime of the watcher.
    pub lines_read: u64,
    /// Line bytes read over the lifetime of the watcher.
    pub bytes_read: u64,
    pub stat: FileStat,
}

/// Every internal event in this crate has a corresponding
/// method in this trait which should emit the event.
pub trait FileSourceInternalEvents: Send + Sync + Clone + 'static {
    fn emit_file_added(&self, path: &Path);

    fn emit_file_resumed(&self, path: &Path, file_position: u64);

    fn emit_file_watch_error(&self, path: &Path, error: Error);

    fn emit_file_read_started(&self, path: &Path, info: &FileReadStartedInfo);

    fn emit_file_unwatched(&self, path: &Path, info: &FileUnwatchedInfo);

    fn emit_file_deleted(&self, path: &Path);

    fn emit_file_delete_error(&self, path: &Path, error: Error);

    fn emit_file_fingerprint_read_error(&self, path: &Path, error: Error);

    fn emit_file_checkpointed(&self, count: usize, duration: Duration);

    fn emit_file_checksum_failed(&self, path: &Path);

    fn emit_file_checkpoint_write_error(&self, error: Error);

    fn emit_files_open(&self, count: usize);

    fn emit_path_globbing_failed(&self, path: &Path, error: &Error);

    fn emit_file_line_too_long(
        &self,
        truncated_bytes: &BytesMut,
        configured_limit: usize,
        encountered_size_so_far: usize,
    );
}
