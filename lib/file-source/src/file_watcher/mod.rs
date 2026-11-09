use async_compression::tokio::bufread::GzipDecoder;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use std::{
    io::{self, SeekFrom},
    path::PathBuf,
    time::Duration,
};
use tokio::{
    fs::File,
    io::{AsyncBufRead, AsyncBufReadExt, AsyncSeekExt, BufReader},
    time::Instant,
};
use tracing::{debug, info};
use vector_common::constants::GZIP_MAGIC;

use file_source_common::{
    AsyncFileInfo, FilePosition, FileStat, PortableFileExt, ReadFrom,
    buffer::{ReadResult, read_until_with_max_size},
};

const EOF_READ_BACKOFF_MIN: Duration = Duration::from_millis(1);
const EOF_READ_BACKOFF_MAX: Duration = Duration::from_millis(250);

#[cfg(test)]
mod tests;

/// The `RawLine` struct is a thin wrapper around the bytes that have been read
/// in order to retain the context of where in the file they have been read from.
///
/// The offset field contains the byte offset of the beginning of the line within
/// the file that it was read from.
#[derive(Debug)]
pub struct RawLine {
    pub offset: u64,
    pub bytes: Bytes,
}

#[derive(Debug)]
pub struct RawLineResult {
    pub raw_line: Option<RawLine>,
    pub discarded_for_size_and_truncated: Vec<BytesMut>,
}

/// How a watcher tells whether its file has unread bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UnreadCheck {
    /// The read position is below the size the last discovery pass saw.
    Size,
    /// The reader has not reached the end of the file yet. Used for gzipped files, whose read
    /// position counts decompressed bytes while their size is compressed; a gzipped file is not
    /// appended to, so it has unread bytes until it has been read to the end.
    Eof,
}

impl UnreadCheck {
    fn for_file(gzipped: bool) -> Self {
        if gzipped { Self::Eof } else { Self::Size }
    }
}

/// The `FileWatcher` struct defines the polling based state machine which reads
/// from a file path, transparently updating the underlying file descriptor when
/// the file has been rolled over, as is common for logs.
///
/// The `FileWatcher` is expected to live for the lifetime of the file
/// path. `FileServer` is responsible for clearing away `FileWatchers` which no
/// longer exist.
pub struct FileWatcher {
    pub path: PathBuf,
    pub is_active: bool,
    findable: bool,
    reader: Box<dyn AsyncBufRead + Send + Sync + Unpin>,
    file_position: FilePosition,
    devno: u64,
    inode: u64,
    is_dead: bool,
    reached_eof: bool,
    last_read_attempt: Instant,
    last_read_success: Instant,
    read_retry_delay: Duration,
    idle_read_interval: Duration,
    last_seen: Instant,
    max_line_bytes: usize,
    line_delimiter: Bytes,
    buf: BytesMut,
    /// Whether this watcher starts reading its file from offset 0 and has not yet reported
    /// its first read attempt (see `take_read_started`).
    read_started_pending: bool,
    lines_read: u64,
    bytes_read: u64,
    last_seen_stat: FileStat,
    /// Start offset retained with a partial line across EOF polling.
    buffered_line_start: Option<FilePosition>,
    /// How `unread_age` tells whether this file has unread bytes; `None` for files this watcher
    /// skips.
    unread_check: Option<UnreadCheck>,
}

impl FileWatcher {
    /// Create a new `FileWatcher`
    ///
    /// The input path will be used by `FileWatcher` to prime its state
    /// machine. A `FileWatcher` tracks _only one_ file. This function returns
    /// None if the path does not exist or is not readable by the current process.
    pub async fn new(
        path: PathBuf,
        read_from: ReadFrom,
        ignore_before: Option<DateTime<Utc>>,
        start_reading_at: Option<DateTime<Utc>>,
        max_line_bytes: usize,
        line_delimiter: Bytes,
        idle_read_interval: Duration,
    ) -> Result<FileWatcher, std::io::Error> {
        let f = File::open(&path).await?;
        let file_info = f.file_info().await?;
        let (devno, ino) = (file_info.portable_dev(), file_info.portable_ino());

        #[cfg(unix)]
        let metadata = file_info;
        #[cfg(windows)]
        let metadata = f.metadata().await?;

        let mut reader = BufReader::new(f);

        let too_old = if let (Some(ignore_before), Ok(modified_time)) = (
            ignore_before,
            metadata.modified().map(DateTime::<Utc>::from),
        ) {
            modified_time < ignore_before
        } else {
            false
        };

        // If the file was modified before the start_reading_at timestamp, this component is inactive. Therefore, we don't read it.
        let inactive = if let (Some(start_reading_at), Ok(modified_time)) = (
            start_reading_at,
            metadata.modified().map(DateTime::<Utc>::from),
        ) {
            modified_time < start_reading_at
        } else {
            false
        };

        let gzipped = is_gzipped(&mut reader).await?;

        // Determine the actual position at which we should start reading. Arms that skip the
        // file (`null_reader`) clear `reads_file`.
        let mut reads_file = true;
        let (reader, file_position): (Box<dyn AsyncBufRead + Send + Sync + Unpin>, FilePosition) =
            if inactive {
                debug!(
                    message = "Not reading file modified before `start_reading_at`.",
                    ?path,
                );
                reads_file = false;
                (Box::new(null_reader()), 0)
            } else {
                match (gzipped, too_old, read_from) {
                    (true, true, _) => {
                        if too_old {
                            debug!(
                                message = "Not reading gzipped file older than `ignore_older`.",
                                ?path,
                            );
                        }
                        reads_file = false;
                        (Box::new(null_reader()), 0)
                    }
                    (true, _, ReadFrom::Checkpoint(file_position)) => {
                        info!(
                            message = "Resuming gzipped file from offset.",
                            ?path,
                            %file_position,
                        );
                        (
                            gzip_reader_at_offset(reader, file_position).await?,
                            file_position,
                        )
                    }
                    // TODO: This may become the default, leading us to stop reading gzipped files that
                    // we were reading before. Should we merge this and the next branch to read
                    // compressed file from the beginning even when `read_from = "end"` (implicitly via
                    // default or explicitly via config)?
                    (true, _, ReadFrom::End) => {
                        debug!(
                            message = "Can't read from the end of already-compressed file.",
                            ?path,
                        );
                        reads_file = false;
                        (Box::new(null_reader()), 0)
                    }
                    (true, false, ReadFrom::Beginning) => {
                        (Box::new(BufReader::new(GzipDecoder::new(reader))), 0)
                    }
                    (false, true, _) => {
                        let pos = reader.seek(SeekFrom::End(0)).await.unwrap();
                        (Box::new(reader), pos)
                    }
                    (false, false, ReadFrom::Checkpoint(file_position)) => {
                        let pos = reader.seek(SeekFrom::Start(file_position)).await.unwrap();
                        (Box::new(reader), pos)
                    }
                    (false, false, ReadFrom::Beginning) => {
                        let pos = reader.seek(SeekFrom::Start(0)).await.unwrap();
                        (Box::new(reader), pos)
                    }
                    (false, false, ReadFrom::End) => {
                        let pos = reader.seek(SeekFrom::End(0)).await.unwrap();
                        (Box::new(reader), pos)
                    }
                }
            };
        // Reading the file from the beginning means a real reader positioned at offset 0. That
        // includes a file that is still empty when seeked to its end.
        let read_started_pending = reads_file && file_position == 0;

        let last_seen_stat = FileStat::from_metadata(&metadata);
        let unread_check = reads_file.then(|| UnreadCheck::for_file(gzipped));
        let ts = metadata
            .modified()
            .ok()
            .and_then(|mtime| mtime.elapsed().ok())
            .and_then(|diff| Instant::now().checked_sub(diff))
            .unwrap_or_else(Instant::now);

        Ok(FileWatcher {
            path,
            findable: true,
            reader,
            file_position,
            devno,
            inode: ino,
            is_dead: false,
            reached_eof: false,
            last_read_attempt: ts,
            last_read_success: ts,
            read_retry_delay: EOF_READ_BACKOFF_MIN,
            idle_read_interval,
            last_seen: ts,
            max_line_bytes,
            line_delimiter,
            buf: BytesMut::new(),
            buffered_line_start: None,
            is_active: !inactive,
            read_started_pending,
            lines_read: 0,
            bytes_read: 0,
            last_seen_stat,
            unread_check,
        })
    }

    pub async fn update_path(&mut self, path: PathBuf) -> io::Result<()> {
        let file_handle = File::open(&path).await?;

        let file_info = file_handle.file_info().await?;
        if (file_info.portable_dev(), file_info.portable_ino()) != (self.devno, self.inode) {
            let mut reader = BufReader::new(File::open(&path).await?);
            let gzipped = is_gzipped(&mut reader).await?;
            let new_reader: Box<dyn AsyncBufRead + Send + Sync + Unpin> = if gzipped {
                gzip_reader_at_offset(reader, self.file_position).await?
            } else {
                reader.seek(io::SeekFrom::Start(self.file_position)).await?;
                Box::new(reader)
            };
            self.reader = new_reader;
            self.unread_check = Some(UnreadCheck::for_file(gzipped));

            let file_info = file_handle.file_info().await?;
            self.devno = file_info.portable_dev();
            self.inode = file_info.portable_ino();
        }
        self.reached_eof = false;
        self.read_retry_delay = EOF_READ_BACKOFF_MIN;
        self.path = path;
        Ok(())
    }

    pub fn set_file_findable(&mut self, f: bool) {
        self.findable = f;
        if f {
            self.last_seen = Instant::now();
        }
    }

    pub fn file_findable(&self) -> bool {
        self.findable
    }

    pub fn set_dead(&mut self) {
        self.is_dead = true;
    }

    pub fn dead(&self) -> bool {
        self.is_dead
    }

    pub fn get_file_position(&self) -> FilePosition {
        self.file_position
    }

    /// The read position minus bytes buffered for a line still awaiting its delimiter, i.e. the
    /// end of the last line `read_line` returned. Checkpoint here, not at `get_file_position`, or
    /// a restart skips the buffered bytes.
    pub fn get_checkpoint_position(&self) -> FilePosition {
        self.file_position.saturating_sub(self.buf.len() as u64)
    }

    /// Returns `true` exactly once per watcher, on the first call, if the watcher reads its
    /// file from offset 0. Watchers resumed from a nonzero checkpoint, seeked to a nonempty
    /// file's end, or skipped never do.
    pub fn take_read_started(&mut self) -> bool {
        std::mem::take(&mut self.read_started_pending)
    }

    /// Lines returned by `read_line` over the lifetime of this watcher.
    pub fn lines_read(&self) -> u64 {
        self.lines_read
    }

    /// Line bytes returned by `read_line` over the lifetime of this watcher.
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// The file's mtime and size as of watcher creation or the last discovery pass that
    /// found it (see `refresh_last_seen_stat`). Never does I/O.
    pub fn last_seen_stat(&self) -> FileStat {
        self.last_seen_stat
    }

    /// Records the stat a discovery pass just looked up for this watcher's current path.
    /// `None` (no lookup this pass, e.g. a cached archive fingerprint) keeps the old value.
    pub fn refresh_last_seen_stat(&mut self, stat: Option<FileStat>) {
        if let Some(stat) = stat {
            self.last_seen_stat = stat;
        }
    }

    /// A lower bound on the age of this file's oldest unread byte at `now_ms`: how long ago the
    /// file was last written if it has unread bytes (see `UnreadCheck`), and zero otherwise.
    /// `None` when this watcher skips the file or the stat it needs is missing.
    ///
    /// The newest write happened at the mtime, so every unread byte is at least that old. A file
    /// that is still being written therefore reads close to zero however far behind it is; its
    /// backlog only ages once writes stop, for example after an hourly file has rotated and been
    /// compressed.
    pub fn unread_age(&self, now_ms: i64) -> Option<Duration> {
        let mtime_ms = self.last_seen_stat.mtime_ms?;
        let unread = match self.unread_check? {
            UnreadCheck::Size => self.last_seen_stat.size_bytes? > self.file_position,
            UnreadCheck::Eof => !self.reached_eof,
        };
        if !unread {
            return Some(Duration::ZERO);
        }
        let age_ms = u64::try_from(now_ms.saturating_sub(mtime_ms)).unwrap_or(0);
        Some(Duration::from_millis(age_ms))
    }

    /// Read a single line from the underlying file
    ///
    /// This function will attempt to read a new line from its file, blocking,
    /// up to some maximum but unspecified amount of time. `read_line` will open
    /// a new file handler as needed, transparently to the caller.
    pub(super) async fn read_line(&mut self) -> io::Result<RawLineResult> {
        let result = self.read_raw_line().await;
        if let Ok(RawLineResult {
            raw_line: Some(line),
            ..
        }) = &result
        {
            self.lines_read += 1;
            self.bytes_read += line.bytes.len() as u64;
        }
        result
    }

    async fn read_raw_line(&mut self) -> io::Result<RawLineResult> {
        self.track_read_attempt();

        let reader = &mut self.reader;
        let file_position = &mut self.file_position;
        let initial_position = self.buffered_line_start.unwrap_or(*file_position);
        match read_until_with_max_size(
            reader.as_mut(),
            file_position,
            self.line_delimiter.as_ref(),
            &mut self.buf,
            self.max_line_bytes,
        )
        .await
        {
            Ok(ReadResult {
                successfully_read: Some(_),
                discarded_for_size_and_truncated,
            }) => {
                self.buffered_line_start = None;
                self.track_read_success();
                Ok(RawLineResult {
                    raw_line: Some(RawLine {
                        offset: initial_position,
                        bytes: self.buf.split().freeze(),
                    }),
                    discarded_for_size_and_truncated,
                })
            }
            Ok(ReadResult {
                successfully_read: None,
                discarded_for_size_and_truncated,
            }) => {
                if !self.file_findable() {
                    // Death is decided by the run-loop (reaps archives now, keeps
                    // non-archives for `rotate_wait`), not here.
                    // File has been deleted, so return what we have in the buffer, even though it
                    // didn't end with a newline. This is not a perfect signal for when we should
                    // give up waiting for a newline, but it's decent.
                    let buf = self.buf.split().freeze();
                    self.buffered_line_start = None;
                    if buf.is_empty() {
                        // EOF
                        self.reached_eof = true;
                        Ok(RawLineResult {
                            raw_line: None,
                            discarded_for_size_and_truncated,
                        })
                    } else {
                        Ok(RawLineResult {
                            raw_line: Some(RawLine {
                                offset: initial_position,
                                bytes: buf,
                            }),
                            discarded_for_size_and_truncated,
                        })
                    }
                } else {
                    self.buffered_line_start = (!self.buf.is_empty()).then_some(initial_position);
                    self.track_read_eof();
                    Ok(RawLineResult {
                        raw_line: None,
                        discarded_for_size_and_truncated,
                    })
                }
            }
            Err(e) => {
                if let io::ErrorKind::NotFound = e.kind() {
                    self.set_dead();
                }
                Err(e)
            }
        }
    }

    #[inline]
    fn track_read_attempt(&mut self) {
        self.last_read_attempt = Instant::now();
    }

    #[inline]
    fn track_read_success(&mut self) {
        self.reached_eof = false;
        self.read_retry_delay = EOF_READ_BACKOFF_MIN;
        self.last_read_success = Instant::now();
    }

    #[inline]
    fn track_read_eof(&mut self) {
        self.read_retry_delay = if self.reached_eof {
            std::cmp::min(
                self.read_retry_delay.saturating_mul(2),
                EOF_READ_BACKOFF_MAX,
            )
        } else {
            EOF_READ_BACKOFF_MIN
        };
        self.reached_eof = true;
    }

    #[inline]
    pub fn last_read_success(&self) -> Instant {
        self.last_read_success
    }

    #[inline]
    pub fn should_read(&self) -> bool {
        if self.reached_eof && self.last_read_attempt.elapsed() < self.read_retry_delay {
            return false;
        }

        self.last_read_success.elapsed() < self.idle_read_interval
            || self.last_read_attempt.elapsed() > self.idle_read_interval
    }

    #[inline]
    pub fn last_seen(&self) -> Instant {
        self.last_seen
    }

    #[inline]
    pub fn reached_eof(&self) -> bool {
        self.reached_eof
    }
}

async fn is_gzipped(r: &mut BufReader<File>) -> io::Result<bool> {
    let header_bytes = r.fill_buf().await?;
    // WARN: The paired `BufReader::consume` is not called intentionally. If we
    // do we'll chop a decent part of the potential gzip stream off.
    Ok(header_bytes.starts_with(GZIP_MAGIC))
}

fn null_reader() -> impl AsyncBufRead {
    io::Cursor::new(Vec::new())
}

/// Build a boxed gzip reader that decompresses and skips to the given offset.
async fn gzip_reader_at_offset(
    reader: BufReader<File>,
    offset: u64,
) -> io::Result<Box<dyn AsyncBufRead + Send + Sync + Unpin>> {
    let mut gzip_reader = BufReader::new(GzipDecoder::new(reader));
    if offset > 0 {
        skip_first_n_bytes(&mut gzip_reader, offset as usize).await?;
    }
    Ok(Box::new(gzip_reader))
}

/// Skip the first `n` bytes from an async buffered reader by consuming
/// chunks until `n` bytes have been discarded.
async fn skip_first_n_bytes<R: AsyncBufRead + Unpin>(reader: &mut R, n: usize) -> io::Result<()> {
    let mut skipped = 0;
    while skipped < n {
        let chunk = reader.fill_buf().await?;
        if chunk.is_empty() {
            break;
        }
        let to_skip = std::cmp::min(chunk.len(), n - skipped);
        reader.consume(to_skip);
        skipped += to_skip;
    }
    Ok(())
}
