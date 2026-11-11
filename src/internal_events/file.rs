#![allow(dead_code)] // TODO requires optional feature compilation

use std::borrow::Cow;

use metrics::{counter, gauge};
use vector_lib::{
    NamedInternalEvent,
    configurable::configurable_component,
    internal_event::{
        ComponentEventsDropped, InternalEvent, UNINTENTIONAL, error_stage, error_type,
    },
};

#[cfg(any(feature = "sources-file", feature = "sources-kubernetes_logs"))]
pub use self::source::*;

/// Configuration of internal metrics for file-based components.
#[configurable_component]
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct FileInternalMetricsConfig {
    /// Whether or not to include the "file" tag on the component's corresponding internal metrics.
    ///
    /// This is useful for distinguishing between different files while monitoring. However, the tag's
    /// cardinality is unbounded.
    #[serde(default = "crate::serde::default_false")]
    pub include_file_tag: bool,
}

#[derive(Debug, NamedInternalEvent)]
pub struct FileOpen {
    pub count: usize,
}

impl InternalEvent for FileOpen {
    fn emit(self) {
        gauge!("open_files").set(self.count as f64);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct FileBytesSent<'a> {
    pub byte_size: usize,
    pub file: Cow<'a, str>,
    pub include_file_metric_tag: bool,
}

impl InternalEvent for FileBytesSent<'_> {
    fn emit(self) {
        trace!(
            message = "Bytes sent.",
            byte_size = %self.byte_size,
            protocol = "file",
            file = %self.file,
        );
        if self.include_file_metric_tag {
            counter!(
                "component_sent_bytes_total",
                "protocol" => "file",
                "file" => self.file.clone().into_owned(),
            )
        } else {
            counter!(
                "component_sent_bytes_total",
                "protocol" => "file",
            )
        }
        .increment(self.byte_size as u64);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct FileIoError<'a, P> {
    pub error: std::io::Error,
    pub code: &'static str,
    pub message: &'static str,
    pub path: &'a P,
    pub dropped_events: usize,
}

impl<P: std::fmt::Debug> InternalEvent for FileIoError<'_, P> {
    fn emit(self) {
        error!(
            message = %self.message,
            path = ?self.path,
            error = %self.error,
            error_code = %self.code,
            error_type = error_type::IO_FAILED,
            stage = error_stage::SENDING,
        );
        counter!(
            "component_errors_total",
            "error_code" => self.code,
            "error_type" => error_type::IO_FAILED,
            "stage" => error_stage::SENDING,
        )
        .increment(1);

        if self.dropped_events > 0 {
            emit!(ComponentEventsDropped::<UNINTENTIONAL> {
                count: self.dropped_events,
                reason: self.message,
            });
        }
    }
}

#[cfg(any(feature = "sources-file", feature = "sources-kubernetes_logs"))]
mod source {
    use std::{io::Error, path::Path, time::Duration};

    use bytes::BytesMut;
    use metrics::{counter, gauge};
    use vector_lib::{
        NamedInternalEvent, emit,
        file_source_common::internal_events::{
            FileReadStartedInfo, FileSourceInternalEvents, FileUnwatchedInfo,
        },
        internal_event::{ComponentEventsDropped, INTENTIONAL, error_stage, error_type},
        json_size::JsonSize,
    };

    use super::{FileOpen, InternalEvent};

    #[derive(Debug, NamedInternalEvent)]
    pub struct FileBytesReceived<'a> {
        pub byte_size: usize,
        pub file: &'a str,
        pub include_file_metric_tag: bool,
    }

    impl InternalEvent for FileBytesReceived<'_> {
        fn emit(self) {
            trace!(
                message = "Bytes received.",
                byte_size = %self.byte_size,
                protocol = "file",
                file = %self.file,
            );
            if self.include_file_metric_tag {
                counter!(
                    "component_received_bytes_total",
                    "protocol" => "file",
                    "file" => self.file.to_owned()
                )
            } else {
                counter!(
                    "component_received_bytes_total",
                    "protocol" => "file",
                )
            }
            .increment(self.byte_size as u64);
        }
    }

    #[derive(Debug, NamedInternalEvent)]
    pub struct FileEventsReceived<'a> {
        pub count: usize,
        pub file: &'a str,
        pub byte_size: JsonSize,
        pub include_file_metric_tag: bool,
    }

    impl InternalEvent for FileEventsReceived<'_> {
        fn emit(self) {
            trace!(
                message = "Events received.",
                count = %self.count,
                byte_size = %self.byte_size,
                file = %self.file
            );
            if self.include_file_metric_tag {
                counter!(
                    "component_received_events_total",
                    "file" => self.file.to_owned(),
                )
                .increment(self.count as u64);
                counter!(
                    "component_received_event_bytes_total",
                    "file" => self.file.to_owned(),
                )
                .increment(self.byte_size.get() as u64);
            } else {
                counter!("component_received_events_total").increment(self.count as u64);
                counter!("component_received_event_bytes_total")
                    .increment(self.byte_size.get() as u64);
            }
        }
    }

    #[derive(Debug, NamedInternalEvent)]
    pub struct FileChecksumFailed<'a> {
        pub file: &'a Path,
        pub include_file_metric_tag: bool,
    }

    impl InternalEvent for FileChecksumFailed<'_> {
        fn emit(self) {
            warn!(
                message = "Currently ignoring file too small to fingerprint.",
                file = %self.file.display(),
                internal_log_rate_limit = false,
            );
            if self.include_file_metric_tag {
                counter!(
                    "checksum_errors_total",
                    "file" => self.file.to_string_lossy().into_owned(),
                )
            } else {
                counter!("checksum_errors_total")
            }
            .increment(1);
        }
    }

    #[derive(Debug, NamedInternalEvent)]
    pub struct FileFingerprintReadError<'a> {
        pub file: &'a Path,
        pub error: Error,
        pub include_file_metric_tag: bool,
    }

    impl InternalEvent for FileFingerprintReadError<'_> {
        fn emit(self) {
            error!(
                message = "Failed reading file for fingerprinting.",
                file = %self.file.display(),
                error = %self.error,
                error_code = "reading_fingerprint",
                error_type = error_type::READER_FAILED,
                stage = error_stage::RECEIVING,
            );
            if self.include_file_metric_tag {
                counter!(
                    "component_errors_total",
                    "error_code" => "reading_fingerprint",
                    "error_type" => error_type::READER_FAILED,
                    "stage" => error_stage::RECEIVING,
                    "file" => self.file.to_string_lossy().into_owned(),
                )
            } else {
                counter!(
                    "component_errors_total",
                    "error_code" => "reading_fingerprint",
                    "error_type" => error_type::READER_FAILED,
                    "stage" => error_stage::RECEIVING,
                )
            }
            .increment(1);
        }
    }

    const DELETION_FAILED: &str = "deletion_failed";

    #[derive(Debug, NamedInternalEvent)]
    pub struct FileDeleteError<'a> {
        pub file: &'a Path,
        pub error: Error,
        pub include_file_metric_tag: bool,
    }

    impl InternalEvent for FileDeleteError<'_> {
        fn emit(self) {
            error!(
                message = "Failed in deleting file.",
                file = %self.file.display(),
                error = %self.error,
                error_code = DELETION_FAILED,
                error_type = error_type::COMMAND_FAILED,
                stage = error_stage::RECEIVING,
            );
            if self.include_file_metric_tag {
                counter!(
                    "component_errors_total",
                    "file" => self.file.to_string_lossy().into_owned(),
                    "error_code" => DELETION_FAILED,
                    "error_type" => error_type::COMMAND_FAILED,
                    "stage" => error_stage::RECEIVING,
                )
            } else {
                counter!(
                    "component_errors_total",
                    "error_code" => DELETION_FAILED,
                    "error_type" => error_type::COMMAND_FAILED,
                    "stage" => error_stage::RECEIVING,
                )
            }
            .increment(1);
        }
    }

    #[derive(Debug, NamedInternalEvent)]
    pub struct FileDeleted<'a> {
        pub file: &'a Path,
        pub include_file_metric_tag: bool,
    }

    impl InternalEvent for FileDeleted<'_> {
        fn emit(self) {
            info!(
                message = "File deleted.",
                file = %self.file.display(),
                internal_log_rate_limit = false,
                vector_event_type = "VECTOR_FILE_EVENT",
                vector_file_event_type = "LOGGING_AGENT_FILE_DELETED"
            );
            if self.include_file_metric_tag {
                counter!(
                    "files_deleted_total",
                    "file" => self.file.to_string_lossy().into_owned(),
                )
            } else {
                counter!("files_deleted_total")
            }
            .increment(1);
        }
    }

    #[derive(Debug, NamedInternalEvent)]
    pub struct FileReadStarted<'a> {
        pub file: &'a Path,
        pub info: &'a FileReadStartedInfo,
        pub include_file_metric_tag: bool,
    }

    impl InternalEvent for FileReadStarted<'_> {
        fn emit(self) {
            info!(
                message = "Started reading file from the beginning.",
                file = %self.file.display(),
                fingerprint = %self.info.fingerprint,
                num_lines_read = self.info.lines_read,
                num_bytes_read = self.info.bytes_read,
                file_mtime_ms = self.info.stat.mtime_ms,
                file_size_bytes = self.info.stat.size_bytes,
                internal_log_rate_limit = false,
                vector_event_type = "VECTOR_FILE_EVENT",
                vector_file_event_type = "LOGGING_AGENT_FILE_READ_STARTED"
            );
            if self.include_file_metric_tag {
                counter!(
                    "files_read_started_total",
                    "file" => self.file.to_string_lossy().into_owned(),
                )
            } else {
                counter!("files_read_started_total")
            }
            .increment(1);
        }
    }

    #[derive(Debug, NamedInternalEvent)]
    pub struct FileUnwatched<'a> {
        pub file: &'a Path,
        pub info: &'a FileUnwatchedInfo,
        pub include_file_metric_tag: bool,
    }

    impl InternalEvent for FileUnwatched<'_> {
        fn emit(self) {
            let reached_eof = if self.info.reached_eof {
                "true"
            } else {
                "false"
            };
            info!(
                message = "Stopped watching file.",
                file = %self.file.display(),
                reached_eof,
                fingerprint = %self.info.fingerprint,
                stop_reason = self.info.reason.as_str(),
                end_offset = self.info.end_offset,
                num_lines_read = self.info.lines_read,
                num_bytes_read = self.info.bytes_read,
                file_mtime_ms = self.info.stat.mtime_ms,
                file_size_bytes = self.info.stat.size_bytes,
                internal_log_rate_limit = false,
                vector_event_type = "VECTOR_FILE_EVENT",
                vector_file_event_type = "LOGGING_AGENT_FILE_STOPPED"
            );
            if self.include_file_metric_tag {
                counter!(
                    "files_unwatched_total",
                    "file" => self.file.to_string_lossy().into_owned(),
                    "reached_eof" => reached_eof,
                )
            } else {
                counter!(
                    "files_unwatched_total",
                    "reached_eof" => reached_eof,
                )
            }
            .increment(1);

            // Same as `files_unwatched_total`, but kept under `delivery_events_total`
            // to centralize delivery observability.
            vector_lib::internal_event::delivery_singleton()
                .emit_file_unwatched(self.info.reached_eof);
        }
    }

    #[derive(Debug, NamedInternalEvent)]
    struct FileWatchError<'a> {
        pub file: &'a Path,
        pub error: Error,
        pub include_file_metric_tag: bool,
    }

    impl InternalEvent for FileWatchError<'_> {
        fn emit(self) {
            error!(
                message = "Failed to watch file.",
                error = %self.error,
                error_code = "watching",
                error_type = error_type::COMMAND_FAILED,
                stage = error_stage::RECEIVING,
                file = %self.file.display(),
            );
            if self.include_file_metric_tag {
                counter!(
                    "component_errors_total",
                    "error_code" => "watching",
                    "error_type" => error_type::COMMAND_FAILED,
                    "stage" => error_stage::RECEIVING,
                    "file" => self.file.to_string_lossy().into_owned(),
                )
            } else {
                counter!(
                    "component_errors_total",
                    "error_code" => "watching",
                    "error_type" => error_type::COMMAND_FAILED,
                    "stage" => error_stage::RECEIVING,
                )
            }
            .increment(1);
        }
    }

    #[derive(Debug, NamedInternalEvent)]
    pub struct FileResumed<'a> {
        pub file: &'a Path,
        pub file_position: u64,
        pub include_file_metric_tag: bool,
    }

    impl InternalEvent for FileResumed<'_> {
        fn emit(self) {
            info!(
                message = "Resuming to watch file.",
                file = %self.file.display(),
                file_position = %self.file_position,
                internal_log_rate_limit = false,
            );
            if self.include_file_metric_tag {
                counter!(
                    "files_resumed_total",
                    "file" => self.file.to_string_lossy().into_owned(),
                )
            } else {
                counter!("files_resumed_total")
            }
            .increment(1);
        }
    }

    #[derive(Debug, NamedInternalEvent)]
    pub struct FileAdded<'a> {
        pub file: &'a Path,
        pub include_file_metric_tag: bool,
    }

    impl InternalEvent for FileAdded<'_> {
        fn emit(self) {
            // Not a Vector event log (VEL) file event: LOGGING_AGENT_FILE_READ_STARTED
            // follows within milliseconds for nearly every new file and carries the same
            // fields plus the fingerprint and stat, so emitting both doubled VEL file-event
            // volume.
            info!(
                message = "Found new file to watch.",
                file = %self.file.display(),
                internal_log_rate_limit = false,
            );
            if self.include_file_metric_tag {
                counter!(
                    "files_added_total",
                    "file" => self.file.to_string_lossy().into_owned(),
                )
            } else {
                counter!("files_added_total")
            }
            .increment(1);
        }
    }

    #[derive(Debug, NamedInternalEvent)]
    pub struct FileCheckpointed {
        pub count: usize,
        pub duration: Duration,
    }

    impl InternalEvent for FileCheckpointed {
        fn emit(self) {
            debug!(
                message = "Files checkpointed.",
                count = %self.count,
                duration_ms = self.duration.as_millis() as u64,
            );
            counter!("checkpoints_total").increment(self.count as u64);
        }
    }

    /// `file_backlog_behind{over}` thresholds: a source is behind by more than one when its
    /// largest unread-byte age is.
    pub const BACKLOG_BEHIND_THRESHOLDS: [(&str, Duration); 2] = [
        ("900s", Duration::from_secs(900)),
        ("3600s", Duration::from_secs(3600)),
    ];

    /// For each of `BACKLOG_BEHIND_THRESHOLDS`, 1 if `age` is past it and 0 otherwise.
    pub fn backlog_behind(age: Duration) -> [(&'static str, f64); 2] {
        BACKLOG_BEHIND_THRESHOLDS
            .map(|(label, threshold)| (label, if age > threshold { 1.0 } else { 0.0 }))
    }

    #[derive(Debug, NamedInternalEvent)]
    pub struct FileBacklogMaxAge {
        pub age: Duration,
    }

    impl InternalEvent for FileBacklogMaxAge {
        fn emit(self) {
            gauge!("file_backlog_max_age_seconds").set(self.age.as_secs_f64());
            // Always set, including 0, so `avg` across sources is the fraction behind and `sum`
            // the number behind.
            for (over, behind) in backlog_behind(self.age) {
                gauge!("file_backlog_behind", "over" => over).set(behind);
            }
        }
    }

    #[derive(Debug, NamedInternalEvent)]
    pub struct FileDiscoveryPassDelayed;

    impl InternalEvent for FileDiscoveryPassDelayed {
        fn emit(self) {
            counter!("file_discovery_passes_delayed_total").increment(1);
        }
    }

    #[derive(Debug, NamedInternalEvent)]
    pub struct FileCheckpointWriteError {
        pub error: Error,
    }

    impl InternalEvent for FileCheckpointWriteError {
        fn emit(self) {
            error!(
                message = "Failed writing checkpoints.",
                error = %self.error,
                error_code = "writing_checkpoints",
                error_type = error_type::WRITER_FAILED,
                stage = error_stage::RECEIVING,
            );
            counter!(
                "component_errors_total",
                "error_code" => "writing_checkpoints",
                "error_type" => error_type::WRITER_FAILED,
                "stage" => error_stage::RECEIVING,
            )
            .increment(1);
        }
    }

    #[derive(Debug, NamedInternalEvent)]
    pub struct PathGlobbingError<'a> {
        pub path: &'a Path,
        pub error: &'a Error,
    }

    impl InternalEvent for PathGlobbingError<'_> {
        fn emit(self) {
            error!(
                message = "Failed to glob path.",
                error = %self.error,
                error_code = "globbing",
                error_type = error_type::READER_FAILED,
                stage = error_stage::RECEIVING,
                path = %self.path.display(),
            );
            counter!(
                "component_errors_total",
                "error_code" => "globbing",
                "error_type" => error_type::READER_FAILED,
                "stage" => error_stage::RECEIVING,
            )
            .increment(1);
        }
    }

    #[derive(Debug, NamedInternalEvent)]
    pub struct FileLineTooBigError<'a> {
        pub truncated_bytes: &'a BytesMut,
        pub configured_limit: usize,
        pub encountered_size_so_far: usize,
    }

    impl InternalEvent for FileLineTooBigError<'_> {
        fn emit(self) {
            error!(
                message = "Found line that exceeds max_line_bytes; discarding.",
                truncated_bytes = ?self.truncated_bytes,
                configured_limit = self.configured_limit,
                encountered_size_so_far = self.encountered_size_so_far,
                error_type = error_type::CONDITION_FAILED,
                stage = error_stage::RECEIVING,
            );
            counter!(
                "component_errors_total",
                "error_code" => "reading_line_from_file",
                "error_type" => error_type::CONDITION_FAILED,
                "stage" => error_stage::RECEIVING,
            )
            .increment(1);
            emit!(ComponentEventsDropped::<INTENTIONAL> {
                count: 1,
                reason: "Found line that exceeds max_line_bytes; discarding.",
            });
        }
    }

    #[derive(Clone)]
    pub struct FileSourceInternalEventsEmitter {
        pub include_file_metric_tag: bool,
    }

    impl FileSourceInternalEvents for FileSourceInternalEventsEmitter {
        fn emit_file_added(&self, file: &Path) {
            emit!(FileAdded {
                file,
                include_file_metric_tag: self.include_file_metric_tag
            });
        }

        fn emit_file_resumed(&self, file: &Path, file_position: u64) {
            emit!(FileResumed {
                file,
                file_position,
                include_file_metric_tag: self.include_file_metric_tag
            });
        }

        fn emit_file_watch_error(&self, file: &Path, error: Error) {
            emit!(FileWatchError {
                file,
                error,
                include_file_metric_tag: self.include_file_metric_tag
            });
        }

        fn emit_file_read_started(&self, file: &Path, info: &FileReadStartedInfo) {
            emit!(FileReadStarted {
                file,
                info,
                include_file_metric_tag: self.include_file_metric_tag
            });
        }

        fn emit_file_unwatched(&self, file: &Path, info: &FileUnwatchedInfo) {
            emit!(FileUnwatched {
                file,
                info,
                include_file_metric_tag: self.include_file_metric_tag
            });
        }

        fn emit_file_deleted(&self, file: &Path) {
            emit!(FileDeleted {
                file,
                include_file_metric_tag: self.include_file_metric_tag
            });
        }

        fn emit_file_delete_error(&self, file: &Path, error: Error) {
            emit!(FileDeleteError {
                file,
                error,
                include_file_metric_tag: self.include_file_metric_tag
            });
        }

        fn emit_file_fingerprint_read_error(&self, file: &Path, error: Error) {
            emit!(FileFingerprintReadError {
                file,
                error,
                include_file_metric_tag: self.include_file_metric_tag
            });
        }

        fn emit_file_checksum_failed(&self, file: &Path) {
            emit!(FileChecksumFailed {
                file,
                include_file_metric_tag: self.include_file_metric_tag
            });
        }

        fn emit_file_checkpointed(&self, count: usize, duration: Duration) {
            emit!(FileCheckpointed { count, duration });
        }

        fn emit_file_checkpoint_write_error(&self, error: Error) {
            emit!(FileCheckpointWriteError { error });
        }

        fn emit_files_open(&self, count: usize) {
            emit!(FileOpen { count });
        }

        fn emit_path_globbing_failed(&self, path: &Path, error: &Error) {
            emit!(PathGlobbingError { path, error });
        }

        fn emit_discovery_pass_delayed(&self) {
            emit!(FileDiscoveryPassDelayed);
        }

        fn discovery_pass_timestamp(&self) -> Box<dyn Fn(f64) + Send + Sync> {
            let gauge = gauge!("file_discovery_last_pass_timestamp_seconds");
            Box::new(move |timestamp| gauge.set(timestamp))
        }

        fn emit_backlog_max_age(&self, age: Duration) {
            emit!(FileBacklogMaxAge { age });
        }

        fn emit_file_line_too_long(
            &self,
            truncated_bytes: &bytes::BytesMut,
            configured_limit: usize,
            encountered_size_so_far: usize,
        ) {
            emit!(FileLineTooBigError {
                truncated_bytes,
                configured_limit,
                encountered_size_so_far
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        path::Path,
        sync::{Arc, Mutex},
    };

    use tracing::{
        Subscriber,
        field::{Field, Visit},
    };
    use tracing_subscriber::{Layer, layer::Context, prelude::*};
    use vector_lib::file_source_common::{
        FileFingerprint, FileReadStartedInfo, FileSourceInternalEvents, FileStat, FileStopReason,
        FileUnwatchedInfo,
    };

    use super::FileSourceInternalEventsEmitter;

    type Fields = BTreeMap<String, String>;

    /// Captures the fields of every event that carries a `vector_file_event_type`.
    #[derive(Clone, Default)]
    struct FileEventCapture(Arc<Mutex<Vec<Fields>>>);

    struct FieldsVisitor(Fields);

    impl Visit for FieldsVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0.insert(field.name().to_owned(), format!("{value:?}"));
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.insert(field.name().to_owned(), value.to_owned());
        }
    }

    impl<S: Subscriber> Layer<S> for FileEventCapture {
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            let mut visitor = FieldsVisitor(Fields::new());
            event.record(&mut visitor);
            if visitor.0.contains_key("vector_file_event_type") {
                self.0.lock().unwrap().push(visitor.0);
            }
        }
    }

    fn capture_all(emit: impl FnOnce(&FileSourceInternalEventsEmitter)) -> Vec<Fields> {
        let capture = FileEventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        tracing::subscriber::with_default(subscriber, || {
            emit(&FileSourceInternalEventsEmitter {
                include_file_metric_tag: false,
            })
        });
        capture.0.lock().unwrap().clone()
    }

    fn capture(emit: impl FnOnce(&FileSourceInternalEventsEmitter)) -> Fields {
        let mut events = capture_all(emit);
        assert_eq!(events.len(), 1, "{events:?}");
        events.pop().unwrap()
    }

    fn field<'a>(fields: &'a Fields, name: &str) -> Option<&'a str> {
        fields.get(name).map(String::as_str)
    }

    // Field names match `VectorFileEvent` in universe's `vector_event_log.proto`.
    #[test]
    fn read_started_logs_proto_fields() {
        let info = FileReadStartedInfo {
            fingerprint: FileFingerprint::FirstLinesChecksum(42),
            lines_read: 2,
            bytes_read: 21,
            stat: FileStat {
                mtime_ms: Some(1_700_000_000_000),
                size_bytes: Some(23),
            },
        };
        let fields = capture(|e| e.emit_file_read_started(Path::new("/var/log/app.log"), &info));

        assert_eq!(
            field(&fields, "vector_file_event_type"),
            Some("LOGGING_AGENT_FILE_READ_STARTED")
        );
        assert_eq!(
            field(&fields, "vector_event_type"),
            Some("VECTOR_FILE_EVENT")
        );
        assert_eq!(field(&fields, "file"), Some("/var/log/app.log"));
        assert_eq!(
            field(&fields, "fingerprint"),
            Some("first_lines_checksum:42")
        );
        assert_eq!(field(&fields, "num_lines_read"), Some("2"));
        assert_eq!(field(&fields, "num_bytes_read"), Some("21"));
        assert_eq!(field(&fields, "file_mtime_ms"), Some("1700000000000"));
        assert_eq!(field(&fields, "file_size_bytes"), Some("23"));
    }

    #[test]
    fn added_is_not_a_vel_file_event() {
        let events = capture_all(|e| e.emit_file_added(Path::new("/var/log/app.log")));
        assert!(events.is_empty(), "{events:?}");
    }

    #[test]
    fn unwatched_logs_proto_fields_and_omits_missing_stat() {
        let info = FileUnwatchedInfo {
            fingerprint: FileFingerprint::DevInode(1, 2),
            reason: FileStopReason::FileGone,
            reached_eof: false,
            end_offset: 100,
            lines_read: 3,
            bytes_read: 90,
            stat: FileStat::default(),
        };
        let fields = capture(|e| e.emit_file_unwatched(Path::new("/var/log/app.log"), &info));

        assert_eq!(
            field(&fields, "vector_file_event_type"),
            Some("LOGGING_AGENT_FILE_STOPPED")
        );
        assert_eq!(field(&fields, "fingerprint"), Some("dev_inode:1:2"));
        assert_eq!(
            field(&fields, "stop_reason"),
            Some("VECTOR_FILE_STOP_REASON_FILE_GONE")
        );
        assert_eq!(field(&fields, "reached_eof"), Some("false"));
        assert_eq!(field(&fields, "end_offset"), Some("100"));
        assert_eq!(field(&fields, "num_lines_read"), Some("3"));
        assert_eq!(field(&fields, "num_bytes_read"), Some("90"));
        assert_eq!(field(&fields, "file_mtime_ms"), None);
        assert_eq!(field(&fields, "file_size_bytes"), None);
    }

    #[test]
    fn backlog_behind_flags_each_threshold_strictly_exceeded() {
        use std::time::Duration;

        use super::backlog_behind;

        let flags = |secs| backlog_behind(Duration::from_secs(secs));
        assert_eq!(flags(0), [("900s", 0.0), ("3600s", 0.0)]);
        assert_eq!(flags(900), [("900s", 0.0), ("3600s", 0.0)]);
        assert_eq!(flags(901), [("900s", 1.0), ("3600s", 0.0)]);
        assert_eq!(flags(3600), [("900s", 1.0), ("3600s", 0.0)]);
        assert_eq!(flags(3601), [("900s", 1.0), ("3600s", 1.0)]);
    }
}
