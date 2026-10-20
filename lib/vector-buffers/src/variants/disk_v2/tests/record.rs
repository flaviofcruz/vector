use std::{
    cmp,
    io::Cursor,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use tokio::io::{AsyncRead, ReadBuf};

use super::{create_buffer_v2_with_reader_progress_recheck_interval, create_standalone_ledger};
use crate::{
    test::{SizedRecord, with_temp_dir},
    variants::disk_v2::{reader::RecordReader, writer::RecordWriter},
};

/// A file-like reader whose contents can grow without waking the reader.
struct GrowingReader {
    bytes: Arc<Mutex<Vec<u8>>>,
    position: usize,
    reached_eof: Arc<AtomicBool>,
}

impl AsyncRead for GrowingReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let available = {
            let bytes = self.bytes.lock().expect("growing reader lock poisoned");
            if self.position == bytes.len() {
                self.reached_eof.store(true, Ordering::Release);
                return Poll::Ready(Ok(()));
            }

            let available = cmp::min(buf.remaining(), bytes.len() - self.position);
            buf.put_slice(&bytes[self.position..self.position + available]);
            available
        };
        self.position += available;
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn roundtrip_through_record_writer_and_record_reader() {
    // Create a duplex stream that's more than big enough to ship a record through.
    let (writer_io, reader_io) = tokio::io::duplex(4096);

    let mut record_writer = RecordWriter::new(writer_io, 0, 16_384, u64::MAX, 2048);
    let mut record_reader = RecordReader::new(reader_io);
    let ledger = create_standalone_ledger().await;

    let record = SizedRecord::new(73);

    let (bytes_written, flush_result) = record_writer
        .write_record(314, record.clone())
        .await
        .expect("write should not fail");
    record_writer.flush().await.expect("flush should not fail");

    let read_token = record_reader
        .try_next_record(true, &ledger)
        .await
        .expect("read should not fail");
    assert!(read_token.is_some());

    let read_token = read_token.unwrap();
    assert_eq!(bytes_written, read_token.record_bytes());
    assert_eq!(flush_result, None);
    assert_eq!(314, read_token.record_id());

    let roundtrip_record = record_reader
        .read_record(read_token)
        .expect("read should not fail");
    assert_eq!(record, roundtrip_record);
}

#[tokio::test]
async fn record_reader_always_returns_none_when_no_data() {
    let reader_io = Cursor::new(Vec::new());

    let mut record_reader = RecordReader::<_, SizedRecord>::new(reader_io);
    let ledger = create_standalone_ledger().await;
    let read_token = record_reader
        .try_next_record(true, &ledger)
        .await
        .expect("read should not fail");
    assert!(read_token.is_none());
}

#[tokio::test]
async fn partial_record_retries_after_watchdog_timeout_without_notification() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            let (_writer, _reader, ledger) =
                create_buffer_v2_with_reader_progress_recheck_interval::<_, SizedRecord>(
                    data_dir,
                    Some(Duration::from_millis(25)),
                )
                .await;

            let expected = SizedRecord::new(73);
            let mut record_writer =
                RecordWriter::new(Cursor::new(Vec::new()), 0, 16_384, u64::MAX, 2048);
            record_writer
                .write_record(1, expected.clone())
                .await
                .expect("write should not fail");
            record_writer.flush().await.expect("flush should not fail");
            let encoded = record_writer.get_ref().get_ref().clone();

            // Expose the complete length delimiter and only part of the payload. Appending the
            // remainder below deliberately provides no writer notification.
            let split = 8 + (encoded.len() - 8) / 2;
            let bytes = Arc::new(Mutex::new(encoded[..split].to_vec()));
            let reached_eof = Arc::new(AtomicBool::new(false));
            let mut record_reader: RecordReader<_, SizedRecord> =
                RecordReader::new(GrowingReader {
                    bytes: Arc::clone(&bytes),
                    position: 0,
                    reached_eof: Arc::clone(&reached_eof),
                });
            let read_ledger = Arc::clone(&ledger);
            let read_task = tokio::spawn(async move {
                let result = record_reader.try_next_record(true, &read_ledger).await;
                (record_reader, result)
            });

            while !reached_eof.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
            bytes
                .lock()
                .expect("growing reader lock poisoned")
                .extend_from_slice(&encoded[split..]);

            let (mut record_reader, result) =
                tokio::time::timeout(Duration::from_secs(2), read_task)
                    .await
                    .expect("reader did not retry the partial record after the watchdog timeout")
                    .expect("reader task should not panic");
            let token = result
                .expect("read should not fail")
                .expect("record should be complete after the retry");
            let actual = record_reader
                .read_record(token)
                .expect("record should decode");
            assert_eq!(actual, expected);
        }
    })
    .await;
}
