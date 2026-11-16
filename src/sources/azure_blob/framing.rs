pub(super) use crate::sources::util::TruncatedArchiveFramer;

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use async_compression::tokio::bufread::GzipDecoder;
    use bytes::{Bytes, BytesMut};
    use flate2::{Compression, write::GzEncoder};
    use futures::StreamExt;
    use tokio::io::{AsyncRead, BufReader};
    use tokio_util::codec::{Decoder, FramedRead};
    use vector_lib::codecs::decoding::{BoxedFramingError, Framer};
    use vector_lib::codecs::{
        BytesDecoder, CharacterDelimitedDecoder, LengthDelimitedDecoder, NewlineDelimitedDecoder,
    };

    use super::*;
    use crate::sources::util::TolerateTrailingEof;

    fn newline_framer() -> Framer {
        Framer::NewlineDelimited(NewlineDelimitedDecoder::new())
    }

    async fn read_gzip(
        data: &[u8],
        finish: bool,
        tolerate: bool,
        framer: Framer,
    ) -> (Vec<Bytes>, bool, Option<BoxedFramingError>) {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data).unwrap();
        let compressed = if finish {
            encoder.finish().unwrap()
        } else {
            // Commit the decoded prefix without the final deflate block or gzip footer.
            encoder.flush().unwrap();
            encoder.get_ref().clone()
        };
        read_compressed(&compressed, tolerate, framer).await
    }

    async fn read_compressed(
        compressed: &[u8],
        tolerate: bool,
        framer: Framer,
    ) -> (Vec<Bytes>, bool, Option<BoxedFramingError>) {
        let mut reader = GzipDecoder::new(BufReader::new(compressed));
        reader.multiple_members(true);
        let truncated_tail = Arc::new(AtomicBool::new(false));
        let reader: Box<dyn AsyncRead + Unpin + Send + '_> = if tolerate {
            Box::new(TolerateTrailingEof::new(
                reader,
                Arc::clone(&truncated_tail),
            ))
        } else {
            Box::new(reader)
        };
        let mut stream = FramedRead::new(
            reader,
            TruncatedArchiveFramer::new(framer, Arc::clone(&truncated_tail)),
        );
        let mut frames = Vec::new();
        let mut error = None;
        while let Some(result) = stream.next().await {
            match result {
                Ok(frame) => frames.push(frame),
                Err(err) => {
                    error = Some(err);
                    break;
                }
            }
        }
        (frames, truncated_tail.load(Ordering::Relaxed), error)
    }

    #[tokio::test]
    async fn truncated_archive_discards_unterminated_base64_tail() {
        // CgFh is itself valid base64/protobuf, but it could be a torn prefix of CgFhEAI=.
        for tail in ["G", "CgFh"] {
            let data = format!("Zmlyc3Q=\n{tail}");
            let (frames, truncated, error) =
                read_gzip(data.as_bytes(), false, true, newline_framer()).await;
            assert!(truncated);
            assert!(error.is_none());
            assert_eq!(frames, vec![Bytes::from_static(b"Zmlyc3Q=")]);
        }
    }

    #[tokio::test]
    async fn compressed_cut_does_not_emit_a_valid_but_shorter_protobuf() {
        // First 24 bytes of gzip("Zmlyc3Q=\nCgFhEAI=\n"). The decoded prefix ends in
        // CgFh: valid base64 for protobuf 0a0161, missing field 2 from 0a01611002.
        let compressed = b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\x03\x8b\xca\xcd\xa9\x4c\x36\x0e\xb4\xe5\x72\x4e\x77\xcb\x70";
        let (frames, truncated, error) = read_compressed(compressed, true, newline_framer()).await;
        assert!(truncated);
        assert!(error.is_none());
        assert_eq!(frames, vec![Bytes::from_static(b"Zmlyc3Q=")]);
    }

    #[tokio::test]
    async fn unfinished_member_preserves_all_terminated_lines() {
        let (frames, truncated, error) =
            read_gzip(b"first\r\nsecond\n", false, true, newline_framer()).await;
        assert!(truncated);
        assert!(error.is_none());
        assert_eq!(frames, vec![Bytes::from("first\r"), Bytes::from("second")]);
    }

    #[tokio::test]
    async fn missing_footer_preserves_terminated_lines_but_discards_unterminated_tail() {
        for tail in ["", "tail"] {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder
                .write_all(format!("first\nsecond\n{tail}").as_bytes())
                .unwrap();
            let mut compressed = encoder.finish().unwrap();
            compressed.truncate(compressed.len() - 8);
            let (frames, truncated, error) =
                read_compressed(&compressed, true, newline_framer()).await;
            assert!(truncated);
            assert!(error.is_none());
            assert_eq!(frames, vec![Bytes::from("first"), Bytes::from("second")]);
        }
    }

    #[tokio::test]
    async fn clean_archive_preserves_unterminated_final_line() {
        let (frames, truncated, error) =
            read_gzip(b"first\nsecond", true, true, newline_framer()).await;
        assert!(!truncated);
        assert!(error.is_none());
        assert_eq!(frames, vec![Bytes::from("first"), Bytes::from("second")]);
    }

    #[tokio::test]
    async fn disabled_tolerance_preserves_archive_error() {
        let (_, truncated, error) = read_gzip(b"first\ntail", false, false, newline_framer()).await;
        assert!(!truncated);
        assert!(error.is_some());
    }

    #[tokio::test]
    async fn character_delimiter_discards_only_unterminated_tail() {
        let framer = Framer::CharacterDelimited(CharacterDelimitedDecoder::new(b';'));
        let (frames, truncated, error) = read_gzip(b"first;second;tail", false, true, framer).await;
        assert!(truncated);
        assert!(error.is_none());
        assert_eq!(frames, vec![Bytes::from("first"), Bytes::from("second")]);
    }

    #[tokio::test]
    async fn bytes_framing_preserves_decoded_prefix() {
        let framer = Framer::Bytes(BytesDecoder::new());
        let (frames, truncated, error) = read_gzip(b"all decoded bytes", false, true, framer).await;
        assert!(truncated);
        assert!(error.is_none());
        assert_eq!(frames, vec![Bytes::from("all decoded bytes")]);
    }

    #[tokio::test]
    async fn length_framing_preserves_partial_frame_error() {
        let framer = Framer::LengthDelimited(LengthDelimitedDecoder::default());
        let (frames, truncated, error) = read_gzip(
            b"\x00\x00\x00\x03one\x00\x00\x00\x03tw",
            false,
            true,
            framer,
        )
        .await;
        assert!(truncated);
        assert!(error.is_some());
        assert_eq!(frames, vec![Bytes::from("one")]);
    }

    #[test]
    fn synthetic_eof_drains_buffered_complete_frames_before_clearing_tail() {
        let truncated = Arc::new(AtomicBool::new(true));
        let mut framer = TruncatedArchiveFramer::new(newline_framer(), truncated);
        let mut src = BytesMut::from("first\nsecond\ntail");
        assert_eq!(framer.decode_eof(&mut src).unwrap(), Some("first".into()));
        assert_eq!(framer.decode_eof(&mut src).unwrap(), Some("second".into()));
        assert_eq!(framer.decode_eof(&mut src).unwrap(), None);
        assert!(src.is_empty());
    }
}
