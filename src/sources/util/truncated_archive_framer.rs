use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use bytes::{Bytes, BytesMut};
use tokio_util::codec::Decoder;
use vector_lib::codecs::decoding::{BoxedFramingError, Framer};

/// Prevents a synthetic EOF from turning an unconfirmed archive tail into a record.
pub(crate) struct TruncatedArchiveFramer {
    inner: Framer,
    truncated_tail: Arc<AtomicBool>,
    dropped_tail_bytes: Option<Arc<AtomicU64>>,
}

impl TruncatedArchiveFramer {
    pub(crate) const fn new(inner: Framer, truncated_tail: Arc<AtomicBool>) -> Self {
        Self {
            inner,
            truncated_tail,
            dropped_tail_bytes: None,
        }
    }

    pub(crate) const fn with_dropped_tail_bytes(
        inner: Framer,
        truncated_tail: Arc<AtomicBool>,
        dropped_tail_bytes: Arc<AtomicU64>,
    ) -> Self {
        Self {
            inner,
            truncated_tail,
            dropped_tail_bytes: Some(dropped_tail_bytes),
        }
    }
}

impl Decoder for TruncatedArchiveFramer {
    type Item = Bytes;
    type Error = BoxedFramingError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode(src)
    }

    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if self.truncated_tail.load(Ordering::Relaxed)
            && matches!(
                self.inner,
                Framer::NewlineDelimited(_) | Framer::CharacterDelimited(_)
            )
        {
            // Drain delimiter-terminated frames before discarding the unconfirmed tail. The tail
            // may still parse successfully as a shorter base64 or protobuf record.
            let frame = self.inner.decode(src)?;
            if frame.is_none() {
                if let Some(dropped_tail_bytes) = &self.dropped_tail_bytes {
                    dropped_tail_bytes.fetch_add(src.len() as u64, Ordering::Relaxed);
                }
                src.clear();
            }
            Ok(frame)
        } else {
            self.inner.decode_eof(src)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    };

    use bytes::BytesMut;
    use tokio_util::codec::Decoder;
    use vector_lib::codecs::{
        NewlineDelimitedDecoderConfig,
        decoding::{FramingConfig, NewlineDelimitedDecoderOptions},
    };

    use super::TruncatedArchiveFramer;

    #[test]
    fn counts_dropped_unframed_tail_bytes() {
        let truncated_tail = Arc::new(AtomicBool::new(true));
        let dropped_tail_bytes = Arc::new(AtomicU64::new(0));
        let framer = FramingConfig::NewlineDelimited(NewlineDelimitedDecoderConfig {
            newline_delimited: NewlineDelimitedDecoderOptions { max_length: None },
        })
        .build();
        let mut framer = TruncatedArchiveFramer::with_dropped_tail_bytes(
            framer,
            truncated_tail,
            Arc::clone(&dropped_tail_bytes),
        );
        let mut source = BytesMut::from(&b"complete\npartial"[..]);

        assert!(framer.decode_eof(&mut source).unwrap().is_some());
        assert!(framer.decode_eof(&mut source).unwrap().is_none());
        assert_eq!(dropped_tail_bytes.load(Ordering::Relaxed), 7);
    }
}
