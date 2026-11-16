use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use bytes::{Bytes, BytesMut};
use tokio_util::codec::Decoder;
use vector_lib::codecs::decoding::{BoxedFramingError, Framer};

/// Prevents a synthetic EOF from turning an unconfirmed archive tail into a record.
pub(crate) struct TruncatedArchiveFramer {
    inner: Framer,
    truncated_tail: Arc<AtomicBool>,
}

impl TruncatedArchiveFramer {
    pub(crate) const fn new(inner: Framer, truncated_tail: Arc<AtomicBool>) -> Self {
        Self {
            inner,
            truncated_tail,
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
                src.clear();
            }
            Ok(frame)
        } else {
            self.inner.decode_eof(src)
        }
    }
}
