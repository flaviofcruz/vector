use bytes::BytesMut;
use futures::Stream;
use std::{
    io,
    pin::Pin,
    sync::Arc,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll},
};
use tokio_util::codec::{Decoder, FramedRead};

use pin_project::pin_project;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pub trait AfterReadExt {
    fn after_read<F>(self, after_read: F) -> AfterRead<Self, F>
    where
        Self: Sized;
}

impl<T: AsyncRead + AsyncWrite> AfterReadExt for T {
    fn after_read<F>(self, after_read: F) -> AfterRead<Self, F> {
        AfterRead::new(self, after_read)
    }
}

/// This wraps the inner socket and emits `BytesReceived` with the
/// actual number of bytes read before handling framing.
#[pin_project]
pub struct AfterRead<T, F> {
    #[pin]
    inner: T,
    after_read: F,
}

impl<T, F> AfterRead<T, F> {
    pub const fn new(inner: T, after_read: F) -> Self {
        Self { inner, after_read }
    }

    #[cfg(feature = "listenfd")]
    pub const fn get_ref(&self) -> &T {
        &self.inner
    }

    #[cfg(all(unix, feature = "sources-utils-net-unix"))]
    pub const fn get_mut_ref(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T: AsyncRead, F> AsyncRead for AfterRead<T, F>
where
    F: Fn(usize),
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<tokio::io::Result<()>> {
        let before = buf.filled().len();
        let this = self.project();
        let result = this.inner.poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = result {
            (this.after_read)(buf.filled().len() - before);
        }
        result
    }
}

impl<T: AsyncWrite, F> AsyncWrite for AfterRead<T, F> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        self.project().inner.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.project().inner.poll_shutdown(cx)
    }
}

pub enum DecoderError<E> {
    IO(io::Error),
    Other(E),
}

impl<E> DecoderError<E>
where
    E: From<io::Error>,
{
    fn into_inner(self) -> E {
        match self {
            DecoderError::IO(e) => E::from(e),
            DecoderError::Other(e) => e,
        }
    }
}

impl<E> From<io::Error> for DecoderError<E> {
    fn from(e: io::Error) -> Self {
        DecoderError::IO(e)
    }
}

pub struct LenientFramedReadDecoder<D> {
    inner: D,
}

impl<D> LenientFramedReadDecoder<D>
where
    D: Decoder,
{
    pub const fn new(inner: D) -> Self {
        Self { inner }
    }
}

impl<D> Decoder for LenientFramedReadDecoder<D>
where
    D: Decoder,
    D::Error: From<io::Error>,
{
    type Item = D::Item;
    type Error = DecoderError<D::Error>;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode(src).map_err(DecoderError::Other)
    }

    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode_eof(src).map_err(DecoderError::Other)
    }
}

/// A wrapper around an `FramedRead` that silently ignores `ConnectionReset`
/// errors if the frame buffer is empty.
#[pin_project]
pub struct LenientFramedRead<T, D> {
    #[pin]
    inner: FramedRead<T, LenientFramedReadDecoder<D>>,
}

impl<T, D> LenientFramedRead<T, D>
where
    T: AsyncRead,
    D: Decoder,
{
    /// Creates a new `LenientFramedRead` with the given `decoder`.
    pub fn new(inner: T, decoder: D) -> Self {
        Self {
            inner: FramedRead::new(inner, LenientFramedReadDecoder::new(decoder)),
        }
    }

    /// Returns a reference to the underlying I/O stream wrapped by
    /// `FramedRead`.
    ///
    /// Note that care should be taken to not tamper with the underlying stream
    /// of data coming in as it may corrupt the stream of frames otherwise
    /// being worked with.
    pub fn get_ref(&self) -> &T {
        self.inner.get_ref()
    }

    /// Returns a mutable reference to the underlying I/O stream wrapped by
    /// `FramedRead`.
    ///
    /// Note that care should be taken to not tamper with the underlying stream
    /// of data coming in as it may corrupt the stream of frames otherwise
    /// being worked with.
    pub fn get_mut(&mut self) -> &mut T {
        self.inner.get_mut()
    }
}

impl<T, D> Stream for LenientFramedRead<T, D>
where
    T: AsyncRead + Unpin,
    D: Decoder,
{
    type Item = Result<D::Item, D::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match this.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Err(DecoderError::IO(e))))
                if e.kind() == io::ErrorKind::ConnectionReset =>
            {
                let buffer = this.inner.read_buffer();

                if buffer.is_empty() {
                    // Clean RST - no partial data, treat as EOF
                    Poll::Ready(None)
                } else {
                    // Partial frame in buffer
                    Poll::Ready(Some(Err(D::Error::from(e))))
                }
            }
            other => other.map_err(|e| e.into_inner()),
        }
    }
}

/// Wraps a reader so a trailing `UnexpectedEof` becomes a clean end-of-stream.
///
/// A compressed stream (gzip/zstd) that ends part-way through a member makes the decoder emit
/// `io::ErrorKind::UnexpectedEof` once its committed bytes are exhausted — i.e. the archive is
/// truncated. For an immutable object read from a queue-backed source, every redelivery reads
/// identical bytes and fails identically, so leaving the read to error means the message is never
/// acked and redelivers forever (re-ingesting the decodable prefix as duplicates each time).
/// Converting the trailing EOF to a clean end lets the source ingest the records that did decode
/// and ack the message. Cloud-agnostic: used by the object-storage sources (azure_blob, and the
/// aws_s3 / gcp_gcs follow-ups).
///
/// Guarded by `produced_output`: the tolerance only applies once the reader has yielded some
/// bytes, so a body that fails from the very first byte (empty / non-decodable, likelier a
/// transient fetch problem) still errors and redelivers. Callers should map genuine transport
/// failures to a kind other than `UnexpectedEof` so those still propagate as retriable.
///
/// `tolerated` is set to `true` iff a trailing `UnexpectedEof` (after output) was converted to a
/// clean EOF, so a caller that has since moved the reader into a `FramedRead` can still observe
/// whether truncation was tolerated. It does NOT by itself justify acking: a caller that wants to
/// distinguish a truncated *archive* from a short *transfer* must additionally verify it read the
/// whole object (e.g. bytes pulled == `Content-Length`).
///
/// `done` fuses the wrapper: once it has emitted the synthetic EOF it never re-polls the inner
/// reader (whose behavior after returning an error is not contractually defined).
pub struct TolerateTrailingEof<R> {
    inner: R,
    produced_output: bool,
    done: bool,
    tolerated: Arc<AtomicBool>,
}

impl<R> TolerateTrailingEof<R> {
    /// Wraps `inner`, setting `tolerated` to `true` if a trailing `UnexpectedEof` after some
    /// output is converted to a clean end-of-stream.
    pub fn new(inner: R, tolerated: Arc<AtomicBool>) -> Self {
        Self {
            inner,
            produced_output: false,
            done: false,
            tolerated,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for TolerateTrailingEof<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.done {
            // Fused: never re-poll a reader that already errored.
            return Poll::Ready(Ok(()));
        }
        let before = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        // Count bytes decoded in THIS poll as output even when the same poll also errors:
        // async-compression can fill a member's bytes and then hit the truncated next member in a
        // single call, so checking output only on the Ok branch would miss it (and forwarding an
        // Err with bytes filled violates the AsyncRead contract read_to_end asserts).
        if buf.filled().len() > before {
            self.produced_output = true;
        }
        match result {
            Poll::Ready(Err(e))
                if e.kind() == io::ErrorKind::UnexpectedEof && self.produced_output =>
            {
                self.tolerated.store(true, Ordering::Relaxed);
                self.done = true;
                // Deliver any bytes this poll already filled; the fused next poll reports EOF.
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt as _;

    /// Mock reader that yields a fixed byte prefix, then optionally one error, then clean EOF.
    /// Exercises `TolerateTrailingEof` without building real (de)compression state.
    struct MockRead {
        data: Vec<u8>,
        pos: usize,
        error: Option<io::ErrorKind>,
    }

    impl AsyncRead for MockRead {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if self.pos < self.data.len() {
                let n = std::cmp::min(buf.remaining(), self.data.len() - self.pos);
                let start = self.pos;
                buf.put_slice(&self.data[start..start + n]);
                self.pos += n;
                return Poll::Ready(Ok(()));
            }
            match self.error.take() {
                Some(kind) => Poll::Ready(Err(io::Error::new(kind, "mock"))),
                None => Poll::Ready(Ok(())),
            }
        }
    }

    async fn tolerate(data: &[u8], error: Option<io::ErrorKind>) -> (io::Result<Vec<u8>>, bool) {
        let tolerated = Arc::new(AtomicBool::new(false));
        let mut reader = TolerateTrailingEof::new(
            MockRead {
                data: data.to_vec(),
                pos: 0,
                error,
            },
            Arc::clone(&tolerated),
        );
        let mut out = Vec::new();
        let result = reader.read_to_end(&mut out).await.map(|_| out);
        (result, tolerated.load(Ordering::Relaxed))
    }

    #[tokio::test]
    async fn trailing_unexpected_eof_after_output_is_tolerated() {
        // A truncated gzip member surfaces as UnexpectedEof after some decoded output: ingest the
        // decodable prefix and end cleanly so the queue message can be acked.
        let (result, tolerated) =
            tolerate(b"line1\nline2\n", Some(io::ErrorKind::UnexpectedEof)).await;
        assert_eq!(result.unwrap(), b"line1\nline2\n");
        assert!(tolerated);
    }

    #[tokio::test]
    async fn unexpected_eof_with_no_output_is_not_tolerated() {
        // Nothing decoded before the EOF (empty / non-gzip body, likelier a transient fetch
        // problem): keep the error so the message redelivers instead of acking an unread blob.
        let (result, tolerated) = tolerate(b"", Some(io::ErrorKind::UnexpectedEof)).await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
        assert!(!tolerated);
    }

    #[tokio::test]
    async fn non_eof_error_is_not_tolerated() {
        // A genuine mid-stream transport failure is mapped to ErrorKind::Other, not UnexpectedEof,
        // and must still propagate (retriable) even though output was produced first.
        let (result, tolerated) = tolerate(b"abc", Some(io::ErrorKind::Other)).await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Other);
        assert!(!tolerated);
    }
}
