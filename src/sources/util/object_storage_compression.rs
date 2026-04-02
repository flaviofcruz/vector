/// Shared compression types and detection/decoding logic for object storage sources
/// (`aws_s3`, `azure_blob`, `gcp_gcs`).
use vector_lib::configurable::configurable_component;

/// Compression scheme for objects retrieved from cloud object storage.
#[configurable_component]
#[configurable(metadata(docs::advanced))]
#[derive(Clone, Copy, Debug, Derivative, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[derivative(Default)]
pub enum Compression {
    /// Automatically attempt to determine the compression scheme.
    ///
    /// The compression scheme of the object is determined from its `Content-Encoding` and
    /// `Content-Type` metadata, as well as the key suffix (for example, `.gz`).
    ///
    /// It is set to `none` if the compression scheme cannot be determined.
    #[derivative(Default)]
    Auto,

    /// Uncompressed.
    None,

    /// GZIP.
    Gzip,

    /// ZSTD.
    Zstd,
}

impl Compression {
    /// Detect compression from object metadata.
    ///
    /// Checks in the following priority order:
    /// 1. `content_encoding` (e.g. `"gzip"`)
    /// 2. `content_type` (e.g. `"application/gzip"`)
    /// 3. File extension of `key` (e.g. `.gz`, `.zst`)
    ///
    /// Returns `None` if the compression scheme cannot be determined.
    pub fn detect(
        content_encoding: Option<&str>,
        content_type: Option<&str>,
        key: &str,
    ) -> Option<Self> {
        content_encoding
            .and_then(Self::from_content_encoding)
            .or_else(|| content_type.and_then(Self::from_content_type))
            .or_else(|| Self::from_key(key))
    }

    /// If `self` is [`Compression::Auto`], detect from metadata and fall back to
    /// [`Compression::None`] (uncompressed) if nothing matches. Otherwise return `self` unchanged.
    pub fn resolve(
        self,
        content_encoding: Option<&str>,
        content_type: Option<&str>,
        key: &str,
    ) -> Self {
        match self {
            Compression::Auto => {
                Self::detect(content_encoding, content_type, key).unwrap_or(Compression::None)
            }
            other => other,
        }
    }

    /// Wrap a byte stream in the appropriate decompressor for this compression scheme.
    ///
    /// - An empty stream returns an empty `AsyncRead`.
    /// - Gzip and Zstd streams support multiple concatenated members.
    /// - [`Compression::Auto`] panics: call [`resolve`][Self::resolve] first.
    ///
    /// The caller is responsible for converting cloud-provider-specific stream error types
    /// to `std::io::Error` before calling this method (typically via `.map_err(io::Error::other)`).
    #[cfg(any(
        feature = "sources-aws_s3",
        feature = "sources-azure_blob",
        feature = "sources-gcp_gcs"
    ))]
    pub async fn build_decoder<S>(self, mut body: S) -> Box<dyn tokio::io::AsyncRead + Send + Unpin>
    where
        S: futures::Stream<Item = std::io::Result<bytes::Bytes>> + Send + Unpin + 'static,
    {
        use async_compression::tokio::bufread;
        use futures::StreamExt;
        use tokio_util::io::StreamReader;

        // Peek at the first chunk to detect an empty body before building the reader.
        let first = match body.next().await {
            Some(chunk) => chunk,
            None => return Box::new(tokio::io::empty()),
        };

        let r = tokio::io::BufReader::new(StreamReader::new(
            futures::stream::iter(Some(first)).chain(body),
        ));

        match self {
            Compression::None => Box::new(r),
            Compression::Gzip => Box::new({
                let mut decoder = bufread::GzipDecoder::new(r);
                decoder.multiple_members(true);
                decoder
            }),
            Compression::Zstd => Box::new({
                let mut decoder = bufread::ZstdDecoder::new(r);
                decoder.multiple_members(true);
                decoder
            }),
            Compression::Auto => {
                unreachable!("call Compression::resolve() before build_decoder()")
            }
        }
    }

    fn from_content_encoding(encoding: &str) -> Option<Self> {
        match encoding {
            "gzip" => Some(Compression::Gzip),
            "zstd" => Some(Compression::Zstd),
            _ => None,
        }
    }

    fn from_content_type(content_type: &str) -> Option<Self> {
        match content_type {
            "application/gzip" | "application/x-gzip" => Some(Compression::Gzip),
            "application/zstd" => Some(Compression::Zstd),
            _ => None,
        }
    }

    fn from_key(key: &str) -> Option<Self> {
        let extension = std::path::Path::new(key)
            .extension()
            .and_then(std::ffi::OsStr::to_str);

        extension.and_then(|ext| match ext {
            "gz" => Some(Compression::Gzip),
            "zst" => Some(Compression::Zstd),
            _ => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------------
    // Detection tests
    // -------------------------------------------------------------------------

    #[test]
    fn detect_content_encoding_wins() {
        assert_eq!(
            Compression::detect(Some("gzip"), Some("application/zstd"), "file.zst"),
            Some(Compression::Gzip),
        );
        assert_eq!(
            Compression::detect(Some("zstd"), Some("application/gzip"), "file.gz"),
            Some(Compression::Zstd),
        );
    }

    #[test]
    fn detect_content_type_wins_over_extension() {
        assert_eq!(
            Compression::detect(None, Some("application/zstd"), "file.gz"),
            Some(Compression::Zstd),
        );
        assert_eq!(
            Compression::detect(None, Some("application/gzip"), "file.zst"),
            Some(Compression::Gzip),
        );
    }

    #[test]
    fn detect_from_content_encoding() {
        assert_eq!(
            Compression::detect(Some("gzip"), None, "out.log"),
            Some(Compression::Gzip)
        );
        assert_eq!(
            Compression::detect(Some("zstd"), None, "out.log"),
            Some(Compression::Zstd)
        );
        // Unknown encoding values produce no compression
        assert_eq!(Compression::detect(Some("br"), None, "out.log"), None);
        assert_eq!(Compression::detect(Some("unknown"), None, "out.log"), None);
        assert_eq!(Compression::detect(Some("identity"), None, "out.log"), None);
    }

    #[test]
    fn detect_from_content_type() {
        assert_eq!(
            Compression::detect(None, Some("application/gzip"), "out.log"),
            Some(Compression::Gzip)
        );
        assert_eq!(
            Compression::detect(None, Some("application/x-gzip"), "out.log"),
            Some(Compression::Gzip)
        );
        assert_eq!(
            Compression::detect(None, Some("application/zstd"), "out.log"),
            Some(Compression::Zstd)
        );
        assert_eq!(
            Compression::detect(None, Some("text/plain"), "out.log"),
            None
        );
    }

    #[test]
    fn detect_from_extension() {
        assert_eq!(
            Compression::detect(None, None, "out.log.gz"),
            Some(Compression::Gzip)
        );
        assert_eq!(
            Compression::detect(None, None, "out.log.zst"),
            Some(Compression::Zstd)
        );
        assert_eq!(Compression::detect(None, None, "out.txt"), None);
        assert_eq!(Compression::detect(None, None, "out.log"), None);
    }

    #[test]
    fn resolve_auto_falls_back_to_none() {
        assert_eq!(
            Compression::Auto.resolve(None, None, "out.log"),
            Compression::None
        );
    }

    #[test]
    fn resolve_auto_detects() {
        assert_eq!(
            Compression::Auto.resolve(Some("gzip"), None, "out.log"),
            Compression::Gzip,
        );
    }

    #[test]
    fn resolve_passthrough_for_explicit_compression() {
        assert_eq!(
            Compression::Gzip.resolve(Some("zstd"), Some("application/zstd"), "file.zst"),
            Compression::Gzip,
        );
        assert_eq!(
            Compression::None.resolve(Some("gzip"), None, "file.gz"),
            Compression::None,
        );
    }

    // -------------------------------------------------------------------------
    // Decoder tests — require async-compression via one of the source features
    // -------------------------------------------------------------------------

    #[cfg(any(
        feature = "sources-aws_s3",
        feature = "sources-azure_blob",
        feature = "sources-gcp_gcs"
    ))]
    mod decoder {
        use tokio::io::AsyncReadExt;

        use super::*;

        fn stream_from_bytes(
            data: Vec<u8>,
        ) -> impl futures::Stream<Item = std::io::Result<bytes::Bytes>> + Send + Unpin + 'static
        {
            futures::stream::once(futures::future::ready(Ok(bytes::Bytes::from(data))))
        }

        fn empty_stream()
        -> impl futures::Stream<Item = std::io::Result<bytes::Bytes>> + Send + Unpin + 'static
        {
            futures::stream::empty()
        }

        #[tokio::test]
        async fn decode_empty_body_returns_empty() {
            for compression in [Compression::None, Compression::Gzip, Compression::Zstd] {
                let mut data = Vec::new();
                compression
                    .build_decoder(empty_stream())
                    .await
                    .read_to_end(&mut data)
                    .await
                    .unwrap();
                assert!(
                    data.is_empty(),
                    "{compression:?}: empty body should produce empty output"
                );
            }
        }

        #[tokio::test]
        async fn decode_uncompressed() {
            let payload = b"hello world\n".to_vec();
            let mut out = Vec::new();
            Compression::None
                .build_decoder(stream_from_bytes(payload.clone()))
                .await
                .read_to_end(&mut out)
                .await
                .unwrap();
            assert_eq!(out, payload);
        }

        #[tokio::test]
        async fn decode_gzip() {
            use std::io::Write;
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            encoder.write_all(b"hello gzip\n").unwrap();
            let compressed = encoder.finish().unwrap();

            let mut out = Vec::new();
            Compression::Gzip
                .build_decoder(stream_from_bytes(compressed))
                .await
                .read_to_end(&mut out)
                .await
                .unwrap();
            assert_eq!(out, b"hello gzip\n");
        }

        #[tokio::test]
        async fn decode_zstd() {
            let compressed = zstd::encode_all(b"hello zstd\n".as_ref(), 0).unwrap();

            let mut out = Vec::new();
            Compression::Zstd
                .build_decoder(stream_from_bytes(compressed))
                .await
                .read_to_end(&mut out)
                .await
                .unwrap();
            assert_eq!(out, b"hello zstd\n");
        }

        #[tokio::test]
        async fn resolve_auto_then_decode_gzip() {
            use std::io::Write;
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            encoder.write_all(b"auto gzip\n").unwrap();
            let compressed = encoder.finish().unwrap();

            let mut out = Vec::new();
            Compression::Auto
                .resolve(Some("gzip"), None, "file.log")
                .build_decoder(stream_from_bytes(compressed))
                .await
                .read_to_end(&mut out)
                .await
                .unwrap();
            assert_eq!(out, b"auto gzip\n");
        }
    }
}
