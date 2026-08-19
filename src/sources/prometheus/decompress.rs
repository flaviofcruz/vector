//! Shared gzip handling for the Prometheus scrape sources — some targets (e.g.
//! the classic-DBR driver) gzip-encode `/metrics`, so both sources inflate here.

use std::{borrow::Cow, io::Read as _};

use flate2::read::MultiGzDecoder;

const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];

/// Inflate `body` when gzip-encoded (via `Content-Encoding` or the leading gzip
/// magic bytes, so targets that gzip without advertising it still work);
/// returns it unchanged otherwise.
pub(crate) fn maybe_gunzip<'a>(
    content_encoding: Option<&[u8]>,
    body: &'a [u8],
) -> std::io::Result<Cow<'a, [u8]>> {
    let is_gzip = content_encoding.is_some_and(|v| v.eq_ignore_ascii_case(b"gzip"))
        || body.starts_with(&GZIP_MAGIC);

    if is_gzip {
        let mut out = Vec::new();
        MultiGzDecoder::new(body).read_to_end(&mut out)?;
        Ok(Cow::Owned(out))
    } else {
        Ok(Cow::Borrowed(body))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut encoder =
            flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn passes_plaintext_through_borrowed() {
        let body = b"# HELP foo bar\nfoo 1\n";
        let out = maybe_gunzip(None, body).unwrap();
        assert!(matches!(out, Cow::Borrowed(_)), "plaintext should not be copied");
        assert_eq!(out.as_ref(), body);
    }

    #[test]
    fn decodes_via_content_encoding_header() {
        let plain = b"# TYPE foo counter\nfoo 7\n";
        let compressed = gzip(plain);
        let out = maybe_gunzip(Some(b"gzip"), &compressed).unwrap();
        assert_eq!(out.as_ref(), plain);
    }

    #[test]
    fn decodes_via_magic_bytes_without_header() {
        // Server gzips unconditionally and sends no Content-Encoding.
        let plain = b"# TYPE foo counter\nfoo 7\n";
        let compressed = gzip(plain);
        let out = maybe_gunzip(None, &compressed).unwrap();
        assert_eq!(out.as_ref(), plain);
    }

    #[test]
    fn content_encoding_match_is_case_insensitive() {
        let plain = b"foo 1\n";
        let compressed = gzip(plain);
        assert_eq!(maybe_gunzip(Some(b"GZIP"), &compressed).unwrap().as_ref(), plain);
    }
}
