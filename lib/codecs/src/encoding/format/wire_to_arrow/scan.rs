//! Low-level proto wire-format readers used for the packed-repeated-scalar
//! inner loop.
//!
//! The outer message scan uses `zeroparser::wire::try_parse_field`, but
//! that yields one `WireField` at a time (tag + value). Packed repeated
//! scalars live inside a single `WireValue::Len(inner_bytes)` blob — the
//! inner bytes are a sequence of raw scalar values with no tags. These
//! helpers read those raw values.
//!
//! Also holds the error mapping from `zeroparser::ParseError` into this
//! crate's [`WireToArrowError`](super::errors::WireToArrowError).

use zeroparser::ParseError;

use super::errors::{Result, WireToArrowError};

/// Read a single varint and advance `pos`. Caps at 10 bytes per proto spec.
#[inline]
pub(crate) fn decode_varint(bytes: &[u8], pos: &mut usize) -> Result<u64> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    for _ in 0..10 {
        if *pos >= bytes.len() {
            return Err(WireToArrowError::UnexpectedEof);
        }
        let b = bytes[*pos];
        *pos += 1;
        value |= u64::from(b & 0x7f) << shift;
        if b < 0x80 {
            return Ok(value);
        }
        shift += 7;
    }
    Err(WireToArrowError::VarintOverflow)
}

/// Read 8 little-endian bytes and advance `pos`.
#[inline]
pub(crate) fn read_fixed64(bytes: &[u8], pos: &mut usize) -> Result<u64> {
    if *pos + 8 > bytes.len() {
        return Err(WireToArrowError::UnexpectedEof);
    }
    let v = u64::from_le_bytes(bytes[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Ok(v)
}

/// Read 4 little-endian bytes and advance `pos`.
#[inline]
pub(crate) fn read_fixed32(bytes: &[u8], pos: &mut usize) -> Result<u32> {
    if *pos + 4 > bytes.len() {
        return Err(WireToArrowError::UnexpectedEof);
    }
    let v = u32::from_le_bytes(bytes[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Ok(v)
}

/// Map a `zeroparser::ParseError` into this crate's error type.
///
/// `zeroparser` emits a richer set of parse errors than we currently
/// distinguish; we collapse most of them onto the pre-existing variants.
pub(crate) fn map_parse_error(err: ParseError) -> WireToArrowError {
    match err {
        ParseError::TruncatedVarint | ParseError::BufferTooShort { .. } => {
            WireToArrowError::UnexpectedEof
        }
        ParseError::VarintTooLong => WireToArrowError::VarintOverflow,
        ParseError::InvalidWireType(wt) => WireToArrowError::InvalidWireType { wire_type: wt },
        ParseError::InvalidUtf8 { .. } => WireToArrowError::InvalidUtf8,
        _ => WireToArrowError::ProtoParser { source: err },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_varint(mut value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        while value >= 0x80 {
            out.push((value as u8) | 0x80);
            value >>= 7;
        }
        out.push(value as u8);
        out
    }

    #[test]
    fn varint_roundtrip_small() {
        for v in [0u64, 1, 127, 128, 255, 256, 16383, 16384, u32::MAX as u64, u64::MAX] {
            let encoded = encode_varint(v);
            let mut pos = 0;
            let decoded = decode_varint(&encoded, &mut pos).unwrap();
            assert_eq!(v, decoded, "mismatch on {v}");
            assert_eq!(pos, encoded.len(), "position not advanced");
        }
    }

    #[test]
    fn varint_eof() {
        let bytes = &[0x80u8];
        let mut pos = 0;
        assert!(matches!(
            decode_varint(bytes, &mut pos),
            Err(WireToArrowError::UnexpectedEof)
        ));
    }

    #[test]
    fn varint_overflow_detected() {
        let bytes = [0xffu8; 11];
        let mut pos = 0;
        assert!(matches!(
            decode_varint(&bytes, &mut pos),
            Err(WireToArrowError::VarintOverflow)
        ));
    }

    #[test]
    fn fixed32_roundtrip() {
        let bytes = 0x12345678u32.to_le_bytes();
        let mut pos = 0;
        assert_eq!(read_fixed32(&bytes, &mut pos).unwrap(), 0x12345678u32);
        assert_eq!(pos, 4);
    }

    #[test]
    fn fixed64_roundtrip() {
        let v: u64 = 0x0011_2233_4455_6677;
        let bytes = v.to_le_bytes();
        let mut pos = 0;
        assert_eq!(read_fixed64(&bytes, &mut pos).unwrap(), v);
        assert_eq!(pos, 8);
    }

    #[test]
    fn map_parse_error_truncated_varint() {
        let mapped = map_parse_error(ParseError::TruncatedVarint);
        assert!(matches!(mapped, WireToArrowError::UnexpectedEof));
    }

    #[test]
    fn map_parse_error_invalid_wire_type() {
        let mapped = map_parse_error(ParseError::InvalidWireType(7));
        assert!(matches!(
            mapped,
            WireToArrowError::InvalidWireType { wire_type: 7 }
        ));
    }
}
