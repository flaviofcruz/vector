//! Low-level proto wire-format scanning primitives.
//!
//! All readers advance a `&mut usize` position pointer into a borrowed byte
//! slice. They never allocate.

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

/// Skip a field of the given wire type (for unknown fields).
pub(crate) fn skip_field(wire_type: u8, bytes: &[u8], pos: &mut usize) -> Result<()> {
    match wire_type {
        0 => {
            decode_varint(bytes, pos)?;
            Ok(())
        }
        1 => {
            read_fixed64(bytes, pos)?;
            Ok(())
        }
        2 => {
            let len = decode_varint(bytes, pos)? as usize;
            if *pos + len > bytes.len() {
                return Err(WireToArrowError::UnexpectedEof);
            }
            *pos += len;
            Ok(())
        }
        5 => {
            read_fixed32(bytes, pos)?;
            Ok(())
        }
        other => Err(WireToArrowError::InvalidWireType { wire_type: other }),
    }
}

/// Decode proto zigzag-encoded signed 32-bit integer.
#[inline]
pub(crate) fn zigzag32(v: u32) -> i32 {
    ((v >> 1) as i32) ^ -((v & 1) as i32)
}

/// Decode proto zigzag-encoded signed 64-bit integer.
#[inline]
pub(crate) fn zigzag64(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
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
        let bytes = &[0x80u8]; // continuation bit set, no follow-up byte
        let mut pos = 0;
        assert!(matches!(
            decode_varint(bytes, &mut pos),
            Err(WireToArrowError::UnexpectedEof)
        ));
    }

    #[test]
    fn varint_overflow_detected() {
        let bytes = [0xffu8; 11]; // 11 bytes all with continuation bit
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
    fn fixed32_eof() {
        let bytes = [0u8; 3];
        let mut pos = 0;
        assert!(matches!(
            read_fixed32(&bytes, &mut pos),
            Err(WireToArrowError::UnexpectedEof)
        ));
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
    fn zigzag_roundtrip() {
        for v in [0i32, 1, -1, 2, -2, i32::MIN, i32::MAX] {
            let encoded = ((v << 1) ^ (v >> 31)) as u32;
            assert_eq!(zigzag32(encoded), v, "zigzag32 mismatch for {v}");
        }
        for v in [0i64, 1, -1, 2, -2, i64::MIN, i64::MAX] {
            let encoded = ((v << 1) ^ (v >> 63)) as u64;
            assert_eq!(zigzag64(encoded), v, "zigzag64 mismatch for {v}");
        }
    }

    #[test]
    fn skip_field_varint() {
        let bytes = encode_varint(12345);
        let mut pos = 0;
        skip_field(0, &bytes, &mut pos).unwrap();
        assert_eq!(pos, bytes.len());
    }

    #[test]
    fn skip_field_length_delimited() {
        let mut bytes = encode_varint(5); // length
        bytes.extend_from_slice(b"hello");
        let mut pos = 0;
        skip_field(2, &bytes, &mut pos).unwrap();
        assert_eq!(pos, bytes.len());
    }

    #[test]
    fn skip_field_invalid_wire_type() {
        let mut pos = 0;
        assert!(matches!(
            skip_field(7, &[], &mut pos),
            Err(WireToArrowError::InvalidWireType { wire_type: 7 })
        ));
    }

    #[test]
    fn skip_field_length_delimited_eof() {
        let mut bytes = encode_varint(10);
        bytes.extend_from_slice(b"short"); // only 5 bytes, not 10
        let mut pos = 0;
        assert!(matches!(
            skip_field(2, &bytes, &mut pos),
            Err(WireToArrowError::UnexpectedEof)
        ));
    }
}
