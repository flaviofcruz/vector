//! Proto wire-format reading and writing shared by the Lumberjack Prime union VRL functions.
//!
//! The union splice works on a record's wire bytes and never decodes it: a route is the populated
//! member of a oneof, read from the field numbers and lengths that the bytes carry. Every read is
//! bounds-checked, and a malformed or truncated record is an error, so the caller fails closed.

/// The largest valid proto field number.
pub(crate) const MAX_FIELD_NUMBER: u64 = (1 << 29) - 1;

/// One top-level field of a message.
#[derive(Clone, Copy, Debug)]
pub(crate) struct WireField<'a> {
    pub(crate) number: u64,
    pub(crate) wire_type: u8,
    /// The field's encoded bytes, tag included.
    pub(crate) encoded: &'a [u8],
    /// The field's value: the payload of a length-delimited field, or the encoded varint or fixed
    /// bytes of the other wire types.
    pub(crate) value: &'a [u8],
}

/// The top-level fields of `buf`, in wire order. Rejects truncated fields, varints longer than ten
/// bytes, field numbers outside 1..=2^29-1, and the group wire types.
pub(crate) fn fields(buf: &[u8]) -> Result<Vec<WireField<'_>>, String> {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos < buf.len() {
        let start = pos;
        let tag = read_varint(buf, &mut pos)?;
        let number = tag >> 3;
        if number == 0 || number > MAX_FIELD_NUMBER {
            return Err(format!("invalid field number {number}"));
        }
        let wire_type = u8::try_from(tag & 7).unwrap_or(u8::MAX);
        let value_start = pos;
        match wire_type {
            0 => {
                read_varint(buf, &mut pos)?;
            }
            1 => pos = skip(buf, pos, 8)?,
            2 => {
                let len = usize::try_from(read_varint(buf, &mut pos)?)
                    .map_err(|_| "length overflows usize".to_string())?;
                let value_start = pos;
                pos = skip(buf, pos, len)?;
                out.push(WireField {
                    number,
                    wire_type,
                    encoded: &buf[start..pos],
                    value: &buf[value_start..pos],
                });
                continue;
            }
            5 => pos = skip(buf, pos, 4)?,
            other => return Err(format!("unsupported wire type {other}")),
        }
        out.push(WireField {
            number,
            wire_type,
            encoded: &buf[start..pos],
            value: &buf[value_start..pos],
        });
    }
    Ok(out)
}

/// Reads one base-128 varint at `pos`, advancing it.
pub(crate) fn read_varint(buf: &[u8], pos: &mut usize) -> Result<u64, String> {
    let mut value: u64 = 0;
    for shift in (0..64).step_by(7) {
        let byte = *buf.get(*pos).ok_or("truncated varint")?;
        *pos += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err("varint longer than 10 bytes".into())
}

/// Advances `pos` by `len` bytes, failing if the field runs past the end of the buffer.
pub(crate) fn skip(buf: &[u8], pos: usize, len: usize) -> Result<usize, String> {
    pos.checked_add(len)
        .filter(|end| *end <= buf.len())
        .ok_or_else(|| "truncated field".to_string())
}

pub(crate) fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = u8::try_from(value & 0x7f).unwrap_or(0);
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

#[cfg(test)]
pub(crate) mod test_fields {
    use super::write_varint;

    /// A varint field: `number` with the value `value`.
    pub(crate) fn varint_field(number: u64, value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        write_varint(&mut out, number << 3);
        write_varint(&mut out, value);
        out
    }

    /// A length-delimited field: `number` holding `payload`.
    pub(crate) fn bytes_field(number: u64, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        write_varint(&mut out, (number << 3) | 2);
        write_varint(&mut out, payload.len() as u64);
        out.extend_from_slice(payload);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::test_fields::{bytes_field, varint_field};
    use super::*;

    #[test]
    fn reads_each_wire_type_in_order() {
        let mut buf = varint_field(1, 150);
        buf.extend(bytes_field(2, b"ab"));
        buf.extend([(3 << 3) | 1, 1, 2, 3, 4, 5, 6, 7, 8]);
        buf.extend([(4 << 3) | 5, 9, 9, 9, 9]);
        let found = fields(&buf).unwrap();
        let numbers: Vec<(u64, u8)> = found.iter().map(|f| (f.number, f.wire_type)).collect();
        assert_eq!(numbers, vec![(1, 0), (2, 2), (3, 1), (4, 5)]);
        assert_eq!(found[1].value, b"ab");
        assert_eq!(found[1].encoded, bytes_field(2, b"ab").as_slice());
    }

    #[test]
    fn empty_message_has_no_fields() {
        assert!(fields(&[]).unwrap().is_empty());
    }

    #[test]
    fn malformed_varint_tag_is_an_error() {
        // A tag whose varint never ends within ten bytes.
        assert!(fields(&[0xff; 11]).is_err());
        // A tag varint cut off by the end of the buffer.
        assert!(fields(&[0x80]).is_err());
    }

    #[test]
    fn truncated_length_delimited_field_is_an_error() {
        // Declares 10 bytes of payload and carries 2.
        assert!(fields(&[(7 << 3) | 2, 10, 1, 2]).is_err());
    }

    #[test]
    fn truncated_fixed_fields_are_errors() {
        assert!(fields(&[(1 << 3) | 1, 1, 2, 3]).is_err());
        assert!(fields(&[(1 << 3) | 5, 1]).is_err());
    }

    #[test]
    fn field_number_zero_is_an_error() {
        assert!(fields(&[0x00, 0x01]).is_err());
    }

    #[test]
    fn group_wire_types_are_errors() {
        assert!(fields(&[(1 << 3) | 3]).is_err());
        assert!(fields(&[(1 << 3) | 4]).is_err());
    }

    #[test]
    fn a_length_that_overflows_is_an_error() {
        let mut buf = vec![(1 << 3) | 2];
        write_varint(&mut buf, u64::MAX);
        assert!(fields(&buf).is_err());
    }

    #[test]
    fn varint_round_trips() {
        for value in [0_u64, 1, 127, 128, 300, u64::MAX] {
            let mut buf = Vec::new();
            write_varint(&mut buf, value);
            let mut pos = 0;
            assert_eq!(read_varint(&buf, &mut pos).unwrap(), value);
            assert_eq!(pos, buf.len());
        }
    }
}
