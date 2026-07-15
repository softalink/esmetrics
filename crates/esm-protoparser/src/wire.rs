//! Minimal protobuf wire-format reader.
//!
//! Replaces the `easyproto` primitives (`FieldContext.NextField`,
//! `.MessageData()`, `.Double()`, `.Int64()`, ...) consumed by the upstream
//! VictoriaMetrics v1.146.0 `lib/prompb` unmarshalers. Only the wire types
//! actually used by decode-only `prompb` parsing are supported: varint (0),
//! fixed64 (1), length-delimited (2), and fixed32 (5). Wire types 3 and 4
//! (deprecated proto2 groups) are rejected as errors, matching easyproto's
//! `NextField` behavior.
//!
//! This module is crate-internal: later phases (OTLP) reuse it for their own
//! decoding.

use std::fmt;

/// Error returned by [`WireReader`] methods.
#[derive(Debug, PartialEq, Eq)]
pub enum WireError {
    /// The input ended before a complete field, varint, or fixed-width value
    /// could be read.
    UnexpectedEof,
    /// A varint continuation bit was set for more than 10 bytes, which
    /// cannot represent a valid 64-bit value.
    VarintOverflow,
    /// A length-delimited field declared a length that exceeds the
    /// remaining input.
    LengthOutOfRange,
    /// Field wire type 3 (`STARTGROUP`) or 4 (`ENDGROUP`), or a wire type
    /// that did not match what the caller expected for a given field.
    InvalidWireType(u8),
    /// A varint value did not fit into the narrower integer type the field
    /// is declared as (e.g. a `uint32`/`sint32` field whose varint decodes
    /// to a value outside that range).
    IntegerOutOfRange,
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WireError::UnexpectedEof => write!(f, "unexpected end of protobuf input"),
            WireError::VarintOverflow => write!(f, "varint is too long (more than 10 bytes)"),
            WireError::LengthOutOfRange => {
                write!(f, "length-delimited field length exceeds remaining input")
            }
            WireError::InvalidWireType(wt) => write!(f, "unsupported protobuf wire type {wt}"),
            WireError::IntegerOutOfRange => {
                write!(f, "varint value does not fit into the target integer type")
            }
        }
    }
}

impl std::error::Error for WireError {}

/// A cursor over a protobuf-encoded byte slice.
///
/// Go: replaces `easyproto.FieldContext` plus the accessor methods used by
/// `lib/prompb/write_request_unmarshaler.go`.
pub(crate) struct WireReader<'a> {
    src: &'a [u8],
    pos: usize,
}

impl<'a> WireReader<'a> {
    pub(crate) fn new(src: &'a [u8]) -> Self {
        WireReader { src, pos: 0 }
    }

    pub(crate) fn is_eof(&self) -> bool {
        self.pos >= self.src.len()
    }

    /// Reads a field tag and splits it into `(field_number, wire_type)`.
    ///
    /// Go: `easyproto.FieldContext.NextField` (the tag-parsing half; callers
    /// then use the field-specific accessor for the value).
    pub(crate) fn read_tag(&mut self) -> Result<(u32, u8), WireError> {
        let tag = self.read_varint()?;
        let wire_type = (tag & 0x7) as u8;
        let field_num = (tag >> 3) as u32;
        match wire_type {
            0 | 1 | 2 | 5 => Ok((field_num, wire_type)),
            _ => Err(WireError::InvalidWireType(wire_type)),
        }
    }

    /// Reads a base-128 varint (LEB128, unsigned).
    pub(crate) fn read_varint(&mut self) -> Result<u64, WireError> {
        let mut result: u64 = 0;
        let mut shift: u32 = 0;
        loop {
            if shift >= 70 {
                return Err(WireError::VarintOverflow);
            }
            let byte = *self.src.get(self.pos).ok_or(WireError::UnexpectedEof)?;
            self.pos += 1;
            result |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
        }
    }

    /// Reads a length-delimited field's payload (wire type 2): a varint
    /// length followed by that many raw bytes, borrowed from the input.
    pub(crate) fn read_len_delim(&mut self) -> Result<&'a [u8], WireError> {
        let len = self.read_varint()?;
        let len = usize::try_from(len).map_err(|_| WireError::LengthOutOfRange)?;
        self.advance(len)
    }

    /// Reads a fixed64 field (wire type 1) as a IEEE-754 double.
    pub(crate) fn read_double(&mut self) -> Result<f64, WireError> {
        Ok(f64::from_bits(self.read_fixed64()?))
    }

    /// Reads a fixed64 field (wire type 1) as a raw little-endian `u64`.
    ///
    /// Go: `easyproto.FieldContext.Fixed64`. Used by OTLP `fixed64` fields
    /// (`time_unix_nano`, `count`, `zero_count`, ...).
    pub(crate) fn read_fixed64(&mut self) -> Result<u64, WireError> {
        let bytes = self.advance(8)?;
        let mut buf = [0u8; 8];
        buf.copy_from_slice(bytes);
        Ok(u64::from_le_bytes(buf))
    }

    /// Reads a fixed64 field (wire type 1) reinterpreted as a two's-complement
    /// `i64`.
    ///
    /// Go: `easyproto.FieldContext.Sfixed64`. Not zigzag-encoded — plain bit
    /// reinterpretation of the 8 little-endian bytes, matching
    /// `int64(u64)` in Go.
    pub(crate) fn read_sfixed64(&mut self) -> Result<i64, WireError> {
        Ok(self.read_fixed64()? as i64)
    }

    /// Reads a varint (wire type 0) as a `uint32`, erroring if the decoded
    /// value overflows 32 bits.
    ///
    /// Go: `easyproto.FieldContext.Uint32` (`getUint32` returns `!ok` on
    /// overflow).
    pub(crate) fn read_uint32(&mut self) -> Result<u32, WireError> {
        let v = self.read_varint()?;
        u32::try_from(v).map_err(|_| WireError::IntegerOutOfRange)
    }

    /// Reads a varint (wire type 0) as a plain (non-zigzag) `int64`.
    ///
    /// Go: `easyproto.FieldContext.Int64` (two's-complement reinterpretation
    /// of the 64-bit varint, same as [`WireReader::read_varint`] cast to
    /// `i64`).
    pub(crate) fn read_int64(&mut self) -> Result<i64, WireError> {
        Ok(self.read_varint()? as i64)
    }

    /// Reads a varint (wire type 0) as a `bool` (`0` is `false`, anything
    /// else is `true`).
    ///
    /// Go: `easyproto.FieldContext.Bool`.
    pub(crate) fn read_bool(&mut self) -> Result<bool, WireError> {
        Ok(self.read_varint()? != 0)
    }

    /// Reads a varint (wire type 0) as a zigzag-encoded `sint32`, erroring if
    /// the underlying varint overflows 32 bits.
    ///
    /// Go: `easyproto.FieldContext.Sint32`.
    pub(crate) fn read_sint32(&mut self) -> Result<i32, WireError> {
        let v = self.read_varint()?;
        let v32 = u32::try_from(v).map_err(|_| WireError::IntegerOutOfRange)?;
        Ok(((v32 >> 1) as i32) ^ -((v32 & 1) as i32))
    }

    /// Appends a `repeated fixed64` field's value(s) to `dst`, accepting
    /// either the packed encoding (wire type 2: a length-delimited run of
    /// 8-byte little-endian values) or the legacy unpacked encoding (wire
    /// type 1: one 8-byte value per field occurrence). Call once per
    /// occurrence of the field encountered while iterating; both forms
    /// accumulate correctly across repeats.
    ///
    /// Go: `easyproto.FieldContext.UnpackFixed64s` (both branches).
    pub(crate) fn read_packed_fixed64s(
        &mut self,
        wire_type: u8,
        dst: &mut Vec<u64>,
    ) -> Result<(), WireError> {
        match wire_type {
            1 => {
                dst.push(self.read_fixed64()?);
                Ok(())
            }
            2 => {
                let data = self.read_len_delim()?;
                if data.len() % 8 != 0 {
                    return Err(WireError::LengthOutOfRange);
                }
                for chunk in data.chunks_exact(8) {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(chunk);
                    dst.push(u64::from_le_bytes(buf));
                }
                Ok(())
            }
            _ => Err(WireError::InvalidWireType(wire_type)),
        }
    }

    /// Appends a `repeated double` field's value(s) to `dst`, accepting
    /// either the packed encoding (wire type 2) or the legacy unpacked
    /// encoding (wire type 1), same acceptance rules as
    /// [`WireReader::read_packed_fixed64s`].
    ///
    /// Go: `easyproto.FieldContext.UnpackDoubles`.
    pub(crate) fn read_packed_doubles(
        &mut self,
        wire_type: u8,
        dst: &mut Vec<f64>,
    ) -> Result<(), WireError> {
        match wire_type {
            1 => {
                dst.push(self.read_double()?);
                Ok(())
            }
            2 => {
                let data = self.read_len_delim()?;
                if data.len() % 8 != 0 {
                    return Err(WireError::LengthOutOfRange);
                }
                for chunk in data.chunks_exact(8) {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(chunk);
                    dst.push(f64::from_bits(u64::from_le_bytes(buf)));
                }
                Ok(())
            }
            _ => Err(WireError::InvalidWireType(wire_type)),
        }
    }

    /// Appends a `repeated uint64` field's value(s) to `dst`, accepting
    /// either the packed encoding (wire type 2: a length-delimited run of
    /// varints) or the legacy unpacked encoding (wire type 0: one varint per
    /// field occurrence).
    ///
    /// Go: `easyproto.FieldContext.UnpackUint64s`.
    pub(crate) fn read_packed_uint64s(
        &mut self,
        wire_type: u8,
        dst: &mut Vec<u64>,
    ) -> Result<(), WireError> {
        match wire_type {
            0 => {
                dst.push(self.read_varint()?);
                Ok(())
            }
            2 => {
                let data = self.read_len_delim()?;
                let mut sub = WireReader::new(data);
                while !sub.is_eof() {
                    dst.push(sub.read_varint()?);
                }
                Ok(())
            }
            _ => Err(WireError::InvalidWireType(wire_type)),
        }
    }

    /// Skips a field's value given its wire type, without interpreting it.
    ///
    /// Go: the default (no matching `case`) branch of the `switch fc.FieldNum`
    /// loops in `write_request_unmarshaler.go`, where easyproto's
    /// `FieldContext.NextField` has already consumed the value bytes.
    pub(crate) fn skip(&mut self, wire_type: u8) -> Result<(), WireError> {
        match wire_type {
            0 => {
                self.read_varint()?;
            }
            1 => {
                self.advance(8)?;
            }
            2 => {
                self.read_len_delim()?;
            }
            5 => {
                self.advance(4)?;
            }
            _ => return Err(WireError::InvalidWireType(wire_type)),
        }
        Ok(())
    }

    fn advance(&mut self, len: usize) -> Result<&'a [u8], WireError> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(WireError::LengthOutOfRange)?;
        let bytes = self
            .src
            .get(self.pos..end)
            .ok_or(WireError::LengthOutOfRange)?;
        self.pos = end;
        Ok(bytes)
    }
}

/// Decodes a protobuf `sint64` zigzag-encoded varint value.
///
/// Not used by `prompb` (its `int64`/`timestamp` fields are plain varints,
/// not zigzag) but needed by OTLP decoding in a later phase — verify the
/// field's proto type is `sint32`/`sint64` before reaching for this.
#[allow(dead_code)]
pub(crate) fn zigzag_decode(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn append_varint(dst: &mut Vec<u8>, mut v: u64) {
        loop {
            let byte = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                dst.push(byte);
                break;
            }
            dst.push(byte | 0x80);
        }
    }

    #[test]
    fn read_varint_roundtrips_small_and_large_values() {
        for v in [0u64, 1, 127, 128, 300, u64::MAX] {
            let mut buf = Vec::new();
            append_varint(&mut buf, v);
            let mut r = WireReader::new(&buf);
            assert_eq!(r.read_varint().unwrap(), v);
            assert!(r.is_eof());
        }
    }

    #[test]
    fn read_varint_errors_on_truncated_input() {
        let buf = [0x80u8]; // continuation bit set, no following byte
        let mut r = WireReader::new(&buf);
        assert_eq!(r.read_varint().unwrap_err(), WireError::UnexpectedEof);
    }

    #[test]
    fn read_varint_errors_on_overlong_varint() {
        let buf = [0x80u8; 11]; // 11 continuation bytes, never terminates
        let mut r = WireReader::new(&buf);
        assert_eq!(r.read_varint().unwrap_err(), WireError::VarintOverflow);
    }

    #[test]
    fn read_tag_splits_field_number_and_wire_type() {
        let mut buf = Vec::new();
        append_varint(&mut buf, (5u64 << 3) | 2); // field 5, wire type 2
        let mut r = WireReader::new(&buf);
        assert_eq!(r.read_tag().unwrap(), (5, 2));
    }

    #[test]
    fn read_tag_rejects_group_wire_types() {
        for wt in [3u64, 4] {
            let mut buf = Vec::new();
            append_varint(&mut buf, (1u64 << 3) | wt);
            let mut r = WireReader::new(&buf);
            assert_eq!(
                r.read_tag().unwrap_err(),
                WireError::InvalidWireType(wt as u8)
            );
        }
    }

    #[test]
    fn read_len_delim_borrows_input_without_copying() {
        let mut buf = Vec::new();
        append_varint(&mut buf, 3);
        buf.extend_from_slice(b"abc");
        buf.push(0xff); // trailing byte must not be consumed
        let mut r = WireReader::new(&buf);
        let got = r.read_len_delim().unwrap();
        assert_eq!(got, b"abc");
        assert_eq!(got.as_ptr(), buf[1..].as_ptr());
        assert!(!r.is_eof());
    }

    #[test]
    fn read_len_delim_errors_when_length_exceeds_remaining_input() {
        let mut buf = Vec::new();
        append_varint(&mut buf, 10);
        buf.extend_from_slice(b"ab");
        let mut r = WireReader::new(&buf);
        assert_eq!(r.read_len_delim().unwrap_err(), WireError::LengthOutOfRange);
    }

    #[test]
    fn read_double_decodes_little_endian_ieee754() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&42.5f64.to_le_bytes());
        let mut r = WireReader::new(&buf);
        assert_eq!(r.read_double().unwrap(), 42.5);
    }

    #[test]
    fn skip_advances_past_each_wire_type() {
        // varint
        let mut buf = Vec::new();
        append_varint(&mut buf, 300);
        let mut r = WireReader::new(&buf);
        r.skip(0).unwrap();
        assert!(r.is_eof());

        // fixed64
        let buf = [0u8; 8];
        let mut r = WireReader::new(&buf);
        r.skip(1).unwrap();
        assert!(r.is_eof());

        // length-delimited
        let mut buf = Vec::new();
        append_varint(&mut buf, 2);
        buf.extend_from_slice(b"xy");
        let mut r = WireReader::new(&buf);
        r.skip(2).unwrap();
        assert!(r.is_eof());

        // fixed32
        let buf = [0u8; 4];
        let mut r = WireReader::new(&buf);
        r.skip(5).unwrap();
        assert!(r.is_eof());
    }

    #[test]
    fn zigzag_decode_matches_protobuf_spec() {
        assert_eq!(zigzag_decode(0), 0);
        assert_eq!(zigzag_decode(1), -1);
        assert_eq!(zigzag_decode(2), 1);
        assert_eq!(zigzag_decode(3), -2);
    }

    #[test]
    fn read_fixed64_decodes_little_endian() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&0x0102_0304_0506_0708u64.to_le_bytes());
        let mut r = WireReader::new(&buf);
        assert_eq!(r.read_fixed64().unwrap(), 0x0102_0304_0506_0708);
    }

    #[test]
    fn read_sfixed64_reinterprets_bits_as_twos_complement() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(-1i64).to_le_bytes());
        let mut r = WireReader::new(&buf);
        assert_eq!(r.read_sfixed64().unwrap(), -1);
    }

    #[test]
    fn read_uint32_accepts_in_range_and_rejects_overflow() {
        let mut buf = Vec::new();
        append_varint(&mut buf, u64::from(u32::MAX));
        let mut r = WireReader::new(&buf);
        assert_eq!(r.read_uint32().unwrap(), u32::MAX);

        let mut buf = Vec::new();
        append_varint(&mut buf, u64::from(u32::MAX) + 1);
        let mut r = WireReader::new(&buf);
        assert_eq!(r.read_uint32().unwrap_err(), WireError::IntegerOutOfRange);
    }

    #[test]
    fn read_int64_reinterprets_varint_as_twos_complement() {
        let mut buf = Vec::new();
        append_varint(&mut buf, u64::MAX); // -1 as u64
        let mut r = WireReader::new(&buf);
        assert_eq!(r.read_int64().unwrap(), -1);
    }

    #[test]
    fn read_bool_maps_zero_and_nonzero() {
        for (v, want) in [(0u64, false), (1u64, true), (42u64, true)] {
            let mut buf = Vec::new();
            append_varint(&mut buf, v);
            let mut r = WireReader::new(&buf);
            assert_eq!(r.read_bool().unwrap(), want);
        }
    }

    #[test]
    fn read_sint32_zigzag_decodes_within_32_bits() {
        for (encoded, want) in [
            (0u64, 0i32),
            (1, -1),
            (2, 1),
            (3, -2),
            (4_294_967_294, i32::MAX),
        ] {
            let mut buf = Vec::new();
            append_varint(&mut buf, encoded);
            let mut r = WireReader::new(&buf);
            assert_eq!(r.read_sint32().unwrap(), want);
        }
    }

    #[test]
    fn read_sint32_rejects_overflow() {
        let mut buf = Vec::new();
        append_varint(&mut buf, u64::from(u32::MAX) + 1);
        let mut r = WireReader::new(&buf);
        assert_eq!(r.read_sint32().unwrap_err(), WireError::IntegerOutOfRange);
    }

    #[test]
    fn read_packed_fixed64s_accepts_packed_and_unpacked_forms() {
        // Packed: one length-delimited field of two 8-byte values.
        let mut packed = Vec::new();
        packed.extend_from_slice(&1u64.to_le_bytes());
        packed.extend_from_slice(&2u64.to_le_bytes());
        let mut buf = Vec::new();
        append_varint(&mut buf, packed.len() as u64);
        buf.extend_from_slice(&packed);
        let mut r = WireReader::new(&buf);
        let mut dst = Vec::new();
        r.read_packed_fixed64s(2, &mut dst).unwrap();
        assert_eq!(dst, vec![1, 2]);

        // Unpacked: two separate wire-type-1 occurrences, called once each.
        let buf = 3u64.to_le_bytes();
        let mut r = WireReader::new(&buf);
        let mut dst = Vec::new();
        r.read_packed_fixed64s(1, &mut dst).unwrap();
        let buf = 4u64.to_le_bytes();
        let mut r = WireReader::new(&buf);
        r.read_packed_fixed64s(1, &mut dst).unwrap();
        assert_eq!(dst, vec![3, 4]);
    }

    #[test]
    fn read_packed_doubles_accepts_packed_and_unpacked_forms() {
        let mut packed = Vec::new();
        packed.extend_from_slice(&1.5f64.to_le_bytes());
        packed.extend_from_slice(&2.5f64.to_le_bytes());
        let mut buf = Vec::new();
        append_varint(&mut buf, packed.len() as u64);
        buf.extend_from_slice(&packed);
        let mut r = WireReader::new(&buf);
        let mut dst = Vec::new();
        r.read_packed_doubles(2, &mut dst).unwrap();
        assert_eq!(dst, vec![1.5, 2.5]);

        let buf = 9.5f64.to_le_bytes();
        let mut r = WireReader::new(&buf);
        r.read_packed_doubles(1, &mut dst).unwrap();
        assert_eq!(dst, vec![1.5, 2.5, 9.5]);
    }

    #[test]
    fn read_packed_uint64s_accepts_packed_and_unpacked_forms() {
        let mut packed = Vec::new();
        append_varint(&mut packed, 300);
        append_varint(&mut packed, 65536);
        let mut buf = Vec::new();
        append_varint(&mut buf, packed.len() as u64);
        buf.extend_from_slice(&packed);
        let mut r = WireReader::new(&buf);
        let mut dst = Vec::new();
        r.read_packed_uint64s(2, &mut dst).unwrap();
        assert_eq!(dst, vec![300, 65536]);

        let mut buf = Vec::new();
        append_varint(&mut buf, 7);
        let mut r = WireReader::new(&buf);
        r.read_packed_uint64s(0, &mut dst).unwrap();
        assert_eq!(dst, vec![300, 65536, 7]);
    }

    #[test]
    fn read_packed_fixed64s_rejects_misaligned_packed_length() {
        let mut buf = Vec::new();
        append_varint(&mut buf, 5); // not a multiple of 8
        buf.extend_from_slice(&[0u8; 5]);
        let mut r = WireReader::new(&buf);
        let mut dst = Vec::new();
        assert_eq!(
            r.read_packed_fixed64s(2, &mut dst).unwrap_err(),
            WireError::LengthOutOfRange
        );
    }
}
