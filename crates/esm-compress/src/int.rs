//! Primitive binary-encoding helpers, byte-compatible with
//! `lib/encoding/int.go` in VictoriaMetrics v1.144.0.
//!
//! Numeric helpers are big-endian. Variable-length unsigned integers
//! ("varuint") match Protocol Buffers' unsigned varint (LEB128 base-128 with
//! a continuation high-bit). The [`marshal_bytes`] / [`unmarshal_bytes`] pair
//! emits a varuint length followed by raw bytes — VM calls this "Bytes".
//!
//! All `marshal_*` functions append to a caller-owned `Vec<u8>` to match VM's
//! `MarshalXxx(dst []byte, x) []byte` idiom without allocating per call.
//! The `unmarshal_*` functions return `(value, consumed_bytes)` or
//! `Result<value, Error>` and never panic on truncated input.

use thiserror::Error;

/// Error returned by `unmarshal_*` helpers on malformed input.
#[derive(Debug, Error)]
pub enum DecodeError {
    /// The source buffer is shorter than required.
    #[error("truncated input: need {needed} bytes, have {have}")]
    Truncated { needed: usize, have: usize },
    /// A varuint did not terminate within 10 bytes (maximum for a 64-bit value).
    #[error("varuint did not terminate within 10 bytes")]
    VarUintOverflow,
}

/// Append `u` as 4 big-endian bytes.
#[inline]
pub fn marshal_uint32(dst: &mut Vec<u8>, u: u32) {
    dst.extend_from_slice(&u.to_be_bytes());
}

/// Read a big-endian u32 from the head of `src`. Returns the value and the
/// number of bytes consumed (always 4).
///
/// # Errors
/// Returns [`DecodeError::Truncated`] if `src.len() < 4`.
#[inline]
pub fn unmarshal_uint32(src: &[u8]) -> Result<(u32, usize), DecodeError> {
    if src.len() < 4 {
        return Err(DecodeError::Truncated { needed: 4, have: src.len() });
    }
    let v = u32::from_be_bytes([src[0], src[1], src[2], src[3]]);
    Ok((v, 4))
}

/// Append `u` as 8 big-endian bytes.
#[inline]
pub fn marshal_uint64(dst: &mut Vec<u8>, u: u64) {
    dst.extend_from_slice(&u.to_be_bytes());
}

/// Read a big-endian u64 from the head of `src`. Returns the value and the
/// number of bytes consumed (always 8).
///
/// # Errors
/// Returns [`DecodeError::Truncated`] if `src.len() < 8`.
#[inline]
pub fn unmarshal_uint64(src: &[u8]) -> Result<(u64, usize), DecodeError> {
    if src.len() < 8 {
        return Err(DecodeError::Truncated { needed: 8, have: src.len() });
    }
    let v = u64::from_be_bytes([src[0], src[1], src[2], src[3], src[4], src[5], src[6], src[7]]);
    Ok((v, 8))
}

/// Append `u` as a base-128 varuint (LEB128, unsigned). Matches Go's
/// `lib/encoding/int.go:287` `MarshalVarUint64`. The Go fast paths for small
/// values are collapsed into a single loop; the wire bytes are identical.
#[allow(clippy::cast_possible_truncation)]
pub fn marshal_varuint64(dst: &mut Vec<u8>, mut u: u64) {
    while u >= 0x80 {
        // Intentional truncation: we want the low 7 bits with the continuation flag set.
        dst.push((u as u8) | 0x80);
        u >>= 7;
    }
    // `u` is now in [0, 0x80); the cast is exact.
    dst.push(u as u8);
}

/// Read a base-128 varuint from the head of `src`. Returns the value and the
/// number of bytes consumed.
///
/// # Errors
/// - [`DecodeError::Truncated`] if `src` ends mid-varuint.
/// - [`DecodeError::VarUintOverflow`] if no byte with cleared MSB is found
///   within 10 bytes (`ceil(64 / 7)`).
pub fn unmarshal_varuint64(src: &[u8]) -> Result<(u64, usize), DecodeError> {
    let mut result: u64 = 0;
    for (i, &b) in src.iter().enumerate().take(10) {
        result |= u64::from(b & 0x7f) << (7 * i);
        if b & 0x80 == 0 {
            return Ok((result, i + 1));
        }
    }
    if src.len() >= 10 {
        Err(DecodeError::VarUintOverflow)
    } else {
        Err(DecodeError::Truncated { needed: src.len() + 1, have: src.len() })
    }
}

/// Append a sequence of varuints back-to-back. Wire bytes match
/// `MarshalVarUint64s` (`lib/encoding/int.go:305`).
pub fn marshal_varuint64s(dst: &mut Vec<u8>, vals: &[u64]) {
    for &v in vals {
        marshal_varuint64(dst, v);
    }
}

/// Read exactly `dst.len()` varuints from the head of `src` into `dst`,
/// returning the unconsumed remainder.
///
/// # Errors
/// Returns [`DecodeError`] if the source runs out before `dst.len()` values
/// have been decoded, or if any varuint is malformed.
pub fn unmarshal_varuint64s_into<'src>(
    dst: &mut [u64],
    mut src: &'src [u8],
) -> Result<&'src [u8], DecodeError> {
    for slot in dst.iter_mut() {
        let (v, n) = unmarshal_varuint64(src)?;
        *slot = v;
        src = &src[n..];
    }
    Ok(src)
}

/// Zig-zag encode a signed integer into an unsigned varuint and append.
/// Matches VM's `MarshalVarInt64` (`lib/encoding/int.go:87`).
#[allow(clippy::cast_sign_loss)]
pub fn marshal_varint64(dst: &mut Vec<u8>, v: i64) {
    // Bit-cast: zig-zag of i64 produces a wire-equivalent u64; sign
    // semantics are intentionally erased.
    let u = ((v << 1) ^ (v >> 63)) as u64;
    marshal_varuint64(dst, u);
}

/// Append a sequence of zig-zag-encoded signed varints back-to-back.
pub fn marshal_varint64s(dst: &mut Vec<u8>, vals: &[i64]) {
    for &v in vals {
        marshal_varint64(dst, v);
    }
}

/// Decode one zig-zag-encoded signed varint from the head of `src`. Returns
/// `(value, consumed)`.
///
/// # Errors
/// See [`unmarshal_varuint64`].
#[allow(clippy::cast_possible_wrap)]
pub fn unmarshal_varint64(src: &[u8]) -> Result<(i64, usize), DecodeError> {
    let (u, n) = unmarshal_varuint64(src)?;
    // Inverse zig-zag: bit-cast back from u64 to i64.
    let v = ((u >> 1) as i64) ^ -((u & 1) as i64);
    Ok((v, n))
}

/// Read exactly `dst.len()` zig-zag-encoded signed varints into `dst`.
///
/// # Errors
/// See [`unmarshal_varuint64s_into`].
pub fn unmarshal_varint64s_into<'src>(
    dst: &mut [i64],
    mut src: &'src [u8],
) -> Result<&'src [u8], DecodeError> {
    for slot in dst.iter_mut() {
        let (v, n) = unmarshal_varint64(src)?;
        *slot = v;
        src = &src[n..];
    }
    Ok(src)
}

/// Append `b` as a length-prefixed byte string: varuint(len) ++ raw bytes.
/// Equivalent to VM's `lib/encoding/int.go:506` `MarshalBytes`.
pub fn marshal_bytes(dst: &mut Vec<u8>, b: &[u8]) {
    marshal_varuint64(dst, b.len() as u64);
    dst.extend_from_slice(b);
}

/// Read a length-prefixed byte string from the head of `src`. Returns a
/// borrowed slice into `src` and the total bytes consumed (length prefix +
/// payload).
///
/// # Errors
/// Returns [`DecodeError`] if the prefix is malformed or the payload is
/// truncated.
pub fn unmarshal_bytes(src: &[u8]) -> Result<(&[u8], usize), DecodeError> {
    let (n, n_size) = unmarshal_varuint64(src)?;
    let n = usize::try_from(n)
        .map_err(|_| DecodeError::Truncated { needed: usize::MAX, have: src.len() })?;
    let total = n_size + n;
    if src.len() < total {
        return Err(DecodeError::Truncated { needed: total, have: src.len() });
    }
    Ok((&src[n_size..total], total))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uint32_roundtrip_big_endian() {
        let mut buf = Vec::new();
        marshal_uint32(&mut buf, 0x1234_5678);
        assert_eq!(buf, [0x12, 0x34, 0x56, 0x78]);
        let (v, n) = unmarshal_uint32(&buf).unwrap();
        assert_eq!(v, 0x1234_5678);
        assert_eq!(n, 4);
    }

    #[test]
    fn uint64_roundtrip_big_endian() {
        let mut buf = Vec::new();
        marshal_uint64(&mut buf, 0x0102_0304_0506_0708);
        assert_eq!(buf, [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
        let (v, n) = unmarshal_uint64(&buf).unwrap();
        assert_eq!(v, 0x0102_0304_0506_0708);
        assert_eq!(n, 8);
    }

    #[test]
    fn varuint64_small_values_single_byte() {
        for v in 0u64..0x80 {
            let mut buf = Vec::new();
            marshal_varuint64(&mut buf, v);
            assert_eq!(buf.len(), 1);
            assert_eq!(buf[0], u8::try_from(v).unwrap());
            let (out, n) = unmarshal_varuint64(&buf).unwrap();
            assert_eq!(out, v);
            assert_eq!(n, 1);
        }
    }

    #[test]
    fn varuint64_two_byte_boundary() {
        let mut buf = Vec::new();
        marshal_varuint64(&mut buf, 0x80);
        assert_eq!(buf, [0x80, 0x01]);
        let (out, n) = unmarshal_varuint64(&buf).unwrap();
        assert_eq!(out, 0x80);
        assert_eq!(n, 2);
    }

    #[test]
    fn varuint64_max() {
        let mut buf = Vec::new();
        marshal_varuint64(&mut buf, u64::MAX);
        let (out, _) = unmarshal_varuint64(&buf).unwrap();
        assert_eq!(out, u64::MAX);
    }

    #[test]
    fn bytes_roundtrip() {
        let mut buf = Vec::new();
        marshal_bytes(&mut buf, b"hello");
        // Length prefix is varuint(5) = single byte 0x05.
        assert_eq!(buf, b"\x05hello");
        let (out, n) = unmarshal_bytes(&buf).unwrap();
        assert_eq!(out, b"hello");
        assert_eq!(n, 6);
    }

    #[test]
    fn bytes_empty_roundtrip() {
        let mut buf = Vec::new();
        marshal_bytes(&mut buf, b"");
        assert_eq!(buf, b"\x00");
        let (out, n) = unmarshal_bytes(&buf).unwrap();
        assert_eq!(out, b"");
        assert_eq!(n, 1);
    }

    #[test]
    fn truncated_uint64_errors() {
        let r = unmarshal_uint64(&[1, 2, 3]);
        assert!(matches!(r, Err(DecodeError::Truncated { needed: 8, have: 3 })));
    }

    #[test]
    fn signed_varint_roundtrip_small() {
        for v in [-100i64, -1, 0, 1, 100, i64::MIN, i64::MAX] {
            let mut buf = Vec::new();
            marshal_varint64(&mut buf, v);
            let (out, _) = unmarshal_varint64(&buf).unwrap();
            assert_eq!(out, v);
        }
    }

    #[test]
    fn signed_varint_slice_roundtrip() {
        let vals = [-5i64, -1, 0, 1, 2, 100, -100, i64::MAX, i64::MIN];
        let mut buf = Vec::new();
        marshal_varint64s(&mut buf, &vals);
        let mut decoded = [0i64; 9];
        let tail = unmarshal_varint64s_into(&mut decoded, &buf).unwrap();
        assert!(tail.is_empty());
        assert_eq!(decoded, vals);
    }

    #[test]
    fn truncated_bytes_errors_on_payload() {
        // Length prefix says 10 but only 3 payload bytes provided.
        let r = unmarshal_bytes(&[10, b'a', b'b', b'c']);
        assert!(matches!(r, Err(DecodeError::Truncated { .. })));
    }
}
