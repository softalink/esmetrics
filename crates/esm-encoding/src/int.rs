//! Integer marshaling helpers. Port of Go lib/encoding/int.go.
//!
//! All `marshal_*` functions append to `dst`, mirroring Go's append-to-dst API.

/// Appends big-endian `u` to `dst`. Go: MarshalUint16.
#[inline]
pub fn marshal_uint16(dst: &mut Vec<u8>, u: u16) {
    dst.extend_from_slice(&u.to_be_bytes());
}

/// Returns unmarshaled u16 from `src`. The caller must ensure `src.len() >= 2`.
/// Go: UnmarshalUint16.
#[inline]
pub fn unmarshal_uint16(src: &[u8]) -> u16 {
    u16::from_be_bytes([src[0], src[1]])
}

/// Appends big-endian `u` to `dst`. Go: MarshalUint32.
#[inline]
pub fn marshal_uint32(dst: &mut Vec<u8>, u: u32) {
    dst.extend_from_slice(&u.to_be_bytes());
}

/// Returns unmarshaled u32 from `src`. The caller must ensure `src.len() >= 4`.
/// Go: UnmarshalUint32.
#[inline]
pub fn unmarshal_uint32(src: &[u8]) -> u32 {
    u32::from_be_bytes([src[0], src[1], src[2], src[3]])
}

/// Appends big-endian `u` to `dst`. Go: MarshalUint64.
#[inline]
pub fn marshal_uint64(dst: &mut Vec<u8>, u: u64) {
    dst.extend_from_slice(&u.to_be_bytes());
}

/// Returns unmarshaled u64 from `src`. The caller must ensure `src.len() >= 8`.
/// Go: UnmarshalUint64.
#[inline]
pub fn unmarshal_uint64(src: &[u8]) -> u64 {
    u64::from_be_bytes([
        src[0], src[1], src[2], src[3], src[4], src[5], src[6], src[7],
    ])
}

/// Appends zig-zag-encoded big-endian `v` to `dst`. Go: MarshalInt16.
#[inline]
pub fn marshal_int16(dst: &mut Vec<u8>, v: i16) {
    // Zig-zag encoding without branching improves compression for negative v.
    let v = (v << 1) ^ (v >> 15);
    marshal_uint16(dst, v as u16);
}

/// Returns unmarshaled i16 from `src`. The caller must ensure `src.len() >= 2`.
/// Go: UnmarshalInt16.
#[inline]
pub fn unmarshal_int16(src: &[u8]) -> i16 {
    let u = unmarshal_uint16(src);
    // Zig-zag decoding without branching.
    ((u >> 1) as i16) ^ (((u << 15) as i16) >> 15)
}

/// Appends zig-zag-encoded big-endian `v` to `dst`. Go: MarshalInt64.
#[inline]
pub fn marshal_int64(dst: &mut Vec<u8>, v: i64) {
    // Zig-zag encoding without branching improves compression for negative v.
    let v = (v << 1) ^ (v >> 63);
    marshal_uint64(dst, v as u64);
}

/// Returns unmarshaled i64 from `src`. The caller must ensure `src.len() >= 8`.
/// Go: UnmarshalInt64.
#[inline]
pub fn unmarshal_int64(src: &[u8]) -> i64 {
    let u = unmarshal_uint64(src);
    // Zig-zag decoding without branching.
    ((u >> 1) as i64) ^ (((u << 63) as i64) >> 63)
}

/// Appends the full varint encoding of `u` to `dst`.
///
/// Cases are sorted in the descending order of frequency on real data,
/// matching Go's marshalVarUint64sSlow body.
#[inline]
fn append_var_uint64(dst: &mut Vec<u8>, u: u64) {
    if u < (1 << 7) {
        dst.push(u as u8);
        return;
    }
    if u < (1 << (2 * 7)) {
        dst.extend_from_slice(&[(u | 0x80) as u8, (u >> 7) as u8]);
        return;
    }
    if u < (1 << (3 * 7)) {
        dst.extend_from_slice(&[
            (u | 0x80) as u8,
            ((u >> 7) | 0x80) as u8,
            (u >> (2 * 7)) as u8,
        ]);
        return;
    }

    if u >= (1 << (8 * 7)) {
        if u < (1 << (9 * 7)) {
            dst.extend_from_slice(&[
                (u | 0x80) as u8,
                ((u >> 7) | 0x80) as u8,
                ((u >> (2 * 7)) | 0x80) as u8,
                ((u >> (3 * 7)) | 0x80) as u8,
                ((u >> (4 * 7)) | 0x80) as u8,
                ((u >> (5 * 7)) | 0x80) as u8,
                ((u >> (6 * 7)) | 0x80) as u8,
                ((u >> (7 * 7)) | 0x80) as u8,
                (u >> (8 * 7)) as u8,
            ]);
            return;
        }
        dst.extend_from_slice(&[
            (u | 0x80) as u8,
            ((u >> 7) | 0x80) as u8,
            ((u >> (2 * 7)) | 0x80) as u8,
            ((u >> (3 * 7)) | 0x80) as u8,
            ((u >> (4 * 7)) | 0x80) as u8,
            ((u >> (5 * 7)) | 0x80) as u8,
            ((u >> (6 * 7)) | 0x80) as u8,
            ((u >> (7 * 7)) | 0x80) as u8,
            ((u >> (8 * 7)) | 0x80) as u8,
            1,
        ]);
        return;
    }

    if u < (1 << (4 * 7)) {
        dst.extend_from_slice(&[
            (u | 0x80) as u8,
            ((u >> 7) | 0x80) as u8,
            ((u >> (2 * 7)) | 0x80) as u8,
            (u >> (3 * 7)) as u8,
        ]);
        return;
    }
    if u < (1 << (5 * 7)) {
        dst.extend_from_slice(&[
            (u | 0x80) as u8,
            ((u >> 7) | 0x80) as u8,
            ((u >> (2 * 7)) | 0x80) as u8,
            ((u >> (3 * 7)) | 0x80) as u8,
            (u >> (4 * 7)) as u8,
        ]);
        return;
    }
    if u < (1 << (6 * 7)) {
        dst.extend_from_slice(&[
            (u | 0x80) as u8,
            ((u >> 7) | 0x80) as u8,
            ((u >> (2 * 7)) | 0x80) as u8,
            ((u >> (3 * 7)) | 0x80) as u8,
            ((u >> (4 * 7)) | 0x80) as u8,
            (u >> (5 * 7)) as u8,
        ]);
        return;
    }
    if u < (1 << (7 * 7)) {
        dst.extend_from_slice(&[
            (u | 0x80) as u8,
            ((u >> 7) | 0x80) as u8,
            ((u >> (2 * 7)) | 0x80) as u8,
            ((u >> (3 * 7)) | 0x80) as u8,
            ((u >> (4 * 7)) | 0x80) as u8,
            ((u >> (5 * 7)) | 0x80) as u8,
            (u >> (6 * 7)) as u8,
        ]);
        return;
    }
    dst.extend_from_slice(&[
        (u | 0x80) as u8,
        ((u >> 7) | 0x80) as u8,
        ((u >> (2 * 7)) | 0x80) as u8,
        ((u >> (3 * 7)) | 0x80) as u8,
        ((u >> (4 * 7)) | 0x80) as u8,
        ((u >> (5 * 7)) | 0x80) as u8,
        ((u >> (6 * 7)) | 0x80) as u8,
        (u >> (7 * 7)) as u8,
    ]);
}

/// Appends varint-encoded (zig-zag) `v` to `dst`. Go: MarshalVarInt64.
#[inline]
pub fn marshal_var_int64(dst: &mut Vec<u8>, v: i64) {
    let u = ((v << 1) ^ (v >> 63)) as u64;

    if v < (1 << 6) && v > -(1 << 6) {
        dst.push(u as u8);
        return;
    }
    if u < (1 << (2 * 7)) {
        dst.extend_from_slice(&[(u | 0x80) as u8, (u >> 7) as u8]);
        return;
    }
    if u < (1 << (3 * 7)) {
        dst.extend_from_slice(&[
            (u | 0x80) as u8,
            ((u >> 7) | 0x80) as u8,
            (u >> (2 * 7)) as u8,
        ]);
        return;
    }

    // Slow path for big integers.
    append_var_uint64(dst, u);
}

/// Appends varint-encoded (zig-zag) `vs` to `dst`. Go: MarshalVarInt64s.
pub fn marshal_var_int64s(dst: &mut Vec<u8>, vs: &[i64]) {
    let dst_len = dst.len();
    for &v in vs {
        if v >= (1 << 6) || v <= -(1 << 6) {
            dst.truncate(dst_len);
            marshal_var_int64s_slow(dst, vs);
            return;
        }
        let u = ((v << 1) ^ (v >> 63)) as u64;
        dst.push(u as u8);
    }
}

/// Go: marshalVarInt64sSlow.
fn marshal_var_int64s_slow(dst: &mut Vec<u8>, vs: &[i64]) {
    for &v in vs {
        let u = ((v << 1) ^ (v >> 63)) as u64;
        append_var_uint64(dst, u);
    }
}

/// Zig-zag decodes `u`.
#[inline]
fn zigzag_decode(u: u64) -> i64 {
    ((u >> 1) as i64) ^ (((u << 63) as i64) >> 63)
}

/// Go's binary.Uvarint: returns (value, size). size == 0 means src is too
/// small; size < 0 means value overflow (|size| bytes were read).
fn uvarint(src: &[u8]) -> (u64, isize) {
    let mut x: u64 = 0;
    let mut s: u32 = 0;
    for (i, &b) in src.iter().enumerate() {
        if i == 10 {
            // Overflow: more than MaxVarintLen64 bytes.
            return (0, -((i + 1) as isize));
        }
        if b < 0x80 {
            if i == 9 && b > 1 {
                return (0, -((i + 1) as isize)); // overflow
            }
            return (x | (b as u64) << s, (i + 1) as isize);
        }
        x |= ((b & 0x7f) as u64) << s;
        s += 7;
    }
    (0, 0)
}

/// Returns unmarshaled i64 from `src` and its size in bytes, or `None` if
/// `src` doesn't contain a valid varint. Go: UnmarshalVarInt64 (which returns
/// zero/negative size on error).
#[inline]
pub fn unmarshal_var_int64(src: &[u8]) -> Option<(i64, usize)> {
    let (u, n_size) = uvarint(src);
    if n_size <= 0 {
        return None;
    }
    Some((zigzag_decode(u), n_size as usize))
}

/// Unmarshals `dst.len()` i64 values from `src` into `dst` and returns the
/// remaining tail of `src`. Go: UnmarshalVarInt64s.
///
/// Deviation from Go: on the first multi-byte varint the decode continues
/// from the current position (the already-decoded 1-byte prefix is kept)
/// using a word-at-a-time (SWAR) decoder instead of restarting the whole
/// array with a per-byte loop.
pub fn unmarshal_var_int64s<'a>(dst: &mut [i64], src: &'a [u8]) -> Result<&'a [u8], String> {
    if src.len() < dst.len() {
        return Err(format!(
            "too small len(src)={}; it must be bigger or equal to len(dst)={}",
            src.len(),
            dst.len()
        ));
    }
    let n = dst.len();
    // Fast path: all the varints are 1-byte, so the reads below stay
    // in-bounds thanks to the length check above.
    let mut i = 0usize;
    while i < n {
        let c = src[i];
        if c >= 0x80 {
            break;
        }
        dst[i] = ((c >> 1) as i8 ^ (((c << 7) as i8) >> 7)) as i64;
        i += 1;
    }
    if i == n {
        return Ok(&src[n..]);
    }
    // Mixed path: continue from element i at byte offset i.
    let mut idx = i;
    for out in dst[i..].iter_mut() {
        *out = zigzag_decode(read_var_uint64_fast(src, &mut idx)?);
    }
    Ok(&src[idx..])
}

/// Reads a single varint at `*idx` using an 8-byte SWAR window when
/// possible, advancing `*idx`. Falls back to the per-byte reader near the
/// end of `src` and for varints longer than 8 bytes.
#[inline]
fn read_var_uint64_fast(src: &[u8], idx: &mut usize) -> Result<u64, String> {
    let start = *idx;
    if let Some(window) = src.get(start..start + 8) {
        let w = u64::from_le_bytes(window.try_into().expect("window is 8 bytes"));
        let stop = !w & 0x8080_8080_8080_8080;
        if stop != 0 {
            let nbytes = ((stop.trailing_zeros() as usize) >> 3) + 1;
            let mut u = w & 0x7f;
            let mut k = 1;
            while k < nbytes {
                u |= ((w >> (8 * k)) & 0x7f) << (7 * k);
                k += 1;
            }
            *idx = start + nbytes;
            return Ok(u);
        }
        // Longer than 8 bytes: fall through to the per-byte reader.
    }
    read_var_uint64_slow(src, idx)
}

/// Reads a single varint value at `*idx`, advancing `*idx`. Shared body of
/// Go's unmarshalVarInt64sSlow / unmarshalVarUint64sSlow per-item logic.
#[inline]
fn read_var_uint64_slow(src: &[u8], idx: &mut usize) -> Result<u64, String> {
    if *idx >= src.len() {
        return Err("cannot unmarshal varint from empty data".to_string());
    }
    let c = src[*idx];
    *idx += 1;
    if c < 0x80 {
        // Fast path for 1 byte.
        return Ok(c as u64);
    }

    if *idx >= src.len() {
        return Err("unexpected end of encoded varint at byte 1".to_string());
    }
    let d = src[*idx];
    *idx += 1;
    if d < 0x80 {
        // Fast path for 2 bytes.
        return Ok((c & 0x7f) as u64 | (d as u64) << 7);
    }

    if *idx >= src.len() {
        return Err("unexpected end of encoded varint at byte 2".to_string());
    }
    let e = src[*idx];
    *idx += 1;
    if e < 0x80 {
        // Fast path for 3 bytes.
        return Ok((c & 0x7f) as u64 | ((d & 0x7f) as u64) << 7 | (e as u64) << (2 * 7));
    }

    let mut u = (c & 0x7f) as u64 | ((d & 0x7f) as u64) << 7 | ((e & 0x7f) as u64) << (2 * 7);

    // Slow path.
    let j = *idx;
    loop {
        if *idx >= src.len() {
            return Err("unexpected end of encoded varint".to_string());
        }
        let c = src[*idx];
        *idx += 1;
        if c < 0x80 {
            break;
        }
    }

    // These are the most common cases.
    let b = &src[j..*idx];
    u |= match b.len() {
        1 => (b[0] as u64) << (3 * 7),
        2 => ((b[0] & 0x7f) as u64) << (3 * 7) | (b[1] as u64) << (4 * 7),
        3 => {
            ((b[0] & 0x7f) as u64) << (3 * 7)
                | ((b[1] & 0x7f) as u64) << (4 * 7)
                | (b[2] as u64) << (5 * 7)
        }
        4 => {
            ((b[0] & 0x7f) as u64) << (3 * 7)
                | ((b[1] & 0x7f) as u64) << (4 * 7)
                | ((b[2] & 0x7f) as u64) << (5 * 7)
                | (b[3] as u64) << (6 * 7)
        }
        5 => {
            ((b[0] & 0x7f) as u64) << (3 * 7)
                | ((b[1] & 0x7f) as u64) << (4 * 7)
                | ((b[2] & 0x7f) as u64) << (5 * 7)
                | ((b[3] & 0x7f) as u64) << (6 * 7)
                | (b[4] as u64) << (7 * 7)
        }
        6 => {
            ((b[0] & 0x7f) as u64) << (3 * 7)
                | ((b[1] & 0x7f) as u64) << (4 * 7)
                | ((b[2] & 0x7f) as u64) << (5 * 7)
                | ((b[3] & 0x7f) as u64) << (6 * 7)
                | ((b[4] & 0x7f) as u64) << (7 * 7)
                | (b[5] as u64) << (8 * 7)
        }
        7 => {
            if b[6] > 1 {
                return Err("too big encoded varint".to_string());
            }
            ((b[0] & 0x7f) as u64) << (3 * 7)
                | ((b[1] & 0x7f) as u64) << (4 * 7)
                | ((b[2] & 0x7f) as u64) << (5 * 7)
                | ((b[3] & 0x7f) as u64) << (6 * 7)
                | ((b[4] & 0x7f) as u64) << (7 * 7)
                | ((b[5] & 0x7f) as u64) << (8 * 7)
                | 1 << (9 * 7)
        }
        n => {
            return Err(format!(
                "too long encoded varint; the maximum allowed length is 10 bytes; got {} bytes",
                n + 3
            ));
        }
    };
    Ok(u)
}

/// Appends varint-encoded `u` to `dst`. Go: MarshalVarUint64.
#[inline]
pub fn marshal_var_uint64(dst: &mut Vec<u8>, u: u64) {
    // The 1/2/3-byte fast paths in Go match the first cases of the slow path.
    append_var_uint64(dst, u);
}

/// Appends varint-encoded `us` to `dst`. Go: MarshalVarUint64s.
pub fn marshal_var_uint64s(dst: &mut Vec<u8>, us: &[u64]) {
    let dst_len = dst.len();
    for &u in us {
        if u >= (1 << 7) {
            dst.truncate(dst_len);
            marshal_var_uint64s_slow(dst, us);
            return;
        }
        dst.push(u as u8);
    }
}

/// Go: marshalVarUint64sSlow.
fn marshal_var_uint64s_slow(dst: &mut Vec<u8>, us: &[u64]) {
    for &u in us {
        append_var_uint64(dst, u);
    }
}

/// Returns unmarshaled u64 from `src` and its size in bytes, or `None` if
/// `src` doesn't contain a valid varint. Go: UnmarshalVarUint64.
#[inline]
pub fn unmarshal_var_uint64(src: &[u8]) -> Option<(u64, usize)> {
    if src.is_empty() {
        return None;
    }
    if src[0] < 0x80 {
        // Fast path for a single byte.
        return Some((src[0] as u64, 1));
    }
    if src.len() == 1 {
        return None;
    }
    if src[1] < 0x80 {
        // Fast path for two bytes.
        return Some(((src[0] & 0x7f) as u64 | (src[1] as u64) << 7, 2));
    }

    // Slow path for other number of bytes.
    let (u, n_size) = uvarint(src);
    if n_size <= 0 {
        return None;
    }
    Some((u, n_size as usize))
}

/// Unmarshals `dst.len()` u64 values from `src` into `dst` and returns the
/// remaining tail of `src`. Go: UnmarshalVarUint64s.
pub fn unmarshal_var_uint64s<'a>(dst: &mut [u64], src: &'a [u8]) -> Result<&'a [u8], String> {
    if src.len() < dst.len() {
        return Err(format!(
            "too small len(src)={}; it must be bigger or equal to len(dst)={}",
            src.len(),
            dst.len()
        ));
    }
    let n = dst.len();
    // Fast path: all the varints are 1-byte, so the reads below stay
    // in-bounds thanks to the length check above.
    let mut i = 0usize;
    while i < n {
        let c = src[i];
        if c >= 0x80 {
            break;
        }
        dst[i] = c as u64;
        i += 1;
    }
    if i == n {
        return Ok(&src[n..]);
    }
    // Mixed path: continue from element i at byte offset i.
    let mut idx = i;
    for out in dst[i..].iter_mut() {
        *out = read_var_uint64_fast(src, &mut idx)?;
    }
    Ok(&src[idx..])
}

/// Appends marshaled `v` to `dst`. Go: MarshalBool.
#[inline]
pub fn marshal_bool(dst: &mut Vec<u8>, v: bool) {
    dst.push(v as u8);
}

/// Unmarshals bool from `src`. Go: UnmarshalBool.
#[inline]
pub fn unmarshal_bool(src: &[u8]) -> bool {
    src[0] != 0
}

/// Appends varint-length-prefixed `b` to `dst`. Go: MarshalBytes.
pub fn marshal_bytes(dst: &mut Vec<u8>, b: &[u8]) {
    marshal_var_uint64(dst, b.len() as u64);
    dst.extend_from_slice(b);
}

/// Returns unmarshaled bytes from `src` and the total consumed size, or
/// `None` if `src` is malformed. Go: UnmarshalBytes.
pub fn unmarshal_bytes(src: &[u8]) -> Option<(&[u8], usize)> {
    let (n, n_size) = unmarshal_var_uint64(src)?;
    if n > (src.len() - n_size) as u64 {
        return None;
    }
    let start = n_size;
    let n_size = n_size + n as usize;
    Some((&src[start..n_size], n_size))
}
