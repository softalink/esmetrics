//! Prometheus XOR (Gorilla) chunk decoding + the remote-read chunked-frame
//! reader, for `remote-read --remote-read-use-stream` (STREAMED_XOR_CHUNKS).
//! Ports `prometheus/tsdb/chunkenc/{bstream,xor}.go` (the decode path) and
//! `prometheus/storage/remote/chunked.go`'s frame reader.
//!
//! The fast/fallback bit-read pairs in upstream (`readBitFast` then
//! `readBit`) are pure optimizations that both fall through to the
//! buffer-loading read; this port uses the loading reads directly.

use std::io::Read;

const CHUNK_HEADER_SIZE: usize = 2;

/// A bit reader over a byte slice. Ports `bstreamReader`.
struct BstreamReader<'a> {
    stream: &'a [u8],
    stream_offset: usize,
    buffer: u64,
    valid: u8,
    last: u8,
}

impl<'a> BstreamReader<'a> {
    fn new(b: &'a [u8]) -> BstreamReader<'a> {
        let last = if b.is_empty() { 0 } else { b[b.len() - 1] };
        BstreamReader {
            stream: b,
            stream_offset: 0,
            buffer: 0,
            valid: 0,
            last,
        }
    }

    fn read_bit(&mut self) -> Option<bool> {
        if self.valid == 0 && !self.load_next_buffer(1) {
            return None;
        }
        if self.valid == 0 {
            return None;
        }
        self.valid -= 1;
        Some((self.buffer & (1u64 << self.valid)) != 0)
    }

    fn read_bits(&mut self, nbits: u8) -> Option<u64> {
        if self.valid == 0 && !self.load_next_buffer(nbits) {
            return None;
        }
        if nbits <= self.valid {
            return self.read_bits_fast(nbits);
        }
        // Read the remaining valid bits, then part of the next buffer.
        let bitmask = (1u64 << self.valid) - 1;
        let nbits = nbits - self.valid;
        let mut v = (self.buffer & bitmask) << nbits;
        self.valid = 0;
        if !self.load_next_buffer(nbits) {
            return None;
        }
        let bitmask2 = (1u64 << nbits) - 1;
        v |= (self.buffer >> (self.valid - nbits)) & bitmask2;
        self.valid -= nbits;
        Some(v)
    }

    fn read_bits_fast(&mut self, nbits: u8) -> Option<u64> {
        if nbits > self.valid {
            return None;
        }
        let bitmask = if nbits >= 64 {
            u64::MAX
        } else {
            (1u64 << nbits) - 1
        };
        self.valid -= nbits;
        Some((self.buffer >> self.valid) & bitmask)
    }

    fn read_byte(&mut self) -> Option<u8> {
        self.read_bits(8).map(|v| v as u8)
    }

    fn load_next_buffer(&mut self, nbits: u8) -> bool {
        if self.stream_offset >= self.stream.len() {
            return false;
        }
        if self.stream_offset + 8 < self.stream.len() {
            self.buffer = u64::from_be_bytes(
                self.stream[self.stream_offset..self.stream_offset + 8]
                    .try_into()
                    .unwrap(),
            );
            self.stream_offset += 8;
            self.valid = 64;
            return true;
        }
        let mut nbytes = (nbits / 8 + 1) as usize;
        if self.stream_offset + nbytes > self.stream.len() {
            nbytes = self.stream.len() - self.stream_offset;
        }
        let mut buffer = 0u64;
        let mut skip = 0;
        if self.stream_offset + nbytes == self.stream.len() {
            // The last byte may be concurrently written upstream; use the copy.
            buffer |= self.last as u64;
            skip = 1;
        }
        for i in 0..(nbytes - skip) {
            buffer |= (self.stream[self.stream_offset + i] as u64) << (8 * (nbytes - i - 1));
        }
        self.buffer = buffer;
        self.stream_offset += nbytes;
        self.valid = (nbytes * 8) as u8;
        true
    }

    /// Reads a base-128 unsigned varint (byte-aligned). Ports
    /// `binary.ReadUvarint` over the bit reader.
    fn read_uvarint(&mut self) -> Option<u64> {
        let mut x: u64 = 0;
        let mut s: u32 = 0;
        for _ in 0..10 {
            let b = self.read_byte()?;
            if b < 0x80 {
                return Some(x | (b as u64) << s);
            }
            x |= ((b & 0x7f) as u64) << s;
            s += 7;
        }
        None
    }

    /// Reads a zig-zag signed varint. Ports `binary.ReadVarint`.
    fn read_varint(&mut self) -> Option<i64> {
        let ux = self.read_uvarint()?;
        let x = (ux >> 1) as i64;
        Some(if ux & 1 != 0 { !x } else { x })
    }
}

/// Iterates the samples of one XOR-encoded chunk. Ports `xorIterator`.
struct XorIterator<'a> {
    br: BstreamReader<'a>,
    num_total: u16,
    num_read: u16,
    t: i64,
    val: f64,
    leading: u8,
    trailing: u8,
    t_delta: u64,
}

impl<'a> XorIterator<'a> {
    fn new(chunk: &'a [u8]) -> Option<XorIterator<'a>> {
        if chunk.len() < CHUNK_HEADER_SIZE {
            return None;
        }
        let num_total = u16::from_be_bytes([chunk[0], chunk[1]]);
        Some(XorIterator {
            br: BstreamReader::new(&chunk[CHUNK_HEADER_SIZE..]),
            num_total,
            num_read: 0,
            t: i64::MIN,
            val: 0.0,
            leading: 0,
            trailing: 0,
            t_delta: 0,
        })
    }

    /// Returns the next `(timestamp_ms, value)`, or `None` at the end. Ports
    /// `xorIterator.Next`.
    fn next(&mut self) -> Option<(i64, f64)> {
        if self.num_read == self.num_total {
            return None;
        }
        if self.num_read == 0 {
            let t = self.br.read_varint()?;
            let v = self.br.read_bits(64)?;
            self.t = t;
            self.val = f64::from_bits(v);
            self.num_read += 1;
            return Some((self.t, self.val));
        }
        if self.num_read == 1 {
            self.t_delta = self.br.read_uvarint()?;
            self.t += self.t_delta as i64;
            return self.read_value();
        }

        // Delta-of-delta bucket selector.
        let mut d: u8 = 0;
        for _ in 0..4 {
            d <<= 1;
            if !self.br.read_bit()? {
                break;
            }
            d |= 1;
        }
        let mut sz: u8 = 0;
        let mut dod: i64 = 0;
        match d {
            0b0 => {}
            0b10 => sz = 14,
            0b110 => sz = 17,
            0b1110 => sz = 20,
            0b1111 => dod = self.br.read_bits(64)? as i64,
            _ => return None,
        }
        if sz != 0 {
            let mut bits = self.br.read_bits(sz)?;
            if bits > (1u64 << (sz - 1)) {
                // Sign-extend: wraps to a large u64 that reinterprets as a
                // negative i64 (upstream relies on Go's uint64 wraparound).
                bits = bits.wrapping_sub(1u64 << sz);
            }
            dod = bits as i64;
        }
        self.t_delta = (self.t_delta as i64 + dod) as u64;
        self.t += self.t_delta as i64;
        self.read_value()
    }

    fn read_value(&mut self) -> Option<(i64, f64)> {
        xor_read(
            &mut self.br,
            &mut self.val,
            &mut self.leading,
            &mut self.trailing,
        )?;
        self.num_read += 1;
        Some((self.t, self.val))
    }
}

/// Ports `xorRead`: XOR-decodes the next float value in place.
fn xor_read(
    br: &mut BstreamReader,
    value: &mut f64,
    leading: &mut u8,
    trailing: &mut u8,
) -> Option<()> {
    if !br.read_bit()? {
        return Some(()); // control bit 0: value unchanged
    }
    let m_bits: u8;
    if !br.read_bit()? {
        // control bits 10: reuse the previous leading/trailing window.
        m_bits = 64 - *leading - *trailing;
    } else {
        // control bits 11: new leading + meaningful-bit count.
        let new_leading = br.read_bits(5)? as u8;
        let mut mb = br.read_bits(6)? as u8;
        if mb == 0 {
            mb = 64;
        }
        m_bits = mb;
        *leading = new_leading;
        *trailing = 64 - new_leading - m_bits;
    }
    let bits = br.read_bits(m_bits)?;
    let mut vbits = value.to_bits();
    vbits ^= bits << *trailing;
    *value = f64::from_bits(vbits);
    Some(())
}

/// Decodes every `(timestamp_ms, value)` sample in one XOR chunk.
pub(crate) fn decode_xor_chunk(chunk: &[u8]) -> Result<Vec<(i64, f64)>, String> {
    let Some(mut it) = XorIterator::new(chunk) else {
        return Err("XOR chunk shorter than its 2-byte header".to_string());
    };
    let mut out = Vec::with_capacity(it.num_total as usize);
    for _ in 0..it.num_total {
        match it.next() {
            Some(sample) => out.push(sample),
            None => return Err("truncated XOR chunk".to_string()),
        }
    }
    Ok(out)
}

// ---- ChunkedReader frame reader ----

/// Reads one chunked frame from `r`: `uvarint(size)` + big-endian
/// `crc32c(data)` + `size` bytes. Returns `Ok(None)` at a clean end of
/// stream. Ports `remote.ChunkedReader.Next`.
pub(crate) fn read_frame<R: Read>(r: &mut R, size_limit: u64) -> Result<Option<Vec<u8>>, String> {
    let size = match read_uvarint_io(r)? {
        Some(s) => s,
        None => return Ok(None),
    };
    if size > size_limit {
        return Err(format!(
            "chunked frame size {size} exceeds the limit {size_limit}"
        ));
    }
    let mut crc_buf = [0u8; 4];
    read_full(r, &mut crc_buf).map_err(|e| format!("cannot read frame crc: {e}"))?;
    let expected = u32::from_be_bytes(crc_buf);
    let mut data = vec![0u8; size as usize];
    read_full(r, &mut data).map_err(|e| format!("cannot read frame: {e}"))?;
    if crc32c(&data) != expected {
        return Err("chunked frame checksum mismatch".to_string());
    }
    Ok(Some(data))
}

/// Reads a base-128 uvarint from a byte stream. Returns `Ok(None)` if the
/// stream ends cleanly before the first byte (a normal end-of-stream).
fn read_uvarint_io<R: Read>(r: &mut R) -> Result<Option<u64>, String> {
    let mut x: u64 = 0;
    let mut s: u32 = 0;
    for i in 0..10 {
        let mut b = [0u8; 1];
        match r.read(&mut b) {
            Ok(0) => {
                if i == 0 {
                    return Ok(None); // clean EOF at a frame boundary
                }
                return Err("unexpected EOF in frame size".to_string());
            }
            Ok(_) => {}
            Err(e) => return Err(format!("read error: {e}")),
        }
        if b[0] < 0x80 {
            return Ok(Some(x | (b[0] as u64) << s));
        }
        x |= ((b[0] & 0x7f) as u64) << s;
        s += 7;
    }
    Err("uvarint overflow".to_string())
}

fn read_full<R: Read>(r: &mut R, buf: &mut [u8]) -> std::io::Result<()> {
    let mut off = 0;
    while off < buf.len() {
        let n = r.read(&mut buf[off..])?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "eof",
            ));
        }
        off += n;
    }
    Ok(())
}

/// CRC-32/Castagnoli (crc32c) over `data`, matching the remote-read frame
/// checksum (`crc32.MakeTable(crc32.Castagnoli)`).
fn crc32c(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32c_known_vectors() {
        // Standard crc32c test vectors.
        assert_eq!(crc32c(b""), 0x0000_0000);
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn read_frame_round_trip() {
        // Build one frame: uvarint(len) + BE crc32c + data.
        let data = b"hello world payload".to_vec();
        let mut frame = Vec::new();
        // uvarint length
        let mut n = data.len() as u64;
        while n >= 0x80 {
            frame.push((n as u8) | 0x80);
            n >>= 7;
        }
        frame.push(n as u8);
        frame.extend_from_slice(&crc32c(&data).to_be_bytes());
        frame.extend_from_slice(&data);

        let mut cur = std::io::Cursor::new(frame);
        let got = read_frame(&mut cur, 1 << 20).unwrap().unwrap();
        assert_eq!(got, data);
        // Clean EOF afterwards.
        assert!(read_frame(&mut cur, 1 << 20).unwrap().is_none());
    }

    #[test]
    fn read_frame_detects_corruption() {
        let data = vec![1u8, 2, 3];
        let mut frame = vec![data.len() as u8];
        frame.extend_from_slice(&(crc32c(&data) ^ 1).to_be_bytes()); // wrong crc
        frame.extend_from_slice(&data);
        let mut cur = std::io::Cursor::new(frame);
        assert!(read_frame(&mut cur, 1 << 20).is_err());
    }

    /// Encodes a single-sample XOR chunk by hand and decodes it: the first
    /// sample is `varint(t)` + `64-bit float bits`, byte-aligned.
    #[test]
    fn decode_single_sample_chunk() {
        let t: i64 = 1000;
        let v: f64 = 42.5;
        let mut chunk = vec![0u8, 1]; // num_total = 1 (big-endian u16)
                                      // zig-zag varint of t
        let mut uz = ((t << 1) ^ (t >> 63)) as u64;
        while uz >= 0x80 {
            chunk.push((uz as u8) | 0x80);
            uz >>= 7;
        }
        chunk.push(uz as u8);
        // 64-bit value, big-endian (the bit stream writes MSB-first).
        chunk.extend_from_slice(&v.to_bits().to_be_bytes());

        let samples = decode_xor_chunk(&chunk).unwrap();
        assert_eq!(samples, vec![(1000, 42.5)]);
    }

    /// A 3-sample chunk with constant timestamp delta and constant value,
    /// which exercises the second-sample (uvarint delta) path, the
    /// delta-of-delta `d = 0` bucket, and the XOR "value unchanged" control
    /// bit — all hand-encodable.
    #[test]
    fn decode_three_constant_samples() {
        let t0: i64 = 5000;
        let v: f64 = 7.25;
        let delta: u64 = 15; // t1 = 5015, t2 = 5030

        let mut chunk = vec![0u8, 3]; // num_total = 3
                                      // sample 0: zig-zag varint(t0) + 64-bit value.
        let mut uz = ((t0 << 1) ^ (t0 >> 63)) as u64;
        while uz >= 0x80 {
            chunk.push((uz as u8) | 0x80);
            uz >>= 7;
        }
        chunk.push(uz as u8);
        chunk.extend_from_slice(&v.to_bits().to_be_bytes());
        // sample 1: uvarint(delta) (byte-aligned).
        let mut ud = delta;
        while ud >= 0x80 {
            chunk.push((ud as u8) | 0x80);
            ud >>= 7;
        }
        chunk.push(ud as u8);
        // Continuing bitstream (MSB-first), all zero bits:
        //   sample1 xor control '0' (unchanged value)
        //   sample2 dod first bit '0' (d = 0 → dod = 0)
        //   sample2 xor control '0' (unchanged value)
        chunk.push(0x00);

        let samples = decode_xor_chunk(&chunk).unwrap();
        assert_eq!(samples, vec![(5000, 7.25), (5015, 7.25), (5030, 7.25)]);
    }

    /// Full round-trip against a hand-ported XOR encoder (ports
    /// `xorAppender.Append`/`xorWrite`), exercising every delta-of-delta
    /// bucket and value-change path.
    #[test]
    fn encode_decode_round_trip() {
        let samples: Vec<(i64, f64)> = vec![
            (1_600_000_000_000, 1.0),
            (1_600_000_015_000, 1.5),   // dod 0, value change
            (1_600_000_030_000, 1.5),   // dod 0, value unchanged
            (1_600_000_045_100, 2.0),   // dod +100 (14-bit)
            (1_600_000_060_000, -3.25), // dod -200
            (1_600_000_200_000, 1e6),   // large dod (>20-bit)
            (1_600_000_200_001, 1e6),   // tiny delta
            (1_600_000_400_000, f64::from_bits(0x4059_0000_0000_0001)),
        ];
        let chunk = encode_xor_chunk(&samples);
        let decoded = decode_xor_chunk(&chunk).unwrap();
        assert_eq!(decoded, samples);
    }

    struct BWriter {
        stream: Vec<u8>,
        count: u8,
    }

    impl BWriter {
        fn write_bit(&mut self, bit: bool) {
            if self.count == 0 {
                self.stream.push(0);
                self.count = 8;
            }
            let i = self.stream.len() - 1;
            if bit {
                self.stream[i] |= 1 << (self.count - 1);
            }
            self.count -= 1;
        }
        fn write_byte(&mut self, byt: u8) {
            if self.count == 0 {
                self.stream.push(byt);
                return;
            }
            let i = self.stream.len() - 1;
            self.stream[i] |= byt >> (8 - self.count);
            self.stream.push(byt << self.count);
        }
        fn write_bits(&mut self, mut u: u64, mut nbits: u32) {
            u <<= 64 - nbits;
            while nbits >= 8 {
                self.write_byte((u >> 56) as u8);
                u <<= 8;
                nbits -= 8;
            }
            while nbits > 0 {
                self.write_bit((u >> 63) == 1);
                u <<= 1;
                nbits -= 1;
            }
        }
        fn write_uvarint(&mut self, mut v: u64) {
            while v >= 0x80 {
                self.write_byte((v as u8) | 0x80);
                v >>= 7;
            }
            self.write_byte(v as u8);
        }
        fn write_varint(&mut self, v: i64) {
            self.write_uvarint(((v << 1) ^ (v >> 63)) as u64);
        }
    }

    fn bit_range(x: i64, nbits: u32) -> bool {
        -(1i64 << (nbits - 1)) <= x && x < (1i64 << (nbits - 1))
    }

    fn xor_write(w: &mut BWriter, new_v: f64, cur_v: f64, leading: &mut u8, trailing: &mut u8) {
        let delta = new_v.to_bits() ^ cur_v.to_bits();
        if delta == 0 {
            w.write_bit(false);
            return;
        }
        w.write_bit(true);
        let mut new_leading = delta.leading_zeros() as u8;
        let new_trailing = delta.trailing_zeros() as u8;
        if new_leading >= 32 {
            new_leading = 31;
        }
        if *leading != 0xff && new_leading >= *leading && new_trailing >= *trailing {
            w.write_bit(false);
            w.write_bits(delta >> *trailing, 64 - *leading as u32 - *trailing as u32);
            return;
        }
        *leading = new_leading;
        *trailing = new_trailing;
        w.write_bit(true);
        w.write_bits(new_leading as u64, 5);
        let sigbits = 64 - new_leading - new_trailing;
        w.write_bits(sigbits as u64, 6);
        w.write_bits(delta >> new_trailing, sigbits as u32);
    }

    fn encode_xor_chunk(samples: &[(i64, f64)]) -> Vec<u8> {
        let mut w = BWriter {
            stream: vec![0, 0],
            count: 0,
        };
        let (mut prev_t, mut prev_v, mut prev_tdelta) = (0i64, 0f64, 0u64);
        let (mut leading, mut trailing) = (0xffu8, 0u8);
        for (i, &(t, v)) in samples.iter().enumerate() {
            match i {
                0 => {
                    w.write_varint(t);
                    w.write_bits(v.to_bits(), 64);
                }
                1 => {
                    let tdelta = (t - prev_t) as u64;
                    w.write_uvarint(tdelta);
                    xor_write(&mut w, v, prev_v, &mut leading, &mut trailing);
                    prev_tdelta = tdelta;
                }
                _ => {
                    let tdelta = (t - prev_t) as u64;
                    let dod = tdelta as i64 - prev_tdelta as i64;
                    if dod == 0 {
                        w.write_bit(false);
                    } else if bit_range(dod, 14) {
                        w.write_byte(0b10 << 6 | ((dod >> 8) as u8 & 0x3f));
                        w.write_byte(dod as u8);
                    } else if bit_range(dod, 17) {
                        w.write_bits(0b110, 3);
                        w.write_bits(dod as u64, 17);
                    } else if bit_range(dod, 20) {
                        w.write_bits(0b1110, 4);
                        w.write_bits(dod as u64, 20);
                    } else {
                        w.write_bits(0b1111, 4);
                        w.write_bits(dod as u64, 64);
                    }
                    xor_write(&mut w, v, prev_v, &mut leading, &mut trailing);
                    prev_tdelta = tdelta;
                }
            }
            prev_t = t;
            prev_v = v;
        }
        let n = samples.len() as u16;
        w.stream[0] = (n >> 8) as u8;
        w.stream[1] = n as u8;
        w.stream
    }
}
