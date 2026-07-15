//! A minimal Protocol Buffers reader/writer — just enough to encode a
//! Prometheus `ReadRequest` and decode a `ReadResponse`. Avoids a full
//! protobuf dependency for the small, fixed set of messages the `remote-read`
//! mode needs.

/// Appends a base-128 varint.
pub(crate) fn put_varint(dst: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        dst.push((v as u8) | 0x80);
        v >>= 7;
    }
    dst.push(v as u8);
}

fn put_tag(dst: &mut Vec<u8>, field: u32, wire: u32) {
    put_varint(dst, ((field << 3) | wire) as u64);
}

/// Appends an `int64` field (varint, two's-complement). Wire type 0.
pub(crate) fn put_int64(dst: &mut Vec<u8>, field: u32, v: i64) {
    put_tag(dst, field, 0);
    put_varint(dst, v as u64);
}

/// Appends a `string` field. Wire type 2.
pub(crate) fn put_string(dst: &mut Vec<u8>, field: u32, s: &str) {
    put_tag(dst, field, 2);
    put_varint(dst, s.len() as u64);
    dst.extend_from_slice(s.as_bytes());
}

/// Appends a length-delimited (embedded message / bytes) field. Wire type 2.
pub(crate) fn put_message(dst: &mut Vec<u8>, field: u32, msg: &[u8]) {
    put_tag(dst, field, 2);
    put_varint(dst, msg.len() as u64);
    dst.extend_from_slice(msg);
}

/// A cursor over an encoded protobuf message.
pub(crate) struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

/// One decoded field: its number and payload. (Wire type 5 / fixed32 is
/// transparently skipped by [`Reader::next`], since none of the Prometheus
/// remote-read messages use it.)
pub(crate) enum Field<'a> {
    Varint(u32, u64),
    Fixed64(u32, u64),
    LenDelim(u32, &'a [u8]),
}

impl<'a> Reader<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Reader<'a> {
        Reader { data, pos: 0 }
    }

    fn read_varint(&mut self) -> Option<u64> {
        let mut result: u64 = 0;
        let mut shift = 0;
        while self.pos < self.data.len() {
            let b = self.data[self.pos];
            self.pos += 1;
            result |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                return Some(result);
            }
            shift += 7;
            if shift >= 64 {
                return None;
            }
        }
        None
    }

    /// Reads the next field, or `None` at end / on malformed input.
    /// Fixed32 (wire type 5) fields are skipped transparently.
    pub(crate) fn next(&mut self) -> Option<Field<'a>> {
        loop {
            if self.pos >= self.data.len() {
                return None;
            }
            let key = self.read_varint()?;
            let field = (key >> 3) as u32;
            let wire = (key & 0x7) as u32;
            match wire {
                0 => return Some(Field::Varint(field, self.read_varint()?)),
                1 => {
                    if self.pos + 8 > self.data.len() {
                        return None;
                    }
                    let mut v = 0u64;
                    for i in 0..8 {
                        v |= (self.data[self.pos + i] as u64) << (8 * i);
                    }
                    self.pos += 8;
                    return Some(Field::Fixed64(field, v));
                }
                2 => {
                    let len = self.read_varint()? as usize;
                    if self.pos + len > self.data.len() {
                        return None;
                    }
                    let slice = &self.data[self.pos..self.pos + len];
                    self.pos += len;
                    return Some(Field::LenDelim(field, slice));
                }
                5 => {
                    // fixed32: skip 4 bytes and continue.
                    if self.pos + 4 > self.data.len() {
                        return None;
                    }
                    self.pos += 4;
                }
                _ => return None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_fields() {
        let mut buf = Vec::new();
        put_int64(&mut buf, 1, 42);
        put_string(&mut buf, 2, "hi");
        let mut r = Reader::new(&buf);
        match r.next().unwrap() {
            Field::Varint(1, v) => assert_eq!(v, 42),
            _ => panic!("expected varint"),
        }
        match r.next().unwrap() {
            Field::LenDelim(2, b) => assert_eq!(b, b"hi"),
            _ => panic!("expected len-delim"),
        }
        assert!(r.next().is_none());
    }

    #[test]
    fn decodes_fixed64_double() {
        let mut buf = Vec::new();
        // Manually encode a double (field 1, wire 1).
        buf.push((1 << 3) | 1);
        buf.extend_from_slice(&1.5f64.to_bits().to_le_bytes());
        let mut r = Reader::new(&buf);
        match r.next().unwrap() {
            Field::Fixed64(1, bits) => assert_eq!(f64::from_bits(bits), 1.5),
            _ => panic!("expected fixed64"),
        }
    }
}
