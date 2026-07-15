//! Port of Go lib/encoding/int_test.go (moved out of src/int.rs to keep
//! the module under the 800-line guideline; everything tested is public API).
use esm_encoding::*;

// Port of TestMarshalUnmarshalUint16.
#[test]
fn test_marshal_unmarshal_uint16() {
    check_u16(0);
    check_u16(1);
    check_u16(u16::MAX);
    check_u16((1 << 15) + 1);
    check_u16((1 << 15) - 1);
    check_u16(1 << 15);

    for i in 0..10_000u16 {
        check_u16(i);
    }
}

fn check_u16(u: u16) {
    let mut b = Vec::new();
    marshal_uint16(&mut b, u);
    assert_eq!(b.len(), 2, "unexpected b length for u={u}");
    assert_eq!(unmarshal_uint16(&b), u, "unexpected uNew from b={b:x?}");

    let mut b1 = vec![1u8, 2, 3];
    marshal_uint16(&mut b1, u);
    assert_eq!(&b1[..3], &[1, 2, 3], "unexpected prefix for u={u}");
    assert_eq!(&b1[3..], &b[..], "unexpected b for u={u}");
}

// Port of TestMarshalUnmarshalUint32.
#[test]
fn test_marshal_unmarshal_uint32() {
    check_u32(0);
    check_u32(1);
    check_u32(u32::MAX);
    check_u32((1 << 31) + 1);
    check_u32((1 << 31) - 1);
    check_u32(1 << 31);

    for i in 0..10_000u32 {
        check_u32(i);
    }
}

fn check_u32(u: u32) {
    let mut b = Vec::new();
    marshal_uint32(&mut b, u);
    assert_eq!(b.len(), 4, "unexpected b length for u={u}");
    assert_eq!(unmarshal_uint32(&b), u, "unexpected uNew from b={b:x?}");

    let mut b1 = vec![1u8, 2, 3];
    marshal_uint32(&mut b1, u);
    assert_eq!(&b1[..3], &[1, 2, 3], "unexpected prefix for u={u}");
    assert_eq!(&b1[3..], &b[..], "unexpected b for u={u}");
}

// Port of TestMarshalUnmarshalUint64.
#[test]
fn test_marshal_unmarshal_uint64() {
    check_u64(0);
    check_u64(1);
    check_u64(u64::MAX);
    check_u64((1 << 63) + 1);
    check_u64((1 << 63) - 1);
    check_u64(1 << 63);

    for i in 0..10_000u64 {
        check_u64(i);
    }
}

fn check_u64(u: u64) {
    let mut b = Vec::new();
    marshal_uint64(&mut b, u);
    assert_eq!(b.len(), 8, "unexpected b length for u={u}");
    assert_eq!(unmarshal_uint64(&b), u, "unexpected uNew from b={b:x?}");

    let mut b1 = vec![1u8, 2, 3];
    marshal_uint64(&mut b1, u);
    assert_eq!(&b1[..3], &[1, 2, 3], "unexpected prefix for u={u}");
    assert_eq!(&b1[3..], &b[..], "unexpected b for u={u}");
}

// Port of TestMarshalUnmarshalInt16.
#[test]
fn test_marshal_unmarshal_int16() {
    check_i16(0);
    check_i16(1);
    check_i16(-1);
    check_i16(i16::MIN);
    check_i16(i16::MIN + 1);
    check_i16(i16::MAX);

    for i in 0..10_000i16 {
        check_i16(i);
        check_i16(-i);
    }
}

fn check_i16(v: i16) {
    let mut b = Vec::new();
    marshal_int16(&mut b, v);
    assert_eq!(b.len(), 2, "unexpected b length for v={v}");
    assert_eq!(unmarshal_int16(&b), v, "unexpected vNew from b={b:x?}");

    let mut b1 = vec![1u8, 2, 3];
    marshal_int16(&mut b1, v);
    assert_eq!(&b1[..3], &[1, 2, 3], "unexpected prefix for v={v}");
    assert_eq!(&b1[3..], &b[..], "unexpected b for v={v}");
}

// Port of TestMarshalUnmarshalInt64.
#[test]
fn test_marshal_unmarshal_int64() {
    check_i64(0);
    check_i64(1);
    check_i64(-1);
    check_i64(i64::MIN);
    check_i64(i64::MIN + 1);
    check_i64(i64::MAX);

    for i in 0..10_000i64 {
        check_i64(i);
        check_i64(-i);
    }
}

fn check_i64(v: i64) {
    let mut b = Vec::new();
    marshal_int64(&mut b, v);
    assert_eq!(b.len(), 8, "unexpected b length for v={v}");
    assert_eq!(unmarshal_int64(&b), v, "unexpected vNew from b={b:x?}");

    let mut b1 = vec![1u8, 2, 3];
    marshal_int64(&mut b1, v);
    assert_eq!(&b1[..3], &[1, 2, 3], "unexpected prefix for v={v}");
    assert_eq!(&b1[3..], &b[..], "unexpected b for v={v}");
}

// Port of TestMarshalUnmarshalVarInt64.
#[test]
fn test_marshal_unmarshal_var_int64() {
    check_var_i64(0);
    check_var_i64(1);
    check_var_i64(-1);
    check_var_i64((1 << 6) - 1);
    check_var_i64(-(1 << 6) + 1);
    check_var_i64(1 << 6);
    check_var_i64(-(1 << 6));
    check_var_i64((1 << 13) - 1);
    check_var_i64(-(1 << 13) + 1);
    check_var_i64(1 << 13);
    check_var_i64((1 << 13) + 1);
    check_var_i64(-(1 << 13));
    check_var_i64(i64::MIN);
    check_var_i64(i64::MIN + 1);
    check_var_i64(i64::MAX);

    for i in 0..10_000i64 {
        check_var_i64(i);
        check_var_i64(-i);
        check_var_i64(i << 8);
        check_var_i64(-i << 8);
        check_var_i64(i << 16);
        check_var_i64(-i << 16);
        check_var_i64(i << 23);
        check_var_i64(-i << 23);
        check_var_i64(i << 33);
        check_var_i64(-i << 33);
        check_var_i64(i << 35);
        check_var_i64(-i << 35);
        check_var_i64(i << 43);
        check_var_i64(-i << 43);
        check_var_i64(i << 53);
        check_var_i64(-i << 53);
    }
}

fn check_var_i64(v: i64) {
    let mut b = Vec::new();
    marshal_var_int64(&mut b, v);
    let (v_new, n_size) =
        unmarshal_var_int64(&b).unwrap_or_else(|| panic!("cannot unmarshal v={v} from {b:x?}"));
    assert_eq!(v_new, v, "unexpected vNew from b={b:x?}");
    assert_eq!(
        n_size,
        b.len(),
        "unexpected data left after unmarshaling v={v}"
    );

    let mut b1 = vec![1u8, 2, 3];
    marshal_var_int64(&mut b1, v);
    assert_eq!(&b1[..3], &[1, 2, 3], "unexpected prefix for v={v}");
    assert_eq!(&b1[3..], &b[..], "unexpected b for v={v}");

    // Also verify the batch variants roundtrip. Note: bytes may legally
    // differ from marshal_var_int64 (e.g. v=-64 is 2 bytes there but
    // 1 byte here), exactly as in Go.
    let mut bs = Vec::new();
    marshal_var_int64s(&mut bs, &[v]);
    let mut out = [0i64; 1];
    let tail = unmarshal_var_int64s(&mut out, &bs).unwrap();
    assert!(tail.is_empty());
    assert_eq!(out[0], v, "UnmarshalVarInt64s mismatch for v={v}");
}

// Port of TestMarshalUnmarshalVarUint64.
#[test]
fn test_marshal_unmarshal_var_uint64() {
    check_var_u64(0);
    check_var_u64(1);
    check_var_i64((1 << 6) - 1);
    check_var_i64(1 << 6);
    check_var_i64((1 << 13) - 1);
    check_var_i64(1 << 13);
    check_var_i64((1 << 13) + 1);
    check_var_u64((1 << 63) - 1);
    check_var_u64(u64::MAX);

    for i in 0..1024u64 {
        check_var_u64(i);
        check_var_u64(i << 8);
        check_var_u64(i << 16);
        check_var_u64(i << 23);
        check_var_u64(i << 33);
        check_var_u64(i << 35);
        check_var_u64(i << 41);
        check_var_u64(i << 49);
        check_var_u64(i << 54);
    }
}

fn check_var_u64(u: u64) {
    let mut b = Vec::new();
    marshal_var_uint64(&mut b, u);
    let (u_new, n_size) =
        unmarshal_var_uint64(&b).unwrap_or_else(|| panic!("cannot unmarshal u={u} from {b:x?}"));
    assert_eq!(u_new, u, "unexpected uNew from b={b:x?}");
    assert_eq!(
        n_size,
        b.len(),
        "unexpected data left after unmarshaling u={u}"
    );

    let mut b1 = vec![1u8, 2, 3];
    marshal_var_uint64(&mut b1, u);
    assert_eq!(&b1[..3], &[1, 2, 3], "unexpected prefix for u={u}");
    assert_eq!(&b1[3..], &b[..], "unexpected b for u={u}");

    // Also verify the batch variants against the single-value encoding.
    let mut bs = Vec::new();
    marshal_var_uint64s(&mut bs, &[u]);
    assert_eq!(bs, b, "MarshalVarUint64s mismatch for u={u}");
    let mut out = [0u64; 1];
    let tail = unmarshal_var_uint64s(&mut out, &bs).unwrap();
    assert!(tail.is_empty());
    assert_eq!(out[0], u, "UnmarshalVarUint64s mismatch for u={u}");
}

// Port of TestUnmarshalBytesOverflow.
#[test]
fn test_unmarshal_bytes_overflow() {
    let poison_varint = [0xffu8, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01];
    let result = unmarshal_bytes(&poison_varint);
    assert!(
        result.is_none(),
        "expected error from overflow input, got {result:?}"
    );
}

// Port of TestMarshalUnmarshalBytes.
#[test]
fn test_marshal_unmarshal_bytes() {
    check_bytes("");
    check_bytes("x");
    check_bytes("xy");

    let mut bb = String::new();
    for i in 0..100 {
        bb.push_str(&format!(" {i} "));
        let s = bb.clone();
        check_bytes(&s);
    }
}

fn check_bytes(s: &str) {
    let mut b = Vec::new();
    marshal_bytes(&mut b, s.as_bytes());
    let (b_new, n_size) =
        unmarshal_bytes(&b).unwrap_or_else(|| panic!("cannot unmarshal s={s:?} from {b:x?}"));
    assert_eq!(b_new, s.as_bytes(), "unexpected sNew from b={b:x?}");
    assert_eq!(
        n_size,
        b.len(),
        "unexpected data left after unmarshaling s={s:?}"
    );

    let prefix = b"abcde";
    let mut b1 = prefix.to_vec();
    marshal_bytes(&mut b1, s.as_bytes());
    assert_eq!(&b1[..prefix.len()], prefix, "unexpected prefix for s={s:?}");
    assert_eq!(&b1[prefix.len()..], &b[..], "unexpected b for s={s:?}");
}

#[test]
fn test_marshal_unmarshal_bool() {
    for v in [false, true] {
        let mut b = Vec::new();
        marshal_bool(&mut b, v);
        assert_eq!(b.len(), 1);
        assert_eq!(unmarshal_bool(&b), v);
    }
}
