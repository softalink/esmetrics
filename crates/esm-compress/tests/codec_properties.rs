//! Property tests for the time-series codec.
//!
//! `marshal_int64_array` followed by `unmarshal_int64_array` must be the
//! identity for any non-empty i64 sequence at precision_bits=64. At
//! lower precision_bits, the recovered values must equal the
//! quantize-then-encode round-trip.

#![allow(clippy::unwrap_used)]

use esm_compress::timeseries::{marshal_int64_array, unmarshal_int64_array};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig { cases: 200, ..ProptestConfig::default() })]

    /// Lossless mode: round-trip preserves every value exactly.
    #[test]
    fn lossless_roundtrip(src in prop::collection::vec(any::<i64>(), 1..200)) {
        let mut buf = Vec::new();
        let r = marshal_int64_array(&mut buf, &src, 64).unwrap();
        let mut out = Vec::new();
        unmarshal_int64_array(&mut out, &buf, r.marshal_type, r.first_value, src.len()).unwrap();
        prop_assert_eq!(out, src);
    }

    /// Lossy precision_bits=16 stays internally consistent: decoding
    /// produces the same bytes as encoding-then-decoding the second time.
    #[test]
    fn lossy_idempotent(src in prop::collection::vec(any::<i64>(), 1..200)) {
        let mut buf1 = Vec::new();
        let r1 = marshal_int64_array(&mut buf1, &src, 16).unwrap();
        let mut out1 = Vec::new();
        unmarshal_int64_array(&mut out1, &buf1, r1.marshal_type, r1.first_value, src.len()).unwrap();

        let mut buf2 = Vec::new();
        let r2 = marshal_int64_array(&mut buf2, &out1, 16).unwrap();
        let mut out2 = Vec::new();
        unmarshal_int64_array(&mut out2, &buf2, r2.marshal_type, r2.first_value, src.len()).unwrap();

        prop_assert_eq!(out1, out2);
    }
}
