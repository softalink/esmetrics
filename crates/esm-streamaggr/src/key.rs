//! In-memory series-key encoding.
//!
//! Replaces upstream's `promutil.LabelsCompressor` (a process-global label
//! dictionary). Since the key never leaves the process — it is only used as
//! a `HashMap` key to group samples during one aggregation interval — a
//! plain round-tripping encoding is sufficient and matches the porting
//! rule that on-disk/byte-format compatibility is a non-goal.
//!
//! Layout of a full key, ported behaviourally from `compressLabels`:
//!
//! ```text
//! uvarint(len(output_enc)) ++ output_enc ++ input_enc
//! ```
//!
//! where each `*_enc` is a sequence of `uvarint(len(name)) name
//! uvarint(len(value)) value` label records. [`split_key`] recovers the
//! `(input_key, output_key)` halves (ports `getInputOutputKey`), and
//! [`decode_labels`] recovers the label set from an `*_enc` slice (ports
//! `decompressLabels`).

use crate::Label;

fn put_uvarint(dst: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        dst.push((v as u8) | 0x80);
        v >>= 7;
    }
    dst.push(v as u8);
}

/// Reads a base-128 varint from the front of `src`, returning `(value,
/// bytes_read)`. Returns `(0, 0)` if `src` is truncated.
fn get_uvarint(src: &[u8]) -> (u64, usize) {
    let mut result: u64 = 0;
    let mut shift = 0;
    for (i, &b) in src.iter().enumerate() {
        result |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return (result, i + 1);
        }
        shift += 7;
        if shift >= 64 {
            break;
        }
    }
    (0, 0)
}

fn encode_labels(dst: &mut Vec<u8>, labels: &[Label]) {
    for l in labels {
        put_uvarint(dst, l.name.len() as u64);
        dst.extend_from_slice(l.name.as_bytes());
        put_uvarint(dst, l.value.len() as u64);
        dst.extend_from_slice(l.value.as_bytes());
    }
}

/// Encodes a full label set into a plain (non-split) key blob. Used by the
/// standalone [`crate::Deduplicator`], which groups by the whole label set.
/// Ports the `lc.Compress(labels)` call in `deduplicator.go`.
pub(crate) fn encode_plain(labels: &[Label]) -> Vec<u8> {
    let mut dst = Vec::new();
    encode_labels(&mut dst, labels);
    dst
}

/// Decodes a label-record slice (an `*_enc` blob produced by
/// [`compress_labels`]) back into an owned label vector. Ports
/// `decompressLabels`.
pub(crate) fn decode_labels(mut src: &[u8]) -> Vec<Label> {
    let mut out = Vec::new();
    while !src.is_empty() {
        let (nlen, n1) = get_uvarint(src);
        if n1 == 0 {
            break;
        }
        src = &src[n1..];
        if src.len() < nlen as usize {
            break;
        }
        let name = String::from_utf8_lossy(&src[..nlen as usize]).into_owned();
        src = &src[nlen as usize..];
        let (vlen, n2) = get_uvarint(src);
        if n2 == 0 {
            break;
        }
        src = &src[n2..];
        if src.len() < vlen as usize {
            break;
        }
        let value = String::from_utf8_lossy(&src[..vlen as usize]).into_owned();
        src = &src[vlen as usize..];
        out.push(Label { name, value });
    }
    out
}

/// Encodes `input`/`output` label halves into a single key. Ports
/// `compressLabels`.
pub(crate) fn compress_labels(input: &[Label], output: &[Label]) -> Vec<u8> {
    let mut out_enc = Vec::new();
    encode_labels(&mut out_enc, output);

    let mut dst = Vec::with_capacity(out_enc.len() + 16);
    put_uvarint(&mut dst, out_enc.len() as u64);
    dst.extend_from_slice(&out_enc);
    encode_labels(&mut dst, input);
    dst
}

/// Splits a full key into `(input_key, output_key)`. Ports
/// `getInputOutputKey`.
///
/// * `output_key` is the encoded output-label half (used as the grouping
///   map key and decoded for the flushed series' labels).
/// * `input_key` is the per-series identity used by counter-style outputs
///   (`total`/`increase`/`rate`/`count_series`). When de-duplication is
///   enabled (`use_input_key == false`) it is the *whole* key; otherwise it
///   is the encoded input-label half.
pub(crate) fn split_key(key: &[u8], use_input_key: bool) -> (&[u8], &[u8]) {
    let (out_len, n) = get_uvarint(key);
    let rest = &key[n..];
    let out_len = out_len as usize;
    let output_key = &rest[..out_len.min(rest.len())];
    if !use_input_key {
        return (key, output_key);
    }
    let input_key = &rest[out_len.min(rest.len())..];
    (input_key, output_key)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lbl(n: &str, v: &str) -> Label {
        Label {
            name: n.into(),
            value: v.into(),
        }
    }

    #[test]
    fn round_trips_output_half() {
        let output = vec![lbl("__name__", "foo"), lbl("job", "bar")];
        let input = vec![lbl("instance", "host1")];
        let key = compress_labels(&input, &output);
        let (input_key, output_key) = split_key(&key, true);
        assert_eq!(decode_labels(output_key), output);
        assert_eq!(decode_labels(input_key), input);
    }

    #[test]
    fn whole_key_is_input_when_dedup_enabled() {
        let output = vec![lbl("__name__", "foo")];
        let input = vec![lbl("instance", "host1")];
        let key = compress_labels(&input, &output);
        let (input_key, output_key) = split_key(&key, false);
        assert_eq!(input_key, key.as_slice());
        assert_eq!(decode_labels(output_key), output);
    }

    #[test]
    fn empty_input_half() {
        let output = vec![lbl("__name__", "foo")];
        let key = compress_labels(&[], &output);
        let (input_key, output_key) = split_key(&key, true);
        assert!(input_key.is_empty());
        assert_eq!(decode_labels(output_key), output);
    }
}
