//! `PartHeader` — the whole-part summary persisted to `metadata.json`.
//!
//! Format reference: `docs/format/mergeset-part.md` §2.
//! VM source: `lib/mergeset/part_header.go:14-26,28-44`.

use serde::{Deserialize, Serialize};

/// Whole-part summary. Persisted as JSON in `<part>/metadata.json`.
///
/// All four fields are required and validated by VM's loader. EsMetrics
/// matches the validation rules.
#[derive(Debug, Default, Clone)]
pub struct PartHeader {
    /// Total number of items in the part. Must be > 0.
    pub items_count: u64,
    /// Total number of blocks in the part. Must be > 0 and ≤ `items_count`.
    pub blocks_count: u64,
    /// First item in the part (lex-smallest).
    pub first_item: Vec<u8>,
    /// Last item in the part (lex-largest).
    pub last_item: Vec<u8>,
}

/// On-disk JSON shape. VM marshals the byte fields as lower-case hex strings.
/// This is the wire-level representation; convert via [`PartHeader::to_json`]
/// and [`PartHeader::from_json`].
#[derive(Debug, Serialize, Deserialize)]
struct PartHeaderJson {
    #[serde(rename = "ItemsCount")]
    items_count: u64,
    #[serde(rename = "BlocksCount")]
    blocks_count: u64,
    #[serde(rename = "FirstItem")]
    first_item: HexString,
    #[serde(rename = "LastItem")]
    last_item: HexString,
}

/// Newtype that (de)serialises as a JSON string of lower-case hex digits.
/// Matches VM's `hexString` (`lib/mergeset/part_header.go:35-60`).
#[derive(Debug, Default)]
struct HexString(Vec<u8>);

impl Serialize for HexString {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use std::fmt::Write as _;
        // Lower-case hex, no `0x` prefix, no separators. Match `encoding/hex.EncodeToString`.
        let mut s = String::with_capacity(self.0.len() * 2);
        for byte in &self.0 {
            // We intentionally use lower-case to match Go's hex.EncodeToString output.
            // `write!` into a String is infallible.
            let _ = write!(s, "{byte:02x}");
        }
        serializer.serialize_str(&s)
    }
}

impl<'de> Deserialize<'de> for HexString {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let s = String::deserialize(deserializer)?;
        if s.len() % 2 != 0 {
            return Err(D::Error::custom("hex string length must be even"));
        }
        let mut out = Vec::with_capacity(s.len() / 2);
        for chunk in s.as_bytes().chunks(2) {
            let hi = hex_nibble(chunk[0]).map_err(D::Error::custom)?;
            let lo = hex_nibble(chunk[1]).map_err(D::Error::custom)?;
            out.push((hi << 4) | lo);
        }
        Ok(Self(out))
    }
}

fn hex_nibble(c: u8) -> Result<u8, &'static str> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err("invalid hex digit"),
    }
}

impl PartHeader {
    /// Serialise to the canonical `metadata.json` representation.
    ///
    /// # Errors
    /// Returns `serde_json::Error` if JSON serialisation fails (impossible
    /// for this concrete type in practice).
    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        let phj = PartHeaderJson {
            items_count: self.items_count,
            blocks_count: self.blocks_count,
            first_item: HexString(self.first_item.clone()),
            last_item: HexString(self.last_item.clone()),
        };
        serde_json::to_vec(&phj)
    }

    /// Parse a `metadata.json` document. Applies the validity checks VM's
    /// `MustReadMetadata` (`lib/mergeset/part_header.go:81-112`) enforces.
    ///
    /// # Errors
    /// - `serde_json::Error` if the JSON is malformed or fields are missing.
    /// - A custom error if `items_count == 0`, `blocks_count == 0`, or
    ///   `blocks_count > items_count`.
    pub fn from_json(data: &[u8]) -> Result<Self, PartHeaderError> {
        let phj: PartHeaderJson = serde_json::from_slice(data)?;
        if phj.items_count == 0 {
            return Err(PartHeaderError::ZeroItems);
        }
        if phj.blocks_count == 0 {
            return Err(PartHeaderError::ZeroBlocks);
        }
        if phj.blocks_count > phj.items_count {
            return Err(PartHeaderError::BlocksExceedItems {
                blocks: phj.blocks_count,
                items: phj.items_count,
            });
        }
        Ok(Self {
            items_count: phj.items_count,
            blocks_count: phj.blocks_count,
            first_item: phj.first_item.0,
            last_item: phj.last_item.0,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PartHeaderError {
    #[error("malformed metadata.json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("part cannot contain zero items")]
    ZeroItems,
    #[error("part cannot contain zero blocks")]
    ZeroBlocks,
    #[error("blocks_count={blocks} exceeds items_count={items}")]
    BlocksExceedItems { blocks: u64, items: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_json() {
        let ph = PartHeader {
            items_count: 100,
            blocks_count: 4,
            first_item: vec![0x00, 0x01, 0x02],
            last_item: vec![0xfe, 0xff],
        };
        let bytes = ph.to_json().unwrap();
        let s = std::str::from_utf8(&bytes).unwrap();
        // Spot-check key fields are present and hex is lower-case.
        assert!(s.contains(r#""ItemsCount":100"#));
        assert!(s.contains(r#""BlocksCount":4"#));
        assert!(s.contains(r#""FirstItem":"000102""#));
        assert!(s.contains(r#""LastItem":"feff""#));

        let parsed = PartHeader::from_json(&bytes).unwrap();
        assert_eq!(parsed.items_count, 100);
        assert_eq!(parsed.blocks_count, 4);
        assert_eq!(parsed.first_item, vec![0, 1, 2]);
        assert_eq!(parsed.last_item, vec![0xfe, 0xff]);
    }

    #[test]
    fn zero_items_rejected() {
        let payload = br#"{"ItemsCount":0,"BlocksCount":1,"FirstItem":"00","LastItem":"00"}"#;
        assert!(matches!(PartHeader::from_json(payload), Err(PartHeaderError::ZeroItems)));
    }

    #[test]
    fn blocks_exceeding_items_rejected() {
        let payload = br#"{"ItemsCount":3,"BlocksCount":10,"FirstItem":"","LastItem":""}"#;
        assert!(matches!(
            PartHeader::from_json(payload),
            Err(PartHeaderError::BlocksExceedItems { blocks: 10, items: 3 })
        ));
    }
}
