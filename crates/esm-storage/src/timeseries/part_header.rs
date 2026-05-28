//! `PartHeader` — time-series part summary persisted as `metadata.json`.
//!
//! Format reference: `docs/format/timeseries-part.md` §3.
//! VM source: `lib/storage/part_header.go`.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Whole-part summary for a time-series part.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PartHeader {
    pub rows_count: u64,
    pub blocks_count: u64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct PartHeaderJson {
    #[serde(rename = "RowsCount")]
    rows_count: u64,
    #[serde(rename = "BlocksCount")]
    blocks_count: u64,
    #[serde(rename = "MinTimestamp")]
    min_timestamp: i64,
    #[serde(rename = "MaxTimestamp")]
    max_timestamp: i64,
}

impl PartHeader {
    /// Serialise to JSON bytes.
    ///
    /// # Errors
    /// Returns `serde_json::Error` if serialisation fails (effectively
    /// impossible for this concrete type).
    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        let phj = PartHeaderJson {
            rows_count: self.rows_count,
            blocks_count: self.blocks_count,
            min_timestamp: self.min_timestamp,
            max_timestamp: self.max_timestamp,
        };
        serde_json::to_vec(&phj)
    }

    /// Parse a `metadata.json` document and apply VM's validation rules.
    ///
    /// # Errors
    /// See [`PartHeaderError`].
    pub fn from_json(data: &[u8]) -> Result<Self, PartHeaderError> {
        let phj: PartHeaderJson = serde_json::from_slice(data)?;
        if phj.rows_count == 0 {
            return Err(PartHeaderError::ZeroRows);
        }
        if phj.blocks_count == 0 {
            return Err(PartHeaderError::ZeroBlocks);
        }
        if phj.blocks_count > phj.rows_count {
            return Err(PartHeaderError::BlocksExceedRows {
                blocks: phj.blocks_count,
                rows: phj.rows_count,
            });
        }
        if phj.min_timestamp > phj.max_timestamp {
            return Err(PartHeaderError::MinExceedsMax {
                min: phj.min_timestamp,
                max: phj.max_timestamp,
            });
        }
        Ok(Self {
            rows_count: phj.rows_count,
            blocks_count: phj.blocks_count,
            min_timestamp: phj.min_timestamp,
            max_timestamp: phj.max_timestamp,
        })
    }
}

#[derive(Debug, Error)]
pub enum PartHeaderError {
    #[error("malformed metadata.json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("rows_count must be > 0")]
    ZeroRows,
    #[error("blocks_count must be > 0")]
    ZeroBlocks,
    #[error("blocks_count={blocks} exceeds rows_count={rows}")]
    BlocksExceedRows { blocks: u64, rows: u64 },
    #[error("min_timestamp={min} exceeds max_timestamp={max}")]
    MinExceedsMax { min: i64, max: i64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_json() {
        let ph = PartHeader {
            rows_count: 1000,
            blocks_count: 5,
            min_timestamp: 100,
            max_timestamp: 999,
        };
        let bytes = ph.to_json().unwrap();
        let parsed = PartHeader::from_json(&bytes).unwrap();
        assert_eq!(parsed, ph);
    }

    #[test]
    fn zero_rows_rejected() {
        let bad = br#"{"RowsCount":0,"BlocksCount":1,"MinTimestamp":0,"MaxTimestamp":1}"#;
        assert!(matches!(PartHeader::from_json(bad), Err(PartHeaderError::ZeroRows)));
    }

    #[test]
    fn blocks_exceeding_rows_rejected() {
        let bad = br#"{"RowsCount":5,"BlocksCount":99,"MinTimestamp":0,"MaxTimestamp":1}"#;
        assert!(matches!(
            PartHeader::from_json(bad),
            Err(PartHeaderError::BlocksExceedRows { .. })
        ));
    }

    #[test]
    fn min_exceeds_max_rejected() {
        let bad = br#"{"RowsCount":10,"BlocksCount":1,"MinTimestamp":999,"MaxTimestamp":1}"#;
        assert!(matches!(PartHeader::from_json(bad), Err(PartHeaderError::MinExceedsMax { .. })));
    }
}
