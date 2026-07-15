//! Port of the upstream VictoriaMetrics v1.146.0 lib/storage/part_header.go: part
//! metadata stored in `metadata.json`.
//!
//! PORT-SKIP: the pre-v1.90 fallback (`partHeader.ParseFromPath`, which
//! parses `RowsCount_BlocksCount_MinTimestamp_MaxTimestamp_Garbage` part dir
//! names plus the legacy `min_dedup_interval` file) is not ported — this
//! green-field port always writes `metadata.json`.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::part::METADATA_FILENAME;

/// Part header. Go: partHeader.
///
/// The serde field names match the Go JSON encoding of `metadata.json`
/// exactly (`RowsCount`, `BlocksCount`, ...).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PartHeader {
    /// The total number of rows in the part.
    pub rows_count: u64,

    /// The total number of blocks in the part.
    pub blocks_count: u64,

    /// The minimum timestamp in the part.
    pub min_timestamp: i64,

    /// The maximum timestamp in the part.
    pub max_timestamp: i64,

    /// The minimal dedup interval in milliseconds across all the blocks in
    /// the part.
    #[serde(default)]
    pub min_dedup_interval: i64,
}

impl Default for PartHeader {
    fn default() -> PartHeader {
        PartHeader {
            rows_count: 0,
            blocks_count: 0,
            min_timestamp: i64::MAX,
            max_timestamp: i64::MIN,
            min_dedup_interval: 0,
        }
    }
}

impl std::fmt::Display for PartHeader {
    // Go: partHeader.String.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "partHeader{{rowsCount={},blocksCount={},minTimestamp={},maxTimestamp={}}}",
            self.rows_count, self.blocks_count, self.min_timestamp, self.max_timestamp
        )
    }
}

impl PartHeader {
    /// Resets the header. Go: partHeader.Reset.
    pub fn reset(&mut self) {
        *self = PartHeader::default();
    }

    /// Reads the part header from `<part_path>/metadata.json`, panicking on
    /// failure. Go: partHeader.MustReadMetadata.
    pub fn must_read_metadata(&mut self, part_path: &Path) {
        self.reset();

        let metadata_path = part_path.join(METADATA_FILENAME);
        let metadata = std::fs::read(&metadata_path)
            .unwrap_or_else(|err| panic!("FATAL: cannot read {metadata_path:?}: {err}"));
        *self = serde_json::from_slice(&metadata)
            .unwrap_or_else(|err| panic!("FATAL: cannot parse {metadata_path:?}: {err}"));

        // Perform various checks.
        assert!(
            self.min_timestamp <= self.max_timestamp,
            "FATAL: minTimestamp cannot exceed maxTimestamp at {metadata_path:?}; got {} vs {}",
            self.min_timestamp,
            self.max_timestamp
        );
        assert!(
            self.rows_count > 0,
            "FATAL: rowsCount must be greater than 0 at {metadata_path:?}"
        );
        assert!(
            self.blocks_count > 0,
            "FATAL: blocksCount must be greater than 0 at {metadata_path:?}"
        );
        assert!(
            self.blocks_count <= self.rows_count,
            "FATAL: blocksCount cannot be bigger than rowsCount at {metadata_path:?}; \
             got blocksCount={}, rowsCount={}",
            self.blocks_count,
            self.rows_count
        );
    }

    /// Writes the part header to `<part_path>/metadata.json`.
    /// Go: partHeader.MustWriteMetadata.
    pub fn must_write_metadata(&self, part_path: &Path) {
        let metadata = serde_json::to_vec(self)
            .unwrap_or_else(|err| panic!("BUG: cannot marshal partHeader metadata: {err}"));
        let metadata_path = part_path.join(METADATA_FILENAME);
        // There is no need in calling must_write_atomic() here, since the
        // file is created only once during part creation and the part
        // directory is synced afterwards.
        esm_common::fs::must_write_sync(&metadata_path, &metadata);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_field_names_match_go() {
        let ph = PartHeader {
            rows_count: 1,
            blocks_count: 1,
            min_timestamp: -5,
            max_timestamp: 10,
            min_dedup_interval: 60000,
        };
        let s = serde_json::to_string(&ph).unwrap();
        assert_eq!(
            s,
            r#"{"RowsCount":1,"BlocksCount":1,"MinTimestamp":-5,"MaxTimestamp":10,"MinDedupInterval":60000}"#
        );

        // MinDedupInterval is optional on read (older metadata.json files).
        let ph2: PartHeader = serde_json::from_str(
            r#"{"RowsCount":2,"BlocksCount":1,"MinTimestamp":0,"MaxTimestamp":1}"#,
        )
        .unwrap();
        assert_eq!(ph2.min_dedup_interval, 0);
        assert_eq!(ph2.rows_count, 2);
    }

    #[test]
    fn metadata_roundtrip() {
        let dir = std::env::temp_dir().join(format!(
            "esm-storage-part-header-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let ph = PartHeader {
            rows_count: 123,
            blocks_count: 7,
            min_timestamp: -1000,
            max_timestamp: 5000,
            min_dedup_interval: 30000,
        };
        ph.must_write_metadata(&dir);

        let mut ph2 = PartHeader::default();
        ph2.must_read_metadata(&dir);
        assert_eq!(ph2, ph);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn default_is_reset_state() {
        let ph = PartHeader::default();
        assert_eq!(ph.min_timestamp, i64::MAX);
        assert_eq!(ph.max_timestamp, i64::MIN);
        assert_eq!(ph.rows_count, 0);
        assert_eq!(ph.blocks_count, 0);
        assert_eq!(ph.min_dedup_interval, 0);
    }
}
