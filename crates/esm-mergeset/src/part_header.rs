//! Port of `part_header.go`: part metadata stored in `metadata.json`.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::filenames::METADATA_FILENAME;
use crate::util::{hex_decode, hex_encode};

/// Part metadata.
#[derive(Debug, Default, Clone)]
pub(crate) struct PartHeader {
    /// The number of items the part contains.
    pub items_count: u64,
    /// The number of blocks the part contains.
    pub blocks_count: u64,
    /// The first item in the part.
    pub first_item: Vec<u8>,
    /// The last item in the part.
    pub last_item: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct PartHeaderJson {
    items_count: u64,
    blocks_count: u64,
    first_item: String,
    last_item: String,
}

impl PartHeader {
    pub fn reset(&mut self) {
        self.items_count = 0;
        self.blocks_count = 0;
        self.first_item.clear();
        self.last_item.clear();
    }

    pub fn copy_from(&mut self, src: &PartHeader) {
        self.items_count = src.items_count;
        self.blocks_count = src.blocks_count;
        self.first_item.clear();
        self.first_item.extend_from_slice(&src.first_item);
        self.last_item.clear();
        self.last_item.extend_from_slice(&src.last_item);
    }

    pub fn must_read_metadata(&mut self, part_path: &Path) {
        self.reset();

        let metadata_path = part_path.join(METADATA_FILENAME);
        let metadata = std::fs::read(&metadata_path)
            .unwrap_or_else(|e| panic!("FATAL: cannot read {metadata_path:?}: {e}"));

        let phj: PartHeaderJson = serde_json::from_slice(&metadata)
            .unwrap_or_else(|e| panic!("FATAL: cannot parse {metadata_path:?}: {e}"));

        assert!(
            phj.items_count > 0,
            "FATAL: part {part_path:?} cannot contain zero items"
        );
        self.items_count = phj.items_count;

        assert!(
            phj.blocks_count > 0,
            "FATAL: part {part_path:?} cannot contain zero blocks"
        );
        assert!(
            phj.blocks_count <= phj.items_count,
            "FATAL: the number of blocks cannot exceed the number of items in the part {part_path:?}; \
             got blocksCount={}, itemsCount={}",
            phj.blocks_count,
            phj.items_count
        );
        self.blocks_count = phj.blocks_count;

        self.first_item = hex_decode(&phj.first_item).unwrap_or_else(|e| {
            panic!("FATAL: cannot hex-decode FirstItem at {metadata_path:?}: {e}")
        });
        self.last_item = hex_decode(&phj.last_item).unwrap_or_else(|e| {
            panic!("FATAL: cannot hex-decode LastItem at {metadata_path:?}: {e}")
        });
    }

    pub fn must_write_metadata(&self, part_path: &Path) {
        let phj = PartHeaderJson {
            items_count: self.items_count,
            blocks_count: self.blocks_count,
            first_item: hex_encode(&self.first_item),
            last_item: hex_encode(&self.last_item),
        };
        let metadata = serde_json::to_vec(&phj)
            .unwrap_or_else(|e| panic!("BUG: cannot marshal partHeader metadata: {e}"));
        let metadata_path = part_path.join(METADATA_FILENAME);
        // There is no need in calling must_write_atomic() here, since the file
        // is created only once during part creation and the part directory is
        // synced afterward.
        esm_common::fs::must_write_sync(&metadata_path, &metadata);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_roundtrip() {
        let dir = std::env::temp_dir().join("esm-mergeset-part-header-test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let ph = PartHeader {
            items_count: 123,
            blocks_count: 7,
            first_item: b"first\x00item".to_vec(),
            last_item: b"last item \xff".to_vec(),
        };
        ph.must_write_metadata(&dir);

        let mut ph2 = PartHeader::default();
        ph2.must_read_metadata(&dir);
        assert_eq!(ph2.items_count, 123);
        assert_eq!(ph2.blocks_count, 7);
        assert_eq!(ph2.first_item, ph.first_item);
        assert_eq!(ph2.last_item, ph.last_item);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn json_field_names_match_go() {
        let ph = PartHeader {
            items_count: 1,
            blocks_count: 1,
            first_item: b"a".to_vec(),
            last_item: b"b".to_vec(),
        };
        let phj = PartHeaderJson {
            items_count: ph.items_count,
            blocks_count: ph.blocks_count,
            first_item: hex_encode(&ph.first_item),
            last_item: hex_encode(&ph.last_item),
        };
        let s = serde_json::to_string(&phj).unwrap();
        assert_eq!(
            s,
            r#"{"ItemsCount":1,"BlocksCount":1,"FirstItem":"61","LastItem":"62"}"#
        );
    }
}
