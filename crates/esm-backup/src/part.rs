//! Backup part model. Go: lib/backup/common/part.go

use std::sync::atomic::{AtomicU64, Ordering};

/// Files bigger than this are split into multiple parts.
pub const MAX_PART_SIZE: u64 = 1024 * 1024 * 1024;

/// A contiguous piece of a file, the unit of backup transfer/diffing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Part {
    /// Canonical file path relative to the backup root, `/`-separated.
    pub path: String,
    /// Size of the whole file this part belongs to.
    pub file_size: u64,
    /// Offset of this part within the file.
    pub offset: u64,
    /// Expected part length.
    pub size: u64,
    /// Observed length (differs from `size` for broken/partial remote parts).
    pub actual_size: u64,
}

impl Part {
    /// Identity for set-diffing. `file_size` is deliberately excluded so
    /// partially-restored files resume correctly (upstream comment).
    /// Mutable files (`parts.json`) get a unique key so they always re-copy.
    pub fn key(&self) -> String {
        if self.path.ends_with("/parts.json") || self.path == "parts.json" {
            static UNIQUE: AtomicU64 = AtomicU64::new(0);
            return format!("unique-{:016X}", UNIQUE.fetch_add(1, Ordering::Relaxed));
        }
        format!(
            "{}#{:016X}#{:016X}#{:016X}",
            self.path, self.offset, self.size, self.actual_size
        )
    }

    /// Remote object key: `<prefix>/<path>/<FILE_SIZE>_<OFFSET>_<SIZE>`.
    pub fn remote_path(&self, prefix: &str) -> String {
        let prefix = prefix.trim_end_matches('/');
        let sep = if prefix.is_empty() { "" } else { "/" };
        format!(
            "{prefix}{sep}{}/{:016X}_{:016X}_{:016X}",
            self.path, self.file_size, self.offset, self.size
        )
    }

    /// Inverse of `remote_path`. `remote` must already be prefix-stripped.
    pub fn parse_from_remote_path(remote: &str, actual_size: u64) -> Option<Part> {
        let (path, name) = remote.rsplit_once('/')?;
        let fields: Vec<&str> = name.split('_').collect();
        if path.is_empty() || fields.len() != 3 || fields.iter().any(|f| f.len() != 16) {
            return None;
        }
        let parse = |s: &str| u64::from_str_radix(s, 16).ok();
        Some(Part {
            path: path.to_string(),
            file_size: parse(fields[0])?,
            offset: parse(fields[1])?,
            size: parse(fields[2])?,
            actual_size,
        })
    }
}

/// Splits a file into `<= MAX_PART_SIZE` parts. Zero-length files produce a
/// single empty part so they are preserved by backup/restore.
pub fn split_into_parts(path: &str, file_size: u64) -> Vec<Part> {
    let mut parts = Vec::new();
    let mut offset = 0u64;
    loop {
        let n = (file_size - offset).min(MAX_PART_SIZE);
        parts.push(Part {
            path: path.to_string(),
            file_size,
            offset,
            size: n,
            actual_size: n,
        });
        offset += n;
        if offset >= file_size {
            return parts;
        }
    }
}

pub fn sort_parts(parts: &mut [Part]) {
    parts.sort_by(|a, b| a.path.cmp(&b.path).then(a.offset.cmp(&b.offset)));
}

/// Returns parts present in `a` but missing from `b` (a \ b).
pub fn parts_difference(a: &[Part], b: &[Part]) -> Vec<Part> {
    let keys: std::collections::HashSet<String> = b.iter().map(Part::key).collect();
    a.iter()
        .filter(|p| !keys.contains(&p.key()))
        .cloned()
        .collect()
}

/// Returns parts present in both `a` and `b`.
pub fn parts_intersect(a: &[Part], b: &[Part]) -> Vec<Part> {
    let keys: std::collections::HashSet<String> = a.iter().map(Part::key).collect();
    b.iter()
        .filter(|p| keys.contains(&p.key()))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn part(path: &str, offset: u64, size: u64) -> Part {
        Part {
            path: path.into(),
            file_size: 4096,
            offset,
            size,
            actual_size: size,
        }
    }

    #[test]
    fn remote_path_roundtrip() {
        let p = part("data/small/2026_07/0000000000000001/values.bin", 0, 4096);
        let rp = p.remote_path("base/dir");
        assert_eq!(
            rp,
            "base/dir/data/small/2026_07/0000000000000001/values.bin/\
             0000000000001000_0000000000000000_0000000000001000"
        );
        let stripped = rp.strip_prefix("base/dir/").unwrap();
        assert_eq!(Part::parse_from_remote_path(stripped, 4096).unwrap(), p);
    }

    #[test]
    fn parse_rejects_malformed() {
        assert!(Part::parse_from_remote_path("no-slash", 0).is_none());
        assert!(Part::parse_from_remote_path("a/b/short_1_2", 0).is_none());
        assert!(Part::parse_from_remote_path(
            "a/0000000000000000_0000000000000000_000000000000ZZZZ",
            0
        )
        .is_none());
    }

    #[test]
    fn split_into_parts_covers_file() {
        assert_eq!(split_into_parts("f", 0).len(), 1); // zero-length file
        assert_eq!(split_into_parts("f", 0)[0].size, 0);
        assert_eq!(split_into_parts("f", MAX_PART_SIZE).len(), 1);
        let parts = split_into_parts("f", 2 * MAX_PART_SIZE + 5);
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[2].offset, 2 * MAX_PART_SIZE);
        assert_eq!(parts[2].size, 5);
        assert_eq!(
            parts.iter().map(|p| p.size).sum::<u64>(),
            2 * MAX_PART_SIZE + 5
        );
    }

    #[test]
    fn diff_and_intersect() {
        let a = vec![part("x", 0, 10), part("y", 0, 10)];
        let b = vec![part("y", 0, 10), part("z", 0, 10)];
        assert_eq!(parts_difference(&a, &b), vec![part("x", 0, 10)]);
        assert_eq!(parts_intersect(&a, &b), vec![part("y", 0, 10)]);
        // actual_size participates in identity:
        let mut broken = part("y", 0, 10);
        broken.actual_size = 3;
        assert_eq!(parts_difference(&[broken.clone()], &b), vec![broken]);
    }

    #[test]
    fn parts_json_is_never_equal_to_itself() {
        let pj = part("data/small/2026_07/parts.json", 0, 64);
        assert_eq!(
            parts_difference(std::slice::from_ref(&pj), std::slice::from_ref(&pj)).len(),
            1
        );
        assert!(parts_intersect(std::slice::from_ref(&pj), std::slice::from_ref(&pj)).is_empty());
    }
}
