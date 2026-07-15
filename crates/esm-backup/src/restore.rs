//! The Restore action. Go: lib/backup/actions/restore.go

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use anyhow::Context;

use crate::localfs::LocalFs;
use crate::names;
use crate::part::{parts_difference, sort_parts, Part};
use crate::remote::RemoteFs;
use crate::run_parallel;

pub struct Restore<'a> {
    pub concurrency: usize,
    pub src: &'a dyn RemoteFs,
    pub dst_dir: PathBuf,
    pub skip_backup_complete_check: bool,
}

impl Restore<'_> {
    pub fn run(&self) -> anyhow::Result<()> {
        let started = std::time::Instant::now();
        std::fs::create_dir_all(&self.dst_dir)?;
        // Fails if an esmetrics server currently owns the data dir.
        let _flock = esm_common::fs::must_create_flock_file(&self.dst_dir);

        if !self.skip_backup_complete_check {
            anyhow::ensure!(
                self.src.has_file(names::BACKUP_COMPLETE_FILENAME)?,
                "cannot find {} in {}: the backup is incomplete or the -src path is wrong",
                names::BACKUP_COMPLETE_FILENAME,
                self.src.describe(),
            );
        }

        // Mark the restore as in progress; esm-storage refuses to open the
        // dir until this file is removed (i.e. until we succeed).
        let lock_path = self.dst_dir.join(names::RESTORE_IN_PROGRESS_FILENAME);
        std::fs::write(&lock_path, b"")?;

        let local = LocalFs::new(&self.dst_dir);
        local.cleanup_tmp_files()?;

        let mut src_parts = self.src.list_parts().context("cannot list backup parts")?;
        validate_parts(&self.dst_dir, &mut src_parts)?;

        // rsync --delete semantics: drop local files whose offset-0 part is
        // not in the backup (per-file identity lives in the offset-0 key).
        let local_parts = local.list_parts()?;
        let to_delete = parts_difference(&local_parts, &src_parts);
        let paths_to_delete: HashSet<&str> = to_delete
            .iter()
            .filter(|p| p.offset == 0)
            .map(|p| p.path.as_str())
            .collect();
        log::info!(
            "deleting {} local files missing from the backup",
            paths_to_delete.len()
        );
        for path in &paths_to_delete {
            local.delete_path(path)?;
        }
        if !paths_to_delete.is_empty() {
            local.remove_empty_dirs()?;
        }

        // Download missing parts, grouped per file, files in parallel.
        let local_parts = local.list_parts()?;
        let to_copy = parts_difference(&src_parts, &local_parts);
        let mut per_path: HashMap<String, Vec<Part>> = HashMap::new();
        for p in to_copy {
            per_path.entry(p.path.clone()).or_default().push(p);
        }
        // `to_copy` preserves `src_parts`'s global (path, offset) sort from
        // validate_parts, so each per-path group is already offset-ordered.
        let groups: Vec<Vec<Part>> = per_path.into_values().collect();
        let total: u64 = groups.iter().flatten().map(|p| p.size).sum();
        log::info!(
            "downloading {} files ({total} bytes) from {}",
            groups.len(),
            self.src.describe()
        );
        run_parallel(&groups, self.concurrency.max(1), |parts| {
            let local = LocalFs::new(&self.dst_dir);
            for p in parts {
                // One in-flight part buffer per worker (≤ 1 GiB worst case;
                // esm-storage part files are typically MBs). If this proves
                // heavy, swap Vec for the ChannelWriter/Reader pipe in
                // remote/mod.rs — out of scope now.
                let mut buf = Vec::with_capacity(p.size.min(64 * 1024 * 1024) as usize);
                self.src.download_part(p, &mut buf)?;
                local.write_part_at(p, &mut buf.as_slice())?;
            }
            Ok(())
        })?;

        std::fs::remove_file(&lock_path)?;
        log::info!(
            "restore from {} complete in {:.3}s",
            self.src.describe(),
            started.elapsed().as_secs_f64()
        );
        Ok(())
    }
}

/// Path-traversal guard + whole-file contiguity check.
/// Go: restore.go:94-137.
fn validate_parts(dst_dir: &std::path::Path, parts: &mut [Part]) -> anyhow::Result<()> {
    for p in parts.iter() {
        let ok = !p.path.is_empty()
            && !p.path.starts_with('/')
            && p.path
                .split('/')
                .all(|c| !c.is_empty() && c != "." && c != "..");
        anyhow::ensure!(
            ok,
            "part path {:?} escapes the restore dir {dst_dir:?}",
            p.path
        );
    }
    sort_parts(parts);
    let mut i = 0;
    while i < parts.len() {
        let path = &parts[i].path;
        let file_size = parts[i].file_size;
        let mut expected_offset = 0u64;
        let mut j = i;
        while j < parts.len() && parts[j].path == *path {
            let p = &parts[j];
            anyhow::ensure!(
                p.offset == expected_offset && p.size == p.actual_size && p.file_size == file_size,
                "corrupted backup: part {:?} offset={} size={} actual_size={} file_size={} \
                 (expected offset {expected_offset}, file_size {file_size})",
                p.path,
                p.offset,
                p.size,
                p.actual_size,
                p.file_size
            );
            expected_offset += p.size;
            j += 1;
        }
        anyhow::ensure!(
            expected_offset == file_size,
            "corrupted backup: file {:?} has {expected_offset} bytes of parts, want {file_size}",
            path
        );
        i = j;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    /// Builds a `Part` with `actual_size` defaulted to `size`; override the
    /// field directly on the returned value for actual/expected mismatches.
    fn part(path: &str, file_size: u64, offset: u64, size: u64) -> Part {
        Part {
            path: path.into(),
            file_size,
            offset,
            size,
            actual_size: size,
        }
    }

    #[test]
    fn rejects_paths_that_escape_the_restore_dir() {
        let dst = Path::new("/dst");
        let bad_paths = ["../evil", "/etc/passwd", "a//b", "a/./b", ""];
        for bad in bad_paths {
            let mut parts = vec![part(bad, 0, 0, 0)];
            assert!(
                validate_parts(dst, &mut parts).is_err(),
                "expected {bad:?} to be rejected"
            );
        }
    }

    #[test]
    fn accepts_normal_nested_path() {
        let dst = Path::new("/dst");
        let mut parts = vec![part("data/small/2026_07/x.bin", 10, 0, 10)];
        assert!(validate_parts(dst, &mut parts).is_ok());
    }

    #[test]
    fn rejects_gap_between_parts() {
        let dst = Path::new("/dst");
        let mut parts = vec![part("data/f", 10, 0, 4), part("data/f", 10, 8, 2)];
        assert!(validate_parts(dst, &mut parts).is_err());
    }

    #[test]
    fn rejects_overlapping_parts() {
        let dst = Path::new("/dst");
        let mut parts = vec![part("data/f", 10, 0, 6), part("data/f", 10, 4, 6)];
        assert!(validate_parts(dst, &mut parts).is_err());
    }

    #[test]
    fn rejects_size_actual_size_mismatch() {
        let dst = Path::new("/dst");
        let mut p = part("data/f", 4, 0, 4);
        p.actual_size = 3;
        let mut parts = vec![p];
        assert!(validate_parts(dst, &mut parts).is_err());
    }

    #[test]
    fn rejects_total_size_not_matching_file_size() {
        let dst = Path::new("/dst");
        let mut parts = vec![part("data/f", 10, 0, 4)];
        assert!(validate_parts(dst, &mut parts).is_err());
    }

    #[test]
    fn rejects_inconsistent_file_size_across_parts() {
        let dst = Path::new("/dst");
        let mut parts = vec![part("data/f", 10, 0, 5), part("data/f", 20, 5, 5)];
        assert!(validate_parts(dst, &mut parts).is_err());
    }

    #[test]
    fn accepts_contiguous_parts_fed_out_of_order() {
        let dst = Path::new("/dst");
        // Fed offset-descending to prove validate_parts sorts before checking.
        let mut parts = vec![part("data/f", 10, 5, 5), part("data/f", 10, 0, 5)];
        assert!(validate_parts(dst, &mut parts).is_ok());
    }

    #[test]
    fn accepts_zero_length_file() {
        let dst = Path::new("/dst");
        let mut parts = vec![part("data/f", 0, 0, 0)];
        assert!(validate_parts(dst, &mut parts).is_ok());
    }
}
