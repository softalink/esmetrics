//! The Backup action. Go: lib/backup/actions/backup.go

use anyhow::Context;
use serde::Serialize;

use crate::localfs::LocalFs;
use crate::names;
use crate::part::{parts_difference, parts_intersect};
use crate::remote::{cross_copy, RemoteFs};
use crate::run_parallel;
use crate::timeutil;

#[derive(Serialize)]
struct BackupMetadata {
    created_at: String,
    completed_at: String,
}

pub struct Backup<'a> {
    pub concurrency: usize,
    pub src: &'a LocalFs,
    pub dst: &'a dyn RemoteFs,
    pub origin: Option<&'a dyn RemoteFs>,
    /// RFC3339 creation time (derived from the snapshot name by esbackup);
    /// defaults to now.
    pub created_at: Option<String>,
}

impl Backup<'_> {
    pub fn run(&self) -> anyhow::Result<()> {
        let started = std::time::Instant::now();
        let concurrency = self.concurrency.max(1);

        // Remove the completion marker FIRST so an interrupted backup is
        // detectably incomplete.
        self.dst
            .delete_file(names::BACKUP_COMPLETE_FILENAME)
            .context("cannot delete backup_complete marker")?;

        let src_parts = self.src.list_parts().context("cannot list src parts")?;
        let dst_parts = self.dst.list_parts().context("cannot list dst parts")?;
        let origin_parts = match self.origin {
            Some(o) => o.list_parts().context("cannot list origin parts")?,
            None => Vec::new(),
        };

        // 1. Delete parts that vanished from src (also stale/broken parts —
        //    their actual_size key mismatch lands them here).
        let to_delete = parts_difference(&dst_parts, &src_parts);
        log::info!(
            "deleting {} obsolete parts from {}",
            to_delete.len(),
            self.dst.describe()
        );
        run_parallel(&to_delete, concurrency, |p| self.dst.delete_part(p))?;
        if !to_delete.is_empty() {
            self.dst.remove_empty_dirs()?;
        }

        // 2. Server-side copy of parts available in origin.
        let to_copy = parts_difference(&src_parts, &dst_parts);
        let from_origin = parts_intersect(&origin_parts, &to_copy);
        if let Some(origin) = self.origin {
            log::info!(
                "server-side copying {} parts from {}",
                from_origin.len(),
                origin.describe()
            );
            run_parallel(&from_origin, concurrency, |p| {
                if !self.dst.copy_part_from(origin, p)? {
                    cross_copy(origin, self.dst, p)?;
                }
                Ok(())
            })?;
        }

        // 3. Upload everything else from the local snapshot.
        let to_upload = parts_difference(&to_copy, &origin_parts);
        let total: u64 = to_upload.iter().map(|p| p.size).sum();
        log::info!(
            "uploading {} parts ({} bytes) to {}",
            to_upload.len(),
            total,
            self.dst.describe()
        );
        run_parallel(&to_upload, concurrency, |p| {
            let mut r = self.src.open_part_reader(p)?;
            self.dst
                .upload_part(p, &mut r)
                .with_context(|| format!("cannot upload part {}", p.path))
        })?;

        // 4. Metadata, then the completion marker LAST.
        let meta = BackupMetadata {
            created_at: self
                .created_at
                .clone()
                .unwrap_or_else(timeutil::now_rfc3339),
            completed_at: timeutil::now_rfc3339(),
        };
        self.dst
            .create_file(names::BACKUP_METADATA_FILENAME, &serde_json::to_vec(&meta)?)?;
        self.dst.create_file(names::BACKUP_COMPLETE_FILENAME, b"")?;

        log::info!(
            "backup to {} complete in {:.3}s",
            self.dst.describe(),
            started.elapsed().as_secs_f64()
        );
        Ok(())
    }
}
