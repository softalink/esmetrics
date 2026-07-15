//! fs:// round-trip tests for the Backup and Restore actions.

use std::path::{Path, PathBuf};

use esm_backup::backup::Backup;
use esm_backup::localfs::LocalFs;
use esm_backup::names;
use esm_backup::remote::new_remote_fs;
use esm_backup::restore::Restore;

fn test_dir(name: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("esm-backup-actions-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_file(dir: &Path, rel: &str, data: &[u8]) {
    let p = dir.join(rel);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(p, data).unwrap();
}

/// Recursively reads all regular files as (canonical-rel-path, contents).
fn read_tree(dir: &Path) -> Vec<(String, Vec<u8>)> {
    fn walk(root: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                walk(root, &entry.path(), out);
            } else {
                let rel = entry
                    .path()
                    .strip_prefix(root)
                    .unwrap()
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy().into_owned())
                    .collect::<Vec<_>>()
                    .join("/");
                out.push((rel, std::fs::read(entry.path()).unwrap()));
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
}

fn make_src_tree(src: &Path) {
    write_file(
        src,
        "data/small/2026_07/0000000000000001/values.bin",
        &[7u8; 4096],
    );
    write_file(src, "data/small/2026_07/0000000000000001/index.bin", b"idx");
    write_file(
        src,
        "data/small/2026_07/parts.json",
        b"{\"Small\":[\"0000000000000001\"],\"Big\":[]}",
    );
    write_file(src, "data/indexdb/2026_07/parts.json", b"[]");
    write_file(src, "empty.bin", b"");
    write_file(src, "flock.lock", b"never backed up");
}

#[test]
fn backup_writes_markers_and_all_parts() {
    let src_dir = test_dir("backup-src");
    let dst_dir = test_dir("backup-dst");
    make_src_tree(&src_dir);

    let src = LocalFs::new(&src_dir);
    let dst = new_remote_fs(&format!("fs://{}", dst_dir.display())).unwrap();
    Backup {
        concurrency: 2,
        src: &src,
        dst: dst.as_ref(),
        origin: None,
        created_at: Some("2026-07-05T00:00:00Z".into()),
    }
    .run()
    .unwrap();

    assert!(dst.has_file(names::BACKUP_COMPLETE_FILENAME).unwrap());
    let meta = dst.read_file(names::BACKUP_METADATA_FILENAME).unwrap();
    let meta: serde_json::Value = serde_json::from_slice(&meta).unwrap();
    assert_eq!(meta["created_at"], "2026-07-05T00:00:00Z");
    assert!(meta["completed_at"].is_string());

    // 5 parts uploaded (flock.lock excluded, zero-length file kept).
    assert_eq!(dst.list_parts().unwrap().len(), 5);

    let _ = std::fs::remove_dir_all(&src_dir);
    let _ = std::fs::remove_dir_all(&dst_dir);
}

#[test]
fn incremental_backup_uploads_only_changes() {
    let src_dir = test_dir("incr-src");
    let dst_dir = test_dir("incr-dst");
    make_src_tree(&src_dir);
    let src = LocalFs::new(&src_dir);
    let dst = new_remote_fs(&format!("fs://{}", dst_dir.display())).unwrap();
    let backup = || {
        Backup {
            concurrency: 2,
            src: &src,
            dst: dst.as_ref(),
            origin: None,
            created_at: None,
        }
        .run()
        .unwrap()
    };
    backup();
    let first = dst.list_parts().unwrap().len();

    // Simulate a merge: one part dir replaced by another, parts.json rewritten
    // with the SAME byte length (so only the unique-key rule re-uploads it).
    std::fs::remove_dir_all(src_dir.join("data/small/2026_07/0000000000000001")).unwrap();
    write_file(
        &src_dir,
        "data/small/2026_07/0000000000000002/values.bin",
        &[8u8; 2048],
    );
    write_file(
        &src_dir,
        "data/small/2026_07/0000000000000002/index.bin",
        b"idx",
    );
    write_file(
        &src_dir,
        "data/small/2026_07/parts.json",
        b"{\"Small\":[\"0000000000000002\"],\"Big\":[]}",
    );
    backup();

    let parts = dst.list_parts().unwrap();
    let paths: Vec<&str> = parts.iter().map(|p| p.path.as_str()).collect();
    assert!(paths.iter().any(|p| p.contains("0000000000000002")));
    assert!(
        !paths.iter().any(|p| p.contains("0000000000000001")),
        "old part must be deleted from dst"
    );
    assert_eq!(parts.len(), first, "same file count after replace");
    assert!(dst.has_file(names::BACKUP_COMPLETE_FILENAME).unwrap());

    let _ = std::fs::remove_dir_all(&src_dir);
    let _ = std::fs::remove_dir_all(&dst_dir);
}

#[test]
fn backup_with_origin_server_side_copies() {
    let src_dir = test_dir("origin-src");
    let old_dir = test_dir("origin-old");
    let new_dir = test_dir("origin-new");
    make_src_tree(&src_dir);
    let src = LocalFs::new(&src_dir);
    let old = new_remote_fs(&format!("fs://{}", old_dir.display())).unwrap();
    let new = new_remote_fs(&format!("fs://{}", new_dir.display())).unwrap();

    Backup {
        concurrency: 2,
        src: &src,
        dst: old.as_ref(),
        origin: None,
        created_at: None,
    }
    .run()
    .unwrap();
    Backup {
        concurrency: 2,
        src: &src,
        dst: new.as_ref(),
        origin: Some(old.as_ref()),
        created_at: None,
    }
    .run()
    .unwrap();

    assert_eq!(
        new.list_parts().unwrap().len(),
        old.list_parts().unwrap().len()
    );
    assert!(new.has_file(names::BACKUP_COMPLETE_FILENAME).unwrap());

    let _ = std::fs::remove_dir_all(&src_dir);
    let _ = std::fs::remove_dir_all(&old_dir);
    let _ = std::fs::remove_dir_all(&new_dir);
}

#[test]
fn restore_roundtrips_byte_for_byte() {
    let src_dir = test_dir("restore-src");
    let bak_dir = test_dir("restore-bak");
    let out_dir = test_dir("restore-out");
    make_src_tree(&src_dir);
    let src = LocalFs::new(&src_dir);
    let bak = new_remote_fs(&format!("fs://{}", bak_dir.display())).unwrap();
    Backup {
        concurrency: 2,
        src: &src,
        dst: bak.as_ref(),
        origin: None,
        created_at: None,
    }
    .run()
    .unwrap();

    Restore {
        concurrency: 2,
        src: bak.as_ref(),
        dst_dir: out_dir.clone(),
        skip_backup_complete_check: false,
    }
    .run()
    .unwrap();

    // Identical trees, minus local-only special files.
    let mut want = read_tree(&src_dir);
    want.retain(|(p, _)| p != "flock.lock");
    let mut got = read_tree(&out_dir);
    got.retain(|(p, _)| p != "flock.lock"); // restore creates its own flock
    assert_eq!(got, want);
    // The in-progress marker must be gone on success.
    assert!(!out_dir.join("restore-in-progress").exists());

    let _ = std::fs::remove_dir_all(&src_dir);
    let _ = std::fs::remove_dir_all(&bak_dir);
    let _ = std::fs::remove_dir_all(&out_dir);
}

#[test]
fn restore_refuses_incomplete_backup() {
    let bak_dir = test_dir("incomplete-bak");
    let out_dir = test_dir("incomplete-out");
    let bak = new_remote_fs(&format!("fs://{}", bak_dir.display())).unwrap();
    // A part but no completion marker.
    let p = esm_backup::part::Part {
        path: "f.bin".into(),
        file_size: 1,
        offset: 0,
        size: 1,
        actual_size: 1,
    };
    bak.upload_part(&p, &mut &b"x"[..]).unwrap();

    let err = Restore {
        concurrency: 1,
        src: bak.as_ref(),
        dst_dir: out_dir.clone(),
        skip_backup_complete_check: false,
    }
    .run()
    .unwrap_err();
    assert!(
        err.to_string().contains("backup_complete.ignore"),
        "err: {err}"
    );

    // With the skip flag it proceeds.
    Restore {
        concurrency: 1,
        src: bak.as_ref(),
        dst_dir: out_dir.clone(),
        skip_backup_complete_check: true,
    }
    .run()
    .unwrap();
    assert_eq!(std::fs::read(out_dir.join("f.bin")).unwrap(), b"x");

    let _ = std::fs::remove_dir_all(&bak_dir);
    let _ = std::fs::remove_dir_all(&out_dir);
}

#[test]
fn restore_deletes_local_files_missing_from_backup() {
    let src_dir = test_dir("rsync-src");
    let bak_dir = test_dir("rsync-bak");
    let out_dir = test_dir("rsync-out");
    make_src_tree(&src_dir);
    let src = LocalFs::new(&src_dir);
    let bak = new_remote_fs(&format!("fs://{}", bak_dir.display())).unwrap();
    Backup {
        concurrency: 2,
        src: &src,
        dst: bak.as_ref(),
        origin: None,
        created_at: None,
    }
    .run()
    .unwrap();

    // Pre-populate the restore target with a file NOT in the backup.
    write_file(
        &out_dir,
        "data/small/2026_07/9999999999999999/values.bin",
        b"stale",
    );
    Restore {
        concurrency: 2,
        src: bak.as_ref(),
        dst_dir: out_dir.clone(),
        skip_backup_complete_check: false,
    }
    .run()
    .unwrap();
    assert!(!out_dir.join("data/small/2026_07/9999999999999999").exists());

    let _ = std::fs::remove_dir_all(&src_dir);
    let _ = std::fs::remove_dir_all(&bak_dir);
    let _ = std::fs::remove_dir_all(&out_dir);
}
