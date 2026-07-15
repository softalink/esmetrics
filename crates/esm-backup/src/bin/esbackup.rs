//! esbackup — backs up an esmetrics snapshot to fs/s3/gs/azblob.
//! Go: app/vmbackup.

use esm_backup::backup::Backup;
use esm_backup::cliflags::FlagSet;
use esm_backup::localfs::LocalFs;
use esm_backup::remote::new_remote_fs;
use esm_backup::timeutil;

const FLAG_DEFS: &[(&str, &str, &str)] = &[
    ("storageDataPath", "esmetrics-data", "Path to esmetrics data. Must match the server's -storageDataPath"),
    ("snapshotName", "", "Name of an existing snapshot under <storageDataPath>/snapshots to back up. \
      Not needed if -snapshot.createURL is set"),
    ("snapshot.createURL", "", "esmetrics create-snapshot URL, e.g. http://localhost:8428/snapshot/create. \
      When set, a snapshot is created, backed up and deleted afterwards"),
    ("dst", "", "Destination URL: fs:///abs/dir, s3://bucket/dir, gs://bucket/dir or azblob://container/dir. \
      Pointing -dst to an existing backup makes it incremental"),
    ("origin", "", "Optional URL of an existing backup for server-side copy of unchanged parts"),
    ("concurrency", "10", "The number of concurrent workers"),
];

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let mut flags = FlagSet::new("esbackup", FLAG_DEFS);
    flags.parse();
    if let Err(e) = run(&flags) {
        log::error!("esbackup failed: {e:#}");
        std::process::exit(1);
    }
}

fn run(flags: &FlagSet) -> anyhow::Result<()> {
    // ---- Read and validate every flag value up front. Nothing below this
    // block may fail (die()/process::exit/ensure!) once a snapshot has been
    // created via -snapshot.createURL, so that the delete-after-create step
    // further down always runs unconditionally. ----
    let storage_data_path = std::path::PathBuf::from(flags.get("storageDataPath"));
    let create_url = flags.get("snapshot.createURL").to_string();
    let snapshot_name_flag = flags.get("snapshotName").to_string();
    let dst_url = flags.get("dst").to_string();
    let origin_url = flags.get("origin").to_string();
    let concurrency = flags.get_usize("concurrency");

    anyhow::ensure!(!dst_url.is_empty(), "-dst cannot be empty");

    let use_create_url = !create_url.is_empty();
    if use_create_url {
        anyhow::ensure!(
            snapshot_name_flag.is_empty(),
            "-snapshotName and -snapshot.createURL cannot be set simultaneously"
        );
    } else {
        anyhow::ensure!(
            !snapshot_name_flag.is_empty(),
            "either -snapshotName or -snapshot.createURL must be set"
        );
        validate_snapshot_name(&snapshot_name_flag)?;
    }

    // Validate the create URL shape (and derive the delete URL) before
    // creating anything.
    let delete_url_base = if use_create_url {
        Some(derive_delete_url(&create_url)?)
    } else {
        None
    };

    // Refuse fs:// destinations inside the storage data path. Both sides are
    // lexically normalized first: std::path::absolute does not collapse
    // `..`, so a raw starts_with comparison could be bypassed with a
    // "storage/../elsewhere" style -dst.
    if let Some(dst_path) = dst_url.strip_prefix("fs://") {
        let storage_abs = normalize_lexically(&std::path::absolute(&storage_data_path)?);
        let dst_abs = normalize_lexically(&std::path::absolute(std::path::Path::new(dst_path))?);
        anyhow::ensure!(
            !dst_abs.starts_with(&storage_abs),
            "-dst must not point inside -storageDataPath"
        );
    }

    // ---- All validation is done. Create the snapshot, if requested. ----
    let mut created_via_url = false;
    let snapshot_name = if use_create_url {
        let name = create_snapshot(&create_url)?;
        created_via_url = true;
        log::info!("created snapshot {name}");
        name
    } else {
        snapshot_name_flag
    };

    // ---- Everything fallible from here on happens inside this closure, so
    // its outcome can be captured and the delete-after-create step below
    // still runs unconditionally regardless of success or failure. ----
    let result = (|| -> anyhow::Result<()> {
        if created_via_url {
            validate_snapshot_name(&snapshot_name)?;
        }

        let src_dir = storage_data_path.join("snapshots").join(&snapshot_name);
        anyhow::ensure!(src_dir.is_dir(), "snapshot dir {src_dir:?} does not exist");

        let src = LocalFs::new(&src_dir);
        let dst = new_remote_fs(&dst_url)?;
        let origin = match origin_url.as_str() {
            "" => None,
            url => Some(new_remote_fs(url)?),
        };
        Backup {
            concurrency,
            src: &src,
            dst: dst.as_ref(),
            origin: origin.as_deref(),
            // Go storeMetadata derives CreatedAt from snapshotutil.Time and
            // hard-errors on failure. The name is already validated above, so
            // this is Ok; passing Some(..) means the now-fallback in
            // Backup::run is never reached here.
            created_at: Some(timeutil::rfc3339_from_snapshot_name(&snapshot_name)?),
        }
        .run()
    })();

    // Always try to delete an auto-created snapshot, success or failure.
    if created_via_url {
        if let Some(delete_url_base) = &delete_url_base {
            if let Err(e) = delete_snapshot(delete_url_base, &snapshot_name) {
                log::warn!("cannot delete snapshot {snapshot_name}: {e:#}");
            }
        }
    }
    result
}

/// Splits `create_url` into (base, query) at the first `?`, requires `base`
/// to end with `/create`, and returns the corresponding delete URL (with the
/// same query string, if any, reattached). Must be called before creating
/// the snapshot so a malformed -snapshot.createURL is rejected up front.
fn derive_delete_url(create_url: &str) -> anyhow::Result<String> {
    let (base, query) = match create_url.split_once('?') {
        Some((b, q)) => (b, Some(q)),
        None => (create_url, None),
    };
    anyhow::ensure!(
        base.ends_with("/create"),
        "-snapshot.createURL must end with /create (before any query string)"
    );
    let delete_base = format!("{}/delete", &base[..base.len() - "/create".len()]);
    Ok(match query {
        Some(q) => format!("{delete_base}?{q}"),
        None => delete_base,
    })
}

/// Lexically normalizes `p` by collapsing `.` and `..` components without
/// touching the filesystem. `std::path::absolute` deliberately does not do
/// this (it only makes a path absolute), so containment checks that rely on
/// `Path::starts_with` need this first or `..` can bypass them.
fn normalize_lexically(p: &std::path::Path) -> std::path::PathBuf {
    use std::path::Component;
    let mut out = std::path::PathBuf::new();
    for comp in p.components() {
        match comp {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                out.push(comp.as_os_str());
            }
        }
    }
    out
}

/// Validates `name` exactly like Go `lib/snapshot/snapshotutil.Validate`:
/// it must match `^[0-9]{14}-[0-9A-Fa-f]+$` and carry a real calendar
/// timestamp. This is strictly stronger than a path-component check (the
/// regexp admits no `/`, `\`, `.` or `..`), so it also guarantees `name` is
/// safe to use as a single path component under <storageDataPath>/snapshots.
fn validate_snapshot_name(name: &str) -> anyhow::Result<()> {
    timeutil::rfc3339_from_snapshot_name(name)?;
    Ok(())
}

fn create_snapshot(create_url: &str) -> anyhow::Result<String> {
    let body = reqwest::blocking::get(create_url)
        .and_then(reqwest::blocking::Response::error_for_status)
        .map_err(|e| anyhow::anyhow!("snapshot create request failed: {}", e.without_url()))?
        .text()
        .map_err(|e| anyhow::anyhow!("snapshot create request failed: {}", e.without_url()))?;
    let v: serde_json::Value = serde_json::from_str(&body)?;
    anyhow::ensure!(
        v["status"] == "ok",
        "unexpected response from snapshot create: {body}"
    );
    v["snapshot"]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!("no snapshot name in response: {body}"))
}

fn delete_snapshot(delete_url_base: &str, name: &str) -> anyhow::Result<()> {
    let sep = if delete_url_base.contains('?') {
        '&'
    } else {
        '?'
    };
    let url = format!("{delete_url_base}{sep}snapshot={name}");
    reqwest::blocking::get(&url)
        .and_then(reqwest::blocking::Response::error_for_status)
        .map_err(|e| anyhow::anyhow!("snapshot delete request failed: {}", e.without_url()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    #[test]
    fn normalize_lexically_collapses_parent_dir() {
        assert_eq!(
            normalize_lexically(Path::new("/data/foo/../bar")),
            PathBuf::from("/data/bar")
        );
    }

    #[test]
    fn normalize_lexically_collapses_cur_dir() {
        assert_eq!(
            normalize_lexically(Path::new("/data/./foo/./bar")),
            PathBuf::from("/data/foo/bar")
        );
    }

    #[test]
    fn normalize_lexically_keeps_root() {
        assert_eq!(
            normalize_lexically(Path::new("/data/foo/../../bar")),
            PathBuf::from("/bar")
        );
    }

    #[test]
    fn derive_delete_url_requires_create_suffix() {
        assert!(derive_delete_url("http://localhost:8428/snapshot/other").is_err());
    }

    #[test]
    fn derive_delete_url_replaces_create_with_delete() {
        assert_eq!(
            derive_delete_url("http://localhost:8428/snapshot/create").unwrap(),
            "http://localhost:8428/snapshot/delete"
        );
    }

    #[test]
    fn derive_delete_url_reattaches_query_before_create_suffix_check() {
        assert_eq!(
            derive_delete_url("http://localhost:8428/snapshot/create?authKey=secret").unwrap(),
            "http://localhost:8428/snapshot/delete?authKey=secret"
        );
    }

    #[test]
    fn validate_snapshot_name_rejects_path_separators_and_dots() {
        assert!(validate_snapshot_name("").is_err());
        assert!(validate_snapshot_name(".").is_err());
        assert!(validate_snapshot_name("..").is_err());
        assert!(validate_snapshot_name("a/b").is_err());
        assert!(validate_snapshot_name("a\\b").is_err());
        assert!(validate_snapshot_name("20260705000000-0000000A").is_ok());
    }

    #[test]
    fn validate_snapshot_name_enforces_snapshotutil_pattern() {
        // These are safe single path components (the old check accepted them)
        // but upstream snapshotutil.Validate rejects them, so esbackup must
        // too rather than backing them up with a bogus fallback timestamp.
        assert!(validate_snapshot_name("mysnapshot").is_err());
        assert!(validate_snapshot_name("20260705123456").is_err()); // no hex suffix
        assert!(validate_snapshot_name("20261305000000-0A").is_err()); // month 13
        assert!(validate_snapshot_name("20260705000000-XYZ").is_err()); // non-hex suffix
    }
}
