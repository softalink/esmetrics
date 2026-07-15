//! parts.json handling and on-disk recovery (`mustOpenParts` and friends
//! from `table.go`).

use std::path::Path;
use std::sync::Arc;

use crate::filenames::PARTS_FILENAME;
use crate::part::must_open_file_part;
use crate::part_wrapper::PartWrapper;

pub(crate) fn must_write_part_names(pws: &[Arc<PartWrapper>], dst_dir: &Path) {
    let mut part_names: Vec<String> = pws
        .iter()
        .filter(|pw| pw.mp.is_none()) // skip in-memory parts
        .map(|pw| {
            pw.p.path
                .file_name()
                .unwrap_or_else(|| panic!("BUG: part path {:?} has no base name", pw.p.path))
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    part_names.sort();
    let data = serde_json::to_vec(&part_names)
        .unwrap_or_else(|e| panic!("BUG: cannot marshal partNames to JSON: {e}"));
    let parts_file = dst_dir.join(PARTS_FILENAME);
    esm_common::fs::must_write_atomic(&parts_file, &data, true);
}

pub(crate) fn must_read_part_names(parts_file: &Path, src_dir: &Path) -> Vec<String> {
    if esm_common::fs::is_path_exist(parts_file) {
        let data = std::fs::read(parts_file)
            .unwrap_or_else(|e| panic!("FATAL: cannot read {parts_file:?}: {e}"));
        let part_names: Vec<String> = serde_json::from_slice(&data)
            .unwrap_or_else(|e| panic!("FATAL: cannot parse {parts_file:?}: {e}"));
        return part_names;
    }
    // The parts.json is missing (upgrade from old versions). Read part names
    // from the directories under src_dir.
    let mut part_names = Vec::new();
    for de in esm_common::fs::must_read_dir(src_dir) {
        if !esm_common::fs::is_dir_or_symlink(&de) {
            // Skip non-directories.
            continue;
        }
        let part_name = de.file_name().to_string_lossy().into_owned();
        if is_special_dir(&part_name) {
            // Skip special dirs.
            continue;
        }
        part_names.push(part_name);
    }
    part_names
}

pub(crate) fn must_open_parts(path: &Path) -> Vec<Arc<PartWrapper>> {
    // Remove txn and tmp directories, which may be left after unclean
    // shutdown of old versions.
    esm_common::fs::must_remove_dir(path.join("txn"));
    esm_common::fs::must_remove_dir(path.join("tmp"));

    let parts_file = path.join(PARTS_FILENAME);
    let part_names = must_read_part_names(&parts_file, path);

    // Remove dirs missing in part_names. These dirs may be left after unclean
    // shutdown or after the update from old versions.
    let mut m = std::collections::HashSet::new();
    for part_name in &part_names {
        // Make sure the part exists on disk. If it is missing, then manual
        // action from the user is needed, since this is an unexpected state,
        // which cannot occur under normal operation, including unclean
        // shutdown.
        let part_path = path.join(part_name);
        assert!(
            esm_common::fs::is_path_exist(&part_path),
            "FATAL: part {part_path:?} is listed in {parts_file:?}, but is missing on disk; \
             ensure {parts_file:?} contents is not corrupted; remove {part_path:?} from \
             {parts_file:?} in order to restore access to the remaining data",
        );
        m.insert(part_name.clone());
    }
    for de in esm_common::fs::must_read_dir(path) {
        if !esm_common::fs::is_dir_or_symlink(&de) {
            // Skip non-directories.
            continue;
        }
        let fn_name = de.file_name().to_string_lossy().into_owned();
        if !m.contains(&fn_name) {
            let delete_path = path.join(&fn_name);
            log::info!(
                "deleting {delete_path:?} because it isn't listed in {parts_file:?}; \
                 this is the expected case after unclean shutdown"
            );
            esm_common::fs::must_remove_dir(&delete_path);
        }
    }

    // Open the parts.
    let pws: Vec<Arc<PartWrapper>> = part_names
        .iter()
        .map(|part_name| {
            let part_path = path.join(part_name);
            PartWrapper::new_from_file_part(must_open_file_part(&part_path))
        })
        .collect();

    if !esm_common::fs::is_path_exist(&parts_file) {
        // Create parts.json if it doesn't exist yet: this protects from
        // possible crashloops just after the migration from old versions.
        must_write_part_names(&pws, path);
    }

    pws
}

pub(crate) fn is_special_dir(name: &str) -> bool {
    // Snapshots and cache dirs aren't used anymore.
    // Keep them here for backwards compatibility.
    name == "tmp" || name == "txn" || name == "snapshots" || name == "cache"
}
