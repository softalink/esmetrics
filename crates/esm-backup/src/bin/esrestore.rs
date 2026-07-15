//! esrestore — restores esmetrics data from a backup made by esbackup.
//! The esmetrics server must be stopped. Go: app/vmrestore.

use esm_backup::cliflags::FlagSet;
use esm_backup::remote::new_remote_fs;
use esm_backup::restore::Restore;

const FLAG_DEFS: &[(&str, &str, &str)] = &[
    ("src", "", "Source backup URL: fs:///abs/dir, s3://bucket/dir, gs://bucket/dir or azblob://container/dir"),
    ("storageDataPath", "esmetrics-data", "Destination path. Data is synced with the backup \
      (extra local files are DELETED, like rsync --delete)"),
    ("concurrency", "10", "The number of concurrent workers"),
    ("skipBackupCompleteCheck", "false", "Whether to skip checking for the backup_complete.ignore marker"),
];

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let mut flags = FlagSet::new("esrestore", FLAG_DEFS);
    flags.parse();
    if let Err(e) = run(&flags) {
        log::error!("esrestore failed: {e:#}");
        std::process::exit(1);
    }
}

fn run(flags: &FlagSet) -> anyhow::Result<()> {
    let src_url = flags.get("src").to_string();
    anyhow::ensure!(!src_url.is_empty(), "-src cannot be empty");
    let src = new_remote_fs(&src_url)?;
    Restore {
        concurrency: flags.get_usize("concurrency"),
        src: src.as_ref(),
        dst_dir: std::path::PathBuf::from(flags.get("storageDataPath")),
        skip_backup_complete_check: flags.get_bool("skipBackupCompleteCheck"),
    }
    .run()
}
