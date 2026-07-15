//! Reserved file names. Go: lib/backup/backupnames.

/// Written to the destination as the LAST step of a backup; its presence
/// means the backup is complete and valid.
pub const BACKUP_COMPLETE_FILENAME: &str = "backup_complete.ignore";
/// JSON `{created_at, completed_at}` written just before the complete marker.
pub const BACKUP_METADATA_FILENAME: &str = "backup_metadata.ignore";
/// Created locally at restore start, removed on success. esm-storage
/// refuses to open a data dir containing it.
pub const RESTORE_IN_PROGRESS_FILENAME: &str = "restore-in-progress";
/// Reserved by upstream backupmanager; excluded from local listings.
pub const RESTORE_MARK_FILENAME: &str = "backup_restore.ignore";
/// esm-storage's exclusive-lock file; never backed up.
pub const FLOCK_FILENAME: &str = "flock.lock";
