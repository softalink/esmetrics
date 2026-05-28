//! Path canonicalisation and validation.
//!
//! Centralises the handful of cross-platform path concerns EsMetrics cares
//! about:
//! * Resolving a possibly-relative path to its absolute form without requiring
//!   the path to exist (data directories are created lazily).
//! * Rejecting path components that contain characters NTFS forbids but POSIX
//!   tolerates — surfacing those mistakes early on Linux and macOS, not just
//!   when a user moves a backup to a Windows box.

use std::io;
use std::path::{Path, PathBuf};

/// Canonicalise a path that may not yet exist, suitable for use as a data
/// directory root.
///
/// Returns the absolute form of `path`. On Windows, additionally rejects path
/// components containing characters forbidden by NTFS (`<`, `>`, `"`, `|`,
/// `?`, `*`).
pub fn canonical_data_path(path: &Path) -> io::Result<PathBuf> {
    let absolute = std::path::absolute(path)?;
    #[cfg(windows)]
    validate_windows_path(&absolute)?;
    Ok(absolute)
}

/// Returns `true` if `c` is a character NTFS rejects in a path component but
/// that POSIX accepts. Used by both Windows runtime validation and a
/// cross-platform pre-check helper exposed for early surface-area testing.
#[must_use]
pub fn is_forbidden_on_windows(c: char) -> bool {
    matches!(c, '<' | '>' | '"' | '|' | '?' | '*')
}

#[cfg(windows)]
fn validate_windows_path(path: &Path) -> io::Result<()> {
    for component in path.components() {
        let std::path::Component::Normal(raw) = component else { continue };
        let s = raw.to_string_lossy();
        if let Some(bad) = s.chars().find(|c| is_forbidden_on_windows(*c)) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("path component '{s}' contains character '{bad}' forbidden on Windows"),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_path_becomes_absolute() {
        let result = canonical_data_path(Path::new("./tmp/data")).unwrap();
        assert!(result.is_absolute(), "expected absolute, got {result:?}");
    }

    #[test]
    fn nonexistent_path_does_not_error() {
        let result = canonical_data_path(Path::new("/nonexistent/path/that/does/not/exist"));
        assert!(result.is_ok(), "absolute() should not require existence");
    }

    #[test]
    fn forbidden_chars_classification() {
        assert!(is_forbidden_on_windows('<'));
        assert!(is_forbidden_on_windows('?'));
        assert!(!is_forbidden_on_windows('a'));
        assert!(!is_forbidden_on_windows('/'));
        assert!(!is_forbidden_on_windows(':'));
    }
}
