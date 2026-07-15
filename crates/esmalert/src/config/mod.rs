//! Rule-group YAML config parsing.
//!
//! Port of `app/vmalert/config/config.go` (structs + strict parsing,
//! validation, expr/template validation, and per-group checksum).

mod checksum;
mod types;
mod validate;

// Re-exported for later CLI wiring; not yet consumed outside this module,
// so the re-export itself looks unused today.
#[allow(unused_imports)]
pub use types::{Config, Group, Header, Rule};
#[allow(unused_imports)]
pub use validate::{validate_config, validate_group};
// Used by `manager` to give every built `RuleKind` a stable identity `id`
// (see that function's doc comment); crate-internal only, not part of this
// module's public parsing/validation API.
pub(crate) use validate::rule_identity_hash;

use std::fmt;

/// Error returned by [`parse_config_str`]/[`load_config`]. Never panics on
/// malformed input; wraps the underlying YAML/IO/glob failure with context.
#[derive(Debug)]
pub struct ConfigError {
    msg: String,
}

impl ConfigError {
    fn new(msg: impl Into<String>) -> Self {
        ConfigError { msg: msg.into() }
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for ConfigError {}

impl From<serde_yaml_ng::Error> for ConfigError {
    fn from(e: serde_yaml_ng::Error) -> Self {
        ConfigError::new(e.to_string())
    }
}

impl From<std::io::Error> for ConfigError {
    fn from(e: std::io::Error) -> Self {
        ConfigError::new(e.to_string())
    }
}

/// Parses a single rule-group YAML document (**strict**: unknown fields on
/// `Group`/`Rule` are rejected). Port of `parseConfig` (`config.go:297-323`),
/// minus the multi-document-per-file loop (not needed by vmalert's actual
/// rule files) and the `envtemplate` env-var substitution pass.
pub fn parse_config_str(yaml: &str) -> Result<Config, ConfigError> {
    let cfg: Config = serde_yaml_ng::from_str(yaml)?;
    Ok(cfg)
}

/// Expands each glob pattern, parses every matched file, and concatenates
/// their groups into one [`Config`]. Port of `Parse`/`parse`
/// (`config.go:252-295`), minus per-group validation (Task 9).
pub fn load_config(globs: &[String]) -> Result<Config, ConfigError> {
    let mut groups = Vec::new();
    for pattern in globs {
        let paths = glob::glob(pattern)
            .map_err(|e| ConfigError::new(format!("invalid glob pattern {pattern:?}: {e}")))?;
        for entry in paths {
            let path =
                entry.map_err(|e| ConfigError::new(format!("failed to read glob entry: {e}")))?;
            let content = std::fs::read_to_string(&path)?;
            let cfg = parse_config_str(&content).map_err(|e| {
                ConfigError::new(format!("failed to parse {}: {e}", path.display()))
            })?;
            groups.extend(cfg.groups);
        }
    }
    Ok(Config { groups })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_config_concatenates_groups_from_matched_files() {
        let dir = std::env::temp_dir().join(format!(
            "esmalert-load-config-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("a.yml"), "groups:\n  - name: g1\n    rules: []\n").unwrap();
        std::fs::write(dir.join("b.yml"), "groups:\n  - name: g2\n    rules: []\n").unwrap();

        let pattern = dir.join("*.yml").to_string_lossy().to_string();
        let cfg = load_config(&[pattern]).unwrap();

        let mut names: Vec<&str> = cfg.groups.iter().map(|g| g.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, vec!["g1", "g2"]);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn load_config_surfaces_parse_error_with_file_context() {
        let dir = std::env::temp_dir().join(format!(
            "esmalert-load-config-err-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("bad.yml"), "groups:\n  - name: g1\n    bogus: 1\n").unwrap();

        let pattern = dir.join("*.yml").to_string_lossy().to_string();
        let err = load_config(&[pattern]).unwrap_err();
        assert!(
            err.to_string().contains("bad.yml"),
            "unexpected error: {err}"
        );

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
