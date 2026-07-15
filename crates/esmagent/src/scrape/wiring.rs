//! Wires the scrape engine (`ScrapeManager`) into the running esmagent
//! binary: `-promscrape.config` file loading/validation, the
//! `-promscrape.maxScrapeSize` per-job default, and the `GET
//! /api/v1/targets` HTTP route body. Kept out of `lib.rs` to keep that file
//! under the repo's 800-line cap (see its module doc).
//!
//! The per-SD-kind flag-default post-processing (the ~20
//! `apply_*_sd_check_interval` helpers, `apply_default_max_scrape_size`, and
//! `apply_kubernetes_attach_metadata_defaults`) plus their shared
//! [`apply_flag_defaults`] orchestration live in the sibling
//! [`super::wiring_intervals`] module (extracted to keep this file under the
//! 800-line cap); they are re-exported here so `scrape::wiring::apply_*`
//! references stay valid.
//!
//! ## Global relabel: why it's loaded twice
//!
//! `crate::sink::ForwardingSink` takes its `global_relabel:
//! Option<esm_relabel::ParsedConfigs>` BY VALUE (`ParsedConfigs` is not
//! `Clone`), and the scraped-data path (this module's `ScrapeManager`)
//! needs the identical global relabel config applied to what it pushes so
//! pushed and scraped series are relabeled consistently. Rather than thread
//! a shared `Arc<ParsedConfigs>` through `ForwardingSink` (a `lib.rs`-level
//! API change out of this task's scope), [`build_scrape_manager`] simply
//! re-reads and re-parses `-remoteWrite.relabelConfig` a second time via
//! [`crate::load_relabel_config`] — cheap (a small YAML file, read once at
//! startup and again on each `-promscrape.config` reload) and avoids
//! touching the pushed-ingestion pipeline at all.
//!
//! ## `-promscrape.suppressScrapeErrors`
//!
//! Wired end-to-end: [`build_scrape_manager`] sets
//! [`crate::scrape::manager::ManagerDeps::suppress_scrape_errors`] from
//! `flags.promscrape_suppress_scrape_errors`, which the manager threads to
//! every per-target worker. When `false` (the default, matching upstream
//! vmagent's default-on scrape-error logging), a worker whose scrape fails
//! emits a single `log::warn!` for that failure; when `true`, that log is
//! suppressed. Either way the failure is still recorded into
//! `ActiveTarget::last_error` for `/api/v1/targets` reporting — the log is
//! additive. See `crate::scrape::manager::worker::record_result`.

use std::sync::Arc;

use crate::flags::Flags;
use crate::scrape::config::{parse_scrape_config, validate, ScrapeConfigFile};
use crate::scrape::manager::{ManagerDeps, ScrapeManager, TargetsHandle};
use crate::scrape::status::targets_json;
use crate::sink::SeriesConsumer;

use super::wiring_intervals::apply_flag_defaults;
pub use super::wiring_intervals::*;

/// Reads, parses, and validates `-promscrape.config` at `path`. Never
/// carries the file's contents in its error — only `path` and the parser's
/// message (same convention as `crate::load_relabel_config`).
pub fn validate_scrape_config(path: &str) -> Result<ScrapeConfigFile, String> {
    let yaml = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read -promscrape.config {path:?}: {e}"))?;
    let cfg = parse_scrape_config(&yaml)
        .map_err(|e| format!("invalid -promscrape.config {path:?}: {e}"))?;
    validate(&cfg).map_err(|e| format!("invalid -promscrape.config {path:?}: {e}"))?;
    Ok(cfg)
}

/// Builds the running [`ScrapeManager`] from `flags`, or `Ok(None)` if
/// `-promscrape.config` is unset (scrape engine disabled). See the module
/// doc for the global-relabel double-load rationale.
pub fn build_scrape_manager(
    flags: &Flags,
    consumer: Arc<dyn SeriesConsumer>,
) -> Result<Option<ScrapeManager>, String> {
    let Some(path) = flags.promscrape_config.as_deref() else {
        return Ok(None);
    };

    let mut cfg = validate_scrape_config(path)?;
    apply_flag_defaults(&mut cfg, flags);

    let global_relabel = load_manager_relabel_copy(flags)?;

    let manager = ScrapeManager::start(
        cfg,
        ManagerDeps {
            global_relabel,
            consumer,
            suppress_scrape_errors: flags.promscrape_suppress_scrape_errors,
        },
    )
    .map_err(|e| format!("-promscrape.config: {e}"))?;
    Ok(Some(manager))
}

/// Re-parses `-remoteWrite.relabelConfig` for the scrape manager's own
/// copy — see the module doc's "why it's loaded twice" section.
fn load_manager_relabel_copy(flags: &Flags) -> Result<Option<esm_relabel::ParsedConfigs>, String> {
    if flags.remote_write_relabel_config.is_empty() {
        return Ok(None);
    }
    crate::load_relabel_config(&flags.remote_write_relabel_config)
        .map(Some)
        .map_err(|e| format!("-remoteWrite.relabelConfig: {e}"))
}

/// Re-reads, re-validates, and re-resolves `-promscrape.config` at `path`
/// (applying the `-promscrape.maxScrapeSize` default the same way
/// [`build_scrape_manager`] does), then hands the fresh config to
/// `manager.reload`. Returns whatever error reading/parsing/validating/
/// reloading produced; the caller (`App::reload_scrape_config`) logs it and
/// keeps the manager's previous config running rather than propagating a
/// panic or crash — this function itself never panics.
pub fn reload_scrape_manager(
    manager: &mut ScrapeManager,
    flags: &Flags,
    path: &str,
) -> Result<(), String> {
    let mut cfg = validate_scrape_config(path)?;
    apply_flag_defaults(&mut cfg, flags);
    manager.reload(cfg).map_err(|e| e.to_string())
}

/// Builds the `GET /api/v1/targets` JSON body for `handle`'s current
/// snapshot, applying `state`'s `active`/`dropped` filter — see
/// `scrape::status::targets_json`'s doc for the filter semantics.
pub fn targets_route_body(handle: &TargetsHandle, state: Option<&str>) -> String {
    targets_json(&handle.snapshot(), state)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_file(name: &str, contents: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "esmagent-wiring-test-{}-{}-{}",
            std::process::id(),
            name,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join(name);
        std::fs::write(&path, contents).expect("write temp file");
        path
    }

    #[test]
    fn validate_scrape_config_rejects_missing_file() {
        let err = validate_scrape_config("/nonexistent/esmagent/scrape.yml").unwrap_err();
        assert!(err.contains("-promscrape.config"), "{err}");
    }

    #[test]
    fn validate_scrape_config_accepts_a_valid_file() {
        let path = temp_file(
            "good.yml",
            "scrape_configs:\n  - job_name: node\n    static_configs:\n      - targets: ['h1:9100']\n",
        );
        let cfg = validate_scrape_config(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.scrape_configs.len(), 1);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn validate_scrape_config_rejects_cloud_sd_and_dup_job() {
        let path = temp_file(
            "bad.yml",
            "scrape_configs:\n  - job_name: k\n    azure_sd_configs: [{}]\n",
        );
        let err = validate_scrape_config(path.to_str().unwrap()).unwrap_err();
        assert!(err.contains("-promscrape.config"), "{err}");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());

        let dup_path = temp_file(
            "dup.yml",
            "scrape_configs:\n  - job_name: a\n    static_configs: [{targets: [x]}]\n  - job_name: a\n    static_configs: [{targets: [y]}]\n",
        );
        let err = validate_scrape_config(dup_path.to_str().unwrap()).unwrap_err();
        assert!(err.contains("-promscrape.config"), "{err}");
        let _ = std::fs::remove_dir_all(dup_path.parent().unwrap());
    }
}
