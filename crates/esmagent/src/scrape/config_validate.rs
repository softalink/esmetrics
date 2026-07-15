//! Post-parse validation for a parsed [`super::ScrapeConfigFile`]: unique
//! `job_name`, `scheme`, `scrape_timeout` vs `scrape_interval`, and the
//! per-`kubernetes_sd_config` role/auth checks. Split out of [`super`]
//! (`config.rs`) to keep that file under the repo's 800-line cap.

use crate::scrape::kubernetes::oauth2;

use super::*;

/// Validates a parsed [`ScrapeConfigFile`]: unique `job_name`, `scheme` is
/// `http`/`https`, and each scrape config's resolved `scrape_timeout` (its
/// own value, else the global's) doesn't exceed its resolved
/// `scrape_interval`. Every relabel config already compiled successfully
/// during [`parse_scrape_config`], so there's nothing left to check there.
pub fn validate(cfg: &ScrapeConfigFile) -> Result<(), ScrapeError> {
    let mut seen_job_names = std::collections::HashSet::new();
    for sc in &cfg.scrape_configs {
        if !seen_job_names.insert(sc.job_name.as_str()) {
            return Err(ScrapeError::new(format!(
                "duplicate job_name {:?} in scrape_configs",
                sc.job_name
            )));
        }
        if sc.scheme != "http" && sc.scheme != "https" {
            return Err(ScrapeError::new(format!(
                "job_name {:?}: unexpected `scheme` {:?}; supported values: http or https",
                sc.job_name, sc.scheme
            )));
        }
        let scrape_interval = sc.scrape_interval.unwrap_or(cfg.global.scrape_interval);
        let scrape_timeout = sc.scrape_timeout.unwrap_or(cfg.global.scrape_timeout);
        if scrape_timeout > scrape_interval {
            return Err(ScrapeError::new(format!(
                "job_name {:?}: scrape_timeout ({scrape_timeout:?}) exceeds scrape_interval ({scrape_interval:?})",
                sc.job_name
            )));
        }
        for k in &sc.kubernetes_sd_configs {
            validate_kubernetes_sd_config(&sc.job_name, k)?;
        }
    }
    Ok(())
}

/// Port of `api.go`'s `newAPIConfig` role validation: `role` must be one of
/// `pod`/`node`/`service`/`ingress`/`endpoints`/`endpointslice` (the
/// `endpointslices` alias is already normalized to `endpointslice` in
/// [`build_kubernetes_sd_config`]); anything else is rejected with upstream's
/// exact wording. `api_server` and `kubeconfig_file` are mutually exclusive;
/// `kubeconfig_file` alone is now valid (its file is read and parsed at
/// discovery-resolution time, not here — see `kubernetes::kubeconfig`).
fn validate_kubernetes_sd_config(
    job_name: &str,
    k: &KubernetesSdConfig,
) -> Result<(), ScrapeError> {
    match k.role.as_str() {
        "pod" | "node" | "service" | "ingress" | "endpoints" | "endpointslice" => {}
        other => {
            return Err(ScrapeError::new(format!(
                "job_name {job_name:?}: unexpected role: {other}; must be one of node, pod, service, endpoints, endpointslice or ingress"
            )));
        }
    }
    if k.api_server.is_some() && k.kubeconfig_file.is_some() {
        return Err(ScrapeError::new(format!(
            "job_name {job_name:?}: `api_server` and `kubeconfig_file` cannot be set simultaneously"
        )));
    }
    if let Some(o) = &k.oauth2 {
        oauth2::validate(o)
            .map_err(|e| ScrapeError::new(format!("job_name {job_name:?}: {}", e.msg)))?;
    }
    Ok(())
}
