//! `scrape_configs` YAML parsing and validation (config parse + validate
//! only — see the module doc in `scrape/mod.rs` for what's deferred).
//!
//! Port of `lib/promscrape/config.go`'s `Config`/`GlobalConfig`/
//! `ScrapeConfig` yaml shapes plus the parse-time and post-parse validation
//! upstream applies before starting service discovery. The supported
//! discovery families (`static_configs`/`file_sd_configs`/`http_sd_configs`/
//! `kubernetes_sd_configs`/`consul_sd_configs`/`consulagent_sd_configs`/
//! `ec2_sd_configs`/
//! `gce_sd_configs`/`digitalocean_sd_configs`/`hetzner_sd_configs`/
//! `nomad_sd_configs`/`marathon_sd_configs`/`vultr_sd_configs`/
//! `puppetdb_sd_configs`/`kuma_sd_configs`/`eureka_sd_configs`/
//! `yandexcloud_sd_configs`/
//! `ovhcloud_sd_configs`/`openstack_sd_configs`/`dns_sd_configs`/
//! `docker_sd_configs`/`dockerswarm_sd_configs`) parse into typed configs.
//! With the Docker Swarm provider now ported, the ENTIRE non-Kubernetes
//! cloud-SD surface is implemented and [`CLOUD_SD_KEYS`] is empty — no SD key
//! is rejected as "deferred" anymore; only genuinely-unknown keys are
//! rejected (as unknown fields) so a typo still fails loudly instead of
//! silently scraping nothing.
//!
//! ## Deliberate divergences from `config.go` (all task-brief-driven, noted
//! here rather than scattered across the file)
//!
//! - `GlobalConfig` here has no `relabel_configs`/`metric_relabel_configs`
//!   (upstream has both) — target-relabel is a later task's scope, not
//!   this one's.
//! - `honor_timestamps` defaults to `true` here, vs. upstream vmagent's
//!   deliberate `false` default (`config.go`'s comment on
//!   `ScrapeConfig.HonorTimestamps`) — the task brief specifies `true`
//!   (matching plain Prometheus semantics) for this port.
//! - `scrape_timeout > scrape_interval` is a hard validation error here;
//!   upstream silently clamps `scrape_timeout` down to `scrape_interval`
//!   (`getScrapeWorkConfig`). The task brief lists this under `validate`
//!   alongside the other hard errors (unique `job_name`, `scheme`), so it's
//!   treated the same way.
//! - `max_scrape_size` is a plain `u64` byte count here; upstream accepts a
//!   suffixed byte-size string (`"16MB"`) via `flagutil.ParseBytes`. No
//!   test in this task's scope exercises a suffixed value, so the simpler
//!   shape was chosen (YAGNI) rather than porting a byte-size parser.
//! - `file_sd_configs`/`http_sd_configs` are parse-only placeholders here
//!   (shape + defaults for a later discovery task) — upstream's
//!   `FileSDConfig` doesn't even have `refresh_interval` (a global flag
//!   controls the file-SD poll interval instead); the task brief asks for
//!   the field anyway, matching plain Prometheus's `file_sd_config` shape,
//!   for the discovery task to consume later.
//! - `serde_json` was not added as a dependency: everything here is
//!   YAML-only (`serde_yaml_ng::Value` covers the "capture raw, re-parse"
//!   need for `relabel_configs`/`metric_relabel_configs`), so adding it
//!   would be an unused dependency.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use serde::Deserialize;

use crate::client::{AuthConfig, TlsConfig};
use crate::scrape::kubernetes::oauth2::OAuth2Config;

/// Cloud service-discovery keys still deferred (rejected at parse time with
/// the dedicated "unsupported (deferred)" message). With `dockerswarm_sd_configs`
/// now ported — the last remaining deferred cloud SD key — the ENTIRE
/// non-Kubernetes promscrape SD surface is implemented, so this list is EMPTY.
/// Every SD family (`static`/`file`/`http`/`kubernetes` plus
/// `consul`/`consulagent`/`ec2`/`gce`/`azure`/`digitalocean`/`hetzner`/
/// `nomad`/`marathon`/`vultr`/`puppetdb`/`kuma`/`eureka`/`yandexcloud`/
/// `ovhcloud`/`openstack`/`dns`/`docker`/`dockerswarm`) parses into a typed
/// config. It is kept as a (currently empty) slice so `reject_unsupported_keys`
/// keeps compiling and a future not-yet-ported key can be re-added here; an
/// empty slice means nothing is rejected as a "deferred cloud key" — unknown
/// keys still fail as unknown fields.
const CLOUD_SD_KEYS: &[&str] = &[];

/// Error returned by [`parse_scrape_config`]/[`validate`]. Never carries a
/// full YAML document, only the underlying parser's message — mirrors
/// `esm_relabel::RelabelError`'s shape.
#[derive(Debug)]
pub struct ScrapeError {
    pub msg: String,
}

impl ScrapeError {
    pub(crate) fn new(msg: impl Into<String>) -> Self {
        ScrapeError { msg: msg.into() }
    }
}

impl fmt::Display for ScrapeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for ScrapeError {}

/// Port of `GlobalConfig` (`config.go:278-286`), narrowed to this task's
/// scope — see the module doc for what's omitted.
#[derive(Debug, Clone, PartialEq)]
pub struct GlobalConfig {
    pub scrape_interval: Duration,
    pub scrape_timeout: Duration,
    pub external_labels: BTreeMap<String, String>,
    pub sample_limit: usize,
    pub label_limit: usize,
}

/// Upstream default `scrape_interval` (`config.go:1389`, `time.Minute`).
const DEFAULT_SCRAPE_INTERVAL: Duration = Duration::from_secs(60);
/// Upstream default `scrape_timeout` (`config.go:1390`).
const DEFAULT_SCRAPE_TIMEOUT: Duration = Duration::from_secs(10);
/// Default Prometheus `file_sd_config.refresh_interval`.
const DEFAULT_FILE_SD_REFRESH_INTERVAL: Duration = Duration::from_secs(5 * 60);
/// Default Prometheus `http_sd_config.refresh_interval`.
const DEFAULT_HTTP_SD_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// `consul_sd_config`'s typed shape lives in `scrape::consul` (see its doc);
/// re-exported here so the parse layer and every existing
/// `scrape::config::ConsulSdConfig` reference keep working.
pub use super::consul::ConsulSdConfig;

/// `consulagent_sd_config`'s typed shape lives in `scrape::consulagent` (see
/// its doc); re-exported here (mirroring [`ConsulSdConfig`]) so the parse layer
/// and every `scrape::config::ConsulagentSdConfig` reference keep working while
/// keeping `config.rs` under the repo's 800-line cap.
pub use super::consulagent::ConsulagentSdConfig;

/// `ec2_sd_config`'s typed shape lives in `scrape::ec2` (see its doc);
/// re-exported here (mirroring [`ConsulSdConfig`]) so the parse layer and
/// every `scrape::config::Ec2SdConfig` reference keep working while keeping
/// `config.rs` under the repo's 800-line cap.
pub use super::ec2::{Ec2Filter, Ec2SdConfig};

/// `gce_sd_config`'s typed shape lives in `scrape::gce` (see its doc);
/// re-exported here (mirroring [`Ec2SdConfig`]) so the parse layer and every
/// `scrape::config::GceSdConfig` reference keep working while keeping
/// `config.rs` under the repo's 800-line cap.
pub use super::gce::GceSdConfig;

/// `azure_sd_config`'s typed shape lives in `scrape::azure` (see its doc);
/// re-exported here (mirroring [`GceSdConfig`]) so the parse layer and every
/// `scrape::config::AzureSdConfig` reference keep working while keeping
/// `config.rs` under the repo's 800-line cap.
pub use super::azure::AzureSdConfig;

/// `digitalocean_sd_config`'s typed shape lives in `scrape::digitalocean` (see
/// its doc); re-exported here (mirroring [`ConsulSdConfig`] / [`Ec2SdConfig`])
/// so the parse layer and every `scrape::config::DigitaloceanSdConfig`
/// reference keep working while keeping `config.rs` under the repo's 800-line
/// cap.
pub use super::digitalocean::DigitaloceanSdConfig;

/// `hetzner_sd_config`'s typed shape lives in `scrape::hetzner` (see its doc);
/// re-exported here (mirroring [`DigitaloceanSdConfig`]) so the parse layer and
/// every `scrape::config::HetznerSdConfig` reference keep working while keeping
/// `config.rs` under the repo's 800-line cap.
pub use super::hetzner::HetznerSdConfig;

/// `nomad_sd_config`'s typed shape lives in `scrape::nomad` (see its doc);
/// re-exported here (mirroring [`ConsulSdConfig`] / [`Ec2SdConfig`] /
/// [`DigitaloceanSdConfig`]) so the parse layer and every
/// `scrape::config::NomadSdConfig` reference keep working while keeping
/// `config.rs` under the repo's 800-line cap.
pub use super::nomad::NomadSdConfig;

/// `marathon_sd_config`'s typed shape lives in `scrape::marathon` (see its doc);
/// re-exported here (mirroring [`NomadSdConfig`] / [`DigitaloceanSdConfig`]) so
/// the parse layer and every `scrape::config::MarathonSdConfig` reference keep
/// working while keeping `config.rs` under the repo's 800-line cap.
pub use super::marathon::MarathonSdConfig;

/// `vultr_sd_config`'s typed shape lives in `scrape::vultr` (see its doc);
/// re-exported here (mirroring [`DigitaloceanSdConfig`]) so the parse layer and
/// every `scrape::config::VultrSdConfig` reference keep working while keeping
/// `config.rs` under the repo's 800-line cap.
pub use super::vultr::VultrSdConfig;

/// `puppetdb_sd_config`'s typed shape lives in `scrape::puppetdb` (see its
/// doc); re-exported here (mirroring [`DigitaloceanSdConfig`] /
/// [`NomadSdConfig`]) so the parse layer and every
/// `scrape::config::PuppetdbSdConfig` reference keep working while keeping
/// `config.rs` under the repo's 800-line cap.
pub use super::puppetdb::PuppetdbSdConfig;

/// `kuma_sd_config`'s typed shape lives in `scrape::kuma` (see its doc);
/// re-exported here (mirroring [`PuppetdbSdConfig`]) so the parse layer and
/// every `scrape::config::KumaSdConfig` reference keep working while keeping
/// `config.rs` under the repo's 800-line cap.
pub use super::kuma::KumaSdConfig;

/// `eureka_sd_config`'s typed shape lives in `scrape::eureka` (see its doc);
/// re-exported here (mirroring [`DigitaloceanSdConfig`] / [`Ec2SdConfig`]) so
/// the parse layer and every `scrape::config::EurekaSdConfig` reference keep
/// working while keeping `config.rs` under the repo's 800-line cap.
pub use super::eureka::EurekaSdConfig;

/// `yandexcloud_sd_config`'s typed shape lives in `scrape::yandexcloud` (see its
/// doc); re-exported here (mirroring [`GceSdConfig`]) so the parse layer and
/// every `scrape::config::YandexcloudSdConfig` reference keep working while
/// keeping `config.rs` under the repo's 800-line cap.
pub use super::yandexcloud::YandexcloudSdConfig;

/// `ovhcloud_sd_config`'s typed shape lives in `scrape::ovhcloud` (see its
/// doc); re-exported here (mirroring [`HetznerSdConfig`]) so the parse layer and
/// every `scrape::config::OvhcloudSdConfig` reference keep working while keeping
/// `config.rs` under the repo's 800-line cap.
pub use super::ovhcloud::OvhcloudSdConfig;

/// `openstack_sd_config`'s typed shape lives in `scrape::openstack` (see its
/// doc); re-exported here (mirroring [`OvhcloudSdConfig`]) so the parse layer
/// and every `scrape::config::OpenstackSdConfig` reference keep working while
/// keeping `config.rs` under the repo's 800-line cap.
pub use super::openstack::OpenstackSdConfig;

/// `dns_sd_config`'s typed shape lives in `scrape::dns` (see its doc);
/// re-exported here (mirroring [`DigitaloceanSdConfig`]) so the parse layer
/// and every `scrape::config::DnsSdConfig` reference keep working while
/// keeping `config.rs` under the repo's 800-line cap.
pub use super::dns::DnsSdConfig;

/// `docker_sd_config`'s typed shape lives in `scrape::docker` (see its doc);
/// re-exported here (mirroring [`DigitaloceanSdConfig`]) so the parse layer
/// and every `scrape::config::DockerSdConfig` reference keep working while
/// keeping `config.rs` under the repo's 800-line cap.
pub use super::docker::DockerSdConfig;

/// `dockerswarm_sd_config`'s typed shape lives in `scrape::dockerswarm` (see
/// its doc); re-exported here (mirroring [`DockerSdConfig`]) so the parse
/// layer and every `scrape::config::DockerswarmSdConfig` reference keep
/// working while keeping `config.rs` under the repo's 800-line cap.
pub use super::dockerswarm::DockerswarmSdConfig;

impl Default for GlobalConfig {
    fn default() -> Self {
        GlobalConfig {
            scrape_interval: DEFAULT_SCRAPE_INTERVAL,
            scrape_timeout: DEFAULT_SCRAPE_TIMEOUT,
            external_labels: BTreeMap::new(),
            sample_limit: 0,
            label_limit: 0,
        }
    }
}

/// Port of `ScrapeConfig` (`config.go:291-352`), narrowed to this task's
/// scope (parse + validate; no service discovery). `scrape_interval`/
/// `scrape_timeout` stay `Option` — unset means "fall back to
/// [`GlobalConfig`]", resolved in [`validate`].
#[derive(Debug, Clone, Default)]
pub struct ScrapeConfig {
    pub job_name: String,
    pub scrape_interval: Option<Duration>,
    pub scrape_timeout: Option<Duration>,
    pub metrics_path: String,
    pub scheme: String,
    pub honor_labels: bool,
    pub honor_timestamps: bool,
    pub params: BTreeMap<String, Vec<String>>,
    pub relabel_configs: Vec<esm_relabel::RelabelConfig>,
    pub metric_relabel_configs: Vec<esm_relabel::RelabelConfig>,
    pub sample_limit: usize,
    pub label_limit: usize,
    pub max_scrape_size: u64,
    pub enable_compression: bool,
    pub auth: AuthConfig,
    pub tls: TlsConfig,
    pub static_configs: Vec<StaticConfig>,
    pub file_sd_configs: Vec<FileSdConfig>,
    pub http_sd_configs: Vec<HttpSdConfig>,
    pub kubernetes_sd_configs: Vec<KubernetesSdConfig>,
    pub consul_sd_configs: Vec<ConsulSdConfig>,
    pub consulagent_sd_configs: Vec<ConsulagentSdConfig>,
    pub ec2_sd_configs: Vec<Ec2SdConfig>,
    pub gce_sd_configs: Vec<GceSdConfig>,
    pub azure_sd_configs: Vec<AzureSdConfig>,
    pub digitalocean_sd_configs: Vec<DigitaloceanSdConfig>,
    pub hetzner_sd_configs: Vec<HetznerSdConfig>,
    pub nomad_sd_configs: Vec<NomadSdConfig>,
    pub marathon_sd_configs: Vec<MarathonSdConfig>,
    pub vultr_sd_configs: Vec<VultrSdConfig>,
    pub puppetdb_sd_configs: Vec<PuppetdbSdConfig>,
    pub kuma_sd_configs: Vec<KumaSdConfig>,
    pub eureka_sd_configs: Vec<EurekaSdConfig>,
    pub yandexcloud_sd_configs: Vec<YandexcloudSdConfig>,
    pub ovhcloud_sd_configs: Vec<OvhcloudSdConfig>,
    pub openstack_sd_configs: Vec<OpenstackSdConfig>,
    pub dns_sd_configs: Vec<DnsSdConfig>,
    pub docker_sd_configs: Vec<DockerSdConfig>,
    pub dockerswarm_sd_configs: Vec<DockerswarmSdConfig>,
}

/// Port of `StaticConfig` (`config.go:446-449`).
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct StaticConfig {
    pub targets: Vec<String>,
    pub labels: BTreeMap<String, String>,
}

/// Port of Prometheus's `file_sd_config` shape (upstream `FileSDConfig` has
/// no `refresh_interval` — see the module doc).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct FileSdConfig {
    pub files: Vec<String>,
    #[serde(deserialize_with = "deserialize_duration")]
    pub refresh_interval: Duration,
}

impl Default for FileSdConfig {
    fn default() -> Self {
        FileSdConfig {
            files: Vec::new(),
            refresh_interval: DEFAULT_FILE_SD_REFRESH_INTERVAL,
        }
    }
}

/// Simplified local `http_sd_config` shape for a later discovery task to
/// consume — not upstream's `discovery/http.SDConfig` (see the module doc).
/// Built via [`build_http_sd_config`] from [`RawHttpSdConfig`] rather than
/// deserialized directly, for the same reason as [`ScrapeConfig::auth`]:
/// `basic_auth:`/`bearer_token:` are top-level yaml keys, not a nested
/// `AuthConfig`-shaped object.
#[derive(Debug, Clone, PartialEq)]
pub struct HttpSdConfig {
    pub url: String,
    pub refresh_interval: Duration,
    pub auth: AuthConfig,
    pub tls: TlsConfig,
}

impl Default for HttpSdConfig {
    fn default() -> Self {
        HttpSdConfig {
            url: String::new(),
            refresh_interval: DEFAULT_HTTP_SD_REFRESH_INTERVAL,
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
        }
    }
}

/// Simplified local `kubernetes_sd_config` shape. Not upstream's
/// `kubernetes.SDConfig`: this is the subset of fields this port validates
/// and drives discovery from — all six roles (`pod`/`node`/`service`/
/// `ingress`/`endpoints`/`endpointslice`) plus `attach_metadata` are wired
/// (see `kubernetes::roles`/`kubernetes::registry`); `kubeconfig_file` auth
/// (and its cluster `proxy_url`) is resolved at discovery time by
/// `kubernetes::kubeconfig`, and is mutually exclusive with `api_server`.
/// Built via [`build_kubernetes_sd_config`] from [`RawKubernetesSdConfig`],
/// mirroring [`HttpSdConfig`]'s inline `auth`/`tls` handling.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct KubernetesSdConfig {
    pub role: String,
    pub api_server: Option<String>,
    pub kubeconfig_file: Option<String>,
    /// `proxy_url` — an HTTP proxy for the API-server client, applied on the
    /// explicit-`api_server` and in-cluster auth paths (the `kubeconfig_file`
    /// path uses the kubeconfig's own cluster `proxy-url` instead). Port of
    /// upstream `kubernetes.SDConfig.ProxyURL`.
    pub proxy_url: Option<String>,
    pub namespaces: K8sNamespaces,
    pub selectors: Vec<K8sSelector>,
    pub attach_metadata: Option<K8sAttachMetadata>,
    /// OAuth2 client-credentials auth for the API-server client. Port of
    /// upstream `kubernetes.SDConfig.OAuth2` (`promauth.OAuth2Config`).
    pub oauth2: Option<OAuth2Config>,
    pub auth: AuthConfig,
    pub tls: TlsConfig,
}

/// Port of `kubernetes.Namespaces` (`kubernetes.go`). Deserialized directly
/// (no raw/typed split needed — every field is already a natural yaml
/// shape).
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct K8sNamespaces {
    pub own_namespace: bool,
    pub names: Vec<String>,
}

/// Port of a `kubernetes_sd_config`'s `selectors[]` entry
/// (`kubernetes.go`'s `Selector`).
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct K8sSelector {
    pub role: String,
    pub label: Option<String>,
    pub field: Option<String>,
}

/// Port of `kubernetes_sd_config`'s `attach_metadata` (`kubernetes.go`'s
/// `AttachMetadata`).
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct K8sAttachMetadata {
    pub node: bool,
    pub namespace: bool,
}

/// Top-level parsed document: `global:` + `scrape_configs:`. Port of
/// `Config` (`config.go:129-136`), narrowed to the two fields this task
/// parses (`scrape_config_files` — external file inclusion — is out of
/// scope).
#[derive(Debug, Clone, Default)]
pub struct ScrapeConfigFile {
    pub global: GlobalConfig,
    pub scrape_configs: Vec<ScrapeConfig>,
}

/// A duration deserialized via [`esm_metricsql::duration_value`] (same
/// Prometheus duration grammar used by every other duration in this
/// codebase — see `esmagent::flags::parse_duration_flag`), rejecting
/// negative values rather than wrapping them into a huge `Duration`.
fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    parse_duration(&s).map_err(serde::de::Error::custom)
}

fn parse_duration(s: &str) -> Result<Duration, ScrapeError> {
    let ms = esm_metricsql::duration_value(s, 0)
        .map_err(|e| ScrapeError::new(format!("invalid duration {s:?}: {e}")))?;
    if ms < 0 {
        return Err(ScrapeError::new(format!(
            "invalid duration {s:?}: duration must be non-negative"
        )));
    }
    Ok(Duration::from_millis(ms as u64))
}

/// Raw `serde_yaml_ng` deserialization, the `build_*` converters, and the
/// [`parse_scrape_config`] entry point live in a sibling module so this file
/// stays under the repo's 800-line cap.
#[path = "config_raw.rs"]
mod config_raw;
pub use config_raw::parse_scrape_config;
/// `RawAuthFields`/`RawBasicAuth` are consumed by the per-provider SD modules
/// (`scrape::consul`/`ec2`/`digitalocean`/`nomad`), so re-export them at
/// `config`'s level to keep their `super::config::RawAuthFields` imports valid.
pub(crate) use config_raw::{RawAuthFields, RawBasicAuth};

/// Post-parse [`validate`] lives in a sibling module (same rationale).
#[path = "config_validate.rs"]
mod config_validate;
pub use config_validate::validate;

#[cfg(test)]
#[path = "config_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "config_tests2.rs"]
mod tests2;
