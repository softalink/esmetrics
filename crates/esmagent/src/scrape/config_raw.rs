//! Raw `serde_yaml_ng` deserialization structs, their `build_*` converters,
//! and the parse entry point ([`parse_scrape_config`]) for `scrape_configs`
//! YAML. Split out of [`super`] (`config.rs`) to keep that file under the
//! repo's 800-line cap; the typed output structs live in `config.rs` and
//! post-parse validation in `config_validate.rs`.
//!
//! Each cloud provider's undefaulted `Raw<Provider>SdConfig` list-entry shape
//! and its `build_<provider>_sd_config` converter live in that provider's own
//! module (alongside its typed `<Provider>SdConfig`). [`parse_scrape_config`]
//! deserializes into the Raw shapes and calls the build fns.

use super::*;
use crate::scrape::azure::{build_azure_sd_config, RawAzureSdConfig};
use crate::scrape::consul::{build_consul_sd_config, RawConsulSdConfig};
use crate::scrape::consulagent::{build_consulagent_sd_config, RawConsulagentSdConfig};
use crate::scrape::digitalocean::{build_digitalocean_sd_config, RawDigitaloceanSdConfig};
use crate::scrape::dns::{build_dns_sd_config, RawDnsSdConfig};
use crate::scrape::docker::{build_docker_sd_config, RawDockerSdConfig};
use crate::scrape::dockerswarm::{build_dockerswarm_sd_config, RawDockerswarmSdConfig};
use crate::scrape::ec2::{build_ec2_sd_config, RawEc2SdConfig};
use crate::scrape::eureka::{build_eureka_sd_config, RawEurekaSdConfig};
use crate::scrape::gce::{build_gce_sd_config, RawGceSdConfig};
use crate::scrape::hetzner::{build_hetzner_sd_config, RawHetznerSdConfig};
use crate::scrape::kuma::{build_kuma_sd_config, RawKumaSdConfig};
use crate::scrape::marathon::{build_marathon_sd_config, RawMarathonSdConfig};
use crate::scrape::nomad::{build_nomad_sd_config, RawNomadSdConfig};
use crate::scrape::openstack::{build_openstack_sd_config, RawOpenstackSdConfig};
use crate::scrape::ovhcloud::{build_ovhcloud_sd_config, RawOvhcloudSdConfig};
use crate::scrape::puppetdb::{build_puppetdb_sd_config, RawPuppetdbSdConfig};
use crate::scrape::vultr::{build_vultr_sd_config, RawVultrSdConfig};
use crate::scrape::yandexcloud::{build_yandexcloud_sd_config, RawYandexcloudSdConfig};

/// Raw `basic_auth:`/`bearer_token:` shape, reused by both a per-job
/// scrape config and [`HttpSdConfig`]. Kept distinct from
/// [`crate::client::AuthConfig`] (whose `basic` field is a
/// `(username, password)` tuple, not a natural YAML shape) — converted via
/// [`RawAuthFields::into_auth_config`].
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub(crate) struct RawAuthFields {
    pub(crate) basic_auth: Option<RawBasicAuth>,
    pub(crate) bearer_token: Option<String>,
}

impl RawAuthFields {
    pub(crate) fn into_auth_config(self) -> AuthConfig {
        AuthConfig {
            basic: self.basic_auth.map(|b| (b.username, b.password)),
            bearer: self.bearer_token.filter(|s| !s.is_empty()),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub(crate) struct RawBasicAuth {
    username: String,
    password: String,
}

/// Undefaulted top-level document shape for `serde_yaml_ng`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct RawFile {
    global: RawGlobalConfig,
    scrape_configs: Vec<RawScrapeConfig>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct RawGlobalConfig {
    scrape_interval: Option<String>,
    scrape_timeout: Option<String>,
    external_labels: BTreeMap<String, String>,
    sample_limit: usize,
    label_limit: usize,
}

/// Undefaulted `scrape_config` list-entry shape. `relabel_configs`/
/// `metric_relabel_configs` are captured as raw [`serde_yaml_ng::Value`]
/// (not directly deserializable into `esm_relabel::RelabelConfig` — see the
/// module doc) and re-parsed via `esm_relabel::parse_relabel_configs` in
/// [`build_scrape_config`]. Every yaml key this task supports is named
/// explicitly; anything else lands in `extra` via `#[serde(flatten)]`,
/// which [`build_scrape_config`] rejects — a cloud-SD key with the
/// dedicated "unsupported (deferred)" message, anything else as an unknown
/// field (matching upstream's strict-by-default parsing,
/// `-promscrape.config.strictParse`).
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct RawScrapeConfig {
    job_name: String,
    scrape_interval: Option<String>,
    scrape_timeout: Option<String>,
    metrics_path: Option<String>,
    scheme: Option<String>,
    honor_labels: bool,
    honor_timestamps: Option<bool>,
    params: BTreeMap<String, Vec<String>>,
    relabel_configs: Option<serde_yaml_ng::Value>,
    metric_relabel_configs: Option<serde_yaml_ng::Value>,
    sample_limit: usize,
    label_limit: usize,
    max_scrape_size: u64,
    enable_compression: Option<bool>,
    basic_auth: Option<RawBasicAuth>,
    bearer_token: Option<String>,
    tls_config: Option<TlsConfig>,
    static_configs: Vec<StaticConfig>,
    file_sd_configs: Vec<FileSdConfig>,
    http_sd_configs: Vec<RawHttpSdConfig>,
    kubernetes_sd_configs: Vec<RawKubernetesSdConfig>,
    consul_sd_configs: Vec<RawConsulSdConfig>,
    consulagent_sd_configs: Vec<RawConsulagentSdConfig>,
    ec2_sd_configs: Vec<RawEc2SdConfig>,
    gce_sd_configs: Vec<RawGceSdConfig>,
    azure_sd_configs: Vec<RawAzureSdConfig>,
    digitalocean_sd_configs: Vec<RawDigitaloceanSdConfig>,
    hetzner_sd_configs: Vec<RawHetznerSdConfig>,
    nomad_sd_configs: Vec<RawNomadSdConfig>,
    marathon_sd_configs: Vec<RawMarathonSdConfig>,
    vultr_sd_configs: Vec<RawVultrSdConfig>,
    puppetdb_sd_configs: Vec<RawPuppetdbSdConfig>,
    kuma_sd_configs: Vec<RawKumaSdConfig>,
    eureka_sd_configs: Vec<RawEurekaSdConfig>,
    yandexcloud_sd_configs: Vec<RawYandexcloudSdConfig>,
    ovhcloud_sd_configs: Vec<RawOvhcloudSdConfig>,
    openstack_sd_configs: Vec<RawOpenstackSdConfig>,
    dns_sd_configs: Vec<RawDnsSdConfig>,
    docker_sd_configs: Vec<RawDockerSdConfig>,
    dockerswarm_sd_configs: Vec<RawDockerswarmSdConfig>,
    #[serde(flatten)]
    extra: BTreeMap<String, serde_yaml_ng::Value>,
}

/// Undefaulted `http_sd_config` list-entry shape — see [`HttpSdConfig`]'s
/// doc for why this isn't a direct deserialize target.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct RawHttpSdConfig {
    url: String,
    refresh_interval: Option<String>,
    basic_auth: Option<RawBasicAuth>,
    bearer_token: Option<String>,
    tls_config: Option<TlsConfig>,
}

fn build_http_sd_config(raw: RawHttpSdConfig) -> Result<HttpSdConfig, ScrapeError> {
    let refresh_interval = match raw.refresh_interval {
        Some(s) => parse_duration(&s)?,
        None => DEFAULT_HTTP_SD_REFRESH_INTERVAL,
    };
    let auth_fields = RawAuthFields {
        basic_auth: raw.basic_auth,
        bearer_token: raw.bearer_token,
    };
    Ok(HttpSdConfig {
        url: raw.url,
        refresh_interval,
        auth: auth_fields.into_auth_config(),
        tls: raw.tls_config.unwrap_or_default(),
    })
}

/// Undefaulted `kubernetes_sd_config` list-entry shape — see
/// [`KubernetesSdConfig`]'s doc for why this isn't a direct deserialize
/// target (inline `basic_auth`/`bearer_token` need [`RawAuthFields`]
/// conversion, same as [`RawHttpSdConfig`]).
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct RawKubernetesSdConfig {
    role: String,
    api_server: Option<String>,
    kubeconfig_file: Option<String>,
    proxy_url: Option<String>,
    namespaces: K8sNamespaces,
    selectors: Vec<K8sSelector>,
    attach_metadata: Option<K8sAttachMetadata>,
    oauth2: Option<RawOAuth2Config>,
    basic_auth: Option<RawBasicAuth>,
    bearer_token: Option<String>,
    tls_config: Option<TlsConfig>,
}

/// Undefaulted `oauth2:` shape (natural YAML), converted to [`OAuth2Config`]
/// via [`build_oauth2_config`]. `Debug` is hand-written to redact
/// `client_secret` (this struct holds it and [`RawKubernetesSdConfig`] derives
/// `Debug`).
#[derive(Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
struct RawOAuth2Config {
    client_id: String,
    client_secret: Option<String>,
    client_secret_file: Option<String>,
    scopes: Vec<String>,
    token_url: String,
    endpoint_params: BTreeMap<String, String>,
    tls_config: Option<TlsConfig>,
    proxy_url: Option<String>,
}

impl fmt::Debug for RawOAuth2Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawOAuth2Config")
            .field("client_id", &self.client_id)
            .field(
                "client_secret",
                &self.client_secret.as_ref().map(|_| "<redacted>"),
            )
            .field("client_secret_file", &self.client_secret_file)
            .field("scopes", &self.scopes)
            .field("token_url", &self.token_url)
            .field("endpoint_params", &self.endpoint_params)
            .field("tls_config", &self.tls_config)
            .field("proxy_url", &self.proxy_url)
            .finish()
    }
}

fn build_oauth2_config(raw: RawOAuth2Config) -> OAuth2Config {
    OAuth2Config {
        client_id: raw.client_id,
        // Treat empty strings as unset so `oauth2::validate`'s "exactly one of
        // client_secret/client_secret_file" mirrors upstream (which checks for
        // an empty string / nil), matching how `bearer_token` empties are
        // filtered in `RawAuthFields::into_auth_config`.
        client_secret: raw.client_secret.filter(|s| !s.is_empty()),
        client_secret_file: raw.client_secret_file.filter(|s| !s.is_empty()),
        scopes: raw.scopes,
        token_url: raw.token_url,
        endpoint_params: raw.endpoint_params,
        tls: raw.tls_config.unwrap_or_default(),
        proxy_url: raw.proxy_url.filter(|s| !s.is_empty()),
    }
}

fn build_kubernetes_sd_config(raw: RawKubernetesSdConfig) -> KubernetesSdConfig {
    let auth_fields = RawAuthFields {
        basic_auth: raw.basic_auth,
        bearer_token: raw.bearer_token,
    };
    // Upstream `SDConfig.role()` normalizes the `endpointslices` alias (kept
    // for VictoriaMetrics-operator compat) to the canonical `endpointslice`.
    let role = if raw.role == "endpointslices" {
        "endpointslice".to_string()
    } else {
        raw.role
    };
    KubernetesSdConfig {
        role,
        api_server: raw.api_server,
        kubeconfig_file: raw.kubeconfig_file,
        proxy_url: raw.proxy_url,
        namespaces: raw.namespaces,
        selectors: raw.selectors,
        attach_metadata: raw.attach_metadata,
        oauth2: raw.oauth2.map(build_oauth2_config),
        auth: auth_fields.into_auth_config(),
        tls: raw.tls_config.unwrap_or_default(),
    }
}

/// Parses a `scrape_configs` YAML document (`-promscrape.config`'s
/// contents) into a [`ScrapeConfigFile`]. Never panics on malformed input —
/// every failure (bad YAML, an unsupported cloud-SD key, an unknown field,
/// a bad duration, a relabel config that fails to compile) is returned as a
/// [`ScrapeError`].
pub fn parse_scrape_config(yaml: &str) -> Result<ScrapeConfigFile, ScrapeError> {
    let raw: RawFile = serde_yaml_ng::from_str(yaml)
        .map_err(|e| ScrapeError::new(format!("cannot parse scrape config: {e}")))?;

    let global = build_global_config(raw.global)?;
    let scrape_configs = raw
        .scrape_configs
        .into_iter()
        .map(build_scrape_config)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(ScrapeConfigFile {
        global,
        scrape_configs,
    })
}

fn build_global_config(raw: RawGlobalConfig) -> Result<GlobalConfig, ScrapeError> {
    let scrape_interval = match raw.scrape_interval {
        Some(s) => parse_duration(&s)?,
        None => DEFAULT_SCRAPE_INTERVAL,
    };
    let scrape_timeout = match raw.scrape_timeout {
        Some(s) => parse_duration(&s)?,
        None => DEFAULT_SCRAPE_TIMEOUT,
    };
    Ok(GlobalConfig {
        scrape_interval,
        scrape_timeout,
        external_labels: raw.external_labels,
        sample_limit: raw.sample_limit,
        label_limit: raw.label_limit,
    })
}

fn build_scrape_config(raw: RawScrapeConfig) -> Result<ScrapeConfig, ScrapeError> {
    // Port of `config.go`'s `getScrapeWorkConfig`: an empty/absent `job_name`
    // is rejected rather than silently producing an empty `job` label.
    if raw.job_name.is_empty() {
        return Err(ScrapeError::new(
            "missing `job_name` field in `scrape_config`".to_string(),
        ));
    }
    reject_unsupported_keys(&raw.job_name, &raw.extra)?;

    let scrape_interval = raw
        .scrape_interval
        .as_deref()
        .map(parse_duration)
        .transpose()?;
    let scrape_timeout = raw
        .scrape_timeout
        .as_deref()
        .map(parse_duration)
        .transpose()?;
    let relabel_configs =
        parse_relabel_value(&raw.job_name, "relabel_configs", raw.relabel_configs)?;
    let metric_relabel_configs = parse_relabel_value(
        &raw.job_name,
        "metric_relabel_configs",
        raw.metric_relabel_configs,
    )?;

    let auth_fields = RawAuthFields {
        basic_auth: raw.basic_auth,
        bearer_token: raw.bearer_token,
    };
    let http_sd_configs = raw
        .http_sd_configs
        .into_iter()
        .map(build_http_sd_config)
        .collect::<Result<Vec<_>, _>>()?;
    let kubernetes_sd_configs = raw
        .kubernetes_sd_configs
        .into_iter()
        .map(build_kubernetes_sd_config)
        .collect();
    let consul_sd_configs = raw
        .consul_sd_configs
        .into_iter()
        .map(build_consul_sd_config)
        .collect();
    let consulagent_sd_configs = raw
        .consulagent_sd_configs
        .into_iter()
        .map(build_consulagent_sd_config)
        .collect();
    let ec2_sd_configs = raw
        .ec2_sd_configs
        .into_iter()
        .map(build_ec2_sd_config)
        .collect::<Result<Vec<_>, _>>()?;
    let gce_sd_configs = raw
        .gce_sd_configs
        .into_iter()
        .map(build_gce_sd_config)
        .collect::<Result<Vec<_>, _>>()?;
    let azure_sd_configs = raw
        .azure_sd_configs
        .into_iter()
        .map(build_azure_sd_config)
        .collect::<Result<Vec<_>, _>>()?;
    let digitalocean_sd_configs = raw
        .digitalocean_sd_configs
        .into_iter()
        .map(build_digitalocean_sd_config)
        .collect();
    let hetzner_sd_configs = raw
        .hetzner_sd_configs
        .into_iter()
        .map(build_hetzner_sd_config)
        .collect::<Result<Vec<_>, _>>()?;
    let nomad_sd_configs = raw
        .nomad_sd_configs
        .into_iter()
        .map(build_nomad_sd_config)
        .collect();
    let marathon_sd_configs = raw
        .marathon_sd_configs
        .into_iter()
        .map(build_marathon_sd_config)
        .collect();
    let vultr_sd_configs = raw
        .vultr_sd_configs
        .into_iter()
        .map(build_vultr_sd_config)
        .collect();
    let puppetdb_sd_configs = raw
        .puppetdb_sd_configs
        .into_iter()
        .map(build_puppetdb_sd_config)
        .collect::<Result<Vec<_>, _>>()?;
    let kuma_sd_configs = raw
        .kuma_sd_configs
        .into_iter()
        .map(build_kuma_sd_config)
        .collect::<Result<Vec<_>, _>>()?;
    let eureka_sd_configs = raw
        .eureka_sd_configs
        .into_iter()
        .map(build_eureka_sd_config)
        .collect();
    let yandexcloud_sd_configs = raw
        .yandexcloud_sd_configs
        .into_iter()
        .map(build_yandexcloud_sd_config)
        .collect::<Result<Vec<_>, _>>()?;
    let ovhcloud_sd_configs = raw
        .ovhcloud_sd_configs
        .into_iter()
        .map(build_ovhcloud_sd_config)
        .collect::<Result<Vec<_>, _>>()?;
    let openstack_sd_configs = raw
        .openstack_sd_configs
        .into_iter()
        .map(build_openstack_sd_config)
        .collect::<Result<Vec<_>, _>>()?;
    let dns_sd_configs = raw
        .dns_sd_configs
        .into_iter()
        .map(build_dns_sd_config)
        .collect::<Result<Vec<_>, _>>()?;
    let docker_sd_configs = raw
        .docker_sd_configs
        .into_iter()
        .map(build_docker_sd_config)
        .collect();
    let dockerswarm_sd_configs = raw
        .dockerswarm_sd_configs
        .into_iter()
        .map(build_dockerswarm_sd_config)
        .collect();

    Ok(ScrapeConfig {
        job_name: raw.job_name,
        scrape_interval,
        scrape_timeout,
        metrics_path: raw
            .metrics_path
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "/metrics".to_string()),
        // Port of `config.go`'s `scheme := strings.ToLower(sc.Scheme)`:
        // lowercase before defaulting/validating so `HTTPS` is accepted.
        scheme: raw
            .scheme
            .map(|s| s.to_lowercase())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "http".to_string()),
        honor_labels: raw.honor_labels,
        honor_timestamps: raw.honor_timestamps.unwrap_or(true),
        params: raw.params,
        relabel_configs,
        metric_relabel_configs,
        sample_limit: raw.sample_limit,
        label_limit: raw.label_limit,
        max_scrape_size: raw.max_scrape_size,
        enable_compression: raw.enable_compression.unwrap_or(true),
        auth: auth_fields.into_auth_config(),
        tls: raw.tls_config.unwrap_or_default(),
        static_configs: raw.static_configs,
        file_sd_configs: raw.file_sd_configs,
        http_sd_configs,
        kubernetes_sd_configs,
        consul_sd_configs,
        consulagent_sd_configs,
        ec2_sd_configs,
        gce_sd_configs,
        azure_sd_configs,
        digitalocean_sd_configs,
        hetzner_sd_configs,
        nomad_sd_configs,
        marathon_sd_configs,
        vultr_sd_configs,
        puppetdb_sd_configs,
        kuma_sd_configs,
        eureka_sd_configs,
        yandexcloud_sd_configs,
        ovhcloud_sd_configs,
        openstack_sd_configs,
        dns_sd_configs,
        docker_sd_configs,
        dockerswarm_sd_configs,
    })
}

/// Rejects any key this task's `RawScrapeConfig` doesn't model explicitly:
/// a known cloud-SD key gets the "unsupported (deferred)" message from the
/// module doc; anything else is reported as an unknown field.
fn reject_unsupported_keys(
    job_name: &str,
    extra: &BTreeMap<String, serde_yaml_ng::Value>,
) -> Result<(), ScrapeError> {
    let Some(key) = extra.keys().next() else {
        return Ok(());
    };
    if CLOUD_SD_KEYS.contains(&key.as_str()) {
        return Err(ScrapeError::new(format!(
            "job_name {job_name:?}: unsupported (deferred): {key}"
        )));
    }
    Err(ScrapeError::new(format!(
        "job_name {job_name:?}: unknown field in scrape_config: {key}"
    )))
}

/// Re-serializes a captured `relabel_configs`/`metric_relabel_configs` YAML
/// node back to a string and hands it to `esm_relabel::parse_relabel_configs`
/// — see the module doc for why this can't be a direct
/// `Vec<esm_relabel::RelabelConfig>` deserialize.
fn parse_relabel_value(
    job_name: &str,
    field: &str,
    value: Option<serde_yaml_ng::Value>,
) -> Result<Vec<esm_relabel::RelabelConfig>, ScrapeError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let yaml = serde_yaml_ng::to_string(&value).map_err(|e| {
        ScrapeError::new(format!(
            "job_name {job_name:?}: cannot re-serialize `{field}`: {e}"
        ))
    })?;
    esm_relabel::parse_relabel_configs(&yaml)
        .map_err(|e| ScrapeError::new(format!("job_name {job_name:?}: invalid `{field}`: {e}")))
}
