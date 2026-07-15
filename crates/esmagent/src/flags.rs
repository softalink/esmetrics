//! Go-`flag`-style command-line parsing for the esmagent binary.
//!
//! Mirrors `esmalert::flags` (`crates/esmalert/src/flags.rs`): syntax
//! follows Go's `flag` package (`-name=value`, `-name value`, boolean flags
//! without a value); an unknown flag mirrors Go's `flag provided but not
//! defined: -name` message with the usage text appended.
//!
//! `-remoteWrite.url` is repeatable (one destination per occurrence); the
//! per-destination auth/TLS/relabel flags (`-remoteWrite.basicAuth.*`,
//! `-remoteWrite.bearerToken*`, `-remoteWrite.tls*`,
//! `-remoteWrite.urlRelabelConfig`) are themselves repeatable and matched to
//! `-remoteWrite.url` *by position*: the Nth occurrence of e.g.
//! `-remoteWrite.basicAuth.username` configures the Nth `-remoteWrite.url`.
//! A destination with no corresponding occurrence gets that field's zero
//! value (no auth/no per-URL relabel) — this is upstream vmagent's own
//! `flagutil` array-flag convention (`app/vmagent/remotewrite/remotewrite.go`),
//! not something invented here. `main.rs`'s wiring resolves these
//! positional slices into a `client::AuthConfig`/`client::TlsConfig` pair
//! per destination at startup, reading any `*File` secret from disk there
//! (never here — this module never touches the filesystem, and never
//! formats a secret value into an error message).

use std::time::Duration;

/// Printed by `-version`.
pub const VERSION_STRING: &str = concat!(
    "EsMetrics esmagent v",
    env!("CARGO_PKG_VERSION"),
    " (Softalink LLC)"
);

/// Default `-remoteWrite.tmpDataPath`, mirroring upstream vmagent's
/// `vmagent-remotewrite-data` default (renamed for this port).
const DEFAULT_TMP_DATA_PATH: &str = "esmagent-remotewrite-data";

/// Raw per-destination auth/TLS flag values for every `-remoteWrite.url`
/// destination, indexed by occurrence order (see the module doc). Each
/// `Vec` may be shorter than `remote_write_urls` — a missing index means
/// "unset" for that destination, read via [`at`]/[`bool_at`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RemoteWriteAuthFlags {
    pub username: Vec<String>,
    pub password: Vec<String>,
    pub password_file: Vec<String>,
    pub bearer_token: Vec<String>,
    pub bearer_token_file: Vec<String>,
    pub tls_ca_file: Vec<String>,
    pub tls_cert_file: Vec<String>,
    pub tls_key_file: Vec<String>,
    pub tls_server_name: Vec<String>,
    pub tls_insecure_skip_verify: Vec<bool>,
}

/// Reads the `i`th element of a positional flag `Vec`, or `""` if `i` is
/// out of range (that destination never got this flag).
pub fn at(v: &[String], i: usize) -> &str {
    v.get(i).map(String::as_str).unwrap_or("")
}

/// Reads the `i`th element of a positional boolean flag `Vec`, or `false`
/// if `i` is out of range.
pub fn bool_at(v: &[bool], i: usize) -> bool {
    v.get(i).copied().unwrap_or(false)
}

/// Parsed command-line flags with upstream-vmagent-compatible defaults.
#[derive(Debug, Clone, PartialEq)]
pub struct Flags {
    /// `-remoteWrite.url`; repeatable. One forwarding destination per
    /// occurrence.
    pub remote_write_urls: Vec<String>,
    /// `-remoteWrite.tmpDataPath`; directory holding each destination's
    /// durable queue (one subdirectory per destination).
    pub remote_write_tmp_data_path: String,
    /// `-remoteWrite.maxDiskUsagePerURL`; bytes. `0` means unlimited.
    pub remote_write_max_disk_usage_per_url: u64,
    /// `-remoteWrite.queues`; worker threads per destination.
    pub remote_write_queues: usize,
    pub remote_write_max_block_size: usize,
    pub remote_write_flush_interval: Duration,
    pub remote_write_retry_min_interval: Duration,
    pub remote_write_retry_max_interval: Duration,
    /// `-remoteWrite.relabelConfig`; path to a global relabel YAML file,
    /// applied to every destination before fan-out. Empty means disabled.
    pub remote_write_relabel_config: String,
    /// `-remoteWrite.urlRelabelConfig`; repeatable, positional (see the
    /// module doc). Path to a per-destination relabel YAML file; an empty
    /// string at index `i` means destination `i` has no per-URL relabel.
    pub remote_write_url_relabel_configs: Vec<String>,
    /// `-remoteWrite.streamAggr.config`; repeatable, positional. Per-URL
    /// stream-aggregation config path; empty at index `i` = no aggregation
    /// for destination `i`.
    pub remote_write_stream_aggr_config: Vec<String>,
    /// `-remoteWrite.streamAggr.keepInput`; repeatable, positional per-URL.
    pub remote_write_stream_aggr_keep_input: Vec<bool>,
    /// `-remoteWrite.streamAggr.dedupInterval`; repeatable, positional per-URL.
    pub remote_write_stream_aggr_dedup_interval: Vec<Duration>,
    pub remote_write_auth: RemoteWriteAuthFlags,
    pub http_listen_addr: String,
    pub metrics_auth_key: String,
    pub http_read_timeout: Duration,
    pub dry_run: bool,
    /// `-promscrape.config`; path to a `scrape_configs` YAML file. Unset
    /// means the scrape engine is disabled entirely (forwarding-only mode,
    /// this crate's original behavior).
    pub promscrape_config: Option<String>,
    /// `-promscrape.configCheckInterval`; how often `main`'s event loop
    /// re-reads `-promscrape.config` and reloads it. `0` (the default)
    /// disables interval polling — the config is still reloadable via
    /// SIGHUP, matching upstream vmagent's "SIGHUP or interval, your
    /// choice" convention.
    pub promscrape_config_check_interval: Duration,
    /// `-promscrape.suppressScrapeErrors`; parsed but not yet wired into the
    /// scrape worker's error handling (see `scrape::wiring`'s module doc for
    /// why this is deferred).
    pub promscrape_suppress_scrape_errors: bool,
    /// `-promscrape.maxScrapeSize`; default byte cap on a target's raw scrape
    /// response, applied to any `scrape_config` job whose own
    /// `max_scrape_size` is unset (`0`). Default `16 * 1024 * 1024` (16MiB),
    /// matching this crate's other `N * 1024 * 1024`-style byte defaults
    /// (e.g. `remote_write_max_block_size`) rather than replicating
    /// upstream's decimal-vs-binary suffix-string ambiguity (see
    /// `scrape::config`'s module doc on why `max_scrape_size` is a plain
    /// `u64` here, not a suffixed string).
    pub promscrape_max_scrape_size: u64,
    /// `-promscrape.config.dryRun`; validates `-promscrape.config` alone
    /// (independent of `-remoteWrite.url`) and exits.
    pub promscrape_config_dry_run: bool,
    /// `-promscrape.kubernetes.attachNodeMetadataAll`; the default
    /// `attach_metadata.node` for every `kubernetes_sd_config`. A per-config
    /// `attach_metadata` fully overrides it (see `scrape::wiring`'s
    /// `apply_kubernetes_attach_metadata_defaults`).
    pub promscrape_kubernetes_attach_node_metadata_all: bool,
    /// `-promscrape.kubernetes.attachNamespaceMetadataAll`; the default
    /// `attach_metadata.namespace` for every `kubernetes_sd_config`. A
    /// per-config `attach_metadata` fully overrides it.
    pub promscrape_kubernetes_attach_namespace_metadata_all: bool,
    /// `-promscrape.consulSDCheckInterval`; the refresh interval for every
    /// `consul_sd_config` (upstream reads it from this same flag rather than
    /// a per-config YAML field). Default 30s; applied via
    /// `scrape::wiring::apply_consul_sd_check_interval`.
    pub promscrape_consul_sd_check_interval: Duration,
    /// `-promscrape.consulagentSDCheckInterval`; the refresh interval for every
    /// `consulagent_sd_config` (upstream reads it from this same flag rather
    /// than a per-config YAML field). Default 30s; applied via
    /// `scrape::wiring::apply_consulagent_sd_check_interval`.
    pub promscrape_consulagent_sd_check_interval: Duration,
    /// `-promscrape.ec2SDCheckInterval`; the refresh interval for every
    /// `ec2_sd_config` (upstream reads it from this same flag rather than a
    /// per-config YAML field). Default 60s; applied via
    /// `scrape::wiring::apply_ec2_sd_check_interval`.
    pub promscrape_ec2_sd_check_interval: Duration,
    /// `-promscrape.gceSDCheckInterval`; the refresh interval for every
    /// `gce_sd_config` (upstream reads it from this same flag rather than a
    /// per-config YAML field). Default 60s; applied via
    /// `scrape::wiring::apply_gce_sd_check_interval`.
    pub promscrape_gce_sd_check_interval: Duration,
    /// `-promscrape.azureSDCheckInterval`; the refresh interval for every
    /// `azure_sd_config` (upstream reads it from this same flag rather than a
    /// per-config YAML field). Default 60s; applied via
    /// `scrape::wiring::apply_azure_sd_check_interval`.
    pub promscrape_azure_sd_check_interval: Duration,
    /// `-promscrape.digitaloceanSDCheckInterval`; the refresh interval for
    /// every `digitalocean_sd_config` (upstream reads it from this same flag
    /// rather than a per-config YAML field). Default 60s; applied via
    /// `scrape::wiring::apply_digitalocean_sd_check_interval`.
    pub promscrape_digitalocean_sd_check_interval: Duration,
    /// `-promscrape.hetznerSDCheckInterval`; the refresh interval for every
    /// `hetzner_sd_config` (upstream reads it from this same flag rather than a
    /// per-config YAML field). Default 60s; applied via
    /// `scrape::wiring::apply_hetzner_sd_check_interval`.
    pub promscrape_hetzner_sd_check_interval: Duration,
    /// `-promscrape.nomadSDCheckInterval`; the refresh interval for every
    /// `nomad_sd_config` (upstream reads it from this same flag rather than a
    /// per-config YAML field). Default 30s; applied via
    /// `scrape::wiring::apply_nomad_sd_check_interval`.
    pub promscrape_nomad_sd_check_interval: Duration,
    /// `-promscrape.marathonSDCheckInterval`; the refresh interval for every
    /// `marathon_sd_config` (upstream reads it from this same flag rather than a
    /// per-config YAML field). Default 30s; applied via
    /// `scrape::wiring::apply_marathon_sd_check_interval`.
    pub promscrape_marathon_sd_check_interval: Duration,
    /// `-promscrape.vultrSDCheckInterval`; the refresh interval for every
    /// `vultr_sd_config` (upstream reads it from this same flag rather than a
    /// per-config YAML field). Default 30s; applied via
    /// `scrape::wiring::apply_vultr_sd_check_interval`.
    pub promscrape_vultr_sd_check_interval: Duration,
    /// `-promscrape.puppetdbSDCheckInterval`; the refresh interval for every
    /// `puppetdb_sd_config` (upstream reads it from this same flag rather than a
    /// per-config YAML field). Default 30s; applied via
    /// `scrape::wiring::apply_puppetdb_sd_check_interval`.
    pub promscrape_puppetdb_sd_check_interval: Duration,
    /// `-promscrape.kumaSDCheckInterval`; the refresh interval for every
    /// `kuma_sd_config` (upstream reads it from this same flag rather than a
    /// per-config YAML field). Default 30s; applied via
    /// `scrape::wiring::apply_kuma_sd_check_interval`.
    pub promscrape_kuma_sd_check_interval: Duration,
    /// `-promscrape.eurekaSDCheckInterval`; the refresh interval for every
    /// `eureka_sd_config` (upstream reads it from this same flag rather than a
    /// per-config YAML field). Default 30s; applied via
    /// `scrape::wiring::apply_eureka_sd_check_interval`.
    pub promscrape_eureka_sd_check_interval: Duration,
    /// `-promscrape.yandexcloudSDCheckInterval`; the refresh interval for every
    /// `yandexcloud_sd_config` (upstream reads it from this same flag rather than
    /// a per-config YAML field). Default 30s; applied via
    /// `scrape::wiring::apply_yandexcloud_sd_check_interval`.
    pub promscrape_yandexcloud_sd_check_interval: Duration,
    /// `-promscrape.openstackSDCheckInterval`; the refresh interval for every
    /// `openstack_sd_config` (upstream reads it from this same flag rather than
    /// a per-config YAML field). Applied to every parsed config by
    /// `scrape::wiring::apply_openstack_sd_check_interval`.
    pub promscrape_openstack_sd_check_interval: Duration,
    /// `-promscrape.dnsSDCheckInterval`; the refresh interval for every
    /// `dns_sd_config` (upstream reads it from this same flag rather than a
    /// per-config YAML field). Default 30s; applied via
    /// `scrape::wiring::apply_dns_sd_check_interval`.
    pub promscrape_dns_sd_check_interval: Duration,
    /// `-promscrape.ovhcloudSDCheckInterval`; the refresh interval for every
    /// `ovhcloud_sd_config` (upstream reads it from this same flag rather than a
    /// per-config YAML field). Default 30s; applied via
    /// `scrape::wiring::apply_ovhcloud_sd_check_interval`.
    pub promscrape_ovhcloud_sd_check_interval: Duration,
    /// `-promscrape.dockerSDCheckInterval`; the refresh interval for every
    /// `docker_sd_config` (upstream reads it from this same flag rather than a
    /// per-config YAML field). Default 30s; applied via
    /// `scrape::wiring::apply_docker_sd_check_interval`.
    pub promscrape_docker_sd_check_interval: Duration,
    /// `-promscrape.dockerswarmSDCheckInterval`; the refresh interval for every
    /// `dockerswarm_sd_config` (upstream reads it from this same flag rather
    /// than a per-config YAML field). Default 30s; applied via
    /// `scrape::wiring::apply_dockerswarm_sd_check_interval`.
    pub promscrape_dockerswarm_sd_check_interval: Duration,
    /// `-streamAggr.config`; path to a global stream-aggregation config YAML
    /// file, applied after global relabel and before fan-out. `None` (the
    /// default) disables stream aggregation entirely (zero cost). See
    /// `crate::streamagg`.
    pub stream_aggr_config: Option<String>,
    /// `-streamAggr.keepInput`; when true, all input series are forwarded in
    /// addition to the aggregated output. By default, series consumed by an
    /// aggregator are dropped from the direct forward path.
    pub stream_aggr_keep_input: bool,
    /// `-streamAggr.dedupInterval`; global de-duplication interval applied
    /// before aggregation. `0` (the default) disables it.
    pub stream_aggr_dedup_interval: Duration,
    /// `-streamAggr.dropInputLabels`; comma-separated labels dropped from every
    /// sample before de-duplication and aggregation.
    pub stream_aggr_drop_input_labels: Vec<String>,
    /// `-streamAggr.ignoreOldSamples`; ignore samples older than the current
    /// aggregation interval.
    pub stream_aggr_ignore_old_samples: bool,
    /// `-streamAggr.ignoreFirstIntervals`; number of initial aggregation
    /// intervals to skip (dropping their output).
    pub stream_aggr_ignore_first_intervals: usize,
    /// `-streamAggr.flushOnShutdown`; flush incomplete aggregation state on
    /// shutdown.
    pub stream_aggr_flush_on_shutdown: bool,
    /// `-streamAggr.enableWindows`; enable the blue/green aggregation-window
    /// mode for the global `-streamAggr.config`.
    pub stream_aggr_enable_windows: bool,
}

/// Default `-promscrape.maxScrapeSize`: 16MiB. See the field doc on
/// [`Flags::promscrape_max_scrape_size`].
const DEFAULT_PROMSCRAPE_MAX_SCRAPE_SIZE: u64 = 16 * 1024 * 1024;

impl Default for Flags {
    fn default() -> Flags {
        Flags {
            remote_write_urls: Vec::new(),
            remote_write_tmp_data_path: DEFAULT_TMP_DATA_PATH.to_string(),
            remote_write_max_disk_usage_per_url: 0,
            remote_write_queues: 1,
            remote_write_max_block_size: 8 * 1024 * 1024,
            remote_write_flush_interval: Duration::from_secs(1),
            remote_write_retry_min_interval: Duration::from_secs(1),
            remote_write_retry_max_interval: Duration::from_secs(30),
            remote_write_relabel_config: String::new(),
            remote_write_url_relabel_configs: Vec::new(),
            remote_write_stream_aggr_config: Vec::new(),
            remote_write_stream_aggr_keep_input: Vec::new(),
            remote_write_stream_aggr_dedup_interval: Vec::new(),
            remote_write_auth: RemoteWriteAuthFlags::default(),
            http_listen_addr: ":8429".to_string(),
            metrics_auth_key: String::new(),
            http_read_timeout: Duration::from_secs(30),
            dry_run: false,
            promscrape_config: None,
            promscrape_config_check_interval: Duration::ZERO,
            promscrape_suppress_scrape_errors: false,
            promscrape_max_scrape_size: DEFAULT_PROMSCRAPE_MAX_SCRAPE_SIZE,
            promscrape_config_dry_run: false,
            promscrape_kubernetes_attach_node_metadata_all: false,
            promscrape_kubernetes_attach_namespace_metadata_all: false,
            promscrape_consul_sd_check_interval: DEFAULT_CONSUL_SD_CHECK_INTERVAL,
            promscrape_consulagent_sd_check_interval: DEFAULT_CONSULAGENT_SD_CHECK_INTERVAL,
            promscrape_ec2_sd_check_interval: DEFAULT_EC2_SD_CHECK_INTERVAL,
            promscrape_gce_sd_check_interval: DEFAULT_GCE_SD_CHECK_INTERVAL,
            promscrape_azure_sd_check_interval: DEFAULT_AZURE_SD_CHECK_INTERVAL,
            promscrape_digitalocean_sd_check_interval: DEFAULT_DIGITALOCEAN_SD_CHECK_INTERVAL,
            promscrape_hetzner_sd_check_interval: DEFAULT_HETZNER_SD_CHECK_INTERVAL,
            promscrape_nomad_sd_check_interval: DEFAULT_NOMAD_SD_CHECK_INTERVAL,
            promscrape_marathon_sd_check_interval: DEFAULT_MARATHON_SD_CHECK_INTERVAL,
            promscrape_vultr_sd_check_interval: DEFAULT_VULTR_SD_CHECK_INTERVAL,
            promscrape_puppetdb_sd_check_interval: DEFAULT_PUPPETDB_SD_CHECK_INTERVAL,
            promscrape_kuma_sd_check_interval: DEFAULT_KUMA_SD_CHECK_INTERVAL,
            promscrape_eureka_sd_check_interval: DEFAULT_EUREKA_SD_CHECK_INTERVAL,
            promscrape_yandexcloud_sd_check_interval: DEFAULT_YANDEXCLOUD_SD_CHECK_INTERVAL,
            promscrape_ovhcloud_sd_check_interval: DEFAULT_OVHCLOUD_SD_CHECK_INTERVAL,
            promscrape_openstack_sd_check_interval: DEFAULT_OPENSTACK_SD_CHECK_INTERVAL,
            promscrape_dns_sd_check_interval: DEFAULT_DNS_SD_CHECK_INTERVAL,
            promscrape_docker_sd_check_interval: DEFAULT_DOCKER_SD_CHECK_INTERVAL,
            promscrape_dockerswarm_sd_check_interval: DEFAULT_DOCKERSWARM_SD_CHECK_INTERVAL,
            stream_aggr_config: None,
            stream_aggr_keep_input: false,
            stream_aggr_dedup_interval: Duration::ZERO,
            stream_aggr_drop_input_labels: Vec::new(),
            stream_aggr_ignore_old_samples: false,
            stream_aggr_enable_windows: false,
            stream_aggr_ignore_first_intervals: 0,
            stream_aggr_flush_on_shutdown: false,
        }
    }
}

/// Default `-promscrape.consulSDCheckInterval`: 30s (matches upstream's
/// flag default). See [`Flags::promscrape_consul_sd_check_interval`].
const DEFAULT_CONSUL_SD_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Default `-promscrape.consulagentSDCheckInterval`: 30s (matches upstream
/// `consulagent.SDCheckInterval`'s `30*time.Second`). See
/// [`Flags::promscrape_consulagent_sd_check_interval`].
const DEFAULT_CONSULAGENT_SD_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Default `-promscrape.ec2SDCheckInterval`: 60s (matches upstream
/// `ec2.SDCheckInterval`'s `time.Minute`). See
/// [`Flags::promscrape_ec2_sd_check_interval`].
const DEFAULT_EC2_SD_CHECK_INTERVAL: Duration = Duration::from_secs(60);

/// Default `-promscrape.gceSDCheckInterval`: 60s (matches upstream
/// `gce.SDCheckInterval`'s `time.Minute`). See
/// [`Flags::promscrape_gce_sd_check_interval`].
const DEFAULT_GCE_SD_CHECK_INTERVAL: Duration = Duration::from_secs(60);

/// Default `-promscrape.azureSDCheckInterval`: 60s (matches upstream
/// `azure.SDCheckInterval`'s `time.Minute`). See
/// [`Flags::promscrape_azure_sd_check_interval`].
const DEFAULT_AZURE_SD_CHECK_INTERVAL: Duration = Duration::from_secs(60);

/// Default `-promscrape.digitaloceanSDCheckInterval`: 60s (matches upstream
/// `digitalocean.SDCheckInterval`'s `time.Minute`). See
/// [`Flags::promscrape_digitalocean_sd_check_interval`].
const DEFAULT_DIGITALOCEAN_SD_CHECK_INTERVAL: Duration = Duration::from_secs(60);

/// Default `-promscrape.hetznerSDCheckInterval`: 60s (matches upstream
/// `hetzner.SDCheckInterval`'s `time.Minute`). See
/// [`Flags::promscrape_hetzner_sd_check_interval`].
const DEFAULT_HETZNER_SD_CHECK_INTERVAL: Duration = Duration::from_secs(60);

/// Default `-promscrape.nomadSDCheckInterval`: 30s (matches upstream
/// `nomad.SDCheckInterval`'s `30*time.Second`). See
/// [`Flags::promscrape_nomad_sd_check_interval`].
const DEFAULT_NOMAD_SD_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Default `-promscrape.marathonSDCheckInterval`: 30s (matches upstream
/// `marathon.SDCheckInterval`'s `30*time.Second`). See
/// [`Flags::promscrape_marathon_sd_check_interval`].
const DEFAULT_MARATHON_SD_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Default `-promscrape.vultrSDCheckInterval`: 30s (matches upstream
/// `vultr.SDCheckInterval`'s `30*time.Second`). See
/// [`Flags::promscrape_vultr_sd_check_interval`].
const DEFAULT_VULTR_SD_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Default `-promscrape.puppetdbSDCheckInterval`: 30s (matches upstream
/// `puppetdb.SDCheckInterval`'s `30*time.Second`). See
/// [`Flags::promscrape_puppetdb_sd_check_interval`].
const DEFAULT_PUPPETDB_SD_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Default `-promscrape.kumaSDCheckInterval`: 30s (matches upstream
/// `kuma.SDCheckInterval`'s `30*time.Second`). See
/// [`Flags::promscrape_kuma_sd_check_interval`].
const DEFAULT_KUMA_SD_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Default `-promscrape.eurekaSDCheckInterval`: 30s (matches upstream
/// `eureka.SDCheckInterval`'s `30*time.Second`). See
/// [`Flags::promscrape_eureka_sd_check_interval`].
const DEFAULT_EUREKA_SD_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Default `-promscrape.yandexcloudSDCheckInterval`: 30s (matches upstream
/// `yandexcloud.SDCheckInterval`'s `30*time.Second`). See
/// [`Flags::promscrape_yandexcloud_sd_check_interval`].
const DEFAULT_YANDEXCLOUD_SD_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Default `-promscrape.ovhcloudSDCheckInterval`: 30s (matches upstream
/// `ovhcloud.SDCheckInterval`'s `30*time.Second`). See
/// [`Flags::promscrape_ovhcloud_sd_check_interval`].
const DEFAULT_OVHCLOUD_SD_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Default `-promscrape.openstackSDCheckInterval`: 30s (matches upstream
/// `openstack.SDCheckInterval`'s `30*time.Second`). See
/// [`Flags::promscrape_openstack_sd_check_interval`].
const DEFAULT_OPENSTACK_SD_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Default `-promscrape.dnsSDCheckInterval`: 30s (matches upstream
/// `dns.SDCheckInterval`'s `30*time.Second`). See
/// [`Flags::promscrape_dns_sd_check_interval`].
const DEFAULT_DNS_SD_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Default `-promscrape.dockerSDCheckInterval`: 30s (matches upstream
/// `docker.SDCheckInterval`'s `30*time.Second`). See
/// [`Flags::promscrape_docker_sd_check_interval`].
const DEFAULT_DOCKER_SD_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Default `-promscrape.dockerswarmSDCheckInterval`: 30s (matches upstream
/// `dockerswarm.SDCheckInterval`'s `30*time.Second`). See
/// [`Flags::promscrape_dockerswarm_sd_check_interval`].
const DEFAULT_DOCKERSWARM_SD_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Error returned by [`parse_flags`]. `Help`/`Version` are control-flow
/// signals (mirroring esmalert's `FlagError`), not real parse failures; the
/// caller (`main`) matches them to print [`usage`]/[`VERSION_STRING`] and
/// exit 0.
#[derive(Debug, PartialEq)]
pub enum FlagError {
    Help,
    Version,
    Invalid(String),
}

impl std::fmt::Display for FlagError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlagError::Help => write!(f, "{}", usage()),
            FlagError::Version => write!(f, "{VERSION_STRING}"),
            FlagError::Invalid(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for FlagError {}

/// The `-help` text lives in a sibling module ([`flags_usage`]) as a plain
/// `const` so this file stays under the repo's 800-line cap.
#[path = "flags_usage.rs"]
mod flags_usage;

/// Returns the `-help` text. Kept intentionally compact (not a per-flag
/// generated table) given the positional per-destination auth/TLS surface;
/// see the field docs on [`Flags`] and the module doc for the authoritative
/// per-flag description. The text itself is [`flags_usage::USAGE`].
pub fn usage() -> String {
    flags_usage::USAGE.to_string()
}

/// The argument scanner ([`parse_flags`]) and its parsing helpers live in a
/// sibling module so this file stays under the repo's 800-line cap.
#[path = "flags_parse.rs"]
mod flags_parse;
pub use flags_parse::parse_flags;

#[cfg(test)]
#[path = "flags_tests.rs"]
mod tests;
