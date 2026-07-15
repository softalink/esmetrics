//! Flag-default post-processing for a parsed `-promscrape.config`: the
//! per-SD-kind `refresh_interval` overrides driven by each
//! `-promscrape.<x>SDCheckInterval` flag, the `-promscrape.maxScrapeSize`
//! per-job default, and the Kubernetes `attachNodeMetadataAll`/
//! `attachNamespaceMetadataAll` resolution.
//!
//! Split out of `wiring.rs` to keep that file under the repo's 800-line cap:
//! the ~20 `apply_*` helpers plus the single [`apply_flag_defaults`]
//! orchestration (shared verbatim by `wiring::build_scrape_manager` and
//! `wiring::reload_scrape_manager`, so the two entry points can never drift)
//! are a cohesive "apply flag defaults into the parsed config" unit. All the
//! `apply_*` helpers stay `pub` and are re-exported from `wiring` so existing
//! `scrape::wiring::apply_*` references remain valid.

use crate::flags::Flags;
use crate::scrape::config::{K8sAttachMetadata, ScrapeConfigFile};

/// Applies every `-promscrape.*` flag default into the freshly parsed `cfg`,
/// in the same order both entry points used inline before this was extracted.
/// Shared by `wiring::build_scrape_manager` and `wiring::reload_scrape_manager`
/// so the two can never diverge.
pub(crate) fn apply_flag_defaults(cfg: &mut ScrapeConfigFile, flags: &Flags) {
    apply_default_max_scrape_size(cfg, flags.promscrape_max_scrape_size);
    apply_kubernetes_attach_metadata_defaults(
        cfg,
        flags.promscrape_kubernetes_attach_node_metadata_all,
        flags.promscrape_kubernetes_attach_namespace_metadata_all,
    );
    apply_consul_sd_check_interval(cfg, flags.promscrape_consul_sd_check_interval);
    apply_consulagent_sd_check_interval(cfg, flags.promscrape_consulagent_sd_check_interval);
    apply_ec2_sd_check_interval(cfg, flags.promscrape_ec2_sd_check_interval);
    apply_gce_sd_check_interval(cfg, flags.promscrape_gce_sd_check_interval);
    apply_azure_sd_check_interval(cfg, flags.promscrape_azure_sd_check_interval);
    apply_digitalocean_sd_check_interval(cfg, flags.promscrape_digitalocean_sd_check_interval);
    apply_hetzner_sd_check_interval(cfg, flags.promscrape_hetzner_sd_check_interval);
    apply_nomad_sd_check_interval(cfg, flags.promscrape_nomad_sd_check_interval);
    apply_marathon_sd_check_interval(cfg, flags.promscrape_marathon_sd_check_interval);
    apply_vultr_sd_check_interval(cfg, flags.promscrape_vultr_sd_check_interval);
    apply_puppetdb_sd_check_interval(cfg, flags.promscrape_puppetdb_sd_check_interval);
    apply_kuma_sd_check_interval(cfg, flags.promscrape_kuma_sd_check_interval);
    apply_eureka_sd_check_interval(cfg, flags.promscrape_eureka_sd_check_interval);
    apply_yandexcloud_sd_check_interval(cfg, flags.promscrape_yandexcloud_sd_check_interval);
    apply_ovhcloud_sd_check_interval(cfg, flags.promscrape_ovhcloud_sd_check_interval);
    apply_openstack_sd_check_interval(cfg, flags.promscrape_openstack_sd_check_interval);
    apply_dns_sd_check_interval(cfg, flags.promscrape_dns_sd_check_interval);
    apply_docker_sd_check_interval(cfg, flags.promscrape_docker_sd_check_interval);
    apply_dockerswarm_sd_check_interval(cfg, flags.promscrape_dockerswarm_sd_check_interval);
}

/// Fills in `-promscrape.maxScrapeSize` as the per-job default for any
/// `scrape_config` whose own `max_scrape_size` is unset (`0`) — matching
/// upstream vmagent's "flag is the default, YAML overrides it" convention.
/// A job that explicitly sets `max_scrape_size: 0` in YAML is
/// indistinguishable from "unset" here (both parse to `0`), which is an
/// accepted, documented simplification: `0` already means "unlimited"
/// everywhere else in this crate's `max_scrape_size`/`sample_limit`/
/// `label_limit` convention, so once a nonzero global default is
/// configured a job can no longer ask for literal "unlimited" via `0` — it
/// can only opt out with a large explicit value.
pub fn apply_default_max_scrape_size(cfg: &mut ScrapeConfigFile, default_size: u64) {
    for sc in &mut cfg.scrape_configs {
        if sc.max_scrape_size == 0 {
            sc.max_scrape_size = default_size;
        }
    }
}

/// Resolves the global `-promscrape.kubernetes.attachNodeMetadataAll` /
/// `attachNamespaceMetadataAll` flags into each `kubernetes_sd_config`'s
/// `attach_metadata`, matching upstream vmagent's `newAPIWatcher`: the two
/// global flags supply the default `{node, namespace}`, and a per-config
/// `attach_metadata` (when present) fully overrides them. Only configs whose
/// own `attach_metadata` is `None` are filled, and only when at least one
/// global flag is set — a config that opted out (or in) explicitly is left
/// untouched. By resolving into `attach_metadata` here, the discovery layer's
/// existing per-config handling needs no change.
pub fn apply_kubernetes_attach_metadata_defaults(
    cfg: &mut ScrapeConfigFile,
    node_all: bool,
    namespace_all: bool,
) {
    if !node_all && !namespace_all {
        return;
    }
    for sc in &mut cfg.scrape_configs {
        for k in &mut sc.kubernetes_sd_configs {
            if k.attach_metadata.is_none() {
                k.attach_metadata = Some(K8sAttachMetadata {
                    node: node_all,
                    namespace: namespace_all,
                });
            }
        }
    }
}

/// Sets every `consul_sd_config`'s `refresh_interval` from
/// `-promscrape.consulSDCheckInterval`. Upstream has no per-config
/// `refresh_interval` YAML field for Consul — the flag is the sole source —
/// so unlike `apply_default_max_scrape_size` this overwrites unconditionally
/// (there is no "job set it in YAML" case to preserve). Mirrors
/// `apply_default_max_scrape_size`'s flag->config mutation shape.
pub fn apply_consul_sd_check_interval(cfg: &mut ScrapeConfigFile, interval: std::time::Duration) {
    for sc in &mut cfg.scrape_configs {
        for consul in &mut sc.consul_sd_configs {
            consul.refresh_interval = interval;
        }
    }
}

/// Sets every `consulagent_sd_config`'s `refresh_interval` from
/// `-promscrape.consulagentSDCheckInterval`. Same flag-is-sole-source shape as
/// [`apply_consul_sd_check_interval`]: upstream has no per-config
/// `refresh_interval` YAML field for Consul Agent, so this overwrites
/// unconditionally.
pub fn apply_consulagent_sd_check_interval(
    cfg: &mut ScrapeConfigFile,
    interval: std::time::Duration,
) {
    for sc in &mut cfg.scrape_configs {
        for consulagent in &mut sc.consulagent_sd_configs {
            consulagent.refresh_interval = interval;
        }
    }
}

/// Sets every `ec2_sd_config`'s `refresh_interval` from
/// `-promscrape.ec2SDCheckInterval`. Same flag-is-sole-source shape as
/// [`apply_consul_sd_check_interval`]: upstream has no per-config
/// `refresh_interval` YAML field for EC2, so this overwrites unconditionally.
pub fn apply_ec2_sd_check_interval(cfg: &mut ScrapeConfigFile, interval: std::time::Duration) {
    for sc in &mut cfg.scrape_configs {
        for ec2 in &mut sc.ec2_sd_configs {
            ec2.refresh_interval = interval;
        }
    }
}

/// Sets every `gce_sd_config`'s `refresh_interval` from
/// `-promscrape.gceSDCheckInterval`. Same flag-is-sole-source shape as
/// [`apply_consul_sd_check_interval`]: upstream has no per-config
/// `refresh_interval` YAML field for GCE, so this overwrites unconditionally.
pub fn apply_gce_sd_check_interval(cfg: &mut ScrapeConfigFile, interval: std::time::Duration) {
    for sc in &mut cfg.scrape_configs {
        for gce in &mut sc.gce_sd_configs {
            gce.refresh_interval = interval;
        }
    }
}

/// Sets every `azure_sd_config`'s `refresh_interval` from
/// `-promscrape.azureSDCheckInterval`. Same flag-is-sole-source shape as
/// [`apply_consul_sd_check_interval`]: upstream has no per-config
/// `refresh_interval` YAML field for Azure, so this overwrites unconditionally.
pub fn apply_azure_sd_check_interval(cfg: &mut ScrapeConfigFile, interval: std::time::Duration) {
    for sc in &mut cfg.scrape_configs {
        for azure in &mut sc.azure_sd_configs {
            azure.refresh_interval = interval;
        }
    }
}

/// Sets every `digitalocean_sd_config`'s `refresh_interval` from
/// `-promscrape.digitaloceanSDCheckInterval`. Same flag-is-sole-source shape
/// as [`apply_consul_sd_check_interval`]: upstream has no per-config
/// `refresh_interval` YAML field for DigitalOcean, so this overwrites
/// unconditionally.
pub fn apply_digitalocean_sd_check_interval(
    cfg: &mut ScrapeConfigFile,
    interval: std::time::Duration,
) {
    for sc in &mut cfg.scrape_configs {
        for droplet_sd in &mut sc.digitalocean_sd_configs {
            droplet_sd.refresh_interval = interval;
        }
    }
}

/// Sets every `hetzner_sd_config`'s `refresh_interval` from
/// `-promscrape.hetznerSDCheckInterval`. Same flag-is-sole-source shape as
/// [`apply_consul_sd_check_interval`]: upstream has no per-config
/// `refresh_interval` YAML field for Hetzner, so this overwrites
/// unconditionally.
pub fn apply_hetzner_sd_check_interval(cfg: &mut ScrapeConfigFile, interval: std::time::Duration) {
    for sc in &mut cfg.scrape_configs {
        for hetzner in &mut sc.hetzner_sd_configs {
            hetzner.refresh_interval = interval;
        }
    }
}

/// Sets every `ovhcloud_sd_config`'s `refresh_interval` from
/// `-promscrape.ovhcloudSDCheckInterval`. Same flag-is-sole-source shape as
/// [`apply_hetzner_sd_check_interval`]: upstream has no per-config
/// `refresh_interval` YAML field for OVHcloud, so this overwrites
/// unconditionally.
pub fn apply_ovhcloud_sd_check_interval(cfg: &mut ScrapeConfigFile, interval: std::time::Duration) {
    for sc in &mut cfg.scrape_configs {
        for ovhcloud in &mut sc.ovhcloud_sd_configs {
            ovhcloud.refresh_interval = interval;
        }
    }
}

/// Sets every `openstack_sd_config`'s `refresh_interval` from
/// `-promscrape.openstackSDCheckInterval`. Same flag-is-sole-source shape as
/// [`apply_hetzner_sd_check_interval`]: upstream has no per-config
/// `refresh_interval` YAML field for OpenStack, so this overwrites
/// unconditionally.
pub fn apply_openstack_sd_check_interval(
    cfg: &mut ScrapeConfigFile,
    interval: std::time::Duration,
) {
    for sc in &mut cfg.scrape_configs {
        for openstack in &mut sc.openstack_sd_configs {
            openstack.refresh_interval = interval;
        }
    }
}

/// Sets every `nomad_sd_config`'s `refresh_interval` from
/// `-promscrape.nomadSDCheckInterval`. Same flag-is-sole-source shape as
/// [`apply_consul_sd_check_interval`]: upstream has no per-config
/// `refresh_interval` YAML field for Nomad, so this overwrites
/// unconditionally.
pub fn apply_nomad_sd_check_interval(cfg: &mut ScrapeConfigFile, interval: std::time::Duration) {
    for sc in &mut cfg.scrape_configs {
        for nomad in &mut sc.nomad_sd_configs {
            nomad.refresh_interval = interval;
        }
    }
}

/// Sets every `marathon_sd_config`'s `refresh_interval` from
/// `-promscrape.marathonSDCheckInterval`. Same flag-is-sole-source shape as
/// [`apply_consul_sd_check_interval`]: upstream has no per-config
/// `refresh_interval` YAML field for Marathon, so this overwrites
/// unconditionally.
pub fn apply_marathon_sd_check_interval(cfg: &mut ScrapeConfigFile, interval: std::time::Duration) {
    for sc in &mut cfg.scrape_configs {
        for marathon in &mut sc.marathon_sd_configs {
            marathon.refresh_interval = interval;
        }
    }
}

/// Sets every `vultr_sd_config`'s `refresh_interval` from
/// `-promscrape.vultrSDCheckInterval`. Same flag-is-sole-source shape as
/// [`apply_consul_sd_check_interval`]: upstream has no per-config
/// `refresh_interval` YAML field for Vultr, so this overwrites unconditionally.
pub fn apply_vultr_sd_check_interval(cfg: &mut ScrapeConfigFile, interval: std::time::Duration) {
    for sc in &mut cfg.scrape_configs {
        for vultr in &mut sc.vultr_sd_configs {
            vultr.refresh_interval = interval;
        }
    }
}

/// Sets every `puppetdb_sd_config`'s `refresh_interval` from
/// `-promscrape.puppetdbSDCheckInterval`. Same flag-is-sole-source shape as
/// [`apply_consul_sd_check_interval`]: upstream has no per-config
/// `refresh_interval` YAML field for PuppetDB, so this overwrites
/// unconditionally.
pub fn apply_puppetdb_sd_check_interval(cfg: &mut ScrapeConfigFile, interval: std::time::Duration) {
    for sc in &mut cfg.scrape_configs {
        for puppetdb in &mut sc.puppetdb_sd_configs {
            puppetdb.refresh_interval = interval;
        }
    }
}

/// Sets every `kuma_sd_config`'s `refresh_interval` from
/// `-promscrape.kumaSDCheckInterval`. Same flag-is-sole-source shape as
/// [`apply_consul_sd_check_interval`]: upstream has no per-config
/// `refresh_interval` YAML field for Kuma, so this overwrites
/// unconditionally.
pub fn apply_kuma_sd_check_interval(cfg: &mut ScrapeConfigFile, interval: std::time::Duration) {
    for sc in &mut cfg.scrape_configs {
        for kuma in &mut sc.kuma_sd_configs {
            kuma.refresh_interval = interval;
        }
    }
}

/// Sets every `eureka_sd_config`'s `refresh_interval` from
/// `-promscrape.eurekaSDCheckInterval`. Same flag-is-sole-source shape as
/// [`apply_consul_sd_check_interval`]: upstream has no per-config
/// `refresh_interval` YAML field for Eureka, so this overwrites
/// unconditionally.
pub fn apply_eureka_sd_check_interval(cfg: &mut ScrapeConfigFile, interval: std::time::Duration) {
    for sc in &mut cfg.scrape_configs {
        for eureka in &mut sc.eureka_sd_configs {
            eureka.refresh_interval = interval;
        }
    }
}

/// Sets every `yandexcloud_sd_config`'s `refresh_interval` from
/// `-promscrape.yandexcloudSDCheckInterval`. Same flag-is-sole-source shape as
/// [`apply_consul_sd_check_interval`]: upstream has no per-config
/// `refresh_interval` YAML field for Yandex Cloud, so this overwrites
/// unconditionally.
pub fn apply_yandexcloud_sd_check_interval(
    cfg: &mut ScrapeConfigFile,
    interval: std::time::Duration,
) {
    for sc in &mut cfg.scrape_configs {
        for yandexcloud in &mut sc.yandexcloud_sd_configs {
            yandexcloud.refresh_interval = interval;
        }
    }
}

/// Sets every `dns_sd_config`'s `refresh_interval` from
/// `-promscrape.dnsSDCheckInterval`. Same flag-is-sole-source shape as
/// [`apply_consul_sd_check_interval`]: upstream has no per-config
/// `refresh_interval` YAML field for DNS, so this overwrites unconditionally.
pub fn apply_dns_sd_check_interval(cfg: &mut ScrapeConfigFile, interval: std::time::Duration) {
    for sc in &mut cfg.scrape_configs {
        for dns in &mut sc.dns_sd_configs {
            dns.refresh_interval = interval;
        }
    }
}

/// Sets every `docker_sd_config`'s `refresh_interval` from
/// `-promscrape.dockerSDCheckInterval`. Same flag-is-sole-source shape as
/// [`apply_consul_sd_check_interval`]: upstream has no per-config
/// `refresh_interval` YAML field for Docker, so this overwrites
/// unconditionally.
pub fn apply_docker_sd_check_interval(cfg: &mut ScrapeConfigFile, interval: std::time::Duration) {
    for sc in &mut cfg.scrape_configs {
        for docker in &mut sc.docker_sd_configs {
            docker.refresh_interval = interval;
        }
    }
}

/// Sets every `dockerswarm_sd_config`'s `refresh_interval` from
/// `-promscrape.dockerswarmSDCheckInterval`. Same flag-is-sole-source shape as
/// [`apply_docker_sd_check_interval`]: upstream has no per-config
/// `refresh_interval` YAML field for Docker Swarm, so this overwrites
/// unconditionally.
pub fn apply_dockerswarm_sd_check_interval(
    cfg: &mut ScrapeConfigFile,
    interval: std::time::Duration,
) {
    for sc in &mut cfg.scrape_configs {
        for dockerswarm in &mut sc.dockerswarm_sd_configs {
            dockerswarm.refresh_interval = interval;
        }
    }
}

#[cfg(test)]
#[path = "wiring_intervals_tests.rs"]
mod tests;
