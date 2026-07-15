//! SD-provider assembly and secret hashing for [`super::manager`].
//!
//! Split out of `manager.rs` to keep that file under the repo's 800-line
//! cap: this module owns the two per-provider-growing units — assembling a
//! job's discovery providers ([`build_providers`], one arm per SD kind) and
//! feeding every job secret into the reload-checksum hasher
//! ([`hash_secrets`], one loop per SD kind). Both are `pub(crate)` and called
//! from `manager.rs`'s `build_job`/`job_checksum`.

use std::hash::Hasher;

use super::azure::AzureDiscovery;
use super::config::{ScrapeConfig, ScrapeError};
use super::consul::ConsulDiscovery;
use super::consulagent::ConsulagentDiscovery;
use super::digitalocean::DigitaloceanDiscovery;
use super::discovery::{Discovery, FileSdDiscovery, HttpSdDiscovery, StaticDiscovery};
use super::dns::DnsDiscovery;
use super::docker::DockerDiscovery;
use super::dockerswarm::DockerswarmDiscovery;
use super::ec2::Ec2Discovery;
use super::eureka::EurekaDiscovery;
use super::gce::GceDiscovery;
use super::hetzner::HetznerDiscovery;
use super::kubernetes::KubernetesDiscovery;
use super::kuma::KumaDiscovery;
use super::marathon::MarathonDiscovery;
use super::nomad::NomadDiscovery;
use super::openstack::OpenstackDiscovery;
use super::ovhcloud::OvhcloudDiscovery;
use super::puppetdb::PuppetdbDiscovery;
use super::vultr::VultrDiscovery;
use super::yandexcloud::YandexcloudDiscovery;

/// Builds every discovery provider `sc` configures: one [`StaticDiscovery`]
/// (only if `static_configs` is non-empty — an empty provider would just
/// poll to an empty `Vec` forever, so it's skipped), one [`FileSdDiscovery`]
/// per `file_sd_configs` entry, one [`HttpSdDiscovery`] per `http_sd_configs`
/// entry, one [`KubernetesDiscovery`] per `kubernetes_sd_configs` entry, one
/// [`ConsulDiscovery`] per `consul_sd_configs` entry, one [`Ec2Discovery`]
/// per `ec2_sd_configs` entry, and one [`DigitaloceanDiscovery`] per
/// `digitalocean_sd_configs` entry.
///
/// Fallible only because of the last four groups: [`KubernetesDiscovery::new`],
/// [`ConsulDiscovery::new`], [`Ec2Discovery::new`], and
/// [`DigitaloceanDiscovery::new`] resolve that entry's
/// auth/config (reading local token/CA files, requiring in-cluster env vars
/// when a k8s `api_server` is unset, or rejecting a deferred EC2 `role_arn`),
/// which can fail on a genuinely bad config — see their docs. A
/// down-at-startup Consul/k8s/AWS server does NOT fail here (discovery retries
/// on its background thread); only bad config propagates up through
/// `build_job` and `ScrapeManager::start`/`reload`, so a misconfigured SD
/// entry is caught at startup/reload time rather than silently discovering
/// nothing.
pub(crate) fn build_providers(sc: &ScrapeConfig) -> Result<Vec<Box<dyn Discovery>>, ScrapeError> {
    let mut providers: Vec<Box<dyn Discovery>> = Vec::new();
    if !sc.static_configs.is_empty() {
        providers.push(Box::new(StaticDiscovery::new(
            &sc.static_configs,
            &sc.job_name,
        )));
    }
    for file_sd in &sc.file_sd_configs {
        providers.push(Box::new(FileSdDiscovery::new(file_sd, &sc.job_name)));
    }
    for http_sd in &sc.http_sd_configs {
        providers.push(Box::new(HttpSdDiscovery::new(
            http_sd.clone(),
            &sc.job_name,
        )));
    }
    for k8s_sd in &sc.kubernetes_sd_configs {
        providers.push(Box::new(KubernetesDiscovery::new(k8s_sd)?));
    }
    for consul_sd in &sc.consul_sd_configs {
        providers.push(Box::new(ConsulDiscovery::new(consul_sd, &sc.job_name)?));
    }
    for consulagent_sd in &sc.consulagent_sd_configs {
        providers.push(Box::new(ConsulagentDiscovery::new(
            consulagent_sd,
            &sc.job_name,
        )?));
    }
    for ec2_sd in &sc.ec2_sd_configs {
        providers.push(Box::new(Ec2Discovery::new(ec2_sd, &sc.job_name)?));
    }
    for gce_sd in &sc.gce_sd_configs {
        providers.push(Box::new(GceDiscovery::new(gce_sd, &sc.job_name)?));
    }
    for azure_sd in &sc.azure_sd_configs {
        providers.push(Box::new(AzureDiscovery::new(azure_sd, &sc.job_name)?));
    }
    for do_sd in &sc.digitalocean_sd_configs {
        providers.push(Box::new(DigitaloceanDiscovery::new(do_sd, &sc.job_name)?));
    }
    for hetzner_sd in &sc.hetzner_sd_configs {
        providers.push(Box::new(HetznerDiscovery::new(hetzner_sd, &sc.job_name)?));
    }
    for nomad_sd in &sc.nomad_sd_configs {
        providers.push(Box::new(NomadDiscovery::new(nomad_sd, &sc.job_name)?));
    }
    for marathon_sd in &sc.marathon_sd_configs {
        providers.push(Box::new(MarathonDiscovery::new(marathon_sd, &sc.job_name)?));
    }
    for vultr_sd in &sc.vultr_sd_configs {
        providers.push(Box::new(VultrDiscovery::new(vultr_sd, &sc.job_name)?));
    }
    for puppetdb_sd in &sc.puppetdb_sd_configs {
        providers.push(Box::new(PuppetdbDiscovery::new(puppetdb_sd, &sc.job_name)?));
    }
    for kuma_sd in &sc.kuma_sd_configs {
        providers.push(Box::new(KumaDiscovery::new(kuma_sd, &sc.job_name)?));
    }
    for eureka_sd in &sc.eureka_sd_configs {
        providers.push(Box::new(EurekaDiscovery::new(eureka_sd, &sc.job_name)?));
    }
    for yandexcloud_sd in &sc.yandexcloud_sd_configs {
        providers.push(Box::new(YandexcloudDiscovery::new(
            yandexcloud_sd,
            &sc.job_name,
        )?));
    }
    for ovhcloud_sd in &sc.ovhcloud_sd_configs {
        providers.push(Box::new(OvhcloudDiscovery::new(ovhcloud_sd, &sc.job_name)?));
    }
    for openstack_sd in &sc.openstack_sd_configs {
        providers.push(Box::new(OpenstackDiscovery::new(
            openstack_sd,
            &sc.job_name,
        )?));
    }
    for dns_sd in &sc.dns_sd_configs {
        providers.push(Box::new(DnsDiscovery::new(dns_sd, &sc.job_name)?));
    }
    for docker_sd in &sc.docker_sd_configs {
        providers.push(Box::new(DockerDiscovery::new(docker_sd, &sc.job_name)?));
    }
    for dockerswarm_sd in &sc.dockerswarm_sd_configs {
        providers.push(Box::new(DockerswarmDiscovery::new(
            dockerswarm_sd,
            &sc.job_name,
        )?));
    }
    Ok(providers)
}

/// Feeds an optional secret string into `hasher` as raw bytes, with a
/// presence marker (`0` absent / `1` present) and a `0xff` terminator so
/// adjacent fields can't concatenate ambiguously (e.g. `Some("ab")` vs
/// `Some("a") + Some("b")`). The value is written only via
/// [`Hasher::write`] — it is never turned into a `String` or logged.
fn hash_secret_opt(value: Option<&str>, hasher: &mut impl Hasher) {
    match value {
        Some(v) => {
            hasher.write_u8(1);
            hasher.write(v.as_bytes());
            hasher.write_u8(0xff);
        }
        None => hasher.write_u8(0),
    }
}

/// Feeds the secret values of a shared [`crate::client::AuthConfig`] (the
/// bearer token and the basic-auth *password* — the username is not secret)
/// into `hasher`.
fn hash_auth_secrets(auth: &crate::client::AuthConfig, hasher: &mut impl Hasher) {
    hash_secret_opt(auth.bearer.as_deref(), hasher);
    hash_secret_opt(auth.basic.as_ref().map(|(_, pass)| pass.as_str()), hasher);
}

/// Feeds every real secret reachable from `sc` into `hasher`, in a fixed,
/// deterministic order (each provider `Vec` in declaration order; a fixed
/// field order within each config). Secrets are written only as bytes — see
/// `manager::job_checksum` for why this is required and [`hash_secret_opt`]
/// for the leak-free encoding. TLS `*_file`, `credentials_file`,
/// `kubeconfig_file`, and `service_account_key_file` hold filesystem *paths*
/// (already covered by the non-redacted `Debug`), not secret material, so
/// they are not hashed here.
pub(crate) fn hash_secrets(sc: &ScrapeConfig, hasher: &mut impl Hasher) {
    // Job-level auth (also the fallback for many providers).
    hash_auth_secrets(&sc.auth, hasher);

    for k in &sc.kubernetes_sd_configs {
        hash_auth_secrets(&k.auth, hasher);
        match &k.oauth2 {
            Some(o) => {
                hasher.write_u8(1);
                hash_secret_opt(o.client_secret.as_deref(), hasher);
            }
            None => hasher.write_u8(0),
        }
    }
    for c in &sc.consul_sd_configs {
        hash_secret_opt(c.token.as_deref(), hasher);
        hash_secret_opt(c.password.as_deref(), hasher);
        hash_auth_secrets(&c.auth, hasher);
    }
    for c in &sc.consulagent_sd_configs {
        hash_secret_opt(c.token.as_deref(), hasher);
        hash_secret_opt(c.password.as_deref(), hasher);
        hash_auth_secrets(&c.auth, hasher);
    }
    for e in &sc.ec2_sd_configs {
        hash_secret_opt(e.secret_key.as_deref(), hasher);
        hash_secret_opt(e.session_token.as_deref(), hasher);
    }
    for g in &sc.gce_sd_configs {
        hash_secret_opt(g.bearer_token.as_deref(), hasher);
    }
    for a in &sc.azure_sd_configs {
        hash_secret_opt(a.client_secret.as_deref(), hasher);
    }
    for d in &sc.digitalocean_sd_configs {
        hash_auth_secrets(&d.auth, hasher);
    }
    for h in &sc.hetzner_sd_configs {
        hash_auth_secrets(&h.auth, hasher);
    }
    for n in &sc.nomad_sd_configs {
        hash_auth_secrets(&n.auth, hasher);
    }
    for m in &sc.marathon_sd_configs {
        hash_auth_secrets(&m.auth, hasher);
    }
    for v in &sc.vultr_sd_configs {
        hash_auth_secrets(&v.auth, hasher);
    }
    for p in &sc.puppetdb_sd_configs {
        hash_auth_secrets(&p.auth, hasher);
    }
    for k in &sc.kuma_sd_configs {
        hash_auth_secrets(&k.auth, hasher);
    }
    for e in &sc.eureka_sd_configs {
        hash_auth_secrets(&e.auth, hasher);
    }
    for y in &sc.yandexcloud_sd_configs {
        hash_secret_opt(y.yandex_passport_oauth_token.as_deref(), hasher);
    }
    for o in &sc.ovhcloud_sd_configs {
        // OVH secrets are non-`Option` `String`s (always present).
        hash_secret_opt(Some(o.application_secret.as_str()), hasher);
        hash_secret_opt(Some(o.consumer_key.as_str()), hasher);
    }
    for o in &sc.openstack_sd_configs {
        // Provider-specific secrets NOT carried in the shared AuthConfig.
        hash_secret_opt(o.password.as_deref(), hasher);
        hash_secret_opt(o.application_credential_secret.as_deref(), hasher);
    }
    for d in &sc.docker_sd_configs {
        hash_auth_secrets(&d.auth, hasher);
    }
    for d in &sc.dockerswarm_sd_configs {
        hash_auth_secrets(&d.auth, hasher);
    }
}
