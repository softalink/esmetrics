use super::*;
use crate::scrape::config::ScrapeConfig;

#[test]
fn apply_kubernetes_attach_metadata_defaults_fills_only_unset_configs() {
    use crate::scrape::config::{K8sAttachMetadata, KubernetesSdConfig};
    let mut cfg = ScrapeConfigFile {
        scrape_configs: vec![ScrapeConfig {
            job_name: "k".to_string(),
            kubernetes_sd_configs: vec![
                // No attach_metadata -> should be filled from the flag.
                KubernetesSdConfig {
                    role: "pod".to_string(),
                    attach_metadata: None,
                    ..KubernetesSdConfig::default()
                },
                // Already set -> fully overrides, left untouched.
                KubernetesSdConfig {
                    role: "pod".to_string(),
                    attach_metadata: Some(K8sAttachMetadata {
                        node: false,
                        namespace: true,
                    }),
                    ..KubernetesSdConfig::default()
                },
            ],
            ..ScrapeConfig::default()
        }],
        ..ScrapeConfigFile::default()
    };
    apply_kubernetes_attach_metadata_defaults(&mut cfg, true, false);
    let sds = &cfg.scrape_configs[0].kubernetes_sd_configs;
    assert_eq!(
        sds[0].attach_metadata,
        Some(K8sAttachMetadata {
            node: true,
            namespace: false
        })
    );
    assert_eq!(
        sds[1].attach_metadata,
        Some(K8sAttachMetadata {
            node: false,
            namespace: true
        })
    );
}

#[test]
fn apply_kubernetes_attach_metadata_defaults_noop_when_both_flags_false() {
    use crate::scrape::config::KubernetesSdConfig;
    let mut cfg = ScrapeConfigFile {
        scrape_configs: vec![ScrapeConfig {
            job_name: "k".to_string(),
            kubernetes_sd_configs: vec![KubernetesSdConfig {
                role: "pod".to_string(),
                attach_metadata: None,
                ..KubernetesSdConfig::default()
            }],
            ..ScrapeConfig::default()
        }],
        ..ScrapeConfigFile::default()
    };
    apply_kubernetes_attach_metadata_defaults(&mut cfg, false, false);
    assert!(cfg.scrape_configs[0].kubernetes_sd_configs[0]
        .attach_metadata
        .is_none());
}

#[test]
fn apply_default_max_scrape_size_only_fills_unset_jobs() {
    let mut cfg = ScrapeConfigFile {
        scrape_configs: vec![
            ScrapeConfig {
                job_name: "a".to_string(),
                max_scrape_size: 0,
                ..ScrapeConfig::default()
            },
            ScrapeConfig {
                job_name: "b".to_string(),
                max_scrape_size: 999,
                ..ScrapeConfig::default()
            },
        ],
        ..ScrapeConfigFile::default()
    };
    apply_default_max_scrape_size(&mut cfg, 12345);
    assert_eq!(cfg.scrape_configs[0].max_scrape_size, 12345);
    assert_eq!(cfg.scrape_configs[1].max_scrape_size, 999);
}

#[test]
fn apply_consul_sd_check_interval_sets_every_consul_config() {
    use crate::scrape::config::ConsulSdConfig;
    use std::time::Duration;
    let mut cfg = ScrapeConfigFile {
        scrape_configs: vec![ScrapeConfig {
            job_name: "consul".to_string(),
            consul_sd_configs: vec![
                ConsulSdConfig {
                    server: "a:8500".to_string(),
                    ..ConsulSdConfig::default()
                },
                ConsulSdConfig {
                    server: "b:8500".to_string(),
                    ..ConsulSdConfig::default()
                },
            ],
            ..ScrapeConfig::default()
        }],
        ..ScrapeConfigFile::default()
    };
    apply_consul_sd_check_interval(&mut cfg, Duration::from_secs(15));
    for c in &cfg.scrape_configs[0].consul_sd_configs {
        assert_eq!(c.refresh_interval, Duration::from_secs(15));
    }
}

#[test]
fn apply_consulagent_sd_check_interval_sets_every_consulagent_config() {
    use crate::scrape::config::ConsulagentSdConfig;
    use std::time::Duration;
    let mut cfg = ScrapeConfigFile {
        scrape_configs: vec![ScrapeConfig {
            job_name: "consulagent".to_string(),
            consulagent_sd_configs: vec![
                ConsulagentSdConfig {
                    server: "a:8500".to_string(),
                    ..ConsulagentSdConfig::default()
                },
                ConsulagentSdConfig {
                    server: "b:8500".to_string(),
                    ..ConsulagentSdConfig::default()
                },
            ],
            ..ScrapeConfig::default()
        }],
        ..ScrapeConfigFile::default()
    };
    apply_consulagent_sd_check_interval(&mut cfg, Duration::from_secs(25));
    for c in &cfg.scrape_configs[0].consulagent_sd_configs {
        assert_eq!(c.refresh_interval, Duration::from_secs(25));
    }
}

#[test]
fn apply_ec2_sd_check_interval_sets_every_ec2_config() {
    use crate::scrape::config::Ec2SdConfig;
    use std::time::Duration;
    let mut cfg = ScrapeConfigFile {
        scrape_configs: vec![ScrapeConfig {
            job_name: "ec2".to_string(),
            ec2_sd_configs: vec![
                Ec2SdConfig {
                    region: "us-east-1".to_string(),
                    ..Ec2SdConfig::default()
                },
                Ec2SdConfig {
                    region: "eu-west-2".to_string(),
                    ..Ec2SdConfig::default()
                },
            ],
            ..ScrapeConfig::default()
        }],
        ..ScrapeConfigFile::default()
    };
    apply_ec2_sd_check_interval(&mut cfg, Duration::from_secs(45));
    for e in &cfg.scrape_configs[0].ec2_sd_configs {
        assert_eq!(e.refresh_interval, Duration::from_secs(45));
    }
}

#[test]
fn apply_gce_sd_check_interval_sets_every_gce_config() {
    use crate::scrape::config::GceSdConfig;
    use std::time::Duration;
    let mut cfg = ScrapeConfigFile {
        scrape_configs: vec![ScrapeConfig {
            job_name: "gce".to_string(),
            gce_sd_configs: vec![
                GceSdConfig {
                    project: "p1".to_string(),
                    ..GceSdConfig::default()
                },
                GceSdConfig {
                    project: "p2".to_string(),
                    ..GceSdConfig::default()
                },
            ],
            ..ScrapeConfig::default()
        }],
        ..ScrapeConfigFile::default()
    };
    apply_gce_sd_check_interval(&mut cfg, Duration::from_secs(75));
    for g in &cfg.scrape_configs[0].gce_sd_configs {
        assert_eq!(g.refresh_interval, Duration::from_secs(75));
    }
}

#[test]
fn apply_openstack_sd_check_interval_sets_every_openstack_config() {
    use crate::scrape::config::OpenstackSdConfig;
    use std::time::Duration;
    let mut cfg = ScrapeConfigFile {
        scrape_configs: vec![ScrapeConfig {
            job_name: "openstack".to_string(),
            openstack_sd_configs: vec![
                OpenstackSdConfig {
                    role: "instance".to_string(),
                    ..OpenstackSdConfig::default()
                },
                OpenstackSdConfig {
                    role: "hypervisor".to_string(),
                    ..OpenstackSdConfig::default()
                },
            ],
            ..ScrapeConfig::default()
        }],
        ..ScrapeConfigFile::default()
    };
    apply_openstack_sd_check_interval(&mut cfg, Duration::from_secs(77));
    for o in &cfg.scrape_configs[0].openstack_sd_configs {
        assert_eq!(o.refresh_interval, Duration::from_secs(77));
    }
}

#[test]
fn apply_azure_sd_check_interval_sets_every_azure_config() {
    use crate::scrape::config::AzureSdConfig;
    use std::time::Duration;
    let mut cfg = ScrapeConfigFile {
        scrape_configs: vec![ScrapeConfig {
            job_name: "azure".to_string(),
            azure_sd_configs: vec![
                AzureSdConfig {
                    subscription_id: "s1".to_string(),
                    ..AzureSdConfig::default()
                },
                AzureSdConfig {
                    subscription_id: "s2".to_string(),
                    ..AzureSdConfig::default()
                },
            ],
            ..ScrapeConfig::default()
        }],
        ..ScrapeConfigFile::default()
    };
    apply_azure_sd_check_interval(&mut cfg, Duration::from_secs(50));
    for a in &cfg.scrape_configs[0].azure_sd_configs {
        assert_eq!(a.refresh_interval, Duration::from_secs(50));
    }
}

#[test]
fn apply_digitalocean_sd_check_interval_sets_every_digitalocean_config() {
    use crate::scrape::config::DigitaloceanSdConfig;
    use std::time::Duration;
    let mut cfg = ScrapeConfigFile {
        scrape_configs: vec![ScrapeConfig {
            job_name: "do".to_string(),
            digitalocean_sd_configs: vec![
                DigitaloceanSdConfig {
                    server: "a".to_string(),
                    ..DigitaloceanSdConfig::default()
                },
                DigitaloceanSdConfig {
                    server: "b".to_string(),
                    ..DigitaloceanSdConfig::default()
                },
            ],
            ..ScrapeConfig::default()
        }],
        ..ScrapeConfigFile::default()
    };
    apply_digitalocean_sd_check_interval(&mut cfg, Duration::from_secs(90));
    for d in &cfg.scrape_configs[0].digitalocean_sd_configs {
        assert_eq!(d.refresh_interval, Duration::from_secs(90));
    }
}

#[test]
fn apply_vultr_sd_check_interval_sets_every_vultr_config() {
    use crate::scrape::config::VultrSdConfig;
    use std::time::Duration;
    let mut cfg = ScrapeConfigFile {
        scrape_configs: vec![ScrapeConfig {
            job_name: "vultr".to_string(),
            vultr_sd_configs: vec![
                VultrSdConfig {
                    server: "a".to_string(),
                    ..VultrSdConfig::default()
                },
                VultrSdConfig {
                    server: "b".to_string(),
                    ..VultrSdConfig::default()
                },
            ],
            ..ScrapeConfig::default()
        }],
        ..ScrapeConfigFile::default()
    };
    apply_vultr_sd_check_interval(&mut cfg, Duration::from_secs(90));
    for v in &cfg.scrape_configs[0].vultr_sd_configs {
        assert_eq!(v.refresh_interval, Duration::from_secs(90));
    }
}

#[test]
fn apply_puppetdb_sd_check_interval_sets_every_puppetdb_config() {
    use crate::scrape::config::PuppetdbSdConfig;
    use std::time::Duration;
    let mut cfg = ScrapeConfigFile {
        scrape_configs: vec![ScrapeConfig {
            job_name: "puppetdb".to_string(),
            puppetdb_sd_configs: vec![
                PuppetdbSdConfig {
                    url: "https://a.example".to_string(),
                    query: "resources {}".to_string(),
                    ..PuppetdbSdConfig::default()
                },
                PuppetdbSdConfig {
                    url: "https://b.example".to_string(),
                    query: "resources {}".to_string(),
                    ..PuppetdbSdConfig::default()
                },
            ],
            ..ScrapeConfig::default()
        }],
        ..ScrapeConfigFile::default()
    };
    apply_puppetdb_sd_check_interval(&mut cfg, Duration::from_secs(90));
    for p in &cfg.scrape_configs[0].puppetdb_sd_configs {
        assert_eq!(p.refresh_interval, Duration::from_secs(90));
    }
}

#[test]
fn apply_kuma_sd_check_interval_sets_every_kuma_config() {
    use crate::scrape::config::KumaSdConfig;
    use std::time::Duration;
    let mut cfg = ScrapeConfigFile {
        scrape_configs: vec![ScrapeConfig {
            job_name: "kuma".to_string(),
            kuma_sd_configs: vec![
                KumaSdConfig {
                    server: "https://a.example:5676".to_string(),
                    ..KumaSdConfig::default()
                },
                KumaSdConfig {
                    server: "https://b.example:5676".to_string(),
                    ..KumaSdConfig::default()
                },
            ],
            ..ScrapeConfig::default()
        }],
        ..ScrapeConfigFile::default()
    };
    apply_kuma_sd_check_interval(&mut cfg, Duration::from_secs(90));
    for k in &cfg.scrape_configs[0].kuma_sd_configs {
        assert_eq!(k.refresh_interval, Duration::from_secs(90));
    }
}

#[test]
fn apply_eureka_sd_check_interval_sets_every_eureka_config() {
    use crate::scrape::config::EurekaSdConfig;
    use std::time::Duration;
    let mut cfg = ScrapeConfigFile {
        scrape_configs: vec![ScrapeConfig {
            job_name: "eureka".to_string(),
            eureka_sd_configs: vec![
                EurekaSdConfig {
                    server: "a.example:8080".to_string(),
                    ..EurekaSdConfig::default()
                },
                EurekaSdConfig {
                    server: "b.example:8080".to_string(),
                    ..EurekaSdConfig::default()
                },
            ],
            ..ScrapeConfig::default()
        }],
        ..ScrapeConfigFile::default()
    };
    apply_eureka_sd_check_interval(&mut cfg, Duration::from_secs(90));
    for e in &cfg.scrape_configs[0].eureka_sd_configs {
        assert_eq!(e.refresh_interval, Duration::from_secs(90));
    }
}

#[test]
fn apply_dns_sd_check_interval_sets_every_dns_config() {
    use crate::scrape::config::DnsSdConfig;
    use crate::scrape::dns::DnsRecordType;
    use std::time::Duration;
    let mut cfg = ScrapeConfigFile {
        scrape_configs: vec![ScrapeConfig {
            job_name: "dns".to_string(),
            dns_sd_configs: vec![
                DnsSdConfig {
                    names: vec!["_a._tcp.svc".to_string()],
                    ..DnsSdConfig::default()
                },
                DnsSdConfig {
                    names: vec!["h".to_string()],
                    record_type: DnsRecordType::A,
                    port: Some(80),
                    ..DnsSdConfig::default()
                },
            ],
            ..ScrapeConfig::default()
        }],
        ..ScrapeConfigFile::default()
    };
    apply_dns_sd_check_interval(&mut cfg, Duration::from_secs(90));
    for d in &cfg.scrape_configs[0].dns_sd_configs {
        assert_eq!(d.refresh_interval, Duration::from_secs(90));
    }
}

#[test]
fn apply_docker_sd_check_interval_sets_every_docker_config() {
    use crate::scrape::config::DockerSdConfig;
    use std::time::Duration;
    let mut cfg = ScrapeConfigFile {
        scrape_configs: vec![ScrapeConfig {
            job_name: "docker".to_string(),
            docker_sd_configs: vec![
                DockerSdConfig {
                    host: "unix:///var/run/docker.sock".to_string(),
                    ..DockerSdConfig::default()
                },
                DockerSdConfig {
                    host: "tcp://dockerd:2375".to_string(),
                    ..DockerSdConfig::default()
                },
            ],
            ..ScrapeConfig::default()
        }],
        ..ScrapeConfigFile::default()
    };
    apply_docker_sd_check_interval(&mut cfg, Duration::from_secs(90));
    for d in &cfg.scrape_configs[0].docker_sd_configs {
        assert_eq!(d.refresh_interval, Duration::from_secs(90));
    }
}

#[test]
fn apply_dockerswarm_sd_check_interval_sets_every_config() {
    use crate::scrape::config::DockerswarmSdConfig;
    use std::time::Duration;
    let mut cfg = ScrapeConfigFile {
        scrape_configs: vec![ScrapeConfig {
            job_name: "swarm".to_string(),
            dockerswarm_sd_configs: vec![
                DockerswarmSdConfig {
                    host: "unix:///var/run/docker.sock".to_string(),
                    role: "nodes".to_string(),
                    ..DockerswarmSdConfig::default()
                },
                DockerswarmSdConfig {
                    host: "tcp://dockerd:2375".to_string(),
                    role: "tasks".to_string(),
                    ..DockerswarmSdConfig::default()
                },
            ],
            ..ScrapeConfig::default()
        }],
        ..ScrapeConfigFile::default()
    };
    apply_dockerswarm_sd_check_interval(&mut cfg, Duration::from_secs(90));
    for d in &cfg.scrape_configs[0].dockerswarm_sd_configs {
        assert_eq!(d.refresh_interval, Duration::from_secs(90));
    }
}
