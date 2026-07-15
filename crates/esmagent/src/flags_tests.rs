//! Tests for [`super`] (`flags.rs`) — split out per this crate's
//! `#[path]`-sibling convention (see `config_tests.rs`) to keep `flags.rs`
//! under the 800-line cap.

use super::*;

fn args(a: &[&str]) -> Vec<String> {
    a.iter().map(|s| s.to_string()).collect()
}

#[test]
fn parses_core_flags() {
    let f = parse_flags(
        &[
            "-remoteWrite.url=http://a/api/v1/write",
            "-remoteWrite.url=http://b/api/v1/write",
            "-httpListenAddr=:8429",
            "-remoteWrite.tmpDataPath=/tmp/eq",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>(),
    )
    .unwrap();
    assert_eq!(f.remote_write_urls.len(), 2);
    assert_eq!(f.http_listen_addr, ":8429");
}

#[test]
fn defaults_are_sensible() {
    let f = parse_flags(&args(&["-remoteWrite.url=http://a"])).unwrap();
    assert_eq!(f.http_listen_addr, ":8429");
    assert_eq!(f.remote_write_tmp_data_path, "esmagent-remotewrite-data");
    assert_eq!(f.remote_write_max_disk_usage_per_url, 0);
    assert_eq!(f.remote_write_queues, 1);
    assert_eq!(f.remote_write_max_block_size, 8 * 1024 * 1024);
    assert_eq!(f.remote_write_flush_interval, Duration::from_secs(1));
    assert_eq!(f.remote_write_retry_min_interval, Duration::from_secs(1));
    assert_eq!(f.remote_write_retry_max_interval, Duration::from_secs(30));
    assert_eq!(f.http_read_timeout, Duration::from_secs(30));
    assert!(!f.dry_run);
    assert!(f.remote_write_url_relabel_configs.is_empty());
    assert_eq!(f.remote_write_relabel_config, "");
    assert_eq!(f.metrics_auth_key, "");
    assert_eq!(f.promscrape_config, None);
    assert_eq!(f.promscrape_config_check_interval, Duration::ZERO);
    assert!(!f.promscrape_suppress_scrape_errors);
    assert_eq!(f.promscrape_max_scrape_size, 16 * 1024 * 1024);
    assert!(!f.promscrape_config_dry_run);
    assert_eq!(
        f.promscrape_consul_sd_check_interval,
        Duration::from_secs(30)
    );
    assert_eq!(
        f.promscrape_consulagent_sd_check_interval,
        Duration::from_secs(30)
    );
    assert_eq!(f.promscrape_ec2_sd_check_interval, Duration::from_secs(60));
    assert_eq!(f.promscrape_gce_sd_check_interval, Duration::from_secs(60));
    assert_eq!(
        f.promscrape_azure_sd_check_interval,
        Duration::from_secs(60)
    );
    assert_eq!(
        f.promscrape_digitalocean_sd_check_interval,
        Duration::from_secs(60)
    );
    assert_eq!(
        f.promscrape_hetzner_sd_check_interval,
        Duration::from_secs(60)
    );
    assert_eq!(
        f.promscrape_nomad_sd_check_interval,
        Duration::from_secs(30)
    );
    assert_eq!(
        f.promscrape_marathon_sd_check_interval,
        Duration::from_secs(30)
    );
    assert_eq!(
        f.promscrape_vultr_sd_check_interval,
        Duration::from_secs(30)
    );
    assert_eq!(
        f.promscrape_puppetdb_sd_check_interval,
        Duration::from_secs(30)
    );
    assert_eq!(f.promscrape_kuma_sd_check_interval, Duration::from_secs(30));
    assert_eq!(
        f.promscrape_eureka_sd_check_interval,
        Duration::from_secs(30)
    );
    assert_eq!(
        f.promscrape_yandexcloud_sd_check_interval,
        Duration::from_secs(30)
    );
    assert_eq!(
        f.promscrape_ovhcloud_sd_check_interval,
        Duration::from_secs(30)
    );
    assert_eq!(
        f.promscrape_openstack_sd_check_interval,
        Duration::from_secs(30)
    );
    assert_eq!(f.promscrape_dns_sd_check_interval, Duration::from_secs(30));
    assert_eq!(
        f.promscrape_docker_sd_check_interval,
        Duration::from_secs(30)
    );
    assert_eq!(
        f.promscrape_dockerswarm_sd_check_interval,
        Duration::from_secs(30)
    );
}

#[test]
fn parses_promscrape_flags() {
    let f = parse_flags(&args(&[
        "-remoteWrite.url=http://a/api/v1/write",
        "-promscrape.config=/etc/scrape.yml",
        "-promscrape.configCheckInterval=30s",
        "-promscrape.suppressScrapeErrors",
        "-promscrape.maxScrapeSize=1048576",
        "-promscrape.consulSDCheckInterval=15s",
        "-promscrape.consulagentSDCheckInterval=25s",
        "-promscrape.ec2SDCheckInterval=45s",
        "-promscrape.gceSDCheckInterval=75s",
        "-promscrape.azureSDCheckInterval=50s",
        "-promscrape.digitaloceanSDCheckInterval=90s",
        "-promscrape.hetznerSDCheckInterval=55s",
        "-promscrape.nomadSDCheckInterval=15s",
        "-promscrape.marathonSDCheckInterval=20s",
        "-promscrape.vultrSDCheckInterval=35s",
        "-promscrape.puppetdbSDCheckInterval=42s",
        "-promscrape.kumaSDCheckInterval=33s",
        "-promscrape.eurekaSDCheckInterval=18s",
        "-promscrape.yandexcloudSDCheckInterval=12s",
        "-promscrape.ovhcloudSDCheckInterval=17s",
        "-promscrape.openstackSDCheckInterval=19s",
        "-promscrape.dnsSDCheckInterval=11s",
        "-promscrape.dockerSDCheckInterval=13s",
        "-promscrape.dockerswarmSDCheckInterval=14s",
        "-promscrape.config.dryRun",
    ]))
    .unwrap();
    assert_eq!(f.promscrape_config.as_deref(), Some("/etc/scrape.yml"));
    assert_eq!(f.promscrape_config_check_interval, Duration::from_secs(30));
    assert!(f.promscrape_suppress_scrape_errors);
    assert_eq!(f.promscrape_max_scrape_size, 1_048_576);
    assert_eq!(
        f.promscrape_consul_sd_check_interval,
        Duration::from_secs(15)
    );
    assert_eq!(
        f.promscrape_consulagent_sd_check_interval,
        Duration::from_secs(25)
    );
    assert_eq!(f.promscrape_ec2_sd_check_interval, Duration::from_secs(45));
    assert_eq!(f.promscrape_gce_sd_check_interval, Duration::from_secs(75));
    assert_eq!(
        f.promscrape_azure_sd_check_interval,
        Duration::from_secs(50)
    );
    assert_eq!(
        f.promscrape_digitalocean_sd_check_interval,
        Duration::from_secs(90)
    );
    assert_eq!(
        f.promscrape_hetzner_sd_check_interval,
        Duration::from_secs(55)
    );
    assert_eq!(
        f.promscrape_nomad_sd_check_interval,
        Duration::from_secs(15)
    );
    assert_eq!(
        f.promscrape_marathon_sd_check_interval,
        Duration::from_secs(20)
    );
    assert_eq!(
        f.promscrape_vultr_sd_check_interval,
        Duration::from_secs(35)
    );
    assert_eq!(
        f.promscrape_puppetdb_sd_check_interval,
        Duration::from_secs(42)
    );
    assert_eq!(f.promscrape_kuma_sd_check_interval, Duration::from_secs(33));
    assert_eq!(
        f.promscrape_eureka_sd_check_interval,
        Duration::from_secs(18)
    );
    assert_eq!(
        f.promscrape_yandexcloud_sd_check_interval,
        Duration::from_secs(12)
    );
    assert_eq!(
        f.promscrape_ovhcloud_sd_check_interval,
        Duration::from_secs(17)
    );
    assert_eq!(
        f.promscrape_openstack_sd_check_interval,
        Duration::from_secs(19)
    );
    assert_eq!(f.promscrape_dns_sd_check_interval, Duration::from_secs(11));
    assert_eq!(
        f.promscrape_docker_sd_check_interval,
        Duration::from_secs(13)
    );
    assert_eq!(
        f.promscrape_dockerswarm_sd_check_interval,
        Duration::from_secs(14)
    );
    assert!(f.promscrape_config_dry_run);
}

#[test]
fn parses_kubernetes_attach_metadata_all_flags() {
    let f = parse_flags(&args(&[
        "-remoteWrite.url=http://a",
        "-promscrape.kubernetes.attachNodeMetadataAll=true",
        "-promscrape.kubernetes.attachNamespaceMetadataAll",
    ]))
    .unwrap();
    assert!(f.promscrape_kubernetes_attach_node_metadata_all);
    assert!(f.promscrape_kubernetes_attach_namespace_metadata_all);
}

#[test]
fn kubernetes_attach_metadata_all_defaults_false() {
    let f = parse_flags(&args(&["-remoteWrite.url=http://a"])).unwrap();
    assert!(!f.promscrape_kubernetes_attach_node_metadata_all);
    assert!(!f.promscrape_kubernetes_attach_namespace_metadata_all);
}

#[test]
fn remote_write_url_and_url_relabel_config_are_repeatable() {
    let f = parse_flags(&args(&[
        "-remoteWrite.url=http://a",
        "-remoteWrite.url",
        "http://b",
        "-remoteWrite.urlRelabelConfig=/a.yml",
        "-remoteWrite.urlRelabelConfig=/b.yml",
    ]))
    .unwrap();
    assert_eq!(f.remote_write_urls, vec!["http://a", "http://b"]);
    assert_eq!(f.remote_write_url_relabel_configs, vec!["/a.yml", "/b.yml"]);
}

#[test]
fn accepts_all_flag_syntaxes() {
    for a in [
        &["-remoteWrite.url=x", "-httpListenAddr=127.0.0.1:9999"][..],
        &["-remoteWrite.url=x", "--httpListenAddr=127.0.0.1:9999"][..],
        &["-remoteWrite.url=x", "-httpListenAddr", "127.0.0.1:9999"][..],
        &["-remoteWrite.url=x", "--httpListenAddr", "127.0.0.1:9999"][..],
    ] {
        let f = parse_flags(&args(a)).unwrap();
        assert_eq!(f.http_listen_addr, "127.0.0.1:9999", "args: {a:?}");
    }
}

#[test]
fn parses_positional_basic_auth_bearer_and_tls_flags() {
    let f = parse_flags(&args(&[
        "-remoteWrite.url=http://a",
        "-remoteWrite.url=http://b",
        "-remoteWrite.basicAuth.username=alice",
        "-remoteWrite.basicAuth.username=",
        "-remoteWrite.basicAuth.password=s3cr3t",
        "-remoteWrite.bearerToken=",
        "-remoteWrite.bearerToken=tok-b",
        "-remoteWrite.tlsCAFile=/ca.pem",
        "-remoteWrite.tlsInsecureSkipVerify",
        "-remoteWrite.tlsInsecureSkipVerify=false",
    ]))
    .unwrap();
    assert_eq!(f.remote_write_auth.username, vec!["alice", ""]);
    assert_eq!(f.remote_write_auth.password, vec!["s3cr3t"]);
    assert_eq!(f.remote_write_auth.bearer_token, vec!["", "tok-b"]);
    assert_eq!(f.remote_write_auth.tls_ca_file, vec!["/ca.pem"]);
    assert_eq!(
        f.remote_write_auth.tls_insecure_skip_verify,
        vec![true, false]
    );
    assert_eq!(at(&f.remote_write_auth.username, 0), "alice");
    assert_eq!(at(&f.remote_write_auth.username, 5), "");
    assert!(bool_at(&f.remote_write_auth.tls_insecure_skip_verify, 0));
    assert!(!bool_at(&f.remote_write_auth.tls_insecure_skip_verify, 5));
}

#[test]
fn parses_remote_write_tuning_and_web_gating_flags() {
    let f = parse_flags(&args(&[
        "-remoteWrite.url=http://a",
        "-remoteWrite.maxDiskUsagePerURL=1000000",
        "-remoteWrite.queues=4",
        "-remoteWrite.maxBlockSize=2048",
        "-remoteWrite.flushInterval=2s",
        "-remoteWrite.retryMinInterval=500ms",
        "-remoteWrite.retryMaxInterval=1m",
        "-remoteWrite.relabelConfig=/global.yml",
        "-httpReadTimeout=15s",
        "-metrics.authKey=msecret",
        "-dryRun",
    ]))
    .unwrap();
    assert_eq!(f.remote_write_max_disk_usage_per_url, 1_000_000);
    assert_eq!(f.remote_write_queues, 4);
    assert_eq!(f.remote_write_max_block_size, 2048);
    assert_eq!(f.remote_write_flush_interval, Duration::from_secs(2));
    assert_eq!(
        f.remote_write_retry_min_interval,
        Duration::from_millis(500)
    );
    assert_eq!(f.remote_write_retry_max_interval, Duration::from_secs(60));
    assert_eq!(f.remote_write_relabel_config, "/global.yml");
    assert_eq!(f.http_read_timeout, Duration::from_secs(15));
    assert_eq!(f.metrics_auth_key, "msecret");
    assert!(f.dry_run);
}

#[test]
fn version_flag_is_boolean() {
    assert_eq!(parse_flags(&args(&["-version"])), Err(FlagError::Version));
    assert_eq!(parse_flags(&args(&["--version"])), Err(FlagError::Version));
    assert_eq!(
        parse_flags(&args(&["-version=true"])),
        Err(FlagError::Version)
    );
    assert!(parse_flags(&args(&["-version=false"])).is_ok());
    assert!(matches!(
        parse_flags(&args(&["-version=maybe"])),
        Err(FlagError::Invalid(_))
    ));
}

#[test]
fn help_flag_variants() {
    for a in [&["-help"][..], &["--help"][..], &["-h"][..]] {
        assert_eq!(parse_flags(&args(a)), Err(FlagError::Help), "args: {a:?}");
    }
}

#[test]
fn unknown_flag_is_an_error_with_usage() {
    let err = parse_flags(&args(&["-bogus"])).unwrap_err();
    match err {
        FlagError::Invalid(msg) => {
            assert!(
                msg.contains("flag provided but not defined: -bogus"),
                "{msg}"
            );
            assert!(msg.contains("Usage of esmagent"), "{msg}");
        }
        other => panic!("expected Invalid, got {other:?}"),
    }
}

#[test]
fn missing_value_is_an_error() {
    let err = parse_flags(&args(&["-httpListenAddr"])).unwrap_err();
    match err {
        FlagError::Invalid(msg) => {
            assert!(
                msg.contains("missing value for flag -httpListenAddr"),
                "{msg}"
            )
        }
        other => panic!("expected Invalid, got {other:?}"),
    }
}

#[test]
fn invalid_numeric_and_duration_values_are_errors() {
    assert!(matches!(
        parse_flags(&args(&["-remoteWrite.queues=abc"])),
        Err(FlagError::Invalid(_))
    ));
    assert!(matches!(
        parse_flags(&args(&["-remoteWrite.flushInterval=notaduration"])),
        Err(FlagError::Invalid(_))
    ));
    assert!(matches!(
        parse_flags(&args(&["-remoteWrite.retryMinInterval=-5s"])),
        Err(FlagError::Invalid(_))
    ));
    assert!(matches!(
        parse_flags(&args(&["-remoteWrite.maxDiskUsagePerURL=abc"])),
        Err(FlagError::Invalid(_))
    ));
}

#[test]
fn positional_argument_is_an_error() {
    assert!(parse_flags(&args(&["serve"])).is_err());
}
