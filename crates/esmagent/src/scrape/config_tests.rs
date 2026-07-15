//! Tests for [`super`] — split out per this crate's `#[path]`-sibling
//! convention (see `manager_tests.rs`) to keep `config.rs` under the
//! 800-line cap.

use super::*;

#[test]
fn parses_a_scrape_config() {
    let y = r#"
global:
  scrape_interval: 30s
  external_labels: { env: prod }
scrape_configs:
  - job_name: node
    metrics_path: /metrics
    scheme: http
    static_configs:
      - targets: ['h1:9100', 'h2:9100']
        labels: { team: infra }
    relabel_configs:
      - source_labels: [__address__]
        target_label: instance
        action: replace
"#;
    let c = parse_scrape_config(y).unwrap();
    assert_eq!(c.global.scrape_interval, Duration::from_secs(30));
    assert_eq!(c.global.external_labels["env"], "prod");
    assert_eq!(c.scrape_configs[0].job_name, "node");
    assert_eq!(
        c.scrape_configs[0].static_configs[0].targets,
        vec!["h1:9100".to_string(), "h2:9100".to_string()]
    );
    assert_eq!(c.scrape_configs[0].relabel_configs.len(), 1);
    validate(&c).unwrap();
}

#[test]
fn rejects_cloud_sd_and_dup_job() {
    // Every cloud SD family is now supported (CLOUD_SD_KEYS is empty), so a
    // genuinely-unknown key proves the reject-unknown-field path still fires.
    assert!(
        parse_scrape_config("scrape_configs:\n  - job_name: k\n    bogus_sd_configs: [{}]\n")
            .is_err()
    );
    let dup = "scrape_configs:\n  - job_name: a\n    static_configs: [{targets: [x]}]\n  - job_name: a\n    static_configs: [{targets: [y]}]\n";
    assert!(validate(&parse_scrape_config(dup).unwrap()).is_err());
}

#[test]
fn rejects_scrape_config_without_job_name() {
    // Absent job_name.
    let err =
        parse_scrape_config("scrape_configs:\n  - static_configs: [{targets: [x]}]\n").unwrap_err();
    assert!(err.msg.contains("job_name"), "{}", err.msg);
    // Explicitly-empty job_name.
    let err = parse_scrape_config(
        "scrape_configs:\n  - job_name: ''\n    static_configs: [{targets: [x]}]\n",
    )
    .unwrap_err();
    assert!(err.msg.contains("job_name"), "{}", err.msg);
}

#[test]
fn scheme_is_lowercased_before_validation() {
    // Mixed/upper-case scheme is lowercased at build time (matches Go's
    // strings.ToLower), so `HTTPS` is accepted and normalized to `https`.
    let c = parse_scrape_config(
        "scrape_configs:\n  - job_name: s\n    scheme: HTTPS\n    static_configs: [{targets: [x]}]\n",
    )
    .unwrap();
    assert_eq!(c.scrape_configs[0].scheme, "https");
    validate(&c).unwrap();

    let c = parse_scrape_config(
        "scrape_configs:\n  - job_name: s\n    scheme: Http\n    static_configs: [{targets: [x]}]\n",
    )
    .unwrap();
    assert_eq!(c.scrape_configs[0].scheme, "http");
    validate(&c).unwrap();

    // A genuinely unsupported scheme is still rejected by validate.
    let c = parse_scrape_config(
        "scrape_configs:\n  - job_name: s\n    scheme: ftp\n    static_configs: [{targets: [x]}]\n",
    )
    .unwrap();
    assert!(validate(&c).is_err());
}

#[test]
fn parses_ec2_sd_config_and_no_longer_rejects_it() {
    let y = r#"
scrape_configs:
  - job_name: ec2
    ec2_sd_configs:
      - region: us-east-1
        access_key: AKID
        secret_key: SECRET
        port: 9100
        filters:
          - name: instance-state-name
            values: [running]
"#;
    let c = parse_scrape_config(y).unwrap();
    let e = &c.scrape_configs[0].ec2_sd_configs[0];
    assert_eq!(e.region, "us-east-1");
    assert_eq!(e.access_key, "AKID");
    assert_eq!(e.secret_key.as_deref(), Some("SECRET"));
    assert_eq!(e.port, 9100);
    assert_eq!(e.filters[0].name, "instance-state-name");
    assert_eq!(e.filters[0].values, vec!["running".to_string()]);
    // Default port when unset is 80.
    let c2 = parse_scrape_config(
        "scrape_configs:\n  - job_name: e\n    ec2_sd_configs:\n      - region: us-east-1\n",
    )
    .unwrap();
    assert_eq!(c2.scrape_configs[0].ec2_sd_configs[0].port, 80);
    // Default refresh interval before wiring overrides it.
    assert_eq!(
        e.refresh_interval,
        crate::scrape::ec2::DEFAULT_REFRESH_INTERVAL
    );
    validate(&c).unwrap();
    // secret_key must not leak through Debug.
    let dbg = format!("{e:?}");
    assert!(!dbg.contains("SECRET"), "{dbg}");
}

#[test]
fn rejects_ec2_role_arn_as_deferred() {
    // role_arn (STS AssumeRole) is DEFERRED: rejected at build (parse) time.
    let err = parse_scrape_config(
        "scrape_configs:\n  - job_name: e\n    ec2_sd_configs:\n      - region: us-east-1\n        role_arn: arn:aws:iam::123:role/foo\n",
    )
    .unwrap_err();
    assert!(err.msg.contains("role_arn"), "{}", err.msg);
    assert!(err.msg.contains("deferred"), "{}", err.msg);
}

#[test]
fn parses_consul_sd_config_and_no_longer_rejects_it() {
    let y = r#"
scrape_configs:
  - job_name: consul
    consul_sd_configs:
      - server: 'consul:8500'
        services: [web]
        tags: [prod]
        token: t
"#;
    let c = parse_scrape_config(y).unwrap();
    let cs = &c.scrape_configs[0].consul_sd_configs[0];
    assert_eq!(cs.server, "consul:8500");
    assert_eq!(cs.services, vec!["web".to_string()]);
    assert_eq!(cs.tags, vec!["prod".to_string()]);
    assert_eq!(cs.token.as_deref(), Some("t"));
    // Default refresh interval before wiring overrides it.
    assert_eq!(
        cs.refresh_interval,
        crate::scrape::consul::DEFAULT_REFRESH_INTERVAL
    );
    validate(&c).unwrap();
    // Other cloud SD keys are still rejected.
    assert!(
        parse_scrape_config("scrape_configs:\n  - job_name: k\n    bogus_sd_configs: [{}]\n")
            .is_err()
    );
}

#[test]
fn parses_consulagent_sd_config_and_no_longer_rejects_it() {
    let y = r#"
scrape_configs:
  - job_name: consulagent
    consulagent_sd_configs:
      - server: 'localhost:8500'
        services: [web]
        token: t
        filter: 'Service == "web"'
"#;
    let c = parse_scrape_config(y).unwrap();
    let cs = &c.scrape_configs[0].consulagent_sd_configs[0];
    assert_eq!(cs.server, "localhost:8500");
    assert_eq!(cs.services, vec!["web".to_string()]);
    assert_eq!(cs.token.as_deref(), Some("t"));
    assert_eq!(cs.filter, "Service == \"web\"");
    // Default refresh interval before wiring overrides it.
    assert_eq!(
        cs.refresh_interval,
        crate::scrape::consulagent::DEFAULT_REFRESH_INTERVAL
    );
    validate(&c).unwrap();
    // Other cloud SD keys are still rejected.
    assert!(
        parse_scrape_config("scrape_configs:\n  - job_name: k\n    bogus_sd_configs: [{}]\n")
            .is_err()
    );
}

#[test]
fn parses_dns_sd_config_and_no_longer_rejects_it() {
    // SRV (default type) needs no port.
    let srv = r#"
scrape_configs:
  - job_name: dns-srv
    dns_sd_configs:
      - names: ['_svc._tcp.example.com']
"#;
    let c = parse_scrape_config(srv).unwrap();
    let ds = &c.scrape_configs[0].dns_sd_configs[0];
    assert_eq!(ds.names, vec!["_svc._tcp.example.com".to_string()]);
    assert_eq!(ds.record_type, crate::scrape::dns::DnsRecordType::Srv);
    assert_eq!(ds.port, None);
    assert_eq!(
        ds.refresh_interval,
        crate::scrape::dns::DEFAULT_REFRESH_INTERVAL
    );
    validate(&c).unwrap();

    // A/AAAA/MX parse with a port.
    for typ in ["A", "AAAA", "MX"] {
        let y = format!(
            "scrape_configs:\n  - job_name: d\n    dns_sd_configs:\n      - names: ['h']\n        type: {typ}\n        port: 9100\n"
        );
        let c = parse_scrape_config(&y).unwrap();
        assert_eq!(c.scrape_configs[0].dns_sd_configs[0].port, Some(9100));
    }

    // Empty names, a bad type, and A/AAAA/MX without a port all reject at parse.
    assert!(parse_scrape_config(
        "scrape_configs:\n  - job_name: d\n    dns_sd_configs:\n      - names: []\n"
    )
    .is_err());
    assert!(parse_scrape_config(
        "scrape_configs:\n  - job_name: d\n    dns_sd_configs:\n      - names: ['h']\n        type: CNAME\n        port: 80\n"
    )
    .is_err());
    assert!(parse_scrape_config(
        "scrape_configs:\n  - job_name: d\n    dns_sd_configs:\n      - names: ['h']\n        type: A\n"
    )
    .is_err());

    // dns_sd_configs is no longer a rejected cloud key; only a genuinely-unknown
    // SD key rejects now (every cloud SD family is supported).
    assert!(
        parse_scrape_config("scrape_configs:\n  - job_name: k\n    bogus_sd_configs: [{}]\n")
            .is_err()
    );
}

#[test]
fn parses_digitalocean_sd_config_and_no_longer_rejects_it() {
    let y = r#"
scrape_configs:
  - job_name: do
    digitalocean_sd_configs:
      - bearer_token: do-token
        port: 9100
"#;
    let c = parse_scrape_config(y).unwrap();
    let ds = &c.scrape_configs[0].digitalocean_sd_configs[0];
    assert_eq!(ds.port, 9100);
    assert_eq!(ds.auth.bearer.as_deref(), Some("do-token"));
    // Default refresh interval before wiring overrides it.
    assert_eq!(
        ds.refresh_interval,
        crate::scrape::digitalocean::DEFAULT_REFRESH_INTERVAL
    );
    validate(&c).unwrap();
    // bearer_token must not leak through Debug.
    let dbg = format!("{ds:?}");
    assert!(!dbg.contains("do-token"), "{dbg}");

    // Default port when unset is 80.
    let c2 = parse_scrape_config(
        "scrape_configs:\n  - job_name: d\n    digitalocean_sd_configs:\n      - bearer_token: t\n",
    )
    .unwrap();
    assert_eq!(c2.scrape_configs[0].digitalocean_sd_configs[0].port, 80);

    // digitalocean_sd_configs is no longer a rejected cloud key, but other
    // cloud SD keys still reject.
    assert!(
        parse_scrape_config("scrape_configs:\n  - job_name: k\n    bogus_sd_configs: [{}]\n")
            .is_err()
    );
}

#[test]
fn parses_docker_sd_config_and_no_longer_rejects_it() {
    let y = r#"
scrape_configs:
  - job_name: docker
    docker_sd_configs:
      - host: unix:///var/run/docker.sock
        port: 9100
        host_networking_host: node.local
        match_first_network: false
        bearer_token: docker-token
        filters:
          - name: label
            values: [prometheus]
"#;
    let c = parse_scrape_config(y).unwrap();
    let ds = &c.scrape_configs[0].docker_sd_configs[0];
    assert_eq!(ds.host, "unix:///var/run/docker.sock");
    assert_eq!(ds.port, 9100);
    assert_eq!(ds.host_networking_host, "node.local");
    assert!(!ds.match_first_network);
    assert_eq!(ds.auth.bearer.as_deref(), Some("docker-token"));
    assert_eq!(ds.filters.len(), 1);
    assert_eq!(ds.filters[0].name, "label");
    // Default refresh interval before wiring overrides it.
    assert_eq!(
        ds.refresh_interval,
        crate::scrape::docker::DEFAULT_REFRESH_INTERVAL
    );
    validate(&c).unwrap();
    // bearer_token must not leak through Debug.
    let dbg = format!("{ds:?}");
    assert!(!dbg.contains("docker-token"), "{dbg}");

    // Defaults when unset: port 80, host_networking_host localhost,
    // match_first_network true.
    let c2 = parse_scrape_config(
        "scrape_configs:\n  - job_name: d\n    docker_sd_configs:\n      - host: tcp://d:2375\n",
    )
    .unwrap();
    let ds2 = &c2.scrape_configs[0].docker_sd_configs[0];
    assert_eq!(ds2.port, 80);
    assert_eq!(ds2.host_networking_host, "localhost");
    assert!(ds2.match_first_network);

    // docker_sd_configs is no longer a rejected cloud key; only a
    // genuinely-unknown key rejects now.
    assert!(
        parse_scrape_config("scrape_configs:\n  - job_name: k\n    bogus_sd_configs: [{}]\n")
            .is_err()
    );
}

#[test]
fn parses_dockerswarm_sd_config_all_roles_and_no_longer_rejects_it() {
    // services role with filters, auth, and custom port.
    let y = r#"
scrape_configs:
  - job_name: swarm
    dockerswarm_sd_configs:
      - host: unix:///var/run/docker.sock
        role: services
        port: 9100
        bearer_token: swarm-token
        filters:
          - name: name
            values: [redis]
"#;
    let c = parse_scrape_config(y).unwrap();
    let ds = &c.scrape_configs[0].dockerswarm_sd_configs[0];
    assert_eq!(ds.host, "unix:///var/run/docker.sock");
    assert_eq!(ds.role, "services");
    assert_eq!(ds.port, 9100);
    assert_eq!(ds.auth.bearer.as_deref(), Some("swarm-token"));
    assert_eq!(ds.filters.len(), 1);
    assert_eq!(ds.filters[0].name, "name");
    assert_eq!(ds.filters[0].values, vec!["redis".to_string()]);
    assert_eq!(
        ds.refresh_interval,
        crate::scrape::dockerswarm::DEFAULT_REFRESH_INTERVAL
    );
    validate(&c).unwrap();
    // bearer_token must not leak through Debug.
    let dbg = format!("{ds:?}");
    assert!(!dbg.contains("swarm-token"), "{dbg}");

    // All three roles parse; port defaults to 80 when unset.
    for role in ["services", "tasks", "nodes"] {
        let yr = format!(
            "scrape_configs:\n  - job_name: s\n    dockerswarm_sd_configs:\n      - host: tcp://d:2375\n        role: {role}\n"
        );
        let cr = parse_scrape_config(&yr).unwrap();
        let dsr = &cr.scrape_configs[0].dockerswarm_sd_configs[0];
        assert_eq!(dsr.role, role);
        assert_eq!(dsr.port, 80);
        validate(&cr).unwrap();
    }
}

#[test]
fn parses_vultr_sd_config_and_no_longer_rejects_it() {
    let y = r#"
scrape_configs:
  - job_name: vultr
    vultr_sd_configs:
      - bearer_token: vultr-token
        port: 9100
"#;
    let c = parse_scrape_config(y).unwrap();
    let vs = &c.scrape_configs[0].vultr_sd_configs[0];
    assert_eq!(vs.port, 9100);
    assert_eq!(vs.auth.bearer.as_deref(), Some("vultr-token"));
    // Default refresh interval before wiring overrides it.
    assert_eq!(
        vs.refresh_interval,
        crate::scrape::vultr::DEFAULT_REFRESH_INTERVAL
    );
    validate(&c).unwrap();
    // bearer_token must not leak through Debug.
    let dbg = format!("{vs:?}");
    assert!(!dbg.contains("vultr-token"), "{dbg}");

    // Default port when unset is 80.
    let c2 = parse_scrape_config(
        "scrape_configs:\n  - job_name: v\n    vultr_sd_configs:\n      - bearer_token: t\n",
    )
    .unwrap();
    assert_eq!(c2.scrape_configs[0].vultr_sd_configs[0].port, 80);

    // vultr_sd_configs is no longer a rejected cloud key, but other cloud SD
    // keys still reject.
    assert!(
        parse_scrape_config("scrape_configs:\n  - job_name: k\n    bogus_sd_configs: [{}]\n")
            .is_err()
    );
}

#[test]
fn parses_hetzner_sd_config_both_roles_and_no_longer_rejects_it() {
    // hcloud role: bearer_token auth.
    let hcloud = r#"
scrape_configs:
  - job_name: hcloud
    hetzner_sd_configs:
      - role: hcloud
        bearer_token: hc-token
        port: 9100
"#;
    let c = parse_scrape_config(hcloud).unwrap();
    let hs = &c.scrape_configs[0].hetzner_sd_configs[0];
    assert_eq!(hs.role, "hcloud");
    assert_eq!(hs.port, 9100);
    assert_eq!(hs.auth.bearer.as_deref(), Some("hc-token"));
    assert_eq!(
        hs.refresh_interval,
        crate::scrape::hetzner::DEFAULT_REFRESH_INTERVAL
    );
    validate(&c).unwrap();
    // bearer_token must not leak through Debug.
    assert!(!format!("{hs:?}").contains("hc-token"), "{hs:?}");

    // robot role: basic_auth, default port 80.
    let robot = r#"
scrape_configs:
  - job_name: robot
    hetzner_sd_configs:
      - role: robot
        basic_auth:
          username: rob
          password: rob-secret
"#;
    let c2 = parse_scrape_config(robot).unwrap();
    let rs = &c2.scrape_configs[0].hetzner_sd_configs[0];
    assert_eq!(rs.role, "robot");
    assert_eq!(rs.port, 80);
    assert_eq!(
        rs.auth.basic,
        Some(("rob".to_string(), "rob-secret".to_string()))
    );
    // password must not leak through Debug.
    assert!(!format!("{rs:?}").contains("rob-secret"), "{rs:?}");

    // An invalid role is rejected at parse time.
    let err = parse_scrape_config(
        "scrape_configs:\n  - job_name: h\n    hetzner_sd_configs:\n      - role: bogus\n",
    )
    .unwrap_err();
    assert!(err.msg.contains("role"), "{}", err.msg);

    // hetzner_sd_configs is no longer a rejected cloud key, but other cloud SD
    // keys still reject.
    assert!(
        parse_scrape_config("scrape_configs:\n  - job_name: k\n    bogus_sd_configs: [{}]\n")
            .is_err()
    );
}

#[test]
fn parses_gce_sd_config_and_no_longer_rejects_it() {
    let y = r#"
scrape_configs:
  - job_name: gce
    gce_sd_configs:
      - project: my-project
        zone: [us-east1-b, us-east1-c]
        filter: 'status = RUNNING'
        port: 9100
        tag_separator: '|'
        bearer_token: gce-token
"#;
    let c = parse_scrape_config(y).unwrap();
    let g = &c.scrape_configs[0].gce_sd_configs[0];
    assert_eq!(g.project, "my-project");
    assert_eq!(
        g.zones,
        vec!["us-east1-b".to_string(), "us-east1-c".to_string()]
    );
    assert_eq!(g.filter, "status = RUNNING");
    assert_eq!(g.port, 9100);
    assert_eq!(g.tag_separator, "|");
    assert_eq!(g.bearer_token.as_deref(), Some("gce-token"));
    // Default refresh interval before wiring overrides it.
    assert_eq!(
        g.refresh_interval,
        crate::scrape::gce::DEFAULT_REFRESH_INTERVAL
    );
    validate(&c).unwrap();
    // bearer_token must not leak through Debug.
    let dbg = format!("{g:?}");
    assert!(!dbg.contains("gce-token"), "{dbg}");

    // A single scalar `zone` is accepted too, and port/tag_separator default.
    let c2 = parse_scrape_config(
        "scrape_configs:\n  - job_name: g\n    gce_sd_configs:\n      - project: p\n        zone: us-east1-b\n",
    )
    .unwrap();
    let g2 = &c2.scrape_configs[0].gce_sd_configs[0];
    assert_eq!(g2.zones, vec!["us-east1-b".to_string()]);
    assert_eq!(g2.port, 80);
    assert_eq!(g2.tag_separator, ",");

    // gce_sd_configs is no longer a rejected cloud key, but other cloud SD
    // keys still reject.
    assert!(
        parse_scrape_config("scrape_configs:\n  - job_name: k\n    bogus_sd_configs: [{}]\n")
            .is_err()
    );
}

#[test]
fn rejects_gce_credentials_file_as_deferred() {
    // The service-account JSON key file (credentials_file) is DEFERRED:
    // rejected at build (parse) time.
    let err = parse_scrape_config(
        "scrape_configs:\n  - job_name: g\n    gce_sd_configs:\n      - project: p\n        zone: z\n        credentials_file: /etc/gcp/key.json\n",
    )
    .unwrap_err();
    assert!(err.msg.contains("credentials_file"), "{}", err.msg);
    assert!(err.msg.contains("deferred"), "{}", err.msg);
}

#[test]
fn parses_yandexcloud_sd_config_and_no_longer_rejects_it() {
    let y = r#"
scrape_configs:
  - job_name: yandexcloud
    yandexcloud_sd_configs:
      - service: compute
        yandex_passport_oauth_token: oauth-secret
        folder_ids: [folder-a, folder-b]
        api_endpoint: https://api.cloud.yandex.net
"#;
    let c = parse_scrape_config(y).unwrap();
    let yc = &c.scrape_configs[0].yandexcloud_sd_configs[0];
    assert_eq!(yc.service, "compute");
    assert_eq!(
        yc.yandex_passport_oauth_token.as_deref(),
        Some("oauth-secret")
    );
    assert_eq!(
        yc.folder_ids,
        vec!["folder-a".to_string(), "folder-b".to_string()]
    );
    assert_eq!(
        yc.api_endpoint.as_deref(),
        Some("https://api.cloud.yandex.net")
    );
    // Default refresh interval before wiring overrides it.
    assert_eq!(
        yc.refresh_interval,
        crate::scrape::yandexcloud::DEFAULT_REFRESH_INTERVAL
    );
    validate(&c).unwrap();
    // The OAuth token must not leak through Debug.
    let dbg = format!("{yc:?}");
    assert!(!dbg.contains("oauth-secret"), "{dbg}");
}

#[test]
fn rejects_yandexcloud_non_compute_service() {
    let err = parse_scrape_config(
        "scrape_configs:\n  - job_name: y\n    yandexcloud_sd_configs:\n      - service: storage\n",
    )
    .unwrap_err();
    assert!(err.msg.contains("compute"), "{}", err.msg);
}

#[test]
fn rejects_yandexcloud_service_account_key_file_as_deferred() {
    // The service-account authorized-key JSON (JWT -> IAM exchange) is
    // DEFERRED: rejected at build (parse) time.
    let err = parse_scrape_config(
        "scrape_configs:\n  - job_name: y\n    yandexcloud_sd_configs:\n      - service: compute\n        service_account_key_file: /etc/yc/key.json\n",
    )
    .unwrap_err();
    assert!(err.msg.contains("service_account_key_file"), "{}", err.msg);
    assert!(err.msg.contains("deferred"), "{}", err.msg);
}
