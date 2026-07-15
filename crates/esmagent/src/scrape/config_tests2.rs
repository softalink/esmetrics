//! Tests for [`super`] — split out per this crate's `#[path]`-sibling
//! convention (see `manager_tests.rs`) to keep `config.rs` under the
//! 800-line cap.
//!
//! Continuation of `config_tests.rs`, split to keep both files under the
//! 800-line cap.

use super::*;

#[test]
fn parses_azure_sd_config_and_no_longer_rejects_it() {
    let y = r#"
scrape_configs:
  - job_name: azure
    azure_sd_configs:
      - subscription_id: sub-1
        tenant_id: tenant-1
        client_id: client-1
        client_secret: azure-secret
        resource_group: my-rg
        port: 9100
"#;
    let c = parse_scrape_config(y).unwrap();
    let a = &c.scrape_configs[0].azure_sd_configs[0];
    assert_eq!(a.subscription_id, "sub-1");
    assert_eq!(a.tenant_id, "tenant-1");
    assert_eq!(a.client_id, "client-1");
    assert_eq!(a.client_secret.as_deref(), Some("azure-secret"));
    assert_eq!(a.resource_group, "my-rg");
    assert_eq!(a.port, 9100);
    // Default refresh interval before wiring overrides it.
    assert_eq!(
        a.refresh_interval,
        crate::scrape::azure::DEFAULT_REFRESH_INTERVAL
    );
    validate(&c).unwrap();
    // client_secret must not leak through Debug.
    let dbg = format!("{a:?}");
    assert!(!dbg.contains("azure-secret"), "{dbg}");

    // ManagedIdentity needs no OAuth credentials; port defaults to 80.
    let c2 = parse_scrape_config(
        "scrape_configs:\n  - job_name: a\n    azure_sd_configs:\n      - subscription_id: sub\n        authentication_method: ManagedIdentity\n",
    )
    .unwrap();
    let a2 = &c2.scrape_configs[0].azure_sd_configs[0];
    assert_eq!(a2.port, 80);
    assert_eq!(a2.authentication_method, "ManagedIdentity");

    // azure_sd_configs is no longer a rejected cloud key, but other cloud SD
    // keys still reject.
    assert!(
        parse_scrape_config("scrape_configs:\n  - job_name: k\n    bogus_sd_configs: [{}]\n")
            .is_err()
    );
}

#[test]
fn rejects_bad_azure_authentication_method() {
    let err = parse_scrape_config(
        "scrape_configs:\n  - job_name: a\n    azure_sd_configs:\n      - subscription_id: sub\n        authentication_method: Kerberos\n",
    )
    .unwrap_err();
    assert!(err.msg.contains("authentication_method"), "{}", err.msg);
}

#[test]
fn rejects_azure_oauth_missing_credentials() {
    // OAuth (the default) requires tenant_id/client_id/client_secret.
    let err = parse_scrape_config(
        "scrape_configs:\n  - job_name: a\n    azure_sd_configs:\n      - subscription_id: sub\n",
    )
    .unwrap_err();
    assert!(err.msg.contains("OAuth"), "{}", err.msg);
}

#[test]
fn parses_nomad_sd_config_and_no_longer_rejects_it() {
    let y = r#"
scrape_configs:
  - job_name: nomad
    nomad_sd_configs:
      - server: 'nomad:4646'
        namespace: prod
        region: eu
        bearer_token: nomad-token
"#;
    let c = parse_scrape_config(y).unwrap();
    let ns = &c.scrape_configs[0].nomad_sd_configs[0];
    assert_eq!(ns.server, "nomad:4646");
    assert_eq!(ns.namespace.as_deref(), Some("prod"));
    assert_eq!(ns.region.as_deref(), Some("eu"));
    assert_eq!(ns.auth.bearer.as_deref(), Some("nomad-token"));
    // Default refresh interval before wiring overrides it.
    assert_eq!(
        ns.refresh_interval,
        crate::scrape::nomad::DEFAULT_REFRESH_INTERVAL
    );
    validate(&c).unwrap();
    // bearer_token must not leak through Debug.
    let dbg = format!("{ns:?}");
    assert!(!dbg.contains("nomad-token"), "{dbg}");

    // nomad_sd_configs is no longer a rejected cloud key, but other cloud SD
    // keys still reject.
    assert!(
        parse_scrape_config("scrape_configs:\n  - job_name: k\n    bogus_sd_configs: [{}]\n")
            .is_err()
    );
}

#[test]
fn parses_openstack_sd_config_both_roles_and_no_longer_rejects_it() {
    // instance role with password auth.
    let inst = r#"
scrape_configs:
  - job_name: os-inst
    openstack_sd_configs:
      - identity_endpoint: http://keystone:5000/v3
        username: admin
        password: secret-pass
        domain_name: default
        project_name: admin
        region: RegionOne
        role: instance
        port: 9100
        all_tenants: true
        availability: internal
"#;
    let c = parse_scrape_config(inst).unwrap();
    let o = &c.scrape_configs[0].openstack_sd_configs[0];
    assert_eq!(o.identity_endpoint, "http://keystone:5000/v3");
    assert_eq!(o.username, "admin");
    assert_eq!(o.password.as_deref(), Some("secret-pass"));
    assert_eq!(o.role, "instance");
    assert_eq!(o.port, 9100);
    assert!(o.all_tenants);
    assert_eq!(o.availability, "internal");
    assert_eq!(
        o.refresh_interval,
        crate::scrape::openstack::DEFAULT_REFRESH_INTERVAL
    );
    validate(&c).unwrap();
    // The password must not leak through Debug.
    let dbg = format!("{o:?}");
    assert!(!dbg.contains("secret-pass"), "{dbg}");

    // hypervisor role with application-credential auth; port defaults to 80.
    let hv = r#"
scrape_configs:
  - job_name: os-hv
    openstack_sd_configs:
      - identity_endpoint: http://keystone:5000/v3
        application_credential_id: cred-id
        application_credential_secret: cred-secret
        role: hypervisor
"#;
    let c2 = parse_scrape_config(hv).unwrap();
    let o2 = &c2.scrape_configs[0].openstack_sd_configs[0];
    assert_eq!(o2.role, "hypervisor");
    assert_eq!(o2.port, 80);
    assert_eq!(o2.application_credential_id, "cred-id");
    let dbg2 = format!("{o2:?}");
    assert!(!dbg2.contains("cred-secret"), "{dbg2}");

    // An invalid role is rejected at parse time.
    let err = parse_scrape_config(
        "scrape_configs:\n  - job_name: o\n    openstack_sd_configs:\n      - role: bogus\n",
    )
    .unwrap_err();
    assert!(err.msg.contains("role"), "{}", err.msg);

    // openstack_sd_configs is no longer a rejected cloud key, but other cloud
    // SD keys still reject.
    assert!(
        parse_scrape_config("scrape_configs:\n  - job_name: k\n    bogus_sd_configs: [{}]\n")
            .is_err()
    );
}

#[test]
fn parses_marathon_sd_config_and_no_longer_rejects_it() {
    let y = r#"
scrape_configs:
  - job_name: marathon
    marathon_sd_configs:
      - servers:
          - 'https://marathon1:8080'
          - 'https://marathon2:8080'
        bearer_token: marathon-token
"#;
    let c = parse_scrape_config(y).unwrap();
    let ms = &c.scrape_configs[0].marathon_sd_configs[0];
    assert_eq!(
        ms.servers,
        vec![
            "https://marathon1:8080".to_string(),
            "https://marathon2:8080".to_string()
        ]
    );
    assert_eq!(ms.auth.bearer.as_deref(), Some("marathon-token"));
    // Default refresh interval before wiring overrides it.
    assert_eq!(
        ms.refresh_interval,
        crate::scrape::marathon::DEFAULT_REFRESH_INTERVAL
    );
    validate(&c).unwrap();
    // bearer_token must not leak through Debug.
    let dbg = format!("{ms:?}");
    assert!(!dbg.contains("marathon-token"), "{dbg}");

    // marathon_sd_configs is no longer a rejected cloud key, but other cloud SD
    // keys (still deferred) reject.
    assert!(
        parse_scrape_config("scrape_configs:\n  - job_name: k\n    bogus_sd_configs: [{}]\n")
            .is_err()
    );
}

#[test]
fn parses_puppetdb_sd_config_and_no_longer_rejects_it() {
    let y = r#"
scrape_configs:
  - job_name: puppetdb
    puppetdb_sd_configs:
      - url: https://puppetdb.example.com
        query: 'resources { type = "Class" }'
        include_parameters: true
        port: 9100
        bearer_token: pdb-token
"#;
    let c = parse_scrape_config(y).unwrap();
    let ps = &c.scrape_configs[0].puppetdb_sd_configs[0];
    assert_eq!(ps.url, "https://puppetdb.example.com");
    assert_eq!(ps.query, r#"resources { type = "Class" }"#);
    assert!(ps.include_parameters);
    assert_eq!(ps.port, 9100);
    assert_eq!(ps.auth.bearer.as_deref(), Some("pdb-token"));
    // Default refresh interval before wiring overrides it.
    assert_eq!(
        ps.refresh_interval,
        crate::scrape::puppetdb::DEFAULT_REFRESH_INTERVAL
    );
    validate(&c).unwrap();
    // bearer_token must not leak through Debug.
    let dbg = format!("{ps:?}");
    assert!(!dbg.contains("pdb-token"), "{dbg}");

    // A missing `url` or `query` is rejected at parse time.
    assert!(parse_scrape_config(
        "scrape_configs:\n  - job_name: p\n    puppetdb_sd_configs:\n      - query: 'x'\n"
    )
    .is_err());
    assert!(parse_scrape_config(
        "scrape_configs:\n  - job_name: p\n    puppetdb_sd_configs:\n      - url: https://p.example\n"
    )
    .is_err());

    // puppetdb_sd_configs is no longer a rejected cloud key, but other cloud
    // SD keys still reject.
    assert!(
        parse_scrape_config("scrape_configs:\n  - job_name: k\n    bogus_sd_configs: [{}]\n")
            .is_err()
    );
}

#[test]
fn parses_kuma_sd_config_and_no_longer_rejects_it() {
    let y = r#"
scrape_configs:
  - job_name: kuma
    kuma_sd_configs:
      - server: https://kuma.example.com:5676/base
        client_id: my-agent
        bearer_token: kuma-token
"#;
    let c = parse_scrape_config(y).unwrap();
    let ks = &c.scrape_configs[0].kuma_sd_configs[0];
    assert_eq!(ks.server, "https://kuma.example.com:5676/base");
    assert_eq!(ks.client_id, "my-agent");
    assert_eq!(ks.auth.bearer.as_deref(), Some("kuma-token"));
    // Default refresh interval before wiring overrides it.
    assert_eq!(
        ks.refresh_interval,
        crate::scrape::kuma::DEFAULT_REFRESH_INTERVAL
    );
    validate(&c).unwrap();
    // bearer_token must not leak through Debug.
    let dbg = format!("{ks:?}");
    assert!(!dbg.contains("kuma-token"), "{dbg}");

    // A missing `server` is rejected at parse time.
    assert!(parse_scrape_config(
        "scrape_configs:\n  - job_name: k\n    kuma_sd_configs:\n      - client_id: x\n"
    )
    .is_err());

    // kuma_sd_configs is no longer a rejected cloud key, but still-deferred
    // cloud SD keys reject.
    assert!(
        parse_scrape_config("scrape_configs:\n  - job_name: k\n    bogus_sd_configs: [{}]\n")
            .is_err()
    );
}

#[test]
fn parses_ovhcloud_sd_config_both_services_and_no_longer_rejects_it() {
    // vps service.
    let vps = r#"
scrape_configs:
  - job_name: ovh-vps
    ovhcloud_sd_configs:
      - endpoint: ovh-eu
        application_key: app-key
        application_secret: app-secret
        consumer_key: cons-key
        service: vps
"#;
    let c = parse_scrape_config(vps).unwrap();
    let os = &c.scrape_configs[0].ovhcloud_sd_configs[0];
    assert_eq!(os.endpoint, "ovh-eu");
    assert_eq!(os.service, "vps");
    assert_eq!(os.application_key, "app-key");
    assert_eq!(os.application_secret, "app-secret");
    assert_eq!(os.consumer_key, "cons-key");
    assert_eq!(
        os.refresh_interval,
        crate::scrape::ovhcloud::DEFAULT_REFRESH_INTERVAL
    );
    validate(&c).unwrap();
    // Secrets must not leak through Debug.
    let dbg = format!("{os:?}");
    assert!(!dbg.contains("app-secret"), "{dbg}");
    assert!(!dbg.contains("cons-key"), "{dbg}");

    // dedicated_server service; endpoint defaults to ovh-eu when unset.
    let ded = r#"
scrape_configs:
  - job_name: ovh-ded
    ovhcloud_sd_configs:
      - application_key: k
        application_secret: s
        consumer_key: c
        service: dedicated_server
"#;
    let c2 = parse_scrape_config(ded).unwrap();
    let ds = &c2.scrape_configs[0].ovhcloud_sd_configs[0];
    assert_eq!(ds.service, "dedicated_server");
    assert_eq!(ds.endpoint, "ovh-eu");

    // An invalid service is rejected at parse time.
    let err = parse_scrape_config(
        "scrape_configs:\n  - job_name: o\n    ovhcloud_sd_configs:\n      - service: bogus\n",
    )
    .unwrap_err();
    assert!(err.msg.contains("service"), "{}", err.msg);

    // An invalid endpoint is rejected at parse time.
    let err = parse_scrape_config(
        "scrape_configs:\n  - job_name: o\n    ovhcloud_sd_configs:\n      - service: vps\n        endpoint: nope\n",
    )
    .unwrap_err();
    assert!(err.msg.contains("endpoint"), "{}", err.msg);

    // ovhcloud_sd_configs is no longer a rejected cloud key, but other cloud SD
    // keys still reject.
    assert!(
        parse_scrape_config("scrape_configs:\n  - job_name: k\n    bogus_sd_configs: [{}]\n")
            .is_err()
    );
}

#[test]
fn parses_eureka_sd_config_and_no_longer_rejects_it() {
    let y = r#"
scrape_configs:
  - job_name: eureka
    eureka_sd_configs:
      - server: 'https://eureka.example:8080/eureka/v2'
        bearer_token: eureka-token
"#;
    let c = parse_scrape_config(y).unwrap();
    let es = &c.scrape_configs[0].eureka_sd_configs[0];
    assert_eq!(es.server, "https://eureka.example:8080/eureka/v2");
    assert_eq!(es.auth.bearer.as_deref(), Some("eureka-token"));
    // Default refresh interval before wiring overrides it.
    assert_eq!(
        es.refresh_interval,
        crate::scrape::eureka::DEFAULT_REFRESH_INTERVAL
    );
    validate(&c).unwrap();
    // bearer_token must not leak through Debug.
    let dbg = format!("{es:?}");
    assert!(!dbg.contains("eureka-token"), "{dbg}");

    // eureka_sd_configs is no longer a rejected cloud key, but other cloud SD
    // keys (still deferred) reject.
    assert!(
        parse_scrape_config("scrape_configs:\n  - job_name: k\n    bogus_sd_configs: [{}]\n")
            .is_err()
    );
    assert!(
        parse_scrape_config("scrape_configs:\n  - job_name: k\n    bogus_sd_configs: [{}]\n")
            .is_err()
    );
}

#[test]
fn parses_and_validates_kubernetes_sd() {
    let y = r#"
scrape_configs:
  - job_name: k8s
    kubernetes_sd_configs:
      - role: pod
        namespaces: { names: [default, kube-system] }
        selectors:
          - { role: pod, label: "app=web" }
"#;
    let c = parse_scrape_config(y).unwrap();
    let k = &c.scrape_configs[0].kubernetes_sd_configs[0];
    assert_eq!(k.role, "pod");
    assert_eq!(
        k.namespaces.names,
        vec!["default".to_string(), "kube-system".to_string()]
    );
    assert_eq!(k.selectors[0].label.as_deref(), Some("app=web"));
    validate(&c).unwrap();
}

#[test]
fn accepts_endpoints_roles_and_normalizes_alias() {
    let y = "scrape_configs:\n  - job_name: j\n    kubernetes_sd_configs: [{role: endpoints}, {role: endpointslices}]\n";
    let c = parse_scrape_config(y).unwrap();
    assert_eq!(
        c.scrape_configs[0].kubernetes_sd_configs[0].role,
        "endpoints"
    );
    assert_eq!(
        c.scrape_configs[0].kubernetes_sd_configs[1].role,
        "endpointslice"
    );
    validate(&c).unwrap();
    let bad = "scrape_configs:\n  - job_name: j\n    kubernetes_sd_configs: [{role: bogus}]\n";
    let err = validate(&parse_scrape_config(bad).unwrap()).unwrap_err();
    assert!(err.msg.contains("endpointslice"), "{}", err.msg);
    // kubeconfig_file alone now validates — the file is read/parsed at
    // discovery-resolution time, not by validate().
    let kc = "scrape_configs:\n  - job_name: j\n    kubernetes_sd_configs: [{role: pod, kubeconfig_file: /x}]\n";
    assert!(validate(&parse_scrape_config(kc).unwrap()).is_ok());
}

#[test]
fn parses_and_validates_k8s_oauth2() {
    let y = "scrape_configs:\n  - job_name: j\n    kubernetes_sd_configs: [{role: pod, api_server: 'https://api:6443', oauth2: {client_id: id, client_secret: sec, token_url: 'https://idp/token', scopes: [a, b], endpoint_params: {resource: r1}}}]\n";
    let c = parse_scrape_config(y).unwrap();
    let k = &c.scrape_configs[0].kubernetes_sd_configs[0];
    let o = k.oauth2.as_ref().unwrap();
    assert_eq!(o.client_id, "id");
    assert_eq!(o.client_secret.as_deref(), Some("sec"));
    assert_eq!(o.token_url, "https://idp/token");
    assert_eq!(o.scopes, vec!["a".to_string(), "b".to_string()]);
    assert_eq!(
        o.endpoint_params.get("resource").map(String::as_str),
        Some("r1")
    );
    validate(&c).unwrap();

    // A malformed oauth2 (missing token_url) fails validate with upstream
    // wording, prefixed by the job name.
    let bad = "scrape_configs:\n  - job_name: j\n    kubernetes_sd_configs: [{role: pod, oauth2: {client_id: id, client_secret: sec}}]\n";
    let err = validate(&parse_scrape_config(bad).unwrap()).unwrap_err();
    assert!(err.msg.contains("token_url cannot be empty"), "{}", err.msg);
    assert!(err.msg.contains("job_name \"j\""), "{}", err.msg);
}

#[test]
fn parses_k8s_standalone_proxy_url() {
    let y = "scrape_configs:\n  - job_name: j\n    kubernetes_sd_configs: [{role: pod, proxy_url: 'http://proxy:3128'}]\n";
    let c = parse_scrape_config(y).unwrap();
    let k = &c.scrape_configs[0].kubernetes_sd_configs[0];
    assert_eq!(k.proxy_url.as_deref(), Some("http://proxy:3128"));
    // No proxy_url -> None.
    let y2 = "scrape_configs:\n  - job_name: j\n    kubernetes_sd_configs: [{role: pod}]\n";
    let c2 = parse_scrape_config(y2).unwrap();
    assert!(c2.scrape_configs[0].kubernetes_sd_configs[0]
        .proxy_url
        .is_none());
}

#[test]
fn rejects_k8s_api_server_and_kubeconfig_file_together() {
    let y = "scrape_configs:\n  - job_name: j\n    kubernetes_sd_configs: [{role: pod, api_server: 'https://k8s:6443', kubeconfig_file: /x}]\n";
    let err = validate(&parse_scrape_config(y).unwrap()).unwrap_err();
    assert!(err.msg.contains("api_server"), "{}", err.msg);
    assert!(err.msg.contains("kubeconfig_file"), "{}", err.msg);
}
