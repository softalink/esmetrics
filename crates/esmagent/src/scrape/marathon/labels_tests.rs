//! Label-builder tests for [`super`], ported from upstream
//! `marathon_test.go`'s `TestGetAppLabels` (exact fixture + expected
//! `__meta_marathon_*` set) plus the `app_test.go` null-tolerance fixture.

use super::*;

/// The exact `/v2/apps` fixture from `marathon_test.go`.
const APPS_JSON: &str = r#"{
    "apps": [
        {
            "id": "/app-test",
            "tasks": [
                {"id": "app-test.b44e0f85-a586-11ef-b308-02429c08177d", "host": "pre2",
                 "ports": [20651], "ipAddresses": [{"ipAddress": "172.17.0.12", "protocol": "IPv4"}]},
                {"id": "app-test.7cbfcce5-a586-11ef-b308-02429c08177d", "host": "pre3",
                 "ports": [20681], "ipAddresses": [{"ipAddress": "172.17.0.19", "protocol": "IPv4"}]},
                {"id": "app-test.b0a7c3d4-a586-11ef-b308-02429c08177d", "host": "pre1",
                 "ports": [20337], "ipAddresses": [{"ipAddress": "172.17.0.13", "protocol": "IPv4"}]},
                {"id": "app-test.7c26c12c-a586-11ef-b308-02429c08177d", "host": "pre2",
                 "ports": [20668], "ipAddresses": [{"ipAddress": "172.17.0.9", "protocol": "IPv4"}]}
            ],
            "tasksRunning": 4,
            "labels": {
                "HAPROXY_0_HTTP_BACKEND_PROXYPASS_GLUE": "  reqirep  \"^([^ :]*)\\ {proxypath}/?(.*)\" \"\\1\\ /\\2\"\n",
                "HAPROXY_0_HTTP_BACKEND_PROXYPASS_PATH": "/app-test",
                "HAPROXY_0_PATH": "-i /app-test",
                "HAPROXY_0_VHOST": "pre1,pre2,pre3,pre4,pre1:9000,pre",
                "HAPROXY_GROUP": "local",
                "micrometer_prometheus": "/actuator/prometheus"
            },
            "container": {
                "docker": {"image": "docker.local/app-test:1.0.0", "portMappings": null},
                "portMappings": [
                    {"labels": {"portMappingsLabel1": "portMappingsValue1"},
                     "containerPort": 8080, "hostPort": 0, "servicePort": 11002}
                ]
            },
            "portDefinitions": [],
            "networks": [{"name": "", "mode": "container/bridge"}],
            "requirePorts": false
        },
        {
            "id": "/app-test-for-port-definition",
            "tasks": [
                {"id": "app-test.b44e0f85-a586-11ef-b308-02429c08177d", "host": "pre2",
                 "ports": [20651], "ipAddresses": [{"ipAddress": "172.17.0.12", "protocol": "IPv4"}]}
            ],
            "tasksRunning": 1,
            "labels": {
                "HAPROXY_0_HTTP_BACKEND_PROXYPASS_GLUE": "  reqirep  \"^([^ :]*)\\ {proxypath}/?(.*)\" \"\\1\\ /\\2\"\n",
                "HAPROXY_0_HTTP_BACKEND_PROXYPASS_PATH": "/app-test",
                "HAPROXY_0_PATH": "-i /app-test",
                "HAPROXY_0_VHOST": "pre1,pre2,pre3,pre4,pre1:9000,pre",
                "HAPROXY_GROUP": "local",
                "micrometer_prometheus": "/actuator/prometheus"
            },
            "container": {
                "docker": {"image": "docker.local/app-test:1.0.0", "portMappings": null},
                "portMappings": []
            },
            "portDefinitions": [
                {"port": 9091, "name": "prometheus", "labels": {"metrics": "/metrics"}}
            ],
            "networks": [{"name": "", "mode": "container/bridge"}],
            "requirePorts": false
        }
    ]
}"#;

const APP_LABELS: &[(&str, &str)] = &[
    (
        "__meta_marathon_app_label_HAPROXY_0_HTTP_BACKEND_PROXYPASS_GLUE",
        "  reqirep  \"^([^ :]*)\\ {proxypath}/?(.*)\" \"\\1\\ /\\2\"\n",
    ),
    (
        "__meta_marathon_app_label_HAPROXY_0_HTTP_BACKEND_PROXYPASS_PATH",
        "/app-test",
    ),
    ("__meta_marathon_app_label_HAPROXY_0_PATH", "-i /app-test"),
    (
        "__meta_marathon_app_label_HAPROXY_0_VHOST",
        "pre1,pre2,pre3,pre4,pre1:9000,pre",
    ),
    ("__meta_marathon_app_label_HAPROXY_GROUP", "local"),
    (
        "__meta_marathon_app_label_micrometer_prometheus",
        "/actuator/prometheus",
    ),
    ("__meta_marathon_image", "docker.local/app-test:1.0.0"),
    ("__meta_marathon_port_index", "0"),
];

#[test]
fn get_app_labels_matches_upstream() {
    let apps = parse_app_list(APPS_JSON.as_bytes()).unwrap();
    let groups = append_apps_labels(&apps, "job/marathon");
    assert_eq!(groups.len(), 5, "{groups:#?}");

    // The four `/app-test` tasks, keyed by their task id -> expected address.
    let expect_addr = [
        (
            "app-test.b44e0f85-a586-11ef-b308-02429c08177d",
            "pre2:20651",
        ),
        (
            "app-test.7cbfcce5-a586-11ef-b308-02429c08177d",
            "pre3:20681",
        ),
        (
            "app-test.b0a7c3d4-a586-11ef-b308-02429c08177d",
            "pre1:20337",
        ),
        (
            "app-test.7c26c12c-a586-11ef-b308-02429c08177d",
            "pre2:20668",
        ),
    ];
    for (task_id, addr) in expect_addr {
        let g = groups
            .iter()
            .find(|g| {
                g.labels.get("__meta_marathon_app").map(String::as_str) == Some("/app-test")
                    && g.labels.get("__meta_marathon_task").map(String::as_str) == Some(task_id)
            })
            .unwrap_or_else(|| panic!("missing group for task {task_id}"));
        assert_eq!(g.targets, vec![addr.to_string()]);
        assert_eq!(g.source, "job/marathon");
        assert!(!g.labels.contains_key("__address__"));
        for (k, v) in APP_LABELS {
            assert_eq!(g.labels.get(*k).map(String::as_str), Some(*v), "{k}");
        }
        assert_eq!(
            g.labels
                .get("__meta_marathon_port_mapping_label_portMappingsLabel1")
                .map(String::as_str),
            Some("portMappingsValue1")
        );
        // Port-mapping app must NOT carry a port-definition label.
        assert!(!g
            .labels
            .keys()
            .any(|k| k.starts_with("__meta_marathon_port_definition_label_")));
    }

    // The port-definition app: one task, address pre2:20651 (port read from the
    // task since requirePorts=false), a `__meta_marathon_port_definition_label_`
    // and no `__meta_marathon_port_mapping_label_`.
    let pd = groups
        .iter()
        .find(|g| {
            g.labels.get("__meta_marathon_app").map(String::as_str)
                == Some("/app-test-for-port-definition")
        })
        .expect("port-definition group");
    assert_eq!(pd.targets, vec!["pre2:20651".to_string()]);
    assert_eq!(
        pd.labels
            .get("__meta_marathon_port_definition_label_metrics")
            .map(String::as_str),
        Some("/metrics")
    );
    assert_eq!(
        pd.labels
            .get("__meta_marathon_port_index")
            .map(String::as_str),
        Some("0")
    );
    assert!(!pd
        .labels
        .keys()
        .any(|k| k.starts_with("__meta_marathon_port_mapping_label_")));
}

/// `container: null` and `portMappings: null` must parse (Go treats JSON
/// `null` as the zero value) — regression cover for [`null_default`], ported
/// from `app_test.go`'s fixture shape.
#[test]
fn parses_null_container_and_port_mappings() {
    let json = r#"{"apps":[{
        "id":"/myapp",
        "container": null,
        "portDefinitions":[{"labels":{"pdl1":"pdl1"},"port":1999}],
        "labels":{},
        "requirePorts": false
    }]}"#;
    let apps = parse_app_list(json.as_bytes()).unwrap();
    assert_eq!(apps.apps.len(), 1);
    let app = &apps.apps[0];
    assert_eq!(app.id, "/myapp");
    assert_eq!(app.port_definitions.len(), 1);
    assert_eq!(app.port_definitions[0].port, 1999);
    // No tasks -> no target groups, but building must not panic.
    assert!(append_app_target_labels(app, "src").is_empty());
}

/// Host-networked apps expose ports only on the task; the builder must adopt
/// them (upstream's `ports = t.Ports` fallback) and set `port_index`.
#[test]
fn host_networking_uses_task_ports() {
    let json = r#"{"apps":[{
        "id":"/host-net",
        "tasks":[{"id":"t1","host":"h1","ports":[9000,9001]}],
        "labels":{},
        "container":{"docker":{"image":"img"}},
        "networks":[{"mode":"host"}]
    }]}"#;
    let apps = parse_app_list(json.as_bytes()).unwrap();
    let groups = append_app_target_labels(&apps.apps[0], "src");
    assert_eq!(groups.len(), 2);
    assert_eq!(groups[0].targets, vec!["h1:9000".to_string()]);
    assert_eq!(groups[0].labels["__meta_marathon_port_index"], "0");
    assert_eq!(groups[1].targets, vec!["h1:9001".to_string()]);
    assert_eq!(groups[1].labels["__meta_marathon_port_index"], "1");
}

/// Container networking connects to the container port at the task's first
/// container IP (bracketed if IPv6).
#[test]
fn container_networking_uses_container_ip_and_port() {
    let json = r#"{"apps":[{
        "id":"/cnet",
        "tasks":[{"id":"t1","host":"ignored","ipAddresses":[{"ipAddress":"10.1.2.3","protocol":"IPv4"}]}],
        "labels":{},
        "container":{"docker":{"image":"img"},"portMappings":[{"containerPort":8080,"hostPort":0}]},
        "networks":[{"mode":"container"}]
    }]}"#;
    let apps = parse_app_list(json.as_bytes()).unwrap();
    let groups = append_app_target_labels(&apps.apps[0], "src");
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].targets, vec!["10.1.2.3:8080".to_string()]);
}
