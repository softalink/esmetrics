//! `DockerswarmDiscovery` tests: config-build defaults, bad-config rejection,
//! plus (on Unix) an end-to-end `nodes`-role poll against a stub Docker Swarm
//! API served over a Unix socket.

use super::*;

#[test]
fn build_dockerswarm_sd_config_applies_defaults() {
    let raw = RawDockerswarmSdConfig {
        host: "unix:///var/run/docker.sock".to_string(),
        role: "nodes".to_string(),
        ..RawDockerswarmSdConfig::default()
    };
    let cfg = build_dockerswarm_sd_config(raw);
    assert_eq!(cfg.host, "unix:///var/run/docker.sock");
    assert_eq!(cfg.role, "nodes");
    assert_eq!(cfg.port, DEFAULT_PORT);
    assert_eq!(cfg.refresh_interval, DEFAULT_REFRESH_INTERVAL);
}

#[test]
fn new_rejects_missing_host() {
    let cfg = DockerswarmSdConfig {
        role: "nodes".to_string(),
        ..DockerswarmSdConfig::default()
    };
    assert!(DockerswarmDiscovery::new(&cfg, "job").is_err());
}

#[test]
fn new_rejects_invalid_role() {
    let cfg = DockerswarmSdConfig {
        host: "tcp://dockerd:2375".to_string(),
        role: "bogus".to_string(),
        ..DockerswarmSdConfig::default()
    };
    assert!(DockerswarmDiscovery::new(&cfg, "job").is_err());
}

#[cfg(unix)]
mod unix_e2e {
    use super::*;
    use std::io::{Read, Write};
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    static COUNTER: AtomicU32 = AtomicU32::new(0);

    const NODES: &str = r#"[
      { "ID": "qauwmifceyvqs0sipvzu8oslu",
        "Spec": { "Role": "manager", "Availability": "active" },
        "Description": { "Hostname": "ip-172-31-40-97",
          "Platform": { "Architecture": "x86_64", "OS": "linux" },
          "Engine": { "EngineVersion": "19.03.11" } },
        "Status": { "State": "ready", "Addr": "172.31.40.97" } }
    ]"#;

    fn chunked_response(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
             Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n\
             {:x}\r\n{}\r\n0\r\n\r\n",
            body.len(),
            body
        )
    }

    fn serve(mut conn: UnixStream) {
        let mut buf = [0u8; 2048];
        let n = conn.read(&mut buf).unwrap_or(0);
        let req = String::from_utf8_lossy(&buf[..n]);
        let path = req
            .lines()
            .next()
            .unwrap_or("")
            .split(' ')
            .nth(1)
            .unwrap_or("");
        let body = if path.starts_with("/nodes") {
            NODES
        } else {
            "[]"
        };
        let _ = conn.write_all(chunked_response(body).as_bytes());
    }

    #[test]
    fn discovers_node_targets_over_unix_socket() {
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let path = std::env::temp_dir().join(format!(
            "esmagent-dockerswarm-sd-{}-{}.sock",
            std::process::id(),
            n
        ));
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path).expect("bind");
        listener.set_nonblocking(true).unwrap();

        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let server_stop = Arc::clone(&stop);
        let server = thread::spawn(move || loop {
            if server_stop.load(Ordering::SeqCst) {
                return;
            }
            match listener.accept() {
                Ok((conn, _)) => {
                    conn.set_nonblocking(false).unwrap();
                    serve(conn);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(_) => return,
            }
        });

        let cfg = DockerswarmSdConfig {
            host: format!("unix://{}", path.display()),
            role: "nodes".to_string(),
            port: 9100,
            refresh_interval: Duration::from_millis(20),
            ..DockerswarmSdConfig::default()
        };
        let mut disco = DockerswarmDiscovery::new(&cfg, "job").expect("new");

        // Bounded poll until the node target appears.
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut groups = disco.poll();
        while groups.is_empty() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
            groups = disco.poll();
        }

        assert_eq!(groups.len(), 1, "expected one node target group");
        let g = &groups[0];
        assert_eq!(g.targets, vec!["172.31.40.97:9100".to_string()]);
        assert_eq!(g.source, "job/dockerswarm");
        assert_eq!(
            g.labels["__meta_dockerswarm_node_id"],
            "qauwmifceyvqs0sipvzu8oslu"
        );
        assert_eq!(
            g.labels["__meta_dockerswarm_node_hostname"],
            "ip-172-31-40-97"
        );
        assert_eq!(g.labels["__meta_dockerswarm_node_role"], "manager");
        assert_eq!(g.labels["__meta_dockerswarm_node_status"], "ready");
        assert!(!g.labels.contains_key("__address__"));

        // Clean stop: dropping the discovery joins its refresh thread.
        drop(disco);
        stop.store(true, Ordering::SeqCst);
        server.join().unwrap();
        let _ = std::fs::remove_file(&path);
    }
}
