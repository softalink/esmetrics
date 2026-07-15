//! `DockerDiscovery` tests: config-build defaults plus (on Unix) an
//! end-to-end poll against a stub Docker API served over a Unix socket.

use super::*;

#[test]
fn build_docker_sd_config_applies_defaults() {
    let raw = RawDockerSdConfig {
        host: "unix:///var/run/docker.sock".to_string(),
        ..RawDockerSdConfig::default()
    };
    let cfg = build_docker_sd_config(raw);
    assert_eq!(cfg.host, "unix:///var/run/docker.sock");
    assert_eq!(cfg.port, DEFAULT_PORT);
    assert_eq!(cfg.host_networking_host, DEFAULT_HOST_NETWORKING_HOST);
    assert!(cfg.match_first_network);
    assert_eq!(cfg.refresh_interval, DEFAULT_REFRESH_INTERVAL);
}

#[test]
fn new_rejects_missing_host() {
    assert!(DockerDiscovery::new(&DockerSdConfig::default(), "job").is_err());
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

    const NETWORKS: &str = r#"[
      { "Name": "bridge", "Id": "netid1", "Scope": "local",
        "Internal": false, "Ingress": false, "Labels": {} }
    ]"#;

    const CONTAINERS: &str = r#"[
      { "Id": "cid1", "Names": ["/crow"],
        "Ports": [{ "IP": "0.0.0.0", "PrivatePort": 8080, "PublicPort": 18080, "Type": "tcp" }],
        "Labels": { "com.example.role": "web" },
        "HostConfig": { "NetworkMode": "bridge" },
        "NetworkSettings": { "Networks": {
          "bridge": { "NetworkID": "netid1", "IPAddress": "172.17.0.2" } } } }
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
        let body = if path.starts_with("/networks") {
            NETWORKS
        } else if path.starts_with("/containers/json") {
            CONTAINERS
        } else {
            "[]"
        };
        let _ = conn.write_all(chunked_response(body).as_bytes());
    }

    #[test]
    fn discovers_container_targets_over_unix_socket() {
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let path = std::env::temp_dir().join(format!(
            "esmagent-docker-sd-{}-{}.sock",
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

        let cfg = DockerSdConfig {
            host: format!("unix://{}", path.display()),
            refresh_interval: Duration::from_millis(20),
            ..DockerSdConfig::default()
        };
        let mut disco = DockerDiscovery::new(&cfg, "job").expect("new");

        // Bounded poll until the container target appears.
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut groups = disco.poll();
        while groups.is_empty() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
            groups = disco.poll();
        }

        assert_eq!(groups.len(), 1, "expected one target group");
        let g = &groups[0];
        assert_eq!(g.targets, vec!["172.17.0.2:8080".to_string()]);
        assert_eq!(g.source, "job/docker");
        assert_eq!(g.labels["__meta_docker_container_name"], "/crow");
        assert_eq!(g.labels["__meta_docker_network_name"], "bridge");
        assert_eq!(g.labels["__meta_docker_port_private"], "8080");
        assert_eq!(g.labels["__meta_docker_port_public"], "18080");
        assert_eq!(
            g.labels["__meta_docker_container_label_com_example_role"],
            "web"
        );

        // Clean stop: dropping the discovery joins its refresh thread.
        drop(disco);
        stop.store(true, Ordering::SeqCst);
        server.join().unwrap();
        let _ = std::fs::remove_file(&path);
    }
}
