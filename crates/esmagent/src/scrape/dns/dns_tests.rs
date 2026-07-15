//! Integration tests for `dns_sd` discovery: the A-record path (getaddrinfo
//! on `localhost`), the SRV/MX path against an in-process stub DNS server
//! (pointed at via the `nameserver` override), the [`DnsDiscovery`] lifecycle,
//! and [`build_dns_sd_config`] validation.

use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use super::labels::{
    META_DNS_MX_RECORD_TARGET, META_DNS_NAME, META_DNS_SRV_RECORD_PORT, META_DNS_SRV_RECORD_TARGET,
};
use super::wire::{build_test_response, QTYPE_MX, QTYPE_SRV};
use super::*;

/// An in-process stub DNS server: a UDP socket that answers every query with a
/// canned SRV or MX response echoing the request's transaction id. Stops (and
/// joins) its thread on `Drop`.
struct StubDns {
    addr: String,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl StubDns {
    fn start(qtype: u16, target: &'static str, port: u16) -> StubDns {
        let sock = UdpSocket::bind("127.0.0.1:0").expect("bind stub dns");
        sock.set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let addr = sock.local_addr().unwrap().to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            let mut buf = [0u8; 512];
            while !stop_thread.load(Ordering::SeqCst) {
                match sock.recv_from(&mut buf) {
                    Ok((n, from)) if n >= 2 => {
                        let id = u16::from_be_bytes([buf[0], buf[1]]);
                        let resp = build_test_response(id, qtype, target, port);
                        let _ = sock.send_to(&resp, from);
                    }
                    Ok(_) => {}
                    Err(_) => {} // read timeout — loop and re-check stop.
                }
            }
        });
        StubDns {
            addr,
            stop,
            handle: Some(handle),
        }
    }
}

impl Drop for StubDns {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

#[test]
fn resolves_srv_against_stub_dns() {
    let stub = StubDns::start(QTYPE_SRV, "sip.example.com", 5060);
    let cfg = DnsSdConfig {
        names: vec!["_svc._tcp.svc.local".to_string()],
        record_type: DnsRecordType::Srv,
        port: None,
        nameserver: Some(stub.addr.clone()),
        ..DnsSdConfig::default()
    };
    let groups = resolve::resolve_target_groups(&cfg, "job/dns").unwrap();
    assert_eq!(groups.len(), 1);
    let g = &groups[0];
    // SRV carries its own port (5060 from the record), not a config port.
    assert_eq!(g.targets, vec!["sip.example.com:5060".to_string()]);
    assert_eq!(g.labels[META_DNS_NAME], "_svc._tcp.svc.local");
    assert_eq!(g.labels[META_DNS_SRV_RECORD_TARGET], "sip.example.com");
    assert_eq!(g.labels[META_DNS_SRV_RECORD_PORT], "5060");
    assert_eq!(g.source, "job/dns");
}

#[test]
fn resolves_mx_against_stub_dns() {
    let stub = StubDns::start(QTYPE_MX, "mail.example.com", 0);
    let cfg = DnsSdConfig {
        names: vec!["example.com".to_string()],
        record_type: DnsRecordType::Mx,
        port: Some(2525),
        nameserver: Some(stub.addr.clone()),
        ..DnsSdConfig::default()
    };
    let groups = resolve::resolve_target_groups(&cfg, "job/dns").unwrap();
    assert_eq!(groups.len(), 1);
    let g = &groups[0];
    // MX carries no port — the configured port (2525) is used.
    assert_eq!(g.targets, vec!["mail.example.com:2525".to_string()]);
    assert_eq!(g.labels[META_DNS_NAME], "example.com");
    assert_eq!(g.labels[META_DNS_MX_RECORD_TARGET], "mail.example.com");
    assert!(!g.labels.contains_key(META_DNS_SRV_RECORD_TARGET));
}

#[test]
fn discovery_resolves_a_record_for_localhost() {
    let cfg = DnsSdConfig {
        names: vec!["localhost".to_string()],
        record_type: DnsRecordType::A,
        port: Some(9100),
        ..DnsSdConfig::default()
    };
    let mut d = DnsDiscovery::new(&cfg, "node").expect("discovery new");

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut found = false;
    while Instant::now() < deadline {
        let groups = d.poll();
        if groups
            .iter()
            .any(|g| g.targets == vec!["127.0.0.1:9100".to_string()])
        {
            found = true;
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        found,
        "expected localhost A record 127.0.0.1:9100 to appear"
    );
    // Clean stop via Drop.
    drop(d);
}

#[test]
fn discovery_resolves_srv_against_stub_dns() {
    let stub = StubDns::start(QTYPE_SRV, "sip.example.com", 5060);
    let cfg = DnsSdConfig {
        names: vec!["_svc._tcp.svc.local".to_string()],
        record_type: DnsRecordType::Srv,
        port: None,
        nameserver: Some(stub.addr.clone()),
        refresh_interval: Duration::from_millis(50),
    };
    let mut d = DnsDiscovery::new(&cfg, "node").expect("discovery new");

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut found = false;
    while Instant::now() < deadline {
        if d.poll()
            .iter()
            .any(|g| g.targets == vec!["sip.example.com:5060".to_string()])
        {
            found = true;
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(found, "expected SRV target to appear via stub DNS");
    drop(d);
}

#[test]
fn new_rejects_empty_names() {
    let cfg = DnsSdConfig {
        names: vec![],
        record_type: DnsRecordType::A,
        port: Some(80),
        ..DnsSdConfig::default()
    };
    assert!(DnsDiscovery::new(&cfg, "job").is_err());
}

#[test]
fn new_rejects_a_record_without_port() {
    let cfg = DnsSdConfig {
        names: vec!["h".to_string()],
        record_type: DnsRecordType::A,
        port: None,
        ..DnsSdConfig::default()
    };
    assert!(DnsDiscovery::new(&cfg, "job").is_err());
}

#[test]
fn build_defaults_type_to_srv() {
    let cfg = build_dns_sd_config(RawDnsSdConfig {
        names: vec!["_svc._tcp.q".to_string()],
        record_type: None,
        port: None,
    })
    .unwrap();
    assert_eq!(cfg.record_type, DnsRecordType::Srv);
    assert_eq!(cfg.refresh_interval, DEFAULT_REFRESH_INTERVAL);
}

#[test]
fn build_parses_each_type_case_insensitively() {
    for (raw, want) in [
        ("srv", DnsRecordType::Srv),
        ("A", DnsRecordType::A),
        ("aaaa", DnsRecordType::Aaaa),
        ("Mx", DnsRecordType::Mx),
    ] {
        let cfg = build_dns_sd_config(RawDnsSdConfig {
            names: vec!["h".to_string()],
            record_type: Some(raw.to_string()),
            port: Some(80),
        })
        .unwrap();
        assert_eq!(cfg.record_type, want, "type {raw}");
    }
}

#[test]
fn build_rejects_empty_names() {
    let err = build_dns_sd_config(RawDnsSdConfig {
        names: vec![],
        record_type: Some("SRV".to_string()),
        port: None,
    })
    .unwrap_err();
    assert!(err.msg.contains("names"), "{}", err.msg);
}

#[test]
fn build_rejects_bad_type() {
    let err = build_dns_sd_config(RawDnsSdConfig {
        names: vec!["h".to_string()],
        record_type: Some("CNAME".to_string()),
        port: Some(80),
    })
    .unwrap_err();
    assert!(err.msg.contains("type"), "{}", err.msg);
}

#[test]
fn build_rejects_a_aaaa_without_port() {
    for typ in ["A", "AAAA"] {
        let err = build_dns_sd_config(RawDnsSdConfig {
            names: vec!["h".to_string()],
            record_type: Some(typ.to_string()),
            port: None,
        })
        .unwrap_err();
        assert!(err.msg.contains("port"), "type {typ}: {}", err.msg);
    }
}

#[test]
fn build_allows_mx_without_port() {
    // MX no longer requires a port — upstream getMXAddrLabels defaults it to 25.
    let cfg = build_dns_sd_config(RawDnsSdConfig {
        names: vec!["example.com".to_string()],
        record_type: Some("MX".to_string()),
        port: None,
    })
    .unwrap();
    assert_eq!(cfg.record_type, DnsRecordType::Mx);
    assert_eq!(cfg.port, None);
}

#[test]
fn resolves_mx_without_port_defaults_to_25() {
    let stub = StubDns::start(QTYPE_MX, "mail.example.com", 0);
    let cfg = DnsSdConfig {
        names: vec!["example.com".to_string()],
        record_type: DnsRecordType::Mx,
        port: None, // no configured port -> upstream default of 25
        nameserver: Some(stub.addr.clone()),
        ..DnsSdConfig::default()
    };
    let groups = resolve::resolve_target_groups(&cfg, "job/dns").unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].targets, vec!["mail.example.com:25".to_string()]);
    assert_eq!(
        groups[0].labels[META_DNS_MX_RECORD_TARGET],
        "mail.example.com"
    );
}

#[test]
fn resolves_mx_with_port_uses_configured_port() {
    let stub = StubDns::start(QTYPE_MX, "mail.example.com", 0);
    let cfg = DnsSdConfig {
        names: vec!["example.com".to_string()],
        record_type: DnsRecordType::Mx,
        port: Some(2525), // explicit port wins over the default
        nameserver: Some(stub.addr.clone()),
        ..DnsSdConfig::default()
    };
    let groups = resolve::resolve_target_groups(&cfg, "job/dns").unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].targets, vec!["mail.example.com:2525".to_string()]);
}

#[test]
fn build_allows_srv_without_port() {
    let cfg = build_dns_sd_config(RawDnsSdConfig {
        names: vec!["_svc._tcp.q".to_string()],
        record_type: Some("SRV".to_string()),
        port: None,
    })
    .unwrap();
    assert_eq!(cfg.port, None);
}
