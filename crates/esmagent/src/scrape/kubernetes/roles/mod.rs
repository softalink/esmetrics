//! Per-role `K8sObject -> TargetGroup` builders.
//!
//! Each role (node, pod, service, ingress) gets its own submodule with a
//! `<role>_target_groups(&<Role>) -> Vec<TargetGroup>` function that ports
//! the corresponding upstream `getTargetLabels` from vmagent's
//! `lib/promscrape/discoveryutils/kubernetes/<role>.go`.

pub mod endpoints;
pub mod endpointslice;
pub mod ingress;
pub mod node;
pub mod pod;
pub mod service;

/// Joins a host and port into a single address string, matching Go's
/// `net.JoinHostPort`: an IPv6 host (contains `:` and isn't already
/// bracketed) is wrapped in `[...]` so the result stays unambiguous.
pub fn join_host_port(host: &str, port: i64) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn joins_ipv4_host_and_port() {
        assert_eq!(join_host_port("10.0.0.5", 10250), "10.0.0.5:10250");
    }

    #[test]
    fn brackets_ipv6_host() {
        assert_eq!(join_host_port("::1", 10250), "[::1]:10250");
        assert_eq!(join_host_port("fe80::1", 9100), "[fe80::1]:9100");
    }

    #[test]
    fn leaves_already_bracketed_host_alone() {
        assert_eq!(join_host_port("[::1]", 10250), "[::1]:10250");
    }
}
