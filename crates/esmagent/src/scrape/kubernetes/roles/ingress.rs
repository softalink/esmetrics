//! `ingress` role target-group builder.
//!
//! Ports upstream vmagent's `getTargetLabels`/`getSchemeForHost`/
//! `matchesHostPattern`/`getIngressRulePaths`
//! (`lib/promscrape/discoveryutils/kubernetes/ingress.go`).

use std::collections::BTreeMap;

use crate::scrape::discovery::TargetGroup;
use crate::scrape::kubernetes::labels::register_labels_and_annotations;
use crate::scrape::kubernetes::object::{Ingress, IngressPath, IngressTLS, K8sObject};
use crate::scrape::kubernetes::registry::BuildCtx;

/// Builds the [`TargetGroup`]s for a single ingress: one per (rule host,
/// resolved path) pair across all of its rules.
pub fn ingress_target_groups(ig: &Ingress, ctx: &BuildCtx) -> Vec<TargetGroup> {
    let mut groups = Vec::new();
    for rule in &ig.spec.rules {
        let paths = ingress_rule_paths(&rule.http.paths);
        let scheme = scheme_for_host(&rule.host, &ig.spec.tls);
        for path in paths {
            groups.push(build_target_group(ig, ctx, scheme, &rule.host, &path));
        }
    }
    groups
}

/// Returns the resolved path list for a rule: each declared path (with an
/// empty path defaulting to `/`), or `["/"]` if the rule declares no paths.
fn ingress_rule_paths(paths: &[IngressPath]) -> Vec<String> {
    if paths.is_empty() {
        return vec!["/".to_string()];
    }
    paths
        .iter()
        .map(|p| {
            if p.path.is_empty() {
                "/".to_string()
            } else {
                p.path.clone()
            }
        })
        .collect()
}

/// Returns `"https"` if `host` matches any TLS host pattern declared across
/// `tlss`, else `"http"`.
fn scheme_for_host(host: &str, tlss: &[IngressTLS]) -> &'static str {
    for tls in tlss {
        for host_pattern in &tls.hosts {
            if matches_host_pattern(host_pattern, host) {
                return "https";
            }
        }
    }
    "http"
}

/// Ports upstream `matchesHostPattern` exactly: an exact match always wins;
/// otherwise `pattern` must be a `*.<suffix>` wildcard, and `host` matches
/// when dropping its first `.`-terminated label leaves exactly `<suffix>`.
fn matches_host_pattern(pattern: &str, host: &str) -> bool {
    if pattern == host {
        return true;
    }
    let Some(suffix) = pattern.strip_prefix("*.") else {
        return false;
    };
    let Some(dot) = host.find('.') else {
        return false;
    };
    suffix == &host[dot + 1..]
}

/// Builds a single [`TargetGroup`] for one (ingress, scheme, host, path).
fn build_target_group(
    ig: &Ingress,
    ctx: &BuildCtx,
    scheme: &str,
    host: &str,
    path: &str,
) -> TargetGroup {
    let mut labels = BTreeMap::new();
    labels.insert(
        "__meta_kubernetes_namespace".to_string(),
        ig.metadata.namespace.clone(),
    );
    labels.insert(
        "__meta_kubernetes_ingress_name".to_string(),
        ig.metadata.name.clone(),
    );
    labels.insert(
        "__meta_kubernetes_ingress_scheme".to_string(),
        scheme.to_string(),
    );
    labels.insert(
        "__meta_kubernetes_ingress_host".to_string(),
        host.to_string(),
    );
    labels.insert(
        "__meta_kubernetes_ingress_path".to_string(),
        path.to_string(),
    );
    labels.insert(
        "__meta_kubernetes_ingress_class_name".to_string(),
        ig.spec.ingress_class_name.clone(),
    );

    if ctx.attach_namespace_metadata {
        if let Some(o) = ctx
            .registry
            .get_object("namespace", "", &ig.metadata.namespace)
        {
            if let K8sObject::Namespace(ns) = &*o {
                register_labels_and_annotations(
                    "__meta_kubernetes_namespace",
                    &ns.metadata,
                    &mut labels,
                );
            }
        }
    }

    register_labels_and_annotations("__meta_kubernetes_ingress", &ig.metadata, &mut labels);

    let source = format!(
        "kubernetes_sd/ingress/{}/{}",
        ig.metadata.namespace, ig.metadata.name
    );

    TargetGroup {
        targets: vec![host.to_string()],
        labels,
        source,
    }
}

#[cfg(test)]
mod tests {
    use crate::scrape::kubernetes::registry::BuildCtx;

    #[test]
    fn ingress_fans_out_hosts_paths_and_tls_scheme() {
        let j = br#"{"items":[{"metadata":{"name":"web","namespace":"prod"},
            "spec":{"ingressClassName":"nginx",
              "tls":[{"hosts":["secure.example.com"]}],
              "rules":[
                {"host":"secure.example.com","http":{"paths":[{"path":"/a"},{"path":"/b"}]}},
                {"host":"plain.example.com","http":{"paths":[]}}]}}]}"#;
        let (objs, _) = crate::scrape::kubernetes::object::parse_list("ingress", j).unwrap();
        let g = objs[0].target_groups(&BuildCtx::detached());
        assert_eq!(g.len(), 3); // /a,/b on secure + / on plain
        let secure_a = g
            .iter()
            .find(|x| x.labels["__meta_kubernetes_ingress_path"] == "/a")
            .unwrap();
        assert_eq!(secure_a.targets, vec!["secure.example.com".to_string()]);
        assert_eq!(secure_a.labels["__meta_kubernetes_ingress_scheme"], "https");
        assert_eq!(
            secure_a.labels["__meta_kubernetes_ingress_class_name"],
            "nginx"
        );
        let plain = g
            .iter()
            .find(|x| x.labels["__meta_kubernetes_ingress_host"] == "plain.example.com")
            .unwrap();
        assert_eq!(plain.labels["__meta_kubernetes_ingress_scheme"], "http");
        assert_eq!(plain.labels["__meta_kubernetes_ingress_path"], "/");
    }

    #[test]
    fn wildcard_tls_host_matches_single_label_prefix() {
        let j = br#"{"items":[{"metadata":{"name":"web","namespace":"prod"},
            "spec":{"tls":[{"hosts":["*.example.com"]}],
              "rules":[{"host":"a.example.com","http":{"paths":[]}}]}}]}"#;
        let (objs, _) = crate::scrape::kubernetes::object::parse_list("ingress", j).unwrap();
        let g = objs[0].target_groups(&BuildCtx::detached());
        assert_eq!(g.len(), 1);
        assert_eq!(g[0].labels["__meta_kubernetes_ingress_scheme"], "https");
    }

    #[test]
    fn wildcard_tls_host_does_not_match_multi_label_subdomain() {
        let j = br#"{"items":[{"metadata":{"name":"web","namespace":"prod"},
            "spec":{"tls":[{"hosts":["*.example.com"]}],
              "rules":[{"host":"a.b.example.com","http":{"paths":[]}}]}}]}"#;
        let (objs, _) = crate::scrape::kubernetes::object::parse_list("ingress", j).unwrap();
        let g = objs[0].target_groups(&BuildCtx::detached());
        assert_eq!(g[0].labels["__meta_kubernetes_ingress_scheme"], "http");
    }

    #[test]
    fn ingress_attach_namespace_metadata_joins_namespace_labels() {
        use crate::scrape::kubernetes::registry::ObjectRegistry;
        let (igs, _) = crate::scrape::kubernetes::object::parse_list(
            "ingress",
            br#"{"items":[{"metadata":{"name":"web","namespace":"d"},
                "spec":{"rules":[{"host":"a.example.com","http":{"paths":[]}}]}}]}"#,
        )
        .unwrap();
        let (nss, _) = crate::scrape::kubernetes::object::parse_list(
            "namespace",
            br#"{"items":[{"metadata":{"name":"d","labels":{"team":"a"}}}]}"#,
        )
        .unwrap();
        let mut m = std::collections::HashMap::new();
        for o in nss {
            m.insert(o.key(), std::sync::Arc::new(o));
        }
        let mut reg = ObjectRegistry::default();
        reg.register(
            "namespace",
            None,
            std::sync::Arc::new(std::sync::Mutex::new(m)),
        );
        let ctx = BuildCtx {
            registry: &reg,
            attach_node_metadata: false,
            attach_namespace_metadata: true,
        };
        let g = igs[0].target_groups(&ctx);
        assert_eq!(g[0].labels["__meta_kubernetes_namespace_label_team"], "a");
        // without the flag the joined label is absent
        let g2 = igs[0].target_groups(&BuildCtx::detached());
        assert!(!g2[0]
            .labels
            .contains_key("__meta_kubernetes_namespace_label_team"));
    }
}
