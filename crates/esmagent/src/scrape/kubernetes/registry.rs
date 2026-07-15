//! Shared cross-role object registry + per-build context.
//!
//! A single [`super::KubernetesDiscovery`] may own several watchers: the
//! primary role's watchers plus dependency-role watchers started for
//! `attach_metadata` (and, later, the endpoints roles). The
//! [`ObjectRegistry`] gives the role builders a way to reach *other* roles'
//! cached objects — the analog of upstream `groupWatcher`'s
//! `getObjectByRoleLocked` (`api_watcher.go`). [`BuildCtx`] bundles that
//! registry plus the resolved `attach_metadata` flags into the single
//! parameter every role builder now takes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::object::K8sObject;

/// Shared object cache view across every watcher a single
/// `KubernetesDiscovery` owns — the analog of upstream `groupWatcher`'s
/// `getObjectByRoleLocked` (`api_watcher.go`). Entries are registered once
/// at discovery construction; lookups scan entries matching role +
/// namespace scope. Never held across I/O: `get_object` locks one cache
/// just long enough to clone the `Arc`.
#[derive(Default)]
pub struct ObjectRegistry {
    entries: Vec<RegistryEntry>,
}

struct RegistryEntry {
    role: String,
    /// The watcher's namespace scope: `None` = cluster-wide.
    namespace: Option<String>,
    cache: Arc<Mutex<HashMap<String, Arc<K8sObject>>>>,
}

impl ObjectRegistry {
    pub fn register(
        &mut self,
        role: &str,
        namespace: Option<&str>,
        cache: Arc<Mutex<HashMap<String, Arc<K8sObject>>>>,
    ) {
        self.entries.push(RegistryEntry {
            role: role.to_string(),
            namespace: namespace.map(|s| s.to_string()),
            cache,
        });
    }

    /// Upstream `getObjectByRoleLocked`: `node`/`namespace` lookups force an
    /// empty namespace (cluster-scoped kinds, key `"/<name>"`); a namespaced
    /// entry only matches when its scope equals the requested namespace (a
    /// cluster-wide entry matches any).
    pub fn get_object(&self, role: &str, namespace: &str, name: &str) -> Option<Arc<K8sObject>> {
        let namespace = if role == "node" || role == "namespace" {
            ""
        } else {
            namespace
        };
        let key = format!("{namespace}/{name}");
        for e in &self.entries {
            if e.role != role {
                continue;
            }
            if !namespace.is_empty() {
                if let Some(scope) = &e.namespace {
                    if scope != namespace {
                        continue;
                    }
                }
            }
            if let Some(obj) = e.cache.lock().unwrap().get(&key) {
                return Some(Arc::clone(obj));
            }
        }
        None
    }
}

/// Everything a role builder needs beyond the object itself.
pub struct BuildCtx<'a> {
    pub registry: &'a ObjectRegistry,
    pub attach_node_metadata: bool,
    pub attach_namespace_metadata: bool,
}

impl BuildCtx<'_> {
    /// A ctx with an empty registry and no attach flags — Phase A behavior.
    /// Used by tests and any caller without cross-role state.
    pub fn detached() -> BuildCtx<'static> {
        static EMPTY: std::sync::OnceLock<ObjectRegistry> = std::sync::OnceLock::new();
        BuildCtx {
            registry: EMPTY.get_or_init(ObjectRegistry::default),
            attach_node_metadata: false,
            attach_namespace_metadata: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scrape::kubernetes::object::parse_list;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    fn cache_with(role: &str, list_json: &[u8]) -> Arc<Mutex<HashMap<String, Arc<K8sObject>>>> {
        let (objs, _) = parse_list(role, list_json).unwrap();
        let mut m = HashMap::new();
        for o in objs {
            m.insert(o.key(), Arc::new(o));
        }
        Arc::new(Mutex::new(m))
    }

    #[test]
    fn get_object_scopes_by_role_and_namespace() {
        let mut reg = ObjectRegistry::default();
        reg.register(
            "pod",
            Some("d"),
            cache_with(
                "pod",
                br#"{"items":[{"metadata":{"name":"p1","namespace":"d"},
                "spec":{"containers":[{"name":"c"}]},
                "status":{"phase":"Running","podIP":"10.0.0.1"}}]}"#,
            ),
        );
        reg.register(
            "node",
            None,
            cache_with("node", br#"{"items":[{"metadata":{"name":"n1"}}]}"#),
        );
        assert!(reg.get_object("pod", "d", "p1").is_some());
        assert!(reg.get_object("pod", "other", "p1").is_none());
        assert!(reg.get_object("service", "d", "p1").is_none());
        // node lookup ignores the requested namespace (cluster-scoped key "/n1")
        assert!(reg.get_object("node", "d", "n1").is_some());
    }
}
