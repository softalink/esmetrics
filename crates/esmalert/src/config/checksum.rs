//! Per-group checksum computation.
//!
//! Port of the `Checksum` field computed in `Group.UnmarshalYAML`
//! (`app/vmalert/config/config.go:57-71`): a stable hash over the group's
//! shape, used to detect changes such as rule re-ordering. Upstream hashes
//! an FNV-1a over a `yaml.Marshal` of the group; here we hash SHA-256 over a
//! `serde_yaml_ng` marshal of the group. The exact bytes don't need to match
//! upstream's YAML shape — only determinism (same group -> same checksum)
//! and sensitivity (any field change -> a different checksum) matter.

use sha2::{Digest, Sha256};

use super::types::Group;

impl Group {
    /// Returns a stable, content-sensitive hex-encoded checksum for this
    /// group.
    pub fn checksum(&self) -> String {
        // `serde_yaml_ng::to_string` on this plain data struct (Strings,
        // BTreeMaps, Vecs, Options, Durations — no custom `Serialize` impl
        // that can fail) is infallible in practice; the `unwrap_or_else`
        // fallback below exists only so this function can never panic, per
        // project convention, while staying deterministic and
        // content-sensitive even in that unreachable branch.
        let canonical = serde_yaml_ng::to_string(self).unwrap_or_else(|_| format!("{self:?}"));
        let mut hasher = Sha256::new();
        hasher.update(canonical.as_bytes());
        hasher
            .finalize()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::Rule;
    use super::*;

    fn group_with_expr(expr: &str) -> Group {
        Group {
            name: "g".into(),
            rules: vec![Rule {
                alert: Some("a".into()),
                expr: expr.to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn checksum_stable_and_sensitive() {
        let g1 = group_with_expr("up");
        let mut g2 = g1.clone();
        g2.rules[0].expr = "down".into();
        assert_eq!(g1.checksum(), g1.clone().checksum());
        assert_ne!(g1.checksum(), g2.checksum());
    }

    #[test]
    fn checksum_is_a_64_char_hex_string() {
        let g = group_with_expr("up");
        let sum = g.checksum();
        assert_eq!(sum.len(), 64);
        assert!(sum.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn checksum_is_sensitive_to_rule_order() {
        let mut g1 = group_with_expr("up");
        g1.rules.push(Rule {
            record: Some("r".into()),
            expr: "down".into(),
            ..Default::default()
        });
        let mut g2 = g1.clone();
        g2.rules.reverse();
        assert_ne!(g1.checksum(), g2.checksum());
    }
}
