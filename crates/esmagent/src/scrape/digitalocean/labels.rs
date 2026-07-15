//! DigitalOcean droplet serde structs, the pagination-response parser, and
//! the `__meta_digitalocean_*` label builder ([`append_target_labels`]).
//!
//! Port of `lib/promscrape/discovery/digitalocean/digitalocean.go`'s
//! `droplet`/`networks`/`listDropletResponse` structs and `addDropletLabels`,
//! plus `api.go`'s `parseAPIResponse` and `nextURLPath`, reshaped for this
//! crate's [`TargetGroup`] shape (one group per droplet: the droplet's
//! `__address__` is the group's single target, and the `__meta_digitalocean_*`
//! set becomes the group's `labels`).
//!
//! Upstream includes `__address__` in the returned label set because a
//! Prometheus label set *is* the target; this crate's [`TargetGroup`] carries
//! the address separately in `targets`, so [`append_target_labels`] puts it
//! there and leaves it out of `labels` — mirroring `scrape::consul::labels`.
//!
//! Unlike Consul/EC2, DigitalOcean's tag and feature values go into
//! *comma-wrapped label values* (`,a,b,`), not into per-tag label *keys*, so
//! no `sanitize_label_name` is needed — every `__meta_digitalocean_*` key is a
//! fixed literal.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::scrape::config::ScrapeError;
use crate::scrape::discovery::TargetGroup;

/// One DigitalOcean droplet. Port of `digitalocean.go`'s `droplet`
/// (`/v2/droplets` array element). `#[serde(default)]` tolerates the many
/// response fields this port doesn't read.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Droplet {
    pub id: i64,
    pub name: String,
    pub status: String,
    pub features: Vec<String>,
    pub image: DropletImage,
    pub size_slug: String,
    pub networks: Networks,
    pub region: DropletRegion,
    pub tags: Vec<String>,
    pub vpc_uuid: String,
}

/// The `image` block of a [`Droplet`]. Port of `dropletImage`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct DropletImage {
    pub name: String,
    pub slug: String,
}

/// The `region` block of a [`Droplet`], narrowed to `slug`. Port of
/// `dropletRegion`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct DropletRegion {
    pub slug: String,
}

/// The `networks` block of a [`Droplet`]. Port of `networks`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Networks {
    pub v4: Vec<Network>,
    pub v6: Vec<Network>,
}

/// One network interface of a [`Droplet`]. Port of `network`. `type` is a
/// Rust keyword, so the field is renamed at deserialize time.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Network {
    pub ip_address: String,
    /// `private` | `public`.
    #[serde(rename = "type")]
    pub kind: String,
}

impl Droplet {
    /// Port of `droplet.getIPByNet`: first `ip_address` in the given network
    /// version (`v4`/`v6`) matching `net_type` (`private`/`public`), or empty.
    fn ip_by_net(&self, networks: &[Network], net_type: &str) -> String {
        networks
            .iter()
            .find(|n| n.kind == net_type)
            .map(|n| n.ip_address.clone())
            .unwrap_or_default()
    }
}

/// `/v2/droplets` list response. Port of `listDropletResponse`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct ListDropletResponse {
    pub droplets: Vec<Droplet>,
    pub links: Links,
}

/// The `links` block of a [`ListDropletResponse`]. Port of `links`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Links {
    pub pages: LinksPages,
}

/// The `links.pages` block: pagination cursors. Port of `linksPages`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct LinksPages {
    pub last: String,
    pub next: String,
}

/// Parses a `/v2/droplets` response body. Port of `parseAPIResponse`.
pub fn parse_api_response(data: &[u8]) -> Result<ListDropletResponse, ScrapeError> {
    serde_json::from_slice(data).map_err(|e| ScrapeError {
        msg: format!("failed parse digitalocean api response: {e}"),
    })
}

/// Extracts the request URI (path + query) of the next page from
/// `links.pages.next`, or `None` when there is no next page. Port of
/// `listDropletResponse.nextURLPath`: the absolute `next` URL is parsed and
/// reduced to its path+query so the follow-up request reuses the configured
/// base URL (matching upstream's `url.RequestURI()`).
pub fn next_url_path(next: &str) -> Result<Option<String>, ScrapeError> {
    if next.is_empty() {
        return Ok(None);
    }
    let url = url::Url::parse(next).map_err(|e| ScrapeError {
        msg: format!("cannot parse digital ocean next url: {next}: {e}"),
    })?;
    let mut path = url.path().to_string();
    if let Some(query) = url.query() {
        path.push('?');
        path.push_str(query);
    }
    Ok(Some(path))
}

/// Builds a [`TargetGroup`] per droplet that has at least one IPv4 network,
/// mirroring `addDropletLabels`. `__address__` is `publicIPv4:default_port`
/// (carried in the group's `targets`); every `__meta_digitalocean_*` label
/// goes in `labels`. `source` is threaded through unchanged so the reconcile
/// diff stays stable across refreshes. Droplets with no IPv4 network are
/// skipped (upstream `if len(droplet.Networks.V4) == 0 { continue }`).
pub fn append_target_labels(
    droplets: &[Droplet],
    default_port: u16,
    source: &str,
) -> Vec<TargetGroup> {
    let mut groups = Vec::new();
    for droplet in droplets {
        if droplet.networks.v4.is_empty() {
            continue;
        }

        let private_ipv4 = droplet.ip_by_net(&droplet.networks.v4, "private");
        let public_ipv4 = droplet.ip_by_net(&droplet.networks.v4, "public");
        let public_ipv6 = droplet.ip_by_net(&droplet.networks.v6, "public");

        let address = join_host_port(&public_ipv4, default_port);

        let mut m: BTreeMap<String, String> = BTreeMap::new();
        m.insert(
            "__meta_digitalocean_droplet_id".into(),
            droplet.id.to_string(),
        );
        m.insert(
            "__meta_digitalocean_droplet_name".into(),
            droplet.name.clone(),
        );
        m.insert(
            "__meta_digitalocean_image".into(),
            droplet.image.slug.clone(),
        );
        m.insert(
            "__meta_digitalocean_image_name".into(),
            droplet.image.name.clone(),
        );
        m.insert("__meta_digitalocean_private_ipv4".into(), private_ipv4);
        m.insert("__meta_digitalocean_public_ipv4".into(), public_ipv4);
        m.insert("__meta_digitalocean_public_ipv6".into(), public_ipv6);
        m.insert(
            "__meta_digitalocean_region".into(),
            droplet.region.slug.clone(),
        );
        m.insert("__meta_digitalocean_size".into(), droplet.size_slug.clone());
        m.insert("__meta_digitalocean_status".into(), droplet.status.clone());
        m.insert("__meta_digitalocean_vpc".into(), droplet.vpc_uuid.clone());
        if !droplet.features.is_empty() {
            m.insert(
                "__meta_digitalocean_features".into(),
                format!(",{},", droplet.features.join(",")),
            );
        }
        if !droplet.tags.is_empty() {
            m.insert(
                "__meta_digitalocean_tags".into(),
                format!(",{},", droplet.tags.join(",")),
            );
        }

        groups.push(TargetGroup {
            targets: vec![address],
            labels: m,
            source: source.to_string(),
        });
    }
    groups
}

/// `host:port`, bracketing `host` when it is an IPv6 address (contains a
/// `:`). Port of `discoveryutil.JoinHostPort` (local copy, matching
/// `scrape::consul`/`scrape::ec2`).
fn join_host_port(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Port of upstream `digitalocean_test.go::TestAddDropletLabels`: one
    /// fully-populated droplet must produce exactly the expected
    /// `__meta_digitalocean_*` set (features/tags comma-wrapped) and the
    /// `publicIPv4:port` `__address__`.
    #[test]
    fn add_droplet_labels_matches_upstream() {
        let droplet = Droplet {
            id: 15,
            tags: vec!["private".into(), "test".into()],
            status: "active".into(),
            name: "ubuntu-1".into(),
            region: DropletRegion { slug: "do".into() },
            features: vec!["feature-1".into(), "feature-2".into()],
            size_slug: "base-1".into(),
            vpc_uuid: "vpc-1".into(),
            image: DropletImage {
                name: "ubuntu".into(),
                slug: "18".into(),
            },
            networks: Networks {
                v4: vec![
                    Network {
                        kind: "public".into(),
                        ip_address: "100.100.100.100".into(),
                    },
                    Network {
                        kind: "private".into(),
                        ip_address: "10.10.10.10".into(),
                    },
                ],
                v6: vec![Network {
                    kind: "public".into(),
                    ip_address: "::1".into(),
                }],
            },
        };

        let groups = append_target_labels(&[droplet], 9100, "job/digitalocean");
        assert_eq!(groups.len(), 1);
        let g = &groups[0];
        assert_eq!(g.targets, vec!["100.100.100.100:9100".to_string()]);
        assert_eq!(g.source, "job/digitalocean");
        let l = &g.labels;
        assert_eq!(l["__meta_digitalocean_droplet_id"], "15");
        assert_eq!(l["__meta_digitalocean_droplet_name"], "ubuntu-1");
        assert_eq!(l["__meta_digitalocean_features"], ",feature-1,feature-2,");
        assert_eq!(l["__meta_digitalocean_image"], "18");
        assert_eq!(l["__meta_digitalocean_image_name"], "ubuntu");
        assert_eq!(l["__meta_digitalocean_private_ipv4"], "10.10.10.10");
        assert_eq!(l["__meta_digitalocean_public_ipv4"], "100.100.100.100");
        assert_eq!(l["__meta_digitalocean_public_ipv6"], "::1");
        assert_eq!(l["__meta_digitalocean_region"], "do");
        assert_eq!(l["__meta_digitalocean_size"], "base-1");
        assert_eq!(l["__meta_digitalocean_status"], "active");
        assert_eq!(l["__meta_digitalocean_tags"], ",private,test,");
        assert_eq!(l["__meta_digitalocean_vpc"], "vpc-1");
        // __address__ is the target, not a label.
        assert!(!l.contains_key("__address__"));
    }

    /// A droplet with no IPv4 network is skipped, matching upstream's
    /// `if len(droplet.Networks.V4) == 0 { continue }`.
    #[test]
    fn droplet_without_ipv4_is_skipped() {
        let droplet = Droplet {
            id: 1,
            networks: Networks {
                v4: vec![],
                v6: vec![Network {
                    kind: "public".into(),
                    ip_address: "::1".into(),
                }],
            },
            ..Droplet::default()
        };
        assert!(append_target_labels(&[droplet], 80, "s").is_empty());
    }

    /// Absent features/tags omit their labels entirely (upstream only adds
    /// them when the slice is non-empty).
    #[test]
    fn empty_features_and_tags_omit_labels() {
        let droplet = Droplet {
            id: 2,
            networks: Networks {
                v4: vec![Network {
                    kind: "public".into(),
                    ip_address: "1.2.3.4".into(),
                }],
                v6: vec![],
            },
            ..Droplet::default()
        };
        let groups = append_target_labels(&[droplet], 80, "s");
        let l = &groups[0].labels;
        assert!(!l.contains_key("__meta_digitalocean_features"));
        assert!(!l.contains_key("__meta_digitalocean_tags"));
        // ipv6/private ipv4 absent -> empty strings, still present as labels.
        assert_eq!(l["__meta_digitalocean_public_ipv6"], "");
        assert_eq!(l["__meta_digitalocean_private_ipv4"], "");
    }

    /// Port of upstream `api_test.go::TestParseAPIResponse`: one droplet plus
    /// the `links.pages.{last,next}` cursors parse out of a real DO body.
    #[test]
    fn parse_api_response_extracts_droplet_and_links() {
        let data = br#"
{
  "droplets": [
    {
      "id": 3164444,
      "name": "example.com",
      "status": "active",
      "features": ["backups", "ipv6", "virtio"],
      "image": { "id": 6918990, "name": "14.04 x64", "slug": "ubuntu-16-04-x64" },
      "size_slug": "s-1vcpu-1gb",
      "networks": {
        "v4": [{ "ip_address": "104.236.32.182", "type": "public" }],
        "v6": [{ "ip_address": "2604:A880::02DD:4001", "type": "public" }]
      },
      "region": { "name": "New York 3", "slug": "nyc3" },
      "tags": ["tag1", "tag2"],
      "vpc_uuid": "f9b0769c-e118-42fb-a0c4-fed15ef69662"
    }
  ],
  "links": {
    "pages": {
      "last": "https://api.digitalocean.com/v2/droplets?page=3&per_page=1",
      "next": "https://api.digitalocean.com/v2/droplets?page=2&per_page=1"
    }
  }
}"#;
        let resp = parse_api_response(data).unwrap();
        assert_eq!(resp.droplets.len(), 1);
        let d = &resp.droplets[0];
        assert_eq!(d.id, 3164444);
        assert_eq!(d.name, "example.com");
        assert_eq!(d.image.slug, "ubuntu-16-04-x64");
        assert_eq!(d.region.slug, "nyc3");
        assert_eq!(d.size_slug, "s-1vcpu-1gb");
        assert_eq!(d.features, vec!["backups", "ipv6", "virtio"]);
        assert_eq!(d.tags, vec!["tag1", "tag2"]);
        assert_eq!(d.networks.v4[0].ip_address, "104.236.32.182");
        assert_eq!(
            resp.links.pages.next,
            "https://api.digitalocean.com/v2/droplets?page=2&per_page=1"
        );
    }

    /// `next_url_path` reduces an absolute next URL to path+query, and an
    /// empty `next` yields `None` (end of pagination). Port of `nextURLPath`.
    #[test]
    fn next_url_path_reduces_to_request_uri() {
        assert_eq!(
            next_url_path("https://api.digitalocean.com/v2/droplets?page=2&per_page=1").unwrap(),
            Some("/v2/droplets?page=2&per_page=1".to_string())
        );
        assert_eq!(next_url_path("").unwrap(), None);
    }

    #[test]
    fn ipv6_host_is_bracketed() {
        assert_eq!(join_host_port("::1", 80), "[::1]:80");
        assert_eq!(join_host_port("10.0.0.1", 80), "10.0.0.1:80");
    }
}
