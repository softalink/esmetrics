//! Eureka `/eureka/apps` XML response structs (parsed with `quick-xml`'s serde
//! support) and the `__meta_eureka_*` label builder.
//!
//! Port of `lib/promscrape/discovery/eureka/eureka.go` (v1.146.0) — the
//! `applications`/`Application`/`Instance`/`Port`/`DataCenterInfo` structs and
//! `addInstanceLabels`, plus `api.go`'s `parseAPIResponse` — reshaped for this
//! crate's [`TargetGroup`] (one group per instance, whose single `__address__`
//! from the instance's hostname and port is carried in `targets` and whose
//! `__meta_eureka_*` set is `labels`), mirroring `scrape::ec2::labels`.
//!
//! Upstream includes `__address__` and `instance` in the returned label set
//! because a Prometheus label set *is* the target; this crate's
//! [`TargetGroup`] carries the address separately in `targets`, so
//! [`append_target_labels`] puts `__address__` there. The `instance` label
//! (upstream overrides the default with the instance id) is kept in `labels`
//! and honored by `target::assemble_labels` (which applies `group.labels`
//! last, so it wins over the `instance`=`__address__` default).
//!
//! ## Dynamic metadata elements
//!
//! Eureka serializes instance/datacenter metadata as arbitrarily-named child
//! elements (`<metadata><foo>bar</foo></metadata>`), captured by the [`Metadata`]
//! newtype (a `BTreeMap<String, String>` keyed by element name). XML attributes
//! on the `<metadata>` element (e.g. `class="..."`) surface as `@`-prefixed map
//! keys and are filtered out — only the real child metadata tags become
//! `__meta_eureka_app_instance_metadata_<k>` labels, matching upstream's
//! `MetaData.Items` (which holds only child elements).
//!
//! [`Metadata`] deserializes *tolerantly*: a metadata child that itself contains
//! nested elements (`<foo><bar>x</bar></foo>`) is flattened to the concatenation
//! of its descendants' text rather than aborting the parse. This mirrors
//! upstream `eureka.go`'s `xml:",innerxml"` (which never errors) and, crucially,
//! stops one weird instance's structured metadata from dropping *all* targets of
//! the Eureka server — `quick-xml`'s default `BTreeMap<String, String>` value
//! deserialization returns `Err(UnexpectedStart)` on such nesting, which would
//! propagate out of [`parse_applications`] and fail the entire `/apps` response.

use std::collections::BTreeMap;
use std::fmt;

use serde::de::{Deserializer, MapAccess, Visitor};
use serde::Deserialize;

use crate::scrape::discovery::TargetGroup;
use crate::scrape::kubernetes::labels::sanitize_label_name;

/// Default port for the discovered target's `__address__` when an instance's
/// `<port>` is absent/zero — matches `addInstanceLabels`'s `instancePort := 80`.
const DEFAULT_INSTANCE_PORT: i64 = 80;

/// `/eureka/apps` response root. Port of `eureka.go`'s `applications`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Applications {
    #[serde(rename = "application")]
    pub applications: Vec<Application>,
}

/// A Eureka application. Port of `eureka.go`'s `Application`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Application {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "instance")]
    pub instances: Vec<Instance>,
}

/// A Eureka instance port: the `<port enabled="...">N</port>` element's text is
/// the port number and its `enabled` attribute the flag. Port of `eureka.go`'s
/// `Port` (`xml:",chardata"` + `enabled,attr`) — `quick-xml` spells the text
/// content `$text` and the attribute `@enabled`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Port {
    #[serde(rename = "@enabled")]
    pub enabled: bool,
    #[serde(rename = "$text")]
    pub port: i64,
}

/// A Eureka instance. Port of `eureka.go`'s `Instance`, narrowed to the fields
/// `addInstanceLabels` reads. `metadata`/`data_center_info.metadata` are the
/// dynamic-key maps described in the module doc.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Instance {
    #[serde(rename = "hostName")]
    pub host_name: String,
    #[serde(rename = "homePageUrl")]
    pub home_page_url: String,
    #[serde(rename = "statusPageUrl")]
    pub status_page_url: String,
    #[serde(rename = "healthCheckUrl")]
    pub health_check_url: String,
    #[serde(rename = "app")]
    pub app: String,
    #[serde(rename = "ipAddr")]
    pub ip_addr: String,
    #[serde(rename = "vipAddress")]
    pub vip_address: String,
    #[serde(rename = "secureVipAddress")]
    pub secure_vip_address: String,
    #[serde(rename = "status")]
    pub status: String,
    #[serde(rename = "port")]
    pub port: Port,
    #[serde(rename = "securePort")]
    pub secure_port: Port,
    #[serde(rename = "dataCenterInfo")]
    pub data_center_info: DataCenterInfo,
    #[serde(rename = "metadata")]
    pub metadata: Metadata,
    #[serde(rename = "countryId")]
    pub country_id: i64,
    #[serde(rename = "instanceId")]
    pub instance_id: String,
}

/// A Eureka instance's datacenter metadata. Port of `eureka.go`'s
/// `DataCenterInfo`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct DataCenterInfo {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "metadata")]
    pub metadata: Metadata,
}

/// A `<metadata>` element: direct child element names → their text value.
///
/// Deserializes tolerantly of nested content — see the module doc. A child whose
/// value is plain text (`<foo>bar</foo>`) maps to `"bar"`; a child that contains
/// nested elements (`<foo><bar>x</bar></foo>`) maps to the concatenation of its
/// descendants' text (`"x"`). XML attributes on `<metadata>` surface as
/// `@`-prefixed keys, exactly as with `quick-xml`'s plain-map deserialization, so
/// [`add_metadata_labels`] can keep filtering them out.
#[derive(Debug, Default)]
pub struct Metadata(pub BTreeMap<String, String>);

impl<'de> Deserialize<'de> for Metadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MetadataVisitor;

        impl<'de> Visitor<'de> for MetadataVisitor {
            type Value = Metadata;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a eureka <metadata> element")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Metadata, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut out = BTreeMap::new();
                while let Some(key) = map.next_key::<String>()? {
                    let value = map.next_value::<MetaValue>()?;
                    out.insert(key, value.0);
                }
                Ok(Metadata(out))
            }
        }

        deserializer.deserialize_map(MetadataVisitor)
    }
}

/// A single metadata child's value, tolerant of nested element content. Plain
/// text is taken verbatim; a child with nested elements is flattened to the
/// concatenation of its descendants' text, so structured metadata degrades to a
/// best-effort string instead of failing the whole `/apps` response.
struct MetaValue(String);

impl<'de> Deserialize<'de> for MetaValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(MetaValueVisitor)
    }
}

struct MetaValueVisitor;

impl<'de> Visitor<'de> for MetaValueVisitor {
    type Value = MetaValue;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a eureka metadata value (text or nested elements)")
    }

    fn visit_str<E>(self, v: &str) -> Result<MetaValue, E> {
        Ok(MetaValue(v.to_owned()))
    }

    fn visit_string<E>(self, v: String) -> Result<MetaValue, E> {
        Ok(MetaValue(v))
    }

    fn visit_unit<E>(self) -> Result<MetaValue, E> {
        Ok(MetaValue(String::new()))
    }

    fn visit_none<E>(self) -> Result<MetaValue, E> {
        Ok(MetaValue(String::new()))
    }

    fn visit_some<D>(self, deserializer: D) -> Result<MetaValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(MetaValueVisitor)
    }

    /// A nested child (`<foo><bar>x</bar>...</foo>`) — flatten to the
    /// concatenation of the descendants' text, never erroring.
    fn visit_map<A>(self, mut map: A) -> Result<MetaValue, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut acc = String::new();
        while let Some((_key, value)) = map.next_entry::<String, MetaValue>()? {
            acc.push_str(&value.0);
        }
        Ok(MetaValue(acc))
    }
}

/// Parses a `/eureka/apps` XML response body into [`Applications`]. Port of
/// `api.go`'s `parseAPIResponse`.
pub fn parse_applications(data: &[u8]) -> Result<Applications, String> {
    let text = std::str::from_utf8(data).map_err(|e| format!("response is not utf-8: {e}"))?;
    quick_xml::de::from_str(text).map_err(|e| format!("failed to parse eureka api response: {e}"))
}

/// Builds one [`TargetGroup`] per instance across every application, mirroring
/// `addInstanceLabels`. `__address__` (hostname + instance port) is carried in
/// each group's `targets`; every `__meta_eureka_*` label plus the `instance`
/// override goes in `labels`. `source` is threaded through unchanged so the
/// reconcile diff stays stable across refreshes.
pub fn append_target_labels(apps: &Applications, source: &str) -> Vec<TargetGroup> {
    let mut groups = Vec::new();
    for app in &apps.applications {
        for inst in &app.instances {
            groups.push(build_group(app, inst, source.to_string()));
        }
    }
    groups
}

/// Builds the [`TargetGroup`] for a single instance. Split out of
/// [`append_target_labels`] to keep the per-instance label logic focused.
fn build_group(app: &Application, inst: &Instance, source: String) -> TargetGroup {
    let instance_port = if inst.port.port != 0 {
        inst.port.port
    } else {
        DEFAULT_INSTANCE_PORT
    };
    let address = join_host_port(&inst.host_name, instance_port);

    let mut m: BTreeMap<String, String> = BTreeMap::new();
    m.insert("instance".into(), inst.instance_id.clone());
    m.insert("__meta_eureka_app_name".into(), app.name.clone());
    m.insert(
        "__meta_eureka_app_instance_hostname".into(),
        inst.host_name.clone(),
    );
    m.insert(
        "__meta_eureka_app_instance_homepage_url".into(),
        inst.home_page_url.clone(),
    );
    m.insert(
        "__meta_eureka_app_instance_statuspage_url".into(),
        inst.status_page_url.clone(),
    );
    m.insert(
        "__meta_eureka_app_instance_healthcheck_url".into(),
        inst.health_check_url.clone(),
    );
    m.insert(
        "__meta_eureka_app_instance_ip_addr".into(),
        inst.ip_addr.clone(),
    );
    m.insert(
        "__meta_eureka_app_instance_vip_address".into(),
        inst.vip_address.clone(),
    );
    m.insert(
        "__meta_eureka_app_instance_secure_vip_address".into(),
        inst.secure_vip_address.clone(),
    );
    m.insert(
        "__meta_eureka_app_instance_status".into(),
        inst.status.clone(),
    );
    m.insert(
        "__meta_eureka_app_instance_country_id".into(),
        inst.country_id.to_string(),
    );
    m.insert(
        "__meta_eureka_app_instance_id".into(),
        inst.instance_id.clone(),
    );
    if inst.port.port != 0 {
        m.insert(
            "__meta_eureka_app_instance_port".into(),
            inst.port.port.to_string(),
        );
        m.insert(
            "__meta_eureka_app_instance_port_enabled".into(),
            inst.port.enabled.to_string(),
        );
    }
    if inst.secure_port.port != 0 {
        m.insert(
            "__meta_eureka_app_instance_secure_port".into(),
            inst.secure_port.port.to_string(),
        );
        m.insert(
            "__meta_eureka_app_instance_secure_port_enabled".into(),
            inst.secure_port.enabled.to_string(),
        );
    }
    if !inst.data_center_info.name.is_empty() {
        m.insert(
            "__meta_eureka_app_instance_datacenterinfo_name".into(),
            inst.data_center_info.name.clone(),
        );
        add_metadata_labels(
            &mut m,
            &inst.data_center_info.metadata,
            "__meta_eureka_app_instance_datacenterinfo_metadata_",
        );
    }
    add_metadata_labels(
        &mut m,
        &inst.metadata,
        "__meta_eureka_app_instance_metadata_",
    );

    TargetGroup {
        targets: vec![address],
        labels: m,
        source,
    }
}

/// Emits `<prefix><k>`=`<v>` (name sanitized) for each real metadata child,
/// skipping `@`-prefixed keys (XML attributes surfaced by `quick-xml`, not part
/// of upstream's `MetaData.Items`). Mirrors `addInstanceLabels`'s metadata loops.
fn add_metadata_labels(m: &mut BTreeMap<String, String>, metadata: &Metadata, prefix: &str) {
    for (k, v) in &metadata.0 {
        if k.starts_with('@') {
            continue;
        }
        m.insert(sanitize_label_name(&format!("{prefix}{k}")), v.clone());
    }
}

/// `host:port`, bracketing `host` when it is an IPv6 address (contains a `:`).
/// Port of `discoveryutil.JoinHostPort`.
fn join_host_port(host: &str, port: i64) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// XML crafted to reproduce upstream `eureka_test.go`'s
    /// `TestAddInstanceLabels` expected label vector EXACTLY (one app, one
    /// instance, `port=9100` disabled, one metadata tag, no securePort, no
    /// datacenter).
    #[test]
    fn append_labels_matches_upstream_eureka_test() {
        let data = br#"<applications>
  <application>
    <name>test-app</name>
    <instance>
      <hostName>host-1</hostName>
      <homePageUrl>some-home-url</homePageUrl>
      <statusPageUrl>some-status-url</statusPageUrl>
      <healthCheckUrl>some-url</healthCheckUrl>
      <ipAddr>10.15.11.11</ipAddr>
      <vipAddress>10.15.11.11</vipAddress>
      <status>Ok</status>
      <port enabled="false">9100</port>
      <countryId>5</countryId>
      <instanceId>some-id</instanceId>
      <metadata><key-1>value-1</key-1></metadata>
    </instance>
  </application>
</applications>"#;
        let apps = parse_applications(data).unwrap();
        let groups = append_target_labels(&apps, "job/eureka");
        assert_eq!(groups.len(), 1);
        let g = &groups[0];

        // __address__ is the target, not a label.
        assert_eq!(g.targets, vec!["host-1:9100".to_string()]);
        assert!(!g.labels.contains_key("__address__"));
        assert_eq!(g.source, "job/eureka");

        let l = &g.labels;
        assert_eq!(l["instance"], "some-id");
        assert_eq!(l["__meta_eureka_app_name"], "test-app");
        assert_eq!(l["__meta_eureka_app_instance_hostname"], "host-1");
        assert_eq!(
            l["__meta_eureka_app_instance_homepage_url"],
            "some-home-url"
        );
        assert_eq!(
            l["__meta_eureka_app_instance_statuspage_url"],
            "some-status-url"
        );
        assert_eq!(l["__meta_eureka_app_instance_healthcheck_url"], "some-url");
        assert_eq!(l["__meta_eureka_app_instance_ip_addr"], "10.15.11.11");
        assert_eq!(l["__meta_eureka_app_instance_vip_address"], "10.15.11.11");
        assert_eq!(l["__meta_eureka_app_instance_secure_vip_address"], "");
        assert_eq!(l["__meta_eureka_app_instance_status"], "Ok");
        assert_eq!(l["__meta_eureka_app_instance_country_id"], "5");
        assert_eq!(l["__meta_eureka_app_instance_id"], "some-id");
        assert_eq!(l["__meta_eureka_app_instance_port"], "9100");
        assert_eq!(l["__meta_eureka_app_instance_port_enabled"], "false");
        assert_eq!(l["__meta_eureka_app_instance_metadata_key_1"], "value-1");

        // securePort/datacenter absent for this instance.
        assert!(!l.contains_key("__meta_eureka_app_instance_secure_port"));
        assert!(!l.contains_key("__meta_eureka_app_instance_datacenterinfo_name"));
        // The `@class`-style attribute key must never leak into labels.
        assert!(l.keys().all(
            |k| !k.contains("class") && !k.starts_with("__meta_eureka_app_instance_metadata_@")
        ));
    }

    /// The real `/eureka/apps` fixture from upstream `api_test.go`
    /// (`TestParseAPIResponse`): a full HELLO-NETFLIX-OSS instance with an
    /// enabled `port=8080`, a `securePort=443`, and a `dataCenterInfo` name.
    /// Validates XML parsing of every non-trivial field.
    #[test]
    fn parse_and_label_real_apps_fixture() {
        let data = br#"<applications>
  <versions__delta>1</versions__delta>
  <apps__hashcode>UP_1_</apps__hashcode>
  <application>
    <name>HELLO-NETFLIX-OSS</name>
    <instance>
      <hostName>98de25ebef42</hostName>
      <app>HELLO-NETFLIX-OSS</app>
      <ipAddr>10.10.0.3</ipAddr>
      <status>UP</status>
      <overriddenstatus>UNKNOWN</overriddenstatus>
      <port enabled="true">8080</port>
      <securePort enabled="false">443</securePort>
      <countryId>1</countryId>
      <dataCenterInfo class="com.netflix.appinfo.InstanceInfo$DefaultDataCenterInfo">
        <name>MyOwn</name>
      </dataCenterInfo>
      <metadata class="java.util.Collections$EmptyMap"/>
      <homePageUrl>http://98de25ebef42:8080/</homePageUrl>
      <statusPageUrl>http://98de25ebef42:8080/Status</statusPageUrl>
      <healthCheckUrl>http://98de25ebef42:8080/healthcheck</healthCheckUrl>
      <vipAddress>HELLO-NETFLIX-OSS</vipAddress>
    </instance>
  </application>
</applications>"#;
        let apps = parse_applications(data).unwrap();
        let groups = append_target_labels(&apps, "src");
        assert_eq!(groups.len(), 1);
        let g = &groups[0];

        // port enabled=true, so __address__ uses 8080.
        assert_eq!(g.targets, vec!["98de25ebef42:8080".to_string()]);
        let l = &g.labels;
        assert_eq!(l["__meta_eureka_app_name"], "HELLO-NETFLIX-OSS");
        assert_eq!(l["__meta_eureka_app_instance_ip_addr"], "10.10.0.3");
        assert_eq!(l["__meta_eureka_app_instance_status"], "UP");
        assert_eq!(l["__meta_eureka_app_instance_port"], "8080");
        assert_eq!(l["__meta_eureka_app_instance_port_enabled"], "true");
        assert_eq!(l["__meta_eureka_app_instance_secure_port"], "443");
        assert_eq!(l["__meta_eureka_app_instance_secure_port_enabled"], "false");
        assert_eq!(l["__meta_eureka_app_instance_datacenterinfo_name"], "MyOwn");
        assert_eq!(
            l["__meta_eureka_app_instance_homepage_url"],
            "http://98de25ebef42:8080/"
        );
        assert_eq!(l["__meta_eureka_app_instance_country_id"], "1");
        // Empty metadata element (only a `class` attribute) yields no metadata labels.
        assert!(l
            .keys()
            .all(|k| !k.starts_with("__meta_eureka_app_instance_metadata_")));
    }

    #[test]
    fn missing_port_defaults_to_80() {
        let data = br#"<applications><application><name>a</name>
          <instance><hostName>h</hostName><instanceId>i</instanceId></instance>
        </application></applications>"#;
        let apps = parse_applications(data).unwrap();
        let groups = append_target_labels(&apps, "s");
        assert_eq!(groups[0].targets, vec!["h:80".to_string()]);
        // No port element => no port_* labels (upstream only adds them when Port != 0).
        assert!(!groups[0]
            .labels
            .contains_key("__meta_eureka_app_instance_port"));
    }

    #[test]
    fn ipv6_hostname_is_bracketed() {
        assert_eq!(join_host_port("::1", 80), "[::1]:80");
        assert_eq!(join_host_port("10.0.0.1", 9100), "10.0.0.1:9100");
    }

    #[test]
    fn parse_failure_on_garbage() {
        assert!(parse_applications(b"not xml <<<").is_err());
    }

    /// A single instance whose `<metadata>` child contains a *nested* element
    /// must NOT fail the whole `/apps` response (upstream `xml:",innerxml"` never
    /// errors; `quick-xml`'s plain `BTreeMap<String, String>` value returns
    /// `Err(UnexpectedStart)` here and would drop every target). The first
    /// instance has structured metadata; the second has flat metadata. Both must
    /// still be discovered, the flat sibling tag captured, and the second
    /// instance's labels intact.
    #[test]
    fn nested_metadata_does_not_drop_targets() {
        let data = br#"<applications>
  <application>
    <name>app-a</name>
    <instance>
      <hostName>host-a</hostName>
      <instanceId>id-a</instanceId>
      <port enabled="true">7000</port>
      <metadata><flat>v1</flat><nested><inner>x</inner></nested></metadata>
    </instance>
  </application>
  <application>
    <name>app-b</name>
    <instance>
      <hostName>host-b</hostName>
      <instanceId>id-b</instanceId>
      <port enabled="true">8000</port>
      <metadata><plain>v2</plain></metadata>
    </instance>
  </application>
</applications>"#;

        // (a) parsing succeeds — the nested metadata does not error the response.
        let apps = parse_applications(data).unwrap();
        let groups = append_target_labels(&apps, "src");

        // (b) BOTH instances produced targets.
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].targets, vec!["host-a:7000".to_string()]);
        assert_eq!(groups[1].targets, vec!["host-b:8000".to_string()]);

        // (c) the flat sibling of the nested tag is captured verbatim, and the
        // nested tag degrades to its descendants' concatenated text.
        let a = &groups[0].labels;
        assert_eq!(a["__meta_eureka_app_instance_metadata_flat"], "v1");
        assert_eq!(a["__meta_eureka_app_instance_metadata_nested"], "x");

        // (d) the second instance's targets/labels are fully intact.
        let b = &groups[1].labels;
        assert_eq!(b["__meta_eureka_app_name"], "app-b");
        assert_eq!(b["__meta_eureka_app_instance_hostname"], "host-b");
        assert_eq!(b["__meta_eureka_app_instance_id"], "id-b");
        assert_eq!(b["instance"], "id-b");
        assert_eq!(b["__meta_eureka_app_instance_metadata_plain"], "v2");
        assert_eq!(b["__meta_eureka_app_instance_port"], "8000");
    }
}
