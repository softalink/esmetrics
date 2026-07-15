//! EC2 `DescribeInstances` / `DescribeAvailabilityZones` XML response structs
//! (parsed with `quick-xml`'s serde support) and the `__meta_ec2_*` label
//! builder.
//!
//! Port of `lib/promscrape/discovery/ec2/instance.go` (`Instance`/
//! `Reservation`/... structs + `appendTargetLabels`) and `az.go`
//! (`AvailabilityZone` structs), reshaped for this crate's [`TargetGroup`]:
//! one group per instance, whose single `__address__` (the instance's
//! private IP + configured port) is carried in `targets` and whose
//! `__meta_ec2_*` set is `labels` — mirroring `scrape::consul::labels`.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::scrape::discovery::TargetGroup;
use crate::scrape::kubernetes::labels::sanitize_label_name;

/// `DescribeInstances` response. Port of `instance.go`'s `InstancesResponse`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct InstancesResponse {
    #[serde(rename = "reservationSet")]
    pub reservation_set: ReservationSet,
    #[serde(rename = "nextToken")]
    pub next_token: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ReservationSet {
    #[serde(rename = "item")]
    pub items: Vec<Reservation>,
}

/// Port of `instance.go`'s `Reservation`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Reservation {
    #[serde(rename = "ownerId")]
    pub owner_id: String,
    #[serde(rename = "instancesSet")]
    pub instance_set: InstanceSet,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct InstanceSet {
    #[serde(rename = "item")]
    pub items: Vec<Instance>,
}

/// Port of `instance.go`'s `Instance`, narrowed to the fields
/// `appendTargetLabels` reads.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Instance {
    #[serde(rename = "privateIpAddress")]
    pub private_ip_address: String,
    #[serde(rename = "architecture")]
    pub architecture: String,
    #[serde(rename = "placement")]
    pub placement: Placement,
    #[serde(rename = "imageId")]
    pub image_id: String,
    #[serde(rename = "instanceId")]
    pub id: String,
    #[serde(rename = "instanceLifecycle")]
    pub lifecycle: String,
    #[serde(rename = "instanceState")]
    pub state: InstanceState,
    #[serde(rename = "instanceType")]
    pub instance_type: String,
    #[serde(rename = "platform")]
    pub platform: String,
    #[serde(rename = "subnetId")]
    pub subnet_id: String,
    #[serde(rename = "privateDnsName")]
    pub private_dns_name: String,
    #[serde(rename = "dnsName")]
    pub public_dns_name: String,
    #[serde(rename = "ipAddress")]
    pub public_ip_address: String,
    #[serde(rename = "vpcId")]
    pub vpc_id: String,
    #[serde(rename = "networkInterfaceSet")]
    pub network_interface_set: NetworkInterfaceSet,
    #[serde(rename = "tagSet")]
    pub tag_set: TagSet,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Placement {
    #[serde(rename = "availabilityZone")]
    pub availability_zone: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct InstanceState {
    #[serde(rename = "name")]
    pub name: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct NetworkInterfaceSet {
    #[serde(rename = "item")]
    pub items: Vec<NetworkInterface>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct NetworkInterface {
    #[serde(rename = "subnetId")]
    pub subnet_id: String,
    #[serde(rename = "ipv6AddressesSet")]
    pub ipv6_addresses_set: Ipv6AddressesSet,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Ipv6AddressesSet {
    #[serde(rename = "item")]
    pub items: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct TagSet {
    #[serde(rename = "item")]
    pub items: Vec<Tag>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Tag {
    #[serde(rename = "key")]
    pub key: String,
    #[serde(rename = "value")]
    pub value: String,
}

/// `DescribeAvailabilityZones` response. Port of `az.go`'s
/// `AvailabilityZonesResponse`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct AvailabilityZonesResponse {
    #[serde(rename = "availabilityZoneInfo")]
    pub availability_zone_info: AvailabilityZoneInfo,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct AvailabilityZoneInfo {
    #[serde(rename = "item")]
    pub items: Vec<AvailabilityZone>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct AvailabilityZone {
    #[serde(rename = "zoneName")]
    pub zone_name: String,
    #[serde(rename = "zoneId")]
    pub zone_id: String,
}

/// Parses a `DescribeInstances` XML response. Port of
/// `parseInstancesResponse`.
pub fn parse_instances_response(data: &[u8]) -> Result<InstancesResponse, String> {
    let text = std::str::from_utf8(data).map_err(|e| format!("response is not utf-8: {e}"))?;
    quick_xml::de::from_str(text).map_err(|e| format!("cannot unmarshal InstancesResponse: {e}"))
}

/// Parses a `DescribeAvailabilityZones` XML response. Port of
/// `parseAvailabilityZonesResponse`.
pub fn parse_availability_zones_response(data: &[u8]) -> Result<AvailabilityZonesResponse, String> {
    let text = std::str::from_utf8(data).map_err(|e| format!("response is not utf-8: {e}"))?;
    quick_xml::de::from_str(text)
        .map_err(|e| format!("cannot unmarshal DescribeAvailabilityZonesResponse: {e}"))
}

/// Builds the availability-zone-name -> availability-zone-id map from a
/// parsed `DescribeAvailabilityZones` response. Port of `getAZMap`'s map
/// build.
pub fn build_az_map(azr: &AvailabilityZonesResponse) -> BTreeMap<String, String> {
    azr.availability_zone_info
        .items
        .iter()
        .map(|az| (az.zone_name.clone(), az.zone_id.clone()))
        .collect()
}

/// Builds the [`TargetGroup`] for one EC2 instance, mirroring
/// `appendTargetLabels`. Returns `None` for an instance with no private IP
/// (upstream skips it — it cannot be scraped). `__address__` (private IP +
/// `port`) is carried in the group's `targets`; every `__meta_ec2_*` label
/// goes in `labels`. `source` is threaded through unchanged.
pub fn append_target_labels(
    inst: &Instance,
    owner_id: &str,
    region: &str,
    port: u16,
    az_map: &BTreeMap<String, String>,
    source: String,
) -> Option<TargetGroup> {
    if inst.private_ip_address.is_empty() {
        // Cannot scrape an instance without a private IP address.
        return None;
    }
    let address = join_host_port(&inst.private_ip_address, port);

    let mut m: BTreeMap<String, String> = BTreeMap::new();
    m.insert("__meta_ec2_architecture".into(), inst.architecture.clone());
    m.insert("__meta_ec2_ami".into(), inst.image_id.clone());
    m.insert(
        "__meta_ec2_availability_zone".into(),
        inst.placement.availability_zone.clone(),
    );
    m.insert(
        "__meta_ec2_availability_zone_id".into(),
        az_map
            .get(&inst.placement.availability_zone)
            .cloned()
            .unwrap_or_default(),
    );
    m.insert("__meta_ec2_instance_id".into(), inst.id.clone());
    m.insert(
        "__meta_ec2_instance_lifecycle".into(),
        inst.lifecycle.clone(),
    );
    m.insert("__meta_ec2_instance_state".into(), inst.state.name.clone());
    m.insert(
        "__meta_ec2_instance_type".into(),
        inst.instance_type.clone(),
    );
    m.insert("__meta_ec2_owner_id".into(), owner_id.to_string());
    m.insert("__meta_ec2_platform".into(), inst.platform.clone());
    m.insert(
        "__meta_ec2_primary_subnet_id".into(),
        inst.subnet_id.clone(),
    );
    m.insert(
        "__meta_ec2_private_dns_name".into(),
        inst.private_dns_name.clone(),
    );
    m.insert(
        "__meta_ec2_private_ip".into(),
        inst.private_ip_address.clone(),
    );
    m.insert(
        "__meta_ec2_public_dns_name".into(),
        inst.public_dns_name.clone(),
    );
    m.insert(
        "__meta_ec2_public_ip".into(),
        inst.public_ip_address.clone(),
    );
    m.insert("__meta_ec2_region".into(), region.to_string());
    m.insert("__meta_ec2_vpc_id".into(), inst.vpc_id.clone());

    if !inst.vpc_id.is_empty() {
        let mut subnets: Vec<String> = Vec::new();
        let mut seen: BTreeMap<String, ()> = BTreeMap::new();
        let mut ipv6_addrs: Vec<String> = Vec::new();
        for ni in &inst.network_interface_set.items {
            if ni.subnet_id.is_empty() {
                continue;
            }
            // Deduplicate subnet IDs, preserving the interface order.
            if seen.insert(ni.subnet_id.clone(), ()).is_none() {
                subnets.push(ni.subnet_id.clone());
            }
            ipv6_addrs.extend(ni.ipv6_addresses_set.items.iter().cloned());
        }
        // Surround the list with the separator so relabel regexes don't have
        // to consider element position.
        m.insert(
            "__meta_ec2_subnet_id".into(),
            format!(",{},", subnets.join(",")),
        );
        if !ipv6_addrs.is_empty() {
            m.insert(
                "__meta_ec2_ipv6_addresses".into(),
                format!(",{},", ipv6_addrs.join(",")),
            );
        }
    }

    for t in &inst.tag_set.items {
        if t.key.is_empty() || t.value.is_empty() {
            continue;
        }
        m.insert(
            sanitize_label_name(&format!("__meta_ec2_tag_{}", t.key)),
            t.value.clone(),
        );
    }

    Some(TargetGroup {
        targets: vec![address],
        labels: m,
        source,
    })
}

/// `host:port`, bracketing `host` when it is an IPv6 address (contains a
/// `:`). Port of `discoveryutil.JoinHostPort`.
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

    /// The `DescribeInstances` fixture from upstream
    /// `instance_test.go` (`TestParseInstancesResponse`), trimmed of the
    /// fields this port does not read but keeping the exact shape.
    const INSTANCES_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<DescribeInstancesResponse xmlns="http://ec2.amazonaws.com/doc/2013-10-15/">
    <requestId>98667f8e-7fb6-441b-a612-41c6268c6399</requestId>
    <reservationSet>
        <item>
            <reservationId>r-05534f81f74ea7036</reservationId>
            <ownerId>793614593844</ownerId>
            <instancesSet>
                <item>
                    <instanceId>i-0e730b692d9c15460</instanceId>
                    <imageId>ami-0eb89db7593b5d434</imageId>
                    <instanceState>
                        <code>16</code>
                        <name>running</name>
                    </instanceState>
                    <privateDnsName>ip-172-31-11-152.eu-west-2.compute.internal</privateDnsName>
                    <dnsName>ec2-3-8-232-141.eu-west-2.compute.amazonaws.com</dnsName>
                    <instanceType>t2.micro</instanceType>
                    <placement>
                        <availabilityZone>eu-west-2c</availabilityZone>
                        <tenancy>default</tenancy>
                    </placement>
                    <subnetId>subnet-57044c3e</subnetId>
                    <vpcId>vpc-f1eaad99</vpcId>
                    <privateIpAddress>172.31.11.152</privateIpAddress>
                    <ipAddress>3.8.232.141</ipAddress>
                    <architecture>x86_64</architecture>
                    <tagSet>
                        <item>
                            <key>foo</key>
                            <value>bar</value>
                        </item>
                    </tagSet>
                    <networkInterfaceSet>
                        <item>
                            <networkInterfaceId>eni-01d7b338ea037a60b</networkInterfaceId>
                            <subnetId>subnet-57044c3e</subnetId>
                            <vpcId>vpc-f1eaad99</vpcId>
                        </item>
                    </networkInterfaceSet>
                    <instanceLifecycle>spot</instanceLifecycle>
                    <platform>windows</platform>
                </item>
            </instancesSet>
        </item>
    </reservationSet>
</DescribeInstancesResponse>
"#;

    #[test]
    fn parses_instances_response_fields() {
        let resp = parse_instances_response(INSTANCES_XML.as_bytes()).unwrap();
        assert_eq!(resp.next_token, "");
        let rs = &resp.reservation_set.items;
        assert_eq!(rs.len(), 1);
        assert_eq!(rs[0].owner_id, "793614593844");
        let inst = &rs[0].instance_set.items[0];
        assert_eq!(inst.id, "i-0e730b692d9c15460");
        assert_eq!(inst.private_ip_address, "172.31.11.152");
        assert_eq!(inst.public_ip_address, "3.8.232.141");
        assert_eq!(inst.state.name, "running");
        assert_eq!(inst.placement.availability_zone, "eu-west-2c");
        assert_eq!(inst.instance_type, "t2.micro");
        assert_eq!(inst.lifecycle, "spot");
        assert_eq!(inst.platform, "windows");
        assert_eq!(inst.vpc_id, "vpc-f1eaad99");
        assert_eq!(inst.tag_set.items[0].key, "foo");
        assert_eq!(inst.tag_set.items[0].value, "bar");
        assert_eq!(
            inst.network_interface_set.items[0].subnet_id,
            "subnet-57044c3e"
        );
    }

    /// The label set + `__address__` must match the expected output from
    /// upstream `instance_test.go` (`appendTargetLabels(nil, ownerID,
    /// "region-a", 423, {"eu-west-2c": "foobar-zone"})`).
    #[test]
    fn builds_meta_ec2_labels_matching_upstream_vector() {
        let resp = parse_instances_response(INSTANCES_XML.as_bytes()).unwrap();
        let r = &resp.reservation_set.items[0];
        let inst = &r.instance_set.items[0];
        let az_map: BTreeMap<String, String> =
            [("eu-west-2c".to_string(), "foobar-zone".to_string())].into();

        let g = append_target_labels(inst, &r.owner_id, "region-a", 423, &az_map, "src".into())
            .expect("instance has a private IP");

        // __address__ is the target, private IP + port.
        assert_eq!(g.targets, vec!["172.31.11.152:423".to_string()]);
        assert!(!g.labels.contains_key("__address__"));

        let l = &g.labels;
        assert_eq!(l["__meta_ec2_architecture"], "x86_64");
        assert_eq!(l["__meta_ec2_availability_zone"], "eu-west-2c");
        assert_eq!(l["__meta_ec2_availability_zone_id"], "foobar-zone");
        assert_eq!(l["__meta_ec2_ami"], "ami-0eb89db7593b5d434");
        assert_eq!(l["__meta_ec2_instance_id"], "i-0e730b692d9c15460");
        assert_eq!(l["__meta_ec2_instance_lifecycle"], "spot");
        assert_eq!(l["__meta_ec2_instance_state"], "running");
        assert_eq!(l["__meta_ec2_instance_type"], "t2.micro");
        assert_eq!(l["__meta_ec2_owner_id"], "793614593844");
        assert_eq!(l["__meta_ec2_platform"], "windows");
        assert_eq!(l["__meta_ec2_primary_subnet_id"], "subnet-57044c3e");
        assert_eq!(
            l["__meta_ec2_private_dns_name"],
            "ip-172-31-11-152.eu-west-2.compute.internal"
        );
        assert_eq!(l["__meta_ec2_private_ip"], "172.31.11.152");
        assert_eq!(
            l["__meta_ec2_public_dns_name"],
            "ec2-3-8-232-141.eu-west-2.compute.amazonaws.com"
        );
        assert_eq!(l["__meta_ec2_public_ip"], "3.8.232.141");
        assert_eq!(l["__meta_ec2_region"], "region-a");
        assert_eq!(l["__meta_ec2_subnet_id"], ",subnet-57044c3e,");
        assert_eq!(l["__meta_ec2_tag_foo"], "bar");
        assert_eq!(l["__meta_ec2_vpc_id"], "vpc-f1eaad99");
    }

    #[test]
    fn skips_instance_without_private_ip() {
        let inst = Instance::default();
        let g = append_target_labels(&inst, "owner", "r", 80, &BTreeMap::new(), "src".into());
        assert!(g.is_none());
    }

    #[test]
    fn dedups_subnets_and_wraps_ipv6_addresses() {
        let inst = Instance {
            private_ip_address: "10.0.0.5".into(),
            vpc_id: "vpc-1".into(),
            network_interface_set: NetworkInterfaceSet {
                items: vec![
                    NetworkInterface {
                        subnet_id: "subnet-a".into(),
                        ipv6_addresses_set: Ipv6AddressesSet {
                            items: vec!["2001:db8::1".into()],
                        },
                    },
                    NetworkInterface {
                        subnet_id: "subnet-a".into(),
                        ipv6_addresses_set: Ipv6AddressesSet {
                            items: vec!["2001:db8::2".into()],
                        },
                    },
                    NetworkInterface {
                        subnet_id: "subnet-b".into(),
                        ipv6_addresses_set: Ipv6AddressesSet::default(),
                    },
                ],
            },
            ..Instance::default()
        };
        let g = append_target_labels(&inst, "o", "r", 80, &BTreeMap::new(), "s".into()).unwrap();
        assert_eq!(g.labels["__meta_ec2_subnet_id"], ",subnet-a,subnet-b,");
        assert_eq!(
            g.labels["__meta_ec2_ipv6_addresses"],
            ",2001:db8::1,2001:db8::2,"
        );
    }

    #[test]
    fn parses_availability_zones_response() {
        let data = r#"<DescribeAvailabilityZonesResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
    <requestId>e23c5a54-a29c-43ee-8b55-0c13c26e9e01</requestId>
    <availabilityZoneInfo>
        <item>
            <zoneName>us-west-2a</zoneName>
            <zoneId>usw2-az1</zoneId>
        </item>
        <item>
            <zoneName>us-west-2b</zoneName>
            <zoneId>usw2-az2</zoneId>
        </item>
    </availabilityZoneInfo>
</DescribeAvailabilityZonesResponse>
"#;
        let azr = parse_availability_zones_response(data.as_bytes()).unwrap();
        let m = build_az_map(&azr);
        assert_eq!(m["us-west-2a"], "usw2-az1");
        assert_eq!(m["us-west-2b"], "usw2-az2");
    }

    #[test]
    fn ipv4_host_not_bracketed() {
        assert_eq!(join_host_port("10.0.0.1", 80), "10.0.0.1:80");
        assert_eq!(join_host_port("::1", 80), "[::1]:80");
    }
}
