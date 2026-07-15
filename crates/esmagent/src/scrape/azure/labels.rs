//! Azure ARM JSON response structs (parsed with `serde_json`) and the
//! `__meta_azure_*` label builder.
//!
//! Port of `lib/promscrape/discovery/azure/machine.go` (`virtualMachine` /
//! `virtualMachineProperties` / ... structs) + `nic.go`
//! (`networkInterface`/...) + `azure.go`'s `appendMachineLabels`, reshaped for
//! this crate's [`TargetGroup`]: one group per VM private IP, whose single
//! `__address__` (the VM's private IP + configured port) is carried in
//! `targets` and whose `__meta_azure_*` set is `labels` — mirroring
//! `scrape::gce::labels` / `scrape::ec2::labels`.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::scrape::discovery::TargetGroup;
use crate::scrape::kubernetes::labels::sanitize_label_name;

/// An Azure virtual machine (standalone or created by a VMSS). Port of
/// `machine.go`'s `virtualMachine`. `scale_set` and `ip_addresses` are
/// enriched during discovery (not deserialized from the list response).
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct VirtualMachine {
    pub id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub location: String,
    pub properties: VirtualMachineProperties,
    pub tags: BTreeMap<String, String>,
    /// Enriched: the scale-set name for a VMSS VM (empty for a standalone VM).
    #[serde(skip)]
    pub scale_set: String,
    /// Enriched: the private/public IPs resolved from the VM's primary NIC(s).
    #[serde(skip)]
    pub ip_addresses: Vec<VmIpAddress>,
}

/// A private/public IP pair resolved from a VM's network interface. Port of
/// `machine.go`'s `vmIPAddress`.
#[derive(Debug, Default, Clone)]
pub struct VmIpAddress {
    pub public_ip: String,
    pub private_ip: String,
}

/// Port of `machine.go`'s `virtualMachineProperties`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct VirtualMachineProperties {
    #[serde(rename = "networkProfile")]
    pub network_profile: NetworkProfile,
    #[serde(rename = "osProfile")]
    pub os_profile: OsProfile,
    #[serde(rename = "storageProfile")]
    pub storage_profile: StorageProfile,
    #[serde(rename = "hardwareProfile")]
    pub hardware_profile: HardwareProfile,
}

/// Port of `machine.go`'s `hardwareProfile`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct HardwareProfile {
    #[serde(rename = "vmSize")]
    pub vm_size: String,
}

/// Port of `machine.go`'s `storageProfile`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct StorageProfile {
    #[serde(rename = "osDisk")]
    pub os_disk: OsDisk,
}

/// Port of `machine.go`'s `osDisk`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct OsDisk {
    #[serde(rename = "osType")]
    pub os_type: String,
}

/// Port of `machine.go`'s `osProfile`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct OsProfile {
    #[serde(rename = "computerName")]
    pub computer_name: String,
}

/// Port of `machine.go`'s `networkProfile`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct NetworkProfile {
    #[serde(rename = "networkInterfaces")]
    pub network_interfaces: Vec<NetworkInterfaceReference>,
}

/// Port of `machine.go`'s `networkInterfaceReference`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct NetworkInterfaceReference {
    pub id: String,
}

/// Port of `machine.go`'s `scaleSet`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ScaleSet {
    pub name: String,
    pub id: String,
}

/// A network interface. Port of `nic.go`'s `networkInterface`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct NetworkInterface {
    pub properties: NetworkProperties,
}

/// Port of `nic.go`'s `networkProperties`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct NetworkProperties {
    pub primary: bool,
    #[serde(rename = "ipConfigurations")]
    pub ip_configurations: Vec<IpConfiguration>,
}

/// Port of `nic.go`'s `ipConfiguration`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct IpConfiguration {
    pub properties: IpProperties,
}

/// Port of `nic.go`'s `ipProperties`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct IpProperties {
    #[serde(rename = "publicIPAddress")]
    pub public_ip_address: PublicIpAddress,
    #[serde(rename = "privateIPAddress")]
    pub private_ip_address: String,
}

/// Port of `nic.go`'s `publicIPAddress`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct PublicIpAddress {
    pub properties: PublicIpProperties,
}

/// Port of `nic.go`'s `publicIPProperties`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct PublicIpProperties {
    #[serde(rename = "ipAddress")]
    pub ip_address: String,
}

/// Builds the [`TargetGroup`]s for one Azure VM — one per non-empty private
/// IP, mirroring `azure.go`'s `appendMachineLabels`. Returns an empty `Vec`
/// for a VM with no resolved private IP (upstream skips it — there is no
/// address to scrape). `__address__` (private IP + `port`) is carried in the
/// group's `targets`; every `__meta_azure_*` label goes in `labels`.
/// `subscription_id`/`tenant_id`/`source` are threaded through unchanged.
pub fn append_machine_labels(
    vm: &VirtualMachine,
    subscription_id: &str,
    tenant_id: &str,
    port: u16,
    source: &str,
) -> Vec<TargetGroup> {
    let mut groups = Vec::new();
    for ips in &vm.ip_addresses {
        if ips.private_ip.is_empty() {
            continue;
        }
        let address = join_host_port(&ips.private_ip, port);
        let mut m: BTreeMap<String, String> = BTreeMap::new();
        m.insert(
            "__meta_azure_subscription_id".into(),
            subscription_id.to_string(),
        );
        m.insert("__meta_azure_machine_id".into(), vm.id.clone());
        m.insert("__meta_azure_machine_name".into(), vm.name.clone());
        m.insert("__meta_azure_machine_location".into(), vm.location.clone());
        m.insert(
            "__meta_azure_machine_private_ip".into(),
            ips.private_ip.clone(),
        );
        if !tenant_id.is_empty() {
            m.insert("__meta_azure_tenant_id".into(), tenant_id.to_string());
        }
        // /subscriptions/SUB/resourceGroups/RG/providers/PROVIDER/TYPE/NAME
        let id_path: Vec<&str> = vm.id.split('/').collect();
        if id_path.len() > 4 {
            m.insert(
                "__meta_azure_machine_resource_group".into(),
                id_path[4].to_string(),
            );
        }
        if !vm.properties.storage_profile.os_disk.os_type.is_empty() {
            m.insert(
                "__meta_azure_machine_os_type".into(),
                vm.properties.storage_profile.os_disk.os_type.clone(),
            );
        }
        if !vm.properties.os_profile.computer_name.is_empty() {
            m.insert(
                "__meta_azure_machine_computer_name".into(),
                vm.properties.os_profile.computer_name.clone(),
            );
        }
        if !ips.public_ip.is_empty() {
            m.insert(
                "__meta_azure_machine_public_ip".into(),
                ips.public_ip.clone(),
            );
        }
        if !vm.scale_set.is_empty() {
            m.insert(
                "__meta_azure_machine_scale_set".into(),
                vm.scale_set.clone(),
            );
        }
        if !vm.properties.hardware_profile.vm_size.is_empty() {
            m.insert(
                "__meta_azure_machine_size".into(),
                vm.properties.hardware_profile.vm_size.clone(),
            );
        }
        for (k, v) in &vm.tags {
            m.insert(
                sanitize_label_name(&format!("__meta_azure_machine_tag_{k}")),
                v.clone(),
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

    /// Builds the single-VM fixture from upstream `azure_test.go`
    /// (`TestAppendMachineLabels` / "single vm").
    fn single_vm() -> VirtualMachine {
        VirtualMachine {
            name: "vm-1".into(),
            id: "id-2".into(),
            type_: "Azure".into(),
            location: "eu-west-1".into(),
            properties: VirtualMachineProperties {
                os_profile: OsProfile {
                    computer_name: "test-1".into(),
                },
                storage_profile: StorageProfile {
                    os_disk: OsDisk {
                        os_type: "Linux".into(),
                    },
                },
                hardware_profile: HardwareProfile {
                    vm_size: "big".into(),
                },
                ..VirtualMachineProperties::default()
            },
            tags: [("key-1".to_string(), "value-1".to_string())].into(),
            ip_addresses: vec![VmIpAddress {
                private_ip: "10.10.10.1".into(),
                public_ip: String::new(),
            }],
            ..VirtualMachine::default()
        }
    }

    /// The label set + `__address__` must match the expected output from
    /// upstream `azure_test.go` (`appendMachineLabels(vms, 80,
    /// &SDConfig{SubscriptionID: "some-id"})`).
    #[test]
    fn builds_meta_azure_labels_matching_upstream_vector() {
        let vm = single_vm();
        let groups = append_machine_labels(&vm, "some-id", "", 80, "job/azure");
        assert_eq!(groups.len(), 1);
        let g = &groups[0];

        // __address__ is the target, private IP + port.
        assert_eq!(g.targets, vec!["10.10.10.1:80".to_string()]);
        assert!(!g.labels.contains_key("__address__"));
        assert_eq!(g.source, "job/azure");

        let l = &g.labels;
        assert_eq!(l["__meta_azure_subscription_id"], "some-id");
        assert_eq!(l["__meta_azure_machine_id"], "id-2");
        assert_eq!(l["__meta_azure_machine_name"], "vm-1");
        assert_eq!(l["__meta_azure_machine_location"], "eu-west-1");
        assert_eq!(l["__meta_azure_machine_private_ip"], "10.10.10.1");
        assert_eq!(l["__meta_azure_machine_os_type"], "Linux");
        assert_eq!(l["__meta_azure_machine_computer_name"], "test-1");
        assert_eq!(l["__meta_azure_machine_size"], "big");
        assert_eq!(l["__meta_azure_machine_tag_key_1"], "value-1");
        // Empty tenant_id, no `/` in the id (-> no resource_group), no public
        // IP, no scale set: those conditionals stay unset.
        assert!(!l.contains_key("__meta_azure_tenant_id"));
        assert!(!l.contains_key("__meta_azure_machine_resource_group"));
        assert!(!l.contains_key("__meta_azure_machine_public_ip"));
        assert!(!l.contains_key("__meta_azure_machine_scale_set"));
        // Exactly the upstream set: 5 always-present + os_type + computer_name
        // + size + 1 tag = 9 labels.
        assert_eq!(l.len(), 9, "labels={l:?}");
    }

    #[test]
    fn resource_group_public_ip_tenant_and_scale_set_conditionals() {
        let vm = VirtualMachine {
            name: "vmss-0".into(),
            id: "/subscriptions/sub/resourceGroups/my-rg/providers/Microsoft.Compute/virtualMachineScaleSets/ss/virtualMachines/0".into(),
            location: "westus".into(),
            properties: VirtualMachineProperties {
                storage_profile: StorageProfile {
                    os_disk: OsDisk { os_type: "Windows".into() },
                },
                ..VirtualMachineProperties::default()
            },
            scale_set: "ss".into(),
            ip_addresses: vec![VmIpAddress {
                private_ip: "172.20.2.4".into(),
                public_ip: "20.30.40.50".into(),
            }],
            ..VirtualMachine::default()
        };
        let groups = append_machine_labels(&vm, "sub", "tenant-1", 9100, "s");
        assert_eq!(groups.len(), 1);
        let l = &groups[0].labels;
        assert_eq!(groups[0].targets, vec!["172.20.2.4:9100".to_string()]);
        assert_eq!(l["__meta_azure_machine_resource_group"], "my-rg");
        assert_eq!(l["__meta_azure_machine_public_ip"], "20.30.40.50");
        assert_eq!(l["__meta_azure_tenant_id"], "tenant-1");
        assert_eq!(l["__meta_azure_machine_scale_set"], "ss");
    }

    #[test]
    fn skips_ip_without_private_ip_and_brackets_ipv6() {
        let vm = VirtualMachine {
            id: "id".into(),
            ip_addresses: vec![
                VmIpAddress {
                    private_ip: String::new(),
                    public_ip: "1.2.3.4".into(),
                },
                VmIpAddress {
                    private_ip: "fd00::1".into(),
                    public_ip: String::new(),
                },
            ],
            ..VirtualMachine::default()
        };
        let groups = append_machine_labels(&vm, "sub", "", 80, "s");
        // Only the private-IP entry yields a group; the IPv6 host is bracketed.
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].targets, vec!["[fd00::1]:80".to_string()]);
    }

    #[test]
    fn parses_virtual_machine_from_arm_json() {
        let json = r#"{
          "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/vm-1",
          "name": "vm-1",
          "type": "Microsoft.Compute/virtualMachines",
          "location": "eastus",
          "tags": {"env": "prod"},
          "properties": {
            "osProfile": {"computerName": "Test"},
            "storageProfile": {"osDisk": {"osType": "Windows"}},
            "hardwareProfile": {"vmSize": "Standard_D1"},
            "networkProfile": {
              "networkInterfaces": [
                {"id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Network/networkInterfaces/nic-1", "properties": {"primary": true}}
              ]
            }
          }
        }"#;
        let vm: VirtualMachine = serde_json::from_str(json).unwrap();
        assert_eq!(vm.name, "vm-1");
        assert_eq!(vm.type_, "Microsoft.Compute/virtualMachines");
        assert_eq!(vm.location, "eastus");
        assert_eq!(vm.tags["env"], "prod");
        assert_eq!(vm.properties.os_profile.computer_name, "Test");
        assert_eq!(vm.properties.storage_profile.os_disk.os_type, "Windows");
        assert_eq!(vm.properties.hardware_profile.vm_size, "Standard_D1");
        assert_eq!(vm.properties.network_profile.network_interfaces.len(), 1);
        assert_eq!(
            vm.properties.network_profile.network_interfaces[0].id,
            "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Network/networkInterfaces/nic-1"
        );
    }

    #[test]
    fn parses_nic_ip_configurations() {
        let json = r#"{
          "name": "test-nic",
          "properties": {
            "primary": true,
            "ipConfigurations": [
              {"properties": {
                "privateIPAddress": "172.20.2.4",
                "publicIPAddress": {"properties": {"ipAddress": "20.30.40.50"}},
                "primary": true
              }}
            ]
          }
        }"#;
        let nic: NetworkInterface = serde_json::from_str(json).unwrap();
        assert!(nic.properties.primary);
        assert_eq!(nic.properties.ip_configurations.len(), 1);
        let ip = &nic.properties.ip_configurations[0].properties;
        assert_eq!(ip.private_ip_address, "172.20.2.4");
        assert_eq!(ip.public_ip_address.properties.ip_address, "20.30.40.50");
    }
}
