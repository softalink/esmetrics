//! Stub-server tests for [`super::Ec2Discovery`] — split out per this crate's
//! `#[path]`-sibling convention to keep `mod.rs` under the 800-line cap. Uses
//! STATIC credentials + an `endpoint` override so the test never touches
//! IMDS.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use esm_http::{Request, ResponseWriter, Server};

use super::*;

/// Polls `check` until it returns `true` or `timeout` elapses. Bounds every
/// wait so a wiring bug fails fast instead of hanging the suite.
fn wait_until(timeout: Duration, mut check: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if check() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// A running stub EC2 endpoint. `requests` records every request's `Action`.
struct Ec2Stub {
    server: Server,
    requests: Arc<Mutex<Vec<String>>>,
}

impl Ec2Stub {
    fn addr(&self) -> String {
        self.server.local_addr().to_string()
    }

    fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }

    fn stop(&self) {
        self.server.stop();
    }
}

const DESCRIBE_INSTANCES_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<DescribeInstancesResponse xmlns="http://ec2.amazonaws.com/doc/2013-10-15/">
    <requestId>req-1</requestId>
    <reservationSet>
        <item>
            <ownerId>793614593844</ownerId>
            <instancesSet>
                <item>
                    <instanceId>i-0e730b692d9c15460</instanceId>
                    <imageId>ami-0eb89db7593b5d434</imageId>
                    <instanceState><code>16</code><name>running</name></instanceState>
                    <instanceType>t2.micro</instanceType>
                    <placement><availabilityZone>us-east-1a</availabilityZone></placement>
                    <subnetId>subnet-57044c3e</subnetId>
                    <vpcId>vpc-f1eaad99</vpcId>
                    <privateIpAddress>172.31.11.152</privateIpAddress>
                    <ipAddress>3.8.232.141</ipAddress>
                    <architecture>x86_64</architecture>
                    <tagSet><item><key>env</key><value>prod</value></item></tagSet>
                    <networkInterfaceSet>
                        <item><subnetId>subnet-57044c3e</subnetId></item>
                    </networkInterfaceSet>
                </item>
            </instancesSet>
        </item>
    </reservationSet>
</DescribeInstancesResponse>
"#;

const DESCRIBE_AZS_XML: &str = r#"<DescribeAvailabilityZonesResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
    <requestId>req-2</requestId>
    <availabilityZoneInfo>
        <item><zoneName>us-east-1a</zoneName><zoneId>use1-az1</zoneId></item>
    </availabilityZoneInfo>
</DescribeAvailabilityZonesResponse>
"#;

/// Starts a stub EC2 endpoint dispatching on the `Action` query param:
/// `DescribeInstances` -> one running instance, `DescribeAvailabilityZones`
/// -> one AZ. Anything else -> 400.
fn start_ec2_stub() -> Ec2Stub {
    let server = Server::bind("127.0.0.1:0").expect("bind ec2 stub");
    let requests = Arc::new(Mutex::new(Vec::new()));
    let requests_for_handler = Arc::clone(&requests);

    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            let query = req.query().to_string();
            let action = if query.contains("Action=DescribeInstances") {
                "DescribeInstances"
            } else if query.contains("Action=DescribeAvailabilityZones") {
                "DescribeAvailabilityZones"
            } else {
                ""
            };
            requests_for_handler
                .lock()
                .unwrap()
                .push(action.to_string());

            match action {
                "DescribeInstances" => {
                    w.write_status(200);
                    w.write_body(DESCRIBE_INSTANCES_XML.as_bytes());
                }
                "DescribeAvailabilityZones" => {
                    w.write_status(200);
                    w.write_body(DESCRIBE_AZS_XML.as_bytes());
                }
                _ => w.write_status(400),
            }
        },
    ));

    Ec2Stub { server, requests }
}

/// With static creds + an `endpoint` override + `region: us-east-1`, the stub
/// must yield exactly the one instance target with its `__meta_ec2_*` labels
/// and `__address__` (private IP + configured port), then stop cleanly.
#[test]
fn discovers_ec2_instance_target() {
    let stub = start_ec2_stub();
    let cfg = Ec2SdConfig {
        region: "us-east-1".to_string(),
        endpoint: Some(format!("http://{}", stub.addr())),
        access_key: "AKIDEXAMPLE".to_string(),
        secret_key: Some("secret".to_string()),
        port: 9100,
        refresh_interval: Duration::from_millis(50),
        ..Ec2SdConfig::default()
    };

    let mut d = Ec2Discovery::new(&cfg, "job").expect("new");

    let found = wait_until(Duration::from_secs(5), || {
        d.poll().iter().any(|g| {
            g.labels.get("__meta_ec2_instance_id").map(String::as_str)
                == Some("i-0e730b692d9c15460")
        })
    });
    assert!(
        found,
        "ec2 instance target never discovered; requests={:?}",
        stub.requests()
    );

    let groups = d.poll();
    let inst = groups
        .iter()
        .find(|g| {
            g.labels.get("__meta_ec2_instance_id").map(String::as_str)
                == Some("i-0e730b692d9c15460")
        })
        .expect("instance group");
    assert_eq!(inst.targets, vec!["172.31.11.152:9100".to_string()]);
    assert_eq!(inst.labels["__meta_ec2_private_ip"], "172.31.11.152");
    assert_eq!(inst.labels["__meta_ec2_public_ip"], "3.8.232.141");
    assert_eq!(inst.labels["__meta_ec2_instance_type"], "t2.micro");
    assert_eq!(inst.labels["__meta_ec2_instance_state"], "running");
    assert_eq!(inst.labels["__meta_ec2_availability_zone"], "us-east-1a");
    // AZ id comes from the DescribeAvailabilityZones join.
    assert_eq!(inst.labels["__meta_ec2_availability_zone_id"], "use1-az1");
    assert_eq!(inst.labels["__meta_ec2_vpc_id"], "vpc-f1eaad99");
    assert_eq!(inst.labels["__meta_ec2_subnet_id"], ",subnet-57044c3e,");
    assert_eq!(inst.labels["__meta_ec2_tag_env"], "prod");
    assert_eq!(inst.labels["__meta_ec2_region"], "us-east-1");
    assert_eq!(inst.labels["__meta_ec2_owner_id"], "793614593844");
    assert_eq!(inst.source, "job/ec2/us-east-1");
    // __address__ is a target, not a label.
    assert!(!inst.labels.contains_key("__address__"));

    // Clean stop: dropping the discovery joins the refresh thread promptly.
    drop(d);
    stub.stop();
}

/// A set `role_arn` is DEFERRED and must fail `new()` with a clear message.
#[test]
fn role_arn_is_rejected_as_deferred() {
    let cfg = Ec2SdConfig {
        region: "us-east-1".to_string(),
        access_key: "AKID".to_string(),
        secret_key: Some("secret".to_string()),
        role_arn: Some("arn:aws:iam::123:role/foo".to_string()),
        ..Ec2SdConfig::default()
    };
    let err = match Ec2Discovery::new(&cfg, "job") {
        Ok(_) => panic!("role_arn must be rejected"),
        Err(e) => e,
    };
    assert!(err.msg.contains("role_arn"), "{}", err.msg);
    assert!(err.msg.contains("deferred"), "{}", err.msg);
}
