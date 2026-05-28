//! Service discovery backends for `esm-agent`.
//!
//! v1.0 ships Tier 1 only: `static_configs` and `file_sd_configs`. Additional
//! backends (kubernetes, consul, ec2, gce, azure, dns, http, dockerswarm,
//! nomad, kuma) are scheduled for v1.x and live behind feature gates so users
//! can build minimal binaries.
