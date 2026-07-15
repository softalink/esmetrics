# esmagent

A from-scratch Rust port of upstream VictoriaMetrics
[`vmagent`](https://docs.victoriametrics.com/victoriametrics/vmagent/):
**scrape + remote-write forwarding**. It accepts metrics via any of
`esmetrics`' push ingestion protocols AND actively scrapes `/metrics`
targets it discovers itself (`-promscrape.config`); either way, every
accepted series applies an optional global relabel config, then fans out to
one or more `-remoteWrite.url` destinations (each with its own optional
per-URL relabel config), durably queued + retried per destination
independently.

> **Scope.** Pushed ingestion and active scraping share the same
> relabel -> fan-out -> durable-queue pipeline (see "Delivery pipeline"
> below). The scrape engine covers static/`file_sd`/`http_sd`/Kubernetes
> (`pod`/`node`/`service`/`ingress`/`endpoints`/`endpointslice` roles,
> Phase A+B)/Consul target discovery, per-target
> `honor_labels`/`honor_timestamps`, metric-relabel,
> `sample_limit`/`label_limit`, staleness tracking, and auto-metrics
> (`up`, `scrape_duration_seconds`, ...) — see "Scraping" below for what's
> in and the "Scraping limitations" list for what's deliberately deferred
> (the remaining non-Kubernetes cloud service discovery — Docker,
> Dockerswarm — plus Kubernetes SD's remaining auth/tuning gaps, EC2's
> deferred `role_arn`/web-identity/shared-file credential modes, GCE's
> deferred service-account key file, and Consul's blocking-query long-poll).

## Build

```bash
cargo build --release -p esmagent
```

## Usage

```bash
esmagent \
  -remoteWrite.url=http://localhost:8428/api/v1/write \
  -remoteWrite.url=http://backup-region:8428/api/v1/write \
  -remoteWrite.tmpDataPath=/var/lib/esmagent-remotewrite-data \
  -httpListenAddr=:8429
```

Any of `esmetrics`' push protocols work as the input side — e.g. Influx line
protocol:

```bash
curl -X POST 'http://localhost:8429/write' \
  --data-binary 'cpu,host=web01 usage_user=42.5,usage_system=7.1'
```

or Prometheus remote-write (`/api/v1/write`, `/api/v1/push`, and their
`/prometheus/...`-prefixed aliases). Every accepted series is forwarded to
**both** configured destinations, independently: a relabel config drops a
series from every destination's block, and each `-remoteWrite.url`
destination gets its own durable on-disk queue under
`-remoteWrite.tmpDataPath`, so one destination being down never blocks or
loses data bound for another.

### Relabeling

```yaml
# relabel.yml — drop noisy debug metrics before they reach any destination
- source_labels: [__name__]
  regex: "debug_.*"
  action: drop
```

```bash
esmagent -remoteWrite.url=http://localhost:8428/api/v1/write \
  -remoteWrite.relabelConfig=relabel.yml \
  -remoteWrite.urlRelabelConfig=per-destination-0.yml
```

`-remoteWrite.relabelConfig` applies once, globally, before fan-out.
`-remoteWrite.urlRelabelConfig` is repeatable and positional: the Nth
occurrence configures the Nth `-remoteWrite.url`'s destination-specific
relabel, applied on top of whatever the global config already let through.
See [`crates/esm-relabel/README.md`](../esm-relabel/README.md) for the
relabel engine itself (supported actions, `if:` gating).

### Scraping

Set `-promscrape.config` to enable active pull-based scraping alongside (or
instead of) push ingestion:

```yaml
# scrape.yml
global:
  scrape_interval: 30s
scrape_configs:
  - job_name: node
    static_configs:
      - targets: ['host1:9100', 'host2:9100']
    file_sd_configs:
      - files: ['targets/*.json']
    relabel_configs:
      - source_labels: [__address__]
        target_label: instance
```

```bash
esmagent -remoteWrite.url=http://localhost:8428/api/v1/write \
  -promscrape.config=scrape.yml
```

Supported service discovery (the **complete** promscrape SD surface — no SD
key is deferred): `static_configs`, `file_sd_configs`, `http_sd_configs`,
`kubernetes_sd_configs`, `consul_sd_configs`, `consulagent_sd_configs`,
`ec2_sd_configs`, `gce_sd_configs`, `azure_sd_configs`,
`digitalocean_sd_configs`, `hetzner_sd_configs`, `nomad_sd_configs`,
`marathon_sd_configs`, `vultr_sd_configs`, `puppetdb_sd_configs`,
`kuma_sd_configs`, `eureka_sd_configs`, `yandexcloud_sd_configs`,
`ovhcloud_sd_configs`, `openstack_sd_configs`, `dns_sd_configs`,
`docker_sd_configs`, and `dockerswarm_sd_configs` (each has its own
"… service discovery" section below).
Each `scrape_config` job supports `scrape_interval`/
`scrape_timeout`, `metrics_path`/`scheme`/`params`, `honor_labels`/
`honor_timestamps`, `relabel_configs` (target-relabel, runs before the
target is scraped) and `metric_relabel_configs` (runs on each scraped row),
`sample_limit`/`label_limit`, `max_scrape_size`, `basic_auth`/
`bearer_token`/`tls_config`, and `global.external_labels`. A scraped
target's series flow through the identical relabel -> fan-out -> durable
queue pipeline pushed data uses (see "Delivery pipeline" below) — one
global relabel config, one set of `-remoteWrite.url` destinations, for both
sources.

Auto-metrics (`up`, `scrape_duration_seconds`, `scrape_samples_scraped`,
`scrape_samples_post_metric_relabeling`, `scrape_series_added`,
`scrape_response_size_bytes`, `scrape_timeout_seconds`, plus
`scrape_samples_limit`/`scrape_labels_limit` when the job sets
`sample_limit`/`label_limit`) are emitted every scrape, success or failure.
A target that stops responding gets `up=0` on its next scheduled scrape
tick and a `STALE_NAN` marker for every series it was previously
contributing — both forwarded through the same pipeline as everything
else.

`GET /api/v1/targets` (JSON; optional `?state=active` or `?state=dropped`
filter) reports every discovered target's health, last scrape error, and
labels. `-promscrape.config` reloads on `SIGHUP` or, if set, every
`-promscrape.configCheckInterval`; a bad reload (unreadable file, invalid
YAML, an unbuildable job) is logged and leaves the previously-running
config untouched rather than crashing the process.

See "Scraping limitations" below for what's deliberately out of scope.

#### Kubernetes service discovery

```yaml
scrape_configs:
  - job_name: k8s-nodes
    kubernetes_sd_configs:
      - role: node
        # api_server: 'https://k8s.internal:6443'   # omit for in-cluster auth
        namespaces:
          names: [default, kube-system]
        selectors:
          - { role: node, label: "kubernetes.io/os=linux" }
    relabel_configs:
      - source_labels: [__meta_kubernetes_node_name]
        target_label: node
```

Set `kubernetes_sd_configs` on a job to discover targets from a Kubernetes
API server. Supported roles: **`pod`**, **`node`**, **`service`**,
**`ingress`**, **`endpoints`**, **`endpointslice`**. Auth is either
**in-cluster** (leave `api_server` unset —
the projected service-account token + cluster CA are auto-discovered from
the standard `/var/run/secrets/kubernetes.io/serviceaccount/*` paths and
`KUBERNETES_SERVICE_HOST`/`_PORT` env vars, with the token file re-read on
every request so a rotated token is picked up), an **explicit
`api_server`** URL with inline `basic_auth`/`bearer_token` and
`tls_config`, or a **`kubeconfig_file`** whose `current-context` supplies the
server, TLS material (CA / client cert+key, from file paths or inline
base64 `*-data`), and token/token-file/basic auth — a cluster `proxy-url` in
the kubeconfig is honored (`api_server` and `kubeconfig_file` are mutually
exclusive). Targets are discovered via a **list+watch** client backed by
an in-memory per-role cache: an initial paginated LIST seeds the cache,
then a long-lived WATCH stream applies `ADDED`/`MODIFIED`/`DELETED` events
incrementally; the watch resumes from the last-seen `resourceVersion`
after a clean stream close, and re-LISTs promptly on a `410 Gone` ("too old
resourceVersion") event. `namespaces.names`/`namespaces.own_namespace`
restrict which namespace(s) are watched (ignored for the cluster-scoped
`node` role); `selectors` apply `labelSelector`/`fieldSelector` filtering
per role at the API-server level. Each role emits its own
`__meta_kubernetes_*` label family (e.g. `__meta_kubernetes_node_name`/
`__meta_kubernetes_node_label_*`/`__meta_kubernetes_node_address_*` for
`node`; `__meta_kubernetes_pod_name`/`__meta_kubernetes_pod_container_*`/
etc. for `pod`), consumable by `relabel_configs` exactly like any other
discovery source's labels. The `endpoints` and `endpointslice` roles join
across kinds through a shared cross-role object cache — the same-named
`Service` and each address's `targetRef` `Pod` — and `attach_metadata:
{node}` / `{namespace}` joins the resolved `Node` / `Namespace` labels onto
`pod`/`endpoints`/`endpointslice` (and `namespace` onto `service`/`ingress`)
targets. The global `-promscrape.kubernetes.attachNodeMetadataAll` /
`attachNamespaceMetadataAll` flags set the default `attach_metadata` for every
`kubernetes_sd_config` (a per-config `attach_metadata` fully overrides them),
and a standalone `proxy_url` SDConfig field routes the explicit-`api_server`
and in-cluster API clients through an HTTP proxy.

**Kubernetes SD limitations** (Phase A+B; documented here rather than left
to be discovered):

- **Auth modes supported:** in-cluster (service-account token + cluster CA),
  explicit `api_server` (inline bearer/basic + TLS), `kubeconfig_file`
  (including its cluster `proxy-url`), the standalone `proxy_url` SDConfig
  field (applied to the explicit-`api_server` and in-cluster API clients), and
  **OAuth2 client-credentials** (`oauth2:` block — `client_id`, `client_secret`
  or `client_secret_file`, `scopes`, `token_url`, `endpoint_params`, plus a
  `tls_config`/`proxy_url` for the token endpoint). OAuth2 fetches a token via
  the client-credentials grant, caches it until shortly before `expires_in`,
  and attaches it as a bearer token on API requests (taking precedence over the
  static bearer/basic chain). Residual OAuth2 deferrals: the `headers` field
  for the token request, and autodetect of the credential style (this port
  always sends `client_id`/`client_secret` in the POST body,
  `AuthStyleInParams`). The kubeconfig `exec` credential-plugin and `act-as*`
  impersonation auth are rejected.
- **Per-config watchers, no cross-config dedup** — upstream's `groupWatcher`
  shares one watch per (role, namespace, selector) across every
  `kubernetes_sd_config` in a job; this port starts an independent watcher
  per config, so two configs watching the same resource issue two watches.
- **`tls_config.server_name`** (SNI override) is parsed but **not applied**
  — same `reqwest::blocking` limitation as every other TLS config in this
  crate (see `client::build_client`'s doc).
- **The watch `timeoutSeconds` is a fixed 60s** — there is no separate
  `-promscrape.kubernetes.apiServerTimeout` knob.

### Consul service discovery

```yaml
scrape_configs:
  - job_name: consul
    consul_sd_configs:
      - server: 'localhost:8500'      # default; scheme prepended (https if TLS)
        # datacenter: dc1              # omit to resolve via /v1/agent/self
        services: [web, db]           # allowlist (empty = all services)
        tags: [prod]                  # keep only services carrying every tag
        # token: <acl-token>          # or CONSUL_HTTP_TOKEN[_FILE] env
    relabel_configs:
      - source_labels: [__meta_consul_service]
        target_label: service
```

Set `consul_sd_configs` on a job to discover targets from a Consul agent.
Each config queries `/v1/catalog/services` for the service list (filtered by
the `services` allowlist and required `tags`) and `/v1/health/service/<svc>`
for each kept service's nodes, emitting one target per node with the full
`__meta_consul_*` label set (`__meta_consul_service`, `_node`, `_dc`,
`_health`, `_service_address`, `_service_port`, `_tags` + per-tag
`_tag_<name>`/`_tagpresent_<name>`, node/service `_metadata_*`, and
`_tagged_address_*`). Auth: `token` (or `CONSUL_HTTP_TOKEN_FILE` /
`CONSUL_HTTP_TOKEN` env) becomes a bearer token; `username`/`password` or an
inline `basic_auth` becomes HTTP basic; a `tls_config` applies CA / client
cert / `insecure_skip_verify`. Enterprise `namespace` (falls back to the
`CONSUL_NAMESPACE` env var when unset, matching the token's env fallback)
/`partition`, `node_meta` filters, a `filter` expression, `tag_separator`
(default `,`),
and `allow_stale` (default on) are all honored. The refresh interval comes
from `-promscrape.consulSDCheckInterval` (default 30s), not a per-config YAML
field. A Consul agent that is down at startup does not fail startup —
discovery retries on its background thread and reports no targets until the
first successful list. **Deviation:** this port re-lists every refresh
interval rather than using Consul's blocking-query long-poll (`?index=&wait=`)
— see "Scraping limitations".

### Consul Agent service discovery

```yaml
scrape_configs:
  - job_name: consulagent
    consulagent_sd_configs:
      - server: 'localhost:8500'      # default; scheme prepended (https if TLS)
        # datacenter: dc1             # omit to resolve via /v1/agent/self
        services: [web, db]           # allowlist (empty = all services)
        # filter: 'Service == "web"'  # Consul agent filter expression
        # token: <acl-token>          # or CONSUL_HTTP_TOKEN[_FILE] env
    relabel_configs:
      - source_labels: [__meta_consulagent_service]
        target_label: service
```

Set `consulagent_sd_configs` on a job to discover targets from the **local**
Consul agent (as opposed to `consul_sd_configs`, which queries the cluster
catalog). Each config queries `/v1/agent/self` for the agent's
datacenter/node/member-address/metadata, `/v1/agent/services` for the
registered service list (filtered to the resolved datacenter, the `services`
allowlist, and the optional `filter` expression), and
`/v1/agent/health/service/name/<svc>` for each kept service's nodes, emitting
one target per node with the full `__meta_consulagent_*` label set
(`__meta_consulagent_service`, `_node`, `_dc`, `_address` and `_health` from
the local agent, `_service_address`, `_service_id`, `_service_port`,
`_namespace`, `_tags` + per-tag `_tag_<name>`/`_tagpresent_<name>`, agent/
service `_metadata_*`, and node/service `_tagged_address_*`). Unlike
`consul_sd_configs`, there is no `partition`/`tags`/`node_meta`/`allow_stale`
support and no `__meta_consulagent_partition` label — matching upstream's
consulagent SD. Auth: `token` (or `CONSUL_HTTP_TOKEN_FILE` /
`CONSUL_HTTP_TOKEN` env) becomes a bearer token; `username`/`password` or an
inline `basic_auth` becomes HTTP basic; a `tls_config` applies CA / client
cert / `insecure_skip_verify`. Enterprise `namespace` (falls back to the
`CONSUL_NAMESPACE` env var when unset) and `tag_separator` (default `,`) are
honored. The refresh interval comes from
`-promscrape.consulagentSDCheckInterval` (default 30s), not a per-config YAML
field. An agent that is down at startup does not fail startup — discovery
retries on its background thread and reports no targets until the first
successful list. **Deviation:** this port re-lists every refresh interval
rather than running one long-poll goroutine per service.

### EC2 service discovery

```yaml
scrape_configs:
  - job_name: ec2
    ec2_sd_configs:
      - region: us-east-1            # omit to resolve via IMDS / AWS_REGION
        # access_key: AKID           # or AWS_ACCESS_KEY_ID env, or IMDS role
        # secret_key: SECRET         # or AWS_SECRET_ACCESS_KEY env, or IMDS role
        port: 9100                   # default 80; used for __address__
        filters:
          - name: instance-state-name
            values: [running]
    relabel_configs:
      - source_labels: [__meta_ec2_tag_Name]
        target_label: instance
```

Set `ec2_sd_configs` on a job to discover EC2 instances. Each config
`DescribeInstances` (paginating on `nextToken`, applying `filters` as
`Filter.N.*` query params) and emits one target per instance that has a
private IP, with `__address__` = private IP + `port` and the full
`__meta_ec2_*` label set (`_instance_id`, `_instance_state`,
`_instance_type`, `_instance_lifecycle`, `_ami`, `_architecture`,
`_availability_zone`, `_availability_zone_id`, `_owner_id`, `_platform`,
`_private_ip`/`_private_dns_name`, `_public_ip`/`_public_dns_name`,
`_primary_subnet_id`, `_subnet_id` (comma-wrapped, deduplicated),
`_ipv6_addresses`, `_vpc_id`, `_region`, and per-tag `_tag_<key>`). The
`_availability_zone_id` label is joined from a best-effort
`DescribeAvailabilityZones` call (unset if that call fails). Requests are
signed with **AWS Signature V4**.

**Credentials** are resolved in order (first that yields keys wins): static
`access_key`/`secret_key` (+ optional `session_token`) from the config;
`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` (+ optional `AWS_SESSION_TOKEN`)
from the environment; the **IMDSv2 instance role** (token PUT → role list →
role credentials, cached until near expiration, every IMDS request bounded by
a short 2s timeout so a non-AWS host fails fast). The `region` comes from the
config, else `AWS_REGION`, else IMDS. A custom `endpoint` overrides the
default `https://ec2.<region>.amazonaws.com/`. An EC2 endpoint that is down
at startup does not fail startup — discovery retries on its background thread.
The refresh interval comes from `-promscrape.ec2SDCheckInterval` (default
60s), not a per-config YAML field.

**EC2 SD limitations** (credential subset — documented here rather than left
to be discovered): STS **`role_arn`** (`AssumeRole`) is DEFERRED and rejected
at config build time with a clear "unsupported (deferred): role_arn" error;
the **web-identity token file** (`AWS_WEB_IDENTITY_TOKEN_FILE`) and the shared
**`~/.aws` config/credentials files** (and `profile`) are not ported. Query
API version `2016-11-15` is used, matching upstream.

### GCE service discovery

```yaml
scrape_configs:
  - job_name: gce
    gce_sd_configs:
      - project: my-project           # optional; auto-detected on GCE
        zone: [us-east1-b, us-east1-c] # a single zone, a list, or "*" (all)
        filter: 'status = RUNNING'     # optional GCE list filter
        port: 9100                     # default 80
        tag_separator: ','             # default ","
        # bearer_token: <token>        # optional static Compute API token
    relabel_configs:
      - source_labels: [__meta_gce_label_env]
        regex: prod
        action: keep
```

Set `gce_sd_configs` on a job to discover Google Compute Engine instances.
Each config lists instances per zone via the Compute API
(`.../projects/<project>/zones/<zone>/instances`, paginated on
`nextPageToken`); each instance's first network interface becomes one target
(`__address__` = its `networkIP` + `port`) carrying the full `__meta_gce_*`
label set (`_instance_id`, `_instance_name`, `_instance_status`,
`_machine_type`, `_network`, `_private_ip`, `_project`, `_subnetwork`,
`_zone`, per-interface `_interface_ipv4_<name>`, comma-wrapped `_tags`,
sanitized `_metadata_<k>` / `_label_<k>`, and the conditional `_public_ip` /
`_public_ipv6` / `_internal_ipv6`).

`project` and `zone` default to auto-detection via the GCE metadata server
(`.../project/project-id`, `.../instance/zone`) when unset; `zone: '*'` lists
every zone for the project. **Auth** is scoped to two modes: a static
`bearer_token` from the config (wins when set), else the GCE
**metadata-server access token**
(`.../instance/service-accounts/default/token` with `Metadata-Flavor:
Google`, cached to its `expires_in`). A GCE/metadata endpoint that is down at
startup does not fail startup — discovery retries on its background thread.
The refresh interval comes from `-promscrape.gceSDCheckInterval` (default
60s), not a per-config YAML field.

**GCE SD limitations:** the **service-account JSON key file**
(`credentials_file` / `GOOGLE_APPLICATION_CREDENTIALS`, the RS256-JWT ->
token-exchange flow) is DEFERRED and rejected at config build time with a
clear "unsupported (deferred): service-account key file (credentials_file)"
error; use `bearer_token` or run on GCE (metadata token) instead.

### Azure service discovery

```yaml
scrape_configs:
  - job_name: azure
    azure_sd_configs:
      - subscription_id: <sub>          # required
        authentication_method: OAuth    # OAuth (default) or ManagedIdentity
        tenant_id: <tenant>             # required for OAuth
        client_id: <client>             # required for OAuth
        client_secret: <secret>         # required for OAuth
        environment: AzureCloud         # default; see below
        resource_group: my-rg           # optional; scopes the listing
        port: 9100                      # default 80
    relabel_configs:
      - source_labels: [__meta_azure_machine_tag_env]
        regex: prod
        action: keep
```

Set `azure_sd_configs` on a job to discover Azure virtual machines (standalone
and VM-scale-set VMs). Each config lists VMs via the ARM API
(`/subscriptions/<sub>/providers/Microsoft.Compute/virtualMachines`, plus the
scale-set VMs, paginated on `nextLink`), resolves each VM's primary network
interface for its private/public IP, and emits one target per private IP
(`__address__` = private IP + `port`) carrying the `__meta_azure_*` label set
(`_subscription_id`, `_machine_id`, `_machine_name`, `_machine_location`,
`_machine_private_ip`, and the conditional `_tenant_id`,
`_machine_resource_group`, `_machine_os_type`, `_machine_computer_name`,
`_machine_public_ip`, `_machine_scale_set`, `_machine_size`, and sanitized
`_machine_tag_<k>`).

**Both `authentication_method`s are supported:** `OAuth` (the default —
`client_id`/`client_secret`/`tenant_id` are exchanged for a bearer token at the
Active Directory endpoint) and `ManagedIdentity` (a token from the Azure IMDS,
`http://169.254.169.254/...`, with a short timeout so a non-Azure host fails
fast). The token is cached until shortly before it expires. **Cloud
environments** `AzureCloud`/`AzurePublicCloud`, `AzureChinaCloud`,
`AzureGermanCloud`, and `AzureUSGovernment` select the AD + Resource Manager
endpoints (`AzureStackCloud`'s file-based endpoints are not ported). An Azure
endpoint that is down at startup does not fail startup — discovery retries on
its background thread. The refresh interval comes from
`-promscrape.azureSDCheckInterval` (default 60s), not a per-config YAML field.

**Azure SD limitations:** a bad `authentication_method` (anything other than
`OAuth`/`ManagedIdentity`) and, for `OAuth`, missing
`tenant_id`/`client_id`/`client_secret` are rejected at config build time. The
NIC resolution runs sequentially (a simplification of upstream's worker pool),
and `AzureStackCloud`'s file-sourced endpoints are not ported.

### DigitalOcean service discovery

```yaml
scrape_configs:
  - job_name: digitalocean
    digitalocean_sd_configs:
      - bearer_token: dop_v1_...       # DigitalOcean API token
        port: 9100                     # default 80; used for __address__
        # server: https://api.digitalocean.com   # default endpoint
    relabel_configs:
      - source_labels: [__meta_digitalocean_tags]
        regex: '.*,monitored,.*'
        action: keep
```

Set `digitalocean_sd_configs` on a job to discover DigitalOcean droplets.
Each config pages through `/v2/droplets` (following the `links.pages.next`
cursor until exhausted) and emits one target per droplet that has an IPv4
network, with `__address__` = the droplet's **public IPv4** + `port` and the
full `__meta_digitalocean_*` label set (`_droplet_id`, `_droplet_name`,
`_image`/`_image_name`, `_private_ipv4`, `_public_ipv4`, `_public_ipv6`,
`_region`, `_size`, `_status`, `_vpc`, and — only when non-empty —
`_features` and `_tags`, each comma-wrapped like `,a,b,`). Auth is a
`bearer_token` sent as `Authorization: Bearer`. A custom `server` overrides
the default `https://api.digitalocean.com` endpoint. A DigitalOcean API that
is down at startup does not fail startup — discovery retries on its background
thread. The refresh interval comes from
`-promscrape.digitaloceanSDCheckInterval` (default 60s), not a per-config YAML
field.

### Hetzner service discovery

```yaml
scrape_configs:
  - job_name: hetzner-hcloud
    hetzner_sd_configs:
      - role: hcloud                 # Hetzner Cloud API
        bearer_token: <hcloud-token> # required for role: hcloud
        port: 9100                   # default 80; used for __address__
    relabel_configs:
      - source_labels: [__meta_hetzner_hcloud_server_type]
        regex: cx11
        action: keep
  - job_name: hetzner-robot
    hetzner_sd_configs:
      - role: robot                  # Hetzner Robot (dedicated servers) API
        basic_auth:                  # required for role: robot
          username: <webservice-user>
          password: <webservice-password>
```

Set `hetzner_sd_configs` on a job to discover Hetzner servers. `role` is
required and must be `hcloud` or `robot` (any other value is rejected at
config-parse time). `port` defaults to 80.

- **`role: hcloud`** — Bearer-auth (`bearer_token`), pages through
  `/v1/servers` **and** `/v1/networks` (following `meta.pagination.next_page`)
  against `https://api.hetzner.cloud`. One target per server, `__address__` =
  the server's **public IPv4** + `port`, with the common `__meta_hetzner_*`
  labels (`_role`, `_server_id`, `_server_name`, `_server_status`,
  `_public_ipv4`, `_public_ipv6_network`, `_datacenter`) plus the
  `__meta_hetzner_hcloud_*` set (`_location`/`_location_network_zone` and their
  deprecated `_datacenter_location*` aliases, `_server_type`, `_cpu_cores`,
  `_cpu_type`, `_memory_size_gb`, `_disk_size_gb`, `_image_*` when an image is
  attached, sanitized `_private_ipv4_<network>` joined against the networks
  list, and sanitized `_label_<k>`/`_labelpresent_<k>` per server label).
- **`role: robot`** — HTTP-Basic (`basic_auth`), a single `/server` GET
  against `https://robot-ws.your-server.de`. One target per dedicated server,
  `__address__` = the server's **public IPv4** + `port`, with the common
  `__meta_hetzner_*` labels plus `__meta_hetzner_robot_*`
  (`_datacenter` (lowercased), `_product`, `_cancelled`); the first IPv6
  subnet, when present, becomes `_public_ipv6_network` as `ip/mask`.

A Hetzner API that is down at startup does not fail startup — discovery
retries on its background thread. The refresh interval comes from
`-promscrape.hetznerSDCheckInterval` (default 60s), not a per-config YAML
field.

### Nomad service discovery

```yaml
scrape_configs:
  - job_name: nomad
    nomad_sd_configs:
      - server: localhost:4646         # default; or NOMAD_ADDR
        # namespace: default           # or NOMAD_NAMESPACE
        # region: global               # default; or NOMAD_REGION
        # bearer_token: <token>        # or NOMAD_TOKEN env var
    relabel_configs:
      - source_labels: [__meta_nomad_tags]
        regex: '.*,monitored,.*'
        action: keep
```

Set `nomad_sd_configs` on a job to discover targets from a HashiCorp Nomad
agent's service catalog. Each config lists `/v1/services`, then fetches each
service's registrations from `/v1/service/<name>`, emitting one target per
registration with `__address__` = the registration's `Address` + `Port` and
the full `__meta_nomad_*` label set (`_service`, `_service_id`,
`_service_address`, `_service_port`, `_service_alloc_id`, `_service_job_id`,
`_address`, `_dc`, `_namespace`, `_node_id`, plus per-tag `_tag_<k>` /
`_tagpresent_<k>` and the comma-wrapped `_tags`). Auth resolves the token from
the `NOMAD_TOKEN` env var or an inline `bearer_token` (setting both is
rejected) and sends it as `Authorization: Bearer` (matching upstream vmagent —
not Nomad's native `X-Nomad-Token` header); inline `basic_auth` and
`tls_config` are also supported. `server` defaults to `NOMAD_ADDR`, else
`localhost:4646`; `region` defaults to `NOMAD_REGION`, else `global`;
`namespace` defaults to `NOMAD_NAMESPACE`. `allow_stale` is honored (adds
`&stale`, on by default). A Nomad agent that is down at startup does not fail
startup — discovery retries on its background thread. The refresh interval
comes from `-promscrape.nomadSDCheckInterval` (default 30s), not a per-config
YAML field; this port re-lists on that interval rather than using Nomad's
blocking-query long-poll (`?index=&wait=`), so `-promscrape.nomad.waitTime`
has no analog.

### Vultr service discovery

```yaml
scrape_configs:
  - job_name: vultr
    vultr_sd_configs:
      - bearer_token: <vultr-api-key>   # required
        port: 9100                      # default 80; used for __address__
        # server: https://api.vultr.com # default endpoint
    relabel_configs:
      - source_labels: [__meta_vultr_instance_tags]
        regex: '.*,monitored,.*'
        action: keep
```

Set `vultr_sd_configs` on a job to discover Vultr instances (VPS). Each config
pages through `/v2/instances?per_page=100` (following the opaque
`meta.links.next` cursor until it is empty) and emits one target per instance,
with `__address__` = the instance's `main_ip` + `port` and the full
`__meta_vultr_*` label set (`_instance_id`, `_instance_label`, `_instance_os`,
`_instance_os_id`, `_instance_region`, `_instance_plan`, `_instance_main_ip`,
`_instance_internal_ip`, `_instance_main_ipv6`, `_instance_hostname`,
`_instance_server_status`, `_instance_vcpu_count`, `_instance_ram_mb`,
`_instance_allowed_bandwidth_gb`, `_instance_disk_gb`, and — only when
non-empty — `_instance_features` and `_instance_tags`, each comma-wrapped like
`,a,b,`). Auth is a `bearer_token` (Vultr API key) sent as
`Authorization: Bearer`. A custom `server` overrides the default
`https://api.vultr.com` endpoint — this `server` field is an esmagent
extension not present in upstream's Vultr `SDConfig` (which hardcodes the
endpoint); it defaults to the real API and mainly exists to let tests point at
a stub, same as the EC2/GCE/Azure endpoint overrides. A Vultr API that is down
at startup does not fail startup — discovery retries on its background
thread. The refresh interval comes from `-promscrape.vultrSDCheckInterval`
(default 30s), not a per-config YAML field. Vultr's API-side filter query
params (`label`/`main_ip`/`region`/`firewall_group_id`/`hostname`) are not
ported — filter via `relabel_configs` instead.

### PuppetDB service discovery

```yaml
scrape_configs:
  - job_name: puppetdb
    puppetdb_sd_configs:
      - url: https://puppetdb.example.com   # required (http/https, with a host)
        query: 'resources { type = "Class" and title = "Prometheus::Node_exporter" }'  # required PQL
        include_parameters: false           # default false; enabling can leak secrets
        port: 9100                          # default 80; used for __address__
        # bearer_token / basic_auth / tls_config are supported
    relabel_configs:
      - source_labels: [__meta_puppetdb_certname]
        target_label: instance
```

Set `puppetdb_sd_configs` on a job to discover targets from a PuppetDB
instance. Each config issues a single `POST <url>/pdb/query/v4` with a JSON
`{"query": "<pql>"}` body (the `query` is a PuppetDB PQL expression) and emits
one target per returned resource, with `__address__` = the resource's
`certname` + `port` and the `__meta_puppetdb_*` label set: `_query`,
`_certname`, `_environment`, `_exported`, `_file`, `_resource`, `_title`,
`_type`, and — only when the resource has tags — `_tags` (comma-wrapped like
`,a,b,`). Auth is a `bearer_token` (`Authorization: Bearer`) or `basic_auth`,
with `tls_config` for the transport.

`include_parameters` (default `false`) additionally emits a sanitized
`__meta_puppetdb_parameter_<key>` label per resource parameter — list-valued
params are comma-joined, nested objects are flattened as
`__meta_puppetdb_parameter_<obj>_<key>`, bool/number params are stringified,
and empty or unrepresentable params are dropped. It defaults off because a
resource's parameters can expose secrets. A PuppetDB that is down at startup
does not fail startup — discovery retries on its background thread. The refresh
interval comes from `-promscrape.puppetdbSDCheckInterval` (default 30s), not a
per-config YAML field.

### Kuma service discovery

```yaml
scrape_configs:
  - job_name: kuma
    kuma_sd_configs:
      - server: https://kuma-control-plane:5676   # required (MADS endpoint; http:// assumed if no scheme)
        client_id: my-agent                        # optional; defaults to hostname, else "vmagent"
        # bearer_token / basic_auth / tls_config are supported
    relabel_configs:
      - source_labels: [__meta_kuma_service]
        target_label: service
```

Set `kuma_sd_configs` on a job to discover targets from a Kuma
(service-mesh) control plane's Monitoring Assignment Discovery Service (MADS).
Each config issues a single `POST <server>/v3/discovery:monitoringassignments`
with an xDS `DiscoveryRequest` JSON body (`node.id` = `client_id`, the
`MonitoringAssignment` `type_url`) — any base path and query on `server` are
preserved before the MADS suffix is appended. It emits one target per
assignment target, with `__address__` = the target's address and the label
set: `instance` (= target name), `__scheme__`, `__metrics_path__`,
`__meta_kuma_dataplane` (= target name), `__meta_kuma_mesh`,
`__meta_kuma_service`, and `__meta_kuma_label_<key>` (sanitized) for each
assignment- and target-level label (target-level wins on a collision). Auth is
a `bearer_token` (`Authorization: Bearer`) or `basic_auth`, with `tls_config`
for the transport. A control plane that is down at startup does not fail
startup — discovery retries on its background thread. The refresh interval
comes from `-promscrape.kumaSDCheckInterval` (default 30s), not a per-config
YAML field.

### Marathon service discovery

```yaml
scrape_configs:
  - job_name: marathon
    marathon_sd_configs:
      - servers:
          - https://marathon1:8080   # required; tried in order
          - https://marathon2:8080
        bearer_token: <marathon-token> # optional; or basic_auth / tls_config
    relabel_configs:
      - source_labels: [__meta_marathon_app_label_prometheus]
        regex: 'true'
        action: keep
```

Set `marathon_sd_configs` on a job to discover targets from a Marathon
(Mesosphere) cluster. Each config lists Marathon `servers` (tried in order
until one responds) and queries `/v2/apps/?embed=apps.tasks`, emitting one
target per app task per port. `__address__` is the task host (or, under
container networking, the task's first container IP) joined with the selected
port; ports come from `container.portMappings`, the legacy
`container.docker.portMappings`, or `portDefinitions`, falling back to the
task's own `ports` for host networking. The full `__meta_marathon_*` label set
is emitted: `_app`, `_image`, `_task`, `_port_index`, sanitized
`_app_label_<k>`, and — per port — sanitized `_port_mapping_label_<k>` or
`_port_definition_label_<k>`.

A task exposing **multiple ports** yields one target per (task, port) here. This
diverges from upstream, whose duplicate-key `Labels` + `RemoveDuplicates`
pipeline collapses a multi-port task to a *single* target (with `__address__`
from the first port but the surviving `_port_index`/port-labels from the last).
The common one-port-per-task case is identical; multi-port tasks are scraped on
every port here (arguably more useful, and free of upstream's
first-address/last-labels mismatch).

Auth is `bearer_token` (sent as `Authorization: Bearer`) or `basic_auth`, plus
`tls_config`. Note: upstream VictoriaMetrics v1.146.0's `marathon_sd_config`
authenticates only via this HTTP-client config — it has no `auth_token` field
and sends no Marathon-specific `Authorization: token=` header (that was
Prometheus's older shape), so esmagent matches upstream and does not accept
`auth_token`. One further deviation from upstream: it picks a single random
server per refresh with no failover, whereas esmagent tries each `server` in
order (strictly more robust; identical for a one-server config). A Marathon
server that is down at startup does not fail startup — discovery retries on its
background thread. The refresh interval comes from
`-promscrape.marathonSDCheckInterval` (default 30s), not a per-config YAML
field.

### Eureka service discovery

```yaml
scrape_configs:
  - job_name: eureka
    eureka_sd_configs:
      - server: http://eureka.example:8080/eureka/v2  # default localhost:8080/eureka/v2
        # bearer_token / basic_auth / tls_config are supported
    relabel_configs:
      - source_labels: [__meta_eureka_app_instance_status]
        regex: UP
        action: keep
```

Set `eureka_sd_configs` on a job to discover targets from a Netflix Eureka
registry. Each config issues a single `GET <server>/apps` (with
`Accept: application/xml`) and emits one target per registered instance, with
`__address__` = the instance's `hostName` + port (the instance's `<port>`,
falling back to `80`) and the `__meta_eureka_*` label set: `_app_name`,
`_app_instance_id`/`_hostname`/`_ip_addr`/`_vip_address`/`_secure_vip_address`/
`_status`/`_country_id`/`_homepage_url`/`_statuspage_url`/`_healthcheck_url`,
the `_port`/`_port_enabled` (and `_secure_port`/`_secure_port_enabled`) pairs
when present, the `_datacenterinfo_name` (+ sanitized
`_datacenterinfo_metadata_<k>`) when a datacenter is set, and a sanitized
`_metadata_<k>` per instance-metadata entry. The `instance` label is overridden
with the Eureka instance id. Auth is a `bearer_token`
(`Authorization: Bearer`) or `basic_auth`, with `tls_config` for the transport.
A Eureka server that is down at startup does not fail startup — discovery
retries on its background thread. The refresh interval comes from
`-promscrape.eurekaSDCheckInterval` (default 30s), not a per-config YAML field.

### Yandex Cloud service discovery

```yaml
scrape_configs:
  - job_name: yandexcloud
    yandexcloud_sd_configs:
      - service: compute                       # required; only `compute` supported
        yandex_passport_oauth_token: <token>   # optional; else the metadata IAM token
        # api_endpoint: https://api.cloud.yandex.net  # default
        # folder_ids: [b1g...]                  # optional; else org -> cloud -> folder enumeration
        # tls_config: { ... }
    relabel_configs:
      - source_labels: [__meta_yandexcloud_instance_status]
        regex: RUNNING
        action: keep
```

Set `yandexcloud_sd_configs` on a job to discover Yandex Cloud compute
instances. Each config resolves the per-service endpoints from
`GET <api_endpoint>/endpoints`, then either lists instances directly for the
configured `folder_ids` or enumerates organizations -> clouds -> folders via the
resource-manager API and lists instances per folder
(`/compute/v1/instances?folderId=...`, paginated on `nextPageToken`). Each
instance becomes one target whose `__address__` is the instance **FQDN** (no
port is appended), carrying the full `__meta_yandexcloud_*` label set
(`_instance_name`, `_instance_fqdn`, `_instance_id`, `_instance_status`,
`_instance_platform_id`, `_instance_resources_cores` / `_core_fraction` /
`_memory`, `_folder_id`, sanitized `_instance_label_<k>`, per-interface
`_instance_private_ip_<index>` and the conditional `_instance_public_ip_<index>`,
and `_instance_private_dns_<n>` / `_instance_public_dns_<n>`).

**Auth** is scoped to two modes: a static `yandex_passport_oauth_token` (wins
when set) exchanged for an IAM token at `<iam>/iam/v1/tokens`, else the compute
**metadata-server IAM token** (`GET
http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token`
with `Metadata-Flavor: Google`, cached to its `expires_in`). The IAM token is
cached until shortly before it expires. A Yandex Cloud / metadata endpoint that
is down at startup does not fail startup — discovery retries on its background
thread. The refresh interval comes from
`-promscrape.yandexcloudSDCheckInterval` (default 30s), not a per-config YAML
field.

**Yandex Cloud SD limitations:** `service` must be `compute` (rejected at config
build time otherwise). The **service-account authorized-key JSON** (the
JWT -> IAM-token-exchange flow, `service_account_key_file`) is DEFERRED and
rejected at config build time with a clear "unsupported (deferred):
service-account key" error; use `yandex_passport_oauth_token` or run in Yandex
Cloud (metadata token) instead. Upstream's disabled EC2 IMDSv1 credential
fallback is not ported.

### OVHcloud service discovery

```yaml
scrape_configs:
  - job_name: ovhcloud
    ovhcloud_sd_configs:
      - service: vps                 # required; `vps` or `dedicated_server`
        application_key: <key>
        application_secret: <secret>
        consumer_key: <consumer-key>
        # endpoint: ovh-eu           # default; OVH region (ovh-eu/ovh-ca/ovh-us/
        #                            #   kimsufi-eu/kimsufi-ca/soyoustart-eu/soyoustart-ca)
    relabel_configs:
      - source_labels: [__meta_ovhcloud_vps_state]
        regex: running
        action: keep
```

Set `ovhcloud_sd_configs` on a job to discover OVHcloud **VPS** or **dedicated
servers**. Each config resolves the `endpoint` region name to its API base URL,
signs every request with the OVH scheme (an `X-Ovh-Signature` header of
`"$1$" + sha1(application_secret+consumer_key+GET+<url>++<timestamp>)`, with the
timestamp corrected by the server clock fetched from `/auth/time`), lists the
instances (`GET /vps` or `GET /dedicated/server`), then fetches each instance's
detail plus its IPs (`.../ips`). Each instance becomes one target whose
`__address__` is its default IP — the IPv4 if present, else the IPv6, **with no
port appended** (matching upstream) — carrying the `instance` label (the
instance name) and the full `__meta_ovhcloud_vps_*` or
`__meta_ovhcloud_dedicated_server_*` label set.

An OVH API that is down at startup does not fail startup — discovery retries on
its background thread; a per-instance detail failure logs and skips that
instance without dropping the rest. The refresh interval comes from
`-promscrape.ovhcloudSDCheckInterval` (default 30s), not a per-config YAML
field.

**OVHcloud SD limitations:** `service` must be `vps` or `dedicated_server`, and
`endpoint` must be one of the seven known OVH regions — both are rejected at
config build time otherwise. Upstream's inline `HTTPClientConfig` / `proxy_url`
knobs are not ported.

### OpenStack service discovery

```yaml
scrape_configs:
  - job_name: openstack
    openstack_sd_configs:
      - identity_endpoint: http://keystone:5000/v3   # Keystone v3 base URL
        role: instance                # required; `instance` or `hypervisor`
        region: RegionOne
        # password auth:
        username: admin
        password: <secret>
        domain_name: default
        project_name: admin
        # or application-credential auth:
        # application_credential_id: <id>
        # application_credential_secret: <secret>
        # all_tenants: false          # instance role only
        # availability: public        # public | internal | admin (default public)
        # port: 80                    # appended to the discovered IP
    relabel_configs:
      - source_labels: [__meta_openstack_instance_status]
        regex: ACTIVE
        action: keep
```

Set `openstack_sd_configs` on a job to discover OpenStack Nova **instances**
(`role: instance`) or **hypervisors** (`role: hypervisor`). Each config
authenticates against Keystone v3 (`POST <identity_endpoint>/auth/tokens`),
reads the returned `X-Subject-Token` plus the service catalog to resolve the
Nova/compute endpoint for the configured `region` + `availability`, then lists
targets (paginated `servers/detail` or `os-hypervisors/detail`). The token +
compute URL are cached until the token's `expires_at`, with a 401-triggered
re-auth as a safety net.

- **instance role** — one target per server address (per pool), `__address__`
  is the fixed IP + `port`; a pool's floating IP becomes
  `__meta_openstack_public_ip`. Labels: `__meta_openstack_instance_id`/`_name`/
  `_status`/`_flavor`, `_project_id`, `_user_id`, `_address_pool`,
  `_private_ip`, `_public_ip`, and `_tag_<k>` per instance metadata entry.
- **hypervisor role** — one target per hypervisor, `__address__` is
  `host_ip` + `port`. Labels: `__meta_openstack_hypervisor_hostname`/`_id`/
  `_type`/`_state`/`_status`/`_host_ip`.

**Auth methods:** Keystone v3 **password** auth (scoped by `project_*`/
`domain_*`) and **application-credential** auth (id+secret, or name+secret with
user/domain) are supported; when `identity_endpoint` is unset the credentials
fall back to the standard `OS_*` environment variables. The legacy **`v2.0`
identity endpoint is rejected** at build time (upstream also rejects it). An
OpenStack API down at startup does not fail startup — discovery retries on its
background thread. The refresh interval comes from
`-promscrape.openstackSDCheckInterval` (default 30s), not a per-config YAML
field. Upstream's inline `HTTPClientConfig` / `proxy_url` knobs are not ported.

### DNS service discovery

```yaml
scrape_configs:
  - job_name: dns-srv
    dns_sd_configs:
      - names: ['_prometheus._tcp.example.com']   # required; one query per name
        # type: SRV       # SRV (default) | A | AAAA | MX
        # port: 9100      # required for A/AAAA; MX defaults to 25; ignored for SRV
  - job_name: dns-a
    dns_sd_configs:
      - names: ['node.example.com']
        type: A
        port: 9100
    relabel_configs:
      - source_labels: [__meta_dns_name]
        target_label: dns_query
```

Set `dns_sd_configs` on a job to discover targets from DNS records. Each config
resolves every entry in `names` on the refresh interval and emits one target
per resolved record:

- **SRV** (the default `type`) — each SRV answer becomes a target at
  `<record-target>:<record-port>` (the SRV record carries its own port; the
  config's `port` is ignored). Labels: `__meta_dns_name` (the queried name),
  `__meta_dns_srv_record_target`, and `__meta_dns_srv_record_port`.
- **A / AAAA** — the name is resolved through the OS resolver (getaddrinfo);
  each IPv4 (A) or IPv6 (AAAA) address becomes a target at `<ip>:<port>` using
  the config's required `port`. The same `__meta_dns_*` labels are attached
  (`__meta_dns_srv_record_target` holds the resolved IP), matching upstream's
  reuse of the SRV label builder for address records.
- **MX** — each MX answer becomes a target at `<exchange>:<port>` using the
  config's `port`, or `25` when `port` is omitted (matching upstream's MX
  default). Labels: `__meta_dns_name` and `__meta_dns_mx_record_target`.

**Resolution approach.** A/AAAA go through `std::net::ToSocketAddrs`
(getaddrinfo) — cross-platform, no nameserver configuration needed. SRV/MX need
record types getaddrinfo can't answer, so they are queried with a small
built-in synchronous DNS client (UDP, falling back to TCP on a truncated
response) pointed at the first `nameserver` in `/etc/resolv.conf` on unix.
**Platform note:** on non-unix (Windows) there is no `resolv.conf`, so SRV/MX
discovery finds no nameserver and yields no targets (a warning is logged);
A/AAAA discovery is unaffected. A DNS server that is down at startup does not
fail startup — discovery retries on its background thread, and a refresh where
every name fails keeps the last-good targets. The refresh interval comes from
`-promscrape.dnsSDCheckInterval` (default 30s), not a per-config YAML field.
**Port defaulting:** A/AAAA require an explicit `port`; MX defaults to `25`
when `port` is omitted and SRV ignores it — matching upstream. SRV/MX queries
are read-timeout-bounded; A/AAAA go through the OS resolver (getaddrinfo),
whose lookup is bounded by the OS resolver rather than an explicit timeout.

### Docker service discovery

```yaml
scrape_configs:
  - job_name: docker
    docker_sd_configs:
      - host: unix:///var/run/docker.sock   # required; or tcp://host:2375, http(s)://host
        port: 9100                          # default 80; fallback __address__ port
        # host_networking_host: localhost   # __address__ for host-network containers (default)
        # match_first_network: true         # default; keep only the first network per container
        # filters:                          # Docker Engine list filters
        #   - name: label
        #     values: [prometheus-scrape]
    relabel_configs:
      - source_labels: [__meta_docker_container_label_prometheus_scrape]
        regex: 'true'
        action: keep
```

Set `docker_sd_configs` on a job to discover running Docker containers. Each
config fetches `/networks` (for the `__meta_docker_network_*` label map) then
`/containers/json`, emitting one target per container **network × exposed TCP
port**: `__address__` = `<container-ip>:<private-port>` with
`__meta_docker_port_private`/`_port_public`/`_port_public_ip`. A container with
no exposed TCP port yields one fallback target — `<container-ip>:<port>`, or
`host_networking_host` when its `network_mode` is `host`. The label set is the
full upstream `__meta_docker_*`: `_container_id`/`_name`/`_network_mode`,
sanitized `_container_label_<k>`, `_network_ip`, and the joined
`_network_id`/`_name`/`_scope`/`_internal`/`_ingress`/sanitized `_network_label_<k>`.
`match_first_network` (default `true`) keeps only the lowest-named network per
container; containers with `network_mode: container:<id>` inherit the linked
container's networks.

**Host transports.** `host` accepts `unix:///var/run/docker.sock` (a Unix
socket, spoken via a small built-in HTTP/1.1 client that decodes chunked
responses), `tcp://host:port` (mapped to `http://host:port`), or an
`http(s)://` URL (using `reqwest` with the config's `basic_auth`/`bearer_token`/
`tls_config`). **Platform note:** Unix-socket hosts are only supported on Unix
platforms; on Windows a `unix://` host errors at fetch time (use a `tcp://`/
`http(s)://` host instead). A Docker daemon that is down at startup does not
fail startup — discovery retries on its background thread and keeps the
last-good targets across a failed refresh. The refresh interval comes from
`-promscrape.dockerSDCheckInterval` (default 30s), not a per-config YAML field.

### Docker Swarm service discovery

```yaml
scrape_configs:
  - job_name: dockerswarm
    dockerswarm_sd_configs:
      - host: unix:///var/run/docker.sock   # required; or tcp://host:2375, http(s)://host
        role: services                      # required: services | tasks | nodes
        port: 9100                          # default 80; fallback __address__ port
        # filters:                          # Docker Swarm list filters (applied to the role endpoint)
        #   - name: name
        #     values: [redis]
    relabel_configs:
      - source_labels: [__meta_dockerswarm_service_name]
        regex: 'redis'
        action: keep
```

Set `dockerswarm_sd_configs` on a job to discover a Docker Swarm cluster. The
required `role` selects what is discovered (matching upstream exactly):

- **`nodes`** — one target per Swarm node (`__address__` = node `Status.Addr` +
  `port`), with the full `__meta_dockerswarm_node_*` labels
  (`_id`/`_address`/`_hostname`/`_role`/`_availability`/`_status`/`_engine_version`/
  `_platform_architecture`/`_platform_os`/`_manager_*` + sanitized `_label_<k>`).
  Fetches `/nodes`.
- **`services`** — one target per service virtual-IP × published TCP port (or one
  per VIP when the service publishes none), joining the `__meta_dockerswarm_service_*`
  labels with the network-ID-keyed `__meta_dockerswarm_network_*` labels. Fetches
  `/services` + `/networks`.
- **`tasks`** — one target per task container-status port and per network
  attachment × service port, joining `__meta_dockerswarm_task_*`/`_container_label_<k>`
  with the task's node (`__meta_dockerswarm_node_*`), service
  (`__meta_dockerswarm_service_*`), and network (`__meta_dockerswarm_network_*`)
  labels. Fetches `/tasks` + `/services` + `/nodes` + `/networks`.

**Host transports.** `host` accepts the same schemes as `docker_sd_configs`
(`unix://` Unix socket — Unix-only, **reusing the Docker provider's built-in
HTTP/1.1 chunked client**; `tcp://host:port`; or `http(s)://` with
`basic_auth`/`bearer_token`/`tls_config`). `filters` is applied only to the
role's own endpoint (the joined endpoints are always listed unfiltered),
matching upstream. An invalid `role` or `host` is rejected when the job is
built; a Swarm manager that is down at startup does not fail startup —
discovery retries on its background thread and keeps the last-good targets
across a failed refresh. The refresh interval comes from
`-promscrape.dockerswarmSDCheckInterval` (default 30s), not a per-config YAML
field.

### Flags

| Flag | Default | Notes |
|---|---|---|
| `-remoteWrite.url=<url>` | (required) | Repeatable; one forwarding destination per occurrence |
| `-remoteWrite.tmpDataPath=<dir>` | `esmagent-remotewrite-data` | One durable queue subdirectory per destination |
| `-remoteWrite.maxDiskUsagePerURL=<n>` | `0` (unlimited) | Bytes; drop-oldest eviction once exceeded |
| `-remoteWrite.queues=<n>` | `1` | Worker threads per destination draining its queue |
| `-remoteWrite.maxBlockSize=<n>` | `8388608` | Estimated uncompressed bytes buffered before a block is sealed and queued |
| `-remoteWrite.flushInterval` | `1s` | Ceiling on how long a partial block waits before being sealed anyway |
| `-remoteWrite.retryMinInterval` | `1s` | Initial retry backoff for a retryable (5xx/429/transport) failure |
| `-remoteWrite.retryMaxInterval` | `30s` | Backoff ceiling (doubles from `retryMinInterval` on each consecutive failure) |
| `-remoteWrite.relabelConfig=<path>` | unset | Global relabel, applied once before fan-out |
| `-remoteWrite.urlRelabelConfig=<path>` | unset | Repeatable, positional; per-destination relabel |
| `-httpListenAddr` | `:8429` | |
| `-httpReadTimeout` | `30s` | |
| `-metrics.authKey` | unset (open) | Gates `GET /metrics` |
| `-dryRun` | `false` | Validate config (destinations + relabel files parse) and exit; touches no network, spawns no thread |
| `-promscrape.config=<path>` | unset | `scrape_configs` YAML; enables the scrape engine |
| `-promscrape.configCheckInterval` | `0` (SIGHUP-only) | Poll interval for reloading `-promscrape.config` |
| `-promscrape.suppressScrapeErrors` | `false` | Suppress the per-failed-scrape `log::warn!`; `last_error` on `/api/v1/targets` is unaffected either way |
| `-promscrape.maxScrapeSize=<n>` | `16777216` | Default per-target response byte cap; a job's own `max_scrape_size` overrides it |
| `-promscrape.kubernetes.attachNodeMetadataAll` | `false` | Default `attach_metadata.node` for every `kubernetes_sd_config`; a per-config `attach_metadata` overrides it |
| `-promscrape.kubernetes.attachNamespaceMetadataAll` | `false` | Default `attach_metadata.namespace` for every `kubernetes_sd_config`; a per-config `attach_metadata` overrides it |
| `-promscrape.consulSDCheckInterval` | `30s` | Refresh interval for every `consul_sd_config` (there is no per-config YAML `refresh_interval` — this flag is the sole source, matching upstream) |
| `-promscrape.consulagentSDCheckInterval` | `30s` | Refresh interval for every `consulagent_sd_config` (there is no per-config YAML `refresh_interval` — this flag is the sole source, matching upstream) |
| `-promscrape.ec2SDCheckInterval` | `60s` | Refresh interval for every `ec2_sd_config` (there is no per-config YAML `refresh_interval` — this flag is the sole source, matching upstream) |
| `-promscrape.gceSDCheckInterval` | `60s` | Refresh interval for every `gce_sd_config` (there is no per-config YAML `refresh_interval` — this flag is the sole source, matching upstream) |
| `-promscrape.azureSDCheckInterval` | `60s` | Refresh interval for every `azure_sd_config` (there is no per-config YAML `refresh_interval` — this flag is the sole source, matching upstream) |
| `-promscrape.digitaloceanSDCheckInterval` | `60s` | Refresh interval for every `digitalocean_sd_config` (there is no per-config YAML `refresh_interval` — this flag is the sole source, matching upstream) |
| `-promscrape.hetznerSDCheckInterval` | `60s` | Refresh interval for every `hetzner_sd_config` (there is no per-config YAML `refresh_interval` — this flag is the sole source, matching upstream) |
| `-promscrape.nomadSDCheckInterval` | `30s` | Refresh interval for every `nomad_sd_config` (there is no per-config YAML `refresh_interval` — this flag is the sole source, matching upstream) |
| `-promscrape.marathonSDCheckInterval` | `30s` | Refresh interval for every `marathon_sd_config` (there is no per-config YAML `refresh_interval` — this flag is the sole source, matching upstream) |
| `-promscrape.vultrSDCheckInterval` | `30s` | Refresh interval for every `vultr_sd_config` (there is no per-config YAML `refresh_interval` — this flag is the sole source, matching upstream) |
| `-promscrape.puppetdbSDCheckInterval` | `30s` | Refresh interval for every `puppetdb_sd_config` (there is no per-config YAML `refresh_interval` — this flag is the sole source, matching upstream) |
| `-promscrape.kumaSDCheckInterval` | `30s` | Refresh interval for every `kuma_sd_config` (there is no per-config YAML `refresh_interval` — this flag is the sole source, matching upstream) |
| `-promscrape.eurekaSDCheckInterval` | `30s` | Refresh interval for every `eureka_sd_config` (there is no per-config YAML `refresh_interval` — this flag is the sole source, matching upstream) |
| `-promscrape.yandexcloudSDCheckInterval` | `30s` | Refresh interval for every `yandexcloud_sd_config` (there is no per-config YAML `refresh_interval` — this flag is the sole source, matching upstream) |
| `-promscrape.ovhcloudSDCheckInterval` | `30s` | Refresh interval for every `ovhcloud_sd_config` (there is no per-config YAML `refresh_interval` — this flag is the sole source, matching upstream) |
| `-promscrape.openstackSDCheckInterval` | `30s` | Refresh interval for every `openstack_sd_config` (there is no per-config YAML `refresh_interval` — this flag is the sole source, matching upstream) |
| `-promscrape.dnsSDCheckInterval` | `30s` | Refresh interval for every `dns_sd_config` (there is no per-config YAML `refresh_interval` — this flag is the sole source, matching upstream) |
| `-promscrape.dockerSDCheckInterval` | `30s` | Refresh interval for every `docker_sd_config` (there is no per-config YAML `refresh_interval` — this flag is the sole source, matching upstream) |
| `-promscrape.dockerswarmSDCheckInterval` | `30s` | Refresh interval for every `dockerswarm_sd_config` (there is no per-config YAML `refresh_interval` — this flag is the sole source, matching upstream) |
| `-promscrape.config.dryRun` | `false` | Validate `-promscrape.config` alone (independent of `-remoteWrite.url`) and exit |

`-remoteWrite.*` also takes a `.basicAuth.username`/`.basicAuth.password[File]`/
`.bearerToken[File]`/`.tlsCAFile`/`.tlsCertFile`/`.tlsKeyFile`/
`.tlsServerName`/`.tlsInsecureSkipVerify` set, each repeatable and matched to
`-remoteWrite.url` **by position** (the Nth occurrence configures the Nth
destination) — upstream vmagent's own `flagutil` array-flag convention, not
something invented here. Run `esmagent -help` for the full text.

### HTTP API

Served on `-httpListenAddr`: every `esm-insert` push protocol's endpoints
(same set as `esmetrics`: `/write`, `/api/v1/write`, `/api/v1/import*`,
`/opentelemetry/*`, `/datadog/*`, ...), plus `GET /-/healthy`,
`GET /metrics` (gated by `-metrics.authKey`), and — when
`-promscrape.config` is set — `GET /api/v1/targets` (404 otherwise). There
is no query API and no web UI — esmagent only forwards, it never stores or
serves queries; `/api/v1/targets` is JSON-only, the HTML `/targets` page is
not ported.

## Delivery pipeline

Per destination: relabel (per-URL) -> block accumulation (`PendingSeries`,
sealed at `-remoteWrite.maxBlockSize` or `-remoteWrite.flushInterval`,
whichever comes first) -> durable on-disk queue (`PersistentQueue`) ->
`-remoteWrite.queues` worker threads retrying delivery. A block's fate on
each delivery attempt:

- **2xx (including 204)** — delivered; the worker moves to the next block.
- **5xx, 429, or a transport-level error/timeout** — retryable; the worker
  backs off (`retryMinInterval` doubling to `retryMaxInterval`) and retries
  the *same* block, so one destination being down never drops data, only
  delays it.
- **any other 4xx** — the block can never succeed against this endpoint; it
  is logged and dropped.
- **queue over `-remoteWrite.maxDiskUsagePerURL`** — the oldest queued
  block(s) are dropped (from memory and disk) to make room, matching
  upstream's default drop-oldest behavior. `0` (the default) means
  unlimited.

This simplifies upstream's retry-almost-everything policy into three
buckets (retry / drop / never-retry-this-endpoint) — document the
simplification, don't assume 1:1 parity with every upstream retry edge
case.

## Stream aggregation

`-streamAggr.config <path>` enables a **global** stream-aggregation stage
(`esm-streamaggr`) between the global relabel and the fan-out. Incoming series
are aggregated per the config (all 18 outputs, `by`/`without`, dedup,
input/output relabeling); the aggregated output is forwarded to every
destination, and input series consumed by an aggregator are dropped from the
direct forward path unless `-streamAggr.keepInput` is set. Options:
`-streamAggr.keepInput`, `-streamAggr.dedupInterval`,
`-streamAggr.dropInputLabels`, `-streamAggr.ignoreOldSamples`,
`-streamAggr.ignoreFirstIntervals`, `-streamAggr.flushOnShutdown`. On shutdown
the aggregators run a final flush (gated by `-streamAggr.flushOnShutdown`)
through the still-running destinations before the pipeline is torn down. See
`tests/e2e.rs`'s `stream_aggregation_*` test for the end-to-end path.

Per-URL aggregation is also supported: `-remoteWrite.streamAggr.config`
(repeatable, positional — one per `-remoteWrite.url`), with per-URL
`-remoteWrite.streamAggr.keepInput` and `-remoteWrite.streamAggr.dedupInterval`.
Each destination's aggregated output re-enters that destination's own
relabel → buffer → queue path (see `tests/e2e.rs`'s
`per_url_stream_aggregation_isolates_destinations`).

## Limitations

This port covers the forwarding pipeline exercised end to end (see
`tests/e2e.rs`: two destinations, per-destination failure isolation, durable
queuing across an outage, and survival of a process restart) and the scrape
pipeline (see `tests/scrape_e2e.rs`: scrape -> relabel -> forward,
`/api/v1/targets` reporting `up`, and target-death staleness; plus
`tests/kubernetes_sd_e2e.rs`: the same pipeline driven by a
`kubernetes_sd_configs` job against a stub k8s API server). The following
are deliberately out of scope or incomplete; documented here rather than
left to be discovered:

### Scraping limitations

- **The entire promscrape service-discovery surface is supported** — every
  provider in `lib/promscrape/discovery/*` is ported and no SD key is deferred.
  `static_configs`, `file_sd_configs`, `http_sd_configs`,
  `kubernetes_sd_configs` (Phase A+B — see "Kubernetes service discovery"
  above), `consul_sd_configs`, `consulagent_sd_configs`, `ec2_sd_configs`,
  `gce_sd_configs`, `azure_sd_configs`, `digitalocean_sd_configs`,
  `hetzner_sd_configs`, `nomad_sd_configs`, `marathon_sd_configs`,
  `vultr_sd_configs`, `puppetdb_sd_configs`, `kuma_sd_configs`,
  `eureka_sd_configs`, `yandexcloud_sd_configs`, `ovhcloud_sd_configs`,
  `openstack_sd_configs`, `dns_sd_configs`, `docker_sd_configs`, and
  `dockerswarm_sd_configs` (see each provider's "… service discovery" section
  above) all parse into typed configs. A genuinely-unknown SD key still fails
  loudly at config parse (as an unknown field), so a typo can't silently
  disable discovery.
- **GCE SD supports a subset of the credential chain** — a static
  `bearer_token` and the GCE metadata-server access token are supported; the
  service-account JSON key file (`credentials_file` /
  `GOOGLE_APPLICATION_CREDENTIALS`, RS256-JWT -> token exchange) is rejected at
  build time. `project`/`zone` auto-detect via the metadata server, and
  `zone: '*'` lists every zone for the project.
- **Yandex Cloud SD supports a subset of the credential chain** — a static
  `yandex_passport_oauth_token` (exchanged for an IAM token) and the compute
  metadata-server IAM token are supported; the service-account authorized-key
  JSON (`service_account_key_file`, the JWT -> IAM-exchange flow) is rejected at
  build time, and upstream's disabled EC2 IMDSv1 credential fallback is not
  ported. `service` must be `compute`.
- **EC2 SD supports a subset of the AWS credential chain** — static
  (config/env) and IMDSv2 instance-role credentials are supported; STS
  `role_arn` (`AssumeRole`) is rejected at build time, and web-identity token
  files (`AWS_WEB_IDENTITY_TOKEN_FILE`) and shared `~/.aws` config/credentials
  files (`profile`) are not ported. The query API version is `2016-11-15`,
  matching upstream.
- **Consul SD uses interval polling, not blocking-query long-poll** — this
  port re-lists on `-promscrape.consulSDCheckInterval` (default 30s) instead
  of upstream's `?index=&wait=` blocking queries, so target changes surface
  within one refresh interval rather than near-instantly. `allow_stale` is
  honored (adds `&stale`, on by default); `-promscrape.consul.waitTime` has
  no analog (there is no long-poll to tune).
- **JSON `/api/v1/targets` only** — the HTML `/targets` page is not ported.
- **`scrape_config_files`** (external scrape-config includes) is not
  supported; put every `scrape_config` directly in `-promscrape.config`.
- **No per-target series cap** — `series_limit` (and its bloom-filter
  implementation) is not implemented. Global `-promscrape.maxLabelNameLen`/
  `-promscrape.maxLabelValueLen` are not implemented either; per-job
  `sample_limit`/`label_limit` are.
- **No per-target interval/timeout override via relabel** — a relabel rule
  that rewrites `__scrape_interval__`/`__scrape_timeout__` does not change
  that target's actual tick period; the effective interval/timeout is
  always the job's value, else the global default. The
  `__scrape_interval__`/`__scrape_timeout__` synthetic labels themselves
  are only set when the job configures them (upstream always sets both).
- **Target diff is keyed by `scrape_url` only** — a target whose *labels*
  change while its `scrape_url` stays the same keeps its existing worker
  running with its old labels until the owning job's config changes on
  reload (upstream diffs on the full label set).
- **No `exported_` rename on auto-metric name collision** — a real scraped
  series literally named `up` or `scrape_*` is not renamed
  `exported_up`/`exported_scrape_*` the way upstream renames it to avoid
  clashing with the auto-metric of the same name.
- **Internal self-counters not yet exposed** —
  `vm_promscrape_stale_samples_created_total`,
  `vm_promscrape_scrapes_skipped_by_sample_limit_total`, and
  `..._by_label_limit_total` are tracked internally but not yet written to
  esmagent's own `GET /metrics`.
- **Scrape/`http_sd` auth**: `basic_auth`, `bearer_token`, and TLS
  (`tls_config`) are supported; OAuth2 is not (OAuth2 client-credentials auth
  *is* supported for `kubernetes_sd_configs` — see "Kubernetes SD
  limitations").

### Forwarding limitations

- Stream aggregation: both the GLOBAL `-streamAggr.config` stage and the
  PER-URL `-remoteWrite.streamAggr.config` variant are supported (see "Stream
  aggregation" above)
- `-remoteWrite.oauth2.*` auth flags
- Multitenancy (`-remoteWrite.multitenantURL`)
- The blocking (non-drop) backpressure mode — only the default drop-oldest
  `-remoteWrite.maxDiskUsagePerURL` behavior is implemented

**Persistent queue:** faithful *behavior* port of upstream's
`lib/persistentqueue` (durable FIFO, in-memory + disk, size-capped
drop-oldest), not its exact on-disk chunk/metadata file format — this queue
has no external reader, so the format is a private implementation detail
(see `src/queue.rs`'s module doc). Durability is process-crash-safe (every
push is fsync'd + atomically renamed before it returns); power-loss
durability additionally needs the directory-fsync step in
`flush_to_disk`/`close`, which is best-effort and silently skipped on
Windows.

**Fixed send timeout:** `-remoteWrite.sendTimeout` is not a flag yet — every
destination's HTTP client uses a fixed 30-second per-request timeout.

**TLS:** `tlsServerName` (SNI override) is accepted but not applied —
`reqwest`'s blocking client has no SNI-override knob independent of the
request URL's host (same gap as `esmalert`/`esmauth`).

**Retry simplification:** 2xx delivers, 5xx/429/transport retries with
backoff, any other 4xx drops — a coarser three-bucket classification than
upstream's more granular retry policy; see "Delivery pipeline" above.
