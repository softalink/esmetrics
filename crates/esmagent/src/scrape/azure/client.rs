//! Azure ARM (Resource Manager) client: auth-token resolution (an `OAuth`
//! client-credentials token from the Active Directory endpoint, or a
//! `ManagedIdentity` token from the Azure IMDS), cloud-environment endpoint
//! selection, the paginated VM / VM-scale-set listing, and per-VM NIC
//! resolution.
//!
//! Port of the SCOPED subset of `lib/promscrape/discovery/azure/api.go` +
//! `machine.go` + `nic.go`. Upstream supports two `authentication_method`s —
//! `OAuth` (client_id/client_secret/tenant_id -> a bearer token from the AD
//! endpoint) and `ManagedIdentity` (an IMDS token) — and this port supports
//! both. The token is cached until shortly before it expires (monotonic
//! [`Instant`], mirroring `scrape::kubernetes::oauth2` / `scrape::gce`).

use std::sync::Mutex;
use std::time::{Duration, Instant};

use reqwest::blocking::Client as HttpClient;
use serde::de::DeserializeOwned;
use serde::Deserialize;

use crate::scrape::config::ScrapeError;

use super::labels::{NetworkInterface, ScaleSet, VirtualMachine, VmIpAddress};
use super::AzureSdConfig;

/// Per-request timeout for ARM calls (VM / VMSS listing, NIC GET). Each page
/// GET is bounded so a hung endpoint can't stall the refresh thread — and thus
/// [`super::AzureDiscovery`]'s `Drop`/`stop` — indefinitely. Matches
/// upstream's 30s intent for the ARM client.
const ARM_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// Timeout for the `OAuth` token POST to the Active Directory endpoint.
const OAUTH_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// SHORT timeout for `ManagedIdentity` IMDS calls. A non-Azure host has
/// nothing answering on `169.254.169.254`, so the IMDS GET is bounded tightly
/// to fail fast rather than hang.
const IMDS_HTTP_TIMEOUT: Duration = Duration::from_secs(2);

/// Seconds shaved off the token's reported lifetime so it is refreshed
/// slightly early (avoiding a race where a request goes out with a token that
/// expires in transit). Mirrors upstream `mustGetAuthToken`'s 30s slack.
const TOKEN_SAFETY_MARGIN_SECS: u64 = 30;

/// Default Azure IMDS token endpoint host used for `ManagedIdentity` (port of
/// `api.go`'s `http://169.254.169.254/metadata/identity/oauth2/token`).
const DEFAULT_IMDS_BASE: &str = "http://169.254.169.254";

/// ARM API version for the compute list endpoints (port of `machine.go`).
const COMPUTE_API_VERSION: &str = "2022-03-01";

/// The `activeDirectoryEndpoint` + `resourceManagerEndpoint` for a cloud
/// environment. Port of `api.go`'s `cloudEnvironmentEndpoints`.
struct CloudEnv {
    active_directory: &'static str,
    resource_manager: &'static str,
}

/// Resolves a cloud-environment name to its AD + ARM endpoints. Port of
/// `api.go`'s `cloudEnvironments` map + `getCloudEnvByName` (case-insensitive;
/// empty defaults to the public cloud). `AZURESTACKCLOUD` (file-based
/// endpoints) is not ported — callers override the endpoints directly instead.
fn cloud_env_by_name(name: &str) -> Result<CloudEnv, ScrapeError> {
    let up = name.to_uppercase();
    let (ad, rm) = match up.as_str() {
        "" | "AZURECLOUD" | "AZUREPUBLICCLOUD" => (
            "https://login.microsoftonline.com",
            "https://management.azure.com",
        ),
        "AZURECHINACLOUD" => (
            "https://login.chinacloudapi.cn",
            "https://management.chinacloudapi.cn",
        ),
        "AZUREGERMANCLOUD" => (
            "https://login.microsoftonline.de",
            "https://management.microsoftazure.de",
        ),
        "AZUREUSGOVERNMENT" | "AZUREUSGOVERNMENTCLOUD" => (
            "https://login.microsoftonline.us",
            "https://management.usgovcloudapi.net",
        ),
        _ => {
            return Err(ScrapeError {
                msg: format!(
                    "unsupported azure `environment: {name:?}`; supported values: \
                     AzureCloud, AzurePublicCloud, AzureChinaCloud, AzureGermanCloud, \
                     AzureUSGovernment"
                ),
            });
        }
    };
    Ok(CloudEnv {
        active_directory: ad,
        resource_manager: rm,
    })
}

/// How the ARM bearer token is obtained. Both variants hit an HTTP endpoint
/// and cache the result (see [`AzureApi::get_token`]).
enum Auth {
    /// `OAuth` client-credentials: POST to `<AD>/<tenant>/oauth2/token`.
    OAuth {
        token_url: String,
        client_id: String,
        client_secret: String,
        resource: String,
    },
    /// `ManagedIdentity`: GET the Azure IMDS (or MSI) token endpoint with the
    /// [`MiHeaders`] appropriate to the detected environment.
    ManagedIdentity {
        token_url: String,
        headers: MiHeaders,
    },
}

/// Which headers a `ManagedIdentity` token GET must carry. Port of the
/// `modifyRequest` closure in Go `api.go`'s `getRefreshTokenFunc`
/// `managedidentity` case: an `MSI_SECRET` (App Service / Functions / older
/// ACI) swaps the default IMDS `Metadata: true` header for a `secret` header
/// (and, when `IDENTITY_HEADER` is also present, an `X-IDENTITY-HEADER`).
#[derive(Debug, PartialEq, Eq)]
enum MiHeaders {
    /// Default IMDS: `Metadata: true`.
    Metadata,
    /// `MSI_SECRET` set: `secret: <val>`, plus `X-IDENTITY-HEADER: <val>` when
    /// `x_identity_header` is set. Both use the same `MSI_SECRET` value,
    /// mirroring Go (which sets `X-IDENTITY-HEADER` to `msiSecret`, not to
    /// `IDENTITY_HEADER`).
    Secret {
        secret: String,
        x_identity_header: bool,
    },
}

/// The resolved `ManagedIdentity` token request: the full token URL (endpoint
/// + query) and the headers to send.
struct ManagedIdentityRequest {
    token_url: String,
    headers: MiHeaders,
}

/// A cached ARM access token and the monotonic instant it should be considered
/// expired at (already reduced by [`TOKEN_SAFETY_MARGIN_SECS`]).
struct CachedToken {
    access_token: String,
    expires_at: Instant,
}

/// Resolved Azure ARM access: HTTP clients (a 30s one for ARM/OAuth, a short
/// one for IMDS), the auth source (+ token cache), the effective ARM base URL,
/// and the subscription/resource-group the list URLs are built from.
///
/// `Debug` is hand-written to redact the `OAuth` client secret and never print
/// the cached token — defense-in-depth against a future `{:?}` in a log line
/// (mirrors `GceApi` / `OAuth2TokenSource`).
pub struct AzureApi {
    http: HttpClient,
    imds_http: HttpClient,
    auth: Auth,
    token_cache: Mutex<Option<CachedToken>>,
    arm_base: String,
    subscription_id: String,
    resource_group: String,
}

impl std::fmt::Debug for AzureApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let auth = match &self.auth {
            Auth::OAuth { token_url, .. } => format!("OAuth {{ token_url: {token_url:?} }}"),
            Auth::ManagedIdentity { token_url, .. } => {
                format!("ManagedIdentity {{ token_url: {token_url:?} }}")
            }
        };
        f.debug_struct("AzureApi")
            .field("auth", &auth)
            .field("arm_base", &self.arm_base)
            .field("subscription_id", &self.subscription_id)
            .field("resource_group", &self.resource_group)
            .finish()
    }
}

/// Trims a trailing `/` off a base URL override (schemes are assumed present —
/// the config docs/tests pass full `http://host:port` values).
fn normalize_base(url: &str) -> String {
    url.trim_end_matches('/').to_string()
}

/// Builds an [`AzureApi`] from `cfg`: resolves the cloud environment (applying
/// the ARM / AD / IMDS endpoint overrides), the auth method, and the ARM base.
///
/// Fails on an unknown `environment`, an unsupported `authentication_method`,
/// or a bad HTTP-client build — never because Azure is unreachable (the token
/// fetch and the first listing happen later on the refresh thread).
pub fn new_azure_api(cfg: &AzureSdConfig) -> Result<AzureApi, ScrapeError> {
    let env = cloud_env_by_name(&cfg.environment)?;
    let arm_base = match cfg
        .resource_manager_endpoint
        .as_deref()
        .filter(|s| !s.is_empty())
    {
        Some(u) => normalize_base(u),
        None => env.resource_manager.to_string(),
    };
    let ad_base = match cfg
        .active_directory_endpoint
        .as_deref()
        .filter(|s| !s.is_empty())
    {
        Some(u) => normalize_base(u),
        None => env.active_directory.to_string(),
    };
    let auth = build_auth(cfg, &ad_base, &arm_base)?;

    let http = HttpClient::builder().build().map_err(|e| ScrapeError {
        msg: format!("cannot build azure http client: {e}"),
    })?;
    let imds_http = HttpClient::builder()
        .timeout(IMDS_HTTP_TIMEOUT)
        .build()
        .map_err(|e| ScrapeError {
            msg: format!("cannot build azure imds http client: {e}"),
        })?;

    Ok(AzureApi {
        http,
        imds_http,
        auth,
        token_cache: Mutex::new(None),
        arm_base,
        subscription_id: cfg.subscription_id.clone(),
        resource_group: cfg.resource_group.clone(),
    })
}

/// Resolves the [`Auth`] from `cfg.authentication_method` (default `OAuth`).
/// Port of `api.go`'s `getRefreshTokenFunc` credential validation — the
/// required-field checks also run in `super::build_azure_sd_config` (reject at
/// parse), so this is defense-in-depth for a directly-constructed config.
fn build_auth(cfg: &AzureSdConfig, ad_base: &str, arm_base: &str) -> Result<Auth, ScrapeError> {
    match cfg.authentication_method.to_lowercase().as_str() {
        "" | "oauth" => {
            if cfg.tenant_id.is_empty() {
                return Err(ScrapeError {
                    msg: "missing `tenant_id` for `authentication_method: OAuth`".to_string(),
                });
            }
            if cfg.client_id.is_empty() {
                return Err(ScrapeError {
                    msg: "missing `client_id` for `authentication_method: OAuth`".to_string(),
                });
            }
            let secret = cfg.client_secret.clone().unwrap_or_default();
            if secret.is_empty() {
                return Err(ScrapeError {
                    msg: "missing `client_secret` for `authentication_method: OAuth`".to_string(),
                });
            }
            Ok(Auth::OAuth {
                token_url: format!("{ad_base}/{}/oauth2/token", cfg.tenant_id),
                client_id: cfg.client_id.clone(),
                client_secret: secret,
                resource: arm_base.to_string(),
            })
        }
        "managedidentity" => {
            // Consult the MSI env vars exactly like Go's `getRefreshTokenFunc`:
            // `MSI_ENDPOINT` overrides the token endpoint (App Service /
            // Functions / older ACI), and `MSI_SECRET` / `IDENTITY_HEADER`
            // change the api-version, client-id param name, and headers.
            let msi_endpoint = std::env::var("MSI_ENDPOINT").ok().filter(|s| !s.is_empty());
            let msi_secret = std::env::var("MSI_SECRET").ok().filter(|s| !s.is_empty());
            let identity_header = std::env::var("IDENTITY_HEADER")
                .ok()
                .filter(|s| !s.is_empty());
            let req = build_managed_identity_request(
                cfg.imds_endpoint.as_deref().filter(|s| !s.is_empty()),
                msi_endpoint.as_deref(),
                arm_base,
                &cfg.client_id,
                msi_secret.as_deref(),
                identity_header.as_deref(),
            );
            Ok(Auth::ManagedIdentity {
                token_url: req.token_url,
                headers: req.headers,
            })
        }
        other => Err(ScrapeError {
            msg: format!(
                "unsupported `authentication_method: {other:?}`; only `OAuth` and \
                 `ManagedIdentity` are supported"
            ),
        }),
    }
}

/// Builds the `ManagedIdentity` token request (URL + headers). Pure port of
/// the endpoint / api-version / client-id-param / header selection in Go
/// `api.go`'s `getRefreshTokenFunc` `managedidentity` case, with the env-var
/// reads hoisted to the caller so this stays unit-testable.
///
/// Endpoint precedence: the esmagent `imds_endpoint` config wins when set
/// (an explicit override), else `MSI_ENDPOINT` (Go's only knob), else the
/// default `http://169.254.169.254/metadata/identity/oauth2/token`.
///
/// api-version / client-id param follow Go's last-wins order (default ->
/// `MSI_SECRET` -> `IDENTITY_HEADER`); headers follow `MSI_SECRET` alone.
fn build_managed_identity_request(
    imds_endpoint_cfg: Option<&str>,
    msi_endpoint_env: Option<&str>,
    arm_base: &str,
    client_id: &str,
    msi_secret: Option<&str>,
    identity_header: Option<&str>,
) -> ManagedIdentityRequest {
    let base_endpoint = match imds_endpoint_cfg {
        Some(cfg) => format!("{}/metadata/identity/oauth2/token", normalize_base(cfg)),
        None => match msi_endpoint_env {
            Some(ep) => ep.to_string(),
            None => format!("{DEFAULT_IMDS_BASE}/metadata/identity/oauth2/token"),
        },
    };

    let (client_id_param, api_version) = if identity_header.is_some() {
        ("client_id", "2019-08-01")
    } else if msi_secret.is_some() {
        ("clientid", "2017-09-01")
    } else {
        ("client_id", "2018-02-01")
    };

    // Preserve any query already on an overridden endpoint, appending ours.
    let sep = if base_endpoint.contains('?') {
        '&'
    } else {
        '?'
    };
    let mut token_url = format!(
        "{base_endpoint}{sep}api-version={api_version}&resource={}",
        query_escape(arm_base)
    );
    if !client_id.is_empty() {
        token_url.push_str(&format!("&{client_id_param}={}", query_escape(client_id)));
    }

    let headers = match msi_secret {
        Some(s) => MiHeaders::Secret {
            secret: s.to_string(),
            x_identity_header: identity_header.is_some(),
        },
        None => MiHeaders::Metadata,
    };

    ManagedIdentityRequest { token_url, headers }
}

/// The token JSON returned by both the AD `oauth2/token` and the IMDS
/// endpoints. `expires_in`/`expires_on` are strings in Azure responses. Port
/// of `api.go`'s `tokenResponse`.
#[derive(Deserialize)]
struct TokenResponse {
    #[serde(default)]
    access_token: String,
    #[serde(default)]
    expires_in: String,
    #[serde(default)]
    expires_on: String,
}

/// The paginated ARM list envelope: `{ "value": [...], "nextLink": "..." }`.
/// Port of `machine.go`'s `listAPIResponse`, generic over the item type.
#[derive(Deserialize)]
struct ListApiResponse<T> {
    #[serde(default = "Vec::new")]
    value: Vec<T>,
    #[serde(rename = "nextLink", default)]
    next_link: String,
}

impl AzureApi {
    /// Returns a valid ARM bearer token, fetching a fresh one only when the
    /// cache is empty or the cached token has reached its (safety-margin
    /// adjusted) expiry. The cache mutex is held only to read/store — never
    /// across the bounded network fetch.
    fn get_token(&self) -> Result<String, ScrapeError> {
        {
            let guard = self.token_cache.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(c) = guard.as_ref() {
                if Instant::now() < c.expires_at {
                    return Ok(c.access_token.clone());
                }
            }
        }
        let (access_token, expires_in) = self.fetch_token()?;
        let expires_at = if expires_in == 0 {
            Instant::now()
        } else {
            Instant::now()
                + Duration::from_secs(expires_in.saturating_sub(TOKEN_SAFETY_MARGIN_SECS))
        };
        let mut guard = self.token_cache.lock().unwrap_or_else(|e| e.into_inner());
        *guard = Some(CachedToken {
            access_token: access_token.clone(),
            expires_at,
        });
        Ok(access_token)
    }

    /// Performs the token request for the configured [`Auth`] and returns
    /// `(access_token, expires_in_secs)`. Never logs the secret or the token.
    fn fetch_token(&self) -> Result<(String, u64), ScrapeError> {
        let resp = match &self.auth {
            Auth::OAuth {
                token_url,
                client_id,
                client_secret,
                resource,
            } => {
                let params = [
                    ("grant_type", "client_credentials"),
                    ("client_id", client_id.as_str()),
                    ("client_secret", client_secret.as_str()),
                    ("resource", resource.as_str()),
                ];
                self.http
                    .post(token_url)
                    .timeout(OAUTH_HTTP_TIMEOUT)
                    .form(&params)
                    .send()
                    .map_err(|e| ScrapeError {
                        msg: format!("azure oauth token request failed: {e}"),
                    })?
            }
            Auth::ManagedIdentity { token_url, headers } => {
                let mut req = self.imds_http.get(token_url);
                match headers {
                    MiHeaders::Metadata => {
                        req = req.header("Metadata", "true");
                    }
                    MiHeaders::Secret {
                        secret,
                        x_identity_header,
                    } => {
                        req = req.header("secret", secret);
                        if *x_identity_header {
                            req = req.header("X-IDENTITY-HEADER", secret);
                        }
                    }
                }
                req.send().map_err(|e| ScrapeError {
                    msg: format!(
                        "azure managed-identity token request failed (is this running on Azure?): {e}"
                    ),
                })?
            }
        };
        let status = resp.status();
        let body = resp.bytes().map_err(|e| ScrapeError {
            msg: format!("cannot read azure token response: {e}"),
        })?;
        if !status.is_success() {
            return Err(ScrapeError {
                msg: format!("azure token endpoint returned status {status}"),
            });
        }
        // The parse error names the JSON problem, not the token value.
        let tr: TokenResponse = serde_json::from_slice(&body).map_err(|e| ScrapeError {
            msg: format!("cannot parse azure token response: {e}"),
        })?;
        if tr.access_token.is_empty() {
            return Err(ScrapeError {
                msg: "azure token response is missing access_token".to_string(),
            });
        }
        let expiry = parse_token_expiry(&tr);
        Ok((tr.access_token, expiry))
    }

    /// Lists every virtual machine for the subscription (optionally scoped to
    /// `resource_group`), the VMSS VMs, then resolves each VM's primary-NIC
    /// IPs. Port of `machine.go`'s `getVirtualMachines`.
    pub fn get_virtual_machines(&self) -> Result<Vec<VirtualMachine>, ScrapeError> {
        let mut vms = self.list_vms()?;
        let scale_sets = self.list_scale_set_refs()?;
        vms.extend(self.list_scale_set_vms(&scale_sets)?);
        for vm in &mut vms {
            self.enrich_vm_nics(vm)?;
        }
        Ok(vms)
    }

    /// Port of `machine.go`'s `listVMs`.
    fn list_vms(&self) -> Result<Vec<VirtualMachine>, ScrapeError> {
        let path = format!(
            "{}/providers/Microsoft.Compute/virtualMachines?api-version={COMPUTE_API_VERSION}",
            self.subscription_scope()
        );
        self.list_paginated(&path)
    }

    /// Port of `machine.go`'s `listScaleSetRefs`.
    fn list_scale_set_refs(&self) -> Result<Vec<ScaleSet>, ScrapeError> {
        let path = format!(
            "{}/providers/Microsoft.Compute/virtualMachineScaleSets?api-version={COMPUTE_API_VERSION}",
            self.subscription_scope()
        );
        self.list_paginated(&path)
    }

    /// Port of `machine.go`'s `listScaleSetVMs`: lists the VMs of each scale
    /// set and tags them with the scale-set name.
    fn list_scale_set_vms(&self, sss: &[ScaleSet]) -> Result<Vec<VirtualMachine>, ScrapeError> {
        let mut vms = Vec::new();
        for ss in sss {
            let path = format!(
                "{}/virtualMachines?api-version={COMPUTE_API_VERSION}",
                ss.id
            );
            let mut ss_vms: Vec<VirtualMachine> = self.list_paginated(&path)?;
            for vm in &mut ss_vms {
                vm.scale_set = ss.name.clone();
            }
            vms.extend(ss_vms);
        }
        Ok(vms)
    }

    /// `/subscriptions/<sub>` optionally suffixed with
    /// `/resourceGroups/<rg>` when a resource-group filter is set.
    fn subscription_scope(&self) -> String {
        let mut s = format!("/subscriptions/{}", self.subscription_id);
        if !self.resource_group.is_empty() {
            s.push_str(&format!("/resourceGroups/{}", self.resource_group));
        }
        s
    }

    /// Resolves a VM's primary-NIC IP addresses. Port of `nic.go`'s
    /// `enrichVMNetworkInterfaces` (only primary NICs contribute). Done
    /// sequentially rather than upstream's worker pool — see the module doc in
    /// [`super`].
    fn enrich_vm_nics(&self, vm: &mut VirtualMachine) -> Result<(), ScrapeError> {
        let is_scale_set_vm = !vm.scale_set.is_empty();
        let nic_ids: Vec<String> = vm
            .properties
            .network_profile
            .network_interfaces
            .iter()
            .map(|r| r.id.clone())
            .collect();
        for nic_id in nic_ids {
            let nic = self.get_nic(&nic_id, is_scale_set_vm)?;
            if nic.properties.primary {
                for ip_cfg in &nic.properties.ip_configurations {
                    vm.ip_addresses.push(VmIpAddress {
                        public_ip: ip_cfg
                            .properties
                            .public_ip_address
                            .properties
                            .ip_address
                            .clone(),
                        private_ip: ip_cfg.properties.private_ip_address.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Port of `nic.go`'s `getNIC`.
    fn get_nic(&self, id: &str, is_scale_set_vm: bool) -> Result<NetworkInterface, ScrapeError> {
        let api_version = if is_scale_set_vm {
            "2021-03-01"
        } else {
            "2021-08-01"
        };
        let path =
            format!("{id}?api-version={api_version}&$expand=ipConfigurations/publicIPAddress");
        let data = self.arm_get(&path)?;
        serde_json::from_slice(&data).map_err(|e| ScrapeError {
            msg: format!("cannot parse azure network-interface response: {e}"),
        })
    }

    /// Follows an ARM list endpoint's `nextLink` pagination, collecting every
    /// `value[]` item. Port of `machine.go`'s `visitAllAPIObjects`; the
    /// `nextLink`'s path+query is re-targeted at [`arm_base`](AzureApi) (so an
    /// overridden endpoint keeps working), the deliberate analog of upstream's
    /// host-verify + `RequestURI` re-request.
    fn list_paginated<T: DeserializeOwned>(&self, first: &str) -> Result<Vec<T>, ScrapeError> {
        let mut out = Vec::new();
        let mut uri = first.to_string();
        loop {
            let data = self.arm_get(&uri)?;
            let resp: ListApiResponse<T> =
                serde_json::from_slice(&data).map_err(|e| ScrapeError {
                    msg: format!("cannot parse azure list response: {e}"),
                })?;
            out.extend(resp.value);
            if resp.next_link.is_empty() {
                return Ok(out);
            }
            uri = next_link_path_query(&resp.next_link)?;
        }
    }

    /// One ARM GET (`arm_base` + `path_and_query`) with a bearer token and a
    /// [`ARM_HTTP_TIMEOUT`] cap, returning the body bytes on a 2xx.
    fn arm_get(&self, path_and_query: &str) -> Result<Vec<u8>, ScrapeError> {
        let token = self.get_token()?;
        let url = format!("{}{}", self.arm_base, path_and_query);
        let resp = self
            .http
            .get(&url)
            .timeout(ARM_HTTP_TIMEOUT)
            .bearer_auth(token)
            .send()
            .map_err(|e| ScrapeError {
                msg: format!("azure arm request to {path_and_query:?} failed: {e}"),
            })?;
        let status = resp.status();
        let body = resp.bytes().map_err(|e| ScrapeError {
            msg: format!("azure arm response from {path_and_query:?}: {e}"),
        })?;
        if !status.is_success() {
            return Err(ScrapeError {
                msg: format!("azure arm request to {path_and_query:?}: status {status}"),
            });
        }
        Ok(body.to_vec())
    }
}

/// Returns the token lifetime in seconds. Port of `api.go`'s
/// `parseTokenExpiry`: prefer `expires_in`; else `expires_on` (a unix
/// timestamp) minus now; `0` if neither parses.
fn parse_token_expiry(tr: &TokenResponse) -> u64 {
    if !tr.expires_in.is_empty() {
        return tr.expires_in.parse::<u64>().unwrap_or(0);
    }
    if !tr.expires_on.is_empty() {
        if let Ok(on) = tr.expires_on.parse::<i64>() {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            return (on - now).max(0) as u64;
        }
    }
    0
}

/// Extracts the path + query from a `nextLink` URL for re-targeting at
/// `arm_base`.
fn next_link_path_query(next_link: &str) -> Result<String, ScrapeError> {
    let u = reqwest::Url::parse(next_link).map_err(|e| ScrapeError {
        msg: format!("cannot parse azure nextLink {next_link:?}: {e}"),
    })?;
    let mut s = u.path().to_string();
    if let Some(q) = u.query() {
        s.push('?');
        s.push_str(q);
    }
    Ok(s)
}

/// Go `url.QueryEscape`-equivalent for the IMDS `resource`/`client_id` query
/// args: unreserved (`A-Za-z0-9-_.~`) pass through, space becomes `+`,
/// everything else is `%XX`. Local copy of the GCE/EC2 ports' helper.
fn query_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oauth_cfg() -> AzureSdConfig {
        AzureSdConfig {
            subscription_id: "sub".into(),
            tenant_id: "tenant".into(),
            client_id: "cid".into(),
            client_secret: Some("super-secret".into()),
            ..AzureSdConfig::default()
        }
    }

    #[test]
    fn oauth_api_builds_and_redacts_secret_in_debug() {
        let api = new_azure_api(&oauth_cfg()).unwrap();
        assert!(matches!(api.auth, Auth::OAuth { .. }));
        assert_eq!(api.arm_base, "https://management.azure.com");
        let dbg = format!("{api:?}");
        assert!(!dbg.contains("super-secret"), "{dbg}");
    }

    #[test]
    fn oauth_token_url_uses_ad_endpoint_and_tenant() {
        let mut c = oauth_cfg();
        c.active_directory_endpoint = Some("http://127.0.0.1:1/".into());
        let api = new_azure_api(&c).unwrap();
        match api.auth {
            Auth::OAuth { token_url, .. } => {
                assert_eq!(token_url, "http://127.0.0.1:1/tenant/oauth2/token");
            }
            _ => panic!("expected OAuth"),
        }
    }

    #[test]
    fn managed_identity_targets_imds_with_metadata() {
        let c = AzureSdConfig {
            subscription_id: "sub".into(),
            authentication_method: "ManagedIdentity".into(),
            ..AzureSdConfig::default()
        };
        let api = new_azure_api(&c).unwrap();
        match api.auth {
            Auth::ManagedIdentity { token_url, headers } => {
                assert!(
                    token_url.starts_with("http://169.254.169.254/metadata/identity/oauth2/token")
                );
                assert!(token_url.contains("resource=https%3A%2F%2Fmanagement.azure.com"));
                // No MSI_SECRET on a plain IMDS host -> `Metadata: true`.
                // (Skip the assertion if the test host happens to export
                // MSI_SECRET, so the suite stays deterministic there.)
                if std::env::var("MSI_SECRET")
                    .ok()
                    .filter(|s| !s.is_empty())
                    .is_none()
                {
                    assert_eq!(headers, MiHeaders::Metadata);
                }
            }
            _ => panic!("expected ManagedIdentity"),
        }
    }

    #[test]
    fn managed_identity_default_endpoint_and_metadata_header() {
        let req = build_managed_identity_request(
            None,
            None,
            "https://management.azure.com",
            "",
            None,
            None,
        );
        assert!(req
            .token_url
            .starts_with("http://169.254.169.254/metadata/identity/oauth2/token"));
        assert!(req.token_url.contains("api-version=2018-02-01"));
        assert!(req
            .token_url
            .contains("resource=https%3A%2F%2Fmanagement.azure.com"));
        assert_eq!(req.headers, MiHeaders::Metadata);
    }

    #[test]
    fn managed_identity_honors_msi_endpoint_env() {
        let req = build_managed_identity_request(
            None,
            Some("http://127.0.0.1:41741/msi/token"),
            "https://management.azure.com",
            "cid",
            None,
            None,
        );
        assert!(req
            .token_url
            .starts_with("http://127.0.0.1:41741/msi/token?"));
        assert!(req.token_url.contains("api-version=2018-02-01"));
        assert!(req.token_url.contains("&client_id=cid"));
        assert_eq!(req.headers, MiHeaders::Metadata);
    }

    #[test]
    fn managed_identity_msi_secret_switches_apiversion_and_header() {
        let req = build_managed_identity_request(
            None,
            Some("http://127.0.0.1:41741/msi/token"),
            "https://management.azure.com",
            "cid",
            Some("s3cr3t"),
            None,
        );
        assert!(req.token_url.contains("api-version=2017-09-01"));
        // MSI_SECRET renames the client-id query param to `clientid`.
        assert!(req.token_url.contains("&clientid=cid"));
        assert_eq!(
            req.headers,
            MiHeaders::Secret {
                secret: "s3cr3t".into(),
                x_identity_header: false,
            }
        );
    }

    #[test]
    fn managed_identity_identity_header_wins_apiversion() {
        // IDENTITY_HEADER present but MSI_SECRET absent: api-version 2019-08-01
        // and `client_id` param, yet headers stay `Metadata: true` (Go only
        // emits `secret`/`X-IDENTITY-HEADER` when MSI_SECRET is set).
        let req = build_managed_identity_request(
            None,
            None,
            "https://management.azure.com",
            "cid",
            None,
            Some("hdr"),
        );
        assert!(req.token_url.contains("api-version=2019-08-01"));
        assert!(req.token_url.contains("&client_id=cid"));
        assert_eq!(req.headers, MiHeaders::Metadata);
    }

    #[test]
    fn managed_identity_secret_and_identity_header_set_both_headers() {
        let req = build_managed_identity_request(
            None,
            None,
            "https://management.azure.com",
            "cid",
            Some("s3cr3t"),
            Some("hdr"),
        );
        assert!(req.token_url.contains("api-version=2019-08-01"));
        assert!(req.token_url.contains("&client_id=cid"));
        assert_eq!(
            req.headers,
            MiHeaders::Secret {
                secret: "s3cr3t".into(),
                x_identity_header: true,
            }
        );
    }

    #[test]
    fn managed_identity_imds_endpoint_cfg_beats_msi_endpoint_env() {
        let req = build_managed_identity_request(
            Some("http://config-host:9000/"),
            Some("http://msi-env:1234/token"),
            "https://management.azure.com",
            "",
            None,
            None,
        );
        assert!(req
            .token_url
            .starts_with("http://config-host:9000/metadata/identity/oauth2/token?"));
        assert!(!req.token_url.contains("msi-env"));
    }

    #[test]
    fn unknown_environment_is_rejected() {
        let mut c = oauth_cfg();
        c.environment = "AzureBogusCloud".into();
        assert!(new_azure_api(&c).unwrap_err().msg.contains("environment"));
    }

    #[test]
    fn china_cloud_endpoints_resolved() {
        let mut c = oauth_cfg();
        c.environment = "AzureChinaCloud".into();
        let api = new_azure_api(&c).unwrap();
        assert_eq!(api.arm_base, "https://management.chinacloudapi.cn");
    }

    #[test]
    fn parse_token_expiry_prefers_expires_in() {
        let tr = TokenResponse {
            access_token: "t".into(),
            expires_in: "3600".into(),
            expires_on: "0".into(),
        };
        assert_eq!(parse_token_expiry(&tr), 3600);
    }

    #[test]
    fn next_link_path_query_strips_host() {
        let s = next_link_path_query(
            "https://management.azure.com/subscriptions/x/providers/Microsoft.Compute/virtualMachines?api-version=2022-03-01&$skiptoken=abc",
        )
        .unwrap();
        assert!(s.starts_with("/subscriptions/x/providers/Microsoft.Compute/virtualMachines?"));
        assert!(!s.contains("management.azure.com"));
    }
}
