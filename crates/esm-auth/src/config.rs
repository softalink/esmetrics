//! Configuration types and parsing for the vmauth proxy.
//!
//! Port of the YAML types and validation from `app/vmauth/auth_config.go`.
//! This module produces the parsed, validated, immutable config shape only;
//! the runtime load-balancing/health state built on top of it lives in later
//! tasks (see `docs/PORTING.md`).

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer};

/// Parsed `-auth.config`. Immutable; hot-reload swaps whole `Arc<AuthConfig>`.
#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    pub users: Vec<UserInfo>,
    pub unauthorized_user: Option<UserInfo>,
}

#[derive(Debug, Clone, Default)]
pub struct UserInfo {
    pub name: Option<String>,
    pub bearer_token: Option<String>,
    pub auth_token: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    /// Scalar-or-list `url_prefix`, normalized to a `Vec<String>`.
    pub url_prefix: Option<Vec<String>>,
    pub url_map: Vec<UrlMap>,
    /// Scalar-or-list `default_url`, normalized to a `Vec<String>`.
    pub default_url: Option<Vec<String>>,
    /// Request headers to set, as parsed `(name, value)` pairs.
    pub headers: Vec<(String, String)>,
    /// Response headers to set, as parsed `(name, value)` pairs.
    pub response_headers: Vec<(String, String)>,
    pub keep_original_host: Option<bool>,
    pub retry_status_codes: Vec<u16>,
    pub load_balancing_policy: LoadBalancingPolicy,
    pub max_concurrent_requests: Option<usize>,
    pub drop_src_path_prefix_parts: Option<usize>,
    pub merge_query_args: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct UrlMap {
    /// Anchored (`^(?:...)$`) regexes matching the request path, like upstream.
    pub src_paths: Vec<regex::Regex>,
    /// Anchored (`^(?:...)$`) regexes matching the request host, like upstream.
    pub src_hosts: Vec<regex::Regex>,
    pub url_prefix: Vec<String>,
    pub headers: Vec<(String, String)>,
    pub response_headers: Vec<(String, String)>,
    pub retry_status_codes: Vec<u16>,
    pub load_balancing_policy: LoadBalancingPolicy,
    pub drop_src_path_prefix_parts: Option<usize>,
    pub merge_query_args: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LoadBalancingPolicy {
    #[default]
    LeastLoaded,
    FirstAvailable,
}

impl LoadBalancingPolicy {
    /// Mirrors `URLPrefix.setLoadBalancingPolicy` in auth_config.go: an empty
    /// string is equivalent to `least_loaded`, and any other value is an error.
    fn parse(s: &str) -> Result<Self, String> {
        match s {
            "" | "least_loaded" => Ok(LoadBalancingPolicy::LeastLoaded),
            "first_available" => Ok(LoadBalancingPolicy::FirstAvailable),
            other => Err(format!(
                "unexpected load_balancing_policy: {other:?}; want least_loaded or first_available"
            )),
        }
    }
}

/// Parses a top-level `-auth.config` YAML document into an [`AuthConfig`].
pub fn parse_auth_config(yaml: &str) -> Result<AuthConfig, String> {
    let raw: RawAuthConfig =
        serde_yaml_ng::from_str(yaml).map_err(|e| format!("cannot parse auth config: {e}"))?;

    let users = raw
        .users
        .into_iter()
        .map(RawUserInfo::into_user_info)
        .collect::<Result<Vec<_>, _>>()?;
    let unauthorized_user = raw
        .unauthorized_user
        .map(RawUserInfo::into_user_info)
        .transpose()?;

    Ok(AuthConfig {
        users,
        unauthorized_user,
    })
}

// ---------------------------------------------------------------------------
// Raw (serde-facing) types. These mirror the YAML shape from auth_config.go;
// `into_user_info`/`into_url_map` perform the post-deserialize validation
// upstream does in `URLPrefix.UnmarshalYAML` / `sanitizeURLPrefix`.
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct RawAuthConfig {
    users: Vec<RawUserInfo>,
    unauthorized_user: Option<RawUserInfo>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct RawUserInfo {
    name: Option<String>,
    bearer_token: Option<String>,
    auth_token: Option<String>,
    username: Option<String>,
    password: Option<String>,
    #[serde(deserialize_with = "deserialize_opt_scalar_or_seq")]
    url_prefix: Option<Vec<String>>,
    url_map: Vec<RawUrlMap>,
    #[serde(deserialize_with = "deserialize_opt_scalar_or_seq")]
    default_url: Option<Vec<String>>,
    #[serde(deserialize_with = "deserialize_headers")]
    headers: Vec<(String, String)>,
    #[serde(deserialize_with = "deserialize_headers")]
    response_headers: Vec<(String, String)>,
    keep_original_host: Option<bool>,
    retry_status_codes: Vec<u16>,
    #[serde(deserialize_with = "deserialize_load_balancing_policy")]
    load_balancing_policy: LoadBalancingPolicy,
    max_concurrent_requests: Option<usize>,
    drop_src_path_prefix_parts: Option<usize>,
    merge_query_args: Vec<String>,
}

impl RawUserInfo {
    fn into_user_info(self) -> Result<UserInfo, String> {
        if let Some(urls) = &self.url_prefix {
            validate_url_prefix("url_prefix", urls)?;
        }
        if let Some(urls) = &self.default_url {
            validate_url_prefix("default_url", urls)?;
        }
        let url_map = self
            .url_map
            .into_iter()
            .map(RawUrlMap::into_url_map)
            .collect::<Result<Vec<_>, _>>()?;

        // Mirrors `initURLs` (auth_config.go:1185-1187): a user must specify
        // at least one of `url_prefix` / `url_map`.
        if self.url_prefix.is_none() && url_map.is_empty() {
            return Err("missing `url_prefix` or `url_map`".to_string());
        }

        Ok(UserInfo {
            name: self.name,
            bearer_token: self.bearer_token,
            auth_token: self.auth_token,
            username: self.username,
            password: self.password,
            url_prefix: self.url_prefix,
            url_map,
            default_url: self.default_url,
            headers: self.headers,
            response_headers: self.response_headers,
            keep_original_host: self.keep_original_host,
            retry_status_codes: self.retry_status_codes,
            load_balancing_policy: self.load_balancing_policy,
            max_concurrent_requests: self.max_concurrent_requests,
            drop_src_path_prefix_parts: self.drop_src_path_prefix_parts,
            merge_query_args: self.merge_query_args,
        })
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct RawUrlMap {
    #[serde(deserialize_with = "deserialize_regexes")]
    src_paths: Vec<AnchoredRegex>,
    #[serde(deserialize_with = "deserialize_regexes")]
    src_hosts: Vec<AnchoredRegex>,
    // Presence-only: esmauth does not yet implement `src_query_args`/
    // `src_headers` matching. Upstream requires ALL of src_paths/src_hosts/
    // src_query_args/src_headers to match; silently ignoring these would
    // mis-route requests that upstream would gate. Rejected loudly in
    // `into_url_map` below.
    src_query_args: Option<serde_yaml_ng::Value>,
    src_headers: Option<serde_yaml_ng::Value>,
    #[serde(deserialize_with = "deserialize_opt_scalar_or_seq")]
    url_prefix: Option<Vec<String>>,
    #[serde(deserialize_with = "deserialize_headers")]
    headers: Vec<(String, String)>,
    #[serde(deserialize_with = "deserialize_headers")]
    response_headers: Vec<(String, String)>,
    retry_status_codes: Vec<u16>,
    #[serde(deserialize_with = "deserialize_load_balancing_policy")]
    load_balancing_policy: LoadBalancingPolicy,
    drop_src_path_prefix_parts: Option<usize>,
    merge_query_args: Vec<String>,
}

impl RawUrlMap {
    fn into_url_map(self) -> Result<UrlMap, String> {
        // esmauth only matches `src_paths`/`src_hosts`. Upstream requires ALL
        // of src_paths/src_hosts/src_query_args/src_headers to match, so
        // silently ignoring these would mis-route requests that upstream
        // would gate. Fail loudly until the feature exists.
        if self.src_query_args.is_some() || self.src_headers.is_some() {
            return Err(
                "url_map `src_query_args`/`src_headers` matching is not supported by esmauth"
                    .to_string(),
            );
        }
        if self.src_paths.is_empty() && self.src_hosts.is_empty() {
            return Err("missing `src_paths` and `src_hosts` in `url_map`".to_string());
        }
        let url_prefix = match &self.url_prefix {
            Some(urls) => {
                validate_url_prefix("url_prefix", urls)?;
                urls.clone()
            }
            None => return Err("missing `url_prefix` in `url_map`".to_string()),
        };

        Ok(UrlMap {
            src_paths: self.src_paths.into_iter().map(|r| r.0).collect(),
            src_hosts: self.src_hosts.into_iter().map(|r| r.0).collect(),
            url_prefix,
            headers: self.headers,
            response_headers: self.response_headers,
            retry_status_codes: self.retry_status_codes,
            load_balancing_policy: self.load_balancing_policy,
            drop_src_path_prefix_parts: self.drop_src_path_prefix_parts,
            merge_query_args: self.merge_query_args,
        })
    }
}

/// Validates a normalized `url_prefix`/`default_url` list like upstream's
/// `sanitizeURLPrefix` (auth_config.go:1305): scheme must be http/https and
/// the host must be non-empty.
fn validate_url_prefix(field: &str, urls: &[String]) -> Result<(), String> {
    if urls.is_empty() {
        return Err(format!("`{field}` must contain at least a single url"));
    }
    for u in urls {
        let parsed =
            reqwest::Url::parse(u).map_err(|e| format!("cannot unmarshal {u:?} into url: {e}"))?;
        let scheme = parsed.scheme();
        if scheme != "http" && scheme != "https" {
            return Err(format!(
                "unsupported scheme for `{field}: {u}`: {scheme:?}; must be `http` or `https`"
            ));
        }
        // Go's net/url rejects an empty authority (e.g. `http:///bar` → Host="")
        // but the WHATWG-based `url` crate accepts it, treating `bar` as the
        // host. Match Go by checking the raw authority segment: after
        // `scheme://`, the host runs up to the next `/`, `?`, or `#` and must
        // be non-empty.
        if raw_authority_is_empty(u, scheme) {
            return Err(format!("missing hostname in `{field} {u}`"));
        }
        match parsed.host_str() {
            Some(h) if !h.is_empty() => {}
            _ => return Err(format!("missing hostname in `{field} {u}`")),
        }
    }
    Ok(())
}

/// Returns true when the raw URL's authority segment is empty. Given the
/// original string and its parsed scheme, this strips the `scheme://` prefix
/// (case-insensitively, matching Go `net/url` and RFC 3986 §3.1 — a scheme
/// like `HTTP://host` is just as valid as `http://host`) and inspects the
/// host portion up to the next `/`, `?`, or `#`. Used to reproduce Go
/// `net/url`'s rejection of empty-authority URLs like `http:///bar`, which
/// the WHATWG `url` crate would otherwise accept.
fn raw_authority_is_empty(raw: &str, scheme: &str) -> bool {
    // `scheme` comes from `Url::scheme()`, which the `url` crate always
    // normalizes to lowercase; `raw` is the operator's original string and
    // may use any case for the scheme (e.g. `HTTP://host`). Compare
    // case-insensitively so an uppercase/mixed-case scheme isn't spuriously
    // treated as having no `scheme://` marker at all. `.get()` (not slice
    // indexing) avoids a panic if `raw` is shorter than expected.
    let scheme_matches = raw
        .get(..scheme.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(scheme));
    let sep_start = scheme.len();
    if !scheme_matches || raw.get(sep_start..sep_start + 3) != Some("://") {
        // No `://` authority marker at all (e.g. `http:foo`); treat as empty.
        return true;
    }
    let rest = &raw[sep_start + 3..];
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    rest[..authority_end].is_empty()
}

/// Newtype around a compiled regex, anchored exactly like upstream's
/// `Regex.UnmarshalYAML` (auth_config.go:761-770): `"^(?:" + s + ")$"`.
#[derive(Debug)]
struct AnchoredRegex(regex::Regex);

impl<'de> Deserialize<'de> for AnchoredRegex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let anchored = format!("^(?:{s})$");
        let re = regex::Regex::new(&anchored)
            .map_err(|e| D::Error::custom(format!("cannot build regexp from {s:?}: {e}")))?;
        Ok(AnchoredRegex(re))
    }
}

fn deserialize_regexes<'de, D>(deserializer: D) -> Result<Vec<AnchoredRegex>, D::Error>
where
    D: Deserializer<'de>,
{
    let v: Option<Vec<AnchoredRegex>> = Option::deserialize(deserializer)?;
    Ok(v.unwrap_or_default())
}

/// Scalar-or-sequence helper for `url_prefix`/`default_url`, mirroring
/// `URLPrefix.UnmarshalYAML` (auth_config.go:696-734), which accepts either a
/// single string or an array of strings.
#[derive(Deserialize)]
#[serde(untagged)]
enum ScalarOrSeq {
    Scalar(String),
    Seq(Vec<String>),
}

fn deserialize_opt_scalar_or_seq<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    let v: Option<ScalarOrSeq> = Option::deserialize(deserializer)?;
    Ok(v.map(|v| match v {
        ScalarOrSeq::Scalar(s) => vec![s],
        ScalarOrSeq::Seq(v) => v,
    }))
}

/// Parses `"Name: Value"` header strings like upstream's `Header.UnmarshalYAML`
/// (auth_config.go:214-228).
fn parse_header(s: &str) -> Result<(String, String), String> {
    match s.split_once(':') {
        Some((name, value)) => Ok((name.trim().to_string(), value.trim().to_string())),
        None => Err(format!(
            "missing separator char ':' between Name and Value in the header {s:?}; expected format - 'Name: Value'"
        )),
    }
}

fn deserialize_headers<'de, D>(deserializer: D) -> Result<Vec<(String, String)>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw: Vec<String> = Vec::deserialize(deserializer)?;
    raw.iter()
        .map(|s| parse_header(s).map_err(D::Error::custom))
        .collect()
}

fn deserialize_load_balancing_policy<'de, D>(
    deserializer: D,
) -> Result<LoadBalancingPolicy, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    LoadBalancingPolicy::parse(&s).map_err(D::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_bearer_user_with_scalar_url_prefix() {
        let yaml = r#"
users:
- bearer_token: t
  url_prefix: "http://b:8428"
"#;
        let cfg = parse_auth_config(yaml).expect("should parse");
        assert_eq!(cfg.users.len(), 1);
        assert_eq!(cfg.users[0].bearer_token.as_deref(), Some("t"));
        assert_eq!(
            cfg.users[0].url_prefix,
            Some(vec!["http://b:8428".to_string()])
        );
    }

    #[test]
    fn parses_url_prefix_list() {
        let yaml = r#"
users:
- bearer_token: t
  url_prefix:
  - http://b1:8428
  - http://b2:8428
"#;
        let cfg = parse_auth_config(yaml).expect("should parse");
        assert_eq!(
            cfg.users[0].url_prefix,
            Some(vec![
                "http://b1:8428".to_string(),
                "http://b2:8428".to_string()
            ])
        );
    }

    #[test]
    fn parses_url_map() {
        let yaml = r#"
users:
- bearer_token: t
  url_map:
  - src_paths: ["/api/v1/write"]
    url_prefix: "http://ins:8480"
"#;
        let cfg = parse_auth_config(yaml).expect("should parse");
        let um = &cfg.users[0].url_map;
        assert_eq!(um.len(), 1);
        assert_eq!(um[0].url_prefix, vec!["http://ins:8480".to_string()]);
        assert!(um[0].src_paths[0].is_match("/api/v1/write"));
        assert!(!um[0].src_paths[0].is_match("/api/v1/query"));
    }

    #[test]
    fn rejects_invalid_scheme() {
        let yaml = r#"
users:
- bearer_token: t
  url_prefix: "ftp://x"
"#;
        let err = parse_auth_config(yaml).expect_err("should fail");
        assert!(err.contains("scheme"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_missing_host() {
        let yaml = r#"
users:
- bearer_token: t
  url_prefix: "http://"
"#;
        assert!(parse_auth_config(yaml).is_err());
    }

    #[test]
    fn default_load_balancing_is_least_loaded() {
        let yaml = r#"
users:
- bearer_token: t
  url_prefix: "http://b:8428"
"#;
        let cfg = parse_auth_config(yaml).expect("should parse");
        assert_eq!(
            cfg.users[0].load_balancing_policy,
            LoadBalancingPolicy::LeastLoaded
        );
    }

    #[test]
    fn parses_first_available() {
        let yaml = r#"
users:
- bearer_token: t
  url_prefix: "http://b:8428"
  load_balancing_policy: first_available
"#;
        let cfg = parse_auth_config(yaml).expect("should parse");
        assert_eq!(
            cfg.users[0].load_balancing_policy,
            LoadBalancingPolicy::FirstAvailable
        );
    }

    #[test]
    fn rejects_unknown_load_balancing_policy() {
        let yaml = r#"
users:
- bearer_token: t
  url_prefix: "http://b:8428"
  load_balancing_policy: bogus
"#;
        let err = parse_auth_config(yaml).expect_err("should fail");
        assert!(
            err.contains("load_balancing_policy"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn parses_unauthorized_user() {
        let yaml = r#"
unauthorized_user:
  merge_query_args: [extra_filters]
  url_map:
  - src_paths: ["/select/.+"]
    url_prefix: 'http://victoria-logs:9428/'
"#;
        let cfg = parse_auth_config(yaml).expect("should parse");
        assert!(cfg.users.is_empty());
        let uu = cfg.unauthorized_user.expect("unauthorized_user set");
        assert_eq!(uu.merge_query_args, vec!["extra_filters".to_string()]);
        assert_eq!(uu.url_map.len(), 1);
    }

    #[test]
    fn parses_headers() {
        let yaml = r#"
users:
- bearer_token: t
  url_prefix: "http://b:8428"
  headers:
  - "X-Foo: bar"
  response_headers:
  - "X-Resp: baz"
"#;
        let cfg = parse_auth_config(yaml).expect("should parse");
        assert_eq!(
            cfg.users[0].headers,
            vec![("X-Foo".to_string(), "bar".to_string())]
        );
        assert_eq!(
            cfg.users[0].response_headers,
            vec![("X-Resp".to_string(), "baz".to_string())]
        );
    }

    #[test]
    fn rejects_header_without_colon() {
        let yaml = r#"
users:
- bearer_token: t
  url_prefix: "http://b:8428"
  headers:
  - foobar
"#;
        assert!(parse_auth_config(yaml).is_err());
    }

    #[test]
    fn rejects_empty_url_prefix_list() {
        let yaml = r#"
users:
- bearer_token: t
  url_prefix: []
"#;
        assert!(parse_auth_config(yaml).is_err());
    }

    #[test]
    fn rejects_url_map_missing_url_prefix() {
        let yaml = r#"
users:
- bearer_token: t
  url_map:
  - src_paths: ["/foo/bar"]
"#;
        assert!(parse_auth_config(yaml).is_err());
    }

    #[test]
    fn rejects_url_map_missing_src_paths_and_hosts() {
        let yaml = r#"
users:
- bearer_token: t
  url_map:
  - url_prefix: "http://foobar"
"#;
        assert!(parse_auth_config(yaml).is_err());
    }

    #[test]
    fn rejects_url_map_with_src_query_args() {
        let yaml = r#"
users:
- bearer_token: t
  url_map:
  - src_paths: ["/api/v1/query"]
    src_query_args: ["db=foo"]
    url_prefix: "http://foobar"
"#;
        let err = parse_auth_config(yaml).expect_err("should fail");
        assert!(err.contains("src_query_args"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_url_map_with_src_headers() {
        let yaml = r#"
users:
- bearer_token: t
  url_map:
  - src_paths: ["/api/v1/query"]
    src_headers: ["TenantID: 123"]
    url_prefix: "http://foobar"
"#;
        let err = parse_auth_config(yaml).expect_err("should fail");
        assert!(err.contains("src_headers"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_invalid_regexp_in_src_paths() {
        let yaml = r#"
users:
- bearer_token: t
  url_map:
  - src_paths: ["fo[obar"]
    url_prefix: "http://foobar"
"#;
        assert!(parse_auth_config(yaml).is_err());
    }

    #[test]
    fn rejects_user_without_url_prefix_or_url_map() {
        let yaml = r#"
users:
- bearer_token: t
"#;
        let err = parse_auth_config(yaml).expect_err("should fail");
        assert!(
            err.contains("url_prefix") && err.contains("url_map"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn accepts_uppercase_scheme_prefix() {
        // `raw_authority_is_empty` used to strip `scheme://` case-sensitively,
        // so an uppercase (or mixed-case) scheme like `HTTP://` was spuriously
        // treated as having no authority marker at all and rejected as
        // "missing hostname" even though the host is present.
        let yaml = r#"
users:
- bearer_token: t
  url_prefix: "HTTP://host:8428"
"#;
        let cfg = parse_auth_config(yaml).expect("uppercase scheme should be accepted");
        assert_eq!(
            cfg.users[0].url_prefix,
            Some(vec!["HTTP://host:8428".to_string()])
        );
    }

    #[test]
    fn rejects_empty_authority_url() {
        // Go net/url parses `http:///bar` with an empty Host and rejects it;
        // the WHATWG `url` crate would accept it with host "bar". We must reject.
        let yaml = r#"
users:
- bearer_token: t
  url_prefix: "http:///bar"
"#;
        let err = parse_auth_config(yaml).expect_err("should fail");
        assert!(
            err.contains("hostname") || err.contains("host"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn src_paths_anchoring_is_full_match() {
        // Proves `^(?:...)$` anchoring: exact match only, no superstring or
        // substring/prefixed match (which a naive substring matcher allows).
        let yaml = r#"
users:
- bearer_token: t
  url_map:
  - src_paths: ["/api/v1/write"]
    url_prefix: "http://ins:8480"
"#;
        let cfg = parse_auth_config(yaml).expect("should parse");
        let re = &cfg.users[0].url_map[0].src_paths[0];
        assert!(re.is_match("/api/v1/write"));
        assert!(!re.is_match("/api/v1/write/extra"));
        assert!(!re.is_match("/x/api/v1/write"));
    }

    #[test]
    fn default_url_parses_and_validates() {
        let yaml = r#"
users:
- bearer_token: t
  url_map:
  - src_paths: ["/foo"]
    url_prefix: "http://ins:8480"
  default_url:
  - http://d1:8428
  - http://d2:8428
"#;
        let cfg = parse_auth_config(yaml).expect("should parse");
        assert_eq!(
            cfg.users[0].default_url,
            Some(vec![
                "http://d1:8428".to_string(),
                "http://d2:8428".to_string()
            ])
        );

        // Invalid default_url is rejected through the same validation path.
        let bad = r#"
users:
- bearer_token: t
  url_prefix: "http://b:8428"
  default_url: "ftp://x"
"#;
        assert!(parse_auth_config(bad).is_err());
    }

    #[test]
    fn retry_status_codes_and_drop_src_path_prefix_parts() {
        let yaml = r#"
users:
- username: foo
  password: bar
  url_prefix:
  - http://node1:343/bbb
  retry_status_codes: [500, 501]
  merge_query_args: [foo, bar]
  drop_src_path_prefix_parts: 1
"#;
        let cfg = parse_auth_config(yaml).expect("should parse");
        let u = &cfg.users[0];
        assert_eq!(u.retry_status_codes, vec![500, 501]);
        assert_eq!(
            u.merge_query_args,
            vec!["foo".to_string(), "bar".to_string()]
        );
        assert_eq!(u.drop_src_path_prefix_parts, Some(1));
    }
}
