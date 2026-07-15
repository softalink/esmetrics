//! AWS Signature Version 4 request signing for the `ec2` query API.
//!
//! Port of `lib/awsapi/sign.go` (v1.146.0): the canonical-request ->
//! string-to-sign -> HMAC-SHA256 signing-key chain -> `Authorization:
//! AWS4-HMAC-SHA256 ...` header. Only the GET-request path is ported (EC2
//! discovery issues signed GETs with an empty body), so the payload hash is
//! always `SHA256("")`.
//!
//! See <https://docs.aws.amazon.com/general/latest/gr/sigv4-signed-request-examples.html>.

use std::fmt::Write as _;
use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use url::Url;

type HmacSha256 = Hmac<Sha256>;

/// AWS API credentials used to sign a request. A `token` (STS session token)
/// is attached as the `X-Amz-Security-Token` header when present.
///
/// `Debug` is hand-written to redact `secret_key` / `token` — defense-in-depth
/// against a future `{:?}` in a log line (mirrors `Ec2Api`/`Ec2SdConfig`).
#[derive(Clone, Default)]
pub struct AwsCredentials {
    pub access_key: String,
    pub secret_key: String,
    pub token: Option<String>,
}

impl std::fmt::Debug for AwsCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsCredentials")
            .field("access_key", &self.access_key)
            .field("secret_key", &"<redacted>")
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// The SigV4 algorithm identifier.
const ALGORITHM: &str = "AWS4-HMAC-SHA256";

/// Computes the signed-request headers for a GET to `url`, mirroring
/// `signRequestWithTime` with the empty-body payload hash. Returns the
/// headers to attach: `x-amz-date`, `Authorization`, and (when a session
/// token is present) `X-Amz-Security-Token`.
///
/// `amz_date` is `YYYYMMDDThhmmssZ` and `date_stamp` is `YYYYMMDD`, both in
/// UTC — see [`format_amz_time`]. They are parameters (rather than read from
/// the clock here) so the signing is deterministic and testable against a
/// fixed AWS SigV4 vector.
pub fn signed_get_headers(
    url: &Url,
    service: &str,
    region: &str,
    creds: &AwsCredentials,
    amz_date: &str,
    date_stamp: &str,
) -> Vec<(String, String)> {
    let host = canonical_host(url);
    let canonical_uri = url.path();
    let canonical_qs = canonical_query_string(url);
    let canonical_headers = format!("host:{host}\nx-amz-date:{amz_date}\n");
    let signed_headers = "host;x-amz-date";
    let payload_hash = sha256_hex(b"");

    let canonical_request = [
        "GET",
        canonical_uri,
        &canonical_qs,
        &canonical_headers,
        signed_headers,
        &payload_hash,
    ]
    .join("\n");

    let credential_scope = format!("{date_stamp}/{region}/{service}/aws4_request");
    let string_to_sign = [
        ALGORITHM,
        amz_date,
        &credential_scope,
        &sha256_hex(canonical_request.as_bytes()),
    ]
    .join("\n");

    let signing_key = signature_key(&creds.secret_key, date_stamp, region, service);
    let signature = to_hex(&hmac_sha256(&signing_key, string_to_sign.as_bytes()));

    let authorization = format!(
        "{ALGORITHM} Credential={}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}",
        creds.access_key
    );

    let mut headers = vec![
        ("x-amz-date".to_string(), amz_date.to_string()),
        ("Authorization".to_string(), authorization),
    ];
    if let Some(token) = creds.token.as_ref().filter(|t| !t.is_empty()) {
        headers.push(("X-Amz-Security-Token".to_string(), token.clone()));
    }
    headers
}

/// The `Host` header value the signature covers: the URL host, plus `:port`
/// only when the URL carries an explicit non-default port (matching Go's
/// `uri.Host` and the `Host` header `reqwest` sends).
fn canonical_host(url: &Url) -> String {
    let host = url.host_str().unwrap_or_default();
    match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    }
}

/// Builds the canonical query string: query params sorted by key (values in
/// original order for a repeated key), each key and value AWS-URI-encoded
/// (unreserved `A-Za-z0-9-_.~` pass through, everything else — including
/// space — becomes uppercase `%XX`). Equivalent to Go's
/// `url.Values.Encode()` followed by the `"+"` -> `"%20"` fixup.
fn canonical_query_string(url: &Url) -> String {
    let mut pairs: Vec<(String, String)> = url.query_pairs().into_owned().collect();
    // Stable sort by key only, preserving value order for repeated keys —
    // matches Go's url.Values.Encode.
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", aws_uri_encode(k), aws_uri_encode(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// AWS URI encoding for a query-string component: unreserved characters
/// (`A-Za-z0-9-_.~`) pass through; every other byte becomes uppercase
/// `%XX` (UTF-8 byte-wise). Space therefore encodes to `%20`.
fn aws_uri_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => {
                let _ = write!(out, "%{byte:02X}");
            }
        }
    }
    out
}

/// The SigV4 signing key: HMAC-SHA256 chain seeded with `"AWS4"+secret`, then
/// keyed successively by `date_stamp`, `region`, `service`, `"aws4_request"`.
fn signature_key(secret: &str, date_stamp: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date_stamp.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    to_hex(&hasher.finalize())
}

fn to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Formats a UTC [`SystemTime`] as `(amz_date, date_stamp)` =
/// (`YYYYMMDDThhmmssZ`, `YYYYMMDD`). Computed from the Unix timestamp with a
/// self-contained civil-date algorithm (no date-library dependency).
pub fn format_amz_time(t: SystemTime) -> (String, String) {
    let secs = t
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hh = sod / 3600;
    let mm = (sod % 3600) / 60;
    let ss = sod % 60;
    let amz_date = format!("{year:04}{month:02}{day:02}T{hh:02}{mm:02}{ss:02}Z");
    let date_stamp = format!("{year:04}{month:02}{day:02}");
    (amz_date, date_stamp)
}

/// Converts a count of days since the Unix epoch to a `(year, month, day)`
/// Gregorian date. Howard Hinnant's `civil_from_days` algorithm.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The load-bearing correctness test: an exact AWS SigV4 vector.
    ///
    /// Vector taken from upstream `lib/awsapi/sign_test.go`
    /// (`TestNewSignedRequest`): access key `fake-access-key` x2, secret
    /// `foobar` x10, time `Unix(0,0)` UTC (`19700101T000000Z`), service
    /// `ec2`, region `us-east-1`, GET
    /// `https://ec2.amazonaws.com/?Action=DescribeRegions&Version=2013-10-15`.
    /// The resulting `Authorization` header must match byte-for-byte.
    #[test]
    fn signs_known_aws_sigv4_vector() {
        let url =
            Url::parse("https://ec2.amazonaws.com/?Action=DescribeRegions&Version=2013-10-15")
                .unwrap();
        let creds = AwsCredentials {
            access_key: "fake-access-key".repeat(2),
            secret_key: "foobar".repeat(10),
            token: None,
        };
        // Unix(0,0) UTC.
        let (amz_date, date_stamp) = format_amz_time(UNIX_EPOCH);
        assert_eq!(amz_date, "19700101T000000Z");
        assert_eq!(date_stamp, "19700101");

        let headers = signed_get_headers(&url, "ec2", "us-east-1", &creds, &amz_date, &date_stamp);
        let auth = headers
            .iter()
            .find(|(k, _)| k == "Authorization")
            .map(|(_, v)| v.as_str())
            .expect("Authorization header");

        assert_eq!(
            auth,
            "AWS4-HMAC-SHA256 Credential=fake-access-keyfake-access-key/19700101/us-east-1/ec2/aws4_request, SignedHeaders=host;x-amz-date, Signature=e6c0f635693173f83eea9f443ae364d9099c98b0f5e7b1356e7cfc9c742daea2"
        );

        // No session token -> no X-Amz-Security-Token header.
        assert!(!headers.iter().any(|(k, _)| k == "X-Amz-Security-Token"));
        // x-amz-date is set to the provided timestamp.
        assert!(headers
            .iter()
            .any(|(k, v)| k == "x-amz-date" && v == "19700101T000000Z"));
    }

    #[test]
    fn attaches_session_token_when_present() {
        let url =
            Url::parse("https://ec2.us-east-1.amazonaws.com/?Action=DescribeInstances").unwrap();
        let creds = AwsCredentials {
            access_key: "AKID".into(),
            secret_key: "secret".into(),
            token: Some("session-token-xyz".into()),
        };
        let headers = signed_get_headers(
            &url,
            "ec2",
            "us-east-1",
            &creds,
            "20200101T000000Z",
            "20200101",
        );
        assert!(headers
            .iter()
            .any(|(k, v)| k == "X-Amz-Security-Token" && v == "session-token-xyz"));
    }

    #[test]
    fn canonical_host_includes_explicit_port() {
        let url = Url::parse("http://127.0.0.1:8123/?Action=DescribeInstances").unwrap();
        assert_eq!(canonical_host(&url), "127.0.0.1:8123");
        let url2 = Url::parse("https://ec2.amazonaws.com/?x=1").unwrap();
        assert_eq!(canonical_host(&url2), "ec2.amazonaws.com");
    }

    #[test]
    fn canonical_query_string_sorts_and_encodes() {
        let url = Url::parse("https://h/?B=2&A=a+b&Version=2013-10-15").unwrap();
        // Sorted by key; space in the value encodes to %20.
        assert_eq!(
            canonical_query_string(&url),
            "A=a%20b&B=2&Version=2013-10-15"
        );
    }

    #[test]
    fn aws_credentials_debug_redacts_secret_and_token() {
        let creds = AwsCredentials {
            access_key: "AKIDEXAMPLE".into(),
            secret_key: "super-secret-key".into(),
            token: Some("session-token-xyz".into()),
        };
        let dbg = format!("{creds:?}");
        assert!(dbg.contains("AKIDEXAMPLE"), "{dbg}");
        assert!(!dbg.contains("super-secret-key"), "{dbg}");
        assert!(!dbg.contains("session-token-xyz"), "{dbg}");
    }

    #[test]
    fn civil_from_days_epoch_and_known_dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        // 2020-02-29 is day 18321 since the epoch (leap day).
        let (y, m, d) = civil_from_days(18_321);
        assert_eq!((y, m, d), (2020, 2, 29));
    }
}
