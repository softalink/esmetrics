//! HTTP client for exporting/importing time series via the native protocol.
//! Ports `app/vmctl/native/client.go`.

use std::io::Read;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use reqwest::blocking::{Client, Response};

use crate::auth::AuthConfig;

const NATIVE_TENANTS_ADDR: &str = "admin/tenants";
const NATIVE_METRIC_NAMES_ADDR: &str = "api/v1/label/__name__/values";

/// A native export/import filter. Ports `native.Filter`.
#[derive(Clone, Default)]
pub(crate) struct Filter {
    pub(crate) matcher: String,
    pub(crate) time_start: String,
    pub(crate) time_end: String,
}

/// An HTTP client for one endpoint (source or destination). Ports
/// `native.Client`.
pub(crate) struct NativeClient {
    pub(crate) addr: String,
    pub(crate) auth: AuthConfig,
    pub(crate) extra_labels: Vec<String>,
    pub(crate) http: Client,
}

impl NativeClient {
    /// Discovers metric names matching `filter` in `[start, end]`. Ports
    /// `Client.Explore`.
    pub(crate) fn explore(
        &self,
        filter: &crate::native::Filter,
        tenant_id: &str,
        start_rfc3339: &str,
        end_rfc3339: &str,
    ) -> Result<Vec<String>, String> {
        let url = if tenant_id.is_empty() {
            format!("{}/{}", self.addr, NATIVE_METRIC_NAMES_ADDR)
        } else {
            format!(
                "{}/select/{}/prometheus/{}",
                self.addr, tenant_id, NATIVE_METRIC_NAMES_ADDR
            )
        };
        let rb = self.http.get(&url).query(&[
            ("start", start_rfc3339),
            ("end", end_rfc3339),
            ("match[]", &filter.matcher),
        ]);
        let resp = self.send(rb, 200)?;
        let body = resp
            .text()
            .map_err(|e| format!("cannot read series response: {e}"))?;
        let parsed: MetricNamesResponse = serde_json::from_str(&body)
            .map_err(|e| format!("cannot decode series response: {e}"))?;
        Ok(parsed.data)
    }

    /// Opens a streaming export reader for `filter`. Ports `Client.ExportPipe`.
    pub(crate) fn export(
        &self,
        url: &str,
        filter: &crate::native::Filter,
    ) -> Result<Response, String> {
        let mut query: Vec<(&str, &str)> = vec![("match[]", &filter.matcher)];
        if !filter.time_start.is_empty() {
            query.push(("start", &filter.time_start));
        }
        if !filter.time_end.is_empty() {
            query.push(("end", &filter.time_end));
        }
        let rb = self
            .http
            .get(url)
            .query(&query)
            // Disable compression: it is meaningless for the native format.
            .header("Accept-Encoding", "identity");
        self.send(rb, 200)
    }

    /// Streams `body` to the import endpoint. Ports `Client.ImportPipe`.
    /// Returns the number of bytes streamed.
    pub(crate) fn import<R: Read + Send + 'static>(
        &self,
        dst_url: &str,
        body: R,
    ) -> Result<u64, String> {
        let counter = Arc::new(AtomicU64::new(0));
        let counting = CountingReader {
            inner: body,
            count: Arc::clone(&counter),
        };
        let rb = self
            .http
            .post(dst_url)
            .body(reqwest::blocking::Body::new(counting));
        self.send(rb, 204)?;
        Ok(counter.load(Ordering::SeqCst))
    }

    /// Discovers source tenants. Ports `Client.GetSourceTenants`.
    pub(crate) fn get_source_tenants(
        &self,
        filter: &crate::native::Filter,
    ) -> Result<Vec<String>, String> {
        let url = format!("{}/{}", self.addr, NATIVE_TENANTS_ADDR);
        let mut query: Vec<(&str, &str)> = Vec::new();
        if !filter.time_start.is_empty() {
            query.push(("start", &filter.time_start));
        }
        if !filter.time_end.is_empty() {
            query.push(("end", &filter.time_end));
        }
        let rb = self.http.get(&url).query(&query);
        let resp = self.send(rb, 200)?;
        let body = resp
            .text()
            .map_err(|e| format!("cannot read tenants response: {e}"))?;
        let parsed: TenantsResponse = serde_json::from_str(&body)
            .map_err(|e| format!("cannot decode tenants response: {e}"))?;
        Ok(parsed.data)
    }

    fn send(
        &self,
        rb: reqwest::blocking::RequestBuilder,
        expected: u16,
    ) -> Result<Response, String> {
        let rb = self.auth.apply(rb);
        let resp = rb
            .send()
            .map_err(|e| format!("unexpected error when performing request: {e}"))?;
        let status = resp.status().as_u16();
        if status != expected {
            let body = resp.text().unwrap_or_default();
            return Err(format!("unexpected response code {status}: {body}"));
        }
        Ok(resp)
    }
}

/// Appends `extra_labels` (each `name=value`) as `extra_label` query params to
/// an import path. Ports `vm.AddExtraLabelsToImportPath`.
pub(crate) fn add_extra_labels_to_import_path(
    path: &str,
    extra_labels: &[String],
) -> Result<String, String> {
    if extra_labels.is_empty() {
        return Ok(path.to_string());
    }
    let mut out = path.to_string();
    let mut sep = if path.contains('?') { '&' } else { '?' };
    for label in extra_labels {
        if !label.contains('=') || label.starts_with('=') {
            return Err(format!(
                "incorrect format for extra_label flag, it must be `name=value`, got: {label:?}"
            ));
        }
        out.push(sep);
        out.push_str("extra_label=");
        out.push_str(label);
        sep = '&';
    }
    Ok(out)
}

struct CountingReader<R> {
    inner: R,
    count: Arc<AtomicU64>,
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.count.fetch_add(n as u64, Ordering::SeqCst);
        Ok(n)
    }
}

#[derive(serde::Deserialize)]
struct MetricNamesResponse {
    #[serde(default)]
    data: Vec<String>,
}

#[derive(serde::Deserialize)]
struct TenantsResponse {
    #[serde(default)]
    data: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extra_labels_appended() {
        let got = add_extra_labels_to_import_path(
            "api/v1/import",
            &["a=1".to_string(), "b=2".to_string()],
        )
        .unwrap();
        assert_eq!(got, "api/v1/import?extra_label=a=1&extra_label=b=2");
    }

    #[test]
    fn extra_labels_appended_with_existing_query() {
        let got =
            add_extra_labels_to_import_path("api/v1/import?x=1", &["a=1".to_string()]).unwrap();
        assert_eq!(got, "api/v1/import?x=1&extra_label=a=1");
    }

    #[test]
    fn extra_labels_reject_bad_format() {
        assert!(add_extra_labels_to_import_path("p", &["nope".to_string()]).is_err());
        assert!(add_extra_labels_to_import_path("p", &["=v".to_string()]).is_err());
    }

    #[test]
    fn no_extra_labels_is_identity() {
        assert_eq!(add_extra_labels_to_import_path("p", &[]).unwrap(), "p");
    }
}
