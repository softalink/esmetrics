//! Request parameter collection mirroring Go `net/http.Request.Form`
//! semantics: POST bodies (`application/x-www-form-urlencoded`) are merged
//! with URL query args; body values come first, so `FormValue` prefers them
//! — except for the `extra_*` security args, which prefer URL query values
//! (see `searchutil.GetExtraTagFilters`).

use esm_http::{Method, Request};

/// Cap on the decoded form body size (Go's `http.maxFormSize` is 10 MiB).
const MAX_FORM_SIZE: usize = 10 << 20;

pub(crate) struct Params {
    /// Decoded URL query args, in order.
    query: Vec<(String, String)>,
    /// Decoded POST form args, in order.
    form: Vec<(String, String)>,
}

impl Params {
    /// Collects args from the URL query string and, for POST requests, the
    /// urlencoded body. An unreadable/oversized body yields form-less
    /// params.
    pub(crate) fn from_request(req: &mut Request<'_>) -> Params {
        let query = req
            .query_params()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        let mut form = Vec::new();
        if req.method() == Method::Post {
            let mut body = Vec::new();
            if req.read_body_to(&mut body, MAX_FORM_SIZE).is_ok() {
                if let Ok(body) = std::str::from_utf8(&body) {
                    form = esm_http::parse_form(body)
                        .map(|(k, v)| (k.into_owned(), v.into_owned()))
                        .collect();
                }
            }
        }
        Params { query, form }
    }

    #[cfg(test)]
    pub(crate) fn from_query_string(query: &str) -> Params {
        Params {
            query: esm_http::parse_query(query)
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect(),
            form: Vec::new(),
        }
    }

    /// Go `Request.FormValue`: first value in `r.Form` (POST form values
    /// precede URL query values).
    pub(crate) fn get(&self, key: &str) -> Option<&str> {
        self.form
            .iter()
            .chain(self.query.iter())
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// All values for `key` in `r.Form` order (form first, then query).
    pub(crate) fn get_all(&self, key: &str) -> Vec<&str> {
        self.form
            .iter()
            .chain(self.query.iter())
            .filter(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
            .collect()
    }

    /// URL query values when the key is present there, otherwise form
    /// values (`searchutil.GetExtraTagFilters` precedence rule).
    pub(crate) fn get_all_url_preferred(&self, key: &str) -> Vec<&str> {
        let from_query: Vec<&str> = self
            .query
            .iter()
            .filter(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
            .collect();
        if !from_query.is_empty() {
            return from_query;
        }
        self.form
            .iter()
            .filter(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
            .collect()
    }

    /// `match[]` plus legacy `match` args, in `r.Form` order.
    pub(crate) fn matches(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .get_all("match[]")
            .into_iter()
            .map(str::to_string)
            .collect();
        out.extend(self.get_all("match").into_iter().map(str::to_string));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_value_wins_and_get_all_collects() {
        let p = Params {
            query: vec![
                ("a".into(), "1".into()),
                ("a".into(), "2".into()),
                ("b".into(), "q".into()),
            ],
            form: vec![("b".into(), "f".into())],
        };
        assert_eq!(p.get("a"), Some("1"));
        // Form value precedes the query value, like Go's r.Form for POST.
        assert_eq!(p.get("b"), Some("f"));
        assert_eq!(p.get_all("a"), vec!["1", "2"]);
        assert_eq!(p.get_all("b"), vec!["f", "q"]);
        // extra_* precedence: query wins when present.
        assert_eq!(p.get_all_url_preferred("b"), vec!["q"]);
        assert_eq!(p.get("missing"), None);
    }
}
