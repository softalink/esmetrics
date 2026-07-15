//! Query-string / form-urlencoded helpers.

use std::borrow::Cow;

/// Decodes `%XX` percent-escapes. Invalid or truncated escapes are passed
/// through literally. Returns `Cow::Borrowed` when no decoding is needed.
pub fn percent_decode(s: &str) -> Cow<'_, str> {
    decode(s, false)
}

/// Decodes `%XX` percent-escapes and treats `'+'` as a space
/// (query-string / `application/x-www-form-urlencoded` semantics).
pub fn percent_decode_plus(s: &str) -> Cow<'_, str> {
    decode(s, true)
}

/// Parses a raw query string (`a=1&b=hello+world&c=%2Fx`) into decoded
/// key/value pairs. Keys without `=` yield an empty value. Empty segments
/// (`a=1&&b=2`) are skipped.
pub fn parse_query(query: &str) -> impl Iterator<Item = (Cow<'_, str>, Cow<'_, str>)> {
    query.split('&').filter(|kv| !kv.is_empty()).map(|kv| {
        let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
        (percent_decode_plus(k), percent_decode_plus(v))
    })
}

/// Parses an `application/x-www-form-urlencoded` body. The wire format is
/// identical to a query string.
pub fn parse_form(body: &str) -> impl Iterator<Item = (Cow<'_, str>, Cow<'_, str>)> {
    parse_query(body)
}

/// Strictly decodes `%XX` percent-escapes in a URL *path*, mirroring Go's
/// `url.Parse`: an invalid or truncated escape is an error (Go's net/http
/// answers 400), and `+` stays literal (no form semantics in paths).
/// Returns `Cow::Borrowed` when no decoding is needed.
///
/// Deviation from Go: decoded bytes must be valid UTF-8 (Go strings carry
/// arbitrary bytes); non-UTF-8 escapes are rejected as an error too. No
/// real client sends such paths, and they could never match a route.
pub fn percent_decode_path(s: &str) -> Result<Cow<'_, str>, ()> {
    let bytes = s.as_bytes();
    if !bytes.contains(&b'%') {
        return Ok(Cow::Borrowed(s));
    }
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return Err(());
            }
            match (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                (Some(hi), Some(lo)) => {
                    out.push((hi << 4) | lo);
                    i += 3;
                }
                _ => return Err(()),
            }
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map(Cow::Owned).map_err(|_| ())
}

fn decode(s: &str, plus_as_space: bool) -> Cow<'_, str> {
    let bytes = s.as_bytes();
    let needs_decode = bytes
        .iter()
        .any(|&b| b == b'%' || (plus_as_space && b == b'+'));
    if !needs_decode {
        return Cow::Borrowed(s);
    }
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' if plus_as_space => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                if let (Some(hi), Some(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                    out.push((hi << 4) | lo);
                    i += 3;
                } else {
                    out.push(b'%');
                    i += 1;
                }
            }
            other => {
                out.push(other);
                i += 1;
            }
        }
    }
    match String::from_utf8(out) {
        Ok(decoded) => Cow::Owned(decoded),
        Err(err) => Cow::Owned(String::from_utf8_lossy(err.as_bytes()).into_owned()),
    }
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod path_decode_tests {
    use super::*;

    #[test]
    fn percent_decode_path_decodes_like_go_url_path() {
        // No escapes: borrowed, unchanged.
        assert!(matches!(
            percent_decode_path("/api/v1/write").unwrap(),
            Cow::Borrowed("/api/v1/write")
        ));
        // %2F decodes to '/', changing routing — Go routes on URL.Path.
        assert_eq!(
            percent_decode_path("/api/v1%2Fwrite").unwrap(),
            "/api/v1/write"
        );
        assert_eq!(percent_decode_path("/he%61lth").unwrap(), "/health");
        assert_eq!(percent_decode_path("/a%20b").unwrap(), "/a b");
        // '+' stays literal in paths (no form semantics).
        assert_eq!(percent_decode_path("/a+b").unwrap(), "/a+b");
    }

    #[test]
    fn percent_decode_path_rejects_invalid_escapes() {
        // Go's url.Parse fails on these and net/http answers 400.
        assert!(percent_decode_path("/a%zz").is_err());
        assert!(percent_decode_path("/a%4").is_err());
        assert!(percent_decode_path("/a%").is_err());
        // Non-UTF-8 decode result (documented deviation: rejected).
        assert!(percent_decode_path("/a%ff%fe").is_err());
    }
}
