//! Minimal synchronous HTTP/1.1 client over a Unix domain socket, plus the
//! shared HTTP response parser (status line + headers + body, honoring
//! `Transfer-Encoding: chunked` or `Content-Length`).
//!
//! `reqwest::blocking` cannot speak HTTP over a Unix socket, but the Docker
//! Engine API is most commonly reached via `unix:///var/run/docker.sock`. This
//! module fills that gap with a hand-rolled HTTP/1.1 `GET`: connect the
//! [`UnixStream`], write a `Connection: close` request, read the whole
//! response to EOF, and parse it. It is deliberately generic (socket path +
//! request path -> status + body) so the forthcoming `dockerswarm_sd_configs`
//! provider can reuse it unchanged.
//!
//! The parser never panics on malformed input — every short/garbled buffer
//! yields a [`ScrapeError`] rather than an out-of-bounds slice. Chunked
//! decoding is required because dockerd streams `/containers/json` and
//! `/networks` with `Transfer-Encoding: chunked`.
//!
//! Unix-socket support is gated behind `#[cfg(unix)]`; the `#[cfg(not(unix))]`
//! stub returns an error so the Windows build still compiles.

use std::time::Duration;

use crate::scrape::config::ScrapeError;

/// Hard ceiling on a single Docker Engine API response body (before framing
/// is removed). Comfortably above any real `/containers/json` or `/networks`
/// response, but bounds memory so a lying/hostile `Content-Length` or an
/// endless stream can't grow the read buffer without limit. Enforced by
/// [`read_capped`], which is what backs the `read_to_end` on the socket.
#[cfg(unix)]
const DOCKER_MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

/// A parsed HTTP response: the numeric status code and the decoded body
/// bytes (chunked/content-length framing already removed).
pub(crate) struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

/// Issues a synchronous HTTP/1.1 `GET request_path` over the Unix socket at
/// `socket_path`, returning the parsed [`HttpResponse`]. Connect, read, and
/// write are all bounded by `timeout` so a hung dockerd can't stall the
/// caller (and thus a provider `Drop`/`stop`) indefinitely.
///
/// Never panics: connection, I/O, and parse failures are all returned as
/// [`ScrapeError`].
#[cfg(unix)]
pub(crate) fn unix_socket_get(
    socket_path: &str,
    request_path: &str,
    timeout: Duration,
) -> Result<HttpResponse, ScrapeError> {
    use std::io::Write;
    use std::os::unix::net::UnixStream;

    let mut stream = UnixStream::connect(socket_path).map_err(|e| ScrapeError {
        msg: format!("cannot connect to docker unix socket {socket_path:?}: {e}"),
    })?;
    stream
        .set_read_timeout(Some(timeout))
        .and_then(|()| stream.set_write_timeout(Some(timeout)))
        .map_err(|e| ScrapeError {
            msg: format!("cannot set docker unix socket timeout: {e}"),
        })?;

    let request = format!(
        "GET {request_path} HTTP/1.1\r\nHost: localhost\r\nAccept: application/json\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .and_then(|()| stream.flush())
        .map_err(|e| ScrapeError {
            msg: format!("cannot write to docker unix socket {socket_path:?}: {e}"),
        })?;

    let raw = read_capped(&stream, DOCKER_MAX_RESPONSE_BYTES, socket_path)?;

    parse_http_response(&raw)
}

/// Reads `reader` to EOF but refuses to buffer more than `max` bytes: reads
/// at most `max + 1` and errors if that boundary is crossed, so a
/// lying/huge/endless response can't grow memory without limit. Returns the
/// full body on success (`<= max` bytes).
#[cfg(unix)]
fn read_capped(
    reader: impl std::io::Read,
    max: usize,
    socket_path: &str,
) -> Result<Vec<u8>, ScrapeError> {
    use std::io::Read;

    let mut raw = Vec::new();
    reader
        .take(max as u64 + 1)
        .read_to_end(&mut raw)
        .map_err(|e| ScrapeError {
            msg: format!("cannot read from docker unix socket {socket_path:?}: {e}"),
        })?;
    if raw.len() > max {
        return Err(ScrapeError {
            msg: format!("docker response from {socket_path:?} exceeded {max} bytes"),
        });
    }
    Ok(raw)
}

/// Non-Unix stub: there is no Unix-socket transport on this platform, so a
/// `unix://` Docker host is rejected at fetch time. Keeps the Windows build
/// compiling.
#[cfg(not(unix))]
pub(crate) fn unix_socket_get(
    _socket_path: &str,
    _request_path: &str,
    _timeout: Duration,
) -> Result<HttpResponse, ScrapeError> {
    Err(ScrapeError {
        msg: "docker unix socket hosts are not supported on this platform".to_string(),
    })
}

/// Parses a raw HTTP/1.1 response buffer into an [`HttpResponse`]: the status
/// line's numeric code, then the body framed by `Transfer-Encoding: chunked`
/// (decoded) or `Content-Length` (truncated), else taken verbatim (the
/// `Connection: close` read-to-EOF case). Bounds-safe on every path.
#[cfg(unix)]
pub(crate) fn parse_http_response(raw: &[u8]) -> Result<HttpResponse, ScrapeError> {
    let header_end = find_subsequence(raw, b"\r\n\r\n").ok_or_else(|| ScrapeError {
        msg: "malformed docker response: no header/body separator".to_string(),
    })?;
    let head = &raw[..header_end];
    let body_raw = &raw[header_end + 4..];

    let mut lines = head.split(|&b| b == b'\n').map(trim_cr);
    let status_line = lines.next().ok_or_else(|| ScrapeError {
        msg: "malformed docker response: empty status line".to_string(),
    })?;
    let status = parse_status_code(status_line)?;

    let mut is_chunked = false;
    let mut content_length: Option<usize> = None;
    for line in lines {
        let Some(colon) = line.iter().position(|&b| b == b':') else {
            continue;
        };
        let name = trim_ascii(&line[..colon]);
        let value = trim_ascii(&line[colon + 1..]);
        if name.eq_ignore_ascii_case(b"transfer-encoding")
            && contains_token_ignore_case(value, b"chunked")
        {
            is_chunked = true;
        } else if name.eq_ignore_ascii_case(b"content-length") {
            content_length = std::str::from_utf8(value)
                .ok()
                .and_then(|s| s.trim().parse::<usize>().ok());
        }
    }

    let body = if is_chunked {
        decode_chunked(body_raw)?
    } else if let Some(len) = content_length {
        body_raw[..len.min(body_raw.len())].to_vec()
    } else {
        body_raw.to_vec()
    };

    Ok(HttpResponse { status, body })
}

/// Parses `HTTP/1.1 <code> <reason>`'s numeric status code.
#[cfg(unix)]
fn parse_status_code(status_line: &[u8]) -> Result<u16, ScrapeError> {
    let mut parts = status_line.split(|&b| b == b' ').filter(|p| !p.is_empty());
    let _http = parts.next();
    let code = parts.next().ok_or_else(|| ScrapeError {
        msg: "malformed docker response: no status code".to_string(),
    })?;
    std::str::from_utf8(code)
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| ScrapeError {
            msg: "malformed docker response: unparsable status code".to_string(),
        })
}

/// Decodes a `Transfer-Encoding: chunked` body. Each chunk is a hex length
/// line (chunk extensions after `;` ignored), the chunk data, and a trailing
/// CRLF; a zero-length chunk terminates. Returns an error on any truncated or
/// malformed chunk rather than panicking.
#[cfg(unix)]
fn decode_chunked(data: &[u8]) -> Result<Vec<u8>, ScrapeError> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    loop {
        let rel = find_subsequence(&data[pos..], b"\r\n").ok_or_else(|| ScrapeError {
            msg: "malformed docker chunked body: no chunk-size CRLF".to_string(),
        })?;
        let size_line = trim_ascii(&data[pos..pos + rel]);
        let hex = match size_line.iter().position(|&b| b == b';') {
            Some(i) => &size_line[..i],
            None => size_line,
        };
        let size =
            usize::from_str_radix(std::str::from_utf8(hex).unwrap_or(""), 16).map_err(|_| {
                ScrapeError {
                    msg: "malformed docker chunked body: bad chunk size".to_string(),
                }
            })?;
        pos += rel + 2;
        if size == 0 {
            return Ok(out);
        }
        let end = pos.checked_add(size).filter(|&e| e <= data.len());
        let Some(end) = end else {
            return Err(ScrapeError {
                msg: "malformed docker chunked body: chunk exceeds buffer".to_string(),
            });
        };
        out.extend_from_slice(&data[pos..end]);
        pos = end;
        // Skip the CRLF that follows every chunk's data.
        if data.get(pos) == Some(&b'\r') && data.get(pos + 1) == Some(&b'\n') {
            pos += 2;
        } else {
            return Err(ScrapeError {
                msg: "malformed docker chunked body: missing chunk terminator".to_string(),
            });
        }
    }
}

/// First index of `needle` in `haystack`, or `None`.
#[cfg(unix)]
fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Drops a single trailing `\r` (for lines produced by splitting on `\n`).
#[cfg(unix)]
fn trim_cr(line: &[u8]) -> &[u8] {
    match line.split_last() {
        Some((b'\r', rest)) => rest,
        _ => line,
    }
}

/// Trims leading/trailing ASCII whitespace.
#[cfg(unix)]
fn trim_ascii(mut s: &[u8]) -> &[u8] {
    while let [first, rest @ ..] = s {
        if first.is_ascii_whitespace() {
            s = rest;
        } else {
            break;
        }
    }
    while let [rest @ .., last] = s {
        if last.is_ascii_whitespace() {
            s = rest;
        } else {
            break;
        }
    }
    s
}

/// Whether `value` contains `token` (case-insensitive) as one of its
/// comma-separated tokens — matches how `Transfer-Encoding: gzip, chunked`
/// lists coding tokens.
#[cfg(unix)]
fn contains_token_ignore_case(value: &[u8], token: &[u8]) -> bool {
    value
        .split(|&b| b == b',')
        .map(trim_ascii)
        .any(|t| t.eq_ignore_ascii_case(token))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::os::unix::net::UnixListener;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::thread;

    static COUNTER: AtomicU32 = AtomicU32::new(0);

    fn unique_socket_path() -> std::path::PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!(
            "esmagent-docker-transport-{}-{}.sock",
            std::process::id(),
            n
        ))
    }

    /// A Unix-socket HTTP/1.1 server that answers one request with a canned
    /// chunked response carrying `json_body`, then closes. Proves the
    /// transport's HTTP/1.1 client + chunked decoding.
    #[test]
    fn unix_socket_get_decodes_chunked_response() {
        let path = unique_socket_path();
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path).expect("bind unix listener");

        let json_body = r#"[{"Id":"abc","Names":["/x"]}]"#;
        let server_body = json_body.to_string();
        let server = thread::spawn(move || {
            let (mut conn, _) = listener.accept().expect("accept");
            // Drain the request headers.
            let mut buf = [0u8; 1024];
            let _ = conn.read(&mut buf);
            // Two data chunks + terminator, exercising multi-chunk decode.
            let (a, b) = server_body.split_at(server_body.len() / 2);
            let response = format!(
                "HTTP/1.1 200 OK\r\n\
                 Content-Type: application/json\r\n\
                 Transfer-Encoding: chunked\r\n\
                 Connection: close\r\n\r\n\
                 {:x}\r\n{}\r\n{:x}\r\n{}\r\n0\r\n\r\n",
                a.len(),
                a,
                b.len(),
                b
            );
            conn.write_all(response.as_bytes()).expect("write response");
        });

        let resp = unix_socket_get(
            path.to_str().unwrap(),
            "/containers/json",
            Duration::from_secs(5),
        )
        .expect("unix_socket_get");
        server.join().unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, json_body.as_bytes());
    }

    #[test]
    fn parse_content_length_response() {
        let raw = b"HTTP/1.1 201 Created\r\nContent-Length: 5\r\n\r\nhello world";
        let resp = parse_http_response(raw).unwrap();
        assert_eq!(resp.status, 201);
        assert_eq!(resp.body, b"hello");
    }

    #[test]
    fn parse_rejects_missing_separator() {
        assert!(parse_http_response(b"HTTP/1.1 200 OK").is_err());
    }

    #[test]
    fn parse_rejects_truncated_chunk() {
        // Declares a 10-byte chunk but supplies 2 bytes.
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\na\r\nhi";
        assert!(parse_http_response(raw).is_err());
    }

    #[test]
    fn read_capped_errors_when_response_exceeds_cap() {
        let big = [b'x'; 100];
        let err = read_capped(&big[..], 10, "/tmp/docker.sock").unwrap_err();
        assert!(err.msg.contains("exceeded"), "{}", err.msg);
    }

    #[test]
    fn read_capped_accepts_response_at_cap() {
        let body = [b'y'; 10];
        let out = read_capped(&body[..], 10, "/tmp/docker.sock").expect("within cap");
        assert_eq!(out, body);
    }
}
