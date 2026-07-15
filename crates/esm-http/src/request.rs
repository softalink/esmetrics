//! Request head parsing and body reading (Content-Length, chunked, gzip).

use std::borrow::Cow;
use std::io::{self, Read};
use std::net::TcpStream;

use flate2::read::GzDecoder;

use crate::query::parse_query;

/// HTTP request method. Unrecognized methods parse as [`Method::Other`];
/// the handler decides how to respond (typically 405).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Method {
    Get,
    Head,
    Post,
    Put,
    Delete,
    Options,
    Patch,
    Other,
}

impl Method {
    fn parse(s: &str) -> Method {
        match s {
            "GET" => Method::Get,
            "HEAD" => Method::Head,
            "POST" => Method::Post,
            "PUT" => Method::Put,
            "DELETE" => Method::Delete,
            "OPTIONS" => Method::Options,
            "PATCH" => Method::Patch,
            _ => Method::Other,
        }
    }
}

/// Body `Content-Encoding`. Values mirror what upstream
/// `protoparserutil.GetUncompressedReader` accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ContentEncoding {
    #[default]
    Identity,
    Gzip,
    Zstd,
    Deflate,
    Snappy,
    /// Any other value; handlers answer 400 like upstream's
    /// "unsupported contentType" error.
    Unsupported,
}

impl ContentEncoding {
    /// The upstream string form, for esm-protoparser's `&str`-typed APIs.
    pub fn as_str(self) -> &'static str {
        match self {
            ContentEncoding::Identity => "",
            ContentEncoding::Gzip => "gzip",
            ContentEncoding::Zstd => "zstd",
            ContentEncoding::Deflate => "deflate",
            ContentEncoding::Snappy => "snappy",
            ContentEncoding::Unsupported => "unsupported",
        }
    }
}

/// Malformed request head; the connection loop answers 400 and closes.
#[derive(Debug)]
pub(crate) struct ParseError;

/// Parsed request head. Only the handful of headers the endpoints need are
/// stored; everything else is skipped without allocating.
pub(crate) struct Head<'a> {
    pub method: Method,
    /// Percent-DECODED request path, like Go's `r.URL.Path` (all upstream
    /// routing happens on the decoded form; `%2F` decodes to `/`).
    pub path: Cow<'a, str>,
    pub query: &'a str,
    pub host: &'a str,
    pub content_length: Option<u64>,
    pub chunked: bool,
    pub content_encoding: ContentEncoding,
    /// Raw (whitespace-trimmed) first `Content-Encoding` header value, kept
    /// so error messages can echo what the client actually sent when the
    /// encoding is [`ContentEncoding::Unsupported`] (Go reports the real
    /// header value).
    pub content_encoding_raw: Option<&'a str>,
    /// Raw (whitespace-trimmed) `Content-Type` header value, if present.
    /// Only a handful of ingestion paths (currently OTLP) need this, to
    /// reject `application/json` bodies exactly like upstream
    /// `req.Header.Get("Content-Type") == "application/json"`.
    pub content_type: Option<&'a str>,
    pub accept_gzip: bool,
    pub keep_alive: bool,
    pub expect_continue: bool,
    /// Every header in wire order, `(name, value)`. Empty unless capture was
    /// requested via [`parse_head_capturing`] — the TSDB fast path never
    /// enables this, so it never allocates for it.
    pub all_headers: Vec<(String, String)>,
}

/// Parses the header section (`raw` includes the terminating `\r\n\r\n`),
/// without capturing all headers. Equivalent to
/// `parse_head_capturing(raw, false)`. The connection loop calls
/// `parse_head_capturing` directly (to thread `ServerConfig`); this wrapper
/// remains as the non-capturing convenience entry point used by tests.
#[cfg(test)]
pub(crate) fn parse_head(raw: &[u8]) -> Result<Head<'_>, ParseError> {
    parse_head_capturing(raw, false)
}

/// Parses the header section (`raw` includes the terminating `\r\n\r\n`).
/// When `capture` is true, every header is additionally recorded (in wire
/// order) into [`Head::all_headers`] for consumers that need the full set
/// (e.g. an auth proxy forwarding headers upstream).
pub(crate) fn parse_head_capturing(raw: &[u8], capture: bool) -> Result<Head<'_>, ParseError> {
    let text = std::str::from_utf8(raw).map_err(|_| ParseError)?;
    let mut lines = text.split("\r\n");

    let request_line = lines.next().ok_or(ParseError)?;
    let mut parts = request_line.split(' ');
    let method = Method::parse(parts.next().ok_or(ParseError)?);
    let target = parts.next().ok_or(ParseError)?;
    let version = parts.next().ok_or(ParseError)?;
    if parts.next().is_some() || target.is_empty() {
        return Err(ParseError);
    }
    let keep_alive_default = match version {
        "HTTP/1.1" => true,
        "HTTP/1.0" => false,
        _ => return Err(ParseError),
    };
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p, q),
        None => (target, ""),
    };
    // Go routes on the percent-decoded `r.URL.Path`; invalid escapes fail
    // URL parsing there and net/http answers 400 — ParseError does the same
    // here.
    let path = crate::query::percent_decode_path(path).map_err(|_| ParseError)?;

    let mut head = Head {
        method,
        path,
        query,
        host: "",
        content_length: None,
        chunked: false,
        content_encoding: ContentEncoding::Identity,
        content_encoding_raw: None,
        content_type: None,
        accept_gzip: false,
        keep_alive: keep_alive_default,
        expect_continue: false,
        all_headers: Vec::new(),
    };

    for line in lines {
        if line.is_empty() {
            continue; // trailing empties from the final \r\n\r\n
        }
        let (name, value) = line.split_once(':').ok_or(ParseError)?;
        let value = value.trim();
        if capture {
            head.all_headers.push((name.to_owned(), value.to_owned()));
        }
        if name.eq_ignore_ascii_case("content-length") {
            // Go's `readTransfer.fixLength` de-duplicates the Content-Length
            // header list and rejects the request (400) when the distinct
            // values differ — request-smuggling hardening. Identical repeated
            // values collapse to one.
            let parsed: u64 = value.parse().map_err(|_| ParseError)?;
            match head.content_length {
                Some(existing) if existing != parsed => return Err(ParseError),
                _ => head.content_length = Some(parsed),
            }
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            if value.eq_ignore_ascii_case("chunked") {
                head.chunked = true;
            } else {
                return Err(ParseError); // unsupported framing
            }
        } else if name.eq_ignore_ascii_case("content-encoding") {
            // First occurrence wins on duplicate headers, like Go's
            // `Header.Get`.
            if head.content_encoding_raw.is_none() {
                head.content_encoding_raw = Some(value);
                head.content_encoding =
                    if value.eq_ignore_ascii_case("gzip") || value.eq_ignore_ascii_case("x-gzip") {
                        ContentEncoding::Gzip
                    } else if value.eq_ignore_ascii_case("zstd") {
                        ContentEncoding::Zstd
                    } else if value.eq_ignore_ascii_case("deflate") {
                        ContentEncoding::Deflate
                    } else if value.eq_ignore_ascii_case("snappy") {
                        ContentEncoding::Snappy
                    } else if value.is_empty()
                        || value.eq_ignore_ascii_case("none")
                        || value.eq_ignore_ascii_case("identity")
                    {
                        ContentEncoding::Identity
                    } else {
                        ContentEncoding::Unsupported
                    };
            }
        } else if name.eq_ignore_ascii_case("accept-encoding") {
            head.accept_gzip = ascii_contains(value, "gzip");
        } else if name.eq_ignore_ascii_case("connection") {
            for token in value.split(',') {
                let token = token.trim();
                if token.eq_ignore_ascii_case("close") {
                    head.keep_alive = false;
                } else if token.eq_ignore_ascii_case("keep-alive") {
                    head.keep_alive = true;
                }
            }
        } else if name.eq_ignore_ascii_case("expect") {
            if value.eq_ignore_ascii_case("100-continue") {
                head.expect_continue = true;
            } else {
                return Err(ParseError);
            }
        } else if name.eq_ignore_ascii_case("host") {
            head.host = value;
        } else if name.eq_ignore_ascii_case("content-type") {
            // First occurrence wins, like Go's `Header.Get`.
            if head.content_type.is_none() {
                head.content_type = Some(value);
            }
        }
    }

    if head.chunked {
        // RFC 9112: Transfer-Encoding overrides Content-Length.
        head.content_length = None;
    }
    Ok(head)
}

fn ascii_contains(haystack: &str, needle: &str) -> bool {
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|w| w.eq_ignore_ascii_case(needle.as_bytes()))
}

#[derive(Clone, Copy)]
enum ChunkState {
    Size,
    Data { remaining: u64 },
    DataCrlf,
    Trailers,
    Done,
}

enum BodyKind {
    Empty,
    Sized { remaining: u64 },
    Chunked { state: ChunkState },
}

/// Raw (framed but not decompressed) request body. `Read` yields the body
/// bytes after Content-Length / chunked framing is stripped.
///
/// Bytes already read past the header section (`prefix`) are consumed first,
/// then reads go directly to the connection.
pub struct Body<'c> {
    stream: &'c TcpStream,
    prefix: &'c [u8],
    prefix_pos: usize,
    kind: BodyKind,
}

impl<'c> Body<'c> {
    pub(crate) fn new(
        stream: &'c TcpStream,
        prefix: &'c [u8],
        chunked: bool,
        content_length: Option<u64>,
    ) -> Body<'c> {
        let kind = if chunked {
            BodyKind::Chunked {
                state: ChunkState::Size,
            }
        } else {
            match content_length {
                Some(n) if n > 0 => BodyKind::Sized { remaining: n },
                _ => BodyKind::Empty,
            }
        };
        Body {
            stream,
            prefix,
            prefix_pos: 0,
            kind,
        }
    }

    /// How many bytes of the buffered prefix this body consumed. The
    /// connection loop uses it to locate the start of the next request.
    pub(crate) fn prefix_consumed(&self) -> usize {
        self.prefix_pos
    }

    /// Reads the remaining body to the end so the next keep-alive request
    /// starts at a message boundary. Returns `Ok(false)` when more than
    /// `cap` bytes would have to be discarded (caller should close instead).
    pub(crate) fn drain(&mut self, cap: u64) -> io::Result<bool> {
        let mut scratch = [0u8; 4096];
        let mut total: u64 = 0;
        loop {
            if let BodyKind::Sized { remaining } = self.kind {
                if remaining > cap - total {
                    return Ok(false);
                }
            }
            let n = self.read(&mut scratch)?;
            if n == 0 {
                return Ok(true);
            }
            total += n as u64;
            if total > cap {
                return Ok(false);
            }
        }
    }

    fn read_raw(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.prefix_pos < self.prefix.len() {
            let avail = &self.prefix[self.prefix_pos..];
            let n = avail.len().min(out.len());
            out[..n].copy_from_slice(&avail[..n]);
            self.prefix_pos += n;
            return Ok(n);
        }
        let mut stream = self.stream;
        stream.read(out)
    }

    fn read_byte(&mut self) -> io::Result<u8> {
        let mut b = [0u8; 1];
        if self.read_raw(&mut b)? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed mid-body",
            ));
        }
        Ok(b[0])
    }

    fn chunk_state(&self) -> ChunkState {
        match &self.kind {
            BodyKind::Chunked { state } => *state,
            _ => unreachable!("chunk_state on non-chunked body"),
        }
    }

    fn set_chunk_state(&mut self, state: ChunkState) {
        self.kind = BodyKind::Chunked { state };
    }

    /// Reads a `size[;ext]\r\n` chunk-size line.
    fn read_chunk_size(&mut self) -> io::Result<u64> {
        let mut size: u64 = 0;
        let mut digits = 0u32;
        loop {
            let b = self.read_byte()?;
            let digit = match b {
                b'0'..=b'9' => b - b'0',
                b'a'..=b'f' => b - b'a' + 10,
                b'A'..=b'F' => b - b'A' + 10,
                b';' => {
                    // Skip chunk extensions up to CR.
                    loop {
                        if self.read_byte()? == b'\r' {
                            break;
                        }
                    }
                    self.expect_lf()?;
                    return self.finish_chunk_size(size, digits);
                }
                b'\r' => {
                    self.expect_lf()?;
                    return self.finish_chunk_size(size, digits);
                }
                _ => return Err(invalid_chunk()),
            };
            digits += 1;
            if digits > 16 {
                return Err(invalid_chunk());
            }
            size = (size << 4) | u64::from(digit);
        }
    }

    fn finish_chunk_size(&self, size: u64, digits: u32) -> io::Result<u64> {
        if digits == 0 {
            return Err(invalid_chunk());
        }
        Ok(size)
    }

    fn expect_lf(&mut self) -> io::Result<()> {
        if self.read_byte()? == b'\n' {
            Ok(())
        } else {
            Err(invalid_chunk())
        }
    }

    /// Skips trailer lines after the terminal chunk, up to the empty line.
    fn skip_trailers(&mut self) -> io::Result<()> {
        loop {
            let mut len = 0usize;
            loop {
                let b = self.read_byte()?;
                if b == b'\n' {
                    break;
                }
                if b != b'\r' {
                    len += 1;
                    if len > 8192 {
                        return Err(invalid_chunk());
                    }
                }
            }
            if len == 0 {
                return Ok(());
            }
        }
    }

    fn read_chunked(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        loop {
            match self.chunk_state() {
                ChunkState::Done => return Ok(0),
                ChunkState::Size => {
                    let size = self.read_chunk_size()?;
                    self.set_chunk_state(if size == 0 {
                        ChunkState::Trailers
                    } else {
                        ChunkState::Data { remaining: size }
                    });
                }
                ChunkState::Data { remaining } => {
                    let want = out
                        .len()
                        .min(usize::try_from(remaining).unwrap_or(usize::MAX));
                    let got = self.read_raw(&mut out[..want])?;
                    if got == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "connection closed mid-chunk",
                        ));
                    }
                    let left = remaining - got as u64;
                    self.set_chunk_state(if left == 0 {
                        ChunkState::DataCrlf
                    } else {
                        ChunkState::Data { remaining: left }
                    });
                    return Ok(got);
                }
                ChunkState::DataCrlf => {
                    if self.read_byte()? != b'\r' {
                        return Err(invalid_chunk());
                    }
                    self.expect_lf()?;
                    self.set_chunk_state(ChunkState::Size);
                }
                ChunkState::Trailers => {
                    self.skip_trailers()?;
                    self.set_chunk_state(ChunkState::Done);
                    return Ok(0);
                }
            }
        }
    }
}

fn invalid_chunk() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "malformed chunked encoding")
}

impl Read for Body<'_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        match &mut self.kind {
            BodyKind::Empty => Ok(0),
            BodyKind::Sized { remaining } => {
                if *remaining == 0 || out.is_empty() {
                    return Ok(0);
                }
                let want = out
                    .len()
                    .min(usize::try_from(*remaining).unwrap_or(usize::MAX));
                let got = self.read_raw(&mut out[..want])?;
                if got == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "connection closed mid-body",
                    ));
                }
                match &mut self.kind {
                    BodyKind::Sized { remaining } => *remaining -= got as u64,
                    _ => unreachable!(),
                }
                Ok(got)
            }
            BodyKind::Chunked { .. } => self.read_chunked(out),
        }
    }
}

/// Body reader that transparently gunzips when the request carried
/// `Content-Encoding: gzip`. Enum instead of `Box<dyn Read>` to avoid the
/// allocation and dynamic dispatch on the hot (plain) path.
pub enum BodyReader<'r, 'c> {
    Plain(&'r mut Body<'c>),
    Gzip(GzDecoder<&'r mut Body<'c>>),
}

impl Read for BodyReader<'_, '_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        match self {
            BodyReader::Plain(body) => body.read(out),
            BodyReader::Gzip(dec) => dec.read(out),
        }
    }
}

/// A single HTTP request. Borrows the connection's reusable read buffer
/// (`path`/`query`/`host`) and the socket (body).
pub struct Request<'c> {
    head: Head<'c>,
    body: Body<'c>,
}

impl<'c> Request<'c> {
    pub(crate) fn new(head: Head<'c>, body: Body<'c>) -> Request<'c> {
        Request { head, body }
    }

    pub fn method(&self) -> Method {
        self.head.method
    }

    /// Request path, percent-DECODED like Go's `r.URL.Path` (routing
    /// happens on the decoded form; `%2F` decodes to `/`).
    pub fn path(&self) -> &str {
        &self.head.path
    }

    /// Raw (undecoded) query string, without the leading `?`.
    pub fn query(&self) -> &'c str {
        self.head.query
    }

    pub fn host(&self) -> &'c str {
        self.head.host
    }

    pub fn content_length(&self) -> Option<u64> {
        self.head.content_length
    }

    pub fn is_chunked(&self) -> bool {
        self.head.chunked
    }

    /// True when the request body is gzip-compressed
    /// (`Content-Encoding: gzip`).
    pub fn is_gzipped(&self) -> bool {
        self.head.content_encoding == ContentEncoding::Gzip
    }

    /// The request body's content encoding.
    pub fn content_encoding(&self) -> ContentEncoding {
        self.head.content_encoding
    }

    /// The content encoding as a string for error messages and the
    /// `&str`-typed decompression APIs: the canonical name for supported
    /// encodings, or the client's actual header value verbatim when the
    /// encoding is unsupported (Go error messages echo the real value).
    pub fn content_encoding_str(&self) -> &'c str {
        match self.head.content_encoding {
            ContentEncoding::Unsupported => self.head.content_encoding_raw.unwrap_or("unsupported"),
            other => other.as_str(),
        }
    }

    /// Raw (whitespace-trimmed) `Content-Type` header value, if the request
    /// carried one. Go: `req.Header.Get("Content-Type")`.
    pub fn content_type(&self) -> Option<&'c str> {
        self.head.content_type
    }

    /// Decoded query-string parameters.
    pub fn query_params(&self) -> impl Iterator<Item = (Cow<'c, str>, Cow<'c, str>)> {
        parse_query(self.head.query)
    }

    /// Every header in wire order, `(name, value)`. Empty unless the server
    /// was configured with `ServerConfig::capture_all_headers`.
    pub fn all_headers(&self) -> &[(String, String)] {
        &self.head.all_headers
    }

    /// Raw framed body (no gzip decoding).
    pub fn body(&mut self) -> &mut Body<'c> {
        &mut self.body
    }

    /// Body reader with transparent gzip decoding when the client sent
    /// `Content-Encoding: gzip`.
    pub fn body_reader(&mut self) -> BodyReader<'_, 'c> {
        if self.head.content_encoding == ContentEncoding::Gzip {
            BodyReader::Gzip(GzDecoder::new(&mut self.body))
        } else {
            BodyReader::Plain(&mut self.body)
        }
    }

    /// Reads the whole (decoded) body, appending to `buf`. Fails with
    /// `InvalidData` if the decoded size would exceed `max_size`.
    pub fn read_body_to(&mut self, buf: &mut Vec<u8>, max_size: usize) -> io::Result<()> {
        let mut reader = self.body_reader();
        let mut chunk = [0u8; 8192];
        loop {
            let n = reader.read(&mut chunk)?;
            if n == 0 {
                return Ok(());
            }
            if buf.len() + n > max_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "request body exceeds max_size",
                ));
            }
            buf.extend_from_slice(&chunk[..n]);
        }
    }

    pub(crate) fn prefix_consumed(&self) -> usize {
        self.body.prefix_consumed()
    }

    pub(crate) fn drain_body(&mut self, cap: u64) -> io::Result<bool> {
        self.body.drain(cap)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_encoding_is_parsed() {
        for (hdr, want) in [
            ("gzip", ContentEncoding::Gzip),
            ("x-gzip", ContentEncoding::Gzip),
            ("ZSTD", ContentEncoding::Zstd),
            ("deflate", ContentEncoding::Deflate),
            ("snappy", ContentEncoding::Snappy),
            ("br", ContentEncoding::Unsupported),
        ] {
            let raw = format!("POST /w HTTP/1.1\r\nContent-Encoding: {hdr}\r\n\r\n");
            let head = parse_head(raw.as_bytes()).unwrap();
            assert_eq!(head.content_encoding, want, "header {hdr:?}");
        }
        let head = parse_head(b"POST /w HTTP/1.1\r\n\r\n").unwrap();
        assert_eq!(head.content_encoding, ContentEncoding::Identity);
    }

    #[test]
    fn duplicate_content_headers_first_wins() {
        // Go's Header.Get returns the FIRST occurrence of a duplicated
        // header; the parser must not let a later occurrence override it.
        let raw =
            b"POST /w HTTP/1.1\r\nContent-Encoding: identity\r\nContent-Encoding: gzip\r\n\r\n";
        let head = parse_head(raw).unwrap();
        assert_eq!(head.content_encoding, ContentEncoding::Identity);
        assert_eq!(head.content_encoding_raw, Some("identity"));

        let raw = b"POST /w HTTP/1.1\r\nContent-Type: text/plain\r\nContent-Type: application/json\r\n\r\n";
        let head = parse_head(raw).unwrap();
        assert_eq!(head.content_type, Some("text/plain"));
    }

    #[test]
    fn conflicting_content_length_is_rejected() {
        // Go's readTransfer.fixLength de-duplicates the Content-Length header
        // list and rejects the request when the distinct values differ
        // (request-smuggling hardening).
        let raw = b"POST /w HTTP/1.1\r\nContent-Length: 5\r\nContent-Length: 6\r\n\r\n";
        assert!(parse_head(raw).is_err());
    }

    #[test]
    fn duplicate_equal_content_length_is_accepted() {
        // Identical repeated Content-Length values collapse to one; only
        // differing values are an error.
        let raw = b"POST /w HTTP/1.1\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\n";
        let head = parse_head(raw).unwrap();
        assert_eq!(head.content_length, Some(5));
    }

    #[test]
    fn unsupported_content_encoding_keeps_the_raw_value() {
        let raw = b"POST /w HTTP/1.1\r\nContent-Encoding: br\r\n\r\n";
        let head = parse_head(raw).unwrap();
        assert_eq!(head.content_encoding, ContentEncoding::Unsupported);
        // Error messages echo what the client actually sent, like Go.
        assert_eq!(head.content_encoding_raw, Some("br"));
    }

    #[test]
    fn content_type_is_parsed() {
        let raw = b"POST /w HTTP/1.1\r\nContent-Type: application/json\r\n\r\n";
        let head = parse_head(raw).unwrap();
        assert_eq!(head.content_type, Some("application/json"));

        let head = parse_head(b"POST /w HTTP/1.1\r\n\r\n").unwrap();
        assert_eq!(head.content_type, None);

        // Value is trimmed but not otherwise normalized (case/params kept
        // as-is), matching Go's `req.Header.Get` (canonicalizes the header
        // *name*, not the value).
        let raw = b"POST /w HTTP/1.1\r\nContent-Type:   application/json; charset=utf-8  \r\n\r\n";
        let head = parse_head(raw).unwrap();
        assert_eq!(head.content_type, Some("application/json; charset=utf-8"));
    }

    #[test]
    fn all_headers_captured_when_enabled() {
        let raw = b"GET /p HTTP/1.1\r\nHost: h\r\nX-Custom: a\r\nAuthorization: Bearer t\r\nX-Custom: b\r\n\r\n";
        let head = parse_head_capturing(raw, true).unwrap();
        let hs = &head.all_headers;
        assert!(hs
            .iter()
            .any(|(n, v)| n.eq_ignore_ascii_case("x-custom") && v == "a"));
        assert!(hs
            .iter()
            .any(|(n, v)| n.eq_ignore_ascii_case("authorization") && v == "Bearer t"));
        // Duplicate headers are all captured, in wire order.
        let customs: Vec<&str> = hs
            .iter()
            .filter(|(n, _)| n.eq_ignore_ascii_case("x-custom"))
            .map(|(_, v)| v.as_str())
            .collect();
        assert_eq!(customs, ["a", "b"]);
        let head2 = parse_head_capturing(raw, false).unwrap();
        assert!(head2.all_headers.is_empty());
    }
}
