//! Response writing: buffered (single vectored write) and chunked streaming.

use std::borrow::Cow;
use std::io::{self, IoSlice, Read, Write};
use std::net::TcpStream;

use flate2::write::GzEncoder;
use flate2::Compression;

/// gzhttp `DefaultMinSize`: responses smaller than this are sent
/// uncompressed. Matches `gzhttp.NewWrapper` defaults in upstream
/// `lib/httpserver/httpserver.go` (`gzipHandlerWrapper`).
const GZIP_MIN_SIZE: usize = 1024;

/// Flush a streamed chunk once the pending buffer reaches this size.
const STREAM_FLUSH_THRESHOLD: usize = 8 * 1024;
/// Read block size for [`ResponseWriter::stream_response`].
const STREAM_RESPONSE_READ_SIZE: usize = 16 * 1024;

/// Writes one HTTP/1.1 response.
///
/// Two modes:
/// - **Buffered** (default): body accumulates in a per-connection reusable
///   buffer; on finish, status + headers + body go out in a single vectored
///   write with `Content-Length`. If the server was configured with
///   `compress_responses` and the client sent `Accept-Encoding: gzip`, the
///   buffered body is gzipped (flate2 level 1) before sending.
/// - **Streaming** ([`ResponseWriter::begin_stream`]): headers are sent
///   immediately with `Transfer-Encoding: chunked`; subsequent
///   [`io::Write`] writes are coalesced into chunks. Streamed responses are
///   never gzipped.
///
/// For `HEAD` requests the connection loop constructs the writer in
/// head-only mode: headers (including `Content-Length` of the would-be
/// body) are sent, the body is suppressed.
pub struct ResponseWriter<'c> {
    stream: &'c TcpStream,
    body: &'c mut Vec<u8>,
    head: &'c mut Vec<u8>,
    status: u16,
    content_type: Option<Cow<'static, str>>,
    extra: Vec<(String, String)>,
    keep_alive: bool,
    is_head: bool,
    gzip_allowed: bool,
    streaming: bool,
    finished: bool,
}

impl<'c> ResponseWriter<'c> {
    pub(crate) fn new(
        stream: &'c TcpStream,
        body: &'c mut Vec<u8>,
        head: &'c mut Vec<u8>,
        is_head: bool,
        keep_alive: bool,
        gzip_allowed: bool,
    ) -> ResponseWriter<'c> {
        body.clear();
        head.clear();
        ResponseWriter {
            stream,
            body,
            head,
            status: 200,
            content_type: None,
            extra: Vec::new(),
            keep_alive,
            is_head,
            gzip_allowed,
            streaming: false,
            finished: false,
        }
    }

    /// Sets the status code. Must be called before `begin_stream`.
    pub fn set_status(&mut self, status: u16) {
        self.status = status;
    }

    /// Sets the `Content-Type` header. Must be set before `begin_stream`.
    pub fn set_content_type(&mut self, content_type: impl Into<Cow<'static, str>>) {
        self.content_type = Some(content_type.into());
    }

    /// Adds an extra response header. Must be set before `begin_stream`.
    /// `Content-Length`, `Connection`, `Transfer-Encoding` and
    /// `Content-Encoding` are managed by the writer; do not set them here.
    pub fn set_header(&mut self, name: &str, value: &str) {
        self.extra.push((name.to_owned(), value.to_owned()));
    }

    /// Responds with `status` and an empty body.
    pub fn write_status(&mut self, status: u16) {
        self.status = status;
        self.body.clear();
    }

    /// Responds with `status` and a JSON body.
    pub fn write_json(&mut self, status: u16, json: &str) {
        self.status = status;
        self.content_type = Some(Cow::Borrowed("application/json"));
        self.body.clear();
        self.body.extend_from_slice(json.as_bytes());
    }

    /// Appends bytes to the buffered body (or the pending stream chunk).
    /// In streaming mode, data is sent on [`io::Write::flush`], when the
    /// pending chunk exceeds an internal threshold, or at end of request.
    pub fn write_body(&mut self, bytes: &[u8]) {
        self.body.extend_from_slice(bytes);
    }

    /// Switches to chunked streaming: sends status + headers immediately
    /// with `Transfer-Encoding: chunked`. Idempotent.
    pub fn begin_stream(&mut self) -> io::Result<()> {
        if self.streaming {
            return Ok(());
        }
        self.streaming = true;
        self.body.clear();
        self.build_head(None, false);
        let mut stream = self.stream;
        stream.write_all(self.head)
    }

    /// True once `begin_stream` has been called.
    pub fn is_streaming(&self) -> bool {
        self.streaming
    }

    /// Writes `status` + `headers`, then streams `body` to the client as
    /// chunked transfer-encoding — for pass-through responses (e.g. an auth
    /// proxy relaying a backend response) whose length isn't known ahead of
    /// time. Reads `body` in 16 KiB blocks, writing one chunk per block,
    /// then the terminating `0\r\n\r\n`. Marks the response finished so the
    /// connection loop's `finish()` call becomes a no-op afterward.
    ///
    /// Hop-by-hop headers (RFC 9110 §7.6.1: `Connection`, `Keep-Alive`,
    /// `Proxy-Authenticate`, `Proxy-Authorization`, `TE`, `Trailer`,
    /// `Transfer-Encoding`, `Upgrade`) are stripped, along with any header
    /// named in a `Connection` header's comma-separated option list.
    /// `Content-Length` is also dropped: this function re-frames the body
    /// as chunked, and a passthrough length would desync the client.
    ///
    /// For a `HEAD` request (per the `is_head` this writer was constructed
    /// with — see [`ResponseWriter::new`]), RFC 7230 §3.3.3 forbids a
    /// response body: only the status line and headers are written, with no
    /// `Transfer-Encoding`, no chunked body, and no terminating `0\r\n\r\n`.
    /// `body` is not read in that case.
    pub fn stream_response(
        &mut self,
        status: u16,
        headers: &[(String, String)],
        body: &mut dyn Read,
    ) -> io::Result<()> {
        // Connection-listed options are hop-by-hop too (RFC 9110 §7.6.1).
        let connection_options: Vec<&str> = headers
            .iter()
            .filter(|(name, _)| name.eq_ignore_ascii_case("connection"))
            .flat_map(|(_, value)| value.split(','))
            .map(str::trim)
            .filter(|opt| !opt.is_empty())
            .collect();

        let mut stream = self.stream;
        self.head.clear();
        let _ = write!(self.head, "HTTP/1.1 {status} {}\r\n", status_text(status));
        for (name, value) in headers {
            if is_hop_by_hop(name)
                || connection_options
                    .iter()
                    .any(|opt| name.eq_ignore_ascii_case(opt))
            {
                continue;
            }
            let _ = write!(self.head, "{name}: {value}\r\n");
        }

        if self.is_head {
            // No Transfer-Encoding, no body, no terminator — a HEAD response
            // MUST NOT carry a message body (RFC 7230 §3.3.3).
            self.head.extend_from_slice(b"\r\n");
            stream.write_all(self.head)?;
            self.finished = true;
            return Ok(());
        }

        self.head
            .extend_from_slice(b"Transfer-Encoding: chunked\r\n\r\n");
        stream.write_all(self.head)?;

        let mut buf = [0u8; STREAM_RESPONSE_READ_SIZE];
        loop {
            let n = body.read(&mut buf)?;
            if n == 0 {
                break;
            }
            self.head.clear();
            let _ = write!(self.head, "{n:x}\r\n");
            write_all_vectored(&mut stream, &[self.head, &buf[..n], b"\r\n"])?;
        }
        stream.write_all(b"0\r\n\r\n")?;
        self.finished = true;
        Ok(())
    }

    /// Completes the response. Called by the connection loop after the
    /// handler returns; safe to call at most once (subsequent calls no-op).
    pub(crate) fn finish(&mut self) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        let mut stream = self.stream;

        if self.streaming {
            self.flush_chunk()?;
            if !self.is_head {
                stream.write_all(b"0\r\n\r\n")?;
            }
            return stream.flush();
        }

        let mut gzip = false;
        if self.gzip_allowed
            && !self.is_head
            && self.body.len() >= GZIP_MIN_SIZE
            && content_type_gzippable(self.content_type.as_deref())
        {
            let mut enc = GzEncoder::new(
                Vec::with_capacity(self.body.len() / 2 + 32),
                Compression::new(1),
            );
            enc.write_all(self.body)?;
            let compressed = enc.finish()?;
            self.body.clear();
            self.body.extend_from_slice(&compressed);
            gzip = true;
        }

        let body_len = self.body.len();
        self.build_head(Some(body_len), gzip);
        if self.is_head {
            stream.write_all(self.head)
        } else {
            write_all_vectored(&mut stream, &[self.head, self.body])
        }
    }

    /// Sends the pending stream buffer as one chunk.
    fn flush_chunk(&mut self) -> io::Result<()> {
        if self.is_head || self.body.is_empty() {
            self.body.clear();
            return Ok(());
        }
        // `head` is free once the header section went out; reuse it for the
        // chunk-size line.
        self.head.clear();
        let _ = write!(self.head, "{:x}\r\n", self.body.len());
        let mut stream = self.stream;
        write_all_vectored(&mut stream, &[self.head, self.body, b"\r\n"])?;
        self.body.clear();
        Ok(())
    }

    fn build_head(&mut self, body_len: Option<usize>, gzip: bool) {
        self.head.clear();
        let _ = write!(
            self.head,
            "HTTP/1.1 {} {}\r\n",
            self.status,
            status_text(self.status)
        );
        if let Some(ct) = &self.content_type {
            let _ = write!(self.head, "Content-Type: {ct}\r\n");
        }
        for (name, value) in &self.extra {
            let _ = write!(self.head, "{name}: {value}\r\n");
        }
        if gzip {
            self.head.extend_from_slice(b"Content-Encoding: gzip\r\n");
        }
        self.head.extend_from_slice(if self.keep_alive {
            b"Connection: keep-alive\r\n".as_slice()
        } else {
            b"Connection: close\r\n".as_slice()
        });
        match body_len {
            Some(n) => {
                let _ = write!(self.head, "Content-Length: {n}\r\n");
            }
            None => self
                .head
                .extend_from_slice(b"Transfer-Encoding: chunked\r\n"),
        }
        self.head.extend_from_slice(b"\r\n");
    }
}

impl Write for ResponseWriter<'_> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.body.extend_from_slice(data);
        if self.streaming && self.body.len() >= STREAM_FLUSH_THRESHOLD {
            self.flush_chunk()?;
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.streaming {
            self.flush_chunk()?;
            let mut stream = self.stream;
            stream.flush()?;
        }
        Ok(())
    }
}

/// Hop-by-hop headers (RFC 9110 §7.6.1) that must not be forwarded by
/// [`ResponseWriter::stream_response`], plus `Content-Length` (the body is
/// re-framed as chunked, so a passthrough length would desync the client).
fn is_hop_by_hop(name: &str) -> bool {
    const HOP_BY_HOP: &[&str] = &[
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "content-length",
    ];
    HOP_BY_HOP.iter().any(|h| name.eq_ignore_ascii_case(h))
}

/// Port of gzhttp `DefaultContentTypeFilter`: compress responses with an
/// empty/unknown content type, but skip already-compressed audio, video,
/// image and archive formats. An absent `Content-Type` is treated as empty
/// (compressible), matching gzhttp's `ct == ""` fast path.
fn content_type_gzippable(content_type: Option<&str>) -> bool {
    // gzhttp `excludeContainsDefault` / `excludePrefixDefault`.
    const EXCLUDE_CONTAINS: &[&str] = &[
        "compress", "zip", "snappy", "lzma", "xz", "zstd", "brotli", "stuffit",
    ];
    const EXCLUDE_PREFIX: &[&str] = &["video/", "audio/", "image/jp"];

    let ct = match content_type {
        Some(ct) => ct.trim().to_ascii_lowercase(),
        None => return true,
    };
    if ct.is_empty() {
        return true;
    }
    if EXCLUDE_CONTAINS.iter().any(|s| ct.contains(s)) {
        return false;
    }
    if EXCLUDE_PREFIX.iter().any(|p| ct.starts_with(p)) {
        return false;
    }
    true
}

fn status_text(code: u16) -> &'static str {
    match code {
        100 => "Continue",
        200 => "OK",
        204 => "No Content",
        206 => "Partial Content",
        301 => "Moved Permanently",
        302 => "Found",
        304 => "Not Modified",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        413 => "Payload Too Large",
        422 => "Unprocessable Entity",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        503 => "Service Unavailable",
        _ => "",
    }
}

/// Writes all of `parts` with as few syscalls as the OS allows
/// (writev-style). Retries on `Interrupted` and short writes.
fn write_all_vectored(w: &mut impl Write, parts: &[&[u8]]) -> io::Result<()> {
    let total: usize = parts.iter().map(|p| p.len()).sum();
    let mut done = 0usize;
    while done < total {
        let mut slices = [IoSlice::new(&[]); 3];
        let mut count = 0;
        let mut skip = done;
        for part in parts {
            if skip >= part.len() {
                skip -= part.len();
                continue;
            }
            slices[count] = IoSlice::new(&part[skip..]);
            skip = 0;
            count += 1;
        }
        match w.write_vectored(&slices[..count]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole response",
                ))
            }
            Ok(n) => done += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
