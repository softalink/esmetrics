//! Per-connection keep-alive loop.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use crate::request::{parse_head_capturing, Body, Request};
use crate::response::ResponseWriter;
use crate::{Handler, Method, ServerInner};

/// Reject header sections larger than this (spec: 64 KiB).
const MAX_HEAD_SIZE: usize = 64 * 1024;
const HEAD_READ_CHUNK: usize = 4 * 1024;
/// Max unread request-body bytes to discard to preserve keep-alive; beyond
/// this the connection is closed instead of draining.
const DRAIN_CAP: u64 = 1 << 20;

enum HeadStatus {
    /// Header section complete; value is the offset just past `\r\n\r\n`.
    Complete(usize),
    /// Peer closed the connection between requests.
    CleanEof,
    /// Peer closed mid-header.
    Truncated,
    TooLarge,
    /// Read error (including read timeout and `stop()` shutdown).
    Io,
}

/// Serves requests on one connection until EOF, `Connection: close`, a
/// parse/IO error, or server stop. Buffers (`buf`, `body_buf`,
/// `head_scratch`) are allocated once per connection and reused across
/// requests.
pub(crate) fn handle_connection(inner: &ServerInner, stream: &TcpStream, handler: &Handler) {
    let _ = stream.set_nodelay(true);
    if let Some(timeout) = inner.config.read_timeout {
        let _ = stream.set_read_timeout(Some(timeout));
    }

    let mut buf: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut body_buf: Vec<u8> = Vec::with_capacity(4 * 1024);
    let mut head_scratch: Vec<u8> = Vec::with_capacity(256);

    while !inner.is_stopped() {
        let head_end = match read_head(inner, stream, &mut buf) {
            HeadStatus::Complete(end) => end,
            HeadStatus::CleanEof | HeadStatus::Io => break,
            HeadStatus::Truncated | HeadStatus::TooLarge => {
                write_bad_request(stream);
                break;
            }
        };

        let (head_bytes, rest) = buf.split_at(head_end);
        let head = match parse_head_capturing(head_bytes, inner.config.capture_all_headers) {
            Ok(head) => head,
            Err(_) => {
                write_bad_request(stream);
                break;
            }
        };

        if head.expect_continue {
            let mut s = stream;
            if s.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").is_err() {
                break;
            }
        }

        let client_keep_alive = head.keep_alive;
        let is_head = head.method == Method::Head;
        let gzip_allowed = inner.config.compress_responses && head.accept_gzip;

        let body = Body::new(stream, rest, head.chunked, head.content_length);
        let mut req = Request::new(head, body);
        let mut rw = ResponseWriter::new(
            stream,
            &mut body_buf,
            &mut head_scratch,
            is_head,
            client_keep_alive,
            gzip_allowed,
        );

        handler(&mut req, &mut rw);

        let write_ok = rw.finish().is_ok();
        // Discard any body bytes the handler left unread so the next
        // request starts at a message boundary.
        let drained = write_ok && matches!(req.drain_body(DRAIN_CAP), Ok(true));
        let consumed = head_end + req.prefix_consumed();
        // `req` borrows `buf`; its last use is above, so `buf` is free again.

        // Shift pipelined leftover bytes to the front of the reusable buffer.
        buf.copy_within(consumed.., 0);
        buf.truncate(buf.len() - consumed);

        if !(write_ok && drained && client_keep_alive) {
            break;
        }
    }
}

/// Reads until the header section (`\r\n\r\n`) is complete. `buf` may
/// already contain bytes left over from the previous request.
/// While waiting for a request head the stream reads tick at this interval
/// so the thread observes `stopped` promptly. POSIX `shutdown()` interrupts
/// a blocked `recv`, but WinSock does not, so a Windows connection parked in
/// `read()` would otherwise ride out the whole stop-drain timeout.
const HEAD_READ_TICK: Duration = Duration::from_millis(500);

fn read_head(inner: &ServerInner, stream: &TcpStream, buf: &mut Vec<u8>) -> HeadStatus {
    let configured = inner.config.read_timeout;
    let tick = configured.map_or(HEAD_READ_TICK, |t| t.min(HEAD_READ_TICK));
    let _ = stream.set_read_timeout(Some(tick));
    let started = Instant::now();

    let mut scanned = 0usize;
    let status = loop {
        if let Some(end) = find_head_end(buf, scanned) {
            break HeadStatus::Complete(end);
        }
        scanned = buf.len().saturating_sub(3);
        if buf.len() > MAX_HEAD_SIZE {
            break HeadStatus::TooLarge;
        }
        let old_len = buf.len();
        buf.resize(old_len + HEAD_READ_CHUNK, 0);
        let mut s = stream;
        match s.read(&mut buf[old_len..]) {
            Ok(0) => {
                buf.truncate(old_len);
                break if buf.is_empty() {
                    HeadStatus::CleanEof
                } else {
                    HeadStatus::Truncated
                };
            }
            Ok(n) => buf.truncate(old_len + n),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                buf.truncate(old_len);
                if inner.is_stopped() {
                    break HeadStatus::Io;
                }
                if let Some(t) = configured {
                    if started.elapsed() >= t {
                        // Honor the configured idle timeout.
                        break HeadStatus::Io;
                    }
                }
            }
            Err(_) => {
                buf.truncate(old_len);
                break HeadStatus::Io;
            }
        }
    };
    // Body reads (and anything after the head) use the configured timeout.
    let _ = stream.set_read_timeout(configured);
    status
}

fn find_head_end(buf: &[u8], from: usize) -> Option<usize> {
    buf[from..]
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|pos| from + pos + 4)
}

fn write_bad_request(stream: &TcpStream) {
    let mut s = stream;
    let _ =
        s.write_all(b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
}
