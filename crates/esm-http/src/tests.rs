//! End-to-end tests using raw std TcpStream clients.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;

use crate::{parse_form, parse_query, percent_decode, Handler, Method, Server, ServerConfig};

// ---------------------------------------------------------------- helpers

fn router() -> Arc<Handler> {
    Arc::new(|req, rw| match (req.method(), req.path()) {
        (Method::Get, "/ping") => rw.write_json(200, "{\"ok\":true}"),
        (Method::Post, "/echo") => {
            let mut body = Vec::new();
            match req.read_body_to(&mut body, 1 << 20) {
                Ok(()) => {
                    rw.set_content_type("application/octet-stream");
                    rw.write_body(&body);
                }
                Err(_) => rw.write_status(400),
            }
        }
        (Method::Get | Method::Head, "/text") => {
            rw.set_content_type("text/plain");
            rw.write_body(b"hello head");
        }
        (Method::Get, "/large") => {
            rw.set_content_type("text/plain");
            rw.write_body(&[b'a'; 2000]);
        }
        (Method::Get, "/stream") => {
            rw.set_content_type("text/plain");
            rw.begin_stream().unwrap();
            rw.write_all(b"hello ").unwrap();
            rw.flush().unwrap();
            rw.write_all(b"world").unwrap();
        }
        (Method::Get | Method::Head, "/stream-response") => {
            rw.stream_response(
                200,
                &[("X-Backend".into(), "b1".into())],
                &mut "hello world".as_bytes(),
            )
            .unwrap();
        }
        (Method::Get, "/stream-response-hop") => {
            rw.stream_response(
                200,
                &[
                    ("Connection".into(), "X-Hop".into()),
                    ("X-Hop".into(), "secret".into()),
                    ("Transfer-Encoding".into(), "gzip".into()),
                    ("Content-Length".into(), "5".into()),
                    ("X-Keep".into(), "v".into()),
                ],
                &mut "hello world".as_bytes(),
            )
            .unwrap();
        }
        (Method::Get, "/query") => {
            let mut out = String::new();
            for (k, v) in req.query_params() {
                out.push_str(&k);
                out.push('=');
                out.push_str(&v);
                out.push('\n');
            }
            rw.set_content_type("text/plain");
            rw.write_body(out.as_bytes());
        }
        (Method::Post, "/form") => {
            let mut body = Vec::new();
            req.read_body_to(&mut body, 1 << 20).unwrap();
            let text = std::str::from_utf8(&body).unwrap();
            let mut out = String::new();
            for (k, v) in parse_form(text) {
                out.push_str(&k);
                out.push('=');
                out.push_str(&v);
                out.push('\n');
            }
            rw.set_content_type("text/plain");
            rw.write_body(out.as_bytes());
        }
        _ => rw.write_status(404),
    })
}

fn start_server(config: ServerConfig) -> Server {
    let server = Server::bind_with_config("127.0.0.1:0", config).unwrap();
    server.serve(router());
    server
}

fn start_default() -> Server {
    start_server(ServerConfig::default())
}

struct Client {
    writer: TcpStream,
    reader: BufReader<TcpStream>,
}

#[derive(Debug)]
struct Resp {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Resp {
    fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| *k == name)
            .map(|(_, v)| v.as_str())
    }
}

impl Client {
    fn connect(addr: SocketAddr) -> Client {
        let stream = TcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let reader = BufReader::new(stream.try_clone().unwrap());
        Client {
            writer: stream,
            reader,
        }
    }

    fn send(&mut self, data: &[u8]) {
        self.writer.write_all(data).unwrap();
    }

    fn read_line(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        line.trim_end_matches(['\r', '\n']).to_owned()
    }

    fn read_response(&mut self, head_only: bool) -> Resp {
        let status_line = self.read_line();
        let status: u16 = status_line
            .split(' ')
            .nth(1)
            .unwrap_or_else(|| panic!("bad status line: {status_line:?}"))
            .parse()
            .unwrap();
        let mut headers = Vec::new();
        loop {
            let line = self.read_line();
            if line.is_empty() {
                break;
            }
            let (name, value) = line.split_once(':').unwrap();
            headers.push((name.trim().to_ascii_lowercase(), value.trim().to_owned()));
        }
        let resp_no_body = Resp {
            status,
            headers,
            body: Vec::new(),
        };
        if head_only || status == 100 || status == 204 {
            return resp_no_body;
        }
        let mut body = Vec::new();
        if resp_no_body
            .header("transfer-encoding")
            .is_some_and(|v| v.eq_ignore_ascii_case("chunked"))
        {
            loop {
                let size_line = self.read_line();
                let size = usize::from_str_radix(size_line.split(';').next().unwrap(), 16).unwrap();
                if size == 0 {
                    loop {
                        if self.read_line().is_empty() {
                            break;
                        }
                    }
                    break;
                }
                let mut chunk = vec![0u8; size];
                self.reader.read_exact(&mut chunk).unwrap();
                body.extend_from_slice(&chunk);
                let mut crlf = [0u8; 2];
                self.reader.read_exact(&mut crlf).unwrap();
                assert_eq!(&crlf, b"\r\n");
            }
        } else if let Some(len) = resp_no_body.header("content-length") {
            let len: usize = len.parse().unwrap();
            body.resize(len, 0);
            self.reader.read_exact(&mut body).unwrap();
        } else {
            self.reader.read_to_end(&mut body).unwrap();
        }
        Resp {
            body,
            ..resp_no_body
        }
    }
}

fn gzip(data: &[u8]) -> Vec<u8> {
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(data).unwrap();
    enc.finish().unwrap()
}

fn gunzip(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    GzDecoder::new(data).read_to_end(&mut out).unwrap();
    out
}

// ------------------------------------------------------------------ tests

#[test]
fn get_roundtrip() {
    let server = start_default();
    let mut c = Client::connect(server.local_addr());
    c.send(b"GET /ping HTTP/1.1\r\nHost: localhost\r\n\r\n");
    let resp = c.read_response(false);
    assert_eq!(resp.status, 200);
    assert_eq!(resp.header("content-type"), Some("application/json"));
    assert_eq!(resp.body, b"{\"ok\":true}");
}

#[test]
fn unknown_path_is_404() {
    let server = start_default();
    let mut c = Client::connect(server.local_addr());
    c.send(b"GET /nope HTTP/1.1\r\nHost: localhost\r\n\r\n");
    assert_eq!(c.read_response(false).status, 404);
}

#[test]
fn post_with_content_length() {
    let server = start_default();
    let mut c = Client::connect(server.local_addr());
    let body = b"abc123-payload";
    c.send(
        format!(
            "POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .as_bytes(),
    );
    c.send(body);
    let resp = c.read_response(false);
    assert_eq!(resp.status, 200);
    assert_eq!(resp.body, body);
}

#[test]
fn keep_alive_two_requests_one_connection() {
    let server = start_default();
    let mut c = Client::connect(server.local_addr());
    c.send(b"GET /ping HTTP/1.1\r\nHost: localhost\r\n\r\n");
    let first = c.read_response(false);
    assert_eq!(first.status, 200);
    assert_eq!(first.header("connection"), Some("keep-alive"));
    c.send(b"GET /text HTTP/1.1\r\nHost: localhost\r\n\r\n");
    let second = c.read_response(false);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello head");
}

#[test]
fn chunked_request_body() {
    let server = start_default();
    let mut c = Client::connect(server.local_addr());
    c.send(b"POST /echo HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n");
    c.send(b"3\r\nfoo\r\n4;ext=1\r\nbar!\r\n0\r\n\r\n");
    let resp = c.read_response(false);
    assert_eq!(resp.status, 200);
    assert_eq!(resp.body, b"foobar!");
    // Connection must stay usable after a chunked body.
    c.send(b"GET /ping HTTP/1.1\r\nHost: localhost\r\n\r\n");
    assert_eq!(c.read_response(false).status, 200);
}

#[test]
fn gzipped_request_body() {
    let server = start_default();
    let mut c = Client::connect(server.local_addr());
    let plain = b"hello gzip body hello gzip body";
    let compressed = gzip(plain);
    c.send(
        format!(
            "POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\n\r\n",
            compressed.len()
        )
        .as_bytes(),
    );
    c.send(&compressed);
    let resp = c.read_response(false);
    assert_eq!(resp.status, 200);
    assert_eq!(resp.body, plain);
}

#[test]
fn expect_100_continue() {
    let server = start_default();
    let mut c = Client::connect(server.local_addr());
    let body = b"continue-body";
    c.send(
        format!(
            "POST /echo HTTP/1.1\r\nHost: localhost\r\nExpect: 100-continue\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .as_bytes(),
    );
    let interim = c.read_response(false);
    assert_eq!(interim.status, 100);
    c.send(body);
    let resp = c.read_response(false);
    assert_eq!(resp.status, 200);
    assert_eq!(resp.body, body);
}

#[test]
fn head_sends_headers_without_body() {
    let server = start_default();
    let mut c = Client::connect(server.local_addr());
    c.send(b"HEAD /text HTTP/1.1\r\nHost: localhost\r\n\r\n");
    let resp = c.read_response(true);
    assert_eq!(resp.status, 200);
    assert_eq!(resp.header("content-length"), Some("10"));
    // No body was written: the next request on the same connection works.
    c.send(b"GET /ping HTTP/1.1\r\nHost: localhost\r\n\r\n");
    let next = c.read_response(false);
    assert_eq!(next.status, 200);
    assert_eq!(next.body, b"{\"ok\":true}");
}

#[test]
fn garbage_gets_400_and_close() {
    let server = start_default();
    let mut c = Client::connect(server.local_addr());
    c.send(b"NOT A VALID REQUEST\r\nstill garbage\r\n\r\n");
    let resp = c.read_response(false);
    assert_eq!(resp.status, 400);
    assert_eq!(resp.header("connection"), Some("close"));
    // Server closes: further reads hit EOF.
    let mut rest = Vec::new();
    c.reader.read_to_end(&mut rest).unwrap();
    assert!(rest.is_empty());
}

#[test]
fn response_gzip_when_accepted_and_enabled() {
    let server = start_server(ServerConfig {
        compress_responses: true,
        ..ServerConfig::default()
    });
    let mut c = Client::connect(server.local_addr());
    // gzhttp DefaultMinSize = 1024: only bodies >= 1024 bytes are compressed.
    c.send(b"GET /large HTTP/1.1\r\nHost: localhost\r\nAccept-Encoding: gzip, deflate\r\n\r\n");
    let resp = c.read_response(false);
    assert_eq!(resp.status, 200);
    assert_eq!(resp.header("content-encoding"), Some("gzip"));
    assert_eq!(gunzip(&resp.body), vec![b'a'; 2000]);
}

#[test]
fn response_not_gzipped_below_min_size() {
    // gzhttp DefaultMinSize = 1024: a body smaller than 1024 bytes is sent
    // uncompressed even when the client accepts gzip and compression is on.
    let server = start_server(ServerConfig {
        compress_responses: true,
        ..ServerConfig::default()
    });
    let mut c = Client::connect(server.local_addr());
    c.send(b"GET /text HTTP/1.1\r\nHost: localhost\r\nAccept-Encoding: gzip, deflate\r\n\r\n");
    let resp = c.read_response(false);
    assert_eq!(resp.status, 200);
    assert_eq!(resp.header("content-encoding"), None);
    assert_eq!(resp.body, b"hello head");
}

#[test]
fn response_not_gzipped_without_accept_encoding() {
    let server = start_server(ServerConfig {
        compress_responses: true,
        ..ServerConfig::default()
    });
    let mut c = Client::connect(server.local_addr());
    c.send(b"GET /text HTTP/1.1\r\nHost: localhost\r\n\r\n");
    let resp = c.read_response(false);
    assert_eq!(resp.status, 200);
    assert_eq!(resp.header("content-encoding"), None);
    assert_eq!(resp.body, b"hello head");
}

#[test]
fn response_not_gzipped_when_disabled() {
    let server = start_default();
    let mut c = Client::connect(server.local_addr());
    c.send(b"GET /text HTTP/1.1\r\nHost: localhost\r\nAccept-Encoding: gzip\r\n\r\n");
    let resp = c.read_response(false);
    assert_eq!(resp.header("content-encoding"), None);
    assert_eq!(resp.body, b"hello head");
}

#[test]
fn chunked_response_streaming() {
    let server = start_default();
    let mut c = Client::connect(server.local_addr());
    c.send(b"GET /stream HTTP/1.1\r\nHost: localhost\r\n\r\n");
    let resp = c.read_response(false);
    assert_eq!(resp.status, 200);
    assert_eq!(resp.header("transfer-encoding"), Some("chunked"));
    assert_eq!(resp.body, b"hello world");
    // Keep-alive still works after a chunked response.
    c.send(b"GET /ping HTTP/1.1\r\nHost: localhost\r\n\r\n");
    assert_eq!(c.read_response(false).status, 200);
}

#[test]
fn stream_response_passthrough() {
    let server = start_default();
    let mut c = Client::connect(server.local_addr());
    c.send(b"GET /stream-response HTTP/1.1\r\nHost: localhost\r\n\r\n");
    let resp = c.read_response(false);
    assert_eq!(resp.status, 200);
    assert_eq!(resp.header("x-backend"), Some("b1"));
    assert_eq!(resp.body, b"hello world");
    // Keep-alive still works after a stream_response.
    c.send(b"GET /ping HTTP/1.1\r\nHost: localhost\r\n\r\n");
    assert_eq!(c.read_response(false).status, 200);
}

#[test]
fn stream_response_head_request_has_no_body() {
    // RFC 7230 SS3.3.3: a HEAD response MUST NOT carry a message body. Prove
    // `stream_response` (used for proxied/pass-through responses) honors the
    // request's HEAD method: headers are sent, but no Transfer-Encoding,
    // chunked body, or terminator.
    let server = start_default();
    let mut c = Client::connect(server.local_addr());
    c.send(b"HEAD /stream-response HTTP/1.1\r\nHost: localhost\r\n\r\n");
    let resp = c.read_response(true);
    assert_eq!(resp.status, 200);
    assert_eq!(resp.header("x-backend"), Some("b1"));
    assert_eq!(resp.header("transfer-encoding"), None);
    // Keep-alive still works after a HEAD stream_response — proves no
    // leftover chunked framing desynced the connection.
    c.send(b"GET /ping HTTP/1.1\r\nHost: localhost\r\n\r\n");
    assert_eq!(c.read_response(false).status, 200);
}

#[test]
fn stream_response_strips_hop_by_hop_headers() {
    let server = start_default();
    let mut c = Client::connect(server.local_addr());
    c.send(b"GET /stream-response-hop HTTP/1.1\r\nHost: localhost\r\n\r\n");
    let resp = c.read_response(false);
    assert_eq!(resp.status, 200);
    // End-to-end header survives.
    assert_eq!(resp.header("x-keep"), Some("v"));
    // The writer's own chunked framing is the only Transfer-Encoding.
    let te: Vec<&str> = resp
        .headers
        .iter()
        .filter(|(k, _)| k == "transfer-encoding")
        .map(|(_, v)| v.as_str())
        .collect();
    assert_eq!(te, ["chunked"]);
    assert_eq!(resp.body, b"hello world");
    // Hop-by-hop and framing headers were stripped: the Connection-listed
    // X-Hop, the passthrough Transfer-Encoding: gzip, and Content-Length.
    assert_eq!(resp.header("x-hop"), None);
    assert_eq!(resp.header("content-length"), None);
    assert!(!resp
        .headers
        .iter()
        .any(|(k, v)| k == "connection" && v.eq_ignore_ascii_case("x-hop")));
}

#[test]
fn query_string_decoding_end_to_end() {
    let server = start_default();
    let mut c = Client::connect(server.local_addr());
    c.send(b"GET /query?a=1&b=hello+world&c=%2Fpath%2F&flag HTTP/1.1\r\nHost: localhost\r\n\r\n");
    let resp = c.read_response(false);
    assert_eq!(resp.status, 200);
    assert_eq!(
        std::str::from_utf8(&resp.body).unwrap(),
        "a=1\nb=hello world\nc=/path/\nflag=\n"
    );
}

#[test]
fn form_body_parsing() {
    let server = start_default();
    let mut c = Client::connect(server.local_addr());
    let body = b"name=John+Q&msg=hi%21&query=up%7B%7D";
    c.send(
        format!(
            "POST /form HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .as_bytes(),
    );
    c.send(body);
    let resp = c.read_response(false);
    assert_eq!(resp.status, 200);
    assert_eq!(
        std::str::from_utf8(&resp.body).unwrap(),
        "name=John Q\nmsg=hi!\nquery=up{}\n"
    );
}

#[test]
fn percent_decode_units() {
    assert_eq!(percent_decode("plain"), "plain");
    assert_eq!(percent_decode("a%20b"), "a b");
    assert_eq!(percent_decode("a+b"), "a+b"); // '+' untouched without plus mode
    assert_eq!(percent_decode("%zz"), "%zz"); // invalid escape passes through
    assert_eq!(percent_decode("%2"), "%2"); // truncated escape passes through
    assert_eq!(percent_decode("100%"), "100%");
}

#[test]
fn parse_query_units() {
    let pairs: Vec<_> = parse_query("k=v+1&x=%2Fpath&&flag&e=").collect();
    assert_eq!(pairs.len(), 4);
    assert_eq!(pairs[0], ("k".into(), "v 1".into()));
    assert_eq!(pairs[1], ("x".into(), "/path".into()));
    assert_eq!(pairs[2], ("flag".into(), "".into()));
    assert_eq!(pairs[3], ("e".into(), "".into()));
}

#[test]
fn concurrent_connections() {
    let server = Arc::new(start_default());
    let addr = server.local_addr();
    let mut handles = Vec::new();
    for _ in 0..32 {
        handles.push(std::thread::spawn(move || {
            let mut c = Client::connect(addr);
            c.send(b"GET /ping HTTP/1.1\r\nHost: localhost\r\n\r\n");
            let resp = c.read_response(false);
            assert_eq!(resp.status, 200);
            assert_eq!(resp.body, b"{\"ok\":true}");
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn graceful_stop_unblocks_accept_and_closes_connections() {
    let server = start_default();
    let addr = server.local_addr();

    // Idle connection: its thread is blocked reading the next request.
    let mut idle = TcpStream::connect(addr).unwrap();
    idle.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    // Give the accept loop a moment to register it.
    std::thread::sleep(Duration::from_millis(100));

    let started = Instant::now();
    server.stop();
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "stop() took {:?}",
        started.elapsed()
    );

    // The idle connection was shut down by stop().
    let mut scratch = [0u8; 16];
    match idle.read(&mut scratch) {
        Ok(0) | Err(_) => {}
        Ok(n) => panic!("expected EOF on stopped connection, got {n} bytes"),
    }

    // stop() is idempotent and Drop is safe after stop.
    server.stop();
    drop(server);
}

#[test]
fn curl_roundtrip_if_available() {
    let curl_ok = std::process::Command::new("curl")
        .arg("--version")
        .output()
        .is_ok_and(|out| out.status.success());
    if !curl_ok {
        return; // curl not installed; skip silently
    }
    let server = start_default();
    let out = std::process::Command::new("curl")
        .args(["-sS", &format!("http://{}/ping", server.local_addr())])
        .output()
        .unwrap();
    assert!(out.status.success(), "curl failed: {out:?}");
    assert_eq!(out.stdout, b"{\"ok\":true}");
}
