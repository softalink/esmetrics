//! A minimal synchronous DNS client over `std::net` for the record types
//! `std::net::ToSocketAddrs` (getaddrinfo) can't answer: `SRV` and `MX`.
//!
//! Hand-rolled rather than pulling `hickory-proto` — see the module doc in
//! [`super`]. It builds one standard query (header + single question),
//! sends it over UDP, and parses the answer section. If the UDP response has
//! the truncation (`TC`) bit set it retries over TCP (2-byte length prefix).
//! DNS name compression is handled in the parser ([`read_name`]).
//!
//! No async, no tokio: a blocking [`std::net::UdpSocket`] / [`std::net::
//! TcpStream`] with a read timeout so the caller's refresh thread (and its
//! `Drop`) stay responsive.

use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs, UdpSocket};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

/// DNS `QTYPE`/`TYPE` for `SRV` records (RFC 2782).
pub const QTYPE_SRV: u16 = 33;
/// DNS `QTYPE`/`TYPE` for `MX` records.
pub const QTYPE_MX: u16 = 15;
/// DNS `CLASS` `IN` (Internet).
const CLASS_IN: u16 = 1;
/// Recursion-desired flag in the header's flags word.
const FLAG_RD: u16 = 0x0100;
/// Truncation (`TC`) flag in a response's flags word.
const FLAG_TC: u16 = 0x0200;
/// Fixed DNS header size in bytes.
const HEADER_LEN: usize = 12;
/// Standard DNS server port.
const DNS_PORT: u16 = 53;
/// Cap on a single UDP datagram we'll read (enough for any non-truncated
/// answer; larger responses set `TC` and we fall back to TCP).
const UDP_BUF: usize = 4096;

/// One parsed SRV or MX answer record. For SRV, `port` is the record's own
/// port and `target` its target host; for MX, `port` is unused (the caller
/// supplies the configured port) and `target` is the exchange host. Trailing
/// root dots are already stripped.
#[derive(Debug, Clone, PartialEq)]
pub struct DnsRecord {
    pub target: String,
    pub port: u16,
}

/// Sends a `qtype` query for `qname` to `nameserver` (a `host:port` or bare
/// `host` — bare hosts default to `:53`) and returns the parsed SRV/MX answer
/// records. `timeout` bounds each socket read. Never panics: any transport,
/// timeout, or malformed-response condition is an `Err`.
pub fn query(
    nameserver: &str,
    qname: &str,
    qtype: u16,
    timeout: Duration,
) -> io::Result<Vec<DnsRecord>> {
    let addr = resolve_nameserver_addr(nameserver)?;
    let query_id = next_query_id();
    let msg = build_query(query_id, qname, qtype)?;

    let response = send_udp(&addr, &msg, timeout)?;
    let response = if is_truncated(&response) {
        send_tcp(&addr, &msg, timeout)?
    } else {
        response
    };
    parse_answers(&response, query_id, qtype)
}

/// A per-query pseudo-random 16-bit transaction id. A process-startup seed
/// (from a non-deterministic `RandomState` hasher, so it varies per process)
/// is XORed with a monotonic counter, so consecutive queries get different
/// ids without a fixed constant. Not cryptographic — just enough that an
/// off-path attacker can't trivially predict the id (the source-address check
/// in [`send_udp`] and the id check in [`parse_answers`] do the real
/// spoof-resistance).
fn next_query_id() -> u16 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    use std::sync::OnceLock;

    static SEED: OnceLock<u16> = OnceLock::new();
    static COUNTER: AtomicU32 = AtomicU32::new(0);

    let seed = *SEED.get_or_init(|| {
        let mut hasher = RandomState::new().build_hasher();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        hasher.write_u64(nanos);
        hasher.finish() as u16
    });
    seed ^ (COUNTER.fetch_add(1, Ordering::Relaxed) as u16)
}

/// Turns a `nameserver` string into a concrete socket address, defaulting the
/// port to 53 when absent. Accepts `1.2.3.4`, `1.2.3.4:5353`, `[::1]:53`, or a
/// hostname.
fn resolve_nameserver_addr(nameserver: &str) -> io::Result<std::net::SocketAddr> {
    let has_port = if let Some(rest) = nameserver.strip_prefix('[') {
        rest.contains("]:")
    } else {
        // A bare IPv6 literal has multiple colons and no port; anything with
        // exactly one colon is host:port.
        nameserver.matches(':').count() == 1
    };
    let with_port = if has_port {
        nameserver.to_string()
    } else {
        format!("{nameserver}:{DNS_PORT}")
    };
    with_port
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "nameserver did not resolve"))
}

/// Builds a standard recursion-desired query: 12-byte header, one question.
fn build_query(id: u16, qname: &str, qtype: u16) -> io::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(HEADER_LEN + qname.len() + 6);
    buf.extend_from_slice(&id.to_be_bytes());
    buf.extend_from_slice(&FLAG_RD.to_be_bytes());
    buf.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT
    buf.extend_from_slice(&0u16.to_be_bytes()); // ANCOUNT
    buf.extend_from_slice(&0u16.to_be_bytes()); // NSCOUNT
    buf.extend_from_slice(&0u16.to_be_bytes()); // ARCOUNT
    encode_name(qname, &mut buf)?;
    buf.extend_from_slice(&qtype.to_be_bytes());
    buf.extend_from_slice(&CLASS_IN.to_be_bytes());
    Ok(buf)
}

/// Encodes a domain name into length-prefixed labels terminated by a zero
/// byte. A trailing dot is tolerated. Rejects labels > 63 bytes.
fn encode_name(name: &str, buf: &mut Vec<u8>) -> io::Result<()> {
    for label in name.split('.') {
        if label.is_empty() {
            continue; // root, or a trailing dot.
        }
        if label.len() > 63 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DNS label exceeds 63 bytes",
            ));
        }
        buf.push(label.len() as u8);
        buf.extend_from_slice(label.as_bytes());
    }
    buf.push(0);
    Ok(())
}

fn is_truncated(response: &[u8]) -> bool {
    response.len() >= HEADER_LEN && (u16::from_be_bytes([response[2], response[3]]) & FLAG_TC) != 0
}

/// Sends `msg` over UDP and returns the datagram received from the nameserver.
/// Binds an ephemeral local socket matching the nameserver's address family.
/// Any datagram whose source address is not the nameserver we queried is
/// dropped and the read retried — so an off-path host can't as easily inject a
/// spoofed response — with the read timeout bounding the retry loop.
fn send_udp(addr: &std::net::SocketAddr, msg: &[u8], timeout: Duration) -> io::Result<Vec<u8>> {
    let bind_addr = if addr.is_ipv6() {
        "[::]:0"
    } else {
        "0.0.0.0:0"
    };
    let sock = UdpSocket::bind(bind_addr)?;
    sock.set_read_timeout(Some(timeout))?;
    sock.set_write_timeout(Some(timeout))?;
    sock.send_to(msg, addr)?;
    let mut buf = vec![0u8; UDP_BUF];
    loop {
        let (n, from) = sock.recv_from(&mut buf)?;
        if from == *addr {
            buf.truncate(n);
            return Ok(buf);
        }
        // Datagram from an unexpected source (possible spoof); drop it and read
        // again. The socket read timeout bounds this loop.
    }
}

/// Sends `msg` over TCP (2-byte big-endian length prefix on request and
/// response) and returns the response message body.
fn send_tcp(addr: &std::net::SocketAddr, msg: &[u8], timeout: Duration) -> io::Result<Vec<u8>> {
    let mut stream = TcpStream::connect_timeout(addr, timeout)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    let len = u16::try_from(msg.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "DNS query too large for TCP"))?;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(msg)?;
    let mut len_buf = [0u8; 2];
    stream.read_exact(&mut len_buf)?;
    let resp_len = u16::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; resp_len];
    stream.read_exact(&mut buf)?;
    Ok(buf)
}

/// Parses the answer section of `response`, keeping only records of `qtype`
/// (SRV or MX). Validates the response id and skips the question section.
fn parse_answers(response: &[u8], query_id: u16, qtype: u16) -> io::Result<Vec<DnsRecord>> {
    if response.len() < HEADER_LEN {
        return Err(malformed("response shorter than DNS header"));
    }
    let id = u16::from_be_bytes([response[0], response[1]]);
    if id != query_id {
        return Err(malformed("response id does not match query id"));
    }
    let qdcount = u16::from_be_bytes([response[4], response[5]]);
    let ancount = u16::from_be_bytes([response[6], response[7]]);

    let mut pos = HEADER_LEN;
    for _ in 0..qdcount {
        let (_name, next) = read_name(response, pos)?;
        pos = next + 4; // skip QTYPE(2) + QCLASS(2)
        if pos > response.len() {
            return Err(malformed("truncated question section"));
        }
    }

    let mut records = Vec::new();
    for _ in 0..ancount {
        let (_name, next) = read_name(response, pos)?;
        pos = next;
        if pos + 10 > response.len() {
            return Err(malformed("truncated resource record header"));
        }
        let rtype = u16::from_be_bytes([response[pos], response[pos + 1]]);
        let rdlength = u16::from_be_bytes([response[pos + 8], response[pos + 9]]) as usize;
        let rdata_start = pos + 10;
        let rdata_end = rdata_start + rdlength;
        if rdata_end > response.len() {
            return Err(malformed("resource record rdata out of bounds"));
        }
        if rtype == qtype {
            if let Some(record) = parse_rdata(response, rdata_start, qtype)? {
                records.push(record);
            }
        }
        pos = rdata_end;
    }
    Ok(records)
}

/// Parses one SRV/MX RDATA record starting at `rdata_start`.
fn parse_rdata(msg: &[u8], rdata_start: usize, qtype: u16) -> io::Result<Option<DnsRecord>> {
    match qtype {
        QTYPE_SRV => {
            // priority(2) weight(2) port(2) target(name)
            if rdata_start + 6 > msg.len() {
                return Err(malformed("truncated SRV rdata"));
            }
            let port = u16::from_be_bytes([msg[rdata_start + 4], msg[rdata_start + 5]]);
            let (target, _) = read_name(msg, rdata_start + 6)?;
            Ok(Some(DnsRecord {
                target: strip_root_dot(&target),
                port,
            }))
        }
        QTYPE_MX => {
            // preference(2) exchange(name)
            if rdata_start + 2 > msg.len() {
                return Err(malformed("truncated MX rdata"));
            }
            let (target, _) = read_name(msg, rdata_start + 2)?;
            Ok(Some(DnsRecord {
                target: strip_root_dot(&target),
                port: 0,
            }))
        }
        _ => Ok(None),
    }
}

fn strip_root_dot(name: &str) -> String {
    name.trim_end_matches('.').to_string()
}

/// Reads a (possibly compressed) domain name at `pos`. Returns the decoded
/// name (dot-joined, no trailing root dot) and the offset just past the name
/// *at this position* — i.e. past the first compression pointer or the
/// terminating zero byte, so the caller advances correctly even when the name
/// jumped elsewhere via a pointer. Guards against pointer loops.
fn read_name(msg: &[u8], pos: usize) -> io::Result<(String, usize)> {
    let mut labels: Vec<String> = Vec::new();
    let mut cur = pos;
    let mut next_after: Option<usize> = None;
    let mut jumps = 0usize;

    loop {
        if cur >= msg.len() {
            return Err(malformed("name offset out of bounds"));
        }
        let len = msg[cur] as usize;
        if len & 0xc0 == 0xc0 {
            // Compression pointer: two bytes, 14-bit offset.
            if cur + 1 >= msg.len() {
                return Err(malformed("truncated compression pointer"));
            }
            let ptr = ((len & 0x3f) << 8) | msg[cur + 1] as usize;
            if next_after.is_none() {
                next_after = Some(cur + 2);
            }
            jumps += 1;
            if jumps > 128 {
                return Err(malformed("too many compression pointers"));
            }
            cur = ptr;
            continue;
        }
        if len == 0 {
            let end = next_after.unwrap_or(cur + 1);
            return Ok((labels.join("."), end));
        }
        cur += 1;
        if cur + len > msg.len() {
            return Err(malformed("label runs past end of message"));
        }
        let label = String::from_utf8_lossy(&msg[cur..cur + len]).into_owned();
        labels.push(label);
        cur += len;
    }
}

fn malformed(msg: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("malformed DNS response: {msg}"),
    )
}

/// Builds a canned single-answer response for tests (the parser tests here and
/// the stub-DNS integration test in `dns_tests`). `qtype` is SRV or MX;
/// `port`/`target` populate the one answer record. The question section uses a
/// fixed placeholder name (the parser skips the question without comparing it).
#[cfg(test)]
pub(crate) fn build_test_response(id: u16, qtype: u16, target: &str, port: u16) -> Vec<u8> {
    let mut msg = Vec::new();
    msg.extend_from_slice(&id.to_be_bytes());
    msg.extend_from_slice(&0x8180u16.to_be_bytes()); // response, RD+RA
    msg.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT
    msg.extend_from_slice(&1u16.to_be_bytes()); // ANCOUNT
    msg.extend_from_slice(&0u16.to_be_bytes());
    msg.extend_from_slice(&0u16.to_be_bytes());
    encode_name("_svc._tcp.q", &mut msg).unwrap();
    msg.extend_from_slice(&qtype.to_be_bytes());
    msg.extend_from_slice(&CLASS_IN.to_be_bytes());
    // Answer: name pointer back to the question at offset 12.
    msg.push(0xc0);
    msg.push(HEADER_LEN as u8);
    msg.extend_from_slice(&qtype.to_be_bytes());
    msg.extend_from_slice(&CLASS_IN.to_be_bytes());
    msg.extend_from_slice(&300u32.to_be_bytes()); // TTL
    let mut rdata = Vec::new();
    match qtype {
        QTYPE_SRV => {
            rdata.extend_from_slice(&10u16.to_be_bytes()); // priority
            rdata.extend_from_slice(&20u16.to_be_bytes()); // weight
            rdata.extend_from_slice(&port.to_be_bytes());
            encode_name(target, &mut rdata).unwrap();
        }
        QTYPE_MX => {
            rdata.extend_from_slice(&10u16.to_be_bytes()); // preference
            encode_name(target, &mut rdata).unwrap();
        }
        _ => unreachable!("build_test_response only supports SRV/MX"),
    }
    msg.extend_from_slice(&(rdata.len() as u16).to_be_bytes());
    msg.extend_from_slice(&rdata);
    msg
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_name_round_trips_through_read_name() {
        let mut buf = Vec::new();
        encode_name("sip.example.com", &mut buf).unwrap();
        let (name, end) = read_name(&buf, 0).unwrap();
        assert_eq!(name, "sip.example.com");
        assert_eq!(end, buf.len());
    }

    #[test]
    fn encode_name_tolerates_trailing_dot() {
        let mut buf = Vec::new();
        encode_name("example.com.", &mut buf).unwrap();
        let (name, _) = read_name(&buf, 0).unwrap();
        assert_eq!(name, "example.com");
    }

    #[test]
    fn read_name_follows_compression_pointer() {
        // Message: at offset 0, "com" root; a pointer elsewhere points back.
        let mut msg = Vec::new();
        // offset 0: labels for "example.com"
        encode_name("example.com", &mut msg).unwrap();
        let ptr_pos = msg.len();
        // A name "sip" + pointer to offset 0.
        msg.push(3);
        msg.extend_from_slice(b"sip");
        msg.push(0xc0);
        msg.push(0x00);
        let (name, end) = read_name(&msg, ptr_pos).unwrap();
        assert_eq!(name, "sip.example.com");
        assert_eq!(end, msg.len());
    }

    #[test]
    fn parse_answers_extracts_srv_record() {
        let msg = build_test_response(0x4553, QTYPE_SRV, "sip.example.com", 5060);
        let recs = parse_answers(&msg, 0x4553, QTYPE_SRV).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].target, "sip.example.com");
        assert_eq!(recs[0].port, 5060);
    }

    #[test]
    fn parse_answers_extracts_mx_record() {
        let msg = build_test_response(0x4553, QTYPE_MX, "mail.example.com", 0);
        let recs = parse_answers(&msg, 0x4553, QTYPE_MX).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].target, "mail.example.com");
    }

    #[test]
    fn parse_answers_rejects_id_mismatch() {
        let msg = build_test_response(0x1111, QTYPE_SRV, "sip.example.com", 5060);
        assert!(parse_answers(&msg, 0x4553, QTYPE_SRV).is_err());
    }

    #[test]
    fn next_query_id_varies_between_calls() {
        // Consecutive ids differ (the counter advances) — no fixed constant.
        assert_ne!(next_query_id(), next_query_id());
    }

    #[test]
    fn read_name_rejects_self_referential_pointer_loop() {
        // A two-byte compression pointer at offset 0 pointing back to offset 0.
        let msg = vec![0xc0, 0x00];
        assert!(read_name(&msg, 0).is_err(), "self-loop must Err, not hang");
    }

    #[test]
    fn read_name_rejects_mutual_pointer_loop() {
        // Offset 0 -> 2, offset 2 -> 0: two pointers referencing each other.
        let msg = vec![0xc0, 0x02, 0xc0, 0x00];
        assert!(
            read_name(&msg, 0).is_err(),
            "mutual loop must Err, not hang"
        );
    }

    #[test]
    fn parse_answers_rejects_packet_shorter_than_header() {
        assert!(parse_answers(&[0u8; 5], 0x1234, QTYPE_SRV).is_err());
    }

    #[test]
    fn parse_answers_rejects_truncated_rr_header() {
        // Header claims one answer, but the packet is cut off inside the RR
        // header (before the 10-byte type/class/ttl/rdlength block).
        let mut msg = header_with_one_answer();
        msg.push(0); // answer name = root
        msg.extend_from_slice(&QTYPE_SRV.to_be_bytes()); // then truncated
        assert!(parse_answers(&msg, 0x1234, QTYPE_SRV).is_err());
    }

    #[test]
    fn parse_answers_rejects_rdata_past_end() {
        // A complete RR header whose rdlength runs past the buffer end.
        let mut msg = header_with_one_answer();
        msg.push(0); // answer name = root
        msg.extend_from_slice(&QTYPE_SRV.to_be_bytes());
        msg.extend_from_slice(&CLASS_IN.to_be_bytes());
        msg.extend_from_slice(&300u32.to_be_bytes()); // TTL
        msg.extend_from_slice(&1000u16.to_be_bytes()); // rdlength >> remaining
                                                       // no rdata bytes follow
        assert!(parse_answers(&msg, 0x1234, QTYPE_SRV).is_err());
    }

    /// A 12-byte DNS header (id 0x1234, response flags) with QDCOUNT=0 and
    /// ANCOUNT=1, for the truncated-answer adversarial tests.
    fn header_with_one_answer() -> Vec<u8> {
        let mut msg = Vec::new();
        msg.extend_from_slice(&0x1234u16.to_be_bytes()); // id
        msg.extend_from_slice(&0x8180u16.to_be_bytes()); // response, RD+RA
        msg.extend_from_slice(&0u16.to_be_bytes()); // QDCOUNT
        msg.extend_from_slice(&1u16.to_be_bytes()); // ANCOUNT
        msg.extend_from_slice(&0u16.to_be_bytes()); // NSCOUNT
        msg.extend_from_slice(&0u16.to_be_bytes()); // ARCOUNT
        msg
    }
}
