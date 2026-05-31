#![allow(clippy::used_underscore_binding)]
//! Ingest buffer-path cost decomposition (measurement only; no production code
//! touched). The profiler shows buffer-ingest is ~65% of ingest at ~2.0M
//! samples/s (~500 ns/sample). This splits that budget into its parts —
//! double-key-hash vs map probes vs push — so we know whether a "hash once"
//! rewrite (eliminating the duplicate full-key FNV that shard routing and the
//! name->tsid lookup each do) can plausibly close the gap to VM.
//!
//! ```text
//! cargo test -p esm-storage --release --test ingest_hash_split -- --ignored --nocapture
//! ```
#![allow(clippy::print_stderr, clippy::cast_precision_loss, clippy::unwrap_used)]

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::time::Instant;

/// Exact replica of `storage.rs`'s FnvHasher (FNV-1a 64-bit).
#[derive(Default)]
struct Fnv(u64);
impl Hasher for Fnv {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        let mut h = if self.0 == 0 { 0xcbf2_9ce4_8422_2325 } else { self.0 };
        for &b in bytes {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        self.0 = h;
    }
}
type FnvMap<K, V> = HashMap<K, V, BuildHasherDefault<Fnv>>;

/// Inline FNV-1a over a byte slice (matches `sharded.rs::shard_idx`).
fn fnv(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// TSBS cpu-only key shape: `cpu_usage_<field>{hostname="host_N",region=...}`.
/// 1000 hosts × 10 fields = 10k distinct ~230-byte keys.
fn build_keys() -> Vec<Vec<u8>> {
    const HOSTS: usize = 1000;
    const FIELDS: [&str; 10] = [
        "usage_user",
        "usage_system",
        "usage_idle",
        "usage_nice",
        "usage_iowait",
        "usage_irq",
        "usage_softirq",
        "usage_steal",
        "usage_guest",
        "usage_guest_nice",
    ];
    let mut keys = Vec::with_capacity(HOSTS * FIELDS.len());
    for h in 0..HOSTS {
        let tags = format!(
            "{{hostname=\"host_{h}\",region=\"us-west-1\",datacenter=\"us-west-1a\",rack=\"12\",os=\"Ubuntu16\",arch=\"x64\",team=\"SF\",service=\"3\",service_version=\"1\",service_environment=\"test\"}}"
        );
        for f in FIELDS {
            keys.push(format!("cpu_{f}{tags}").into_bytes());
        }
    }
    keys
}

#[test]
#[ignore = "manual measurement; run with --release --ignored --nocapture"]
fn ingest_hash_split() {
    let keys = build_keys();
    let n = keys.len();
    let avg_len = keys.iter().map(Vec::len).sum::<usize>() / n;
    // Steady state: every key already interned (the sustained-load path).
    let mut name_to_tsid: FnvMap<Vec<u8>, u64> = FnvMap::default();
    for (i, k) in keys.iter().enumerate() {
        name_to_tsid.insert(k.clone(), i as u64);
    }
    let mut pending: FnvMap<u64, Vec<(i64, i64)>> = FnvMap::default();
    for i in 0..n as u64 {
        pending.insert(i, Vec::new());
    }

    let rounds = 200u32;
    let total = n as f64 * f64::from(rounds);
    let ms = |d: std::time::Duration| d.as_secs_f64() * 1000.0;
    let nsps = |d: std::time::Duration| d.as_secs_f64() * 1e9 / total;

    eprintln!("--- ingest buffer-path cost split ---");
    eprintln!("{n} keys, avg {avg_len} bytes, {rounds} rounds, {total:.0} samples\n");

    // (A) Double full-key hash — what shard_idx + name_to_tsid probe pay today.
    let t = Instant::now();
    let mut sink = 0u64;
    for _ in 0..rounds {
        for k in &keys {
            sink = sink.wrapping_add(fnv(k)).wrapping_add(fnv(k));
        }
    }
    let d_hash2 = t.elapsed();

    // (B) Single full-key hash — the "hash once" ceiling for the routing+probe hash.
    let t = Instant::now();
    for _ in 0..rounds {
        for k in &keys {
            sink = sink.wrapping_add(fnv(k));
        }
    }
    let d_hash1 = t.elapsed();

    // (C) 24-byte tsid hash (the pending probe's key hash today).
    let t = Instant::now();
    for _ in 0..rounds {
        for i in 0..n as u64 {
            sink = sink.wrapping_add(fnv(&i.to_le_bytes()));
        }
    }
    let d_hashtsid = t.elapsed();

    // (D) Two map probes per sample (name->tsid, then tsid->pending) + push.
    let t = Instant::now();
    for _ in 0..rounds {
        for k in &keys {
            let tsid = *name_to_tsid.get(k).unwrap();
            pending.get_mut(&tsid).unwrap().push((1, 2));
            sink = sink.wrapping_add(tsid);
        }
    }
    let d_maps = t.elapsed();
    for v in pending.values_mut() {
        v.clear();
    }

    // (E) FULL current path: hash for shard route + name probe + tsid hash for
    //     pending probe + push. (Mirrors ShardedStorage route + buffer_one.)
    let t = Instant::now();
    for _ in 0..rounds {
        for k in &keys {
            let _shard = fnv(k) % 32; // shard_idx
            let tsid = *name_to_tsid.get(k).unwrap(); // re-hashes k internally
            pending.get_mut(&tsid).unwrap().push((1, 2));
            sink = sink.wrapping_add(tsid).wrapping_add(_shard);
        }
    }
    let d_full = t.elapsed();
    for v in pending.values_mut() {
        v.clear();
    }

    eprintln!(
        "(A) hash full key x2     : {:>7.1} ns/sample  [{:.1} ms]",
        nsps(d_hash2),
        ms(d_hash2)
    );
    eprintln!(
        "(B) hash full key x1     : {:>7.1} ns/sample  [{:.1} ms]",
        nsps(d_hash1),
        ms(d_hash1)
    );
    eprintln!(
        "(C) hash 8-byte tsid     : {:>7.1} ns/sample  [{:.1} ms]",
        nsps(d_hashtsid),
        ms(d_hashtsid)
    );
    eprintln!("(D) 2 map probes + push  : {:>7.1} ns/sample  [{:.1} ms]", nsps(d_maps), ms(d_maps));
    eprintln!("(E) FULL current path    : {:>7.1} ns/sample  [{:.1} ms]", nsps(d_full), ms(d_full));
    eprintln!();
    eprintln!("hash-once saving (A-B)   : {:>7.1} ns/sample", nsps(d_hash2) - nsps(d_hash1));
    let full = nsps(d_full);
    eprintln!(
        "  => removing 1 full-key hash is ~{:.0}% of the FULL buffer path",
        100.0 * (nsps(d_hash2) - nsps(d_hash1)) / full
    );
    eprintln!("(E) implies buffer throughput ~{:.2}M samples/s", 1000.0 / full);
    eprintln!("sink={sink}");
}
