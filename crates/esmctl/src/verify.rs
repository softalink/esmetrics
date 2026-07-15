//! The `verify-block` subcommand: parses a native-format export block file and
//! validates every block by fully unmarshaling it. Ports the `verify-block`
//! command in `main.go` plus the native stream framing from
//! `lib/protoparser/native/stream/streamparser.go`.
//!
//! The block decode reuses `esm-storage`'s real `MetricName::unmarshal` +
//! `Block::unmarshal_portable`/`unmarshal_data`, so a corrupt block fails
//! exactly as it would on import.

use esm_storage::{Block, MetricName, TimeRange};

const MAX_BUF_SIZE: usize = 1024 * 1024;

/// Verifies the native export block file at `path`. Ports the `verify-block`
/// action; `gunzip` gzip-decompresses the file before parsing.
pub(crate) fn run(path: &str, gunzip: bool) -> Result<(), String> {
    log::info!("verifying block at path={path:?}");
    let raw =
        std::fs::read(path).map_err(|e| format!("cannot open exported block at {path:?}: {e}"))?;
    let data = if gunzip {
        let mut out = Vec::new();
        std::io::Read::read_to_end(&mut flate2::read::GzDecoder::new(&raw[..]), &mut out)
            .map_err(|e| format!("cannot gunzip exported block at {path:?}: {e}"))?;
        out
    } else {
        raw
    };

    let mut cur = &data[..];
    // Time range: two big-endian int64 (min, max).
    if cur.len() < 16 {
        return Err("cannot read time range".to_string());
    }
    let tr = TimeRange {
        min_timestamp: i64::from_be_bytes(cur[0..8].try_into().unwrap()),
        max_timestamp: i64::from_be_bytes(cur[8..16].try_into().unwrap()),
    };
    cur = &cur[16..];

    let mut blocks: u64 = 0;
    let mut mn = MetricName::default();
    let mut block = Block::default();
    let mut ts_buf: Vec<i64> = Vec::new();
    let mut val_buf: Vec<f64> = Vec::new();

    while !cur.is_empty() {
        // metricName.
        let (mn_buf, rest) = read_sized(cur, "metricName")?;
        cur = rest;
        mn.unmarshal(mn_buf).map_err(|e| {
            format!(
                "cannot unmarshal metricName from {} bytes: {e}",
                mn_buf.len()
            )
        })?;

        // native block.
        let (blk_buf, rest) = read_sized(cur, "native block")?;
        cur = rest;
        let tail = block.unmarshal_portable(blk_buf).map_err(|e| {
            format!(
                "cannot unmarshal native block from {} bytes: {e}",
                blk_buf.len()
            )
        })?;
        if !tail.is_empty() {
            return Err(format!(
                "unexpected non-empty tail left after unmarshaling native block; len(tail)={}",
                tail.len()
            ));
        }
        // Decode + range-filter the rows to validate the block payload.
        block
            .unmarshal_data()
            .map_err(|e| format!("cannot decode native block data: {e}"))?;
        ts_buf.clear();
        val_buf.clear();
        block.append_rows_with_time_range_filter(&mut ts_buf, &mut val_buf, tr);

        blocks += 1;
    }

    log::info!("successfully verified block at path={path:?}, blockCount={blocks}");
    Ok(())
}

/// Reads a 4-byte big-endian size prefix and the following payload slice,
/// returning `(payload, rest)`. Ports the `sizeBuf`/`ResizeNoCopy` reads.
fn read_sized<'a>(buf: &'a [u8], what: &str) -> Result<(&'a [u8], &'a [u8]), String> {
    if buf.len() < 4 {
        return Err(format!("cannot read {what} size"));
    }
    let size = u32::from_be_bytes(buf[0..4].try_into().unwrap()) as usize;
    if size > MAX_BUF_SIZE {
        return Err(format!(
            "too big {what} size; got {size}; shouldn't exceed {MAX_BUF_SIZE}"
        ));
    }
    let rest = &buf[4..];
    if rest.len() < size {
        return Err(format!("cannot read {what} with size {size} bytes"));
    }
    Ok((&rest[..size], &rest[size..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gunzip_of_valid_empty_block() {
        // A gzip-compressed 16-byte time-range header with no blocks →
        // gunzip decodes, zero blocks, success.
        use std::io::Write;
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        enc.write_all(&[0u8; 16]).unwrap();
        let gz = enc.finish().unwrap();
        let dir = std::env::temp_dir();
        let path = dir.join("esmctl_verify_test_gz.bin.gz");
        std::fs::write(&path, gz).unwrap();
        let r = run(path.to_str().unwrap(), true);
        let _ = std::fs::remove_file(&path);
        assert!(r.is_ok());
    }

    #[test]
    fn missing_file_errors() {
        assert!(run("/nonexistent/block.bin", false).is_err());
    }

    #[test]
    fn truncated_time_range_errors() {
        let dir = std::env::temp_dir();
        let path = dir.join("esmctl_verify_test_short.bin");
        std::fs::write(&path, b"short").unwrap();
        let r = run(path.to_str().unwrap(), false);
        let _ = std::fs::remove_file(&path);
        assert!(r.is_err());
    }

    #[test]
    fn empty_after_time_range_is_ok() {
        // 16 bytes of time range and no blocks → zero blocks, success.
        let dir = std::env::temp_dir();
        let path = dir.join("esmctl_verify_test_empty.bin");
        std::fs::write(&path, [0u8; 16]).unwrap();
        let r = run(path.to_str().unwrap(), false);
        let _ = std::fs::remove_file(&path);
        assert!(r.is_ok());
    }
}
