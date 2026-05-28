//! CSV import.
//!
//! Default column layout: `metric,timestamp_ms,value`. Optional header row
//! starting with `#` is ignored.
//! A `tags` column (comma-separated `k=v;k=v` pairs in a single column)
//! can be added; tags become labels.

#![allow(clippy::cast_possible_truncation)]

use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedSample {
    pub metric_name: Vec<u8>,
    pub timestamp_ms: i64,
    pub value: i64,
}

/// Parse a CSV body. Strict format: 3 or 4 columns (metric, timestamp_ms,
/// value, [tags]). Comma-separated, no quoting needed for our use case.
///
/// # Errors
/// Returns [`ParseError`] on a malformed line.
pub fn parse(input: &str) -> Result<Vec<ParsedSample>, ParseError> {
    let mut out = Vec::new();
    for (line_no, raw) in input.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        parse_line(line, &mut out).map_err(|message| ParseError::Line { line_no, message })?;
    }
    Ok(out)
}

fn parse_line(line: &str, out: &mut Vec<ParsedSample>) -> Result<(), String> {
    let parts: Vec<&str> = line.split(',').collect();
    if parts.len() < 3 {
        return Err(format!("expected 3+ comma-separated fields, got {}", parts.len()));
    }
    let metric = parts[0].trim();
    let ts: i64 = parts[1].trim().parse().map_err(|_| format!("bad ts {:?}", parts[1]))?;
    let value_f: f64 = parts[2].trim().parse().map_err(|_| format!("bad value {:?}", parts[2]))?;
    let value = value_f as i64;
    let mut tags: Vec<(String, String)> = Vec::new();
    if let Some(tag_field) = parts.get(3) {
        for tag in tag_field.split(';') {
            let Some(eq) = tag.find('=') else { continue };
            tags.push((tag[..eq].to_string(), tag[eq + 1..].to_string()));
        }
        tags.sort_by(|a, b| a.0.cmp(&b.0));
    }
    let mut metric_name = Vec::new();
    metric_name.extend_from_slice(metric.as_bytes());
    if !tags.is_empty() {
        metric_name.push(b'{');
        for (i, (k, v)) in tags.iter().enumerate() {
            if i > 0 {
                metric_name.push(b',');
            }
            metric_name.extend_from_slice(k.as_bytes());
            metric_name.extend_from_slice(b"=\"");
            metric_name.extend_from_slice(v.as_bytes());
            metric_name.push(b'"');
        }
        metric_name.push(b'}');
    }
    out.push(ParsedSample { metric_name, timestamp_ms: ts, value });
    Ok(())
}

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("line {line_no}: {message}")]
    Line { line_no: usize, message: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_with_tags() {
        let s = "# header\ncpu,1700000000000,42,host=a;region=us\n";
        let out = parse(s).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric_name, br#"cpu{host="a",region="us"}"#);
        assert_eq!(out[0].timestamp_ms, 1_700_000_000_000);
        assert_eq!(out[0].value, 42);
    }
}
