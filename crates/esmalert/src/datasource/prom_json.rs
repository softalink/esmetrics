//! Prometheus HTTP API JSON response parsing. Port of
//! `app/vmalert/datasource/client_prom.go:26-172` (`promResponse` +
//! `promInstant`/`promRange`/`promScalar` `.metrics()`), using `serde_json`
//! instead of `fastjson`.
//!
//! <https://prometheus.io/docs/prometheus/latest/querying/api/#instant-queries>

use serde::Deserialize;
use serde_json::Value;

use super::{DsError, Metric, QueryResult};

const STATUS_SUCCESS: &str = "success";
const STATUS_ERROR: &str = "error";
const RESULT_TYPE_VECTOR: &str = "vector";
const RESULT_TYPE_MATRIX: &str = "matrix";
const RESULT_TYPE_SCALAR: &str = "scalar";

#[derive(Deserialize)]
struct PromResponse {
    status: String,
    #[serde(default, rename = "errorType")]
    error_type: String,
    #[serde(default)]
    error: String,
    #[serde(default)]
    data: PromData,
    #[serde(default, rename = "isPartial")]
    is_partial: Option<bool>,
}

#[derive(Deserialize, Default)]
struct PromData {
    #[serde(default, rename = "resultType")]
    result_type: String,
    #[serde(default)]
    result: Value,
}

/// Parses a Prometheus HTTP API JSON response body. Handles `resultType`
/// `vector`/`matrix`/`scalar`; a `"status":"error"` body (or any status
/// other than `"success"`) becomes `Err`. Never panics — malformed JSON,
/// missing fields, and non-numeric sample values all surface as `DsError`.
pub fn parse_prom_response(body: &[u8]) -> Result<QueryResult, DsError> {
    let resp: PromResponse = serde_json::from_slice(body)
        .map_err(|e| DsError::new(format!("failed to decode response: {e}")))?;
    if resp.status == STATUS_ERROR {
        return Err(DsError::new(format!(
            "response error {:?}: {}",
            resp.error_type, resp.error
        )));
    }
    if resp.status != STATUS_SUCCESS {
        return Err(DsError::new(format!(
            "unknown response status {:?}",
            resp.status
        )));
    }
    let data = match resp.data.result_type.as_str() {
        RESULT_TYPE_VECTOR => parse_vector(&resp.data.result)?,
        RESULT_TYPE_MATRIX => parse_matrix(&resp.data.result)?,
        RESULT_TYPE_SCALAR => parse_scalar(&resp.data.result)?,
        other => return Err(DsError::new(format!("unknown result type {other:?}"))),
    };
    Ok(QueryResult {
        data,
        is_partial: resp.is_partial,
    })
}

fn parse_vector(result: &Value) -> Result<Vec<Metric>, DsError> {
    let arr = result
        .as_array()
        .ok_or_else(|| DsError::new("vector result is not an array"))?;
    arr.iter()
        .map(|item| {
            let labels = parse_labels(item.get("metric"))?;
            let value = item
                .get("value")
                .ok_or_else(|| DsError::new(format!("missing `value` field in {item}")))?;
            let (ts, v) = parse_sample(value)?;
            Ok(Metric {
                labels,
                timestamps: vec![ts],
                values: vec![v],
            })
        })
        .collect()
}

fn parse_matrix(result: &Value) -> Result<Vec<Metric>, DsError> {
    let arr = result
        .as_array()
        .ok_or_else(|| DsError::new("matrix result is not an array"))?;
    arr.iter()
        .map(|item| {
            let labels = parse_labels(item.get("metric"))?;
            let values = item
                .get("values")
                .and_then(Value::as_array)
                .ok_or_else(|| DsError::new(format!("missing `values` array in {item}")))?;
            let mut timestamps = Vec::with_capacity(values.len());
            let mut vals = Vec::with_capacity(values.len());
            for sample in values {
                let (ts, v) = parse_sample(sample)?;
                timestamps.push(ts);
                vals.push(v);
            }
            if vals.is_empty() {
                return Err(DsError::new(format!("metric {item} contains no values")));
            }
            Ok(Metric {
                labels,
                timestamps,
                values: vals,
            })
        })
        .collect()
}

fn parse_scalar(result: &Value) -> Result<Vec<Metric>, DsError> {
    let (ts, v) = parse_sample(result)?;
    Ok(vec![Metric {
        labels: Vec::new(),
        timestamps: vec![ts],
        values: vec![v],
    }])
}

fn parse_labels(metric: Option<&Value>) -> Result<Vec<(String, String)>, DsError> {
    let obj = metric
        .and_then(Value::as_object)
        .ok_or_else(|| DsError::new("missing `metric` object"))?;
    obj.iter()
        .map(|(k, v)| {
            let val = v
                .as_str()
                .ok_or_else(|| DsError::new(format!("label {k:?} value is not a string")))?;
            Ok((k.clone(), val.to_string()))
        })
        .collect()
}

/// Parses a `[timestamp, "value"]` sample pair. The value is always a JSON
/// string per the Prometheus API (e.g. `"1"`), parsed to `f64` here.
fn parse_sample(value: &Value) -> Result<(i64, f64), DsError> {
    let arr = value
        .as_array()
        .ok_or_else(|| DsError::new(format!("sample {value} is not an array")))?;
    if arr.len() != 2 {
        return Err(DsError::new(format!(
            "sample {value} should contain 2 values, got {}",
            arr.len()
        )));
    }
    let ts = arr[0]
        .as_f64()
        .ok_or_else(|| DsError::new(format!("sample timestamp {} is not a number", arr[0])))?
        as i64;
    let val_str = arr[1]
        .as_str()
        .ok_or_else(|| DsError::new(format!("sample value {} is not a string", arr[1])))?;
    let v: f64 = val_str
        .parse()
        .map_err(|e| DsError::new(format!("cannot parse float64 from {val_str:?}: {e}")))?;
    Ok((ts, v))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_vector_response() {
        let body = br#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","instance":"h1"},"value":[1700000000,"1"]}]}}"#;
        let r = parse_prom_response(body).unwrap();
        assert_eq!(r.data.len(), 1);
        assert_eq!(r.data[0].values, vec![1.0]);
        assert!(r.data[0]
            .labels
            .iter()
            .any(|(k, v)| k == "instance" && v == "h1"));
    }

    #[test]
    fn surfaces_error_status() {
        let body = br#"{"status":"error","errorType":"bad_data","error":"parse error"}"#;
        assert!(parse_prom_response(body).is_err());
    }

    #[test]
    fn parses_matrix_response() {
        let body = br#"{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"h1"},"values":[[1000,"1"],[1001,"2"]]}]}}"#;
        let r = parse_prom_response(body).unwrap();
        assert_eq!(r.data.len(), 1);
        assert_eq!(r.data[0].timestamps, vec![1000, 1001]);
        assert_eq!(r.data[0].values, vec![1.0, 2.0]);
    }

    #[test]
    fn parses_scalar_response() {
        let body =
            br#"{"status":"success","data":{"resultType":"scalar","result":[1700000000,"42"]}}"#;
        let r = parse_prom_response(body).unwrap();
        assert_eq!(r.data.len(), 1);
        assert!(r.data[0].labels.is_empty());
        assert_eq!(r.data[0].values, vec![42.0]);
    }

    #[test]
    fn is_partial_flag_is_propagated() {
        let body =
            br#"{"status":"success","isPartial":true,"data":{"resultType":"vector","result":[]}}"#;
        let r = parse_prom_response(body).unwrap();
        assert_eq!(r.is_partial, Some(true));
        assert!(r.data.is_empty());
    }

    #[test]
    fn unknown_result_type_is_an_error() {
        let body = br#"{"status":"success","data":{"resultType":"weird","result":[]}}"#;
        assert!(parse_prom_response(body).is_err());
    }

    #[test]
    fn non_numeric_sample_value_is_an_error_not_a_panic() {
        let body = br#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1700000000,"not-a-number"]}]}}"#;
        assert!(parse_prom_response(body).is_err());
    }

    #[test]
    fn malformed_json_is_an_error_not_a_panic() {
        let body = b"{not json";
        assert!(parse_prom_response(body).is_err());
    }

    #[test]
    fn unknown_status_is_an_error() {
        let body = br#"{"status":"weird"}"#;
        assert!(parse_prom_response(body).is_err());
    }
}
