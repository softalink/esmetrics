//! [`ForwardingSink`]: the `esm_insert::RowSink` that decodes ingested rows,
//! applies the global relabel config, and hands survivors to a
//! [`SeriesConsumer`] (the fan-out seam; implemented in a later task).
//!
//! Mirrors `app/vmagent/remotewrite/remotewrite.go`'s `Push`/`tryPush`:
//! decode -> global relabel -> fan-out.

use std::collections::HashMap;
use std::sync::Arc;

use esm_insert::{MetricRow, RowSink};
use esm_protoparser::prompb::Sample;
use esm_relabel::{Label, ParsedConfigs};
use esm_storage::MetricName;

use crate::series::OwnedSeries;

/// Destination for decoded, relabeled series. The fan-out to remote targets
/// is one implementation of this trait, built in a later task.
pub trait SeriesConsumer: Send + Sync {
    fn push(&self, series: &[OwnedSeries]);
}

/// Decodes `metric_name_raw`, applies `global_relabel`, and forwards
/// surviving series to `consumer`.
pub struct ForwardingSink {
    pub global_relabel: Option<ParsedConfigs>,
    pub consumer: Arc<dyn SeriesConsumer>,
}

impl RowSink for ForwardingSink {
    fn add_rows(&self, rows: &[MetricRow<'_>]) -> Result<(), String> {
        let mut series: Vec<OwnedSeries> = Vec::new();
        // Maps a distinct `metric_name_raw` to its index in `series`, so
        // rows sharing a name (interleaved or not) land in one OwnedSeries
        // and the name is decoded only once.
        let mut index: HashMap<Vec<u8>, usize> = HashMap::new();

        for row in rows {
            let sample = Sample {
                value: row.value,
                timestamp: row.timestamp,
            };
            if let Some(&i) = index.get(row.metric_name_raw) {
                series[i].samples.push(sample);
                continue;
            }
            // Never panic on a malformed name: skip just this row.
            let Ok(labels) = decode_labels(row.metric_name_raw) else {
                continue;
            };
            index.insert(row.metric_name_raw.to_vec(), series.len());
            series.push(OwnedSeries {
                labels,
                samples: vec![sample],
            });
        }

        push_series(&self.global_relabel, &self.consumer, series);
        Ok(())
    }
}

/// Applies `global_relabel` (if any) to `series`, dropping any series it
/// rejects, then hands the survivors to `consumer`. Shared seam: both
/// [`ForwardingSink::add_rows`] (pushed data) and the scrape engine
/// (`crate::scrape::scrapework`, scraped data) route through this same
/// global-relabel -> fan-out path.
pub fn push_series(
    global_relabel: &Option<ParsedConfigs>,
    consumer: &Arc<dyn SeriesConsumer>,
    mut series: Vec<OwnedSeries>,
) {
    if let Some(gr) = global_relabel {
        series.retain_mut(|s| gr.apply(&mut s.labels));
    }
    consumer.push(&series);
}

/// Decodes `metric_name_raw` into a relabel-ready label set: `__name__` from
/// `metric_group`, plus one [`Label`] per tag (bytes -> UTF-8 lossily,
/// matching VM labels being UTF-8 in practice).
fn decode_labels(metric_name_raw: &[u8]) -> Result<Vec<Label>, String> {
    let mut mn = MetricName::default();
    mn.unmarshal_raw(metric_name_raw)?;
    let mut labels = Vec::with_capacity(mn.tags.len() + 1);
    labels.push(Label {
        name: "__name__".to_string(),
        value: String::from_utf8_lossy(&mn.metric_group).into_owned(),
    });
    for tag in &mn.tags {
        labels.push(Label {
            name: String::from_utf8_lossy(&tag.key).into_owned(),
            value: String::from_utf8_lossy(&tag.value).into_owned(),
        });
    }
    Ok(labels)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sink_decodes_relabels_and_forwards() {
        use std::sync::Mutex;
        struct Cap(Mutex<Vec<OwnedSeries>>);
        impl SeriesConsumer for Cap {
            fn push(&self, s: &[OwnedSeries]) {
                self.0.lock().unwrap().extend_from_slice(s);
            }
        }
        let cap = Arc::new(Cap(Mutex::new(vec![])));
        // global relabel: drop temp_* metrics
        let gr = ParsedConfigs::parse(
            "- source_labels: [__name__]\n  regex: \"temp_.*\"\n  action: drop\n",
        )
        .unwrap();
        let sink = ForwardingSink {
            global_relabel: Some(gr),
            consumer: cap.clone(),
        };
        // build two rows via esm_storage::marshal_metric_name_raw
        let mut n1 = Vec::new();
        esm_storage::marshal_metric_name_raw(&mut n1, &[(b"", b"up"), (b"job", b"x")]);
        let mut n2 = Vec::new();
        esm_storage::marshal_metric_name_raw(&mut n2, &[(b"", b"temp_cpu")]);
        sink.add_rows(&[
            esm_insert::MetricRow {
                metric_name_raw: &n1,
                timestamp: 1000,
                value: 1.0,
            },
            esm_insert::MetricRow {
                metric_name_raw: &n2,
                timestamp: 1000,
                value: 2.0,
            },
        ])
        .unwrap();
        let got = cap.0.lock().unwrap();
        assert_eq!(got.len(), 1); // temp_cpu dropped
        assert!(got[0]
            .labels
            .iter()
            .any(|l| l.name == "__name__" && l.value == "up"));
        assert!(got[0]
            .labels
            .iter()
            .any(|l| l.name == "job" && l.value == "x"));
        assert_eq!(got[0].samples[0].value, 1.0);
    }
}
