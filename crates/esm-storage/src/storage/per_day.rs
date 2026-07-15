//! The per-day index registration path of storage.go: hour/next-day
//! metricID caches, `updatePerDateData` and `prefillNextIndexDB`.

use super::*;

/// Fast cache of the metricIDs seen during one hour. Go: hourMetricIDs.
pub(crate) struct HourMetricIds {
    pub m: Set,
    pub hour: u64,
    pub idb_id: u64,
}

/// Cache holding the metricIDs pre-registered for the next day.
/// Go: nextDayMetricIDs.
pub(crate) struct NextDayMetricIds {
    /// The id of the indexDB that stores the next day (date+1) metrics.
    pub idb_id: u64,
    /// The date relatively to which the next day is taken.
    pub date: u64,
    pub metric_ids: Set,
}

impl StorageInner {
    /// During the last `idb_prefill_start_seconds` before the month
    /// rollover, gradually pre-creates the indexes of the active series in
    /// the next month's partition indexDB, so the rollover doesn't cause an
    /// ingestion latency spike. Go: Storage.prefillNextIndexDB.
    pub(super) fn prefill_next_index_db(
        &self,
        rows: &[RawRow],
        row_names: &[&[u8]],
    ) -> Result<(), String> {
        let now_ms = now_unix_milli();
        let next_month_ms = TimeRange::from_partition_timestamp(now_ms).max_timestamp + 1;
        let d = (next_month_ms - now_ms) as f64 / 1000.0;
        if d >= self.idb_prefill_start_seconds as f64 {
            // Fast path: nothing to pre-fill because it is too early.
            return Ok(());
        }

        // Slower path: pre-populate idbNext with the increasing probability
        // until the rotation. The probability increases from 0% to 100%
        // proportionally to d=[idb_prefill_start_seconds .. 0].
        let p_min = d / self.idb_prefill_start_seconds as f64;

        let ptw_next = self.tb.must_get_partition(next_month_ms);
        let idb_next = ptw_next.pt().idb();
        let mut is_next = idb_next.get_index_search(NO_DEADLINE);

        let mut first_error: Option<String> = None;
        let mut mn = MetricName::default();

        // Only prefill the index for samples whose timestamps fall within
        // the last idb_prefill_start_seconds of the current month.
        let tr = TimeRange {
            min_timestamp: next_month_ms - self.idb_prefill_start_seconds * 1000,
            max_timestamp: next_month_ms - 1,
        };
        // Use the first date of the next month for prefilling the index.
        let date = next_month_ms as u64 / MSEC_PER_DAY as u64;

        let mut timeseries_pre_created = 0u64;
        for (r, name) in rows.iter().zip(row_names) {
            if !tr.contains(r.timestamp) {
                continue;
            }

            let p = (fast_hash_uint64(r.tsid.metric_id) as u32) as f64 / (1u64 << 32) as f64;
            if p < p_min {
                // Fast path: it is too early to pre-fill indexes for the
                // given metricID.
                continue;
            }

            // Check whether the given metricID is already present in idbNext.
            if is_next.has_metric_id(r.tsid.metric_id) {
                continue;
            }

            // Slow path: pre-fill the indexes in idbNext.
            if mn.unmarshal_raw(name).is_err() {
                if first_error.is_none() {
                    first_error = Some(format!("cannot unmarshal MetricNameRaw {name:?}"));
                }
                self.invalid_raw_metric_names
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }
            mn.sort_tags();
            idb_next.create_global_indexes(&r.tsid, &mn);
            idb_next.create_per_day_indexes(date, &r.tsid, &mn);
            timeseries_pre_created += 1;
        }
        drop(is_next);
        self.timeseries_pre_created
            .fetch_add(timeseries_pre_created, Ordering::Relaxed);

        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// Registers the `(date, metricID)` entries of the given rows in the
    /// per-day index, unless the hour caches show that they are registered
    /// already. Go: Storage.updatePerDateData.
    pub(super) fn update_per_date_data(
        &self,
        rows: &[RawRow],
        row_names: &[&[u8]],
        hm_prev: &HourMetricIds,
        hm_curr: &HourMetricIds,
    ) -> Result<(), String> {
        if self.env.idb_ctx.disable_per_day_index {
            return Ok(());
        }

        let mut date = 0u64;
        let mut hour = 0u64;
        let mut prev_timestamp = 0i64;
        // These are used for speeding up bulk imports when multiple adjacent
        // rows contain the same (metricID, date) pairs.
        let mut prev_date = 0u64;
        let mut prev_metric_id = 0u64;

        let hm_prev_date = hm_prev.hour / 24;
        let hm_curr_date = hm_curr.hour / 24;
        let next_day_cache = Arc::clone(&self.next_day_metric_ids.read());
        let next_day_idb_id = next_day_cache.idb_id;
        let next_day_date = next_day_cache.date + 1;
        let next_day_metric_ids = &next_day_cache.metric_ids;
        let ts = fasttime::unix_timestamp();
        // Start pre-populating the next per-day inverted index during the
        // last hour of the current day. p_min linearly increases from 0 to 1
        // during the last hour of the day.
        let p_min = (ts % (3600 * 24)) as f64 / 3600.0 - 23.0;
        let current_hour = ts / 3600;
        let first_hour_of_day = is_first_hour_of_day(ts);

        struct PendingDateMetricId<'a> {
            date: u64,
            tsid: Tsid,
            metric_name_raw: &'a [u8],
        }
        let mut pending_date_metric_ids: Vec<PendingDateMetricId<'_>> = Vec::new();
        let mut pending_next_day_metric_ids: Vec<u64> = Vec::new();

        // Batch-local memo of (date, metricID) entries confirmed by the
        // shared dateMetricIDCache. Positive answers are immutable facts
        // (the per-day index entry exists), so they can be reused for the
        // rest of the batch without re-taking the shared cache lock. A
        // batch rarely spans more than a couple of dates, hence the Vec.
        let mut confirmed_date_metric_ids: Vec<(u64, Set)> = Vec::new();

        let mut ptw: Option<Arc<crate::table::PartitionWrapper>> = None;
        for (r, name) in rows.iter().zip(row_names) {
            if r.timestamp != prev_timestamp {
                date = r.timestamp as u64 / MSEC_PER_DAY as u64;
                hour = r.timestamp as u64 / MSEC_PER_HOUR as u64;
                prev_timestamp = r.timestamp;
            }
            let metric_id = r.tsid.metric_id;
            if metric_id == prev_metric_id && date == prev_date {
                // Fast path for bulk import of multiple rows with the same
                // (date, metricID) pairs.
                continue;
            }
            prev_date = date;
            prev_metric_id = metric_id;

            if hm_curr.idb_id == next_day_idb_id && p_min > 0.0 && hour == current_hour {
                // Gradually pre-populate the per-day inverted index for the
                // next day during the last hour of the current day. Do this
                // only when the next day is in the same partition indexDB
                // (the cross-month case is handled by prefillNextIndexDB).
                let p = (fast_hash_uint64(metric_id) as u32) as f64 / (1u64 << 32) as f64;
                if p < p_min && !next_day_metric_ids.has(metric_id) {
                    pending_date_metric_ids.push(PendingDateMetricId {
                        date: date + 1,
                        tsid: r.tsid,
                        metric_name_raw: name,
                    });
                    pending_next_day_metric_ids.push(metric_id);
                }
            }

            if first_hour_of_day && date == next_day_date && next_day_metric_ids.has(metric_id) {
                // Fast path: the metricID has already been added to the
                // per-day index during the next-day prefill.
                continue;
            }

            if date == hm_curr_date && hm_curr.m.has(metric_id) {
                // Fast path: the metricID is in the current hour cache,
                // which means it has already been added to the per-day index.
                continue;
            }

            if date == hm_prev_date && hm_prev.m.has(metric_id) {
                // Fast path: the metricID is already registered for its day
                // on the previous hour.
                continue;
            }

            // Slower path: check the batch-local memo, then the
            // dateMetricIDCache.
            let confirmed = match confirmed_date_metric_ids
                .iter_mut()
                .find(|(d, _)| *d == date)
            {
                Some((_, set)) => set,
                None => {
                    confirmed_date_metric_ids.push((date, Set::default()));
                    &mut confirmed_date_metric_ids.last_mut().unwrap().1
                }
            };
            if confirmed.has(metric_id) {
                continue;
            }
            let switch = match &ptw {
                Some(w) => !w.pt().has_timestamp(r.timestamp),
                None => true,
            };
            if switch {
                ptw = Some(self.tb.must_get_partition(r.timestamp));
            }
            let idb = ptw.as_ref().unwrap().pt().idb();
            if idb.date_metric_id_cache.has(date, metric_id) {
                confirmed.add(metric_id);
                continue;
            }

            // Slow path: store the (date, metricID) entry in the indexDB.
            pending_date_metric_ids.push(PendingDateMetricId {
                date,
                tsid: r.tsid,
                metric_name_raw: name,
            });
        }
        drop(ptw);

        if !pending_next_day_metric_ids.is_empty() {
            let mut pending = self.pending_next_day_metric_ids.lock();
            for metric_id in &pending_next_day_metric_ids {
                pending.add(*metric_id);
            }
        }
        if pending_date_metric_ids.is_empty() {
            // Fast path - there are no new (date, metricID) entries.
            return Ok(());
        }

        // Slow path - add new (date, metricID) entries to the indexDB.
        self.slow_per_day_index_inserts
            .fetch_add(pending_date_metric_ids.len() as u64, Ordering::Relaxed);
        // Sort by (date, metricID) in order to speed up the index search in
        // the loop below.
        pending_date_metric_ids.sort_unstable_by_key(|a| (a.date, a.tsid.metric_id));

        let mut first_error: Option<String> = None;
        let mut mn = MetricName::default();
        let mut k = 0;
        while k < pending_date_metric_ids.len() {
            let timestamp = pending_date_metric_ids[k].date as i64 * MSEC_PER_DAY;
            let ptw = self.tb.must_get_partition(timestamp);
            let idb = ptw.pt().idb();
            let mut is = idb.get_index_search(NO_DEADLINE);
            while k < pending_date_metric_ids.len() {
                let dmid = &pending_date_metric_ids[k];
                let timestamp = dmid.date as i64 * MSEC_PER_DAY;
                if !ptw.pt().has_timestamp(timestamp) {
                    break;
                }
                k += 1;

                if !is.has_date_metric_id(dmid.date, dmid.tsid.metric_id) {
                    // The (date, metricID) entry is missing in the indexDB.
                    // Add it there together with the per-day indexes. It is
                    // OK if the entry is added multiple times by concurrent
                    // threads.
                    if mn.unmarshal_raw(dmid.metric_name_raw).is_err() {
                        if first_error.is_none() {
                            first_error = Some(format!(
                                "cannot unmarshal MetricNameRaw {:?}",
                                dmid.metric_name_raw
                            ));
                        }
                        self.invalid_raw_metric_names
                            .fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    mn.sort_tags();
                    idb.create_per_day_indexes(dmid.date, &dmid.tsid, &mn);
                }
            }
        }

        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    // --- hour/next-day metricID updaters (Go: *MetricIDsUpdater) ---

    pub(super) fn start_curr_hour_metric_ids_updater(self: &Arc<Self>) {
        let inner = Arc::clone(self);
        let handle = std::thread::spawn(move || {
            let d = add_jitter_to_duration(Duration::from_secs(10));
            loop {
                let stopped = inner.shutdown.wait_timeout(d);
                inner.update_curr_hour_metric_ids(fasttime::unix_hour());
                if stopped {
                    return;
                }
            }
        });
        self.threads.lock().push(handle);
    }

    pub(super) fn start_next_day_metric_ids_updater(self: &Arc<Self>) {
        let inner = Arc::clone(self);
        let handle = std::thread::spawn(move || {
            let d = add_jitter_to_duration(Duration::from_secs(11));
            loop {
                let stopped = inner.shutdown.wait_timeout(d);
                inner.update_next_day_metric_ids(fasttime::unix_timestamp());
                if stopped {
                    return;
                }
            }
        });
        self.threads.lock().push(handle);
    }

    /// Go: Storage.updateCurrHourMetricIDs.
    fn update_curr_hour_metric_ids(&self, hour: u64) {
        let hm = Arc::clone(&self.curr_hour_metric_ids.read());
        let new_metric_ids = std::mem::take(&mut *self.pending_hour_entries.lock());

        if new_metric_ids.is_empty() && hm.hour == hour {
            // Fast path: nothing to update.
            return;
        }

        // Slow path: hm.m must be updated with the pending entries.
        let mut idb_id = hm.idb_id;
        let m = if hm.hour == hour {
            let mut m = hm.m.clone();
            m.union(&new_metric_ids);
            m
        } else {
            idb_id = self.tb.must_get_index_db_id_by_hour(hour);
            if hour % 24 == 0 {
                // Do not add pending metricIDs from the previous hour to the
                // current hour on the next day, since this may result in
                // missing registration of the metricIDs in the per-day
                // inverted index.
                Set::default()
            } else {
                new_metric_ids
            }
        };
        *self.curr_hour_metric_ids.write() = Arc::new(HourMetricIds { m, hour, idb_id });
        if hm.hour != hour {
            *self.prev_hour_metric_ids.write() = hm;
        }
    }

    /// Go: Storage.updateNextDayMetricIDs.
    fn update_next_day_metric_ids(&self, timestamp: u64) {
        let date = timestamp / (3600 * 24);
        let next_day_idb_id = self
            .tb
            .must_get_partition((date + 1) as i64 * MSEC_PER_DAY)
            .pt()
            .idb()
            .id();
        let e = Arc::clone(&self.next_day_metric_ids.read());
        let mut pending_metric_ids = std::mem::take(&mut *self.pending_next_day_metric_ids.lock());

        if e.date + 1 == date && is_first_hour_of_day(timestamp) {
            // Do not reset nextDayMetricIDs during the first hour of the
            // next day to speed up the creation of the per-day indexes in
            // update_per_date_data().
            return;
        }

        if pending_metric_ids.is_empty() && e.date == date {
            // Fast path: nothing to update.
            return;
        }

        // Slow path: union the pending metricIDs with e.metric_ids.
        if e.date == date {
            pending_metric_ids.union(&e.metric_ids);
        } else {
            // Do not carry the pending metricIDs from the previous day to
            // the current day, since this may result in missing registration
            // of the metricIDs in the per-day inverted index.
            pending_metric_ids = Set::default();
        }
        *self.next_day_metric_ids.write() = Arc::new(NextDayMetricIds {
            idb_id: next_day_idb_id,
            date,
            metric_ids: pending_metric_ids,
        });
    }
}

/// Go: fastHashUint64.
fn fast_hash_uint64(mut x: u64) -> u64 {
    x ^= x >> 12; // a
    x ^= x << 25; // b
    x ^= x >> 27; // c
    x.wrapping_mul(2685821657736338717)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fast_hash_matches_go_shape() {
        // Deterministic and non-trivial.
        assert_ne!(fast_hash_uint64(1), fast_hash_uint64(2));
        assert_eq!(fast_hash_uint64(42), fast_hash_uint64(42));
    }
}
