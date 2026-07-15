//! Process-wide persistent worker pool for the per-series rollup fan-out.
//!
//! Replaces the per-evaluation `std::thread::scope` spawning: the pool is
//! created once (lazily) with `default_max_workers()` threads and serves all
//! queries. Each rollup evaluation submits a *job* consisting of
//! `num_slots` work slots (one per logical worker id); pool workers and the
//! submitting thread race to claim slots, so single-slot jobs never pay a
//! context switch and idle pool workers steal slots from concurrent queries
//! without cross-query interference (per-job state is fully isolated).
//!
//! Nested-eval rule: work submitted from a pool worker thread runs inline on
//! the current thread (the Go analog caps concurrency instead); this makes
//! re-entrant evaluation (binary ops evaluating args, future subqueries)
//! deadlock-free by construction.

use crate::eval::default_max_workers;
use parking_lot::{Condvar, Mutex};
use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

/// One submitted evaluation: `num_slots` slots executed by running the
/// caller-provided closure with each slot id in `0..num_slots` exactly once.
struct Job {
    /// Lifetime-erased pointer to the submitting thread's closure.
    ///
    /// SAFETY invariant: the submitting thread blocks in [`run_slots`] until
    /// `completed == num_slots`, and the closure is only ever dereferenced
    /// between a successful slot claim (`claim() < num_slots`) and the
    /// matching `completed` increment, so the referent strictly outlives
    /// every dereference. Slots claimed after exhaustion never dereference.
    task: *const (dyn Fn(usize) + Sync),
    num_slots: usize,
    next_slot: AtomicUsize,
    state: Mutex<JobState>,
    done: Condvar,
}

struct JobState {
    completed: usize,
    panicked: bool,
}

// SAFETY: `task` is only dereferenced under the invariant documented on the
// field; all other fields are Send + Sync.
unsafe impl Send for Job {}
unsafe impl Sync for Job {}

impl Job {
    /// Claims the next slot; ids >= `num_slots` mean the job is exhausted.
    fn claim(&self) -> usize {
        self.next_slot.fetch_add(1, Ordering::Relaxed)
    }

    /// Runs the closure for `slot` and records completion (and panics).
    fn run_slot(&self, slot: usize) {
        // SAFETY: see the invariant on `task`.
        let task = unsafe { &*self.task };
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| task(slot)));
        let mut state = self.state.lock();
        state.completed += 1;
        if result.is_err() {
            state.panicked = true;
        }
        if state.completed == self.num_slots {
            self.done.notify_all();
        }
    }
}

struct Pool {
    queue: Mutex<VecDeque<Arc<Job>>>,
    work_available: Condvar,
}

impl Pool {
    /// Claims one slot from the front job, rotating multi-slot jobs to the
    /// back of the queue. Round-robin service shares the pool workers fairly
    /// across concurrent queries: front-only service lets the front job
    /// monopolize the pool, leaving queued queries to run almost serially on
    /// their submitting threads (observed as 2.4x-median tail latency under
    /// concurrent double-groupby load, vs Go's fair goroutine scheduling at
    /// ~1.2x).
    fn try_claim(queue: &mut VecDeque<Arc<Job>>) -> Option<(Arc<Job>, usize)> {
        loop {
            let job = queue.front()?;
            let slot = job.claim();
            if slot >= job.num_slots {
                // Exhausted job (its remaining work is finishing on other
                // threads); drop it and look for the next one.
                queue.pop_front();
                continue;
            }
            let job = queue.pop_front().expect("BUG: front job vanished");
            if slot + 1 < job.num_slots {
                queue.push_back(Arc::clone(&job));
            }
            return Some((job, slot));
        }
    }

    fn worker_loop(&self) {
        IS_POOL_WORKER.set(true);
        loop {
            let (job, slot) = {
                let mut queue = self.queue.lock();
                loop {
                    if let Some(claimed) = Self::try_claim(&mut queue) {
                        break claimed;
                    }
                    self.work_available.wait(&mut queue);
                }
            };
            job.run_slot(slot);
        }
    }
}

thread_local! {
    static IS_POOL_WORKER: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn pool() -> &'static Pool {
    static POOL: OnceLock<&'static Pool> = OnceLock::new();
    POOL.get_or_init(|| {
        let pool: &'static Pool = Box::leak(Box::new(Pool {
            queue: Mutex::new(VecDeque::new()),
            work_available: Condvar::new(),
        }));
        for i in 0..default_max_workers() {
            std::thread::Builder::new()
                .name(format!("promql-eval-{i}"))
                .spawn(move || pool.worker_loop())
                .expect("failed to spawn promql eval worker");
        }
        pool
    })
}

/// Runs `task(slot)` exactly once for every slot in `0..num_slots`,
/// distributing slots over the persistent pool; the calling thread
/// participates in the work, so it never merely blocks on a single slot.
///
/// Calls from a pool worker thread (nested evaluation) run all slots inline
/// on the current thread — worker-id semantics are preserved and deadlocks
/// are impossible.
///
/// Panics if any slot panics (after all slots have completed).
pub(crate) fn run_slots(num_slots: usize, task: &(dyn Fn(usize) + Sync)) {
    if num_slots == 0 {
        return;
    }
    if num_slots == 1 || IS_POOL_WORKER.get() {
        for slot in 0..num_slots {
            task(slot);
        }
        return;
    }

    let pool = pool();
    // SAFETY: erases the borrow lifetime only; the invariant documented on
    // `Job::task` (this function blocks until all slots completed) keeps
    // every dereference within the borrow.
    let task_erased: &'static (dyn Fn(usize) + Sync) =
        unsafe { std::mem::transmute::<&(dyn Fn(usize) + Sync), _>(task) };
    let job = Arc::new(Job {
        task: task_erased as *const (dyn Fn(usize) + Sync),
        num_slots,
        next_slot: AtomicUsize::new(0),
        state: Mutex::new(JobState {
            completed: 0,
            panicked: false,
        }),
        done: Condvar::new(),
    });
    {
        pool.queue.lock().push_back(Arc::clone(&job));
    }
    pool.work_available.notify_all();

    // The submitting thread steals slots from its own job.
    loop {
        let slot = job.claim();
        if slot >= num_slots {
            break;
        }
        job.run_slot(slot);
    }

    // Wait for slots claimed by pool workers.
    let mut state = job.state.lock();
    while state.completed < num_slots {
        job.done.wait(&mut state);
    }
    let panicked = state.panicked;
    drop(state);
    if panicked {
        panic!("worker thread panicked during rollup evaluation");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    #[test]
    fn runs_every_slot_exactly_once() {
        for num_slots in [1usize, 2, 3, 7, 64] {
            let counters: Vec<AtomicUsize> = (0..num_slots).map(|_| AtomicUsize::new(0)).collect();
            run_slots(num_slots, &|slot| {
                counters[slot].fetch_add(1, Ordering::SeqCst);
            });
            for (slot, c) in counters.iter().enumerate() {
                assert_eq!(c.load(Ordering::SeqCst), 1, "slot {slot} of {num_slots}");
            }
        }
    }

    #[test]
    fn nested_submission_does_not_deadlock() {
        // Depth-2 nesting from within pool-executed slots; inner jobs must
        // run inline on pool workers instead of waiting for a free worker.
        let total = AtomicU64::new(0);
        let workers = default_max_workers().max(2);
        run_slots(workers * 4, &|_outer| {
            run_slots(workers * 4, &|_inner| {
                run_slots(2, &|_| {
                    total.fetch_add(1, Ordering::SeqCst);
                });
            });
        });
        assert_eq!(
            total.load(Ordering::SeqCst),
            (workers * 4 * workers * 4 * 2) as u64
        );
    }

    #[test]
    fn concurrent_jobs_do_not_interfere() {
        let handles: Vec<_> = (0..8)
            .map(|_| {
                std::thread::spawn(|| {
                    let sum = AtomicU64::new(0);
                    run_slots(16, &|slot| {
                        sum.fetch_add(slot as u64 + 1, Ordering::SeqCst);
                    });
                    assert_eq!(sum.load(Ordering::SeqCst), (1..=16).sum::<u64>());
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn slot_panic_propagates_to_submitter() {
        let result = std::panic::catch_unwind(|| {
            run_slots(4, &|slot| {
                if slot == 2 {
                    panic!("boom");
                }
            });
        });
        assert!(result.is_err());
        // The pool must remain usable afterwards.
        let n = AtomicUsize::new(0);
        run_slots(4, &|_| {
            n.fetch_add(1, Ordering::SeqCst);
        });
        assert_eq!(n.load(Ordering::SeqCst), 4);
    }
}
