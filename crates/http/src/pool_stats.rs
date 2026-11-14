use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::Instant;

/// One slot per configured client worker, shared across compatible generations.
pub struct PoolStats {
    slots: Vec<Slot>,
}
#[derive(Default)]
struct Slot {
    tracked_open: AtomicU64,
    busy: AtomicU64,
    attempts: AtomicU64,
    succeeded: AtomicU64,
    errors: AtomicU64,
    timeouts: AtomicU64,
    cancelled: AtomicU64,
    connecting: AtomicU64,
    opened: AtomicU64,
    closed: AtomicU64,
    open: AtomicU64,
    connect_micros: AtomicU64,
    tls_attempts: AtomicU64,
    tls_success: AtomicU64,
    tls_micros: AtomicU64,
    first_responses: AtomicU64,
    reused_responses: AtomicU64,
}
#[derive(Default, Clone, Copy, Debug)]
pub struct PoolSnapshot {
    pub tracked_open: u64,
    pub busy: u64,
    pub attempts: u64,
    pub succeeded: u64,
    pub errors: u64,
    pub timeouts: u64,
    pub cancelled: u64,
    pub connecting: u64,
    pub opened: u64,
    pub closed: u64,
    pub open: u64,
    pub connect_micros: u64,
    pub tls_attempts: u64,
    pub tls_success: u64,
    pub tls_micros: u64,
    pub first_responses: u64,
    pub reused_responses: u64,
}
impl Default for PoolStats {
    fn default() -> Self {
        Self::new(1)
    }
}
impl PoolStats {
    pub fn new(workers: usize) -> Self {
        Self {
            slots: (0..workers.clamp(1, 32)).map(|_| Slot::default()).collect(),
        }
    }
    fn slot(&self, worker: usize) -> &Slot {
        &self.slots[worker % self.slots.len()]
    }
    pub fn snapshot(&self) -> PoolSnapshot {
        let mut total = PoolSnapshot::default();
        for s in &self.slots {
            macro_rules! add { ($($field:ident),*) => { $(total.$field = total.$field.saturating_add(s.$field.load(Ordering::Relaxed));)* }; }
            add!(
                tracked_open,
                busy,
                attempts,
                succeeded,
                errors,
                timeouts,
                cancelled,
                connecting,
                opened,
                closed,
                open,
                connect_micros,
                tls_attempts,
                tls_success,
                tls_micros,
                first_responses,
                reused_responses
            );
        }
        total.busy = total.busy.min(total.tracked_open);
        total
    }
    pub(crate) fn attempt(self: &Arc<Self>, worker: usize, tls: bool) -> Attempt {
        let slot = self.slot(worker);
        slot.attempts.fetch_add(1, Ordering::Relaxed);
        slot.connecting.fetch_add(1, Ordering::Relaxed);
        if tls {
            slot.tls_attempts.fetch_add(1, Ordering::Relaxed);
        }
        Attempt {
            stats: self.clone(),
            worker,
            tls,
            start: Instant::now(),
            outcome: 3,
        }
    }
}
pub(crate) struct Attempt {
    stats: Arc<PoolStats>,
    worker: usize,
    tls: bool,
    start: Instant,
    outcome: usize,
}
impl Attempt {
    pub fn error(&mut self, timeout: bool) {
        self.outcome = if timeout { 2 } else { 1 };
    }
    pub fn connected(
        &mut self,
        reuse: bool,
        tracked: bool,
    ) -> (SocketLease, Option<ConnectionTag>) {
        self.outcome = 0;
        let s = self.stats.slot(self.worker);
        s.succeeded.fetch_add(1, Ordering::Relaxed);
        s.opened.fetch_add(1, Ordering::Relaxed);
        s.open.fetch_add(1, Ordering::Relaxed);
        if tracked {
            s.tracked_open.fetch_add(1, Ordering::Relaxed);
        }
        if self.tls {
            s.tls_success.fetch_add(1, Ordering::Relaxed);
        }
        let tag = reuse.then(|| {
            ConnectionTag(Arc::new(Usage {
                stats: self.stats.clone(),
                worker: self.worker,
                responses: AtomicU64::new(0),
                activity: Mutex::new(ActivityState {
                    open: true,
                    active: 0,
                }),
            }))
        });
        (
            SocketLease {
                tracked,
                usage: tag.clone(),
                stats: self.stats.clone(),
                worker: self.worker,
            },
            tag,
        )
    }
}
impl Drop for Attempt {
    fn drop(&mut self) {
        let s = self.stats.slot(self.worker);
        s.connecting.fetch_sub(1, Ordering::Relaxed);
        let elapsed = self.start.elapsed().as_micros().min(u64::MAX as u128) as u64;
        s.connect_micros.fetch_add(elapsed, Ordering::Relaxed);
        if self.tls {
            s.tls_micros.fetch_add(elapsed, Ordering::Relaxed);
        }
        match self.outcome {
            1 => &s.errors,
            2 => &s.timeouts,
            3 => &s.cancelled,
            _ => return,
        }
        .fetch_add(1, Ordering::Relaxed);
    }
}
pub(crate) struct SocketLease {
    tracked: bool,
    usage: Option<ConnectionTag>,
    stats: Arc<PoolStats>,
    worker: usize,
}
impl Drop for SocketLease {
    fn drop(&mut self) {
        if let Some(tag) = &self.usage {
            let mut state = tag.0.activity.lock().unwrap();
            state.open = false;
            if state.active > 0 {
                tag.0
                    .stats
                    .slot(tag.0.worker)
                    .busy
                    .fetch_sub(1, Ordering::Relaxed);
            }
        }
        let s = self.stats.slot(self.worker);
        s.closed.fetch_add(1, Ordering::Relaxed);
        s.open.fetch_sub(1, Ordering::Relaxed);
        if self.tracked {
            s.tracked_open.fetch_sub(1, Ordering::Relaxed);
        }
    }
}
#[derive(Clone)]
pub(crate) struct ConnectionTag(Arc<Usage>);
struct Usage {
    activity: Mutex<ActivityState>,
    stats: Arc<PoolStats>,
    worker: usize,
    responses: AtomicU64,
}
struct ActivityState {
    open: bool,
    active: u64,
}
pub(crate) struct Activity(ConnectionTag);
impl Drop for Activity {
    fn drop(&mut self) {
        let mut state = self.0 .0.activity.lock().unwrap();
        state.active -= 1;
        if state.active == 0 && state.open {
            self.0
                 .0
                .stats
                .slot(self.0 .0.worker)
                .busy
                .fetch_sub(1, Ordering::Relaxed);
        }
    }
}
impl ConnectionTag {
    pub fn activate(&self) -> Arc<Activity> {
        let mut state = self.0.activity.lock().unwrap();
        if state.active == 0 && state.open {
            self.0
                .stats
                .slot(self.0.worker)
                .busy
                .fetch_add(1, Ordering::Relaxed);
        }
        state.active += 1;
        Arc::new(Activity(self.clone()))
    }
    pub fn response(&self) {
        let s = self.0.stats.slot(self.0.worker);
        if self.0.responses.fetch_add(1, Ordering::Relaxed) == 0 {
            s.first_responses.fetch_add(1, Ordering::Relaxed);
        } else {
            s.reused_responses.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn attempts_cancel_fail_and_transports_close_independently_of_tags() {
        let stats = Arc::new(PoolStats::new(2));
        let pending = stats.attempt(0, true);
        assert_eq!(stats.snapshot().connecting, 1);
        drop(pending);
        let mut failed = stats.attempt(1, false);
        failed.error(false);
        drop(failed);
        let mut timed = stats.attempt(0, true);
        timed.error(true);
        drop(timed);
        let mut good = stats.attempt(1, true);
        let (lease, tag) = good.connected(true, false);
        drop(good);
        let tag = tag.unwrap();
        tag.response();
        tag.clone().response();
        drop(lease);
        let s = stats.snapshot();
        assert_eq!(
            (
                s.attempts,
                s.connecting,
                s.succeeded,
                s.errors,
                s.timeouts,
                s.cancelled
            ),
            (4, 0, 1, 1, 1, 1)
        );
        assert_eq!((s.opened, s.closed, s.open), (1, 1, 0));
        assert_eq!((s.tls_attempts, s.tls_success), (3, 1));
        assert_eq!((s.first_responses, s.reused_responses), (1, 1));
        // Response metadata may outlive the transport without holding it open.
        drop(tag);
        assert_eq!(stats.snapshot().closed, 1);
    }
}
