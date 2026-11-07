use prometheus::{local::LocalHistogram, Histogram};
use std::cell::RefCell;

struct Recorder {
    histogram: LocalHistogram,
    pending: usize,
}
impl Drop for Recorder {
    fn drop(&mut self) {
        self.histogram.flush();
    }
}
pub(crate) struct Timer<'a> {
    histogram: &'a Histogram,
    started: std::time::Instant,
}
impl<'a> Timer<'a> {
    pub fn new(histogram: &'a Histogram) -> Self {
        Self {
            histogram,
            started: std::time::Instant::now(),
        }
    }
}
impl Drop for Timer<'_> {
    fn drop(&mut self) {
        observe(self.histogram, self.started.elapsed().as_secs_f64());
    }
}
thread_local! { static RECORDER: RefCell<Option<Recorder>> = const { RefCell::new(None) }; }

pub(crate) fn initialize(histogram: &Histogram) {
    RECORDER.with(|slot| {
        *slot.borrow_mut() = Some(Recorder {
            histogram: histogram.local(),
            pending: 0,
        })
    });
}
pub(crate) fn observe(histogram: &Histogram, seconds: f64) {
    RECORDER.with(|slot| {
        let mut slot = slot.borrow_mut();
        if let Some(recorder) = slot.as_mut() {
            recorder.histogram.observe(seconds);
            recorder.pending += 1;
            if recorder.pending >= 64 {
                recorder.histogram.flush();
                recorder.pending = 0;
            }
        } else {
            histogram.observe(seconds);
        }
    });
}
pub(crate) fn flush() {
    RECORDER.with(|slot| {
        if let Some(recorder) = slot.borrow_mut().as_mut() {
            recorder.histogram.flush();
            recorder.pending = 0;
        }
    });
}
