use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub const NOMINAL_FLOWS: u64 = 4;

pub struct AdmissionController {
    active: Arc<AtomicU64>,
    max: u64,
}

impl AdmissionController {
    pub fn new(max: u64) -> Self {
        Self {
            active: Arc::new(AtomicU64::new(0)),
            max,
        }
    }
    pub fn nominal() -> Self {
        Self::new(NOMINAL_FLOWS)
    }
    pub fn try_admit(&self) -> Option<AdmissionGuard> {
        let cur = self.active.fetch_add(1, Ordering::SeqCst);
        if cur < self.max {
            Some(AdmissionGuard {
                counter: Arc::clone(&self.active),
                terminal: false,
            })
        } else {
            self.active.fetch_sub(1, Ordering::SeqCst);
            None
        }
    }
    pub fn active_count(&self) -> u64 {
        self.active.load(Ordering::SeqCst)
    }
}

pub struct AdmissionGuard {
    counter: Arc<AtomicU64>,
    terminal: bool,
}
impl AdmissionGuard {
    /// Release only after matching terminal evidence. By default even an aborted
    /// broker future retains occupancy: Drop by itself is not stop evidence.
    pub fn release_terminal(mut self) {
        self.terminal = true;
    }
}
impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        if self.terminal {
            self.counter.fetch_sub(1, Ordering::SeqCst);
        }
    }
}
