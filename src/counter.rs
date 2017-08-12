use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct Counter {
    number: AtomicUsize,
}

impl Counter {
    pub fn incr(&self) {
        self.number.fetch_add(1, Ordering::Relaxed);
    }

    pub fn reset(&self) -> usize {
        self.number.swap(0, Ordering::Relaxed)
    }

    pub fn new() -> Counter {
        Counter{ number: AtomicUsize::new(0) }
    }
}
