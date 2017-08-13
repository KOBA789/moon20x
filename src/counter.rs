use std::sync::atomic::{AtomicUsize, Ordering};
use futures::sink::Sink;
use futures::unsync::mpsc::{channel, Sender, Receiver};

pub struct Counter {
    number: AtomicUsize,
    emitter: Sender<bool>,
}

impl Counter {
    pub fn incr(&mut self) {
        self.emitter.start_send(true).ok();
        self.number.fetch_add(1, Ordering::Relaxed);
    }

    pub fn reset(&self) -> usize {
        self.number.swap(0, Ordering::Relaxed)
    }

    pub fn new() -> (Receiver<bool>, Counter) {
        let (emitter, rx) = channel(1);
        (rx, Counter{ number: AtomicUsize::new(0), emitter })
    }
}
