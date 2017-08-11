use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::mpsc::TrySendError::Disconnected;

pub struct Increr {
    counter: Arc<AtomicUsize>,
    sink: mpsc::SyncSender<bool>,
}

impl Increr {
    pub fn new(counter: Arc<AtomicUsize>, sink: mpsc::SyncSender<bool>) -> Increr {
        Increr { counter, sink }
    }

    pub fn incr(&self) {
        self.counter.fetch_add(1, Ordering::Relaxed);
        if let Err(Disconnected(_)) = self.sink.try_send(true) {
            panic!("Sink channel is disconnected");
        }
    }
}
