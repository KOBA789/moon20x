use futures::sync::mpsc::{Sender, Receiver, channel};
use futures::Sink;

use session_id::SessionIdBody;

pub type Event = (u64, SessionIdBody);
pub type EventReceiver = Receiver<Event>;

pub struct Increr {
    sink: Sender<(u64, SessionIdBody)>,
}

impl Increr {
    pub fn new() -> (Increr, EventReceiver) {
        let (sink, src) = channel(100);
        (Increr { sink }, src)
    }

    pub fn incr(&mut self, timestamp: u64, sid: SessionIdBody) -> bool {
        self.sink.start_send((timestamp, sid)).map(|_| true).unwrap_or(false)
    }
}
