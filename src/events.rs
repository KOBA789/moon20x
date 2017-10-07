use std::time;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use futures::sync::mpsc;
use futures::unsync::mpsc as unsync_mpsc;
use futures::{Future, Stream, Sink, AsyncSink};
use tokio_core::reactor::{Handle, Interval};
use arrayvec::ArrayVec;

use syncer::Syncer;
use session_data::SessionId;

const NUM_BUTTON: usize = 4;

pub type Incr = (u64, SessionId);
pub type EventChanTx = mpsc::Sender<Incr>;
pub type EventChanRx = mpsc::Receiver<Incr>;

pub type EventChanBundleTx = [EventChanTx; NUM_BUTTON];
pub type EventChanBundleRx = [EventChanRx; NUM_BUTTON];

pub type ClientChanTx = mpsc::Sender<EventChanBundleRx>;
pub type ClientChanRx = mpsc::Receiver<EventChanBundleRx>;

pub type MergedEventChanTx = unsync_mpsc::Sender<Incr>;
pub type MergedEventChanRx = unsync_mpsc::Receiver<Incr>;

const EVENT_CHAN_SIZE: usize = 10000;
pub fn create_event_bundle_channel() -> (EventChanRouter, EventChanBundleRx) {
    let (tx0, rx0) = mpsc::channel(EVENT_CHAN_SIZE);
    let (tx1, rx1) = mpsc::channel(EVENT_CHAN_SIZE);
    let (tx2, rx2) = mpsc::channel(EVENT_CHAN_SIZE);
    let (tx3, rx3) = mpsc::channel(EVENT_CHAN_SIZE);
    (EventChanRouter([tx0, tx1, tx2, tx3]), [rx0, rx1, rx2, rx3])
}

pub fn create_merged_event_channel() -> (MergedEventChanTx, MergedEventChanRx) {
    unsync_mpsc::channel(1_000_000)
}

pub enum EventChanError {
    NotRouted,
    NoCapacity,
    Internal,
}

pub struct EventChanRouter(EventChanBundleTx);
impl EventChanRouter {
    pub fn send(&mut self, kind_idx: usize, event: Incr) -> Result<(), EventChanError> {
        if kind_idx >= self.0.len() {
            return Err(EventChanError::NotRouted);
        }
        let mut chan = &mut self.0[kind_idx];
        match chan.start_send(event) {
            Ok(AsyncSink::Ready) => Ok(()),
            Ok(AsyncSink::NotReady(_)) => Err(EventChanError::NoCapacity),
            Err(_) => Err(EventChanError::Internal),
        }
    }
}

pub struct Counter(AtomicUsize);
impl Counter {
    pub fn incr(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    pub fn reset(&self) -> usize {
        self.0.swap(0, Ordering::Relaxed)
    }

    pub fn new(init: usize) -> Counter {
        Counter(AtomicUsize::new(init))
    }
}

impl Default for Counter {
    fn default() -> Counter {
        Self::new(0)
    }
}

pub struct Handler {
    name: String,
    counter: Counter,
    syncer: Arc<Syncer>,
}
impl Handler {
    pub fn new(name: &str, syncer: Arc<Syncer>) -> Self {
        Handler {
            name: name.into(),
            counter: Counter::default(),
            syncer,
        }
    }

    pub fn run(self, merged_events: MergedEventChanRx, handle: &Handle) -> impl Future<Item = (), Error = ()> {
        enum Input {
            Incr(Incr),
            Sync,
        };
        let ticks = Interval::new(time::Duration::from_millis(100), handle).unwrap();
        ticks.map_err(|_| ())
            .map(|_| Input::Sync)
            .select(merged_events.map(|e| Input::Incr(e)))
            .fold(self, move |me, event| {
                match event {
                    Input::Incr(event) => {
                        me.counter.incr()
                    },
                    Input::Sync => {
                        me.syncer.sync(&me.counter, &me.name);
                    }
                };
                Ok::<_, ()>(me)
            }).map(|_| ())
    }
}

pub struct Acceptor {
    merged_events_tx: ArrayVec<[MergedEventChanTx; NUM_BUTTON]>,
}

impl Acceptor {
    pub fn new(merged_events_tx_vec: &mut Vec<MergedEventChanTx>) -> Self {
        assert!(merged_events_tx_vec.len() == NUM_BUTTON, "size match");
        let merged_events_tx = merged_events_tx_vec.drain(..).collect();
        Acceptor { merged_events_tx }
    }

    pub fn run(
        self,
        handle: Handle,
        client_rx: ClientChanRx,
    ) -> impl Future<Item = (), Error = ()> {
        client_rx
            .fold(self, move |me, event_chan_bundle| {
                {
                    let bundle_iter = ArrayVec::from(event_chan_bundle).into_iter();
                    let zipped = bundle_iter.zip(me.merged_events_tx.iter());
                    for (src, merged_orig) in zipped {
                        let accept = src.fold(merged_orig.clone(), move |merged, event| {
                            merged.send(event).map_err(|e| panic!("{}", e))
                        });
                        handle.spawn(accept.map(|_| ()));
                    }
                }
                Ok::<_, ()>(me)
            })
            .map(|_| ())
    }
}
