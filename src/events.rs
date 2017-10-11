use std::sync::atomic::{AtomicUsize, Ordering};
use futures::sync::mpsc;
use futures::unsync::mpsc as unsync_mpsc;
use futures::{Future, Stream, Sink};
use tokio_core::reactor::Handle;
use arrayvec::ArrayVec;

use session_data::SessionId;
use acl;
use influxdb;

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
        let chan = &mut self.0[kind_idx];
        match chan.try_send(event) {
            Ok(_) => Ok(()),
            Err(err) => {
                if err.is_full() {
                    Err(EventChanError::NoCapacity)
                } else {
                    Err(EventChanError::Internal)
                }
            }
        }
    }
}

pub struct Counter {
    diff: AtomicUsize,
    remote: AtomicUsize,
}
impl Counter {
    pub fn incr(&self) {
        self.diff.fetch_add(1, Ordering::Relaxed);
    }

    pub fn reset(&self) -> usize {
        self.diff.swap(0, Ordering::Relaxed)
    }

    pub fn swap_remote(&self, new: usize) -> usize {
        let old = self.remote.swap(new, Ordering::Relaxed);
        if new != old {

        }
        old
    }

    pub fn count(&self) -> usize {
        self.remote.load(Ordering::Relaxed)
    }

    pub fn new() -> Counter {
        Counter {
            diff: AtomicUsize::new(0),
            remote: AtomicUsize::new(0),
        }
    }
}

pub struct Handler<'a> {
    counter: &'a Counter,
    influx_writer: influxdb::IndexedWriter,
}
impl<'a> Handler<'a> {
    pub fn new(counter: &'a Counter, influx_writer: influxdb::IndexedWriter) -> Self {
        Handler {
            counter,
            influx_writer,
        }
    }

    pub fn run(self, merged_events: MergedEventChanRx, acl_rx: acl::AclRx) -> impl Future<Item = (), Error = ()> {
        enum Input {
            Incr(Incr),
            Acl(acl::Acl),
        };
        let mut acl = acl::Acl::empty();
        merged_events.map(Input::Incr)
            .select(acl_rx.map(Input::Acl))
            .for_each(move |event| {
                match event {
                    Input::Incr(incr) => {
                        if acl.is_allowed(&incr.1) {
                            self.counter.incr();
                        }
                        self.influx_writer.send(incr);
                    },
                    Input::Acl(new_acl) => {
                        acl = new_acl;
                    }
                };
                Ok(())
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
