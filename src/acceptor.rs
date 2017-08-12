use std::sync::mpsc::sync_channel;
use std::sync::Arc;
use std::ops::Deref;

use futures::{Sink, Stream, Future, future};
use futures::unsync::mpsc::{channel, Sender, Receiver};
use futures::sync;
use tokio_core::reactor::Handle;

use counter::Counter;
use increr::EventReceiver;
use influxdb::{InfluxWriter, DataPoint};
use acl::Gatekeeper;
use session_id::SessionIdBody;

pub struct Acceptor {
    pub counter: Arc<Counter>,
    gatekeeper: Gatekeeper,
    influx: InfluxWriter,
}

impl Acceptor {
    pub fn new(influx: InfluxWriter, gatekeeper: Gatekeeper) -> Acceptor {
        let acceptor = Acceptor {
            counter: Arc::new(Counter::new()),
            gatekeeper,
            influx,
        };

        acceptor
    }

    pub fn spawn(self, handle: &Handle, handle2: Handle) -> (Arc<Counter>, Receiver<bool>, sync::mpsc::Sender<EventReceiver>) {
        let (counter_tx, counter_rx) = channel(1);
        let (influx_tx, influx_rx) = channel::<DataPoint>(100);
        let (recv_tx, recv_rx) = sync::mpsc::channel::<EventReceiver>(100);

        let flow = influx_rx.fold(self.influx, |influx, dp| {
            influx.send(dp).map_err(|_| ())
        }).and_then(|_| Ok(()));
        handle.spawn(flow);

        let gk = Arc::new(self.gatekeeper);
        let counter = self.counter.clone();
        let flow = recv_rx.fold(handle2, move |handle, recv| {
            let counter = counter.clone();
            let counter_tx = counter_tx.clone();
            let gk = gk.clone();
            let flow = recv.filter_map(move |(timestamp, sid)| {
                if gk.is_allowed(&sid) {
                    Some(DataPoint::new(sid, timestamp))
                } else {
                    None
                }
            }).fold(influx_tx.clone(), move |influx, dp| {
                counter.clone().incr();
                counter_tx.clone().start_send(true);
                influx.send(dp).map_err(|_| ())
            }).and_then(|_| Ok(()));
            handle.spawn(flow);
            future::ok::<Handle, ()>(handle)
        });
        handle.spawn(flow.and_then(|_| Ok(())));

        (self.counter.clone(), counter_rx, recv_tx)
    }

    /*
    pub fn increr(&self) -> Increr {

        //let counter_tx = self.counter_tx.clone();
        let gk = self.gatekeeper.clone();
        let influx = self.influx_tx.clone();
        let (tx, rx) = channel::<(u64, SessionIdBody)>(100);
        let counter = self.counter.clone();
        let flow = rx.filter_map(move |(timestamp, sid)| {
                if gk.is_allowed(&sid) {
                    Some(DataPoint::new(sid, timestamp))
                } else {
                    None
                }
            })
            .fold(influx, move |influx, dp| {
                counter.incr();
                //counter_tx.start_send(true);
                influx.send(dp).map_err(|_| ())
            });
        self.handle.spawn(flow.and_then(|_| Ok(())));
        Increr::new(tx)
    }
    */
}