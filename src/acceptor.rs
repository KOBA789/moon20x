use std::time::Duration;

use futures::{Sink, Stream, Future, future};
use futures::unsync::mpsc::channel;
use futures::sync;
use tokio_core::reactor::{Handle, Timeout};

use counter::Counter;
use increr::{EventReceiver, Incr};
use influxdb::{InfluxWriter, DataPoint};
use syncer::Syncer;
use acl::{AclStream, Acl};

enum Event {
    CounterSync,
    AclUpdate(Acl),
    Incr(Incr),
}

pub fn spawn<'a>(
    clients_rx: sync::mpsc::Receiver<EventReceiver>,
    acl_stream: AclStream,
    syncer: Syncer,
    influx: InfluxWriter,
    handle: Handle,
) -> impl Future<Item = (), Error = ()> + 'a {
    let handle_for_clients = handle.clone();
    let handle_outer = handle.clone();
    let handle_for_timer = handle.clone();

    let (raw_updates, mut counter) = Counter::new();
    let (merged_incr_tx, merged_incr_rx) = channel::<Incr>(100);

    handle_outer.spawn(
        clients_rx
            .map(move |recv| {
                // 各クライアント用に書き込みチャネルを複製
                let merged_tx = merged_incr_tx.clone();
                recv.forward(merged_tx.sink_map_err(|e| println!("{}", e)))
                    .and_then(|_| Ok(()))
            })
            .for_each(move |flow| {
                handle_for_clients.spawn(flow);
                Ok(())
            }),
    );

    let acl_update_events = acl_stream.stream().map(Event::AclUpdate);
    let acl = Acl::empty();

    let reduced_sync_events = raw_updates
        .and_then(move |_| {
            Timeout::new(Duration::from_millis(100), &handle_for_timer).unwrap().map_err(|_| ())
        })
        .map(|_| Event::CounterSync);

    let merged_incr_events = merged_incr_rx.map(Event::Incr);

    merged_incr_events
        .select(reduced_sync_events)
        .select(acl_update_events)
        .fold((influx, acl), move |(mut influx, acl), event| match event {
            Event::Incr((ts, sid)) => {
                if acl.is_allowed(&sid) {
                    counter.incr();
                }
                //influx.start_send(DataPoint::new(sid, ts)).ok();
                future::ok((influx, acl))
            }
            Event::CounterSync => {
                syncer.sync(&counter);
                future::ok((influx, acl))
            }
            Event::AclUpdate(acl) => {
                future::ok((influx, acl))
            }
        })
        .and_then(|_| Ok(()))
}
