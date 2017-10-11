#![feature(conservative_impl_trait)]

extern crate ws;
extern crate hyper;
extern crate hex;
extern crate crypto;
extern crate byteorder;
extern crate time;
extern crate rand;
extern crate bytes;
extern crate futures;
extern crate futures_cpupool;
extern crate tokio_io;
extern crate tokio_proto;
extern crate tokio_core;
extern crate tokio_timer;
extern crate redis_async;
extern crate url;
extern crate regex;
extern crate arrayvec;

mod redis_url;
mod session_data;
mod ws_server;
mod events;
mod config;
mod updater;
mod acl;
mod influxdb;

use std::thread;
use config::Config;
use futures::{Future, Stream, Sink, future};
use futures::sync::mpsc;
use events::Handler;

fn create_button<'a>(
    counter: &'a events::Counter,
    influx_writer: influxdb::IndexedWriter,
) -> (events::MergedEventChanTx, acl::AclTx, impl Future<Item = (), Error = ()> + 'a) {
    let (acl_tx, acl_rx) = acl::create_acl_channel();
    let (tx, rx) = events::create_merged_event_channel();
    let handler = Handler::new(counter, influx_writer);
    (tx, acl_tx, handler.run(rx, acl_rx))
}

// 0: saikoh
// 1: emoi
// 2: itf
// 3: wtc

/*
|count| {
                    let mut buf = Vec::with_capacity(9);
                    buf[0] = idx;
                    buf[1..].as_mut().write_u64::<byteorder::BE>(count as u64).unwrap();
                    sender.send(buf).expect("send count");
                }
*/

fn main() {
    let config = Config::new();

    let influx_writer = influxdb::WriterThread::new(config.influxdb()).run();

    let (client_tx, client_rx) = mpsc::channel(100);

    let listen_addr = config.listen();
    let issuer = session_data::SessionIssuer::new([0u8; 32]);
    let app = ws_server::build_ws_server(client_tx, &issuer);
    let _sender = app.broadcaster();
    thread::spawn(move || {
        let mut core = tokio_core::reactor::Core::new().unwrap();

        let updater_fut = updater::Updater::connect(config.redis(), &core.handle());
        let acl_stream_fut = acl::AclStream::connect(config.redis(), core.handle());

        let preparation = updater_fut.join(acl_stream_fut);

        let counters: Vec<_> = (0u8..4)
            .map(|idx| {
                let counter = events::Counter::new();
                (format!("{}", idx), counter)
            })
            .collect();

        let hd = core.handle();
        let handlers = preparation.and_then(|(updater, acl_stream)| {
            let mut txs = vec![];
            let mut handler_futs = vec![];
            let mut acl_txs = vec![];

            for (idx, &(_, ref counter)) in counters.iter().enumerate() {
                let indexed = influxdb::IndexedWriter::new(idx as u8, influx_writer.clone());
                let (tx, acl_tx, fut) = create_button(counter, indexed);
                txs.push(tx);
                handler_futs.push(fut);
                acl_txs.push(acl_tx);
            }

            hd.clone().spawn(acl_stream.stream().for_each(move |acl| {
                for acl_tx in acl_txs.iter_mut() {
                    acl_tx.start_send(acl.clone()).ok();
                }
                Ok(())
            }));

            let acceptor = events::Acceptor::new(&mut txs).run(hd, client_rx);
            future::join_all(handler_futs)
                .join(acceptor)
                .join(updater.start(&counters))
                .map(|_| ())
        });

        core.run(handlers).unwrap();
        panic!("counter thread exited");
    });
    app.listen(listen_addr).unwrap();
}
