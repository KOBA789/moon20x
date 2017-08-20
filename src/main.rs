#![feature(conservative_impl_trait)]
#![feature(lookup_host)]

extern crate ws;
extern crate hyper;
extern crate hex;
extern crate crypto;
extern crate byteorder;
extern crate time;
extern crate rand;
extern crate bytes;
extern crate futures;
extern crate tokio_io;
extern crate tokio_proto;
extern crate tokio_core;
extern crate redis_async;

mod session_id;
mod counter;
mod increr;
mod syncer;
mod ws_server;
mod influxdb;
mod acl;
mod acceptor;

use std::env;
use std::thread;
use std::str::FromStr;
use std::net::{SocketAddr,lookup_host};
use futures::Future;
use futures::sync::mpsc::channel;
use tokio_core::reactor::Core;

use syncer::Syncer;
use increr::EventReceiver;
use acl::AclStream;

fn main() {
    let redis_host_str = env::var("REDIS_HOST").unwrap();
    let redis_port_str = env::var("REDIS_PORT").unwrap();
    let redis_port = u16::from_str(&redis_port_str).unwrap();

    let mut redis_addr = lookup_host(&redis_host_str).unwrap().next().unwrap();
    redis_addr.set_port(redis_port);

    let influx_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let acl_stream_fut = AclStream::connect(&redis_addr, handle.clone());
    let syncer_fut = Syncer::connect(&redis_addr, "saikoh", &handle);
    let prepare = acl_stream_fut.join(syncer_fut);

    let (clients_tx, clients_rx) = channel::<EventReceiver>(1000);

    //let influx = influxdb::connect(influx_addr, "saikoh", &handle);
    let influx_tx = influxdb::WriterThread::new(influx_addr, "saikoh").run();
    let handle2 = core.handle();
    handle.spawn(prepare.and_then(move |(acl_stream, syncer)| {
        acceptor::spawn(clients_rx, acl_stream, syncer, handle2)
    }));

    let secret = [0u8; 32]; // FIXME
    thread::spawn(move || {
        ws_server::run_ws_server(clients_tx, influx_tx, secret, "0.0.0.0:3012");
    });

    core.run(::futures::empty::<(), ()>()).unwrap();
}
