#![feature(conservative_impl_trait)]

extern crate ws;
extern crate redis;
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

use std::ops::Deref;
use std::thread;
use std::net::SocketAddr;
use tokio_core::reactor::Core;

use increr::Increr;
use syncer::Syncer;

fn main() {
    let redis_addr: SocketAddr = "127.0.0.1:6397".parse().unwrap();
    let influx_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let handle2 = core.handle();

    let acl = acl::Acl::new();
    let acl_syncer = acl::AclSyncer::new(redis_addr, acl.clone());
    let gatekeeper = acl::Gatekeeper::new(acl.clone());

    let influx = influxdb::connect(influx_addr, "saikoh", &handle);
    let acceptor = acceptor::Acceptor::new(influx, gatekeeper);
    let (counter, counter_rx, incr_tx) = acceptor.spawn(&handle, handle2);
    let syncer = Syncer::new(redis_addr, "saikoh", counter);

    acl_syncer.spawn(&handle);
    syncer.spawn(counter_rx, &handle);

    let secret = [0u8; 32]; // FIXME
    thread::spawn(move || {
        ws_server::run_ws_server(incr_tx, secret, "0.0.0.0:3012");
    });

    core.run(::futures::empty::<(), ()>()).unwrap();
}
