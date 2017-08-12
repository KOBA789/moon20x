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
mod increr;
mod syncer;
mod ws_server;
mod influxdb;
mod acl;

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc;
use std::thread;

use increr::Increr;
use syncer::Syncer;

fn main() {
    let counter = Arc::new(AtomicUsize::new(0));
    let (sink, src) = mpsc::sync_channel::<bool>(1);
    let syncer = Syncer::new("saikoh".to_string(), counter.clone(), src);
    thread::spawn(move || { syncer.run(); });

    let secret = [0u8; 32]; // FIXME
    let increr = Increr::new(counter.clone(), sink);
    ws_server::run_ws_server(increr, secret, "0.0.0.0:3012");
}
