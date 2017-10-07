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
//extern crate tokio_timer;
extern crate redis_async;
extern crate url;
extern crate regex;
extern crate arrayvec;

mod redis_url;
mod session_data;
mod ws_server;
mod events;
mod config;
mod syncer;

use config::Config;
use futures::{Future, Stream, Sink, future};
use futures::future::Executor;
use futures::sync::mpsc::{self, Sender, Receiver};
use futures_cpupool::CpuPool;
use events::Handler;
use std::sync::Arc;
//use redis_url::RedisUrl;
//use std::net::ToSocketAddrs;

fn main() {
    let config = Config::new();

    let mut core = tokio_core::reactor::Core::new().unwrap();
    let pool = CpuPool::new(4);

    //let (update_tx, update_rx) = mpsc::channel(100);
    let (client_tx, client_rx) = mpsc::channel(100);

    let handler_hd = core.handle();
    let acceptor_hd = core.handle();
    let handlers = syncer::Syncer::connect(config.redis(), &core.handle()).and_then(|syncer| {
        let syncer = Arc::new(syncer);
        let handler0 = Handler::new("saikoh", syncer.clone());
        let handler1 = Handler::new("emoi", syncer.clone());
        let handler2 = Handler::new("itf", syncer.clone());
        let handler3 = Handler::new("wtc", syncer.clone());

        let (tx0, rx0) = events::create_merged_event_channel();
        let (tx1, rx1) = events::create_merged_event_channel();
        let (tx2, rx2) = events::create_merged_event_channel();
        let (tx3, rx3) = events::create_merged_event_channel();

        let acceptor = events::Acceptor::new(&mut vec![tx0, tx1, tx2, tx3]).run(acceptor_hd, client_rx);

        future::join_all(vec![
            handler0.run(rx0, &handler_hd.clone()),
            handler1.run(rx1, &handler_hd.clone()),
            handler2.run(rx2, &handler_hd.clone()),
            handler3.run(rx3, &handler_hd.clone()),
        ]).join(acceptor).map(|_| ())
    });

    let listen_addr = config.listen();
    let ws_serve = pool.spawn_fn(move || {
        ws_server::run_ws_server(client_tx, [0u8; 32], listen_addr);
        panic!("Websocket Server is down");
        Ok::<(), ()>(())
    });

    let app = ws_serve.join(handlers);

    core.run(app).unwrap();
}
