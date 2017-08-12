use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time;
use std::net::SocketAddr;

use futures::{Future, Stream};
use futures::unsync::mpsc::Receiver;
use tokio_core::reactor::Handle;
use redis_async::client::{paired_connect, pubsub_connect};

use counter::Counter;

pub struct Syncer {
    addr: SocketAddr,
    name: String,
    counter: Arc<Counter>,
}

impl Syncer {
    pub fn new<A, S>(addr: A, name: S, counter: Arc<Counter>) -> Syncer
    where
        A: Into<SocketAddr>,
        S: ToString,
    {
        Syncer {
            addr: addr.into(),
            name: name.to_string(),
            counter,
        }
    }

    pub fn spawn(&self, src: Receiver<bool>, handle: &Handle) {
        paired_connect(&self.addr, handle).map(|conn| {
            src.map(|_| {
                let value = self.counter.reset();
                conn.send(vec!["INCRBY".to_string(), format!("{}", value)]);
            });
        });
    }
}
