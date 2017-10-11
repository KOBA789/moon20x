use std::net::ToSocketAddrs;

use futures::{Future, Stream, future};
use future::{Either, Loop};
use tokio_core::reactor::Handle;
use tokio_timer::Timer;
use std::time;
use redis_async::client::{paired_connect, PairedConnection};
use redis_async::resp::RespValue;

use redis_url::RedisUrl;
use events::Counter;

pub struct Updater {
    conn: PairedConnection,
}

impl Updater {
    pub fn connect<'a>(
        url: RedisUrl,
        handle: &Handle,
    ) -> impl Future<Item = Self, Error = ()> + 'a
    {
        let db = url.db();
        let addr = url.to_socket_addrs().unwrap().next().unwrap();
        paired_connect(&addr, handle).and_then(move |conn| {
            println!("redis connected");
            conn.send(vec![
                "SELECT".to_string(),
                format!("{}", db),
            ]).map(|_| {
                Updater { conn }
            })
        }).map_err(|e| panic!("{}", e))
    }

    pub fn sync<'a>(self, counter: &'a Counter, idx_str: &'a str) -> impl Future<Item = Self, Error = ()> + 'a {
        let value = counter.reset();
        let is_zero = value == 0;
        self.conn.send(vec![
            "INCRBY".to_string(),
            idx_str.into(),
            format!("{}", value),
        ]).and_then(move |resp| {
            if let RespValue::Integer(new) = resp {
                counter.swap_remote(new);
                if !is_zero {
                    return Either::A(self.conn.send(vec![
                        "PUBLISH".to_string(),
                        "counter_updates".to_string(),
                        idx_str.into()
                    ]).map(|_| self));
                }
            };
            Either::B(future::ok(self))
        }).map_err(|_| ())
    }

    pub fn start<'a>(self, counters: &'a [(String, Counter)]) -> impl Future<Item = (), Error = ()> + 'a {
        let timer = Timer::default();
        let ticks = timer.interval(time::Duration::from_millis(110)).map_err(|_| ());
        ticks.fold(self, move |me, _| {
            let iter = counters.iter();
            future::loop_fn((iter, me), |(mut iter, me)| {
                if let Some(&(ref idx_str, ref counter)) = iter.next() {
                    Either::A(me.sync(counter, idx_str).map(move |me| Loop::Continue((iter, me))))
                } else {
                    Either::B(future::ok(Loop::Break((iter, me))))
                }
            }).map(|(_iter, me)| me)
        }).then(|_| Ok(()))
    }
}
