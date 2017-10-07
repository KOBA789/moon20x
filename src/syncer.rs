use std::net::{SocketAddr, ToSocketAddrs};

use futures::Future;
use tokio_core::reactor::Handle;
use redis_async::client::{paired_connect, PairedConnection};

use redis_url::RedisUrl;
use events::Counter;

pub struct Syncer {
    conn: PairedConnection,
}

impl Syncer {
    pub fn connect<'a>(
        url: RedisUrl,
        handle: &Handle,
    ) -> impl Future<Item = Syncer, Error = ()> + 'a
    {
        let db = url.db();
        let addr = url.to_socket_addrs().unwrap().next().unwrap();
        println!("{}", addr);
        paired_connect(&addr, handle).and_then(move |conn| {
            println!("redis connected");
            conn.send(vec![
                "SELECT".to_string(),
                format!("{}", db),
            ]).map(|_| {
                Syncer { conn }
            })
        }).map_err(|e| panic!("{}", e))
    }

    pub fn sync(&self, counter: &Counter, name: &str) {
        let value = counter.reset();
        self.conn.send(vec![
            "INCRBY".to_string(),
            name.into(),
            format!("{}", value),
        ]);
        self.conn.send(vec![
            "PUBLISH".to_string(),
            "counter_updates".to_string(),
            name.into()
        ]);
    }
}
