use std::net::SocketAddr;

use futures::Future;
use tokio_core::reactor::Handle;
use redis_async::client::{paired_connect, PairedConnection};

use counter::Counter;

pub struct Syncer {
    name: String,
    conn: PairedConnection,
}

impl Syncer {
    pub fn connect<'a, S>(
        addr: &SocketAddr,
        name: S,
        handle: &Handle,
    ) -> impl Future<Item = Syncer, Error = ()> + 'a
    where
        S: ToString,
    {
        let name = name.to_string();
        paired_connect(addr, handle).map_err(|_| ()).map(|conn| {
            Syncer { conn, name: name }
        })
    }

    pub fn sync(&self, counter: &Counter) {
        let value = counter.reset();
        self.conn.send(vec![
            "INCRBY".to_string(),
            self.name.clone(),
            format!("{}", value),
        ]);
        self.conn.send(vec![
            "PUBLISH".to_string(),
            "counter_updates".to_string(),
            self.name.clone()
        ]);
    }
}
