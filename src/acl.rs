use std::collections::HashSet;
use std::net::{SocketAddr, ToSocketAddrs};

use futures::{future, Future, Stream, Sink};
use futures::unsync::mpsc;
use tokio_core::reactor::Handle;
use redis_async::client::{PairedConnection, paired_connect, pubsub_connect};
use redis_async::resp::RespValue;

use session_data::SessionId;
use redis_url::RedisUrl;

pub type AclTx = mpsc::Sender<Acl>;
pub type AclRx = mpsc::Receiver<Acl>;

pub fn create_acl_channel() -> (AclTx, AclRx) {
    mpsc::channel(10)
}

type Blacklist = HashSet<SessionId>;

#[derive(Clone, Debug)]
pub struct Acl {
    blacklist: Blacklist,
}

impl Acl {
    pub fn empty() -> Acl {
        Acl { blacklist: HashSet::new() }
    }

    pub fn is_allowed(&self, sid: &SessionId) -> bool {
        !self.blacklist.contains(sid)
    }
}

pub struct AclFetcher {
    conn: PairedConnection,
}

impl AclFetcher {
    pub fn connect<'a>(
        addr: &SocketAddr,
        handle: &Handle,
    ) -> impl Future<Item = AclFetcher, Error = ::redis_async::error::Error> + 'a {
        paired_connect(addr, handle)
            .map(|conn| AclFetcher { conn })
    }

    fn unpack_resp(resp: RespValue) -> HashSet<SessionId> {
        if let RespValue::Array(raw_sids) = resp {
            let sids: HashSet<_> = raw_sids
                .into_iter()
                .filter_map(|sid| {
                    match sid {
                        RespValue::BulkString(sid) => Some(sid),
                        RespValue::SimpleString(sid) => Some(sid.into()),
                        _ => None,
                    }
                })
                .map(|sid_str| SessionId::from_hex(sid_str))
                .filter_map(|sid_opt| sid_opt)
                .collect();
            return sids;
        }
        return HashSet::new();
    }

    pub fn fetch<'a>(&self) -> impl Future<Item = Acl, Error = ()> + 'a {
        self.conn
            .send(vec!["SMEMBERS", "blacklist"])
            .map(AclFetcher::unpack_resp)
            .map(|blacklist| Acl { blacklist })
            .map_err(|_| ())
    }
}

pub struct AclStream {
    blacklist_stream: mpsc::Receiver<()>,
    fetcher: AclFetcher,
}

impl AclStream {
    pub fn connect<'a>(
        url: RedisUrl,
        handle: Handle,
    ) -> impl Future<Item = AclStream, Error = ()> + 'a {
        let addr = url.to_socket_addrs().unwrap().next().unwrap();
        let fetcher_fut = AclFetcher::connect(&addr, &handle);
        pubsub_connect(&addr, &handle)
            .and_then(move |pubsub| pubsub.subscribe("blacklist").join(fetcher_fut))
            .map(|(blacklist_stream, fetcher)| (future::ok(()).into_stream().chain(blacklist_stream.map(|_| ())), fetcher))
            .map(move |(blacklist_stream, fetcher)| {
                let (tx, rx) = mpsc::channel::<()>(10);
                handle.spawn(blacklist_stream.forward(tx.sink_map_err(|e| panic!("{}", e))).then(|_| Ok(())));
                AclStream {
                    blacklist_stream: rx,
                    fetcher,
                }
            })
            .map_err(|_| ())
    }

    pub fn stream<'a>(self) -> impl Stream<Item = Acl, Error = ()> + 'a {
        let fetcher = self.fetcher;
        self.blacklist_stream.and_then(move |_| {
            fetcher.fetch()
        })
    }
}
