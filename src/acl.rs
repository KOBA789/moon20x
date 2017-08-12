use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{RwLock, Arc};

use futures::{Future, Stream, BoxFuture};
use tokio_core::reactor::Handle;
use redis_async::client::{paired_connect, pubsub_connect};
use redis_async::resp::RespValue;

use session_id::SessionIdBody;

type Blacklist = HashSet<SessionIdBody>;

pub struct Acl {
    blacklist: Blacklist,
}

impl Acl {
    pub fn new() -> Arc<RwLock<Acl>> {
        Arc::new(RwLock::new(Acl { blacklist: HashSet::new() }))
    }
}

pub struct Gatekeeper {
    acl: Arc<RwLock<Acl>>,
}

impl Gatekeeper {
    pub fn is_allowed(&self, sid_body: &SessionIdBody) -> bool {
        self.acl
            .try_read()
            .map(|acl| !acl.blacklist.contains(sid_body))
            .unwrap_or(false)
    }

    pub fn new(acl: Arc<RwLock<Acl>>) -> Gatekeeper {
        Gatekeeper { acl }
    }
}

pub struct AclSyncer {
    addr: SocketAddr,
    acl: Arc<RwLock<Acl>>,
}

impl AclSyncer {
    fn update(&self, new_sids: Vec<SessionIdBody>) {
        while let Err(_) = self.acl.try_write().map(|mut acl| {
            acl.blacklist.clear();
            for sid in new_sids.iter() {
                acl.blacklist.insert(sid.clone());
            }
        })
        { /* spin */ }
    }

    fn unpack_resp(resp: RespValue) -> Vec<SessionIdBody> {
        if let RespValue::Array(raw_sids) = resp {
            let sids: Vec<_> = raw_sids
                .into_iter()
                .filter_map(|sid| {
                    if let RespValue::SimpleString(sid) = sid {
                        return Some(sid);
                    }
                    return None;
                })
                .map(|sid_str| SessionIdBody::from_hex(sid_str))
                .filter_map(|sid_opt| sid_opt)
                .collect();
            return sids;
        }
        return vec![];
    }
    /*
    fn sync(self, handle: &Handle) -> BoxFuture<Self, ()> {
        paired_connect(&self.addr, handle).and_then(move |conn| {
            conn.send(vec!["SMEMBERS", "blacklist"])
                .map(AclSyncer::unpack_resp)
                .map(move |sids| self.update(sids))
        })
    }
*/
    pub fn spawn(self, handle: &Handle) {
        let pubsub = pubsub_connect(&self.addr, handle);
        pubsub
            .and_then(|pubsub| {
                //self.sync(handle);
                pubsub.subscribe("blacklist")
            })
            .map(move |blacklist_stream| {
                blacklist_stream.fold(self, move |me, _| {
                    paired_connect(&me.addr, handle).and_then(move |conn| {
                        conn.send(vec!["SMEMBERS", "blacklist"])
                            .map(AclSyncer::unpack_resp)
                            .map(move |sids| me.update(sids))
                    }).map(|_| me).map_err(|_| ())
                });
            });
    }

    pub fn new<A: Into<SocketAddr>>(addr: A, acl: Arc<RwLock<Acl>>) -> AclSyncer {
        let addr: SocketAddr = addr.into();
        AclSyncer { addr, acl }
    }
}
