use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::thread;

use redis;
use redis::Commands;

use session_id::ValidSessionId;

struct Acl {
    blacklist: RwLock<HashSet<Vec<u8>>>,
}

impl Acl {
    pub fn is_allowed(&self, sid: ValidSessionId) -> bool {
        let sid_body_hex = sid.body();
        //self.blacklist.read()
        !self.blacklist.contains(sid_body_hex.as_ref())
    }

    pub fn run(&self) {
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let conn = client.get_connection().unwrap();
        let mut pubsub = client.get_pubsub().unwrap();
        loop {
            self.blacklist.insert("".to_owned().as_bytes().to_vec());
        }
    }
}
