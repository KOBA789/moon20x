use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time;

use redis;
use redis::Commands;

pub struct Syncer {
    name: String,
    counter: Arc<AtomicUsize>,
    src: mpsc::Receiver<bool>,
}

impl Syncer {
    pub fn new(name: String, counter: Arc<AtomicUsize>, src: mpsc::Receiver<bool>) -> Syncer {
        Syncer { name, counter, src }
    }

    pub fn run(&self) {
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let conn = client.get_connection().unwrap();
        let mut pubsub = client.get_pubsub().unwrap();
        loop {
            self.src.recv().unwrap();
            let local_value = self.counter.swap(0, Ordering::Relaxed);
            let new_value: i64 = conn.incr(&self.name, local_value).unwrap();
            redis::cmd("PUBLISH")
                .arg(&self.name)
                .arg(new_value)
                .execute(&conn);
            thread::sleep(time::Duration::from_millis(10));
        }
    }
}