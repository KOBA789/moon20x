extern crate ws;
extern crate redis;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::mpsc::TrySendError::Disconnected;
use std::net::ToSocketAddrs;
use std::fmt::Debug;
use std::{thread, time};

use redis::Commands;

struct Syncer {
    name: String,
    counter: Arc<AtomicUsize>,
    src: mpsc::Receiver<bool>,
}

impl Syncer {
    fn new(name: String, counter: Arc<AtomicUsize>, src: mpsc::Receiver<bool>) -> Syncer {
        Syncer { name, counter, src }
    }

    fn run(&self) {
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let conn = client.get_connection().unwrap();
        loop {
            if let Err(e) = self.src.recv() {
                panic!(e);
            }
            let local_value = self.counter.swap(0, Ordering::Relaxed);
            let _: i64 = conn.incr(&self.name, local_value).unwrap();
            thread::sleep(time::Duration::from_millis(10));
        }
    }
}

struct Increr {
    counter: Arc<AtomicUsize>,
    sink: mpsc::SyncSender<bool>,
}

impl Increr {
    fn new(counter: Arc<AtomicUsize>, sink: mpsc::SyncSender<bool>) -> Increr {
        Increr { counter, sink }
    }

    fn incr(&self) {
        self.counter.fetch_add(1, Ordering::Relaxed);
        if let Err(Disconnected(_)) = self.sink.try_send(true) {
            panic!("Sink channel is disconnected");
        }
    }

    fn run<A>(&self, addr: A) where A: ToSocketAddrs + Debug {
        ws::Builder::new()
            .with_settings(ws::Settings {
                max_connections: 10000,
                in_buffer_grow: true,
                ..ws::Settings::default()
            })
            .build(|_| {
                IncomingServer {
                    increr: &self,
                }
            }).unwrap().listen(addr).unwrap();
    }
}

struct Forbidden;

impl ws::Handler for Forbidden {
    fn on_request(&mut self, req: &ws::Request) -> ws::Result<(ws::Response)> {
        let mut res = ws::Response::from_request(req)?;
        res.set_status(401);
        res.set_reason("Forbidden");
        Ok(res)
    }
}

struct IncomingServer<'a> {
    increr: &'a Increr,
}

impl<'a> ws::Handler for IncomingServer<'a> {
    // TODO: authentication/authorization

    fn on_open(&mut self, _: ws::Handshake) -> ws::Result<()> {
        Ok(())
    }

    fn on_message(&mut self, _: ws::Message) -> ws::Result<()> {
        self.increr.incr();
        Ok(())
    }

    fn on_close(&mut self, code: ws::CloseCode, reason: &str) {
        match code {
            ws::CloseCode::Normal => (),
            _ => println!("Closed in: {:?} with {}", code, reason),
        }
    }

    fn on_error(&mut self, err: ws::Error) {
        println!("The server encountered an error: {:?}", err);
    }
}

fn main() {
    let counter = Arc::new(AtomicUsize::new(0));
    let (sink, src) = mpsc::sync_channel::<bool>(1);
    let syncer = Syncer::new("saikoh".to_string(), counter.clone(), src);
    thread::spawn(move|| {
        syncer.run();
    });

    let increr = Increr::new(counter.clone(), sink);
    increr.run("0.0.0.0:3012");
}
