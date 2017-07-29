extern crate ws;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use ws::{listen, Handler, Sender, Result, Message, Handshake, CloseCode, Error};

struct Server<'a> {
    out: Sender,
    count: &'a AtomicUsize,
}

impl<'a> Handler for Server<'a> {

    fn on_open(&mut self, _: Handshake) -> Result<()> {
        Ok(())
    }

    fn on_message(&mut self, msg: Message) -> Result<()> {
        self.count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn on_close(&mut self, code: CloseCode, reason: &str) {
        match code {
            CloseCode::Normal => println!("The client is done with the connection."),
            CloseCode::Away   => println!("The client is leaving the site."),
            CloseCode::Abnormal => println!(
                "Closing handshake failed! Unable to obtain closing status from client."),
            _ => println!("The client encountered an error: {}", reason),
        }
    }

    fn on_error(&mut self, err: Error) {
        println!("The server encountered an error: {:?}", err);
    }

}

fn main() {
    let count = AtomicUsize::new(0);
    listen("127.0.0.1:3012", |out| {
        Server { out: out, count: &count }
    }).unwrap()
}
