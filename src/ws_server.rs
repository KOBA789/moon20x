use std::net::ToSocketAddrs;
use futures::{Sink, AsyncSink};
use futures::sync::mpsc::{Sender, Receiver};
use time;

use ws;
use hyper;

//use increr::{Increr, EventReceiver, Incr};
use session_data::{SessionIssuer, SessionData, SignedSessionData, SessionId};
use events;

struct IncomingServer<'a> {
    client_tx: events::ClientChanTx,
    event_chan: Option<events::EventChanRouter>,
    issuer: &'a SessionIssuer,
    out: ws::Sender,
    session_data: Option<SessionData>,
}

impl<'a> IncomingServer<'a> {
    fn get_cookie(&self, req: &ws::Request) -> Option<hyper::header::Cookie> {
        use hyper::header::{Header, Cookie};

        let raw_cookie = match req.header("Cookie") {
            Some(raw_cookie) => raw_cookie,
            None => return None,
        };
        Cookie::parse_header(&raw_cookie.clone().into()).ok()
    }

    fn get_session_data(&self, req: &ws::Request) -> Option<SessionData> {
        self.get_cookie(req).and_then(|cookie| {
            cookie
                .get("sid")
                .and_then(SignedSessionData::from_hex)
                .and_then(|u| self.issuer.validate(u))
        })
    }

    fn authenticate(&mut self, req: &ws::Request, res: &mut ws::Response) {
        let given_session_data = self.get_session_data(req);
        let session_data =
            given_session_data.unwrap_or_else(|| SessionData::generate(time::precise_time_ns()));
        let set_cookie = format!(
            "sid={}; Path=/; Expires=Thu, 31 Dec 2037 15:00:00 GMT",
            self.issuer.sign(&session_data).to_string()
        );
        res.headers_mut().push((
            "Set-Cookie".into(),
            set_cookie.as_bytes().to_vec(),
        ));
        self.session_data = Some(session_data);
    }
}

impl<'a> ws::Handler for IncomingServer<'a> {
    fn on_request(&mut self, req: &ws::Request) -> ws::Result<(ws::Response)> {
        let mut res = ws::Response::from_request(req)?;
        self.authenticate(req, &mut res);
        Ok(res)
    }

    fn on_open(&mut self, _: ws::Handshake) -> ws::Result<()> {
        let (tx, rx) = events::create_event_bundle_channel();

        self.event_chan = Some(tx);

        match self.client_tx.start_send(rx) {
            Ok(AsyncSink::Ready) => Ok(()),
            Ok(AsyncSink::NotReady(_)) => Err(ws::Error::new(
                ws::ErrorKind::Capacity,
                "Client queue is full",
            )),
            Err(_) => Err(ws::Error::new(
                ws::ErrorKind::Internal,
                "Failed to send event",
            )),
        }
    }

    fn on_message(&mut self, msg: ws::Message) -> ws::Result<()> {
        if !msg.is_binary() {
            return Ok(());
        }
        let data = msg.into_data();
        if data.len() != 1 {
            return Ok(());
        }
        let kind_idx = data[0];
        let timestamp = time::precise_time_ns();
        let session_id = self.session_data.as_ref().unwrap().id();
        let incr = (timestamp, session_id);
        match self.event_chan.as_mut().unwrap().send(kind_idx as usize, incr) {
            Ok(()) => Ok(()),
            Err(events::EventChanError::NoCapacity) => Err(ws::Error::new(
                ws::ErrorKind::Capacity,
                "Event queue is full",
            )),
            Err(_) => Err(ws::Error::new(
                ws::ErrorKind::Internal,
                "Failed to send event",
            )),
        }
    }

    fn on_close(&mut self, code: ws::CloseCode, reason: &str) {
        match code {
            ws::CloseCode::Normal => (),
            _ => println!("Closed: {:?}: {}", code, reason),
        }
    }

    fn on_error(&mut self, err: ws::Error) {
        println!("Error: {:?}", err);
        //self.out.close(ws::CloseCode::Invalid);
    }
}

pub fn run_ws_server<A: ToSocketAddrs>(client_tx: events::ClientChanTx, secret: [u8; 32], addr: A) {
    let issuer = SessionIssuer::new(secret);
    ws::Builder::new()
        .with_settings(ws::Settings {
            max_connections: 10000,
            in_buffer_capacity: 2048 * 100,
            in_buffer_grow: true,
            ..ws::Settings::default()
        })
        .build(move |out| {
            IncomingServer {
                client_tx: client_tx.clone(),
                event_chan: None,
                issuer: &issuer,
                out,
                session_data: None,
            }
        })
        .unwrap()
        .listen(addr)
        .unwrap();
}
