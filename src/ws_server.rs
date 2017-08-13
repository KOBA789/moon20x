use std::net::ToSocketAddrs;
use std::sync::mpsc::SyncSender;
use futures::sync::mpsc::Sender;
use futures::Sink;
use time;

use ws;
use hyper;

use increr::{Increr, EventReceiver, Incr};
use session_id::{SessionIssuer, UnreliableSessionId, ValidSessionId};

struct IncomingServer<'a> {
    increr: Increr,
    influx: SyncSender<Incr>,
    issuer: &'a SessionIssuer,
    out: ws::Sender,
    session_id: Option<ValidSessionId>,
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

    fn get_session_id(&self, req: &ws::Request) -> Option<ValidSessionId> {
        self.get_cookie(req).and_then(|cookie| {
            cookie
                .get("sid")
                .and_then(UnreliableSessionId::from_hex)
                .and_then(|u| self.issuer.validate(u))
        })
    }

    fn authenticate(&mut self, req: &ws::Request, res: &mut ws::Response) {
        let given_session_id = self.get_session_id(req);
        let session_id = given_session_id.unwrap_or_else(|| self.issuer.issue_now());
        let set_cookie = format!(
            "sid={}; Path=/; Expires=Thu, 31 Dec 2037 15:00:00 GMT",
            &session_id.to_string()
        );
        res.headers_mut().push((
            "Set-Cookie".into(),
            set_cookie.as_bytes().to_vec(),
        ));
        self.session_id = Some(session_id);
    }
}

impl<'a> ws::Handler for IncomingServer<'a> {
    fn on_request(&mut self, req: &ws::Request) -> ws::Result<(ws::Response)> {
        let mut res = ws::Response::from_request(req)?;
        self.authenticate(req, &mut res);
        Ok(res)
    }

    fn on_open(&mut self, _: ws::Handshake) -> ws::Result<()> {
        Ok(())
    }

    fn on_message(&mut self, _: ws::Message) -> ws::Result<()> {
        let incr = (time::precise_time_ns(), self.session_id.as_ref().unwrap().body());
        self.increr.incr(incr.clone());
        self.influx.send(incr).unwrap();
        Ok(())
    }

    fn on_close(&mut self, code: ws::CloseCode, reason: &str) {
        match code {
            ws::CloseCode::Normal => (),
            _ => println!("Closed: {:?}: {}", code, reason),
        }
    }

    fn on_error(&mut self, err: ws::Error) {
        println!("Error: {:?}", err);
    }
}

pub fn run_ws_server<A: ToSocketAddrs>(incr_tx: Sender<EventReceiver>, influx: SyncSender<Incr>, secret: [u8; 32], addr: A) {
    let issuer = SessionIssuer::new(secret);
    ws::Builder::new()
        .with_settings(ws::Settings {
            max_connections: 10000,
            in_buffer_grow: true,
            ..ws::Settings::default()
        })
        .build(move |out| {
            let (increr, receiver) = Increr::new();
            // 一気に接続が来るとここの unwrap で死ぬ
            incr_tx.clone().start_send(receiver).unwrap();
            IncomingServer {
                increr: increr,
                influx: influx.clone(),
                issuer: &issuer,
                out,
                session_id: None,
            }
        })
        .unwrap()
        .listen(addr)
        .unwrap();
}
