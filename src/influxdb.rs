use std::net::SocketAddr;
use std::io::Write;
use std::net;
use std::sync::mpsc::{sync_channel, SyncSender, TryRecvError};
use std::thread;

use increr::Incr;

pub struct WriterThread {
    addr: SocketAddr,
    btn_name: String,
}

const CHAN_CAP: usize = 100;

impl WriterThread {
    pub fn new<A: Into<SocketAddr>, S: ToString>(addr: A, btn_name: S) -> WriterThread {
        WriterThread { addr: addr.into(), btn_name: btn_name.to_string() }
    }

    pub fn run(self) -> SyncSender<Incr> {
        let (tx, rx) = sync_channel(CHAN_CAP);
        thread::spawn(move|| {
            let local_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let udp = net::UdpSocket::bind(local_addr).unwrap();
            let measurement = format!("{}_incr", self.btn_name);
            let mut recv_buf: Vec<Incr> = Vec::with_capacity(CHAN_CAP);
            let mut buf = Vec::with_capacity(110 * CHAN_CAP);
            loop {
                let mut num_mes = 0;
                recv_buf.clear();
                recv_buf.push(rx.recv().expect("recv first datapoint"));
                loop {
                    match rx.try_recv() {
                        Ok(incr) => {
                            recv_buf.push(incr);
                            num_mes += 1;
                            if num_mes >= CHAN_CAP {
                                break;
                            }
                        },
                        Err(TryRecvError::Empty) => break,
                        Err(e) => panic!(e),
                    }
                }
                buf.clear();
                for pair in recv_buf.iter() {
                    buf.write_fmt(format_args!("{},sid={} event_time={}\n", measurement, pair.1.clone(), pair.0.clone())).expect("write datapoints");
                }
                udp.send_to(&buf, self.addr).expect("send datapoints");
            }
        });
        tx
    }
}

#[test]
fn it_works() {
    use tokio_core::reactor::Core;
    use futures::Sink;
    use std::str::FromStr;
    use session_id::UnreliableSessionId;

    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let addr = SocketAddr::from_str("127.0.0.1:8080").unwrap();
    let mut influx = connect(addr, "saikoh", &handle);
    let sid = UnreliableSessionId::from_hex("1bb13b8dcfcfe4805549776f5674a131d84847cb44c41f9bc89eb80dcebc4c7d010000000000000000079114fcaa7737da2886e144b88dd657c9bbd2fdad7e95").unwrap();
    core.run(influx.send(DataPoint {
        sid_body: sid.body(),
        timestamp: 123,
    })).unwrap();
}
