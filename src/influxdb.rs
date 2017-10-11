use std::net::SocketAddr;
use std::io::Write;
use std::net;
use std::sync::mpsc::{channel, Sender, TryRecvError};
use std::thread;
use std::time;

use time::precise_time_ns;

use events::Incr;

pub type IndexedIncr = (u8, Incr);
pub type InfluxWriter = Sender<IndexedIncr>;

#[derive(Clone)]
pub struct IndexedWriter {
    idx: u8,
    writer: InfluxWriter,
}

impl IndexedWriter {
    pub fn new(idx: u8, writer: InfluxWriter) -> Self {
        Self { idx, writer }
    }

    pub fn send(&self, incr: Incr) {
        self.writer.send((self.idx, incr)).ok();
    }
}

pub struct WriterThread {
    addr: SocketAddr,
}

const BATCH_TIMEOUT: i64 = 1000;
const BATCH_SIZE: usize = 500;
const DP_SIZE: usize = 90;

fn time_ms() -> i64 {
    (precise_time_ns() / 1_000_000) as i64
}

impl WriterThread {
    pub fn new(addr: SocketAddr) -> WriterThread {
        WriterThread { addr: addr.into() }
    }

    pub fn run(self) -> InfluxWriter {
        let (tx, rx) = channel::<IndexedIncr>();
        thread::spawn(move|| {
            let local_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
            let udp = net::UdpSocket::bind(local_addr).unwrap();
            let mut buf = Vec::with_capacity(DP_SIZE * BATCH_SIZE);
            loop {
                let start_time = time_ms();
                let mut num_mes = 0;
                loop {
                    match rx.try_recv() {
                        Ok((btn_idx, (ref timestamp, ref sid))) => {
                            buf.write_fmt(
                                format_args!(
                                    "incr_{} sid=\"{}\",event_time={} {}\n",
                                    btn_idx, sid, timestamp, timestamp)
                                ).expect("write datapoints");
                            num_mes += 1;
                            if num_mes >= BATCH_SIZE {
                                break;
                            }
                        },
                        Err(TryRecvError::Empty) => {
                            let now = time_ms();
                            let diff = BATCH_TIMEOUT - (now - start_time);
                            if diff <= 0 {
                                break;
                            }
                            let timeout = time::Duration::from_millis(diff as u64);
                            thread::sleep(timeout);
                        }
                        Err(e) => panic!(e),
                    }
                }
                if num_mes == 0 {
                    continue;
                }
                udp.send_to(&buf, self.addr).expect("send datapoints");
                buf.clear();
            }
            panic!("influxdb writer exited");
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
