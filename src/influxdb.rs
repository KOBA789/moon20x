use std::net::SocketAddr;
use std::io;
use std::io::Write;

use tokio_core::net::{UdpCodec, UdpSocket, UdpFramed};
use tokio_core::reactor::Handle;
use bytes::BufMut;

use session_id::SessionIdBody;

#[derive(Debug)]
pub struct DataPoint {
    sid_body: SessionIdBody,
    timestamp: u64, // danger: influxdb accepts 64bit signed only
}

impl DataPoint {
    pub fn new(sid: SessionIdBody, timestamp: u64) -> DataPoint {
        DataPoint {
            sid_body: sid,
            timestamp,
        }
    }
}

pub struct WriteCodec {
    measurement: String,
    server_addr: SocketAddr,
}

impl UdpCodec for WriteCodec {
    type In = ();
    type Out = DataPoint;

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> SocketAddr {
        buf.writer()
            .write_fmt(format_args!(
                "{},sid={} event_time={}\n",
                &self.measurement,
                msg.sid_body,
                msg.timestamp
            ))
            .ok();
        self.server_addr
    }

    fn decode(&mut self, _: &SocketAddr, _: &[u8]) -> io::Result<Self::In> {
        Ok(())
    }
}

pub type InfluxWriter = UdpFramed<WriteCodec>;

pub fn connect<A: Into<SocketAddr>, S: ToString>(addr: A, btn_name: S, handle: &Handle) -> InfluxWriter {
    let local_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let measurement = format!("{}_incr", btn_name.to_string());
    let sock = UdpSocket::bind(&local_addr, &handle).unwrap();
    sock.framed(WriteCodec {
        measurement,
        server_addr: addr.into(),
    })
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
