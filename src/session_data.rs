use hex::{FromHex, ToHex};
use std::ops::Deref;
use crypto::hmac::Hmac;
use crypto::sha2::Sha256;
use crypto::digest::Digest;
use crypto::mac::{Mac, MacResult};
use byteorder::{ByteOrder, BigEndian};
use std::io::Write;
use std::fmt;
use rand::os::OsRng;
use rand::Rng;

fn clone_into_array<A, T>(slice: &[T]) -> A
where
    A: Sized + Default + AsMut<[T]>,
    T: Clone,
{
    let mut a = Default::default();
    <A as AsMut<[T]>>::as_mut(&mut a).clone_from_slice(slice);
    a
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct SessionId([u8; 16]);

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.as_ref().write_hex(f)
    }
}

impl fmt::Debug for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/*
 *
 * +----+----------------------+-------------------------+-------------------------+-------------------------+
 * | 00 | 01 02 03 04 05 06 07 | 08 09 10 11 12 13 14 15 | 16 17 18 19 20 21 22 23 | 24 25 26 27 28 29 30 31 |
 * +----+----------------------+-------------------------+-------------------------+-------------------------+
 * |                                               HMAC-SHA256                                               |
 * +----+----------------------+-------------------------+---------------------------------------------------+
 * | .  |        zero          |        timestamp        |                        id                         |
 * +--\-+----------------------+-------------------------+---------------------------------------------------+
 *     \_version
 *
 * timestamp is represented in BigEndian
 */
pub struct SignedSessionData([u8; 64]);

impl SignedSessionData {
    fn signature_bytes(&self) -> &[u8] {
        &self[..32]
    }

    fn body(&self) -> SessionData {
        SessionData(clone_into_array(&self[32..64]))
    }

    fn signature(&self) -> MacResult {
        MacResult::new(self.signature_bytes())
    }

    pub fn from_hex<T: AsRef<[u8]>>(hex: T) -> Option<SignedSessionData> {
        Vec::from_hex(hex).ok().and_then(|vec| {
            if vec.len() != 64 {
                return None;
            }
            let mut arr = [0u8; 64];
            (&mut arr[..]).write_all(vec.as_slice()).unwrap();
            Some(SignedSessionData(arr))
        })
    }

    pub fn to_string(&self) -> String {
        self.as_ref().to_hex()
    }
}

impl Deref for SignedSessionData {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SessionData([u8; 32]);

impl SessionData {
    fn digest(&self) -> Sha256 {
        let mut sha256 = Sha256::new();
        sha256.input(&self.0);
        sha256
    }

    pub fn version(&self) -> u8 {
        self[0]
    }

    pub fn timestamp_bytes(&self) -> &[u8] {
        &self[8..16]
    }

    pub fn timestamp(&self) -> u64 {
        BigEndian::read_u64(&self.timestamp_bytes())
    }

    pub fn id_bytes(&self) -> &[u8] {
        &self[16..32]
    }

    pub fn id(&self) -> SessionId {
        SessionId(clone_into_array(self.id_bytes()))
    }

    fn new(timestamp: u64, id: [u8; 16]) -> SessionData {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x01; // version
        BigEndian::write_u64(&mut bytes[8..16], timestamp);
        bytes[16..32].as_mut().write_all(&id).unwrap();
        SessionData(bytes)
    }

    pub fn generate(timestamp: u64) -> SessionData {
        let mut rng = OsRng::new().unwrap();
        let mut id = [0u8; 16];
        rng.fill_bytes(&mut id);
        SessionData::new(timestamp, id)
    }
}

impl Deref for SessionData {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for SessionData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.as_ref().write_hex(f)
    }
}

pub struct SessionIssuer {
    secret: [u8; 32],
}

impl SessionIssuer {
    pub fn new(secret: [u8; 32]) -> SessionIssuer {
        SessionIssuer { secret }
    }

    pub fn sign(&self, data: &SessionData) -> SignedSessionData {
        let signature = self.signature(data);
        let signature_bytes = signature.code();
        let mut signed_bytes = [0u8; 64];
        signed_bytes[0..32].as_mut().write_all(signature_bytes).unwrap();
        signed_bytes[32..64].as_mut().write_all(data).unwrap();
        SignedSessionData(signed_bytes)
    }

    fn signature(&self, data: &SessionData) -> MacResult {
        Hmac::new(data.digest(), &self.secret).result()
    }

    pub fn validate(&self, signed: SignedSessionData) -> Option<SessionData> {
        let body = signed.body();
        if self.signature(&body) == signed.signature() {
            return Some(body);
        }
        None
    }
}
