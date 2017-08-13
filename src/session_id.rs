use hex::{FromHex, ToHex};
use std::ops::Deref;
use crypto::hmac::Hmac;
use crypto::sha2::Sha256;
use crypto::digest::Digest;
use crypto::mac::Mac;
use crypto::mac::MacResult;
use byteorder::{ByteOrder, BigEndian};
use std::io::Write;
use std::fmt;
use rand::os::OsRng;
use rand::Rng;
use time;

fn clone_into_array<A, T>(slice: &[T]) -> A
where
    A: Sized + Default + AsMut<[T]>,
    T: Clone,
{
    let mut a = Default::default();
    <A as AsMut<[T]>>::as_mut(&mut a).clone_from_slice(slice);
    a
}

/*
 *
 * +----+----------------------+-------------------------+-------------------------+-------------------------+
 * | 00 | 01 02 03 04 05 06 07 | 08 09 10 11 12 13 14 15 | 16 17 18 19 20 21 22 23 | 24 25 26 27 28 29 30 31 |
 * +----+----------------------+-------------------------+-------------------------+-------------------------+
 * |                                               HMAC-SHA256                                               |
 * +----+----------------------+-------------------------+---------------------------------------------------+
 * | .  |        zero          |        timestamp        |                       salt                        |
 * +--\-+----------------------+-------------------------+---------------------------------------------------+
 *     \_version
 *
 * timestamp is represented in BigEndian
 */
pub struct SessionId([u8; 64]);

impl SessionId {
    fn hash(&self) -> &[u8] {
        &self[..32]
    }

    fn version(&self) -> u8 {
        self[32]
    }

    fn timestamp(&self) -> &[u8] {
        &self[40..48]
    }

    fn salt(&self) -> &[u8] {
        &self[48..64]
    }

    pub fn body(&self) -> SessionIdBody {
        SessionIdBody(clone_into_array(&self[32..64]))
    }

    fn signature(&self) -> MacResult {
        MacResult::new(self.hash())
    }
}

impl Deref for SessionId {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SessionIdBody([u8; 32]);

impl SessionIdBody {
    fn digest(&self) -> Sha256 {
        let mut sha256 = Sha256::new();
        sha256.input(&self.0);
        sha256
    }

    pub fn from_hex<T: AsRef<[u8]>>(hex: T) -> Option<SessionIdBody> {
        Vec::from_hex(hex).ok().and_then(|bytes| {
            if bytes.len() != 32 {
                return None;
            }
            Some(SessionIdBody(clone_into_array(&bytes[32..64])))
        })
    }
}

impl Deref for SessionIdBody {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for SessionIdBody {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.as_ref().write_hex(f)
    }
}

pub struct UnreliableSessionId(SessionId);

impl UnreliableSessionId {
    pub fn from_hex<T: AsRef<[u8]>>(hex: T) -> Option<UnreliableSessionId> {
        Vec::from_hex(hex).ok().and_then(|bytes| {
            if bytes.len() != 64 {
                return None;
            }
            let mut arr = [0u8; 64];
            (&mut arr[..]).write_all(bytes.as_slice()).unwrap();
            Some(UnreliableSessionId(SessionId(arr)))
        })
    }
}

impl Deref for UnreliableSessionId {
    type Target = SessionId;
    fn deref(&self) -> &Self::Target {
        return &self.0;
    }
}

pub struct ValidSessionId(SessionId);

impl ValidSessionId {
    pub fn to_string(&self) -> String {
        self.as_ref().to_hex()
    }
}

impl Deref for ValidSessionId {
    type Target = SessionId;
    fn deref(&self) -> &Self::Target {
        return &self.0;
    }
}

pub struct SessionIssuer {
    secret: [u8; 32],
}

impl SessionIssuer {
    pub fn new(secret: [u8; 32]) -> SessionIssuer {
        SessionIssuer { secret }
    }

    fn signature(&self, digest: Sha256) -> MacResult {
        Hmac::new(digest, &self.secret).result()
    }

    pub fn validate(&self, unreliable: UnreliableSessionId) -> Option<ValidSessionId> {
        if self.signature(unreliable.body().digest()) == unreliable.signature() {
            return Some(ValidSessionId(unreliable.0));
        }
        None
    }

    fn issue_with_opts(&self, timestamp: u64, salt: [u8; 16]) -> ValidSessionId {
        let mut body_bytes = [0u8; 32];
        body_bytes[0] = 0x01; // version
        BigEndian::write_u64(&mut body_bytes[8..16], timestamp);
        body_bytes[16..32].as_mut().write_all(&salt).unwrap();
        let body = SessionIdBody(body_bytes);

        let digest = body.digest();
        let hash = self.signature(digest);
        let mut bytes = [0u8; 64];
        bytes[0..32].as_mut().write_all(hash.code()).unwrap();
        bytes[32..64].as_mut().write_all(&body_bytes).unwrap();

        ValidSessionId(SessionId(bytes))
    }

    pub fn issue(&self, timestamp: u64) -> ValidSessionId {
        let mut rng = OsRng::new().unwrap();
        let mut salt = [0u8; 16];
        rng.fill_bytes(&mut salt);
        self.issue_with_opts(timestamp, salt)
    }

    pub fn issue_now(&self) -> ValidSessionId {
        self.issue(time::precise_time_ns())
    }
}
