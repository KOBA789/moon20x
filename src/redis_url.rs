use std::str::FromStr;
use std::net::ToSocketAddrs;
use std::io;
use url::{self, Url, SocketAddrs};
use regex::Regex;

#[derive(Clone)]
pub struct RedisUrl {
    inner: Url,
    db: usize,
}

#[derive(Debug)]
pub enum ParseError {
    InvalidScheme,
    InvalidPath,
    InvalidDbNumber,
    Url(url::ParseError),
}

impl From<url::ParseError> for ParseError {
    fn from(original_error: url::ParseError) -> Self {
        ParseError::Url(original_error)
    }
}

impl RedisUrl {
    pub fn parse_db_str(input: &str) -> Result<usize, ParseError> {
        if input.len() == 0 {
            return Ok(0);
        }
        let re = Regex::new(r"^[1-9][0-9]*$").unwrap();
        if re.is_match(input) {
            return input.parse::<usize>().map_err(
                |_| ParseError::InvalidDbNumber,
            );
        }
        Err(ParseError::InvalidDbNumber)
    }

    pub fn parse(input: &str) -> Result<RedisUrl, ParseError> {
        let inner = Url::parse(input)?;
        let db = {
            if inner.scheme() != "redis" {
                return Err(ParseError::InvalidScheme);
            }
            let mut path_segments = match inner.path_segments() {
                Some(s) => s,
                None => return Err(ParseError::InvalidPath),
            };
            // Result<Option<usize>, ParseError>
            let first = path_segments.next();
            let second = path_segments.next();
            match (first, second) {
                (None, _) => Ok(0),
                (Some(db_str), None) => RedisUrl::parse_db_str(db_str),
                // Only one segment is allowed
                (Some(_), Some(_)) => Err(ParseError::InvalidPath),
            }?
        };

        Ok(RedisUrl { inner, db })
    }

    pub fn db(&self) -> usize {
        self.db
    }

    pub fn set_db(&mut self, db: usize) {
        self.db = db;
    }

    fn default_port(url: &Url) -> Result<u16, ()> {
        match url.scheme() {
            "redis" => Ok(6379),
            _ => Err(()),
        }
    }
}

impl ::std::ops::Deref for RedisUrl {
    type Target = Url;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl ToSocketAddrs for RedisUrl {
    type Iter = SocketAddrs;

    fn to_socket_addrs(&self) -> io::Result<Self::Iter> {
        self.inner
            .with_default_port(RedisUrl::default_port)?
            .to_socket_addrs()
    }
}

impl FromStr for RedisUrl {
    type Err = ParseError;
    fn from_str(s: &str) -> Result<RedisUrl, ParseError> {
        RedisUrl::parse(s)
    }
}
