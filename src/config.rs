use std::env;
use std::net::SocketAddr;
use redis_url::RedisUrl;

pub struct Config {
    listen: SocketAddr,
    redis: RedisUrl,
    influxdb: SocketAddr,
}

impl Config {
    pub fn new() -> Config {
        Config {
            listen: env::var("LISTEN_ADDR")
                .unwrap_or("0.0.0.0:8124".to_string())
                .parse()
                .unwrap(),
            redis: env::var("REDIS_URL")
                .unwrap_or("redis://127.0.0.1".to_string())
                .parse()
                .unwrap(),
            influxdb: env::var("INFLUXDB_ADDR")
                .unwrap_or("127.0.0.1:8080".to_string())
                .parse()
                .unwrap(),
        }
    }

    pub fn listen(&self) -> SocketAddr {
        self.listen.clone()
    }

    pub fn redis(&self) -> RedisUrl {
        self.redis.clone()
    }

    pub fn influxdb(&self) -> SocketAddr {
        self.influxdb.clone()
    }
}
