extern crate redis;

pub use redis::{Client as RedisClient};
use redis::{Script as RedisScript};

const LUA_SCRIPT: &'static str = include_str!("limiter.lua");

pub trait Limiter {
    fn get_token_count<'a>(&self, key: &'a str) -> Option<u32>;
    fn consume<'a>(
        &self,
        key: &'a str,
        interval: u32,
        capacity: u32,
        n: Option<u32>,
    ) -> bool;
}

pub struct RedisLimiter {
    redis_cli: RedisClient,
}

impl RedisLimiter {
    pub fn from(redis_cli: RedisClient) -> Self {
        RedisLimiter{ redis_cli }
    }
    pub fn from_config<'a>(host: &'a str, port: u32, db: u32) -> Self {
        let url = format!("redis://{}:{}/{}", host, port, db);
        let client = RedisClient::open(url.as_str()).unwrap();
        Self::from(client)
    }
    fn get_redis_key<'a>(self, key: &'a str, interval: u32) -> String {
        format!("limiter:{}:{}", key, interval)
    }
}

impl Limiter for RedisLimiter {
    fn get_token_count<'a>(&self, key: &'a str) -> Option<u32> {
        None
    }

    fn consume<'a>(
        &self,
        key: &'a str,
        interval: u32,
        capacity: u32,
        n: Option<u32>,
    ) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
