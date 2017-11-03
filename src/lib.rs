extern crate redis;
extern crate chrono;

use std::default::Default;

use chrono::{DateTime, Utc};
use redis::{
    Client as RedisClient,
    Script as RedisScript,
    Commands,
};

const LUA_SCRIPT: &'static str = include_str!("limiter.lua");
const REDIS_HOST: &'static str = "localhost";
const REDIS_PORT: u16 = 6379;
const REDIS_DB: u16 = 0;

fn timestamp_ms(t: DateTime<Utc>) -> i64 {
    t.timestamp() * 1000 + t.timestamp_subsec_millis() as i64
}

fn now_ms() -> i64 {
    timestamp_ms(Utc::now())
}

pub trait Limiter {
    fn get_token_count<'a>(&self, key: &'a str, interval: u32) -> Option<u32>;
    fn consume<'a>(
        &self,
        key: &'a str, interval: u32, capacity: u32,
        n: Option<u32>,
    ) -> bool;
    fn consume_one<'a>(
        &self,
        key: &'a str, interval: u32, capacity: u32
    ) -> bool {
        self.consume(key, interval, capacity, Some(1))
    }
}

pub struct RedisLimiter {
    redis_cli: RedisClient,
    key_prefix: String,
    script: RedisScript,
}

pub struct RedisLimiterBuilder<'a> {
    redis_cli: Option<RedisClient>,
    host: Option<&'a str>,
    port: Option<u16>,
    db: Option<u16>,
    key_prefix: Option<&'a str>,
    script_str: Option<&'a str>,
}

impl<'a> RedisLimiterBuilder<'a> {
    pub fn new() -> Self {
        RedisLimiterBuilder{
            redis_cli: None,
            host: None,
            port: None,
            db: None,
            key_prefix: None,
            script_str: None,
        }
    }
    pub fn build(self) -> RedisLimiter {
        let script_str = self.script_str.unwrap_or(LUA_SCRIPT);
        let key_prefix = self.key_prefix.unwrap_or("limiter");
        if let Some(redis_cli) = self.redis_cli {
            RedisLimiter::from(redis_cli, key_prefix, script_str)
        } else {
            let url = format!(
                "redis://{}:{}/{}",
                self.host.unwrap_or(REDIS_HOST),
                self.port.unwrap_or(REDIS_PORT),
                self.db.unwrap_or(REDIS_DB)
            );
            let client = RedisClient::open(url.as_str()).unwrap();
            RedisLimiter::from(client, key_prefix, script_str)
        }
    }

    pub fn redis_cli(&mut self, client: RedisClient) -> &mut Self {
        self.redis_cli = Some(client);
        self
    }
    pub fn host(&mut self, value: &'a str) -> &mut Self {
        self.host = Some(value);
        self
    }
    pub fn port(&mut self, value: u16) -> &mut Self {
        self.port = Some(value);
        self
    }
    pub fn db(&mut self, value: u16) -> &mut Self {
        self.db = Some(value);
        self
    }
    pub fn key_prefix(&mut self, value: &'a str) -> &mut Self {
        self.key_prefix = Some(value);
        self
    }
    pub fn script_str(&mut self, value: &'a str) -> &mut Self {
        self.script_str = Some(value);
        self
    }
}

impl RedisLimiter {
    pub fn from<'a>(
        redis_cli: RedisClient,
        key_prefix: &'a str,
        script_str: &'a str,
    ) -> Self {
        let key_prefix = key_prefix.to_owned();
        let script = RedisScript::new(script_str);
        RedisLimiter{ redis_cli, key_prefix, script }
    }

    pub fn get_redis_key<'a>(&self, key: &'a str, interval: u32) -> String {
        format!("{}:{}:{}", self.key_prefix, key, interval)
    }
}

impl Default for RedisLimiter {
    fn default() -> Self { RedisLimiterBuilder::new().build() }
}

impl Limiter for RedisLimiter {
    fn get_token_count<'a>(&self, key: &'a str, interval: u32) -> Option<u32> {
        self.redis_cli
            .get_connection()
            .unwrap()
            .hget(self.get_redis_key(key, interval), "tokens")
            .ok()
    }

    fn consume<'a>(
        &self,
        key: &'a str,
        interval: u32,
        capacity: u32,
        n: Option<u32>,
    ) -> bool {
        let redis_key = self.get_redis_key(key, interval);
        let expire = interval * 3 + 60;
        let interval_ms = interval * 1000;
        let n = n.unwrap_or(1);
        let conn = self.redis_cli
            .get_connection()
            .unwrap();
        self.script
            .key(redis_key)
            .arg(interval_ms)
            .arg(capacity)
            .arg(n)
            .arg(now_ms())
            .arg(expire)
            .invoke(&conn) == Ok(1)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::thread;
    use super::*;

    fn redis_client() -> RedisClient {
        let url = format!("redis://{}:{}/{}", REDIS_HOST, REDIS_PORT, REDIS_DB);
        RedisClient::open(url.as_str()).unwrap()
    }

    fn consume_many<'a>(
        limiter: &RedisLimiter,
        key: &'a str, interval: u32, capacity: u32, n: u32) {
        for i in 0..n {
            let (success, count) = if i >= capacity {
                (false, Some(0))
            } else {
                (true, Some(capacity - i - 1))
            };
            assert_eq!(limiter.consume_one(key, interval, capacity), success);
            assert_eq!(limiter.get_token_count(key, interval), count);
        }
    }

    #[test]
    fn test_basic() {
        let limiter = RedisLimiter::default();
        let key = "test_basic";
        let interval = 10;
        let capacity = 6;

        assert_eq!(limiter.get_token_count(key, interval), None);
        consume_many(&limiter, key, interval, capacity, 12);

        let _: () = redis_client()
            .del(limiter.get_redis_key(key, interval))
            .unwrap();
    }

    #[test]
    fn test_refill() {
        let limiter = RedisLimiter::default();
        let key = "test_refill";
        let interval = 2;
        let capacity = 5;

        assert_eq!(limiter.get_token_count(key, interval), None);
        consume_many(&limiter, key, interval, capacity, 6);
        assert_eq!(limiter.consume_one(key, interval, capacity), false);
        assert_eq!(limiter.get_token_count(key, interval), Some(0));

        thread::sleep(Duration::from_millis(2001));
        assert_eq!(limiter.consume_one(key, interval, capacity), true);
        assert_eq!(limiter.get_token_count(key, interval), Some(capacity-1));

        let _: () = redis_client()
            .del(limiter.get_redis_key(key, interval))
            .unwrap();
    }
}
