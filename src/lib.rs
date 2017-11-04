extern crate redis;
extern crate chrono;

use std::default::Default;

use chrono::{DateTime, Utc};
use redis::{
    Client as RedisClient,
    Script as RedisScript,
    Commands,
};

const LUA_SCRIPT: &str = include_str!("limiter.lua");
const KEY_PREFIX: &str = "limiter";
const REDIS_HOST: &str = "localhost";
const REDIS_PORT: u16 = 6379;
const REDIS_DB: u16 = 0;

fn timestamp_ms(t: DateTime<Utc>) -> i64 {
    t.timestamp() * 1000 + i64::from(t.timestamp_subsec_millis())
}

fn now_ms() -> i64 {
    timestamp_ms(Utc::now())
}

pub trait Limiter {
    fn get_token_count<'a>(&self, key: &'a str, interval: u32) -> Option<u32>;
    fn consume<'a>(&self, args: Vec<(&'a str, u32, u32, u32)>)
                   -> Result<(), RedisConsumeError>;
    fn consume_one<'a>(&self, key: &'a str, interval: u32, capacity: u32, n: u32)
                       -> Result<(), RedisConsumeError> {
        self.consume(vec![(key, interval, capacity, n)])
    }
}

#[derive(Debug)]
pub enum RedisConsumeError {
    Denied {
        redis_key: String,
        interval: u32,
        capacity: u32,
        current_tokens: u32,
        last_fill_at: i64,
    },
    BadArg(String),
    Redis(redis::RedisError)
}

pub struct RedisLimiter {
    redis_cli: RedisClient,
    key_prefix: String,
    script: RedisScript,
}

#[derive(Default)]
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
        let key_prefix = self.key_prefix.unwrap_or(KEY_PREFIX);
        if let Some(redis_cli) = self.redis_cli {
            RedisLimiter::new(redis_cli, key_prefix, script_str)
        } else {
            let url = format!(
                "redis://{}:{}/{}",
                self.host.unwrap_or(REDIS_HOST),
                self.port.unwrap_or(REDIS_PORT),
                self.db.unwrap_or(REDIS_DB)
            );
            let client = RedisClient::open(url.as_str()).unwrap();
            RedisLimiter::new(client, key_prefix, script_str)
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
    pub fn new<'a>(
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

    fn consume<'a>(&self, args: Vec<(&'a str, u32, u32, u32)>)
                   -> Result<(), RedisConsumeError> {
        let now_ms = now_ms();
        let mut invocation = self.script.prepare_invoke();
        for (ref key, interval, capacity, n) in args {
            if key.len() < 1 || n < 1 || interval < 1 || capacity < 1 {
                return Err(RedisConsumeError::BadArg(format!(
                    "[BadArg]: key={}, interval={}, capacity={}, n={}",
                    key, interval, capacity, n
                )));
            }
            let redis_key = self.get_redis_key(key, interval);
            let expire = interval * 2 + 15;
            let interval_ms = interval * 1000;
            invocation
                .key(redis_key)
                .arg(interval_ms)
                .arg(capacity)
                .arg(n)
                .arg(now_ms)
                .arg(expire);
        }
        let conn = try!{
            self.redis_cli
                .get_connection()
                .map_err(|e| RedisConsumeError::Redis(e))
        };
        match invocation.invoke(&conn) {
            Ok((_, 0, 0, 0, 0)) => Ok(()),
            Ok((redis_key, interval_ms, capacity,
                current_tokens, last_fill_at)) => {
                let interval = interval_ms / 1000;
                Err(RedisConsumeError::Denied{
                    redis_key, interval, capacity,
                    current_tokens, last_fill_at
                })
            }
            Err(e) => Err(RedisConsumeError::Redis(e))
        }
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
            assert_eq!(limiter.consume_one(key, interval, capacity, 1).is_ok(), success);
            assert_eq!(limiter.get_token_count(key, interval), count);
        }
    }

    fn del_keys<'a>(limiter: &RedisLimiter, args: Vec<(&'a str, u32)>) {
        let client = redis_client();
        for (key, interval) in args {
            let _: () = client
                .del(limiter.get_redis_key(key, interval))
                .unwrap();
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

        del_keys(&limiter, vec![(key, interval)]);
    }

    #[test]
    fn test_refill() {
        let limiter = RedisLimiter::default();
        let key = "test_refill";
        let interval = 1;
        let capacity = 5;

        assert_eq!(limiter.get_token_count(key, interval), None);
        consume_many(&limiter, key, interval, capacity, 6);
        assert_eq!(limiter.consume_one(key, interval, capacity, 1).is_ok(), false);
        assert_eq!(limiter.get_token_count(key, interval), Some(0));

        thread::sleep(Duration::from_millis((interval * 1000 + 2) as u64));
        assert_eq!(limiter.consume_one(key, interval, capacity, 1).is_ok(), true);
        assert_eq!(limiter.get_token_count(key, interval), Some(capacity-1));

        del_keys(&limiter, vec![(key, interval)]);
    }

    #[test]
    fn test_multiple() {
        let limiter = RedisLimiter::default();
        let key = "test_multiple";

        let (key_1, interval_1, capacity_1, n_1) = (format!("{}-1", key), 2, 3, 1);
        let (key_2, interval_2, capacity_2, n_2) = (format!("{}-2", key), 4, 4, 1);
        // [Step.prepare]: Consume all tokens in key_1
        for _ in 0..capacity_1 {
            assert_eq!(limiter.consume_one(key_1.as_str(), interval_1, capacity_1, n_1).is_ok(), true);
        }
        for (sleep_ms, args, should_ok, token_count_1, token_count_2) in vec![
            // [Step.1]: All key_1 comsumed, so should be Error, and key_2 not touched yet.
            (0,
             vec![
                 (key_1.as_str(), interval_1, capacity_1, n_1),
                 (key_2.as_str(), interval_2, capacity_2, n_2),
             ],
             false,
             Some(0), None),
            // [Step.2]: Touch key_2 first then it has token_count=${capacity_2}, Error because key_1 still empty
            (0,
             vec![
                 (key_2.as_str(), interval_2, capacity_2, n_2),
                 (key_1.as_str(), interval_1, capacity_1, n_1),
             ],
             false,
             Some(0), Some(capacity_2)),
            // [Step.3]: Sleep more than interval_1 ms, then consume,
            //           key_1's token_count become (capacity_1 - 1), key_2' token_count become (capacity_2 - 1)
            ((interval_1 * 1000 + 2) as u64,
             vec![
                 (key_2.as_str(), interval_2, capacity_2, n_2),
                 (key_1.as_str(), interval_1, capacity_1, n_1),
             ],
             true,
             Some(capacity_1 - 1), Some(capacity_2 - 1)),
        ] {
            if sleep_ms > 0 {
                thread::sleep(Duration::from_millis(sleep_ms));
            }
            let rv = limiter.consume(args);
            if !should_ok {
                assert_eq!(rv.is_err(), true);
                let _ = rv.map_err(|err| {
                    match err {
                        RedisConsumeError::Denied {
                            redis_key, interval, capacity,
                            current_tokens, last_fill_at: _
                        } => {
                            assert_eq!(redis_key, limiter.get_redis_key(key_1.as_str(), interval_1));
                            assert_eq!(interval, interval_1);
                            assert_eq!(capacity, capacity_1);
                            assert_eq!(current_tokens, 0);
                        }
                        e @ _ => {
                            panic!("Invalid RedisConsumeError: {:?}", e)
                        }
                    }
                });
            }
            assert_eq!(limiter.get_token_count(key_1.as_str(), interval_1), token_count_1);
            assert_eq!(limiter.get_token_count(key_2.as_str(), interval_2), token_count_2);
        }

        del_keys(&limiter, vec![(key_1.as_str(), interval_1), (key_2.as_str(), interval_2)]);
    }
}
