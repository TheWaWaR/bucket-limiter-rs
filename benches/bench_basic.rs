#![feature(test)]
extern crate test;
extern crate redis;
extern crate bucket_limiter;

use test::Bencher;
use redis::Commands;
use bucket_limiter::{Limiter, RedisLimiter};

#[bench]
fn bench_simple(b: &mut Bencher) {
    let limiter = RedisLimiter::default();
    let key = "bench_simple";
    let interval = 600;
    let capacity = 1000_0000;
    b.iter(|| {
        limiter.consume_one(key, interval, capacity, 1).is_ok();
    });
    let _: () = redis::Client::open("redis://127.0.0.1:6379")
        .unwrap()
        .del(limiter.get_redis_key(key, interval))
        .unwrap();
}
