# bucket-limiter-rs
Token bucket like limiter library based on redis storage
[Python version](https://github.com/TheWaWaR/bucket-limiter-py)

# Example (use case)
Assume you have a API service, and want to limit user request rate like:
  - 10,000 requests/day
  - 600 requests/1-hour
  - 10 requests/10-seconds

You can do this:

``` rust
let limiter = RedisLimiter::default();
let key = format!("{}:{}", request.endpoint, request.method);
// Once one request has come
if (limiter.consume_one(key, 10, 10) &&
    limiter.consume_one(key, 3600, 600) &&
    limiter.consume_one(key, 24*3600, 10000)) {
    // Process the request
} else {
    // Reject the request
}
```

