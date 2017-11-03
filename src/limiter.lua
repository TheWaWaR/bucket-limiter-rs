
local key = KEYS[1]
local interval = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local nTokens = tonumber(ARGV[3])
local timeNow = tonumber(ARGV[4])
local expire = tonumber(ARGV[5])

local currentTokens

if redis.call('exists', key) == 0 then
  currentTokens = capacity
  redis.call('hset', key, 'lastFillAt', timeNow)
else
  local tokens = tonumber(redis.call('hget', key, 'tokens'))
  local lastFillAt = tonumber(redis.call('hget', key, 'lastFillAt'))
  if timeNow - lastFillAt > interval then
    currentTokens = capacity
    redis.call('hset', key, 'lastFillAt', timeNow)
  else
    currentTokens = tokens
  end
end

assert(currentTokens >= 0)

if expire > 0 then
  redis.call('expire', key, expire)
end

if nTokens > currentTokens then
  redis.call('hset', key, 'tokens', currentTokens)
  return 0
else
  redis.call('hset', key, 'tokens', currentTokens - nTokens)
  return 1
end
