
local effects = {}
for idx, key in ipairs(KEYS) do
  local idxBase = (idx - 1) * 5
  local interval = tonumber(ARGV[idxBase + 1])
  local capacity = tonumber(ARGV[idxBase + 2])
  local nTokens = tonumber(ARGV[idxBase + 3])
  local timeNow = tonumber(ARGV[idxBase + 4])
  local expire = tonumber(ARGV[idxBase + 5])
  local currentTokens = -1
  local lastFillAt = timeNow

  if redis.call('exists', key) == 0 then
    currentTokens = capacity
    redis.call('hset', key, 'lastFillAt', timeNow)
  else
    lastFillAt = tonumber(redis.call('hget', key, 'lastFillAt'))
    if timeNow - lastFillAt > interval then
      currentTokens = capacity
      redis.call('hset', key, 'lastFillAt', timeNow)
    else
      currentTokens = tonumber(redis.call('hget', key, 'tokens'))
      if currentTokens > capacity then
        currentTokens = capacity
      end
    end
  end

  assert(currentTokens >= 0)

  if expire > 0 then
    redis.call('expire', key, expire)
  end

  if nTokens > currentTokens then
    redis.call('hset', key, 'tokens', currentTokens)
    for i, effect in ipairs(effects) do
      redis.call('hset', effect[1], 'tokens', effect[2])
    end
    return {key, interval, capacity, currentTokens, lastFillAt}
  else
    table.insert(effects, {key, currentTokens, nTokens})
  end
end

for i, effect in ipairs(effects) do
  redis.call('hset', effect[1], 'tokens', effect[2] - effect[3])
end

return {'', 0, 0, 0, 0}
