# Dredis

A .NET 10 implementation of the Redis Serialization Protocol using DotNetty and C# with a storage abstraction.

## RESP implementation status

Currently implemented RESP commands and behavior:

- Connection: `PING`, `ECHO`
- Strings: `GET`, `SET` (supports `EX`, `PX`, `NX`, `XX`), `MGET`, `MSET`
- Keys: `DEL`, `EXISTS`
- Counters: `INCR`, `INCRBY`, `DECR`, `DECRBY`
- Expiration: `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`
- Hashes: `HSET`, `HGET`, `HDEL`, `HGETALL`
- Lists: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX`, `LSET`, `LTRIM`
- Sets: `SADD`, `SREM`, `SMEMBERS`, `SCARD`
- Sorted sets: `ZADD`, `ZREM`, `ZRANGE`, `ZCARD`, `ZSCORE`, `ZRANGEBYSCORE`, `ZINCRBY`, `ZCOUNT`, `ZRANK`, `ZREVRANK`, `ZREMRANGEBYSCORE`
- Streams: `XADD`, `XDEL`, `XLEN`, `XTRIM`, `XRANGE`, `XREVRANGE`, `XREAD`, `XINFO`, `XSETID`, `XGROUP CREATE`, `XGROUP DESTROY`, `XGROUP SETID`, `XGROUP DELCONSUMER`, `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM`
- Pub/Sub: `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`
- Transactions: `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`

Notes:

- `XREAD` supports `COUNT` and `BLOCK`.
- `XREADGROUP` supports `COUNT` and `BLOCK`.
- `XPENDING` supports both summary and extended forms with filtering (IDLE, consumer, range).
- `XCLAIM` supports all options: `IDLE`, `TIME`, `RETRYCOUNT`, `FORCE`, `JUSTID`.
- `XINFO` supports `STREAM`, `GROUPS`, and `CONSUMERS`.
- `ZRANGE` and `ZRANGEBYSCORE` both support `WITHSCORES` option.
- `ZINCRBY` increments member scores and creates members if they don't exist.
- `ZRANK` and `ZREVRANK` return 0-based ranks in ascending and descending order respectively.
- Consumer groups track pending entries with delivery count, idle time, and consumer ownership.
- `PUBLISH` returns the number of clients that received the message.
- `SUBSCRIBE` sends subscription confirmations and receives published messages via push messages.
- `UNSUBSCRIBE` with no arguments unsubscribes from all channels.
- `PSUBSCRIBE` supports glob-style patterns (`*`, `?`, `[abc]`) for channel matching.
- `PUNSUBSCRIBE` with no arguments unsubscribes from all patterns.
- Pattern subscriptions receive `pmessage` responses with pattern, channel, and message.
- `MULTI` begins a transaction, queueing subsequent commands.
- `EXEC` executes all queued commands atomically, returning an array of results.
- `DISCARD` cancels a transaction and discards all queued commands.
- `WATCH` provides optimistic locking by monitoring keys for modifications.
- `UNWATCH` clears all watched keys (automatically cleared by `EXEC` and `DISCARD`).
- Transactions support optimistic locking via `WATCH`: if a watched key is modified before `EXEC`, the transaction is aborted and returns null.

## Feature matrix

| Area | Supported | Notes |
| --- | --- | --- |
| RESP parsing/encoding | Yes | DotNetty Redis codec |
| Strings | Yes | `GET`, `SET`, `MGET`, `MSET` |
| Keys | Yes | `DEL`, `EXISTS` |
| Counters | Yes | `INCR`, `INCRBY`, `DECR`, `DECRBY` |
| Expiration | Yes | `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL` |
| Hashes | Yes | `HSET`, `HGET`, `HDEL`, `HGETALL` |
| Lists | Yes | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX`, `LSET`, `LTRIM` |
| Sets | Yes | `SADD`, `SREM`, `SMEMBERS`, `SCARD` |
| Sorted sets | Yes | `ZADD`, `ZREM`, `ZRANGE`, `ZCARD`, `ZSCORE`, `ZRANGEBYSCORE`, `ZINCRBY`, `ZCOUNT`, `ZRANK`, `ZREVRANK`, `ZREMRANGEBYSCORE` |
| Streams | Yes | `XADD`, `XDEL`, `XLEN`, `XTRIM`, `XRANGE`, `XREVRANGE`, `XREAD`, `XINFO`, `XSETID` |
| Consumer groups | Yes | `XGROUP CREATE/DESTROY/SETID/DELCONSUMER`, `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM` |
| Pub/Sub | Yes | `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE` |
| Transactions | Yes | `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH` with optimistic locking |

## Short roadmap

- Additional Redis commands as needed
