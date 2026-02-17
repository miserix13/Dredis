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
- Streams: `XADD`, `XDEL`, `XLEN`, `XRANGE`, `XREAD`, `XGROUP CREATE`, `XGROUP DESTROY`, `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM`

Notes:

- `XREAD` supports `COUNT` and does not support `BLOCK`.
- `XREADGROUP` supports `COUNT` and `BLOCK`.
- `XPENDING` supports both summary and extended forms with filtering (IDLE, consumer, range).
- `XCLAIM` supports all options: `IDLE`, `TIME`, `RETRYCOUNT`, `FORCE`, `JUSTID`.
- Consumer groups track pending entries with delivery count, idle time, and consumer ownership.

## Feature matrix

| Area | Supported | Notes |
| --- | --- | --- |
| RESP parsing/encoding | Yes | DotNetty Redis codec |
| Strings | Yes | `GET`, `SET`, `MGET`, `MSET` |
| Keys | Yes | `DEL`, `EXISTS` |
| Counters | Yes | `INCR`, `INCRBY`, `DECR`, `DECRBY` |
| Expiration | Yes | `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL` |
| Hashes | Yes | `HSET`, `HGET`, `HDEL`, `HGETALL` |
| Streams | Partial | `XADD`, `XDEL`, `XLEN`, `XRANGE`, `XREAD` |
| Consumer groups | Yes | `XGROUP CREATE/DESTROY`, `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM` |
| Lists | No | Planned |
| Sets | No | Planned |
| Sorted sets | No | Planned |
| Pub/Sub | No | Planned |
| Transactions | No | Planned |

## Short roadmap

- Streams: Better `BLOCK` behavior for `XREAD`
- Lists: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`
- Sets: `SADD`, `SREM`, `SMEMBERS`, `SCARD`
- Sorted sets: `ZADD`, `ZREM`, `ZRANGE`, `ZCARD`
- Pub/Sub: `SUBSCRIBE`, `PUBLISH`
