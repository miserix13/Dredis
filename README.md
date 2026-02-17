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
- Sorted sets: `ZADD`, `ZREM`, `ZRANGE`, `ZCARD`
- Streams: `XADD`, `XDEL`, `XLEN`, `XTRIM`, `XRANGE`, `XREVRANGE`, `XREAD`, `XINFO`, `XSETID`, `XGROUP CREATE`, `XGROUP DESTROY`, `XGROUP SETID`, `XGROUP DELCONSUMER`, `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM`

Notes:

- `XREAD` supports `COUNT` and `BLOCK`.
- `XREADGROUP` supports `COUNT` and `BLOCK`.
- `XPENDING` supports both summary and extended forms with filtering (IDLE, consumer, range).
- `XCLAIM` supports all options: `IDLE`, `TIME`, `RETRYCOUNT`, `FORCE`, `JUSTID`.
- `XINFO` supports `STREAM`, `GROUPS`, and `CONSUMERS`.
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
| Lists | Yes | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX`, `LSET`, `LTRIM` |
| Sets | Yes | `SADD`, `SREM`, `SMEMBERS`, `SCARD` |
| Sorted sets | Yes | `ZADD`, `ZREM`, `ZRANGE`, `ZCARD` |
| Streams | Yes | `XADD`, `XDEL`, `XLEN`, `XTRIM`, `XRANGE`, `XREVRANGE`, `XREAD`, `XINFO`, `XSETID` |
| Consumer groups | Yes | `XGROUP CREATE/DESTROY/SETID/DELCONSUMER`, `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM` |
| Sets | No | Planned |
| Sorted sets | No | Planned |
| Pub/Sub | No | Planned |
| Transactions | No | Planned |

## Short roadmap

- Pub/Sub: `SUBSCRIBE`, `PUBLISH`
