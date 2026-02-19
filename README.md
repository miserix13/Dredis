# Dredis

A .NET 10 implementation of the Redis Serialization Protocol using DotNetty and C# with a storage abstraction.

## RESP implementation status

Currently implemented RESP commands and behavior:

- Connection: `PING`, `ECHO`
- Strings: `GET`, `SET` (supports `EX`, `PX`, `NX`, `XX`), `MGET`, `MSET`
- Keys: `DEL`, `EXISTS`
- Counters: `INCR`, `INCRBY`, `DECR`, `DECRBY`
- Bitmaps: `GETBIT`, `SETBIT`, `BITCOUNT`, `BITOP`, `BITPOS`, `BITFIELD` (`GET`, `SET`, `INCRBY`, `OVERFLOW`)
- Expiration: `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`
- Hashes: `HSET`, `HGET`, `HDEL`, `HGETALL`
- Lists: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX`, `LSET`, `LTRIM`
- Sets: `SADD`, `SREM`, `SMEMBERS`, `SCARD`
- Sorted sets: `ZADD`, `ZREM`, `ZRANGE`, `ZCARD`, `ZSCORE`, `ZRANGEBYSCORE`, `ZINCRBY`, `ZCOUNT`, `ZRANK`, `ZREVRANK`, `ZREMRANGEBYSCORE`
- Streams: `XADD`, `XDEL`, `XLEN`, `XTRIM`, `XRANGE`, `XREVRANGE`, `XREAD`, `XINFO`, `XSETID`, `XGROUP CREATE`, `XGROUP DESTROY`, `XGROUP SETID`, `XGROUP DELCONSUMER`, `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM`
- Pub/Sub: `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`
- Transactions: `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`
- JSON: `JSON.SET`, `JSON.GET`, `JSON.DEL`, `JSON.TYPE`, `JSON.STRLEN`, `JSON.ARRLEN`, `JSON.ARRAPPEND`, `JSON.ARRINDEX`, `JSON.ARRINSERT`, `JSON.ARRREM`, `JSON.ARRTRIM`, `JSON.MGET`
- Vectors: `VSET`, `VGET`, `VDIM`, `VDEL`, `VSIM`, `VSEARCH` (metrics: `COSINE`, `DOT`, `L2`)

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
- `JSON.SET` stores JSON values at specified JSONPath locations.
- `JSON.GET` retrieves JSON values from one or multiple paths (defaults to root path `$`).
- `JSON.DEL` deletes JSON values at specified paths and returns the count of deleted paths.
- `JSON.TYPE` returns the JSON type(s) at the specified path(s) (object, array, string, number, boolean, null).
- `JSON.STRLEN` returns the string length at the specified path(s).
- `JSON.ARRLEN` returns the array length at the specified path(s).
- `JSON.ARRAPPEND` appends one or more JSON values to the array at the specified path.
- `JSON.ARRINDEX` returns the index of a JSON value in the array at the specified path, or -1 if not found.
- `JSON.ARRINSERT` inserts one or more JSON values at the specified index in the array.
- `JSON.ARRREM` removes an element at the specified index from the array (or last element if no index provided).
- `JSON.ARRTRIM` trims the array at the specified path to the specified range.
- `JSON.MGET` retrieves JSON values from multiple keys at the specified path.
- `VSEARCH` supports paging/options via `LIMIT <n>` and optional `OFFSET <n>` before query vector components.
- `VSEARCH` is backward-compatible with positional top-k form: `VSEARCH prefix topK metric ...`.
- `VSEARCH` strict option order: metric form must be `VSEARCH prefix metric LIMIT n [OFFSET n] ...`; positional form supports only optional `OFFSET`.

## Feature matrix

| Area | Supported | Notes |
| --- | --- | --- |
| RESP parsing/encoding | Yes | DotNetty Redis codec |
| Strings | Yes | `GET`, `SET`, `MGET`, `MSET` |
| Keys | Yes | `DEL`, `EXISTS` |
| Counters | Yes | `INCR`, `INCRBY`, `DECR`, `DECRBY` |
| Bitmaps | Yes | `GETBIT`, `SETBIT`, `BITCOUNT`, `BITOP`, `BITPOS`, `BITFIELD` (`GET`, `SET`, `INCRBY`, `OVERFLOW`) |
| Expiration | Yes | `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL` |
| Hashes | Yes | `HSET`, `HGET`, `HDEL`, `HGETALL` |
| Lists | Yes | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX`, `LSET`, `LTRIM` |
| Sets | Yes | `SADD`, `SREM`, `SMEMBERS`, `SCARD` |
| Sorted sets | Yes | `ZADD`, `ZREM`, `ZRANGE`, `ZCARD`, `ZSCORE`, `ZRANGEBYSCORE`, `ZINCRBY`, `ZCOUNT`, `ZRANK`, `ZREVRANK`, `ZREMRANGEBYSCORE` |
| Streams | Yes | `XADD`, `XDEL`, `XLEN`, `XTRIM`, `XRANGE`, `XREVRANGE`, `XREAD`, `XINFO`, `XSETID` |
| Consumer groups | Yes | `XGROUP CREATE/DESTROY/SETID/DELCONSUMER`, `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM` |
| Pub/Sub | Yes | `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE` |
| Transactions | Yes | `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH` with optimistic locking |
| JSON | Yes | `JSON.SET`, `JSON.GET`, `JSON.DEL`, `JSON.TYPE`, `JSON.STRLEN`, `JSON.ARRLEN`, `JSON.ARRAPPEND`, `JSON.ARRINDEX`, `JSON.ARRINSERT`, `JSON.ARRREM`, `JSON.ARRTRIM`, `JSON.MGET` |
| Vectors | Yes | `VSET`, `VGET`, `VDIM`, `VDEL`, `VSIM`, `VSEARCH` |

## Client compatibility

Dredis includes handshake compatibility for common .NET Redis clients, including `StackExchange.Redis` and NRedis-family clients (`NRedisStack`).

- Handshake/probe commands supported: `CLIENT` (`SETNAME`, `SETINFO`, `ID`, `GETNAME`), `COMMAND`, `CONFIG GET`, `INFO`, `SELECT`, `READONLY`, `READWRITE`.
- Inline command parsing is supported in addition to array-style RESP command frames.
- Missing tie-break key (`__Booksleeve_TieBreak`) is handled with a valid bulk-string response shape expected by StackExchange.Redis.
- End-to-end compatibility is validated by `Dredis.Tests/DredisNRedisCompatibilityTests.cs`.

## Architecture

Dredis is built on a modular architecture with the following components:

- **DredisServer**: Network server using DotNetty for high-performance async I/O
- **DredisCommandHandler**: RESP command processor with support for Redis commands and transactions
- **IKeyValueStore**: Storage abstraction interface for implementing custom storage backends
- **JSON Support**: Dedicated JSON command handlers in `DredisCommandHandler.Json.cs` supporting JSONPath operations

The JSON implementation uses `System.Text.Json` for parsing and manipulation, and supports JSONPath syntax for navigating JSON documents. All JSON commands support both single and multi-path operations where applicable.

## Short roadmap

- Additional Redis commands as needed
