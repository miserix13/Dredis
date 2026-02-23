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
- Probabilistic: `PFADD`, `PFCOUNT`, `PFMERGE` (HyperLogLog)
- Bloom filter: `BF.RESERVE`, `BF.ADD`, `BF.MADD`, `BF.EXISTS`, `BF.MEXISTS`, `BF.INFO`
- Cuckoo filter: `CF.RESERVE`, `CF.ADD`, `CF.ADDNX`, `CF.INSERT`, `CF.INSERTNX`, `CF.EXISTS`, `CF.DEL`, `CF.COUNT`, `CF.INFO`
- t-digest: `TDIGEST.CREATE`, `TDIGEST.RESET`, `TDIGEST.ADD`, `TDIGEST.QUANTILE`, `TDIGEST.CDF`, `TDIGEST.RANK`, `TDIGEST.REVRANK`, `TDIGEST.BYRANK`, `TDIGEST.BYREVRANK`, `TDIGEST.TRIMMED_MEAN`, `TDIGEST.MIN`, `TDIGEST.MAX`, `TDIGEST.INFO`
- Top-K: `TOPK.RESERVE`, `TOPK.ADD`, `TOPK.INCRBY`, `TOPK.QUERY`, `TOPK.COUNT`, `TOPK.LIST`, `TOPK.INFO`
- Time series: `TS.CREATE`, `TS.ADD`, `TS.INCRBY`, `TS.DECRBY`, `TS.GET`, `TS.RANGE`, `TS.REVRANGE`, `TS.MRANGE`, `TS.MREVRANGE`, `TS.DEL`, `TS.INFO`
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
- `TS.CREATE` supports optional `RETENTION <milliseconds>`, `ON_DUPLICATE <LAST|FIRST|MIN|MAX|SUM|BLOCK>`, and `LABELS <name value ...>`.
- `TS.ADD` supports optional `ON_DUPLICATE <LAST|FIRST|MIN|MAX|SUM|BLOCK>`.
- `TS.RANGE` and `TS.REVRANGE` support optional `COUNT <n>` and `AGGREGATION <AVG|SUM|MIN|MAX|COUNT> <bucketMs>`.
- `TS.MRANGE` and `TS.MREVRANGE` support `FILTER <label=value ...>` with optional `COUNT` and `AGGREGATION`.

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
| Probabilistic | Yes | `PFADD`, `PFCOUNT`, `PFMERGE` (HyperLogLog) |
| Bloom filter | Yes | `BF.RESERVE`, `BF.ADD`, `BF.MADD`, `BF.EXISTS`, `BF.MEXISTS`, `BF.INFO` |
| Cuckoo filter | Yes | `CF.RESERVE`, `CF.ADD`, `CF.ADDNX`, `CF.INSERT`, `CF.INSERTNX`, `CF.EXISTS`, `CF.DEL`, `CF.COUNT`, `CF.INFO` |
| t-digest | Yes | `TDIGEST.CREATE`, `TDIGEST.RESET`, `TDIGEST.ADD`, `TDIGEST.QUANTILE`, `TDIGEST.CDF`, `TDIGEST.RANK`, `TDIGEST.REVRANK`, `TDIGEST.BYRANK`, `TDIGEST.BYREVRANK`, `TDIGEST.TRIMMED_MEAN`, `TDIGEST.MIN`, `TDIGEST.MAX`, `TDIGEST.INFO` |
| Top-K | Yes | `TOPK.RESERVE`, `TOPK.ADD`, `TOPK.INCRBY`, `TOPK.QUERY`, `TOPK.COUNT`, `TOPK.LIST`, `TOPK.INFO` |
| Time series | Yes | `TS.CREATE`, `TS.ADD`, `TS.INCRBY`, `TS.DECRBY`, `TS.GET`, `TS.RANGE`, `TS.REVRANGE`, `TS.MRANGE`, `TS.MREVRANGE`, `TS.DEL`, `TS.INFO` |
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

## Custom commands

You can register custom RESP commands on `DredisServer` before calling `StartAsync` or `RunAsync`. Registered commands are automatically applied to each connection's `DredisCommandHandler`.

```csharp
using Dredis;
using Dredis.Abstractions.Command;
using Dredis.Abstractions.Storage;

public sealed class HelloCommand : ICommand
{
 public string Name => "HELLO";

 public Task<string> ExecuteAsync(params string[] parameters)
 {
  var payload = parameters.Length == 0 ? "world" : string.Join(',', parameters);
  return Task.FromResult($"hello:{payload}");
 }
}

var store = new MyKeyValueStore(); // your IKeyValueStore implementation
var server = new DredisServer(store);

server.Register(new HelloCommand());

await server.RunAsync(6379);
```

From a Redis client, this command can be called as `HELLO` or `HELLO one two`, and returns a bulk-string reply.

## Server options and configuration

`DredisServer` now supports an options model (`DredisServerOptions`) and configuration binding via `Microsoft.Extensions.Configuration`.

### Option keys

- `BindAddress` (default: `127.0.0.1`)
- `Port` (default: `6379`)
- `BossGroupThreadCount` (default: `1`)
- `WorkerGroupThreadCount` (optional; when omitted, DotNetty default sizing is used)

Validation note:

- Invalid values fail fast during options binding/server construction.
- `BindAddress` must be a valid IP address.
- `Port` must be between `1` and `65535`.
- `BossGroupThreadCount` and `WorkerGroupThreadCount` (when provided) must be greater than `0`.

### Configure with appsettings.json

```json
{
    "DredisServer": {
        "BindAddress": "127.0.0.1",
        "Port": 6379,
        "BossGroupThreadCount": 1,
        "WorkerGroupThreadCount": 4
    }
}
```

```csharp
using Dredis;
using Dredis.Abstractions.Storage;
using Microsoft.Extensions.Configuration;

var configuration = new ConfigurationBuilder()
        .AddJsonFile("appsettings.json", optional: false)
        .Build();

var store = new MyKeyValueStore();
var server = new DredisServer(store, configuration);

await server.RunAsync(); // Uses DredisServer section values
```

You can still override just the port at call-time:

```csharp
await server.RunAsync(6380);
```

### Choosing an extension model

Use `ICommand` when:

- You want to add application-level custom commands with simple string in/out behavior.
- You prefer explicit registration on `DredisServer` via `server.Register(...)`.
- You do not need to extend your storage abstraction.

Use `ICustomDataTypeStore` when:

- You are implementing storage-backed custom data type commands.
- You want unknown commands to flow through the storage layer without changing `IKeyValueStore`.
- You need richer RESP response shapes (integer, error, bulk, null bulk, array).

Compatibility note:

- Existing `IKeyValueStore` implementations remain fully compatible with no code changes.
- `ICustomDataTypeStore` is optional and only needed when you want unknown-command flow-through for custom data types.
- If `ICustomDataTypeStore` is not implemented, unknown commands continue through registered `ICommand` handlers and then standard unknown-command behavior.

Example skeleton:

```csharp
using Dredis.Abstractions.Storage;

public sealed class MyStore : IKeyValueStore, ICustomDataTypeStore
{
    public Task<CustomDataTypeResult> TryExecuteCustomDataTypeAsync(string command, string[] args, CancellationToken token = default)
    {
        if (string.Equals(command, "CTYPE.ECHO", StringComparison.OrdinalIgnoreCase))
        {
            var payload = args.Length == 0 ? string.Empty : string.Join(',', args);
            return Task.FromResult(CustomDataTypeResult.BulkString(System.Text.Encoding.UTF8.GetBytes($"ctype:{payload}")));
        }

        return Task.FromResult(CustomDataTypeResult.NotHandled);
    }

    // Implement IKeyValueStore members...
}
```

Dispatch order for unknown commands is:

1. `ICustomDataTypeStore` (if implemented by the active store)
2. Registered `ICommand` handlers
3. Standard unknown-command error

## Changelog

- Added optional `ICustomDataTypeStore` extension to support storage-backed custom data type commands without introducing breaking changes to `IKeyValueStore`.
- Unknown command resolution now flows through `ICustomDataTypeStore` (when implemented), then registered `ICommand` handlers, then standard unknown-command behavior.

## Short roadmap

- Additional Redis commands as needed
