using System.Globalization;
using System.Linq;
using System.Text;
using DotNetty.Buffers;
using DotNetty.Codecs.Redis.Messages;
using DotNetty.Transport.Channels.Embedded;
using Dredis.Abstractions.Storage;
using Xunit;

namespace Dredis.Tests
{
    /// <summary>
    /// Command handler tests covering RESP command behavior.
    /// </summary>
    public sealed class DredisCommandHandlerTests
    {
        private static readonly Encoding Utf8 = new UTF8Encoding(false);

        [Fact]
        /// <summary>
        /// Verifies PING without args returns PONG.
        /// </summary>
        public async Task Ping_NoArgs_ReturnsPong()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("PING"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);

                var simple = Assert.IsType<SimpleStringRedisMessage>(response);
                Assert.Equal("PONG", simple.Content);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies PING with payload returns bulk string.
        /// </summary>
        public async Task Ping_WithMessage_ReturnsBulk()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("PING", "hello"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);

                var bulk = Assert.IsType<FullBulkStringRedisMessage>(response);
                Assert.Equal("hello", GetBulkString(bulk));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies ECHO rejects invalid argument count.
        /// </summary>
        public async Task Echo_WrongArgs_ReturnsError()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("ECHO"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);

                var error = Assert.IsType<ErrorRedisMessage>(response);
                Assert.Equal("ERR wrong number of arguments for 'echo' command", error.Content);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies SET followed by GET returns the stored value.
        /// </summary>
        public async Task Set_Get_RoundTrip()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("SET", "alpha", "bravo"));
                channel.RunPendingTasks();

                var setResponse = ReadOutbound(channel);
                var setOk = Assert.IsType<SimpleStringRedisMessage>(setResponse);
                Assert.Equal("OK", setOk.Content);

                channel.WriteInbound(Command("GET", "alpha"));
                channel.RunPendingTasks();

                var getResponse = ReadOutbound(channel);
                var bulk = Assert.IsType<FullBulkStringRedisMessage>(getResponse);
                Assert.Equal("bravo", GetBulkString(bulk));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies SET NX fails when the key already exists.
        /// </summary>
        public async Task Set_Nx_WhenExists_ReturnsNullBulk()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("SET", "key", "first"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("SET", "key", "second", "NX"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                Assert.Same(FullBulkStringRedisMessage.Null, response);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies INCRBY rejects non-integer values.
        /// </summary>
        public async Task IncrBy_NonInteger_ReturnsError()
        {
            var store = new InMemoryKeyValueStore();
            store.Seed("counter", "not-a-number");
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("INCRBY", "counter", "1"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var error = Assert.IsType<ErrorRedisMessage>(response);
                Assert.Equal("ERR value is not an integer or out of range", error.Content);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies MGET returns values and nulls in order.
        /// </summary>
        public async Task MGet_ReturnsValuesAndNulls()
        {
            var store = new InMemoryKeyValueStore();
            store.Seed("one", "1");
            store.Seed("three", "3");
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("MGET", "one", "two", "three"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                Assert.Equal(3, array.Children.Count);

                Assert.Equal("1", GetBulkOrNull(array.Children[0]));
                Assert.Null(GetBulkOrNull(array.Children[1]));
                Assert.Equal("3", GetBulkOrNull(array.Children[2]));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies MSET stores multiple values.
        /// </summary>
        public async Task MSet_SetsValues_ReturnsOk()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("MSET", "alpha", "1", "bravo", "2"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var ok = Assert.IsType<SimpleStringRedisMessage>(response);
                Assert.Equal("OK", ok.Content);

                channel.WriteInbound(Command("GET", "alpha"));
                channel.RunPendingTasks();

                var getResponse = ReadOutbound(channel);
                var bulk = Assert.IsType<FullBulkStringRedisMessage>(getResponse);
                Assert.Equal("1", GetBulkString(bulk));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies DEL removes keys and returns count.
        /// </summary>
        public async Task Del_RemovesKeys_ReturnsCount()
        {
            var store = new InMemoryKeyValueStore();
            store.Seed("a", "1");
            store.Seed("b", "2");
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("DEL", "a", "b", "c"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var integer = Assert.IsType<IntegerRedisMessage>(response);
                Assert.Equal(2, integer.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies EXISTS returns correct counts for single and multiple keys.
        /// </summary>
        public async Task Exists_SingleAndMulti_ReturnsCounts()
        {
            var store = new InMemoryKeyValueStore();
            store.Seed("a", "1");
            store.Seed("b", "2");
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("EXISTS", "a"));
                channel.RunPendingTasks();

                var singleResponse = ReadOutbound(channel);
                var single = Assert.IsType<IntegerRedisMessage>(singleResponse);
                Assert.Equal(1, single.Value);

                channel.WriteInbound(Command("EXISTS", "a", "b", "c"));
                channel.RunPendingTasks();

                var multiResponse = ReadOutbound(channel);
                var multi = Assert.IsType<IntegerRedisMessage>(multiResponse);
                Assert.Equal(2, multi.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies INCR/DECR/INCRBY change values as expected.
        /// </summary>
        public async Task Incr_Decr_RoundTrip()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("INCR", "counter"));
                channel.RunPendingTasks();

                var incrResponse = ReadOutbound(channel);
                var incr = Assert.IsType<IntegerRedisMessage>(incrResponse);
                Assert.Equal(1, incr.Value);

                channel.WriteInbound(Command("DECR", "counter"));
                channel.RunPendingTasks();

                var decrResponse = ReadOutbound(channel);
                var decr = Assert.IsType<IntegerRedisMessage>(decrResponse);
                Assert.Equal(0, decr.Value);

                channel.WriteInbound(Command("INCRBY", "counter", "5"));
                channel.RunPendingTasks();

                var incrByResponse = ReadOutbound(channel);
                var incrBy = Assert.IsType<IntegerRedisMessage>(incrByResponse);
                Assert.Equal(5, incrBy.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies EXPIRE and TTL report remaining seconds.
        /// </summary>
        public async Task Expire_Ttl_ReturnsRemainingSeconds()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("SET", "temp", "value"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("EXPIRE", "temp", "2"));
                channel.RunPendingTasks();

                var expireResponse = ReadOutbound(channel);
                var expire = Assert.IsType<IntegerRedisMessage>(expireResponse);
                Assert.Equal(1, expire.Value);

                channel.WriteInbound(Command("TTL", "temp"));
                channel.RunPendingTasks();

                var ttlResponse = ReadOutbound(channel);
                var ttl = Assert.IsType<IntegerRedisMessage>(ttlResponse);
                Assert.InRange(ttl.Value, 0, 2);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies PEXPIRE and PTTL report remaining milliseconds.
        /// </summary>
        public async Task PExpire_Pttl_ReturnsRemainingMilliseconds()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("SET", "temp", "value"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("PEXPIRE", "temp", "1500"));
                channel.RunPendingTasks();

                var expireResponse = ReadOutbound(channel);
                var expire = Assert.IsType<IntegerRedisMessage>(expireResponse);
                Assert.Equal(1, expire.Value);

                channel.WriteInbound(Command("PTTL", "temp"));
                channel.RunPendingTasks();

                var ttlResponse = ReadOutbound(channel);
                var ttl = Assert.IsType<IntegerRedisMessage>(ttlResponse);
                Assert.InRange(ttl.Value, 0, 1500);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies TTL returns -2 for missing keys.
        /// </summary>
        public async Task Ttl_MissingKey_ReturnsMinusTwo()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("TTL", "missing"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var ttl = Assert.IsType<IntegerRedisMessage>(response);
                Assert.Equal(-2, ttl.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies HSET/HGET round-trip a field value.
        /// </summary>
        public async Task HSet_HGet_RoundTrip()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("HSET", "hash", "field", "value"));
                channel.RunPendingTasks();

                var setResponse = ReadOutbound(channel);
                var setCount = Assert.IsType<IntegerRedisMessage>(setResponse);
                Assert.Equal(1, setCount.Value);

                channel.WriteInbound(Command("HGET", "hash", "field"));
                channel.RunPendingTasks();

                var getResponse = ReadOutbound(channel);
                var bulk = Assert.IsType<FullBulkStringRedisMessage>(getResponse);
                Assert.Equal("value", GetBulkString(bulk));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies HSET returns zero when updating existing field.
        /// </summary>
        public async Task HSet_UpdateField_ReturnsZero()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("HSET", "hash", "field", "first"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("HSET", "hash", "field", "second"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var count = Assert.IsType<IntegerRedisMessage>(response);
                Assert.Equal(0, count.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies HDEL removes fields and returns removed count.
        /// </summary>
        public async Task HDel_RemovesFields_ReturnsCount()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("HSET", "hash", "a", "1", "b", "2"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("HDEL", "hash", "a", "b", "c"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var count = Assert.IsType<IntegerRedisMessage>(response);
                Assert.Equal(2, count.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies HGETALL returns all field/value pairs.
        /// </summary>
        public async Task HGetAll_ReturnsAllPairs()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("HSET", "hash", "a", "1", "b", "2"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("HGETALL", "hash"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                Assert.Equal(4, array.Children.Count);

                var pairs = GetHashPairs(array.Children);
                Assert.Equal("1", pairs["a"]);
                Assert.Equal("2", pairs["b"]);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies LPUSH/RPUSH update list length and order.
        /// </summary>
        public async Task ListPush_ReturnsLengthAndOrder()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("LPUSH", "list", "a", "b"));
                channel.RunPendingTasks();
                var len = Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel));
                Assert.Equal(2, len.Value);

                channel.WriteInbound(Command("RPUSH", "list", "c"));
                channel.RunPendingTasks();
                var len2 = Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel));
                Assert.Equal(3, len2.Value);

                channel.WriteInbound(Command("LRANGE", "list", "0", "-1"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                Assert.Equal(3, array.Children.Count);
                Assert.Equal("b", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(array.Children[0])));
                Assert.Equal("a", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(array.Children[1])));
                Assert.Equal("c", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(array.Children[2])));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies LPOP/RPOP return values and handle empty lists.
        /// </summary>
        public async Task ListPop_ReturnsValues()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("RPUSH", "list", "a", "b"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("LPOP", "list"));
                channel.RunPendingTasks();
                var left = Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel));
                Assert.Equal("a", GetBulkString(left));

                channel.WriteInbound(Command("RPOP", "list"));
                channel.RunPendingTasks();
                var right = Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel));
                Assert.Equal("b", GetBulkString(right));

                channel.WriteInbound(Command("LPOP", "list"));
                channel.RunPendingTasks();
                var empty = ReadOutbound(channel);
                Assert.True(ReferenceEquals(empty, FullBulkStringRedisMessage.Null));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies LRANGE honors negative indices.
        /// </summary>
        public async Task ListRange_NegativeIndices()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("RPUSH", "list", "a", "b", "c", "d"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("LRANGE", "list", "-3", "-2"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                Assert.Equal(2, array.Children.Count);
                Assert.Equal("b", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(array.Children[0])));
                Assert.Equal("c", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(array.Children[1])));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies list commands return WRONGTYPE for non-list keys.
        /// </summary>
        public async Task List_WrongType_ReturnsError()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("SET", "list", "value"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("LPUSH", "list", "a"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var error = Assert.IsType<ErrorRedisMessage>(response);
                Assert.Equal("WRONGTYPE Operation against a key holding the wrong kind of value", error.Content);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies LLEN returns list length or zero for missing lists.
        /// </summary>
        public async Task ListLength_ReturnsLength()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("LLEN", "list"));
                channel.RunPendingTasks();
                var emptyLength = Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel));
                Assert.Equal(0, emptyLength.Value);

                channel.WriteInbound(Command("RPUSH", "list", "a", "b", "c"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("LLEN", "list"));
                channel.RunPendingTasks();
                var length = Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel));
                Assert.Equal(3, length.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies LINDEX returns values and null for out of range.
        /// </summary>
        public async Task ListIndex_ReturnsValue()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("RPUSH", "list", "a", "b", "c"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("LINDEX", "list", "1"));
                channel.RunPendingTasks();
                var value = Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel));
                Assert.Equal("b", GetBulkString(value));

                channel.WriteInbound(Command("LINDEX", "list", "-1"));
                channel.RunPendingTasks();
                var last = Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel));
                Assert.Equal("c", GetBulkString(last));

                channel.WriteInbound(Command("LINDEX", "list", "10"));
                channel.RunPendingTasks();
                var missing = ReadOutbound(channel);
                Assert.True(ReferenceEquals(missing, FullBulkStringRedisMessage.Null));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies LSET updates a list index and errors on out of range.
        /// </summary>
        public async Task ListSet_UpdatesIndex()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("RPUSH", "list", "a", "b", "c"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("LSET", "list", "1", "z"));
                channel.RunPendingTasks();
                var ok = Assert.IsType<SimpleStringRedisMessage>(ReadOutbound(channel));
                Assert.Equal("OK", ok.Content);

                channel.WriteInbound(Command("LINDEX", "list", "1"));
                channel.RunPendingTasks();
                var value = Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel));
                Assert.Equal("z", GetBulkString(value));

                channel.WriteInbound(Command("LSET", "list", "10", "x"));
                channel.RunPendingTasks();
                var error = Assert.IsType<ErrorRedisMessage>(ReadOutbound(channel));
                Assert.Equal("ERR index out of range", error.Content);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies LTRIM keeps the requested range.
        /// </summary>
        public async Task ListTrim_TrimsList()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("RPUSH", "list", "a", "b", "c", "d"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("LTRIM", "list", "1", "2"));
                channel.RunPendingTasks();
                var ok = Assert.IsType<SimpleStringRedisMessage>(ReadOutbound(channel));
                Assert.Equal("OK", ok.Content);

                channel.WriteInbound(Command("LRANGE", "list", "0", "-1"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                Assert.Equal(2, array.Children.Count);
                Assert.Equal("b", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(array.Children[0])));
                Assert.Equal("c", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(array.Children[1])));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies SADD returns added count and SMEMBERS returns all members.
        /// </summary>
        public async Task SetAdd_ReturnsCountAndMembers()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("SADD", "set", "a", "b", "a"));
                channel.RunPendingTasks();
                var added = Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel));
                Assert.Equal(2, added.Value);

                channel.WriteInbound(Command("SMEMBERS", "set"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                var members = array.Children
                    .Select(msg => GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(msg)))
                    .ToHashSet(StringComparer.Ordinal);

                Assert.Contains("a", members);
                Assert.Contains("b", members);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies SCARD returns set cardinality.
        /// </summary>
        public async Task SetCardinality_ReturnsCount()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("SCARD", "set"));
                channel.RunPendingTasks();
                var empty = Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel));
                Assert.Equal(0, empty.Value);

                channel.WriteInbound(Command("SADD", "set", "a", "b"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("SCARD", "set"));
                channel.RunPendingTasks();
                var count = Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel));
                Assert.Equal(2, count.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies SREM removes members and returns count removed.
        /// </summary>
        public async Task SetRemove_RemovesMembers()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("SADD", "set", "a", "b"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("SREM", "set", "a", "c"));
                channel.RunPendingTasks();
                var removed = Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel));
                Assert.Equal(1, removed.Value);

                channel.WriteInbound(Command("SMEMBERS", "set"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                Assert.Single(array.Children);
                var member = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(array.Children[0]));
                Assert.Equal("b", member);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies set commands return WRONGTYPE for non-set keys.
        /// </summary>
        public async Task Set_WrongType_ReturnsError()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("SET", "set", "value"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("SADD", "set", "a"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);
                var error = Assert.IsType<ErrorRedisMessage>(response);
                Assert.Equal("WRONGTYPE Operation against a key holding the wrong kind of value", error.Content);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies ZADD returns count and ZRANGE orders by score.
        /// </summary>
        public async Task SortedSet_AddAndRange_ReturnsOrderedMembers()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("ZADD", "zset", "1", "one", "0", "zero", "1", "two"));
                channel.RunPendingTasks();
                var added = Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel));
                Assert.Equal(3, added.Value);

                channel.WriteInbound(Command("ZRANGE", "zset", "0", "-1"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                Assert.Equal(3, array.Children.Count);
                Assert.Equal("zero", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(array.Children[0])));
                Assert.Equal("one", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(array.Children[1])));
                Assert.Equal("two", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(array.Children[2])));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies ZRANGE WITHSCORES returns score pairs.
        /// </summary>
        public async Task SortedSet_Range_WithScores_ReturnsPairs()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("ZADD", "zset", "2.5", "a"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("ZRANGE", "zset", "0", "-1", "WITHSCORES"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                Assert.Equal(2, array.Children.Count);
                Assert.Equal("a", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(array.Children[0])));
                Assert.Equal("2.5", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(array.Children[1])));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies ZREM removes members and ZCARD returns count.
        /// </summary>
        public async Task SortedSet_RemoveAndCardinality()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("ZADD", "zset", "1", "a", "2", "b"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("ZREM", "zset", "a", "c"));
                channel.RunPendingTasks();
                var removed = Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel));
                Assert.Equal(1, removed.Value);

                channel.WriteInbound(Command("ZCARD", "zset"));
                channel.RunPendingTasks();
                var count = Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel));
                Assert.Equal(1, count.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies sorted set commands return WRONGTYPE for non-zset keys.
        /// </summary>
        public async Task SortedSet_WrongType_ReturnsError()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("SET", "zset", "value"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("ZADD", "zset", "1", "a"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);
                var error = Assert.IsType<ErrorRedisMessage>(response);
                Assert.Equal("WRONGTYPE Operation against a key holding the wrong kind of value", error.Content);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task SortedSet_ZScore_ReturnsScore()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("ZADD", "zset", "1.5", "member1"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("ZSCORE", "zset", "member1"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);
                var bulk = Assert.IsType<FullBulkStringRedisMessage>(response);
                Assert.Equal("1.5", GetBulkString(bulk));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task SortedSet_ZScore_NonExistent_ReturnsNull()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("ZADD", "zset", "1.5", "member1"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("ZSCORE", "zset", "nonexistent"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);
                Assert.Same(FullBulkStringRedisMessage.Null, response);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task SortedSet_ZRangeByScore_ReturnsEntriesInRange()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("ZADD", "zset", "1", "one", "2", "two", "3", "three"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("ZRANGEBYSCORE", "zset", "1", "2"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                Assert.Equal(2, array.Children.Count);
                
                var child0 = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(array.Children[0]));
                Assert.Equal("one", child0);
                
                var child1 = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(array.Children[1]));
                Assert.Equal("two", child1);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task SortedSet_ZRangeByScore_WithScores()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("ZADD", "zset", "1", "one", "2", "two"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("ZRANGEBYSCORE", "zset", "1", "2", "WITHSCORES"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                Assert.Equal(4, array.Children.Count);
                
                var member0 = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(array.Children[0]));
                Assert.Equal("one", member0);
                
                var score0 = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(array.Children[1]));
                Assert.Equal("1", score0);
                
                var member1 = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(array.Children[2]));
                Assert.Equal("two", member1);
                
                var score1 = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(array.Children[3]));
                Assert.Equal("2", score1);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task SortedSet_ZIncrBy_IncrementsScore()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("ZADD", "zset", "1", "member1"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("ZINCRBY", "zset", "2.5", "member1"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);
                var bulk = Assert.IsType<FullBulkStringRedisMessage>(response);
                Assert.Equal("3.5", GetBulkString(bulk));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task SortedSet_ZIncrBy_NonExistentMember_InitializesScore()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("ZINCRBY", "zset", "5", "member1"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);
                var bulk = Assert.IsType<FullBulkStringRedisMessage>(response);
                Assert.Equal("5", GetBulkString(bulk));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task SortedSet_ZCount_CountsMembersInRange()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("ZADD", "zset", "1", "one", "2", "two", "3", "three", "4", "four"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("ZCOUNT", "zset", "2", "3"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);
                var integer = Assert.IsType<IntegerRedisMessage>(response);
                Assert.Equal(2, integer.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task SortedSet_ZCount_NonExistentKey_ReturnsZero()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("ZCOUNT", "nonexistent", "1", "10"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);
                var integer = Assert.IsType<IntegerRedisMessage>(response);
                Assert.Equal(0, integer.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task SortedSet_ZRank_ReturnsRankAscending()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("ZADD", "zset", "1", "one", "2", "two", "3", "three"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("ZRANK", "zset", "two"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);
                var integer = Assert.IsType<IntegerRedisMessage>(response);
                Assert.Equal(1, integer.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task SortedSet_ZRank_NonExistentMember_ReturnsNull()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("ZADD", "zset", "1", "one"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("ZRANK", "zset", "nonexistent"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);
                Assert.Same(FullBulkStringRedisMessage.Null, response);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task SortedSet_ZRevRank_ReturnsRankDescending()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("ZADD", "zset", "1", "one", "2", "two", "3", "three"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("ZREVRANK", "zset", "two"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);
                var integer = Assert.IsType<IntegerRedisMessage>(response);
                Assert.Equal(1, integer.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task SortedSet_ZRevRank_NonExistentMember_ReturnsNull()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("ZADD", "zset", "1", "one"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("ZREVRANK", "zset", "nonexistent"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);
                Assert.Same(FullBulkStringRedisMessage.Null, response);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task SortedSet_ZRemRangeByScore_RemovesAndReturnsCount()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("ZADD", "zset", "1", "one", "2", "two", "3", "three", "4", "four"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("ZREMRANGEBYSCORE", "zset", "2", "3"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);
                var integer = Assert.IsType<IntegerRedisMessage>(response);
                Assert.Equal(2, integer.Value);

                // Verify they were actually removed
                channel.WriteInbound(Command("ZCOUNT", "zset", "0", "10"));
                channel.RunPendingTasks();
                var countResponse = ReadOutbound(channel);
                var count = Assert.IsType<IntegerRedisMessage>(countResponse);
                Assert.Equal(2, count.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task SortedSet_ZRemRangeByScore_NonExistentKey_ReturnsZero()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("ZREMRANGEBYSCORE", "nonexistent", "1", "10"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);
                var integer = Assert.IsType<IntegerRedisMessage>(response);
                Assert.Equal(0, integer.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task Publish_NoSubscribers_ReturnsZero()
        {
            DredisCommandHandler.PubSubManager.Clear();
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("PUBLISH", "channel1", "hello"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);
                var integer = Assert.IsType<IntegerRedisMessage>(response);
                Assert.Equal(0, integer.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task Subscribe_SendsSubscriptionConfirmation()
        {
            DredisCommandHandler.PubSubManager.Clear();
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("SUBSCRIBE", "channel1", "channel2"));
                channel.RunPendingTasks();

                // First subscription confirmation
                var response1 = ReadOutbound(channel);
                var array1 = Assert.IsType<ArrayRedisMessage>(response1);
                Assert.Equal(3, array1.Children.Count);

                var type1 = Assert.IsType<FullBulkStringRedisMessage>(array1.Children[0]);
                AssertBulkStringEqual(type1, "subscribe");

                var channel_name1 = Assert.IsType<FullBulkStringRedisMessage>(array1.Children[1]);
                AssertBulkStringEqual(channel_name1, "channel1");

                var count1 = Assert.IsType<IntegerRedisMessage>(array1.Children[2]);
                Assert.Equal(1, count1.Value);

                // Second subscription confirmation
                var response2 = ReadOutbound(channel);
                var array2 = Assert.IsType<ArrayRedisMessage>(response2);
                Assert.Equal(3, array2.Children.Count);

                var type2 = Assert.IsType<FullBulkStringRedisMessage>(array2.Children[0]);
                AssertBulkStringEqual(type2, "subscribe");

                var channel_name2 = Assert.IsType<FullBulkStringRedisMessage>(array2.Children[1]);
                AssertBulkStringEqual(channel_name2, "channel2");

                var count2 = Assert.IsType<IntegerRedisMessage>(array2.Children[2]);
                Assert.Equal(2, count2.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task Publish_WithSubscriber_ReturnCountAndDeliversMessage()
        {
            DredisCommandHandler.PubSubManager.Clear();
            var store = new InMemoryKeyValueStore();
            var publisher = new EmbeddedChannel(new DredisCommandHandler(store));
            var subscriber = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                // Subscriber subscribes to channel
                subscriber.WriteInbound(Command("SUBSCRIBE", "events"));
                subscriber.RunPendingTasks();
                
                // Read subscription confirmation
                _ = ReadOutbound(subscriber);

                // Publisher publishes a message
                publisher.WriteInbound(Command("PUBLISH", "events", "hello"));
                publisher.RunPendingTasks();
                
                // Publisher gets subscriber count
                var publishResponse = ReadOutbound(publisher);
                var integer = Assert.IsType<IntegerRedisMessage>(publishResponse);
                Assert.Equal(1, integer.Value);

                // Subscriber receives message
                var messageResponse = ReadOutbound(subscriber);
                var messageArray = Assert.IsType<ArrayRedisMessage>(messageResponse);
                Assert.Equal(3, messageArray.Children.Count);

                var messageType = Assert.IsType<FullBulkStringRedisMessage>(messageArray.Children[0]);
                AssertBulkStringEqual(messageType, "message");

                var channelName = Assert.IsType<FullBulkStringRedisMessage>(messageArray.Children[1]);
                AssertBulkStringEqual(channelName, "events");

                var messageData = Assert.IsType<FullBulkStringRedisMessage>(messageArray.Children[2]);
                AssertBulkStringEqual(messageData, "hello");
            }
            finally
            {
                await publisher.CloseAsync();
                await subscriber.CloseAsync();
            }
        }

        [Fact]
        public async Task Unsubscribe_FromSpecificChannels()
        {
            DredisCommandHandler.PubSubManager.Clear();
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                // Subscribe to channels
                channel.WriteInbound(Command("SUBSCRIBE", "channel1", "channel2", "channel3"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel); // subscription confirmation for channel1
                _ = ReadOutbound(channel); // subscription confirmation for channel2
                _ = ReadOutbound(channel); // subscription confirmation for channel3

                // Unsubscribe from channel1 and channel2
                channel.WriteInbound(Command("UNSUBSCRIBE", "channel1", "channel2"));
                channel.RunPendingTasks();

                // First unsubscribe confirmation
                var response1 = ReadOutbound(channel);
                var array1 = Assert.IsType<ArrayRedisMessage>(response1);
                Assert.Equal(3, array1.Children.Count);

                var type1 = Assert.IsType<FullBulkStringRedisMessage>(array1.Children[0]);
                AssertBulkStringEqual(type1, "unsubscribe");

                var channel_name1 = Assert.IsType<FullBulkStringRedisMessage>(array1.Children[1]);
                AssertBulkStringEqual(channel_name1, "channel1");

                var count1 = Assert.IsType<IntegerRedisMessage>(array1.Children[2]);
                Assert.Equal(2, count1.Value); // 2 channels remaining

                // Second unsubscribe confirmation
                var response2 = ReadOutbound(channel);
                var array2 = Assert.IsType<ArrayRedisMessage>(response2);
                Assert.Equal(3, array2.Children.Count);

                var type2 = Assert.IsType<FullBulkStringRedisMessage>(array2.Children[0]);
                AssertBulkStringEqual(type2, "unsubscribe");

                var channel_name2 = Assert.IsType<FullBulkStringRedisMessage>(array2.Children[1]);
                AssertBulkStringEqual(channel_name2, "channel2");

                var count2 = Assert.IsType<IntegerRedisMessage>(array2.Children[2]);
                Assert.Equal(1, count2.Value); // 1 channel remaining
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task Unsubscribe_FromAllChannels()
        {
            DredisCommandHandler.PubSubManager.Clear();
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                // Subscribe to channels
                channel.WriteInbound(Command("SUBSCRIBE", "channel1", "channel2"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);
                _ = ReadOutbound(channel);

                // Unsubscribe from all (no arguments)
                channel.WriteInbound(Command("UNSUBSCRIBE"));
                channel.RunPendingTasks();

                // Should receive two unsubscribe confirmations
                var response1 = ReadOutbound(channel);
                var array1 = Assert.IsType<ArrayRedisMessage>(response1);
                var type1 = Assert.IsType<FullBulkStringRedisMessage>(array1.Children[0]);
                AssertBulkStringEqual(type1, "unsubscribe");

                var response2 = ReadOutbound(channel);
                var array2 = Assert.IsType<ArrayRedisMessage>(response2);
                var type2 = Assert.IsType<FullBulkStringRedisMessage>(array2.Children[0]);
                AssertBulkStringEqual(type2, "unsubscribe");
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task PSubscribe_SendsSubscriptionConfirmation()
        {
            DredisCommandHandler.PubSubManager.Clear();
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("PSUBSCRIBE", "news.*", "events.*"));
                channel.RunPendingTasks();

                // First subscription confirmation
                var response1 = ReadOutbound(channel);
                var array1 = Assert.IsType<ArrayRedisMessage>(response1);
                Assert.Equal(3, array1.Children.Count);

                var type1 = Assert.IsType<FullBulkStringRedisMessage>(array1.Children[0]);
                AssertBulkStringEqual(type1, "psubscribe");

                var pattern1 = Assert.IsType<FullBulkStringRedisMessage>(array1.Children[1]);
                AssertBulkStringEqual(pattern1, "news.*");

                var count1 = Assert.IsType<IntegerRedisMessage>(array1.Children[2]);
                Assert.Equal(1, count1.Value);

                // Second subscription confirmation
                var response2 = ReadOutbound(channel);
                var array2 = Assert.IsType<ArrayRedisMessage>(response2);
                Assert.Equal(3, array2.Children.Count);

                var type2 = Assert.IsType<FullBulkStringRedisMessage>(array2.Children[0]);
                AssertBulkStringEqual(type2, "psubscribe");

                var pattern2 = Assert.IsType<FullBulkStringRedisMessage>(array2.Children[1]);
                AssertBulkStringEqual(pattern2, "events.*");

                var count2 = Assert.IsType<IntegerRedisMessage>(array2.Children[2]);
                Assert.Equal(2, count2.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task Publish_WithPatternSubscriber_DeliversPMessage()
        {
            DredisCommandHandler.PubSubManager.Clear();
            var store = new InMemoryKeyValueStore();
            var publisher = new EmbeddedChannel(new DredisCommandHandler(store));
            var subscriber = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                // Subscriber subscribes to pattern
                subscriber.WriteInbound(Command("PSUBSCRIBE", "news.*"));
                subscriber.RunPendingTasks();
                _ = ReadOutbound(subscriber); // subscription confirmation

                // Publisher publishes to matching channel
                publisher.WriteInbound(Command("PUBLISH", "news.sports", "breaking news"));
                publisher.RunPendingTasks();
                _ = ReadOutbound(publisher); // subscriber count

                // Subscriber receives pmessage
                var messageResponse = ReadOutbound(subscriber);
                var messageArray = Assert.IsType<ArrayRedisMessage>(messageResponse);
                Assert.Equal(4, messageArray.Children.Count);

                var messageType = Assert.IsType<FullBulkStringRedisMessage>(messageArray.Children[0]);
                AssertBulkStringEqual(messageType, "pmessage");

                var pattern = Assert.IsType<FullBulkStringRedisMessage>(messageArray.Children[1]);
                AssertBulkStringEqual(pattern, "news.*");

                var channelName = Assert.IsType<FullBulkStringRedisMessage>(messageArray.Children[2]);
                AssertBulkStringEqual(channelName, "news.sports");

                var messageData = Assert.IsType<FullBulkStringRedisMessage>(messageArray.Children[3]);
                AssertBulkStringEqual(messageData, "breaking news");
            }
            finally
            {
                await publisher.CloseAsync();
                await subscriber.CloseAsync();
            }
        }

        [Fact]
        public async Task PUnsubscribe_FromSpecificPatterns()
        {
            DredisCommandHandler.PubSubManager.Clear();
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                // Subscribe to patterns
                channel.WriteInbound(Command("PSUBSCRIBE", "news.*", "events.*", "alerts.*"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);
                _ = ReadOutbound(channel);
                _ = ReadOutbound(channel);

                // Unsubscribe from specific patterns
                channel.WriteInbound(Command("PUNSUBSCRIBE", "news.*"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                Assert.Equal(3, array.Children.Count);

                var type = Assert.IsType<FullBulkStringRedisMessage>(array.Children[0]);
                AssertBulkStringEqual(type, "punsubscribe");

                var pattern = Assert.IsType<FullBulkStringRedisMessage>(array.Children[1]);
                AssertBulkStringEqual(pattern, "news.*");

                var count = Assert.IsType<IntegerRedisMessage>(array.Children[2]);
                Assert.Equal(2, count.Value); // 2 patterns remaining
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task PUnsubscribe_FromAllPatterns()
        {
            DredisCommandHandler.PubSubManager.Clear();
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                // Subscribe to patterns
                channel.WriteInbound(Command("PSUBSCRIBE", "news.*", "events.*"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);
                _ = ReadOutbound(channel);

                // Unsubscribe from all (no arguments)
                channel.WriteInbound(Command("PUNSUBSCRIBE"));
                channel.RunPendingTasks();

                // Should receive two punsubscribe confirmations
                var response1 = ReadOutbound(channel);
                var array1 = Assert.IsType<ArrayRedisMessage>(response1);
                var type1 = Assert.IsType<FullBulkStringRedisMessage>(array1.Children[0]);
                AssertBulkStringEqual(type1, "punsubscribe");

                var response2 = ReadOutbound(channel);
                var array2 = Assert.IsType<ArrayRedisMessage>(response2);
                var type2 = Assert.IsType<FullBulkStringRedisMessage>(array2.Children[0]);
                AssertBulkStringEqual(type2, "punsubscribe");
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task PatternMatching_WildcardAndQuestionMark()
        {
            DredisCommandHandler.PubSubManager.Clear();
            var store = new InMemoryKeyValueStore();
            var subscriber = new EmbeddedChannel(new DredisCommandHandler(store));
            var publisher = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                // Subscribe to pattern with ? and *
                subscriber.WriteInbound(Command("PSUBSCRIBE", "h?llo", "h*o"));
                subscriber.RunPendingTasks();
                _ = ReadOutbound(subscriber);
                _ = ReadOutbound(subscriber);

                // Test ? pattern (matches single character)
                publisher.WriteInbound(Command("PUBLISH", "hello", "msg1"));
                publisher.RunPendingTasks();
                _ = ReadOutbound(publisher);

                var msg1 = ReadOutbound(subscriber);
                var array1 = Assert.IsType<ArrayRedisMessage>(msg1);
                var pattern1 = Assert.IsType<FullBulkStringRedisMessage>(array1.Children[1]);
                AssertBulkStringEqual(pattern1, "h?llo");

                // Test * pattern (matches multiple characters)
                publisher.WriteInbound(Command("PUBLISH", "heeeeello", "msg2"));
                publisher.RunPendingTasks();
                _ = ReadOutbound(publisher);

                var msg2 = ReadOutbound(subscriber);
                var array2 = Assert.IsType<ArrayRedisMessage>(msg2);
                var pattern2 = Assert.IsType<FullBulkStringRedisMessage>(array2.Children[1]);
                AssertBulkStringEqual(pattern2, "h*o");
            }
            finally
            {
                await subscriber.CloseAsync();
                await publisher.CloseAsync();
            }
        }

        [Fact]
        public async Task Multi_Exec_ExecutesQueuedCommands()
        {
            DredisCommandHandler.TransactionManager.Clear();
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                // Start transaction
                channel.WriteInbound(Command("MULTI"));
                channel.RunPendingTasks();
                var multiResponse = ReadOutbound(channel);
                var multiStr = Assert.IsType<SimpleStringRedisMessage>(multiResponse);
                Assert.Equal("OK", multiStr.Content);

                // Queue commands
                channel.WriteInbound(Command("SET", "key1", "value1"));
                channel.RunPendingTasks();
                var queued1 = ReadOutbound(channel);
                var q1 = Assert.IsType<SimpleStringRedisMessage>(queued1);
                Assert.Equal("QUEUED", q1.Content);

                channel.WriteInbound(Command("SET", "key2", "value2"));
                channel.RunPendingTasks();
                var queued2 = ReadOutbound(channel);
                var q2 = Assert.IsType<SimpleStringRedisMessage>(queued2);
                Assert.Equal("QUEUED", q2.Content);

                channel.WriteInbound(Command("GET", "key1"));
                channel.RunPendingTasks();
                var queued3 = ReadOutbound(channel);
                var q3 = Assert.IsType<SimpleStringRedisMessage>(queued3);
                Assert.Equal("QUEUED", q3.Content);

                // Execute transaction
                channel.WriteInbound(Command("EXEC"));
                channel.RunPendingTasks();
                var execResponse = ReadOutbound(channel);
                var execArray = Assert.IsType<ArrayRedisMessage>(execResponse);
                Assert.Equal(3, execArray.Children.Count);

                // Verify results
                var result1 = Assert.IsType<SimpleStringRedisMessage>(execArray.Children[0]);
                Assert.Equal("OK", result1.Content);

                var result2 = Assert.IsType<SimpleStringRedisMessage>(execArray.Children[1]);
                Assert.Equal("OK", result2.Content);

                var result3 = Assert.IsType<FullBulkStringRedisMessage>(execArray.Children[2]);
                AssertBulkStringEqual(result3, "value1");
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task Discard_CancelsTransaction()
        {
            DredisCommandHandler.TransactionManager.Clear();
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                // Start transaction
                channel.WriteInbound(Command("MULTI"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                // Queue commands
                channel.WriteInbound(Command("SET", "key1", "value1"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                // Discard transaction
                channel.WriteInbound(Command("DISCARD"));
                channel.RunPendingTasks();
                var discardResponse = ReadOutbound(channel);
                var discardStr = Assert.IsType<SimpleStringRedisMessage>(discardResponse);
                Assert.Equal("OK", discardStr.Content);

                // Verify key was not set
                channel.WriteInbound(Command("GET", "key1"));
                channel.RunPendingTasks();
                var getResponse = ReadOutbound(channel);
                Assert.Same(FullBulkStringRedisMessage.Null, getResponse);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task Watch_PreventsExecWhenKeyModified()
        {
            DredisCommandHandler.TransactionManager.Clear();
            var store = new InMemoryKeyValueStore();
            var channel1 = new EmbeddedChannel(new DredisCommandHandler(store));
            var channel2 = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                // Set initial value
                channel1.WriteInbound(Command("SET", "mykey", "10"));
                channel1.RunPendingTasks();
                _ = ReadOutbound(channel1);

                // Client 1 watches the key
                channel1.WriteInbound(Command("WATCH", "mykey"));
                channel1.RunPendingTasks();
                var watchResponse = ReadOutbound(channel1);
                var watchStr = Assert.IsType<SimpleStringRedisMessage>(watchResponse);
                Assert.Equal("OK", watchStr.Content);

                // Client 2 modifies the key
                channel2.WriteInbound(Command("SET", "mykey", "20"));
                channel2.RunPendingTasks();
                _ = ReadOutbound(channel2);

                // Client 1 starts transaction
                channel1.WriteInbound(Command("MULTI"));
                channel1.RunPendingTasks();
                _ = ReadOutbound(channel1);

                // Queue command
                channel1.WriteInbound(Command("SET", "mykey", "30"));
                channel1.RunPendingTasks();
                _ = ReadOutbound(channel1);

                // Execute - should fail because key was modified
                channel1.WriteInbound(Command("EXEC"));
                channel1.RunPendingTasks();
                var execResponse = ReadOutbound(channel1);
                
                // Should return null to indicate transaction was aborted
                Assert.Same(FullBulkStringRedisMessage.Null, execResponse);

                // Verify key still has value from client 2
                channel1.WriteInbound(Command("GET", "mykey"));
                channel1.RunPendingTasks();
                var getResponse = ReadOutbound(channel1);
                var getValue = Assert.IsType<FullBulkStringRedisMessage>(getResponse);
                AssertBulkStringEqual(getValue, "20");
            }
            finally
            {
                await channel1.CloseAsync();
                await channel2.CloseAsync();
            }
        }

        [Fact]
        public async Task Unwatch_ClearsWatchedKeys()
        {
            DredisCommandHandler.TransactionManager.Clear();
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                // Watch a key
                channel.WriteInbound(Command("WATCH", "mykey"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                // Unwatch all keys
                channel.WriteInbound(Command("UNWATCH"));
                channel.RunPendingTasks();
                var unwatchResponse = ReadOutbound(channel);
                var unwatchStr = Assert.IsType<SimpleStringRedisMessage>(unwatchResponse);
                Assert.Equal("OK", unwatchStr.Content);

                // Verify transaction can proceed even after key modification
                channel.WriteInbound(Command("SET", "mykey", "value"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("MULTI"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("GET", "mykey"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("EXEC"));
                channel.RunPendingTasks();
                var execResponse = ReadOutbound(channel);
                var execArray = Assert.IsType<ArrayRedisMessage>(execResponse);
                Assert.Equal(1, execArray.Children.Count);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task Multi_WithoutExec_ReturnsError()
        {
            DredisCommandHandler.TransactionManager.Clear();
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                // Try EXEC without MULTI
                channel.WriteInbound(Command("EXEC"));
                channel.RunPendingTasks();
                var execResponse = ReadOutbound(channel);
                var error = Assert.IsType<ErrorRedisMessage>(execResponse);
                Assert.Contains("EXEC without MULTI", error.Content);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task Multi_Nested_ReturnsError()
        {
            DredisCommandHandler.TransactionManager.Clear();
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                // Start transaction
                channel.WriteInbound(Command("MULTI"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                // Try nested MULTI
                channel.WriteInbound(Command("MULTI"));
                channel.RunPendingTasks();
                var multiResponse = ReadOutbound(channel);
                var error = Assert.IsType<ErrorRedisMessage>(multiResponse);
                Assert.Contains("MULTI calls can not be nested", error.Content);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies XADD and XLEN return expected stream length.
        /// </summary>
        public async Task XAdd_XLen_ReturnsCount()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("XADD", "stream", "*", "field", "value"));
                channel.RunPendingTasks();

                var addResponse = ReadOutbound(channel);
                var addId = Assert.IsType<FullBulkStringRedisMessage>(addResponse);
                Assert.Contains("-", GetBulkString(addId));

                channel.WriteInbound(Command("XLEN", "stream"));
                channel.RunPendingTasks();

                var lenResponse = ReadOutbound(channel);
                var len = Assert.IsType<IntegerRedisMessage>(lenResponse);
                Assert.Equal(1, len.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies XTRIM MAXLEN removes the oldest entries.
        /// </summary>
        public async Task XTrim_MaxLen_RemovesOldest()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("XADD", "stream", "*", "a", "1"));
                channel.RunPendingTasks();
                var id1 = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                channel.WriteInbound(Command("XADD", "stream", "*", "b", "2"));
                channel.RunPendingTasks();
                var id2 = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                channel.WriteInbound(Command("XADD", "stream", "*", "c", "3"));
                channel.RunPendingTasks();
                var id3 = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                channel.WriteInbound(Command("XTRIM", "stream", "MAXLEN", "2"));
                channel.RunPendingTasks();

                var trimResponse = ReadOutbound(channel);
                var trimmed = Assert.IsType<IntegerRedisMessage>(trimResponse);
                Assert.Equal(1, trimmed.Value);

                channel.WriteInbound(Command("XRANGE", "stream", "-", "+"));
                channel.RunPendingTasks();

                var rangeResponse = ReadOutbound(channel);
                var rangeArray = Assert.IsType<ArrayRedisMessage>(rangeResponse);
                var entries = GetStreamEntries(rangeArray.Children);
                Assert.Equal(2, entries.Count);
                Assert.False(entries.ContainsKey(id1));
                Assert.True(entries.ContainsKey(id2));
                Assert.True(entries.ContainsKey(id3));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies XTRIM MINID removes entries older than the minimum id.
        /// </summary>
        public async Task XTrim_MinId_RemovesOlder()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("XADD", "stream", "*", "a", "1"));
                channel.RunPendingTasks();
                var id1 = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                channel.WriteInbound(Command("XADD", "stream", "*", "b", "2"));
                channel.RunPendingTasks();
                var id2 = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                channel.WriteInbound(Command("XADD", "stream", "*", "c", "3"));
                channel.RunPendingTasks();
                var id3 = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                channel.WriteInbound(Command("XTRIM", "stream", "MINID", id2));
                channel.RunPendingTasks();

                var trimResponse = ReadOutbound(channel);
                var trimmed = Assert.IsType<IntegerRedisMessage>(trimResponse);
                Assert.Equal(1, trimmed.Value);

                channel.WriteInbound(Command("XRANGE", "stream", "-", "+"));
                channel.RunPendingTasks();

                var rangeResponse = ReadOutbound(channel);
                var rangeArray = Assert.IsType<ArrayRedisMessage>(rangeResponse);
                var entries = GetStreamEntries(rangeArray.Children);
                Assert.Equal(2, entries.Count);
                Assert.False(entries.ContainsKey(id1));
                Assert.True(entries.ContainsKey(id2));
                Assert.True(entries.ContainsKey(id3));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies XREAD returns entries after the provided id.
        /// </summary>
        public async Task XRead_ReturnsEntriesAfterId()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("XADD", "stream", "*", "a", "1"));
                channel.RunPendingTasks();
                var firstId = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                channel.WriteInbound(Command("XADD", "stream", "*", "b", "2"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("XREAD", "STREAMS", "stream", firstId));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                Assert.Single(array.Children);

                var streamMessage = Assert.IsType<ArrayRedisMessage>(array.Children[0]);
                var entriesMessage = Assert.IsType<ArrayRedisMessage>(streamMessage.Children[1]);
                var entries = GetStreamEntries(entriesMessage.Children);

                Assert.Single(entries);
                using var enumerator = entries.GetEnumerator();
                Assert.True(enumerator.MoveNext());
                Assert.Equal("2", enumerator.Current.Value["b"]);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies XREAD BLOCK waits for entries.
        /// </summary>
        public async Task XRead_Block_WaitsForEntry()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("XREAD", "BLOCK", "50", "STREAMS", "stream", "$"));
                channel.RunPendingTasks();

                await Task.Delay(10);

                channel.WriteInbound(Command("XADD", "stream", "*", "a", "1"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                await Task.Delay(60);
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                Assert.Single(array.Children);

                var streamMessage = Assert.IsType<ArrayRedisMessage>(array.Children[0]);
                var entriesMessage = Assert.IsType<ArrayRedisMessage>(streamMessage.Children[1]);
                var entries = GetStreamEntries(entriesMessage.Children);
                Assert.Single(entries);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies XDEL removes entries and returns count.
        /// </summary>
        public async Task XDel_RemovesEntries_ReturnsCount()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("XADD", "stream", "*", "a", "1"));
                channel.RunPendingTasks();
                var firstId = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                channel.WriteInbound(Command("XADD", "stream", "*", "b", "2"));
                channel.RunPendingTasks();
                var secondId = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                channel.WriteInbound(Command("XDEL", "stream", firstId, secondId));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var removed = Assert.IsType<IntegerRedisMessage>(response);
                Assert.Equal(2, removed.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies XRANGE returns entries in the requested range.
        /// </summary>
        public async Task XRange_ReturnsEntriesInRange()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("XADD", "stream", "*", "a", "1"));
                channel.RunPendingTasks();
                var firstId = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                channel.WriteInbound(Command("XADD", "stream", "*", "b", "2"));
                channel.RunPendingTasks();
                var secondId = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                channel.WriteInbound(Command("XRANGE", "stream", firstId, secondId));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                Assert.Collection(array.Children, _ => { }, _ => { });

                var entries = GetStreamEntries(array.Children);
                Assert.True(entries.Count == 2);
                Assert.Equal("1", entries[firstId]["a"]);
                Assert.Equal("2", entries[secondId]["b"]);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies XRANGE COUNT limits returned entries.
        /// </summary>
        public async Task XRange_Count_LimitsEntries()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("XADD", "stream", "*", "a", "1"));
                channel.RunPendingTasks();
                var firstId = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                channel.WriteInbound(Command("XADD", "stream", "*", "b", "2"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("XRANGE", "stream", "-", "+", "COUNT", "1"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                Assert.Single(array.Children);

                var entries = GetStreamEntries(array.Children);
                Assert.Single(entries);
                Assert.True(entries.ContainsKey(firstId));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies XREVRANGE returns entries in reverse order.
        /// </summary>
        public async Task XRevRange_ReturnsEntriesInReverse()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("XADD", "stream", "*", "a", "1"));
                channel.RunPendingTasks();
                var firstId = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                channel.WriteInbound(Command("XADD", "stream", "*", "b", "2"));
                channel.RunPendingTasks();
                var secondId = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                channel.WriteInbound(Command("XADD", "stream", "*", "c", "3"));
                channel.RunPendingTasks();
                var thirdId = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                channel.WriteInbound(Command("XREVRANGE", "stream", "+", "-"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                Assert.Equal(3, array.Children.Count);

                var entries = GetStreamEntries(array.Children);
                Assert.True(entries.ContainsKey(firstId));
                Assert.True(entries.ContainsKey(secondId));
                Assert.True(entries.ContainsKey(thirdId));

                var first = Assert.IsType<ArrayRedisMessage>(array.Children[0]);
                var firstEntryId = GetBulkOrNull(first.Children[0]);
                Assert.Equal(thirdId, firstEntryId);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies XREVRANGE COUNT limits returned entries.
        /// </summary>
        public async Task XRevRange_Count_LimitsEntries()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("XADD", "stream", "*", "a", "1"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("XADD", "stream", "*", "b", "2"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("XREVRANGE", "stream", "+", "-", "COUNT", "1"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                Assert.Single(array.Children);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies XGROUP CREATE MKSTREAM creates group on empty stream.
        /// </summary>
        public async Task XGroupCreate_MkStream_CreatesGroup()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("XGROUP", "CREATE", "stream", "group", "$", "MKSTREAM"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var ok = Assert.IsType<SimpleStringRedisMessage>(response);
                Assert.Equal("OK", ok.Content);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies XINFO STREAM returns metadata and entries.
        /// </summary>
        public async Task XInfo_Stream_ReturnsMetadata()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("XADD", "stream", "*", "a", "1"));
                channel.RunPendingTasks();
                var id1 = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                channel.WriteInbound(Command("XADD", "stream", "*", "b", "2"));
                channel.RunPendingTasks();
                var id2 = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                channel.WriteInbound(Command("XINFO", "STREAM", "stream"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                Assert.True(array.Children.Count >= 8);

                var items = GetHashPairs(array.Children);
                Assert.Equal("2", items["length"]);
                Assert.Equal(id2, items["last-generated-id"]);

                var firstEntry = Assert.IsType<ArrayRedisMessage>(array.Children[5]);
                var firstId = GetBulkOrNull(firstEntry.Children[0]);
                Assert.Equal(id1, firstId);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies XINFO GROUPS returns group metadata.
        /// </summary>
        public async Task XInfo_Groups_ReturnsGroups()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("XGROUP", "CREATE", "stream", "group", "-", "MKSTREAM"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("XINFO", "GROUPS", "stream"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                Assert.Single(array.Children);

                var groupArray = Assert.IsType<ArrayRedisMessage>(array.Children[0]);
                var groupItems = GetHashPairs(groupArray.Children);
                Assert.Equal("group", groupItems["name"]);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies XINFO CONSUMERS returns consumer metadata.
        /// </summary>
        public async Task XInfo_Consumers_ReturnsConsumers()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("XGROUP", "CREATE", "stream", "group", "-", "MKSTREAM"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("XADD", "stream", "*", "a", "1"));
                channel.RunPendingTasks();
                var id = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                channel.WriteInbound(Command("XREADGROUP", "GROUP", "group", "consumer", "STREAMS", "stream", ">"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("XINFO", "CONSUMERS", "stream", "group"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                Assert.Single(array.Children);

                var consumerArray = Assert.IsType<ArrayRedisMessage>(array.Children[0]);
                var consumerItems = GetHashPairs(consumerArray.Children);
                Assert.Equal("consumer", consumerItems["name"]);
                Assert.Equal("1", consumerItems["pending"]);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies XGROUP CREATE fails without existing stream.
        /// </summary>
        public async Task XGroupCreate_WithoutStream_ReturnsError()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("XGROUP", "CREATE", "stream", "group", "$"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var error = Assert.IsType<ErrorRedisMessage>(response);
                Assert.Equal("ERR The XGROUP subcommand requires the key to exist", error.Content);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies duplicate XGROUP CREATE returns BUSYGROUP.
        /// </summary>
        public async Task XGroupCreate_Duplicate_ReturnsBusyGroup()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("XGROUP", "CREATE", "stream", "group", "$", "MKSTREAM"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("XGROUP", "CREATE", "stream", "group", "$"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var error = Assert.IsType<ErrorRedisMessage>(response);
                Assert.Equal("BUSYGROUP Consumer Group name already exists", error.Content);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies XGROUP DESTROY removes the group and returns count.
        /// </summary>
        public async Task XGroupDestroy_RemovesGroup_ReturnsCount()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("XGROUP", "CREATE", "stream", "group", "$", "MKSTREAM"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("XGROUP", "DESTROY", "stream", "group"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var removed = Assert.IsType<IntegerRedisMessage>(response);
                Assert.Equal(1, removed.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies XSETID initializes an empty stream and updates last-generated-id.
        /// </summary>
        public async Task XSetId_SetsLastId()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("XSETID", "stream", "42-0"));
                channel.RunPendingTasks();

                var ok = Assert.IsType<SimpleStringRedisMessage>(ReadOutbound(channel));
                Assert.Equal("OK", ok.Content);

                channel.WriteInbound(Command("XINFO", "STREAM", "stream"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                var items = GetHashPairs(array.Children);
                Assert.Equal("0", items["length"]);
                Assert.Equal("42-0", items["last-generated-id"]);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies XGROUP SETID updates the group's last-delivered-id.
        /// </summary>
        public async Task XGroupSetId_UpdatesLastDelivered()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("XADD", "stream", "*", "a", "1"));
                channel.RunPendingTasks();
                var id1 = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                channel.WriteInbound(Command("XGROUP", "CREATE", "stream", "group", "-"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("XGROUP", "SETID", "stream", "group", id1));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("XINFO", "GROUPS", "stream"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                var groupArray = Assert.IsType<ArrayRedisMessage>(array.Children[0]);
                var groupItems = GetHashPairs(groupArray.Children);
                Assert.Equal(id1, groupItems["last-delivered-id"]);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies XGROUP DELCONSUMER removes pending entries for a consumer.
        /// </summary>
        public async Task XGroupDelConsumer_RemovesPending()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("XGROUP", "CREATE", "stream", "group", "-", "MKSTREAM"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("XADD", "stream", "*", "a", "1"));
                channel.RunPendingTasks();
                var id = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                channel.WriteInbound(Command("XREADGROUP", "GROUP", "group", "consumer", "STREAMS", "stream", ">"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("XGROUP", "DELCONSUMER", "stream", "group", "consumer"));
                channel.RunPendingTasks();
                var removed = Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel));
                Assert.Equal(1, removed.Value);

                channel.WriteInbound(Command("XPENDING", "stream", "group"));
                channel.RunPendingTasks();
                var pendingResponse = ReadOutbound(channel);
                var pendingArray = Assert.IsType<ArrayRedisMessage>(pendingResponse);
                var totalCount = Assert.IsType<IntegerRedisMessage>(pendingArray.Children[0]);
                Assert.Equal(0, totalCount.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies XREADGROUP returns entries and XACK removes pending.
        /// </summary>
        public async Task XReadGroup_ReturnsEntriesAndAckRemovesPending()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("XGROUP", "CREATE", "stream", "group", "-", "MKSTREAM"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("XADD", "stream", "*", "a", "1"));
                channel.RunPendingTasks();
                var id = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                channel.WriteInbound(Command("XREADGROUP", "GROUP", "group", "consumer", "STREAMS", "stream", ">"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                Assert.Single(array.Children);

                var streamMessage = Assert.IsType<ArrayRedisMessage>(array.Children[0]);
                var entriesMessage = Assert.IsType<ArrayRedisMessage>(streamMessage.Children[1]);
                var entries = GetStreamEntries(entriesMessage.Children);
                Assert.True(entries.ContainsKey(id));

                channel.WriteInbound(Command("XACK", "stream", "group", id));
                channel.RunPendingTasks();

                var ackResponse = ReadOutbound(channel);
                var ackCount = Assert.IsType<IntegerRedisMessage>(ackResponse);
                Assert.Equal(1, ackCount.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies XREADGROUP BLOCK waits for entries.
        /// </summary>
        public async Task XReadGroup_Block_WaitsForEntry()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("XGROUP", "CREATE", "stream", "group", "-", "MKSTREAM"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("XREADGROUP", "GROUP", "group", "consumer", "BLOCK", "50", "STREAMS", "stream", ">"));
                channel.RunPendingTasks();

                await Task.Delay(10);

                channel.WriteInbound(Command("XADD", "stream", "*", "a", "1"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                await Task.Delay(60);
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                Assert.Single(array.Children);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies XPENDING summary and extended forms.
        /// </summary>
        public async Task XPending_ReturnsSummaryAndExtendedInfo()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                // Create stream with consumer group
                channel.WriteInbound(Command("XGROUP", "CREATE", "stream", "group", "-", "MKSTREAM"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                // Add entries
                channel.WriteInbound(Command("XADD", "stream", "*", "a", "1"));
                channel.RunPendingTasks();
                var id1 = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                channel.WriteInbound(Command("XADD", "stream", "*", "b", "2"));
                channel.RunPendingTasks();
                var id2 = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                channel.WriteInbound(Command("XADD", "stream", "*", "c", "3"));
                channel.RunPendingTasks();
                var id3 = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                // Read entries with consumer1 (creates pending entries)
                channel.WriteInbound(Command("XREADGROUP", "GROUP", "group", "consumer1", "COUNT", "2", "STREAMS", "stream", ">"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                // Read entry with consumer2
                channel.WriteInbound(Command("XREADGROUP", "GROUP", "group", "consumer2", "STREAMS", "stream", ">"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                // Test summary form: XPENDING stream group
                channel.WriteInbound(Command("XPENDING", "stream", "group"));
                channel.RunPendingTasks();

                var summaryResponse = ReadOutbound(channel);
                var summaryArray = Assert.IsType<ArrayRedisMessage>(summaryResponse);
                Assert.Equal(4, summaryArray.Children.Count);

                // Count
                var count = Assert.IsType<IntegerRedisMessage>(summaryArray.Children[0]);
                Assert.Equal(3, count.Value);

                // Smallest ID
                var smallestIdMessage = Assert.IsType<SimpleStringRedisMessage>(summaryArray.Children[1]);
                var smallestId = smallestIdMessage.Content;
                Assert.Equal(id1, smallestId);

                // Largest ID
                var largestIdMessage = Assert.IsType<SimpleStringRedisMessage>(summaryArray.Children[2]);
                var largestId = largestIdMessage.Content;
                Assert.Equal(id3, largestId);

                // Consumers
                var consumers = Assert.IsType<ArrayRedisMessage>(summaryArray.Children[3]);
                Assert.Equal(2, consumers.Children.Count);

                // Test extended form: XPENDING stream group - + 10
                channel.WriteInbound(Command("XPENDING", "stream", "group", "-", "+", "10"));
                channel.RunPendingTasks();

                var extendedResponse = ReadOutbound(channel);
                var extendedArray = Assert.IsType<ArrayRedisMessage>(extendedResponse);
                Assert.Equal(3, extendedArray.Children.Count);

                // Each entry should have: [id, consumer, idle_time, delivery_count]
                var entry1 = Assert.IsType<ArrayRedisMessage>(extendedArray.Children[0]);
                Assert.Equal(4, entry1.Children.Count);
                var entryIdMessage = Assert.IsType<SimpleStringRedisMessage>(entry1.Children[0]);
                var entryId1 = entryIdMessage.Content;
                Assert.Equal(id1, entryId1);

                // Test with consumer filter
                channel.WriteInbound(Command("XPENDING", "stream", "group", "-", "+", "10", "consumer1"));
                channel.RunPendingTasks();

                var filteredResponse = ReadOutbound(channel);
                var filteredArray = Assert.IsType<ArrayRedisMessage>(filteredResponse);
                Assert.Equal(2, filteredArray.Children.Count); // Only entries for consumer1

                // Test ACK removes from pending
                channel.WriteInbound(Command("XACK", "stream", "group", id1));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                // Verify pending count decreased
                channel.WriteInbound(Command("XPENDING", "stream", "group"));
                channel.RunPendingTasks();

                var finalSummary = ReadOutbound(channel);
                var finalArray = Assert.IsType<ArrayRedisMessage>(finalSummary);
                var finalCount = Assert.IsType<IntegerRedisMessage>(finalArray.Children[0]);
                Assert.Equal(2, finalCount.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies XCLAIM transfers pending entries between consumers.
        /// </summary>
        public async Task XClaim_TransfersPendingEntries()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                // Create stream with consumer group
                channel.WriteInbound(Command("XGROUP", "CREATE", "stream", "group", "-", "MKSTREAM"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                // Add entries
                channel.WriteInbound(Command("XADD", "stream", "*", "a", "1"));
                channel.RunPendingTasks();
                var id1 = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                channel.WriteInbound(Command("XADD", "stream", "*", "b", "2"));
                channel.RunPendingTasks();
                var id2 = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                channel.WriteInbound(Command("XADD", "stream", "*", "c", "3"));
                channel.RunPendingTasks();
                var id3 = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                // Consumer1 reads entries (creates pending entries)
                channel.WriteInbound(Command("XREADGROUP", "GROUP", "group", "consumer1", "COUNT", "2", "STREAMS", "stream", ">"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                // Wait a bit to ensure idle time > 0
                await Task.Delay(50);

                // Consumer2 claims the entries from consumer1
                channel.WriteInbound(Command("XCLAIM", "stream", "group", "consumer2", "0", id1, id2));
                channel.RunPendingTasks();

                var claimResponse = ReadOutbound(channel);
                var claimArray = Assert.IsType<ArrayRedisMessage>(claimResponse);
                Assert.Equal(2, claimArray.Children.Count);

                // Verify the entries were returned
                var entry1 = Assert.IsType<ArrayRedisMessage>(claimArray.Children[0]);
                var entryId = Assert.IsType<SimpleStringRedisMessage>(entry1.Children[0]);
                Assert.Equal(id1, entryId.Content);

                // Verify pending entries are now owned by consumer2
                channel.WriteInbound(Command("XPENDING", "stream", "group", "-", "+", "10", "consumer2"));
                channel.RunPendingTasks();

                var pendingResponse = ReadOutbound(channel);
                var pendingArray = Assert.IsType<ArrayRedisMessage>(pendingResponse);
                Assert.Equal(2, pendingArray.Children.Count); // consumer2 should have 2 entries

                // Test with minimum idle time - shouldn't claim fresh entries
                channel.WriteInbound(Command("XCLAIM", "stream", "group", "consumer3", "10000", id1, id2));
                channel.RunPendingTasks();

                var noClaimResponse = ReadOutbound(channel);
                var noClaimArray = Assert.IsType<ArrayRedisMessage>(noClaimResponse);
                Assert.Empty(noClaimArray.Children); // No entries claimed due to idle time
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies XCLAIM options like JUSTID, RETRYCOUNT, and FORCE.
        /// </summary>
        public async Task XClaim_WithOptions()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                // Create stream with consumer group
                channel.WriteInbound(Command("XGROUP", "CREATE", "stream", "group", "-", "MKSTREAM"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                // Add entry
                channel.WriteInbound(Command("XADD", "stream", "*", "field", "value"));
                channel.RunPendingTasks();
                var id = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                // Consumer1 reads the entry
                channel.WriteInbound(Command("XREADGROUP", "GROUP", "group", "consumer1", "STREAMS", "stream", ">"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                // Test JUSTID option - should return only IDs
                channel.WriteInbound(Command("XCLAIM", "stream", "group", "consumer2", "0", id, "JUSTID"));
                channel.RunPendingTasks();

                var justIdResponse = ReadOutbound(channel);
                var justIdArray = Assert.IsType<ArrayRedisMessage>(justIdResponse);
                Assert.Single(justIdArray.Children);
                var returnedId = Assert.IsType<SimpleStringRedisMessage>(justIdArray.Children[0]);
                Assert.Equal(id, returnedId.Content);

                // Test RETRYCOUNT option
                channel.WriteInbound(Command("XCLAIM", "stream", "group", "consumer3", "0", id, "RETRYCOUNT", "5"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                // Verify retry count was set
                channel.WriteInbound(Command("XPENDING", "stream", "group", "-", "+", "10"));
                channel.RunPendingTasks();

                var pendingResponse = ReadOutbound(channel);
                var pendingArray = Assert.IsType<ArrayRedisMessage>(pendingResponse);
                var pendingEntry = Assert.IsType<ArrayRedisMessage>(pendingArray.Children[0]);
                var deliveryCount = Assert.IsType<IntegerRedisMessage>(pendingEntry.Children[3]);
                Assert.Equal(5, deliveryCount.Value);

                // Test FORCE option with non-pending entry
                channel.WriteInbound(Command("XADD", "stream", "*", "field2", "value2"));
                channel.RunPendingTasks();
                var id2 = GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel)));

                // FORCE should create a pending entry even though it's not pending
                channel.WriteInbound(Command("XCLAIM", "stream", "group", "consumer4", "0", id2, "FORCE"));
                channel.RunPendingTasks();

                var forceResponse = ReadOutbound(channel);
                var forceArray = Assert.IsType<ArrayRedisMessage>(forceResponse);
                Assert.Single(forceArray.Children); // Should claim the entry

                // Verify it's now in pending
                channel.WriteInbound(Command("XPENDING", "stream", "group"));
                channel.RunPendingTasks();

                var summaryResponse = ReadOutbound(channel);
                var summaryArray = Assert.IsType<ArrayRedisMessage>(summaryResponse);
                var totalCount = Assert.IsType<IntegerRedisMessage>(summaryArray.Children[0]);
                Assert.Equal(2, totalCount.Value); // Both entries should be pending now
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        /// <summary>
        /// Reads the next outbound message after running pending tasks.
        /// </summary>
        private static IRedisMessage ReadOutbound(EmbeddedChannel channel)
        {
            channel.RunPendingTasks();
            channel.RunScheduledPendingTasks();
            return channel.ReadOutbound<IRedisMessage>();
        }

        /// <summary>
        /// Builds a RESP array message from string parts.
        /// </summary>
        private static ArrayRedisMessage Command(params string[] parts)
        {
            var children = new IRedisMessage[parts.Length];
            for (int i = 0; i < parts.Length; i++)
            {
                var bytes = Utf8.GetBytes(parts[i]);
                children[i] = new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(bytes));
            }

            return new ArrayRedisMessage(children);
        }

        /// <summary>
        /// Extracts a UTF-8 string from a bulk string message.
        /// </summary>
        private static string GetBulkString(FullBulkStringRedisMessage message)
        {
            var length = message.Content.ReadableBytes;
            var buffer = new byte[length];
            message.Content.GetBytes(message.Content.ReaderIndex, buffer, 0, length);
            return Utf8.GetString(buffer, 0, length);
        }

        /// <summary>
        /// Asserts that a bulk string message contains the expected string value.
        /// </summary>
        private static void AssertBulkStringEqual(FullBulkStringRedisMessage message, string expected)
        {
            var actual = GetBulkString(message);
            Assert.Equal(expected, actual);
        }

        /// <summary>
        /// Extracts a UTF-8 string from a bulk message or returns null.
        /// </summary>
        private static string? GetBulkOrNull(IRedisMessage message)
        {
            if (ReferenceEquals(message, FullBulkStringRedisMessage.Null))
            {
                return null;
            }

            if (message is FullBulkStringRedisMessage bulk)
            {
                return GetBulkString(bulk);
            }

            return null;
        }

        /// <summary>
        /// Converts a RESP array of hash pairs into a dictionary.
        /// </summary>
        private static Dictionary<string, string> GetHashPairs(IList<IRedisMessage> children)
        {
            var result = new Dictionary<string, string>(StringComparer.Ordinal);
            for (int i = 0; i < children.Count; i += 2)
            {
                var field = GetBulkOrNull(children[i]);
                var value = GetHashValueOrNull(children[i + 1]);
                if (field != null && value != null)
                {
                    result[field] = value;
                }
            }

            return result;
        }

        /// <summary>
        /// Extracts a hash value from RESP messages (bulk, simple, or integer).
        /// </summary>
        private static string? GetHashValueOrNull(IRedisMessage message)
        {
            if (ReferenceEquals(message, FullBulkStringRedisMessage.Null))
            {
                return null;
            }

            if (message is FullBulkStringRedisMessage bulk)
            {
                return GetBulkString(bulk);
            }

            if (message is SimpleStringRedisMessage simple)
            {
                return simple.Content;
            }

            if (message is IntegerRedisMessage integer)
            {
                return integer.Value.ToString(CultureInfo.InvariantCulture);
            }

            return null;
        }

        /// <summary>
        /// Converts a RESP array of stream entries into a dictionary.
        /// </summary>
        private static Dictionary<string, Dictionary<string, string>> GetStreamEntries(IList<IRedisMessage> children)
        {
            var result = new Dictionary<string, Dictionary<string, string>>(StringComparer.Ordinal);
            foreach (var entryMessage in children)
            {
                var entryArray = Assert.IsType<ArrayRedisMessage>(entryMessage);
                var id = GetBulkOrNull(entryArray.Children[0]) ?? string.Empty;
                var fieldsArray = Assert.IsType<ArrayRedisMessage>(entryArray.Children[1]);
                result[id] = GetHashPairs(fieldsArray.Children);
            }

            return result;
        }
    }

    /// <summary>
    /// In-memory implementation of <see cref="IKeyValueStore"/> for tests.
    /// </summary>
    internal sealed class InMemoryKeyValueStore : IKeyValueStore
    {
        private readonly Dictionary<string, byte[]> _data = new(StringComparer.Ordinal);
        private readonly Dictionary<string, Dictionary<string, byte[]>> _hashes = new(StringComparer.Ordinal);
        private readonly Dictionary<string, List<byte[]>> _lists = new(StringComparer.Ordinal);
        private readonly Dictionary<string, Dictionary<string, byte[]>> _sets = new(StringComparer.Ordinal);
        private readonly Dictionary<string, Dictionary<string, SortedSetMember>> _sortedSets = new(StringComparer.Ordinal);
        private readonly Dictionary<string, List<StreamEntryModel>> _streams = new(StringComparer.Ordinal);
        private readonly Dictionary<string, StreamId> _streamLastIds = new(StringComparer.Ordinal);
        private readonly Dictionary<string, Dictionary<string, StreamGroupState>> _streamGroups = new(StringComparer.Ordinal);
        private readonly Dictionary<string, DateTimeOffset?> _expirations = new(StringComparer.Ordinal);
        private static readonly Encoding Utf8 = new UTF8Encoding(false);

        /// <summary>
        /// Internal stream entry model used for test storage.
        /// </summary>
        private sealed class StreamEntryModel
        {
            /// <summary>
            /// Initializes a new instance of the <see cref="StreamEntryModel"/> class.
            /// </summary>
            public StreamEntryModel(string id, StreamId parsedId, KeyValuePair<string, byte[]>[] fields)
            {
                Id = id;
                ParsedId = parsedId;
                Fields = fields;
            }

            public string Id { get; }
            public StreamId ParsedId { get; }
            public KeyValuePair<string, byte[]>[] Fields { get; }
        }

        /// <summary>
        /// Internal state for a stream consumer group.
        /// </summary>
        private sealed class StreamGroupState
        {
            /// <summary>
            /// Initializes a new instance of the <see cref="StreamGroupState"/> class.
            /// </summary>
            public StreamGroupState(StreamId lastDeliveredId)
            {
                LastDeliveredId = lastDeliveredId;
                Pending = new Dictionary<string, PendingEntryInfo>(StringComparer.Ordinal);
            }

            public StreamId LastDeliveredId { get; set; }
            public Dictionary<string, PendingEntryInfo> Pending { get; }
        }

        /// <summary>
        /// Tracks pending entry metadata for consumer groups.
        /// </summary>
        private sealed class PendingEntryInfo
        {
            /// <summary>
            /// Initializes a new instance of the <see cref="PendingEntryInfo"/> class.
            /// </summary>
            public PendingEntryInfo(string consumer, DateTimeOffset deliveryTime, long deliveryCount)
            {
                Consumer = consumer;
                DeliveryTime = deliveryTime;
                DeliveryCount = deliveryCount;
            }

            public string Consumer { get; set; }
            public DateTimeOffset DeliveryTime { get; set; }
            public long DeliveryCount { get; set; }
        }

        /// <summary>
        /// Internal model for a sorted set member.
        /// </summary>
        private sealed class SortedSetMember
        {
            /// <summary>
            /// Initializes a new instance of the <see cref="SortedSetMember"/> class.
            /// </summary>
            public SortedSetMember(string key, byte[] member, double score)
            {
                Key = key;
                Member = member;
                Score = score;
            }

            public string Key { get; }
            public byte[] Member { get; }
            public double Score { get; set; }
        }

        /// <summary>
        /// Represents a parsed stream id for comparison.
        /// </summary>
        private readonly struct StreamId : IComparable<StreamId>
        {
            /// <summary>
            /// Initializes a new instance of the <see cref="StreamId"/> struct.
            /// </summary>
            public StreamId(long ms, long seq)
            {
                Ms = ms;
                Seq = seq;
            }

            public long Ms { get; }
            public long Seq { get; }

            /// <summary>
            /// Compares two stream ids for ordering.
            /// </summary>
            public int CompareTo(StreamId other)
            {
                var cmp = Ms.CompareTo(other.Ms);
                return cmp != 0 ? cmp : Seq.CompareTo(other.Seq);
            }
        }

        /// <summary>
        /// Seeds the store with a string value for tests.
        /// </summary>
        public void Seed(string key, string value)
        {
            _data[key] = Utf8.GetBytes(value);
            _hashes.Remove(key);
            _lists.Remove(key);
            _sets.Remove(key);
            _sortedSets.Remove(key);
            _streams.Remove(key);
            _streamLastIds.Remove(key);
            _streamGroups.Remove(key);
            _expirations.Remove(key);
        }

        /// <summary>
        /// Gets the value for a key.
        /// </summary>
        public Task<byte[]?> GetAsync(string key, CancellationToken token = default)
        {
            return Task.FromResult(GetValue(key));
        }

        /// <summary>
        /// Sets a value with optional expiration and condition.
        /// </summary>
        public Task<bool> SetAsync(
            string key,
            byte[] value,
            TimeSpan? expiration,
            SetCondition condition,
            CancellationToken token = default)
        {
            var exists = GetValue(key) != null;

            if (condition == SetCondition.Nx && exists)
            {
                return Task.FromResult(false);
            }

            if (condition == SetCondition.Xx && !exists)
            {
                return Task.FromResult(false);
            }

            _data[key] = value;
            _hashes.Remove(key);
            _lists.Remove(key);
            _sets.Remove(key);
            _sortedSets.Remove(key);
            _streams.Remove(key);
            _streamLastIds.Remove(key);
            _streamGroups.Remove(key);

            if (expiration.HasValue)
            {
                _expirations[key] = DateTimeOffset.UtcNow.Add(expiration.Value);
            }
            else
            {
                _expirations.Remove(key);
            }

            return Task.FromResult(true);
        }

        /// <summary>
        /// Gets values for multiple keys.
        /// </summary>
        public Task<byte[]?[]> GetManyAsync(string[] keys, CancellationToken token = default)
        {
            var result = new byte[]?[keys.Length];
            for (int i = 0; i < keys.Length; i++)
            {
                result[i] = GetValue(keys[i]);
            }

            return Task.FromResult(result);
        }

        /// <summary>
        /// Sets multiple key/value pairs.
        /// </summary>
        public Task<bool> SetManyAsync(KeyValuePair<string, byte[]>[] items, CancellationToken token = default)
        {
            foreach (var item in items)
            {
                _data[item.Key] = item.Value;
                _hashes.Remove(item.Key);
                _lists.Remove(item.Key);
                _sets.Remove(item.Key);
                _sortedSets.Remove(item.Key);
                _streams.Remove(item.Key);
                _streamLastIds.Remove(item.Key);
                _streamGroups.Remove(item.Key);
                _expirations.Remove(item.Key);
            }

            return Task.FromResult(true);
        }

        /// <summary>
        /// Deletes keys and returns the removal count.
        /// </summary>
        public Task<long> DeleteAsync(string[] keys, CancellationToken token = default)
        {
            long removed = 0;
            foreach (var key in keys)
            {
                if (RemoveKey(key))
                {
                    removed++;
                }
            }

            return Task.FromResult(removed);
        }

        /// <summary>
        /// Returns whether a single key exists.
        /// </summary>
        public Task<bool> ExistsAsync(string key, CancellationToken token = default)
        {
            return Task.FromResult(HasKey(key));
        }

        /// <summary>
        /// Returns the count of existing keys.
        /// </summary>
        public Task<long> ExistsAsync(string[] keys, CancellationToken token = default)
        {
            long count = 0;
            foreach (var key in keys)
            {
                if (HasKey(key))
                {
                    count++;
                }
            }

            return Task.FromResult(count);
        }

        /// <summary>
        /// Increments a key by a delta and returns the new value.
        /// </summary>
        public Task<long?> IncrByAsync(string key, long delta, CancellationToken token = default)
        {
            var value = GetValue(key);
            long current = 0;
            if (value != null)
            {
                var text = Utf8.GetString(value);
                if (!long.TryParse(text, out current))
                {
                    return Task.FromResult<long?>(null);
                }
            }

            try
            {
                var next = checked(current + delta);
                _data[key] = Utf8.GetBytes(next.ToString());
                _expirations.Remove(key);
                return Task.FromResult<long?>(next);
            }
            catch (OverflowException)
            {
                return Task.FromResult<long?>(null);
            }
        }

        /// <summary>
        /// Sets a key expiration in seconds.
        /// </summary>
        public Task<bool> ExpireAsync(string key, TimeSpan expiration, CancellationToken token = default)
        {
            if (!HasKey(key))
            {
                return Task.FromResult(false);
            }

            _expirations[key] = DateTimeOffset.UtcNow.Add(expiration);
            return Task.FromResult(true);
        }

        /// <summary>
        /// Sets a key expiration in milliseconds.
        /// </summary>
        public Task<bool> PExpireAsync(string key, TimeSpan expiration, CancellationToken token = default)
        {
            if (!HasKey(key))
            {
                return Task.FromResult(false);
            }

            _expirations[key] = DateTimeOffset.UtcNow.Add(expiration);
            return Task.FromResult(true);
        }

        /// <summary>
        /// Returns remaining TTL in seconds, or special values for missing/no-expiry keys.
        /// </summary>
        public Task<long> TtlAsync(string key, CancellationToken token = default)
        {
            if (!HasKey(key))
            {
                return Task.FromResult(-2L);
            }

            if (!_expirations.TryGetValue(key, out var expiresAt) || expiresAt == null)
            {
                return Task.FromResult(-1L);
            }

            var remaining = expiresAt.Value - DateTimeOffset.UtcNow;
            if (remaining <= TimeSpan.Zero)
            {
                _data.Remove(key);
                _expirations.Remove(key);
                return Task.FromResult(-2L);
            }

            return Task.FromResult((long)remaining.TotalSeconds);
        }

        /// <summary>
        /// Returns remaining TTL in milliseconds, or special values for missing/no-expiry keys.
        /// </summary>
        public Task<long> PttlAsync(string key, CancellationToken token = default)
        {
            if (!HasKey(key))
            {
                return Task.FromResult(-2L);
            }

            if (!_expirations.TryGetValue(key, out var expiresAt) || expiresAt == null)
            {
                return Task.FromResult(-1L);
            }

            var remaining = expiresAt.Value - DateTimeOffset.UtcNow;
            if (remaining <= TimeSpan.Zero)
            {
                _data.Remove(key);
                _expirations.Remove(key);
                return Task.FromResult(-2L);
            }

            return Task.FromResult((long)remaining.TotalMilliseconds);
        }

        /// <summary>
        /// Removes expired keys and returns the number removed.
        /// </summary>
        public Task<long> CleanUpExpiredKeysAsync(CancellationToken token = default)
        {
            long removed = 0;
            var now = DateTimeOffset.UtcNow;
            var expiredKeys = new List<string>();

            foreach (var pair in _expirations)
            {
                if (pair.Value.HasValue && pair.Value.Value <= now)
                {
                    expiredKeys.Add(pair.Key);
                }
            }

            foreach (var key in expiredKeys)
            {
                removed += RemoveKey(key) ? 1 : 0;
            }

            return Task.FromResult(removed);
        }

        /// <summary>
        /// Adds a stream entry and returns its id.
        /// </summary>
        public Task<string?> StreamAddAsync(
            string key,
            string id,
            KeyValuePair<string, byte[]>[] fields,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (!_streams.TryGetValue(key, out var stream))
            {
                stream = new List<StreamEntryModel>();
                _streams[key] = stream;
                _data.Remove(key);
                _hashes.Remove(key);
                _lists.Remove(key);
                _sets.Remove(key);
                _sortedSets.Remove(key);
            }

            var lastId = _streamLastIds.TryGetValue(key, out var last) ? last : new StreamId(-1, -1);
            StreamId nextId;
            string nextIdText;

            if (id == "*")
            {
                var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                var seq = lastId.Ms == now ? lastId.Seq + 1 : 0;
                nextId = new StreamId(now, seq);
                nextIdText = FormatStreamId(nextId);
            }
            else
            {
                if (!TryParseStreamId(id, out nextId) || nextId.CompareTo(lastId) <= 0)
                {
                    return Task.FromResult<string?>(null);
                }

                nextIdText = id;
            }

            stream.Add(new StreamEntryModel(nextIdText, nextId, fields));
            _streamLastIds[key] = nextId;
            return Task.FromResult<string?>(nextIdText);
        }

        /// <summary>
        /// Removes stream entries by id and returns the count removed.
        /// </summary>
        public Task<long> StreamDeleteAsync(string key, string[] ids, CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
                return Task.FromResult(0L);
            }

            if (!_streams.TryGetValue(key, out var stream))
            {
                return Task.FromResult(0L);
            }

            var toRemove = new HashSet<string>(ids, StringComparer.Ordinal);
            long removed = 0;
            for (int i = stream.Count - 1; i >= 0; i--)
            {
                if (toRemove.Contains(stream[i].Id))
                {
                    stream.RemoveAt(i);
                    removed++;
                }
            }

            if (stream.Count == 0)
            {
                _streams.Remove(key);
                _streamLastIds.Remove(key);
                _streamGroups.Remove(key);
                _expirations.Remove(key);
            }

            return Task.FromResult(removed);
        }

        /// <summary>
        /// Returns the number of entries in the stream.
        /// </summary>
        public Task<long> StreamLengthAsync(string key, CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
                return Task.FromResult(0L);
            }

            return Task.FromResult(_streams.TryGetValue(key, out var stream) ? stream.Count : 0L);
        }

        /// <summary>
        /// Returns the last entry id for a stream, or null if empty.
        /// </summary>
        public Task<string?> StreamLastIdAsync(string key, CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
                return Task.FromResult<string?>(null);
            }

            if (!_streams.TryGetValue(key, out var stream) || stream.Count == 0)
            {
                return Task.FromResult<string?>(null);
            }

            if (_streamLastIds.TryGetValue(key, out var last))
            {
                return Task.FromResult<string?>(FormatStreamId(last));
            }

            return Task.FromResult<string?>(stream[stream.Count - 1].Id);
        }

        /// <summary>
        /// Trims a stream by max length or minimum id.
        /// </summary>
        public Task<long> StreamTrimAsync(
            string key,
            int? maxLength = null,
            string? minId = null,
            bool approximate = false,
            CancellationToken token = default)
        {
            _ = approximate;

            if (IsExpired(key))
            {
                RemoveKey(key);
                return Task.FromResult(0L);
            }

            if (!_streams.TryGetValue(key, out var stream) || stream.Count == 0)
            {
                return Task.FromResult(0L);
            }

            var removed = 0L;
            var removedIds = new List<string>();

            if (maxLength.HasValue)
            {
                var target = maxLength.Value;
                if (target < 0)
                {
                    return Task.FromResult(0L);
                }

                var toRemove = Math.Max(0, stream.Count - target);
                for (int i = 0; i < toRemove; i++)
                {
                    removedIds.Add(stream[i].Id);
                }

                if (toRemove > 0)
                {
                    stream.RemoveRange(0, toRemove);
                    removed = toRemove;
                }
            }
            else if (!string.IsNullOrEmpty(minId) && TryParseStreamId(minId, out var minParsed))
            {
                var toRemove = 0;
                foreach (var entry in stream)
                {
                    if (entry.ParsedId.CompareTo(minParsed) < 0)
                    {
                        removedIds.Add(entry.Id);
                        toRemove++;
                    }
                    else
                    {
                        break;
                    }
                }

                if (toRemove > 0)
                {
                    stream.RemoveRange(0, toRemove);
                    removed = toRemove;
                }
            }

            if (removedIds.Count > 0 && _streamGroups.TryGetValue(key, out var groups))
            {
                foreach (var state in groups.Values)
                {
                    foreach (var id in removedIds)
                    {
                        state.Pending.Remove(id);
                    }
                }
            }

            if (stream.Count == 0)
            {
                _streams.Remove(key);
                _streamLastIds.Remove(key);
                _streamGroups.Remove(key);
                _expirations.Remove(key);
            }

            return Task.FromResult(removed);
        }

        /// <summary>
        /// Reads entries after the specified ids for each stream.
        /// </summary>
        public Task<StreamReadResult[]> StreamReadAsync(
            string[] keys,
            string[] ids,
            int? count,
            CancellationToken token = default)
        {
            var results = new List<StreamReadResult>();

            for (int i = 0; i < keys.Length; i++)
            {
                var key = keys[i];
                if (IsExpired(key))
                {
                    RemoveKey(key);
                    continue;
                }

                if (!_streams.TryGetValue(key, out var stream))
                {
                    continue;
                }

                var startId = ids[i] == "$" && _streamLastIds.TryGetValue(key, out var last)
                    ? last
                    : TryParseStreamId(ids[i], out var parsed) ? parsed : new StreamId(-1, -1);

                var entries = new List<StreamEntry>();
                foreach (var entry in stream)
                {
                    if (entry.ParsedId.CompareTo(startId) <= 0)
                    {
                        continue;
                    }

                    entries.Add(new StreamEntry(entry.Id, entry.Fields));
                    if (count.HasValue && entries.Count >= count.Value)
                    {
                        break;
                    }
                }

                if (entries.Count > 0)
                {
                    results.Add(new StreamReadResult(key, entries.ToArray()));
                }
            }

            return Task.FromResult(results.ToArray());
        }

        /// <summary>
        /// Returns stream metadata for XINFO STREAM.
        /// </summary>
        public Task<StreamInfoResult> StreamInfoAsync(string key, CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
                return Task.FromResult(new StreamInfoResult(StreamInfoResultStatus.NoStream, null));
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key))
            {
                return Task.FromResult(new StreamInfoResult(StreamInfoResultStatus.WrongType, null));
            }

            if (!_streams.TryGetValue(key, out var stream))
            {
                return Task.FromResult(new StreamInfoResult(StreamInfoResultStatus.NoStream, null));
            }

            var length = stream.Count;
            var firstEntry = length > 0 ? new StreamEntry(stream[0].Id, stream[0].Fields) : null;
            var lastEntry = length > 0 ? new StreamEntry(stream[length - 1].Id, stream[length - 1].Fields) : null;
            var lastId = _streamLastIds.TryGetValue(key, out var last) ? FormatStreamId(last) : lastEntry?.Id;

            var info = new StreamInfo(length, lastId, firstEntry, lastEntry);
            return Task.FromResult(new StreamInfoResult(StreamInfoResultStatus.Ok, info));
        }

        /// <summary>
        /// Returns group metadata for XINFO GROUPS.
        /// </summary>
        public Task<StreamGroupsInfoResult> StreamGroupsInfoAsync(string key, CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
                return Task.FromResult(new StreamGroupsInfoResult(StreamInfoResultStatus.NoStream, Array.Empty<StreamGroupInfo>()));
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key))
            {
                return Task.FromResult(new StreamGroupsInfoResult(StreamInfoResultStatus.WrongType, Array.Empty<StreamGroupInfo>()));
            }

            if (!_streams.ContainsKey(key))
            {
                return Task.FromResult(new StreamGroupsInfoResult(StreamInfoResultStatus.NoStream, Array.Empty<StreamGroupInfo>()));
            }

            if (!_streamGroups.TryGetValue(key, out var groups))
            {
                return Task.FromResult(new StreamGroupsInfoResult(StreamInfoResultStatus.Ok, Array.Empty<StreamGroupInfo>()));
            }

            var groupInfos = new List<StreamGroupInfo>();
            foreach (var pair in groups)
            {
                var state = pair.Value;
                var consumerCount = state.Pending.Values.Select(p => p.Consumer).Distinct(StringComparer.Ordinal).LongCount();
                var pendingCount = state.Pending.Count;
                var lastDelivered = FormatStreamId(state.LastDeliveredId);
                groupInfos.Add(new StreamGroupInfo(pair.Key, consumerCount, pendingCount, lastDelivered));
            }

            return Task.FromResult(new StreamGroupsInfoResult(StreamInfoResultStatus.Ok, groupInfos.ToArray()));
        }

        /// <summary>
        /// Returns consumer metadata for XINFO CONSUMERS.
        /// </summary>
        public Task<StreamConsumersInfoResult> StreamConsumersInfoAsync(
            string key,
            string group,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
                return Task.FromResult(new StreamConsumersInfoResult(StreamInfoResultStatus.NoStream, Array.Empty<StreamConsumerInfo>()));
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key))
            {
                return Task.FromResult(new StreamConsumersInfoResult(StreamInfoResultStatus.WrongType, Array.Empty<StreamConsumerInfo>()));
            }

            if (!_streams.ContainsKey(key))
            {
                return Task.FromResult(new StreamConsumersInfoResult(StreamInfoResultStatus.NoStream, Array.Empty<StreamConsumerInfo>()));
            }

            if (!_streamGroups.TryGetValue(key, out var groups) || !groups.TryGetValue(group, out var state))
            {
                return Task.FromResult(new StreamConsumersInfoResult(StreamInfoResultStatus.NoGroup, Array.Empty<StreamConsumerInfo>()));
            }

            var now = DateTimeOffset.UtcNow;
            var consumers = state.Pending
                .GroupBy(p => p.Value.Consumer)
                .Select(grouping =>
                {
                    var latest = grouping.Max(p => p.Value.DeliveryTime);
                    var idleMs = (long)(now - latest).TotalMilliseconds;
                    return new StreamConsumerInfo(grouping.Key, grouping.LongCount(), idleMs);
                })
                .ToArray();

            return Task.FromResult(new StreamConsumersInfoResult(StreamInfoResultStatus.Ok, consumers));
        }

        /// <summary>
        /// Sets the stream last generated id.
        /// </summary>
        public Task<StreamSetIdResultStatus> StreamSetIdAsync(
            string key,
            string lastId,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key))
            {
                return Task.FromResult(StreamSetIdResultStatus.WrongType);
            }

            if (!TryParseStreamId(lastId, out var parsed))
            {
                return Task.FromResult(StreamSetIdResultStatus.InvalidId);
            }

            if (!_streams.ContainsKey(key))
            {
                _streams[key] = new List<StreamEntryModel>();
                _streamGroups.Remove(key);
            }

            _streamLastIds[key] = parsed;
            return Task.FromResult(StreamSetIdResultStatus.Ok);
        }

        /// <summary>
        /// Creates a consumer group for a stream.
        /// </summary>
        public Task<StreamGroupCreateResult> StreamGroupCreateAsync(
            string key,
            string group,
            string startId,
            bool mkStream,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key))
            {
                return Task.FromResult(StreamGroupCreateResult.WrongType);
            }

            var streamExists = _streams.TryGetValue(key, out var stream);
            if (!streamExists && !mkStream)
            {
                return Task.FromResult(StreamGroupCreateResult.NoStream);
            }

            if (!streamExists)
            {
                stream = new List<StreamEntryModel>();
                _streams[key] = stream;
                _streamLastIds.Remove(key);
            }

            if (!_streamGroups.TryGetValue(key, out var groups))
            {
                groups = new Dictionary<string, StreamGroupState>(StringComparer.Ordinal);
                _streamGroups[key] = groups;
            }

            if (groups.ContainsKey(group))
            {
                return Task.FromResult(StreamGroupCreateResult.Exists);
            }

            if (!TryResolveGroupStartId(key, stream, startId, out var lastId))
            {
                return Task.FromResult(StreamGroupCreateResult.InvalidId);
            }

            groups[group] = new StreamGroupState(lastId);
            return Task.FromResult(StreamGroupCreateResult.Ok);
        }

        /// <summary>
        /// Destroys a consumer group for a stream.
        /// </summary>
        public Task<StreamGroupDestroyResult> StreamGroupDestroyAsync(
            string key,
            string group,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
                return Task.FromResult(StreamGroupDestroyResult.NotFound);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key))
            {
                return Task.FromResult(StreamGroupDestroyResult.WrongType);
            }

            if (!_streamGroups.TryGetValue(key, out var groups))
            {
                return Task.FromResult(StreamGroupDestroyResult.NotFound);
            }

            if (!groups.Remove(group))
            {
                return Task.FromResult(StreamGroupDestroyResult.NotFound);
            }

            if (groups.Count == 0)
            {
                _streamGroups.Remove(key);
            }

            return Task.FromResult(StreamGroupDestroyResult.Removed);
        }

        /// <summary>
        /// Sets the last delivered id for a consumer group.
        /// </summary>
        public Task<StreamGroupSetIdResultStatus> StreamGroupSetIdAsync(
            string key,
            string group,
            string lastId,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
                return Task.FromResult(StreamGroupSetIdResultStatus.NoStream);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key))
            {
                return Task.FromResult(StreamGroupSetIdResultStatus.WrongType);
            }

            if (!_streams.TryGetValue(key, out var stream))
            {
                return Task.FromResult(StreamGroupSetIdResultStatus.NoStream);
            }

            if (!_streamGroups.TryGetValue(key, out var groups) || !groups.TryGetValue(group, out var state))
            {
                return Task.FromResult(StreamGroupSetIdResultStatus.NoGroup);
            }

            if (!TryResolveGroupStartId(key, stream, lastId, out var resolvedId))
            {
                return Task.FromResult(StreamGroupSetIdResultStatus.InvalidId);
            }

            state.LastDeliveredId = resolvedId;
            return Task.FromResult(StreamGroupSetIdResultStatus.Ok);
        }

        /// <summary>
        /// Removes a consumer from a group and clears pending entries.
        /// </summary>
        public Task<StreamGroupDelConsumerResult> StreamGroupDelConsumerAsync(
            string key,
            string group,
            string consumer,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
                return Task.FromResult(new StreamGroupDelConsumerResult(StreamGroupDelConsumerResultStatus.NoStream, 0));
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key))
            {
                return Task.FromResult(new StreamGroupDelConsumerResult(StreamGroupDelConsumerResultStatus.WrongType, 0));
            }

            if (!_streams.ContainsKey(key))
            {
                return Task.FromResult(new StreamGroupDelConsumerResult(StreamGroupDelConsumerResultStatus.NoStream, 0));
            }

            if (!_streamGroups.TryGetValue(key, out var groups) || !groups.TryGetValue(group, out var state))
            {
                return Task.FromResult(new StreamGroupDelConsumerResult(StreamGroupDelConsumerResultStatus.NoGroup, 0));
            }

            var removed = 0L;
            var toRemove = state.Pending
                .Where(p => string.Equals(p.Value.Consumer, consumer, StringComparison.Ordinal))
                .Select(p => p.Key)
                .ToArray();

            foreach (var id in toRemove)
            {
                if (state.Pending.Remove(id))
                {
                    removed++;
                }
            }

            return Task.FromResult(new StreamGroupDelConsumerResult(StreamGroupDelConsumerResultStatus.Ok, removed));
        }

        /// <summary>
        /// Reads entries for a consumer group.
        /// </summary>
        public async Task<StreamGroupReadResult> StreamGroupReadAsync(
            string group,
            string consumer,
            string[] keys,
            string[] ids,
            int? count,
            TimeSpan? block,
            CancellationToken token = default)
        {
            var initial = TryReadGroup(group, consumer, keys, ids, count, allowBlock: false, out var status, out var results);
            if (initial)
            {
                return new StreamGroupReadResult(status, results);
            }

            if (status != StreamGroupReadResultStatus.Ok)
            {
                return new StreamGroupReadResult(status, Array.Empty<StreamReadResult>());
            }

            if (block.HasValue && block.Value > TimeSpan.Zero)
            {
                await Task.Delay(block.Value, token);
                _ = TryReadGroup(group, consumer, keys, ids, count, allowBlock: true, out status, out results);
                return new StreamGroupReadResult(status, results);
            }

            return new StreamGroupReadResult(StreamGroupReadResultStatus.Ok, Array.Empty<StreamReadResult>());
        }

        /// <summary>
        /// Acknowledges pending entries for a consumer group.
        /// </summary>
        public Task<StreamAckResult> StreamAckAsync(
            string key,
            string group,
            string[] ids,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
                return Task.FromResult(new StreamAckResult(StreamAckResultStatus.NoStream, 0));
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key))
            {
                return Task.FromResult(new StreamAckResult(StreamAckResultStatus.WrongType, 0));
            }

            if (!_streams.ContainsKey(key))
            {
                return Task.FromResult(new StreamAckResult(StreamAckResultStatus.NoStream, 0));
            }

            if (!_streamGroups.TryGetValue(key, out var groups) || !groups.TryGetValue(group, out var state))
            {
                return Task.FromResult(new StreamAckResult(StreamAckResultStatus.NoGroup, 0));
            }

            long removed = 0;
            foreach (var id in ids)
            {
                if (state.Pending.Remove(id))
                {
                    removed++;
                }
            }

            return Task.FromResult(new StreamAckResult(StreamAckResultStatus.Ok, removed));
        }

        /// <summary>
        /// Returns pending entry information for a consumer group.
        /// </summary>
        public Task<StreamPendingResult> StreamPendingAsync(
            string key,
            string group,
            long? minIdleTimeMs = null,
            string? start = null,
            string? end = null,
            int? count = null,
            string? consumer = null,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
                return Task.FromResult(new StreamPendingResult(
                    StreamPendingResultStatus.NoStream, 0, null, null,
                    Array.Empty<StreamPendingConsumerInfo>(), Array.Empty<StreamPendingEntry>()));
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key))
            {
                return Task.FromResult(new StreamPendingResult(
                    StreamPendingResultStatus.WrongType, 0, null, null,
                    Array.Empty<StreamPendingConsumerInfo>(), Array.Empty<StreamPendingEntry>()));
            }

            if (!_streams.ContainsKey(key))
            {
                return Task.FromResult(new StreamPendingResult(
                    StreamPendingResultStatus.NoStream, 0, null, null,
                    Array.Empty<StreamPendingConsumerInfo>(), Array.Empty<StreamPendingEntry>()));
            }

            if (!_streamGroups.TryGetValue(key, out var groups) || !groups.TryGetValue(group, out var state))
            {
                return Task.FromResult(new StreamPendingResult(
                    StreamPendingResultStatus.NoGroup, 0, null, null,
                    Array.Empty<StreamPendingConsumerInfo>(), Array.Empty<StreamPendingEntry>()));
            }

            var now = DateTimeOffset.UtcNow;
            var pendingList = state.Pending
                .Select(p => new
                {
                    Id = p.Key,
                    Info = p.Value,
                    IdleMs = (long)(now - p.Value.DeliveryTime).TotalMilliseconds
                })
                .ToList();

            // Apply filters
            if (minIdleTimeMs.HasValue)
            {
                pendingList = pendingList.Where(p => p.IdleMs >= minIdleTimeMs.Value).ToList();
            }

            if (!string.IsNullOrEmpty(consumer))
            {
                pendingList = pendingList.Where(p => p.Info.Consumer == consumer).ToList();
            }

            if (!string.IsNullOrEmpty(start) && TryParseStreamId(start, out var startId))
            {
                pendingList = pendingList.Where(p =>
                {
                    if (TryParseStreamId(p.Id, out var id))
                    {
                        return id.CompareTo(startId) >= 0;
                    }
                    return false;
                }).ToList();
            }

            if (!string.IsNullOrEmpty(end) && TryParseStreamId(end, out var endId))
            {
                pendingList = pendingList.Where(p =>
                {
                    if (TryParseStreamId(p.Id, out var id))
                    {
                        return id.CompareTo(endId) <= 0;
                    }
                    return false;
                }).ToList();
            }

            // If start/end/count is specified, return extended form
            if (start != null || end != null || count.HasValue)
            {
                var limitedList = count.HasValue
                    ? pendingList.Take(count.Value).ToList()
                    : pendingList;

                var entries = limitedList
                    .Select(p => new StreamPendingEntry(p.Id, p.Info.Consumer, p.IdleMs, p.Info.DeliveryCount))
                    .ToArray();

                return Task.FromResult(new StreamPendingResult(
                    StreamPendingResultStatus.Ok, pendingList.Count, null, null,
                    Array.Empty<StreamPendingConsumerInfo>(), entries));
            }

            // Return summary form
            if (pendingList.Count == 0)
            {
                return Task.FromResult(new StreamPendingResult(
                    StreamPendingResultStatus.Ok, 0, null, null,
                    Array.Empty<StreamPendingConsumerInfo>(), Array.Empty<StreamPendingEntry>()));
            }

            var orderedIds = pendingList
                .Select(p => new { p.Id, Parsed = TryParseStreamId(p.Id, out var parsed) ? parsed : new StreamId(0, 0) })
                .OrderBy(x => x.Parsed)
                .ToList();

            var smallestId = orderedIds.First().Id;
            var largestId = orderedIds.Last().Id;

            var consumerCounts = pendingList
                .GroupBy(p => p.Info.Consumer)
                .Select(g => new StreamPendingConsumerInfo(g.Key, g.Count()))
                .ToArray();

            return Task.FromResult(new StreamPendingResult(
                StreamPendingResultStatus.Ok, pendingList.Count, smallestId, largestId,
                consumerCounts, Array.Empty<StreamPendingEntry>()));
        }

        /// <summary>
        /// Claims pending entries for a new consumer.
        /// </summary>
        public Task<StreamClaimResult> StreamClaimAsync(
            string key,
            string group,
            string consumer,
            long minIdleTimeMs,
            string[] ids,
            long? idleMs = null,
            long? timeMs = null,
            long? retryCount = null,
            bool force = false,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
                return Task.FromResult(new StreamClaimResult(
                    StreamClaimResultStatus.NoStream, Array.Empty<StreamEntry>()));
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key))
            {
                return Task.FromResult(new StreamClaimResult(
                    StreamClaimResultStatus.WrongType, Array.Empty<StreamEntry>()));
            }

            if (!_streams.TryGetValue(key, out var stream))
            {
                return Task.FromResult(new StreamClaimResult(
                    StreamClaimResultStatus.NoStream, Array.Empty<StreamEntry>()));
            }

            if (!_streamGroups.TryGetValue(key, out var groups) || !groups.TryGetValue(group, out var state))
            {
                return Task.FromResult(new StreamClaimResult(
                    StreamClaimResultStatus.NoGroup, Array.Empty<StreamEntry>()));
            }

            var now = DateTimeOffset.UtcNow;
            var claimedEntries = new List<StreamEntry>();

            foreach (var id in ids)
            {
                // Find the stream entry
                var streamEntry = stream.FirstOrDefault(e => e.Id == id);
                if (streamEntry == null)
                {
                    continue;
                }

                // Check if entry exists in pending
                if (state.Pending.TryGetValue(id, out var pendingInfo))
                {
                    // Check if idle time is sufficient
                    var idleTime = (long)(now - pendingInfo.DeliveryTime).TotalMilliseconds;
                    if (idleTime < minIdleTimeMs)
                    {
                        continue;
                    }

                    // Update the pending entry
                    pendingInfo.Consumer = consumer;
                    pendingInfo.DeliveryTime = timeMs.HasValue 
                        ? DateTimeOffset.FromUnixTimeMilliseconds(timeMs.Value) 
                        : now;
                    
                    if (idleMs.HasValue)
                    {
                        pendingInfo.DeliveryTime = now.AddMilliseconds(-idleMs.Value);
                    }
                    
                    if (retryCount.HasValue)
                    {
                        pendingInfo.DeliveryCount = retryCount.Value;
                    }
                    else
                    {
                        pendingInfo.DeliveryCount++;
                    }

                    claimedEntries.Add(new StreamEntry(streamEntry.Id, streamEntry.Fields));
                }
                else if (force)
                {
                    // FORCE creates the pending entry if it doesn't exist
                    var deliveryTime = timeMs.HasValue
                        ? DateTimeOffset.FromUnixTimeMilliseconds(timeMs.Value)
                        : now;
                    
                    if (idleMs.HasValue)
                    {
                        deliveryTime = now.AddMilliseconds(-idleMs.Value);
                    }

                    var count = retryCount ?? 1;
                    state.Pending[id] = new PendingEntryInfo(consumer, deliveryTime, count);
                    claimedEntries.Add(new StreamEntry(streamEntry.Id, streamEntry.Fields));
                }
            }

            return Task.FromResult(new StreamClaimResult(
                StreamClaimResultStatus.Ok, claimedEntries.ToArray()));
        }

        /// <summary>
        /// Returns stream entries within the specified id range.
        /// </summary>
        public Task<StreamEntry[]> StreamRangeAsync(
            string key,
            string start,
            string end,
            int? count,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
                return Task.FromResult(Array.Empty<StreamEntry>());
            }

            if (!_streams.TryGetValue(key, out var stream))
            {
                return Task.FromResult(Array.Empty<StreamEntry>());
            }

            var startId = start == "-" ? new StreamId(-1, -1) : TryParseStreamId(start, out var parsedStart)
                ? parsedStart
                : new StreamId(-1, -1);

            var endId = end == "+" ? new StreamId(long.MaxValue, long.MaxValue) : TryParseStreamId(end, out var parsedEnd)
                ? parsedEnd
                : new StreamId(long.MaxValue, long.MaxValue);

            var entries = new List<StreamEntry>();
            foreach (var entry in stream)
            {
                if (entry.ParsedId.CompareTo(startId) < 0)
                {
                    continue;
                }

                if (entry.ParsedId.CompareTo(endId) > 0)
                {
                    break;
                }

                entries.Add(new StreamEntry(entry.Id, entry.Fields));
                if (count.HasValue && entries.Count >= count.Value)
                {
                    break;
                }
            }

            return Task.FromResult(entries.ToArray());
        }

        /// <summary>
        /// Returns stream entries within the specified id range, in reverse order.
        /// </summary>
        public Task<StreamEntry[]> StreamRangeReverseAsync(
            string key,
            string start,
            string end,
            int? count,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
                return Task.FromResult(Array.Empty<StreamEntry>());
            }

            if (!_streams.TryGetValue(key, out var stream))
            {
                return Task.FromResult(Array.Empty<StreamEntry>());
            }

            var startId = start == "+" ? new StreamId(long.MaxValue, long.MaxValue) : TryParseStreamId(start, out var parsedStart)
                ? parsedStart
                : new StreamId(long.MaxValue, long.MaxValue);

            var endId = end == "-" ? new StreamId(-1, -1) : TryParseStreamId(end, out var parsedEnd)
                ? parsedEnd
                : new StreamId(-1, -1);

            var entries = new List<StreamEntry>();
            for (int i = stream.Count - 1; i >= 0; i--)
            {
                var entry = stream[i];
                if (entry.ParsedId.CompareTo(startId) > 0)
                {
                    continue;
                }

                if (entry.ParsedId.CompareTo(endId) < 0)
                {
                    break;
                }

                entries.Add(new StreamEntry(entry.Id, entry.Fields));
                if (count.HasValue && entries.Count >= count.Value)
                {
                    break;
                }
            }

            return Task.FromResult(entries.ToArray());
        }

        /// <summary>
        /// Sets a hash field value.
        /// </summary>
        public Task<bool> HashSetAsync(string key, string field, byte[] value, CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (!_hashes.TryGetValue(key, out var hash))
            {
                hash = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                _hashes[key] = hash;
                _data.Remove(key);
                _lists.Remove(key);
                _sets.Remove(key);
                _sortedSets.Remove(key);
                _streams.Remove(key);
                _streamLastIds.Remove(key);
                _streamGroups.Remove(key);
            }

            var isNew = !hash.ContainsKey(field);
            hash[field] = value;
            return Task.FromResult(isNew);
        }

        /// <summary>
        /// Gets a hash field value.
        /// </summary>
        public Task<byte[]?> HashGetAsync(string key, string field, CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
                return Task.FromResult<byte[]?>(null);
            }

            if (!_hashes.TryGetValue(key, out var hash) || !hash.TryGetValue(field, out var value))
            {
                return Task.FromResult<byte[]?>(null);
            }

            return Task.FromResult<byte[]?>(value);
        }

        /// <summary>
        /// Deletes hash fields and returns the count removed.
        /// </summary>
        public Task<long> HashDeleteAsync(string key, string[] fields, CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
                return Task.FromResult(0L);
            }

            if (!_hashes.TryGetValue(key, out var hash))
            {
                return Task.FromResult(0L);
            }

            long removed = 0;
            foreach (var field in fields)
            {
                if (hash.Remove(field))
                {
                    removed++;
                }
            }

            if (hash.Count == 0)
            {
                _hashes.Remove(key);
                _expirations.Remove(key);
            }

            return Task.FromResult(removed);
        }

        /// <summary>
        /// Gets all hash fields and values.
        /// </summary>
        public Task<KeyValuePair<string, byte[]>[]> HashGetAllAsync(string key, CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
                return Task.FromResult(Array.Empty<KeyValuePair<string, byte[]>>());
            }

            if (!_hashes.TryGetValue(key, out var hash))
            {
                return Task.FromResult(Array.Empty<KeyValuePair<string, byte[]>>());
            }

            var items = new KeyValuePair<string, byte[]>[hash.Count];
            int index = 0;
            foreach (var pair in hash)
            {
                items[index++] = pair;
            }

            return Task.FromResult(items);
        }

        /// <summary>
        /// Pushes values onto a list and returns the new length.
        /// </summary>
        public Task<ListPushResult> ListPushAsync(
            string key,
            byte[][] values,
            bool left,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key) || _sets.ContainsKey(key) || _sortedSets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(new ListPushResult(ListResultStatus.WrongType, 0));
            }

            if (!_lists.TryGetValue(key, out var list))
            {
                list = new List<byte[]>();
                _lists[key] = list;
                _data.Remove(key);
                _hashes.Remove(key);
                _sets.Remove(key);
                _sortedSets.Remove(key);
                _streams.Remove(key);
                _streamLastIds.Remove(key);
                _streamGroups.Remove(key);
            }

            if (left)
            {
                for (int i = 0; i < values.Length; i++)
                {
                    list.Insert(0, values[i]);
                }
            }
            else
            {
                list.AddRange(values);
            }

            return Task.FromResult(new ListPushResult(ListResultStatus.Ok, list.Count));
        }

        /// <summary>
        /// Pops a value from a list.
        /// </summary>
        public Task<ListPopResult> ListPopAsync(
            string key,
            bool left,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key) || _sets.ContainsKey(key) || _sortedSets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(new ListPopResult(ListResultStatus.WrongType, null));
            }

            if (!_lists.TryGetValue(key, out var list) || list.Count == 0)
            {
                return Task.FromResult(new ListPopResult(ListResultStatus.Ok, null));
            }

            byte[] value;
            if (left)
            {
                value = list[0];
                list.RemoveAt(0);
            }
            else
            {
                var index = list.Count - 1;
                value = list[index];
                list.RemoveAt(index);
            }

            if (list.Count == 0)
            {
                _lists.Remove(key);
                _expirations.Remove(key);
            }

            return Task.FromResult(new ListPopResult(ListResultStatus.Ok, value));
        }

        /// <summary>
        /// Returns a range of values from a list.
        /// </summary>
        public Task<ListRangeResult> ListRangeAsync(
            string key,
            int start,
            int stop,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key) || _sets.ContainsKey(key) || _sortedSets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(new ListRangeResult(ListResultStatus.WrongType, Array.Empty<byte[]>()));
            }

            if (!_lists.TryGetValue(key, out var list) || list.Count == 0)
            {
                return Task.FromResult(new ListRangeResult(ListResultStatus.Ok, Array.Empty<byte[]>()));
            }

            var length = list.Count;
            var startIndex = start < 0 ? length + start : start;
            var stopIndex = stop < 0 ? length + stop : stop;

            if (startIndex < 0)
            {
                startIndex = 0;
            }

            if (stopIndex < 0)
            {
                stopIndex = 0;
            }

            if (startIndex >= length)
            {
                return Task.FromResult(new ListRangeResult(ListResultStatus.Ok, Array.Empty<byte[]>()));
            }

            if (stopIndex >= length)
            {
                stopIndex = length - 1;
            }

            if (stopIndex < startIndex)
            {
                return Task.FromResult(new ListRangeResult(ListResultStatus.Ok, Array.Empty<byte[]>()));
            }

            var count = stopIndex - startIndex + 1;
            var values = new byte[count][];
            for (int i = 0; i < count; i++)
            {
                values[i] = list[startIndex + i];
            }

            return Task.FromResult(new ListRangeResult(ListResultStatus.Ok, values));
        }

        /// <summary>
        /// Returns the length of a list.
        /// </summary>
        public Task<ListLengthResult> ListLengthAsync(string key, CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key) || _sets.ContainsKey(key) || _sortedSets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(new ListLengthResult(ListResultStatus.WrongType, 0));
            }

            return Task.FromResult(_lists.TryGetValue(key, out var list)
                ? new ListLengthResult(ListResultStatus.Ok, list.Count)
                : new ListLengthResult(ListResultStatus.Ok, 0));
        }

        /// <summary>
        /// Returns the value at a list index.
        /// </summary>
        public Task<ListIndexResult> ListIndexAsync(string key, int index, CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key) || _sets.ContainsKey(key) || _sortedSets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(new ListIndexResult(ListResultStatus.WrongType, null));
            }

            if (!_lists.TryGetValue(key, out var list) || list.Count == 0)
            {
                return Task.FromResult(new ListIndexResult(ListResultStatus.Ok, null));
            }

            var resolved = index < 0 ? list.Count + index : index;
            if (resolved < 0 || resolved >= list.Count)
            {
                return Task.FromResult(new ListIndexResult(ListResultStatus.Ok, null));
            }

            return Task.FromResult(new ListIndexResult(ListResultStatus.Ok, list[resolved]));
        }

        /// <summary>
        /// Sets the value at a list index.
        /// </summary>
        public Task<ListSetResult> ListSetAsync(
            string key,
            int index,
            byte[] value,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key) || _sets.ContainsKey(key) || _sortedSets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(new ListSetResult(ListSetResultStatus.WrongType));
            }

            if (!_lists.TryGetValue(key, out var list) || list.Count == 0)
            {
                return Task.FromResult(new ListSetResult(ListSetResultStatus.OutOfRange));
            }

            var resolved = index < 0 ? list.Count + index : index;
            if (resolved < 0 || resolved >= list.Count)
            {
                return Task.FromResult(new ListSetResult(ListSetResultStatus.OutOfRange));
            }

            list[resolved] = value;
            return Task.FromResult(new ListSetResult(ListSetResultStatus.Ok));
        }

        /// <summary>
        /// Trims a list to the specified range.
        /// </summary>
        public Task<ListResultStatus> ListTrimAsync(
            string key,
            int start,
            int stop,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key) || _sets.ContainsKey(key) || _sortedSets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(ListResultStatus.WrongType);
            }

            if (!_lists.TryGetValue(key, out var list) || list.Count == 0)
            {
                return Task.FromResult(ListResultStatus.Ok);
            }

            var length = list.Count;
            var startIndex = start < 0 ? length + start : start;
            var stopIndex = stop < 0 ? length + stop : stop;

            if (startIndex < 0)
            {
                startIndex = 0;
            }

            if (stopIndex < 0)
            {
                stopIndex = 0;
            }

            if (startIndex >= length || stopIndex < startIndex)
            {
                _lists.Remove(key);
                _expirations.Remove(key);
                return Task.FromResult(ListResultStatus.Ok);
            }

            if (stopIndex >= length)
            {
                stopIndex = length - 1;
            }

            var count = stopIndex - startIndex + 1;
            var trimmed = new List<byte[]>(count);
            for (int i = 0; i < count; i++)
            {
                trimmed.Add(list[startIndex + i]);
            }

            _lists[key] = trimmed;
            return Task.FromResult(ListResultStatus.Ok);
        }

        /// <summary>
        /// Adds members to a set.
        /// </summary>
        public Task<SetCountResult> SetAddAsync(
            string key,
            byte[][] members,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sortedSets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(new SetCountResult(SetResultStatus.WrongType, 0));
            }

            if (!_sets.TryGetValue(key, out var set))
            {
                set = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                _sets[key] = set;
                _data.Remove(key);
                _hashes.Remove(key);
                _lists.Remove(key);
                _sortedSets.Remove(key);
                _streams.Remove(key);
                _streamLastIds.Remove(key);
                _streamGroups.Remove(key);
            }

            long added = 0;
            foreach (var member in members)
            {
                var encoded = Convert.ToBase64String(member);
                if (!set.ContainsKey(encoded))
                {
                    set[encoded] = member;
                    added++;
                }
            }

            return Task.FromResult(new SetCountResult(SetResultStatus.Ok, added));
        }

        /// <summary>
        /// Removes members from a set.
        /// </summary>
        public Task<SetCountResult> SetRemoveAsync(
            string key,
            byte[][] members,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sortedSets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(new SetCountResult(SetResultStatus.WrongType, 0));
            }

            if (!_sets.TryGetValue(key, out var set) || set.Count == 0)
            {
                return Task.FromResult(new SetCountResult(SetResultStatus.Ok, 0));
            }

            long removed = 0;
            foreach (var member in members)
            {
                var encoded = Convert.ToBase64String(member);
                if (set.Remove(encoded))
                {
                    removed++;
                }
            }

            if (set.Count == 0)
            {
                _sets.Remove(key);
                _expirations.Remove(key);
            }

            return Task.FromResult(new SetCountResult(SetResultStatus.Ok, removed));
        }

        /// <summary>
        /// Returns all members of a set.
        /// </summary>
        public Task<SetMembersResult> SetMembersAsync(
            string key,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sortedSets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(new SetMembersResult(SetResultStatus.WrongType, Array.Empty<byte[]>()));
            }

            if (!_sets.TryGetValue(key, out var set) || set.Count == 0)
            {
                return Task.FromResult(new SetMembersResult(SetResultStatus.Ok, Array.Empty<byte[]>()));
            }

            var members = new byte[set.Count][];
            int index = 0;
            foreach (var value in set.Values)
            {
                members[index++] = value;
            }

            return Task.FromResult(new SetMembersResult(SetResultStatus.Ok, members));
        }

        /// <summary>
        /// Returns the cardinality of a set.
        /// </summary>
        public Task<SetCountResult> SetCardinalityAsync(string key, CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sortedSets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(new SetCountResult(SetResultStatus.WrongType, 0));
            }

            return Task.FromResult(_sets.TryGetValue(key, out var set)
                ? new SetCountResult(SetResultStatus.Ok, set.Count)
                : new SetCountResult(SetResultStatus.Ok, 0));
        }

        /// <summary>
        /// Adds members to a sorted set.
        /// </summary>
        public Task<SortedSetCountResult> SortedSetAddAsync(
            string key,
            SortedSetEntry[] entries,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(new SortedSetCountResult(SortedSetResultStatus.WrongType, 0));
            }

            if (!_sortedSets.TryGetValue(key, out var set))
            {
                set = new Dictionary<string, SortedSetMember>(StringComparer.Ordinal);
                _sortedSets[key] = set;
                _data.Remove(key);
                _hashes.Remove(key);
                _lists.Remove(key);
                _sets.Remove(key);
                _streams.Remove(key);
                _streamLastIds.Remove(key);
                _streamGroups.Remove(key);
            }

            long added = 0;
            foreach (var entry in entries)
            {
                var encoded = Convert.ToBase64String(entry.Member);
                if (set.TryGetValue(encoded, out var existing))
                {
                    existing.Score = entry.Score;
                }
                else
                {
                    set[encoded] = new SortedSetMember(encoded, entry.Member, entry.Score);
                    added++;
                }
            }

            return Task.FromResult(new SortedSetCountResult(SortedSetResultStatus.Ok, added));
        }

        /// <summary>
        /// Removes members from a sorted set.
        /// </summary>
        public Task<SortedSetCountResult> SortedSetRemoveAsync(
            string key,
            byte[][] members,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(new SortedSetCountResult(SortedSetResultStatus.WrongType, 0));
            }

            if (!_sortedSets.TryGetValue(key, out var set) || set.Count == 0)
            {
                return Task.FromResult(new SortedSetCountResult(SortedSetResultStatus.Ok, 0));
            }

            long removed = 0;
            foreach (var member in members)
            {
                var encoded = Convert.ToBase64String(member);
                if (set.Remove(encoded))
                {
                    removed++;
                }
            }

            if (set.Count == 0)
            {
                _sortedSets.Remove(key);
                _expirations.Remove(key);
            }

            return Task.FromResult(new SortedSetCountResult(SortedSetResultStatus.Ok, removed));
        }

        /// <summary>
        /// Returns a range of members in a sorted set.
        /// </summary>
        public Task<SortedSetRangeResult> SortedSetRangeAsync(
            string key,
            int start,
            int stop,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(new SortedSetRangeResult(SortedSetResultStatus.WrongType, Array.Empty<SortedSetEntry>()));
            }

            if (!_sortedSets.TryGetValue(key, out var set) || set.Count == 0)
            {
                return Task.FromResult(new SortedSetRangeResult(SortedSetResultStatus.Ok, Array.Empty<SortedSetEntry>()));
            }

            var ordered = set.Values
                .OrderBy(entry => entry.Score)
                .ThenBy(entry => entry.Key, StringComparer.Ordinal)
                .ToList();

            var length = ordered.Count;
            var startIndex = start < 0 ? length + start : start;
            var stopIndex = stop < 0 ? length + stop : stop;

            if (startIndex < 0)
            {
                startIndex = 0;
            }

            if (stopIndex < 0)
            {
                stopIndex = 0;
            }

            if (startIndex >= length || stopIndex < startIndex)
            {
                return Task.FromResult(new SortedSetRangeResult(SortedSetResultStatus.Ok, Array.Empty<SortedSetEntry>()));
            }

            if (stopIndex >= length)
            {
                stopIndex = length - 1;
            }

            var count = stopIndex - startIndex + 1;
            var entries = new SortedSetEntry[count];
            for (int i = 0; i < count; i++)
            {
                var entry = ordered[startIndex + i];
                entries[i] = new SortedSetEntry(entry.Member, entry.Score);
            }

            return Task.FromResult(new SortedSetRangeResult(SortedSetResultStatus.Ok, entries));
        }

        /// <summary>
        /// Returns the cardinality of a sorted set.
        /// </summary>
        public Task<SortedSetCountResult> SortedSetCardinalityAsync(string key, CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(new SortedSetCountResult(SortedSetResultStatus.WrongType, 0));
            }

            return Task.FromResult(_sortedSets.TryGetValue(key, out var set)
                ? new SortedSetCountResult(SortedSetResultStatus.Ok, set.Count)
                : new SortedSetCountResult(SortedSetResultStatus.Ok, 0));
        }

        public Task<SortedSetRangeResult> SortedSetRangeByScoreAsync(string key, double minScore, double maxScore, CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(new SortedSetRangeResult(SortedSetResultStatus.WrongType, Array.Empty<SortedSetEntry>()));
            }

            if (!_sortedSets.TryGetValue(key, out var set))
            {
                return Task.FromResult(new SortedSetRangeResult(SortedSetResultStatus.Ok, Array.Empty<SortedSetEntry>()));
            }

            var entries = set
                .Where(m => m.Value.Score >= minScore && m.Value.Score <= maxScore)
                .OrderBy(m => m.Value.Score)
                .ThenBy(m => m.Key, StringComparer.Ordinal)
                .Select(m => new SortedSetEntry(Convert.FromBase64String(m.Key), m.Value.Score))
                .ToArray();

            return Task.FromResult(new SortedSetRangeResult(SortedSetResultStatus.Ok, entries));
        }

        public Task<SortedSetScoreResult> SortedSetScoreAsync(string key, byte[] member, CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(new SortedSetScoreResult(SortedSetResultStatus.WrongType, null));
            }

            if (!_sortedSets.TryGetValue(key, out var set))
            {
                return Task.FromResult(new SortedSetScoreResult(SortedSetResultStatus.Ok, null));
            }

            var memberKey = Convert.ToBase64String(member);
            if (set.TryGetValue(memberKey, out var member_data))
            {
                return Task.FromResult(new SortedSetScoreResult(SortedSetResultStatus.Ok, member_data.Score));
            }

            return Task.FromResult(new SortedSetScoreResult(SortedSetResultStatus.Ok, null));
        }

        public Task<SortedSetScoreResult> SortedSetIncrementAsync(string key, double increment, byte[] member, CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(new SortedSetScoreResult(SortedSetResultStatus.WrongType, null));
            }

            if (!_sortedSets.TryGetValue(key, out var set))
            {
                set = new Dictionary<string, SortedSetMember>();
                _sortedSets[key] = set;
            }

            var memberKey = Convert.ToBase64String(member);
            double newScore;
            if (set.TryGetValue(memberKey, out var member_data))
            {
                newScore = member_data.Score + increment;
                set[memberKey] = new SortedSetMember(memberKey, member, newScore);
            }
            else
            {
                newScore = increment;
                set[memberKey] = new SortedSetMember(memberKey, member, newScore);
            }

            return Task.FromResult(new SortedSetScoreResult(SortedSetResultStatus.Ok, newScore));
        }

        public Task<SortedSetCountResult> SortedSetCountByScoreAsync(string key, double minScore, double maxScore, CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(new SortedSetCountResult(SortedSetResultStatus.WrongType, 0));
            }

            if (!_sortedSets.TryGetValue(key, out var set))
            {
                return Task.FromResult(new SortedSetCountResult(SortedSetResultStatus.Ok, 0));
            }

            var count = set.Values.Count(m => m.Score >= minScore && m.Score <= maxScore);
            return Task.FromResult(new SortedSetCountResult(SortedSetResultStatus.Ok, count));
        }

        public Task<SortedSetRankResult> SortedSetRankAsync(string key, byte[] member, CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(new SortedSetRankResult(SortedSetResultStatus.WrongType, null));
            }

            if (!_sortedSets.TryGetValue(key, out var set))
            {
                return Task.FromResult(new SortedSetRankResult(SortedSetResultStatus.Ok, null));
            }

            var memberKey = Convert.ToBase64String(member);
            if (!set.ContainsKey(memberKey))
            {
                return Task.FromResult(new SortedSetRankResult(SortedSetResultStatus.Ok, null));
            }

            var ordered = set.OrderBy(m => m.Value.Score).ThenBy(m => m.Key).ToList();
            var rank = ordered.FindIndex(m => m.Key == memberKey);
            return Task.FromResult(new SortedSetRankResult(SortedSetResultStatus.Ok, (long)rank));
        }

        public Task<SortedSetRankResult> SortedSetReverseRankAsync(string key, byte[] member, CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(new SortedSetRankResult(SortedSetResultStatus.WrongType, null));
            }

            if (!_sortedSets.TryGetValue(key, out var set))
            {
                return Task.FromResult(new SortedSetRankResult(SortedSetResultStatus.Ok, null));
            }

            var memberKey = Convert.ToBase64String(member);
            if (!set.ContainsKey(memberKey))
            {
                return Task.FromResult(new SortedSetRankResult(SortedSetResultStatus.Ok, null));
            }

            var ordered = set.OrderByDescending(m => m.Value.Score).ThenBy(m => m.Key).ToList();
            var rank = ordered.FindIndex(m => m.Key == memberKey);
            return Task.FromResult(new SortedSetRankResult(SortedSetResultStatus.Ok, (long)rank));
        }

        public Task<SortedSetRemoveRangeResult> SortedSetRemoveRangeByScoreAsync(string key, double minScore, double maxScore, CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(new SortedSetRemoveRangeResult(SortedSetResultStatus.WrongType, 0));
            }

            if (!_sortedSets.TryGetValue(key, out var set))
            {
                return Task.FromResult(new SortedSetRemoveRangeResult(SortedSetResultStatus.Ok, 0));
            }

            var toRemove = set.Where(m => m.Value.Score >= minScore && m.Value.Score <= maxScore).Select(m => m.Key).ToList();
            foreach (var memberKey in toRemove)
            {
                set.Remove(memberKey);
            }

            if (set.Count == 0)
            {
                _sortedSets.Remove(key);
            }

            return Task.FromResult(new SortedSetRemoveRangeResult(SortedSetResultStatus.Ok, toRemove.Count));
        }

        /// <summary>
        /// Retrieves a stored value while honoring expiration.
        /// </summary>
        private byte[]? GetValue(string key)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
                return null;
            }

            if (!_data.TryGetValue(key, out var value))
            {
                return null;
            }

            if (!_expirations.TryGetValue(key, out var expiresAt) || expiresAt == null)
            {
                return value;
            }

            if (expiresAt.Value > DateTimeOffset.UtcNow)
            {
                return value;
            }

            RemoveKey(key);
            return null;
        }

        /// <summary>
        /// Determines whether a key exists in any data structure.
        /// </summary>
        private bool HasKey(string key)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
                return false;
            }

            return _data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sets.ContainsKey(key) || _sortedSets.ContainsKey(key) || _streams.ContainsKey(key);
        }

        /// <summary>
        /// Returns whether a key is expired.
        /// </summary>
        private bool IsExpired(string key)
        {
            if (!_expirations.TryGetValue(key, out var expiresAt) || expiresAt == null)
            {
                return false;
            }

            return expiresAt.Value <= DateTimeOffset.UtcNow;
        }

        /// <summary>
        /// Removes a key from all structures.
        /// </summary>
        private bool RemoveKey(string key)
        {
            var removed = _data.Remove(key);
            removed |= _hashes.Remove(key);
            removed |= _lists.Remove(key);
            removed |= _sets.Remove(key);
            removed |= _sortedSets.Remove(key);
            removed |= _streams.Remove(key);
            _streamLastIds.Remove(key);
            _streamGroups.Remove(key);
            _expirations.Remove(key);
            return removed;
        }

        /// <summary>
        /// Parses a stream id into a comparable structure.
        /// </summary>
        private static bool TryParseStreamId(string text, out StreamId id)
        {
            id = default;
            var parts = text.Split('-');
            if (parts.Length != 2)
            {
                return false;
            }

            if (!long.TryParse(parts[0], out var ms) || !long.TryParse(parts[1], out var seq))
            {
                return false;
            }

            id = new StreamId(ms, seq);
            return true;
        }

        /// <summary>
        /// Formats a stream id from its components.
        /// </summary>
        private static string FormatStreamId(StreamId id)
        {
            return $"{id.Ms}-{id.Seq}";
        }

        /// <summary>
        /// Resolves the start id for a consumer group creation request.
        /// </summary>
        private bool TryResolveGroupStartId(
            string key,
            List<StreamEntryModel> stream,
            string startId,
            out StreamId lastId)
        {
            lastId = new StreamId(-1, -1);

            if (startId == "-")
            {
                return true;
            }

            if (startId == "$")
            {
                if (_streamLastIds.TryGetValue(key, out var lastStreamId))
                {
                    lastId = lastStreamId;
                }
                else if (stream.Count > 0)
                {
                    lastId = stream[stream.Count - 1].ParsedId;
                }

                return true;
            }

            return TryParseStreamId(startId, out lastId);
        }

        /// <summary>
        /// Reads entries for a consumer group, optionally allowing block semantics.
        /// </summary>
        private bool TryReadGroup(
            string group,
            string consumer,
            string[] keys,
            string[] ids,
            int? count,
            bool allowBlock,
            out StreamGroupReadResultStatus status,
            out StreamReadResult[] results)
        {
            status = StreamGroupReadResultStatus.Ok;
            var output = new List<StreamReadResult>();

            for (int i = 0; i < keys.Length; i++)
            {
                var key = keys[i];
                if (IsExpired(key))
                {
                    RemoveKey(key);
                    status = StreamGroupReadResultStatus.NoStream;
                    results = Array.Empty<StreamReadResult>();
                    return true;
                }

                if (_data.ContainsKey(key) || _hashes.ContainsKey(key))
                {
                    status = StreamGroupReadResultStatus.WrongType;
                    results = Array.Empty<StreamReadResult>();
                    return true;
                }

                if (!_streams.TryGetValue(key, out var stream))
                {
                    status = StreamGroupReadResultStatus.NoStream;
                    results = Array.Empty<StreamReadResult>();
                    return true;
                }

                if (!_streamGroups.TryGetValue(key, out var groups) || !groups.TryGetValue(group, out var state))
                {
                    status = StreamGroupReadResultStatus.NoGroup;
                    results = Array.Empty<StreamReadResult>();
                    return true;
                }

                if (!TryResolveReadStartId(ids[i], state, out var startId, out var usePending))
                {
                    status = StreamGroupReadResultStatus.InvalidId;
                    results = Array.Empty<StreamReadResult>();
                    return true;
                }

                var entries = new List<StreamEntry>();
                foreach (var entry in stream)
                {
                    if (usePending)
                    {
                        if (!state.Pending.ContainsKey(entry.Id))
                        {
                            continue;
                        }
                    }

                    if (entry.ParsedId.CompareTo(startId) <= 0)
                    {
                        continue;
                    }

                    entries.Add(new StreamEntry(entry.Id, entry.Fields));
                    if (!usePending)
                    {
                        var now = DateTimeOffset.UtcNow;
                        if (state.Pending.TryGetValue(entry.Id, out var existing))
                        {
                            existing.DeliveryCount++;
                            existing.DeliveryTime = now;
                            existing.Consumer = consumer;
                        }
                        else
                        {
                            state.Pending[entry.Id] = new PendingEntryInfo(consumer, now, 1);
                        }
                    }

                    if (count.HasValue && entries.Count >= count.Value)
                    {
                        break;
                    }
                }

                if (entries.Count > 0)
                {
                    if (!usePending)
                    {
                        var lastEntry = entries[entries.Count - 1];
                        if (TryParseStreamId(lastEntry.Id, out var parsed))
                        {
                            state.LastDeliveredId = parsed;
                        }
                    }

                    output.Add(new StreamReadResult(key, entries.ToArray()));
                }
            }

            results = output.ToArray();
            return results.Length > 0 || allowBlock;
        }

        /// <summary>
        /// Resolves the read start id and whether to use pending entries.
        /// </summary>
        private static bool TryResolveReadStartId(
            string id,
            StreamGroupState state,
            out StreamId startId,
            out bool usePending)
        {
            usePending = false;
            if (id == ">")
            {
                startId = state.LastDeliveredId;
                return true;
            }

            usePending = true;
            return TryParseStreamId(id, out startId);
        }
    }
}
