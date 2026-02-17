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

            return _data.ContainsKey(key) || _hashes.ContainsKey(key) || _streams.ContainsKey(key);
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
