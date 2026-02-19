using System.Globalization;
using System.Linq;
using System.Numerics;
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
        /// Verifies SETBIT/GETBIT round-trip with automatic bitmap expansion.
        /// </summary>
        public async Task SetBit_GetBit_RoundTrip()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("SETBIT", "bits", "9", "1"));
                channel.RunPendingTasks();

                var setResponse = ReadOutbound(channel);
                var setPrevious = Assert.IsType<IntegerRedisMessage>(setResponse);
                Assert.Equal(0, setPrevious.Value);

                channel.WriteInbound(Command("GETBIT", "bits", "9"));
                channel.RunPendingTasks();

                var getSetBitResponse = ReadOutbound(channel);
                var getSetBit = Assert.IsType<IntegerRedisMessage>(getSetBitResponse);
                Assert.Equal(1, getSetBit.Value);

                channel.WriteInbound(Command("GETBIT", "bits", "8"));
                channel.RunPendingTasks();

                var getUnsetBitResponse = ReadOutbound(channel);
                var getUnsetBit = Assert.IsType<IntegerRedisMessage>(getUnsetBitResponse);
                Assert.Equal(0, getUnsetBit.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies BITCOUNT over full values and ranged byte windows.
        /// </summary>
        public async Task BitCount_CountsFullAndRangedWindows()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("SETBIT", "bits", "0", "1"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("SETBIT", "bits", "8", "1"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("SETBIT", "bits", "15", "1"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("BITCOUNT", "bits"));
                channel.RunPendingTasks();

                var allBitsResponse = ReadOutbound(channel);
                var allBits = Assert.IsType<IntegerRedisMessage>(allBitsResponse);
                Assert.Equal(3, allBits.Value);

                channel.WriteInbound(Command("BITCOUNT", "bits", "0", "0"));
                channel.RunPendingTasks();

                var firstByteResponse = ReadOutbound(channel);
                var firstByte = Assert.IsType<IntegerRedisMessage>(firstByteResponse);
                Assert.Equal(1, firstByte.Value);

                channel.WriteInbound(Command("BITCOUNT", "bits", "-1", "-1"));
                channel.RunPendingTasks();

                var lastByteResponse = ReadOutbound(channel);
                var lastByte = Assert.IsType<IntegerRedisMessage>(lastByteResponse);
                Assert.Equal(2, lastByte.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies BITFIELD supports GET/SET/INCRBY and overflow handling.
        /// </summary>
        public async Task BitField_GetSetIncrBy_AndOverflowModes()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("BITFIELD", "bf", "SET", "u8", "0", "200", "GET", "u8", "0", "INCRBY", "u8", "0", "60"));
                channel.RunPendingTasks();

                var firstResponse = ReadOutbound(channel);
                var firstArray = Assert.IsType<ArrayRedisMessage>(firstResponse);
                Assert.Equal(3, firstArray.Children.Count);
                Assert.Equal(0, Assert.IsType<IntegerRedisMessage>(firstArray.Children[0]).Value);
                Assert.Equal(200, Assert.IsType<IntegerRedisMessage>(firstArray.Children[1]).Value);
                Assert.Equal(4, Assert.IsType<IntegerRedisMessage>(firstArray.Children[2]).Value);

                channel.WriteInbound(Command("BITFIELD", "bf", "SET", "i8", "0", "120"));
                channel.RunPendingTasks();

                var setSignedResponse = ReadOutbound(channel);
                var setSignedArray = Assert.IsType<ArrayRedisMessage>(setSignedResponse);
                Assert.Single(setSignedArray.Children);
                Assert.Equal(4, Assert.IsType<IntegerRedisMessage>(setSignedArray.Children[0]).Value);

                channel.WriteInbound(Command("BITFIELD", "bf", "OVERFLOW", "SAT", "INCRBY", "i8", "0", "20"));
                channel.RunPendingTasks();

                var satResponse = ReadOutbound(channel);
                var satArray = Assert.IsType<ArrayRedisMessage>(satResponse);
                Assert.Single(satArray.Children);
                Assert.Equal(127, Assert.IsType<IntegerRedisMessage>(satArray.Children[0]).Value);

                channel.WriteInbound(Command("BITFIELD", "bf", "OVERFLOW", "FAIL", "INCRBY", "i8", "0", "1", "GET", "i8", "0"));
                channel.RunPendingTasks();

                var failResponse = ReadOutbound(channel);
                var failArray = Assert.IsType<ArrayRedisMessage>(failResponse);
                Assert.Equal(2, failArray.Children.Count);
                Assert.Same(FullBulkStringRedisMessage.Null, failArray.Children[0]);
                Assert.Equal(127, Assert.IsType<IntegerRedisMessage>(failArray.Children[1]).Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies BITOP supports AND/OR/XOR/NOT over string bitmaps.
        /// </summary>
        public async Task BitOp_AndOrXorNot_ReturnExpectedResults()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("SETBIT", "k1", "0", "1"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("SETBIT", "k1", "7", "1"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("SETBIT", "k2", "1", "1"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("SETBIT", "k2", "7", "1"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("BITOP", "AND", "k_and", "k1", "k2"));
                channel.RunPendingTasks();
                Assert.Equal(1, Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel)).Value);

                channel.WriteInbound(Command("BITCOUNT", "k_and"));
                channel.RunPendingTasks();
                Assert.Equal(1, Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel)).Value);

                channel.WriteInbound(Command("BITOP", "OR", "k_or", "k1", "k2"));
                channel.RunPendingTasks();
                Assert.Equal(1, Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel)).Value);

                channel.WriteInbound(Command("BITCOUNT", "k_or"));
                channel.RunPendingTasks();
                Assert.Equal(3, Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel)).Value);

                channel.WriteInbound(Command("BITOP", "XOR", "k_xor", "k1", "k2"));
                channel.RunPendingTasks();
                Assert.Equal(1, Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel)).Value);

                channel.WriteInbound(Command("BITCOUNT", "k_xor"));
                channel.RunPendingTasks();
                Assert.Equal(2, Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel)).Value);

                channel.WriteInbound(Command("BITOP", "NOT", "k_not", "k1"));
                channel.RunPendingTasks();
                Assert.Equal(1, Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel)).Value);

                channel.WriteInbound(Command("BITCOUNT", "k_not"));
                channel.RunPendingTasks();
                Assert.Equal(6, Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel)).Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        /// <summary>
        /// Verifies BITPOS supports default, bounded, and BIT-unit range searches.
        /// </summary>
        public async Task BitPos_DefaultAndRangeModes_ReturnExpectedPositions()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("SETBIT", "bp", "1", "1"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("SETBIT", "bp", "15", "1"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("BITPOS", "bp", "1"));
                channel.RunPendingTasks();
                Assert.Equal(1, Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel)).Value);

                channel.WriteInbound(Command("BITPOS", "bp", "0"));
                channel.RunPendingTasks();
                Assert.Equal(0, Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel)).Value);

                channel.WriteInbound(Command("BITPOS", "bp", "1", "1", "1", "BYTE"));
                channel.RunPendingTasks();
                Assert.Equal(15, Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel)).Value);

                channel.WriteInbound(Command("BITPOS", "bp", "1", "8", "15", "BIT"));
                channel.RunPendingTasks();
                Assert.Equal(15, Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel)).Value);

                for (int i = 0; i < 8; i++)
                {
                    channel.WriteInbound(Command("SETBIT", "full", i.ToString(CultureInfo.InvariantCulture), "1"));
                    channel.RunPendingTasks();
                    _ = ReadOutbound(channel);
                }

                channel.WriteInbound(Command("BITPOS", "full", "0"));
                channel.RunPendingTasks();
                Assert.Equal(8, Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel)).Value);

                channel.WriteInbound(Command("BITPOS", "full", "0", "0", "0"));
                channel.RunPendingTasks();
                Assert.Equal(-1, Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel)).Value);

                channel.WriteInbound(Command("BITPOS", "missing", "1"));
                channel.RunPendingTasks();
                Assert.Equal(-1, Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel)).Value);

                channel.WriteInbound(Command("BITPOS", "missing", "0"));
                channel.RunPendingTasks();
                Assert.Equal(0, Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel)).Value);
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
        public async Task HyperLogLog_PfAdd_ThenPfCount_ReturnsApproximateCount()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("PFADD", "hll", "a", "b", "c"));
                channel.RunPendingTasks();
                var addResponse = ReadOutbound(channel);
                var added = Assert.IsType<IntegerRedisMessage>(addResponse);
                Assert.Equal(1, added.Value);

                channel.WriteInbound(Command("PFADD", "hll", "a", "b", "c"));
                channel.RunPendingTasks();
                var addAgainResponse = ReadOutbound(channel);
                var addAgain = Assert.IsType<IntegerRedisMessage>(addAgainResponse);
                Assert.Equal(0, addAgain.Value);

                channel.WriteInbound(Command("PFCOUNT", "hll"));
                channel.RunPendingTasks();
                var countResponse = ReadOutbound(channel);
                var count = Assert.IsType<IntegerRedisMessage>(countResponse);
                Assert.InRange(count.Value, 1, 10);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task HyperLogLog_PfCount_MultiKey_ComputesUnion()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("PFADD", "hll:1", "a", "b", "c"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("PFADD", "hll:2", "c", "d", "e"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("PFCOUNT", "hll:1"));
                channel.RunPendingTasks();
                var singleResponse = ReadOutbound(channel);
                var single = Assert.IsType<IntegerRedisMessage>(singleResponse);

                channel.WriteInbound(Command("PFCOUNT", "hll:1", "hll:2"));
                channel.RunPendingTasks();
                var unionResponse = ReadOutbound(channel);
                var union = Assert.IsType<IntegerRedisMessage>(unionResponse);

                Assert.True(union.Value >= single.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task HyperLogLog_PfMerge_CreatesMergedSketch()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("PFADD", "left", "x", "y"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("PFADD", "right", "y", "z"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("PFMERGE", "merged", "left", "right"));
                channel.RunPendingTasks();
                var mergeResponse = ReadOutbound(channel);
                var mergeOk = Assert.IsType<SimpleStringRedisMessage>(mergeResponse);
                Assert.Equal("OK", mergeOk.Content);

                channel.WriteInbound(Command("PFCOUNT", "left"));
                channel.RunPendingTasks();
                var leftResponse = ReadOutbound(channel);
                var leftCount = Assert.IsType<IntegerRedisMessage>(leftResponse);

                channel.WriteInbound(Command("PFCOUNT", "merged"));
                channel.RunPendingTasks();
                var mergedResponse = ReadOutbound(channel);
                var mergedCount = Assert.IsType<IntegerRedisMessage>(mergedResponse);

                Assert.True(mergedCount.Value >= leftCount.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task HyperLogLog_PfAdd_OnPlainString_ReturnsWrongType()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("SET", "not-hll", "value"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("PFADD", "not-hll", "a"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);

                var error = Assert.IsType<ErrorRedisMessage>(response);
                Assert.Equal("WRONGTYPE Key is not a valid HyperLogLog string value.", error.Content);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task Bloom_ReserveAddExistsAndInfo_Workflow()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("BF.RESERVE", "bf:1", "0.01", "1000"));
                channel.RunPendingTasks();
                var reserve = Assert.IsType<SimpleStringRedisMessage>(ReadOutbound(channel));
                Assert.Equal("OK", reserve.Content);

                channel.WriteInbound(Command("BF.ADD", "bf:1", "alpha"));
                channel.RunPendingTasks();
                var added = Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel));
                Assert.Equal(1, added.Value);

                channel.WriteInbound(Command("BF.EXISTS", "bf:1", "alpha"));
                channel.RunPendingTasks();
                var exists = Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel));
                Assert.Equal(1, exists.Value);

                channel.WriteInbound(Command("BF.INFO", "bf:1"));
                channel.RunPendingTasks();
                var info = Assert.IsType<ArrayRedisMessage>(ReadOutbound(channel));
                Assert.True(info.Children.Count >= 10);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task Cuckoo_ReserveAddNxCountDel_Workflow()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("CF.RESERVE", "cf:1", "128"));
                channel.RunPendingTasks();
                var reserve = Assert.IsType<SimpleStringRedisMessage>(ReadOutbound(channel));
                Assert.Equal("OK", reserve.Content);

                channel.WriteInbound(Command("CF.ADDNX", "cf:1", "beta"));
                channel.RunPendingTasks();
                var addNx = Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel));
                Assert.Equal(1, addNx.Value);

                channel.WriteInbound(Command("CF.ADDNX", "cf:1", "beta"));
                channel.RunPendingTasks();
                var addNxAgain = Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel));
                Assert.Equal(0, addNxAgain.Value);

                channel.WriteInbound(Command("CF.COUNT", "cf:1", "beta"));
                channel.RunPendingTasks();
                var count = Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel));
                Assert.Equal(1, count.Value);

                channel.WriteInbound(Command("CF.DEL", "cf:1", "beta"));
                channel.RunPendingTasks();
                var del = Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel));
                Assert.Equal(1, del.Value);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task Cuckoo_Insert_ParsesOptionsAndReturnsArray()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("CF.INSERT", "cf:opt", "CAPACITY", "64", "ITEMS", "a", "b", "a"));
                channel.RunPendingTasks();
                var array = Assert.IsType<ArrayRedisMessage>(ReadOutbound(channel));
                Assert.Equal(3, array.Children.Count);

                channel.WriteInbound(Command("CF.COUNT", "cf:opt", "a"));
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
        public async Task TDigest_CreateAddQuantileMinMax_Workflow()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("TDIGEST.CREATE", "td:1", "COMPRESSION", "100"));
                channel.RunPendingTasks();
                var created = Assert.IsType<SimpleStringRedisMessage>(ReadOutbound(channel));
                Assert.Equal("OK", created.Content);

                channel.WriteInbound(Command("TDIGEST.ADD", "td:1", "1", "2", "3", "4", "5"));
                channel.RunPendingTasks();
                var added = Assert.IsType<SimpleStringRedisMessage>(ReadOutbound(channel));
                Assert.Equal("OK", added.Content);

                channel.WriteInbound(Command("TDIGEST.QUANTILE", "td:1", "0.5"));
                channel.RunPendingTasks();
                var q = Assert.IsType<ArrayRedisMessage>(ReadOutbound(channel));
                Assert.Single(q.Children);

                channel.WriteInbound(Command("TDIGEST.MIN", "td:1"));
                channel.RunPendingTasks();
                var min = Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel));
                Assert.Equal("1", GetBulkString(min));

                channel.WriteInbound(Command("TDIGEST.MAX", "td:1"));
                channel.RunPendingTasks();
                var max = Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel));
                Assert.Equal("5", GetBulkString(max));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task TDigest_ParityCommands_RankByRankTrimmedMean()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("TDIGEST.CREATE", "td:parity"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("TDIGEST.ADD", "td:parity", "1", "2", "3", "4", "5"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("TDIGEST.RANK", "td:parity", "3"));
                channel.RunPendingTasks();
                var rank = Assert.IsType<ArrayRedisMessage>(ReadOutbound(channel));
                Assert.Single(rank.Children);

                channel.WriteInbound(Command("TDIGEST.BYRANK", "td:parity", "2"));
                channel.RunPendingTasks();
                var byRank = Assert.IsType<ArrayRedisMessage>(ReadOutbound(channel));
                Assert.Single(byRank.Children);

                channel.WriteInbound(Command("TDIGEST.TRIMMED_MEAN", "td:parity", "0.2", "0.8"));
                channel.RunPendingTasks();
                var tm = Assert.IsType<FullBulkStringRedisMessage>(ReadOutbound(channel));
                Assert.NotNull(GetBulkString(tm));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task TopK_ReserveAddQueryCountList_Workflow()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("TOPK.RESERVE", "topk:1", "2"));
                channel.RunPendingTasks();
                var reserve = Assert.IsType<SimpleStringRedisMessage>(ReadOutbound(channel));
                Assert.Equal("OK", reserve.Content);

                channel.WriteInbound(Command("TOPK.ADD", "topk:1", "a", "b", "a", "c"));
                channel.RunPendingTasks();
                var add = Assert.IsType<ArrayRedisMessage>(ReadOutbound(channel));
                Assert.Equal(4, add.Children.Count);

                channel.WriteInbound(Command("TOPK.QUERY", "topk:1", "a", "b", "c"));
                channel.RunPendingTasks();
                var query = Assert.IsType<ArrayRedisMessage>(ReadOutbound(channel));
                Assert.Equal(3, query.Children.Count);

                channel.WriteInbound(Command("TOPK.COUNT", "topk:1", "a", "b", "c"));
                channel.RunPendingTasks();
                var count = Assert.IsType<ArrayRedisMessage>(ReadOutbound(channel));
                Assert.Equal(3, count.Children.Count);

                channel.WriteInbound(Command("TOPK.LIST", "topk:1", "WITHCOUNT"));
                channel.RunPendingTasks();
                var list = Assert.IsType<ArrayRedisMessage>(ReadOutbound(channel));
                Assert.True(list.Children.Count >= 2);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task Vector_SetGetDim_RoundTrip()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("VSET", "emb:1", "1", "2.5", "-3"));
                channel.RunPendingTasks();
                var setResponse = ReadOutbound(channel);
                var setOk = Assert.IsType<SimpleStringRedisMessage>(setResponse);
                Assert.Equal("OK", setOk.Content);

                channel.WriteInbound(Command("VDIM", "emb:1"));
                channel.RunPendingTasks();
                var dimResponse = ReadOutbound(channel);
                var dim = Assert.IsType<IntegerRedisMessage>(dimResponse);
                Assert.Equal(3, dim.Value);

                channel.WriteInbound(Command("VGET", "emb:1"));
                channel.RunPendingTasks();
                var getResponse = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(getResponse);
                Assert.Equal(3, array.Children.Count);
                Assert.Equal("1", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(array.Children[0])));
                Assert.Equal("2.5", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(array.Children[1])));
                Assert.Equal("-3", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(array.Children[2])));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task Vector_Similarity_DefaultCosine_ReturnsValue()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("VSET", "a", "1", "0"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("VSET", "b", "1", "0"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("VSIM", "a", "b"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);
                var bulk = Assert.IsType<FullBulkStringRedisMessage>(response);
                Assert.Equal("1", GetBulkString(bulk));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task Vector_Similarity_DotAndL2_ReturnExpectedValues()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("VSET", "a", "1", "2"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("VSET", "b", "3", "4"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("VSIM", "a", "b", "DOT"));
                channel.RunPendingTasks();
                var dotResponse = ReadOutbound(channel);
                var dot = Assert.IsType<FullBulkStringRedisMessage>(dotResponse);
                Assert.Equal("11", GetBulkString(dot));

                channel.WriteInbound(Command("VSIM", "a", "b", "L2"));
                channel.RunPendingTasks();
                var l2Response = ReadOutbound(channel);
                var l2 = Assert.IsType<FullBulkStringRedisMessage>(l2Response);
                Assert.Equal(Math.Sqrt(8).ToString("G17", CultureInfo.InvariantCulture), GetBulkString(l2));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task Vector_WrongTypeAndInvalidOp_ReturnErrors()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("SET", "plain", "value"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("VSET", "plain", "1", "2"));
                channel.RunPendingTasks();
                var wrongTypeResponse = ReadOutbound(channel);
                var wrongType = Assert.IsType<ErrorRedisMessage>(wrongTypeResponse);
                Assert.Equal("WRONGTYPE Operation against a key holding the wrong kind of value", wrongType.Content);

                channel.WriteInbound(Command("VSET", "x", "1", "2"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("VSET", "y", "1"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("VSIM", "x", "y", "COSINE"));
                channel.RunPendingTasks();
                var invalidResponse = ReadOutbound(channel);
                var invalid = Assert.IsType<ErrorRedisMessage>(invalidResponse);
                Assert.Equal("ERR invalid vector operation", invalid.Content);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task Vector_Delete_ReturnsCountAndRemovesVector()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("VSET", "vec:1", "1", "2"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("VDEL", "vec:1"));
                channel.RunPendingTasks();
                var delResponse = ReadOutbound(channel);
                var deleted = Assert.IsType<IntegerRedisMessage>(delResponse);
                Assert.Equal(1, deleted.Value);

                channel.WriteInbound(Command("VGET", "vec:1"));
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
        public async Task Vector_Search_ReturnsTopKByMetric()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("VSET", "emb:a", "1", "0"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("VSET", "emb:b", "0.9", "0.1"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("VSET", "emb:c", "-1", "0"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("VSEARCH", "emb:", "2", "COSINE", "1", "0"));
                channel.RunPendingTasks();
                var cosineResponse = ReadOutbound(channel);
                var cosine = Assert.IsType<ArrayRedisMessage>(cosineResponse);
                Assert.Equal(4, cosine.Children.Count);
                Assert.Equal("emb:a", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(cosine.Children[0])));
                Assert.Equal("emb:b", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(cosine.Children[2])));

                channel.WriteInbound(Command("VSEARCH", "emb:", "1", "L2", "1", "0"));
                channel.RunPendingTasks();
                var l2Response = ReadOutbound(channel);
                var l2 = Assert.IsType<ArrayRedisMessage>(l2Response);
                Assert.Equal(2, l2.Children.Count);
                Assert.Equal("emb:a", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(l2.Children[0])));
                Assert.Equal("0", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(l2.Children[1])));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task Vector_Search_InvalidMetric_ReturnsError()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("VSET", "emb:a", "1", "0"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("VSEARCH", "emb:", "5", "BAD", "1", "0"));
                channel.RunPendingTasks();
                var response = ReadOutbound(channel);
                var error = Assert.IsType<ErrorRedisMessage>(response);
                Assert.Equal("ERR invalid vector operation", error.Content);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task Vector_Search_WithOffset_PaginatesResults()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("VSET", "emb:a", "1", "0"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("VSET", "emb:b", "0.8", "0.2"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("VSET", "emb:c", "0.6", "0.4"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("VSEARCH", "emb:", "2", "COSINE", "OFFSET", "1", "1", "0"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                Assert.Equal(4, array.Children.Count);
                Assert.Equal("emb:b", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(array.Children[0])));
                Assert.Equal("emb:c", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(array.Children[2])));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task Vector_Search_InvalidOffset_ReturnsError()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("VSET", "emb:a", "1", "0"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("VSEARCH", "emb:", "2", "COSINE", "OFFSET", "-1", "1", "0"));
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
        public async Task Vector_Search_LimitSyntax_WorksWithoutPositionalTopK()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("VSET", "emb:a", "1", "0"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("VSET", "emb:b", "0.8", "0.2"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("VSEARCH", "emb:", "COSINE", "LIMIT", "1", "1", "0"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var array = Assert.IsType<ArrayRedisMessage>(response);
                Assert.Equal(2, array.Children.Count);
                Assert.Equal("emb:a", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(array.Children[0])));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task Vector_Search_LimitMissing_ReturnsError()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("VSEARCH", "emb:", "COSINE", "1", "0"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var error = Assert.IsType<ErrorRedisMessage>(response);
                Assert.Equal("ERR LIMIT is required", error.Content);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task Vector_Search_OffsetBeforeLimit_ReturnsSyntaxError()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("VSET", "emb:a", "1", "0"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("VSEARCH", "emb:", "COSINE", "OFFSET", "1", "LIMIT", "1", "1", "0"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var error = Assert.IsType<ErrorRedisMessage>(response);
                Assert.Equal("ERR LIMIT is required", error.Content);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task Vector_Search_LimitInPositionalForm_ReturnsSyntaxError()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("VSET", "emb:a", "1", "0"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("VSEARCH", "emb:", "2", "COSINE", "LIMIT", "1", "1", "0"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var error = Assert.IsType<ErrorRedisMessage>(response);
                Assert.Equal("ERR syntax error", error.Content);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task Vector_Search_DuplicateOffset_ReturnsSyntaxError()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("VSET", "emb:a", "1", "0"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("VSEARCH", "emb:", "2", "COSINE", "OFFSET", "0", "OFFSET", "1", "1", "0"));
                channel.RunPendingTasks();

                var response = ReadOutbound(channel);
                var error = Assert.IsType<ErrorRedisMessage>(response);
                Assert.Equal("ERR syntax error", error.Content);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task TimeSeries_CreateAddGetRangeInfo_Workflow()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("TS.CREATE", "ts:1", "RETENTION", "5000"));
                channel.RunPendingTasks();
                var created = Assert.IsType<SimpleStringRedisMessage>(ReadOutbound(channel));
                Assert.Equal("OK", created.Content);

                channel.WriteInbound(Command("TS.ADD", "ts:1", "1000", "1.5"));
                channel.RunPendingTasks();
                var firstAdd = Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel));
                Assert.Equal(1000, firstAdd.Value);

                channel.WriteInbound(Command("TS.ADD", "ts:1", "2000", "2.5"));
                channel.RunPendingTasks();
                var secondAdd = Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel));
                Assert.Equal(2000, secondAdd.Value);

                channel.WriteInbound(Command("TS.GET", "ts:1"));
                channel.RunPendingTasks();
                var get = Assert.IsType<ArrayRedisMessage>(ReadOutbound(channel));
                Assert.Equal(2, get.Children.Count);
                Assert.Equal(2000, Assert.IsType<IntegerRedisMessage>(get.Children[0]).Value);
                Assert.Equal("2.5", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(get.Children[1])));

                channel.WriteInbound(Command("TS.RANGE", "ts:1", "-", "+"));
                channel.RunPendingTasks();
                var range = Assert.IsType<ArrayRedisMessage>(ReadOutbound(channel));
                Assert.Equal(2, range.Children.Count);

                var firstSample = Assert.IsType<ArrayRedisMessage>(range.Children[0]);
                Assert.Equal(1000, Assert.IsType<IntegerRedisMessage>(firstSample.Children[0]).Value);
                Assert.Equal("1.5", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(firstSample.Children[1])));

                channel.WriteInbound(Command("TS.INFO", "ts:1"));
                channel.RunPendingTasks();
                var info = Assert.IsType<ArrayRedisMessage>(ReadOutbound(channel));
                var infoFields = GetHashPairs(info.Children);
                Assert.Equal("2", infoFields["totalSamples"]);
                Assert.Equal("5000", infoFields["retentionTime"]);
                Assert.Equal("1000", infoFields["firstTimestamp"]);
                Assert.Equal("2000", infoFields["lastTimestamp"]);
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task TimeSeries_IncrByDecrBy_UpdatesSamples()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("TS.INCRBY", "ts:counter", "2"));
                channel.RunPendingTasks();
                var autoTimestamp = Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel));
                Assert.True(autoTimestamp.Value > 0);

                channel.WriteInbound(Command("TS.INCRBY", "ts:counter", "3", "TIMESTAMP", "5000"));
                channel.RunPendingTasks();
                var incr = Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel));
                Assert.Equal(5000, incr.Value);

                channel.WriteInbound(Command("TS.DECRBY", "ts:counter", "1", "TIMESTAMP", "5000"));
                channel.RunPendingTasks();
                var decr = Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel));
                Assert.Equal(5000, decr.Value);

                channel.WriteInbound(Command("TS.RANGE", "ts:counter", "5000", "5000"));
                channel.RunPendingTasks();
                var range = Assert.IsType<ArrayRedisMessage>(ReadOutbound(channel));
                Assert.Single(range.Children);

                var sample = Assert.IsType<ArrayRedisMessage>(range.Children[0]);
                Assert.Equal(5000, Assert.IsType<IntegerRedisMessage>(sample.Children[0]).Value);
                Assert.Equal("2", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(sample.Children[1])));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task TimeSeries_DelAndRevRange_ReturnExpectedResults()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("TS.ADD", "ts:2", "1000", "1"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("TS.ADD", "ts:2", "1500", "2"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("TS.ADD", "ts:2", "2000", "3"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("TS.ADD", "ts:2", "2200", "5"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("TS.DEL", "ts:2", "1200", "1800"));
                channel.RunPendingTasks();
                var deleted = Assert.IsType<IntegerRedisMessage>(ReadOutbound(channel));
                Assert.Equal(1, deleted.Value);

                channel.WriteInbound(Command("TS.REVRANGE", "ts:2", "-", "+", "AGGREGATION", "AVG", "1000", "COUNT", "2"));
                channel.RunPendingTasks();
                var rev = Assert.IsType<ArrayRedisMessage>(ReadOutbound(channel));
                Assert.Equal(2, rev.Children.Count);

                var bucket2 = Assert.IsType<ArrayRedisMessage>(rev.Children[0]);
                Assert.Equal(2000, Assert.IsType<IntegerRedisMessage>(bucket2.Children[0]).Value);
                Assert.Equal("4", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(bucket2.Children[1])));

                var bucket1 = Assert.IsType<ArrayRedisMessage>(rev.Children[1]);
                Assert.Equal(1000, Assert.IsType<IntegerRedisMessage>(bucket1.Children[0]).Value);
                Assert.Equal("1", GetBulkString(Assert.IsType<FullBulkStringRedisMessage>(bucket1.Children[1])));
            }
            finally
            {
                await channel.CloseAsync();
            }
        }

        [Fact]
        public async Task TimeSeries_WrongTypeAndMissingInfo_ReturnErrors()
        {
            var store = new InMemoryKeyValueStore();
            var channel = new EmbeddedChannel(new DredisCommandHandler(store));

            try
            {
                channel.WriteInbound(Command("SET", "plain", "value"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("TS.ADD", "plain", "1000", "1"));
                channel.RunPendingTasks();
                var wrongType = Assert.IsType<ErrorRedisMessage>(ReadOutbound(channel));
                Assert.Equal("WRONGTYPE Operation against a key holding the wrong kind of value", wrongType.Content);

                channel.WriteInbound(Command("TS.INFO", "missing:ts"));
                channel.RunPendingTasks();
                var missing = Assert.IsType<ErrorRedisMessage>(ReadOutbound(channel));
                Assert.Equal("ERR key does not exist", missing.Content);

                channel.WriteInbound(Command("TS.CREATE", "dup:ts"));
                channel.RunPendingTasks();
                _ = ReadOutbound(channel);

                channel.WriteInbound(Command("TS.CREATE", "dup:ts"));
                channel.RunPendingTasks();
                var duplicate = Assert.IsType<ErrorRedisMessage>(ReadOutbound(channel));
                Assert.Equal("ERR TSDB: key already exists", duplicate.Content);

                channel.WriteInbound(Command("TS.RANGE", "dup:ts", "-", "+", "AGGREGATION", "BAD", "1000"));
                channel.RunPendingTasks();
                var badAggregation = Assert.IsType<ErrorRedisMessage>(ReadOutbound(channel));
                Assert.Equal("ERR invalid arguments", badAggregation.Content);
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
                Assert.Single(execArray.Children);
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
        private readonly Dictionary<string, double[]> _vectors = new(StringComparer.Ordinal);
        private readonly Dictionary<string, List<StreamEntryModel>> _streams = new(StringComparer.Ordinal);
        private readonly Dictionary<string, StreamId> _streamLastIds = new(StringComparer.Ordinal);
        private readonly Dictionary<string, Dictionary<string, StreamGroupState>> _streamGroups = new(StringComparer.Ordinal);
        private readonly Dictionary<string, BloomSketchModel> _bloom = new(StringComparer.Ordinal);
        private readonly Dictionary<string, CuckooSketchModel> _cuckoo = new(StringComparer.Ordinal);
        private readonly Dictionary<string, TDigestSketchModel> _tdigest = new(StringComparer.Ordinal);
        private readonly Dictionary<string, TopKSketchModel> _topk = new(StringComparer.Ordinal);
        private readonly Dictionary<string, SortedDictionary<long, double>> _timeSeries = new(StringComparer.Ordinal);
        private readonly Dictionary<string, long> _timeSeriesRetention = new(StringComparer.Ordinal);
        private readonly Dictionary<string, DateTimeOffset?> _expirations = new(StringComparer.Ordinal);
        private static readonly Encoding Utf8 = new UTF8Encoding(false);
        private static readonly byte[] HyperLogLogMagic = Encoding.ASCII.GetBytes("DHLL");
        private const int HyperLogLogPrecision = 14;
        private const int HyperLogLogRegistersCount = 1 << HyperLogLogPrecision;

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

        private sealed class BloomSketchModel
        {
            public BloomSketchModel(double errorRate, long capacity)
            {
                ErrorRate = errorRate;
                Capacity = capacity;
                Members = new HashSet<string>(StringComparer.Ordinal);
            }

            public double ErrorRate { get; }
            public long Capacity { get; }
            public HashSet<string> Members { get; }
        }

        private sealed class CuckooSketchModel
        {
            public CuckooSketchModel(long capacity)
            {
                Capacity = capacity;
                Members = new Dictionary<string, long>(StringComparer.Ordinal);
            }

            public long Capacity { get; }
            public Dictionary<string, long> Members { get; }
        }

        private sealed class TDigestSketchModel
        {
            public TDigestSketchModel(int compression)
            {
                Compression = compression;
                Values = new List<double>();
            }

            public int Compression { get; }
            public List<double> Values { get; }
        }

        private sealed class TopKSketchModel
        {
            public TopKSketchModel(int k, int width, int depth, double decay)
            {
                K = k;
                Width = width;
                Depth = depth;
                Decay = decay;
                Counts = new Dictionary<string, long>(StringComparer.Ordinal);
            }

            public int K { get; }
            public int Width { get; }
            public int Depth { get; }
            public double Decay { get; }
            public Dictionary<string, long> Counts { get; }
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
            _vectors.Remove(key);
            _streams.Remove(key);
            _streamLastIds.Remove(key);
            _streamGroups.Remove(key);
            _bloom.Remove(key);
            _cuckoo.Remove(key);
            _tdigest.Remove(key);
            _topk.Remove(key);
            _timeSeries.Remove(key);
            _timeSeriesRetention.Remove(key);
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
            _vectors.Remove(key);
            _streams.Remove(key);
            _streamLastIds.Remove(key);
            _streamGroups.Remove(key);
            _bloom.Remove(key);
            _cuckoo.Remove(key);
            _tdigest.Remove(key);
            _topk.Remove(key);
            _timeSeries.Remove(key);
            _timeSeriesRetention.Remove(key);

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
                _vectors.Remove(item.Key);
                _streams.Remove(item.Key);
                _streamLastIds.Remove(item.Key);
                _streamGroups.Remove(item.Key);
                _bloom.Remove(item.Key);
                _cuckoo.Remove(item.Key);
                _tdigest.Remove(item.Key);
                _topk.Remove(item.Key);
                _timeSeries.Remove(item.Key);
                _timeSeriesRetention.Remove(item.Key);
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
        /// Sets a JSON value at the specified key and path.
        /// </summary>
        public Task<JsonSetResult> JsonSetAsync(
            string key,
            string path,
            byte[] value,
            CancellationToken token = default)
        {
            try
            {
                // Get or create the JSON document
                byte[] docBytes;
                bool isNew = false;

                if (!_data.TryGetValue(key, out var existing))
                {
                    if (path != "$")
                    {
                        return Task.FromResult(new JsonSetResult(JsonResultStatus.PathNotFound));
                    }
                    // Create new root document
                    docBytes = value;
                    isNew = true;
                }
                else
                {
                    docBytes = existing;
                }

                var wrapper = JsonWrapper.TryParse(docBytes);
                if (wrapper == null)
                {
                    return Task.FromResult(new JsonSetResult(JsonResultStatus.InvalidJson));
                }

                // For now, only support setting the root
                if (path != "$")
                {
                    return Task.FromResult(new JsonSetResult(JsonResultStatus.InvalidPath));
                }

                var newWrapper = JsonWrapper.TryParse(value);
                if (newWrapper == null)
                {
                    return Task.FromResult(new JsonSetResult(JsonResultStatus.InvalidJson));
                }

                _data[key] = value;
                return Task.FromResult(new JsonSetResult(JsonResultStatus.Ok, isNew));
            }
            catch
            {
                return Task.FromResult(new JsonSetResult(JsonResultStatus.InvalidJson));
            }
        }

        /// <summary>
        /// Gets a JSON value at the specified path(s).
        /// </summary>
        public Task<JsonGetResult> JsonGetAsync(
            string key,
            string[] paths,
            CancellationToken token = default)
        {
            try
            {
                if (!_data.TryGetValue(key, out var docBytes))
                {
                    return Task.FromResult(new JsonGetResult(JsonResultStatus.PathNotFound));
                }

                var wrapper = JsonWrapper.TryParse(docBytes);
                if (wrapper == null)
                {
                    return Task.FromResult(new JsonGetResult(JsonResultStatus.InvalidJson));
                }

                if (paths.Length == 1)
                {
                    var elements = wrapper.GetAtPaths(paths[0]);
                    if (elements.Count == 0)
                    {
                        return Task.FromResult(new JsonGetResult(JsonResultStatus.PathNotFound));
                    }

                    var value = System.Text.Encoding.UTF8.GetBytes(elements[0].GetRawText());
                    return Task.FromResult(new JsonGetResult(JsonResultStatus.Ok, value: value));
                }
                else
                {
                    var values = new List<byte[]>();
                    foreach (var path in paths)
                    {
                        var elements = wrapper.GetAtPaths(path);
                        if (elements.Count > 0)
                        {
                            values.Add(System.Text.Encoding.UTF8.GetBytes(elements[0].GetRawText()));
                        }
                    }

                    return Task.FromResult(new JsonGetResult(JsonResultStatus.Ok, values: values.ToArray()));
                }
            }
            catch
            {
                return Task.FromResult(new JsonGetResult(JsonResultStatus.InvalidJson));
            }
        }

        /// <summary>
        /// Deletes values at the specified path(s).
        /// </summary>
        public Task<JsonDelResult> JsonDelAsync(
            string key,
            string[] paths,
            CancellationToken token = default)
        {
            try
            {
                if (!_data.TryGetValue(key, out var docBytes))
                {
                    return Task.FromResult(new JsonDelResult(JsonResultStatus.PathNotFound, 0));
                }

                var wrapper = JsonWrapper.TryParse(docBytes);
                if (wrapper == null)
                {
                    return Task.FromResult(new JsonDelResult(JsonResultStatus.InvalidJson, 0));
                }

                // For now, only support deleting the root
                if (paths.Length == 1 && paths[0] == "$")
                {
                    _data.Remove(key);
                    return Task.FromResult(new JsonDelResult(JsonResultStatus.Ok, 1));
                }

                return Task.FromResult(new JsonDelResult(JsonResultStatus.InvalidPath, 0));
            }
            catch
            {
                return Task.FromResult(new JsonDelResult(JsonResultStatus.InvalidJson, 0));
            }
        }

        /// <summary>
        /// Gets the type of value at the specified path(s).
        /// </summary>
        public Task<JsonTypeResult> JsonTypeAsync(
            string key,
            string[] paths,
            CancellationToken token = default)
        {
            try
            {
                if (!_data.TryGetValue(key, out var docBytes))
                {
                    return Task.FromResult(new JsonTypeResult(JsonResultStatus.PathNotFound));
                }

                var wrapper = JsonWrapper.TryParse(docBytes);
                if (wrapper == null)
                {
                    return Task.FromResult(new JsonTypeResult(JsonResultStatus.InvalidJson));
                }

                var types = new List<string>();
                foreach (var path in paths)
                {
                    var elements = wrapper.GetAtPaths(path);
                    if (elements.Count == 0)
                    {
                        continue;
                    }

                    var type = elements[0].ValueKind switch
                    {
                        System.Text.Json.JsonValueKind.Null => "null",
                        System.Text.Json.JsonValueKind.True => "boolean",
                        System.Text.Json.JsonValueKind.False => "boolean",
                        System.Text.Json.JsonValueKind.Number => "number",
                        System.Text.Json.JsonValueKind.String => "string",
                        System.Text.Json.JsonValueKind.Array => "array",
                        System.Text.Json.JsonValueKind.Object => "object",
                        _ => "unknown"
                    };
                    types.Add(type);
                }

                return Task.FromResult(new JsonTypeResult(JsonResultStatus.Ok, types.ToArray()));
            }
            catch
            {
                return Task.FromResult(new JsonTypeResult(JsonResultStatus.InvalidJson));
            }
        }

        /// <summary>
        /// Gets string length at the specified path(s).
        /// </summary>
        public Task<JsonArrayResult> JsonStrlenAsync(
            string key,
            string[] paths,
            CancellationToken token = default)
        {
            try
            {
                if (!_data.TryGetValue(key, out var docBytes))
                {
                    return Task.FromResult(new JsonArrayResult(JsonResultStatus.PathNotFound));
                }

                var wrapper = JsonWrapper.TryParse(docBytes);
                if (wrapper == null)
                {
                    return Task.FromResult(new JsonArrayResult(JsonResultStatus.InvalidJson));
                }

                if (paths.Length == 1)
                {
                    var elements = wrapper.GetAtPaths(paths[0]);
                    if (elements.Count == 0 || elements[0].ValueKind != System.Text.Json.JsonValueKind.String)
                    {
                        return Task.FromResult(new JsonArrayResult(JsonResultStatus.WrongType));
                    }

                    var len = elements[0].GetString()?.Length ?? 0;
                    return Task.FromResult(new JsonArrayResult(JsonResultStatus.Ok, count: len));
                }
                else
                {
                    var lengths = new List<long>();
                    foreach (var path in paths)
                    {
                        var elements = wrapper.GetAtPaths(path);
                        if (elements.Count == 0)
                        {
                            lengths.Add(0);
                        }
                        else if (elements[0].ValueKind != System.Text.Json.JsonValueKind.String)
                        {
                            return Task.FromResult(new JsonArrayResult(JsonResultStatus.WrongType));
                        }
                        else
                        {
                            lengths.Add(elements[0].GetString()?.Length ?? 0);
                        }
                    }

                    return Task.FromResult(new JsonArrayResult(JsonResultStatus.Ok, counts: lengths.ToArray()));
                }
            }
            catch
            {
                return Task.FromResult(new JsonArrayResult(JsonResultStatus.InvalidJson));
            }
        }

        /// <summary>
        /// Gets array length at the specified path(s).
        /// </summary>
        public Task<JsonArrayResult> JsonArrlenAsync(
            string key,
            string[] paths,
            CancellationToken token = default)
        {
            try
            {
                if (!_data.TryGetValue(key, out var docBytes))
                {
                    return Task.FromResult(new JsonArrayResult(JsonResultStatus.PathNotFound));
                }

                var wrapper = JsonWrapper.TryParse(docBytes);
                if (wrapper == null)
                {
                    return Task.FromResult(new JsonArrayResult(JsonResultStatus.InvalidJson));
                }

                if (paths.Length == 1)
                {
                    var elements = wrapper.GetAtPaths(paths[0]);
                    if (elements.Count == 0 || elements[0].ValueKind != System.Text.Json.JsonValueKind.Array)
                    {
                        return Task.FromResult(new JsonArrayResult(JsonResultStatus.WrongType));
                    }

                    var len = elements[0].GetArrayLength();
                    return Task.FromResult(new JsonArrayResult(JsonResultStatus.Ok, count: len));
                }
                else
                {
                    var lengths = new List<long>();
                    foreach (var path in paths)
                    {
                        var elements = wrapper.GetAtPaths(path);
                        if (elements.Count == 0)
                        {
                            lengths.Add(0);
                        }
                        else if (elements[0].ValueKind != System.Text.Json.JsonValueKind.Array)
                        {
                            return Task.FromResult(new JsonArrayResult(JsonResultStatus.WrongType));
                        }
                        else
                        {
                            lengths.Add(elements[0].GetArrayLength());
                        }
                    }

                    return Task.FromResult(new JsonArrayResult(JsonResultStatus.Ok, counts: lengths.ToArray()));
                }
            }
            catch
            {
                return Task.FromResult(new JsonArrayResult(JsonResultStatus.InvalidJson));
            }
        }

        /// <summary>
        /// Appends value(s) to arrays at the specified path(s).
        /// </summary>
        public Task<JsonArrayResult> JsonArrappendAsync(
            string key,
            string path,
            byte[][] values,
            CancellationToken token = default)
        {
            return Task.FromResult(new JsonArrayResult(JsonResultStatus.InvalidPath));
        }

        /// <summary>
        /// Gets values from an array at the specified path(s).
        /// </summary>
        public Task<JsonGetResult> JsonArrindexAsync(
            string key,
            string path,
            byte[] value,
            CancellationToken token = default)
        {
            return Task.FromResult(new JsonGetResult(JsonResultStatus.InvalidPath));
        }

        /// <summary>
        /// Inserts value(s) at an index in arrays.
        /// </summary>
        public Task<JsonArrayResult> JsonArrinsertAsync(
            string key,
            string path,
            int index,
            byte[][] values,
            CancellationToken token = default)
        {
            return Task.FromResult(new JsonArrayResult(JsonResultStatus.InvalidPath));
        }

        /// <summary>
        /// Removes elements from arrays.
        /// </summary>
        public Task<JsonArrayResult> JsonArrremAsync(
            string key,
            string path,
            int? index,
            CancellationToken token = default)
        {
            return Task.FromResult(new JsonArrayResult(JsonResultStatus.InvalidPath));
        }

        /// <summary>
        /// Trims arrays to a range.
        /// </summary>
        public Task<JsonArrayResult> JsonArrtrimAsync(
            string key,
            string path,
            int start,
            int stop,
            CancellationToken token = default)
        {
            return Task.FromResult(new JsonArrayResult(JsonResultStatus.InvalidPath));
        }

        /// <summary>
        /// Gets multiple JSON documents by key.
        /// </summary>
        public Task<JsonMGetResult> JsonMgetAsync(
            string[] keys,
            string path,
            CancellationToken token = default)
        {
            try
            {
                var values = new List<byte[]>();

                foreach (var key in keys)
                {
                    if (_data.TryGetValue(key, out var docBytes))
                    {
                        var wrapper = JsonWrapper.TryParse(docBytes);
                        if (wrapper != null)
                        {
                            var elements = wrapper.GetAtPaths(path);
                            if (elements.Count > 0)
                            {
                                values.Add(System.Text.Encoding.UTF8.GetBytes(elements[0].GetRawText()));
                            }
                        }
                    }
                }

                return Task.FromResult(new JsonMGetResult(JsonResultStatus.Ok, values.ToArray()));
            }
            catch
            {
                return Task.FromResult(new JsonMGetResult(JsonResultStatus.InvalidJson));
            }
        }

        public Task<HyperLogLogAddResult> HyperLogLogAddAsync(
            string key,
            byte[][] elements,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sets.ContainsKey(key) || _sortedSets.ContainsKey(key) || _vectors.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(new HyperLogLogAddResult(HyperLogLogResultStatus.WrongType, false));
            }

            byte[] registers;
            if (_data.TryGetValue(key, out var existing))
            {
                if (!TryDecodeHyperLogLog(existing, out registers))
                {
                    return Task.FromResult(new HyperLogLogAddResult(HyperLogLogResultStatus.WrongType, false));
                }
            }
            else
            {
                registers = new byte[HyperLogLogRegistersCount];
            }

            var changed = false;
            foreach (var element in elements)
            {
                var hash = ComputeHyperLogLogHash(element);
                var index = (int)(hash >> (64 - HyperLogLogPrecision));
                var rank = ComputeHyperLogLogRank(hash);
                if (rank > registers[index])
                {
                    registers[index] = (byte)rank;
                    changed = true;
                }
            }

            _data[key] = EncodeHyperLogLog(registers);
            return Task.FromResult(new HyperLogLogAddResult(HyperLogLogResultStatus.Ok, changed));
        }

        public Task<HyperLogLogCountResult> HyperLogLogCountAsync(
            string[] keys,
            CancellationToken token = default)
        {
            var unionRegisters = new byte[HyperLogLogRegistersCount];

            foreach (var key in keys)
            {
                if (IsExpired(key))
                {
                    RemoveKey(key);
                    continue;
                }

                if (_hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sets.ContainsKey(key) || _sortedSets.ContainsKey(key) || _vectors.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
                {
                    return Task.FromResult(new HyperLogLogCountResult(HyperLogLogResultStatus.WrongType, 0));
                }

                if (!_data.TryGetValue(key, out var existing))
                {
                    continue;
                }

                if (!TryDecodeHyperLogLog(existing, out var registers))
                {
                    return Task.FromResult(new HyperLogLogCountResult(HyperLogLogResultStatus.WrongType, 0));
                }

                for (int i = 0; i < unionRegisters.Length; i++)
                {
                    if (registers[i] > unionRegisters[i])
                    {
                        unionRegisters[i] = registers[i];
                    }
                }
            }

            var estimate = EstimateHyperLogLogCount(unionRegisters);
            return Task.FromResult(new HyperLogLogCountResult(HyperLogLogResultStatus.Ok, estimate));
        }

        public Task<HyperLogLogMergeResult> HyperLogLogMergeAsync(
            string destinationKey,
            string[] sourceKeys,
            CancellationToken token = default)
        {
            if (IsExpired(destinationKey))
            {
                RemoveKey(destinationKey);
            }

            if (_hashes.ContainsKey(destinationKey) || _lists.ContainsKey(destinationKey) || _sets.ContainsKey(destinationKey) || _sortedSets.ContainsKey(destinationKey) || _vectors.ContainsKey(destinationKey) || _streams.ContainsKey(destinationKey) || _streamGroups.ContainsKey(destinationKey))
            {
                return Task.FromResult(new HyperLogLogMergeResult(HyperLogLogResultStatus.WrongType));
            }

            var mergedRegisters = new byte[HyperLogLogRegistersCount];
            foreach (var key in sourceKeys)
            {
                if (IsExpired(key))
                {
                    RemoveKey(key);
                    continue;
                }

                if (_hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sets.ContainsKey(key) || _sortedSets.ContainsKey(key) || _vectors.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
                {
                    return Task.FromResult(new HyperLogLogMergeResult(HyperLogLogResultStatus.WrongType));
                }

                if (!_data.TryGetValue(key, out var existing))
                {
                    continue;
                }

                if (!TryDecodeHyperLogLog(existing, out var registers))
                {
                    return Task.FromResult(new HyperLogLogMergeResult(HyperLogLogResultStatus.WrongType));
                }

                for (int i = 0; i < mergedRegisters.Length; i++)
                {
                    if (registers[i] > mergedRegisters[i])
                    {
                        mergedRegisters[i] = registers[i];
                    }
                }
            }

            _data[destinationKey] = EncodeHyperLogLog(mergedRegisters);
            return Task.FromResult(new HyperLogLogMergeResult(HyperLogLogResultStatus.Ok));
        }

        public Task<ProbabilisticResultStatus> BloomReserveAsync(string key, double errorRate, long capacity, CancellationToken token = default)
        {
            if (errorRate <= 0 || errorRate >= 1 || capacity <= 0)
            {
                return Task.FromResult(ProbabilisticResultStatus.InvalidArgument);
            }

            if (_bloom.ContainsKey(key))
            {
                return Task.FromResult(ProbabilisticResultStatus.Exists);
            }

            if (HasNonBloomType(key))
            {
                return Task.FromResult(ProbabilisticResultStatus.WrongType);
            }

            _bloom[key] = new BloomSketchModel(errorRate, capacity);
            _data.Remove(key);
            return Task.FromResult(ProbabilisticResultStatus.Ok);
        }

        public Task<ProbabilisticBoolResult> BloomAddAsync(string key, byte[] element, CancellationToken token = default)
        {
            if (HasNonBloomType(key))
            {
                return Task.FromResult(new ProbabilisticBoolResult(ProbabilisticResultStatus.WrongType, false));
            }

            if (!_bloom.TryGetValue(key, out var bloom))
            {
                bloom = new BloomSketchModel(0.01, 100);
                _bloom[key] = bloom;
                _data.Remove(key);
            }

            var added = bloom.Members.Add(Convert.ToBase64String(element));
            return Task.FromResult(new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, added));
        }

        public Task<ProbabilisticArrayResult> BloomMAddAsync(string key, byte[][] elements, CancellationToken token = default)
        {
            if (HasNonBloomType(key))
            {
                return Task.FromResult(new ProbabilisticArrayResult(ProbabilisticResultStatus.WrongType, Array.Empty<long>()));
            }

            if (!_bloom.TryGetValue(key, out var bloom))
            {
                bloom = new BloomSketchModel(0.01, 100);
                _bloom[key] = bloom;
                _data.Remove(key);
            }

            var result = new long[elements.Length];
            for (int i = 0; i < elements.Length; i++)
            {
                result[i] = bloom.Members.Add(Convert.ToBase64String(elements[i])) ? 1 : 0;
            }

            return Task.FromResult(new ProbabilisticArrayResult(ProbabilisticResultStatus.Ok, result));
        }

        public Task<ProbabilisticBoolResult> BloomExistsAsync(string key, byte[] element, CancellationToken token = default)
        {
            if (HasNonBloomType(key))
            {
                return Task.FromResult(new ProbabilisticBoolResult(ProbabilisticResultStatus.WrongType, false));
            }

            if (!_bloom.TryGetValue(key, out var bloom))
            {
                return Task.FromResult(new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, false));
            }

            var exists = bloom.Members.Contains(Convert.ToBase64String(element));
            return Task.FromResult(new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, exists));
        }

        public Task<ProbabilisticArrayResult> BloomMExistsAsync(string key, byte[][] elements, CancellationToken token = default)
        {
            if (HasNonBloomType(key))
            {
                return Task.FromResult(new ProbabilisticArrayResult(ProbabilisticResultStatus.WrongType, Array.Empty<long>()));
            }

            if (!_bloom.TryGetValue(key, out var bloom))
            {
                return Task.FromResult(new ProbabilisticArrayResult(ProbabilisticResultStatus.Ok, new long[elements.Length]));
            }

            var values = new long[elements.Length];
            for (int i = 0; i < elements.Length; i++)
            {
                values[i] = bloom.Members.Contains(Convert.ToBase64String(elements[i])) ? 1 : 0;
            }

            return Task.FromResult(new ProbabilisticArrayResult(ProbabilisticResultStatus.Ok, values));
        }

        public Task<ProbabilisticInfoResult> BloomInfoAsync(string key, CancellationToken token = default)
        {
            if (HasNonBloomType(key))
            {
                return Task.FromResult(new ProbabilisticInfoResult(ProbabilisticResultStatus.WrongType, Array.Empty<KeyValuePair<string, string>>()));
            }

            if (!_bloom.TryGetValue(key, out var bloom))
            {
                return Task.FromResult(new ProbabilisticInfoResult(ProbabilisticResultStatus.NotFound, Array.Empty<KeyValuePair<string, string>>()));
            }

            var fields = new[]
            {
                new KeyValuePair<string, string>("Capacity", bloom.Capacity.ToString(CultureInfo.InvariantCulture)),
                new KeyValuePair<string, string>("Size", bloom.Members.Count.ToString(CultureInfo.InvariantCulture)),
                new KeyValuePair<string, string>("Number of filters", "1"),
                new KeyValuePair<string, string>("Number of items inserted", bloom.Members.Count.ToString(CultureInfo.InvariantCulture)),
                new KeyValuePair<string, string>("Expansion rate", "2")
            };

            return Task.FromResult(new ProbabilisticInfoResult(ProbabilisticResultStatus.Ok, fields));
        }

        public Task<ProbabilisticResultStatus> CuckooReserveAsync(string key, long capacity, CancellationToken token = default)
        {
            if (capacity <= 0)
            {
                return Task.FromResult(ProbabilisticResultStatus.InvalidArgument);
            }

            if (_cuckoo.ContainsKey(key))
            {
                return Task.FromResult(ProbabilisticResultStatus.Exists);
            }

            if (HasNonCuckooType(key))
            {
                return Task.FromResult(ProbabilisticResultStatus.WrongType);
            }

            _cuckoo[key] = new CuckooSketchModel(capacity);
            _data.Remove(key);
            return Task.FromResult(ProbabilisticResultStatus.Ok);
        }

        public Task<ProbabilisticBoolResult> CuckooAddAsync(string key, byte[] item, bool noCreate, CancellationToken token = default)
        {
            if (HasNonCuckooType(key))
            {
                return Task.FromResult(new ProbabilisticBoolResult(ProbabilisticResultStatus.WrongType, false));
            }

            if (!_cuckoo.TryGetValue(key, out var cuckoo))
            {
                if (noCreate)
                {
                    return Task.FromResult(new ProbabilisticBoolResult(ProbabilisticResultStatus.NotFound, false));
                }

                cuckoo = new CuckooSketchModel(100);
                _cuckoo[key] = cuckoo;
                _data.Remove(key);
            }

            var encoded = Convert.ToBase64String(item);
            if (!cuckoo.Members.TryGetValue(encoded, out var count))
            {
                cuckoo.Members[encoded] = 1;
            }
            else
            {
                cuckoo.Members[encoded] = count + 1;
            }

            return Task.FromResult(new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, true));
        }

        public Task<ProbabilisticBoolResult> CuckooAddNxAsync(string key, byte[] item, bool noCreate, CancellationToken token = default)
        {
            if (HasNonCuckooType(key))
            {
                return Task.FromResult(new ProbabilisticBoolResult(ProbabilisticResultStatus.WrongType, false));
            }

            if (!_cuckoo.TryGetValue(key, out var cuckoo))
            {
                if (noCreate)
                {
                    return Task.FromResult(new ProbabilisticBoolResult(ProbabilisticResultStatus.NotFound, false));
                }

                cuckoo = new CuckooSketchModel(100);
                _cuckoo[key] = cuckoo;
                _data.Remove(key);
            }

            var encoded = Convert.ToBase64String(item);
            if (cuckoo.Members.ContainsKey(encoded))
            {
                return Task.FromResult(new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, false));
            }

            cuckoo.Members[encoded] = 1;
            return Task.FromResult(new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, true));
        }

        public Task<ProbabilisticBoolResult> CuckooExistsAsync(string key, byte[] item, CancellationToken token = default)
        {
            if (HasNonCuckooType(key))
            {
                return Task.FromResult(new ProbabilisticBoolResult(ProbabilisticResultStatus.WrongType, false));
            }

            if (!_cuckoo.TryGetValue(key, out var cuckoo))
            {
                return Task.FromResult(new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, false));
            }

            var exists = cuckoo.Members.TryGetValue(Convert.ToBase64String(item), out var count) && count > 0;
            return Task.FromResult(new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, exists));
        }

        public Task<ProbabilisticBoolResult> CuckooDeleteAsync(string key, byte[] item, CancellationToken token = default)
        {
            if (HasNonCuckooType(key))
            {
                return Task.FromResult(new ProbabilisticBoolResult(ProbabilisticResultStatus.WrongType, false));
            }

            if (!_cuckoo.TryGetValue(key, out var cuckoo))
            {
                return Task.FromResult(new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, false));
            }

            var encoded = Convert.ToBase64String(item);
            if (!cuckoo.Members.TryGetValue(encoded, out var count) || count <= 0)
            {
                return Task.FromResult(new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, false));
            }

            if (count == 1)
            {
                cuckoo.Members.Remove(encoded);
            }
            else
            {
                cuckoo.Members[encoded] = count - 1;
            }

            return Task.FromResult(new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, true));
        }

        public Task<ProbabilisticCountResult> CuckooCountAsync(string key, byte[] item, CancellationToken token = default)
        {
            if (HasNonCuckooType(key))
            {
                return Task.FromResult(new ProbabilisticCountResult(ProbabilisticResultStatus.WrongType, 0));
            }

            if (!_cuckoo.TryGetValue(key, out var cuckoo))
            {
                return Task.FromResult(new ProbabilisticCountResult(ProbabilisticResultStatus.Ok, 0));
            }

            var encoded = Convert.ToBase64String(item);
            var count = cuckoo.Members.TryGetValue(encoded, out var value) ? value : 0;
            return Task.FromResult(new ProbabilisticCountResult(ProbabilisticResultStatus.Ok, count));
        }

        public Task<ProbabilisticInfoResult> CuckooInfoAsync(string key, CancellationToken token = default)
        {
            if (HasNonCuckooType(key))
            {
                return Task.FromResult(new ProbabilisticInfoResult(ProbabilisticResultStatus.WrongType, Array.Empty<KeyValuePair<string, string>>()));
            }

            if (!_cuckoo.TryGetValue(key, out var cuckoo))
            {
                return Task.FromResult(new ProbabilisticInfoResult(ProbabilisticResultStatus.NotFound, Array.Empty<KeyValuePair<string, string>>()));
            }

            var total = cuckoo.Members.Values.Sum();
            var fields = new[]
            {
                new KeyValuePair<string, string>("Size", total.ToString(CultureInfo.InvariantCulture)),
                new KeyValuePair<string, string>("Number of buckets", cuckoo.Capacity.ToString(CultureInfo.InvariantCulture)),
                new KeyValuePair<string, string>("Number of filters", "1"),
                new KeyValuePair<string, string>("Number of items inserted", total.ToString(CultureInfo.InvariantCulture)),
                new KeyValuePair<string, string>("Bucket size", "2"),
                new KeyValuePair<string, string>("Expansion rate", "1")
            };

            return Task.FromResult(new ProbabilisticInfoResult(ProbabilisticResultStatus.Ok, fields));
        }

        public Task<ProbabilisticResultStatus> TDigestCreateAsync(string key, int compression, CancellationToken token = default)
        {
            if (compression <= 0)
            {
                return Task.FromResult(ProbabilisticResultStatus.InvalidArgument);
            }

            if (_tdigest.ContainsKey(key))
            {
                return Task.FromResult(ProbabilisticResultStatus.Exists);
            }

            if (HasNonTDigestType(key))
            {
                return Task.FromResult(ProbabilisticResultStatus.WrongType);
            }

            _tdigest[key] = new TDigestSketchModel(compression);
            _data.Remove(key);
            return Task.FromResult(ProbabilisticResultStatus.Ok);
        }

        public Task<ProbabilisticResultStatus> TDigestResetAsync(string key, CancellationToken token = default)
        {
            if (HasNonTDigestType(key))
            {
                return Task.FromResult(ProbabilisticResultStatus.WrongType);
            }

            if (!_tdigest.TryGetValue(key, out var digest))
            {
                return Task.FromResult(ProbabilisticResultStatus.NotFound);
            }

            digest.Values.Clear();
            return Task.FromResult(ProbabilisticResultStatus.Ok);
        }

        public Task<ProbabilisticResultStatus> TDigestAddAsync(string key, double[] values, CancellationToken token = default)
        {
            if (HasNonTDigestType(key))
            {
                return Task.FromResult(ProbabilisticResultStatus.WrongType);
            }

            if (!_tdigest.TryGetValue(key, out var digest))
            {
                return Task.FromResult(ProbabilisticResultStatus.NotFound);
            }

            digest.Values.AddRange(values);
            return Task.FromResult(ProbabilisticResultStatus.Ok);
        }

        public Task<ProbabilisticDoubleArrayResult> TDigestQuantileAsync(string key, double[] quantiles, CancellationToken token = default)
        {
            if (HasNonTDigestType(key))
            {
                return Task.FromResult(new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.WrongType, Array.Empty<double>()));
            }

            if (!_tdigest.TryGetValue(key, out var digest))
            {
                return Task.FromResult(new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.NotFound, Array.Empty<double>()));
            }

            var sorted = digest.Values.OrderBy(v => v).ToArray();
            var output = new double[quantiles.Length];
            if (sorted.Length == 0)
            {
                return Task.FromResult(new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.Ok, Enumerable.Repeat(double.NaN, quantiles.Length).ToArray()));
            }

            for (int i = 0; i < quantiles.Length; i++)
            {
                var q = Math.Clamp(quantiles[i], 0d, 1d);
                var position = q * (sorted.Length - 1);
                var low = (int)Math.Floor(position);
                var high = (int)Math.Ceiling(position);
                if (low == high)
                {
                    output[i] = sorted[low];
                }
                else
                {
                    var frac = position - low;
                    output[i] = sorted[low] + (sorted[high] - sorted[low]) * frac;
                }
            }

            return Task.FromResult(new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.Ok, output));
        }

        public Task<ProbabilisticDoubleArrayResult> TDigestCdfAsync(string key, double[] values, CancellationToken token = default)
        {
            if (HasNonTDigestType(key))
            {
                return Task.FromResult(new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.WrongType, Array.Empty<double>()));
            }

            if (!_tdigest.TryGetValue(key, out var digest))
            {
                return Task.FromResult(new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.NotFound, Array.Empty<double>()));
            }

            var sorted = digest.Values.OrderBy(v => v).ToArray();
            var output = new double[values.Length];
            if (sorted.Length == 0)
            {
                return Task.FromResult(new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.Ok, output));
            }

            for (int i = 0; i < values.Length; i++)
            {
                var index = Array.BinarySearch(sorted, values[i]);
                if (index < 0)
                {
                    index = ~index;
                }
                else
                {
                    while (index < sorted.Length && sorted[index] <= values[i])
                    {
                        index++;
                    }
                }

                output[i] = (double)index / sorted.Length;
            }

            return Task.FromResult(new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.Ok, output));
        }

        public Task<ProbabilisticArrayResult> TDigestRankAsync(string key, double[] values, CancellationToken token = default)
        {
            if (HasNonTDigestType(key))
            {
                return Task.FromResult(new ProbabilisticArrayResult(ProbabilisticResultStatus.WrongType, Array.Empty<long>()));
            }

            if (!_tdigest.TryGetValue(key, out var digest))
            {
                return Task.FromResult(new ProbabilisticArrayResult(ProbabilisticResultStatus.NotFound, Array.Empty<long>()));
            }

            var sorted = digest.Values.OrderBy(v => v).ToArray();
            var output = new long[values.Length];
            for (int i = 0; i < values.Length; i++)
            {
                var idx = Array.BinarySearch(sorted, values[i]);
                if (idx < 0)
                {
                    idx = ~idx;
                }
                else
                {
                    while (idx > 0 && sorted[idx - 1] >= values[i])
                    {
                        idx--;
                    }
                }

                output[i] = idx;
            }

            return Task.FromResult(new ProbabilisticArrayResult(ProbabilisticResultStatus.Ok, output));
        }

        public Task<ProbabilisticArrayResult> TDigestRevRankAsync(string key, double[] values, CancellationToken token = default)
        {
            if (HasNonTDigestType(key))
            {
                return Task.FromResult(new ProbabilisticArrayResult(ProbabilisticResultStatus.WrongType, Array.Empty<long>()));
            }

            if (!_tdigest.TryGetValue(key, out var digest))
            {
                return Task.FromResult(new ProbabilisticArrayResult(ProbabilisticResultStatus.NotFound, Array.Empty<long>()));
            }

            var sorted = digest.Values.OrderBy(v => v).ToArray();
            var output = new long[values.Length];
            for (int i = 0; i < values.Length; i++)
            {
                var idx = Array.BinarySearch(sorted, values[i]);
                if (idx < 0)
                {
                    idx = ~idx;
                }
                else
                {
                    while (idx < sorted.Length && sorted[idx] <= values[i])
                    {
                        idx++;
                    }
                }

                output[i] = sorted.Length - idx;
            }

            return Task.FromResult(new ProbabilisticArrayResult(ProbabilisticResultStatus.Ok, output));
        }

        public Task<ProbabilisticDoubleArrayResult> TDigestByRankAsync(string key, long[] ranks, CancellationToken token = default)
        {
            if (HasNonTDigestType(key))
            {
                return Task.FromResult(new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.WrongType, Array.Empty<double>()));
            }

            if (!_tdigest.TryGetValue(key, out var digest))
            {
                return Task.FromResult(new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.NotFound, Array.Empty<double>()));
            }

            var sorted = digest.Values.OrderBy(v => v).ToArray();
            var output = new double[ranks.Length];
            for (int i = 0; i < ranks.Length; i++)
            {
                var rank = ranks[i];
                if (rank < 0 || rank >= sorted.Length)
                {
                    output[i] = double.NaN;
                }
                else
                {
                    output[i] = sorted[rank];
                }
            }

            return Task.FromResult(new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.Ok, output));
        }

        public Task<ProbabilisticDoubleArrayResult> TDigestByRevRankAsync(string key, long[] ranks, CancellationToken token = default)
        {
            if (HasNonTDigestType(key))
            {
                return Task.FromResult(new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.WrongType, Array.Empty<double>()));
            }

            if (!_tdigest.TryGetValue(key, out var digest))
            {
                return Task.FromResult(new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.NotFound, Array.Empty<double>()));
            }

            var sorted = digest.Values.OrderBy(v => v).ToArray();
            var output = new double[ranks.Length];
            for (int i = 0; i < ranks.Length; i++)
            {
                var rank = ranks[i];
                if (rank < 0 || rank >= sorted.Length)
                {
                    output[i] = double.NaN;
                }
                else
                {
                    output[i] = sorted[sorted.Length - 1 - rank];
                }
            }

            return Task.FromResult(new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.Ok, output));
        }

        public Task<ProbabilisticDoubleResult> TDigestTrimmedMeanAsync(string key, double lowerQuantile, double upperQuantile, CancellationToken token = default)
        {
            if (HasNonTDigestType(key))
            {
                return Task.FromResult(new ProbabilisticDoubleResult(ProbabilisticResultStatus.WrongType, null));
            }

            if (!_tdigest.TryGetValue(key, out var digest))
            {
                return Task.FromResult(new ProbabilisticDoubleResult(ProbabilisticResultStatus.NotFound, null));
            }

            if (lowerQuantile < 0 || lowerQuantile > 1 || upperQuantile < 0 || upperQuantile > 1 || lowerQuantile > upperQuantile)
            {
                return Task.FromResult(new ProbabilisticDoubleResult(ProbabilisticResultStatus.InvalidArgument, null));
            }

            var sorted = digest.Values.OrderBy(v => v).ToArray();
            if (sorted.Length == 0)
            {
                return Task.FromResult(new ProbabilisticDoubleResult(ProbabilisticResultStatus.Ok, double.NaN));
            }

            var start = (int)Math.Floor(lowerQuantile * (sorted.Length - 1));
            var end = (int)Math.Ceiling(upperQuantile * (sorted.Length - 1));
            start = Math.Clamp(start, 0, sorted.Length - 1);
            end = Math.Clamp(end, 0, sorted.Length - 1);
            if (end < start)
            {
                return Task.FromResult(new ProbabilisticDoubleResult(ProbabilisticResultStatus.Ok, double.NaN));
            }

            double sum = 0;
            var count = 0;
            for (int i = start; i <= end; i++)
            {
                sum += sorted[i];
                count++;
            }

            return Task.FromResult(new ProbabilisticDoubleResult(ProbabilisticResultStatus.Ok, count == 0 ? double.NaN : sum / count));
        }

        public Task<ProbabilisticDoubleResult> TDigestMinAsync(string key, CancellationToken token = default)
        {
            if (HasNonTDigestType(key))
            {
                return Task.FromResult(new ProbabilisticDoubleResult(ProbabilisticResultStatus.WrongType, null));
            }

            if (!_tdigest.TryGetValue(key, out var digest))
            {
                return Task.FromResult(new ProbabilisticDoubleResult(ProbabilisticResultStatus.NotFound, null));
            }

            var value = digest.Values.Count == 0 ? (double?)null : digest.Values.Min();
            return Task.FromResult(new ProbabilisticDoubleResult(ProbabilisticResultStatus.Ok, value));
        }

        public Task<ProbabilisticDoubleResult> TDigestMaxAsync(string key, CancellationToken token = default)
        {
            if (HasNonTDigestType(key))
            {
                return Task.FromResult(new ProbabilisticDoubleResult(ProbabilisticResultStatus.WrongType, null));
            }

            if (!_tdigest.TryGetValue(key, out var digest))
            {
                return Task.FromResult(new ProbabilisticDoubleResult(ProbabilisticResultStatus.NotFound, null));
            }

            var value = digest.Values.Count == 0 ? (double?)null : digest.Values.Max();
            return Task.FromResult(new ProbabilisticDoubleResult(ProbabilisticResultStatus.Ok, value));
        }

        public Task<ProbabilisticInfoResult> TDigestInfoAsync(string key, CancellationToken token = default)
        {
            if (HasNonTDigestType(key))
            {
                return Task.FromResult(new ProbabilisticInfoResult(ProbabilisticResultStatus.WrongType, Array.Empty<KeyValuePair<string, string>>()));
            }

            if (!_tdigest.TryGetValue(key, out var digest))
            {
                return Task.FromResult(new ProbabilisticInfoResult(ProbabilisticResultStatus.NotFound, Array.Empty<KeyValuePair<string, string>>()));
            }

            var fields = new[]
            {
                new KeyValuePair<string, string>("Compression", digest.Compression.ToString(CultureInfo.InvariantCulture)),
                new KeyValuePair<string, string>("Merged nodes", digest.Values.Count.ToString(CultureInfo.InvariantCulture)),
                new KeyValuePair<string, string>("Unmerged nodes", "0"),
                new KeyValuePair<string, string>("Observations", digest.Values.Count.ToString(CultureInfo.InvariantCulture)),
                new KeyValuePair<string, string>("Total compressions", "0")
            };

            return Task.FromResult(new ProbabilisticInfoResult(ProbabilisticResultStatus.Ok, fields));
        }

        public Task<ProbabilisticResultStatus> TopKReserveAsync(string key, int k, int width, int depth, double decay, CancellationToken token = default)
        {
            if (k <= 0 || width <= 0 || depth <= 0 || decay <= 0 || decay >= 1)
            {
                return Task.FromResult(ProbabilisticResultStatus.InvalidArgument);
            }

            if (_topk.ContainsKey(key))
            {
                return Task.FromResult(ProbabilisticResultStatus.Exists);
            }

            if (HasNonTopKType(key))
            {
                return Task.FromResult(ProbabilisticResultStatus.WrongType);
            }

            _topk[key] = new TopKSketchModel(k, width, depth, decay);
            _data.Remove(key);
            return Task.FromResult(ProbabilisticResultStatus.Ok);
        }

        public Task<ProbabilisticStringArrayResult> TopKAddAsync(string key, byte[][] items, CancellationToken token = default)
        {
            if (HasNonTopKType(key))
            {
                return Task.FromResult(new ProbabilisticStringArrayResult(ProbabilisticResultStatus.WrongType, Array.Empty<string?>()));
            }

            if (!_topk.TryGetValue(key, out var topk))
            {
                return Task.FromResult(new ProbabilisticStringArrayResult(ProbabilisticResultStatus.NotFound, Array.Empty<string?>()));
            }

            var dropped = new string?[items.Length];
            for (int i = 0; i < items.Length; i++)
            {
                dropped[i] = TopKApplyIncrement(topk, items[i], 1);
            }

            return Task.FromResult(new ProbabilisticStringArrayResult(ProbabilisticResultStatus.Ok, dropped));
        }

        public Task<ProbabilisticStringArrayResult> TopKIncrByAsync(string key, KeyValuePair<byte[], long>[] increments, CancellationToken token = default)
        {
            if (HasNonTopKType(key))
            {
                return Task.FromResult(new ProbabilisticStringArrayResult(ProbabilisticResultStatus.WrongType, Array.Empty<string?>()));
            }

            if (!_topk.TryGetValue(key, out var topk))
            {
                return Task.FromResult(new ProbabilisticStringArrayResult(ProbabilisticResultStatus.NotFound, Array.Empty<string?>()));
            }

            var dropped = new string?[increments.Length];
            for (int i = 0; i < increments.Length; i++)
            {
                dropped[i] = TopKApplyIncrement(topk, increments[i].Key, increments[i].Value);
            }

            return Task.FromResult(new ProbabilisticStringArrayResult(ProbabilisticResultStatus.Ok, dropped));
        }

        public Task<ProbabilisticArrayResult> TopKQueryAsync(string key, byte[][] items, CancellationToken token = default)
        {
            if (HasNonTopKType(key))
            {
                return Task.FromResult(new ProbabilisticArrayResult(ProbabilisticResultStatus.WrongType, Array.Empty<long>()));
            }

            if (!_topk.TryGetValue(key, out var topk))
            {
                return Task.FromResult(new ProbabilisticArrayResult(ProbabilisticResultStatus.NotFound, Array.Empty<long>()));
            }

            var top = TopKOrdered(topk).Select(x => x.Key).ToHashSet(StringComparer.Ordinal);
            var result = new long[items.Length];
            for (int i = 0; i < items.Length; i++)
            {
                result[i] = top.Contains(Convert.ToBase64String(items[i])) ? 1 : 0;
            }

            return Task.FromResult(new ProbabilisticArrayResult(ProbabilisticResultStatus.Ok, result));
        }

        public Task<ProbabilisticArrayResult> TopKCountAsync(string key, byte[][] items, CancellationToken token = default)
        {
            if (HasNonTopKType(key))
            {
                return Task.FromResult(new ProbabilisticArrayResult(ProbabilisticResultStatus.WrongType, Array.Empty<long>()));
            }

            if (!_topk.TryGetValue(key, out var topk))
            {
                return Task.FromResult(new ProbabilisticArrayResult(ProbabilisticResultStatus.NotFound, Array.Empty<long>()));
            }

            var result = new long[items.Length];
            for (int i = 0; i < items.Length; i++)
            {
                result[i] = topk.Counts.TryGetValue(Convert.ToBase64String(items[i]), out var count) ? count : 0;
            }

            return Task.FromResult(new ProbabilisticArrayResult(ProbabilisticResultStatus.Ok, result));
        }

        public Task<ProbabilisticStringArrayResult> TopKListAsync(string key, bool withCount, CancellationToken token = default)
        {
            if (HasNonTopKType(key))
            {
                return Task.FromResult(new ProbabilisticStringArrayResult(ProbabilisticResultStatus.WrongType, Array.Empty<string?>()));
            }

            if (!_topk.TryGetValue(key, out var topk))
            {
                return Task.FromResult(new ProbabilisticStringArrayResult(ProbabilisticResultStatus.NotFound, Array.Empty<string?>()));
            }

            var ordered = TopKOrdered(topk);
            if (!withCount)
            {
                var values = ordered.Select(x => Utf8.GetString(Convert.FromBase64String(x.Key))).Cast<string?>().ToArray();
                return Task.FromResult(new ProbabilisticStringArrayResult(ProbabilisticResultStatus.Ok, values));
            }

            var output = new string?[ordered.Count * 2];
            for (int i = 0; i < ordered.Count; i++)
            {
                output[i * 2] = Utf8.GetString(Convert.FromBase64String(ordered[i].Key));
                output[i * 2 + 1] = ordered[i].Value.ToString(CultureInfo.InvariantCulture);
            }

            return Task.FromResult(new ProbabilisticStringArrayResult(ProbabilisticResultStatus.Ok, output));
        }

        public Task<ProbabilisticInfoResult> TopKInfoAsync(string key, CancellationToken token = default)
        {
            if (HasNonTopKType(key))
            {
                return Task.FromResult(new ProbabilisticInfoResult(ProbabilisticResultStatus.WrongType, Array.Empty<KeyValuePair<string, string>>()));
            }

            if (!_topk.TryGetValue(key, out var topk))
            {
                return Task.FromResult(new ProbabilisticInfoResult(ProbabilisticResultStatus.NotFound, Array.Empty<KeyValuePair<string, string>>()));
            }

            var fields = new[]
            {
                new KeyValuePair<string, string>("k", topk.K.ToString(CultureInfo.InvariantCulture)),
                new KeyValuePair<string, string>("width", topk.Width.ToString(CultureInfo.InvariantCulture)),
                new KeyValuePair<string, string>("depth", topk.Depth.ToString(CultureInfo.InvariantCulture)),
                new KeyValuePair<string, string>("decay", topk.Decay.ToString("G17", CultureInfo.InvariantCulture))
            };

            return Task.FromResult(new ProbabilisticInfoResult(ProbabilisticResultStatus.Ok, fields));
        }

        public Task<VectorSetResult> VectorSetAsync(
            string key,
            double[] vector,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sets.ContainsKey(key) || _sortedSets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(new VectorSetResult(VectorResultStatus.WrongType));
            }

            _vectors[key] = vector.ToArray();
            _data.Remove(key);
            _hashes.Remove(key);
            _lists.Remove(key);
            _sets.Remove(key);
            _sortedSets.Remove(key);
            _streams.Remove(key);
            _streamLastIds.Remove(key);
            _streamGroups.Remove(key);
            return Task.FromResult(new VectorSetResult(VectorResultStatus.Ok));
        }

        public Task<VectorGetResult> VectorGetAsync(
            string key,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sets.ContainsKey(key) || _sortedSets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(new VectorGetResult(VectorResultStatus.WrongType, null));
            }

            if (!_vectors.TryGetValue(key, out var vector))
            {
                return Task.FromResult(new VectorGetResult(VectorResultStatus.NotFound, null));
            }

            return Task.FromResult(new VectorGetResult(VectorResultStatus.Ok, vector.ToArray()));
        }

        public Task<VectorSizeResult> VectorSizeAsync(
            string key,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sets.ContainsKey(key) || _sortedSets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(new VectorSizeResult(VectorResultStatus.WrongType, 0));
            }

            if (!_vectors.TryGetValue(key, out var vector))
            {
                return Task.FromResult(new VectorSizeResult(VectorResultStatus.NotFound, 0));
            }

            return Task.FromResult(new VectorSizeResult(VectorResultStatus.Ok, vector.LongLength));
        }

        public Task<VectorSimilarityResult> VectorSimilarityAsync(
            string key,
            string otherKey,
            string metric,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (IsExpired(otherKey))
            {
                RemoveKey(otherKey);
            }

            if ((_data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sets.ContainsKey(key) || _sortedSets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key)) ||
                (_data.ContainsKey(otherKey) || _hashes.ContainsKey(otherKey) || _lists.ContainsKey(otherKey) || _sets.ContainsKey(otherKey) || _sortedSets.ContainsKey(otherKey) || _streams.ContainsKey(otherKey) || _streamGroups.ContainsKey(otherKey)))
            {
                return Task.FromResult(new VectorSimilarityResult(VectorResultStatus.WrongType, null));
            }

            if (!_vectors.TryGetValue(key, out var left) || !_vectors.TryGetValue(otherKey, out var right))
            {
                return Task.FromResult(new VectorSimilarityResult(VectorResultStatus.NotFound, null));
            }

            if (left.Length != right.Length || left.Length == 0)
            {
                return Task.FromResult(new VectorSimilarityResult(VectorResultStatus.InvalidArgument, null));
            }

            if (metric.Equals("DOT", StringComparison.OrdinalIgnoreCase))
            {
                double dot = 0;
                for (int i = 0; i < left.Length; i++)
                {
                    dot += left[i] * right[i];
                }

                return Task.FromResult(new VectorSimilarityResult(VectorResultStatus.Ok, dot));
            }

            if (metric.Equals("COSINE", StringComparison.OrdinalIgnoreCase))
            {
                double dot = 0;
                double leftNorm = 0;
                double rightNorm = 0;
                for (int i = 0; i < left.Length; i++)
                {
                    dot += left[i] * right[i];
                    leftNorm += left[i] * left[i];
                    rightNorm += right[i] * right[i];
                }

                if (leftNorm <= 0 || rightNorm <= 0)
                {
                    return Task.FromResult(new VectorSimilarityResult(VectorResultStatus.InvalidArgument, null));
                }

                var cosine = dot / (Math.Sqrt(leftNorm) * Math.Sqrt(rightNorm));
                return Task.FromResult(new VectorSimilarityResult(VectorResultStatus.Ok, cosine));
            }

            if (metric.Equals("L2", StringComparison.OrdinalIgnoreCase))
            {
                double sum = 0;
                for (int i = 0; i < left.Length; i++)
                {
                    var delta = left[i] - right[i];
                    sum += delta * delta;
                }

                return Task.FromResult(new VectorSimilarityResult(VectorResultStatus.Ok, Math.Sqrt(sum)));
            }

            return Task.FromResult(new VectorSimilarityResult(VectorResultStatus.InvalidArgument, null));
        }

        public Task<VectorDeleteResult> VectorDeleteAsync(
            string key,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (_data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sets.ContainsKey(key) || _sortedSets.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key))
            {
                return Task.FromResult(new VectorDeleteResult(VectorResultStatus.WrongType, 0));
            }

            var removed = _vectors.Remove(key);
            if (removed)
            {
                _expirations.Remove(key);
            }

            return Task.FromResult(new VectorDeleteResult(VectorResultStatus.Ok, removed ? 1 : 0));
        }

        public Task<VectorSearchResult> VectorSearchAsync(
            string keyPrefix,
            int topK,
            int offset,
            string metric,
            double[] queryVector,
            CancellationToken token = default)
        {
            if (queryVector.Length == 0 || topK <= 0 || offset < 0)
            {
                return Task.FromResult(new VectorSearchResult(VectorResultStatus.InvalidArgument, Array.Empty<VectorSearchEntry>()));
            }

            var scored = new List<VectorSearchEntry>();
            foreach (var kvp in _vectors)
            {
                if (!kvp.Key.StartsWith(keyPrefix, StringComparison.Ordinal))
                {
                    continue;
                }

                if (IsExpired(kvp.Key))
                {
                    RemoveKey(kvp.Key);
                    continue;
                }

                var candidate = kvp.Value;
                if (candidate.Length != queryVector.Length)
                {
                    continue;
                }

                double score;
                if (metric.Equals("DOT", StringComparison.OrdinalIgnoreCase))
                {
                    score = 0;
                    for (int i = 0; i < candidate.Length; i++)
                    {
                        score += queryVector[i] * candidate[i];
                    }
                }
                else if (metric.Equals("COSINE", StringComparison.OrdinalIgnoreCase))
                {
                    double dot = 0;
                    double queryNorm = 0;
                    double candNorm = 0;
                    for (int i = 0; i < candidate.Length; i++)
                    {
                        dot += queryVector[i] * candidate[i];
                        queryNorm += queryVector[i] * queryVector[i];
                        candNorm += candidate[i] * candidate[i];
                    }

                    if (queryNorm <= 0 || candNorm <= 0)
                    {
                        continue;
                    }

                    score = dot / (Math.Sqrt(queryNorm) * Math.Sqrt(candNorm));
                }
                else if (metric.Equals("L2", StringComparison.OrdinalIgnoreCase))
                {
                    double sum = 0;
                    for (int i = 0; i < candidate.Length; i++)
                    {
                        var delta = queryVector[i] - candidate[i];
                        sum += delta * delta;
                    }

                    score = Math.Sqrt(sum);
                }
                else
                {
                    return Task.FromResult(new VectorSearchResult(VectorResultStatus.InvalidArgument, Array.Empty<VectorSearchEntry>()));
                }

                scored.Add(new VectorSearchEntry(kvp.Key, score));
            }

            IEnumerable<VectorSearchEntry> ordered;
            if (metric.Equals("L2", StringComparison.OrdinalIgnoreCase))
            {
                ordered = scored.OrderBy(entry => entry.Score).ThenBy(entry => entry.Key, StringComparer.Ordinal);
            }
            else
            {
                ordered = scored.OrderByDescending(entry => entry.Score).ThenBy(entry => entry.Key, StringComparer.Ordinal);
            }

            var top = ordered.Skip(offset).Take(topK).ToArray();
            return Task.FromResult(new VectorSearchResult(VectorResultStatus.Ok, top));
        }

        public Task<TimeSeriesResultStatus> TimeSeriesCreateAsync(string key, long? retentionTimeMs, CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (retentionTimeMs.HasValue && retentionTimeMs.Value < 0)
            {
                return Task.FromResult(TimeSeriesResultStatus.InvalidArgument);
            }

            if (_timeSeries.ContainsKey(key))
            {
                return Task.FromResult(TimeSeriesResultStatus.Exists);
            }

            if (HasNonTimeSeriesType(key))
            {
                return Task.FromResult(TimeSeriesResultStatus.WrongType);
            }

            _timeSeries[key] = new SortedDictionary<long, double>();
            _timeSeriesRetention[key] = retentionTimeMs ?? 0;
            _data.Remove(key);
            return Task.FromResult(TimeSeriesResultStatus.Ok);
        }

        public Task<TimeSeriesAddResult> TimeSeriesAddAsync(
            string key,
            long timestamp,
            double value,
            bool createIfMissing,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (double.IsNaN(value) || double.IsInfinity(value))
            {
                return Task.FromResult(new TimeSeriesAddResult(TimeSeriesResultStatus.InvalidArgument, null));
            }

            if (HasNonTimeSeriesType(key))
            {
                return Task.FromResult(new TimeSeriesAddResult(TimeSeriesResultStatus.WrongType, null));
            }

            if (!_timeSeries.TryGetValue(key, out var samples))
            {
                if (!createIfMissing)
                {
                    return Task.FromResult(new TimeSeriesAddResult(TimeSeriesResultStatus.NotFound, null));
                }

                samples = new SortedDictionary<long, double>();
                _timeSeries[key] = samples;
                _data.Remove(key);
                _hashes.Remove(key);
                _lists.Remove(key);
                _sets.Remove(key);
                _sortedSets.Remove(key);
                _vectors.Remove(key);
                _streams.Remove(key);
                _streamLastIds.Remove(key);
                _streamGroups.Remove(key);
                _bloom.Remove(key);
                _cuckoo.Remove(key);
                _tdigest.Remove(key);
                _topk.Remove(key);
                _timeSeriesRetention[key] = 0;
            }

            samples[timestamp] = value;
            ApplyTimeSeriesRetention(key, samples, timestamp);
            return Task.FromResult(new TimeSeriesAddResult(TimeSeriesResultStatus.Ok, timestamp));
        }

        public Task<TimeSeriesAddResult> TimeSeriesIncrementByAsync(
            string key,
            double increment,
            long? timestamp,
            bool createIfMissing,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
            }

            if (double.IsNaN(increment) || double.IsInfinity(increment))
            {
                return Task.FromResult(new TimeSeriesAddResult(TimeSeriesResultStatus.InvalidArgument, null));
            }

            if (HasNonTimeSeriesType(key))
            {
                return Task.FromResult(new TimeSeriesAddResult(TimeSeriesResultStatus.WrongType, null));
            }

            if (!_timeSeries.TryGetValue(key, out var samples))
            {
                if (!createIfMissing)
                {
                    return Task.FromResult(new TimeSeriesAddResult(TimeSeriesResultStatus.NotFound, null));
                }

                samples = new SortedDictionary<long, double>();
                _timeSeries[key] = samples;
                _timeSeriesRetention[key] = 0;
            }

            var targetTimestamp = timestamp ?? (samples.Count > 0 ? samples.Last().Key : DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            var baseValue = samples.TryGetValue(targetTimestamp, out var existing) ? existing : 0d;
            samples[targetTimestamp] = baseValue + increment;
            ApplyTimeSeriesRetention(key, samples, targetTimestamp);
            return Task.FromResult(new TimeSeriesAddResult(TimeSeriesResultStatus.Ok, targetTimestamp));
        }

        public Task<TimeSeriesGetResult> TimeSeriesGetAsync(string key, CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
                return Task.FromResult(new TimeSeriesGetResult(TimeSeriesResultStatus.NotFound, null));
            }

            if (HasNonTimeSeriesType(key))
            {
                return Task.FromResult(new TimeSeriesGetResult(TimeSeriesResultStatus.WrongType, null));
            }

            if (!_timeSeries.TryGetValue(key, out var samples) || samples.Count == 0)
            {
                return Task.FromResult(new TimeSeriesGetResult(TimeSeriesResultStatus.NotFound, null));
            }

            var latest = samples.Last();
            return Task.FromResult(new TimeSeriesGetResult(TimeSeriesResultStatus.Ok, new TimeSeriesSample(latest.Key, latest.Value)));
        }

        public Task<TimeSeriesRangeResult> TimeSeriesRangeAsync(
            string key,
            long fromTimestamp,
            long toTimestamp,
            bool reverse,
            int? count,
            string? aggregationType,
            long? bucketDurationMs,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
                return Task.FromResult(new TimeSeriesRangeResult(TimeSeriesResultStatus.NotFound, Array.Empty<TimeSeriesSample>()));
            }

            if (HasNonTimeSeriesType(key))
            {
                return Task.FromResult(new TimeSeriesRangeResult(TimeSeriesResultStatus.WrongType, Array.Empty<TimeSeriesSample>()));
            }

            if (count.HasValue && count.Value < 0)
            {
                return Task.FromResult(new TimeSeriesRangeResult(TimeSeriesResultStatus.InvalidArgument, Array.Empty<TimeSeriesSample>()));
            }

            if (!string.IsNullOrEmpty(aggregationType) && (!bucketDurationMs.HasValue || bucketDurationMs.Value <= 0))
            {
                return Task.FromResult(new TimeSeriesRangeResult(TimeSeriesResultStatus.InvalidArgument, Array.Empty<TimeSeriesSample>()));
            }

            if (!_timeSeries.TryGetValue(key, out var samples))
            {
                return Task.FromResult(new TimeSeriesRangeResult(TimeSeriesResultStatus.NotFound, Array.Empty<TimeSeriesSample>()));
            }

            IEnumerable<TimeSeriesSample> filtered;
            if (string.IsNullOrEmpty(aggregationType))
            {
                filtered = samples
                    .Where(x => x.Key >= fromTimestamp && x.Key <= toTimestamp)
                    .Select(x => new TimeSeriesSample(x.Key, x.Value));
            }
            else
            {
                var bucketMs = bucketDurationMs!.Value;
                var buckets = new SortedDictionary<long, List<double>>();
                foreach (var point in samples)
                {
                    if (point.Key < fromTimestamp || point.Key > toTimestamp)
                    {
                        continue;
                    }

                    var bucketStart = point.Key >= 0 ? (point.Key / bucketMs) * bucketMs : ((point.Key - bucketMs + 1) / bucketMs) * bucketMs;
                    if (!buckets.TryGetValue(bucketStart, out var list))
                    {
                        list = new List<double>();
                        buckets[bucketStart] = list;
                    }

                    list.Add(point.Value);
                }

                filtered = buckets.Select(bucket =>
                {
                    var values = bucket.Value;
                    double aggregated = aggregationType switch
                    {
                        "SUM" => values.Sum(),
                        "MIN" => values.Min(),
                        "MAX" => values.Max(),
                        "COUNT" => values.Count,
                        _ => values.Average()
                    };

                    return new TimeSeriesSample(bucket.Key, aggregated);
                });
            }

            filtered = reverse ? filtered.Reverse() : filtered;
            if (count.HasValue)
            {
                filtered = filtered.Take(count.Value);
            }

            var result = filtered.ToArray();
            return Task.FromResult(new TimeSeriesRangeResult(TimeSeriesResultStatus.Ok, result));
        }

        public Task<TimeSeriesDeleteResult> TimeSeriesDeleteAsync(
            string key,
            long fromTimestamp,
            long toTimestamp,
            CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
                return Task.FromResult(new TimeSeriesDeleteResult(TimeSeriesResultStatus.NotFound, 0));
            }

            if (HasNonTimeSeriesType(key))
            {
                return Task.FromResult(new TimeSeriesDeleteResult(TimeSeriesResultStatus.WrongType, 0));
            }

            if (!_timeSeries.TryGetValue(key, out var samples) || samples.Count == 0)
            {
                return Task.FromResult(new TimeSeriesDeleteResult(TimeSeriesResultStatus.NotFound, 0));
            }

            var keysToDelete = samples.Keys.Where(ts => ts >= fromTimestamp && ts <= toTimestamp).ToArray();
            foreach (var ts in keysToDelete)
            {
                samples.Remove(ts);
            }

            if (samples.Count == 0)
            {
                _timeSeries.Remove(key);
                _timeSeriesRetention.Remove(key);
                _expirations.Remove(key);
            }

            return Task.FromResult(new TimeSeriesDeleteResult(TimeSeriesResultStatus.Ok, keysToDelete.LongLength));
        }

        public Task<TimeSeriesInfoResult> TimeSeriesInfoAsync(string key, CancellationToken token = default)
        {
            if (IsExpired(key))
            {
                RemoveKey(key);
                return Task.FromResult(new TimeSeriesInfoResult(TimeSeriesResultStatus.NotFound, 0, null, null, 0));
            }

            if (HasNonTimeSeriesType(key))
            {
                return Task.FromResult(new TimeSeriesInfoResult(TimeSeriesResultStatus.WrongType, 0, null, null, 0));
            }

            if (!_timeSeries.TryGetValue(key, out var samples))
            {
                return Task.FromResult(new TimeSeriesInfoResult(TimeSeriesResultStatus.NotFound, 0, null, null, 0));
            }

            long? first = samples.Count > 0 ? samples.First().Key : null;
            long? last = samples.Count > 0 ? samples.Last().Key : null;
            var retention = _timeSeriesRetention.TryGetValue(key, out var value) ? value : 0;
            return Task.FromResult(new TimeSeriesInfoResult(TimeSeriesResultStatus.Ok, samples.Count, first, last, retention));
        }

        private void ApplyTimeSeriesRetention(string key, SortedDictionary<long, double> samples, long referenceTimestamp)
        {
            if (!_timeSeriesRetention.TryGetValue(key, out var retentionMs) || retentionMs <= 0 || samples.Count == 0)
            {
                return;
            }

            var minAllowed = referenceTimestamp - retentionMs;
            var toRemove = samples.Keys.Where(ts => ts < minAllowed).ToArray();
            foreach (var ts in toRemove)
            {
                samples.Remove(ts);
            }
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
                _vectors.Remove(key);
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

        private static ulong ComputeHyperLogLogHash(byte[] data)
        {
            unchecked
            {
                ulong hash = 14695981039346656037UL;
                for (int i = 0; i < data.Length; i++)
                {
                    hash ^= data[i];
                    hash *= 1099511628211UL;
                }

                hash ^= hash >> 33;
                hash *= 0xff51afd7ed558ccdUL;
                hash ^= hash >> 33;
                hash *= 0xc4ceb9fe1a85ec53UL;
                hash ^= hash >> 33;

                return hash;
            }
        }

        private static int ComputeHyperLogLogRank(ulong hash)
        {
            var remaining = hash << HyperLogLogPrecision;
            var rank = BitOperations.LeadingZeroCount(remaining) + 1;
            var maxRank = 64 - HyperLogLogPrecision + 1;
            return Math.Min(rank, maxRank);
        }

        private static byte[] EncodeHyperLogLog(byte[] registers)
        {
            var payload = new byte[HyperLogLogMagic.Length + 1 + 2 + HyperLogLogRegistersCount];
            Buffer.BlockCopy(HyperLogLogMagic, 0, payload, 0, HyperLogLogMagic.Length);
            payload[4] = 1;
            payload[5] = (byte)HyperLogLogPrecision;
            payload[6] = 0;
            Buffer.BlockCopy(registers, 0, payload, 7, HyperLogLogRegistersCount);
            return payload;
        }

        private static bool TryDecodeHyperLogLog(byte[] payload, out byte[] registers)
        {
            registers = Array.Empty<byte>();

            var expectedLength = HyperLogLogMagic.Length + 1 + 2 + HyperLogLogRegistersCount;
            if (payload.Length != expectedLength)
            {
                return false;
            }

            for (int i = 0; i < HyperLogLogMagic.Length; i++)
            {
                if (payload[i] != HyperLogLogMagic[i])
                {
                    return false;
                }
            }

            if (payload[4] != 1 || payload[5] != HyperLogLogPrecision)
            {
                return false;
            }

            registers = new byte[HyperLogLogRegistersCount];
            Buffer.BlockCopy(payload, 7, registers, 0, HyperLogLogRegistersCount);
            return true;
        }

        private static long EstimateHyperLogLogCount(byte[] registers)
        {
            const double alpha = 0.7213 / (1.0 + (1.079 / HyperLogLogRegistersCount));
            double invSum = 0.0;
            var zeroCount = 0;

            for (int i = 0; i < registers.Length; i++)
            {
                var registerValue = registers[i];
                if (registerValue == 0)
                {
                    zeroCount++;
                }

                invSum += Math.Pow(2.0, -registerValue);
            }

            var estimate = alpha * HyperLogLogRegistersCount * HyperLogLogRegistersCount / invSum;
            if (estimate <= 2.5 * HyperLogLogRegistersCount && zeroCount > 0)
            {
                estimate = HyperLogLogRegistersCount * Math.Log((double)HyperLogLogRegistersCount / zeroCount);
            }

            if (estimate < 0)
            {
                return 0;
            }

            return (long)Math.Round(estimate, MidpointRounding.AwayFromZero);
        }

        private bool HasNonBloomType(string key)
        {
            return _data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sets.ContainsKey(key) || _sortedSets.ContainsKey(key) || _vectors.ContainsKey(key) || _streams.ContainsKey(key) || _cuckoo.ContainsKey(key) || _tdigest.ContainsKey(key) || _topk.ContainsKey(key) || _timeSeries.ContainsKey(key);
        }

        private bool HasNonCuckooType(string key)
        {
            return _data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sets.ContainsKey(key) || _sortedSets.ContainsKey(key) || _vectors.ContainsKey(key) || _streams.ContainsKey(key) || _bloom.ContainsKey(key) || _tdigest.ContainsKey(key) || _topk.ContainsKey(key) || _timeSeries.ContainsKey(key);
        }

        private bool HasNonTDigestType(string key)
        {
            return _data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sets.ContainsKey(key) || _sortedSets.ContainsKey(key) || _vectors.ContainsKey(key) || _streams.ContainsKey(key) || _bloom.ContainsKey(key) || _cuckoo.ContainsKey(key) || _topk.ContainsKey(key) || _timeSeries.ContainsKey(key);
        }

        private bool HasNonTopKType(string key)
        {
            return _data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sets.ContainsKey(key) || _sortedSets.ContainsKey(key) || _vectors.ContainsKey(key) || _streams.ContainsKey(key) || _bloom.ContainsKey(key) || _cuckoo.ContainsKey(key) || _tdigest.ContainsKey(key) || _timeSeries.ContainsKey(key);
        }

        private bool HasNonTimeSeriesType(string key)
        {
            return _data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sets.ContainsKey(key) || _sortedSets.ContainsKey(key) || _vectors.ContainsKey(key) || _streams.ContainsKey(key) || _streamGroups.ContainsKey(key) || _bloom.ContainsKey(key) || _cuckoo.ContainsKey(key) || _tdigest.ContainsKey(key) || _topk.ContainsKey(key);
        }

        private static List<KeyValuePair<string, long>> TopKOrdered(TopKSketchModel topk)
        {
            return topk.Counts
                .OrderByDescending(x => x.Value)
                .ThenBy(x => x.Key, StringComparer.Ordinal)
                .Take(topk.K)
                .ToList();
        }

        private string? TopKApplyIncrement(TopKSketchModel topk, byte[] item, long increment)
        {
            var before = TopKOrdered(topk).Select(x => x.Key).ToHashSet(StringComparer.Ordinal);
            var encoded = Convert.ToBase64String(item);
            if (topk.Counts.TryGetValue(encoded, out var count))
            {
                topk.Counts[encoded] = count + increment;
            }
            else
            {
                topk.Counts[encoded] = increment;
            }

            var after = TopKOrdered(topk).Select(x => x.Key).ToHashSet(StringComparer.Ordinal);
            var dropped = before.FirstOrDefault(x => !after.Contains(x));
            return dropped == null ? null : Utf8.GetString(Convert.FromBase64String(dropped));
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

            return _data.ContainsKey(key) || _hashes.ContainsKey(key) || _lists.ContainsKey(key) || _sets.ContainsKey(key) || _sortedSets.ContainsKey(key) || _vectors.ContainsKey(key) || _streams.ContainsKey(key) || _bloom.ContainsKey(key) || _cuckoo.ContainsKey(key) || _tdigest.ContainsKey(key) || _topk.ContainsKey(key) || _timeSeries.ContainsKey(key);
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
            removed |= _vectors.Remove(key);
            removed |= _streams.Remove(key);
            removed |= _bloom.Remove(key);
            removed |= _cuckoo.Remove(key);
            removed |= _tdigest.Remove(key);
            removed |= _topk.Remove(key);
            removed |= _timeSeries.Remove(key);
            _timeSeriesRetention.Remove(key);
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
