using System.Text;
using DotNetty.Buffers;
using DotNetty.Codecs.Redis.Messages;
using DotNetty.Transport.Channels.Embedded;
using Xunit;

namespace Dredis.Tests
{
    public sealed class DredisCommandHandlerTests
    {
        private static readonly Encoding Utf8 = new UTF8Encoding(false);

        [Fact]
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

        private static IRedisMessage ReadOutbound(EmbeddedChannel channel)
        {
            channel.RunPendingTasks();
            channel.RunScheduledPendingTasks();
            return channel.ReadOutbound<IRedisMessage>();
        }

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

        private static string GetBulkString(FullBulkStringRedisMessage message)
        {
            var length = message.Content.ReadableBytes;
            var buffer = new byte[length];
            message.Content.GetBytes(message.Content.ReaderIndex, buffer, 0, length);
            return Utf8.GetString(buffer, 0, length);
        }
    }

    internal sealed class InMemoryKeyValueStore : IKeyValueStore
    {
        private readonly Dictionary<string, byte[]> _data = new(StringComparer.Ordinal);
        private readonly Dictionary<string, DateTimeOffset?> _expirations = new(StringComparer.Ordinal);
        private static readonly Encoding Utf8 = new UTF8Encoding(false);

        public void Seed(string key, string value)
        {
            _data[key] = Utf8.GetBytes(value);
            _expirations.Remove(key);
        }

        public Task<byte[]?> GetAsync(string key, CancellationToken token = default)
        {
            return Task.FromResult(GetValue(key));
        }

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

        public Task<byte[]?[]> GetManyAsync(string[] keys, CancellationToken token = default)
        {
            var result = new byte[]?[keys.Length];
            for (int i = 0; i < keys.Length; i++)
            {
                result[i] = GetValue(keys[i]);
            }

            return Task.FromResult(result);
        }

        public Task<bool> SetManyAsync(KeyValuePair<string, byte[]>[] items, CancellationToken token = default)
        {
            foreach (var item in items)
            {
                _data[item.Key] = item.Value;
                _expirations.Remove(item.Key);
            }

            return Task.FromResult(true);
        }

        public Task<long> DeleteAsync(string[] keys, CancellationToken token = default)
        {
            long removed = 0;
            foreach (var key in keys)
            {
                if (_data.Remove(key))
                {
                    removed++;
                    _expirations.Remove(key);
                }
            }

            return Task.FromResult(removed);
        }

        public Task<bool> ExistsAsync(string key, CancellationToken token = default)
        {
            var exists = GetValue(key) != null;
            return Task.FromResult(exists);
        }

        public Task<long> ExistsAsync(string[] keys, CancellationToken token = default)
        {
            long count = 0;
            foreach (var key in keys)
            {
                if (GetValue(key) != null)
                {
                    count++;
                }
            }

            return Task.FromResult(count);
        }

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

        public Task<bool> ExpireAsync(string key, TimeSpan expiration, CancellationToken token = default)
        {
            if (GetValue(key) == null)
            {
                return Task.FromResult(false);
            }

            _expirations[key] = DateTimeOffset.UtcNow.Add(expiration);
            return Task.FromResult(true);
        }

        public Task<bool> PExpireAsync(string key, TimeSpan expiration, CancellationToken token = default)
        {
            if (GetValue(key) == null)
            {
                return Task.FromResult(false);
            }

            _expirations[key] = DateTimeOffset.UtcNow.Add(expiration);
            return Task.FromResult(true);
        }

        public Task<long> TtlAsync(string key, CancellationToken token = default)
        {
            if (GetValue(key) == null)
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

        public Task<long> PttlAsync(string key, CancellationToken token = default)
        {
            if (GetValue(key) == null)
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
                if (_data.Remove(key))
                {
                    removed++;
                }

                _expirations.Remove(key);
            }

            return Task.FromResult(removed);
        }

        private byte[]? GetValue(string key)
        {
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

            _data.Remove(key);
            _expirations.Remove(key);
            return null;
        }
    }
}
