using System.Net;
using System.Net.Sockets;
using Dredis.Abstractions.Storage;
using StackExchange.Redis;
using Take.Elephant.Redis;
using Take.Elephant.Redis.Serializers;
using Xunit;

namespace Dredis.Tests
{
    /// <summary>
    /// Compatibility tests using the Take.Elephant Redis provider.
    /// </summary>
    public sealed class DredisTakeElephantCompatibilityTests
    {
        [Fact]
        /// <summary>
        /// Verifies a basic Take.Elephant Redis string map round-trips through Dredis.
        /// </summary>
        public async Task TakeElephantRedisStringMap_BasicRoundTrip()
        {
            var store = new InMemoryKeyValueStore();
            var server = new DredisServer(store);
            var port = GetFreePort();

            var serverTask = server.RunAsync(port);
            ConnectionMultiplexer? multiplexer = null;

            try
            {
                multiplexer = await ConnectAsync(port);
                var map = new RedisStringMap<string, int>(
                    "elephant:compat",
                    multiplexer,
                    new ValueSerializer<int>());

                var added = await map.TryAddAsync("map:key", 42);
                Assert.True(added);

                var exists = await map.ContainsKeyAsync("map:key");
                Assert.True(exists);

                var value = await map.GetValueOrDefaultAsync("map:key");
                Assert.Equal(42, value);

                var removed = await map.TryRemoveAsync("map:key");
                Assert.True(removed);

                var existsAfterRemove = await map.ContainsKeyAsync("map:key");
                Assert.False(existsAfterRemove);
            }
            finally
            {
                if (multiplexer is not null)
                {
                    await multiplexer.CloseAsync();
                    multiplexer.Dispose();
                }

                await server.StopAsync();
                await serverTask;
            }
        }

        private static int GetFreePort()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return port;
        }

        private static async Task<ConnectionMultiplexer> ConnectAsync(int port)
        {
            var options = new ConfigurationOptions
            {
                AbortOnConnectFail = false,
                ConnectRetry = 5,
                ConnectTimeout = 2000,
                Protocol = RedisProtocol.Resp2,
                SyncTimeout = 2000
            };

            options.EndPoints.Add(IPAddress.Loopback, port);
            return await ConnectionMultiplexer.ConnectAsync(options);
        }
    }
}
