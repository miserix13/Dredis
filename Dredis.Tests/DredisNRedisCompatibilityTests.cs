using System.Net;
using System.Net.Sockets;
using Dredis.Abstractions.Storage;
using StackExchange.Redis;
using Xunit;

namespace Dredis.Tests
{
    /// <summary>
    /// Compatibility tests using an NRedis-family client (NRedisStack + StackExchange.Redis).
    /// </summary>
    public sealed class DredisNRedisCompatibilityTests
    {
        [Fact(Skip = "Known compatibility gap: StackExchange.Redis/NRedisStack handshake is not yet fully supported")]
        /// <summary>
        /// Verifies basic string and counter commands work through the client connection.
        /// </summary>
        public async Task NRedisClient_BasicRedisCommands_RoundTrip()
        {
            var store = new InMemoryKeyValueStore();
            var server = new DredisServer(store);
            var port = GetFreePort();

            var serverTask = server.RunAsync(port);
            ConnectionMultiplexer? multiplexer = null;

            try
            {
                multiplexer = await ConnectAsync(port);
                var db = multiplexer.GetDatabase();

                var ping = await db.PingAsync();
                Assert.True(ping >= TimeSpan.Zero);

                var set = await db.StringSetAsync("compat:key", "value");
                Assert.True(set);

                var get = await db.StringGetAsync("compat:key");
                Assert.Equal("value", get.ToString());

                var incremented = await db.StringIncrementAsync("compat:counter", 5);
                Assert.Equal(5, incremented);

                var exists = await db.KeyExistsAsync("compat:key");
                Assert.True(exists);
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

            options.CommandMap = CommandMap.Create(new HashSet<string> { "ECHO" }, available: false);

            options.EndPoints.Add(IPAddress.Loopback, port);
            return await ConnectionMultiplexer.ConnectAsync(options);
        }
    }
}
