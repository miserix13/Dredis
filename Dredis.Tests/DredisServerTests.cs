using System.Net;
using System.Net.Sockets;
using System.Text;
using Dredis.Abstractions.Storage;
using Xunit;

namespace Dredis.Tests
{
    /// <summary>
    /// Integration-style tests for the Dredis server.
    /// </summary>
    public sealed class DredisServerTests
    {
        private static readonly Encoding Utf8 = new UTF8Encoding(false);

        [Fact]
        /// <summary>
        /// Verifies PING round-trips and server shuts down cleanly.
        /// </summary>
        public async Task RunAsync_PingRespondsAndShutsDownOnCancel()
        {
            var store = new InMemoryKeyValueStore();
            var server = new DredisServer(store);
            var port = GetFreePort();

            var serverTask = server.RunAsync(port);

            using var client = await ConnectWithRetryAsync(port);
            await SendAsync(client, "*1\r\n$4\r\nPING\r\n");

            var response = await ReadLineAsync(client);
            Assert.Equal("+PONG", response);

            client.Close();
            await server.StopAsync();
            await serverTask;
        }

        [Fact]
        /// <summary>
        /// Verifies SET/GET round-trip over the server socket.
        /// </summary>
        public async Task RunAsync_SetGetRoundTrip()
        {
            var store = new InMemoryKeyValueStore();
            var server = new DredisServer(store);
            var port = GetFreePort();

            var serverTask = server.RunAsync(port);

            using var client = await ConnectWithRetryAsync(port);

            await SendAsync(client, "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
            var setResponse = await ReadLineAsync(client);
            Assert.Equal("+OK", setResponse);

            await SendAsync(client, "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
            var bulkHeader = await ReadLineAsync(client);
            Assert.Equal("$5", bulkHeader);

            var bulkValue = await ReadBulkStringAsync(client, 5);
            Assert.Equal("value", bulkValue);

            client.Close();
            await server.StopAsync();
            await serverTask;
        }

        /// <summary>
        /// Finds an available TCP port on localhost.
        /// </summary>
        private static int GetFreePort()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return port;
        }

        /// <summary>
        /// Connects to the server with retry until timeout.
        /// </summary>
        private static async Task<TcpClient> ConnectWithRetryAsync(int port)
        {
            var deadline = DateTimeOffset.UtcNow.AddSeconds(5);
            Exception? last = null;

            while (DateTimeOffset.UtcNow < deadline)
            {
                var client = new TcpClient();
                try
                {
                    await client.ConnectAsync(IPAddress.Loopback, port);
                    client.ReceiveTimeout = 2000;
                    return client;
                }
                catch (Exception ex)
                {
                    last = ex;
                    client.Dispose();
                    await Task.Delay(100);
                }
            }

            throw new InvalidOperationException("Failed to connect to server.", last);
        }

        /// <summary>
        /// Sends a raw RESP command to the server.
        /// </summary>
        private static async Task SendAsync(TcpClient client, string text)
        {
            var bytes = Utf8.GetBytes(text);
            await client.GetStream().WriteAsync(bytes, 0, bytes.Length);
        }

        /// <summary>
        /// Reads a RESP line terminated by CRLF.
        /// </summary>
        private static async Task<string> ReadLineAsync(TcpClient client)
        {
            var stream = client.GetStream();
            var buffer = new List<byte>();

            while (true)
            {
                var b = await ReadByteAsync(stream);
                if (b == -1)
                {
                    throw new InvalidOperationException("Connection closed before response.");
                }

                if (b == '\r')
                {
                    var next = await ReadByteAsync(stream);
                    if (next != '\n')
                    {
                        throw new InvalidOperationException("Invalid line ending in response.");
                    }

                    break;
                }

                buffer.Add((byte)b);
            }

            return Utf8.GetString(buffer.ToArray());
        }

        /// <summary>
        /// Reads a RESP bulk string with known length.
        /// </summary>
        private static async Task<string> ReadBulkStringAsync(TcpClient client, int length)
        {
            var stream = client.GetStream();
            var buffer = new byte[length];
            var offset = 0;

            while (offset < length)
            {
                var read = await stream.ReadAsync(buffer, offset, length - offset);
                if (read == 0)
                {
                    throw new InvalidOperationException("Connection closed before bulk string.");
                }

                offset += read;
            }

            var cr = await ReadByteAsync(stream);
            var lf = await ReadByteAsync(stream);
            if (cr != '\r' || lf != '\n')
            {
                throw new InvalidOperationException("Invalid bulk string terminator.");
            }

            return Utf8.GetString(buffer);
        }

        /// <summary>
        /// Reads a single byte from the network stream.
        /// </summary>
        private static async Task<int> ReadByteAsync(NetworkStream stream)
        {
            var buffer = new byte[1];
            var read = await stream.ReadAsync(buffer, 0, 1);
            return read == 0 ? -1 : buffer[0];
        }

    }
}
