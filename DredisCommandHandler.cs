using System.Text;
using DotNetty.Buffers;
using DotNetty.Codecs.Redis.Messages;
using DotNetty.Transport.Channels;

namespace Dredis
{
    /// <summary>
    /// Bridges Redis commands (via DotNetty codec) to the IKeyValueStore abstraction.
    /// </summary>
    public sealed class DredisCommandHandler : SimpleChannelInboundHandler<IRedisMessage>
    {
        private readonly IKeyValueStore _store;
        private static readonly Encoding Utf8 = new UTF8Encoding(false);

        public DredisCommandHandler(IKeyValueStore store)
        {
            _store = store;
        }

        protected override void ChannelRead0(IChannelHandlerContext ctx, IRedisMessage msg)
        {
            if (msg is IRedisArrayMessage array)
            {
                _ = HandleCommandAsync(ctx, array);
            }
            else
            {
                WriteError(ctx, "ERR protocol error: expected array");
            }
        }

        private async Task HandleCommandAsync(IChannelHandlerContext ctx, IRedisArrayMessage array)
        {
            var elements = array.Children;
            if (elements == null || elements.Count == 0)
            {
                WriteError(ctx, "ERR empty command");
                return;
            }

            var cmd = GetString(elements[0]).ToUpperInvariant();

            switch (cmd)
            {
                case "PING":
                    WriteSimpleString(ctx, "PONG");
                    break;

                case "GET":
                    await HandleGetAsync(ctx, elements);
                    break;

                case "SET":
                    await HandleSetAsync(ctx, elements);
                    break;

                case "DEL":
                    await HandleDelAsync(ctx, elements);
                    break;

                case "EXISTS":
                    await HandleExistsAsync(ctx, elements);
                    break;

                case "EXPIRE":
                    await HandleExpireAsync(ctx, elements);
                    break;

                case "TTL":
                    await HandleTtlAsync(ctx, elements);
                    break;

                default:
                    WriteError(ctx, $"ERR unknown command '{cmd}'");
                    break;
            }
        }

        private async Task HandleGetAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'get' command");
                return;
            }

            var key = GetString(args[1]);
            var value = await _store.GetAsync(key).ConfigureAwait(false);

            if (value == null)
            {
                WriteNullBulkString(ctx);
            }
            else
            {
                WriteBulkString(ctx, value);
            }
        }

        private async Task HandleSetAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count < 3)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'set' command");
                return;
            }

            var key = GetString(args[1]);
            var value = GetBytes(args[2]);

            // Optional: SET key value EX seconds
            TimeSpan? expiration = null;
            if (args.Count >= 5 &&
                GetString(args[3]).Equals("EX", StringComparison.OrdinalIgnoreCase))
            {
                if (!long.TryParse(GetString(args[4]), out var seconds) || seconds <= 0)
                {
                    WriteError(ctx, "ERR invalid expire time in set");
                    return;
                }

                expiration = TimeSpan.FromSeconds(seconds);
            }

            var ok = await _store.SetAsync(key, value, expiration).ConfigureAwait(false);
            if (ok)
            {
                WriteSimpleString(ctx, "OK");
            }
            else
            {
                WriteError(ctx, "ERR set failed");
            }
        }

        private async Task HandleDelAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count < 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'del' command");
                return;
            }

            var keys = new string[args.Count - 1];
            for (int i = 1; i < args.Count; i++)
            {
                keys[i - 1] = GetString(args[i]);
            }

            var removed = await _store.DeleteAsync(keys).ConfigureAwait(false);
            WriteInteger(ctx, removed);
        }

        private async Task HandleExistsAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'exists' command");
                return;
            }

            var key = GetString(args[1]);
            var exists = await _store.ExistsAsync(key).ConfigureAwait(false);
            WriteInteger(ctx, exists ? 1 : 0);
        }

        private async Task HandleExpireAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 3)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'expire' command");
                return;
            }

            var key = GetString(args[1]);
            if (!long.TryParse(GetString(args[2]), out var seconds))
            {
                WriteError(ctx, "ERR value is not an integer or out of range");
                return;
            }

            var ok = await _store.ExpireAsync(key, TimeSpan.FromSeconds(seconds))
                .ConfigureAwait(false);

            WriteInteger(ctx, ok ? 1 : 0);
        }

        private async Task HandleTtlAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'ttl' command");
                return;
            }

            var key = GetString(args[1]);
            var ttl = await _store.TtlAsync(key).ConfigureAwait(false);
            WriteInteger(ctx, ttl);
        }

        private static string GetString(IRedisMessage msg)
        {
            switch (msg)
            {
                case IRedisBulkStringMessage bulk when bulk.Content != null:
                    return Utf8.GetString(
                        bulk.Content.Array!,
                        bulk.Content.ArrayOffset,
                        bulk.Content.Count);

                case IRedisSimpleStringMessage simple:
                    return simple.Content;

                default:
                    return string.Empty;
            }
        }

        private static byte[] GetBytes(IRedisMessage msg)
        {
            switch (msg)
            {
                case IRedisBulkStringMessage bulk when bulk.Content != null:
                    var buffer = new byte[bulk.Content.Count];
                    Buffer.BlockCopy(
                        bulk.Content.Array!,
                        bulk.Content.ArrayOffset,
                        buffer,
                        0,
                        bulk.Content.Count);
                    return buffer;

                case IRedisSimpleStringMessage simple:
                    return Utf8.GetBytes(simple.Content);

                default:
                    return Array.Empty<byte>();
            }
        }

        private static void WriteSimpleString(IChannelHandlerContext ctx, string text) =>
            ctx.WriteAndFlushAsync(new SimpleStringRedisMessage(text));

        private static void WriteError(IChannelHandlerContext ctx, string error) =>
            ctx.WriteAndFlushAsync(new ErrorRedisMessage(error));

        private static void WriteInteger(IChannelHandlerContext ctx, long value) =>
            ctx.WriteAndFlushAsync(new IntegerRedisMessage(value));

        private static void WriteNullBulkString(IChannelHandlerContext ctx) =>
            ctx.WriteAndFlushAsync(FullBulkStringRedisMessage.Null);

        private static void WriteBulkString(IChannelHandlerContext ctx, byte[] data) =>
            ctx.WriteAndFlushAsync(new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(data)));
    }
}