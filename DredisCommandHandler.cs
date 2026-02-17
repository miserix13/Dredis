using System.Collections.Generic;
using System.Text;
using DotNetty.Buffers;
using DotNetty.Codecs.Redis.Messages;
using DotNetty.Transport.Channels;
using Dredis.Abstractions.Storage;

namespace Dredis
{
    /// <summary>
    /// Bridges Redis commands (via DotNetty codec) to the IKeyValueStore abstraction.
    /// </summary>
    public sealed class DredisCommandHandler : SimpleChannelInboundHandler<IRedisMessage>
    {
        private readonly IKeyValueStore _store;
        private static readonly Encoding Utf8 = new UTF8Encoding(false);

        /// <summary>
        /// Initializes a new instance of the <see cref="DredisCommandHandler"/> class.
        /// </summary>
        /// <param name="store">The storage abstraction used for command execution.</param>
        public DredisCommandHandler(IKeyValueStore store)
        {
            _store = store;
        }

        /// <summary>
        /// Handles incoming Redis messages from the channel and dispatches commands to appropriate handlers.
        /// </summary>
        /// <param name="ctx">The channel handler context.</param>
        /// <param name="msg">The Redis message to process.</param>
        protected override void ChannelRead0(IChannelHandlerContext ctx, IRedisMessage msg)
        {
            if (msg is IArrayRedisMessage array)
            {
                _ = HandleCommandAsync(ctx, array);
            }
            else
            {
                WriteError(ctx, "ERR protocol error: expected array");
            }
        }

        /// <summary>
        /// Parses the command array and routes it to the appropriate handler.
        /// </summary>
        /// <param name="ctx">The channel handler context.</param>
        /// <param name="array">The command array message.</param>
        private async Task HandleCommandAsync(IChannelHandlerContext ctx, IArrayRedisMessage array)
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
                    await HandlePingAsync(ctx, elements);
                    break;

                case "ECHO":
                    await HandleEchoAsync(ctx, elements);
                    break;

                case "GET":
                    await HandleGetAsync(ctx, elements);
                    break;

                case "SET":
                    await HandleSetAsync(ctx, elements);
                    break;

                case "MGET":
                    await HandleMGetAsync(ctx, elements);
                    break;

                case "MSET":
                    await HandleMSetAsync(ctx, elements);
                    break;

                case "DEL":
                    await HandleDelAsync(ctx, elements);
                    break;

                case "EXISTS":
                    await HandleExistsAsync(ctx, elements);
                    break;

                case "INCR":
                    await HandleIncrByAsync(ctx, elements, 1, commandName: "incr");
                    break;

                case "INCRBY":
                    await HandleIncrByAsync(ctx, elements, null, commandName: "incrby");
                    break;

                case "DECR":
                    await HandleIncrByAsync(ctx, elements, -1, commandName: "decr");
                    break;

                case "DECRBY":
                    await HandleIncrByAsync(ctx, elements, null, isDecr: true, commandName: "decrby");
                    break;

                case "EXPIRE":
                    await HandleExpireAsync(ctx, elements);
                    break;

                case "PEXPIRE":
                    await HandlePExpireAsync(ctx, elements);
                    break;

                case "TTL":
                    await HandleTtlAsync(ctx, elements);
                    break;

                case "PTTL":
                    await HandlePttlAsync(ctx, elements);
                    break;

                case "HSET":
                    await HandleHSetAsync(ctx, elements);
                    break;

                case "HGET":
                    await HandleHGetAsync(ctx, elements);
                    break;

                case "HDEL":
                    await HandleHDelAsync(ctx, elements);
                    break;

                case "HGETALL":
                    await HandleHGetAllAsync(ctx, elements);
                    break;

                case "XADD":
                    await HandleXAddAsync(ctx, elements);
                    break;

                case "XDEL":
                    await HandleXDelAsync(ctx, elements);
                    break;

                case "XLEN":
                    await HandleXLenAsync(ctx, elements);
                    break;

                case "XREAD":
                    await HandleXReadAsync(ctx, elements);
                    break;

                case "XRANGE":
                    await HandleXRangeAsync(ctx, elements);
                    break;

                case "XGROUP":
                    await HandleXGroupAsync(ctx, elements);
                    break;

                case "XREADGROUP":
                    await HandleXReadGroupAsync(ctx, elements);
                    break;

                case "XACK":
                    await HandleXAckAsync(ctx, elements);
                    break;

                case "XPENDING":
                    await HandleXPendingAsync(ctx, elements);
                    break;

                case "XCLAIM":
                    await HandleXClaimAsync(ctx, elements);
                    break;

                default:
                    WriteError(ctx, $"ERR unknown command '{cmd}'");
                    break;
            }
        }

        /// <summary>
        /// Handles the PING command.
        /// </summary>
        private Task HandlePingAsync(IChannelHandlerContext ctx, IList<IRedisMessage> args)
        {
            if (args.Count == 1)
            {
                WriteSimpleString(ctx, "PONG");
                return Task.CompletedTask;
            }

            if (args.Count != 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'ping' command");
                return Task.CompletedTask;
            }

            if (!TryGetBytes(args[1], out var value))
            {
                WriteError(ctx, "ERR null bulk string");
                return Task.CompletedTask;
            }

            WriteBulkString(ctx, value);
            return Task.CompletedTask;
        }

        /// <summary>
        /// Handles the ECHO command.
        /// </summary>
        private Task HandleEchoAsync(IChannelHandlerContext ctx, IList<IRedisMessage> args)
        {
            if (args.Count != 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'echo' command");
                return Task.CompletedTask;
            }

            if (!TryGetBytes(args[1], out var value))
            {
                WriteError(ctx, "ERR null bulk string");
                return Task.CompletedTask;
            }

            WriteBulkString(ctx, value);
            return Task.CompletedTask;
        }

        /// <summary>
        /// Handles the GET command.
        /// </summary>
        private async Task HandleGetAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'get' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }
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

        /// <summary>
        /// Handles the SET command with optional NX/XX and expiration options.
        /// </summary>
        private async Task HandleSetAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count < 3)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'set' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            if (!TryGetBytes(args[2], out var value))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var expiration = (TimeSpan?)null;
            var condition = SetCondition.None;

            for (int i = 3; i < args.Count; i++)
            {
                if (!TryGetString(args[i], out var option))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }

                switch (option.ToUpperInvariant())
                {
                    case "EX":
                        if (i + 1 >= args.Count)
                        {
                            WriteError(ctx, "ERR syntax error");
                            return;
                        }

                        if (!TryGetString(args[++i], out var exValue) ||
                            !long.TryParse(exValue, out var seconds) || seconds <= 0)
                        {
                            WriteError(ctx, "ERR invalid expire time in set");
                            return;
                        }

                        expiration = TimeSpan.FromSeconds(seconds);
                        break;

                    case "PX":
                        if (i + 1 >= args.Count)
                        {
                            WriteError(ctx, "ERR syntax error");
                            return;
                        }

                        if (!TryGetString(args[++i], out var pxValue) ||
                            !long.TryParse(pxValue, out var milliseconds) || milliseconds <= 0)
                        {
                            WriteError(ctx, "ERR invalid expire time in set");
                            return;
                        }

                        expiration = TimeSpan.FromMilliseconds(milliseconds);
                        break;

                    case "NX":
                        if (condition == SetCondition.Xx)
                        {
                            WriteError(ctx, "ERR syntax error");
                            return;
                        }

                        condition = SetCondition.Nx;
                        break;

                    case "XX":
                        if (condition == SetCondition.Nx)
                        {
                            WriteError(ctx, "ERR syntax error");
                            return;
                        }

                        condition = SetCondition.Xx;
                        break;

                    default:
                        WriteError(ctx, "ERR syntax error");
                        return;
                }
            }

            var ok = await _store.SetAsync(key, value, expiration, condition).ConfigureAwait(false);
            if (ok)
            {
                WriteSimpleString(ctx, "OK");
                return;
            }

            WriteNullBulkString(ctx);
        }

        /// <summary>
        /// Handles the MGET command.
        /// </summary>
        private async Task HandleMGetAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count < 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'mget' command");
                return;
            }

            var keys = new string[args.Count - 1];
            for (int i = 1; i < args.Count; i++)
            {
                if (!TryGetString(args[i], out var key))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }

                keys[i - 1] = key;
            }

            var values = await _store.GetManyAsync(keys).ConfigureAwait(false);
            var children = new IRedisMessage[values.Length];

            for (int i = 0; i < values.Length; i++)
            {
                var value = values[i];
                children[i] = value == null
                    ? FullBulkStringRedisMessage.Null
                    : new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(value));
            }

            WriteArray(ctx, children);
        }

        /// <summary>
        /// Handles the MSET command.
        /// </summary>
        private async Task HandleMSetAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count < 3 || args.Count % 2 == 0)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'mset' command");
                return;
            }

            var items = new KeyValuePair<string, byte[]>[(args.Count - 1) / 2];
            int index = 0;
            for (int i = 1; i < args.Count; i += 2)
            {
                if (!TryGetString(args[i], out var key))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }

                if (!TryGetBytes(args[i + 1], out var value))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }

                items[index++] = new KeyValuePair<string, byte[]>(key, value);
            }

            var ok = await _store.SetManyAsync(items).ConfigureAwait(false);
            if (ok)
            {
                WriteSimpleString(ctx, "OK");
            }
            else
            {
                WriteError(ctx, "ERR mset failed");
            }
        }

        /// <summary>
        /// Handles the DEL command.
        /// </summary>
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
                if (!TryGetString(args[i], out var key))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }

                keys[i - 1] = key;
            }

            var removed = await _store.DeleteAsync(keys).ConfigureAwait(false);
            WriteInteger(ctx, removed);
        }

        /// <summary>
        /// Handles the EXISTS command.
        /// </summary>
        private async Task HandleExistsAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count < 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'exists' command");
                return;
            }

            if (args.Count == 2)
            {
                if (!TryGetString(args[1], out var key))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }

                var exists = await _store.ExistsAsync(key).ConfigureAwait(false);
                WriteInteger(ctx, exists ? 1 : 0);
                return;
            }

            var keys = new string[args.Count - 1];
            for (int i = 1; i < args.Count; i++)
            {
                if (!TryGetString(args[i], out var key))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }

                keys[i - 1] = key;
            }

            var count = await _store.ExistsAsync(keys).ConfigureAwait(false);
            WriteInteger(ctx, count);
        }

        /// <summary>
        /// Handles INCR/INCRBY/DECR/DECRBY commands.
        /// </summary>
        private async Task HandleIncrByAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args,
            long? fixedDelta,
            bool isDecr = false,
            string commandName = "incrby")
        {
            if (fixedDelta.HasValue && args.Count != 2)
            {
                WriteError(ctx, $"ERR wrong number of arguments for '{commandName}' command");
                return;
            }

            if (!fixedDelta.HasValue && args.Count != 3)
            {
                WriteError(ctx, $"ERR wrong number of arguments for '{commandName}' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            long delta;
            if (fixedDelta.HasValue)
            {
                delta = fixedDelta.Value;
            }
            else
            {
                if (!TryGetString(args[2], out var deltaText) ||
                    !long.TryParse(deltaText, out delta))
                {
                    WriteError(ctx, "ERR value is not an integer or out of range");
                    return;
                }

                if (isDecr)
                {
                    delta = -delta;
                }
            }

            var value = await _store.IncrByAsync(key, delta).ConfigureAwait(false);
            if (!value.HasValue)
            {
                WriteError(ctx, "ERR value is not an integer or out of range");
                return;
            }

            WriteInteger(ctx, value.Value);
        }

        /// <summary>
        /// Handles the EXPIRE command.
        /// </summary>
        private async Task HandleExpireAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 3)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'expire' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            if (!TryGetString(args[2], out var secondsText) ||
                !long.TryParse(secondsText, out var seconds))
            {
                WriteError(ctx, "ERR value is not an integer or out of range");
                return;
            }

            var ok = await _store.ExpireAsync(key, TimeSpan.FromSeconds(seconds))
                .ConfigureAwait(false);

            WriteInteger(ctx, ok ? 1 : 0);
        }

        /// <summary>
        /// Handles the PEXPIRE command.
        /// </summary>
        private async Task HandlePExpireAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 3)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'pexpire' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            if (!TryGetString(args[2], out var msText) ||
                !long.TryParse(msText, out var milliseconds))
            {
                WriteError(ctx, "ERR value is not an integer or out of range");
                return;
            }

            var ok = await _store.PExpireAsync(key, TimeSpan.FromMilliseconds(milliseconds))
                .ConfigureAwait(false);

            WriteInteger(ctx, ok ? 1 : 0);
        }

        /// <summary>
        /// Handles the TTL command.
        /// </summary>
        private async Task HandleTtlAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'ttl' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var ttl = await _store.TtlAsync(key).ConfigureAwait(false);
            WriteInteger(ctx, ttl);
        }

        /// <summary>
        /// Handles the PTTL command.
        /// </summary>
        private async Task HandlePttlAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'pttl' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var ttl = await _store.PttlAsync(key).ConfigureAwait(false);
            WriteInteger(ctx, ttl);
        }

        /// <summary>
        /// Handles the HSET command.
        /// </summary>
        private async Task HandleHSetAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count < 4 || (args.Count - 2) % 2 != 0)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'hset' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            long added = 0;
            for (int i = 2; i < args.Count; i += 2)
            {
                if (!TryGetString(args[i], out var field))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }

                if (!TryGetBytes(args[i + 1], out var value))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }

                var isNew = await _store.HashSetAsync(key, field, value).ConfigureAwait(false);
                if (isNew)
                {
                    added++;
                }
            }

            WriteInteger(ctx, added);
        }

        /// <summary>
        /// Handles the HGET command.
        /// </summary>
        private async Task HandleHGetAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 3)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'hget' command");
                return;
            }

            if (!TryGetString(args[1], out var key) || !TryGetString(args[2], out var field))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var value = await _store.HashGetAsync(key, field).ConfigureAwait(false);
            if (value == null)
            {
                WriteNullBulkString(ctx);
                return;
            }

            WriteBulkString(ctx, value);
        }

        /// <summary>
        /// Handles the HDEL command.
        /// </summary>
        private async Task HandleHDelAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count < 3)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'hdel' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var fields = new string[args.Count - 2];
            for (int i = 2; i < args.Count; i++)
            {
                if (!TryGetString(args[i], out var field))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }

                fields[i - 2] = field;
            }

            var removed = await _store.HashDeleteAsync(key, fields).ConfigureAwait(false);
            WriteInteger(ctx, removed);
        }

        /// <summary>
        /// Handles the HGETALL command.
        /// </summary>
        private async Task HandleHGetAllAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'hgetall' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var entries = await _store.HashGetAllAsync(key).ConfigureAwait(false);
            var children = new IRedisMessage[entries.Length * 2];

            for (int i = 0; i < entries.Length; i++)
            {
                var fieldBytes = Utf8.GetBytes(entries[i].Key);
                children[i * 2] = new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(fieldBytes));
                children[i * 2 + 1] = new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(entries[i].Value));
            }

            WriteArray(ctx, children);
        }

        /// <summary>
        /// Handles the XADD command.
        /// </summary>
        private async Task HandleXAddAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count < 5 || (args.Count - 3) % 2 != 0)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'xadd' command");
                return;
            }

            if (!TryGetString(args[1], out var key) || !TryGetString(args[2], out var id))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            if (id != "*" && !TryParseStreamIdText(id))
            {
                WriteError(ctx, "ERR invalid stream id");
                return;
            }

            var fields = new KeyValuePair<string, byte[]>[(args.Count - 3) / 2];
            int index = 0;
            for (int i = 3; i < args.Count; i += 2)
            {
                if (!TryGetString(args[i], out var field))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }

                if (!TryGetBytes(args[i + 1], out var value))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }

                fields[index++] = new KeyValuePair<string, byte[]>(field, value);
            }

            var createdId = await _store.StreamAddAsync(key, id, fields).ConfigureAwait(false);
            if (createdId == null)
            {
                WriteError(ctx, "ERR invalid stream id");
                return;
            }

            WriteBulkString(ctx, Utf8.GetBytes(createdId));
        }

        /// <summary>
        /// Handles the XDEL command.
        /// </summary>
        private async Task HandleXDelAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count < 3)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'xdel' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var ids = new string[args.Count - 2];
            for (int i = 2; i < args.Count; i++)
            {
                if (!TryGetString(args[i], out var id))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }

                ids[i - 2] = id;
            }

            var removed = await _store.StreamDeleteAsync(key, ids).ConfigureAwait(false);
            WriteInteger(ctx, removed);
        }

        /// <summary>
        /// Handles the XLEN command.
        /// </summary>
        private async Task HandleXLenAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'xlen' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var length = await _store.StreamLengthAsync(key).ConfigureAwait(false);
            WriteInteger(ctx, length);
        }

        /// <summary>
        /// Handles the XREAD command.
        /// </summary>
        private async Task HandleXReadAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count < 4)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'xread' command");
                return;
            }

            int index = 1;
            int? count = null;

            while (index < args.Count)
            {
                if (!TryGetString(args[index], out var option))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }

                if (string.Equals(option, "STREAMS", StringComparison.OrdinalIgnoreCase))
                {
                    break;
                }

                if (string.Equals(option, "COUNT", StringComparison.OrdinalIgnoreCase))
                {
                    if (index + 1 >= args.Count || !TryGetString(args[index + 1], out var countText) ||
                        !int.TryParse(countText, out var parsed) || parsed <= 0)
                    {
                        WriteError(ctx, "ERR invalid count");
                        return;
                    }

                    count = parsed;
                    index += 2;
                    continue;
                }

                if (string.Equals(option, "BLOCK", StringComparison.OrdinalIgnoreCase))
                {
                    WriteError(ctx, "ERR BLOCK not supported");
                    return;
                }

                WriteError(ctx, "ERR syntax error");
                return;
            }

            if (index >= args.Count || !TryGetString(args[index], out var streamsKeyword) ||
                !string.Equals(streamsKeyword, "STREAMS", StringComparison.OrdinalIgnoreCase))
            {
                WriteError(ctx, "ERR syntax error");
                return;
            }

            int remaining = args.Count - (index + 1);
            if (remaining < 2 || remaining % 2 != 0)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'xread' command");
                return;
            }

            int streamCount = remaining / 2;
            var keys = new string[streamCount];
            var ids = new string[streamCount];

            for (int i = 0; i < streamCount; i++)
            {
                if (!TryGetString(args[index + 1 + i], out var key) ||
                    !TryGetString(args[index + 1 + i + streamCount], out var id))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }

                if (id != "$" && !TryParseStreamIdText(id))
                {
                    WriteError(ctx, "ERR invalid stream id");
                    return;
                }

                keys[i] = key;
                ids[i] = id;
            }

            var results = await _store.StreamReadAsync(keys, ids, count).ConfigureAwait(false);
            var streamMessages = new List<IRedisMessage>();

            foreach (var result in results)
            {
                if (result.Entries.Length == 0)
                {
                    continue;
                }

                var entryMessages = new IRedisMessage[result.Entries.Length];
                for (int i = 0; i < result.Entries.Length; i++)
                {
                    var entry = result.Entries[i];
                    var fieldChildren = new IRedisMessage[entry.Fields.Length * 2];

                    for (int j = 0; j < entry.Fields.Length; j++)
                    {
                        var fieldBytes = Utf8.GetBytes(entry.Fields[j].Key);
                        fieldChildren[j * 2] = new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(fieldBytes));
                        fieldChildren[j * 2 + 1] = new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(entry.Fields[j].Value));
                    }

                    var entryChildren = new IRedisMessage[2]
                    {
                        new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(Utf8.GetBytes(entry.Id))),
                        new ArrayRedisMessage(fieldChildren)
                    };

                    entryMessages[i] = new ArrayRedisMessage(entryChildren);
                }

                var streamChildren = new IRedisMessage[2]
                {
                    new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(Utf8.GetBytes(result.Key))),
                    new ArrayRedisMessage(entryMessages)
                };

                streamMessages.Add(new ArrayRedisMessage(streamChildren));
            }

            WriteArray(ctx, streamMessages);
        }

        /// <summary>
        /// Handles the XRANGE command.
        /// </summary>
        private async Task HandleXRangeAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 4 && args.Count != 6)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'xrange' command");
                return;
            }

            if (!TryGetString(args[1], out var key) ||
                !TryGetString(args[2], out var start) ||
                !TryGetString(args[3], out var end))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            if (!IsRangeId(start) || !IsRangeId(end))
            {
                WriteError(ctx, "ERR invalid stream id");
                return;
            }

            int? count = null;
            if (args.Count == 6)
            {
                if (!TryGetString(args[4], out var countOption) ||
                    !string.Equals(countOption, "COUNT", StringComparison.OrdinalIgnoreCase) ||
                    !TryGetString(args[5], out var countText) ||
                    !int.TryParse(countText, out var parsed) || parsed <= 0)
                {
                    WriteError(ctx, "ERR syntax error");
                    return;
                }

                count = parsed;
            }

            var entries = await _store.StreamRangeAsync(key, start, end, count).ConfigureAwait(false);
            var entryMessages = new IRedisMessage[entries.Length];

            for (int i = 0; i < entries.Length; i++)
            {
                var entry = entries[i];
                var fieldChildren = new IRedisMessage[entry.Fields.Length * 2];

                for (int j = 0; j < entry.Fields.Length; j++)
                {
                    var fieldBytes = Utf8.GetBytes(entry.Fields[j].Key);
                    fieldChildren[j * 2] = new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(fieldBytes));
                    fieldChildren[j * 2 + 1] = new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(entry.Fields[j].Value));
                }

                var entryChildren = new IRedisMessage[2]
                {
                    new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(Utf8.GetBytes(entry.Id))),
                    new ArrayRedisMessage(fieldChildren)
                };

                entryMessages[i] = new ArrayRedisMessage(entryChildren);
            }

            WriteArray(ctx, entryMessages);
        }

        /// <summary>
        /// Handles the XGROUP command and dispatches subcommands.
        /// </summary>
        private async Task HandleXGroupAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count < 2 || !TryGetString(args[1], out var subcommand))
            {
                WriteError(ctx, "ERR wrong number of arguments for 'xgroup' command");
                return;
            }

            switch (subcommand.ToUpperInvariant())
            {
                case "CREATE":
                    await HandleXGroupCreateAsync(ctx, args);
                    break;

                case "DESTROY":
                    await HandleXGroupDestroyAsync(ctx, args);
                    break;

                default:
                    WriteError(ctx, "ERR unknown subcommand or wrong number of arguments for 'xgroup' command");
                    break;
            }
        }

        /// <summary>
        /// Handles the XGROUP CREATE subcommand.
        /// </summary>
        private async Task HandleXGroupCreateAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 5 && args.Count != 6)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'xgroup create' command");
                return;
            }

            if (!TryGetString(args[2], out var key) ||
                !TryGetString(args[3], out var group) ||
                !TryGetString(args[4], out var id))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            bool mkStream = false;
            if (args.Count == 6)
            {
                if (!TryGetString(args[5], out var option) ||
                    !string.Equals(option, "MKSTREAM", StringComparison.OrdinalIgnoreCase))
                {
                    WriteError(ctx, "ERR syntax error");
                    return;
                }

                mkStream = true;
            }

            if (!IsGroupCreateId(id))
            {
                WriteError(ctx, "ERR invalid stream id");
                return;
            }

            var result = await _store.StreamGroupCreateAsync(key, group, id, mkStream).ConfigureAwait(false);
            switch (result)
            {
                case StreamGroupCreateResult.Ok:
                    WriteSimpleString(ctx, "OK");
                    break;

                case StreamGroupCreateResult.Exists:
                    WriteError(ctx, "BUSYGROUP Consumer Group name already exists");
                    break;

                case StreamGroupCreateResult.NoStream:
                    WriteError(ctx, "ERR The XGROUP subcommand requires the key to exist");
                    break;

                case StreamGroupCreateResult.WrongType:
                    WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                    break;

                default:
                    WriteError(ctx, "ERR invalid stream id");
                    break;
            }
        }

        /// <summary>
        /// Handles the XGROUP DESTROY subcommand.
        /// </summary>
        private async Task HandleXGroupDestroyAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 4)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'xgroup destroy' command");
                return;
            }

            if (!TryGetString(args[2], out var key) || !TryGetString(args[3], out var group))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var result = await _store.StreamGroupDestroyAsync(key, group).ConfigureAwait(false);
            if (result == StreamGroupDestroyResult.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            WriteInteger(ctx, result == StreamGroupDestroyResult.Removed ? 1 : 0);
        }

        /// <summary>
        /// Handles the XREADGROUP command.
        /// </summary>
        private async Task HandleXReadGroupAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count < 6)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'xreadgroup' command");
                return;
            }

            int index = 1;
            int? count = null;
            TimeSpan? block = null;
            string? group = null;
            string? consumer = null;

            while (index < args.Count)
            {
                if (!TryGetString(args[index], out var token))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }

                if (string.Equals(token, "GROUP", StringComparison.OrdinalIgnoreCase))
                {
                    if (index + 2 >= args.Count ||
                        !TryGetString(args[index + 1], out var groupName) ||
                        !TryGetString(args[index + 2], out var consumerName))
                    {
                        WriteError(ctx, "ERR syntax error");
                        return;
                    }

                    group = groupName;
                    consumer = consumerName;
                    index += 3;
                    continue;
                }

                if (string.Equals(token, "COUNT", StringComparison.OrdinalIgnoreCase))
                {
                    if (index + 1 >= args.Count ||
                        !TryGetString(args[index + 1], out var countText) ||
                        !int.TryParse(countText, out var parsed) || parsed <= 0)
                    {
                        WriteError(ctx, "ERR invalid count");
                        return;
                    }

                    count = parsed;
                    index += 2;
                    continue;
                }

                if (string.Equals(token, "BLOCK", StringComparison.OrdinalIgnoreCase))
                {
                    if (index + 1 >= args.Count ||
                        !TryGetString(args[index + 1], out var blockText) ||
                        !long.TryParse(blockText, out var ms) || ms < 0)
                    {
                        WriteError(ctx, "ERR invalid block" );
                        return;
                    }

                    block = TimeSpan.FromMilliseconds(ms);
                    index += 2;
                    continue;
                }

                if (string.Equals(token, "STREAMS", StringComparison.OrdinalIgnoreCase))
                {
                    break;
                }

                WriteError(ctx, "ERR syntax error");
                return;
            }

            if (group == null || consumer == null)
            {
                WriteError(ctx, "ERR syntax error");
                return;
            }

            if (index >= args.Count || !TryGetString(args[index], out var streamsKeyword) ||
                !string.Equals(streamsKeyword, "STREAMS", StringComparison.OrdinalIgnoreCase))
            {
                WriteError(ctx, "ERR syntax error");
                return;
            }

            int remaining = args.Count - (index + 1);
            if (remaining < 2 || remaining % 2 != 0)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'xreadgroup' command");
                return;
            }

            int streamCount = remaining / 2;
            var keys = new string[streamCount];
            var ids = new string[streamCount];

            for (int i = 0; i < streamCount; i++)
            {
                if (!TryGetString(args[index + 1 + i], out var key) ||
                    !TryGetString(args[index + 1 + i + streamCount], out var id))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }

                if (id != ">" && !TryParseStreamIdText(id))
                {
                    WriteError(ctx, "ERR invalid stream id");
                    return;
                }

                keys[i] = key;
                ids[i] = id;
            }

            var result = await _store.StreamGroupReadAsync(group, consumer, keys, ids, count, block).ConfigureAwait(false);
            switch (result.Status)
            {
                case StreamGroupReadResultStatus.WrongType:
                    WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                    return;

                case StreamGroupReadResultStatus.NoStream:
                    WriteError(ctx, "ERR The XREADGROUP subcommand requires the key to exist");
                    return;

                case StreamGroupReadResultStatus.NoGroup:
                    WriteError(ctx, "NOGROUP No such consumer group");
                    return;

                case StreamGroupReadResultStatus.InvalidId:
                    WriteError(ctx, "ERR invalid stream id");
                    return;
            }

            if (result.Results.Length == 0)
            {
                WriteNullArray(ctx);
                return;
            }

            var streamMessages = new List<IRedisMessage>();
            foreach (var stream in result.Results)
            {
                var entryMessages = new IRedisMessage[stream.Entries.Length];
                for (int i = 0; i < stream.Entries.Length; i++)
                {
                    var entry = stream.Entries[i];
                    var fieldChildren = new IRedisMessage[entry.Fields.Length * 2];

                    for (int j = 0; j < entry.Fields.Length; j++)
                    {
                        var fieldBytes = Utf8.GetBytes(entry.Fields[j].Key);
                        fieldChildren[j * 2] = new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(fieldBytes));
                        fieldChildren[j * 2 + 1] = new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(entry.Fields[j].Value));
                    }

                    var entryChildren = new IRedisMessage[2]
                    {
                        new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(Utf8.GetBytes(entry.Id))),
                        new ArrayRedisMessage(fieldChildren)
                    };

                    entryMessages[i] = new ArrayRedisMessage(entryChildren);
                }

                var streamChildren = new IRedisMessage[2]
                {
                    new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(Utf8.GetBytes(stream.Key))),
                    new ArrayRedisMessage(entryMessages)
                };

                streamMessages.Add(new ArrayRedisMessage(streamChildren));
            }

            WriteArray(ctx, streamMessages);
        }

        /// <summary>
        /// Handles the XACK command.
        /// </summary>
        private async Task HandleXAckAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count < 4)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'xack' command");
                return;
            }

            if (!TryGetString(args[1], out var key) || !TryGetString(args[2], out var group))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var ids = new string[args.Count - 3];
            for (int i = 3; i < args.Count; i++)
            {
                if (!TryGetString(args[i], out var id) || !TryParseStreamIdText(id))
                {
                    WriteError(ctx, "ERR invalid stream id");
                    return;
                }

                ids[i - 3] = id;
            }

            var result = await _store.StreamAckAsync(key, group, ids).ConfigureAwait(false);
            switch (result.Status)
            {
                case StreamAckResultStatus.WrongType:
                    WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                    return;

                case StreamAckResultStatus.NoStream:
                    WriteError(ctx, "ERR The XACK subcommand requires the key to exist");
                    return;

                case StreamAckResultStatus.NoGroup:
                    WriteError(ctx, "NOGROUP No such consumer group");
                    return;
            }

            WriteInteger(ctx, result.Count);
        }

        /// <summary>
        /// Handles the XPENDING command.
        /// </summary>
        private async Task HandleXPendingAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            // XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
            if (args.Count < 3)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'xpending' command");
                return;
            }

            if (!TryGetString(args[1], out var key) || !TryGetString(args[2], out var group))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            long? minIdleTimeMs = null;
            string? start = null;
            string? end = null;
            int? count = null;
            string? consumer = null;

            int argIndex = 3;

            // Check for IDLE option
            if (argIndex < args.Count && TryGetString(args[argIndex], out var idleArg) &&
                idleArg.Equals("IDLE", StringComparison.OrdinalIgnoreCase))
            {
                argIndex++;
                if (argIndex >= args.Count || !TryGetString(args[argIndex], out var idleStr) ||
                    !long.TryParse(idleStr, out var idleValue))
                {
                    WriteError(ctx, "ERR invalid idle time");
                    return;
                }
                minIdleTimeMs = idleValue;
                argIndex++;
            }

            // Check for extended form (start end count)
            if (argIndex < args.Count)
            {
                if (!TryGetString(args[argIndex], out start) || !IsRangeId(start))
                {
                    WriteError(ctx, "ERR invalid stream id");
                    return;
                }
                argIndex++;

                if (argIndex >= args.Count || !TryGetString(args[argIndex], out end) || !IsRangeId(end))
                {
                    WriteError(ctx, "ERR invalid stream id");
                    return;
                }
                argIndex++;

                if (argIndex >= args.Count || !TryGetString(args[argIndex], out var countStr) ||
                    !int.TryParse(countStr, out var countValue) || countValue < 0)
                {
                    WriteError(ctx, "ERR invalid count");
                    return;
                }
                count = countValue;
                argIndex++;

                // Check for optional consumer
                if (argIndex < args.Count && TryGetString(args[argIndex], out consumer))
                {
                    argIndex++;
                }
            }

            if (argIndex < args.Count)
            {
                WriteError(ctx, "ERR syntax error");
                return;
            }

            var result = await _store.StreamPendingAsync(
                key, group, minIdleTimeMs, start, end, count, consumer).ConfigureAwait(false);

            switch (result.Status)
            {
                case StreamPendingResultStatus.WrongType:
                    WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                    return;

                case StreamPendingResultStatus.NoStream:
                    WriteError(ctx, "ERR The XPENDING command requires the key to exist");
                    return;

                case StreamPendingResultStatus.NoGroup:
                    WriteError(ctx, "NOGROUP No such consumer group");
                    return;
            }

            // Extended form - return detailed entries
            if (result.Entries.Length > 0)
            {
                var array = new List<IRedisMessage>();
                foreach (var entry in result.Entries)
                {
                    var entryArray = new List<IRedisMessage>
                    {
                        new SimpleStringRedisMessage(entry.Id),
                        new SimpleStringRedisMessage(entry.Consumer),
                        new IntegerRedisMessage(entry.IdleTimeMs),
                        new IntegerRedisMessage(entry.DeliveryCount)
                    };
                    array.Add(new ArrayRedisMessage(entryArray));
                }
                await ctx.WriteAndFlushAsync(new ArrayRedisMessage(array));
                return;
            }

            // Summary form
            if (result.Count == 0)
            {
                var empty = new List<IRedisMessage>
                {
                    new IntegerRedisMessage(0),
                    FullBulkStringRedisMessage.Null,
                    FullBulkStringRedisMessage.Null,
                    FullBulkStringRedisMessage.Null
                };
                await ctx.WriteAndFlushAsync(new ArrayRedisMessage(empty));
                return;
            }

            var summary = new List<IRedisMessage>
            {
                new IntegerRedisMessage(result.Count),
                new SimpleStringRedisMessage(result.SmallestId!),
                new SimpleStringRedisMessage(result.LargestId!),
            };

            var consumersList = new List<IRedisMessage>();
            foreach (var consumerInfo in result.Consumers)
            {
                var consumerArray = new List<IRedisMessage>
                {
                    new SimpleStringRedisMessage(consumerInfo.Name),
                    new SimpleStringRedisMessage(consumerInfo.Count.ToString())
                };
                consumersList.Add(new ArrayRedisMessage(consumerArray));
            }
            summary.Add(new ArrayRedisMessage(consumersList));

            await ctx.WriteAndFlushAsync(new ArrayRedisMessage(summary));
        }

        /// <summary>
        /// Handles the XCLAIM command.
        /// </summary>
        private async Task HandleXClaimAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            // XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME ms-unix-time] [RETRYCOUNT count] [FORCE] [JUSTID]
            if (args.Count < 6)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'xclaim' command");
                return;
            }

            if (!TryGetString(args[1], out var key) || 
                !TryGetString(args[2], out var group) ||
                !TryGetString(args[3], out var consumer) ||
                !TryGetString(args[4], out var minIdleStr) ||
                !long.TryParse(minIdleStr, out var minIdleTimeMs) || minIdleTimeMs < 0)
            {
                WriteError(ctx, "ERR invalid arguments");
                return;
            }

            // Collect IDs and options
            var idList = new List<string>();
            long? idleMs = null;
            long? timeMs = null;
            long? retryCount = null;
            bool force = false;
            bool justId = false;

            int i = 5;
            while (i < args.Count)
            {
                if (!TryGetString(args[i], out var arg))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }

                var upperArg = arg.ToUpperInvariant();
                if (upperArg == "IDLE")
                {
                    i++;
                    if (i >= args.Count || !TryGetString(args[i], out var idleStr) ||
                        !long.TryParse(idleStr, out var idleValue) || idleValue < 0)
                    {
                        WriteError(ctx, "ERR invalid IDLE option");
                        return;
                    }
                    idleMs = idleValue;
                    i++;
                }
                else if (upperArg == "TIME")
                {
                    i++;
                    if (i >= args.Count || !TryGetString(args[i], out var timeStr) ||
                        !long.TryParse(timeStr, out var timeValue) || timeValue < 0)
                    {
                        WriteError(ctx, "ERR invalid TIME option");
                        return;
                    }
                    timeMs = timeValue;
                    i++;
                }
                else if (upperArg == "RETRYCOUNT")
                {
                    i++;
                    if (i >= args.Count || !TryGetString(args[i], out var retryStr) ||
                        !long.TryParse(retryStr, out var retryValue) || retryValue < 0)
                    {
                        WriteError(ctx, "ERR invalid RETRYCOUNT option");
                        return;
                    }
                    retryCount = retryValue;
                    i++;
                }
                else if (upperArg == "FORCE")
                {
                    force = true;
                    i++;
                }
                else if (upperArg == "JUSTID")
                {
                    justId = true;
                    i++;
                }
                else
                {
                    // This should be an ID
                    if (!TryParseStreamIdText(arg))
                    {
                        WriteError(ctx, "ERR invalid stream id");
                        return;
                    }
                    idList.Add(arg);
                    i++;
                }
            }

            if (idList.Count == 0)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'xclaim' command");
                return;
            }

            var result = await _store.StreamClaimAsync(
                key, group, consumer, minIdleTimeMs, idList.ToArray(),
                idleMs, timeMs, retryCount, force).ConfigureAwait(false);

            switch (result.Status)
            {
                case StreamClaimResultStatus.WrongType:
                    WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                    return;

                case StreamClaimResultStatus.NoStream:
                    WriteError(ctx, "ERR The XCLAIM command requires the key to exist");
                    return;

                case StreamClaimResultStatus.NoGroup:
                    WriteError(ctx, "NOGROUP No such consumer group");
                    return;
            }

            // Return results
            if (justId)
            {
                // JUSTID: Return only entry IDs
                var idArray = new List<IRedisMessage>();
                foreach (var entry in result.Entries)
                {
                    idArray.Add(new SimpleStringRedisMessage(entry.Id));
                }
                await ctx.WriteAndFlushAsync(new ArrayRedisMessage(idArray));
            }
            else
            {
                // Return full entries
                var array = new List<IRedisMessage>();
                foreach (var entry in result.Entries)
                {
                    var entryArray = new List<IRedisMessage>
                    {
                        new SimpleStringRedisMessage(entry.Id)
                    };

                    var fieldsArray = new List<IRedisMessage>();
                    foreach (var field in entry.Fields)
                    {
                        fieldsArray.Add(new SimpleStringRedisMessage(field.Key));
                        fieldsArray.Add(new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(field.Value)));
                    }
                    entryArray.Add(new ArrayRedisMessage(fieldsArray));

                    array.Add(new ArrayRedisMessage(entryArray));
                }
                await ctx.WriteAndFlushAsync(new ArrayRedisMessage(array));
            }
        }

        /// <summary>
        /// Validates a stream id in the form "ms-seq".
        /// </summary>
        private static bool TryParseStreamIdText(string text)
        {
            var parts = text.Split('-');
            if (parts.Length != 2)
            {
                return false;
            }

            return long.TryParse(parts[0], out _) && long.TryParse(parts[1], out _);
        }

        /// <summary>
        /// Determines whether a range id is valid for XRANGE bounds.
        /// </summary>
        private static bool IsRangeId(string text)
        {
            return text == "-" || text == "+" || TryParseStreamIdText(text);
        }

        /// <summary>
        /// Determines whether a group create id is valid for XGROUP CREATE.
        /// </summary>
        private static bool IsGroupCreateId(string text)
        {
            return text == "-" || text == "$" || TryParseStreamIdText(text);
        }

        /// <summary>
        /// Reads a string representation from a Redis message.
        /// </summary>
        private static string GetString(IRedisMessage msg)
        {
            switch (msg)
            {
                case FullBulkStringRedisMessage bulk when bulk.Content != null:
                    // ReadableBytes provides the length of the readable data in the buffer.
                    var length = bulk.Content.ReadableBytes;
                    var tmp = new byte[length];
                    bulk.Content.GetBytes(bulk.Content.ReaderIndex, tmp, 0, length);
                    return Utf8.GetString(tmp, 0, length);

                case SimpleStringRedisMessage simple:
                    return simple.Content;

                default:
                    return string.Empty;
            }
        }

        /// <summary>
        /// Tries to read a string from a Redis message.
        /// </summary>
        private static bool TryGetString(IRedisMessage msg, out string value)
        {
            switch (msg)
            {
                case FullBulkStringRedisMessage bulk when bulk.Content != null:
                    var length = bulk.Content.ReadableBytes;
                    var tmp = new byte[length];
                    bulk.Content.GetBytes(bulk.Content.ReaderIndex, tmp, 0, length);
                    value = Utf8.GetString(tmp, 0, length);
                    return true;

                case SimpleStringRedisMessage simple:
                    value = simple.Content;
                    return true;

                default:
                    value = string.Empty;
                    return false;
            }
        }

        /// <summary>
        /// Tries to read bytes from a Redis message.
        /// </summary>
        private static bool TryGetBytes(IRedisMessage msg, out byte[] value)
        {
            switch (msg)
            {
                case FullBulkStringRedisMessage bulk when bulk.Content != null:
                    var length = bulk.Content.ReadableBytes;
                    var buffer = new byte[length];
                    bulk.Content.GetBytes(bulk.Content.ReaderIndex, buffer, 0, length);
                    value = buffer;
                    return true;

                case SimpleStringRedisMessage simple:
                    value = Utf8.GetBytes(simple.Content);
                    return true;

                default:
                    value = Array.Empty<byte>();
                    return false;
            }
        }

        /// <summary>
        /// Writes a simple string reply.
        /// </summary>
        private static void WriteSimpleString(IChannelHandlerContext ctx, string text) =>
            ctx.WriteAndFlushAsync(new SimpleStringRedisMessage(text));

        /// <summary>
        /// Writes an error reply.
        /// </summary>
        private static void WriteError(IChannelHandlerContext ctx, string error) =>
            ctx.WriteAndFlushAsync(new ErrorRedisMessage(error));

        /// <summary>
        /// Writes an integer reply.
        /// </summary>
        private static void WriteInteger(IChannelHandlerContext ctx, long value) =>
            ctx.WriteAndFlushAsync(new IntegerRedisMessage(value));

        /// <summary>
        /// Writes a null bulk string reply.
        /// </summary>
        private static void WriteNullBulkString(IChannelHandlerContext ctx) =>
            ctx.WriteAndFlushAsync(FullBulkStringRedisMessage.Null);

        /// <summary>
        /// Writes a bulk string reply.
        /// </summary>
        private static void WriteBulkString(IChannelHandlerContext ctx, byte[] data) =>
            ctx.WriteAndFlushAsync(new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(data)));

        /// <summary>
        /// Writes an array reply.
        /// </summary>
        private static void WriteArray(IChannelHandlerContext ctx, IList<IRedisMessage> children) =>
            ctx.WriteAndFlushAsync(new ArrayRedisMessage(children));

        /// <summary>
        /// Writes a null array reply.
        /// </summary>
        private static void WriteNullArray(IChannelHandlerContext ctx) =>
            ctx.WriteAndFlushAsync(new ArrayRedisMessage(null));
    }
}