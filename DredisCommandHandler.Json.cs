using System.Collections.Generic;
using System.Linq;
using DotNetty.Buffers;
using DotNetty.Codecs.Redis.Messages;
using DotNetty.Transport.Channels;
using Dredis.Abstractions.Storage;

namespace Dredis
{
    /// <summary>
    /// JSON command handlers for Dredis.
    /// Implements RedisJSON-compatible commands for storing and manipulating JSON documents.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This partial class extends <see cref="DredisCommandHandler"/> with JSON document support.
    /// All commands use JSONPath syntax for navigating and manipulating JSON structures.
    /// </para>
    /// <para>
    /// Supported commands:
    /// <list type="bullet">
    /// <item><description><c>JSON.SET key path value</c> - Sets JSON value at the specified path</description></item>
    /// <item><description><c>JSON.GET key [path ...]</c> - Gets JSON values from one or more paths (defaults to root "$")</description></item>
    /// <item><description><c>JSON.DEL key [path ...]</c> - Deletes JSON values at specified paths</description></item>
    /// <item><description><c>JSON.TYPE key [path ...]</c> - Returns JSON type(s) at specified path(s)</description></item>
    /// <item><description><c>JSON.STRLEN key [path ...]</c> - Returns string length(s) at specified path(s)</description></item>
    /// <item><description><c>JSON.ARRLEN key [path ...]</c> - Returns array length(s) at specified path(s)</description></item>
    /// <item><description><c>JSON.ARRAPPEND key path value [value ...]</c> - Appends values to array at path</description></item>
    /// <item><description><c>JSON.ARRINDEX key path value</c> - Returns index of value in array, or -1 if not found</description></item>
    /// <item><description><c>JSON.ARRINSERT key path index value [value ...]</c> - Inserts values at index in array</description></item>
    /// <item><description><c>JSON.ARRREM key path [index]</c> - Removes element at index (or last element if no index)</description></item>
    /// <item><description><c>JSON.ARRTRIM key path start stop</c> - Trims array to specified range</description></item>
    /// <item><description><c>JSON.MGET key [key ...] path</c> - Gets JSON values from multiple keys at specified path</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// JSONPath syntax supports standard path expressions:
    /// <list type="bullet">
    /// <item><description><c>$</c> - Root element</description></item>
    /// <item><description><c>$.property</c> - Object property access</description></item>
    /// <item><description><c>$[0]</c> - Array index access</description></item>
    /// <item><description><c>$.users[0].name</c> - Nested path traversal</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// JSON types returned by JSON.TYPE:
    /// <c>object</c>, <c>array</c>, <c>string</c>, <c>number</c>, <c>boolean</c>, <c>null</c>
    /// </para>
    /// </remarks>
    public partial class DredisCommandHandler
    {
        /// <summary>
        /// Handles the JSON.SET command.
        /// </summary>
        private async Task HandleJsonSetAsync(IChannelHandlerContext ctx, IList<IRedisMessage> args)
        {
            if (args.Count < 4)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'json.set' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            if (!TryGetString(args[2], out var path))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            if (!TryGetBytes(args[3], out var value))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var result = await _store.JsonSetAsync(key, path, value).ConfigureAwait(false);
            
            if (result.Status == JsonResultStatus.Ok)
            {
                WriteSimpleString(ctx, "OK");
                Transactions.NotifyKeyModified(key, _store);
            }
            else if (result.Status == JsonResultStatus.InvalidJson)
            {
                WriteError(ctx, "ERR invalid JSON");
            }
            else if (result.Status == JsonResultStatus.InvalidPath)
            {
                WriteError(ctx, "ERR invalid path");
            }
            else
            {
                WriteError(ctx, "ERR operation failed");
            }
        }

        /// <summary>
        /// Handles the JSON.GET command.
        /// </summary>
        private async Task HandleJsonGetAsync(IChannelHandlerContext ctx, IList<IRedisMessage> args)
        {
            if (args.Count < 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'json.get' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var paths = new List<string>();
            for (int i = 2; i < args.Count; i++)
            {
                if (TryGetString(args[i], out var path))
                {
                    paths.Add(path);
                }
            }

            if (paths.Count == 0)
            {
                paths.Add("$");
            }

            var result = await _store.JsonGetAsync(key, paths.ToArray()).ConfigureAwait(false);

            if (result.Status == JsonResultStatus.Ok)
            {
                if (result.Value != null)
                {
                    WriteBulkString(ctx, result.Value);
                }
                else if (result.Values != null)
                {
                    var array = new IRedisMessage[result.Values.Length];
                    for (int i = 0; i < result.Values.Length; i++)
                    {
                        array[i] = new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(result.Values[i]));
                    }
                    WriteArray(ctx, array);
                }
            }
            else if (result.Status == JsonResultStatus.PathNotFound)
            {
                WriteNullBulkString(ctx);
            }
            else
            {
                WriteError(ctx, "ERR operation failed");
            }
        }

        /// <summary>
        /// Handles the JSON.DEL command.
        /// </summary>
        private async Task HandleJsonDelAsync(IChannelHandlerContext ctx, IList<IRedisMessage> args)
        {
            if (args.Count < 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'json.del' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var paths = new List<string>();
            for (int i = 2; i < args.Count; i++)
            {
                if (TryGetString(args[i], out var path))
                {
                    paths.Add(path);
                }
            }

            if (paths.Count == 0)
            {
                paths.Add("$");
            }

            var result = await _store.JsonDelAsync(key, paths.ToArray()).ConfigureAwait(false);

            if (result.Status == JsonResultStatus.Ok)
            {
                WriteInteger(ctx, result.Deleted);
                Transactions.NotifyKeyModified(key, _store);
            }
            else
            {
                WriteError(ctx, "ERR operation failed");
            }
        }

        /// <summary>
        /// Handles the JSON.TYPE command.
        /// </summary>
        private async Task HandleJsonTypeAsync(IChannelHandlerContext ctx, IList<IRedisMessage> args)
        {
            if (args.Count < 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'json.type' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var paths = new List<string>();
            for (int i = 2; i < args.Count; i++)
            {
                if (TryGetString(args[i], out var path))
                {
                    paths.Add(path);
                }
            }

            if (paths.Count == 0)
            {
                paths.Add("$");
            }

            var result = await _store.JsonTypeAsync(key, paths.ToArray()).ConfigureAwait(false);

            if (result.Status == JsonResultStatus.Ok && result.Types != null)
            {
                if (result.Types.Length == 1)
                {
                    WriteBulkString(ctx, System.Text.Encoding.UTF8.GetBytes(result.Types[0]));
                }
                else
                {
                    var array = new IRedisMessage[result.Types.Length];
                    for (int i = 0; i < result.Types.Length; i++)
                    {
                        array[i] = new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(System.Text.Encoding.UTF8.GetBytes(result.Types[i])));
                    }
                    WriteArray(ctx, array);
                }
            }
            else
            {
                WriteError(ctx, "ERR operation failed");
            }
        }

        /// <summary>
        /// Handles the JSON.STRLEN command.
        /// </summary>
        private async Task HandleJsonStrlenAsync(IChannelHandlerContext ctx, IList<IRedisMessage> args)
        {
            if (args.Count < 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'json.strlen' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var paths = new List<string>();
            for (int i = 2; i < args.Count; i++)
            {
                if (TryGetString(args[i], out var path))
                {
                    paths.Add(path);
                }
            }

            if (paths.Count == 0)
            {
                paths.Add("$");
            }

            var result = await _store.JsonStrlenAsync(key, paths.ToArray()).ConfigureAwait(false);

            if (result.Status == JsonResultStatus.Ok)
            {
                if (result.Count.HasValue)
                {
                    WriteInteger(ctx, result.Count.Value);
                }
                else if (result.Counts != null)
                {
                    var array = new List<IRedisMessage>();
                    foreach (var count in result.Counts)
                    {
                        array.Add(new IntegerRedisMessage(count));
                    }
                    WriteArray(ctx, array);
                }
            }
            else
            {
                WriteError(ctx, "ERR operation failed");
            }
        }

        /// <summary>
        /// Handles the JSON.ARRLEN command.
        /// </summary>
        private async Task HandleJsonArrlenAsync(IChannelHandlerContext ctx, IList<IRedisMessage> args)
        {
            if (args.Count < 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'json.arrlen' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var paths = new List<string>();
            for (int i = 2; i < args.Count; i++)
            {
                if (TryGetString(args[i], out var path))
                {
                    paths.Add(path);
                }
            }

            if (paths.Count == 0)
            {
                paths.Add("$");
            }

            var result = await _store.JsonArrlenAsync(key, paths.ToArray()).ConfigureAwait(false);

            if (result.Status == JsonResultStatus.Ok)
            {
                if (result.Count.HasValue)
                {
                    WriteInteger(ctx, result.Count.Value);
                }
                else if (result.Counts != null)
                {
                    var array = new List<IRedisMessage>();
                    foreach (var count in result.Counts)
                    {
                        array.Add(new IntegerRedisMessage(count));
                    }
                    WriteArray(ctx, array);
                }
            }
            else
            {
                WriteError(ctx, "ERR operation failed");
            }
        }

        /// <summary>
        /// Handles the JSON.ARRAPPEND command.
        /// </summary>
        private async Task HandleJsonArrappendAsync(IChannelHandlerContext ctx, IList<IRedisMessage> args)
        {
            if (args.Count < 4)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'json.arrappend' command");
                return;
            }

            if (!TryGetString(args[1], out var key) || !TryGetString(args[2], out var path))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var values = new List<byte[]>();
            for (int i = 3; i < args.Count; i++)
            {
                if (TryGetBytes(args[i], out var value))
                {
                    values.Add(value);
                }
            }

            var result = await _store.JsonArrappendAsync(key, path, values.ToArray()).ConfigureAwait(false);

            if (result.Status == JsonResultStatus.Ok && result.Count.HasValue)
            {
                WriteInteger(ctx, result.Count.Value);
                Transactions.NotifyKeyModified(key, _store);
            }
            else
            {
                WriteError(ctx, "ERR operation failed");
            }
        }

        /// <summary>
        /// Handles the JSON.ARRINDEX command.
        /// </summary>
        private async Task HandleJsonArrindexAsync(IChannelHandlerContext ctx, IList<IRedisMessage> args)
        {
            if (args.Count < 4)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'json.arrindex' command");
                return;
            }

            if (!TryGetString(args[1], out var key) || !TryGetString(args[2], out var path) || !TryGetBytes(args[3], out var value))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var result = await _store.JsonArrindexAsync(key, path, value).ConfigureAwait(false);

            if (result.Status == JsonResultStatus.Ok && result.Value != null)
            {
                WriteBulkString(ctx, result.Value);
            }
            else
            {
                WriteError(ctx, "ERR operation failed");
            }
        }

        /// <summary>
        /// Handles the JSON.ARRINSERT command.
        /// </summary>
        private async Task HandleJsonArrinsertAsync(IChannelHandlerContext ctx, IList<IRedisMessage> args)
        {
            if (args.Count < 5)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'json.arrinsert' command");
                return;
            }

            if (!TryGetString(args[1], out var key) || !TryGetString(args[2], out var path) || 
                !TryGetString(args[3], out var indexStr) || !long.TryParse(indexStr, out var indexVal))
            {
                WriteError(ctx, "ERR invalid argument");
                return;
            }

            var values = new List<byte[]>();
            for (int i = 4; i < args.Count; i++)
            {
                if (TryGetBytes(args[i], out var value))
                {
                    values.Add(value);
                }
            }

            var result = await _store.JsonArrinsertAsync(key, path, (int)indexVal, values.ToArray()).ConfigureAwait(false);

            if (result.Status == JsonResultStatus.Ok && result.Count.HasValue)
            {
                WriteInteger(ctx, result.Count.Value);
                Transactions.NotifyKeyModified(key, _store);
            }
            else
            {
                WriteError(ctx, "ERR operation failed");
            }
        }

        /// <summary>
        /// Handles the JSON.ARRREM command.
        /// </summary>
        private async Task HandleJsonArrremAsync(IChannelHandlerContext ctx, IList<IRedisMessage> args)
        {
            if (args.Count < 3)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'json.arrrem' command");
                return;
            }

            if (!TryGetString(args[1], out var key) || !TryGetString(args[2], out var path))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            int? index = null;
            if (args.Count >= 4 && TryGetString(args[3], out var indexStr) && long.TryParse(indexStr, out var indexVal))
            {
                index = (int)indexVal;
            }

            var result = await _store.JsonArrremAsync(key, path, index).ConfigureAwait(false);

            if (result.Status == JsonResultStatus.Ok && result.Count.HasValue)
            {
                WriteInteger(ctx, result.Count.Value);
                Transactions.NotifyKeyModified(key, _store);
            }
            else
            {
                WriteError(ctx, "ERR operation failed");
            }
        }

        /// <summary>
        /// Handles the JSON.ARRTRIM command.
        /// </summary>
        private async Task HandleJsonArrtrimAsync(IChannelHandlerContext ctx, IList<IRedisMessage> args)
        {
            if (args.Count < 5)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'json.arrtrim' command");
                return;
            }

            if (!TryGetString(args[1], out var key) || !TryGetString(args[2], out var path) || 
                !TryGetString(args[3], out var startStr) || !long.TryParse(startStr, out var startVal) ||
                !TryGetString(args[4], out var stopStr) || !long.TryParse(stopStr, out var stopVal))
            {
                WriteError(ctx, "ERR invalid argument");
                return;
            }

            var result = await _store.JsonArrtrimAsync(key, path, (int)startVal, (int)stopVal).ConfigureAwait(false);

            if (result.Status == JsonResultStatus.Ok && result.Count.HasValue)
            {
                WriteInteger(ctx, result.Count.Value);
                Transactions.NotifyKeyModified(key, _store);
            }
            else
            {
                WriteError(ctx, "ERR operation failed");
            }
        }

        /// <summary>
        /// Handles the JSON.MGET command.
        /// </summary>
        private async Task HandleJsonMgetAsync(IChannelHandlerContext ctx, IList<IRedisMessage> args)
        {
            if (args.Count < 3)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'json.mget' command");
                return;
            }

            var keys = new List<string>();
            string path = "$";

            for (int i = 1; i < args.Count - 1; i++)
            {
                if (TryGetString(args[i], out var key))
                {
                    keys.Add(key);
                }
            }

            if (TryGetString(args[args.Count - 1], out var lastArg))
            {
                if (lastArg.StartsWith("$") || lastArg.Contains("."))
                {
                    path = lastArg;
                }
                else
                {
                    keys.Add(lastArg);
                }
            }

            var result = await _store.JsonMgetAsync(keys.ToArray(), path).ConfigureAwait(false);

            if (result.Status == JsonResultStatus.Ok && result.Values != null)
            {
var array = new IRedisMessage[result.Values.Length];
                    for (int i = 0; i < result.Values.Length; i++)
                    {
                        array[i] = new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(result.Values[i]));
                }
                WriteArray(ctx, array);
            }
            else
            {
                WriteError(ctx, "ERR operation failed");
            }
        }
    }
}
