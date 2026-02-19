using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Text;
using DotNetty.Buffers;
using DotNetty.Codecs.Redis.Messages;
using DotNetty.Common;
using DotNetty.Common.Concurrency;
using DotNetty.Common.Utilities;
using DotNetty.Transport.Channels;
using DotNetty.Transport.Channels.Embedded;
using Dredis.Abstractions.Storage;

namespace Dredis
{
    /// <summary>
    /// Manages Pub/Sub subscriptions for Redis channels.
    /// </summary>
    public sealed class PubSubManager
    {
        private readonly Dictionary<string, HashSet<IChannelHandlerContext>> _subscriptions = new(StringComparer.Ordinal);
        private readonly Dictionary<string, HashSet<IChannelHandlerContext>> _patternSubscriptions = new(StringComparer.Ordinal);
        private readonly Dictionary<IChannelHandlerContext, HashSet<string>> _contextSubscriptions = new();
        private readonly Dictionary<IChannelHandlerContext, HashSet<string>> _contextPatternSubscriptions = new();
        private readonly object _lock = new object();

        /// <summary>
        /// Subscribes a channel context to one or more channels.
        /// </summary>
        public void Subscribe(IChannelHandlerContext ctx, params string[] channels)
        {
            lock (_lock)
            {
                foreach (var channel in channels)
                {
                    if (!_subscriptions.TryGetValue(channel, out var subscribers))
                    {
                        subscribers = new HashSet<IChannelHandlerContext>();
                        _subscriptions[channel] = subscribers;
                    }

                    subscribers.Add(ctx);

                    if (!_contextSubscriptions.TryGetValue(ctx, out var ctxChannels))
                    {
                        ctxChannels = new HashSet<string>(StringComparer.Ordinal);
                        _contextSubscriptions[ctx] = ctxChannels;
                    }

                    ctxChannels.Add(channel);
                }
            }
        }

        /// <summary>
        /// Unsubscribes a channel context from one or more channels. If no channels specified, returns all subscribed channels.
        /// </summary>
        public string[] GetChannelsToUnsubscribe(IChannelHandlerContext ctx, params string[] channels)
        {
            lock (_lock)
            {
                // If no channels specified, return all channels
                if (channels.Length == 0)
                {
                    if (_contextSubscriptions.TryGetValue(ctx, out var ctxChannels))
                    {
                        return ctxChannels.ToArray();
                    }
                    else
                    {
                        return Array.Empty<string>();
                    }
                }

                return channels;
            }
        }

        /// <summary>
        /// Unsubscribes a channel context from a single channel.
        /// </summary>
        public void UnsubscribeOne(IChannelHandlerContext ctx, string channel)
        {
            lock (_lock)
            {
                if (_subscriptions.TryGetValue(channel, out var subscribers))
                {
                    subscribers.Remove(ctx);
                    if (subscribers.Count == 0)
                    {
                        _subscriptions.Remove(channel);
                    }
                }

                if (_contextSubscriptions.TryGetValue(ctx, out var ctxChannels))
                {
                    ctxChannels.Remove(channel);
                    if (ctxChannels.Count == 0)
                    {
                        _contextSubscriptions.Remove(ctx);
                    }
                }
            }
        }

        /// <summary>
        /// Subscribes a channel context to one or more channel patterns.
        /// </summary>
        public void PSubscribe(IChannelHandlerContext ctx, params string[] patterns)
        {
            lock (_lock)
            {
                foreach (var pattern in patterns)
                {
                    if (!_patternSubscriptions.TryGetValue(pattern, out var subscribers))
                    {
                        subscribers = new HashSet<IChannelHandlerContext>();
                        _patternSubscriptions[pattern] = subscribers;
                    }

                    subscribers.Add(ctx);

                    if (!_contextPatternSubscriptions.TryGetValue(ctx, out var ctxPatterns))
                    {
                        ctxPatterns = new HashSet<string>(StringComparer.Ordinal);
                        _contextPatternSubscriptions[ctx] = ctxPatterns;
                    }

                    ctxPatterns.Add(pattern);
                }
            }
        }

        /// <summary>
        /// Unsubscribes a channel context from one or more channel patterns. If no patterns specified, returns all subscribed patterns.
        /// </summary>
        public string[] GetPatternsToUnsubscribe(IChannelHandlerContext ctx, params string[] patterns)
        {
            lock (_lock)
            {
                // If no patterns specified, return all patterns
                if (patterns.Length == 0)
                {
                    if (_contextPatternSubscriptions.TryGetValue(ctx, out var ctxPatterns))
                    {
                        return ctxPatterns.ToArray();
                    }
                    else
                    {
                        return Array.Empty<string>();
                    }
                }

                return patterns;
            }
        }

        /// <summary>
        /// Unsubscribes a channel context from a single channel pattern.
        /// </summary>
        public void PUnsubscribeOne(IChannelHandlerContext ctx, string pattern)
        {
            lock (_lock)
            {
                if (_patternSubscriptions.TryGetValue(pattern, out var subscribers))
                {
                    subscribers.Remove(ctx);
                    if (subscribers.Count == 0)
                    {
                        _patternSubscriptions.Remove(pattern);
                    }
                }

                if (_contextPatternSubscriptions.TryGetValue(ctx, out var ctxPatterns))
                {
                    ctxPatterns.Remove(pattern);
                    if (ctxPatterns.Count == 0)
                    {
                        _contextPatternSubscriptions.Remove(ctx);
                    }
                }
            }
        }

        /// <summary>
        /// Gets the total subscription count for a context (channels + patterns).
        /// </summary>
        public int GetSubscriptionCount(IChannelHandlerContext ctx)
        {
            lock (_lock)
            {
                int count = 0;
                if (_contextSubscriptions.TryGetValue(ctx, out var channels))
                {
                    count += channels.Count;
                }
                if (_contextPatternSubscriptions.TryGetValue(ctx, out var patterns))
                {
                    count += patterns.Count;
                }
                return count;
            }
        }

        /// <summary>
        /// Matches a channel name against a glob-style pattern.
        /// </summary>
        private static bool MatchPattern(string pattern, string channel)
        {
            int p = 0, c = 0;
            int starIdx = -1, matchIdx = 0;

            while (c < channel.Length)
            {
                if (p < pattern.Length && (pattern[p] == channel[c] || pattern[p] == '?'))
                {
                    p++;
                    c++;
                }
                else if (p < pattern.Length && pattern[p] == '*')
                {
                    starIdx = p;
                    matchIdx = c;
                    p++;
                }
                else if (p < pattern.Length && pattern[p] == '[')
                {
                    p++;
                    bool matched = false;
                    bool negated = false;
                    
                    if (p < pattern.Length && pattern[p] == '^')
                    {
                        negated = true;
                        p++;
                    }

                    while (p < pattern.Length && pattern[p] != ']')
                    {
                        if (pattern[p] == channel[c])
                        {
                            matched = true;
                        }
                        p++;
                    }

                    if (p < pattern.Length) p++; // skip ']'

                    if ((matched && !negated) || (!matched && negated))
                    {
                        c++;
                    }
                    else if (starIdx != -1)
                    {
                        p = starIdx + 1;
                        matchIdx++;
                        c = matchIdx;
                    }
                    else
                    {
                        return false;
                    }
                }
                else if (p < pattern.Length && pattern[p] == '\\' && p + 1 < pattern.Length)
                {
                    p++;
                    if (pattern[p] == channel[c])
                    {
                        p++;
                        c++;
                    }
                    else if (starIdx != -1)
                    {
                        p = starIdx + 1;
                        matchIdx++;
                        c = matchIdx;
                    }
                    else
                    {
                        return false;
                    }
                }
                else if (starIdx != -1)
                {
                    p = starIdx + 1;
                    matchIdx++;
                    c = matchIdx;
                }
                else
                {
                    return false;
                }
            }

            while (p < pattern.Length && pattern[p] == '*')
            {
                p++;
            }

            return p == pattern.Length;
        }

        /// <summary>
        /// Publishes a message to a channel and returns the number of subscribers.
        /// </summary>
        public int Publish(string channel, byte[] message)
        {
            lock (_lock)
            {
                var sentTo = new HashSet<IChannelHandlerContext>();

                // Send to direct channel subscribers
                if (_subscriptions.TryGetValue(channel, out var subscribers))
                {
                    var message_array = new IRedisMessage[]
                    {
                        new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(Encoding.UTF8.GetBytes("message"))),
                        new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(Encoding.UTF8.GetBytes(channel))),
                        new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(message))
                    };

                    foreach (var ctx in subscribers)
                    {
                        if (ctx.Channel.Active)
                        {
                            ctx.WriteAndFlushAsync(new ArrayRedisMessage(message_array));
                            sentTo.Add(ctx);
                        }
                    }
                }

                // Send to pattern subscribers
                foreach (var kvp in _patternSubscriptions)
                {
                    if (MatchPattern(kvp.Key, channel))
                    {
                        var pmessage_array = new IRedisMessage[]
                        {
                            new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(Encoding.UTF8.GetBytes("pmessage"))),
                            new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(Encoding.UTF8.GetBytes(kvp.Key))),
                            new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(Encoding.UTF8.GetBytes(channel))),
                            new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(message))
                        };

                        foreach (var ctx in kvp.Value)
                        {
                            if (ctx.Channel.Active)
                            {
                                ctx.WriteAndFlushAsync(new ArrayRedisMessage(pmessage_array));
                                sentTo.Add(ctx);
                            }
                        }
                    }
                }

                return sentTo.Count;
            }
        }

        /// <summary>
        /// Gets the number of subscribers for a channel.
        /// </summary>
        public int GetSubscriberCount(string channel)
        {
            lock (_lock)
            {
                return _subscriptions.TryGetValue(channel, out var subscribers) ? subscribers.Count : 0;
            }
        }

        /// <summary>
        /// Clears all subscriptions (for testing).
        /// </summary>
        public void Clear()
        {
            lock (_lock)
            {
                _subscriptions.Clear();
                _patternSubscriptions.Clear();
                _contextSubscriptions.Clear();
                _contextPatternSubscriptions.Clear();
            }
        }
    }

    /// <summary>
    /// Represents the state of a transaction for a connection.
    /// </summary>
    public sealed class TransactionState
    {
        /// <summary>
        /// Gets or sets a value indicating whether the connection is in a transaction (MULTI has been called).
        /// </summary>
        public bool InTransaction { get; set; }

        /// <summary>
        /// Gets the queue of commands to execute when EXEC is called.
        /// </summary>
        public List<IArrayRedisMessage> CommandQueue { get; } = new List<IArrayRedisMessage>();

        /// <summary>
        /// Gets the set of watched keys with their stored hash codes at watch time.
        /// </summary>
        public Dictionary<string, int> WatchedKeys { get; } = new Dictionary<string, int>(StringComparer.Ordinal);

        /// <summary>
        /// Gets or sets a value indicating whether any watched key has been modified.
        /// </summary>
        public bool WatchedKeyModified { get; set; }

        /// <summary>
        /// Clears the transaction state.
        /// </summary>
        public void Clear()
        {
            InTransaction = false;
            CommandQueue.Clear();
        }

        /// <summary>
        /// Clears all watched keys.
        /// </summary>
        public void ClearWatchedKeys()
        {
            WatchedKeys.Clear();
            WatchedKeyModified = false;
        }
    }

    /// <summary>
    /// Manages transaction state for connections.
    /// </summary>
    public sealed class TransactionManager
    {
        private readonly Dictionary<IChannelHandlerContext, TransactionState> _transactions = new Dictionary<IChannelHandlerContext, TransactionState>();
        private readonly object _lock = new object();

        /// <summary>
        /// Gets or creates transaction state for a connection.
        /// </summary>
        public TransactionState GetOrCreateState(IChannelHandlerContext ctx)
        {
            lock (_lock)
            {
                if (!_transactions.TryGetValue(ctx, out var state))
                {
                    state = new TransactionState();
                    _transactions[ctx] = state;
                }
                return state;
            }
        }

        /// <summary>
        /// Removes transaction state for a connection.
        /// </summary>
        public void RemoveState(IChannelHandlerContext ctx)
        {
            lock (_lock)
            {
                _transactions.Remove(ctx);
            }
        }

        /// <summary>
        /// Clears all transaction state (for testing).
        /// </summary>
        public void Clear()
        {
            lock (_lock)
            {
                _transactions.Clear();
            }
        }

        /// <summary>
        /// Checks if any watched key for a context has been modified and marks the transaction accordingly.
        /// </summary>
        public void CheckWatchedKeys(IChannelHandlerContext ctx, IKeyValueStore store)
        {
            lock (_lock)
            {
                if (!_transactions.TryGetValue(ctx, out var state))
                {
                    return;
                }

                foreach (var kvp in state.WatchedKeys)
                {
                    var currentHash = ComputeKeyHash(kvp.Key, store);
                    if (currentHash != kvp.Value)
                    {
                        state.WatchedKeyModified = true;
                        break;
                    }
                }
            }
        }

        /// <summary>
        /// Notifies all connections watching a key that it has been modified.
        /// </summary>
        public void NotifyKeyModified(string key, IKeyValueStore store)
        {
            lock (_lock)
            {
                foreach (var kvp in _transactions)
                {
                    var state = kvp.Value;
                    if (state.WatchedKeys.ContainsKey(key))
                    {
                        state.WatchedKeyModified = true;
                    }
                }
            }
        }

        /// <summary>
        /// Computes a hash for a key's current value.
        /// </summary>
        private int ComputeKeyHash(string key, IKeyValueStore store)
        {
            // For simplicity, we'll use a counter-based approach
            // In a real implementation, this would hash the actual value
            return key.GetHashCode();
        }
    }

    /// <summary>
    /// A simple context wrapper that captures written messages for transaction execution.
    /// </summary>
    internal sealed class CapturingContext : IChannelHandlerContext
    {
        private IRedisMessage? _capturedMessage;
        
        public IRedisMessage? CapturedMessage => _capturedMessage;

        public Task WriteAndFlushAsync(object message)
        {
            if (message is IRedisMessage redisMessage)
            {
                _capturedMessage = redisMessage;
            }
            return Task.CompletedTask;
        }

        public Task WriteAsync(object message)
        {
            if (message is IRedisMessage redisMessage)
            {
                _capturedMessage = redisMessage;
            }
            return Task.CompletedTask;
        }

        // Required IChannelHandlerContext members with minimal implementations
        public IChannel Channel => throw new NotImplementedException();
        public IByteBufferAllocator Allocator => throw new NotImplementedException();
        public IEventExecutor Executor => throw new NotImplementedException();
        public string Name => "CapturingContext";
        public IChannelHandler Handler => throw new NotImplementedException();
        public bool Removed => false;
        public IChannelHandlerContext FireChannelRegistered() => this;
        public IChannelHandlerContext FireChannelUnregistered() => this;
        public IChannelHandlerContext FireChannelActive() => this;
        public IChannelHandlerContext FireChannelInactive() => this;
        public IChannelHandlerContext FireExceptionCaught(Exception cause) => this;
        public IChannelHandlerContext FireUserEventTriggered(object evt) => this;
        public IChannelHandlerContext FireChannelRead(object msg) => this;
        public IChannelHandlerContext FireChannelReadComplete() => this;
        public IChannelHandlerContext FireChannelWritabilityChanged() => this;
        public Task BindAsync(EndPoint localAddress) => Task.CompletedTask;
        public Task ConnectAsync(EndPoint remoteAddress) => Task.CompletedTask;
        public Task ConnectAsync(EndPoint remoteAddress, EndPoint localAddress) => Task.CompletedTask;
        public Task DisconnectAsync() => Task.CompletedTask;
        public Task DisconnectAsync(object promise) => Task.CompletedTask;
        public Task CloseAsync() => Task.CompletedTask;
        public Task CloseAsync(object promise) => Task.CompletedTask;
        public Task DeregisterAsync() => Task.CompletedTask;
        public Task DeregisterAsync(object promise) => Task.CompletedTask;
        public IChannelHandlerContext Read() => this;
        public Task WriteAsync(object message, object promise) => Task.CompletedTask;
        public IChannelHandlerContext Flush() => this;
        public Task WriteAndFlushAsync(object message, object promise) => Task.CompletedTask;
        public object NewPromise() => throw new NotImplementedException();
        public object NewPromise(object state) => throw new NotImplementedException();
        public object VoidPromise() => throw new NotImplementedException();
        public IAttribute<T> GetAttribute<T>(AttributeKey<T> key) where T : class => throw new NotImplementedException();
        public bool HasAttribute<T>(AttributeKey<T> key) where T : class => false;
    }

    /// <summary>
    /// Bridges Redis commands (via DotNetty codec) to the IKeyValueStore abstraction.
    /// </summary>
    public sealed partial class DredisCommandHandler : SimpleChannelInboundHandler<IRedisMessage>
    {
        private readonly IKeyValueStore _store;
        private static readonly Encoding Utf8 = new UTF8Encoding(false);
        private static readonly PubSubManager PubSub = new PubSubManager();
        private static readonly TransactionManager Transactions = new TransactionManager();

        /// <summary>
        /// Gets the Pub/Sub manager (exposed for testing).
        /// </summary>
        public static PubSubManager PubSubManager => PubSub;

        /// <summary>
        /// Gets the Transaction manager (exposed for testing).
        /// </summary>
        public static TransactionManager TransactionManager => Transactions;

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
        internal async Task HandleCommandAsync(IChannelHandlerContext ctx, IArrayRedisMessage array)
        {
            var elements = array.Children;
            if (elements == null || elements.Count == 0)
            {
                WriteError(ctx, "ERR empty command");
                return;
            }

            var cmd = GetString(elements[0]).ToUpperInvariant();

            // Check if we're in a transaction and this is not a transaction control command
            var txState = Transactions.GetOrCreateState(ctx);
            if (txState.InTransaction && 
                cmd != "MULTI" && cmd != "EXEC" && cmd != "DISCARD" && 
                cmd != "WATCH" && cmd != "UNWATCH")
            {
                // Queue the command for later execution
                // Retain the message to prevent ByteBuf from being released
                array.Retain();
                txState.CommandQueue.Add(array);
                WriteSimpleString(ctx, "QUEUED");
                return;
            }

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

                case "LPUSH":
                    await HandleListPushAsync(ctx, elements, left: true);
                    break;

                case "RPUSH":
                    await HandleListPushAsync(ctx, elements, left: false);
                    break;

                case "LPOP":
                    await HandleListPopAsync(ctx, elements, left: true);
                    break;

                case "RPOP":
                    await HandleListPopAsync(ctx, elements, left: false);
                    break;

                case "LRANGE":
                    await HandleListRangeAsync(ctx, elements);
                    break;

                case "LLEN":
                    await HandleListLengthAsync(ctx, elements);
                    break;

                case "LINDEX":
                    await HandleListIndexAsync(ctx, elements);
                    break;

                case "LSET":
                    await HandleListSetAsync(ctx, elements);
                    break;

                case "LTRIM":
                    await HandleListTrimAsync(ctx, elements);
                    break;

                case "SADD":
                    await HandleSetAddAsync(ctx, elements);
                    break;

                case "SREM":
                    await HandleSetRemoveAsync(ctx, elements);
                    break;

                case "SMEMBERS":
                    await HandleSetMembersAsync(ctx, elements);
                    break;

                case "SCARD":
                    await HandleSetCardinalityAsync(ctx, elements);
                    break;

                case "ZADD":
                    await HandleSortedSetAddAsync(ctx, elements);
                    break;

                case "ZREM":
                    await HandleSortedSetRemoveAsync(ctx, elements);
                    break;

                case "ZRANGE":
                    await HandleSortedSetRangeAsync(ctx, elements);
                    break;

                case "ZCARD":
                    await HandleSortedSetCardinalityAsync(ctx, elements);
                    break;

                case "ZSCORE":
                    await HandleSortedSetScoreAsync(ctx, elements);
                    break;

                case "ZRANGEBYSCORE":
                    await HandleSortedSetRangeByScoreAsync(ctx, elements);
                    break;

                case "ZINCRBY":
                    await HandleSortedSetIncrementAsync(ctx, elements);
                    break;

                case "ZCOUNT":
                    await HandleSortedSetCountByScoreAsync(ctx, elements);
                    break;

                case "ZRANK":
                    await HandleSortedSetRankAsync(ctx, elements);
                    break;

                case "ZREVRANK":
                    await HandleSortedSetReverseRankAsync(ctx, elements);
                    break;

                case "ZREMRANGEBYSCORE":
                    await HandleSortedSetRemoveRangeByScoreAsync(ctx, elements);
                    break;

                case "VSET":
                    await HandleVectorSetAsync(ctx, elements);
                    break;

                case "VGET":
                    await HandleVectorGetAsync(ctx, elements);
                    break;

                case "VDIM":
                    await HandleVectorDimAsync(ctx, elements);
                    break;

                case "VDEL":
                    await HandleVectorDelAsync(ctx, elements);
                    break;

                case "VSIM":
                    await HandleVectorSimAsync(ctx, elements);
                    break;

                case "VSEARCH":
                    await HandleVectorSearchAsync(ctx, elements);
                    break;

                case "PUBLISH":
                    await HandlePublishAsync(ctx, elements);
                    break;

                case "SUBSCRIBE":
                    await HandleSubscribeAsync(ctx, elements);
                    break;

                case "UNSUBSCRIBE":
                    await HandleUnsubscribeAsync(ctx, elements);
                    break;

                case "PSUBSCRIBE":
                    await HandlePSubscribeAsync(ctx, elements);
                    break;

                case "PUNSUBSCRIBE":
                    await HandlePUnsubscribeAsync(ctx, elements);
                    break;

                case "MULTI":
                    HandleMulti(ctx, elements);
                    break;

                case "EXEC":
                    await HandleExecAsync(ctx, elements);
                    break;

                case "DISCARD":
                    HandleDiscard(ctx, elements);
                    break;

                case "WATCH":
                    HandleWatch(ctx, elements);
                    break;

                case "UNWATCH":
                    HandleUnwatch(ctx, elements);
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

                case "XTRIM":
                    await HandleXTrimAsync(ctx, elements);
                    break;

                case "XREAD":
                    await HandleXReadAsync(ctx, elements);
                    break;

                case "XRANGE":
                    await HandleXRangeAsync(ctx, elements);
                    break;

                case "XREVRANGE":
                    await HandleXRevRangeAsync(ctx, elements);
                    break;

                case "XSETID":
                    await HandleXSetIdAsync(ctx, elements);
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

                case "XINFO":
                    await HandleXInfoAsync(ctx, elements);
                    break;

                case "JSON.SET":
                    await HandleJsonSetAsync(ctx, elements);
                    break;

                case "JSON.GET":
                    await HandleJsonGetAsync(ctx, elements);
                    break;

                case "JSON.DEL":
                    await HandleJsonDelAsync(ctx, elements);
                    break;

                case "JSON.TYPE":
                    await HandleJsonTypeAsync(ctx, elements);
                    break;

                case "JSON.STRLEN":
                    await HandleJsonStrlenAsync(ctx, elements);
                    break;

                case "JSON.ARRLEN":
                    await HandleJsonArrlenAsync(ctx, elements);
                    break;

                case "JSON.ARRAPPEND":
                    await HandleJsonArrappendAsync(ctx, elements);
                    break;

                case "JSON.ARRINDEX":
                    await HandleJsonArrindexAsync(ctx, elements);
                    break;

                case "JSON.ARRINSERT":
                    await HandleJsonArrinsertAsync(ctx, elements);
                    break;

                case "JSON.ARRREM":
                    await HandleJsonArrremAsync(ctx, elements);
                    break;

                case "JSON.ARRTRIM":
                    await HandleJsonArrtrimAsync(ctx, elements);
                    break;

                case "JSON.MGET":
                    await HandleJsonMgetAsync(ctx, elements);
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
                // Notify transaction manager that this key was modified
                Transactions.NotifyKeyModified(key, _store);
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
        /// Handles LPUSH/RPUSH commands.
        /// </summary>
        private async Task HandleListPushAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args,
            bool left)
        {
            var commandName = left ? "lpush" : "rpush";
            if (args.Count < 3)
            {
                WriteError(ctx, $"ERR wrong number of arguments for '{commandName}' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var values = new byte[args.Count - 2][];
            for (int i = 2; i < args.Count; i++)
            {
                if (!TryGetBytes(args[i], out var value))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }

                values[i - 2] = value;
            }

            var result = await _store.ListPushAsync(key, values, left).ConfigureAwait(false);
            if (result.Status == ListResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            WriteInteger(ctx, result.Length);
        }

        /// <summary>
        /// Handles LPOP/RPOP commands.
        /// </summary>
        private async Task HandleListPopAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args,
            bool left)
        {
            var commandName = left ? "lpop" : "rpop";
            if (args.Count != 2)
            {
                WriteError(ctx, $"ERR wrong number of arguments for '{commandName}' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var result = await _store.ListPopAsync(key, left).ConfigureAwait(false);
            if (result.Status == ListResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            if (result.Value == null)
            {
                WriteNullBulkString(ctx);
                return;
            }

            WriteBulkString(ctx, result.Value);
        }

        /// <summary>
        /// Handles the LRANGE command.
        /// </summary>
        private async Task HandleListRangeAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 4)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'lrange' command");
                return;
            }

            if (!TryGetString(args[1], out var key) ||
                !TryGetString(args[2], out var startText) ||
                !TryGetString(args[3], out var stopText))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            if (!int.TryParse(startText, out var start) || !int.TryParse(stopText, out var stop))
            {
                WriteError(ctx, "ERR value is not an integer or out of range");
                return;
            }

            var result = await _store.ListRangeAsync(key, start, stop).ConfigureAwait(false);
            if (result.Status == ListResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            var children = new IRedisMessage[result.Values.Length];
            for (int i = 0; i < result.Values.Length; i++)
            {
                children[i] = new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(result.Values[i]));
            }

            WriteArray(ctx, children);
        }

        /// <summary>
        /// Handles the LLEN command.
        /// </summary>
        private async Task HandleListLengthAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'llen' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var result = await _store.ListLengthAsync(key).ConfigureAwait(false);
            if (result.Status == ListResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            WriteInteger(ctx, result.Length);
        }

        /// <summary>
        /// Handles the LINDEX command.
        /// </summary>
        private async Task HandleListIndexAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 3)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'lindex' command");
                return;
            }

            if (!TryGetString(args[1], out var key) ||
                !TryGetString(args[2], out var indexText))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            if (!int.TryParse(indexText, out var index))
            {
                WriteError(ctx, "ERR value is not an integer or out of range");
                return;
            }

            var result = await _store.ListIndexAsync(key, index).ConfigureAwait(false);
            if (result.Status == ListResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            if (result.Value == null)
            {
                WriteNullBulkString(ctx);
                return;
            }

            WriteBulkString(ctx, result.Value);
        }

        /// <summary>
        /// Handles the LSET command.
        /// </summary>
        private async Task HandleListSetAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 4)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'lset' command");
                return;
            }

            if (!TryGetString(args[1], out var key) ||
                !TryGetString(args[2], out var indexText) ||
                !TryGetBytes(args[3], out var value))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            if (!int.TryParse(indexText, out var index))
            {
                WriteError(ctx, "ERR value is not an integer or out of range");
                return;
            }

            var result = await _store.ListSetAsync(key, index, value).ConfigureAwait(false);
            switch (result.Status)
            {
                case ListSetResultStatus.WrongType:
                    WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                    return;
                case ListSetResultStatus.OutOfRange:
                    WriteError(ctx, "ERR index out of range");
                    return;
            }

            WriteSimpleString(ctx, "OK");
        }

        /// <summary>
        /// Handles the LTRIM command.
        /// </summary>
        private async Task HandleListTrimAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 4)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'ltrim' command");
                return;
            }

            if (!TryGetString(args[1], out var key) ||
                !TryGetString(args[2], out var startText) ||
                !TryGetString(args[3], out var stopText))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            if (!int.TryParse(startText, out var start) || !int.TryParse(stopText, out var stop))
            {
                WriteError(ctx, "ERR value is not an integer or out of range");
                return;
            }

            var status = await _store.ListTrimAsync(key, start, stop).ConfigureAwait(false);
            if (status == ListResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            WriteSimpleString(ctx, "OK");
        }

        /// <summary>
        /// Handles the SADD command.
        /// </summary>
        private async Task HandleSetAddAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count < 3)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'sadd' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var members = new byte[args.Count - 2][];
            for (int i = 2; i < args.Count; i++)
            {
                if (!TryGetBytes(args[i], out var value))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }

                members[i - 2] = value;
            }

            var result = await _store.SetAddAsync(key, members).ConfigureAwait(false);
            if (result.Status == SetResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            WriteInteger(ctx, result.Count);
        }

        /// <summary>
        /// Handles the SREM command.
        /// </summary>
        private async Task HandleSetRemoveAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count < 3)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'srem' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var members = new byte[args.Count - 2][];
            for (int i = 2; i < args.Count; i++)
            {
                if (!TryGetBytes(args[i], out var value))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }

                members[i - 2] = value;
            }

            var result = await _store.SetRemoveAsync(key, members).ConfigureAwait(false);
            if (result.Status == SetResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            WriteInteger(ctx, result.Count);
        }

        /// <summary>
        /// Handles the SMEMBERS command.
        /// </summary>
        private async Task HandleSetMembersAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'smembers' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var result = await _store.SetMembersAsync(key).ConfigureAwait(false);
            if (result.Status == SetResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            var children = new IRedisMessage[result.Members.Length];
            for (int i = 0; i < result.Members.Length; i++)
            {
                children[i] = new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(result.Members[i]));
            }

            WriteArray(ctx, children);
        }

        /// <summary>
        /// Handles the SCARD command.
        /// </summary>
        private async Task HandleSetCardinalityAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'scard' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var result = await _store.SetCardinalityAsync(key).ConfigureAwait(false);
            if (result.Status == SetResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            WriteInteger(ctx, result.Count);
        }

        /// <summary>
        /// Handles the ZADD command.
        /// </summary>
        private async Task HandleSortedSetAddAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count < 4 || (args.Count - 2) % 2 != 0)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'zadd' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var entryCount = (args.Count - 2) / 2;
            var entries = new SortedSetEntry[entryCount];
            int index = 0;
            for (int i = 2; i < args.Count; i += 2)
            {
                if (!TryGetString(args[i], out var scoreText))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }

                if (!double.TryParse(scoreText, NumberStyles.Float, CultureInfo.InvariantCulture, out var score) || double.IsNaN(score))
                {
                    WriteError(ctx, "ERR value is not a valid float");
                    return;
                }

                if (!TryGetBytes(args[i + 1], out var member))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }

                entries[index++] = new SortedSetEntry(member, score);
            }

            var result = await _store.SortedSetAddAsync(key, entries).ConfigureAwait(false);
            if (result.Status == SortedSetResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            WriteInteger(ctx, result.Count);
        }

        /// <summary>
        /// Handles the ZREM command.
        /// </summary>
        private async Task HandleSortedSetRemoveAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count < 3)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'zrem' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var members = new byte[args.Count - 2][];
            for (int i = 2; i < args.Count; i++)
            {
                if (!TryGetBytes(args[i], out var value))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }

                members[i - 2] = value;
            }

            var result = await _store.SortedSetRemoveAsync(key, members).ConfigureAwait(false);
            if (result.Status == SortedSetResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            WriteInteger(ctx, result.Count);
        }

        /// <summary>
        /// Handles the ZRANGE command.
        /// </summary>
        private async Task HandleSortedSetRangeAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 4 && args.Count != 5)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'zrange' command");
                return;
            }

            if (!TryGetString(args[1], out var key) ||
                !TryGetString(args[2], out var startText) ||
                !TryGetString(args[3], out var stopText))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            if (!int.TryParse(startText, out var start) || !int.TryParse(stopText, out var stop))
            {
                WriteError(ctx, "ERR value is not an integer or out of range");
                return;
            }

            bool withScores = false;
            if (args.Count == 5)
            {
                if (!TryGetString(args[4], out var option) ||
                    !string.Equals(option, "WITHSCORES", StringComparison.OrdinalIgnoreCase))
                {
                    WriteError(ctx, "ERR syntax error");
                    return;
                }

                withScores = true;
            }

            var result = await _store.SortedSetRangeAsync(key, start, stop).ConfigureAwait(false);
            if (result.Status == SortedSetResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            if (!withScores)
            {
                var children = new IRedisMessage[result.Entries.Length];
                for (int i = 0; i < result.Entries.Length; i++)
                {
                    children[i] = new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(result.Entries[i].Member));
                }

                WriteArray(ctx, children);
                return;
            }

            var withScoreChildren = new IRedisMessage[result.Entries.Length * 2];
            for (int i = 0; i < result.Entries.Length; i++)
            {
                withScoreChildren[i * 2] = new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(result.Entries[i].Member));
                var scoreText = result.Entries[i].Score.ToString("G", CultureInfo.InvariantCulture);
                withScoreChildren[i * 2 + 1] = new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(Utf8.GetBytes(scoreText)));
            }

            WriteArray(ctx, withScoreChildren);
        }

        /// <summary>
        /// Handles the ZCARD command.
        /// </summary>
        private async Task HandleSortedSetCardinalityAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'zcard' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var result = await _store.SortedSetCardinalityAsync(key).ConfigureAwait(false);
            if (result.Status == SortedSetResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            WriteInteger(ctx, result.Count);
        }

        private async Task HandleSortedSetScoreAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 3)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'zscore' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            if (!TryGetBytes(args[2], out var member))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var result = await _store.SortedSetScoreAsync(key, member).ConfigureAwait(false);
            if (result.Status == SortedSetResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            if (result.Score is null)
            {
                WriteNullBulkString(ctx);
                return;
            }

            WriteBulkString(ctx, Utf8.GetBytes(result.Score.Value.ToString("G17", CultureInfo.InvariantCulture)));
        }

        private async Task HandleSortedSetRangeByScoreAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count < 4)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'zrangebyscore' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            if (!TryGetString(args[2], out var minScoreStr) || !double.TryParse(minScoreStr, NumberStyles.Float, CultureInfo.InvariantCulture, out var minScore))
            {
                WriteError(ctx, "ERR min or max is not a float");
                return;
            }

            if (!TryGetString(args[3], out var maxScoreStr) || !double.TryParse(maxScoreStr, NumberStyles.Float, CultureInfo.InvariantCulture, out var maxScore))
            {
                WriteError(ctx, "ERR min or max is not a float");
                return;
            }

            bool withScores = false;
            if (args.Count >= 5)
            {
                if (TryGetString(args[4], out var option) && option.Equals("WITHSCORES", StringComparison.OrdinalIgnoreCase))
                {
                    withScores = true;
                }
            }

            var result = await _store.SortedSetRangeByScoreAsync(key, minScore, maxScore).ConfigureAwait(false);
            if (result.Status == SortedSetResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            if (!withScores)
            {
                var children = new IRedisMessage[result.Entries.Length];
                for (int i = 0; i < result.Entries.Length; i++)
                {
                    children[i] = new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(result.Entries[i].Member));
                }

                WriteArray(ctx, children);
                return;
            }

            var withScoreChildren = new IRedisMessage[result.Entries.Length * 2];
            for (int i = 0; i < result.Entries.Length; i++)
            {
                withScoreChildren[i * 2] = new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(result.Entries[i].Member));
                var scoreText = result.Entries[i].Score.ToString("G", CultureInfo.InvariantCulture);
                withScoreChildren[i * 2 + 1] = new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(Utf8.GetBytes(scoreText)));
            }

            WriteArray(ctx, withScoreChildren);
        }

        private async Task HandleSortedSetIncrementAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 4)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'zincrby' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            if (!TryGetString(args[2], out var incrementStr) || !double.TryParse(incrementStr, NumberStyles.Float, CultureInfo.InvariantCulture, out var increment))
            {
                WriteError(ctx, "ERR value is not a valid float");
                return;
            }

            if (!TryGetBytes(args[3], out var member))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var result = await _store.SortedSetIncrementAsync(key, increment, member).ConfigureAwait(false);
            if (result.Status == SortedSetResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            WriteBulkString(ctx, Utf8.GetBytes(result.Score!.Value.ToString("G17", CultureInfo.InvariantCulture)));
        }

        private async Task HandleSortedSetCountByScoreAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 4)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'zcount' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            if (!TryGetString(args[2], out var minScoreStr) || !double.TryParse(minScoreStr, NumberStyles.Float, CultureInfo.InvariantCulture, out var minScore))
            {
                WriteError(ctx, "ERR min or max is not a float");
                return;
            }

            if (!TryGetString(args[3], out var maxScoreStr) || !double.TryParse(maxScoreStr, NumberStyles.Float, CultureInfo.InvariantCulture, out var maxScore))
            {
                WriteError(ctx, "ERR min or max is not a float");
                return;
            }

            var result = await _store.SortedSetCountByScoreAsync(key, minScore, maxScore).ConfigureAwait(false);
            if (result.Status == SortedSetResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            WriteInteger(ctx, result.Count);
        }

        private async Task HandleSortedSetRankAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 3)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'zrank' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            if (!TryGetBytes(args[2], out var member))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var result = await _store.SortedSetRankAsync(key, member).ConfigureAwait(false);
            if (result.Status == SortedSetResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            if (result.Rank is null)
            {
                WriteNullBulkString(ctx);
            }
            else
            {
                WriteInteger(ctx, result.Rank.Value);
            }
        }

        private async Task HandleSortedSetReverseRankAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 3)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'zrevrank' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            if (!TryGetBytes(args[2], out var member))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var result = await _store.SortedSetReverseRankAsync(key, member).ConfigureAwait(false);
            if (result.Status == SortedSetResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            if (result.Rank is null)
            {
                WriteNullBulkString(ctx);
            }
            else
            {
                WriteInteger(ctx, result.Rank.Value);
            }
        }

        private async Task HandleSortedSetRemoveRangeByScoreAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 4)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'zremrangebyscore' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            if (!TryGetString(args[2], out var minScoreStr) || !double.TryParse(minScoreStr, NumberStyles.Float, CultureInfo.InvariantCulture, out var minScore))
            {
                WriteError(ctx, "ERR min or max is not a float");
                return;
            }

            if (!TryGetString(args[3], out var maxScoreStr) || !double.TryParse(maxScoreStr, NumberStyles.Float, CultureInfo.InvariantCulture, out var maxScore))
            {
                WriteError(ctx, "ERR min or max is not a float");
                return;
            }

            var result = await _store.SortedSetRemoveRangeByScoreAsync(key, minScore, maxScore).ConfigureAwait(false);
            if (result.Status == SortedSetResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            WriteInteger(ctx, result.Removed);
        }

        private async Task HandleVectorSetAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count < 3)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'vset' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var vector = new double[args.Count - 2];
            for (int i = 2; i < args.Count; i++)
            {
                if (!TryGetString(args[i], out var valueText) ||
                    !double.TryParse(valueText, NumberStyles.Float, CultureInfo.InvariantCulture, out var value) ||
                    double.IsNaN(value) ||
                    double.IsInfinity(value))
                {
                    WriteError(ctx, "ERR value is not a valid float");
                    return;
                }

                vector[i - 2] = value;
            }

            var result = await _store.VectorSetAsync(key, vector).ConfigureAwait(false);
            if (result.Status == VectorResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            WriteSimpleString(ctx, "OK");
            Transactions.NotifyKeyModified(key, _store);
        }

        private async Task HandleVectorGetAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'vget' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var result = await _store.VectorGetAsync(key).ConfigureAwait(false);
            if (result.Status == VectorResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            if (result.Status == VectorResultStatus.NotFound || result.Vector == null)
            {
                WriteNullBulkString(ctx);
                return;
            }

            var children = new IRedisMessage[result.Vector.Length];
            for (int i = 0; i < result.Vector.Length; i++)
            {
                var text = result.Vector[i].ToString("G17", CultureInfo.InvariantCulture);
                children[i] = new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(Utf8.GetBytes(text)));
            }

            WriteArray(ctx, children);
        }

        private async Task HandleVectorDimAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'vdim' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var result = await _store.VectorSizeAsync(key).ConfigureAwait(false);
            if (result.Status == VectorResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            WriteInteger(ctx, result.Size);
        }

        private async Task HandleVectorSimAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 3 && args.Count != 4)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'vsim' command");
                return;
            }

            if (!TryGetString(args[1], out var key) || !TryGetString(args[2], out var otherKey))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var metric = "COSINE";
            if (args.Count == 4)
            {
                if (!TryGetString(args[3], out metric))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }
            }

            var result = await _store.VectorSimilarityAsync(key, otherKey, metric).ConfigureAwait(false);
            if (result.Status == VectorResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            if (result.Status == VectorResultStatus.InvalidArgument)
            {
                WriteError(ctx, "ERR invalid vector operation");
                return;
            }

            if (result.Status == VectorResultStatus.NotFound || !result.Value.HasValue)
            {
                WriteNullBulkString(ctx);
                return;
            }

            WriteBulkString(ctx, Utf8.GetBytes(result.Value.Value.ToString("G17", CultureInfo.InvariantCulture)));
        }

        private async Task HandleVectorDelAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'vdel' command");
                return;
            }

            if (!TryGetString(args[1], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var result = await _store.VectorDeleteAsync(key).ConfigureAwait(false);
            if (result.Status == VectorResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            WriteInteger(ctx, result.Deleted);
            if (result.Deleted > 0)
            {
                Transactions.NotifyKeyModified(key, _store);
            }
        }

        private async Task HandleVectorSearchAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count < 5)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'vsearch' command");
                return;
            }

            if (!TryGetString(args[1], out var keyPrefix))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            if (!TryGetString(args[2], out var token2))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            int topK;
            int offset = 0;
            string metric;
            int index;
            bool positionalForm;

            if (int.TryParse(token2, out var positionalTopK))
            {
                if (positionalTopK <= 0)
                {
                    WriteError(ctx, "ERR value is not an integer or out of range");
                    return;
                }

                topK = positionalTopK;
                if (!TryGetString(args[3], out metric))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }

                index = 4;
                positionalForm = true;
            }
            else
            {
                metric = token2;
                topK = -1;
                index = 3;
                positionalForm = false;
            }

            if (positionalForm)
            {
                if (index + 1 < args.Count && TryGetString(args[index], out var option))
                {
                    if (option.Equals("LIMIT", StringComparison.OrdinalIgnoreCase))
                    {
                        WriteError(ctx, "ERR syntax error");
                        return;
                    }

                    if (option.Equals("OFFSET", StringComparison.OrdinalIgnoreCase))
                    {
                        if (!TryGetString(args[index + 1], out var offsetText) || !int.TryParse(offsetText, out offset) || offset < 0)
                        {
                            WriteError(ctx, "ERR value is not an integer or out of range");
                            return;
                        }

                        index += 2;
                    }
                }
            }
            else
            {
                if (args.Count < 4)
                {
                    WriteError(ctx, "ERR wrong number of arguments for 'vsearch' command");
                    return;
                }

                if (!TryGetString(args[index], out var limitToken) ||
                    !limitToken.Equals("LIMIT", StringComparison.OrdinalIgnoreCase))
                {
                    WriteError(ctx, "ERR LIMIT is required");
                    return;
                }

                if (!TryGetString(args[index + 1], out var limitText) || !int.TryParse(limitText, out topK) || topK <= 0)
                {
                    WriteError(ctx, "ERR value is not an integer or out of range");
                    return;
                }

                index += 2;

                if (index + 1 < args.Count && TryGetString(args[index], out var offsetToken) &&
                    offsetToken.Equals("OFFSET", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryGetString(args[index + 1], out var offsetText) || !int.TryParse(offsetText, out offset) || offset < 0)
                    {
                        WriteError(ctx, "ERR value is not an integer or out of range");
                        return;
                    }

                    index += 2;
                }
            }

            if (index < args.Count && TryGetString(args[index], out var unexpectedToken) &&
                (unexpectedToken.Equals("LIMIT", StringComparison.OrdinalIgnoreCase) ||
                 unexpectedToken.Equals("OFFSET", StringComparison.OrdinalIgnoreCase)))
            {
                WriteError(ctx, "ERR syntax error");
                return;
            }

            if (args.Count <= index)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'vsearch' command");
                return;
            }

            var queryVector = new double[args.Count - index];
            for (int i = index; i < args.Count; i++)
            {
                if (!TryGetString(args[i], out var valueText) ||
                    !double.TryParse(valueText, NumberStyles.Float, CultureInfo.InvariantCulture, out var value) ||
                    double.IsNaN(value) ||
                    double.IsInfinity(value))
                {
                    WriteError(ctx, "ERR value is not a valid float");
                    return;
                }

                queryVector[i - index] = value;
            }

            var result = await _store.VectorSearchAsync(keyPrefix, topK, offset, metric, queryVector).ConfigureAwait(false);
            if (result.Status == VectorResultStatus.WrongType)
            {
                WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            if (result.Status == VectorResultStatus.InvalidArgument)
            {
                WriteError(ctx, "ERR invalid vector operation");
                return;
            }

            var children = new IRedisMessage[result.Entries.Length * 2];
            for (int i = 0; i < result.Entries.Length; i++)
            {
                children[i * 2] = new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(Utf8.GetBytes(result.Entries[i].Key)));
                var scoreText = result.Entries[i].Score.ToString("G17", CultureInfo.InvariantCulture);
                children[i * 2 + 1] = new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(Utf8.GetBytes(scoreText)));
            }

            WriteArray(ctx, children);
        }

        private async Task HandlePublishAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 3)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'publish' command");
                return;
            }

            if (!TryGetString(args[1], out var channel))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            if (!TryGetBytes(args[2], out var message))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var subscribers = PubSub.Publish(channel, message);
            WriteInteger(ctx, subscribers);
        }

        private Task HandleSubscribeAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count < 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'subscribe' command");
                return Task.CompletedTask;
            }

            var channels = new List<string>();
            for (int i = 1; i < args.Count; i++)
            {
                if (!TryGetString(args[i], out var channel))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return Task.CompletedTask;
                }

                channels.Add(channel);
            }

            // Subscribe to all channels
            for (int i = 0; i < channels.Count; i++)
            {
                var channel = channels[i];
                PubSub.Subscribe(ctx, channel);
                
                // Send subscription confirmation: ["subscribe", channel, subscription_count]
                // subscription_count is the total number of channels/patterns this client is now subscribed to
                var subscriptionCount = PubSub.GetSubscriptionCount(ctx);
                var subscriptionMsg = new IRedisMessage[]
                {
                    new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(Utf8.GetBytes("subscribe"))),
                    new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(Utf8.GetBytes(channel))),
                    new IntegerRedisMessage(subscriptionCount)
                };
                ctx.WriteAndFlushAsync(new ArrayRedisMessage(subscriptionMsg));
            }

            return Task.CompletedTask;
        }

        private Task HandleUnsubscribeAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            var channels = new List<string>();
            
            // If args.Count == 1, unsubscribe from all channels
            if (args.Count == 1)
            {
                // Will be populated by PubSub.GetChannelsToUnsubscribe
            }
            else
            {
                for (int i = 1; i < args.Count; i++)
                {
                    if (!TryGetString(args[i], out var channel))
                    {
                        WriteError(ctx, "ERR null bulk string");
                        return Task.CompletedTask;
                    }

                    channels.Add(channel);
                }
            }

            // Get channels to unsubscribe
            var channelsToUnsubscribe = PubSub.GetChannelsToUnsubscribe(ctx, channels.ToArray());
            
            // Unsubscribe from each channel one at a time and send confirmation
            foreach (var channel in channelsToUnsubscribe)
            {
                PubSub.UnsubscribeOne(ctx, channel);
                var subscriptionCount = PubSub.GetSubscriptionCount(ctx);
                var unsubscriptionMsg = new IRedisMessage[]
                {
                    new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(Utf8.GetBytes("unsubscribe"))),
                    new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(Utf8.GetBytes(channel))),
                    new IntegerRedisMessage(subscriptionCount)
                };
                ctx.WriteAndFlushAsync(new ArrayRedisMessage(unsubscriptionMsg));
            }

            // If no channels were provided and none existed, still send one response
            if (args.Count == 1 && channelsToUnsubscribe.Length == 0)
            {
                var unsubscriptionMsg = new IRedisMessage[]
                {
                    new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(Utf8.GetBytes("unsubscribe"))),
                    FullBulkStringRedisMessage.Null,
                    new IntegerRedisMessage(0)
                };
                ctx.WriteAndFlushAsync(new ArrayRedisMessage(unsubscriptionMsg));
            }

            return Task.CompletedTask;
        }

        private Task HandlePSubscribeAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count < 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'psubscribe' command");
                return Task.CompletedTask;
            }

            var patterns = new List<string>();
            for (int i = 1; i < args.Count; i++)
            {
                if (!TryGetString(args[i], out var pattern))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return Task.CompletedTask;
                }

                patterns.Add(pattern);
            }

            // Subscribe to all patterns
            for (int i = 0; i < patterns.Count; i++)
            {
                var pattern = patterns[i];
                PubSub.PSubscribe(ctx, pattern);
                
                var subscriptionCount = PubSub.GetSubscriptionCount(ctx);
                var subscriptionMsg = new IRedisMessage[]
                {
                    new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(Utf8.GetBytes("psubscribe"))),
                    new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(Utf8.GetBytes(pattern))),
                    new IntegerRedisMessage(subscriptionCount)
                };
                ctx.WriteAndFlushAsync(new ArrayRedisMessage(subscriptionMsg));
            }

            return Task.CompletedTask;
        }

        private Task HandlePUnsubscribeAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            var patterns = new List<string>();
            
            // If args.Count == 1, unsubscribe from all patterns
            if (args.Count == 1)
            {
                // Will be populated by PubSub.GetPatternsToUnsubscribe
            }
            else
            {
                for (int i = 1; i < args.Count; i++)
                {
                    if (!TryGetString(args[i], out var pattern))
                    {
                        WriteError(ctx, "ERR null bulk string");
                        return Task.CompletedTask;
                    }

                    patterns.Add(pattern);
                }
            }

            // Get patterns to unsubscribe
            var patternsToUnsubscribe = PubSub.GetPatternsToUnsubscribe(ctx, patterns.ToArray());
            
            // Unsubscribe from each pattern one at a time and send confirmation
            foreach (var pattern in patternsToUnsubscribe)
            {
                PubSub.PUnsubscribeOne(ctx, pattern);
                var subscriptionCount = PubSub.GetSubscriptionCount(ctx);
                var unsubscriptionMsg = new IRedisMessage[]
                {
                    new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(Utf8.GetBytes("punsubscribe"))),
                    new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(Utf8.GetBytes(pattern))),
                    new IntegerRedisMessage(subscriptionCount)
                };
                ctx.WriteAndFlushAsync(new ArrayRedisMessage(unsubscriptionMsg));
            }

            // If no patterns were provided and none existed, still send one response
            if (args.Count == 1 && patternsToUnsubscribe.Length == 0)
            {
                var unsubscriptionMsg = new IRedisMessage[]
                {
                    new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(Utf8.GetBytes("punsubscribe"))),
                    FullBulkStringRedisMessage.Null,
                    new IntegerRedisMessage(0)
                };
                ctx.WriteAndFlushAsync(new ArrayRedisMessage(unsubscriptionMsg));
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// Handles the MULTI command - starts a transaction.
        /// </summary>
        private void HandleMulti(IChannelHandlerContext ctx, IList<IRedisMessage> args)
        {
            if (args.Count != 1)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'multi' command");
                return;
            }

            var txState = Transactions.GetOrCreateState(ctx);
            if (txState.InTransaction)
            {
                WriteError(ctx, "ERR MULTI calls can not be nested");
                return;
            }

            txState.InTransaction = true;
            txState.CommandQueue.Clear();
            WriteSimpleString(ctx, "OK");
        }

        /// <summary>
        /// Handles the EXEC command - executes all queued commands in a transaction.
        /// </summary>
        private async Task HandleExecAsync(IChannelHandlerContext ctx, IList<IRedisMessage> args)
        {
            try
            {
                if (args.Count != 1)
                {
                    WriteError(ctx, "ERR wrong number of arguments for 'exec' command");
                    return;
                }

                var txState = Transactions.GetOrCreateState(ctx);
                if (!txState.InTransaction)
                {
                    WriteError(ctx, "ERR EXEC without MULTI");
                    return;
                }

                // Check if any watched key was modified
                if (txState.WatchedKeyModified)
                {
                    txState.Clear();
                    txState.ClearWatchedKeys();
                    // Return null bulk string array to indicate transaction was aborted
                    await ctx.WriteAndFlushAsync(FullBulkStringRedisMessage.Null);
                    return;
                }

                // Execute all queued commands
                var results = new List<IRedisMessage>();
                
                // Temporarily disable transaction mode to execute commands
                txState.InTransaction = false;
                
                try
                {
                    foreach (var command in txState.CommandQueue)
                    {
                        var result = await ExecuteQueuedCommandAsync(command);
                        results.Add(result);
                    }
                }
                finally
                {
                    // Release retained messages
                    foreach (var command in txState.CommandQueue)
                    {
                        command.Release();
                    }
                    
                    // Clear transaction state
                    txState.Clear();
                    txState.ClearWatchedKeys();
                }

                // Send array of results
                await ctx.WriteAndFlushAsync(new ArrayRedisMessage(results.ToArray()));
            }
            catch (Exception ex)
            {
                // Log the exception and return an error
                WriteError(ctx, $"ERR transaction execution failed: {ex.Message}");
            }
        }

        /// <summary>
        /// Executes a queued command and returns its result.
        /// </summary>
        private async Task<IRedisMessage> ExecuteQueuedCommandAsync(IArrayRedisMessage command)
        {
            // Create a capturing context and handler to execute the command
            var capturingCtx = new CapturingContext();
            var handler = new DredisCommandHandler(_store);
            
            // Call HandleCommandAsync directly and await it
            await handler.HandleCommandAsync(capturingCtx, command);
            
            // Return the captured response
            return capturingCtx.CapturedMessage ?? new SimpleStringRedisMessage("OK");
        }

        /// <summary>
        /// Handles the DISCARD command - cancels a transaction.
        /// </summary>
        private void HandleDiscard(IChannelHandlerContext ctx, IList<IRedisMessage> args)
        {
            if (args.Count != 1)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'discard' command");
                return;
            }

            var txState = Transactions.GetOrCreateState(ctx);
            if (!txState.InTransaction)
            {
                WriteError(ctx, "ERR DISCARD without MULTI");
                return;
            }

            // Release retained messages
            foreach (var command in txState.CommandQueue)
            {
                command.Release();
            }
            
            txState.Clear();
            WriteSimpleString(ctx, "OK");
        }

        /// <summary>
        /// Handles the WATCH command - marks keys to be watched for modification.
        /// </summary>
        private void HandleWatch(IChannelHandlerContext ctx, IList<IRedisMessage> args)
        {
            if (args.Count < 2)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'watch' command");
                return;
            }

            var txState = Transactions.GetOrCreateState(ctx);
            if (txState.InTransaction)
            {
                WriteError(ctx, "ERR WATCH inside MULTI is not allowed");
                return;
            }

            // Add keys to watch list with their current hash
            for (int i = 1; i < args.Count; i++)
            {
                if (!TryGetString(args[i], out var key))
                {
                    WriteError(ctx, "ERR null bulk string");
                    return;
                }

                var hash = Transactions.GetOrCreateState(ctx).WatchedKeys.ContainsKey(key) 
                    ? txState.WatchedKeys[key] 
                    : key.GetHashCode();
                txState.WatchedKeys[key] = hash;
            }

            WriteSimpleString(ctx, "OK");
        }

        /// <summary>
        /// Handles the UNWATCH command - stops watching all keys.
        /// </summary>
        private void HandleUnwatch(IChannelHandlerContext ctx, IList<IRedisMessage> args)
        {
            if (args.Count != 1)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'unwatch' command");
                return;
            }

            var txState = Transactions.GetOrCreateState(ctx);
            txState.ClearWatchedKeys();
            WriteSimpleString(ctx, "OK");
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
        /// Handles the XTRIM command.
        /// </summary>
        private async Task HandleXTrimAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count < 4)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'xtrim' command");
                return;
            }

            if (!TryGetString(args[1], out var key) || !TryGetString(args[2], out var strategy))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var approx = false;
            int index = 3;

            if (index < args.Count && TryGetString(args[index], out var modifier) &&
                (modifier == "~" || modifier == "="))
            {
                approx = modifier == "~";
                index++;
            }

            if (index >= args.Count)
            {
                WriteError(ctx, "ERR syntax error");
                return;
            }

            if (string.Equals(strategy, "MAXLEN", StringComparison.OrdinalIgnoreCase))
            {
                if (!TryGetString(args[index], out var lenText) ||
                    !int.TryParse(lenText, out var maxLen) || maxLen < 0 || index + 1 != args.Count)
                {
                    WriteError(ctx, "ERR syntax error");
                    return;
                }

                var removed = await _store.StreamTrimAsync(key, maxLength: maxLen, approximate: approx)
                    .ConfigureAwait(false);
                WriteInteger(ctx, removed);
                return;
            }

            if (string.Equals(strategy, "MINID", StringComparison.OrdinalIgnoreCase))
            {
                if (!TryGetString(args[index], out var minId) ||
                    !TryParseStreamIdText(minId) || index + 1 != args.Count)
                {
                    WriteError(ctx, "ERR syntax error");
                    return;
                }

                var removed = await _store.StreamTrimAsync(key, minId: minId, approximate: approx)
                    .ConfigureAwait(false);
                WriteInteger(ctx, removed);
                return;
            }

            WriteError(ctx, "ERR syntax error");
        }

        /// <summary>
        /// Handles the XREAD command.
        /// </summary>
        /// <remarks>
        /// When BLOCK is used, "$" ids are resolved once to preserve last-seen semantics.
        /// </remarks>
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
            TimeSpan? block = null;

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
                    if (index + 1 >= args.Count || !TryGetString(args[index + 1], out var blockText) ||
                        !long.TryParse(blockText, out var blockMs) || blockMs < 0)
                    {
                        WriteError(ctx, "ERR invalid block");
                        return;
                    }

                    block = TimeSpan.FromMilliseconds(blockMs);
                    index += 2;
                    continue;
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

            if (block.HasValue)
            {
                // For BLOCK requests, snapshot "$" to a concrete id per stream key in `keys`.
                for (int i = 0; i < ids.Length; i++)
                {
                    if (ids[i] == "$")
                    {
                        var lastId = await _store.StreamLastIdAsync(keys[i]).ConfigureAwait(false);
                        ids[i] = lastId ?? "0-0";
                    }
                }
            }

            var results = await _store.StreamReadAsync(keys, ids, count).ConfigureAwait(false);
            var streamMessages = BuildStreamMessages(results);

            if (streamMessages.Count == 0 && block.HasValue)
            {
                if (block.Value > TimeSpan.Zero)
                {
                    await Task.Delay(block.Value).ConfigureAwait(false);
                }

                results = await _store.StreamReadAsync(keys, ids, count).ConfigureAwait(false);
                streamMessages = BuildStreamMessages(results);

                if (streamMessages.Count == 0)
                {
                    WriteNullArray(ctx);
                    return;
                }
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
        /// Handles the XREVRANGE command.
        /// </summary>
        private async Task HandleXRevRangeAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 4 && args.Count != 6)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'xrevrange' command");
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

            var entries = await _store.StreamRangeReverseAsync(key, start, end, count).ConfigureAwait(false);
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

                case "SETID":
                    await HandleXGroupSetIdAsync(ctx, args);
                    break;

                case "DELCONSUMER":
                    await HandleXGroupDelConsumerAsync(ctx, args);
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
        /// Handles the XGROUP SETID subcommand.
        /// </summary>
        private async Task HandleXGroupSetIdAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 5)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'xgroup setid' command");
                return;
            }

            if (!TryGetString(args[2], out var key) ||
                !TryGetString(args[3], out var group) ||
                !TryGetString(args[4], out var id))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            if (!IsGroupCreateId(id))
            {
                WriteError(ctx, "ERR invalid stream id");
                return;
            }

            var result = await _store.StreamGroupSetIdAsync(key, group, id).ConfigureAwait(false);
            switch (result)
            {
                case StreamGroupSetIdResultStatus.WrongType:
                    WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                    return;
                case StreamGroupSetIdResultStatus.NoStream:
                    WriteError(ctx, "ERR The XGROUP subcommand requires the key to exist");
                    return;
                case StreamGroupSetIdResultStatus.NoGroup:
                    WriteError(ctx, "NOGROUP No such consumer group");
                    return;
                case StreamGroupSetIdResultStatus.InvalidId:
                    WriteError(ctx, "ERR invalid stream id");
                    return;
            }

            WriteSimpleString(ctx, "OK");
        }

        /// <summary>
        /// Handles the XGROUP DELCONSUMER subcommand.
        /// </summary>
        private async Task HandleXGroupDelConsumerAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 5)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'xgroup delconsumer' command");
                return;
            }

            if (!TryGetString(args[2], out var key) ||
                !TryGetString(args[3], out var group) ||
                !TryGetString(args[4], out var consumer))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            var result = await _store.StreamGroupDelConsumerAsync(key, group, consumer).ConfigureAwait(false);
            switch (result.Status)
            {
                case StreamGroupDelConsumerResultStatus.WrongType:
                    WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                    return;
                case StreamGroupDelConsumerResultStatus.NoStream:
                    WriteError(ctx, "ERR The XGROUP subcommand requires the key to exist");
                    return;
                case StreamGroupDelConsumerResultStatus.NoGroup:
                    WriteError(ctx, "NOGROUP No such consumer group");
                    return;
            }

            WriteInteger(ctx, result.Removed);
        }

        /// <summary>
        /// Handles the XSETID command.
        /// </summary>
        private async Task HandleXSetIdAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count != 3)
            {
                WriteError(ctx, "ERR wrong number of arguments for 'xsetid' command");
                return;
            }

            if (!TryGetString(args[1], out var key) || !TryGetString(args[2], out var id))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            if (!TryParseStreamIdText(id))
            {
                WriteError(ctx, "ERR invalid stream id");
                return;
            }

            var result = await _store.StreamSetIdAsync(key, id).ConfigureAwait(false);
            switch (result)
            {
                case StreamSetIdResultStatus.WrongType:
                    WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                    return;
                case StreamSetIdResultStatus.InvalidId:
                    WriteError(ctx, "ERR invalid stream id");
                    return;
            }

            WriteSimpleString(ctx, "OK");
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
        /// Handles the XINFO command.
        /// </summary>
        private async Task HandleXInfoAsync(
            IChannelHandlerContext ctx,
            IList<IRedisMessage> args)
        {
            if (args.Count < 3 || !TryGetString(args[1], out var subcommand))
            {
                WriteError(ctx, "ERR wrong number of arguments for 'xinfo' command");
                return;
            }

            if (!TryGetString(args[2], out var key))
            {
                WriteError(ctx, "ERR null bulk string");
                return;
            }

            switch (subcommand.ToUpperInvariant())
            {
                case "STREAM":
                    await HandleXInfoStreamAsync(ctx, key).ConfigureAwait(false);
                    break;

                case "GROUPS":
                    await HandleXInfoGroupsAsync(ctx, key).ConfigureAwait(false);
                    break;

                case "CONSUMERS":
                    if (args.Count != 4 || !TryGetString(args[3], out var group))
                    {
                        WriteError(ctx, "ERR wrong number of arguments for 'xinfo' command");
                        return;
                    }
                    await HandleXInfoConsumersAsync(ctx, key, group).ConfigureAwait(false);
                    break;

                default:
                    WriteError(ctx, "ERR unknown subcommand or wrong number of arguments for 'xinfo' command");
                    break;
            }
        }

        private async Task HandleXInfoStreamAsync(IChannelHandlerContext ctx, string key)
        {
            var result = await _store.StreamInfoAsync(key).ConfigureAwait(false);
            switch (result.Status)
            {
                case StreamInfoResultStatus.WrongType:
                    WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                    return;
                case StreamInfoResultStatus.NoStream:
                    WriteError(ctx, "ERR no such key");
                    return;
            }

            var info = result.Info;
            var items = new List<IRedisMessage>
            {
                BulkString("length"),
                new IntegerRedisMessage(info?.Length ?? 0),
                BulkString("last-generated-id"),
                info?.LastGeneratedId == null ? FullBulkStringRedisMessage.Null : BulkString(info.LastGeneratedId),
                BulkString("first-entry"),
                info?.FirstEntry == null ? FullBulkStringRedisMessage.Null : BuildStreamEntryMessage(info.FirstEntry),
                BulkString("last-entry"),
                info?.LastEntry == null ? FullBulkStringRedisMessage.Null : BuildStreamEntryMessage(info.LastEntry)
            };

            await ctx.WriteAndFlushAsync(new ArrayRedisMessage(items));
        }

        private async Task HandleXInfoGroupsAsync(IChannelHandlerContext ctx, string key)
        {
            var result = await _store.StreamGroupsInfoAsync(key).ConfigureAwait(false);
            switch (result.Status)
            {
                case StreamInfoResultStatus.WrongType:
                    WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                    return;
                case StreamInfoResultStatus.NoStream:
                    WriteError(ctx, "ERR no such key");
                    return;
            }

            var groups = new List<IRedisMessage>();
            foreach (var group in result.Groups)
            {
                var groupItems = new List<IRedisMessage>
                {
                    BulkString("name"),
                    BulkString(group.Name),
                    BulkString("consumers"),
                    new IntegerRedisMessage(group.Consumers),
                    BulkString("pending"),
                    new IntegerRedisMessage(group.Pending),
                    BulkString("last-delivered-id"),
                    BulkString(group.LastDeliveredId)
                };
                groups.Add(new ArrayRedisMessage(groupItems));
            }

            await ctx.WriteAndFlushAsync(new ArrayRedisMessage(groups));
        }

        private async Task HandleXInfoConsumersAsync(IChannelHandlerContext ctx, string key, string group)
        {
            var result = await _store.StreamConsumersInfoAsync(key, group).ConfigureAwait(false);
            switch (result.Status)
            {
                case StreamInfoResultStatus.WrongType:
                    WriteError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
                    return;
                case StreamInfoResultStatus.NoStream:
                    WriteError(ctx, "ERR no such key");
                    return;
                case StreamInfoResultStatus.NoGroup:
                    WriteError(ctx, "NOGROUP No such consumer group");
                    return;
            }

            var consumers = new List<IRedisMessage>();
            foreach (var consumer in result.Consumers)
            {
                var consumerItems = new List<IRedisMessage>
                {
                    BulkString("name"),
                    BulkString(consumer.Name),
                    BulkString("pending"),
                    new IntegerRedisMessage(consumer.Pending),
                    BulkString("idle"),
                    new IntegerRedisMessage(consumer.IdleTimeMs)
                };
                consumers.Add(new ArrayRedisMessage(consumerItems));
            }

            await ctx.WriteAndFlushAsync(new ArrayRedisMessage(consumers));
        }

        /// <summary>
        /// Builds RESP stream messages from stream read results.
        /// </summary>
        private static List<IRedisMessage> BuildStreamMessages(StreamReadResult[] results)
        {
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

            return streamMessages;
        }

        private static IRedisMessage BuildStreamEntryMessage(StreamEntry entry)
        {
            var fieldChildren = new IRedisMessage[entry.Fields.Length * 2];
            for (int i = 0; i < entry.Fields.Length; i++)
            {
                var fieldBytes = Utf8.GetBytes(entry.Fields[i].Key);
                fieldChildren[i * 2] = new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(fieldBytes));
                fieldChildren[i * 2 + 1] = new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(entry.Fields[i].Value));
            }

            var entryChildren = new IRedisMessage[2]
            {
                BulkString(entry.Id),
                new ArrayRedisMessage(fieldChildren)
            };

            return new ArrayRedisMessage(entryChildren);
        }

        private static IRedisMessage BulkString(string value)
        {
            return new FullBulkStringRedisMessage(Unpooled.WrappedBuffer(Utf8.GetBytes(value)));
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