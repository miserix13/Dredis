using System.Net;
using DotNetty.Codecs.Redis;
using DotNetty.Transport.Bootstrapping;
using DotNetty.Transport.Channels;
using DotNetty.Transport.Channels.Sockets;
using Dredis.Abstractions.Storage;

namespace Dredis
{
    public sealed class DredisServer
    {
        private readonly IKeyValueStore _store;
        private readonly SemaphoreSlim _lifecycleLock = new(1, 1);
        private IEventLoopGroup? _bossGroup;
        private IEventLoopGroup? _workerGroup;
        private IChannel? _channel;

        public DredisServer(IKeyValueStore store)
        {
            _store = store;
        }

        public async Task RunAsync(int port, CancellationToken token = default)
        {
            await StartAsync(port, token);

            IChannel? channel;
            await _lifecycleLock.WaitAsync(token);
            try
            {
                channel = _channel;
            }
            finally
            {
                _lifecycleLock.Release();
            }

            if (channel == null)
            {
                return;
            }

            try
            {
                using (token.Register(() => _ = StopAsync()))
                {
                    await channel.CloseCompletion;
                }
            }
            finally
            {
                await StopAsync();
            }
        }

        public async Task StartAsync(int port, CancellationToken token = default)
        {
            await _lifecycleLock.WaitAsync(token);

            IEventLoopGroup? bossGroup = null;
            IEventLoopGroup? workerGroup = null;
            IChannel? channel = null;

            try
            {
                if (_channel != null)
                {
                    throw new InvalidOperationException("Server is already running.");
                }

                bossGroup = new MultithreadEventLoopGroup(1);
                workerGroup = new MultithreadEventLoopGroup();

                var bootstrap = new ServerBootstrap()
                    .Group(bossGroup, workerGroup)
                    .Channel<TcpServerSocketChannel>()
                    .ChildHandler(new ActionChannelInitializer<IChannel>(ch =>
                    {
                        var p = ch.Pipeline;

                        // RESP codec (bytes <-> IRedisMessage)
                        p.AddLast("redisDecoder", new RedisDecoder());
                        p.AddLast("redisBulkStringAggregator", new RedisBulkStringAggregator());
                        p.AddLast("redisArrayAggregator", new RedisArrayAggregator());
                        p.AddLast("redisEncoder", new RedisEncoder());

                        // Your abstraction bridge
                        p.AddLast("dredisHandler", new DredisCommandHandler(_store));
                    }));

                channel = await bootstrap.BindAsync(IPAddress.Loopback, port);

                _bossGroup = bossGroup;
                _workerGroup = workerGroup;
                _channel = channel;
            }
            catch
            {
                if (channel != null)
                {
                    await channel.CloseAsync();
                }

                await ShutdownGroupsAsync(bossGroup, workerGroup);
                throw;
            }
            finally
            {
                _lifecycleLock.Release();
            }
        }

        public async Task StopAsync()
        {
            IEventLoopGroup? bossGroup;
            IEventLoopGroup? workerGroup;
            IChannel? channel;

            await _lifecycleLock.WaitAsync();
            try
            {
                bossGroup = _bossGroup;
                workerGroup = _workerGroup;
                channel = _channel;

                _bossGroup = null;
                _workerGroup = null;
                _channel = null;
            }
            finally
            {
                _lifecycleLock.Release();
            }

            if (channel != null)
            {
                await channel.CloseAsync();
            }

            await ShutdownGroupsAsync(bossGroup, workerGroup);
        }

        private static Task ShutdownGroupsAsync(IEventLoopGroup? bossGroup, IEventLoopGroup? workerGroup)
        {
            return Task.WhenAll(
                bossGroup?.ShutdownGracefullyAsync() ?? Task.CompletedTask,
                workerGroup?.ShutdownGracefullyAsync() ?? Task.CompletedTask);
        }
    }
}