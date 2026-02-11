using System.Net;
using DotNetty.Codecs.Redis;
using DotNetty.Transport.Bootstrapping;
using DotNetty.Transport.Channels;
using DotNetty.Transport.Channels.Sockets;

namespace Dredis
{
    public sealed class DredisServer
    {
        private readonly IKeyValueStore _store;

        public DredisServer(IKeyValueStore store)
        {
            _store = store;
        }

        public async Task RunAsync(int port, CancellationToken token = default)
        {
            IEventLoopGroup bossGroup = new MultithreadEventLoopGroup(1);
            IEventLoopGroup workerGroup = new MultithreadEventLoopGroup();

            try
            {
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

                var channel = await bootstrap.BindAsync(IPAddress.Loopback, port);

                using (token.Register(() => channel.CloseAsync()))
                {
                    await channel.CloseCompletion;
                }
            }
            finally
            {
                await Task.WhenAll(
                    bossGroup.ShutdownGracefullyAsync(),
                    workerGroup.ShutdownGracefullyAsync());
            }
        }
    }
}