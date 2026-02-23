using System.Net;
using DotNetty.Codecs.Redis;
using DotNetty.Transport.Bootstrapping;
using DotNetty.Transport.Channels;
using DotNetty.Transport.Channels.Sockets;
using Dredis.Abstractions.Command;
using Dredis.Abstractions.Storage;
using Microsoft.Extensions.Configuration;

namespace Dredis
{
    /// <summary>
    /// Hosts the Dredis server and manages the DotNetty pipeline lifecycle.
    /// </summary>
    public sealed class DredisServer
    {
        private readonly IKeyValueStore _store;
        private readonly DredisServerOptions _options;
        private readonly SemaphoreSlim _lifecycleLock = new(1, 1);
        private readonly Dictionary<string, ICommand> _commands = new(StringComparer.OrdinalIgnoreCase);
        private readonly object _commandsLock = new();
        private IEventLoopGroup? _bossGroup;
        private IEventLoopGroup? _workerGroup;
        private IChannel? _channel;

        /// <summary>
        /// Initializes a new instance of the <see cref="DredisServer"/> class.
        /// </summary>
        /// <param name="store">The storage abstraction used for command handling.</param>
        public DredisServer(IKeyValueStore store)
            : this(store, new DredisServerOptions())
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DredisServer"/> class.
        /// </summary>
        /// <param name="store">The storage abstraction used for command handling.</param>
        /// <param name="options">The server options.</param>
        public DredisServer(IKeyValueStore store, DredisServerOptions options)
        {
            _store = store ?? throw new ArgumentNullException(nameof(store));
            DredisServerOptions.Validate(options);
            _options = options.Clone();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DredisServer"/> class using configuration binding.
        /// </summary>
        /// <param name="store">The storage abstraction used for command handling.</param>
        /// <param name="configuration">The configuration root.</param>
        /// <param name="sectionName">The section containing server options. Defaults to <c>DredisServer</c>.</param>
        public DredisServer(IKeyValueStore store, IConfiguration configuration, string sectionName = "DredisServer")
            : this(store, DredisServerOptions.FromConfiguration(configuration, sectionName))
        {
        }

        /// <summary>
        /// Registers custom commands that will be applied to command handlers for incoming connections.
        /// </summary>
        /// <param name="commands">The custom commands to register.</param>
        public void Register(params ICommand[] commands)
        {
            if (commands == null || commands.Length == 0)
            {
                return;
            }

            lock (_commandsLock)
            {
                foreach (var command in commands)
                {
                    if (command == null || string.IsNullOrWhiteSpace(command.Name))
                    {
                        continue;
                    }

                    _commands[command.Name] = command;
                }
            }
        }

        /// <summary>
        /// Starts the server, waits for shutdown, and ensures resources are released.
        /// </summary>
        /// <param name="port">The TCP port to bind.</param>
        /// <param name="token">A cancellation token used to stop the server.</param>
        public async Task RunAsync(int port, CancellationToken token = default)
        {
            var options = _options.Clone();
            options.Port = port;
            DredisServerOptions.Validate(options);

            await StartAsync(options, token);

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

        /// <summary>
        /// Starts the server without blocking for shutdown.
        /// </summary>
        /// <param name="port">The TCP port to bind.</param>
        /// <param name="token">A cancellation token used to abort startup.</param>
        public async Task StartAsync(int port, CancellationToken token = default)
        {
            var options = _options.Clone();
            options.Port = port;
            DredisServerOptions.Validate(options);

            await StartAsync(options, token);
        }

        /// <summary>
        /// Starts the server, waits for shutdown, and ensures resources are released.
        /// </summary>
        /// <param name="token">A cancellation token used to stop the server.</param>
        public Task RunAsync(CancellationToken token = default)
            => RunAsync(_options, token);

        /// <summary>
        /// Starts the server without blocking for shutdown.
        /// </summary>
        /// <param name="token">A cancellation token used to abort startup.</param>
        public Task StartAsync(CancellationToken token = default)
            => StartAsync(_options, token);

        private async Task RunAsync(DredisServerOptions options, CancellationToken token)
        {
            await StartAsync(options, token);

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

        private async Task StartAsync(DredisServerOptions options, CancellationToken token)
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

                bossGroup = new MultithreadEventLoopGroup(options.BossGroupThreadCount);
                workerGroup = options.WorkerGroupThreadCount.HasValue
                    ? new MultithreadEventLoopGroup(options.WorkerGroupThreadCount.Value)
                    : new MultithreadEventLoopGroup();

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
                        var handler = new DredisCommandHandler(_store);
                        ICommand[] registeredCommands;
                        lock (_commandsLock)
                        {
                            registeredCommands = _commands.Values.ToArray();
                        }

                        handler.Register(registeredCommands);
                        p.AddLast("dredisHandler", handler);
                    }));

                channel = await bootstrap.BindAsync(IPAddress.Parse(options.BindAddress), options.Port);

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

        /// <summary>
        /// Stops the server and releases all network resources.
        /// </summary>
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

        /// <summary>
        /// Shuts down DotNetty event loop groups gracefully.
        /// </summary>
        /// <param name="bossGroup">The boss event loop group, if any.</param>
        /// <param name="workerGroup">The worker event loop group, if any.</param>
        private static Task ShutdownGroupsAsync(IEventLoopGroup? bossGroup, IEventLoopGroup? workerGroup)
        {
            return Task.WhenAll(
                bossGroup?.ShutdownGracefullyAsync() ?? Task.CompletedTask,
                workerGroup?.ShutdownGracefullyAsync() ?? Task.CompletedTask);
        }
    }
}