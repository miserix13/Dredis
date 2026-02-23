using System.Net;
using Microsoft.Extensions.Configuration;

namespace Dredis
{
    /// <summary>
    /// Configuration options used by <see cref="DredisServer"/>.
    /// </summary>
    public sealed class DredisServerOptions
    {
        /// <summary>
        /// Gets or sets the bind address used for the TCP listener.
        /// </summary>
        public string BindAddress { get; set; } = IPAddress.Loopback.ToString();

        /// <summary>
        /// Gets or sets the TCP port used for the server listener.
        /// </summary>
        public int Port { get; set; } = 6379;

        /// <summary>
        /// Gets or sets the number of boss event-loop threads.
        /// </summary>
        public int BossGroupThreadCount { get; set; } = 1;

        /// <summary>
        /// Gets or sets the number of worker event-loop threads.
        /// Null uses DotNetty default sizing.
        /// </summary>
        public int? WorkerGroupThreadCount { get; set; }

        /// <summary>
        /// Creates a new options instance from <see cref="IConfiguration"/>.
        /// </summary>
        /// <param name="configuration">The configuration root.</param>
        /// <param name="sectionName">The section containing server options. Defaults to <c>DredisServer</c>.</param>
        public static DredisServerOptions FromConfiguration(IConfiguration configuration, string sectionName = "DredisServer")
        {
            if (configuration == null)
            {
                throw new ArgumentNullException(nameof(configuration));
            }

            var options = new DredisServerOptions();

            if (string.IsNullOrWhiteSpace(sectionName))
            {
                configuration.Bind(options);
            }
            else
            {
                configuration.GetSection(sectionName).Bind(options);
            }

            Validate(options);
            return options;
        }

        /// <summary>
        /// Validates option values.
        /// </summary>
        /// <param name="options">Options to validate.</param>
        public static void Validate(DredisServerOptions options)
        {
            if (options == null)
            {
                throw new ArgumentNullException(nameof(options));
            }

            if (!IPAddress.TryParse(options.BindAddress, out _))
            {
                throw new ArgumentException("BindAddress must be a valid IP address.", nameof(options));
            }

            if (options.Port is < 1 or > 65535)
            {
                throw new ArgumentOutOfRangeException(nameof(options), "Port must be between 1 and 65535.");
            }

            if (options.BossGroupThreadCount <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(options), "BossGroupThreadCount must be greater than 0.");
            }

            if (options.WorkerGroupThreadCount.HasValue && options.WorkerGroupThreadCount.Value <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(options), "WorkerGroupThreadCount must be greater than 0 when provided.");
            }
        }

        internal DredisServerOptions Clone() =>
            new()
            {
                BindAddress = BindAddress,
                Port = Port,
                BossGroupThreadCount = BossGroupThreadCount,
                WorkerGroupThreadCount = WorkerGroupThreadCount
            };
    }
}