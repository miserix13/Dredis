using Microsoft.Extensions.Configuration;
using Xunit;

namespace Dredis.Tests
{
    /// <summary>
    /// Unit tests for DredisServerOptions configuration binding and validation.
    /// </summary>
    public sealed class DredisServerOptionsTests
    {
        [Fact]
        public void FromConfiguration_BindsSectionValues()
        {
            var values = new Dictionary<string, string?>
            {
                ["DredisServer:BindAddress"] = "127.0.0.1",
                ["DredisServer:Port"] = "6381",
                ["DredisServer:BossGroupThreadCount"] = "2",
                ["DredisServer:WorkerGroupThreadCount"] = "8"
            };

            var configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(values)
                .Build();

            var options = DredisServerOptions.FromConfiguration(configuration);

            Assert.Equal("127.0.0.1", options.BindAddress);
            Assert.Equal(6381, options.Port);
            Assert.Equal(2, options.BossGroupThreadCount);
            Assert.Equal(8, options.WorkerGroupThreadCount);
        }

        [Fact]
        public void FromConfiguration_UsesDefaultsWhenSectionMissing()
        {
            var configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>())
                .Build();

            var options = DredisServerOptions.FromConfiguration(configuration);

            Assert.Equal("127.0.0.1", options.BindAddress);
            Assert.Equal(6379, options.Port);
            Assert.Equal(1, options.BossGroupThreadCount);
            Assert.Null(options.WorkerGroupThreadCount);
        }

        [Fact]
        public void FromConfiguration_ThrowsForInvalidPort()
        {
            var values = new Dictionary<string, string?>
            {
                ["DredisServer:Port"] = "0"
            };

            var configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(values)
                .Build();

            Assert.Throws<ArgumentOutOfRangeException>(() => DredisServerOptions.FromConfiguration(configuration));
        }

        [Fact]
        public void FromConfiguration_ThrowsForInvalidBindAddress()
        {
            var values = new Dictionary<string, string?>
            {
                ["DredisServer:BindAddress"] = "not-an-ip"
            };

            var configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(values)
                .Build();

            Assert.Throws<ArgumentException>(() => DredisServerOptions.FromConfiguration(configuration));
        }
    }
}