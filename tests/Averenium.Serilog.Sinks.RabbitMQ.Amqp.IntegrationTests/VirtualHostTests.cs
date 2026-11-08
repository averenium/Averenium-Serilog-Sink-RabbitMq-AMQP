using Serilog;
using Serilog.Core;
using Serilog.Events;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.IntegrationTests;

/// <summary>
/// Virtual hosts whose name contains '/' (e.g. "/data") must reach the broker exactly — the classic pitfall is that a
/// '/' in the URI path is a separator, so the name has to be encoded as %2F. Verified with the broker's own
/// <c>rabbitmqctl list_queues -p /data</c>, independent of our URI handling.
/// </summary>
[ClassDataSource<SharedRabbitMq>(Shared = SharedType.PerTestSession)]
public sealed class VirtualHostTests(SharedRabbitMq broker)
{
    /// <summary>A unique virtual host with a leading slash, like <c>/data</c> (tests share one broker).</summary>
    private static string NewSlashVirtualHost() => "/data-" + Guid.NewGuid().ToString("N")[..8];

    [Test]
    public async Task Virtual_host_option_with_a_leading_slash_is_used_as_is(CancellationToken ct)
    {
        var node = broker.Node;
        var virtualHost = NewSlashVirtualHost();
        await node.AddVirtualHostAsync(virtualHost, ct);
        var topology = TestTopology.Unique();

        await LogThreeAsync(o =>
        {
            o.Hostnames.Add(node.HostAndPort);
            o.Username = RabbitMqNode.User;
            o.Password = RabbitMqNode.Password;
            o.VirtualHost = virtualHost;
        }, topology);

        await AssertThreeMessagesInVirtualHostAsync(node, virtualHost, topology, ct);
    }

    [Test]
    public async Task Uri_with_an_encoded_slash_reaches_the_same_virtual_host(CancellationToken ct)
    {
        var node = broker.Node;
        var virtualHost = NewSlashVirtualHost();
        await node.AddVirtualHostAsync(virtualHost, ct);
        var topology = TestTopology.Unique();

        await LogThreeAsync(o => o.Hostnames.Add(
            $"amqp://{RabbitMqNode.User}:{RabbitMqNode.Password}@{node.HostAndPort}/%2F{virtualHost[1..]}"), topology);

        await AssertThreeMessagesInVirtualHostAsync(node, virtualHost, topology, ct);
        await Assert.That(await node.ListQueuesAsync("/", ct)).DoesNotContain(topology.Queue)
            .Because("nothing must land in the default virtual host");
    }

    /// <summary>rabbitmqctl statistics for quorum queues are refreshed periodically, so poll until they catch up.</summary>
    private static async Task AssertThreeMessagesInVirtualHostAsync(
        RabbitMqNode node, string virtualHost, TestTopology topology, CancellationToken ct)
    {
        var expected = $"{topology.Queue}\t3";
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        string queues;
        while (!(queues = await node.ListQueuesAsync(virtualHost, ct)).Contains(expected, StringComparison.Ordinal)
               && DateTime.UtcNow < deadline)
        {
            await Task.Delay(1_000, ct);
        }

        await Assert.That(queues).Contains(expected).Because($"rabbitmqctl list_queues -p {virtualHost}: {queues}");
    }

    private static async Task LogThreeAsync(Action<RabbitMqAmqpSinkOptions> connection, TestTopology topology)
    {
        var fallback = new CountingSink();
        var logger = new LoggerConfiguration()
            .WriteTo.FallbackChain(
                wt => wt.RabbitMQAmqp(o =>
                {
                    connection(o);
                    o.Exchange = topology.Exchange;
                    o.RoutingKey = topology.RoutingKey;
                    o.DeclareTopology = true;
                    o.ExchangeType = "direct";
                    o.DeclareQueue = topology.Queue;
                    o.ShutdownTimeout = TimeSpan.FromSeconds(30);
                }),
                wt => wt.Sink(fallback))
            .CreateLogger();

        for (var i = 0; i < 3; i++)
        {
            logger.Information("In a virtual host with a slash {N}", i);
        }

        await logger.DisposeAsync();
        await Assert.That(fallback.Count).IsEqualTo(0);
    }

    private sealed class CountingSink : ILogEventSink
    {
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public void Emit(LogEvent logEvent) => Interlocked.Increment(ref _count);
    }
}
