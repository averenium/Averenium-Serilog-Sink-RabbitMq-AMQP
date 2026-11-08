using Serilog;
using Serilog.Core;
using Serilog.Events;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.IntegrationTests;

/// <summary><c>FallbackAfter</c>: during a long outage events go to the fallback on time instead of waiting.</summary>
[NotInParallel]
public sealed class FallbackAfterTests
{
    [Test]
    public async Task Events_go_to_the_fallback_during_an_outage_and_later_ones_reach_the_broker(CancellationToken ct)
    {
        await using var node = new RabbitMqNode();
        await node.StartAsync(ct);
        var topology = TestTopology.Unique();
        await node.DeclareTopologyAsync(topology, ct);
        var fallback = new CountingSink();
        var logger = new LoggerConfiguration()
            .WriteTo.FallbackChain(
                wt => wt.RabbitMQAmqp(o =>
                {
                    o.Hostnames.Add(node.HostAndPort);
                    o.Username = RabbitMqNode.User;
                    o.Password = RabbitMqNode.Password;
                    o.Exchange = topology.Exchange;
                    o.RoutingKey = topology.RoutingKey;
                    o.FallbackAfter = TimeSpan.FromSeconds(2);
                    o.MinReconnectDelay = TimeSpan.FromMilliseconds(200);
                    o.MaxReconnectDelay = TimeSpan.FromSeconds(1);
                    o.ShutdownTimeout = TimeSpan.FromSeconds(30);
                }),
                wt => wt.Sink(fallback))
            .CreateLogger();

        logger.Information("Before the outage");
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (await node.MessageCountAsync(topology.Queue, ct) < 1 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(100, ct);
        }

        await node.StopAsync(ct);
        for (var i = 0; i < 10; i++)
        {
            logger.Information("During the outage {N}", i);
        }

        await Eventually.TrueAsync(() => fallback.Count == 10, "outage events handed over after FallbackAfter",
            TimeSpan.FromSeconds(15), ct);

        await node.StartAsync(ct);
        await node.WaitUntilAcceptingConnectionsAsync(TimeSpan.FromSeconds(60), ct);
        logger.Information("After the outage");
        await logger.DisposeAsync();

        await Assert.That(await node.MessageCountAsync(topology.Queue, ct)).IsEqualTo(2UL);
        await Assert.That(fallback.Count).IsEqualTo(10);
    }

    private sealed class CountingSink : ILogEventSink
    {
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public void Emit(LogEvent logEvent) => Interlocked.Increment(ref _count);
    }
}
