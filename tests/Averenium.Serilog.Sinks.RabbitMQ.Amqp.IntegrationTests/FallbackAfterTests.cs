using Serilog;
using Serilog.Core;
using Serilog.Events;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.IntegrationTests;

/// <summary><c>FallbackAfter</c>: during a long outage events go to the fallback on time instead of waiting.</summary>
[ParallelLimiter<DisruptiveBrokerTests>]
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
                    o.ClientRecoveryTimeout = TimeSpan.FromSeconds(5); // abandon a stalled client recovery (C13) sooner
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

        // The sink reconnects on its own schedule — possibly only after the supervisor gave up on a stalled client
        // recovery (C13). Until then, events logged now also go to the fallback after 2 s, which is correct; so keep
        // logging until one reaches the broker again.
        var after = 0;
        deadline = DateTime.UtcNow.AddSeconds(60);
        while (await node.MessageCountAsync(topology.Queue, ct) < 2)
        {
            if (DateTime.UtcNow > deadline)
            {
                Assert.Fail("No event reached the broker within 60 s after it came back.");
            }

            logger.Information("After the outage {N}", after++);
            await Task.Delay(500, ct);
        }

        await logger.DisposeAsync();

        var delivered = await node.MessageCountAsync(topology.Queue, ct);
        await Assert.That(delivered).IsGreaterThanOrEqualTo(2UL);
        await Assert.That(fallback.Count).IsGreaterThanOrEqualTo(10).Because("the outage events went to the fallback");
        await Assert.That((ulong)fallback.Count + delivered).IsGreaterThanOrEqualTo((ulong)(1 + 10 + after))
            .Because("no silent loss: every event reached the broker or the fallback");
    }

    private sealed class CountingSink : ILogEventSink
    {
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public void Emit(LogEvent logEvent) => Interlocked.Increment(ref _count);
    }
}
