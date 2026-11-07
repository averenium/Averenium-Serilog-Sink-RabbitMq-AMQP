using Serilog;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.IntegrationTests;

/// <summary>
/// The public API end to end: Serilog logger → <c>WriteTo.RabbitMQAmqp</c> → real RabbitMQ 4.
/// "No silent loss" is asserted as: every emitted event is in the queue (at-least-once, so ≥) and nothing
/// reached the fallback sink.
/// </summary>
[NotInParallel]
public sealed class SinkEndToEndTests
{
    [Test]
    public async Task Logged_events_reach_the_queue(CancellationToken ct)
    {
        await using var node = new RabbitMqNode();
        await node.StartAsync(ct);
        var topology = Topology.Unique();
        await node.DeclareTopologyAsync(topology, ct);
        var fallback = new CollectingSink();

        var logger = CreateLogger([node.AmqpUri], topology, fallback);
        for (var i = 0; i < 200; i++)
        {
            logger.Information("Order {OrderId} processed", i);
        }

        await logger.DisposeAsync();

        await Assert.That(await node.MessageCountAsync(topology.Queue, ct)).IsEqualTo(200UL);
        await Assert.That(fallback.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Topology_is_declared_when_enabled(CancellationToken ct)
    {
        await using var node = new RabbitMqNode();
        await node.StartAsync(ct);
        var topology = Topology.Unique();
        var fallback = new CollectingSink();

        var logger = CreateLogger([node.AmqpUri], topology, fallback, declareTopology: true);
        for (var i = 0; i < 10; i++)
        {
            logger.Warning("Declared {N}", i);
        }

        await logger.DisposeAsync();

        await Assert.That(await node.MessageCountAsync(topology.Queue, ct)).IsEqualTo(10UL);
        await Assert.That(fallback.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Events_logged_while_the_broker_is_down_at_startup_are_delivered_later(CancellationToken ct)
    {
        await using var node = new RabbitMqNode();
        var topology = Topology.Unique();
        var fallback = new CollectingSink();

        var logger = CreateLogger([node.AmqpUri], topology, fallback, declareTopology: true);
        for (var i = 0; i < 100; i++)
        {
            logger.Information("Before broker start {N}", i);
        }

        await Task.Delay(TimeSpan.FromSeconds(2), ct);
        await node.StartAsync(ct);
        await logger.DisposeAsync();

        await Assert.That(await node.MessageCountAsync(topology.Queue, ct)).IsGreaterThanOrEqualTo(100UL);
        await Assert.That(fallback.Count).IsEqualTo(0);
    }

    [Test]
    public async Task No_events_are_lost_across_a_broker_restart_while_logging(CancellationToken ct)
    {
        await using var node = new RabbitMqNode();
        await node.StartAsync(ct);
        var topology = Topology.Unique();
        await node.DeclareTopologyAsync(topology, ct);
        var fallback = new CollectingSink();
        SelfLog.Enable(m => Console.WriteLine($"{DateTime.Now:HH:mm:ss.fff} selflog {m}"));
        var logger = CreateLogger([node.AmqpUri], topology, fallback);

        using var stopLogging = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var emitted = 0;
        var producer = Task.Run(async () =>
        {
            while (!stopLogging.IsCancellationRequested)
            {
                logger.Information("Tick {N}", Interlocked.Increment(ref emitted));
                await Task.Delay(5, CancellationToken.None);
            }
        }, CancellationToken.None);

        await Task.Delay(TimeSpan.FromSeconds(2), ct);
        Phase("stop broker");
        await node.StopAsync(ct);
        Phase("broker stopped");
        await Task.Delay(TimeSpan.FromSeconds(3), ct);
        Phase("start broker");
        await node.StartAsync(ct);
        Phase("broker container started");
        var brokerReady = await node.WaitUntilAcceptingConnectionsAsync(TimeSpan.FromMinutes(2), ct);
        Phase($"broker accepts AMQP connections after {brokerReady.TotalSeconds:F1} s");
        await Task.Delay(TimeSpan.FromSeconds(3), ct);
        await stopLogging.CancelAsync();
        await producer;
        Phase($"logging stopped, emitted {emitted}; disposing logger");
        await logger.DisposeAsync();
        Phase("logger disposed");
        SelfLog.Disable();

        await Assert.That(fallback.Count).IsEqualTo(0);
        await Assert.That(await node.MessageCountAsync(topology.Queue, ct)).IsGreaterThanOrEqualTo((ulong)emitted)
            .Because($"{emitted} events were emitted; at-least-once delivery");
    }

    [Test]
    public async Task No_events_are_lost_when_failing_over_to_another_node(CancellationToken ct)
    {
        await using var nodeA = new RabbitMqNode();
        await using var nodeB = new RabbitMqNode();
        await Task.WhenAll(nodeA.StartAsync(ct), nodeB.StartAsync(ct));
        var topology = Topology.Unique();
        await nodeA.DeclareTopologyAsync(topology, ct);
        await nodeB.DeclareTopologyAsync(topology, ct);
        var fallback = new CollectingSink();
        var logger = CreateLogger([nodeA.AmqpUri, nodeB.AmqpUri], topology, fallback);

        for (var i = 0; i < 50; i++)
        {
            logger.Information("First half {N}", i);
        }

        await Task.Delay(TimeSpan.FromSeconds(1), ct);
        await nodeA.StopAsync(ct);
        for (var i = 0; i < 50; i++)
        {
            logger.Information("Second half {N}", i);
        }

        await logger.DisposeAsync();
        await nodeA.StartAsync(ct);

        var total = await nodeA.MessageCountAsync(topology.Queue, ct) + await nodeB.MessageCountAsync(topology.Queue, ct);
        await Assert.That(total).IsGreaterThanOrEqualTo(100UL);
        await Assert.That(await nodeB.MessageCountAsync(topology.Queue, ct)).IsGreaterThanOrEqualTo(50UL);
        await Assert.That(fallback.Count).IsEqualTo(0);
    }

    private static void Phase(string name) => Console.WriteLine($"{DateTime.Now:HH:mm:ss.fff} phase {name}");

    private static Logger CreateLogger(
        IReadOnlyList<Uri> uris, Topology topology, CollectingSink fallback, bool declareTopology = false) =>
        new LoggerConfiguration()
            .MinimumLevel.Verbose()
            .WriteTo.FallbackChain(
                wt => wt.RabbitMQAmqp(o =>
                {
                    foreach (var uri in uris)
                    {
                        o.Uris.Add(uri);
                    }

                    o.Exchange = topology.Exchange;
                    o.RoutingKey = topology.RoutingKey;
                    o.DeclareTopology = declareTopology;
                    o.ExchangeType = "direct";
                    o.DeclareQueue = topology.Queue;
                    o.ConnectionName = "e2e-" + topology.Exchange;
                    o.MinReconnectDelay = TimeSpan.FromMilliseconds(200);
                    o.MaxReconnectDelay = TimeSpan.FromSeconds(2);
                    o.ShutdownTimeout = TimeSpan.FromSeconds(90);
                }),
                wt => wt.Sink(fallback))
            .CreateLogger();

    private sealed class CollectingSink : ILogEventSink
    {
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public void Emit(LogEvent logEvent) => Interlocked.Increment(ref _count);
    }
}
