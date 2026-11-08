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
[ClassDataSource<SharedRabbitMq>(Shared = SharedType.PerTestSession)]
public sealed class SinkEndToEndTests(SharedRabbitMq broker)
{
    [Test]
    public async Task Logged_events_reach_the_queue(CancellationToken ct)
    {
        var node = broker.Node;
        var topology = TestTopology.Unique();
        await node.DeclareTopologyAsync(topology, ct);
        var fallback = new CollectingSink();

        var logger = CreateLogger([node], topology, fallback);
        for (var i = 0; i < 200; i++)
        {
            logger.Information("Order {OrderId} processed", i);
        }

        await logger.DisposeAsync();

        await Assert.That(await node.MessageCountAsync(topology.Queue, ct)).IsEqualTo(200UL);
        await Assert.That(fallback.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Broker_accepts_every_allowed_message_property_type(CancellationToken ct)
    {
        var node = broker.Node;
        var topology = TestTopology.Unique();
        var fallback = new CollectingSink();

        var logger = CreateLogger([node], topology, fallback, declareTopology: true,
            messageProperties: _ =>
            [
                new("string", "text"), new("bool", true), new("byte", (byte)1), new("sbyte", (sbyte)-1),
                new("short", (short)-2), new("ushort", (ushort)2), new("int", -3), new("uint", 3u),
                new("long", -4L), new("ulong", 4UL), new("float", 1.5f), new("double", 2.5d),
                new("guid", Guid.NewGuid()), new("datetime", DateTime.UtcNow), new("datetimeoffset", DateTimeOffset.UtcNow),
                new("decimal", 12.34m), new("char", 'x'), new("enum", DayOfWeek.Friday),
            ]);
        logger.Information("All property types");
        await logger.DisposeAsync();

        await Assert.That(await node.MessageCountAsync(topology.Queue, ct)).IsEqualTo(1UL);
        await Assert.That(fallback.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Topology_is_declared_when_enabled(CancellationToken ct)
    {
        var node = broker.Node;
        var topology = TestTopology.Unique();
        var fallback = new CollectingSink();

        var logger = CreateLogger([node], topology, fallback, declareTopology: true);
        for (var i = 0; i < 10; i++)
        {
            logger.Warning("Declared {N}", i);
        }

        await logger.DisposeAsync();

        await Assert.That(await node.MessageCountAsync(topology.Queue, ct)).IsEqualTo(10UL);
        await Assert.That(fallback.Count).IsEqualTo(0);
    }

    [Test]
    [ParallelLimiter<DisruptiveBrokerTests>]
    public async Task Events_logged_while_the_broker_is_down_at_startup_are_delivered_later(CancellationToken ct)
    {
        await using var node = new RabbitMqNode();
        var topology = TestTopology.Unique();
        var fallback = new CollectingSink();

        var logger = CreateLogger([node], topology, fallback, declareTopology: true);
        for (var i = 0; i < 100; i++)
        {
            logger.Information("Before broker start {N}", i);
        }

        await node.StartAsync(ct);
        await logger.DisposeAsync();

        await Assert.That(await node.MessageCountAsync(topology.Queue, ct)).IsGreaterThanOrEqualTo(100UL);
        await Assert.That(fallback.Count).IsEqualTo(0);
    }

    [Test]
    [ParallelLimiter<DisruptiveBrokerTests>]
    public async Task Byte_limited_outage_buffer_sends_the_oldest_to_fallback_and_delivers_the_rest(CancellationToken ct)
    {
        await using var node = new RabbitMqNode();
        var topology = TestTopology.Unique();
        var fallback = new CollectingSink();
        const int emitted = 2_000;

        var logger = CreateLogger([node], topology, fallback, declareTopology: true, queueSizeLimitBytes: 128 * 1024);
        for (var i = 0; i < emitted; i++)
        {
            logger.Information("Outage event {N} with some payload {Payload}", i, new string('x', 100));
        }

        await Eventually.TrueAsync(() => fallback.Count > 0, "the byte limit overflowed while the broker was down", TimeSpan.FromSeconds(30), ct);
        await node.StartAsync(ct);
        await logger.DisposeAsync();

        var delivered = await node.MessageCountAsync(topology.Queue, ct);
        await Assert.That(delivered).IsGreaterThan(0UL);
        await Assert.That((long)delivered + fallback.Count).IsGreaterThanOrEqualTo(emitted)
            .Because($"no silent loss: {delivered} delivered + {fallback.Count} in fallback");
        await Assert.That(fallback.WithProperties).IsEqualTo(fallback.Count)
            .Because("events restored for the fallback keep their properties");
    }

    [Test]
    [ParallelLimiter<DisruptiveBrokerTests>]
    public async Task No_events_are_lost_across_a_broker_restart_while_logging(CancellationToken ct)
    {
        await using var node = new RabbitMqNode();
        await node.StartAsync(ct);
        var topology = TestTopology.Unique();
        await node.DeclareTopologyAsync(topology, ct);
        var fallback = new CollectingSink();
        SelfLog.Enable(m => Console.WriteLine($"{DateTime.Now:HH:mm:ss.fff} selflog {m}"));
        var logger = CreateLogger([node], topology, fallback);

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

        await WaitForCountAsync(node, topology, 1, ct); // connected and delivering
        Phase("stop broker");
        await node.StopAsync(ct);
        Phase("broker stopped");
        await Task.Delay(TimeSpan.FromSeconds(1), ct); // the outage itself is the scenario
        Phase("start broker");
        await node.StartAsync(ct);
        Phase("broker container started");
        var brokerReady = await node.WaitUntilAcceptingConnectionsAsync(TimeSpan.FromMinutes(2), ct);
        Phase($"broker accepts AMQP connections after {brokerReady.TotalSeconds:F1} s");
        var beforeReconnect = await node.MessageCountAsync(topology.Queue, ct);
        await WaitForCountAsync(node, topology, beforeReconnect + 1, ct); // the sink is delivering again
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
    [ParallelLimiter<DisruptiveBrokerTests>]
    public async Task No_events_are_lost_when_failing_over_to_another_node(CancellationToken ct)
    {
        await using var nodeA = new RabbitMqNode();
        await using var nodeB = new RabbitMqNode();
        await Task.WhenAll(nodeA.StartAsync(ct), nodeB.StartAsync(ct));
        var topology = TestTopology.Unique();
        await nodeA.DeclareTopologyAsync(topology, ct);
        await nodeB.DeclareTopologyAsync(topology, ct);
        var fallback = new CollectingSink();
        var logger = CreateLogger([nodeA, nodeB], topology, fallback);

        for (var i = 0; i < 50; i++)
        {
            logger.Information("First half {N}", i);
        }

        await WaitForCountAsync(nodeA, topology, 50, ct);
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

    private static async Task WaitForCountAsync(RabbitMqNode node, TestTopology topology, ulong expected, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow.AddSeconds(60);
        while (await node.MessageCountAsync(topology.Queue, ct) < expected)
        {
            if (DateTime.UtcNow > deadline)
            {
                Assert.Fail($"Fewer than {expected} messages arrived within 60 s.");
            }

            await Task.Delay(100, ct);
        }
    }

    private static void Phase(string name) => Console.WriteLine($"{DateTime.Now:HH:mm:ss.fff} phase {name}");

    private static Logger CreateLogger(
        IReadOnlyList<RabbitMqNode> nodes, TestTopology topology, CollectingSink fallback, bool declareTopology = false,
        long queueSizeLimitBytes = 64L * 1024 * 1024,
        Func<LogEvent, IEnumerable<KeyValuePair<string, object>>>? messageProperties = null) =>
        new LoggerConfiguration()
            .MinimumLevel.Verbose()
            .WriteTo.FallbackChain(
                wt => wt.RabbitMQAmqp(o =>
                {
                    // Plain host:port with separate credentials — the recommended configuration.
                    foreach (var node in nodes)
                    {
                        o.Hostnames.Add(node.HostAndPort);
                    }

                    o.Username = RabbitMqNode.User;
                    o.Password = RabbitMqNode.Password;

                    o.Exchange = topology.Exchange;
                    o.RoutingKey = topology.RoutingKey;
                    o.DeclareTopology = declareTopology;
                    o.ExchangeType = "direct";
                    o.DeclareQueue = declareTopology ? topology.Queue : null;
                    o.ConnectionName = "e2e-" + topology.Exchange;
                    o.MinReconnectDelay = TimeSpan.FromMilliseconds(200);
                    o.MaxReconnectDelay = TimeSpan.FromSeconds(2);
                    o.ShutdownTimeout = TimeSpan.FromSeconds(90);
                    o.ClientRecoveryTimeout = TimeSpan.FromSeconds(5); // abandon a stalled client recovery (C13) sooner
                    o.QueueSizeLimitBytes = queueSizeLimitBytes;
                    o.MessageProperties = messageProperties;
                }),
                wt => wt.Sink(fallback))
            .CreateLogger();

    private sealed class CollectingSink : ILogEventSink
    {
        private int _count;
        private int _withProperties;

        public int Count => Volatile.Read(ref _count);

        public int WithProperties => Volatile.Read(ref _withProperties);

        public void Emit(LogEvent logEvent)
        {
            Interlocked.Increment(ref _count);
            if (logEvent.Properties.ContainsKey("N"))
            {
                Interlocked.Increment(ref _withProperties);
            }
        }
    }
}
