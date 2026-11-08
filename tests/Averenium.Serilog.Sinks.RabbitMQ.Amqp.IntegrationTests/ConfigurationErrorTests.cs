using Serilog;
using Serilog.Core;
using Serilog.Events;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.IntegrationTests;

/// <summary>
/// Broker-side configuration problems must be reported loudly (failure listener) instead of looking like an
/// endless outage, while the sink keeps retrying so an operator can fix them.
/// </summary>
[ClassDataSource<SharedRabbitMq>(Shared = SharedType.PerTestSession)]
public sealed class ConfigurationErrorTests(SharedRabbitMq broker)
{
    // The missing-exchange heuristic (RabbitMQ 4.0) needs 5 failed attaches of ~5 s each.
    private static readonly TimeSpan ReportTimeout = TimeSpan.FromSeconds(60);

    [Test]
    public async Task Conflicting_queue_type_is_reported_as_a_configuration_error(CancellationToken ct)
    {
        var node = broker.Node;
        var topology = TestTopology.Unique();
        await node.DeclareClassicQueueAsync(topology.Queue, ct); // sink wants a quorum queue with this name
        var listener = new NotificationListener();

        var logger = CreateLogger(node, topology, listener, declareTopology: true);
        logger.Information("never delivered while the conflict exists");

        await Eventually.TrueAsync(() => listener.Messages.Any(m => m.Contains("precondition failed", StringComparison.Ordinal)),
            "PRECONDITION_FAILED from the queue declaration is recognised", ReportTimeout, ct);
        await logger.DisposeAsync();
    }

    [Test]
    public async Task Missing_exchange_without_topology_declaration_is_reported(CancellationToken ct)
    {
        var node = broker.Node;
        var topology = TestTopology.Unique(); // nothing declared
        var listener = new NotificationListener();

        var logger = CreateLogger(node, topology, listener, declareTopology: false);
        logger.Information("waits for the exchange");

        // RabbitMQ 4.0 lets the attach time out (C10, recognised by the heuristic as "probable"); newer versions
        // (4.4) refuse it with amqp:not-found, which is recognised for certain right away.
        await Eventually.TrueAsync(
            () => listener.Messages.Any(m =>
                m.Contains("cannot attach", StringComparison.Ordinal) || m.Contains("publish target not found", StringComparison.Ordinal)),
            "attaching to a missing exchange is recognised", ReportTimeout, ct);

        // Fix the broker side: the buffered event is delivered without restarting the application.
        await node.DeclareTopologyAsync(topology, ct);
        await Eventually.TrueAsync(() => node.MessageCountAsync(topology.Queue, ct).GetAwaiter().GetResult() == 1,
            "delivered after the exchange was declared", ReportTimeout, ct);
        await logger.DisposeAsync();
    }

    private static Logger CreateLogger(RabbitMqNode node, TestTopology topology, NotificationListener listener, bool declareTopology) =>
        new LoggerConfiguration()
            .WriteTo.Fallible(
                wt => wt.RabbitMQAmqp(o =>
                {
                    o.Hostnames.Add(node.AmqpUri.ToString()); // full amqp:// URI with credentials — the other supported form
                    o.Exchange = topology.Exchange;
                    o.RoutingKey = topology.RoutingKey;
                    o.DeclareTopology = declareTopology;
                    o.ExchangeType = "direct";
                    o.DeclareQueue = declareTopology ? topology.Queue : null;
                    o.MinReconnectDelay = TimeSpan.FromMilliseconds(200);
                    o.MaxReconnectDelay = TimeSpan.FromSeconds(1);
                    o.ShutdownTimeout = TimeSpan.FromSeconds(2);
                }),
                listener)
            .CreateLogger();

    private sealed class NotificationListener : ILoggingFailureListener
    {
        private readonly Lock _gate = new();
        private readonly List<string> _messages = [];

        public IReadOnlyList<string> Messages
        {
            get
            {
                lock (_gate)
                {
                    return [.. _messages];
                }
            }
        }

        public void OnLoggingFailed(object sender, LoggingFailureKind kind, string message, IReadOnlyCollection<LogEvent>? events, Exception? exception)
        {
            lock (_gate)
            {
                _messages.Add(message);
            }
        }
    }
}
