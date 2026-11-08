using System.Diagnostics;
using RabbitMQ.AMQP.Client;
using Serilog;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.IntegrationTests;

/// <summary>What a consumer actually receives: correlation-id from the trace, durability, message-id.</summary>
[NotInParallel]
public sealed class MessageMetadataTests
{
    [Test]
    public async Task Consumers_receive_the_trace_id_as_correlation_id_and_durable_messages(CancellationToken ct)
    {
        await using var node = new RabbitMqNode();
        await node.StartAsync(ct);
        var topology = TestTopology.Unique();
        await node.DeclareTopologyAsync(topology, ct);

        Activity.Current = null; // the test framework runs each test inside its own activity
        using var activity = new Activity("checkout").SetIdFormat(ActivityIdFormat.W3C).Start();
        var logger = CreateLogger(node, topology, durable: true);
        logger.Information("Inside a trace");
        activity.Stop();
        logger.Information("Outside a trace");
        await logger.DisposeAsync();

        var messages = await node.ReceiveAsync(topology.Queue, 2, TimeSpan.FromSeconds(20), ct);

        await Assert.That(messages.Count).IsEqualTo(2);
        var correlationIds = messages.Select(m => m.CorrelationId()?.ToString()).ToList();
        await Assert.That(correlationIds).Contains(activity.TraceId.ToHexString());
        await Assert.That(correlationIds).Contains((string?)null);
        await Assert.That(messages.All(m => m.Durable())).IsTrue();
        await Assert.That(messages.Select(m => m.MessageId()).Distinct().Count()).IsEqualTo(2);
    }

    [Test]
    public async Task Non_durable_messages_are_accepted_and_received_as_non_durable(CancellationToken ct)
    {
        await using var node = new RabbitMqNode();
        await node.StartAsync(ct);
        var topology = TestTopology.Unique();
        await node.DeclareTopologyAsync(topology, ct);

        var logger = CreateLogger(node, topology, durable: false);
        logger.Information("Transient");
        await logger.DisposeAsync();

        var messages = await node.ReceiveAsync(topology.Queue, 1, TimeSpan.FromSeconds(20), ct);

        await Assert.That(messages.Count).IsEqualTo(1);
        await Assert.That(messages[0].Durable()).IsFalse();
    }

    private static global::Serilog.Core.Logger CreateLogger(RabbitMqNode node, TestTopology topology, bool durable) =>
        new LoggerConfiguration()
            .WriteTo.RabbitMQAmqp(o =>
            {
                o.Hostnames.Add(node.HostAndPort);
                o.Username = RabbitMqNode.User;
                o.Password = RabbitMqNode.Password;
                o.Exchange = topology.Exchange;
                o.RoutingKey = topology.RoutingKey;
                o.DurableMessages = durable;
                o.ShutdownTimeout = TimeSpan.FromSeconds(30);
            })
            .CreateLogger();
}
