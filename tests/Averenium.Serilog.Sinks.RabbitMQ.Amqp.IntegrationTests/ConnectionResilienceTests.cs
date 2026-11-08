using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection.Models;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Publishing;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Publishing.Models;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.IntegrationTests;

/// <summary>
/// Failure scenarios against real RabbitMQ 4 brokers. Each test owns its broker(s) because it stops,
/// restarts or disrupts them. Run one at a time to keep container start-up predictable.
/// </summary>
[ClassDataSource<SharedRabbitMq>(Shared = SharedType.PerTestSession)]
public sealed class ConnectionResilienceTests(SharedRabbitMq broker)
{
    private static readonly TimeSpan ReadyTimeout = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan StateTimeout = TimeSpan.FromSeconds(30);

    [Test]
    public async Task Connects_and_publish_is_accepted(CancellationToken ct)
    {
        var node = broker.Node;
        var topology = TestTopology.Unique();
        await node.DeclareTopologyAsync(topology, ct);
        await using var harness = new SupervisorHarness([node.AmqpUri], topology);

        harness.Supervisor.Start();

        await Assert.That(await harness.PublishAsync("hello", ct)).IsEqualTo(OutcomeState.Accepted);
        await Assert.That(await node.MessageCountAsync(topology.Queue, ct)).IsEqualTo(1UL);
    }

    [Test]
    [ParallelLimiter<DisruptiveBrokerTests>]
    public async Task Broker_down_at_startup_connects_once_it_comes_up(CancellationToken ct)
    {
        await using var node = new RabbitMqNode();
        var topology = TestTopology.Unique();
        await using var harness = new SupervisorHarness([node.AmqpUri], topology);

        harness.Supervisor.Start();
        await Eventually.TrueAsync(() => harness.History.Count(s => s == SupervisorState.Faulted) >= 2,
            "connect to a stopped broker fails and is retried", StateTimeout, ct);
        await Assert.That(harness.Supervisor.State).IsNotEqualTo(SupervisorState.Ready);

        await node.StartAsync(ct);
        await node.DeclareTopologyAsync(topology, ct);

        await harness.WaitReadyAsync(ReadyTimeout, ct);
        // The supervisor may connect before the topology above is declared (race with the test, not with the sink).
        await Assert.That(await harness.PublishUntilAcceptedAsync("after start", ReadyTimeout, ct)).IsEqualTo(OutcomeState.Accepted);
    }

    [Test]
    [ParallelLimiter<DisruptiveBrokerTests>]
    public async Task Broker_restart_is_survived(CancellationToken ct)
    {
        await using var node = new RabbitMqNode();
        await node.StartAsync(ct);
        var topology = TestTopology.Unique();
        await node.DeclareTopologyAsync(topology, ct);
        await using var harness = new SupervisorHarness([node.AmqpUri], topology);
        harness.Supervisor.Start();
        await Assert.That(await harness.PublishAsync("before restart", ct)).IsEqualTo(OutcomeState.Accepted);

        harness.ClearHistory();
        await node.StopAsync(ct);
        await Eventually.TrueAsync(() => harness.Supervisor.State != SupervisorState.Ready,
            "broker went away", StateTimeout, ct);
        await node.StartAsync(ct);

        await harness.WaitReadyAsync(ReadyTimeout, ct);
        await Assert.That(await harness.PublishAsync("after restart", ct)).IsEqualTo(OutcomeState.Accepted);
        await Assert.That(await node.MessageCountAsync(topology.Queue, ct)).IsEqualTo(2UL)
            .Because("the quorum queue keeps the message published before the restart");
    }

    [Test]
    [ParallelLimiter<DisruptiveBrokerTests>]
    public async Task Forcefully_closed_connection_is_recovered(CancellationToken ct)
    {
        await using var node = new RabbitMqNode();
        await node.StartAsync(ct);
        var topology = TestTopology.Unique();
        await node.DeclareTopologyAsync(topology, ct);
        await using var harness = new SupervisorHarness([node.AmqpUri], topology);
        harness.Supervisor.Start();
        await harness.WaitReadyAsync(ReadyTimeout, ct);
        harness.ClearHistory();

        await node.CloseAllConnectionsAsync(ct);

        await Eventually.TrueAsync(
            () => harness.History.Any(s => s is SupervisorState.Recovering or SupervisorState.Faulted),
            "the supervisor noticed the connection loss", StateTimeout, ct);
        await harness.WaitReadyAsync(ReadyTimeout, ct);
        await Assert.That(await harness.PublishAsync("after close", ct)).IsEqualTo(OutcomeState.Accepted);
    }

    [Test]
    [ParallelLimiter<DisruptiveBrokerTests>]
    public async Task Fails_over_to_the_next_node_when_the_current_one_stops(CancellationToken ct)
    {
        await using var nodeA = new RabbitMqNode();
        await using var nodeB = new RabbitMqNode();
        await Task.WhenAll(nodeA.StartAsync(ct), nodeB.StartAsync(ct));
        var topology = TestTopology.Unique();
        await nodeA.DeclareTopologyAsync(topology, ct);
        await nodeB.DeclareTopologyAsync(topology, ct);
        await using var harness = new SupervisorHarness([nodeA.AmqpUri, nodeB.AmqpUri], topology);
        harness.Supervisor.Start();

        await Assert.That(await harness.PublishAsync("to A", ct)).IsEqualTo(OutcomeState.Accepted);
        await Assert.That(await nodeA.MessageCountAsync(topology.Queue, ct)).IsEqualTo(1UL)
            .Because("round-robin starts with the first configured node");

        harness.ClearHistory();
        await nodeA.StopAsync(ct);

        // Client recovery usually reconnects to B within ~1 s — often before StopAsync even returns —
        // so assert on the recorded history rather than on observing a non-Ready state.
        await Eventually.TrueAsync(() => harness.History.Contains(SupervisorState.Recovering),
            "the loss of node A was noticed", StateTimeout, ct);
        await harness.WaitReadyAsync(ReadyTimeout, ct);
        await Assert.That(await harness.PublishAsync("to B", ct)).IsEqualTo(OutcomeState.Accepted);
        await Assert.That(await nodeB.MessageCountAsync(topology.Queue, ct)).IsEqualTo(1UL);
    }

    [Test]
    public async Task Missing_target_exchange_keeps_supervisor_out_of_ready_until_declared_again(CancellationToken ct)
    {
        var node = broker.Node;
        var topology = TestTopology.Unique();
        await node.DeclareTopologyAsync(topology, ct);
        await using var harness = new SupervisorHarness([node.AmqpUri], topology);
        harness.Supervisor.Start();
        await harness.WaitReadyAsync(ReadyTimeout, ct);
        harness.ClearHistory();

        var publisher = await harness.WaitReadyAsync(ReadyTimeout, ct);
        await node.DeleteExchangeAsync(topology.Exchange, ct);

        // RabbitMQ 4 keeps the link attached and rejects each message with amqp:not-found
        // (the very first publish may be Released). The publish loop reports that to the supervisor.
        var kind = OutcomeKind.Accepted;
        for (var i = 0; i < 10 && kind != OutcomeKind.TargetMissing; i++)
        {
            var result = await publisher.PublishAsync(new AmqpMessage("probe"), ct).WaitAsync(TimeSpan.FromSeconds(5), ct);
            kind = OutcomeClassifier.Classify(result.Outcome);
            await Task.Delay(200, ct);
        }

        await Assert.That(kind).IsEqualTo(OutcomeKind.TargetMissing);
        harness.Supervisor.ReportTargetMissing(publisher);

        await Eventually.TrueAsync(() => harness.History.Count(s => s == SupervisorState.Faulted) >= 2,
            "re-attaching to a missing exchange fails and is retried, so the supervisor stays out of Ready", StateTimeout, ct);
        await Assert.That(harness.Supervisor.State).IsNotEqualTo(SupervisorState.Ready);

        await node.DeclareTopologyAsync(topology, ct);
        await harness.WaitReadyAsync(ReadyTimeout, ct);
        await Assert.That(await harness.PublishAsync("after redeclare", ct)).IsEqualTo(OutcomeState.Accepted);
    }
}
