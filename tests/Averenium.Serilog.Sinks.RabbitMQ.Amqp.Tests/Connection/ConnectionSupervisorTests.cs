using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection.Models;
using Microsoft.Extensions.Time.Testing;
using NSubstitute;
using RabbitMQ.AMQP.Client;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Connection;

public sealed class ConnectionSupervisorTests : IAsyncDisposable
{
    private static readonly TimeSpan Step = TimeSpan.FromSeconds(1);

    private readonly FakeEnvironment _broker = new();
    private readonly FakeTimeProvider _time = new();
    private readonly ConnectionSupervisor _supervisor;

    public ConnectionSupervisorTests()
    {
        _supervisor = new ConnectionSupervisor(
            _broker.Environment,
            new ConnectionSupervisorOptions
            {
                MinReconnectDelay = TimeSpan.FromSeconds(1),
                MaxReconnectDelay = TimeSpan.FromSeconds(4),
                PublisherRecoveryGrace = TimeSpan.FromSeconds(10),
                HealthCheckInterval = TimeSpan.FromSeconds(1),
                ConnectTimeout = TimeSpan.FromSeconds(30),
                MaxConsecutivePublishTimeouts = 3,
            },
            _time);
    }

    // TUnit creates a class instance per test and disposes it afterwards.
    public async ValueTask DisposeAsync() => await _supervisor.DisposeAsync();

    [Test]
    public async Task Start_connects_attaches_publisher_and_becomes_ready(CancellationToken ct)
    {
        _supervisor.Start();

        var publisher = await _supervisor.WaitReadyAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);

        await Assert.That(publisher).IsSameReferenceAs(_broker.Last!.Publisher);
        await Assert.That(_supervisor.State).IsEqualTo(SupervisorState.Ready);
    }

    [Test]
    public async Task Is_idle_and_not_ready_before_start(CancellationToken ct)
    {
        await Assert.That(_supervisor.State).IsEqualTo(SupervisorState.Idle);
        await Assert.That(_supervisor.WaitReadyAsync(ct).IsCompleted).IsFalse();
        await Assert.That(_broker.ConnectAttempts).IsEqualTo(0);
    }

    [Test]
    public async Task Initial_connect_failure_is_retried_until_broker_is_reachable(CancellationToken ct)
    {
        _broker.FailNextConnect(new InvalidOperationException("broker down"));
        _broker.FailNextConnect(new InvalidOperationException("broker still down"));

        _supervisor.Start();
        await Eventually.TrueAsync(() => _supervisor.State == SupervisorState.Faulted, "first attempt fails", ct);

        await Eventually.TrueWhileAdvancingAsync(_time, Step, () => _supervisor.State == SupervisorState.Ready,
            "supervisor keeps retrying with back-off", ct);

        await Assert.That(_broker.ConnectAttempts).IsEqualTo(3);
    }

    [Test]
    public async Task Client_recovery_goes_through_recovering_back_to_ready_without_rebuild(CancellationToken ct)
    {
        _supervisor.Start();
        await Eventually.TrueAsync(() => _supervisor.State == SupervisorState.Ready, "connected", ct);
        var connection = _broker.Last!;

        connection.SetConnectionState(State.Reconnecting);
        connection.SetPublisherState(State.Reconnecting);
        await Eventually.TrueAsync(() => _supervisor.State == SupervisorState.Recovering, "client is recovering", ct);
        var waiting = _supervisor.WaitReadyAsync(ct);
        await Assert.That(waiting.IsCompleted).IsFalse();

        connection.SetConnectionState(State.Open);
        await Task.Delay(50, ct);
        await Assert.That(_supervisor.State).IsEqualTo(SupervisorState.Recovering)
            .Because("the publisher is not re-attached yet (C4)");

        connection.SetPublisherState(State.Open);
        var publisher = await waiting.WaitAsync(TimeSpan.FromSeconds(5), ct);

        await Assert.That(publisher).IsSameReferenceAs(connection.Publisher);
        await Assert.That(_broker.ConnectAttempts).IsEqualTo(1);
    }

    [Test]
    public async Task Connection_closed_after_client_gave_up_is_rebuilt(CancellationToken ct)
    {
        _supervisor.Start();
        await Eventually.TrueAsync(() => _supervisor.State == SupervisorState.Ready, "connected", ct);
        var first = _broker.Last!;

        first.SetConnectionState(State.Closed);

        await Eventually.TrueWhileAdvancingAsync(_time, Step,
            () => _broker.Connections.Count == 2 && _supervisor.State == SupervisorState.Ready, "rebuilt", ct);
        await Assert.That(first.PublisherClosed).IsTrue();
        await Assert.That(first.ConnectionClosed).IsTrue();
        await Assert.That(await _supervisor.WaitReadyAsync(ct)).IsSameReferenceAs(_broker.Last!.Publisher);
    }

    [Test]
    public async Task Publisher_closed_is_rebuilt(CancellationToken ct)
    {
        _supervisor.Start();
        await Eventually.TrueAsync(() => _supervisor.State == SupervisorState.Ready, "connected", ct);

        _broker.Last!.SetPublisherState(State.Closed);

        await Eventually.TrueWhileAdvancingAsync(_time, Step, () => _broker.Connections.Count == 2, "rebuilt", ct);
    }

    [Test]
    public async Task Publisher_stuck_while_connection_open_is_rebuilt_after_grace_period(CancellationToken ct)
    {
        _supervisor.Start();
        await Eventually.TrueAsync(() => _supervisor.State == SupervisorState.Ready, "connected", ct);
        var first = _broker.Last!;

        first.SetPublisherState(State.Reconnecting);
        await Eventually.TrueAsync(() => _supervisor.State == SupervisorState.Recovering, "publisher re-attaching", ct);

        _time.Advance(TimeSpan.FromSeconds(5));
        await Task.Delay(50, ct);
        await Assert.That(_broker.Connections.Count).IsEqualTo(1).Because("still within the grace period");

        await Eventually.TrueWhileAdvancingAsync(_time, Step, () => _broker.Connections.Count == 2, "grace expired", ct);
        await Assert.That(first.ConnectionClosed).IsTrue();
    }

    [Test]
    public async Task Long_client_recovery_is_never_interrupted(CancellationToken ct)
    {
        _supervisor.Start();
        await Eventually.TrueAsync(() => _supervisor.State == SupervisorState.Ready, "connected", ct);
        var connection = _broker.Last!;

        connection.SetConnectionState(State.Reconnecting);
        connection.SetPublisherState(State.Reconnecting);
        for (var i = 0; i < 20; i++)
        {
            _time.Advance(TimeSpan.FromMinutes(1));
            await Task.Delay(5, ct);
        }

        await Assert.That(_supervisor.State).IsEqualTo(SupervisorState.Recovering);
        await Assert.That(_broker.ConnectAttempts).IsEqualTo(1);
    }

    [Test]
    public async Task Repeated_publish_timeouts_trigger_rebuild(CancellationToken ct)
    {
        _supervisor.Start();
        var publisher = await _supervisor.WaitReadyAsync(ct);

        _supervisor.ReportPublishTimeout(publisher);
        _supervisor.ReportPublishTimeout(publisher);
        _supervisor.ReportPublishSucceeded(publisher);
        _supervisor.ReportPublishTimeout(publisher);
        _supervisor.ReportPublishTimeout(publisher);
        await Task.Delay(50, ct);
        await Assert.That(_broker.Connections.Count).IsEqualTo(1).Because("a success resets the counter");

        _supervisor.ReportPublishTimeout(publisher);
        await Eventually.TrueWhileAdvancingAsync(_time, Step, () => _broker.Connections.Count == 2,
            "third consecutive timeout", ct);
    }

    [Test]
    public async Task Missing_target_triggers_immediate_rebuild(CancellationToken ct)
    {
        _supervisor.Start();
        var publisher = await _supervisor.WaitReadyAsync(ct);

        _supervisor.ReportTargetMissing(publisher);

        await Eventually.TrueWhileAdvancingAsync(_time, Step, () => _broker.Connections.Count == 2, "rebuilt", ct);
        await Assert.That(_broker.Connections[0].ConnectionClosed).IsTrue();
    }

    [Test]
    public async Task Timeouts_reported_for_an_old_publisher_are_ignored(CancellationToken ct)
    {
        _supervisor.Start();
        await Eventually.TrueAsync(() => _supervisor.State == SupervisorState.Ready, "connected", ct);
        var stale = Substitute.For<IPublisher>();

        for (var i = 0; i < 10; i++)
        {
            _supervisor.ReportPublishTimeout(stale);
        }

        await Task.Delay(50, ct);
        await Assert.That(_broker.Connections.Count).IsEqualTo(1);
    }

    [Test]
    public async Task Hanging_connect_is_abandoned_after_timeout_and_late_connection_is_closed(CancellationToken ct)
    {
        var pending = new TaskCompletionSource<IConnection>();
        _broker.HangNextConnect(pending);

        _supervisor.Start();
        await Eventually.TrueWhileAdvancingAsync(_time, TimeSpan.FromSeconds(10),
            () => _supervisor.State == SupervisorState.Ready, "timed out, retried, connected", ct);

        var late = new FakeConnection();
        pending.SetResult(late.Connection);
        await Eventually.TrueAsync(() => late.ConnectionClosed, "a late connection must not leak", ct);
    }

    [Test]
    public async Task Topology_hook_runs_on_every_connect_and_its_failure_causes_retry(CancellationToken ct)
    {
        var calls = 0;
        await using var supervisor = new ConnectionSupervisor(
            _broker.Environment,
            new ConnectionSupervisorOptions
            {
                MinReconnectDelay = TimeSpan.FromSeconds(1),
                MaxReconnectDelay = TimeSpan.FromSeconds(1),
                OnConnectedAsync = (_, _) => Interlocked.Increment(ref calls) == 1
                    ? throw new InvalidOperationException("declare failed")
                    : Task.CompletedTask,
            },
            _time);

        supervisor.Start();
        await Eventually.TrueWhileAdvancingAsync(_time, Step, () => supervisor.State == SupervisorState.Ready,
            "second attempt succeeds", ct);

        await Assert.That(calls).IsEqualTo(2);
        await Assert.That(_broker.Connections[0].ConnectionClosed).IsTrue();
    }

    [Test]
    public async Task Readiness_is_withdrawn_before_a_slow_teardown(CancellationToken ct)
    {
        _supervisor.Start();
        var publisher = await _supervisor.WaitReadyAsync(ct);
        var first = _broker.Last!;
        first.CloseGate = new TaskCompletionSource(); // closing hangs until released

        _supervisor.ReportTargetMissing(publisher);
        await Eventually.TrueAsync(() => first.PublisherClosed, "teardown started", ct);

        await Assert.That(_supervisor.State).IsEqualTo(SupervisorState.Faulted);
        await Assert.That(_supervisor.WaitReadyAsync(ct).IsCompleted).IsFalse()
            .Because("a publisher that is being closed must not be handed out");

        first.CloseGate.SetResult();
        await Eventually.TrueWhileAdvancingAsync(_time, Step, () => _supervisor.State == SupervisorState.Ready, "rebuilt", ct);
        await Assert.That(await _supervisor.WaitReadyAsync(ct)).IsSameReferenceAs(_broker.Last!.Publisher);
    }

    [Test]
    public async Task Configuration_errors_are_reported_once_per_distinct_error_and_retrying_continues(CancellationToken ct)
    {
        var reported = new List<string>();
        _broker.FailNextConnect(new PreconditionFailedException("queue exists with another type, response code: 409"));
        _broker.FailNextConnect(new PreconditionFailedException("queue exists with another type, response code: 409"));
        _broker.FailNextConnect(new InvalidOperationException("ordinary outage"));
        await using var supervisor = new ConnectionSupervisor(
            _broker.Environment,
            new ConnectionSupervisorOptions
            {
                MinReconnectDelay = TimeSpan.FromSeconds(1),
                MaxReconnectDelay = TimeSpan.FromSeconds(1),
                OnConfigurationError = (reason, _) =>
                {
                    lock (reported)
                    {
                        reported.Add(reason.Reason);
                    }
                },
            },
            _time);

        supervisor.Start();
        await Eventually.TrueWhileAdvancingAsync(_time, Step, () => supervisor.State == SupervisorState.Ready,
            "keeps retrying until the broker side is fixed", ct);

        await Assert.That(reported.Count).IsEqualTo(1);
        await Assert.That(reported[0]).Contains("precondition failed");
    }

    [Test]
    public async Task Repeated_attach_failures_on_a_working_connection_are_reported_as_configuration_error(CancellationToken ct)
    {
        var reported = new List<string>();
        var created = 0;
        _broker.OnCreated = c =>
        {
            if (Interlocked.Increment(ref created) <= ConfigurationErrorDetector.AttachFailuresBeforeReport)
            {
                // What the client does when the exchange is missing: the attach just times out.
                c.Builder.BuildAsync(Arg.Any<CancellationToken>())
                    .Returns(Task.FromException<IPublisher>(new TimeoutException("The operation has timed out.")));
            }
        };
        await using var supervisor = new ConnectionSupervisor(
            _broker.Environment,
            new ConnectionSupervisorOptions
            {
                MinReconnectDelay = TimeSpan.FromSeconds(1),
                MaxReconnectDelay = TimeSpan.FromSeconds(1),
                OnConfigurationError = (reason, _) =>
                {
                    lock (reported)
                    {
                        reported.Add(reason.Reason);
                    }
                },
            },
            _time);

        supervisor.Start();
        await Eventually.TrueWhileAdvancingAsync(_time, Step, () => supervisor.State == SupervisorState.Ready,
            "attaches once the target exists", ct);

        await Assert.That(reported.Count).IsEqualTo(1);
        await Assert.That(reported[0]).IsEqualTo(ConfigurationErrorDetector.AttachFailureReason);
    }

    [Test]
    public async Task Dispose_with_a_budget_does_not_wait_for_hanging_closes(CancellationToken ct)
    {
        var broker = new FakeEnvironment { OnCreated = c => c.CloseGate = new TaskCompletionSource() };
        var supervisor = new ConnectionSupervisor(broker.Environment, new ConnectionSupervisorOptions(), TimeProvider.System);
        supervisor.Start();
        await supervisor.WaitReadyAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);

        var started = DateTime.UtcNow;
        await supervisor.DisposeAsync(TimeSpan.FromMilliseconds(300));

        await Assert.That(DateTime.UtcNow - started).IsLessThan(TimeSpan.FromSeconds(2))
            .Because("publisher, connection and environment share one 300 ms budget instead of 5 s each");
    }

    [Test]
    public async Task Dispose_closes_publisher_connection_environment_and_cancels_waiters(CancellationToken ct)
    {
        _supervisor.Start();
        await Eventually.TrueAsync(() => _supervisor.State == SupervisorState.Ready, "connected", ct);
        var connection = _broker.Last!;
        connection.SetConnectionState(State.Reconnecting);
        await Eventually.TrueAsync(() => _supervisor.State == SupervisorState.Recovering, "not ready", ct);
        var waiter = _supervisor.WaitReadyAsync(ct);

        await _supervisor.DisposeAsync();

        await Assert.That(connection.PublisherClosed).IsTrue();
        await Assert.That(connection.ConnectionClosed).IsTrue();
        await _broker.Environment.Received(1).CloseAsync();
        await Assert.That(async () => await waiter).Throws<OperationCanceledException>();
        await Assert.That(_supervisor.State).IsEqualTo(SupervisorState.Disposed);
        await Assert.That(_supervisor.Start).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task Dispose_during_backoff_returns_promptly(CancellationToken ct)
    {
        _broker.FailNextConnect(new InvalidOperationException("down"));
        _supervisor.Start();
        await Eventually.TrueAsync(() => _supervisor.State == SupervisorState.Faulted, "in back-off", ct);

        await _supervisor.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), ct);

        await Assert.That(_supervisor.State).IsEqualTo(SupervisorState.Disposed);
    }
}
