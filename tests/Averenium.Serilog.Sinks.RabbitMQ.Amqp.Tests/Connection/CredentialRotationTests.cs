using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection.Models;
using Microsoft.Extensions.Time.Testing;
using NSubstitute;
using RabbitMQ.AMQP.Client;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Connection;

/// <summary>The supervisor reconnects through a new environment when the credentials change — event-driven, no polling.</summary>
public sealed class CredentialRotationTests : IAsyncDisposable
{
    private readonly RotatingFakeEnvironments _environments = new();
    private readonly FakeTimeProvider _time = new();
    private readonly ConnectionSupervisor _supervisor;

    public CredentialRotationTests()
    {
        _supervisor = new ConnectionSupervisor(
            _environments,
            new ConnectionSupervisorOptions
            {
                MinReconnectDelay = TimeSpan.FromSeconds(10),
                MaxReconnectDelay = TimeSpan.FromSeconds(30),
                HealthCheckInterval = TimeSpan.FromHours(1), // nothing may depend on a periodic check
            },
            _time);
    }

    public async ValueTask DisposeAsync() => await _supervisor.DisposeAsync();

    [Test]
    public async Task Changed_credentials_on_a_working_connection_reconnect_through_a_new_environment(CancellationToken ct)
    {
        _supervisor.Start();
        await Eventually.TrueAsync(() => _supervisor.State == SupervisorState.Ready, "connected", ct);
        var first = _environments.Created[0];

        _environments.Rotate();

        await Eventually.TrueAsync(() => _environments.CreatedCount == 2 && _supervisor.State == SupervisorState.Ready,
            "connected again with the new credentials", ct);
        var publisher = await _supervisor.WaitReadyAsync(ct);
        await Assert.That(publisher).IsSameReferenceAs(_environments.Created[1].Last!.Publisher);
        await Assert.That(first.Last!.ConnectionClosed).IsTrue();
        await first.Environment.Received(1).CloseAsync();
    }

    [Test]
    public async Task Changed_credentials_while_the_client_keeps_reconnecting_replace_the_environment(CancellationToken ct)
    {
        _supervisor.Start();
        await Eventually.TrueAsync(() => _supervisor.State == SupervisorState.Ready, "connected", ct);
        _environments.Created[0].Last!.SetConnectionState(State.Reconnecting); // e.g. old password no longer accepted
        await Eventually.TrueAsync(() => _supervisor.State == SupervisorState.Recovering, "client recovering", ct);

        _environments.Rotate();

        await Eventually.TrueAsync(() => _environments.CreatedCount == 2 && _supervisor.State == SupervisorState.Ready,
            "the client would retry the old credentials forever; the supervisor switches", ct);
    }

    [Test]
    public async Task Changed_credentials_cut_the_rebuild_back_off_short(CancellationToken ct)
    {
        _environments.Created[0].FailNextConnect(new InvalidOperationException("amqp:unauthorized-access"));
        _supervisor.Start();
        await Eventually.TrueAsync(() => _supervisor.State == SupervisorState.Faulted, "first attempt refused", ct);

        _environments.Rotate(); // no fake time passes: the 10 s back-off must not be waited out

        await Eventually.TrueAsync(() => _supervisor.State == SupervisorState.Ready, "connected with the new credentials", ct);
        await Assert.That(_environments.CreatedCount).IsEqualTo(2);
    }

    [Test]
    public async Task Without_a_change_nothing_is_rebuilt(CancellationToken ct)
    {
        _supervisor.Start();
        await Eventually.TrueAsync(() => _supervisor.State == SupervisorState.Ready, "connected", ct);

        _supervisor.Signal();
        _time.Advance(TimeSpan.FromHours(2));
        await Task.Delay(100, ct);

        await Assert.That(_environments.CreatedCount).IsEqualTo(1);
        await Assert.That(_environments.Created[0].ConnectAttempts).IsEqualTo(1);
    }
}
