using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection.Models;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Serilog.Debugging;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.IntegrationTests.Infrastructure;

/// <summary>A real <see cref="ConnectionSupervisor"/> with short timings and a recorded state history.</summary>
internal sealed class SupervisorHarness : IAsyncDisposable
{
    private readonly List<SupervisorState> _history = [];

    public SupervisorHarness(IReadOnlyList<Uri> uris, TestTopology topology)
    {
        var settings = AmqpConnectionSettingsFactory.Create(
            uris,
            "it-" + Guid.NewGuid().ToString("N")[..8],
            minReconnectDelay: TimeSpan.FromMilliseconds(200),
            maxReconnectDelay: TimeSpan.FromSeconds(2));

        Supervisor = new ConnectionSupervisor(
            AmqpEnvironment.Create(settings),
            new ConnectionSupervisorOptions
            {
                ConfigurePublisher = b => b.Exchange(topology.Exchange).Key(topology.RoutingKey),
                MinReconnectDelay = TimeSpan.FromMilliseconds(200),
                MaxReconnectDelay = TimeSpan.FromSeconds(2),
                ConnectTimeout = TimeSpan.FromSeconds(10),
                PublisherRecoveryGrace = TimeSpan.FromSeconds(5),
                HealthCheckInterval = TimeSpan.FromMilliseconds(500),
                // A stalled client recovery (C13) is abandoned after 5 s here instead of MaxReconnectDelay + 15 s.
                ClientRecoveryTimeout = TimeSpan.FromSeconds(5),
            });

        // Diagnostics only: SelfLog is global, so with disruptive tests running side by side (DisruptiveBrokerTests)
        // a line can show up in another test's output. Nothing asserts on it.
        SelfLog.Enable(m => Console.WriteLine($"{DateTime.Now:HH:mm:ss.fff} selflog {m}"));
        Supervisor.StateChanged += s =>
        {
            Console.WriteLine($"{DateTime.Now:HH:mm:ss.fff} state {s}");
            lock (_history)
            {
                _history.Add(s);
            }
        };
    }

    public static void Log(string message) => Console.WriteLine($"{DateTime.Now:HH:mm:ss.fff} test {message}");

    public ConnectionSupervisor Supervisor { get; }

    public IReadOnlyList<SupervisorState> History
    {
        get
        {
            lock (_history)
            {
                return [.. _history];
            }
        }
    }

    public void ClearHistory()
    {
        lock (_history)
        {
            _history.Clear();
        }
    }

    public Task<IPublisher> WaitReadyAsync(TimeSpan timeout, CancellationToken ct) =>
        Supervisor.WaitReadyAsync(ct).WaitAsync(timeout, ct);

    /// <summary>Publishes one message on the current ready publisher and returns the broker outcome.</summary>
    public async Task<OutcomeState> PublishAsync(string body, CancellationToken ct)
    {
        var publisher = await WaitReadyAsync(TimeSpan.FromSeconds(30), ct);
        var result = await publisher.PublishAsync(new AmqpMessage(body), ct).WaitAsync(TimeSpan.FromSeconds(10), ct);
        return result.Outcome.State;
    }

    /// <summary>
    /// Publishes until the broker accepts, handling a missing target the way the sink's publish loop does
    /// (<c>Rejected amqp:not-found</c> → <see cref="ConnectionSupervisor.ReportTargetMissing"/> → rebuild). Newer brokers
    /// (4.2+) may let a publisher attach before its exchange exists and then reject publishes.
    /// </summary>
    public async Task<OutcomeState> PublishUntilAcceptedAsync(string body, TimeSpan timeout, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            var publisher = await WaitReadyAsync(TimeSpan.FromSeconds(30), ct);
            var result = await publisher.PublishAsync(new AmqpMessage(body), ct).WaitAsync(TimeSpan.FromSeconds(10), ct);
            if (result.Outcome.State == OutcomeState.Accepted || DateTime.UtcNow > deadline)
            {
                return result.Outcome.State;
            }

            Log($"publish {result.Outcome.State} ({result.Outcome.Error}); retrying");
            if (result.Outcome.Error?.ErrorCode == "amqp:not-found")
            {
                Supervisor.ReportTargetMissing(publisher);
            }

            await Task.Delay(200, ct);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await Supervisor.DisposeAsync();
        SelfLog.Disable();
    }
}
