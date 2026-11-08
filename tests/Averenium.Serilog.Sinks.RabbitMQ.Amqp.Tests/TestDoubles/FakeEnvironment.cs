using NSubstitute;
using RabbitMQ.AMQP.Client;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.TestDoubles;

/// <summary>Fake AMQP client: an environment that hands out scripted connections with one publisher each.</summary>
internal sealed class FakeEnvironment
{
    private readonly Lock _gate = new();
    private readonly Queue<Func<Task<IConnection>>> _script = new();

    public FakeEnvironment()
    {
        Environment.CreateConnectionAsync(Arg.Any<CancellationToken>()).Returns(_ => Next());
        Environment.CloseAsync().Returns(Task.CompletedTask);
    }

    public IEnvironment Environment { get; } = Substitute.For<IEnvironment>();

    public List<FakeConnection> Connections { get; } = [];

    public int ConnectAttempts { get; private set; }

    /// <summary>Customises every connection this environment creates.</summary>
    public Action<FakeConnection>? OnCreated { get; set; }

    public FakeConnection? Last
    {
        get
        {
            lock (_gate)
            {
                return Connections.Count == 0 ? null : Connections[^1];
            }
        }
    }

    /// <summary>The next connect attempt fails with <paramref name="exception"/>.</summary>
    public void FailNextConnect(Exception exception)
    {
        lock (_gate)
        {
            _script.Enqueue(() => Task.FromException<IConnection>(exception));
        }
    }

    /// <summary>The next connect attempt never completes (client ignores cancellation).</summary>
    public void HangNextConnect(TaskCompletionSource<IConnection> pending)
    {
        lock (_gate)
        {
            _script.Enqueue(() => pending.Task);
        }
    }

    private Task<IConnection> Next()
    {
        lock (_gate)
        {
            ConnectAttempts++;
            if (_script.TryDequeue(out var scripted))
            {
                return scripted();
            }

            var connection = new FakeConnection();
            OnCreated?.Invoke(connection);
            Connections.Add(connection);
            return Task.FromResult(connection.Connection);
        }
    }
}
