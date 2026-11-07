using NSubstitute;
using RabbitMQ.AMQP.Client;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Connection;

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
            Connections.Add(connection);
            return Task.FromResult(connection.Connection);
        }
    }
}

internal sealed class FakeConnection
{
    public FakeConnection()
    {
        Connection.State.Returns(_ => ConnectionState);
        Connection.CloseAsync().Returns(_ =>
        {
            ConnectionClosed = true;
            return Task.CompletedTask;
        });

        Publisher.State.Returns(_ => PublisherState);
        Publisher.CloseAsync().Returns(_ =>
        {
            PublisherClosed = true;
            return Task.CompletedTask;
        });

        Publisher.PublishAsync(Arg.Any<IMessage>(), Arg.Any<CancellationToken>()).Returns(ci =>
        {
            var message = ci.Arg<IMessage>();
            lock (_published)
            {
                _published.Add(message);
            }

            return Task.FromResult(new PublishResult(message, new PublishOutcome(OutcomeState.Accepted, null)));
        });

        Builder.Exchange(Arg.Any<string>()).Returns(Builder);
        Builder.Key(Arg.Any<string>()).Returns(Builder);
        Builder.Queue(Arg.Any<string>()).Returns(Builder);
        Builder.BuildAsync(Arg.Any<CancellationToken>()).Returns(_ => Task.FromResult(Publisher));
        Connection.PublisherBuilder().Returns(Builder);
    }

    private readonly List<IMessage> _published = [];

    public IConnection Connection { get; } = Substitute.For<IConnection>();

    /// <summary>Messages accepted by the fake publisher (it accepts everything by default).</summary>
    public IReadOnlyList<IMessage> Published
    {
        get
        {
            lock (_published)
            {
                return [.. _published];
            }
        }
    }

    public IPublisherBuilder Builder { get; } = Substitute.For<IPublisherBuilder>();

    public IPublisher Publisher { get; } = Substitute.For<IPublisher>();

    public State ConnectionState { get; private set; } = State.Open;

    public State PublisherState { get; private set; } = State.Open;

    public bool ConnectionClosed { get; private set; }

    public bool PublisherClosed { get; private set; }

    public void SetConnectionState(State state)
    {
        var previous = ConnectionState;
        ConnectionState = state;
        Connection.ChangeState += Raise.Event<LifeCycleCallBack>(Connection, previous, state, null);
    }

    public void SetPublisherState(State state)
    {
        var previous = PublisherState;
        PublisherState = state;
        Publisher.ChangeState += Raise.Event<LifeCycleCallBack>(Publisher, previous, state, null);
    }
}
