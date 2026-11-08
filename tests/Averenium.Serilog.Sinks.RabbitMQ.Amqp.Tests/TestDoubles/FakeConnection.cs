using NSubstitute;
using RabbitMQ.AMQP.Client;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.TestDoubles;

/// <summary>A fake open connection with one publisher that accepts every message by default.</summary>
internal sealed class FakeConnection
{
    public FakeConnection()
    {
        Connection.State.Returns(_ => ConnectionState);
        Connection.CloseAsync().Returns(_ =>
        {
            ConnectionClosed = true;
            return CloseGate?.Task ?? Task.CompletedTask;
        });

        Publisher.State.Returns(_ => PublisherState);
        Publisher.CloseAsync().Returns(_ =>
        {
            PublisherClosed = true;
            return CloseGate?.Task ?? Task.CompletedTask;
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

    /// <summary>When set, closing the publisher/connection does not complete until the gate is completed.</summary>
    public TaskCompletionSource? CloseGate { get; set; }

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
