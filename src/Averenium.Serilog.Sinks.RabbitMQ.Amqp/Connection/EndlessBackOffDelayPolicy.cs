using RabbitMQ.AMQP.Client;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;

/// <summary>
/// Back-off policy for the AMQP client's automatic connection recovery that never becomes inactive.
/// The client's default <see cref="BackOffDelayPolicy"/> stops after 12 attempts and leaves the connection
/// <see cref="State.Closed"/> forever, which is unacceptable for a logging sink (see docs/architecture.md, C3).
/// The client's recovery loop still ends when the connection is closed by us, because it also checks
/// that the connection is in <see cref="State.Reconnecting"/>.
/// </summary>
internal sealed class EndlessBackOffDelayPolicy : IBackOffDelayPolicy
{
    private readonly ExponentialBackoff _backoff;

    public EndlessBackOffDelayPolicy(TimeSpan min, TimeSpan max)
        : this(new ExponentialBackoff(min, max))
    {
    }

    internal EndlessBackOffDelayPolicy(ExponentialBackoff backoff) => _backoff = backoff;

    public int CurrentAttempt => _backoff.Attempt;

    public int Delay() => (int)Math.Min(int.MaxValue, _backoff.Next().TotalMilliseconds);

    public void Reset() => _backoff.Reset();

    public bool IsActive() => true;

    public override string ToString() => $"EndlessBackOffDelayPolicy{{ Attempt={CurrentAttempt} }}";
}
