using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting.Models;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Publishing.Models;

/// <summary>
/// A formatted record inside the publish window, with its own retry schedule — one message's progress never
/// resets another message's back-off.
/// </summary>
internal sealed class PendingMessage
{
    public PendingMessage(FormattedEvent record) => Record = record;

    public FormattedEvent Record { get; }

    /// <summary>How many times the broker released this message (e.g. unroutable); bounded by MaxReleasedRetries.</summary>
    public int ReleasedCount { get; set; }

    /// <summary>Failed attempts for any other retryable reason; drives this message's back-off, never a drop.</summary>
    public int FailedAttempts { get; set; }

    /// <summary><see cref="TimeProvider.GetTimestamp"/> before which the message must not be re-sent (0 = now).</summary>
    public long NotBefore { get; set; }
}
