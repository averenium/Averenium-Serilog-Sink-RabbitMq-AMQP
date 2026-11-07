namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp;

/// <summary>
/// What the sink does when its in-memory buffer is full (for example during a long broker outage).
/// Dropped events are always reported to the logging failure listener, so
/// <c>WriteTo.FallbackChain(...)</c> can still write them elsewhere.
/// </summary>
public enum OverflowPolicy
{
    /// <summary>Discard the oldest buffered event to make room — keeps the most recent logs. Default.</summary>
    DropOldest = 0,

    /// <summary>Discard the incoming event — keeps the beginning of an incident.</summary>
    DropNewest = 1,
}
