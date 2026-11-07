using Serilog.Events;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Publishing;

/// <summary>
/// A formatted log event waiting for broker acceptance. The body is formatted once and the
/// <see cref="MessageId"/> is stable across retries, so consumers can de-duplicate (at-least-once delivery).
/// </summary>
internal sealed class PendingMessage
{
    public PendingMessage(LogEvent logEvent, byte[] body, string messageId, string? routingKey)
    {
        LogEvent = logEvent;
        Body = body;
        MessageId = messageId;
        RoutingKey = routingKey;
    }

    public LogEvent LogEvent { get; }

    public byte[] Body { get; }

    public string MessageId { get; }

    /// <summary>Per-event routing key when an anonymous publisher with per-message addresses is used.</summary>
    public string? RoutingKey { get; }

    /// <summary>How many times the broker released this message (e.g. unroutable).</summary>
    public int ReleasedCount { get; set; }
}
