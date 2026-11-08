using Serilog.Events;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting.Models;

/// <summary>
/// A log event after background formatting: everything needed to publish it, without the <see cref="LogEvent"/>
/// (which is released right after formatting — no double memory). This is what the long outage queue holds.
/// </summary>
internal sealed class FormattedEvent
{
    /// <summary>Approximate managed overhead of a record besides the body (object headers, id string, metadata).</summary>
    public const int OverheadBytes = 192;

    public FormattedEvent(
        byte[] body,
        string messageId,
        LogEventLevel level,
        DateTimeOffset timestamp,
        string? sourceContext,
        string? routingKey,
        KeyValuePair<string, object>[]? properties,
        string? correlationId = null,
        long formattedAt = 0)
    {
        Body = body;
        MessageId = messageId;
        Level = level;
        Timestamp = timestamp;
        SourceContext = sourceContext;
        RoutingKey = routingKey;
        Properties = properties;
        CorrelationId = correlationId;
        FormattedAt = formattedAt;
        EstimatedSize = EstimateSize(body, routingKey, sourceContext, properties) + (2 * (correlationId?.Length ?? 0));
    }

    /// <summary>AMQP <c>correlation-id</c> (by default the event's trace id).</summary>
    public string? CorrelationId { get; }

    /// <summary><see cref="TimeProvider.GetTimestamp"/> when formatted — the age used by <c>FallbackAfter</c>.</summary>
    public long FormattedAt { get; }

    /// <summary>Formatted event (UTF-8), sent as the message body.</summary>
    public byte[] Body { get; }

    /// <summary>Stable across retries, so consumers can de-duplicate (at-least-once delivery).</summary>
    public string MessageId { get; }

    public LogEventLevel Level { get; }

    public DateTimeOffset Timestamp { get; }

    public string? SourceContext { get; }

    /// <summary>Per-event routing key when an anonymous publisher with per-message addresses is used.</summary>
    public string? RoutingKey { get; }

    /// <summary>Additional AMQP application properties from <c>MessageProperties</c>.</summary>
    public KeyValuePair<string, object>[]? Properties { get; }

    /// <summary>
    /// Approximate memory held by this record — what <c>QueueSizeLimitBytes</c> counts. Close to, but not exactly,
    /// the managed heap size; the point is a predictable bound, not byte precision.
    /// </summary>
    public int EstimatedSize { get; }

    private static int EstimateSize(byte[] body, string? routingKey, string? sourceContext, KeyValuePair<string, object>[]? properties)
    {
        long size = OverheadBytes + body.Length + (2L * (routingKey?.Length ?? 0)) + (2L * (sourceContext?.Length ?? 0));
        if (properties is not null)
        {
            foreach (var (key, value) in properties)
            {
                size += 48 + (2L * key.Length) + (value is string s ? 2L * s.Length : 16);
            }
        }

        return (int)Math.Min(int.MaxValue, size);
    }
}
