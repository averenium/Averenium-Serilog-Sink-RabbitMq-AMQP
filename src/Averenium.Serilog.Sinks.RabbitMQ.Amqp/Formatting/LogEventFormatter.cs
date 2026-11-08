using System.Globalization;
using System.Text;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting.Models;
using Serilog.Events;
using Serilog.Formatting;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting;

/// <summary>
/// Turns a <see cref="LogEvent"/> into a <see cref="FormattedEvent"/> on the background formatter thread: formats the
/// body once, evaluates <c>RoutingKeySelector</c> and <c>MessageProperties</c> once, and validates property values,
/// so that nothing user-supplied can fail later on every publish attempt.
/// </summary>
internal sealed class LogEventFormatter
{
    private readonly ITextFormatter _formatter;
    private readonly Func<LogEvent, string>? _routingKeySelector;
    private readonly Func<LogEvent, IEnumerable<KeyValuePair<string, object>>>? _messageProperties;
    private readonly Func<LogEvent, string?>? _correlationId;
    private readonly TimeProvider _time;

    /// <param name="formatter">Body formatter.</param>
    /// <param name="routingKeySelector">Per-event routing key.</param>
    /// <param name="messageProperties">Per-event application properties.</param>
    /// <param name="correlationId">Per-event correlation id (see <see cref="CorrelationIdFromTraceId"/>).</param>
    /// <param name="timeProvider">Clock for <see cref="FormattedEvent.FormattedAt"/>.</param>
    public LogEventFormatter(
        ITextFormatter formatter,
        Func<LogEvent, string>? routingKeySelector = null,
        Func<LogEvent, IEnumerable<KeyValuePair<string, object>>>? messageProperties = null,
        Func<LogEvent, string?>? correlationId = null,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(formatter);
        _formatter = formatter;
        _routingKeySelector = routingKeySelector;
        _messageProperties = messageProperties;
        _correlationId = correlationId;
        _time = timeProvider ?? TimeProvider.System;
    }

    /// <summary>The default correlation id: the event's W3C trace id, if it has one.</summary>
    public static string? CorrelationIdFromTraceId(LogEvent logEvent) =>
        logEvent.TraceId is { } traceId && traceId != default ? traceId.ToHexString() : null;

    /// <summary>Throws if the formatter, the routing-key selector or <c>MessageProperties</c> fail (event is dropped).</summary>
    public FormattedEvent Format(LogEvent logEvent)
    {
        ArgumentNullException.ThrowIfNull(logEvent);

        using var writer = new StringWriter(CultureInfo.InvariantCulture);
        _formatter.Format(logEvent, writer);
        var body = EncodeTrimmed(writer.GetStringBuilder());

        var routingKey = _routingKeySelector?.Invoke(logEvent);
        if (_routingKeySelector is not null && string.IsNullOrEmpty(routingKey))
        {
            throw new InvalidOperationException("RoutingKeySelector returned an empty routing key.");
        }

        var sourceContext = logEvent.Properties.TryGetValue("SourceContext", out var value)
            && value is ScalarValue { Value: string source } ? source : null;

        return new FormattedEvent(
            body,
            Guid.NewGuid().ToString("N"),
            logEvent.Level,
            logEvent.Timestamp,
            sourceContext,
            routingKey,
            EvaluateProperties(logEvent),
            _correlationId?.Invoke(logEvent),
            _time.GetTimestamp());
    }

    private KeyValuePair<string, object>[]? EvaluateProperties(LogEvent logEvent)
    {
        if (_messageProperties?.Invoke(logEvent) is not { } properties)
        {
            return null;
        }

        var result = new List<KeyValuePair<string, object>>();
        foreach (var (key, value) in properties)
        {
            ArgumentException.ThrowIfNullOrEmpty(key);
            result.Add(new KeyValuePair<string, object>(key, ToAmqpValue(key, value)));
        }

        return result.Count == 0 ? null : [.. result];
    }

    /// <summary>
    /// Only AMQP simple types that the client encodes and RabbitMQ accepts (verified by tests); anything else would fail
    /// at send time on every retry. <see cref="decimal"/> and <see cref="char"/> are sent as strings (lossless).
    /// </summary>
    private static object ToAmqpValue(string key, object? value) => value switch
    {
        string or bool or byte or sbyte or short or ushort or int or uint or long or ulong
            or float or double or Guid => value,
        decimal number => number.ToString(CultureInfo.InvariantCulture),
        char character => character.ToString(),
        DateTime dateTime => dateTime.ToUniversalTime(),
        DateTimeOffset dateTimeOffset => dateTimeOffset.UtcDateTime,
        Enum enumValue => enumValue.ToString(),
        null => throw new InvalidOperationException($"Message property '{key}' is null."),
        _ => throw new InvalidOperationException(
            $"Message property '{key}' has unsupported type {value.GetType().Name}; use string, number, bool, Guid or DateTime."),
    };

    /// <summary>UTF-8 bytes of the formatted text without trailing line breaks, encoded straight from the builder's chunks.</summary>
    private static byte[] EncodeTrimmed(StringBuilder text)
    {
        var length = text.Length;
        while (length > 0 && text[length - 1] is '\r' or '\n')
        {
            length--;
        }

        text.Length = length;

        // An Encoder keeps state between chunks, so surrogate pairs split across chunk boundaries stay intact.
        var encoder = Encoding.UTF8.GetEncoder();
        var byteCount = 0;
        foreach (var chunk in text.GetChunks())
        {
            byteCount += encoder.GetByteCount(chunk.Span, flush: false);
        }

        byteCount += encoder.GetByteCount([], flush: true);
        encoder.Reset();

        var body = new byte[byteCount];
        var offset = 0;
        foreach (var chunk in text.GetChunks())
        {
            offset += encoder.GetBytes(chunk.Span, body.AsSpan(offset), flush: false);
        }

        encoder.GetBytes([], body.AsSpan(offset), flush: true);
        return body;
    }
}
