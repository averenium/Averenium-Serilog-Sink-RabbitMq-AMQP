using Serilog.Core;
using Serilog.Events;
using Serilog.Parsing;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests;

internal static class TestEvents
{
    private static readonly MessageTemplate Template = new MessageTemplateParser().Parse("Event {N}");

    public static LogEvent Create(int n, LogEventLevel level = LogEventLevel.Information, string? sourceContext = null)
    {
        var properties = new List<LogEventProperty> { new("N", new ScalarValue(n)) };
        if (sourceContext is not null)
        {
            properties.Add(new LogEventProperty("SourceContext", new ScalarValue(sourceContext)));
        }

        return new LogEvent(DateTimeOffset.UtcNow, level, null, Template, properties);
    }

    public static int N(this LogEvent logEvent) => (int)((ScalarValue)logEvent.Properties["N"]).Value!;
}

/// <summary>Records everything reported to the logging failure listener.</summary>
internal sealed class CollectingFailureListener : ILoggingFailureListener
{
    private readonly Lock _gate = new();
    private readonly List<(LoggingFailureKind Kind, string Message, LogEvent Event)> _events = [];

    public IReadOnlyList<(LoggingFailureKind Kind, string Message, LogEvent Event)> Events
    {
        get
        {
            lock (_gate)
            {
                return [.. _events];
            }
        }
    }

    public void OnLoggingFailed(object sender, LoggingFailureKind kind, string message, IReadOnlyCollection<LogEvent>? events, Exception? exception)
    {
        lock (_gate)
        {
            foreach (var logEvent in events ?? [])
            {
                _events.Add((kind, message, logEvent));
            }
        }
    }
}

/// <summary>Collects events written to it, e.g. as the fallback of <c>WriteTo.FallbackChain</c>.</summary>
internal sealed class CollectingSink : ILogEventSink
{
    private readonly Lock _gate = new();
    private readonly List<LogEvent> _events = [];

    public IReadOnlyList<LogEvent> Events
    {
        get
        {
            lock (_gate)
            {
                return [.. _events];
            }
        }
    }

    public void Emit(LogEvent logEvent)
    {
        lock (_gate)
        {
            _events.Add(logEvent);
        }
    }
}
