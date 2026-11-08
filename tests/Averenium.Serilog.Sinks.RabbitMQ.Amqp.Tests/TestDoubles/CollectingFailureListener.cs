using Serilog.Core;
using Serilog.Events;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.TestDoubles;

/// <summary>Records everything reported to the logging failure listener.</summary>
internal sealed class CollectingFailureListener : ILoggingFailureListener
{
    private readonly Lock _gate = new();
    private readonly List<(LoggingFailureKind Kind, string Message, LogEvent Event)> _events = [];
    private readonly List<(LoggingFailureKind Kind, string Message)> _notifications = [];

    /// <summary>Reports without events (e.g. configuration errors).</summary>
    public IReadOnlyList<(LoggingFailureKind Kind, string Message)> Notifications
    {
        get
        {
            lock (_gate)
            {
                return [.. _notifications];
            }
        }
    }

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
            if (events is null)
            {
                _notifications.Add((kind, message));
            }

            foreach (var logEvent in events ?? [])
            {
                _events.Add((kind, message, logEvent));
            }
        }
    }
}
