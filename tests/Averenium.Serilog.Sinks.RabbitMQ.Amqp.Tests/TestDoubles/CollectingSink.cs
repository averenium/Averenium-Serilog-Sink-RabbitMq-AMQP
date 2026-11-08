using Serilog.Core;
using Serilog.Events;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.TestDoubles;

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
