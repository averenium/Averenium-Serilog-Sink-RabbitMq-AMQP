using System.Diagnostics;
using Serilog.Core;
using Serilog.Events;
using Serilog.Parsing;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Support;

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

    /// <summary>An event carrying a W3C trace context, as Serilog captures it from <see cref="Activity.Current"/>.</summary>
    public static LogEvent WithTrace(int n, ActivityTraceId traceId) =>
        new(DateTimeOffset.UtcNow, LogEventLevel.Information, null, Template,
            [new LogEventProperty("N", new ScalarValue(n))], traceId, ActivitySpanId.CreateRandom());

    public static int N(this LogEvent logEvent) =>
        Convert.ToInt32(((ScalarValue)logEvent.Properties["N"]).Value, System.Globalization.CultureInfo.InvariantCulture);
}
