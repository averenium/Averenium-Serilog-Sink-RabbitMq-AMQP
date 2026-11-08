using System.Collections.Concurrent;
using Serilog.Events;
using Serilog.Formatting;
using Serilog.Formatting.Compact;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Support;

/// <summary>
/// A formatter that records which instances formatted something, by the tag passed to its constructor — proves that
/// <c>Serilog.Settings.Configuration</c> created it with the configured arguments. Public: it is created by type name.
/// </summary>
public sealed class RecordingFormatter : ITextFormatter
{
    private static readonly ConcurrentDictionary<string, int> Formatted = new();
    private readonly CompactJsonFormatter _inner = new();
    private readonly string _tag;

    public RecordingFormatter()
        : this("default")
    {
    }

    public RecordingFormatter(string tag) => _tag = tag;

    public static bool HasFormatted(string tag) => Formatted.ContainsKey(tag);

    public void Format(LogEvent logEvent, TextWriter output)
    {
        Formatted.AddOrUpdate(_tag, 1, (_, n) => n + 1);
        _inner.Format(logEvent, output);
    }
}
