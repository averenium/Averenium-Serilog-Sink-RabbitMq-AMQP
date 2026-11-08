using System.Collections.Concurrent;
using Serilog;
using Serilog.Configuration;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.TestDoubles;

/// <summary>
/// <c>WriteTo.Collector(name)</c> for <c>Serilog.Settings.Configuration</c> tests (e.g. as the fallback of a
/// <c>FallbackChain</c> in JSON). Must be public: the binder only scans exported types.
/// </summary>
public static class CollectorSinkExtensions
{
    private static readonly ConcurrentDictionary<string, CollectingSink> Sinks = new();

    /// <summary>Writes to the collecting sink registered under <paramref name="name"/>.</summary>
    public static LoggerConfiguration Collector(this LoggerSinkConfiguration sinkConfiguration, string name)
    {
        ArgumentNullException.ThrowIfNull(sinkConfiguration);
        return sinkConfiguration.Sink(Get(name));
    }

    internal static CollectingSink Get(string name) => Sinks.GetOrAdd(name, _ => new CollectingSink());
}
