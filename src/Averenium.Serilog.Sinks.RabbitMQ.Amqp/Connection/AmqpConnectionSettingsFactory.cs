using RabbitMQ.AMQP.Client;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;

/// <summary>Builds client <see cref="ConnectionSettings"/> with the sink's resilience defaults.</summary>
internal static class AmqpConnectionSettingsFactory
{
    public static ConnectionSettings Create(
        IReadOnlyList<Uri> uris,
        string containerId,
        TimeSpan minReconnectDelay,
        TimeSpan maxReconnectDelay)
    {
        ArgumentNullException.ThrowIfNull(uris);
        if (uris.Count == 0)
        {
            throw new ArgumentException("At least one broker URI is required.", nameof(uris));
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(containerId);

        var recovery = new RecoveryConfiguration()
            .Activated(true)
            // Topology is (re)declared by the sink itself after each connect; the client only
            // replays what was declared on the same connection object.
            .Topology(false)
            .BackOffDelayPolicy(new EndlessBackOffDelayPolicy(minReconnectDelay, maxReconnectDelay));

        var builder = ConnectionSettingsBuilder.Create()
            .ContainerId(containerId)
            .RecoveryConfiguration(recovery);

        builder = uris.Count == 1
            ? builder.Uri(uris[0])
            : builder.Uris(uris).UriSelector(new RoundRobinUriSelector());

        return builder.Build();
    }
}
