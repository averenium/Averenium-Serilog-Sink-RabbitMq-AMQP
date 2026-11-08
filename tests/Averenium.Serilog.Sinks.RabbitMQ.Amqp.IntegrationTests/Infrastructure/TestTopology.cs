namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.IntegrationTests.Infrastructure;

/// <summary>Unique names per test so tests never share broker state.</summary>
internal sealed record TestTopology(string Exchange, string Queue, string RoutingKey)
{
    public static TestTopology Unique()
    {
        var id = Guid.NewGuid().ToString("N")[..8];
        return new TestTopology($"logs-{id}", $"logs-q-{id}", "app");
    }
}
