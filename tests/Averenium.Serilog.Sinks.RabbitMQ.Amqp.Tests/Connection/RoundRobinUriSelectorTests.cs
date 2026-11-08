using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Connection;

public sealed class RoundRobinUriSelectorTests
{
    [Test]
    public async Task Round_robin_selector_cycles_through_all_nodes()
    {
        var uris = new List<Uri> { new("amqp://a:5672"), new("amqp://b:5672"), new("amqp://c:5672") };
        var selector = new RoundRobinUriSelector();

        var picked = Enumerable.Range(0, 7).Select(_ => selector.Select(uris).Host);

        await Assert.That(string.Join(",", picked)).IsEqualTo("a,b,c,a,b,c,a");
    }
}
