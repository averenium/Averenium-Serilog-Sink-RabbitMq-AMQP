namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.IntegrationTests;

internal static class Eventually
{
    public static async Task TrueAsync(Func<bool> condition, string because, TimeSpan timeout, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                Assert.Fail($"Condition not met within {timeout}: {because}");
            }

            await Task.Delay(50, ct);
        }
    }
}
