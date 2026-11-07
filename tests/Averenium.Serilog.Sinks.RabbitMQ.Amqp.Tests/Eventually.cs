using Microsoft.Extensions.Time.Testing;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests;

/// <summary>Polling helpers for code that runs on background tasks.</summary>
internal static class Eventually
{
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(5);

    public static async Task TrueAsync(
        Func<bool> condition, string because, CancellationToken cancellationToken, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                Assert.Fail($"Condition not met in time: {because}");
            }

            await Task.Delay(10, cancellationToken);
        }
    }

    /// <summary>Advances fake time in <paramref name="step"/> increments until the condition holds.</summary>
    public static async Task TrueWhileAdvancingAsync(
        FakeTimeProvider time,
        TimeSpan step,
        Func<bool> condition,
        string because,
        CancellationToken cancellationToken,
        TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                Assert.Fail($"Condition not met in time: {because}");
            }

            time.Advance(step);
            await Task.Delay(10, cancellationToken);
        }
    }
}
