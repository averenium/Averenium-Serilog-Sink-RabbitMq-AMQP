using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Resilience;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Resilience;

public sealed class ExponentialBackoffTests
{
    [Test]
    public async Task Backoff_doubles_from_min_and_is_capped_at_max_without_jitter()
    {
        var backoff = new ExponentialBackoff(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10), jitter: 0);

        var delays = Enumerable.Range(0, 6).Select(_ => backoff.Next().TotalSeconds);

        await Assert.That(string.Join(",", delays)).IsEqualTo("1,2,4,8,10,10");
        await Assert.That(backoff.Attempt).IsEqualTo(6);
    }

    [Test]
    public async Task Backoff_reset_starts_from_min_again()
    {
        var backoff = new ExponentialBackoff(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10), jitter: 0);
        backoff.Next();
        backoff.Next();

        backoff.Reset();

        await Assert.That(backoff.Next()).IsEqualTo(TimeSpan.FromSeconds(1));
    }

    [Test]
    [Arguments(0.0)]
    [Arguments(0.5)]
    [Arguments(0.999)]
    public async Task Backoff_jitter_stays_within_bounds(double random)
    {
        var backoff = new ExponentialBackoff(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30), jitter: 0.2, () => random);
        backoff.Next();
        backoff.Next();

        var delay = backoff.Next(); // base 4 s

        await Assert.That(delay)
            .IsGreaterThanOrEqualTo(TimeSpan.FromSeconds(3.2))
            .And.IsLessThanOrEqualTo(TimeSpan.FromSeconds(4.8));
    }

    [Test]
    public async Task Backoff_never_overflows_after_many_attempts()
    {
        var backoff = new ExponentialBackoff(TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(30));

        var delays = Enumerable.Range(0, 10_000).Select(_ => backoff.Next()).ToArray();

        await Assert.That(delays.Min()).IsGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(500));
        await Assert.That(delays.Max()).IsLessThanOrEqualTo(TimeSpan.FromSeconds(30));
    }
}
