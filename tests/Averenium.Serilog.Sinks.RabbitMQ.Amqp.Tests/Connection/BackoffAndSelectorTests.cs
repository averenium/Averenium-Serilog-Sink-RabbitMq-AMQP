using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;
using RabbitMQ.AMQP.Client;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Connection;

public sealed class BackoffAndSelectorTests
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

    [Test]
    public async Task Endless_policy_is_always_active()
    {
        var policy = new EndlessBackOffDelayPolicy(TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1));

        var delays = Enumerable.Range(0, 1_000).Select(_ => policy.Delay()).ToArray();

        await Assert.That(delays.Min()).IsGreaterThanOrEqualTo(100);
        await Assert.That(delays.Max()).IsLessThanOrEqualTo(1000);
        await Assert.That(policy.IsActive()).IsTrue();
        await Assert.That(policy.CurrentAttempt).IsEqualTo(1_000);

        policy.Reset();
        await Assert.That(policy.CurrentAttempt).IsEqualTo(0);
    }

    [Test]
    public async Task Round_robin_selector_cycles_through_all_nodes()
    {
        var uris = new List<Uri> { new("amqp://a:5672"), new("amqp://b:5672"), new("amqp://c:5672") };
        var selector = new RoundRobinUriSelector();

        var picked = Enumerable.Range(0, 7).Select(_ => selector.Select(uris).Host);

        await Assert.That(string.Join(",", picked)).IsEqualTo("a,b,c,a,b,c,a");
    }

    [Test]
    public async Task Settings_factory_uses_cluster_settings_for_multiple_uris_and_endless_recovery()
    {
        var settings = AmqpConnectionSettingsFactory.Create(
            [new Uri("amqp://u:p@a:5672/"), new Uri("amqp://u:p@b:5672/")],
            "my-app",
            TimeSpan.FromMilliseconds(500),
            TimeSpan.FromSeconds(30));

        await Assert.That(settings).IsTypeOf<ClusterConnectionSettings>();
        await Assert.That(settings.ContainerId).IsEqualTo("my-app");
        await Assert.That(settings.Recovery.IsActivated()).IsTrue();
        await Assert.That(settings.Recovery.IsTopologyActive()).IsFalse();
        await Assert.That(settings.Recovery.GetBackOffDelayPolicy()).IsTypeOf<EndlessBackOffDelayPolicy>();
    }

    [Test]
    public async Task Settings_factory_rejects_empty_uri_list() =>
        await Assert.That(() => AmqpConnectionSettingsFactory.Create(
            [], "x", TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2))).Throws<ArgumentException>();
}
