using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Connection;

public sealed class EndlessBackOffDelayPolicyTests
{
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
}
