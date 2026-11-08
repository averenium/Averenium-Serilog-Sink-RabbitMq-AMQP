using TUnit.Core.Interfaces;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.IntegrationTests.Infrastructure;

/// <summary>
/// Parallel limit for tests that stop, restart or otherwise disrupt <em>their own</em> broker. Their brokers are
/// independent (random host ports), so a few can run at once; more would make timing-sensitive recovery tests flaky on
/// small CI runners (4 vCPU) and exhaust the memory of a local Podman machine.
/// <para>Use with <c>[ParallelLimiter&lt;DisruptiveBrokerTests&gt;]</c>.</para>
/// </summary>
public sealed class DisruptiveBrokerTests : IParallelLimit
{
    public int Limit => 3;
}
