using TUnit.Core.Interfaces;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.IntegrationTests.Infrastructure;

/// <summary>
/// One broker for the whole test session, for tests that only use their own unique topology and never disrupt the broker
/// (no stop, no forced closes, no user changes). Those tests run in parallel on it; a container start (~5–10 s) is paid
/// once instead of per test. Tests that disrupt a broker keep their own <see cref="RabbitMqNode"/>.
/// <para>Use with <c>[ClassDataSource&lt;SharedRabbitMq&gt;(Shared = SharedType.PerTestSession)]</c>.</para>
/// </summary>
public sealed class SharedRabbitMq : IAsyncInitializer, IAsyncDisposable
{
    internal RabbitMqNode Node { get; } = new();

    public Task InitializeAsync() => Node.StartAsync(CancellationToken.None);

    public ValueTask DisposeAsync() => Node.DisposeAsync();
}
