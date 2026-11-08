using TUnit.Core.Interfaces;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.IntegrationTests.Infrastructure;

/// <summary>
/// One TLS-enabled broker (with its <see cref="TestPki"/>) for the session — see <see cref="SharedRabbitMq"/>. The SASL
/// EXTERNAL user of the PKI's client certificate is created once here.
/// </summary>
public sealed class SharedTlsRabbitMq : IAsyncInitializer, IAsyncDisposable
{
    public SharedTlsRabbitMq() => Node = new RabbitMqNode(Pki);

    internal TestPki Pki { get; } = new();

    internal RabbitMqNode Node { get; }

    public async Task InitializeAsync()
    {
        await Node.StartAsync(CancellationToken.None);
        await Node.AddUserAsync(TestPki.ClientUser, CancellationToken.None);
    }

    public async ValueTask DisposeAsync()
    {
        await Node.DisposeAsync();
        Pki.Dispose();
    }
}
