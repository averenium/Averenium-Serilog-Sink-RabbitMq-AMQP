using System.Net;
using System.Net.Sockets;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Testcontainers.RabbitMq;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.IntegrationTests;

/// <summary>
/// A disposable single RabbitMQ 4.x node (AMQP 1.0 is native since 4.0) that tests can stop and start.
/// The AMQP port is bound to a fixed host port so the address survives restarts and is known before
/// the broker is started (to test "broker down at start-up").
/// </summary>
internal sealed class RabbitMqNode : IAsyncDisposable
{
    public const string Image = "rabbitmq:4-management";
    private const string User = "test";
    private const string Password = "test";

    private readonly RabbitMqContainer _container;

    public RabbitMqNode()
    {
        Port = GetFreeTcpPort();
        _container = new RabbitMqBuilder(Image)
            .WithUsername(User)
            .WithPassword(Password)
            .WithPortBinding(Port, RabbitMqBuilder.RabbitMqPort)
            .Build();
    }

    public int Port { get; }

    public Uri AmqpUri => new($"amqp://{User}:{Password}@127.0.0.1:{Port}/");

    public Task StartAsync(CancellationToken ct) => _container.StartAsync(ct);

    public Task StopAsync(CancellationToken ct) => _container.StopAsync(ct);

    /// <summary>Forcefully closes every client connection, like a network blip or an operator action.</summary>
    public async Task CloseAllConnectionsAsync(CancellationToken ct)
    {
        var result = await _container.ExecAsync(["rabbitmqctl", "close_all_connections", "integration test"], ct);
        if (result.ExitCode != 0)
        {
            throw new InvalidOperationException($"rabbitmqctl failed: {result.Stderr}");
        }
    }

    /// <summary>Declares exchange → quorum queue → binding through a separate AMQP connection.</summary>
    public Task DeclareTopologyAsync(Topology topology, CancellationToken ct) =>
        WithManagementAsync(async management =>
        {
            await management.Exchange(topology.Exchange).Type(ExchangeType.DIRECT).DeclareAsync();
            await management.Queue(topology.Queue).Type(QueueType.QUORUM).DeclareAsync();
            await management.Binding()
                .SourceExchange(topology.Exchange)
                .DestinationQueue(topology.Queue)
                .Key(topology.RoutingKey)
                .BindAsync();
            return 0;
        }, ct);

    public Task DeleteExchangeAsync(string exchange, CancellationToken ct) =>
        WithManagementAsync(async management =>
        {
            await management.Exchange(exchange).DeleteAsync();
            return 0;
        }, ct);

    /// <summary>
    /// Polls until the broker accepts AMQP connections and returns how long that took. Testcontainers' readiness
    /// check can pass early after a restart (the startup log line from the previous run is still there).
    /// </summary>
    public async Task<TimeSpan> WaitUntilAcceptingConnectionsAsync(TimeSpan timeout, CancellationToken ct)
    {
        var started = DateTime.UtcNow;
        while (true)
        {
            try
            {
                await WithManagementAsync(_ => Task.FromResult(0), ct);
                return DateTime.UtcNow - started;
            }
            catch (Exception) when (DateTime.UtcNow - started < timeout && !ct.IsCancellationRequested)
            {
                await Task.Delay(200, ct);
            }
        }
    }

    public Task<ulong> MessageCountAsync(string queue, CancellationToken ct) =>
        WithManagementAsync(async management => (await management.GetQueueInfoAsync(queue, ct)).MessageCount(), ct);

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();

    private async Task<T> WithManagementAsync<T>(Func<IManagement, Task<T>> action, CancellationToken ct)
    {
        var settings = ConnectionSettingsBuilder.Create().Uri(AmqpUri).ContainerId("it-admin").Build();
        var environment = AmqpEnvironment.Create(settings);
        try
        {
            var connection = await environment.CreateConnectionAsync(ct);
            return await action(connection.Management());
        }
        finally
        {
            await environment.CloseAsync();
        }
    }

    private static int GetFreeTcpPort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }
}

/// <summary>Unique names per test so tests never share broker state.</summary>
internal sealed record Topology(string Exchange, string Queue, string RoutingKey)
{
    public static Topology Unique()
    {
        var id = Guid.NewGuid().ToString("N")[..8];
        return new Topology($"logs-{id}", $"logs-q-{id}", "app");
    }
}
