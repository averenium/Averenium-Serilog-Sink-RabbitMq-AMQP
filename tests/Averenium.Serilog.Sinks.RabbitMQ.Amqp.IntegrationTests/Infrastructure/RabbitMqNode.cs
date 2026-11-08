using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Testcontainers.RabbitMq;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.IntegrationTests.Infrastructure;

/// <summary>
/// A disposable single RabbitMQ 4.x node (AMQP 1.0 is native since 4.0) that tests can stop and start.
/// The AMQP port is bound to a fixed host port so the address survives restarts and is known before
/// the broker is started (to test "broker down at start-up").
/// </summary>
internal sealed class RabbitMqNode : IAsyncDisposable
{
    public const string Image = "rabbitmq:4-management";
    public const string User = "test";
    public const string Password = "test";

    private const int AmqpsPort = 5671;
    private const string TlsDirectory = "/etc/rabbitmq/tls/";

    private readonly RabbitMqContainer _container;

    /// <param name="pki">
    /// When given, the node also listens for AMQPS (server certificate from the PKI, client certificates verified
    /// against its CA but optional) and accepts SASL EXTERNAL with the certificate's common name as user name.
    /// </param>
    public RabbitMqNode(TestPki? pki = null)
    {
        Port = GetFreeTcpPort();
        var builder = new RabbitMqBuilder(Image)
            .WithUsername(User)
            .WithPassword(Password)
            .WithPortBinding(Port, RabbitMqBuilder.RabbitMqPort);

        if (pki is not null)
        {
            TlsPort = GetFreeTcpPort();
            builder = builder
                .WithPortBinding(TlsPort, AmqpsPort)
                .WithResourceMapping(Encoding.ASCII.GetBytes(pki.Ca.ExportCertificatePem()), TlsDirectory + "ca.pem")
                .WithResourceMapping(Encoding.ASCII.GetBytes(pki.Server.ExportCertificatePem()), TlsDirectory + "server.pem")
                .WithResourceMapping(Encoding.ASCII.GetBytes(pki.Server.GetRSAPrivateKey()!.ExportPkcs8PrivateKeyPem()), TlsDirectory + "server.key")
                .WithResourceMapping(Encoding.ASCII.GetBytes(
                    "[rabbitmq_management,rabbitmq_prometheus,rabbitmq_auth_mechanism_ssl].\n"), "/etc/rabbitmq/enabled_plugins")
                .WithResourceMapping(Encoding.ASCII.GetBytes($"""
                    listeners.ssl.default = {AmqpsPort}
                    ssl_options.cacertfile = {TlsDirectory}ca.pem
                    ssl_options.certfile = {TlsDirectory}server.pem
                    ssl_options.keyfile = {TlsDirectory}server.key
                    ssl_options.verify = verify_peer
                    ssl_options.fail_if_no_peer_cert = false
                    auth_mechanisms.1 = PLAIN
                    auth_mechanisms.2 = AMQPLAIN
                    auth_mechanisms.3 = EXTERNAL
                    ssl_cert_login_from = common_name

                    """), "/etc/rabbitmq/conf.d/90-tls.conf");
        }

        _container = builder.Build();
    }

    public int Port { get; }

    /// <summary>Host port of the AMQPS listener (only with a <see cref="TestPki"/>).</summary>
    public int TlsPort { get; }

    public Uri AmqpUri => new($"amqp://{User}:{Password}@127.0.0.1:{Port}/");

    /// <summary>The node as a plain <c>host:port</c> (credentials given separately).</summary>
    public string HostAndPort => $"127.0.0.1:{Port}";

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

    /// <summary>Creates a virtual host (any name, e.g. <c>/data</c>) and gives the test user full permissions on it.</summary>
    public async Task AddVirtualHostAsync(string virtualHost, CancellationToken ct)
    {
        await RabbitmqctlAsync(["add_vhost", virtualHost], ct);
        await RabbitmqctlAsync(["set_permissions", "-p", virtualHost, User, ".*", ".*", ".*"], ct);
    }

    /// <summary>Creates a user (e.g. the common name of a client certificate) with full permissions on <c>/</c>.</summary>
    public async Task AddUserAsync(string user, CancellationToken ct, string? password = null)
    {
        await RabbitmqctlAsync(["add_user", user, password ?? Guid.NewGuid().ToString("N")], ct);
        await RabbitmqctlAsync(["set_permissions", "-p", "/", user, ".*", ".*", ".*"], ct);
    }

    /// <summary>Rotates a user's password on the broker (existing connections stay open, like in production).</summary>
    public Task ChangePasswordAsync(string user, string password, CancellationToken ct) =>
        RabbitmqctlAsync(["change_password", user, password], ct);

    /// <summary>User names of the open client connections, as the broker sees them.</summary>
    public async Task<string[]> ConnectionUsersAsync(CancellationToken ct) =>
        (await RabbitmqctlAsync(["list_connections", "user", "--no-table-headers", "--quiet"], ct))
        .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    /// <summary>Deletes a user; RabbitMQ closes that user's connections.</summary>
    public Task DeleteUserAsync(string user, CancellationToken ct) => RabbitmqctlAsync(["delete_user", user], ct);

    /// <summary>Consumes (and accepts) up to <paramref name="count"/> messages from <paramref name="queue"/>.</summary>
    public Task<IReadOnlyList<IMessage>> ReceiveAsync(string queue, int count, TimeSpan timeout, CancellationToken ct) =>
        WithManagementAsync<IReadOnlyList<IMessage>>(async (connection, _) =>
        {
            var received = new List<IMessage>();
            var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var consumer = await connection.ConsumerBuilder()
                .Queue(queue)
                .MessageHandler((context, message) =>
                {
                    lock (received)
                    {
                        received.Add(message);
                        if (received.Count >= count)
                        {
                            done.TrySetResult();
                        }
                    }

                    context.Accept();
                    return Task.CompletedTask;
                })
                .BuildAndStartAsync(ct);
            try
            {
                await done.Task.WaitAsync(timeout, ct);
            }
            finally
            {
                await consumer.CloseAsync();
            }

            lock (received)
            {
                return [.. received];
            }
        }, ct);

    /// <summary>The broker's own list of queues in <paramref name="virtualHost"/> — independent of our URI handling.</summary>
    public async Task<string> ListQueuesAsync(string virtualHost, CancellationToken ct) =>
        await RabbitmqctlAsync(["list_queues", "-p", virtualHost, "name", "messages", "--no-table-headers", "--quiet"], ct);

    /// <summary>Declares exchange → quorum queue → binding through a separate AMQP connection.</summary>
    public Task DeclareTopologyAsync(TestTopology topology, CancellationToken ct) =>
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

    public Task DeclareClassicQueueAsync(string queue, CancellationToken ct) =>
        WithManagementAsync(async management =>
        {
            await management.Queue(queue).Type(QueueType.CLASSIC).DeclareAsync();
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

    private async Task<string> RabbitmqctlAsync(IList<string> arguments, CancellationToken ct)
    {
        var result = await _container.ExecAsync(["rabbitmqctl", .. arguments], ct);
        if (result.ExitCode != 0)
        {
            throw new InvalidOperationException($"rabbitmqctl {string.Join(' ', arguments)} failed: {result.Stderr}");
        }

        return result.Stdout;
    }

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();

    private Task<T> WithManagementAsync<T>(Func<IManagement, Task<T>> action, CancellationToken ct) =>
        WithManagementAsync((connection, _) => action(connection.Management()), ct);

    private async Task<T> WithManagementAsync<T>(Func<IConnection, CancellationToken, Task<T>> action, CancellationToken ct)
    {
        var settings = ConnectionSettingsBuilder.Create().Uri(AmqpUri).ContainerId("it-admin").Build();
        var environment = AmqpEnvironment.Create(settings);
        try
        {
            var connection = await environment.CreateConnectionAsync(ct);
            return await action(connection, ct);
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
