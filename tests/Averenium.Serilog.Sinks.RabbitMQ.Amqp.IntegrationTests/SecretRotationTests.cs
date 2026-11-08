using Microsoft.Extensions.Configuration;
using Serilog;
using Serilog.Core;
using Serilog.Events;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.IntegrationTests;

/// <summary>
/// Rotated secrets against a real broker, without restarting the application. In each test the old secret is revoked
/// on the broker <em>first</em> and connections are dropped, so events logged in between can only be delivered if the
/// sink really reconnects with the new secret — and nothing may be lost meanwhile.
/// </summary>
[ParallelLimiter<DisruptiveBrokerTests>]
public sealed class SecretRotationTests
{
    private const string User = "rotating";

    [Test]
    public async Task Password_changed_in_the_configuration_is_used_after_a_reload(CancellationToken ct)
    {
        await using var node = new RabbitMqNode();
        await node.StartAsync(ct);
        await node.AddUserAsync(User, ct, password: "first-password");
        var topology = TestTopology.Unique();
        await node.DeclareTopologyAsync(topology, ct);
        var fallback = new CountingSink();
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Serilog:Using:0"] = "Averenium.Serilog.Sinks.RabbitMQ.Amqp",
                ["Serilog:WriteTo:0:Name"] = "RabbitMQAmqp",
                ["Serilog:WriteTo:0:Args:hostnames:0"] = node.HostAndPort,
                ["Serilog:WriteTo:0:Args:username"] = User,
                ["Serilog:WriteTo:0:Args:password"] = "first-password",
                ["Serilog:WriteTo:0:Args:exchange"] = topology.Exchange,
                ["Serilog:WriteTo:0:Args:routingKey"] = topology.RoutingKey,
                ["Serilog:WriteTo:0:Args:minReconnectDelay"] = "00:00:00.200",
                ["Serilog:WriteTo:0:Args:maxReconnectDelay"] = "00:00:01",
                ["Serilog:WriteTo:0:Args:shutdownTimeout"] = "00:00:30",
            })
            .Build();
        var logger = new LoggerConfiguration()
            .ReadFrom.Configuration(configuration)
            .WriteTo.Sink(fallback, LogEventLevel.Fatal) // not a fallback: only to show nothing else breaks
            .CreateLogger();

        Log(logger, "before", 20);
        await WaitForMessagesAsync(node, topology, 20, ct);

        await node.ChangePasswordAsync(User, "second-password", ct);
        await node.CloseAllConnectionsAsync(ct);
        Log(logger, "while the old password is refused", 20);
        await WaitUntilDisconnectedAsync(node, ct); // from now on the client can only retry the old password

        configuration["Serilog:WriteTo:0:Args:password"] = "second-password";
        configuration.Reload();
        Log(logger, "after", 20);

        await WaitForMessagesAsync(node, topology, 60, ct);
        await logger.DisposeAsync();
        await Assert.That(await node.MessageCountAsync(topology.Queue, ct)).IsGreaterThanOrEqualTo(60UL);
    }

    [Test]
    public async Task Password_updated_through_a_credentials_source_is_used(CancellationToken ct)
    {
        await using var node = new RabbitMqNode();
        await node.StartAsync(ct);
        await node.AddUserAsync(User, ct, password: "first-password");
        var topology = TestTopology.Unique();
        await node.DeclareTopologyAsync(topology, ct);
        var credentials = new RabbitMqAmqpCredentialsSource(User, "first-password");
        var fallback = new CountingSink();
        var logger = new LoggerConfiguration()
            .WriteTo.FallbackChain(
                wt => wt.RabbitMQAmqp(o =>
                {
                    o.Hostnames.Add(node.HostAndPort);
                    o.Credentials = credentials;
                    o.Exchange = topology.Exchange;
                    o.RoutingKey = topology.RoutingKey;
                    o.MinReconnectDelay = TimeSpan.FromMilliseconds(200);
                    o.MaxReconnectDelay = TimeSpan.FromSeconds(1);
                    o.ShutdownTimeout = TimeSpan.FromSeconds(30);
                }),
                wt => wt.Sink(fallback))
            .CreateLogger();

        Log(logger, "before", 20);
        await WaitForMessagesAsync(node, topology, 20, ct);

        await node.ChangePasswordAsync(User, "second-password", ct);
        await node.CloseAllConnectionsAsync(ct);
        Log(logger, "while the old password is refused", 20);
        await WaitUntilDisconnectedAsync(node, ct);

        credentials.Update(User, "second-password");
        Log(logger, "after", 20);

        await WaitForMessagesAsync(node, topology, 60, ct);
        await logger.DisposeAsync();
        await Assert.That(fallback.Count).IsEqualTo(0).Because("nothing was lost during the rotation");
    }

    [Test]
    public async Task Renewed_client_certificate_file_is_used_on_reconnect(CancellationToken ct)
    {
        using var pki = new TestPki();
        await using var node = new RabbitMqNode(pki);
        await node.StartAsync(ct);
        await node.AddUserAsync(TestPki.ClientUser, ct);
        await node.AddUserAsync("app-client-renewed", ct);
        var topology = TestTopology.Unique();
        await node.DeclareTopologyAsync(topology, ct);
        var fallback = new CountingSink();
        var logger = new LoggerConfiguration()
            .WriteTo.FallbackChain(
                wt => wt.RabbitMQAmqp(o =>
                {
                    o.Hostnames.Add($"127.0.0.1:{node.TlsPort}");
                    o.UseTls = true;
                    o.Tls.CaCertificatePath = pki.CaPath;
                    o.Tls.ClientCertificatePath = pki.ClientPfxPath;
                    o.Tls.ClientCertificatePassword = pki.ClientPfxPassword;
                    o.Tls.UseExternalAuthentication = true;
                    o.Exchange = topology.Exchange;
                    o.RoutingKey = topology.RoutingKey;
                    o.MinReconnectDelay = TimeSpan.FromMilliseconds(200);
                    o.MaxReconnectDelay = TimeSpan.FromSeconds(1);
                    o.ShutdownTimeout = TimeSpan.FromSeconds(30);
                }),
                wt => wt.Sink(fallback))
            .CreateLogger();

        Log(logger, "before", 10);
        await WaitForMessagesAsync(node, topology, 10, ct);

        // The certificate is renewed (new key, new identity), the old identity is revoked on the broker and all
        // connections are dropped: only the renewed certificate can get the sink connected again.
        pki.RenewClientCertificate("app-client-renewed");
        await node.DeleteUserAsync(TestPki.ClientUser, ct);
        await node.CloseAllConnectionsAsync(ct);
        Log(logger, "after", 10);

        await WaitForMessagesAsync(node, topology, 20, ct);
        // The broker's connection list lags a little behind closes; wait until it shows only the new identity.
        string[] users;
        var deadline = DateTime.UtcNow.AddSeconds(20);
        while ((users = await node.ConnectionUsersAsync(ct)).Contains(TestPki.ClientUser) && DateTime.UtcNow < deadline)
        {
            await Task.Delay(500, ct);
        }

        await logger.DisposeAsync();
        await Assert.That(users).Contains("app-client-renewed").Because("the sink reconnected with the renewed certificate");
        await Assert.That(users).DoesNotContain(TestPki.ClientUser);
        await Assert.That(fallback.Count).IsEqualTo(0);
    }

    private static async Task WaitUntilDisconnectedAsync(RabbitMqNode node, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow.AddSeconds(20);
        while ((await node.ConnectionUsersAsync(ct)).Contains(User) && DateTime.UtcNow < deadline)
        {
            await Task.Delay(200, ct);
        }
    }

    private static void Log(Logger logger, string phase, int count)
    {
        for (var i = 0; i < count; i++)
        {
            logger.Information("Event {N} {Phase}", i, phase);
        }
    }

    private static async Task WaitForMessagesAsync(RabbitMqNode node, TestTopology topology, ulong expected, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow.AddSeconds(45);
        ulong count;
        while ((count = await node.MessageCountAsync(topology.Queue, ct)) < expected)
        {
            if (DateTime.UtcNow > deadline)
            {
                Assert.Fail($"Only {count} of {expected} messages arrived.");
            }

            await Task.Delay(200, ct);
        }
    }

    private sealed class CountingSink : ILogEventSink
    {
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public void Emit(LogEvent logEvent) => Interlocked.Increment(ref _count);
    }
}
