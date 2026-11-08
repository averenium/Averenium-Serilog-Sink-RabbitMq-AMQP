using Serilog;
using Serilog.Core;
using Serilog.Events;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.IntegrationTests;

/// <summary>
/// AMQPS against a broker with a certificate from a private CA — the usual set-up inside a cluster. The client trusts
/// that CA through <c>TlsCaCertificatePath</c> only (nothing is installed into the OS trust store).
/// </summary>
[ClassDataSource<SharedTlsRabbitMq>(Shared = SharedType.PerTestSession)]
public sealed class TlsTests(SharedTlsRabbitMq tls)
{
    [Test]
    public async Task Events_are_delivered_over_tls_with_a_private_ca(CancellationToken ct)
    {
        var pki = tls.Pki;
        var node = tls.Node;
        var topology = TestTopology.Unique();
        await node.DeclareTopologyAsync(topology, ct);
        var fallback = new CountingSink();

        var logger = CreateLogger(node, topology, fallback, o =>
        {
            o.Username = RabbitMqNode.User;
            o.Password = RabbitMqNode.Password;
            o.Tls.CaCertificatePath = pki.CaPath;
        });
        for (var i = 0; i < 20; i++)
        {
            logger.Information("Secure {N}", i);
        }

        await logger.DisposeAsync();

        await Assert.That(await node.MessageCountAsync(topology.Queue, ct)).IsEqualTo(20UL);
        await Assert.That(fallback.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Mutual_tls_with_sasl_external_authenticates_by_client_certificate(CancellationToken ct)
    {
        var pki = tls.Pki;
        var node = tls.Node;
        var topology = TestTopology.Unique();
        await node.DeclareTopologyAsync(topology, ct);
        var fallback = new CountingSink();

        var logger = CreateLogger(node, topology, fallback, o =>
        {
            o.Tls.CaCertificatePath = pki.CaPath;
            o.Tls.ClientCertificatePath = pki.ClientPfxPath;
            o.Tls.ClientCertificatePassword = pki.ClientPfxPassword;
            o.Tls.UseExternalAuthentication = true;
        });
        for (var i = 0; i < 5; i++)
        {
            logger.Information("Mutual {N}", i);
        }

        await logger.DisposeAsync();

        await Assert.That(await node.MessageCountAsync(topology.Queue, ct)).IsEqualTo(5UL);
        await Assert.That(fallback.Count).IsEqualTo(0);
    }

    [Test]
    public async Task A_broker_certificate_from_an_untrusted_ca_is_refused_and_events_reach_the_fallback(CancellationToken ct)
    {
        var pki = tls.Pki;
        var node = tls.Node;
        var topology = TestTopology.Unique();
        await node.DeclareTopologyAsync(topology, ct);
        var fallback = new CountingSink();

        var logger = CreateLogger(node, topology, fallback, o =>
        {
            o.Username = RabbitMqNode.User;
            o.Password = RabbitMqNode.Password;
            o.Tls.CaCertificatePath = pki.CreateUnrelatedCaFile();
            o.ShutdownTimeout = TimeSpan.FromSeconds(3);
        });
        for (var i = 0; i < 3; i++)
        {
            logger.Information("Refused {N}", i);
        }

        await logger.DisposeAsync(); // the shutdown budget decides: nothing can be delivered, all goes to the fallback

        await Assert.That(await node.MessageCountAsync(topology.Queue, ct)).IsEqualTo(0UL);
        await Assert.That(fallback.Count).IsEqualTo(3).Because("no silent loss even when TLS cannot be established");
    }

    private static Logger CreateLogger(
        RabbitMqNode node, TestTopology topology, CountingSink fallback, Action<RabbitMqAmqpSinkOptions> configure) =>
        new LoggerConfiguration()
            .WriteTo.FallbackChain(
                wt => wt.RabbitMQAmqp(o =>
                {
                    o.Hostnames.Add($"127.0.0.1:{node.TlsPort}");
                    o.UseTls = true;
                    o.Exchange = topology.Exchange;
                    o.RoutingKey = topology.RoutingKey;
                    o.MinReconnectDelay = TimeSpan.FromMilliseconds(200);
                    o.MaxReconnectDelay = TimeSpan.FromSeconds(1);
                    o.ShutdownTimeout = TimeSpan.FromSeconds(30);
                    configure(o);
                }),
                wt => wt.Sink(fallback))
            .CreateLogger();

    private sealed class CountingSink : ILogEventSink
    {
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public void Emit(LogEvent logEvent) => Interlocked.Increment(ref _count);
    }
}
