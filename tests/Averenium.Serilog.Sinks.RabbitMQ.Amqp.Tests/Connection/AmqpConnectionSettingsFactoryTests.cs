using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;
using RabbitMQ.AMQP.Client;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Connection;

public sealed class AmqpConnectionSettingsFactoryTests
{
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

    [Test]
    public async Task Settings_factory_applies_tls_settings_and_sasl_mechanism()
    {
        var tls = new TlsSettings();

        var settings = AmqpConnectionSettingsFactory.Create(
            [new Uri("amqps://rabbit:5671/")], "my-app", TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), tls, SaslMechanism.External);

        await Assert.That(settings.TlsSettings).IsSameReferenceAs(tls);
        await Assert.That(settings.SaslMechanism).IsEqualTo(SaslMechanism.External);
    }
}
