namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Options;

public sealed class RabbitMqAmqpSinkOptionsTests
{
    [Test]
    [Arguments("no target")]
    [Arguments("both exchange and queue")]
    [Arguments("routing key with queue")]
    [Arguments("declare queue without declare topology")]
    [Arguments("binding key without declare queue")]
    [Arguments("unknown exchange type")]
    public async Task Ambiguous_or_unusable_configuration_is_rejected(string scenario)
    {
        var options = Valid();
        switch (scenario)
        {
            case "no target":
                options.Exchange = null;
                break;
            case "both exchange and queue":
                options.Queue = "logs";
                break;
            case "routing key with queue":
                options.Exchange = null;
                options.Queue = "logs";
                break;
            case "declare queue without declare topology":
                options.DeclareQueue = "logs.app";
                break;
            case "binding key without declare queue":
                options.DeclareTopology = true;
                options.BindingKey = "#";
                break;
            case "unknown exchange type":
                options.DeclareTopology = true;
                options.ExchangeType = "topics";
                break;
        }

        await Assert.That(options.Validate).Throws<ArgumentException>();
    }

    [Test]
    [Arguments("topic")]
    [Arguments("direct")]
    [Arguments("fanout")]
    [Arguments("headers")]
    [Arguments("x-delayed-message")]
    public async Task Standard_and_plugin_exchange_types_are_accepted(string exchangeType)
    {
        var options = Valid();
        options.DeclareTopology = true;
        options.DeclareQueue = "logs.app";
        options.ExchangeType = exchangeType;

        await Assert.That(options.Validate).ThrowsNothing();
    }

    [Test]
    public async Task Snapshot_is_independent_of_later_changes()
    {
        var options = Valid();

        var snapshot = options.Snapshot();
        options.Exchange = "changed";
        options.Hostnames.Add("other");

        await Assert.That(snapshot.Exchange).IsEqualTo("logs");
        await Assert.That(snapshot.Hostnames.Count).IsEqualTo(1);
    }

    [Test]
    public async Task Snapshot_copies_the_tls_options()
    {
        var options = Valid();
        options.Tls.CaCertificatePath = "a.pem";

        var snapshot = options.Snapshot();
        options.Tls.CaCertificatePath = "changed.pem";

        await Assert.That(snapshot.Tls.CaCertificatePath).IsEqualTo("a.pem");
    }

    [Test]
    [Arguments("tls option without tls")]
    [Arguments("validation callback and ca file")]
    [Arguments("certificate object and file")]
    [Arguments("key file without certificate file")]
    [Arguments("external auth without client certificate")]
    [Arguments("external auth with username")]
    [Arguments("external auth with credentials in a uri")]
    [Arguments("missing ca file")]
    [Arguments("fallback after zero")]
    public async Task Inconsistent_tls_and_fallback_settings_are_rejected(string scenario)
    {
        var options = Valid();
        options.UseTls = true;
        var existing = typeof(RabbitMqAmqpSinkOptionsTests).Assembly.Location;
        switch (scenario)
        {
            case "tls option without tls":
                options.UseTls = false;
                options.Tls.CheckCertificateRevocation = true;
                break;
            case "validation callback and ca file":
                options.Tls.CertificateValidation = (_, _, _, errors) => errors == System.Net.Security.SslPolicyErrors.None;
                options.Tls.CaCertificatePath = existing;
                break;
            case "certificate object and file":
                options.Tls.ClientCertificate = TestCertificates.CreateCa();
                options.Tls.ClientCertificatePath = existing;
                break;
            case "key file without certificate file":
                options.Tls.ClientCertificateKeyPath = existing;
                break;
            case "external auth without client certificate":
                options.Tls.UseExternalAuthentication = true;
                break;
            case "external auth with username":
                options.Tls.UseExternalAuthentication = true;
                options.Tls.ClientCertificatePath = existing;
                options.Username = "app";
                break;
            case "external auth with credentials in a uri":
                options.Tls.UseExternalAuthentication = true;
                options.Tls.ClientCertificatePath = existing;
                options.Hostnames.Clear();
                options.Hostnames.Add("amqps://app:secret@rabbit");
                options.UseTls = false;
                break;
            case "missing ca file":
                options.Tls.CaCertificatePath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".pem");
                break;
            case "fallback after zero":
                options.FallbackAfter = TimeSpan.Zero;
                break;
        }

        await Assert.That(options.Validate).Throws<ArgumentException>();
    }

    [Test]
    public async Task Tls_options_with_an_amqps_host_are_accepted()
    {
        var options = new RabbitMqAmqpSinkOptions { Exchange = "logs" };
        options.Tls.AcceptablePolicyErrors = System.Net.Security.SslPolicyErrors.RemoteCertificateNameMismatch;
        options.Hostnames.Add("amqps://rabbit");

        await Assert.That(options.Validate).ThrowsNothing();
    }

    private static RabbitMqAmqpSinkOptions Valid()
    {
        var options = new RabbitMqAmqpSinkOptions { Exchange = "logs", RoutingKey = "app" };
        options.Hostnames.Add("localhost");
        return options;
    }
}
