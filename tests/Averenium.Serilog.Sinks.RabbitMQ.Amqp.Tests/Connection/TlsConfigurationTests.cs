using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;
using RabbitMQ.AMQP.Client;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Connection;

public sealed class TlsConfigurationTests : IDisposable
{
    private readonly string _directory = Directory.CreateTempSubdirectory("amqp-sink-tls-").FullName;

    public void Dispose() => Directory.Delete(_directory, recursive: true);

    [Test]
    public async Task Without_tls_options_the_client_defaults_are_kept()
    {
        var (tls, sasl) = TlsConfiguration.Create(Options());

        await Assert.That(tls).IsNull();
        await Assert.That(sasl).IsNull();
    }

    [Test]
    public async Task Protocols_revocation_and_a_validation_callback_are_applied()
    {
        var (tls, _) = TlsConfiguration.Create(Options(o =>
        {
            o.Protocols = SslProtocols.Tls13;
            o.CheckCertificateRevocation = true;
        }));

        await Assert.That(tls).IsNotNull();
        await Assert.That(tls!.Protocols).IsEqualTo(SslProtocols.Tls13);
        await Assert.That(tls.CheckCertificateRevocation).IsTrue();
        await Assert.That(tls.RemoteCertificateValidationCallback).IsNotNull();
    }

    [Test]
    public async Task A_custom_validation_callback_is_used_as_is()
    {
        RemoteCertificateValidationCallback callback = (_, _, _, errors) => errors == SslPolicyErrors.None;

        var (tls, _) = TlsConfiguration.Create(Options(o => o.CertificateValidation = callback));

        await Assert.That(tls!.RemoteCertificateValidationCallback).IsSameReferenceAs(callback);
    }

    [Test]
    public async Task A_server_certificate_issued_by_the_configured_ca_is_trusted()
    {
        using var ca = TestCertificates.CreateCa();
        using var server = TestCertificates.Issue(ca, "rabbit");
        var validate = TlsConfiguration.CreateValidation([ca], SslPolicyErrors.None, checkRevocation: false);

        await Assert.That(validate(this, server, null, SslPolicyErrors.RemoteCertificateChainErrors)).IsTrue();
    }

    [Test]
    public async Task A_server_certificate_from_another_ca_is_refused()
    {
        using var ca = TestCertificates.CreateCa();
        using var other = TestCertificates.CreateCa("Other CA");
        using var server = TestCertificates.Issue(other, "rabbit");
        var validate = TlsConfiguration.CreateValidation([ca], SslPolicyErrors.None, checkRevocation: false);

        await Assert.That(validate(this, server, null, SslPolicyErrors.RemoteCertificateChainErrors)).IsFalse();
    }

    [Test]
    public async Task A_name_mismatch_fails_even_with_a_trusted_ca_unless_it_is_acceptable()
    {
        using var ca = TestCertificates.CreateCa();
        using var server = TestCertificates.Issue(ca, "rabbit");
        const SslPolicyErrors errors = SslPolicyErrors.RemoteCertificateChainErrors | SslPolicyErrors.RemoteCertificateNameMismatch;

        var strict = TlsConfiguration.CreateValidation([ca], SslPolicyErrors.None, checkRevocation: false);
        var lenient = TlsConfiguration.CreateValidation([ca], SslPolicyErrors.RemoteCertificateNameMismatch, checkRevocation: false);

        await Assert.That(strict(this, server, null, errors)).IsFalse();
        await Assert.That(lenient(this, server, null, errors)).IsTrue();
    }

    [Test]
    public async Task Acceptable_policy_errors_are_honoured_without_a_ca_file()
    {
        // The client ignores TlsSettings.AcceptablePolicyErrors, so our callback must apply them.
        var validate = TlsConfiguration.CreateValidation((X509Certificate2Collection?)null, SslPolicyErrors.RemoteCertificateChainErrors, checkRevocation: false);

        await Assert.That(validate(this, null, null, SslPolicyErrors.RemoteCertificateChainErrors)).IsTrue();
        await Assert.That(validate(this, null, null, SslPolicyErrors.RemoteCertificateNameMismatch)).IsFalse();
        await Assert.That(validate(this, null, null, SslPolicyErrors.None)).IsTrue();
    }

    [Test]
    public async Task Ca_certificates_are_loaded_from_a_pem_bundle()
    {
        using var first = TestCertificates.CreateCa("First");
        using var second = TestCertificates.CreateCa("Second");
        var path = Path.Combine(_directory, "ca.pem");
        await File.WriteAllTextAsync(path, first.ExportCertificatePem() + "\n" + second.ExportCertificatePem());

        var loaded = TlsConfiguration.LoadCertificates(path);

        await Assert.That(loaded.Count).IsEqualTo(2);
    }

    [Test]
    public async Task Ca_certificate_is_loaded_from_a_der_file()
    {
        using var ca = TestCertificates.CreateCa();
        var path = Path.Combine(_directory, "ca.cer");
        await File.WriteAllBytesAsync(path, ca.Export(X509ContentType.Cert));

        var loaded = TlsConfiguration.LoadCertificates(path);

        await Assert.That(loaded.Count).IsEqualTo(1);
        await Assert.That(loaded[0].Thumbprint).IsEqualTo(ca.Thumbprint);
    }

    [Test]
    public async Task Client_certificate_is_loaded_from_a_password_protected_pfx()
    {
        using var ca = TestCertificates.CreateCa();
        using var client = TestCertificates.Issue(ca, "app", client: true);
        var path = Path.Combine(_directory, "client.pfx");
        await File.WriteAllBytesAsync(path, client.Export(X509ContentType.Pkcs12, "pfx-pass"));

        using var loaded = TlsConfiguration.LoadClientCertificate(Options(o =>
        {
            o.ClientCertificatePath = path;
            o.ClientCertificatePassword = "pfx-pass";
        }));

        await Assert.That(loaded!.Thumbprint).IsEqualTo(client.Thumbprint);
        await Assert.That(loaded.HasPrivateKey).IsTrue();
    }

    [Test]
    public async Task Client_certificate_is_loaded_from_pem_with_a_separate_key_file()
    {
        using var ca = TestCertificates.CreateCa();
        using var client = TestCertificates.Issue(ca, "app", client: true);
        var certPath = Path.Combine(_directory, "client.crt");
        var keyPath = Path.Combine(_directory, "client.key");
        await File.WriteAllTextAsync(certPath, client.ExportCertificatePem());
        await File.WriteAllTextAsync(keyPath, client.GetRSAPrivateKey()!.ExportPkcs8PrivateKeyPem());

        using var loaded = TlsConfiguration.LoadClientCertificate(Options(o =>
        {
            o.ClientCertificatePath = certPath;
            o.ClientCertificateKeyPath = keyPath;
        }));

        await Assert.That(loaded!.Thumbprint).IsEqualTo(client.Thumbprint);
        await Assert.That(loaded.HasPrivateKey).IsTrue();
    }

    [Test]
    public async Task Client_certificate_ends_up_in_the_tls_settings_and_external_auth_selects_sasl_external()
    {
        using var ca = TestCertificates.CreateCa();
        using var client = TestCertificates.Issue(ca, "app", client: true);

        var (tls, sasl) = TlsConfiguration.Create(Options(o =>
        {
            o.ClientCertificate = client;
            o.UseExternalAuthentication = true;
        }));

        await Assert.That(tls!.ClientCertificates.Count).IsEqualTo(1);
        await Assert.That(sasl).IsEqualTo(SaslMechanism.External);
    }

    [Test]
    public async Task A_renewed_client_certificate_file_is_used_at_the_next_handshake()
    {
        using var ca = TestCertificates.CreateCa();
        using var first = TestCertificates.Issue(ca, "app", client: true);
        using var renewed = TestCertificates.Issue(ca, "app", client: true);
        var path = Path.Combine(_directory, "client.pfx");
        await File.WriteAllBytesAsync(path, first.Export(X509ContentType.Pkcs12, "pw"));
        var (tls, _) = TlsConfiguration.Create(Options(o =>
        {
            o.ClientCertificatePath = path;
            o.ClientCertificatePassword = "pw";
        }));
        var select = tls!.LocalCertificateSelectionCallback!;

        var before = (X509Certificate2)select(this, "rabbit", [], null, [])!;
        await File.WriteAllBytesAsync(path, renewed.Export(X509ContentType.Pkcs12, "pw"));
        var after = (X509Certificate2)select(this, "rabbit", [], null, [])!;

        await Assert.That(before.Thumbprint).IsEqualTo(first.Thumbprint);
        await Assert.That(after.Thumbprint).IsEqualTo(renewed.Thumbprint);
        await Assert.That(after.HasPrivateKey).IsTrue();
    }

    [Test]
    public async Task A_rotated_ca_file_is_trusted_at_the_next_handshake()
    {
        using var oldCa = TestCertificates.CreateCa("Old CA");
        using var newCa = TestCertificates.CreateCa("New CA");
        using var server = TestCertificates.Issue(newCa, "rabbit");
        var path = Path.Combine(_directory, "ca.pem");
        await File.WriteAllTextAsync(path, oldCa.ExportCertificatePem());
        var (tls, _) = TlsConfiguration.Create(Options(o => o.CaCertificatePath = path));
        var validate = tls!.RemoteCertificateValidationCallback!;

        var before = validate(this, server, null, SslPolicyErrors.RemoteCertificateChainErrors);
        await File.WriteAllTextAsync(path, oldCa.ExportCertificatePem() + "\n" + newCa.ExportCertificatePem());
        var after = validate(this, server, null, SslPolicyErrors.RemoteCertificateChainErrors);

        await Assert.That(before).IsFalse();
        await Assert.That(after).IsTrue();
    }

    [Test]
    public async Task A_file_caught_in_the_middle_of_an_update_keeps_the_previous_certificate()
    {
        using var ca = TestCertificates.CreateCa();
        using var server = TestCertificates.Issue(ca, "rabbit");
        var path = Path.Combine(_directory, "ca.pem");
        await File.WriteAllTextAsync(path, ca.ExportCertificatePem());
        var (tls, _) = TlsConfiguration.Create(Options(o => o.CaCertificatePath = path));
        var validate = tls!.RemoteCertificateValidationCallback!;

        await File.WriteAllTextAsync(path, "-----BEGIN CERTIFICATE-----\ntruncated");

        await Assert.That(validate(this, server, null, SslPolicyErrors.RemoteCertificateChainErrors)).IsTrue();
    }

    [Test]
    public async Task A_replaced_client_certificate_is_released_one_renewal_later()
    {
        using var ca = TestCertificates.CreateCa();
        X509Certificate2[] issued = [.. Enumerable.Range(0, 4).Select(_ => TestCertificates.Issue(ca, "app", client: true))];
        var path = Path.Combine(_directory, "client.pfx");
        await File.WriteAllBytesAsync(path, issued[0].Export(X509ContentType.Pkcs12, "pw"));
        var (tls, _) = TlsConfiguration.Create(Options(o =>
        {
            o.ClientCertificatePath = path;
            o.ClientCertificatePassword = "pw";
        }));
        var select = tls!.LocalCertificateSelectionCallback!;
        var initial = (X509Certificate2)tls.ClientCertificates[0];

        var loaded = new List<X509Certificate2>();
        for (var renewal = 1; renewal <= 3; renewal++)
        {
            await File.WriteAllBytesAsync(path, issued[renewal].Export(X509ContentType.Pkcs12, "pw"));
            loaded.Add((X509Certificate2)select(this, "rabbit", [], null, [])!);
        }

        await Assert.That(loaded[0].Handle).IsEqualTo(IntPtr.Zero).Because("released (key container removed) two renewals later");
        await Assert.That(loaded[1].Handle).IsNotEqualTo(IntPtr.Zero).Because("a handshake may still be using the previous one");
        await Assert.That(loaded[2].Handle).IsNotEqualTo(IntPtr.Zero);
        await Assert.That(initial.Handle).IsNotEqualTo(IntPtr.Zero).Because("the client keeps the first one in ClientCertificates");
        foreach (var certificate in issued)
        {
            certificate.Dispose();
        }
    }

    private static RabbitMqAmqpTlsOptions Options(Action<RabbitMqAmqpTlsOptions>? configure = null)
    {
        var options = new RabbitMqAmqpTlsOptions();
        configure?.Invoke(options);
        return options;
    }
}
