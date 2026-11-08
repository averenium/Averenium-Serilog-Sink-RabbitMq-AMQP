using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.IntegrationTests.Infrastructure;

/// <summary>
/// Throw-away PKI for the TLS tests: a CA, a broker certificate for <c>localhost</c>/<c>127.0.0.1</c> and a client
/// certificate whose common name is the RabbitMQ user for SASL EXTERNAL. Files go to a temp directory that the test
/// owns (the sink reads mounted certificate files at start-up — this mimics a Kubernetes secret).
/// </summary>
internal sealed class TestPki : IDisposable
{
    public const string ClientUser = "app-client";

    public TestPki()
    {
        Directory = System.IO.Directory.CreateTempSubdirectory("amqp-sink-pki-").FullName;
        Ca = CreateCa("Integration Test CA");
        Server = Issue(Ca, "localhost", serverAuth: true);
        Client = Issue(Ca, ClientUser, serverAuth: false);

        CaPath = Write("ca.pem", Ca.ExportCertificatePem());
        ClientPfxPath = Path.Combine(Directory, "client.pfx");
        File.WriteAllBytes(ClientPfxPath, Client.Export(X509ContentType.Pkcs12, ClientPfxPassword));
    }

    public string Directory { get; }

    public X509Certificate2 Ca { get; }

    public X509Certificate2 Server { get; }

    public X509Certificate2 Client { get; }

    public string CaPath { get; }

    public string ClientPfxPath { get; }

    public string ClientPfxPassword { get; } = Guid.NewGuid().ToString("N");

    /// <summary>
    /// Renews the client certificate in place: a new key and certificate with <paramref name="commonName"/> (the user for
    /// SASL EXTERNAL) overwrite <see cref="ClientPfxPath"/> — like cert-manager updating a mounted secret.
    /// </summary>
    public void RenewClientCertificate(string commonName)
    {
        using var renewed = Issue(Ca, commonName, serverAuth: false);
        File.WriteAllBytes(ClientPfxPath, renewed.Export(X509ContentType.Pkcs12, ClientPfxPassword));
    }

    /// <summary>A PEM file with a CA that did not issue the broker certificate.</summary>
    public string CreateUnrelatedCaFile()
    {
        using var other = CreateCa("Unrelated CA");
        return Write("other-ca.pem", other.ExportCertificatePem());
    }

    public void Dispose()
    {
        Ca.Dispose();
        Server.Dispose();
        Client.Dispose();
        System.IO.Directory.Delete(Directory, recursive: true);
    }

    private string Write(string name, string content)
    {
        var path = Path.Combine(Directory, name);
        File.WriteAllText(path, content);
        return path;
    }

    private static X509Certificate2 CreateCa(string name)
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest($"CN={name}", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
        request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));
        return request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
    }

    private static X509Certificate2 Issue(X509Certificate2 ca, string commonName, bool serverAuth)
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest($"CN={commonName}", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(
            X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
            [new Oid(serverAuth ? "1.3.6.1.5.5.7.3.1" : "1.3.6.1.5.5.7.3.2")], false));
        request.CertificateExtensions.Add(X509AuthorityKeyIdentifierExtension.CreateFromCertificate(ca, true, false));
        if (serverAuth)
        {
            var san = new SubjectAlternativeNameBuilder();
            san.AddDnsName(commonName);
            san.AddIpAddress(IPAddress.Loopback);
            request.CertificateExtensions.Add(san.Build());
        }

        using var issued = request.Create(
            ca, DateTimeOffset.UtcNow.AddHours(-1), DateTimeOffset.UtcNow.AddDays(29), RandomNumberGenerator.GetBytes(16));
        return issued.CopyWithPrivateKey(key);
    }
}
