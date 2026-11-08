using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Support;

/// <summary>In-memory test PKI: a self-signed CA and certificates issued by it.</summary>
internal static class TestCertificates
{
    public static X509Certificate2 CreateCa(string name = "Test CA")
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest($"CN={name}", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
        return request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
    }

    /// <summary>A certificate with private key, issued by <paramref name="ca"/>.</summary>
    public static X509Certificate2 Issue(X509Certificate2 ca, string commonName, bool client = false)
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest($"CN={commonName}", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
            [new Oid(client ? "1.3.6.1.5.5.7.3.2" : "1.3.6.1.5.5.7.3.1")], false));
        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName(commonName);
        request.CertificateExtensions.Add(san.Build());

        var serial = RandomNumberGenerator.GetBytes(16);
        using var issued = request.Create(ca, DateTimeOffset.UtcNow.AddHours(-1), DateTimeOffset.UtcNow.AddDays(29), serial);
        return issued.CopyWithPrivateKey(key);
    }
}
