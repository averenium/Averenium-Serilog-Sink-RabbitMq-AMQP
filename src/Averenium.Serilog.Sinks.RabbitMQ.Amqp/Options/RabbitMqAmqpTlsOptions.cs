using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

// Public options live in the Options folder but keep the root namespace, so users need no extra using
// (documented exception to "namespace = folder", AGENTS.md).
#pragma warning disable IDE0130 // Namespace does not match folder structure
namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp;
#pragma warning restore IDE0130

/// <summary>
/// TLS details of the connection (<see cref="RabbitMqAmqpSinkOptions.Tls"/>). TLS itself is switched on by
/// <see cref="RabbitMqAmqpSinkOptions.UseTls"/> or <c>amqps://</c> hosts; without any of these settings the operating
/// system's trust store and defaults are used.
/// </summary>
public sealed class RabbitMqAmqpTlsOptions
{
    /// <summary>
    /// CA certificate(s) (PEM bundle or a DER/PEM file) the broker certificate must chain to — for clusters with an
    /// internal CA, e.g. a mounted Kubernetes secret. Trusted for this connection only. Read once at start-up.
    /// </summary>
    public string? CaCertificatePath { get; set; }

    /// <summary>
    /// Client certificate for mutual TLS: a <c>.pfx</c>/<c>.p12</c> file, or a PEM certificate (with the key in the same
    /// file or in <see cref="ClientCertificateKeyPath"/>). Read once at start-up.
    /// </summary>
    public string? ClientCertificatePath { get; set; }

    /// <summary>PEM private key for <see cref="ClientCertificatePath"/> when it is not in the certificate file.</summary>
    public string? ClientCertificateKeyPath { get; set; }

    /// <summary>Password of the <c>.pfx</c> file or of an encrypted PEM key.</summary>
    public string? ClientCertificatePassword { get; set; }

    /// <summary>Client certificate given in code (instead of <see cref="ClientCertificatePath"/>).</summary>
    public X509Certificate2? ClientCertificate { get; set; }

    /// <summary>
    /// Authenticate with the client certificate (SASL <c>EXTERNAL</c>, RabbitMQ plugin <c>rabbitmq_auth_mechanism_ssl</c>)
    /// instead of user name and password. Requires a client certificate. Default off.
    /// </summary>
    public bool UseExternalAuthentication { get; set; }

    /// <summary>TLS protocol versions, e.g. <c>Tls12, Tls13</c>; default: let the operating system choose.</summary>
    public SslProtocols? Protocols { get; set; }

    /// <summary>
    /// Broker certificate errors to tolerate, e.g. <c>RemoteCertificateNameMismatch</c> when connecting by IP. Prefer
    /// <see cref="CaCertificatePath"/> over accepting <c>RemoteCertificateChainErrors</c>. Default: none.
    /// </summary>
    public SslPolicyErrors AcceptablePolicyErrors { get; set; }

    /// <summary>Check the broker certificate against its revocation list (online). Default off.</summary>
    public bool CheckCertificateRevocation { get; set; }

    /// <summary>
    /// Custom broker certificate validation (code only); replaces <see cref="CaCertificatePath"/> and
    /// <see cref="AcceptablePolicyErrors"/> handling.
    /// </summary>
    public RemoteCertificateValidationCallback? CertificateValidation { get; set; }

    /// <summary>Anything besides <see cref="UseExternalAuthentication"/> that needs our own <c>TlsSettings</c>.</summary>
    internal bool HasCustomSettings =>
        Protocols is not null || AcceptablePolicyErrors != SslPolicyErrors.None || CheckCertificateRevocation
        || CaCertificatePath is not null || ClientCertificatePath is not null || ClientCertificate is not null
        || CertificateValidation is not null;

    internal bool IsConfigured => HasCustomSettings || UseExternalAuthentication;

    internal RabbitMqAmqpTlsOptions Clone() => (RabbitMqAmqpTlsOptions)MemberwiseClone();

    /// <summary>Throws <see cref="ArgumentException"/> for inconsistent TLS settings (at logger creation time).</summary>
    internal void Validate(bool usesTls, bool hasCredentials)
    {
        if (IsConfigured && !usesTls)
        {
            throw new ArgumentException("Tls options are set but no host uses TLS: set UseTls or use amqps:// hosts.", nameof(usesTls));
        }

        if (CertificateValidation is not null && CaCertificatePath is not null)
        {
            Fail("Use either Tls.CertificateValidation or Tls.CaCertificatePath, not both.");
        }

        if (ClientCertificate is not null && ClientCertificatePath is not null)
        {
            Fail("Use either Tls.ClientCertificate or Tls.ClientCertificatePath, not both.");
        }

        if (ClientCertificateKeyPath is not null && ClientCertificatePath is null)
        {
            Fail("Tls.ClientCertificateKeyPath requires Tls.ClientCertificatePath.");
        }

        if (UseExternalAuthentication && ClientCertificate is null && ClientCertificatePath is null)
        {
            Fail("Tls.UseExternalAuthentication requires a client certificate.");
        }

        if (UseExternalAuthentication && hasCredentials)
        {
            Fail("Tls.UseExternalAuthentication authenticates with the client certificate; remove Username/Password and credentials from host URIs.");
        }

        foreach (var (path, name) in new[]
                 {
                     (CaCertificatePath, nameof(CaCertificatePath)),
                     (ClientCertificatePath, nameof(ClientCertificatePath)),
                     (ClientCertificateKeyPath, nameof(ClientCertificateKeyPath)),
                 })
        {
            if (path is not null && !File.Exists(path))
            {
                Fail($"Tls.{name}: file '{path}' not found.");
            }
        }

        static void Fail(string message) => throw new ArgumentException(message);
    }
}
