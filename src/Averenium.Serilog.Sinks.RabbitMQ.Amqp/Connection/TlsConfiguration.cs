using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using RabbitMQ.AMQP.Client;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;

/// <summary>
/// Builds the client's <see cref="TlsSettings"/> and SASL mechanism from the sink options.
/// <para>
/// The client does not pass <see cref="TlsSettings.AcceptablePolicyErrors"/> to the TLS stack (verified in its source),
/// so the server certificate is validated by our own callback: it honours the acceptable policy errors and, with a CA
/// file, trusts that CA (custom root trust — typical for clusters with an internal CA).
/// <para>
/// Rotation: the client hands both callbacks and the client-certificate collection to every new connection (C11), so the
/// CA file and the client certificate are re-read at each TLS handshake and reloaded when their contents changed
/// (<see cref="ReloadingFiles{T}"/>) — a renewed mounted secret is used on the next (re)connect, without a restart.
/// Files are only read (at handshakes, i.e. when connecting), never written; nothing is polled.
/// </para>
/// </para>
/// </summary>
internal static class TlsConfiguration
{
    /// <summary>
    /// <see langword="null"/> settings when no TLS option is set (the client then uses its defaults for <c>amqps</c>).
    /// </summary>
    public static (TlsSettings? Tls, SaslMechanism? Sasl) Create(RabbitMqAmqpTlsOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var sasl = options.UseExternalAuthentication ? SaslMechanism.External : null;
        if (!options.HasCustomSettings)
        {
            return (null, sasl);
        }

        var tls = options.Protocols is { } protocols ? new TlsSettings(protocols) : new TlsSettings();
        tls.CheckCertificateRevocation = options.CheckCertificateRevocation;
        tls.AcceptablePolicyErrors = options.AcceptablePolicyErrors;

        if (options.ClientCertificate is { } given)
        {
            tls.ClientCertificates.Add(given);
        }
        else if (options.ClientCertificatePath is { } certificatePath)
        {
            // The first certificate also goes into ClientCertificates (the client hands that collection to every
            // connection, it is never changed afterwards, so it is never released); the selection callback returns the
            // current one. Later replaced certificates are released — on Windows that removes their key containers.
            X509Certificate2? initial = null;
            var client = new ReloadingFiles<X509Certificate2>(
                options.ClientCertificateKeyPath is { } keyPath ? [certificatePath, keyPath] : [certificatePath],
                () => LoadClientCertificate(options)!,
                replaced =>
                {
                    if (!ReferenceEquals(replaced, initial))
                    {
                        replaced.Dispose();
                    }
                });
            initial = client.Current;
            tls.ClientCertificates.Add(initial);
            tls.LocalCertificateSelectionCallback = (_, _, _, _, _) => client.Current;
        }

        Func<X509Certificate2Collection?> caRoots = static () => null;
        if (options.CaCertificatePath is { } caPath)
        {
            var ca = new ReloadingFiles<X509Certificate2Collection>(
                [caPath],
                () => LoadCertificates(caPath),
                replaced =>
                {
                    foreach (var certificate in replaced)
                    {
                        certificate.Dispose();
                    }
                });
            caRoots = () => ca.Current;
        }

        tls.RemoteCertificateValidationCallback = options.CertificateValidation
            ?? CreateValidation(caRoots, options.AcceptablePolicyErrors, options.CheckCertificateRevocation);

        return (tls, sasl);
    }

    /// <summary>
    /// Validates the server certificate: acceptable policy errors are ignored; chain errors are re-checked against the
    /// configured CA roots (if any); name mismatches and missing certificates still fail unless accepted.
    /// </summary>
    internal static RemoteCertificateValidationCallback CreateValidation(
        X509Certificate2Collection? caRoots, SslPolicyErrors acceptable, bool checkRevocation) =>
        CreateValidation(() => caRoots, acceptable, checkRevocation);

    /// <param name="caRoots">Current CA roots, asked at every handshake (reloaded when the file changed).</param>
    /// <param name="acceptable">Policy errors to ignore.</param>
    /// <param name="checkRevocation">Check revocation online.</param>
    internal static RemoteCertificateValidationCallback CreateValidation(
        Func<X509Certificate2Collection?> caRoots, SslPolicyErrors acceptable, bool checkRevocation) =>
        (_, certificate, chain, errors) =>
        {
            var remaining = errors & ~acceptable;
            if (remaining == SslPolicyErrors.None)
            {
                return true;
            }

            var roots = caRoots();
            if (roots is null || certificate is null || remaining != SslPolicyErrors.RemoteCertificateChainErrors)
            {
                return false;
            }

            using var custom = new X509Chain();
            custom.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
            custom.ChainPolicy.CustomTrustStore.AddRange(roots);
            custom.ChainPolicy.RevocationMode = checkRevocation ? X509RevocationMode.Online : X509RevocationMode.NoCheck;
            if (chain is not null)
            {
                foreach (var element in chain.ChainElements)
                {
                    custom.ChainPolicy.ExtraStore.Add(element.Certificate);
                }
            }

            var leaf = certificate as X509Certificate2;
            using var owned = leaf is null ? X509CertificateLoader.LoadCertificate(certificate.GetRawCertData()) : null;
            return custom.Build(leaf ?? owned!);
        };

    /// <summary>PEM bundle or a single DER/PEM certificate file.</summary>
    internal static X509Certificate2Collection LoadCertificates(string path)
    {
        var certificates = new X509Certificate2Collection();
        try
        {
            certificates.ImportFromPemFile(path);
        }
        catch (CryptographicException)
        {
            certificates.Add(X509CertificateLoader.LoadCertificateFromFile(path));
        }

        if (certificates.Count == 0)
        {
            certificates.Add(X509CertificateLoader.LoadCertificateFromFile(path));
        }

        return certificates;
    }

    /// <summary>
    /// The client certificate: given in code, or a <c>.pfx</c>/<c>.p12</c> file (with password), or a PEM certificate
    /// with its key (separate key file, or both in one file; an encrypted key uses the password).
    /// </summary>
    internal static X509Certificate2? LoadClientCertificate(RabbitMqAmqpTlsOptions options)
    {
        if (options.ClientCertificate is { } given)
        {
            return given;
        }

        if (options.ClientCertificatePath is not { } path)
        {
            return null;
        }

        var extension = Path.GetExtension(path);
        if (extension.Equals(".pfx", StringComparison.OrdinalIgnoreCase) || extension.Equals(".p12", StringComparison.OrdinalIgnoreCase))
        {
            return X509CertificateLoader.LoadPkcs12FromFile(path, options.ClientCertificatePassword);
        }

        using var pem = options.ClientCertificatePassword is { } password
            ? X509Certificate2.CreateFromEncryptedPemFile(path, password, options.ClientCertificateKeyPath)
            : X509Certificate2.CreateFromPemFile(path, options.ClientCertificateKeyPath);

        // A PEM key is ephemeral; SslStream (notably on Windows) needs a persisted one — round-trip through PKCS#12.
        return X509CertificateLoader.LoadPkcs12(pem.Export(X509ContentType.Pkcs12), password: null);
    }
}
