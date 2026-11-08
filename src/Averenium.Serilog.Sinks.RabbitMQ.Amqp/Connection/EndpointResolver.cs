using System.Globalization;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;

/// <summary>
/// Turns the configured endpoints into canonical AMQP URIs (<c>amqp(s)://user:pass@host:port/vhost</c>) — the only
/// form the AMQP client accepts for several nodes (it reads credentials and the virtual host from each URI).
/// <para>
/// Each entry may be a bare host (<c>rabbit-1</c>), <c>host:port</c>, <c>[ipv6]:port</c>, or a full
/// <c>amqp://</c>/<c>amqps://</c> URI. Credentials, virtual host and TLS may be given as separate options; giving the
/// same thing twice in conflicting ways (credentials or virtual host both in a URI and as an option, <c>UseTls</c> with an
/// <c>amqp://</c> URI) is rejected instead of silently picking one. The <c>Port</c> option is different by design: it is
/// the default for entries without a port, and a port written with the host wins (nodes may listen on different ports).
/// Credentials belong in the options or in a full URI, never in a plain host entry. User name and password are escaped,
/// so any characters work.
/// </para>
/// </summary>
internal static class EndpointResolver
{
    public const int AmqpPort = 5672;
    public const int AmqpsPort = 5671;
    private const string DefaultVirtualHost = "/";

    /// <exception cref="ArgumentException">An entry is malformed or conflicts with the separate options.</exception>
    public static IReadOnlyList<Uri> Resolve(
        IEnumerable<string> hostnames,
        string? username,
        string? password,
        string? virtualHost,
        int? port,
        bool useTls)
    {
        ArgumentNullException.ThrowIfNull(hostnames);
        if (password is not null && string.IsNullOrEmpty(username))
        {
            throw new ArgumentException("Password is set but Username is not.", nameof(password));
        }

        if (port is < 1 or > 65535)
        {
            throw new ArgumentException($"Port {port} is out of range 1..65535.", nameof(port));
        }

        var resolved = new List<Uri>();
        string? sharedVirtualHost = null;
        foreach (var raw in hostnames)
        {
            var entry = raw?.Trim();
            if (string.IsNullOrEmpty(entry))
            {
                throw new ArgumentException("Empty entry in Hostnames.", nameof(hostnames));
            }

            var endpoint = entry.Contains("://", StringComparison.Ordinal)
                ? FromUri(entry, username, password, virtualHost, port, useTls)
                : FromHost(entry, username, password, virtualHost, port, useTls);

            sharedVirtualHost ??= endpoint.VirtualHost;
            if (!string.Equals(sharedVirtualHost, endpoint.VirtualHost, StringComparison.Ordinal))
            {
                throw new ArgumentException(
                    $"All endpoints must use the same virtual host ('{sharedVirtualHost}' vs '{endpoint.VirtualHost}').", nameof(hostnames));
            }

            resolved.Add(endpoint.Uri);
        }

        if (resolved.Count == 0)
        {
            throw new ArgumentException("At least one RabbitMQ host is required (Hostnames).", nameof(hostnames));
        }

        return resolved;
    }

    /// <summary>
    /// Splits a host list (<c>rabbit-1;rabbit-2</c>, e.g. from an environment variable) at <c>,</c> and <c>;</c> — except
    /// inside the credentials of a URI, where an unescaped separator belongs to the user name or password
    /// (<c>amqp://user:p;w@rabbit-1</c>): a separator is kept when the URI so far has no <c>@</c> yet and an <c>@</c>
    /// follows before the next <c>://</c>.
    /// </summary>
    public static IReadOnlyList<string> SplitList(string list)
    {
        ArgumentNullException.ThrowIfNull(list);
        var parts = new List<string>();
        var start = 0;
        for (var i = 0; i < list.Length; i++)
        {
            if (list[i] is not (',' or ';') || IsInsideUriCredentials(list, start, i))
            {
                continue;
            }

            Add(list[start..i]);
            start = i + 1;
        }

        Add(list[start..]);
        return parts;

        void Add(string part)
        {
            part = part.Trim();
            if (part.Length > 0)
            {
                parts.Add(part);
            }
        }
    }

    private static bool IsInsideUriCredentials(string list, int tokenStart, int separator)
    {
        var scheme = list.IndexOf("://", tokenStart, separator - tokenStart, StringComparison.Ordinal);
        if (scheme < 0 || list.IndexOf('@', scheme, separator - scheme) >= 0)
        {
            return false;
        }

        var at = list.IndexOf('@', separator);
        var nextScheme = list.IndexOf("://", separator, StringComparison.Ordinal);
        return at >= 0 && (nextScheme < 0 || at < nextScheme);
    }

    private static (Uri Uri, string VirtualHost) FromUri(
        string entry, string? username, string? password, string? virtualHost, int? port, bool useTls)
    {
        if (!Uri.TryCreate(entry, UriKind.Absolute, out var uri)
            || uri.Scheme is not ("amqp" or "amqps")
            || string.IsNullOrEmpty(uri.Host))
        {
            throw new ArgumentException($"'{Redact(entry)}' is not a valid amqp:// or amqps:// URI.", nameof(entry));
        }

        if (useTls && uri.Scheme == "amqp")
        {
            throw new ArgumentException($"UseTls is set but '{Redact(entry)}' uses amqp:// — use amqps:// or drop UseTls.", nameof(useTls));
        }

        if (!string.IsNullOrEmpty(uri.UserInfo) && !string.IsNullOrEmpty(username))
        {
            throw new ArgumentException(
                $"Credentials are given both in '{Redact(entry)}' and as Username/Password — use one of them.", nameof(username));
        }

        var uriVirtualHost = VirtualHostOf(uri);
        if (uriVirtualHost is not null && virtualHost is not null
            && !string.Equals(uriVirtualHost, virtualHost, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"Virtual host '{uriVirtualHost}' in '{Redact(entry)}' conflicts with VirtualHost '{virtualHost}'.", nameof(virtualHost));
        }

        var effectivePort = uri.IsDefaultPort || uri.Port < 0
            ? port ?? (uri.Scheme == "amqps" ? AmqpsPort : AmqpPort)
            : uri.Port;
        var userInfo = string.IsNullOrEmpty(uri.UserInfo) ? UserInfo(username, password) : uri.UserInfo;
        var effectiveVirtualHost = uriVirtualHost ?? virtualHost ?? DefaultVirtualHost;
        return (Build(uri.Scheme, userInfo, uri.Host, effectivePort, effectiveVirtualHost), effectiveVirtualHost);
    }

    private static (Uri Uri, string VirtualHost) FromHost(
        string entry, string? username, string? password, string? virtualHost, int? port, bool useTls)
    {
        if (entry.Contains('@', StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"'{Redact(entry)}' looks like credentials in a plain host entry. Use Username/Password, or a full URI " +
                "(amqp://user:password@host; escape special characters in the password).", nameof(entry));
        }

        var (host, entryPort) = SplitHostAndPort(entry);
        if (Uri.CheckHostName(host.Trim('[', ']')) == UriHostNameType.Unknown)
        {
            throw new ArgumentException($"'{Redact(entry)}' is not a valid host name.", nameof(entry));
        }

        var scheme = useTls ? "amqps" : "amqp";
        var effectivePort = entryPort ?? port ?? (useTls ? AmqpsPort : AmqpPort);
        var effectiveVirtualHost = virtualHost ?? DefaultVirtualHost;
        return (Build(scheme, UserInfo(username, password), host, effectivePort, effectiveVirtualHost), effectiveVirtualHost);
    }

    /// <summary><c>host</c>, <c>host:port</c>, <c>[ipv6]</c>, <c>[ipv6]:port</c>, or a bare IPv6 address (no port).</summary>
    private static (string Host, int? Port) SplitHostAndPort(string entry)
    {
        if (entry.StartsWith('['))
        {
            var end = entry.IndexOf(']', StringComparison.Ordinal);
            if (end < 0)
            {
                throw new ArgumentException($"'{Redact(entry)}' has an unclosed '['.", nameof(entry));
            }

            var host = entry[..(end + 1)];
            var rest = entry[(end + 1)..];
            return rest.Length == 0 ? (host, null)
                : rest[0] == ':' ? (host, ParsePort(rest[1..], entry))
                : throw new ArgumentException($"'{Redact(entry)}' is not a valid host name.", nameof(entry));
        }

        var colons = entry.Count(c => c == ':');
        if (colons == 1)
        {
            var separator = entry.IndexOf(':', StringComparison.Ordinal);
            return (entry[..separator], ParsePort(entry[(separator + 1)..], entry));
        }

        return colons > 1 ? ($"[{entry}]", null) : (entry, null);
    }

    private static int ParsePort(string text, string entry) =>
        int.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out var port) && port is >= 1 and <= 65535
            ? port
            : throw new ArgumentException($"'{Redact(entry)}' has an invalid port.", nameof(entry));

    /// <summary>
    /// The virtual host in the URI path (the single segment after the first <c>/</c>, unescaped), or
    /// <see langword="null"/> when the path is empty. A literal <c>/</c> inside the virtual host must be written as
    /// <c>%2F</c> (<c>amqp://host/%2Fdata</c> is the virtual host <c>/data</c>). Anything that would silently lose a
    /// slash — <c>amqp://host//data</c>, <c>amqp://host/a/b</c>, a trailing <c>/</c> — is rejected.
    /// </summary>
    private static string? VirtualHostOf(Uri uri)
    {
        var path = uri.AbsolutePath;
        if (path is "" or "/")
        {
            return null;
        }

        var segment = path[1..];
        if (segment.Contains('/', StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"The path of '{Redact(uri.OriginalString)}' is ambiguous: a '/' inside the virtual host must be written as %2F " +
                "(e.g. amqp://host/%2Fdata for the virtual host '/data'), or use the VirtualHost option, which takes the name as is.");
        }

        return Uri.UnescapeDataString(segment);
    }

    private static string? UserInfo(string? username, string? password) =>
        string.IsNullOrEmpty(username) ? null
        : password is null ? Uri.EscapeDataString(username)
        : $"{Uri.EscapeDataString(username)}:{Uri.EscapeDataString(password)}";

    private static Uri Build(string scheme, string? userInfo, string host, int port, string virtualHost)
    {
        var credentials = string.IsNullOrEmpty(userInfo) ? string.Empty : userInfo + "@";
        var path = virtualHost == DefaultVirtualHost ? "/" : "/" + Uri.EscapeDataString(virtualHost);
        return new Uri(string.Create(CultureInfo.InvariantCulture, $"{scheme}://{credentials}{host}:{port}{path}"));
    }

    /// <summary>
    /// Never put a password into an exception message: everything up to the <em>last</em> <c>@</c> is hidden (a password
    /// may contain an unescaped <c>@</c>), with or without a scheme.
    /// </summary>
    private static string Redact(string entry)
    {
        var at = entry.LastIndexOf('@');
        if (at < 0)
        {
            return entry;
        }

        var schemeEnd = entry.IndexOf("://", StringComparison.Ordinal);
        var scheme = schemeEnd >= 0 && schemeEnd < at ? entry[..(schemeEnd + 3)] : string.Empty;
        return scheme + "***@" + entry[(at + 1)..];
    }
}
