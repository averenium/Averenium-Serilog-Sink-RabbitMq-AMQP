using System.Reflection;
using Serilog.Events;
using Serilog.Formatting;
using Serilog.Formatting.Compact;

// Public options live in the Options folder but keep the root namespace, so users need no extra using
// (documented exception to "namespace = folder", AGENTS.md).
#pragma warning disable IDE0130 // Namespace does not match folder structure
namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp;
#pragma warning restore IDE0130

/// <summary>Configuration of the RabbitMQ AMQP 1.0 sink.</summary>
public sealed class RabbitMqAmqpSinkOptions
{
    private static readonly string[] StandardExchangeTypes = ["topic", "direct", "fanout", "headers"];

    private List<string> _hostnames = [];

    // ---- Connection ------------------------------------------------------------------------------

    /// <summary>
    /// Broker nodes. Each entry is a host (<c>rabbit-1</c>), <c>host:port</c>, <c>[ipv6]:port</c>, or a full
    /// <c>amqp://</c> / <c>amqps://</c> URI. Configure every cluster node (or a load balancer): the sink fails over
    /// between them round-robin. All entries must resolve to the same virtual host.
    /// </summary>
    public IList<string> Hostnames => _hostnames;

    /// <summary>
    /// User name, applied to every host that does not carry its own credentials. Giving credentials both here and in a
    /// URI is rejected. Prefer this (and <see cref="Password"/>, e.g. from a Kubernetes secret) over credentials in URIs.
    /// </summary>
    public string? Username { get; set; }

    /// <summary>Password for <see cref="Username"/>; any characters are allowed (they are escaped).</summary>
    public string? Password { get; set; }

    /// <summary>
    /// Credentials that can change at runtime, instead of <see cref="Username"/>/<see cref="Password"/>: call
    /// <see cref="RabbitMqAmqpCredentialsSource.Update"/> when the secret rotates (e.g. from <c>IOptionsMonitor.OnChange</c>)
    /// and the sink reconnects with the new values. Code only — with <c>appsettings.json</c> the sink follows changes of
    /// its <c>username</c>/<c>password</c> arguments by itself.
    /// </summary>
    public RabbitMqAmqpCredentialsSource? Credentials { get; set; }

    /// <summary>Stops watching the configuration for credential changes (set by the <c>appsettings</c> overload).</summary>
    internal IDisposable? CredentialsWatch { get; set; }

    /// <summary>Virtual host. Default <c>/</c>. Must not conflict with a virtual host given in a URI path.</summary>
    public string? VirtualHost { get; set; }

    /// <summary>Port for hosts given without one. Default 5672, or 5671 with <see cref="UseTls"/>.</summary>
    public int? Port { get; set; }

    /// <summary>Connect with TLS (<c>amqps</c>) to hosts given without a scheme. Default off.</summary>
    public bool UseTls { get; set; }

    /// <summary>AMQP container-id, shown as the connection name in the management UI. Default: entry assembly name @ machine name.</summary>
    public string? ConnectionName { get; set; }

    /// <summary>First delay between reconnect attempts. Default 500 ms.</summary>
    public TimeSpan MinReconnectDelay { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>Upper bound of the delay between reconnect attempts; reconnecting never stops. Default 30 s.</summary>
    public TimeSpan MaxReconnectDelay { get; set; } = TimeSpan.FromSeconds(30);

    // ---- Target and routing ----------------------------------------------------------------------

    /// <summary>Exchange to publish to. Either <see cref="Exchange"/> or <see cref="Queue"/> is required.</summary>
    public string? Exchange { get; set; }

    /// <summary>Routing key used with <see cref="Exchange"/>. Ignored when <see cref="RoutingKeySelector"/> is set.</summary>
    public string? RoutingKey { get; set; }

    /// <summary>
    /// Queue to publish to directly when no <see cref="Exchange"/> is set. Use a quorum queue (or a stream) so
    /// stored logs survive the loss of a node.
    /// </summary>
    public string? Queue { get; set; }

    /// <summary>
    /// Computes the routing key per event (e.g. by level). Requires <see cref="Exchange"/>; messages are then
    /// addressed individually through an anonymous publisher.
    /// </summary>
    public Func<LogEvent, string>? RoutingKeySelector { get; set; }

    /// <summary>
    /// Declare the topology after every (re)connect: the exchange (<see cref="ExchangeType"/>) and, if
    /// <see cref="DeclareQueue"/> is set, a quorum queue bound with <see cref="BindingKey"/>; or the quorum
    /// <see cref="Queue"/> when publishing directly to a queue. Needs the <c>configure</c> permission. Default off.
    /// </summary>
    public bool DeclareTopology { get; set; }

    /// <summary>Exchange type for <see cref="DeclareTopology"/>: <c>topic</c> (default), <c>direct</c>, <c>fanout</c> or <c>headers</c>.</summary>
    public string ExchangeType { get; set; } = "topic";

    /// <summary>Quorum queue to declare and bind to <see cref="Exchange"/> when <see cref="DeclareTopology"/> is on.</summary>
    public string? DeclareQueue { get; set; }

    /// <summary>Binding key for <see cref="DeclareQueue"/>. Default: <see cref="RoutingKey"/>, or <c>#</c> when empty.</summary>
    public string? BindingKey { get; set; }

    // ---- Message ---------------------------------------------------------------------------------

    /// <summary>Formats the message body. Default: compact JSON (CLEF), one event per message.</summary>
    public ITextFormatter Formatter { get; set; } = new CompactJsonFormatter();

    /// <summary>AMQP content-type of the body. Default <c>application/json</c>.</summary>
    public string ContentType { get; set; } = "application/json";

    /// <summary>
    /// Additional AMQP application properties per event (e.g. tenant, service). Evaluated once per event on the
    /// background formatter thread; values must be string, number, bool, Guid or DateTime. If it throws or returns an
    /// unsupported value, that event is dropped and reported. The sink always sets message-id, content-type,
    /// creation-time and the <c>level</c> / <c>source-context</c> properties.
    /// </summary>
    public Func<LogEvent, IEnumerable<KeyValuePair<string, object>>>? MessageProperties { get; set; }

    /// <summary>
    /// AMQP <c>correlation-id</c> per event; return <see langword="null"/> for none. Default (when not set): the event's
    /// W3C trace id if <see cref="CorrelationIdFromTraceId"/> is on.
    /// </summary>
    public Func<LogEvent, string?>? CorrelationIdSelector { get; set; }

    /// <summary>Use the event's trace id as AMQP <c>correlation-id</c> when no selector is set. Default on.</summary>
    public bool CorrelationIdFromTraceId { get; set; } = true;

    /// <summary>Send messages as durable (persistent). Default on — turn off only for transient queues.</summary>
    public bool DurableMessages { get; set; } = true;

    // ---- Buffering -------------------------------------------------------------------------------

    /// <summary>
    /// Outage buffer: approximate memory for formatted events waiting for delivery while the broker is unavailable
    /// (payload plus a small per-event overhead — a predictable bound, not an exact heap size). Default 15 MiB.
    /// </summary>
    public long QueueSizeLimitBytes { get; set; } = 15L * 1024 * 1024;

    /// <summary>
    /// Burst buffer: events waiting for the background formatter (counted in events). It stays almost empty because
    /// formatting never waits for the broker. Default 10 000.
    /// </summary>
    public int QueueLimit { get; set; } = 10_000;

    /// <summary>What to drop when <see cref="QueueLimit"/> or <see cref="QueueSizeLimitBytes"/> is reached. Default <see cref="OverflowPolicy.DropOldest"/>.</summary>
    public OverflowPolicy OverflowPolicy { get; set; } = OverflowPolicy.DropOldest;

    // ---- Delivery and fallback -------------------------------------------------------------------

    /// <summary>
    /// Publish window: messages in flight plus messages waiting for a retry never exceed this; a new message is sent as
    /// soon as a slot frees up. Default 500.
    /// </summary>
    public int BatchSizeLimit { get; set; } = 500;

    /// <summary>How long to wait for the broker outcome of one message before retrying it. Default 30 s.</summary>
    public TimeSpan PublishTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>How often a message the broker released (e.g. unroutable) is retried before it is reported as dropped. Default 5.</summary>
    public int MaxReleasedRetries { get; set; } = 5;

    /// <summary>
    /// Hand events that could not be delivered within this time (e.g. the broker has been down for a while) to the
    /// failure listener / fallback sink instead of keeping them in memory until the buffer is full. They are then not sent
    /// to RabbitMQ anymore. Default: never (keep retrying while memory allows).
    /// </summary>
    public TimeSpan? FallbackAfter { get; set; }

    /// <summary>
    /// Also write every undelivered event (overflow, rejection, shutdown) as its formatted bytes, one per line, to
    /// stdout — the natural fallback in Kubernetes, where log collectors read CLEF/JSON directly. Works with any
    /// formatter and needs no parsing. Default off.
    /// </summary>
    public bool WriteUndeliveredToStandardOutput { get; set; }

    // ---- Shutdown --------------------------------------------------------------------------------

    /// <summary>
    /// Target for the <em>whole</em> shutdown (disposing the logger): delivering buffered events, handing what is left to
    /// the fallbacks, and closing publisher, connection and environment. The time is split between these phases so a
    /// broker that is down cannot starve the fallback hand-over. An idle sink stops immediately. Default 10 s.
    /// </summary>
    public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromSeconds(10);

    // ---- TLS -------------------------------------------------------------------------------------

    /// <summary>
    /// TLS details: private CA, client certificate (mutual TLS), SASL EXTERNAL, protocols, tolerated certificate errors.
    /// Applies to the hosts that use TLS (<see cref="UseTls"/> or <c>amqps://</c>).
    /// </summary>
    public RabbitMqAmqpTlsOptions Tls { get; set; } = new();

    /// <summary>Overrides the supervisor's client-recovery grace (C13); tests only. Default: MaxReconnectDelay + 15 s.</summary>
    internal TimeSpan? ClientRecoveryTimeout { get; set; }

    internal string EffectiveConnectionName =>
        string.IsNullOrWhiteSpace(ConnectionName)
            ? $"{Assembly.GetEntryAssembly()?.GetName().Name ?? "serilog"}@{Environment.MachineName}"
            : ConnectionName;

    internal string EffectiveBindingKey =>
        !string.IsNullOrEmpty(BindingKey) ? BindingKey
        : !string.IsNullOrEmpty(RoutingKey) ? RoutingKey
        : "#";

    /// <summary>
    /// A copy taken when the sink is created, so changing the caller's options object afterwards has no effect.
    /// Delegates and the formatter are shared (they are not copied).
    /// </summary>
    internal RabbitMqAmqpSinkOptions Snapshot()
    {
        var copy = (RabbitMqAmqpSinkOptions)MemberwiseClone();
        copy._hostnames = [.. _hostnames];
        copy.Tls = (Tls ?? new RabbitMqAmqpTlsOptions()).Clone();
        return copy;
    }

    /// <summary>Credentials as configured right now: from <see cref="Credentials"/>, else the static options.</summary>
    internal RabbitMqAmqpCredentialsSource.Snapshot? CurrentCredentials() =>
        Credentials?.Current
        ?? (Username is null && Password is null ? null : new RabbitMqAmqpCredentialsSource.Snapshot(Username ?? string.Empty, Password));

    /// <summary>The configured hosts as canonical AMQP URIs (credentials, virtual host, port and TLS applied).</summary>
    internal IReadOnlyList<Uri> ResolveEndpoints(RabbitMqAmqpCredentialsSource.Snapshot? credentials) =>
        Connection.EndpointResolver.Resolve(
            Hostnames, credentials?.Username is { Length: > 0 } user ? user : null, credentials?.Password, VirtualHost, Port, UseTls);

    /// <summary>Endpoints with the current credentials.</summary>
    internal IReadOnlyList<Uri> ResolveEndpoints() => ResolveEndpoints(CurrentCredentials());

    /// <summary>Throws <see cref="ArgumentException"/> for an unusable or ambiguous configuration (at logger creation time).</summary>
    internal void Validate()
    {
        // Throws with a precise message for malformed hosts and conflicting credentials / virtual host / TLS.
        ArgumentNullException.ThrowIfNull(Tls);
        if (Credentials is not null && (Username is not null || Password is not null))
        {
            throw new ArgumentException("Use either Credentials or Username/Password, not both.", nameof(Credentials));
        }

        var endpoints = ResolveEndpoints();
        Tls.Validate(
            endpoints.Any(uri => uri.Scheme == "amqps"),
            hasCredentials: endpoints.Any(uri => !string.IsNullOrEmpty(uri.UserInfo))); // options or URI userinfo

        var hasExchange = !string.IsNullOrWhiteSpace(Exchange);
        var hasQueue = !string.IsNullOrWhiteSpace(Queue);
        if (hasExchange == hasQueue)
        {
            throw new ArgumentException(
                "Set exactly one publish target: Exchange (+ RoutingKey) or Queue. " +
                "To declare a queue bound to the exchange use DeclareQueue.", nameof(Exchange));
        }

        if (hasQueue && (!string.IsNullOrEmpty(RoutingKey) || RoutingKeySelector is not null))
        {
            throw new ArgumentException("RoutingKey / RoutingKeySelector are only used with Exchange.", nameof(RoutingKey));
        }

        if (!string.IsNullOrWhiteSpace(DeclareQueue) && !(DeclareTopology && hasExchange))
        {
            throw new ArgumentException("DeclareQueue requires DeclareTopology = true and Exchange.", nameof(DeclareQueue));
        }

        if (!string.IsNullOrEmpty(BindingKey) && string.IsNullOrWhiteSpace(DeclareQueue))
        {
            throw new ArgumentException("BindingKey is only used together with DeclareQueue.", nameof(BindingKey));
        }

        if (DeclareTopology && hasExchange
            && !StandardExchangeTypes.Contains(ExchangeType, StringComparer.Ordinal)
            && !ExchangeType.StartsWith("x-", StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"Unknown ExchangeType '{ExchangeType}'. Use topic, direct, fanout, headers or a plugin type (x-...).",
                nameof(ExchangeType));
        }

        ArgumentNullException.ThrowIfNull(Formatter);
        ArgumentException.ThrowIfNullOrWhiteSpace(ContentType);
        ArgumentOutOfRangeException.ThrowIfLessThan(QueueLimit, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(QueueSizeLimitBytes, 1024L);
        ArgumentOutOfRangeException.ThrowIfLessThan(BatchSizeLimit, 1);
        ArgumentOutOfRangeException.ThrowIfNegative(MaxReleasedRetries);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(PublishTimeout, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(MinReconnectDelay, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxReconnectDelay, MinReconnectDelay);
        ArgumentOutOfRangeException.ThrowIfLessThan(ShutdownTimeout, TimeSpan.Zero);
        if (FallbackAfter is { } fallbackAfter)
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(fallbackAfter, TimeSpan.Zero, nameof(FallbackAfter));
        }
    }
}
