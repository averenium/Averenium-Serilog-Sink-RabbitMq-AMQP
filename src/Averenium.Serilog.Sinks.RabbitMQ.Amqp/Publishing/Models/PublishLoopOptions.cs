namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Publishing.Models;

internal sealed class PublishLoopOptions
{
    /// <summary>
    /// Sliding window: messages in flight plus messages waiting for a retry never exceed this, so the loop never
    /// pulls more from the buffer than it can hold.
    /// </summary>
    public int BatchSizeLimit { get; init; } = 500;

    /// <summary>Bound for a single publish — the client ignores cancellation (C6).</summary>
    public TimeSpan PublishTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Releases (e.g. unroutable) tolerated per message before it is dropped and reported.</summary>
    public int MaxReleasedRetries { get; init; } = 5;

    /// <summary>
    /// Spacing of retries after <c>Released</c> or <c>amqp:resource-limit-exceeded</c>, doubling per attempt up to
    /// <see cref="SlowRetryMaxDelay"/>. With the defaults 5 releases take ≈ 31 s, long enough for a binding or a
    /// re-created quorum queue to appear.
    /// </summary>
    public TimeSpan SlowRetryMinDelay { get; init; } = TimeSpan.FromSeconds(1);

    public TimeSpan SlowRetryMaxDelay { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Spacing of retries after connection-type failures (not open, connection lost, target missing).</summary>
    public TimeSpan RetryMinDelay { get; init; } = TimeSpan.FromMilliseconds(100);

    public TimeSpan RetryMaxDelay { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>Records waiting longer than this (queue or retry) go to the fallback instead; <see langword="null"/> = never.</summary>
    public TimeSpan? FallbackAfter { get; init; }
}
