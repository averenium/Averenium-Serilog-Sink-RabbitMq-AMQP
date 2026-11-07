namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Publishing;

internal sealed class PublishLoopOptions
{
    /// <summary>Maximum messages in flight (published concurrently) per round.</summary>
    public int BatchSizeLimit { get; init; } = 500;

    /// <summary>Bound for a single publish — the client ignores cancellation (C6).</summary>
    public TimeSpan PublishTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Releases (e.g. unroutable) tolerated per message before it is dropped and reported.</summary>
    public int MaxReleasedRetries { get; init; } = 5;

    /// <summary>Pause after a round without progress, growing up to <see cref="RetryMaxDelay"/>.</summary>
    public TimeSpan RetryMinDelay { get; init; } = TimeSpan.FromMilliseconds(100);

    public TimeSpan RetryMaxDelay { get; init; } = TimeSpan.FromSeconds(5);
}
