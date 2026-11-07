using System.Threading.Channels;
using Serilog.Core;
using Serilog.Events;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Buffering;

/// <summary>
/// Bounded in-memory buffer between <c>Emit</c> (many writers, never blocks) and the publish loop (single reader).
/// Overflow follows <see cref="OverflowPolicy"/> and every dropped event is reported as
/// <see cref="LoggingFailureKind.Permanent"/>. No disk is ever used (ADR 0002).
/// </summary>
internal sealed class EventBuffer
{
    private readonly Channel<LogEvent> _channel;
    private readonly FailureReporter _reporter;
    private readonly OverflowPolicy _policy;
    private long _dropped;

    public EventBuffer(int capacity, OverflowPolicy policy, FailureReporter reporter)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        ArgumentNullException.ThrowIfNull(reporter);

        _reporter = reporter;
        _policy = policy;
        _channel = Channel.CreateBounded<LogEvent>(
            new BoundedChannelOptions(capacity)
            {
                FullMode = policy == OverflowPolicy.DropOldest ? BoundedChannelFullMode.DropOldest : BoundedChannelFullMode.DropWrite,
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = false,
            },
            OnDropped);
    }

    public ChannelReader<LogEvent> Reader => _channel.Reader;

    /// <summary>Events dropped because the buffer was full.</summary>
    public long DroppedCount => Interlocked.Read(ref _dropped);

    /// <summary>Returns <see langword="false"/> only after <see cref="Complete"/>; a full buffer drops per policy instead.</summary>
    public bool TryWrite(LogEvent logEvent) => _channel.Writer.TryWrite(logEvent);

    /// <summary>No more writes; the reader drains what is left.</summary>
    public void Complete() => _channel.Writer.TryComplete();

    private void OnDropped(LogEvent logEvent)
    {
        Interlocked.Increment(ref _dropped);
        _reporter.Report(
            LoggingFailureKind.Permanent,
            $"RabbitMQ sink buffer is full; event dropped ({_policy})",
            [logEvent]);
    }
}
