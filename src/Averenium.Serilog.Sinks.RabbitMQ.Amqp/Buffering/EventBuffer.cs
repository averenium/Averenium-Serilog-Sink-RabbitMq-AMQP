using System.Threading.Channels;
using Serilog.Events;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Buffering;

/// <summary>
/// Short burst buffer between <c>Emit</c> (many writers, never blocks) and the background formatter (single reader).
/// It stays almost empty because the formatter never waits for the broker; the long outage buffer is the
/// size-bounded <see cref="SizeBoundedQueue{T}"/>. Overflow follows <see cref="OverflowPolicy"/>; every dropped event
/// is handed to <c>onDropped</c> (the caller reports it — no dependency on reporting here). No disk is ever used.
/// </summary>
internal sealed class EventBuffer
{
    private readonly Channel<LogEvent> _channel;
    private readonly Action<LogEvent> _onDropped;
    private long _dropped;

    public EventBuffer(int capacity, OverflowPolicy policy, Action<LogEvent> onDropped)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        ArgumentNullException.ThrowIfNull(onDropped);

        _onDropped = onDropped;
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
        _onDropped(logEvent);
    }
}
