using System.Diagnostics.CodeAnalysis;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Diagnostics;

/// <summary>
/// Lowest layer of "no silent loss": delivers reports to the failure listener (default
/// <see cref="SelfLog.FailureListener"/>; Serilog replaces it, e.g. <c>WriteTo.FallbackChain</c>, through
/// <see cref="ISetLoggingFailureListener"/>) and, with a custom listener, writes an aggregated diagnostic line to
/// <see cref="SelfLog"/> (one line per reason every <see cref="TraceInterval"/>). Knows nothing about formatting —
/// turning records back into events is done one layer up (<c>Formatting/UndeliveredEvents</c>).
/// </summary>
internal sealed class FailureReporter
{
    public static readonly TimeSpan TraceInterval = TimeSpan.FromSeconds(10);

    private readonly object _sender;
    private readonly TimeProvider _time;
    private readonly Action<string> _trace;
    private readonly Lock _gate = new();
    private readonly Dictionary<(LoggingFailureKind Kind, string Message), int> _pending = [];
    private ILoggingFailureListener _listener = SelfLog.FailureListener;
    private long _lastTrace;
    private bool _traced;
    private volatile bool _closed;

    public FailureReporter(object sender, TimeProvider? timeProvider = null, Action<string>? trace = null)
    {
        _sender = sender;
        _time = timeProvider ?? TimeProvider.System;
        _trace = trace ?? (line => SelfLog.WriteLine("{0}", line));
    }

    /// <summary>
    /// <see langword="true"/> when the listener wants the events themselves (e.g. a fallback sink). The default SelfLog
    /// listener only needs to know how many were lost, so callers can skip expensive work (decoding) for it.
    /// </summary>
    public bool ListenerConsumesEvents => !ReferenceEquals(Volatile.Read(ref _listener), SelfLog.FailureListener);

    public void SetListener(ILoggingFailureListener listener)
    {
        ArgumentNullException.ThrowIfNull(listener);
        Volatile.Write(ref _listener, listener);
    }

    /// <summary>Reports lost events.</summary>
    public void Report(LoggingFailureKind kind, string message, IReadOnlyCollection<LogEvent> events, Exception? exception = null)
    {
        ArgumentNullException.ThrowIfNull(events);
        if (TraceIfClosed(kind, message, events.Count))
        {
            return;
        }

        var listener = Volatile.Read(ref _listener);
        if (ListenerConsumesEvents)
        {
            Trace(kind, message, events.Count);
        }

        Notify(listener, kind, message, events, exception);
    }

    /// <summary>Reports lost events by count only (the listener does not consume events, or they are gone).</summary>
    public void ReportCount(LoggingFailureKind kind, string message, int count, Exception? exception = null)
    {
        if (TraceIfClosed(kind, message, count))
        {
            return;
        }

        var listener = Volatile.Read(ref _listener);
        if (ListenerConsumesEvents)
        {
            Trace(kind, message, count);
        }

        Notify(listener, kind, $"{message} ({count} event(s))", events: null, exception);
    }

    /// <summary>Reports a condition without events (e.g. a configuration error).</summary>
    public void Notify(LoggingFailureKind kind, string message, Exception? exception = null)
    {
        if (TraceIfClosed(kind, message, 0))
        {
            return;
        }

        var listener = Volatile.Read(ref _listener);
        if (ListenerConsumesEvents)
        {
            Trace(kind, message, 0);
        }

        Notify(listener, kind, message, events: null, exception);
    }

    /// <summary>
    /// Called at the very end of the sink's dispose: Serilog closes the fallback sinks right after, so from now on reports
    /// (e.g. from a fallback worker that was still stuck in a slow fallback) only go to the trace, never to the listener.
    /// </summary>
    public void Close() => _closed = true;

    /// <summary>Writes the aggregated trace that has not been written yet (call on shutdown).</summary>
    public void Flush()
    {
        lock (_gate)
        {
            FlushLocked();
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types",
        Justification = "A throwing failure listener must not break the sink.")]
    private void Notify(ILoggingFailureListener listener, LoggingFailureKind kind, string message, IReadOnlyCollection<LogEvent>? events, Exception? exception)
    {
        try
        {
            listener.OnLoggingFailed(_sender, kind, message, events, exception);
        }
        catch (Exception ex)
        {
            _trace($"[RabbitMQAmqp] Failure listener threw: {ex}");
        }
    }

    private bool TraceIfClosed(LoggingFailureKind kind, string message, int count)
    {
        if (!_closed)
        {
            return false;
        }

        _trace(count == 0
            ? $"[RabbitMQAmqp] {kind} failure after the sink was disposed: {message}"
            : $"[RabbitMQAmqp] {kind} failure after the sink was disposed, {count} event(s): {message}");
        return true;
    }

    private void Trace(LoggingFailureKind kind, string message, int count)
    {
        lock (_gate)
        {
            var key = (kind, message);
            _pending[key] = _pending.GetValueOrDefault(key) + count;

            var now = _time.GetTimestamp();
            if (!_traced || _time.GetElapsedTime(_lastTrace, now) >= TraceInterval)
            {
                FlushLocked();
            }
        }
    }

    private void FlushLocked()
    {
        foreach (var ((kind, message), count) in _pending)
        {
            _trace(count == 0
                ? $"[RabbitMQAmqp] {kind} failure: {message}"
                : $"[RabbitMQAmqp] {kind} failure, {count} event(s): {message}");
        }

        _pending.Clear();
        _lastTrace = _time.GetTimestamp();
        _traced = true;
    }
}
