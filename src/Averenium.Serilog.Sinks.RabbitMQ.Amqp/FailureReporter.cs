using System.Diagnostics.CodeAnalysis;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp;

/// <summary>
/// Single exit for events that leave the pipeline without being accepted by the broker ("no silent loss").
/// Defaults to <see cref="SelfLog.FailureListener"/>; Serilog replaces it (e.g. <c>WriteTo.FallbackChain</c>)
/// through <see cref="ISetLoggingFailureListener"/>.
/// </summary>
internal sealed class FailureReporter
{
    private readonly object _sender;
    private ILoggingFailureListener _listener = SelfLog.FailureListener;

    public FailureReporter(object sender) => _sender = sender;

    public void SetListener(ILoggingFailureListener listener)
    {
        ArgumentNullException.ThrowIfNull(listener);
        Volatile.Write(ref _listener, listener);
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types",
        Justification = "A throwing failure listener must not break the sink.")]
    public void Report(LoggingFailureKind kind, string message, IReadOnlyCollection<LogEvent>? events, Exception? exception = null)
    {
        var listener = Volatile.Read(ref _listener);
        if (!ReferenceEquals(listener, SelfLog.FailureListener))
        {
            // Keep a diagnostic trace even when a fallback sink receives the events.
            SelfLog.WriteLine("[RabbitMQAmqp] {0} failure, {1} event(s): {2}", kind, events?.Count ?? 0, message);
        }

        try
        {
            listener.OnLoggingFailed(_sender, kind, message, events, exception);
        }
        catch (Exception ex)
        {
            SelfLog.WriteLine("[RabbitMQAmqp] Failure listener threw: {0}", ex);
        }
    }
}
