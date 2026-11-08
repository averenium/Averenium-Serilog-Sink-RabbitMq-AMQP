using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Buffering;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting.Models;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting;

/// <summary>
/// Background stage between <c>Emit</c> and publishing (ADR 0004): takes <see cref="LogEvent"/>s from the short burst
/// channel, formats them and puts the records into the size-bounded outage queue. It never waits for the broker, so
/// the burst channel stays almost empty and a long outage is buffered as compact bytes. Overflow and formatting
/// failures are handed to <see cref="UndeliveredEvents"/> in batches (once per chunk) — the fallbacks run on its own
/// worker, so a slow fallback never slows the formatter down.
/// </summary>
internal sealed class FormatterLoop
{
    private const int ChunkSize = 1_000;

    private readonly ChannelReader<LogEvent> _input;
    private readonly SizeBoundedQueue<FormattedEvent> _output;
    private readonly LogEventFormatter _formatter;
    private readonly UndeliveredEvents _undelivered;
    private readonly string _overflowMessage;

    public FormatterLoop(
        ChannelReader<LogEvent> input,
        SizeBoundedQueue<FormattedEvent> output,
        LogEventFormatter formatter,
        UndeliveredEvents undelivered,
        OverflowPolicy overflowPolicy)
    {
        _input = input;
        _output = output;
        _formatter = formatter;
        _undelivered = undelivered;
        _overflowMessage = $"RabbitMQ sink buffer (QueueSizeLimitBytes) is full; events dropped ({overflowPolicy})";
    }

    /// <summary>
    /// Runs until the input is completed and drained, or <paramref name="abortToken"/> fires; always completes the
    /// output queue at the end so the publish loop can finish. Never throws.
    /// </summary>
    [SuppressMessage("Design", "CA1031:Do not catch general exception types",
        Justification = "The loop must survive unexpected failures; they go to SelfLog.")]
    public async Task RunAsync(CancellationToken abortToken)
    {
        try
        {
            while (await _input.WaitToReadAsync(abortToken).ConfigureAwait(false))
            {
                try
                {
                    FormatChunk();
                }
                catch (Exception ex)
                {
                    SelfLog.WriteLine("[RabbitMQAmqp] Formatter loop failure: {0}", ex);
                }
            }
        }
        catch (OperationCanceledException) when (abortToken.IsCancellationRequested)
        {
            // Shutdown deadline: leftovers in the input are reported by the sink.
        }
        finally
        {
            _output.Complete();
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types",
        Justification = "A formatter / selector / MessageProperties failure drops only that event, reported to the listener.")]
    private void FormatChunk()
    {
        var dropped = new List<FormattedEvent>();
        List<LogEvent>? failed = null;
        Exception? firstError = null;

        for (var i = 0; i < ChunkSize && _input.TryRead(out var logEvent); i++)
        {
            FormattedEvent record;
            try
            {
                record = _formatter.Format(logEvent);
            }
            catch (Exception ex)
            {
                (failed ??= []).Add(logEvent);
                firstError ??= ex;
                continue;
            }

            if (!_output.TryEnqueue(record, dropped))
            {
                dropped.Add(record); // output completed (cannot happen before this loop completes it)
            }
        }

        if (failed is not null)
        {
            _undelivered.Report(LoggingFailureKind.Permanent,
                $"Failed to format log events for RabbitMQ: {firstError!.Message}", failed, firstError);
        }

        if (dropped.Count > 0)
        {
            _undelivered.Report(LoggingFailureKind.Permanent, _overflowMessage, dropped);
        }
    }
}
