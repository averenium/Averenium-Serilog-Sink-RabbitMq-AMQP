using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Diagnostics;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting.Models;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Formatting;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting;

/// <summary>
/// Hands every undelivered event — from any path: burst overflow, formatting failure, outage-buffer overflow, broker
/// rejection, shutdown — to the fallbacks, on its own background worker so neither <c>Emit</c> nor the formatter ever
/// waits for a slow fallback sink:
/// <list type="bullet">
/// <item>optional raw output (stdout): formatted lines; events that were never formatted are formatted here;</item>
/// <item>the failure listener: records are decoded back to <see cref="LogEvent"/>s only if it consumes events.</item>
/// </list>
/// Batches are split into chunks of at most <see cref="MaxEventsPerReport"/>. While the sink runs, at most
/// <see cref="MaxPendingEvents"/> events wait for the fallback; beyond that the excess is counted and reported as lost
/// instead of growing memory. <see cref="LoggingFailureKind.Final"/> batches (shutdown) are exempt: those records are
/// already in memory and nothing new arrives, so every one of them is handed over — and they are handed over
/// <em>first</em>, so a backlog of earlier overflow drops cannot use up the shutdown hand-over time. Events that do not
/// reach a fallback are reported as lost with their own kind.
/// </summary>
internal sealed class UndeliveredEvents
{
    public const int MaxPendingEvents = 20_000;
    public const int MaxEventsPerReport = 1_000;

    private static readonly byte[] NewLine = "\n"u8.ToArray();
    private static readonly TimeSpan FinalLockTimeout = TimeSpan.FromSeconds(1);

    private readonly FailureReporter _reporter;
    private readonly ClefDecoder _decoder;
    private readonly ITextFormatter _formatter;
    private readonly Stream? _rawOutput;
    private readonly int _maxPendingEvents;
    private readonly Channel<Batch> _queue = Channel.CreateUnbounded<Batch>(new UnboundedChannelOptions { SingleReader = false });
    // Guards Process (no interleaved stdout lines) and the worker's local queues (so CompleteAsync can count them).
    private readonly Lock _processing = new();
    private readonly Queue<Batch> _finalBatches = new();
    private readonly Queue<Batch> _otherBatches = new();
    private Task? _worker;
    private volatile bool _stopped;
    private bool _finalized;
    private long _pending;
    private long _lostPermanent;
    private long _lostFinal;
    private int _completed;

    public UndeliveredEvents(FailureReporter reporter, ITextFormatter formatter, Stream? rawOutput, int maxPendingEvents = MaxPendingEvents)
    {
        _reporter = reporter;
        _formatter = formatter;
        _decoder = new ClefDecoder(formatter);
        _rawOutput = rawOutput;
        _maxPendingEvents = maxPendingEvents;
    }

    /// <summary>Events that never reached a fallback (reset when reported).</summary>
    public long LostCount => Interlocked.Read(ref _lostPermanent) + Interlocked.Read(ref _lostFinal);

    /// <summary>Starts the worker. Separate from the constructor so a failed sink construction leaks nothing.</summary>
    public void Start() => _worker ??= Task.Run(RunAsync);

    public void Report(LoggingFailureKind kind, string message, IReadOnlyList<LogEvent> events, Exception? exception = null)
    {
        for (var offset = 0; offset < events.Count; offset += MaxEventsPerReport)
        {
            Enqueue(new Batch(kind, message, exception, Slice(events, offset), null));
        }
    }

    public void Report(LoggingFailureKind kind, string message, IReadOnlyList<FormattedEvent> records, Exception? exception = null)
    {
        for (var offset = 0; offset < records.Count; offset += MaxEventsPerReport)
        {
            Enqueue(new Batch(kind, message, exception, null, Slice(records, offset)));
        }
    }

    /// <summary>
    /// Stops accepting batches and waits (within <paramref name="budget"/>) until everything queued is handed over. On
    /// timeout the worker is stopped: it finishes the chunk it is writing (if any) and hands over nothing more; what is
    /// left is counted and reported as lost. Nothing is written to stdout without the output lock.
    /// </summary>
    public async Task CompleteAsync(TimeSpan budget)
    {
        Interlocked.Exchange(ref _completed, 1);
        _queue.Writer.TryComplete();

        if (_worker is null)
        {
            lock (_processing)
            {
                MoveQueuedToLocal();
                while (_finalBatches.Count > 0 || _otherBatches.Count > 0)
                {
                    HandOverNextChunk();
                }
            }
        }
        else
        {
            try
            {
                await _worker.WaitAsync(budget > TimeSpan.Zero ? budget : TimeSpan.Zero).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                _stopped = true; // the worker stops before its next chunk
            }
        }

        if (_processing.TryEnter(FinalLockTimeout))
        {
            try
            {
                _finalized = true;
                MoveQueuedToLocal();
                CountLocalAsLost();
                ReportLost();
                Flush();
            }
            finally
            {
                _processing.Exit();
            }
        }
        else
        {
            // The worker is still writing a chunk to a slow fallback. Don't touch stdout or its local queues: when the
            // chunk is done it sees the stop flag and counts the rest as lost itself.
            ReportLost();
            _reporter.Flush();
        }
    }

    private void Enqueue(Batch batch)
    {
        if (batch.Count == 0)
        {
            return;
        }

        if (Volatile.Read(ref _completed) != 0)
        {
            AddLost(batch); // after shutdown: never block or write on the caller's thread; the fallbacks are closed
            return;
        }

        if (batch.Kind != LoggingFailureKind.Final && Interlocked.Read(ref _pending) + batch.Count > _maxPendingEvents)
        {
            AddLost(batch);
            return;
        }

        Interlocked.Add(ref _pending, batch.Count);
        if (!_queue.Writer.TryWrite(batch))
        {
            Interlocked.Add(ref _pending, -batch.Count);
            AddLost(batch);
        }
    }

    /// <summary>
    /// Hands batches over one chunk (≤ <see cref="MaxEventsPerReport"/> events) at a time; <see cref="LoggingFailureKind.Final"/>
    /// chunks always go first. Stops as soon as <see cref="CompleteAsync"/> gives up.
    /// </summary>
    private async Task RunAsync()
    {
        while (true)
        {
            bool hasWork;
            var stopped = false;
            lock (_processing)
            {
                if (_finalized)
                {
                    return;
                }

                MoveQueuedToLocal();
                if (_stopped)
                {
                    // CompleteAsync gave up (and could not take the lock): account for the rest here, write nothing.
                    CountLocalAsLost();
                    stopped = true;
                    hasWork = false;
                }
                else
                {
                    hasWork = _finalBatches.Count > 0 || _otherBatches.Count > 0;
                    if (hasWork)
                    {
                        HandOverNextChunk();
                    }
                }
            }

            ReportLost();
            if (stopped)
            {
                return;
            }

            if (!hasWork && !await _queue.Reader.WaitToReadAsync().ConfigureAwait(false))
            {
                lock (_processing)
                {
                    if (_finalBatches.Count == 0 && _otherBatches.Count == 0)
                    {
                        return; // completed and everything handed over
                    }
                }
            }
        }
    }

    /// <summary>Caller holds the lock.</summary>
    private void MoveQueuedToLocal()
    {
        while (_queue.Reader.TryRead(out var batch))
        {
            (batch.Kind == LoggingFailureKind.Final ? _finalBatches : _otherBatches).Enqueue(batch);
        }
    }

    /// <summary>Caller holds the lock.</summary>
    private void CountLocalAsLost()
    {
        while (_finalBatches.TryDequeue(out var final))
        {
            AddLost(final);
        }

        while (_otherBatches.TryDequeue(out var other))
        {
            AddLost(other);
        }
    }

    /// <summary>Takes the next chunk (Final first), merges same-reason batches and processes them. Caller holds the lock.</summary>
    [SuppressMessage("Design", "CA1031:Do not catch general exception types",
        Justification = "The worker must survive a failing fallback; errors go to SelfLog.")]
    private void HandOverNextChunk()
    {
        var source = _finalBatches.Count > 0 ? _finalBatches : _otherBatches;
        var chunk = new List<Batch>();
        var events = 0;
        while (source.TryPeek(out var next) && (chunk.Count == 0 || events + next.Count <= MaxEventsPerReport))
        {
            chunk.Add(source.Dequeue());
            events += next.Count;
        }

        foreach (var group in Coalesce(chunk))
        {
            try
            {
                Process(group);
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine("[RabbitMQAmqp] Reporting undelivered events failed: {0}", ex);
            }
        }

        Interlocked.Add(ref _pending, -events);
    }

    /// <summary>Merges consecutive batches of the same kind and reason (e.g. single-event burst drops) into one report.</summary>
    private static IEnumerable<Batch> Coalesce(List<Batch> batches)
    {
        Batch? current = null;
        foreach (var batch in batches)
        {
            if (current is not null && current.CanMerge(batch))
            {
                current = current.Merge(batch);
                continue;
            }

            if (current is not null)
            {
                yield return current;
            }

            current = batch;
        }

        if (current is not null)
        {
            yield return current;
        }
    }

    private void Process(Batch batch)
    {
        if (_rawOutput is not null)
        {
            WriteRaw(batch);
        }

        if (!_reporter.ListenerConsumesEvents)
        {
            _reporter.ReportCount(batch.Kind, batch.Message, batch.Count, batch.Exception);
            return;
        }

        IReadOnlyCollection<LogEvent> events = batch.Events ?? batch.Records!.Select(_decoder.Decode).ToList();
        _reporter.Report(batch.Kind, batch.Message, events, batch.Exception);
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types",
        Justification = "A broken raw output (e.g. closed stdout) must not break the sink.")]
    private void WriteRaw(Batch batch)
    {
        try
        {
            if (batch.Records is not null)
            {
                foreach (var record in batch.Records)
                {
                    _rawOutput!.Write(record.Body);
                    _rawOutput.Write(NewLine);
                }
            }
            else
            {
                foreach (var logEvent in batch.Events!)
                {
                    _rawOutput!.Write(FormatForRaw(logEvent));
                    _rawOutput.Write(NewLine);
                }
            }

            _rawOutput!.Flush();
        }
        catch (Exception ex)
        {
            SelfLog.WriteLine("[RabbitMQAmqp] Writing undelivered events to stdout failed: {0}", ex.Message);
        }
    }

    /// <summary>The configured formatter; if that is what failed, a minimal CLEF line (time, level, template, exception).</summary>
    [SuppressMessage("Design", "CA1031:Do not catch general exception types",
        Justification = "Falls back to a minimal line when the user's formatter throws.")]
    private byte[] FormatForRaw(LogEvent logEvent)
    {
        try
        {
            using var writer = new StringWriter(CultureInfo.InvariantCulture);
            _formatter.Format(logEvent, writer);
            return Encoding.UTF8.GetBytes(writer.ToString().TrimEnd('\r', '\n'));
        }
        catch (Exception)
        {
            using var buffer = new MemoryStream();
            using (var json = new Utf8JsonWriter(buffer))
            {
                json.WriteStartObject();
                json.WriteString("@t", logEvent.Timestamp);
                json.WriteString("@l", logEvent.Level.ToString());
                json.WriteString("@mt", logEvent.MessageTemplate.Text);
                if (logEvent.Exception is not null)
                {
                    json.WriteString("@x", logEvent.Exception.ToString());
                }

                json.WriteEndObject();
            }

            return buffer.ToArray();
        }
    }

    private void AddLost(Batch batch) =>
        Interlocked.Add(ref batch.Kind == LoggingFailureKind.Final ? ref _lostFinal : ref _lostPermanent, batch.Count);

    private void ReportLost()
    {
        var lostPermanent = Interlocked.Exchange(ref _lostPermanent, 0);
        if (lostPermanent > 0)
        {
            _reporter.ReportCount(LoggingFailureKind.Permanent,
                "The fallback could not keep up with undelivered RabbitMQ events; these were lost",
                (int)Math.Min(int.MaxValue, lostPermanent));
        }

        var lostFinal = Interlocked.Exchange(ref _lostFinal, 0);
        if (lostFinal > 0)
        {
            _reporter.ReportCount(LoggingFailureKind.Final,
                "The shutdown time ran out before these undelivered RabbitMQ events reached the fallback",
                (int)Math.Min(int.MaxValue, lostFinal));
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types",
        Justification = "Flushing a broken stdout must not break shutdown.")]
    private void Flush()
    {
        try
        {
            _rawOutput?.Flush();
        }
        catch (Exception)
        {
            // stdout closed — nothing else to do
        }

        _reporter.Flush();
    }

    private static IReadOnlyList<T> Slice<T>(IReadOnlyList<T> items, int offset)
    {
        if (offset == 0 && items.Count <= MaxEventsPerReport)
        {
            return items;
        }

        var count = Math.Min(MaxEventsPerReport, items.Count - offset);
        var slice = new T[count];
        for (var i = 0; i < count; i++)
        {
            slice[i] = items[offset + i];
        }

        return slice;
    }

    private sealed class Batch(
        LoggingFailureKind kind,
        string message,
        Exception? exception,
        IReadOnlyList<LogEvent>? events,
        IReadOnlyList<FormattedEvent>? records)
    {
        public LoggingFailureKind Kind { get; } = kind;

        public string Message { get; } = message;

        public Exception? Exception { get; } = exception;

        public IReadOnlyList<LogEvent>? Events { get; } = events;

        public IReadOnlyList<FormattedEvent>? Records { get; } = records;

        public int Count => Events?.Count ?? Records!.Count;

        public bool CanMerge(Batch other) =>
            other.Kind == Kind
            && string.Equals(other.Message, Message, StringComparison.Ordinal)
            && (other.Events is null) == (Events is null)
            && Count + other.Count <= MaxEventsPerReport;

        public Batch Merge(Batch other) => Events is not null
            ? new Batch(Kind, Message, Exception ?? other.Exception, [.. Events, .. other.Events!], null)
            : new Batch(Kind, Message, Exception ?? other.Exception, null, [.. Records!, .. other.Records!]);
    }
}
