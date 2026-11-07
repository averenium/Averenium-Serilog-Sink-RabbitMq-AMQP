using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Buffering;
using Serilog.Core;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Buffering;

public sealed class EventBufferTests
{
    [Test]
    public async Task Drop_oldest_keeps_the_most_recent_events_and_reports_the_dropped_ones()
    {
        var listener = new CollectingFailureListener();
        var buffer = CreateBuffer(3, OverflowPolicy.DropOldest, listener);

        for (var n = 1; n <= 5; n++)
        {
            await Assert.That(buffer.TryWrite(TestEvents.Create(n))).IsTrue();
        }

        await Assert.That(string.Join(",", Drain(buffer))).IsEqualTo("3,4,5");
        await Assert.That(string.Join(",", listener.Events.Select(e => e.Event.N()))).IsEqualTo("1,2");
        await Assert.That(listener.Events.All(e => e.Kind == LoggingFailureKind.Permanent)).IsTrue();
        await Assert.That(buffer.DroppedCount).IsEqualTo(2L);
    }

    [Test]
    public async Task Drop_newest_keeps_the_first_events_and_reports_the_rejected_ones()
    {
        var listener = new CollectingFailureListener();
        var buffer = CreateBuffer(3, OverflowPolicy.DropNewest, listener);

        for (var n = 1; n <= 5; n++)
        {
            buffer.TryWrite(TestEvents.Create(n));
        }

        await Assert.That(string.Join(",", Drain(buffer))).IsEqualTo("1,2,3");
        await Assert.That(string.Join(",", listener.Events.Select(e => e.Event.N()))).IsEqualTo("4,5");
    }

    [Test]
    public async Task Writes_after_complete_are_refused()
    {
        var buffer = CreateBuffer(3, OverflowPolicy.DropOldest, new CollectingFailureListener());
        buffer.TryWrite(TestEvents.Create(1));

        buffer.Complete();

        await Assert.That(buffer.TryWrite(TestEvents.Create(2))).IsFalse();
        await Assert.That(string.Join(",", Drain(buffer))).IsEqualTo("1");
    }

    private static EventBuffer CreateBuffer(int capacity, OverflowPolicy policy, CollectingFailureListener listener)
    {
        var reporter = new FailureReporter(new object());
        reporter.SetListener(listener);
        return new EventBuffer(capacity, policy, reporter);
    }

    private static List<int> Drain(EventBuffer buffer)
    {
        var result = new List<int>();
        while (buffer.Reader.TryRead(out var logEvent))
        {
            result.Add(logEvent.N());
        }

        return result;
    }
}
