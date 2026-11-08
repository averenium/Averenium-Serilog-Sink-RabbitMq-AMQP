using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Buffering;
using Serilog.Events;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Buffering;

public sealed class EventBufferTests
{
    private readonly List<LogEvent> _dropped = [];

    [Test]
    public async Task Drop_oldest_keeps_the_most_recent_events_and_hands_over_the_dropped_ones()
    {
        var buffer = new EventBuffer(3, OverflowPolicy.DropOldest, _dropped.Add);

        for (var n = 1; n <= 5; n++)
        {
            await Assert.That(buffer.TryWrite(TestEvents.Create(n))).IsTrue();
        }

        await Assert.That(string.Join(",", Drain(buffer))).IsEqualTo("3,4,5");
        await Assert.That(string.Join(",", _dropped.Select(e => e.N()))).IsEqualTo("1,2");
        await Assert.That(buffer.DroppedCount).IsEqualTo(2L);
    }

    [Test]
    public async Task Drop_newest_keeps_the_first_events_and_hands_over_the_rejected_ones()
    {
        var buffer = new EventBuffer(3, OverflowPolicy.DropNewest, _dropped.Add);

        for (var n = 1; n <= 5; n++)
        {
            buffer.TryWrite(TestEvents.Create(n));
        }

        await Assert.That(string.Join(",", Drain(buffer))).IsEqualTo("1,2,3");
        await Assert.That(string.Join(",", _dropped.Select(e => e.N()))).IsEqualTo("4,5");
    }

    [Test]
    public async Task Writes_after_complete_are_refused()
    {
        var buffer = new EventBuffer(3, OverflowPolicy.DropOldest, _dropped.Add);
        buffer.TryWrite(TestEvents.Create(1));

        buffer.Complete();

        await Assert.That(buffer.TryWrite(TestEvents.Create(2))).IsFalse();
        await Assert.That(string.Join(",", Drain(buffer))).IsEqualTo("1");
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
