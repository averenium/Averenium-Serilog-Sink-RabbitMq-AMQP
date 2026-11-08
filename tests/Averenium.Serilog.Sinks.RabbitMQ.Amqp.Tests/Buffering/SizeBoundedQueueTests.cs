using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Buffering;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Buffering;

public sealed class SizeBoundedQueueTests
{
    [Test]
    public async Task Drop_oldest_evicts_until_the_new_item_fits_the_limit()
    {
        var queue = Create(300, OverflowPolicy.DropOldest);
        var dropped = new List<Item>();

        for (var n = 1; n <= 5; n++)
        {
            queue.TryEnqueue(new Item(n, 100), dropped);
        }

        await Assert.That(string.Join(",", Drain(queue))).IsEqualTo("3,4,5");
        await Assert.That(string.Join(",", dropped.Select(i => i.Id))).IsEqualTo("1,2");
        await Assert.That(queue.Size).IsEqualTo(0L);
    }

    [Test]
    public async Task A_big_item_evicts_several_small_ones()
    {
        var queue = Create(400, OverflowPolicy.DropOldest);
        var dropped = new List<Item>();
        for (var n = 1; n <= 4; n++)
        {
            queue.TryEnqueue(new Item(n, 100), dropped);
        }

        queue.TryEnqueue(new Item(5, 250), dropped);

        await Assert.That(string.Join(",", dropped.Select(i => i.Id))).IsEqualTo("1,2,3");
        await Assert.That(queue.Size).IsEqualTo(350L);
    }

    [Test]
    public async Task Drop_newest_keeps_what_is_queued()
    {
        var queue = Create(200, OverflowPolicy.DropNewest);
        var dropped = new List<Item>();

        for (var n = 1; n <= 4; n++)
        {
            queue.TryEnqueue(new Item(n, 100), dropped);
        }

        await Assert.That(string.Join(",", Drain(queue))).IsEqualTo("1,2");
        await Assert.That(string.Join(",", dropped.Select(i => i.Id))).IsEqualTo("3,4");
    }

    [Test]
    public async Task An_item_larger_than_the_whole_limit_is_dropped_immediately()
    {
        var queue = Create(1_000, OverflowPolicy.DropOldest);
        var dropped = new List<Item>();
        queue.TryEnqueue(new Item(1, 100), dropped);

        queue.TryEnqueue(new Item(2, 5_000), dropped);

        await Assert.That(dropped.Single().Id).IsEqualTo(2);
        await Assert.That(queue.Count).IsEqualTo(1).Because("the queued item is kept");
    }

    [Test]
    public async Task Waiting_reader_is_woken_by_enqueue_and_by_complete(CancellationToken ct)
    {
        var queue = Create(1_000_000, OverflowPolicy.DropOldest);

        var waitForData = queue.WaitToDequeueAsync(ct);
        await Assert.That(waitForData.IsCompleted).IsFalse();
        queue.TryEnqueue(new Item(1, 10), []);
        await Assert.That(await waitForData.WaitAsync(TimeSpan.FromSeconds(5), ct)).IsTrue();

        queue.TryDequeue(out _);
        var waitForEnd = queue.WaitToDequeueAsync(ct);
        queue.Complete();
        await Assert.That(await waitForEnd.WaitAsync(TimeSpan.FromSeconds(5), ct)).IsFalse();
        await Assert.That(queue.IsCompletedAndEmpty).IsTrue();
        await Assert.That(queue.TryEnqueue(new Item(2, 10), [])).IsFalse();
    }

    private static SizeBoundedQueue<Item> Create(long limit, OverflowPolicy policy) => new(limit, policy, i => i.Size);

    private static List<int> Drain(SizeBoundedQueue<Item> queue)
    {
        var ids = new List<int>();
        while (queue.TryDequeue(out var item))
        {
            ids.Add(item.Id);
        }

        return ids;
    }

    private sealed record Item(int Id, long Size);

    [Test]
    public async Task Conditional_dequeue_takes_the_head_only_when_it_matches()
    {
        var queue = new SizeBoundedQueue<string>(1_000, OverflowPolicy.DropOldest, s => s.Length);
        queue.TryEnqueue("old", []);
        queue.TryEnqueue("new", []);

        var tookNew = queue.TryDequeueIf(s => s == "new", out _);
        var tookOld = queue.TryDequeueIf(s => s == "old", out var old);

        await Assert.That(tookNew).IsFalse().Because("only the head is considered");
        await Assert.That(tookOld).IsTrue();
        await Assert.That(old).IsEqualTo("old");
        await Assert.That(queue.Size).IsEqualTo(3L);
    }
}
