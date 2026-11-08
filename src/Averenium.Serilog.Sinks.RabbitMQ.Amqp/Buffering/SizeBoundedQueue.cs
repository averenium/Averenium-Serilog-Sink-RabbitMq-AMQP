namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Buffering;

/// <summary>
/// FIFO queue bounded by the summed size of its items (the outage buffer, <c>QueueSizeLimitBytes</c>).
/// <see cref="System.Threading.Channels.Channel"/> cannot limit by size, hence lock + queue + counter.
/// Generic so that buffering knows nothing about formatting (no folder cycles). One writer, one reader.
/// Items that do not fit are handed back to the caller, which reports them in batches.
/// </summary>
internal sealed class SizeBoundedQueue<T>
    where T : class
{
    private readonly long _limit;
    private readonly OverflowPolicy _policy;
    private readonly Func<T, long> _sizeOf;
    private readonly Lock _gate = new();
    private readonly Queue<(T Item, long Size)> _queue = new();
    private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private TaskCompletionSource<bool>? _waiter;
    private long _size;
    private bool _completed;
    private long _dropped;

    public SizeBoundedQueue(long limit, OverflowPolicy policy, Func<T, long> sizeOf)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        ArgumentNullException.ThrowIfNull(sizeOf);
        _limit = limit;
        _policy = policy;
        _sizeOf = sizeOf;
    }

    public int Count
    {
        get
        {
            lock (_gate)
            {
                return _queue.Count;
            }
        }
    }

    /// <summary>Summed size of the queued items.</summary>
    public long Size
    {
        get
        {
            lock (_gate)
            {
                return _size;
            }
        }
    }

    public long DroppedCount => Interlocked.Read(ref _dropped);

    /// <summary>Completes when the writer calls <see cref="Complete"/> (items may still be queued).</summary>
    public Task Completion => _completion.Task;

    /// <summary>Completed by the writer and drained by the reader.</summary>
    public bool IsCompletedAndEmpty
    {
        get
        {
            lock (_gate)
            {
                return _completed && _queue.Count == 0;
            }
        }
    }

    /// <summary>
    /// Adds an item, evicting per <see cref="OverflowPolicy"/> when the limit would be exceeded. Items that did not fit
    /// (evicted ones, the new one with <see cref="OverflowPolicy.DropNewest"/>, or an item larger than the whole limit)
    /// are added to <paramref name="dropped"/>. Returns <see langword="false"/> after <see cref="Complete"/>.
    /// </summary>
    public bool TryEnqueue(T item, List<T> dropped)
    {
        ArgumentNullException.ThrowIfNull(item);
        ArgumentNullException.ThrowIfNull(dropped);

        var size = _sizeOf(item);
        TaskCompletionSource<bool>? waiter;
        lock (_gate)
        {
            if (_completed)
            {
                return false;
            }

            if (size > _limit || (_policy == OverflowPolicy.DropNewest && _size + size > _limit))
            {
                Drop(item, dropped);
                return true;
            }

            while (_queue.Count > 0 && _size + size > _limit)
            {
                var (oldest, oldestSize) = _queue.Dequeue();
                _size -= oldestSize;
                Drop(oldest, dropped);
            }

            _queue.Enqueue((item, size));
            _size += size;
            waiter = _waiter;
            _waiter = null;
        }

        waiter?.TrySetResult(true);
        return true;
    }

    public bool TryDequeue(out T item)
    {
        lock (_gate)
        {
            if (_queue.TryDequeue(out var next))
            {
                _size -= next.Size;
                item = next.Item;
                return true;
            }
        }

        item = null!;
        return false;
    }

    /// <summary>Dequeues the oldest item only if <paramref name="predicate"/> holds for it (atomic with respect to eviction).</summary>
    public bool TryDequeueIf(Func<T, bool> predicate, out T item)
    {
        lock (_gate)
        {
            if (_queue.TryPeek(out var next) && predicate(next.Item))
            {
                _queue.Dequeue();
                _size -= next.Size;
                item = next.Item;
                return true;
            }
        }

        item = null!;
        return false;
    }

    /// <summary>Completes with <see langword="true"/> when an item is available, <see langword="false"/> when completed and empty.</summary>
    public Task<bool> WaitToDequeueAsync(CancellationToken cancellationToken)
    {
        Task<bool> task;
        lock (_gate)
        {
            if (_queue.Count > 0)
            {
                return Task.FromResult(true);
            }

            if (_completed)
            {
                return Task.FromResult(false);
            }

            _waiter ??= new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            task = _waiter.Task;
        }

        return task.WaitAsync(cancellationToken);
    }

    /// <summary>No more writes; the reader drains what is left.</summary>
    public void Complete()
    {
        TaskCompletionSource<bool>? waiter;
        lock (_gate)
        {
            _completed = true;
            waiter = _waiter;
            _waiter = null;
        }

        waiter?.TrySetResult(false);
        _completion.TrySetResult();
    }

    /// <summary>Removes and returns everything still queued (used for the final report on shutdown).</summary>
    public List<T> DrainAll()
    {
        lock (_gate)
        {
            var all = _queue.Select(e => e.Item).ToList();
            _queue.Clear();
            _size = 0;
            return all;
        }
    }

    private void Drop(T item, List<T> dropped)
    {
        Interlocked.Increment(ref _dropped);
        dropped.Add(item);
    }
}
