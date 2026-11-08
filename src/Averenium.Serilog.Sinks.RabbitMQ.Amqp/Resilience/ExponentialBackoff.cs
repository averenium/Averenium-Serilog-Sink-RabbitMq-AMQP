namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Resilience;

/// <summary>
/// Thread-safe exponential back-off with jitter that never gives up.
/// Delay for attempt <c>n</c> (0-based) is <c>min * 2^n</c>, capped at <c>max</c>, then randomised by ±<c>jitter</c>
/// and clamped to <c>[min, max]</c>.
/// </summary>
internal sealed class ExponentialBackoff
{
    private const int MaxExponent = 30;

    private readonly TimeSpan _min;
    private readonly TimeSpan _max;
    private readonly double _jitter;
    private readonly Func<double> _random;
    private readonly Lock _gate = new();
    private int _attempt;

    public ExponentialBackoff(TimeSpan min, TimeSpan max, double jitter = 0.2, Func<double>? random = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(min, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThan(max, min);
        ArgumentOutOfRangeException.ThrowIfNegative(jitter);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(jitter, 1.0);

        _min = min;
        _max = max;
        _jitter = jitter;
        _random = random ?? Random.Shared.NextDouble;
    }

    /// <summary>Number of delays handed out since the last <see cref="Reset"/>.</summary>
    public int Attempt
    {
        get
        {
            lock (_gate)
            {
                return _attempt;
            }
        }
    }

    public TimeSpan Next()
    {
        int exponent;
        lock (_gate)
        {
            exponent = Math.Min(_attempt, MaxExponent);
            if (_attempt < int.MaxValue)
            {
                _attempt++;
            }
        }

        return Compute(_min, _max, exponent, _jitter, _random());
    }

    /// <summary>
    /// Stateless form for callers that keep the attempt count themselves (e.g. per message).
    /// <paramref name="random"/> is a value in [0, 1).
    /// </summary>
    public static TimeSpan Compute(TimeSpan min, TimeSpan max, int attempt, double jitter, double random)
    {
        var exponent = Math.Clamp(attempt, 0, MaxExponent);
        var baseTicks = Math.Min(max.Ticks, min.Ticks * Math.Pow(2, exponent));
        var factor = 1 - jitter + (2 * jitter * random);
        var ticks = Math.Clamp((long)(baseTicks * factor), min.Ticks, max.Ticks);
        return TimeSpan.FromTicks(ticks);
    }

    public void Reset()
    {
        lock (_gate)
        {
            _attempt = 0;
        }
    }
}
