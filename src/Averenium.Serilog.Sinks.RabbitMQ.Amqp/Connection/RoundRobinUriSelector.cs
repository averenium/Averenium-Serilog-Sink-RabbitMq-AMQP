using RabbitMQ.AMQP.Client;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;

/// <summary>
/// Picks broker URIs in turn. The client asks the selector on every (re)connect attempt, so consecutive
/// attempts go to different nodes and every configured node is tried — unlike the default random selector,
/// which may hit the same dead node repeatedly.
/// </summary>
internal sealed class RoundRobinUriSelector : IUriSelector
{
    private int _next = -1;

    public Uri Select(ICollection<Uri> uris)
    {
        ArgumentNullException.ThrowIfNull(uris);
        if (uris.Count == 0)
        {
            throw new ArgumentException("At least one URI is required.", nameof(uris));
        }

        var index = (int)((uint)Interlocked.Increment(ref _next) % (uint)uris.Count);
        return uris is IList<Uri> list ? list[index] : uris.ElementAt(index);
    }
}
