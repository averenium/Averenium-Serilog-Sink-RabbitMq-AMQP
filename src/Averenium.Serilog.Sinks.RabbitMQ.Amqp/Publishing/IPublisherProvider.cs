using RabbitMQ.AMQP.Client;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Publishing;

/// <summary>What the publish loop needs from connection supervision; implemented by <c>ConnectionSupervisor</c>.</summary>
internal interface IPublisherProvider
{
    /// <summary>Completes with an open publisher once connection and publisher are ready.</summary>
    Task<IPublisher> WaitReadyAsync(CancellationToken cancellationToken);

    void ReportPublishSucceeded(IPublisher publisher);

    void ReportPublishTimeout(IPublisher publisher);

    void ReportTargetMissing(IPublisher publisher);

    /// <summary>Re-check connection state now (a publish failed with "not open" or similar).</summary>
    void Signal();
}
