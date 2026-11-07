using RabbitMQ.AMQP.Client;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;

/// <summary>
/// Idempotently declares the sink's topology through the AMQP 1.0 management API after every (re)connect.
/// Queues are always quorum queues so stored logs survive the loss of a node.
/// </summary>
internal static class TopologyDeclarer
{
    public static async Task DeclareAsync(IConnection connection, RabbitMqAmqpSinkOptions options)
    {
        var management = connection.Management();

        if (!string.IsNullOrWhiteSpace(options.Exchange))
        {
            await management.Exchange(options.Exchange).Type(options.ExchangeType).DeclareAsync().ConfigureAwait(false);

            if (!string.IsNullOrWhiteSpace(options.DeclareQueue))
            {
                await management.Queue(options.DeclareQueue).Type(QueueType.QUORUM).DeclareAsync().ConfigureAwait(false);
                await management.Binding()
                    .SourceExchange(options.Exchange)
                    .DestinationQueue(options.DeclareQueue)
                    .Key(options.EffectiveBindingKey)
                    .BindAsync()
                    .ConfigureAwait(false);
            }
        }
        else if (!string.IsNullOrWhiteSpace(options.Queue))
        {
            await management.Queue(options.Queue).Type(QueueType.QUORUM).DeclareAsync().ConfigureAwait(false);
        }
    }
}
