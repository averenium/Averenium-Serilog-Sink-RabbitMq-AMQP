using RabbitMQ.AMQP.Client;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;

/// <summary>An environment given from outside (tests, advanced scenarios): used as is, never replaced.</summary>
internal sealed class FixedEnvironmentSource(IEnvironment environment) : IEnvironmentSource
{
    public IEnvironment Create() => environment;

    public bool IsOutdated(IEnvironment environment) => false;

    public event Action? Changed
    {
        add { }
        remove { }
    }
}
