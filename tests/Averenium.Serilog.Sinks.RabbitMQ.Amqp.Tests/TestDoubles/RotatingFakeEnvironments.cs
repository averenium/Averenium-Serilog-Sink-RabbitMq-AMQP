using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;
using RabbitMQ.AMQP.Client;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.TestDoubles;

/// <summary>
/// Environment source whose "credentials" are a version number: <see cref="Rotate"/> bumps it and raises
/// <see cref="Changed"/>; every <see cref="Create"/> hands out a new <see cref="FakeEnvironment"/>.
/// </summary>
internal sealed class RotatingFakeEnvironments : IEnvironmentSource
{
    private readonly Lock _gate = new();
    private readonly Dictionary<IEnvironment, int> _versions = [];
    private int _version;

    public event Action? Changed;

    public List<FakeEnvironment> Created { get; } = [];

    /// <summary>Applied to every new environment before it is handed out (e.g. to script failures).</summary>
    public Action<FakeEnvironment>? OnCreated { get; set; }

    public IEnvironment Create()
    {
        var broker = new FakeEnvironment();
        OnCreated?.Invoke(broker);
        lock (_gate)
        {
            Created.Add(broker);
            _versions[broker.Environment] = _version;
        }

        return broker.Environment;
    }

    public bool IsOutdated(IEnvironment environment)
    {
        lock (_gate)
        {
            return _versions.TryGetValue(environment, out var version) && version != _version;
        }
    }

    public void Rotate()
    {
        lock (_gate)
        {
            _version++;
        }

        Changed?.Invoke();
    }

    public int CreatedCount
    {
        get
        {
            lock (_gate)
            {
                return Created.Count;
            }
        }
    }
}
