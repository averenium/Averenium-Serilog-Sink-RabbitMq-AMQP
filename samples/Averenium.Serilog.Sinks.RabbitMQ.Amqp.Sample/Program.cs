// Start a broker first:  docker compose up -d   (from the repository root)
//
//   dotnet run --project samples/Averenium.Serilog.Sinks.RabbitMQ.Amqp.Sample            → configured from appsettings.json
//   dotnet run --project samples/Averenium.Serilog.Sinks.RabbitMQ.Amqp.Sample -- code    → configured in code
//
// Watch the messages arrive in the management UI: http://localhost:15672 (sample / sample), queue "logs.sample".
// Stop the broker while the sample runs (docker compose stop rabbitmq) to see buffering; events that wait longer
// than fallbackAfter (30 s) are written to the console fallback instead.

using System.Diagnostics;
using System.Globalization;
using Microsoft.Extensions.Configuration;
using Serilog;

var logger = args.Contains("code") ? ConfigureInCode() : ConfigureFromAppSettings();

Console.WriteLine("Logging one event per second; press Ctrl+C to stop.");
using var stop = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    stop.Cancel();
};

var n = 0;
while (!stop.IsCancellationRequested)
{
    // Inside an activity the AMQP correlation-id is the trace id (CorrelationIdFromTraceId, on by default).
    using (new Activity("sample-iteration").Start())
    {
        logger.Information("Sample event {Number} at {Time:O}", ++n, DateTimeOffset.UtcNow);
    }

    try
    {
        await Task.Delay(TimeSpan.FromSeconds(1), stop.Token);
    }
    catch (OperationCanceledException)
    {
        break;
    }
}

// Flushes what is still buffered (bounded by ShutdownTimeout); anything undeliverable goes to the fallback.
await logger.DisposeAsync();

static Serilog.Core.Logger ConfigureFromAppSettings()
{
    var configuration = new ConfigurationBuilder()
        .SetBasePath(AppContext.BaseDirectory)
        .AddJsonFile("appsettings.json")
        .AddEnvironmentVariables()
        .Build();

    return new LoggerConfiguration()
        .ReadFrom.Configuration(configuration)
        .CreateLogger();
}

static Serilog.Core.Logger ConfigureInCode() =>
    new LoggerConfiguration()
        .MinimumLevel.Information()
        .WriteTo.FallbackChain(
            wt => wt.RabbitMQAmqp(options =>
            {
                options.Hostnames.Add("localhost:5672");
                options.Username = "sample";
                options.Password = "sample";
                options.Exchange = "logs";
                options.RoutingKey = "sample";
                options.DeclareTopology = true;
                options.DeclareQueue = "logs.sample";
                options.FallbackAfter = TimeSpan.FromSeconds(30);
            }),
            wt => wt.Console(formatProvider: CultureInfo.InvariantCulture))
        .CreateLogger();
