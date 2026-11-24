using System.Diagnostics.Metrics;
using Ktm.DataSaver.Hosting;
using Ktm.DataSaver.Infrastructure.Kafka;
using Ktm.DataSaver.Infrastructure.Observability;
using Ktm.DataSaver.Tests.Ingestion;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Testing;

namespace Ktm.DataSaver.Tests.Host;

public class DataSaverWorkerTests
{
    private sealed class RecordingTerminator : IProcessTerminator
    {
        public List<int> ExitCodes { get; } = [];

        public void Terminate(int exitCode, string reason) => ExitCodes.Add(exitCode);
    }

    private sealed class FakeConsumer : IConsumerLifecycle
    {
        public int Starts { get; private set; }

        public int Disposals { get; private set; }

        public void Start() => Starts++;

        public void Dispose() => Disposals++;
    }

    private static (DataSaverWorker Worker, RecordingTerminator Terminator, FakeLogger<DataSaverWorker> Logger) Create(Harness h, Meter meter)
    {
        var terminator = new RecordingTerminator();
        var logger = new FakeLogger<DataSaverWorker>();
        // Клиенты Kafka создаются без соединения с брокером: цикл работает на fake-порте, им worker лишь закрывает ресурсы.
        var options = new KafkaOptions { BootstrapServers = "localhost:1" };
        var throttle = new LogThrottle(TimeProvider.System, LogThrottle.DefaultInterval);
        var handler = new KafkaFatalErrorHandler(terminator, new FakeLogger<KafkaFatalErrorHandler>(), throttle);
        var worker = new DataSaverWorker(
            h.Loop,
            new FakeConsumer(),
            new KafkaRejectionPublisher(options, handler, new FakeLogger<KafkaRejectionPublisher>(), throttle),
            new DataSaverMetrics(meter),
            terminator,
            logger);
        return (worker, terminator, logger);
    }

    [Fact]
    public async Task LoopFailureDuringShutdown_IsLoggedCriticalAndTerminatesWithCode1()
    {
        var h = new Harness();
        // Остановка и сбой цикла одновременно: раньше фильтр по токену остановки гасил такую ошибку.
        h.Source.OnIdle = _ =>
        {
            h.Stop.Cancel();
            throw new InvalidOperationException("loop defect");
        };
        using var meter = new Meter("test-" + Guid.NewGuid());
        var (worker, terminator, logger) = Create(h, meter);

        await worker.StartAsync(h.Stop.Token);
        await worker.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal([DataSaverWorker.LoopFailureExitCode], terminator.ExitCodes);
        var critical = Assert.Single(logger.Collector.GetSnapshot(), r => r.Level == LogLevel.Critical);
        Assert.IsType<InvalidOperationException>(critical.Exception);
    }

    [Fact]
    public async Task OrdinaryShutdown_DoesNotTerminateTheProcess()
    {
        var h = new Harness();
        h.StopWhenQueueDrained();
        using var meter = new Meter("test-" + Guid.NewGuid());
        var (worker, terminator, logger) = Create(h, meter);

        await worker.StartAsync(h.Stop.Token);
        await worker.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Empty(terminator.ExitCodes);
        Assert.DoesNotContain(logger.Collector.GetSnapshot(), r => r.Level >= LogLevel.Error);
    }
}
