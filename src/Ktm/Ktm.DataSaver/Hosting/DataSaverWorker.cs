using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Infrastructure.Kafka;
using Ktm.DataSaver.Infrastructure.Observability;

namespace Ktm.DataSaver.Hosting;

/// <summary>
/// Запускает <see cref="IngestionLoop"/> и владеет порядком остановки (D-8): цикл дописывает пачку и делает commit,
/// затем закрывается consumer (выход из группы), затем сбрасывается producer. <see cref="BackgroundService.StopAsync"/>
/// только сигнализирует токеном и ждёт <see cref="ExecuteAsync"/> в пределах <c>HostOptions.ShutdownTimeout</c>.
/// Ошибка цикла не глотается: критический лог, ненулевой код выхода и остановка host.
/// </summary>
public sealed class DataSaverWorker(
    IngestionLoop loop,
    KafkaMessageSource consumer,
    KafkaRejectionPublisher producer,
    DataSaverMetrics metrics,
    IProcessTerminator terminator,
    ILogger<DataSaverWorker> logger) : BackgroundService
{
    public const int LoopFailureExitCode = 1;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        metrics.ObserveLoop(loop);

        // Consume блокирующий: уходим с потока запуска host, чтобы не задерживать старт остальных служб.
        await Task.Yield();
        try
        {
            logger.LogInformation("Data Saver ingestion loop started");
            await loop.RunAsync(stoppingToken);
            logger.LogInformation("Data Saver ingestion loop finished");
        }
        catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
        {
            logger.LogCritical(ex, "Ingestion loop failed; the process is terminating");
            terminator.Terminate(LoopFailureExitCode, "Ingestion loop failed: " + ex.GetType().Name);
        }
        finally
        {
            // Цикл вышел: нет ни Consume, ни Commit, поэтому Close() не гоняется с ними. Порядок: consumer, затем producer.
            consumer.Dispose();
            producer.Dispose();
        }
    }
}
