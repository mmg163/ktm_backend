using Ktm.DataSaver.Application.Ingestion;
using Ktm.DataSaver.Application.Resilience;
using Ktm.DataSaver.Infrastructure.Kafka;
using Ktm.DataSaver.Infrastructure.Observability;

namespace Ktm.HostedServices.Hosting;

/// <summary>
/// Запускает <see cref="IngestionLoop"/> и владеет порядком остановки (D-8): цикл дописывает пачку и делает commit,
/// затем закрывается consumer (выход из группы), затем сбрасываются оба producer (rejected и dlq). <see cref="BackgroundService.StopAsync"/>
/// только сигнализирует токеном и ждёт <see cref="ExecuteAsync"/> в пределах <c>HostOptions.ShutdownTimeout</c>.
/// Ошибка цикла не глотается: критический лог, ненулевой код выхода и остановка host.
/// </summary>
public sealed class DataSaverWorker(
    IngestionLoop loop,
    IConsumerLifecycle consumer,
    KafkaRejectionPublisher producer,
    KafkaDeadLetterPublisher deadLetterProducer,
    DlqBreaker breaker,
    DataSaverMetrics metrics,
    IProcessTerminator terminator,
    ILogger<DataSaverWorker> logger,
    RuleRefreshService? rules = null) : BackgroundService
{
    public const int LoopFailureExitCode = 1;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        metrics.ObserveLoop(loop);
        metrics.ObserveBreaker(breaker);

        // Consume блокирующий: уходим с потока запуска host, чтобы не задерживать старт остальных служб.
        await Task.Yield();
        try
        {
            if (rules is not null)
            {
                // Первая загрузка правил до приёма: Consume не вызывается, пока набор не загружен (ADR-0005 п. 2.10).
                await rules.WaitForFirstLoadAsync(stoppingToken);
            }

            // Вход в группу только теперь: до первой загрузки правил consumer не создаётся и не подписывается.
            consumer.Start();
            logger.LogInformation("Data Saver ingestion loop started");
            await loop.RunAsync(stoppingToken);
            logger.LogInformation("Data Saver ingestion loop finished");
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Штатная остановка по токену host.
        }
        catch (Exception ex)
        {
            // Ошибка цикла и во время остановки - сбой (commit/запись могли не завершиться): не глотаем.
            logger.LogCritical(ex, "Ingestion loop failed; the process is terminating");
            terminator.Terminate(LoopFailureExitCode, "Ingestion loop failed: " + ex.GetType().Name);
        }
        finally
        {
            // Цикл вышел: нет ни Consume, ни Commit, поэтому Close() не гоняется с ними. Порядок: consumer, затем оба producer (rejected и dlq).
            try
            {
                consumer.Dispose();
            }
            finally
            {
                try
                {
                    producer.Dispose();
                }
                finally
                {
                    deadLetterProducer.Dispose();
                }
            }
        }
    }
}
