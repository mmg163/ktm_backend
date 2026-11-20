using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Infrastructure.Kafka;
using Npgsql;

namespace Ktm.DataSaver.Health;

/// <summary>Боевая реализация сигналов: цикл, consumer и короткая проверка БД (<c>SELECT 1</c>, предел 2 с).</summary>
public sealed class HealthSignals(
    IngestionLoop loop,
    KafkaMessageSource consumer,
    KafkaFatalErrorHandler fatalHandler,
    NpgsqlDataSource dataSource,
    ILogger<HealthSignals> logger) : IHealthSignals
{
    private static readonly TimeSpan DatabaseProbeTimeout = TimeSpan.FromSeconds(2);

    public DateTimeOffset LastHeartbeat => loop.LastHeartbeat;

    public IngestionState State => loop.State;

    public bool IsConsumerFatal => fatalHandler.IsFatal;

    public bool HasAssignment
    {
        get
        {
            try
            {
                return consumer.Assignment.Count > 0;
            }
            catch (ObjectDisposedException)
            {
                // Остановка: consumer уже закрыт.
                return false;
            }
        }
    }

    public async ValueTask<bool> IsDatabaseAvailableAsync(CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(DatabaseProbeTimeout);
        try
        {
            await using var connection = await dataSource.OpenConnectionAsync(timeout.Token);
            await using var command = new NpgsqlCommand("SELECT 1", connection);
            await command.ExecuteScalarAsync(timeout.Token);
            return true;
        }
        catch (Exception ex) when (ex is NpgsqlException or OperationCanceledException or InvalidOperationException or IOException)
        {
            // Текст исключения может содержать хост БД: в ответ и в лог попадает только тип.
            logger.LogWarning("Readiness database probe failed: {ErrorType}", ex.GetType().Name);
            return false;
        }
    }
}
