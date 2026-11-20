using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Infrastructure.Kafka;
using Npgsql;

namespace Ktm.DataSaver.Health;

/// <summary>
/// Боевая реализация сигналов: цикл, consumer, проверка брокера (запрос метаданных, предел 2 с, кэш 5 с) и короткая
/// проверка БД (<c>SELECT 1</c>, предел 2 с).
/// </summary>
public sealed class HealthSignals : IHealthSignals
{
    public static readonly TimeSpan ProbeTimeout = TimeSpan.FromSeconds(2);
    public static readonly TimeSpan BrokerCacheTtl = TimeSpan.FromSeconds(5);

    private readonly IngestionLoop _loop;
    private readonly KafkaMessageSource _consumer;
    private readonly KafkaFatalErrorHandler _fatalHandler;
    private readonly NpgsqlDataSource _dataSource;
    private readonly ILogger<HealthSignals> _logger;
    private readonly CachedAvailabilityProbe _broker;

    public HealthSignals(
        IngestionLoop loop,
        KafkaMessageSource consumer,
        KafkaFatalErrorHandler fatalHandler,
        NpgsqlDataSource dataSource,
        TimeProvider time,
        ILogger<HealthSignals> logger)
    {
        _loop = loop;
        _consumer = consumer;
        _fatalHandler = fatalHandler;
        _dataSource = dataSource;
        _logger = logger;
        // Текст исключения брокера содержит адреса: в лог попадает только тип.
        _broker = new CachedAvailabilityProbe(
            _ =>
            {
                _consumer.ProbeBroker(ProbeTimeout);
                return Task.CompletedTask;
            },
            time,
            BrokerCacheTtl,
            ProbeTimeout,
            ex => _logger.LogWarning("Readiness broker probe failed: {ErrorType}", ex.GetType().Name));
    }

    public DateTimeOffset LastHeartbeat => _loop.LastHeartbeat;

    public IngestionState State => _loop.State;

    public bool IsConsumerFatal => _fatalHandler.IsFatal;

    public bool HasAssignment => _consumer.HasAssignment;

    public ValueTask<bool> IsBrokerAvailableAsync(CancellationToken cancellationToken) => _broker.IsAvailableAsync(cancellationToken);

    public ValueTask<bool> IsDatabaseAvailableAsync(CancellationToken cancellationToken) =>
        DatabaseProbe.RunAsync(
            async token =>
            {
                await using var connection = await _dataSource.OpenConnectionAsync(token);
                await using var command = new NpgsqlCommand("SELECT 1", connection);
                await command.ExecuteScalarAsync(token);
            },
            ProbeTimeout,
            // Текст исключения может содержать хост БД: в лог попадает только тип.
            ex => _logger.LogWarning("Readiness database probe failed: {ErrorType}", ex.GetType().Name),
            cancellationToken);
}
