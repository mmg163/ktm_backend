using Ktm.DataSaver.Application.Ingestion;
using Ktm.DataSaver.Application.Rules;
using Ktm.HostedServices.Configuration;
using Microsoft.Extensions.Options;

namespace Ktm.HostedServices.Health;

/// <summary>Сигналы состояния сервиса для health-проверок; реализация изолирует вычислитель от Kafka, Npgsql и цикла.</summary>
public interface IHealthSignals
{
    /// <summary>Момент последней отметки цикла приёма.</summary>
    DateTimeOffset LastHeartbeat { get; }

    IngestionState State { get; }

    /// <summary>Получена фатальная ошибка клиента Kafka.</summary>
    bool IsConsumerFatal { get; }

    /// <summary>Consumer подписан и получил назначение партиций.</summary>
    bool HasAssignment { get; }

    /// <summary>Брокер Kafka отвечает на запрос метаданных (активная проверка с пределом и кэшем; наличие назначения у клиента связь не доказывает).</summary>
    ValueTask<bool> IsBrokerAvailableAsync(CancellationToken cancellationToken);

    ValueTask<bool> IsDatabaseAvailableAsync(CancellationToken cancellationToken);
}

/// <summary>Результат проверки. <c>Reason</c> - фиксированный код без строк подключения и текстов исключений.</summary>
public sealed record HealthResult(bool IsHealthy, string? Reason)
{
    public string Status => IsHealthy ? "ok" : "unhealthy";
}

/// <summary>Правила D-10 ADR-0002. Недоступность БД liveness не роняет.</summary>
public sealed class HealthEvaluator(
    IHealthSignals signals,
    IOptions<HealthOptions> options,
    TimeProvider time,
    RuleSnapshotHolder? rules = null)
{
    public HealthResult Live()
    {
        if (signals.IsConsumerFatal)
        {
            return new HealthResult(false, "consumer_fatal_error");
        }

        // Цикл приёма ждёт первую загрузку правил (БД недоступна): отметок нет по замыслу, перезапуск это не лечит.
        if (rules is { HasLoaded: false })
        {
            return new HealthResult(true, null);
        }

        var stale = TimeSpan.FromSeconds(options.Value.LiveStaleSeconds);
        return time.GetUtcNow() - signals.LastHeartbeat > stale
            ? new HealthResult(false, "loop_stalled")
            : new HealthResult(true, null);
    }

    public async ValueTask<HealthResult> ReadyAsync(CancellationToken cancellationToken)
    {
        // Правила включены, но первая загрузка ещё не выполнена (пустая таблица - выполненная): приём не начат.
        if (rules is { HasLoaded: false })
        {
            return new HealthResult(false, "rules_not_loaded");
        }

        if (!signals.HasAssignment)
        {
            return new HealthResult(false, "no_partitions_assigned");
        }

        if (signals.State == IngestionState.Degraded)
        {
            return new HealthResult(false, "ingestion_degraded");
        }

        if (!await signals.IsBrokerAvailableAsync(cancellationToken))
        {
            return new HealthResult(false, "broker_unavailable");
        }

        return await signals.IsDatabaseAvailableAsync(cancellationToken)
            ? new HealthResult(true, null)
            : new HealthResult(false, "database_unavailable");
    }
}
