using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Infrastructure.Postgres;

namespace Ktm.DataSaver.Infrastructure.Observability;

/// <summary>
/// Декоратор порта записи: порт Core возвращает <c>ValueTask</c> без числа, поэтому метрики берут число вставленных
/// строк из <see cref="IBatchWriter"/>. Дубли = размер пачки - вставленные. Сбой записи считается повтором и пробрасывается.
/// </summary>
public sealed class MeteredEventStore(IBatchWriter inner, DataSaverMetrics metrics, TimeProvider? time = null) : IEventStore
{
    private readonly TimeProvider _time = time ?? TimeProvider.System;

    public async ValueTask WriteAsync(IReadOnlyList<ParsedEvent> events, CancellationToken cancellationToken)
    {
        var started = _time.GetTimestamp();
        EventWriteResult result;
        try
        {
            result = await inner.WriteBatchAsync(events, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            metrics.Retry("write");
            throw;
        }

        metrics.BatchWritten(events.Count, result.EventsInserted, result.CorrectionsInserted, _time.GetElapsedTime(started));
    }
}
