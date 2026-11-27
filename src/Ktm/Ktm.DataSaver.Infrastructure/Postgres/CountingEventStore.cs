using Ktm.DataSaver.Application.Model;
using Ktm.DataSaver.Application.Reprocessing;

namespace Ktm.DataSaver.Infrastructure.Postgres;

/// <summary>
/// Запись для задачи повторной обработки: тот же <see cref="IBatchWriter"/>, но с числом реально добавленных событий
/// (разница с размером пачки - дубли, отсечённые уникальным ключом БД). Метрики рабочего потока не затрагивает.
/// Порт <c>IEventStore</c> числа не возвращает и не меняется.
/// </summary>
public sealed class CountingEventStore(IBatchWriter inner) : IReplayEventWriter
{
    public async ValueTask<int> WriteAsync(IReadOnlyList<ParsedEvent> events, DateTimeOffset receivedAt, CancellationToken cancellationToken)
    {
        var result = await inner.WriteBatchAsync(events, receivedAt, cancellationToken);
        return result.EventsInserted;
    }
}
