using Ktm.DataSaver.Application.Model;

namespace Ktm.DataSaver.Application.Reprocessing;

/// <summary>Партиция источника: читается диапазон <c>[StartOffset, EndOffset)</c>; <c>EndOffset</c> снят при открытии (снимок).</summary>
public sealed record ReplayPartition(string Topic, int Partition, long StartOffset, long EndOffset)
{
    /// <summary>Сколько сообщений в диапазоне (оценка: смещения могут иметь пропуски).</summary>
    public long Count => Math.Max(0, EndOffset - StartOffset);
}

/// <summary>
/// Прочитанное сообщение. <c>Message</c> несёт исходные ключ и тело и исходные координаты (из <c>ktm-source-*</c> для
/// <c>dlq</c>/<c>rejected</c>; если заголовков нет - фактические, тогда <c>CoordinatesFromHeaders = false</c>).
/// <c>ActualTopic</c>/<c>ActualPartition</c>/<c>ActualOffset</c> - где сообщение лежит в читаемом топике (для прогресса).
/// </summary>
public sealed record ReplayMessage(
    ConsumedMessage Message,
    string? Reason,
    string? Stage,
    string ActualTopic,
    int ActualPartition,
    long ActualOffset,
    bool CoordinatesFromHeaders);

/// <summary>Чтение открытого источника: отдельный consumer без группы и без commit.</summary>
public interface IReplayReader : IAsyncDisposable
{
    /// <summary>Читаемые партиции с границами, снятыми при открытии.</summary>
    IReadOnlyList<ReplayPartition> Partitions { get; }

    /// <summary>Следующая пачка не больше <paramref name="maxCount"/> сообщений; пустая пачка - всё прочитано.</summary>
    /// <exception cref="ReplaySourceException">Источник недоступен.</exception>
    ValueTask<IReadOnlyList<ReplayMessage>> ReadBatchAsync(int maxCount, CancellationToken cancellationToken);
}

/// <summary>Итог открытия источника: читатель либо ошибки аргументов (период раньше retention и т.п.), но не оба.</summary>
public sealed record ReplayOpenResult
{
    private ReplayOpenResult() { }

    public IReplayReader? Reader { get; private init; }

    public IReadOnlyList<ReprocessValidationError> Errors { get; private init; } = [];

    public bool IsSuccess => Reader is not null;

    public static ReplayOpenResult Opened(IReplayReader reader)
    {
        ArgumentNullException.ThrowIfNull(reader);
        return new ReplayOpenResult { Reader = reader };
    }

    public static ReplayOpenResult Invalid(IReadOnlyList<ReprocessValidationError> errors)
    {
        ArgumentNullException.ThrowIfNull(errors);
        if (errors.Count == 0)
        {
            throw new ArgumentException("At least one error is required.", nameof(errors));
        }

        return new ReplayOpenResult { Errors = errors };
    }
}

/// <summary>
/// Порт источника повторной обработки (ADR-0006 п. 2.6, 2.7): ничего не коммитит, не состоит в рабочей группе
/// и не публикует в топики. Открытие снимает границы чтения и проверяет период относительно retention.
/// </summary>
public interface IReplaySource
{
    /// <exception cref="ReplaySourceException">Источник недоступен (брокер, метаданные).</exception>
    ValueTask<ReplayOpenResult> OpenAsync(ReprocessRequest request, CancellationToken cancellationToken);
}

/// <summary>Источник недоступен. Текст исходного исключения не копируется: он может нести данные сообщения.</summary>
public sealed class ReplaySourceException(string reason) : Exception(reason);

/// <summary>
/// Порт записи для задачи повторной обработки: возвращает число реально добавленных событий (дубли не считаются).
/// Исключение означает откат транзакции целиком; повтор безопасен (идемпотентность по ключу в БД).
/// </summary>
public interface IReplayEventWriter
{
    ValueTask<int> WriteAsync(IReadOnlyList<ParsedEvent> events, DateTimeOffset receivedAt, CancellationToken cancellationToken);
}
