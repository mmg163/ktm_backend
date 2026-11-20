using Ktm.DataSaver.Core.Batching;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Pipeline;

namespace Ktm.DataSaver.Core.Ingestion;

/// <summary>Изменения назначения партиций, накопленные источником с прошлого вызова.</summary>
public sealed record AssignmentChanges(
    IReadOnlyCollection<TopicPartitionKey> Assigned,
    IReadOnlyCollection<TopicPartitionKey> Revoked)
{
    public static AssignmentChanges None { get; } = new([], []);

    public bool IsEmpty => Assigned.Count == 0 && Revoked.Count == 0;
}

/// <summary>
/// Порт чтения (4.1/4.2). Вызывается только из потока цикла. Обработчики ребаланса librdkafka выполняются внутри
/// <see cref="ConsumeAsync"/> и только накапливают изменения назначения; цикл забирает их через
/// <see cref="TakeAssignmentChanges"/> (только из потока цикла).
/// Нефатальные ошибки брокера адаптер поглощает внутри <see cref="ConsumeAsync"/>; фатальные - через IProcessTerminator (4.5).
/// </summary>
public interface IMessageSource
{
    /// <summary>
    /// Блокируется до сообщения или таймаута. Null означает «нет сообщений за таймаут» (цикл подстрахуется от возврата
    /// раньше таймаута паузой). На паузе партиций сообщений не возвращает, но продолжает опрос группы.
    /// </summary>
    ValueTask<ConsumedMessage?> ConsumeAsync(TimeSpan timeout, CancellationToken cancellationToken);

    /// <summary>Текущее назначение партиций.</summary>
    IReadOnlyCollection<TopicPartitionKey> Assignment { get; }

    /// <summary>Забирает назначения/отзывы партиций, случившиеся с прошлого вызова (и обнуляет накопленное).</summary>
    AssignmentChanges TakeAssignmentChanges();

    /// <summary>Ставит партиции на паузу. Принимает любые партиции: неизвестные и отозванные молча игнорирует, не бросает.</summary>
    void Pause(IReadOnlyCollection<TopicPartitionKey> partitions);

    /// <summary>Снимает паузу. Контракт тот же, что у <see cref="Pause"/>: неизвестные и отозванные партиции игнорируются.</summary>
    void Resume(IReadOnlyCollection<TopicPartitionKey> partitions);
}

/// <summary>
/// Порт записи в журнал (5.x): одна транзакция на вызов. Исключение означает, что транзакция откатана целиком;
/// запись идемпотентна по ключу события, поэтому повтор той же пачки безопасен.
/// </summary>
public interface IEventStore
{
    ValueTask WriteAsync(IReadOnlyList<ParsedEvent> events, CancellationToken cancellationToken);
}

/// <summary>Отбракованное сообщение с причиной; <c>Origin</c> - null, если формат сообщения не разобран.</summary>
public sealed record RejectedMessage(ConsumedMessage Message, RejectionInfo Info, EventOrigin? Origin = null);

/// <summary>Итог публикации отбракованных: отказ Kafka - результат, а не исключение (4.4).</summary>
public sealed record RejectionPublishResult
{
    private RejectionPublishResult() { }

    public static RejectionPublishResult Success { get; } = new();

    public string? Error { get; private init; }

    public bool IsSuccess => Error is null;

    public static RejectionPublishResult Failure(string error)
    {
        ArgumentException.ThrowIfNullOrEmpty(error);
        return new RejectionPublishResult { Error = error };
    }
}

/// <summary>
/// Порт публикации отбракованных (4.4): ждёт подтверждения брокера. Реализация обязана иметь собственный таймаут
/// ожидания подтверждений (отказ - <see cref="RejectionPublishResult.Failure"/>); исключение не считается отбраковкой.
/// </summary>
public interface IRejectionPublisher
{
    ValueTask<RejectionPublishResult> PublishAsync(IReadOnlyList<RejectedMessage> rejected, CancellationToken cancellationToken);
}

/// <summary>Порт синхронного commit (4.3): offsets уже со смещением +1. Ошибка (потеря партиции) - исключение.</summary>
public interface IOffsetCommitter
{
    ValueTask CommitAsync(IReadOnlyList<PartitionOffset> offsets, CancellationToken cancellationToken);
}

/// <summary>Порт оповещения администратора о Fatal-config (заглушка до появления канала оповещений).</summary>
public interface IAdminNotifier
{
    void NotifyFatalConfig(Exception error, int attempt);
}
