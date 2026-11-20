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

/// <summary>Порт чтения (4.1/4.2). Вызывается из одного потока цикла; обработчики ребаланса лишь копят изменения.</summary>
public interface IMessageSource
{
    /// <summary>Одно сообщение либо null по таймауту. На паузе партиций сообщений не возвращает, но продолжает опрос группы.</summary>
    ValueTask<ConsumedMessage?> ConsumeAsync(TimeSpan timeout, CancellationToken cancellationToken);

    /// <summary>Текущее назначение партиций.</summary>
    IReadOnlyCollection<TopicPartitionKey> Assignment { get; }

    /// <summary>Забирает назначения/отзывы партиций, случившиеся с прошлого вызова (и обнуляет накопленное).</summary>
    AssignmentChanges TakeAssignmentChanges();

    void Pause(IReadOnlyCollection<TopicPartitionKey> partitions);

    void Resume(IReadOnlyCollection<TopicPartitionKey> partitions);
}

/// <summary>Порт записи в журнал (5.x): одна транзакция на вызов, идемпотентно по ключу события. Сбой - исключение.</summary>
public interface IEventStore
{
    ValueTask WriteAsync(IReadOnlyList<ParsedEvent> events, CancellationToken cancellationToken);
}

/// <summary>Отбракованное сообщение с причиной.</summary>
public sealed record RejectedMessage(ConsumedMessage Message, RejectionInfo Info);

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

/// <summary>Порт публикации отбракованных (4.4): ждёт подтверждения брокера.</summary>
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

/// <summary>Заглушка: только считает оповещения; видимость обеспечивает Error-лог цикла.</summary>
public sealed class CountingAdminNotifier : IAdminNotifier
{
    private long _count;

    public long Count => Interlocked.Read(ref _count);

    public void NotifyFatalConfig(Exception error, int attempt) => Interlocked.Increment(ref _count);
}
