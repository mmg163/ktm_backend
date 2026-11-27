using Ktm.DataSaver.Application.Batching;
using Ktm.DataSaver.Application.Model;
using Ktm.DataSaver.Application.Pipeline;
using Ktm.DataSaver.Application.Resilience;

namespace Ktm.DataSaver.Application.Ingestion;

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
/// <c>receivedAt</c> - момент получения пачки, один на пачку и неизменный при повторах (store время не читает).
/// </summary>
public interface IEventStore
{
    ValueTask WriteAsync(IReadOnlyList<ParsedEvent> events, DateTimeOffset receivedAt, CancellationToken cancellationToken);
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

/// <summary>Сообщение, которое не удалось обработать (ADR-0006): исходное сообщение, происхождение (если разобрано) и причина.</summary>
public sealed record DeadLetteredMessage(ConsumedMessage Message, EventOrigin? Origin, DeadLetterInfo Info);

/// <summary>Итог публикации в dlq: отказ Kafka - результат, а не исключение.</summary>
public sealed record DeadLetterPublishResult
{
    private DeadLetterPublishResult() { }

    public static DeadLetterPublishResult Success { get; } = new();

    public string? Error { get; private init; }

    public bool IsSuccess => Error is null;

    public static DeadLetterPublishResult Failure(string error)
    {
        ArgumentException.ThrowIfNullOrEmpty(error);
        return new DeadLetterPublishResult { Error = error };
    }
}

/// <summary>
/// Порт публикации в dlq (ADR-0006 п. 2.4, 2.5): возвращает <see cref="DeadLetterPublishResult.Success"/> только после
/// подтверждения брокера (цикл коммитит offset сообщения сразу после этого, поэтому до подтверждения коммит недопустим).
/// Реализация обязана иметь собственный таймаут ожидания подтверждений; отказ - <see cref="DeadLetterPublishResult.Failure"/>,
/// исключение порта цикл трактует как отказ. Ни тела, ни текст исключения в лог и заголовки не попадают.
/// </summary>
public interface IDeadLetterPublisher
{
    ValueTask<DeadLetterPublishResult> PublishAsync(IReadOnlyList<DeadLetteredMessage> messages, CancellationToken cancellationToken);
}

/// <summary>Порт наблюдаемости цикла (метрики): реализует Infrastructure, Core метрик не знает.</summary>
public interface IIngestionObserver
{
    /// <summary>Неудачная попытка стадии: <c>pipeline</c>, <c>write</c>, <c>publish</c>, <c>dlq_publish</c>.</summary>
    void Retry(string stage);

    /// <summary>Запущена изоляция ядовитого сообщения при записи.</summary>
    void WriteIsolation();
}

/// <summary>Порт синхронного commit (4.3): offsets уже со смещением +1. Ошибка (потеря партиции) - исключение.</summary>
public interface IOffsetCommitter
{
    ValueTask CommitAsync(IReadOnlyList<PartitionOffset> offsets, CancellationToken cancellationToken);
}

/// <summary>Порт оповещения администратора о Fatal-config и об открытом предохранителе dlq (заглушка до появления канала оповещений).</summary>
public interface IAdminNotifier
{
    void NotifyFatalConfig(Exception error, int attempt);

    /// <summary>
    /// Предохранитель dlq открыт: сообщения повторяются без предела, администратору нужно вмешаться (ошибка данных или кода,
    /// а не конфигурации). Вызывается на каждой неудачной попытке при открытом предохранителе.
    /// </summary>
    void NotifyDeadLetterBreakerOpen(Exception error, int attempt);
}
