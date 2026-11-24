using Ktm.DataSaver.Core.Ingestion;
using Microsoft.Extensions.Logging;

namespace Ktm.DataSaver.Infrastructure.Observability;

/// <summary>
/// Декоратор публикации в dlq (ADR-0006 п. 2.9): после подтверждения брокера считает <c>messages.dead_lettered{reason,stage}</c> и пишет
/// Error-лог на каждое сообщение с координатами, причиной, стадией и типом исключения. Тела сообщений, значений и текста исключения
/// в записи нет. Неудачная публикация считается повтором <c>dlq_publish</c>.
/// </summary>
public sealed class ObservedDeadLetterPublisher(
    IDeadLetterPublisher inner,
    DataSaverMetrics metrics,
    ILogger<ObservedDeadLetterPublisher> logger) : IDeadLetterPublisher
{
    public async ValueTask<DeadLetterPublishResult> PublishAsync(IReadOnlyList<DeadLetteredMessage> messages, CancellationToken cancellationToken)
    {
        var result = await inner.PublishAsync(messages, cancellationToken);
        if (!result.IsSuccess)
        {
            metrics.Retry("dlq_publish");
            return result;
        }

        foreach (var item in messages)
        {
            var info = item.Info;
            metrics.MessageDeadLettered(info.Reason, info.Stage);
            // Source и SourceEventId - из конверта разобранного события; для неразобранного сообщения null.
            logger.LogError(
                "Message sent to the dead letter topic: {Topic} {Partition} {Offset} {Source} {SourceEventId} {Reason} {Stage} {ExceptionType} {Attempts} {SqlState} {ConstraintName} {TableName}",
                item.Message.Topic, item.Message.Partition, item.Message.Offset, item.Origin?.Source, item.Origin?.SourceEventId,
                info.Reason, info.Stage, info.ExceptionType, info.Attempts, info.SqlState, info.ConstraintName, info.TableName);
        }

        return result;
    }
}
