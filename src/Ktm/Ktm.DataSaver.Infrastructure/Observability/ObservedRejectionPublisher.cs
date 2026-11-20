using Ktm.DataSaver.Core.Ingestion;
using Microsoft.Extensions.Logging;

namespace Ktm.DataSaver.Infrastructure.Observability;

/// <summary>
/// Декоратор публикации отбракованных (6.5): после подтверждения брокера пишет Error-лог с координатами сообщения
/// (именованные Topic, Partition, Offset, Source, SourceEventId, причина) и считает отбраковку по причинам. Тело сообщения в лог не попадает.
/// Неудачная публикация считается повтором.
/// </summary>
public sealed class ObservedRejectionPublisher(IRejectionPublisher inner, DataSaverMetrics metrics, ILogger<ObservedRejectionPublisher> logger) : IRejectionPublisher
{
    public async ValueTask<RejectionPublishResult> PublishAsync(IReadOnlyList<RejectedMessage> rejected, CancellationToken cancellationToken)
    {
        var result = await inner.PublishAsync(rejected, cancellationToken);
        if (!result.IsSuccess)
        {
            metrics.Retry("publish");
            return result;
        }

        foreach (var item in rejected)
        {
            metrics.MessageRejected(item.Info.Reason);
            // Source и SourceEventId - из конверта разобранного события; для неразобранного сообщения null, тело не логируется.
            logger.LogError(
                "Message rejected: {Topic} {Partition} {Offset} {Source} {SourceEventId} {Kind} {Reason} {RuleId}",
                item.Message.Topic, item.Message.Partition, item.Message.Offset, item.Origin?.Source, item.Origin?.SourceEventId,
                item.Info.Kind, item.Info.Reason, item.Info.RuleId);
        }

        return result;
    }
}
