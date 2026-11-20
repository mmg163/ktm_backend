using Ktm.DataSaver.Core.Ingestion;
using Microsoft.Extensions.Logging;

namespace Ktm.DataSaver.Infrastructure.Observability;

/// <summary>
/// Декоратор публикации отбракованных (6.5): после подтверждения брокера пишет Error-лог с координатами сообщения
/// (топик, партиция, offset, причина) и считает отбраковку по причинам. Тело сообщения в лог не попадает.
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
            logger.LogError(
                "Message rejected: topic={Topic} partition={Partition} offset={Offset} kind={Kind} reason={Reason} ruleId={RuleId}",
                item.Message.Topic, item.Message.Partition, item.Message.Offset, item.Info.Kind, item.Info.Reason, item.Info.RuleId);
        }

        return result;
    }
}
