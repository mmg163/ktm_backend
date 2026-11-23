using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Core.Pipeline;
using Microsoft.Extensions.Logging;

namespace Ktm.DataSaver.Infrastructure.Observability;

/// <summary>
/// Декоратор публикации отбракованных (6.5): после подтверждения брокера пишет лог с координатами сообщения
/// (именованные Topic, Partition, Offset, Source, SourceEventId, причина) и считает отбраковку: по формату - Error и
/// <c>messages.rejected{reason}</c>; по правилу - Warning, <c>messages.rejected{reason=rule_matched}</c> и <c>rules.rejected</c>
/// (счётчик по правилу требует Origin; без него пишется только лог и <c>messages.rejected</c>). Тело сообщения в лог не попадает.
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
            if (item.Info.Kind == RejectionKind.Rule)
            {
                // Штатная работа фильтра: Warning, без тела и без оповещения администратора. Тег reason метрики фиксирован
                // (rule_matched), чтобы кардинальность не задавалась администратором; rule_id - из снимка правил.
                metrics.MessageRejected("rule_matched");
                if (item.Origin is not null && item.Info.RuleId is not null)
                {
                    metrics.RuleRejected(item.Origin.Source, item.Info.RuleId);
                }

                logger.LogWarning(
                    "Message rejected by rule: {RuleId} {RuleVersion} {ReasonCode} {Source} {SourceEventId} {Topic} {Partition} {Offset}",
                    item.Info.RuleId, item.Info.RuleVersion, item.Info.Reason, item.Origin?.Source, item.Origin?.SourceEventId,
                    item.Message.Topic, item.Message.Partition, item.Message.Offset);
                continue;
            }

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
