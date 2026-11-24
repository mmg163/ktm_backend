using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Pipeline;
using Ktm.DataSaver.Core.Rules;
using Ktm.DataSaver.Infrastructure.Kafka;
using Microsoft.Extensions.Logging;

namespace Ktm.DataSaver.Infrastructure.Observability;

/// <summary>
/// Декоратор публикации отбракованных (6.5): после подтверждения брокера пишет лог с координатами сообщения
/// (именованные Topic, Partition, Offset, Source, SourceEventId, причина) и считает отбраковку: по формату - Error и
/// <c>messages.rejected{reason}</c>; по правилу - Warning, <c>messages.rejected{reason=rule_matched}</c> и <c>rules.rejected</c>
/// (счётчик по правилу требует Origin; без него пишется только лог и <c>messages.rejected</c>). Тело сообщения в лог не попадает.
/// Отбраковка по таймауту regex (<c>rule_eval_timeout</c>) считается как правилом, плюс <c>rules.eval_errors{kind=regex_timeout, rule_id}</c>;
/// вместо обычного Warning - Warning о таймауте не чаще раза в 10 минут на правило. Неудачная публикация считается повтором.
/// </summary>
public sealed class ObservedRejectionPublisher : IRejectionPublisher
{
    /// <summary>Окно подавления повторного Warning о таймауте regex на одно правило.</summary>
    public static readonly TimeSpan TimeoutWarningInterval = TimeSpan.FromMinutes(10);

    private readonly IRejectionPublisher _inner;
    private readonly DataSaverMetrics _metrics;
    private readonly ILogger<ObservedRejectionPublisher> _logger;
    private readonly RegexPolicy _regexPolicy;
    private readonly LogThrottle _timeoutThrottle;

    public ObservedRejectionPublisher(
        IRejectionPublisher inner,
        DataSaverMetrics metrics,
        ILogger<ObservedRejectionPublisher> logger,
        RegexPolicy? regexPolicy = null,
        TimeProvider? timeProvider = null)
    {
        _inner = inner;
        _metrics = metrics;
        _logger = logger;
        _regexPolicy = regexPolicy ?? RegexPolicy.Default;
        _timeoutThrottle = new LogThrottle(timeProvider ?? TimeProvider.System, TimeoutWarningInterval);
    }

    public async ValueTask<RejectionPublishResult> PublishAsync(IReadOnlyList<RejectedMessage> rejected, CancellationToken cancellationToken)
    {
        var result = await _inner.PublishAsync(rejected, cancellationToken);
        if (!result.IsSuccess)
        {
            _metrics.Retry("publish");
            return result;
        }

        foreach (var item in rejected)
        {
            if (item.Info.Kind == RejectionKind.Rule)
            {
                // Штатная работа фильтра: Warning, без тела и без оповещения администратора. Тег reason метрики фиксирован
                // (rule_matched), чтобы кардинальность не задавалась администратором; rule_id - из снимка правил.
                var timeout = item.Info.Reason == RejectReasons.RuleEvalTimeout;
                _metrics.MessageRejected(timeout ? RejectReasons.RuleEvalTimeout : "rule_matched");
                if (item.Origin is not null && item.Info.RuleId is not null)
                {
                    _metrics.RuleRejected(item.Origin.Source, item.Info.RuleId);
                }

                if (timeout)
                {
                    LogRegexTimeout(item);
                    continue;
                }

                _logger.LogWarning(
                    "Message rejected by rule: {RuleId} {RuleVersion} {ReasonCode} {Source} {SourceEventId} {Topic} {Partition} {Offset}",
                    item.Info.RuleId, item.Info.RuleVersion, item.Info.Reason, item.Origin?.Source, item.Origin?.SourceEventId,
                    item.Message.Topic, item.Message.Partition, item.Message.Offset);
                continue;
            }

            _metrics.MessageRejected(item.Info.Reason);
            // Source и SourceEventId - из конверта разобранного события; для неразобранного сообщения null, тело не логируется.
            _logger.LogError(
                "Message rejected: {Topic} {Partition} {Offset} {Source} {SourceEventId} {Kind} {Reason} {RuleId}",
                item.Message.Topic, item.Message.Partition, item.Message.Offset, item.Origin?.Source, item.Origin?.SourceEventId,
                item.Info.Kind, item.Info.Reason, item.Info.RuleId);
        }

        return result;
    }

    // Таймаут - сбой данных правила, а не штатная фильтрация: счётчик на каждое сообщение, запись в лог ограничена по правилу.
    // Тело, значения полей и шаблон в запись не входят; администратор не оповещается.
    private void LogRegexTimeout(RejectedMessage item)
    {
        var ruleId = item.Info.RuleId;
        if (ruleId is not null)
        {
            _metrics.RuleEvalError("regex_timeout", ruleId);
        }

        if (!_timeoutThrottle.TryPass(ruleId ?? string.Empty, out var suppressed))
        {
            return;
        }

        _logger.LogWarning(
            "Rule evaluation aborted by regex match timeout: {RuleId} {RuleVersion} {Source} {SourceEventId} {Topic} {Partition} {Offset} {TimeoutMs} {Suppressed}",
            ruleId, item.Info.RuleVersion, item.Origin?.Source, item.Origin?.SourceEventId,
            item.Message.Topic, item.Message.Partition, item.Message.Offset,
            (long)_regexPolicy.MatchTimeout.TotalMilliseconds, suppressed);
    }
}
