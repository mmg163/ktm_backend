using System.Globalization;
using System.Text;
using Confluent.Kafka;
using Ktm.DataSaver.Application.Ingestion;
using Ktm.DataSaver.Application.Pipeline;
using Microsoft.Extensions.Logging;

namespace Ktm.DataSaver.Infrastructure.Kafka;

/// <summary>
/// Публикует отбракованные сообщения (kafka-event-format.md п. 3): идемпотентный producer, acks=all, исходные ключ и
/// тело, заголовки <c>ktm-reject-*</c>/<c>ktm-source-*</c>. Ждёт подтверждения брокера; отказ - <see cref="RejectionPublishResult"/>.
/// </summary>
public sealed class KafkaRejectionPublisher : IRejectionPublisher, IDisposable
{
    private readonly IProducer<byte[], byte[]> _producer;
    private readonly string _defaultTopic;
    private readonly ILogger<KafkaRejectionPublisher> _logger;
    private int _disposed;

    public KafkaRejectionPublisher(KafkaOptions options, KafkaFatalErrorHandler fatalHandler, ILogger<KafkaRejectionPublisher> logger, LogThrottle throttle)
    {
        _defaultTopic = options.RejectedTopic;
        _logger = logger;
        _producer = KafkaProducerSupport.CreateProducer(options, fatalHandler, logger, throttle);
    }

    /// <summary>
    /// Ждёт подтверждения каждого сообщения и проверяет статус каждой задачи. При частичном сбое возвращает Failure с
    /// числом неподтверждённых. Повтор пачки - решение цикла (он передаёт весь список); идемпотентность producer
    /// защищает только от дублей внутри сессии, поэтому уже подтверждённые сообщения при повторе могут продублироваться
    /// (допустимо для ручного разбора rejected).
    /// </summary>
    public async ValueTask<RejectionPublishResult> PublishAsync(IReadOnlyList<RejectedMessage> rejected, CancellationToken cancellationToken)
    {
        List<Task<DeliveryResult<byte[], byte[]>>> tasks;
        try
        {
            tasks = rejected
                .Select(r => _producer.ProduceAsync(r.Info.TargetTopic ?? _defaultTopic, ToMessage(r), cancellationToken))
                .ToList();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Синхронный отказ постановки в очередь: только тип исключения, без текста.
            _logger.LogError("Rejected topic publish could not be queued: {ErrorType}", ex.GetType().Name);
            return RejectionPublishResult.Failure(ex.GetType().Name);
        }

        var error = await KafkaProducerSupport.AwaitAllAsync(tasks, _logger, "Rejected topic", includeExceptionText: true);
        return error is null ? RejectionPublishResult.Success : RejectionPublishResult.Failure(error);
    }

    /// <summary>Producer закрыт (для проверки порядка остановки в тестах).</summary>
    internal bool IsDisposed => Volatile.Read(ref _disposed) != 0;

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }

    // Лимиты длины заголовков - в UTF-16 code units (string.Length), не в байтах и не в графемах.
    internal const int RuleIdMaxLength = 128;
    internal const int RuleNameMaxLength = 256;
    internal const int DetailMaxLength = 1024;

    internal static Message<byte[], byte[]> ToMessage(RejectedMessage r)
    {
        var message = r.Message;
        var result = new Message<byte[], byte[]>
        {
            Value = message.Body.ToArray(),
            Headers = new Headers
            {
                { KafkaHeaders.RejectReason, Utf8(r.Info.Reason) },
                { KafkaHeaders.RejectDetail, Utf8(r.Info.Kind == RejectionKind.Rule ? Truncate(r.Info.Detail, DetailMaxLength) : r.Info.Detail) },
                { KafkaHeaders.SourceTopic, Utf8(message.Topic) },
                { KafkaHeaders.SourcePartition, Utf8(message.Partition.ToString(CultureInfo.InvariantCulture)) },
                { KafkaHeaders.SourceOffset, Utf8(message.Offset.ToString(CultureInfo.InvariantCulture)) },
                { KafkaHeaders.SourceTimestamp, Utf8(KafkaProducerSupport.Rfc3339(message.Timestamp)) },
            },
        };

        var ruleId = NormalizeRuleId(r.Info.RuleId);
        if (ruleId is not null)
        {
            result.Headers.Add(KafkaHeaders.RejectRuleId, Utf8(ruleId));
        }

        // Название и версия правила - только у отбраковки правилом; форматные отбраковки их не получают.
        if (r.Info.Kind == RejectionKind.Rule)
        {
            var ruleName = Normalize(r.Info.RuleName, RuleNameMaxLength);
            if (ruleName is not null)
            {
                result.Headers.Add(KafkaHeaders.RejectRuleName, Utf8(ruleName));
            }

            if (r.Info.RuleVersion is { } version)
            {
                result.Headers.Add(KafkaHeaders.RejectRuleVersion, Utf8(version.ToString(CultureInfo.InvariantCulture)));
            }
        }

        // Пустой ключ = ключа не было (consumer отдаёт null как пустой массив): сохраняем семантику.
        if (!message.Key.IsEmpty)
        {
            result.Key = message.Key.ToArray();
        }

        return result;
    }

    /// <summary>
    /// Идентификатор правила для заголовка: trim, не длиннее 128 символов (без разрыва суррогатной пары);
    /// null, если идентификатора нет или он состоит из пробелов.
    /// </summary>
    internal static string? NormalizeRuleId(string? ruleId) => Normalize(ruleId, RuleIdMaxLength);

    /// <summary>Trim и усечение до <paramref name="max"/> UTF-16 code units без разрыва суррогатной пары; null для пустого.</summary>
    private static string? Normalize(string? value, int max)
    {
        var trimmed = value?.Trim();
        if (string.IsNullOrEmpty(trimmed))
        {
            return null;
        }

        var result = Truncate(trimmed, max).TrimEnd();
        return result.Length == 0 ? null : result;
    }

    private static string Truncate(string value, int max) => KafkaProducerSupport.Truncate(value, max);

    private static byte[] Utf8(string value) => KafkaProducerSupport.Utf8(value);
}
