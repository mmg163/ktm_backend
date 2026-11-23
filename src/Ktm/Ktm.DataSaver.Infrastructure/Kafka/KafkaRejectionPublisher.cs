using System.Globalization;
using System.Text;
using Confluent.Kafka;
using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Core.Pipeline;
using Microsoft.Extensions.Logging;

namespace Ktm.DataSaver.Infrastructure.Kafka;

/// <summary>
/// Публикует отбракованные сообщения (kafka-event-format.md п. 3): идемпотентный producer, acks=all, исходные ключ и
/// тело, заголовки <c>ktm-reject-*</c>/<c>ktm-source-*</c>. Ждёт подтверждения брокера; отказ - <see cref="RejectionPublishResult"/>.
/// </summary>
public sealed class KafkaRejectionPublisher : IRejectionPublisher, IDisposable
{
    private static readonly HashSet<ErrorCode> NonTransient =
    [
        ErrorCode.TopicAuthorizationFailed,
        ErrorCode.ClusterAuthorizationFailed,
        ErrorCode.Local_Authentication,
        ErrorCode.SaslAuthenticationFailed,
        ErrorCode.MsgSizeTooLarge,
        ErrorCode.InvalidMsgSize,
    ];

    private readonly IProducer<byte[], byte[]> _producer;
    private readonly string _defaultTopic;
    private readonly ILogger<KafkaRejectionPublisher> _logger;
    private int _disposed;

    public KafkaRejectionPublisher(KafkaOptions options, KafkaFatalErrorHandler fatalHandler, ILogger<KafkaRejectionPublisher> logger, LogThrottle throttle)
    {
        _defaultTopic = options.RejectedTopic;
        _logger = logger;
        var nativeLog = new KafkaNativeLogBridge(logger, throttle);
        var config = new ProducerConfig
        {
            EnableIdempotence = true,
            Acks = Acks.All,
            MessageTimeoutMs = options.ProducerMessageTimeoutMs,
        }.ApplyConnection(options);

        _producer = new ProducerBuilder<byte[], byte[]>(config)
            .SetErrorHandler((_, error) => fatalHandler.Handle(error))
            .SetLogHandler((_, message) => nativeLog.Handle(message))
            .Build();
    }

    /// <summary>
    /// Ждёт подтверждения каждого сообщения и проверяет статус каждой задачи. При частичном сбое возвращает Failure с
    /// числом неподтверждённых. Повтор пачки - решение цикла (он передаёт весь список); идемпотентность producer
    /// защищает только от дублей внутри сессии, поэтому уже подтверждённые сообщения при повторе могут продублироваться
    /// (допустимо для ручного разбора rejected).
    /// </summary>
    public async ValueTask<RejectionPublishResult> PublishAsync(IReadOnlyList<RejectedMessage> rejected, CancellationToken cancellationToken)
    {
        var tasks = rejected
            .Select(r => _producer.ProduceAsync(r.Info.TargetTopic ?? _defaultTopic, ToMessage(r), cancellationToken))
            .ToList();
        try
        {
            await Task.WhenAll(tasks);
            return RejectionPublishResult.Success;
        }
        catch (Exception) when (tasks.Any(t => t.IsFaulted))
        {
            var failed = tasks.Where(t => t.IsFaulted).ToList();
            var error = failed.Select(t => (t.Exception!.GetBaseException() as KafkaException)?.Error)
                .FirstOrDefault(e => e is not null);
            var unconfirmed = tasks.Count(t => !t.IsCompletedSuccessfully);
            var code = error?.Code ?? ErrorCode.Unknown;
            var reason = error?.Reason ?? failed[0].Exception!.GetBaseException().Message;

            if (NonTransient.Contains(code))
            {
                _logger.LogError("Rejected topic publish failed with non-transient error {Code}: {Reason}; unconfirmed {Unconfirmed} of {Total}", code, reason, unconfirmed, tasks.Count);
            }
            else
            {
                _logger.LogWarning("Rejected topic publish failed: {Code}: {Reason}; unconfirmed {Unconfirmed} of {Total}", code, reason, unconfirmed, tasks.Count);
            }

            return RejectionPublishResult.Failure($"{code}: {reason}; unconfirmed {unconfirmed} of {tasks.Count}");
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }

    internal const string RuleIdHeader = "ktm-reject-rule-id";
    // Лимиты длины заголовков - в UTF-16 code units (string.Length), не в байтах и не в графемах.
    internal const int RuleIdMaxLength = 128;
    internal const string RuleNameHeader = "ktm-reject-rule-name";
    internal const string RuleVersionHeader = "ktm-reject-rule-version";
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
                { "ktm-reject-reason", Utf8(r.Info.Reason) },
                { "ktm-reject-detail", Utf8(r.Info.Kind == RejectionKind.Rule ? Truncate(r.Info.Detail, DetailMaxLength) : r.Info.Detail) },
                { "ktm-source-topic", Utf8(message.Topic) },
                { "ktm-source-partition", Utf8(message.Partition.ToString(CultureInfo.InvariantCulture)) },
                { "ktm-source-offset", Utf8(message.Offset.ToString(CultureInfo.InvariantCulture)) },
            },
        };

        var ruleId = NormalizeRuleId(r.Info.RuleId);
        if (ruleId is not null)
        {
            result.Headers.Add(RuleIdHeader, Utf8(ruleId));
        }

        // Название и версия правила - только у отбраковки правилом; форматные отбраковки их не получают.
        if (r.Info.Kind == RejectionKind.Rule)
        {
            var ruleName = Normalize(r.Info.RuleName, RuleNameMaxLength);
            if (ruleName is not null)
            {
                result.Headers.Add(RuleNameHeader, Utf8(ruleName));
            }

            if (r.Info.RuleVersion is { } version)
            {
                result.Headers.Add(RuleVersionHeader, Utf8(version.ToString(CultureInfo.InvariantCulture)));
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

    private static string Truncate(string value, int max)
    {
        if (value.Length <= max)
        {
            return value;
        }

        return value[..(char.IsHighSurrogate(value[max - 1]) ? max - 1 : max)];
    }

    private static byte[] Utf8(string value) => Encoding.UTF8.GetBytes(value);
}
