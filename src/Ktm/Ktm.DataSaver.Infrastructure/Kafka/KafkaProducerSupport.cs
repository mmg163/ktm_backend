using System.Globalization;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Ktm.DataSaver.Infrastructure.Kafka;

/// <summary>
/// Общая часть публикаторов rejected и dlq: сборка идемпотентного producer, разбор отказа публикации, формат заголовков.
/// </summary>
internal static class KafkaProducerSupport
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

    /// <summary>Идемпотентный producer, <c>acks=all</c>, предел ожидания подтверждения <c>ProducerMessageTimeoutMs</c>.</summary>
    public static IProducer<byte[], byte[]> CreateProducer(KafkaOptions options, KafkaFatalErrorHandler fatalHandler, ILogger logger, LogThrottle throttle)
    {
        var nativeLog = new KafkaNativeLogBridge(logger, throttle);
        var config = new ProducerConfig
        {
            EnableIdempotence = true,
            Acks = Acks.All,
            MessageTimeoutMs = options.ProducerMessageTimeoutMs,
        }.ApplyConnection(options);

        return new ProducerBuilder<byte[], byte[]>(config)
            .SetErrorHandler((_, error) => fatalHandler.Handle(error))
            .SetLogHandler((_, message) => nativeLog.Handle(message))
            .Build();
    }

    /// <summary>
    /// Ждёт подтверждения всех сообщений. null - всё подтверждено; иначе описание отказа с числом неподтверждённых.
    /// Неповторяемые ошибки (права, размер) пишутся в Error-лог, прочие - в Warning. <paramref name="includeExceptionText"/> -
    /// разрешить ли текст не-Kafka исключения в описании отказа (для dlq запрещено: текст может нести фрагмент тела).
    /// </summary>
    public static async ValueTask<string?> AwaitAllAsync(
        IReadOnlyList<Task<DeliveryResult<byte[], byte[]>>> tasks,
        ILogger logger,
        string topicKind,
        bool includeExceptionText)
    {
        try
        {
            await Task.WhenAll(tasks);
            return null;
        }
        catch (Exception) when (tasks.Any(t => t.IsFaulted))
        {
            var failed = tasks.Where(t => t.IsFaulted).ToList();
            var error = failed.Select(t => (t.Exception!.GetBaseException() as KafkaException)?.Error)
                .FirstOrDefault(e => e is not null);
            var unconfirmed = tasks.Count(t => !t.IsCompletedSuccessfully);
            var code = error?.Code ?? ErrorCode.Unknown;
            var baseException = failed[0].Exception!.GetBaseException();
            var reason = error?.Reason ?? (includeExceptionText ? baseException.Message : baseException.GetType().Name);

            if (NonTransient.Contains(code))
            {
                logger.LogError("{TopicKind} publish failed with non-transient error {Code}: {Reason}; unconfirmed {Unconfirmed} of {Total}", topicKind, code, reason, unconfirmed, tasks.Count);
            }
            else
            {
                logger.LogWarning("{TopicKind} publish failed: {Code}: {Reason}; unconfirmed {Unconfirmed} of {Total}", topicKind, code, reason, unconfirmed, tasks.Count);
            }

            return $"{code}: {reason}; unconfirmed {unconfirmed} of {tasks.Count}";
        }
    }

    public static byte[] Utf8(string value) => Encoding.UTF8.GetBytes(value);

    /// <summary>RFC 3339 в UTC с суффиксом <c>Z</c> и миллисекундами (точность Kafka-timestamp).</summary>
    public static string Rfc3339(DateTimeOffset value) =>
        value.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture);

    /// <summary>Усечение до <paramref name="max"/> UTF-16 code units без разрыва суррогатной пары.</summary>
    public static string Truncate(string value, int max)
    {
        if (value.Length <= max)
        {
            return value;
        }

        return value[..(char.IsHighSurrogate(value[max - 1]) ? max - 1 : max)];
    }
}
