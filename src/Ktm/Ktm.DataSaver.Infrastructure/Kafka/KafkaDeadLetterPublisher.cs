using System.Globalization;
using Confluent.Kafka;
using Ktm.DataSaver.Core.Ingestion;
using Microsoft.Extensions.Logging;

namespace Ktm.DataSaver.Infrastructure.Kafka;

/// <summary>
/// Публикует в <c>ktm.events.dlq</c> сообщения, которые не удалось обработать (ADR-0006 п. 2.4): отдельный идемпотентный producer,
/// <c>acks=all</c>, исходные ключ и тело, заголовки <c>ktm-dlq-*</c> и <c>ktm-source-*</c>. Ждёт подтверждения брокера;
/// отказ - <see cref="DeadLetterPublishResult"/>, не исключение. Заголовки и логи не содержат тела и текста исключения.
/// </summary>
public sealed class KafkaDeadLetterPublisher : IDeadLetterPublisher, IDisposable
{
    // Лимиты длины заголовков - в UTF-16 code units (string.Length), не в байтах и не в графемах.
    internal const int DetailMaxLength = 1024;

    private readonly IProducer<byte[], byte[]> _producer;
    private readonly string _topic;
    private readonly ILogger<KafkaDeadLetterPublisher> _logger;
    private int _disposed;

    public KafkaDeadLetterPublisher(KafkaOptions options, KafkaFatalErrorHandler fatalHandler, ILogger<KafkaDeadLetterPublisher> logger, LogThrottle throttle)
    {
        _topic = options.DlqTopic;
        _logger = logger;
        _producer = KafkaProducerSupport.CreateProducer(options, fatalHandler, logger, throttle);
    }

    /// <summary>
    /// Ждёт подтверждения каждого сообщения. При частичном сбое возвращает Failure с числом неподтверждённых; повтор пачки -
    /// решение цикла (он передаёт весь список), поэтому подтверждённые сообщения при повторе могут продублироваться (at-least-once;
    /// повторная обработка идемпотентна).
    /// </summary>
    public async ValueTask<DeadLetterPublishResult> PublishAsync(IReadOnlyList<DeadLetteredMessage> messages, CancellationToken cancellationToken)
    {
        List<Task<DeliveryResult<byte[], byte[]>>> tasks;
        try
        {
            tasks = messages.Select(m => _producer.ProduceAsync(_topic, ToMessage(m), cancellationToken)).ToList();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Синхронный отказ постановки в очередь: только тип исключения, без текста.
            _logger.LogError("Dead letter topic publish could not be queued: {ErrorType}", ex.GetType().Name);
            return DeadLetterPublishResult.Failure(ex.GetType().Name);
        }

        var error = await KafkaProducerSupport.AwaitAllAsync(tasks, _logger, "Dead letter topic", includeExceptionText: false);
        return error is null ? DeadLetterPublishResult.Success : DeadLetterPublishResult.Failure(error);
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

    internal static Message<byte[], byte[]> ToMessage(DeadLetteredMessage item)
    {
        var message = item.Message;
        var info = item.Info;
        var result = new Message<byte[], byte[]>
        {
            Value = message.Body.ToArray(),
            Headers = new Headers
            {
                { KafkaHeaders.DlqReason, KafkaProducerSupport.Utf8(info.Reason) },
                { KafkaHeaders.DlqStage, KafkaProducerSupport.Utf8(info.Stage) },
                { KafkaHeaders.DlqExceptionType, KafkaProducerSupport.Utf8(info.ExceptionType) },
                { KafkaHeaders.DlqDetail, KafkaProducerSupport.Utf8(KafkaProducerSupport.Truncate(info.Detail, DetailMaxLength)) },
                { KafkaHeaders.DlqAttempts, KafkaProducerSupport.Utf8(info.Attempts.ToString(CultureInfo.InvariantCulture)) },
                { KafkaHeaders.DlqFailedAt, KafkaProducerSupport.Utf8(KafkaProducerSupport.Rfc3339(info.FailedAt)) },
                { KafkaHeaders.SourceTopic, KafkaProducerSupport.Utf8(message.Topic) },
                { KafkaHeaders.SourcePartition, KafkaProducerSupport.Utf8(message.Partition.ToString(CultureInfo.InvariantCulture)) },
                { KafkaHeaders.SourceOffset, KafkaProducerSupport.Utf8(message.Offset.ToString(CultureInfo.InvariantCulture)) },
                { KafkaHeaders.SourceTimestamp, KafkaProducerSupport.Utf8(KafkaProducerSupport.Rfc3339(message.Timestamp)) },
            },
        };

        if (!string.IsNullOrEmpty(info.SqlState))
        {
            result.Headers.Add(KafkaHeaders.DlqSqlState, KafkaProducerSupport.Utf8(info.SqlState));
        }

        // Пустой ключ = ключа не было (consumer отдаёт null как пустой массив): сохраняем семантику.
        if (!message.Key.IsEmpty)
        {
            result.Key = message.Key.ToArray();
        }

        return result;
    }
}
