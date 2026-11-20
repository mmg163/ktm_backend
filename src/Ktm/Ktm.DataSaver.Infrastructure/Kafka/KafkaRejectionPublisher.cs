using System.Globalization;
using System.Text;
using Confluent.Kafka;
using Ktm.DataSaver.Core.Ingestion;
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

    public KafkaRejectionPublisher(KafkaOptions options, KafkaFatalErrorHandler fatalHandler, ILogger<KafkaRejectionPublisher> logger)
    {
        _defaultTopic = options.RejectedTopic;
        _logger = logger;
        var config = new ProducerConfig
        {
            EnableIdempotence = true,
            Acks = Acks.All,
            MessageTimeoutMs = options.ProducerMessageTimeoutMs,
        }.ApplyConnection(options);

        _producer = new ProducerBuilder<byte[], byte[]>(config)
            .SetErrorHandler((_, error) => fatalHandler.Handle(error))
            .Build();
    }

    public async ValueTask<RejectionPublishResult> PublishAsync(IReadOnlyList<RejectedMessage> rejected, CancellationToken cancellationToken)
    {
        try
        {
            var tasks = rejected
                .Select(r => _producer.ProduceAsync(r.Info.TargetTopic ?? _defaultTopic, ToMessage(r), cancellationToken))
                .ToList();
            await Task.WhenAll(tasks);
            return RejectionPublishResult.Success;
        }
        catch (KafkaException ex)
        {
            // ProduceException наследует KafkaException.
            var error = ex.Error;
            if (NonTransient.Contains(error.Code))
            {
                _logger.LogError("Rejected topic publish failed with non-transient error {Code}: {Reason}", error.Code, error.Reason);
            }
            else
            {
                _logger.LogWarning("Rejected topic publish failed: {Code}: {Reason}", error.Code, error.Reason);
            }

            return RejectionPublishResult.Failure($"{error.Code}: {error.Reason}");
        }
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }

    private static Message<byte[], byte[]> ToMessage(RejectedMessage r)
    {
        var message = r.Message;
        return new Message<byte[], byte[]>
        {
            // Пустой ключ = ключа не было (consumer отдаёт null как пустой массив): сохраняем семантику.
            Key = message.Key.IsEmpty ? null! : message.Key.ToArray(),
            Value = message.Body.ToArray(),
            Headers = new Headers
            {
                { "ktm-reject-reason", Utf8(r.Info.Reason) },
                { "ktm-reject-detail", Utf8(r.Info.Detail) },
                { "ktm-source-topic", Utf8(message.Topic) },
                { "ktm-source-partition", Utf8(message.Partition.ToString(CultureInfo.InvariantCulture)) },
                { "ktm-source-offset", Utf8(message.Offset.ToString(CultureInfo.InvariantCulture)) },
            },
        };
    }

    private static byte[] Utf8(string value) => Encoding.UTF8.GetBytes(value);
}
