using Confluent.Kafka;
using Ktm.DataSaver.Core.Batching;
using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Core.Model;
using Microsoft.Extensions.Logging;

namespace Ktm.DataSaver.Infrastructure.Kafka;

/// <summary>
/// Consumer по ADR-0002 D-1. Обработчики ребаланса лишь копят изменения; пауза и очистка сообщений отозванных
/// партиций - дело цикла (<see cref="TakeAssignmentChanges"/>). Чтение и commit - из потока цикла.
/// </summary>
public sealed class KafkaMessageSource : IMessageSource, IDisposable
{
    private readonly IConsumer<byte[], byte[]> _consumer;
    private readonly KafkaFatalErrorHandler _fatalHandler;
    private readonly ILogger<KafkaMessageSource> _logger;
    private readonly Lock _gate = new();
    private readonly List<TopicPartitionKey> _assigned = [];
    private readonly List<TopicPartitionKey> _revoked = [];

    public KafkaMessageSource(KafkaOptions options, KafkaFatalErrorHandler fatalHandler, ILogger<KafkaMessageSource> logger)
    {
        _fatalHandler = fatalHandler;
        _logger = logger;
        var config = new ConsumerConfig
        {
            GroupId = options.GroupId,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            AllowAutoCreateTopics = false,
            SessionTimeoutMs = options.SessionTimeoutMs,
        }.ApplyConnection(options);

        _consumer = new ConsumerBuilder<byte[], byte[]>(config)
            .SetErrorHandler((_, error) => fatalHandler.Handle(error))
            .SetPartitionsAssignedHandler((_, partitions) => Record(_assigned, partitions))
            .SetPartitionsRevokedHandler((_, partitions) => Record(_revoked, partitions.Select(p => p.TopicPartition)))
            .SetPartitionsLostHandler((_, partitions) => Record(_revoked, partitions.Select(p => p.TopicPartition)))
            .Build();
        _consumer.Subscribe(options.Topic);
    }

    public IReadOnlyCollection<TopicPartitionKey> Assignment =>
        _consumer.Assignment.Select(p => new TopicPartitionKey(p.Topic, p.Partition.Value)).ToArray();

    public ValueTask<ConsumedMessage?> ConsumeAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            // Блокирующий poll ограничен таймаутом цикла; librdkafka обслуживает группу внутри Consume.
            var result = _consumer.Consume(timeout);
            if (result is null || result.IsPartitionEOF)
            {
                return ValueTask.FromResult<ConsumedMessage?>(null);
            }

            return ValueTask.FromResult<ConsumedMessage?>(new ConsumedMessage(
                result.Topic,
                result.Partition.Value,
                result.Offset.Value,
                result.Message.Key ?? [],
                result.Message.Value ?? [],
                result.Message.Timestamp.UtcDateTime));
        }
        catch (ConsumeException ex)
        {
            if (_fatalHandler.Handle(ex.Error))
            {
                throw;
            }

            // Нефатальная ошибка чтения: клиент восстановится сам, цикл продолжает опрос.
            _logger.LogError(ex, "Kafka consume error {Code}", ex.Error.Code);
            return ValueTask.FromResult<ConsumedMessage?>(null);
        }
    }

    public AssignmentChanges TakeAssignmentChanges()
    {
        lock (_gate)
        {
            if (_assigned.Count == 0 && _revoked.Count == 0)
            {
                return AssignmentChanges.None;
            }

            var changes = new AssignmentChanges(_assigned.ToArray(), _revoked.ToArray());
            _assigned.Clear();
            _revoked.Clear();
            return changes;
        }
    }

    public void Pause(IReadOnlyCollection<TopicPartitionKey> partitions)
    {
        if (partitions.Count > 0) _consumer.Pause(ToPartitions(partitions));
    }

    public void Resume(IReadOnlyCollection<TopicPartitionKey> partitions)
    {
        if (partitions.Count > 0) _consumer.Resume(ToPartitions(partitions));
    }

    /// <summary>Синхронный commit (для <see cref="KafkaOffsetCommitter"/>); offsets уже со смещением +1.</summary>
    internal void Commit(IReadOnlyList<PartitionOffset> offsets) =>
        _consumer.Commit(offsets.Select(o => new TopicPartitionOffset(o.Topic, o.Partition, o.Offset)));

    public void Dispose()
    {
        try
        {
            _consumer.Close();
        }
        catch (KafkaException ex)
        {
            _logger.LogWarning(ex, "Kafka consumer close failed");
        }

        _consumer.Dispose();
    }

    private static TopicPartition[] ToPartitions(IReadOnlyCollection<TopicPartitionKey> keys) =>
        keys.Select(k => new TopicPartition(k.Topic, k.Partition)).ToArray();

    private void Record(List<TopicPartitionKey> target, IEnumerable<TopicPartition> partitions)
    {
        lock (_gate)
        {
            target.AddRange(partitions.Select(p => new TopicPartitionKey(p.Topic, p.Partition.Value)));
        }
    }
}
