using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Ktm.DataSaver.IntegrationTests.Support;

/// <summary>Тестовый клиент Kafka: публикация, чтение результата, committed offsets, состав группы. Не часть продукта.</summary>
internal sealed class KafkaClient(string bootstrapServers)
{
    public async Task<string> CreateTopicAsync(string name, int partitions = 1)
    {
        using var admin = NewAdmin();
        await admin.CreateTopicsAsync([new TopicSpecification { Name = name, NumPartitions = partitions, ReplicationFactor = 1 }]);
        return name;
    }

    public async Task ProduceAsync(string topic, IEnumerable<(string Key, string Body)> messages)
    {
        using var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrapServers, LingerMs = 5 }).Build();
        var pending = messages
            .Select(m => producer.ProduceAsync(topic, new Message<string, string> { Key = m.Key, Value = m.Body }))
            .ToList();
        await Task.WhenAll(pending);
    }

    /// <summary>Сумма committed offset группы по топику (топик создан в тесте, смещения начинаются с 0): сколько сообщений зафиксировано.</summary>
    public async Task<long> CommittedAsync(string group, string topic)
    {
        using var admin = NewAdmin();
        try
        {
            var result = (await admin.ListConsumerGroupOffsetsAsync([new ConsumerGroupTopicPartitions(group, null)])).Single();
            return result.Partitions.Where(p => p.Topic == topic && p.Offset.Value >= 0).Sum(p => p.Offset.Value);
        }
        catch (KafkaException)
        {
            return 0;
        }
    }

    /// <summary>Состав группы: участник (id) -> назначенные партиции топика; null, пока группа не описывается.</summary>
    public async Task<GroupSnapshot?> DescribeGroupAsync(string group, string topic)
    {
        using var admin = NewAdmin();
        try
        {
            var description = (await admin.DescribeConsumerGroupsAsync([group])).ConsumerGroupDescriptions.Single();
            var members = description.Members.ToDictionary(
                m => m.ConsumerId,
                m => (IReadOnlySet<int>)m.Assignment.TopicPartitions.Where(p => p.Topic == topic).Select(p => p.Partition.Value).ToHashSet());
            return new GroupSnapshot(description.State, members);
        }
        catch (DescribeConsumerGroupsException)
        {
            return null;
        }
    }

    /// <summary>Читает топик с начала, пока не наберётся <paramref name="count"/> сообщений или не выйдет таймаут.</summary>
    public IReadOnlyList<ConsumedRecord> Read(string topic, int count, TimeSpan? timeout = null)
    {
        using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = "reader-" + Guid.NewGuid().ToString("N"),
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        }).Build();
        consumer.Subscribe(topic);
        var records = new List<ConsumedRecord>();
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(30));
        while (records.Count < count && DateTime.UtcNow < deadline)
        {
            var result = consumer.Consume(TimeSpan.FromMilliseconds(200));
            if (result is null) continue;
            var headers = result.Message.Headers.ToDictionary(h => h.Key, h => System.Text.Encoding.UTF8.GetString(h.GetValueBytes()));
            records.Add(new ConsumedRecord(result.Message.Key, System.Text.Encoding.UTF8.GetString(result.Message.Value), headers));
        }

        return records;
    }

    private IAdminClient NewAdmin() =>
        new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();
}

internal sealed record GroupSnapshot(ConsumerGroupState State, IReadOnlyDictionary<string, IReadOnlySet<int>> Members);

internal sealed record ConsumedRecord(byte[]? Key, string Value, IReadOnlyDictionary<string, string> Headers);
