using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Ktm.DataSaver.Infrastructure.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Testcontainers.Kafka;

namespace Ktm.DataSaver.IntegrationTests.Kafka;

/// <summary>Kafka (KRaft) в контейнере; тесты изолируются уникальными именами топиков и групп.</summary>
public sealed class KafkaFixture : IAsyncLifetime
{
    private readonly KafkaContainer _container = new KafkaBuilder("apache/kafka:3.9.1").Build();

    public string BootstrapServers => _container.GetBootstrapAddress();

    public Task InitializeAsync() => _container.StartAsync();

    public async Task DisposeAsync() => await _container.DisposeAsync();

    public async Task<string> CreateTopicAsync(string prefix, int partitions = 1)
    {
        var name = $"{prefix}.{Guid.NewGuid():N}";
        using var admin = NewAdmin();
        await admin.CreateTopicsAsync([new TopicSpecification { Name = name, NumPartitions = partitions, ReplicationFactor = 1 }]);
        return name;
    }

    public IAdminClient NewAdmin() =>
        new AdminClientBuilder(new AdminClientConfig { BootstrapServers = BootstrapServers }).Build();

    public async Task ProduceAsync(string topic, string key, string body)
    {
        using var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = BootstrapServers }).Build();
        await producer.ProduceAsync(topic, new Message<string, string> { Key = key, Value = body });
    }

    public KafkaOptions Options(string topic, string group, string? rejectedTopic = null) => new()
    {
        BootstrapServers = BootstrapServers,
        Topic = topic,
        GroupId = group,
        RejectedTopic = rejectedTopic ?? "ktm.events.rejected",
        SessionTimeoutMs = 10000,
    };

    public KafkaFatalErrorHandler NewFatalHandler() =>
        new(new NoopTerminator(), NullLogger<KafkaFatalErrorHandler>.Instance);

    private sealed class NoopTerminator : IProcessTerminator
    {
        public void Terminate(int exitCode, string reason)
        {
        }
    }
}
