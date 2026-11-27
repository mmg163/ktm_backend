using System.Text;
using Confluent.Kafka;
using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Resilience;
using Ktm.DataSaver.Infrastructure.Kafka;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ktm.DataSaver.IntegrationTests.Kafka;

[Collection(KafkaCollection.Name)]
public sealed class KafkaDeadLetterPublisherTests(KafkaFixture kafka)
{
    private static DeadLetteredMessage Item() => new(
        new ConsumedMessage("ktm.events.raw", 3, 42, Encoding.UTF8.GetBytes("skud:7"), Encoding.UTF8.GetBytes("{broken"), new DateTimeOffset(2026, 11, 24, 6, 30, 15, 123, TimeSpan.Zero)),
        null,
        new DeadLetterInfo(DeadLetterReasons.UnexpectedError, "rules", "System.InvalidOperationException", null, null, null, 3, DateTimeOffset.UtcNow));

    private KafkaDeadLetterPublisher Publisher(string dlqTopic, int timeoutMs = 30000)
    {
        var options = kafka.Options("unused", "g");
        options.DlqTopic = dlqTopic;
        options.ProducerMessageTimeoutMs = timeoutMs;
        return new KafkaDeadLetterPublisher(options, kafka.NewFatalHandler(), NullLogger<KafkaDeadLetterPublisher>.Instance, new LogThrottle(TimeProvider.System, LogThrottle.DefaultInterval));
    }

    [Fact]
    public async Task Published_message_keeps_key_and_body_and_carries_dlq_and_source_headers()
    {
        var topic = await kafka.CreateTopicAsync("ktm.events.dlq");
        using var publisher = Publisher(topic);

        var result = await publisher.PublishAsync([Item()], CancellationToken.None);

        Assert.True(result.IsSuccess);
        using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            GroupId = $"check-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
        }).Build();
        consumer.Subscribe(topic);
        var message = consumer.Consume(TimeSpan.FromSeconds(30))?.Message;

        Assert.NotNull(message);
        Assert.Equal("skud:7", Encoding.UTF8.GetString(message.Key));
        Assert.Equal("{broken", Encoding.UTF8.GetString(message.Value));
        string Header(string name) => Encoding.UTF8.GetString(message.Headers.GetLastBytes(name));
        Assert.Equal(("unexpected_error", "rules", "System.InvalidOperationException", "3"),
            (Header("ktm-dlq-reason"), Header("ktm-dlq-stage"), Header("ktm-dlq-exception-type"), Header("ktm-dlq-attempts")));
        Assert.Equal(("ktm.events.raw", "3", "42", "2026-11-24T06:30:15.123Z"),
            (Header("ktm-source-topic"), Header("ktm-source-partition"), Header("ktm-source-offset"), Header("ktm-source-timestamp")));
    }

    [Fact]
    public async Task Publishing_to_a_missing_topic_yields_failure_result_instead_of_exception()
    {
        // Брокер dev-стенда не создаёт топики автоматически (KAFKA_AUTO_CREATE_TOPICS_ENABLE=false в фикстуре); иначе тест неприменим.
        using var publisher = Publisher("ktm.events.dlq-missing-" + Guid.NewGuid().ToString("N"), timeoutMs: 3000);

        var result = await publisher.PublishAsync([Item()], CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.False(string.IsNullOrEmpty(result.Error));
    }
}
