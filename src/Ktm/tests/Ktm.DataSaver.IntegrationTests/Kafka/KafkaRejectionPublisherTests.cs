using System.Text;
using Confluent.Kafka;
using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Pipeline;
using Ktm.DataSaver.Infrastructure.Kafka;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ktm.DataSaver.IntegrationTests.Kafka;

[Collection(KafkaCollection.Name)]
public sealed class KafkaRejectionPublisherTests(KafkaFixture kafka)
{
    private static RejectedMessage Rejected(string body, string? targetTopic = null) => new(
        new ConsumedMessage("ktm.events.raw", 3, 42, Encoding.UTF8.GetBytes("skud:7"), Encoding.UTF8.GetBytes(body), DateTimeOffset.UtcNow),
        new RejectionInfo(RejectionKind.Format, RejectReasons.MalformedJson, "unexpected token at 5", TargetTopic: targetTopic));

    [Fact]
    public async Task Published_message_keeps_key_body_and_carries_all_reject_headers()
    {
        var rejectedTopic = await kafka.CreateTopicAsync("ktm.events.rejected");
        using var publisher = new KafkaRejectionPublisher(
            kafka.Options("unused", "g", rejectedTopic), kafka.NewFatalHandler(), NullLogger<KafkaRejectionPublisher>.Instance, new LogThrottle(TimeProvider.System, LogThrottle.DefaultInterval));

        var result = await publisher.PublishAsync([Rejected("{broken")], CancellationToken.None);

        Assert.True(result.IsSuccess);
        using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            GroupId = $"check-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
        }).Build();
        consumer.Subscribe(rejectedTopic);
        var message = consumer.Consume(TimeSpan.FromSeconds(30))?.Message;

        Assert.NotNull(message);
        Assert.Equal("skud:7", Encoding.UTF8.GetString(message.Key));
        Assert.Equal("{broken", Encoding.UTF8.GetString(message.Value));
        string Header(string name) => Encoding.UTF8.GetString(message.Headers.GetLastBytes(name));
        Assert.Equal(6, message.Headers.Count);
        Assert.Equal("malformed_json", Header("ktm-reject-reason"));
        Assert.Equal("unexpected token at 5", Header("ktm-reject-detail"));
        Assert.Equal("ktm.events.raw", Header("ktm-source-topic"));
        Assert.Equal("3", Header("ktm-source-partition"));
        Assert.Equal("42", Header("ktm-source-offset"));
        Assert.False(message.Headers.TryGetLastBytes("ktm-reject-rule-id", out _), "malformed_json has no rule id header");
    }

    [Fact]
    public async Task Unreachable_broker_yields_failure_result_instead_of_exception()
    {
        var options = kafka.Options("unused", "g");
        options.BootstrapServers = "127.0.0.1:1";
        options.ProducerMessageTimeoutMs = 2000;
        using var publisher = new KafkaRejectionPublisher(options, kafka.NewFatalHandler(), NullLogger<KafkaRejectionPublisher>.Instance, new LogThrottle(TimeProvider.System, LogThrottle.DefaultInterval));

        var result = await publisher.PublishAsync([Rejected("{broken")], CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.False(string.IsNullOrEmpty(result.Error));
    }

    [Fact]
    public async Task Message_is_routed_to_target_topic_when_specified_else_default()
    {
        var defaultTopic = await kafka.CreateTopicAsync("ktm.events.rejected");
        var custom = await kafka.CreateTopicAsync("ktm.events.custom-rejected");
        using var publisher = new KafkaRejectionPublisher(
            kafka.Options("unused", "g", defaultTopic), kafka.NewFatalHandler(), NullLogger<KafkaRejectionPublisher>.Instance, new LogThrottle(TimeProvider.System, LogThrottle.DefaultInterval));

        var result = await publisher.PublishAsync([Rejected("a"), Rejected("b", custom)], CancellationToken.None);

        Assert.True(result.IsSuccess);
        Assert.Equal("a", Read(defaultTopic));
        Assert.Equal("b", Read(custom));
    }

    private string Read(string topic)
    {
        using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            GroupId = $"check-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
        }).Build();
        consumer.Subscribe(topic);
        return Encoding.UTF8.GetString(consumer.Consume(TimeSpan.FromSeconds(30))!.Message.Value);
    }
}
