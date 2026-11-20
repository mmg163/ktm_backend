using Confluent.Kafka;
using Ktm.DataSaver.Core.Batching;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Infrastructure.Kafka;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ktm.DataSaver.IntegrationTests.Kafka;

[Collection(KafkaCollection.Name)]
public sealed class KafkaMessageSourceTests(KafkaFixture kafka)
{
    private static readonly TimeSpan Overall = TimeSpan.FromSeconds(60);

    private KafkaMessageSource NewSource(string topic, string group) =>
        new(kafka.Options(topic, group), kafka.NewFatalHandler(), NullLogger<KafkaMessageSource>.Instance, new LogThrottle(TimeProvider.System, LogThrottle.DefaultInterval));

    private static async Task<List<ConsumedMessage>> ReadAsync(KafkaMessageSource source, int count)
    {
        var result = new List<ConsumedMessage>();
        var deadline = DateTime.UtcNow + Overall;
        while (result.Count < count && DateTime.UtcNow < deadline)
        {
            if (await source.ConsumeAsync(TimeSpan.FromMilliseconds(500), CancellationToken.None) is { } message)
            {
                result.Add(message);
            }
        }

        return result;
    }

    [Fact]
    public async Task Reads_events_of_all_sources_from_raw_topic_and_ignores_rejected()
    {
        var raw = await kafka.CreateTopicAsync("ktm.events.raw");
        var rejected = await kafka.CreateTopicAsync("ktm.events.rejected");
        await kafka.ProduceAsync(raw, "skud:1", """{"source":"skud"}""");
        await kafka.ProduceAsync(raw, "cv:1", """{"source":"cv"}""");
        await kafka.ProduceAsync(raw, "1c-do:1", """{"source":"1c-do"}""");
        await kafka.ProduceAsync(rejected, "x", "rejected-body");

        using var source = NewSource(raw, $"g-{Guid.NewGuid():N}");
        var messages = await ReadAsync(source, 3);
        var extra = await source.ConsumeAsync(TimeSpan.FromSeconds(2), CancellationToken.None);

        Assert.Equal(3, messages.Count);
        Assert.All(messages, m => Assert.Equal(raw, m.Topic));
        Assert.Equal(["skud:1", "cv:1", "1c-do:1"], messages.Select(m => System.Text.Encoding.UTF8.GetString(m.Key.Span)));
        Assert.Null(extra);
    }

    [Fact]
    public async Task Two_instances_of_same_group_split_partitions_and_report_changes()
    {
        var raw = await kafka.CreateTopicAsync("ktm.events.raw", partitions: 4);
        var group = $"g-{Guid.NewGuid():N}";

        using var first = NewSource(raw, group);
        var assigned = new List<TopicPartitionKey>();
        var secondAssigned = new List<TopicPartitionKey>();
        var revokedFromFirst = new List<TopicPartitionKey>();

        var deadline = DateTime.UtcNow + Overall;
        while (DateTime.UtcNow < deadline && first.Assignment.Count < 4)
        {
            await first.ConsumeAsync(TimeSpan.FromMilliseconds(300), CancellationToken.None);
            assigned.AddRange(first.TakeAssignmentChanges().Assigned);
        }

        using var second = NewSource(raw, group);
        while (DateTime.UtcNow < deadline && !(first.Assignment.Count == 2 && second.Assignment.Count == 2))
        {
            await first.ConsumeAsync(TimeSpan.FromMilliseconds(300), CancellationToken.None);
            await second.ConsumeAsync(TimeSpan.FromMilliseconds(300), CancellationToken.None);
            var changes = first.TakeAssignmentChanges();
            assigned.AddRange(changes.Assigned);
            revokedFromFirst.AddRange(changes.Revoked);
            secondAssigned.AddRange(second.TakeAssignmentChanges().Assigned);
        }

        Assert.Equal(2, first.Assignment.Count);
        Assert.Equal(2, second.Assignment.Count);
        Assert.Empty(first.Assignment.Intersect(second.Assignment));
        Assert.Equal(2, secondAssigned.Distinct().Count());
        // Первый экземпляр сначала держал все 4 партиции, часть из них отозвана (cooperative): цикл увидит отзыв.
        Assert.Equal(4, assigned.Distinct().Count());
        Assert.Equal(2, revokedFromFirst.Distinct().Count());
        Assert.True(first.TakeAssignmentChanges().IsEmpty);
    }

    [Fact]
    public async Task Committed_offset_of_group_equals_number_of_processed_messages()
    {
        var raw = await kafka.CreateTopicAsync("ktm.events.raw");
        var group = $"g-{Guid.NewGuid():N}";
        for (var i = 0; i < 5; i++)
        {
            await kafka.ProduceAsync(raw, $"k{i}", $"body{i}");
        }

        using var source = NewSource(raw, group);
        var committer = new KafkaOffsetCommitter(source);
        var messages = await ReadAsync(source, 5);
        var last = messages[^1];
        await committer.CommitAsync([new PartitionOffset(last.Topic, last.Partition, last.Offset + 1)], CancellationToken.None);

        using var admin = kafka.NewAdmin();
        var committed = await admin.ListConsumerGroupOffsetsAsync([new ConsumerGroupTopicPartitions(group, [new TopicPartition(raw, 0)])]);
        var offset = committed.Single().Partitions.Single().Offset.Value;

        Assert.Equal(5, messages.Count);
        Assert.Equal(5, offset);
    }

    [Fact]
    public async Task Paused_partitions_yield_no_messages_until_resumed()
    {
        var raw = await kafka.CreateTopicAsync("ktm.events.raw");
        using var source = NewSource(raw, $"g-{Guid.NewGuid():N}");
        var deadline = DateTime.UtcNow + Overall;
        while (source.Assignment.Count == 0 && DateTime.UtcNow < deadline)
        {
            await source.ConsumeAsync(TimeSpan.FromMilliseconds(300), CancellationToken.None);
        }

        source.Pause(source.Assignment);
        await kafka.ProduceAsync(raw, "k", "v");
        var whilePaused = await source.ConsumeAsync(TimeSpan.FromSeconds(2), CancellationToken.None);
        source.Resume(source.Assignment);
        var afterResume = await ReadAsync(source, 1);

        Assert.Null(whilePaused);
        Assert.Single(afterResume);
    }

    [Fact]
    public void ProbeBroker_for_missing_topic_does_not_fail_because_broker_is_available()
    {
        using var source = NewSource($"ktm.events.missing.{Guid.NewGuid():N}", $"g-{Guid.NewGuid():N}");

        var error = Record.Exception(() => source.ProbeBroker(TimeSpan.FromSeconds(5)));

        Assert.Null(error);
        Assert.False(source.HasAssignment);
    }

    [Fact]
    public async Task Pause_and_resume_tolerate_partitions_missing_from_assignment()
    {
        var raw = await kafka.CreateTopicAsync("ktm.events.raw");
        using var source = NewSource(raw, $"g-{Guid.NewGuid():N}");
        var missing = new TopicPartitionKey(raw, 7);

        var pause = Record.Exception(() => source.Pause([missing]));
        var resume = Record.Exception(() => source.Resume([missing]));

        Assert.Null(pause);
        Assert.Null(resume);
    }
}
