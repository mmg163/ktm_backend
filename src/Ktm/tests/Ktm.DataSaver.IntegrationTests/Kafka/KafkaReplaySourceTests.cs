using System.Text;
using Confluent.Kafka;
using Ktm.DataSaver.Core.Reprocessing;
using Ktm.DataSaver.Infrastructure.Kafka;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ktm.DataSaver.IntegrationTests.Kafka;

/// <summary>Источник повторной обработки на реальном Kafka: период, снимок, retention, рабочая группа не затронута.</summary>
[Collection(KafkaCollection.Name)]
public sealed class KafkaReplaySourceTests(KafkaFixture kafka)
{
    private static readonly DateTimeOffset T = new(2026, 11, 20, 10, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset T2 = T.AddMinutes(5);

    private KafkaReplaySource Source(string raw, string group = "ktm-data-saver", string? dlq = null, string? rejected = null)
    {
        var options = kafka.Options(raw, group, rejected);
        if (dlq is not null)
        {
            options.DlqTopic = dlq;
        }

        return new KafkaReplaySource(options, NullLogger<KafkaReplaySource>.Instance, new LogThrottle(TimeProvider.System, LogThrottle.DefaultInterval));
    }

    private async Task ProduceAsync(string topic, string body, DateTimeOffset timestamp, Dictionary<string, string>? headers = null)
    {
        using var producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig { BootstrapServers = kafka.BootstrapServers }).Build();
        var message = new Message<byte[], byte[]>
        {
            Key = Encoding.UTF8.GetBytes("k-" + body),
            Value = Encoding.UTF8.GetBytes(body),
            Timestamp = new Timestamp(timestamp),
        };
        if (headers is not null)
        {
            message.Headers = new Headers();
            foreach (var (name, value) in headers)
            {
                message.Headers.Add(name, Encoding.UTF8.GetBytes(value));
            }
        }

        await producer.ProduceAsync(topic, message);
    }

    private static async Task<List<ReplayMessage>> ReadAllAsync(IReplayReader reader)
    {
        var all = new List<ReplayMessage>();
        while (true)
        {
            var batch = await reader.ReadBatchAsync(100, CancellationToken.None);
            if (batch.Count == 0)
            {
                return all;
            }

            all.AddRange(batch);
        }
    }

    private static ReprocessRequest Raw(DateTimeOffset from, DateTimeOffset to) => new(ReprocessSource.Raw, from, to, null, 100);

    [Fact]
    public async Task Raw_SelectedTopic_OverridesRegexSubscription()
    {
        var topic = await kafka.CreateTopicAsync("ktm.events.raw.cv");
        await ProduceAsync(topic, "selected", T);
        var options = kafka.Options("^ktm\\.events\\.raw\\..+", "topic-selection");
        var opened = await Source(options.Topic).OpenAsync(Raw(T, T2) with { Topic = topic }, CancellationToken.None);
        await using var reader = opened.Reader!;
        Assert.Equal(["selected"], (await ReadAllAsync(reader)).Select(m => Encoding.UTF8.GetString(m.Message.Body.Span)));
    }

    [Fact]
    public async Task Raw_PeriodIsHalfOpenByKafkaTimestamp_WithBoundaries()
    {
        var topic = await kafka.CreateTopicAsync("ktm.events.raw");
        await ProduceAsync(topic, "before", T.AddMilliseconds(-1));
        await ProduceAsync(topic, "at-from", T);
        await ProduceAsync(topic, "before-to", T2.AddMilliseconds(-1));
        await ProduceAsync(topic, "at-to", T2);

        var opened = await Source(topic).OpenAsync(Raw(T, T2), CancellationToken.None);

        Assert.True(opened.IsSuccess);
        await using var reader = opened.Reader!;
        var messages = await ReadAllAsync(reader);
        Assert.Equal(["at-from", "before-to"], messages.Select(m => Encoding.UTF8.GetString(m.Message.Body.Span)));
        Assert.All(messages, m => Assert.True(m.CoordinatesFromHeaders));
    }

    [Fact]
    public async Task Raw_PeriodWithoutMessages_ReadsNothing()
    {
        var topic = await kafka.CreateTopicAsync("ktm.events.raw");
        await ProduceAsync(topic, "early", T.AddHours(-2));

        var opened = await Source(topic).OpenAsync(Raw(T, T2), CancellationToken.None);

        await using var reader = opened.Reader!;
        Assert.Empty(await ReadAllAsync(reader));
    }

    [Fact]
    public async Task Raw_PeriodEarlierThanRetainedData_IsRejectedWithEarliestAvailableTime()
    {
        var topic = await kafka.CreateTopicAsync("ktm.events.raw");
        await ProduceAsync(topic, "old", T.AddHours(-3));
        await ProduceAsync(topic, "kept", T);
        using (var admin = kafka.NewAdmin())
        {
            // Имитация retention: начало партиции удалено, low watermark = 1.
            await admin.DeleteRecordsAsync([new TopicPartitionOffset(topic, 0, 1)]);
        }

        var tooEarly = await Source(topic).OpenAsync(Raw(T.AddHours(-2), T2), CancellationToken.None);
        var covered = await Source(topic).OpenAsync(Raw(T, T2), CancellationToken.None);

        Assert.False(tooEarly.IsSuccess);
        var error = Assert.Single(tooEarly.Errors);
        Assert.Equal("from", error.Field);
        Assert.Contains("2026-11-20T10:00:00.000Z", error.Message);
        Assert.True(covered.IsSuccess);
        await covered.Reader!.DisposeAsync();
    }

    [Fact]
    public async Task Snapshot_DoesNotCatchUpWithMessagesPublishedAfterOpening()
    {
        var dlq = await kafka.CreateTopicAsync("ktm.events.dlq");
        await ProduceAsync(dlq, "first", T);
        var opened = await Source("unused", dlq: dlq).OpenAsync(new ReprocessRequest(ReprocessSource.Dlq, null, null, null, 100), CancellationToken.None);
        await using var reader = opened.Reader!;

        await ProduceAsync(dlq, "late", T2);
        var messages = await ReadAllAsync(reader);

        Assert.Equal(["first"], messages.Select(m => Encoding.UTF8.GetString(m.Message.Body.Span)));
    }

    [Fact]
    public async Task Reading_DoesNotTouchOrCreateConsumerGroupOffsets()
    {
        var topic = await kafka.CreateTopicAsync("ktm.events.raw");
        await ProduceAsync(topic, "a", T);
        await ProduceAsync(topic, "b", T);
        var group = "work-" + Guid.NewGuid().ToString("N");
        using (var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
        {
            BootstrapServers = kafka.BootstrapServers, GroupId = group, AutoOffsetReset = AutoOffsetReset.Earliest, EnableAutoCommit = false,
        }).Build())
        {
            consumer.Assign(new TopicPartitionOffset(topic, 0, 0));
            consumer.Consume(TimeSpan.FromSeconds(30));
            consumer.Commit([new TopicPartitionOffset(topic, 0, 1)]);
        }

        var opened = await Source(topic, group).OpenAsync(Raw(T.AddMinutes(-1), T2), CancellationToken.None);
        await using (var reader = opened.Reader!)
        {
            Assert.Equal(2, (await ReadAllAsync(reader)).Count);
        }

        Assert.Equal(1, await kafka.Client.CommittedAsync(group, topic));
        Assert.Equal(0, await kafka.Client.CommittedAsync(group + "-reprocess", topic));
    }

    [Fact]
    public async Task DlqAndRejected_UseSourceCoordinatesAndReasonFromHeaders_OrActualOnesWhenAbsent()
    {
        var dlq = await kafka.CreateTopicAsync("ktm.events.dlq");
        var rejected = await kafka.CreateTopicAsync("ktm.events.rejected");
        await ProduceAsync(dlq, "with-headers", T2, new()
        {
            ["ktm-dlq-reason"] = "unexpected_error",
            ["ktm-dlq-stage"] = "rules",
            ["ktm-source-topic"] = "ktm.events.raw",
            ["ktm-source-partition"] = "4",
            ["ktm-source-offset"] = "77",
            ["ktm-source-timestamp"] = "2026-11-20T10:00:00.123Z",
        });
        await ProduceAsync(dlq, "no-headers", T2);
        await ProduceAsync(rejected, "rej", T2, new() { ["ktm-reject-reason"] = "rule_x" });

        await using var dlqReader = (await Source("unused", dlq: dlq).OpenAsync(new ReprocessRequest(ReprocessSource.Dlq, null, null, null, 100), CancellationToken.None)).Reader!;
        var fromDlq = await ReadAllAsync(dlqReader);
        await using var rejectedReader = (await Source("unused", rejected: rejected).OpenAsync(new ReprocessRequest(ReprocessSource.Rejected, null, null, null, 100), CancellationToken.None)).Reader!;
        var fromRejected = await ReadAllAsync(rejectedReader);

        var withHeaders = fromDlq.Single(m => Encoding.UTF8.GetString(m.Message.Body.Span) == "with-headers");
        Assert.Equal(("ktm.events.raw", 4, 77L, "unexpected_error", "rules", true), (withHeaders.Message.Topic, withHeaders.Message.Partition, withHeaders.Message.Offset, withHeaders.Reason, withHeaders.Stage, withHeaders.CoordinatesFromHeaders));
        Assert.Equal(new DateTimeOffset(2026, 11, 20, 10, 0, 0, 123, TimeSpan.Zero), withHeaders.Message.Timestamp);
        var plain = fromDlq.Single(m => Encoding.UTF8.GetString(m.Message.Body.Span) == "no-headers");
        Assert.Equal((dlq, false, null), (plain.Message.Topic, plain.CoordinatesFromHeaders, plain.Reason));
        Assert.Equal("rule_x", Assert.Single(fromRejected).Reason);
    }

    private static async Task DeleteAllRecordsAsync(KafkaFixture kafka, string topic)
    {
        using var admin = kafka.NewAdmin();
        using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig { BootstrapServers = kafka.BootstrapServers, GroupId = "wm-" + Guid.NewGuid().ToString("N") }).Build();
        var high = consumer.QueryWatermarkOffsets(new TopicPartition(topic, 0), TimeSpan.FromSeconds(10)).High;
        await admin.DeleteRecordsAsync([new TopicPartitionOffset(topic, 0, high)]);
    }

    [Fact]
    public async Task MissingSourceTopic_IsSourceUnavailable_AndTopicIsNotCreated()
    {
        var missingRaw = "ktm.events.raw." + Guid.NewGuid().ToString("N");
        var missingDlq = "ktm.events.dlq." + Guid.NewGuid().ToString("N");

        await Assert.ThrowsAsync<ReplaySourceException>(async () => await Source(missingRaw).OpenAsync(Raw(T, T2), CancellationToken.None));
        await Assert.ThrowsAsync<ReplaySourceException>(async () =>
            await Source("unused", dlq: missingDlq).OpenAsync(new ReprocessRequest(ReprocessSource.Dlq, null, null, null, 100), CancellationToken.None));

        using var admin = kafka.NewAdmin();
        var names = admin.GetMetadata(TimeSpan.FromSeconds(10)).Topics.Select(t => t.Topic).ToList();
        Assert.DoesNotContain(missingRaw, names);
        Assert.DoesNotContain(missingDlq, names);
    }

    [Fact]
    public async Task Raw_PartitionFullyClearedByRetention_RejectsAnyFrom_UntilANewMessageProvesTheStart()
    {
        var topic = await kafka.CreateTopicAsync("ktm.events.raw");
        for (var i = 0; i < 3; i++)
        {
            await ProduceAsync(topic, "old" + i, T.AddHours(-3));
        }

        await DeleteAllRecordsAsync(kafka, topic);

        foreach (var from in new[] { T.AddDays(-30), T, DateTimeOffset.UtcNow.AddDays(1) })
        {
            var opened = await Source(topic).OpenAsync(Raw(from, from.AddHours(1)), CancellationToken.None);
            Assert.False(opened.IsSuccess);
            Assert.Equal("from", Assert.Single(opened.Errors).Field);
        }

        await ProduceAsync(topic, "fresh", T);

        var tooEarly = await Source(topic).OpenAsync(Raw(T.AddMilliseconds(-1), T2), CancellationToken.None);
        var exact = await Source(topic).OpenAsync(Raw(T, T2), CancellationToken.None);

        Assert.False(tooEarly.IsSuccess);
        Assert.True(exact.IsSuccess);
        await using var reader = exact.Reader!;
        Assert.Equal(["fresh"], (await ReadAllAsync(reader)).Select(m => Encoding.UTF8.GetString(m.Message.Body.Span)));
    }

    [Fact]
    public async Task Raw_UntouchedAndEmptyPartitions_AreCoveredForAnyFrom()
    {
        var untouched = await kafka.CreateTopicAsync("ktm.events.raw");
        await ProduceAsync(untouched, "a", T);
        var empty = await kafka.CreateTopicAsync("ktm.events.raw");

        var first = await Source(untouched).OpenAsync(Raw(T.AddDays(-30), T2), CancellationToken.None);
        var second = await Source(empty).OpenAsync(Raw(T.AddDays(-30), T2), CancellationToken.None);

        Assert.True(first.IsSuccess);
        Assert.True(second.IsSuccess);
        await first.Reader!.DisposeAsync();
        await second.Reader!.DisposeAsync();
    }

    [Fact]
    public async Task Dlq_WithoutFrom_ReadsEvenWhenThePartitionHasBeenCleared()
    {
        var dlq = await kafka.CreateTopicAsync("ktm.events.dlq");
        await ProduceAsync(dlq, "gone", T);
        await DeleteAllRecordsAsync(kafka, dlq);
        await ProduceAsync(dlq, "kept", T2);

        var opened = await Source("unused", dlq: dlq).OpenAsync(new ReprocessRequest(ReprocessSource.Dlq, null, null, null, 100), CancellationToken.None);

        Assert.True(opened.IsSuccess);
        await using var reader = opened.Reader!;
        Assert.Equal(["kept"], (await ReadAllAsync(reader)).Select(m => Encoding.UTF8.GetString(m.Message.Body.Span)));
    }

    [Fact]
    public async Task OpenAsync_WithCancelledToken_ThrowsOperationCanceled()
    {
        var topic = await kafka.CreateTopicAsync("ktm.events.raw");
        await ProduceAsync(topic, "a", T);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await Source(topic).OpenAsync(Raw(T, T2), cts.Token));
    }
}
