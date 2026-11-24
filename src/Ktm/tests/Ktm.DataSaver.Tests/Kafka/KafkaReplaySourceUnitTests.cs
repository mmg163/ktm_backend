using System.Text;
using Confluent.Kafka;
using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Pipeline;
using Ktm.DataSaver.Core.Reprocessing;
using Ktm.DataSaver.Core.Resilience;
using Ktm.DataSaver.Infrastructure.Kafka;
using Microsoft.Extensions.Time.Testing;

namespace Ktm.DataSaver.Tests.Kafka;

/// <summary>Источник повторной обработки без брокера: метаданные топика, проверка retention, окно периода, бюджет открытия, разбор заголовков.</summary>
public class KafkaReplaySourceUnitTests
{
    private static readonly DateTimeOffset T1 = new(2026, 11, 20, 10, 0, 0, 123, TimeSpan.Zero);
    private static readonly DateTimeOffset T2 = T1.AddMinutes(10);

    private static PartitionMetadata Partition(int id, ErrorCode error = ErrorCode.NoError) =>
        new(id, 0, [0], [0], new Error(error));

    public static TheoryData<TopicMetadata?> BrokenMetadata => new()
    {
        { null },
        { new TopicMetadata("ktm.events.raw", [], new Error(ErrorCode.UnknownTopicOrPart)) },
        { new TopicMetadata("ktm.events.raw", [], new Error(ErrorCode.LeaderNotAvailable)) },
        { new TopicMetadata("ktm.events.raw", [], new Error(ErrorCode.NoError)) },
    };

    [Theory]
    [MemberData(nameof(BrokenMetadata))]
    public void ResolvePartitions_MissingOrBrokenTopic_IsSourceFailure_NotArgumentError(TopicMetadata? metadata)
    {
        var error = Assert.Throws<ReplaySourceException>(() => KafkaReplaySource.ResolvePartitions("ktm.events.raw", metadata));

        Assert.DoesNotContain("does not exist", error.Message);
    }

    [Fact]
    public void ResolvePartitions_MissingTopic_NamesTheConfiguredTopic_AndOkMetadataGivesSortedIds()
    {
        var missing = Assert.Throws<ReplaySourceException>(() => KafkaReplaySource.ResolvePartitions("ktm.events.dlq", null));
        var ok = KafkaReplaySource.ResolvePartitions("ktm.events.raw", new TopicMetadata("ktm.events.raw", [Partition(2), Partition(0), Partition(1)], new Error(ErrorCode.NoError)));

        Assert.Equal("Kafka: topic 'ktm.events.dlq' not found", missing.Message);
        Assert.Equal([0, 1, 2], ok);
    }

    private static PartitionRetention Kept(int partition, DateTimeOffset firstMessage) => new(partition, 5, 9, firstMessage);

    [Fact]
    public void CheckRetention_PartitionsNeverCleared_CoverAnyFrom()
    {
        PartitionRetention[] partitions = [new(0, 0, 0, null), new(1, 0, 100, null)];

        Assert.Empty(KafkaReplaySource.CheckRetention("t", T1.AddYears(-5), partitions));
    }

    [Fact]
    public void CheckRetention_FromBeforeFirstRetainedMessage_IsRejectedWithItsTime_FromAtOrAfterIsAccepted()
    {
        PartitionRetention[] partitions = [Kept(0, T1)];

        var error = Assert.Single(KafkaReplaySource.CheckRetention("t", T1.AddMilliseconds(-1), partitions));

        Assert.Equal("from", error.Field);
        Assert.Contains("2026-11-20T10:00:00.123Z", error.Message);
        Assert.Empty(KafkaReplaySource.CheckRetention("t", T1, partitions));
        Assert.Empty(KafkaReplaySource.CheckRetention("t", T1.AddMilliseconds(1), partitions));
    }

    [Fact]
    public void CheckRetention_SeveralPartitions_RequiresTheLatestFirstMessageTime()
    {
        PartitionRetention[] partitions = [Kept(0, T1), Kept(1, T2), new(2, 0, 50, null)];

        var error = Assert.Single(KafkaReplaySource.CheckRetention("t", T1.AddMinutes(5), partitions));

        Assert.Contains("2026-11-20T10:10:00.123Z", error.Message);
        Assert.Empty(KafkaReplaySource.CheckRetention("t", T2, partitions));
    }

    [Fact]
    public void CheckRetention_PartitionClearedCompletely_CannotBeProvenForAnyFrom_EvenNextToAHealthyPartition()
    {
        PartitionRetention[] cleared = [new(2, 5, 5, null)];
        PartitionRetention[] withHealthy = [Kept(0, T1), new(2, 5, 5, null)];

        foreach (var from in new[] { T1.AddYears(-1), DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddDays(1) })
        {
            var error = Assert.Single(KafkaReplaySource.CheckRetention("topic-x", from, cleared));
            Assert.Equal("from", error.Field);
            Assert.Contains("2", error.Message);
            Assert.Contains("topic-x", error.Message);
            Assert.DoesNotContain("Z", error.Message);
            Assert.Single(KafkaReplaySource.CheckRetention("topic-x", from, withHealthy));
        }
    }

    [Fact]
    public void CheckRetention_FirstMessageWithoutKnownTime_IsUnverifiable_AndWinsOverTooEarly()
    {
        PartitionRetention[] partitions = [Kept(0, T2), new(3, 5, 9, null), new(5, 7, 7, null)];

        var error = Assert.Single(KafkaReplaySource.CheckRetention("t", T1, partitions));

        Assert.Contains("partition(s) 3, 5", error.Message);
        Assert.Contains("cannot be verified", error.Message);
    }

    [Fact]
    public void CheckRetention_PeriodEntirelyBeforeRetainedData_IsRejectedByFromAlone()
    {
        PartitionRetention[] partitions = [Kept(0, T2)];

        var errors = KafkaReplaySource.CheckRetention("t", T1.AddMinutes(-20), partitions);

        Assert.Equal("from", Assert.Single(errors).Field);
    }

    [Fact]
    public void InWindow_IsHalfOpen_NotAvailableIsIncludedWhenAPeriodIsSet_AndNoPeriodMeansEverything()
    {
        Timestamp At(DateTimeOffset t) => new(t);

        Assert.False(KafkaReplaySource.InWindow(At(T1.AddMilliseconds(-1)), T1, T2));
        Assert.True(KafkaReplaySource.InWindow(At(T1), T1, T2));
        Assert.True(KafkaReplaySource.InWindow(At(T2.AddMilliseconds(-1)), T1, T2));
        Assert.False(KafkaReplaySource.InWindow(At(T2), T1, T2));
        Assert.True(KafkaReplaySource.InWindow(new Timestamp(0, TimestampType.NotAvailable), T1, T2));
        Assert.True(KafkaReplaySource.InWindow(At(T1.AddYears(-9)), null, null));
    }

    [Fact]
    public void OpenBudget_GivesPerCallTimeoutWhileEnough_ThenTheRemainder_ThenFails()
    {
        var time = new FakeTimeProvider();
        var budget = new OpenBudget(time, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(10), CancellationToken.None);

        Assert.Equal(TimeSpan.FromSeconds(10), budget.NextTimeout());
        time.Advance(TimeSpan.FromSeconds(25));
        Assert.Equal(TimeSpan.FromSeconds(5), budget.NextTimeout());
        time.Advance(TimeSpan.FromSeconds(5));

        Assert.Throws<ReplaySourceException>(budget.NextTimeout);
    }

    [Fact]
    public void OpenBudget_CancelledToken_StopsBeforeAnyCall()
    {
        using var cts = new CancellationTokenSource();
        var budget = new OpenBudget(new FakeTimeProvider(), TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(10), cts.Token);
        cts.Cancel();

        Assert.Throws<OperationCanceledException>(budget.NextTimeout);
    }

    private static readonly byte[] Body = Encoding.UTF8.GetBytes("""{"k":"v"}""");
    private static readonly DateTimeOffset SourceTime = new(2026, 11, 24, 9, 30, 15, 123, TimeSpan.Zero);
    private static readonly DateTimeOffset ActualTime = SourceTime.AddHours(1);

    private static ConsumedMessage Original() => new("ktm.events.raw", 3, 42, Encoding.UTF8.GetBytes("skud:7"), Body, SourceTime);

    private static ConsumeResult<byte[], byte[]> Stored(Message<byte[], byte[]> message, string topic, int partition, long offset)
    {
        message.Timestamp = new Timestamp(ActualTime);
        return new ConsumeResult<byte[], byte[]> { Topic = topic, Partition = partition, Offset = offset, Message = message };
    }

    [Fact]
    public void MapMessage_DlqRoundTrip_RestoresSourceCoordinatesReasonAndStage()
    {
        var info = new DeadLetterInfo(DeadLetterReasons.WriteDataError, "write", "System.Exception", "23514", "ck", "events", 3, ActualTime);
        var written = KafkaDeadLetterPublisher.ToMessage(new DeadLetteredMessage(Original(), null, info));

        var mapped = KafkaReplaySource.MapMessage(Stored(written, "ktm.events.dlq", 1, 900), ReprocessSource.Dlq);

        Assert.Equal(("ktm.events.raw", 3, 42L, SourceTime), (mapped.Message.Topic, mapped.Message.Partition, mapped.Message.Offset, mapped.Message.Timestamp));
        Assert.Equal(("write_data_error", "write", true), (mapped.Reason, mapped.Stage, mapped.CoordinatesFromHeaders));
        Assert.Equal(("ktm.events.dlq", 1, 900L), (mapped.ActualTopic, mapped.ActualPartition, mapped.ActualOffset));
        Assert.Equal(Body, mapped.Message.Body.ToArray());
    }

    [Fact]
    public void MapMessage_RejectedRoundTrip_RestoresSourceCoordinatesAndReason()
    {
        var rejected = new RejectedMessage(Original(), new RejectionInfo(RejectionKind.Rule, "rule_matched", "detail", "r-1"));
        var written = KafkaRejectionPublisher.ToMessage(rejected);

        var mapped = KafkaReplaySource.MapMessage(Stored(written, "ktm.events.rejected", 0, 7), ReprocessSource.Rejected);

        Assert.Equal(("ktm.events.raw", 3, 42L, SourceTime), (mapped.Message.Topic, mapped.Message.Partition, mapped.Message.Offset, mapped.Message.Timestamp));
        Assert.Equal(("rule_matched", null, true), (mapped.Reason, mapped.Stage, mapped.CoordinatesFromHeaders));
        Assert.Equal(("ktm.events.rejected", 0, 7L), (mapped.ActualTopic, mapped.ActualPartition, mapped.ActualOffset));
    }

    [Fact]
    public void MapMessage_MissingOrBrokenCoordinates_FallBackToActualOnes_MissingTimestampToMessageTime()
    {
        var info = new DeadLetterInfo(DeadLetterReasons.WriteDataError, "write", "System.Exception", null, "d", null, 3, ActualTime);
        Message<byte[], byte[]> Dlq() => KafkaDeadLetterPublisher.ToMessage(new DeadLetteredMessage(Original(), null, info));

        var noHeaders = KafkaReplaySource.MapMessage(Stored(new Message<byte[], byte[]> { Value = Body }, "ktm.events.dlq", 1, 900), ReprocessSource.Dlq);
        foreach (var broken in new[] { "abc", "-1" })
        {
            var message = Dlq();
            message.Headers.Remove(KafkaHeaders.SourcePartition);
            message.Headers.Add(KafkaHeaders.SourcePartition, Encoding.UTF8.GetBytes(broken));
            var mapped = KafkaReplaySource.MapMessage(Stored(message, "ktm.events.dlq", 1, 900), ReprocessSource.Dlq);

            Assert.Equal((false, "ktm.events.dlq", 1, 900L), (mapped.CoordinatesFromHeaders, mapped.Message.Topic, mapped.Message.Partition, mapped.Message.Offset));
        }

        var noTime = Dlq();
        noTime.Headers.Remove(KafkaHeaders.SourceTimestamp);
        var withoutTimestamp = KafkaReplaySource.MapMessage(Stored(noTime, "ktm.events.dlq", 1, 900), ReprocessSource.Dlq);

        Assert.Equal((false, "ktm.events.dlq", 900L), (noHeaders.CoordinatesFromHeaders, noHeaders.Message.Topic, noHeaders.Message.Offset));
        Assert.Equal((true, 42L, ActualTime), (withoutTimestamp.CoordinatesFromHeaders, withoutTimestamp.Message.Offset, withoutTimestamp.Message.Timestamp));
    }

    [Fact]
    public void MapMessage_Raw_UsesActualCoordinates_WithoutReasonOrStage()
    {
        var info = new DeadLetterInfo(DeadLetterReasons.WriteDataError, "write", "System.Exception", null, "d", null, 3, ActualTime);
        var headersPresent = KafkaDeadLetterPublisher.ToMessage(new DeadLetteredMessage(Original(), null, info));

        var mapped = KafkaReplaySource.MapMessage(Stored(headersPresent, "ktm.events.raw", 2, 11), ReprocessSource.Raw);

        Assert.Equal(("ktm.events.raw", 2, 11L, true, null, null), (mapped.Message.Topic, mapped.Message.Partition, mapped.Message.Offset, mapped.CoordinatesFromHeaders, mapped.Reason, mapped.Stage));
        Assert.Equal(ActualTime, mapped.Message.Timestamp);
    }
}
