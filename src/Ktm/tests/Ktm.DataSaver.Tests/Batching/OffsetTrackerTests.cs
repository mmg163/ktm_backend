using Ktm.DataSaver.Application.Batching;

namespace Ktm.DataSaver.Tests.Batching;

public class OffsetTrackerTests
{
    [Fact]
    public void Commit_offset_is_last_offset_plus_one_per_partition_including_rejected_tail()
    {
        var tracker = new OffsetTracker();

        // Условно: 10, 11 записаны, 12 отбракован - в расчёте участвуют все.
        tracker.Track(new PartitionOffset("t", 0, 10));
        tracker.Track(new PartitionOffset("t", 0, 11));
        tracker.Track(new PartitionOffset("t", 0, 12));
        tracker.Track(new PartitionOffset("t", 1, 5));
        tracker.Track(new PartitionOffset("other", 0, 100));

        Assert.Equivalent(
            new[]
            {
                new PartitionOffset("t", 0, 13),
                new PartitionOffset("t", 1, 6),
                new PartitionOffset("other", 0, 101),
            },
            tracker.GetOffsetsToCommit());
    }

    [Fact]
    public void Batch_of_only_rejected_messages_still_advances_offset()
    {
        var tracker = new OffsetTracker();
        tracker.Track(new PartitionOffset("t", 2, 7));

        Assert.Equal(new PartitionOffset("t", 2, 8), Assert.Single(tracker.GetOffsetsToCommit()));
    }

    [Fact]
    public void Out_of_order_offset_does_not_lower_position()
    {
        var tracker = new OffsetTracker();
        tracker.Track(new PartitionOffset("t", 0, 20));
        tracker.Track(new PartitionOffset("t", 0, 15));

        Assert.Equal(21, Assert.Single(tracker.GetOffsetsToCommit()).Offset);
    }

    [Fact]
    public void Reset_clears_state_after_commit()
    {
        var tracker = new OffsetTracker();
        tracker.Track(new PartitionOffset("t", 0, 1));

        tracker.Reset();

        Assert.True(tracker.IsEmpty);
        Assert.Empty(tracker.GetOffsetsToCommit());
    }

    [Fact]
    public void Revoke_removes_only_given_partitions()
    {
        var tracker = new OffsetTracker();
        tracker.Track(new PartitionOffset("t", 0, 1));
        tracker.Track(new PartitionOffset("t", 1, 2));

        tracker.Revoke([new TopicPartitionKey("t", 0), new TopicPartitionKey("t", 9)]);

        Assert.Equal(new PartitionOffset("t", 1, 3), Assert.Single(tracker.GetOffsetsToCommit()));
    }

    [Fact]
    public void Track_after_revoke_or_reset_starts_partition_from_scratch()
    {
        var tracker = new OffsetTracker();
        tracker.Track(new PartitionOffset("t", 0, 50));
        tracker.Revoke([new TopicPartitionKey("t", 0)]);

        // Контракт: старая позиция не возвращается; новое Track (повторное назначение) начинает с нуля.
        tracker.Track(new PartitionOffset("t", 0, 3));
        Assert.Equal(4, Assert.Single(tracker.GetOffsetsToCommit()).Offset);

        tracker.Reset();
        tracker.Track(new PartitionOffset("t", 0, 1));
        Assert.Equal(2, Assert.Single(tracker.GetOffsetsToCommit()).Offset);
    }
}
