using Ktm.DataSaver.Application.Reprocessing;

namespace Ktm.DataSaver.Tests.Reprocessing;

/// <summary>Оценка остатка и прогресс по смещениям (ADR-0006 п. 2.6): оценка ограничена снятыми границами и скоростью задачи.</summary>
public class ReprocessJobTests
{
    private static readonly DateTimeOffset Now = new(2026, 11, 24, 0, 0, 0, TimeSpan.Zero);

    private static ReprocessJob Job(int maxRate = 100) =>
        new(Guid.NewGuid(), new ReprocessRequest(ReprocessSource.Dlq, null, null, null, maxRate), 100, Now);

    private static ReplayPartition[] Partition(long end) => [new("ktm.events.dlq", 0, 0, end)];

    [Fact]
    public void EstimatedRemaining_IsOnlyForRunningJobWithKnownTotal()
    {
        var queued = Job();
        var empty = Job();
        empty.MarkRunning(Now, Partition(0));
        var finished = Job();
        finished.MarkRunning(Now, Partition(10));
        finished.Finish(ReprocessJobState.Completed, ReprocessOutcome.Succeeded, null, Now);

        Assert.Null(queued.Snapshot().EstimatedRemaining);
        Assert.Null(empty.Snapshot().EstimatedRemaining);
        Assert.Null(finished.Snapshot().EstimatedRemaining);
    }

    [Fact]
    public void EstimatedRemaining_IsRemainingOffsetsDividedByMaxRate_AndZeroAfterPartitionsAreDone()
    {
        var slow = Job(maxRate: 100);
        var fast = Job(maxRate: 300);
        foreach (var job in new[] { slow, fast })
        {
            job.MarkRunning(Now, Partition(1000));
            job.Advance("ktm.events.dlq", 0, 399);
        }

        Assert.Equal(TimeSpan.FromSeconds(6), slow.Snapshot().EstimatedRemaining);
        Assert.Equal(TimeSpan.FromSeconds(2), fast.Snapshot().EstimatedRemaining);

        slow.MarkPartitionsDone();

        Assert.Equal(TimeSpan.Zero, slow.Snapshot().EstimatedRemaining);
    }

    [Fact]
    public void Advance_NeverGoesBackwardsAndNeverPastTheSnapshotEnd()
    {
        var job = Job();
        job.MarkRunning(Now, Partition(1000));

        job.Advance("ktm.events.dlq", 0, 399);
        job.Advance("ktm.events.dlq", 0, 100);
        Assert.Equal(400, job.Snapshot().Partitions.Single().NextOffset);

        job.Advance("ktm.events.dlq", 0, 5000);
        Assert.Equal(1000, job.Snapshot().Partitions.Single().NextOffset);
    }
}
