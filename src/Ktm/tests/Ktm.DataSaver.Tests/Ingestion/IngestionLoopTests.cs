using Ktm.DataSaver.Core.Batching;
using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Core.Pipeline;
using static Ktm.DataSaver.Tests.Ingestion.Harness;

namespace Ktm.DataSaver.Tests.Ingestion;

public class IngestionLoopTests
{
    [Fact]
    public async Task Batch_IsWrittenThenRejectedPublishedThenCommitted_WithOffsetsOfAllMessages()
    {
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 10), Garbage(1, 5), Valid(0, 11));
        h.StopWhenQueueDrained();

        await h.RunAsync();

        Assert.Equal(["write", "publish", "commit"], h.Log);
        Assert.Equal(["e-0-10", "e-0-11"], h.Store.Writes.Single());
        Assert.Equal(["1@5"], h.Publisher.Published.Single());
        // Отбракованное сообщение входит в расчёт offset: иначе оно блокирует партицию.
        Assert.Equal([Next(0, 12), Next(1, 6)], h.Committer.Commits.Single().OrderBy(o => o.Partition));
    }

    [Fact]
    public async Task BatchOfOnlyRejected_IsNotWritten_ButIsCommitted()
    {
        var h = new Harness();
        h.Source.Enqueue(Garbage(0, 0), Garbage(0, 1), Garbage(1, 7));
        h.StopWhenQueueDrained();

        await h.RunAsync();

        Assert.Equal(["publish", "commit"], h.Log);
        Assert.Equal([Next(0, 2), Next(1, 8)], h.Committer.Commits.Single().OrderBy(o => o.Partition));
    }

    [Fact]
    public async Task TransientWriteError_PausesRetriesSameBatchKeepsPolling_ThenResumesAndCommits()
    {
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(1, 0));
        h.Store.Fail = call => call <= 2 ? new IOException("connection reset") : null;
        var polledAtCall = new Dictionary<int, int>();
        var stateAtCall = new Dictionary<int, IngestionState>();
        h.Store.OnCall = call =>
        {
            polledAtCall[call] = h.Source.ConsumeCalls;
            stateAtCall[call] = h.Loop.State;
        };
        h.StopWhenQueueDrained();

        await h.RunAsync();

        // Нет commit до успешной записи; Pause после первого сбоя, Resume - после успеха и до commit.
        Assert.Equal(["write", "pause", "write", "write", "resume", "commit"], h.Log);
        Assert.All(h.Store.Writes, w => Assert.Equal(["e-0-0", "e-0-1", "e-1-0"], w));
        Assert.Equal(h.Source.CurrentAssignment, h.Source.PauseCalls.Single());
        // Во время ожидания Consume продолжает опрашиваться (иначе вылет из группы по max.poll.interval).
        Assert.True(polledAtCall[2] > polledAtCall[1] && polledAtCall[3] > polledAtCall[2]);
        Assert.Equal(IngestionState.Degraded, stateAtCall[3]);
        Assert.Equal(IngestionState.Running, h.Loop.State);
        Assert.Equal(0, h.Loop.RetryCount);
        Assert.Equal([Next(0, 2), Next(1, 1)], h.Committer.Commits.Single().OrderBy(o => o.Partition));
    }

    [Fact]
    public async Task PartitionAssignedWhileDegraded_IsPausedImmediately()
    {
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        h.Store.Fail = call => call == 1 ? new IOException("down") : null;
        h.Source.OnIdle = n =>
        {
            if (n == 3) h.Source.ScheduleAssign(P(7));
            if (n >= 5) h.Stop.Cancel();
        };

        await h.RunAsync();

        Assert.Contains(h.Source.PauseCalls, call => call.Contains(P(7)));
        Assert.Equal(["write", "pause", "pause", "write", "resume", "commit"], h.Log);
    }

    [Fact]
    public async Task FatalConfigError_IsRetriedInDegradedState_AndNotifiesAdminOnEveryAttempt()
    {
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        h.Store.Fail = call => call <= 2 ? new FakeDbException("42501") : null;
        h.StopWhenQueueDrained();

        await h.RunAsync();

        Assert.Equal(2, h.Notifier.Count);
        Assert.Equal(["write", "pause", "write", "write", "resume", "commit"], h.Log);
    }

    [Fact]
    public async Task DataError_DeadLetteredMessageIsExcludedFromRewrite_ButItsOffsetIsCommitted()
    {
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        h.Store.Fail = call => call == 1 ? new FakeDbException("23514") : null;
        h.Handler.Decide = ctx => WriteFailureDecision.DeadLetter([ctx.Batch[1]]);
        h.StopWhenQueueDrained();

        await h.RunAsync();

        Assert.Equal(["e-0-0", "e-0-2"], h.Store.Writes[1]);
        Assert.Equal([Next(0, 3)], h.Committer.Commits.Single());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RejectedPublishFailure_NeverCommitsBeforeSuccess_AndDoesNotRewriteBatch(bool portThrows)
    {
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 0), Garbage(0, 1), Valid(0, 2));
        if (portThrows)
        {
            h.Publisher.Throw = call => call == 1 ? new InvalidOperationException("broker") : null;
        }
        else
        {
            h.Publisher.Result = call => call == 1 ? RejectionPublishResult.Failure("acks timeout") : RejectionPublishResult.Success;
        }

        h.StopWhenQueueDrained();

        await h.RunAsync();

        Assert.Equal(["write", "publish", "pause", "publish", "resume", "commit"], h.Log);
        Assert.Single(h.Store.Writes);
        Assert.Equal([Next(0, 3)], h.Committer.Commits.Single());
    }

    [Fact]
    public async Task RejectedPublishNeverSucceeds_StopEndsLoopWithoutCommit()
    {
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 0), Garbage(0, 1), Valid(0, 2));
        h.Publisher.Result = _ => RejectionPublishResult.Failure("broker down");
        h.Source.OnIdle = n => { if (n >= 2) h.Stop.Cancel(); };

        await h.RunWithClockPumpAsync();

        Assert.Empty(h.Committer.Commits);
        Assert.DoesNotContain("commit", h.Log);
    }

    [Fact]
    public async Task Stop_FinishesAccumulatedBatchWithWriteAndCommit()
    {
        var h = new Harness(batchSize: 10);
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1));
        h.StopWhenQueueDrained();

        await h.RunAsync();

        Assert.Equal(["write", "commit"], h.Log);
        Assert.Equal([Next(0, 2)], h.Committer.Commits.Single());
    }

    [Fact]
    public async Task Stop_WhileDatabaseIsDown_ExitsAtDeadlineWithoutCommit()
    {
        var h = new Harness(batchSize: 10);
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1));
        h.Store.Fail = _ => new IOException("db unreachable");
        h.StopWhenQueueDrained();

        await h.RunWithClockPumpAsync();

        Assert.True(h.Store.Writes.Count >= 1);
        Assert.Empty(h.Committer.Commits);
    }

    [Fact]
    public async Task Stop_DuringBackoffWait_TriesOnceMoreImmediatelyAndCommitsOnSuccess()
    {
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        h.Store.Fail = call => call == 1 ? new IOException("down") : null;
        h.Source.OnIdle = n => { if (n == 2) h.Stop.Cancel(); };

        await h.RunAsync();

        Assert.Equal(["write", "pause", "write", "resume", "commit"], h.Log);
    }

    [Fact]
    public async Task RevokedPartition_MessagesAccumulatedForIt_AreDroppedWithoutWriteOrOffset()
    {
        var h = new Harness(batchSize: 10);
        h.Source.Enqueue(Valid(0, 0), Valid(1, 0), Valid(1, 1));
        h.Source.OnDelivered = n => { if (n == 3) h.Source.ScheduleRevoke(P(1)); };
        h.StopWhenQueueDrained();

        await h.RunAsync();

        Assert.Equal(["e-0-0"], h.Store.Writes.Single());
        Assert.Equal([Next(0, 1)], h.Committer.Commits.Single());
    }

    [Fact]
    public async Task RevokedPartition_DuringRetry_IsExcludedFromRewriteAndCommit()
    {
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 0), Valid(1, 0), Valid(0, 1));
        h.Store.Fail = call => call == 1 ? new IOException("down") : null;
        h.Store.OnCall = call => { if (call == 1) h.Source.ScheduleRevoke(P(1)); };
        h.StopWhenQueueDrained();

        await h.RunAsync();

        Assert.Equal(["e-0-0", "e-1-0", "e-0-1"], h.Store.Writes[0]);
        Assert.Equal(["e-0-0", "e-0-1"], h.Store.Writes[1]);
        Assert.Equal([Next(0, 2)], h.Committer.Commits.Single());
    }

    [Fact]
    public async Task AllMessagesRevoked_NothingIsCommitted()
    {
        var h = new Harness(batchSize: 10);
        h.Source.Enqueue(Valid(1, 0), Garbage(1, 1));
        h.Source.OnDelivered = n => { if (n == 2) h.Source.ScheduleRevoke(P(1)); };
        h.StopWhenQueueDrained();

        await h.RunAsync();

        Assert.Empty(h.Log);
    }
}
