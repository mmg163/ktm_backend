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
        Assert.Contains(P(7), h.Source.ResumeCalls.Single());
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
        h.Source.OnIdle = n =>
        {
            h.Source.ScheduleRevoke(P(1));
            h.Stop.Cancel();
        };

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
        h.Source.OnIdle = n =>
        {
            h.Source.ScheduleRevoke(P(1));
            h.Stop.Cancel();
        };

        await h.RunAsync();

        Assert.Empty(h.Log);
    }

    [Fact]
    public async Task RevokeAndAssignOfSamePartitionInOneConsume_KeepsTheReturnedMessage()
    {
        var h = new Harness(batchSize: 10);
        h.Source.Enqueue(Valid(1, 0), Valid(1, 5));
        // Во время второго Consume: партиция отозвана и назначена снова, и пришло сообщение уже нового назначения.
        h.Source.OnDelivered = n =>
        {
            if (n == 2)
            {
                h.Source.ScheduleRevoke(P(1));
                h.Source.ScheduleAssign(P(1));
            }
        };
        h.StopWhenQueueDrained();

        await h.RunAsync();

        // Старое сообщение (до отзыва) выброшено, новое записано и закоммичено.
        Assert.Equal(["e-1-5"], h.Store.Writes.Single());
        Assert.Equal([Next(1, 6)], h.Committer.Commits.Single());
    }

    private sealed class ThrowOnceStage : IIngestionStage
    {
        private bool _thrown;

        public IngestionVerdict Evaluate(Ktm.DataSaver.Core.Model.ConsumedMessage message, Ktm.DataSaver.Core.Model.ParsedEvent parsed)
        {
            if (_thrown)
            {
                return IngestionVerdict.Accept;
            }

            _thrown = true;
            throw new TimeoutException("C5 is down");
        }
    }

    [Fact]
    public async Task PipelineStageException_IsBatchFailure_RetriedWithoutLossOrRejection()
    {
        var h = new Harness(3, new ThrowOnceStage());
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        h.StopWhenQueueDrained();

        await h.RunAsync();

        Assert.Equal(["pause", "write", "resume", "commit"], h.Log);
        Assert.Equal(["e-0-0", "e-0-1", "e-0-2"], h.Store.Writes.Single());
        Assert.Empty(h.Publisher.Published);
    }

    [Fact]
    public async Task DeadLetterWithoutProgress_BacksOffInsteadOfSpinning()
    {
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        h.Store.Fail = call => call <= 2 ? new FakeDbException("23514") : null;
        // C6 каждый раз «уводит» одно и то же сообщение: первый раз это прогресс, второй - нет.
        h.Handler.Decide = ctx => WriteFailureDecision.DeadLetter([ctx.Batch[0]]);
        var polledAtCall = new Dictionary<int, int>();
        h.Store.OnCall = call => polledAtCall[call] = h.Source.ConsumeCalls;
        h.StopWhenQueueDrained();

        await h.RunAsync();

        Assert.Equal(["write", "write", "pause", "write", "resume", "commit"], h.Log);
        Assert.Equal(polledAtCall[2], polledAtCall[1]);
        Assert.True(polledAtCall[3] > polledAtCall[2]);
        Assert.Equal([Next(0, 3)], h.Committer.Commits.Single());
    }

    [Fact]
    public async Task DeadLetterOfMessagesOutsideTheBatch_IsTreatedAsRetry()
    {
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        h.Store.Fail = call => call == 1 ? new FakeDbException("23514") : null;
        h.Handler.Decide = _ => WriteFailureDecision.DeadLetter([Valid(9, 9)]);
        h.StopWhenQueueDrained();

        await h.RunAsync();

        Assert.Equal(["write", "pause", "write", "resume", "commit"], h.Log);
        Assert.Equal(3, h.Store.Writes[1].Length);
    }

    [Fact]
    public async Task StopAlreadySignalled_RetriesImmediatelyOnce_ThenWaitsWithinDeadline()
    {
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        h.Store.Fail = call => call <= 2 ? new IOException("down") : null;
        h.Store.OnCall = call => { if (call == 1) h.Stop.Cancel(); };

        await h.RunWithClockPumpAsync();

        Assert.Equal(3, h.Store.Writes.Count);
        Assert.Single(h.Committer.Commits);
    }

    [Fact]
    public async Task HungWriteInFlight_IsCancelledByDeadlineCountedFromStopSignal_WithoutCommit()
    {
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        h.Store.Hang = true;
        h.Store.OnCall = _ => h.Stop.Cancel();

        await h.RunWithClockPumpAsync();

        Assert.Empty(h.Committer.Commits);
    }

    [Fact]
    public async Task FailureHandlerCancelledByDeadline_DoesNotLeakOperationCanceledException()
    {
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        h.Store.Fail = _ => new FakeDbException("23514");
        h.Store.OnCall = call => { if (call == 1) h.Stop.Cancel(); };
        h.Handler.Hang = true;

        await h.RunWithClockPumpAsync();

        Assert.Empty(h.Committer.Commits);
    }

    [Fact]
    public async Task CommitFailure_IsLoggedAndLoopContinuesWithNextBatch()
    {
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2), Valid(0, 3), Valid(0, 4), Valid(0, 5));
        h.Committer.Fail = call => call == 1 ? new InvalidOperationException("partition lost") : null;
        h.StopWhenQueueDrained();

        await h.RunAsync();

        Assert.Equal(2, h.Store.Writes.Count);
        Assert.Equal([Next(0, 6)], h.Committer.Commits[1]);
    }

    [Fact]
    public async Task SourceReturningNullImmediately_DoesNotMakeLoopSpin()
    {
        var h = new Harness();
        h.Source.ReturnNullImmediately = true;

        var run = h.RunAsync();
        await Task.Delay(100);
        var callsWhileIdle = h.Source.ConsumeCalls;
        await h.Stop.CancelAsync();
        await run;

        // Время в тесте не идёт: без защиты цикл накрутил бы тысячи опросов.
        Assert.True(callsWhileIdle <= 2, $"loop spun: {callsWhileIdle} Consume calls");
    }

    [Fact]
    public async Task RunAsync_CannotBeStartedTwice()
    {
        var h = new Harness();
        h.StopWhenQueueDrained();
        await h.RunAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(() => h.Loop.RunAsync(CancellationToken.None));
    }

    [Fact]
    public async Task WriteFailure_LogsBatchCoordinatesWithoutBodies_AndHeartbeatAdvances()
    {
        var h = new Harness();
        var startedAt = h.Loop.LastHeartbeat;
        h.Source.Enqueue(Valid(0, 10), Valid(0, 11), Valid(1, 4));
        h.Store.Fail = call => call == 1 ? new IOException("connection reset") : null;
        h.StopWhenQueueDrained();

        await h.RunAsync();

        var warning = Assert.Single(h.Logger.Collector.GetSnapshot(), r => r.Message.StartsWith("Write failed (transient)", StringComparison.Ordinal));
        Assert.Contains("ktm.events.raw[0] offsets 10..11 (2 msgs, first source=skud sourceEventId=e-0-10)", warning.Message);
        Assert.Contains("ktm.events.raw[1] offsets 4..4 (1 msgs, first source=skud sourceEventId=e-1-4)", warning.Message);
        Assert.DoesNotContain("connection reset", warning.Message);
        Assert.True(h.Loop.LastHeartbeat > startedAt);
    }
}
