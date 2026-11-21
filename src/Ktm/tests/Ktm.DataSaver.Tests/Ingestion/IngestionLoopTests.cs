using Ktm.DataSaver.Core.Batching;
using Ktm.DataSaver.Core.Decoding;
using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Pipeline;
using Ktm.DataSaver.Core.Resilience;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Testing;
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
        // Порт C6 решает судьбу только при ошибке данных; временная ошибка его не касается.
        Assert.Equal(0, h.Handler.Calls);
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

    [Fact]
    public async Task DataError_WithBlockingStub_RetriesSameBatch_StaysDegraded_AndCommitsNothing()
    {
        var h = new Harness(batchSize: 3);
        var blocking = new BlockingWriteFailureHandler();
        var loop = new IngestionLoop(
            h.Source,
            new IngestionPipeline(new FormatStage(new EventDecoder()), []),
            h.Store,
            h.Publisher,
            h.Committer,
            blocking,
            h.Notifier,
            new Batcher<ConsumedMessage>(3, TimeSpan.FromHours(1), h.Time),
            new BackoffPolicy(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30), 2, 0, () => 0.5),
            new IngestionLoopOptions(TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(30)),
            h.Time,
            h.Logger);
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        h.Store.Fail = _ => new FakeDbException("23514");
        var stateAtLastWrite = IngestionState.Running;
        h.Store.OnCall = call =>
        {
            stateAtLastWrite = loop.State;
            if (call >= 3) h.Stop.Cancel();
        };

        // Паузы между повторами ждут таймера: часы в тесте двигает «насос».
        using var done = new CancellationTokenSource();
        var pump = Task.Run(async () =>
        {
            while (!done.IsCancellationRequested)
            {
                h.Time.Advance(TimeSpan.FromSeconds(1));
                await Task.Delay(1);
            }
        });

        try
        {
            await loop.RunAsync(h.Stop.Token).WaitAsync(TimeSpan.FromSeconds(10));
        }
        finally
        {
            await done.CancelAsync();
            await pump;
        }

        Assert.True(h.Store.Writes.Count >= 3);
        Assert.All(h.Store.Writes, w => Assert.Equal(["e-0-0", "e-0-1", "e-0-2"], w));
        Assert.Equal(IngestionState.Degraded, stateAtLastWrite);
        Assert.Empty(h.Committer.Commits);
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

    [Fact]
    public async Task ReceivedAt_IsTakenOncePerBatch_AndNotRecomputedOnWriteRetries()
    {
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2), Valid(0, 3), Valid(0, 4), Valid(0, 5));
        h.Store.Fail = call => call <= 2 ? new IOException("connection reset") : null;
        // Время идёт между попытками: пересчёт на каждой попытке дал бы разные значения.
        h.Store.OnCall = _ => h.Time.Advance(TimeSpan.FromSeconds(7));
        h.StopWhenQueueDrained();

        await h.RunAsync();

        Assert.Equal(4, h.Store.ReceivedAts.Count);
        Assert.Single(h.Store.ReceivedAts.Take(3).Distinct());
        Assert.NotEqual(h.Store.ReceivedAts[0], h.Store.ReceivedAts[3]);
    }

    private sealed class ThrowWithBodyStage : IIngestionStage
    {
        private bool _thrown;

        public IngestionVerdict Evaluate(Ktm.DataSaver.Core.Model.ConsumedMessage message, Ktm.DataSaver.Core.Model.ParsedEvent parsed)
        {
            if (_thrown)
            {
                return IngestionVerdict.Accept;
            }

            _thrown = true;
            throw new InvalidOperationException("bad body: Ivanov Ivan Ivanovich");
        }
    }

    [Fact]
    public async Task PipelineStageException_IsLoggedWithoutMessageTextAndStack_ButStillDegradesAndRetries()
    {
        var h = new Harness(3, new ThrowWithBodyStage());
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        h.StopWhenQueueDrained();

        await h.RunAsync();

        var records = h.Logger.Collector.GetSnapshot();
        Assert.Contains(records, r => r.Level == LogLevel.Error && r.Message.Contains(nameof(InvalidOperationException)));
        Assert.All(records, r =>
        {
            Assert.Null(r.Exception);
            Assert.DoesNotContain("Ivanov", r.Message);
        });
        Assert.Equal(["pause", "write", "resume", "commit"], h.Log);
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

    private static Dictionary<string, string?> Fields(FakeLogRecord record) =>
        record.StructuredState!.ToDictionary(p => p.Key, p => p.Value);

    [Fact]
    public async Task WriteFailure_LogsRecordPerPartitionGroupOfAcceptedEvents_AndHeartbeatAdvances()
    {
        var h = new Harness(batchSize: 5);
        var startedAt = h.Loop.LastHeartbeat;
        // Первое сообщение каждой партиции отбраковано: координаты группы берутся по принятым событиям.
        h.Source.Enqueue(Garbage(0, 5), Valid(0, 6), Valid(0, 7), Garbage(1, 3), Valid(1, 4));
        h.Store.Fail = call => call == 1 ? new IOException("connection reset") : null;
        h.StopWhenQueueDrained();

        await h.RunAsync();

        var records = h.Logger.Collector.GetSnapshot().Where(r => r.Level == LogLevel.Warning && Fields(r).ContainsKey("BatchSize")).ToList();
        var main = Assert.Single(records);
        Assert.Null(main.Exception);
        Assert.Equal("IOException", Fields(main)["ErrorChain"]);
        Assert.Equal(("1", "3"), (Fields(main)["Attempt"], Fields(main)["BatchSize"]));

        var groups = h.Logger.Collector.GetSnapshot().Where(r => Fields(r).ContainsKey("LastOffset")).OrderBy(r => Fields(r)["Partition"]).ToList();
        Assert.Equal(2, groups.Count);
        Assert.All(groups, g => Assert.Null(g.Exception));
        Assert.Equal(
            ("ktm.events.raw", "0", "6", "7", "2", "skud", "e-0-6"),
            (Fields(groups[0])["Topic"], Fields(groups[0])["Partition"], Fields(groups[0])["Offset"], Fields(groups[0])["LastOffset"],
                Fields(groups[0])["Count"], Fields(groups[0])["Source"], Fields(groups[0])["SourceEventId"]));
        Assert.Equal(
            ("1", "4", "4", "1", "skud", "e-1-4"),
            (Fields(groups[1])["Partition"], Fields(groups[1])["Offset"], Fields(groups[1])["LastOffset"],
                Fields(groups[1])["Count"], Fields(groups[1])["Source"], Fields(groups[1])["SourceEventId"]));
        Assert.True(h.Loop.LastHeartbeat > startedAt);
    }

    [Fact]
    public async Task DataWriteFailure_NeverLogsExceptionText_OnlyErrorTypeAndSqlState()
    {
        const string secret = "Key (card)=(0012345678) ФИО Иванов";
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        h.Store.Fail = call => call == 1 ? new LeakyDbException(secret, "23505") : null;
        h.Handler.Decide = ctx => WriteFailureDecision.DeadLetter([ctx.Batch[1]]);
        h.StopWhenQueueDrained();

        await h.RunAsync();

        var records = h.Logger.Collector.GetSnapshot();
        var failure = Assert.Single(records, r => Fields(r).ContainsKey("SqlState"));
        Assert.Equal(("LeakyDbException", "23505"), (Fields(failure)["ErrorType"], Fields(failure)["SqlState"]));
        Assert.All(records, r =>
        {
            Assert.Null(r.Exception);
            Assert.DoesNotContain("Иванов", r.Message);
            Assert.DoesNotContain("0012345678", r.Message);
            Assert.All(r.StructuredState!, p => Assert.DoesNotContain("Иванов", p.Value ?? string.Empty));
        });
    }

    [Fact]
    public async Task StageRejection_PassesOriginToPublisher_ButUnparsedMessageHasNone()
    {
        var h = new Harness(3, new RejectingStage());
        h.Source.Enqueue(Valid(0, 0), Garbage(0, 1));
        h.StopWhenQueueDrained();

        await h.RunAsync();

        Assert.Equal([new EventOrigin("skud", "e-0-0"), null], h.Publisher.Origins);
    }

    private static async Task<IReadOnlyList<FakeLogRecord>> RunWithWriteFailure(Exception failure)
    {
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 0));
        h.Store.Fail = call => call == 1 ? failure : null;
        h.StopWhenQueueDrained();

        await h.RunAsync();

        return h.Logger.Collector.GetSnapshot();
    }

    [Fact]
    public async Task TransientWriteFailure_OfForeignType_LogsOnlyTypeNotMessageOrStack()
    {
        var records = await RunWithWriteFailure(new ForeignWriterException("Failing row: card=0012345678 ФИО Иванов"));

        var failure = Assert.Single(records, r => Fields(r).ContainsKey("ErrorType"));
        Assert.Equal("ForeignWriterException", Fields(failure)["ErrorType"]);
        Assert.Null(Fields(failure)["ErrorChain"]);
        Assert.All(records, r =>
        {
            Assert.Null(r.Exception);
            Assert.DoesNotContain("Иванов", r.Message);
            Assert.All(r.StructuredState!, p => Assert.DoesNotContain("0012345678", p.Value ?? string.Empty));
        });
    }

    [Fact]
    public async Task FatalConfigWriteFailure_OfDbError_LogsSqlStateConstraintAndTableWithoutMessage()
    {
        var records = await RunWithWriteFailure(new LeakyDbLocationException("Detail: Key (card)=(0012345678) Иванов", "28P01", "ck_events_source", "events"));

        var failure = Assert.Single(records, r => Fields(r).ContainsKey("SqlState"));
        Assert.Equal(("28P01", "ck_events_source", "events"), (Fields(failure)["SqlState"], Fields(failure)["ConstraintName"], Fields(failure)["TableName"]));
        Assert.All(records, r =>
        {
            Assert.Null(r.Exception);
            Assert.DoesNotContain("Иванов", r.Message);
            Assert.All(r.StructuredState!, p => Assert.DoesNotContain("Иванов", p.Value ?? string.Empty));
        });
    }

    [Fact]
    public async Task TransientWriteFailure_OfSocketError_LogsErrorCodeAndTypeChain()
    {
        var failure = new IOException("outer", new System.Net.Sockets.SocketException((int)System.Net.Sockets.SocketError.ConnectionRefused));

        var records = await RunWithWriteFailure(failure);

        var record = Assert.Single(records, r => Fields(r).ContainsKey("ErrorChain"));
        Assert.Equal("IOException > SocketException", Fields(record)["ErrorChain"]);
        Assert.Contains("ConnectionRefused", Fields(record)["ErrorCode"]);
        Assert.All(records, r => Assert.Null(r.Exception));
    }
}
