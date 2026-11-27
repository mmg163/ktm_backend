using Ktm.DataSaver.Application.Decoding;
using Ktm.DataSaver.Application.Ingestion;
using Ktm.DataSaver.Application.Model;
using Ktm.DataSaver.Application.Pipeline;
using Ktm.DataSaver.Application.Resilience;
using Microsoft.Extensions.Logging;
using static Ktm.DataSaver.Tests.Ingestion.Harness;

namespace Ktm.DataSaver.Tests.Ingestion;

/// <summary>ADR-0006: бюджет попыток, изоляция ядовитого сообщения, dlq до commit, предохранитель.</summary>
public class IngestionLoopDlqTests
{
    private const string Secret = "ФИО Иванов Иван";

    private sealed class ScriptedStage(string stage, Func<ConsumedMessage, int, Exception?> script) : IIngestionStage
    {
        private readonly Dictionary<long, int> _calls = [];

        public string Stage => stage;

        public int CallsFor(long offset) => _calls.GetValueOrDefault(offset);

        public IngestionVerdict Evaluate(ConsumedMessage message, ParsedEvent parsed)
        {
            _calls[message.Offset] = _calls.GetValueOrDefault(message.Offset) + 1;
            if (script(message, _calls[message.Offset]) is { } error)
            {
                throw error;
            }

            return IngestionVerdict.Accept;
        }
    }

    private sealed class ThrowingSchema : ISchemaValidator
    {
        public SchemaValidationResult Validate(System.Text.Json.JsonElement instance) =>
            instance.GetProperty("sourceEventId").GetString() == "e-0-1" ? throw new InvalidOperationException(Secret) : SchemaValidationResult.Valid;
    }

    private static Exception Data() => new FakeDbException("23514");

    private static string ErrorText(Harness h) =>
        string.Join('\n', h.Logger.Collector.GetSnapshot().Select(r => r.Message + " " + string.Join(' ', r.StructuredState?.Select(p => p.Value) ?? [])));

    [Fact]
    public async Task DataErrorOnce_ThenSuccess_WritesAfterRetry_NothingGoesToDlq()
    {
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        h.Store.Fail = call => call == 1 ? Data() : null;
        h.StopWhenQueueDrained();

        await h.RunAsync();

        Assert.Equal(["write", "pause", "write", "resume", "commit"], h.Log);
        Assert.Empty(h.DeadLetters.Published);
        Assert.Equal(0, h.Observer.Isolations);
        Assert.Equal(IngestionState.Running, h.Loop.State);
        Assert.Equal([Next(0, 3)], h.Committer.Commits.Single());
    }

    [Fact]
    public async Task PoisonInWrite_AfterBudget_IsIsolatedAndPublishedToDlq_NeighboursWrittenOnce_OffsetCoversAll()
    {
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        h.Store.FailOn = ids => ids.Contains("e-0-1") ? new LeakyDbLocationException(Secret, "23514", "ck_poison", "events") : null;
        h.StopWhenQueueDrained();

        await h.RunAsync();

        // Запись -> публикация dlq -> commit; отдельного повторного «write» остатка нет.
        Assert.Equal("dlq", h.Log[^3]);
        Assert.Equal(["resume", "commit"], h.Log.TakeLast(2));
        Assert.Equal(1, h.Log.Count(l => l == "dlq"));
        var message = Assert.Single(h.DeadLetters.Messages);
        Assert.Equal("0@1", $"{message.Message.Partition}@{message.Message.Offset}");
        Assert.Equal(new EventOrigin("skud", "e-0-1"), message.Origin);
        var info = message.Info;
        Assert.Equal(
            (DeadLetterReasons.WriteDataError, "write", "23514", "ck_poison", "events", DlqPolicy.DefaultMaxAttempts + 2),
            (info.Reason, info.Stage, info.SqlState, info.ConstraintName, info.TableName, info.Attempts));
        Assert.Equal(1, h.Store.Writes.Count(w => w.SequenceEqual(["e-0-0"])));
        Assert.Equal(1, h.Store.Writes.Count(w => w.SequenceEqual(["e-0-2"])));
        Assert.Equal(1, h.Observer.Isolations);
        Assert.Equal(IngestionState.Running, h.Loop.State);
        Assert.Equal([Next(0, 3)], h.Committer.Commits.Single());
        Assert.Empty(h.Publisher.Published);
        Assert.DoesNotContain("Иванов", ErrorText(h));
    }

    [Fact]
    public async Task UnexpectedInWrite_SpendsTheBudget_WithoutPortC6_AndGoesToDlqAsUnexpectedError()
    {
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        h.Store.FailOn = ids => ids.Contains("e-0-1") ? new InvalidOperationException(Secret) : null;
        h.StopWhenQueueDrained();

        await h.RunAsync();

        var info = Assert.Single(h.DeadLetters.Messages).Info;
        Assert.Equal((DeadLetterReasons.UnexpectedError, "write", typeof(InvalidOperationException).FullName), (info.Reason, info.Stage, info.ExceptionType));
        Assert.Equal(0, h.Handler.Calls);
        Assert.DoesNotContain("Иванов", ErrorText(h) + info.Detail);
    }

    [Fact]
    public async Task InfrastructureErrors_AreRetriedWithoutLimit_NoIsolationNoDlq()
    {
        var transient = new Harness();
        transient.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        transient.Store.Fail = call => call <= 6 ? new IOException("db down") : null;
        transient.StopWhenQueueDrained();
        var fatal = new Harness();
        fatal.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        fatal.Store.Fail = call => call <= 6 ? new FakeDbException("42501") : null;
        fatal.StopWhenQueueDrained();

        await transient.RunAsync();
        await fatal.RunAsync();

        foreach (var h in new[] { transient, fatal })
        {
            Assert.Equal(7, h.Store.Writes.Count);
            Assert.Empty(h.DeadLetters.Published);
            Assert.Equal(0, h.Observer.Isolations);
            Assert.Equal([Next(0, 3)], h.Committer.Commits.Single());
        }

        Assert.Equal(6, fatal.Notifier.FatalConfigCalls);
    }

    [Fact]
    public async Task DlqDisabled_UnexpectedAndDataErrorsAreRetriedWithoutLimit()
    {
        var h = new Harness(new DlqPolicy(false, 3, 20));
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        h.Store.Fail = call => call <= 6 ? (call % 2 == 0 ? new InvalidOperationException("bug") : Data()) : null;
        h.StopWhenQueueDrained();

        await h.RunAsync();

        Assert.Equal(7, h.Store.Writes.Count);
        Assert.Empty(h.DeadLetters.Published);
        Assert.Equal(0, h.Observer.Isolations);
    }

    [Theory]
    [InlineData(PipelineStages.Validation)]
    [InlineData(PipelineStages.Rules)]
    public async Task UnexpectedExceptionInStage_AfterMaxAttempts_GoesToDlqWithStageAndType_NotToRejected(string stageName)
    {
        var stage = new ScriptedStage(stageName, (m, _) => m.Offset == 1 ? new InvalidOperationException(Secret) : null);
        var h = new Harness(3, stage);
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        h.StopWhenQueueDrained();

        await h.RunAsync();

        AssertStagePoison(h, stageName);
        Assert.Equal(3, stage.CallsFor(1));
        Assert.Equal(["pipeline", "pipeline"], h.Observer.Retries);
        Assert.Equal(["write", "dlq", "commit"], h.Log.Where(l => l is "write" or "dlq" or "commit" or "publish"));
    }

    [Fact]
    public async Task UnexpectedExceptionAtParse_AfterMaxAttempts_GoesToDlqWithParseStage()
    {
        var decoder = new EventDecoder(new Dictionary<int, ISchemaValidator> { [1] = new ThrowingSchema() });
        var h = new Harness(decoder);
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        h.StopWhenQueueDrained();

        await h.RunAsync();

        AssertStagePoison(h, PipelineStages.Parse);
    }

    private static void AssertStagePoison(Harness h, string stageName)
    {
        var message = Assert.Single(h.DeadLetters.Messages);
        Assert.Equal(1, message.Message.Offset);
        Assert.Equal(
            (DeadLetterReasons.UnexpectedError, stageName, typeof(InvalidOperationException).FullName, 3),
            (message.Info.Reason, message.Info.Stage, message.Info.ExceptionType, message.Info.Attempts));
        Assert.Empty(h.Publisher.Published);
        Assert.Equal(["e-0-0", "e-0-2"], h.Store.Writes.Single());
        Assert.Equal([Next(0, 3)], h.Committer.Commits.Single());
        var text = ErrorText(h);
        Assert.DoesNotContain("Иванов", text + message.Info.Detail);
        Assert.All(h.Logger.Collector.GetSnapshot(), r => Assert.Null(r.Exception));
        Assert.Contains(h.Logger.Collector.GetSnapshot(), r => r.Level == LogLevel.Error && r.Message.Contains(nameof(InvalidOperationException)));
    }

    [Fact]
    public async Task StageExceptionOnce_ThenSuccess_DoesNotGoToDlq_AndInfrastructureExceptionIsRetriedWithoutLimit()
    {
        var once = new ScriptedStage(PipelineStages.Rules, (m, n) => m.Offset == 1 && n == 1 ? new InvalidOperationException() : null);
        var a = new Harness(3, once);
        a.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        a.StopWhenQueueDrained();
        var network = new ScriptedStage(PipelineStages.Validation, (m, n) => m.Offset == 1 && n <= 7 ? new TimeoutException() : null);
        var b = new Harness(3, network);
        b.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        b.StopWhenQueueDrained();

        await a.RunAsync();
        await b.RunAsync();

        foreach (var h in new[] { a, b })
        {
            Assert.Empty(h.DeadLetters.Published);
            Assert.Equal(["e-0-0", "e-0-1", "e-0-2"], h.Store.Writes.Single());
            Assert.Equal([Next(0, 3)], h.Committer.Commits.Single());
        }

        Assert.Equal(8, network.CallsFor(1));
    }

    [Fact]
    public async Task DlqPublishFailure_BlocksCommit_RetriesOnlyThePublish_AndDoesNotRewriteTheNeighbours()
    {
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        h.Store.FailOn = ids => ids.Contains("e-0-1") ? Data() : null;
        h.DeadLetters.Result = call => call <= 2 ? DeadLetterPublishResult.Failure("broker down") : DeadLetterPublishResult.Success;
        var writesAtFirstPublish = 0;
        h.DeadLetters.OnCall = call => { if (call == 1) writesAtFirstPublish = h.Store.Writes.Count; };
        h.StopWhenQueueDrained();

        await h.RunAsync();

        Assert.Equal(3, h.DeadLetters.Published.Count);
        Assert.Equal(writesAtFirstPublish, h.Store.Writes.Count);
        Assert.Equal(1, h.Observer.Isolations);
        Assert.Equal("commit", h.Log[^1]);
        Assert.Single(h.Committer.Commits);
        Assert.Equal(h.Log.IndexOf("commit"), h.Log.Count - 1);
        Assert.DoesNotContain("commit", h.Log.Take(h.Log.LastIndexOf("dlq")));
    }

    [Fact]
    public async Task DlqPublisherThrowing_IsTreatedAsFailure_WithoutLeakingExceptionText()
    {
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        h.Store.FailOn = ids => ids.Contains("e-0-1") ? Data() : null;
        h.DeadLetters.Throw = call => call == 1 ? new InvalidOperationException(Secret) : null;
        h.StopWhenQueueDrained();

        await h.RunAsync();

        Assert.Equal(2, h.DeadLetters.Published.Count);
        Assert.Single(h.Committer.Commits);
        Assert.DoesNotContain("Иванов", ErrorText(h));
        Assert.All(h.Logger.Collector.GetSnapshot(), r => Assert.Null(r.Exception));
    }

    [Fact]
    public async Task InfrastructureErrorDuringIsolation_ContinuesIsolationWithoutNewBudgetOrRewrite()
    {
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        h.Store.FailOn = ids => ids.Contains("e-0-1") ? Data() : null;
        // Вызовы 1-3 - полная пачка (бюджет), 4 - [e-0-0], 5 - [e-0-1, e-0-2]: инфраструктура падает.
        h.Store.Fail = call => call == 5 ? new IOException("db down") : null;
        h.StopWhenQueueDrained();

        await h.RunAsync();

        Assert.Equal(1, h.Observer.Isolations);
        Assert.Equal(1, h.Store.Writes.Count(w => w.SequenceEqual(["e-0-0"])));
        Assert.Equal("0@1", Assert.Single(h.DeadLetters.Published).Single());
        Assert.Equal([Next(0, 3)], h.Committer.Commits.Single());
    }

    [Fact]
    public async Task PoisonOfRevokedPartition_IsNotPublishedToDlq_AndItsOffsetIsNotCommitted()
    {
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 0), Valid(1, 0), Valid(0, 1));
        h.Store.FailOn = ids => ids.Contains("e-1-0") ? Data() : null;
        h.Store.OnCall = call => { if (call == 4) h.Source.ScheduleRevoke(P(1)); };
        h.StopWhenQueueDrained();

        await h.RunAsync();

        Assert.Empty(h.DeadLetters.Published);
        Assert.Equal([Next(0, 2)], h.Committer.Commits.Single());
    }

    [Fact]
    public async Task StopWhileDlqIsUnavailable_EndsWithoutCommit()
    {
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        h.Store.FailOn = ids => ids.Contains("e-0-1") ? Data() : null;
        h.DeadLetters.Result = _ => DeadLetterPublishResult.Failure("topic missing");
        h.DeadLetters.OnCall = call => { if (call == 1) h.Stop.Cancel(); };

        await h.RunWithClockPumpAsync();

        Assert.Empty(h.Committer.Commits);
        Assert.DoesNotContain("commit", h.Log);
    }

    [Fact]
    public async Task C6DeadLetterOutsideTheBatch_SpendsTheBudget_AndAcceptedDeadLetterDoesNot()
    {
        var spent = new Harness();
        spent.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        spent.Store.FailOn = ids => ids.Contains("e-0-1") ? Data() : null;
        spent.Handler.Decide = _ => WriteFailureDecision.DeadLetter([Valid(9, 9)]);
        spent.StopWhenQueueDrained();
        var accepted = new Harness();
        accepted.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2));
        accepted.Store.FailOn = ids => ids.Contains("e-0-1") ? Data() : null;
        accepted.Handler.Decide = ctx => WriteFailureDecision.DeadLetter([ctx.Batch[1]]);
        accepted.StopWhenQueueDrained();

        await spent.RunAsync();
        await accepted.RunAsync();

        Assert.Equal(1, spent.Observer.Isolations);
        Assert.Single(spent.DeadLetters.Published);
        Assert.Equal(0, accepted.Observer.Isolations);
        Assert.Empty(accepted.DeadLetters.Published);
        Assert.Equal([Next(0, 3)], accepted.Committer.Commits.Single());
    }

    [Theory]
    [InlineData(2)]
    [InlineData(1)]
    public async Task Breaker_OpensAfterThresholdMessagesInARow_NoDlqWhileOpen_ClosesOnFirstSuccessfulWrite(int threshold)
    {
        // Валидатор «сломан» для первых threshold сообщений навсегда, для следующего - на 3 попытки и затем чинится.
        var stage = new ScriptedStage(PipelineStages.Validation, (m, n) => m.Offset < threshold || (m.Offset == threshold && n <= 3) ? new InvalidOperationException() : null);
        var h = new Harness(new DlqPolicy(true, 1, threshold), batchSize: 1, stage);
        h.Source.Enqueue(Enumerable.Range(0, threshold + 1).Select(i => Valid(0, i)).ToArray());
        var openWhileRetrying = false;
        h.Store.OnCall = _ => openWhileRetrying = h.Breaker.IsOpen;
        h.StopWhenQueueDrained();

        await h.RunAsync();

        Assert.Equal(threshold, h.DeadLetters.Messages.Count);
        Assert.Equal(4, stage.CallsFor(threshold));
        Assert.Equal([$"e-0-{threshold}"], h.Store.Writes.Single());
        Assert.True(openWhileRetrying);
        Assert.False(h.Breaker.IsOpen);
        Assert.Contains(h.Logger.Collector.GetSnapshot(), r => r.Message.Contains("breaker is open"));
        Assert.Equal(IngestionState.Running, h.Loop.State);
    }

    [Fact]
    public async Task BreakerOpenInWrite_FourthPoisonStaysInBatch_RetriedWithoutLimit_AdminNotifiedAboutBreaker_NoCommit()
    {
        var h = new Harness(new DlqPolicy(true, 3, 3), batchSize: 4);
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2), Valid(0, 3));
        h.Store.FailOn = _ => Data();
        h.Store.OnCall = call => { if (call >= 12) h.Stop.Cancel(); };

        await h.RunWithClockPumpAsync();

        // Порог 3: три первых ядовитых учтены, четвёртое осталось в пачке; пока пачка не пишется, dlq не используется
        // (сообщения остаются в пачке, commit нет), повтор без предела, оповещение о предохранителе на каждой попытке.
        Assert.True(h.Breaker.IsOpen);
        Assert.Equal(3, h.Breaker.Count);
        Assert.Empty(h.DeadLetters.Published);
        Assert.Equal(["e-0-3"], h.Store.Writes[^1]);
        Assert.True(h.Notifier.BreakerCalls >= 2);
        Assert.Equal(0, h.Notifier.FatalConfigCalls);
        Assert.Empty(h.Committer.Commits);
        Assert.Equal(1, ErrorsContaining(h, "breaker is open"));
        // «Обработчик не помог» остаётся Warning и пишется только пока предохранитель закрыт (3 попытки до изоляции).
        Assert.Equal(0, ErrorsContaining(h, "made no progress"));
        Assert.Equal(3, WarningsContaining(h, "made no progress"));
    }

    [Fact]
    public async Task WriteDataErrorWithClosedBreaker_HandlerMadeNoProgress_IsLoggedAsWarning()
    {
        var h = new Harness(new DlqPolicy(true, 3, 3), batchSize: 2);
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1));
        h.Store.FailOn = _ => Data();
        h.Store.OnCall = call => { if (call >= 2) h.Stop.Cancel(); };

        await h.RunWithClockPumpAsync();

        Assert.False(h.Breaker.IsOpen);
        Assert.True(WarningsContaining(h, "made no progress") >= 1);
        Assert.Equal(0, ErrorsContaining(h, "made no progress"));
    }

    private static int WarningsContaining(Harness h, string text) =>
        h.Logger.Collector.GetSnapshot().Count(r => r.Level == LogLevel.Warning && r.Message.Contains(text));

    private static int ErrorsContaining(Harness h, string text) =>
        h.Logger.Collector.GetSnapshot().Count(r => r.Level == LogLevel.Error && r.Message.Contains(text));

    private static Harness AlternatingBatch(int threshold, string pattern)
    {
        // 'V' - валидное, 'P' - ядовитое при записи; одна пачка на все сообщения.
        var h = new Harness(new DlqPolicy(true, 3, threshold), batchSize: pattern.Length);
        h.Source.Enqueue(pattern.Select((_, i) => Valid(0, i)).ToArray());
        var poison = pattern.Select((c, i) => (c, i)).Where(t => t.c == 'P').Select(t => $"e-0-{t.i}").ToHashSet();
        h.Store.FailOn = ids => ids.Any(poison.Contains) ? Data() : null;
        h.StopWhenQueueDrained();
        return h;
    }

    [Fact]
    public async Task Breaker_AlternatingPoisonAndValid_IsNotOpenedBySuccessfulWritesInBetween()
    {
        var h = AlternatingBatch(3, "VPVPVPVPVPVP");

        await h.RunAsync();

        Assert.Equal(["0@1", "0@3", "0@5", "0@7", "0@9", "0@11"], h.DeadLetters.Messages.Select(m => $"{m.Message.Partition}@{m.Message.Offset}"));
        Assert.False(h.Breaker.IsOpen);
        Assert.Equal(0, h.Notifier.BreakerCalls);
        Assert.Equal(IngestionState.Running, h.Loop.State);
        Assert.Equal([Next(0, 12)], h.Committer.Commits.Single());
        Assert.Equal(0, ErrorsContaining(h, "breaker is open"));
    }

    [Fact]
    public async Task Breaker_CountsByOrderOfEvents_NotByGroups()
    {
        // V V P P P V P: третье ядовитое открывает предохранитель, запись V закрывает его, четвёртое ядовитое уходит в dlq.
        var h = AlternatingBatch(3, "VVPPPVP");

        await h.RunAsync();

        Assert.Equal(["0@2", "0@3", "0@4", "0@6"], h.DeadLetters.Messages.Select(m => $"{m.Message.Partition}@{m.Message.Offset}"));
        Assert.Equal(0, h.Notifier.BreakerCalls);
        Assert.False(h.Breaker.IsOpen);
        Assert.Equal(1, ErrorsContaining(h, "breaker is open"));
        Assert.Equal([Next(0, 7)], h.Committer.Commits.Single());
    }

    [Fact]
    public async Task Breaker_PoisonLeftByOpenBreaker_IsReisolatedAfterASuccessfulWriteClosedIt_AndGoesToDlq()
    {
        var h = AlternatingBatch(3, "PPPPV");

        await h.RunAsync();

        Assert.Equal(["0@0", "0@1", "0@2", "0@3"], h.DeadLetters.Messages.Select(m => $"{m.Message.Partition}@{m.Message.Offset}"));
        Assert.Equal(1, h.Store.Writes.Count(w => w.SequenceEqual(["e-0-4"])));
        Assert.Equal([Next(0, 5)], h.Committer.Commits.Single());
        Assert.Equal(IngestionState.Running, h.Loop.State);
    }

    [Fact]
    public async Task DeadLetterAttempts_AreTheRealNumberOfFailedWritesThatIncludedTheMessage()
    {
        var many = new Harness(new DlqPolicy(true, 3, 20), batchSize: 4);
        many.Source.Enqueue(Valid(0, 0), Valid(0, 1), Valid(0, 2), Valid(0, 3));
        many.Store.FailOn = ids => ids.Contains("e-0-2") ? Data() : null;
        many.StopWhenQueueDrained();
        var one = new Harness(new DlqPolicy(true, 3, 20), batchSize: 1);
        one.Source.Enqueue(Valid(0, 0));
        one.Store.FailOn = _ => Data();
        one.StopWhenQueueDrained();

        await many.RunAsync();
        await one.RunAsync();

        // Три падения всей пачки + часть [2,3] + одиночное [2]; формула MaxAttempts + 1 дала бы 4.
        Assert.Equal(5, Assert.Single(many.DeadLetters.Messages).Info.Attempts);
        // Три падения пачки из одного сообщения + подтверждающая одиночная запись.
        Assert.Equal(4, Assert.Single(one.DeadLetters.Messages).Info.Attempts);
    }

    private sealed class EventScriptedStage(Func<string, int, Exception?> script) : IIngestionStage
    {
        public Dictionary<string, int> Calls { get; } = [];

        public string Stage => PipelineStages.Validation;

        public IngestionVerdict Evaluate(ConsumedMessage message, ParsedEvent parsed)
        {
            Calls[parsed.SourceEventId] = Calls.GetValueOrDefault(parsed.SourceEventId) + 1;
            if (script(parsed.SourceEventId, Calls[parsed.SourceEventId]) is { } error)
            {
                throw error;
            }

            return IngestionVerdict.Accept;
        }
    }

    [Theory]
    [InlineData(1, new[] { "e-0-0", "e-0-1" }, new[] { 0 })]
    [InlineData(0, new[] { "e-1-0" }, new[] { 1 })]
    public async Task RevokeDuringStageBackoff_MessagesOfTheRevokedPartitionAreNeitherProcessedNorCommitted(
        int revokedPartition, string[] expectedWrites, int[] committedPartitions)
    {
        var stage = new EventScriptedStage((id, n) => id == "e-0-0" && n == 1 ? new InvalidOperationException() : null);
        var h = new Harness(3, stage);
        h.Source.Enqueue(Valid(0, 0), Valid(1, 0), Valid(0, 1));
        h.Source.OnIdle = n =>
        {
            if (n == 1) h.Source.ScheduleRevoke(P(revokedPartition));
            if (h.Loop.State == IngestionState.Running) h.Stop.Cancel();
        };

        await h.RunAsync();

        Assert.Equal(expectedWrites, h.Store.Writes.Single());
        Assert.Equal(committedPartitions, h.Committer.Commits.Single().Select(o => o.Partition));
        Assert.Empty(h.DeadLetters.Published);
        Assert.Empty(h.Publisher.Published);
        if (revokedPartition == 1)
        {
            // Сообщение отозванной партиции не дошло до стадии вообще.
            Assert.False(stage.Calls.ContainsKey("e-1-0"));
        }
    }

    [Fact]
    public async Task BreakerOpenAtStage_NotifiesAdminOnEveryAttempt_WithOneErrorPerAttempt_AndOneTransitionError()
    {
        var calls = 0;
        Harness? holder = null;
        var stage = new EventScriptedStage((id, n) =>
        {
            if (id == "e-0-1" && ++calls == 6)
            {
                holder!.Stop.Cancel();
            }

            return new InvalidOperationException();
        });
        var h = new Harness(new DlqPolicy(true, 3, 1), batchSize: 2, stage);
        holder = h;
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1));

        await h.RunWithClockPumpAsync();

        // e-0-0 исчерпало бюджет и открыло предохранитель (порог 1); e-0-1 повторяется без предела.
        var attemptsOfSecond = stage.Calls["e-0-1"];
        Assert.True(attemptsOfSecond >= 6);
        Assert.Equal(attemptsOfSecond, h.Notifier.BreakerCalls);
        Assert.Equal(0, h.Notifier.FatalConfigCalls);
        Assert.Equal(IngestionState.Degraded, h.Loop.State);
        Assert.True(h.Breaker.IsOpen);
        Assert.Equal(1, h.Breaker.Count);
        Assert.Empty(h.DeadLetters.Published);
        Assert.Empty(h.Committer.Commits);
        Assert.Contains("pipeline", h.Observer.Retries);
        // Одна Error-запись цикла на неудачную попытку (две попытки e-0-0 до dlq плюс все попытки e-0-1), без отдельных «breaker is open».
        Assert.Equal(2 + attemptsOfSecond, ErrorsContaining(h, "Pipeline stage failed"));
        Assert.Equal(1, ErrorsContaining(h, "breaker is open"));
    }

    [Fact]
    public async Task BreakerOpenAtStage_SecondMessageNotifiesAdminAlreadyOnItsFirstAttempts()
    {
        var calls = 0;
        Harness? holder = null;
        var stage = new EventScriptedStage((id, n) =>
        {
            // Останов на второй попытке второго сообщения (бюджет MaxAttempts = 3 ещё не исчерпан).
            if (id == "e-0-1" && ++calls == 2)
            {
                holder!.Stop.Cancel();
            }

            return new InvalidOperationException();
        });
        var h = new Harness(new DlqPolicy(true, 3, 1), batchSize: 2, stage);
        holder = h;
        h.Source.Enqueue(Valid(0, 0), Valid(0, 1));

        await h.RunWithClockPumpAsync();

        // Останов срабатывает после завершения текущей попытки; каждая попытка (в том числе первая и вторая) оповестила.
        Assert.True(stage.Calls["e-0-1"] >= 2);
        Assert.Equal(stage.Calls["e-0-1"], h.Notifier.BreakerCalls);
        Assert.Empty(h.Committer.Commits);
    }

    [Fact]
    public async Task RejectionPublisherThrowing_IsRetried_AndItsExceptionTextNeverReachesTheLog()
    {
        var h = new Harness();
        h.Source.Enqueue(Valid(0, 0), Garbage(0, 1), Valid(0, 2));
        h.Publisher.Throw = call => call == 1 ? new InvalidOperationException(Secret) : null;
        h.StopWhenQueueDrained();

        await h.RunAsync();

        Assert.Equal(2, h.Publisher.Published.Count);
        Assert.Equal([Next(0, 3)], h.Committer.Commits.Single());
        Assert.DoesNotContain("Иванов", ErrorText(h));
        Assert.All(h.Logger.Collector.GetSnapshot(), r => Assert.Null(r.Exception));
        Assert.Contains(h.Logger.Collector.GetSnapshot(), r => r.Level == LogLevel.Error && r.Message.Contains(nameof(InvalidOperationException)));
    }
}
