using System.Net.Sockets;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Pipeline;
using Ktm.DataSaver.Core.Reprocessing;
using Ktm.DataSaver.Tests.Ingestion;
using static Ktm.DataSaver.Tests.Reprocessing.ReprocessFixture;

namespace Ktm.DataSaver.Tests.Reprocessing;

/// <summary>ADR-0006 п. 2.6-2.8: раннер повторной обработки (счётчики, отчёт, устойчивость, скорость, отмена).</summary>
public class ReprocessRunnerTests
{
    private const string Marker = "PII-MARKER-Иванов";

    private static IReadOnlyList<ReplayMessage> Ten() =>
    [
        Valid("m0", 0), Valid("m1", 1), Valid("m2", 2), Valid("m3", 3), Valid("m4", 4),
        Valid("m5", 5), Valid("m6", 6), Valid("m7", 7), Valid("m8", 8), Garbage(9),
    ];

    private static ScriptedStage RejectM8() => new((id, _) => id == "m8" ? ScriptedStage.Reject("rule_x") : null);

    [Fact]
    public async Task CountersReportDuplicatesAndRejectedAgain_AndSecondRunWritesNothing()
    {
        var fixture = new ReprocessFixture(RejectM8());
        fixture.Writer.Rows.UnionWith(["m0", "m1"]);

        var (first, snapshot) = await fixture.RunAsync(fixture.NewJob(), new FakeReader(Ten()));

        Assert.Equal(new ReprocessRunResult(ReprocessOutcome.Succeeded, null), first);
        Assert.Equal(10, snapshot.Counters.Read);
        Assert.Equal(6, snapshot.Counters.Written);
        Assert.Equal(2, snapshot.Counters.Duplicates);
        Assert.Equal(new Dictionary<string, long> { ["rule_x"] = 1, [RejectReasons.MalformedJson] = 1 }, snapshot.Counters.RejectedAgain);
        Assert.Equal(0, snapshot.Counters.FailedAgain);
        var job = fixture.NewJob();
        var (_, again) = await fixture.RunAsync(job, new FakeReader(Ten()));
        Assert.Equal((0, 8), (again.Counters.Written, again.Counters.Duplicates));

        var report = job.GetReportPage(100, 0).Entries;
        Assert.Contains(report, e => e is { Kind: ReprocessReportKind.RejectedAgain, Offset: 8, Stage: PipelineStages.Rules, Code: "rule_x" });
        Assert.Contains(report, e => e is { Offset: 9, Stage: PipelineStages.Parse, Code: RejectReasons.MalformedJson });
    }

    [Fact]
    public async Task ReasonFilter_ProcessesOnlyMatchingMessages_AndCountsTheRestAsSkipped()
    {
        var fixture = new ReprocessFixture();
        ReplayMessage[] messages =
        [
            Valid("a", 0, "unexpected_error"), Valid("b", 1, "write_data_error"), Valid("c", 2, null), Valid("d", 3, "unexpected_error"),
        ];
        var request = new ReprocessRequest(ReprocessSource.Dlq, null, null, "unexpected_error", 400);

        var (_, snapshot) = await fixture.RunAsync(fixture.NewJob(request), new FakeReader(messages));

        Assert.Equal((4, 2, 2), (snapshot.Counters.Read, snapshot.Counters.SkippedByFilter, snapshot.Counters.Written));
        Assert.Equal(["a", "d"], fixture.Writer.Rows.Order());
    }

    [Fact]
    public async Task PoisonOnWrite_IsReportedNotPublished_NeighboursWritten_NoTextOfException()
    {
        var fixture = new ReprocessFixture();
        fixture.Writer.Fail = (_, ids) => ids.Contains("bad") ? new LeakyDbException(Marker, "23514") : null;
        ReplayMessage[] messages = [Valid("a", 0), Valid("bad", 1), Valid("c", 2), Valid("d", 3)];
        var job = fixture.NewJob();

        var (result, snapshot) = await fixture.RunAsync(job, new FakeReader(messages));

        Assert.Equal(new ReprocessRunResult(ReprocessOutcome.CompletedWithFailures, null), result);
        Assert.Equal(["a", "c", "d"], fixture.Writer.Rows.Order());
        Assert.Equal(1, snapshot.Counters.FailedAgain);
        var entry = Assert.Single(job.GetReportPage(100, 0).Entries);
        Assert.Equal((ReprocessReportKind.FailedAgain, 1L, PipelineStages.Write, "write_data_error", "23514"), (entry.Kind, entry.Offset, entry.Stage, entry.Code, entry.SqlState));
        Assert.DoesNotContain(Marker, entry.ToString());
        Assert.DoesNotContain(fixture.Logger.Collector.GetSnapshot(), r => r.Message.Contains(Marker));
    }

    [Fact]
    public async Task UnexpectedStageException_SpendsBudgetThenGoesToReport_FlakyMessageIsProcessed()
    {
        var stage = new ScriptedStage((id, call) => id switch
        {
            "boom" => throw new InvalidOperationException(Marker),
            "flaky" when call == 1 => throw new InvalidOperationException(Marker),
            _ => null,
        });
        var fixture = new ReprocessFixture(stage);
        ReplayMessage[] messages = [Valid("ok", 0), Valid("boom", 1), Valid("flaky", 2)];
        var job = fixture.NewJob();

        var (result, snapshot) = await fixture.RunAsync(job, new FakeReader(messages));

        Assert.Equal(ReprocessOutcome.CompletedWithFailures, result.Outcome);
        Assert.Equal(["flaky", "ok"], fixture.Writer.Rows.Order());
        Assert.Equal((3, 2), (stage.CallsFor("boom"), stage.CallsFor("flaky")));
        var entry = Assert.Single(job.GetReportPage(100, 0).Entries);
        Assert.Equal((1L, PipelineStages.Rules, "unexpected_error", typeof(InvalidOperationException).FullName), (entry.Offset, entry.Stage, entry.Code, entry.ExceptionType));
        Assert.Equal(1, snapshot.Counters.FailedAgain);
        Assert.DoesNotContain(fixture.Logger.Collector.GetSnapshot(), r => r.Message.Contains(Marker));
    }

    [Fact]
    public async Task DatabaseUnavailableLongerThanLimit_FailsWithProgress_ShorterOutageIsWaitedOut()
    {
        ReplayMessage[] messages = [Valid("a", 0), Valid("b", 1), Valid("c", 2), Valid("d", 3)];

        var down = new ReprocessFixture(batchSize: 2, maxUnavailable: TimeSpan.FromSeconds(30));
        down.Writer.Fail = (call, _) => call >= 2 ? new SocketException() : null;
        var (failed, progress) = await down.RunAsync(down.NewJob(), new FakeReader(messages));

        Assert.Equal(new ReprocessRunResult(ReprocessOutcome.Failed, ReprocessErrorCodes.DbUnavailable), failed);
        Assert.Equal(2, progress.Counters.Written);

        var blip = new ReprocessFixture(batchSize: 2, maxUnavailable: TimeSpan.FromMinutes(10));
        blip.Writer.Fail = (call, _) => call is 2 or 3 ? new SocketException() : null;
        var (recovered, done) = await blip.RunAsync(blip.NewJob(), new FakeReader(messages));

        Assert.Equal(ReprocessOutcome.Succeeded, recovered.Outcome);
        Assert.Equal(4, done.Counters.Written);
    }

    [Fact]
    public async Task MaxRate100_On1000Messages_TakesAtLeastNineSeconds()
    {
        var fixture = new ReprocessFixture(batchSize: 500);
        var messages = Enumerable.Range(0, 1000).Select(i => Valid($"e{i}", i)).ToArray();
        var request = new ReprocessRequest(ReprocessSource.Dlq, null, null, null, 100);
        var started = fixture.Time.GetUtcNow();

        var (result, snapshot) = await fixture.RunAsync(fixture.NewJob(request), new FakeReader(messages));

        Assert.Equal(ReprocessOutcome.Succeeded, result.Outcome);
        Assert.Equal(1000, snapshot.Counters.Written);
        Assert.True(fixture.Time.GetUtcNow() - started >= TimeSpan.FromSeconds(9));
    }

    [Fact]
    public async Task Cancel_StopsAfterCurrentBatch_KeepingProgress()
    {
        var fixture = new ReprocessFixture();
        var job = fixture.NewJob();
        fixture.Writer.Fail = (call, _) =>
        {
            if (call == 1)
            {
                job.RequestCancel();
            }

            return null;
        };
        var reader = new FakeReader(Enumerable.Range(0, 9).Select(i => Valid($"e{i}", i)).ToArray());

        var (result, snapshot) = await fixture.RunAsync(job, reader);

        Assert.Equal(new ReprocessRunResult(ReprocessOutcome.Cancelled, null), result);
        Assert.Equal(3, snapshot.Counters.Written);
        Assert.Single(reader.Requested);
    }

    [Fact]
    public async Task SourceFailure_FailsWithSourceUnavailable_AndHostStopCancels()
    {
        var fixture = new ReprocessFixture();
        var broken = new FakeReader([Valid("a", 0)]) { BeforeRead = _ => throw new ReplaySourceException("Kafka: Local_Transport") };

        var (failed, _) = await fixture.RunAsync(fixture.NewJob(), broken);

        Assert.Equal(new ReprocessRunResult(ReprocessOutcome.Failed, ReprocessErrorCodes.SourceUnavailable), failed);

        using var stop = new CancellationTokenSource();
        await stop.CancelAsync();
        var (stopped, _) = await fixture.RunAsync(fixture.NewJob(), new FakeReader([Valid("a", 0)]) { BeforeRead = _ => throw new OperationCanceledException(stop.Token) }, stop.Token);

        Assert.Equal(ReprocessOutcome.Cancelled, stopped.Outcome);
    }

    private static ReplayMessage[] Four() => [Valid("a", 0), Valid("b", 1), Valid("c", 2), Valid("d", 3)];

    private static bool LogHas(ReprocessFixture fixture, Predicate<string> predicate) =>
        fixture.Logger.Collector.GetSnapshot().Any(r => r.Level == Microsoft.Extensions.Logging.LogLevel.Error && predicate(r.Message));

    [Fact]
    public async Task FatalConfigOnWrite_FailsFastWithInternal_WithoutWaiting_KeepingEarlierProgress()
    {
        var fixture = new ReprocessFixture(batchSize: 2);
        fixture.Writer.Fail = (call, _) => call == 1 ? null : new LeakyDbException(Marker, "42501");
        var job = fixture.NewJob();
        var reader = new FakeReader(Four());
        job.MarkRunning(fixture.Time.GetUtcNow(), reader.Partitions);
        var started = fixture.Time.GetUtcNow();

        // Без «насоса» часов: любое ожидание повисло бы, а не прошло незаметно.
        var result = await fixture.Runner.RunAsync(job, reader, CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(new ReprocessRunResult(ReprocessOutcome.Failed, ReprocessErrorCodes.Internal), result);
        Assert.Equal(2, fixture.Writer.Calls.Count);
        Assert.Equal(started, fixture.Time.GetUtcNow());
        Assert.Equal(2, job.Snapshot().Counters.Written);
        Assert.True(LogHas(fixture, m => m.Contains(nameof(LeakyDbException)) && m.Contains("42501")));
        Assert.DoesNotContain(fixture.Logger.Collector.GetSnapshot(), r => r.Message.Contains(Marker));
    }

    [Fact]
    public async Task FatalConfigDuringIsolation_FailsFastWithInternal_AfterTheNeighboursWereWritten()
    {
        var fixture = new ReprocessFixture(batchSize: 4, maxUnavailable: TimeSpan.FromHours(1));
        fixture.Writer.Fail = (call, _) => call switch
        {
            <= 3 => new LeakyDbException(Marker, "23514"),
            5 => new LeakyDbException(Marker, "42501"),
            _ => null,
        };
        var started = fixture.Time.GetUtcNow();

        var (result, snapshot) = await fixture.RunAsync(fixture.NewJob(), new FakeReader(Four()));

        Assert.Equal(new ReprocessRunResult(ReprocessOutcome.Failed, ReprocessErrorCodes.Internal), result);
        Assert.Equal(5, fixture.Writer.Calls.Count);
        Assert.Equal(["a", "b"], fixture.Writer.Rows.Order());
        Assert.Equal(2, snapshot.Counters.Written);
        Assert.True(fixture.Time.GetUtcNow() - started < TimeSpan.FromMinutes(5));
        Assert.True(LogHas(fixture, m => m.Contains("42501")));
    }

    [Fact]
    public async Task DataClassStageException_SpendsTheBudget_GoesToTheReport_AndNeverWaitsForInfrastructure()
    {
        var stage = new ScriptedStage((id, _) => id == "bad" ? throw new LeakyDbException(Marker, "23514") : null);
        var fixture = new ReprocessFixture(stage, maxUnavailable: TimeSpan.FromHours(1));
        var job = fixture.NewJob();
        var started = fixture.Time.GetUtcNow();

        var (result, snapshot) = await fixture.RunAsync(job, new FakeReader([Valid("a", 0), Valid("bad", 1), Valid("c", 2)]));

        Assert.Equal(ReprocessOutcome.CompletedWithFailures, result.Outcome);
        Assert.Equal(3, stage.CallsFor("bad"));
        Assert.Equal(["a", "c"], fixture.Writer.Rows.Order());
        var entry = Assert.Single(job.GetReportPage(100, 0).Entries);
        Assert.Equal((ReprocessReportKind.FailedAgain, PipelineStages.Rules, "unexpected_error", 1L), (entry.Kind, entry.Stage, entry.Code, entry.Offset));
        Assert.Equal(1, snapshot.Counters.FailedAgain);
        Assert.True(fixture.Time.GetUtcNow() - started < TimeSpan.FromMinutes(5));
    }

    [Fact]
    public async Task TransientStageException_IsWaitedOut_ButLongerThanTheLimitFailsWithInternal()
    {
        var calls = 0;
        var flaky = new ScriptedStage((id, _) => id == "flaky" && ++calls <= 2 ? throw new SocketException() : null);
        var recovering = new ReprocessFixture(flaky);

        var (recovered, ok) = await recovering.RunAsync(recovering.NewJob(), new FakeReader([Valid("flaky", 0), Valid("z", 1)]));

        Assert.Equal(ReprocessOutcome.Succeeded, recovered.Outcome);
        Assert.Equal(2, ok.Counters.Written);

        var down = new ReprocessFixture(new ScriptedStage((_, _) => throw new SocketException()), maxUnavailable: TimeSpan.FromSeconds(30));
        var (failed, _) = await down.RunAsync(down.NewJob(), new FakeReader([Valid("a", 0)]));

        Assert.Equal(new ReprocessRunResult(ReprocessOutcome.Failed, ReprocessErrorCodes.Internal), failed);
    }

    [Fact]
    public async Task ValueErrorOnWrite_IsReportedAsWriteValueError_WithTheExceptionType_NeighboursWritten()
    {
        var fixture = new ReprocessFixture();
        fixture.Writer.Fail = (_, ids) => ids.Contains("bad") ? new OverflowException(Marker) : null;
        var job = fixture.NewJob();

        var (result, snapshot) = await fixture.RunAsync(job, new FakeReader([Valid("a", 0), Valid("bad", 1), Valid("c", 2)]));

        Assert.Equal(ReprocessOutcome.CompletedWithFailures, result.Outcome);
        Assert.Equal(["a", "c"], fixture.Writer.Rows.Order());
        var entry = Assert.Single(job.GetReportPage(100, 0).Entries);
        Assert.Equal((PipelineStages.Write, "write_value_error", typeof(OverflowException).FullName), (entry.Stage, entry.Code, entry.ExceptionType));
        var everything = System.Text.Json.JsonSerializer.Serialize(entry) + System.Text.Json.JsonSerializer.Serialize(snapshot)
            + string.Join('\n', fixture.Logger.Collector.GetSnapshot().Select(r => r.Message));
        Assert.DoesNotContain(Marker, everything);
    }

    [Fact]
    public async Task Counters_SplitInsertedFromDuplicates_AndIgnoreFailedWrites()
    {
        var plain = new ReprocessFixture(batchSize: 3);
        plain.Writer.Rows.UnionWith(["a", "b"]);

        var (_, one) = await plain.RunAsync(plain.NewJob(), new FakeReader([Valid("a", 0), Valid("b", 1), Valid("c", 2)]));

        Assert.Equal((1, 2), (one.Counters.Written, one.Counters.Duplicates));

        var poisoned = new ReprocessFixture(batchSize: 4);
        poisoned.Writer.Fail = (_, ids) => ids.Contains("bad") ? new LeakyDbException(Marker, "23514") : null;

        var (_, isolated) = await poisoned.RunAsync(poisoned.NewJob(), new FakeReader([Valid("a", 0), Valid("bad", 1), Valid("c", 2), Valid("d", 3)]));

        Assert.Equal((3, 0, 1), (isolated.Counters.Written, isolated.Counters.Duplicates, isolated.Counters.FailedAgain));
    }

    [Fact]
    public async Task Counters_AreNotDoubledWhenInfrastructureInterruptsIsolation()
    {
        var fixture = new ReprocessFixture(batchSize: 4);
        fixture.Writer.Rows.Add("a");
        var interrupted = false;
        fixture.Writer.Fail = (_, ids) =>
        {
            if (ids.Contains("bad"))
            {
                return new LeakyDbException(Marker, "23514");
            }

            if (!interrupted && ids.SequenceEqual(["c", "d"]))
            {
                interrupted = true;
                return new SocketException();
            }

            return null;
        };

        var (result, snapshot) = await fixture.RunAsync(fixture.NewJob(), new FakeReader([Valid("a", 0), Valid("bad", 1), Valid("c", 2), Valid("d", 3)]));

        Assert.Equal(ReprocessOutcome.CompletedWithFailures, result.Outcome);
        Assert.True(interrupted);
        Assert.Equal((2, 1, 1), (snapshot.Counters.Written, snapshot.Counters.Duplicates, snapshot.Counters.FailedAgain));
    }
}
