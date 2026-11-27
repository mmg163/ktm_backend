using System.Text.Json;
using Ktm.DataSaver.Application.Decoding;
using Ktm.DataSaver.Application.Pipeline;
using Ktm.DataSaver.Application.Reprocessing;
using Ktm.DataSaver.Application.Resilience;
using Ktm.DataSaver.Tests.Ingestion;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging.Testing;
using static Ktm.DataSaver.Tests.Reprocessing.ReprocessFixture;

namespace Ktm.DataSaver.Tests.Reprocessing;

/// <summary>ADR-0006 п. 2.6: одна активная задача, отмена, история, отчёт, остановка хоста, отсутствие данных сообщений.</summary>
public class ReprocessJobManagerTests
{
    private const string Marker = "PII-MARKER-Иванов";

    private sealed class FakeSource(Func<ReprocessRequest, ReplayOpenResult> open) : IReplaySource
    {
        public int Opened { get; private set; }

        public ValueTask<ReplayOpenResult> OpenAsync(ReprocessRequest request, CancellationToken cancellationToken)
        {
            Opened++;
            return ValueTask.FromResult(open(request));
        }
    }

    private sealed class Rig
    {
        public readonly FakeLogger<ReprocessJobManager> Logger = new();
        public readonly ReprocessFixture Fixture;
        public readonly FakeSource Source;
        public readonly ReprocessJobManager Manager;

        public Rig(Func<ReprocessRequest, ReplayOpenResult> open, ScriptedStage? stage = null, int maxReportEntries = 1000)
        {
            Fixture = new ReprocessFixture(stage, maxReportEntries: maxReportEntries);
            Source = new FakeSource(open);
            Manager = new ReprocessJobManager(Source, Fixture.Runner, Fixture.Settings, Fixture.Time, Logger);
        }

        public static ReprocessRequest Request { get; } = new(ReprocessSource.Dlq, null, null, null, 200);

        public async Task<ReprocessJobSnapshot> StartedAsync(ReprocessRequest? request = null)
        {
            var result = await Manager.StartAsync(request ?? Request, CancellationToken.None);
            Assert.Equal(StartStatus.Started, result.Status);
            return result.Job!;
        }

        public async Task<ReprocessJobSnapshot> FinishedAsync(Guid id)
        {
            // Паузы раннера (backoff) идут по FakeTimeProvider: часы подгоняются, пока задача не завершится.
            using var done = new CancellationTokenSource();
            var pump = Task.Run(async () =>
            {
                while (!done.IsCancellationRequested)
                {
                    Fixture.Time.Advance(TimeSpan.FromMilliseconds(500));
                    await Task.Delay(1);
                }
            });
            try
            {
                await Manager.WhenFinishedAsync(id).WaitAsync(TimeSpan.FromSeconds(10));
            }
            finally
            {
                await done.CancelAsync();
                await pump;
            }

            return Manager.Get(id)!;
        }
    }

    /// <summary>Читатель, который не отдаёт первую пачку, пока тест не разрешит.</summary>
    private static (FakeReader Reader, TaskCompletionSource Release) Blocked(params ReplayMessage[] messages)
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        return (new FakeReader(messages) { BeforeRead = async call => { if (call == 1) await release.Task; } }, release);
    }

    [Fact]
    public async Task ConcurrentStarts_CreateExactlyOneJob_OthersGetConflictWithActiveId()
    {
        var (reader, release) = Blocked(Valid("a", 0));
        var rig = new Rig(_ => ReplayOpenResult.Opened(reader));

        var results = await Task.WhenAll(Enumerable.Range(0, 12).Select(_ => Task.Run(() => rig.Manager.StartAsync(Rig.Request, CancellationToken.None).AsTask())));

        var started = Assert.Single(results, r => r.Status == StartStatus.Started);
        Assert.All(results.Where(r => r.Status != StartStatus.Started), r => Assert.Equal((StartStatus.Conflict, started.Job!.Id), (r.Status, r.ActiveJobId)));
        Assert.Equal(1, rig.Source.Opened);

        release.SetResult();
        var finished = await rig.FinishedAsync(started.Job!.Id);
        Assert.Equal(ReprocessJobState.Completed, finished.State);
        Assert.Equal(StartStatus.Started, (await rig.Manager.StartAsync(Rig.Request, CancellationToken.None)).Status);
    }

    [Fact]
    public async Task InvalidArguments_AreRejectedBeforeOpeningSource_AndSourceErrorsFreeTheSlot()
    {
        var calls = 0;
        var rig = new Rig(_ => ++calls == 1
            ? ReplayOpenResult.Invalid([new ReprocessValidationError("from", "from is earlier than the oldest retained message; earliest 2026-11-20T00:00:00.000Z")])
            : calls == 2 ? throw new ReplaySourceException("Kafka: down") : ReplayOpenResult.Opened(new FakeReader([])));

        var tooFast = await rig.Manager.StartAsync(Rig.Request with { MaxRate = 100_000 }, CancellationToken.None);
        var retention = await rig.Manager.StartAsync(Rig.Request, CancellationToken.None);
        var broker = await rig.Manager.StartAsync(Rig.Request, CancellationToken.None);
        var ok = await rig.Manager.StartAsync(Rig.Request, CancellationToken.None);

        Assert.Equal((StartStatus.Invalid, "maxRate"), (tooFast.Status, tooFast.Errors.Single().Field));
        Assert.Equal((StartStatus.Invalid, "from"), (retention.Status, retention.Errors.Single().Field));
        Assert.Equal(StartStatus.SourceUnavailable, broker.Status);
        Assert.Equal(StartStatus.Started, ok.Status);
        Assert.Equal(3, rig.Source.Opened);
        Assert.Equal(ok.Job!.Id, Assert.Single(rig.Manager.List()).Id);
    }

    [Fact]
    public async Task Cancel_ActiveJobEndsCancelled_FinishedAndUnknownAreReported()
    {
        var (reader, release) = Blocked(Valid("a", 0), Valid("b", 1), Valid("c", 2), Valid("d", 3));
        var rig = new Rig(_ => ReplayOpenResult.Opened(reader));
        var job = await rig.StartedAsync();

        Assert.Equal(ReportStatus.NotFinished, rig.Manager.GetReport(job.Id, 10, 0).Status);
        Assert.Equal(CancelStatus.Accepted, rig.Manager.Cancel(job.Id));
        release.SetResult();
        var finished = await rig.FinishedAsync(job.Id);

        Assert.Equal((ReprocessJobState.Cancelled, ReprocessOutcome.Cancelled), (finished.State, finished.Outcome));
        Assert.True(reader.Disposed);
        Assert.Equal(CancelStatus.AlreadyFinished, rig.Manager.Cancel(job.Id));
        Assert.Equal(CancelStatus.NotFound, rig.Manager.Cancel(Guid.NewGuid()));
        Assert.Equal(ReportStatus.NotFound, rig.Manager.GetReport(Guid.NewGuid(), 10, 0).Status);
        Assert.Null(rig.Manager.Get(Guid.NewGuid()));
    }

    [Fact]
    public async Task History_KeepsTenNewestJobs()
    {
        var rig = new Rig(_ => ReplayOpenResult.Opened(new FakeReader([])));
        var ids = new List<Guid>();
        for (var i = 0; i < 12; i++)
        {
            var job = await rig.StartedAsync();
            await rig.FinishedAsync(job.Id);
            ids.Add(job.Id);
        }

        var list = rig.Manager.List();

        Assert.Equal(ids.AsEnumerable().Reverse().Take(10), list.Select(j => j.Id));
        Assert.Null(rig.Manager.Get(ids[0]));
    }

    [Fact]
    public async Task Report_IsPagedAndCapped_ButCountersStayComplete()
    {
        var rig = new Rig(
            _ => ReplayOpenResult.Opened(new FakeReader(Enumerable.Range(0, 5).Select(i => Valid($"e{i}", i)).ToArray())),
            new ScriptedStage((_, _) => ScriptedStage.Reject("rule_x")),
            maxReportEntries: 2);
        var job = await rig.StartedAsync();
        var finished = await rig.FinishedAsync(job.Id);

        var page = rig.Manager.GetReport(job.Id, limit: 1, offset: 1);

        Assert.Equal(5, finished.Counters.RejectedAgain["rule_x"]);
        Assert.Equal(ReportStatus.Ok, page.Status);
        Assert.Equal((2, 1, 1, true), (page.Page!.Total, page.Page.Entries.Count, page.Page.Offset, page.Page.Truncated));
        Assert.Equal(1, page.Page.Entries[0].Offset);
    }

    [Fact]
    public async Task SnapshotReportAndLogs_NeverContainMessageData()
    {
        var stage = new ScriptedStage((id, _) => id == "rejected" ? ScriptedStage.Reject("rule_x") : null);
        var rig = new Rig(_ => ReplayOpenResult.Opened(new FakeReader([Valid("rejected", 0, personId: Marker), Valid("bad", 1, personId: Marker)])), stage);
        rig.Fixture.Writer.Fail = (_, ids) => ids.Contains("bad") ? new LeakyDbException(Marker, "23514") : null;
        var job = await rig.StartedAsync();

        var finished = await rig.FinishedAsync(job.Id);

        var everything = JsonSerializer.Serialize(finished)
            + JsonSerializer.Serialize(rig.Manager.GetReport(job.Id, 100, 0).Page)
            + string.Join('\n', rig.Logger.Collector.GetSnapshot().Select(r => r.Message))
            + string.Join('\n', rig.Fixture.Logger.Collector.GetSnapshot().Select(r => r.Message));
        Assert.Equal(ReprocessOutcome.CompletedWithFailures, finished.Outcome);
        Assert.DoesNotContain(Marker, everything);
    }

    [Fact]
    public async Task HostStop_CancelsTheActiveJob_AndRefusesNewOnes()
    {
        var (reader, release) = Blocked(Valid("a", 0));
        var rig = new Rig(_ => ReplayOpenResult.Opened(reader));
        var job = await rig.StartedAsync();

        var stopping = rig.Manager.StopAsync(CancellationToken.None);
        release.SetResult();
        await stopping.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(ReprocessJobState.Cancelled, rig.Manager.Get(job.Id)!.State);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await rig.Manager.StartAsync(Rig.Request, CancellationToken.None));
    }

    private sealed class DelegateSource(Func<ReprocessRequest, CancellationToken, ValueTask<ReplayOpenResult>> open) : IReplaySource
    {
        public int Opened { get; private set; }

        public ValueTask<ReplayOpenResult> OpenAsync(ReprocessRequest request, CancellationToken cancellationToken)
        {
            Opened++;
            return open(request, cancellationToken);
        }
    }

    /// <summary>Раннер, поведение которого подменяет тест; без подмены - настоящее исполнение.</summary>
    private sealed class HookedRunner(ReprocessFixture fixture, Func<ReprocessJob, IReplayReader, CancellationToken, Task<ReprocessRunResult>>? hook)
        : ReprocessRunner(
            new IngestionPipeline(new FormatStage(new EventDecoder()), []),
            fixture.Writer,
            fixture.Settings,
            () => new BackoffPolicy(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30), 2, 0, () => 0.5),
            fixture.Time,
            NullLogger<ReprocessRunner>.Instance)
    {
        public int Runs { get; private set; }

        public override Task<ReprocessRunResult> RunAsync(ReprocessJob job, IReplayReader reader, CancellationToken stopping)
        {
            Runs++;
            return hook is null ? base.RunAsync(job, reader, stopping) : hook(job, reader, stopping);
        }
    }

    /// <summary>Читатель, у которого сбоит нужное место: чтение границ либо закрытие. Считает закрытия.</summary>
    private sealed class FaultyReader(bool partitionsThrow = false, bool disposeThrows = false) : IReplayReader
    {
        public int Disposals { get; private set; }

        public IReadOnlyList<ReplayPartition> Partitions =>
            partitionsThrow ? throw new InvalidOperationException(Marker) : [new ReplayPartition("ktm.events.dlq", 0, 0, 0)];

        public ValueTask<IReadOnlyList<ReplayMessage>> ReadBatchAsync(int maxCount, CancellationToken cancellationToken) =>
            ValueTask.FromResult<IReadOnlyList<ReplayMessage>>([]);

        public ValueTask DisposeAsync()
        {
            Disposals++;
            return disposeThrows ? throw new InvalidOperationException(Marker) : ValueTask.CompletedTask;
        }
    }

    private sealed class OpenRig
    {
        public readonly FakeLogger<ReprocessJobManager> Logger = new();
        public readonly ReprocessFixture Fixture = new();
        public readonly HookedRunner Runner;
        public readonly DelegateSource Source;
        public readonly ReprocessJobManager Manager;

        public OpenRig(Func<ReprocessRequest, CancellationToken, ValueTask<ReplayOpenResult>> open, Func<ReprocessJob, IReplayReader, CancellationToken, Task<ReprocessRunResult>>? run = null)
        {
            Source = new DelegateSource(open);
            Runner = new HookedRunner(Fixture, run);
            Manager = new ReprocessJobManager(Source, Runner, Fixture.Settings, Fixture.Time, Logger);
        }

        public string Log => string.Join('\n', Logger.Collector.GetSnapshot().Select(r => r.Message));

        public async Task<ReprocessJobSnapshot> StartedAndFinishedAsync()
        {
            var result = await Manager.StartAsync(Rig.Request, CancellationToken.None);
            Assert.Equal(StartStatus.Started, result.Status);
            await Manager.WhenFinishedAsync(result.Job!.Id).WaitAsync(TimeSpan.FromSeconds(10));
            return Manager.Get(result.Job.Id)!;
        }
    }

    private static ValueTask<ReplayOpenResult> Opened(IReplayReader reader) => ValueTask.FromResult(ReplayOpenResult.Opened(reader));

    [Fact]
    public async Task MissingSourceTopic_IsSourceUnavailable_WithSafeLog_AndSlotStaysFree()
    {
        const string marker = "sensitive-marker";
        var calls = 0;
        var rig = new OpenRig((_, _) => ++calls == 1
            ? throw new ReplaySourceException(marker)
            : Opened(new FakeReader([])));

        var missing = await rig.Manager.StartAsync(Rig.Request, CancellationToken.None);
        var next = await rig.Manager.StartAsync(Rig.Request, CancellationToken.None);

        Assert.Equal((StartStatus.SourceUnavailable, 0), (missing.Status, missing.Errors.Count));
        Assert.Equal(StartStatus.Started, next.Status);
        var entry = Assert.Single(rig.Logger.Collector.GetSnapshot(), r => r.Level == Microsoft.Extensions.Logging.LogLevel.Error);
        Assert.Contains("source_unavailable", entry.Message);
        Assert.Contains(nameof(ReplaySourceException), entry.Message);
        Assert.DoesNotContain(marker, entry.Message);
        Assert.DoesNotContain(entry.StructuredState!, item => item.Value?.ToString()?.Contains(marker) == true);
    }

    [Fact]
    public async Task InvalidReason_IsRejectedBeforeTheSlotIsTaken_AndTheSourceIsNotOpened()
    {
        var rig = new OpenRig((_, _) => Opened(new FakeReader([])));

        var multiline = await rig.Manager.StartAsync(Rig.Request with { Reason = "a\nb" }, CancellationToken.None);
        var ok = await rig.Manager.StartAsync(Rig.Request with { Reason = "write_data_error" }, CancellationToken.None);

        Assert.Equal((StartStatus.Invalid, "reason"), (multiline.Status, multiline.Errors.Single().Field));
        Assert.Equal(StartStatus.Started, ok.Status);
        Assert.Equal(1, rig.Source.Opened);
    }

    [Fact]
    public async Task SlotIsFreeTheMomentAJobIsObservedFinished()
    {
        var rig = new OpenRig((_, _) => Opened(new FakeReader([])));

        for (var i = 0; i < 300; i++)
        {
            var started = await rig.Manager.StartAsync(Rig.Request, CancellationToken.None);
            Assert.Equal(StartStatus.Started, started.Status);

            var deadline = DateTime.UtcNow.AddSeconds(10);
            while (rig.Manager.Get(started.Job!.Id)!.IsActive)
            {
                Assert.True(DateTime.UtcNow < deadline, "the job did not finish");
                await Task.Yield();
            }
        }
    }

    [Fact]
    public async Task FailureAfterTheSlotIsTaken_ClosesTheReader_AndFreesTheSlot()
    {
        var readers = new List<FaultyReader>();
        var rig = new OpenRig((_, _) =>
        {
            var reader = new FaultyReader(partitionsThrow: readers.Count == 0);
            readers.Add(reader);
            return Opened(reader);
        });

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await rig.Manager.StartAsync(Rig.Request, CancellationToken.None));

        Assert.Equal(1, readers[0].Disposals);
        Assert.Empty(rig.Manager.List());
        await rig.StartedAndFinishedAsync();
        await rig.Manager.StopAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10));
        Assert.DoesNotContain(Marker, rig.Log);
    }

    [Fact]
    public async Task HostStop_WhileTheSourceIsOpening_CancelsTheOpening_FreesTheSlot_AndStartsNoJob()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var rig = new OpenRig(async (_, token) =>
        {
            entered.SetResult();
            await Task.Delay(Timeout.Infinite, token);
            return ReplayOpenResult.Opened(new FaultyReader());
        });

        var start = rig.Manager.StartAsync(Rig.Request, CancellationToken.None).AsTask();
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var stop = rig.Manager.StopAsync(CancellationToken.None);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => start);
        await stop.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Empty(rig.Manager.List());
        Assert.Equal(0, rig.Runner.Runs);
    }

    [Fact]
    public async Task HostStop_ThatArrivesWhileOpeningIgnoresTheToken_StillClosesTheReaderAndWaitsForIt()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var proceed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reader = new FaultyReader();
        var rig = new OpenRig(async (_, _) =>
        {
            entered.SetResult();
            await proceed.Task;
            return ReplayOpenResult.Opened(reader);
        });

        var start = rig.Manager.StartAsync(Rig.Request, CancellationToken.None).AsTask();
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var stop = rig.Manager.StopAsync(CancellationToken.None);
        Assert.False(stop.IsCompleted);
        proceed.SetResult();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => start);
        await stop.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(1, reader.Disposals);
        Assert.Equal(0, rig.Runner.Runs);
        Assert.Empty(rig.Manager.List());
    }

    [Fact]
    public async Task RunnerException_FailsTheJobWithInternal_ClosesTheReader_FreesTheSlot_LogsOnlyTheType()
    {
        var readers = new List<FaultyReader>();
        var rig = new OpenRig(
            (_, _) =>
            {
                readers.Add(new FaultyReader());
                return Opened(readers[^1]);
            },
            (_, _, _) => throw new InvalidOperationException(Marker));

        var finished = await rig.StartedAndFinishedAsync();
        var again = await rig.Manager.StartAsync(Rig.Request, CancellationToken.None);

        Assert.Equal((ReprocessJobState.Failed, ReprocessOutcome.Failed, ReprocessErrorCodes.Internal), (finished.State, finished.Outcome, finished.ErrorCode));
        Assert.Equal(1, readers[0].Disposals);
        Assert.Equal(StartStatus.Started, again.Status);
        Assert.Contains("InvalidOperationException", rig.Log);
        Assert.DoesNotContain(Marker, rig.Log);
    }

    [Fact]
    public async Task ReaderCloseFailure_DoesNotBreakFinishing_AndIsLoggedByTypeOnly()
    {
        var rig = new OpenRig((_, _) => Opened(new FaultyReader(disposeThrows: true)));

        var finished = await rig.StartedAndFinishedAsync();
        var again = await rig.Manager.StartAsync(Rig.Request, CancellationToken.None);

        Assert.Equal(ReprocessJobState.Completed, finished.State);
        Assert.Equal(StartStatus.Started, again.Status);
        Assert.Contains(rig.Logger.Collector.GetSnapshot(), r => r.Level == Microsoft.Extensions.Logging.LogLevel.Warning && r.Message.Contains("InvalidOperationException"));
        Assert.DoesNotContain(Marker, rig.Log);
    }
}
