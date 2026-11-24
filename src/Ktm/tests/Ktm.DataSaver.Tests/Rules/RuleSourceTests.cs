using System.Threading.Channels;
using Ktm.DataSaver.Core.Rules;
using Microsoft.Extensions.Time.Testing;
using static Ktm.DataSaver.Tests.Rules.RuleTestSupport;

namespace Ktm.DataSaver.Tests.Rules;

/// <summary>Источник правил «из памяти» без БД: доказывает, что движок зависит только от порта (требование «Расширяемость»).</summary>
public class RuleSourceTests
{
    private sealed class InMemoryRuleSource : IRuleSource
    {
        public string Revision = "rev-1";
        public List<RuleDefinition> Rules = [];
        public List<RuleProblem> Problems = [];
        public Exception? Failure;
        public List<string?> Requested { get; } = [];

        public ValueTask<RuleSourceResult> ReadAsync(string? previousRevision, CancellationToken cancellationToken)
        {
            Requested.Add(previousRevision);
            if (Failure is not null)
            {
                throw Failure;
            }

            return ValueTask.FromResult(previousRevision == Revision
                ? RuleSourceResult.NotChanged(Revision)
                : RuleSourceResult.Changed(Revision, Rules.ToList(), Problems.ToList()));
        }
    }

    private sealed class ManualSignal : IRuleChangeSignal
    {
        private readonly Channel<RuleChangeHint> _hints = Channel.CreateUnbounded<RuleChangeHint>();

        public void Raise() => _hints.Writer.TryWrite(new RuleChangeHint("test"));

        public IAsyncEnumerable<RuleChangeHint> WatchAsync(CancellationToken cancellationToken) => _hints.Reader.ReadAllAsync(cancellationToken);
    }

    private static (RuleSnapshotRefresher Refresher, RuleSnapshotHolder Holder, FakeTimeProvider Time) Create(InMemoryRuleSource source)
    {
        var time = new FakeTimeProvider();
        var holder = new RuleSnapshotHolder();
        return (new RuleSnapshotRefresher(source, new RuleSnapshotBuilder(new ConditionCompiler()), holder, time), holder, time);
    }

    [Fact]
    public async Task Refresh_LoadsReplacesOnRevisionChange_AndSkipsRebuildWhenUnchanged()
    {
        var source = new InMemoryRuleSource { Rules = [Rule("one")] };
        var (refresher, holder, time) = Create(source);
        Assert.False(holder.HasLoaded);
        Assert.Empty(holder.Current.Rules);

        var first = await refresher.RefreshAsync(CancellationToken.None);
        var firstSnapshot = holder.Current;
        time.Advance(TimeSpan.FromSeconds(60));
        var same = await refresher.RefreshAsync(CancellationToken.None);

        Assert.Equal(RuleRefreshOutcome.Changed, first.Outcome);
        Assert.Equal(RuleRefreshOutcome.Unchanged, same.Outcome);
        Assert.Same(firstSnapshot, holder.Current);
        Assert.Equal(time.GetUtcNow(), holder.LastSuccessfulCheck);
        Assert.Equal([null, "rev-1"], source.Requested);

        source.Revision = "rev-2";
        source.Rules = [Rule("one", version: 2), Rule("two")];
        var replaced = await refresher.RefreshAsync(CancellationToken.None);

        Assert.Equal(RuleRefreshOutcome.Changed, replaced.Outcome);
        Assert.Equal("rev-2", holder.Current.Revision);
        Assert.Equal(["one", "two"], holder.Current.Rules.Select(r => r.Definition.Id));
        Assert.Equal([new RuleRef("two", 1)], holder.Current.Changes.Added);
    }

    [Fact]
    public async Task Refresh_SourceFailure_KeepsPreviousSnapshotAndPropagatesKind()
    {
        var source = new InMemoryRuleSource { Rules = [Rule("one")] };
        var (refresher, holder, time) = Create(source);
        await refresher.RefreshAsync(CancellationToken.None);
        var before = holder.Current;
        var checkedAt = holder.LastSuccessfulCheck;

        source.Failure = new RuleSourceException(RuleSourceFailureKind.Unavailable, "db down");
        time.Advance(TimeSpan.FromMinutes(5));
        var error = await Assert.ThrowsAsync<RuleSourceException>(async () => await refresher.RefreshAsync(CancellationToken.None));

        Assert.Equal(RuleSourceFailureKind.Unavailable, error.Kind);
        Assert.Same(before, holder.Current);
        Assert.Equal(checkedAt, holder.LastSuccessfulCheck);
        Assert.True(holder.HasLoaded);
    }

    [Fact]
    public async Task Refresh_SourceProblemOfOneRule_DoesNotAffectOthers()
    {
        var source = new InMemoryRuleSource
        {
            Rules = [Rule("good")],
            Problems = [new RuleProblem("broken-row", "condition", "не удалось отобразить")],
        };
        var (refresher, holder, _) = Create(source);

        await refresher.RefreshAsync(CancellationToken.None);

        Assert.Equal(["good"], holder.Current.Rules.Select(r => r.Definition.Id));
        Assert.Equal("broken-row", Assert.Single(holder.Current.Errors).RuleId);
    }

    [Fact]
    public async Task ChangeSignal_TriggersOutOfBandRefresh_BeforeTheTimerFires()
    {
        var source = new InMemoryRuleSource { Rules = [Rule("one")] };
        var (refresher, holder, time) = Create(source);
        await refresher.RefreshAsync(CancellationToken.None);
        var signal = new ManualSignal();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var triggers = RuleRefreshTriggers
            .WatchAsync(TimeSpan.FromSeconds(60), time, signal, cts.Token).GetAsyncEnumerator(cts.Token);

        source.Revision = "rev-2";
        source.Rules = [Rule("one"), Rule("two")];
        var pending = triggers.MoveNextAsync();
        signal.Raise();
        Assert.True(await pending);
        Assert.Equal(RuleRefreshTrigger.Signal, triggers.Current);
        await refresher.RefreshAsync(CancellationToken.None);

        Assert.Equal("rev-2", holder.Current.Revision);

        var timerWait = triggers.MoveNextAsync();
        time.Advance(TimeSpan.FromSeconds(60));
        Assert.True(await timerWait);
        Assert.Equal(RuleRefreshTrigger.Timer, triggers.Current);
    }

    [Fact]
    public async Task Refresh_UnchangedOnFirstLoad_IsSourceErrorAndLeavesEmptySnapshot()
    {
        var source = new UnchangedAlwaysSource();
        var holder = new RuleSnapshotHolder();
        var refresher = new RuleSnapshotRefresher(source, new RuleSnapshotBuilder(new ConditionCompiler()), holder, new FakeTimeProvider());

        var error = await Assert.ThrowsAsync<RuleSourceException>(async () => await refresher.RefreshAsync(CancellationToken.None));

        Assert.Equal(RuleSourceFailureKind.Invalid, error.Kind);
        Assert.False(holder.HasLoaded);
        Assert.Same(RuleSnapshot.Empty, holder.Current);
    }

    private sealed class UnchangedAlwaysSource : IRuleSource
    {
        public ValueTask<RuleSourceResult> ReadAsync(string? previousRevision, CancellationToken cancellationToken) =>
            ValueTask.FromResult(RuleSourceResult.NotChanged("rev"));
    }

    [Fact]
    public async Task Triggers_WithoutSignal_AreTimerOnly()
    {
        var time = new FakeTimeProvider();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var triggers = RuleRefreshTriggers
            .WatchAsync(TimeSpan.FromSeconds(60), time, null, cts.Token).GetAsyncEnumerator(cts.Token);

        for (var i = 0; i < 2; i++)
        {
            var next = triggers.MoveNextAsync();
            Assert.False(next.IsCompleted);
            time.Advance(TimeSpan.FromSeconds(60));
            Assert.True(await next);
            Assert.Equal(RuleRefreshTrigger.Timer, triggers.Current);
        }
    }
}
