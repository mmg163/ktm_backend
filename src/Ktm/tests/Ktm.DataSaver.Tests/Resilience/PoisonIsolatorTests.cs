using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Resilience;
using Ktm.DataSaver.Tests.Ingestion;

namespace Ktm.DataSaver.Tests.Resilience;

public class PoisonIsolatorTests
{
    private static readonly DateTimeOffset ReceivedAt = new(2026, 11, 24, 10, 0, 0, TimeSpan.Zero);

    /// <summary>Хранилище: пачка, содержащая ядовитый идентификатор, отвергается; остальное записывается.</summary>
    private sealed class FakeStore : IEventStore
    {
        public HashSet<string> Poison { get; init; } = [];

        public HashSet<string> Written { get; } = [];

        public List<string[]> Calls { get; } = [];

        public List<DateTimeOffset> ReceivedAts { get; } = [];

        /// <summary>Исключение вместо записи для вызова с номером n (с 1).</summary>
        public Func<int, Exception?> FailCall { get; set; } = _ => null;

        public Func<string[], Exception?> Reject { get; set; } = _ => null;

        public ValueTask WriteAsync(IReadOnlyList<ParsedEvent> events, DateTimeOffset receivedAt, CancellationToken cancellationToken)
        {
            var ids = events.Select(e => e.SourceEventId).ToArray();
            Calls.Add(ids);
            ReceivedAts.Add(receivedAt);
            cancellationToken.ThrowIfCancellationRequested();
            if (FailCall(Calls.Count) is { } call)
            {
                throw call;
            }

            if (Reject(ids) is { } rejected)
            {
                throw rejected;
            }

            if (ids.Any(Poison.Contains))
            {
                throw new FakeDbException("23514");
            }

            Written.UnionWith(ids);
            return ValueTask.CompletedTask;
        }
    }

    private static ParsedEvent Event(string id) =>
        new(1, "skud", id, ReceivedAt, "access", null, "p", default, null, default);

    private static List<string> Ids(int count) => Enumerable.Range(0, count).Select(i => $"e{i:D3}").ToList();

    private static ValueTask<IsolationResult<string>> Isolate(
        FakeStore store, IReadOnlyList<string> items, Action<IReadOnlyList<string>>? onWritten = null, Action? touch = null, CancellationToken ct = default) =>
        new PoisonIsolator(store).IsolateAsync(items, id => Event(id), ReceivedAt, onWritten, touch, ct);

    [Fact]
    public async Task OnePoisonOutOf500_IsFoundWithLogarithmicNumberOfWrites_AndEverythingElseIsWrittenOnce()
    {
        var items = Ids(500);
        var store = new FakeStore { Poison = ["e347"] };

        var result = await Isolate(store, items);

        Assert.Equal(IsolationOutcome.Completed, result.Outcome);
        Assert.Equal("e347", Assert.Single(result.Poison).Item);
        Assert.Equal(WriteErrorClass.Data, result.Poison[0].Class);
        Assert.Equal(499, result.Written.Count);
        Assert.Equal(499, store.Written.Count);
        Assert.DoesNotContain("e347", store.Written);
        // Корневая пачка заведомо упала и не пишется; 2 записи на уровень деления: 2 * ceil(log2 500) + 1.
        Assert.True(store.Calls.Count <= 2 * (int)Math.Ceiling(Math.Log2(500)) + 1, $"writes: {store.Calls.Count}");
        // Каждое сообщение записано ровно в одной успешной части: повторной записи записанного нет.
        Assert.Equal(499, store.Calls.Where(c => !c.Contains("e347")).SelectMany(c => c).Count());
        Assert.All(store.ReceivedAts, r => Assert.Equal(ReceivedAt, r));
    }

    [Fact]
    public async Task TwoPoisonInDifferentHalves_AreBothFound_AndAllPoisonBatchEndsWithNothingWritten()
    {
        var items = Ids(16);
        var two = await Isolate(new FakeStore { Poison = ["e002", "e013"] }, items);
        var all = await Isolate(new FakeStore { Poison = [.. items] }, items);

        Assert.Equal(["e002", "e013"], two.Poison.Select(p => p.Item).Order());
        Assert.Equal(14, two.Written.Count);
        Assert.Equal(16, all.Poison.Count);
        Assert.Empty(all.Written);
        Assert.Equal(IsolationOutcome.Completed, all.Outcome);
    }

    [Fact]
    public async Task SingleMessageBatch_IsConfirmedByAWrite_AndIsPoisonWhenRejected()
    {
        var poison = new FakeStore { Poison = ["e000"] };
        var ok = new FakeStore();

        var rejected = await Isolate(poison, ["e000"]);
        var accepted = await Isolate(ok, ["e000"]);

        Assert.Equal("e000", Assert.Single(rejected.Poison).Item);
        Assert.Single(poison.Calls);
        Assert.Equal(["e000"], accepted.Written);
        Assert.Empty(accepted.Poison);
    }

    [Fact]
    public async Task UnexpectedErrorOnSingleMessage_IsPoison_ButInfrastructureErrorInterruptsAndKeepsTheRemainder()
    {
        var items = Ids(4);
        var unexpected = new FakeStore { Reject = ids => ids.Contains("e001") ? new InvalidOperationException("bug") : null };
        var unexpectedResult = await Isolate(unexpected, items);

        // Инфраструктура падает на второй записи: первая часть записана, остаток вернулся для продолжения.
        var broken = new FakeStore { FailCall = call => call == 2 ? new IOException("db down") : null };
        var interrupted = await Isolate(broken, items);

        Assert.Equal("e001", Assert.Single(unexpectedResult.Poison).Item);
        Assert.Equal(WriteErrorClass.Unexpected, unexpectedResult.Poison[0].Class);
        Assert.Equal(IsolationOutcome.Interrupted, interrupted.Outcome);
        Assert.IsType<IOException>(interrupted.InterruptedBy);
        Assert.Equal(["e000", "e001"], interrupted.Written);
        Assert.Equal(["e002", "e003"], interrupted.Remaining);

        // Повторный вызов с остатком не пишет уже записанное и доводит изоляцию до конца.
        var resumed = await Isolate(broken, interrupted.Remaining);
        Assert.Equal(IsolationOutcome.Completed, resumed.Outcome);
        Assert.Equal(["e000", "e001", "e002", "e003"], broken.Written.Order());
        Assert.Equal(1, broken.Calls.Count(c => c.SequenceEqual(["e000", "e001"])));
    }

    [Fact]
    public async Task Cancellation_ReturnsRemainder_AndCallbacksFireAfterEveryWrite()
    {
        using var cts = new CancellationTokenSource();
        var store = new FakeStore { FailCall = call => { if (call == 1) cts.Cancel(); return null; } };
        var written = new List<string[]>();
        var touches = 0;

        var result = await Isolate(store, Ids(8), part => written.Add(part.ToArray()), () => touches++, cts.Token);

        Assert.Equal(IsolationOutcome.Cancelled, result.Outcome);
        Assert.Equal(4, result.Written.Count);
        Assert.Equal(4, result.Remaining.Count);
        Assert.Single(written);
        Assert.Equal(1, touches);
    }

    [Fact]
    public async Task CorrectionAndOriginalInDifferentParts_AreBothWritten()
    {
        // Исправление и исходное делятся пополам: порядок записи не важен, обе записи проходят (ADR-0002 D-4).
        var items = new List<string> { "original", "poison", "x", "correction" };
        var store = new FakeStore { Poison = ["poison"] };

        var result = await Isolate(store, items);

        Assert.Equal(["correction", "original", "x"], result.Written.Order());
        Assert.Equal(["correction", "original", "x"], store.Written.Order());
        Assert.Equal("poison", Assert.Single(result.Poison).Item);
    }

    [Fact]
    public async Task OnPoison_IsInterleavedWithOnWritten_InTheOrderOfTheBatch()
    {
        // V P V P: событие «записано» и «ядовитое» приходят в колбэки в порядке следования сообщений пачки.
        var items = new List<string> { "e000", "e001", "e002", "e003" };
        var store = new FakeStore { Poison = ["e001", "e003"] };
        var order = new List<string>();

        var result = await new PoisonIsolator(store).IsolateAsync(
            items, id => Event(id), ReceivedAt,
            part => order.AddRange(part.Select(id => "written:" + id)),
            touch: null, CancellationToken.None,
            onPoison: poison => order.Add("poison:" + poison.Item));

        Assert.Equal(["written:e000", "poison:e001", "written:e002", "poison:e003"], order);
        Assert.Equal(["e001", "e003"], result.Poison.Select(p => p.Item));
    }

    [Fact]
    public async Task FailedWrites_CountEveryFailedPartThatContainedTheEvent_AndSurviveAnInterruptedIsolation()
    {
        // Пачка 4, ядовитое третье: упали части [2,3] и [2] - по две неудачи; соседи в упавшей части [2,3] тоже получают одну.
        var events = Ids(4).ToDictionary(id => id, id => Event(id));
        var counts = new Dictionary<ParsedEvent, int>(ReferenceEqualityComparer.Instance);
        var continuous = await new PoisonIsolator(new FakeStore { Poison = ["e002"] }).IsolateAsync(
            events.Keys.ToList(), id => events[id], ReceivedAt, null, null, CancellationToken.None, failedWrites: counts);

        Assert.Equal("e002", Assert.Single(continuous.Poison).Item);
        Assert.Equal(2, counts[events["e002"]]);
        Assert.Equal(1, counts[events["e003"]]);
        Assert.False(counts.ContainsKey(events["e000"]));

        // Та же изоляция, прерванная инфраструктурной ошибкой на 3-м вызове и продолженная с тем же словарём: итог тот же.
        var resumed = new Dictionary<ParsedEvent, int>(ReferenceEqualityComparer.Instance);
        var store = new FakeStore { Poison = ["e002"], FailCall = call => call == 3 ? new IOException("db down") : null };
        var isolator = new PoisonIsolator(store);
        var first = await isolator.IsolateAsync(events.Keys.ToList(), id => events[id], ReceivedAt, null, null, CancellationToken.None, failedWrites: resumed);
        Assert.Equal(IsolationOutcome.Interrupted, first.Outcome);
        var second = await isolator.IsolateAsync(first.Remaining, id => events[id], ReceivedAt, null, null, CancellationToken.None, failedWrites: resumed);

        Assert.Equal(IsolationOutcome.Completed, second.Outcome);
        Assert.Equal(counts[events["e002"]], resumed[events["e002"]]);
    }
}
