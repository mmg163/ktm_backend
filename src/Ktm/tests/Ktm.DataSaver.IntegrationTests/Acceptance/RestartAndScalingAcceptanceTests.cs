using Ktm.DataSaver.Application.Batching;
using Ktm.DataSaver.Application.Ingestion;
using Ktm.DataSaver.Infrastructure.Kafka;
using Ktm.DataSaver.IntegrationTests.Host;
using Ktm.DataSaver.IntegrationTests.Kafka;
using Ktm.DataSaver.IntegrationTests.Postgres;
using Ktm.DataSaver.IntegrationTests.Support;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Ktm.DataSaver.IntegrationTests.Acceptance;

/// <summary>Остановка посреди обработки (7.5) и масштабирование копиями (7.7).</summary>
[Collection(AppCollection.Name)]
public sealed class RestartAndScalingAcceptanceTests(KafkaFixture kafka, PostgresFixture postgres)
{
    private const int Total = 5000;

    private Task<TestRun> NewRunAsync(int partitions) => TestRun.CreateAsync(kafka.BootstrapServers, postgres, partitions);

    private static Random NewRandom(out int seed)
    {
        seed = Environment.TickCount;
        return new Random(seed);
    }

    // 7.5 (а): штатная остановка в случайный момент и повторный запуск; повторно прочитанных сообщений нет.
    [Fact]
    public async Task GracefulStopAtRandomMoment_ThenRestart_Exactly5000Keys_NothingReread()
    {
        var run = await NewRunAsync(partitions: 3);
        await run.PublishEventsAsync(0, Total);
        var random = NewRandom(out var seed);
        var stopAfter = random.Next(500, 2500);

        long read1, written1, committed1;
        await using (var first = DataSaverInstance.Start(run))
        {
            await Wait.UntilAsync(() => first.Counters.Written >= stopAfter, $"{stopAfter} events written (seed {seed})");
            await first.StopAsync();
            (read1, written1) = (first.Counters.Read, first.Counters.Written);
            Assert.Equal(0, first.Terminator.Calls);
        }

        committed1 = await run.CommittedAsync();
        // Всё прочитанное дописано и зафиксировано: остановка не теряет и не оставляет хвост без commit.
        Assert.Equal(read1, written1);
        Assert.Equal(read1, committed1);
        Assert.Equal(written1, await run.RowsAsync());
        Assert.InRange(read1, stopAfter, Total - 1);

        await using var second = DataSaverInstance.Start(run);
        await Wait.UntilAsync(async () => await run.CommittedAsync() == Total, "all 5000 offsets committed after restart");

        Assert.Equal(Total, await run.RowsAsync());
        Assert.Equal(0L, await run.DuplicateKeyGroupsAsync());
        Assert.Equal(Total - read1, second.Counters.Read);
        Assert.Equal(0, second.Counters.Duplicates);
    }

    // 7.5 (б): аварийное завершение без commit. Для группы это то же состояние, что после kill процесса или Dispose consumer
    // без Close: записанное в БД остаётся, offsets не дошли до брокера. Commit отключается подменой порта IOffsetCommitter.
    [Fact]
    public async Task CrashWithoutCommit_ThenRestart_Exactly5000Keys_RedeliveredAreDeduplicated()
    {
        var run = await NewRunAsync(partitions: 3);
        await run.PublishEventsAsync(0, Total);
        var random = NewRandom(out var seed);
        var stopAfter = random.Next(1500, 3500);

        long written1, committed1;
        await using (var first = DataSaverInstance.Start(run, services: s =>
                     {
                         s.RemoveAll<IOffsetCommitter>();
                         s.AddSingleton<IOffsetCommitter>(sp => new DroppingCommitter(new KafkaOffsetCommitter(sp.GetRequiredService<KafkaMessageSource>()), commitsKept: 2));
                     }))
        {
            await Wait.UntilAsync(() => first.Counters.Written >= stopAfter, $"{stopAfter} events written (seed {seed})");
            await first.StopAsync();
            written1 = first.Counters.Written;
        }

        committed1 = await run.CommittedAsync();
        Assert.True(committed1 < written1, $"the crash must leave written events without a committed offset (committed {committed1}, written {written1})");
        Assert.Equal(written1, await run.RowsAsync());

        await using var second = DataSaverInstance.Start(run);
        await Wait.UntilAsync(async () => await run.CommittedAsync() == Total, "all 5000 offsets committed after restart");

        Assert.Equal(Total, await run.RowsAsync());
        Assert.Equal(0L, await run.DuplicateKeyGroupsAsync());
        // Заново прочитано всё после committed offset; записанное до аварии гасится ключом, а не дублируется.
        Assert.Equal(Total - committed1, second.Counters.Read);
        Assert.Equal(written1 - committed1, second.Counters.Duplicates);
        Assert.Equal(Total - written1, second.Counters.Written);
    }

    // 7.7: два экземпляра одной группы делят партиции без пересечений; после остановки одного второй забирает все.
    [Fact]
    public async Task TwoInstancesInOneGroup_SplitPartitions_ThenOneStops_EveryEventOnce()
    {
        const int partitions = 4;
        var run = await NewRunAsync(partitions);
        await using var a = DataSaverInstance.Start(run);
        await a.WaitReadyAsync();
        await using var b = DataSaverInstance.Start(run);
        await b.WaitReadyAsync();

        GroupSnapshot? snapshot = null;
        await Wait.UntilAsync(async () =>
        {
            snapshot = await run.Kafka.DescribeGroupAsync(run.Group, run.RawTopic);
            return snapshot is { Members.Count: 2 } && snapshot.Members.Values.All(p => p.Count > 0)
                   && snapshot.Members.Values.Sum(p => p.Count) == partitions;
        }, "partitions split between two members", lastObservation: () => snapshot is null ? "no group" : string.Join(";", snapshot.Members.Select(m => $"{m.Key[..8]}:{string.Join(",", m.Value)}")));
        var union = snapshot!.Members.Values.SelectMany(p => p).ToHashSet();
        Assert.Equal(partitions, union.Count);

        await run.PublishEventsAsync(0, 400);
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 400, "first 400 committed by two instances");
        Assert.True(a.Counters.Read > 0 && b.Counters.Read > 0, $"both instances must work: a={a.Counters.Read}, b={b.Counters.Read}");
        Assert.Equal(400, a.Counters.Read + b.Counters.Read);

        await b.StopAsync();
        await Wait.UntilAsync(async () => (await run.Kafka.DescribeGroupAsync(run.Group, run.RawTopic)) is { Members.Count: 1 } g
                                          && g.Members.Values.Single().Count == partitions, "the remaining instance owns all partitions");
        await run.PublishEventsAsync(400, 400);
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 800, "all 800 committed");

        Assert.Equal(800L, await run.RowsAsync());
        Assert.Equal(0L, await run.DuplicateKeyGroupsAsync());
        Assert.Equal(800, a.Counters.Written + b.Counters.Written);
        Assert.Equal(0, a.Counters.Duplicates + b.Counters.Duplicates);
    }

    /// <summary>Пропускает commit после первых <c>commitsKept</c> вызовов: записанное в БД не фиксируется в Kafka, как при падении процесса.</summary>
    private sealed class DroppingCommitter(IOffsetCommitter inner, int commitsKept) : IOffsetCommitter
    {
        private int _calls;

        public ValueTask CommitAsync(IReadOnlyList<PartitionOffset> offsets, CancellationToken cancellationToken) =>
            Interlocked.Increment(ref _calls) <= commitsKept ? inner.CommitAsync(offsets, cancellationToken) : ValueTask.CompletedTask;
    }
}
