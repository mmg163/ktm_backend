using System.Net;
using Ktm.DataSaver.IntegrationTests.Host;
using Ktm.DataSaver.IntegrationTests.Kafka;
using Ktm.DataSaver.IntegrationTests.Support;
using Microsoft.Extensions.Logging;

namespace Ktm.DataSaver.IntegrationTests.Acceptance;

/// <summary>Недоступность БД (7.4, 7.6): собственный контейнер Postgres с фиксированным host-портом, останавливается и запускается.</summary>
[Collection(AppCollection.Name)]
public sealed class OutageAcceptanceTests(KafkaFixture kafka)
{
    private const int BackoffInitialMs = 200;
    private const int BackoffMaxMs = 2000;
    private const double Jitter = 0.2;

    private static void Backoff(Dictionary<string, string?> settings)
    {
        settings["DataSaver:Backoff:InitialMs"] = BackoffInitialMs.ToString(System.Globalization.CultureInfo.InvariantCulture);
        settings["DataSaver:Backoff:MaxMs"] = BackoffMaxMs.ToString(System.Globalization.CultureInfo.InvariantCulture);
    }

    // 7.4: committed offset не растёт, пока БД недоступна, и растёт только после появления строк (commit строго после commit БД).
    [Fact]
    public async Task DatabaseDown_CommittedOffsetStaysPut_ThenGrowsOnlyBehindRowsInDatabase()
    {
        await using var db = await StoppableDatabase.StartAsync();
        var run = await TestRun.CreateAsync(kafka.BootstrapServers, db);
        await using var app = DataSaverInstance.Start(run, Backoff);
        await app.WaitReadyAsync();

        await run.PublishEventsAsync(0, 20);
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 20, "first 20 events committed");

        await db.StopAsync();
        await run.PublishEventsAsync(20, 40);
        await Wait.UntilAsync(() => app.Counters["ktm.datasaver.retries[write]"] >= 3, "several failed write attempts of the pending batch");
        Assert.Equal(HttpStatusCode.ServiceUnavailable, await app.ReadyStatusAsync());
        Assert.Equal(20L, await run.CommittedAsync());

        await db.RestartAsync();
        // Инвариант на каждом замере: committed читается раньше, чем число строк; commit offset не обгоняет БД.
        var observation = string.Empty;
        await Wait.UntilAsync(async () =>
        {
            var committed = await run.CommittedAsync();
            var rows = await run.RowsAsync();
            observation = $"committed={committed} rows={rows}";
            Assert.True(committed <= rows, "committed offset overtook the journal: " + observation);
            return committed == 60;
        }, "all 60 events committed after the database is back", lastObservation: () => observation);

        Assert.Equal(60L, await run.RowsAsync());
        Assert.Equal(0L, await run.DuplicateKeyGroupsAsync());
    }

    // 7.6 (обычный прогон): 15 с простоя.
    [Fact]
    public Task DatabaseDownFor15Seconds_NoLossNoDuplicates_NoRestart_NoRebalance_BackoffGrows() =>
        RunOutageAsync(TimeSpan.FromSeconds(15));

    // 7.6 (Long): 2 минуты простоя, как в ТЗ.
    [Fact]
    [Trait("Category", "Long")]
    public Task DatabaseDownFor2Minutes_NoLossNoDuplicates_NoRestart_NoRebalance_BackoffCapped() =>
        RunOutageAsync(TimeSpan.FromMinutes(2));

    private async Task RunOutageAsync(TimeSpan outage)
    {
        await using var db = await StoppableDatabase.StartAsync();
        var run = await TestRun.CreateAsync(kafka.BootstrapServers, db);
        await using var app = DataSaverInstance.Start(run, Backoff);
        await app.WaitReadyAsync();

        const int before = 100;
        await run.PublishEventsAsync(0, before);
        await Wait.UntilAsync(async () => await run.CommittedAsync() == before, "events before the outage committed");
        var memberBefore = (await run.Kafka.DescribeGroupAsync(run.Group, run.RawTopic))!.Members.Single().Key;

        await db.StopAsync();
        var outageStarted = DateTime.UtcNow;
        var published = before;
        // Поток событий не прекращается на время простоя (пауза между порциями - темп генератора, а не синхронизация).
        while (DateTime.UtcNow - outageStarted < outage)
        {
            await run.PublishEventsAsync(published, 50);
            published += 50;
            await Task.Delay(1000);
        }

        await db.RestartAsync();
        await Wait.UntilAsync(async () => await run.CommittedAsync() == published, $"all {published} events committed after recovery", TimeSpan.FromSeconds(120));

        Assert.Equal((long)published, await run.RowsAsync());
        Assert.Equal(0L, await run.DuplicateKeyGroupsAsync());
        // Процесс не перезапускался, цикл жив, consumer остался тем же участником группы без ребалансировки.
        Assert.Equal(0, app.Terminator.Calls);
        Assert.Equal(HttpStatusCode.OK, await app.ReadyStatusAsync());
        var after = (await run.Kafka.DescribeGroupAsync(run.Group, run.RawTopic))!;
        Assert.Equal(memberBefore, after.Members.Single().Key);
        Assert.DoesNotContain(app.Logs.GetSnapshot(), r => r.Message.StartsWith("Partitions revoked", StringComparison.Ordinal));
        AssertBackoffIntervals(app, outage);
    }

    /// <summary>
    /// Интервалы между неудачными попытками: n-я пауза = min(Initial * 2^n, Max) с джиттером +-20% и потолком Max после джиттера.
    /// Снизу проверяем строго (минус погрешность таймера), сверху - потолок плюс время самой попытки.
    /// </summary>
    private static void AssertBackoffIntervals(DataSaverInstance app, TimeSpan outage)
    {
        var attempts = app.Logs.GetSnapshot()
            .Where(r => r.Level == LogLevel.Warning && r.Message.StartsWith("Batch write failed: ", StringComparison.Ordinal))
            .Select(r => r.Timestamp)
            .Order()
            .ToList();
        Assert.True(attempts.Count >= 5, $"expected several failed attempts during the outage, got {attempts.Count}");
        // Неудач за простой не больше, чем позволяет минимальная пауза: повтор не крутится без пауз.
        Assert.True(attempts.Count <= outage.TotalMilliseconds / (BackoffMaxMs * (1 - Jitter)) + 12, $"too many attempts: {attempts.Count}");

        var tolerance = TimeSpan.FromMilliseconds(150);
        for (var n = 0; n + 1 < attempts.Count; n++)
        {
            var baseMs = Math.Min(BackoffInitialMs * Math.Pow(2, n), BackoffMaxMs);
            var interval = attempts[n + 1] - attempts[n];
            var minimum = TimeSpan.FromMilliseconds(baseMs * (1 - Jitter)) - tolerance;
            var maximum = TimeSpan.FromMilliseconds(Math.Min(baseMs * (1 + Jitter), BackoffMaxMs)) + TimeSpan.FromSeconds(3);
            Assert.True(interval >= minimum && interval <= maximum,
                $"attempt {n + 1}: interval {interval.TotalMilliseconds:F0} ms is outside [{minimum.TotalMilliseconds:F0}, {maximum.TotalMilliseconds:F0}] ms");
        }
    }
}
