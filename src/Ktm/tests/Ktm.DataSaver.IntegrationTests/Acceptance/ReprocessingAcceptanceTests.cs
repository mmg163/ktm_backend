using Ktm.DataSaver.Core.Reprocessing;
using Ktm.DataSaver.IntegrationTests.Host;
using Ktm.DataSaver.IntegrationTests.Kafka;
using Ktm.DataSaver.IntegrationTests.Postgres;
using Ktm.DataSaver.IntegrationTests.Support;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using System.Text.Json;

namespace Ktm.DataSaver.IntegrationTests.Acceptance;

[Collection(AppCollection.Name)]
public sealed class ReprocessingAcceptanceTests(KafkaFixture kafka, PostgresFixture postgres)
{
    [Fact]
    public async Task Rejected_WithRuleEnabled_RejectsAgain_AfterDisable_Writes_AndMalformedRemainsRejected()
    {
        await using var database = await PerfDatabase.CreateAsync(postgres.AdminConnectionString);
        await RuleTable.InsertAsync(database, "replay-rule", "Replay rule", "skud", """{"==": [{"var": "eventType"}, "access_granted"]}""", "replay_rule");
        var run = await TestRun.CreateAsync(kafka.BootstrapServers, database);
        await using var app = DataSaverInstance.Start(run, settings => settings["DataSaver:Rules:RefreshIntervalSeconds"] = "5");
        await app.WaitReadyAsync();
        await run.PublishAsync([("valid", run.Json(run.EventId(1))), ("invalid", "not-json")]);
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 2, "rejections committed", TimeSpan.FromSeconds(120));
        Assert.Equal(2, run.Kafka.Read(run.RejectedTopic, 2).Count);
        Assert.Equal(0, await run.RowsAsync());
        var manager = app.Services.GetRequiredService<ReprocessJobManager>();
        var request = new ReprocessRequest(ReprocessSource.Rejected, null, null, null, 200);
        var first = await manager.StartAsync(request, CancellationToken.None);
        Assert.Equal(StartStatus.Started, first.Status);
        await manager.WhenFinishedAsync(first.Job!.Id).WaitAsync(TimeSpan.FromSeconds(120));
        var firstResult = manager.Get(first.Job.Id)!;
        Assert.Equal(0, firstResult.Counters.Written);
        Assert.Equal(1, firstResult.Counters.RejectedAgain["replay_rule"]);
        Assert.Equal(1, firstResult.Counters.RejectedAgain["malformed_json"]);
        Assert.Equal(2, run.Kafka.Read(run.RejectedTopic, 3, TimeSpan.FromSeconds(2)).Count);
        await RuleTable.UpdateAsync(database, "replay-rule", "enabled = false, version = version + 1");
        await Wait.UntilAsync(() => app.Counters["ktm.datasaver.rules.reloads[changed]"] >= 2, "disabled rule loaded", TimeSpan.FromSeconds(60));
        var second = await manager.StartAsync(request, CancellationToken.None);
        Assert.Equal(StartStatus.Started, second.Status);
        await manager.WhenFinishedAsync(second.Job!.Id).WaitAsync(TimeSpan.FromSeconds(120));
        var secondResult = manager.Get(second.Job.Id)!;
        Assert.Equal(1, secondResult.Counters.Written);
        Assert.Equal(1, secondResult.Counters.RejectedAgain["malformed_json"]);
        Assert.Equal(1, await run.RowsAsync());
        Assert.Equal(2, run.Kafka.Read(run.RejectedTopic, 3, TimeSpan.FromSeconds(2)).Count);
    }

    [Fact]
    public async Task Dlq_AfterFix_WritesOnce_SecondReplayIsDuplicate()
    {
        var run = await TestRun.CreateAsync(kafka.BootstrapServers, postgres);
        var dlq = await run.Kafka.CreateTopicAsync($"ktm.events.dlq.t{Guid.NewGuid():N}");
        await using var constraint = await PoisonConstraint.AddAsync(postgres.AdminConnectionString);
        await using var app = DataSaverInstance.Start(run, settings => settings["DataSaver:Kafka:DlqTopic"] = dlq);
        await app.WaitReadyAsync();
        await run.PublishAsync([("poison", run.Json(constraint.PoisonId))]);
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 1, "poison committed", TimeSpan.FromSeconds(120));
        Assert.Single(run.Kafka.Read(dlq, 1));
        Assert.Equal(0, await PoisonRowsAsync(postgres, constraint.PoisonId));
        await constraint.DropAsync();

        var manager = app.Services.GetRequiredService<ReprocessJobManager>();
        var request = new ReprocessRequest(ReprocessSource.Dlq, null, null, null, 200);
        var first = await manager.StartAsync(request, CancellationToken.None);
        Assert.Equal(StartStatus.Started, first.Status);
        await manager.WhenFinishedAsync(first.Job!.Id).WaitAsync(TimeSpan.FromSeconds(120));
        var firstResult = manager.Get(first.Job.Id)!;
        Assert.Equal(ReprocessOutcome.Succeeded, firstResult.Outcome);
        var diagnostic = JsonSerializer.Serialize(new { firstResult.Counters, firstResult.Partitions, firstResult.TotalMessages, firstResult.ErrorCode, Report = manager.GetReport(first.Job.Id, 100, 0) });
        Assert.True(firstResult.Counters.Written == 1, diagnostic);
        Assert.Equal(1, await PoisonRowsAsync(postgres, constraint.PoisonId));
        var second = await manager.StartAsync(request, CancellationToken.None);
        Assert.Equal(StartStatus.Started, second.Status);
        await manager.WhenFinishedAsync(second.Job!.Id).WaitAsync(TimeSpan.FromSeconds(120));
        var secondResult = manager.Get(second.Job.Id)!;
        Assert.Equal(ReprocessOutcome.Succeeded, secondResult.Outcome);
        Assert.Equal(0, secondResult.Counters.Written);
        Assert.Equal(1, secondResult.Counters.Duplicates);
        Assert.Equal(1, await PoisonRowsAsync(postgres, constraint.PoisonId));
        Assert.Equal(1, await run.CommittedAsync());
    }

    private static async Task<long> PoisonRowsAsync(PostgresFixture postgres, string poisonId)
    {
        await using var connection = new NpgsqlConnection(postgres.AdminConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT count(*) FROM ktm.events WHERE source = @source AND source_event_id = @id AND occurred_at = @occurred", connection);
        command.Parameters.AddWithValue("source", "skud");
        command.Parameters.AddWithValue("id", poisonId);
        command.Parameters.AddWithValue("occurred", new DateTimeOffset(2026, 11, 19, 4, 58, 12, TimeSpan.Zero));
        return (long)(await command.ExecuteScalarAsync())!;
    }
}
