using System.Net;
using System.Text;
using Confluent.Kafka;
using Ktm.DataSaver.IntegrationTests.Host;
using Ktm.DataSaver.IntegrationTests.Kafka;
using Ktm.DataSaver.IntegrationTests.Postgres;
using Ktm.DataSaver.IntegrationTests.Support;
using Npgsql;

namespace Ktm.DataSaver.IntegrationTests.Acceptance;

[Collection(AppCollection.Name)]
public sealed class DlqAcceptanceTests(KafkaFixture kafka, PostgresFixture postgres)
{
    [Fact]
    public async Task TemporaryDbError_EventWrittenAfterRetry_NotInDlq()
    {
        await using var db = await StoppableDatabase.StartAsync();
        var run = await TestRun.CreateAsync(kafka.BootstrapServers, db);
        var dlq = await run.Kafka.CreateTopicAsync($"ktm.events.dlq.t{Guid.NewGuid():N}");
        await using var app = DataSaverInstance.Start(run, settings => settings["DataSaver:Kafka:DlqTopic"] = dlq);
        await app.WaitReadyAsync();
        await db.StopAsync();
        await run.PublishEventsAsync(0, 1);
        await Wait.UntilAsync(() => app.Counters["ktm.datasaver.retries[write]"] >= 1, "database write retry");
        await Task.Delay(TimeSpan.FromSeconds(15));
        await db.RestartAsync();
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 1, "event committed after recovery", TimeSpan.FromSeconds(120));
        Assert.Equal(1, await run.RowsAsync());
        Assert.Equal(0, await run.DuplicateKeyGroupsAsync());
        Assert.Empty(run.Kafka.Read(dlq, 1, TimeSpan.FromSeconds(2)));
        Assert.Equal(0, app.Terminator.Calls);
    }

    [Fact]
    public async Task PoisonMessage_GoesToDlq_NeighboursWritten_PartitionContinues()
    {
        var run = await TestRun.CreateAsync(kafka.BootstrapServers, postgres);
        var dlq = await run.Kafka.CreateTopicAsync($"ktm.events.dlq.t{Guid.NewGuid():N}");
        await using var constraint = await PoisonConstraint.AddAsync(postgres.AdminConnectionString);
        await using var app = DataSaverInstance.Start(run, settings => settings["DataSaver:Kafka:DlqTopic"] = dlq);
        await app.WaitReadyAsync();
        var body = run.Json(PoisonConstraint.PoisonId);
        await run.PublishAsync([("before", run.Json(run.EventId(0))), ("poison-key", body), ("after", run.Json(run.EventId(1)))]);
        using var sourceReader = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            GroupId = $"source-reader-{Guid.NewGuid():N}",
            EnableAutoCommit = false,
        }).Build();
        sourceReader.Assign(new TopicPartitionOffset(run.RawTopic, 0, 1));
        var sourceRecord = sourceReader.Consume(TimeSpan.FromSeconds(15));
        Assert.NotNull(sourceRecord);
        Assert.Equal(1, sourceRecord.Offset.Value);
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 3, "poison batch committed", TimeSpan.FromSeconds(120));
        await run.PublishEventsAsync(2, 1);
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 4, "following event committed");
        Assert.Equal(3, await run.RowsAsync());
        Assert.Equal(0, await run.DuplicateKeyGroupsAsync());
        await using var connection = new NpgsqlConnection(postgres.AdminConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT count(*) FROM ktm.events WHERE source_event_id = 'poison'", connection);
        Assert.Equal(0L, (long)(await command.ExecuteScalarAsync())!);
        var record = Assert.Single(run.Kafka.Read(dlq, 2, TimeSpan.FromSeconds(3)));
        Assert.Equal("poison-key", Encoding.UTF8.GetString(record.Key!));
        Assert.Equal(body, record.Value);
        Assert.Equal("write_data_error", record.Headers["ktm-dlq-reason"]);
        Assert.Equal("write", record.Headers["ktm-dlq-stage"]);
        Assert.False(string.IsNullOrWhiteSpace(record.Headers["ktm-dlq-exception-type"]));
        Assert.True(int.Parse(record.Headers["ktm-dlq-attempts"], System.Globalization.CultureInfo.InvariantCulture) >= 3);
        Assert.Equal(run.RawTopic, record.Headers["ktm-source-topic"]);
        Assert.Equal("0", record.Headers["ktm-source-partition"]);
        Assert.Equal("1", record.Headers["ktm-source-offset"]);
        Assert.True(DateTimeOffset.TryParse(record.Headers["ktm-source-timestamp"], out var sourceTimestamp));
        Assert.Equal(sourceRecord.Message.Timestamp.UnixTimestampMs, sourceTimestamp.ToUnixTimeMilliseconds());
        Assert.Equal(HttpStatusCode.OK, await app.ReadyStatusAsync());
    }
}
