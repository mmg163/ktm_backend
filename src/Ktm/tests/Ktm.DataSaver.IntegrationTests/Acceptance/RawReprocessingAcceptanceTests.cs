using System.Text;
using Confluent.Kafka;
using Ktm.DataSaver.Core.Reprocessing;
using Ktm.DataSaver.Hosting;
using Ktm.DataSaver.IntegrationTests.Host;
using Ktm.DataSaver.IntegrationTests.Kafka;
using Ktm.DataSaver.IntegrationTests.Postgres;
using Ktm.DataSaver.IntegrationTests.Support;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;

namespace Ktm.DataSaver.IntegrationTests.Acceptance;

[Collection(AppCollection.Name)]
public sealed class RawReprocessingAcceptanceTests(KafkaFixture kafka, PostgresFixture postgres)
{
    [Fact]
    public async Task Raw_Window_WritesOnlyHalfOpenBoundaries_AndRejectsInvalidRequests()
    {
        await using var database = await PerfDatabase.CreateAsync(postgres.AdminConnectionString);
        var run = await TestRun.CreateAsync(kafka.BootstrapServers, database);
        await using var app = DataSaverInstance.Start(run,
            settings => settings["DataSaver:Rules:Enabled"] = "false",
            services =>
            {
                var worker = services.Single(d => d.ServiceType == typeof(IHostedService) && d.ImplementationType == typeof(DataSaverWorker));
                services.Remove(worker);
            });
        var manager = app.Services.GetRequiredService<ReprocessJobManager>();
        var from = new DateTimeOffset(2026, 11, 20, 10, 0, 0, TimeSpan.Zero);
        var to = from.AddMinutes(5);
        using var producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig { BootstrapServers = kafka.BootstrapServers }).Build();
        var times = new[] { from.AddMilliseconds(-1), from, to.AddMilliseconds(-1), to };
        for (var i = 0; i < times.Length; i++)
        {
            await producer.ProduceAsync(run.RawTopic, new Message<byte[], byte[]>
            {
                Key = Encoding.UTF8.GetBytes(run.EventId(i)),
                Value = Encoding.UTF8.GetBytes(run.Json(run.EventId(i))),
                Timestamp = new Timestamp(times[i]),
            });
        }

        Assert.Equal(0, await run.RowsAsync());
        var started = await manager.StartAsync(new(ReprocessSource.Raw, from, to, null, 100), CancellationToken.None);
        Assert.Equal(StartStatus.Started, started.Status);
        await manager.WhenFinishedAsync(started.Job!.Id).WaitAsync(TimeSpan.FromSeconds(120));
        var result = manager.Get(started.Job.Id)!;
        Assert.Equal(ReprocessOutcome.Succeeded, result.Outcome);
        Assert.Equal(2, result.Counters.Written);
        await using var connection = new NpgsqlConnection(database.AdminConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT source_event_id FROM ktm.events WHERE source_event_id LIKE @prefix ORDER BY source_event_id", connection);
        command.Parameters.AddWithValue("prefix", run.Prefix + "-%");
        var ids = new List<string>();
        await using (var reader = await command.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync()) ids.Add(reader.GetString(0));
        }
        Assert.Equal(new[] { run.EventId(1), run.EventId(2) }, ids);

        using (var admin = kafka.NewAdmin())
            await admin.DeleteRecordsAsync([new TopicPartitionOffset(run.RawTopic, 0, 1)]);
        var retained = await manager.StartAsync(new(ReprocessSource.Raw, from.AddSeconds(-1), to, null, 100), CancellationToken.None);
        Assert.Equal(StartStatus.Invalid, retained.Status);
        Assert.Contains(retained.Errors, e => e.Field == "from");
        var missing = await manager.StartAsync(new(ReprocessSource.Raw, null, null, null, 100), CancellationToken.None);
        Assert.Equal(StartStatus.Invalid, missing.Status);
        Assert.Contains(missing.Errors, e => e.Field == "from");
        Assert.Contains(missing.Errors, e => e.Field == "to");
        var rate = await manager.StartAsync(new(ReprocessSource.Raw, from, to, null, 401), CancellationToken.None);
        Assert.Equal(StartStatus.Invalid, rate.Status);
        Assert.Contains(rate.Errors, e => e.Field == "maxRate");
        Assert.Equal(2, await run.RowsAsync());
    }
}
