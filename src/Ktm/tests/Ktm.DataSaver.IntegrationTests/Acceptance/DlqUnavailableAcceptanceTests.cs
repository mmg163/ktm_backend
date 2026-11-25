using System.Net;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Ktm.DataSaver.IntegrationTests.Host;
using Ktm.DataSaver.IntegrationTests.Kafka;
using Ktm.DataSaver.IntegrationTests.Postgres;
using Ktm.DataSaver.IntegrationTests.Support;
using Testcontainers.Kafka;

namespace Ktm.DataSaver.IntegrationTests.Acceptance;

[Collection(AppCollection.Name)]
public sealed class DlqUnavailableAcceptanceTests(PostgresFixture postgres)
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MissingDlq_RetriesWithoutCommit_ThenRecoversOrStops(bool stop)
    {
        await using var kafka = new KafkaBuilder("apache/kafka:3.9.1")
            .WithEnvironment("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false").Build();
        await kafka.StartAsync();
        var run = await TestRun.CreateAsync(kafka.GetBootstrapAddress(), postgres);
        var dlq = $"ktm.events.dlq.t{Guid.NewGuid():N}";
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = run.Bootstrap }).Build();
        var config = await admin.DescribeConfigsAsync([new ConfigResource { Type = ResourceType.Broker, Name = "1" }]);
        Assert.Equal("false", config.Single().Entries["auto.create.topics.enable"].Value);
        Assert.DoesNotContain(admin.GetMetadata(TimeSpan.FromSeconds(10)).Topics, t => t.Topic == dlq);
        await using var constraint = await PoisonConstraint.AddAsync(postgres.AdminConnectionString);
        await using var app = DataSaverInstance.Start(run, settings =>
        {
            settings["DataSaver:Kafka:DlqTopic"] = dlq;
            settings["DataSaver:Kafka:ProducerMessageTimeoutMs"] = "2000";
            settings["DataSaver:Shutdown:TimeoutSeconds"] = "10";
        });
        await app.WaitReadyAsync();
        await run.PublishAsync([("valid", run.Json(run.EventId(0))), ("poison", run.Json(PoisonConstraint.PoisonId))]);
        await Wait.UntilAsync(() => app.Counters["ktm.datasaver.retries[dlq_publish]"] > 0, "DLQ publication retry", TimeSpan.FromSeconds(120));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, await app.ReadyStatusAsync());
        Assert.Equal(0, await run.CommittedAsync());
        Assert.Equal(1, await run.RowsAsync());
        if (stop)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
            await app.StopAsync(timeout.Token);
            Assert.False(timeout.IsCancellationRequested);
            Assert.Equal(0, await run.CommittedAsync());
            Assert.Equal(1, await run.RowsAsync());
            return;
        }
        await run.Kafka.CreateTopicAsync(dlq);
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 2, "DLQ confirmation and commit", TimeSpan.FromSeconds(120));
        Assert.Single(run.Kafka.Read(dlq, 2, TimeSpan.FromSeconds(3)));
        Assert.Equal(1, await run.RowsAsync());
        Assert.Equal(0, await run.DuplicateKeyGroupsAsync());
    }
}
