using System.Net;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Ktm.DataSaver.Infrastructure.Observability;
using Ktm.DataSaver.IntegrationTests.Kafka;
using Ktm.DataSaver.IntegrationTests.Postgres;
using Ktm.DataSaver.IntegrationTests.Support;
using Ktm.HostedServices.Health;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;

namespace Ktm.DataSaver.IntegrationTests.Host;

[Collection(AppCollection.Name)]
public sealed class DataSaverHostTests(KafkaFixture kafka, PostgresFixture postgres)
{
    private static string EventJson(string id) => """
        {"schemaVersion":1,"source":"skud","sourceEventId":"ID","occurredAt":"2026-11-19T04:58:12Z",
         "eventType":"access_granted","deviceId":"turnstile-01-entry","externalPersonId":"card:0012345678",
         "payload":{"direction":"in","zone":"main-gate"}}
        """.Replace("ID", id, StringComparison.Ordinal);

    private static async Task<(HttpStatusCode Status, string Body)> GetAsync(HttpClient client, string path)
    {
        using var response = await client.GetAsync(path);
        return (response.StatusCode, await response.Content.ReadAsStringAsync());
    }

    private static Task WaitReadyAsync(HttpClient client) =>
        Wait.UntilAsync(async () => (await GetAsync(client, "/health/ready")).Status == HttpStatusCode.OK, "readiness 200");

    [Fact]
    public async Task HealthyService_IsReady_AndMetricsMatchTheRun_90Written10Duplicates1Rejected()
    {
        var raw = await kafka.CreateTopicAsync("ktm.events.raw");
        var rejected = await kafka.CreateTopicAsync("ktm.events.rejected");
        var prefix = Guid.NewGuid().ToString("N");
        var db = new NpgsqlConnectionStringBuilder(postgres.AppConnectionString);
        await using var app = new DataSaverAppFactory(DataSaverAppFactory.Settings(kafka.BootstrapServers, raw, rejected, $"g-{Guid.NewGuid():N}", db));
        using var client = app.CreateClient();
        await WaitReadyAsync(client);

        var live = await GetAsync(client, "/health/live");
        var ready = await GetAsync(client, "/health/ready");
        Assert.Equal(HttpStatusCode.OK, live.Status);
        Assert.Equal(HttpStatusCode.OK, ready.Status);
        Assert.DoesNotContain("app-test-password", live.Body + ready.Body);
        Assert.DoesNotContain(db.Host!, live.Body + ready.Body);

        using var counters = new MetricCounters(app.Services.GetRequiredService<DataSaverMetrics>().Meter);
        for (var i = 0; i < 90; i++) await kafka.ProduceAsync(raw, "skud:" + i, EventJson($"{prefix}-{i}"));
        for (var i = 0; i < 10; i++) await kafka.ProduceAsync(raw, "skud:" + i, EventJson($"{prefix}-{i}"));
        await kafka.ProduceAsync(raw, "bad", "not json");

        await Wait.UntilAsync(
            () => Task.FromResult(counters["ktm.datasaver.messages.read"] >= 101
                                  && counters["ktm.datasaver.messages.rejected[malformed_json]"] == 1
                                  && counters["ktm.datasaver.events.written"] + counters["ktm.datasaver.events.duplicates"] == 100),
            "all messages processed");

        Assert.Equal(101, counters["ktm.datasaver.messages.read"]);
        Assert.Equal(90, counters["ktm.datasaver.events.written"]);
        Assert.Equal(10, counters["ktm.datasaver.events.duplicates"]);
        Assert.Equal(1, counters["ktm.datasaver.messages.rejected[malformed_json]"]);
        await using var connection = new NpgsqlConnection(postgres.AdminConnectionString);
        await connection.OpenAsync();
        await using var count = new NpgsqlCommand("SELECT count(*) FROM ktm.events WHERE source_event_id LIKE @p", connection);
        count.Parameters.AddWithValue("p", prefix + "-%");
        Assert.Equal(90L, await count.ExecuteScalarAsync());
    }

    [Fact]
    public async Task AdminArea_IsServedOnTheHealthPort_WithSectionsOpenApiAndSwagger_HealthResponsesUnchanged()
    {
        var raw = await kafka.CreateTopicAsync("ktm.events.raw");
        var rejected = await kafka.CreateTopicAsync("ktm.events.rejected");
        var db = new NpgsqlConnectionStringBuilder(postgres.AppConnectionString);
        await using var app = new DataSaverAppFactory(DataSaverAppFactory.Settings(kafka.BootstrapServers, raw, rejected, $"g-{Guid.NewGuid():N}", db));
        using var client = app.CreateClient();
        await WaitReadyAsync(client);

        var sections = await GetAsync(client, "/admin/v1/sections");
        var document = await GetAsync(client, "/admin/openapi/v1.json");
        var swagger = await GetAsync(client, "/admin/swagger/index.html");
        var jobs = await GetAsync(client, "/admin/v1/reprocessing/jobs");
        var ready = await GetAsync(client, "/health/ready");

        Assert.Equal(HttpStatusCode.OK, sections.Status);
        Assert.Contains("\"reprocessing\"", sections.Body);
        Assert.Equal(HttpStatusCode.OK, document.Status);
        Assert.DoesNotContain("/health", document.Body);
        Assert.Equal(HttpStatusCode.OK, swagger.Status);
        Assert.Equal(HttpStatusCode.OK, jobs.Status);
        Assert.Equal(HttpStatusCode.OK, ready.Status);
        Assert.Contains("\"status\"", ready.Body);
    }

    [Fact]
    public async Task SwaggerDisabled_DocumentAndUiAre404_AdminApiStillServed()
    {
        var raw = await kafka.CreateTopicAsync("ktm.events.raw");
        var rejected = await kafka.CreateTopicAsync("ktm.events.rejected");
        var settings = DataSaverAppFactory.Settings(kafka.BootstrapServers, raw, rejected, $"g-{Guid.NewGuid():N}", new NpgsqlConnectionStringBuilder(postgres.AppConnectionString));
        settings["DataSaver:Admin:SwaggerEnabled"] = "false";
        await using var app = new DataSaverAppFactory(settings);
        using var client = app.CreateClient();
        await WaitReadyAsync(client);

        Assert.Equal(HttpStatusCode.NotFound, (await GetAsync(client, "/admin/openapi/v1.json")).Status);
        Assert.Equal(HttpStatusCode.NotFound, (await GetAsync(client, "/admin/swagger/index.html")).Status);
        Assert.Equal(HttpStatusCode.OK, (await GetAsync(client, "/admin/v1/sections")).Status);
    }

    [Fact]
    public async Task DatabaseStopped_ReadinessIs503_ButLivenessStays200()
    {
        var raw = await kafka.CreateTopicAsync("ktm.events.raw");
        var rejected = await kafka.CreateTopicAsync("ktm.events.rejected");
        await using var database = new PostgreSqlBuilder("timescale/timescaledb:2.32.1-pg16").Build();
        await database.StartAsync();
        // Правила читаются при старте: БД без миграции V002 завершила бы процесс кодом 78 (задача 4.2).
        await DatabaseProvisioner.ProvisionAsync(database.GetConnectionString());
        var db = new NpgsqlConnectionStringBuilder(database.GetConnectionString());
        await using var app = new DataSaverAppFactory(DataSaverAppFactory.Settings(kafka.BootstrapServers, raw, rejected, $"g-{Guid.NewGuid():N}", db));
        using var client = app.CreateClient();
        await WaitReadyAsync(client);

        await database.StopAsync();

        await Wait.UntilAsync(async () => (await GetAsync(client, "/health/ready")).Status == HttpStatusCode.ServiceUnavailable, "readiness 503");
        var ready = await GetAsync(client, "/health/ready");
        var live = await GetAsync(client, "/health/live");
        Assert.Contains("database_unavailable", ready.Body);
        Assert.DoesNotContain(db.Password!, ready.Body);
        Assert.Equal(HttpStatusCode.OK, live.Status);
    }

    [Fact]
    public async Task ConsumerWithoutAssignment_ReadinessIs503_LivenessIs200()
    {
        // В топике одна партиция: второй участник группы остаётся без назначения (cooperative-sticky не отнимает её у первого).
        var raw = await kafka.CreateTopicAsync("ktm.events.raw");
        var rejected = await kafka.CreateTopicAsync("ktm.events.rejected");
        var group = $"g-{Guid.NewGuid():N}";
        var db = new NpgsqlConnectionStringBuilder(postgres.AppConnectionString);
        await using var owner = new DataSaverAppFactory(DataSaverAppFactory.Settings(kafka.BootstrapServers, raw, rejected, group, db));
        using var ownerClient = owner.CreateClient();
        await WaitReadyAsync(ownerClient);

        await using var idle = new DataSaverAppFactory(DataSaverAppFactory.Settings(kafka.BootstrapServers, raw, rejected, group, db));
        using var client = idle.CreateClient();

        // Ждём события, а не времени: второй consumer вступил в группу, группа снова стабильна.
        using var admin = kafka.NewAdmin();
        var last = string.Empty;
        await Wait.UntilAsync(async () =>
        {
            try
            {
                var description = (await admin.DescribeConsumerGroupsAsync([group])).ConsumerGroupDescriptions.Single();
                last = $"{description.State} members={description.Members.Count}";
                return description.State == ConsumerGroupState.Stable && description.Members.Count == 2;
            }
            catch (DescribeConsumerGroupsException ex)
            {
                last = ex.Message;
                return false;
            }
        }, "second consumer joined the group", lastObservation: () => last);
        var ready = await GetAsync(client, "/health/ready");
        var live = await GetAsync(client, "/health/live");

        Assert.Equal(HttpStatusCode.ServiceUnavailable, ready.Status);
        Assert.Contains("no_partitions_assigned", ready.Body);
        Assert.Equal(HttpStatusCode.OK, live.Status);
        Assert.Equal(HttpStatusCode.OK, (await GetAsync(ownerClient, "/health/ready")).Status);
    }

    [Fact]
    public async Task MissingTopic_ReadinessIs503NoPartitionsAssigned_NotBrokerUnavailable()
    {
        var rejected = await kafka.CreateTopicAsync("ktm.events.rejected");
        var db = new NpgsqlConnectionStringBuilder(postgres.AppConnectionString);
        await using var app = new DataSaverAppFactory(DataSaverAppFactory.Settings(
            kafka.BootstrapServers, $"ktm.events.missing.{Guid.NewGuid():N}", rejected, $"g-{Guid.NewGuid():N}", db));
        using var client = app.CreateClient();
        // До первой загрузки правил приём не начат и причина готовности - rules_not_loaded (задача 4.3).
        await Wait.UntilAsync(async () => !(await GetAsync(client, "/health/ready")).Body.Contains("rules_not_loaded", StringComparison.Ordinal), "first rule load");

        // Несколько секунд наблюдаем: проба брокера успешна, поэтому причина никогда не должна стать broker_unavailable.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(6);
        while (DateTime.UtcNow < deadline)
        {
            var ready = await GetAsync(client, "/health/ready");
            Assert.Equal(HttpStatusCode.ServiceUnavailable, ready.Status);
            Assert.Contains("no_partitions_assigned", ready.Body);
            Assert.DoesNotContain("broker_unavailable", ready.Body);
            await Task.Delay(500);
        }
    }

    [Fact]
    public async Task BrokerStopped_ReadinessIs503BrokerUnavailableWithinProbeWindow_LivenessStays200()
    {
        // Собственный брокер: общий контейнер нужен остальным тестам. Восстановление брокера здесь не проверяется:
        // после перезапуска контейнера Testcontainers меняется порт, а клиент держит старый адрес (покрыто unit-тестом).
        await using var broker = new KafkaBuilder("apache/kafka:3.9.1").Build();
        await broker.StartAsync();
        var bootstrap = broker.GetBootstrapAddress();
        string topic;
        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build())
        {
            topic = $"ktm.events.raw.{Guid.NewGuid():N}";
            await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }]);
        }

        var db = new NpgsqlConnectionStringBuilder(postgres.AppConnectionString);
        var settings = DataSaverAppFactory.Settings(bootstrap, topic, "ktm.events.rejected", $"g-{Guid.NewGuid():N}", db);
        // Назначение должно пережить остановку брокера дольше окна проверки: иначе ready скажет no_partitions_assigned.
        settings["DataSaver:Kafka:SessionTimeoutMs"] = "60000";
        await using var app = new DataSaverAppFactory(settings);
        using var client = app.CreateClient();
        await WaitReadyAsync(client);

        await broker.StopAsync();

        // Предел по спеке: интервал кэша (5 с) + предел проверки (2 с); запас на опрос и сеть.
        var window = HealthSignals.BrokerCacheTtl + HealthSignals.ProbeTimeout + TimeSpan.FromSeconds(8);
        var deadline = DateTime.UtcNow + window;
        (HttpStatusCode Status, string Body) ready;
        do
        {
            ready = await GetAsync(client, "/health/ready");
            if (ready.Status == HttpStatusCode.ServiceUnavailable) break;
            await Task.Delay(200);
        }
        while (DateTime.UtcNow < deadline);

        var live = await GetAsync(client, "/health/live");
        Assert.Equal(HttpStatusCode.ServiceUnavailable, ready.Status);
        Assert.Contains("broker_unavailable", ready.Body);
        // Адрес брокера в ответ не попадает.
        Assert.DoesNotContain(new Uri("tcp://" + bootstrap.Replace("PLAINTEXT://", string.Empty, StringComparison.Ordinal)).Host, ready.Body);
        Assert.DoesNotContain(bootstrap, ready.Body);
        Assert.Equal(HttpStatusCode.OK, live.Status);
    }
}
