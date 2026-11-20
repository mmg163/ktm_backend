using System.Diagnostics.Metrics;
using System.Net;
using Ktm.DataSaver.Infrastructure.Observability;
using Ktm.DataSaver.IntegrationTests.Kafka;
using Ktm.DataSaver.IntegrationTests.Postgres;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Testcontainers.PostgreSql;

namespace Ktm.DataSaver.IntegrationTests.Host;

[Collection(AppCollection.Name)]
public sealed class DataSaverHostTests(KafkaFixture kafka, PostgresFixture postgres)
{
    private static readonly TimeSpan Overall = TimeSpan.FromSeconds(90);

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

    private static async Task WaitForAsync(Func<Task<bool>> condition, string what)
    {
        var deadline = DateTime.UtcNow + Overall;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition()) return;
            await Task.Delay(200);
        }

        throw new TimeoutException($"Timed out waiting for: {what}");
    }

    private static Task WaitReadyAsync(HttpClient client) =>
        WaitForAsync(async () => (await GetAsync(client, "/health/ready")).Status == HttpStatusCode.OK, "readiness 200");

    private sealed class Counters : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly Dictionary<string, long> _values = [];

        public Counters(Meter meter)
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (ReferenceEquals(instrument.Meter, meter)) listener.EnableMeasurementEvents(instrument);
            };
            _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
            {
                var key = tags.Length == 0 ? instrument.Name : $"{instrument.Name}[{tags[0].Value}]";
                lock (_values) _values[key] = _values.GetValueOrDefault(key) + value;
            });
            _listener.Start();
        }

        public long this[string key]
        {
            get
            {
                lock (_values) return _values.GetValueOrDefault(key);
            }
        }

        public void Dispose() => _listener.Dispose();
    }

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

        using var counters = new Counters(app.Services.GetRequiredService<DataSaverMetrics>().Meter);
        for (var i = 0; i < 90; i++) await kafka.ProduceAsync(raw, "skud:" + i, EventJson($"{prefix}-{i}"));
        for (var i = 0; i < 10; i++) await kafka.ProduceAsync(raw, "skud:" + i, EventJson($"{prefix}-{i}"));
        await kafka.ProduceAsync(raw, "bad", "not json");

        await WaitForAsync(
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
    public async Task DatabaseStopped_ReadinessIs503_ButLivenessStays200()
    {
        var raw = await kafka.CreateTopicAsync("ktm.events.raw");
        var rejected = await kafka.CreateTopicAsync("ktm.events.rejected");
        await using var database = new PostgreSqlBuilder("timescale/timescaledb:2.32.1-pg16").Build();
        await database.StartAsync();
        var db = new NpgsqlConnectionStringBuilder(database.GetConnectionString());
        await using var app = new DataSaverAppFactory(DataSaverAppFactory.Settings(kafka.BootstrapServers, raw, rejected, $"g-{Guid.NewGuid():N}", db));
        using var client = app.CreateClient();
        await WaitReadyAsync(client);

        await database.StopAsync();

        await WaitForAsync(async () => (await GetAsync(client, "/health/ready")).Status == HttpStatusCode.ServiceUnavailable, "readiness 503");
        var ready = await GetAsync(client, "/health/ready");
        var live = await GetAsync(client, "/health/live");
        Assert.Contains("database_unavailable", ready.Body);
        Assert.DoesNotContain(db.Password!, ready.Body);
        Assert.Equal(HttpStatusCode.OK, live.Status);
    }

    [Fact]
    public async Task ConsumerWithoutAssignment_ReadinessIs503_LivenessIs200()
    {
        var missingTopic = $"ktm.events.raw.missing.{Guid.NewGuid():N}";
        var db = new NpgsqlConnectionStringBuilder(postgres.AppConnectionString);
        await using var app = new DataSaverAppFactory(DataSaverAppFactory.Settings(kafka.BootstrapServers, missingTopic, "ktm.events.rejected", $"g-{Guid.NewGuid():N}", db));
        using var client = app.CreateClient();

        await Task.Delay(TimeSpan.FromSeconds(3));
        var ready = await GetAsync(client, "/health/ready");
        var live = await GetAsync(client, "/health/live");

        Assert.Equal(HttpStatusCode.ServiceUnavailable, ready.Status);
        Assert.Contains("no_partitions_assigned", ready.Body);
        Assert.Equal(HttpStatusCode.OK, live.Status);
    }
}
