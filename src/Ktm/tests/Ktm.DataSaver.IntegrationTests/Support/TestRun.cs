using System.Globalization;
using Ktm.DataSaver.IntegrationTests.Postgres;
using Npgsql;

namespace Ktm.DataSaver.IntegrationTests.Support;

/// <summary>
/// Изоляция одного теста в общих контейнерах: свой топик <c>ktm.events.raw.t&lt;guid&gt;</c>, свой топик отбракованных,
/// своя группа и свой префикс <c>sourceEventId</c> (по нему считаются строки журнала: <c>source</c> ограничен схемой).
/// </summary>
internal sealed class TestRun
{
    private const string DefaultOccurredAt = "2026-11-19T04:58:12Z";

    private TestRun(string bootstrap, IDatabaseEndpoint db, string rawTopic, string rejectedTopic)
    {
        Bootstrap = bootstrap;
        Db = db;
        RawTopic = rawTopic;
        RejectedTopic = rejectedTopic;
        Kafka = new KafkaClient(bootstrap);
    }

    public string Bootstrap { get; }

    public IDatabaseEndpoint Db { get; }

    public KafkaClient Kafka { get; }

    public string RawTopic { get; }

    public string RejectedTopic { get; }

    public string Prefix { get; } = Guid.NewGuid().ToString("N")[..12];

    /// <summary>Группа теста. Новые экземпляры Data Saver по умолчанию вступают в неё (перезапуск продолжает с committed offset).</summary>
    public string Group { get; } = $"g-{Guid.NewGuid():N}";

    public static async Task<TestRun> CreateAsync(string bootstrap, IDatabaseEndpoint db, int partitions = 1, string? targetTopicPrefix = null)
    {
        var raw = $"ktm.events.raw.t{Guid.NewGuid():N}";
        var rejected = $"{targetTopicPrefix ?? "ktm.events.rejected"}.t{Guid.NewGuid():N}";
        var client = new KafkaClient(bootstrap);
        await client.CreateTopicAsync(raw, partitions);
        await client.CreateTopicAsync(rejected);
        return new TestRun(bootstrap, db, raw, rejected);
    }

    public string EventId(int number) => $"{Prefix}-{number.ToString(CultureInfo.InvariantCulture)}";

    public string Json(string sourceEventId, string occurredAt = DefaultOccurredAt, string? correctsId = null, string? correctsOccurredAt = null, string? reason = null)
    {
        var correction = correctsId is null
            ? string.Empty
            : $$"""
                ,"correction":{"correctsSourceEventId":"{{correctsId}}","correctsOccurredAt":"{{correctsOccurredAt ?? DefaultOccurredAt}}"{{(reason is null ? string.Empty : $",\"reason\":\"{reason}\"")}}}
                """;
        return $$"""
            {"schemaVersion":1,"source":"skud","sourceEventId":"{{sourceEventId}}","occurredAt":"{{occurredAt}}","eventType":"access_granted","deviceId":"turnstile-01-entry","externalPersonId":"card:0012345678","payload":{"direction":"in","zone":"main-gate"}{{correction}}}
            """;
    }

    public Task PublishAsync(IEnumerable<(string Key, string Body)> messages) => Kafka.ProduceAsync(RawTopic, messages);

    /// <summary>События с номерами from..from+count-1; ключ сообщения как у адаптеров: <c>source:sourceEventId</c>.</summary>
    public Task PublishEventsAsync(int from, int count) =>
        PublishAsync(Enumerable.Range(from, count).Select(i => ($"skud:{EventId(i)}", Json(EventId(i)))));

    public Task<long> CommittedAsync() => Kafka.CommittedAsync(Group, RawTopic);

    public Task<long> RowsAsync() => ScalarAsync("SELECT count(*) FROM ktm.events WHERE source_event_id LIKE @p");

    public Task<long> DuplicateKeyGroupsAsync() => ScalarAsync(
        "SELECT count(*) FROM (SELECT 1 FROM ktm.events WHERE source_event_id LIKE @p GROUP BY source, source_event_id, occurred_at HAVING count(*) > 1) d");

    public Task<long> CorrectionRowsAsync() => ScalarAsync(
        "SELECT count(*) FROM ktm.event_corrections WHERE correction_source_event_id LIKE @p");

    private async Task<long> ScalarAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(Db.AdminConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("p", Prefix + "-%");
        return (long)(await command.ExecuteScalarAsync())!;
    }
}
