using System.Diagnostics;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Pipeline;
using Ktm.DataSaver.IntegrationTests.Host;
using Ktm.DataSaver.IntegrationTests.Kafka;
using Ktm.DataSaver.IntegrationTests.Postgres;
using Ktm.DataSaver.IntegrationTests.Support;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Ktm.DataSaver.IntegrationTests.Acceptance;

/// <summary>
/// Сквозные критерии приёмки (группа 7) на общих контейнерах Kafka + TimescaleDB: Data Saver in-process, событие
/// проходит весь путь «топик -> конвейер -> БД -> commit offset». Каждый тест живёт в своих топике, группе и префиксе ключей.
/// </summary>
[Collection(AppCollection.Name)]
public sealed class IngestionAcceptanceTests(KafkaFixture kafka, PostgresFixture postgres)
{
    private Task<TestRun> NewRunAsync(int partitions = 1) => TestRun.CreateAsync(kafka.BootstrapServers, postgres, partitions);

    private static void UseDefaultTimings(Dictionary<string, string?> settings)
    {
        foreach (var key in new[] { "DataSaver:Batch:MaxWaitMs", "DataSaver:Backoff:InitialMs", "DataSaver:Backoff:MaxMs", "DataSaver:PollIntervalMs" })
        {
            settings.Remove(key);
        }
    }

    private static async Task<string> RowJsonAsync(IDatabaseEndpoint db, string sql, string sourceEventId)
    {
        await using var connection = new NpgsqlConnection(db.AdminConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("id", sourceEventId);
        return (string)(await command.ExecuteScalarAsync() ?? "<no row>");
    }

    private static Task<string> EventRowAsync(TestRun run, string id) =>
        RowJsonAsync(run.Db, "SELECT coalesce(row_to_json(e)::text, '<no row>') FROM ktm.events e WHERE source_event_id = @id", id);

    // 7.1 (дымок) и 7.2: одно событие в топике -> строка в ktm.events не позже 5 с при настройках по умолчанию.
    [Fact]
    public async Task SingleEvent_AppearsInJournalWithinFiveSeconds_WithDefaultSettings_UnderAppRole()
    {
        var run = await NewRunAsync();
        await using var app = DataSaverInstance.Start(run, UseDefaultTimings);
        await app.WaitReadyAsync();

        var elapsed = Stopwatch.StartNew();
        await run.PublishEventsAsync(0, 1);
        await Wait.UntilAsync(async () => await run.RowsAsync() == 1, "the event row", TimeSpan.FromSeconds(5));
        elapsed.Stop();

        Assert.True(elapsed.Elapsed < TimeSpan.FromSeconds(5), $"row appeared after {elapsed.Elapsed}");
    }

    // 7.3: 100 сообщений с 10 повторами ключа -> ровно 90 строк, метрика дублей 10, ошибок записи в логе нет.
    [Fact]
    public async Task HundredMessagesWithTenDuplicates_Give90Rows_DuplicatesMetric10_NoErrorsLogged()
    {
        var run = await NewRunAsync();
        await using var app = DataSaverInstance.Start(run);
        await app.WaitReadyAsync();

        await run.PublishEventsAsync(0, 90);
        await run.PublishEventsAsync(0, 10);

        await Wait.UntilAsync(() => app.Counters.Written + app.Counters.Duplicates == 100, "all 100 messages processed");
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 100, "offsets committed");

        Assert.Equal(90L, await run.RowsAsync());
        Assert.Equal(0L, await run.DuplicateKeyGroupsAsync());
        Assert.Equal(90, app.Counters.Written);
        Assert.Equal(10, app.Counters.Duplicates);
        var errors = app.Logs.GetSnapshot().Where(r => r.Level >= LogLevel.Error).Select(r => r.Message).ToList();
        Assert.True(errors.Count == 0, "Error log records: " + string.Join(" | ", errors));
        Assert.DoesNotContain(app.Logs.GetSnapshot(), r => r.Message.Contains("Batch write failed", StringComparison.Ordinal));
    }

    // 7.8: корректировка существующего, корректировка раньше исходного, повтор обеих; исходная строка не меняется.
    [Fact]
    public async Task Corrections_LinkToExistingOriginal_OrStayUnlinked_RepeatsAreIdempotent_OriginalRowUntouched()
    {
        var run = await NewRunAsync();
        await using var app = DataSaverInstance.Start(run);
        await app.WaitReadyAsync();
        const string t0 = "2026-11-19T04:00:00Z";
        const string t1 = "2026-11-19T05:00:00Z";
        const string t2 = "2026-11-19T06:00:00Z";
        string original = run.EventId(1), early = run.EventId(2), earlyTarget = run.EventId(3), following = run.EventId(5);
        var linked = run.EventId(4);

        // Исходное событие, затем корректировка существующего.
        await run.PublishAsync([("k1", run.Json(original, t0))]);
        await Wait.UntilAsync(async () => await run.RowsAsync() == 1, "original row");
        var originalBefore = await EventRowAsync(run, original);
        await run.PublishAsync([("k2", run.Json(linked, t1, correctsId: original, correctsOccurredAt: t0, reason: "ошибочный проход"))]);
        await Wait.UntilAsync(async () => await run.CorrectionRowsAsync() == 1, "linked correction");

        // Корректировка раньше исходного: исходного ещё нет; следом идёт обычное событие - партиция не заблокирована.
        await run.PublishAsync([("k3", run.Json(early, t2, correctsId: earlyTarget, correctsOccurredAt: t1)), ("k4", run.Json(following, t2))]);
        await Wait.UntilAsync(async () => await run.CorrectionRowsAsync() == 2 && await run.RowsAsync() == 4, "early correction and the following event");
        // Исходное событие приходит позже.
        await run.PublishAsync([("k5", run.Json(earlyTarget, t1))]);
        await Wait.UntilAsync(async () => await run.RowsAsync() == 5, "late original");

        // Повторная доставка корректировок: ничего нового.
        await run.PublishAsync([("k2", run.Json(linked, t1, correctsId: original, correctsOccurredAt: t0, reason: "ошибочный проход")),
            ("k3", run.Json(early, t2, correctsId: earlyTarget, correctsOccurredAt: t1))]);
        await Wait.UntilAsync(() => app.Counters.Duplicates == 2, "both redeliveries recognised as duplicates");
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 7, "all offsets committed");

        Assert.Equal(5L, await run.RowsAsync());
        Assert.Equal(2L, await run.CorrectionRowsAsync());
        Assert.Equal(2, app.Counters.CorrectionsWritten);
        Assert.Equal(originalBefore, await EventRowAsync(run, original));

        var linkedRow = await RowJsonAsync(run.Db, CorrectionSql, linked);
        var earlyRow = await RowJsonAsync(run.Db, CorrectionSql, early);
        var originalId = System.Text.Json.JsonDocument.Parse(originalBefore).RootElement.GetProperty("event_id").GetString();
        var linkedDoc = System.Text.Json.JsonDocument.Parse(linkedRow).RootElement;
        Assert.Equal(originalId, linkedDoc.GetProperty("original_event_id").GetString());
        Assert.Equal("ошибочный проход", linkedDoc.GetProperty("reason").GetString());
        Assert.Equal("skud", linkedDoc.GetProperty("actor").GetString());
        Assert.True(linkedDoc.GetProperty("matches_event").GetBoolean(), "correction_event_id must equal event_id of the correction row in events");
        var earlyDoc = System.Text.Json.JsonDocument.Parse(earlyRow).RootElement;
        Assert.Equal(System.Text.Json.JsonValueKind.Null, earlyDoc.GetProperty("original_event_id").ValueKind);
        Assert.True(earlyDoc.GetProperty("matches_event").GetBoolean());
    }

    private const string CorrectionSql = """
        SELECT row_to_json(x)::text FROM (
            SELECT c.original_event_id, c.reason, c.actor,
                   c.correction_event_id = (SELECT e.event_id FROM ktm.events e
                                            WHERE e.source = c.source AND e.source_event_id = c.correction_source_event_id
                                              AND e.occurred_at = c.correction_occurred_at) AS matches_event
            FROM ktm.event_corrections c WHERE c.correction_source_event_id = @id) x
        """;

    // 7.9: schemaVersion 2, не JSON, U+0000, пачка из одних отбракованных; затем обычное событие.
    [Fact]
    public async Task RejectedMessages_GoToRejectedTopicWithHeaders_NothingInJournal_OffsetCommitted_PartitionNotBlocked()
    {
        var run = await NewRunAsync();
        await using var app = DataSaverInstance.Start(run);
        await app.WaitReadyAsync();
        var valid = run.Json(run.EventId(0));
        var bad = new (string Key, string Body, string Reason)[]
        {
            ("skud:v2a", valid.Replace("\"schemaVersion\":1", "\"schemaVersion\":2", StringComparison.Ordinal), RejectReasons.UnknownSchemaVersion),
            ("skud:junk", $"not json at all {run.Prefix}", RejectReasons.MalformedJson),
            ("skud:nul", run.Json(run.EventId(1)).Replace("card:0012345678", "card:\\u0000", StringComparison.Ordinal), RejectReasons.InvalidSchema),
            ("skud:v2b", valid.Replace("\"schemaVersion\":1", "\"schemaVersion\":2", StringComparison.Ordinal), RejectReasons.UnknownSchemaVersion),
            ("skud:junk2", "{\"schemaVersion\":1,", RejectReasons.MalformedJson),
        };

        await run.PublishAsync(bad.Select(b => (b.Key, b.Body)));
        await Wait.UntilAsync(async () => await run.CommittedAsync() == bad.Length, "offsets of the all-rejected batch committed");
        var records = run.Kafka.Read(run.RejectedTopic, bad.Length);

        Assert.Equal(bad.Length, records.Count);
        Assert.Equal(0L, await run.RowsAsync());
        foreach (var record in records)
        {
            var offset = int.Parse(record.Headers["ktm-source-offset"], System.Globalization.CultureInfo.InvariantCulture);
            var expected = bad[offset];
            Assert.Equal(expected.Body, record.Value);
            Assert.Equal(expected.Key, System.Text.Encoding.UTF8.GetString(record.Key!));
            Assert.Equal(expected.Reason, record.Headers["ktm-reject-reason"]);
            Assert.False(string.IsNullOrWhiteSpace(record.Headers["ktm-reject-detail"]));
            Assert.Equal(run.RawTopic, record.Headers["ktm-source-topic"]);
            Assert.Equal("0", record.Headers["ktm-source-partition"]);
            // Отбраковка форматом (не правилом) не несёт идентификатор правила.
            Assert.DoesNotContain("ktm-reject-rule-id", record.Headers.Keys);
        }

        Assert.Equal(bad.Length, app.Counters["ktm.datasaver.messages.rejected[" + RejectReasons.MalformedJson + "]"]
            + app.Counters["ktm.datasaver.messages.rejected[" + RejectReasons.UnknownSchemaVersion + "]"]
            + app.Counters["ktm.datasaver.messages.rejected[" + RejectReasons.InvalidSchema + "]"]);

        // Партиция не заблокирована: следующее обычное событие записывается.
        await run.PublishAsync([("skud:ok", valid)]);
        await Wait.UntilAsync(async () => await run.RowsAsync() == 1, "valid event after the rejected ones");
        await Wait.UntilAsync(async () => await run.CommittedAsync() == bad.Length + 1, "offset of the valid event committed");
    }

    // 7.10: точка расширения - стадия правила отбраковывает каждое третье сообщение в отдельный топик с RuleId.
    [Fact]
    public async Task ExtensionStage_RejectsEveryThirdToItsTopicWithRuleId_OnlyAcceptedInJournal_OffsetIsLastPlusOne()
    {
        const int total = 30;
        const string ruleId = "r-every-third";
        var run = await NewRunAsync();
        var ruleTopic = await run.Kafka.CreateTopicAsync($"ktm.events.rule-rejected.t{Guid.NewGuid():N}");
        await using var app = DataSaverInstance.Start(run, services: s => s.AddSingleton<IIngestionStage>(new EveryThirdMessageRule(ruleId, ruleTopic)));
        await app.WaitReadyAsync();

        await run.PublishEventsAsync(0, total);
        await Wait.UntilAsync(async () => await run.CommittedAsync() == total, "committed offset = last message + 1");
        var records = run.Kafka.Read(ruleTopic, total / 3);

        Assert.Equal(total - total / 3, await run.RowsAsync());
        Assert.Equal(total / 3, records.Count);
        var rejectedOffsets = records.Select(r => long.Parse(r.Headers["ktm-source-offset"], System.Globalization.CultureInfo.InvariantCulture)).Order().ToArray();
        Assert.Equal(Enumerable.Range(0, total / 3).Select(i => (long)(i * 3 + 2)), rejectedOffsets);
        Assert.All(records, r =>
        {
            Assert.Equal("rule_matched", r.Headers["ktm-reject-reason"]);
            Assert.Equal(ruleId, r.Headers["ktm-reject-rule-id"]);
            Assert.Equal(run.RawTopic, r.Headers["ktm-source-topic"]);
        });
        // RuleId доходит и до лога отбраковки именованным параметром.
        var logged = app.Logs.GetSnapshot().Where(r => r.Message.StartsWith("Message rejected", StringComparison.Ordinal)).ToList();
        Assert.Equal(total / 3, logged.Count);
        Assert.All(logged, r => Assert.Equal(ruleId, r.StructuredState!.Single(p => p.Key == "RuleId").Value));
        Assert.Empty(run.Kafka.Read(run.RejectedTopic, 1, TimeSpan.FromSeconds(2)));
    }

    private sealed class EveryThirdMessageRule(string ruleId, string targetTopic) : IIngestionStage
    {
        public string Stage => PipelineStages.Rules;

        public IngestionVerdict Evaluate(ConsumedMessage message, ParsedEvent parsed) =>
            message.Offset % 3 == 2
                ? IngestionVerdict.Reject(new RejectionInfo(RejectionKind.Rule, "rule_matched", "every third", ruleId, targetTopic))
                : IngestionVerdict.Accept;
    }
}
