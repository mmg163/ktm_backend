using System.Text;
using System.Text.RegularExpressions;
using Ktm.DataSaver.Application.Rules;
using Ktm.DataSaver.IntegrationTests.Host;
using Ktm.DataSaver.IntegrationTests.Kafka;
using Ktm.DataSaver.IntegrationTests.Postgres;
using Ktm.DataSaver.IntegrationTests.Support;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace Ktm.DataSaver.IntegrationTests.Acceptance;

/// <summary>
/// Задача 9.6 (ADR-0005 п. 2.7): оператор <c>regex</c> на живом сервисе. Kafka + TimescaleDB в контейнерах, правила вносятся SQL-ом
/// под <c>ktm_migrator</c>; каждый тест работает в собственной БД.
/// </summary>
[Collection(AppCollection.Name)]
public sealed class RegexRulesAcceptanceTests(KafkaFixture kafka, PostgresFixture postgres)
{
    private const string OccurredAt = "2026-11-19T04:58:12Z";
    private const string Unique = "card:0012345678";

    private static void FastRefresh(Dictionary<string, string?> settings)
    {
        settings["DataSaver:Rules:RefreshIntervalSeconds"] = "5";
        settings["DataSaver:Backoff:InitialMs"] = "200";
        settings["DataSaver:Backoff:MaxMs"] = "1000";
    }

    private static (string Key, string Body) Msg(TestRun run, int number, string source, string eventType, string payload, string? deviceId = null)
    {
        var body = RuleMessages.Body(source, run.EventId(number), OccurredAt, eventType, payload);
        if (deviceId is not null)
        {
            body = body.Replace($"\"deviceId\":\"{RuleMessages.DeviceFor(source)}\"", $"\"deviceId\":\"{deviceId}\"", StringComparison.Ordinal);
        }

        return ($"{source}:{run.EventId(number)}", body);
    }

    private static string? Field(Microsoft.Extensions.Logging.Testing.FakeLogRecord record, string name) =>
        record.StructuredState!.SingleOrDefault(p => p.Key == name).Value;

    // (а) и (б): невалидный regex не ломает сервис; корректный regex применяется и отбраковывает сквозь весь конвейер.
    [Fact]
    public async Task InvalidRegexRules_DoNotBreakTheService_GoodRegexRuleRejectsEndToEnd_ErrorsLoggedWithoutValues_ThenFixedWithoutRestart()
    {
        await using var database = await PerfDatabase.CreateAsync(postgres.AdminConnectionString);
        await RuleTable.InsertAsync(database, "it-rx-open", "regex: незакрытая группа", "skud", """{"regex": [{"var": "payload.s"}, "("]}""", "bad_open");
        await RuleTable.InsertAsync(database, "it-rx-look", "regex: просмотр вперёд", "skud", """{"regex": [{"var": "payload.s"}, "(?=a)"]}""", "bad_look");
        await RuleTable.InsertAsync(database, "it-rx-big", "regex: слишком большой автомат", "skud", """{"regex": [{"var": "payload.s"}, "(a{1000}){1000}"]}""", "bad_big");
        // CHECK V002 код rule_eval_timeout не знает: БД строку принимает, сервис правило не применяет (код зарезервирован)
        await RuleTable.InsertAsync(database, "it-rx-reserved", "Зарезервированный код", "skud", """{"==": [{"var": "eventType"}, "reserved_probe"]}""", "rule_eval_timeout");
        await RuleTable.InsertAsync(database, "it-rx-device", "regex: тестовое устройство", "skud", """{"regex": [{"var": "deviceId"}, "^TEST-"]}""", "test_device");
        await RuleTable.InsertAsync(database, "it-rx-payload", "regex: payload.s начинается с 1", "skud", """{"regex": [{"var": "payload.s"}, "^1"]}""", "payload_one");
        await RuleTable.InsertAsync(database, "it-plain", "Тестовое событие", "skud", """{"==": [{"var": "eventType"}, "test_event"]}""", "test_data");
        var run = await TestRun.CreateAsync(kafka.BootstrapServers, database);
        await using var app = DataSaverInstance.Start(run, FastRefresh);
        await app.WaitReadyAsync();

        var messages = new[]
        {
            Msg(run, 0, "skud", "access_granted", "{\"s\":\"a\"}"),                     // подошло бы просмотру вперёд: в БД, правило не применяется
            Msg(run, 1, "skud", "reserved_probe", "{}"),                                 // подошло бы правилу с зарезервированным кодом: в БД
            Msg(run, 2, "skud", "access_granted", "{}", deviceId: "TEST-1"),             // regex по deviceId: отбраковано
            Msg(run, 3, "skud", "test_event", "{}"),                                     // правило без regex: отбраковано
            Msg(run, 4, "skud", "access_granted", "{}", deviceId: "PROD-1"),             // не совпало: в БД
            Msg(run, 5, "skud", "access_granted", "{\"s\":1}"),                          // число в поле-строке не совпадает: в БД
            Msg(run, 6, "skud", "access_granted", "{\"s\":\"1x\"}"),                     // совпало с ^1: отбраковано
        };
        await run.PublishAsync(messages);
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 7, "seven messages committed", TimeSpan.FromSeconds(60));

        Assert.Equal(4L, await RuleTable.EventCountAsync(database));
        var rejected = run.Kafka.Read(run.RejectedTopic, 3);
        Assert.Equal(3, rejected.Count);
        var byRule = rejected.ToDictionary(r => r.Headers["ktm-reject-rule-id"]);
        Assert.Equal(["it-plain", "it-rx-device", "it-rx-payload"], byRule.Keys.Order(StringComparer.Ordinal));
        var device = byRule["it-rx-device"];
        Assert.Equal("test_device", device.Headers["ktm-reject-reason"]);
        Assert.EndsWith(": deviceId regex", device.Headers["ktm-reject-detail"], StringComparison.Ordinal);
        Assert.DoesNotContain("TEST-1", device.Headers["ktm-reject-detail"], StringComparison.Ordinal);
        Assert.Equal(Encoding.UTF8.GetBytes(messages[2].Body), Encoding.UTF8.GetBytes(device.Value));
        Assert.Equal(messages[2].Key, Encoding.UTF8.GetString(device.Key!));

        Assert.Equal(4, app.Counters.Gauge("ktm.datasaver.rules.invalid"));
        Assert.Equal(3, app.Counters.Gauge("ktm.datasaver.rules.active"));
        var logs = app.Logs.GetSnapshot();
        var errors = logs.Where(r => r.Level == LogLevel.Error && r.Message.StartsWith("Rejection rule is not applied", StringComparison.Ordinal)).ToList();
        foreach (var id in new[] { "it-rx-open", "it-rx-look", "it-rx-big", "it-rx-reserved" })
        {
            var record = Assert.Single(errors, r => Field(r, "RuleId") == id);
            Assert.False(string.IsNullOrWhiteSpace(Field(record, "Reason")));
        }

        Assert.Contains("зарезервирован", Field(errors.Single(r => Field(r, "RuleId") == "it-rx-reserved"), "Reason"), StringComparison.Ordinal);
        // в записях лога нет значений из сообщений и шаблона целиком
        Assert.DoesNotContain(logs, r => new[] { Unique, "TEST-1", "PROD-1", "1x" }.Any(v => Text(r).Contains(v, StringComparison.Ordinal)));
        Assert.DoesNotContain(logs, r => Text(r).Contains("(a{1000}){1000}", StringComparison.Ordinal));
        Assert.DoesNotContain(logs, r => Text(r).Contains("(?=a)", StringComparison.Ordinal));
        Assert.Equal(0, app.Terminator.Calls);

        // Исправление шаблона и кода причины действует без рестарта.
        await RuleTable.SetConditionAsync(database, "it-rx-open", """{"regex": [{"var": "payload.s"}, "^b"]}""", ", version = version + 1");
        await RuleTable.UpdateAsync(database, "it-rx-reserved", "reason_code = 'reserved_fixed', version = version + 1");
        await Wait.UntilAsync(() => app.Counters.Gauge("ktm.datasaver.rules.invalid") == 2, "fixed rules picked up by the poll", TimeSpan.FromSeconds(60));
        await run.PublishAsync([Msg(run, 7, "skud", "access_granted", "{\"s\":\"b1\"}"), Msg(run, 8, "skud", "reserved_probe", "{}")]);
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 9, "fixed rules applied", TimeSpan.FromSeconds(60));

        Assert.Equal(4L, await RuleTable.EventCountAsync(database));
        var after = run.Kafka.Read(run.RejectedTopic, 5);
        Assert.Equal(5, after.Count);
        Assert.Contains(after, r => r.Headers["ktm-reject-rule-id"] == "it-rx-reserved" && r.Headers["ktm-reject-reason"] == "reserved_fixed");
        Assert.Contains(after, r => r.Headers["ktm-reject-rule-id"] == "it-rx-open" && r.Headers["ktm-reject-rule-version"] == "2");
    }

    private static string Text(Microsoft.Extensions.Logging.Testing.FakeLogRecord record) =>
        record.Message + " " + string.Join(' ', record.StructuredState?.Select(p => p.Value) ?? []);

    // (г) сквозной таймаут: настоящий NonBacktracking по таймауту не падает, поэтому в тестовом хосте фабрика Regex подменена
    // откатной (шов внутреннего типа Core, без прикладной настройки и без публичного API), предел 20 мс.
    [Fact]
    public async Task RegexTimeout_RejectsWithSystemReason_ForPlainAndNegatedRules_StopsEvaluation_CountsEvalErrors_WarnsOncePerRule()
    {
        await using var database = await PerfDatabase.CreateAsync(postgres.AdminConnectionString);
        // Шаблон катастрофичен на откатном движке на значении «A…A?»; на обычных значениях совпадения нет.
        await RuleTable.InsertAsync(database, "it-slow", "regex: медленное", "skud", """{"regex": [{"var": "payload.s"}, "^(\\w+)+!$"]}""", "slow_admin");
        await RuleTable.InsertAsync(database, "it-slow-neg", "regex: медленное под отрицанием", "cv", """{"!": {"regex": [{"var": "payload.s"}, "^(\\w+)+!$"]}}""", "slow_neg_admin", version: 3);
        var run = await TestRun.CreateAsync(kafka.BootstrapServers, database);
        var policy = new RegexPolicy(TimeSpan.FromMilliseconds(20));
        await using var app = DataSaverInstance.Start(run, FastRefresh, services: s =>
        {
            s.RemoveAll<RegexPolicy>();
            s.AddSingleton(policy);
            s.RemoveAll<ConditionCompiler>();
            s.AddSingleton(new ConditionCompiler(OperatorRegistry.CreateDefault(
                policy, (pattern, timeout) => new Regex(pattern, RegexOptions.CultureInvariant, timeout))));
        });
        await app.WaitReadyAsync();

        var slow = "{\"s\":\"" + new string('A', 32) + "?\"}";
        var messages = new[]
        {
            Msg(run, 0, "skud", "access_granted", slow),
            Msg(run, 1, "cv", "presence_detected", slow),
            Msg(run, 2, "skud", "access_granted", "{\"s\":\"ok\"}"),
            Msg(run, 3, "skud", "access_granted", slow),
        };
        await run.PublishAsync(messages);
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 4, "four messages committed", TimeSpan.FromSeconds(60));

        Assert.Equal(1L, await RuleTable.EventCountAsync(database));
        var rejected = run.Kafka.Read(run.RejectedTopic, 3);
        Assert.Equal(3, rejected.Count);
        Assert.All(rejected, r =>
        {
            Assert.Equal("rule_eval_timeout", r.Headers["ktm-reject-reason"]);
            var detail = r.Headers["ktm-reject-detail"];
            Assert.Contains("(20 мс)", detail, StringComparison.Ordinal);
            Assert.DoesNotContain("AAAA", detail, StringComparison.Ordinal);
            Assert.DoesNotContain("(\\w+)+", detail, StringComparison.Ordinal);
            var number = int.Parse(r.Headers["ktm-source-offset"], System.Globalization.CultureInfo.InvariantCulture);
            Assert.Equal(Encoding.UTF8.GetBytes(messages[number].Body), Encoding.UTF8.GetBytes(r.Value));
        });
        Assert.Equal(["it-slow", "it-slow", "it-slow-neg"], rejected.Select(r => r.Headers["ktm-reject-rule-id"]).Order(StringComparer.Ordinal));
        Assert.Equal("3", rejected.Single(r => r.Headers["ktm-reject-rule-id"] == "it-slow-neg").Headers["ktm-reject-rule-version"]);

        Assert.Equal(3, app.Counters.Sum("ktm.datasaver.rules.eval_errors"));
        Assert.Equal(3, app.Counters["ktm.datasaver.messages.rejected[rule_eval_timeout]"]);
        Assert.Equal(3, app.Counters.Sum("ktm.datasaver.rules.rejected"));
        var logs = app.Logs.GetSnapshot();
        var warnings = logs.Where(r => r.Level == LogLevel.Warning && r.Message.StartsWith("Rule evaluation aborted", StringComparison.Ordinal)).ToList();
        Assert.Equal(["it-slow", "it-slow-neg"], warnings.Select(w => Field(w, "RuleId")).Order(StringComparer.Ordinal));
        Assert.DoesNotContain(logs, r => r.Message.StartsWith("Message rejected by rule", StringComparison.Ordinal));
        Assert.DoesNotContain(logs, r => Text(r).Contains("AAAA", StringComparison.Ordinal) || Text(r).Contains("(\\w+)+", StringComparison.Ordinal));
        Assert.Equal(0, app.Terminator.Calls);
    }
}
