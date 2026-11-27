using System.Text;
using Ktm.DataSaver.Application.Ingestion;
using Ktm.DataSaver.Infrastructure.Kafka;
using Ktm.DataSaver.Infrastructure.Observability;
using Ktm.DataSaver.IntegrationTests.Host;
using Ktm.DataSaver.IntegrationTests.Kafka;
using Ktm.DataSaver.IntegrationTests.Postgres;
using Ktm.DataSaver.IntegrationTests.Support;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Ktm.DataSaver.IntegrationTests.Acceptance;

/// <summary>
/// Задачи 6.1-6.4 (ADR-0005): движок правил на живом сервисе, Kafka + TimescaleDB в контейнерах, правила вносятся SQL-ом под
/// <c>ktm_migrator</c>. Каждый тест работает в собственной БД (таблица правил общая для БД, поэтому общую БД фикстуры не трогаем).
/// </summary>
[Collection(AppCollection.Name)]
public sealed class RulesEngineAcceptanceTests(KafkaFixture kafka, PostgresFixture postgres)
{
    private const string ReloadsChanged = "ktm.datasaver.rules.reloads[changed]";
    private const string SkudTestEvent = """{"==": [{"var": "eventType"}, "test_event"]}""";
    private const string OccurredAt = "2026-11-19T04:58:12Z";

    private static void FastRefresh(Dictionary<string, string?> settings)
    {
        // 5 с - нижняя допустимая граница настройки (ADR-0005 п. 2.10: 5..3600); меньше проверка при старте не пропускает.
        settings["DataSaver:Rules:RefreshIntervalSeconds"] = "5";
        settings["DataSaver:Backoff:InitialMs"] = "200";
        settings["DataSaver:Backoff:MaxMs"] = "1000";
    }

    private static Task WaitReloadsAsync(DataSaverInstance app, int changed, string what) =>
        Wait.UntilAsync(() => app.Counters[ReloadsChanged] >= changed, what, TimeSpan.FromSeconds(60));

    private static (string Key, string Body) Msg(TestRun run, int number, string source, string eventType, string? payload = null, string? extra = null) =>
        ($"{source}:{run.EventId(number)}",
         RuleMessages.Body(source, run.EventId(number), OccurredAt, eventType, payload ?? RuleMessages.DefaultPayload(source, number), extra));

    // 6.1: 100 сообщений, 95 в БД, 5 в rejected (общее правило, правило своего источника, обе границы диапазона, чужие правила не мешают).
    [Fact]
    public async Task HundredMessages_95InJournal_5InRejectedWithRuleHeadersAndOriginalBodies_OffsetAtEnd_MetricsMatch()
    {
        await using var database = await PerfDatabase.CreateAsync(postgres.AdminConnectionString);
        await RuleTable.InsertAsync(database, "it-skud-test-event", "СКУД: тестовое событие", "skud", SkudTestEvent, "test_event");
        await RuleTable.InsertAsync(database, "it-cv-low-confidence", "Видео: низкая уверенность", "cv",
            """{"<=": [0, {"var": "payload.confidence"}, 0.3]}""", "low_confidence");
        await RuleTable.InsertAsync(database, "it-1cdo-draft", "1С:ДО: черновик", "1c-do",
            """{"==": [{"var": "payload.status"}, "draft"]}""", "draft_document");
        await RuleTable.InsertAsync(database, "it-common-test-flag", "Общее: тестовый признак", "*",
            """{"==": [{"var": "payload.isTest"}, true]}""", "test_flag");
        // Выключенное правило, подходящее одному сообщению: в БД оно не должно отбраковывать.
        await RuleTable.InsertAsync(database, "it-disabled", "Выключенное", "*",
            """{"==": [{"var": "eventType"}, "disabled_probe"]}""", "disabled_probe", enabled: false);

        var run = await TestRun.CreateAsync(kafka.BootstrapServers, database);
        await using var app = DataSaverInstance.Start(run);
        await app.WaitReadyAsync();

        // Номер -> (источник, тип, payload); остальные 100 - обычные сообщения по кругу источников.
        var special = new Dictionary<int, (string Source, string EventType, string Payload)>
        {
            [7] = ("skud", "test_event", "{\"direction\":\"in\"}"),                                           // свой источник -> отбраковано
            [23] = ("cv", "presence_detected", "{\"confidence\":0.3}"),                                      // верхняя граница диапазона -> отбраковано
            [41] = ("cv", "presence_detected", "{\"confidence\":0}"),                                        // нижняя граница -> отбраковано
            [58] = ("1c-do", "document_changed", "{\"status\":\"draft\",\"title\":\"Приказ № 5\"}"),        // свой источник, кириллица в теле
            [77] = ("skud", "access_granted", "{\"isTest\":true}"),                                          // общее правило
            [12] = ("skud", "access_granted", "{\"status\":\"draft\"}"),                                     // подходит правилу 1c-do, но не своему -> в БД
            [30] = ("cv", "test_event", "{\"confidence\":0.93}"),                                            // подходит правилу skud, но не своему -> в БД
            [50] = ("cv", "presence_detected", "{\"confidence\":0.31}"),                                     // сразу выше границы -> в БД
            [64] = ("cv", "presence_detected", "{\"confidence\":-0.01}"),                                    // сразу ниже границы -> в БД
            [85] = ("1c-do", "disabled_probe", "{\"taskId\":\"T-85\"}"),                                     // выключенное правило -> в БД
        };
        var rejectedRuleByNumber = new Dictionary<int, string>
        {
            [7] = "it-skud-test-event", [23] = "it-cv-low-confidence", [41] = "it-cv-low-confidence",
            [58] = "it-1cdo-draft", [77] = "it-common-test-flag",
        };

        var messages = Enumerable.Range(0, 100).Select(i =>
        {
            if (special.TryGetValue(i, out var s)) return Msg(run, i, s.Source, s.EventType, s.Payload);
            var source = RuleMessages.Sources[i % 3];
            return Msg(run, i, source, RuleMessages.DefaultType(source));
        }).ToList();

        await run.PublishAsync(messages);
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 100, "offsets of all 100 messages committed");
        var records = run.Kafka.Read(run.RejectedTopic, 5);

        Assert.Equal(95L, await RuleTable.EventCountAsync(database));
        Assert.Equal(5, records.Count);
        Assert.Empty(run.Kafka.Read(run.RejectedTopic, 6, TimeSpan.FromSeconds(2)).Skip(5));
        Assert.Equal(rejectedRuleByNumber.Keys.Order(), records.Select(r => int.Parse(r.Headers["ktm-source-offset"], System.Globalization.CultureInfo.InvariantCulture)).Order());
        foreach (var record in records)
        {
            var number = int.Parse(record.Headers["ktm-source-offset"], System.Globalization.CultureInfo.InvariantCulture);
            var expectedRule = rejectedRuleByNumber[number];
            Assert.Equal(expectedRule, record.Headers["ktm-reject-rule-id"]);
            Assert.False(string.IsNullOrWhiteSpace(record.Headers["ktm-reject-rule-name"]));
            Assert.Equal("1", record.Headers["ktm-reject-rule-version"]);
            Assert.Equal(expectedRule switch
            {
                "it-skud-test-event" => "test_event",
                "it-cv-low-confidence" => "low_confidence",
                "it-1cdo-draft" => "draft_document",
                _ => "test_flag",
            }, record.Headers["ktm-reject-reason"]);
            var detail = record.Headers["ktm-reject-detail"];
            Assert.Contains(expectedRule, detail, StringComparison.Ordinal);
            Assert.DoesNotContain("card:0012345678", detail, StringComparison.Ordinal);
            // Тело и ключ - исходные, байт в байт.
            var original = messages[number];
            Assert.Equal(Encoding.UTF8.GetBytes(original.Body), Encoding.UTF8.GetBytes(record.Value));
            Assert.Equal(original.Key, Encoding.UTF8.GetString(record.Key!));
            Assert.Equal(run.RawTopic, record.Headers["ktm-source-topic"]);
        }

        Assert.Equal(5, app.Counters.Sum("ktm.datasaver.rules.rejected"));
        Assert.Equal(2, app.Counters["ktm.datasaver.rules.rejected[skud]"]);
        Assert.Equal(2, app.Counters["ktm.datasaver.rules.rejected[cv]"]);
        Assert.Equal(1, app.Counters["ktm.datasaver.rules.rejected[1c-do]"]);
        Assert.Equal(5, app.Counters["ktm.datasaver.messages.rejected[rule_matched]"]);
        Assert.Equal(95, app.Counters.Written);
        Assert.Equal(0, app.Terminator.Calls);
    }

    // 6.2: изменение таблицы вступает в силу без рестарта процесса (опрос), включая новое правило и правку условия без version.
    [Fact]
    public async Task RuleTableChanges_TakeEffectWithoutRestart_EnableDisableInsertAndConditionChange()
    {
        await using var database = await PerfDatabase.CreateAsync(postgres.AdminConnectionString);
        await RuleTable.InsertAsync(database, "it-r", "Тестовое событие СКУД", "skud", SkudTestEvent, "test_event", enabled: false);
        var run = await TestRun.CreateAsync(kafka.BootstrapServers, database);
        await using var app = DataSaverInstance.Start(run, FastRefresh);
        await app.WaitReadyAsync();
        await WaitReloadsAsync(app, 1, "first load of the rule set");

        // Шаг 1: правило выключено - подходящее событие записано.
        await run.PublishAsync([Msg(run, 0, "skud", "test_event")]);
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 1, "event 0 committed");
        Assert.Equal(1L, await RuleTable.EventCountAsync(database));

        // Шаг 2: включено с увеличением version - подходящее событие уходит в rejected.
        await RuleTable.UpdateAsync(database, "it-r", "enabled = true, version = version + 1");
        await WaitReloadsAsync(app, 2, "enabling picked up by the poll");
        await run.PublishAsync([Msg(run, 1, "skud", "test_event")]);
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 2, "event 1 committed");
        var rejected = run.Kafka.Read(run.RejectedTopic, 1);
        Assert.Single(rejected);
        Assert.Equal("it-r", rejected[0].Headers["ktm-reject-rule-id"]);
        Assert.Equal("2", rejected[0].Headers["ktm-reject-rule-version"]);
        Assert.Equal(1L, await RuleTable.EventCountAsync(database));

        // Шаг 3: выключено - снова в БД.
        await RuleTable.UpdateAsync(database, "it-r", "enabled = false");
        await WaitReloadsAsync(app, 3, "disabling picked up by the poll");
        await run.PublishAsync([Msg(run, 2, "skud", "test_event")]);
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 3, "event 2 committed");
        Assert.Equal(2L, await RuleTable.EventCountAsync(database));

        // Новое правило через INSERT подхватывается так же.
        await RuleTable.InsertAsync(database, "it-new", "Новое правило", "skud", """{"==": [{"var": "eventType"}, "probe"]}""", "probe");
        await WaitReloadsAsync(app, 4, "inserted rule picked up by the poll");
        await run.PublishAsync([Msg(run, 3, "skud", "probe")]);
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 4, "event 3 committed");
        Assert.Equal(2L, await RuleTable.EventCountAsync(database));
        Assert.Equal(2, run.Kafka.Read(run.RejectedTopic, 2).Count);

        // Условие изменено без увеличения version: действует новое условие, в логе Warning с id правила.
        await RuleTable.SetConditionAsync(database, "it-new", """{"==": [{"var": "eventType"}, "probe2"]}""");
        await WaitReloadsAsync(app, 5, "condition change picked up by the poll");
        var warnings = app.Logs.GetSnapshot().Where(r =>
            r.Level == LogLevel.Warning && r.Message.Contains("without increasing version", StringComparison.Ordinal)).ToList();
        // Выключение шага 3 тоже сделано без version (fingerprint включает enabled), поэтому it-r предупреждён один раз, it-new - один раз.
        Assert.Single(warnings, w => w.Message.Contains("it-new", StringComparison.Ordinal));
        Assert.Single(warnings, w => w.Message.Contains("it-r ", StringComparison.Ordinal));
        Assert.Equal(2, warnings.Count);
        await run.PublishAsync([Msg(run, 4, "skud", "probe"), Msg(run, 5, "skud", "probe2")]);
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 6, "events 4 and 5 committed");
        Assert.Equal(3L, await RuleTable.EventCountAsync(database));
        Assert.Equal(3, run.Kafka.Read(run.RejectedTopic, 3).Count);
        Assert.Equal(0, app.Terminator.Calls);
    }

    // 6.3: ошибочные правила не ломают сервис и не применяются, остальные работают; исправление действует без рестарта.
    [Fact]
    public async Task InvalidRules_ServiceStartsAndIngests_GoodRuleApplies_ErrorsLogged_RulesInvalidIs3_ThenFixedWithoutRestart()
    {
        await using var database = await PerfDatabase.CreateAsync(postgres.AdminConnectionString);
        await RuleTable.InsertAsync(database, "it-bad-in", "Неподдерживаемый оператор", "skud", """{"in": [{"var": "eventType"}, ["a"]]}""", "bad_in");
        await RuleTable.InsertAsync(database, "it-bad-path", "Неизвестное поле", "skud", """{"==": [{"var": "unknownField"}, "x"]}""", "bad_path");
        await RuleTable.InsertAsync(database, "it-bad-range", "Перевёрнутый диапазон", "cv", """{"<=": [5, {"var": "payload.confidence"}, 1]}""", "bad_range");
        await RuleTable.InsertAsync(database, "it-good", "Тестовое событие СКУД", "skud", SkudTestEvent, "test_event");
        var run = await TestRun.CreateAsync(kafka.BootstrapServers, database);
        await using var app = DataSaverInstance.Start(run, FastRefresh);
        await app.WaitReadyAsync();

        await run.PublishAsync(
        [
            Msg(run, 0, "skud", "a"),
            Msg(run, 1, "skud", "access_granted", extra: "\"unknownField\":\"x\""),
            Msg(run, 2, "cv", "presence_detected", "{\"confidence\":3}"),
            Msg(run, 3, "skud", "test_event"),
        ]);
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 4, "four messages committed");

        Assert.Equal(3L, await RuleTable.EventCountAsync(database));
        var rejected = run.Kafka.Read(run.RejectedTopic, 1);
        Assert.Single(rejected);
        Assert.Equal("it-good", rejected[0].Headers["ktm-reject-rule-id"]);
        Assert.Equal(3, app.Counters.Gauge("ktm.datasaver.rules.invalid"));
        Assert.Equal(1, app.Counters.Gauge("ktm.datasaver.rules.active"));
        var errors = app.Logs.GetSnapshot().Where(r => r.Level == LogLevel.Error && r.Message.StartsWith("Rejection rule is not applied", StringComparison.Ordinal)).ToList();
        foreach (var id in new[] { "it-bad-in", "it-bad-path", "it-bad-range" })
        {
            var record = Assert.Single(errors, r => (string?)r.StructuredState!.Single(p => p.Key == "RuleId").Value == id);
            Assert.False(string.IsNullOrWhiteSpace(record.StructuredState!.Single(p => p.Key == "Reason").Value));
        }

        // Исправление правки через UPDATE: правило начинает действовать на следующем снимке, процесс не перезапускался.
        await RuleTable.SetConditionAsync(database, "it-bad-in", """{"==": [{"var": "eventType"}, "a"]}""", ", version = version + 1");
        await WaitReloadsAsync(app, 2, "fixed rule picked up by the poll");
        await run.PublishAsync([Msg(run, 4, "skud", "a")]);
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 5, "fifth message committed");

        Assert.Equal(3L, await RuleTable.EventCountAsync(database));
        var all = run.Kafka.Read(run.RejectedTopic, 2);
        Assert.Equal(2, all.Count);
        Assert.Contains(all, r => r.Headers["ktm-reject-rule-id"] == "it-bad-in" && r.Headers["ktm-reject-rule-version"] == "2");
        Assert.Equal(2, app.Counters.Gauge("ktm.datasaver.rules.invalid"));
        Assert.Equal(0, app.Terminator.Calls);
    }

    // 6.4: публикация отбракованного правилом недоступна - offset не коммитится, пачка не пишется повторно, счёт и вывод после успеха.
    [Fact]
    public async Task RuleRejectedPublishUnavailable_OffsetNotCommitted_BatchNotRewritten_CountedOnlyAfterPublish()
    {
        await using var database = await PerfDatabase.CreateAsync(postgres.AdminConnectionString);
        await RuleTable.InsertAsync(database, "it-skud-test-event", "СКУД: тестовое событие", "skud", SkudTestEvent, "test_event");
        var run = await TestRun.CreateAsync(kafka.BootstrapServers, database);
        var gate = new PublishGate();
        await using var app = DataSaverInstance.Start(run, FastRefresh, services: s =>
        {
            s.RemoveAll<IRejectionPublisher>();
            s.AddSingleton<IRejectionPublisher>(sp => new FailingWhileClosedPublisher(
                new ObservedRejectionPublisher(
                    sp.GetRequiredService<KafkaRejectionPublisher>(), sp.GetRequiredService<DataSaverMetrics>(),
                    sp.GetRequiredService<ILogger<ObservedRejectionPublisher>>()),
                gate));
        });
        await app.WaitReadyAsync();

        await run.PublishAsync(
        [
            Msg(run, 0, "skud", "access_granted"), Msg(run, 1, "skud", "test_event"), Msg(run, 2, "cv", "presence_detected"),
            Msg(run, 3, "skud", "test_event"), Msg(run, 4, "1c-do", "task_assigned"),
        ]);
        await Wait.UntilAsync(() => gate.Attempts >= 3, "repeated publish attempts while the topic is unavailable", TimeSpan.FromSeconds(30));

        // Запись принятых идёт до публикации (ADR-0002 D-6) и не повторяется; commit не выполнен; ничего не опубликовано и не посчитано.
        Assert.Equal(0L, await run.CommittedAsync());
        Assert.Equal(3L, await RuleTable.EventCountAsync(database));
        Assert.Empty(run.Kafka.Read(run.RejectedTopic, 1, TimeSpan.FromSeconds(2)));
        Assert.Equal(0, app.Counters.Sum("ktm.datasaver.rules.rejected"));
        Assert.Equal(0, app.Counters["ktm.datasaver.messages.rejected[rule_matched]"]);

        gate.Open();
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 5, "offsets committed after the publication succeeded");

        Assert.Equal(2, run.Kafka.Read(run.RejectedTopic, 2).Count);
        Assert.Equal(3L, await RuleTable.EventCountAsync(database));
        Assert.Equal(3, app.Counters.Written);
        Assert.Equal(0, app.Counters.Duplicates);
        Assert.Equal(2, app.Counters.Sum("ktm.datasaver.rules.rejected"));
        Assert.Equal(0, app.Terminator.Calls);
    }

    private sealed class PublishGate
    {
        private volatile bool _open;
        private int _attempts;

        public int Attempts => Volatile.Read(ref _attempts);

        public bool IsOpen => _open;

        public void Open() => _open = true;

        public void CountAttempt() => Interlocked.Increment(ref _attempts);
    }

    private sealed class FailingWhileClosedPublisher(IRejectionPublisher inner, PublishGate gate) : IRejectionPublisher
    {
        public ValueTask<RejectionPublishResult> PublishAsync(IReadOnlyList<RejectedMessage> rejected, CancellationToken cancellationToken)
        {
            if (gate.IsOpen) return inner.PublishAsync(rejected, cancellationToken);
            gate.CountAttempt();
            return ValueTask.FromResult(RejectionPublishResult.Failure("rejected topic unavailable (test)"));
        }
    }
}
