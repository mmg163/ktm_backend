using System.Net;
using System.Net.Sockets;
using Ktm.DataSaver.IntegrationTests.Host;
using Ktm.DataSaver.IntegrationTests.Kafka;
using Ktm.DataSaver.IntegrationTests.Postgres;
using Ktm.DataSaver.IntegrationTests.Support;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Ktm.DataSaver.IntegrationTests.Acceptance;

/// <summary>
/// Задача 4.4: загрузка правил на живом сервисе - недоступность БД правил после загрузки и при старте, отсутствие таблицы
/// (код выхода 78), аварийное отключение правил. Собственный контейнер Postgres: тесты останавливают и ломают БД.
/// </summary>
[Collection(AppCollection.Name)]
public sealed class RulesLoadingAcceptanceTests(KafkaFixture kafka)
{
    private const string RuleSql = """
        INSERT INTO ktm.rejection_rules (rule_id, name, source, condition, reason_code, enabled)
        VALUES ('it-test-event', 'Тестовое событие', 'skud', '{"==": [{"var": "eventType"}, "test_event"]}'::jsonb, 'test_data', true)
        """;

    private const string OtherRuleSql = """
        INSERT INTO ktm.rejection_rules (rule_id, name, source, condition, reason_code, enabled)
        VALUES ('it-other-type', 'Другой тип', 'skud', '{"==": [{"var": "eventType"}, "other_type"]}'::jsonb, 'other_data', true)
        """;

    private static void Tune(Dictionary<string, string?> settings)
    {
        settings["DataSaver:Backoff:InitialMs"] = "200";
        settings["DataSaver:Backoff:MaxMs"] = "1000";
        settings["DataSaver:Rules:RefreshIntervalSeconds"] = "5";
    }

    private static async Task AsAdminAsync(StoppableDatabase db, string sql)
    {
        await using var connection = new NpgsqlConnection(db.AdminConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private static (string Key, string Body) Message(TestRun run, int number, string eventType) =>
        ($"skud:{run.EventId(number)}", run.Json(run.EventId(number)).Replace("\"access_granted\"", $"\"{eventType}\"", StringComparison.Ordinal));

    private static IEnumerable<(string, string)> Messages(TestRun run, int from, int count, string eventType) =>
        Enumerable.Range(from, count).Select(i => Message(run, i, eventType));

    [Fact]
    public async Task DatabaseDownAfterLoad_RejectionContinuesByPreviousSet_WarnsAndCountsFailed_ThenNewSetIsPickedUp()
    {
        await using var db = await StoppableDatabase.StartAsync();
        await AsAdminAsync(db, RuleSql);
        var run = await TestRun.CreateAsync(kafka.BootstrapServers, db);
        await using var app = DataSaverInstance.Start(run, Tune);
        await app.WaitReadyAsync();

        await run.PublishAsync(Messages(run, 0, 3, "test_event").Concat(Messages(run, 3, 2, "access_granted")));
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 5, "first five messages committed");
        Assert.Equal(2L, await run.RowsAsync());
        Assert.Equal(3, run.Kafka.Read(run.RejectedTopic, 3).Count);

        await db.StopAsync();
        await Wait.UntilAsync(() => app.Counters["ktm.datasaver.rules.reloads[failed]"] >= 1, "failed reload while the database is down");
        Assert.Contains(app.Logs.GetSnapshot(), r => r.Level == LogLevel.Warning && r.Message.StartsWith("Rule reload failed", StringComparison.Ordinal));

        // БД правил недоступна, а набор прежний: правило продолжает отбраковывать (приём одних отбракованных БД не требует).
        await run.PublishAsync(Messages(run, 5, 3, "test_event"));
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 8, "messages rejected by the previous rule set while the database is down");
        Assert.Equal(6, run.Kafka.Read(run.RejectedTopic, 6).Count);
        Assert.DoesNotContain(app.Logs.GetSnapshot(), r => r.Message.Contains(DatabaseProvisioner.AppPassword, StringComparison.Ordinal));
        Assert.Equal(0, app.Terminator.Calls);

        await db.RestartAsync();
        await AsAdminAsync(db, OtherRuleSql);
        await Wait.UntilAsync(() => app.Counters["ktm.datasaver.rules.reloads[changed]"] >= 2, "updated rule set after recovery");

        await run.PublishAsync(Messages(run, 8, 2, "other_type").Concat(Messages(run, 10, 1, "access_granted")));
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 11, "messages after recovery committed");
        Assert.Equal(3L, await run.RowsAsync());
        Assert.Equal(8, run.Kafka.Read(run.RejectedTopic, 8).Count);
    }

    [Fact]
    public async Task StartWithDatabaseDown_ServiceIsNotReadyAndReadsNothing_ThenStartsWhenDatabaseAppears()
    {
        await using var db = await StoppableDatabase.StartAsync();
        await AsAdminAsync(db, RuleSql);
        var run = await TestRun.CreateAsync(kafka.BootstrapServers, db);
        await run.PublishAsync(Messages(run, 0, 4, "access_granted").Concat(Messages(run, 4, 2, "test_event")));
        await db.StopAsync();

        await using var app = DataSaverInstance.Start(run, Tune);
        await Wait.UntilAsync(() => app.Counters["ktm.datasaver.rules.reloads[failed]"] >= 2, "retries of the first rule load");

        Assert.Equal(HttpStatusCode.ServiceUnavailable, await app.ReadyStatusAsync());
        using (var live = await app.Client.GetAsync("/health/live"))
        {
            Assert.Equal(HttpStatusCode.OK, live.StatusCode);
        }

        Assert.Equal(0L, app.Counters.Read);
        Assert.Equal(0L, await run.CommittedAsync());
        Assert.Contains(app.Logs.GetSnapshot(), r => r.Level == LogLevel.Warning && r.Message.StartsWith("Rules are not loaded yet", StringComparison.Ordinal));
        Assert.Equal(0, app.Terminator.Calls);

        await db.RestartAsync();
        await app.WaitReadyAsync(TimeSpan.FromSeconds(60));
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 6, "messages processed after the first rule load");

        Assert.Equal(4L, await run.RowsAsync());
        Assert.Equal(2, run.Kafka.Read(run.RejectedTopic, 2).Count);
    }

    [Fact]
    public async Task StartWithoutRuleTable_ProcessExitsWithCode78_AndMessageHasNoSecrets()
    {
        await using var db = await StoppableDatabase.StartAsync();
        await AsAdminAsync(db, "DROP TABLE ktm.rejection_rules");
        var run = await TestRun.CreateAsync(kafka.BootstrapServers, db);
        var connection = new NpgsqlConnectionStringBuilder(run.Db.AppConnectionString);

        var result = await ConfigExitCodeTests.RunAsync(new Dictionary<string, string>
        {
            ["DataSaver__Kafka__BootstrapServers"] = run.Bootstrap,
            ["DataSaver__Kafka__Topic"] = run.RawTopic,
            ["DataSaver__Kafka__RejectedTopic"] = run.RejectedTopic,
            ["DataSaver__Kafka__GroupId"] = run.Group,
            ["DataSaver__Postgres__Host"] = connection.Host!,
            ["DataSaver__Postgres__Port"] = connection.Port.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["DataSaver__Postgres__Database"] = connection.Database!,
            ["DataSaver__Postgres__Username"] = connection.Username!,
            ["DataSaver__Postgres__Password"] = connection.Password!,
            ["DataSaver__Postgres__SslMode"] = "Disable",
            ["DataSaver__Health__Port"] = FreePort().ToString(System.Globalization.CultureInfo.InvariantCulture),
        });

        Assert.Equal(78, result.ExitCode);
        var output = result.Stdout + result.Stderr;
        Assert.Contains("42P01", output, StringComparison.Ordinal);
        Assert.DoesNotContain(connection.Password!, output, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RulesDisabled_TableIsNotRead_ServiceStartsWithoutItAndAcceptsEverything()
    {
        await using var db = await StoppableDatabase.StartAsync();
        await AsAdminAsync(db, RuleSql);
        // Чтение таблицы завершило бы процесс кодом 78, а правило отбраковало бы тестовые события.
        await AsAdminAsync(db, "DROP TABLE ktm.rejection_rules");
        var run = await TestRun.CreateAsync(kafka.BootstrapServers, db);
        await using var app = DataSaverInstance.Start(run, settings => settings["DataSaver:Rules:Enabled"] = "false");

        await app.WaitReadyAsync();
        await run.PublishAsync(Messages(run, 0, 3, "test_event"));
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 3, "messages committed without rules");

        Assert.Equal(3L, await run.RowsAsync());
        Assert.Equal(0, app.Terminator.Calls);
        Assert.Equal(0L, app.Counters["ktm.datasaver.rules.reloads[changed]"] + app.Counters["ktm.datasaver.rules.reloads[failed]"]);
    }

    private static int FreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }
}
